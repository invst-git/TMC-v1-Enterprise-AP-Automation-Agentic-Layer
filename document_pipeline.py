import os
from typing import Any, Dict, List, Optional

from extraction_validation import extract_and_validate_invoice, finalize_source_document_extraction_best_effort
from invoice_db import save_invoice_to_db
from matching_orchestration import match_invoice_with_review
from pdf_segmentation import build_segmentation_log_lines, segment_pdf_document
from realtime_events import publish_live_update
from source_document_tracking import persist_source_document_segmentation_best_effort
from source_document_tracking import update_source_document_segment_best_effort
from user_facing_errors import DuplicateInvoiceBlockedError, get_user_facing_message
from workflow_task_queue import enqueue_workflow_task


def _segment_label(
    original_filename: Optional[str],
    segment: Dict[str, Any],
    segment_index: int,
    total_segments: int,
) -> str:
    base = original_filename or os.path.basename(segment.get("path") or "")
    if total_segments <= 1:
        return base
    return (
        f"{base} segment {segment_index}/{total_segments} "
        f"(pages {segment.get('page_from')}-{segment.get('page_to')})"
    )


def _mark_invoice_received_best_effort(invoice_id: str, source_document_id: Optional[str]) -> None:
    try:
        from agent_db import set_workflow_state

        set_workflow_state(
            "invoice",
            invoice_id,
            "received",
            current_stage="intake",
            confidence=1.0,
            event_type="invoice_persisted",
            reason="Invoice was persisted and entered the invoice lifecycle.",
            metadata={
                "invoice_id": invoice_id,
                "source_document_id": source_document_id,
            },
        )
    except Exception:
        return


def _mark_source_document_extraction_queued_best_effort(source_document_id: Optional[str], queued_count: int) -> None:
    if not source_document_id:
        return
    try:
        from agent_db import set_workflow_state, update_source_document

        update_source_document(
            source_document_id,
            extraction_status="queued",
            metadata={
                "extraction": {
                    "queued_segment_count": queued_count,
                }
            },
        )
        set_workflow_state(
            "source_document",
            source_document_id,
            "queued_for_extraction",
            current_stage="extraction",
            event_type="extraction_queued",
            reason=f"Queued {queued_count} extraction task(s) for worker processing.",
            metadata={
                "source_document_id": source_document_id,
                "queued_segment_count": queued_count,
            },
        )
    except Exception:
        return


def _agent_worker_enabled() -> bool:
    value = os.getenv("ENABLE_AGENT_WORKER")
    if value is None:
        return False
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _process_saved_invoice_file_sync_from_segments(
    *,
    full_path: str,
    from_email: Optional[str],
    message_id: str,
    vendor_id_override: Optional[str],
    original_filename: Optional[str],
    source_document_id: Optional[str],
    segmentation,
    persisted_segmentation,
    logs: List[str],
) -> Dict[str, Any]:
    invoice_ids: List[str] = []
    validation_results = []
    segments = [segment.to_dict() for segment in segmentation.segments] if segmentation.segments else []
    if not segments:
        segments = [{"page_from": 1, "page_to": 1, "confidence": 1.0, "path": full_path}]
    persisted_segments = (persisted_segmentation or {}).get("segments") or []

    for index, segment in enumerate(segments, start=1):
        segment_path = segment.get("path") or full_path
        label = _segment_label(original_filename, segment, index, len(segments))
        segment_record = persisted_segments[index - 1] if index - 1 < len(persisted_segments) else None
        segment_record_id = segment_record.get("id") if segment_record else None
        if len(segments) > 1:
            logs.append(f"Processing {label}; source file {segment_path}")

        validation = extract_and_validate_invoice(
            segment_path,
            source_document_id=source_document_id,
            source_document_segment_id=segment_record_id,
            from_email=from_email,
            vendor_id_override=vendor_id_override,
        )
        validation_results.append(validation)
        json_path = validation.json_path
        if not json_path:
            logs.append(f"OCR/parse skipped or failed for {label}")
            if validation.issues:
                logs.append(f"Extraction validation for {label}: {validation.issues[0].message}")
            continue
        logs.append(f"OCR/parse completed for {label}; JSON saved to {json_path}")
        if validation.issues:
            logs.append(
                f"Extraction validation for {label}: "
                + "; ".join(issue.message for issue in validation.issues[:3])
            )
        if validation.review_item_id:
            logs.append(f"Extraction review queued for {label}; review item {validation.review_item_id}")
        if getattr(validation, "blocks_persistence", False):
            duplicate_ids = [candidate["id"] for candidate in validation.duplicate_candidates if candidate.get("confidence", 0) >= 0.75]
            logs.append(f"Duplicate invoice blocked for {label}; existing invoice(s) {', '.join(duplicate_ids[:3]) or 'already exist'}")
            _, segment_update_warning = update_source_document_segment_best_effort(
                segment_record_id,
                status="duplicate_blocked",
                metadata={
                    "duplicate_detection": {
                        "decision": "blocked",
                        "duplicate_invoice_ids": duplicate_ids[:10],
                        "review_item_id": validation.review_item_id,
                    }
                },
            )
            if segment_update_warning:
                logs.append(f"Source segment duplicate update skipped for {label}: {segment_update_warning}")
            publish_live_update(
                "invoice.duplicate_blocked",
                {
                    "sourceDocumentId": source_document_id,
                    "sourceDocumentSegmentId": segment_record_id,
                    "duplicateInvoiceIds": duplicate_ids[:10],
                },
            )
            continue

        try:
            save_result = save_invoice_to_db(json_path, segment_path, from_email, message_id, vendor_id_override)
            if not save_result:
                logs.append(f"Failed to save {label} to Supabase")
                continue
            invoice_id = save_result.invoice_id
            invoice_ids.append(invoice_id)
            logs.append(f"Invoice saved to Supabase with id={invoice_id}")
            _mark_invoice_received_best_effort(invoice_id, source_document_id)
            _, segment_update_warning = update_source_document_segment_best_effort(
                segment_record_id,
                status=None if validation.requires_review else "persisted",
                invoice_id=invoice_id,
                metadata={
                    "persistence": {
                        "invoice_id": invoice_id,
                        "decision": validation.decision,
                    }
                },
            )
            if segment_update_warning:
                logs.append(f"Source segment linkage skipped for {label}: {segment_update_warning}")
            publish_live_update(
                "invoice.persisted",
                {
                    "invoiceId": invoice_id,
                    "sourceDocumentId": source_document_id,
                },
            )
        except DuplicateInvoiceBlockedError as exc:
            logs.append(get_user_facing_message(exc))
            publish_live_update(
                "invoice.duplicate_blocked",
                {
                    "sourceDocumentId": source_document_id,
                    "message": get_user_facing_message(exc),
                },
            )
            continue
        except Exception as exc:
            logs.append(f"Could not save {label}: {get_user_facing_message(exc)}")
            continue

        try:
            match_outcome = match_invoice_with_review(
                invoice_id,
                source_document_id=source_document_id,
            )
            if match_outcome.matched_po_id:
                logs.append(f"Invoice {invoice_id} matched to PO {match_outcome.matched_po_id}")
            else:
                logs.append(
                    f"Invoice {invoice_id} not auto-matched ({match_outcome.reason}); "
                    f"workflow state {match_outcome.workflow_state}"
                )
                if match_outcome.review_item_id:
                    logs.append(
                        f"PO matching review queued for invoice {invoice_id}; "
                        f"review item {match_outcome.review_item_id}"
                    )
            publish_live_update(
                "invoice.matching_updated",
                {
                    "invoiceId": invoice_id,
                    "matchedPoId": match_outcome.matched_po_id,
                    "workflowState": match_outcome.workflow_state,
                    "reviewItemId": match_outcome.review_item_id,
                },
            )
        except Exception as exc:
            logs.append(f"PO matching could not complete for invoice {invoice_id}: {get_user_facing_message(exc)}")

    extraction_status = finalize_source_document_extraction_best_effort(source_document_id, validation_results)
    if extraction_status:
        logs.append(f"Source document extraction finalized with status {extraction_status}")
        publish_live_update(
            "source_document.extraction_finalized",
            {
                "sourceDocumentId": source_document_id,
                "extractionStatus": extraction_status,
            },
        )

    return {
        "logs": logs,
        "invoice_ids": invoice_ids,
        "segmentation": segmentation.to_dict(),
        "validation": [result.to_dict() for result in validation_results],
    }


def process_saved_invoice_file(
    full_path: str,
    *,
    from_email: Optional[str],
    message_id: str,
    vendor_id_override: Optional[str] = None,
    original_filename: Optional[str] = None,
    source_document_id: Optional[str] = None,
) -> Dict[str, Any]:
    logs: List[str] = []

    segmentation = segment_pdf_document(full_path)
    logs.extend(build_segmentation_log_lines(original_filename or os.path.basename(full_path), segmentation))
    persisted_segmentation, segmentation_warning = persist_source_document_segmentation_best_effort(
        source_document_id,
        segmentation,
    )
    if persisted_segmentation:
        logs.append(
            f"Persisted {len(persisted_segmentation['segments'])} source document segment record(s) "
            f"for source document {source_document_id}"
        )
        publish_live_update(
            "source_document.segmented",
            {
                "sourceDocumentId": source_document_id,
                "segmentCount": len(persisted_segmentation["segments"]),
            },
        )
    elif segmentation_warning:
        logs.append(f"Source document segmentation persistence skipped: {segmentation_warning}")

    if not source_document_id or not _agent_worker_enabled():
        return _process_saved_invoice_file_sync_from_segments(
            full_path=full_path,
            from_email=from_email,
            message_id=message_id,
            vendor_id_override=vendor_id_override,
            original_filename=original_filename,
            source_document_id=source_document_id,
            segmentation=segmentation,
            persisted_segmentation=persisted_segmentation,
            logs=logs,
        )

    segments = [segment.to_dict() for segment in segmentation.segments] if segmentation.segments else []
    if not segments:
        segments = [{"page_from": 1, "page_to": 1, "confidence": 1.0, "path": full_path}]
    persisted_segments = (persisted_segmentation or {}).get("segments") or []
    queued_tasks: List[Dict[str, Any]] = []

    for index, segment in enumerate(segments, start=1):
        segment_path = segment.get("path") or full_path
        label = _segment_label(original_filename, segment, index, len(segments))
        segment_record = persisted_segments[index - 1] if index - 1 < len(persisted_segments) else None
        segment_record_id = segment_record.get("id") if segment_record else None
        task_payload = {
            "invoice_path": segment_path,
            "message_id": message_id,
            "from_email": from_email,
            "vendor_id_override": vendor_id_override,
            "original_filename": original_filename,
            "source_document_id": source_document_id,
            "source_document_segment_id": segment_record_id,
            "amount_tolerance": 1.0,
            "percent_tolerance": 0.02,
        }
        dedupe_key = (
            f"source_document_segment:{segment_record_id}:extract.validate"
            if segment_record_id
            else f"source_document:{source_document_id}:extract.validate:{index}"
        )
        try:
            task = enqueue_workflow_task(
                task_type="extract.validate",
                entity_type="source_document_segment" if segment_record_id else "source_document",
                entity_id=segment_record_id or source_document_id,
                source_document_id=source_document_id,
                dedupe_key=dedupe_key,
                payload=task_payload,
            )
        except Exception:
            if queued_tasks:
                raise
            return _process_saved_invoice_file_sync_from_segments(
                full_path=full_path,
                from_email=from_email,
                message_id=message_id,
                vendor_id_override=vendor_id_override,
                original_filename=original_filename,
                source_document_id=source_document_id,
                segmentation=segmentation,
                persisted_segmentation=persisted_segmentation,
                logs=logs,
            )

        queued_tasks.append(
            {
                "id": task["id"],
                "segment_label": label,
                "source_document_segment_id": segment_record_id,
                "segment_path": segment_path,
            }
        )
        logs.append(f"Queued extraction for {label}; task {task['id']}")

    _mark_source_document_extraction_queued_best_effort(source_document_id, len(queued_tasks))
    publish_live_update(
        "source_document.extraction_queued",
        {
            "sourceDocumentId": source_document_id,
            "queuedTaskCount": len(queued_tasks),
        },
    )
    return {
        "logs": logs,
        "invoice_ids": [],
        "segmentation": segmentation.to_dict(),
        "validation": [],
        "queued_tasks": queued_tasks,
    }
