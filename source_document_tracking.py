from typing import Any, Dict, Optional, Tuple

from pdf_segmentation import SegmentationResult


def _get_agent_db_ops():
    from agent_db import (
        record_source_document_segments,
        set_workflow_state,
        update_source_document,
        update_source_document_segment,
    )

    return update_source_document, record_source_document_segments, set_workflow_state, update_source_document_segment


def _segmentation_status(segmentation: SegmentationResult) -> str:
    if segmentation.reason == "not_pdf":
        return "not_applicable"
    if segmentation.used_segmentation:
        return "split"
    if segmentation.reason.startswith("segmentation_error:"):
        return "failed_fallback"
    return "single_document"


def persist_source_document_segmentation(
    source_document_id: str,
    segmentation: SegmentationResult,
) -> Dict[str, Any]:
    update_source_document, record_source_document_segments, set_workflow_state, _ = _get_agent_db_ops()

    source_document = update_source_document(
        source_document_id,
        page_count=segmentation.page_count,
        segmentation_status=_segmentation_status(segmentation),
        metadata={
            "segmentation": {
                "reason": segmentation.reason,
                "strategy": segmentation.strategy,
                "used_segmentation": segmentation.used_segmentation,
                "metadata_path": segmentation.metadata_path,
            }
        },
    )

    segments = [segment.to_dict() for segment in segmentation.segments] if segmentation.segments else []
    if not segments:
        segments = [
            {
                "page_from": 1,
                "page_to": max(1, segmentation.page_count),
                "confidence": 1.0,
                "path": segmentation.source_path or None,
                "reason": segmentation.reason,
            }
        ]

    recorded_segments = record_source_document_segments(
        source_document_id,
        [
            {
                "segment_index": index,
                "page_from": segment["page_from"],
                "page_to": segment["page_to"],
                "segment_path": segment.get("path"),
                "confidence": segment.get("confidence"),
                "status": "ready",
                "metadata": {
                    "reason": segment.get("reason"),
                    "segmentation_reason": segmentation.reason,
                    "strategy": segmentation.strategy,
                },
            }
            for index, segment in enumerate(segments, start=1)
        ],
        replace_existing=True,
    )

    set_workflow_state(
        "source_document",
        source_document_id,
        "segmented",
        current_stage="intake",
        event_type="segmentation_persisted",
        reason=f"Persisted {len(recorded_segments)} segment(s) before OCR",
        metadata={
            "source_document_id": source_document_id,
            "page_count": segmentation.page_count,
            "segment_count": len(recorded_segments),
            "used_segmentation": segmentation.used_segmentation,
            "segmentation_reason": segmentation.reason,
        },
    )

    return {
        "source_document": source_document,
        "segments": recorded_segments,
    }


def persist_source_document_segmentation_best_effort(
    source_document_id: Optional[str],
    segmentation: SegmentationResult,
) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    if not source_document_id:
        return None, None
    try:
        return persist_source_document_segmentation(source_document_id, segmentation), None
    except Exception as exc:
        return None, str(exc)


def update_source_document_segment_best_effort(
    segment_id: Optional[str],
    *,
    status: Optional[str] = None,
    invoice_id: Optional[str] = None,
    metadata: Optional[Dict[str, Any]] = None,
) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    if not segment_id:
        return None, None
    try:
        _, _, _, update_source_document_segment = _get_agent_db_ops()
        return (
            update_source_document_segment(
                segment_id,
                status=status,
                invoice_id=invoice_id,
                metadata=metadata,
            ),
            None,
        )
    except Exception as exc:
        return None, str(exc)


def finalize_source_document_extraction_from_segments(
    source_document_id: str,
) -> Optional[str]:
    from agent_db import get_source_document_detail, set_workflow_state, update_source_document

    detail = get_source_document_detail(source_document_id)
    if not detail:
        return None

    segments = detail.get("segments") or []
    if not segments:
        return None

    segment_statuses = [str(segment.get("status") or "").strip().lower() for segment in segments]
    pending_statuses = {"created", "ready", "queued", "extracting", "matching_queued"}
    if any(status in pending_statuses for status in segment_statuses):
        update_source_document(
            source_document_id,
            extraction_status="extracting",
            metadata={
                "extraction": {
                    "segment_statuses": segment_statuses,
                    "pending_segment_count": sum(1 for status in segment_statuses if status in pending_statuses),
                }
            },
        )
        return "extracting"

    success_statuses = {"validated", "persisted", "duplicate_blocked", "matched", "matched_auto"}
    failed_statuses = {"failed"}
    review_statuses = {"review_required", "needs_review", "matching_failed"}

    success_count = sum(1 for status in segment_statuses if status in success_statuses)
    failed_count = sum(1 for status in segment_statuses if status in failed_statuses)
    review_count = sum(1 for status in segment_statuses if status in review_statuses)

    if success_count == 0 and failed_count > 0 and review_count == 0:
        extraction_status = "failed"
        workflow_state = "extraction_failed"
        reason = "All source document segments failed before a validated extraction could be persisted."
    elif review_count > 0 or failed_count > 0:
        extraction_status = "review_required"
        workflow_state = "needs_review"
        reason = "One or more source document segments still require human review after automated extraction."
    else:
        extraction_status = "validated"
        workflow_state = "validated"
        reason = "All source document segments completed automated extraction successfully."

    update_source_document(
        source_document_id,
        extraction_status=extraction_status,
        metadata={
            "extraction": {
                "segment_statuses": segment_statuses,
                "successful_segments": success_count,
                "review_required_segments": review_count,
                "failed_segments": failed_count,
            }
        },
    )
    set_workflow_state(
        "source_document",
        source_document_id,
        workflow_state,
        current_stage="extraction",
        event_type="extraction_completed",
        reason=reason,
        metadata={
            "source_document_id": source_document_id,
            "successful_segments": success_count,
            "review_required_segments": review_count,
            "failed_segments": failed_count,
        },
    )
    return extraction_status


def finalize_source_document_extraction_from_segments_best_effort(
    source_document_id: Optional[str],
) -> Tuple[Optional[str], Optional[str]]:
    if not source_document_id:
        return None, None
    try:
        return finalize_source_document_extraction_from_segments(source_document_id), None
    except Exception as exc:
        return None, str(exc)
