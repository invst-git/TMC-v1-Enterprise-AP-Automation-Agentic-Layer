import os
import socket
import threading
import time
import uuid
from typing import Any, Callable, Dict, Optional

from invoice_detector import is_invoice_attachment
from workflow_task_queue import compute_retry_delay_seconds, enqueue_workflow_task

TaskHandler = Callable[[Dict[str, Any]], Dict[str, Any]]


class TaskPayloadError(ValueError):
    pass


def _get_agent_db_ops():
    from agent_db import (
        claim_next_task,
        complete_task,
        dead_letter_agent_task,
        fail_task,
        mark_task_running,
        record_agent_decision,
        update_source_document,
    )

    return {
        "claim_next_task": claim_next_task,
        "mark_task_running": mark_task_running,
        "complete_task": complete_task,
        "dead_letter_task": dead_letter_agent_task,
        "fail_task": fail_task,
        "record_agent_decision": record_agent_decision,
        "update_source_document": update_source_document,
    }


def _env_bool(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _env_float(name: str, default: float) -> float:
    value = os.getenv(name)
    if value is None:
        return default
    try:
        return float(value)
    except Exception:
        return default


def _env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None:
        return default
    try:
        return int(value)
    except Exception:
        return default


def build_worker_id() -> str:
    return f"{socket.gethostname()}:{os.getpid()}:{uuid.uuid4().hex[:8]}"


def _require_payload(task: Dict[str, Any], *required_fields: str) -> Dict[str, Any]:
    payload = task.get("payload") or {}
    missing_fields = []
    for field in required_fields:
        value = payload.get(field)
        if value is None:
            missing_fields.append(field)
            continue
        if isinstance(value, str) and not value.strip():
            missing_fields.append(field)
            continue
        if isinstance(value, (list, tuple, dict, set)) and not value:
            missing_fields.append(field)
    if missing_fields:
        raise TaskPayloadError(
            f"Task payload missing required field(s): {', '.join(sorted(missing_fields))}"
        )
    return payload


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


def _finalize_source_document_extraction_best_effort(source_document_id: Optional[str]) -> Optional[str]:
    if not source_document_id:
        return None

    from realtime_events import publish_live_update
    from source_document_tracking import finalize_source_document_extraction_from_segments_best_effort

    extraction_status, warning = finalize_source_document_extraction_from_segments_best_effort(source_document_id)
    if extraction_status in {"validated", "review_required", "failed"} and warning is None:
        publish_live_update(
            "source_document.extraction_finalized",
            {
                "sourceDocumentId": source_document_id,
                "extractionStatus": extraction_status,
            },
        )
    return extraction_status


def classify_source_document_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    payload = payload or {}
    metadata = payload.get("metadata") or {}
    email_metadata = metadata.get("email") or {}
    source_type = (payload.get("source_type") or "").strip().lower()
    original_filename = payload.get("original_filename") or ""
    content_type = payload.get("content_type") or ""
    subject = email_metadata.get("subject") or ""

    if source_type == "manual_upload":
        return {
            "decision": "invoice_candidate",
            "confidence": 0.99,
            "reasoning_summary": "Manual upload is treated as an explicit invoice candidate.",
            "inputs": {
                "source_type": source_type,
                "subject": subject,
                "original_filename": original_filename,
                "content_type": content_type,
            },
        }

    is_candidate = is_invoice_attachment(subject, original_filename, content_type)
    if is_candidate:
        return {
            "decision": "invoice_candidate",
            "confidence": 0.82,
            "reasoning_summary": "Filename/content-type matched deterministic invoice attachment heuristics.",
            "inputs": {
                "source_type": source_type or "unknown",
                "subject": subject,
                "original_filename": original_filename,
                "content_type": content_type,
            },
        }

    return {
        "decision": "non_invoice_candidate",
        "confidence": 0.74,
        "reasoning_summary": "Attachment did not match baseline invoice heuristics.",
        "inputs": {
            "source_type": source_type or "unknown",
            "subject": subject,
            "original_filename": original_filename,
            "content_type": content_type,
        },
    }


def handle_intake_classify_task(task: Dict[str, Any]) -> Dict[str, Any]:
    db_ops = _get_agent_db_ops()
    payload = task.get("payload") or {}
    decision = classify_source_document_payload(payload)
    source_document_id = task.get("source_document_id") or payload.get("source_document_id")

    if source_document_id:
        db_ops["update_source_document"](
            source_document_id,
            ingestion_status="classified",
            metadata={
                "classification": {
                    "task_id": task.get("id"),
                    "decision": decision["decision"],
                    "confidence": decision["confidence"],
                    "reasoning_summary": decision["reasoning_summary"],
                    "classified_at_stage": "worker",
                }
            },
        )

    db_ops["record_agent_decision"](
        task_id=task["id"],
        entity_type=task["entity_type"],
        entity_id=task["entity_id"],
        agent_name="baseline_intake_classifier",
        model_name="deterministic_rules_v1",
        prompt_version="deterministic_rules_v1",
        decision_type="intake_classification",
        decision=decision["decision"],
        confidence=decision["confidence"],
        reasoning_summary=decision["reasoning_summary"],
        metadata={
            "inputs": decision["inputs"],
            "source_document_id": source_document_id,
        },
    )

    return {
        "decision": decision["decision"],
        "confidence": decision["confidence"],
        "reasoning_summary": decision["reasoning_summary"],
        "source_document_id": source_document_id,
    }


def handle_extract_validate_task(task: Dict[str, Any]) -> Dict[str, Any]:
    payload = _require_payload(task, "invoice_path", "message_id")
    from extraction_validation import extract_and_validate_invoice
    from invoice_db import save_invoice_to_db
    from realtime_events import publish_live_update
    from source_document_tracking import update_source_document_segment_best_effort
    from user_facing_errors import DuplicateInvoiceBlockedError

    invoice_path = payload["invoice_path"]
    message_id = payload["message_id"]
    source_document_id = payload.get("source_document_id") or task.get("source_document_id")
    source_document_segment_id = payload.get("source_document_segment_id")
    from_email = payload.get("from_email")
    vendor_id_override = payload.get("vendor_id_override")

    validation = extract_and_validate_invoice(
        invoice_path,
        source_document_id=source_document_id,
        source_document_segment_id=source_document_segment_id,
        from_email=from_email,
        vendor_id_override=vendor_id_override,
    )
    result: Dict[str, Any] = {
        "invoice_path": invoice_path,
        "source_document_id": source_document_id,
        "source_document_segment_id": source_document_segment_id,
        "validation": validation.to_dict(),
    }

    if not validation.json_path:
        extraction_status = _finalize_source_document_extraction_best_effort(source_document_id)
        if extraction_status:
            result["source_document_extraction_status"] = extraction_status
        return result

    if getattr(validation, "blocks_persistence", False):
        duplicate_ids = [
            candidate["id"]
            for candidate in validation.duplicate_candidates
            if candidate.get("confidence", 0) >= 0.75
        ]
        update_source_document_segment_best_effort(
            source_document_segment_id,
            status="duplicate_blocked",
            metadata={
                "duplicate_detection": {
                    "decision": "blocked",
                    "duplicate_invoice_ids": duplicate_ids[:10],
                    "review_item_id": validation.review_item_id,
                }
            },
        )
        publish_live_update(
            "invoice.duplicate_blocked",
            {
                "sourceDocumentId": source_document_id,
                "sourceDocumentSegmentId": source_document_segment_id,
                "duplicateInvoiceIds": duplicate_ids[:10],
            },
        )
        extraction_status = _finalize_source_document_extraction_best_effort(source_document_id)
        result["decision"] = "duplicate_blocked"
        result["duplicate_invoice_ids"] = duplicate_ids[:10]
        if extraction_status:
            result["source_document_extraction_status"] = extraction_status
        return result

    try:
        save_result = save_invoice_to_db(
            validation.json_path,
            invoice_path,
            from_email,
            message_id,
            vendor_id_override,
        )
    except DuplicateInvoiceBlockedError as exc:
        update_source_document_segment_best_effort(
            source_document_segment_id,
            status="duplicate_blocked",
            metadata={
                "duplicate_detection": {
                    "decision": "blocked",
                    "review_item_id": validation.review_item_id,
                    "message": str(exc),
                }
            },
        )
        publish_live_update(
            "invoice.duplicate_blocked",
            {
                "sourceDocumentId": source_document_id,
                "sourceDocumentSegmentId": source_document_segment_id,
                "message": str(exc),
            },
        )
        extraction_status = _finalize_source_document_extraction_best_effort(source_document_id)
        result["decision"] = "duplicate_blocked"
        if extraction_status:
            result["source_document_extraction_status"] = extraction_status
        return result

    if not save_result:
        raise RuntimeError("Validated extraction could not be persisted.")

    invoice_id = save_result.invoice_id
    result["invoice_id"] = invoice_id
    result["invoice_status"] = save_result.status
    _mark_invoice_received_best_effort(invoice_id, source_document_id)
    update_source_document_segment_best_effort(
        source_document_segment_id,
        status=None if validation.requires_review else "persisted",
        invoice_id=invoice_id,
        metadata={
            "persistence": {
                "invoice_id": invoice_id,
                "decision": validation.decision,
                "invoice_status": save_result.status,
            }
        },
    )
    publish_live_update(
        "invoice.persisted",
        {
            "invoiceId": invoice_id,
            "sourceDocumentId": source_document_id,
        },
    )

    matching_task = enqueue_workflow_task(
        task_type="matching.evaluate",
        entity_type="invoice",
        entity_id=invoice_id,
        source_document_id=source_document_id,
        dedupe_key=f"invoice:{invoice_id}:matching.evaluate",
        payload={
            "invoice_id": invoice_id,
            "source_document_id": source_document_id,
            "source_document_segment_id": source_document_segment_id,
            "amount_tolerance": payload.get("amount_tolerance", 1.0),
            "percent_tolerance": payload.get("percent_tolerance", 0.02),
        },
    )
    result["matching_task_id"] = matching_task["id"]
    extraction_status = _finalize_source_document_extraction_best_effort(source_document_id)
    if extraction_status:
        result["source_document_extraction_status"] = extraction_status
    publish_live_update(
        "invoice.matching_queued",
        {
            "invoiceId": invoice_id,
            "matchingTaskId": matching_task["id"],
            "sourceDocumentId": source_document_id,
        },
    )
    return result


def handle_matching_evaluate_task(task: Dict[str, Any]) -> Dict[str, Any]:
    payload = _require_payload(task, "invoice_id")
    from realtime_events import publish_live_update
    from matching_orchestration import evaluate_invoice_match_for_worker

    outcome = evaluate_invoice_match_for_worker(
        task["id"],
        payload["invoice_id"],
        source_document_id=payload.get("source_document_id") or task.get("source_document_id"),
        amount_tolerance=float(payload.get("amount_tolerance") or 1.0),
        percent_tolerance=float(payload.get("percent_tolerance") or 0.02),
    )
    publish_live_update(
        "invoice.matching_updated",
        {
            "invoiceId": outcome.invoice_id,
            "matchedPoId": outcome.matched_po_id,
            "workflowState": outcome.workflow_state,
            "reviewItemId": outcome.review_item_id,
            "decision": outcome.decision,
        },
    )
    return outcome.to_dict()


def handle_matching_resolve_exception_task(task: Dict[str, Any]) -> Dict[str, Any]:
    payload = _require_payload(task, "invoice_id")
    from realtime_events import publish_live_update
    from matching_orchestration import resolve_match_exception_for_worker

    outcome = resolve_match_exception_for_worker(
        task["id"],
        payload["invoice_id"],
        source_document_id=payload.get("source_document_id") or task.get("source_document_id"),
        analysis_data=payload.get("analysis"),
        amount_tolerance=float(payload.get("amount_tolerance") or 1.0),
        percent_tolerance=float(payload.get("percent_tolerance") or 0.02),
    )
    publish_live_update(
        "invoice.matching_updated",
        {
            "invoiceId": outcome.invoice_id,
            "matchedPoId": outcome.matched_po_id,
            "workflowState": outcome.workflow_state,
            "reviewItemId": outcome.review_item_id,
            "decision": outcome.decision,
        },
    )
    return outcome.to_dict()


def handle_payment_authorize_task(task: Dict[str, Any]) -> Dict[str, Any]:
    payload = _require_payload(task, "invoice_ids")
    from payment_authorization import request_payment_authorization

    return request_payment_authorization(
        payload["invoice_ids"],
        payload.get("customer") or {},
        currency=payload.get("currency"),
        save_method=bool(payload.get("save_method")),
        requested_by=payload.get("requested_by"),
    )


def handle_payment_route_task(task: Dict[str, Any]) -> Dict[str, Any]:
    payload = _require_payload(task, "invoice_ids")
    from payment_authorization import evaluate_and_route_payment_batch

    return evaluate_and_route_payment_batch(
        payload["invoice_ids"],
        payload.get("customer") or {},
        currency=payload.get("currency"),
        save_method=bool(payload.get("save_method")),
        requested_by=payload.get("requested_by"),
    )


def handle_payment_execute_task(task: Dict[str, Any]) -> Dict[str, Any]:
    payload = _require_payload(task, "request_id")
    from payment_authorization import execute_payment_authorization

    return execute_payment_authorization(payload["request_id"])


def get_task_handlers() -> Dict[str, TaskHandler]:
    return {
        "intake.classify": handle_intake_classify_task,
        "extract.validate": handle_extract_validate_task,
        "matching.evaluate": handle_matching_evaluate_task,
        "matching.resolve_exception": handle_matching_resolve_exception_task,
        "payment.authorize": handle_payment_authorize_task,
        "payment.route": handle_payment_route_task,
        "payment.execute": handle_payment_execute_task,
    }


def process_one_task(
    *,
    worker_id: str,
    handlers: Optional[Dict[str, TaskHandler]] = None,
    lease_seconds: Optional[int] = None,
    retry_delay_seconds: Optional[int] = None,
) -> Optional[Dict[str, Any]]:
    handlers = handlers or get_task_handlers()
    if not handlers:
        return None

    db_ops = _get_agent_db_ops()
    lease_seconds = int(lease_seconds if lease_seconds is not None else _env_int("AGENT_WORKER_LEASE_SECONDS", 300))

    task = db_ops["claim_next_task"](
        worker_id,
        task_types=list(handlers.keys()),
        lease_seconds=lease_seconds,
    )
    if not task:
        return None

    running_task = db_ops["mark_task_running"](
        task["id"],
        worker_id,
        lease_seconds=lease_seconds,
    )
    if not running_task:
        return {
            "task_id": task["id"],
            "task_type": task["task_type"],
            "final_status": "lease_lost",
        }

    handler = handlers.get(running_task["task_type"])
    if handler is None:
        dead_lettered = db_ops["dead_letter_task"](
            running_task["id"],
            error_message=f"No worker handler registered for task type {running_task['task_type']}",
            error_details={"task_type": running_task["task_type"]},
        )
        return {
            "task_id": running_task["id"],
            "task_type": running_task["task_type"],
            "final_status": (dead_lettered or {}).get("status", "dead_letter"),
        }

    try:
        result = handler(running_task)
        completed = db_ops["complete_task"](
            running_task["id"],
            worker_id,
            result=result,
        )
        return {
            "task_id": running_task["id"],
            "task_type": running_task["task_type"],
            "final_status": (completed or {}).get("status", "completed"),
            "result": result,
        }
    except TaskPayloadError as exc:
        dead_lettered = db_ops["dead_letter_task"](
            running_task["id"],
            error_message=str(exc),
            error_details={
                "task_type": running_task["task_type"],
                "entity_type": running_task["entity_type"],
                "entity_id": running_task["entity_id"],
                "payload": running_task.get("payload") or {},
            },
        )
        return {
            "task_id": running_task["id"],
            "task_type": running_task["task_type"],
            "final_status": (dead_lettered or {}).get("status", "dead_letter"),
            "error": str(exc),
        }
    except Exception as exc:
        effective_retry_delay_seconds = (
            int(retry_delay_seconds)
            if retry_delay_seconds is not None
            else compute_retry_delay_seconds(running_task["task_type"], running_task.get("attempt_count") or 1)
        )
        failed = db_ops["fail_task"](
            running_task["id"],
            worker_id,
            error_message=str(exc),
            error_details={
                "task_type": running_task["task_type"],
                "entity_type": running_task["entity_type"],
                "entity_id": running_task["entity_id"],
            },
            retry_delay_seconds=effective_retry_delay_seconds,
        )
        return {
            "task_id": running_task["id"],
            "task_type": running_task["task_type"],
            "final_status": (failed or {}).get("status", "failed"),
            "error": str(exc),
            "retry_delay_seconds": effective_retry_delay_seconds,
        }


def run_worker_loop(
    *,
    stop_event: Optional[threading.Event] = None,
    worker_id: Optional[str] = None,
    handlers: Optional[Dict[str, TaskHandler]] = None,
    poll_interval_seconds: Optional[float] = None,
    lease_seconds: Optional[int] = None,
    retry_delay_seconds: Optional[int] = None,
    max_tasks: Optional[int] = None,
) -> int:
    worker_id = worker_id or build_worker_id()
    handlers = handlers or get_task_handlers()
    poll_interval_seconds = float(
        poll_interval_seconds if poll_interval_seconds is not None else _env_float("AGENT_WORKER_POLL_INTERVAL_SECONDS", 2.0)
    )

    processed_count = 0
    while True:
        if stop_event and stop_event.is_set():
            break
        outcome = process_one_task(
            worker_id=worker_id,
            handlers=handlers,
            lease_seconds=lease_seconds,
            retry_delay_seconds=retry_delay_seconds,
        )
        if outcome is None:
            if max_tasks is not None and processed_count >= max_tasks:
                break
            time.sleep(poll_interval_seconds)
            continue

        processed_count += 1
        if max_tasks is not None and processed_count >= max_tasks:
            break

    return processed_count


def start_agent_worker_thread() -> Optional[threading.Thread]:
    if not _env_bool("ENABLE_AGENT_WORKER", False):
        return None

    thread = threading.Thread(
        target=run_worker_loop,
        kwargs={"worker_id": build_worker_id()},
        daemon=True,
        name="agent-worker",
    )
    thread.start()
    return thread
