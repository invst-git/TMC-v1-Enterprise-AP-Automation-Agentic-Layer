import os
import socket
import threading
import time
import uuid
from typing import Any, Callable, Dict, Optional

from invoice_detector import is_invoice_attachment

TaskHandler = Callable[[Dict[str, Any]], Dict[str, Any]]


def _get_agent_db_ops():
    from agent_db import (
        claim_next_task,
        complete_task,
        fail_task,
        mark_task_running,
        record_agent_decision,
        update_source_document,
    )

    return {
        "claim_next_task": claim_next_task,
        "mark_task_running": mark_task_running,
        "complete_task": complete_task,
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


def get_task_handlers() -> Dict[str, TaskHandler]:
    return {
        "intake.classify": handle_intake_classify_task,
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
    retry_delay_seconds = int(
        retry_delay_seconds if retry_delay_seconds is not None else _env_int("AGENT_WORKER_RETRY_DELAY_SECONDS", 60)
    )

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
        db_ops["fail_task"](
            running_task["id"],
            worker_id,
            error_message=f"No worker handler registered for task type {running_task['task_type']}",
            error_details={"task_type": running_task["task_type"]},
            retry_delay_seconds=0,
        )
        return {
            "task_id": running_task["id"],
            "task_type": running_task["task_type"],
            "final_status": "failed",
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
    except Exception as exc:
        failed = db_ops["fail_task"](
            running_task["id"],
            worker_id,
            error_message=str(exc),
            error_details={
                "task_type": running_task["task_type"],
                "entity_type": running_task["entity_type"],
                "entity_id": running_task["entity_id"],
            },
            retry_delay_seconds=retry_delay_seconds,
        )
        return {
            "task_id": running_task["id"],
            "task_type": running_task["task_type"],
            "final_status": (failed or {}).get("status", "failed"),
            "error": str(exc),
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
