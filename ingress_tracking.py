import hashlib
import mimetypes
import os
from typing import Any, Dict, Optional, Tuple


def _get_agent_db_ops():
    from agent_db import enqueue_agent_task, record_source_document, set_workflow_state

    return record_source_document, set_workflow_state, enqueue_agent_task


def compute_file_hash(content_bytes: Optional[bytes]) -> Optional[str]:
    if content_bytes is None:
        return None
    return hashlib.sha256(content_bytes).hexdigest()


def _resolve_content_type(storage_path: str, original_filename: Optional[str], content_type: Optional[str]) -> str:
    guessed_type, _ = mimetypes.guess_type(original_filename or storage_path)
    return content_type or guessed_type or "application/octet-stream"


def _resolve_file_size_bytes(storage_path: str, content_bytes: Optional[bytes]) -> Optional[int]:
    if content_bytes is not None:
        return len(content_bytes)
    try:
        return os.path.getsize(storage_path)
    except OSError:
        return None


def _merge_metadata(base: Dict[str, Any], extra: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    merged = dict(base)
    if not extra:
        return merged
    for key, value in extra.items():
        if isinstance(value, dict) and isinstance(merged.get(key), dict):
            merged[key] = {**merged[key], **value}
        else:
            merged[key] = value
    return merged


def register_ingress_source_document(
    *,
    source_type: str,
    storage_path: str,
    source_ref: Optional[str] = None,
    original_filename: Optional[str] = None,
    content_type: Optional[str] = None,
    content_bytes: Optional[bytes] = None,
    from_email: Optional[str] = None,
    email_message_id: Optional[str] = None,
    vendor_id: Optional[str] = None,
    metadata: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    record_source_document, set_workflow_state, enqueue_agent_task = _get_agent_db_ops()
    resolved_content_type = _resolve_content_type(storage_path, original_filename, content_type)
    resolved_metadata = _merge_metadata(
        {
            "ingress": {
                "source_type": source_type,
                "source_ref": source_ref,
            }
        },
        metadata,
    )

    source_document = record_source_document(
        source_type=source_type,
        source_ref=source_ref,
        original_filename=original_filename,
        storage_path=storage_path,
        content_type=resolved_content_type,
        file_size_bytes=_resolve_file_size_bytes(storage_path, content_bytes),
        file_hash=compute_file_hash(content_bytes),
        from_email=from_email,
        email_message_id=email_message_id,
        vendor_id=vendor_id,
        ingestion_status="received",
        segmentation_status="not_started",
        extraction_status="not_started",
        metadata=resolved_metadata,
    )

    workflow_metadata = {
        "source_document_id": source_document["id"],
        "source_type": source_type,
        "storage_path": storage_path,
        "original_filename": original_filename,
        "content_type": resolved_content_type,
    }
    if from_email:
        workflow_metadata["from_email"] = from_email
    if vendor_id:
        workflow_metadata["vendor_id"] = vendor_id

    set_workflow_state(
        "source_document",
        source_document["id"],
        "received",
        current_stage="ingress",
        event_type="registered",
        reason=f"Registered {source_type} source document at ingress",
        metadata=workflow_metadata,
    )

    enqueue_agent_task(
        task_type="intake.classify",
        entity_type="source_document",
        entity_id=source_document["id"],
        source_document_id=source_document["id"],
        priority=50,
        dedupe_key=f"source_document:{source_document['id']}:intake.classify",
        payload={
            "source_document_id": source_document["id"],
            "source_type": source_type,
            "storage_path": storage_path,
            "original_filename": original_filename,
            "content_type": resolved_content_type,
            "from_email": from_email,
            "email_message_id": email_message_id,
            "vendor_id": vendor_id,
            "metadata": resolved_metadata,
        },
    )

    return source_document


def register_ingress_source_document_best_effort(**kwargs: Any) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    try:
        return register_ingress_source_document(**kwargs), None
    except Exception as exc:
        return None, str(exc)
