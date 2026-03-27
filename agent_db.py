import datetime
import json
from typing import Any, Dict, Iterable, List, Optional, Sequence

from psycopg.types.json import Jsonb

from db import get_conn

_AGENT_TABLES = (
    "source_documents",
    "source_document_segments",
    "workflow_states",
    "workflow_state_history",
    "agent_tasks",
    "agent_decisions",
    "human_review_queue",
    "vendor_communications",
    "sla_configs",
)

_AGENT_MIGRATION = "migrations/2026-03-27_add_agentic_workflow_backbone.sql"


def _assert_agent_tables(cur) -> None:
    cur.execute(
        """
        SELECT
          to_regclass('public.source_documents'),
          to_regclass('public.source_document_segments'),
          to_regclass('public.workflow_states'),
          to_regclass('public.workflow_state_history'),
          to_regclass('public.agent_tasks'),
          to_regclass('public.agent_decisions'),
          to_regclass('public.human_review_queue'),
          to_regclass('public.vendor_communications'),
          to_regclass('public.sla_configs')
        """
    )
    row = cur.fetchone()
    if not row:
        missing = list(_AGENT_TABLES)
        raise RuntimeError(
            "Agent workflow tables not found. "
            f"Run migration: {_AGENT_MIGRATION}. Missing: {', '.join(missing)}"
        )
    missing = [name for name, value in zip(_AGENT_TABLES, row or ()) if value is None]
    if missing:
        raise RuntimeError(
            "Agent workflow tables not found. "
            f"Run migration: {_AGENT_MIGRATION}. Missing: {', '.join(missing)}"
        )


def _jsonb(value: Any, default: Any) -> Jsonb:
    return Jsonb(default if value is None else value)


def _json_value(value: Any, default: Any) -> Any:
    if value is None:
        return default
    if isinstance(value, str):
        try:
            return json.loads(value)
        except Exception:
            return default
    return value


def _iso(value: Any) -> Optional[str]:
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return None


def _source_document_row(row: Any) -> Dict[str, Any]:
    return {
        "id": str(row[0]),
        "source_type": row[1],
        "source_ref": row[2],
        "original_filename": row[3],
        "storage_provider": row[4],
        "storage_path": row[5],
        "content_type": row[6],
        "file_size_bytes": row[7],
        "file_hash": row[8],
        "page_count": row[9],
        "from_email": row[10],
        "email_message_id": row[11],
        "vendor_id": str(row[12]) if row[12] else None,
        "ingestion_status": row[13],
        "segmentation_status": row[14],
        "extraction_status": row[15],
        "metadata": _json_value(row[16], {}),
        "received_at": _iso(row[17]),
        "created_at": _iso(row[18]),
        "updated_at": _iso(row[19]),
    }


def _segment_row(row: Any) -> Dict[str, Any]:
    return {
        "id": str(row[0]),
        "source_document_id": str(row[1]),
        "segment_index": row[2],
        "page_from": row[3],
        "page_to": row[4],
        "segment_path": row[5],
        "confidence": float(row[6]) if row[6] is not None else None,
        "status": row[7],
        "invoice_id": str(row[8]) if row[8] else None,
        "metadata": _json_value(row[9], {}),
        "created_at": _iso(row[10]),
        "updated_at": _iso(row[11]),
    }


def _workflow_state_row(row: Any, previous_state: Optional[str] = None) -> Dict[str, Any]:
    return {
        "id": str(row[0]),
        "entity_type": row[1],
        "entity_id": str(row[2]),
        "current_state": row[3],
        "current_stage": row[4],
        "confidence": float(row[5]) if row[5] is not None else None,
        "metadata": _json_value(row[6], {}),
        "created_at": _iso(row[7]),
        "updated_at": _iso(row[8]),
        "previous_state": previous_state,
    }


def _workflow_history_row(row: Any) -> Dict[str, Any]:
    return {
        "id": str(row[0]),
        "entity_type": row[1],
        "entity_id": str(row[2]),
        "from_state": row[3],
        "to_state": row[4],
        "event_type": row[5],
        "reason": row[6],
        "actor_type": row[7],
        "actor_id": row[8],
        "metadata": _json_value(row[9], {}),
        "created_at": _iso(row[10]),
    }


def _task_row(row: Any, *, deduped: bool = False) -> Dict[str, Any]:
    return {
        "id": str(row[0]),
        "task_type": row[1],
        "entity_type": row[2],
        "entity_id": str(row[3]),
        "source_document_id": str(row[4]) if row[4] else None,
        "priority": row[5],
        "status": row[6],
        "attempt_count": row[7],
        "max_attempts": row[8],
        "dedupe_key": row[9],
        "available_at": _iso(row[10]),
        "lease_expires_at": _iso(row[11]),
        "locked_by": row[12],
        "locked_at": _iso(row[13]),
        "heartbeat_at": _iso(row[14]),
        "started_at": _iso(row[15]),
        "completed_at": _iso(row[16]),
        "last_error": row[17],
        "payload": _json_value(row[18], {}),
        "result": _json_value(row[19], None),
        "created_at": _iso(row[20]),
        "updated_at": _iso(row[21]),
        "retry_count": max(int(row[7] or 0) - 1, 0),
        "deduped": deduped,
    }


def _decision_row(row: Any) -> Dict[str, Any]:
    return {
        "id": str(row[0]),
        "task_id": str(row[1]),
        "entity_type": row[2],
        "entity_id": str(row[3]),
        "agent_name": row[4],
        "model_name": row[5],
        "prompt_version": row[6],
        "decision_type": row[7],
        "decision": row[8],
        "confidence": float(row[9]) if row[9] is not None else None,
        "reasoning_summary": row[10],
        "tool_calls": _json_value(row[11], []),
        "metadata": _json_value(row[12], {}),
        "created_at": _iso(row[13]),
    }


def _human_review_row(row: Any) -> Dict[str, Any]:
    return {
        "id": str(row[0]),
        "entity_type": row[1],
        "entity_id": str(row[2]),
        "source_document_id": str(row[3]) if row[3] else None,
        "invoice_id": str(row[4]) if row[4] else None,
        "queue_name": row[5],
        "priority": row[6],
        "status": row[7],
        "review_reason": row[8],
        "assigned_to": row[9],
        "due_at": _iso(row[10]),
        "resolution": row[11],
        "metadata": _json_value(row[12], {}),
        "created_at": _iso(row[13]),
        "updated_at": _iso(row[14]),
        "resolved_at": _iso(row[15]),
    }


def _vendor_communication_row(row: Any) -> Dict[str, Any]:
    return {
        "id": str(row[0]),
        "vendor_id": str(row[1]) if row[1] else None,
        "invoice_id": str(row[2]) if row[2] else None,
        "source_document_id": str(row[3]) if row[3] else None,
        "direction": row[4],
        "channel": row[5],
        "status": row[6],
        "recipient": row[7],
        "subject": row[8],
        "body": row[9],
        "approved_by": row[10],
        "sent_at": _iso(row[11]),
        "metadata": _json_value(row[12], {}),
        "created_at": _iso(row[13]),
        "updated_at": _iso(row[14]),
    }


def _sla_config_row(row: Any) -> Dict[str, Any]:
    return {
        "id": str(row[0]),
        "entity_type": row[1],
        "state_name": row[2],
        "target_minutes": row[3],
        "escalation_queue": row[4],
        "is_active": bool(row[5]),
        "metadata": _json_value(row[6], {}),
        "created_at": _iso(row[7]),
        "updated_at": _iso(row[8]),
    }


def _sla_breach_row(row: Any) -> Dict[str, Any]:
    return {
        "entity_type": row[0],
        "entity_id": str(row[1]),
        "current_state": row[2],
        "current_stage": row[3],
        "confidence": float(row[4]) if row[4] is not None else None,
        "workflow_metadata": _json_value(row[5], {}),
        "state_updated_at": _iso(row[6]),
        "target_minutes": int(row[7]),
        "escalation_queue": row[8],
        "sla_metadata": _json_value(row[9], {}),
        "age_minutes": float(row[10]),
        "breach_minutes": float(row[11]),
    }


def _coerce_limit(limit: int, *, default: int = 100, max_value: int = 500) -> int:
    try:
        limit_value = int(limit)
    except Exception:
        limit_value = default
    return max(1, min(limit_value, max_value))


def record_source_document(
    *,
    source_type: str,
    storage_path: str,
    source_ref: Optional[str] = None,
    original_filename: Optional[str] = None,
    storage_provider: str = "local",
    content_type: Optional[str] = None,
    file_size_bytes: Optional[int] = None,
    file_hash: Optional[str] = None,
    page_count: Optional[int] = None,
    from_email: Optional[str] = None,
    email_message_id: Optional[str] = None,
    vendor_id: Optional[str] = None,
    ingestion_status: str = "received",
    segmentation_status: str = "not_started",
    extraction_status: str = "not_started",
    metadata: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    with get_conn() as conn:
        with conn.cursor() as cur:
            _assert_agent_tables(cur)
            cur.execute(
                """
                INSERT INTO source_documents(
                  source_type, source_ref, original_filename, storage_provider, storage_path,
                  content_type, file_size_bytes, file_hash, page_count, from_email,
                  email_message_id, vendor_id, ingestion_status, segmentation_status,
                  extraction_status, metadata
                )
                VALUES(
                  %s, %s, %s, %s, %s,
                  %s, %s, %s, %s, %s,
                  %s, %s, %s, %s,
                  %s, %s
                )
                ON CONFLICT (storage_path)
                DO UPDATE SET
                  source_type = EXCLUDED.source_type,
                  source_ref = COALESCE(EXCLUDED.source_ref, source_documents.source_ref),
                  original_filename = COALESCE(EXCLUDED.original_filename, source_documents.original_filename),
                  storage_provider = EXCLUDED.storage_provider,
                  content_type = COALESCE(EXCLUDED.content_type, source_documents.content_type),
                  file_size_bytes = COALESCE(EXCLUDED.file_size_bytes, source_documents.file_size_bytes),
                  file_hash = COALESCE(EXCLUDED.file_hash, source_documents.file_hash),
                  page_count = COALESCE(EXCLUDED.page_count, source_documents.page_count),
                  from_email = COALESCE(EXCLUDED.from_email, source_documents.from_email),
                  email_message_id = COALESCE(EXCLUDED.email_message_id, source_documents.email_message_id),
                  vendor_id = COALESCE(EXCLUDED.vendor_id, source_documents.vendor_id),
                  ingestion_status = EXCLUDED.ingestion_status,
                  segmentation_status = EXCLUDED.segmentation_status,
                  extraction_status = EXCLUDED.extraction_status,
                  metadata = COALESCE(source_documents.metadata, '{}'::jsonb) || EXCLUDED.metadata,
                  updated_at = now()
                RETURNING
                  id, source_type, source_ref, original_filename, storage_provider, storage_path,
                  content_type, file_size_bytes, file_hash, page_count, from_email,
                  email_message_id, vendor_id, ingestion_status, segmentation_status,
                  extraction_status, metadata, received_at, created_at, updated_at
                """,
                (
                    source_type,
                    source_ref,
                    original_filename,
                    storage_provider,
                    storage_path,
                    content_type,
                    file_size_bytes,
                    file_hash,
                    page_count,
                    from_email,
                    email_message_id,
                    vendor_id,
                    ingestion_status,
                    segmentation_status,
                    extraction_status,
                    _jsonb(metadata, {}),
                ),
            )
            return _source_document_row(cur.fetchone())


def update_source_document(
    source_document_id: str,
    *,
    page_count: Optional[int] = None,
    ingestion_status: Optional[str] = None,
    segmentation_status: Optional[str] = None,
    extraction_status: Optional[str] = None,
    metadata: Optional[Dict[str, Any]] = None,
) -> Optional[Dict[str, Any]]:
    with get_conn() as conn:
        with conn.cursor() as cur:
            _assert_agent_tables(cur)
            cur.execute(
                """
                UPDATE source_documents
                SET
                  page_count = COALESCE(%s, page_count),
                  ingestion_status = COALESCE(%s, ingestion_status),
                  segmentation_status = COALESCE(%s, segmentation_status),
                  extraction_status = COALESCE(%s, extraction_status),
                  metadata = CASE
                    WHEN %s THEN metadata
                    ELSE COALESCE(metadata, '{}'::jsonb) || %s
                  END,
                  updated_at = now()
                WHERE id = %s
                RETURNING
                  id, source_type, source_ref, original_filename, storage_provider, storage_path,
                  content_type, file_size_bytes, file_hash, page_count, from_email,
                  email_message_id, vendor_id, ingestion_status, segmentation_status,
                  extraction_status, metadata, received_at, created_at, updated_at
                """,
                (
                    page_count,
                    ingestion_status,
                    segmentation_status,
                    extraction_status,
                    metadata is None,
                    _jsonb(metadata, {}),
                    source_document_id,
                ),
            )
            row = cur.fetchone()
            return _source_document_row(row) if row else None


def record_source_document_segments(
    source_document_id: str,
    segments: Sequence[Dict[str, Any]],
    *,
    replace_existing: bool = True,
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    with get_conn() as conn:
        with conn.cursor() as cur:
            _assert_agent_tables(cur)
            if replace_existing:
                cur.execute(
                    "DELETE FROM source_document_segments WHERE source_document_id = %s",
                    (source_document_id,),
                )
            for index, segment in enumerate(segments, start=1):
                cur.execute(
                    """
                    INSERT INTO source_document_segments(
                      source_document_id, segment_index, page_from, page_to,
                      segment_path, confidence, status, invoice_id, metadata
                    )
                    VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING
                      id, source_document_id, segment_index, page_from, page_to,
                      segment_path, confidence, status, invoice_id, metadata,
                      created_at, updated_at
                    """,
                    (
                        source_document_id,
                        int(segment.get("segment_index") or index),
                        int(segment["page_from"]),
                        int(segment["page_to"]),
                        segment.get("segment_path"),
                        segment.get("confidence"),
                        segment.get("status") or "created",
                        segment.get("invoice_id"),
                        _jsonb(segment.get("metadata"), {}),
                    ),
                )
                rows.append(_segment_row(cur.fetchone()))
    return rows


def update_source_document_segment(
    segment_id: str,
    *,
    status: Optional[str] = None,
    invoice_id: Optional[str] = None,
    metadata: Optional[Dict[str, Any]] = None,
) -> Optional[Dict[str, Any]]:
    with get_conn() as conn:
        with conn.cursor() as cur:
            _assert_agent_tables(cur)
            cur.execute(
                """
                UPDATE source_document_segments
                SET
                  status = COALESCE(%s, status),
                  invoice_id = COALESCE(%s, invoice_id),
                  metadata = CASE
                    WHEN %s THEN metadata
                    ELSE COALESCE(metadata, '{}'::jsonb) || %s
                  END,
                  updated_at = now()
                WHERE id = %s
                RETURNING
                  id, source_document_id, segment_index, page_from, page_to,
                  segment_path, confidence, status, invoice_id, metadata,
                  created_at, updated_at
                """,
                (
                    status,
                    invoice_id,
                    metadata is None,
                    _jsonb(metadata, {}),
                    segment_id,
                ),
            )
            row = cur.fetchone()
            return _segment_row(row) if row else None


def set_workflow_state(
    entity_type: str,
    entity_id: str,
    current_state: str,
    *,
    current_stage: Optional[str] = None,
    confidence: Optional[float] = None,
    event_type: str = "transition",
    reason: Optional[str] = None,
    actor_type: str = "system",
    actor_id: Optional[str] = None,
    metadata: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    previous_state = None
    with get_conn() as conn:
        with conn.cursor() as cur:
            _assert_agent_tables(cur)
            cur.execute(
                """
                SELECT current_state
                FROM workflow_states
                WHERE entity_type = %s AND entity_id = %s
                """,
                (entity_type, entity_id),
            )
            existing = cur.fetchone()
            if existing:
                previous_state = existing[0]
            cur.execute(
                """
                INSERT INTO workflow_states(
                  entity_type, entity_id, current_state, current_stage, confidence, metadata
                )
                VALUES(%s, %s, %s, %s, %s, %s)
                ON CONFLICT (entity_type, entity_id)
                DO UPDATE SET
                  current_state = EXCLUDED.current_state,
                  current_stage = EXCLUDED.current_stage,
                  confidence = EXCLUDED.confidence,
                  metadata = COALESCE(workflow_states.metadata, '{}'::jsonb) || EXCLUDED.metadata,
                  updated_at = now()
                RETURNING
                  id, entity_type, entity_id, current_state, current_stage,
                  confidence, metadata, created_at, updated_at
                """,
                (
                    entity_type,
                    entity_id,
                    current_state,
                    current_stage,
                    confidence,
                    _jsonb(metadata, {}),
                ),
            )
            row = cur.fetchone()
            cur.execute(
                """
                INSERT INTO workflow_state_history(
                  entity_type, entity_id, from_state, to_state, event_type,
                  reason, actor_type, actor_id, metadata
                )
                VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    entity_type,
                    entity_id,
                    previous_state,
                    current_state,
                    event_type,
                    reason,
                    actor_type,
                    actor_id,
                    _jsonb(metadata, {}),
                ),
            )
            return _workflow_state_row(row, previous_state=previous_state)


def enqueue_agent_task(
    *,
    task_type: str,
    entity_type: str,
    entity_id: str,
    source_document_id: Optional[str] = None,
    priority: int = 100,
    max_attempts: int = 5,
    dedupe_key: Optional[str] = None,
    available_at: Optional[datetime.datetime] = None,
    payload: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    with get_conn() as conn:
        with conn.cursor() as cur:
            _assert_agent_tables(cur)
            if dedupe_key:
                cur.execute(
                    """
                    SELECT
                      id, task_type, entity_type, entity_id, source_document_id,
                      priority, status, attempt_count, max_attempts, dedupe_key,
                      available_at, lease_expires_at, locked_by, locked_at,
                      heartbeat_at, started_at, completed_at, last_error,
                      payload, result, created_at, updated_at
                    FROM agent_tasks
                    WHERE dedupe_key = %s AND status IN ('queued', 'leased', 'running')
                    ORDER BY created_at DESC
                    LIMIT 1
                    """,
                    (dedupe_key,),
                )
                existing = cur.fetchone()
                if existing:
                    return _task_row(existing, deduped=True)
            cur.execute(
                """
                INSERT INTO agent_tasks(
                  task_type, entity_type, entity_id, source_document_id, priority,
                  max_attempts, dedupe_key, available_at, payload
                )
                VALUES(%s, %s, %s, %s, %s, %s, %s, COALESCE(%s, now()), %s)
                RETURNING
                  id, task_type, entity_type, entity_id, source_document_id,
                  priority, status, attempt_count, max_attempts, dedupe_key,
                  available_at, lease_expires_at, locked_by, locked_at,
                  heartbeat_at, started_at, completed_at, last_error,
                  payload, result, created_at, updated_at
                """,
                (
                    task_type,
                    entity_type,
                    entity_id,
                    source_document_id,
                    priority,
                    max_attempts,
                    dedupe_key,
                    available_at,
                    _jsonb(payload, {}),
                ),
            )
            return _task_row(cur.fetchone())


def create_completed_agent_task(
    *,
    task_type: str,
    entity_type: str,
    entity_id: str,
    source_document_id: Optional[str] = None,
    priority: int = 100,
    payload: Optional[Dict[str, Any]] = None,
    result: Optional[Any] = None,
) -> Dict[str, Any]:
    with get_conn() as conn:
        with conn.cursor() as cur:
            _assert_agent_tables(cur)
            cur.execute(
                """
                INSERT INTO agent_tasks(
                  task_type, entity_type, entity_id, source_document_id, priority,
                  status, attempt_count, started_at, completed_at, payload, result
                )
                VALUES(%s, %s, %s, %s, %s, 'completed', 1, now(), now(), %s, %s)
                RETURNING
                  id, task_type, entity_type, entity_id, source_document_id,
                  priority, status, attempt_count, max_attempts, dedupe_key,
                  available_at, lease_expires_at, locked_by, locked_at,
                  heartbeat_at, started_at, completed_at, last_error,
                  payload, result, created_at, updated_at
                """,
                (
                    task_type,
                    entity_type,
                    entity_id,
                    source_document_id,
                    priority,
                    _jsonb(payload, {}),
                    _jsonb(result, None),
                ),
            )
            return _task_row(cur.fetchone())


def claim_next_task(
    worker_id: str,
    *,
    task_types: Optional[Iterable[str]] = None,
    lease_seconds: int = 300,
) -> Optional[Dict[str, Any]]:
    task_type_values = list(task_types or [])
    task_type_sql = ""
    params: List[Any] = []
    if task_type_values:
        task_type_sql = "AND task_type = ANY(%s)"
        params.append(task_type_values)
    params.extend([worker_id, int(lease_seconds)])
    with get_conn() as conn:
        with conn.cursor() as cur:
            _assert_agent_tables(cur)
            cur.execute(
                f"""
                WITH next_task AS (
                  SELECT id
                  FROM agent_tasks
                  WHERE
                    attempt_count < max_attempts
                    {task_type_sql}
                    AND (
                      (status = 'queued' AND available_at <= now())
                      OR (status IN ('leased', 'running') AND lease_expires_at IS NOT NULL AND lease_expires_at <= now())
                    )
                  ORDER BY priority ASC, available_at ASC, created_at ASC
                  FOR UPDATE SKIP LOCKED
                  LIMIT 1
                )
                UPDATE agent_tasks AS t
                SET
                  status = 'leased',
                  locked_by = %s,
                  locked_at = now(),
                  heartbeat_at = now(),
                  lease_expires_at = now() + make_interval(secs => %s),
                  started_at = COALESCE(t.started_at, now()),
                  attempt_count = t.attempt_count + 1,
                  updated_at = now()
                FROM next_task
                WHERE t.id = next_task.id
                RETURNING
                  t.id, t.task_type, t.entity_type, t.entity_id, t.source_document_id,
                  t.priority, t.status, t.attempt_count, t.max_attempts, t.dedupe_key,
                  t.available_at, t.lease_expires_at, t.locked_by, t.locked_at,
                  t.heartbeat_at, t.started_at, t.completed_at, t.last_error,
                  t.payload, t.result, t.created_at, t.updated_at
                """,
                tuple(params),
            )
            row = cur.fetchone()
            return _task_row(row) if row else None


def mark_task_running(task_id: str, worker_id: str, *, lease_seconds: int = 300) -> Optional[Dict[str, Any]]:
    with get_conn() as conn:
        with conn.cursor() as cur:
            _assert_agent_tables(cur)
            cur.execute(
                """
                UPDATE agent_tasks
                SET
                  status = 'running',
                  heartbeat_at = now(),
                  lease_expires_at = now() + make_interval(secs => %s),
                  started_at = COALESCE(started_at, now()),
                  updated_at = now()
                WHERE id = %s AND locked_by = %s AND status IN ('leased', 'running')
                RETURNING
                  id, task_type, entity_type, entity_id, source_document_id,
                  priority, status, attempt_count, max_attempts, dedupe_key,
                  available_at, lease_expires_at, locked_by, locked_at,
                  heartbeat_at, started_at, completed_at, last_error,
                  payload, result, created_at, updated_at
                """,
                (int(lease_seconds), task_id, worker_id),
            )
            row = cur.fetchone()
            return _task_row(row) if row else None


def heartbeat_task(task_id: str, worker_id: str, *, lease_seconds: int = 300) -> bool:
    with get_conn() as conn:
        with conn.cursor() as cur:
            _assert_agent_tables(cur)
            cur.execute(
                """
                UPDATE agent_tasks
                SET
                  heartbeat_at = now(),
                  lease_expires_at = now() + make_interval(secs => %s),
                  updated_at = now()
                WHERE id = %s AND locked_by = %s AND status IN ('leased', 'running')
                """,
                (int(lease_seconds), task_id, worker_id),
            )
            return bool(cur.rowcount)


def complete_task(task_id: str, worker_id: str, *, result: Optional[Any] = None) -> Optional[Dict[str, Any]]:
    with get_conn() as conn:
        with conn.cursor() as cur:
            _assert_agent_tables(cur)
            cur.execute(
                """
                UPDATE agent_tasks
                SET
                  status = 'completed',
                  result = %s,
                  completed_at = now(),
                  lease_expires_at = NULL,
                  locked_by = NULL,
                  locked_at = NULL,
                  heartbeat_at = NULL,
                  updated_at = now()
                WHERE id = %s AND locked_by = %s AND status IN ('leased', 'running')
                RETURNING
                  id, task_type, entity_type, entity_id, source_document_id,
                  priority, status, attempt_count, max_attempts, dedupe_key,
                  available_at, lease_expires_at, locked_by, locked_at,
                  heartbeat_at, started_at, completed_at, last_error,
                  payload, result, created_at, updated_at
                """,
                (_jsonb(result, None), task_id, worker_id),
            )
            row = cur.fetchone()
            return _task_row(row) if row else None


def fail_task(
    task_id: str,
    worker_id: str,
    *,
    error_message: str,
    error_details: Optional[Dict[str, Any]] = None,
    retry_delay_seconds: int = 0,
) -> Optional[Dict[str, Any]]:
    retry_delay_seconds = max(0, int(retry_delay_seconds))
    with get_conn() as conn:
        with conn.cursor() as cur:
            _assert_agent_tables(cur)
            cur.execute(
                """
                UPDATE agent_tasks
                SET
                  status = CASE WHEN attempt_count >= max_attempts THEN 'failed' ELSE 'queued' END,
                  available_at = CASE
                    WHEN attempt_count >= max_attempts THEN available_at
                    ELSE now() + make_interval(secs => %s)
                  END,
                  lease_expires_at = NULL,
                  locked_by = NULL,
                  locked_at = NULL,
                  heartbeat_at = NULL,
                  last_error = %s,
                  result = %s,
                  updated_at = now()
                WHERE id = %s AND locked_by = %s AND status IN ('leased', 'running')
                RETURNING
                  id, task_type, entity_type, entity_id, source_document_id,
                  priority, status, attempt_count, max_attempts, dedupe_key,
                  available_at, lease_expires_at, locked_by, locked_at,
                  heartbeat_at, started_at, completed_at, last_error,
                  payload, result, created_at, updated_at
                """,
                (
                    retry_delay_seconds,
                    error_message,
                    _jsonb(error_details, {}),
                    task_id,
                    worker_id,
                ),
            )
            row = cur.fetchone()
            return _task_row(row) if row else None


def record_agent_decision(
    *,
    task_id: str,
    entity_type: str,
    entity_id: str,
    agent_name: str,
    decision_type: str,
    decision: str,
    model_name: Optional[str] = None,
    prompt_version: Optional[str] = None,
    confidence: Optional[float] = None,
    reasoning_summary: Optional[str] = None,
    tool_calls: Optional[List[Dict[str, Any]]] = None,
    metadata: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    with get_conn() as conn:
        with conn.cursor() as cur:
            _assert_agent_tables(cur)
            cur.execute(
                """
                INSERT INTO agent_decisions(
                  task_id, entity_type, entity_id, agent_name, model_name,
                  prompt_version, decision_type, decision, confidence,
                  reasoning_summary, tool_calls, metadata
                )
                VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING
                  id, task_id, entity_type, entity_id, agent_name, model_name,
                  prompt_version, decision_type, decision, confidence,
                  reasoning_summary, tool_calls, metadata, created_at
                """,
                (
                    task_id,
                    entity_type,
                    entity_id,
                    agent_name,
                    model_name,
                    prompt_version,
                    decision_type,
                    decision,
                    confidence,
                    reasoning_summary,
                    _jsonb(tool_calls, []),
                    _jsonb(metadata, {}),
                ),
            )
            return _decision_row(cur.fetchone())


def create_human_review_item(
    *,
    entity_type: str,
    entity_id: str,
    review_reason: str,
    source_document_id: Optional[str] = None,
    invoice_id: Optional[str] = None,
    queue_name: str = "default",
    priority: int = 100,
    assigned_to: Optional[str] = None,
    due_at: Optional[datetime.datetime] = None,
    metadata: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    with get_conn() as conn:
        with conn.cursor() as cur:
            _assert_agent_tables(cur)
            cur.execute(
                """
                SELECT
                  id, entity_type, entity_id, source_document_id, invoice_id,
                  queue_name, priority, status, review_reason, assigned_to,
                  due_at, resolution, metadata, created_at, updated_at, resolved_at
                FROM human_review_queue
                WHERE entity_type = %s
                  AND entity_id = %s
                  AND review_reason = %s
                  AND status IN ('open', 'assigned')
                ORDER BY created_at DESC
                LIMIT 1
                """,
                (entity_type, entity_id, review_reason),
            )
            existing = cur.fetchone()
            if existing:
                return _human_review_row(existing)
            cur.execute(
                """
                INSERT INTO human_review_queue(
                  entity_type, entity_id, source_document_id, invoice_id,
                  queue_name, priority, status, review_reason, assigned_to,
                  due_at, metadata
                )
                VALUES(%s, %s, %s, %s, %s, %s, 'open', %s, %s, %s, %s)
                RETURNING
                  id, entity_type, entity_id, source_document_id, invoice_id,
                  queue_name, priority, status, review_reason, assigned_to,
                  due_at, resolution, metadata, created_at, updated_at, resolved_at
                """,
                (
                    entity_type,
                    entity_id,
                    source_document_id,
                    invoice_id,
                    queue_name,
                    priority,
                    review_reason,
                    assigned_to,
                    due_at,
                    _jsonb(metadata, {}),
                ),
            )
            return _human_review_row(cur.fetchone())


def update_human_review_item(
    review_item_id: str,
    *,
    status: Optional[str] = None,
    assigned_to: Optional[str] = None,
    resolution: Optional[str] = None,
    metadata: Optional[Dict[str, Any]] = None,
) -> Optional[Dict[str, Any]]:
    resolved_at = datetime.datetime.now(datetime.timezone.utc) if status in {"resolved", "dismissed"} else None
    with get_conn() as conn:
        with conn.cursor() as cur:
            _assert_agent_tables(cur)
            cur.execute(
                """
                UPDATE human_review_queue
                SET
                  status = COALESCE(%s, status),
                  assigned_to = COALESCE(%s, assigned_to),
                  resolution = COALESCE(%s, resolution),
                  resolved_at = CASE
                    WHEN %s IS NULL THEN resolved_at
                    ELSE %s
                  END,
                  metadata = CASE
                    WHEN %s THEN metadata
                    ELSE COALESCE(metadata, '{}'::jsonb) || %s
                  END,
                  updated_at = now()
                WHERE id = %s
                RETURNING
                  id, entity_type, entity_id, source_document_id, invoice_id,
                  queue_name, priority, status, review_reason, assigned_to,
                  due_at, resolution, metadata, created_at, updated_at, resolved_at
                """,
                (
                    status,
                    assigned_to,
                    resolution,
                    resolved_at,
                    resolved_at,
                    metadata is None,
                    _jsonb(metadata, {}),
                    review_item_id,
                ),
            )
            row = cur.fetchone()
            return _human_review_row(row) if row else None


def record_vendor_communication(
    *,
    direction: str,
    vendor_id: Optional[str] = None,
    invoice_id: Optional[str] = None,
    source_document_id: Optional[str] = None,
    channel: str = "email",
    status: str = "draft",
    recipient: Optional[str] = None,
    subject: Optional[str] = None,
    body: Optional[str] = None,
    approved_by: Optional[str] = None,
    sent_at: Optional[datetime.datetime] = None,
    metadata: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    with get_conn() as conn:
        with conn.cursor() as cur:
            _assert_agent_tables(cur)
            cur.execute(
                """
                INSERT INTO vendor_communications(
                  vendor_id, invoice_id, source_document_id, direction, channel,
                  status, recipient, subject, body, approved_by, sent_at, metadata
                )
                VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING
                  id, vendor_id, invoice_id, source_document_id, direction, channel,
                  status, recipient, subject, body, approved_by, sent_at,
                  metadata, created_at, updated_at
                """,
                (
                    vendor_id,
                    invoice_id,
                    source_document_id,
                    direction,
                    channel,
                    status,
                    recipient,
                    subject,
                    body,
                    approved_by,
                    sent_at,
                    _jsonb(metadata, {}),
                ),
            )
            return _vendor_communication_row(cur.fetchone())


def get_vendor_communication(communication_id: str) -> Optional[Dict[str, Any]]:
    with get_conn() as conn:
        with conn.cursor() as cur:
            _assert_agent_tables(cur)
            cur.execute(
                """
                SELECT
                  id, vendor_id, invoice_id, source_document_id, direction, channel,
                  status, recipient, subject, body, approved_by, sent_at,
                  metadata, created_at, updated_at
                FROM vendor_communications
                WHERE id = %s
                """,
                (communication_id,),
            )
            row = cur.fetchone()
            return _vendor_communication_row(row) if row else None


def list_vendor_communications(
    *,
    status: Optional[str] = None,
    direction: Optional[str] = None,
    vendor_id: Optional[str] = None,
    invoice_id: Optional[str] = None,
    source_document_id: Optional[str] = None,
    limit: int = 100,
) -> List[Dict[str, Any]]:
    limit = _coerce_limit(limit)
    with get_conn() as conn:
        with conn.cursor() as cur:
            _assert_agent_tables(cur)
            cur.execute(
                """
                SELECT
                  id, vendor_id, invoice_id, source_document_id, direction, channel,
                  status, recipient, subject, body, approved_by, sent_at,
                  metadata, created_at, updated_at
                FROM vendor_communications
                WHERE (%s IS NULL OR status = %s)
                  AND (%s IS NULL OR direction = %s)
                  AND (%s IS NULL OR vendor_id = %s)
                  AND (%s IS NULL OR invoice_id = %s)
                  AND (%s IS NULL OR source_document_id = %s)
                ORDER BY created_at DESC
                LIMIT %s
                """,
                (
                    status,
                    status,
                    direction,
                    direction,
                    vendor_id,
                    vendor_id,
                    invoice_id,
                    invoice_id,
                    source_document_id,
                    source_document_id,
                    limit,
                ),
            )
            return [_vendor_communication_row(row) for row in cur.fetchall()]


def update_vendor_communication(
    communication_id: str,
    *,
    status: Optional[str] = None,
    recipient: Optional[str] = None,
    subject: Optional[str] = None,
    body: Optional[str] = None,
    approved_by: Optional[str] = None,
    sent_at: Optional[datetime.datetime] = None,
    metadata: Optional[Dict[str, Any]] = None,
) -> Optional[Dict[str, Any]]:
    with get_conn() as conn:
        with conn.cursor() as cur:
            _assert_agent_tables(cur)
            cur.execute(
                """
                UPDATE vendor_communications
                SET
                  status = COALESCE(%s, status),
                  recipient = COALESCE(%s, recipient),
                  subject = COALESCE(%s, subject),
                  body = COALESCE(%s, body),
                  approved_by = COALESCE(%s, approved_by),
                  sent_at = COALESCE(%s, sent_at),
                  metadata = CASE
                    WHEN %s THEN metadata
                    ELSE COALESCE(metadata, '{}'::jsonb) || %s
                  END,
                  updated_at = now()
                WHERE id = %s
                RETURNING
                  id, vendor_id, invoice_id, source_document_id, direction, channel,
                  status, recipient, subject, body, approved_by, sent_at,
                  metadata, created_at, updated_at
                """,
                (
                    status,
                    recipient,
                    subject,
                    body,
                    approved_by,
                    sent_at,
                    metadata is None,
                    _jsonb(metadata, {}),
                    communication_id,
                ),
            )
            row = cur.fetchone()
            return _vendor_communication_row(row) if row else None


def upsert_sla_config(
    *,
    entity_type: str,
    state_name: str,
    target_minutes: int,
    escalation_queue: Optional[str] = None,
    is_active: bool = True,
    metadata: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    with get_conn() as conn:
        with conn.cursor() as cur:
            _assert_agent_tables(cur)
            cur.execute(
                """
                INSERT INTO sla_configs(
                  entity_type, state_name, target_minutes, escalation_queue, is_active, metadata
                )
                VALUES(%s, %s, %s, %s, %s, %s)
                ON CONFLICT (entity_type, state_name)
                DO UPDATE SET
                  target_minutes = EXCLUDED.target_minutes,
                  escalation_queue = EXCLUDED.escalation_queue,
                  is_active = EXCLUDED.is_active,
                  metadata = COALESCE(sla_configs.metadata, '{}'::jsonb) || EXCLUDED.metadata,
                  updated_at = now()
                RETURNING
                  id, entity_type, state_name, target_minutes, escalation_queue,
                  is_active, metadata, created_at, updated_at
                """,
                (
                    entity_type,
                    state_name,
                    int(target_minutes),
                    escalation_queue,
                    is_active,
                    _jsonb(metadata, {}),
                ),
            )
            return _sla_config_row(cur.fetchone())


def list_source_documents(
    *,
    ingestion_status: Optional[str] = None,
    segmentation_status: Optional[str] = None,
    extraction_status: Optional[str] = None,
    vendor_id: Optional[str] = None,
    source_type: Optional[str] = None,
    limit: int = 100,
) -> List[Dict[str, Any]]:
    limit = _coerce_limit(limit)
    with get_conn() as conn:
        with conn.cursor() as cur:
            _assert_agent_tables(cur)
            cur.execute(
                """
                SELECT
                  id, source_type, source_ref, original_filename, storage_provider, storage_path,
                  content_type, file_size_bytes, file_hash, page_count, from_email,
                  email_message_id, vendor_id, ingestion_status, segmentation_status,
                  extraction_status, metadata, received_at, created_at, updated_at
                FROM source_documents
                WHERE (%s IS NULL OR ingestion_status = %s)
                  AND (%s IS NULL OR segmentation_status = %s)
                  AND (%s IS NULL OR extraction_status = %s)
                  AND (%s IS NULL OR vendor_id = %s)
                  AND (%s IS NULL OR source_type = %s)
                ORDER BY received_at DESC, created_at DESC
                LIMIT %s
                """,
                (
                    ingestion_status,
                    ingestion_status,
                    segmentation_status,
                    segmentation_status,
                    extraction_status,
                    extraction_status,
                    vendor_id,
                    vendor_id,
                    source_type,
                    source_type,
                    limit,
                ),
            )
            return [_source_document_row(row) for row in cur.fetchall()]


def list_source_document_segments(source_document_id: str) -> List[Dict[str, Any]]:
    with get_conn() as conn:
        with conn.cursor() as cur:
            _assert_agent_tables(cur)
            cur.execute(
                """
                SELECT
                  id, source_document_id, segment_index, page_from, page_to,
                  segment_path, confidence, status, invoice_id, metadata,
                  created_at, updated_at
                FROM source_document_segments
                WHERE source_document_id = %s
                ORDER BY segment_index ASC, created_at ASC
                """,
                (source_document_id,),
            )
            return [_segment_row(row) for row in cur.fetchall()]


def get_workflow_state(entity_type: str, entity_id: str) -> Optional[Dict[str, Any]]:
    with get_conn() as conn:
        with conn.cursor() as cur:
            _assert_agent_tables(cur)
            cur.execute(
                """
                SELECT
                  id, entity_type, entity_id, current_state, current_stage,
                  confidence, metadata, created_at, updated_at
                FROM workflow_states
                WHERE entity_type = %s AND entity_id = %s
                """,
                (entity_type, entity_id),
            )
            row = cur.fetchone()
            return _workflow_state_row(row) if row else None


def list_workflow_states(
    *,
    entity_type: Optional[str] = None,
    current_state: Optional[str] = None,
    current_stage: Optional[str] = None,
    limit: int = 100,
) -> List[Dict[str, Any]]:
    limit = _coerce_limit(limit)
    with get_conn() as conn:
        with conn.cursor() as cur:
            _assert_agent_tables(cur)
            cur.execute(
                """
                SELECT
                  id, entity_type, entity_id, current_state, current_stage,
                  confidence, metadata, created_at, updated_at
                FROM workflow_states
                WHERE (%s IS NULL OR entity_type = %s)
                  AND (%s IS NULL OR current_state = %s)
                  AND (%s IS NULL OR current_stage = %s)
                ORDER BY updated_at DESC, created_at DESC
                LIMIT %s
                """,
                (
                    entity_type,
                    entity_type,
                    current_state,
                    current_state,
                    current_stage,
                    current_stage,
                    limit,
                ),
            )
            return [_workflow_state_row(row) for row in cur.fetchall()]


def list_workflow_history(
    entity_type: str,
    entity_id: str,
    *,
    limit: int = 100,
) -> List[Dict[str, Any]]:
    limit = _coerce_limit(limit)
    with get_conn() as conn:
        with conn.cursor() as cur:
            _assert_agent_tables(cur)
            cur.execute(
                """
                SELECT
                  id, entity_type, entity_id, from_state, to_state, event_type,
                  reason, actor_type, actor_id, metadata, created_at
                FROM workflow_state_history
                WHERE entity_type = %s AND entity_id = %s
                ORDER BY created_at DESC
                LIMIT %s
                """,
                (entity_type, entity_id, limit),
            )
            return [_workflow_history_row(row) for row in cur.fetchall()]


def list_agent_tasks(
    *,
    status: Optional[str] = None,
    task_type: Optional[str] = None,
    entity_type: Optional[str] = None,
    entity_id: Optional[str] = None,
    source_document_id: Optional[str] = None,
    retries_only: bool = False,
    limit: int = 100,
) -> List[Dict[str, Any]]:
    limit = _coerce_limit(limit)
    with get_conn() as conn:
        with conn.cursor() as cur:
            _assert_agent_tables(cur)
            cur.execute(
                """
                SELECT
                  id, task_type, entity_type, entity_id, source_document_id,
                  priority, status, attempt_count, max_attempts, dedupe_key,
                  available_at, lease_expires_at, locked_by, locked_at,
                  heartbeat_at, started_at, completed_at, last_error,
                  payload, result, created_at, updated_at
                FROM agent_tasks
                WHERE (%s IS NULL OR status = %s)
                  AND (%s IS NULL OR task_type = %s)
                  AND (%s IS NULL OR entity_type = %s)
                  AND (%s IS NULL OR entity_id = %s)
                  AND (%s IS NULL OR source_document_id = %s)
                  AND (%s = false OR attempt_count > 1)
                ORDER BY updated_at DESC, created_at DESC
                LIMIT %s
                """,
                (
                    status,
                    status,
                    task_type,
                    task_type,
                    entity_type,
                    entity_type,
                    entity_id,
                    entity_id,
                    source_document_id,
                    source_document_id,
                    retries_only,
                    limit,
                ),
            )
            return [_task_row(row) for row in cur.fetchall()]


def list_agent_decisions(
    *,
    entity_type: Optional[str] = None,
    entity_id: Optional[str] = None,
    task_id: Optional[str] = None,
    source_document_id: Optional[str] = None,
    limit: int = 100,
) -> List[Dict[str, Any]]:
    limit = _coerce_limit(limit)
    with get_conn() as conn:
        with conn.cursor() as cur:
            _assert_agent_tables(cur)
            cur.execute(
                """
                SELECT
                  d.id, d.task_id, d.entity_type, d.entity_id, d.agent_name, d.model_name,
                  d.prompt_version, d.decision_type, d.decision, d.confidence,
                  d.reasoning_summary, d.tool_calls, d.metadata, d.created_at
                FROM agent_decisions AS d
                LEFT JOIN agent_tasks AS t
                  ON t.id = d.task_id
                WHERE (%s IS NULL OR d.entity_type = %s)
                  AND (%s IS NULL OR d.entity_id = %s)
                  AND (%s IS NULL OR d.task_id = %s)
                  AND (%s IS NULL OR t.source_document_id = %s)
                ORDER BY d.created_at DESC
                LIMIT %s
                """,
                (
                    entity_type,
                    entity_type,
                    entity_id,
                    entity_id,
                    task_id,
                    task_id,
                    source_document_id,
                    source_document_id,
                    limit,
                ),
            )
            return [_decision_row(row) for row in cur.fetchall()]


def list_human_review_items(
    *,
    status: Optional[str] = None,
    queue_name: Optional[str] = None,
    entity_type: Optional[str] = None,
    entity_id: Optional[str] = None,
    source_document_id: Optional[str] = None,
    invoice_id: Optional[str] = None,
    assigned_to: Optional[str] = None,
    limit: int = 100,
) -> List[Dict[str, Any]]:
    limit = _coerce_limit(limit)
    with get_conn() as conn:
        with conn.cursor() as cur:
            _assert_agent_tables(cur)
            cur.execute(
                """
                SELECT
                  id, entity_type, entity_id, source_document_id, invoice_id,
                  queue_name, priority, status, review_reason, assigned_to,
                  due_at, resolution, metadata, created_at, updated_at, resolved_at
                FROM human_review_queue
                WHERE (%s IS NULL OR status = %s)
                  AND (%s IS NULL OR queue_name = %s)
                  AND (%s IS NULL OR entity_type = %s)
                  AND (%s IS NULL OR entity_id = %s)
                  AND (%s IS NULL OR source_document_id = %s)
                  AND (%s IS NULL OR invoice_id = %s)
                  AND (%s IS NULL OR assigned_to = %s)
                ORDER BY priority ASC, created_at DESC
                LIMIT %s
                """,
                (
                    status,
                    status,
                    queue_name,
                    queue_name,
                    entity_type,
                    entity_type,
                    entity_id,
                    entity_id,
                    source_document_id,
                    source_document_id,
                    invoice_id,
                    invoice_id,
                    assigned_to,
                    assigned_to,
                    limit,
                ),
            )
            return [_human_review_row(row) for row in cur.fetchall()]


def list_sla_configs(
    *,
    entity_type: Optional[str] = None,
    active_only: bool = False,
) -> List[Dict[str, Any]]:
    with get_conn() as conn:
        with conn.cursor() as cur:
            _assert_agent_tables(cur)
            cur.execute(
                """
                SELECT
                  id, entity_type, state_name, target_minutes, escalation_queue,
                  is_active, metadata, created_at, updated_at
                FROM sla_configs
                WHERE (%s IS NULL OR entity_type = %s)
                  AND (%s = false OR is_active = true)
                ORDER BY entity_type ASC, state_name ASC
                """,
                (
                    entity_type,
                    entity_type,
                    active_only,
                ),
            )
            return [_sla_config_row(row) for row in cur.fetchall()]


def list_sla_breaches(
    *,
    entity_type: Optional[str] = None,
    current_state: Optional[str] = None,
    limit: int = 100,
) -> List[Dict[str, Any]]:
    limit = _coerce_limit(limit)
    with get_conn() as conn:
        with conn.cursor() as cur:
            _assert_agent_tables(cur)
            cur.execute(
                """
                SELECT
                  ws.entity_type,
                  ws.entity_id,
                  ws.current_state,
                  ws.current_stage,
                  ws.confidence,
                  ws.metadata,
                  ws.updated_at,
                  sc.target_minutes,
                  sc.escalation_queue,
                  sc.metadata,
                  ROUND((EXTRACT(EPOCH FROM (now() - ws.updated_at)) / 60.0)::numeric, 2) AS age_minutes,
                  ROUND(((EXTRACT(EPOCH FROM (now() - ws.updated_at)) / 60.0) - sc.target_minutes)::numeric, 2) AS breach_minutes
                FROM workflow_states AS ws
                INNER JOIN sla_configs AS sc
                  ON sc.entity_type = ws.entity_type
                 AND sc.state_name = ws.current_state
                 AND sc.is_active = true
                WHERE (%s IS NULL OR ws.entity_type = %s)
                  AND (%s IS NULL OR ws.current_state = %s)
                  AND (EXTRACT(EPOCH FROM (now() - ws.updated_at)) / 60.0) > sc.target_minutes
                ORDER BY breach_minutes DESC, ws.updated_at ASC
                LIMIT %s
                """,
                (
                    entity_type,
                    entity_type,
                    current_state,
                    current_state,
                    limit,
                ),
            )
            return [_sla_breach_row(row) for row in cur.fetchall()]


def get_source_document_detail(source_document_id: str) -> Optional[Dict[str, Any]]:
    with get_conn() as conn:
        with conn.cursor() as cur:
            _assert_agent_tables(cur)
            cur.execute(
                """
                SELECT
                  id, source_type, source_ref, original_filename, storage_provider, storage_path,
                  content_type, file_size_bytes, file_hash, page_count, from_email,
                  email_message_id, vendor_id, ingestion_status, segmentation_status,
                  extraction_status, metadata, received_at, created_at, updated_at
                FROM source_documents
                WHERE id = %s
                """,
                (source_document_id,),
            )
            source_row = cur.fetchone()
            if not source_row:
                return None
            source_document = _source_document_row(source_row)

            cur.execute(
                """
                SELECT
                  id, source_document_id, segment_index, page_from, page_to,
                  segment_path, confidence, status, invoice_id, metadata,
                  created_at, updated_at
                FROM source_document_segments
                WHERE source_document_id = %s
                ORDER BY segment_index ASC, created_at ASC
                """,
                (source_document_id,),
            )
            segments = [_segment_row(row) for row in cur.fetchall()]

            cur.execute(
                """
                SELECT
                  id, entity_type, entity_id, current_state, current_stage,
                  confidence, metadata, created_at, updated_at
                FROM workflow_states
                WHERE entity_type = 'source_document' AND entity_id = %s
                """,
                (source_document_id,),
            )
            workflow_row = cur.fetchone()
            workflow_state = _workflow_state_row(workflow_row) if workflow_row else None

            cur.execute(
                """
                SELECT
                  id, entity_type, entity_id, from_state, to_state, event_type,
                  reason, actor_type, actor_id, metadata, created_at
                FROM workflow_state_history
                WHERE entity_type = 'source_document' AND entity_id = %s
                ORDER BY created_at DESC
                LIMIT 200
                """,
                (source_document_id,),
            )
            workflow_history = [_workflow_history_row(row) for row in cur.fetchall()]

            cur.execute(
                """
                SELECT
                  id, task_type, entity_type, entity_id, source_document_id,
                  priority, status, attempt_count, max_attempts, dedupe_key,
                  available_at, lease_expires_at, locked_by, locked_at,
                  heartbeat_at, started_at, completed_at, last_error,
                  payload, result, created_at, updated_at
                FROM agent_tasks
                WHERE source_document_id = %s
                   OR (entity_type = 'source_document' AND entity_id = %s)
                ORDER BY updated_at DESC, created_at DESC
                LIMIT 200
                """,
                (source_document_id, source_document_id),
            )
            tasks = [_task_row(row) for row in cur.fetchall()]

            cur.execute(
                """
                SELECT
                  d.id, d.task_id, d.entity_type, d.entity_id, d.agent_name, d.model_name,
                  d.prompt_version, d.decision_type, d.decision, d.confidence,
                  d.reasoning_summary, d.tool_calls, d.metadata, d.created_at
                FROM agent_decisions AS d
                INNER JOIN agent_tasks AS t
                  ON t.id = d.task_id
                WHERE t.source_document_id = %s
                   OR (d.entity_type = 'source_document' AND d.entity_id = %s)
                ORDER BY d.created_at DESC
                LIMIT 200
                """,
                (source_document_id, source_document_id),
            )
            decisions = [_decision_row(row) for row in cur.fetchall()]

            cur.execute(
                """
                SELECT
                  id, entity_type, entity_id, source_document_id, invoice_id,
                  queue_name, priority, status, review_reason, assigned_to,
                  due_at, resolution, metadata, created_at, updated_at, resolved_at
                FROM human_review_queue
                WHERE source_document_id = %s
                   OR (entity_type = 'source_document' AND entity_id = %s)
                ORDER BY priority ASC, created_at DESC
                LIMIT 200
                """,
                (source_document_id, source_document_id),
            )
            review_items = [_human_review_row(row) for row in cur.fetchall()]

            return {
                "source_document": source_document,
                "segments": segments,
                "workflow_state": workflow_state,
                "workflow_history": workflow_history,
                "tasks": tasks,
                "decisions": decisions,
                "review_items": review_items,
            }


def get_agent_operations_overview() -> Dict[str, Any]:
    with get_conn() as conn:
        with conn.cursor() as cur:
            _assert_agent_tables(cur)

            cur.execute(
                """
                SELECT ingestion_status, COUNT(*)
                FROM source_documents
                GROUP BY ingestion_status
                ORDER BY ingestion_status ASC
                """
            )
            ingestion_counts = {row[0]: int(row[1]) for row in cur.fetchall()}

            cur.execute(
                """
                SELECT segmentation_status, COUNT(*)
                FROM source_documents
                GROUP BY segmentation_status
                ORDER BY segmentation_status ASC
                """
            )
            segmentation_counts = {row[0]: int(row[1]) for row in cur.fetchall()}

            cur.execute(
                """
                SELECT extraction_status, COUNT(*)
                FROM source_documents
                GROUP BY extraction_status
                ORDER BY extraction_status ASC
                """
            )
            extraction_counts = {row[0]: int(row[1]) for row in cur.fetchall()}

            cur.execute(
                """
                SELECT status, COUNT(*)
                FROM agent_tasks
                GROUP BY status
                ORDER BY status ASC
                """
            )
            task_counts = {row[0]: int(row[1]) for row in cur.fetchall()}

            cur.execute("SELECT COUNT(*) FROM agent_tasks WHERE attempt_count > 1")
            retry_task_count_row = cur.fetchone()
            retry_task_count = int(retry_task_count_row[0]) if retry_task_count_row else 0

            cur.execute(
                """
                SELECT status, COUNT(*)
                FROM human_review_queue
                GROUP BY status
                ORDER BY status ASC
                """
            )
            review_counts = {row[0]: int(row[1]) for row in cur.fetchall()}

            cur.execute(
                """
                SELECT entity_type, current_state, COUNT(*)
                FROM workflow_states
                GROUP BY entity_type, current_state
                ORDER BY COUNT(*) DESC, entity_type ASC, current_state ASC
                LIMIT 20
                """
            )
            workflow_counts = [
                {
                    "entity_type": row[0],
                    "current_state": row[1],
                    "count": int(row[2]),
                }
                for row in cur.fetchall()
            ]

            cur.execute(
                """
                SELECT COUNT(*)
                FROM workflow_states AS ws
                INNER JOIN sla_configs AS sc
                  ON sc.entity_type = ws.entity_type
                 AND sc.state_name = ws.current_state
                 AND sc.is_active = true
                WHERE (EXTRACT(EPOCH FROM (now() - ws.updated_at)) / 60.0) > sc.target_minutes
                """
            )
            sla_breach_count_row = cur.fetchone()
            sla_breach_count = int(sla_breach_count_row[0]) if sla_breach_count_row else 0

            return {
                "source_documents": {
                    "ingestion": ingestion_counts,
                    "segmentation": segmentation_counts,
                    "extraction": extraction_counts,
                },
                "tasks": {
                    "by_status": task_counts,
                    "retried_task_count": retry_task_count,
                },
                "review_queue": {
                    "by_status": review_counts,
                },
                "workflow_states": workflow_counts,
                "sla": {
                    "active_breach_count": sla_breach_count,
                },
            }
