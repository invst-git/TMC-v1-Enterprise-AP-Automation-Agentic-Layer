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
_SLA_RUNTIME_MIGRATION = "migrations/2026-03-29_add_sla_runtime_fields.sql"


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
    cur.execute(
        """
        SELECT
          EXISTS(
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = 'workflow_states'
              AND column_name = 'breach_risk'
          ),
          EXISTS(
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = 'sla_configs'
              AND column_name = 'warning_minutes'
          ),
          EXISTS(
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = 'sla_configs'
              AND column_name = 'breach_minutes'
          )
        """
    )
    runtime_columns = cur.fetchone()
    if not runtime_columns or not all(bool(value) for value in runtime_columns):
        raise RuntimeError(
            "Agent SLA runtime columns not found. "
            f"Run migration: {_SLA_RUNTIME_MIGRATION}."
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
        "breach_risk": row[6] or "ok",
        "metadata": _json_value(row[7], {}),
        "created_at": _iso(row[8]),
        "updated_at": _iso(row[9]),
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


def _review_status_display(status: Optional[str]) -> str:
    lowered = str(status or "").strip().lower()
    if lowered == "open":
        return "pending"
    if lowered == "assigned":
        return "in_review"
    return lowered or "pending"


def _review_reason_label(review_reason: Optional[str]) -> str:
    mapping = {
        "missing_po_number": "Missing PO Number",
        "no_open_po_candidates": "No Open PO Candidate",
        "amount_outside_tolerance": "Amount Outside Tolerance",
        "candidate_missing_po_total": "PO Total Missing",
        "vendor_mismatch": "Vendor Mismatch",
        "candidate_vendor_mismatch": "Vendor Candidate Mismatch",
        "candidate_vendor_and_currency_mismatch": "Vendor and Currency Mismatch",
        "candidate_currency_mismatch": "Currency Mismatch",
        "missing_invoice_total": "Missing Invoice Total",
        "potential_duplicate_invoice": "Potential Duplicate Invoice",
        "approval_required": "Approval Required",
        "ocr_extraction_failed": "OCR Extraction Failed",
        "ocr_output_unreadable": "OCR Output Unreadable",
        "needs_clarification": "Needs Clarification",
    }
    key = str(review_reason or "").strip()
    if key in mapping:
        return mapping[key]
    if not key:
        return "Needs Review"
    return " ".join(part.capitalize() for part in key.replace("-", "_").split("_"))


def _load_invoice_summaries(invoice_ids: Sequence[str]) -> Dict[str, Dict[str, Any]]:
    normalized_invoice_ids = [str(invoice_id) for invoice_id in invoice_ids if invoice_id]
    if not normalized_invoice_ids:
        return {}

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                  i.id,
                  i.invoice_number,
                  i.invoice_date,
                  i.total_amount,
                  i.status,
                  i.po_number,
                  i.currency,
                  i.supplier_name,
                  v.name AS vendor_name
                FROM invoices AS i
                LEFT JOIN vendors AS v
                  ON v.id = i.vendor_id
                WHERE i.id = ANY(%s)
                """,
                (normalized_invoice_ids,),
            )
            summaries = {}
            for row in cur.fetchall():
                invoice_date = row[2]
                summaries[str(row[0])] = {
                    "id": str(row[0]),
                    "invoice_number": row[1] or "",
                    "invoice_date": invoice_date.isoformat() if invoice_date else None,
                    "total_amount": float(row[3]) if row[3] is not None else None,
                    "status": row[4] or "",
                    "po_number": row[5] or "",
                    "currency": row[6] or "USD",
                    "supplier_name": row[7] or "",
                    "vendor_name": row[8] or row[7] or "Unknown Vendor",
                }
            return summaries


def _load_segment_invoice_ids(segment_ids: Sequence[str]) -> Dict[str, Optional[str]]:
    normalized_segment_ids = [str(segment_id) for segment_id in segment_ids if segment_id]
    if not normalized_segment_ids:
        return {}

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, invoice_id, metadata
                FROM source_document_segments
                WHERE id = ANY(%s)
                """,
                (normalized_segment_ids,),
            )
            resolved: Dict[str, Optional[str]] = {}
            for row in cur.fetchall():
                segment_id = str(row[0])
                invoice_id = str(row[1]) if row[1] else None
                metadata = _json_value(row[2], {})
                if not invoice_id:
                    invoice_id = (metadata.get("persistence") or {}).get("invoice_id")
                    invoice_id = str(invoice_id) if invoice_id else None
                resolved[segment_id] = invoice_id
            return resolved


def _load_extracted_fields_from_review_metadata(metadata: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    extracted_fields = metadata.get("extracted_fields")
    if isinstance(extracted_fields, dict) and extracted_fields:
        return extracted_fields

    json_path = metadata.get("json_path")
    if not json_path:
        return None

    try:
        with open(json_path, "r", encoding="utf-8") as handle:
            payload = json.load(handle)
    except Exception:
        return None

    return payload if isinstance(payload, dict) else None


def _coerce_float(value: Any) -> Optional[float]:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except Exception:
        return None


def _invoice_summary_from_extracted_fields(
    extracted_fields: Optional[Dict[str, Any]],
    *,
    invoice_id: Optional[str] = None,
    status: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    if not isinstance(extracted_fields, dict) or not extracted_fields:
        return None

    supplier_name = str(extracted_fields.get("supplier_name") or "").strip()
    vendor_name = str(extracted_fields.get("vendor_name") or supplier_name).strip()
    invoice_date = extracted_fields.get("invoice_date")
    invoice_date = str(invoice_date).strip() if invoice_date else None

    return {
        "id": str(invoice_id) if invoice_id else None,
        "invoice_number": str(extracted_fields.get("invoice_number") or "").strip(),
        "invoice_date": invoice_date or None,
        "total_amount": _coerce_float(extracted_fields.get("total_amount")),
        "status": str(status or extracted_fields.get("status") or "").strip(),
        "po_number": str(extracted_fields.get("po_number") or "").strip(),
        "currency": str(extracted_fields.get("currency") or "USD").strip() or "USD",
        "supplier_name": supplier_name,
        "vendor_name": vendor_name or supplier_name or "Unknown Vendor",
    }


def _load_payment_authorization_summaries(request_ids: Sequence[str]) -> Dict[str, Dict[str, Any]]:
    normalized_request_ids = [str(request_id) for request_id in request_ids if request_id]
    if not normalized_request_ids:
        return {}

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                  id,
                  approval_status,
                  invoice_ids,
                  customer,
                  currency,
                  save_method,
                  total_amount,
                  invoice_count,
                  risk_level,
                  recommendation,
                  risk_reasons,
                  metadata
                FROM payment_authorization_requests
                WHERE id = ANY(%s)
                """,
                (normalized_request_ids,),
            )
            summaries: Dict[str, Dict[str, Any]] = {}
            for row in cur.fetchall():
                metadata = _json_value(row[11], {})
                customer = _json_value(row[3], {})
                invoice_ids = _json_value(row[2], [])
                invoice_summaries = metadata.get("invoice_summaries") or []
                vendor_names = list(
                    dict.fromkeys(
                        [
                            str(invoice.get("supplier_name") or "").strip()
                            for invoice in invoice_summaries
                            if str(invoice.get("supplier_name") or "").strip()
                        ]
                    )
                )
                if not vendor_names:
                    vendor_names = list(
                        dict.fromkeys(
                            [
                                str(invoice.get("vendor_name") or "").strip()
                                for invoice in invoice_summaries
                                if str(invoice.get("vendor_name") or "").strip()
                            ]
                        )
                    )
                summaries[str(row[0])] = {
                    "id": str(row[0]),
                    "approval_status": row[1] or "pending_approval",
                    "invoice_ids": invoice_ids,
                    "customer": customer,
                    "currency": row[4] or "USD",
                    "save_method": bool(row[5]),
                    "total_amount": float(row[6]) if row[6] is not None else None,
                    "invoice_count": int(row[7] or 0),
                    "risk_level": row[8] or "medium",
                    "recommendation": row[9] or "approval_required",
                    "risk_reasons": _json_value(row[10], []),
                    "risk_signals": metadata.get("risk_signals") or {},
                    "invoice_summaries": invoice_summaries,
                    "vendor_names": vendor_names,
                }
            return summaries


def _resolve_review_item_invoice_ids(items: Sequence[Dict[str, Any]]) -> Dict[str, Optional[str]]:
    resolved: Dict[str, Optional[str]] = {}
    segment_ids: List[str] = []

    for item in items:
        metadata = item.get("metadata") or {}
        persistence = metadata.get("persistence") or {}
        invoice_id = item.get("invoice_id") or persistence.get("invoice_id")
        if invoice_id:
            resolved[item["id"]] = str(invoice_id)
            continue
        if item.get("entity_type") == "source_document_segment" and item.get("entity_id"):
            segment_ids.append(str(item["entity_id"]))

    segment_invoice_ids = _load_segment_invoice_ids(segment_ids)
    for item in items:
        if item["id"] in resolved:
            continue
        if item.get("entity_type") == "source_document_segment":
            resolved[item["id"]] = segment_invoice_ids.get(str(item.get("entity_id") or ""))
        else:
            resolved[item["id"]] = None
    return resolved


def _review_candidate_pos(metadata: Dict[str, Any]) -> List[Dict[str, Any]]:
    candidate_sources = []
    analysis = metadata.get("analysis") or {}
    resolution_packet = metadata.get("resolution_packet") or {}
    candidate_sources.extend(analysis.get("candidates") or [])
    candidate_sources.extend((resolution_packet.get("initial_analysis") or {}).get("candidates") or [])
    candidate_sources.extend((resolution_packet.get("final_analysis") or {}).get("candidates") or [])

    normalized = {}
    for candidate in candidate_sources:
        po_id = str(candidate.get("po_id") or "")
        if not po_id:
            continue
        existing = normalized.get(po_id)
        confidence = float(candidate.get("confidence") or 0.0)
        record = {
            "po_id": po_id,
            "po_number": candidate.get("po_number") or "",
            "total_amount": candidate.get("total_amount"),
            "currency": candidate.get("currency") or "USD",
            "vendor_id": candidate.get("vendor_id"),
            "amount_diff": candidate.get("diff"),
            "vendor_match": bool(candidate.get("vendor_match")),
            "currency_match": bool(candidate.get("currency_match")),
            "within_tolerance": bool(candidate.get("within_tolerance")),
            "similarity_score": confidence,
            "eligibility_reason": candidate.get("eligibility_reason") or "",
        }
        if existing is None or confidence > float(existing.get("similarity_score") or 0.0):
            normalized[po_id] = record

    return sorted(
        normalized.values(),
        key=lambda item: (-float(item.get("similarity_score") or 0.0), str(item.get("po_number") or "")),
    )


def _payment_authorization_invoice_summary(summary: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not summary:
        return None

    invoice_summaries = summary.get("invoice_summaries") or []
    primary_invoice = invoice_summaries[0] if invoice_summaries else {}
    vendor_names = summary.get("vendor_names") or []
    vendor_name = ""
    if len(vendor_names) == 1:
        vendor_name = vendor_names[0]
    elif len(vendor_names) > 1:
        vendor_name = f"{len(vendor_names)} vendors"

    invoice_number = (
        primary_invoice.get("invoice_number")
        if summary.get("invoice_count") == 1 and primary_invoice
        else f"{summary.get('invoice_count') or 0} invoices"
    )

    return {
        "id": primary_invoice.get("id"),
        "invoice_number": invoice_number,
        "invoice_date": primary_invoice.get("due_date"),
        "total_amount": summary.get("total_amount"),
        "status": summary.get("approval_status"),
        "po_number": primary_invoice.get("po_number") or "",
        "currency": summary.get("currency") or primary_invoice.get("currency") or "USD",
        "supplier_name": vendor_name,
        "vendor_name": vendor_name,
    }


def _enrich_human_review_items(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    resolved_invoice_ids = _resolve_review_item_invoice_ids(items)
    invoice_ids = [invoice_id for invoice_id in resolved_invoice_ids.values() if invoice_id]
    invoice_summaries = _load_invoice_summaries(invoice_ids)
    payment_authorization_ids = [
        str(item.get("entity_id"))
        for item in items
        if item.get("entity_type") == "payment_authorization" and item.get("entity_id")
    ]
    payment_authorization_summaries = _load_payment_authorization_summaries(payment_authorization_ids)

    enriched_items = []
    for item in items:
        metadata = item.get("metadata") or {}
        effective_invoice_id = resolved_invoice_ids.get(item["id"])
        payment_authorization_summary = (
            payment_authorization_summaries.get(str(item.get("entity_id")))
            if item.get("entity_type") == "payment_authorization"
            else None
        )
        resolution_packet = metadata.get("resolution_packet") or {}
        attempts = resolution_packet.get("attempts") or []
        recommended_action = (
            metadata.get("recommended_action")
            or resolution_packet.get("recommended_action")
            or (
                "Approve to allow payment execution, or reject to stop this payment batch."
                if item.get("entity_type") == "payment_authorization"
                else None
            )
            or metadata.get("summary")
            or "Review the packet and decide the next step."
        )
        candidate_pos = _review_candidate_pos(metadata)
        invoice_summary = (
            invoice_summaries.get(effective_invoice_id)
            or _payment_authorization_invoice_summary(payment_authorization_summary)
        )
        if not invoice_summary:
            invoice_summary = _invoice_summary_from_extracted_fields(
                _load_extracted_fields_from_review_metadata(metadata),
                invoice_id=effective_invoice_id,
                status=(metadata.get("persistence") or {}).get("invoice_status"),
            )
        enriched = {
            **item,
            "invoice_id": effective_invoice_id,
            "display_status": _review_status_display(item.get("status")),
            "review_reason_label": _review_reason_label(item.get("review_reason")),
            "invoice_summary": invoice_summary,
            "automated_attempt_count": int(
                resolution_packet.get("attempt_count")
                or len(attempts)
                or 0
            ),
            "recommended_action": recommended_action,
            "resolution_packet": resolution_packet,
            "candidate_pos": candidate_pos,
            "payment_authorization_summary": payment_authorization_summary,
            "is_urgent": int(item.get("priority") or 100) <= 80,
        }
        enriched_items.append(enriched)
    return enriched_items


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
        "warning_minutes": int(row[4]) if row[4] is not None else None,
        "breach_minutes": int(row[5]) if row[5] is not None else int(row[3]),
        "escalation_queue": row[6],
        "is_active": bool(row[7]),
        "metadata": _json_value(row[8], {}),
        "created_at": _iso(row[9]),
        "updated_at": _iso(row[10]),
    }


def _sla_breach_row(row: Any) -> Dict[str, Any]:
    return {
        "entity_type": row[0],
        "entity_id": str(row[1]),
        "current_state": row[2],
        "current_stage": row[3],
        "confidence": float(row[4]) if row[4] is not None else None,
        "breach_risk": row[5] or "ok",
        "workflow_metadata": _json_value(row[6], {}),
        "state_updated_at": _iso(row[7]),
        "target_minutes": int(row[8]),
        "warning_minutes": int(row[9]),
        "breach_threshold_minutes": int(row[10]),
        "escalation_queue": row[11],
        "sla_metadata": _json_value(row[12], {}),
        "age_minutes": float(row[13]),
        "warning_delta_minutes": float(row[14]),
        "breach_delta_minutes": float(row[15]),
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
    breach_risk: Optional[str] = None,
    event_type: str = "transition",
    reason: Optional[str] = None,
    actor_type: str = "system",
    actor_id: Optional[str] = None,
    metadata: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    previous_state = None
    breach_risk_value = (breach_risk or "ok").strip().lower() or "ok"
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
                  entity_type, entity_id, current_state, current_stage, confidence, breach_risk, metadata
                )
                VALUES(%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (entity_type, entity_id)
                DO UPDATE SET
                  current_state = EXCLUDED.current_state,
                  current_stage = EXCLUDED.current_stage,
                  confidence = EXCLUDED.confidence,
                  breach_risk = EXCLUDED.breach_risk,
                  metadata = COALESCE(workflow_states.metadata, '{}'::jsonb) || EXCLUDED.metadata,
                  updated_at = now()
                RETURNING
                  id, entity_type, entity_id, current_state, current_stage,
                  confidence, breach_risk, metadata, created_at, updated_at
                """,
                (
                    entity_type,
                    entity_id,
                    current_state,
                    current_stage,
                    confidence,
                    breach_risk_value,
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


def upsert_workflow_state_snapshot(
    entity_type: str,
    entity_id: str,
    current_state: str,
    *,
    current_stage: Optional[str] = None,
    confidence: Optional[float] = None,
    breach_risk: str = "ok",
    metadata: Optional[Dict[str, Any]] = None,
    state_updated_at: Optional[datetime.datetime] = None,
) -> Dict[str, Any]:
    timestamp = state_updated_at
    breach_risk_value = (breach_risk or "ok").strip().lower() or "ok"
    with get_conn() as conn:
        with conn.cursor() as cur:
            _assert_agent_tables(cur)
            cur.execute(
                """
                INSERT INTO workflow_states(
                  entity_type, entity_id, current_state, current_stage,
                  confidence, breach_risk, metadata, created_at, updated_at
                )
                VALUES(%s, %s, %s, %s, %s, %s, %s, COALESCE(%s, now()), COALESCE(%s, now()))
                ON CONFLICT (entity_type, entity_id)
                DO UPDATE SET
                  current_state = EXCLUDED.current_state,
                  current_stage = COALESCE(EXCLUDED.current_stage, workflow_states.current_stage),
                  confidence = COALESCE(EXCLUDED.confidence, workflow_states.confidence),
                  breach_risk = COALESCE(EXCLUDED.breach_risk, workflow_states.breach_risk),
                  metadata = COALESCE(workflow_states.metadata, '{}'::jsonb) || EXCLUDED.metadata,
                  updated_at = COALESCE(EXCLUDED.updated_at, workflow_states.updated_at)
                RETURNING
                  id, entity_type, entity_id, current_state, current_stage,
                  confidence, breach_risk, metadata, created_at, updated_at
                """,
                (
                    entity_type,
                    entity_id,
                    current_state,
                    current_stage,
                    confidence,
                    breach_risk_value,
                    _jsonb(metadata, {}),
                    timestamp,
                    timestamp,
                ),
            )
            return _workflow_state_row(cur.fetchone())


def update_workflow_breach_risk(
    entity_type: str,
    entity_id: str,
    breach_risk: str,
    *,
    metadata: Optional[Dict[str, Any]] = None,
) -> Optional[Dict[str, Any]]:
    breach_risk_value = (breach_risk or "ok").strip().lower() or "ok"
    with get_conn() as conn:
        with conn.cursor() as cur:
            _assert_agent_tables(cur)
            cur.execute(
                """
                UPDATE workflow_states
                SET
                  breach_risk = %s,
                  metadata = CASE
                    WHEN %s THEN metadata
                    ELSE COALESCE(metadata, '{}'::jsonb) || %s
                  END
                WHERE entity_type = %s AND entity_id = %s
                RETURNING
                  id, entity_type, entity_id, current_state, current_stage,
                  confidence, breach_risk, metadata, created_at, updated_at
                """,
                (
                    breach_risk_value,
                    metadata is None,
                    _jsonb(metadata, {}),
                    entity_type,
                    entity_id,
                ),
            )
            row = cur.fetchone()
            return _workflow_state_row(row) if row else None


def record_workflow_history_event(
    entity_type: str,
    entity_id: str,
    *,
    event_type: str,
    reason: Optional[str] = None,
    actor_type: str = "system",
    actor_id: Optional[str] = None,
    metadata: Optional[Dict[str, Any]] = None,
) -> Optional[Dict[str, Any]]:
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
            row = cur.fetchone()
            current_state = row[0] if row else None
            if current_state is None:
                return None
            cur.execute(
                """
                INSERT INTO workflow_state_history(
                  entity_type, entity_id, from_state, to_state, event_type,
                  reason, actor_type, actor_id, metadata
                )
                VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING
                  id, entity_type, entity_id, from_state, to_state, event_type,
                  reason, actor_type, actor_id, metadata, created_at
                """,
                (
                    entity_type,
                    entity_id,
                    current_state,
                    current_state,
                    event_type,
                    reason,
                    actor_type,
                    actor_id,
                    _jsonb(metadata, {}),
                ),
            )
            return _workflow_history_row(cur.fetchone())


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


def requeue_agent_task(
    task_id: str,
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
                  status = 'queued',
                  available_at = now() + make_interval(secs => %s),
                  lease_expires_at = NULL,
                  locked_by = NULL,
                  locked_at = NULL,
                  heartbeat_at = NULL,
                  last_error = %s,
                  result = %s,
                  updated_at = now()
                WHERE id = %s AND status IN ('leased', 'running')
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
                ),
            )
            row = cur.fetchone()
            return _task_row(row) if row else None


def dead_letter_agent_task(
    task_id: str,
    *,
    error_message: str,
    error_details: Optional[Dict[str, Any]] = None,
) -> Optional[Dict[str, Any]]:
    with get_conn() as conn:
        with conn.cursor() as cur:
            _assert_agent_tables(cur)
            cur.execute(
                """
                UPDATE agent_tasks
                SET
                  status = 'dead_letter',
                  lease_expires_at = NULL,
                  locked_by = NULL,
                  locked_at = NULL,
                  heartbeat_at = NULL,
                  last_error = %s,
                  result = %s,
                  completed_at = now(),
                  updated_at = now()
                WHERE id = %s AND status IN ('leased', 'running', 'queued', 'failed')
                RETURNING
                  id, task_type, entity_type, entity_id, source_document_id,
                  priority, status, attempt_count, max_attempts, dedupe_key,
                  available_at, lease_expires_at, locked_by, locked_at,
                  heartbeat_at, started_at, completed_at, last_error,
                  payload, result, created_at, updated_at
                """,
                (
                    error_message,
                    _jsonb(error_details, {}),
                    task_id,
                ),
            )
            row = cur.fetchone()
            return _task_row(row) if row else None


def list_stalled_agent_tasks(
    *,
    stale_after_seconds: int,
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
                WHERE status IN ('leased', 'running')
                  AND COALESCE(heartbeat_at, locked_at, started_at, created_at)
                        <= now() - make_interval(secs => %s)
                ORDER BY COALESCE(heartbeat_at, locked_at, started_at, created_at) ASC
                LIMIT %s
                """,
                (int(stale_after_seconds), limit),
            )
            return [_task_row(row) for row in cur.fetchall()]


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
    invoice_id: Optional[str] = None,
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
                  invoice_id = COALESCE(%s, invoice_id),
                  status = COALESCE(%s, status),
                  assigned_to = COALESCE(%s, assigned_to),
                  resolution = COALESCE(%s, resolution),
                  resolved_at = COALESCE(%s::timestamptz, resolved_at),
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
                    invoice_id,
                    status,
                    assigned_to,
                    resolution,
                    resolved_at,
                    metadata is None,
                    _jsonb(metadata, {}),
                    review_item_id,
                ),
            )
            row = cur.fetchone()
            return _human_review_row(row) if row else None


def get_human_review_item(review_item_id: str) -> Optional[Dict[str, Any]]:
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
                WHERE id = %s
                LIMIT %s
                """,
                (review_item_id, 1),
            )
            row = cur.fetchone()
            if not row:
                return None
            return _enrich_human_review_items([_human_review_row(row)])[0]


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
                WHERE (%s::text IS NULL OR status = %s::text)
                  AND (%s::text IS NULL OR direction = %s::text)
                  AND (%s::uuid IS NULL OR vendor_id = %s::uuid)
                  AND (%s::uuid IS NULL OR invoice_id = %s::uuid)
                  AND (%s::uuid IS NULL OR source_document_id = %s::uuid)
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
    warning_minutes: Optional[int] = None,
    breach_minutes: Optional[int] = None,
    escalation_queue: Optional[str] = None,
    is_active: bool = True,
    metadata: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    target_minutes_value = int(target_minutes)
    warning_minutes_value = int(warning_minutes) if warning_minutes is not None else max(1, target_minutes_value // 2)
    breach_minutes_value = int(breach_minutes) if breach_minutes is not None else target_minutes_value
    with get_conn() as conn:
        with conn.cursor() as cur:
            _assert_agent_tables(cur)
            cur.execute(
                """
                INSERT INTO sla_configs(
                  entity_type, state_name, target_minutes, warning_minutes, breach_minutes,
                  escalation_queue, is_active, metadata
                )
                VALUES(%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (entity_type, state_name)
                DO UPDATE SET
                  target_minutes = EXCLUDED.target_minutes,
                  warning_minutes = EXCLUDED.warning_minutes,
                  breach_minutes = EXCLUDED.breach_minutes,
                  escalation_queue = EXCLUDED.escalation_queue,
                  is_active = EXCLUDED.is_active,
                  metadata = COALESCE(sla_configs.metadata, '{}'::jsonb) || EXCLUDED.metadata,
                  updated_at = now()
                RETURNING
                  id, entity_type, state_name, target_minutes, warning_minutes, breach_minutes,
                  escalation_queue, is_active, metadata, created_at, updated_at
                """,
                (
                    entity_type,
                    state_name,
                    target_minutes_value,
                    warning_minutes_value,
                    breach_minutes_value,
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
                WHERE (%s::text IS NULL OR ingestion_status = %s::text)
                  AND (%s::text IS NULL OR segmentation_status = %s::text)
                  AND (%s::text IS NULL OR extraction_status = %s::text)
                  AND (%s::uuid IS NULL OR vendor_id = %s::uuid)
                  AND (%s::text IS NULL OR source_type = %s::text)
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
                  confidence, breach_risk, metadata, created_at, updated_at
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
                  confidence, breach_risk, metadata, created_at, updated_at
                FROM workflow_states
                WHERE (%s::text IS NULL OR entity_type = %s::text)
                  AND (%s::text IS NULL OR current_state = %s::text)
                  AND (%s::text IS NULL OR current_stage = %s::text)
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
                WHERE (%s::text IS NULL OR status = %s::text)
                  AND (%s::text IS NULL OR task_type = %s::text)
                  AND (%s::text IS NULL OR entity_type = %s::text)
                  AND (%s::uuid IS NULL OR entity_id = %s::uuid)
                  AND (%s::uuid IS NULL OR source_document_id = %s::uuid)
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


def get_agent_task(task_id: str) -> Optional[Dict[str, Any]]:
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
                WHERE id = %s
                LIMIT 1
                """,
                (task_id,),
            )
            row = cur.fetchone()
            return _task_row(row) if row else None


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
                WHERE (%s::text IS NULL OR d.entity_type = %s::text)
                  AND (%s::uuid IS NULL OR d.entity_id = %s::uuid)
                  AND (%s::uuid IS NULL OR d.task_id = %s::uuid)
                  AND (%s::uuid IS NULL OR t.source_document_id = %s::uuid)
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


def _parse_iso_datetime(value: Any) -> Optional[datetime.datetime]:
    if value is None:
        return None
    if isinstance(value, datetime.datetime):
        return value
    text = str(value)
    if not text:
        return None
    try:
        return datetime.datetime.fromisoformat(text.replace("Z", "+00:00"))
    except Exception:
        return None


def _decision_tone(decision: Optional[str], metadata: Dict[str, Any]) -> str:
    lowered = str(decision or "").strip().lower()
    if any(token in lowered for token in ("failed", "rejected", "blocked", "dead_letter", "escalat")):
        return "failure"
    if lowered in {"unmatched", "vendor_mismatch", "needs_review"}:
        return "failure"
    if any(token in lowered for token in ("warning", "retry", "review", "pending", "breaching")):
        return "warning"
    if metadata.get("outcome") == "failed":
        return "failure"
    return "success"


def _workflow_tone(event_type: Optional[str], metadata: Dict[str, Any]) -> str:
    lowered = str(event_type or "").strip().lower()
    if lowered == "sla_risk_changed":
        breach_risk = str(metadata.get("breach_risk") or "").strip().lower()
        if breach_risk == "breached":
            return "failure"
        if breach_risk in {"warning", "breaching"}:
            return "warning"
    return "state_transition"


def _review_override_payload(review_item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if not review_item:
        return None
    metadata = review_item.get("metadata") or {}
    resolution = review_item.get("resolution")
    status = review_item.get("status")
    if status not in {"resolved", "dismissed"} and not resolution:
        return None
    override_reason = (
        resolution
        or metadata.get("override_reason")
        or metadata.get("resolution_reason")
        or metadata.get("approved_by")
        or metadata.get("rejected_by")
        or "Resolved during human review"
    )
    return {
        "review_item_id": review_item.get("id"),
        "review_status": status,
        "override_reason": override_reason,
        "resolved_at": review_item.get("resolved_at") or review_item.get("updated_at"),
    }


def _event_counts_toward_processing_time(event: Dict[str, Any]) -> bool:
    if event.get("type") != "state_change":
        return True

    event_type = str(event.get("event_type") or "").strip().lower()
    after_state = str(event.get("after_state") or "").strip().lower()

    # Payment-side invoice status syncs happen after the invoice is already
    # ready for payment, so they should not extend processing-time metrics.
    if event_type == "invoice_status_synced":
        return False
    if after_state in {"payment_pending", "paid"}:
        return False
    return True


def get_invoice_audit_trail(invoice_id: str, *, limit: int = 500) -> Optional[Dict[str, Any]]:
    limit = _coerce_limit(limit, default=500, max_value=1000)
    with get_conn() as conn:
        with conn.cursor() as cur:
            _assert_agent_tables(cur)
            cur.execute("SELECT 1 FROM invoices WHERE id = %s LIMIT 1", (invoice_id,))
            if not cur.fetchone():
                return None

            cur.execute(
                """
                SELECT
                  d.id, d.task_id, d.entity_type, d.entity_id, d.agent_name, d.model_name,
                  d.prompt_version, d.decision_type, d.decision, d.confidence,
                  d.reasoning_summary, d.tool_calls, d.metadata, d.created_at
                FROM agent_decisions AS d
                WHERE d.entity_type = 'invoice' AND d.entity_id = %s
                ORDER BY d.created_at ASC
                LIMIT %s
                """,
                (invoice_id, limit),
            )
            decisions = [_decision_row(row) for row in cur.fetchall()]

            cur.execute(
                """
                SELECT
                  id, entity_type, entity_id, from_state, to_state, event_type,
                  reason, actor_type, actor_id, metadata, created_at
                FROM workflow_state_history
                WHERE entity_type = 'invoice' AND entity_id = %s
                ORDER BY created_at ASC
                LIMIT %s
                """,
                (invoice_id, limit),
            )
            workflow_history = [_workflow_history_row(row) for row in cur.fetchall()]

            cur.execute(
                """
                SELECT
                  id, entity_type, entity_id, source_document_id, invoice_id,
                  queue_name, priority, status, review_reason, assigned_to,
                  due_at, resolution, metadata, created_at, updated_at, resolved_at
                FROM human_review_queue
                WHERE (entity_type = 'invoice' AND entity_id = %s)
                   OR invoice_id = %s
                ORDER BY created_at ASC
                LIMIT %s
                """,
                (invoice_id, invoice_id, limit),
            )
            review_items = [_human_review_row(row) for row in cur.fetchall()]

    review_overrides = {}
    for review_item in review_items:
        payload = _review_override_payload(review_item)
        if payload:
            review_overrides[review_item["id"]] = payload

    events: List[Dict[str, Any]] = []
    agent_names = set()
    human_touchpoint_count = 0

    for decision in decisions:
        metadata = decision.get("metadata") or {}
        review_item_id = metadata.get("review_item_id")
        override_payload = review_overrides.get(review_item_id) if review_item_id else None
        timestamp = decision.get("created_at")
        events.append(
            {
                "id": decision["id"],
                "timestamp": timestamp,
                "type": "decision",
                "display_tone": _decision_tone(decision.get("decision"), metadata),
                "agent_name": decision.get("agent_name"),
                "decision_type": decision.get("decision_type"),
                "action": decision.get("decision"),
                "confidence": decision.get("confidence"),
                "reasoning": decision.get("reasoning_summary") or "",
                "metadata": metadata,
                "review_item_id": review_item_id,
                "overridden": bool(override_payload),
                "override_reason": (override_payload or {}).get("override_reason"),
                "override_resolved_at": (override_payload or {}).get("resolved_at"),
            }
        )
        if decision.get("agent_name"):
            agent_names.add(decision["agent_name"])

    for history_item in workflow_history:
        metadata = history_item.get("metadata") or {}
        events.append(
            {
                "id": history_item["id"],
                "timestamp": history_item.get("created_at"),
                "type": "state_change",
                "display_tone": _workflow_tone(history_item.get("event_type"), metadata),
                "event_type": history_item.get("event_type"),
                "before_state": history_item.get("from_state"),
                "after_state": history_item.get("to_state"),
                "reason": history_item.get("reason"),
                "actor_type": history_item.get("actor_type"),
                "actor_id": history_item.get("actor_id"),
                "metadata": metadata,
            }
        )
        if history_item.get("actor_type") == "human":
            human_touchpoint_count += 1

    for review_item in review_items:
        if review_item.get("assigned_to"):
            human_touchpoint_count += 1
        elif review_item.get("status") in {"resolved", "dismissed"} and (
            review_item.get("resolution") or (review_item.get("metadata") or {})
        ):
            human_touchpoint_count += 1

    events.sort(
        key=lambda item: (
            _parse_iso_datetime(item.get("timestamp")) or datetime.datetime.min.replace(tzinfo=datetime.timezone.utc),
            0 if item.get("type") == "state_change" else 1,
            str(item.get("id") or ""),
        )
    )

    timestamps = [
        _parse_iso_datetime(item.get("timestamp"))
        for item in events
        if item.get("timestamp") and _event_counts_toward_processing_time(item)
    ]
    timestamps = [value for value in timestamps if value is not None]
    first_event_at = timestamps[0].isoformat() if timestamps else None
    last_event_at = timestamps[-1].isoformat() if timestamps else None
    total_processing_time_seconds = 0
    if len(timestamps) >= 2:
        total_processing_time_seconds = int((timestamps[-1] - timestamps[0]).total_seconds())

    return {
        "invoice_id": invoice_id,
        "events": events,
        "summary": {
            "agent_count": len(agent_names),
            "human_touchpoint_count": human_touchpoint_count,
            "event_count": len(events),
            "first_event_at": first_event_at,
            "last_event_at": last_event_at,
            "total_processing_time_seconds": total_processing_time_seconds,
        },
    }


def list_human_review_items(
    *,
    status: Optional[str] = None,
    active_only: bool = False,
    queue_name: Optional[str] = None,
    entity_type: Optional[str] = None,
    entity_id: Optional[str] = None,
    source_document_id: Optional[str] = None,
    invoice_id: Optional[str] = None,
    assigned_to: Optional[str] = None,
    limit: int = 100,
) -> List[Dict[str, Any]]:
    limit = _coerce_limit(limit)
    normalized_status = str(status or "").strip().lower() or None
    if normalized_status == "pending":
        normalized_status = "open"
    elif normalized_status == "in_review":
        normalized_status = "assigned"
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
                WHERE (%s::text IS NULL OR status = %s::text)
                  AND (%s::boolean = false OR status IN ('open', 'assigned'))
                  AND (%s::text IS NULL OR queue_name = %s::text)
                  AND (%s::text IS NULL OR entity_type = %s::text)
                  AND (%s::uuid IS NULL OR entity_id = %s::uuid)
                  AND (%s::uuid IS NULL OR source_document_id = %s::uuid)
                  AND (%s::uuid IS NULL OR invoice_id = %s::uuid)
                  AND (%s::text IS NULL OR assigned_to = %s::text)
                ORDER BY priority ASC, created_at DESC
                LIMIT %s
                """,
                (
                    normalized_status,
                    normalized_status,
                    active_only,
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
            return _enrich_human_review_items([_human_review_row(row) for row in cur.fetchall()])


def get_human_review_queue_counts(*, urgent_priority_threshold: int = 80) -> Dict[str, int]:
    urgent_priority_threshold = max(0, int(urgent_priority_threshold))
    with get_conn() as conn:
        with conn.cursor() as cur:
            _assert_agent_tables(cur)
            cur.execute(
                """
                SELECT
                  COUNT(*) FILTER (WHERE status = 'open') AS open_count,
                  COUNT(*) FILTER (WHERE status = 'assigned') AS in_review_count,
                  COUNT(*) FILTER (WHERE status IN ('open', 'assigned') AND priority <= %s) AS urgent_count
                FROM human_review_queue
                """,
                (urgent_priority_threshold,),
            )
            row = cur.fetchone() or (0, 0, 0)
            open_count = int(row[0] or 0)
            in_review_count = int(row[1] or 0)
            urgent_count = int(row[2] or 0)
            return {
                "pending_count": open_count + in_review_count,
                "open_count": open_count,
                "in_review_count": in_review_count,
                "urgent_count": urgent_count,
            }


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
                  id, entity_type, state_name, target_minutes, warning_minutes, breach_minutes,
                  escalation_queue, is_active, metadata, created_at, updated_at
                FROM sla_configs
                WHERE (%s::text IS NULL OR entity_type = %s::text)
                  AND (%s::boolean = false OR is_active = true)
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
                  ws.breach_risk,
                  ws.metadata,
                  ws.updated_at,
                  sc.target_minutes,
                  sc.warning_minutes,
                  sc.breach_minutes,
                  sc.escalation_queue,
                  sc.metadata,
                  ROUND((EXTRACT(EPOCH FROM (now() - ws.updated_at)) / 60.0)::numeric, 2) AS age_minutes,
                  ROUND(((EXTRACT(EPOCH FROM (now() - ws.updated_at)) / 60.0) - sc.warning_minutes)::numeric, 2) AS warning_delta_minutes,
                  ROUND(((EXTRACT(EPOCH FROM (now() - ws.updated_at)) / 60.0) - sc.breach_minutes)::numeric, 2) AS breach_delta_minutes
                FROM workflow_states AS ws
                INNER JOIN sla_configs AS sc
                  ON sc.entity_type = ws.entity_type
                 AND sc.state_name = ws.current_state
                 AND sc.is_active = true
                WHERE (%s::text IS NULL OR ws.entity_type = %s::text)
                  AND (%s::text IS NULL OR ws.current_state = %s::text)
                  AND ws.breach_risk IN ('warning', 'breaching', 'breached')
                ORDER BY breach_delta_minutes DESC, ws.updated_at ASC
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
                  confidence, breach_risk, metadata, created_at, updated_at
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
                SELECT
                  breach_risk,
                  COUNT(*)
                FROM workflow_states
                WHERE breach_risk IN ('warning', 'breaching', 'breached')
                GROUP BY breach_risk
                """
            )
            sla_risk_counts = {row[0]: int(row[1]) for row in cur.fetchall()}

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
                    "active_breach_count": (
                        sla_risk_counts.get("warning", 0)
                        + sla_risk_counts.get("breaching", 0)
                        + sla_risk_counts.get("breached", 0)
                    ),
                    "by_risk": sla_risk_counts,
                },
            }


def get_agent_operations_metrics(*, window_days: int = 30) -> Dict[str, Any]:
    window_days = max(1, min(int(window_days or 30), 365))
    window_start = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=window_days)

    with get_conn() as conn:
        with conn.cursor() as cur:
            _assert_agent_tables(cur)
            cur.execute("SELECT to_regclass('public.payment_authorization_requests')")
            payment_auth_table = cur.fetchone()
            has_payment_authorization_requests = bool(payment_auth_table and payment_auth_table[0])

            cur.execute(
                """
                SELECT id::text, created_at
                FROM invoices
                WHERE created_at >= %s
                ORDER BY created_at DESC, id DESC
                """,
                (window_start,),
            )
            invoice_rows = cur.fetchall()
            invoice_ids = [row[0] for row in invoice_rows if row and row[0]]

            review_touchpoints: Dict[str, int] = {}
            payment_touchpoints: Dict[str, int] = {}
            vendor_touchpoints: Dict[str, int] = {}
            processing_durations: List[float] = []

            if invoice_ids:
                cur.execute(
                    """
                    SELECT invoice_id::text, COUNT(*)
                    FROM human_review_queue
                    WHERE invoice_id::text = ANY(%s)
                    GROUP BY invoice_id::text
                    """,
                    (invoice_ids,),
                )
                review_touchpoints = {row[0]: int(row[1]) for row in cur.fetchall() if row and row[0]}

                cur.execute(
                    """
                    SELECT invoice_id::text, COUNT(*)
                    FROM vendor_communications
                    WHERE invoice_id::text = ANY(%s)
                      AND (
                        approved_by IS NOT NULL
                        OR status IN ('approved', 'sent', 'outbound')
                      )
                    GROUP BY invoice_id::text
                    """,
                    (invoice_ids,),
                )
                vendor_touchpoints = {row[0]: int(row[1]) for row in cur.fetchall() if row and row[0]}

                if has_payment_authorization_requests:
                    cur.execute(
                        """
                        SELECT elem.value AS invoice_id, COUNT(*)
                        FROM payment_authorization_requests AS par
                        CROSS JOIN LATERAL jsonb_array_elements_text(par.invoice_ids) AS elem(value)
                        WHERE elem.value = ANY(%s)
                          AND (par.approved_by IS NOT NULL OR par.rejected_by IS NOT NULL)
                        GROUP BY elem.value
                        """,
                        (invoice_ids,),
                    )
                    payment_touchpoints = {
                        row[0]: int(row[1]) for row in cur.fetchall() if row and row[0]
                    }

                cur.execute(
                    """
                    SELECT entity_id::text, MIN(created_at), MAX(created_at)
                    FROM (
                      SELECT entity_id, created_at
                      FROM workflow_state_history
                      WHERE entity_type = 'invoice'
                        AND entity_id::text = ANY(%s)
                        AND COALESCE(event_type, '') <> 'invoice_status_synced'
                        AND COALESCE(to_state, '') NOT IN ('payment_pending', 'paid')
                      UNION ALL
                      SELECT entity_id, created_at
                      FROM agent_decisions
                      WHERE entity_type = 'invoice' AND entity_id::text = ANY(%s)
                      UNION ALL
                      SELECT invoice_id AS entity_id, created_at
                      FROM human_review_queue
                      WHERE invoice_id::text = ANY(%s)
                    ) AS invoice_events
                    GROUP BY entity_id::text
                    """,
                    (invoice_ids, invoice_ids, invoice_ids),
                )
                for invoice_id, first_event_at, last_event_at in cur.fetchall():
                    if not invoice_id or not first_event_at or not last_event_at:
                        continue
                    processing_durations.append(
                        max(
                            0.0,
                            (last_event_at - first_event_at).total_seconds(),
                        )
                    )

            fully_automated = 0
            fast_approval = 0
            deeper_human_involvement = 0
            for invoice_id in invoice_ids:
                review_count = review_touchpoints.get(invoice_id, 0)
                payment_count = payment_touchpoints.get(invoice_id, 0)
                vendor_count = vendor_touchpoints.get(invoice_id, 0)
                total_touchpoints = review_count + payment_count + vendor_count

                if total_touchpoints == 0:
                    fully_automated += 1
                elif total_touchpoints == 1 and review_count == 0 and vendor_count == 0 and payment_count == 1:
                    fast_approval += 1
                else:
                    deeper_human_involvement += 1

            cur.execute(
                """
                SELECT
                  agent_name,
                  COUNT(*) AS decision_count,
                  AVG(confidence) AS average_confidence
                FROM agent_decisions
                WHERE created_at >= %s
                GROUP BY agent_name
                ORDER BY decision_count DESC, agent_name ASC
                """,
                (window_start,),
            )
            agent_activity = [
                {
                    "agent_name": row[0],
                    "decision_count": int(row[1]),
                    "average_confidence": float(row[2]) if row[2] is not None else None,
                }
                for row in cur.fetchall()
            ]

            cur.execute(
                """
                SELECT COALESCE(breach_risk, 'ok') AS breach_risk, COUNT(*)
                FROM workflow_states
                WHERE entity_type = 'invoice'
                  AND COALESCE(current_state, '') NOT IN ('paid', 'rejected', 'dismissed', 'canceled', 'dead_letter')
                GROUP BY COALESCE(breach_risk, 'ok')
                """
            )
            sla_health = {"ok": 0, "warning": 0, "breaching": 0, "breached": 0}
            for breach_risk, count in cur.fetchall():
                if breach_risk in sla_health:
                    sla_health[breach_risk] = int(count)

            cur.execute(
                """
                SELECT COUNT(DISTINCT entity_id::text)
                FROM agent_decisions
                WHERE entity_type = 'invoice'
                  AND agent_name = 'exception_resolution_agent'
                  AND decision = 'resolved'
                  AND created_at >= %s
                """,
                (window_start,),
            )
            auto_resolved_row = cur.fetchone()
            auto_resolved = int(auto_resolved_row[0]) if auto_resolved_row else 0

            cur.execute(
                """
                SELECT COUNT(DISTINCT invoice_id::text)
                FROM human_review_queue
                WHERE invoice_id IS NOT NULL
                  AND queue_name = 'po_matching'
                  AND created_at >= %s
                """,
                (window_start,),
            )
            escalated_row = cur.fetchone()
            escalated = int(escalated_row[0]) if escalated_row else 0

            total_invoices_processed = len(invoice_ids)
            automation_rate_percent = (
                round((fully_automated / total_invoices_processed) * 100.0, 2)
                if total_invoices_processed
                else 0.0
            )
            average_processing_time_seconds = (
                round(sum(processing_durations) / len(processing_durations), 2)
                if processing_durations
                else 0.0
            )

            return {
                "window_days": window_days,
                "generated_at": _iso(datetime.datetime.now(datetime.timezone.utc)),
                "totals": {
                    "total_invoices_processed": total_invoices_processed,
                    "fully_automated": fully_automated,
                    "fast_approval": fast_approval,
                    "deeper_human_involvement": deeper_human_involvement,
                },
                "automation_rate_percent": automation_rate_percent,
                "average_processing_time_seconds": average_processing_time_seconds,
                "agent_activity": agent_activity,
                "sla_health": sla_health,
                "exceptions": {
                    "auto_resolved": auto_resolved,
                    "escalated": escalated,
                },
            }
