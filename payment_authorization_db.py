import json
from typing import Any, Dict, List, Optional

from psycopg.types.json import Jsonb

from db import get_conn

_PAYMENT_AUTH_MIGRATION = "migrations/2026-03-27_add_payment_authorization_requests.sql"


def _assert_payment_authorization_table(cur) -> None:
    cur.execute("SELECT to_regclass('public.payment_authorization_requests')")
    row = cur.fetchone()
    if not row or row[0] is None:
        raise RuntimeError(
            "Payment authorization table not found. "
            f"Run migration: {_PAYMENT_AUTH_MIGRATION}"
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


def _row(row: Any) -> Dict[str, Any]:
    return {
        "id": str(row[0]),
        "review_item_id": str(row[1]) if row[1] else None,
        "approval_status": row[2],
        "invoice_ids": _json_value(row[3], []),
        "customer": _json_value(row[4], {}),
        "currency": row[5],
        "save_method": bool(row[6]),
        "total_amount": float(row[7]) if row[7] is not None else None,
        "invoice_count": int(row[8]),
        "risk_level": row[9],
        "recommendation": row[10],
        "risk_reasons": _json_value(row[11], []),
        "metadata": _json_value(row[12], {}),
        "approved_by": row[13],
        "approved_at": _iso(row[14]),
        "rejected_by": row[15],
        "rejected_at": _iso(row[16]),
        "executed_payment_id": row[17],
        "executed_payment_intent_id": row[18],
        "created_at": _iso(row[19]),
        "updated_at": _iso(row[20]),
    }


def create_payment_authorization_request(
    *,
    invoice_ids: List[str],
    customer: Dict[str, Any],
    currency: Optional[str],
    save_method: bool,
    total_amount: Optional[float],
    invoice_count: int,
    risk_level: str,
    recommendation: str,
    risk_reasons: List[str],
    metadata: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    with get_conn() as conn:
        with conn.cursor() as cur:
            _assert_payment_authorization_table(cur)
            cur.execute(
                """
                INSERT INTO payment_authorization_requests(
                  invoice_ids, customer, currency, save_method, total_amount,
                  invoice_count, risk_level, recommendation, risk_reasons, metadata
                )
                VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING
                  id, review_item_id, approval_status, invoice_ids, customer, currency,
                  save_method, total_amount, invoice_count, risk_level, recommendation,
                  risk_reasons, metadata, approved_by, approved_at, rejected_by,
                  rejected_at, executed_payment_id, executed_payment_intent_id,
                  created_at, updated_at
                """,
                (
                    _jsonb(invoice_ids, []),
                    _jsonb(customer, {}),
                    currency,
                    bool(save_method),
                    total_amount,
                    int(invoice_count),
                    risk_level,
                    recommendation,
                    _jsonb(risk_reasons, []),
                    _jsonb(metadata, {}),
                ),
            )
            return _row(cur.fetchone())


def get_payment_authorization_request(request_id: str) -> Optional[Dict[str, Any]]:
    with get_conn() as conn:
        with conn.cursor() as cur:
            _assert_payment_authorization_table(cur)
            cur.execute(
                """
                SELECT
                  id, review_item_id, approval_status, invoice_ids, customer, currency,
                  save_method, total_amount, invoice_count, risk_level, recommendation,
                  risk_reasons, metadata, approved_by, approved_at, rejected_by,
                  rejected_at, executed_payment_id, executed_payment_intent_id,
                  created_at, updated_at
                FROM payment_authorization_requests
                WHERE id = %s
                """,
                (request_id,),
            )
            row = cur.fetchone()
            return _row(row) if row else None


def list_payment_authorization_requests(
    *,
    approval_status: Optional[str] = None,
    risk_level: Optional[str] = None,
    limit: int = 100,
) -> List[Dict[str, Any]]:
    limit = max(1, min(int(limit), 500))
    with get_conn() as conn:
        with conn.cursor() as cur:
            _assert_payment_authorization_table(cur)
            cur.execute(
                """
                SELECT
                  id, review_item_id, approval_status, invoice_ids, customer, currency,
                  save_method, total_amount, invoice_count, risk_level, recommendation,
                  risk_reasons, metadata, approved_by, approved_at, rejected_by,
                  rejected_at, executed_payment_id, executed_payment_intent_id,
                  created_at, updated_at
                FROM payment_authorization_requests
                WHERE (%s IS NULL OR approval_status = %s)
                  AND (%s IS NULL OR risk_level = %s)
                ORDER BY created_at DESC
                LIMIT %s
                """,
                (
                    approval_status,
                    approval_status,
                    risk_level,
                    risk_level,
                    limit,
                ),
            )
            return [_row(record) for record in cur.fetchall()]


def update_payment_authorization_request(
    request_id: str,
    *,
    review_item_id: Optional[str] = None,
    approval_status: Optional[str] = None,
    metadata: Optional[Dict[str, Any]] = None,
    approved_by: Optional[str] = None,
    approved_at: Optional[Any] = None,
    rejected_by: Optional[str] = None,
    rejected_at: Optional[Any] = None,
    executed_payment_id: Optional[str] = None,
    executed_payment_intent_id: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    with get_conn() as conn:
        with conn.cursor() as cur:
            _assert_payment_authorization_table(cur)
            cur.execute(
                """
                UPDATE payment_authorization_requests
                SET
                  review_item_id = COALESCE(%s, review_item_id),
                  approval_status = COALESCE(%s, approval_status),
                  metadata = CASE
                    WHEN %s THEN metadata
                    ELSE COALESCE(metadata, '{}'::jsonb) || %s
                  END,
                  approved_by = COALESCE(%s, approved_by),
                  approved_at = COALESCE(%s, approved_at),
                  rejected_by = COALESCE(%s, rejected_by),
                  rejected_at = COALESCE(%s, rejected_at),
                  executed_payment_id = COALESCE(%s, executed_payment_id),
                  executed_payment_intent_id = COALESCE(%s, executed_payment_intent_id),
                  updated_at = now()
                WHERE id = %s
                RETURNING
                  id, review_item_id, approval_status, invoice_ids, customer, currency,
                  save_method, total_amount, invoice_count, risk_level, recommendation,
                  risk_reasons, metadata, approved_by, approved_at, rejected_by,
                  rejected_at, executed_payment_id, executed_payment_intent_id,
                  created_at, updated_at
                """,
                (
                    review_item_id,
                    approval_status,
                    metadata is None,
                    _jsonb(metadata, {}),
                    approved_by,
                    approved_at,
                    rejected_by,
                    rejected_at,
                    executed_payment_id,
                    executed_payment_intent_id,
                    request_id,
                ),
            )
            row = cur.fetchone()
            return _row(row) if row else None
