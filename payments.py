import os
import json
import hashlib
from typing import List, Dict, Any, Optional
from dotenv import load_dotenv
from db import get_conn
import stripe
from realtime_events import publish_live_update
from user_facing_errors import UserFacingError

load_dotenv()

STRIPE_SECRET_KEY = os.getenv("STRIPE_SECRET_KEY")

if STRIPE_SECRET_KEY:
    stripe.api_key = STRIPE_SECRET_KEY


def _set_invoice_workflow_state_best_effort(
    invoice_id: str,
    state: str,
    *,
    reason: str,
    metadata: Optional[Dict[str, Any]] = None,
) -> None:
    try:
        from agent_db import set_workflow_state

        set_workflow_state(
            "invoice",
            invoice_id,
            state,
            current_stage="payments",
            confidence=1.0,
            event_type="invoice_status_synced",
            reason=reason,
            metadata=metadata or {},
        )
    except Exception:
        return None


def _json_value(value: Any, default: Any) -> Any:
    if value is None:
        return default
    if isinstance(value, str):
        try:
            return json.loads(value)
        except Exception:
            return default
    return value


def _assert_payment_tables(cur) -> None:
    """Ensure required payment tables exist; raise with guidance if missing."""
    cur.execute("SELECT to_regclass('public.payments'), to_regclass('public.payment_invoices')")
    row = cur.fetchone()
    if not row or row[0] is None or row[1] is None:
        raise RuntimeError(
            "Payments tables not found. Run migration: migrations/2025-11-09_add_payments_tables.sql"
        )


def _minor_units(amount: float, currency: str) -> int:
    # Assumes 2-decimal currencies for now
    return int(round(float(amount) * 100))


def _payment_intent_idempotency_key(
    invoice_ids: List[str],
    *,
    email: str,
    total: float,
    currency: str,
    save_method: bool,
) -> str:
    key_parts = [
        ",".join(sorted(str(invoice_id) for invoice_id in invoice_ids)),
        (email or "").strip().lower(),
        f"{float(total):.2f}",
        (currency or "").strip().lower(),
        "save_method=1" if save_method else "save_method=0",
    ]
    return hashlib.sha256("|".join(key_parts).encode()).hexdigest()


def _get_linked_invoice_ids(cur, payment_id: str) -> set[str]:
    cur.execute("SELECT invoice_id FROM payment_invoices WHERE payment_id=%s", (payment_id,))
    return {str(row[0]) for row in cur.fetchall()}


def _normalize_text(value: Any) -> str:
    return str(value or "").strip().lower()


def _invoice_signature(row: tuple) -> Optional[str]:
    rid, amount, curr, status, vendor_id, invoice_number, invoice_date, supplier_name = row
    if not invoice_number or amount is None:
        return None
    vendor_key = str(vendor_id) if vendor_id else _normalize_text(supplier_name)
    if not vendor_key:
        return None
    return "|".join(
        [
            vendor_key,
            _normalize_text(invoice_number),
            f"{float(amount):.2f}",
            invoice_date.isoformat() if invoice_date else "",
        ]
    )


def _assert_no_duplicate_payment_conflicts(cur, rows: List[tuple], requested_invoice_ids: set[str]) -> None:
    signatures: Dict[str, str] = {}
    for row in rows:
        signature = _invoice_signature(row)
        if not signature:
            continue
        invoice_id = str(row[0])
        existing = signatures.get(signature)
        if existing and existing != invoice_id:
            raise UserFacingError(
                "A duplicate invoice was detected in this payment request. Remove the duplicate and try again.",
                code="duplicate_payment_blocked",
                status_code=409,
            )
        signatures[signature] = invoice_id

    for row in rows:
        signature = _invoice_signature(row)
        if not signature:
            continue
        rid, amount, curr, status, vendor_id, invoice_number, invoice_date, supplier_name = row
        cur.execute(
            """
            SELECT id, status
            FROM invoices
            WHERE id <> %s
              AND lower(coalesce(invoice_number, '')) = lower(%s)
              AND abs(coalesce(total_amount, 0) - %s) <= 0.01
              AND (
                (%s IS NOT NULL AND vendor_id = %s)
                OR (
                    %s IS NULL
                    AND vendor_id IS NULL
                    AND lower(coalesce(supplier_name, '')) = lower(%s)
                )
              )
              AND (
                (%s IS NULL AND invoice_date IS NULL)
                OR invoice_date = %s
              )
            LIMIT 10
            """,
            (
                rid,
                invoice_number,
                float(amount),
                vendor_id,
                vendor_id,
                vendor_id,
                supplier_name,
                invoice_date,
                invoice_date,
            ),
        )
        for duplicate_invoice_id, duplicate_status in cur.fetchall():
            if str(duplicate_invoice_id) in requested_invoice_ids:
                continue
            if duplicate_status in {"paid", "payment_pending"}:
                raise UserFacingError(
                    "A duplicate version of this invoice has already been paid or is already in payment. This payment was blocked to prevent a double charge.",
                    code="duplicate_payment_blocked",
                    status_code=409,
                )


def create_payment_intent_for_invoices(
    invoice_ids: List[str],
    customer: Dict[str, Any],
    currency: Optional[str] = None,
    save_method: bool = False,
) -> Dict[str, Any]:
    if not STRIPE_SECRET_KEY:
        raise RuntimeError("STRIPE_SECRET_KEY not configured")
    if not invoice_ids:
        raise ValueError("invoiceIds required")

    email = (customer.get("email") or "").strip()
    name = (customer.get("name") or "").strip()
    address = customer.get("address") or {}

    with get_conn() as conn:
        with conn.cursor() as cur:
            _assert_payment_tables(cur)

            # Lock invoices and validate
            cur.execute(
                """
                SELECT id, total_amount, currency, status, vendor_id, invoice_number, invoice_date, supplier_name
                FROM invoices
                WHERE id = ANY(%s)
                FOR UPDATE
                """,
                (invoice_ids,)
            )
            rows = cur.fetchall()
            if len(rows) != len(invoice_ids):
                raise ValueError("Some invoices not found")
            requested_invoice_ids = {str(row[0]) for row in rows}

            total = 0.0
            currencies = set()
            allowed_status = {"matched_auto", "ready_for_payment"}
            _assert_no_duplicate_payment_conflicts(cur, rows, requested_invoice_ids)
            for rid, amount, curr, status, vendor_id, invoice_number, invoice_date, supplier_name in rows:
                if status == "paid":
                    raise ValueError("One or more invoices are already paid")
                if amount is None:
                    raise ValueError("Invoice missing total amount")
                total += float(amount)
                if curr:
                    currencies.add(str(curr))

            if currency:
                currencies.add(currency)
            if len(currencies) > 1:
                raise ValueError("Mixed currency selection is not allowed")
            final_currency = (list(currencies)[0] if currencies else (currency or "USD")).lower()

            # Create or retrieve the PaymentIntent first. Stripe may return the same
            # object for identical idempotent retries, and the local DB should reuse
            # the existing payment row in that case.
            idemp_key = _payment_intent_idempotency_key(
                invoice_ids,
                email=email,
                total=total,
                currency=final_currency,
                save_method=save_method,
            )
            intent = stripe.PaymentIntent.create(
                amount=_minor_units(total, final_currency),
                currency=final_currency,
                metadata={
                    "invoice_ids": ",".join(invoice_ids),
                    "customer_email": email or "",
                },
                receipt_email=email or None,
                setup_future_usage=("off_session" if save_method else None),
                automatic_payment_methods={"enabled": True},
                idempotency_key=idemp_key,
            )

            # Insert or reuse the payment record for this Stripe intent.
            cur.execute(
                """
                INSERT INTO payments(stripe_payment_intent_id, amount, currency, customer_email, status)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (stripe_payment_intent_id)
                DO UPDATE SET
                    amount = EXCLUDED.amount,
                    currency = EXCLUDED.currency,
                    customer_email = EXCLUDED.customer_email,
                    status = EXCLUDED.status
                RETURNING id
                """,
                (intent.id, total, final_currency, email or None, "requires_confirmation"),
            )
            payment_id = cur.fetchone()[0]

            existing_linked_ids = _get_linked_invoice_ids(cur, payment_id)
            requested_invoice_ids = {str(rid) for rid, *_ in rows}
            if not existing_linked_ids.issubset(requested_invoice_ids):
                raise RuntimeError("Existing payment links do not match the requested invoice selection")

            for rid, amount, curr, status, vendor_id, invoice_number, invoice_date, supplier_name in rows:
                rid_str = str(rid)
                if status == "payment_pending":
                    if rid_str not in existing_linked_ids:
                        raise ValueError("One or more invoices are already pending under another payment")
                    continue
                if status not in allowed_status:
                    raise ValueError("One or more invoices are not eligible for payment")

            # Insert any missing link rows, and set eligible invoices to payment_pending.
            for rid, amount, curr, status, vendor_id, invoice_number, invoice_date, supplier_name in rows:
                if str(rid) in existing_linked_ids:
                    continue
                cur.execute(
                    """
                    INSERT INTO payment_invoices(payment_id, invoice_id, amount_applied, previous_status)
                    VALUES (%s, %s, %s, %s)
                    """,
                    (payment_id, rid, amount, status),
                )
            # Update invoices only if still in allowed statuses
            cur.execute(
                """
                UPDATE invoices
                SET status = 'payment_pending'
                WHERE id = ANY(%s) AND status IN ('matched_auto','ready_for_payment')
                """,
                (invoice_ids,),
            )
            for invoice_id in invoice_ids:
                _set_invoice_workflow_state_best_effort(
                    str(invoice_id),
                    "payment_pending",
                    reason="Payment intent was created for this invoice batch.",
                    metadata={
                        "payment_intent_id": intent.id,
                        "payment_id": str(payment_id),
                    },
                )

            result = {
                "paymentId": str(payment_id),
                "clientSecret": intent.client_secret,
                "paymentIntentId": intent.id,
                "amount": total,
                "currency": final_currency,
                "invoiceIds": invoice_ids,
            }
            publish_live_update(
                "payment.intent_created",
                {
                    "paymentId": str(payment_id),
                    "paymentIntentId": intent.id,
                    "invoiceIds": invoice_ids,
                    "amount": total,
                    "currency": final_currency,
                },
            )
            return result


def list_pending_payment_confirmations(limit: int = 25) -> List[Dict[str, Any]]:
    limit = max(1, min(int(limit), 100))
    with get_conn() as conn:
        with conn.cursor() as cur:
            _assert_payment_tables(cur)
            cur.execute("SELECT to_regclass('public.payment_authorization_requests')")
            payment_authorization_table = cur.fetchone()
            if not payment_authorization_table or payment_authorization_table[0] is None:
                return []

            cur.execute(
                """
                SELECT
                  par.id,
                  par.invoice_ids,
                  par.customer,
                  par.currency,
                  par.total_amount,
                  par.invoice_count,
                  par.metadata,
                  par.executed_payment_intent_id,
                  p.status
                FROM payment_authorization_requests AS par
                INNER JOIN payments AS p
                  ON p.stripe_payment_intent_id = par.executed_payment_intent_id
                WHERE par.approval_status = 'payment_intent_created'
                  AND p.status = 'requires_confirmation'
                ORDER BY p.created_at DESC, par.created_at DESC
                LIMIT %s
                """,
                (limit,),
            )

            confirmations: List[Dict[str, Any]] = []
            for row in cur.fetchall():
                metadata = _json_value(row[6], {})
                payment_result = metadata.get("execution_result") or {}
                customer = _json_value(row[2], {})
                invoice_ids = _json_value(row[1], [])
                invoice_summaries = metadata.get("invoice_summaries") or []
                vendor_names = list(
                    dict.fromkeys(
                        [
                            str(invoice.get("supplier_name") or invoice.get("vendor_name") or "").strip()
                            for invoice in invoice_summaries
                            if str(invoice.get("supplier_name") or invoice.get("vendor_name") or "").strip()
                        ]
                    )
                )
                primary_vendor_name = (
                    vendor_names[0]
                    if len(vendor_names) == 1
                    else f"{len(vendor_names)} vendors" if vendor_names else "Unknown Vendor"
                )

                confirmations.append(
                    {
                        "authorizationRequestId": str(row[0]),
                        "invoiceIds": invoice_ids,
                        "customer": customer,
                        "currency": row[3] or payment_result.get("currency") or "USD",
                        "totalAmount": float(row[4]) if row[4] is not None else None,
                        "invoiceCount": int(row[5] or len(invoice_ids)),
                        "paymentResult": payment_result,
                        "paymentIntentId": row[7] or payment_result.get("paymentIntentId"),
                        "clientSecret": payment_result.get("clientSecret"),
                        "paymentStatus": row[8] or "requires_confirmation",
                        "vendorNames": vendor_names,
                        "primaryVendorName": primary_vendor_name,
                    }
                )

            return confirmations


def list_payment_history(
    limit: int = 25,
    *,
    vendor_id: Optional[str] = None,
    currency: Optional[str] = None,
) -> List[Dict[str, Any]]:
    limit = max(1, min(int(limit), 100))
    vendor_id = str(vendor_id).strip() if vendor_id else None
    currency = str(currency).strip() if currency else None

    with get_conn() as conn:
        with conn.cursor() as cur:
            _assert_payment_tables(cur)
            cur.execute(
                """
                SELECT
                  p.id,
                  p.stripe_payment_intent_id,
                  p.amount,
                  p.currency,
                  p.customer_email,
                  p.status,
                  p.created_at,
                  COUNT(DISTINCT pi.invoice_id) AS invoice_count,
                  COALESCE(array_remove(array_agg(DISTINCT COALESCE(v.name, i.supplier_name)), NULL), ARRAY[]::text[]) AS vendor_names,
                  COALESCE(array_remove(array_agg(DISTINCT i.invoice_number), NULL), ARRAY[]::text[]) AS invoice_numbers,
                  COALESCE(array_remove(array_agg(DISTINCT pi.invoice_id::text), NULL), ARRAY[]::text[]) AS invoice_ids
                FROM payments AS p
                LEFT JOIN payment_invoices AS pi
                  ON pi.payment_id = p.id
                LEFT JOIN invoices AS i
                  ON i.id = pi.invoice_id
                LEFT JOIN vendors AS v
                  ON v.id = i.vendor_id
                WHERE (%s::text IS NULL OR lower(coalesce(p.currency, '')) = lower(%s::text))
                  AND (
                    %s::uuid IS NULL
                    OR EXISTS (
                      SELECT 1
                      FROM payment_invoices AS pi_filter
                      INNER JOIN invoices AS i_filter
                        ON i_filter.id = pi_filter.invoice_id
                      WHERE pi_filter.payment_id = p.id
                        AND i_filter.vendor_id = %s::uuid
                    )
                  )
                GROUP BY
                  p.id,
                  p.stripe_payment_intent_id,
                  p.amount,
                  p.currency,
                  p.customer_email,
                  p.status,
                  p.created_at
                ORDER BY p.created_at DESC
                LIMIT %s
                """,
                (currency, currency, vendor_id, vendor_id, limit),
            )

            history: List[Dict[str, Any]] = []
            for row in cur.fetchall():
                vendor_names = [str(name).strip() for name in (row[8] or []) if str(name or "").strip()]
                invoice_numbers = [str(number).strip() for number in (row[9] or []) if str(number or "").strip()]
                invoice_ids = [str(invoice_id).strip() for invoice_id in (row[10] or []) if str(invoice_id or "").strip()]
                primary_vendor_name = (
                    vendor_names[0]
                    if len(vendor_names) == 1
                    else f"{len(vendor_names)} vendors" if vendor_names else "Unknown Vendor"
                )
                history.append(
                    {
                        "paymentId": str(row[0]),
                        "paymentIntentId": row[1],
                        "amount": float(row[2]) if row[2] is not None else None,
                        "currency": row[3] or "USD",
                        "customerEmail": row[4],
                        "status": row[5] or "unknown",
                        "createdAt": row[6].isoformat() if row[6] is not None else None,
                        "invoiceCount": int(row[7] or len(invoice_ids)),
                        "vendorNames": vendor_names,
                        "primaryVendorName": primary_vendor_name,
                        "invoiceNumbers": invoice_numbers,
                        "invoiceIds": invoice_ids,
                    }
                )

            return history


def mark_payment_succeeded(payment_intent_id: str) -> None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            _assert_payment_tables(cur)
            cur.execute("SELECT id FROM payments WHERE stripe_payment_intent_id=%s LIMIT 1", (payment_intent_id,))
            row = cur.fetchone()
            if not row:
                return
            payment_id = row[0]
            # Update invoices to paid
            cur.execute("SELECT invoice_id FROM payment_invoices WHERE payment_id=%s", (payment_id,))
            ids = [r[0] for r in cur.fetchall()]
            if ids:
                cur.execute("UPDATE invoices SET status='paid' WHERE id = ANY(%s)", (ids,))
            cur.execute("UPDATE payments SET status='succeeded' WHERE id=%s", (payment_id,))
            for invoice_id in ids:
                _set_invoice_workflow_state_best_effort(
                    str(invoice_id),
                    "paid",
                    reason="Stripe confirmed that the payment succeeded.",
                    metadata={"payment_intent_id": payment_intent_id},
                )
            publish_live_update(
                "payment.succeeded",
                {
                    "paymentIntentId": payment_intent_id,
                    "invoiceIds": [str(invoice_id) for invoice_id in ids],
                },
            )


def mark_payment_failed_or_canceled(payment_intent_id: str) -> None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            _assert_payment_tables(cur)
            cur.execute("SELECT id FROM payments WHERE stripe_payment_intent_id=%s LIMIT 1", (payment_intent_id,))
            row = cur.fetchone()
            if not row:
                return
            payment_id = row[0]
            # Revert invoice statuses
            cur.execute("SELECT invoice_id, previous_status FROM payment_invoices WHERE payment_id=%s", (payment_id,))
            reverted_ids = []
            for inv_id, prev in cur.fetchall():
                cur.execute("UPDATE invoices SET status=%s WHERE id=%s", (prev or 'ready_for_payment', inv_id))
                reverted_ids.append(str(inv_id))
                _set_invoice_workflow_state_best_effort(
                    str(inv_id),
                    prev or "ready_for_payment",
                    reason="Payment intent was canceled or failed, so the invoice returned to its prior state.",
                    metadata={"payment_intent_id": payment_intent_id},
                )
            cur.execute("UPDATE payments SET status='failed' WHERE id=%s", (payment_id,))
            publish_live_update(
                "payment.reverted",
                {
                    "paymentIntentId": payment_intent_id,
                    "invoiceIds": reverted_ids,
                },
            )


def confirm_payment_intent(payment_intent_id: str) -> Dict[str, Any]:
    """Verify with Stripe and update DB to mark invoices paid if succeeded.
       Returns a summary dict with status and affected invoice ids.
    """
    if not STRIPE_SECRET_KEY:
        raise RuntimeError("STRIPE_SECRET_KEY not configured")
    # Retrieve from Stripe to verify status
    intent = stripe.PaymentIntent.retrieve(payment_intent_id)
    status = intent.get("status") if isinstance(intent, dict) else getattr(intent, 'status', None)
    meta = intent.get("metadata", {}) if isinstance(intent, dict) else getattr(intent, 'metadata', {})
    invoice_ids = []
    if isinstance(meta, dict) and meta.get("invoice_ids"):
        invoice_ids = [s for s in str(meta.get("invoice_ids")).split(',') if s]

    if status == "succeeded":
        mark_payment_succeeded(payment_intent_id)
        return {"status": "succeeded", "invoiceIds": invoice_ids}
    elif status in ("canceled", "requires_payment_method"):
        # Revert locks if any were set during intent creation
        mark_payment_failed_or_canceled(payment_intent_id)
        return {"status": status, "invoiceIds": invoice_ids}
    else:
        # Pending or requires_action; do nothing
        return {"status": status or "unknown", "invoiceIds": invoice_ids}
