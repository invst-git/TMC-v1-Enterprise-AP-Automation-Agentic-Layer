import datetime
import os
from typing import Any, Dict, List, Optional

from db import get_conn


def _get_dependencies():
    from agent_db import (
        create_completed_agent_task,
        create_human_review_item,
        record_agent_decision,
        set_workflow_state,
        update_human_review_item,
    )
    from payment_authorization_db import (
        create_payment_authorization_request,
        get_payment_authorization_request,
        list_payment_authorization_requests,
        update_payment_authorization_request,
    )
    from payments import create_payment_intent_for_invoices

    return {
        "create_completed_agent_task": create_completed_agent_task,
        "create_human_review_item": create_human_review_item,
        "record_agent_decision": record_agent_decision,
        "set_workflow_state": set_workflow_state,
        "update_human_review_item": update_human_review_item,
        "create_payment_authorization_request": create_payment_authorization_request,
        "get_payment_authorization_request": get_payment_authorization_request,
        "list_payment_authorization_requests": list_payment_authorization_requests,
        "update_payment_authorization_request": update_payment_authorization_request,
        "create_payment_intent_for_invoices": create_payment_intent_for_invoices,
    }


def _risk_threshold(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except Exception:
        return default


def _invoice_count_threshold(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except Exception:
        return default


def _load_batch_invoices(invoice_ids: List[str]) -> List[Dict[str, Any]]:
    if not invoice_ids:
        raise ValueError("invoiceIds required")

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                  i.id,
                  i.invoice_number,
                  i.total_amount,
                  i.currency,
                  i.status,
                  i.vendor_id,
                  i.due_date,
                  i.supplier_name
                FROM invoices AS i
                WHERE i.id = ANY(%s)
                ORDER BY i.created_at DESC NULLS LAST, i.id DESC
                """,
                (invoice_ids,),
            )
            rows = cur.fetchall()

    if len(rows) != len(invoice_ids):
        raise ValueError("Some invoices not found")

    invoices = []
    for row in rows:
        invoices.append(
            {
                "id": str(row[0]),
                "invoice_number": row[1] or "",
                "total_amount": float(row[2]) if row[2] is not None else None,
                "currency": row[3] or "USD",
                "status": row[4] or "",
                "vendor_id": str(row[5]) if row[5] else None,
                "due_date": row[6].isoformat() if row[6] else None,
                "supplier_name": row[7] or "",
            }
        )
    return invoices


def _analyze_payment_batch(invoices: List[Dict[str, Any]], currency: Optional[str]) -> Dict[str, Any]:
    allowed_statuses = {"matched_auto", "ready_for_payment"}
    currencies = {invoice["currency"] for invoice in invoices if invoice.get("currency")}
    if currency:
        currencies.add(currency)
    if len(currencies) > 1:
        raise ValueError("Mixed currency selection is not allowed")

    total_amount = 0.0
    vendor_ids = set()
    risk_reasons: List[str] = []

    for invoice in invoices:
        if invoice["status"] not in allowed_statuses:
            raise ValueError("One or more invoices are not eligible for payment authorization")
        if invoice["total_amount"] is None:
            raise ValueError("One or more invoices are missing total amount")
        total_amount += float(invoice["total_amount"])
        if invoice.get("vendor_id"):
            vendor_ids.add(invoice["vendor_id"])
        else:
            risk_reasons.append("missing_vendor_reference")
        if not invoice.get("due_date"):
            risk_reasons.append("missing_due_date")

    if len(vendor_ids) > 1:
        risk_reasons.append("multi_vendor_batch")

    high_amount_threshold = _risk_threshold("PAYMENT_AUTH_HIGH_RISK_TOTAL", 50000.0)
    medium_amount_threshold = _risk_threshold("PAYMENT_AUTH_MEDIUM_RISK_TOTAL", 10000.0)
    high_count_threshold = _invoice_count_threshold("PAYMENT_AUTH_HIGH_RISK_INVOICE_COUNT", 10)
    medium_count_threshold = _invoice_count_threshold("PAYMENT_AUTH_MEDIUM_RISK_INVOICE_COUNT", 4)

    risk_level = "low"
    if total_amount >= high_amount_threshold or len(invoices) >= high_count_threshold:
        risk_level = "high"
    elif total_amount >= medium_amount_threshold or len(invoices) >= medium_count_threshold:
        risk_level = "medium"

    if "multi_vendor_batch" in risk_reasons:
        risk_level = "high"
    elif risk_reasons and risk_level == "low":
        risk_level = "medium"

    confidence = 0.88 if risk_level == "low" else 0.74 if risk_level == "medium" else 0.59
    recommendation = "approval_required"

    return {
        "invoice_count": len(invoices),
        "currency": next(iter(currencies), currency or "USD"),
        "total_amount": round(total_amount, 2),
        "vendor_count": len(vendor_ids),
        "risk_level": risk_level,
        "risk_reasons": sorted(set(risk_reasons)),
        "recommendation": recommendation,
        "confidence": confidence,
    }


def request_payment_authorization(
    invoice_ids: List[str],
    customer: Dict[str, Any],
    *,
    currency: Optional[str] = None,
    save_method: bool = False,
    requested_by: Optional[str] = None,
) -> Dict[str, Any]:
    dependencies = _get_dependencies()
    invoices = _load_batch_invoices(invoice_ids)
    analysis = _analyze_payment_batch(invoices, currency)
    request_record = dependencies["create_payment_authorization_request"](
        invoice_ids=invoice_ids,
        customer=customer or {},
        currency=analysis["currency"],
        save_method=bool(save_method),
        total_amount=analysis["total_amount"],
        invoice_count=analysis["invoice_count"],
        risk_level=analysis["risk_level"],
        recommendation=analysis["recommendation"],
        risk_reasons=analysis["risk_reasons"],
        metadata={
            "requested_by": requested_by,
            "invoice_summaries": invoices,
        },
    )
    task = dependencies["create_completed_agent_task"](
        task_type="payment.authorize",
        entity_type="payment_authorization",
        entity_id=request_record["id"],
        priority=90,
        payload={
            "invoice_ids": invoice_ids,
            "currency": analysis["currency"],
            "save_method": bool(save_method),
        },
        result=analysis,
    )
    dependencies["record_agent_decision"](
        task_id=task["id"],
        entity_type="payment_authorization",
        entity_id=request_record["id"],
        agent_name="payment_authorization_agent",
        model_name="deterministic_risk_rules",
        prompt_version="v1",
        decision_type="payment_authorization",
        decision=analysis["recommendation"],
        confidence=analysis["confidence"],
        reasoning_summary=(
            "Payment batch was evaluated with deterministic eligibility and risk rules; "
            "execution requires explicit approval."
        ),
        metadata={
            "risk_level": analysis["risk_level"],
            "risk_reasons": analysis["risk_reasons"],
            "requested_by": requested_by,
        },
    )
    dependencies["set_workflow_state"](
        "payment_authorization",
        request_record["id"],
        "pending_approval",
        current_stage="payments",
        confidence=analysis["confidence"],
        reason="Payment batch requires human approval before Stripe intent creation",
        metadata={
            "risk_level": analysis["risk_level"],
            "invoice_count": analysis["invoice_count"],
        },
    )
    review_item = dependencies["create_human_review_item"](
        entity_type="payment_authorization",
        entity_id=request_record["id"],
        queue_name="payment_authorization",
        priority=90 if analysis["risk_level"] == "high" else 70,
        review_reason="approval_required",
        metadata={
            "invoice_ids": invoice_ids,
            "risk_level": analysis["risk_level"],
            "risk_reasons": analysis["risk_reasons"],
            "requested_by": requested_by,
        },
    )
    request_record = dependencies["update_payment_authorization_request"](
        request_record["id"],
        review_item_id=review_item["id"],
        metadata={"review_item_id": review_item["id"]},
    ) or request_record
    return {
        "authorization_request": request_record,
        "review_item": review_item,
        "analysis": analysis,
        "task_id": task["id"],
    }


def list_payment_authorizations(
    *,
    approval_status: Optional[str] = None,
    risk_level: Optional[str] = None,
    limit: int = 100,
) -> List[Dict[str, Any]]:
    return _get_dependencies()["list_payment_authorization_requests"](
        approval_status=approval_status,
        risk_level=risk_level,
        limit=limit,
    )


def get_payment_authorization(request_id: str) -> Optional[Dict[str, Any]]:
    return _get_dependencies()["get_payment_authorization_request"](request_id)


def approve_payment_authorization(request_id: str, *, approved_by: str) -> Dict[str, Any]:
    dependencies = _get_dependencies()
    request_record = dependencies["get_payment_authorization_request"](request_id)
    if not request_record:
        raise ValueError("Payment authorization request not found")
    if request_record["approval_status"] != "pending_approval":
        raise ValueError("Only pending payment authorization requests can be approved")

    approved_at = datetime.datetime.now(datetime.timezone.utc)
    request_record = dependencies["update_payment_authorization_request"](
        request_id,
        approval_status="approved",
        approved_by=approved_by,
        approved_at=approved_at,
        metadata={"approved_by": approved_by},
    ) or request_record
    dependencies["set_workflow_state"](
        "payment_authorization",
        request_id,
        "approved",
        current_stage="payments",
        confidence=1.0,
        actor_type="human",
        actor_id=approved_by,
        reason="Payment authorization approved",
        metadata={"approved_by": approved_by},
    )
    if request_record.get("review_item_id"):
        dependencies["update_human_review_item"](
            request_record["review_item_id"],
            status="resolved",
            resolution="approved",
            metadata={"approved_by": approved_by},
        )
    return request_record


def reject_payment_authorization(request_id: str, *, rejected_by: str) -> Dict[str, Any]:
    dependencies = _get_dependencies()
    request_record = dependencies["get_payment_authorization_request"](request_id)
    if not request_record:
        raise ValueError("Payment authorization request not found")
    if request_record["approval_status"] != "pending_approval":
        raise ValueError("Only pending payment authorization requests can be rejected")

    rejected_at = datetime.datetime.now(datetime.timezone.utc)
    request_record = dependencies["update_payment_authorization_request"](
        request_id,
        approval_status="rejected",
        rejected_by=rejected_by,
        rejected_at=rejected_at,
        metadata={"rejected_by": rejected_by},
    ) or request_record
    dependencies["set_workflow_state"](
        "payment_authorization",
        request_id,
        "rejected",
        current_stage="payments",
        confidence=1.0,
        actor_type="human",
        actor_id=rejected_by,
        reason="Payment authorization rejected",
        metadata={"rejected_by": rejected_by},
    )
    if request_record.get("review_item_id"):
        dependencies["update_human_review_item"](
            request_record["review_item_id"],
            status="dismissed",
            resolution="rejected",
            metadata={"rejected_by": rejected_by},
        )
    return request_record


def execute_payment_authorization(request_id: str) -> Dict[str, Any]:
    dependencies = _get_dependencies()
    request_record = dependencies["get_payment_authorization_request"](request_id)
    if not request_record:
        raise ValueError("Payment authorization request not found")
    if request_record["approval_status"] != "approved":
        raise ValueError("Payment authorization must be approved before execution")

    task = dependencies["create_completed_agent_task"](
        task_type="payment.execute",
        entity_type="payment_authorization",
        entity_id=request_id,
        priority=95,
        payload={
            "invoice_ids": request_record["invoice_ids"],
            "currency": request_record["currency"],
            "save_method": request_record["save_method"],
        },
        result={"status": "executing"},
    )
    try:
        result = dependencies["create_payment_intent_for_invoices"](
            request_record["invoice_ids"],
            request_record["customer"],
            request_record["currency"],
            request_record["save_method"],
        )
    except Exception as exc:
        dependencies["record_agent_decision"](
            task_id=task["id"],
            entity_type="payment_authorization",
            entity_id=request_id,
            agent_name="payment_authorization_agent",
            model_name="deterministic_risk_rules",
            prompt_version="v1",
            decision_type="payment_execution",
            decision="execution_failed",
            confidence=0.0,
            reasoning_summary=str(exc),
            metadata={"error": str(exc)},
        )
        dependencies["update_payment_authorization_request"](
            request_id,
            approval_status="execution_failed",
            metadata={"execution_error": str(exc)},
        )
        dependencies["set_workflow_state"](
            "payment_authorization",
            request_id,
            "execution_failed",
            current_stage="payments",
            confidence=0.0,
            reason="Stripe payment intent creation failed",
            metadata={"error": str(exc)},
        )
        raise

    dependencies["record_agent_decision"](
        task_id=task["id"],
        entity_type="payment_authorization",
        entity_id=request_id,
        agent_name="payment_authorization_agent",
        model_name="deterministic_risk_rules",
        prompt_version="v1",
        decision_type="payment_execution",
        decision="payment_intent_created",
        confidence=1.0,
        reasoning_summary="Approved payment batch executed through the existing Stripe integration.",
        metadata={"payment_intent_id": result.get("paymentIntentId")},
    )
    request_record = dependencies["update_payment_authorization_request"](
        request_id,
        approval_status="payment_intent_created",
        executed_payment_id=str(result.get("paymentId") or ""),
        executed_payment_intent_id=result.get("paymentIntentId"),
        metadata={"execution_result": result},
    ) or request_record
    dependencies["set_workflow_state"](
        "payment_authorization",
        request_id,
        "payment_intent_created",
        current_stage="payments",
        confidence=1.0,
        reason="Stripe payment intent created for approved payment batch",
        metadata={"payment_intent_id": result.get("paymentIntentId")},
    )
    return {
        "authorization_request": request_record,
        "payment_result": result,
    }
