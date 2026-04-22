import datetime
import hashlib
import os
import uuid
from typing import Any, Dict, List, Optional

from db import get_conn
from realtime_events import publish_live_update


SYSTEM_AUTO_APPROVER = "system:auto"


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


def _normalize_invoice_ids(invoice_ids: List[str]) -> List[str]:
    normalized: List[str] = []
    for raw_invoice_id in invoice_ids or []:
        invoice_id = str(raw_invoice_id or "").strip()
        if invoice_id:
            normalized.append(invoice_id)
    if not normalized:
        raise ValueError("invoiceIds required")
    return normalized


def _canonical_invoice_ids(invoice_ids: List[str]) -> List[str]:
    return sorted(_unique_invoice_ids(invoice_ids))


def _unique_invoice_ids(invoice_ids: List[str]) -> List[str]:
    return list(dict.fromkeys(_normalize_invoice_ids(invoice_ids)))


def _duplicate_selection_invoice_ids(invoice_ids: List[str]) -> List[str]:
    seen = set()
    duplicates: List[str] = []
    for invoice_id in _normalize_invoice_ids(invoice_ids):
        if invoice_id in seen and invoice_id not in duplicates:
            duplicates.append(invoice_id)
        seen.add(invoice_id)
    return duplicates


def _load_batch_invoices(invoice_ids: List[str]) -> List[Dict[str, Any]]:
    unique_invoice_ids = _unique_invoice_ids(invoice_ids)

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
                (unique_invoice_ids,),
            )
            rows = cur.fetchall()

    if len(rows) != len(unique_invoice_ids):
        raise ValueError("Some invoices not found")

    invoices_by_id: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        invoices_by_id[str(row[0])] = {
            "id": str(row[0]),
            "invoice_number": row[1] or "",
            "total_amount": float(row[2]) if row[2] is not None else None,
            "currency": row[3] or "USD",
            "status": row[4] or "",
            "vendor_id": str(row[5]) if row[5] else None,
            "due_date": row[6].isoformat() if row[6] else None,
            "supplier_name": row[7] or "",
        }
    return [invoices_by_id[invoice_id] for invoice_id in unique_invoice_ids]


def _load_vendor_successful_payment_history(vendor_ids: List[str]) -> Dict[str, bool]:
    normalized_vendor_ids = [str(vendor_id) for vendor_id in vendor_ids if vendor_id]
    if not normalized_vendor_ids:
        return {}

    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT i.vendor_id
                FROM invoices AS i
                JOIN payment_invoices AS pi ON pi.invoice_id = i.id
                JOIN payments AS p ON p.id = pi.payment_id
                WHERE i.vendor_id = ANY(%s)
                  AND p.status = 'succeeded'
                """,
                (normalized_vendor_ids,),
            )
            successful_vendor_ids = {str(row[0]) for row in cur.fetchall() if row and row[0]}

    return {
        vendor_id: vendor_id in successful_vendor_ids
        for vendor_id in normalized_vendor_ids
    }


def _risk_rank(level: str) -> int:
    return {"low": 0, "medium": 1, "high": 2}[level]


def _max_risk_level(*levels: str) -> str:
    return max(levels, key=_risk_rank)


def _threshold_risk_level(value: float, *, medium_threshold: float, high_threshold: float) -> str:
    normalized_high_threshold = max(float(high_threshold), float(medium_threshold))
    normalized_medium_threshold = min(float(medium_threshold), normalized_high_threshold)
    if value >= normalized_high_threshold:
        return "high"
    if value >= normalized_medium_threshold:
        return "medium"
    return "low"


def _format_money(amount: float, currency: str) -> str:
    return f"{(currency or 'USD').upper()} {float(amount):,.2f}"


def _risk_phrase(level: str) -> str:
    return f"{level} risk"


def _build_risk_reasoning(analysis: Dict[str, Any]) -> str:
    signals = analysis["signals"]
    amount_signal = signals["total_batch_amount"]
    count_signal = signals["invoice_count"]
    vendor_signal = signals["vendor_payment_history"]
    duplicate_signal = signals["duplicate_selection"]

    reasoning_parts = [
        (
            f"Batch total {_format_money(amount_signal['value'], analysis['currency'])} "
            f"was compared against the medium-risk threshold of "
            f"{_format_money(amount_signal['medium_threshold'], analysis['currency'])} "
            f"and the high-risk threshold of "
            f"{_format_money(amount_signal['high_threshold'], analysis['currency'])}, "
            f"so the amount signal is {_risk_phrase(amount_signal['risk_level'])}."
        ),
        (
            f"Invoice count {count_signal['value']} was compared against the medium-risk "
            f"threshold of {count_signal['medium_threshold']} and the high-risk threshold "
            f"of {count_signal['high_threshold']}, so the count signal is "
            f"{_risk_phrase(count_signal['risk_level'])}."
        ),
    ]

    vendor_history_text = (
        f"All {vendor_signal['vendor_count']} vendors in the batch have prior successfully "
        f"paid invoices"
        if vendor_signal["all_vendors_have_prior_successful_payments"]
        else (
            f"Only {vendor_signal['vendors_with_history']} of {vendor_signal['vendor_count']} "
            f"vendors in the batch have prior successfully paid invoices"
        )
    )
    reasoning_parts.append(
        f"{vendor_history_text}, so the vendor-history signal is "
        f"{_risk_phrase(vendor_signal['risk_level'])}."
    )

    if duplicate_signal["has_duplicate_selection"]:
        duplicate_ids = ", ".join(duplicate_signal["duplicate_invoice_ids"])
        reasoning_parts.append(
            f"Duplicate selection detected for invoice ids {duplicate_ids}, so the "
            f"duplicate-selection signal is {_risk_phrase(duplicate_signal['risk_level'])}."
        )
    else:
        reasoning_parts.append(
            f"Duplicate selection detected: no, so the duplicate-selection signal is "
            f"{_risk_phrase(duplicate_signal['risk_level'])}."
        )

    if analysis["recommendation"] == "auto_execute":
        reasoning_parts.append(
            f"Combined risk level is {analysis['risk_level']}, so the batch qualified for "
            f"automatic approval and immediate Stripe payment intent creation."
        )
    else:
        reasoning_parts.append(
            f"Combined risk level is {analysis['risk_level']}, so the batch was routed for "
            f"human approval before Stripe payment intent creation."
        )

    return " ".join(reasoning_parts)


def _analysis_from_request_record(request_record: Dict[str, Any]) -> Dict[str, Any]:
    metadata = request_record.get("metadata") or {}
    signals = metadata.get("risk_signals") or {}
    return {
        "invoice_count": int(request_record.get("invoice_count") or 0),
        "currency": request_record.get("currency") or "USD",
        "total_amount": float(request_record.get("total_amount") or 0.0),
        "vendor_count": int((signals.get("vendor_payment_history") or {}).get("vendor_count") or 0),
        "risk_level": request_record.get("risk_level") or "medium",
        "risk_reasons": list(request_record.get("risk_reasons") or []),
        "recommendation": request_record.get("recommendation") or "approval_required",
        "confidence": float((signals.get("confidence") or 0.78) if isinstance(signals, dict) else 0.78),
        "signals": signals,
    }


def _matches_authorization_batch(
    request_record: Dict[str, Any],
    *,
    invoice_ids: List[str],
    currency: Optional[str] = None,
) -> bool:
    request_invoice_ids = _canonical_invoice_ids(request_record.get("invoice_ids") or [])
    if request_invoice_ids != _canonical_invoice_ids(invoice_ids):
        return False
    if currency:
        request_currency = str(request_record.get("currency") or "").strip().upper()
        if request_currency and request_currency != str(currency).strip().upper():
            return False
    return True


def _find_matching_pending_authorization_request(
    dependencies: Dict[str, Any],
    *,
    invoice_ids: List[str],
    currency: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    requests = dependencies["list_payment_authorization_requests"](
        approval_status="pending_approval",
        risk_level=None,
        limit=200,
    )
    for request_record in requests:
        if _matches_authorization_batch(
            request_record,
            invoice_ids=invoice_ids,
            currency=currency,
        ):
            return request_record
    return None


def _existing_pending_authorization_payload(
    request_record: Dict[str, Any],
) -> Dict[str, Any]:
    return {
        "status": "pending_approval",
        "authorization_request_id": request_record["id"],
        "authorization_request": request_record,
        "analysis": _analysis_from_request_record(request_record),
        "reused_existing_request": True,
    }


def _analyze_payment_batch(
    invoice_ids: List[str],
    invoices: List[Dict[str, Any]],
    currency: Optional[str],
) -> Dict[str, Any]:
    allowed_statuses = {"matched_auto", "ready_for_payment"}
    currencies = {invoice["currency"] for invoice in invoices if invoice.get("currency")}
    if currency:
        currencies.add(currency)
    if len(currencies) > 1:
        raise ValueError("Mixed currency selection is not allowed")

    total_amount = 0.0
    vendor_ids: List[str] = []
    risk_reasons: List[str] = []
    duplicate_invoice_ids = _duplicate_selection_invoice_ids(invoice_ids)

    for invoice in invoices:
        if invoice["status"] not in allowed_statuses:
            raise ValueError("One or more invoices are not eligible for payment authorization")
        if invoice["total_amount"] is None:
            raise ValueError("One or more invoices are missing total amount")
        total_amount += float(invoice["total_amount"])
        if invoice.get("vendor_id"):
            vendor_ids.append(invoice["vendor_id"])

    vendor_ids = list(dict.fromkeys(vendor_ids))

    high_amount_threshold = _risk_threshold("PAYMENT_AUTH_HIGH_RISK_TOTAL", 50000.0)
    medium_amount_threshold = _risk_threshold("PAYMENT_AUTH_MEDIUM_RISK_TOTAL", 10000.0)
    high_count_threshold = _invoice_count_threshold("PAYMENT_AUTH_HIGH_RISK_INVOICE_COUNT", 10)
    medium_count_threshold = _invoice_count_threshold("PAYMENT_AUTH_MEDIUM_RISK_INVOICE_COUNT", 4)

    amount_risk_level = _threshold_risk_level(
        float(total_amount),
        medium_threshold=medium_amount_threshold,
        high_threshold=high_amount_threshold,
    )
    count_risk_level = _threshold_risk_level(
        float(len(invoices)),
        medium_threshold=float(medium_count_threshold),
        high_threshold=float(high_count_threshold),
    )
    vendor_history = _load_vendor_successful_payment_history(vendor_ids)
    vendors_with_history = sum(1 for vendor_id in vendor_ids if vendor_history.get(vendor_id))
    all_vendors_have_history = bool(vendor_ids) and vendors_with_history == len(vendor_ids)
    vendor_history_risk_level = "low" if all_vendors_have_history else "medium"
    duplicate_selection_risk_level = "high" if duplicate_invoice_ids else "low"

    if amount_risk_level != "low":
        risk_reasons.append(f"amount_{amount_risk_level}_risk")
    if count_risk_level != "low":
        risk_reasons.append(f"invoice_count_{count_risk_level}_risk")
    if vendor_history_risk_level != "low":
        risk_reasons.append("vendor_payment_history_missing")
    if duplicate_selection_risk_level != "low":
        risk_reasons.append("duplicate_selection_detected")

    risk_level = _max_risk_level(
        amount_risk_level,
        count_risk_level,
        vendor_history_risk_level,
        duplicate_selection_risk_level,
    )
    confidence = 0.94 if risk_level == "low" else 0.78 if risk_level == "medium" else 0.61
    recommendation = "auto_execute" if risk_level == "low" else "approval_required"
    final_currency = next(iter(currencies), currency or "USD")

    return {
        "invoice_count": len(invoices),
        "currency": final_currency,
        "total_amount": round(total_amount, 2),
        "vendor_count": len(vendor_ids),
        "risk_level": risk_level,
        "risk_reasons": sorted(set(risk_reasons)),
        "recommendation": recommendation,
        "confidence": confidence,
        "signals": {
            "total_batch_amount": {
                "value": round(total_amount, 2),
                "currency": final_currency,
                "medium_threshold": medium_amount_threshold,
                "high_threshold": high_amount_threshold,
                "risk_level": amount_risk_level,
            },
            "invoice_count": {
                "value": len(invoices),
                "medium_threshold": medium_count_threshold,
                "high_threshold": high_count_threshold,
                "risk_level": count_risk_level,
            },
            "vendor_payment_history": {
                "vendor_count": len(vendor_ids),
                "vendors_with_history": vendors_with_history,
                "all_vendors_have_prior_successful_payments": all_vendors_have_history,
                "risk_level": vendor_history_risk_level,
            },
            "duplicate_selection": {
                "has_duplicate_selection": bool(duplicate_invoice_ids),
                "duplicate_invoice_ids": duplicate_invoice_ids,
                "risk_level": duplicate_selection_risk_level,
            },
        },
    }


def _create_authorization_request(
    dependencies: Dict[str, Any],
    *,
    invoice_ids: List[str],
    customer: Dict[str, Any],
    analysis: Dict[str, Any],
    requested_by: Optional[str],
    save_method: bool,
    invoices: List[Dict[str, Any]],
) -> Dict[str, Any]:
    return dependencies["create_payment_authorization_request"](
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
            "risk_signals": analysis["signals"],
        },
    )


def _record_routing_decision(
    dependencies: Dict[str, Any],
    *,
    request_record: Dict[str, Any],
    invoice_ids: List[str],
    analysis: Dict[str, Any],
    requested_by: Optional[str],
    save_method: bool,
    task_type: str,
) -> Dict[str, Any]:
    task = dependencies["create_completed_agent_task"](
        task_type=task_type,
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
        prompt_version="v2",
        decision_type="payment_risk_routing",
        decision=analysis["recommendation"],
        confidence=analysis["confidence"],
        reasoning_summary=_build_risk_reasoning(analysis),
        metadata={
            "risk_level": analysis["risk_level"],
            "risk_reasons": analysis["risk_reasons"],
            "risk_signals": analysis["signals"],
            "requested_by": requested_by,
        },
    )
    return task


def _queue_authorization_for_review(
    dependencies: Dict[str, Any],
    *,
    request_record: Dict[str, Any],
    invoice_ids: List[str],
    analysis: Dict[str, Any],
    requested_by: Optional[str],
) -> Dict[str, Any]:
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
            "risk_signals": analysis["signals"],
            "requested_by": requested_by,
        },
    )
    request_record = dependencies["update_payment_authorization_request"](
        request_record["id"],
        review_item_id=review_item["id"],
        metadata={"review_item_id": review_item["id"]},
    ) or request_record
    publish_live_update(
        "payment.authorization_pending",
        {
            "authorizationRequestId": request_record["id"],
            "invoiceIds": invoice_ids,
            "riskLevel": analysis["risk_level"],
        },
    )
    return {
        "authorization_request": request_record,
        "review_item": review_item,
        "analysis": analysis,
    }


def _auto_approve_authorization(
    dependencies: Dict[str, Any],
    *,
    request_record: Dict[str, Any],
    analysis: Dict[str, Any],
    requested_by: Optional[str],
) -> Dict[str, Any]:
    approved_at = datetime.datetime.now(datetime.timezone.utc)
    request_record = dependencies["update_payment_authorization_request"](
        request_record["id"],
        approval_status="approved",
        approved_by=SYSTEM_AUTO_APPROVER,
        approved_at=approved_at,
        metadata={
            "auto_execution": True,
            "auto_execution_requested_by": requested_by,
        },
    ) or request_record
    dependencies["set_workflow_state"](
        "payment_authorization",
        request_record["id"],
        "approved",
        current_stage="payments",
        confidence=analysis["confidence"],
        actor_type="agent",
        actor_id="payment_authorization_agent",
        reason="Low-risk payment batch was auto-approved for immediate execution",
        metadata={
            "risk_level": analysis["risk_level"],
            "requested_by": requested_by,
        },
    )
    publish_live_update(
        "payment.authorization_auto_approved",
        {
            "authorizationRequestId": request_record["id"],
            "riskLevel": analysis["risk_level"],
        },
    )
    return request_record


def request_payment_authorization(
    invoice_ids: List[str],
    customer: Dict[str, Any],
    *,
    currency: Optional[str] = None,
    save_method: bool = False,
    requested_by: Optional[str] = None,
) -> Dict[str, Any]:
    dependencies = _get_dependencies()
    unique_invoice_ids = _unique_invoice_ids(invoice_ids)
    existing_request = _find_matching_pending_authorization_request(
        dependencies,
        invoice_ids=unique_invoice_ids,
        currency=currency,
    )
    if existing_request:
        return _existing_pending_authorization_payload(existing_request)
    invoices = _load_batch_invoices(unique_invoice_ids)
    analysis = _analyze_payment_batch(invoice_ids, invoices, currency)
    manual_analysis = dict(analysis)
    manual_analysis["recommendation"] = "approval_required"
    request_record = _create_authorization_request(
        dependencies,
        invoice_ids=unique_invoice_ids,
        customer=customer,
        analysis=manual_analysis,
        requested_by=requested_by,
        save_method=save_method,
        invoices=invoices,
    )
    task = _record_routing_decision(
        dependencies,
        request_record=request_record,
        invoice_ids=unique_invoice_ids,
        analysis=manual_analysis,
        requested_by=requested_by,
        save_method=save_method,
        task_type="payment.authorize",
    )
    queued = _queue_authorization_for_review(
        dependencies,
        request_record=request_record,
        invoice_ids=unique_invoice_ids,
        analysis=manual_analysis,
        requested_by=requested_by,
    )
    queued["task_id"] = task["id"]
    return queued


def evaluate_and_route_payment_batch(
    invoice_ids: List[str],
    customer: Dict[str, Any],
    *,
    currency: Optional[str] = None,
    save_method: bool = False,
    requested_by: Optional[str] = None,
) -> Dict[str, Any]:
    dependencies = _get_dependencies()
    unique_invoice_ids = _unique_invoice_ids(invoice_ids)
    existing_request = _find_matching_pending_authorization_request(
        dependencies,
        invoice_ids=unique_invoice_ids,
        currency=currency,
    )
    if existing_request:
        return _existing_pending_authorization_payload(existing_request)
    invoices = _load_batch_invoices(unique_invoice_ids)
    analysis = _analyze_payment_batch(invoice_ids, invoices, currency)
    request_record = _create_authorization_request(
        dependencies,
        invoice_ids=unique_invoice_ids,
        customer=customer,
        analysis=analysis,
        requested_by=requested_by,
        save_method=save_method,
        invoices=invoices,
    )
    _record_routing_decision(
        dependencies,
        request_record=request_record,
        invoice_ids=unique_invoice_ids,
        analysis=analysis,
        requested_by=requested_by,
        save_method=save_method,
        task_type="payment.route",
    )

    if analysis["recommendation"] == "auto_execute":
        request_record = _auto_approve_authorization(
            dependencies,
            request_record=request_record,
            analysis=analysis,
            requested_by=requested_by,
        )
        execution_result = execute_payment_authorization(request_record["id"])
        return {
            "status": "auto_executed",
            "authorization_request_id": request_record["id"],
            "authorization_request": execution_result["authorization_request"],
            "analysis": analysis,
            "payment_result": execution_result["payment_result"],
        }

    queued = _queue_authorization_for_review(
        dependencies,
        request_record=request_record,
        invoice_ids=unique_invoice_ids,
        analysis=analysis,
        requested_by=requested_by,
    )
    return {
        "status": "pending_approval",
        "authorization_request_id": queued["authorization_request"]["id"],
        "authorization_request": queued["authorization_request"],
        "review_item": queued["review_item"],
        "analysis": analysis,
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


def _payment_batch_dedupe_key(
    task_type: str,
    invoice_ids: List[str],
    currency: Optional[str],
    requested_by: Optional[str],
) -> str:
    unique_invoice_ids = _canonical_invoice_ids(invoice_ids)
    raw_key = "|".join(
        [
            task_type,
            ",".join(unique_invoice_ids),
            (currency or "").upper(),
            str(requested_by or ""),
        ]
    )
    return f"{task_type}:{hashlib.sha256(raw_key.encode('utf-8')).hexdigest()}"


def _raise_for_task_failure(task: Optional[Dict[str, Any]], fallback_message: str) -> None:
    if not task:
        raise RuntimeError(fallback_message)
    error_message = task.get("last_error") or fallback_message
    if task.get("status") == "dead_letter":
        raise ValueError(error_message)
    raise RuntimeError(error_message)


def _submit_payment_task_and_wait(
    *,
    task_type: str,
    entity_type: str,
    entity_id: str,
    payload: Dict[str, Any],
    dedupe_key: Optional[str],
    sync_loader,
):
    if not _env_bool("ENABLE_AGENT_WORKER", False):
        return sync_loader()

    from workflow_task_queue import enqueue_workflow_task, wait_for_task_completion

    task = enqueue_workflow_task(
        task_type=task_type,
        entity_type=entity_type,
        entity_id=entity_id,
        dedupe_key=dedupe_key,
        payload=payload,
    )
    completed_task = wait_for_task_completion(
        task["id"],
        timeout_seconds=_env_float("AGENT_PAYMENT_WAIT_TIMEOUT_SECONDS", 45.0),
        poll_interval_seconds=0.2,
    )
    if completed_task and completed_task.get("status") == "completed":
        return completed_task.get("result")
    if task_type in {"payment.authorize", "payment.route"}:
        dependencies = _get_dependencies()
        existing_request = _find_matching_pending_authorization_request(
            dependencies,
            invoice_ids=payload.get("invoice_ids") or [],
            currency=payload.get("currency"),
        )
        if existing_request:
            return _existing_pending_authorization_payload(existing_request)
    _raise_for_task_failure(
        completed_task,
        "Payment routing is still being evaluated. Please wait a moment and try again.",
    )


def submit_payment_authorization_request(
    invoice_ids: List[str],
    customer: Dict[str, Any],
    *,
    currency: Optional[str] = None,
    save_method: bool = False,
    requested_by: Optional[str] = None,
) -> Dict[str, Any]:
    normalized_invoice_ids = _unique_invoice_ids(invoice_ids)
    return _submit_payment_task_and_wait(
        task_type="payment.authorize",
        entity_type="payment_batch",
        entity_id=str(uuid.uuid4()),
        dedupe_key=_payment_batch_dedupe_key("payment.authorize", normalized_invoice_ids, currency, requested_by),
        payload={
            "invoice_ids": normalized_invoice_ids,
            "customer": customer or {},
            "currency": currency,
            "save_method": bool(save_method),
            "requested_by": requested_by,
        },
        sync_loader=lambda: request_payment_authorization(
            normalized_invoice_ids,
            customer,
            currency=currency,
            save_method=save_method,
            requested_by=requested_by,
        ),
    )


def submit_payment_route(
    invoice_ids: List[str],
    customer: Dict[str, Any],
    *,
    currency: Optional[str] = None,
    save_method: bool = False,
    requested_by: Optional[str] = None,
) -> Dict[str, Any]:
    normalized_invoice_ids = _unique_invoice_ids(invoice_ids)
    return _submit_payment_task_and_wait(
        task_type="payment.route",
        entity_type="payment_batch",
        entity_id=str(uuid.uuid4()),
        dedupe_key=_payment_batch_dedupe_key("payment.route", normalized_invoice_ids, currency, requested_by),
        payload={
            "invoice_ids": normalized_invoice_ids,
            "customer": customer or {},
            "currency": currency,
            "save_method": bool(save_method),
            "requested_by": requested_by,
        },
        sync_loader=lambda: evaluate_and_route_payment_batch(
            normalized_invoice_ids,
            customer,
            currency=currency,
            save_method=save_method,
            requested_by=requested_by,
        ),
    )


def submit_payment_execution(request_id: str) -> Dict[str, Any]:
    return _submit_payment_task_and_wait(
        task_type="payment.execute",
        entity_type="payment_authorization",
        entity_id=request_id,
        dedupe_key=f"payment.execute:{request_id}",
        payload={"request_id": request_id},
        sync_loader=lambda: execute_payment_authorization(request_id),
    )
