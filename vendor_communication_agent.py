import datetime
import re
from typing import Any, Dict, Optional

from db import get_conn


def _get_agent_db_ops():
    from agent_db import (
        create_completed_agent_task,
        create_human_review_item,
        get_vendor_communication,
        record_agent_decision,
        record_vendor_communication,
        set_workflow_state,
        update_human_review_item,
        update_vendor_communication,
    )

    return {
        "create_completed_agent_task": create_completed_agent_task,
        "create_human_review_item": create_human_review_item,
        "get_vendor_communication": get_vendor_communication,
        "record_agent_decision": record_agent_decision,
        "record_vendor_communication": record_vendor_communication,
        "set_workflow_state": set_workflow_state,
        "update_human_review_item": update_human_review_item,
        "update_vendor_communication": update_vendor_communication,
    }


def _extract_email(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    match = re.search(r"[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}", str(value), flags=re.IGNORECASE)
    return match.group(0) if match else None


def _load_invoice_vendor_context(invoice_id: str) -> Optional[Dict[str, Any]]:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                  i.id,
                  i.invoice_number,
                  i.po_number,
                  i.total_amount,
                  i.currency,
                  i.status,
                  i.supplier_name,
                  i.supplier_email,
                  i.vendor_id,
                  v.name,
                  v.contact_info,
                  i.due_date
                FROM invoices AS i
                LEFT JOIN vendors AS v
                  ON v.id = i.vendor_id
                WHERE i.id = %s
                LIMIT 1
                """,
                (invoice_id,),
            )
            row = cur.fetchone()
            if not row:
                return None
            due_date = row[11].isoformat() if row[11] else None
            return {
                "invoice_id": str(row[0]),
                "invoice_number": row[1] or "",
                "po_number": row[2] or "",
                "total_amount": float(row[3]) if row[3] is not None else None,
                "currency": row[4] or "USD",
                "status": row[5] or "",
                "supplier_name": row[6] or "",
                "supplier_email": row[7] or "",
                "vendor_id": str(row[8]) if row[8] else None,
                "vendor_name": row[9] or row[6] or "Unknown Vendor",
                "vendor_contact": row[10] or "",
                "due_date": due_date,
            }


def _build_draft_template(context: Dict[str, Any], review_reason: str) -> Dict[str, str]:
    invoice_number = context.get("invoice_number") or context["invoice_id"]
    vendor_name = context.get("vendor_name") or "team"
    po_number = context.get("po_number") or "the related purchase order"
    total_amount = context.get("total_amount")
    currency = context.get("currency") or "USD"
    amount_text = f"{currency} {total_amount:,.2f}" if total_amount is not None else "the invoice amount"

    reason_templates = {
        "missing_po_number": {
            "subject": f"Clarification needed for invoice {invoice_number}",
            "body": (
                f"Hello {vendor_name},\n\n"
                f"We received invoice {invoice_number}, but it does not include a purchase order reference. "
                f"Please reply with the correct PO number so we can continue processing.\n\n"
                f"Thank you."
            ),
            "reasoning": "The invoice is missing a PO reference, so the safest outbound clarification is a PO-number request.",
        },
        "vendor_mismatch": {
            "subject": f"Supplier confirmation for invoice {invoice_number}",
            "body": (
                f"Hello {vendor_name},\n\n"
                f"We are validating invoice {invoice_number} and need confirmation of the supplier entity that issued it. "
                f"Please confirm the billing legal entity and any supporting remittance details.\n\n"
                f"Thank you."
            ),
            "reasoning": "The extracted supplier identity does not align cleanly with the vendor record, so the draft asks for entity confirmation.",
        },
        "amount_outside_tolerance": {
            "subject": f"Amount clarification for invoice {invoice_number}",
            "body": (
                f"Hello {vendor_name},\n\n"
                f"We are reviewing invoice {invoice_number} for {amount_text}. "
                f"The amount does not align with {po_number} within our processing tolerance. "
                f"Please confirm whether the invoice amount is correct and share any supporting explanation.\n\n"
                f"Thank you."
            ),
            "reasoning": "The amount mismatch needs human-verifiable support before matching or payment can proceed.",
        },
    }
    default_template = {
        "subject": f"Clarification needed for invoice {invoice_number}",
        "body": (
            f"Hello {vendor_name},\n\n"
            f"We need clarification to continue processing invoice {invoice_number}. "
            f"Please reply with any details that will help us resolve the outstanding review item.\n\n"
            f"Thank you."
        ),
        "reasoning": "A generic clarification draft is safest when the exception reason is not tied to a narrower template.",
    }
    return reason_templates.get(review_reason, default_template)


def create_vendor_communication_draft(
    invoice_id: str,
    *,
    review_reason: str,
    source_document_id: Optional[str] = None,
) -> Dict[str, Any]:
    context = _load_invoice_vendor_context(invoice_id)
    if not context:
        raise ValueError("Invoice not found")

    ops = _get_agent_db_ops()
    recipient = _extract_email(context.get("supplier_email")) or _extract_email(context.get("vendor_contact"))
    template = _build_draft_template(context, review_reason)
    confidence = 0.86 if recipient else 0.63

    communication = ops["record_vendor_communication"](
        direction="draft",
        vendor_id=context.get("vendor_id"),
        invoice_id=invoice_id,
        source_document_id=source_document_id,
        channel="email",
        status="pending_approval",
        recipient=recipient,
        subject=template["subject"],
        body=template["body"],
        metadata={
            "review_reason": review_reason,
            "approval_required": True,
            "recipient_inferred": bool(recipient),
            "invoice_number": context.get("invoice_number"),
        },
    )
    task = ops["create_completed_agent_task"](
        task_type="vendor_communication.plan",
        entity_type="vendor_communication",
        entity_id=communication["id"],
        source_document_id=source_document_id,
        priority=80,
        payload={
            "invoice_id": invoice_id,
            "review_reason": review_reason,
        },
        result={
            "status": "pending_approval",
            "recipient": recipient,
        },
    )
    ops["record_agent_decision"](
        task_id=task["id"],
        entity_type="vendor_communication",
        entity_id=communication["id"],
        agent_name="vendor_communication_planner",
        model_name="deterministic_templates",
        prompt_version="v1",
        decision_type="vendor_communication_plan",
        decision="draft_pending_approval",
        confidence=confidence,
        reasoning_summary=template["reasoning"],
        tool_calls=[],
        metadata={
            "review_reason": review_reason,
            "recipient_found": bool(recipient),
            "invoice_id": invoice_id,
        },
    )
    ops["set_workflow_state"](
        "vendor_communication",
        communication["id"],
        "pending_approval",
        current_stage="vendor_outreach",
        confidence=confidence,
        reason="Outbound vendor communication draft requires human approval",
        metadata={
            "invoice_id": invoice_id,
            "review_reason": review_reason,
        },
    )
    review_item = ops["create_human_review_item"](
        entity_type="vendor_communication",
        entity_id=communication["id"],
        source_document_id=source_document_id,
        invoice_id=invoice_id,
        queue_name="vendor_communications",
        priority=80,
        review_reason="approval_required",
        metadata={
            "invoice_id": invoice_id,
            "review_reason": review_reason,
            "recipient": recipient,
        },
    )
    communication = ops["update_vendor_communication"](
        communication["id"],
        metadata={"review_item_id": review_item["id"]},
    ) or communication
    return {
        "communication": communication,
        "review_item": review_item,
        "task_id": task["id"],
    }


def approve_vendor_communication(communication_id: str, *, approved_by: str) -> Dict[str, Any]:
    ops = _get_agent_db_ops()
    communication = ops["get_vendor_communication"](communication_id)
    if not communication:
        raise ValueError("Vendor communication not found")

    approved_at = datetime.datetime.now(datetime.timezone.utc).isoformat()
    communication = ops["update_vendor_communication"](
        communication_id,
        status="approved",
        approved_by=approved_by,
        metadata={"approved_at": approved_at},
    ) or communication
    ops["set_workflow_state"](
        "vendor_communication",
        communication_id,
        "approved",
        current_stage="vendor_outreach",
        confidence=1.0,
        actor_type="human",
        actor_id=approved_by,
        reason="Vendor communication approved for outbound delivery",
        metadata={"approved_by": approved_by},
    )
    review_item_id = (communication.get("metadata") or {}).get("review_item_id")
    if review_item_id:
        ops["update_human_review_item"](
            review_item_id,
            status="resolved",
            resolution="approved",
            metadata={"approved_by": approved_by},
        )
    return communication


def reject_vendor_communication(communication_id: str, *, rejected_by: str) -> Dict[str, Any]:
    ops = _get_agent_db_ops()
    communication = ops["get_vendor_communication"](communication_id)
    if not communication:
        raise ValueError("Vendor communication not found")

    rejected_at = datetime.datetime.now(datetime.timezone.utc).isoformat()
    communication = ops["update_vendor_communication"](
        communication_id,
        status="rejected",
        metadata={
            "rejected_by": rejected_by,
            "rejected_at": rejected_at,
        },
    ) or communication
    ops["set_workflow_state"](
        "vendor_communication",
        communication_id,
        "rejected",
        current_stage="vendor_outreach",
        confidence=1.0,
        actor_type="human",
        actor_id=rejected_by,
        reason="Vendor communication rejected",
        metadata={"rejected_by": rejected_by},
    )
    review_item_id = (communication.get("metadata") or {}).get("review_item_id")
    if review_item_id:
        ops["update_human_review_item"](
            review_item_id,
            status="dismissed",
            resolution="rejected",
            metadata={"rejected_by": rejected_by},
        )
    return communication


def mark_vendor_communication_sent(communication_id: str, *, sent_by: str) -> Dict[str, Any]:
    ops = _get_agent_db_ops()
    communication = ops["get_vendor_communication"](communication_id)
    if not communication:
        raise ValueError("Vendor communication not found")
    if communication.get("status") != "approved":
        raise ValueError("Vendor communication must be approved before it can be marked sent")

    sent_at = datetime.datetime.now(datetime.timezone.utc)
    communication = ops["update_vendor_communication"](
        communication_id,
        status="sent",
        sent_at=sent_at,
        metadata={"sent_by": sent_by},
    ) or communication
    ops["set_workflow_state"](
        "vendor_communication",
        communication_id,
        "sent",
        current_stage="vendor_outreach",
        confidence=1.0,
        actor_type="human",
        actor_id=sent_by,
        reason="Vendor communication manually marked sent",
        metadata={"sent_by": sent_by},
    )
    return communication
