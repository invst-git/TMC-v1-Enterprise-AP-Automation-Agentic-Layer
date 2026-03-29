import os
from typing import Any, Dict, Optional

from realtime_events import publish_live_update


def _env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None:
        return default
    try:
        return int(value)
    except Exception:
        return default


def _get_dependencies():
    from agent_db import (
        create_completed_agent_task,
        get_human_review_item,
        get_human_review_queue_counts,
        record_agent_decision,
        set_workflow_state,
        update_human_review_item,
    )
    from invoice_db import update_invoice_matching_outcome
    from vendor_communication_agent import create_vendor_communication_draft

    return {
        "create_completed_agent_task": create_completed_agent_task,
        "get_human_review_item": get_human_review_item,
        "get_human_review_queue_counts": get_human_review_queue_counts,
        "record_agent_decision": record_agent_decision,
        "set_workflow_state": set_workflow_state,
        "update_human_review_item": update_human_review_item,
        "update_invoice_matching_outcome": update_invoice_matching_outcome,
        "create_vendor_communication_draft": create_vendor_communication_draft,
    }


def _review_target(item: Dict[str, Any]) -> Dict[str, Optional[str]]:
    invoice_id = item.get("invoice_id") or (item.get("entity_id") if item.get("entity_type") == "invoice" else None)
    entity_type = "invoice" if invoice_id else item.get("entity_type")
    entity_id = invoice_id or item.get("entity_id")
    return {
        "invoice_id": invoice_id,
        "entity_type": entity_type,
        "entity_id": entity_id,
    }


def _select_candidate_po(item: Dict[str, Any], preferred_po_id: Optional[str] = None) -> Optional[Dict[str, Any]]:
    candidate_pos = item.get("candidate_pos") or []
    if preferred_po_id:
        for candidate in candidate_pos:
            if str(candidate.get("po_id") or "") == str(preferred_po_id):
                return candidate
    if candidate_pos:
        return candidate_pos[0]

    metadata = item.get("metadata") or {}
    analysis = metadata.get("analysis") or {}
    best_po_id = analysis.get("best_po_id")
    if best_po_id:
        return {
            "po_id": str(best_po_id),
            "po_number": analysis.get("invoice_po_number") or "",
            "similarity_score": float(analysis.get("confidence") or 0.0),
            "amount_diff": analysis.get("best_diff"),
        }
    resolution_packet = metadata.get("resolution_packet") or {}
    for analysis_key in ("final_analysis", "initial_analysis"):
        packet_analysis = resolution_packet.get(analysis_key) or {}
        best_po_id = packet_analysis.get("best_po_id")
        if best_po_id:
            return {
                "po_id": str(best_po_id),
                "po_number": packet_analysis.get("invoice_po_number") or "",
                "similarity_score": float(packet_analysis.get("confidence") or 0.0),
                "amount_diff": packet_analysis.get("best_diff"),
            }
    return None


def _record_human_review_decision(
    *,
    deps: Dict[str, Any],
    item: Dict[str, Any],
    reviewer: str,
    action: str,
    resolution_notes: Optional[str],
    metadata: Optional[Dict[str, Any]] = None,
) -> Optional[Dict[str, Any]]:
    target = _review_target(item)
    entity_type = target["entity_type"]
    entity_id = target["entity_id"]
    if not entity_type or not entity_id:
        return None

    task = deps["create_completed_agent_task"](
        task_type=f"human_review.{action}",
        entity_type=entity_type,
        entity_id=entity_id,
        source_document_id=item.get("source_document_id"),
        priority=item.get("priority") or 100,
        payload={
            "review_item_id": item["id"],
            "reviewer": reviewer,
            "action": action,
            "queue_name": item.get("queue_name"),
            "review_reason": item.get("review_reason"),
        },
        result={
            "review_item_id": item["id"],
            "action": action,
            "resolution_notes": resolution_notes,
            "metadata": metadata or {},
        },
    )
    deps["record_agent_decision"](
        task_id=task["id"],
        entity_type=entity_type,
        entity_id=entity_id,
        agent_name="human_reviewer",
        model_name="human_operator",
        prompt_version="manual_review_v1",
        decision_type="human_review_resolution",
        decision=action,
        confidence=1.0,
        reasoning_summary=resolution_notes or f"Human reviewer {reviewer} selected {action}.",
        metadata={
            "review_item_id": item["id"],
            "reviewer": reviewer,
            "queue_name": item.get("queue_name"),
            "review_reason": item.get("review_reason"),
            "resolution_notes": resolution_notes,
            **(metadata or {}),
        },
    )
    return task


def _publish_review_queue_update(item: Dict[str, Any], *, action: str) -> None:
    publish_live_update(
        "review_queue.updated",
        {
            "reviewItemId": item.get("id"),
            "invoiceId": item.get("invoice_id"),
            "queueName": item.get("queue_name"),
            "action": action,
        },
    )


def assign_review_item(review_item_id: str, *, reviewer: str) -> Dict[str, Any]:
    deps = _get_dependencies()
    item = deps["get_human_review_item"](review_item_id)
    if not item:
        raise ValueError("Review item not found")
    if item.get("status") not in {"open", "assigned"}:
        raise ValueError("Only pending review items can be assigned")

    updated = deps["update_human_review_item"](
        review_item_id,
        status="assigned",
        assigned_to=reviewer,
        metadata={
            "assigned_by": reviewer,
            "assigned_at": item.get("updated_at"),
        },
    )
    updated = deps["get_human_review_item"](review_item_id) if updated else item
    target = _review_target(updated)
    if target["invoice_id"]:
        deps["set_workflow_state"](
            "invoice",
            target["invoice_id"],
            "in_review",
            current_stage="human_review",
            confidence=1.0,
            actor_type="human",
            actor_id=reviewer,
            event_type="human_review_assigned",
            reason="A reviewer took ownership of the queue item.",
            metadata={
                "review_item_id": review_item_id,
                "reviewer": reviewer,
                "queue_name": updated.get("queue_name"),
            },
        )
    _record_human_review_decision(
        deps=deps,
        item=updated,
        reviewer=reviewer,
        action="assigned",
        resolution_notes=f"Human reviewer {reviewer} took ownership of the review item.",
    )
    _publish_review_queue_update(updated, action="assigned")
    return updated


def resolve_review_item(
    review_item_id: str,
    *,
    reviewer: str,
    action: str,
    resolution_notes: str,
    selected_po_id: Optional[str] = None,
) -> Dict[str, Any]:
    normalized_action = str(action or "").strip().lower()
    if normalized_action not in {"approve_match", "request_vendor_clarification"}:
        raise ValueError("Unsupported review resolution action")
    if not str(resolution_notes or "").strip():
        raise ValueError("resolutionNotes is required")

    deps = _get_dependencies()
    item = deps["get_human_review_item"](review_item_id)
    if not item:
        raise ValueError("Review item not found")
    if item.get("status") not in {"open", "assigned"}:
        raise ValueError("Only pending review items can be resolved")

    target = _review_target(item)
    invoice_id = target["invoice_id"]
    resolution_metadata: Dict[str, Any] = {
        "resolved_by": reviewer,
        "resolved_action": normalized_action,
        "resolution_notes": resolution_notes,
    }

    if normalized_action == "approve_match":
        if not invoice_id:
            raise ValueError("Only invoice review items can approve a PO match")
        candidate = _select_candidate_po(item, preferred_po_id=selected_po_id)
        if not candidate:
            raise ValueError("No candidate PO was available in the review packet")
        deps["update_invoice_matching_outcome"](
            invoice_id,
            status="ready_for_payment",
            matched_po_id=candidate["po_id"],
            confidence=float(candidate.get("similarity_score") or 1.0),
        )
        deps["set_workflow_state"](
            "invoice",
            invoice_id,
            "matched_manual",
            current_stage="human_review",
            confidence=float(candidate.get("similarity_score") or 1.0),
            actor_type="human",
            actor_id=reviewer,
            event_type="human_review_resolved",
            reason="A human reviewer approved a purchase order match from the review packet.",
            metadata={
                "review_item_id": review_item_id,
                "reviewer": reviewer,
                "matched_po_id": candidate["po_id"],
                "resolution_notes": resolution_notes,
            },
        )
        resolution_metadata["matched_po_id"] = candidate["po_id"]
        resolution_metadata["matched_po_number"] = candidate.get("po_number")
    else:
        communication = None
        if invoice_id:
            try:
                communication = deps["create_vendor_communication_draft"](
                    invoice_id,
                    review_reason=item.get("review_reason") or "needs_clarification",
                    source_document_id=item.get("source_document_id"),
                )
            except Exception:
                communication = None
            deps["set_workflow_state"](
                "invoice",
                invoice_id,
                "vendor_clarification_requested",
                current_stage="human_review",
                confidence=1.0,
                actor_type="human",
                actor_id=reviewer,
                event_type="human_review_resolved",
                reason="A human reviewer requested vendor clarification before any further invoice action.",
                metadata={
                    "review_item_id": review_item_id,
                    "reviewer": reviewer,
                    "resolution_notes": resolution_notes,
                    "vendor_communication_id": (
                        ((communication or {}).get("communication") or {}).get("id")
                        if communication else None
                    ),
                },
            )
        if communication:
            resolution_metadata["vendor_communication_id"] = communication["communication"]["id"]
            resolution_metadata["vendor_communication_review_item_id"] = communication["review_item"]["id"]

    updated = deps["update_human_review_item"](
        review_item_id,
        status="resolved",
        assigned_to=reviewer,
        resolution=normalized_action,
        metadata=resolution_metadata,
    )
    updated = deps["get_human_review_item"](review_item_id) if updated else item
    _record_human_review_decision(
        deps=deps,
        item=updated,
        reviewer=reviewer,
        action=normalized_action,
        resolution_notes=resolution_notes,
        metadata=resolution_metadata,
    )
    _publish_review_queue_update(updated, action=normalized_action)
    return updated


def reject_review_item(
    review_item_id: str,
    *,
    reviewer: str,
    resolution_notes: str,
) -> Dict[str, Any]:
    if not str(resolution_notes or "").strip():
        raise ValueError("resolutionNotes is required")

    deps = _get_dependencies()
    item = deps["get_human_review_item"](review_item_id)
    if not item:
        raise ValueError("Review item not found")
    if item.get("status") not in {"open", "assigned"}:
        raise ValueError("Only pending review items can be rejected")

    target = _review_target(item)
    invoice_id = target["invoice_id"]
    if invoice_id:
        deps["update_invoice_matching_outcome"](
            invoice_id,
            status="rejected",
            matched_po_id=None,
            confidence=1.0,
        )
        deps["set_workflow_state"](
            "invoice",
            invoice_id,
            "rejected",
            current_stage="human_review",
            confidence=1.0,
            actor_type="human",
            actor_id=reviewer,
            event_type="human_review_rejected",
            reason="A human reviewer rejected the review queue item.",
            metadata={
                "review_item_id": review_item_id,
                "reviewer": reviewer,
                "resolution_notes": resolution_notes,
            },
        )

    updated = deps["update_human_review_item"](
        review_item_id,
        status="dismissed",
        assigned_to=reviewer,
        resolution="rejected",
        metadata={
            "rejected_by": reviewer,
            "resolved_action": "rejected",
            "resolution_notes": resolution_notes,
        },
    )
    updated = deps["get_human_review_item"](review_item_id) if updated else item
    _record_human_review_decision(
        deps=deps,
        item=updated,
        reviewer=reviewer,
        action="rejected",
        resolution_notes=resolution_notes,
        metadata={"review_outcome": "dismissed"},
    )
    _publish_review_queue_update(updated, action="rejected")
    return updated


def get_review_queue_counts() -> Dict[str, int]:
    deps = _get_dependencies()
    return deps["get_human_review_queue_counts"](
        urgent_priority_threshold=_env_int("REVIEW_QUEUE_URGENT_PRIORITY_MAX", 80)
    )
