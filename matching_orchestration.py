from dataclasses import dataclass
from typing import Any, Dict, Optional

from invoice_db import update_invoice_matching_outcome
from po_matching import InvoiceMatchAnalysis, analyze_invoice_match, apply_match_analysis


def _get_agent_db_ops():
    from agent_db import create_completed_agent_task, create_human_review_item, record_agent_decision, set_workflow_state

    return create_completed_agent_task, record_agent_decision, create_human_review_item, set_workflow_state


@dataclass
class MatchingOutcome:
    invoice_id: str
    decision: str
    reason: str
    matched_po_id: Optional[str] = None
    confidence: float = 0.0
    review_item_id: Optional[str] = None
    workflow_state: Optional[str] = None
    task_id: Optional[str] = None
    invoice_status: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "invoice_id": self.invoice_id,
            "decision": self.decision,
            "reason": self.reason,
            "matched_po_id": self.matched_po_id,
            "confidence": self.confidence,
            "review_item_id": self.review_item_id,
            "workflow_state": self.workflow_state,
            "task_id": self.task_id,
            "invoice_status": self.invoice_status,
        }


def _review_summary(analysis: InvoiceMatchAnalysis) -> str:
    mapping = {
        "vendor_mismatch": "Invoice vendor does not match the selected vendor.",
        "missing_po_number": "Invoice is missing a PO number, so deterministic matching could not run.",
        "missing_invoice_total": "Invoice is missing a total amount, so deterministic matching could not run.",
        "no_open_po_candidates": "No open purchase order was found for the invoice PO number.",
        "candidate_vendor_mismatch": "Matching PO candidates exist, but the vendor does not align.",
        "candidate_currency_mismatch": "Matching PO candidates exist, but the currency does not align.",
        "candidate_vendor_and_currency_mismatch": "Matching PO candidates exist, but both vendor and currency are inconsistent.",
        "candidate_missing_po_total": "Matching PO candidates exist, but PO totals are missing.",
        "amount_outside_tolerance": "A PO candidate exists, but the invoice total is outside allowed tolerance.",
        "invoice_not_found": "Invoice was not found during PO matching.",
    }
    return mapping.get(analysis.reason, "PO matching requires human review.")


def _status_for_analysis(analysis: InvoiceMatchAnalysis) -> Optional[str]:
    if analysis.decision == "matched_auto":
        return "matched_auto"
    if analysis.reason == "vendor_mismatch":
        return "vendor_mismatch"
    if analysis.reason == "no_open_po_candidates":
        return "unmatched"
    if analysis.reason == "invoice_not_found":
        return None
    return "needs_review"


def _workflow_state_for_analysis(analysis: InvoiceMatchAnalysis) -> str:
    if analysis.decision == "matched_auto":
        return "matched_auto"
    if analysis.reason == "vendor_mismatch":
        return "vendor_mismatch"
    if analysis.reason == "no_open_po_candidates":
        return "unmatched"
    return "needs_review"


def _record_matching_decision(
    invoice_id: str,
    source_document_id: Optional[str],
    analysis: InvoiceMatchAnalysis,
    review_item_id: Optional[str],
) -> Optional[str]:
    try:
        create_completed_agent_task, record_agent_decision, _, _ = _get_agent_db_ops()
        task = create_completed_agent_task(
            task_type="matching.evaluate",
            entity_type="invoice",
            entity_id=invoice_id,
            source_document_id=source_document_id,
            priority=60,
            payload={
                "invoice_id": invoice_id,
                "source_document_id": source_document_id,
                "invoice_po_number": analysis.invoice_po_number,
                "invoice_total": analysis.invoice_total,
                "invoice_currency": analysis.invoice_currency,
                "invoice_vendor_id": analysis.invoice_vendor_id,
            },
            result={
                "decision": analysis.decision,
                "reason": analysis.reason,
                "matched_po_id": analysis.best_po_id,
                "confidence": analysis.confidence,
                "review_item_id": review_item_id,
            },
        )
        record_agent_decision(
            task_id=task["id"],
            entity_type="invoice",
            entity_id=invoice_id,
            agent_name="baseline_po_matcher",
            model_name="deterministic_rules_v1",
            prompt_version="deterministic_rules_v1",
            decision_type="po_matching",
            decision=analysis.decision,
            confidence=analysis.confidence,
            reasoning_summary=_review_summary(analysis),
            metadata={
                "analysis": analysis.to_dict(),
                "review_item_id": review_item_id,
            },
        )
        return task["id"]
    except Exception:
        return None


def match_invoice_with_review(
    invoice_id: str,
    *,
    source_document_id: Optional[str] = None,
    amount_tolerance: float = 1.0,
    percent_tolerance: float = 0.02,
) -> MatchingOutcome:
    analysis = analyze_invoice_match(
        invoice_id,
        amount_tolerance=amount_tolerance,
        percent_tolerance=percent_tolerance,
    )
    review_item_id: Optional[str] = None
    matched_po_id: Optional[str] = None

    if analysis.decision == "matched_auto":
        matched_po_id = apply_match_analysis(invoice_id, analysis)
    elif analysis.reason != "invoice_not_found":
        invoice_status = _status_for_analysis(analysis)
        if invoice_status:
            update_invoice_matching_outcome(
                invoice_id,
                status=invoice_status,
                confidence=analysis.confidence,
            )
        try:
            _, _, create_human_review_item, _ = _get_agent_db_ops()
            review_item = create_human_review_item(
                entity_type="invoice",
                entity_id=invoice_id,
                source_document_id=source_document_id,
                invoice_id=invoice_id,
                queue_name="po_matching",
                review_reason=analysis.reason,
                metadata={
                    "summary": _review_summary(analysis),
                    "analysis": analysis.to_dict(),
                },
            )
            review_item_id = review_item["id"]
        except Exception:
            review_item_id = None

    workflow_state = _workflow_state_for_analysis(analysis)
    try:
        _, _, _, set_workflow_state = _get_agent_db_ops()
        set_workflow_state(
            "invoice",
            invoice_id,
            workflow_state,
            current_stage="matching",
            confidence=analysis.confidence,
            event_type="po_matching_completed",
            reason=_review_summary(analysis),
            metadata={
                "invoice_id": invoice_id,
                "matched_po_id": matched_po_id,
                "review_item_id": review_item_id,
                "analysis": analysis.to_dict(),
            },
        )
    except Exception:
        pass

    task_id = _record_matching_decision(invoice_id, source_document_id, analysis, review_item_id)
    return MatchingOutcome(
        invoice_id=invoice_id,
        decision=analysis.decision,
        reason=analysis.reason,
        matched_po_id=matched_po_id,
        confidence=analysis.confidence,
        review_item_id=review_item_id,
        workflow_state=workflow_state,
        task_id=task_id,
        invoice_status=_status_for_analysis(analysis),
    )
