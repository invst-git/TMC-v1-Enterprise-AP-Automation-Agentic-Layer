from dataclasses import dataclass
from typing import Any, Dict, Optional

from exception_resolution_agent import resolve_match_exception
from invoice_db import update_invoice_matching_outcome
from po_matching import MatchCandidate, InvoiceMatchAnalysis, analyze_invoice_match, apply_match_analysis


def _get_agent_db_ops():
    from agent_db import (
        create_completed_agent_task,
        create_human_review_item,
        record_agent_decision,
        set_workflow_state,
        update_human_review_item,
    )

    return create_completed_agent_task, record_agent_decision, create_human_review_item, set_workflow_state, update_human_review_item


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
    resolution_packet: Optional[Dict[str, Any]] = None

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
            "resolution_packet": self.resolution_packet,
        }


def _review_summary(analysis: InvoiceMatchAnalysis) -> str:
    mapping = {
        "deterministic_po_match": "The invoice matched an open purchase order within the default deterministic tolerance.",
        "resolved_fuzzy_po_match": "The exception resolution agent linked the invoice to an open purchase order through fuzzy PO matching.",
        "resolved_amount_tier_small": "The exception resolution agent auto-linked the invoice through the small-variance amount recovery tier.",
        "resolved_amount_tier_moderate": "The exception resolution agent auto-linked the invoice through the moderate-variance amount recovery tier after checking vendor precedent.",
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
    if analysis.decision == "matched_auto":
        return mapping.get(analysis.reason, "The invoice was recovered and matched automatically.")
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
    resolution_packet: Optional[Dict[str, Any]],
) -> Optional[str]:
    try:
        create_completed_agent_task, record_agent_decision, _, _, _ = _get_agent_db_ops()
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
                "resolution_packet": resolution_packet,
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
                "resolution_packet": resolution_packet,
            },
        )
        return task["id"]
    except Exception:
        return None


def _analysis_from_dict(data: Optional[Dict[str, Any]]) -> Optional[InvoiceMatchAnalysis]:
    if not data:
        return None
    candidates = []
    for candidate in data.get("candidates") or []:
        candidates.append(
            MatchCandidate(
                po_id=str(candidate.get("po_id") or ""),
                po_number=str(candidate.get("po_number") or ""),
                total_amount=candidate.get("total_amount"),
                currency=candidate.get("currency"),
                vendor_id=candidate.get("vendor_id"),
                diff=candidate.get("diff"),
                vendor_match=bool(candidate.get("vendor_match")),
                currency_match=bool(candidate.get("currency_match")),
                within_tolerance=bool(candidate.get("within_tolerance")),
                confidence=float(candidate.get("confidence") or 0.0),
                eligibility_reason=str(candidate.get("eligibility_reason") or ""),
            )
        )
    return InvoiceMatchAnalysis(
        invoice_id=str(data.get("invoice_id") or ""),
        invoice_status=str(data.get("invoice_status") or ""),
        invoice_po_number=data.get("invoice_po_number"),
        invoice_total=data.get("invoice_total"),
        invoice_currency=data.get("invoice_currency"),
        invoice_vendor_id=data.get("invoice_vendor_id"),
        decision=str(data.get("decision") or ""),
        reason=str(data.get("reason") or ""),
        confidence=float(data.get("confidence") or 0.0),
        best_po_id=data.get("best_po_id"),
        best_diff=data.get("best_diff"),
        candidates=candidates,
    )


def _enqueue_exception_resolution_task(
    *,
    invoice_id: str,
    source_document_id: Optional[str],
    analysis: InvoiceMatchAnalysis,
    amount_tolerance: float,
    percent_tolerance: float,
) -> Dict[str, Any]:
    from workflow_task_queue import enqueue_workflow_task

    return enqueue_workflow_task(
        task_type="matching.resolve_exception",
        entity_type="invoice",
        entity_id=invoice_id,
        source_document_id=source_document_id,
        priority=65,
        dedupe_key=f"invoice:{invoice_id}:matching.resolve_exception",
        payload={
            "invoice_id": invoice_id,
            "source_document_id": source_document_id,
            "analysis": analysis.to_dict(),
            "amount_tolerance": amount_tolerance,
            "percent_tolerance": percent_tolerance,
        },
    )


def evaluate_invoice_match_for_worker(
    task_id: str,
    invoice_id: str,
    *,
    source_document_id: Optional[str] = None,
    amount_tolerance: float = 1.0,
    percent_tolerance: float = 0.02,
) -> MatchingOutcome:
    _, record_agent_decision, _, set_workflow_state, _ = _get_agent_db_ops()
    analysis = analyze_invoice_match(
        invoice_id,
        amount_tolerance=amount_tolerance,
        percent_tolerance=percent_tolerance,
    )
    matched_po_id: Optional[str] = None
    workflow_state: Optional[str]
    invoice_status: Optional[str] = None
    resolution_task_id: Optional[str] = None
    reasoning = _review_summary(analysis)
    decision = analysis.decision

    if analysis.decision == "matched_auto":
        matched_po_id = apply_match_analysis(invoice_id, analysis)
        invoice_status = "matched_auto"
        update_invoice_matching_outcome(
            invoice_id,
            status=invoice_status,
            matched_po_id=matched_po_id,
            confidence=analysis.confidence,
        )
        workflow_state = _workflow_state_for_analysis(analysis)
    elif analysis.reason == "invoice_not_found":
        workflow_state = "matching_failed"
        reasoning = "Invoice was not found when the worker attempted deterministic PO matching."
        decision = "matching_failed"
    else:
        resolution_task = _enqueue_exception_resolution_task(
            invoice_id=invoice_id,
            source_document_id=source_document_id,
            analysis=analysis,
            amount_tolerance=amount_tolerance,
            percent_tolerance=percent_tolerance,
        )
        resolution_task_id = resolution_task["id"]
        workflow_state = "resolving_exception"
        decision = "queued_exception_resolution"
        reasoning = (
            f"{_review_summary(analysis)} Automated exception recovery was queued "
            "before any human escalation."
        )

    set_workflow_state(
        "invoice",
        invoice_id,
        workflow_state,
        current_stage="matching",
        confidence=analysis.confidence,
        event_type="po_matching_completed",
        reason=reasoning,
        metadata={
            "invoice_id": invoice_id,
            "matched_po_id": matched_po_id,
            "analysis": analysis.to_dict(),
            "resolution_task_id": resolution_task_id,
        },
    )
    record_agent_decision(
        task_id=task_id,
        entity_type="invoice",
        entity_id=invoice_id,
        agent_name="baseline_po_matcher",
        model_name="deterministic_rules_v1",
        prompt_version="deterministic_rules_v1",
        decision_type="po_matching",
        decision=decision,
        confidence=analysis.confidence,
        reasoning_summary=reasoning,
        metadata={
            "analysis": analysis.to_dict(),
            "matched_po_id": matched_po_id,
            "resolution_task_id": resolution_task_id,
        },
    )
    return MatchingOutcome(
        invoice_id=invoice_id,
        decision=decision,
        reason=analysis.reason,
        matched_po_id=matched_po_id,
        confidence=analysis.confidence,
        review_item_id=None,
        workflow_state=workflow_state,
        task_id=task_id,
        invoice_status=invoice_status,
        resolution_packet={"resolution_task_id": resolution_task_id} if resolution_task_id else None,
    )


def resolve_match_exception_for_worker(
    task_id: str,
    invoice_id: str,
    *,
    source_document_id: Optional[str] = None,
    analysis_data: Optional[Dict[str, Any]] = None,
    amount_tolerance: float = 1.0,
    percent_tolerance: float = 0.02,
) -> MatchingOutcome:
    _, record_agent_decision, create_human_review_item, set_workflow_state, update_human_review_item = _get_agent_db_ops()
    analysis = _analysis_from_dict(analysis_data) or analyze_invoice_match(
        invoice_id,
        amount_tolerance=amount_tolerance,
        percent_tolerance=percent_tolerance,
    )
    resolution_result = resolve_match_exception(
        invoice_id,
        analysis,
        source_document_id=source_document_id,
        amount_tolerance=amount_tolerance,
        percent_tolerance=percent_tolerance,
    )
    resolution_packet = resolution_result.to_review_packet()
    final_analysis = resolution_result.final_analysis
    matched_po_id = resolution_result.matched_po_id if resolution_result.resolved else None
    review_item_id: Optional[str] = None
    invoice_status = _status_for_analysis(final_analysis)

    if invoice_status:
        update_invoice_matching_outcome(
            invoice_id,
            status=invoice_status,
            matched_po_id=matched_po_id,
            confidence=final_analysis.confidence,
        )

    if final_analysis.decision != "matched_auto":
        review_metadata = {
            "summary": _review_summary(final_analysis),
            "analysis": final_analysis.to_dict(),
            "resolution_packet": resolution_packet,
            "recommended_action": resolution_result.recommended_action,
        }
        review_item = create_human_review_item(
            entity_type="invoice",
            entity_id=invoice_id,
            source_document_id=source_document_id,
            invoice_id=invoice_id,
            queue_name="po_matching",
            review_reason=final_analysis.reason,
            metadata=review_metadata,
        )
        review_item_id = review_item["id"]
        update_human_review_item(
            review_item_id,
            metadata=review_metadata,
        )

    workflow_state = _workflow_state_for_analysis(final_analysis)
    set_workflow_state(
        "invoice",
        invoice_id,
        workflow_state,
        current_stage="matching",
        confidence=final_analysis.confidence,
        event_type="exception_resolution_completed",
        reason=_review_summary(final_analysis),
        metadata={
            "invoice_id": invoice_id,
            "matched_po_id": matched_po_id,
            "review_item_id": review_item_id,
            "analysis": final_analysis.to_dict(),
            "resolution_packet": resolution_packet,
        },
    )
    record_agent_decision(
        task_id=task_id,
        entity_type="invoice",
        entity_id=invoice_id,
        agent_name="baseline_po_matcher",
        model_name="deterministic_rules_v1",
        prompt_version="deterministic_rules_v1",
        decision_type="po_matching",
        decision=final_analysis.decision,
        confidence=final_analysis.confidence,
        reasoning_summary=_review_summary(final_analysis),
        metadata={
            "analysis": final_analysis.to_dict(),
            "review_item_id": review_item_id,
            "resolution_packet": resolution_packet,
        },
    )
    return MatchingOutcome(
        invoice_id=invoice_id,
        decision=final_analysis.decision,
        reason=final_analysis.reason,
        matched_po_id=matched_po_id,
        confidence=final_analysis.confidence,
        review_item_id=review_item_id,
        workflow_state=workflow_state,
        task_id=task_id,
        invoice_status=invoice_status,
        resolution_packet=resolution_packet,
    )


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
    resolution_packet: Optional[Dict[str, Any]] = None

    if analysis.decision == "matched_auto":
        matched_po_id = apply_match_analysis(invoice_id, analysis)
    elif analysis.reason != "invoice_not_found":
        resolution_result = resolve_match_exception(
            invoice_id,
            analysis,
            source_document_id=source_document_id,
            amount_tolerance=amount_tolerance,
            percent_tolerance=percent_tolerance,
        )
        resolution_packet = resolution_result.to_review_packet()
        analysis = resolution_result.final_analysis
        if resolution_result.resolved:
            matched_po_id = resolution_result.matched_po_id
        invoice_status = _status_for_analysis(analysis)
        if invoice_status:
            update_invoice_matching_outcome(
                invoice_id,
                status=invoice_status,
                matched_po_id=matched_po_id,
                confidence=analysis.confidence,
            )
        if analysis.decision != "matched_auto":
            try:
                _, _, create_human_review_item, _, update_human_review_item = _get_agent_db_ops()
                review_metadata = {
                    "summary": _review_summary(analysis),
                    "analysis": analysis.to_dict(),
                    "resolution_packet": resolution_packet,
                    "recommended_action": resolution_result.recommended_action,
                }
                review_item = create_human_review_item(
                    entity_type="invoice",
                    entity_id=invoice_id,
                    source_document_id=source_document_id,
                    invoice_id=invoice_id,
                    queue_name="po_matching",
                    review_reason=analysis.reason,
                    metadata=review_metadata,
                )
                review_item_id = review_item["id"]
                update_human_review_item(
                    review_item_id,
                    metadata=review_metadata,
                )
            except Exception:
                review_item_id = None

    workflow_state = _workflow_state_for_analysis(analysis)
    try:
        _, _, _, set_workflow_state, _ = _get_agent_db_ops()
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
                "resolution_packet": resolution_packet,
            },
        )
    except Exception:
        pass

    task_id = _record_matching_decision(invoice_id, source_document_id, analysis, review_item_id, resolution_packet)
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
        resolution_packet=resolution_packet,
    )
