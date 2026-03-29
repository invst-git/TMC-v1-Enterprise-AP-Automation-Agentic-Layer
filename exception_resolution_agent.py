import datetime
import json
import os
import re
from dataclasses import dataclass, field
from difflib import SequenceMatcher
from typing import Any, Dict, List, Optional

from db import get_conn
from po_matching import InvoiceMatchAnalysis, analyze_invoice_match


PO_RELATED_REASONS = {
    "missing_po_number",
    "no_open_po_candidates",
    "amount_outside_tolerance",
    "candidate_missing_po_total",
}
VENDOR_RELATED_REASONS = {
    "vendor_mismatch",
    "candidate_vendor_mismatch",
    "candidate_vendor_and_currency_mismatch",
}


@dataclass
class RecoveryAttempt:
    path_name: str
    outcome: str
    summary: str
    findings: Dict[str, Any] = field(default_factory=dict)
    confidence: float = 0.0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "path_name": self.path_name,
            "outcome": self.outcome,
            "summary": self.summary,
            "findings": self.findings,
            "confidence": self.confidence,
        }


@dataclass
class ExceptionResolutionResult:
    invoice_id: str
    initial_analysis: InvoiceMatchAnalysis
    final_analysis: InvoiceMatchAnalysis
    attempts: List[RecoveryAttempt] = field(default_factory=list)
    matched_po_id: Optional[str] = None

    @property
    def resolved(self) -> bool:
        return self.final_analysis.decision == "matched_auto" and bool(self.matched_po_id or self.final_analysis.best_po_id)

    @property
    def recommended_action(self) -> str:
        if self.resolved:
            return "No human action is required. The exception resolution agent recovered this invoice automatically."
        mapping = {
            "missing_po_number": "Review the invoice and confirm the correct purchase order number before approving it.",
            "no_open_po_candidates": "Verify whether the invoice belongs to a closed, missing, or different purchase order.",
            "amount_outside_tolerance": "Compare the invoice total against the candidate purchase order totals and decide whether the variance is acceptable.",
            "vendor_mismatch": "Confirm the correct vendor identity before matching or paying this invoice.",
            "candidate_vendor_mismatch": "Confirm the correct vendor identity before matching or paying this invoice.",
            "candidate_vendor_and_currency_mismatch": "Confirm the vendor and currency before matching or paying this invoice.",
            "candidate_missing_po_total": "Review the candidate purchase order totals and confirm which purchase order should be linked.",
            "missing_invoice_total": "Verify the invoice totals and correct the extracted amount before continuing.",
            "candidate_currency_mismatch": "Confirm the invoice currency before linking it to a purchase order.",
        }
        return mapping.get(
            self.final_analysis.reason,
            "Review the attempted recovery steps and decide how this invoice should be matched.",
        )

    def to_review_packet(self) -> Dict[str, Any]:
        return {
            "invoice_id": self.invoice_id,
            "initial_analysis": self.initial_analysis.to_dict(),
            "final_analysis": self.final_analysis.to_dict(),
            "attempt_count": len(self.attempts),
            "attempts": [attempt.to_dict() for attempt in self.attempts],
            "matched_po_id": self.matched_po_id or self.final_analysis.best_po_id,
            "recommended_action": self.recommended_action,
        }


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except Exception:
        return default


def _normalize_text(value: Any) -> str:
    return str(value or "").strip()


def _normalize_token(value: Any) -> str:
    return re.sub(r"[^A-Za-z0-9]+", "", _normalize_text(value)).upper()


def _ocr_similarity_token(value: Any) -> str:
    return (
        _normalize_token(value)
        .replace("O", "0")
        .replace("I", "1")
        .replace("L", "1")
    )


def _string_similarity(left: Any, right: Any) -> float:
    normalized_left = _normalize_token(left)
    normalized_right = _normalize_token(right)
    if not normalized_left or not normalized_right:
        return 0.0
    if normalized_left == normalized_right:
        return 1.0
    raw_similarity = SequenceMatcher(None, normalized_left, normalized_right).ratio()
    ocr_similarity = SequenceMatcher(None, _ocr_similarity_token(left), _ocr_similarity_token(right)).ratio()
    return round(max(raw_similarity, ocr_similarity), 4)


def _parse_date(value: Any) -> Optional[datetime.date]:
    if not value:
        return None
    if isinstance(value, datetime.date):
        return value
    try:
        return datetime.date.fromisoformat(str(value))
    except Exception:
        return None


def _to_float(value: Any) -> Optional[float]:
    if value in {None, ""}:
        return None
    try:
        return float(value)
    except Exception:
        return None


def _amount_diff(invoice_total: Optional[float], po_total: Optional[float]) -> Optional[float]:
    if invoice_total is None or po_total is None:
        return None
    return abs(float(invoice_total) - float(po_total))


def _within_amount_tolerance(
    invoice_total: Optional[float],
    diff: Optional[float],
    *,
    amount_tolerance: float,
    percent_tolerance: float,
) -> bool:
    if invoice_total is None or diff is None:
        return False
    return diff <= float(amount_tolerance) or diff <= abs(float(invoice_total)) * float(percent_tolerance)


def _load_invoice_context(invoice_id: str) -> Optional[Dict[str, Any]]:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                  i.id,
                  i.status,
                  i.po_number,
                  i.total_amount,
                  i.currency,
                  i.vendor_id,
                  i.supplier_name,
                  i.supplier_tax_id,
                  i.file_path,
                  i.fields_json_path,
                  i.invoice_number,
                  i.invoice_date,
                  i.matched_po_id,
                  i.confidence
                FROM invoices AS i
                WHERE i.id = %s
                LIMIT 1
                """,
                (invoice_id,),
            )
            row = cur.fetchone()
            if not row:
                return None
            cur.execute(
                """
                SELECT DISTINCT po_number
                FROM invoice_lines
                WHERE invoice_id = %s
                  AND po_number IS NOT NULL
                  AND btrim(po_number) <> ''
                ORDER BY po_number
                """,
                (invoice_id,),
            )
            line_po_numbers = [value for (value,) in cur.fetchall() if value]

    return {
        "id": str(row[0]),
        "status": row[1] or "",
        "po_number": row[2] or "",
        "total_amount": float(row[3]) if row[3] is not None else None,
        "currency": row[4] or "USD",
        "vendor_id": str(row[5]) if row[5] else None,
        "supplier_name": row[6] or "",
        "supplier_tax_id": row[7] or "",
        "file_path": row[8] or "",
        "fields_json_path": row[9] or "",
        "invoice_number": row[10] or "",
        "invoice_date": row[11].isoformat() if row[11] else None,
        "matched_po_id": str(row[12]) if row[12] else None,
        "confidence": float(row[13]) if row[13] is not None else None,
        "line_po_numbers": line_po_numbers,
    }


def _load_open_purchase_orders(vendor_id: Optional[str]) -> List[Dict[str, Any]]:
    if not vendor_id:
        return []
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, po_number, total_amount, currency, vendor_id
                FROM purchase_orders
                WHERE vendor_id = %s
                  AND status IN ('open', 'partially_received')
                ORDER BY created_at DESC NULLS LAST, id DESC
                """,
                (vendor_id,),
            )
            rows = cur.fetchall()
    purchase_orders: List[Dict[str, Any]] = []
    for row in rows:
        purchase_orders.append(
            {
                "po_id": str(row[0]),
                "po_number": row[1] or "",
                "total_amount": float(row[2]) if row[2] is not None else None,
                "currency": row[3] or "USD",
                "vendor_id": str(row[4]) if row[4] else None,
            }
        )
    return purchase_orders


def _load_vendor_candidates() -> List[Dict[str, Any]]:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, name, tax_id
                FROM vendors
                ORDER BY name
                """
            )
            rows = cur.fetchall()
    return [
        {
            "vendor_id": str(row[0]),
            "name": row[1] or "",
            "tax_id": row[2] or "",
        }
        for row in rows
    ]


def _vendor_has_prior_precedent(vendor_id: Optional[str], current_invoice_id: str) -> bool:
    if not vendor_id:
        return False
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT 1
                FROM invoices
                WHERE vendor_id = %s
                  AND id <> %s
                  AND matched_po_id IS NOT NULL
                  AND status IN ('matched_auto', 'ready_for_payment', 'payment_pending', 'paid')
                LIMIT 1
                """,
                (vendor_id, current_invoice_id),
            )
            return bool(cur.fetchone())


def _get_agent_db_ops():
    from agent_db import create_completed_agent_task, record_agent_decision

    return create_completed_agent_task, record_agent_decision


def _record_attempt_best_effort(
    invoice_id: str,
    source_document_id: Optional[str],
    attempt: RecoveryAttempt,
) -> None:
    try:
        create_completed_agent_task, record_agent_decision = _get_agent_db_ops()
        task = create_completed_agent_task(
            task_type="matching.recovery_attempt",
            entity_type="invoice",
            entity_id=invoice_id,
            source_document_id=source_document_id,
            priority=65,
            payload={
                "invoice_id": invoice_id,
                "path_name": attempt.path_name,
            },
            result=attempt.to_dict(),
        )
        decision_type = "matching_ocr_reextract" if attempt.path_name == "targeted_ocr_reextract" else f"recovery_attempt:{attempt.path_name}"
        record_agent_decision(
            task_id=task["id"],
            entity_type="invoice",
            entity_id=invoice_id,
            agent_name="exception_resolution_agent",
            model_name="deterministic_recovery_rules",
            prompt_version="v1",
            decision_type=decision_type,
            decision=attempt.outcome,
            confidence=attempt.confidence,
            reasoning_summary=attempt.summary,
            metadata=attempt.to_dict(),
        )
    except Exception:
        return None


def _append_attempt(
    attempts: List[RecoveryAttempt],
    invoice_id: str,
    source_document_id: Optional[str],
    attempt: RecoveryAttempt,
) -> RecoveryAttempt:
    attempts.append(attempt)
    _record_attempt_best_effort(invoice_id, source_document_id, attempt)
    return attempt


def _build_matched_analysis(
    context: Dict[str, Any],
    *,
    po_id: str,
    confidence: float,
    reason: str,
    diff: Optional[float],
) -> InvoiceMatchAnalysis:
    return InvoiceMatchAnalysis(
        invoice_id=context["id"],
        invoice_status="matched_auto",
        invoice_po_number=context.get("po_number") or None,
        invoice_total=context.get("total_amount"),
        invoice_currency=context.get("currency"),
        invoice_vendor_id=context.get("vendor_id"),
        decision="matched_auto",
        reason=reason,
        confidence=round(confidence, 4),
        best_po_id=po_id,
        best_diff=round(diff, 4) if diff is not None else None,
        candidates=[],
    )


def _update_invoice_po_resolution(
    invoice_id: str,
    *,
    po_id: str,
    po_number: str,
    confidence: float,
) -> None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE invoices
                SET
                  po_number = %s,
                  matched_po_id = %s,
                  status = 'matched_auto',
                  confidence = %s
                WHERE id = %s
                """,
                (po_number, po_id, round(confidence, 4), invoice_id),
            )


def _update_invoice_vendor_link(invoice_id: str, *, vendor_id: str, confidence: float) -> None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE invoices
                SET
                  vendor_id = %s,
                  status = 'unmatched',
                  confidence = GREATEST(COALESCE(confidence, 0), %s),
                  matched_po_id = NULL
                WHERE id = %s
                """,
                (vendor_id, round(confidence, 4), invoice_id),
            )


def _reconcile_invoice_vendor_status(invoice_id: str) -> Dict[str, Any]:
    context = _load_invoice_context(invoice_id)
    if not context:
        raise ValueError("Invoice not found")

    vendor_id = context.get("vendor_id")
    normalized_supplier_tax_id = _normalize_token(context.get("supplier_tax_id"))
    normalized_supplier_name = _normalize_text(context.get("supplier_name")).lower()

    with get_conn() as conn:
        with conn.cursor() as cur:
            if not vendor_id:
                if normalized_supplier_tax_id:
                    cur.execute(
                        "SELECT id FROM vendors WHERE tax_id = %s LIMIT 1",
                        (context.get("supplier_tax_id"),),
                    )
                    row = cur.fetchone()
                    if row:
                        vendor_id = str(row[0])
                if not vendor_id and normalized_supplier_name:
                    cur.execute(
                        "SELECT id FROM vendors WHERE lower(name) = lower(%s) LIMIT 1",
                        (context.get("supplier_name"),),
                    )
                    row = cur.fetchone()
                    if row:
                        vendor_id = str(row[0])

            next_status = context["status"] or "unmatched"
            if vendor_id:
                cur.execute(
                    "SELECT name, tax_id FROM vendors WHERE id = %s LIMIT 1",
                    (vendor_id,),
                )
                vendor_row = cur.fetchone()
                if vendor_row:
                    vendor_name = _normalize_text(vendor_row[0]).lower()
                    vendor_tax_id = _normalize_token(vendor_row[1])
                    mismatch = False
                    if normalized_supplier_tax_id and vendor_tax_id and normalized_supplier_tax_id != vendor_tax_id:
                        mismatch = True
                    elif not (normalized_supplier_tax_id and vendor_tax_id and normalized_supplier_tax_id == vendor_tax_id) and normalized_supplier_name and vendor_name and normalized_supplier_name != vendor_name:
                        mismatch = True
                    next_status = "vendor_mismatch" if mismatch else ("matched_auto" if context.get("matched_po_id") else "unmatched")
                else:
                    next_status = "unmatched"
            else:
                next_status = "unmatched"

            cur.execute(
                """
                UPDATE invoices
                SET
                  vendor_id = %s,
                  status = %s
                WHERE id = %s
                """,
                (vendor_id, next_status, invoice_id),
            )
    return _load_invoice_context(invoice_id) or context


def _vendor_match_confidence(context: Dict[str, Any], vendor_candidate: Dict[str, Any]) -> float:
    supplier_name = context.get("supplier_name")
    supplier_tax_id = context.get("supplier_tax_id")
    vendor_name = vendor_candidate.get("name")
    vendor_tax_id = vendor_candidate.get("tax_id")

    name_similarity = _string_similarity(supplier_name, vendor_name)
    supplier_tax = _normalize_token(supplier_tax_id)
    vendor_tax = _normalize_token(vendor_tax_id)
    tax_exact = bool(supplier_tax and vendor_tax and supplier_tax == vendor_tax)
    tax_conflict = bool(supplier_tax and vendor_tax and supplier_tax != vendor_tax)

    if tax_conflict:
        return 0.0
    if tax_exact and name_similarity >= 0.7:
        return 0.99
    if tax_exact:
        return 0.95
    if name_similarity >= 0.97:
        return 0.94
    if name_similarity >= 0.92:
        return 0.9
    return round(name_similarity * 0.88, 4)


def _attempt_vendor_fuzzy_match(context: Dict[str, Any]) -> RecoveryAttempt:
    if not context.get("supplier_name") and not context.get("supplier_tax_id"):
        return RecoveryAttempt(
            path_name="vendor_fuzzy_match",
            outcome="skipped",
            summary="Vendor fuzzy matching was skipped because the invoice has neither supplier name nor supplier tax id.",
        )

    candidates = []
    for vendor_candidate in _load_vendor_candidates():
        confidence = _vendor_match_confidence(context, vendor_candidate)
        if confidence <= 0:
            continue
        candidates.append(
            {
                "vendor_id": vendor_candidate["vendor_id"],
                "vendor_name": vendor_candidate["name"],
                "tax_id": vendor_candidate["tax_id"],
                "confidence": confidence,
            }
        )

    if not candidates:
        return RecoveryAttempt(
            path_name="vendor_fuzzy_match",
            outcome="failed",
            summary="Vendor fuzzy matching found no viable vendor candidates for this invoice.",
        )

    candidates.sort(key=lambda item: (-item["confidence"], item["vendor_name"]))
    best = candidates[0]
    runner_up_confidence = candidates[1]["confidence"] if len(candidates) > 1 else 0.0
    minimum_confidence = _env_float("EXCEPTION_RESOLUTION_VENDOR_FUZZY_MIN_CONFIDENCE", 0.9)
    minimum_gap = _env_float("EXCEPTION_RESOLUTION_VENDOR_FUZZY_MIN_GAP", 0.05)

    if best["confidence"] >= minimum_confidence and (best["confidence"] - runner_up_confidence) >= minimum_gap:
        _update_invoice_vendor_link(
            context["id"],
            vendor_id=best["vendor_id"],
            confidence=best["confidence"],
        )
        updated_context = _reconcile_invoice_vendor_status(context["id"])
        return RecoveryAttempt(
            path_name="vendor_fuzzy_match",
            outcome="resolved",
            summary=(
                f"Vendor fuzzy matching linked the invoice to vendor {best['vendor_name']} "
                f"with confidence {best['confidence']:.2f}."
            ),
            findings={
                "resolved_vendor_id": best["vendor_id"],
                "resolved_vendor_name": best["vendor_name"],
                "runner_up_confidence": runner_up_confidence,
                "updated_vendor_id": updated_context.get("vendor_id"),
            },
            confidence=best["confidence"],
        )

    return RecoveryAttempt(
        path_name="vendor_fuzzy_match",
        outcome="failed",
        summary="Vendor fuzzy matching did not reach the confidence needed for automatic relinking.",
        findings={
            "best_candidate": best,
            "runner_up_confidence": runner_up_confidence,
            "minimum_confidence": minimum_confidence,
            "minimum_gap": minimum_gap,
        },
        confidence=best["confidence"],
    )


def _fuzzy_po_reference(context: Dict[str, Any]) -> str:
    if _normalize_text(context.get("po_number")):
        return _normalize_text(context.get("po_number"))
    line_po_numbers = context.get("line_po_numbers") or []
    return _normalize_text(line_po_numbers[0]) if line_po_numbers else ""


def _attempt_fuzzy_po_match(context: Dict[str, Any]) -> RecoveryAttempt:
    po_reference = _fuzzy_po_reference(context)
    if not context.get("vendor_id"):
        return RecoveryAttempt(
            path_name="fuzzy_po_match",
            outcome="skipped",
            summary="Fuzzy PO matching was skipped because the invoice is not linked to a confirmed vendor yet.",
        )
    if not po_reference:
        return RecoveryAttempt(
            path_name="fuzzy_po_match",
            outcome="skipped",
            summary="Fuzzy PO matching was skipped because the invoice has no PO reference to compare.",
        )
    if context.get("total_amount") is None:
        return RecoveryAttempt(
            path_name="fuzzy_po_match",
            outcome="skipped",
            summary="Fuzzy PO matching was skipped because the invoice total is missing.",
        )

    minimum_similarity = _env_float("EXCEPTION_RESOLUTION_PO_FUZZY_MIN_SIMILARITY", 0.84)
    maximum_amount_tolerance = _env_float("EXCEPTION_RESOLUTION_FUZZY_PO_AMOUNT_TOLERANCE", 5.0)
    maximum_percent_tolerance = _env_float("EXCEPTION_RESOLUTION_FUZZY_PO_PERCENT_TOLERANCE", 0.03)

    candidates = []
    for purchase_order in _load_open_purchase_orders(context["vendor_id"]):
        diff = _amount_diff(context.get("total_amount"), purchase_order.get("total_amount"))
        similarity = _string_similarity(po_reference, purchase_order.get("po_number"))
        currency_match = not context.get("currency") or not purchase_order.get("currency") or context["currency"] == purchase_order["currency"]
        within_delta = _within_amount_tolerance(
            context.get("total_amount"),
            diff,
            amount_tolerance=maximum_amount_tolerance,
            percent_tolerance=maximum_percent_tolerance,
        )
        candidates.append(
            {
                **purchase_order,
                "similarity": similarity,
                "diff": diff,
                "currency_match": currency_match,
                "within_delta": within_delta,
            }
        )

    eligible = [
        candidate
        for candidate in candidates
        if candidate["currency_match"]
        and candidate["within_delta"]
        and candidate["similarity"] >= minimum_similarity
    ]
    eligible.sort(key=lambda item: (-item["similarity"], item["diff"] if item["diff"] is not None else float("inf"), item["po_number"]))
    if eligible:
        best = eligible[0]
        runner_up = eligible[1] if len(eligible) > 1 else None
        if not runner_up or (best["similarity"] - runner_up["similarity"] >= 0.04):
            confidence = min(0.99, 0.55 + (best["similarity"] * 0.3) + (0.14 if best["diff"] is not None and best["diff"] <= 1.0 else 0.07))
            _update_invoice_po_resolution(
                context["id"],
                po_id=best["po_id"],
                po_number=best["po_number"],
                confidence=confidence,
            )
            return RecoveryAttempt(
                path_name="fuzzy_po_match",
                outcome="resolved",
                summary=(
                    f"Fuzzy PO matching linked the invoice to PO {best['po_number']} "
                    f"with similarity {best['similarity']:.2f} and amount delta {best['diff'] or 0:.2f}."
                ),
                findings={
                    "po_id": best["po_id"],
                    "po_number": best["po_number"],
                    "similarity": best["similarity"],
                    "amount_delta": best["diff"],
                },
                confidence=round(confidence, 4),
            )

    top_candidate = candidates[0] if candidates else None
    return RecoveryAttempt(
        path_name="fuzzy_po_match",
        outcome="failed",
        summary="Fuzzy PO matching did not find a single open purchase order within the similarity and amount bounds.",
        findings={
            "po_reference": po_reference,
            "minimum_similarity": minimum_similarity,
            "candidate_count": len(candidates),
            "best_candidate": top_candidate,
        },
        confidence=round(top_candidate["similarity"], 4) if top_candidate else 0.0,
    )


def _rank_amount_tier(
    invoice_total: Optional[float],
    diff: Optional[float],
    *,
    small_amount_tolerance: float,
    small_percent_tolerance: float,
    moderate_amount_tolerance: float,
    moderate_percent_tolerance: float,
) -> Optional[str]:
    if _within_amount_tolerance(
        invoice_total,
        diff,
        amount_tolerance=small_amount_tolerance,
        percent_tolerance=small_percent_tolerance,
    ):
        return "small"
    if _within_amount_tolerance(
        invoice_total,
        diff,
        amount_tolerance=moderate_amount_tolerance,
        percent_tolerance=moderate_percent_tolerance,
    ):
        return "moderate"
    return None


def _attempt_amount_tolerance_recovery(context: Dict[str, Any]) -> RecoveryAttempt:
    if not context.get("vendor_id"):
        return RecoveryAttempt(
            path_name="amount_tolerance_tiers",
            outcome="skipped",
            summary="Tiered amount recovery was skipped because the invoice is not linked to a confirmed vendor yet.",
        )
    if context.get("total_amount") is None:
        return RecoveryAttempt(
            path_name="amount_tolerance_tiers",
            outcome="skipped",
            summary="Tiered amount recovery was skipped because the invoice total is missing.",
        )

    small_amount_tolerance = _env_float("EXCEPTION_RESOLUTION_SMALL_AMOUNT_TOLERANCE", 3.0)
    small_percent_tolerance = _env_float("EXCEPTION_RESOLUTION_SMALL_PERCENT_TOLERANCE", 0.02)
    moderate_amount_tolerance = _env_float("EXCEPTION_RESOLUTION_MODERATE_AMOUNT_TOLERANCE", 15.0)
    moderate_percent_tolerance = _env_float("EXCEPTION_RESOLUTION_MODERATE_PERCENT_TOLERANCE", 0.05)
    vendor_has_precedent = _vendor_has_prior_precedent(context["vendor_id"], context["id"])
    po_reference = _fuzzy_po_reference(context)

    candidates = []
    for purchase_order in _load_open_purchase_orders(context["vendor_id"]):
        currency_match = not context.get("currency") or not purchase_order.get("currency") or context["currency"] == purchase_order["currency"]
        if not currency_match:
            continue
        diff = _amount_diff(context.get("total_amount"), purchase_order.get("total_amount"))
        tier = _rank_amount_tier(
            context.get("total_amount"),
            diff,
            small_amount_tolerance=small_amount_tolerance,
            small_percent_tolerance=small_percent_tolerance,
            moderate_amount_tolerance=moderate_amount_tolerance,
            moderate_percent_tolerance=moderate_percent_tolerance,
        )
        if not tier:
            continue
        similarity = _string_similarity(po_reference, purchase_order.get("po_number")) if po_reference else 0.0
        candidates.append(
            {
                **purchase_order,
                "diff": diff,
                "tier": tier,
                "po_similarity": similarity,
            }
        )

    if not candidates:
        return RecoveryAttempt(
            path_name="amount_tolerance_tiers",
            outcome="failed",
            summary="Tiered amount recovery found no open purchase orders close enough in value to auto-link.",
            findings={
                "vendor_has_precedent": vendor_has_precedent,
                "small_amount_tolerance": small_amount_tolerance,
                "moderate_amount_tolerance": moderate_amount_tolerance,
            },
        )

    tier_rank = {"small": 0, "moderate": 1}
    candidates.sort(key=lambda item: (tier_rank[item["tier"]], item["diff"] if item["diff"] is not None else float("inf"), -item["po_similarity"], item["po_number"]))
    best = candidates[0]
    same_tier_candidates = [candidate for candidate in candidates if candidate["tier"] == best["tier"]]
    ambiguous = len(same_tier_candidates) > 1 and (
        same_tier_candidates[1]["diff"] == best["diff"]
        or abs((same_tier_candidates[1]["diff"] or 0.0) - (best["diff"] or 0.0)) <= 0.01
    )
    if ambiguous:
        return RecoveryAttempt(
            path_name="amount_tolerance_tiers",
            outcome="failed",
            summary="Tiered amount recovery found multiple equally plausible purchase orders, so it did not auto-link.",
            findings={
                "best_candidates": same_tier_candidates[:3],
                "vendor_has_precedent": vendor_has_precedent,
            },
            confidence=0.45,
        )

    if best["tier"] == "moderate" and not vendor_has_precedent:
        return RecoveryAttempt(
            path_name="amount_tolerance_tiers",
            outcome="failed",
            summary="Tiered amount recovery found only a moderate-variance candidate, and this vendor has no prior precedent for auto-accepting that variance.",
            findings={
                "best_candidate": best,
                "vendor_has_precedent": vendor_has_precedent,
            },
            confidence=0.55,
        )

    confidence = 0.9 if best["tier"] == "small" else 0.82
    _update_invoice_po_resolution(
        context["id"],
        po_id=best["po_id"],
        po_number=best["po_number"],
        confidence=confidence,
    )
    return RecoveryAttempt(
        path_name="amount_tolerance_tiers",
        outcome="resolved",
        summary=(
            f"Tiered amount recovery linked the invoice to PO {best['po_number']} in the {best['tier']} variance tier "
            f"with amount delta {best['diff'] or 0:.2f}."
        ),
        findings={
            "po_id": best["po_id"],
            "po_number": best["po_number"],
            "amount_delta": best["diff"],
            "variance_tier": best["tier"],
            "vendor_has_precedent": vendor_has_precedent,
        },
        confidence=confidence,
    )


def _load_json_file(path: str) -> Dict[str, Any]:
    if not path or not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as handle:
            data = json.load(handle)
            return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _metadata_path_for_json(json_path: str) -> str:
    return f"{json_path[:-5]}.meta.json" if json_path.lower().endswith(".json") else f"{json_path}.meta.json"


def _extract_numeric_confidence(value: Any) -> Optional[float]:
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        numeric = float(value)
        if 0.0 <= numeric <= 1.0:
            return numeric
        if 0.0 <= numeric <= 100.0:
            return numeric / 100.0
        return None
    if isinstance(value, dict):
        for key in ("confidence", "score", "ocr_confidence", "field_confidence", "probability"):
            if key in value:
                nested = _extract_numeric_confidence(value.get(key))
                if nested is not None:
                    return nested
        for nested_value in value.values():
            nested = _extract_numeric_confidence(nested_value)
            if nested is not None:
                return nested
        return None
    if isinstance(value, list):
        scores = [score for score in (_extract_numeric_confidence(item) for item in value) if score is not None]
        if scores:
            return sum(scores) / len(scores)
    return None


def _heuristic_field_confidence(field_name: str, value: Any) -> float:
    if value in {None, "", []}:
        return 0.18
    if field_name in {"total_amount", "subtotal_amount", "tax_amount", "shipping_amount", "discount_amount"}:
        return 0.78 if _to_float(value) is not None else 0.35
    if field_name in {"invoice_date", "due_date"}:
        return 0.76 if _parse_date(value) else 0.4
    if field_name == "po_number":
        return 0.82 if len(_normalize_token(value)) >= 4 else 0.45
    if field_name == "supplier_tax_id":
        return 0.86 if len(_normalize_token(value)) >= 6 else 0.4
    if field_name == "supplier_name":
        return 0.8 if len(_normalize_text(value).split()) >= 2 else 0.48
    if field_name == "currency":
        return 0.8 if len(_normalize_text(value)) == 3 else 0.5
    return 0.74


def _field_confidences(fields: Dict[str, Any], extraction_metadata: Dict[str, Any]) -> Dict[str, float]:
    confidences: Dict[str, float] = {}
    for field_name, value in fields.items():
        metadata_value = extraction_metadata.get(field_name) if isinstance(extraction_metadata, dict) else None
        metadata_confidence = _extract_numeric_confidence(metadata_value)
        confidences[field_name] = round(
            metadata_confidence if metadata_confidence is not None else _heuristic_field_confidence(field_name, value),
            4,
        )
    return confidences


def _fields_from_context(context: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "supplier_name": context.get("supplier_name"),
        "supplier_tax_id": context.get("supplier_tax_id"),
        "invoice_number": context.get("invoice_number"),
        "invoice_date": context.get("invoice_date"),
        "currency": context.get("currency"),
        "total_amount": context.get("total_amount"),
        "po_number": context.get("po_number"),
    }


def _low_confidence_target_fields(analysis: InvoiceMatchAnalysis, context: Dict[str, Any]) -> List[str]:
    reason_to_fields = {
        "vendor_mismatch": ["supplier_name", "supplier_tax_id"],
        "candidate_vendor_mismatch": ["supplier_name", "supplier_tax_id"],
        "candidate_vendor_and_currency_mismatch": ["supplier_name", "supplier_tax_id", "currency"],
        "missing_po_number": ["po_number"],
        "no_open_po_candidates": ["po_number"],
        "amount_outside_tolerance": ["po_number", "total_amount"],
        "candidate_missing_po_total": ["total_amount"],
        "missing_invoice_total": ["total_amount", "subtotal_amount", "tax_amount", "shipping_amount", "discount_amount"],
        "candidate_currency_mismatch": ["currency", "po_number"],
    }
    candidate_fields = reason_to_fields.get(analysis.reason, [])
    base_fields = _load_json_file(context.get("fields_json_path")) or _fields_from_context(context)
    extraction_metadata = _load_json_file(_metadata_path_for_json(context.get("fields_json_path", "")))
    confidences = _field_confidences(base_fields, extraction_metadata)
    threshold = _env_float("EXCEPTION_RESOLUTION_OCR_FIELD_CONFIDENCE_THRESHOLD", 0.72)
    return [field_name for field_name in candidate_fields if confidences.get(field_name, _heuristic_field_confidence(field_name, base_fields.get(field_name))) < threshold]


def _has_prior_ocr_retry(invoice_id: str) -> bool:
    try:
        with get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT to_regclass('public.agent_decisions')")
                row = cur.fetchone()
                if not row or row[0] is None:
                    return False
                cur.execute(
                    """
                    SELECT 1
                    FROM agent_decisions
                    WHERE entity_type = 'invoice'
                      AND entity_id = %s
                      AND decision_type = 'matching_ocr_reextract'
                    LIMIT 1
                    """,
                    (invoice_id,),
                )
                return bool(cur.fetchone())
    except Exception:
        return False


def _apply_reextracted_fields_update(
    invoice_id: str,
    current_context: Dict[str, Any],
    new_fields: Dict[str, Any],
    new_json_path: str,
) -> None:
    updates = {
        "supplier_name": new_fields.get("supplier_name") or current_context.get("supplier_name"),
        "supplier_tax_id": new_fields.get("supplier_tax_id") or current_context.get("supplier_tax_id"),
        "supplier_address": new_fields.get("supplier_address"),
        "buyer_name": new_fields.get("buyer_name"),
        "company_code": new_fields.get("company_code"),
        "cost_center": new_fields.get("cost_center"),
        "invoice_number": new_fields.get("invoice_number") or current_context.get("invoice_number"),
        "invoice_date": _parse_date(new_fields.get("invoice_date")) or _parse_date(current_context.get("invoice_date")),
        "due_date": _parse_date(new_fields.get("due_date")),
        "currency": new_fields.get("currency") or current_context.get("currency"),
        "payment_terms": new_fields.get("payment_terms"),
        "subtotal_amount": _to_float(new_fields.get("subtotal_amount")),
        "tax_amount": _to_float(new_fields.get("tax_amount")),
        "shipping_amount": _to_float(new_fields.get("shipping_amount")),
        "discount_amount": _to_float(new_fields.get("discount_amount")),
        "total_amount": _to_float(new_fields.get("total_amount")) if _to_float(new_fields.get("total_amount")) is not None else current_context.get("total_amount"),
        "po_number": new_fields.get("po_number") or current_context.get("po_number"),
        "bank_account": new_fields.get("bank_account"),
        "swift_bic": new_fields.get("swift_bic"),
        "remittance_reference": new_fields.get("remittance_reference"),
        "invoice_type": new_fields.get("invoice_type"),
    }
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE invoices
                SET
                  supplier_name = %s,
                  supplier_tax_id = %s,
                  supplier_address = COALESCE(%s, supplier_address),
                  buyer_name = COALESCE(%s, buyer_name),
                  company_code = COALESCE(%s, company_code),
                  cost_center = COALESCE(%s, cost_center),
                  invoice_number = %s,
                  invoice_date = %s,
                  due_date = COALESCE(%s, due_date),
                  currency = %s,
                  payment_terms = COALESCE(%s, payment_terms),
                  subtotal_amount = COALESCE(%s, subtotal_amount),
                  tax_amount = COALESCE(%s, tax_amount),
                  shipping_amount = COALESCE(%s, shipping_amount),
                  discount_amount = COALESCE(%s, discount_amount),
                  total_amount = %s,
                  po_number = %s,
                  bank_account = COALESCE(%s, bank_account),
                  swift_bic = COALESCE(%s, swift_bic),
                  remittance_reference = COALESCE(%s, remittance_reference),
                  invoice_type = COALESCE(%s, invoice_type),
                  fields_json_path = %s
                WHERE id = %s
                """,
                (
                    updates["supplier_name"],
                    updates["supplier_tax_id"],
                    updates["supplier_address"],
                    updates["buyer_name"],
                    updates["company_code"],
                    updates["cost_center"],
                    updates["invoice_number"],
                    updates["invoice_date"],
                    updates["due_date"],
                    updates["currency"],
                    updates["payment_terms"],
                    updates["subtotal_amount"],
                    updates["tax_amount"],
                    updates["shipping_amount"],
                    updates["discount_amount"],
                    updates["total_amount"],
                    updates["po_number"],
                    updates["bank_account"],
                    updates["swift_bic"],
                    updates["remittance_reference"],
                    updates["invoice_type"],
                    new_json_path,
                    invoice_id,
                ),
            )


def _attempt_targeted_reextract(context: Dict[str, Any], analysis: InvoiceMatchAnalysis) -> RecoveryAttempt:
    if not context.get("file_path"):
        return RecoveryAttempt(
            path_name="targeted_ocr_reextract",
            outcome="skipped",
            summary="Targeted OCR re-extraction was skipped because the original invoice file path is missing.",
        )
    if _has_prior_ocr_retry(context["id"]):
        return RecoveryAttempt(
            path_name="targeted_ocr_reextract",
            outcome="skipped",
            summary="Targeted OCR re-extraction was skipped because this invoice has already been retried once.",
        )

    low_confidence_fields = _low_confidence_target_fields(analysis, context)
    if not low_confidence_fields:
        return RecoveryAttempt(
            path_name="targeted_ocr_reextract",
            outcome="skipped",
            summary="Targeted OCR re-extraction was skipped because no relevant low-confidence fields were detected.",
        )

    current_fields = _load_json_file(context.get("fields_json_path")) or _fields_from_context(context)
    current_metadata = _load_json_file(_metadata_path_for_json(context.get("fields_json_path", "")))
    current_confidences = _field_confidences(current_fields, current_metadata)
    threshold = _env_float("EXCEPTION_RESOLUTION_OCR_FIELD_CONFIDENCE_THRESHOLD", 0.72)

    try:
        from ocr_landingai import ocr_invoice_to_json
    except Exception as exc:
        return RecoveryAttempt(
            path_name="targeted_ocr_reextract",
            outcome="failed",
            summary=f"Targeted OCR re-extraction could not start because the LandingAI client was unavailable: {exc}",
            findings={"error": str(exc), "target_fields": low_confidence_fields},
        )

    try:
        retry_json_path = ocr_invoice_to_json(
            context["file_path"],
            hint_fields=low_confidence_fields,
            output_label="retry",
        )
    except Exception as exc:
        return RecoveryAttempt(
            path_name="targeted_ocr_reextract",
            outcome="failed",
            summary=f"Targeted OCR re-extraction failed: {exc}",
            findings={"error": str(exc), "target_fields": low_confidence_fields},
        )

    if not retry_json_path:
        return RecoveryAttempt(
            path_name="targeted_ocr_reextract",
            outcome="failed",
            summary="Targeted OCR re-extraction did not return any extracted fields JSON.",
            findings={"target_fields": low_confidence_fields},
        )

    retried_fields = _load_json_file(retry_json_path)
    retried_metadata = _load_json_file(_metadata_path_for_json(retry_json_path))
    retried_confidences = _field_confidences(retried_fields, retried_metadata)

    old_scores = [current_confidences.get(field_name, 0.0) for field_name in low_confidence_fields]
    new_scores = [retried_confidences.get(field_name, 0.0) for field_name in low_confidence_fields]
    average_old = sum(old_scores) / len(old_scores)
    average_new = sum(new_scores) / len(new_scores)
    improved_fields = [
        field_name
        for field_name in low_confidence_fields
        if retried_confidences.get(field_name, 0.0) > current_confidences.get(field_name, 0.0) + 0.05
        or (
            not current_fields.get(field_name)
            and retried_fields.get(field_name)
        )
    ]

    if average_new >= threshold and improved_fields:
        _apply_reextracted_fields_update(
            context["id"],
            current_context=context,
            new_fields=retried_fields,
            new_json_path=retry_json_path,
        )
        _reconcile_invoice_vendor_status(context["id"])
        return RecoveryAttempt(
            path_name="targeted_ocr_reextract",
            outcome="improved",
            summary=(
                f"Targeted OCR re-extraction improved the confidence of fields {', '.join(improved_fields)} "
                f"from {average_old:.2f} to {average_new:.2f}."
            ),
            findings={
                "target_fields": low_confidence_fields,
                "improved_fields": improved_fields,
                "old_confidences": {field_name: current_confidences.get(field_name, 0.0) for field_name in low_confidence_fields},
                "new_confidences": {field_name: retried_confidences.get(field_name, 0.0) for field_name in low_confidence_fields},
                "retry_json_path": retry_json_path,
            },
            confidence=round(min(0.95, average_new), 4),
        )

    return RecoveryAttempt(
        path_name="targeted_ocr_reextract",
        outcome="failed",
        summary="Targeted OCR re-extraction did not improve the relevant field confidence enough to update the invoice.",
        findings={
            "target_fields": low_confidence_fields,
            "old_confidences": {field_name: current_confidences.get(field_name, 0.0) for field_name in low_confidence_fields},
            "new_confidences": {field_name: retried_confidences.get(field_name, 0.0) for field_name in low_confidence_fields},
            "retry_json_path": retry_json_path,
        },
        confidence=round(average_new, 4),
    )


def _run_recovery_paths(
    invoice_id: str,
    source_document_id: Optional[str],
    current_analysis: InvoiceMatchAnalysis,
    attempts: List[RecoveryAttempt],
) -> InvoiceMatchAnalysis:
    context = _load_invoice_context(invoice_id)
    if not context:
        return current_analysis

    if current_analysis.reason in VENDOR_RELATED_REASONS or not context.get("vendor_id"):
        vendor_attempt = _append_attempt(
            attempts,
            invoice_id,
            source_document_id,
            _attempt_vendor_fuzzy_match(context),
        )
        if vendor_attempt.outcome == "resolved":
            current_analysis = analyze_invoice_match(invoice_id)
            if current_analysis.decision == "matched_auto":
                return current_analysis
            context = _load_invoice_context(invoice_id) or context

    if current_analysis.reason in PO_RELATED_REASONS:
        fuzzy_po_attempt = _append_attempt(
            attempts,
            invoice_id,
            source_document_id,
            _attempt_fuzzy_po_match(context),
        )
        if fuzzy_po_attempt.outcome == "resolved":
            updated_context = _load_invoice_context(invoice_id) or context
            return _build_matched_analysis(
                updated_context,
                po_id=fuzzy_po_attempt.findings["po_id"],
                confidence=fuzzy_po_attempt.confidence,
                reason="resolved_fuzzy_po_match",
                diff=fuzzy_po_attempt.findings.get("amount_delta"),
            )

        amount_attempt = _append_attempt(
            attempts,
            invoice_id,
            source_document_id,
            _attempt_amount_tolerance_recovery(context),
        )
        if amount_attempt.outcome == "resolved":
            updated_context = _load_invoice_context(invoice_id) or context
            return _build_matched_analysis(
                updated_context,
                po_id=amount_attempt.findings["po_id"],
                confidence=amount_attempt.confidence,
                reason=f"resolved_amount_tier_{amount_attempt.findings.get('variance_tier')}",
                diff=amount_attempt.findings.get("amount_delta"),
            )

    return current_analysis


def resolve_match_exception(
    invoice_id: str,
    initial_analysis: InvoiceMatchAnalysis,
    *,
    source_document_id: Optional[str] = None,
    amount_tolerance: float = 1.0,
    percent_tolerance: float = 0.02,
) -> ExceptionResolutionResult:
    current_analysis = initial_analysis
    attempts: List[RecoveryAttempt] = []

    current_analysis = _run_recovery_paths(
        invoice_id,
        source_document_id,
        current_analysis,
        attempts,
    )
    if current_analysis.decision != "matched_auto":
        context = _load_invoice_context(invoice_id)
        if context:
            retry_attempt = _append_attempt(
                attempts,
                invoice_id,
                source_document_id,
                _attempt_targeted_reextract(context, current_analysis),
            )
            if retry_attempt.outcome == "improved":
                refreshed_analysis = analyze_invoice_match(
                    invoice_id,
                    amount_tolerance=amount_tolerance,
                    percent_tolerance=percent_tolerance,
                )
                current_analysis = _run_recovery_paths(
                    invoice_id,
                    source_document_id,
                    refreshed_analysis,
                    attempts,
                )
                if current_analysis.decision != "matched_auto":
                    current_analysis = analyze_invoice_match(
                        invoice_id,
                        amount_tolerance=amount_tolerance,
                        percent_tolerance=percent_tolerance,
                    )

    final_context = _load_invoice_context(invoice_id) or {"matched_po_id": None}
    return ExceptionResolutionResult(
        invoice_id=invoice_id,
        initial_analysis=initial_analysis,
        final_analysis=current_analysis,
        attempts=attempts,
        matched_po_id=final_context.get("matched_po_id") or current_analysis.best_po_id,
    )
