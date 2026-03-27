from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from db import get_conn


@dataclass
class MatchCandidate:
    po_id: str
    po_number: str
    total_amount: Optional[float]
    currency: Optional[str]
    vendor_id: Optional[str]
    diff: Optional[float]
    vendor_match: bool
    currency_match: bool
    within_tolerance: bool
    confidence: float
    eligibility_reason: str

    def to_dict(self) -> Dict[str, Any]:
        return {
            "po_id": self.po_id,
            "po_number": self.po_number,
            "total_amount": self.total_amount,
            "currency": self.currency,
            "vendor_id": self.vendor_id,
            "diff": self.diff,
            "vendor_match": self.vendor_match,
            "currency_match": self.currency_match,
            "within_tolerance": self.within_tolerance,
            "confidence": self.confidence,
            "eligibility_reason": self.eligibility_reason,
        }


@dataclass
class InvoiceMatchAnalysis:
    invoice_id: str
    invoice_status: str
    invoice_po_number: Optional[str]
    invoice_total: Optional[float]
    invoice_currency: Optional[str]
    invoice_vendor_id: Optional[str]
    decision: str
    reason: str
    confidence: float = 0.0
    best_po_id: Optional[str] = None
    best_diff: Optional[float] = None
    candidates: List[MatchCandidate] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "invoice_id": self.invoice_id,
            "invoice_status": self.invoice_status,
            "invoice_po_number": self.invoice_po_number,
            "invoice_total": self.invoice_total,
            "invoice_currency": self.invoice_currency,
            "invoice_vendor_id": self.invoice_vendor_id,
            "decision": self.decision,
            "reason": self.reason,
            "confidence": self.confidence,
            "best_po_id": self.best_po_id,
            "best_diff": self.best_diff,
            "candidates": [candidate.to_dict() for candidate in self.candidates],
        }


def _compute_match_confidence(invoice_total: float, diff: float) -> float:
    return max(0.0, 1.0 - min(1.0, diff / max(1.0, abs(float(invoice_total)))))


def _within_tolerance(invoice_total: float, diff: float, amount_tolerance: float, percent_tolerance: float) -> bool:
    return diff <= amount_tolerance or diff <= abs(float(invoice_total)) * percent_tolerance


def analyze_invoice_match(
    invoice_id: str,
    amount_tolerance: float = 1.0,
    percent_tolerance: float = 0.02,
) -> InvoiceMatchAnalysis:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, po_number, total_amount, currency, vendor_id, status
                FROM invoices
                WHERE id=%s
                """,
                (invoice_id,),
            )
            row = cur.fetchone()
            if not row:
                return InvoiceMatchAnalysis(
                    invoice_id=invoice_id,
                    invoice_status="missing",
                    invoice_po_number=None,
                    invoice_total=None,
                    invoice_currency=None,
                    invoice_vendor_id=None,
                    decision="no_action",
                    reason="invoice_not_found",
                )

            _, inv_po_number, inv_total, inv_currency, inv_vendor_id, inv_status = row
            invoice_total = float(inv_total) if inv_total is not None else None
            invoice_currency = str(inv_currency) if inv_currency else None
            invoice_vendor = str(inv_vendor_id) if inv_vendor_id else None
            analysis = InvoiceMatchAnalysis(
                invoice_id=str(invoice_id),
                invoice_status=inv_status or "",
                invoice_po_number=inv_po_number,
                invoice_total=invoice_total,
                invoice_currency=invoice_currency,
                invoice_vendor_id=invoice_vendor,
                decision="unmatched",
                reason="not_evaluated",
            )

            if inv_status == "vendor_mismatch":
                analysis.decision = "review"
                analysis.reason = "vendor_mismatch"
                analysis.confidence = 0.99
                return analysis
            if not inv_po_number:
                analysis.decision = "review"
                analysis.reason = "missing_po_number"
                analysis.confidence = 0.25
                return analysis
            if invoice_total is None:
                analysis.decision = "review"
                analysis.reason = "missing_invoice_total"
                analysis.confidence = 0.2
                return analysis

            cur.execute(
                """
                SELECT id, po_number, total_amount, currency, vendor_id
                FROM purchase_orders
                WHERE po_number=%s
                  AND status IN ('open','partially_received')
                """,
                (inv_po_number,),
            )
            rows = cur.fetchall()
            if not rows:
                analysis.decision = "review"
                analysis.reason = "no_open_po_candidates"
                analysis.confidence = 0.35
                return analysis

            eligible_candidates: List[MatchCandidate] = []
            vendor_mismatch_seen = False
            currency_mismatch_seen = False
            missing_total_seen = False

            for po_id, po_number, po_total, po_currency, po_vendor_id in rows:
                po_total_float = float(po_total) if po_total is not None else None
                po_currency_str = str(po_currency) if po_currency else None
                po_vendor_str = str(po_vendor_id) if po_vendor_id else None
                vendor_match = not invoice_vendor or not po_vendor_str or invoice_vendor == po_vendor_str
                currency_match = not invoice_currency or not po_currency_str or invoice_currency == po_currency_str

                diff = abs(invoice_total - po_total_float) if po_total_float is not None else None
                within_tolerance = bool(
                    diff is not None and _within_tolerance(invoice_total, diff, amount_tolerance, percent_tolerance)
                )
                confidence = _compute_match_confidence(invoice_total, diff) if diff is not None else 0.0

                if po_total_float is None:
                    eligibility_reason = "candidate_missing_po_total"
                    missing_total_seen = True
                elif not vendor_match and not currency_match:
                    eligibility_reason = "candidate_vendor_and_currency_mismatch"
                    vendor_mismatch_seen = True
                    currency_mismatch_seen = True
                elif not vendor_match:
                    eligibility_reason = "candidate_vendor_mismatch"
                    vendor_mismatch_seen = True
                elif not currency_match:
                    eligibility_reason = "candidate_currency_mismatch"
                    currency_mismatch_seen = True
                elif not within_tolerance:
                    eligibility_reason = "amount_outside_tolerance"
                else:
                    eligibility_reason = "eligible"

                candidate = MatchCandidate(
                    po_id=str(po_id),
                    po_number=po_number or "",
                    total_amount=po_total_float,
                    currency=po_currency_str,
                    vendor_id=po_vendor_str,
                    diff=round(diff, 4) if diff is not None else None,
                    vendor_match=vendor_match,
                    currency_match=currency_match,
                    within_tolerance=within_tolerance,
                    confidence=round(confidence, 4),
                    eligibility_reason=eligibility_reason,
                )
                analysis.candidates.append(candidate)

                if eligibility_reason == "eligible":
                    eligible_candidates.append(candidate)

            if not eligible_candidates:
                if vendor_mismatch_seen and currency_mismatch_seen:
                    analysis.reason = "candidate_vendor_and_currency_mismatch"
                    analysis.confidence = 0.65
                elif vendor_mismatch_seen:
                    analysis.reason = "candidate_vendor_mismatch"
                    analysis.confidence = 0.72
                elif currency_mismatch_seen:
                    analysis.reason = "candidate_currency_mismatch"
                    analysis.confidence = 0.72
                elif missing_total_seen:
                    analysis.reason = "candidate_missing_po_total"
                    analysis.confidence = 0.55
                else:
                    analysis.reason = "amount_outside_tolerance"
                    analysis.confidence = 0.68
                analysis.decision = "review"
                return analysis

            best_candidate = min(
                eligible_candidates,
                key=lambda candidate: (candidate.diff if candidate.diff is not None else float("inf"), candidate.po_id),
            )
            analysis.decision = "matched_auto"
            analysis.reason = "deterministic_po_match"
            analysis.best_po_id = best_candidate.po_id
            analysis.best_diff = best_candidate.diff
            analysis.confidence = round(best_candidate.confidence, 4)
            return analysis


def apply_match_analysis(invoice_id: str, analysis: InvoiceMatchAnalysis) -> Optional[str]:
    if analysis.decision != "matched_auto" or not analysis.best_po_id:
        return None
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE invoices
                SET matched_po_id=%s,status='matched_auto',confidence=%s
                WHERE id=%s
                """,
                (analysis.best_po_id, analysis.confidence, invoice_id),
            )
    return str(analysis.best_po_id)


def match_invoice(invoice_id: str, amount_tolerance: float = 1.0, percent_tolerance: float = 0.02) -> Optional[str]:
    analysis = analyze_invoice_match(
        invoice_id,
        amount_tolerance=amount_tolerance,
        percent_tolerance=percent_tolerance,
    )
    return apply_match_analysis(invoice_id, analysis)
