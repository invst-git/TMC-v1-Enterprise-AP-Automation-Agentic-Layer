import datetime
import json
import os
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from invoice_db import find_duplicate_invoice_candidates
from ocr_landingai import ocr_invoice_to_json
from source_document_tracking import update_source_document_segment_best_effort


def _get_agent_db_ops():
    from agent_db import create_human_review_item, set_workflow_state, update_source_document

    return update_source_document, set_workflow_state, create_human_review_item


def _env_float(name: str, default: float) -> float:
    value = os.getenv(name)
    if value is None:
        return default
    try:
        return float(value)
    except Exception:
        return default


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
    if value is None or value == "":
        return None
    try:
        return float(value)
    except Exception:
        return None


@dataclass
class ValidationIssue:
    code: str
    severity: str
    message: str
    details: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "code": self.code,
            "severity": self.severity,
            "message": self.message,
            "details": self.details,
        }


@dataclass
class ExtractionValidationResult:
    invoice_path: str
    json_path: Optional[str]
    decision: str
    extraction_status: str
    issues: List[ValidationIssue] = field(default_factory=list)
    duplicate_candidates: List[Dict[str, Any]] = field(default_factory=list)
    review_reason_code: Optional[str] = None
    review_item_id: Optional[str] = None
    extracted_fields: Optional[Dict[str, Any]] = None

    @property
    def requires_review(self) -> bool:
        return self.decision == "needs_review"

    @property
    def blocks_persistence(self) -> bool:
        return any(issue.code == "potential_duplicate_invoice" for issue in self.issues)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "invoice_path": self.invoice_path,
            "json_path": self.json_path,
            "decision": self.decision,
            "extraction_status": self.extraction_status,
            "issues": [issue.to_dict() for issue in self.issues],
            "duplicate_candidates": self.duplicate_candidates,
            "review_reason_code": self.review_reason_code,
            "review_item_id": self.review_item_id,
            "extracted_fields": self.extracted_fields,
        }


def _highest_priority_review_reason(issues: List[ValidationIssue]) -> Optional[str]:
    priority = {
        "potential_duplicate_invoice": 1,
        "missing_required_fields": 2,
        "invoice_total_mismatch": 3,
        "invalid_invoice_dates": 4,
        "ocr_extraction_failed": 5,
        "ocr_output_unreadable": 6,
        "duplicate_check_failed": 7,
    }
    ranked = sorted(issues, key=lambda issue: priority.get(issue.code, 999))
    return ranked[0].code if ranked else None


def _summarize_issues(issues: List[ValidationIssue]) -> str:
    if not issues:
        return "Extraction validated with no blocking issues."
    return "; ".join(issue.message for issue in issues[:3])


def _validate_extracted_fields(
    fields: Dict[str, Any],
    duplicate_candidates: List[Dict[str, Any]],
) -> List[ValidationIssue]:
    issues: List[ValidationIssue] = []
    missing_fields = [
        field_name
        for field_name in ("supplier_name", "invoice_number", "total_amount")
        if not fields.get(field_name)
    ]
    if missing_fields:
        issues.append(
            ValidationIssue(
                code="missing_required_fields",
                severity="critical",
                message=f"LandingAI output is missing required fields: {', '.join(missing_fields)}.",
                details={"missing_fields": missing_fields},
            )
        )

    subtotal_amount = _to_float(fields.get("subtotal_amount"))
    tax_amount = _to_float(fields.get("tax_amount")) or 0.0
    shipping_amount = _to_float(fields.get("shipping_amount")) or 0.0
    discount_amount = _to_float(fields.get("discount_amount")) or 0.0
    total_amount = _to_float(fields.get("total_amount"))
    arithmetic_tolerance = _env_float("EXTRACTION_VALIDATION_TOTAL_TOLERANCE", 0.05)
    if subtotal_amount is not None and total_amount is not None:
        expected_total = subtotal_amount + tax_amount + shipping_amount - discount_amount
        if abs(expected_total - total_amount) > arithmetic_tolerance:
            issues.append(
                ValidationIssue(
                    code="invoice_total_mismatch",
                    severity="high",
                    message="Extracted totals are arithmetically inconsistent.",
                    details={
                        "subtotal_amount": subtotal_amount,
                        "tax_amount": tax_amount,
                        "shipping_amount": shipping_amount,
                        "discount_amount": discount_amount,
                        "expected_total": round(expected_total, 2),
                        "total_amount": total_amount,
                    },
                )
            )

    invoice_date = _parse_date(fields.get("invoice_date"))
    due_date = _parse_date(fields.get("due_date"))
    if invoice_date and due_date and due_date < invoice_date:
        issues.append(
            ValidationIssue(
                code="invalid_invoice_dates",
                severity="high",
                message="Due date is earlier than invoice date.",
                details={
                    "invoice_date": invoice_date.isoformat(),
                    "due_date": due_date.isoformat(),
                },
            )
        )

    strong_duplicates = [candidate for candidate in duplicate_candidates if candidate.get("confidence", 0) >= 0.75]
    if strong_duplicates:
        issues.append(
            ValidationIssue(
                code="potential_duplicate_invoice",
                severity="high",
                message="Existing invoices match this invoice number closely enough to be treated as potential duplicates.",
                details={"candidate_ids": [candidate["id"] for candidate in strong_duplicates[:5]]},
            )
        )

    return issues


def _create_review_item_best_effort(
    *,
    source_document_id: Optional[str],
    source_document_segment_id: Optional[str],
    review_reason_code: str,
    review_summary: str,
    result: ExtractionValidationResult,
) -> Optional[str]:
    if not source_document_id and not source_document_segment_id:
        return None
    try:
        _, _, create_human_review_item = _get_agent_db_ops()
        review_item = create_human_review_item(
            entity_type="source_document_segment" if source_document_segment_id else "source_document",
            entity_id=source_document_segment_id or source_document_id,
            source_document_id=source_document_id,
            queue_name="extraction_validation",
            review_reason=review_reason_code,
            metadata={
                "summary": review_summary,
                "json_path": result.json_path,
                "issues": [issue.to_dict() for issue in result.issues],
                "duplicate_candidates": result.duplicate_candidates,
            },
        )
        return review_item["id"]
    except Exception:
        return None


def _mark_extraction_started_best_effort(
    source_document_id: Optional[str],
    source_document_segment_id: Optional[str],
    invoice_path: str,
) -> None:
    if source_document_id:
        try:
            update_source_document, _, _ = _get_agent_db_ops()
            update_source_document(
                source_document_id,
                extraction_status="extracting",
                metadata={"extraction": {"active_invoice_path": invoice_path}},
            )
        except Exception:
            pass
    update_source_document_segment_best_effort(
        source_document_segment_id,
        status="extracting",
        metadata={"extraction": {"invoice_path": invoice_path}},
    )


def finalize_source_document_extraction_best_effort(
    source_document_id: Optional[str],
    validation_results: List[ExtractionValidationResult],
) -> Optional[str]:
    if not source_document_id:
        return None

    successful = [result for result in validation_results if result.json_path]
    requires_review = [result for result in validation_results if result.requires_review]
    failed = [result for result in validation_results if result.extraction_status == "failed"]

    if not successful:
        extraction_status = "failed"
        workflow_state = "extraction_failed"
        reason = "No segments produced validated extraction output."
    elif requires_review:
        extraction_status = "review_required"
        workflow_state = "needs_review"
        reason = "One or more extracted segments require human review."
    else:
        extraction_status = "validated"
        workflow_state = "validated"
        reason = "All extracted segments validated successfully."

    try:
        update_source_document, set_workflow_state, _ = _get_agent_db_ops()
        update_source_document(
            source_document_id,
            extraction_status=extraction_status,
            metadata={
                "extraction": {
                    "successful_segments": len(successful),
                    "review_required_segments": len(requires_review),
                    "failed_segments": len(failed),
                }
            },
        )
        set_workflow_state(
            "source_document",
            source_document_id,
            workflow_state,
            current_stage="extraction",
            event_type="extraction_completed",
            reason=reason,
            metadata={
                "source_document_id": source_document_id,
                "successful_segments": len(successful),
                "review_required_segments": len(requires_review),
                "failed_segments": len(failed),
            },
        )
        return extraction_status
    except Exception:
        return None


def extract_and_validate_invoice(
    invoice_path: str,
    *,
    source_document_id: Optional[str] = None,
    source_document_segment_id: Optional[str] = None,
    from_email: Optional[str] = None,
    vendor_id_override: Optional[str] = None,
) -> ExtractionValidationResult:
    _mark_extraction_started_best_effort(source_document_id, source_document_segment_id, invoice_path)

    try:
        json_path = ocr_invoice_to_json(invoice_path)
    except Exception as exc:
        issue = ValidationIssue(
            code="ocr_extraction_failed",
            severity="critical",
            message=f"LandingAI extraction failed: {exc}",
            details={"exception": str(exc)},
        )
        update_source_document_segment_best_effort(
            source_document_segment_id,
            status="failed",
            metadata={"extraction": {"error": str(exc)}},
        )
        result = ExtractionValidationResult(
            invoice_path=invoice_path,
            json_path=None,
            decision="failed",
            extraction_status="failed",
            issues=[issue],
        )
        result.review_reason_code = issue.code
        result.review_item_id = _create_review_item_best_effort(
            source_document_id=source_document_id,
            source_document_segment_id=source_document_segment_id,
            review_reason_code=issue.code,
            review_summary=issue.message,
            result=result,
        )
        return result

    if not json_path:
        issue = ValidationIssue(
            code="ocr_extraction_failed",
            severity="critical",
            message="LandingAI extraction returned no JSON output.",
        )
        update_source_document_segment_best_effort(
            source_document_segment_id,
            status="failed",
            metadata={"extraction": {"error": "missing_json_output"}},
        )
        result = ExtractionValidationResult(
            invoice_path=invoice_path,
            json_path=None,
            decision="failed",
            extraction_status="failed",
            issues=[issue],
        )
        result.review_reason_code = issue.code
        result.review_item_id = _create_review_item_best_effort(
            source_document_id=source_document_id,
            source_document_segment_id=source_document_segment_id,
            review_reason_code=issue.code,
            review_summary=issue.message,
            result=result,
        )
        return result

    try:
        with open(json_path, "r", encoding="utf-8") as handle:
            extracted_fields = json.load(handle)
    except Exception as exc:
        issue = ValidationIssue(
            code="ocr_output_unreadable",
            severity="critical",
            message=f"LandingAI JSON output could not be read: {exc}",
            details={"exception": str(exc), "json_path": json_path},
        )
        update_source_document_segment_best_effort(
            source_document_segment_id,
            status="failed",
            metadata={"extraction": {"error": str(exc), "json_path": json_path}},
        )
        result = ExtractionValidationResult(
            invoice_path=invoice_path,
            json_path=json_path,
            decision="failed",
            extraction_status="failed",
            issues=[issue],
        )
        result.review_reason_code = issue.code
        result.review_item_id = _create_review_item_best_effort(
            source_document_id=source_document_id,
            source_document_segment_id=source_document_segment_id,
            review_reason_code=issue.code,
            review_summary=issue.message,
            result=result,
        )
        return result

    duplicate_candidates: List[Dict[str, Any]] = []
    duplicate_check_issue: Optional[ValidationIssue] = None
    try:
        duplicate_candidates = find_duplicate_invoice_candidates(
            extracted_fields.get("invoice_number"),
            vendor_id=vendor_id_override,
            supplier_name=extracted_fields.get("supplier_name"),
            total_amount=_to_float(extracted_fields.get("total_amount")),
            invoice_date=extracted_fields.get("invoice_date"),
        )
    except Exception as exc:
        duplicate_check_issue = ValidationIssue(
            code="duplicate_check_failed",
            severity="medium",
            message=f"Duplicate invoice check failed: {exc}",
            details={"exception": str(exc)},
        )

    issues = _validate_extracted_fields(extracted_fields, duplicate_candidates)
    if duplicate_check_issue:
        issues.append(duplicate_check_issue)

    requires_review = any(issue.severity in {"high", "critical"} for issue in issues)
    decision = "needs_review" if requires_review else "persist"
    extraction_status = "review_required" if requires_review else "validated"

    result = ExtractionValidationResult(
        invoice_path=invoice_path,
        json_path=json_path,
        decision=decision,
        extraction_status=extraction_status,
        issues=issues,
        duplicate_candidates=duplicate_candidates,
        extracted_fields=extracted_fields,
    )
    result.review_reason_code = _highest_priority_review_reason(issues) if requires_review else None
    if result.review_reason_code:
        result.review_item_id = _create_review_item_best_effort(
            source_document_id=source_document_id,
            source_document_segment_id=source_document_segment_id,
            review_reason_code=result.review_reason_code,
            review_summary=_summarize_issues(issues),
            result=result,
        )

    update_source_document_segment_best_effort(
        source_document_segment_id,
        status=extraction_status,
        metadata={
            "extraction": {
                "json_path": json_path,
                "decision": decision,
                "issue_count": len(issues),
                "duplicate_candidate_count": len(duplicate_candidates),
                "review_item_id": result.review_item_id,
                "from_email": from_email,
            }
        },
    )

    return result
