from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass
class ErrorDetails:
    message: str
    code: str
    status_code: int


class UserFacingError(Exception):
    def __init__(self, message: str, *, code: str = "request_failed", status_code: int = 400):
        super().__init__(message)
        self.user_message = message
        self.code = code
        self.status_code = status_code


class DuplicateInvoiceBlockedError(UserFacingError):
    def __init__(self, message: str, *, duplicate_invoice_id: Optional[str] = None):
        super().__init__(
            message,
            code="duplicate_invoice_blocked",
            status_code=409,
        )
        self.duplicate_invoice_id = duplicate_invoice_id


def _contains(text: str, *patterns: str) -> bool:
    lowered = text.lower()
    return any(pattern in lowered for pattern in patterns)


def get_error_details(exc: Exception, *, default_status: int = 500) -> ErrorDetails:
    if isinstance(exc, UserFacingError):
        return ErrorDetails(exc.user_message, exc.code, exc.status_code)

    message = str(exc or "").strip()
    lowered = message.lower()

    if not message:
        return ErrorDetails(
            "Something went wrong. Please try again.",
            "unknown_error",
            default_status,
        )

    if _contains(lowered, "connection timeout", "could not connect", "connection refused", "sslmode", "name or service not known"):
        return ErrorDetails(
            "The app cannot reach a required service right now. Please try again in a moment.",
            "service_unreachable",
            503,
        )

    if _contains(lowered, "migration missing"):
        return ErrorDetails(
            message,
            "system_not_configured",
            503 if default_status >= 500 else default_status,
        )

    if _contains(lowered, "not configured", "missing", "tables not found", "does not exist"):
        return ErrorDetails(
            "This feature is not fully set up yet. Please check the application configuration and try again.",
            "system_not_configured",
            503 if default_status >= 500 else default_status,
        )

    if _contains(lowered, "duplicate invoice", "potential duplicate invoice"):
        return ErrorDetails(
            "This invoice was already received earlier, so it has been blocked to prevent duplicate payment.",
            "duplicate_invoice_blocked",
            409,
        )

    if _contains(lowered, "already paid"):
        return ErrorDetails(
            "This invoice has already been paid.",
            "invoice_already_paid",
            409,
        )

    if _contains(lowered, "already pending under another payment", "already pending"):
        return ErrorDetails(
            "This invoice is already in an active payment flow.",
            "invoice_payment_pending",
            409,
        )

    if _contains(lowered, "duplicate payment", "duplicate payable invoice", "existing payment links do not match"):
        return ErrorDetails(
            "A duplicate invoice was detected in this payment request, so the payment was blocked for safety.",
            "duplicate_payment_blocked",
            409,
        )

    if _contains(lowered, "idempotent requests", "payment_intent"):
        return ErrorDetails(
            "This payment request is already being processed. Please wait a moment and try again.",
            "payment_in_progress",
            409,
        )

    if _contains(lowered, "mixed currency"):
        return ErrorDetails(
            "Invoices with different currencies cannot be paid together.",
            "mixed_currency_selection",
            400,
        )

    if _contains(
        lowered,
        "invoiceid is required",
        "invoiceids required",
        "paymentintentid is required",
        "approvedby is required",
        "rejectedby is required",
        "sentby is required",
        "reviewer is required",
        "resolutionnotes is required",
        "action is required",
        "name is required",
        "vendor is required",
        "file is required",
    ):
        return ErrorDetails(
            message,
            "missing_required_input",
            400,
        )

    if _contains(
        lowered,
        "only pending review items can be assigned",
        "only pending review items can be resolved",
        "only pending review items can be rejected",
    ):
        return ErrorDetails(
            "This review item is no longer pending, so it cannot be changed again.",
            "review_item_not_pending",
            409,
        )

    if _contains(lowered, "no candidate po was available in the review packet"):
        return ErrorDetails(
            "This review item does not have an available purchase order candidate to approve.",
            "review_candidate_missing",
            409,
        )

    if _contains(lowered, "only invoice review items can approve a po match"):
        return ErrorDetails(
            "Only invoice review items can be approved as a purchase order match.",
            "review_action_not_allowed",
            400,
        )

    if _contains(lowered, "must be approved before execution"):
        return ErrorDetails(
            message,
            "approval_required",
            409 if default_status == 500 else default_status,
        )

    if _contains(lowered, "not found"):
        return ErrorDetails(
            "The requested record could not be found.",
            "not_found",
            404 if default_status == 500 else default_status,
        )

    if _contains(lowered, "ocr", "landingai", "extraction"):
        return ErrorDetails(
            "We could not read that invoice clearly. Please try again or send it for review.",
            "invoice_extraction_failed",
            422 if default_status == 500 else default_status,
        )

    if _contains(lowered, "not eligible for payment"):
        return ErrorDetails(
            "This invoice is not ready to be paid yet.",
            "invoice_not_payable",
            409,
        )

    if _contains(lowered, "vendor mismatch", "chat does not belong to vendor"):
        return ErrorDetails(
            "That action is not allowed for the selected vendor.",
            "vendor_scope_mismatch",
            403 if default_status == 500 else default_status,
        )

    if _contains(lowered, "violates unique constraint", "duplicate key value violates unique constraint"):
        return ErrorDetails(
            "This record already exists, so the duplicate action was not applied.",
            "duplicate_record",
            409,
        )

    return ErrorDetails(
        "We could not complete that request. Please try again.",
        "request_failed",
        default_status,
    )


def get_user_facing_message(exc: Exception, *, default_status: int = 500) -> str:
    return get_error_details(exc, default_status=default_status).message
