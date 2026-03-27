import json
import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

try:
    from pypdf import PdfReader, PdfWriter
except Exception:
    PdfReader = None
    PdfWriter = None


INVOICE_HEADER_KEYWORDS = (
    "invoice",
    "tax invoice",
    "gst invoice",
    "credit note",
    "debit note",
)
TOTAL_KEYWORDS = (
    "grand total",
    "invoice total",
    "total due",
    "amount due",
    "balance due",
)
CONTINUATION_KEYWORDS = (
    "continued",
    "carry over",
    "carried forward",
    "page subtotal",
    "previous balance",
)
TABLE_KEYWORDS = (
    "description",
    "qty",
    "quantity",
    "unit price",
    "line total",
    "amount",
    "item",
)
INVOICE_NUMBER_PATTERNS = (
    r"(?:invoice|inv|credit note|debit note)\s*(?:number|no|num|#)?\s*[:#-]?\s*([A-Z0-9][A-Z0-9\-\/]{2,})",
    r"\binv\s*#\s*([A-Z0-9][A-Z0-9\-\/]{2,})",
)
PAGE_MARKER_PATTERNS = (
    r"\bpage\s*(\d+)\s*(?:of|/)\s*(\d+)\b",
    r"\b(\d+)\s*/\s*(\d+)\b",
)
SEGMENT_STRATEGY = "pypdf_text_boundary_v1"


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


def _env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None:
        return default
    try:
        return int(value)
    except Exception:
        return default


def _collapse_ws(text: str) -> str:
    return " ".join((text or "").split())


def _extract_lines(text: str) -> List[str]:
    return [line.strip() for line in (text or "").splitlines() if line.strip()]


def _find_invoice_number(text: str) -> Optional[str]:
    haystack = (text or "").upper()
    for pattern in INVOICE_NUMBER_PATTERNS:
        match = re.search(pattern, haystack, flags=re.IGNORECASE)
        if not match:
            continue
        candidate = (match.group(1) or "").strip(" .:-#").upper()
        if not candidate:
            continue
        if candidate in {"DATE", "NUMBER", "NO"}:
            continue
        return candidate
    return None


def _find_page_marker(text: str) -> Tuple[Optional[int], Optional[int]]:
    haystack = (text or "").lower()
    for pattern in PAGE_MARKER_PATTERNS:
        match = re.search(pattern, haystack, flags=re.IGNORECASE)
        if not match:
            continue
        try:
            current = int(match.group(1))
            total = int(match.group(2))
        except Exception:
            continue
        if current < 1 or total < current or total > 1000:
            continue
        return current, total
    return None, None


def _count_keyword_hits(text: str, keywords: Tuple[str, ...]) -> int:
    lowered = (text or "").lower()
    return sum(1 for keyword in keywords if keyword in lowered)


def _looks_like_table(text: str, lines: List[str]) -> bool:
    lowered = (text or "").lower()
    keyword_hits = _count_keyword_hits(lowered, TABLE_KEYWORDS)
    numeric_lines = sum(1 for line in lines if len(re.findall(r"\d", line)) >= 3)
    return keyword_hits >= 2 or (keyword_hits >= 1 and numeric_lines >= 3)


def _find_vendor_anchor(lines: List[str]) -> Optional[str]:
    for line in lines[:6]:
        lowered = line.lower()
        if "invoice" in lowered or "page " in lowered or lowered.startswith("bill to"):
            continue
        letters = re.sub(r"[^a-z]", "", lowered)
        if len(letters) >= 5:
            return line[:80]
    return None


@dataclass
class PageSignal:
    page_number: int
    text_chars: int
    sparse_text: bool
    invoice_keyword_top: bool
    invoice_number: Optional[str]
    page_marker_current: Optional[int]
    page_marker_total: Optional[int]
    has_total: bool
    total_near_bottom: bool
    has_due_date: bool
    table_like: bool
    continuation_keyword: bool
    vendor_anchor: Optional[str]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "page_number": self.page_number,
            "text_chars": self.text_chars,
            "sparse_text": self.sparse_text,
            "invoice_keyword_top": self.invoice_keyword_top,
            "invoice_number": self.invoice_number,
            "page_marker_current": self.page_marker_current,
            "page_marker_total": self.page_marker_total,
            "has_total": self.has_total,
            "total_near_bottom": self.total_near_bottom,
            "has_due_date": self.has_due_date,
            "table_like": self.table_like,
            "continuation_keyword": self.continuation_keyword,
            "vendor_anchor": self.vendor_anchor,
        }


@dataclass
class SegmentInfo:
    page_from: int
    page_to: int
    confidence: float
    reason: str = ""
    path: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "page_from": self.page_from,
            "page_to": self.page_to,
            "confidence": self.confidence,
            "reason": self.reason,
            "path": self.path,
        }


@dataclass
class SegmentationResult:
    source_path: str
    page_count: int
    strategy: str
    used_segmentation: bool
    reason: str
    segments: List[SegmentInfo] = field(default_factory=list)
    pages: List[PageSignal] = field(default_factory=list)
    metadata_path: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "source_path": self.source_path,
            "page_count": self.page_count,
            "strategy": self.strategy,
            "used_segmentation": self.used_segmentation,
            "reason": self.reason,
            "metadata_path": self.metadata_path,
            "segments": [segment.to_dict() for segment in self.segments],
            "pages": [page.to_dict() for page in self.pages],
        }


def _scan_page_signal(page_number: int, text: str) -> PageSignal:
    lines = _extract_lines(text)
    top_text = "\n".join(lines[:10])
    bottom_text = "\n".join(lines[-10:])
    normalized = _collapse_ws(text)
    lowered = normalized.lower()
    page_current, page_total = _find_page_marker("\n".join(lines[:8] + lines[-8:]))
    text_chars = len(normalized)
    min_chars = _env_int("PDF_SEGMENTATION_MIN_TEXT_CHARS", 60)
    return PageSignal(
        page_number=page_number,
        text_chars=text_chars,
        sparse_text=text_chars < min_chars,
        invoice_keyword_top=_count_keyword_hits(top_text, INVOICE_HEADER_KEYWORDS) > 0,
        invoice_number=_find_invoice_number(top_text or normalized[:400]),
        page_marker_current=page_current,
        page_marker_total=page_total,
        has_total=_count_keyword_hits(lowered, TOTAL_KEYWORDS) > 0,
        total_near_bottom=_count_keyword_hits(bottom_text, TOTAL_KEYWORDS) > 0,
        has_due_date=("due date" in lowered) or ("invoice date" in lowered),
        table_like=_looks_like_table(lowered, lines),
        continuation_keyword=_count_keyword_hits(lowered, CONTINUATION_KEYWORDS) > 0,
        vendor_anchor=_find_vendor_anchor(lines),
    )


def _score_boundary(
    previous: PageSignal,
    current: PageSignal,
    active_invoice_number: Optional[str],
) -> Tuple[bool, float, List[str]]:
    start_score = 0.0
    continue_score = 0.0
    reasons: List[str] = []

    if current.invoice_keyword_top:
        start_score += 0.22
        reasons.append("new invoice header")
    if current.invoice_number:
        start_score += 0.25
        reasons.append(f"invoice number {current.invoice_number}")
    if current.page_marker_current == 1:
        start_score += 0.20
        reasons.append("page counter reset")
    if current.has_due_date:
        start_score += 0.05
    if current.has_total and current.page_marker_current == 1:
        start_score += 0.05
    if previous.total_near_bottom:
        start_score += 0.10
        reasons.append("previous page ends with totals")
    if active_invoice_number and current.invoice_number and current.invoice_number != active_invoice_number:
        start_score += 0.30
        reasons.append("invoice number changed")

    if current.page_marker_current and current.page_marker_current > 1:
        continue_score += 0.25
    if (
        previous.page_marker_current
        and previous.page_marker_total
        and current.page_marker_current
        and current.page_marker_total
        and previous.page_marker_total == current.page_marker_total
        and current.page_marker_current == previous.page_marker_current + 1
    ):
        continue_score += 0.30
    if active_invoice_number and current.invoice_number and current.invoice_number == active_invoice_number:
        continue_score += 0.45
    if current.continuation_keyword:
        continue_score += 0.20
    if (not current.invoice_keyword_top) and current.table_like:
        continue_score += 0.15
    if previous.invoice_keyword_top and (not current.invoice_number) and current.table_like:
        continue_score += 0.10
    if current.sparse_text and (not current.invoice_keyword_top) and (not current.invoice_number):
        continue_score += 0.05

    delta = start_score - continue_score
    confidence = max(0.0, min(0.99, 0.5 + delta / 2.0))
    split = start_score >= 0.55 and delta >= 0.15
    return split, round(confidence, 2), reasons


def segment_text_pages(
    page_texts: List[str],
    *,
    source_path: str = "",
    min_confidence: Optional[float] = None,
) -> SegmentationResult:
    page_count = len(page_texts)
    pages = [_scan_page_signal(index + 1, text) for index, text in enumerate(page_texts)]
    result = SegmentationResult(
        source_path=source_path,
        page_count=page_count,
        strategy=SEGMENT_STRATEGY,
        used_segmentation=False,
        reason="not_evaluated",
        pages=pages,
    )

    if page_count <= 1:
        result.reason = "single_page_pdf"
        result.segments = [SegmentInfo(page_from=1, page_to=max(1, page_count), confidence=1.0, path=source_path or None)]
        return result

    rich_pages = sum(1 for page in pages if not page.sparse_text)
    if rich_pages < 2:
        result.reason = "insufficient_text_for_confident_split"
        result.segments = [SegmentInfo(page_from=1, page_to=page_count, confidence=1.0, path=source_path or None)]
        return result

    min_confidence = min_confidence if min_confidence is not None else _env_float("PDF_SEGMENTATION_MIN_CONFIDENCE", 0.68)
    boundaries: List[Tuple[int, float, str]] = []
    active_invoice_number = pages[0].invoice_number

    for index in range(1, len(pages)):
        split, confidence, reasons = _score_boundary(pages[index - 1], pages[index], active_invoice_number)
        if split and confidence >= min_confidence:
            boundaries.append((index + 1, confidence, ", ".join(reasons[:4])))
            active_invoice_number = pages[index].invoice_number
            continue
        if active_invoice_number is None and pages[index].invoice_number:
            active_invoice_number = pages[index].invoice_number

    if not boundaries:
        result.reason = "no_high_confidence_boundaries"
        result.segments = [SegmentInfo(page_from=1, page_to=page_count, confidence=1.0, path=source_path or None)]
        return result

    segments: List[SegmentInfo] = []
    segment_start = 1
    pending_confidence = 1.0
    pending_reason = ""
    for boundary_page, confidence, boundary_reason in boundaries:
        segments.append(
            SegmentInfo(
                page_from=segment_start,
                page_to=boundary_page - 1,
                confidence=pending_confidence,
                reason=pending_reason,
            )
        )
        segment_start = boundary_page
        pending_confidence = confidence
        pending_reason = boundary_reason
    segments.append(
        SegmentInfo(
            page_from=segment_start,
            page_to=page_count,
            confidence=pending_confidence,
            reason=pending_reason,
        )
    )

    result.used_segmentation = len(segments) > 1
    result.reason = "split_detected" if result.used_segmentation else "single_document"
    result.segments = segments if segments else [SegmentInfo(page_from=1, page_to=page_count, confidence=1.0, path=source_path or None)]
    return result


def _write_segment_files(pdf_path: str, result: SegmentationResult) -> SegmentationResult:
    if not result.used_segmentation:
        if not result.segments:
            result.segments = [SegmentInfo(page_from=1, page_to=result.page_count, confidence=1.0, path=pdf_path)]
        else:
            result.segments[0].path = pdf_path
        return result

    if PdfReader is None or PdfWriter is None:
        result.used_segmentation = False
        result.reason = "pypdf_unavailable_for_split_write"
        result.segments = [SegmentInfo(page_from=1, page_to=result.page_count, confidence=1.0, path=pdf_path)]
        return result

    source = Path(pdf_path)
    segment_dir = source.parent / f"{source.stem}_segments"
    segment_dir.mkdir(parents=True, exist_ok=True)

    reader = PdfReader(str(source))
    for index, segment in enumerate(result.segments, start=1):
        writer = PdfWriter()
        for page_number in range(segment.page_from - 1, segment.page_to):
            writer.add_page(reader.pages[page_number])
        output_path = segment_dir / f"{source.stem}_part{index:03d}_p{segment.page_from}-p{segment.page_to}.pdf"
        with open(output_path, "wb") as handle:
            writer.write(handle)
        segment.path = str(output_path)
    return result


def _write_metadata(result: SegmentationResult) -> SegmentationResult:
    if not result.source_path:
        return result
    source = Path(result.source_path)
    metadata_path = source.with_suffix(".segments.json")
    payload = result.to_dict()
    payload["feature_enabled"] = _env_bool("ENABLE_PDF_SEGMENTATION", True)
    with open(metadata_path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)
    result.metadata_path = str(metadata_path)
    return result


def segment_pdf_document(pdf_path: str) -> SegmentationResult:
    source = Path(pdf_path)
    if not _env_bool("ENABLE_PDF_SEGMENTATION", True):
        return SegmentationResult(
            source_path=str(source),
            page_count=1,
            strategy=SEGMENT_STRATEGY,
            used_segmentation=False,
            reason="segmentation_disabled",
            segments=[SegmentInfo(page_from=1, page_to=1, confidence=1.0, path=str(source))],
        )
    if source.suffix.lower() != ".pdf":
        return SegmentationResult(
            source_path=str(source),
            page_count=1,
            strategy=SEGMENT_STRATEGY,
            used_segmentation=False,
            reason="not_pdf",
            segments=[SegmentInfo(page_from=1, page_to=1, confidence=1.0, path=str(source))],
        )
    if PdfReader is None:
        return SegmentationResult(
            source_path=str(source),
            page_count=1,
            strategy=SEGMENT_STRATEGY,
            used_segmentation=False,
            reason="pypdf_not_installed",
            segments=[SegmentInfo(page_from=1, page_to=1, confidence=1.0, path=str(source))],
        )

    try:
        reader = PdfReader(str(source))
        page_texts = []
        for page in reader.pages:
            try:
                page_texts.append(page.extract_text() or "")
            except Exception:
                page_texts.append("")
        result = segment_text_pages(page_texts, source_path=str(source))
        result = _write_segment_files(str(source), result)
        return _write_metadata(result)
    except Exception as exc:
        return SegmentationResult(
            source_path=str(source),
            page_count=1,
            strategy=SEGMENT_STRATEGY,
            used_segmentation=False,
            reason=f"segmentation_error: {exc}",
            segments=[SegmentInfo(page_from=1, page_to=1, confidence=1.0, path=str(source))],
        )


def build_segmentation_log_lines(filename: str, result: SegmentationResult) -> List[str]:
    lines: List[str] = []
    if result.used_segmentation:
        lines.append(
            f"Detected {len(result.segments)} invoice documents in {filename}; "
            f"page ranges: {', '.join([f'{segment.page_from}-{segment.page_to}' for segment in result.segments])}"
        )
    elif result.reason in {"pypdf_not_installed", "segmentation_disabled"} or result.reason.startswith("segmentation_error:"):
        lines.append(f"PDF boundary detection skipped for {filename} ({result.reason}).")
    elif result.page_count > 1 and result.reason not in {"not_pdf", "single_page_pdf"}:
        lines.append(
            f"Checked {result.page_count}-page PDF for invoice boundaries and kept it as one document "
            f"({result.reason})."
        )
    if result.metadata_path:
        lines.append(f"Segmentation metadata saved to {result.metadata_path}")
    return lines
