from typing import Any, Dict, Optional, Tuple

from pdf_segmentation import SegmentationResult


def _get_agent_db_ops():
    from agent_db import (
        record_source_document_segments,
        set_workflow_state,
        update_source_document,
        update_source_document_segment,
    )

    return update_source_document, record_source_document_segments, set_workflow_state, update_source_document_segment


def _segmentation_status(segmentation: SegmentationResult) -> str:
    if segmentation.reason == "not_pdf":
        return "not_applicable"
    if segmentation.used_segmentation:
        return "split"
    if segmentation.reason.startswith("segmentation_error:"):
        return "failed_fallback"
    return "single_document"


def persist_source_document_segmentation(
    source_document_id: str,
    segmentation: SegmentationResult,
) -> Dict[str, Any]:
    update_source_document, record_source_document_segments, set_workflow_state, _ = _get_agent_db_ops()

    source_document = update_source_document(
        source_document_id,
        page_count=segmentation.page_count,
        segmentation_status=_segmentation_status(segmentation),
        metadata={
            "segmentation": {
                "reason": segmentation.reason,
                "strategy": segmentation.strategy,
                "used_segmentation": segmentation.used_segmentation,
                "metadata_path": segmentation.metadata_path,
            }
        },
    )

    segments = [segment.to_dict() for segment in segmentation.segments] if segmentation.segments else []
    if not segments:
        segments = [
            {
                "page_from": 1,
                "page_to": max(1, segmentation.page_count),
                "confidence": 1.0,
                "path": segmentation.source_path or None,
                "reason": segmentation.reason,
            }
        ]

    recorded_segments = record_source_document_segments(
        source_document_id,
        [
            {
                "segment_index": index,
                "page_from": segment["page_from"],
                "page_to": segment["page_to"],
                "segment_path": segment.get("path"),
                "confidence": segment.get("confidence"),
                "status": "ready",
                "metadata": {
                    "reason": segment.get("reason"),
                    "segmentation_reason": segmentation.reason,
                    "strategy": segmentation.strategy,
                },
            }
            for index, segment in enumerate(segments, start=1)
        ],
        replace_existing=True,
    )

    set_workflow_state(
        "source_document",
        source_document_id,
        "segmented",
        current_stage="intake",
        event_type="segmentation_persisted",
        reason=f"Persisted {len(recorded_segments)} segment(s) before OCR",
        metadata={
            "source_document_id": source_document_id,
            "page_count": segmentation.page_count,
            "segment_count": len(recorded_segments),
            "used_segmentation": segmentation.used_segmentation,
            "segmentation_reason": segmentation.reason,
        },
    )

    return {
        "source_document": source_document,
        "segments": recorded_segments,
    }


def persist_source_document_segmentation_best_effort(
    source_document_id: Optional[str],
    segmentation: SegmentationResult,
) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    if not source_document_id:
        return None, None
    try:
        return persist_source_document_segmentation(source_document_id, segmentation), None
    except Exception as exc:
        return None, str(exc)


def update_source_document_segment_best_effort(
    segment_id: Optional[str],
    *,
    status: Optional[str] = None,
    invoice_id: Optional[str] = None,
    metadata: Optional[Dict[str, Any]] = None,
) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    if not segment_id:
        return None, None
    try:
        _, _, _, update_source_document_segment = _get_agent_db_ops()
        return (
            update_source_document_segment(
                segment_id,
                status=status,
                invoice_id=invoice_id,
                metadata=metadata,
            ),
            None,
        )
    except Exception as exc:
        return None, str(exc)
