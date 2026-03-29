import datetime
import os
from typing import Any, Dict, List, Optional

from db import get_conn
from realtime_events import publish_live_update

SYSTEM_AGENT_NAME = "sla_monitor_agent"
SYSTEM_MODEL_NAME = "deterministic_rules_v1"
TERMINAL_INVOICE_STATUSES = {"paid", "duplicate_blocked"}

DEFAULT_INVOICE_SLA_CONFIGS = (
    {
        "state_name": "received",
        "warning_minutes": 15,
        "breach_minutes": 30,
        "escalation_queue": "invoice_ingest",
        "metadata": {"description": "Invoice should move out of intake quickly after persistence."},
    },
    {
        "state_name": "unmatched",
        "warning_minutes": 120,
        "breach_minutes": 240,
        "escalation_queue": "po_matching",
        "metadata": {"description": "Unmatched invoices should be resolved or reviewed within the same business window."},
    },
    {
        "state_name": "vendor_mismatch",
        "warning_minutes": 120,
        "breach_minutes": 240,
        "escalation_queue": "po_matching",
        "metadata": {"description": "Vendor mismatches should be corrected before payment readiness."},
    },
    {
        "state_name": "needs_review",
        "warning_minutes": 120,
        "breach_minutes": 240,
        "escalation_queue": "po_matching",
        "metadata": {"description": "Human review backlog should be cleared within the same operating shift."},
    },
    {
        "state_name": "matched_auto",
        "warning_minutes": 240,
        "breach_minutes": 480,
        "escalation_queue": "payments",
        "metadata": {"description": "Matched invoices should be handed to payment readiness within one working day."},
    },
    {
        "state_name": "ready_for_payment",
        "warning_minutes": 720,
        "breach_minutes": 1440,
        "escalation_queue": "payments",
        "metadata": {"description": "Ready invoices should be scheduled for payment within one day."},
    },
    {
        "state_name": "payment_pending",
        "warning_minutes": 30,
        "breach_minutes": 120,
        "escalation_queue": "payments",
        "metadata": {"description": "Payment intents should be confirmed or cleared quickly once created."},
    },
    {
        "state_name": "paid",
        "warning_minutes": 1440,
        "breach_minutes": 2880,
        "escalation_queue": "payments",
        "metadata": {"description": "Terminal paid state is seeded for completeness but not monitored."},
    },
)


def _env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None:
        return default
    try:
        return int(value)
    except Exception:
        return default


def _utcnow() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc)


def _parse_dt(value: Any) -> Optional[datetime.datetime]:
    if value is None:
        return None
    if isinstance(value, datetime.datetime):
        return value if value.tzinfo else value.replace(tzinfo=datetime.timezone.utc)
    text = str(value)
    if not text:
        return None
    try:
        parsed = datetime.datetime.fromisoformat(text.replace("Z", "+00:00"))
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=datetime.timezone.utc)
    except Exception:
        return None


def _invoice_stage_for_status(status: str) -> str:
    lowered = (status or "").strip().lower()
    if lowered in {"received"}:
        return "intake"
    if lowered in {"matched_auto", "ready_for_payment", "payment_pending", "paid"}:
        return "payments"
    return "matching"


def _compute_breach_risk(
    *,
    age_minutes: float,
    warning_minutes: int,
    breach_minutes: int,
    previous_risk: str,
) -> str:
    previous = (previous_risk or "ok").strip().lower() or "ok"
    if age_minutes < float(warning_minutes):
        return "ok"
    if age_minutes < float(breach_minutes):
        return "warning"
    if previous in {"breaching", "breached"}:
        return "breached"
    return "breaching"


def _risk_reasoning(
    *,
    current_state: str,
    previous_risk: str,
    next_risk: str,
    age_minutes: float,
    warning_minutes: int,
    breach_minutes: int,
) -> str:
    return (
        f"Invoice has been in state {current_state} for {age_minutes:.2f} minutes. "
        f"Warning threshold is {warning_minutes} minutes and breach threshold is {breach_minutes} minutes. "
        f"Breach risk moved from {previous_risk or 'ok'} to {next_risk} because the current age "
        f"{'is below both thresholds' if next_risk == 'ok' else 'crossed the warning threshold' if next_risk == 'warning' else 'crossed the breach threshold for the first monitor cycle' if next_risk == 'breaching' else 'remained above the breach threshold across consecutive monitor cycles'}."
    )


def _stalled_task_reasoning(task: Dict[str, Any], *, stale_after_seconds: int, outcome: str) -> str:
    return (
        f"Task {task.get('task_type')} for {task.get('entity_type')} {task.get('entity_id')} "
        f"was still {task.get('status')} after {stale_after_seconds} seconds without a fresh lease heartbeat. "
        f"Attempt {task.get('attempt_count')} of {task.get('max_attempts')} resulted in {outcome}."
    )


def _record_decision(
    *,
    deps: Dict[str, Any],
    entity_type: str,
    entity_id: str,
    task_type: str,
    decision_type: str,
    decision: str,
    reasoning_summary: str,
    confidence: float,
    metadata: Dict[str, Any],
    source_document_id: Optional[str] = None,
) -> None:
    task = deps["create_completed_agent_task"](
        task_type=task_type,
        entity_type=entity_type,
        entity_id=entity_id,
        source_document_id=source_document_id,
        priority=40,
        payload=metadata,
        result={
            "decision": decision,
            "decision_type": decision_type,
            "metadata": metadata,
        },
    )
    deps["record_agent_decision"](
        task_id=task["id"],
        entity_type=entity_type,
        entity_id=entity_id,
        agent_name=SYSTEM_AGENT_NAME,
        model_name=SYSTEM_MODEL_NAME,
        prompt_version=SYSTEM_MODEL_NAME,
        decision_type=decision_type,
        decision=decision,
        confidence=confidence,
        reasoning_summary=reasoning_summary,
        metadata=metadata,
    )


def _get_dependencies() -> Dict[str, Any]:
    from agent_db import (
        create_completed_agent_task,
        dead_letter_agent_task,
        list_sla_configs,
        list_stalled_agent_tasks,
        record_agent_decision,
        record_workflow_history_event,
        requeue_agent_task,
        update_workflow_breach_risk,
        upsert_sla_config,
        upsert_workflow_state_snapshot,
    )

    return {
        "get_conn": get_conn,
        "list_sla_configs": list_sla_configs,
        "upsert_sla_config": upsert_sla_config,
        "upsert_workflow_state_snapshot": upsert_workflow_state_snapshot,
        "update_workflow_breach_risk": update_workflow_breach_risk,
        "record_workflow_history_event": record_workflow_history_event,
        "create_completed_agent_task": create_completed_agent_task,
        "record_agent_decision": record_agent_decision,
        "list_stalled_agent_tasks": list_stalled_agent_tasks,
        "requeue_agent_task": requeue_agent_task,
        "dead_letter_agent_task": dead_letter_agent_task,
        "publish_live_update": publish_live_update,
    }


def _seed_default_sla_configs_if_empty(deps: Dict[str, Any]) -> int:
    existing = deps["list_sla_configs"](entity_type="invoice", active_only=False)
    if existing:
        return 0
    for config in DEFAULT_INVOICE_SLA_CONFIGS:
        deps["upsert_sla_config"](
            entity_type="invoice",
            state_name=config["state_name"],
            target_minutes=config["breach_minutes"],
            warning_minutes=config["warning_minutes"],
            breach_minutes=config["breach_minutes"],
            escalation_queue=config["escalation_queue"],
            is_active=True,
            metadata=config["metadata"],
        )
    return len(DEFAULT_INVOICE_SLA_CONFIGS)


def _load_invoice_snapshots(deps: Dict[str, Any]) -> List[Dict[str, Any]]:
    with deps["get_conn"]() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                  i.id,
                  i.status,
                  i.created_at,
                  i.updated_at,
                  ws.current_state,
                  ws.current_stage,
                  ws.confidence,
                  ws.breach_risk,
                  ws.updated_at,
                  ws.metadata
                FROM invoices AS i
                LEFT JOIN workflow_states AS ws
                  ON ws.entity_type = 'invoice'
                 AND ws.entity_id = i.id
                WHERE i.status NOT IN ('paid', 'duplicate_blocked')
                ORDER BY COALESCE(ws.updated_at, i.updated_at, i.created_at) ASC, i.id ASC
                """
            )
            snapshots = []
            for row in cur.fetchall():
                snapshots.append(
                    {
                        "invoice_id": str(row[0]),
                        "invoice_status": row[1],
                        "invoice_created_at": row[2],
                        "invoice_updated_at": row[3],
                        "workflow_state": row[4],
                        "workflow_stage": row[5],
                        "workflow_confidence": float(row[6]) if row[6] is not None else None,
                        "breach_risk": row[7] or "ok",
                        "state_updated_at": row[8],
                        "workflow_metadata": row[9] or {},
                    }
                )
            return snapshots


def _sync_invoice_workflow_state(deps: Dict[str, Any], snapshot: Dict[str, Any]) -> Dict[str, Any]:
    desired_state = snapshot["invoice_status"]
    state_timestamp = snapshot.get("invoice_updated_at") or snapshot.get("invoice_created_at") or _utcnow()
    synced = deps["upsert_workflow_state_snapshot"](
        "invoice",
        snapshot["invoice_id"],
        desired_state,
        current_stage=_invoice_stage_for_status(desired_state),
        confidence=snapshot.get("workflow_confidence"),
        breach_risk="ok" if snapshot.get("workflow_state") != desired_state else (snapshot.get("breach_risk") or "ok"),
        metadata={
            "sla_monitor_sync": {
                "invoice_status": desired_state,
                "synced_at": _utcnow().isoformat(),
            }
        },
        state_updated_at=state_timestamp,
    )
    if snapshot.get("workflow_state") != desired_state:
        deps["record_workflow_history_event"](
            "invoice",
            snapshot["invoice_id"],
            event_type="state_synchronized",
            reason="SLA monitor synchronized the workflow snapshot with the invoice status.",
            actor_type="system",
            actor_id=SYSTEM_AGENT_NAME,
            metadata={
                "from_status": snapshot.get("workflow_state"),
                "to_status": desired_state,
            },
        )
    return synced


def run_sla_monitor_once(*, triggered_by: str = "scheduler") -> Dict[str, Any]:
    deps = _get_dependencies()
    seeded_config_count = _seed_default_sla_configs_if_empty(deps)
    configs = deps["list_sla_configs"](entity_type="invoice", active_only=True)
    configs_by_state = {config["state_name"]: config for config in configs}
    now = _utcnow()

    monitored_invoice_count = 0
    risk_changes = 0
    warning_event_count = 0
    breached_event_count = 0

    for snapshot in _load_invoice_snapshots(deps):
        synced_state = _sync_invoice_workflow_state(deps, snapshot)
        current_state = synced_state["current_state"]
        config = configs_by_state.get(current_state)
        if not config:
            continue
        monitored_invoice_count += 1
        state_updated_at = _parse_dt(synced_state.get("updated_at")) or now
        age_minutes = max(0.0, (now - state_updated_at).total_seconds() / 60.0)
        previous_risk = synced_state.get("breach_risk") or "ok"
        next_risk = _compute_breach_risk(
            age_minutes=age_minutes,
            warning_minutes=int(config["warning_minutes"]),
            breach_minutes=int(config["breach_minutes"]),
            previous_risk=previous_risk,
        )
        if next_risk == previous_risk:
            continue

        risk_changes += 1
        reasoning = _risk_reasoning(
            current_state=current_state,
            previous_risk=previous_risk,
            next_risk=next_risk,
            age_minutes=age_minutes,
            warning_minutes=int(config["warning_minutes"]),
            breach_minutes=int(config["breach_minutes"]),
        )
        metadata = {
            "triggered_by": triggered_by,
            "invoice_id": snapshot["invoice_id"],
            "current_state": current_state,
            "previous_risk": previous_risk,
            "next_risk": next_risk,
            "age_minutes": round(age_minutes, 2),
            "warning_minutes": int(config["warning_minutes"]),
            "breach_minutes": int(config["breach_minutes"]),
            "state_updated_at": synced_state.get("updated_at"),
        }

        deps["update_workflow_breach_risk"](
            "invoice",
            snapshot["invoice_id"],
            next_risk,
            metadata={"sla_monitor": metadata},
        )
        deps["record_workflow_history_event"](
            "invoice",
            snapshot["invoice_id"],
            event_type="sla_risk_changed",
            reason=reasoning,
            actor_type="agent",
            actor_id=SYSTEM_AGENT_NAME,
            metadata={"breach_risk": next_risk, **metadata},
        )
        _record_decision(
            deps=deps,
            entity_type="invoice",
            entity_id=snapshot["invoice_id"],
            task_type="sla.monitor",
            decision_type="sla_risk_update",
            decision=next_risk,
            reasoning_summary=reasoning,
            confidence=1.0,
            metadata=metadata,
        )

        if next_risk == "warning":
            warning_event_count += 1
        if next_risk == "breached":
            breached_event_count += 1

        if next_risk in {"warning", "breached"}:
            deps["publish_live_update"](
                "invoice.sla_risk_changed",
                {
                    "invoiceId": snapshot["invoice_id"],
                    "currentState": current_state,
                    "breachRisk": next_risk,
                    "ageMinutes": round(age_minutes, 2),
                    "warningMinutes": int(config["warning_minutes"]),
                    "breachMinutes": int(config["breach_minutes"]),
                },
            )

    lease_seconds = _env_int("AGENT_WORKER_LEASE_SECONDS", 300)
    stalled_after_seconds = _env_int("SLA_MONITOR_STALLED_TASK_SECONDS", max(lease_seconds * 2, 60))
    stalled_task_count = 0
    requeued_task_count = 0
    dead_letter_task_count = 0

    for task in deps["list_stalled_agent_tasks"](stale_after_seconds=stalled_after_seconds, limit=200):
        stalled_task_count += 1
        metadata = {
            "task_id": task["id"],
            "task_type": task["task_type"],
            "entity_type": task["entity_type"],
            "entity_id": task["entity_id"],
            "attempt_count": task["attempt_count"],
            "max_attempts": task["max_attempts"],
            "stale_after_seconds": stalled_after_seconds,
            "lease_expires_at": task.get("lease_expires_at"),
            "heartbeat_at": task.get("heartbeat_at"),
            "triggered_by": triggered_by,
        }
        if int(task.get("attempt_count") or 0) >= int(task.get("max_attempts") or 0):
            deps["dead_letter_agent_task"](
                task["id"],
                error_message="Task exceeded the maximum retry budget after stalling.",
                error_details=metadata,
            )
            reasoning = _stalled_task_reasoning(task, stale_after_seconds=stalled_after_seconds, outcome="dead_lettered")
            _record_decision(
                deps=deps,
                entity_type=task["entity_type"],
                entity_id=task["entity_id"],
                task_type="sla.monitor.task_recovery",
                decision_type="stalled_task_recovery",
                decision="dead_lettered",
                reasoning_summary=reasoning,
                confidence=1.0,
                metadata=metadata,
                source_document_id=task.get("source_document_id"),
            )
            deps["publish_live_update"](
                "agent.task_dead_lettered",
                {
                    "taskId": task["id"],
                    "taskType": task["task_type"],
                    "entityType": task["entity_type"],
                    "entityId": task["entity_id"],
                    "attemptCount": task["attempt_count"],
                    "maxAttempts": task["max_attempts"],
                },
            )
            dead_letter_task_count += 1
            continue

        deps["requeue_agent_task"](
            task["id"],
            error_message="Task stalled beyond the lease window and was returned to the queue.",
            error_details=metadata,
            retry_delay_seconds=0,
        )
        reasoning = _stalled_task_reasoning(task, stale_after_seconds=stalled_after_seconds, outcome="requeued")
        _record_decision(
            deps=deps,
            entity_type=task["entity_type"],
            entity_id=task["entity_id"],
            task_type="sla.monitor.task_recovery",
            decision_type="stalled_task_recovery",
            decision="requeued",
            reasoning_summary=reasoning,
            confidence=1.0,
            metadata=metadata,
            source_document_id=task.get("source_document_id"),
        )
        requeued_task_count += 1

    return {
        "triggered_by": triggered_by,
        "seeded_config_count": seeded_config_count,
        "monitored_invoice_count": monitored_invoice_count,
        "risk_changes": risk_changes,
        "warning_event_count": warning_event_count,
        "breached_event_count": breached_event_count,
        "stalled_task_count": stalled_task_count,
        "requeued_task_count": requeued_task_count,
        "dead_letter_task_count": dead_letter_task_count,
        "ran_at": now.isoformat(),
    }
