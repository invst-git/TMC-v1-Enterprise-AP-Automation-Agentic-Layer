import os
import time
from typing import Any, Dict, Optional


_TASK_POLICIES: Dict[str, Dict[str, int]] = {
    "intake.classify": {
        "priority": 50,
        "max_attempts": 3,
        "base_retry_seconds": 15,
        "max_retry_seconds": 300,
    },
    "extract.validate": {
        "priority": 55,
        "max_attempts": 4,
        "base_retry_seconds": 30,
        "max_retry_seconds": 900,
    },
    "matching.evaluate": {
        "priority": 60,
        "max_attempts": 4,
        "base_retry_seconds": 30,
        "max_retry_seconds": 900,
    },
    "matching.resolve_exception": {
        "priority": 65,
        "max_attempts": 3,
        "base_retry_seconds": 45,
        "max_retry_seconds": 1200,
    },
    "payment.authorize": {
        "priority": 90,
        "max_attempts": 1,
        "base_retry_seconds": 0,
        "max_retry_seconds": 0,
    },
    "payment.route": {
        "priority": 90,
        "max_attempts": 1,
        "base_retry_seconds": 0,
        "max_retry_seconds": 0,
    },
    "payment.execute": {
        "priority": 95,
        "max_attempts": 1,
        "base_retry_seconds": 0,
        "max_retry_seconds": 0,
    },
}


def _env_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None:
        return default
    try:
        return int(value)
    except Exception:
        return default


def _get_agent_db_ops():
    from agent_db import enqueue_agent_task, get_agent_task

    return {
        "enqueue_agent_task": enqueue_agent_task,
        "get_agent_task": get_agent_task,
    }


def get_task_policy(task_type: str) -> Dict[str, int]:
    base_retry_seconds = _env_int("AGENT_WORKER_RETRY_DELAY_SECONDS", 60)
    max_retry_seconds = _env_int("AGENT_WORKER_RETRY_DELAY_MAX_SECONDS", 1800)
    policy = dict(
        _TASK_POLICIES.get(
            task_type,
            {
                "priority": 100,
                "max_attempts": 3,
                "base_retry_seconds": base_retry_seconds,
                "max_retry_seconds": max_retry_seconds,
            },
        )
    )
    policy["base_retry_seconds"] = max(0, int(policy.get("base_retry_seconds", base_retry_seconds)))
    policy["max_retry_seconds"] = max(
        policy["base_retry_seconds"],
        int(policy.get("max_retry_seconds", max_retry_seconds)),
    )
    policy["max_attempts"] = max(1, int(policy.get("max_attempts", 3)))
    policy["priority"] = max(0, int(policy.get("priority", 100)))
    return policy


def get_task_max_attempts(task_type: str) -> int:
    return get_task_policy(task_type)["max_attempts"]


def compute_retry_delay_seconds(task_type: str, attempt_count: int) -> int:
    policy = get_task_policy(task_type)
    if policy["max_attempts"] <= 1:
        return 0
    base_delay_seconds = max(1, int(policy["base_retry_seconds"]))
    exponent = max(0, int(attempt_count or 1) - 1)
    delay_seconds = base_delay_seconds * (2 ** exponent)
    return min(int(policy["max_retry_seconds"]), int(delay_seconds))


def enqueue_workflow_task(
    *,
    task_type: str,
    entity_type: str,
    entity_id: str,
    source_document_id: Optional[str] = None,
    priority: Optional[int] = None,
    max_attempts: Optional[int] = None,
    dedupe_key: Optional[str] = None,
    available_at: Optional[Any] = None,
    payload: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    db_ops = _get_agent_db_ops()
    policy = get_task_policy(task_type)
    return db_ops["enqueue_agent_task"](
        task_type=task_type,
        entity_type=entity_type,
        entity_id=entity_id,
        source_document_id=source_document_id,
        priority=policy["priority"] if priority is None else priority,
        max_attempts=policy["max_attempts"] if max_attempts is None else max_attempts,
        dedupe_key=dedupe_key,
        available_at=available_at,
        payload=payload or {},
    )


def wait_for_task_completion(
    task_id: str,
    *,
    timeout_seconds: float = 20.0,
    poll_interval_seconds: float = 0.25,
) -> Optional[Dict[str, Any]]:
    db_ops = _get_agent_db_ops()
    deadline = time.time() + max(0.1, float(timeout_seconds))
    latest_task = None
    while time.time() <= deadline:
        latest_task = db_ops["get_agent_task"](task_id)
        if latest_task is None:
            return None
        if latest_task["status"] in {"completed", "failed", "dead_letter", "canceled"}:
            return latest_task
        time.sleep(max(0.05, float(poll_interval_seconds)))
    return latest_task
