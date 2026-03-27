from __future__ import annotations

import json
import queue
import threading
import time
from datetime import datetime, timezone
from typing import Dict, Generator, Optional


_subscribers: set[queue.Queue] = set()
_lock = threading.Lock()
_revision = 0
_last_event_type = "startup"
_last_event_at = datetime.now(timezone.utc)


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _snapshot(event_type: Optional[str] = None, payload: Optional[Dict] = None) -> Dict:
    return {
        "revision": _revision,
        "eventType": event_type or _last_event_type,
        "occurredAt": _last_event_at.isoformat(),
        "serverTime": _iso_now(),
        "payload": payload or {},
    }


def _format_sse(event_name: str, payload: Dict) -> str:
    return f"event: {event_name}\ndata: {json.dumps(payload, separators=(',', ':'))}\n\n"


def publish_live_update(event_type: str, payload: Optional[Dict] = None) -> Dict:
    global _revision, _last_event_type, _last_event_at

    with _lock:
        _revision += 1
        _last_event_type = event_type
        _last_event_at = datetime.now(timezone.utc)
        event = _snapshot(event_type, payload)
        subscribers = list(_subscribers)

    for subscriber in subscribers:
        try:
            subscriber.put_nowait(event)
        except queue.Full:
            try:
                subscriber.get_nowait()
            except queue.Empty:
                pass
            try:
                subscriber.put_nowait(event)
            except queue.Full:
                pass

    return event


def stream_live_updates(*, heartbeat_seconds: float = 1.0) -> Generator[str, None, None]:
    subscriber: queue.Queue = queue.Queue(maxsize=50)
    with _lock:
        _subscribers.add(subscriber)
        initial = _snapshot("connected", {})

    try:
        yield "retry: 1000\n\n"
        yield _format_sse("ready", initial)
        while True:
            try:
                event = subscriber.get(timeout=heartbeat_seconds)
                yield _format_sse("change", event)
            except queue.Empty:
                yield _format_sse("heartbeat", _snapshot("heartbeat", {}))
    finally:
        with _lock:
            _subscribers.discard(subscriber)


def start_live_clock() -> None:
    # Reserved for future shared heartbeat work. Kept for import symmetry.
    return None
