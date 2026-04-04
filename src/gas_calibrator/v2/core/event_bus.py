from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
import threading
from typing import Any, Callable, DefaultDict


class EventType(Enum):
    WORKFLOW_STARTED = "workflow_started"
    WORKFLOW_COMPLETED = "workflow_completed"
    PHASE_CHANGED = "phase_changed"
    POINT_STARTED = "point_started"
    POINT_COMPLETED = "point_completed"
    STABILITY_PASSED = "stability_passed"
    SAMPLE_COLLECTED = "sample_collected"
    DEVICE_ERROR = "device_error"
    WARNING_RAISED = "warning_raised"


@dataclass(frozen=True)
class Event:
    type: EventType
    data: Any
    timestamp: datetime


class EventBus:
    """Thread-safe in-process event bus."""

    def __init__(self) -> None:
        self._handlers: DefaultDict[EventType, list[Callable[[Event], None]]] = defaultdict(list)
        self._lock = threading.RLock()

    def subscribe(self, event_type: EventType, handler: Callable[[Event], None]) -> None:
        with self._lock:
            handlers = self._handlers[event_type]
            if handler not in handlers:
                handlers.append(handler)

    def unsubscribe(self, event_type: EventType, handler: Callable[[Event], None]) -> None:
        with self._lock:
            handlers = self._handlers.get(event_type)
            if not handlers:
                return
            self._handlers[event_type] = [item for item in handlers if item is not handler]
            if not self._handlers[event_type]:
                self._handlers.pop(event_type, None)

    def publish(self, event_type: EventType, data: Any = None) -> None:
        event = Event(type=event_type, data=data, timestamp=datetime.now())
        with self._lock:
            handlers = list(self._handlers.get(event_type, []))
        for handler in handlers:
            handler(event)

    def clear(self) -> None:
        with self._lock:
            self._handlers.clear()
