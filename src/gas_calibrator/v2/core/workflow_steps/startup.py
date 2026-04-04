from __future__ import annotations

from typing import Any

from ..event_bus import EventBus, EventType
from ..session import RunSession


class StartupStep:
    def __init__(self, session: RunSession, event_bus: EventBus, service: Any):
        self.session = session
        self.event_bus = event_bus
        self.service = service

    def execute(self) -> None:
        self.event_bus.publish(EventType.WORKFLOW_STARTED, {"run_id": self.session.run_id})
        self.service._run_initialization_impl()

    def can_skip(self) -> bool:
        return False
