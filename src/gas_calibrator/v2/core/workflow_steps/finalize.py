from __future__ import annotations

from typing import Any

from ..event_bus import EventBus
from ..session import RunSession


class FinalizeStep:
    def __init__(self, session: RunSession, event_bus: EventBus, service: Any):
        self.session = session
        self.event_bus = event_bus
        self.service = service

    def execute(self) -> None:
        self.service._run_finalization_impl()

    def can_skip(self) -> bool:
        return False
