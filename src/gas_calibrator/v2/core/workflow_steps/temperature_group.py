from __future__ import annotations

from typing import Any, Sequence

from ..event_bus import EventBus
from ..models import CalibrationPoint
from ..runners.temperature_group_runner import TemperatureGroupRunner
from ..session import RunSession


class TemperatureGroupStep:
    def __init__(
        self,
        session: RunSession,
        event_bus: EventBus,
        service: Any,
        points: Sequence[CalibrationPoint],
        *,
        next_group: Sequence[CalibrationPoint] | None = None,
    ):
        self.session = session
        self.event_bus = event_bus
        self.service = service
        self.points = list(points)
        self.next_group = list(next_group or [])

    def execute(self) -> None:
        TemperatureGroupRunner(self.service, self.points, next_group=self.next_group).execute()

    def can_skip(self) -> bool:
        return not self.points
