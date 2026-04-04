from __future__ import annotations

from typing import Any, Sequence

from ..event_bus import EventBus, EventType
from ..models import CalibrationPoint
from ..runners.h2o_route_runner import H2oRouteRunner
from ..session import RunSession


class H2oRouteStep:
    def __init__(
        self,
        session: RunSession,
        event_bus: EventBus,
        service: Any,
        points: Sequence[CalibrationPoint],
        pressure_points: Sequence[CalibrationPoint],
    ):
        self.session = session
        self.event_bus = event_bus
        self.service = service
        self.points = list(points)
        self.pressure_points = list(pressure_points)

    def execute(self) -> None:
        H2oRouteRunner(self.service, self.points, self.pressure_points).execute()

    def can_skip(self) -> bool:
        return not self.points
