from __future__ import annotations

from typing import Any, Sequence

from ..event_bus import EventBus, EventType
from ..models import CalibrationPoint
from ..runners.co2_route_runner import Co2RouteRunner
from ..session import RunSession


class Co2RouteStep:
    def __init__(
        self,
        session: RunSession,
        event_bus: EventBus,
        service: Any,
        point: CalibrationPoint,
        pressure_points: Sequence[CalibrationPoint],
    ):
        self.session = session
        self.event_bus = event_bus
        self.service = service
        self.point = point
        self.pressure_points = list(pressure_points)

    def execute(self) -> None:
        Co2RouteRunner(self.service, self.point, self.pressure_points).execute()

    def can_skip(self) -> bool:
        return False
