from __future__ import annotations

from typing import Any, Sequence

from .. import EventBus, EventType
from ..models import CalibrationPhase, CalibrationPoint
from ..runners.co2_route_runner import Co2RouteRunner
from ..runners.h2o_route_runner import H2oRouteRunner
from ..runners.temperature_group_runner import TemperatureGroupRunner
from ..models import RunSession


class PrecheckStep:
    def __init__(self, session: RunSession, event_bus: EventBus, service: Any):
        self.session = session
        self.event_bus = event_bus
        self.service = service

    def execute(self) -> None:
        self.service._run_precheck_impl()

    def can_skip(self) -> bool:
        return not bool(self.service.config.workflow.precheck.enabled)


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


class SamplingStep:
    def __init__(
        self,
        session: RunSession,
        event_bus: EventBus,
        service: Any,
        point: CalibrationPoint,
        *,
        phase: str = "",
        point_tag: str = "",
    ):
        self.session = session
        self.event_bus = event_bus
        self.service = service
        self.point = point
        self.phase = str(phase or ("h2o" if point.is_h2o_point else "co2")).strip().lower()
        self.point_tag = str(point_tag or "").strip()

    def execute(self) -> None:
        point = self.point
        self.service._check_stop()
        self.service._update_status(
            phase=CalibrationPhase.SAMPLING,
            current_point=point,
            message=f"Sampling point {point.index} ({self.phase})",
        )

        analyzers = self.service._active_gas_analyzers()
        if not analyzers:
            self.service._log("No gas analyzers registered; sampling skipped")
            return

        results = self.service._sample_point(
            point,
            phase=self.phase,
            point_tag=self.point_tag,
        )
        for result in results:
            self.event_bus.publish(EventType.SAMPLE_COLLECTED, result)
        self.service._run_point_qc(
            point,
            phase=self.phase,
            point_tag=self.point_tag,
        )

    def can_skip(self) -> bool:
        return False


class FinalizeStep:
    def __init__(self, session: RunSession, event_bus: EventBus, service: Any):
        self.session = session
        self.event_bus = event_bus
        self.service = service

    def execute(self) -> None:
        self.service._run_finalization_impl()

    def can_skip(self) -> bool:
        return False

__all__ = [
    "Co2RouteStep",
    "FinalizeStep",
    "H2oRouteStep",
    "PrecheckStep",
    "SamplingStep",
    "StartupStep",
    "TemperatureGroupStep",
]
