from __future__ import annotations

from typing import Any

from ..event_bus import EventBus, EventType
from ..models import CalibrationPhase, CalibrationPoint
from ..session import RunSession


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

        results = self.service._sample_point(point, phase=self.phase, point_tag=self.point_tag)
        for result in results:
            self.event_bus.publish(EventType.SAMPLE_COLLECTED, result)
        self.service._run_point_qc(point, phase=self.phase, point_tag=self.point_tag)

    def can_skip(self) -> bool:
        return False
