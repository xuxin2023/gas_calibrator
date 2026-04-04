from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional

from .models import CalibrationPhase, CalibrationPoint


@dataclass
class RouteContext:
    """Lightweight route execution state exposed to runners and future UI consumers."""

    current_route: str = ""
    current_phase: Optional[CalibrationPhase] = None
    current_point: Optional[CalibrationPoint] = None
    source_point: Optional[CalibrationPoint] = None
    active_point: Optional[CalibrationPoint] = None
    point_tag: str = ""
    retry: int = 0
    route_state: dict[str, Any] = field(default_factory=dict)

    def enter(
        self,
        *,
        current_route: str,
        current_phase: Optional[CalibrationPhase] = None,
        current_point: Optional[CalibrationPoint] = None,
        source_point: Optional[CalibrationPoint] = None,
        active_point: Optional[CalibrationPoint] = None,
        point_tag: str = "",
        retry: int = 0,
        route_state: Optional[dict[str, Any]] = None,
    ) -> None:
        self.current_route = str(current_route or "").strip().lower()
        self.current_phase = current_phase
        self.current_point = current_point
        self.source_point = current_point if source_point is None else source_point
        self.active_point = current_point if active_point is None else active_point
        self.point_tag = str(point_tag or "").strip()
        self.retry = max(0, int(retry))
        self.route_state = dict(route_state or {})

    def update(
        self,
        *,
        current_phase: Optional[CalibrationPhase] = None,
        current_point: Optional[CalibrationPoint] = None,
        source_point: Optional[CalibrationPoint] = None,
        active_point: Optional[CalibrationPoint] = None,
        point_tag: Optional[str] = None,
        retry: Optional[int] = None,
        route_state: Optional[dict[str, Any]] = None,
    ) -> None:
        if current_phase is not None:
            self.current_phase = current_phase
        if current_point is not None:
            self.current_point = current_point
            if active_point is None:
                self.active_point = current_point
        if source_point is not None:
            self.source_point = source_point
        if active_point is not None:
            self.active_point = active_point
        if point_tag is not None:
            self.point_tag = str(point_tag or "").strip()
        if retry is not None:
            self.retry = max(0, int(retry))
        if route_state:
            self.route_state.update(route_state)

    def clear(self) -> None:
        self.current_route = ""
        self.current_phase = None
        self.current_point = None
        self.source_point = None
        self.active_point = None
        self.point_tag = ""
        self.retry = 0
        self.route_state.clear()
