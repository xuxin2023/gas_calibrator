from __future__ import annotations

from dataclasses import dataclass, field

from ..models import CalibrationPoint


@dataclass
class RouteRunResult:
    success: bool
    completed_points: list[CalibrationPoint] = field(default_factory=list)
    completed_point_indices: list[int] = field(default_factory=list)
    sampled_points: list[CalibrationPoint] = field(default_factory=list)
    sampled_point_indices: list[int] = field(default_factory=list)
    skipped_point_indices: list[int] = field(default_factory=list)
    stopped: bool = False
    error: str | None = None
