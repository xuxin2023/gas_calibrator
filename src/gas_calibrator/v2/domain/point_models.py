from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from .enums import PointStatus, WorkflowPhase


@dataclass
class CalibrationPoint:
    """Platform-level calibration point definition."""

    index: int
    name: str
    enabled: bool = True
    target_temperature_c: Optional[float] = None
    target_pressure: Optional[float] = None
    target_h2o: Optional[float] = None
    target_co2: Optional[float] = None
    sample_seconds: int = 30
    stability_seconds: int = 60
    remarks: str = ""


@dataclass
class PointExecutionState:
    """Execution state for one calibration point."""

    point_index: int
    status: PointStatus = PointStatus.PENDING
    phase: WorkflowPhase = WorkflowPhase.POINT_EXECUTION
    started_at: Optional[datetime] = None
    ended_at: Optional[datetime] = None
    stable: bool = False
    rejected: bool = False
    reject_reason: str = ""
    sample_count: int = 0
