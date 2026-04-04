from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

from .enums import RunStatus, WorkflowPhase


@dataclass
class RunContext:
    """Top-level context for one calibration run."""

    run_id: str
    task_name: str
    started_at: datetime
    output_dir: str
    config_path: Optional[str] = None
    points_path: Optional[str] = None
    operator: Optional[str] = None
    status: RunStatus = RunStatus.IDLE
    current_phase: WorkflowPhase = WorkflowPhase.STARTUP
    current_point_index: Optional[int] = None
    message: str = ""


@dataclass
class RunSummary:
    """Summary of a completed or interrupted run."""

    run_id: str
    status: RunStatus
    total_points: int
    passed_points: int
    failed_points: int
    started_at: datetime
    ended_at: datetime
    duration_sec: float
    warnings: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
