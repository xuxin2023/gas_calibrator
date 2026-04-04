from __future__ import annotations

from enum import Enum


class RunStatus(str, Enum):
    """Run lifecycle status."""

    IDLE = "IDLE"
    READY = "READY"
    RUNNING = "RUNNING"
    PAUSED = "PAUSED"
    STOPPED = "STOPPED"
    FINISHED = "FINISHED"
    FAILED = "FAILED"


class WorkflowPhase(str, Enum):
    """Workflow execution phase."""

    STARTUP = "STARTUP"
    DEVICE_PREPARE = "DEVICE_PREPARE"
    POINT_EXECUTION = "POINT_EXECUTION"
    STABILITY_WAIT = "STABILITY_WAIT"
    SAMPLING = "SAMPLING"
    POINT_FINALIZE = "POINT_FINALIZE"
    RUN_FINALIZE = "RUN_FINALIZE"


class PointStatus(str, Enum):
    """Calibration-point execution status."""

    PENDING = "PENDING"
    RUNNING = "RUNNING"
    STABLE = "STABLE"
    SAMPLED = "SAMPLED"
    REJECTED = "REJECTED"
    DONE = "DONE"
    FAILED = "FAILED"


class QCLevel(str, Enum):
    """QC decision level."""

    PASS = "PASS"
    WARN = "WARN"
    FAIL = "FAIL"
