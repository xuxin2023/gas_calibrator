from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Optional

# Shared validation owner; these are in-memory simulation models only.
# The QC implementation remains V2-owned; this keeps the transport field
# structurally open without a shared-validation reverse import.
OutlierResult = Any


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


class RunMode(str, Enum):
    AUTO_CALIBRATION = "auto_calibration"
    CO2_MEASUREMENT = "co2_measurement"
    H2O_MEASUREMENT = "h2o_measurement"
    EXPERIMENT_MEASUREMENT = "experiment_measurement"


_RUN_MODE_ALIASES = {
    "": RunMode.AUTO_CALIBRATION,
    "auto": RunMode.AUTO_CALIBRATION,
    "auto_calibration": RunMode.AUTO_CALIBRATION,
    "automatic_calibration": RunMode.AUTO_CALIBRATION,
    "calibration": RunMode.AUTO_CALIBRATION,
    "co2": RunMode.CO2_MEASUREMENT,
    "co2_measurement": RunMode.CO2_MEASUREMENT,
    "co2_measure": RunMode.CO2_MEASUREMENT,
    "co2_test": RunMode.CO2_MEASUREMENT,
    "h2o": RunMode.H2O_MEASUREMENT,
    "h2o_measurement": RunMode.H2O_MEASUREMENT,
    "water": RunMode.H2O_MEASUREMENT,
    "water_measurement": RunMode.H2O_MEASUREMENT,
    "humidity_measurement": RunMode.H2O_MEASUREMENT,
    "experiment": RunMode.EXPERIMENT_MEASUREMENT,
    "experiment_measurement": RunMode.EXPERIMENT_MEASUREMENT,
    "lab": RunMode.EXPERIMENT_MEASUREMENT,
}


def normalize_run_mode(value: Any, default: RunMode = RunMode.AUTO_CALIBRATION) -> RunMode:
    if isinstance(value, RunMode):
        return value
    normalized = str(getattr(value, "value", value) or "").strip().lower()
    return _RUN_MODE_ALIASES.get(normalized, default)


def run_mode_label(value: Any) -> str:
    run_mode = normalize_run_mode(value)
    return {
        RunMode.AUTO_CALIBRATION: "自动校准",
        RunMode.CO2_MEASUREMENT: "CO2 测量",
        RunMode.H2O_MEASUREMENT: "水汽测量",
        RunMode.EXPERIMENT_MEASUREMENT: "实验测量",
    }[run_mode]


@dataclass(frozen=True)
class ModeProfile:
    run_mode: RunMode = RunMode.AUTO_CALIBRATION
    route_mode: Optional[str] = None
    formal_calibration_report: Optional[bool] = None

    @classmethod
    def from_value(cls, payload: Any = None) -> "ModeProfile":
        if isinstance(payload, ModeProfile):
            return payload

        data = dict(payload or {}) if isinstance(payload, dict) else {}
        run_mode = normalize_run_mode(data.get("run_mode", payload))
        route_mode = data.get("route_mode")
        if route_mode not in (None, ""):
            route_mode = str(route_mode).strip().lower()
        else:
            route_mode = None
        formal_calibration_report = data.get("formal_calibration_report")
        if formal_calibration_report is not None:
            formal_calibration_report = bool(formal_calibration_report)
        return cls(
            run_mode=run_mode,
            route_mode=route_mode,
            formal_calibration_report=formal_calibration_report,
        )

    def effective_route_mode(self, default: str = "h2o_then_co2") -> str:
        if self.route_mode:
            return str(self.route_mode)
        if self.run_mode == RunMode.CO2_MEASUREMENT:
            return "co2_only"
        if self.run_mode == RunMode.H2O_MEASUREMENT:
            return "h2o_only"
        return str(default or "h2o_then_co2")

    def formal_report_enabled(self) -> bool:
        if self.formal_calibration_report is not None:
            return bool(self.formal_calibration_report)
        return self.run_mode == RunMode.AUTO_CALIBRATION

    def to_dict(self) -> dict[str, Any]:
        payload = {
            "run_mode": self.run_mode.value,
        }
        if self.route_mode:
            payload["route_mode"] = str(self.route_mode)
        if self.formal_calibration_report is not None:
            payload["formal_calibration_report"] = bool(self.formal_calibration_report)
        return payload


@dataclass
class RawSample:
    """Raw sample collected from one analyzer at one timestamp."""

    timestamp: datetime
    point_index: int
    analyzer_name: str
    co2: Optional[float] = None
    h2o: Optional[float] = None
    pressure: Optional[float] = None
    temperature_c: Optional[float] = None
    dewpoint: Optional[float] = None
    chamber_temp_c: Optional[float] = None
    case_temp_c: Optional[float] = None
    extra: dict[str, Any] = field(default_factory=dict)


@dataclass
class SampleWindow:
    """Window of raw samples collected for one point."""

    point_index: int
    started_at: datetime
    ended_at: Optional[datetime] = None
    samples: list[RawSample] = field(default_factory=list)


@dataclass
class PointResult:
    """Aggregated result for a single calibration point."""

    point_index: int
    mean_co2: Optional[float] = None
    mean_h2o: Optional[float] = None
    std_co2: Optional[float] = None
    std_h2o: Optional[float] = None
    chamber_temp_c: Optional[float] = None
    case_temp_c: Optional[float] = None
    sample_count: int = 0
    stable: bool = False
    accepted: bool = True
    notes: str = ""


@dataclass
class RunArtifactManifest:
    """Run artifact manifest."""

    run_id: str
    raw_samples_file: str
    point_results_file: str
    run_summary_file: str
    config_snapshot_file: str


@dataclass
class QCDecision:
    """QC decision for one point or stage."""

    point_index: int
    level: QCLevel
    accepted: bool
    reasons: list[str] = field(default_factory=list)
    score: float = 0.0


@dataclass
class CleanedData:
    """Cleaned samples after QC filtering."""

    point_index: int
    original_count: int
    cleaned_count: int
    removed_count: int
    removed_indices: list[int] = field(default_factory=list)
    samples: list[RawSample] = field(default_factory=list)
    outlier_result: Optional["OutlierResult"] = None


@dataclass
class Recommendation:
    """Structured recommendation with rationale."""

    action: str = ""
    reason: str = ""
    details: list[str] = field(default_factory=list)
    confidence: float = 0.0
    alternatives: list[str] = field(default_factory=list)

    def explain(self) -> str:
        detail_text = f" Details: {'; '.join(self.details)}." if self.details else ""
        alt_text = f" Alternatives: {', '.join(self.alternatives)}." if self.alternatives else ""
        return f"Action={self.action or 'unspecified'} because {self.reason or 'no reason provided'}.{detail_text}{alt_text}"


@dataclass
class AlgorithmRecommendation:
    """Recommendation for algorithm selection."""

    selected_algorithm: str
    reason: str
    comparison_summary: str
    ranking: list[str] = field(default_factory=list)
    scores: dict[str, float] = field(default_factory=dict)
    recommendation: Recommendation = field(default_factory=Recommendation)

    def explain(self) -> str:
        ranking_text = f" Ranking: {', '.join(self.ranking)}." if self.ranking else ""
        return f"Selected {self.selected_algorithm} because {self.reason}. {self.comparison_summary}.{ranking_text}".strip()


@dataclass
class PointRejection:
    """Explanation for rejecting or warning on a point."""

    point_index: int
    rejected: bool
    reasons: list[str] = field(default_factory=list)
    qc_score: float = 0.0
    sample_count: int = 0
    outlier_count: int = 0
    recommendation: Recommendation = field(default_factory=Recommendation)

    def explain(self) -> str:
        state = "rejected" if self.rejected else "accepted"
        reasons = ", ".join(self.reasons) if self.reasons else "no explicit reasons"
        return f"Point {self.point_index} {state}: {reasons}. QC={self.qc_score:.2f}, samples={self.sample_count}, outliers={self.outlier_count}"


@dataclass
class RunExplanation:
    """Human-readable explanation for one run."""

    run_id: str
    total_points: int
    valid_points: int
    rejected_points: int
    algorithm_recommendation: Optional[AlgorithmRecommendation] = None
    point_rejections: list[PointRejection] = field(default_factory=list)
    overall_quality: float = 0.0
    overall_confidence: float = 0.0
    final_recommendation: Recommendation = field(default_factory=Recommendation)

    def explain(self) -> str:
        return self.to_report()

    def to_report(self) -> str:
        lines = [
            f"Run {self.run_id}: {self.valid_points}/{self.total_points} valid points, {self.rejected_points} rejected.",
            f"Overall quality={self.overall_quality:.2f}, confidence={self.overall_confidence:.2f}.",
        ]
        if self.algorithm_recommendation is not None:
            lines.append(self.algorithm_recommendation.explain())
        for rejection in self.point_rejections:
            lines.append(rejection.explain())
        if self.final_recommendation.action or self.final_recommendation.reason:
            lines.append(self.final_recommendation.explain())
        return "\n".join(lines)


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


__all__ = [
    "AlgorithmRecommendation",
    "CalibrationPoint",
    "CleanedData",
    "ModeProfile",
    "PointExecutionState",
    "PointRejection",
    "PointResult",
    "PointStatus",
    "QCDecision",
    "QCLevel",
    "RawSample",
    "Recommendation",
    "RunMode",
    "RunArtifactManifest",
    "RunContext",
    "RunExplanation",
    "RunStatus",
    "RunSummary",
    "SampleWindow",
    "WorkflowPhase",
    "normalize_run_mode",
    "run_mode_label",
]
