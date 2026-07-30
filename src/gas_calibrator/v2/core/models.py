"""
V2 核心数据模型。
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field, is_dataclass
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional, Set

from gas_calibrator.validation.simulation.runtime_point import CalibrationPoint

if TYPE_CHECKING:
    from ..config.models import AppConfig


class CalibrationPhase(Enum):
    """校准阶段。"""

    IDLE = "idle"
    INITIALIZING = "initializing"
    PRECHECK = "precheck"
    TEMPERATURE_GROUP = "temperature_group"
    H2O_ROUTE = "h2o_route"
    CO2_ROUTE = "co2_route"
    SAMPLING = "sampling"
    FINALIZING = "finalizing"
    COMPLETED = "completed"
    ERROR = "error"
    STOPPED = "stopped"


@dataclass(frozen=True)
class CalibrationStatus:
    """V2 校准状态。"""

    phase: CalibrationPhase = CalibrationPhase.IDLE
    current_point: Optional[CalibrationPoint] = None
    total_points: int = 0
    completed_points: int = 0
    progress: float = 0.0
    message: str = ""
    elapsed_s: float = 0.0
    error: Optional[str] = None


class RunSession:
    """Runtime context for a single calibration run."""

    def __init__(self, config: AppConfig):
        self.run_id: str = datetime.now().strftime("run_%Y%m%d_%H%M%S")
        self.config: AppConfig = config
        self.started_at: Optional[datetime] = None
        self.ended_at: Optional[datetime] = None
        self.phase: CalibrationPhase = CalibrationPhase.IDLE
        self.current_point: Optional[CalibrationPoint] = None
        self.total_points: int = 0
        self.completed_points: int = 0
        self.progress: float = 0.0
        self.enabled_devices: Set[str] = self._collect_enabled_devices(config)
        self.output_dir: Path = Path(config.paths.output_dir) / self.run_id
        self.stop_reason: str = ""
        self.warnings: list[str] = []
        self.errors: list[str] = []

    def start(self) -> None:
        self.started_at = datetime.now()
        self.ended_at = None
        self.stop_reason = ""
        self.phase = CalibrationPhase.IDLE
        self.current_point = None
        self.completed_points = 0
        self.progress = 0.0

    def end(self, reason: str = "") -> None:
        self.ended_at = datetime.now()
        self.stop_reason = str(reason or "")

    def add_warning(self, msg: str) -> None:
        text = str(msg or "").strip()
        if text:
            self.warnings.append(text)

    def add_error(self, msg: str) -> None:
        text = str(msg or "").strip()
        if text:
            self.errors.append(text)

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "config": self._serialize_config(self.config),
            "started_at": None if self.started_at is None else self.started_at.isoformat(timespec="seconds"),
            "ended_at": None if self.ended_at is None else self.ended_at.isoformat(timespec="seconds"),
            "phase": self.phase.value,
            "current_point": self._serialize_point(self.current_point),
            "total_points": self.total_points,
            "completed_points": self.completed_points,
            "progress": self.progress,
            "enabled_devices": sorted(self.enabled_devices),
            "output_dir": str(self.output_dir),
            "stop_reason": self.stop_reason,
            "warnings": list(self.warnings),
            "errors": list(self.errors),
        }

    @staticmethod
    def _serialize_config(config: AppConfig) -> Any:
        if is_dataclass(config):
            return asdict(config)
        return config

    @staticmethod
    def _serialize_point(point: Optional[CalibrationPoint]) -> Optional[dict[str, Any]]:
        if point is None:
            return None
        if is_dataclass(point):
            return asdict(point)
        return {
            "index": getattr(point, "index", None),
            "temperature_c": getattr(point, "temperature_c", None),
            "co2_ppm": getattr(point, "co2_ppm", None),
            "humidity_pct": getattr(point, "humidity_pct", None),
            "pressure_hpa": getattr(point, "pressure_hpa", None),
            "route": getattr(point, "route", None),
        }

    @staticmethod
    def _collect_enabled_devices(config: AppConfig) -> Set[str]:
        enabled: set[str] = set()
        devices = config.devices
        single_names = (
            "pressure_controller",
            "pressure_meter",
            "dewpoint_meter",
            "humidity_generator",
            "temperature_chamber",
            "relay_a",
            "relay_b",
        )
        for name in single_names:
            item = getattr(devices, name, None)
            if item is not None and bool(getattr(item, "enabled", True)):
                enabled.add(name)

        for index, item in enumerate(getattr(devices, "gas_analyzers", []) or []):
            if bool(getattr(item, "enabled", True)):
                enabled.add(f"gas_analyzer_{index}")
        return enabled


@dataclass(frozen=True)
class SamplingResult:
    """单条采样结果。"""

    point: CalibrationPoint
    analyzer_id: str
    timestamp: datetime
    co2_ppm: Optional[float] = None
    h2o_mmol: Optional[float] = None
    h2o_signal: Optional[float] = None
    co2_signal: Optional[float] = None
    co2_ratio_f: Optional[float] = None
    co2_ratio_raw: Optional[float] = None
    h2o_ratio_f: Optional[float] = None
    h2o_ratio_raw: Optional[float] = None
    ref_signal: Optional[float] = None
    temperature_c: Optional[float] = None
    pressure_hpa: Optional[float] = None
    pressure_gauge_hpa: Optional[float] = None
    pressure_reference_status: str = ""
    thermometer_temp_c: Optional[float] = None
    thermometer_reference_status: str = ""
    dew_point_c: Optional[float] = None
    analyzer_pressure_kpa: Optional[float] = None
    analyzer_chamber_temp_c: Optional[float] = None
    case_temp_c: Optional[float] = None
    frame_has_data: bool = True
    frame_usable: bool = True
    frame_status: str = ""
    point_phase: str = ""
    point_tag: str = ""
    sample_index: int = 0
    stability_time_s: Optional[float] = None
    total_time_s: Optional[float] = None


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
