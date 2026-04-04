from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional

from .models import CalibrationPoint, SamplingResult


@dataclass
class ArtifactRuntimeState:
    output_files: list[str] = field(default_factory=list)
    export_statuses: dict[str, dict[str, Any]] = field(default_factory=dict)


@dataclass
class QCRuntimeState:
    cleaned_point_samples: dict[int, list[SamplingResult]] = field(default_factory=dict)
    point_qc_inputs: list[tuple[CalibrationPoint, list[SamplingResult]]] = field(default_factory=list)
    point_validations: list[Any] = field(default_factory=list)
    run_quality_score: Any = None
    qc_report: Any = None


@dataclass
class AnalyzerRuntimeState:
    disabled: set[str] = field(default_factory=set)
    disabled_reasons: dict[str, str] = field(default_factory=dict)
    disabled_last_reprobe_ts: dict[str, float] = field(default_factory=dict)
    last_live_snapshot_ts: float = 0.0


@dataclass
class HumidityRuntimeState:
    preseal_dewpoint_snapshot: Optional[dict[str, Any]] = None
    h2o_pressure_prepared_target: Optional[float] = None
    post_h2o_co2_zero_flush_pending: bool = False
    initial_co2_zero_flush_pending: bool = False
    first_co2_route_soak_pending: bool = True
    active_post_h2o_co2_zero_flush: bool = False
    last_hgen_target: tuple[Optional[float], Optional[float]] = field(default_factory=lambda: (None, None))
    last_hgen_setpoint_ready: bool = False


@dataclass
class TemperatureRuntimeState:
    snapshot_keys: set[tuple[float, str]] = field(default_factory=set)
    snapshots: list[dict[str, Any]] = field(default_factory=list)
    ready_target_c: Optional[float] = None
    last_target_c: Optional[float] = None
    last_soak_done: bool = False
    last_wait_result: Any = None


@dataclass
class TimingRuntimeState:
    point_contexts: dict[str, dict[str, Any]] = field(default_factory=dict)


@dataclass
class RunState:
    artifacts: ArtifactRuntimeState = field(default_factory=ArtifactRuntimeState)
    qc: QCRuntimeState = field(default_factory=QCRuntimeState)
    analyzers: AnalyzerRuntimeState = field(default_factory=AnalyzerRuntimeState)
    humidity: HumidityRuntimeState = field(default_factory=HumidityRuntimeState)
    temperature: TemperatureRuntimeState = field(default_factory=TemperatureRuntimeState)
    timing: TimingRuntimeState = field(default_factory=TimingRuntimeState)

    def reset(self, *, initial_co2_zero_flush_pending: bool) -> None:
        self.artifacts.output_files.clear()
        self.artifacts.export_statuses.clear()

        self.qc.cleaned_point_samples.clear()
        self.qc.point_qc_inputs.clear()
        self.qc.point_validations.clear()
        self.qc.run_quality_score = None
        self.qc.qc_report = None

        self.analyzers.disabled.clear()
        self.analyzers.disabled_reasons.clear()
        self.analyzers.disabled_last_reprobe_ts.clear()
        self.analyzers.last_live_snapshot_ts = 0.0

        self.humidity.preseal_dewpoint_snapshot = None
        self.humidity.h2o_pressure_prepared_target = None
        self.humidity.post_h2o_co2_zero_flush_pending = False
        self.humidity.initial_co2_zero_flush_pending = bool(initial_co2_zero_flush_pending)
        self.humidity.first_co2_route_soak_pending = True
        self.humidity.active_post_h2o_co2_zero_flush = False
        self.humidity.last_hgen_target = (None, None)
        self.humidity.last_hgen_setpoint_ready = False

        self.temperature.snapshot_keys.clear()
        self.temperature.snapshots.clear()
        self.temperature.ready_target_c = None
        self.temperature.last_target_c = None
        self.temperature.last_soak_done = False
        self.temperature.last_wait_result = None

        self.timing.point_contexts.clear()
