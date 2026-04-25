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
class PressureRuntimeState:
    sealed_route: str = ""
    sealed_source_point_index: Optional[int] = None
    final_vent_off_command_sent: bool = False
    sealed_pressure_hpa: Optional[float] = None
    preseal_pressure_peak_hpa: Optional[float] = None
    preseal_pressure_last_hpa: Optional[float] = None
    preseal_trigger: str = ""
    preseal_trigger_pressure_hpa: Optional[float] = None
    preseal_trigger_threshold_hpa: Optional[float] = None
    preseal_final_atmosphere_exit_required: bool = False
    preseal_final_atmosphere_exit_started: bool = False
    preseal_final_atmosphere_exit_verified: bool = False
    preseal_final_atmosphere_exit_phase: str = ""
    preseal_final_atmosphere_exit_reason: str = ""
    preseal_watchlist_status_seen: bool = False
    preseal_watchlist_status_accepted: bool = False
    preseal_watchlist_status_reason: str = ""
    seal_transition_completed: bool = False
    seal_transition_status: str = ""
    seal_transition_reason: str = ""
    control_ready_watchlist_status_accepted: bool = False
    sealed_route_pressure_control_started: bool = False
    sealed_route_last_controlled_pressure_hpa: Optional[float] = None


@dataclass
class TemperatureRuntimeState:
    snapshot_keys: set[tuple[float, str]] = field(default_factory=set)
    snapshots: list[dict[str, Any]] = field(default_factory=list)
    analyzer_chamber_temp_stability_evidence: dict[str, Any] = field(default_factory=dict)
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
    pressure: PressureRuntimeState = field(default_factory=PressureRuntimeState)
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

        self.pressure.sealed_route = ""
        self.pressure.sealed_source_point_index = None
        self.pressure.final_vent_off_command_sent = False
        self.pressure.sealed_pressure_hpa = None
        self.pressure.preseal_pressure_peak_hpa = None
        self.pressure.preseal_pressure_last_hpa = None
        self.pressure.preseal_trigger = ""
        self.pressure.preseal_trigger_pressure_hpa = None
        self.pressure.preseal_trigger_threshold_hpa = None
        self.pressure.preseal_final_atmosphere_exit_required = False
        self.pressure.preseal_final_atmosphere_exit_started = False
        self.pressure.preseal_final_atmosphere_exit_verified = False
        self.pressure.preseal_final_atmosphere_exit_phase = ""
        self.pressure.preseal_final_atmosphere_exit_reason = ""
        self.pressure.preseal_watchlist_status_seen = False
        self.pressure.preseal_watchlist_status_accepted = False
        self.pressure.preseal_watchlist_status_reason = ""
        self.pressure.seal_transition_completed = False
        self.pressure.seal_transition_status = ""
        self.pressure.seal_transition_reason = ""
        self.pressure.control_ready_watchlist_status_accepted = False
        self.pressure.sealed_route_pressure_control_started = False
        self.pressure.sealed_route_last_controlled_pressure_hpa = None

        self.temperature.snapshot_keys.clear()
        self.temperature.snapshots.clear()
        self.temperature.analyzer_chamber_temp_stability_evidence.clear()
        self.temperature.ready_target_c = None
        self.temperature.last_target_c = None
        self.temperature.last_soak_done = False
        self.temperature.last_wait_result = None

        self.timing.point_contexts.clear()
