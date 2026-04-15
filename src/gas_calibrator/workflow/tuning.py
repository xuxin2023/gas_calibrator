from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional, Tuple

from ..config import get as cfg_get


@dataclass(frozen=True)
class TunableParameterSpec:
    group: str
    path: str
    label: str
    unit: str
    default: Any
    min_value: Optional[float] = None
    max_value: Optional[float] = None


WORKFLOW_TUNABLE_PARAMETERS: Tuple[TunableParameterSpec, ...] = (
    TunableParameterSpec("pressure", "workflow.pressure.pressurize_wait_after_vent_off_s", "Vent-off settle wait", "s", 5.0, 0.0, 30.0),
    TunableParameterSpec("pressure", "workflow.pressure.co2_post_h2o_vent_off_wait_s", "CO2 post-H2O vent-off settle wait", "s", 5.0, 0.0, 30.0),
    TunableParameterSpec("pressure", "workflow.pressure.vent_time_s", "Vent time", "s", 5.0, 1.0, 30.0),
    TunableParameterSpec("pressure", "workflow.pressure.vent_transition_timeout_s", "Vent transition timeout", "s", 30.0, 5.0, 120.0),
    TunableParameterSpec("pressure", "workflow.pressure.stabilize_timeout_s", "Pressure stabilize timeout", "s", 120.0, 30.0, 600.0),
    TunableParameterSpec("pressure", "workflow.pressure.restabilize_retries", "Pressure retry count", "count", 2, 0.0, 5.0),
    TunableParameterSpec("pressure", "workflow.pressure.restabilize_retry_interval_s", "Pressure retry interval", "s", 10.0, 1.0, 60.0),
    TunableParameterSpec("pressure", "workflow.pressure.co2_reseal_retry_count", "CO2 reseal retry count", "count", 1, 0.0, 3.0),
    TunableParameterSpec("pressure", "workflow.pressure.post_stable_sample_delay_s", "Post-stable sample delay", "s", 10.0, 0.0, 60.0),
    TunableParameterSpec("pressure", "workflow.pressure.co2_post_stable_sample_delay_s", "CO2 post-stable sample delay", "s", 10.0, 0.0, 60.0),
    TunableParameterSpec("pressure", "workflow.pressure.superambient_precharge_enabled", "Superambient precharge enabled", "bool", True),
    TunableParameterSpec("pressure", "workflow.pressure.superambient_trigger_margin_hpa", "Superambient trigger margin", "hPa", 5.0, 0.0, 50.0),
    TunableParameterSpec("pressure", "workflow.pressure.superambient_precharge_margin_hpa", "Superambient precharge margin", "hPa", 8.0, 0.0, 50.0),
    TunableParameterSpec("pressure", "workflow.pressure.superambient_precharge_timeout_s", "Superambient precharge timeout", "s", 30.0, 1.0, 120.0),
    TunableParameterSpec("pressure", "workflow.pressure.superambient_precharge_same_gas_only", "Superambient same-gas only", "bool", True),
    TunableParameterSpec("pressure", "workflow.pressure.superambient_reject_without_closed_path", "Reject without closed precharge path", "bool", True),
    TunableParameterSpec("pressure", "workflow.pressure.superambient_forbid_atmosphere_fallback", "Forbid atmosphere fallback on superambient", "bool", True),
    TunableParameterSpec("pressure", "workflow.pressure.co2_post_isolation_diagnostic_enabled", "Low-pressure post-isolation diagnostic", "bool", True),
    TunableParameterSpec("pressure", "workflow.pressure.co2_post_isolation_window_s", "Post-isolation diagnostic window", "s", 10.0, 1.0, 60.0),
    TunableParameterSpec("pressure", "workflow.pressure.co2_post_isolation_poll_s", "Post-isolation diagnostic poll", "s", 0.5, 0.1, 5.0),
    TunableParameterSpec("pressure", "workflow.pressure.co2_post_isolation_pressure_drift_hpa", "Post-isolation pressure drift limit", "hPa", 0.35, 0.0, 5.0),
    TunableParameterSpec("pressure", "workflow.pressure.co2_post_isolation_pressure_stable_span_hpa", "Post-isolation stable pressure span", "hPa", 0.20, 0.0, 5.0),
    TunableParameterSpec("pressure", "workflow.pressure.co2_post_isolation_dewpoint_rise_c", "Post-isolation dewpoint rise limit", "C", 0.12, 0.0, 5.0),
    TunableParameterSpec("pressure", "workflow.pressure.co2_post_isolation_dewpoint_slope_c_per_s", "Post-isolation dewpoint slope limit", "C/s", 0.01, 0.0, 1.0),
    TunableParameterSpec("pressure", "workflow.pressure.co2_post_isolation_ambient_recovery_min_hpa", "Post-isolation ambient recovery min", "hPa", 0.20, 0.0, 5.0),
    TunableParameterSpec("pressure", "workflow.pressure.post_isolation_fast_capture_enabled", "Post-isolation fast capture", "bool", True),
    TunableParameterSpec("pressure", "workflow.pressure.post_isolation_fast_capture_allow_early_sample", "Allow early sample after fast capture", "bool", False),
    TunableParameterSpec("pressure", "workflow.pressure.post_isolation_fast_capture_min_s", "Post-isolation fast capture min window", "s", 5.0, 1.0, 20.0),
    TunableParameterSpec("pressure", "workflow.pressure.post_isolation_fast_capture_require_vent_zero", "Fast capture requires vent zero", "bool", True),
    TunableParameterSpec("pressure", "workflow.pressure.post_isolation_fast_capture_require_isol_closed", "Fast capture requires isolation closed", "bool", True),
    TunableParameterSpec("pressure", "workflow.pressure.post_isolation_fast_capture_fallback_to_extended_diag", "Fallback to extended diagnostic after fast capture fail", "bool", True),
    TunableParameterSpec("pressure", "workflow.pressure.post_isolation_extended_diag_window_s", "Post-isolation extended diagnostic window", "s", 20.0, 5.0, 60.0),
    TunableParameterSpec("pressure", "workflow.pressure.fast_capture_pressure_drift_max_hpa", "Fast capture pressure drift max", "hPa", 0.18, 0.0, 5.0),
    TunableParameterSpec("pressure", "workflow.pressure.fast_capture_pressure_slope_max_hpa_s", "Fast capture pressure slope max", "hPa/s", 0.05, 0.0, 1.0),
    TunableParameterSpec("pressure", "workflow.pressure.fast_capture_dewpoint_rise_max_c", "Fast capture dewpoint rise max", "C", 0.06, 0.0, 5.0),
    TunableParameterSpec("pressure", "workflow.pressure.postseal_same_gas_dead_volume_purge_enabled", "Post-seal same-gas dead-volume purge", "bool", False),
    TunableParameterSpec("pressure", "workflow.pressure.continuous_atmosphere_hold", "Continuous atmosphere hold", "bool", True),
    TunableParameterSpec("pressure", "workflow.pressure.vent_hold_interval_s", "Atmosphere hold refresh interval", "s", 2.0, 0.1, 10.0),
    TunableParameterSpec("temperature", "workflow.stability.temperature.tol", "Temperature tolerance", "C", 0.2, 0.05, 1.0),
    TunableParameterSpec("temperature", "workflow.stability.temperature.timeout_s", "Temperature timeout", "s", 1800.0, 60.0, 7200.0),
    TunableParameterSpec("temperature", "workflow.stability.temperature.soak_after_reach_s", "Temperature soak after reach", "s", 1800.0, 0.0, 7200.0),
    TunableParameterSpec("temperature", "workflow.stability.temperature.command_offset_c", "Temperature command offset", "C", 0.0, -5.0, 5.0),
    TunableParameterSpec("temperature", "workflow.stability.temperature.wait_for_target_before_continue", "Wait for chamber target before continue", "bool", True),
    TunableParameterSpec("temperature", "workflow.stability.temperature.reuse_running_in_tol_without_soak", "Reuse running in-tol chamber", "bool", True),
    TunableParameterSpec("temperature", "workflow.stability.temperature.restart_on_target_change", "Restart chamber on target change", "bool", False),
    TunableParameterSpec("temperature", "workflow.stability.temperature.precondition_next_group_enabled", "Precondition next-group chamber target", "bool", False),
    TunableParameterSpec("temperature", "workflow.stability.temperature.transition_check_window_s", "Temperature transition movement check window", "s", 120.0, 0.0, 600.0),
    TunableParameterSpec("temperature", "workflow.stability.temperature.transition_min_delta_c", "Temperature transition minimum movement", "C", 0.3, 0.0, 5.0),
    TunableParameterSpec("temperature", "workflow.stability.temperature.analyzer_chamber_temp_enabled", "Analyzer chamber-temp stability enabled", "bool", True),
    TunableParameterSpec("temperature", "workflow.stability.temperature.analyzer_chamber_temp_window_s", "Analyzer chamber-temp stable window", "s", 60.0, 5.0, 600.0),
    TunableParameterSpec("temperature", "workflow.stability.temperature.analyzer_chamber_temp_span_c", "Analyzer chamber-temp span tolerance", "C", 0.03, 0.001, 1.0),
    TunableParameterSpec("temperature", "workflow.stability.temperature.analyzer_chamber_temp_timeout_s", "Analyzer chamber-temp timeout", "s", 3600.0, 60.0, 14400.0),
    TunableParameterSpec("temperature", "workflow.stability.temperature.analyzer_chamber_temp_first_valid_timeout_s", "Analyzer chamber-temp first-valid timeout", "s", 120.0, 5.0, 3600.0),
    TunableParameterSpec("temperature", "workflow.stability.temperature.analyzer_chamber_temp_poll_s", "Analyzer chamber-temp poll interval", "s", 1.0, 0.1, 10.0),
    TunableParameterSpec("humidity_generator", "workflow.stability.humidity_generator.temp_tol_c", "Humidity-generator temperature tolerance", "C", 1.0, 0.05, 1.5),
    TunableParameterSpec("humidity_generator", "workflow.stability.humidity_generator.rh_tol_pct", "Humidity-generator RH tolerance", "%RH", 4.5, 0.2, 10.0),
    TunableParameterSpec("humidity_generator", "workflow.stability.humidity_generator.rh_stable_window_s", "Humidity-generator RH stable window", "s", 60.0, 10.0, 600.0),
    TunableParameterSpec("humidity_generator", "workflow.stability.humidity_generator.rh_stable_span_pct", "Humidity-generator RH stable span", "%RH", 0.3, 0.1, 2.0),
    TunableParameterSpec("humidity_generator", "workflow.stability.humidity_generator.precondition_next_group_enabled", "Precondition next-group humidity generator", "bool", True),
    TunableParameterSpec("route", "workflow.stability.h2o_route.preseal_soak_s", "H2O preseal soak", "s", 300.0, 0.0, 600.0),
    TunableParameterSpec("route", "workflow.stability.co2_route.preseal_soak_s", "CO2 preseal soak", "s", 180.0, 0.0, 600.0),
    TunableParameterSpec("route", "workflow.stability.co2_route.first_point_preseal_soak_s", "First CO2-point preseal soak", "s", 300.0, 0.0, 1200.0),
    TunableParameterSpec("route", "workflow.stability.co2_route.post_h2o_zero_ppm_soak_s", "CO2 post-H2O zero-gas flush", "s", 900.0, 0.0, 1800.0),
    TunableParameterSpec("sensor", "workflow.stability.sensor.co2_ratio_f_preseal_tol", "CO2 preseal tolerance", "ratio", 0.01, 0.0001, 0.1),
    TunableParameterSpec("sensor", "workflow.stability.sensor.co2_ratio_f_preseal_window_s", "CO2 preseal stable window", "s", 60.0, 5.0, 300.0),
    TunableParameterSpec("sensor", "workflow.stability.sensor.co2_ratio_f_preseal_min_samples", "CO2 preseal min samples", "count", 10, 3.0, 100.0),
    TunableParameterSpec("sensor", "workflow.stability.sensor.h2o_ratio_f_pressure_tol", "H2O pressure-stage tolerance", "ratio", 0.001, 0.0001, 0.1),
    TunableParameterSpec("sensor", "workflow.stability.sensor.h2o_ratio_f_pressure_window_s", "H2O pressure-stage stable window", "s", 15.0, 5.0, 300.0),
    TunableParameterSpec("dewpoint", "workflow.stability.dewpoint.window_s", "Dewpoint stable window", "s", 40.0, 10.0, 300.0),
    TunableParameterSpec("dewpoint", "workflow.stability.dewpoint.timeout_s", "Dewpoint timeout", "s", 1800.0, 0.0, 3600.0),
    TunableParameterSpec("dewpoint", "workflow.stability.dewpoint.poll_s", "Dewpoint poll interval", "s", 1.0, 0.1, 10.0),
    TunableParameterSpec("dewpoint", "workflow.stability.dewpoint.temp_match_tol_c", "Dewpoint temperature match tolerance", "C", 0.55, 0.05, 1.0),
    TunableParameterSpec("dewpoint", "workflow.stability.dewpoint.rh_match_tol_pct", "Dewpoint RH match tolerance", "%RH", 5.5, 0.5, 10.0),
    TunableParameterSpec("dewpoint", "workflow.stability.dewpoint.stability_tol_c", "Dewpoint stability tolerance", "C", 0.06, 0.001, 0.5),
    TunableParameterSpec("sampling", "workflow.sampling.count", "Sample count", "count", 10, 1.0, 200.0),
    TunableParameterSpec("sampling", "workflow.sampling.stable_count", "Stable sample count", "count", 10, 1.0, 200.0),
    TunableParameterSpec("sampling", "workflow.sampling.interval_s", "Sample interval", "s", 1.0, 0.1, 10.0),
    TunableParameterSpec("sampling", "workflow.sampling.h2o_interval_s", "H2O sample interval", "s", 1.0, 0.1, 10.0),
    TunableParameterSpec("sampling", "workflow.sampling.co2_interval_s", "CO2 sample interval", "s", 1.0, 0.1, 10.0),
    TunableParameterSpec("retry", "workflow.sensor_read_retry.retries", "Sensor read retries", "count", 1, 0.0, 10.0),
    TunableParameterSpec("retry", "workflow.sensor_read_retry.delay_s", "Sensor read retry delay", "s", 0.05, 0.01, 2.0),
    TunableParameterSpec("retry", "workflow.analyzer_reprobe.cooldown_s", "Analyzer re-probe cooldown", "s", 300.0, 0.0, 3600.0),
)


def workflow_param(cfg: dict[str, Any], path: str, default: Any = None) -> Any:
    return cfg_get(cfg, path, default)


def get_workflow_tunable_parameters() -> Tuple[TunableParameterSpec, ...]:
    return WORKFLOW_TUNABLE_PARAMETERS
