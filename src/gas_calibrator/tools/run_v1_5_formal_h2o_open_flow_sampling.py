"""Run V1.5 formal open-flow no-write H2O sampling.

This sidecar mirrors the CO2 open-flow sidecar but uses only the water route.
It keeps PACE open to atmosphere only while the H2O route is flowing, waits for
the humidity/dewpoint gates, samples, then closes the route and safe-stops the
humidity generator. It never enters sealed pressure control and never writes
analyzer coefficients or IDs. The default analyzer path may issue MODE2/active
upload setup once, then samples by reading the stream instead of repeatedly
polling READDATA; FTD writes require an explicit 1Hz trial flag.
"""

from __future__ import annotations

import argparse
import csv
import copy
import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path
from statistics import mean
from typing import Any, Dict, Iterable, List, Optional

from ..config import load_config
from ..data.points import CalibrationPoint
from ..logging_utils import RunLogger
from ..validation.common import analyze_sample_rows
from ..validation.reporting import ValidationMetadata, write_validation_report
from ..workflow.runner import CalibrationRunner
from .run_headless import _build_devices, _close_devices
from .run_v1_5_formal_open_flow_sampling import (
    _apply_analyzer_acquisition_policy,
    _configured_analyzer_labels,
    _defer_startup_mode2_disabled_analyzers,
    _enable_formal_summary_outlier_filter,
    _enter_continuous_atmosphere,
    _read_dewpoint_snapshot,
    _read_optional_float,
    _stop_continuous_atmosphere,
    _write_machine_readable_samples,
    _write_purge_trace,
)


DEFAULT_H2O_OPEN_FLOW_PURGE_S = 720.0


def _log(message: str) -> None:
    print(message, flush=True)


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run V1.5 no-write open-flow H2O sampling without sealed pressure control."
    )
    parser.add_argument("--config", required=True, help="Runtime config JSON.")
    parser.add_argument("--run-id", default=None, help="Optional fixed run folder name.")
    parser.add_argument("--output-dir", default=None, help="Optional output directory override.")
    parser.add_argument("--temp", type=float, default=20.0, help="Temperature chamber setpoint metadata.")
    parser.add_argument("--hgen-temp", type=float, required=True, help="Humidity generator target temperature.")
    parser.add_argument("--hgen-rh", type=float, required=True, help="Humidity generator target RH percent.")
    parser.add_argument("--certificate-h2o-mmol", type=float, default=None, help="Certificate H2O mmol/mol value.")
    parser.add_argument("--certificate-dewpoint-c", type=float, default=None, help="Certificate/reference dewpoint C.")
    parser.add_argument("--certificate-uncertainty-mmol", type=float, default=None)
    parser.add_argument("--purge-s", type=float, default=DEFAULT_H2O_OPEN_FLOW_PURGE_S)
    parser.add_argument(
        "--minimum-purge-s",
        type=float,
        default=DEFAULT_H2O_OPEN_FLOW_PURGE_S,
        help=(
            "Formal minimum purge evidence for this H2O point. The route may purge longer, "
            "but candidate-fit readiness must not treat shorter evidence as A grade."
        ),
    )
    parser.add_argument("--purge-trace-interval-s", type=float, default=10.0)
    parser.add_argument("--max-open-flow-pressure-hpa", type=float, default=1100.0)
    parser.add_argument(
        "--open-flow-pressure-transient-grace-s",
        type=float,
        default=30.0,
        help="Allowed duration for startup/open-valve pressure spikes above --max-open-flow-pressure-hpa.",
    )
    parser.add_argument(
        "--open-flow-pressure-safety-hard-limit-hpa",
        type=float,
        default=1300.0,
        help="Immediate safety abort pressure during open-flow purge.",
    )
    parser.add_argument(
        "--hgen-flow-lpm",
        type=float,
        default=None,
        help=(
            "Optional humidity-generator flow target applied after the H2O route is opened. "
            "Omit to leave flow under the generator's internal control."
        ),
    )
    parser.add_argument(
        "--hgen-flow-readback-timeout-s",
        type=float,
        default=60.0,
        help="Maximum readback wait when an optional explicit flow target is requested.",
    )
    parser.add_argument(
        "--hgen-flow-readback-poll-s",
        type=float,
        default=1.0,
        help="Polling interval while waiting for humidity-generator flow readback.",
    )
    parser.add_argument(
        "--hgen-flow-readback-tolerance-lpm",
        type=float,
        default=0.5,
        help="Allowed absolute flow readback error recorded as flow evidence only.",
    )
    parser.add_argument("--sample-count", type=int, default=10)
    parser.add_argument("--sample-interval-s", type=float, default=1.0)
    parser.add_argument("--sensor-read-interval-s", type=float, default=5.0)
    parser.add_argument(
        "--pressure-diagnostic-only",
        action="store_true",
        help=(
            "After the H2O route and stability gates are ready, collect analyzer internal "
            "pressure evidence and stop without formal H2O sampling."
        ),
    )
    parser.add_argument(
        "--pressure-diagnostic-after-purge",
        action="store_true",
        help=(
            "For engineering diagnosis, collect the analyzer pressure diagnostic immediately "
            "after open-flow purge instead of waiting for formal dewpoint/H2O stability gates."
        ),
    )
    parser.add_argument(
        "--pressure-diagnostic-observe-hgen-only",
        action="store_true",
        help=(
            "Engineering diagnostic only: with --pressure-diagnostic-only and "
            "--pressure-diagnostic-after-purge, do not send humidity-generator target, "
            "control, flow, or safe-stop commands. The generator is only read as evidence."
        ),
    )
    parser.add_argument(
        "--pressure-diagnostic-route-closed-baseline",
        action="store_true",
        help=(
            "Engineering diagnostic only: keep all route valves closed, do not prepare/open "
            "the H2O route, and collect analyzer internal pressure baseline evidence."
        ),
    )
    parser.add_argument(
        "--route-closed-baseline-settle-s",
        type=float,
        default=10.0,
        help="Optional wait after confirming route valves are closed before collecting baseline pressure evidence.",
    )
    parser.add_argument("--pressure-diagnostic-s", type=float, default=180.0)
    parser.add_argument("--pressure-diagnostic-interval-s", type=float, default=1.0)
    parser.add_argument(
        "--pressure-diagnostic-reference-interval-s",
        type=float,
        default=5.0,
        help=(
            "Slow-reference polling interval for COM22/PACE/dewpoint during the H2O "
            "pressure diagnostic. Analyzer active-stream frames are still read every "
            "--pressure-diagnostic-interval-s."
        ),
    )
    parser.add_argument(
        "--pressure-diagnostic-analyzer-drain-s",
        type=float,
        default=0.18,
        help=(
            "Per-analyzer active-stream drain time used by the H2O pressure diagnostic. "
            "This reads uploaded frames and does not send READDATA commands."
        ),
    )
    parser.add_argument("--pressure-diagnostic-window-s", type=float, default=60.0)
    parser.add_argument("--pressure-diagnostic-span-hpa", type=float, default=2.0)
    parser.add_argument("--pressure-diagnostic-min-samples", type=int, default=10)
    parser.add_argument(
        "--pressure-diagnostic-fail-on-unstable",
        action="store_true",
        help="Return exit code 1 when the diagnostic status is fail/unstable.",
    )
    parser.add_argument(
        "--pace-vent-after-valve-diagnostic",
        action="store_true",
        help=(
            "Engineering diagnostic only: use PACE vent-after-valve atmosphere release "
            "during the H2O serial pressure diagnostic. Requires --pressure-diagnostic-only "
            "and is closed again during cleanup."
        ),
    )
    parser.add_argument(
        "--min-valid-analyzers",
        type=int,
        default=1,
        help=(
            "Minimum analyzers that must show a stable H2O ratio before formal sampling. "
            "Other analyzers remain in the evidence set and are classified by Frame QC."
        ),
    )
    parser.add_argument(
        "--analyzer-acquisition",
        choices=("active_stream_10hz", "active_stream_1hz", "passive_query"),
        default="active_stream_1hz",
        help=(
            "Gas-analyzer acquisition policy. The V1.5 formal default is "
            "active_stream_1hz, which sends FTD=01 and records one uploaded frame "
            "per formal sample anchor; active_stream_10hz reads the native 10 Hz "
            "stream without FTD; passive_query keeps the older READDATA fallback."
        ),
    )
    ftd_group = parser.add_mutually_exclusive_group()
    ftd_group.add_argument(
        "--allow-ftd-write",
        dest="allow_ftd_write",
        action="store_true",
        default=True,
        help="Allow the V1.5 formal 1 Hz active-upload setup command FTD=01. Never writes SENCO or ID.",
    )
    ftd_group.add_argument(
        "--no-ftd-write",
        dest="allow_ftd_write",
        action="store_false",
        help="Do not send FTD even if active_stream_1hz is selected; records expected 1 Hz only.",
    )
    parser.add_argument(
        "--strict-humidity-reference-match",
        action="store_true",
        help=(
            "Opt in to the older hard match between dewpoint-meter RH and humidity-generator RH. "
            "By default the dewpoint meter is treated as the H2O reference and generator RH mismatch "
            "is recorded as review evidence unless it is a severe saturation contradiction."
        ),
    )
    parser.add_argument("--skip-humidity-generator-gate", action="store_true")
    parser.add_argument("--skip-dewpoint-gate", action="store_true")
    parser.add_argument(
        "--h2o-pressure-presample-policy",
        choices=("warn", "fail", "skip"),
        default="skip",
        help=(
            "Policy when the analyzer internal pressure pre-sample gate is unstable. "
            "Formal open-flow H2O sampling defaults to skip because pressure movement "
            "under flow is diagnostic evidence, not a pre-sample stability gate. "
            "warn can be used for engineering review; fail blocks the sample window."
        ),
    )
    parser.add_argument(
        "--keep-hgen-running-after-point",
        action="store_true",
        help=(
            "Queue-managed H2O operation: close the water route after this point, but do not "
            "safe-stop the humidity generator. The queue must perform a final safe stop."
        ),
    )
    parser.add_argument("--no-prompt", action="store_true")
    return parser.parse_args(list(argv) if argv is not None else None)


def _prepare_runtime_cfg(
    cfg: Dict[str, Any],
    *,
    output_dir: Optional[str],
    sample_count: int,
    sample_interval_s: float,
    sensor_read_interval_s: float,
    hgen_flow_lpm: Optional[float] = None,
    analyzer_acquisition: str = "active_stream_1hz",
    allow_ftd_write: bool = True,
    min_valid_analyzers: int = 1,
    h2o_pressure_presample_policy: str = "skip",
    keep_hgen_running_after_point: bool = False,
) -> Dict[str, Any]:
    runtime_cfg = copy.deepcopy(cfg)
    workflow_cfg = runtime_cfg.setdefault("workflow", {})
    workflow_cfg["collect_only"] = True
    workflow_cfg["skip_h2o"] = False
    workflow_cfg["route_mode"] = "h2o_open_flow_sidecar"

    metadata = runtime_cfg.setdefault("metadata", {})
    metadata["formal_h2o_open_flow_sidecar"] = True
    metadata["writes_senco"] = False
    metadata["writes_device_id"] = False
    metadata["sealed_pressure_points_enter_formal_fit"] = False
    metadata["ftd_write_enabled"] = False
    metadata["idle_continuous_atmosphere_hold"] = False
    metadata["continuous_atmosphere_hold_scope"] = "h2o_open_flow_purge_and_sampling_only"
    metadata["startup_mode2_missing_policy"] = "defer_to_sampling_qc"
    metadata["h2o_open_flow_hgen_flow_control"] = (
        "optional_explicit_target" if hgen_flow_lpm is not None else "not_controlled_by_default"
    )
    metadata["h2o_open_flow_hgen_flow_lpm"] = None if hgen_flow_lpm is None else float(hgen_flow_lpm)
    metadata["h2o_hgen_shutdown_policy"] = (
        "queue_managed_keep_running_between_points"
        if keep_hgen_running_after_point
        else "safe_stop_after_point"
    )
    metadata["h2o_open_flow_sampling_physical_contract"] = {
        "sample_window_requires_route_open": True,
        "sample_window_requires_humidity_reference_flow": True,
        "route_close_allowed_only_after_sample_window": True,
        "dewpoint_reference_gate_required": True,
        "per_analyzer_h2o_ratio_stability_required": True,
        "per_analyzer_status_register_qc_required": True,
        "unstable_analyzer_handling": "independent_grade_or_reject_do_not_block_all_when_min_valid_met",
        "pressure_role": "diagnostic_or_qc_input_not_h2o_fit_hard_blocker",
    }

    devices_cfg = runtime_cfg.setdefault("devices", {})
    if isinstance(devices_cfg.get("humidity_generator"), dict):
        devices_cfg["humidity_generator"]["enabled"] = True

    pressure_cfg = workflow_cfg.setdefault("pressure", {})
    pressure_cfg["continuous_atmosphere_hold"] = False

    init_cfg = workflow_cfg.setdefault("analyzer_mode2_init", {})
    init_cfg["read_first_before_config"] = True
    init_cfg["sniff_stream_before_config"] = True
    init_cfg["write_config_on_read_first_fail"] = False
    init_cfg["send_active_freq"] = False

    sampling_cfg = workflow_cfg.setdefault("sampling", {})
    sampling_cfg["count"] = int(sample_count)
    sampling_cfg["stable_count"] = int(sample_count)
    sampling_cfg["interval_s"] = float(sample_interval_s)
    sampling_cfg["h2o_interval_s"] = float(sample_interval_s)
    _enable_formal_summary_outlier_filter(runtime_cfg)
    _apply_analyzer_acquisition_policy(
        runtime_cfg,
        analyzer_acquisition=analyzer_acquisition,
        sensor_read_interval_s=sensor_read_interval_s,
        sample_interval_s=sample_interval_s,
        allow_ftd_write=allow_ftd_write,
    )

    stability_cfg = workflow_cfg.setdefault("stability", {})
    sensor_cfg = stability_cfg.setdefault("sensor", {})
    sensor_cfg["read_interval_s"] = max(0.2, float(sensor_read_interval_s))
    temp_cfg = stability_cfg.setdefault("temperature", {})
    current_span = float(temp_cfg.get("analyzer_chamber_temp_span_c", 0.08) or 0.08)
    temp_cfg["analyzer_chamber_temp_span_c"] = max(current_span, 0.08)
    current_window = float(temp_cfg.get("analyzer_chamber_temp_window_s", 60.0) or 60.0)
    temp_cfg["analyzer_chamber_temp_window_s"] = max(current_window, 60.0)
    dewpoint_cfg = stability_cfg.setdefault("dewpoint", {})
    dewpoint_cfg["enabled"] = True
    dewpoint_cfg["window_s"] = max(float(dewpoint_cfg.get("window_s", 60.0) or 60.0), 60.0)
    dewpoint_cfg["timeout_s"] = max(float(dewpoint_cfg.get("timeout_s", 1800.0) or 1800.0), 1800.0)
    dewpoint_cfg["poll_s"] = max(float(dewpoint_cfg.get("poll_s", 1.0) or 1.0), 1.0)
    dewpoint_cfg["stability_tol_c"] = max(float(dewpoint_cfg.get("stability_tol_c", 0.05) or 0.05), 0.05)
    stability_cfg["water_route_dewpoint_gate_enabled"] = True
    stability_cfg["water_route_dewpoint_gate_policy"] = str(
        stability_cfg.get("water_route_dewpoint_gate_policy") or "warn"
    )
    stability_cfg["water_route_dewpoint_gate_window_s"] = max(
        float(stability_cfg.get("water_route_dewpoint_gate_window_s", 60.0) or 60.0),
        60.0,
    )
    stability_cfg["water_route_dewpoint_gate_max_total_wait_s"] = max(
        float(stability_cfg.get("water_route_dewpoint_gate_max_total_wait_s", 1080.0) or 1080.0),
        1080.0,
    )
    stability_cfg["water_route_dewpoint_gate_poll_s"] = max(
        float(stability_cfg.get("water_route_dewpoint_gate_poll_s", 2.0) or 2.0),
        2.0,
    )
    stability_cfg["water_route_dewpoint_gate_tail_span_max_c"] = max(
        float(stability_cfg.get("water_route_dewpoint_gate_tail_span_max_c", 0.45) or 0.45),
        0.45,
    )
    stability_cfg["water_route_dewpoint_gate_tail_slope_abs_max_c_per_s"] = max(
        float(stability_cfg.get("water_route_dewpoint_gate_tail_slope_abs_max_c_per_s", 0.005) or 0.005),
        0.005,
    )
    stability_cfg["water_route_dewpoint_gate_rebound_window_s"] = max(
        float(stability_cfg.get("water_route_dewpoint_gate_rebound_window_s", 180.0) or 180.0),
        180.0,
    )
    stability_cfg["water_route_dewpoint_gate_rebound_min_rise_c"] = max(
        float(stability_cfg.get("water_route_dewpoint_gate_rebound_min_rise_c", 1.3) or 1.3),
        1.3,
    )
    stability_cfg["water_route_dewpoint_gate_log_interval_s"] = max(
        float(stability_cfg.get("water_route_dewpoint_gate_log_interval_s", 15.0) or 15.0),
        15.0,
    )
    sensor_cfg["h2o_ratio_f_preseal_policy"] = str(sensor_cfg.get("h2o_ratio_f_preseal_policy") or "warn")
    h2o_base_tol = float(sensor_cfg.get("h2o_ratio_f_tol", 0.001) or 0.001)
    h2o_current_tol = float(sensor_cfg.get("h2o_ratio_f_preseal_tol", h2o_base_tol) or h2o_base_tol)
    sensor_cfg["h2o_ratio_f_preseal_tol"] = min(h2o_current_tol, h2o_base_tol)
    sensor_cfg["h2o_ratio_f_preseal_window_s"] = max(
        float(sensor_cfg.get("h2o_ratio_f_preseal_window_s", sensor_cfg.get("window_s", 60.0)) or 60.0),
        60.0,
    )
    sensor_cfg["h2o_ratio_f_preseal_timeout_s"] = max(
        float(sensor_cfg.get("h2o_ratio_f_preseal_timeout_s", sensor_cfg.get("timeout_s", 300.0)) or 300.0),
        300.0,
    )
    sensor_cfg["h2o_ratio_f_preseal_min_samples"] = max(
        int(sensor_cfg.get("h2o_ratio_f_preseal_min_samples", 10) or 10),
        10,
    )
    sensor_cfg["h2o_ratio_f_preseal_read_interval_s"] = max(
        float(sensor_cfg.get("h2o_ratio_f_preseal_read_interval_s", sensor_cfg.get("read_interval_s", 1.0)) or 1.0),
        1.0,
    )
    policy = str(h2o_pressure_presample_policy or "skip").strip().lower()
    if policy not in {"warn", "fail", "skip"}:
        policy = "skip"
    sensor_cfg["h2o_pressure_kpa_presample_enabled"] = (
        bool(sensor_cfg.get("h2o_pressure_kpa_presample_enabled", True)) and policy != "skip"
    )
    sensor_cfg["h2o_pressure_kpa_presample_tol"] = min(
        float(sensor_cfg.get("h2o_pressure_kpa_presample_tol", 0.2) or 0.2),
        0.2,
    )
    sensor_cfg["h2o_pressure_kpa_presample_window_s"] = max(
        float(sensor_cfg.get("h2o_pressure_kpa_presample_window_s", 60.0) or 60.0),
        60.0,
    )
    sensor_cfg["h2o_pressure_kpa_presample_timeout_s"] = max(
        float(sensor_cfg.get("h2o_pressure_kpa_presample_timeout_s", 300.0) or 300.0),
        300.0,
    )
    sensor_cfg["h2o_pressure_kpa_presample_min_samples"] = max(
        int(sensor_cfg.get("h2o_pressure_kpa_presample_min_samples", 10) or 10),
        10,
    )
    sensor_cfg["h2o_pressure_kpa_presample_read_interval_s"] = max(
        float(
            sensor_cfg.get(
                "h2o_pressure_kpa_presample_read_interval_s",
                sensor_cfg.get("h2o_ratio_f_preseal_read_interval_s", 1.0),
            )
            or 1.0
        ),
        1.0,
    )
    pressure_required_for_a_grade = bool(
        sensor_cfg.get("h2o_pressure_kpa_presample_required_for_a_grade", False)
    )
    if policy in {"warn", "skip"}:
        pressure_required_for_a_grade = False
    sensor_cfg["h2o_pressure_kpa_presample_policy"] = policy
    sensor_cfg["h2o_pressure_kpa_presample_required_for_a_grade"] = pressure_required_for_a_grade
    analyzer_labels = _configured_analyzer_labels(runtime_cfg)
    min_valid = max(1, int(min_valid_analyzers))
    if analyzer_labels:
        min_valid = min(min_valid, len(analyzer_labels))
    stability_cfg["analyzer_gate_min_valid_analyzers"] = min_valid
    stability_cfg["analyzer_gate_optional_labels"] = analyzer_labels
    stability_cfg["analyzer_gate_required_labels"] = []
    stability_cfg["analyzer_gate_allow_pass_with_dropped_optional"] = True
    stability_cfg["analyzer_gate_disable_dropped_optional"] = False
    stability_cfg["analyzer_gate_zero_value_policy"] = "drop_optional_not_block"
    stability_cfg["analyzer_gate_invalid_frame_min_count"] = 3
    stability_cfg["analyzer_gate_silent_timeout_s"] = max(15.0, float(sensor_read_interval_s) * 3.0)
    stability_cfg["analyzer_gate_max_wait_s"] = max(
        float(sensor_cfg.get("h2o_ratio_f_preseal_timeout_s", sensor_cfg.get("timeout_s", 300.0)) or 300.0),
        90.0,
    )
    metadata["h2o_open_flow_wait_contract"] = "v1_5_dewpoint_tail_h2o_ratio_with_pressure_diagnostic_only"
    metadata["h2o_pressure_kpa_presample_policy"] = policy
    metadata["h2o_pressure_kpa_presample_required_for_a_grade"] = pressure_required_for_a_grade
    metadata["humidity_reference_role"] = "dewpoint_meter_primary_hgen_state_review"

    postrun_cfg = workflow_cfg.setdefault("postrun_corrected_delivery", {})
    postrun_cfg["enabled"] = False
    postrun_cfg["write_devices"] = False
    postrun_cfg["write_pressure_coefficients"] = False
    startup_pressure_cfg = workflow_cfg.setdefault("startup_pressure_sensor_calibration", {})
    startup_pressure_cfg["enabled"] = False
    startup_pressure_cfg["apply_write"] = False

    if output_dir:
        runtime_cfg.setdefault("paths", {})["output_dir"] = str(Path(output_dir).resolve())
    return runtime_cfg


def _build_h2o_open_flow_point(
    *,
    temp_c: float,
    hgen_temp_c: float,
    hgen_rh_pct: float,
    certificate_dewpoint_c: Optional[float],
    certificate_h2o_mmol: Optional[float],
) -> CalibrationPoint:
    return CalibrationPoint(
        index=1,
        temp_chamber_c=float(temp_c),
        co2_ppm=None,
        hgen_temp_c=float(hgen_temp_c),
        hgen_rh_pct=float(hgen_rh_pct),
        target_pressure_hpa=None,
        dewpoint_c=certificate_dewpoint_c,
        h2o_mmol=certificate_h2o_mmol,
        raw_h2o=None,
        co2_group=None,
    )


def _safe_stop_humidity_generator(devices: Dict[str, Any]) -> None:
    hgen = devices.get("humidity_gen")
    if hgen is None:
        return
    safe_stop = getattr(hgen, "safe_stop", None)
    if callable(safe_stop):
        try:
            safe_stop()
        except Exception:
            pass


def _read_humidity_generator_snapshot(device: Any) -> Dict[str, Any]:
    if device is None:
        return {}
    reader = getattr(device, "fetch_all", None)
    if callable(reader):
        try:
            return dict(reader() or {})
        except Exception as exc:
            return {"error": str(exc)}
    return {}


def _pick_hgen_flow(snapshot: Dict[str, Any]) -> Optional[float]:
    data = snapshot.get("data", {}) if isinstance(snapshot, dict) else {}
    if not isinstance(data, dict):
        return None
    for key in ("Fl", "Flux"):
        try:
            value = data.get(key)
            if value is not None:
                return float(value)
        except Exception:
            continue
    return None


def _pick_hgen_value(snapshot: Dict[str, Any], *keys: str) -> Optional[float]:
    data = snapshot.get("data", {}) if isinstance(snapshot, dict) else {}
    if not isinstance(data, dict):
        return None
    for key in keys:
        try:
            value = data.get(key)
            if value is not None:
                return float(value)
        except Exception:
            continue
    return None


def _pick_snapshot_float(snapshot: Dict[str, Any], key: str) -> Optional[float]:
    if not isinstance(snapshot, dict):
        return None
    try:
        value = snapshot.get(key)
        if value is not None:
            return float(value)
    except Exception:
        return None
    return None


def _dewpoint_snapshot_has_measurement(snapshot: Dict[str, Any]) -> bool:
    return any(
        _pick_snapshot_float(snapshot, key) is not None
        for key in ("dewpoint_c", "temp_c", "rh_pct")
    )


def _read_dewpoint_snapshot_for_evidence(
    device: Any,
    *,
    attempts: int = 3,
    sleep_s: float = 0.2,
) -> Dict[str, Any]:
    total_attempts = max(1, int(attempts))
    last_snapshot: Dict[str, Any] = {}
    for attempt in range(total_attempts):
        snapshot = _read_dewpoint_snapshot(device)
        snapshot = dict(snapshot or {}) if isinstance(snapshot, dict) else {}
        snapshot["_evidence_read_attempt"] = attempt + 1
        snapshot["_evidence_read_attempts_max"] = total_attempts
        if _dewpoint_snapshot_has_measurement(snapshot):
            return snapshot
        last_snapshot = snapshot
        if attempt < total_attempts - 1:
            time.sleep(max(0.0, float(sleep_s)))
    return last_snapshot


def _build_humidity_reference_check_legacy_unused(
    dewpoint_snapshot: Dict[str, Any],
    humidity_generator_snapshot: Dict[str, Any],
) -> Dict[str, Any]:
    dew_rh = _pick_snapshot_float(dewpoint_snapshot, "rh_pct")
    hgen_rh = _pick_hgen_value(humidity_generator_snapshot, "Uw")
    dew_temp = _pick_snapshot_float(dewpoint_snapshot, "temp_c")
    hgen_temp = _pick_hgen_value(humidity_generator_snapshot, "Tc")
    rh_diff = None if dew_rh is None or hgen_rh is None else abs(float(dew_rh) - float(hgen_rh))
    temp_diff = None if dew_temp is None or hgen_temp is None else abs(float(dew_temp) - float(hgen_temp))
    reasons = []
    if dew_rh is not None and float(dew_rh) >= 99.0 and hgen_rh is not None and float(hgen_rh) < 95.0:
        reasons.append("dewpoint_meter_reports_saturation_but_hgen_is_not_saturated")
    if rh_diff is not None and rh_diff > 5.5:
        reasons.append("dewpoint_rh_not_consistent_with_humidity_generator")
    if temp_diff is not None and temp_diff > 0.55:
        reasons.append("dewpoint_temperature_not_consistent_with_humidity_generator")
    status = "fail" if reasons else "review"
    if not reasons and dew_rh is not None and hgen_rh is not None:
        status = "pass"
    return {
        "status": status,
        "reasons": reasons,
        "dewpoint_rh_pct": dew_rh,
        "hgen_rh_pct": hgen_rh,
        "rh_diff_pct": rh_diff,
        "dewpoint_temp_c": dew_temp,
        "hgen_temp_c": hgen_temp,
        "temp_diff_c": temp_diff,
        "human_summary": (
            "露点仪显示接近/达到饱和，但湿度发生器未处于饱和湿度；水汽参考不一致，"
            "本次水路不能进入正式采样。"
            if reasons
            else "水汽参考一致性需要结合完整门禁日志复核。"
        ),
    }


def _build_humidity_reference_check(
    dewpoint_snapshot: Dict[str, Any],
    humidity_generator_snapshot: Dict[str, Any],
) -> Dict[str, Any]:
    dew_rh = _pick_snapshot_float(dewpoint_snapshot, "rh_pct")
    hgen_rh = _pick_hgen_value(humidity_generator_snapshot, "Uw")
    dew_temp = _pick_snapshot_float(dewpoint_snapshot, "temp_c")
    hgen_temp = _pick_hgen_value(humidity_generator_snapshot, "Tc")
    rh_diff = None if dew_rh is None or hgen_rh is None else abs(float(dew_rh) - float(hgen_rh))
    temp_diff = None if dew_temp is None or hgen_temp is None else abs(float(dew_temp) - float(hgen_temp))
    hard_reasons = []
    warnings = []
    if dew_rh is not None and float(dew_rh) >= 99.0 and hgen_rh is not None and float(hgen_rh) < 95.0:
        hard_reasons.append("dewpoint_meter_reports_saturation_but_hgen_is_not_saturated")
    if rh_diff is not None and rh_diff > 5.5:
        warnings.append("dewpoint_rh_not_consistent_with_humidity_generator")
    if temp_diff is not None and temp_diff > 0.55:
        warnings.append("dewpoint_temperature_not_consistent_with_humidity_generator")
    reasons = [*hard_reasons, *warnings]
    status = "fail" if hard_reasons else "warn" if warnings else "review"
    if not hard_reasons and not warnings and dew_rh is not None and hgen_rh is not None:
        status = "pass"
    return {
        "status": status,
        "reasons": reasons,
        "hard_reasons": hard_reasons,
        "warnings": warnings,
        "hard_block": bool(hard_reasons),
        "reference_policy": "dewpoint_meter_primary_hgen_state_review",
        "dewpoint_rh_pct": dew_rh,
        "hgen_rh_pct": hgen_rh,
        "rh_diff_pct": rh_diff,
        "dewpoint_temp_c": dew_temp,
        "hgen_temp_c": hgen_temp,
        "temp_diff_c": temp_diff,
        "human_summary": (
            "Dewpoint meter reports saturation while the humidity generator does not; block formal H2O sampling."
            if hard_reasons
            else (
                "Dewpoint meter is the primary H2O reference; humidity-generator RH mismatch is review evidence."
                if warnings
                else "Humidity reference consistency passed or needs review with the full gate evidence."
            )
        ),
    }


def _write_humidity_reference_review(
    logger: RunLogger,
    *,
    point: CalibrationPoint,
    devices: Dict[str, Any],
    route_opened: bool,
) -> Dict[str, Any]:
    path = logger.run_dir / "h2o_humidity_reference_review.json"
    dewpoint_snapshot = _read_dewpoint_snapshot_for_evidence(devices.get("dewpoint"))
    humidity_generator_snapshot = _read_humidity_generator_snapshot(devices.get("humidity_gen"))
    check = _build_humidity_reference_check(dewpoint_snapshot, humidity_generator_snapshot)
    payload = {
        "schema_version": "v1_5_formal_h2o_open_flow_humidity_reference_review_v0",
        "timestamp": datetime.now().isoformat(timespec="milliseconds"),
        "route_opened": bool(route_opened),
        "hgen_target": {
            "temp_c": point.hgen_temp_c,
            "rh_pct": point.hgen_rh_pct,
        },
        "dewpoint_snapshot": dewpoint_snapshot,
        "humidity_generator_snapshot": humidity_generator_snapshot,
        "humidity_reference_check": check,
        "physical_interpretation": (
            "The calibrated dewpoint meter is the primary H2O reference for V1.5 open-flow sampling. "
            "The humidity generator state is retained as source-state evidence; medium RH disagreement "
            "does not by itself block sampling. Severe contradictions such as dewpoint-meter saturation "
            "while the generator is clearly non-saturated remain hard blockers."
        ),
    }
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, default=str) + "\n",
        encoding="utf-8",
    )
    return payload


def _set_h2o_open_flow_hgen_flow(
    devices: Dict[str, Any],
    flow_lpm: float,
    *,
    readback_timeout_s: float = 60.0,
    readback_poll_s: float = 1.0,
    readback_tolerance_lpm: float = 0.5,
) -> Dict[str, Any]:
    hgen = devices.get("humidity_gen")
    requested = float(flow_lpm)
    result: Dict[str, Any] = {
        "schema_version": "v1_5_h2o_open_flow_hgen_flow_set_v0",
        "timestamp": datetime.now().isoformat(timespec="milliseconds"),
        "requested_flow_lpm": requested,
        "ok": False,
        "flow_control_role": "optional_evidence_only",
        "before_snapshot": {},
        "after_snapshot": {},
        "observed_flow_lpm": None,
        "target_tolerance_lpm": abs(float(readback_tolerance_lpm)),
        "target_reached": False,
        "readback_timeout_s": max(0.0, float(readback_timeout_s)),
        "readback_poll_s": max(0.05, float(readback_poll_s)),
        "readback_snapshots": [],
    }
    if hgen is None:
        result["error"] = "humidity_generator_missing"
        return result
    setter = getattr(hgen, "set_flow_target", None)
    if not callable(setter):
        result["error"] = "set_flow_target_unsupported"
        return result

    result["before_snapshot"] = _read_humidity_generator_snapshot(hgen)
    try:
        setter(requested)
    except Exception as exc:
        result["error"] = str(exc)
        result["after_snapshot"] = _read_humidity_generator_snapshot(hgen)
        return result

    after: Dict[str, Any] = {}
    observed: Optional[float] = None
    tolerance = abs(float(readback_tolerance_lpm))
    deadline = time.time() + max(0.0, float(readback_timeout_s))
    while True:
        after = _read_humidity_generator_snapshot(hgen)
        observed = _pick_hgen_flow(after)
        result["readback_snapshots"].append(
            {
                "timestamp": datetime.now().isoformat(timespec="milliseconds"),
                "flow_lpm": observed,
                "target_error_lpm": None if observed is None else observed - requested,
                "snapshot": after,
            }
        )
        target_reached = bool(observed is not None and abs(observed - requested) <= tolerance)
        if target_reached or time.time() >= deadline:
            result["target_reached"] = target_reached
            break
        time.sleep(max(0.05, float(readback_poll_s)))
    result["after_snapshot"] = after
    result["observed_flow_lpm"] = observed
    result["ok"] = True
    if not result["target_reached"]:
        result["warning"] = "flow_target_readback_not_within_tolerance"
    return result


def _write_gate_failure(
    logger: RunLogger,
    *,
    reason: str,
    point: CalibrationPoint,
    devices: Dict[str, Any],
    route_opened: bool,
) -> Path:
    path = logger.run_dir / "formal_h2o_open_flow_gate_failure.json"
    dewpoint_snapshot = _read_dewpoint_snapshot_for_evidence(devices.get("dewpoint"))
    humidity_generator_snapshot = _read_humidity_generator_snapshot(devices.get("humidity_gen"))
    humidity_reference_check = _build_humidity_reference_check(dewpoint_snapshot, humidity_generator_snapshot)
    payload = {
        "schema_version": "v1_5_formal_h2o_open_flow_gate_failure_v0",
        "timestamp": datetime.now().isoformat(timespec="milliseconds"),
        "reason": str(reason),
        "route_opened": bool(route_opened),
        "hgen_target": {
            "temp_c": point.hgen_temp_c,
            "rh_pct": point.hgen_rh_pct,
        },
        "certificate": {
            "dewpoint_c": point.dewpoint_c,
            "h2o_mmol": point.h2o_mmol,
        },
        "dewpoint_snapshot": dewpoint_snapshot,
        "humidity_generator_snapshot": humidity_generator_snapshot,
        "humidity_reference_check": humidity_reference_check,
        "human_reject_reason": humidity_reference_check.get("human_summary"),
        "pace_pressure_hpa": _read_optional_float(devices.get("pace"), "read_pressure"),
        "com22_pressure_hpa": _read_optional_float(devices.get("pressure_gauge"), "read_pressure"),
        "physical_interpretation": (
            "H2O formal sampling was blocked before the sample window because the "
            "humidity evidence did not satisfy the configured gate. No H2O point "
            "from this run is eligible for formal fitting."
        ),
    }
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, default=str) + "\n",
        encoding="utf-8",
    )
    return path


def _validation_report_prefix(run_dir: Path) -> str:
    """Use compact report names only when Windows path length would be risky."""

    formal_prefix = "formal_h2o_open_flow_sampling_validation"
    if os.name != "nt":
        return formal_prefix
    metadata_path = run_dir / f"{formal_prefix}_meta.json"
    if len(str(metadata_path)) >= 240:
        return "h2o_validation"
    return formal_prefix


def _wait_h2o_analyzer_pressure_presample_gate(
    runner: CalibrationRunner,
    point: CalibrationPoint,
) -> bool:
    sensor_cfg = runner.cfg.get("workflow", {}).get("stability", {}).get("sensor", {})
    if not bool(sensor_cfg.get("h2o_pressure_kpa_presample_enabled", True)):
        runner.log("H2O analyzer pressure pre-sample gate skipped by configuration")
        set_runtime = getattr(runner, "_set_point_runtime_fields", None)
        if callable(set_runtime):
            set_runtime(
                point,
                phase="h2o",
                h2o_pressure_presample_gate_status="skipped",
                h2o_pressure_presample_gate_policy="skip",
                h2o_pressure_presample_gate_reason="disabled_by_configuration",
            )
        return True

    policy = str(sensor_cfg.get("h2o_pressure_kpa_presample_policy", "skip") or "skip").strip().lower()
    if policy not in {"warn", "fail", "skip"}:
        policy = "skip"
    if policy == "skip":
        runner.log("H2O analyzer pressure pre-sample gate skipped by policy")
        set_runtime = getattr(runner, "_set_point_runtime_fields", None)
        if callable(set_runtime):
            set_runtime(
                point,
                phase="h2o",
                h2o_pressure_presample_gate_status="skipped",
                h2o_pressure_presample_gate_policy=policy,
                h2o_pressure_presample_gate_reason="skipped_by_policy",
            )
        return True

    tol_kpa = float(sensor_cfg.get("h2o_pressure_kpa_presample_tol", 0.2) or 0.2)
    window_s = float(sensor_cfg.get("h2o_pressure_kpa_presample_window_s", 60.0) or 60.0)
    timeout_s = float(sensor_cfg.get("h2o_pressure_kpa_presample_timeout_s", 300.0) or 300.0)
    min_samples = int(sensor_cfg.get("h2o_pressure_kpa_presample_min_samples", 10) or 10)
    read_interval_s = float(sensor_cfg.get("h2o_pressure_kpa_presample_read_interval_s", 1.0) or 1.0)

    append_trace = getattr(runner, "_append_pressure_trace_row", None)
    if callable(append_trace):
        append_trace(
            point=point,
            route="h2o",
            point_phase="h2o",
            trace_stage="h2o_presample_analyzer_pressure_gate_begin",
            pressure_target_hpa=point.target_pressure_hpa,
            refresh_pace_state=False,
            note=(
                "key=pressure_kpa "
                f"tol_kpa={tol_kpa:.6g} window_s={window_s:.3f} "
                f"timeout_s={timeout_s:.3f} min_samples={min_samples} "
                "pressure_control=False"
            ),
        )

    disabled_before = set(getattr(runner, "_disabled_analyzers", set()) or set())
    reasons_before = dict(getattr(runner, "_disabled_analyzer_reasons", {}) or {})
    reprobe_before = dict(getattr(runner, "_disabled_analyzer_last_reprobe_ts", {}) or {})

    ok = runner._wait_primary_sensor_stable(
        point,
        value_key="pressure_kpa",
        require_pressure_in_limits=False,
        tol_override=tol_kpa,
        window_override=window_s,
        timeout_override=timeout_s,
        min_samples_override=min_samples,
        read_interval_override=read_interval_s,
    )

    pressure_disabled_labels: List[str] = []
    if policy == "warn":
        disabled_after = set(getattr(runner, "_disabled_analyzers", set()) or set())
        reasons_after = dict(getattr(runner, "_disabled_analyzer_reasons", {}) or {})
        pressure_disabled_labels = sorted(
            label
            for label in disabled_after - disabled_before
            if str(reasons_after.get(label, "")).startswith("pressure_kpa")
        )
        if pressure_disabled_labels:
            setattr(runner, "_disabled_analyzers", disabled_before)
            setattr(runner, "_disabled_analyzer_reasons", reasons_before)
            setattr(runner, "_disabled_analyzer_last_reprobe_ts", reprobe_before)
            runner.log(
                "H2O analyzer pressure pre-sample gate restored analyzers under warn policy: "
                f"labels={','.join(pressure_disabled_labels)}; pressure is QC evidence, "
                "not a formal H2O open-flow sampling exclusion gate"
            )

    if callable(append_trace):
        append_trace(
            point=point,
            route="h2o",
            point_phase="h2o",
            trace_stage="h2o_presample_analyzer_pressure_gate_end",
            pressure_target_hpa=point.target_pressure_hpa,
            refresh_pace_state=False,
            note=(
                f"result={'pass' if ok else 'fail'} key=pressure_kpa "
                f"tol_kpa={tol_kpa:.6g} window_s={window_s:.3f} "
                f"timeout_s={timeout_s:.3f} min_samples={min_samples}"
            ),
        )
    if ok and not pressure_disabled_labels:
        runner.log(
            "H2O analyzer pressure pre-sample gate passed: "
            f"tol={tol_kpa:g} kPa window_s={window_s:g} min_samples={min_samples}"
        )
        set_runtime = getattr(runner, "_set_point_runtime_fields", None)
        if callable(set_runtime):
            set_runtime(
                point,
                phase="h2o",
                h2o_pressure_presample_gate_status="pass",
                h2o_pressure_presample_gate_policy=policy,
                h2o_pressure_presample_gate_reason="ok",
            )
    else:
        message = (
            "H2O analyzer pressure pre-sample gate failed: "
            f"tol={tol_kpa:g} kPa window_s={window_s:g} min_samples={min_samples} policy={policy}"
        )
        if policy == "warn":
            if pressure_disabled_labels:
                message = (
                    "H2O analyzer pressure pre-sample gate found analyzer pressure instability: "
                    f"labels={','.join(pressure_disabled_labels)} "
                    f"tol={tol_kpa:g} kPa window_s={window_s:g} "
                    f"min_samples={min_samples} policy={policy}"
                )
            runner.log(message + "; continuing with diagnostic pressure evidence")
            set_runtime = getattr(runner, "_set_point_runtime_fields", None)
            if callable(set_runtime):
                set_runtime(
                    point,
                    phase="h2o",
                    h2o_pressure_presample_gate_status="warn",
                    h2o_pressure_presample_gate_policy=policy,
                    h2o_pressure_presample_gate_reason="analyzer_pressure_not_stable_before_sample_window",
                    h2o_pressure_presample_restored_analyzers=",".join(pressure_disabled_labels),
                    h2o_pressure_presample_fit_scope="diagnostic_not_fit_gate",
                    h2o_pressure_presample_grade_scope="not_required_for_a_grade_by_default",
                    h2o_pressure_presample_report_warning=(
                        "analyzer_internal_pressure_unstable_before_sample_window;"
                        "review_h2o_ratio_dewpoint_and_flow_evidence"
                    ),
                )
            return True
        runner.log(message)
        set_runtime = getattr(runner, "_set_point_runtime_fields", None)
        if callable(set_runtime):
            set_runtime(
                point,
                phase="h2o",
                h2o_pressure_presample_gate_status="fail",
                h2o_pressure_presample_gate_policy=policy,
                h2o_pressure_presample_gate_reason="analyzer_pressure_not_stable_before_sample_window",
                h2o_calibration_fit_blocked_reason="analyzer_pressure_presample_gate_fail",
                point_quality_status="fail",
                point_quality_reason="analyzer_pressure_presample_gate_fail",
                point_quality_flags="analyzer_pressure_presample_gate_fail",
                point_quality_blocked=True,
            )
    return bool(ok)


def _stats(values: List[float]) -> Dict[str, Optional[float]]:
    if not values:
        return {
            "count": 0,
            "first": None,
            "last": None,
            "min": None,
            "max": None,
            "mean": None,
            "span": None,
        }
    return {
        "count": len(values),
        "first": float(values[0]),
        "last": float(values[-1]),
        "min": float(min(values)),
        "max": float(max(values)),
        "mean": float(mean(values)),
        "span": float(max(values) - min(values)),
    }


def _analyzer_port_text(ga: Any) -> str:
    return str(getattr(getattr(ga, "ser", None), "port", "") or "")


def _enter_h2o_pressure_diagnostic_atmosphere(
    pace: Any,
    *,
    hold_interval_s: float = 2.0,
    vent_after_valve: bool = False,
) -> bool:
    if pace is None:
        return False
    if not vent_after_valve:
        _enter_continuous_atmosphere(pace, hold_interval_s=hold_interval_s)
        return False
    enter_open = getattr(pace, "enter_atmosphere_mode_with_open_vent_valve", None)
    if not callable(enter_open):
        raise RuntimeError("PACE_VENT_AFTER_VALVE_DIAGNOSTIC_UNSUPPORTED")
    enter_open(timeout_s=30.0, poll_s=0.25, popup_ack_enabled=False)
    start_hold = getattr(pace, "start_atmosphere_hold", None)
    if callable(start_hold):
        start_hold(interval_s=hold_interval_s)
    return True


def _close_pace_vent_after_valve_if_opened(pace: Any, opened: bool) -> None:
    if pace is None or not opened:
        return
    setter = getattr(pace, "set_vent_after_valve_open", None)
    if callable(setter):
        try:
            setter(False)
        except Exception:
            pass


def _read_h2o_pressure_diagnostic_analyzer_frame(
    runner: CalibrationRunner,
    ga: Any,
    *,
    drain_s: float,
    read_timeout_s: float = 0.02,
) -> tuple[str, Optional[Dict[str, Any]], str, int]:
    drain_lines = getattr(ga, "_drain_stream_lines", None)
    parse_mode2 = getattr(ga, "parse_line_mode2", None)
    parse_any = getattr(runner, "_parse_sensor_line", None)
    drain = max(0.02, float(drain_s))
    timeout = max(0.005, float(read_timeout_s))
    if callable(drain_lines):
        try:
            lines = list(drain_lines(drain_s=drain, read_timeout_s=timeout) or [])
        except TypeError:
            lines = list(drain_lines() or [])
        last_line = str(lines[-1] or "") if lines else ""
        for candidate in reversed(lines):
            text = str(candidate or "")
            if callable(parse_mode2):
                parsed = parse_mode2(text)
            elif callable(parse_any):
                parsed = parse_any(ga, text)
            else:
                parsed = None
            if isinstance(parsed, dict) and runner._as_float(parsed.get("pressure_kpa")) is not None:
                cache = getattr(runner, "_cache_live_analyzer_frame", None)
                if callable(cache):
                    try:
                        cache(
                            ga,
                            text,
                            parsed,
                            category="mode2_pressure_diagnostic",
                            label=str(getattr(ga, "_runtime_label", "") or ""),
                            source="h2o_pressure_diagnostic_active_stream",
                            is_live=True,
                        )
                    except Exception:
                        pass
                return text, parsed, "active_stream_drain", len(lines)
        if lines:
            return last_line, None, "active_stream_no_pressure_frame", len(lines)

    try:
        line, parsed = runner._read_sensor_parsed(
            ga,
            required_key="pressure_kpa",
            require_usable=False,
            frame_acceptance_mode="required_key_relaxed",
        )
        return line, parsed, "fallback_sensor_read", 0
    except Exception as exc:
        return "", {"status": f"read_error:{exc}"}, "read_error", 0


def _collect_h2o_pressure_stability_diagnostic(
    runner: CalibrationRunner,
    devices: Dict[str, Any],
    *,
    output_dir: Path,
    duration_s: float,
    interval_s: float,
    window_s: float,
    analyzer_span_hpa: float,
    min_samples: int,
    min_valid_analyzers: int,
    reference_interval_s: float = 5.0,
    analyzer_drain_s: float = 0.18,
) -> Dict[str, Any]:
    duration = max(1.0, float(duration_s))
    interval = max(0.2, float(interval_s))
    reference_interval = max(interval, float(reference_interval_s))
    analyzer_drain = max(0.02, float(analyzer_drain_s))
    window = max(interval, float(window_s))
    span_limit = max(0.0, float(analyzer_span_hpa))
    min_samples_required = max(1, int(min_samples))
    min_valid_required = max(1, int(min_valid_analyzers))
    trace_path = output_dir / "h2o_pressure_stability_diagnostic_trace.csv"
    summary_path = output_dir / "h2o_pressure_stability_diagnostic_summary.json"
    rows: List[Dict[str, Any]] = []
    start = time.time()
    end_at = start + duration
    fields = [
        "timestamp",
        "elapsed_s",
        "analyzer_label",
        "analyzer_port",
        "analyzer_raw_frame",
        "analyzer_device_id",
        "analyzer_pressure_kpa",
        "analyzer_pressure_hpa",
        "analyzer_h2o_ratio_f",
        "analyzer_h2o_mmol",
        "analyzer_co2_ppm",
        "analyzer_chamber_temp_c",
        "analyzer_case_temp_c",
        "analyzer_status",
        "frame_received",
        "parsed_ok",
        "pace_pressure_hpa",
        "com22_pressure_hpa",
        "dewpoint_c",
        "dew_temp_c",
        "dew_rh_pct",
        "dewpoint_flow_lpm",
        "hgen_temp_c",
        "hgen_rh_pct",
        "hgen_flow_lpm",
        "hgen_pc",
        "hgen_ps",
        "reference_age_s",
        "analyzer_frame_source",
        "analyzer_stream_line_count",
    ]

    last_reference: Dict[str, Any] = {}
    last_reference_elapsed: Optional[float] = None
    next_reference_at = start
    with trace_path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        while True:
            now = time.time()
            elapsed = now - start
            if not last_reference or now >= next_reference_at:
                dew = _read_dewpoint_snapshot(devices.get("dewpoint"))
                hgen = _read_humidity_generator_snapshot(devices.get("humidity_gen"))
                last_reference_elapsed = elapsed
                last_reference = {
                    "pace_pressure_hpa": _read_optional_float(devices.get("pace"), "read_pressure"),
                    "com22_pressure_hpa": _read_optional_float(devices.get("pressure_gauge"), "read_pressure"),
                    "dewpoint_c": _pick_snapshot_float(dew, "dewpoint_c"),
                    "dew_temp_c": _pick_snapshot_float(dew, "temp_c"),
                    "dew_rh_pct": _pick_snapshot_float(dew, "rh_pct"),
                    "dewpoint_flow_lpm": _pick_snapshot_float(dew, "flow_lpm"),
                    "hgen_temp_c": _pick_hgen_value(hgen, "Tc"),
                    "hgen_rh_pct": _pick_hgen_value(hgen, "Uw"),
                    "hgen_flow_lpm": _pick_hgen_flow(hgen),
                    "hgen_pc": _pick_hgen_value(hgen, "Pc"),
                    "hgen_ps": _pick_hgen_value(hgen, "Ps"),
                }
                next_reference_at = time.time() + reference_interval
                elapsed = time.time() - start
            shared = {
                "timestamp": datetime.now().isoformat(timespec="milliseconds"),
                "elapsed_s": elapsed,
                **last_reference,
                "reference_age_s": (
                    None if last_reference_elapsed is None else max(0.0, elapsed - last_reference_elapsed)
                ),
            }
            for label, ga, _cfg in runner._all_gas_analyzers():
                try:
                    setattr(ga, "_runtime_label", label)
                except Exception:
                    pass
                line, parsed, frame_source, stream_line_count = _read_h2o_pressure_diagnostic_analyzer_frame(
                    runner,
                    ga,
                    drain_s=analyzer_drain,
                )
                parsed_ok = isinstance(parsed, dict) and bool(parsed)
                pressure_kpa = runner._as_float(parsed.get("pressure_kpa")) if parsed_ok else None
                row = {
                    **shared,
                    "analyzer_label": label,
                    "analyzer_port": _analyzer_port_text(ga),
                    "analyzer_raw_frame": line,
                    "analyzer_device_id": parsed.get("id") if parsed_ok else None,
                    "analyzer_pressure_kpa": pressure_kpa,
                    "analyzer_pressure_hpa": None if pressure_kpa is None else pressure_kpa * 10.0,
                    "analyzer_h2o_ratio_f": runner._as_float(parsed.get("h2o_ratio_f")) if parsed_ok else None,
                    "analyzer_h2o_mmol": runner._as_float(parsed.get("h2o_mmol")) if parsed_ok else None,
                    "analyzer_co2_ppm": runner._as_float(parsed.get("co2_ppm")) if parsed_ok else None,
                    "analyzer_chamber_temp_c": runner._as_float(parsed.get("chamber_temp_c")) if parsed_ok else None,
                    "analyzer_case_temp_c": runner._as_float(parsed.get("case_temp_c")) if parsed_ok else None,
                    "analyzer_status": parsed.get("status") if parsed_ok else None,
                    "frame_received": bool(line),
                    "parsed_ok": parsed_ok,
                    "analyzer_frame_source": frame_source,
                    "analyzer_stream_line_count": stream_line_count,
                }
                rows.append(row)
                writer.writerow(row)
            handle.flush()
            if now >= end_at:
                break
            time.sleep(max(0.0, min(interval, end_at - now)))

    max_elapsed = max((float(row.get("elapsed_s") or 0.0) for row in rows), default=0.0)
    tail_cutoff = max(0.0, max_elapsed - window)
    by_label: Dict[str, List[Dict[str, Any]]] = {}
    for row in rows:
        by_label.setdefault(str(row.get("analyzer_label") or ""), []).append(row)

    per_analyzer: List[Dict[str, Any]] = []
    stable_count = 0
    for label, label_rows in sorted(by_label.items()):
        tail_rows = [row for row in label_rows if float(row.get("elapsed_s") or 0.0) >= tail_cutoff]
        all_p = [
            float(row["analyzer_pressure_hpa"])
            for row in label_rows
            if row.get("analyzer_pressure_hpa") is not None
        ]
        tail_p = [
            float(row["analyzer_pressure_hpa"])
            for row in tail_rows
            if row.get("analyzer_pressure_hpa") is not None
        ]
        offsets = [
            float(row["analyzer_pressure_hpa"]) - float(row["com22_pressure_hpa"])
            for row in tail_rows
            if row.get("analyzer_pressure_hpa") is not None and row.get("com22_pressure_hpa") is not None
        ]
        tail_stats = _stats(tail_p)
        stable = (
            int(tail_stats["count"] or 0) >= min_samples_required
            and tail_stats["span"] is not None
            and float(tail_stats["span"]) <= span_limit
        )
        if stable:
            stable_count += 1
        first = next((row for row in label_rows if row.get("analyzer_pressure_hpa") is not None), {})
        last = next((row for row in reversed(label_rows) if row.get("analyzer_pressure_hpa") is not None), {})
        per_analyzer.append(
            {
                "label": label,
                "port": first.get("analyzer_port") or last.get("analyzer_port"),
                "device_id_first": first.get("analyzer_device_id"),
                "device_id_last": last.get("analyzer_device_id"),
                "all_pressure_hpa": _stats(all_p),
                "tail_pressure_hpa": tail_stats,
                "tail_offset_from_com22_hpa": _stats(offsets),
                "stable": stable,
                "status": "stable" if stable else "unstable_or_insufficient",
            }
        )

    tail_rows_all = [row for row in rows if float(row.get("elapsed_s") or 0.0) >= tail_cutoff]
    com22_values = [
        float(row["com22_pressure_hpa"])
        for row in tail_rows_all
        if row.get("com22_pressure_hpa") is not None
    ]
    pace_values = [
        float(row["pace_pressure_hpa"])
        for row in tail_rows_all
        if row.get("pace_pressure_hpa") is not None
    ]
    dew_values = [
        float(row["dewpoint_c"])
        for row in tail_rows_all
        if row.get("dewpoint_c") is not None
    ]
    dew_flow_values = [
        float(row["dewpoint_flow_lpm"])
        for row in tail_rows_all
        if row.get("dewpoint_flow_lpm") is not None
    ]
    com22_stats = _stats(com22_values)
    pace_stats = _stats(pace_values)
    dew_stats = _stats(dew_values)
    dew_flow_stats = _stats(dew_flow_values)
    external_pressure_stable = (
        (com22_stats["span"] is None or float(com22_stats["span"]) <= 0.5)
        and (pace_stats["span"] is None or float(pace_stats["span"]) <= 1.0)
    )
    enough_analyzers = stable_count >= min(min_valid_required, max(1, len(by_label)))
    if not external_pressure_stable:
        status = "fail"
        interpretation = "external_pressure_reference_unstable"
    elif enough_analyzers:
        max_abs_offset = max(
            (
                abs(float(item["tail_offset_from_com22_hpa"]["mean"]))
                for item in per_analyzer
                if item["tail_offset_from_com22_hpa"]["mean"] is not None
            ),
            default=0.0,
        )
        if max_abs_offset > 5.0:
            status = "review"
            interpretation = "stable_wet_route_local_backpressure_observed"
        else:
            status = "pass"
            interpretation = "analyzer_internal_pressure_ready_for_h2o_sampling"
    else:
        status = "fail"
        interpretation = "analyzer_internal_pressure_unstable_or_cache_not_refreshed"

    payload = {
        "schema_version": "v1_5_h2o_pressure_stability_diagnostic_v0",
        "timestamp": datetime.now().isoformat(timespec="milliseconds"),
        "duration_s": duration,
        "interval_s": interval,
        "reference_interval_s": reference_interval,
        "analyzer_drain_s": analyzer_drain,
        "tail_window_s": window,
        "analyzer_pressure_span_limit_hpa": span_limit,
        "min_samples": min_samples_required,
        "min_valid_analyzers": min_valid_required,
        "rows": len(rows),
        "trace_csv": str(trace_path),
        "per_analyzer": per_analyzer,
        "tail_com22_pressure_hpa": com22_stats,
        "tail_pace_pressure_hpa": pace_stats,
        "tail_dewpoint_c": dew_stats,
        "tail_dewpoint_flow_lpm": dew_flow_stats,
        "stable_analyzer_count": stable_count,
        "external_pressure_stable": external_pressure_stable,
        "status": status,
        "physical_interpretation": interpretation,
        "no_write_assertion": {
            "writes_senco": False,
            "writes_device_id": False,
            "writes_ftd": False,
            "sealed_pressure_control": False,
            "pace_output_control": False,
        },
    }
    summary_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, default=str) + "\n",
        encoding="utf-8",
    )
    return payload


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    if not args.no_prompt:
        _log("Refusing to run real H2O open-flow sampling without --no-prompt in this sidecar tool.")
        return 2
    if args.pace_vent_after_valve_diagnostic and not args.pressure_diagnostic_only:
        _log("Refusing --pace-vent-after-valve-diagnostic outside --pressure-diagnostic-only.")
        return 2
    if args.pressure_diagnostic_observe_hgen_only:
        if not args.pressure_diagnostic_only or not args.pressure_diagnostic_after_purge:
            _log(
                "Refusing --pressure-diagnostic-observe-hgen-only outside "
                "--pressure-diagnostic-only --pressure-diagnostic-after-purge."
            )
            return 2
    if args.pressure_diagnostic_route_closed_baseline and not args.pressure_diagnostic_only:
        _log("Refusing --pressure-diagnostic-route-closed-baseline without --pressure-diagnostic-only.")
        return 2

    cfg = load_config(args.config)
    runtime_cfg = _prepare_runtime_cfg(
        cfg,
        output_dir=args.output_dir,
        sample_count=args.sample_count,
        sample_interval_s=args.sample_interval_s,
        sensor_read_interval_s=args.sensor_read_interval_s,
        hgen_flow_lpm=args.hgen_flow_lpm,
        analyzer_acquisition=args.analyzer_acquisition,
        allow_ftd_write=args.allow_ftd_write,
        min_valid_analyzers=args.min_valid_analyzers,
        h2o_pressure_presample_policy=args.h2o_pressure_presample_policy,
        keep_hgen_running_after_point=args.keep_hgen_running_after_point,
    )
    metadata = runtime_cfg.setdefault("metadata", {})
    metadata["strict_humidity_reference_match"] = bool(args.strict_humidity_reference_match)
    metadata["humidity_reference_match_gate"] = (
        "hard_opt_in" if args.strict_humidity_reference_match else "dewpoint_primary_review_default"
    )
    metadata["h2o_pressure_diagnostic_hgen_control_role"] = (
        "observe_only_no_prepare_no_flow_no_safe_stop"
        if args.pressure_diagnostic_observe_hgen_only or args.pressure_diagnostic_route_closed_baseline
        else metadata.get("h2o_hgen_shutdown_policy")
    )
    output_dir = Path(runtime_cfg["paths"]["output_dir"]).resolve()
    run_id = args.run_id or f"formal_h2o_open_flow_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    logger = RunLogger(output_dir, run_id=run_id, cfg=runtime_cfg)
    runtime_snapshot_path = logger.run_dir / "runtime_config_snapshot.json"
    runtime_snapshot_path.write_text(
        json.dumps(runtime_cfg, ensure_ascii=False, indent=2, default=str) + "\n",
        encoding="utf-8",
    )
    devices: Dict[str, Any] = {}
    runner: Optional[CalibrationRunner] = None
    route_opened = False
    pace_vent_after_valve_opened = False

    try:
        point = _build_h2o_open_flow_point(
            temp_c=args.temp,
            hgen_temp_c=args.hgen_temp,
            hgen_rh_pct=args.hgen_rh,
            certificate_dewpoint_c=args.certificate_dewpoint_c,
            certificate_h2o_mmol=args.certificate_h2o_mmol,
        )
        point_tag = runner_tag = f"h2o_{int(round(args.hgen_temp))}c_{int(round(args.hgen_rh))}rh_open_flow"
        ftd_state = "FTD=01 enabled" if args.allow_ftd_write else "FTD write disabled"
        _log(
            "V1.5 H2O open-flow sidecar: no sealed pressure control, no OUTP control, "
            f"no SENCO/ID writes, {ftd_state}."
        )
        devices = _build_devices(runtime_cfg, io_logger=logger)
        runner = CalibrationRunner(runtime_cfg, devices, logger, _log, lambda *_: None)
        runner._configure_devices()
        runner._startup_preflight_reset()
        _defer_startup_mode2_disabled_analyzers(runner)

        if args.pressure_diagnostic_route_closed_baseline:
            pace = devices.get("pace")
            pace_vent_after_valve_opened = _enter_h2o_pressure_diagnostic_atmosphere(
                pace,
                hold_interval_s=2.0,
                vent_after_valve=False,
            )
            runner._apply_valve_states([])
            settle_s = max(0.0, float(args.route_closed_baseline_settle_s))
            if settle_s > 0:
                _log(f"H2O route-closed pressure baseline settle: {settle_s:g}s")
                time.sleep(settle_s)
            baseline_path = logger.run_dir / "h2o_pressure_route_closed_baseline.json"
            baseline_path.write_text(
                json.dumps(
                    {
                        "schema_version": "v1_5_h2o_pressure_route_closed_baseline_v0",
                        "timestamp": datetime.now().isoformat(timespec="milliseconds"),
                        "control_role": "route_closed_analyzer_internal_pressure_baseline",
                        "physical_purpose": (
                            "Check whether analyzer internal pressure_kpa moves when the H2O route is "
                            "not opened and the humidity generator is not controlled. This separates "
                            "sensor/cache behavior from serial water-route local pressure behavior."
                        ),
                        "route_opened": False,
                        "open_valves": [],
                        "settle_s": settle_s,
                        "humidity_generator_snapshot": _read_humidity_generator_snapshot(
                            devices.get("humidity_gen")
                        ),
                        "no_write_assertion": {
                            "opens_h2o_route": False,
                            "sends_hgen_target": False,
                            "sends_hgen_control": False,
                            "sends_hgen_flow": False,
                            "sends_hgen_safe_stop": False,
                            "writes_senco": False,
                            "writes_device_id": False,
                            "writes_ftd": False,
                            "sealed_pressure_control": False,
                            "pace_output_control": False,
                        },
                    },
                    ensure_ascii=False,
                    indent=2,
                    default=str,
                )
                + "\n",
                encoding="utf-8",
            )
            diagnostic = _collect_h2o_pressure_stability_diagnostic(
                runner,
                devices,
                output_dir=logger.run_dir,
                duration_s=args.pressure_diagnostic_s,
                interval_s=args.pressure_diagnostic_interval_s,
                window_s=args.pressure_diagnostic_window_s,
                analyzer_span_hpa=args.pressure_diagnostic_span_hpa,
                min_samples=args.pressure_diagnostic_min_samples,
                min_valid_analyzers=args.min_valid_analyzers,
                reference_interval_s=args.pressure_diagnostic_reference_interval_s,
                analyzer_drain_s=args.pressure_diagnostic_analyzer_drain_s,
            )
            _log(
                "H2O route-closed analyzer pressure baseline saved: "
                f"{diagnostic.get('trace_csv')} status={diagnostic.get('status')} "
                f"stable_analyzers={diagnostic.get('stable_analyzer_count')} "
                f"physical={diagnostic.get('physical_interpretation')}"
            )
            return 1 if args.pressure_diagnostic_fail_on_unstable and diagnostic.get("status") == "fail" else 0

        if args.pressure_diagnostic_observe_hgen_only:
            hgen_observe_path = logger.run_dir / "h2o_pressure_diagnostic_hgen_observe_only.json"
            hgen_observe_path.write_text(
                json.dumps(
                    {
                        "schema_version": "v1_5_h2o_pressure_diagnostic_hgen_observe_only_v0",
                        "timestamp": datetime.now().isoformat(timespec="milliseconds"),
                        "control_role": "observe_only_no_prepare_no_flow_no_safe_stop",
                        "physical_purpose": (
                            "Separate H2O serial-route pressure behavior from humidity-generator "
                            "target/control/flow commands. The route and PACE atmosphere hold are "
                            "tested while the generator state is only observed."
                        ),
                        "humidity_generator_snapshot": _read_humidity_generator_snapshot(
                            devices.get("humidity_gen")
                        ),
                        "no_write_assertion": {
                            "sends_hgen_target": False,
                            "sends_hgen_control": False,
                            "sends_hgen_flow": False,
                            "sends_hgen_safe_stop": False,
                            "writes_senco": False,
                            "writes_device_id": False,
                            "writes_ftd": False,
                        },
                    },
                    ensure_ascii=False,
                    indent=2,
                    default=str,
                )
                + "\n",
                encoding="utf-8",
            )
            _log(
                "H2O pressure diagnostic observe-only mode: humidity generator target/control/"
                "flow/safe-stop commands are skipped; snapshot saved."
            )
        else:
            runner._prepare_humidity_generator(point)
        if (
            not args.pressure_diagnostic_observe_hgen_only
            and not args.skip_humidity_generator_gate
            and not runner._wait_humidity_generator_stable(point)
        ):
            failure_path = _write_gate_failure(
                logger,
                reason="humidity_generator_gate_failed",
                point=point,
                devices=devices,
                route_opened=route_opened,
            )
            _log(f"H2O open-flow gate failure evidence saved: {failure_path}")
            _log("H2O open-flow humidity-generator gate failed.")
            return 1

        pace = devices.get("pace")
        pace_vent_after_valve_opened = _enter_h2o_pressure_diagnostic_atmosphere(
            pace,
            hold_interval_s=2.0,
            vent_after_valve=bool(args.pace_vent_after_valve_diagnostic),
        )
        open_valves = runner._h2o_open_valves(point)
        runner._apply_valve_states(open_valves)
        route_opened = True
        _log(f"H2O open-flow route opened: valves={open_valves}")
        if args.pressure_diagnostic_observe_hgen_only:
            flow_result = {
                "schema_version": "v1_5_h2o_open_flow_hgen_flow_set_v0",
                "timestamp": datetime.now().isoformat(timespec="milliseconds"),
                "flow_control_role": "observe_only_no_hgen_flow_command",
                "requested_flow_lpm": None,
                "ok": True,
                "before_snapshot": _read_humidity_generator_snapshot(devices.get("humidity_gen")),
                "after_snapshot": {},
                "observed_flow_lpm": None,
                "target_reached": None,
                "note": (
                    "Flow target command skipped by observe-only pressure diagnostic; "
                    "humidity-generator state is evidence, not software-controlled input."
                ),
            }
        elif args.hgen_flow_lpm is None:
            flow_result = {
                "schema_version": "v1_5_h2o_open_flow_hgen_flow_set_v0",
                "timestamp": datetime.now().isoformat(timespec="milliseconds"),
                "flow_control_role": "internal_control_not_touched",
                "requested_flow_lpm": None,
                "ok": True,
                "before_snapshot": _read_humidity_generator_snapshot(devices.get("humidity_gen")),
                "after_snapshot": {},
                "observed_flow_lpm": None,
                "target_reached": None,
                "note": "Flow target command skipped; humidity generator internal control is authoritative.",
            }
        else:
            flow_result = _set_h2o_open_flow_hgen_flow(
                devices,
                args.hgen_flow_lpm,
                readback_timeout_s=args.hgen_flow_readback_timeout_s,
                readback_poll_s=args.hgen_flow_readback_poll_s,
                readback_tolerance_lpm=args.hgen_flow_readback_tolerance_lpm,
            )
        flow_path = logger.run_dir / "formal_h2o_open_flow_hgen_flow_set.json"
        flow_path.write_text(
            json.dumps(flow_result, ensure_ascii=False, indent=2, default=str) + "\n",
            encoding="utf-8",
        )
        if args.pressure_diagnostic_observe_hgen_only:
            _log(
                "H2O pressure diagnostic observe-only: humidity-generator flow command skipped; "
                "current state is recorded as evidence only."
            )
        elif args.hgen_flow_lpm is None:
            _log("H2O open-flow humidity-generator flow target skipped; internal flow control left untouched.")
        else:
            _log(
                "H2O open-flow humidity-generator flow target recorded: "
                f"requested={args.hgen_flow_lpm:g}L/min observed={flow_result.get('observed_flow_lpm')} "
                f"target_reached={flow_result.get('target_reached')} warning={flow_result.get('warning')}"
            )

        meta_path = logger.run_dir / "formal_h2o_open_flow_sidecar_metadata.json"
        meta_path.write_text(
            json.dumps(
                {
                    "schema_version": "v1_5_formal_h2o_open_flow_sidecar_v0",
                    "run_id": run_id,
                    "hgen_temp_c": args.hgen_temp,
                    "hgen_rh_pct": args.hgen_rh,
                    "certificate_h2o_mmol": args.certificate_h2o_mmol,
                    "certificate_dewpoint_c": args.certificate_dewpoint_c,
                    "certificate_uncertainty_mmol": args.certificate_uncertainty_mmol,
                    "hgen_flow_lpm": args.hgen_flow_lpm,
                    "hgen_flow_readback_timeout_s": args.hgen_flow_readback_timeout_s,
                    "hgen_flow_readback_poll_s": args.hgen_flow_readback_poll_s,
                    "hgen_flow_readback_tolerance_lpm": args.hgen_flow_readback_tolerance_lpm,
                    "pressure_diagnostic_observe_hgen_only": bool(args.pressure_diagnostic_observe_hgen_only),
                    "h2o_pressure_diagnostic_hgen_control_role": metadata.get(
                        "h2o_pressure_diagnostic_hgen_control_role"
                    ),
                    "open_valves": open_valves,
                    "pace_mode": (
                        "vent_after_valve_diagnostic_atmosphere_hold"
                        if args.pace_vent_after_valve_diagnostic
                        else "continuous_atmosphere_hold"
                    ),
                    "continuous_atmosphere_hold_scope": "h2o_open_flow_purge_and_sampling_only",
                    "pace_vent_after_valve_diagnostic": bool(args.pace_vent_after_valve_diagnostic),
                    "idle_continuous_atmosphere_hold": False,
                    "analyzer_acquisition_policy": metadata.get("analyzer_acquisition_policy"),
                    "analyzer_stream_native_hz": metadata.get("analyzer_stream_native_hz"),
                    "formal_sample_anchor_interval_s": metadata.get("formal_sample_anchor_interval_s"),
                    "formal_sample_decimation": metadata.get("formal_sample_decimation"),
                    "strict_humidity_reference_match": bool(args.strict_humidity_reference_match),
                    "humidity_reference_match_gate": metadata.get("humidity_reference_match_gate"),
                    "humidity_reference_role": metadata.get("humidity_reference_role"),
                    "h2o_open_flow_wait_contract": metadata.get("h2o_open_flow_wait_contract"),
                    "actual_purge_s": float(args.purge_s),
                    "minimum_purge_s": float(args.minimum_purge_s),
                    "route_open_until_sample_end": True,
                    "pressure_diagnostic_only": bool(args.pressure_diagnostic_only),
                    "pressure_diagnostic_after_purge": bool(args.pressure_diagnostic_after_purge),
                    "pressure_diagnostic_s": float(args.pressure_diagnostic_s),
                    "pressure_diagnostic_interval_s": float(args.pressure_diagnostic_interval_s),
                    "pressure_diagnostic_reference_interval_s": float(args.pressure_diagnostic_reference_interval_s),
                    "pressure_diagnostic_analyzer_drain_s": float(args.pressure_diagnostic_analyzer_drain_s),
                    "pressure_diagnostic_window_s": float(args.pressure_diagnostic_window_s),
                    "pressure_diagnostic_span_hpa": float(args.pressure_diagnostic_span_hpa),
                    "pressure_diagnostic_min_samples": int(args.pressure_diagnostic_min_samples),
                    "h2o_pressure_presample_policy": args.h2o_pressure_presample_policy,
                    "h2o_hgen_shutdown_policy": metadata.get("h2o_hgen_shutdown_policy"),
                    "ftd_write_enabled": bool(args.allow_ftd_write),
                    "sealed_pressure_control": False,
                    "writes_senco": False,
                    "writes_device_id": False,
                },
                ensure_ascii=False,
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )

        purge_path = logger.run_dir / "formal_h2o_open_flow_purge_trace.csv"
        _log(f"H2O open-flow purge start: {args.purge_s:g}s")
        _write_purge_trace(
            purge_path,
            devices=devices,
            open_valves=open_valves,
            purge_s=float(args.purge_s),
            interval_s=float(args.purge_trace_interval_s),
            max_pressure_hpa=float(args.max_open_flow_pressure_hpa),
            transient_grace_s=float(args.open_flow_pressure_transient_grace_s),
            hard_limit_hpa=float(args.open_flow_pressure_safety_hard_limit_hpa),
        )
        _log(f"H2O open-flow purge trace saved: {purge_path}")

        if args.pressure_diagnostic_after_purge:
            if not args.pressure_diagnostic_only:
                _log("Refusing --pressure-diagnostic-after-purge without --pressure-diagnostic-only.")
                return 2
            diagnostic = _collect_h2o_pressure_stability_diagnostic(
                runner,
                devices,
                output_dir=logger.run_dir,
                duration_s=args.pressure_diagnostic_s,
                interval_s=args.pressure_diagnostic_interval_s,
                window_s=args.pressure_diagnostic_window_s,
                analyzer_span_hpa=args.pressure_diagnostic_span_hpa,
                min_samples=args.pressure_diagnostic_min_samples,
                min_valid_analyzers=args.min_valid_analyzers,
                reference_interval_s=args.pressure_diagnostic_reference_interval_s,
                analyzer_drain_s=args.pressure_diagnostic_analyzer_drain_s,
            )
            _log(
                "H2O serial pressure-gradient diagnostic saved after purge: "
                f"{diagnostic.get('trace_csv')} status={diagnostic.get('status')} "
                f"stable_analyzers={diagnostic.get('stable_analyzer_count')} "
                f"physical={diagnostic.get('physical_interpretation')}"
            )
            return 1 if args.pressure_diagnostic_fail_on_unstable and diagnostic.get("status") == "fail" else 0

        if not args.skip_dewpoint_gate:
            if not runner._ensure_dewpoint_meter_ready():
                failure_path = _write_gate_failure(
                    logger,
                    reason="dewpoint_meter_ready_check_failed",
                    point=point,
                    devices=devices,
                    route_opened=route_opened,
                )
                _log(f"H2O open-flow gate failure evidence saved: {failure_path}")
                _log("H2O open-flow dewpoint meter ready check failed.")
                return 1
            reference_review = _write_humidity_reference_review(
                logger,
                point=point,
                devices=devices,
                route_opened=route_opened,
            )
            reference_check = reference_review.get("humidity_reference_check", {})
            if reference_check.get("hard_block"):
                failure_path = _write_gate_failure(
                    logger,
                    reason="severe_humidity_reference_contradiction",
                    point=point,
                    devices=devices,
                    route_opened=route_opened,
                )
                _log(f"H2O open-flow gate failure evidence saved: {failure_path}")
                _log("H2O open-flow humidity reference contradiction is severe; sampling blocked.")
                return 1
            if args.strict_humidity_reference_match:
                if not runner._wait_dewpoint_alignment_stable(point):
                    failure_path = _write_gate_failure(
                        logger,
                        reason="dewpoint_alignment_gate_failed",
                        point=point,
                        devices=devices,
                        route_opened=route_opened,
                    )
                    _log(f"H2O open-flow gate failure evidence saved: {failure_path}")
                    _log("H2O open-flow dewpoint/humidity-generator strict alignment gate failed.")
                    return 1
            else:
                _log(
                    "H2O open-flow uses dewpoint meter as the primary humidity reference; "
                    "humidity-generator RH agreement is recorded as review evidence, not a hard gate."
                )
            if not runner._wait_h2o_route_dewpoint_gate_before_sampling(point, log_context="H2O sidecar route opened"):
                failure_path = _write_gate_failure(
                    logger,
                    reason="route_dewpoint_gate_failed",
                    point=point,
                    devices=devices,
                    route_opened=route_opened,
                )
                _log(f"H2O open-flow gate failure evidence saved: {failure_path}")
                _log("H2O open-flow route dewpoint gate failed.")
                return 1
            if not runner._wait_h2o_precondition_primary_sensor_gate(point):
                failure_path = _write_gate_failure(
                    logger,
                    reason="analyzer_h2o_signal_gate_failed",
                    point=point,
                    devices=devices,
                    route_opened=route_opened,
                )
                _log(f"H2O open-flow gate failure evidence saved: {failure_path}")
                _log("H2O open-flow analyzer H2O signal gate failed.")
                return 1
            if args.pressure_diagnostic_only:
                diagnostic = _collect_h2o_pressure_stability_diagnostic(
                    runner,
                    devices,
                    output_dir=logger.run_dir,
                    duration_s=args.pressure_diagnostic_s,
                    interval_s=args.pressure_diagnostic_interval_s,
                    window_s=args.pressure_diagnostic_window_s,
                    analyzer_span_hpa=args.pressure_diagnostic_span_hpa,
                    min_samples=args.pressure_diagnostic_min_samples,
                    min_valid_analyzers=args.min_valid_analyzers,
                    reference_interval_s=args.pressure_diagnostic_reference_interval_s,
                    analyzer_drain_s=args.pressure_diagnostic_analyzer_drain_s,
                )
                _log(
                    "H2O pressure stability diagnostic saved: "
                    f"{diagnostic.get('trace_csv')} status={diagnostic.get('status')} "
                    f"stable_analyzers={diagnostic.get('stable_analyzer_count')} "
                    f"physical={diagnostic.get('physical_interpretation')}"
                )
                return 1 if args.pressure_diagnostic_fail_on_unstable and diagnostic.get("status") == "fail" else 0
            if not _wait_h2o_analyzer_pressure_presample_gate(runner, point):
                failure_path = _write_gate_failure(
                    logger,
                    reason="analyzer_pressure_presample_gate_failed",
                    point=point,
                    devices=devices,
                    route_opened=route_opened,
                )
                _log(f"H2O open-flow gate failure evidence saved: {failure_path}")
                _log("H2O open-flow analyzer internal pressure gate failed.")
                return 1

        runner._set_point_runtime_fields(
            point,
            phase="h2o",
            formal_h2o_open_flow_sidecar=True,
            route_open_until_sample_end=True,
            h2o_route_open_until_sample_end=True,
            actual_purge_s=float(args.purge_s),
            minimum_purge_s=float(args.minimum_purge_s),
            open_flow_purge_elapsed_s=float(args.purge_s),
            sample_readiness_basis=(
                "minimum_purge_plus_dewpoint_h2o_ratio_pressure_temperature_traceability"
            ),
            standard_h2o_certificate_value_mmol=args.certificate_h2o_mmol,
            standard_h2o_certificate_uncertainty_mmol=args.certificate_uncertainty_mmol,
            standard_h2o_certificate_dewpoint_c=args.certificate_dewpoint_c,
            sealed_pressure_control=False,
        )
        runner._sample_and_log(point, phase="h2o", point_tag=runner_tag)
        machine_sample_paths = _write_machine_readable_samples(logger.run_dir, runner._all_samples)
        tables = analyze_sample_rows(runner._all_samples, cfg=runtime_cfg, gas="h2o", modes=("current",))
        validation_prefix = _validation_report_prefix(logger.run_dir)
        outputs = write_validation_report(
            logger.run_dir,
            prefix=validation_prefix,
            metadata=ValidationMetadata(
                tool_name="run_v1_5_formal_h2o_open_flow_sampling",
                analyzers=sorted(
                    {
                        str(row.get("Analyzer") or "")
                        for row in tables["frame_quality_summary"]
                        if row.get("Analyzer")
                    }
                ),
                input_paths=[
                    str(logger.samples_path),
                    str(machine_sample_paths["csv"]),
                    str(machine_sample_paths["jsonl"]),
                    str(logger.points_path),
                    str(runtime_snapshot_path),
                    str(meta_path),
                    str(flow_path),
                    str(purge_path),
                ],
                output_dir=str(logger.run_dir),
                config_path=str(Path(args.config).resolve()),
                config_summary={
                    "hgen_temp_c": float(args.hgen_temp),
                    "hgen_rh_pct": float(args.hgen_rh),
                    "hgen_flow_lpm": None if args.hgen_flow_lpm is None else float(args.hgen_flow_lpm),
                    "hgen_flow_readback_timeout_s": float(args.hgen_flow_readback_timeout_s),
                    "hgen_flow_readback_poll_s": float(args.hgen_flow_readback_poll_s),
                    "hgen_flow_readback_tolerance_lpm": float(args.hgen_flow_readback_tolerance_lpm),
                    "sample_count": int(args.sample_count),
                    "sample_interval_s": float(args.sample_interval_s),
                    "purge_s": float(args.purge_s),
                    "minimum_purge_s": float(args.minimum_purge_s),
                    "route_open_until_sample_end": True,
                    "max_open_flow_pressure_hpa": float(args.max_open_flow_pressure_hpa),
                    "sensor_read_interval_s": float(args.sensor_read_interval_s),
                    "analyzer_acquisition_policy": metadata.get("analyzer_acquisition_policy"),
                    "analyzer_stream_native_hz": metadata.get("analyzer_stream_native_hz"),
                    "formal_sample_anchor_interval_s": metadata.get("formal_sample_anchor_interval_s"),
                    "formal_sample_decimation": metadata.get("formal_sample_decimation"),
                    "strict_humidity_reference_match": bool(args.strict_humidity_reference_match),
                    "humidity_reference_match_gate": metadata.get("humidity_reference_match_gate"),
                    "humidity_reference_role": metadata.get("humidity_reference_role"),
                    "h2o_open_flow_wait_contract": metadata.get("h2o_open_flow_wait_contract"),
                    "h2o_pressure_presample_policy": args.h2o_pressure_presample_policy,
                    "ftd_write_enabled": False,
                    "idle_continuous_atmosphere_hold": False,
                    "continuous_atmosphere_hold_scope": "h2o_open_flow_purge_and_sampling_only",
                    "sealed_pressure_control": False,
                },
                notes=[
                    "PACE was held open to atmosphere only during H2O route purge/sampling.",
                    (
                        "H2O route valves are closed after sampling; the humidity generator remains "
                        "running for the queue-level next point and final safe-stop."
                        if args.keep_hgen_running_after_point
                        else "H2O route valves are closed and the humidity generator is safe-stopped after sampling."
                    ),
                    "Humidity generator flow is left under internal device control by default; explicit flow targets are evidence-only.",
                    (
                        "No SENCO or ID writes are performed; FTD=01 is used only when "
                        "--allow-ftd-write is enabled for formal 1 Hz active upload."
                    ),
                    "This sidecar samples one open-flow H2O condition and does not run sealed pressure points.",
                ],
            ),
            tables=tables,
        )
        _log(f"Formal H2O open-flow sampling validation saved: {outputs['workbook']}")
        if validation_prefix != "formal_h2o_open_flow_sampling_validation":
            _log(f"Formal H2O validation used compact artifact prefix: {validation_prefix}")
        return 0
    except Exception as exc:
        _log(f"Formal H2O open-flow sampling failed: {exc}")
        return 1
    finally:
        if runner is not None and route_opened:
            try:
                runner._apply_valve_states([])
                _log("H2O open-flow route closed")
            except Exception as exc:
                _log(f"H2O route close failed: {exc}")
        _stop_continuous_atmosphere(devices.get("pace"))
        _close_pace_vent_after_valve_if_opened(devices.get("pace"), pace_vent_after_valve_opened)
        if args.pressure_diagnostic_observe_hgen_only or args.pressure_diagnostic_route_closed_baseline:
            _log("Humidity generator safe-stop skipped by observe-only pressure diagnostic mode.")
        elif args.keep_hgen_running_after_point:
            _log("Humidity generator safe-stop skipped; H2O queue will manage final safe-stop.")
        else:
            _safe_stop_humidity_generator(devices)
        _close_devices(devices)
        try:
            logger.close()
        except Exception:
            pass


if __name__ == "__main__":
    sys.exit(main())
