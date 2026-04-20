"""Guarded minimal V1 live validation for AtmosphereGate and RouteFlush gates.

This tool is intentionally narrower than formal smoke:
- no full calibration run
- no setpoint sweep
- no analyzer sampling flow
- only V1 live engineering validation
"""

from __future__ import annotations

import argparse
import copy
import csv
import json
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional

from ..config import load_config
from ..data.points import CalibrationPoint
from ..logging_utils import RunLogger
from ..workflow.runner import CalibrationRunner
from .run_headless import _build_devices, _close_devices
from .safe_stop import perform_safe_stop_with_retries


REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_OUTPUT_DIR = REPO_ROOT / "results" / "pressure_gate_live"


def _log(message: str) -> None:
    print(message, flush=True)


def _read_csv_rows(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def _trace_row_count(path: Path) -> int:
    return len(_read_csv_rows(path))


def _disable_unneeded_devices(
    runtime_cfg: Dict[str, Any],
    *,
    need_dewpoint: bool,
    need_analyzer_pressure: bool,
) -> None:
    devices_cfg = runtime_cfg.setdefault("devices", {})
    keep_enabled = {"pressure_controller", "pressure_gauge", "relay", "relay_8"}
    if need_dewpoint:
        keep_enabled.add("dewpoint_meter")
    if need_analyzer_pressure:
        keep_enabled.add("gas_analyzer")
    for name, dev_cfg in devices_cfg.items():
        if name == "gas_analyzers" and isinstance(dev_cfg, list):
            for item in dev_cfg:
                if isinstance(item, dict):
                    item["enabled"] = False
            continue
        if not isinstance(dev_cfg, dict) or "enabled" not in dev_cfg:
            continue
        dev_cfg["enabled"] = name in keep_enabled and bool(dev_cfg.get("enabled", False))


def _prepare_runtime_cfg(
    cfg: Mapping[str, Any],
    args: argparse.Namespace,
    *,
    need_dewpoint: bool,
    need_analyzer_pressure: bool,
) -> Dict[str, Any]:
    runtime_cfg = copy.deepcopy(dict(cfg))
    runtime_cfg.setdefault("paths", {})["output_dir"] = str(
        Path(args.output_dir).resolve() if args.output_dir else DEFAULT_OUTPUT_DIR.resolve()
    )
    runtime_cfg.setdefault("workflow", {})["collect_only"] = True
    runtime_cfg["workflow"]["skip_h2o"] = True
    _disable_unneeded_devices(
        runtime_cfg,
        need_dewpoint=need_dewpoint,
        need_analyzer_pressure=need_analyzer_pressure,
    )
    devices_cfg = runtime_cfg.setdefault("devices", {})
    if args.relay_port and isinstance(devices_cfg.get("relay"), dict):
        devices_cfg["relay"]["port"] = str(args.relay_port).strip()
    if args.relay_8_port and isinstance(devices_cfg.get("relay_8"), dict):
        devices_cfg["relay_8"]["port"] = str(args.relay_8_port).strip()

    pressure_cfg = runtime_cfg["workflow"].setdefault("pressure", {})
    pressure_cfg["vent_time_s"] = 0.0
    pressure_cfg["atmosphere_gate_monitor_s"] = float(args.atmosphere_monitor_s)
    pressure_cfg["atmosphere_gate_poll_s"] = float(args.atmosphere_poll_s)
    pressure_cfg["atmosphere_gate_min_samples"] = int(args.atmosphere_min_samples)
    pressure_cfg["atmosphere_gate_pressure_tolerance_hpa"] = float(args.atmosphere_tolerance_hpa)
    pressure_cfg["atmosphere_gate_pressure_rising_slope_max_hpa_s"] = float(args.atmosphere_rising_slope_max_hpa_s)
    pressure_cfg["atmosphere_gate_pressure_rising_min_delta_hpa"] = float(args.atmosphere_rising_min_delta_hpa)
    pressure_cfg["flush_guard_pressure_tolerance_hpa"] = float(args.flush_guard_tolerance_hpa)
    pressure_cfg["flush_guard_pressure_rising_slope_max_hpa_s"] = float(args.flush_guard_rising_slope_max_hpa_s)
    pressure_cfg["flush_guard_pressure_rising_min_delta_hpa"] = float(args.flush_guard_rising_min_delta_hpa)
    pressure_cfg["route_open_guard_enabled"] = True
    pressure_cfg["route_open_guard_monitor_s"] = float(args.route_open_guard_monitor_s)
    pressure_cfg["route_open_guard_poll_s"] = float(args.route_open_guard_poll_s)
    pressure_cfg["route_open_guard_pressure_tolerance_hpa"] = float(args.route_open_guard_tolerance_hpa)
    pressure_cfg["route_open_guard_pressure_rising_slope_max_hpa_s"] = float(
        args.route_open_guard_rising_slope_max_hpa_s
    )
    pressure_cfg["route_open_guard_pressure_rising_min_delta_hpa"] = float(
        args.route_open_guard_rising_min_delta_hpa
    )
    pressure_cfg["route_open_guard_analyzer_warning_kpa"] = float(args.route_open_guard_analyzer_warning_kpa)
    pressure_cfg["route_open_guard_analyzer_abort_kpa"] = float(args.route_open_guard_analyzer_abort_kpa)
    pressure_cfg["route_open_guard_dewpoint_line_tolerance_hpa"] = float(
        args.route_open_guard_dewpoint_line_tolerance_hpa
    )

    stability_cfg = runtime_cfg["workflow"].setdefault("stability", {})
    stability_cfg["gas_route_dewpoint_gate_enabled"] = True
    stability_cfg["gas_route_dewpoint_gate_policy"] = "reject"
    stability_cfg["gas_route_dewpoint_gate_window_s"] = float(args.dewpoint_gate_window_s)
    stability_cfg["gas_route_dewpoint_gate_max_total_wait_s"] = float(args.dewpoint_gate_max_wait_s)
    stability_cfg["gas_route_dewpoint_gate_poll_s"] = float(args.dewpoint_gate_poll_s)
    stability_cfg["gas_route_dewpoint_gate_log_interval_s"] = float(args.dewpoint_gate_log_interval_s)
    return runtime_cfg


def _build_co2_point(
    args: argparse.Namespace,
    *,
    index: int,
    co2_ppm: Optional[float] = None,
    co2_group: Optional[str] = None,
) -> CalibrationPoint:
    point = CalibrationPoint(
        index=index,
        temp_chamber_c=20.0,
        co2_ppm=float(co2_ppm if co2_ppm is not None else args.co2_ppm),
        hgen_temp_c=None,
        hgen_rh_pct=None,
        target_pressure_hpa=float(args.target_pressure_hpa),
        dewpoint_c=None,
        h2o_mmol=None,
        raw_h2o=None,
        co2_group=co2_group,
    )
    return point


def _build_h2o_point(args: argparse.Namespace, *, index: int) -> CalibrationPoint:
    return CalibrationPoint(
        index=index,
        temp_chamber_c=20.0,
        co2_ppm=None,
        hgen_temp_c=20.0,
        hgen_rh_pct=30.0,
        target_pressure_hpa=float(args.target_pressure_hpa),
        dewpoint_c=None,
        h2o_mmol=None,
        raw_h2o=None,
    )


def _build_point(args: argparse.Namespace, *, index: int) -> CalibrationPoint:
    return _build_co2_point(args, index=index)


def _scenario_trace_rows(trace_path: Path, start_row_count: int) -> List[Dict[str, Any]]:
    rows = _read_csv_rows(trace_path)
    return rows[start_row_count:]


def _relay_state_snapshot(devices: Mapping[str, Any]) -> Dict[str, Any]:
    snapshot: Dict[str, Any] = {}
    for relay_name, count in (("relay", 16), ("relay_8", 8)):
        relay = devices.get(relay_name)
        read_coils = getattr(relay, "read_coils", None) if relay is not None else None
        if not callable(read_coils):
            continue
        try:
            bits = list(read_coils(0, count)[:count])
        except Exception as exc:
            snapshot[relay_name] = {"error": str(exc)}
        else:
            snapshot[relay_name] = [bool(value) for value in bits]
    return snapshot


def _drain_pace_errors_for_live_step(runner: CalibrationRunner, *, reason: str) -> Dict[str, Any]:
    drained_errors = list(runner._drain_pace_system_errors(reason=reason) or [])
    post_drain_error = str(runner._read_pace_system_error_text() or "").strip()
    return {
        "pre_existing_error_drained": bool(drained_errors),
        "drained_errors": drained_errors,
        "post_drain_error": post_drain_error,
    }


def _status_from_abort(ok: bool, abort_reason: str) -> str:
    if ok:
        return "pass"
    if abort_reason:
        return "aborted"
    return "fail"


def _pace_error_is_clear(text: Any) -> bool:
    raw = str(text or "").strip()
    if not raw:
        return True
    return raw.startswith("0,") or raw.startswith(':SYST:ERR 0') or raw.startswith(':SYST:ERR 0,')


def _route_guard_summary_payload(runner: CalibrationRunner) -> Dict[str, Any]:
    summary = dict(runner._last_route_pressure_guard_summary or {})
    summary.setdefault("analyzer_pressure_available", False)
    summary.setdefault("analyzer_pressure_protection_active", False)
    summary.setdefault("analyzer_pressure_status", "unavailable")
    if hasattr(runner, "_pace_error_attribution_counts"):
        summary.update(dict(runner._pace_error_attribution_counts() or {}))
    return summary


def _runner_source_stage_safety(runner: CalibrationRunner) -> Dict[str, Any]:
    if hasattr(runner, "_source_stage_safety_snapshot"):
        return dict(runner._source_stage_safety_snapshot() or {})
    return {}


def _enrich_live_result_with_pace_diagnostics(
    runner: CalibrationRunner,
    result: Dict[str, Any],
    *,
    final_syst_err: str = "",
) -> Dict[str, Any]:
    enriched = dict(result or {})
    if hasattr(runner, "_pace_error_attribution_counts"):
        enriched.update(dict(runner._pace_error_attribution_counts() or {}))
    else:
        enriched.setdefault("pace_error_attribution_count", 0)
        enriched.setdefault("optional_probe_error_count", 0)
        enriched.setdefault("hidden_syst_err_count", 0)
        enriched.setdefault("unclassified_syst_err_count", 0)
    if hasattr(runner, "_pace_error_attribution_log_snapshot"):
        enriched["pace_error_attribution_log"] = list(runner._pace_error_attribution_log_snapshot() or [])
    else:
        enriched.setdefault("pace_error_attribution_log", [])
    enriched["source_stage_safety"] = _runner_source_stage_safety(runner)
    effective_final_syst_err = str(final_syst_err or runner._read_pace_system_error_text() or "").strip()
    enriched["final_syst_err"] = effective_final_syst_err
    if (
        str(enriched.get("status") or "") == "pass"
        and (
            int(enriched.get("hidden_syst_err_count") or 0) > 0
            or int(enriched.get("unclassified_syst_err_count") or 0) > 0
            or not _pace_error_is_clear(effective_final_syst_err)
        )
    ):
        enriched["status"] = (
            "diagnostic_error"
            if int(enriched.get("unclassified_syst_err_count") or 0) > 0
            else "pass_with_diagnostic_error"
        )
        if int(enriched.get("unclassified_syst_err_count") or 0) > 0 and not str(enriched.get("abort_reason") or "").strip():
            enriched["abort_reason"] = "UnclassifiedPaceSystErrDuringRouteStage"
    return enriched


def _run_atmosphere_gate_only(
    runner: CalibrationRunner,
    trace_path: Path,
) -> Dict[str, Any]:
    trace_start = _trace_row_count(trace_path)
    runner._set_co2_route_baseline(reason="live AtmosphereGate-only validation")
    summary = dict(runner._last_atmosphere_gate_summary or {})
    return {
        "scenario": "atmosphere_gate_only",
        "status": "pass" if bool(summary.get("atmosphere_ready")) else "fail",
        "atmosphere_summary": summary,
        "pressure_trace_rows": _scenario_trace_rows(trace_path, trace_start),
    }


def _run_baseline_atmosphere_hold_60s(
    runner: CalibrationRunner,
    trace_path: Path,
    devices: Mapping[str, Any],
    args: argparse.Namespace,
) -> Dict[str, Any]:
    trace_start = _trace_row_count(trace_path)
    drain_summary = _drain_pace_errors_for_live_step(runner, reason="baseline_atmosphere_hold_60s pre-step")
    runner._set_co2_route_baseline(reason="baseline atmosphere hold 60s")
    baseline_summary = dict(runner._last_atmosphere_gate_summary or {})
    pressure_rows: List[tuple[float, float]] = []
    first_sample: Optional[float] = None
    last_sample: Optional[float] = None
    deadline = time.monotonic() + float(args.baseline_hold_monitor_s)
    while True:
        sample = runner._read_current_pressure_hpa_for_atmosphere()
        pressure_hpa = runner._as_float(sample.get("pressure_hpa"))
        now_monotonic = time.monotonic()
        if pressure_hpa is not None:
            pressure_rows.append((now_monotonic, float(pressure_hpa)))
            if first_sample is None:
                first_sample = float(pressure_hpa)
            last_sample = float(pressure_hpa)
        runner._append_pressure_trace_row(
            point=None,
            route="pressure",
            trace_stage="baseline_atmosphere_hold_sample",
            pressure_gauge_hpa=runner._as_float(sample.get("pressure_gauge_hpa")),
            pace_pressure_hpa=runner._as_float(sample.get("pace_pressure_hpa")),
            refresh_pace_state=False,
            note=(
                f"scenario=baseline_atmosphere_hold_60s valve_route_state={runner._current_valve_route_state_text()} "
                f"relay_states={json.dumps(_relay_state_snapshot(devices), ensure_ascii=False)}"
            ),
        )
        if now_monotonic >= deadline:
            break
        time.sleep(max(0.1, float(args.baseline_hold_poll_s)))
    metrics = runner._numeric_series_metrics(pressure_rows)
    pressure_first = runner._as_float(metrics.get("first_value"))
    pressure_last = runner._as_float(metrics.get("last_value"))
    pressure_rise = None if pressure_first is None or pressure_last is None else float(pressure_last) - float(pressure_first)
    pressure_delta = None
    ambient_hpa = runner._as_float(baseline_summary.get("ambient_hpa"))
    if pressure_last is not None and ambient_hpa is not None:
        pressure_delta = float(pressure_last) - float(ambient_hpa)
    result = {
        "scenario": "baseline_atmosphere_hold_60s",
        "status": "pass"
        if bool(baseline_summary.get("atmosphere_ready")) and (pressure_delta is None or abs(float(pressure_delta)) <= 15.0)
        else "fail",
        "atmosphere_summary": baseline_summary,
        "pressure_first_hpa": pressure_first,
        "pressure_last_hpa": pressure_last,
        "pressure_delta_from_ambient_hpa": pressure_delta,
        "pressure_rise_hpa": pressure_rise,
        "pressure_slope_hpa_s": runner._as_float(metrics.get("slope_per_s")),
        "sample_count": int(metrics.get("count") or len(pressure_rows)),
        "valve_route_state": runner._current_valve_route_state_text(),
        "relay_states": _relay_state_snapshot(devices),
        **drain_summary,
        "pressure_trace_rows": _scenario_trace_rows(trace_path, trace_start),
    }
    return _enrich_live_result_with_pace_diagnostics(runner, result)


def _run_route_flush_dewpoint_gate(
    runner: CalibrationRunner,
    trace_path: Path,
    args: argparse.Namespace,
) -> Dict[str, Any]:
    point = _build_point(args, index=9001)
    trace_start = _trace_row_count(trace_path)
    route_open_ok = runner._open_co2_route_for_conditioning(point, point_tag="live_route_flush_dewpoint_gate")
    if route_open_ok:
        ok = runner._wait_co2_route_dewpoint_gate_before_seal(
            point,
            base_soak_s=float(args.route_flush_soak_s),
            log_context="minimal live route flush + dewpoint gate",
        )
    else:
        ok = False
    point_state = dict(runner._point_runtime_state(point, phase="co2") or {})
    abort_reason = str(point_state.get("abort_reason") or "").strip()
    return {
        "scenario": "route_flush_dewpoint_gate",
        "status": _status_from_abort(bool(ok), abort_reason),
        "gate_passed": bool(ok),
        "route_open_passed": bool(route_open_ok),
        "abort_reason": abort_reason,
        "point_runtime_state": point_state,
        "atmosphere_summary": dict(runner._last_atmosphere_gate_summary or {}),
        "route_pressure_guard_summary": dict(runner._last_route_pressure_guard_summary or {}),
        "pressure_trace_rows": _scenario_trace_rows(trace_path, trace_start),
    }


def _run_route_synchronized_atmosphere_flush_co2_a_no_source(
    runner: CalibrationRunner,
    trace_path: Path,
    args: argparse.Namespace,
) -> Dict[str, Any]:
    point = _build_co2_point(args, index=9010, co2_ppm=600.0, co2_group="A")
    trace_start = _trace_row_count(trace_path)
    drain_summary = _drain_pace_errors_for_live_step(runner, reason="route_synchronized_atmosphere_flush_co2_a_no_source pre-step")
    runner._clear_last_sealed_pressure_route_context(reason="live synchronized CO2 A no-source route flush")
    runner._clear_pressure_sequence_context(reason="live synchronized CO2 A no-source route flush")
    runner._set_co2_route_baseline(reason="live synchronized CO2 A no-source route flush baseline")
    open_valves = runner._co2_open_valves(point, include_total_valve=True, include_source_valve=False)
    ok = runner._open_route_with_pressure_guard(
        point,
        phase="co2",
        point_tag="live_route_sync_atmosphere_flush_co2_a_no_source",
        open_valves=open_valves,
        log_context="live synchronized CO2 A no-source route flush",
    )
    point_state = dict(runner._point_runtime_state(point, phase="co2") or {})
    abort_reason = str(point_state.get("abort_reason") or "").strip()
    return _enrich_live_result_with_pace_diagnostics(runner, {
        "scenario": "route_synchronized_atmosphere_flush_co2_a_no_source",
        "status": _status_from_abort(bool(ok), abort_reason),
        "route_open_passed": bool(ok),
        "abort_reason": abort_reason,
        "point_runtime_state": point_state,
        "open_valves": open_valves,
        "atmosphere_summary": dict(runner._last_atmosphere_gate_summary or {}),
        "route_pressure_guard_summary": _route_guard_summary_payload(runner),
        **drain_summary,
        "pressure_trace_rows": _scenario_trace_rows(trace_path, trace_start),
    })


def _run_route_synchronized_atmosphere_flush_co2_b_no_source(
    runner: CalibrationRunner,
    trace_path: Path,
    args: argparse.Namespace,
) -> Dict[str, Any]:
    point = _build_co2_point(args, index=9011, co2_ppm=500.0, co2_group="B")
    trace_start = _trace_row_count(trace_path)
    drain_summary = _drain_pace_errors_for_live_step(runner, reason="route_synchronized_atmosphere_flush_co2_b_no_source pre-step")
    runner._clear_last_sealed_pressure_route_context(reason="live synchronized CO2 B no-source route flush")
    runner._clear_pressure_sequence_context(reason="live synchronized CO2 B no-source route flush")
    runner._set_co2_route_baseline(reason="live synchronized CO2 B no-source route flush baseline")
    open_valves = runner._co2_open_valves(point, include_total_valve=True, include_source_valve=False)
    ok = runner._open_route_with_pressure_guard(
        point,
        phase="co2",
        point_tag="live_route_sync_atmosphere_flush_co2_b_no_source",
        open_valves=open_valves,
        log_context="live synchronized CO2 B no-source route flush",
    )
    point_state = dict(runner._point_runtime_state(point, phase="co2") or {})
    abort_reason = str(point_state.get("abort_reason") or "").strip()
    return _enrich_live_result_with_pace_diagnostics(runner, {
        "scenario": "route_synchronized_atmosphere_flush_co2_b_no_source",
        "status": _status_from_abort(bool(ok), abort_reason),
        "route_open_passed": bool(ok),
        "abort_reason": abort_reason,
        "point_runtime_state": point_state,
        "open_valves": open_valves,
        "atmosphere_summary": dict(runner._last_atmosphere_gate_summary or {}),
        "route_pressure_guard_summary": _route_guard_summary_payload(runner),
        **drain_summary,
        "pressure_trace_rows": _scenario_trace_rows(trace_path, trace_start),
    })


def _run_route_synchronized_atmosphere_flush_co2_a_source_guarded(
    runner: CalibrationRunner,
    trace_path: Path,
    args: argparse.Namespace,
) -> Dict[str, Any]:
    point = _build_co2_point(args, index=9013, co2_ppm=600.0, co2_group="A")
    trace_start = _trace_row_count(trace_path)
    open_valves = runner._co2_open_valves(point, include_total_valve=True, include_source_valve=True)
    if not bool(getattr(args, "allow_source_open", False)):
        return _enrich_live_result_with_pace_diagnostics(runner, {
            "scenario": "route_synchronized_atmosphere_flush_co2_a_source_guarded",
            "status": "skipped",
            "skipped_reason": "SourceOpenRequiresExplicitAllowFlag",
            "operator_must_confirm_upstream_source_pressure_limited": True,
            "open_valves": open_valves,
            "pressure_trace_rows": _scenario_trace_rows(trace_path, trace_start),
        })
    _log("operator_must_confirm_upstream_source_pressure_limited=true")
    drain_summary = _drain_pace_errors_for_live_step(runner, reason="route_synchronized_atmosphere_flush_co2_a_source_guarded pre-step")
    ok = runner._open_co2_route_for_conditioning(point, point_tag="live_route_sync_atmosphere_flush_co2_a_source_guarded")
    point_state = dict(runner._point_runtime_state(point, phase="co2") or {})
    abort_reason = str(point_state.get("abort_reason") or "").strip()
    return _enrich_live_result_with_pace_diagnostics(runner, {
        "scenario": "route_synchronized_atmosphere_flush_co2_a_source_guarded",
        "status": _status_from_abort(bool(ok), abort_reason),
        "route_open_passed": bool(ok),
        "abort_reason": abort_reason,
        "operator_must_confirm_upstream_source_pressure_limited": True,
        "point_runtime_state": point_state,
        "open_valves": open_valves,
        "atmosphere_summary": dict(runner._last_atmosphere_gate_summary or {}),
        "route_pressure_guard_summary": _route_guard_summary_payload(runner),
        **drain_summary,
        "pressure_trace_rows": _scenario_trace_rows(trace_path, trace_start),
    })


def _run_route_synchronized_atmosphere_flush_co2_b_source_guarded(
    runner: CalibrationRunner,
    trace_path: Path,
    args: argparse.Namespace,
) -> Dict[str, Any]:
    point = _build_co2_point(args, index=9014, co2_ppm=500.0, co2_group="B")
    trace_start = _trace_row_count(trace_path)
    open_valves = runner._co2_open_valves(point, include_total_valve=True, include_source_valve=True)
    if not bool(getattr(args, "allow_source_open", False)):
        return _enrich_live_result_with_pace_diagnostics(runner, {
            "scenario": "route_synchronized_atmosphere_flush_co2_b_source_guarded",
            "status": "skipped",
            "skipped_reason": "SourceOpenRequiresExplicitAllowFlag",
            "operator_must_confirm_upstream_source_pressure_limited": True,
            "open_valves": open_valves,
            "pressure_trace_rows": _scenario_trace_rows(trace_path, trace_start),
        })
    _log("operator_must_confirm_upstream_source_pressure_limited=true")
    drain_summary = _drain_pace_errors_for_live_step(runner, reason="route_synchronized_atmosphere_flush_co2_b_source_guarded pre-step")
    ok = runner._open_co2_route_for_conditioning(point, point_tag="live_route_sync_atmosphere_flush_co2_b_source_guarded")
    point_state = dict(runner._point_runtime_state(point, phase="co2") or {})
    abort_reason = str(point_state.get("abort_reason") or "").strip()
    return _enrich_live_result_with_pace_diagnostics(runner, {
        "scenario": "route_synchronized_atmosphere_flush_co2_b_source_guarded",
        "status": _status_from_abort(bool(ok), abort_reason),
        "route_open_passed": bool(ok),
        "abort_reason": abort_reason,
        "operator_must_confirm_upstream_source_pressure_limited": True,
        "point_runtime_state": point_state,
        "open_valves": open_valves,
        "atmosphere_summary": dict(runner._last_atmosphere_gate_summary or {}),
        "route_pressure_guard_summary": _route_guard_summary_payload(runner),
        **drain_summary,
        "pressure_trace_rows": _scenario_trace_rows(trace_path, trace_start),
    })


def _run_route_synchronized_atmosphere_flush_h2o_no_final(
    runner: CalibrationRunner,
    trace_path: Path,
    args: argparse.Namespace,
) -> Dict[str, Any]:
    point = _build_h2o_point(args, index=9015)
    trace_start = _trace_row_count(trace_path)
    drain_summary = _drain_pace_errors_for_live_step(runner, reason="route_synchronized_atmosphere_flush_h2o_no_final pre-step")
    runner._clear_last_sealed_pressure_route_context(reason="live synchronized H2O no-final route flush")
    runner._clear_pressure_sequence_context(reason="live synchronized H2O no-final route flush")
    runner._apply_route_baseline_valves()
    runner._set_pressure_controller_vent(True, reason="live synchronized H2O no-final route flush baseline")
    open_valves = runner._h2o_open_valves(point, include_final_stage=False)
    ok = runner._open_route_with_pressure_guard(
        point,
        phase="h2o",
        point_tag="live_route_sync_atmosphere_flush_h2o_no_final",
        open_valves=open_valves,
        log_context="live synchronized H2O no-final route flush",
    )
    point_state = dict(runner._point_runtime_state(point, phase="h2o") or {})
    abort_reason = str(point_state.get("abort_reason") or "").strip()
    return _enrich_live_result_with_pace_diagnostics(runner, {
        "scenario": "route_synchronized_atmosphere_flush_h2o_no_final",
        "status": _status_from_abort(bool(ok), abort_reason),
        "route_open_passed": bool(ok),
        "abort_reason": abort_reason,
        "point_runtime_state": point_state,
        "open_valves": open_valves,
        "skipped_final_stage": 10,
        "skipped_reason": "H2OFinalStage10RequiresExplicitAllowFlag",
        "atmosphere_summary": dict(runner._last_atmosphere_gate_summary or {}),
        "route_pressure_guard_summary": _route_guard_summary_payload(runner),
        **drain_summary,
        "pressure_trace_rows": _scenario_trace_rows(trace_path, trace_start),
    })


def _run_route_synchronized_atmosphere_flush_h2o(
    runner: CalibrationRunner,
    trace_path: Path,
    args: argparse.Namespace,
) -> Dict[str, Any]:
    point = _build_h2o_point(args, index=9012)
    trace_start = _trace_row_count(trace_path)
    open_valves = runner._h2o_open_valves(point)
    if not bool(getattr(args, "allow_h2o_final_stage_open", False)):
        return _enrich_live_result_with_pace_diagnostics(runner, {
            "scenario": "route_synchronized_atmosphere_flush_h2o",
            "status": "skipped",
            "skipped_reason": "H2OFinalStage10RequiresExplicitAllowFlag",
            "operator_must_confirm_h2o_upstream_pressure_limited": True,
            "open_valves": open_valves,
            "pressure_trace_rows": _scenario_trace_rows(trace_path, trace_start),
        })
    drain_summary = _drain_pace_errors_for_live_step(runner, reason="route_synchronized_atmosphere_flush_h2o pre-step")
    runner._clear_last_sealed_pressure_route_context(reason="live synchronized H2O route flush")
    runner._clear_pressure_sequence_context(reason="live synchronized H2O route flush")
    runner._apply_route_baseline_valves()
    runner._set_pressure_controller_vent(True, reason="live synchronized H2O route flush baseline")
    ok = runner._open_route_with_pressure_guard(
        point,
        phase="h2o",
        point_tag="live_route_sync_atmosphere_flush_h2o",
        open_valves=open_valves,
        log_context="live synchronized H2O route flush",
    )
    point_state = dict(runner._point_runtime_state(point, phase="h2o") or {})
    abort_reason = str(point_state.get("abort_reason") or "").strip()
    return _enrich_live_result_with_pace_diagnostics(runner, {
        "scenario": "route_synchronized_atmosphere_flush_h2o",
        "status": _status_from_abort(bool(ok), abort_reason),
        "route_open_passed": bool(ok),
        "abort_reason": abort_reason,
        "operator_must_confirm_h2o_upstream_pressure_limited": True,
        "point_runtime_state": point_state,
        "open_valves": open_valves,
        "atmosphere_summary": dict(runner._last_atmosphere_gate_summary or {}),
        "route_pressure_guard_summary": _route_guard_summary_payload(runner),
        **drain_summary,
        "pressure_trace_rows": _scenario_trace_rows(trace_path, trace_start),
    })


def _run_route_open_pressure_guard(
    runner: CalibrationRunner,
    trace_path: Path,
    args: argparse.Namespace,
) -> Dict[str, Any]:
    point = _build_point(args, index=9002)
    trace_start = _trace_row_count(trace_path)
    ok = runner._open_co2_route_for_conditioning(point, point_tag="live_route_open_pressure_guard")
    point_state = dict(runner._point_runtime_state(point, phase="co2") or {})
    abort_reason = str(point_state.get("abort_reason") or "").strip()
    return _enrich_live_result_with_pace_diagnostics(runner, {
        "scenario": "route_open_pressure_guard",
        "status": _status_from_abort(bool(ok), abort_reason),
        "route_open_passed": bool(ok),
        "abort_reason": abort_reason,
        "point_runtime_state": point_state,
        "atmosphere_summary": dict(runner._last_atmosphere_gate_summary or {}),
        "route_pressure_guard_summary": _route_guard_summary_payload(runner),
        "pressure_trace_rows": _scenario_trace_rows(trace_path, trace_start),
    })


def _run_pace_optional_query_error_attribution(
    runner: CalibrationRunner,
    trace_path: Path,
) -> Dict[str, Any]:
    trace_start = _trace_row_count(trace_path)
    query_rows = runner._pace_optional_query_error_attribution(
        (
            ":STAT:OPER:PRES:EVEN?",
            ":SOUR:PRES:COMP1?",
            ":SOUR:PRES:COMP2?",
            ":SENS:PRES:SLEW?",
            ":SENS:PRES:BAR?",
            ":SENS:PRES:INL:TIME?",
        ),
        reason="pace optional query attribution",
    )
    return _enrich_live_result_with_pace_diagnostics(runner, {
        "scenario": "pace_optional_query_error_attribution",
        "status": "pass",
        "query_results": query_rows,
        "pressure_trace_rows": _scenario_trace_rows(trace_path, trace_start),
    })


def _valve_set_label(valves: Iterable[int]) -> str:
    ordered = [int(value) for value in valves]
    return "|".join(str(value) for value in ordered)


def _dedupe_valve_sets(raw_sets: Iterable[Iterable[int]]) -> List[List[int]]:
    seen: set[tuple[int, ...]] = set()
    deduped: List[List[int]] = []
    for raw_set in raw_sets:
        normalized = tuple(int(value) for value in raw_set)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        deduped.append(list(normalized))
    return deduped


def _route_valve_isolation_ladders(runner: CalibrationRunner, point: CalibrationPoint) -> List[Dict[str, Any]]:
    latest_group = [4, 7, 8, 11]
    legacy_group = [8, 11, 16, 24]
    config_co2_group = runner._co2_open_valves(point, include_total_valve=True)
    config_h2o_group = runner._h2o_open_valves(point)
    ladders: List[Dict[str, Any]] = [
        {
            "name": "latest_group",
            "steps": [[4], [4, 7], [4, 7, 8], [4, 7, 8, 11]],
        },
        {
            "name": "legacy_group",
            "steps": [[8], [8, 11], [8, 11, 16], [8, 11, 16, 24]],
        },
    ]
    if config_co2_group:
        ladders.append(
            {
                "name": "config_co2_route",
                "steps": [config_co2_group[: idx] for idx in range(1, len(config_co2_group) + 1)],
            }
        )
    if config_h2o_group:
        ladders.append(
            {
                "name": "config_h2o_route",
                "steps": [config_h2o_group[: idx] for idx in range(1, len(config_h2o_group) + 1)],
            }
        )
    return ladders


def _run_route_valve_isolation_step(
    runner: CalibrationRunner,
    *,
    point: CalibrationPoint,
    valve_set: List[int],
    step_name: str,
) -> Dict[str, Any]:
    drain_summary = _drain_pace_errors_for_live_step(runner, reason=f"route_valve_isolation {step_name} pre-step")
    runner._set_co2_route_baseline(reason=f"route valve isolation before {step_name}")
    atmosphere_summary = dict(runner._last_atmosphere_gate_summary or {})
    runner._apply_valve_states(list(valve_set))
    ok, guard_summary = runner._run_route_open_pressure_guard(
        point,
        phase="co2",
        log_context="route valve isolation",
        point_tag="route_valve_isolation",
        stage_label=_valve_set_label(valve_set),
    )
    result_summary = dict(guard_summary or {})
    post_step_error = str(runner._read_pace_system_error_text() or "").strip()
    post_step_error_clear = runner._pace_system_error_is_clear(post_step_error)
    valve_ids = [int(value) for value in valve_set]
    if ok and not post_step_error_clear:
        result_label = "diagnostic_error"
    elif ok:
        result_label = "safe"
    else:
        result_label = "pressure_rise" if result_summary.get("abort_reason") else "unknown"
    result = {
        "step_name": step_name,
        "valve_set": valve_ids,
        "valve_roles": runner.valve_role_map_for_ids(valve_ids),
        "pressure_start_hpa": result_summary.get("pressure_start_hpa", atmosphere_summary.get("pressure_hpa")),
        "pressure_end_hpa": result_summary.get("pressure_end_hpa", result_summary.get("pressure_hpa")),
        "pressure_peak_hpa": result_summary.get("pressure_peak_hpa", result_summary.get("pressure_hpa")),
        "pressure_delta_from_ambient_hpa": result_summary.get("pressure_delta_from_ambient_hpa"),
        "pressure_slope_hpa_s": result_summary.get("pressure_slope_hpa_s"),
        "ambient_hpa": result_summary.get("ambient_hpa", atmosphere_summary.get("ambient_hpa")),
        "analyzer_p_kpa": result_summary.get("analyzer_pressure_kpa"),
        "dewpoint_line_pressure_hpa": result_summary.get("dewpoint_line_pressure_hpa"),
        "abort_reason": result_summary.get("abort_reason", ""),
        "syst_err": post_step_error if post_step_error else result_summary.get("pace_syst_err_query", ""),
        "offending_route": result_summary.get("offending_route", ""),
        "offending_valve_or_group": result_summary.get("offending_valve_or_group", ""),
        "pre_existing_error_drained": drain_summary.get("pre_existing_error_drained", False),
        "drained_errors": drain_summary.get("drained_errors", []),
        "result": result_label,
    }
    runner._set_co2_route_baseline(reason=f"route valve isolation after {step_name}")
    return result


def _run_route_valve_isolation(
    runner: CalibrationRunner,
    trace_path: Path,
    args: argparse.Namespace,
) -> Dict[str, Any]:
    point = _build_point(args, index=9003)
    trace_start = _trace_row_count(trace_path)
    ladders = _route_valve_isolation_ladders(runner, point)
    single_valves = _dedupe_valve_sets(
        [[valve] for ladder in ladders for step in ladder["steps"] for valve in step]
    )
    step_results: List[Dict[str, Any]] = []
    for valve_set in single_valves:
        step_results.append(
            _run_route_valve_isolation_step(
                runner,
                point=point,
                valve_set=list(valve_set),
                step_name=f"single_{_valve_set_label(valve_set)}",
            )
        )

    blocked_single_valves = {
        int(result["valve_set"][0])
        for result in step_results
        if result.get("result") != "safe" and len(result.get("valve_set", [])) == 1
    }
    ladder_results: List[Dict[str, Any]] = []
    offending_groups: List[str] = []
    for ladder in ladders:
        ladder_step_results: List[Dict[str, Any]] = []
        for step in _dedupe_valve_sets(ladder["steps"]):
            if any(int(valve) in blocked_single_valves for valve in step) and len(step) > 1:
                ladder_step_results.append(
                    {
                        "step_name": f"{ladder['name']}_{_valve_set_label(step)}",
                        "valve_set": [int(value) for value in step],
                        "valve_roles": runner.valve_role_map_for_ids([int(value) for value in step]),
                        "pressure_start_hpa": None,
                        "pressure_end_hpa": None,
                        "pressure_peak_hpa": None,
                        "pressure_delta_from_ambient_hpa": None,
                        "pressure_slope_hpa_s": None,
                        "ambient_hpa": None,
                        "analyzer_p_kpa": None,
                        "dewpoint_line_pressure_hpa": None,
                        "abort_reason": "SkippedAfterSingleValveFailure",
                        "syst_err": "",
                        "offending_route": "",
                        "offending_valve_or_group": _valve_set_label(step),
                        "result": "skipped",
                    }
                )
                break
            result = _run_route_valve_isolation_step(
                runner,
                point=point,
                valve_set=list(step),
                step_name=f"{ladder['name']}_{_valve_set_label(step)}",
            )
            ladder_step_results.append(result)
            if result["result"] != "safe":
                offending_groups.append(str(result.get("offending_valve_or_group") or _valve_set_label(step)))
                break
        ladder_results.append({"name": ladder["name"], "steps": ladder_step_results})

    all_results = step_results + [item for ladder in ladder_results for item in ladder["steps"]]
    dangerous_groups = [
        item for item in all_results if item.get("result") in {"pressure_rise", "unknown", "diagnostic_error"}
    ]
    status = "pass" if not dangerous_groups else "aborted"
    return {
        "scenario": "route_valve_isolation",
        "status": status,
        "abort_reason": dangerous_groups[0].get("abort_reason", "") if dangerous_groups else "",
        "valve_role_map": runner.valve_role_map_for_ids([4, 7, 8, 11, 16, 24]),
        "single_steps": step_results,
        "ladder_results": ladder_results,
        "dangerous_groups": dangerous_groups,
        "offending_groups": offending_groups,
        "pressure_trace_rows": _scenario_trace_rows(trace_path, trace_start),
    }


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(dict(payload), ensure_ascii=False, indent=2, default=str) + "\n",
        encoding="utf-8",
    )


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Minimal V1 live validation for AtmosphereGate and RouteFlush gates.")
    parser.add_argument("--config", default="configs/default_config.json")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--run-id", default=None)
    parser.add_argument(
        "--scenario",
        choices=(
            "atmosphere_gate_only",
            "baseline_atmosphere_hold_60s",
            "pace_optional_query_error_attribution",
            "route_open_pressure_guard",
            "route_synchronized_atmosphere_flush_co2_a_no_source",
            "route_synchronized_atmosphere_flush_co2_b_no_source",
            "route_synchronized_atmosphere_flush_co2_a_source_guarded",
            "route_synchronized_atmosphere_flush_co2_b_source_guarded",
            "route_synchronized_atmosphere_flush_co2_a",
            "route_synchronized_atmosphere_flush_co2_b",
            "route_synchronized_atmosphere_flush_h2o_no_final",
            "route_synchronized_atmosphere_flush_h2o",
            "route_flush_dewpoint_gate",
            "route_valve_isolation",
        ),
        required=True,
        help="Minimal live validation scenario to execute.",
    )
    parser.add_argument(
        "--real-device",
        action="store_true",
        help="Required to enable real-device COM access for this minimal V1 validation tool.",
    )
    parser.add_argument("--co2-ppm", type=float, default=600.0)
    parser.add_argument("--target-pressure-hpa", type=float, default=1000.0)
    parser.add_argument("--relay-port", default=None)
    parser.add_argument("--relay-8-port", default=None)
    parser.add_argument("--route-flush-soak-s", type=float, default=0.0)
    parser.add_argument("--baseline-hold-monitor-s", type=float, default=60.0)
    parser.add_argument("--baseline-hold-poll-s", type=float, default=1.0)
    parser.add_argument("--atmosphere-monitor-s", type=float, default=4.0)
    parser.add_argument("--atmosphere-poll-s", type=float, default=0.5)
    parser.add_argument("--atmosphere-min-samples", type=int, default=6)
    parser.add_argument("--atmosphere-tolerance-hpa", type=float, default=15.0)
    parser.add_argument("--atmosphere-rising-slope-max-hpa-s", type=float, default=0.05)
    parser.add_argument("--atmosphere-rising-min-delta-hpa", type=float, default=0.5)
    parser.add_argument("--flush-guard-tolerance-hpa", type=float, default=15.0)
    parser.add_argument("--flush-guard-rising-slope-max-hpa-s", type=float, default=0.05)
    parser.add_argument("--flush-guard-rising-min-delta-hpa", type=float, default=0.5)
    parser.add_argument("--route-open-guard-monitor-s", type=float, default=8.0)
    parser.add_argument("--route-open-guard-poll-s", type=float, default=1.0)
    parser.add_argument("--route-open-guard-tolerance-hpa", type=float, default=30.0)
    parser.add_argument("--route-open-guard-rising-slope-max-hpa-s", type=float, default=0.2)
    parser.add_argument("--route-open-guard-rising-min-delta-hpa", type=float, default=2.0)
    parser.add_argument("--route-open-guard-analyzer-warning-kpa", type=float, default=120.0)
    parser.add_argument("--route-open-guard-analyzer-abort-kpa", type=float, default=150.0)
    parser.add_argument("--route-open-guard-dewpoint-line-tolerance-hpa", type=float, default=30.0)
    parser.add_argument(
        "--allow-source-open",
        action="store_true",
        help="Required to execute guarded CO2 source-open scenarios.",
    )
    parser.add_argument(
        "--allow-h2o-final-stage-open",
        action="store_true",
        help="Required to execute H2O full-route final stage valve 10 live scenarios.",
    )
    parser.add_argument("--dewpoint-gate-window-s", type=float, default=30.0)
    parser.add_argument("--dewpoint-gate-max-wait-s", type=float, default=120.0)
    parser.add_argument("--dewpoint-gate-poll-s", type=float, default=1.0)
    parser.add_argument("--dewpoint-gate-log-interval-s", type=float, default=5.0)
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    if not args.real_device:
        _log("Dry-run only: pass --real-device to enable the minimal V1 live validation.")
        return 2

    cfg = load_config(args.config)
    need_dewpoint = args.scenario == "route_flush_dewpoint_gate"
    need_analyzer_pressure = args.scenario in {
        "route_open_pressure_guard",
        "route_synchronized_atmosphere_flush_co2_a_no_source",
        "route_synchronized_atmosphere_flush_co2_b_no_source",
        "route_synchronized_atmosphere_flush_co2_a_source_guarded",
        "route_synchronized_atmosphere_flush_co2_b_source_guarded",
        "route_synchronized_atmosphere_flush_co2_a",
        "route_synchronized_atmosphere_flush_co2_b",
        "route_synchronized_atmosphere_flush_h2o_no_final",
        "route_synchronized_atmosphere_flush_h2o",
        "route_flush_dewpoint_gate",
        "route_valve_isolation",
    }
    runtime_cfg = _prepare_runtime_cfg(
        cfg,
        args,
        need_dewpoint=need_dewpoint,
        need_analyzer_pressure=need_analyzer_pressure,
    )
    output_root = Path(runtime_cfg["paths"]["output_dir"]).resolve()
    run_id = args.run_id or f"{args.scenario}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    logger = RunLogger(output_root, run_id=run_id, cfg=runtime_cfg)
    devices: Dict[str, Any] = {}
    runner: Optional[CalibrationRunner] = None
    summary: Dict[str, Any] = {
        "tool": "run_v1_pressure_gate_live",
        "run_id": run_id,
        "scenario": args.scenario,
        "real_device": True,
        "validation_scope": "minimal_live_engineering_validation_only",
        "not_real_acceptance_evidence": True,
        "config_path": str(Path(args.config).resolve()),
        "output_dir": str(logger.run_dir),
        "io_csv": str(logger.io_path),
        "pressure_transition_trace_csv": str(logger.run_dir / "pressure_transition_trace.csv"),
        "scenario_result": {},
        "cleanup_safe_stop": {},
        "status": "running",
    }
    exit_code = 0
    trace_path = logger.run_dir / "pressure_transition_trace.csv"

    try:
        _log(f"Pressure gate live run dir: {logger.run_dir}")
        devices = _build_devices(runtime_cfg, io_logger=logger)
        runner = CalibrationRunner(runtime_cfg, devices, logger, _log, _log)
        runner._configure_devices(configure_gas_analyzers=False)
        summary["k0472_capability_snapshot"] = runner._capture_pace_capability_snapshot(
            reason=f"live scenario {args.scenario}",
            include_optional_probe=True,
        )

        if args.scenario == "atmosphere_gate_only":
            scenario_result = _run_atmosphere_gate_only(runner, trace_path)
        elif args.scenario == "baseline_atmosphere_hold_60s":
            scenario_result = _run_baseline_atmosphere_hold_60s(runner, trace_path, devices, args)
        elif args.scenario == "pace_optional_query_error_attribution":
            scenario_result = _run_pace_optional_query_error_attribution(runner, trace_path)
        elif args.scenario == "route_open_pressure_guard":
            scenario_result = _run_route_open_pressure_guard(runner, trace_path, args)
        elif args.scenario == "route_synchronized_atmosphere_flush_co2_a_no_source":
            scenario_result = _run_route_synchronized_atmosphere_flush_co2_a_no_source(runner, trace_path, args)
        elif args.scenario == "route_synchronized_atmosphere_flush_co2_b_no_source":
            scenario_result = _run_route_synchronized_atmosphere_flush_co2_b_no_source(runner, trace_path, args)
        elif args.scenario == "route_synchronized_atmosphere_flush_co2_a_source_guarded":
            scenario_result = _run_route_synchronized_atmosphere_flush_co2_a_source_guarded(runner, trace_path, args)
        elif args.scenario == "route_synchronized_atmosphere_flush_co2_b_source_guarded":
            scenario_result = _run_route_synchronized_atmosphere_flush_co2_b_source_guarded(runner, trace_path, args)
        elif args.scenario == "route_synchronized_atmosphere_flush_co2_a":
            scenario_result = _run_route_synchronized_atmosphere_flush_co2_a_source_guarded(runner, trace_path, args)
        elif args.scenario == "route_synchronized_atmosphere_flush_co2_b":
            scenario_result = _run_route_synchronized_atmosphere_flush_co2_b_source_guarded(runner, trace_path, args)
        elif args.scenario == "route_synchronized_atmosphere_flush_h2o_no_final":
            scenario_result = _run_route_synchronized_atmosphere_flush_h2o_no_final(runner, trace_path, args)
        elif args.scenario == "route_synchronized_atmosphere_flush_h2o":
            scenario_result = _run_route_synchronized_atmosphere_flush_h2o(runner, trace_path, args)
        elif args.scenario == "route_valve_isolation":
            scenario_result = _run_route_valve_isolation(runner, trace_path, args)
        else:
            scenario_result = _run_route_flush_dewpoint_gate(runner, trace_path, args)
        summary["scenario_result"] = scenario_result
        summary["status"] = "completed"
        _log(
            "Scenario result: "
            f"scenario={scenario_result.get('scenario')} "
            f"status={scenario_result.get('status')} "
            f"abort_reason={scenario_result.get('abort_reason', '')}"
        )
    except KeyboardInterrupt:
        summary["status"] = "cancelled"
        exit_code = 130
        _log("Pressure gate live validation cancelled by user.")
    except Exception as exc:
        summary["status"] = "error"
        summary["error"] = str(exc)
        exit_code = 1
        _log(f"Pressure gate live validation failed: {exc}")
    finally:
        if devices:
            try:
                cleanup_result = perform_safe_stop_with_retries(devices, log_fn=_log, cfg=runtime_cfg)
                summary["cleanup_safe_stop"] = dict(cleanup_result or {})
            except Exception as exc:
                summary["cleanup_safe_stop"] = {"error": str(exc)}
                _log(f"Cleanup safe-stop failed: {exc}")
        cleanup_syst_errs: List[str] = []
        if runner is not None:
            cleanup_syst_errs = list(
                runner._drain_pace_system_errors(
                    reason=f"live scenario {args.scenario} post-cleanup final drain",
                    classification="cleanup_syst_err",
                    action="post_cleanup_drain",
                )
                or []
            )
            final_syst_err = str(runner._read_pace_system_error_text() or "").strip()
            summary["cleanup_syst_errs"] = cleanup_syst_errs
            summary["final_syst_err"] = final_syst_err
            if isinstance(summary.get("scenario_result"), dict):
                summary["scenario_result"] = _enrich_live_result_with_pace_diagnostics(
                    runner,
                    dict(summary.get("scenario_result") or {}),
                    final_syst_err=final_syst_err,
                )
                summary["scenario_result"]["cleanup_syst_errs"] = cleanup_syst_errs
                if cleanup_syst_errs and str(summary["scenario_result"].get("status") or "") == "pass":
                    summary["scenario_result"]["status"] = "pass_with_diagnostic_error"
        summary_path = logger.run_dir / "pressure_gate_live_summary.json"
        try:
            _write_json(summary_path, summary)
        finally:
            _close_devices(devices)
            try:
                logger.close()
            except Exception:
                pass
        _log(f"Summary JSON: {summary_path}")
        _log(f"IO CSV: {logger.io_path}")
        if trace_path.exists():
            _log(f"Pressure trace CSV: {trace_path}")
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
