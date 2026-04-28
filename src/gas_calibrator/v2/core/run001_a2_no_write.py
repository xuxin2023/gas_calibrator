from __future__ import annotations

import copy
import csv
from datetime import datetime, timezone
import json
from pathlib import Path
import re
from typing import Any, Mapping, Optional

from ..config import AppConfig
from .no_write_guard import NoWriteGuard
from .run001_a1_dry_run import (
    RUN001_FAIL,
    RUN001_NOT_EXECUTED,
    RUN001_PASS,
    build_run001_a1_evidence_payload,
    evaluate_run001_a1_readiness,
    load_point_rows,
    write_run001_a1_artifacts,
)
from .services.timing_monitor_service import (
    TIMING_EVENT_FIELDS,
    WORKFLOW_TIMING_SUMMARY_FILENAME,
    WORKFLOW_TIMING_TRACE_FILENAME,
    ensure_workflow_timing_artifacts,
    load_workflow_timing_events,
)


A2_AUTHORIZED_PRESSURE_POINTS_HPA = [1100.0, 1000.0, 900.0, 800.0, 700.0, 600.0, 500.0]
A2_REQUIRED_ARTIFACTS = {
    "summary": "summary.json",
    "no_write_guard": "no_write_guard.json",
    "run_manifest": "run_manifest.json",
    "human_readable_report": "human_readable_report.md",
    "effective_analyzer_fleet": "effective_analyzer_fleet.json",
    "temperature_stability_evidence": "temperature_stability_evidence.json",
    "pressure_gate_evidence": "pressure_gate_evidence.json",
    "preseal_atmosphere_hold_evidence": "preseal_atmosphere_hold_evidence.json",
    "preseal_atmosphere_hold_samples": "preseal_atmosphere_hold_samples.csv",
    "positive_preseal_pressurization_evidence": "positive_preseal_pressurization_evidence.json",
    "positive_preseal_pressurization_samples": "positive_preseal_pressurization_samples.csv",
    "positive_preseal_timing_diagnostics": "positive_preseal_timing_diagnostics.json",
    "co2_route_conditioning_evidence": "co2_route_conditioning_evidence.json",
    "route_open_pressure_surge_evidence": "route_open_pressure_surge_evidence.json",
    "pressure_read_latency_diagnostics": "pressure_read_latency_diagnostics.json",
    "pressure_read_latency_samples": "pressure_read_latency_samples.csv",
    "high_pressure_first_point_evidence": "high_pressure_first_point_evidence.json",
    "critical_pressure_freshness_evidence": "critical_pressure_freshness_evidence.json",
    "route_trace": "route_trace.jsonl",
    "points": "points.csv",
    "io_log": "io_log.csv",
    "run_log": "run.log",
    "samples": "samples.csv",
    "route_pressure_sample_trace": "route_pressure_sample_trace.json",
}

PRESEAL_ATMOSPHERE_HOLD_SAMPLE_FIELDS = [
    "timestamp",
    "elapsed_s",
    "vent_command_sent",
    "vent_query_status",
    "output_state",
    "isolation_state",
    "measured_pressure_hpa",
    "pressure_sample_source",
    "pressure_sample_timestamp",
    "pressure_sample_age_s",
    "pressure_sample_is_stale",
    "pressure_sample_sequence_id",
    "read_latency_s",
    "is_cached",
    "usable_for_abort",
    "usable_for_ready",
    "usable_for_seal",
    "pace_pressure_hpa",
    "pace_pressure_latency_s",
    "pace_pressure_age_s",
    "pace_pressure_stale",
    "digital_gauge_pressure_hpa",
    "digital_gauge_latency_s",
    "digital_gauge_age_s",
    "digital_gauge_stale",
    "pressure_source_used_for_decision",
    "source_selection_reason",
    "pressure_source_disagreement_hpa",
    "pressure_source_disagreement_warning",
    "pressure_limit_hpa",
    "pressure_limit_exceeded",
    "route_opened",
    "sealed",
    "pressure_control_started",
    "decision",
    "failure_reason",
]

PRESSURE_READ_LATENCY_SAMPLE_FIELDS = [
    "run_id",
    "timestamp",
    "stage",
    "point_index",
    "source",
    "pressure_hpa",
    "request_sent_at",
    "response_received_at",
    "request_sent_monotonic_s",
    "response_received_monotonic_s",
    "read_latency_s",
    "sample_recorded_at",
    "sample_recorded_monotonic_s",
    "sample_age_s",
    "is_cached",
    "is_stale",
    "stale_threshold_s",
    "serial_port",
    "command",
    "raw_response",
    "parse_ok",
    "error",
    "sequence_id",
    "usable_for_abort",
    "usable_for_ready",
    "usable_for_seal",
    "primary_pressure_source",
    "pressure_source_used_for_decision",
    "source_selection_reason",
    "source_disagreement_hpa",
    "source_disagreement_warning",
    "digital_gauge_mode",
    "digital_gauge_continuous_mode",
    "digital_gauge_continuous_started",
    "digital_gauge_continuous_active",
    "digital_gauge_stream_first_frame_at",
    "digital_gauge_stream_last_frame_at",
    "continuous_stream_stale",
    "continuous_stream_age_s",
    "digital_gauge_stream_stale",
    "digital_gauge_stream_stale_threshold_s",
    "digital_gauge_drain_empty_count",
    "digital_gauge_drain_nonempty_count",
    "latest_frame_age_s",
    "latest_frame_interval_s",
    "latest_frame_sequence_id",
    "digital_gauge_latest_sequence_id",
    "last_pressure_command",
    "last_pressure_command_may_cancel_continuous",
    "continuous_interrupted_by_command",
    "continuous_restart_attempted",
    "continuous_restart_result",
    "pressure_source_selected",
    "pressure_source_selection_reason",
    "selected_pressure_source",
    "selected_pressure_sample_age_s",
    "selected_pressure_sample_is_stale",
    "selected_pressure_parse_ok",
    "selected_pressure_freshness_ok",
    "pressure_freshness_decision_source",
    "selected_pressure_fail_closed_reason",
    "critical_window_blocking_query_count",
    "critical_window_blocking_query_total_s",
    "critical_window_uses_latest_frame",
    "critical_window_uses_query",
    "conditioning_pressure_abort_hpa",
    "pressure_overlimit_seen",
    "pressure_overlimit_source",
    "pressure_overlimit_hpa",
    "vent_heartbeat_gap_exceeded",
    "digital_gauge_sequence_progress",
    "digital_gauge_latest_age_s",
    "stream_stale",
    "fail_closed_before_vent_off",
]

POSITIVE_PRESEAL_PRESSURIZATION_SAMPLE_FIELDS = [
    "timestamp",
    "stage",
    "target_pressure_hpa",
    "measured_atmospheric_pressure_hpa",
    "measured_atmospheric_pressure_source",
    "ambient_reference_pressure_hpa",
    "ambient_reference_source",
    "ambient_reference_timestamp",
    "ambient_reference_age_s",
    "current_line_pressure_hpa",
    "positive_preseal_pressure_hpa",
    "pressure_sample_source",
    "pressure_sample_timestamp",
    "pressure_sample_age_s",
    "pressure_sample_is_stale",
    "pressure_sample_sequence_id",
    "ready_pressure_hpa",
    "abort_pressure_hpa",
    "preseal_ready_pressure_hpa",
    "preseal_abort_pressure_hpa",
    "preseal_ready_timeout_s",
    "preseal_pressure_poll_interval_s",
    "preseal_vent_close_arm_pressure_hpa",
    "preseal_vent_close_arm_margin_hpa",
    "preseal_vent_close_arm_time_to_ready_s",
    "vent_close_arm_trigger",
    "vent_close_arm_pressure_hpa",
    "vent_close_arm_elapsed_s",
    "estimated_time_to_ready_s",
    "ready_reached_before_vent_close_completed",
    "ready_reached_during_vent_close",
    "ready_to_vent_close_start_s",
    "ready_to_vent_close_end_s",
    "ready_to_seal_command_s",
    "ready_to_seal_confirm_s",
    "pressure_delta_during_vent_close_hpa",
    "pressure_delta_after_ready_before_seal_hpa",
    "vent_closed_at",
    "vent_command_result",
    "output_state",
    "isolation_state",
    "elapsed_s",
    "pressure_hpa",
    "pressure_rise_rate_hpa_per_s",
    "pressure_samples_count",
    "pressure_max_hpa",
    "pressure_min_hpa",
    "ready_reached",
    "ready_reached_at_pressure_hpa",
    "seal_command_sent",
    "seal_trigger_pressure_hpa",
    "seal_trigger_elapsed_s",
    "seal_command_blocked_reason",
    "sealed",
    "pressure_control_started",
    "abort_reason",
    "decision",
]


def _as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def _as_float(value: Any) -> Optional[float]:
    try:
        return float(value)
    except Exception:
        return None


def _as_float_list(values: Any) -> list[float]:
    raw = values if isinstance(values, list) else [values]
    out: list[float] = []
    for item in raw:
        value = _as_float(item)
        if value is not None:
            out.append(float(value))
    return out


def _section(raw_cfg: Mapping[str, Any], name: str) -> dict[str, Any]:
    candidate = raw_cfg.get(name)
    return dict(candidate) if isinstance(candidate, Mapping) else {}


def _a2_policy(raw_cfg: Mapping[str, Any]) -> dict[str, Any]:
    return _section(raw_cfg, "run001_a2")


def is_run001_a2_no_write_pressure_sweep(raw_cfg: Optional[Mapping[str, Any]]) -> bool:
    if not isinstance(raw_cfg, Mapping):
        return False
    policy = _a2_policy(raw_cfg)
    if not policy:
        return False
    mode = str(policy.get("mode", raw_cfg.get("mode", "")) or "").strip().lower()
    scope = str(policy.get("scope", "") or "").strip().lower()
    return mode == "real_machine_dry_run" and scope == "run001_a2_co2_no_write_pressure_sweep"


def _a1_compatible_raw_cfg(raw_cfg: Mapping[str, Any]) -> dict[str, Any]:
    copied = copy.deepcopy(dict(raw_cfg))
    policy = _a2_policy(raw_cfg)
    copied["run001_a1"] = copy.deepcopy(policy)
    return copied


def _pressure_points_from_rows(point_rows: list[dict[str, Any]]) -> list[float]:
    out: list[float] = []
    seen: set[float] = set()
    for row in point_rows:
        pressure = _as_float(row.get("pressure_hpa", row.get("target_pressure_hpa", row.get("pressure"))))
        if pressure is None:
            continue
        key = round(float(pressure), 6)
        if key in seen:
            continue
        seen.add(key)
        out.append(float(pressure))
    return out


def _same_pressure_list(left: list[float], right: list[float]) -> bool:
    if len(left) != len(right):
        return False
    return all(abs(float(a) - float(b)) <= 1e-6 for a, b in zip(left, right))


def _workflow_pressure(raw_cfg: Mapping[str, Any]) -> dict[str, Any]:
    workflow = _section(raw_cfg, "workflow")
    pressure = workflow.get("pressure")
    return dict(pressure) if isinstance(pressure, Mapping) else {}


def _preseal_timing_thresholds(pressure_cfg: Mapping[str, Any]) -> dict[str, Any]:
    ready_timeout = _as_float(pressure_cfg.get("preseal_ready_timeout_s"))
    vent_interval = _as_float(pressure_cfg.get("vent_hold_interval_s"))
    return {
        "pressure_rise_detection_threshold_hpa": (
            _as_float(pressure_cfg.get("pressure_rise_detection_threshold_hpa")) or 2.0
        ),
        "expected_route_open_to_first_pressure_rise_max_s": (
            _as_float(pressure_cfg.get("expected_route_open_to_first_pressure_rise_max_s")) or 10.0
        ),
        "expected_route_open_to_ready_max_s": (
            _as_float(pressure_cfg.get("expected_route_open_to_ready_max_s"))
            or (ready_timeout + 10.0 if ready_timeout is not None else 40.0)
        ),
        "expected_positive_preseal_to_ready_max_s": (
            _as_float(pressure_cfg.get("expected_positive_preseal_to_ready_max_s"))
            or (ready_timeout if ready_timeout is not None else 30.0)
        ),
        "expected_ready_to_seal_command_max_s": (
            _as_float(pressure_cfg.get("expected_ready_to_seal_command_max_s")) or 0.5
        ),
        "expected_ready_to_seal_confirm_max_s": (
            _as_float(pressure_cfg.get("expected_ready_to_seal_confirm_max_s")) or 2.0
        ),
        "expected_max_pressure_increase_after_ready_hpa": (
            _as_float(pressure_cfg.get("expected_max_pressure_increase_after_ready_hpa")) or 10.0
        ),
        "expected_vent_hold_tick_interval_s": (
            _as_float(pressure_cfg.get("expected_vent_hold_tick_interval_s"))
            or (vent_interval if vent_interval is not None else 2.0)
        ),
        "expected_vent_hold_pressure_rise_rate_max_hpa_per_s": (
            _as_float(pressure_cfg.get("expected_vent_hold_pressure_rise_rate_max_hpa_per_s")) or 25.0
        ),
        "expected_abort_margin_min_hpa": (
            _as_float(pressure_cfg.get("expected_abort_margin_min_hpa")) or 10.0
        ),
        "primary_pressure_source": str(pressure_cfg.get("primary_pressure_source") or "digital_pressure_gauge"),
        "pressure_source_cross_check_enabled": _as_bool(
            pressure_cfg.get("pressure_source_cross_check_enabled", True)
        ),
        "pressure_source_disagreement_warn_hpa": (
            _as_float(pressure_cfg.get("pressure_source_disagreement_warn_hpa")) or 10.0
        ),
        "pressure_sample_stale_threshold_s": (
            _as_float(pressure_cfg.get("pressure_sample_stale_threshold_s"))
            or _as_float(pressure_cfg.get("pressure_sample_stale_max_s"))
            or 2.0
        ),
        "pressure_read_latency_warn_s": (
            _as_float(pressure_cfg.get("pressure_read_latency_warn_s")) or 0.5
        ),
        "route_open_first_pressure_request_expected_max_s": (
            _as_float(pressure_cfg.get("route_open_first_pressure_request_expected_max_s")) or 0.5
        ),
        "route_open_first_pressure_response_expected_max_s": (
            _as_float(pressure_cfg.get("route_open_first_pressure_response_expected_max_s")) or 1.0
        ),
        "pressure_latency_warning_only": _as_bool(pressure_cfg.get("pressure_latency_warning_only", True)),
        "digital_gauge_continuous_enabled": _as_bool(
            pressure_cfg.get("digital_gauge_continuous_enabled", True)
        ),
        "digital_gauge_continuous_mode": str(pressure_cfg.get("digital_gauge_continuous_mode") or "P4"),
        "digital_gauge_latest_frame_stale_max_s": (
            _as_float(pressure_cfg.get("digital_gauge_latest_frame_stale_max_s"))
            or _as_float(pressure_cfg.get("critical_pressure_latest_frame_stale_max_s"))
            or 0.5
        ),
        "pace_aux_enabled": _as_bool(pressure_cfg.get("pace_aux_enabled", True)),
        "pace_aux_disagreement_warn_hpa": (
            _as_float(pressure_cfg.get("pace_aux_disagreement_warn_hpa"))
            or _as_float(pressure_cfg.get("pressure_source_disagreement_warn_hpa"))
            or 10.0
        ),
        "high_pressure_first_point_mode_configured": _as_bool(
            pressure_cfg.get("high_pressure_first_point_mode_enabled", True)
        ),
        "high_pressure_first_point_margin_hpa": (
            _as_float(pressure_cfg.get("high_pressure_first_point_margin_hpa")) or 0.0
        ),
        "high_pressure_first_point_route_open_request_expected_max_s": (
            _as_float(pressure_cfg.get("high_pressure_first_point_route_open_request_expected_max_s"))
            or _as_float(pressure_cfg.get("route_open_first_pressure_request_expected_max_s"))
            or 0.05
        ),
        "high_pressure_first_point_route_open_response_expected_max_s": (
            _as_float(pressure_cfg.get("high_pressure_first_point_route_open_response_expected_max_s"))
            or _as_float(pressure_cfg.get("route_open_first_pressure_response_expected_max_s"))
            or 1.0
        ),
        "preseal_vent_close_arm_pressure_hpa": _as_float(
            pressure_cfg.get("preseal_vent_close_arm_pressure_hpa")
        ),
        "preseal_vent_close_arm_margin_hpa": (
            _as_float(pressure_cfg.get("preseal_vent_close_arm_margin_hpa")) or 30.0
        ),
        "preseal_vent_close_arm_time_to_ready_s": (
            _as_float(pressure_cfg.get("preseal_vent_close_arm_time_to_ready_s")) or 3.0
        ),
        "timing_warning_only": _as_bool(pressure_cfg.get("timing_warning_only", True)),
    }


def _preseal_atmosphere_hold_limit_hpa(
    raw_cfg: Mapping[str, Any],
    point_rows: list[dict[str, Any]],
) -> float:
    pressure = _workflow_pressure(raw_cfg)
    configured = _as_float(pressure.get("preseal_atmosphere_hold_max_hpa"))
    if configured is not None:
        return float(configured)
    default_limit = _as_float(pressure.get("preseal_atmosphere_hold_default_max_hpa"))
    margin = _as_float(pressure.get("preseal_atmosphere_hold_margin_hpa"))
    point_pressures = _pressure_points_from_rows(point_rows)
    target_limit = max(point_pressures) + abs(10.0 if margin is None else float(margin)) if point_pressures else 1110.0
    return max(1110.0 if default_limit is None else float(default_limit), target_limit)


def _first_pressure_hpa(point_rows: list[dict[str, Any]]) -> Optional[float]:
    pressures = _pressure_points_from_rows(point_rows)
    return pressures[0] if pressures else None


def _positive_preseal_ready_pressure_hpa(
    raw_cfg: Mapping[str, Any],
    point_rows: list[dict[str, Any]],
) -> Optional[float]:
    pressure = _workflow_pressure(raw_cfg)
    configured = _as_float(pressure.get("preseal_ready_pressure_hpa"))
    if configured is not None:
        return float(configured)
    target = _first_pressure_hpa(point_rows)
    if target is None:
        return None
    margin = _as_float(pressure.get("preseal_ready_margin_hpa"))
    return float(target) + float(0.0 if margin is None else margin)


def _positive_preseal_abort_pressure_hpa(
    raw_cfg: Mapping[str, Any],
    point_rows: list[dict[str, Any]],
) -> Optional[float]:
    pressure = _workflow_pressure(raw_cfg)
    configured = _as_float(pressure.get("preseal_abort_pressure_hpa"))
    if configured is not None:
        return float(configured)
    ready = _positive_preseal_ready_pressure_hpa(raw_cfg, point_rows)
    if ready is None:
        return None
    margin = _as_float(pressure.get("preseal_abort_margin_hpa"))
    return float(ready) + abs(40.0 if margin is None else float(margin))


def _completed_pressure_points_from_trace(run_dir: str | Path | None) -> list[float]:
    if run_dir is None:
        return []
    trace_path = Path(run_dir) / "route_trace.jsonl"
    if not trace_path.exists():
        return []
    out: list[float] = []
    seen: set[float] = set()
    pattern = re.compile(r"_(\d+(?:\.\d+)?)hpa\b", re.IGNORECASE)
    for line in trace_path.read_text(encoding="utf-8").splitlines():
        try:
            item = json.loads(line)
        except Exception:
            continue
        if str(item.get("action") or "") != "sample_end":
            continue
        if str(item.get("result") or "").lower() != "ok":
            continue
        tag = str(item.get("point_tag") or "")
        match = pattern.search(tag)
        if not match:
            continue
        pressure = float(match.group(1))
        key = round(pressure, 6)
        if key in seen:
            continue
        seen.add(key)
        out.append(pressure)
    return out


def _load_json_dict(path: str | Path | None) -> dict[str, Any]:
    if path is None:
        return {}
    target = Path(path)
    if not target.exists():
        return {}
    try:
        payload = json.loads(target.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return dict(payload) if isinstance(payload, Mapping) else {}


def _load_route_trace_rows(run_dir: str | Path) -> list[dict[str, Any]]:
    trace_path = Path(run_dir) / "route_trace.jsonl"
    rows: list[dict[str, Any]] = []
    if not trace_path.exists():
        return rows
    for line in trace_path.read_text(encoding="utf-8").splitlines():
        try:
            item = json.loads(line)
        except Exception:
            continue
        if isinstance(item, Mapping):
            rows.append(dict(item))
    return rows


def _final_safe_stop_evidence(run_dir: str | Path) -> dict[str, Any]:
    defaults: dict[str, Any] = {
        "final_safe_stop_warning_count": 0,
        "final_safe_stop_warnings": [],
        "final_safe_stop_chamber_stop_warning": "",
        "final_safe_stop_chamber_stop_attempted": False,
        "final_safe_stop_chamber_stop_command_sent": False,
        "final_safe_stop_chamber_stop_result": "not_observed",
    }
    for row in reversed(_load_route_trace_rows(run_dir)):
        if str(row.get("action") or "") != "final_safe_stop_routes":
            continue
        actual = row.get("actual")
        actual = actual if isinstance(actual, Mapping) else {}
        warnings = actual.get("final_safe_stop_warnings")
        warnings = list(warnings) if isinstance(warnings, list) else []
        payload = dict(defaults)
        payload.update(
            {
                "final_safe_stop_warning_count": int(
                    actual.get("final_safe_stop_warning_count")
                    if actual.get("final_safe_stop_warning_count") is not None
                    else len(warnings)
                ),
                "final_safe_stop_warnings": warnings,
                "final_safe_stop_chamber_stop_warning": str(
                    actual.get("final_safe_stop_chamber_stop_warning") or ""
                ),
                "final_safe_stop_chamber_stop_attempted": bool(
                    actual.get("final_safe_stop_chamber_stop_attempted", False)
                ),
                "final_safe_stop_chamber_stop_command_sent": bool(
                    actual.get("final_safe_stop_chamber_stop_command_sent", False)
                ),
                "final_safe_stop_chamber_stop_result": str(
                    actual.get("final_safe_stop_chamber_stop_result") or "not_observed"
                ),
            }
        )
        return payload
    return defaults


def _status_to_mapping(status: Any) -> dict[str, Any]:
    if status is None:
        return {}
    if isinstance(status, Mapping):
        return dict(status)
    phase = getattr(status, "phase", "")
    return {
        "phase": str(getattr(phase, "value", phase) or "").strip().lower(),
        "total_points": getattr(status, "total_points", None),
        "completed_points": getattr(status, "completed_points", None),
        "progress": getattr(status, "progress", None),
        "message": getattr(status, "message", ""),
        "elapsed_s": getattr(status, "elapsed_s", None),
        "error": getattr(status, "error", None),
    }


def _service_status_snapshot(service: Any) -> dict[str, Any]:
    getter = getattr(service, "get_status", None)
    if callable(getter):
        try:
            return _status_to_mapping(getter())
        except Exception:
            return {}
    return _status_to_mapping(getattr(service, "status", None))


def evaluate_run001_a2_readiness(
    raw_cfg: Mapping[str, Any],
    *,
    config_path: str | Path | None = None,
    point_rows: Optional[list[dict[str, Any]]] = None,
    attempted_write_count: int = 0,
    blocked_write_events: Optional[list[dict[str, Any]]] = None,
    artifact_paths: Optional[Mapping[str, Any]] = None,
    require_runtime_artifacts: bool = False,
) -> dict[str, Any]:
    compat = _a1_compatible_raw_cfg(raw_cfg)
    rows = list(point_rows if point_rows is not None else load_point_rows(config_path, raw_cfg))
    base = evaluate_run001_a1_readiness(
        compat,
        config_path=config_path,
        point_rows=rows,
        attempted_write_count=attempted_write_count,
        blocked_write_events=blocked_write_events,
        artifact_paths=artifact_paths,
        require_runtime_artifacts=require_runtime_artifacts,
    )
    policy = _a2_policy(raw_cfg)
    workflow = _section(raw_cfg, "workflow")
    devices = _section(raw_cfg, "devices")
    reasons = list(base.get("hard_stop_reasons") or [])

    if not policy:
        reasons.append("run001_a2_policy_missing")
    if str(policy.get("scope") or "").strip() != "run001_a2_co2_no_write_pressure_sweep":
        reasons.append("run001_a2_scope_mismatch")
    if not _as_bool(policy.get("no_write")):
        reasons.append("a2_no_write_not_true")
    if _as_bool(policy.get("default_cutover_to_v2")):
        reasons.append("a2_default_cutover_to_v2_true")
    if _as_bool(policy.get("full_h2o_co2_group")):
        reasons.append("a2_full_group_requested")
    if str(workflow.get("route_mode") or "").strip().lower() != "co2_only":
        reasons.append("a2_route_mode_not_co2_only")

    for key in ("dewpoint_meter", "humidity_generator"):
        device = devices.get(key)
        if isinstance(device, Mapping) and _as_bool(device.get("enabled")):
            reasons.append(f"a2_{key}_enabled")

    configured_pressures = _as_float_list(policy.get("authorized_pressure_points_hpa"))
    if not _same_pressure_list(configured_pressures, A2_AUTHORIZED_PRESSURE_POINTS_HPA):
        reasons.append("a2_authorized_pressure_points_mismatch")
    point_pressures = _pressure_points_from_rows(rows)
    if not _same_pressure_list(point_pressures, A2_AUTHORIZED_PRESSURE_POINTS_HPA):
        reasons.append("a2_point_pressure_list_mismatch")
    point_routes = {str(row.get("route", "") or "").strip().lower() for row in rows}
    if point_routes != {"co2"}:
        reasons.append("a2_points_not_co2_only")

    deduped = list(dict.fromkeys(reasons))
    return {
        **base,
        "readiness_result": RUN001_PASS if not deduped else RUN001_FAIL,
        "final_decision": RUN001_PASS if not deduped else RUN001_FAIL,
        "hard_stop_reasons": deduped,
        "a2_authorized_pressure_points_hpa": list(A2_AUTHORIZED_PRESSURE_POINTS_HPA),
        "a2_point_pressure_points_hpa": point_pressures,
        "a2_scope": "co2_single_route_full_pressure_no_write",
    }


def authorize_run001_a2_no_write_pressure_sweep(
    config: AppConfig,
    raw_cfg: Mapping[str, Any],
    cli_args: Any,
    *,
    config_path: str,
) -> dict[str, Any]:
    del config
    rows = load_point_rows(config_path, raw_cfg)
    readiness = evaluate_run001_a2_readiness(raw_cfg, config_path=config_path, point_rows=rows)
    reasons = list(readiness.get("hard_stop_reasons") or [])
    if not _as_bool(getattr(cli_args, "execute", False)):
        reasons.append("execute_flag_missing")
    if not _as_bool(getattr(cli_args, "confirm_real_machine_no_write", False)):
        reasons.append("confirm_real_machine_no_write_missing")
    if not _as_bool(getattr(cli_args, "confirm_a2_no_write_pressure_sweep", False)):
        reasons.append("confirm_a2_no_write_pressure_sweep_missing")
    if reasons:
        raise RuntimeError("Run-001/A2 no-write pressure sweep gate blocked: " + "; ".join(dict.fromkeys(reasons)))
    return {
        "gate_id": "run001_a2_no_write_pressure_sweep",
        "status": "authorized",
        "scope": "Run-001/A2 CO2-only 7-pressure no-write real-machine validation",
        "authorized_pressure_points_hpa": list(A2_AUTHORIZED_PRESSURE_POINTS_HPA),
        "requires_execute_flag": True,
        "requires_confirm_real_machine_no_write": True,
        "requires_confirm_a2_no_write_pressure_sweep": True,
    }


def _build_pressure_gate_evidence(run_dir: str | Path, payload: Mapping[str, Any]) -> dict[str, Any]:
    trace_path = Path(run_dir) / "route_trace.jsonl"
    records: list[dict[str, Any]] = []
    if trace_path.exists():
        for line in trace_path.read_text(encoding="utf-8").splitlines():
            try:
                item = json.loads(line)
            except Exception:
                continue
            action = str(item.get("action") or "").lower()
            if "pressure" in action or "seal" in action or action == "set_vent":
                records.append(dict(item))
    return {
        "schema_version": "run001_a2.pressure_gate_evidence.1",
        "artifact_type": "run001_a2_pressure_gate_evidence",
        "run_id": payload.get("run_id"),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "pressure_gate_policy": payload.get("pressure_gate_policy"),
        "authorized_pressure_points_hpa": payload.get("a2_authorized_pressure_points_hpa"),
        "completed_pressure_points_hpa": payload.get("planned_pressure_points_completed"),
        "record_count": len(records),
        "records": records,
        "not_real_acceptance_evidence": True,
    }


def _parse_trace_ts(value: Any) -> Optional[datetime]:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except Exception:
        return None


def _trace_elapsed_s(ts: Optional[datetime], start_ts: Optional[datetime]) -> Optional[float]:
    if ts is None or start_ts is None:
        return None
    try:
        return round(max(0.0, (ts - start_ts).total_seconds()), 3)
    except Exception:
        return None


def _trace_pressure_hpa(actual: Mapping[str, Any]) -> Optional[float]:
    for key in (
        "pressure_hpa",
        "current_line_pressure_hpa",
        "positive_preseal_pressure_hpa",
        "preseal_pressure_peak_hpa",
        "preseal_pressure_last_hpa",
        "preseal_trigger_pressure_hpa",
        "sealed_pressure_hpa",
    ):
        value = _as_float(actual.get(key))
        if value is not None:
            return value
    pressure_evidence = actual.get("pressure_evidence")
    if isinstance(pressure_evidence, Mapping):
        for key in ("pressure_hpa", "preseal_pressure_peak_hpa", "preseal_pressure_last_hpa"):
            value = _as_float(pressure_evidence.get(key))
            if value is not None:
                return value
    return None


def _pressure_sample_source(actual: Mapping[str, Any], *, fallback: str = "unknown") -> str:
    source = (
        actual.get("pressure_sample_source")
        or actual.get("source")
        or actual.get("pressure_source_used_for_decision")
        or actual.get("pressure_source_used")
        or actual.get("abort_decision_pressure_source")
        or fallback
    )
    text = str(source or "").strip()
    aliases = {
        "pressure_gauge": "digital_pressure_gauge",
        "pressure_meter": "digital_pressure_gauge",
        "gauge": "digital_pressure_gauge",
        "pressure_controller": "pace_controller",
        "pace": "pace_controller",
    }
    return aliases.get(text, text or fallback)


def _pressure_sample_timestamp(actual: Mapping[str, Any], fallback_ts: Any = "") -> str:
    return str(
        actual.get("pressure_sample_timestamp")
        or actual.get("sample_recorded_at")
        or actual.get("response_received_at")
        or actual.get("timestamp")
        or fallback_ts
        or ""
    )


def _pressure_sample_meta(actual: Mapping[str, Any], *, fallback_source: str = "unknown") -> dict[str, Any]:
    source = _pressure_sample_source(actual, fallback=fallback_source)
    age_s = _as_float(actual.get("pressure_sample_age_s", actual.get("sample_age_s")))
    latency_s = _as_float(actual.get("read_latency_s"))
    is_stale = actual.get("pressure_sample_is_stale", actual.get("is_stale"))
    if isinstance(is_stale, str):
        is_stale = is_stale.strip().lower() in {"1", "true", "yes", "on"}
    return {
        "pressure_sample_source": source,
        "pressure_sample_timestamp": _pressure_sample_timestamp(actual),
        "pressure_sample_age_s": age_s,
        "pressure_sample_is_stale": bool(is_stale) if is_stale is not None else None,
        "pressure_sample_sequence_id": actual.get("pressure_sample_sequence_id", actual.get("sequence_id")),
        "read_latency_s": latency_s,
        "is_cached": bool(actual.get("is_cached", False)),
        "usable_for_abort": actual.get("usable_for_abort"),
        "usable_for_ready": actual.get("usable_for_ready"),
        "usable_for_seal": actual.get("usable_for_seal"),
        "pace_pressure_hpa": _as_float(actual.get("pace_pressure_hpa")),
        "pace_pressure_latency_s": _as_float(actual.get("pace_pressure_latency_s")),
        "pace_pressure_age_s": _as_float(actual.get("pace_pressure_age_s")),
        "pace_pressure_stale": actual.get("pace_pressure_stale"),
        "digital_gauge_pressure_hpa": _as_float(actual.get("digital_gauge_pressure_hpa")),
        "digital_gauge_latency_s": _as_float(actual.get("digital_gauge_latency_s")),
        "digital_gauge_age_s": _as_float(actual.get("digital_gauge_age_s")),
        "digital_gauge_stale": actual.get("digital_gauge_stale"),
        "pressure_source_used_for_decision": actual.get("pressure_source_used_for_decision"),
        "source_selection_reason": actual.get("source_selection_reason"),
        "pressure_source_disagreement_hpa": _as_float(actual.get("pressure_source_disagreement_hpa")),
        "pressure_source_disagreement_warning": bool(actual.get("pressure_source_disagreement_warning", False)),
        "digital_gauge_mode": actual.get("digital_gauge_mode"),
        "digital_gauge_continuous_active": actual.get("digital_gauge_continuous_active"),
        "digital_gauge_continuous_mode": actual.get("digital_gauge_continuous_mode"),
        "latest_frame_age_s": _as_float(actual.get("latest_frame_age_s")),
        "latest_frame_sequence_id": actual.get("latest_frame_sequence_id"),
        "critical_window_uses_latest_frame": actual.get("critical_window_uses_latest_frame"),
        "critical_window_uses_query": actual.get("critical_window_uses_query"),
        "critical_window_blocking_query_count": actual.get("critical_window_blocking_query_count"),
        "critical_window_blocking_query_total_s": _as_float(
            actual.get("critical_window_blocking_query_total_s")
        ),
        "pace_aux_enabled": actual.get("pace_aux_enabled"),
        "pace_digital_overlap_samples": actual.get("pace_digital_overlap_samples"),
        "pace_digital_max_diff_hpa": _as_float(actual.get("pace_digital_max_diff_hpa")),
    }


def _latency_sample_from_payload(
    payload: Mapping[str, Any],
    *,
    run_id: Any,
    timestamp: Any = "",
    stage: Any = "",
    point_index: Any = None,
    selection: Optional[Mapping[str, Any]] = None,
) -> dict[str, Any]:
    selection = dict(selection or {})
    source = _pressure_sample_source(payload, fallback=str(payload.get("source") or "unknown"))
    return {
        "run_id": run_id,
        "timestamp": timestamp or payload.get("sample_recorded_at") or payload.get("pressure_sample_timestamp") or "",
        "stage": stage or payload.get("stage") or "",
        "point_index": point_index if point_index is not None else payload.get("point_index"),
        "source": source,
        "pressure_hpa": _as_float(payload.get("pressure_hpa")),
        "request_sent_at": payload.get("request_sent_at"),
        "response_received_at": payload.get("response_received_at"),
        "request_sent_monotonic_s": _as_float(payload.get("request_sent_monotonic_s")),
        "response_received_monotonic_s": _as_float(payload.get("response_received_monotonic_s")),
        "read_latency_s": _as_float(payload.get("read_latency_s")),
        "sample_recorded_at": payload.get("sample_recorded_at") or payload.get("pressure_sample_timestamp"),
        "sample_recorded_monotonic_s": _as_float(
            payload.get("sample_recorded_monotonic_s", payload.get("pressure_sample_monotonic_s"))
        ),
        "sample_age_s": _as_float(payload.get("sample_age_s", payload.get("pressure_sample_age_s"))),
        "is_cached": bool(payload.get("is_cached", False)),
        "is_stale": bool(payload.get("is_stale", payload.get("pressure_sample_is_stale", False))),
        "stale_threshold_s": _as_float(payload.get("stale_threshold_s")),
        "serial_port": payload.get("serial_port"),
        "command": payload.get("command"),
        "raw_response": payload.get("raw_response"),
        "parse_ok": payload.get("parse_ok"),
        "error": payload.get("error"),
        "sequence_id": payload.get("sequence_id", payload.get("pressure_sample_sequence_id")),
        "usable_for_abort": payload.get("usable_for_abort"),
        "usable_for_ready": payload.get("usable_for_ready"),
        "usable_for_seal": payload.get("usable_for_seal"),
        "primary_pressure_source": selection.get("primary_pressure_source"),
        "pressure_source_used_for_decision": selection.get("pressure_source_used_for_decision"),
        "source_selection_reason": selection.get("source_selection_reason"),
        "source_disagreement_hpa": selection.get("pressure_source_disagreement_hpa"),
        "source_disagreement_warning": selection.get("pressure_source_disagreement_warning"),
        "digital_gauge_mode": payload.get("digital_gauge_mode") or selection.get("digital_gauge_mode"),
        "digital_gauge_continuous_active": payload.get("digital_gauge_continuous_active"),
        "latest_frame_age_s": _as_float(payload.get("latest_frame_age_s")),
        "latest_frame_interval_s": _as_float(payload.get("latest_frame_interval_s")),
        "latest_frame_sequence_id": payload.get("latest_frame_sequence_id"),
        "critical_window_blocking_query_count": payload.get("critical_window_blocking_query_count"),
        "critical_window_blocking_query_total_s": _as_float(
            payload.get("critical_window_blocking_query_total_s")
        ),
        "critical_window_uses_latest_frame": payload.get("critical_window_uses_latest_frame"),
        "critical_window_uses_query": payload.get("critical_window_uses_query"),
        "conditioning_pressure_abort_hpa": _as_float(
            payload.get("conditioning_pressure_abort_hpa")
            if payload.get("conditioning_pressure_abort_hpa") is not None
            else selection.get("conditioning_pressure_abort_hpa")
        ),
        "pressure_overlimit_seen": bool(
            payload.get("pressure_overlimit_seen", selection.get("pressure_overlimit_seen", False))
        ),
        "pressure_overlimit_source": payload.get("pressure_overlimit_source")
        or selection.get("pressure_overlimit_source"),
        "pressure_overlimit_hpa": _as_float(
            payload.get("pressure_overlimit_hpa")
            if payload.get("pressure_overlimit_hpa") is not None
            else selection.get("pressure_overlimit_hpa")
        ),
        "vent_heartbeat_gap_exceeded": bool(
            payload.get("vent_heartbeat_gap_exceeded", selection.get("vent_heartbeat_gap_exceeded", False))
        ),
        "digital_gauge_sequence_progress": payload.get(
            "digital_gauge_sequence_progress",
            selection.get("digital_gauge_sequence_progress"),
        ),
        "digital_gauge_latest_age_s": _as_float(
            payload.get("digital_gauge_latest_age_s")
            if payload.get("digital_gauge_latest_age_s") is not None
            else selection.get("digital_gauge_latest_age_s")
        ),
        "stream_stale": bool(payload.get("stream_stale", selection.get("stream_stale", False))),
        "fail_closed_before_vent_off": bool(
            payload.get("fail_closed_before_vent_off", selection.get("fail_closed_before_vent_off", False))
        ),
    }


def _collect_pressure_read_latency_samples(run_dir: str | Path, payload: Mapping[str, Any]) -> list[dict[str, Any]]:
    run_id = payload.get("run_id")
    samples: list[dict[str, Any]] = []
    for item in list(payload.get("pressure_read_latency_samples") or []):
        if isinstance(item, Mapping):
            samples.append(_latency_sample_from_payload(item, run_id=run_id))
    events = load_workflow_timing_events(Path(run_dir) / WORKFLOW_TIMING_TRACE_FILENAME)
    for event in events:
        name = str(event.get("event_name") or "")
        if name not in {
            "pace_pressure_read_end",
            "gauge_pressure_read_end",
            "pressure_source_selected",
            "pressure_source_selection",
            "digital_gauge_latest_frame_used",
            "digital_gauge_latest_frame_stale",
        }:
            continue
        state = event.get("route_state")
        state = state if isinstance(state, Mapping) else {}
        if name in {"pressure_source_selected", "pressure_source_selection"}:
            for key in ("pace_pressure_sample", "digital_gauge_pressure_sample"):
                sample = state.get(key)
                if isinstance(sample, Mapping):
                    samples.append(
                        _latency_sample_from_payload(
                            sample,
                            run_id=run_id,
                            timestamp=event.get("timestamp_local") or event.get("timestamp"),
                            stage=event.get("stage"),
                            point_index=event.get("point_index"),
                            selection=state,
                        )
                    )
            continue
        if name in {"digital_gauge_latest_frame_used", "digital_gauge_latest_frame_stale"}:
            samples.append(
                _latency_sample_from_payload(
                    state,
                    run_id=run_id,
                    timestamp=event.get("timestamp_local") or event.get("timestamp"),
                    stage=event.get("stage"),
                    point_index=event.get("point_index"),
                    selection=state,
                )
            )
            continue
        samples.append(
            _latency_sample_from_payload(
                state,
                run_id=run_id,
                timestamp=event.get("timestamp_local") or event.get("timestamp"),
                stage=event.get("stage"),
                point_index=event.get("point_index"),
            )
        )
    if samples:
        return samples
    for row in _load_route_trace_rows(run_dir):
        actual = row.get("actual")
        actual = actual if isinstance(actual, Mapping) else {}
        pressure = _trace_pressure_hpa(actual)
        if pressure is None:
            continue
        source = _pressure_sample_source(actual, fallback="legacy_route_trace_pressure")
        sample = _latency_sample_from_payload(
            {
                **actual,
                "pressure_hpa": pressure,
                "pressure_sample_source": source,
                "parse_ok": True,
            },
            run_id=run_id,
            timestamp=row.get("ts") or row.get("timestamp") or "",
            stage=row.get("action") or "",
            point_index=row.get("point_index"),
        )
        if source == "legacy_route_trace_pressure":
            sample["error"] = "legacy_sample_missing_source_latency_metadata"
        samples.append(sample)
    return samples


def _first_after(rows: list[Mapping[str, Any]], timestamp: Optional[datetime]) -> Optional[Mapping[str, Any]]:
    if timestamp is None:
        return rows[0] if rows else None
    for row in rows:
        ts = _parse_trace_ts(row.get("ts") or row.get("timestamp"))
        if ts is not None and ts >= timestamp:
            return row
    return None


def _positive_preseal_ambient_reference_from_trace(
    trace_rows: list[Mapping[str, Any]],
    payload: Mapping[str, Any],
) -> dict[str, Any]:
    start_ts: Optional[datetime] = None
    candidates: list[dict[str, Any]] = []
    for item in trace_rows:
        action = str(item.get("action") or "").lower()
        actual = item.get("actual")
        actual = dict(actual) if isinstance(actual, Mapping) else {}
        target = item.get("target")
        target = dict(target) if isinstance(target, Mapping) else {}
        ts_text = str(item.get("ts") or item.get("timestamp") or "")
        ts = _parse_trace_ts(ts_text)
        if action == "positive_preseal_pressurization_start" and start_ts is None:
            start_ts = ts
            for key in ("ambient_reference_pressure_hpa", "measured_atmospheric_pressure_hpa"):
                value = _as_float(actual.get(key))
                if value is not None and key == "ambient_reference_pressure_hpa":
                    return {
                        "ambient_reference_pressure_hpa": value,
                        "ambient_reference_source": str(actual.get("ambient_reference_source") or "positive_preseal_start"),
                        "ambient_reference_timestamp": str(actual.get("ambient_reference_timestamp") or ""),
                        "ambient_reference_age_s": _as_float(actual.get("ambient_reference_age_s")),
                        "measured_atmospheric_pressure_hpa": value,
                        "measured_atmospheric_pressure_source": "deprecated_alias_of_ambient_reference_pressure_hpa",
                    }
            continue
        if action != "set_vent" or target.get("vent_on") is not True:
            continue
        pressure = _as_float(actual.get("pressure_hpa"))
        if pressure is None:
            continue
        if bool(actual.get("atmosphere_ready")) or actual.get("output_state") == 0 or actual.get("isolation_state") == 1:
            candidates.append(
                {
                    "pressure": pressure,
                    "timestamp": ts_text,
                    "parsed_ts": ts,
                    "source": "route_trace:set_vent_atmosphere_hold",
                }
            )
    if candidates:
        chosen = candidates[-1]
        age_s = _trace_elapsed_s(start_ts, chosen.get("parsed_ts")) if start_ts and chosen.get("parsed_ts") else None
        return {
            "ambient_reference_pressure_hpa": chosen["pressure"],
            "ambient_reference_source": chosen["source"],
            "ambient_reference_timestamp": chosen["timestamp"],
            "ambient_reference_age_s": age_s,
            "measured_atmospheric_pressure_hpa": chosen["pressure"],
            "measured_atmospheric_pressure_source": "deprecated_alias_of_ambient_reference_pressure_hpa",
        }
    payload_pressure = _as_float(payload.get("ambient_reference_pressure_hpa"))
    return {
        "ambient_reference_pressure_hpa": payload_pressure,
        "ambient_reference_source": str(payload.get("ambient_reference_source") or "unavailable"),
        "ambient_reference_timestamp": str(payload.get("ambient_reference_timestamp") or ""),
        "ambient_reference_age_s": _as_float(payload.get("ambient_reference_age_s")),
        "measured_atmospheric_pressure_hpa": payload_pressure,
        "measured_atmospheric_pressure_source": (
            "deprecated_alias_of_ambient_reference_pressure_hpa"
            if payload_pressure is not None
            else "deprecated_alias_unavailable"
        ),
    }


def _build_preseal_atmosphere_hold_evidence(run_dir: str | Path, payload: Mapping[str, Any]) -> dict[str, Any]:
    trace_path = Path(run_dir) / "route_trace.jsonl"
    pressure_limit = _as_float(payload.get("preseal_atmosphere_hold_pressure_limit_hpa"))
    pressure_limit = 1110.0 if pressure_limit is None else float(pressure_limit)
    route_open_ts: Optional[datetime] = None
    route_opened = False
    sealed = False
    pressure_control_started = False
    sample_started = False
    samples: list[dict[str, Any]] = []
    max_pressure: Optional[float] = None
    pressure_at_abort: Optional[float] = None
    pressure_max_before_abort: Optional[float] = None
    pressure_max_after_abort: Optional[float] = None
    pressure_max_during_safe_stop: Optional[float] = None
    abort_seen = False
    limit_exceeded = False
    failure_reasons: list[str] = []
    periodic_vent_count = 0

    if trace_path.exists():
        for line in trace_path.read_text(encoding="utf-8").splitlines():
            try:
                item = json.loads(line)
            except Exception:
                continue
            action = str(item.get("action") or "")
            action_lc = action.lower()
            actual = item.get("actual")
            actual = dict(actual) if isinstance(actual, Mapping) else {}
            target = item.get("target")
            target = dict(target) if isinstance(target, Mapping) else {}
            ts = _parse_trace_ts(item.get("ts") or item.get("timestamp"))

            if action_lc == "set_co2_valves":
                route_opened = True
                route_open_ts = route_open_ts or ts
            if action_lc in {"seal_transition", "seal_route"}:
                sealed = True
            if action_lc == "pressure_control_ready_gate":
                pressure_control_started = bool(actual.get("sealed_route_pressure_control_started"))
            if action_lc == "sample_start":
                sample_started = True

            measured_pressure = _trace_pressure_hpa(actual)
            if measured_pressure is not None:
                max_pressure = measured_pressure if max_pressure is None else max(max_pressure, measured_pressure)
                if abort_seen:
                    pressure_max_after_abort = (
                        measured_pressure
                        if pressure_max_after_abort is None
                        else max(pressure_max_after_abort, measured_pressure)
                    )
                else:
                    pressure_max_before_abort = (
                        measured_pressure
                        if pressure_max_before_abort is None
                        else max(pressure_max_before_abort, measured_pressure)
                    )
            row_exceeded = bool(measured_pressure is not None and measured_pressure > pressure_limit)
            limit_exceeded = limit_exceeded or row_exceeded
            if row_exceeded:
                failure_reasons.append("preseal_atmosphere_hold_pressure_limit_exceeded")
                if pressure_at_abort is None:
                    pressure_at_abort = measured_pressure
                    abort_seen = True
            if action_lc in {"set_output", "set_vent", "final_safe_stop_pressure"} and abort_seen and measured_pressure is not None:
                pressure_max_during_safe_stop = (
                    measured_pressure
                    if pressure_max_during_safe_stop is None
                    else max(pressure_max_during_safe_stop, measured_pressure)
                )

            vent_command_sent = bool(action_lc == "set_vent" and target.get("vent_on") is True)
            if vent_command_sent and route_opened:
                periodic_vent_count += 1

            include_row = action_lc in {
                "set_vent",
                "set_co2_valves",
                "wait_route_soak",
                "co2_preseal_atmosphere_hold_pressure_guard",
                "seal_transition",
                "seal_route",
                "pressure_control_ready_gate",
                "sample_start",
                "a2_abort_operator_observation",
            } or measured_pressure is not None
            if not include_row:
                continue

            vent_query_status = actual.get("vent_status_raw", actual.get("pressure_controller_vent_status"))
            output_state = actual.get("output_state", actual.get("pressure_controller_output_state"))
            isolation_state = actual.get("isolation_state", actual.get("pressure_controller_isolation_state"))
            decision = RUN001_FAIL if row_exceeded or str(item.get("result") or "").lower() == "fail" else ""
            failure_reason = ""
            if row_exceeded:
                failure_reason = "preseal_atmosphere_hold_pressure_limit_exceeded"
            elif str(item.get("result") or "").lower() == "fail":
                failure_reason = str(item.get("message") or "")
            sample_meta = _pressure_sample_meta(actual, fallback_source="legacy_route_trace_pressure")
            samples.append(
                {
                    "timestamp": item.get("ts") or item.get("timestamp") or "",
                    "elapsed_s": _trace_elapsed_s(ts, route_open_ts),
                    "vent_command_sent": vent_command_sent,
                    "vent_query_status": vent_query_status,
                    "output_state": output_state,
                    "isolation_state": isolation_state,
                    "measured_pressure_hpa": measured_pressure,
                    **sample_meta,
                    "pressure_limit_hpa": pressure_limit,
                    "pressure_limit_exceeded": row_exceeded,
                    "route_opened": route_opened,
                    "sealed": sealed,
                    "pressure_control_started": pressure_control_started,
                    "decision": decision,
                    "failure_reason": failure_reason,
                }
            )

    if limit_exceeded:
        decision = RUN001_FAIL
    else:
        decision = RUN001_NOT_EXECUTED if payload.get("a2_final_decision") == RUN001_NOT_EXECUTED else RUN001_PASS
    deduped_reasons = list(dict.fromkeys(failure_reasons))
    if payload.get("a2_final_decision") == RUN001_FAIL and not deduped_reasons:
        deduped_reasons = [str(payload.get("abort_reason") or payload.get("a2_fail_reason") or "a2_failed")]
    return {
        "schema_version": "run001_a2.preseal_atmosphere_hold.1",
        "artifact_type": "run001_a2_preseal_atmosphere_hold_evidence",
        "run_id": payload.get("run_id"),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "pressure_limit_hpa": pressure_limit,
        "vent_hold_interval_s": payload.get("vent_hold_interval_s"),
        "continuous_atmosphere_hold": payload.get("continuous_atmosphere_hold"),
        "periodic_vent_reassertion_count": periodic_vent_count,
        "route_opened": route_opened,
        "sealed": sealed,
        "pressure_control_started": pressure_control_started,
        "sample_started": sample_started,
        "max_measured_pressure_hpa": max_pressure,
        "pressure_at_abort_hpa": pressure_at_abort,
        "pressure_max_before_abort_hpa": pressure_max_before_abort,
        "pressure_max_after_abort_hpa": pressure_max_after_abort,
        "pressure_max_before_seal_hpa": pressure_max_before_abort,
        "pressure_max_during_safe_stop_hpa": pressure_max_during_safe_stop,
        "preseal_pressure_max_hpa_total": max_pressure,
        "pressure_limit_exceeded": limit_exceeded,
        "decision": decision,
        "failure_reason": "; ".join(str(item) for item in deduped_reasons if str(item)),
        "vent_status_2_is_not_continuous_atmosphere_evidence": True,
        "sample_count": len(samples),
        "samples": samples,
        "not_real_acceptance_evidence": True,
        "v2_replaces_v1_claim": False,
    }


def _build_pressure_read_latency_diagnostics(
    run_dir: str | Path,
    payload: Mapping[str, Any],
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    events = load_workflow_timing_events(Path(run_dir) / WORKFLOW_TIMING_TRACE_FILENAME)
    route_open_start = _first_timing_event(events, "co2_route_open_start")
    route_open_end = _first_timing_event(events, "co2_route_open_end")
    route_open_start_ts = route_open_start.get("timestamp_local") if isinstance(route_open_start, Mapping) else ""
    route_open_end_ts = route_open_end.get("timestamp_local") if isinstance(route_open_end, Mapping) else ""
    route_open_end_mono = _as_float(route_open_end.get("timestamp_monotonic_s")) if isinstance(route_open_end, Mapping) else None
    route_open_end_dt = _parse_trace_ts(route_open_end_ts)
    samples = _collect_pressure_read_latency_samples(run_dir, payload)

    def _is_decision_pressure_sample(sample: Mapping[str, Any]) -> bool:
        if _as_float(sample.get("pressure_hpa")) is None:
            return False
        stage = str(sample.get("stage") or "").lower()
        if stage in {
            "high_pressure_first_point",
            "co2_preseal_atmosphere_hold_pressure_guard",
            "preseal_atmosphere_flush_pressure_check",
            "preseal_atmosphere_flush_ready_handoff",
            "preseal_vent_close_arm_triggered",
            "preseal_vent_close_arm",
            "positive_preseal_pressure_check",
            "positive_preseal_ready",
            "positive_preseal_abort",
            "pressure_rise_detected",
            "route_open_pressure_first_sample",
            "route_open_pressure_surge_detected",
            "route_open_pressure_abort",
            "co2_route_conditioning_at_atmosphere",
            "co2_route_conditioning_pressure_sample",
            "co2_route_conditioning_pressure_warning",
        }:
            return True
        return str(sample.get("result") or "").lower() == "fail"

    first_after_route: Optional[dict[str, Any]] = None
    for sample in samples:
        if not _is_decision_pressure_sample(sample):
            continue
        request_mono = _as_float(sample.get("request_sent_monotonic_s"))
        response_mono = _as_float(sample.get("response_received_monotonic_s"))
        sample_dt = _parse_trace_ts(
            sample.get("request_sent_at")
            or sample.get("response_received_at")
            or sample.get("timestamp")
            or sample.get("sample_recorded_at")
        )
        if route_open_end_mono is None or (request_mono is not None and request_mono >= route_open_end_mono) or (
            request_mono is None and response_mono is not None and response_mono >= route_open_end_mono
        ) or (
            request_mono is None
            and response_mono is None
            and route_open_end_dt is not None
            and sample_dt is not None
            and sample_dt >= route_open_end_dt
        ):
            first_after_route = sample
            break
    count_by_source: dict[str, int] = {}
    max_latency_by_source: dict[str, Optional[float]] = {}
    stale_count_by_source: dict[str, int] = {}
    warnings: list[dict[str, Any]] = []
    disagreement_max: Optional[float] = None
    for sample in samples:
        source = str(sample.get("source") or "unknown")
        count_by_source[source] = count_by_source.get(source, 0) + 1
        latency = _as_float(sample.get("read_latency_s"))
        if latency is not None:
            current = max_latency_by_source.get(source)
            max_latency_by_source[source] = latency if current is None else max(float(current), float(latency))
        else:
            max_latency_by_source.setdefault(source, None)
        if bool(sample.get("is_stale")):
            stale_count_by_source[source] = stale_count_by_source.get(source, 0) + 1
        disagreement = _as_float(sample.get("source_disagreement_hpa"))
        if disagreement is not None:
            disagreement_max = disagreement if disagreement_max is None else max(float(disagreement_max), disagreement)
    digital_samples = [
        sample
        for sample in samples
        if str(sample.get("source") or "") in {"digital_pressure_gauge", "digital_pressure_gauge_continuous"}
    ]
    digital_first = next((sample for sample in digital_samples if _as_float(sample.get("pressure_hpa")) is not None), {})
    latest_frame_ages = [
        _as_float(sample.get("latest_frame_age_s", sample.get("sample_age_s")))
        for sample in digital_samples
        if _as_float(sample.get("latest_frame_age_s", sample.get("sample_age_s"))) is not None
    ]
    latest_frame_intervals = [
        _as_float(sample.get("latest_frame_interval_s"))
        for sample in digital_samples
        if _as_float(sample.get("latest_frame_interval_s")) is not None
    ]
    critical_window_blocking_events = [
        event for event in events if str(event.get("event_name") or "") == "critical_window_blocking_query"
    ]
    critical_window_blocking_query_count = len(critical_window_blocking_events)
    critical_window_blocking_query_total_s = round(
        sum(float(_as_float(event.get("duration_s")) or 0.0) for event in critical_window_blocking_events),
        3,
    )
    for sample in samples:
        count = _as_float(sample.get("critical_window_blocking_query_count"))
        if count is not None:
            critical_window_blocking_query_count = max(critical_window_blocking_query_count, int(count))
        total = _as_float(sample.get("critical_window_blocking_query_total_s"))
        if total is not None:
            critical_window_blocking_query_total_s = max(critical_window_blocking_query_total_s, round(total, 3))
    first_request_mono = _as_float((first_after_route or {}).get("request_sent_monotonic_s"))
    first_response_mono = _as_float((first_after_route or {}).get("response_received_monotonic_s"))
    route_open_to_first_request_s = (
        round(first_request_mono - route_open_end_mono, 3)
        if first_request_mono is not None and route_open_end_mono is not None
        else None
    )
    route_open_to_first_response_s = (
        round(first_response_mono - route_open_end_mono, 3)
        if first_response_mono is not None and route_open_end_mono is not None
        else None
    )
    if first_after_route is None:
        warnings.append({"warning_code": "first_pressure_after_route_open_missing", "warning_only": True})
    if first_after_route and first_after_route.get("error") == "legacy_sample_missing_source_latency_metadata":
        warnings.append(
            {
                "warning_code": "legacy_pressure_sample_missing_source_latency_metadata",
                "warning_only": True,
            }
        )
    if first_after_route and bool(first_after_route.get("is_stale")):
        warnings.append({"warning_code": "first_pressure_sample_stale", "warning_only": True})
    request_expected = _as_float(
        payload.get("high_pressure_first_point_route_open_request_expected_max_s")
        or payload.get("route_open_first_pressure_request_expected_max_s")
    )
    response_expected = _as_float(
        payload.get("high_pressure_first_point_route_open_response_expected_max_s")
        or payload.get("route_open_first_pressure_response_expected_max_s")
    )
    if (
        route_open_to_first_request_s is not None
        and request_expected is not None
        and float(route_open_to_first_request_s) > float(request_expected)
    ):
        warnings.append(
            {
                "warning_code": "route_open_to_first_pressure_request_s_long",
                "actual": route_open_to_first_request_s,
                "expected": request_expected,
                "warning_only": True,
            }
        )
    if (
        route_open_to_first_response_s is not None
        and response_expected is not None
        and float(route_open_to_first_response_s) > float(response_expected)
    ):
        warnings.append(
            {
                "warning_code": "route_open_to_first_pressure_response_s_long",
                "actual": route_open_to_first_response_s,
                "expected": response_expected,
                "warning_only": True,
            }
        )
    latency_warn = _as_float(payload.get("pressure_read_latency_warn_s"))
    for source, latency in max_latency_by_source.items():
        if latency_warn is not None and latency is not None and float(latency) > float(latency_warn):
            warnings.append(
                {
                    "warning_code": "pressure_read_latency_s_long",
                    "source": source,
                    "actual": round(float(latency), 3),
                    "expected": latency_warn,
                    "warning_only": True,
                }
            )
    primary = str(payload.get("primary_pressure_source") or "digital_pressure_gauge")
    first = first_after_route or {}
    diagnostics = {
        "schema_version": "run001_a2.pressure_read_latency_diagnostics.1",
        "artifact_type": "run001_a2_pressure_read_latency_diagnostics",
        "run_id": payload.get("run_id"),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "route_open_started_at": route_open_start_ts,
        "route_open_completed_at": route_open_end_ts,
        "first_pressure_poll_requested_at": first.get("request_sent_at"),
        "first_pressure_response_received_at": first.get("response_received_at") or first.get("timestamp"),
        "route_open_to_first_pressure_request_s": route_open_to_first_request_s,
        "route_open_to_first_pressure_response_s": route_open_to_first_response_s,
        "first_pressure_read_latency_s": first.get("read_latency_s"),
        "first_pressure_source": first.get("source"),
        "first_pressure_hpa": first.get("pressure_hpa"),
        "first_pressure_age_s": first.get("sample_age_s"),
        "first_pressure_is_stale": first.get("is_stale"),
        "digital_gauge_mode": first.get("digital_gauge_mode")
        or digital_first.get("digital_gauge_mode")
        or ("continuous" if str(first.get("source") or "") == "digital_pressure_gauge_continuous" else "query"),
        "digital_gauge_continuous_active": bool(
            first.get("digital_gauge_continuous_active", digital_first.get("digital_gauge_continuous_active", False))
        ),
        "latest_frame_age_s": first.get("latest_frame_age_s")
        if first.get("latest_frame_age_s") is not None
        else first.get("sample_age_s"),
        "latest_frame_interval_s": first.get("latest_frame_interval_s"),
        "latest_frame_sequence_id": first.get("latest_frame_sequence_id")
        or first.get("sequence_id"),
        "critical_window_blocking_query_count": critical_window_blocking_query_count,
        "critical_window_blocking_query_total_s": critical_window_blocking_query_total_s,
        "critical_window_uses_latest_frame": bool(first.get("critical_window_uses_latest_frame"))
        or str(first.get("source") or "") == "digital_pressure_gauge_continuous",
        "critical_window_uses_query": bool(first.get("critical_window_uses_query"))
        or critical_window_blocking_query_count > 0,
        "source_selection_reason": first.get("source_selection_reason"),
        "pace_pressure_hpa": next((sample.get("pressure_hpa") for sample in samples if sample.get("source") == "pace_controller"), None),
        "pace_pressure_latency_s": next((sample.get("read_latency_s") for sample in samples if sample.get("source") == "pace_controller"), None),
        "pace_pressure_age_s": next((sample.get("sample_age_s") for sample in samples if sample.get("source") == "pace_controller"), None),
        "digital_gauge_pressure_hpa": digital_first.get("pressure_hpa"),
        "digital_gauge_latency_s": digital_first.get("read_latency_s"),
        "digital_gauge_age_s": digital_first.get("sample_age_s"),
        "digital_gauge_latest_frame_age_max_s": max(latest_frame_ages) if latest_frame_ages else None,
        "digital_gauge_latest_frame_interval_max_s": max(latest_frame_intervals) if latest_frame_intervals else None,
        "source_disagreement_hpa": disagreement_max,
        "primary_pressure_source": primary,
        "pressure_source_used_for_abort": first.get("pressure_source_used_for_decision") or first.get("source"),
        "pressure_source_used_for_ready": first.get("pressure_source_used_for_decision") or first.get("source"),
        "pressure_source_used_for_seal": first.get("pressure_source_used_for_decision") or first.get("source"),
        "pressure_sample_count_by_source": count_by_source,
        "max_read_latency_by_source": max_latency_by_source,
        "stale_sample_count_by_source": stale_count_by_source,
        "decision": payload.get("a2_final_decision"),
        "warnings": warnings,
        "warning_count": len(warnings),
        "suspected_cause": [
            "pressure_read_latency_or_source_metadata_insufficient" if warnings else "no_pressure_latency_warning",
            "cross_check_pace_controller_and_digital_pressure_gauge_on_next_authorized_run",
        ],
        "not_real_acceptance_evidence": True,
        "v2_replaces_v1_claim": False,
    }
    return diagnostics, samples


def _build_route_open_pressure_surge_evidence(
    run_dir: str | Path,
    payload: Mapping[str, Any],
    *,
    preseal_payload: Optional[Mapping[str, Any]] = None,
    latency_payload: Optional[Mapping[str, Any]] = None,
) -> dict[str, Any]:
    rows = _load_route_trace_rows(run_dir)
    events = load_workflow_timing_events(Path(run_dir) / WORKFLOW_TIMING_TRACE_FILENAME)
    route_open_start = _first_timing_event(events, "co2_route_open_start")
    route_open_end = _first_timing_event(events, "co2_route_open_end")
    route_row = next((row for row in rows if str(row.get("action") or "").lower() == "set_co2_valves"), None)
    route_ts = _parse_trace_ts((route_row or {}).get("ts") or (route_row or {}).get("timestamp"))

    def _is_route_open_pressure_sample(row: Mapping[str, Any]) -> bool:
        action = str(row.get("action") or "").lower()
        if action in {
            "co2_preseal_atmosphere_hold_pressure_guard",
            "preseal_atmosphere_flush_pressure_check",
            "preseal_atmosphere_flush_ready_handoff",
            "positive_preseal_pressure_check",
            "positive_preseal_ready",
            "positive_preseal_abort",
            "pressure_rise_detected",
            "route_open_pressure_first_sample",
            "route_open_pressure_surge_detected",
            "route_open_pressure_abort",
        }:
            return True
        return str(row.get("result") or "").lower() == "fail"

    before_rows: list[Mapping[str, Any]] = []
    after_rows: list[Mapping[str, Any]] = []
    after_pressure_rows: list[Mapping[str, Any]] = []
    abort_row: Optional[Mapping[str, Any]] = None
    for row in rows:
        actual = row.get("actual")
        actual = actual if isinstance(actual, Mapping) else {}
        pressure = _trace_pressure_hpa(actual)
        if pressure is None:
            continue
        ts = _parse_trace_ts(row.get("ts") or row.get("timestamp"))
        if route_ts is not None and ts is not None and ts < route_ts:
            before_rows.append(row)
        else:
            after_rows.append(row)
            if _is_route_open_pressure_sample(row):
                after_pressure_rows.append(row)
        if str(row.get("result") or "").lower() == "fail" and abort_row is None:
            abort_row = row
    before_row = before_rows[-1] if before_rows else None
    first_after = after_pressure_rows[0] if after_pressure_rows else (after_rows[0] if after_rows else None)
    before_actual = before_row.get("actual") if isinstance(before_row, Mapping) else {}
    before_actual = before_actual if isinstance(before_actual, Mapping) else {}
    first_actual = first_after.get("actual") if isinstance(first_after, Mapping) else {}
    first_actual = first_actual if isinstance(first_actual, Mapping) else {}
    abort_actual = abort_row.get("actual") if isinstance(abort_row, Mapping) else {}
    abort_actual = abort_actual if isinstance(abort_actual, Mapping) else {}
    before_pressure = _trace_pressure_hpa(before_actual)
    first_pressure = _trace_pressure_hpa(first_actual)
    pressure_at_route_open = _event_pressure(route_open_end)
    if first_pressure is None:
        first_pressure = pressure_at_route_open
    route_open_ts = _parse_trace_ts((route_row or {}).get("ts") or (route_row or {}).get("timestamp"))
    first_ts = _parse_trace_ts((first_after or {}).get("ts") or (first_after or {}).get("timestamp"))
    route_open_to_first_sample_s = _trace_elapsed_s(first_ts, route_open_ts)
    pressure_delta = (
        round(float(first_pressure) - float(before_pressure), 3)
        if first_pressure is not None and before_pressure is not None
        else None
    )
    pressure_rise_rate = (
        round(float(pressure_delta) / float(route_open_to_first_sample_s), 3)
        if pressure_delta is not None and route_open_to_first_sample_s not in (None, 0)
        else None
    )
    abort_threshold = _as_float(payload.get("positive_preseal_abort_pressure_hpa")) or _as_float(
        payload.get("preseal_atmosphere_hold_pressure_limit_hpa")
    )
    abort_pressure = _trace_pressure_hpa(abort_actual)
    preseal = dict(preseal_payload or {})
    latency = dict(latency_payload or {})
    warning_codes: list[str] = []
    if first_pressure is not None and abort_threshold is not None and float(first_pressure) > float(abort_threshold):
        warning_codes.append("first_sample_after_route_open_exceeded_abort_threshold")
    if pressure_delta is not None and abs(float(pressure_delta)) > 50.0:
        warning_codes.append("pressure_delta_after_route_open_high")
    if pressure_rise_rate is not None and abs(float(pressure_rise_rate)) > 25.0:
        warning_codes.append("pressure_rise_rate_after_route_open_high")
    if latency.get("first_pressure_is_stale"):
        warning_codes.append("first_pressure_sample_stale")
    before_meta = _pressure_sample_meta(before_actual, fallback_source="legacy_route_trace_pressure")
    first_meta = _pressure_sample_meta(first_actual, fallback_source=latency.get("first_pressure_source") or "legacy_route_trace_pressure")
    return {
        "schema_version": "run001_a2.route_open_pressure_surge.1",
        "artifact_type": "run001_a2_route_open_pressure_surge_evidence",
        "run_id": payload.get("run_id"),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "route_open_started_at": (route_open_start or {}).get("timestamp_local") or "",
        "route_open_completed_at": (route_open_end or {}).get("timestamp_local") or (route_row or {}).get("ts") or "",
        "route_open_command_duration_s": (route_open_end or {}).get("duration_s"),
        "pressure_before_route_open_hpa": before_pressure,
        "pressure_before_route_open_source": before_meta.get("pressure_sample_source"),
        "pressure_before_route_open_timestamp": before_meta.get("pressure_sample_timestamp") or (before_row or {}).get("ts"),
        "pressure_before_route_open_age_s": before_meta.get("pressure_sample_age_s"),
        "pressure_first_sample_after_route_open_hpa": first_pressure,
        "pressure_first_sample_after_route_open_source": first_meta.get("pressure_sample_source"),
        "pressure_first_sample_after_route_open_timestamp": first_meta.get("pressure_sample_timestamp") or (first_after or {}).get("ts"),
        "pressure_first_sample_after_route_open_age_s": first_meta.get("pressure_sample_age_s"),
        "route_open_to_first_pressure_sample_s": route_open_to_first_sample_s,
        "pressure_delta_after_route_open_hpa": pressure_delta,
        "pressure_rise_rate_after_route_open_hpa_per_s": pressure_rise_rate,
        "vent_status_at_route_open": before_actual.get("vent_status_raw", before_actual.get("pressure_controller_vent_status")),
        "output_state_at_route_open": before_actual.get("output_state", before_actual.get("pressure_controller_output_state")),
        "isolation_state_at_route_open": before_actual.get("isolation_state", before_actual.get("pressure_controller_isolation_state")),
        "vent_status_at_first_sample": first_actual.get("vent_status_raw", first_actual.get("pressure_controller_vent_status")),
        "output_state_at_first_sample": first_actual.get("output_state", first_actual.get("pressure_controller_output_state")),
        "isolation_state_at_first_sample": first_actual.get("isolation_state", first_actual.get("pressure_controller_isolation_state")),
        "abort_threshold_hpa": abort_threshold,
        "abort_triggered": abort_pressure is not None and abort_threshold is not None and abort_pressure > abort_threshold,
        "abort_trigger_pressure_hpa": abort_pressure,
        "abort_trigger_elapsed_s": _trace_elapsed_s(
            _parse_trace_ts((abort_row or {}).get("ts") or (abort_row or {}).get("timestamp")),
            route_open_ts,
        ),
        "post_abort_pressure_max_hpa": preseal.get("pressure_max_after_abort_hpa"),
        "pre_abort_pressure_max_hpa": preseal.get("pressure_max_before_abort_hpa"),
        "pressure_source_used": latency.get("first_pressure_source") or first_meta.get("pressure_sample_source"),
        "pace_pressure_before_route_open_hpa": before_actual.get("pace_pressure_hpa"),
        "gauge_pressure_before_route_open_hpa": before_actual.get("digital_gauge_pressure_hpa", before_pressure),
        "pace_pressure_first_after_route_open_hpa": first_actual.get("pace_pressure_hpa"),
        "gauge_pressure_first_after_route_open_hpa": first_actual.get("digital_gauge_pressure_hpa", first_pressure),
        "pace_read_latency_s": first_actual.get("pace_pressure_latency_s", latency.get("pace_pressure_latency_s")),
        "gauge_read_latency_s": first_actual.get("digital_gauge_latency_s", latency.get("digital_gauge_latency_s")),
        "pace_sample_age_s": first_actual.get("pace_pressure_age_s", latency.get("pace_pressure_age_s")),
        "gauge_sample_age_s": first_actual.get("digital_gauge_age_s", latency.get("digital_gauge_age_s")),
        "pressure_source_disagreement_hpa": first_actual.get(
            "pressure_source_disagreement_hpa",
            latency.get("source_disagreement_hpa"),
        ),
        "pressure_source_disagreement_warning": bool(
            first_actual.get("pressure_source_disagreement_warning", False)
            or latency.get("source_disagreement_hpa") not in (None, "")
        ),
        "abort_decision_pressure_source": first_actual.get("pressure_source_used_for_abort")
        or latency.get("pressure_source_used_for_abort")
        or latency.get("first_pressure_source"),
        "abort_decision_pressure_age_s": first_meta.get("pressure_sample_age_s"),
        "abort_decision_pressure_latency_s": first_meta.get("read_latency_s") or latency.get("first_pressure_read_latency_s"),
        "decision": payload.get("a2_final_decision"),
        "warning_codes": warning_codes,
        "warning_count": len(warning_codes),
        "suspected_cause": [
            "high_co2_inlet_flow_or_upstream_pressure",
            "vent_exhaust_capacity_may_be_insufficient",
            "pressure_read_latency_or_source_metadata_requires_next_run_confirmation",
        ],
        "recommendations": [
            "reduce_co2_inlet_flow_or_upstream_pressure_before_next_authorized_run",
            "confirm_exhaust_and_bypass_capacity",
            "keep_1150_hpa_abort_threshold_as_hard_fail",
        ],
        "not_real_acceptance_evidence": True,
        "v2_replaces_v1_claim": False,
    }


def _build_high_pressure_first_point_evidence(
    run_dir: str | Path,
    payload: Mapping[str, Any],
    *,
    latency_payload: Optional[Mapping[str, Any]] = None,
    latency_samples: Optional[list[Mapping[str, Any]]] = None,
    timing_summary: Optional[Mapping[str, Any]] = None,
    positive_preseal_payload: Optional[Mapping[str, Any]] = None,
) -> dict[str, Any]:
    events = load_workflow_timing_events(Path(run_dir) / WORKFLOW_TIMING_TRACE_FILENAME)
    latency = dict(latency_payload or {})
    timing = dict(timing_summary or {})
    positive = dict(positive_preseal_payload or {})
    samples = [dict(sample) for sample in list(latency_samples or []) if isinstance(sample, Mapping)]

    def event_state(event: Optional[Mapping[str, Any]]) -> dict[str, Any]:
        if not event:
            return {}
        state = event.get("route_state")
        state = state if isinstance(state, Mapping) else {}
        nested = state.get("route_state")
        return dict(nested if isinstance(nested, Mapping) else state)

    mode_event = _first_timing_event(events, "high_pressure_first_point_mode_enabled")
    mode_state = event_state(mode_event)
    baseline_sample = next(
        (
            sample
            for sample in samples
            if str(sample.get("stage") or "") == "high_pressure_first_point_prearm"
            and str(sample.get("source") or "") == "digital_pressure_gauge"
        ),
        None,
    )
    first_sample = next(
        (
            sample
            for sample in samples
            if str(sample.get("stage") or "") == "high_pressure_first_point"
            and _as_float(sample.get("pressure_hpa")) is not None
        ),
        None,
    )
    route_open_start = _first_timing_event(events, "co2_route_open_start")
    route_open_end = _first_timing_event(events, "co2_route_open_end")
    conditioning_end = _first_timing_event(events, "co2_route_conditioning_end")
    preseal_gate_end = _first_timing_event(events, "preseal_analyzer_gate_end")
    seal_preparation_start = _first_timing_event(events, "seal_preparation_after_conditioning_start")
    vent_off_event = _first_timing_event(events, "seal_preparation_vent_off")
    vent_off_settle_end = _first_timing_event(events, "seal_preparation_vent_off_settle_end")
    ready_wait_start = _first_timing_event(events, "high_pressure_ready_wait_started_after_conditioning")
    ready_event = (
        _first_timing_event(events, "high_pressure_ready_detected_after_conditioning")
        or _first_timing_event(events, "high_pressure_ready_detected")
        or _first_timing_event(events, "preseal_atmosphere_flush_ready_handoff")
        or _first_timing_event(events, "positive_preseal_ready")
    )
    seal_command_event = _first_timing_event(events, "high_pressure_seal_command_sent") or _first_timing_event(
        events,
        "positive_preseal_seal_start",
    )
    seal_confirm_event = _first_timing_event(events, "high_pressure_seal_confirmed") or _first_timing_event(
        events,
        "positive_preseal_seal_end",
    )
    abort_event = (
        _first_timing_event(events, "high_pressure_abort")
        or _first_timing_event(events, "route_open_pressure_abort")
        or _first_timing_event(events, "positive_preseal_abort")
    )
    enabled = bool(
        timing.get("high_pressure_first_point_enabled")
        or mode_state.get("enabled")
        or str((mode_event or {}).get("decision") or "").lower() == "enabled"
        or first_sample is not None
    )
    ambient_pressure = (
        _as_float(mode_state.get("ambient_reference_pressure_hpa"))
        or _as_float(mode_state.get("current_ambient_reference_pressure_hpa"))
        or _as_float(mode_state.get("baseline_pressure_hpa"))
        or _as_float(positive.get("ambient_reference_pressure_hpa"))
    )
    baseline_pressure = _as_float((baseline_sample or {}).get("pressure_hpa")) or _as_float(
        mode_state.get("baseline_pressure_hpa")
    )
    first_pressure = _as_float(latency.get("first_pressure_hpa")) or _as_float(
        (first_sample or {}).get("pressure_hpa")
    )
    abort_pressure = _as_float(payload.get("positive_preseal_abort_pressure_hpa")) or _as_float(
        positive.get("preseal_abort_pressure_hpa")
    )
    abort_triggered = bool(abort_event is not None)
    abort_reason = str(
        (abort_event or {}).get("error_code")
        or (abort_event or {}).get("blocking_condition")
        or event_state(abort_event).get("abort_reason")
        or event_state(abort_event).get("reason")
        or ""
    )
    decision = "abort" if abort_triggered else ("sealed" if seal_confirm_event else str(payload.get("a2_final_decision") or ""))
    conditioning_completed_at = (conditioning_end or {}).get("timestamp_local") or event_state(conditioning_end).get(
        "conditioning_completed_at"
    )
    conditioning_completed_before_high_pressure_mode = bool(
        conditioning_completed_at
        and (
            not mode_event
            or (
                _as_float((conditioning_end or {}).get("timestamp_monotonic_s")) is not None
                and _as_float((mode_event or {}).get("timestamp_monotonic_s")) is not None
                and float(_as_float((mode_event or {}).get("timestamp_monotonic_s")) or 0.0)
                >= float(_as_float((conditioning_end or {}).get("timestamp_monotonic_s")) or 0.0)
            )
        )
    )
    sealed_after_conditioning = bool(
        seal_command_event
        and conditioning_end
        and _as_float((seal_command_event or {}).get("timestamp_monotonic_s")) is not None
        and _as_float((conditioning_end or {}).get("timestamp_monotonic_s")) is not None
        and float(_as_float((seal_command_event or {}).get("timestamp_monotonic_s")) or 0.0)
        >= float(_as_float((conditioning_end or {}).get("timestamp_monotonic_s")) or 0.0)
    )
    return {
        "schema_version": "run001_a2.high_pressure_first_point.1",
        "artifact_type": "run001_a2_high_pressure_first_point_evidence",
        "run_id": payload.get("run_id"),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "enabled": enabled,
        "first_target_pressure_hpa": _as_float(
            mode_state.get("first_target_pressure_hpa")
            or payload.get("high_pressure_first_point_first_target_pressure_hpa")
            or payload.get("positive_preseal_target_pressure_hpa")
        ),
        "ambient_reference_pressure_hpa": ambient_pressure,
        "trigger_reason": str(mode_state.get("trigger_reason") or ("enabled_from_runtime_trace" if enabled else "disabled")),
        "baseline_pressure_sample": baseline_sample or mode_state.get("baseline_pressure_sample"),
        "baseline_pressure_source": (baseline_sample or {}).get("source")
        or mode_state.get("baseline_pressure_source"),
        "baseline_pressure_age_s": _as_float((baseline_sample or {}).get("sample_age_s"))
        or _as_float(mode_state.get("baseline_pressure_age_s")),
        "baseline_pressure_hpa": baseline_pressure,
        "conditioning_completed_before_high_pressure_mode": conditioning_completed_before_high_pressure_mode,
        "conditioning_completed_at": conditioning_completed_at,
        "preseal_analyzer_gate_passed": str((preseal_gate_end or {}).get("decision") or "").upper() in {"PASS", "OK"},
        "seal_preparation_started_at": (seal_preparation_start or {}).get("timestamp_local"),
        "vent_off_sent_at": event_state(vent_off_event).get("vent_off_sent_at") or (vent_off_event or {}).get("timestamp_local"),
        "vent_off_settle_s": event_state(vent_off_settle_end).get("vent_off_settle_s")
        or (vent_off_settle_end or {}).get("duration_s"),
        "high_pressure_ready_wait_started_at": (ready_wait_start or {}).get("timestamp_local"),
        "route_open_started_at": (route_open_start or {}).get("timestamp_local") or latency.get("route_open_started_at"),
        "route_open_completed_at": (route_open_end or {}).get("timestamp_local")
        or latency.get("route_open_completed_at"),
        "first_pressure_request_at": latency.get("first_pressure_poll_requested_at")
        or (first_sample or {}).get("request_sent_at"),
        "first_pressure_response_at": latency.get("first_pressure_response_received_at")
        or (first_sample or {}).get("response_received_at"),
        "route_open_to_first_pressure_request_s": latency.get("route_open_to_first_pressure_request_s")
        if latency.get("route_open_to_first_pressure_request_s") is not None
        else timing.get("route_open_to_first_pressure_request_s"),
        "route_open_to_first_pressure_response_s": latency.get("route_open_to_first_pressure_response_s")
        if latency.get("route_open_to_first_pressure_response_s") is not None
        else timing.get("route_open_to_first_pressure_response_s"),
        "first_pressure_hpa": first_pressure,
        "first_pressure_source": latency.get("first_pressure_source") or (first_sample or {}).get("source"),
        "first_pressure_age_s": latency.get("first_pressure_age_s") if latency.get("first_pressure_age_s") is not None else (first_sample or {}).get("sample_age_s"),
        "first_pressure_stale": bool(
            latency.get("first_pressure_is_stale", (first_sample or {}).get("is_stale", False))
        ),
        "first_pressure_read_latency_s": latency.get("first_pressure_read_latency_s")
        if latency.get("first_pressure_read_latency_s") is not None
        else timing.get("first_pressure_read_latency_s"),
        "ready_pressure_hpa": _as_float(payload.get("positive_preseal_ready_pressure_hpa"))
        or _as_float(positive.get("preseal_ready_pressure_hpa")),
        "ready_reached_at": (ready_event or {}).get("timestamp_local"),
        "seal_command_sent_at": (seal_command_event or {}).get("timestamp_local")
        or positive.get("seal_command_sent_at"),
        "sealed_after_conditioning": sealed_after_conditioning,
        "ready_to_seal_command_s": timing.get("ready_to_seal_command_s")
        if timing.get("ready_to_seal_command_s") is not None
        else positive.get("ready_to_seal_command_s"),
        "seal_confirmed_at": (seal_confirm_event or {}).get("timestamp_local"),
        "pressure_at_seal_hpa": _event_pressure(seal_confirm_event)
        or _as_float(positive.get("current_line_pressure_hpa")),
        "abort_pressure_hpa": abort_pressure,
        "abort_triggered": abort_triggered,
        "abort_reason": abort_reason,
        "decision": decision,
        "warnings": list(timing.get("high_pressure_first_point_warnings") or []),
        "warning_count": timing.get("high_pressure_first_point_warning_count", 0),
        "pace_pressure_role": "cross_check_only_not_safety_decision",
        "primary_safety_pressure_source": "digital_pressure_gauge",
        "not_real_acceptance_evidence": True,
        "v2_replaces_v1_claim": False,
    }


def _build_co2_route_conditioning_evidence(
    run_dir: str | Path,
    payload: Mapping[str, Any],
    *,
    timing_summary: Optional[Mapping[str, Any]] = None,
) -> dict[str, Any]:
    events = load_workflow_timing_events(Path(run_dir) / WORKFLOW_TIMING_TRACE_FILENAME)
    timing = dict(timing_summary or {})

    def event_state(event: Optional[Mapping[str, Any]]) -> dict[str, Any]:
        if not event:
            return {}
        state = event.get("route_state")
        state = state if isinstance(state, Mapping) else {}
        nested = state.get("route_state")
        return dict(nested if isinstance(nested, Mapping) else state)

    start_event = _first_timing_event(events, "co2_route_conditioning_start")
    end_event = _first_timing_event(events, "co2_route_conditioning_end")
    route_open_start = _first_timing_event(events, "co2_route_open_start")
    route_open_end = _first_timing_event(events, "co2_route_open_end")
    seal_event = _first_timing_event(events, "high_pressure_seal_command_sent") or _first_timing_event(
        events,
        "positive_preseal_seal_start",
    )
    vent_ticks = [
        {**event_state(event), "timestamp": event.get("timestamp_local") or event_state(event).get("timestamp")}
        for event in events
        if str(event.get("event_name") or "") == "co2_route_conditioning_vent_tick"
    ]
    pressure_events = [
        event
        for event in events
        if str(event.get("event_name") or "")
        in {"co2_route_conditioning_pressure_sample", "co2_route_conditioning_pressure_warning"}
    ]
    guard_events = [
        event
        for event in events
        if str(event.get("event_name") or "")
        in {
            "co2_route_conditioning_fail_closed",
            "co2_route_conditioning_vent_heartbeat_gap",
            "co2_route_conditioning_route_open_first_vent_gap",
            "co2_route_conditioning_stream_stale",
            "co2_route_conditioning_pressure_overlimit",
            "co2_route_conditioning_vent_command_failed",
        }
    ]
    pressure_values = [
        value
        for value in (_as_float(event.get("pressure_hpa")) for event in pressure_events)
        if value is not None
    ]
    pressure_values.extend(
        value
        for value in (_as_float(item.get("digital_gauge_pressure_hpa")) for item in vent_ticks)
        if value is not None
    )
    start_state = event_state(start_event)
    end_state = event_state(end_event)
    latest_frame_ages = [
        value
        for value in (_as_float(item.get("pressure_sample_age_s") or item.get("latest_frame_age_s")) for item in vent_ticks)
        if value is not None
    ]
    abnormal_events = [
        {**event_state(event), "timestamp": event.get("timestamp_local")}
        for event in pressure_events
        if str(event.get("event_name") or "") == "co2_route_conditioning_pressure_warning"
        or str(event.get("event_type") or "") in {"fail", "warning"}
    ]

    def event_time(event: Optional[Mapping[str, Any]]) -> Optional[float]:
        return _as_float((event or {}).get("timestamp_monotonic_s"))

    vent_tick_events = [
        event for event in events if str(event.get("event_name") or "") == "co2_route_conditioning_vent_tick"
    ]
    vent_gaps: list[float] = []
    previous_vent_time: Optional[float] = None
    for event in vent_tick_events:
        state = event_state(event)
        gap = _as_float(state.get("vent_heartbeat_gap_s", state.get("vent_tick_gap_s")))
        current_time = event_time(event)
        if gap is None and previous_vent_time is not None and current_time is not None:
            gap = round(float(current_time) - float(previous_vent_time), 3)
        if gap is not None:
            vent_gaps.append(float(gap))
        if current_time is not None:
            previous_vent_time = current_time
    route_open_end_time = event_time(route_open_end)
    first_vent_after_route = next(
        (
            event
            for event in vent_tick_events
            if route_open_end_time is None
            or event_time(event) is None
            or float(event_time(event) or 0.0) >= float(route_open_end_time)
        ),
        None,
    )
    route_open_to_first_vent_s = None
    if first_vent_after_route is not None:
        route_open_to_first_vent_s = _as_float(event_state(first_vent_after_route).get("route_open_to_first_vent_s"))
        if route_open_to_first_vent_s is None and route_open_end_time is not None and event_time(first_vent_after_route) is not None:
            route_open_to_first_vent_s = round(float(event_time(first_vent_after_route) or 0.0) - float(route_open_end_time), 3)
    terminal_event = end_event or (guard_events[-1] if guard_events else None)
    terminal_state = event_state(terminal_event)
    all_conditioning_events = vent_tick_events + pressure_events + guard_events
    all_conditioning_states = [event_state(event) for event in all_conditioning_events]
    pressure_overlimit_states = [
        state
        for state in all_conditioning_states
        if bool(state.get("pressure_overlimit_seen"))
        or _as_float(state.get("pressure_overlimit_hpa")) is not None
    ]
    first_pressure_overlimit = pressure_overlimit_states[0] if pressure_overlimit_states else {}
    continuous_stream_stale_seen = any(
        bool(state.get("continuous_stream_stale") or state.get("digital_gauge_stream_stale"))
        for state in all_conditioning_states
    )
    selected_pressure_freshness_values = [
        state.get("selected_pressure_freshness_ok")
        for state in all_conditioning_states
        if state.get("selected_pressure_freshness_ok") is not None
    ]
    selected_pressure_sample_stale_seen = any(
        bool(
            state.get("selected_pressure_sample_is_stale")
            or state.get("pressure_sample_stale")
            or state.get("stream_stale")
        )
        or str(event.get("event_name") or "") == "co2_route_conditioning_stream_stale"
        for event, state in zip(all_conditioning_events, all_conditioning_states)
    )
    selected_pressure_freshness_ok = (
        None
        if not selected_pressure_freshness_values
        else all(bool(value) for value in selected_pressure_freshness_values)
    )
    stream_stale_seen = bool(selected_pressure_sample_stale_seen)
    sequence_progress_values = [
        state.get("digital_gauge_sequence_progress")
        for state in all_conditioning_states
        if state.get("digital_gauge_sequence_progress") is not None
    ]
    digital_gauge_sequence_progress = (
        None if not sequence_progress_values else all(bool(value) for value in sequence_progress_values)
    )
    latest_age_candidates = [
        _as_float(
            state.get("digital_gauge_latest_age_s")
            or state.get("latest_frame_age_s")
            or state.get("pressure_sample_age_s")
            or state.get("sample_age_s")
        )
        for state in all_conditioning_states
    ]
    latest_age_candidates = [value for value in latest_age_candidates if value is not None]
    vent_gap_exceeded = any(
        bool(state.get("vent_heartbeat_gap_exceeded"))
        or str(event.get("event_name") or "")
        in {"co2_route_conditioning_vent_heartbeat_gap", "co2_route_conditioning_route_open_first_vent_gap"}
        for event, state in zip(all_conditioning_events, all_conditioning_states)
    )
    stream_state_at_start = start_state.get("stream_state_at_start")
    stream_state_at_start = stream_state_at_start if isinstance(stream_state_at_start, Mapping) else {}

    def latest_state_value(key: str, default: Any = None) -> Any:
        for state in [terminal_state, end_state] + list(reversed(all_conditioning_states)) + [start_state]:
            if key not in state:
                continue
            value = state.get(key)
            if value is not None and value != "":
                return value
        return default

    sealed_during_conditioning = bool(timing.get("sealed_during_conditioning"))
    if not sealed_during_conditioning and seal_event and start_event:
        seal_time = event_time(seal_event)
        start_time = event_time(start_event)
        end_time = event_time(end_event)
        sealed_during_conditioning = bool(
            seal_time is not None
            and start_time is not None
            and float(seal_time) >= float(start_time)
            and (end_time is None or float(seal_time) <= float(end_time))
        )
    decision = str((end_event or {}).get("decision") or end_state.get("conditioning_decision") or terminal_state.get("conditioning_decision") or "")
    if not decision:
        decision = "FAIL" if abnormal_events or guard_events else ("PASS" if end_event else "NOT_EXECUTED")
    return {
        "schema_version": "run001_a2.co2_route_conditioning.1",
        "artifact_type": "run001_a2_co2_route_conditioning_evidence",
        "run_id": payload.get("run_id"),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "route_open_started_at": (route_open_start or {}).get("timestamp_local"),
        "route_open_completed_at": (route_open_end or {}).get("timestamp_local"),
        "atmosphere_vent_enabled": bool(start_state.get("atmosphere_vent_enabled") or vent_ticks),
        "vent_command_before_route_open": any(str(item.get("phase") or "") == "before_route_open" for item in vent_ticks),
        "conditioning_soak_s": _as_float(start_state.get("conditioning_soak_s") or end_state.get("conditioning_soak_s")),
        "conditioning_started_at": (start_event or {}).get("timestamp_local") or start_state.get("conditioning_started_at"),
        "conditioning_completed_at": (end_event or {}).get("timestamp_local") or end_state.get("conditioning_completed_at"),
        "conditioning_duration_s": timing.get("co2_route_conditioning_duration_s")
        if timing.get("co2_route_conditioning_duration_s") is not None
        else end_state.get("conditioning_duration_s", terminal_state.get("conditioning_duration_s")),
        "pressure_monitoring_enabled": bool(start_state.get("pressure_monitoring_enabled") or pressure_events),
        "pressure_max_during_conditioning_hpa": timing.get("co2_route_conditioning_pressure_max_hpa")
        if timing.get("co2_route_conditioning_pressure_max_hpa") is not None
        else (max(pressure_values) if pressure_values else None),
        "pressure_min_during_conditioning_hpa": timing.get("co2_route_conditioning_pressure_min_hpa")
        if timing.get("co2_route_conditioning_pressure_min_hpa") is not None
        else (min(pressure_values) if pressure_values else None),
        "pressure_source": start_state.get("pressure_source") or "digital_pressure_gauge_continuous",
        "digital_gauge_continuous_mode": latest_state_value(
            "digital_gauge_continuous_mode",
            stream_state_at_start.get("digital_gauge_continuous_mode"),
        ),
        "digital_gauge_continuous_started": bool(
            latest_state_value("digital_gauge_continuous_started", stream_state_at_start.get("stream_started_at"))
        ),
        "digital_gauge_continuous_active": bool(
            latest_state_value(
                "digital_gauge_continuous_active",
                stream_state_at_start.get("digital_gauge_continuous_active"),
            )
        ),
        "digital_gauge_stream_first_frame_at": latest_state_value(
            "digital_gauge_stream_first_frame_at",
            stream_state_at_start.get("stream_first_frame_at"),
        ),
        "digital_gauge_stream_last_frame_at": latest_state_value(
            "digital_gauge_stream_last_frame_at",
            stream_state_at_start.get("stream_last_frame_at"),
        ),
        "continuous_stream_stale": bool(latest_state_value("continuous_stream_stale", continuous_stream_stale_seen)),
        "continuous_stream_age_s": latest_state_value(
            "continuous_stream_age_s",
            latest_state_value("digital_gauge_latest_age_s"),
        ),
        "digital_gauge_stream_stale": bool(
            latest_state_value("digital_gauge_stream_stale", continuous_stream_stale_seen)
        ),
        "digital_gauge_stream_stale_threshold_s": latest_state_value(
            "digital_gauge_stream_stale_threshold_s",
            latest_state_value("digital_gauge_max_age_s"),
        ),
        "digital_gauge_drain_empty_count": latest_state_value(
            "digital_gauge_drain_empty_count",
            stream_state_at_start.get("digital_gauge_drain_empty_count", 0),
        ),
        "digital_gauge_drain_nonempty_count": latest_state_value(
            "digital_gauge_drain_nonempty_count",
            stream_state_at_start.get("digital_gauge_drain_nonempty_count", 0),
        ),
        "latest_frame_age_max_s": end_state.get("latest_frame_age_max_s")
        if end_state.get("latest_frame_age_max_s") is not None
        else (max(latest_frame_ages) if latest_frame_ages else None),
        "digital_gauge_latest_age_s": terminal_state.get("digital_gauge_latest_age_s")
        if terminal_state.get("digital_gauge_latest_age_s") is not None
        else (latest_age_candidates[-1] if latest_age_candidates else None),
        "digital_gauge_latest_sequence_id": latest_state_value(
            "digital_gauge_latest_sequence_id",
            latest_state_value("latest_frame_sequence_id"),
        ),
        "digital_gauge_sequence_progress": digital_gauge_sequence_progress,
        "last_pressure_command": latest_state_value("last_pressure_command"),
        "last_pressure_command_may_cancel_continuous": bool(
            latest_state_value("last_pressure_command_may_cancel_continuous", False)
        ),
        "continuous_interrupted_by_command": bool(
            latest_state_value("continuous_interrupted_by_command", False)
        ),
        "continuous_restart_attempted": bool(latest_state_value("continuous_restart_attempted", False)),
        "continuous_restart_result": latest_state_value("continuous_restart_result", ""),
        "pressure_source_selected": latest_state_value(
            "pressure_source_selected",
            start_state.get("pressure_source_selected"),
        ),
        "pressure_source_selection_reason": latest_state_value(
            "pressure_source_selection_reason",
            start_state.get("pressure_source_selection_reason"),
        ),
        "selected_pressure_source": latest_state_value(
            "selected_pressure_source",
            latest_state_value("pressure_source_selected", start_state.get("pressure_source_selected")),
        ),
        "selected_pressure_sample_age_s": latest_state_value("selected_pressure_sample_age_s"),
        "selected_pressure_sample_is_stale": bool(
            latest_state_value("selected_pressure_sample_is_stale", selected_pressure_sample_stale_seen)
        ),
        "selected_pressure_parse_ok": latest_state_value("selected_pressure_parse_ok"),
        "selected_pressure_freshness_ok": selected_pressure_freshness_ok
        if selected_pressure_freshness_ok is not None
        else latest_state_value("selected_pressure_freshness_ok"),
        "pressure_freshness_decision_source": latest_state_value("pressure_freshness_decision_source"),
        "selected_pressure_fail_closed_reason": latest_state_value("selected_pressure_fail_closed_reason", ""),
        "conditioning_pressure_abort_hpa": (
            terminal_state.get("conditioning_pressure_abort_hpa")
            or end_state.get("conditioning_pressure_abort_hpa")
            or start_state.get("conditioning_pressure_abort_hpa")
        ),
        "pressure_overlimit_seen": bool(pressure_overlimit_states),
        "pressure_overlimit_source": first_pressure_overlimit.get("pressure_overlimit_source")
        or first_pressure_overlimit.get("pressure_sample_source")
        or first_pressure_overlimit.get("source"),
        "pressure_overlimit_hpa": _as_float(
            first_pressure_overlimit.get("pressure_overlimit_hpa", first_pressure_overlimit.get("pressure_hpa"))
        )
        if first_pressure_overlimit
        else None,
        "abnormal_pressure_events": abnormal_events or list(end_state.get("abnormal_pressure_events") or []),
        "conditioning_decision": decision,
        "did_not_seal_during_conditioning": not sealed_during_conditioning,
        "sealed_during_conditioning": sealed_during_conditioning,
        "vent_ticks": vent_ticks,
        "vent_tick_count": len(vent_ticks),
        "vent_tick_avg_gap_s": None if not vent_gaps else round(sum(vent_gaps) / len(vent_gaps), 3),
        "vent_tick_max_gap_s": None if not vent_gaps else round(max(vent_gaps), 3),
        "route_open_to_first_vent_s": route_open_to_first_vent_s
        if route_open_to_first_vent_s is not None
        else terminal_state.get("route_open_to_first_vent_s"),
        "last_vent_command_age_s": terminal_state.get("last_vent_command_age_s")
        or end_state.get("last_vent_command_age_s"),
        "vent_heartbeat_interval_s": start_state.get("vent_heartbeat_interval_s")
        or terminal_state.get("vent_heartbeat_interval_s"),
        "vent_heartbeat_gap_exceeded": vent_gap_exceeded,
        "pressure_monitor_interval_s": start_state.get("pressure_monitor_interval_s")
        or terminal_state.get("pressure_monitor_interval_s"),
        "fail_closed_before_vent_off": bool(
            terminal_state.get("fail_closed_before_vent_off")
            or end_state.get("fail_closed_before_vent_off")
            or guard_events
        ),
        "vent_off_sent_at": terminal_state.get("vent_off_sent_at") or end_state.get("vent_off_sent_at") or "",
        "seal_command_sent": bool(terminal_state.get("seal_command_sent") or end_state.get("seal_command_sent")),
        "sample_count": int(terminal_state.get("sample_count") or end_state.get("sample_count") or payload.get("sample_count") or 0),
        "points_completed": int(
            terminal_state.get("points_completed") or end_state.get("points_completed") or payload.get("points_completed") or 0
        ),
        "stream_stale": stream_stale_seen,
        "periodic_vent_tick_evidence_present": bool(vent_ticks),
        "route_open_allowed": bool(not sealed_during_conditioning and decision.upper() != "FAIL"),
        "route_open_blocked_reason": "" if not sealed_during_conditioning else "sealed_during_conditioning",
        "workflow_summary": {
            "co2_route_conditioning_duration_s": timing.get("co2_route_conditioning_duration_s"),
            "co2_route_conditioning_pressure_warning_count": timing.get(
                "co2_route_conditioning_pressure_warning_count"
            ),
            "co2_route_conditioning_vent_tick_count": timing.get("co2_route_conditioning_vent_tick_count"),
            "co2_route_conditioning_vent_tick_avg_gap_s": timing.get("co2_route_conditioning_vent_tick_avg_gap_s"),
            "co2_route_conditioning_vent_tick_max_gap_s": timing.get("co2_route_conditioning_vent_tick_max_gap_s"),
            "route_open_to_first_vent_s": timing.get("route_open_to_first_vent_s"),
            "vent_heartbeat_gap_exceeded": timing.get("co2_route_conditioning_vent_heartbeat_gap_exceeded"),
            "seal_preparation_started_after_conditioning": timing.get(
                "seal_preparation_started_after_conditioning"
            ),
            "high_pressure_mode_started_after_conditioning": timing.get(
                "high_pressure_mode_started_after_conditioning"
            ),
        },
        "not_real_acceptance_evidence": True,
        "v2_replaces_v1_claim": False,
    }


def _build_critical_pressure_freshness_evidence(
    run_dir: str | Path,
    payload: Mapping[str, Any],
    *,
    pressure_latency_payload: Optional[Mapping[str, Any]] = None,
    timing_summary: Optional[Mapping[str, Any]] = None,
    high_pressure_payload: Optional[Mapping[str, Any]] = None,
) -> dict[str, Any]:
    events = load_workflow_timing_events(Path(run_dir) / WORKFLOW_TIMING_TRACE_FILENAME)
    latency = dict(pressure_latency_payload or {})
    timing = dict(timing_summary or {})
    high_pressure = dict(high_pressure_payload or {})

    def event_state(event: Optional[Mapping[str, Any]]) -> dict[str, Any]:
        if not event:
            return {}
        state = event.get("route_state")
        state = state if isinstance(state, Mapping) else {}
        nested = state.get("route_state")
        return dict(nested if isinstance(nested, Mapping) else state)

    def latest_age_from(event: Optional[Mapping[str, Any]]) -> Optional[float]:
        state = event_state(event)
        return _as_float(
            state.get("latest_frame_age_s")
            or state.get("pressure_sample_age_s")
            or state.get("sample_age_s")
        )

    def latest_sequence_from(event: Optional[Mapping[str, Any]]) -> Any:
        state = event_state(event)
        return (
            state.get("latest_frame_sequence_id")
            or state.get("pressure_sample_sequence_id")
            or state.get("sequence_id")
        )

    stream_start = _first_timing_event(events, "digital_gauge_stream_start")
    stream_first = _first_timing_event(events, "digital_gauge_stream_first_frame")
    route_open_end = _first_timing_event(events, "co2_route_open_end")
    ready_event = (
        _first_timing_event(events, "high_pressure_ready_detected")
        or _first_timing_event(events, "positive_preseal_ready")
        or _first_timing_event(events, "preseal_atmosphere_flush_ready_handoff")
    )
    abort_event = (
        _first_timing_event(events, "high_pressure_abort")
        or _first_timing_event(events, "positive_preseal_abort")
        or _first_timing_event(events, "route_open_pressure_abort")
    )
    seal_command_event = _first_timing_event(events, "high_pressure_seal_command_sent")
    seal_confirm_event = _first_timing_event(events, "high_pressure_seal_confirmed")
    source_event = _first_timing_event(events, "pressure_source_selection") or _first_timing_event(
        events,
        "pressure_source_selected",
    )
    stream_state = event_state(stream_first) or event_state(stream_start)
    route_open_state = event_state(route_open_end)
    route_stream_state = route_open_state.get("digital_gauge_stream")
    route_stream_state = route_stream_state if isinstance(route_stream_state, Mapping) else {}
    latest_before_route = (
        route_stream_state.get("latest_frame")
        if isinstance(route_stream_state.get("latest_frame"), Mapping)
        else high_pressure.get("baseline_pressure_sample")
    )
    source_state = event_state(source_event)
    ready_state = event_state(ready_event)
    abort_state = event_state(abort_event)
    seal_state = event_state(seal_command_event) or event_state(seal_confirm_event)
    stale_count = len(
        [
            event
            for event in events
            if str(event.get("event_name") or "")
            in {"digital_gauge_latest_frame_stale", "pressure_sample_stale"}
        ]
    )
    stale_count = max(stale_count, int(timing.get("stale_pressure_sample_count") or 0))
    blocking_query_count = int(timing.get("critical_window_blocking_query_count") or 0)
    blocking_query_count = max(
        blocking_query_count,
        len([event for event in events if str(event.get("event_name") or "") == "critical_window_blocking_query"]),
    )
    decision = "abort" if abort_event else ("sealed" if seal_confirm_event else str(payload.get("a2_final_decision") or ""))
    return {
        "schema_version": "run001_a2.critical_pressure_freshness.1",
        "artifact_type": "run001_a2_critical_pressure_freshness_evidence",
        "run_id": payload.get("run_id"),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "digital_gauge_continuous_enabled": bool(
            stream_state.get("digital_gauge_continuous_enabled")
            or route_stream_state.get("digital_gauge_continuous_enabled")
            or latency.get("digital_gauge_continuous_active")
            or payload.get("digital_gauge_continuous_enabled")
        ),
        "digital_gauge_continuous_mode": (
            stream_state.get("digital_gauge_continuous_mode")
            or route_stream_state.get("digital_gauge_continuous_mode")
            or payload.get("digital_gauge_continuous_mode")
            or "P4"
        ),
        "stream_started_at": (stream_start or {}).get("timestamp_local") or stream_state.get("stream_started_at"),
        "stream_first_frame_at": (stream_first or {}).get("timestamp_local") or stream_state.get("stream_first_frame_at"),
        "stream_latest_frame_before_route_open": latest_before_route,
        "latest_frame_age_at_route_open_s": _as_float(route_stream_state.get("latest_frame_age_s"))
        if route_stream_state
        else _as_float(high_pressure.get("baseline_pressure_age_s")),
        "latest_frame_age_at_ready_s": latest_age_from(ready_event),
        "latest_frame_age_at_abort_s": latest_age_from(abort_event),
        "latest_frame_age_at_seal_s": latest_age_from(seal_command_event) or latest_age_from(seal_confirm_event),
        "latest_frame_sequence_at_route_open": route_stream_state.get("latest_frame_sequence_id")
        if route_stream_state
        else (latest_before_route or {}).get("pressure_sample_sequence_id")
        if isinstance(latest_before_route, Mapping)
        else None,
        "latest_frame_sequence_at_ready": latest_sequence_from(ready_event),
        "latest_frame_sequence_at_abort": latest_sequence_from(abort_event),
        "pressure_source_used_for_ready": ready_state.get("pressure_source_used_for_ready")
        or timing.get("pressure_source_used_for_ready")
        or source_state.get("pressure_source_used_for_ready"),
        "pressure_source_used_for_abort": abort_state.get("pressure_source_used_for_abort")
        or timing.get("pressure_source_used_for_abort")
        or latency.get("pressure_source_used_for_abort"),
        "pressure_source_used_for_seal": seal_state.get("pressure_source_used_for_seal")
        or timing.get("pressure_source_used_for_seal")
        or source_state.get("pressure_source_used_for_seal"),
        "pace_aux_enabled": bool(source_state.get("pace_aux_enabled", payload.get("pace_aux_enabled", True))),
        "pace_aux_pressure_hpa": source_state.get("pace_pressure_hpa") or latency.get("pace_pressure_hpa"),
        "pace_aux_latency_s": source_state.get("pace_pressure_latency_s") or latency.get("pace_pressure_latency_s"),
        "pace_digital_overlap_samples": source_state.get("pace_digital_overlap_samples"),
        "pace_digital_max_diff_hpa": source_state.get("pace_digital_max_diff_hpa")
        or timing.get("pace_digital_disagreement_max_hpa")
        or latency.get("source_disagreement_hpa"),
        "source_selection_decision": source_state.get("source_selection_reason")
        or latency.get("source_selection_reason"),
        "stale_frame_count": stale_count,
        "blocking_query_count_in_critical_window": blocking_query_count,
        "critical_window_blocking_query_total_s": timing.get("critical_window_blocking_query_total_s")
        or latency.get("critical_window_blocking_query_total_s"),
        "decision": decision,
        "not_real_acceptance_evidence": True,
        "v2_replaces_v1_claim": False,
    }


def _build_positive_preseal_pressurization_evidence(
    run_dir: str | Path,
    payload: Mapping[str, Any],
) -> dict[str, Any]:
    trace_path = Path(run_dir) / "route_trace.jsonl"
    trace_rows: list[dict[str, Any]] = []
    if trace_path.exists():
        for line in trace_path.read_text(encoding="utf-8").splitlines():
            try:
                item = json.loads(line)
            except Exception:
                continue
            if isinstance(item, Mapping):
                trace_rows.append(dict(item))
    ambient_reference = _positive_preseal_ambient_reference_from_trace(trace_rows, payload)
    samples: list[dict[str, Any]] = []
    started = False
    vent_closed_at = ""
    vent_command_result = ""
    ready_reached = False
    ready_reached_at_pressure_hpa: Optional[float] = None
    seal_command_sent = False
    seal_trigger_pressure_hpa: Optional[float] = None
    seal_trigger_elapsed_s: Optional[float] = None
    sealed = False
    pressure_control_started = False
    abort_reason = ""
    decision = RUN001_NOT_EXECUTED
    max_pressure: Optional[float] = None
    latest: dict[str, Any] = {
        "stage": "positive_preseal_pressurization",
        "target_pressure_hpa": payload.get("positive_preseal_target_pressure_hpa"),
        **ambient_reference,
        "preseal_ready_pressure_hpa": payload.get("positive_preseal_ready_pressure_hpa"),
        "ready_pressure_hpa": payload.get("positive_preseal_ready_pressure_hpa"),
        "preseal_abort_pressure_hpa": payload.get("positive_preseal_abort_pressure_hpa"),
        "abort_pressure_hpa": payload.get("positive_preseal_abort_pressure_hpa"),
        "preseal_ready_timeout_s": payload.get("positive_preseal_ready_timeout_s"),
        "preseal_pressure_poll_interval_s": payload.get("positive_preseal_pressure_poll_interval_s"),
        "preseal_vent_close_arm_pressure_hpa": payload.get("preseal_vent_close_arm_pressure_hpa"),
        "preseal_vent_close_arm_margin_hpa": payload.get("preseal_vent_close_arm_margin_hpa"),
        "preseal_vent_close_arm_time_to_ready_s": payload.get("preseal_vent_close_arm_time_to_ready_s"),
        "vent_close_arm_trigger": "",
        "vent_close_arm_pressure_hpa": None,
        "vent_close_arm_elapsed_s": None,
        "estimated_time_to_ready_s": None,
        "ready_reached_before_vent_close_completed": False,
        "ready_reached_during_vent_close": False,
        "ready_to_vent_close_start_s": None,
        "ready_to_vent_close_end_s": None,
        "ready_to_seal_command_s": None,
        "ready_to_seal_confirm_s": None,
        "pressure_delta_during_vent_close_hpa": None,
        "pressure_delta_after_ready_before_seal_hpa": None,
        "vent_closed_at": "",
        "vent_command_result": "",
        "output_state": None,
        "isolation_state": None,
        "elapsed_s": None,
        "pressure_hpa": None,
        "current_line_pressure_hpa": None,
        "positive_preseal_pressure_hpa": None,
        "pressure_sample_source": "",
        "pressure_sample_timestamp": "",
        "pressure_sample_age_s": None,
        "pressure_sample_is_stale": False,
        "pressure_sample_sequence_id": None,
        "pressure_rise_rate_hpa_per_s": None,
        "pressure_samples_count": 0,
        "pressure_max_hpa": None,
        "pressure_min_hpa": None,
        "ready_reached": False,
        "ready_reached_at_pressure_hpa": None,
        "seal_command_sent": False,
        "seal_trigger_pressure_hpa": None,
        "seal_trigger_elapsed_s": None,
        "seal_command_blocked_reason": "",
        "sealed": False,
        "pressure_control_started": False,
        "abort_reason": "",
        "decision": decision,
    }

    def update_latest(row: Mapping[str, Any], actual: Mapping[str, Any]) -> None:
        for key in POSITIVE_PRESEAL_PRESSURIZATION_SAMPLE_FIELDS:
            if key == "measured_atmospheric_pressure_hpa" and "ambient_reference_pressure_hpa" not in actual:
                continue
            if key in actual and actual.get(key) not in (None, ""):
                latest[key] = actual.get(key)
        for key, value in ambient_reference.items():
            if value not in (None, ""):
                latest[key] = value
        latest["timestamp"] = row.get("ts") or row.get("timestamp") or latest.get("timestamp", "")
        pressure = _trace_pressure_hpa(actual)
        if pressure is not None:
            latest["pressure_hpa"] = pressure
            latest["current_line_pressure_hpa"] = actual.get("current_line_pressure_hpa", pressure)
            latest["positive_preseal_pressure_hpa"] = actual.get("positive_preseal_pressure_hpa", pressure)

    if trace_rows:
        previous_pressure: Optional[float] = None
        previous_elapsed: Optional[float] = None
        min_pressure: Optional[float] = None
        for item in trace_rows:
            if not isinstance(item, Mapping):
                continue
            action = str(item.get("action") or "").lower()
            actual = item.get("actual")
            actual = dict(actual) if isinstance(actual, Mapping) else {}
            target = item.get("target")
            target = dict(target) if isinstance(target, Mapping) else {}
            result = str(item.get("result") or "").lower()

            if action == "positive_preseal_pressurization_start":
                started = True
                decision = "START"
                update_latest(item, actual)
            elif action == "set_vent" and target.get("vent_on") is False:
                message = str(item.get("message") or actual.get("reason") or "")
                if "positive" not in message.lower() and "preseal" not in message.lower():
                    continue
                vent_closed_at = str(item.get("ts") or item.get("timestamp") or "")
                vent_command_result = "ok" if result == "ok" else (result or "unknown")
                latest["vent_closed_at"] = vent_closed_at
                latest["vent_command_result"] = vent_command_result
                latest["output_state"] = actual.get("output_state", actual.get("pressure_controller_output_state"))
                latest["isolation_state"] = actual.get("isolation_state", actual.get("pressure_controller_isolation_state"))
            elif action in {
                "positive_preseal_pressure_check",
                "positive_preseal_ready",
                "positive_preseal_abort",
            }:
                update_latest(item, actual)
            elif action in {
                "seal_transition",
                "seal_route",
                "sealed_pressure_control_start",
                "pressure_control_ready_gate",
                "set_pressure",
            }:
                if action == "seal_route":
                    update_latest(item, actual)
                pass
            else:
                continue

            pressure = _trace_pressure_hpa(actual)
            elapsed = _as_float(actual.get("elapsed_s"))
            rise_rate = _as_float(actual.get("pressure_rise_rate_hpa_per_s"))
            pressure_belongs_to_positive_preseal = action in {
                "positive_preseal_pressure_check",
                "positive_preseal_ready",
                "positive_preseal_abort",
            }
            if pressure is not None and pressure_belongs_to_positive_preseal:
                max_pressure = pressure if max_pressure is None else max(max_pressure, pressure)
                min_pressure = pressure if min_pressure is None else min(min_pressure, pressure)
                if rise_rate is None and previous_pressure is not None and previous_elapsed is not None and elapsed is not None:
                    delta_t = float(elapsed) - float(previous_elapsed)
                    if delta_t > 0:
                        rise_rate = (float(pressure) - float(previous_pressure)) / delta_t
                previous_pressure = float(pressure)
                previous_elapsed = elapsed
            if action == "positive_preseal_ready":
                ready_reached = True
                ready_reached_at_pressure_hpa = _as_float(
                    actual.get("ready_reached_at_pressure_hpa", pressure)
                )
                seal_trigger_pressure_hpa = _as_float(actual.get("seal_trigger_pressure_hpa", pressure))
                seal_trigger_elapsed_s = _as_float(actual.get("seal_trigger_elapsed_s", elapsed))
                decision = "READY"
            if action in {"positive_preseal_abort", "sealed_pressure_control_start"} and result == "fail":
                abort_reason = str(actual.get("abort_reason") or actual.get("reason") or item.get("message") or "")
                decision = RUN001_FAIL
            if action == "seal_route":
                seal_command_sent = True
                sealed = result == "ok"
                if sealed and decision != RUN001_FAIL:
                    decision = RUN001_PASS
            if action == "pressure_control_ready_gate" and result == "ok":
                pressure_control_started = bool(actual.get("sealed_route_pressure_control_started", False))
            if action == "set_pressure" and result == "ok":
                pressure_control_started = True
            if action in {"positive_preseal_pressure_check", "positive_preseal_ready", "positive_preseal_abort"}:
                pressure_sample_count = sum(
                    1 for sample in samples if sample.get("pressure_hpa") not in (None, "")
                ) + (1 if pressure is not None else 0)
                row = {
                    **latest,
                    "timestamp": item.get("ts") or item.get("timestamp") or "",
                    "stage": "positive_preseal_pressurization",
                    "elapsed_s": elapsed,
                    "pressure_hpa": pressure,
                    "current_line_pressure_hpa": actual.get("current_line_pressure_hpa", pressure),
                    "positive_preseal_pressure_hpa": actual.get("positive_preseal_pressure_hpa", pressure),
                    "pressure_rise_rate_hpa_per_s": rise_rate,
                    "pressure_samples_count": actual.get("pressure_samples_count", pressure_sample_count),
                    "pressure_max_hpa": actual.get("pressure_max_hpa", max_pressure),
                    "pressure_min_hpa": actual.get("pressure_min_hpa", min_pressure),
                    "ready_reached": ready_reached or bool(actual.get("ready_reached")),
                    "ready_reached_at_pressure_hpa": ready_reached_at_pressure_hpa,
                    "seal_command_sent": seal_command_sent,
                    "seal_trigger_pressure_hpa": seal_trigger_pressure_hpa,
                    "seal_trigger_elapsed_s": seal_trigger_elapsed_s,
                    "sealed": sealed,
                    "pressure_control_started": pressure_control_started,
                    "abort_reason": abort_reason,
                    "decision": decision,
                }
                samples.append({field: row.get(field) for field in POSITIVE_PRESEAL_PRESSURIZATION_SAMPLE_FIELDS})

    if not started:
        decision = RUN001_NOT_EXECUTED
    elif abort_reason:
        decision = RUN001_FAIL
    elif sealed:
        decision = RUN001_PASS
    elif ready_reached:
        decision = "READY"

    latest.update(
        {
            "ready_reached": ready_reached,
            "ready_reached_at_pressure_hpa": ready_reached_at_pressure_hpa,
            "seal_command_sent": seal_command_sent,
            "seal_trigger_pressure_hpa": seal_trigger_pressure_hpa,
            "seal_trigger_elapsed_s": seal_trigger_elapsed_s,
            "sealed": sealed,
            "pressure_control_started": pressure_control_started,
            "abort_reason": abort_reason,
            "decision": decision,
            "pressure_samples_count": sum(
                1 for sample in samples if sample.get("pressure_hpa") not in (None, "")
            ),
            "pressure_max_hpa": max_pressure,
            "pressure_min_hpa": min(
                [
                    float(sample["pressure_hpa"])
                    for sample in samples
                    if sample.get("pressure_hpa") not in (None, "")
                ],
                default=None,
            ),
        }
    )
    return {
        "schema_version": "run001_a2.positive_preseal_pressurization.1",
        "artifact_type": "run001_a2_positive_preseal_pressurization_evidence",
        "run_id": payload.get("run_id"),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "stage": "positive_preseal_pressurization",
        "target_pressure_hpa": latest.get("target_pressure_hpa"),
        "measured_atmospheric_pressure_hpa": latest.get("measured_atmospheric_pressure_hpa"),
        "measured_atmospheric_pressure_source": latest.get("measured_atmospheric_pressure_source"),
        "ambient_reference_pressure_hpa": latest.get("ambient_reference_pressure_hpa"),
        "ambient_reference_source": latest.get("ambient_reference_source"),
        "ambient_reference_timestamp": latest.get("ambient_reference_timestamp"),
        "ambient_reference_age_s": latest.get("ambient_reference_age_s"),
        "current_line_pressure_hpa": latest.get("current_line_pressure_hpa"),
        "positive_preseal_pressure_hpa": latest.get("positive_preseal_pressure_hpa"),
        "pressure_sample_source": latest.get("pressure_sample_source"),
        "pressure_sample_timestamp": latest.get("pressure_sample_timestamp"),
        "pressure_sample_age_s": latest.get("pressure_sample_age_s"),
        "pressure_sample_is_stale": latest.get("pressure_sample_is_stale"),
        "pressure_sample_sequence_id": latest.get("pressure_sample_sequence_id"),
        "ready_pressure_hpa": latest.get("ready_pressure_hpa", latest.get("preseal_ready_pressure_hpa")),
        "abort_pressure_hpa": latest.get("abort_pressure_hpa", latest.get("preseal_abort_pressure_hpa")),
        "preseal_ready_pressure_hpa": latest.get("preseal_ready_pressure_hpa"),
        "preseal_abort_pressure_hpa": latest.get("preseal_abort_pressure_hpa"),
        "preseal_ready_timeout_s": latest.get("preseal_ready_timeout_s"),
        "preseal_pressure_poll_interval_s": latest.get("preseal_pressure_poll_interval_s"),
        "preseal_vent_close_arm_pressure_hpa": latest.get("preseal_vent_close_arm_pressure_hpa"),
        "preseal_vent_close_arm_margin_hpa": latest.get("preseal_vent_close_arm_margin_hpa"),
        "preseal_vent_close_arm_time_to_ready_s": latest.get("preseal_vent_close_arm_time_to_ready_s"),
        "vent_close_arm_trigger": latest.get("vent_close_arm_trigger"),
        "vent_close_arm_pressure_hpa": latest.get("vent_close_arm_pressure_hpa"),
        "vent_close_arm_elapsed_s": latest.get("vent_close_arm_elapsed_s"),
        "estimated_time_to_ready_s": latest.get("estimated_time_to_ready_s"),
        "ready_reached_before_vent_close_completed": bool(
            latest.get("ready_reached_before_vent_close_completed")
        ),
        "ready_reached_during_vent_close": bool(latest.get("ready_reached_during_vent_close")),
        "ready_to_vent_close_start_s": latest.get("ready_to_vent_close_start_s"),
        "ready_to_vent_close_end_s": latest.get("ready_to_vent_close_end_s"),
        "ready_to_seal_command_s": latest.get("ready_to_seal_command_s"),
        "ready_to_seal_confirm_s": latest.get("ready_to_seal_confirm_s"),
        "pressure_delta_during_vent_close_hpa": latest.get("pressure_delta_during_vent_close_hpa"),
        "pressure_delta_after_ready_before_seal_hpa": latest.get("pressure_delta_after_ready_before_seal_hpa"),
        "vent_closed_at": vent_closed_at or latest.get("vent_closed_at"),
        "vent_command_result": vent_command_result or latest.get("vent_command_result"),
        "output_state": latest.get("output_state"),
        "isolation_state": latest.get("isolation_state"),
        "elapsed_s": latest.get("elapsed_s"),
        "pressure_hpa": latest.get("pressure_hpa"),
        "pressure_rise_rate_hpa_per_s": latest.get("pressure_rise_rate_hpa_per_s"),
        "pressure_samples_count": latest.get("pressure_samples_count", len(samples)),
        "pressure_max_hpa": latest.get("pressure_max_hpa", max_pressure),
        "pressure_min_hpa": latest.get("pressure_min_hpa"),
        "ready_reached": ready_reached,
        "ready_reached_at_pressure_hpa": ready_reached_at_pressure_hpa,
        "seal_command_sent": seal_command_sent,
        "seal_trigger_pressure_hpa": seal_trigger_pressure_hpa,
        "seal_trigger_elapsed_s": seal_trigger_elapsed_s,
        "seal_command_blocked_reason": latest.get("seal_command_blocked_reason"),
        "sealed": sealed,
        "pressure_control_started": pressure_control_started,
        "abort_reason": abort_reason,
        "decision": decision,
        "max_measured_pressure_hpa": max_pressure,
        "pressure_max_hpa": latest.get("pressure_max_hpa", max_pressure),
        "pressure_min_hpa": latest.get("pressure_min_hpa"),
        "sample_count": len(samples),
        "samples": samples,
        "not_real_acceptance_evidence": True,
        "v2_replaces_v1_claim": False,
    }


def _first_timing_event(events: list[Mapping[str, Any]], name: str) -> Optional[Mapping[str, Any]]:
    return next((event for event in events if str(event.get("event_name") or "") == name), None)


def _last_timing_event(events: list[Mapping[str, Any]], name: str) -> Optional[Mapping[str, Any]]:
    return next((event for event in reversed(events) if str(event.get("event_name") or "") == name), None)


def _event_ts(event: Optional[Mapping[str, Any]]) -> Optional[datetime]:
    if not event:
        return None
    return _parse_trace_ts(event.get("timestamp_local"))


def _event_pressure(event: Optional[Mapping[str, Any]]) -> Optional[float]:
    if not event:
        return None
    return _as_float(event.get("pressure_hpa"))


def _timing_duration(events: list[Mapping[str, Any]], start_name: str, end_name: str) -> Optional[float]:
    start = _first_timing_event(events, start_name)
    end = _first_timing_event(events, end_name)
    explicit = _as_float((end or {}).get("duration_s"))
    if explicit is not None:
        return round(max(0.0, explicit), 3)
    return _trace_elapsed_s(_event_ts(end), _event_ts(start))


def _build_positive_preseal_timing_diagnostics(
    run_dir: str | Path,
    payload: Mapping[str, Any],
    *,
    timing_summary: Optional[Mapping[str, Any]] = None,
) -> dict[str, Any]:
    trace_rows = _load_route_trace_rows(run_dir)
    timing_events = load_workflow_timing_events(Path(run_dir) / WORKFLOW_TIMING_TRACE_FILENAME)
    thresholds = {
        key: payload.get(key)
        for key in (
            "pressure_rise_detection_threshold_hpa",
            "expected_route_open_to_first_pressure_rise_max_s",
            "expected_route_open_to_ready_max_s",
            "expected_positive_preseal_to_ready_max_s",
            "expected_ready_to_seal_command_max_s",
            "expected_ready_to_seal_confirm_max_s",
            "expected_max_pressure_increase_after_ready_hpa",
            "expected_vent_hold_tick_interval_s",
            "expected_vent_hold_pressure_rise_rate_max_hpa_per_s",
            "expected_abort_margin_min_hpa",
            "preseal_vent_close_arm_pressure_hpa",
            "preseal_vent_close_arm_margin_hpa",
            "preseal_vent_close_arm_time_to_ready_s",
            "timing_warning_only",
        )
    }
    pressure_rise_threshold = _as_float(thresholds.get("pressure_rise_detection_threshold_hpa")) or 2.0
    route_open_row: Optional[Mapping[str, Any]] = None
    positive_start_row: Optional[Mapping[str, Any]] = None
    positive_ready_row: Optional[Mapping[str, Any]] = None
    handoff_ready_row: Optional[Mapping[str, Any]] = None
    arm_row: Optional[Mapping[str, Any]] = None
    vent_close_row: Optional[Mapping[str, Any]] = None
    seal_row: Optional[Mapping[str, Any]] = None
    positive_abort_row: Optional[Mapping[str, Any]] = None
    preseal_abort_row: Optional[Mapping[str, Any]] = None
    pressure_samples: list[dict[str, Any]] = []
    vent_hold_samples: list[dict[str, Any]] = []
    positive_samples: list[dict[str, Any]] = []

    for row in trace_rows:
        action = str(row.get("action") or "").lower()
        actual = row.get("actual")
        actual = dict(actual) if isinstance(actual, Mapping) else {}
        target = row.get("target")
        target = dict(target) if isinstance(target, Mapping) else {}
        ts_text = str(row.get("ts") or row.get("timestamp") or "")
        ts = _parse_trace_ts(ts_text)
        pressure = _trace_pressure_hpa(actual)
        if action == "set_co2_valves" and route_open_row is None:
            route_open_row = row
        if action == "positive_preseal_pressurization_start" and positive_start_row is None:
            positive_start_row = row
        if action == "positive_preseal_ready" and positive_ready_row is None:
            positive_ready_row = row
        if action == "preseal_atmosphere_flush_ready_handoff" and handoff_ready_row is None:
            handoff_ready_row = row
        if action == "preseal_vent_close_arm_triggered" and arm_row is None:
            arm_row = row
        if action == "set_vent" and target.get("vent_on") is False and vent_close_row is None:
            message = str(row.get("message") or actual.get("reason") or "")
            if "positive" in message.lower() and "preseal" in message.lower():
                vent_close_row = row
        if action == "seal_route" and seal_row is None:
            seal_row = row
        if action == "positive_preseal_abort" and positive_abort_row is None:
            positive_abort_row = row
        if (
            action == "co2_preseal_atmosphere_hold_pressure_guard"
            and str(row.get("result") or "").lower() == "fail"
            and preseal_abort_row is None
        ):
            preseal_abort_row = row
        if pressure is not None:
            sample = {
                "timestamp": ts_text,
                "parsed_ts": ts,
                "action": action,
                "pressure_hpa": pressure,
                "elapsed_s": _as_float(actual.get("elapsed_s")),
            }
            pressure_samples.append(sample)
            route_open_ts = _parse_trace_ts((route_open_row or {}).get("ts") or (route_open_row or {}).get("timestamp"))
            positive_start_ts = _parse_trace_ts(
                (positive_start_row or {}).get("ts") or (positive_start_row or {}).get("timestamp")
            )
            if route_open_ts is not None and ts is not None and (positive_start_ts is None or ts <= positive_start_ts):
                if action == "set_vent" and target.get("vent_on") is True:
                    vent_hold_samples.append(sample)
                elif action in {
                    "co2_preseal_atmosphere_hold_pressure_guard",
                    "preseal_atmosphere_flush_ready_handoff",
                }:
                    vent_hold_samples.append(sample)
            if action in {"positive_preseal_pressure_check", "positive_preseal_ready", "positive_preseal_abort"}:
                positive_samples.append(sample)

    route_open_start_event = _first_timing_event(timing_events, "co2_route_open_start")
    route_open_end_event = _first_timing_event(timing_events, "co2_route_open_end")
    positive_start_event = _first_timing_event(timing_events, "positive_preseal_pressurization_start")
    positive_ready_event = _first_timing_event(timing_events, "positive_preseal_ready")
    vent_close_start_event = _first_timing_event(timing_events, "positive_preseal_vent_close_start")
    vent_close_end_event = _first_timing_event(timing_events, "positive_preseal_vent_close_end")
    seal_start_event = _first_timing_event(timing_events, "positive_preseal_seal_start")
    seal_end_event = _first_timing_event(timing_events, "positive_preseal_seal_end")
    arm_event = _first_timing_event(timing_events, "preseal_vent_close_arm_triggered")

    route_open_ts = _event_ts(route_open_end_event) or _parse_trace_ts(
        (route_open_row or {}).get("ts") or (route_open_row or {}).get("timestamp")
    )
    route_opened_at = route_open_ts.isoformat() if route_open_ts is not None else ""
    co2_route_open_elapsed_s = _timing_duration(timing_events, "co2_route_open_start", "co2_route_open_end")
    route_open_actual = (route_open_row or {}).get("actual")
    route_open_actual = dict(route_open_actual) if isinstance(route_open_actual, Mapping) else {}
    pressure_at_route_open_hpa = _trace_pressure_hpa(route_open_actual) or _event_pressure(route_open_end_event)
    if pressure_at_route_open_hpa is None and pressure_samples:
        pressure_at_route_open_hpa = pressure_samples[0]["pressure_hpa"]

    first_pressure_sample = pressure_samples[0] if pressure_samples else {}
    first_pressure_sample_ts = first_pressure_sample.get("parsed_ts") if first_pressure_sample else None
    baseline_pressure = pressure_at_route_open_hpa
    rise_sample: dict[str, Any] = {}
    if baseline_pressure is not None:
        for sample in pressure_samples:
            sample_ts = sample.get("parsed_ts")
            if route_open_ts is not None and sample_ts is not None and sample_ts < route_open_ts:
                continue
            delta = float(sample["pressure_hpa"]) - float(baseline_pressure)
            if delta >= pressure_rise_threshold:
                rise_sample = {**sample, "delta_hpa": round(delta, 3)}
                break
    ready_candidates = [row for row in (handoff_ready_row, positive_ready_row) if row is not None]
    ready_row = min(
        ready_candidates,
        key=lambda row: _parse_trace_ts(row.get("ts") or row.get("timestamp")) or datetime.max.replace(tzinfo=timezone.utc),
    ) if ready_candidates else None
    ready_actual = (ready_row or {}).get("actual")
    ready_actual = dict(ready_actual) if isinstance(ready_actual, Mapping) else {}
    arm_actual = (arm_row or {}).get("actual")
    arm_actual = dict(arm_actual) if isinstance(arm_actual, Mapping) else {}
    if not arm_actual and isinstance((arm_event or {}).get("route_state"), Mapping):
        arm_actual = dict((arm_event or {}).get("route_state") or {})
    if not arm_actual and isinstance((arm_event or {}).get("details"), Mapping):
        details = dict((arm_event or {}).get("details") or {})
        route_state = details.get("route_state")
        if isinstance(route_state, Mapping):
            arm_actual = dict(route_state)
    positive_ready_actual = (positive_ready_row or {}).get("actual")
    positive_ready_actual = dict(positive_ready_actual) if isinstance(positive_ready_actual, Mapping) else {}
    ready_ts = _parse_trace_ts((ready_row or {}).get("ts") or (ready_row or {}).get("timestamp"))
    arm_ts = _event_ts(arm_event) or _parse_trace_ts((arm_row or {}).get("ts") or (arm_row or {}).get("timestamp"))
    positive_ready_ts = _event_ts(positive_ready_event) or _parse_trace_ts(
        (positive_ready_row or {}).get("ts") or (positive_ready_row or {}).get("timestamp")
    )
    positive_start_ts = _event_ts(positive_start_event) or _parse_trace_ts(
        (positive_start_row or {}).get("ts") or (positive_start_row or {}).get("timestamp")
    )
    positive_start_pressure = _trace_pressure_hpa(
        dict((positive_start_row or {}).get("actual") or {}) if isinstance((positive_start_row or {}).get("actual"), Mapping) else {}
    )
    if positive_start_pressure is None and positive_samples:
        positive_start_pressure = positive_samples[0]["pressure_hpa"]
    ready_pressure_hpa = (
        _as_float(ready_actual.get("ready_pressure_hpa"))
        or _as_float(ready_actual.get("preseal_ready_pressure_hpa"))
        or _as_float(payload.get("positive_preseal_ready_pressure_hpa"))
    )
    seal_trigger_pressure = _as_float(positive_ready_actual.get("seal_trigger_pressure_hpa")) or _trace_pressure_hpa(
        positive_ready_actual
    )
    positive_ready_pressure = _trace_pressure_hpa(positive_ready_actual) or seal_trigger_pressure

    preseal_abort_ts = _parse_trace_ts((preseal_abort_row or {}).get("ts") or (preseal_abort_row or {}).get("timestamp"))
    if preseal_abort_ts is not None:
        vent_hold_samples = [
            sample
            for sample in vent_hold_samples
            if sample.get("parsed_ts") is None or sample.get("parsed_ts") <= preseal_abort_ts
        ]
    vent_hold_start_ts = vent_hold_samples[0]["parsed_ts"] if vent_hold_samples else None
    vent_hold_end_ts = vent_hold_samples[-1]["parsed_ts"] if vent_hold_samples else None
    pressure_at_vent_hold_start_hpa = vent_hold_samples[0]["pressure_hpa"] if vent_hold_samples else None
    pressure_at_vent_hold_end_hpa = vent_hold_samples[-1]["pressure_hpa"] if vent_hold_samples else None
    vent_hold_duration_s = _trace_elapsed_s(vent_hold_end_ts, vent_hold_start_ts)
    vent_hold_pressure_delta_hpa = (
        round(float(pressure_at_vent_hold_end_hpa) - float(pressure_at_vent_hold_start_hpa), 3)
        if pressure_at_vent_hold_start_hpa is not None and pressure_at_vent_hold_end_hpa is not None
        else None
    )
    vent_hold_pressure_rise_rate_hpa_per_s = (
        round(float(vent_hold_pressure_delta_hpa) / float(vent_hold_duration_s), 3)
        if vent_hold_pressure_delta_hpa is not None and vent_hold_duration_s and vent_hold_duration_s > 0
        else None
    )
    vent_hold_tick_count = len(
        [
            row
            for row in trace_rows
            if str(row.get("action") or "").lower() == "set_vent"
            and isinstance(row.get("target"), Mapping)
            and dict(row.get("target") or {}).get("vent_on") is True
            and route_open_ts is not None
            and (_parse_trace_ts(row.get("ts") or row.get("timestamp")) or route_open_ts) >= route_open_ts
            and (
                preseal_abort_ts is None
                or (_parse_trace_ts(row.get("ts") or row.get("timestamp")) or route_open_ts) <= preseal_abort_ts
            )
        ]
    )

    vent_close_command_ts = _event_ts(vent_close_start_event) or _parse_trace_ts(
        (vent_close_row or {}).get("ts") or (vent_close_row or {}).get("timestamp")
    )
    vent_close_confirmed_ts = _event_ts(vent_close_end_event) or _parse_trace_ts(
        (vent_close_row or {}).get("ts") or (vent_close_row or {}).get("timestamp")
    )
    vent_close_duration_s = (
        _as_float((timing_summary or {}).get("positive_preseal_vent_close_duration_s"))
        or _timing_duration(timing_events, "positive_preseal_vent_close_start", "positive_preseal_vent_close_end")
        or _trace_elapsed_s(vent_close_confirmed_ts, vent_close_command_ts)
    )
    arm_elapsed_s = _trace_elapsed_s(arm_ts, route_open_ts)
    time_from_route_open_to_ready_s = _trace_elapsed_s(ready_ts, route_open_ts)
    time_from_positive_preseal_start_to_ready_s = _trace_elapsed_s(positive_ready_ts, positive_start_ts)
    positive_preseal_pressure_rise_rate_hpa_per_s = (
        round((float(positive_ready_pressure) - float(positive_start_pressure)) / float(time_from_positive_preseal_start_to_ready_s), 3)
        if positive_ready_pressure is not None
        and positive_start_pressure is not None
        and time_from_positive_preseal_start_to_ready_s
        and time_from_positive_preseal_start_to_ready_s > 0
        else None
    )
    seal_command_ts = _event_ts(seal_start_event) or _parse_trace_ts((seal_row or {}).get("ts") or (seal_row or {}).get("timestamp"))
    seal_confirm_ts = _event_ts(seal_end_event) or _parse_trace_ts((seal_row or {}).get("ts") or (seal_row or {}).get("timestamp"))
    seal_actual = (seal_row or {}).get("actual")
    seal_actual = dict(seal_actual) if isinstance(seal_actual, Mapping) else {}
    pressure_at_seal_confirm_hpa = _event_pressure(seal_end_event) or _trace_pressure_hpa(seal_actual)
    seal_command_latency_after_ready_s = _trace_elapsed_s(seal_command_ts, positive_ready_ts)
    seal_confirm_latency_after_ready_s = _trace_elapsed_s(seal_confirm_ts, positive_ready_ts)
    ready_to_vent_close_start_s = _trace_elapsed_s(vent_close_command_ts, ready_ts)
    ready_to_vent_close_end_s = _trace_elapsed_s(vent_close_confirmed_ts, ready_ts)
    pressure_max_before_seal_hpa: Optional[float] = None
    for sample in pressure_samples:
        sample_ts = sample.get("parsed_ts")
        if seal_confirm_ts is not None and sample_ts is not None and sample_ts > seal_confirm_ts:
            continue
        if preseal_abort_ts is not None and sample_ts is not None and sample_ts > preseal_abort_ts:
            continue
        pressure_max_before_seal_hpa = (
            sample["pressure_hpa"]
            if pressure_max_before_seal_hpa is None
            else max(float(pressure_max_before_seal_hpa), float(sample["pressure_hpa"]))
        )
    if pressure_max_before_seal_hpa is None:
        pressure_max_before_seal_hpa = _as_float(payload.get("positive_preseal_pressure_max_hpa"))
    pressure_increase_after_ready_before_seal_hpa = None
    if seal_trigger_pressure is not None:
        if pressure_at_seal_confirm_hpa is not None:
            pressure_increase_after_ready_before_seal_hpa = round(
                float(pressure_at_seal_confirm_hpa) - float(seal_trigger_pressure),
                3,
            )
        elif pressure_max_before_seal_hpa is not None:
            pressure_increase_after_ready_before_seal_hpa = round(
                float(pressure_max_before_seal_hpa) - float(seal_trigger_pressure),
                3,
            )
    abort_pressure_hpa = _as_float(payload.get("positive_preseal_abort_pressure_hpa")) or _as_float(
        ready_actual.get("abort_pressure_hpa")
    )
    abort_margin_min_hpa = (
        round(float(abort_pressure_hpa) - float(pressure_max_before_seal_hpa), 3)
        if abort_pressure_hpa is not None and pressure_max_before_seal_hpa is not None
        else None
    )
    pressure_delta_during_vent_close_hpa = (
        round(float(positive_start_pressure) - float(arm_actual.get("vent_close_arm_pressure_hpa")), 3)
        if positive_start_pressure is not None and _as_float(arm_actual.get("vent_close_arm_pressure_hpa")) is not None
        else None
    )
    pressure_delta_after_ready_before_seal_hpa = pressure_increase_after_ready_before_seal_hpa
    ready_reached_before_vent_close_completed = (
        ready_ts is not None
        and vent_close_confirmed_ts is not None
        and ready_ts <= vent_close_confirmed_ts
    )
    ready_reached_during_vent_close = (
        ready_ts is not None
        and vent_close_command_ts is not None
        and vent_close_confirmed_ts is not None
        and vent_close_command_ts <= ready_ts <= vent_close_confirmed_ts
    )
    seal_command_blocked_reason = ""
    if ready_ts is not None and seal_start_event is None and seal_row is None:
        seal_command_blocked_reason = (
            str(((positive_abort_row or {}).get("actual") or {}).get("abort_reason") or "")
            if isinstance((positive_abort_row or {}).get("actual"), Mapping)
            else ""
        ) or "ready_without_seal_start"

    warnings: list[dict[str, Any]] = []

    def add_warning(code: str, *, actual: Any = None, expected: Any = None, detail: str = "") -> None:
        if isinstance(code, Mapping):
            warning_code = str(code.get("warning_code") or "").strip()
            payload = dict(code)
        else:
            warning_code = str(code or "").strip()
            payload = {
                "warning_code": warning_code,
                "actual": actual,
                "expected": expected,
                "detail": detail,
                "warning_only": bool(thresholds.get("timing_warning_only", True)),
            }
        if not warning_code:
            return
        if any(str(item.get("warning_code") or "") == warning_code for item in warnings):
            return
        payload["warning_code"] = warning_code
        payload.setdefault("warning_only", bool(thresholds.get("timing_warning_only", True)))
        warnings.append(payload)

    first_rise_elapsed_s = _trace_elapsed_s(rise_sample.get("parsed_ts"), route_open_ts) if rise_sample else None
    checks = [
        (
            "route_open_to_first_pressure_rise_s_long",
            first_rise_elapsed_s,
            _as_float(thresholds.get("expected_route_open_to_first_pressure_rise_max_s")),
        ),
        (
            "route_open_to_ready_s_long",
            time_from_route_open_to_ready_s,
            _as_float(thresholds.get("expected_route_open_to_ready_max_s")),
        ),
        (
            "positive_preseal_start_to_ready_s_long",
            time_from_positive_preseal_start_to_ready_s,
            _as_float(thresholds.get("expected_positive_preseal_to_ready_max_s")),
        ),
        (
            "vent_hold_pressure_rise_rate_high",
            vent_hold_pressure_rise_rate_hpa_per_s,
            _as_float(thresholds.get("expected_vent_hold_pressure_rise_rate_max_hpa_per_s")),
        ),
        (
            "ready_to_seal_command_s_long",
            seal_command_latency_after_ready_s,
            _as_float(thresholds.get("expected_ready_to_seal_command_max_s")),
        ),
        (
            "ready_to_seal_confirm_s_long",
            seal_confirm_latency_after_ready_s,
            _as_float(thresholds.get("expected_ready_to_seal_confirm_max_s")),
        ),
        (
            "pressure_increase_after_ready_before_seal_hpa_high",
            pressure_increase_after_ready_before_seal_hpa,
            _as_float(thresholds.get("expected_max_pressure_increase_after_ready_hpa")),
        ),
    ]
    for code, actual_value, expected_value in checks:
        if actual_value is not None and expected_value is not None and float(actual_value) > float(expected_value):
            add_warning(code, actual=actual_value, expected=expected_value)
    if (
        vent_hold_pressure_rise_rate_hpa_per_s is not None
        and _as_float(thresholds.get("expected_vent_hold_pressure_rise_rate_max_hpa_per_s")) is not None
        and float(vent_hold_pressure_rise_rate_hpa_per_s)
        > float(_as_float(thresholds.get("expected_vent_hold_pressure_rise_rate_max_hpa_per_s")))
        and (ready_ts is None or time_from_route_open_to_ready_s is None)
    ):
        add_warning(
            "vent_hold_fast_pressure_rise_without_handoff",
            actual=vent_hold_pressure_rise_rate_hpa_per_s,
            expected=thresholds.get("expected_vent_hold_pressure_rise_rate_max_hpa_per_s"),
        )
    if abort_margin_min_hpa is not None and float(abort_margin_min_hpa) <= float(
        _as_float(thresholds.get("expected_abort_margin_min_hpa")) or 10.0
    ):
        add_warning(
            "pressure_max_before_seal_near_abort_threshold",
            actual=abort_margin_min_hpa,
            expected=thresholds.get("expected_abort_margin_min_hpa"),
        )
    expected_tick_interval = _as_float(thresholds.get("expected_vent_hold_tick_interval_s"))
    if expected_tick_interval is not None and vent_hold_duration_s is not None and expected_tick_interval > 0:
        expected_tick_count = int(vent_hold_duration_s / expected_tick_interval) + 1
        if abs(vent_hold_tick_count - expected_tick_count) > 1:
            add_warning(
                "vent_hold_tick_count_interval_mismatch",
                actual=vent_hold_tick_count,
                expected=expected_tick_count,
                detail=f"duration_s={vent_hold_duration_s} interval_s={expected_tick_interval}",
            )
    if ready_ts is not None and seal_start_event is None and seal_row is None:
        add_warning("positive_preseal_ready_without_seal_start")
    if seal_start_event is not None and seal_end_event is None and seal_row is None:
        add_warning("positive_preseal_seal_start_without_seal_end")

    summary_abnormal = (timing_summary or {}).get("abnormal_waits")
    if isinstance(summary_abnormal, list):
        for item in summary_abnormal:
            if isinstance(item, Mapping):
                add_warning(item)
    abnormal_waits = list(warnings)
    decision = str(
        ((positive_abort_row or {}).get("actual") or {}).get("decision")
        if isinstance((positive_abort_row or {}).get("actual"), Mapping)
        else ""
    ) or str(payload.get("positive_preseal_pressurization_decision") or payload.get("a2_final_decision") or "")
    return {
        "schema_version": "run001_a2.positive_preseal_timing_diagnostics.1",
        "artifact_type": "run001_a2_positive_preseal_timing_diagnostics",
        "run_id": payload.get("run_id"),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "co2_route_opened_at": route_opened_at,
        "co2_route_open_elapsed_s": co2_route_open_elapsed_s,
        "first_pressure_sample_at": str(first_pressure_sample.get("timestamp") or ""),
        "first_pressure_sample_hpa": first_pressure_sample.get("pressure_hpa"),
        "first_pressure_rise_detected_at": str(rise_sample.get("timestamp") or ""),
        "first_pressure_rise_detected_elapsed_s": first_rise_elapsed_s,
        "first_pressure_rise_delta_hpa": rise_sample.get("delta_hpa"),
        "pressure_rise_detection_threshold_hpa": pressure_rise_threshold,
        "preseal_vent_close_arm_pressure_hpa": (
            _as_float(arm_actual.get("preseal_vent_close_arm_pressure_hpa"))
            or _as_float(thresholds.get("preseal_vent_close_arm_pressure_hpa"))
        ),
        "preseal_vent_close_arm_margin_hpa": (
            _as_float(arm_actual.get("preseal_vent_close_arm_margin_hpa"))
            or _as_float(thresholds.get("preseal_vent_close_arm_margin_hpa"))
        ),
        "preseal_vent_close_arm_time_to_ready_s": (
            _as_float(arm_actual.get("preseal_vent_close_arm_time_to_ready_s"))
            or _as_float(thresholds.get("preseal_vent_close_arm_time_to_ready_s"))
        ),
        "vent_close_arm_trigger": str(arm_actual.get("vent_close_arm_trigger") or ""),
        "vent_close_arm_pressure_hpa": _as_float(arm_actual.get("vent_close_arm_pressure_hpa")),
        "vent_close_arm_elapsed_s": arm_elapsed_s,
        "estimated_time_to_ready_s": _as_float(arm_actual.get("estimated_time_to_ready_s")),
        "pressure_at_route_open_hpa": pressure_at_route_open_hpa,
        "pressure_at_vent_hold_start_hpa": pressure_at_vent_hold_start_hpa,
        "pressure_at_vent_hold_end_hpa": pressure_at_vent_hold_end_hpa,
        "vent_hold_duration_s": vent_hold_duration_s,
        "vent_hold_pressure_delta_hpa": vent_hold_pressure_delta_hpa,
        "vent_hold_pressure_rise_rate_hpa_per_s": vent_hold_pressure_rise_rate_hpa_per_s,
        "vent_hold_pressure_max_hpa": max((sample["pressure_hpa"] for sample in vent_hold_samples), default=None),
        "vent_hold_pressure_min_hpa": min((sample["pressure_hpa"] for sample in vent_hold_samples), default=None),
        "vent_hold_tick_count": vent_hold_tick_count,
        "vent_close_command_at": vent_close_command_ts.isoformat() if vent_close_command_ts else "",
        "vent_close_confirmed_at": vent_close_confirmed_ts.isoformat() if vent_close_confirmed_ts else "",
        "vent_close_duration_s": vent_close_duration_s,
        "positive_preseal_started_at": positive_start_ts.isoformat() if positive_start_ts else "",
        "positive_preseal_start_pressure_hpa": positive_start_pressure,
        "ready_pressure_hpa": ready_pressure_hpa,
        "ready_reached_at": ready_ts.isoformat() if ready_ts else "",
        "ready_reached_elapsed_s": _as_float(positive_ready_actual.get("elapsed_s")),
        "ready_reached_before_vent_close_completed": ready_reached_before_vent_close_completed,
        "ready_reached_during_vent_close": ready_reached_during_vent_close,
        "ready_to_vent_close_start_s": ready_to_vent_close_start_s,
        "ready_to_vent_close_end_s": ready_to_vent_close_end_s,
        "time_from_route_open_to_ready_s": time_from_route_open_to_ready_s,
        "time_from_positive_preseal_start_to_ready_s": time_from_positive_preseal_start_to_ready_s,
        "positive_preseal_pressure_rise_rate_hpa_per_s": positive_preseal_pressure_rise_rate_hpa_per_s,
        "seal_command_sent_at": seal_command_ts.isoformat() if seal_command_ts else "",
        "seal_confirmed_at": seal_confirm_ts.isoformat() if seal_confirm_ts else "",
        "seal_command_latency_after_ready_s": seal_command_latency_after_ready_s,
        "seal_confirm_latency_after_ready_s": seal_confirm_latency_after_ready_s,
        "seal_trigger_pressure_hpa": seal_trigger_pressure,
        "pressure_at_seal_confirm_hpa": pressure_at_seal_confirm_hpa,
        "pressure_delta_during_vent_close_hpa": pressure_delta_during_vent_close_hpa,
        "pressure_delta_after_ready_before_seal_hpa": pressure_delta_after_ready_before_seal_hpa,
        "pressure_increase_after_ready_before_seal_hpa": pressure_increase_after_ready_before_seal_hpa,
        "pressure_max_before_seal_hpa": pressure_max_before_seal_hpa,
        "abort_pressure_hpa": abort_pressure_hpa,
        "abort_margin_min_hpa": abort_margin_min_hpa,
        "seal_command_sent": bool(seal_command_ts),
        "seal_command_blocked_reason": seal_command_blocked_reason,
        "abort_reason": (
            str(((positive_abort_row or {}).get("actual") or {}).get("abort_reason") or "")
            if isinstance((positive_abort_row or {}).get("actual"), Mapping)
            else ""
        ),
        "decision": decision,
        "warning_codes": [str(item.get("warning_code") or "") for item in warnings],
        "warning_count": len(warnings),
        "abnormal_waits": abnormal_waits,
        "warning_thresholds": thresholds,
        "no_write_guard_active": bool(payload.get("no_write_guard_active", payload.get("no_write", False))),
        "warning_only": bool(thresholds.get("timing_warning_only", True)),
        "not_real_acceptance_evidence": True,
        "v2_replaces_v1_claim": False,
    }


def _merge_preseal_diagnostic_warnings(
    timing_summary: Mapping[str, Any],
    diagnostics: Mapping[str, Any],
) -> dict[str, Any]:
    summary = dict(timing_summary)
    merged: list[dict[str, Any]] = []

    def add_warning(item: Any) -> None:
        if isinstance(item, Mapping):
            code = str(item.get("warning_code") or "").strip()
            payload = dict(item)
        else:
            code = str(item or "").strip()
            payload = {"warning_code": code, "warning_only": True}
        if not code:
            return
        if any(str(existing.get("warning_code") or "") == code for existing in merged):
            return
        payload["warning_code"] = code
        merged.append(payload)

    for item in list(diagnostics.get("abnormal_waits") or []):
        add_warning(item)
    for code in list(diagnostics.get("warning_codes") or []):
        add_warning(code)
    for item in list(summary.get("preseal_timing_warnings") or []):
        add_warning(item)

    severe_codes = {
        "positive_preseal_ready_without_seal_start",
        "positive_preseal_seal_start_without_seal_end",
        "pressure_max_before_seal_near_abort_threshold",
        "ready_to_seal_command_s_long",
        "ready_to_seal_confirm_s_long",
        "pressure_increase_after_ready_before_seal_hpa_high",
    }
    severe = [
        item
        for item in merged
        if str(item.get("warning_code") or "") in severe_codes
    ]
    summary.update(
        {
            "preseal_timing_warnings": merged,
            "preseal_timing_warning_count": len(merged),
            "preseal_timing_warnings_all": merged,
            "preseal_timing_warning_count_total": len(merged),
            "preseal_timing_warnings_severe": severe,
            "preseal_timing_warning_count_severe": len(severe),
            "severe_preseal_timing_warning_count": len(severe),
        }
    )
    diagnostic_fields = {
        "preseal_vent_close_arm_trigger": diagnostics.get("vent_close_arm_trigger"),
        "preseal_vent_close_arm_elapsed_s": diagnostics.get("vent_close_arm_elapsed_s"),
        "preseal_vent_close_arm_pressure_hpa": diagnostics.get("vent_close_arm_pressure_hpa"),
        "estimated_time_to_ready_s": diagnostics.get("estimated_time_to_ready_s"),
        "ready_to_vent_close_start_s": diagnostics.get("ready_to_vent_close_start_s"),
        "ready_to_vent_close_end_s": diagnostics.get("ready_to_vent_close_end_s"),
        "ready_to_seal_command_s": diagnostics.get("seal_command_latency_after_ready_s"),
        "pressure_delta_during_vent_close_hpa": diagnostics.get("pressure_delta_during_vent_close_hpa"),
        "ready_without_seal_reason": diagnostics.get("seal_command_blocked_reason"),
    }
    for key, value in diagnostic_fields.items():
        if value not in (None, ""):
            summary[key] = value
    return summary


def _merge_pressure_latency_and_route_surge_summary(
    timing_summary: Mapping[str, Any],
    latency: Mapping[str, Any],
    surge: Mapping[str, Any],
) -> dict[str, Any]:
    summary = dict(timing_summary)
    stale_counts = dict(latency.get("stale_sample_count_by_source") or {})
    warning_items = list(latency.get("warnings") or [])
    route_warnings = [
        {"warning_code": str(code), "warning_only": True}
        for code in list(surge.get("warning_codes") or [])
        if str(code or "")
    ]
    summary.update(
        {
            "pace_pressure_read_latency_max_s": dict(latency.get("max_read_latency_by_source") or {}).get(
                "pace_controller"
            ),
            "pace_pressure_read_latency_avg_s": dict(latency.get("max_read_latency_by_source") or {}).get(
                "pace_controller"
            ),
            "gauge_pressure_read_latency_max_s": dict(latency.get("max_read_latency_by_source") or {}).get(
                "digital_pressure_gauge"
            ),
            "gauge_pressure_read_latency_avg_s": dict(latency.get("max_read_latency_by_source") or {}).get(
                "digital_pressure_gauge"
            ),
            "pressure_read_latency_warning_count": len(warning_items),
            "pressure_read_latency_warnings": warning_items,
            "stale_pressure_sample_count": sum(int(value or 0) for value in stale_counts.values()),
            "pressure_source_disagreement_max_hpa": latency.get("source_disagreement_hpa"),
            "primary_pressure_source": latency.get("primary_pressure_source"),
            "abort_decision_pressure_source": latency.get("pressure_source_used_for_abort"),
            "route_open_to_first_pressure_request_s": latency.get("route_open_to_first_pressure_request_s"),
            "route_open_to_first_pressure_response_s": latency.get("route_open_to_first_pressure_response_s"),
            "route_open_to_first_pressure_sample_s": surge.get("route_open_to_first_pressure_sample_s"),
            "route_open_pressure_delta_hpa": surge.get("pressure_delta_after_route_open_hpa"),
            "route_open_pressure_rise_rate_hpa_per_s": surge.get(
                "pressure_rise_rate_after_route_open_hpa_per_s"
            ),
            "route_open_first_sample_hpa": surge.get("pressure_first_sample_after_route_open_hpa"),
            "route_open_first_sample_exceeded_abort": bool(surge.get("abort_triggered")),
            "pressure_max_before_abort_hpa": surge.get("pre_abort_pressure_max_hpa"),
            "pressure_at_abort_hpa": surge.get("abort_trigger_pressure_hpa"),
            "pressure_max_after_abort_hpa": surge.get("post_abort_pressure_max_hpa"),
            "pressure_max_during_safe_stop_hpa": surge.get("post_abort_pressure_max_hpa"),
            "route_open_surge_warnings": route_warnings,
            "route_open_surge_warning_count": len(route_warnings),
        }
    )
    return summary


def build_run001_a2_evidence_payload(
    raw_cfg: Mapping[str, Any],
    *,
    config_path: str | Path | None = None,
    run_id: Optional[str] = None,
    run_dir: str | Path | None = None,
    point_rows: Optional[list[dict[str, Any]]] = None,
    guard: Optional[NoWriteGuard] = None,
    artifact_paths: Optional[Mapping[str, Any]] = None,
    require_runtime_artifacts: bool = False,
    service_summary: Optional[Mapping[str, Any]] = None,
    service_status: Optional[Mapping[str, Any]] = None,
    temperature_stability_evidence: Optional[Mapping[str, Any]] = None,
) -> dict[str, Any]:
    rows = list(point_rows if point_rows is not None else load_point_rows(config_path, raw_cfg))
    pressure_cfg = _workflow_pressure(raw_cfg)
    preseal_timing_thresholds = _preseal_timing_thresholds(pressure_cfg)
    compat = _a1_compatible_raw_cfg(raw_cfg)
    payload = build_run001_a1_evidence_payload(
        compat,
        config_path=config_path,
        run_id=run_id,
        run_dir=run_dir,
        point_rows=rows,
        guard=guard,
        artifact_paths=artifact_paths,
        require_runtime_artifacts=require_runtime_artifacts,
        service_summary=service_summary,
        service_status=service_status,
        temperature_stability_evidence=temperature_stability_evidence,
    )
    if run_id is None and str(payload.get("run_id") or "").startswith("run001_a1_preflight_"):
        payload["run_id"] = str(payload["run_id"]).replace("run001_a1_preflight_", "run001_a2_preflight_", 1)
    readiness = evaluate_run001_a2_readiness(
        raw_cfg,
        config_path=config_path,
        point_rows=rows,
        attempted_write_count=int(payload.get("attempted_write_count", 0) or 0),
        blocked_write_events=list(payload.get("blocked_write_events") or []),
        artifact_paths=artifact_paths,
        require_runtime_artifacts=require_runtime_artifacts,
    )
    runtime_expected = bool(require_runtime_artifacts or service_summary or service_status)
    completed_pressures = _completed_pressure_points_from_trace(run_dir)
    reasons = list(readiness.get("hard_stop_reasons") or [])
    a1_reasons = list(payload.get("a1_decision_reasons") or [])
    if runtime_expected:
        if payload.get("a1_final_decision") != RUN001_PASS:
            reasons.extend(a1_reasons or ["runtime_execution_failed"])
        if not _same_pressure_list(completed_pressures, A2_AUTHORIZED_PRESSURE_POINTS_HPA):
            reasons.append("planned_pressure_points_not_completed")
        if int(payload.get("points_completed", 0) or 0) != len(A2_AUTHORIZED_PRESSURE_POINTS_HPA):
            reasons.append("points_completed_not_7")
    deduped = list(dict.fromkeys(reasons))
    a2_decision = RUN001_NOT_EXECUTED if not runtime_expected else (RUN001_PASS if not deduped else RUN001_FAIL)
    payload.update(
        {
            "schema_version": "run001_a2.no_write_pressure_sweep.1",
            "artifact_type": "run001_a2_no_write_pressure_sweep_evidence",
            "a2_scope": "co2_single_route_full_pressure_no_write",
            "a2_authorized_pressure_points_hpa": list(A2_AUTHORIZED_PRESSURE_POINTS_HPA),
            "a2_point_pressure_points_hpa": _pressure_points_from_rows(rows),
            "continuous_atmosphere_hold": _as_bool(pressure_cfg.get("continuous_atmosphere_hold")),
            "vent_hold_interval_s": _as_float(pressure_cfg.get("vent_hold_interval_s")),
            "atmosphere_vent_heartbeat_interval_s": _as_float(
                pressure_cfg.get("atmosphere_vent_heartbeat_interval_s")
                if pressure_cfg.get("atmosphere_vent_heartbeat_interval_s") is not None
                else pressure_cfg.get("conditioning_vent_heartbeat_interval_s", 1.0)
            ),
            "atmosphere_vent_max_gap_s": _as_float(
                pressure_cfg.get("atmosphere_vent_max_gap_s")
                if pressure_cfg.get("atmosphere_vent_max_gap_s") is not None
                else pressure_cfg.get("conditioning_vent_max_gap_s", 3.0)
            ),
            "pressure_monitor_interval_s": _as_float(
                pressure_cfg.get("pressure_monitor_interval_s")
                if pressure_cfg.get("pressure_monitor_interval_s") is not None
                else pressure_cfg.get("conditioning_pressure_monitor_interval_s", 0.5)
            ),
            "digital_gauge_max_age_s": _as_float(
                pressure_cfg.get("conditioning_digital_gauge_max_age_s")
                if pressure_cfg.get("conditioning_digital_gauge_max_age_s") is not None
                else pressure_cfg.get("digital_gauge_max_age_s", 3.0)
            ),
            "conditioning_pressure_abort_hpa": _as_float(
                pressure_cfg.get("conditioning_pressure_abort_hpa")
                if pressure_cfg.get("conditioning_pressure_abort_hpa") is not None
                else pressure_cfg.get("preseal_atmosphere_flush_abort_pressure_hpa", pressure_cfg.get("preseal_abort_pressure_hpa", 1150.0))
            ),
            "preseal_atmosphere_hold_pressure_limit_hpa": _preseal_atmosphere_hold_limit_hpa(raw_cfg, rows),
            "positive_preseal_pressurization_enabled": _as_bool(
                pressure_cfg.get("positive_preseal_pressurization_enabled")
            ),
            "positive_preseal_target_pressure_hpa": _first_pressure_hpa(rows),
            "positive_preseal_ready_pressure_hpa": _positive_preseal_ready_pressure_hpa(raw_cfg, rows),
            "positive_preseal_abort_pressure_hpa": _positive_preseal_abort_pressure_hpa(raw_cfg, rows),
            "positive_preseal_ready_timeout_s": _as_float(pressure_cfg.get("preseal_ready_timeout_s")),
            "positive_preseal_pressure_poll_interval_s": _as_float(
                pressure_cfg.get("preseal_pressure_poll_interval_s")
            ),
            "high_pressure_first_point_mode_configured": _as_bool(
                pressure_cfg.get("high_pressure_first_point_mode_enabled", True)
            ),
            "high_pressure_first_point_candidate": bool(
                _as_bool(pressure_cfg.get("high_pressure_first_point_mode_enabled", True))
                and 1100.0 in [round(float(value), 3) for value in _pressure_points_from_rows(rows)]
            ),
            "high_pressure_first_point_margin_hpa": (
                _as_float(pressure_cfg.get("high_pressure_first_point_margin_hpa")) or 0.0
            ),
            "high_pressure_first_point_first_target_pressure_hpa": _first_pressure_hpa(rows),
            **preseal_timing_thresholds,
            "planned_pressure_points_completed": completed_pressures,
            "planned_pressure_point_count": len(A2_AUTHORIZED_PRESSURE_POINTS_HPA),
            "a2_final_decision": a2_decision,
            "a2_execution_result": "completed" if a2_decision == RUN001_PASS else ("preflight_only" if not runtime_expected else "failed"),
            "a2_fail_reason": "" if a2_decision in {RUN001_PASS, RUN001_NOT_EXECUTED} else "; ".join(deduped),
            "a2_decision_reasons": [] if a2_decision == RUN001_PASS else deduped,
            "final_decision": a2_decision if runtime_expected else readiness.get("final_decision", RUN001_FAIL),
            "readiness_result": readiness.get("readiness_result"),
            "hard_stop_reasons": readiness.get("hard_stop_reasons", []),
            "not_real_acceptance_evidence": True,
            "v2_replaces_v1_claim": False,
            "h2o_allowed": False,
            "full_group_allowed": False,
            "real_write_allowed": False,
        }
    )
    return payload


def render_run001_a2_human_report(payload: Mapping[str, Any]) -> str:
    reasons = list(payload.get("a2_decision_reasons") or [])
    reason_lines = "\n".join(f"- {item}" for item in reasons) if reasons else "- none"
    pressures = ", ".join(f"{float(value):g}" for value in list(payload.get("a2_authorized_pressure_points_hpa") or []))
    completed = ", ".join(f"{float(value):g}" for value in list(payload.get("planned_pressure_points_completed") or []))
    timing_summary = payload.get("workflow_timing_summary")
    timing_summary = dict(timing_summary) if isinstance(timing_summary, Mapping) else {}
    timing_warning_count = sum(
        len(list(timing_summary.get(key) or []))
        for key in (
            "abnormal_waits",
            "timeout_events",
            "repeated_sleep_warnings",
            "missing_end_events",
            "state_mismatch_warnings",
        )
    )
    longest_stage = timing_summary.get("longest_stage") if isinstance(timing_summary.get("longest_stage"), Mapping) else {}
    longest_wait = timing_summary.get("longest_wait") if isinstance(timing_summary.get("longest_wait"), Mapping) else {}
    temperature_policy = payload.get("temperature_stability_policy")
    temperature_policy = dict(temperature_policy) if isinstance(temperature_policy, Mapping) else {}
    preseal_timing = payload.get("positive_preseal_timing_diagnostics")
    preseal_timing = dict(preseal_timing) if isinstance(preseal_timing, Mapping) else {}
    route_surge = payload.get("route_open_pressure_surge_evidence")
    route_surge = dict(route_surge) if isinstance(route_surge, Mapping) else {}
    co2_conditioning = payload.get("co2_route_conditioning_evidence")
    co2_conditioning = dict(co2_conditioning) if isinstance(co2_conditioning, Mapping) else {}
    pressure_latency = payload.get("pressure_read_latency_diagnostics")
    pressure_latency = dict(pressure_latency) if isinstance(pressure_latency, Mapping) else {}
    high_pressure = payload.get("high_pressure_first_point_evidence")
    high_pressure = dict(high_pressure) if isinstance(high_pressure, Mapping) else {}
    critical_freshness = payload.get("critical_pressure_freshness_evidence")
    critical_freshness = dict(critical_freshness) if isinstance(critical_freshness, Mapping) else {}
    summary_warning_items = list(
        timing_summary.get("preseal_timing_warnings_all")
        or timing_summary.get("preseal_timing_warnings")
        or []
    )
    preseal_warning_codes = [
        str(item.get("warning_code") or "")
        for item in summary_warning_items
        if isinstance(item, Mapping) and str(item.get("warning_code") or "")
    ]
    if not preseal_warning_codes:
        preseal_warning_codes = list(
            preseal_timing.get("warning_codes") or payload.get("positive_preseal_timing_warning_codes") or []
        )
    preseal_warning_count_total = timing_summary.get(
        "preseal_timing_warning_count_total",
        len(preseal_warning_codes),
    )
    preseal_warning_count_severe = timing_summary.get("preseal_timing_warning_count_severe")
    preseal_warning_text = ", ".join(str(item) for item in preseal_warning_codes) if preseal_warning_codes else "none"
    return "\n".join(
        [
            "# Run-001 / A2 CO2 no-write pressure sweep evidence",
            "",
            f"- run_id: {payload.get('run_id')}",
            f"- mode: {payload.get('mode')}",
            f"- no_write: {payload.get('no_write')}",
            f"- authorized_pressure_points_hpa: {pressures}",
            f"- completed_pressure_points_hpa: {completed}",
            f"- a2_final_decision: {payload.get('a2_final_decision')}",
            f"- final_decision: {payload.get('final_decision')}",
            f"- points_completed: {payload.get('points_completed')}",
            f"- sample_count: {payload.get('sample_count')}",
            f"- attempted_write_count: {payload.get('attempted_write_count')}",
            f"- identity_write_command_sent: {payload.get('identity_write_command_sent')}",
            f"- persistent_write_command_sent: {payload.get('persistent_write_command_sent')}",
            f"- route_completed: {payload.get('route_completed')}",
            f"- pressure_completed: {payload.get('pressure_completed')}",
            f"- wait_gate_completed: {payload.get('wait_gate_completed')}",
            f"- sample_completed: {payload.get('sample_completed')}",
            f"- effective_analyzer_mapping_status: {payload.get('effective_analyzer_mapping_status')}",
            f"- all_enabled_analyzers_mode2_ready: {payload.get('all_enabled_analyzers_mode2_ready')}",
            f"- temperature_stability_tolerance_c: {payload.get('temperature_stability_tolerance_c')}",
            f"- temperature_stability_window_s: {payload.get('temperature_stability_window_s')}",
            f"- temperature_chamber_settle_timeout_s: {temperature_policy.get('temperature_chamber_timeout_s')}",
            f"- analyzer_chamber_temperature_stability_timeout_s: {temperature_policy.get('analyzer_chamber_temp_timeout_s')}",
            f"- preseal_atmosphere_hold_decision: {payload.get('preseal_atmosphere_hold_decision')}",
            f"- preseal_atmosphere_hold_pressure_limit_hpa: {payload.get('preseal_atmosphere_hold_pressure_limit_hpa')}",
            f"- preseal_atmosphere_hold_max_measured_pressure_hpa: {payload.get('preseal_atmosphere_hold_max_measured_pressure_hpa')}",
            f"- preseal_atmosphere_hold_pressure_limit_exceeded: {payload.get('preseal_atmosphere_hold_pressure_limit_exceeded')}",
            f"- preseal_atmosphere_hold_evidence: {payload.get('preseal_atmosphere_hold_evidence_artifact')}",
            f"- preseal_atmosphere_hold_samples: {payload.get('preseal_atmosphere_hold_samples_artifact')}",
            "",
            "## CO2 通大气洗刷与封路时序审计",
            f"- co2_route_conditioning_evidence: {payload.get('co2_route_conditioning_evidence_artifact')}",
            f"- conditioning_at_atmosphere: {co2_conditioning.get('atmosphere_vent_enabled')}",
            f"- conditioning_soak_s: {co2_conditioning.get('conditioning_soak_s')}",
            f"- conditioning_duration_s: {co2_conditioning.get('conditioning_duration_s')}",
            f"- vent_on_during_conditioning: {co2_conditioning.get('atmosphere_vent_enabled')}",
            f"- vent_tick_count: {co2_conditioning.get('vent_tick_count')}",
            f"- pressure_max_during_conditioning_hpa: {co2_conditioning.get('pressure_max_during_conditioning_hpa')}",
            f"- pressure_min_during_conditioning_hpa: {co2_conditioning.get('pressure_min_during_conditioning_hpa')}",
            f"- pressure_warning_count: {timing_summary.get('co2_route_conditioning_pressure_warning_count')}",
            f"- sealed_during_conditioning: {co2_conditioning.get('sealed_during_conditioning')}",
            f"- did_not_seal_during_conditioning: {co2_conditioning.get('did_not_seal_during_conditioning')}",
            f"- preseal_analyzer_gate_passed: {high_pressure.get('preseal_analyzer_gate_passed')}",
            f"- vent_off_sent_at_after_conditioning: {high_pressure.get('vent_off_sent_at')}",
            f"- high_pressure_ready_wait_started_at: {high_pressure.get('high_pressure_ready_wait_started_at')}",
            f"- ready_to_seal_command_s: {high_pressure.get('ready_to_seal_command_s')}",
            f"- v1_order_contract_aligned: {co2_conditioning.get('did_not_seal_during_conditioning') and high_pressure.get('conditioning_completed_before_high_pressure_mode')}",
            "- next_run_recommendation: only rerun A2 after explicit human authorization; conditioning must show periodic vent tick evidence and no pre-conditioning seal.",
            "",
            "## Positive preseal pressurization summary",
            f"- positive_preseal_pressurization_enabled: {payload.get('positive_preseal_pressurization_enabled')}",
            f"- preseal_ready_pressure_hpa: {payload.get('positive_preseal_ready_pressure_hpa')}",
            f"- preseal_abort_pressure_hpa: {payload.get('positive_preseal_abort_pressure_hpa')}",
            f"- positive_preseal_decision: {payload.get('positive_preseal_pressurization_decision')}",
            f"- positive_preseal_ready_reached: {payload.get('positive_preseal_ready_reached')}",
            f"- positive_preseal_seal_trigger_pressure_hpa: {payload.get('positive_preseal_seal_trigger_pressure_hpa')}",
            f"- positive_preseal_abort_reason: {payload.get('positive_preseal_abort_reason')}",
            f"- ambient_reference_pressure_hpa: {payload.get('ambient_reference_pressure_hpa')}",
            f"- ambient_reference_source: {payload.get('ambient_reference_source')}",
            f"- positive_preseal_evidence: {payload.get('positive_preseal_pressurization_evidence_artifact')}",
            f"- positive_preseal_samples: {payload.get('positive_preseal_pressurization_samples_artifact')}",
            "",
            "## 正压封路升压时序诊断",
            f"- positive_preseal_timing_diagnostics: {payload.get('positive_preseal_timing_diagnostics_artifact')}",
            f"- vent_close_arm_trigger: {preseal_timing.get('vent_close_arm_trigger')}",
            f"- vent_close_arm_pressure_hpa: {preseal_timing.get('vent_close_arm_pressure_hpa')}",
            f"- vent_close_arm_elapsed_s: {preseal_timing.get('vent_close_arm_elapsed_s')}",
            f"- estimated_time_to_ready_s: {preseal_timing.get('estimated_time_to_ready_s')}",
            f"- route_open_to_first_pressure_rise_s: {preseal_timing.get('first_pressure_rise_detected_elapsed_s')}",
            f"- route_open_to_ready_s: {preseal_timing.get('time_from_route_open_to_ready_s')}",
            f"- vent_hold_pressure_delta_hpa: {preseal_timing.get('vent_hold_pressure_delta_hpa')}",
            f"- vent_hold_pressure_rise_rate_hpa_per_s: {preseal_timing.get('vent_hold_pressure_rise_rate_hpa_per_s')}",
            f"- vent_close_duration_s: {preseal_timing.get('vent_close_duration_s')}",
            f"- ready_to_vent_close_start_s: {preseal_timing.get('ready_to_vent_close_start_s')}",
            f"- ready_to_vent_close_end_s: {preseal_timing.get('ready_to_vent_close_end_s')}",
            f"- positive_preseal_pressure_rise_rate_hpa_per_s: {preseal_timing.get('positive_preseal_pressure_rise_rate_hpa_per_s')}",
            f"- ready_to_seal_command_s: {preseal_timing.get('seal_command_latency_after_ready_s')}",
            f"- ready_to_seal_confirm_s: {preseal_timing.get('seal_confirm_latency_after_ready_s')}",
            f"- pressure_delta_during_vent_close_hpa: {preseal_timing.get('pressure_delta_during_vent_close_hpa')}",
            f"- pressure_increase_after_ready_before_seal_hpa: {preseal_timing.get('pressure_increase_after_ready_before_seal_hpa')}",
            f"- pressure_max_before_seal_hpa: {preseal_timing.get('pressure_max_before_seal_hpa')}",
            f"- abort_pressure_hpa: {preseal_timing.get('abort_pressure_hpa')}",
            f"- abort_margin_min_hpa: {preseal_timing.get('abort_margin_min_hpa')}",
            f"- seal_command_sent: {preseal_timing.get('seal_command_sent')}",
            f"- seal_command_blocked_reason: {preseal_timing.get('seal_command_blocked_reason')}",
            f"- timing_warning_only: {preseal_timing.get('warning_only')}",
            f"- preseal_timing_warning_count_total: {preseal_warning_count_total}",
            f"- preseal_timing_warning_count_severe: {preseal_warning_count_severe}",
            f"- warning_codes: {preseal_warning_text}",
            "",
            "## 开阀瞬间升压诊断",
            f"- route_open_pressure_surge_evidence: {payload.get('route_open_pressure_surge_evidence_artifact')}",
            f"- pressure_before_route_open_hpa: {route_surge.get('pressure_before_route_open_hpa')}",
            f"- pressure_first_sample_after_route_open_hpa: {route_surge.get('pressure_first_sample_after_route_open_hpa')}",
            f"- route_open_to_first_pressure_sample_s: {route_surge.get('route_open_to_first_pressure_sample_s')}",
            f"- pressure_delta_after_route_open_hpa: {route_surge.get('pressure_delta_after_route_open_hpa')}",
            f"- pressure_rise_rate_after_route_open_hpa_per_s: {route_surge.get('pressure_rise_rate_after_route_open_hpa_per_s')}",
            f"- vent_status_at_route_open: {route_surge.get('vent_status_at_route_open')}",
            f"- output_state_at_route_open: {route_surge.get('output_state_at_route_open')}",
            f"- isolation_state_at_route_open: {route_surge.get('isolation_state_at_route_open')}",
            f"- vent_status_at_first_sample: {route_surge.get('vent_status_at_first_sample')}",
            f"- output_state_at_first_sample: {route_surge.get('output_state_at_first_sample')}",
            f"- isolation_state_at_first_sample: {route_surge.get('isolation_state_at_first_sample')}",
            f"- abort_trigger_pressure_hpa: {route_surge.get('abort_trigger_pressure_hpa')}",
            f"- post_abort_pressure_max_hpa: {route_surge.get('post_abort_pressure_max_hpa')}",
            f"- warning_codes: {', '.join(str(code) for code in list(route_surge.get('warning_codes') or [])) or 'none'}",
            "- next_run_recommendation: lower CO2 inlet flow/upstream pressure, confirm exhaust capacity, keep 1150 hPa hard abort.",
            "",
            "## 1100 高压首点正压封路诊断",
            f"- high_pressure_first_point_evidence: {payload.get('high_pressure_first_point_evidence_artifact')}",
            f"- high_pressure_first_point_mode_enabled: {high_pressure.get('enabled')}",
            f"- ambient_reference_pressure_hpa: {high_pressure.get('ambient_reference_pressure_hpa')}",
            f"- first_target_pressure_hpa: {high_pressure.get('first_target_pressure_hpa')}",
            f"- baseline_pressure_hpa: {high_pressure.get('baseline_pressure_hpa')}",
            f"- baseline_pressure_source: {high_pressure.get('baseline_pressure_source')}",
            f"- route_open_to_first_pressure_request_s: {high_pressure.get('route_open_to_first_pressure_request_s')}",
            f"- route_open_to_first_pressure_response_s: {high_pressure.get('route_open_to_first_pressure_response_s')}",
            f"- first_pressure_hpa: {high_pressure.get('first_pressure_hpa')}",
            f"- first_pressure_source: {high_pressure.get('first_pressure_source')}",
            f"- first_pressure_stale: {high_pressure.get('first_pressure_stale')}",
            f"- ready_pressure_hpa: {high_pressure.get('ready_pressure_hpa')}",
            f"- ready_reached_at: {high_pressure.get('ready_reached_at')}",
            f"- seal_command_sent_at: {high_pressure.get('seal_command_sent_at')}",
            f"- ready_to_seal_command_s: {high_pressure.get('ready_to_seal_command_s')}",
            f"- seal_confirmed_at: {high_pressure.get('seal_confirmed_at')}",
            f"- pressure_at_seal_hpa: {high_pressure.get('pressure_at_seal_hpa')}",
            f"- missed_seal_window_due_to_pressure_latency: {bool(high_pressure.get('warning_count'))}",
            f"- abort_triggered: {high_pressure.get('abort_triggered')}",
            f"- abort_reason: {high_pressure.get('abort_reason')}",
            "- field_recommendation: 降低流量/上游压力，保证数字压力计快速读取，确认阀门动作响应时间。",
            "",
            "## 压力读取延迟与双压力源诊断",
            f"- pressure_read_latency_diagnostics: {payload.get('pressure_read_latency_diagnostics_artifact')}",
            f"- pressure_read_latency_samples: {payload.get('pressure_read_latency_samples_artifact')}",
            f"- primary_pressure_source: {pressure_latency.get('primary_pressure_source')}",
            f"- abort_decision_pressure_source: {pressure_latency.get('pressure_source_used_for_abort')}",
            f"- pace_pressure_hpa: {pressure_latency.get('pace_pressure_hpa')}",
            f"- pace_pressure_latency_s: {pressure_latency.get('pace_pressure_latency_s')}",
            f"- pace_pressure_age_s: {pressure_latency.get('pace_pressure_age_s')}",
            f"- digital_gauge_pressure_hpa: {pressure_latency.get('digital_gauge_pressure_hpa')}",
            f"- digital_gauge_latency_s: {pressure_latency.get('digital_gauge_latency_s')}",
            f"- digital_gauge_age_s: {pressure_latency.get('digital_gauge_age_s')}",
            f"- route_open_to_first_pressure_request_s: {pressure_latency.get('route_open_to_first_pressure_request_s')}",
            f"- route_open_to_first_pressure_response_s: {pressure_latency.get('route_open_to_first_pressure_response_s')}",
            f"- first_pressure_is_stale: {pressure_latency.get('first_pressure_is_stale')}",
            f"- source_disagreement_hpa: {pressure_latency.get('source_disagreement_hpa')}",
            f"- warning_count: {pressure_latency.get('warning_count')}",
            "- stale_policy: stale pressure samples are not usable for ready/seal/abort decisions.",
            "",
            "<!-- legacy_heading_marker: 娴佺▼鏃跺簭鎽樿 -->",
            "## 流程时序摘要",
            "## 关键压力取数新鲜度诊断",
            f"- critical_pressure_freshness_evidence: {payload.get('critical_pressure_freshness_evidence_artifact')}",
            f"- digital_gauge_mode: {pressure_latency.get('digital_gauge_mode') or critical_freshness.get('digital_gauge_mode')}",
            f"- digital_gauge_continuous_enabled: {critical_freshness.get('digital_gauge_continuous_enabled')}",
            f"- digital_gauge_continuous_mode: {critical_freshness.get('digital_gauge_continuous_mode')}",
            f"- route_open_has_fresh_latest_frame: {critical_freshness.get('latest_frame_age_at_route_open_s') is not None and not bool(high_pressure.get('baseline_pressure_stale'))}",
            f"- decision_uses_latest_frame: {pressure_latency.get('critical_window_uses_latest_frame')}",
            f"- latest_frame_age_s: {pressure_latency.get('latest_frame_age_s')}",
            f"- latest_frame_age_at_route_open_s: {critical_freshness.get('latest_frame_age_at_route_open_s')}",
            f"- latest_frame_age_at_ready_s: {critical_freshness.get('latest_frame_age_at_ready_s')}",
            f"- latest_frame_age_at_abort_s: {critical_freshness.get('latest_frame_age_at_abort_s')}",
            f"- latest_frame_age_at_seal_s: {critical_freshness.get('latest_frame_age_at_seal_s')}",
            f"- critical_window_blocking_query_count: {critical_freshness.get('blocking_query_count_in_critical_window')}",
            f"- critical_window_blocking_query_total_s: {critical_freshness.get('critical_window_blocking_query_total_s')}",
            f"- pace_aux_enabled: {critical_freshness.get('pace_aux_enabled')}",
            f"- pace_aux_pressure_hpa: {critical_freshness.get('pace_aux_pressure_hpa')}",
            f"- pace_digital_overlap_samples: {critical_freshness.get('pace_digital_overlap_samples')}",
            f"- pace_digital_max_diff_hpa: {critical_freshness.get('pace_digital_max_diff_hpa')}",
            f"- source_selection_decision: {critical_freshness.get('source_selection_decision')}",
            "- pace_substitution_policy: PACE can assist only when fresh, topology-connected, and consistent with recent digital-gauge frames; large disagreement is warning/fail-closed only.",
            f"- failure_likely_due_to: {'pressure_data_path_latency_or_unavailable_latest_frame' if critical_freshness.get('blocking_query_count_in_critical_window') or bool(pressure_latency.get('critical_window_uses_query')) or bool(pressure_latency.get('first_pressure_is_stale')) else 'physical_pressure_rise_or_field_condition'}",
            "- next_run_recommendation: lower CO2 flow/upstream pressure, verify digital-gauge continuous output latency, and confirm valve actuation response time.",
            "",
            "## 流程时序摘要",
            f"- workflow_timing_trace: {payload.get('workflow_timing_trace_artifact')}",
            f"- workflow_timing_summary: {payload.get('workflow_timing_summary_artifact')}",
            f"- total_duration_s: {timing_summary.get('total_duration_s')}",
            f"- longest_stage: {longest_stage.get('name')} ({longest_stage.get('duration_s')}s)",
            f"- longest_wait: {longest_wait.get('name')} ({longest_wait.get('duration_s')}s)",
            f"- temperature_chamber_settle_duration_s: {timing_summary.get('temperature_chamber_settle_duration_s')}",
            f"- analyzer_chamber_temperature_stability_duration_s: {timing_summary.get('analyzer_chamber_temperature_stability_duration_s')}",
            f"- preseal_soak_duration_s: {timing_summary.get('preseal_soak_duration_s')}",
            f"- positive_preseal_duration_s: {timing_summary.get('positive_preseal_duration_s')}",
            f"- positive_preseal_vent_close_duration_s: {timing_summary.get('positive_preseal_vent_close_duration_s')}",
            f"- positive_preseal_vent_close_status: {timing_summary.get('positive_preseal_vent_close_status')}",
            f"- route_open_to_first_pressure_rise_s: {timing_summary.get('route_open_to_first_pressure_rise_s')}",
            f"- route_open_to_ready_s: {timing_summary.get('route_open_to_ready_s')}",
            f"- positive_preseal_start_to_ready_s: {timing_summary.get('positive_preseal_start_to_ready_s')}",
            f"- ready_to_seal_command_s: {timing_summary.get('ready_to_seal_command_s')}",
            f"- ready_to_seal_confirm_s: {timing_summary.get('ready_to_seal_confirm_s')}",
            f"- preseal_timing_warning_count: {timing_summary.get('preseal_timing_warning_count')}",
            f"- preseal_timing_warning_count_total: {timing_summary.get('preseal_timing_warning_count_total')}",
            f"- preseal_timing_warning_count_severe: {timing_summary.get('preseal_timing_warning_count_severe')}",
            f"- preseal_vent_tick_count: {timing_summary.get('preseal_vent_tick_count')}",
            f"- timing_warning_count: {timing_warning_count}",
            "",
            "## A2 decision reasons",
            reason_lines,
            "",
            "This A2 no-write evidence does not authorize H2O, full group execution, real calibration writes, identity writes, device_id changes, V1 changes, default V2 cutover, or any statement that V2 can replace V1.",
            "",
        ]
    )


def _required_artifact_status(run_dir: str | Path) -> dict[str, dict[str, Any]]:
    directory = Path(run_dir)
    status: dict[str, dict[str, Any]] = {}
    for key, filename in A2_REQUIRED_ARTIFACTS.items():
        path = directory / filename
        status[key] = {"path": str(path), "exists": path.exists()}
    return status


def _finalize_artifact_decision(payload: dict[str, Any], run_dir: str | Path) -> dict[str, Any]:
    status = _required_artifact_status(run_dir)
    reasons = list(payload.get("a2_decision_reasons") or [])
    if payload.get("a2_final_decision") != RUN001_NOT_EXECUTED:
        for key, item in status.items():
            if not bool(item.get("exists")):
                reasons.append(f"a2_required_artifact_missing_{key}")
        conditioning = payload.get("co2_route_conditioning_evidence")
        conditioning = dict(conditioning) if isinstance(conditioning, Mapping) else {}
        if conditioning and int(conditioning.get("vent_tick_count") or 0) <= 0:
            reasons.append("a2_co2_route_conditioning_missing_vent_tick_evidence")
        if conditioning and bool(conditioning.get("sealed_during_conditioning")):
            reasons.append("a2_co2_route_conditioning_sealed_before_completion")
        if conditioning and bool(conditioning.get("vent_heartbeat_gap_exceeded")):
            reasons.append("a2_co2_route_conditioning_vent_heartbeat_gap_exceeded")
        if conditioning and bool(conditioning.get("pressure_overlimit_seen")):
            reasons.append("a2_co2_route_conditioning_pressure_overlimit")
        if conditioning and conditioning.get("selected_pressure_freshness_ok") is False:
            reason = str(conditioning.get("selected_pressure_fail_closed_reason") or "").strip()
            reasons.append(reason or "a2_co2_route_conditioning_selected_pressure_not_fresh")
        if conditioning and bool(conditioning.get("stream_stale")):
            reasons.append("a2_co2_route_conditioning_digital_gauge_stream_stale")
        if conditioning and conditioning.get("digital_gauge_sequence_progress") is False:
            reasons.append("a2_co2_route_conditioning_digital_gauge_sequence_not_progressing")
    deduped = list(dict.fromkeys(reasons))
    if payload.get("a2_final_decision") != RUN001_NOT_EXECUTED:
        payload["a2_final_decision"] = RUN001_PASS if not deduped else RUN001_FAIL
        payload["final_decision"] = payload["a2_final_decision"]
        payload["a2_execution_result"] = "completed" if payload["a2_final_decision"] == RUN001_PASS else "failed"
        payload["a2_fail_reason"] = "" if payload["a2_final_decision"] == RUN001_PASS else "; ".join(deduped)
    payload["a2_decision_reasons"] = deduped
    payload["a2_required_artifact_status"] = status
    return payload


def write_run001_a2_artifacts(run_dir: str | Path, payload: Mapping[str, Any]) -> dict[str, str]:
    directory = Path(run_dir)
    directory.mkdir(parents=True, exist_ok=True)
    enriched = dict(payload)
    common_paths = write_run001_a1_artifacts(directory, enriched)
    pressure_path = directory / "pressure_gate_evidence.json"
    preseal_evidence_path = directory / "preseal_atmosphere_hold_evidence.json"
    preseal_samples_path = directory / "preseal_atmosphere_hold_samples.csv"
    positive_preseal_evidence_path = directory / "positive_preseal_pressurization_evidence.json"
    positive_preseal_samples_path = directory / "positive_preseal_pressurization_samples.csv"
    positive_preseal_timing_path = directory / "positive_preseal_timing_diagnostics.json"
    co2_route_conditioning_path = directory / "co2_route_conditioning_evidence.json"
    route_open_surge_path = directory / "route_open_pressure_surge_evidence.json"
    pressure_latency_path = directory / "pressure_read_latency_diagnostics.json"
    pressure_latency_samples_path = directory / "pressure_read_latency_samples.csv"
    high_pressure_first_point_path = directory / "high_pressure_first_point_evidence.json"
    critical_pressure_freshness_path = directory / "critical_pressure_freshness_evidence.json"
    common_paths["pressure_gate_evidence"] = str(pressure_path)
    common_paths["preseal_atmosphere_hold_evidence"] = str(preseal_evidence_path)
    common_paths["preseal_atmosphere_hold_samples"] = str(preseal_samples_path)
    common_paths["positive_preseal_pressurization_evidence"] = str(positive_preseal_evidence_path)
    common_paths["positive_preseal_pressurization_samples"] = str(positive_preseal_samples_path)
    common_paths["positive_preseal_timing_diagnostics"] = str(positive_preseal_timing_path)
    common_paths["co2_route_conditioning_evidence"] = str(co2_route_conditioning_path)
    common_paths["route_open_pressure_surge_evidence"] = str(route_open_surge_path)
    common_paths["pressure_read_latency_diagnostics"] = str(pressure_latency_path)
    common_paths["pressure_read_latency_samples"] = str(pressure_latency_samples_path)
    common_paths["high_pressure_first_point_evidence"] = str(high_pressure_first_point_path)
    common_paths["critical_pressure_freshness_evidence"] = str(critical_pressure_freshness_path)
    timing_trace_path = directory / WORKFLOW_TIMING_TRACE_FILENAME
    timing_summary_path = directory / WORKFLOW_TIMING_SUMMARY_FILENAME
    common_paths["workflow_timing_trace"] = str(timing_trace_path)
    common_paths["workflow_timing_summary"] = str(timing_summary_path)
    enriched["artifact_paths"] = dict(common_paths)
    pressure_payload = _build_pressure_gate_evidence(directory, enriched)
    pressure_path.write_text(json.dumps(pressure_payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    preseal_payload = _build_preseal_atmosphere_hold_evidence(directory, enriched)
    preseal_evidence_path.write_text(
        json.dumps(preseal_payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    with preseal_samples_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=PRESEAL_ATMOSPHERE_HOLD_SAMPLE_FIELDS, extrasaction="ignore")
        writer.writeheader()
        for sample in list(preseal_payload.get("samples") or []):
            writer.writerow(dict(sample) if isinstance(sample, Mapping) else {})
    positive_preseal_payload = _build_positive_preseal_pressurization_evidence(directory, enriched)
    positive_preseal_evidence_path.write_text(
        json.dumps(positive_preseal_payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    with positive_preseal_samples_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=POSITIVE_PRESEAL_PRESSURIZATION_SAMPLE_FIELDS,
            extrasaction="ignore",
        )
        writer.writeheader()
        for sample in list(positive_preseal_payload.get("samples") or []):
            writer.writerow(dict(sample) if isinstance(sample, Mapping) else {})
    enriched.update(
        {
            "preseal_atmosphere_hold_evidence_artifact": str(preseal_evidence_path),
            "preseal_atmosphere_hold_samples_artifact": str(preseal_samples_path),
            "positive_preseal_pressurization_evidence_artifact": str(positive_preseal_evidence_path),
            "positive_preseal_pressurization_samples_artifact": str(positive_preseal_samples_path),
            "preseal_atmosphere_hold_decision": preseal_payload.get("decision"),
            "preseal_atmosphere_hold_pressure_limit_hpa": preseal_payload.get("pressure_limit_hpa"),
            "preseal_atmosphere_hold_max_measured_pressure_hpa": preseal_payload.get("max_measured_pressure_hpa"),
            "pressure_at_abort_hpa": preseal_payload.get("pressure_at_abort_hpa"),
            "pressure_max_before_abort_hpa": preseal_payload.get("pressure_max_before_abort_hpa"),
            "pressure_max_after_abort_hpa": preseal_payload.get("pressure_max_after_abort_hpa"),
            "pressure_max_before_seal_hpa": preseal_payload.get("pressure_max_before_seal_hpa"),
            "pressure_max_during_safe_stop_hpa": preseal_payload.get("pressure_max_during_safe_stop_hpa"),
            "preseal_pressure_max_hpa_total": preseal_payload.get("preseal_pressure_max_hpa_total"),
            "preseal_atmosphere_hold_pressure_limit_exceeded": preseal_payload.get("pressure_limit_exceeded"),
            "preseal_atmosphere_hold_periodic_vent_reassertion_count": preseal_payload.get(
                "periodic_vent_reassertion_count"
            ),
            "preseal_atmosphere_hold_failure_reason": preseal_payload.get("failure_reason"),
            "positive_preseal_pressurization_decision": positive_preseal_payload.get("decision"),
            "positive_preseal_ready_reached": positive_preseal_payload.get("ready_reached"),
            "positive_preseal_ready_reached_at_pressure_hpa": positive_preseal_payload.get(
                "ready_reached_at_pressure_hpa"
            ),
            "positive_preseal_seal_trigger_pressure_hpa": positive_preseal_payload.get(
                "seal_trigger_pressure_hpa"
            ),
            "positive_preseal_seal_trigger_elapsed_s": positive_preseal_payload.get(
                "seal_trigger_elapsed_s"
            ),
            "positive_preseal_abort_reason": positive_preseal_payload.get("abort_reason"),
            "ambient_reference_pressure_hpa": positive_preseal_payload.get("ambient_reference_pressure_hpa"),
            "ambient_reference_source": positive_preseal_payload.get("ambient_reference_source"),
            "ambient_reference_timestamp": positive_preseal_payload.get("ambient_reference_timestamp"),
            "ambient_reference_age_s": positive_preseal_payload.get("ambient_reference_age_s"),
            "positive_preseal_pressure_max_hpa": positive_preseal_payload.get("pressure_max_hpa")
            or positive_preseal_payload.get("max_measured_pressure_hpa"),
            "positive_preseal_pressure_min_hpa": positive_preseal_payload.get("pressure_min_hpa"),
            "vent_status_2_is_not_continuous_atmosphere_evidence": True,
        }
    )
    timing_artifacts = ensure_workflow_timing_artifacts(directory, enriched)
    timing_summary = dict(timing_artifacts.get("summary_payload") or {})
    pressure_latency_payload, pressure_latency_samples = _build_pressure_read_latency_diagnostics(directory, enriched)
    pressure_latency_path.write_text(
        json.dumps(pressure_latency_payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    with pressure_latency_samples_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=PRESSURE_READ_LATENCY_SAMPLE_FIELDS, extrasaction="ignore")
        writer.writeheader()
        for sample in pressure_latency_samples:
            writer.writerow(dict(sample) if isinstance(sample, Mapping) else {})
    route_open_surge_payload = _build_route_open_pressure_surge_evidence(
        directory,
        enriched,
        preseal_payload=preseal_payload,
        latency_payload=pressure_latency_payload,
    )
    route_open_surge_path.write_text(
        json.dumps(route_open_surge_payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    positive_preseal_timing_payload = _build_positive_preseal_timing_diagnostics(
        directory,
        enriched,
        timing_summary=timing_summary,
    )
    timing_summary = _merge_preseal_diagnostic_warnings(timing_summary, positive_preseal_timing_payload)
    timing_summary = _merge_pressure_latency_and_route_surge_summary(
        timing_summary,
        pressure_latency_payload,
        route_open_surge_payload,
    )
    co2_route_conditioning_payload = _build_co2_route_conditioning_evidence(
        directory,
        enriched,
        timing_summary=timing_summary,
    )
    co2_route_conditioning_path.write_text(
        json.dumps(co2_route_conditioning_payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    final_safe_stop_payload = _final_safe_stop_evidence(directory)
    high_pressure_first_point_payload = _build_high_pressure_first_point_evidence(
        directory,
        enriched,
        latency_payload=pressure_latency_payload,
        latency_samples=pressure_latency_samples,
        timing_summary=timing_summary,
        positive_preseal_payload=positive_preseal_payload,
    )
    high_pressure_first_point_path.write_text(
        json.dumps(high_pressure_first_point_payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    critical_pressure_freshness_payload = _build_critical_pressure_freshness_evidence(
        directory,
        enriched,
        pressure_latency_payload=pressure_latency_payload,
        timing_summary=timing_summary,
        high_pressure_payload=high_pressure_first_point_payload,
    )
    critical_pressure_freshness_path.write_text(
        json.dumps(critical_pressure_freshness_payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    timing_summary_path.write_text(json.dumps(timing_summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    positive_preseal_timing_path.write_text(
        json.dumps(positive_preseal_timing_payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    enriched.update(
        {
            "workflow_timing_trace_artifact": str(timing_trace_path),
            "workflow_timing_summary_artifact": str(timing_summary_path),
            "workflow_timing_summary": timing_summary,
            "workflow_timing_event_fields": list(TIMING_EVENT_FIELDS),
            "workflow_timing_artifacts_retrospective": bool(timing_summary.get("retrospective", False)),
            "positive_preseal_timing_diagnostics_artifact": str(positive_preseal_timing_path),
            "positive_preseal_timing_diagnostics": positive_preseal_timing_payload,
            "co2_route_conditioning_evidence_artifact": str(co2_route_conditioning_path),
            "co2_route_conditioning_evidence": co2_route_conditioning_payload,
            **final_safe_stop_payload,
            "chamber_stop_command_sent": bool(
                enriched.get("chamber_stop_command_sent")
                or final_safe_stop_payload.get("final_safe_stop_chamber_stop_command_sent")
            ),
            "route_open_pressure_surge_evidence_artifact": str(route_open_surge_path),
            "route_open_pressure_surge_evidence": route_open_surge_payload,
            "pressure_read_latency_diagnostics_artifact": str(pressure_latency_path),
            "pressure_read_latency_samples_artifact": str(pressure_latency_samples_path),
            "pressure_read_latency_diagnostics": pressure_latency_payload,
            "high_pressure_first_point_evidence_artifact": str(high_pressure_first_point_path),
            "high_pressure_first_point_evidence": high_pressure_first_point_payload,
            "critical_pressure_freshness_evidence_artifact": str(critical_pressure_freshness_path),
            "critical_pressure_freshness_evidence": critical_pressure_freshness_payload,
            "high_pressure_first_point_enabled": high_pressure_first_point_payload.get("enabled"),
            "high_pressure_first_point_decision": high_pressure_first_point_payload.get("decision"),
            "high_pressure_first_point_warning_count": high_pressure_first_point_payload.get("warning_count"),
            "primary_pressure_source": pressure_latency_payload.get("primary_pressure_source"),
            "abort_decision_pressure_source": pressure_latency_payload.get("pressure_source_used_for_abort"),
            "pressure_read_latency_warning_count": pressure_latency_payload.get("warning_count"),
            "stale_pressure_sample_count": sum(
                int(value or 0) for value in dict(pressure_latency_payload.get("stale_sample_count_by_source") or {}).values()
            ),
            "route_open_to_first_pressure_request_s": pressure_latency_payload.get(
                "route_open_to_first_pressure_request_s"
            ),
            "route_open_to_first_pressure_response_s": pressure_latency_payload.get(
                "route_open_to_first_pressure_response_s"
            ),
            "route_open_to_first_pressure_sample_s": route_open_surge_payload.get(
                "route_open_to_first_pressure_sample_s"
            ),
            "route_open_pressure_delta_hpa": route_open_surge_payload.get("pressure_delta_after_route_open_hpa"),
            "route_open_pressure_rise_rate_hpa_per_s": route_open_surge_payload.get(
                "pressure_rise_rate_after_route_open_hpa_per_s"
            ),
            "positive_preseal_timing_warning_codes": [
                str(item.get("warning_code") or "")
                for item in list(timing_summary.get("preseal_timing_warnings_all") or [])
                if isinstance(item, Mapping) and str(item.get("warning_code") or "")
            ],
            "positive_preseal_timing_warning_count": timing_summary.get(
                "preseal_timing_warning_count_total",
                positive_preseal_timing_payload.get("warning_count"),
            ),
            "positive_preseal_timing_warning_count_total": timing_summary.get(
                "preseal_timing_warning_count_total",
                positive_preseal_timing_payload.get("warning_count"),
            ),
            "positive_preseal_timing_warning_count_severe": timing_summary.get(
                "preseal_timing_warning_count_severe"
            ),
            "route_open_to_first_pressure_rise_s": positive_preseal_timing_payload.get(
                "first_pressure_rise_detected_elapsed_s"
            ),
            "route_open_to_ready_s": positive_preseal_timing_payload.get("time_from_route_open_to_ready_s"),
            "positive_preseal_start_to_ready_s": positive_preseal_timing_payload.get(
                "time_from_positive_preseal_start_to_ready_s"
            ),
            "ready_to_seal_command_s": positive_preseal_timing_payload.get(
                "seal_command_latency_after_ready_s"
            ),
            "ready_to_seal_confirm_s": positive_preseal_timing_payload.get(
                "seal_confirm_latency_after_ready_s"
            ),
            "pressure_increase_after_ready_before_seal_hpa": positive_preseal_timing_payload.get(
                "pressure_increase_after_ready_before_seal_hpa"
            ),
            "digital_gauge_mode": pressure_latency_payload.get("digital_gauge_mode"),
            "digital_gauge_continuous_active": pressure_latency_payload.get("digital_gauge_continuous_active"),
            "latest_frame_age_s": pressure_latency_payload.get("latest_frame_age_s"),
            "critical_window_blocking_query_count": pressure_latency_payload.get(
                "critical_window_blocking_query_count"
            ),
            "critical_window_uses_latest_frame": pressure_latency_payload.get("critical_window_uses_latest_frame"),
            "critical_window_uses_query": pressure_latency_payload.get("critical_window_uses_query"),
            "co2_route_conditioning_evidence_artifact": str(co2_route_conditioning_path),
            "co2_route_conditioning_decision": co2_route_conditioning_payload.get("conditioning_decision"),
            "co2_route_conditioning_vent_tick_count": co2_route_conditioning_payload.get("vent_tick_count"),
            "co2_route_conditioning_vent_tick_avg_gap_s": co2_route_conditioning_payload.get("vent_tick_avg_gap_s"),
            "co2_route_conditioning_vent_tick_max_gap_s": co2_route_conditioning_payload.get("vent_tick_max_gap_s"),
            "route_open_to_first_vent_s": co2_route_conditioning_payload.get("route_open_to_first_vent_s"),
            "last_vent_command_age_s": co2_route_conditioning_payload.get("last_vent_command_age_s"),
            "vent_heartbeat_interval_s": co2_route_conditioning_payload.get("vent_heartbeat_interval_s"),
            "vent_heartbeat_gap_exceeded": co2_route_conditioning_payload.get("vent_heartbeat_gap_exceeded"),
            "pressure_monitor_interval_s": co2_route_conditioning_payload.get("pressure_monitor_interval_s"),
            "digital_gauge_latest_age_s": co2_route_conditioning_payload.get("digital_gauge_latest_age_s"),
            "continuous_stream_stale": co2_route_conditioning_payload.get("continuous_stream_stale"),
            "continuous_stream_age_s": co2_route_conditioning_payload.get("continuous_stream_age_s"),
            "digital_gauge_stream_stale": co2_route_conditioning_payload.get("digital_gauge_stream_stale"),
            "selected_pressure_source": co2_route_conditioning_payload.get("selected_pressure_source"),
            "selected_pressure_sample_age_s": co2_route_conditioning_payload.get(
                "selected_pressure_sample_age_s"
            ),
            "selected_pressure_sample_is_stale": co2_route_conditioning_payload.get(
                "selected_pressure_sample_is_stale"
            ),
            "selected_pressure_parse_ok": co2_route_conditioning_payload.get("selected_pressure_parse_ok"),
            "selected_pressure_freshness_ok": co2_route_conditioning_payload.get(
                "selected_pressure_freshness_ok"
            ),
            "pressure_freshness_decision_source": co2_route_conditioning_payload.get(
                "pressure_freshness_decision_source"
            ),
            "selected_pressure_fail_closed_reason": co2_route_conditioning_payload.get(
                "selected_pressure_fail_closed_reason"
            ),
            "digital_gauge_sequence_progress": co2_route_conditioning_payload.get("digital_gauge_sequence_progress"),
            "conditioning_pressure_abort_hpa": co2_route_conditioning_payload.get("conditioning_pressure_abort_hpa"),
            "pressure_overlimit_seen": co2_route_conditioning_payload.get("pressure_overlimit_seen"),
            "pressure_overlimit_source": co2_route_conditioning_payload.get("pressure_overlimit_source"),
            "pressure_overlimit_hpa": co2_route_conditioning_payload.get("pressure_overlimit_hpa"),
            "fail_closed_before_vent_off": co2_route_conditioning_payload.get("fail_closed_before_vent_off"),
            "vent_off_sent_at": co2_route_conditioning_payload.get("vent_off_sent_at"),
            "seal_command_sent": co2_route_conditioning_payload.get("seal_command_sent"),
            "co2_route_conditioning_pressure_max_hpa": co2_route_conditioning_payload.get(
                "pressure_max_during_conditioning_hpa"
            ),
            "sealed_during_conditioning": co2_route_conditioning_payload.get("sealed_during_conditioning"),
        }
    )
    if bool(final_safe_stop_payload.get("final_safe_stop_chamber_stop_command_sent")):
        reasons = list(enriched.get("a2_decision_reasons") or [])
        reasons.append("chamber_stop_command_sent")
        enriched["a2_decision_reasons"] = list(dict.fromkeys(reasons))
    enriched = _finalize_artifact_decision(enriched, directory)

    (directory / "summary.json").write_text(json.dumps(enriched, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    (directory / "no_write_guard.json").write_text(
        json.dumps(
            {
                "run_id": enriched.get("run_id"),
                "no_write": enriched.get("no_write"),
                "attempted_write_count": enriched.get("attempted_write_count"),
                "blocked_write_events": enriched.get("blocked_write_events"),
                "identity_write_command_sent": enriched.get("identity_write_command_sent"),
                "persistent_write_command_sent": enriched.get("persistent_write_command_sent"),
                "chamber_stop_command_sent": enriched.get("chamber_stop_command_sent"),
                "final_safe_stop_warning_count": enriched.get("final_safe_stop_warning_count"),
                "final_safe_stop_warnings": enriched.get("final_safe_stop_warnings"),
                "final_safe_stop_chamber_stop_warning": enriched.get("final_safe_stop_chamber_stop_warning"),
                "final_safe_stop_chamber_stop_attempted": enriched.get(
                    "final_safe_stop_chamber_stop_attempted"
                ),
                "final_safe_stop_chamber_stop_command_sent": enriched.get(
                    "final_safe_stop_chamber_stop_command_sent"
                ),
                "final_safe_stop_chamber_stop_result": enriched.get("final_safe_stop_chamber_stop_result"),
                "readiness_result": enriched.get("readiness_result"),
                "a2_final_decision": enriched.get("a2_final_decision"),
                "a2_execution_result": enriched.get("a2_execution_result"),
                "a2_fail_reason": enriched.get("a2_fail_reason"),
                "final_decision": enriched.get("final_decision"),
                "real_machine_acceptance_evidence": False,
            },
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    (directory / "readiness.json").write_text(
        json.dumps(
            {
                "run_id": enriched.get("run_id"),
                "readiness_result": enriched.get("readiness_result"),
                "a2_final_decision": enriched.get("a2_final_decision"),
                "a2_execution_result": enriched.get("a2_execution_result"),
                "a2_decision_reasons": enriched.get("a2_decision_reasons"),
                "a2_authorized_pressure_points_hpa": enriched.get("a2_authorized_pressure_points_hpa"),
                "planned_pressure_points_completed": enriched.get("planned_pressure_points_completed"),
                "points_completed": enriched.get("points_completed"),
                "sample_count": enriched.get("sample_count"),
                "attempted_write_count": enriched.get("attempted_write_count"),
                "identity_write_command_sent": enriched.get("identity_write_command_sent"),
                "persistent_write_command_sent": enriched.get("persistent_write_command_sent"),
                "chamber_stop_command_sent": enriched.get("chamber_stop_command_sent"),
                "final_safe_stop_warning_count": enriched.get("final_safe_stop_warning_count"),
                "final_safe_stop_warnings": enriched.get("final_safe_stop_warnings"),
                "final_safe_stop_chamber_stop_warning": enriched.get("final_safe_stop_chamber_stop_warning"),
                "final_safe_stop_chamber_stop_attempted": enriched.get(
                    "final_safe_stop_chamber_stop_attempted"
                ),
                "final_safe_stop_chamber_stop_command_sent": enriched.get(
                    "final_safe_stop_chamber_stop_command_sent"
                ),
                "final_safe_stop_chamber_stop_result": enriched.get("final_safe_stop_chamber_stop_result"),
                "effective_analyzer_mapping_status": enriched.get("effective_analyzer_mapping_status"),
                "all_enabled_analyzers_mode2_ready": enriched.get("all_enabled_analyzers_mode2_ready"),
                "preseal_atmosphere_hold_decision": enriched.get("preseal_atmosphere_hold_decision"),
                "preseal_atmosphere_hold_pressure_limit_hpa": enriched.get(
                    "preseal_atmosphere_hold_pressure_limit_hpa"
                ),
                "preseal_atmosphere_hold_max_measured_pressure_hpa": enriched.get(
                    "preseal_atmosphere_hold_max_measured_pressure_hpa"
                ),
                "pressure_at_abort_hpa": enriched.get("pressure_at_abort_hpa"),
                "pressure_max_before_abort_hpa": enriched.get("pressure_max_before_abort_hpa"),
                "pressure_max_after_abort_hpa": enriched.get("pressure_max_after_abort_hpa"),
                "pressure_max_before_seal_hpa": enriched.get("pressure_max_before_seal_hpa"),
                "pressure_max_during_safe_stop_hpa": enriched.get("pressure_max_during_safe_stop_hpa"),
                "preseal_atmosphere_hold_pressure_limit_exceeded": enriched.get(
                    "preseal_atmosphere_hold_pressure_limit_exceeded"
                ),
                "positive_preseal_pressurization_decision": enriched.get(
                    "positive_preseal_pressurization_decision"
                ),
                "positive_preseal_ready_reached": enriched.get("positive_preseal_ready_reached"),
                "positive_preseal_seal_trigger_pressure_hpa": enriched.get(
                    "positive_preseal_seal_trigger_pressure_hpa"
                ),
            "positive_preseal_abort_reason": enriched.get("positive_preseal_abort_reason"),
            "ambient_reference_pressure_hpa": enriched.get("ambient_reference_pressure_hpa"),
            "ambient_reference_source": enriched.get("ambient_reference_source"),
            "ambient_reference_timestamp": enriched.get("ambient_reference_timestamp"),
            "ambient_reference_age_s": enriched.get("ambient_reference_age_s"),
            "positive_preseal_pressure_max_hpa": enriched.get("positive_preseal_pressure_max_hpa"),
            "positive_preseal_timing_diagnostics_artifact": enriched.get(
                "positive_preseal_timing_diagnostics_artifact"
            ),
            "co2_route_conditioning_evidence_artifact": enriched.get(
                "co2_route_conditioning_evidence_artifact"
            ),
            "positive_preseal_timing_warning_codes": enriched.get("positive_preseal_timing_warning_codes"),
            "positive_preseal_timing_warning_count": enriched.get("positive_preseal_timing_warning_count"),
            "positive_preseal_timing_warning_count_total": enriched.get(
                "positive_preseal_timing_warning_count_total"
            ),
            "positive_preseal_timing_warning_count_severe": enriched.get(
                "positive_preseal_timing_warning_count_severe"
            ),
            "route_open_pressure_surge_evidence_artifact": enriched.get(
                "route_open_pressure_surge_evidence_artifact"
            ),
            "pressure_read_latency_diagnostics_artifact": enriched.get(
                "pressure_read_latency_diagnostics_artifact"
            ),
            "pressure_read_latency_samples_artifact": enriched.get("pressure_read_latency_samples_artifact"),
            "high_pressure_first_point_evidence_artifact": enriched.get(
                "high_pressure_first_point_evidence_artifact"
            ),
            "critical_pressure_freshness_evidence_artifact": enriched.get(
                "critical_pressure_freshness_evidence_artifact"
            ),
            "high_pressure_first_point_enabled": enriched.get("high_pressure_first_point_enabled"),
            "primary_pressure_source": enriched.get("primary_pressure_source"),
            "abort_decision_pressure_source": enriched.get("abort_decision_pressure_source"),
            "a2_required_artifact_status": enriched.get("a2_required_artifact_status"),
            "real_machine_acceptance_evidence": False,
        },
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    manifest_path = directory / "run_manifest.json"
    manifest = _load_json_dict(manifest_path)
    manifest.update(
        {
            "a2_final_decision": enriched.get("a2_final_decision"),
            "a2_authorized_pressure_points_hpa": enriched.get("a2_authorized_pressure_points_hpa"),
            "planned_pressure_points_completed": enriched.get("planned_pressure_points_completed"),
            "pressure_gate_evidence_artifact": str(pressure_path),
            "preseal_atmosphere_hold_evidence_artifact": str(preseal_evidence_path),
            "preseal_atmosphere_hold_samples_artifact": str(preseal_samples_path),
            "positive_preseal_pressurization_evidence_artifact": str(positive_preseal_evidence_path),
            "positive_preseal_pressurization_samples_artifact": str(positive_preseal_samples_path),
            "positive_preseal_timing_diagnostics_artifact": str(positive_preseal_timing_path),
            "co2_route_conditioning_evidence_artifact": str(co2_route_conditioning_path),
            "workflow_timing_trace_artifact": str(timing_trace_path),
            "workflow_timing_summary_artifact": str(timing_summary_path),
            "workflow_timing_summary": timing_summary,
            "preseal_atmosphere_hold_decision": enriched.get("preseal_atmosphere_hold_decision"),
            "preseal_atmosphere_hold_pressure_limit_hpa": enriched.get(
                "preseal_atmosphere_hold_pressure_limit_hpa"
            ),
            "preseal_atmosphere_hold_max_measured_pressure_hpa": enriched.get(
                "preseal_atmosphere_hold_max_measured_pressure_hpa"
            ),
            "pressure_at_abort_hpa": enriched.get("pressure_at_abort_hpa"),
            "pressure_max_before_abort_hpa": enriched.get("pressure_max_before_abort_hpa"),
            "pressure_max_after_abort_hpa": enriched.get("pressure_max_after_abort_hpa"),
            "pressure_max_before_seal_hpa": enriched.get("pressure_max_before_seal_hpa"),
            "pressure_max_during_safe_stop_hpa": enriched.get("pressure_max_during_safe_stop_hpa"),
            "preseal_atmosphere_hold_pressure_limit_exceeded": enriched.get(
                "preseal_atmosphere_hold_pressure_limit_exceeded"
            ),
            "positive_preseal_pressurization_decision": enriched.get(
                "positive_preseal_pressurization_decision"
            ),
            "positive_preseal_ready_reached": enriched.get("positive_preseal_ready_reached"),
            "positive_preseal_seal_trigger_pressure_hpa": enriched.get(
                "positive_preseal_seal_trigger_pressure_hpa"
            ),
            "positive_preseal_abort_reason": enriched.get("positive_preseal_abort_reason"),
            "ambient_reference_pressure_hpa": enriched.get("ambient_reference_pressure_hpa"),
            "ambient_reference_source": enriched.get("ambient_reference_source"),
            "positive_preseal_pressure_max_hpa": enriched.get("positive_preseal_pressure_max_hpa"),
            "positive_preseal_timing_warning_codes": enriched.get("positive_preseal_timing_warning_codes"),
            "positive_preseal_timing_warning_count": enriched.get("positive_preseal_timing_warning_count"),
            "positive_preseal_timing_warning_count_total": enriched.get(
                "positive_preseal_timing_warning_count_total"
            ),
            "positive_preseal_timing_warning_count_severe": enriched.get(
                "positive_preseal_timing_warning_count_severe"
            ),
            "co2_route_conditioning_decision": enriched.get("co2_route_conditioning_decision"),
            "co2_route_conditioning_vent_tick_count": enriched.get("co2_route_conditioning_vent_tick_count"),
            "sealed_during_conditioning": enriched.get("sealed_during_conditioning"),
            "route_open_pressure_surge_evidence_artifact": str(route_open_surge_path),
            "pressure_read_latency_diagnostics_artifact": str(pressure_latency_path),
            "pressure_read_latency_samples_artifact": str(pressure_latency_samples_path),
            "high_pressure_first_point_evidence_artifact": str(high_pressure_first_point_path),
            "critical_pressure_freshness_evidence_artifact": str(critical_pressure_freshness_path),
            "high_pressure_first_point_enabled": enriched.get("high_pressure_first_point_enabled"),
            "high_pressure_first_point_decision": enriched.get("high_pressure_first_point_decision"),
            "primary_pressure_source": enriched.get("primary_pressure_source"),
            "abort_decision_pressure_source": enriched.get("abort_decision_pressure_source"),
            "not_real_acceptance_evidence": True,
            "v2_replaces_v1_claim": False,
        }
    )
    artifacts = manifest.get("artifacts")
    if not isinstance(artifacts, dict):
        artifacts = {}
    output_files = list(artifacts.get("output_files") or [])
    for path in (
        str(positive_preseal_evidence_path),
        str(positive_preseal_samples_path),
        str(positive_preseal_timing_path),
        str(co2_route_conditioning_path),
        str(route_open_surge_path),
        str(pressure_latency_path),
        str(pressure_latency_samples_path),
        str(high_pressure_first_point_path),
        str(critical_pressure_freshness_path),
        str(timing_trace_path),
        str(timing_summary_path),
    ):
        if path not in output_files:
            output_files.append(path)
    artifacts["output_files"] = output_files
    manifest["artifacts"] = artifacts
    manifest["workflow_timing_artifacts"] = {
        "trace": str(timing_trace_path),
        "summary": str(timing_summary_path),
        "event_fields": list(TIMING_EVENT_FIELDS),
        "not_real_acceptance_evidence": True,
    }
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    (directory / "human_readable_report.md").write_text(render_run001_a2_human_report(enriched), encoding="utf-8")
    return common_paths


def export_runtime_run001_a2_artifacts(host: Any, run_dir: str | Path) -> dict[str, str]:
    service = getattr(host, "service", None)
    raw_cfg = getattr(service, "_raw_cfg", None)
    if not isinstance(raw_cfg, Mapping) or not is_run001_a2_no_write_pressure_sweep(raw_cfg):
        return {}
    guard = getattr(service, "no_write_guard", None)
    config_path = getattr(service, "_config_path", None)
    artifact_paths = {
        "summary": str(Path(run_dir) / "summary.json"),
        "manifest": str(Path(run_dir) / "manifest.json"),
        "trace": str(Path(run_dir) / "route_trace.jsonl"),
    }
    service_summary = _load_json_dict(Path(run_dir) / "summary.json")
    service_status = _service_status_snapshot(service)
    temperature_evidence = getattr(host, "_last_analyzer_chamber_temp_stability_evidence", None)
    if not isinstance(temperature_evidence, Mapping):
        run_state = getattr(host, "run_state", None)
        temperature_state = getattr(run_state, "temperature", None)
        temperature_evidence = getattr(temperature_state, "analyzer_chamber_temp_stability_evidence", None)
    payload = build_run001_a2_evidence_payload(
        raw_cfg,
        config_path=config_path,
        run_id=getattr(service, "run_id", None),
        run_dir=run_dir,
        point_rows=None,
        guard=guard if isinstance(guard, NoWriteGuard) else None,
        artifact_paths=artifact_paths,
        require_runtime_artifacts=True,
        service_summary=service_summary,
        service_status=service_status,
        temperature_stability_evidence=temperature_evidence if isinstance(temperature_evidence, Mapping) else None,
    )
    pressure_read_samples = getattr(host, "_pressure_read_latency_samples", None)
    if isinstance(pressure_read_samples, list):
        payload["pressure_read_latency_samples"] = [
            dict(item) for item in pressure_read_samples if isinstance(item, Mapping)
        ]
    return write_run001_a2_artifacts(run_dir, payload)
