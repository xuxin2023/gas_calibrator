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
    "pressure_limit_hpa",
    "pressure_limit_exceeded",
    "route_opened",
    "sealed",
    "pressure_control_started",
    "decision",
    "failure_reason",
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
    "ready_pressure_hpa",
    "abort_pressure_hpa",
    "preseal_ready_pressure_hpa",
    "preseal_abort_pressure_hpa",
    "preseal_ready_timeout_s",
    "preseal_pressure_poll_interval_s",
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
            row_exceeded = bool(measured_pressure is not None and measured_pressure > pressure_limit)
            limit_exceeded = limit_exceeded or row_exceeded
            if row_exceeded:
                failure_reasons.append("preseal_atmosphere_hold_pressure_limit_exceeded")

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
            samples.append(
                {
                    "timestamp": item.get("ts") or item.get("timestamp") or "",
                    "elapsed_s": _trace_elapsed_s(ts, route_open_ts),
                    "vent_command_sent": vent_command_sent,
                    "vent_query_status": vent_query_status,
                    "output_state": output_state,
                    "isolation_state": isolation_state,
                    "measured_pressure_hpa": measured_pressure,
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
        "pressure_limit_exceeded": limit_exceeded,
        "decision": decision,
        "failure_reason": "; ".join(str(item) for item in deduped_reasons if str(item)),
        "vent_status_2_is_not_continuous_atmosphere_evidence": True,
        "sample_count": len(samples),
        "samples": samples,
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
        "vent_closed_at": "",
        "vent_command_result": "",
        "output_state": None,
        "isolation_state": None,
        "elapsed_s": None,
        "pressure_hpa": None,
        "current_line_pressure_hpa": None,
        "positive_preseal_pressure_hpa": None,
        "pressure_rise_rate_hpa_per_s": None,
        "pressure_samples_count": 0,
        "pressure_max_hpa": None,
        "pressure_min_hpa": None,
        "ready_reached": False,
        "ready_reached_at_pressure_hpa": None,
        "seal_command_sent": False,
        "seal_trigger_pressure_hpa": None,
        "seal_trigger_elapsed_s": None,
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
        "ready_pressure_hpa": latest.get("ready_pressure_hpa", latest.get("preseal_ready_pressure_hpa")),
        "abort_pressure_hpa": latest.get("abort_pressure_hpa", latest.get("preseal_abort_pressure_hpa")),
        "preseal_ready_pressure_hpa": latest.get("preseal_ready_pressure_hpa"),
        "preseal_abort_pressure_hpa": latest.get("preseal_abort_pressure_hpa"),
        "preseal_ready_timeout_s": latest.get("preseal_ready_timeout_s"),
        "preseal_pressure_poll_interval_s": latest.get("preseal_pressure_poll_interval_s"),
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
    common_paths["pressure_gate_evidence"] = str(pressure_path)
    common_paths["preseal_atmosphere_hold_evidence"] = str(preseal_evidence_path)
    common_paths["preseal_atmosphere_hold_samples"] = str(preseal_samples_path)
    common_paths["positive_preseal_pressurization_evidence"] = str(positive_preseal_evidence_path)
    common_paths["positive_preseal_pressurization_samples"] = str(positive_preseal_samples_path)
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
    enriched = _finalize_artifact_decision(enriched, directory)
    timing_artifacts = ensure_workflow_timing_artifacts(directory, enriched)
    timing_summary = dict(timing_artifacts.get("summary_payload") or {})
    enriched.update(
        {
            "workflow_timing_trace_artifact": str(timing_trace_path),
            "workflow_timing_summary_artifact": str(timing_summary_path),
            "workflow_timing_summary": timing_summary,
            "workflow_timing_event_fields": list(TIMING_EVENT_FIELDS),
            "workflow_timing_artifacts_retrospective": bool(timing_summary.get("retrospective", False)),
        }
    )

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
                "effective_analyzer_mapping_status": enriched.get("effective_analyzer_mapping_status"),
                "all_enabled_analyzers_mode2_ready": enriched.get("all_enabled_analyzers_mode2_ready"),
                "preseal_atmosphere_hold_decision": enriched.get("preseal_atmosphere_hold_decision"),
                "preseal_atmosphere_hold_pressure_limit_hpa": enriched.get(
                    "preseal_atmosphere_hold_pressure_limit_hpa"
                ),
                "preseal_atmosphere_hold_max_measured_pressure_hpa": enriched.get(
                    "preseal_atmosphere_hold_max_measured_pressure_hpa"
                ),
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
    return write_run001_a2_artifacts(run_dir, payload)
