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
            f"- preseal_atmosphere_hold_decision: {payload.get('preseal_atmosphere_hold_decision')}",
            f"- preseal_atmosphere_hold_pressure_limit_hpa: {payload.get('preseal_atmosphere_hold_pressure_limit_hpa')}",
            f"- preseal_atmosphere_hold_max_measured_pressure_hpa: {payload.get('preseal_atmosphere_hold_max_measured_pressure_hpa')}",
            f"- preseal_atmosphere_hold_pressure_limit_exceeded: {payload.get('preseal_atmosphere_hold_pressure_limit_exceeded')}",
            f"- preseal_atmosphere_hold_evidence: {payload.get('preseal_atmosphere_hold_evidence_artifact')}",
            f"- preseal_atmosphere_hold_samples: {payload.get('preseal_atmosphere_hold_samples_artifact')}",
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
    common_paths["pressure_gate_evidence"] = str(pressure_path)
    common_paths["preseal_atmosphere_hold_evidence"] = str(preseal_evidence_path)
    common_paths["preseal_atmosphere_hold_samples"] = str(preseal_samples_path)
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
    enriched.update(
        {
            "preseal_atmosphere_hold_evidence_artifact": str(preseal_evidence_path),
            "preseal_atmosphere_hold_samples_artifact": str(preseal_samples_path),
            "preseal_atmosphere_hold_decision": preseal_payload.get("decision"),
            "preseal_atmosphere_hold_pressure_limit_hpa": preseal_payload.get("pressure_limit_hpa"),
            "preseal_atmosphere_hold_max_measured_pressure_hpa": preseal_payload.get("max_measured_pressure_hpa"),
            "preseal_atmosphere_hold_pressure_limit_exceeded": preseal_payload.get("pressure_limit_exceeded"),
            "preseal_atmosphere_hold_periodic_vent_reassertion_count": preseal_payload.get(
                "periodic_vent_reassertion_count"
            ),
            "preseal_atmosphere_hold_failure_reason": preseal_payload.get("failure_reason"),
            "vent_status_2_is_not_continuous_atmosphere_evidence": True,
        }
    )
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
            "not_real_acceptance_evidence": True,
            "v2_replaces_v1_claim": False,
        }
    )
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
