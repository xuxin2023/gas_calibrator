from __future__ import annotations

import ast
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import subprocess
from typing import Any, Mapping, Optional

from .no_write_guard import NoWriteGuard, is_run001_real_machine_dry_run


RUN001_ARTIFACT_NAMES = {
    "summary": "summary.json",
    "no_write_guard": "no_write_guard.json",
    "readiness": "readiness.json",
    "trace": "route_pressure_sample_trace.json",
    "effective_analyzer_fleet": "effective_analyzer_fleet.json",
    "manifest": "run_manifest.json",
    "report": "human_readable_report.md",
}
RUN001_MODE = "real_machine_dry_run"
RUN001_PASS = "PASS"
RUN001_FAIL = "FAIL"
RUN001_NOT_EXECUTED = "NOT_EXECUTED"
RUN001_SUCCESS_PHASES = {"completed", "complete", "success", "succeeded", "pass", "passed", "ok"}
RUN001_DUPLICATE_DEVICE_ID_BLOCK_POLICY = "block_a1_current_storage_uses_frame_id_unique_key"
RUN001_PRESSURE_GATE_POLICY = {
    "policy_id": "run001_a1_v1_compatible_pressure_ready_gate",
    "version": 1,
    "v1_semantic_compatibility": True,
    "vent_status_2_strategy": (
        "warning_only_when_controller_allows_control_and_seal_output_isolation_pressure_evidence_is_valid"
    ),
    "hard_blockers": [
        "vent_status_unavailable",
        "vent_status_in_progress_or_dangerous",
        "vent_status_not_allowed_by_controller",
        "final_vent_off_not_confirmed",
        "preseal_exit_not_verified",
        "seal_transition_not_completed",
        "seal_open_channels_present",
        "output_state_not_idle",
        "isolation_state_not_open",
        "pressure_evidence_missing",
    ],
    "warnings": [
        "vent_status_2_observed_accepted_under_v1_compatible_pressure_evidence",
    ],
}


def run001_a1_pressure_gate_policy() -> dict[str, Any]:
    return json.loads(json.dumps(RUN001_PRESSURE_GATE_POLICY))


def _as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def _as_float(value: Any) -> Optional[float]:
    try:
        return float(value)
    except Exception:
        return None


def _as_int_list(values: Any) -> list[int]:
    if values is None:
        return []
    raw = values if isinstance(values, list) else [values]
    out: list[int] = []
    for item in raw:
        try:
            out.append(int(float(item)))
        except Exception:
            continue
    return out


def _as_float_list(values: Any) -> list[float]:
    if values is None:
        return []
    raw = values if isinstance(values, list) else [values]
    out: list[float] = []
    for item in raw:
        value = _as_float(item)
        if value is not None:
            out.append(value)
    return out


def _policy(raw_cfg: Optional[Mapping[str, Any]]) -> dict[str, Any]:
    if not isinstance(raw_cfg, Mapping):
        return {}
    candidate = raw_cfg.get("run001_a1")
    if isinstance(candidate, Mapping):
        return dict(candidate)
    candidate = raw_cfg.get("run001")
    if isinstance(candidate, Mapping):
        return dict(candidate)
    return {}


def _section(raw_cfg: Optional[Mapping[str, Any]], name: str) -> dict[str, Any]:
    if not isinstance(raw_cfg, Mapping):
        return {}
    value = raw_cfg.get(name)
    return dict(value) if isinstance(value, Mapping) else {}


def _workflow(raw_cfg: Optional[Mapping[str, Any]]) -> dict[str, Any]:
    return _section(raw_cfg, "workflow")


def _paths(raw_cfg: Optional[Mapping[str, Any]]) -> dict[str, Any]:
    return _section(raw_cfg, "paths")


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[4]


def _git_value(args: list[str], default: str = "") -> str:
    try:
        completed = subprocess.run(
            ["git", *args],
            cwd=_repo_root(),
            text=True,
            capture_output=True,
            timeout=5,
            check=False,
        )
    except Exception:
        return default
    if completed.returncode != 0:
        return default
    return completed.stdout.strip() or default


def git_snapshot() -> dict[str, str]:
    return {
        "git_commit": _git_value(["rev-parse", "HEAD"], ""),
        "branch": _git_value(["branch", "--show-current"], ""),
    }


def sha256_file(path: str | Path) -> str:
    target = Path(path)
    digest = hashlib.sha256()
    with target.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def resolve_config_relative_path(config_path: str | Path | None, raw_path: Any) -> Optional[Path]:
    text = str(raw_path or "").strip()
    if not text:
        return None
    candidate = Path(text).expanduser()
    if candidate.is_absolute():
        return candidate
    base = Path(config_path).expanduser().resolve().parent if config_path else _repo_root()
    return (base / candidate).resolve()


def load_point_rows(config_path: str | Path | None, raw_cfg: Mapping[str, Any]) -> list[dict[str, Any]]:
    points_path = resolve_config_relative_path(config_path, _paths(raw_cfg).get("points_excel"))
    if points_path is None or not points_path.exists() or points_path.suffix.lower() != ".json":
        return []
    try:
        payload = json.loads(points_path.read_text(encoding="utf-8"))
    except Exception:
        return []
    if isinstance(payload, list):
        return [dict(item) for item in payload if isinstance(item, Mapping)]
    if isinstance(payload, Mapping):
        points = payload.get("points")
        if isinstance(points, list):
            return [dict(item) for item in points if isinstance(item, Mapping)]
    return []


def build_v1_fallback_status(*, disable_v1: bool = False, changed_paths: Optional[list[str]] = None) -> dict[str, Any]:
    root = _repo_root()
    run_app = root / "run_app.py"
    workflow_runner = root / "src" / "gas_calibrator" / "workflow" / "runner.py"
    forbidden = []
    for path in list(changed_paths or []):
        normalized = str(path).replace("\\", "/").lower().lstrip("./")
        if normalized == "run_app.py" or normalized.startswith("src/gas_calibrator/workflow/"):
            forbidden.append(str(path))
    available = run_app.exists() and workflow_runner.exists() and not bool(disable_v1) and not forbidden
    return {
        "available": bool(available),
        "disable_v1": bool(disable_v1),
        "run_app_exists": run_app.exists(),
        "workflow_runner_exists": workflow_runner.exists(),
        "forbidden_changed_paths": forbidden,
        "status": "available" if available else "blocked",
    }


def summarize_plan(raw_cfg: Mapping[str, Any], point_rows: Optional[list[dict[str, Any]]] = None) -> dict[str, Any]:
    workflow = _workflow(raw_cfg)
    points = list(point_rows or [])
    pressure_points: list[Any] = []
    routes: list[str] = []
    temperatures: list[float] = []
    co2_values: list[float] = []
    for row in points:
        route = str(row.get("route", "") or "").strip().lower()
        if route:
            routes.append(route)
        temp = _as_float(row.get("temperature_c", row.get("temperature", row.get("temp_chamber_c"))))
        if temp is not None:
            temperatures.append(temp)
        pressure = _as_float(row.get("pressure_hpa", row.get("target_pressure_hpa", row.get("pressure"))))
        if pressure is not None:
            pressure_points.append(pressure)
        co2 = _as_float(row.get("co2_ppm", row.get("co2")))
        if co2 is not None:
            co2_values.append(co2)
    if not pressure_points:
        selected_pressure = workflow.get("selected_pressure_points")
        if selected_pressure is not None:
            pressure_points = list(selected_pressure if isinstance(selected_pressure, list) else [selected_pressure])
    selected_temps = _as_float_list(workflow.get("selected_temps_c"))
    if not selected_temps:
        selected_temps = sorted(set(temperatures))
    sampling_cfg = workflow.get("sampling") if isinstance(workflow.get("sampling"), Mapping) else {}
    return {
        "route_id": "co2_single_route",
        "route_name": "CO2 only single route",
        "routes": sorted(set(routes)) if routes else [str(workflow.get("route_mode", "") or "").strip().lower()],
        "temperature_group": selected_temps,
        "pressure_points": pressure_points,
        "co2_points": co2_values,
        "sample_plan": {
            "count": sampling_cfg.get("count"),
            "stable_count": sampling_cfg.get("stable_count"),
            "interval_s": sampling_cfg.get("interval_s"),
            "co2_interval_s": sampling_cfg.get("co2_interval_s"),
        },
    }


def summarize_enabled_analyzers(raw_cfg: Mapping[str, Any]) -> list[dict[str, Any]]:
    devices = _section(raw_cfg, "devices")
    analyzers = devices.get("gas_analyzers")
    if not isinstance(analyzers, list):
        return []
    enabled: list[dict[str, Any]] = []
    for index, item in enumerate(analyzers):
        if not isinstance(item, Mapping) or not _as_bool(item.get("enabled", True)):
            continue
        enabled.append(
            {
                "index": len(enabled),
                "config_index": index,
                "name": str(item.get("name") or f"analyzer_{len(enabled)}"),
                "port": str(item.get("port") or "").strip().upper(),
                "device_id": str(item.get("device_id") or "").strip(),
                "mode": item.get("mode"),
                "active_send": _as_bool(item.get("active_send")),
                "baud": item.get("baud"),
            }
        )
    return enabled


def summarize_duplicate_device_ids(raw_cfg: Mapping[str, Any]) -> dict[str, Any]:
    enabled = summarize_enabled_analyzers(raw_cfg)
    ports_by_id: dict[str, list[str]] = {}
    for item in enabled:
        device_id = str(item.get("device_id") or "").strip()
        port = str(item.get("port") or "").strip().upper()
        if not device_id or not port:
            continue
        ports_by_id.setdefault(device_id, []).append(port)
    duplicate_map = {
        device_id: ports for device_id, ports in ports_by_id.items()
        if len(ports) > 1
    }
    duplicate_values = sorted(duplicate_map)
    duplicate_value = duplicate_values[0] if duplicate_values else ""
    configured_policy = str(_policy(raw_cfg).get("duplicate_device_id_policy") or "").strip()
    policy = configured_policy or (RUN001_DUPLICATE_DEVICE_ID_BLOCK_POLICY if duplicate_map else "not_applicable")
    policy_lower = policy.lower()
    blocking = bool(duplicate_map) and (
        policy_lower.startswith("block")
        or policy_lower in {"fail", "fail_a1", "block_a1"}
    )
    return {
        "duplicate_device_id_detected": bool(duplicate_map),
        "duplicate_device_id_value": duplicate_value,
        "duplicate_device_id_ports": duplicate_map.get(duplicate_value, []),
        "duplicate_device_id_map": duplicate_map,
        "duplicate_device_id_policy": policy,
        "duplicate_device_id_status": "blocked" if blocking else ("warning" if duplicate_map else "not_detected"),
        "duplicate_device_id_blocks_a1": blocking,
    }


def collect_trace_summary(run_dir: str | Path | None) -> dict[str, Any]:
    if run_dir is None:
        return {
            "actual_route_steps": [],
            "actual_pressure_steps": [],
            "wait_gate_summary": [],
            "sample_summary": [],
        }
    trace_path = Path(run_dir) / "route_trace.jsonl"
    entries: list[dict[str, Any]] = []
    if trace_path.exists():
        for line in trace_path.read_text(encoding="utf-8").splitlines():
            text = line.strip()
            if not text:
                continue
            try:
                item = json.loads(text)
            except Exception:
                continue
            if isinstance(item, Mapping):
                entries.append(dict(item))
    route_steps = [
        item for item in entries
        if "route" in str(item.get("action", "")).lower() or "valve" in str(item.get("action", "")).lower()
    ]
    pressure_steps = [
        item for item in entries
        if "pressure" in str(item.get("action", "")).lower() or "seal" in str(item.get("action", "")).lower()
    ]
    wait_steps = [
        item for item in entries
        if str(item.get("action", "")).lower().startswith("wait") or "stable" in str(item.get("action", "")).lower()
    ]
    sample_steps = [
        item for item in entries
        if "sample" in str(item.get("action", "")).lower()
    ]
    return {
        "actual_route_steps": route_steps,
        "actual_pressure_steps": pressure_steps,
        "wait_gate_summary": wait_steps,
        "sample_summary": sample_steps,
    }


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


def _phase_text(value: Any) -> str:
    raw = getattr(value, "value", value)
    return str(raw or "").strip().lower()


def _status_to_mapping(status: Any) -> dict[str, Any]:
    if status is None:
        return {}
    if isinstance(status, Mapping):
        return dict(status)
    return {
        "phase": _phase_text(getattr(status, "phase", "")),
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


def _as_nonnegative_int(value: Any, default: int = 0) -> int:
    try:
        return max(0, int(value))
    except Exception:
        return default


def _nested(mapping: Mapping[str, Any], *keys: str) -> Any:
    current: Any = mapping
    for key in keys:
        if not isinstance(current, Mapping):
            return None
        current = current.get(key)
    return current


def _port_text(value: Any) -> str:
    return str(value or "").strip().upper()


def _load_mode2_truth_audit(
    raw_cfg: Mapping[str, Any],
    *,
    config_path: str | Path | None = None,
) -> tuple[Optional[Path], dict[str, Any]]:
    policy = _policy(raw_cfg)
    audit_path = resolve_config_relative_path(config_path, policy.get("mode2_truth_audit_path"))
    if audit_path is None:
        return None, {}
    return audit_path, _load_json_dict(audit_path)


def _truth_analyzers_by_port(audit_payload: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    rows = audit_payload.get("analyzers")
    if not isinstance(rows, list):
        return {}
    by_port: dict[str, dict[str, Any]] = {}
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        port = _port_text(row.get("configured_port") or row.get("port"))
        if port:
            by_port[port] = dict(row)
    return by_port


def _truth_mode2_ready_ports(audit_payload: Mapping[str, Any]) -> list[str]:
    ports: list[str] = []
    for port, row in _truth_analyzers_by_port(audit_payload).items():
        if (
            _as_bool(row.get("active_send_detected"))
            and _as_nonnegative_int(row.get("mode2_frame_count")) > 0
            and str(row.get("stable_device_id") or "").strip()
        ):
            ports.append(port)
    return sorted(ports)


def build_effective_analyzer_fleet_payload(
    raw_cfg: Mapping[str, Any],
    *,
    config_path: str | Path | None = None,
    guard_payload: Optional[Mapping[str, Any]] = None,
) -> dict[str, Any]:
    policy = _policy(raw_cfg)
    enabled = summarize_enabled_analyzers(raw_cfg)
    audit_path, audit_payload = _load_mode2_truth_audit(raw_cfg, config_path=config_path)
    truth_by_port = _truth_analyzers_by_port(audit_payload)
    truth_ready_ports = _truth_mode2_ready_ports(audit_payload)
    configured_ports = sorted(_port_text(item.get("port")) for item in enabled if _port_text(item.get("port")))
    mapping_source = str(policy.get("enabled_analyzer_list_source") or "config.devices.gas_analyzers")
    checked = bool(audit_payload)
    intended_effective_match = True if not checked else configured_ports == truth_ready_ports

    guard = dict(guard_payload or {})
    no_write_guard_active = _as_bool(policy.get("no_write", raw_cfg.get("no_write"))) or _as_bool(guard.get("no_write"))
    persistent_write_command_sent = bool(guard.get("persistent_write_command_sent", False))
    identity_write_command_sent = bool(guard.get("identity_write_command_sent", False))

    fleet: list[dict[str, Any]] = []
    for item in enabled:
        port = _port_text(item.get("port"))
        truth = truth_by_port.get(port, {})
        bytes_received = _as_nonnegative_int(truth.get("bytes_received"))
        mode2_frames = _as_nonnegative_int(truth.get("mode2_frame_count"))
        mode1_frames = _as_nonnegative_int(truth.get("mode1_frame_count"))
        stable_device_id = str(truth.get("stable_device_id") or "").strip()
        expected_device_id = str(item.get("device_id") or "").strip()
        identity_matches = bool(stable_device_id and expected_device_id and stable_device_id == expected_device_id)
        active_send_detected = _as_bool(truth.get("active_send_detected")) or bytes_received > 0
        mode2_ready = bool(active_send_detected and mode2_frames > 0 and identity_matches)
        if mode2_ready:
            readdata_response_status = "not_required_passive_mode2_active_send"
        elif truth:
            if active_send_detected and mode1_frames > 0:
                readdata_response_status = "not_required_passive_active_send_not_mode2"
            elif active_send_detected:
                readdata_response_status = "not_required_passive_active_send_identity_not_ready"
            else:
                readdata_response_status = "not_required_passive_timeout_no_active_send"
        else:
            readdata_response_status = "not_checked"
        fleet.append(
            {
                "logical_name": str(item.get("name") or ""),
                "expected_device_id": expected_device_id,
                "configured_port": port,
                "resolved_port": port,
                "enabled": True,
                "mapping_source": mapping_source,
                "whether_from_detected_truth_list": bool(truth),
                "active_send_bytes_received": bytes_received,
                "mode2_ready": mode2_ready,
                "stable_device_id": stable_device_id,
                "readdata_response_status": readdata_response_status,
                "persistent_write_command_sent": persistent_write_command_sent,
                "identity_write_command_sent": identity_write_command_sent,
                "no_write_guard_active": no_write_guard_active,
                "active_send_detected": active_send_detected,
                "mode1_frame_count": mode1_frames,
                "mode2_frame_count": mode2_frames,
                "identity_matches_expected": identity_matches,
            }
        )

    all_enabled_mode2_ready = all(item["mode2_ready"] for item in fleet) if fleet else False
    if not checked:
        mapping_status = "not_checked"
    elif intended_effective_match and all_enabled_mode2_ready:
        mapping_status = "match"
    else:
        mapping_status = "mismatch"
    return {
        "schema_version": "run001_a1.effective_analyzer_fleet.1",
        "artifact_type": "run001_a1_effective_analyzer_fleet",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "mapping_source": mapping_source,
        "mode2_truth_audit_path": "" if audit_path is None else str(audit_path),
        "truth_audit_checked": checked,
        "configured_ports": configured_ports,
        "detected_truth_mode2_ports": truth_ready_ports,
        "intended_effective_match": intended_effective_match,
        "all_enabled_mode2_ready": all_enabled_mode2_ready,
        "mapping_status": mapping_status,
        "analyzers": fleet,
        "identity_write_command_sent": identity_write_command_sent,
        "persistent_write_command_sent": persistent_write_command_sent,
        "no_write_guard_active": no_write_guard_active,
        "not_real_acceptance_evidence": True,
    }


def _extract_failed_devices(*texts: Any) -> list[str]:
    for value in texts:
        text = str(value or "")
        if not text:
            continue
        match = re.search(r"failed_devices\s*=\s*(\[[^\]]*\])", text)
        if match:
            try:
                parsed = ast.literal_eval(match.group(1))
            except Exception:
                parsed = []
            if isinstance(parsed, (list, tuple)):
                return [str(item) for item in parsed if str(item or "").strip()]
        if "Device precheck failed" in text:
            return []
    return []


def _is_device_precheck_failure(*texts: Any) -> bool:
    return any("device precheck failed" in str(text or "").lower() for text in texts)


def _is_device_id_mismatch_failure(*texts: Any) -> bool:
    return any(
        marker in str(text or "").lower()
        for text in texts
        for marker in ("device_id_mismatch", "device-id mismatch", "device_id mismatch", "device id mismatch")
    )


def _step_has_planned_execution_evidence(step: Any) -> bool:
    if not isinstance(step, Mapping):
        return False
    action = str(step.get("action", "") or "").strip().lower()
    message = str(step.get("message", "") or "").strip().lower()
    result = str(step.get("result", "") or "").strip().lower()
    if any(marker in action for marker in ("final_safe_stop", "safe_stop", "restore_baseline")):
        return False
    if any(marker in message for marker in ("final safe stop", "restore baseline")):
        return False
    if result in {"error", "failed", "fail"}:
        return False
    if step.get("point_index") is not None or str(step.get("point_tag", "") or "").strip():
        return True
    if str(step.get("route", "") or "").strip():
        return True
    target = step.get("target")
    return isinstance(target, Mapping) and bool(target)


def _has_completed_execution_step(steps: Any) -> bool:
    return any(_step_has_planned_execution_evidence(step) for step in list(steps or []))


def _service_summary_numbers(
    service_summary: Mapping[str, Any],
    service_status: Mapping[str, Any],
) -> tuple[int, int]:
    points_completed = _nested(service_summary, "points_completed")
    if points_completed is None:
        points_completed = _nested(service_summary, "completed_points")
    if points_completed is None:
        points_completed = _nested(service_summary, "status", "completed_points")
    if points_completed is None:
        points_completed = service_status.get("completed_points")

    sample_count = _nested(service_summary, "stats", "sample_count")
    if sample_count is None:
        sample_count = _nested(service_summary, "sample_count")

    return _as_nonnegative_int(points_completed), _as_nonnegative_int(sample_count)


def build_run001_a1_execution_decision(
    *,
    readiness: Mapping[str, Any],
    trace: Mapping[str, Any],
    service_summary: Optional[Mapping[str, Any]] = None,
    service_status: Optional[Mapping[str, Any]] = None,
    runtime_expected: bool = False,
) -> dict[str, Any]:
    summary = dict(service_summary or {})
    status = _status_to_mapping(service_status or summary.get("status"))
    if not runtime_expected and not summary and not status:
        return {
            "a1_final_decision": RUN001_NOT_EXECUTED,
            "a1_execution_result": "preflight_only",
            "a1_fail_reason": "",
            "a1_decision_reasons": [],
            "device_precheck_result": "NOT_EVALUATED",
            "failed_devices": [],
            "points_completed": 0,
            "sample_count": 0,
            "route_completed": False,
            "pressure_completed": False,
            "wait_gate_completed": False,
            "sample_completed": False,
            "service_status_phase": "",
            "service_status_message": "",
            "service_status_error": "",
            "real_machine_acceptance_evidence": False,
        }

    phase = _phase_text(status.get("phase") or _nested(summary, "status", "phase"))
    message = str(status.get("message") or _nested(summary, "status", "message") or "")
    error = str(status.get("error") or _nested(summary, "status", "error") or "")
    points_completed, sample_count = _service_summary_numbers(summary, status)
    route_completed = _has_completed_execution_step(trace.get("actual_route_steps"))
    pressure_completed = _has_completed_execution_step(trace.get("actual_pressure_steps"))
    wait_gate_completed = bool(list(trace.get("wait_gate_summary") or []))
    sample_completed = bool(list(trace.get("sample_summary") or [])) and sample_count > 0
    attempted_write_count = _as_nonnegative_int(readiness.get("attempted_write_count"))
    blocked_write_events = list(readiness.get("blocked_write_events") or [])
    artifact_status = dict(readiness.get("artifact_status") or {})
    failed_devices = _extract_failed_devices(error, message)
    device_precheck_failed = _is_device_precheck_failure(error, message)
    device_id_mismatch_failed = _is_device_id_mismatch_failure(error, message)
    reasons: list[str] = []

    if phase not in RUN001_SUCCESS_PHASES:
        reasons.append("service_status_not_success")
    if device_precheck_failed:
        reasons.append("device_precheck_failed")
    if device_id_mismatch_failed:
        reasons.append("device_id_mismatch")
    if points_completed <= 0:
        reasons.append("points_completed_zero")
    if sample_count <= 0:
        reasons.append("sample_count_zero")
    if not route_completed:
        reasons.append("route_not_completed")
    if not pressure_completed:
        reasons.append("pressure_not_completed")
    if not wait_gate_completed:
        reasons.append("wait_gate_not_completed")
    if not sample_completed:
        reasons.append("sample_not_completed")
    if attempted_write_count > 0 or blocked_write_events:
        reasons.append("attempted_write_count_gt_0")
    for name, status_payload in artifact_status.items():
        if isinstance(status_payload, Mapping) and not bool(status_payload.get("exists")):
            reasons.append(f"required_artifact_missing_{name}")
    if readiness.get("readiness_result") == RUN001_FAIL:
        reasons.append("readiness_failed")

    deduped_reasons = list(dict.fromkeys(reasons))
    fail_reason = "; ".join(deduped_reasons)
    if device_id_mismatch_failed:
        fail_reason = "device_id_mismatch"
    elif device_precheck_failed and failed_devices:
        fail_reason = f"Device precheck failed [failed_devices={failed_devices}]"
    elif device_precheck_failed:
        fail_reason = "Device precheck failed"
    elif error:
        fail_reason = error
    elif message and deduped_reasons:
        fail_reason = message

    passed = not deduped_reasons
    return {
        "a1_final_decision": RUN001_PASS if passed else RUN001_FAIL,
        "a1_execution_result": "completed" if passed else "failed",
        "a1_fail_reason": "" if passed else fail_reason,
        "a1_decision_reasons": deduped_reasons,
        "device_precheck_result": RUN001_FAIL if (device_precheck_failed or device_id_mismatch_failed) else "NOT_TRIGGERED",
        "failed_devices": failed_devices,
        "points_completed": points_completed,
        "sample_count": sample_count,
        "route_completed": route_completed,
        "pressure_completed": pressure_completed,
        "wait_gate_completed": wait_gate_completed,
        "sample_completed": sample_completed,
        "service_status_phase": phase,
        "service_status_message": message,
        "service_status_error": error,
        "real_machine_acceptance_evidence": False,
    }


def evaluate_run001_a1_readiness(
    raw_cfg: Mapping[str, Any],
    *,
    config_path: str | Path | None = None,
    point_rows: Optional[list[dict[str, Any]]] = None,
    attempted_write_count: int = 0,
    blocked_write_events: Optional[list[dict[str, Any]]] = None,
    artifact_paths: Optional[Mapping[str, Any]] = None,
    require_runtime_artifacts: bool = False,
    changed_paths: Optional[list[str]] = None,
) -> dict[str, Any]:
    policy = _policy(raw_cfg)
    workflow = _workflow(raw_cfg)
    duplicate_ids = summarize_duplicate_device_ids(raw_cfg)
    hard_stop_reasons: list[str] = []

    mode = str(policy.get("mode", raw_cfg.get("mode", "")) or "").strip().lower()
    if mode != RUN001_MODE:
        hard_stop_reasons.append("mode_not_real_machine_dry_run")

    no_write = policy.get("no_write", raw_cfg.get("no_write"))
    if not _as_bool(no_write):
        hard_stop_reasons.append("no_write_not_true")

    write_flags = {
        "allow_write_coefficients": policy.get("allow_write_coefficients"),
        "allow_write_zero": policy.get("allow_write_zero"),
        "allow_write_span": policy.get("allow_write_span"),
        "allow_write_calibration_parameters": policy.get("allow_write_calibration_parameters"),
    }
    for key, value in write_flags.items():
        if _as_bool(value):
            hard_stop_reasons.append(f"{key}_true")

    required_true_flags = (
        "allow_real_route",
        "allow_real_pressure",
        "allow_real_wait",
        "allow_real_sample",
        "allow_artifact",
        "co2_only",
        "single_route",
        "single_temperature_group",
    )
    for key in required_true_flags:
        if not _as_bool(policy.get(key)):
            hard_stop_reasons.append(f"{key}_not_true")

    route_mode = str(workflow.get("route_mode", "") or "").strip().lower()
    if route_mode != "co2_only":
        hard_stop_reasons.append("route_mode_not_co2_only")
    if "h2o" in route_mode:
        hard_stop_reasons.append("h2o_scope_requested")
    if _as_bool(policy.get("full_h2o_co2_group")) or _as_bool(policy.get("full_single_temperature_h2o_co2_group")):
        hard_stop_reasons.append("full_h2o_co2_group_requested")

    skip_co2_ppm = _as_int_list(workflow.get("skip_co2_ppm"))
    if skip_co2_ppm != [0]:
        hard_stop_reasons.append("skip_co2_ppm_not_locked_to_0")

    selected_temps = _as_float_list(workflow.get("selected_temps_c"))
    if len(selected_temps) != 1:
        hard_stop_reasons.append("not_single_temperature_group")

    points = list(point_rows or [])
    if points:
        point_routes = {
            str(row.get("route", "") or "").strip().lower()
            for row in points
            if str(row.get("route", "") or "").strip()
        }
        if any("h2o" in route for route in point_routes):
            hard_stop_reasons.append("points_include_h2o_route")
        point_temps = {
            value for value in (
                _as_float(row.get("temperature_c", row.get("temperature", row.get("temp_chamber_c"))))
                for row in points
            )
            if value is not None
        }
        if len(point_temps) > 1:
            hard_stop_reasons.append("points_include_multiple_temperature_groups")

    if _as_bool(policy.get("default_cutover_to_v2")):
        hard_stop_reasons.append("default_cutover_to_v2_true")
    disable_v1 = _as_bool(policy.get("disable_v1", raw_cfg.get("disable_v1")))
    v1_fallback_status = build_v1_fallback_status(disable_v1=disable_v1, changed_paths=changed_paths)
    if not v1_fallback_status["available"]:
        hard_stop_reasons.append("v1_fallback_unavailable_or_disabled")
    if v1_fallback_status["forbidden_changed_paths"]:
        hard_stop_reasons.append("v1_forbidden_change_detected")

    blocked_events = list(blocked_write_events or [])
    if int(attempted_write_count) > 0 or blocked_events:
        hard_stop_reasons.append("attempted_write_count_gt_0")
    if duplicate_ids["duplicate_device_id_blocks_a1"]:
        hard_stop_reasons.append("duplicate_device_id_detected")

    effective_fleet = build_effective_analyzer_fleet_payload(raw_cfg, config_path=config_path)
    if effective_fleet.get("truth_audit_checked"):
        if not bool(effective_fleet.get("intended_effective_match")):
            hard_stop_reasons.append("effective_analyzer_list_mismatch")
        if not bool(effective_fleet.get("all_enabled_mode2_ready")):
            hard_stop_reasons.append("enabled_analyzer_not_mode2_ready")

    artifact_status: dict[str, Any] = {}
    if require_runtime_artifacts:
        required = ("summary", "manifest", "trace")
        for key in required:
            path = Path(str((artifact_paths or {}).get(key, "") or ""))
            exists = path.exists() if str(path) else False
            artifact_status[key] = {"path": str(path), "exists": exists}
            if not exists:
                hard_stop_reasons.append(f"required_artifact_missing_{key}")

    passed = not hard_stop_reasons
    return {
        "readiness_result": RUN001_PASS if passed else RUN001_FAIL,
        "final_decision": RUN001_PASS if passed else RUN001_FAIL,
        "hard_stop_reasons": hard_stop_reasons,
        "attempted_write_count": int(attempted_write_count),
        "blocked_write_events": blocked_events,
        "v1_fallback_status": v1_fallback_status,
        "artifact_status": artifact_status,
        "effective_analyzer_fleet_summary": {
            "mapping_status": effective_fleet.get("mapping_status"),
            "truth_audit_checked": effective_fleet.get("truth_audit_checked"),
            "configured_ports": effective_fleet.get("configured_ports"),
            "detected_truth_mode2_ports": effective_fleet.get("detected_truth_mode2_ports"),
            "intended_effective_match": effective_fleet.get("intended_effective_match"),
            "all_enabled_mode2_ready": effective_fleet.get("all_enabled_mode2_ready"),
        },
        **duplicate_ids,
        "skip_co2_ppm": skip_co2_ppm,
        "selected_temps_c": selected_temps,
        "route_mode": route_mode,
        "h2o_single_route_readiness": "yellow",
        "full_single_temperature_h2o_co2_group_readiness": "yellow",
        "not_real_acceptance_evidence": True,
    }


def build_run001_a1_evidence_payload(
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
    changed_paths: Optional[list[str]] = None,
) -> dict[str, Any]:
    points = list(point_rows if point_rows is not None else load_point_rows(config_path, raw_cfg))
    guard_payload = guard.to_artifact() if guard is not None else {}
    attempted_write_count = int(guard_payload.get("attempted_write_count", 0) or 0)
    blocked_write_events = list(guard_payload.get("blocked_write_events", []) or [])
    identity_write_command_sent = bool(guard_payload.get("identity_write_command_sent", False))
    persistent_write_command_sent = bool(guard_payload.get("persistent_write_command_sent", False))
    effective_fleet = build_effective_analyzer_fleet_payload(
        raw_cfg,
        config_path=config_path,
        guard_payload=guard_payload,
    )
    readiness = evaluate_run001_a1_readiness(
        raw_cfg,
        config_path=config_path,
        point_rows=points,
        attempted_write_count=attempted_write_count,
        blocked_write_events=blocked_write_events,
        artifact_paths=artifact_paths,
        require_runtime_artifacts=require_runtime_artifacts,
        changed_paths=changed_paths,
    )
    policy = _policy(raw_cfg)
    workflow = _workflow(raw_cfg)
    config_safety = dict(raw_cfg.get("_config_safety") or {}) if isinstance(raw_cfg, Mapping) else {}
    execution_gate = dict(config_safety.get("execution_gate") or {})
    unsafe_step2_bypass_used = bool(
        execution_gate.get("allow_unsafe_step2_config_flag")
        and execution_gate.get("allow_unsafe_step2_config_env")
    )
    plan = summarize_plan(raw_cfg, points)
    trace = collect_trace_summary(run_dir)
    runtime_expected = bool(require_runtime_artifacts or service_summary or service_status)
    execution_decision = build_run001_a1_execution_decision(
        readiness=readiness,
        trace=trace,
        service_summary=service_summary,
        service_status=service_status,
        runtime_expected=runtime_expected,
    )
    git = git_snapshot()
    config_path_text = "" if config_path is None else str(Path(config_path).expanduser().resolve())
    config_hash = sha256_file(config_path_text) if config_path_text and Path(config_path_text).exists() else ""
    resolved_run_id = run_id or f"run001_a1_preflight_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
    enabled_analyzers = summarize_enabled_analyzers(raw_cfg)
    duplicate_ids = summarize_duplicate_device_ids(raw_cfg)
    fail_reason = str(execution_decision.get("a1_fail_reason") or "")
    if not fail_reason and readiness["hard_stop_reasons"]:
        fail_reason = "; ".join(str(item) for item in readiness["hard_stop_reasons"])
    overall_final_decision = str(readiness["final_decision"])
    if runtime_expected and execution_decision.get("a1_final_decision") != RUN001_PASS:
        overall_final_decision = RUN001_FAIL
    return {
        "schema_version": "run001_a1.no_write.1",
        "artifact_type": "run001_a1_no_write_dry_run_evidence",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "run_id": resolved_run_id,
        "git_commit": git["git_commit"],
        "branch": git["branch"],
        "config_path": config_path_text,
        "config_hash": config_hash,
        "mode": RUN001_MODE,
        "no_write": _as_bool(policy.get("no_write", raw_cfg.get("no_write"))),
        "co2_only": _as_bool(policy.get("co2_only")),
        "skip_co2_ppm": readiness["skip_co2_ppm"],
        "single_route": _as_bool(policy.get("single_route")),
        "single_temperature_group": _as_bool(policy.get("single_temperature_group")),
        "allow_real_route": _as_bool(policy.get("allow_real_route")),
        "allow_real_pressure": _as_bool(policy.get("allow_real_pressure")),
        "allow_real_wait": _as_bool(policy.get("allow_real_wait")),
        "allow_real_sample": _as_bool(policy.get("allow_real_sample")),
        "allow_artifact": _as_bool(policy.get("allow_artifact")),
        "temperature_chamber_settle_required": _as_bool(policy.get("temperature_chamber_settle_required")),
        "temperature_chamber_wait_policy": str(policy.get("temperature_chamber_wait_policy") or ""),
        "temperature_chamber_keep_running_intentional": True,
        "temperature_chamber_not_closed_is_expected_for_this_run": True,
        "temperature_chamber_not_a_failure_reason": True,
        "temperature_wait_for_target_before_continue": _as_bool(
            _nested(workflow, "stability", "temperature", "wait_for_target_before_continue")
        ),
        "allow_write_coefficients": _as_bool(policy.get("allow_write_coefficients")),
        "allow_write_zero": _as_bool(policy.get("allow_write_zero")),
        "allow_write_span": _as_bool(policy.get("allow_write_span")),
        "allow_write_calibration_parameters": _as_bool(policy.get("allow_write_calibration_parameters")),
        "unsafe_step2_bypass_used": unsafe_step2_bypass_used,
        "unsafe_step2_cli_flag_used": bool(execution_gate.get("allow_unsafe_step2_config_flag", False)),
        "unsafe_step2_env_enabled": bool(execution_gate.get("allow_unsafe_step2_config_env", False)),
        "default_cutover_to_v2": _as_bool(policy.get("default_cutover_to_v2")),
        "disable_v1": _as_bool(policy.get("disable_v1", raw_cfg.get("disable_v1"))),
        "route_id": plan["route_id"],
        "route_name": plan["route_name"],
        "temperature_group": plan["temperature_group"],
        "pressure_points": plan["pressure_points"],
        "pressure_gate_policy": run001_a1_pressure_gate_policy(),
        "sample_plan": plan["sample_plan"],
        "enabled_analyzers": enabled_analyzers,
        "analyzer_ports": [item["port"] for item in enabled_analyzers],
        "analyzer_device_ids": [item["device_id"] for item in enabled_analyzers],
        "effective_analyzer_fleet": effective_fleet,
        "effective_analyzer_fleet_summary": readiness["effective_analyzer_fleet_summary"],
        "effective_analyzer_mapping_status": effective_fleet.get("mapping_status"),
        "intended_effective_analyzer_list_match": effective_fleet.get("intended_effective_match"),
        "all_enabled_analyzers_mode2_ready": effective_fleet.get("all_enabled_mode2_ready"),
        **duplicate_ids,
        "actual_route_steps": trace["actual_route_steps"],
        "actual_pressure_steps": trace["actual_pressure_steps"],
        "wait_gate_summary": trace["wait_gate_summary"],
        "sample_summary": trace["sample_summary"],
        "artifact_paths": {str(k): str(v) for k, v in dict(artifact_paths or {}).items()},
        "attempted_write_count": readiness["attempted_write_count"],
        "blocked_write_events": readiness["blocked_write_events"],
        "identity_write_command_sent": identity_write_command_sent,
        "persistent_write_command_sent": persistent_write_command_sent,
        "v1_fallback_status": readiness["v1_fallback_status"],
        "readiness_result": readiness["readiness_result"],
        "readiness_final_decision": readiness["final_decision"],
        "final_decision": overall_final_decision,
        "hard_stop_reasons": readiness["hard_stop_reasons"],
        "artifact_status": readiness["artifact_status"],
        **execution_decision,
        "fail_reason": fail_reason,
        "h2o_single_route_readiness": readiness["h2o_single_route_readiness"],
        "full_single_temperature_h2o_co2_group_readiness": readiness[
            "full_single_temperature_h2o_co2_group_readiness"
        ],
        "not_real_acceptance_evidence": True,
        "evidence_source": "real_machine_dry_run_no_write",
        "workflow_snapshot": {
            "run_mode": workflow.get("run_mode"),
            "route_mode": workflow.get("route_mode"),
            "selected_temps_c": workflow.get("selected_temps_c"),
            "skip_co2_ppm": workflow.get("skip_co2_ppm"),
        },
    }


def render_human_report(payload: Mapping[str, Any]) -> str:
    readiness_decision = str(payload.get("readiness_result", payload.get("final_decision", RUN001_FAIL)))
    a1_decision = str(payload.get("a1_final_decision", RUN001_NOT_EXECUTED))
    execution_result = str(payload.get("a1_execution_result", "preflight_only"))
    fail_reason = str(payload.get("a1_fail_reason") or "")
    reasons = list(payload.get("hard_stop_reasons") or [])
    reason_lines = "\n".join(f"- {item}" for item in reasons) if reasons else "- none"
    a1_reasons = list(payload.get("a1_decision_reasons") or [])
    a1_reason_lines = "\n".join(f"- {item}" for item in a1_reasons) if a1_reasons else "- none"
    failed_devices = list(payload.get("failed_devices") or [])
    failed_device_line = ", ".join(str(item) for item in failed_devices) if failed_devices else "none"
    return "\n".join(
        [
            "# Run-001 / A1 no-write dry-run evidence",
            "",
            f"- run_id: {payload.get('run_id')}",
            f"- mode: {payload.get('mode')}",
            f"- no_write: {payload.get('no_write')}",
            f"- no_write_readiness_result: {readiness_decision}",
            f"- execution_result: {execution_result}",
            f"- a1_final_decision: {a1_decision}",
            f"- a1_fail_reason: {fail_reason or 'none'}",
            f"- failed_devices: {failed_device_line}",
            f"- duplicate_device_id_status: {payload.get('duplicate_device_id_status')}",
            f"- duplicate_device_id_policy: {payload.get('duplicate_device_id_policy')}",
            f"- effective_analyzer_mapping_status: {payload.get('effective_analyzer_mapping_status')}",
            f"- intended_effective_analyzer_list_match: {payload.get('intended_effective_analyzer_list_match')}",
            f"- all_enabled_analyzers_mode2_ready: {payload.get('all_enabled_analyzers_mode2_ready')}",
            f"- attempted_write_count: {payload.get('attempted_write_count')}",
            f"- identity_write_command_sent: {payload.get('identity_write_command_sent')}",
            f"- temperature_chamber_wait_policy: {payload.get('temperature_chamber_wait_policy')}",
            f"- temperature_chamber_keep_running_intentional: {payload.get('temperature_chamber_keep_running_intentional')}",
            f"- temperature_chamber_not_closed_is_expected_for_this_run: {payload.get('temperature_chamber_not_closed_is_expected_for_this_run')}",
            f"- temperature_chamber_not_a_failure_reason: {payload.get('temperature_chamber_not_a_failure_reason')}",
            f"- temperature_wait_for_target_before_continue: {payload.get('temperature_wait_for_target_before_continue')}",
            f"- pressure_gate_policy: {payload.get('pressure_gate_policy', {}).get('policy_id')}",
            f"- unsafe_step2_bypass_used: {payload.get('unsafe_step2_bypass_used')}",
            f"- V1 fallback: {payload.get('v1_fallback_status', {}).get('status')}",
            f"- H2O single-route readiness: {payload.get('h2o_single_route_readiness')}",
            f"- Full H2O+CO2 group readiness: {payload.get('full_single_temperature_h2o_co2_group_readiness')}",
            "",
            "## Hard stops",
            reason_lines,
            "",
            "## A1 execution decision",
            a1_reason_lines,
            "",
            "Readiness PASS only means the no-write/readiness gate passed; it is not A1 execution PASS.",
            "Execution FAIL means Run-001/A1 FAIL and does not authorize A2.",
            "This artifact is not real acceptance evidence and does not authorize V2 cutover or real writes.",
            "",
        ]
    )


def write_run001_a1_artifacts(run_dir: str | Path, payload: Mapping[str, Any]) -> dict[str, str]:
    directory = Path(run_dir)
    directory.mkdir(parents=True, exist_ok=True)
    artifact_paths = {key: str(directory / filename) for key, filename in RUN001_ARTIFACT_NAMES.items()}
    enriched = dict(payload)
    enriched["artifact_paths"] = dict(artifact_paths)
    summary_path = Path(artifact_paths["summary"])
    guard_path = Path(artifact_paths["no_write_guard"])
    readiness_path = Path(artifact_paths["readiness"])
    trace_path = Path(artifact_paths["trace"])
    effective_analyzer_fleet_path = Path(artifact_paths["effective_analyzer_fleet"])
    manifest_path = Path(artifact_paths["manifest"])
    report_path = Path(artifact_paths["report"])
    effective_fleet = dict(enriched.get("effective_analyzer_fleet") or {})
    effective_fleet["run_id"] = enriched.get("run_id")
    effective_fleet["artifact_path"] = artifact_paths["effective_analyzer_fleet"]
    enriched["effective_analyzer_fleet"] = effective_fleet

    summary_path.write_text(json.dumps(enriched, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    guard_path.write_text(
        json.dumps(
            {
                "run_id": enriched.get("run_id"),
                "no_write": enriched.get("no_write"),
                "attempted_write_count": enriched.get("attempted_write_count"),
                "blocked_write_events": enriched.get("blocked_write_events"),
                "identity_write_command_sent": enriched.get("identity_write_command_sent"),
                "persistent_write_command_sent": enriched.get("persistent_write_command_sent"),
                "final_decision": enriched.get("final_decision"),
                "readiness_result": enriched.get("readiness_result"),
                "a1_final_decision": enriched.get("a1_final_decision"),
                "a1_execution_result": enriched.get("a1_execution_result"),
                "a1_fail_reason": enriched.get("a1_fail_reason"),
                "real_machine_acceptance_evidence": False,
            },
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    readiness_path.write_text(
        json.dumps(
            {
                "run_id": enriched.get("run_id"),
                "readiness_result": enriched.get("readiness_result"),
                "final_decision": enriched.get("final_decision"),
                "readiness_final_decision": enriched.get("readiness_final_decision"),
                "a1_final_decision": enriched.get("a1_final_decision"),
                "a1_execution_result": enriched.get("a1_execution_result"),
                "a1_fail_reason": enriched.get("a1_fail_reason"),
                "a1_decision_reasons": enriched.get("a1_decision_reasons"),
                "device_precheck_result": enriched.get("device_precheck_result"),
                "failed_devices": enriched.get("failed_devices"),
                "points_completed": enriched.get("points_completed"),
                "sample_count": enriched.get("sample_count"),
                "route_completed": enriched.get("route_completed"),
                "pressure_completed": enriched.get("pressure_completed"),
                "wait_gate_completed": enriched.get("wait_gate_completed"),
                "sample_completed": enriched.get("sample_completed"),
                "service_status_phase": enriched.get("service_status_phase"),
                "service_status_message": enriched.get("service_status_message"),
                "service_status_error": enriched.get("service_status_error"),
                "hard_stop_reasons": enriched.get("hard_stop_reasons"),
                "effective_analyzer_fleet_summary": enriched.get("effective_analyzer_fleet_summary"),
                "effective_analyzer_mapping_status": enriched.get("effective_analyzer_mapping_status"),
                "intended_effective_analyzer_list_match": enriched.get(
                    "intended_effective_analyzer_list_match"
                ),
                "all_enabled_analyzers_mode2_ready": enriched.get("all_enabled_analyzers_mode2_ready"),
                "identity_write_command_sent": enriched.get("identity_write_command_sent"),
                "persistent_write_command_sent": enriched.get("persistent_write_command_sent"),
                "duplicate_device_id_detected": enriched.get("duplicate_device_id_detected"),
                "duplicate_device_id_value": enriched.get("duplicate_device_id_value"),
                "duplicate_device_id_ports": enriched.get("duplicate_device_id_ports"),
                "duplicate_device_id_policy": enriched.get("duplicate_device_id_policy"),
                "duplicate_device_id_status": enriched.get("duplicate_device_id_status"),
                "v1_fallback_status": enriched.get("v1_fallback_status"),
                "artifact_status": enriched.get("artifact_status"),
                "real_machine_acceptance_evidence": False,
            },
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    trace_path.write_text(
        json.dumps(
            {
                "run_id": enriched.get("run_id"),
                "route_id": enriched.get("route_id"),
                "route_name": enriched.get("route_name"),
                "pressure_points": enriched.get("pressure_points"),
                "sample_plan": enriched.get("sample_plan"),
                "actual_route_steps": enriched.get("actual_route_steps"),
                "actual_pressure_steps": enriched.get("actual_pressure_steps"),
                "wait_gate_summary": enriched.get("wait_gate_summary"),
                "sample_summary": enriched.get("sample_summary"),
            },
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    effective_analyzer_fleet_path.write_text(
        json.dumps(effective_fleet, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    manifest_path.write_text(
        json.dumps(
            {
                "run_id": enriched.get("run_id"),
                "git_commit": enriched.get("git_commit"),
                "branch": enriched.get("branch"),
                "config_path": enriched.get("config_path"),
                "config_hash": enriched.get("config_hash"),
                "mode": enriched.get("mode"),
                "enabled_analyzers": enriched.get("enabled_analyzers"),
                "analyzer_ports": enriched.get("analyzer_ports"),
                "analyzer_device_ids": enriched.get("analyzer_device_ids"),
                "effective_analyzer_fleet_artifact": artifact_paths["effective_analyzer_fleet"],
                "effective_analyzer_mapping_status": enriched.get("effective_analyzer_mapping_status"),
                "intended_effective_analyzer_list_match": enriched.get(
                    "intended_effective_analyzer_list_match"
                ),
                "all_enabled_analyzers_mode2_ready": enriched.get("all_enabled_analyzers_mode2_ready"),
                "duplicate_device_id_detected": enriched.get("duplicate_device_id_detected"),
                "duplicate_device_id_value": enriched.get("duplicate_device_id_value"),
                "duplicate_device_id_ports": enriched.get("duplicate_device_id_ports"),
                "duplicate_device_id_policy": enriched.get("duplicate_device_id_policy"),
                "duplicate_device_id_status": enriched.get("duplicate_device_id_status"),
                "identity_write_command_sent": enriched.get("identity_write_command_sent"),
                "persistent_write_command_sent": enriched.get("persistent_write_command_sent"),
                "temperature_chamber_settle_required": enriched.get("temperature_chamber_settle_required"),
                "temperature_chamber_wait_policy": enriched.get("temperature_chamber_wait_policy"),
                "temperature_chamber_keep_running_intentional": enriched.get(
                    "temperature_chamber_keep_running_intentional"
                ),
                "temperature_chamber_not_closed_is_expected_for_this_run": enriched.get(
                    "temperature_chamber_not_closed_is_expected_for_this_run"
                ),
                "temperature_chamber_not_a_failure_reason": enriched.get("temperature_chamber_not_a_failure_reason"),
                "temperature_wait_for_target_before_continue": enriched.get(
                    "temperature_wait_for_target_before_continue"
                ),
                "pressure_gate_policy": enriched.get("pressure_gate_policy"),
                "readiness_result": enriched.get("readiness_result"),
                "a1_final_decision": enriched.get("a1_final_decision"),
                "a1_execution_result": enriched.get("a1_execution_result"),
                "a1_fail_reason": enriched.get("a1_fail_reason"),
                "artifact_paths": enriched.get("artifact_paths"),
                "not_real_acceptance_evidence": True,
                "real_machine_acceptance_evidence": False,
            },
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    report_path.write_text(render_human_report(enriched), encoding="utf-8")
    return artifact_paths


def export_runtime_run001_a1_artifacts(host: Any, run_dir: str | Path) -> dict[str, str]:
    service = getattr(host, "service", None)
    raw_cfg = getattr(service, "_raw_cfg", None)
    if not isinstance(raw_cfg, Mapping) or not is_run001_real_machine_dry_run(raw_cfg):
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
    payload = build_run001_a1_evidence_payload(
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
    )
    return write_run001_a1_artifacts(run_dir, payload)
