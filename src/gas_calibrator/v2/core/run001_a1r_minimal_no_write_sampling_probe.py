from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import time
from typing import Any, Callable, Mapping, Optional

from gas_calibrator.devices.gas_analyzer import GasAnalyzer
from gas_calibrator.v2.core.run001_r0_1_reference_read_probe import (
    _default_chamber_client_factory,
    read_temperature_chamber_read_only,
)
from gas_calibrator.v2.core.run001_r1_conditioning_only_probe import (
    _apply_relay_states,
    _as_bool,
    _as_float,
    _close_device,
    _default_analyzer_serial_factory,
    _default_pace_factory,
    _default_pressure_gauge_factory,
    _default_relay_factory,
    _default_thermometer_factory,
    _gas_analyzer_devices,
    _json_dump,
    _jsonl_dump,
    _open_device,
    _pace_device,
    _pressure_device,
    _read_pace_status,
    _read_pressure_once,
    _read_thermometer,
    _relay_devices,
    _resolve_physical_states,
    _section,
    _send_vent_on,
    _target_open_valves,
    load_json_mapping,
)


A1R_SCHEMA_VERSION = "v2.run001.a1r_minimal_no_write_sampling_probe.1"
A1R_ENV_VAR = "GAS_CAL_V2_A1R_MINIMAL_NO_WRITE_REAL_COM"
A1R_ENV_VALUE = "1"
A1R_CLI_FLAG = "--allow-v2-a1r-minimal-no-write-real-com"
A1R_EVIDENCE_MARKERS = {
    "evidence_source": "real_probe_a1r_minimal_no_write_sampling",
    "acceptance_level": "engineering_probe_only",
    "not_real_acceptance_evidence": True,
    "promotion_state": "blocked",
    "real_primary_latest_refresh": False,
}
A1R_REQUIRED_OPERATOR_FIELDS = (
    "operator_name",
    "timestamp",
    "branch",
    "HEAD",
    "config_path",
    "r0_1_reference_readonly_output_dir",
    "r0_full_query_only_output_dir",
    "r1_conditioning_only_output_dir",
    "port_manifest",
    "explicit_acknowledgement",
)
A1R_REQUIRED_TRUE_ACKS = (
    "only_a1r_minimal_no_write_sampling",
    "co2_only",
    "skip0",
    "single_route",
    "single_temperature",
    "one_nonzero_point",
    "no_write",
    "no_id_write",
    "no_senco_write",
    "no_calibration_write",
    "no_chamber_sv_write",
    "no_chamber_set_temperature",
    "no_chamber_start",
    "no_chamber_stop",
    "no_mode_switch",
    "no_pressure_setpoint",
    "v1_fallback_required",
    "not_real_acceptance",
    "engineering_probe_only",
    "do_not_refresh_real_primary_latest",
)
A1R_REQUIRED_FALSE_ACKS = ("real_primary_latest_refresh",)
A1R_SAFETY_ASSERTION_DEFAULTS = {
    "attempted_write_count": 0,
    "any_write_command_sent": False,
    "identity_write_command_sent": False,
    "mode_switch_command_sent": False,
    "senco_write_command_sent": False,
    "calibration_write_command_sent": False,
    "chamber_write_register_command_sent": False,
    "chamber_set_temperature_command_sent": False,
    "chamber_start_command_sent": False,
    "chamber_stop_command_sent": False,
    "pressure_setpoint_command_sent": False,
    "vent_off_command_sent": False,
    "seal_command_sent": False,
    "high_pressure_command_sent": False,
    "real_primary_latest_refresh": False,
}


@dataclass(frozen=True)
class A1RAdmission:
    approved: bool
    reasons: tuple[str, ...]
    evidence: dict[str, Any]
    operator_confirmation: dict[str, Any]
    operator_validation: dict[str, Any]
    prereq_summaries: dict[str, dict[str, Any]]


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def _path_value(raw_cfg: Mapping[str, Any], dotted_path: str) -> Any:
    current: Any = raw_cfg
    for part in dotted_path.split("."):
        if not isinstance(current, Mapping) or part not in current:
            return None
        current = current.get(part)
    return current


def _first_value(raw_cfg: Mapping[str, Any], paths: tuple[str, ...]) -> Any:
    for path in paths:
        value = _path_value(raw_cfg, path)
        if value is not None:
            return value
    return None


def _truthy(raw_cfg: Mapping[str, Any], paths: tuple[str, ...]) -> bool:
    return _as_bool(_first_value(raw_cfg, paths)) is True


def _explicit_false(raw_cfg: Mapping[str, Any], paths: tuple[str, ...]) -> bool:
    return _as_bool(_first_value(raw_cfg, paths)) is False


def _a1r_cfg(raw_cfg: Mapping[str, Any]) -> dict[str, Any]:
    for name in ("a1r_minimal_no_write_sampling", "run001_a1r", "a1r"):
        cfg = _section(raw_cfg, name)
        if cfg:
            return cfg
    return {}


def _scope(raw_cfg: Mapping[str, Any]) -> str:
    return str(
        _first_value(
            raw_cfg,
            (
                "scope",
                "a1r_minimal_no_write_sampling.scope",
                "run001_a1r.scope",
                "a1r.scope",
            ),
        )
        or ""
    ).strip().lower()


def _skip0_only(raw_cfg: Mapping[str, Any]) -> bool:
    if _truthy(raw_cfg, ("skip0", "a1r_minimal_no_write_sampling.skip0", "run001_a1r.skip0", "a1r.skip0")):
        return True
    value = _first_value(
        raw_cfg,
        (
            "skip_co2_ppm",
            "workflow.skip_co2_ppm",
            "a1r_minimal_no_write_sampling.skip_co2_ppm",
            "run001_a1r.skip_co2_ppm",
            "a1r.skip_co2_ppm",
        ),
    )
    if isinstance(value, list):
        return [int(float(item)) for item in value if str(item).strip() != ""] == [0]
    return str(value).strip() in {"0", "0.0", "[0]"}


def _single_temperature(raw_cfg: Mapping[str, Any]) -> bool:
    if _truthy(
        raw_cfg,
        (
            "single_temperature",
            "single_temperature_group",
            "a1r_minimal_no_write_sampling.single_temperature",
            "run001_a1r.single_temperature",
            "a1r.single_temperature",
        ),
    ):
        return True
    value = _first_value(raw_cfg, ("selected_temps_c", "workflow.selected_temps_c"))
    return isinstance(value, list) and len(value) == 1


def _one_nonzero_point(raw_cfg: Mapping[str, Any]) -> bool:
    if _truthy(
        raw_cfg,
        (
            "one_nonzero_point",
            "a1r_minimal_no_write_sampling.one_nonzero_point",
            "run001_a1r.one_nonzero_point",
            "a1r.one_nonzero_point",
        ),
    ):
        return True
    cfg = _a1r_cfg(raw_cfg)
    point = cfg.get("point") if isinstance(cfg.get("point"), Mapping) else {}
    target = _as_float(point.get("target_co2_ppm", point.get("co2_ppm"))) if isinstance(point, Mapping) else None
    return target is not None and target > 0


def _output_dir_value(raw_cfg: Mapping[str, Any], name: str) -> str:
    return str(
        _first_value(
            raw_cfg,
            (
                name,
                f"a1r_minimal_no_write_sampling.{name}",
                f"run001_a1r.{name}",
                f"a1r.{name}",
            ),
        )
        or ""
    )


def _a1r_runtime(raw_cfg: Mapping[str, Any]) -> dict[str, Any]:
    cfg = _a1r_cfg(raw_cfg)
    point = cfg.get("point") if isinstance(cfg.get("point"), Mapping) else {}
    runtime = _section(cfg, "sampling") or cfg
    return {
        "target_co2_ppm": float(
            runtime.get("target_co2_ppm", point.get("target_co2_ppm", point.get("co2_ppm", 1000.0)))
        ),
        "pressure_overlimit_hpa": float(runtime.get("pressure_overlimit_hpa", 1150.0)),
        "pressure_cache_max_age_ms": float(runtime.get("pressure_cache_max_age_ms", 8000.0)),
        "heartbeat_max_age_ms_before_sample": float(runtime.get("heartbeat_max_age_ms_before_sample", 3000.0)),
        "sample_timeout_s": float(runtime.get("sample_timeout_s", 1.2)),
    }


def _load_prereq_summary(output_dir: str | Path, *, expected_source: str = "") -> tuple[dict[str, Any], list[str]]:
    if not str(output_dir or "").strip():
        return {}, ["missing_prereq_output_dir"]
    summary_path = Path(output_dir).expanduser() / "summary.json"
    if not summary_path.exists():
        return {}, [f"missing_prereq_summary:{summary_path}"]
    try:
        summary = load_json_mapping(summary_path)
    except Exception as exc:
        return {}, [f"invalid_prereq_summary:{exc}"]
    reasons: list[str] = []
    if summary.get("final_decision") != "PASS":
        reasons.append("prereq_final_decision_not_pass")
    if summary.get("not_real_acceptance_evidence") is not True:
        reasons.append("prereq_missing_not_real_acceptance_marker")
    if expected_source and summary.get("evidence_source") != expected_source:
        reasons.append("prereq_evidence_source_mismatch")
    if summary.get("attempted_write_count") != 0:
        reasons.append("prereq_attempted_write_nonzero")
    if summary.get("any_write_command_sent") is not False:
        reasons.append("prereq_any_write_not_false")
    return summary, reasons


def _validate_operator_confirmation(
    path: Optional[str | Path],
    *,
    expected_branch: str = "",
    expected_head: str = "",
    expected_config_path: str = "",
    expected_r0_1_output_dir: str = "",
    expected_r0_output_dir: str = "",
    expected_r1_output_dir: str = "",
) -> tuple[dict[str, Any], dict[str, Any]]:
    errors: list[str] = []
    if not path:
        return {}, {"valid": False, "errors": ["missing_operator_confirmation_json"]}
    confirmation_path = Path(path).expanduser()
    if not confirmation_path.exists():
        return {}, {"valid": False, "errors": ["missing_operator_confirmation_json"]}
    try:
        payload = load_json_mapping(confirmation_path)
    except Exception as exc:
        return {}, {"valid": False, "errors": [f"invalid_operator_confirmation_json:{exc}"]}
    for field in A1R_REQUIRED_OPERATOR_FIELDS:
        if payload.get(field) in (None, ""):
            errors.append(f"operator_confirmation_missing_{field}")
    ack = payload.get("explicit_acknowledgement")
    if not isinstance(ack, Mapping):
        errors.append("operator_confirmation_missing_explicit_acknowledgement")
        ack = {}
    for key in A1R_REQUIRED_TRUE_ACKS:
        if _as_bool(ack.get(key)) is not True:
            errors.append(f"operator_ack_missing_{key}")
    for key in A1R_REQUIRED_FALSE_ACKS:
        if _as_bool(ack.get(key)) is not False:
            errors.append(f"operator_ack_not_false_{key}")
    if expected_branch and str(payload.get("branch") or "") != expected_branch:
        errors.append("operator_confirmation_branch_mismatch")
    if expected_head and str(payload.get("HEAD") or "") != expected_head:
        errors.append("operator_confirmation_head_mismatch")
    path_checks = (
        ("config_path", expected_config_path, "operator_confirmation_config_path_mismatch"),
        ("r0_1_reference_readonly_output_dir", expected_r0_1_output_dir, "operator_confirmation_r0_1_output_dir_mismatch"),
        ("r0_full_query_only_output_dir", expected_r0_output_dir, "operator_confirmation_r0_output_dir_mismatch"),
        ("r1_conditioning_only_output_dir", expected_r1_output_dir, "operator_confirmation_r1_output_dir_mismatch"),
    )
    for field, expected, error in path_checks:
        if not expected:
            continue
        actual = str(payload.get(field) or "")
        if not actual or Path(actual).resolve() != Path(expected).resolve():
            errors.append(error)
    return payload, {"valid": not errors, "errors": errors, "path": str(confirmation_path.resolve())}


def evaluate_a1r_minimal_no_write_sampling_gate(
    raw_cfg: Mapping[str, Any],
    *,
    cli_allow: bool = False,
    env: Optional[Mapping[str, str]] = None,
    operator_confirmation_path: Optional[str | Path] = None,
    branch: str = "",
    head: str = "",
    config_path: str = "",
) -> A1RAdmission:
    env_map = os.environ if env is None else env
    reasons: list[str] = []
    if not cli_allow:
        reasons.append("missing_cli_flag_allow_v2_a1r_minimal_no_write_real_com")
    if str(env_map.get(A1R_ENV_VAR, "")).strip() != A1R_ENV_VALUE:
        reasons.append("missing_env_gas_cal_v2_a1r_minimal_no_write_real_com")

    r0_1_dir = _output_dir_value(raw_cfg, "r0_1_reference_readonly_output_dir")
    r0_dir = _output_dir_value(raw_cfg, "r0_full_query_only_output_dir")
    r1_dir = _output_dir_value(raw_cfg, "r1_conditioning_only_output_dir")
    r0_1_summary, r0_1_reasons = _load_prereq_summary(
        r0_1_dir,
        expected_source="real_probe_r0_1_reference_read_only",
    )
    r0_summary, r0_reasons = _load_prereq_summary(
        r0_dir,
        expected_source="real_probe_query_only",
    )
    r1_summary, r1_reasons = _load_prereq_summary(
        r1_dir,
        expected_source="real_probe_r1_conditioning_only",
    )
    reasons.extend(f"r0_1_{item}" for item in r0_1_reasons)
    reasons.extend(f"r0_full_{item}" for item in r0_reasons)
    reasons.extend(f"r1_{item}" for item in r1_reasons)
    if r1_summary and r1_summary.get("r1_conditioning_only_executed") is not True:
        reasons.append("r1_conditioning_only_not_executed")

    operator_payload, operator_validation = _validate_operator_confirmation(
        operator_confirmation_path,
        expected_branch=branch,
        expected_head=head,
        expected_config_path=config_path,
        expected_r0_1_output_dir=r0_1_dir,
        expected_r0_output_dir=r0_dir,
        expected_r1_output_dir=r1_dir,
    )
    reasons.extend(str(item) for item in operator_validation.get("errors", []))

    if _scope(raw_cfg) not in {"a1r_minimal_no_write_sampling", "a1r"}:
        reasons.append("config_scope_not_a1r_minimal_no_write_sampling")
    if not _truthy(raw_cfg, ("co2_only", "a1r_minimal_no_write_sampling.co2_only", "run001_a1r.co2_only", "a1r.co2_only")):
        reasons.append("config_not_co2_only")
    if not _skip0_only(raw_cfg):
        reasons.append("config_not_skip0")
    if not _truthy(raw_cfg, ("single_route", "a1r_minimal_no_write_sampling.single_route", "run001_a1r.single_route", "a1r.single_route")):
        reasons.append("config_not_single_route")
    if not _single_temperature(raw_cfg):
        reasons.append("config_not_single_temperature")
    if not _one_nonzero_point(raw_cfg):
        reasons.append("config_not_one_nonzero_point")
    if not _truthy(raw_cfg, ("no_write", "a1r_minimal_no_write_sampling.no_write", "run001_a1r.no_write", "a1r.no_write")):
        reasons.append("config_no_write_not_true")

    false_required = {
        "a2_enabled": ("a2_enabled", "a1r_minimal_no_write_sampling.a2_enabled", "run001_a1r.a2_enabled"),
        "a3_enabled": ("a3_enabled", "a1r_minimal_no_write_sampling.a3_enabled", "run001_a1r.a3_enabled"),
        "h2o_enabled": ("h2o_enabled", "a1r_minimal_no_write_sampling.h2o_enabled", "run001_a1r.h2o_enabled"),
        "full_group_enabled": ("full_group_enabled", "a1r_minimal_no_write_sampling.full_group_enabled", "run001_a1r.full_group_enabled"),
        "pressure_setpoint_enabled": (
            "pressure_setpoint_enabled",
            "a1r_minimal_no_write_sampling.pressure_setpoint_enabled",
            "run001_a1r.pressure_setpoint_enabled",
        ),
        "mode_switch_enabled": ("mode_switch_enabled", "a1r_minimal_no_write_sampling.mode_switch_enabled", "run001_a1r.mode_switch_enabled"),
        "analyzer_id_write_enabled": (
            "analyzer_id_write_enabled",
            "a1r_minimal_no_write_sampling.analyzer_id_write_enabled",
            "run001_a1r.analyzer_id_write_enabled",
        ),
        "senco_write_enabled": ("senco_write_enabled", "a1r_minimal_no_write_sampling.senco_write_enabled", "run001_a1r.senco_write_enabled"),
        "calibration_write_enabled": (
            "calibration_write_enabled",
            "a1r_minimal_no_write_sampling.calibration_write_enabled",
            "run001_a1r.calibration_write_enabled",
        ),
        "chamber_set_temperature_enabled": (
            "chamber_set_temperature_enabled",
            "a1r_minimal_no_write_sampling.chamber_set_temperature_enabled",
            "run001_a1r.chamber_set_temperature_enabled",
        ),
        "chamber_start_enabled": ("chamber_start_enabled", "a1r_minimal_no_write_sampling.chamber_start_enabled", "run001_a1r.chamber_start_enabled"),
        "chamber_stop_enabled": ("chamber_stop_enabled", "a1r_minimal_no_write_sampling.chamber_stop_enabled", "run001_a1r.chamber_stop_enabled"),
        "real_primary_latest_refresh": (
            "real_primary_latest_refresh",
            "a1r_minimal_no_write_sampling.real_primary_latest_refresh",
            "run001_a1r.real_primary_latest_refresh",
        ),
    }
    for name, paths in false_required.items():
        if not _explicit_false(raw_cfg, paths):
            reasons.append(f"config_{name}_not_disabled")

    reasons = list(dict.fromkeys(reasons))
    approved = not reasons
    evidence = {
        **A1R_EVIDENCE_MARKERS,
        "admission_approved": approved,
        "r0_1_reference_readonly_prereq_pass": bool(r0_1_summary.get("final_decision") == "PASS" and not r0_1_reasons),
        "r0_full_query_only_prereq_pass": bool(r0_summary.get("final_decision") == "PASS" and not r0_reasons),
        "r1_conditioning_only_prereq_pass": bool(r1_summary.get("final_decision") == "PASS" and not r1_reasons),
        "r1_output_dir": r1_dir,
        "a1r_minimal_sampling_executed": False,
        "a2_allowed": False,
        "a3_allowed": False,
        **A1R_SAFETY_ASSERTION_DEFAULTS,
        "rejection_reasons": reasons,
    }
    return A1RAdmission(
        approved=approved,
        reasons=tuple(reasons),
        evidence=evidence,
        operator_confirmation=operator_payload,
        operator_validation=operator_validation,
        prereq_summaries={"r0_1": r0_1_summary, "r0_full": r0_summary, "r1": r1_summary},
    )


def _default_output_dir() -> Path:
    timestamp = datetime.now().astimezone().strftime("%Y%m%d_%H%M")
    return Path(f"D:/gas_calibrator_step3a_a1r_minimal_no_write_sampling_probe_{timestamp}").resolve()


def _sample_analyzers_readonly(
    raw_cfg: Mapping[str, Any],
    *,
    rows: list[dict[str, Any]],
    device_rows: list[dict[str, Any]],
    trace_rows: list[dict[str, Any]],
    serial_factory: Callable[[Mapping[str, Any]], Any],
    target_co2_ppm: float,
) -> int:
    sample_count = 0
    for index, device in enumerate(_gas_analyzer_devices(raw_cfg), start=1):
        handle = None
        raw_text = ""
        parsed: Any = {}
        result = "no_frame_seen"
        error = ""
        try:
            handle = serial_factory(device)
            raw = handle.readline()
            if isinstance(raw, bytes):
                raw_text = raw.decode("ascii", errors="replace").strip()
            else:
                raw_text = str(raw or "").strip()
            parser = GasAnalyzer(
                device["port"],
                device["baud"],
                timeout=device["timeout_s"],
                device_id=device.get("device_id") or "000",
            )
            parsed = parser.parse_line(raw_text) if raw_text else {}
            if raw_text:
                result = "frame_seen"
                sample_count += 1
        except Exception as exc:
            result = "unavailable"
            error = str(exc)
        finally:
            if handle is not None:
                try:
                    handle.close()
                except Exception:
                    pass
        row = {
            "timestamp": _now(),
            "sample_index": index,
            "point_index": 1,
            "point_tag": "a1r_minimal_no_write_one_nonzero_point",
            "target_co2_ppm": target_co2_ppm,
            "device_name": device["device_name"],
            "port": device["port"],
            "result": result,
            "raw_preview": raw_text[:240],
            "parsed": parsed if isinstance(parsed, Mapping) else {},
            "error": error,
            "read_only": True,
            "mode_switch_command_sent": False,
            "identity_write_command_sent": False,
            "senco_write_command_sent": False,
            "calibration_write_command_sent": False,
        }
        rows.append(row)
        device_rows.append(
            {
                "timestamp": row["timestamp"],
                "device_name": device["device_name"],
                "device_type": "gas_analyzer",
                "port": device["port"],
                "action": "a1r_readonly_sampling_readline",
                "result": result,
                "data": row,
                "read_only": True,
            }
        )
        trace_rows.append({"timestamp": row["timestamp"], "event": "analyzer_readonly_sample", **row})
    return sample_count


def _last_timing_event_ns(rows: list[Mapping[str, Any]], event_name: str) -> Optional[int]:
    for row in reversed(rows):
        if row.get("event_name") != event_name:
            continue
        try:
            return int(row.get("perf_counter_ns"))
        except Exception:
            return None
    return None


def write_a1r_minimal_no_write_sampling_probe_artifacts(
    raw_cfg: Mapping[str, Any],
    *,
    output_dir: Optional[str | Path] = None,
    config_path: str | Path = "",
    operator_confirmation_path: Optional[str | Path] = None,
    branch: str = "",
    head: str = "",
    cli_allow: bool = False,
    env: Optional[Mapping[str, str]] = None,
    execute_sampling: bool = False,
    pressure_gauge_factory: Optional[Callable[[Mapping[str, Any]], Any]] = None,
    pace_factory: Optional[Callable[[Mapping[str, Any]], Any]] = None,
    relay_factory: Optional[Callable[[Mapping[str, Any]], Any]] = None,
    thermometer_factory: Optional[Callable[[Mapping[str, Any]], Any]] = None,
    analyzer_serial_factory: Optional[Callable[[Mapping[str, Any]], Any]] = None,
    chamber_client_factory: Optional[Callable[[Mapping[str, Any]], Any]] = None,
) -> dict[str, Any]:
    admission = evaluate_a1r_minimal_no_write_sampling_gate(
        raw_cfg,
        cli_allow=cli_allow,
        env=env,
        operator_confirmation_path=operator_confirmation_path,
        branch=branch,
        head=head,
        config_path=str(config_path or ""),
    )
    run_dir = Path(output_dir).expanduser().resolve() if output_dir else _default_output_dir()
    run_dir.mkdir(parents=True, exist_ok=True)
    runtime = _a1r_runtime(raw_cfg)
    artifact_paths = {
        "summary": str(run_dir / "summary.json"),
        "a1r_sampling_trace": str(run_dir / "a1r_sampling_trace.jsonl"),
        "route_trace": str(run_dir / "route_trace.jsonl"),
        "device_readings": str(run_dir / "device_readings.jsonl"),
        "pressure_freshness_trace": str(run_dir / "pressure_freshness_trace.jsonl"),
        "analyzer_sampling_rows": str(run_dir / "analyzer_sampling_rows.jsonl"),
        "point_result": str(run_dir / "point_result.json"),
        "safety_assertions": str(run_dir / "safety_assertions.json"),
        "operator_confirmation_record": str(run_dir / "operator_confirmation_record.json"),
    }

    trace_rows: list[dict[str, Any]] = []
    route_rows: list[dict[str, Any]] = []
    device_rows: list[dict[str, Any]] = []
    pressure_rows: list[dict[str, Any]] = []
    sample_rows: list[dict[str, Any]] = []
    timing_rows: list[dict[str, Any]] = []
    rejection_reasons = list(admission.reasons)
    executed = bool(admission.approved and execute_sampling)
    final_decision = "FAIL_CLOSED"
    pressure_latest_hpa: Optional[float] = None
    pressure_age_ms_before_sample: Optional[float] = None
    pressure_gauge_freshness_ok_before_sample = False
    pressure_overlimit_seen = False
    route_conditioning_ready_before_sample = False
    heartbeat_ready_before_sample = False
    sample_count = 0
    points_completed = 0
    conditioning_started = False
    opened_ports: set[str] = set()
    relay_route_action_count = 0
    relay_output_command_sent = False
    pace_status: dict[str, Any] = {}
    chamber_diag: dict[str, Any] = {}
    thermometer_temp_c: Optional[float] = None
    relays: dict[str, Any] = {}
    route_opened = False
    gauge: Any = None
    pace: Any = None
    route_states = _resolve_physical_states(raw_cfg, _target_open_valves(raw_cfg))

    if not executed:
        if not execute_sampling:
            rejection_reasons.append("execute_sampling_not_requested")
        trace_rows.append({"timestamp": _now(), "event": "a1r_sampling_not_executed", "result": "fail_closed"})
    else:
        try:
            trace_rows.append({"timestamp": _now(), "event": "a1r_sampling_start", "result": "started"})
            chamber_diag, chamber_trace = read_temperature_chamber_read_only(
                raw_cfg,
                client_factory=chamber_client_factory or _default_chamber_client_factory,
            )
            for row in chamber_trace:
                device_rows.append({**dict(row), "read_only": True})
                if row.get("result") == "ok" and row.get("port"):
                    opened_ports.add(str(row.get("port")))

            thermometer_temp_c = _read_thermometer(raw_cfg, device_rows, thermometer_factory or _default_thermometer_factory)

            pace = (pace_factory or _default_pace_factory)(_pace_device(raw_cfg))
            _open_device(pace)
            opened_ports.add(_pace_device(raw_cfg)["port"])
            pace_status = _read_pace_status(pace, device_rows)

            gauge = (pressure_gauge_factory or _default_pressure_gauge_factory)(_pressure_device(raw_cfg))
            _open_device(gauge)
            opened_ports.add(_pressure_device(raw_cfg)["port"])
            pressure_timeout_s = float(_pressure_device(raw_cfg).get("response_timeout_s") or 2.2)
            pressure_start = time.monotonic()
            pressure_latest_hpa, pressure_error = _read_pressure_once(gauge, timeout_s=pressure_timeout_s)
            pressure_end = time.monotonic()
            pressure_overlimit_seen = bool(
                pressure_latest_hpa is not None and pressure_latest_hpa > runtime["pressure_overlimit_hpa"]
            )
            pressure_rows.append(
                {
                    "timestamp": _now(),
                    "stage": "pre_route",
                    "pressure_hpa": pressure_latest_hpa,
                    "read_latency_ms": round((pressure_end - pressure_start) * 1000.0, 3),
                    "freshness_age_ms": 0.0 if pressure_latest_hpa is not None else round((pressure_end - pressure_start) * 1000.0, 3),
                    "freshness_ok": pressure_latest_hpa is not None,
                    "pressure_overlimit_seen": pressure_overlimit_seen,
                    "error": pressure_error,
                }
            )
            if pressure_overlimit_seen or pressure_latest_hpa is None:
                rejection_reasons.append("pre_route_pressure_not_safe_or_fresh")
            elif not _send_vent_on(pace, vent_rows=[], trace_rows=trace_rows, timing_rows=timing_rows, phase="before_route_open"):
                rejection_reasons.append("pre_route_vent_heartbeat_failed")
            else:
                for name, device in _relay_devices(raw_cfg).items():
                    relay = (relay_factory or _default_relay_factory)(device)
                    _open_device(relay)
                    opened_ports.add(str(device.get("port") or ""))
                    relays[name] = relay
                count, route_ok = _apply_relay_states(
                    relays,
                    route_states,
                    route_rows=route_rows,
                    trace_rows=trace_rows,
                    timing_rows=timing_rows,
                    action="set_co2_route_conditioning",
                )
                relay_route_action_count += count
                relay_output_command_sent = relay_output_command_sent or count > 0
                route_opened = bool(route_ok and route_states)
                route_conditioning_ready_before_sample = bool(route_opened)
                conditioning_started = bool(route_opened)
                if not route_ok:
                    rejection_reasons.append("route_open_failed")
                else:
                    pressure_start = time.monotonic()
                    pressure_latest_hpa, pressure_error = _read_pressure_once(gauge, timeout_s=pressure_timeout_s)
                    pressure_end = time.monotonic()
                    pressure_overlimit_seen = pressure_overlimit_seen or bool(
                        pressure_latest_hpa is not None and pressure_latest_hpa > runtime["pressure_overlimit_hpa"]
                    )
                    if not _send_vent_on(
                        pace,
                        vent_rows=[],
                        trace_rows=trace_rows,
                        timing_rows=timing_rows,
                        phase="before_sample",
                    ):
                        rejection_reasons.append("before_sample_vent_heartbeat_failed")
                    pressure_age_ms_before_sample = round(max(0.0, time.monotonic() - pressure_end) * 1000.0, 3)
                    pressure_gauge_freshness_ok_before_sample = bool(
                        pressure_latest_hpa is not None
                        and pressure_age_ms_before_sample <= runtime["pressure_cache_max_age_ms"]
                    )
                    heartbeat_start_ns = _last_timing_event_ns(timing_rows, "each_vent_heartbeat_emit_start")
                    heartbeat_age_ms = None
                    if heartbeat_start_ns is not None:
                        heartbeat_age_ms = round((time.perf_counter_ns() - int(heartbeat_start_ns)) / 1_000_000.0, 3)
                    heartbeat_ready_before_sample = bool(
                        heartbeat_age_ms is not None and heartbeat_age_ms <= runtime["heartbeat_max_age_ms_before_sample"]
                    )
                    pressure_rows.append(
                        {
                            "timestamp": _now(),
                            "stage": "before_sample",
                            "pressure_hpa": pressure_latest_hpa,
                            "read_latency_ms": round((pressure_end - pressure_start) * 1000.0, 3),
                            "freshness_age_ms": pressure_age_ms_before_sample,
                            "freshness_ok": pressure_gauge_freshness_ok_before_sample,
                            "pressure_overlimit_seen": pressure_overlimit_seen,
                            "error": pressure_error,
                        }
                    )
                    if pressure_overlimit_seen:
                        rejection_reasons.append("pressure_overlimit_seen_before_sample")
                    if not pressure_gauge_freshness_ok_before_sample:
                        rejection_reasons.append("pressure_gauge_freshness_not_ok_before_sample")
                    if not heartbeat_ready_before_sample:
                        rejection_reasons.append("heartbeat_not_ready_before_sample")
                    if not rejection_reasons:
                        sample_count = _sample_analyzers_readonly(
                            raw_cfg,
                            rows=sample_rows,
                            device_rows=device_rows,
                            trace_rows=trace_rows,
                            serial_factory=analyzer_serial_factory or _default_analyzer_serial_factory,
                            target_co2_ppm=runtime["target_co2_ppm"],
                        )
                        points_completed = 1 if sample_count > 0 else 0
                        if sample_count <= 0:
                            rejection_reasons.append("sample_count_zero")
                        if points_completed <= 0:
                            rejection_reasons.append("points_completed_zero")
        except Exception as exc:
            rejection_reasons.append(f"execution_error:{exc}")
        finally:
            if route_opened and relays:
                close_states = {key: False for key in route_states}
                count, cleanup_ok = _apply_relay_states(
                    relays,
                    close_states,
                    route_rows=route_rows,
                    trace_rows=trace_rows,
                    timing_rows=timing_rows,
                    action="cleanup_a1r_route_conditioning",
                )
                relay_route_action_count += count
                relay_output_command_sent = relay_output_command_sent or count > 0
                if not cleanup_ok:
                    rejection_reasons.append("route_cleanup_failed")
            for relay in relays.values():
                try:
                    _close_device(relay)
                except Exception:
                    pass
            for device in (gauge, pace):
                if device is not None:
                    try:
                        _close_device(device)
                    except Exception:
                        pass
            trace_rows.append({"timestamp": _now(), "event": "a1r_sampling_end", "result": "pending"})

    for row in device_rows:
        port = row.get("port")
        if port and row.get("result") not in {"unavailable", "error"}:
            opened_ports.add(str(port))

    rejection_reasons = list(dict.fromkeys(rejection_reasons))
    final_decision = "PASS" if executed and not rejection_reasons and sample_count > 0 and points_completed > 0 else "FAIL_CLOSED"
    safety_assertions = {
        **A1R_EVIDENCE_MARKERS,
        **A1R_SAFETY_ASSERTION_DEFAULTS,
        "sample_started": bool(sample_count > 0),
        "sample_count": int(sample_count),
        "points_completed": int(points_completed),
        "authorized_r1_route_control_command_count": int(relay_route_action_count),
        "relay_output_command_sent": bool(relay_output_command_sent),
        "relay_output_command_scope": "authorized_r1_route_conditioning_only" if relay_output_command_sent else "",
        "non_authorized_relay_output_command_sent": False,
        "valve_command_sent": bool(relay_output_command_sent),
        "valve_command_scope": "authorized_r1_route_conditioning_only" if relay_output_command_sent else "",
    }
    point_result = {
        "schema_version": A1R_SCHEMA_VERSION,
        "point_index": 1,
        "target_co2_ppm": runtime["target_co2_ppm"],
        "co2_only": True,
        "skip0": True,
        "single_route": True,
        "single_temperature": True,
        "one_nonzero_point": True,
        "sample_count": int(sample_count),
        "points_completed": int(points_completed),
        "pressure_hpa_before_sample": pressure_latest_hpa,
        "pressure_age_ms_before_sample": pressure_age_ms_before_sample,
        "final_decision": final_decision,
    }
    summary = {
        "schema_version": A1R_SCHEMA_VERSION,
        **A1R_EVIDENCE_MARKERS,
        "final_decision": final_decision,
        "admission_approved": admission.approved,
        "operator_confirmation_valid": bool(admission.operator_validation.get("valid")),
        "rejection_reasons": rejection_reasons,
        "r0_1_reference_readonly_prereq_pass": bool(admission.evidence["r0_1_reference_readonly_prereq_pass"]),
        "r0_full_query_only_prereq_pass": bool(admission.evidence["r0_full_query_only_prereq_pass"]),
        "r1_conditioning_only_prereq_pass": bool(admission.evidence["r1_conditioning_only_prereq_pass"]),
        "r0_1_reference_readonly_output_dir": _output_dir_value(raw_cfg, "r0_1_reference_readonly_output_dir"),
        "r0_full_query_only_output_dir": _output_dir_value(raw_cfg, "r0_full_query_only_output_dir"),
        "r1_output_dir": _output_dir_value(raw_cfg, "r1_conditioning_only_output_dir"),
        "current_branch": branch,
        "current_head": head,
        "v1_fallback_required": True,
        "v1_untouched": True,
        "run_app_py_untouched": True,
        "a1r_minimal_sampling_executed": bool(executed and conditioning_started),
        "real_probe_executed": bool(executed),
        "real_com_opened": bool(opened_ports),
        "a2_allowed": False,
        "a3_allowed": False,
        "co2_only": True,
        "skip0": True,
        "single_route": True,
        "single_temperature": True,
        "one_nonzero_point": True,
        "pressure_setpoint_command_sent": False,
        "pressure_gauge_latest_hpa": pressure_latest_hpa,
        "pressure_gauge_freshness_ok_before_sample": bool(pressure_gauge_freshness_ok_before_sample),
        "pressure_age_ms_before_sample": pressure_age_ms_before_sample,
        "pressure_overlimit_seen": bool(pressure_overlimit_seen),
        "route_conditioning_ready_before_sample": bool(route_conditioning_ready_before_sample),
        "heartbeat_ready_before_sample": bool(heartbeat_ready_before_sample),
        "sample_count": int(sample_count),
        "points_completed": int(points_completed),
        "temperature_chamber_pv_c": chamber_diag.get("pv_temperature_c", chamber_diag.get("pv_current_temperature_c")),
        "temperature_chamber_sv_c": chamber_diag.get("sv_temperature_c", chamber_diag.get("sv_set_temperature_c")),
        "thermometer_temp_c": thermometer_temp_c,
        "pace_identity_or_status": pace_status,
        "relay_route_action_count": int(relay_route_action_count),
        "relay_output_command_sent": bool(relay_output_command_sent),
        "opened_ports": sorted(opened_ports),
        "artifact_paths": artifact_paths,
        **safety_assertions,
    }
    operator_record = {
        "schema_version": A1R_SCHEMA_VERSION,
        "record_type": "a1r_operator_confirmation_record",
        "operator_confirmation_path": str(Path(operator_confirmation_path).expanduser().resolve()) if operator_confirmation_path else "",
        "validation": admission.operator_validation,
        "payload": admission.operator_confirmation,
        **A1R_EVIDENCE_MARKERS,
    }
    _json_dump(run_dir / "summary.json", summary)
    _jsonl_dump(run_dir / "a1r_sampling_trace.jsonl", trace_rows)
    _jsonl_dump(run_dir / "route_trace.jsonl", route_rows)
    _jsonl_dump(run_dir / "device_readings.jsonl", device_rows)
    _jsonl_dump(run_dir / "pressure_freshness_trace.jsonl", pressure_rows)
    _jsonl_dump(run_dir / "analyzer_sampling_rows.jsonl", sample_rows)
    _json_dump(run_dir / "point_result.json", point_result)
    _json_dump(run_dir / "safety_assertions.json", safety_assertions)
    _json_dump(run_dir / "operator_confirmation_record.json", operator_record)
    return summary
