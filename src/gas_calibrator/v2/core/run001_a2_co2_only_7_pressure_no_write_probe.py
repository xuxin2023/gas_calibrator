from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import csv
import hashlib
import json
import os
from pathlib import Path
import re
from types import SimpleNamespace
from typing import Any, Callable, Mapping, Optional

from gas_calibrator.v2.core.run001_r1_conditioning_only_probe import (
    _as_bool,
    _as_float,
    _json_dump,
    load_json_mapping,
)
from gas_calibrator.v2.core.services.trace_size_guard import (
    load_guarded_jsonl,
    summarize_trace_guard_rows,
    write_guarded_jsonl,
)


A2_SCHEMA_VERSION = "v2.run001.a2_co2_7_pressure_no_write_probe.1"
A2_1_HEARTBEAT_GAP_ACCOUNTING_FIX_PRESENT = True
A2_3_V1_PRESSURE_GAUGE_READ_POLICY_PRESENT = True
A2_4_V1_PRESSURE_GAUGE_READ_POLICY_PRESENT = True
A2_ENV_VAR = "GAS_CAL_V2_A2_CO2_7_PRESSURE_NO_WRITE_REAL_COM"
A2_ENV_VALUE = "1"
A2_CLI_FLAG = "--allow-v2-a2-co2-7-pressure-no-write-real-com"
A2_ALLOWED_PRESSURE_POINTS_HPA = (1100.0, 1000.0, 900.0, 800.0, 700.0, 600.0, 500.0)
A2_CURRENT_EVIDENCE_SOURCE = "real_probe_a2_12r_co2_7_pressure_no_write"
A2_LEGACY_EVIDENCE_SOURCES = [
    "real_probe_a2_12_co2_7_pressure_no_write",
    "real_probe_a2_10_co2_7_pressure_no_write",
]
A2_EVIDENCE_MARKERS = {
    "probe_identity": "A2.12R CO2-only seven-pressure no-write engineering probe",
    "probe_version": "A2.12R",
    "evidence_source": A2_CURRENT_EVIDENCE_SOURCE,
    "legacy_evidence_source": A2_LEGACY_EVIDENCE_SOURCES[0],
    "legacy_evidence_sources": A2_LEGACY_EVIDENCE_SOURCES,
    "acceptance_level": "engineering_probe_only",
    "not_real_acceptance_evidence": True,
    "promotion_state": "blocked",
    "real_primary_latest_refresh": False,
}
A2_REQUIRED_OPERATOR_FIELDS = (
    "operator_name",
    "timestamp",
    "branch",
    "HEAD",
    "config_path",
    "a1r_output_dir",
    "pressure_points_hpa",
    "pressure_source",
    "port_manifest",
    "explicit_acknowledgement",
)
A2_REQUIRED_TRUE_ACKS = (
    "only_a2_co2_7_pressure_no_write",
    "co2_only",
    "skip0",
    "single_route",
    "single_temperature",
    "skip_temperature_stabilization_wait",
    "seven_pressure_points",
    "no_write",
    "no_id_write",
    "no_senco_write",
    "no_calibration_write",
    "no_chamber_sv_write",
    "no_chamber_set_temperature",
    "no_chamber_start",
    "no_chamber_stop",
    "no_mode_switch",
    "not_real_acceptance",
    "engineering_probe_only",
    "v1_fallback_required",
    "authorized_pressure_control_scope_acknowledged",
    "do_not_refresh_real_primary_latest",
)
A2_REQUIRED_FALSE_ACKS = (
    "a3_enabled",
    "h2o_enabled",
    "full_group_enabled",
    "multi_temperature_enabled",
    "real_primary_latest_refresh",
)
A2_SAFETY_ASSERTION_DEFAULTS = {
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
    "final_safe_stop_chamber_stop_blocked_by_no_write": False,
    "real_primary_latest_refresh": False,
    "v1_fallback_required": True,
    "run_app_py_untouched": True,
}
A2_INTERRUPTED_FAIL_CLOSED_REASON = "probe_execution_interrupted_required_artifacts_incomplete"
A2_REQUIRED_ARTIFACT_KEYS = (
    "probe_admission_record",
    "operator_confirmation_record",
    "summary",
    "safety_assertions",
    "process_exit_record",
    "a2_pressure_sweep_trace",
    "route_trace",
    "pressure_trace",
    "pressure_ready_trace",
    "heartbeat_trace",
    "analyzer_sampling_rows",
    "point_results",
    "point_results_csv",
)


@dataclass(frozen=True)
class A2Admission:
    approved: bool
    reasons: tuple[str, ...]
    evidence: dict[str, Any]
    operator_confirmation: dict[str, Any]
    operator_validation: dict[str, Any]
    prereq_summaries: dict[str, dict[str, Any]]


class A2PointsConfigAlignmentError(RuntimeError):
    """Raised when wrapper/downstream A2 points config cannot be aligned safely."""


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def _artifact_completeness(
    artifact_paths: Mapping[str, str],
    *,
    assume_present_keys: Optional[set[str]] = None,
) -> dict[str, Any]:
    assumed = assume_present_keys or set()
    expected: list[str] = []
    present: list[str] = []
    missing: list[str] = []
    present_keys: list[str] = []
    missing_keys: list[str] = []
    for key in A2_REQUIRED_ARTIFACT_KEYS:
        path_text = artifact_paths.get(key, "")
        if not path_text:
            continue
        filename = Path(path_text).name
        expected.append(filename)
        if key in assumed or Path(path_text).exists():
            present.append(filename)
            present_keys.append(key)
        else:
            missing.append(filename)
            missing_keys.append(key)
    return {
        "required_artifacts_expected": expected,
        "required_artifacts_present": present,
        "required_artifacts_missing": missing,
        "required_artifact_keys_present": present_keys,
        "required_artifact_keys_missing": missing_keys,
        "artifact_completeness_pass": not missing,
        "artifact_completeness_fail_reason": "" if not missing else "required_artifacts_missing",
    }


def _build_operator_record(
    admission: A2Admission,
    *,
    operator_confirmation_path: Optional[str | Path],
) -> dict[str, Any]:
    return {
        "schema_version": A2_SCHEMA_VERSION,
        "record_type": "a2_operator_confirmation_record",
        "operator_confirmation_path": (
            str(Path(operator_confirmation_path).expanduser().resolve())
            if operator_confirmation_path
            else ""
        ),
        "validation": admission.operator_validation,
        "payload": admission.operator_confirmation,
        **A2_EVIDENCE_MARKERS,
    }


def _build_probe_admission_record(
    admission: A2Admission,
    *,
    artifact_paths: Mapping[str, str],
    config_path: str | Path,
    branch: str,
    head: str,
    cli_allow: bool,
    execute_probe: bool,
    run_app_py_untouched: bool,
) -> dict[str, Any]:
    return {
        "schema_version": A2_SCHEMA_VERSION,
        "record_type": "a2_probe_admission_record",
        "created_at": _now(),
        "admission_approved": bool(admission.approved),
        "admission_reasons": list(admission.reasons),
        "operator_confirmation_valid": bool(admission.operator_validation.get("valid")),
        "operator_validation": admission.operator_validation,
        "operator_confirmation": admission.operator_confirmation,
        "prereq_summaries": admission.prereq_summaries,
        "evidence": admission.evidence,
        "config_path": str(config_path or ""),
        "current_branch": branch,
        "current_head": head,
        "cli_allow": bool(cli_allow),
        "execute_probe_requested": bool(execute_probe),
        "run_app_py_untouched": bool(run_app_py_untouched),
        "artifact_paths": dict(artifact_paths),
        **A2_EVIDENCE_MARKERS,
    }


def _partial_safety_assertions(
    *,
    real_probe_executed: bool,
    real_com_opened: Any,
    any_device_command_sent: Any,
    any_write_command_sent: Any,
    no_write_assertion_status: str,
    device_command_audit_complete: bool,
    must_not_claim_no_write_pass: bool,
    safe_stop_triggered: Any,
) -> dict[str, Any]:
    no_write_pass_like = str(no_write_assertion_status).startswith("pass")
    safety = {
        **A2_EVIDENCE_MARKERS,
        **A2_SAFETY_ASSERTION_DEFAULTS,
        "record_type": "a2_safety_assertions_partial",
        "safety_assertions_complete": False,
        "real_probe_executed": bool(real_probe_executed),
        "real_com_opened": real_com_opened,
        "any_device_command_sent": any_device_command_sent,
        "any_write_command_sent": any_write_command_sent,
        "no_write": bool(no_write_pass_like and not must_not_claim_no_write_pass),
        "no_write_assertion_status": no_write_assertion_status,
        "device_command_audit_complete": bool(device_command_audit_complete),
        "must_not_claim_no_write_pass": bool(must_not_claim_no_write_pass),
        "safe_stop_triggered": safe_stop_triggered,
    }
    if any_write_command_sent == "unknown":
        safety["attempted_write_count_status"] = "unknown"
    return safety


def _write_process_exit_record(
    run_dir: Path,
    *,
    artifact_paths: Mapping[str, str],
    process_state: str,
    final_decision: str,
    interrupted_execution: bool,
    interruption_source: str,
    interruption_stage: str,
    fail_closed_reason: str,
    execution_error: str,
    real_probe_executed: bool,
    real_com_opened: Any,
    any_device_command_sent: Any,
    any_write_command_sent: Any,
    safe_stop_triggered: Any,
    no_write_assertion_status: str,
) -> None:
    completeness = _artifact_completeness(
        artifact_paths,
        assume_present_keys={"process_exit_record", "summary"},
    )
    payload = {
        "schema_version": A2_SCHEMA_VERSION,
        "record_type": "a2_process_exit_record",
        "updated_at": _now(),
        "process_state": process_state,
        "final_decision": final_decision,
        "fail_closed_reason": fail_closed_reason,
        "interrupted_execution": bool(interrupted_execution),
        "interruption_source": interruption_source,
        "interruption_stage": interruption_stage,
        "execution_error": execution_error,
        "real_probe_executed": bool(real_probe_executed),
        "real_com_opened": real_com_opened,
        "any_device_command_sent": any_device_command_sent,
        "any_write_command_sent": any_write_command_sent,
        "safe_stop_triggered": safe_stop_triggered,
        "no_write_assertion_status": no_write_assertion_status,
        "artifact_paths": dict(artifact_paths),
        **completeness,
        **A2_EVIDENCE_MARKERS,
    }
    _json_dump(run_dir / "process_exit_record.json", payload)


def _write_interrupted_guard_artifacts(
    run_dir: Path,
    *,
    admission: A2Admission,
    artifact_paths: Mapping[str, str],
    operator_confirmation_path: Optional[str | Path],
    config_path: str | Path,
    branch: str,
    head: str,
    cli_allow: bool,
    execute_probe: bool,
    run_app_py_untouched: bool,
    interruption_source: str,
    interruption_stage: str,
    real_probe_executed: bool,
    real_com_opened: Any,
    any_device_command_sent: Any,
    any_write_command_sent: Any,
    no_write_assertion_status: str,
    safe_stop_triggered: Any,
    device_command_audit_complete: bool,
    must_not_claim_no_write_pass: bool,
    execution_error: str = "",
) -> dict[str, Any]:
    operator_record = _build_operator_record(
        admission,
        operator_confirmation_path=operator_confirmation_path,
    )
    admission_record = _build_probe_admission_record(
        admission,
        artifact_paths=artifact_paths,
        config_path=config_path,
        branch=branch,
        head=head,
        cli_allow=cli_allow,
        execute_probe=execute_probe,
        run_app_py_untouched=run_app_py_untouched,
    )
    partial_safety = _partial_safety_assertions(
        real_probe_executed=real_probe_executed,
        real_com_opened=real_com_opened,
        any_device_command_sent=any_device_command_sent,
        any_write_command_sent=any_write_command_sent,
        no_write_assertion_status=no_write_assertion_status,
        device_command_audit_complete=device_command_audit_complete,
        must_not_claim_no_write_pass=must_not_claim_no_write_pass,
        safe_stop_triggered=safe_stop_triggered,
    )
    _json_dump(run_dir / "probe_admission_record.json", admission_record)
    _json_dump(run_dir / "operator_confirmation_record.json", operator_record)
    _json_dump(run_dir / "safety_assertions.json", partial_safety)
    _write_process_exit_record(
        run_dir,
        artifact_paths=artifact_paths,
        process_state="interrupted_guard_active",
        final_decision="FAIL_CLOSED",
        interrupted_execution=True,
        interruption_source=interruption_source,
        interruption_stage=interruption_stage,
        fail_closed_reason=A2_INTERRUPTED_FAIL_CLOSED_REASON,
        execution_error=execution_error,
        real_probe_executed=real_probe_executed,
        real_com_opened=real_com_opened,
        any_device_command_sent=any_device_command_sent,
        any_write_command_sent=any_write_command_sent,
        safe_stop_triggered=safe_stop_triggered,
        no_write_assertion_status=no_write_assertion_status,
    )
    completeness = _artifact_completeness(artifact_paths, assume_present_keys={"summary"})
    summary = {
        "schema_version": A2_SCHEMA_VERSION,
        **A2_EVIDENCE_MARKERS,
        "final_decision": "FAIL_CLOSED",
        "fail_closed_reason": A2_INTERRUPTED_FAIL_CLOSED_REASON,
        "interrupted_execution": True,
        "interrupted_at": _now(),
        "interruption_source": interruption_source,
        "interruption_stage": interruption_stage,
        "rejection_reasons": [A2_INTERRUPTED_FAIL_CLOSED_REASON],
        "admission_approved": bool(admission.approved),
        "operator_confirmation_valid": bool(admission.operator_validation.get("valid")),
        "real_probe_executed": bool(real_probe_executed),
        "real_com_opened": real_com_opened,
        "any_device_command_sent": any_device_command_sent,
        "any_write_command_sent": any_write_command_sent,
        "safe_stop_triggered": safe_stop_triggered,
        "no_write_assertion_status": no_write_assertion_status,
        "safety_assertions_complete": False,
        "device_command_audit_complete": bool(device_command_audit_complete),
        "must_not_claim_no_write_pass": bool(must_not_claim_no_write_pass),
        "a3_allowed": False,
        "real_primary_latest_refresh": False,
        "execution_error": execution_error,
        "artifact_paths": dict(artifact_paths),
        **completeness,
    }
    _json_dump(run_dir / "summary.json", summary)
    return summary


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


def _a2_cfg(raw_cfg: Mapping[str, Any]) -> dict[str, Any]:
    for name in ("a2_co2_7_pressure_no_write_probe", "run001_a2", "a2"):
        candidate = raw_cfg.get(name)
        if isinstance(candidate, Mapping):
            return dict(candidate)
    return {}


def _scope(raw_cfg: Mapping[str, Any]) -> str:
    return str(
        _first_value(
            raw_cfg,
            (
                "scope",
                "a2_co2_7_pressure_no_write_probe.scope",
                "run001_a2.probe_scope",
                "run001_a2.scope",
                "a2.scope",
            ),
        )
        or ""
    ).strip().lower()


def _output_dir_value(raw_cfg: Mapping[str, Any], name: str) -> str:
    return str(
        _first_value(
            raw_cfg,
            (
                name,
                f"a2_co2_7_pressure_no_write_probe.{name}",
                f"run001_a2.{name}",
                f"a2.{name}",
            ),
        )
        or ""
    )


def _float_list(value: Any) -> list[float]:
    raw = value if isinstance(value, list) else [value]
    out: list[float] = []
    for item in raw:
        parsed = _as_float(item)
        if parsed is not None:
            out.append(float(parsed))
    return out


def _same_pressure_points(value: Any) -> bool:
    points = _float_list(value)
    if len(points) != len(A2_ALLOWED_PRESSURE_POINTS_HPA):
        return False
    return all(abs(float(a) - float(b)) <= 1e-6 for a, b in zip(points, A2_ALLOWED_PRESSURE_POINTS_HPA))


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _device_cfg(raw_cfg: Mapping[str, Any], *names: str) -> dict[str, Any]:
    devices = _mapping(raw_cfg.get("devices"))
    for name in names:
        value = devices.get(name)
        if isinstance(value, Mapping):
            return dict(value)
    return {}


def _serial_settings(device: Mapping[str, Any], *, default_baud: int = 9600) -> dict[str, Any]:
    return {
        "baud": int(device.get("baud") or device.get("baudrate") or default_baud),
        "parity": str(device.get("parity") or "N"),
        "stopbits": device.get("stopbits", 1),
        "bytesize": device.get("bytesize", 8),
        "timeout_s": float(device.get("timeout") or device.get("timeout_s") or 1.0),
    }


def _line_ending_label(value: Any) -> str:
    text = str(value or "").strip()
    if text in {"\n", "LF", "lf"}:
        return "LF"
    if text in {"\r\n", "CRLF", "crlf"}:
        return "CRLF"
    if text in {"\r", "CR", "cr"}:
        return "CR"
    return text or "LF"


def _route_action_row(route_rows: list[dict[str, Any]], action: str) -> dict[str, Any]:
    for row in reversed(route_rows):
        if str(row.get("action") or "") == action:
            return dict(row)
    return {}


def _route_action_result(route_rows: list[dict[str, Any]], action: str) -> tuple[str, str]:
    row = _route_action_row(route_rows, action)
    if not row:
        return "not_attempted", ""
    return str(row.get("result") or ""), str(row.get("message") or "")


def _extract_pace_command_error(text: Any) -> str:
    message = str(text or "").strip()
    return message if "PACE_COMMAND_ERROR" in message else ""


def _pressure_controller_command_diagnostics(
    raw_cfg: Mapping[str, Any],
    route_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    cfg = _device_cfg(raw_cfg, "pressure_controller")
    output_result, output_error = _route_action_result(route_rows, "set_output")
    vent_result, vent_error = _route_action_result(route_rows, "set_vent")
    pace_error = _extract_pace_command_error(output_error) or _extract_pace_command_error(vent_error)
    identity_command = str(cfg.get("identity_query_command") or "*IDN?").strip() or "*IDN?"
    identity_result = str(cfg.get("identity_query_result") or "").strip()
    if not identity_result:
        identity_result = "unsupported_identity_query_not_offline_decision"
    protocol_profile = str(cfg.get("protocol_profile") or "pace5000_scpi_v1_aligned").strip()
    profile_aligned = protocol_profile in {"pace5000_scpi_v1_aligned", "pace5000_scpi", "k0472_pace_v1"}
    return {
        "pressure_controller_driver_profile": str(
            cfg.get("driver_profile") or "gas_calibrator.devices.pace5000.Pace5000"
        ),
        "pressure_controller_configured_port": str(cfg.get("port") or ""),
        "pressure_controller_serial_settings": _serial_settings(cfg, default_baud=9600),
        "pressure_controller_protocol_profile": protocol_profile,
        "pressure_controller_command_terminator": _line_ending_label(cfg.get("line_ending")),
        "pressure_controller_identity_query_command": identity_command,
        "pressure_controller_identity_query_result": identity_result,
        "pressure_controller_identity_query_raw_response": str(cfg.get("identity_query_raw_response") or ""),
        "pressure_controller_identity_query_error": str(
            cfg.get("identity_query_error")
            or ("unsupported_identity_query" if identity_command.upper() == "*IDN?" else "")
        ),
        "pressure_controller_output_command": ":OUTP:STAT 0",
        "pressure_controller_output_command_result": output_result,
        "pressure_controller_output_command_error": output_error,
        "pressure_controller_vent_command": ":SOUR:PRES:LEV:IMM:AMPL:VENT 1",
        "pressure_controller_vent_command_result": vent_result,
        "pressure_controller_vent_command_error": vent_error,
        "pressure_controller_pace_command_error_raw": pace_error,
        "pressure_controller_v1_protocol_profile": (
            "Pace5000 LF terminator; read-only profile/status queries; "
            "output=:OUTP:STAT; vent=:SOUR:PRES:LEV:IMM:AMPL:VENT"
        ),
        "v1_v2_pressure_controller_command_alignment": (
            "control_commands_aligned_identity_query_not_online_gate"
            if profile_aligned
            else f"profile_mismatch:{protocol_profile}"
        ),
    }


def _pressure_meter_diagnostics(
    raw_cfg: Mapping[str, Any],
    point_results: list[dict[str, Any]],
) -> dict[str, Any]:
    devices = _mapping(raw_cfg.get("devices"))
    candidates = ["pressure_meter", "pressure_gauge", "digital_pressure_gauge_p3"]
    selected = next((name for name in candidates if isinstance(devices.get(name), Mapping)), "")
    cfg = _device_cfg(raw_cfg, selected) if selected else {}
    first_point = dict(point_results[0]) if point_results else {}
    selected_source = str(
        first_point.get("selected_pressure_source")
        or first_point.get("pressure_source_selected")
        or ""
    )
    read_result = str(first_point.get("pressure_ready_gate_result") or "not_attempted")
    raw_response = str(first_point.get("pressure_meter_raw_response") or first_point.get("raw_response") or "")
    timeout_value = cfg.get("response_timeout_s") or cfg.get("timeout") or cfg.get("timeout_s") or 1.0
    mode = str(
        _first_value(
            raw_cfg,
            (
                "workflow.pressure.digital_gauge_continuous_mode",
                "workflow.pressure.pressure_gauge_continuous_mode",
                "workflow.sampling.pressure_gauge_continuous_mode",
            ),
        )
        or "P4"
    )
    drain_value = _first_value(
        raw_cfg,
        (
            "workflow.pressure.digital_gauge_continuous_drain_s",
            "workflow.pressure.pressure_gauge_continuous_drain_s",
            "workflow.sampling.pressure_gauge_continuous_drain_s",
        ),
    )
    return {
        "pressure_meter_name_mapping": "pressure_meter<-pressure_gauge|digital_pressure_gauge_p3",
        "pressure_meter_alias_resolved": bool(selected),
        "pressure_meter_alias_candidates": candidates,
        "pressure_meter_selected_device_key": selected,
        "pressure_meter_port": str(cfg.get("port") or ""),
        "pressure_meter_dest_id": str(cfg.get("dest_id") or "01"),
        "pressure_meter_protocol_profile": "paroscientific_p3_readonly",
        "pressure_meter_mode": mode,
        "digital_pressure_gauge_p3_available": selected_source in {
            "digital_pressure_gauge_p3",
            "pressure_gauge",
            "pressure_meter",
        }
        and _as_bool(first_point.get("selected_pressure_parse_ok")) is True,
        "pressure_meter_first_read_attempted": bool(point_results),
        "pressure_meter_first_read_result": read_result,
        "pressure_meter_raw_response": raw_response,
        "pressure_meter_parse_ok": _as_bool(first_point.get("selected_pressure_parse_ok")) is True,
        "pressure_meter_continuous_mode_detected": bool(str(mode or "").strip()),
        "pressure_meter_drain_attempted": drain_value is not None,
        "pressure_meter_read_timeout_s": float(timeout_value),
        "v1_v2_pressure_meter_read_alignment": (
            "aligned_paroscientific_p3_with_pressure_gauge_alias"
            if selected in {"pressure_meter", "pressure_gauge", "digital_pressure_gauge_p3"}
            else "missing_pressure_meter_alias"
        ),
    }


def _relay_diagnostics(raw_cfg: Mapping[str, Any], route_rows: list[dict[str, Any]]) -> dict[str, Any]:
    relay_a = _device_cfg(raw_cfg, "relay_a", "relay")
    relay_b = _device_cfg(raw_cfg, "relay_b", "relay_8")
    valves = _mapping(raw_cfg.get("valves"))
    relay_map = _mapping(valves.get("relay_map"))
    relay_output_sent = any(
        str(row.get("action") or "") in {"set_valve", "set_valves_bulk", "open_route", "route_open"}
        for row in route_rows
    )
    return {
        "relay_a_configured_port": str(relay_a.get("port") or ""),
        "relay_b_configured_port": str(relay_b.get("port") or ""),
        "relay_driver_profile": "gas_calibrator.devices.relay.RelayController",
        "relay_channel_mapping": relay_map,
        "relay_init_open_result": "not_observed_by_a2_wrapper",
        "relay_readonly_identity_supported": False,
        "relay_control_available": bool(relay_a or relay_b),
        "relay_output_command_allowed_in_probe": False,
        "relay_output_command_sent": bool(relay_output_sent),
    }


_DEVICE_PRECHECK_ALIASES = {
    "pressure_controller": ("pressure_controller",),
    "pressure_meter": ("pressure_meter", "pressure_gauge", "digital_pressure_gauge_p3"),
    "relay_a": ("relay_a", "relay"),
    "relay_b": ("relay_b", "relay_8"),
    "temperature_chamber": ("temperature_chamber",),
}

_DEVICE_PRECHECK_CRITICAL_DEFAULT = ("pressure_controller", "pressure_meter", "relay_a", "relay_b")
_DEVICE_PRECHECK_LEGACY_EXPECTED_PORTS = {
    "pressure_controller": "COM23",
    "pressure_meter": "COM22",
    "relay_a": "COM20",
    "relay_b": "COM21",
    "temperature_chamber": "COM19",
}
A2_21_PRESSURE_PORT_MAPPING_SOURCE = "query_only_com_sanity_probe"
A2_21_PRESSURE_PORT_MAPPING_VERIFIED_AT = "2026-05-01T03:17:57.940+00:00"
A2_21_VERIFIED_PRESSURE_CONTROLLER_PORT = "COM23"
A2_21_VERIFIED_PRESSURE_METER_PORT = "COM22"
A2_21_REJECTED_PRESSURE_CONTROLLER_PORT = "COM31"
A2_21_REJECTED_PRESSURE_METER_PORT = "COM30"
A2_21_RELAY_A_CANDIDATE_PORTS = ["COM20", "COM28"]
A2_21_RELAY_B_CANDIDATE_PORTS = ["COM21", "COM29"]
A2_21_ADVANTECH_COM_SHIFT_MAPPING = {
    "COM24": "COM16",
    "COM25": "COM17",
    "COM26": "COM18",
    "COM27": "COM19",
    "COM28": "COM20",
    "COM29": "COM21",
    "COM30": "COM22",
    "COM31": "COM23",
}
A2_21_ADVANTECH_MAPPED_PORTS = {
    "humidity_generator": "COM16",
    "dewpoint_meter": "COM17",
    "thermometer": "COM18",
    "temperature_chamber": "COM19",
    "relay_a": "COM20",
    "relay_b": "COM21",
    "pressure_meter": "COM22",
    "pressure_controller": "COM23",
}


def _a2_21_pressure_port_alignment_metadata() -> dict[str, Any]:
    config_ports = {
        "humidity_generator": A2_21_ADVANTECH_MAPPED_PORTS["humidity_generator"],
        "dewpoint_meter": A2_21_ADVANTECH_MAPPED_PORTS["dewpoint_meter"],
        "thermometer": A2_21_ADVANTECH_MAPPED_PORTS["thermometer"],
        "temperature_chamber": A2_21_ADVANTECH_MAPPED_PORTS["temperature_chamber"],
        "relay": A2_21_ADVANTECH_MAPPED_PORTS["relay_a"],
        "relay_a": A2_21_ADVANTECH_MAPPED_PORTS["relay_a"],
        "relay_8": A2_21_ADVANTECH_MAPPED_PORTS["relay_b"],
        "relay_b": A2_21_ADVANTECH_MAPPED_PORTS["relay_b"],
        "pressure_gauge": A2_21_ADVANTECH_MAPPED_PORTS["pressure_meter"],
        "pressure_meter": A2_21_ADVANTECH_MAPPED_PORTS["pressure_meter"],
        "pressure_controller": A2_21_ADVANTECH_MAPPED_PORTS["pressure_controller"],
    }
    return {
        "advantech_com_shift_mapping_applied": True,
        "advantech_com_shift_old_range": "COM24-COM31",
        "advantech_com_shift_new_range": "COM16-COM23",
        "advantech_com_shift_delta": -8,
        "verified_pressure_controller_port": A2_21_VERIFIED_PRESSURE_CONTROLLER_PORT,
        "verified_pressure_meter_port": A2_21_VERIFIED_PRESSURE_METER_PORT,
        "mapped_relay_a_port": A2_21_ADVANTECH_MAPPED_PORTS["relay_a"],
        "mapped_relay_b_port": A2_21_ADVANTECH_MAPPED_PORTS["relay_b"],
        "mapped_temperature_chamber_port": A2_21_ADVANTECH_MAPPED_PORTS["temperature_chamber"],
        "mapped_thermometer_port": A2_21_ADVANTECH_MAPPED_PORTS["thermometer"],
        "mapped_dewpoint_port": A2_21_ADVANTECH_MAPPED_PORTS["dewpoint_meter"],
        "mapped_humidity_generator_port": A2_21_ADVANTECH_MAPPED_PORTS["humidity_generator"],
        "rejected_pressure_controller_candidate_port": A2_21_REJECTED_PRESSURE_CONTROLLER_PORT,
        "rejected_pressure_meter_candidate_port": A2_21_REJECTED_PRESSURE_METER_PORT,
        "rejected_stale_pressure_controller_port": A2_21_REJECTED_PRESSURE_CONTROLLER_PORT,
        "rejected_stale_pressure_meter_port": A2_21_REJECTED_PRESSURE_METER_PORT,
        "pressure_port_mapping_source": A2_21_PRESSURE_PORT_MAPPING_SOURCE,
        "pressure_port_mapping_verified_at": A2_21_PRESSURE_PORT_MAPPING_VERIFIED_AT,
        "stale_advantech_ports_found": False,
        "stale_advantech_ports": [],
        "a2_real_probe_config_ports_after_mapping": config_ports,
        "a2_real_probe_config_ports_mapping_source": "advantech_fixed_shift_old_com_minus_8",
        "a2_19_device_precheck_failure_likely_pressure_port_mismatch": True,
        "a2_19_device_precheck_failure_root_cause": "stale_advantech_com_shift_config_mismatch",
        "a2_19_state_machine_real_verified": False,
        "relay_port_identity_confirmed": False,
        "relay_port_identity_confirmation_reason": "advantech_fixed_shift_mapping_applied_open_only_not_identity",
        "relay_a_candidate_ports": list(A2_21_RELAY_A_CANDIDATE_PORTS),
        "relay_b_candidate_ports": list(A2_21_RELAY_B_CANDIDATE_PORTS),
        "temperature_chamber_probe_import_path_fixed": True,
        "temperature_chamber_port_identity_confirmed": False,
        "temperature_chamber_optional_context": True,
    }


def _apply_a2_21_pressure_port_alignment(aligned_raw_cfg: dict[str, Any]) -> dict[str, Any]:
    metadata = _a2_21_pressure_port_alignment_metadata()
    devices = aligned_raw_cfg.setdefault("devices", {})
    if not isinstance(devices, dict):
        devices = {}
        aligned_raw_cfg["devices"] = devices
    stale_ports = sorted(
        {
            str(device.get("port") or "").upper()
            for device in devices.values()
            if isinstance(device, Mapping)
            and str(device.get("port") or "").upper() in A2_21_ADVANTECH_COM_SHIFT_MAPPING
        }
    )

    pressure_controller = devices.setdefault("pressure_controller", {})
    if isinstance(pressure_controller, dict):
        pressure_controller["port"] = A2_21_ADVANTECH_MAPPED_PORTS["pressure_controller"]
        pressure_controller.setdefault("enabled", True)
        pressure_controller.setdefault("baud", 9600)
        pressure_controller.setdefault("line_ending", "LF")

    pressure_aliases = [name for name in ("pressure_meter", "pressure_gauge") if isinstance(devices.get(name), dict)]
    if not pressure_aliases:
        pressure_aliases = ["pressure_gauge"]
        devices["pressure_gauge"] = {"enabled": True, "baud": 9600, "dest_id": "01"}
    for alias in pressure_aliases:
        pressure_meter = devices.get(alias)
        if isinstance(pressure_meter, dict):
            pressure_meter["port"] = A2_21_ADVANTECH_MAPPED_PORTS["pressure_meter"]
            pressure_meter.setdefault("enabled", True)
            pressure_meter.setdefault("baud", 9600)
            pressure_meter.setdefault("dest_id", "01")

    direct_port_updates = {
        "relay": A2_21_ADVANTECH_MAPPED_PORTS["relay_a"],
        "relay_a": A2_21_ADVANTECH_MAPPED_PORTS["relay_a"],
        "relay_8": A2_21_ADVANTECH_MAPPED_PORTS["relay_b"],
        "relay_b": A2_21_ADVANTECH_MAPPED_PORTS["relay_b"],
        "temperature_chamber": A2_21_ADVANTECH_MAPPED_PORTS["temperature_chamber"],
        "thermometer": A2_21_ADVANTECH_MAPPED_PORTS["thermometer"],
        "dewpoint_meter": A2_21_ADVANTECH_MAPPED_PORTS["dewpoint_meter"],
        "humidity_generator": A2_21_ADVANTECH_MAPPED_PORTS["humidity_generator"],
    }
    for device_name, mapped_port in direct_port_updates.items():
        device_cfg = devices.get(device_name)
        if isinstance(device_cfg, dict):
            device_cfg["port"] = mapped_port

    metadata["stale_advantech_ports_found"] = bool(stale_ports)
    metadata["stale_advantech_ports"] = stale_ports

    for section_name in ("run001_a2", "a2_co2_7_pressure_no_write_probe"):
        section = aligned_raw_cfg.setdefault(section_name, {})
        if isinstance(section, dict):
            section.update(metadata)
    workflow = aligned_raw_cfg.setdefault("workflow", {})
    if not isinstance(workflow, dict):
        workflow = {}
        aligned_raw_cfg["workflow"] = workflow
    pressure_cfg = workflow.setdefault("pressure", {})
    if not isinstance(pressure_cfg, dict):
        pressure_cfg = {}
        workflow["pressure"] = pressure_cfg
    pressure_cfg.update(metadata)
    aligned_raw_cfg["a2_21_pressure_port_alignment"] = dict(metadata)
    return metadata


def _device_precheck_service_value(service_summary: Mapping[str, Any], *keys: str) -> Any:
    stats = service_summary.get("stats")
    for key in keys:
        if key in service_summary:
            return service_summary.get(key)
    if isinstance(stats, Mapping):
        for key in keys:
            if key in stats:
                return stats.get(key)
    return None


def _coerce_string_list(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    if isinstance(value, tuple):
        return [str(item).strip() for item in value if str(item).strip()]
    if value in (None, ""):
        return []
    text = str(value).strip()
    if text.startswith("[") and text.endswith("]"):
        items = re.findall(r"'([^']+)'|\"([^\"]+)\"|([^,\s\[\]]+)", text)
        return [next(part for part in item if part).strip() for item in items if any(item)]
    return [text]


def _extract_named_list_from_text(text: str, key: str) -> list[str]:
    match = re.search(rf"{re.escape(key)}\s*=\s*\[([^\]]*)\]", text)
    if not match:
        return []
    return _coerce_string_list(f"[{match.group(1)}]")


def _device_precheck_text_blob(
    service_summary: Mapping[str, Any],
    execution: Mapping[str, Any],
    route_rows: list[dict[str, Any]],
) -> str:
    parts: list[str] = []
    for source in (service_summary, _mapping(service_summary.get("stats")), execution):
        for key in (
            "service_status_message",
            "service_status_error",
            "failure_reason",
            "fail_reason",
            "a1_fail_reason",
            "execution_error",
        ):
            value = source.get(key) if isinstance(source, Mapping) else None
            if value not in (None, ""):
                parts.append(str(value))
    for key in ("run_log_text", "run_log_tail"):
        value = execution.get(key)
        if value not in (None, ""):
            parts.append(str(value))
    for row in route_rows:
        message = row.get("message")
        if message not in (None, ""):
            parts.append(str(message))
        actual = row.get("actual")
        if isinstance(actual, Mapping):
            command_error = actual.get("command_error")
            if command_error not in (None, ""):
                parts.append(str(command_error))
    return "\n".join(parts)


def _selected_device_cfg(raw_cfg: Mapping[str, Any], device_name: str) -> tuple[str, dict[str, Any]]:
    devices = _mapping(raw_cfg.get("devices"))
    for alias in _DEVICE_PRECHECK_ALIASES.get(device_name, (device_name,)):
        value = devices.get(alias)
        if isinstance(value, Mapping):
            return alias, dict(value)
    return "", {}


def _device_precheck_config_ports(raw_cfg: Mapping[str, Any]) -> dict[str, Any]:
    ports: dict[str, Any] = {}
    for device_name in _DEVICE_PRECHECK_ALIASES:
        selected_key, cfg = _selected_device_cfg(raw_cfg, device_name)
        ports[device_name] = {
            "selected_config_key": selected_key,
            "port": str(cfg.get("port") or ""),
            "baud": int(cfg.get("baud") or cfg.get("baudrate") or 9600) if cfg else None,
            "enabled": (_as_bool(cfg.get("enabled")) if cfg else None),
        }
        if cfg and ports[device_name]["enabled"] is None:
            ports[device_name]["enabled"] = True
    return ports


def _normalized_path_text(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    return str(Path(text)).replace("\\", "/").rstrip("/").lower()


def _safe_float_from_rows(rows: list[dict[str, Any]], *names: str) -> Optional[float]:
    for row in rows:
        for name in names:
            parsed = _as_float(row.get(name))
            if parsed is not None:
                return float(parsed)
        actual = row.get("actual")
        if isinstance(actual, Mapping):
            for name in names:
                parsed = _as_float(actual.get(name))
                if parsed is not None:
                    return float(parsed)
    return None


def _pace_error_detail(text: str) -> tuple[str, str, str]:
    match = re.search(r"PACE_COMMAND_ERROR\(command=([^,)]*),\s*error=([^)]*)\)", text)
    if not match:
        return "", "", ""
    command = match.group(1).strip()
    detail = match.group(2).strip()
    return command, "PACE_COMMAND_ERROR", detail


def _precheck_command_attempts(
    execution: Mapping[str, Any],
    route_rows: list[dict[str, Any]],
    text_blob: str,
) -> list[dict[str, Any]]:
    attempts: list[dict[str, Any]] = []
    for row in execution.get("io_log_rows") or []:
        if not isinstance(row, Mapping):
            continue
        command = str(row.get("data") or row.get("command") or "").strip()
        if not command:
            continue
        device_name = ""
        if "set_in_limits" in command or "set_output" in command or "set_vent" in command:
            device_name = "pressure_controller"
        elif "set_valve" in command:
            device_name = "relay"
        attempts.append(
            {
                "source": "io_log",
                "device_name": device_name,
                "command": command,
                "direction": str(row.get("direction") or ""),
                "no_write_proxy": str(row.get("device") or "") == "NoWriteDeviceProxy",
            }
        )
    for row in route_rows:
        action = str(row.get("action") or "").strip()
        if action not in {"set_output", "set_vent", "set_valve", "set_valves_bulk"}:
            continue
        attempts.append(
            {
                "source": "route_trace",
                "device_name": "pressure_controller" if action in {"set_output", "set_vent"} else "relay",
                "command": action,
                "direction": "TX",
                "result": str(row.get("result") or ""),
                "message": str(row.get("message") or ""),
                "no_write_proxy": False,
            }
        )
    for command, detail in re.findall(
        r"PACE_COMMAND_ERROR\(command=([^,)]*),\s*error=([^)]*)\)",
        text_blob,
    ):
        attempts.append(
            {
                "source": "error_text",
                "device_name": "pressure_controller",
                "command": command.strip(),
                "direction": "TX",
                "result": "fail",
                "exception_type": "PACE_COMMAND_ERROR",
                "exception_message": detail.strip(),
                "no_write_proxy": False,
            }
        )
    return attempts


def _device_precheck_diagnostics(
    raw_cfg: Mapping[str, Any],
    execution: Mapping[str, Any],
    service_summary: Mapping[str, Any],
    route_rows: list[dict[str, Any]],
    point_results: list[dict[str, Any]],
) -> dict[str, Any]:
    text_blob = _device_precheck_text_blob(service_summary, execution, route_rows)
    failure_stage = str(
        _device_precheck_service_value(service_summary, "failure_stage")
        or ("device_precheck" if "Device precheck failed" in text_blob else "")
    ).strip()
    failed_devices = _coerce_string_list(
        _device_precheck_service_value(service_summary, "failed_devices")
    ) or _extract_named_list_from_text(text_blob, "failed_devices")
    critical_failed = _coerce_string_list(
        _device_precheck_service_value(service_summary, "critical_devices_failed")
    ) or _extract_named_list_from_text(text_blob, "critical_devices_failed")
    optional_failed = _coerce_string_list(
        _device_precheck_service_value(service_summary, "optional_context_devices_failed")
    ) or _extract_named_list_from_text(text_blob, "optional_context_devices_failed")
    critical_devices = _coerce_string_list(
        _device_precheck_service_value(service_summary, "critical_devices_required")
    ) or list(_DEVICE_PRECHECK_CRITICAL_DEFAULT)
    if not critical_failed:
        critical_failed = [name for name in failed_devices if name in critical_devices]
    config_ports = _device_precheck_config_ports(raw_cfg)
    command_attempts = _precheck_command_attempts(execution, route_rows, text_blob)
    open_all_results: dict[str, Any] = {}
    health_check_results: dict[str, Any] = {}
    failure_reason_by_device: dict[str, str] = {}
    device_details: list[dict[str, Any]] = []
    device_names = list(dict.fromkeys(list(_DEVICE_PRECHECK_ALIASES) + failed_devices + optional_failed))
    inferred_failure_phase = "open_all" if failure_stage == "device_precheck" and failed_devices else ""

    controller_command, controller_exc_type, controller_exc_message = _pace_error_detail(text_blob)
    controller_precheck_command = (
        controller_command
        or str(_device_precheck_service_value(service_summary, "pressure_controller_precheck_command") or "")
    )
    pressure_meter_exc = "NO_RESPONSE" if "pressure_meter): NO_RESPONSE" in text_blob else ""
    pressure_controller_no_response = "NO_RESPONSE" if "pressure_controller): NO_RESPONSE" in text_blob else ""

    for device_name in device_names:
        selected_key, cfg = _selected_device_cfg(raw_cfg, device_name)
        failed = device_name in failed_devices or device_name in critical_failed or device_name in optional_failed
        critical_or_optional = "optional" if device_name in optional_failed and device_name not in critical_failed else "critical"
        enabled = _as_bool(cfg.get("enabled")) if cfg else None
        if enabled is None and cfg:
            enabled = True
        open_attempted = bool(failure_stage == "device_precheck" and enabled is not False)
        open_ok = False if failed and inferred_failure_phase == "open_all" else (True if open_attempted else None)
        health_attempted = False
        health_ok = None
        command_sent = any(attempt.get("device_name") in {device_name, "relay"} for attempt in command_attempts)
        exception_type = ""
        exception_message = ""
        raw_response = ""
        if device_name == "pressure_controller":
            exception_type = controller_exc_type or ("RuntimeError" if pressure_controller_no_response else "")
            exception_message = controller_exc_message or pressure_controller_no_response
        elif device_name == "pressure_meter":
            exception_type = "RuntimeError" if pressure_meter_exc else ""
            exception_message = pressure_meter_exc
        elif device_name in {"relay_a", "relay_b"} and failed:
            exception_message = "relay_init_unavailable_or_no_response"
        failure_reason = ""
        if failed:
            failure_reason = exception_message or "device_precheck_failed"
        failure_reason_by_device[device_name] = failure_reason
        open_all_results[device_name] = {
            "attempted": open_attempted,
            "ok": open_ok,
            "failure_reason": failure_reason if inferred_failure_phase == "open_all" else "",
        }
        health_check_results[device_name] = {
            "attempted": health_attempted,
            "ok": health_ok,
            "failure_reason": failure_reason if health_attempted else "",
        }
        device_details.append(
            {
                "device_name": device_name,
                "selected_config_key": selected_key,
                "configured_port": str(cfg.get("port") or ""),
                "baud": int(cfg.get("baud") or cfg.get("baudrate") or 9600) if cfg else None,
                "enabled": enabled,
                "critical_or_optional": critical_or_optional,
                "open_attempted": open_attempted,
                "open_ok": open_ok,
                "health_check_attempted": health_attempted,
                "health_check_ok": health_ok,
                "command_sent": command_sent,
                "raw_response": raw_response,
                "timeout_s": _as_float(
                    cfg.get("response_timeout_s") or cfg.get("timeout") or cfg.get("timeout_s")
                )
                if cfg
                else None,
                "exception_type": exception_type,
                "exception_message": exception_message,
                "failure_phase": inferred_failure_phase if failed else "",
                "failure_reason": failure_reason,
            }
        )

    points_alignment = _mapping(execution.get("points_alignment"))
    manifest = _mapping(execution.get("run_manifest")) or _mapping(execution.get("manifest"))
    wrapper_config = (
        points_alignment.get("downstream_aligned_config_path")
        or execution.get("execution_config_path")
        or execution.get("config_path")
    )
    underlying_config = manifest.get("config_path") or execution.get("underlying_config_path") or execution.get("config_path")
    config_match: Any = None
    if wrapper_config or underlying_config:
        config_match = bool(
            wrapper_config
            and underlying_config
            and _normalized_path_text(wrapper_config) == _normalized_path_text(underlying_config)
        )

    safe_stop_duration_s = _as_float(
        _device_precheck_service_value(service_summary, "safe_stop_duration_s")
    ) or _safe_float_from_rows(
        [dict(row) for row in execution.get("timing_trace_rows") or [] if isinstance(row, Mapping)],
        "duration_s",
    )
    safe_stop_bounded_timeout_s = _safe_float_from_rows(
        [dict(row) for row in execution.get("timing_trace_rows") or [] if isinstance(row, Mapping)],
        "expected_max_s",
    )
    if safe_stop_bounded_timeout_s is None:
        safe_stop_bounded_timeout_s = 30.0
    safe_stop_pace_command_error_detail = controller_exc_message
    if controller_exc_type and not safe_stop_pace_command_error_detail:
        safe_stop_pace_command_error_detail = "empty_error_detail"
    controller_unavailable = "pressure_controller" in failed_devices or bool(pressure_controller_no_response)
    pressure_meter_unavailable = "pressure_meter" in failed_devices or bool(pressure_meter_exc)
    pressure_meter_cfg = _device_cfg(raw_cfg, "pressure_meter", "pressure_gauge", "digital_pressure_gauge_p3")
    pressure_meter_command = str(
        _device_precheck_service_value(service_summary, "pressure_meter_precheck_command") or ""
    )
    if not pressure_meter_command and pressure_meter_cfg:
        pressure_meter_command = f"*{str(pressure_meter_cfg.get('dest_id') or '01')}00P3\\r\\n"

    legacy_match = {
        name: str(config_ports.get(name, {}).get("port") or "").upper() == expected
        for name, expected in _DEVICE_PRECHECK_LEGACY_EXPECTED_PORTS.items()
    }
    return {
        "device_precheck_failure_stage": failure_stage,
        "device_precheck_failure_phase": inferred_failure_phase,
        "device_precheck_failed_devices": failed_devices,
        "device_precheck_critical_failed_devices": critical_failed,
        "device_precheck_optional_failed_devices": optional_failed,
        "device_precheck_device_details": device_details,
        "device_precheck_config_ports": config_ports,
        "device_precheck_legacy_expected_ports": dict(_DEVICE_PRECHECK_LEGACY_EXPECTED_PORTS),
        "device_precheck_legacy_expected_ports_match": legacy_match,
        "device_precheck_wrapper_underlying_config_match": config_match,
        "device_precheck_open_all_results": open_all_results,
        "device_precheck_health_check_results": health_check_results,
        "device_precheck_command_attempts": command_attempts,
        "device_precheck_failure_reason_by_device": failure_reason_by_device,
        "pressure_controller_precheck_command": controller_precheck_command,
        "pressure_controller_precheck_raw_response": "",
        "pressure_controller_precheck_exception_type": controller_exc_type
        or ("RuntimeError" if pressure_controller_no_response else ""),
        "pressure_controller_precheck_exception_message": controller_exc_message
        or pressure_controller_no_response,
        "pressure_controller_precheck_sens_pres_query_sent": ":SENS:PRES?" in text_blob,
        "pressure_controller_precheck_output_state_query_sent": ":OUTP:STAT?" in text_blob,
        "pressure_controller_precheck_line_ending": _line_ending_label(
            _device_cfg(raw_cfg, "pressure_controller").get("line_ending")
        ),
        "pressure_controller_precheck_timeout_s": _as_float(
            _device_cfg(raw_cfg, "pressure_controller").get("timeout")
        ),
        "pressure_meter_precheck_command": pressure_meter_command,
        "pressure_meter_precheck_raw_response": "",
        "pressure_meter_precheck_exception_type": "RuntimeError" if pressure_meter_exc else "",
        "pressure_meter_precheck_exception_message": pressure_meter_exc,
        "pressure_meter_precheck_line_ending": "CRLF",
        "pressure_meter_precheck_timeout_s": _as_float(
            pressure_meter_cfg.get("response_timeout_s")
            or pressure_meter_cfg.get("timeout")
            or pressure_meter_cfg.get("timeout_s")
        ),
        "relay_a_precheck_failure_reason": failure_reason_by_device.get("relay_a", ""),
        "relay_b_precheck_failure_reason": failure_reason_by_device.get("relay_b", ""),
        "safe_stop_controller_unavailable": bool(controller_unavailable),
        "safe_stop_skipped_pressure_command_due_to_unavailable_controller": False,
        "safe_stop_pace_command_error_detail": safe_stop_pace_command_error_detail,
        "safe_stop_bounded_timeout_s": safe_stop_bounded_timeout_s,
        "safe_stop_duration_exceeded_expected": bool(
            safe_stop_duration_s is not None
            and safe_stop_bounded_timeout_s is not None
            and float(safe_stop_duration_s) > float(safe_stop_bounded_timeout_s)
        ),
        "safe_stop_no_pressure_sample_available_reason": (
            "pressure_controller_and_pressure_meter_unavailable"
            if controller_unavailable
            and pressure_meter_unavailable
            and not any(
                _as_float(point.get("pressure_gauge_hpa_before_ready")) is not None
                or _as_float(point.get("pressure_gauge_hpa_before_sample")) is not None
                for point in point_results
            )
            else ""
        ),
    }


def _pressure_points(raw_cfg: Mapping[str, Any]) -> list[float]:
    value = _first_value(
        raw_cfg,
        (
            "pressure_points_hpa",
            "authorized_pressure_points_hpa",
            "a2_co2_7_pressure_no_write_probe.pressure_points_hpa",
            "a2_co2_7_pressure_no_write_probe.authorized_pressure_points_hpa",
            "run001_a2.pressure_points_hpa",
            "run001_a2.authorized_pressure_points_hpa",
        ),
    )
    return _float_list(value)


def _skip0_only(raw_cfg: Mapping[str, Any]) -> bool:
    if _truthy(raw_cfg, ("skip0", "a2_co2_7_pressure_no_write_probe.skip0", "run001_a2.skip0")):
        return True
    value = _first_value(
        raw_cfg,
        (
            "skip_co2_ppm",
            "workflow.skip_co2_ppm",
            "a2_co2_7_pressure_no_write_probe.skip_co2_ppm",
            "run001_a2.skip_co2_ppm",
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
            "a2_co2_7_pressure_no_write_probe.single_temperature",
            "run001_a2.single_temperature",
            "run001_a2.single_temperature_group",
        ),
    ):
        return True
    value = _first_value(raw_cfg, ("selected_temps_c", "workflow.selected_temps_c"))
    return isinstance(value, list) and len(value) == 1


def _sample_min_count(raw_cfg: Mapping[str, Any]) -> int:
    value = _first_value(
        raw_cfg,
        (
            "sample_min_count_per_pressure",
            "a2_co2_7_pressure_no_write_probe.sample_min_count_per_pressure",
            "run001_a2.sample_min_count_per_pressure",
            "workflow.sampling.count",
        ),
    )
    try:
        return max(1, int(float(value)))
    except Exception:
        return 4


def _pressure_freshness_max_age_ms(raw_cfg: Mapping[str, Any]) -> float:
    value = _first_value(
        raw_cfg,
        (
            "pressure_cache_max_age_ms",
            "a2_co2_7_pressure_no_write_probe.pressure_cache_max_age_ms",
            "run001_a2.pressure_cache_max_age_ms",
            "workflow.pressure.pressure_sample_stale_threshold_s",
        ),
    )
    parsed = _as_float(value)
    if parsed is None:
        return 2000.0
    if "pressure_sample_stale_threshold_s" in str(value):
        return float(parsed) * 1000.0
    return float(parsed) * (1000.0 if float(parsed) < 50.0 else 1.0)


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


def _load_json_mapping_accept_bom(path: str | Path) -> dict[str, Any]:
    return load_json_mapping(path)


def _validate_operator_confirmation(
    path: Optional[str | Path],
    *,
    expected_branch: str = "",
    expected_head: str = "",
    expected_config_path: str = "",
    expected_a1r_output_dir: str = "",
) -> tuple[dict[str, Any], dict[str, Any]]:
    errors: list[str] = []
    if not path:
        return {}, {"valid": False, "errors": ["missing_operator_confirmation_json"]}
    confirmation_path = Path(path).expanduser()
    if not confirmation_path.exists():
        return {}, {"valid": False, "errors": ["missing_operator_confirmation_json"]}
    try:
        payload = _load_json_mapping_accept_bom(confirmation_path)
    except Exception as exc:
        return {}, {"valid": False, "errors": [f"invalid_operator_confirmation_json:{exc}"]}

    for field in A2_REQUIRED_OPERATOR_FIELDS:
        if payload.get(field) in (None, ""):
            errors.append(f"operator_confirmation_missing_{field}")
    if not _same_pressure_points(payload.get("pressure_points_hpa")):
        errors.append("operator_confirmation_pressure_points_mismatch")
    if str(payload.get("pressure_source") or "").strip().lower() != "v1_aligned":
        errors.append("operator_confirmation_pressure_source_not_v1_aligned")

    ack = payload.get("explicit_acknowledgement")
    if not isinstance(ack, Mapping):
        errors.append("operator_confirmation_missing_explicit_acknowledgement")
        ack = {}
    for key in A2_REQUIRED_TRUE_ACKS:
        if _as_bool(ack.get(key)) is not True:
            errors.append(f"operator_ack_missing_{key}")
    for key in A2_REQUIRED_FALSE_ACKS:
        if _as_bool(ack.get(key)) is not False:
            errors.append(f"operator_ack_not_false_{key}")

    if expected_branch and str(payload.get("branch") or "") != expected_branch:
        errors.append("operator_confirmation_branch_mismatch")
    if expected_head and str(payload.get("HEAD") or "") != expected_head:
        errors.append("operator_confirmation_head_mismatch")
    if expected_config_path:
        actual = str(payload.get("config_path") or "")
        if not actual or Path(actual).resolve() != Path(expected_config_path).resolve():
            errors.append("operator_confirmation_config_path_mismatch")
    if expected_a1r_output_dir:
        actual = str(payload.get("a1r_output_dir") or "")
        if not actual or Path(actual).resolve() != Path(expected_a1r_output_dir).resolve():
            errors.append("operator_confirmation_a1r_output_dir_mismatch")

    return payload, {"valid": not errors, "errors": errors, "path": str(confirmation_path.resolve())}


def evaluate_a2_co2_7_pressure_no_write_gate(
    raw_cfg: Mapping[str, Any],
    *,
    cli_allow: bool = False,
    env: Optional[Mapping[str, str]] = None,
    operator_confirmation_path: Optional[str | Path] = None,
    branch: str = "",
    head: str = "",
    config_path: str = "",
    run_app_py_untouched: bool = True,
) -> A2Admission:
    env_map = os.environ if env is None else env
    reasons: list[str] = []
    if not cli_allow:
        reasons.append("missing_cli_flag_allow_v2_a2_co2_7_pressure_no_write_real_com")
    if str(env_map.get(A2_ENV_VAR, "")).strip() != A2_ENV_VALUE:
        reasons.append("missing_env_gas_cal_v2_a2_co2_7_pressure_no_write_real_com")

    a1r_dir = _output_dir_value(raw_cfg, "a1r_output_dir")
    a1r_summary, a1r_reasons = _load_prereq_summary(
        a1r_dir,
        expected_source="real_probe_a1r_minimal_no_write_sampling",
    )
    reasons.extend(f"a1r_{item}" for item in a1r_reasons)
    if a1r_summary and a1r_summary.get("a1r_minimal_sampling_executed") is not True:
        reasons.append("a1r_minimal_sampling_not_executed")

    r0_1_pass = bool(a1r_summary.get("r0_1_reference_readonly_prereq_pass") is True)
    r0_full_pass = bool(a1r_summary.get("r0_full_query_only_prereq_pass") is True)
    r1_pass = bool(a1r_summary.get("r1_conditioning_only_prereq_pass") is True)
    a1r_pass = bool(a1r_summary.get("final_decision") == "PASS" and not a1r_reasons)
    if not r0_1_pass:
        reasons.append("r0_1_reference_readonly_prereq_missing_or_not_pass")
    if not r0_full_pass:
        reasons.append("r0_full_query_only_prereq_missing_or_not_pass")
    if not r1_pass:
        reasons.append("r1_conditioning_only_prereq_missing_or_not_pass")
    if not a1r_pass:
        reasons.append("a1r_minimal_sampling_prereq_missing_or_not_pass")

    operator_payload, operator_validation = _validate_operator_confirmation(
        operator_confirmation_path,
        expected_branch=branch,
        expected_head=head,
        expected_config_path=config_path,
        expected_a1r_output_dir=a1r_dir,
    )
    reasons.extend(str(item) for item in operator_validation.get("errors", []))

    if branch and branch != "codex/run001-a1-no-write-dry-run":
        reasons.append("current_branch_not_run001_a1_no_write_dry_run")
    if not str(head or "").strip():
        reasons.append("current_head_missing")
    if _scope(raw_cfg) not in {"a2_co2_7_pressure_no_write", "run001_a2_co2_no_write_pressure_sweep"}:
        reasons.append("config_scope_not_a2_co2_7_pressure_no_write")
    if not _truthy(raw_cfg, ("co2_only", "a2_co2_7_pressure_no_write_probe.co2_only", "run001_a2.co2_only")):
        reasons.append("config_not_co2_only")
    if not _skip0_only(raw_cfg):
        reasons.append("config_not_skip0")
    if not _truthy(raw_cfg, ("single_route", "a2_co2_7_pressure_no_write_probe.single_route", "run001_a2.single_route")):
        reasons.append("config_not_single_route")
    if not _single_temperature(raw_cfg):
        reasons.append("config_not_single_temperature")
    if not _truthy(raw_cfg, ("no_write", "a2_co2_7_pressure_no_write_probe.no_write", "run001_a2.no_write")):
        reasons.append("config_no_write_not_true")
    if not _same_pressure_points(_pressure_points(raw_cfg)):
        reasons.append("config_pressure_points_not_exact_a2_set")
    if not _truthy(raw_cfg, ("v1_fallback_required", "a2_co2_7_pressure_no_write_probe.v1_fallback_required")):
        reasons.append("config_v1_fallback_required_not_true")
    if not run_app_py_untouched:
        reasons.append("run_app_py_not_untouched")

    false_required = {
        "a3_enabled": ("a3_enabled", "a2_co2_7_pressure_no_write_probe.a3_enabled", "run001_a2.a3_enabled"),
        "h2o_enabled": ("h2o_enabled", "a2_co2_7_pressure_no_write_probe.h2o_enabled", "run001_a2.h2o_enabled"),
        "full_group_enabled": (
            "full_group_enabled",
            "a2_co2_7_pressure_no_write_probe.full_group_enabled",
            "run001_a2.full_group_enabled",
            "run001_a2.full_h2o_co2_group",
        ),
        "multi_temperature_enabled": (
            "multi_temperature_enabled",
            "a2_co2_7_pressure_no_write_probe.multi_temperature_enabled",
            "run001_a2.multi_temperature_enabled",
        ),
        "mode_switch_enabled": (
            "mode_switch_enabled",
            "a2_co2_7_pressure_no_write_probe.mode_switch_enabled",
            "run001_a2.mode_switch_enabled",
        ),
        "analyzer_id_write_enabled": (
            "analyzer_id_write_enabled",
            "a2_co2_7_pressure_no_write_probe.analyzer_id_write_enabled",
            "run001_a2.analyzer_id_write_enabled",
        ),
        "senco_write_enabled": (
            "senco_write_enabled",
            "a2_co2_7_pressure_no_write_probe.senco_write_enabled",
            "run001_a2.senco_write_enabled",
        ),
        "calibration_write_enabled": (
            "calibration_write_enabled",
            "a2_co2_7_pressure_no_write_probe.calibration_write_enabled",
            "run001_a2.calibration_write_enabled",
        ),
        "chamber_set_temperature_enabled": (
            "chamber_set_temperature_enabled",
            "a2_co2_7_pressure_no_write_probe.chamber_set_temperature_enabled",
            "run001_a2.chamber_set_temperature_enabled",
        ),
        "chamber_start_enabled": (
            "chamber_start_enabled",
            "a2_co2_7_pressure_no_write_probe.chamber_start_enabled",
            "run001_a2.chamber_start_enabled",
        ),
        "chamber_stop_enabled": (
            "chamber_stop_enabled",
            "a2_co2_7_pressure_no_write_probe.chamber_stop_enabled",
            "run001_a2.chamber_stop_enabled",
        ),
        "real_primary_latest_refresh": (
            "real_primary_latest_refresh",
            "a2_co2_7_pressure_no_write_probe.real_primary_latest_refresh",
            "run001_a2.real_primary_latest_refresh",
        ),
    }
    for name, paths in false_required.items():
        if not _explicit_false(raw_cfg, paths):
            reasons.append(f"config_{name}_not_disabled")

    reasons = list(dict.fromkeys(reasons))
    approved = not reasons
    evidence = {
        **A2_EVIDENCE_MARKERS,
        "admission_approved": approved,
        "r0_1_reference_readonly_prereq_pass": r0_1_pass,
        "r0_full_query_only_prereq_pass": r0_full_pass,
        "r1_conditioning_only_prereq_pass": r1_pass,
        "a1r_minimal_sampling_prereq_pass": a1r_pass,
        "a1r_output_dir": a1r_dir,
        "a2_pressure_sweep_executed": False,
        "a3_allowed": False,
        **A2_SAFETY_ASSERTION_DEFAULTS,
        "rejection_reasons": reasons,
    }
    return A2Admission(
        approved=approved,
        reasons=tuple(reasons),
        evidence=evidence,
        operator_confirmation=operator_payload,
        operator_validation=operator_validation,
        prereq_summaries={"a1r": a1r_summary},
    )


def _default_output_dir() -> Path:
    timestamp = datetime.now().astimezone().strftime("%Y%m%d_%H%M")
    return Path(f"D:/gas_calibrator_step3a_a2_co2_7_pressure_no_write_probe_{timestamp}").resolve()


def _sha256_file(path: str | Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _json_clone_mapping(raw_cfg: Mapping[str, Any]) -> dict[str, Any]:
    return json.loads(json.dumps(dict(raw_cfg), ensure_ascii=False))


def _selected_temperature_c(raw_cfg: Mapping[str, Any]) -> float:
    value = _first_value(
        raw_cfg,
        (
            "workflow.selected_temps_c",
            "selected_temps_c",
            "a2_co2_7_pressure_no_write_probe.temperature_c",
            "a2_co2_7_pressure_no_write_probe.temp_chamber_c",
        ),
    )
    if isinstance(value, list) and value:
        parsed = _as_float(value[0])
    else:
        parsed = _as_float(value)
    return float(parsed) if parsed is not None else 20.0


def _co2_point_ppm(raw_cfg: Mapping[str, Any]) -> float:
    value = _first_value(
        raw_cfg,
        (
            "a2_co2_7_pressure_no_write_probe.co2_ppm",
            "a2_co2_7_pressure_no_write_probe.target_co2_ppm",
            "run001_a2.co2_ppm",
            "run001_a2.target_co2_ppm",
            "workflow.co2_ppm",
        ),
    )
    parsed = _as_float(value)
    return float(parsed) if parsed is not None else 100.0


def _build_a2_downstream_point_rows(raw_cfg: Mapping[str, Any]) -> list[dict[str, Any]]:
    temperature_c = _selected_temperature_c(raw_cfg)
    co2_ppm = _co2_point_ppm(raw_cfg)
    rows: list[dict[str, Any]] = []
    for index, pressure in enumerate(A2_ALLOWED_PRESSURE_POINTS_HPA, start=1):
        rows.append(
            {
                "index": index,
                "route": "co2",
                "pressure_hpa": float(pressure),
                "target_pressure_hpa": float(pressure),
                "co2_ppm": co2_ppm,
                "temperature_c": temperature_c,
                "temp_chamber_c": temperature_c,
                "co2_group": "A",
                "cylinder_nominal_ppm": co2_ppm,
            }
        )
    return rows


def _point_row_pressure(row: Mapping[str, Any]) -> Optional[float]:
    for key in ("pressure_hpa", "target_pressure_hpa", "pressure"):
        parsed = _as_float(row.get(key))
        if parsed is not None:
            return float(parsed)
    return None


def _validate_a2_downstream_point_rows(rows: list[Mapping[str, Any]]) -> list[str]:
    reasons: list[str] = []
    if len(rows) != len(A2_ALLOWED_PRESSURE_POINTS_HPA):
        reasons.append("a2_point_count_mismatch")
    routes = []
    pressures = []
    for row in rows:
        route = str(row.get("route", "") or "").strip().lower()
        routes.append(route)
        pressure = _point_row_pressure(row)
        if not route:
            reasons.append("a2_point_route_missing")
        if pressure is None:
            reasons.append("a2_point_pressure_missing")
        else:
            pressures.append(pressure)
    if set(routes) != {"co2"}:
        reasons.append("a2_points_not_co2_only")
    if not _same_pressure_points(pressures):
        reasons.append("a2_point_pressure_list_mismatch")
    return list(dict.fromkeys(reasons))


def _write_json_no_bom(path: Path, payload: Mapping[str, Any] | list[Mapping[str, Any]]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def prepare_a2_downstream_points_config(
    raw_cfg: Mapping[str, Any],
    *,
    config_path: str | Path,
    output_dir: str | Path,
) -> tuple[Path, dict[str, Any]]:
    from gas_calibrator.v2.core.run001_a1_dry_run import load_point_rows, resolve_config_relative_path

    run_dir = Path(output_dir).expanduser().resolve()
    run_dir.mkdir(parents=True, exist_ok=True)
    original_config_path = Path(config_path).expanduser().resolve()
    raw_paths = raw_cfg.get("paths") if isinstance(raw_cfg.get("paths"), Mapping) else {}
    raw_points_value = str((raw_paths or {}).get("points_excel") or "").strip()
    resolved_points = resolve_config_relative_path(original_config_path, raw_points_value)

    if raw_points_value:
        suffix = Path(raw_points_value).suffix.lower() or (resolved_points.suffix.lower() if resolved_points else "")
        if suffix and suffix != ".json":
            raise A2PointsConfigAlignmentError("a2_points_json_suffix_not_json")

    generated = False
    if resolved_points is not None and resolved_points.exists():
        points_path = resolved_points
        rows = load_point_rows(original_config_path, raw_cfg)
        reasons = _validate_a2_downstream_point_rows(rows)
        if reasons:
            raise A2PointsConfigAlignmentError("; ".join(reasons))
    else:
        generated = True
        rows = _build_a2_downstream_point_rows(raw_cfg)
        reasons = _validate_a2_downstream_point_rows(rows)
        if reasons:
            raise A2PointsConfigAlignmentError("; ".join(reasons))
        points_path = run_dir / "a2_3_v1_aligned_points.json"
        _write_json_no_bom(points_path, rows)

    aligned_raw_cfg = _json_clone_mapping(raw_cfg)
    pressure_port_alignment = _apply_a2_21_pressure_port_alignment(aligned_raw_cfg)
    paths = aligned_raw_cfg.setdefault("paths", {})
    if not isinstance(paths, dict):
        paths = {}
        aligned_raw_cfg["paths"] = paths
    paths["points_excel"] = str(points_path)
    workflow = aligned_raw_cfg.setdefault("workflow", {})
    if not isinstance(workflow, dict):
        workflow = {}
        aligned_raw_cfg["workflow"] = workflow
    pressure_cfg = workflow.setdefault("pressure", {})
    if not isinstance(pressure_cfg, dict):
        pressure_cfg = {}
        workflow["pressure"] = pressure_cfg
    pressure_cfg["a2_conditioning_pressure_source"] = "v1_aligned"
    pressure_cfg.setdefault("route_conditioning_high_frequency_vent_interval_s", 0.5)
    pressure_cfg.setdefault("route_conditioning_high_frequency_max_gap_s", 1.0)
    pressure_cfg.setdefault("route_conditioning_high_frequency_vent_window_s", 20.0)
    pressure_cfg.setdefault("route_conditioning_vent_maintenance_interval_s", 1.0)
    pressure_cfg.setdefault("route_conditioning_vent_maintenance_max_gap_s", 2.0)
    pressure_cfg.setdefault("route_conditioning_fast_vent_max_duration_s", 0.5)
    pressure_cfg.setdefault("route_conditioning_scheduler_sleep_step_s", 0.1)
    pressure_cfg.setdefault("route_conditioning_diagnostic_budget_ms", 100.0)
    pressure_cfg.setdefault("route_conditioning_pressure_monitor_budget_ms", 100.0)
    pressure_cfg.setdefault("route_conditioning_trace_write_budget_ms", 50.0)
    pressure_cfg.setdefault("route_conditioning_hard_abort_pressure_hpa", 1250.0)
    pressure_cfg.setdefault("a2_prearm_baseline_freshness_max_s", 2.0)
    pressure_cfg.setdefault(
        "preseal_capture_urgent_seal_threshold_hpa",
        pressure_cfg.get("preseal_abort_pressure_hpa", 1150.0),
    )
    pressure_cfg.setdefault("preseal_capture_hard_abort_pressure_hpa", 1250.0)
    pressure_cfg.setdefault("route_open_transient_window_enabled", True)
    pressure_cfg.setdefault("route_open_transient_recovery_timeout_s", 10.0)
    pressure_cfg.setdefault("route_open_transient_recovery_band_hpa", 10.0)
    pressure_cfg.setdefault("route_open_transient_stable_hold_s", 2.0)
    pressure_cfg.setdefault("route_open_transient_stable_span_hpa", 10.0)
    pressure_cfg.setdefault("route_open_transient_stable_slope_hpa_per_s", 1.0)
    stability_cfg = workflow.setdefault("stability", {})
    if not isinstance(stability_cfg, dict):
        stability_cfg = {}
        workflow["stability"] = stability_cfg
    temperature_cfg = stability_cfg.setdefault("temperature", {})
    if not isinstance(temperature_cfg, dict):
        temperature_cfg = {}
        stability_cfg["temperature"] = temperature_cfg
    temperature_cfg["skip_temperature_stabilization_wait"] = True
    temperature_cfg["temperature_stabilization_wait_skipped"] = True
    temperature_cfg["temperature_gate_mode"] = "current_pv_engineering_probe"
    temperature_cfg["temperature_not_part_of_acceptance"] = True
    temperature_cfg["wait_for_target_before_continue"] = False
    temperature_cfg["analyzer_chamber_temp_enabled"] = False
    temperature_cfg["soak_after_reach_s"] = 0.0
    probe_cfg = aligned_raw_cfg.setdefault("a2_co2_7_pressure_no_write_probe", {})
    if isinstance(probe_cfg, dict):
        probe_cfg["pressure_source"] = "v1_aligned"
        probe_cfg["temperature_stabilization_wait_skipped"] = True
        probe_cfg["temperature_gate_mode"] = "current_pv_engineering_probe"
        probe_cfg["temperature_not_part_of_acceptance"] = True
    aligned_config_path = run_dir / "a2_3_v1_aligned_downstream_config.json"
    _write_json_no_bom(aligned_config_path, aligned_raw_cfg)

    loaded_rows = load_point_rows(aligned_config_path, aligned_raw_cfg)
    loaded_reasons = _validate_a2_downstream_point_rows(loaded_rows)
    if loaded_reasons:
        raise A2PointsConfigAlignmentError("; ".join(loaded_reasons))

    point_pressures = [float(_point_row_pressure(row) or 0.0) for row in loaded_rows]
    point_routes = [str(row.get("route", "") or "").strip().lower() for row in loaded_rows]
    metadata = {
        "points_config_alignment_ready": True,
        "generated_points_json_path": str(points_path) if generated else "",
        "generated_points_json_sha256": _sha256_file(points_path) if generated else "",
        "effective_points_json_path": str(points_path),
        "effective_points_json_sha256": _sha256_file(points_path),
        "downstream_aligned_config_path": str(aligned_config_path),
        "downstream_points_row_count": len(loaded_rows),
        "downstream_point_routes": point_routes,
        "downstream_point_pressures_hpa": point_pressures,
        "downstream_points_gate_reasons": [],
        "downstream_points_generated": generated,
        **pressure_port_alignment,
    }
    return aligned_config_path, metadata


def execute_existing_v2_a2_pressure_sweep(config_path: str | Path) -> dict[str, Any]:
    from gas_calibrator.v2.core.no_write_guard import build_no_write_guard_from_raw_config
    from gas_calibrator.v2.core.run001_a2_no_write import (
        authorize_run001_a2_no_write_pressure_sweep,
    )
    from gas_calibrator.v2.entry import create_calibration_service_from_config, load_config_bundle

    resolved_config_path, raw_cfg, config = load_config_bundle(
        str(config_path),
        simulation_mode=False,
        allow_unsafe_step2_config=False,
        enforce_step2_execution_gate=False,
    )
    cli_args = SimpleNamespace(
        execute=True,
        confirm_real_machine_no_write=True,
        confirm_a2_no_write_pressure_sweep=True,
    )
    gate = authorize_run001_a2_no_write_pressure_sweep(
        config,
        raw_cfg,
        cli_args,
        config_path=resolved_config_path,
    )
    raw_cfg["_run001_a2_safety_gate"] = gate
    setattr(config, "_run001_a2_safety_gate", gate)
    service = create_calibration_service_from_config(
        config,
        raw_cfg=raw_cfg,
        preload_points=True,
        require_no_write_guard=True,
    )
    build_no_write_guard_from_raw_config(raw_cfg)
    service.run()
    run_dir = Path(service.session.output_dir)
    summary = _load_json_dict(run_dir / "summary.json")
    run_log_text = ""
    run_log_path = run_dir / "run.log"
    if run_log_path.exists():
        try:
            run_log_text = run_log_path.read_text(encoding="utf-8", errors="ignore")[-20000:]
        except Exception:
            run_log_text = ""
    return {
        "execution_run_dir": str(run_dir),
        "underlying_config_path": str(resolved_config_path),
        "run_manifest": _load_json_dict(run_dir / "run_manifest.json"),
        "manifest": _load_json_dict(run_dir / "manifest.json"),
        "no_write_guard": _load_json_dict(run_dir / "no_write_guard.json"),
        "workflow_timing_summary": _load_json_dict(run_dir / "workflow_timing_summary.json"),
        "service_summary": summary,
        "route_trace_rows": _load_jsonl(run_dir / "route_trace.jsonl"),
        "timing_trace_rows": _load_jsonl(run_dir / "workflow_timing_trace.jsonl"),
        "io_log_rows": _load_csv_dicts(run_dir / "io_log.csv"),
        "sample_rows": _load_csv_dicts(run_dir / "samples.csv"),
        "run_log_tail": run_log_text,
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


def _trace_name_for_jsonl(path: str | Path | None) -> str:
    if path is None:
        return ""
    filename = Path(path).name
    if filename.endswith(".jsonl"):
        return filename[:-6]
    return Path(path).stem


def _jsonl_dump(path: Path, rows: list[Mapping[str, Any]], *, trace_name: str | None = None) -> dict[str, Any]:
    return write_guarded_jsonl(path, rows, trace_name=trace_name or _trace_name_for_jsonl(path))


def _load_jsonl(path: str | Path | None, *, trace_name: str | None = None) -> list[dict[str, Any]]:
    if path is None:
        return []
    target = Path(path)
    if not target.exists():
        return []
    return load_guarded_jsonl(target, trace_name=trace_name or _trace_name_for_jsonl(target))


def _merge_trace_guard_stats(
    write_stats: Mapping[str, Mapping[str, Any]],
    loaded_rows: list[Mapping[str, Any]],
) -> dict[str, Any]:
    row_stats = summarize_trace_guard_rows(loaded_rows)
    return {
        "trace_guard_applied_to_route_trace": True,
        "trace_guard_applied_to_pressure_trace": True,
        "trace_event_truncated_count": int(row_stats["trace_event_truncated_count"])
        + sum(int(stats.get("trace_event_truncated_count") or 0) for stats in write_stats.values()),
        "trace_event_max_original_size_bytes": max(
            [int(row_stats["trace_event_max_original_size_bytes"] or 0)]
            + [int(stats.get("trace_event_max_original_size_bytes") or 0) for stats in write_stats.values()]
        ),
        "trace_event_max_truncated_size_bytes": max(
            [int(row_stats["trace_event_max_truncated_size_bytes"] or 0)]
            + [int(stats.get("trace_event_max_truncated_size_bytes") or 0) for stats in write_stats.values()]
        ),
        "trace_large_line_warning_count": int(row_stats["trace_large_line_warning_count"])
        + sum(int(stats.get("trace_large_line_warning_count") or 0) for stats in write_stats.values()),
        "trace_file_size_guard_triggered": bool(row_stats["trace_file_size_guard_triggered"])
        or any(stats.get("trace_file_size_guard_triggered") is True for stats in write_stats.values()),
        "trace_streaming_read_used": bool(row_stats["trace_streaming_read_used"]),
        "trace_inline_load_blocked": bool(row_stats["trace_inline_load_blocked"])
        or any(stats.get("trace_inline_load_blocked") is True for stats in write_stats.values()),
    }


def _load_csv_dicts(path: str | Path | None) -> list[dict[str, Any]]:
    if path is None:
        return []
    target = Path(path)
    if not target.exists():
        return []
    with target.open("r", encoding="utf-8", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _pressure_key(value: Any) -> str:
    parsed = _as_float(value)
    if parsed is None:
        return str(value or "")
    return str(int(parsed)) if abs(parsed - int(parsed)) <= 1e-6 else f"{parsed:.6g}"


def _point_tag_pressure(point_tag: Any) -> Optional[float]:
    match = re.search(r"_(\d+(?:\.\d+)?)hpa\b", str(point_tag or ""), flags=re.IGNORECASE)
    if not match:
        return None
    return float(match.group(1))


def _row_pressure(row: Mapping[str, Any]) -> Optional[float]:
    for key in ("target_pressure_hpa", "pressure_hpa"):
        parsed = _as_float(row.get(key))
        if parsed is not None:
            return float(parsed)
    actual = row.get("actual")
    if isinstance(actual, Mapping):
        for key in (
            "target_pressure_hpa",
            "pressure_hpa",
            "sealed_pressure_hpa",
            "preseal_trigger_pressure_hpa",
        ):
            parsed = _as_float(actual.get(key))
            if parsed is not None:
                return float(parsed)
    return _point_tag_pressure(row.get("point_tag"))


def _route_rows_for_pressure(route_rows: list[dict[str, Any]], pressure: float, index: int) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for row in route_rows:
        row_index = row.get("point_index")
        parsed_pressure = _row_pressure(row)
        if row_index is not None and int(float(row_index)) == int(index):
            out.append(row)
        elif parsed_pressure is not None and abs(parsed_pressure - float(pressure)) <= 1e-6:
            out.append(row)
    return out


def _extract_actual(row: Mapping[str, Any]) -> dict[str, Any]:
    actual = row.get("actual")
    return dict(actual) if isinstance(actual, Mapping) else {}


def _extract_pressure_age_ms(actual: Mapping[str, Any]) -> Optional[float]:
    for key in ("pressure_age_ms_before_sample", "pressure_sample_age_ms"):
        parsed = _as_float(actual.get(key))
        if parsed is not None:
            return float(parsed)
    for key in ("pressure_sample_age_s", "sample_age_s", "digital_gauge_age_s"):
        parsed = _as_float(actual.get(key))
        if parsed is not None:
            return float(parsed) * 1000.0
    return None


def _extract_pressure_hpa(actual: Mapping[str, Any]) -> Optional[float]:
    for key in (
        "pressure_gauge_hpa_before_sample",
        "pressure_gauge_hpa_before_ready",
        "digital_gauge_pressure_hpa",
        "pressure_hpa",
        "sealed_pressure_hpa",
        "preseal_trigger_pressure_hpa",
    ):
        parsed = _as_float(actual.get(key))
        if parsed is not None:
            return float(parsed)
    evidence = actual.get("pressure_evidence")
    if isinstance(evidence, Mapping):
        return _extract_pressure_hpa(evidence)
    return None


def _first_metric_from_rows(rows: list[dict[str, Any]], *keys: str) -> Any:
    for row in reversed(rows):
        for source in (row.get("actual"), row.get("route_state"), row):
            if not isinstance(source, Mapping):
                continue
            for key in keys:
                if key in source:
                    return source.get(key)
    return None


def _pre_route_atmosphere_pressure_from_route_rows(route_rows: list[dict[str, Any]]) -> dict[str, Any]:
    for row in route_rows:
        if not isinstance(row, Mapping):
            continue
        action = str(row.get("action") or "").strip()
        if action != "set_vent":
            continue
        target = row.get("target")
        actual = row.get("actual")
        if not isinstance(actual, Mapping):
            continue
        target_vent_on = _as_bool(target.get("vent_on")) if isinstance(target, Mapping) else None
        message = str(row.get("message") or "").lower()
        route_phase = str(actual.get("route_conditioning_phase") or "").lower()
        before_route_context = bool(
            target_vent_on is True
            and (
                "before co2 route conditioning" in message
                or "before_route" in route_phase
                or actual.get("route_open_completed_monotonic_s") in (None, "")
            )
        )
        atmosphere_ready = _as_bool(actual.get("atmosphere_ready"))
        pressure_hpa = _as_float(actual.get("pressure_hpa"))
        if before_route_context and pressure_hpa is not None and atmosphere_ready is not False:
            age_s = _as_float(
                actual.get("pressure_sample_age_s")
                or actual.get("selected_pressure_sample_age_s")
                or actual.get("sample_age_s")
            )
            return {
                "pressure_hpa": round(float(pressure_hpa), 3),
                "source": "route_trace_pre_route_vent_pressure",
                "sample_age_s": 0.0 if age_s is None else round(float(age_s), 3),
            }
    return {"pressure_hpa": None, "source": "", "sample_age_s": None}


def _a2_3_pressure_source_strategy(raw_cfg: Mapping[str, Any]) -> str:
    value = str(
        _first_value(
            raw_cfg,
            (
                "workflow.pressure.a2_conditioning_pressure_source",
                "workflow.pressure.conditioning_pressure_source",
                "a2_co2_7_pressure_no_write_probe.workflow.pressure.a2_conditioning_pressure_source",
                "run001_a2.workflow.pressure.a2_conditioning_pressure_source",
            ),
        )
        or "continuous"
    ).strip().lower()
    aliases = {
        "p3": "p3_fast_poll",
        "p3_fast": "p3_fast_poll",
        "fast_poll": "p3_fast_poll",
        "continuous_stream": "continuous",
        "v1": "v1_aligned",
        "v1_aligned_p3": "v1_aligned",
    }
    value = aliases.get(value, value)
    return value if value in {"v1_aligned", "continuous", "p3_fast_poll", "auto"} else "continuous"


def _pressure_source_strategy_from_config_path(path: str | Path | None) -> str:
    if not path:
        return ""
    try:
        cfg = load_json_mapping(Path(path))
    except Exception:
        return ""
    return _a2_3_pressure_source_strategy(cfg)


def _pressure_gate_source_is_v1_aligned(source: Any) -> bool:
    text = str(source or "").strip().lower()
    if not text:
        return False
    if text in {"v1_aligned", "digital_pressure_gauge_p3", "digital_pressure_gauge_p3_fast_poll"}:
        return True
    return bool("digital_pressure_gauge" in text and "p3" in text)


def _sample_count_by_pressure(sample_rows: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in sample_rows:
        pressure = _as_float(row.get("target_pressure_hpa", row.get("pressure_hpa")))
        if pressure is None:
            continue
        key = _pressure_key(pressure)
        counts[key] = counts.get(key, 0) + 1
    return counts


def _coerce_point_result(row: Mapping[str, Any], raw_cfg: Mapping[str, Any]) -> dict[str, Any]:
    pressure = float(row.get("target_pressure_hpa"))
    index = int(row.get("pressure_point_index", row.get("point_index", 0)) or 0)
    sample_count = int(row.get("sample_count", 0) or 0)
    valid_count = int(row.get("valid_frame_count", sample_count) or 0)
    pressure_age = row.get("pressure_age_ms_before_sample")
    if pressure_age is not None:
        pressure_age = float(pressure_age)
    fresh = _as_bool(row.get("pressure_gauge_freshness_ok_before_sample")) is True
    point_completed = _as_bool(row.get("point_completed")) is True
    pressure_ready = str(row.get("pressure_ready_gate_result") or "").strip().upper()
    if not pressure_ready:
        pressure_ready = "PASS" if point_completed else "FAIL_CLOSED"
    final = str(row.get("point_final_decision") or "").strip().upper()
    if not final:
        final = "PASS" if point_completed else "FAIL_CLOSED"
    return {
        "target_pressure_hpa": pressure,
        "pressure_point_index": index,
        "pressure_setpoint_command_sent": _as_bool(row.get("pressure_setpoint_command_sent")) is True,
        "pressure_setpoint_scope": str(row.get("pressure_setpoint_scope") or ""),
        "vent_state_before_point": row.get("vent_state_before_point"),
        "seal_state_before_point": row.get("seal_state_before_point"),
        "pressure_gauge_hpa_before_ready": row.get("pressure_gauge_hpa_before_ready"),
        "pressure_gauge_hpa_before_sample": row.get("pressure_gauge_hpa_before_sample"),
        "pressure_age_ms_before_sample": pressure_age,
        "pressure_gauge_freshness_ok_before_sample": fresh,
        "pressure_ready_gate_result": pressure_ready,
        "pressure_ready_gate_latency_ms": row.get("pressure_ready_gate_latency_ms"),
        "heartbeat_ready_before_sample": _as_bool(row.get("heartbeat_ready_before_sample")) is True,
        "heartbeat_gap_observed_ms": row.get("heartbeat_gap_observed_ms"),
        "heartbeat_emission_gap_ms": row.get("heartbeat_emission_gap_ms"),
        "blocking_operation_duration_ms": row.get("blocking_operation_duration_ms"),
        "route_conditioning_ready_before_sample": _as_bool(row.get("route_conditioning_ready_before_sample")) is True,
        "pressure_source_selected": row.get("pressure_source_selected"),
        "pressure_source_selection_reason": row.get("pressure_source_selection_reason"),
        "selected_pressure_source": row.get("selected_pressure_source"),
        "selected_pressure_sample_age_s": row.get("selected_pressure_sample_age_s"),
        "selected_pressure_sample_is_stale": _as_bool(row.get("selected_pressure_sample_is_stale")) is True,
        "selected_pressure_parse_ok": _as_bool(row.get("selected_pressure_parse_ok")) is True,
        "selected_pressure_freshness_ok": _as_bool(row.get("selected_pressure_freshness_ok")) is True,
        "pressure_freshness_decision_source": row.get("pressure_freshness_decision_source"),
        "selected_pressure_fail_closed_reason": row.get("selected_pressure_fail_closed_reason") or "",
        "continuous_stream_stale": _as_bool(row.get("continuous_stream_stale")) is True,
        "sample_count": sample_count,
        "valid_frame_count": valid_count,
        "frame_has_data": _as_bool(row.get("frame_has_data")) is True,
        "frame_usable": _as_bool(row.get("frame_usable")) is True,
        "frame_status": str(row.get("frame_status") or ""),
        "analyzer_ids_seen": list(row.get("analyzer_ids_seen") or []),
        "point_completed": point_completed,
        "point_final_decision": final,
    }


def _point_results_from_execution(
    execution: Mapping[str, Any],
    raw_cfg: Mapping[str, Any],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    explicit = execution.get("point_results")
    if isinstance(explicit, list):
        point_results = [_coerce_point_result(row, raw_cfg) for row in explicit if isinstance(row, Mapping)]
        return (
            point_results,
            [dict(row) for row in execution.get("route_trace_rows") or [] if isinstance(row, Mapping)],
            [dict(row) for row in execution.get("pressure_trace_rows") or [] if isinstance(row, Mapping)],
            [dict(row) for row in execution.get("sample_rows") or [] if isinstance(row, Mapping)],
        )

    run_dir = execution.get("execution_run_dir")
    route_rows = [dict(row) for row in execution.get("route_trace_rows") or [] if isinstance(row, Mapping)]
    if not route_rows and run_dir:
        route_rows = _load_jsonl(Path(str(run_dir)) / "route_trace.jsonl")
    timing_rows = [dict(row) for row in execution.get("timing_trace_rows") or [] if isinstance(row, Mapping)]
    if not timing_rows and run_dir:
        timing_rows = _load_jsonl(Path(str(run_dir)) / "workflow_timing_trace.jsonl")
    sample_rows = [dict(row) for row in execution.get("sample_rows") or [] if isinstance(row, Mapping)]
    if not sample_rows and run_dir:
        sample_rows = _load_csv_dicts(Path(str(run_dir)) / "samples.csv")
    service_summary = dict(execution.get("service_summary") or {})
    completed = {
        float(item)
        for item in _float_list(service_summary.get("planned_pressure_points_completed", []))
    }
    sample_counts = _sample_count_by_pressure(sample_rows)
    freshness_max_age_ms = _pressure_freshness_max_age_ms(raw_cfg)

    point_results: list[dict[str, Any]] = []
    pressure_trace_rows: list[dict[str, Any]] = []
    for index, pressure in enumerate(A2_ALLOWED_PRESSURE_POINTS_HPA, start=1):
        rows = _route_rows_for_pressure(route_rows, pressure, index)
        timing_for_point = _route_rows_for_pressure(timing_rows, pressure, index)
        metric_rows = rows + timing_for_point
        ready_row = next((row for row in rows if row.get("action") == "pressure_control_ready_gate"), {})
        ready_actual = _extract_actual(ready_row)
        sample_count = sample_counts.get(_pressure_key(pressure), 0)
        pressure_age_ms = _extract_pressure_age_ms(ready_actual)
        pressure_hpa = _extract_pressure_hpa(ready_actual)
        pressure_ready = (
            "PASS"
            if str(ready_actual.get("gate_decision") or ready_actual.get("control_ready_status") or "").lower()
            in {"ready", "pass", "ok"}
            or ready_row.get("result") == "ok"
            else "FAIL_CLOSED"
        )
        fresh = bool(pressure_age_ms is not None and pressure_age_ms <= freshness_max_age_ms)
        point_completed = pressure in completed or sample_count > 0
        setpoint_sent = any(str(row.get("action") or "") == "set_pressure" for row in rows)
        heartbeat_ready = any(str(row.get("action") or "") == "set_vent" and row.get("result") == "ok" for row in rows)
        route_ready = any(str(row.get("action") or "") in {"set_co2_valves", "route_baseline"} and row.get("result") == "ok" for row in rows)
        result = {
            "target_pressure_hpa": pressure,
            "pressure_point_index": index,
            "pressure_setpoint_command_sent": bool(setpoint_sent),
            "pressure_setpoint_scope": "authorized_a2_pressure_control_scope" if setpoint_sent else "",
            "vent_state_before_point": ready_actual.get("vent_status_raw", ready_actual.get("pressure_controller_vent_status")),
            "seal_state_before_point": ready_actual.get("seal_transition_status"),
            "pressure_gauge_hpa_before_ready": pressure_hpa,
            "pressure_gauge_hpa_before_sample": pressure_hpa,
            "pressure_age_ms_before_sample": pressure_age_ms,
            "pressure_gauge_freshness_ok_before_sample": fresh,
            "pressure_ready_gate_result": pressure_ready,
            "pressure_ready_gate_latency_ms": None,
            "heartbeat_ready_before_sample": heartbeat_ready,
            "heartbeat_gap_observed_ms": _first_metric_from_rows(
                metric_rows,
                "heartbeat_gap_observed_ms",
                "vent_heartbeat_gap_ms",
            ),
            "heartbeat_emission_gap_ms": _first_metric_from_rows(metric_rows, "heartbeat_emission_gap_ms"),
            "blocking_operation_duration_ms": _first_metric_from_rows(
                metric_rows,
                "blocking_operation_duration_ms",
            ),
            "route_conditioning_ready_before_sample": route_ready,
            "pressure_source_selected": _first_metric_from_rows(metric_rows, "pressure_source_selected"),
            "pressure_source_selection_reason": _first_metric_from_rows(
                metric_rows,
                "pressure_source_selection_reason",
                "source_selection_reason",
            ),
            "selected_pressure_source": _first_metric_from_rows(metric_rows, "selected_pressure_source"),
            "selected_pressure_sample_age_s": _first_metric_from_rows(
                metric_rows,
                "selected_pressure_sample_age_s",
            ),
            "selected_pressure_sample_is_stale": _as_bool(
                _first_metric_from_rows(metric_rows, "selected_pressure_sample_is_stale")
            )
            is True,
            "selected_pressure_parse_ok": _as_bool(
                _first_metric_from_rows(metric_rows, "selected_pressure_parse_ok")
            )
            is True,
            "selected_pressure_freshness_ok": _as_bool(
                _first_metric_from_rows(metric_rows, "selected_pressure_freshness_ok")
            )
            is True,
            "pressure_freshness_decision_source": _first_metric_from_rows(
                metric_rows,
                "pressure_freshness_decision_source",
            ),
            "selected_pressure_fail_closed_reason": _first_metric_from_rows(
                metric_rows,
                "selected_pressure_fail_closed_reason",
            )
            or "",
            "continuous_stream_stale": _as_bool(
                _first_metric_from_rows(metric_rows, "continuous_stream_stale")
            )
            is True,
            "sample_count": int(sample_count),
            "valid_frame_count": int(sample_count),
            "frame_has_data": sample_count > 0,
            "frame_usable": sample_count > 0,
            "frame_status": "frames_seen" if sample_count > 0 else "no_frame_seen",
            "analyzer_ids_seen": sorted({str(row.get("device_id") or row.get("id") or "") for row in sample_rows if row.get("device_id") or row.get("id")}),
            "point_completed": bool(point_completed),
            "point_final_decision": "PASS" if point_completed else "FAIL_CLOSED",
        }
        point_results.append(result)
        pressure_trace_rows.extend(rows)
    return point_results, route_rows, pressure_trace_rows, sample_rows


def _downstream_after_failure(point_results: list[Mapping[str, Any]]) -> bool:
    seen_failure = False
    for point in point_results:
        final = str(point.get("point_final_decision") or "").upper()
        if seen_failure and (
            _as_bool(point.get("pressure_setpoint_command_sent")) is True
            or int(point.get("sample_count") or 0) > 0
            or _as_bool(point.get("point_completed")) is True
        ):
            return True
        if final != "PASS":
            seen_failure = True
    return False


def _write_point_results_csv(path: Path, point_results: list[Mapping[str, Any]]) -> None:
    fields = [
        "pressure_point_index",
        "target_pressure_hpa",
        "pressure_ready_gate_result",
        "pressure_gauge_freshness_ok_before_sample",
        "pressure_age_ms_before_sample",
        "heartbeat_gap_observed_ms",
        "heartbeat_emission_gap_ms",
        "blocking_operation_duration_ms",
        "pressure_source_selected",
        "pressure_source_selection_reason",
        "selected_pressure_source",
        "selected_pressure_sample_age_s",
        "selected_pressure_sample_is_stale",
        "selected_pressure_parse_ok",
        "selected_pressure_freshness_ok",
        "pressure_freshness_decision_source",
        "selected_pressure_fail_closed_reason",
        "selected_pressure_sample_stale_duration_ms",
        "selected_pressure_sample_stale_budget_ms",
        "selected_pressure_sample_stale_budget_exceeded",
        "selected_pressure_sample_stale_deferred_for_vent_priority",
        "continuous_latest_fresh_fast_path_used",
        "continuous_latest_fresh_duration_ms",
        "continuous_latest_fresh_budget_exceeded",
        "continuous_stream_stale",
        "sample_count",
        "valid_frame_count",
        "point_completed",
        "point_final_decision",
    ]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for row in point_results:
            writer.writerow({field: row.get(field) for field in fields})


def write_a2_co2_7_pressure_no_write_probe_artifacts(
    raw_cfg: Mapping[str, Any],
    *,
    output_dir: Optional[str | Path] = None,
    config_path: str | Path = "",
    operator_confirmation_path: Optional[str | Path] = None,
    branch: str = "",
    head: str = "",
    cli_allow: bool = False,
    env: Optional[Mapping[str, str]] = None,
    execute_probe: bool = False,
    run_app_py_untouched: bool = True,
    executor: Optional[Callable[[str | Path], Mapping[str, Any]]] = None,
) -> dict[str, Any]:
    admission = evaluate_a2_co2_7_pressure_no_write_gate(
        raw_cfg,
        cli_allow=cli_allow,
        env=env,
        operator_confirmation_path=operator_confirmation_path,
        branch=branch,
        head=head,
        config_path=str(config_path or ""),
        run_app_py_untouched=run_app_py_untouched,
    )
    run_dir = Path(output_dir).expanduser().resolve() if output_dir else _default_output_dir()
    run_dir.mkdir(parents=True, exist_ok=True)
    artifact_paths = {
        "probe_admission_record": str(run_dir / "probe_admission_record.json"),
        "summary": str(run_dir / "summary.json"),
        "a2_pressure_sweep_trace": str(run_dir / "a2_pressure_sweep_trace.jsonl"),
        "route_trace": str(run_dir / "route_trace.jsonl"),
        "pressure_trace": str(run_dir / "pressure_trace.jsonl"),
        "pressure_ready_trace": str(run_dir / "pressure_ready_trace.jsonl"),
        "heartbeat_trace": str(run_dir / "heartbeat_trace.jsonl"),
        "analyzer_sampling_rows": str(run_dir / "analyzer_sampling_rows.jsonl"),
        "point_results": str(run_dir / "point_results.json"),
        "point_results_csv": str(run_dir / "point_results.csv"),
        "safety_assertions": str(run_dir / "safety_assertions.json"),
        "operator_confirmation_record": str(run_dir / "operator_confirmation_record.json"),
        "process_exit_record": str(run_dir / "process_exit_record.json"),
    }

    rejection_reasons = list(admission.reasons)
    executed = bool(admission.approved and execute_probe)
    execution_started = False
    execution_interrupted = False
    interrupted_at = ""
    interruption_source = ""
    interruption_stage = ""
    execution_error = ""
    execution: Mapping[str, Any] = {}
    points_alignment: dict[str, Any] = {
        "points_config_alignment_ready": False,
        "generated_points_json_path": "",
        "generated_points_json_sha256": "",
        "effective_points_json_path": "",
        "effective_points_json_sha256": "",
        "downstream_aligned_config_path": "",
        "downstream_points_row_count": 0,
        "downstream_point_routes": [],
        "downstream_point_pressures_hpa": [],
        "downstream_points_gate_reasons": [],
        "downstream_points_generated": False,
        **_a2_21_pressure_port_alignment_metadata(),
    }
    execution_config_path = str(config_path)
    if admission.approved:
        _write_interrupted_guard_artifacts(
            run_dir,
            admission=admission,
            artifact_paths=artifact_paths,
            operator_confirmation_path=operator_confirmation_path,
            config_path=config_path,
            branch=branch,
            head=head,
            cli_allow=cli_allow,
            execute_probe=execute_probe,
            run_app_py_untouched=run_app_py_untouched,
            interruption_source="pre_com_guard",
            interruption_stage="admission_output_dir_created",
            real_probe_executed=False,
            real_com_opened=False,
            any_device_command_sent=False,
            any_write_command_sent=False,
            no_write_assertion_status="pass_pre_com",
            safe_stop_triggered=False,
            device_command_audit_complete=True,
            must_not_claim_no_write_pass=False,
        )
    if not execute_probe:
        rejection_reasons.append("execute_probe_not_requested")
    elif not admission.approved:
        rejection_reasons.append("admission_not_approved")
    else:
        try:
            aligned_config_path, points_alignment = prepare_a2_downstream_points_config(
                raw_cfg,
                config_path=config_path,
                output_dir=run_dir,
            )
            execution_config_path = str(aligned_config_path)
        except Exception as exc:
            execution_error = str(exc)
            rejection_reasons.append(f"points_config_alignment_error:{exc}")
        else:
            _write_interrupted_guard_artifacts(
                run_dir,
                admission=admission,
                artifact_paths=artifact_paths,
                operator_confirmation_path=operator_confirmation_path,
                config_path=config_path,
                branch=branch,
                head=head,
                cli_allow=cli_allow,
                execute_probe=execute_probe,
                run_app_py_untouched=run_app_py_untouched,
                interruption_source="downstream_executor_started",
                interruption_stage="downstream_executor_started_real_com_status_unknown",
                real_probe_executed=True,
                real_com_opened="unknown",
                any_device_command_sent="unknown",
                any_write_command_sent="unknown",
                no_write_assertion_status="unknown",
                safe_stop_triggered="unknown",
                device_command_audit_complete=False,
                must_not_claim_no_write_pass=True,
            )
            execution_started = True
            try:
                execution = (executor or execute_existing_v2_a2_pressure_sweep)(execution_config_path)
            except KeyboardInterrupt as exc:
                interrupted_at = _now()
                interruption_source = "KeyboardInterrupt"
                interruption_stage = "downstream_executor_keyboard_interrupt"
                execution_interrupted = True
                execution_error = "KeyboardInterrupt"
                partial = getattr(exc, "partial_execution", None)
                execution = dict(partial) if isinstance(partial, Mapping) else {}
                rejection_reasons.append(A2_INTERRUPTED_FAIL_CLOSED_REASON)
            except TimeoutError as exc:
                interrupted_at = _now()
                interruption_source = "TimeoutError"
                interruption_stage = "downstream_executor_timeout"
                execution_interrupted = True
                execution_error = str(exc)
                partial = getattr(exc, "partial_execution", None)
                execution = dict(partial) if isinstance(partial, Mapping) else {}
                rejection_reasons.append(A2_INTERRUPTED_FAIL_CLOSED_REASON)
            except Exception as exc:
                interrupted_at = _now()
                interruption_source = exc.__class__.__name__
                interruption_stage = "downstream_executor_exception"
                execution_interrupted = True
                execution_error = str(exc)
                partial = getattr(exc, "partial_execution", None)
                execution = dict(partial) if isinstance(partial, Mapping) else {}
                rejection_reasons.append(A2_INTERRUPTED_FAIL_CLOSED_REASON)
                rejection_reasons.append(f"execution_error:{exc}")
            if isinstance(execution, Mapping) and isinstance(execution.get("points_alignment"), Mapping):
                points_alignment.update(dict(execution.get("points_alignment") or {}))
            elif isinstance(execution, Mapping):
                execution = {**dict(execution), "points_alignment": points_alignment}
            else:
                execution = {"points_alignment": points_alignment}

    point_results, route_rows, pressure_rows, sample_rows = _point_results_from_execution(execution, raw_cfg)
    if not point_results:
        point_results = [
            {
                "target_pressure_hpa": pressure,
                "pressure_point_index": index,
                "pressure_setpoint_command_sent": False,
                "pressure_setpoint_scope": "",
                "vent_state_before_point": None,
                "seal_state_before_point": None,
                "pressure_gauge_hpa_before_ready": None,
                "pressure_gauge_hpa_before_sample": None,
                "pressure_age_ms_before_sample": None,
                "pressure_gauge_freshness_ok_before_sample": False,
                "pressure_ready_gate_result": "FAIL_CLOSED",
                "pressure_ready_gate_latency_ms": None,
                "heartbeat_ready_before_sample": False,
                "heartbeat_gap_observed_ms": None,
                "heartbeat_emission_gap_ms": None,
                "blocking_operation_duration_ms": None,
                "route_conditioning_ready_before_sample": False,
                "sample_count": 0,
                "valid_frame_count": 0,
                "frame_has_data": False,
                "frame_usable": False,
                "frame_status": "not_executed",
                "analyzer_ids_seen": [],
                "point_completed": False,
                "point_final_decision": "FAIL_CLOSED",
            }
            for index, pressure in enumerate(A2_ALLOWED_PRESSURE_POINTS_HPA, start=1)
        ]

    point_results = sorted(point_results, key=lambda row: int(row.get("pressure_point_index") or 0))
    sample_count_by_pressure = {
        _pressure_key(point["target_pressure_hpa"]): int(point.get("sample_count") or 0)
        for point in point_results
    }
    sample_min_count = _sample_min_count(raw_cfg)
    pressure_points_completed = sum(1 for point in point_results if _as_bool(point.get("point_completed")) is True)
    sample_count_total = sum(int(point.get("sample_count") or 0) for point in point_results)
    all_have_fresh = all(_as_bool(point.get("pressure_gauge_freshness_ok_before_sample")) is True for point in point_results)
    all_have_samples = all(int(point.get("sample_count") or 0) >= sample_min_count for point in point_results)
    all_completed = pressure_points_completed == len(A2_ALLOWED_PRESSURE_POINTS_HPA)
    all_ready = all(str(point.get("pressure_ready_gate_result") or "").upper() == "PASS" for point in point_results)
    all_heartbeat = all(_as_bool(point.get("heartbeat_ready_before_sample")) is True for point in point_results)
    all_route = all(_as_bool(point.get("route_conditioning_ready_before_sample")) is True for point in point_results)
    downstream_after_failure = _downstream_after_failure(point_results)
    if downstream_after_failure:
        rejection_reasons.append("downstream_point_executed_after_fail_closed_point")

    service_summary = dict(execution.get("service_summary") or {}) if isinstance(execution, Mapping) else {}

    def service_summary_value(*keys: str) -> Any:
        stats = service_summary.get("stats")
        for key in keys:
            if key in service_summary:
                return service_summary.get(key)
        if isinstance(stats, Mapping):
            for key in keys:
                if key in stats:
                    return stats.get(key)
        return None

    execution_audit: dict[str, Any] = {}
    if isinstance(execution, Mapping):
        for audit_key in ("interruption_audit", "command_audit", "safety_audit"):
            audit_payload = execution.get(audit_key)
            if isinstance(audit_payload, Mapping):
                execution_audit.update(dict(audit_payload))
        for audit_key in (
            "real_com_opened",
            "any_device_command_sent",
            "any_write_command_sent",
            "device_command_audit_complete",
            "safe_stop_triggered",
        ):
            if audit_key in execution:
                execution_audit[audit_key] = execution.get(audit_key)

    def audit_value(*keys: str) -> Any:
        for key in keys:
            if key in execution_audit:
                return execution_audit.get(key)
        return service_summary_value(*keys)

    real_com_opened_value: Any = audit_value("real_com_opened")
    parsed_real_com_opened = _as_bool(real_com_opened_value)
    if parsed_real_com_opened is not None:
        real_com_opened_value = parsed_real_com_opened
    elif not execution_started:
        real_com_opened_value = False
    elif execution_interrupted:
        real_com_opened_value = "unknown"
    else:
        real_com_opened_value = bool(route_rows or pressure_rows or sample_rows)

    any_device_command_sent_value: Any = audit_value("any_device_command_sent")
    parsed_device_command_sent = _as_bool(any_device_command_sent_value)
    if parsed_device_command_sent is not None:
        any_device_command_sent_value = parsed_device_command_sent
    elif not execution_started:
        any_device_command_sent_value = False
    elif execution_interrupted:
        any_device_command_sent_value = "unknown"
    else:
        any_device_command_sent_value = bool(
            route_rows
            or any(_as_bool(point.get("pressure_setpoint_command_sent")) is True for point in point_results)
        )

    safe_stop_triggered_value: Any = audit_value(
        "safe_stop_triggered",
        "final_safe_stop_triggered",
        "final_safe_stop_chamber_stop_attempted",
    )
    parsed_safe_stop_triggered = _as_bool(safe_stop_triggered_value)
    if parsed_safe_stop_triggered is not None:
        safe_stop_triggered_value = parsed_safe_stop_triggered
    elif not execution_started:
        safe_stop_triggered_value = False
    elif execution_interrupted:
        safe_stop_triggered_value = "unknown"
    else:
        safe_stop_triggered_value = False

    device_command_audit_complete_value = _as_bool(audit_value("device_command_audit_complete"))
    if device_command_audit_complete_value is None:
        device_command_audit_complete_value = not execution_interrupted

    no_write_ok = True
    safety_assertions = {
        **A2_EVIDENCE_MARKERS,
        **A2_SAFETY_ASSERTION_DEFAULTS,
        "authorized_pressure_control_scope": "A2 CO2-only 7-pressure transient pressure readiness/control only",
        "pressure_setpoint_command_sent": any(
            _as_bool(point.get("pressure_setpoint_command_sent")) is True for point in point_results
        ),
        "pressure_setpoint_scope": "authorized_a2_pressure_control_scope",
        "vent_off_command_sent": any(str(row.get("action") or "") == "set_vent" and _as_bool((row.get("target") or {}).get("vent_on")) is False for row in route_rows if isinstance(row.get("target"), Mapping)),
        "vent_off_command_scope": "authorized_a2_pressure_control_scope",
        "seal_command_sent": any(str(row.get("action") or "") in {"seal_route", "seal_transition"} for row in route_rows),
        "seal_command_scope": "authorized_a2_pressure_control_scope",
        "chamber_stop_command_sent": _as_bool(
            service_summary_value("chamber_stop_command_sent")
            or service_summary_value("final_safe_stop_chamber_stop_command_sent")
        )
        is True,
        "final_safe_stop_warning_count": int(service_summary_value("final_safe_stop_warning_count") or 0),
        "final_safe_stop_warnings": list(service_summary_value("final_safe_stop_warnings") or []),
        "final_safe_stop_chamber_stop_warning": service_summary_value(
            "final_safe_stop_chamber_stop_warning",
        )
        or "",
        "final_safe_stop_chamber_stop_attempted": _as_bool(
            service_summary_value("final_safe_stop_chamber_stop_attempted")
        )
        is True,
        "final_safe_stop_chamber_stop_command_sent": _as_bool(
            service_summary_value("final_safe_stop_chamber_stop_command_sent")
        )
        is True,
        "final_safe_stop_chamber_stop_result": service_summary_value(
            "final_safe_stop_chamber_stop_result",
        )
        or "not_observed",
        "final_safe_stop_chamber_stop_blocked_by_no_write": _as_bool(
            service_summary_value("final_safe_stop_chamber_stop_blocked_by_no_write")
        )
        is True,
        "high_pressure_1100_hpa_prearm_recorded": any(
            str(row.get("action") or "") == "high_pressure_first_point_mode_enabled" for row in route_rows
        ),
        "sample_count_total": int(sample_count_total),
        "pressure_points_completed": int(pressure_points_completed),
        "no_write": no_write_ok,
    }
    restricted_write_command_sent = bool(
        _as_bool(safety_assertions.get("any_write_command_sent")) is True
        or _as_bool(safety_assertions.get("identity_write_command_sent")) is True
        or _as_bool(safety_assertions.get("mode_switch_command_sent")) is True
        or _as_bool(safety_assertions.get("senco_write_command_sent")) is True
        or _as_bool(safety_assertions.get("calibration_write_command_sent")) is True
        or _as_bool(safety_assertions.get("chamber_write_register_command_sent")) is True
        or _as_bool(safety_assertions.get("chamber_set_temperature_command_sent")) is True
        or _as_bool(safety_assertions.get("chamber_start_command_sent")) is True
        or _as_bool(safety_assertions.get("chamber_stop_command_sent")) is True
    )
    audited_any_write = audit_value("any_write_command_sent")
    parsed_audited_any_write = _as_bool(audited_any_write)
    if parsed_audited_any_write is not None:
        any_write_command_sent_value: Any = parsed_audited_any_write
        restricted_write_command_sent = bool(parsed_audited_any_write)
    elif execution_interrupted and not device_command_audit_complete_value:
        any_write_command_sent_value = "unknown"
    else:
        any_write_command_sent_value = restricted_write_command_sent

    if restricted_write_command_sent:
        no_write_assertion_status = "fail"
        must_not_claim_no_write_pass = False
    elif real_com_opened_value is False and any_device_command_sent_value is False:
        no_write_assertion_status = "pass_pre_com"
        must_not_claim_no_write_pass = False
    elif execution_interrupted and not device_command_audit_complete_value:
        no_write_assertion_status = "unknown"
        must_not_claim_no_write_pass = True
    elif execution_interrupted:
        no_write_assertion_status = "pass_partial_audited"
        must_not_claim_no_write_pass = False
    else:
        no_write_assertion_status = "pass"
        must_not_claim_no_write_pass = False
    no_write_ok = bool(
        int(safety_assertions.get("attempted_write_count") or 0) == 0
        and any_write_command_sent_value is not True
        and _as_bool(safety_assertions.get("identity_write_command_sent")) is not True
        and _as_bool(safety_assertions.get("mode_switch_command_sent")) is not True
        and _as_bool(safety_assertions.get("senco_write_command_sent")) is not True
        and _as_bool(safety_assertions.get("calibration_write_command_sent")) is not True
        and _as_bool(safety_assertions.get("chamber_write_register_command_sent")) is not True
        and _as_bool(safety_assertions.get("chamber_set_temperature_command_sent")) is not True
        and _as_bool(safety_assertions.get("chamber_start_command_sent")) is not True
        and _as_bool(safety_assertions.get("chamber_stop_command_sent")) is not True
        and no_write_assertion_status != "unknown"
    )
    safety_assertions.update(
        {
            "record_type": "a2_safety_assertions",
            "safety_assertions_complete": bool(device_command_audit_complete_value and not execution_interrupted),
            "real_com_opened": real_com_opened_value,
            "any_device_command_sent": any_device_command_sent_value,
            "any_write_command_sent": any_write_command_sent_value,
            "no_write": no_write_ok,
            "no_write_assertion_status": no_write_assertion_status,
            "device_command_audit_complete": bool(device_command_audit_complete_value),
            "must_not_claim_no_write_pass": bool(must_not_claim_no_write_pass),
            "safe_stop_triggered": safe_stop_triggered_value,
        }
    )

    evidence_metric_rows = route_rows + pressure_rows + sample_rows

    def metric_or_summary(*keys: str) -> Any:
        value = _first_metric_from_rows(evidence_metric_rows, *keys)
        if value is not None:
            return value
        for key in keys:
            value = service_summary_value(key)
            if value is not None:
                return value
        return None

    raw_config_pressure_source_strategy = _a2_3_pressure_source_strategy(raw_cfg)
    downstream_aligned_config_path = str(
        points_alignment.get("downstream_aligned_config_path") or execution_config_path or ""
    )
    downstream_aligned_pressure_source_strategy = _pressure_source_strategy_from_config_path(
        downstream_aligned_config_path
    )
    runtime_pressure_source_strategy_observed = _first_metric_from_rows(
        evidence_metric_rows,
        "a2_conditioning_pressure_source_strategy",
        "a2_conditioning_pressure_source",
    )
    runtime_pressure_source_strategy_observed = (
        str(runtime_pressure_source_strategy_observed).strip().lower()
        if runtime_pressure_source_strategy_observed not in (None, "")
        else ""
    )
    service_summary_pressure_source_strategy = str(
        service_summary_value("a2_conditioning_pressure_source_strategy")
        or service_summary_value("a2_conditioning_pressure_source")
        or ""
    ).strip().lower()
    if runtime_pressure_source_strategy_observed:
        a2_conditioning_pressure_source_strategy = runtime_pressure_source_strategy_observed
        a2_conditioning_pressure_source_strategy_source = "runtime_metric"
    elif downstream_aligned_pressure_source_strategy:
        a2_conditioning_pressure_source_strategy = downstream_aligned_pressure_source_strategy
        a2_conditioning_pressure_source_strategy_source = "downstream_aligned_config"
    elif service_summary_pressure_source_strategy:
        a2_conditioning_pressure_source_strategy = service_summary_pressure_source_strategy
        a2_conditioning_pressure_source_strategy_source = "service_summary"
    else:
        a2_conditioning_pressure_source_strategy = raw_config_pressure_source_strategy
        a2_conditioning_pressure_source_strategy_source = "raw_config"
    pressure_source_strategy_aggregation_mismatch = bool(
        downstream_aligned_pressure_source_strategy
        and a2_conditioning_pressure_source_strategy
        and downstream_aligned_pressure_source_strategy != a2_conditioning_pressure_source_strategy
    )
    pressure_source_strategy_aggregation_mismatch_reason = (
        "runtime_strategy_differs_from_downstream_aligned_config"
        if pressure_source_strategy_aggregation_mismatch
        and a2_conditioning_pressure_source_strategy_source == "runtime_metric"
        else (
            "summary_strategy_differs_from_downstream_aligned_config"
            if pressure_source_strategy_aggregation_mismatch
            else ""
        )
    )

    atmosphere_fallback = _pre_route_atmosphere_pressure_from_route_rows(route_rows)
    measured_atmospheric_pressure_hpa = _as_float(metric_or_summary("measured_atmospheric_pressure_hpa"))
    measured_atmospheric_pressure_source = str(
        metric_or_summary("measured_atmospheric_pressure_source") or ""
    ).strip()
    measured_atmospheric_pressure_sample_age_s = _as_float(
        metric_or_summary("measured_atmospheric_pressure_sample_age_s")
    )
    if measured_atmospheric_pressure_hpa is None and atmosphere_fallback.get("pressure_hpa") is not None:
        measured_atmospheric_pressure_hpa = _as_float(atmosphere_fallback.get("pressure_hpa"))
        measured_atmospheric_pressure_source = str(atmosphere_fallback.get("source") or "")
        measured_atmospheric_pressure_sample_age_s = _as_float(atmosphere_fallback.get("sample_age_s"))
    if measured_atmospheric_pressure_source == "" and measured_atmospheric_pressure_hpa is not None:
        measured_atmospheric_pressure_source = "route_conditioning_summary"
    route_conditioning_pressure_before_route_open_hpa = _as_float(
        metric_or_summary("route_conditioning_pressure_before_route_open_hpa")
    )
    if route_conditioning_pressure_before_route_open_hpa is None:
        route_conditioning_pressure_before_route_open_hpa = measured_atmospheric_pressure_hpa
    route_open_transient_recovery_target_hpa = _as_float(
        metric_or_summary("route_open_transient_recovery_target_hpa")
    )
    if route_open_transient_recovery_target_hpa is None:
        route_open_transient_recovery_target_hpa = measured_atmospheric_pressure_hpa

    route_conditioning_pressure_overlimit = _as_bool(
        metric_or_summary("route_conditioning_pressure_overlimit", "pressure_overlimit_seen")
    ) is True
    route_conditioning_hard_abort_exceeded = _as_bool(
        metric_or_summary("route_conditioning_hard_abort_exceeded")
    ) is True
    route_open_transient_recovery_required = _as_bool(
        metric_or_summary("route_open_transient_recovery_required")
    ) is True
    route_open_transient_accepted = _as_bool(metric_or_summary("route_open_transient_accepted")) is True
    route_open_transient_rejection_reason = str(
        metric_or_summary("route_open_transient_rejection_reason") or ""
    ).strip()
    raw_route_conditioning_vent_gap_exceeded = _as_bool(
        metric_or_summary(
            "route_conditioning_vent_gap_exceeded",
            "vent_heartbeat_gap_exceeded",
        )
    ) is True
    max_vent_pulse_gap_ms = _as_float(metric_or_summary("max_vent_pulse_gap_ms"))
    max_vent_pulse_write_gap_ms = _as_float(metric_or_summary("max_vent_pulse_write_gap_ms"))
    max_vent_pulse_write_gap_ms_including_terminal_gap = _as_float(
        metric_or_summary("max_vent_pulse_write_gap_ms_including_terminal_gap")
    )
    max_vent_pulse_gap_limit_ms = _as_float(metric_or_summary("max_vent_pulse_gap_limit_ms"))
    terminal_vent_write_age_ms_at_gap_gate = _as_float(
        metric_or_summary("terminal_vent_write_age_ms_at_gap_gate")
    )
    defer_reschedule_latency_ms = _as_float(
        metric_or_summary("defer_reschedule_latency_ms", "defer_to_next_vent_loop_ms")
    )
    defer_reschedule_latency_budget_ms = _as_float(
        metric_or_summary("defer_reschedule_latency_budget_ms")
    )
    if defer_reschedule_latency_budget_ms is None:
        defer_reschedule_latency_budget_ms = 200.0
    defer_reschedule_latency_exceeded = bool(
        _as_bool(metric_or_summary("defer_reschedule_latency_exceeded")) is True
        or (
            defer_reschedule_latency_ms is not None
            and float(defer_reschedule_latency_ms) > float(defer_reschedule_latency_budget_ms)
        )
    )
    defer_reschedule_latency_warning = bool(
        _as_bool(metric_or_summary("defer_reschedule_latency_warning")) is True
        or defer_reschedule_latency_exceeded
    )
    vent_gap_after_defer_ms = _as_float(metric_or_summary("vent_gap_after_defer_ms"))
    vent_gap_after_defer_threshold_ms = _as_float(metric_or_summary("vent_gap_after_defer_threshold_ms"))
    if vent_gap_after_defer_threshold_ms is None:
        vent_gap_after_defer_threshold_ms = max_vent_pulse_gap_limit_ms
    actual_vent_gap_candidates = [
        item
        for item in (
            max_vent_pulse_gap_ms,
            max_vent_pulse_write_gap_ms,
            max_vent_pulse_write_gap_ms_including_terminal_gap,
            terminal_vent_write_age_ms_at_gap_gate,
            vent_gap_after_defer_ms,
        )
        if item is not None
    ]
    actual_vent_gap_exceeded = bool(
        max_vent_pulse_gap_limit_ms is not None
        and any(float(item) > float(max_vent_pulse_gap_limit_ms) for item in actual_vent_gap_candidates)
    )
    if (
        max_vent_pulse_gap_ms is not None
        and max_vent_pulse_gap_limit_ms is not None
        and float(max_vent_pulse_gap_ms) > float(max_vent_pulse_gap_limit_ms)
    ):
        actual_vent_gap_exceeded = True
    if (
        max_vent_pulse_write_gap_ms is not None
        and max_vent_pulse_gap_limit_ms is not None
        and float(max_vent_pulse_write_gap_ms) > float(max_vent_pulse_gap_limit_ms)
    ):
        actual_vent_gap_exceeded = True
    if (
        max_vent_pulse_write_gap_ms_including_terminal_gap is not None
        and max_vent_pulse_gap_limit_ms is not None
        and float(max_vent_pulse_write_gap_ms_including_terminal_gap) > float(max_vent_pulse_gap_limit_ms)
    ):
        actual_vent_gap_exceeded = True
    raw_vent_gap_source = str(
        metric_or_summary("route_conditioning_vent_gap_exceeded_source", "terminal_gap_source") or ""
    ).strip()
    fast_vent_after_defer_sent_value = _as_bool(metric_or_summary("fast_vent_after_defer_sent")) is True
    false_defer_latency_gap = bool(
        raw_route_conditioning_vent_gap_exceeded
        and raw_vent_gap_source == "defer_path_no_reschedule"
        and max_vent_pulse_gap_limit_ms is not None
        and actual_vent_gap_candidates
        and not actual_vent_gap_exceeded
    )
    route_conditioning_vent_gap_exceeded = bool(
        actual_vent_gap_exceeded
        or (raw_route_conditioning_vent_gap_exceeded and not false_defer_latency_gap)
    )
    defer_reschedule_caused_vent_gap_exceeded = bool(
        _as_bool(metric_or_summary("defer_reschedule_caused_vent_gap_exceeded")) is True
        or (raw_vent_gap_source == "defer_path_no_reschedule" and actual_vent_gap_exceeded)
    )
    vent_gap_exceeded_after_defer = bool(
        _as_bool(metric_or_summary("vent_gap_exceeded_after_defer")) is True
        or defer_reschedule_caused_vent_gap_exceeded
    )
    terminal_gap_after_defer_value = bool(
        (_as_bool(metric_or_summary("terminal_gap_after_defer")) is True and actual_vent_gap_exceeded)
        or vent_gap_exceeded_after_defer
    )
    terminal_gap_after_defer_ms_value = (
        metric_or_summary("terminal_gap_after_defer_ms") if terminal_gap_after_defer_value else None
    )
    if terminal_gap_after_defer_ms_value is None and terminal_gap_after_defer_value:
        terminal_gap_after_defer_ms_value = vent_gap_after_defer_ms
    defer_returned_to_vent_loop_value = bool(
        _as_bool(metric_or_summary("defer_returned_to_vent_loop")) is True
        or (
            false_defer_latency_gap
            and defer_reschedule_latency_ms is not None
            and fast_vent_after_defer_sent_value
        )
    )
    defer_reschedule_completed_value = bool(
        _as_bool(metric_or_summary("defer_reschedule_completed")) is True
        or (
            false_defer_latency_gap
            and defer_reschedule_latency_ms is not None
            and not defer_reschedule_caused_vent_gap_exceeded
        )
    )
    route_conditioning_fast_vent_timeout = _as_bool(
        metric_or_summary("route_conditioning_fast_vent_command_timeout", "pre_route_fast_vent_timeout")
    ) is True
    route_conditioning_fast_vent_not_supported = _as_bool(
        metric_or_summary("route_conditioning_fast_vent_not_supported")
    ) is True
    route_conditioning_diagnostic_blocked = _as_bool(
        metric_or_summary("route_conditioning_diagnostic_blocked_vent_scheduler")
    ) is True
    route_open_transition_blocked = _as_bool(
        metric_or_summary("route_open_transition_blocked_vent_scheduler")
    ) is True
    route_open_settle_wait_blocked = _as_bool(
        metric_or_summary("route_open_settle_wait_blocked_vent_scheduler")
    ) is True
    route_conditioning_vent_command_failed = _as_bool(
        metric_or_summary("vent_command_failed_during_flush")
    ) is True or any(
        str(row.get("action") or "") == "co2_route_conditioning_vent_command_failed"
        or str((row.get("actual") or {}).get("command_result") or "").lower() in {"fail", "failed", "blocked"}
        for row in route_rows
        if isinstance(row.get("actual"), Mapping)
    )
    unsafe_flush_action_seen = any(
        _as_bool(metric_or_summary(key)) is True
        for key in (
            "vent_off_command_during_flush",
            "seal_command_during_flush",
            "pressure_setpoint_command_during_flush",
            "sample_command_during_flush",
        )
    )
    unsafe_vent_command_sent = _as_bool(
        metric_or_summary("unsafe_vent_after_seal_or_pressure_control_command_sent")
    ) is True
    raw_vent_blocked_after_flush = _as_bool(metric_or_summary("vent_pulse_blocked_after_flush_phase")) is True
    raw_vent_blocked_after_flush_is_failure = metric_or_summary("vent_blocked_after_flush_phase_is_failure")
    normal_maintenance_blocked_after_flush = _as_bool(
        metric_or_summary("normal_maintenance_vent_blocked_after_flush_phase")
    ) is True
    vent_blocked_after_flush = bool(
        raw_vent_blocked_after_flush
        and (
            (_as_bool(raw_vent_blocked_after_flush_is_failure) is True)
            if raw_vent_blocked_after_flush_is_failure is not None
            else normal_maintenance_blocked_after_flush
        )
    )
    positive_preseal_abort_reason = str(
        metric_or_summary("positive_preseal_abort_reason", "abort_reason") or ""
    ).strip()
    preseal_capture_abort_reason = str(
        metric_or_summary(
            "preseal_capture_abort_reason",
            "high_pressure_first_point_abort_reason",
        )
        or ""
    ).strip()
    if not positive_preseal_abort_reason and preseal_capture_abort_reason:
        positive_preseal_abort_reason = preseal_capture_abort_reason
    positive_preseal_pressure_overlimit = bool(
        _as_bool(metric_or_summary("positive_preseal_pressure_overlimit")) is True
        or _as_bool(metric_or_summary("positive_preseal_overlimit_fail_closed")) is True
        or _as_bool(metric_or_summary("vent_off_settle_wait_overlimit_seen")) is True
        or metric_or_summary("preseal_capture_abort_pressure_hpa") is not None
        or positive_preseal_abort_reason
        in {
            "preseal_abort_pressure_exceeded",
            "preseal_capture_abort_pressure_exceeded",
            "co2_preseal_atmosphere_flush_abort_pressure_exceeded",
        }
    )
    route_open_transient_interrupted_by_vent_gap = _as_bool(
        metric_or_summary("route_open_transient_interrupted_by_vent_gap")
    ) is True
    if false_defer_latency_gap:
        route_open_transient_interrupted_by_vent_gap = False
        if route_open_transient_rejection_reason == "vent_gap_exceeded_before_recovery_evaluation":
            route_open_transient_rejection_reason = ""
    if (
        route_conditioning_vent_gap_exceeded
        and not route_open_transient_accepted
        and not route_conditioning_hard_abort_exceeded
    ):
        route_open_transient_interrupted_by_vent_gap = True
        if not route_open_transient_rejection_reason:
            route_open_transient_rejection_reason = "vent_gap_exceeded_before_recovery_evaluation"
    route_open_transient_interrupted_reason = str(
        metric_or_summary("route_open_transient_interrupted_reason") or ""
    ).strip()
    if false_defer_latency_gap and route_open_transient_interrupted_reason == (
        "vent_gap_exceeded_before_recovery_evaluation"
    ):
        route_open_transient_interrupted_reason = ""
    if route_open_transient_interrupted_by_vent_gap and not route_open_transient_interrupted_reason:
        route_open_transient_interrupted_reason = route_open_transient_rejection_reason
    route_open_transient_evaluation_state = str(
        metric_or_summary("route_open_transient_evaluation_state") or ""
    ).strip()
    if route_open_transient_evaluation_state not in {
        "not_started",
        "evaluating",
        "accepted",
        "rejected",
        "interrupted_by_vent_gap",
        "continuing_after_defer_warning",
        "hard_abort",
    }:
        if route_conditioning_hard_abort_exceeded:
            route_open_transient_evaluation_state = "hard_abort"
        elif route_open_transient_accepted:
            route_open_transient_evaluation_state = "accepted"
        elif route_open_transient_interrupted_by_vent_gap:
            route_open_transient_evaluation_state = "interrupted_by_vent_gap"
        elif defer_reschedule_latency_warning and not route_conditioning_vent_gap_exceeded:
            route_open_transient_evaluation_state = "continuing_after_defer_warning"
        elif route_open_transient_rejection_reason or (
            route_open_transient_recovery_required and not route_open_transient_accepted
        ):
            route_open_transient_evaluation_state = "rejected"
        elif metric_or_summary("route_open_transient_peak_pressure_hpa") is not None:
            route_open_transient_evaluation_state = "evaluating"
        else:
            route_open_transient_evaluation_state = "not_started"
    elif false_defer_latency_gap and route_open_transient_evaluation_state == "interrupted_by_vent_gap":
        route_open_transient_evaluation_state = "continuing_after_defer_warning"
    route_open_transient_summary_source = str(
        metric_or_summary("route_open_transient_summary_source") or ""
    ).strip()
    if false_defer_latency_gap and route_open_transient_summary_source == "route_conditioning_vent_gap":
        route_open_transient_summary_source = "route_conditioning_defer_latency_warning"
    if not route_open_transient_summary_source:
        route_open_transient_summary_source = (
            "route_conditioning_vent_gap"
            if route_open_transient_interrupted_by_vent_gap
            else (
                "route_trace_pre_route_vent_pressure"
                if atmosphere_fallback.get("pressure_hpa") is not None
                else "route_conditioning_summary"
            )
        )
    if route_conditioning_hard_abort_exceeded:
        rejection_reasons.append("a2_route_conditioning_hard_abort_pressure_exceeded")
    if route_conditioning_pressure_overlimit:
        rejection_reasons.append("a2_route_conditioning_pressure_overlimit")
    if route_open_transient_recovery_required and not route_open_transient_accepted:
        rejection_reasons.append(
            route_open_transient_rejection_reason
            or "a2_route_conditioning_route_open_transient_not_recovered"
        )
    if route_conditioning_vent_gap_exceeded:
        rejection_reasons.append("a2_route_conditioning_vent_gap_exceeded")
    if route_conditioning_vent_command_failed:
        rejection_reasons.append("a2_route_conditioning_vent_command_failed")
    if route_conditioning_fast_vent_timeout:
        rejection_reasons.append("a2_route_conditioning_fast_vent_command_timeout")
    if route_conditioning_fast_vent_not_supported:
        rejection_reasons.append("a2_route_conditioning_fast_vent_not_supported")
    if route_conditioning_diagnostic_blocked:
        rejection_reasons.append("a2_route_conditioning_diagnostic_blocked_vent_scheduler")
    if route_open_transition_blocked:
        rejection_reasons.append("a2_route_open_transition_blocked_vent_scheduler")
    if route_open_settle_wait_blocked:
        rejection_reasons.append("a2_route_open_settle_wait_blocked_vent_scheduler")
    if unsafe_flush_action_seen:
        rejection_reasons.append("a2_route_conditioning_unsafe_action_before_flush_completed")
    if unsafe_vent_command_sent:
        rejection_reasons.append("a2_route_conditioning_unsafe_vent_after_seal_or_pressure_control")
    if vent_blocked_after_flush:
        rejection_reasons.append("a2_route_conditioning_vent_blocked_after_flush_phase")
    if positive_preseal_pressure_overlimit:
        rejection_reasons.append("a2_positive_preseal_pressure_overlimit")

    a2_3_strategy = a2_conditioning_pressure_source_strategy or raw_config_pressure_source_strategy
    a2_4_temperature_skip_requested = _as_bool(
        _first_value(
            raw_cfg,
            (
                "a2_co2_7_pressure_no_write_probe.temperature_stabilization_wait_skipped",
                "workflow.stability.temperature.temperature_stabilization_wait_skipped",
                "workflow.stability.temperature.skip_temperature_stabilization_wait",
            ),
        )
    ) is True or _as_bool(
        _mapping(admission.operator_confirmation.get("explicit_acknowledgement")).get(
            "skip_temperature_stabilization_wait"
        )
    ) is True
    temperature_stabilization_wait_skipped = (
        _as_bool(metric_or_summary("temperature_stabilization_wait_skipped", "wait_skipped")) is True
        or a2_4_temperature_skip_requested
    )
    temperature_gate_mode = str(
        metric_or_summary("temperature_gate_mode")
        or _first_value(
            raw_cfg,
            (
                "a2_co2_7_pressure_no_write_probe.temperature_gate_mode",
                "workflow.stability.temperature.temperature_gate_mode",
                "workflow.stability.temperature.gate_mode",
            ),
        )
        or ("current_pv_engineering_probe" if temperature_stabilization_wait_skipped else "")
    )
    temperature_not_part_of_acceptance = (
        _as_bool(metric_or_summary("temperature_not_part_of_acceptance")) is True
        or _as_bool(
            _first_value(
                raw_cfg,
                (
                    "a2_co2_7_pressure_no_write_probe.temperature_not_part_of_acceptance",
                    "workflow.stability.temperature.temperature_not_part_of_acceptance",
                ),
            )
        )
        is True
        or a2_4_temperature_skip_requested
    )

    def summary_bool(*keys: str, default: Optional[bool] = None) -> Optional[bool]:
        parsed = _as_bool(metric_or_summary(*keys))
        return default if parsed is None else parsed

    def summary_list(key: str) -> list[Any]:
        value = metric_or_summary(key)
        if isinstance(value, list):
            return list(value)
        if value in (None, ""):
            return []
        return [value]

    temperature_chamber_optional_in_skip_temp_wait = bool(
        bool(
            a2_4_temperature_skip_requested
            and temperature_stabilization_wait_skipped
            and temperature_gate_mode == "current_pv_engineering_probe"
            and temperature_not_part_of_acceptance
        )
        or summary_bool("temperature_chamber_optional_in_skip_temp_wait", default=False) is True
    )
    temperature_chamber_required_for_a2 = bool(
        False
        if temperature_chamber_optional_in_skip_temp_wait
        else summary_bool("temperature_chamber_required_for_a2", default=True)
    )
    temperature_chamber_init_attempted = summary_bool("temperature_chamber_init_attempted", default=None)
    temperature_chamber_init_ok = summary_bool("temperature_chamber_init_ok", default=None)
    temperature_chamber_init_failed = bool(summary_bool("temperature_chamber_init_failed", default=False))
    temperature_chamber_init_failure_blocks_a2 = bool(
        summary_bool(
            "temperature_chamber_init_failure_blocks_a2",
            default=bool(temperature_chamber_init_failed and temperature_chamber_required_for_a2),
        )
    )
    temperature_context_available = summary_bool(
        "temperature_context_available",
        default=False if temperature_chamber_init_failed and temperature_chamber_optional_in_skip_temp_wait else None,
    )
    temperature_context_source = str(metric_or_summary("temperature_context_source") or "").strip()
    temperature_context_unavailable_reason = str(
        metric_or_summary("temperature_context_unavailable_reason") or ""
    ).strip()
    temperature_chamber_readonly_probe_attempted = summary_bool(
        "temperature_chamber_readonly_probe_attempted",
        default=temperature_chamber_optional_in_skip_temp_wait
        and (temperature_chamber_init_attempted is not False),
    )
    temperature_chamber_readonly_probe_result = str(
        metric_or_summary("temperature_chamber_readonly_probe_result")
        or (
            "unavailable"
            if temperature_context_available is False and temperature_chamber_optional_in_skip_temp_wait
            else (
                "available_pending_current_pv_read"
                if temperature_context_available is True and temperature_chamber_optional_in_skip_temp_wait
                else "not_applicable"
            )
        )
    ).strip()
    critical_devices_required = summary_list("critical_devices_required")
    if not critical_devices_required:
        critical_devices_required = ["pressure_controller", "pressure_meter", "relay_a", "relay_b"]
        if temperature_chamber_required_for_a2:
            critical_devices_required.append("temperature_chamber")
    critical_devices_failed = summary_list("critical_devices_failed")
    optional_context_devices = summary_list("optional_context_devices")
    if not optional_context_devices and temperature_chamber_optional_in_skip_temp_wait:
        optional_context_devices = ["temperature_chamber"]
    optional_context_devices_failed = summary_list("optional_context_devices_failed")
    critical_device_init_failure_blocks_probe = bool(
        summary_bool("critical_device_init_failure_blocks_probe", default=bool(critical_devices_failed))
    )
    optional_context_failure_blocks_probe = bool(
        summary_bool("optional_context_failure_blocks_probe", default=False)
    )

    a2_4_probe_required = bool(
        a2_4_temperature_skip_requested
        or str(admission.operator_confirmation.get("pressure_source") or "").strip().lower() == "v1_aligned"
    )
    conditioning_monitor_source = str(
        metric_or_summary("selected_pressure_source_for_conditioning_monitor") or ""
    ).strip()
    pressure_gate_source_observed = str(
        metric_or_summary("selected_pressure_source_for_pressure_gate") or ""
    ).strip()
    if not pressure_gate_source_observed:
        for point in reversed(point_results):
            if not isinstance(point, Mapping):
                continue
            if not (
                _as_bool(point.get("point_completed")) is True
                or int(point.get("sample_count") or 0) > 0
                or point.get("pressure_gauge_hpa_before_ready") not in (None, "")
                or point.get("pressure_gauge_hpa_before_sample") not in (None, "")
            ):
                continue
            pressure_gate_source_observed = str(
                point.get("selected_pressure_source")
                or point.get("pressure_source_selected")
                or point.get("pressure_freshness_decision_source")
                or ""
            ).strip()
            if pressure_gate_source_observed:
                break
    conditioning_monitor_pressure_source_allowed = bool(
        not conditioning_monitor_source
        or conditioning_monitor_source == "digital_pressure_gauge_continuous"
        or _pressure_gate_source_is_v1_aligned(conditioning_monitor_source)
    )
    pressure_gate_evidence_present = bool(
        pressure_gate_source_observed
        or any(
            _as_bool(point.get("point_completed")) is True
            or int(point.get("sample_count") or 0) > 0
            or point.get("pressure_gauge_hpa_before_ready") not in (None, "")
            or point.get("pressure_gauge_hpa_before_sample") not in (None, "")
            for point in point_results
        )
    )
    route_conditioning_fail_closed = bool(
        pressure_points_completed == 0
        and (
            route_conditioning_hard_abort_exceeded
            or route_conditioning_pressure_overlimit
            or (route_open_transient_recovery_required and not route_open_transient_accepted)
            or route_conditioning_vent_gap_exceeded
            or route_conditioning_fast_vent_timeout
            or route_conditioning_fast_vent_not_supported
            or route_conditioning_diagnostic_blocked
            or route_open_transition_blocked
            or route_open_settle_wait_blocked
            or route_conditioning_vent_command_failed
            or unsafe_flush_action_seen
            or unsafe_vent_command_sent
            or vent_blocked_after_flush
        )
    )
    positive_preseal_fail_closed = bool(pressure_points_completed == 0 and positive_preseal_pressure_overlimit)
    pressure_gate_reached = bool(pressure_gate_evidence_present and not route_conditioning_fail_closed)
    pressure_gate_not_reached_reason = ""
    if not pressure_gate_reached:
        pressure_gate_not_reached_reason = (
            "route_conditioning_fail_closed"
            if route_conditioning_fail_closed
            else ("positive_preseal_fail_closed" if positive_preseal_fail_closed else "")
        )
    pressure_gate_source_required = "v1_aligned" if a2_4_probe_required else ""
    pressure_gate_source_alignment_reasons: list[str] = []
    pressure_gate_source_alignment_ready: Optional[bool] = None
    if a2_4_probe_required:
        if pressure_gate_reached:
            pressure_gate_source_alignment_ready = _pressure_gate_source_is_v1_aligned(pressure_gate_source_observed)
            if not pressure_gate_source_alignment_ready:
                pressure_gate_source_alignment_reasons.append("pressure_gate_source_not_v1_aligned")
                rejection_reasons.append("a2_pressure_gate_source_not_v1_aligned")
        else:
            pressure_gate_source_alignment_ready = False
    if a2_4_temperature_skip_requested and not temperature_stabilization_wait_skipped:
        rejection_reasons.append("a2_4_temperature_stabilization_wait_not_skipped")
    if a2_4_temperature_skip_requested and temperature_gate_mode != "current_pv_engineering_probe":
        rejection_reasons.append("a2_4_temperature_gate_mode_not_current_pv_engineering_probe")
    if a2_4_temperature_skip_requested and not temperature_not_part_of_acceptance:
        rejection_reasons.append("a2_4_temperature_not_part_of_acceptance_missing")

    pass_conditions = [
        admission.approved,
        executed,
        not rejection_reasons,
        all_ready,
        all_have_fresh,
        all_have_samples,
        all_completed,
        all_heartbeat,
        all_route,
        no_write_ok,
        not downstream_after_failure,
        not a2_4_temperature_skip_requested or temperature_stabilization_wait_skipped,
        not a2_4_temperature_skip_requested or temperature_gate_mode == "current_pv_engineering_probe",
        not a2_4_temperature_skip_requested or temperature_not_part_of_acceptance,
    ]
    final_decision = "PASS" if all(pass_conditions) else "FAIL_CLOSED"
    if final_decision != "PASS" and not rejection_reasons:
        rejection_reasons.append("a2_pass_conditions_not_met")
    rejection_reasons = list(dict.fromkeys(rejection_reasons))
    fail_closed_reason = ""
    if final_decision != "PASS":
        fail_closed_reason = (
            A2_INTERRUPTED_FAIL_CLOSED_REASON
            if execution_interrupted
            else (rejection_reasons[0] if rejection_reasons else "a2_pass_conditions_not_met")
        )

    pressure_ready_rows = [
        {
            "timestamp": _now(),
            "event": "a2_pressure_ready_gate",
            "target_pressure_hpa": point.get("target_pressure_hpa"),
            "pressure_point_index": point.get("pressure_point_index"),
            "pressure_ready_gate_result": point.get("pressure_ready_gate_result"),
            "pressure_ready_gate_latency_ms": point.get("pressure_ready_gate_latency_ms"),
            "pressure_gauge_hpa_before_ready": point.get("pressure_gauge_hpa_before_ready"),
            "pressure_gauge_hpa_before_sample": point.get("pressure_gauge_hpa_before_sample"),
            "pressure_age_ms_before_sample": point.get("pressure_age_ms_before_sample"),
            "pressure_gauge_freshness_ok_before_sample": point.get("pressure_gauge_freshness_ok_before_sample"),
            "heartbeat_gap_observed_ms": point.get("heartbeat_gap_observed_ms"),
            "heartbeat_emission_gap_ms": point.get("heartbeat_emission_gap_ms"),
            "blocking_operation_duration_ms": point.get("blocking_operation_duration_ms"),
            "pressure_source_selected": point.get("pressure_source_selected"),
            "pressure_source_selection_reason": point.get("pressure_source_selection_reason"),
            "selected_pressure_source": point.get("selected_pressure_source"),
            "selected_pressure_sample_age_s": point.get("selected_pressure_sample_age_s"),
            "selected_pressure_sample_is_stale": point.get("selected_pressure_sample_is_stale"),
            "selected_pressure_parse_ok": point.get("selected_pressure_parse_ok"),
            "selected_pressure_freshness_ok": point.get("selected_pressure_freshness_ok"),
            "pressure_freshness_decision_source": point.get("pressure_freshness_decision_source"),
            "selected_pressure_fail_closed_reason": point.get("selected_pressure_fail_closed_reason"),
            "continuous_stream_stale": point.get("continuous_stream_stale"),
        }
        for point in point_results
    ]
    heartbeat_rows = [
        {
            "timestamp": _now(),
            "event": "a2_heartbeat_before_sample",
            "target_pressure_hpa": point.get("target_pressure_hpa"),
            "pressure_point_index": point.get("pressure_point_index"),
            "heartbeat_ready_before_sample": point.get("heartbeat_ready_before_sample"),
            "heartbeat_gap_observed_ms": point.get("heartbeat_gap_observed_ms"),
            "heartbeat_emission_gap_ms": point.get("heartbeat_emission_gap_ms"),
            "blocking_operation_duration_ms": point.get("blocking_operation_duration_ms"),
            "route_conditioning_ready_before_sample": point.get("route_conditioning_ready_before_sample"),
        }
        for point in point_results
    ]
    sweep_rows = [
        {
            "timestamp": _now(),
            "event": "a2_pressure_point_result",
            **dict(point),
        }
        for point in point_results
    ]
    diagnostic_cfg = _load_json_dict(execution_config_path) if execution_config_path else {}
    if not diagnostic_cfg:
        diagnostic_cfg = raw_cfg
    pressure_controller_diagnostics = _pressure_controller_command_diagnostics(diagnostic_cfg, route_rows)
    pressure_meter_diagnostic_fields = _pressure_meter_diagnostics(diagnostic_cfg, point_results)
    relay_diagnostic_fields = _relay_diagnostics(diagnostic_cfg, route_rows)
    device_precheck_diagnostic_fields = _device_precheck_diagnostics(
        diagnostic_cfg,
        execution,
        service_summary,
        route_rows,
        point_results,
    )
    positive_preseal_pressure_hpa = metric_or_summary("positive_preseal_pressure_hpa")
    if positive_preseal_pressure_hpa is None:
        positive_preseal_pressure_hpa = metric_or_summary(
            "preseal_capture_abort_pressure_hpa",
            "high_pressure_first_point_abort_pressure_hpa",
            "first_over_abort_pressure_hpa",
        )
    positive_preseal_pressure_missing_reason = str(
        metric_or_summary("positive_preseal_pressure_missing_reason") or ""
    )
    if positive_preseal_pressure_hpa is not None and positive_preseal_pressure_missing_reason:
        positive_preseal_pressure_missing_reason = ""

    summary = {
        "schema_version": A2_SCHEMA_VERSION,
        **A2_EVIDENCE_MARKERS,
        "final_decision": final_decision,
        "fail_closed_reason": fail_closed_reason,
        "rejection_reasons": rejection_reasons,
        "interrupted_execution": bool(execution_interrupted),
        "interrupted_at": interrupted_at,
        "interruption_source": interruption_source,
        "interruption_stage": interruption_stage,
        "admission_approved": admission.approved,
        "operator_confirmation_valid": bool(admission.operator_validation.get("valid")),
        "r0_1_reference_readonly_prereq_pass": bool(admission.evidence["r0_1_reference_readonly_prereq_pass"]),
        "r0_full_query_only_prereq_pass": bool(admission.evidence["r0_full_query_only_prereq_pass"]),
        "r1_conditioning_only_prereq_pass": bool(admission.evidence["r1_conditioning_only_prereq_pass"]),
        "a1r_minimal_sampling_prereq_pass": bool(admission.evidence["a1r_minimal_sampling_prereq_pass"]),
        "a1r_output_dir": _output_dir_value(raw_cfg, "a1r_output_dir"),
        "current_branch": branch,
        "current_head": head,
        "v1_fallback_required": True,
        "v1_untouched": True,
        "run_app_py_untouched": bool(run_app_py_untouched),
        "a2_1_heartbeat_gap_accounting_fix_present": A2_1_HEARTBEAT_GAP_ACCOUNTING_FIX_PRESENT,
        "a2_3_v1_pressure_gauge_read_policy_present": A2_3_V1_PRESSURE_GAUGE_READ_POLICY_PRESENT,
        "a2_4_v1_pressure_gauge_read_policy_present": A2_4_V1_PRESSURE_GAUGE_READ_POLICY_PRESENT,
        "a2_3_pressure_source_strategy": a2_3_strategy,
        "a2_4_pressure_source_strategy": a2_3_strategy,
        "raw_config_pressure_source_strategy": raw_config_pressure_source_strategy,
        "downstream_aligned_pressure_source_strategy": downstream_aligned_pressure_source_strategy,
        "runtime_pressure_source_strategy_observed": runtime_pressure_source_strategy_observed,
        "a2_conditioning_pressure_source_strategy_source": a2_conditioning_pressure_source_strategy_source,
        "pressure_source_strategy_aggregation_mismatch": pressure_source_strategy_aggregation_mismatch,
        "pressure_source_strategy_aggregation_mismatch_reason": (
            pressure_source_strategy_aggregation_mismatch_reason
        ),
        "temperature_stabilization_wait_skipped": temperature_stabilization_wait_skipped,
        "temperature_gate_mode": temperature_gate_mode,
        "temperature_not_part_of_acceptance": temperature_not_part_of_acceptance,
        "temperature_chamber_required_for_a2": temperature_chamber_required_for_a2,
        "temperature_chamber_init_attempted": temperature_chamber_init_attempted,
        "temperature_chamber_init_ok": temperature_chamber_init_ok,
        "temperature_chamber_init_failed": temperature_chamber_init_failed,
        "temperature_chamber_init_failure_blocks_a2": temperature_chamber_init_failure_blocks_a2,
        "temperature_chamber_optional_in_skip_temp_wait": temperature_chamber_optional_in_skip_temp_wait,
        "temperature_context_available": temperature_context_available,
        "temperature_context_source": temperature_context_source,
        "temperature_context_unavailable_reason": temperature_context_unavailable_reason,
        "temperature_chamber_readonly_probe_attempted": temperature_chamber_readonly_probe_attempted,
        "temperature_chamber_readonly_probe_result": temperature_chamber_readonly_probe_result,
        "critical_devices_required": critical_devices_required,
        "critical_devices_failed": critical_devices_failed,
        "optional_context_devices": optional_context_devices,
        "optional_context_devices_failed": optional_context_devices_failed,
        "critical_device_init_failure_blocks_probe": critical_device_init_failure_blocks_probe,
        "optional_context_failure_blocks_probe": optional_context_failure_blocks_probe,
        **device_precheck_diagnostic_fields,
        **pressure_controller_diagnostics,
        **pressure_meter_diagnostic_fields,
        **relay_diagnostic_fields,
        "pressure_source_selected": _first_metric_from_rows(evidence_metric_rows, "pressure_source_selected"),
        "pressure_source_selection_reason": _first_metric_from_rows(
            evidence_metric_rows,
            "pressure_source_selection_reason",
            "source_selection_reason",
        ),
        "continuous_stream_stale": _as_bool(metric_or_summary("continuous_stream_stale")) is True,
        "selected_pressure_source": metric_or_summary("selected_pressure_source"),
        "selected_pressure_source_for_conditioning_monitor": metric_or_summary(
            "selected_pressure_source_for_conditioning_monitor"
        )
        or "",
        "selected_pressure_source_for_pressure_gate": pressure_gate_source_observed,
        "conditioning_monitor_pressure_source_allowed": conditioning_monitor_pressure_source_allowed,
        "pressure_gate_reached": pressure_gate_reached,
        "pressure_gate_not_reached_reason": pressure_gate_not_reached_reason,
        "pressure_gate_source_required": pressure_gate_source_required,
        "pressure_gate_source_observed": pressure_gate_source_observed,
        "pressure_gate_source_alignment_ready": pressure_gate_source_alignment_ready,
        "pressure_gate_source_alignment_reasons": pressure_gate_source_alignment_reasons,
        "a2_conditioning_pressure_source_strategy": a2_conditioning_pressure_source_strategy,
        "selected_pressure_sample_age_s": metric_or_summary("selected_pressure_sample_age_s"),
        "selected_pressure_sample_is_stale": _as_bool(
            metric_or_summary("selected_pressure_sample_is_stale")
        )
        is True,
        "selected_pressure_parse_ok": _as_bool(metric_or_summary("selected_pressure_parse_ok")) is True,
        "selected_pressure_freshness_ok": _as_bool(
            metric_or_summary("selected_pressure_freshness_ok")
        )
        is True,
        "pressure_freshness_decision_source": metric_or_summary("pressure_freshness_decision_source"),
        "selected_pressure_fail_closed_reason": metric_or_summary("selected_pressure_fail_closed_reason") or "",
        "selected_pressure_sample_stale_duration_ms": metric_or_summary(
            "selected_pressure_sample_stale_duration_ms"
        ),
        "selected_pressure_sample_stale_budget_ms": metric_or_summary("selected_pressure_sample_stale_budget_ms"),
        "selected_pressure_sample_stale_budget_exceeded": _as_bool(
            metric_or_summary("selected_pressure_sample_stale_budget_exceeded")
        )
        is True,
        "selected_pressure_sample_stale_performed_io": _as_bool(
            metric_or_summary("selected_pressure_sample_stale_performed_io")
        )
        is True,
        "selected_pressure_sample_stale_triggered_source_selection": _as_bool(
            metric_or_summary("selected_pressure_sample_stale_triggered_source_selection")
        )
        is True,
        "selected_pressure_sample_stale_triggered_p3_fallback": _as_bool(
            metric_or_summary("selected_pressure_sample_stale_triggered_p3_fallback")
        )
        is True,
        "selected_pressure_sample_stale_deferred_for_vent_priority": _as_bool(
            metric_or_summary("selected_pressure_sample_stale_deferred_for_vent_priority")
        )
        is True,
        "conditioning_monitor_latest_frame_age_s": metric_or_summary("conditioning_monitor_latest_frame_age_s"),
        "conditioning_monitor_latest_frame_fresh": _as_bool(
            metric_or_summary("conditioning_monitor_latest_frame_fresh")
        )
        is True,
        "conditioning_monitor_latest_frame_unavailable": _as_bool(
            metric_or_summary("conditioning_monitor_latest_frame_unavailable")
        )
        is True,
        "conditioning_monitor_pressure_deferred_count": int(
            metric_or_summary("conditioning_monitor_pressure_deferred_count") or 0
        ),
        "conditioning_monitor_pressure_deferred_elapsed_ms": metric_or_summary(
            "conditioning_monitor_pressure_deferred_elapsed_ms"
        ),
        "conditioning_monitor_max_defer_ms": metric_or_summary("conditioning_monitor_max_defer_ms"),
        "conditioning_monitor_pressure_stale_timeout": _as_bool(
            metric_or_summary("conditioning_monitor_pressure_stale_timeout")
        )
        is True,
        "conditioning_monitor_pressure_unavailable_fail_closed": _as_bool(
            metric_or_summary("conditioning_monitor_pressure_unavailable_fail_closed")
        )
        is True,
        "continuous_latest_fresh_fast_path_used": _as_bool(
            metric_or_summary("continuous_latest_fresh_fast_path_used")
        )
        is True,
        "continuous_latest_fresh_duration_ms": metric_or_summary("continuous_latest_fresh_duration_ms"),
        "continuous_latest_fresh_lock_acquire_ms": metric_or_summary("continuous_latest_fresh_lock_acquire_ms"),
        "continuous_latest_fresh_lock_timeout": _as_bool(
            metric_or_summary("continuous_latest_fresh_lock_timeout")
        )
        is True,
        "continuous_latest_fresh_waited_for_frame": _as_bool(
            metric_or_summary("continuous_latest_fresh_waited_for_frame")
        )
        is True,
        "continuous_latest_fresh_performed_io": _as_bool(
            metric_or_summary("continuous_latest_fresh_performed_io")
        )
        is True,
        "continuous_latest_fresh_triggered_stream_restart": _as_bool(
            metric_or_summary("continuous_latest_fresh_triggered_stream_restart")
        )
        is True,
        "continuous_latest_fresh_triggered_drain": _as_bool(
            metric_or_summary("continuous_latest_fresh_triggered_drain")
        )
        is True,
        "continuous_latest_fresh_triggered_p3_fallback": _as_bool(
            metric_or_summary("continuous_latest_fresh_triggered_p3_fallback")
        )
        is True,
        "continuous_latest_fresh_budget_ms": metric_or_summary("continuous_latest_fresh_budget_ms"),
        "continuous_latest_fresh_budget_exceeded": _as_bool(
            metric_or_summary("continuous_latest_fresh_budget_exceeded")
        )
        is True,
        "critical_window_uses_latest_frame": _as_bool(
            _first_metric_from_rows(evidence_metric_rows, "critical_window_uses_latest_frame")
        )
        is True,
        "critical_window_uses_query": _as_bool(
            _first_metric_from_rows(evidence_metric_rows, "critical_window_uses_query")
        )
        is True,
        "p3_fast_fallback_attempted": _as_bool(
            _first_metric_from_rows(evidence_metric_rows, "p3_fast_fallback_attempted")
        )
        is True,
        "p3_fast_fallback_result": _first_metric_from_rows(evidence_metric_rows, "p3_fast_fallback_result") or "",
        "normal_p3_fallback_attempted": _as_bool(
            _first_metric_from_rows(evidence_metric_rows, "normal_p3_fallback_attempted")
        )
        is True,
        "normal_p3_fallback_result": _first_metric_from_rows(evidence_metric_rows, "normal_p3_fallback_result") or "",
        "digital_gauge_stream_stale": _as_bool(
            _first_metric_from_rows(evidence_metric_rows, "digital_gauge_stream_stale")
        )
        is True,
        "continuous_restart_attempted": _as_bool(
            _first_metric_from_rows(evidence_metric_rows, "continuous_restart_attempted")
        )
        is True,
        "continuous_restart_result": _first_metric_from_rows(evidence_metric_rows, "continuous_restart_result") or "",
        "route_conditioning_vent_maintenance_active": _as_bool(
            metric_or_summary("route_conditioning_vent_maintenance_active")
        )
        is True,
        "route_conditioning_phase": metric_or_summary("route_conditioning_phase") or "",
        "ready_to_seal_phase_started": _as_bool(metric_or_summary("ready_to_seal_phase_started")) is True,
        "route_conditioning_flush_min_time_completed": _as_bool(
            metric_or_summary("route_conditioning_flush_min_time_completed")
        )
        is True,
        "vent_maintenance_started_at": metric_or_summary("vent_maintenance_started_at") or "",
        "vent_maintenance_started_monotonic_s": metric_or_summary("vent_maintenance_started_monotonic_s"),
        "pre_route_vent_phase_started": _as_bool(metric_or_summary("pre_route_vent_phase_started")) is True,
        "pre_route_fast_vent_required": (
            metric_or_summary("pre_route_fast_vent_required") is None
            or _as_bool(metric_or_summary("pre_route_fast_vent_required")) is True
        ),
        "pre_route_fast_vent_sent": _as_bool(metric_or_summary("pre_route_fast_vent_sent")) is True,
        "pre_route_fast_vent_duration_ms": metric_or_summary("pre_route_fast_vent_duration_ms"),
        "pre_route_fast_vent_timeout": _as_bool(metric_or_summary("pre_route_fast_vent_timeout")) is True,
        "fast_vent_reassert_supported": _as_bool(metric_or_summary("fast_vent_reassert_supported")) is True,
        "fast_vent_reassert_used": _as_bool(metric_or_summary("fast_vent_reassert_used")) is True,
        "vent_command_write_started_at": metric_or_summary("vent_command_write_started_at") or "",
        "vent_command_write_sent_at": metric_or_summary("vent_command_write_sent_at") or "",
        "vent_command_write_completed_at": metric_or_summary("vent_command_write_completed_at") or "",
        "vent_command_write_duration_ms": metric_or_summary("vent_command_write_duration_ms"),
        "vent_command_total_duration_ms": metric_or_summary("vent_command_total_duration_ms"),
        "vent_command_wait_after_command_s": metric_or_summary("vent_command_wait_after_command_s"),
        "vent_command_capture_pressure_enabled": _as_bool(
            metric_or_summary("vent_command_capture_pressure_enabled")
        )
        is True,
        "vent_command_query_state_enabled": _as_bool(metric_or_summary("vent_command_query_state_enabled")) is True,
        "vent_command_confirm_transition_enabled": _as_bool(
            metric_or_summary("vent_command_confirm_transition_enabled")
        )
        is True,
        "vent_command_blocking_phase": metric_or_summary("vent_command_blocking_phase") or "",
        "route_conditioning_fast_vent_command_timeout": route_conditioning_fast_vent_timeout,
        "route_conditioning_fast_vent_not_supported": route_conditioning_fast_vent_not_supported,
        "route_conditioning_diagnostic_blocked_vent_scheduler": route_conditioning_diagnostic_blocked,
        "vent_scheduler_priority_mode": _as_bool(metric_or_summary("vent_scheduler_priority_mode")) is True,
        "vent_scheduler_checked_before_diagnostic": _as_bool(
            metric_or_summary("vent_scheduler_checked_before_diagnostic")
        )
        is True,
        "diagnostic_deferred_for_vent_priority": _as_bool(
            metric_or_summary("diagnostic_deferred_for_vent_priority")
        )
        is True,
        "diagnostic_deferred_count": int(metric_or_summary("diagnostic_deferred_count") or 0),
        "diagnostic_budget_ms": metric_or_summary("diagnostic_budget_ms"),
        "diagnostic_budget_exceeded": _as_bool(metric_or_summary("diagnostic_budget_exceeded")) is True,
        "diagnostic_blocking_component": metric_or_summary("diagnostic_blocking_component") or "",
        "diagnostic_blocking_operation": metric_or_summary("diagnostic_blocking_operation") or "",
        "diagnostic_blocking_duration_ms": metric_or_summary("diagnostic_blocking_duration_ms"),
        "pressure_monitor_nonblocking": _as_bool(metric_or_summary("pressure_monitor_nonblocking")) is True,
        "pressure_monitor_deferred_for_vent_priority": _as_bool(
            metric_or_summary("pressure_monitor_deferred_for_vent_priority")
        )
        is True,
        "pressure_monitor_budget_ms": metric_or_summary("pressure_monitor_budget_ms"),
        "pressure_monitor_duration_ms": metric_or_summary("pressure_monitor_duration_ms"),
        "pressure_monitor_blocked_vent_scheduler": _as_bool(
            metric_or_summary("pressure_monitor_blocked_vent_scheduler")
        )
        is True,
        "conditioning_monitor_pressure_deferred": _as_bool(
            metric_or_summary("conditioning_monitor_pressure_deferred")
        )
        is True,
        "trace_write_budget_ms": metric_or_summary("trace_write_budget_ms"),
        "trace_write_duration_ms": metric_or_summary("trace_write_duration_ms"),
        "trace_write_blocked_vent_scheduler": _as_bool(
            metric_or_summary("trace_write_blocked_vent_scheduler")
        )
        is True,
        "trace_write_deferred_for_vent_priority": _as_bool(
            metric_or_summary("trace_write_deferred_for_vent_priority")
        )
        is True,
        "route_open_transition_started": _as_bool(metric_or_summary("route_open_transition_started")) is True,
        "route_open_transition_started_at": metric_or_summary("route_open_transition_started_at") or "",
        "route_open_transition_started_monotonic_s": metric_or_summary(
            "route_open_transition_started_monotonic_s"
        ),
        "route_open_command_write_started_at": metric_or_summary("route_open_command_write_started_at") or "",
        "route_open_command_write_completed_at": metric_or_summary("route_open_command_write_completed_at") or "",
        "route_open_command_write_duration_ms": metric_or_summary("route_open_command_write_duration_ms"),
        "route_open_settle_wait_sliced": _as_bool(metric_or_summary("route_open_settle_wait_sliced")) is True,
        "route_open_settle_wait_slice_count": int(metric_or_summary("route_open_settle_wait_slice_count") or 0),
        "route_open_settle_wait_total_ms": metric_or_summary("route_open_settle_wait_total_ms"),
        "route_open_transition_total_duration_ms": metric_or_summary("route_open_transition_total_duration_ms"),
        "vent_ticks_during_route_open_transition": int(
            metric_or_summary("vent_ticks_during_route_open_transition") or 0
        ),
        "route_open_transition_max_vent_write_gap_ms": metric_or_summary(
            "route_open_transition_max_vent_write_gap_ms"
        ),
        "route_open_transition_terminal_vent_write_age_ms": metric_or_summary(
            "route_open_transition_terminal_vent_write_age_ms"
        ),
        "route_open_transition_blocked_vent_scheduler": route_open_transition_blocked,
        "route_open_settle_wait_blocked_vent_scheduler": route_open_settle_wait_blocked,
        "terminal_vent_write_age_ms_at_gap_gate": metric_or_summary(
            "terminal_vent_write_age_ms_at_gap_gate"
        ),
        "max_vent_pulse_write_gap_ms_including_terminal_gap": (
            max_vent_pulse_write_gap_ms_including_terminal_gap
        ),
        "route_conditioning_vent_gap_exceeded_source": metric_or_summary(
            "route_conditioning_vent_gap_exceeded_source"
        )
        or "",
        "terminal_gap_source": metric_or_summary("terminal_gap_source") or "",
        "terminal_gap_operation": metric_or_summary("terminal_gap_operation") or "",
        "terminal_gap_duration_ms": metric_or_summary("terminal_gap_duration_ms"),
        "terminal_gap_started_at": metric_or_summary("terminal_gap_started_at") or "",
        "terminal_gap_detected_at": metric_or_summary("terminal_gap_detected_at") or "",
        "terminal_gap_stack_marker": metric_or_summary("terminal_gap_stack_marker") or "",
        "defer_source": metric_or_summary("defer_source") or "",
        "defer_operation": metric_or_summary("defer_operation") or "",
        "defer_started_at": metric_or_summary("defer_started_at") or "",
        "defer_returned_to_vent_loop": defer_returned_to_vent_loop_value,
        "defer_to_next_vent_loop_ms": metric_or_summary("defer_to_next_vent_loop_ms"),
        "defer_reschedule_latency_ms": defer_reschedule_latency_ms,
        "defer_reschedule_latency_budget_ms": defer_reschedule_latency_budget_ms,
        "defer_reschedule_latency_exceeded": defer_reschedule_latency_exceeded,
        "defer_reschedule_latency_warning": defer_reschedule_latency_warning,
        "defer_reschedule_caused_vent_gap_exceeded": defer_reschedule_caused_vent_gap_exceeded,
        "defer_reschedule_requested": _as_bool(
            metric_or_summary("defer_reschedule_requested")
        )
        is True,
        "defer_reschedule_completed": defer_reschedule_completed_value,
        "defer_reschedule_reason": metric_or_summary("defer_reschedule_reason") or "",
        "vent_tick_after_defer_ms": metric_or_summary("vent_tick_after_defer_ms"),
        "fast_vent_after_defer_sent": fast_vent_after_defer_sent_value,
        "fast_vent_after_defer_write_ms": metric_or_summary("fast_vent_after_defer_write_ms"),
        "terminal_gap_after_defer": terminal_gap_after_defer_value,
        "terminal_gap_after_defer_ms": terminal_gap_after_defer_ms_value,
        "vent_gap_exceeded_after_defer": vent_gap_exceeded_after_defer,
        "vent_gap_after_defer_ms": vent_gap_after_defer_ms,
        "vent_gap_after_defer_threshold_ms": vent_gap_after_defer_threshold_ms,
        "defer_path_no_reschedule": bool(
            (_as_bool(metric_or_summary("defer_path_no_reschedule")) is True)
            and defer_reschedule_caused_vent_gap_exceeded
        ),
        "defer_path_no_reschedule_reason": (
            metric_or_summary("defer_path_no_reschedule_reason") or ""
        )
        if defer_reschedule_caused_vent_gap_exceeded
        else "",
        "fail_closed_path_started": _as_bool(metric_or_summary("fail_closed_path_started")) is True,
        "fail_closed_path_started_while_route_open": _as_bool(
            metric_or_summary("fail_closed_path_started_while_route_open")
        )
        is True,
        "fail_closed_path_vent_maintenance_required": _as_bool(
            metric_or_summary("fail_closed_path_vent_maintenance_required")
        )
        is True,
        "fail_closed_path_vent_maintenance_active": _as_bool(
            metric_or_summary("fail_closed_path_vent_maintenance_active")
        )
        is True,
        "fail_closed_path_duration_ms": metric_or_summary("fail_closed_path_duration_ms"),
        "fail_closed_path_blocked_vent_scheduler": _as_bool(
            metric_or_summary("fail_closed_path_blocked_vent_scheduler")
        )
        is True,
        "route_open_high_frequency_vent_phase_started": _as_bool(
            metric_or_summary("route_open_high_frequency_vent_phase_started")
        )
        is True,
        "route_open_to_first_vent_ms": metric_or_summary("route_open_to_first_vent_ms"),
        "route_open_to_first_vent_s": metric_or_summary("route_open_to_first_vent_s"),
        "route_open_to_first_vent_write_ms": metric_or_summary("route_open_to_first_vent_write_ms"),
        "route_open_to_first_pressure_read_ms": metric_or_summary("route_open_to_first_pressure_read_ms"),
        "route_open_to_overlimit_ms": metric_or_summary("route_open_to_overlimit_ms"),
        "max_vent_pulse_gap_ms": max_vent_pulse_gap_ms,
        "max_vent_pulse_write_gap_ms": max_vent_pulse_write_gap_ms,
        "max_vent_command_total_duration_ms": metric_or_summary("max_vent_command_total_duration_ms"),
        "max_vent_pulse_gap_limit_ms": max_vent_pulse_gap_limit_ms,
        "max_vent_pulse_write_gap_phase": metric_or_summary("max_vent_pulse_write_gap_phase") or "",
        "max_vent_pulse_write_gap_threshold_ms": metric_or_summary(
            "max_vent_pulse_write_gap_threshold_ms"
        ),
        "max_vent_pulse_write_gap_threshold_source": metric_or_summary(
            "max_vent_pulse_write_gap_threshold_source"
        )
        or "",
        "max_vent_pulse_write_gap_exceeded": _as_bool(
            metric_or_summary("max_vent_pulse_write_gap_exceeded")
        )
        is True,
        "max_vent_pulse_write_gap_not_exceeded_reason": metric_or_summary(
            "max_vent_pulse_write_gap_not_exceeded_reason"
        )
        or "",
        "vent_scheduler_tick_count": int(metric_or_summary("vent_scheduler_tick_count") or 0),
        "vent_scheduler_loop_gap_ms": metric_or_summary("vent_scheduler_loop_gap_ms") or [],
        "max_vent_scheduler_loop_gap_ms": metric_or_summary("max_vent_scheduler_loop_gap_ms"),
        "vent_pulse_count": int(metric_or_summary("vent_pulse_count") or 0),
        "vent_pulse_interval_ms": metric_or_summary("vent_pulse_interval_ms") or [],
        "pressure_drop_after_vent_hpa": metric_or_summary("pressure_drop_after_vent_hpa") or [],
        "measured_atmospheric_pressure_hpa": measured_atmospheric_pressure_hpa,
        "measured_atmospheric_pressure_source": measured_atmospheric_pressure_source,
        "measured_atmospheric_pressure_sample_age_s": measured_atmospheric_pressure_sample_age_s,
        "route_conditioning_pressure_before_route_open_hpa": (
            route_conditioning_pressure_before_route_open_hpa
        ),
        "route_conditioning_pressure_after_route_open_hpa": metric_or_summary(
            "route_conditioning_pressure_after_route_open_hpa"
        ),
        "route_conditioning_pressure_rise_rate_hpa_per_s": metric_or_summary(
            "route_conditioning_pressure_rise_rate_hpa_per_s"
        ),
        "route_conditioning_peak_pressure_hpa": metric_or_summary("route_conditioning_peak_pressure_hpa"),
        "route_open_transient_window_enabled": _as_bool(
            metric_or_summary("route_open_transient_window_enabled")
        )
        is True,
        "route_open_transient_peak_pressure_hpa": metric_or_summary("route_open_transient_peak_pressure_hpa"),
        "route_open_transient_peak_time_ms": metric_or_summary("route_open_transient_peak_time_ms"),
        "route_open_transient_recovery_required": _as_bool(
            metric_or_summary("route_open_transient_recovery_required")
        )
        is True,
        "route_open_transient_recovered_to_atmosphere": _as_bool(
            metric_or_summary("route_open_transient_recovered_to_atmosphere")
        )
        is True,
        "route_open_transient_recovery_time_ms": metric_or_summary("route_open_transient_recovery_time_ms"),
        "route_open_transient_recovery_target_hpa": route_open_transient_recovery_target_hpa,
        "route_open_transient_recovery_band_hpa": metric_or_summary("route_open_transient_recovery_band_hpa"),
        "route_open_transient_stable_hold_s": metric_or_summary("route_open_transient_stable_hold_s"),
        "route_open_transient_stable_pressure_mean_hpa": metric_or_summary(
            "route_open_transient_stable_pressure_mean_hpa"
        ),
        "route_open_transient_stable_pressure_span_hpa": metric_or_summary(
            "route_open_transient_stable_pressure_span_hpa"
        ),
        "route_open_transient_stable_pressure_slope_hpa_per_s": metric_or_summary(
            "route_open_transient_stable_pressure_slope_hpa_per_s"
        ),
        "route_open_transient_accepted": _as_bool(metric_or_summary("route_open_transient_accepted")) is True,
        "route_open_transient_rejection_reason": route_open_transient_rejection_reason,
        "route_open_transient_evaluation_state": route_open_transient_evaluation_state,
        "route_open_transient_interrupted_by_vent_gap": route_open_transient_interrupted_by_vent_gap,
        "route_open_transient_interrupted_reason": route_open_transient_interrupted_reason,
        "route_open_transient_summary_source": route_open_transient_summary_source,
        "sustained_pressure_rise_after_route_open": _as_bool(
            metric_or_summary("sustained_pressure_rise_after_route_open")
        )
        is True,
        "pressure_rise_despite_valid_vent_scheduler": _as_bool(
            metric_or_summary("pressure_rise_despite_valid_vent_scheduler")
        )
        is True,
        "route_conditioning_hard_abort_pressure_hpa": metric_or_summary(
            "route_conditioning_hard_abort_pressure_hpa"
        ),
        "route_conditioning_hard_abort_exceeded": _as_bool(
            metric_or_summary("route_conditioning_hard_abort_exceeded")
        )
        is True,
        "route_conditioning_pressure_overlimit": route_conditioning_pressure_overlimit,
        "route_conditioning_vent_gap_exceeded": route_conditioning_vent_gap_exceeded,
        "positive_preseal_phase_started": _as_bool(metric_or_summary("positive_preseal_phase_started")) is True,
        "positive_preseal_phase_started_at": metric_or_summary("positive_preseal_phase_started_at") or "",
        "positive_preseal_pressure_guard_checked": _as_bool(
            metric_or_summary("positive_preseal_pressure_guard_checked")
        )
        is True,
        "positive_preseal_pressure_hpa": positive_preseal_pressure_hpa,
        "positive_preseal_pressure_source": metric_or_summary("positive_preseal_pressure_source") or "",
        "positive_preseal_pressure_source_path": metric_or_summary(
            "positive_preseal_pressure_source_path",
            "preseal_abort_source_path",
        )
        or "",
        "positive_preseal_pressure_missing_reason": positive_preseal_pressure_missing_reason,
        "positive_preseal_pressure_sample_age_s": metric_or_summary(
            "positive_preseal_pressure_sample_age_s"
        ),
        "positive_preseal_abort_pressure_hpa": metric_or_summary(
            "positive_preseal_abort_pressure_hpa",
            "preseal_abort_pressure_hpa",
            "abort_pressure_hpa",
        ),
        "positive_preseal_pressure_overlimit": positive_preseal_pressure_overlimit,
        "positive_preseal_abort_reason": positive_preseal_abort_reason,
        "positive_preseal_setpoint_sent": _as_bool(metric_or_summary("positive_preseal_setpoint_sent")) is True,
        "positive_preseal_setpoint_hpa": metric_or_summary("positive_preseal_setpoint_hpa"),
        "positive_preseal_output_enabled": _as_bool(metric_or_summary("positive_preseal_output_enabled")) is True,
        "positive_preseal_route_open": _as_bool(metric_or_summary("positive_preseal_route_open")) is True,
        "positive_preseal_seal_command_sent": _as_bool(
            metric_or_summary("positive_preseal_seal_command_sent")
        )
        is True,
        "positive_preseal_pressure_setpoint_command_sent": _as_bool(
            metric_or_summary("positive_preseal_pressure_setpoint_command_sent")
        )
        is True,
        "positive_preseal_sample_started": _as_bool(metric_or_summary("positive_preseal_sample_started")) is True,
        "positive_preseal_overlimit_fail_closed": _as_bool(
            metric_or_summary("positive_preseal_overlimit_fail_closed")
        )
        is True,
        "positive_preseal_overlimit_root_cause_candidate": metric_or_summary(
            "positive_preseal_overlimit_root_cause_candidate"
        )
        or "",
        "positive_preseal_overlimit_first_seen_elapsed_s": metric_or_summary(
            "positive_preseal_overlimit_first_seen_elapsed_s"
        ),
        "positive_preseal_overlimit_first_seen_pressure_hpa": metric_or_summary(
            "positive_preseal_overlimit_first_seen_pressure_hpa"
        ),
        "positive_preseal_overlimit_first_seen_source": metric_or_summary(
            "positive_preseal_overlimit_first_seen_source"
        )
        or "",
        "positive_preseal_overlimit_first_seen_sample_age_s": metric_or_summary(
            "positive_preseal_overlimit_first_seen_sample_age_s"
        ),
        "positive_preseal_overlimit_first_seen_sequence_id": metric_or_summary(
            "positive_preseal_overlimit_first_seen_sequence_id"
        ),
        "positive_preseal_pressure_peak_hpa": metric_or_summary("positive_preseal_pressure_peak_hpa"),
        "positive_preseal_pressure_peak_elapsed_s": metric_or_summary("positive_preseal_pressure_peak_elapsed_s"),
        "positive_preseal_pressure_peak_source": metric_or_summary("positive_preseal_pressure_peak_source") or "",
        "positive_preseal_pressure_rise_rate_peak_hpa_per_s": metric_or_summary(
            "positive_preseal_pressure_rise_rate_peak_hpa_per_s"
        ),
        "positive_preseal_setpoint_command_sent": _as_bool(
            metric_or_summary(
                "positive_preseal_setpoint_command_sent",
                "positive_preseal_pressure_setpoint_command_sent",
                "positive_preseal_setpoint_sent",
            )
        )
        is True,
        "positive_preseal_setpoint_pressure_hpa": metric_or_summary(
            "positive_preseal_setpoint_pressure_hpa",
            "positive_preseal_setpoint_hpa",
        ),
        "positive_preseal_output_enable_sent": _as_bool(
            metric_or_summary(
                "positive_preseal_output_enable_sent",
                "positive_preseal_output_enabled",
            )
        )
        is True,
        "positive_preseal_output_disable_sent": _as_bool(
            metric_or_summary("positive_preseal_output_disable_sent")
        )
        is True,
        "positive_preseal_output_disable_latency_s": metric_or_summary(
            "positive_preseal_output_disable_latency_s"
        ),
        "positive_preseal_vent_close_arm_trigger": metric_or_summary(
            "positive_preseal_vent_close_arm_trigger",
            "vent_close_arm_trigger",
        )
        or "",
        "positive_preseal_vent_close_command_sent": _as_bool(
            metric_or_summary("positive_preseal_vent_close_command_sent")
        )
        is True,
        "positive_preseal_ready_reached_before_vent_close_completed": _as_bool(
            metric_or_summary("positive_preseal_ready_reached_before_vent_close_completed")
        )
        is True,
        "positive_preseal_ready_reached_during_vent_close": _as_bool(
            metric_or_summary("positive_preseal_ready_reached_during_vent_close")
        )
        is True,
        "positive_preseal_ready_to_abort_latency_s": metric_or_summary(
            "positive_preseal_ready_to_abort_latency_s"
        ),
        "positive_preseal_abort_to_relief_latency_s": metric_or_summary(
            "positive_preseal_abort_to_relief_latency_s"
        ),
        "positive_preseal_pressure_source_used_for_abort": metric_or_summary(
            "positive_preseal_pressure_source_used_for_abort",
            "abort_decision_pressure_source",
        )
        or "",
        "positive_preseal_digital_gauge_pressure_hpa": metric_or_summary(
            "positive_preseal_digital_gauge_pressure_hpa"
        ),
        "positive_preseal_pace_pressure_hpa": metric_or_summary("positive_preseal_pace_pressure_hpa"),
        "positive_preseal_source_disagreement_hpa": metric_or_summary(
            "positive_preseal_source_disagreement_hpa"
        ),
        "preseal_capture_started": _as_bool(metric_or_summary("preseal_capture_started")) is True,
        "preseal_capture_not_pressure_control": _as_bool(
            metric_or_summary("preseal_capture_not_pressure_control")
        )
        is True,
        "preseal_capture_pressure_rise_expected_after_vent_close": _as_bool(
            metric_or_summary("preseal_capture_pressure_rise_expected_after_vent_close")
        )
        is True,
        "preseal_capture_monitor_armed_before_vent_close_command": _as_bool(
            metric_or_summary("preseal_capture_monitor_armed_before_vent_close_command")
        )
        is True,
        "preseal_capture_monitor_covers_abort_path": _as_bool(
            metric_or_summary("preseal_capture_monitor_covers_abort_path")
        )
        is True,
        "preseal_capture_abort_reason": preseal_capture_abort_reason,
        "preseal_capture_abort_pressure_hpa": metric_or_summary(
            "preseal_capture_abort_pressure_hpa",
            "high_pressure_first_point_abort_pressure_hpa",
        ),
        "preseal_capture_abort_source": metric_or_summary("preseal_capture_abort_source") or "",
        "preseal_capture_abort_sample_age_s": metric_or_summary("preseal_capture_abort_sample_age_s"),
        "preseal_capture_ready_window_min_hpa": metric_or_summary(
            "preseal_capture_ready_window_min_hpa",
            "first_target_ready_to_seal_min_hpa",
        ),
        "preseal_capture_ready_window_max_hpa": metric_or_summary(
            "preseal_capture_ready_window_max_hpa",
            "first_target_ready_to_seal_max_hpa",
        ),
        "preseal_capture_ready_window_action": metric_or_summary(
            "preseal_capture_ready_window_action"
        )
        or "",
        "preseal_capture_over_abort_action": metric_or_summary("preseal_capture_over_abort_action") or "",
        "preseal_capture_urgent_seal_threshold_hpa": metric_or_summary(
            "preseal_capture_urgent_seal_threshold_hpa"
        ),
        "preseal_capture_hard_abort_pressure_hpa": metric_or_summary(
            "preseal_capture_hard_abort_pressure_hpa"
        ),
        "preseal_capture_over_urgent_threshold_action": metric_or_summary(
            "preseal_capture_over_urgent_threshold_action"
        )
        or "",
        "preseal_capture_urgent_seal_triggered": _as_bool(
            metric_or_summary("preseal_capture_urgent_seal_triggered")
        )
        is True,
        "preseal_capture_urgent_seal_pressure_hpa": metric_or_summary(
            "preseal_capture_urgent_seal_pressure_hpa"
        ),
        "preseal_capture_urgent_seal_reason": metric_or_summary(
            "preseal_capture_urgent_seal_reason"
        )
        or "",
        "preseal_capture_hard_abort_triggered": _as_bool(
            metric_or_summary("preseal_capture_hard_abort_triggered")
        )
        is True,
        "preseal_capture_hard_abort_reason": metric_or_summary(
            "preseal_capture_hard_abort_reason"
        )
        or "",
        "preseal_capture_continue_to_control_after_seal": _as_bool(
            metric_or_summary("preseal_capture_continue_to_control_after_seal")
        )
        is True,
        "pressure_control_allowed_after_seal_confirmed": _as_bool(
            metric_or_summary("pressure_control_allowed_after_seal_confirmed")
        )
        is True,
        "pressure_control_target_after_preseal_hpa": metric_or_summary(
            "pressure_control_target_after_preseal_hpa"
        ),
        "preseal_capture_predictive_ready_to_seal": _as_bool(
            metric_or_summary("preseal_capture_predictive_ready_to_seal")
        )
        is True,
        "preseal_capture_pressure_rise_rate_hpa_per_s": metric_or_summary(
            "preseal_capture_pressure_rise_rate_hpa_per_s"
        ),
        "preseal_capture_estimated_time_to_target_s": metric_or_summary(
            "preseal_capture_estimated_time_to_target_s"
        ),
        "preseal_capture_seal_completion_latency_s": metric_or_summary(
            "preseal_capture_seal_completion_latency_s"
        ),
        "preseal_capture_predicted_seal_completion_pressure_hpa": metric_or_summary(
            "preseal_capture_predicted_seal_completion_pressure_hpa"
        ),
        "preseal_capture_predictive_trigger_reason": metric_or_summary(
            "preseal_capture_predictive_trigger_reason"
        )
        or "",
        "preseal_abort_source_path": metric_or_summary("preseal_abort_source_path") or "",
        "first_over_1100_before_vent_close": _as_bool(
            metric_or_summary("first_over_1100_before_vent_close")
        )
        is True,
        "first_over_1100_not_actionable_reason": metric_or_summary(
            "first_over_1100_not_actionable_reason"
        )
        or "",
        "high_pressure_first_point_abort_pressure_hpa": metric_or_summary(
            "high_pressure_first_point_abort_pressure_hpa",
            "preseal_capture_abort_pressure_hpa",
        ),
        "high_pressure_first_point_abort_reason": metric_or_summary(
            "high_pressure_first_point_abort_reason",
            "preseal_capture_abort_reason",
        )
        or "",
        "monitor_context_propagated_to_wrapper_summary": _as_bool(
            metric_or_summary("monitor_context_propagated_to_wrapper_summary")
        )
        is True,
        "preseal_guard_armed": _as_bool(metric_or_summary("preseal_guard_armed")) is True,
        "preseal_guard_armed_at": metric_or_summary("preseal_guard_armed_at") or "",
        "preseal_guard_arm_source": metric_or_summary("preseal_guard_arm_source") or "",
        "preseal_guard_armed_from_vent_close_command": _as_bool(
            metric_or_summary("preseal_guard_armed_from_vent_close_command")
        )
        is True,
        "vent_close_to_preseal_guard_arm_latency_s": metric_or_summary(
            "vent_close_to_preseal_guard_arm_latency_s"
        ),
        "vent_close_to_positive_preseal_start_latency_s": metric_or_summary(
            "vent_close_to_positive_preseal_start_latency_s"
        ),
        "vent_off_settle_wait_pressure_monitored": _as_bool(
            metric_or_summary("vent_off_settle_wait_pressure_monitored")
        )
        is True,
        "vent_off_settle_wait_overlimit_seen": _as_bool(
            metric_or_summary("vent_off_settle_wait_overlimit_seen")
        )
        is True,
        "vent_off_settle_wait_ready_to_seal_seen": _as_bool(
            metric_or_summary("vent_off_settle_wait_ready_to_seal_seen")
        )
        is True,
        "vent_off_settle_monitor_started": _as_bool(
            metric_or_summary("vent_off_settle_monitor_started")
        )
        is True,
        "vent_off_settle_monitor_started_at": metric_or_summary(
            "vent_off_settle_monitor_started_at"
        )
        or "",
        "vent_off_settle_monitor_sample_count": metric_or_summary(
            "vent_off_settle_monitor_sample_count"
        ),
        "vent_off_settle_first_ready_to_seal_sample_hpa": metric_or_summary(
            "vent_off_settle_first_ready_to_seal_sample_hpa"
        ),
        "vent_off_settle_first_ready_to_seal_sample_at": metric_or_summary(
            "vent_off_settle_first_ready_to_seal_sample_at"
        )
        or "",
        "vent_off_settle_first_over_abort_sample_hpa": metric_or_summary(
            "vent_off_settle_first_over_abort_sample_hpa"
        ),
        "vent_off_settle_first_over_abort_sample_at": metric_or_summary(
            "vent_off_settle_first_over_abort_sample_at"
        )
        or "",
        "first_target_ready_to_seal_min_hpa": metric_or_summary("first_target_ready_to_seal_min_hpa"),
        "first_target_ready_to_seal_max_hpa": metric_or_summary("first_target_ready_to_seal_max_hpa"),
        "first_target_ready_to_seal_pressure_hpa": metric_or_summary(
            "first_target_ready_to_seal_pressure_hpa"
        ),
        "first_target_ready_to_seal_elapsed_s": metric_or_summary("first_target_ready_to_seal_elapsed_s"),
        "first_target_ready_to_seal_before_abort": _as_bool(
            metric_or_summary("first_target_ready_to_seal_before_abort")
        )
        is True,
        "first_target_ready_to_seal_missed": _as_bool(
            metric_or_summary("first_target_ready_to_seal_missed")
        )
        is True,
        "first_target_ready_to_seal_missed_reason": metric_or_summary(
            "first_target_ready_to_seal_missed_reason"
        )
        or "",
        "first_over_abort_pressure_hpa": metric_or_summary("first_over_abort_pressure_hpa"),
        "first_over_abort_elapsed_s": metric_or_summary("first_over_abort_elapsed_s"),
        "first_over_abort_source": metric_or_summary("first_over_abort_source") or "",
        "first_over_abort_sample_age_s": metric_or_summary("first_over_abort_sample_age_s"),
        "first_over_abort_to_abort_latency_s": metric_or_summary("first_over_abort_to_abort_latency_s"),
        "positive_preseal_guard_started_before_first_over_abort": _as_bool(
            metric_or_summary("positive_preseal_guard_started_before_first_over_abort")
        )
        is True,
        "positive_preseal_guard_started_after_first_over_abort": _as_bool(
            metric_or_summary("positive_preseal_guard_started_after_first_over_abort")
        )
        is True,
        "positive_preseal_guard_late_reason": metric_or_summary("positive_preseal_guard_late_reason") or "",
        "seal_command_allowed_after_atmosphere_vent_closed": _as_bool(
            metric_or_summary("seal_command_allowed_after_atmosphere_vent_closed")
        )
        is True,
        "seal_command_blocked_reason": metric_or_summary("seal_command_blocked_reason") or "",
        "pressure_control_started_after_seal_confirmed": _as_bool(
            metric_or_summary("pressure_control_started_after_seal_confirmed")
        )
        is True,
        "setpoint_command_blocked_before_seal": _as_bool(
            metric_or_summary("setpoint_command_blocked_before_seal")
        )
        is True,
        "output_enable_blocked_before_seal": _as_bool(
            metric_or_summary("output_enable_blocked_before_seal")
        )
        is True,
        "normal_atmosphere_vent_attempted_after_pressure_points_started": _as_bool(
            metric_or_summary("normal_atmosphere_vent_attempted_after_pressure_points_started")
        )
        is True,
        "normal_atmosphere_vent_blocked_after_pressure_points_started": _as_bool(
            metric_or_summary("normal_atmosphere_vent_blocked_after_pressure_points_started")
        )
        is True,
        "emergency_relief_after_pressure_control_is_abort_only": _as_bool(
            metric_or_summary("emergency_relief_after_pressure_control_is_abort_only")
        )
        is True,
        "resume_after_emergency_relief_allowed": _as_bool(
            metric_or_summary("resume_after_emergency_relief_allowed")
        )
        is True,
        "emergency_abort_relief_vent_required": _as_bool(
            metric_or_summary("emergency_abort_relief_vent_required")
        )
        is True,
        "emergency_abort_relief_vent_allowed": _as_bool(
            metric_or_summary("emergency_abort_relief_vent_allowed")
        )
        is True,
        "emergency_abort_relief_vent_blocked_reason": metric_or_summary(
            "emergency_abort_relief_vent_blocked_reason"
        )
        or "",
        "emergency_abort_relief_vent_command_sent": _as_bool(
            metric_or_summary("emergency_abort_relief_vent_command_sent")
        )
        is True,
        "emergency_abort_relief_vent_phase": metric_or_summary("emergency_abort_relief_vent_phase") or "",
        "emergency_abort_relief_reason": metric_or_summary("emergency_abort_relief_reason") or "",
        "emergency_abort_relief_pressure_hpa": metric_or_summary("emergency_abort_relief_pressure_hpa"),
        "emergency_abort_relief_route_open": _as_bool(
            metric_or_summary("emergency_abort_relief_route_open")
        )
        is True,
        "emergency_abort_relief_seal_command_sent": _as_bool(
            metric_or_summary("emergency_abort_relief_seal_command_sent")
        )
        is True,
        "emergency_abort_relief_pressure_setpoint_command_sent": _as_bool(
            metric_or_summary("emergency_abort_relief_pressure_setpoint_command_sent")
        )
        is True,
        "emergency_abort_relief_sample_started": _as_bool(
            metric_or_summary("emergency_abort_relief_sample_started")
        )
        is True,
        "emergency_abort_relief_may_mix_air": _as_bool(
            metric_or_summary("emergency_abort_relief_may_mix_air")
        )
        is True,
        "normal_maintenance_vent_blocked_after_flush_phase": _as_bool(
            metric_or_summary("normal_maintenance_vent_blocked_after_flush_phase")
        )
        is True,
        "cleanup_vent_classification": metric_or_summary("cleanup_vent_classification") or "",
        "cleanup_vent_requested": _as_bool(metric_or_summary("cleanup_vent_requested")) is True,
        "cleanup_vent_phase": metric_or_summary("cleanup_vent_phase") or "",
        "cleanup_vent_reason": metric_or_summary("cleanup_vent_reason") or "",
        "cleanup_vent_allowed": _as_bool(metric_or_summary("cleanup_vent_allowed")) is True,
        "cleanup_vent_blocked_reason": metric_or_summary("cleanup_vent_blocked_reason") or "",
        "cleanup_vent_is_normal_maintenance": _as_bool(
            metric_or_summary("cleanup_vent_is_normal_maintenance")
        )
        is True,
        "cleanup_vent_is_safe_stop_relief": _as_bool(
            metric_or_summary("cleanup_vent_is_safe_stop_relief")
        )
        is True,
        "safe_stop_relief_required": _as_bool(metric_or_summary("safe_stop_relief_required")) is True,
        "safe_stop_relief_allowed": _as_bool(metric_or_summary("safe_stop_relief_allowed")) is True,
        "safe_stop_relief_command_sent": _as_bool(
            metric_or_summary("safe_stop_relief_command_sent")
        )
        is True,
        "safe_stop_relief_blocked_reason": metric_or_summary("safe_stop_relief_blocked_reason") or "",
        "vent_blocked_after_flush_phase_is_failure": _as_bool(
            metric_or_summary("vent_blocked_after_flush_phase_is_failure")
        )
        is True,
        "vent_blocked_after_flush_phase_context": metric_or_summary(
            "vent_blocked_after_flush_phase_context"
        )
        or {},
        "safe_stop_pressure_relief_result": metric_or_summary("safe_stop_pressure_relief_result") or "",
        "vent_pulse_blocked_after_flush_phase": raw_vent_blocked_after_flush,
        "vent_pulse_blocked_reason": metric_or_summary("vent_pulse_blocked_reason") or "",
        "attempted_unsafe_vent_after_seal_or_pressure_control": _as_bool(
            metric_or_summary("attempted_unsafe_vent_after_seal_or_pressure_control")
        )
        is True,
        "unsafe_vent_after_seal_or_pressure_control_command_sent": unsafe_vent_command_sent,
        "vent_off_blocked_during_flush": (
            metric_or_summary("vent_off_blocked_during_flush") is None
            or _as_bool(metric_or_summary("vent_off_blocked_during_flush")) is True
        ),
        "seal_blocked_during_flush": (
            metric_or_summary("seal_blocked_during_flush") is None
            or _as_bool(metric_or_summary("seal_blocked_during_flush")) is True
        ),
        "pressure_setpoint_blocked_during_flush": (
            metric_or_summary("pressure_setpoint_blocked_during_flush") is None
            or _as_bool(metric_or_summary("pressure_setpoint_blocked_during_flush")) is True
        ),
        "sample_blocked_during_flush": (
            metric_or_summary("sample_blocked_during_flush") is None
            or _as_bool(metric_or_summary("sample_blocked_during_flush")) is True
        ),
        "a2_pressure_sweep_executed": bool(executed),
        "a2_pressure_sweep_execution_started": bool(execution_started),
        "real_probe_executed": bool(execution_started),
        "underlying_execution_dir": str(execution.get("execution_run_dir") or ""),
        "execution_error": execution_error,
        "execution_config_path": execution_config_path,
        "co2_only": True,
        "skip0": True,
        "single_route": True,
        "single_temperature": True,
        "pressure_points_hpa": list(A2_ALLOWED_PRESSURE_POINTS_HPA),
        "pressure_points_expected": len(A2_ALLOWED_PRESSURE_POINTS_HPA),
        "pressure_points_completed": int(pressure_points_completed),
        "completed_pressure_points_hpa": [
            float(point["target_pressure_hpa"])
            for point in point_results
            if _as_bool(point.get("point_completed")) is True
        ],
        "points_completed": int(pressure_points_completed),
        "sample_count_total": int(sample_count_total),
        "sample_count_by_pressure": sample_count_by_pressure,
        "sample_min_count_per_pressure": sample_min_count,
        "all_pressure_points_have_fresh_pressure_before_sample": bool(all_have_fresh),
        "all_pressure_points_have_samples": bool(all_have_samples),
        "all_pressure_points_completed": bool(all_completed),
        "all_pressure_points_pressure_ready": bool(all_ready),
        "all_pressure_points_heartbeat_ready_before_sample": bool(all_heartbeat),
        "all_pressure_points_route_conditioning_ready_before_sample": bool(all_route),
        "a3_allowed": False,
        "artifact_paths": artifact_paths,
        **points_alignment,
        **safety_assertions,
    }
    operator_record = _build_operator_record(
        admission,
        operator_confirmation_path=operator_confirmation_path,
    )
    admission_record = _build_probe_admission_record(
        admission,
        artifact_paths=artifact_paths,
        config_path=config_path,
        branch=branch,
        head=head,
        cli_allow=cli_allow,
        execute_probe=execute_probe,
        run_app_py_untouched=run_app_py_untouched,
    )

    _json_dump(run_dir / "probe_admission_record.json", admission_record)
    trace_guard_write_stats = {
        "a2_pressure_sweep_trace": _jsonl_dump(
            run_dir / "a2_pressure_sweep_trace.jsonl",
            sweep_rows,
            trace_name="a2_pressure_sweep_trace",
        ),
        "route_trace": _jsonl_dump(run_dir / "route_trace.jsonl", route_rows, trace_name="route_trace"),
        "pressure_trace": _jsonl_dump(run_dir / "pressure_trace.jsonl", pressure_rows, trace_name="pressure_trace"),
        "pressure_ready_trace": _jsonl_dump(
            run_dir / "pressure_ready_trace.jsonl",
            pressure_ready_rows,
            trace_name="pressure_ready_trace",
        ),
        "heartbeat_trace": _jsonl_dump(run_dir / "heartbeat_trace.jsonl", heartbeat_rows, trace_name="heartbeat_trace"),
        "analyzer_sampling_rows": _jsonl_dump(
            run_dir / "analyzer_sampling_rows.jsonl",
            sample_rows,
            trace_name="analyzer_sampling_rows",
        ),
    }
    loaded_trace_rows_for_guard = [
        row
        for rows in (route_rows, pressure_rows, pressure_ready_rows, heartbeat_rows, sample_rows, sweep_rows)
        for row in rows
        if isinstance(row, Mapping)
    ]
    summary["trace_guard_summary"] = trace_guard_write_stats
    summary.update(_merge_trace_guard_stats(trace_guard_write_stats, loaded_trace_rows_for_guard))
    _json_dump(run_dir / "point_results.json", {"points": point_results})
    _write_point_results_csv(run_dir / "point_results.csv", point_results)
    _json_dump(run_dir / "safety_assertions.json", safety_assertions)
    _json_dump(run_dir / "operator_confirmation_record.json", operator_record)
    completeness = _artifact_completeness(
        artifact_paths,
        assume_present_keys={"summary", "process_exit_record"},
    )
    summary.update(completeness)
    if not completeness["artifact_completeness_pass"]:
        summary["final_decision"] = "FAIL_CLOSED"
        summary["a3_allowed"] = False
        summary["fail_closed_reason"] = "required_artifacts_missing"
        summary["rejection_reasons"] = list(
            dict.fromkeys([*summary["rejection_reasons"], "required_artifacts_missing"])
        )
    _json_dump(run_dir / "summary.json", summary)
    _write_process_exit_record(
        run_dir,
        artifact_paths=artifact_paths,
        process_state="interrupted" if execution_interrupted else "completed",
        final_decision=str(summary.get("final_decision") or final_decision),
        interrupted_execution=execution_interrupted,
        interruption_source=interruption_source,
        interruption_stage=interruption_stage,
        fail_closed_reason=str(summary.get("fail_closed_reason") or ""),
        execution_error=execution_error,
        real_probe_executed=bool(execution_started),
        real_com_opened=real_com_opened_value,
        any_device_command_sent=any_device_command_sent_value,
        any_write_command_sent=any_write_command_sent_value,
        safe_stop_triggered=safe_stop_triggered_value,
        no_write_assertion_status=no_write_assertion_status,
    )
    return summary
