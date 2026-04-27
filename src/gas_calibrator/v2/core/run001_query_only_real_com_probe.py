from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import json
import os
from pathlib import Path
from typing import Any, Callable, Mapping, Optional


QUERY_ONLY_REAL_COM_ENV_VAR = "GAS_CAL_V2_QUERY_ONLY_REAL_COM"
QUERY_ONLY_REAL_COM_ENV_VALUE = "1"
QUERY_ONLY_REAL_COM_CLI_FLAG = "--allow-v2-query-only-real-com"
QUERY_ONLY_SCHEMA_VERSION = "v2.run001.query_only_real_com_probe.1"
QUERY_ONLY_EVIDENCE_MARKERS = {
    "evidence_source": "real_probe_query_only",
    "not_real_acceptance_evidence": True,
    "acceptance_level": "engineering_probe_only",
    "promotion_state": "blocked",
    "real_primary_latest_refresh": False,
    "attempted_write_count": 0,
    "identity_write_command_sent": False,
    "calibration_write_command_sent": False,
    "senco_write_command_sent": False,
    "route_open_command_sent": False,
    "relay_output_command_sent": False,
    "valve_command_sent": False,
    "pressure_setpoint_command_sent": False,
    "vent_off_sent": False,
    "seal_command_sent": False,
    "high_pressure_started": False,
    "sample_count": 0,
    "points_completed": 0,
}
REQUIRED_OPERATOR_FIELDS = (
    "operator_name",
    "timestamp",
    "branch",
    "HEAD",
    "config_path",
    "port_manifest",
    "explicit_acknowledgement",
)
REQUIRED_OPERATOR_ACKS = (
    "query_only",
    "no_write",
    "no_route_open",
    "no_relay_output",
    "no_valve_command",
    "no_pressure_setpoint",
    "no_seal",
    "no_vent_off",
    "no_high_pressure",
    "no_sample",
    "not_real_acceptance",
    "v1_fallback_required",
)
BLOCKED_COMMAND_TOKENS = (
    "SENCO",
    "COEFF",
    "CAL",
    "SET",
    "WRITE",
    "SAVE",
    "STORE",
    "COMMIT",
    "MODE",
    "ID,",
    "VALVE",
    "RELAY",
    "OUTP",
    "SOUR",
)
DEVICE_ORDER = (
    "pressure_controller",
    "pressure_gauge",
    "temperature_chamber",
    "thermometer",
    "relay",
    "relay_8",
    "dewpoint_meter",
    "humidity_generator",
)
H2O_DEVICE_NAMES = {"dewpoint_meter", "humidity_generator"}


@dataclass(frozen=True)
class QueryOnlyRealComAdmission:
    approved: bool
    reasons: tuple[str, ...]
    evidence: dict[str, Any]
    operator_confirmation: dict[str, Any]
    device_inventory: list[dict[str, Any]]

    def to_dict(self) -> dict[str, Any]:
        return {
            "approved": self.approved,
            "reasons": list(self.reasons),
            "evidence": dict(self.evidence),
            "operator_confirmation": dict(self.operator_confirmation),
            "device_inventory": list(self.device_inventory),
        }


def load_json_mapping(path: str | Path) -> dict[str, Any]:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(payload, Mapping):
        raise ValueError(f"JSON payload must be an object: {path}")
    return dict(payload)


def _json_dump(path: Path, payload: Mapping[str, Any] | list[dict[str, Any]]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _as_bool(value: Any) -> Optional[bool]:
    if isinstance(value, bool):
        return value
    if value is None or value == "":
        return None
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "on"}:
        return True
    if text in {"0", "false", "no", "off"}:
        return False
    return None


def _section(value: Any, name: str) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        return {}
    candidate = value.get(name)
    return dict(candidate) if isinstance(candidate, Mapping) else {}


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


def _scope(raw_cfg: Mapping[str, Any]) -> str:
    return str(_first_value(raw_cfg, ("scope", "config.scope", "real_com_probe.scope", "r0.scope")) or "").lower()


def _h2o_disabled(raw_cfg: Mapping[str, Any]) -> bool:
    if _truthy(raw_cfg, ("h2o_enabled", "real_com_probe.h2o_enabled", "r0.h2o_enabled")):
        return False
    if _explicit_false(raw_cfg, ("h2o_enabled", "real_com_probe.h2o_enabled", "r0.h2o_enabled")):
        return True
    dewpoint = _as_bool(_path_value(raw_cfg, "devices.dewpoint_meter.enabled"))
    humidity = _as_bool(_path_value(raw_cfg, "devices.humidity_generator.enabled"))
    return dewpoint is False and humidity is False


def _real_primary_latest_refresh_disabled(raw_cfg: Mapping[str, Any]) -> bool:
    return _explicit_false(
        raw_cfg,
        (
            "real_primary_latest_refresh",
            "real_primary_latest_refresh_enabled",
            "real_primary_latest.refresh",
            "real_com_probe.real_primary_latest_refresh",
            "r0.real_primary_latest_refresh",
        ),
    )


def _device_entries(raw_cfg: Mapping[str, Any]) -> list[dict[str, Any]]:
    devices = _section(raw_cfg, "devices")
    entries: list[dict[str, Any]] = []
    for name in DEVICE_ORDER:
        device = devices.get(name)
        if not isinstance(device, Mapping):
            continue
        enabled = _as_bool(device.get("enabled"))
        enabled = True if enabled is None else bool(enabled)
        is_h2o = name in H2O_DEVICE_NAMES
        entries.append(
            {
                "device_name": name,
                "device_type": name,
                "enabled": enabled,
                "h2o_device": is_h2o,
                "port": str(device.get("port") or ""),
                "baud": device.get("baud"),
                "timeout_s": device.get("timeout", device.get("response_timeout_s", 1.0)),
                "will_open": bool(enabled and not is_h2o),
                "read_only": True,
            }
        )
    for item in devices.get("gas_analyzers") or []:
        if not isinstance(item, Mapping):
            continue
        enabled = _as_bool(item.get("enabled"))
        enabled = True if enabled is None else bool(enabled)
        entries.append(
            {
                "device_name": str(item.get("name") or "gas_analyzer"),
                "device_type": "gas_analyzer",
                "enabled": enabled,
                "h2o_device": False,
                "port": str(item.get("port") or ""),
                "baud": item.get("baud"),
                "timeout_s": item.get("timeout", 1.0),
                "device_id": str(item.get("device_id") or ""),
                "will_open": bool(enabled),
                "read_only": True,
            }
        )
    return entries


def _validate_operator_confirmation(
    path: Optional[str | Path],
    *,
    expected_branch: str = "",
    expected_head: str = "",
    expected_config_path: str = "",
) -> tuple[dict[str, Any], list[str]]:
    if not path:
        return {}, ["missing_operator_confirmation_json"]
    confirmation_path = Path(path)
    if not confirmation_path.exists():
        return {}, ["missing_operator_confirmation_json"]
    try:
        payload = load_json_mapping(confirmation_path)
    except Exception:
        return {}, ["invalid_operator_confirmation_json"]
    reasons: list[str] = []
    for field in REQUIRED_OPERATOR_FIELDS:
        if payload.get(field) in (None, ""):
            reasons.append(f"operator_confirmation_missing_{field}")
    acks = payload.get("explicit_acknowledgement")
    if not isinstance(acks, Mapping):
        reasons.append("operator_confirmation_missing_explicit_acknowledgement")
        acks = {}
    for ack in REQUIRED_OPERATOR_ACKS:
        if _as_bool(acks.get(ack)) is not True:
            reasons.append(f"operator_ack_missing_{ack}")
    if expected_branch and str(payload.get("branch") or "") != expected_branch:
        reasons.append("operator_confirmation_branch_mismatch")
    if expected_head and str(payload.get("HEAD") or "") != expected_head:
        reasons.append("operator_confirmation_head_mismatch")
    if expected_config_path and Path(str(payload.get("config_path") or "")).resolve() != Path(expected_config_path).resolve():
        reasons.append("operator_confirmation_config_path_mismatch")
    return payload, reasons


def evaluate_query_only_real_com_gate(
    raw_cfg: Mapping[str, Any],
    *,
    cli_allow: bool = False,
    env: Optional[Mapping[str, str]] = None,
    operator_confirmation_path: Optional[str | Path] = None,
    branch: str = "",
    head: str = "",
    config_path: str = "",
) -> QueryOnlyRealComAdmission:
    env_map = os.environ if env is None else env
    reasons: list[str] = []
    if not cli_allow:
        reasons.append("missing_cli_flag_allow_v2_query_only_real_com")
    if str(env_map.get(QUERY_ONLY_REAL_COM_ENV_VAR, "")).strip() != QUERY_ONLY_REAL_COM_ENV_VALUE:
        reasons.append("missing_env_gas_cal_v2_query_only_real_com")
    confirmation, confirmation_reasons = _validate_operator_confirmation(
        operator_confirmation_path,
        expected_branch=branch,
        expected_head=head,
        expected_config_path=config_path,
    )
    reasons.extend(confirmation_reasons)

    scope = _scope(raw_cfg)
    if scope not in {"query_only", "r0_query_only", "device_inventory_query_only"}:
        reasons.append("config_scope_not_query_only")
    if not _truthy(raw_cfg, ("query_only", "r0.query_only", "real_com_probe.query_only")):
        reasons.append("config_query_only_not_true")
    if not _truthy(raw_cfg, ("no_write", "r0.no_write", "real_com_probe.no_write")):
        reasons.append("config_no_write_not_true")
    if not _h2o_disabled(raw_cfg):
        reasons.append("config_h2o_not_disabled")
    if not _explicit_false(raw_cfg, ("full_group_enabled", "r0.full_group_enabled", "real_com_probe.full_group_enabled")):
        reasons.append("config_full_group_not_disabled")
    for name in (
        "route_open_enabled",
        "sample_enabled",
        "relay_output_enabled",
        "valve_command_enabled",
        "pressure_setpoint_enabled",
        "vent_off_enabled",
        "seal_enabled",
        "high_pressure_enabled",
        "a1r_enabled",
        "a2_enabled",
        "a3_enabled",
        "analyzer_id_write_enabled",
        "mode_switch_enabled",
        "senco_write_enabled",
        "calibration_write_enabled",
    ):
        if not _explicit_false(raw_cfg, (name, f"r0.{name}", f"real_com_probe.{name}")):
            reasons.append(f"config_{name}_not_disabled")
    if not _real_primary_latest_refresh_disabled(raw_cfg):
        reasons.append("config_real_primary_latest_refresh_not_disabled")

    inventory = _device_entries(raw_cfg)
    if not any(entry.get("will_open") for entry in inventory):
        reasons.append("config_no_enabled_query_only_ports")

    approved = not reasons
    evidence = {
        **QUERY_ONLY_EVIDENCE_MARKERS,
        "admission_approved": approved,
        "query_only": True,
        "dry_admission_only": True,
        "real_com_opened": False,
        "real_probe_executed": False,
        "operator_confirmation_recorded": bool(confirmation),
        "device_count": len(inventory),
        "ports_to_query_count": sum(1 for entry in inventory if entry.get("will_open")),
        "blocked_capabilities": {
            "route_open": True,
            "relay_output": True,
            "valve_command": True,
            "pressure_setpoint": True,
            "vent_off": True,
            "seal": True,
            "high_pressure": True,
            "sample": True,
            "h2o": True,
            "full_group": True,
            "a1r": True,
            "a2": True,
            "a3": True,
            "real_primary_latest_refresh": True,
        },
        "rejection_reasons": list(dict.fromkeys(reasons)),
    }
    return QueryOnlyRealComAdmission(
        approved=approved,
        reasons=tuple(dict.fromkeys(reasons)),
        evidence=evidence,
        operator_confirmation=confirmation,
        device_inventory=inventory,
    )


def _safe_read_commands(device: Mapping[str, Any], raw_cfg: Mapping[str, Any]) -> list[dict[str, Any]]:
    device_type = str(device.get("device_type") or "")
    commands: list[str] = []
    if device_type == "pressure_controller":
        pressure_queries = _path_value(raw_cfg, "devices.pressure_controller.pressure_queries")
        commands.extend(str(item) for item in pressure_queries or [])
        commands.insert(0, "*IDN?")
    elif device_type in {"pressure_gauge", "gas_analyzer", "thermometer"}:
        commands.append("<read_frame>")
    elif device_type == "temperature_chamber":
        commands.extend(["PV?", "SV?"])
    elif device_type in {"relay", "relay_8"}:
        commands.append("<open_close_only>")
    else:
        commands.append("<unsupported>")
    out: list[dict[str, Any]] = []
    for command in commands:
        command_text = str(command)
        safe = (
            command_text.startswith("<")
            or command_text.endswith("?")
        ) and not any(token in command_text.upper() for token in BLOCKED_COMMAND_TOKENS)
        out.append(
            {
                "command": command_text,
                "read_only": bool(safe),
                "supported": bool(safe and command_text != "<unsupported>"),
            }
        )
    return out


def _trace_row(device: Mapping[str, Any], *, action: str, result: str, details: Optional[Mapping[str, Any]] = None) -> dict[str, Any]:
    return {
        "timestamp": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
        "device_name": device.get("device_name"),
        "device_type": device.get("device_type"),
        "port": device.get("port"),
        "action": action,
        "result": result,
        "details": dict(details or {}),
    }


def _default_output_dir(config_path: str | Path) -> Path:
    timestamp = datetime.now().astimezone().strftime("%Y%m%d_%H%M%S")
    config = Path(config_path).expanduser().resolve() if config_path else Path.cwd()
    return config.parents[1] / "output" / "run001_r0_query_only_real_com" / f"run_{timestamp}"


def _open_serial_default(device: Mapping[str, Any]) -> Any:
    import serial  # type: ignore[import-not-found]

    return serial.Serial(
        port=str(device.get("port") or ""),
        baudrate=int(device.get("baud") or 9600),
        timeout=float(device.get("timeout_s") or 1.0),
    )


def _execute_device_query(
    device: Mapping[str, Any],
    raw_cfg: Mapping[str, Any],
    serial_factory: Callable[[Mapping[str, Any]], Any],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    trace: list[dict[str, Any]] = []
    results: list[dict[str, Any]] = []
    handle: Any = None
    try:
        handle = serial_factory(device)
        trace.append(_trace_row(device, action="open", result="ok"))
        for command in _safe_read_commands(device, raw_cfg):
            if not command.get("supported"):
                results.append({**dict(device), **command, "result": "unsupported", "raw_response": ""})
                continue
            command_text = str(command.get("command") or "")
            raw_response = b""
            if command_text == "<read_frame>":
                raw_response = handle.readline()
            elif command_text == "<open_close_only>":
                raw_response = b""
            else:
                handle.write((command_text + "\n").encode("ascii"))
                raw_response = handle.readline()
            results.append(
                {
                    **dict(device),
                    **command,
                    "result": "available" if raw_response else "unavailable",
                    "raw_response": raw_response.decode("utf-8", errors="replace") if raw_response else "",
                }
            )
    except (PermissionError, OSError) as exc:
        trace.append(_trace_row(device, action="open", result="occupied", details={"error": str(exc)}))
        results.append({**dict(device), "result": "occupied_port", "error": str(exc)})
    except Exception as exc:
        trace.append(_trace_row(device, action="query", result="unavailable", details={"error": str(exc)}))
        results.append({**dict(device), "result": "unavailable", "error": str(exc)})
    finally:
        if handle is not None:
            try:
                handle.close()
                trace.append(_trace_row(device, action="close", result="ok"))
            except Exception as exc:
                trace.append(_trace_row(device, action="close", result="error", details={"error": str(exc)}))
    return trace, results


def write_query_only_real_com_probe_artifacts(
    raw_cfg: Mapping[str, Any],
    *,
    output_dir: Optional[str | Path] = None,
    config_path: str | Path = "",
    cli_allow: bool = False,
    env: Optional[Mapping[str, str]] = None,
    operator_confirmation_path: Optional[str | Path] = None,
    branch: str = "",
    head: str = "",
    execute_query_only: bool = False,
    serial_factory: Optional[Callable[[Mapping[str, Any]], Any]] = None,
) -> dict[str, Any]:
    admission = evaluate_query_only_real_com_gate(
        raw_cfg,
        cli_allow=cli_allow,
        env=env,
        operator_confirmation_path=operator_confirmation_path,
        branch=branch,
        head=head,
        config_path=str(config_path or ""),
    )
    run_dir = Path(output_dir).expanduser().resolve() if output_dir else _default_output_dir(config_path)
    run_dir.mkdir(parents=True, exist_ok=True)
    trace_rows: list[dict[str, Any]] = []
    query_results: list[dict[str, Any]] = []
    opened_any = False

    if admission.approved and execute_query_only:
        factory = serial_factory or _open_serial_default
        for device in admission.device_inventory:
            if not device.get("will_open"):
                continue
            device_trace, device_results = _execute_device_query(device, raw_cfg, factory)
            trace_rows.extend(device_trace)
            query_results.extend(device_results)
        opened_any = any(row.get("action") == "open" and row.get("result") == "ok" for row in trace_rows)
    else:
        for device in admission.device_inventory:
            if not device.get("will_open"):
                continue
            trace_rows.append(_trace_row(device, action="dry_admission_no_open", result="not_executed"))
            for command in _safe_read_commands(device, raw_cfg):
                query_results.append({**dict(device), **command, "result": "admission_only_not_queried", "raw_response": ""})

    occupied_ports = [row for row in query_results if row.get("result") == "occupied_port"]
    query_failures = [
        row
        for row in query_results
        if execute_query_only
        and (
            row.get("result") in {"unsupported", "unavailable"}
            and row.get("command") != "<open_close_only>"
        )
    ]
    final_decision = (
        "FAIL_CLOSED"
        if admission.reasons or occupied_ports or query_failures
        else ("PASS" if execute_query_only else "ADMISSION_APPROVED")
    )
    artifact_paths = {
        "summary": str(run_dir / "summary.json"),
        "device_inventory": str(run_dir / "device_inventory.json"),
        "query_results": str(run_dir / "query_results.json"),
        "port_open_close_trace": str(run_dir / "port_open_close_trace.jsonl"),
        "operator_confirmation_record": str(run_dir / "operator_confirmation_record.json"),
    }
    summary = {
        "schema_version": QUERY_ONLY_SCHEMA_VERSION,
        **QUERY_ONLY_EVIDENCE_MARKERS,
        "final_decision": final_decision,
        "admission_approved": admission.approved,
        "execute_query_only": bool(execute_query_only),
        "real_com_opened": bool(opened_any),
        "real_probe_executed": bool(opened_any),
        "operator_confirmation_recorded": bool(admission.operator_confirmation),
        "rejection_reasons": list(admission.reasons),
        "occupied_port_seen": bool(occupied_ports),
        "occupied_ports": occupied_ports,
        "query_failure_seen": bool(query_failures),
        "query_failures": query_failures,
        "device_count": len(admission.device_inventory),
        "query_result_count": len(query_results),
        "artifact_paths": artifact_paths,
    }
    _json_dump(run_dir / "summary.json", summary)
    _json_dump(run_dir / "device_inventory.json", admission.device_inventory)
    _json_dump(run_dir / "query_results.json", query_results)
    (run_dir / "port_open_close_trace.jsonl").write_text(
        "\n".join(json.dumps(row, ensure_ascii=False) for row in trace_rows) + ("\n" if trace_rows else ""),
        encoding="utf-8",
    )
    _json_dump(run_dir / "operator_confirmation_record.json", admission.operator_confirmation)
    return summary
