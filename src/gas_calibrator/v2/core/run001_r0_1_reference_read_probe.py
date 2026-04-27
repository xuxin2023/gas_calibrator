from __future__ import annotations

from datetime import datetime, timezone
import json
import os
from pathlib import Path
import time
from typing import Any, Callable, Mapping, Optional

from gas_calibrator.devices.paroscientific import ParoscientificGauge
from gas_calibrator.devices.temperature_chamber import TemperatureChamber


R0_1_SCHEMA_VERSION = "v2.run001.r0_1_reference_read_probe.1"
R0_1_ENV_VAR = "GAS_CAL_V2_QUERY_ONLY_REAL_COM"
R0_1_ENV_VALUE = "1"
R0_1_EVIDENCE_MARKERS = {
    "evidence_source": "real_probe_r0_1_reference_read_only",
    "acceptance_level": "engineering_probe_only",
    "promotion_state": "blocked",
    "not_real_acceptance_evidence": True,
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


def load_json_mapping(path: str | Path) -> dict[str, Any]:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(payload, Mapping):
        raise ValueError(f"JSON payload must be an object: {path}")
    return dict(payload)


def _json_dump(path: Path, payload: Mapping[str, Any] | list[dict[str, Any]]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _section(value: Any, name: str) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        return {}
    candidate = value.get(name)
    return dict(candidate) if isinstance(candidate, Mapping) else {}


def _device_cfg(raw_cfg: Mapping[str, Any], name: str) -> dict[str, Any]:
    devices = _section(raw_cfg, "devices")
    value = devices.get(name)
    return dict(value) if isinstance(value, Mapping) else {}


def _path_value(raw_cfg: Mapping[str, Any], dotted_path: str) -> Any:
    current: Any = raw_cfg
    for part in dotted_path.split("."):
        if not isinstance(current, Mapping) or part not in current:
            return None
        current = current.get(part)
    return current


def _trace_row(
    *,
    device_name: str,
    device_type: str,
    port: str,
    action: str,
    result: str,
    details: Optional[Mapping[str, Any]] = None,
) -> dict[str, Any]:
    return {
        "timestamp": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
        "device_name": device_name,
        "device_type": device_type,
        "port": port,
        "action": action,
        "result": result,
        "details": dict(details or {}),
    }


def _default_output_dir(config_path: str | Path) -> Path:
    timestamp = datetime.now().astimezone().strftime("%Y%m%d_%H%M%S")
    config = Path(config_path).expanduser().resolve() if config_path else Path.cwd()
    return config.parents[1] / "output" / "run001_r0_1_reference_read_probe" / f"run_{timestamp}"


def _to_bytes(value: Any) -> bytes:
    if value is None:
        return b""
    if isinstance(value, bytes):
        return value
    return str(value).encode("utf-8", errors="replace")


def _decode_ascii_preview(raw: bytes, limit: int = 160) -> str:
    return raw[:limit].decode("ascii", errors="replace")


def _hex_preview(raw: bytes, limit: int = 80) -> str:
    return " ".join(f"{byte:02X}" for byte in raw[:limit])


def _pressure_device(raw_cfg: Mapping[str, Any]) -> dict[str, Any]:
    cfg = _device_cfg(raw_cfg, "pressure_gauge")
    return {
        "device_name": "pressure_gauge",
        "device_type": "pressure_gauge",
        "port": str(cfg.get("port") or "COM30"),
        "baud": int(cfg.get("baud") or cfg.get("baudrate") or 9600),
        "timeout_s": float(cfg.get("timeout") or 1.0),
        "response_timeout_s": float(cfg.get("response_timeout_s") or 2.2),
        "dest_id": str(cfg.get("dest_id") or "01"),
        "read_only": True,
    }


def _chamber_device(raw_cfg: Mapping[str, Any]) -> dict[str, Any]:
    cfg = _device_cfg(raw_cfg, "temperature_chamber")
    return {
        "device_name": "temperature_chamber",
        "device_type": "temperature_chamber",
        "port": str(cfg.get("port") or "COM27"),
        "baud": int(cfg.get("baud") or cfg.get("baudrate") or 9600),
        "addr": int(cfg.get("addr") or 1),
        "read_only": True,
        "protocol_candidate": "modbus_rtu",
    }


def _open_serial_default(device: Mapping[str, Any]) -> Any:
    import serial  # type: ignore[import-not-found]

    return serial.Serial(
        port=str(device.get("port") or ""),
        baudrate=int(device.get("baud") or 9600),
        timeout=float(device.get("timeout_s") or 1.0),
    )


def _read_raw_window(
    handle: Any,
    *,
    read_window_s: float,
    poll_interval_s: float,
    max_reads: int,
) -> bytes:
    chunks: list[bytes] = []
    deadline = time.monotonic() + max(0.0, float(read_window_s))
    reads = 0
    while reads < max(1, int(max_reads)):
        reads += 1
        raw = b""
        waiting = int(getattr(handle, "in_waiting", 0) or 0)
        if waiting > 0 and hasattr(handle, "read"):
            raw = _to_bytes(handle.read(waiting))
        elif hasattr(handle, "readline"):
            raw = _to_bytes(handle.readline())
        if raw:
            chunks.append(raw)
        if time.monotonic() >= deadline:
            break
        if not raw:
            time.sleep(max(0.0, float(poll_interval_s)))
    return b"".join(chunks)


def _parse_pressure_capture(raw: bytes) -> tuple[str, Optional[float]]:
    if not raw:
        return "no_raw_bytes", None
    lines = [_decode_ascii_preview(line, 200).strip() for line in raw.splitlines()]
    value = ParoscientificGauge._parse_latest_pressure_lines(lines)
    if value is None:
        return "unparseable", None
    return "parse_ok", float(value)


def read_pressure_gauge_raw_capture(
    raw_cfg: Mapping[str, Any],
    *,
    serial_factory: Callable[[Mapping[str, Any]], Any],
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    device = _pressure_device(raw_cfg)
    read_window_s = float(_path_value(raw_cfg, "r0_1.pressure_gauge.read_window_s") or 0.35)
    poll_interval_s = float(_path_value(raw_cfg, "r0_1.pressure_gauge.poll_interval_s") or 0.02)
    max_reads = int(_path_value(raw_cfg, "r0_1.pressure_gauge.max_reads") or 40)
    trace: list[dict[str, Any]] = []
    handle: Any = None
    opened = False
    closed = False
    raw = b""
    error = ""
    try:
        handle = serial_factory(device)
        opened = True
        trace.append(_trace_row(device_name="pressure_gauge", device_type="pressure_gauge", port=device["port"], action="open", result="ok"))
        raw = _read_raw_window(handle, read_window_s=read_window_s, poll_interval_s=poll_interval_s, max_reads=max_reads)
    except (PermissionError, OSError) as exc:
        error = str(exc)
        trace.append(
            _trace_row(
                device_name="pressure_gauge",
                device_type="pressure_gauge",
                port=device["port"],
                action="open",
                result="occupied",
                details={"error": error},
            )
        )
    except Exception as exc:
        error = str(exc)
        trace.append(
            _trace_row(
                device_name="pressure_gauge",
                device_type="pressure_gauge",
                port=device["port"],
                action="read_raw",
                result="unavailable",
                details={"error": error},
            )
        )
    finally:
        if handle is not None:
            try:
                handle.close()
                closed = True
                trace.append(_trace_row(device_name="pressure_gauge", device_type="pressure_gauge", port=device["port"], action="close", result="ok"))
            except Exception as exc:
                trace.append(
                    _trace_row(
                        device_name="pressure_gauge",
                        device_type="pressure_gauge",
                        port=device["port"],
                        action="close",
                        result="error",
                        details={"error": str(exc)},
                    )
                )

    parser_status, pressure_hpa = _parse_pressure_capture(raw)
    if raw:
        stream_assessment = "continuous_output"
    elif error:
        stream_assessment = "read_unavailable"
    else:
        stream_assessment = "no_continuous_output_seen_query_response_unresolved"
    diagnostics = {
        **device,
        "driver_detected": True,
        "driver_name": "gas_calibrator.devices.paroscientific.ParoscientificGauge",
        "driver_read_only_methods": ["read_pressure", "read_pressure_fast"],
        "raw_capture_only": True,
        "read_window_s": read_window_s,
        "raw_bytes_len": len(raw),
        "raw_hex_preview": _hex_preview(raw),
        "raw_ascii_preview": _decode_ascii_preview(raw),
        "parser_status": parser_status,
        "pressure_hpa": pressure_hpa,
        "stream_mode_assessment": stream_assessment,
        "query_response_supported_by_driver": True,
        "state_changing_command_sent": False,
        "write_command_sent": False,
        "control_command_sent": False,
        "port_open_close_ok": bool(opened and closed),
        "error": error,
    }
    return diagnostics, trace


def _read_chamber_registers(chamber: TemperatureChamber) -> dict[str, Any]:
    current_temp_c = chamber.read_temp_c()
    set_temp_c = chamber.read_set_temp_c()
    return {
        "pv_current_temperature_c": float(current_temp_c),
        "sv_set_temperature_c": float(set_temp_c),
    }


def read_temperature_chamber_read_only(
    raw_cfg: Mapping[str, Any],
    *,
    client_factory: Callable[[Mapping[str, Any]], Any],
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    device = _chamber_device(raw_cfg)
    trace: list[dict[str, Any]] = []
    try:
        client = client_factory(device)
        chamber = TemperatureChamber(
            port=device["port"],
            baudrate=int(device["baud"]),
            addr=int(device["addr"]),
            client=client,
        )
    except Exception as exc:
        diagnostics = {
            **device,
            "driver_detected": True,
            "driver_name": "gas_calibrator.devices.temperature_chamber.TemperatureChamber",
            "protocol_candidate": "modbus_rtu",
            "protocol_status": "temperature_chamber_protocol_unresolved",
            "legacy_ascii_queries": ["PV?", "SV?"],
            "legacy_ascii_query_status": "unsupported_for_configured_modbus_driver",
            "read_only_registers_attempted": [],
            "write_register_sent": False,
            "write_coil_sent": False,
            "start_command_sent": False,
            "stop_command_sent": False,
            "set_temperature_command_sent": False,
            "control_command_sent": False,
            "port_open_close_ok": False,
            "error": str(exc),
        }
        trace.append(
            _trace_row(
                device_name="temperature_chamber",
                device_type="temperature_chamber",
                port=device["port"],
                action="create_modbus_client",
                result="unavailable",
                details={"error": str(exc)},
            )
        )
        return diagnostics, trace
    opened = False
    closed = False
    values: dict[str, Any] = {}
    status = "temperature_chamber_protocol_unresolved"
    error = ""
    read_methods: list[str] = []
    try:
        chamber.open()
        opened = True
        trace.append(_trace_row(device_name="temperature_chamber", device_type="temperature_chamber", port=device["port"], action="open", result="ok"))
        values = _read_chamber_registers(chamber)
        read_methods = ["read_input_registers(7991,1)", "read_holding_registers(8100,1)"]
        status = "read_only_driver_fallback_used"
    except Exception as exc:
        error = str(exc)
        trace.append(
            _trace_row(
                device_name="temperature_chamber",
                device_type="temperature_chamber",
                port=device["port"],
                action="read_modbus_registers",
                result="unavailable",
                details={"error": error},
            )
        )
    finally:
        try:
            chamber.close()
            closed = True
            trace.append(_trace_row(device_name="temperature_chamber", device_type="temperature_chamber", port=device["port"], action="close", result="ok"))
        except Exception as exc:
            trace.append(
                _trace_row(
                    device_name="temperature_chamber",
                    device_type="temperature_chamber",
                    port=device["port"],
                    action="close",
                    result="error",
                    details={"error": str(exc)},
                )
            )
    diagnostics = {
        **device,
        "driver_detected": True,
        "driver_name": "gas_calibrator.devices.temperature_chamber.TemperatureChamber",
        "protocol_candidate": "modbus_rtu",
        "protocol_status": status,
        "legacy_ascii_queries": ["PV?", "SV?"],
        "legacy_ascii_query_status": "unsupported_for_configured_modbus_driver",
        "read_only_registers_attempted": read_methods,
        "write_register_sent": False,
        "write_coil_sent": False,
        "start_command_sent": False,
        "stop_command_sent": False,
        "set_temperature_command_sent": False,
        "control_command_sent": False,
        "port_open_close_ok": bool(opened and closed),
        "error": error,
        **values,
    }
    return diagnostics, trace


def _default_chamber_client_factory(device: Mapping[str, Any]) -> Any:
    return TemperatureChamber(
        port=str(device.get("port") or ""),
        baudrate=int(device.get("baud") or 9600),
        addr=int(device.get("addr") or 1),
    ).client


def write_r0_1_reference_read_probe_artifacts(
    raw_cfg: Mapping[str, Any],
    *,
    output_dir: Optional[str | Path] = None,
    config_path: str | Path = "",
    cli_allow: bool = False,
    env: Optional[Mapping[str, str]] = None,
    execute_read_only: bool = False,
    pressure_serial_factory: Optional[Callable[[Mapping[str, Any]], Any]] = None,
    chamber_client_factory: Optional[Callable[[Mapping[str, Any]], Any]] = None,
) -> dict[str, Any]:
    env_map = os.environ if env is None else env
    run_dir = Path(output_dir).expanduser().resolve() if output_dir else _default_output_dir(config_path)
    run_dir.mkdir(parents=True, exist_ok=True)
    rejection_reasons: list[str] = []
    if execute_read_only and not cli_allow:
        rejection_reasons.append("missing_cli_flag_allow_v2_query_only_real_com")
    if execute_read_only and str(env_map.get(R0_1_ENV_VAR, "")).strip() != R0_1_ENV_VALUE:
        rejection_reasons.append("missing_env_gas_cal_v2_query_only_real_com")

    trace_rows: list[dict[str, Any]] = []
    if rejection_reasons or not execute_read_only:
        pressure_diag = {
            **_pressure_device(raw_cfg),
            "driver_detected": True,
            "driver_name": "gas_calibrator.devices.paroscientific.ParoscientificGauge",
            "raw_capture_only": True,
            "result": "not_executed",
            "state_changing_command_sent": False,
            "write_command_sent": False,
            "control_command_sent": False,
        }
        chamber_diag = {
            **_chamber_device(raw_cfg),
            "driver_detected": True,
            "driver_name": "gas_calibrator.devices.temperature_chamber.TemperatureChamber",
            "protocol_candidate": "modbus_rtu",
            "protocol_status": "not_executed",
            "legacy_ascii_query_status": "unsupported_for_configured_modbus_driver",
            "write_register_sent": False,
            "write_coil_sent": False,
            "control_command_sent": False,
        }
    else:
        pressure_diag, pressure_trace = read_pressure_gauge_raw_capture(
            raw_cfg,
            serial_factory=pressure_serial_factory or _open_serial_default,
        )
        chamber_diag, chamber_trace = read_temperature_chamber_read_only(
            raw_cfg,
            client_factory=chamber_client_factory or _default_chamber_client_factory,
        )
        trace_rows.extend(pressure_trace)
        trace_rows.extend(chamber_trace)

    pressure_unavailable = pressure_diag.get("parser_status") != "parse_ok"
    chamber_unavailable = chamber_diag.get("protocol_status") != "read_only_driver_fallback_used"
    r1_block_reasons: list[str] = []
    if rejection_reasons:
        r1_block_reasons.extend(rejection_reasons)
    if pressure_unavailable:
        r1_block_reasons.append("pressure_gauge_reference_unavailable")
    if chamber_unavailable:
        r1_block_reasons.append(str(chamber_diag.get("protocol_status") or "temperature_chamber_protocol_unresolved"))
    r1_block_reasons = list(dict.fromkeys(r1_block_reasons))
    final_decision = "FAIL_CLOSED" if r1_block_reasons else ("PASS" if execute_read_only else "ADMISSION_APPROVED")
    diagnostics = {
        "schema_version": R0_1_SCHEMA_VERSION,
        "pressure_gauge": pressure_diag,
        "temperature_chamber": chamber_diag,
        "r1_blocked": bool(r1_block_reasons),
        "r1_block_reasons": r1_block_reasons,
        "no_write": True,
    }
    artifact_paths = {
        "summary": str(run_dir / "summary.json"),
        "r0_1_reference_read_diagnostics": str(run_dir / "r0_1_reference_read_diagnostics.json"),
        "raw_capture_COM30_pressure_gauge": str(run_dir / "raw_capture_COM30_pressure_gauge.json"),
        "chamber_read_diagnostics_COM27": str(run_dir / "chamber_read_diagnostics_COM27.json"),
        "port_open_close_trace": str(run_dir / "port_open_close_trace.jsonl"),
    }
    summary = {
        "schema_version": R0_1_SCHEMA_VERSION,
        **R0_1_EVIDENCE_MARKERS,
        "final_decision": final_decision,
        "execute_read_only": bool(execute_read_only and not rejection_reasons),
        "real_com_opened": any(row.get("action") == "open" and row.get("result") == "ok" for row in trace_rows),
        "real_probe_executed": bool(execute_read_only and not rejection_reasons),
        "r1_conditioning_allowed": False,
        "r1_blocked": bool(r1_block_reasons),
        "r1_block_reasons": r1_block_reasons,
        "pressure_gauge_parser_status": pressure_diag.get("parser_status", "not_executed"),
        "temperature_chamber_protocol_status": chamber_diag.get("protocol_status", "not_executed"),
        "artifact_paths": artifact_paths,
    }
    _json_dump(run_dir / "summary.json", summary)
    _json_dump(run_dir / "r0_1_reference_read_diagnostics.json", diagnostics)
    _json_dump(run_dir / "raw_capture_COM30_pressure_gauge.json", pressure_diag)
    _json_dump(run_dir / "chamber_read_diagnostics_COM27.json", chamber_diag)
    (run_dir / "port_open_close_trace.jsonl").write_text(
        "\n".join(json.dumps(row, ensure_ascii=False) for row in trace_rows) + ("\n" if trace_rows else ""),
        encoding="utf-8",
    )
    return summary
