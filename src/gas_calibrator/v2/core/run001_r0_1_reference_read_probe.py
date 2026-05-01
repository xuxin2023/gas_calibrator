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
    "any_write_command_sent": False,
    "persistent_config_write_sent": False,
    "pressure_gauge_setting_write_sent": False,
    "identity_write_command_sent": False,
    "calibration_write_command_sent": False,
    "senco_write_command_sent": False,
    "route_open_command_sent": False,
    "relay_output_command_sent": False,
    "valve_command_sent": False,
    "pressure_setpoint_command_sent": False,
    "vent_off_command_sent": False,
    "vent_off_sent": False,
    "seal_command_sent": False,
    "high_pressure_command_sent": False,
    "high_pressure_started": False,
    "sample_started": False,
    "sample_count": 0,
    "points_completed": 0,
    "mode_switch_command_sent": False,
    "chamber_write_register_command_sent": False,
    "chamber_set_temperature_command_sent": False,
    "chamber_start_command_sent": False,
    "chamber_stop_command_sent": False,
    "r1_allowed": False,
    "a1r_allowed": False,
    "a2_allowed": False,
    "a3_allowed": False,
}
R0_1_ALLOWED_PORTS = {"COM30", "COM27"}
R0_1_REQUIRED_TRUE_ACKS = [
    "query_only",
    "read_only",
    "no_write",
    "no_route_open",
    "no_relay_output",
    "no_valve_command",
    "no_pressure_setpoint",
    "no_vent_off",
    "no_seal",
    "no_high_pressure",
    "no_sample",
    "no_mode_switch",
    "no_id_write",
    "no_senco_write",
    "no_calibration_write",
    "no_chamber_write_register",
    "no_chamber_set_temperature",
    "no_chamber_start",
    "no_chamber_stop",
    "not_real_acceptance",
    "engineering_probe_only",
    "v1_fallback_required",
]
R0_1_REQUIRED_FALSE_ACKS = ["real_primary_latest_refresh"]


def load_json_mapping(path: str | Path) -> dict[str, Any]:
    payload = json.loads(Path(path).read_text(encoding="utf-8-sig"))
    if not isinstance(payload, Mapping):
        raise ValueError(f"JSON payload must be an object: {path}")
    return dict(payload)


def _json_dump(path: Path, payload: Mapping[str, Any] | list[dict[str, Any]]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _jsonl_dump(path: Path, rows: list[Mapping[str, Any]]) -> None:
    path.write_text(
        "\n".join(json.dumps(dict(row), ensure_ascii=False) for row in rows) + ("\n" if rows else ""),
        encoding="utf-8",
    )


def load_json_mapping_utf8_no_bom(path: str | Path) -> dict[str, Any]:
    raw = Path(path).read_bytes()
    if raw.startswith(b"\xef\xbb\xbf"):
        raise ValueError(f"JSON file must be UTF-8 without BOM: {path}")
    payload = json.loads(raw.decode("utf-8"))
    if not isinstance(payload, Mapping):
        raise ValueError(f"JSON payload must be an object: {path}")
    return dict(payload)


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


def _normalize_port(value: Any) -> str:
    return str(value or "").strip().upper()


def _manifest_port(manifest: Mapping[str, Any], *names: str) -> str:
    for name in names:
        value = manifest.get(name)
        if value:
            return _normalize_port(value)
    return ""


def _operator_confirmation_validation(
    payload: Mapping[str, Any],
    raw_cfg: Mapping[str, Any],
    *,
    config_path: str | Path = "",
    branch: str = "",
    head: str = "",
) -> dict[str, Any]:
    errors: list[str] = []
    manifest = payload.get("port_manifest")
    ack = payload.get("explicit_acknowledgement")
    if not isinstance(manifest, Mapping):
        errors.append("missing_port_manifest")
        manifest = {}
    if not isinstance(ack, Mapping):
        errors.append("missing_explicit_acknowledgement")
        ack = {}

    expected_pressure_port = _normalize_port(_pressure_device(raw_cfg).get("port"))
    expected_chamber_port = _normalize_port(_chamber_device(raw_cfg).get("port"))
    pressure_port = _manifest_port(manifest, "pressure_gauge", "digital_gauge")
    chamber_port = _manifest_port(manifest, "temperature_chamber", "chamber")
    if pressure_port != expected_pressure_port:
        errors.append(f"pressure_gauge_port_mismatch:{pressure_port or '<missing>'}!={expected_pressure_port}")
    if chamber_port != expected_chamber_port:
        errors.append(f"temperature_chamber_port_mismatch:{chamber_port or '<missing>'}!={expected_chamber_port}")

    declared_allowed_ports = {
        _normalize_port(value)
        for value in manifest.get("allowed_ports", [])
        if _normalize_port(value)
    } if isinstance(manifest.get("allowed_ports"), list) else set()
    if declared_allowed_ports and declared_allowed_ports != R0_1_ALLOWED_PORTS:
        errors.append(f"allowed_ports_mismatch:{sorted(declared_allowed_ports)}")

    forbidden_manifest_keys = [
        "pace",
        "pressure_controller",
        "relay",
        "relay_1",
        "relay_2",
        "relay_8",
        "analyzers",
        "gas_analyzers",
        "thermometer",
    ]
    for key in forbidden_manifest_keys:
        value = manifest.get(key)
        if value in (None, "", [], {}, False):
            continue
        errors.append(f"forbidden_port_manifest_entry:{key}")
    h2o_devices = manifest.get("h2o_devices")
    if h2o_devices not in (None, "", False, "disabled", [], {}):
        errors.append("h2o_devices_must_be_disabled")

    for key in R0_1_REQUIRED_TRUE_ACKS:
        if ack.get(key) is not True:
            errors.append(f"ack_required_true:{key}")
    for key in R0_1_REQUIRED_FALSE_ACKS:
        if ack.get(key) is not False:
            errors.append(f"ack_required_false:{key}")

    payload_config_path = str(payload.get("config_path") or "")
    if config_path and payload_config_path and Path(payload_config_path) != Path(config_path):
        errors.append("config_path_mismatch")
    if branch and payload.get("branch") not in ("", None, branch):
        errors.append("branch_mismatch")
    if head and payload.get("HEAD") not in ("", None, head):
        errors.append("head_mismatch")

    return {
        "valid": not errors,
        "errors": errors,
        "allowed_ports": sorted(R0_1_ALLOWED_PORTS),
        "pressure_gauge_port": pressure_port,
        "temperature_chamber_port": chamber_port,
        "utf8_no_bom": True,
        "only_com30_com27": not errors and {pressure_port, chamber_port} == R0_1_ALLOWED_PORTS,
    }


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
    timeout_s = float(cfg.get("timeout") or 1.0)
    return {
        "device_name": "pressure_gauge",
        "device_type": "pressure_gauge",
        "port": str(cfg.get("port") or "COM30"),
        "baud": int(cfg.get("baud") or cfg.get("baudrate") or 9600),
        "timeout": timeout_s,
        "timeout_s": timeout_s,
        "response_timeout_s": float(cfg.get("response_timeout_s") or 2.2),
        "dest_id": str(cfg.get("dest_id") or "01"),
        "read_only": True,
    }


def _chamber_device(raw_cfg: Mapping[str, Any]) -> dict[str, Any]:
    cfg = _device_cfg(raw_cfg, "temperature_chamber")
    addr = int(cfg.get("addr") or cfg.get("slave") or cfg.get("unit") or cfg.get("unit_id") or 1)
    return {
        "device_name": "temperature_chamber",
        "device_type": "temperature_chamber",
        "port": str(cfg.get("port") or "COM27"),
        "baud": int(cfg.get("baud") or cfg.get("baudrate") or 9600),
        "baudrate": int(cfg.get("baud") or cfg.get("baudrate") or 9600),
        "parity": str(cfg.get("parity") or "N"),
        "stopbits": float(cfg.get("stopbits") or 1),
        "bytesize": int(cfg.get("bytesize") or 8),
        "timeout": float(cfg.get("timeout") or 1.0),
        "timeout_s": float(cfg.get("timeout") or 1.0),
        "addr": addr,
        "slave_id": addr,
        "unit_id": addr,
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


def _raw_lines_sample(raw: bytes, limit: int = 8) -> list[str]:
    if not raw:
        return []
    lines = _decode_ascii_preview(raw, 400).splitlines()
    return [line.strip() for line in lines[:limit] if line.strip()]


def _pressure_serial_factory_adapter(
    device: Mapping[str, Any],
    serial_factory: Callable[[Mapping[str, Any]], Any],
) -> Callable[..., Any]:
    def _factory(**kwargs: Any) -> Any:
        merged = dict(device)
        if "port" in kwargs:
            merged["port"] = kwargs.get("port")
        if "baudrate" in kwargs:
            merged["baud"] = kwargs.get("baudrate")
        if "timeout" in kwargs:
            merged["timeout_s"] = kwargs.get("timeout")
        if "parity" in kwargs:
            merged["parity"] = kwargs.get("parity")
        if "stopbits" in kwargs:
            merged["stopbits"] = kwargs.get("stopbits")
        if "bytesize" in kwargs:
            merged["bytesize"] = kwargs.get("bytesize")
        return serial_factory(merged)

    return _factory


def _new_pressure_gauge(
    device: Mapping[str, Any],
    serial_factory: Callable[[Mapping[str, Any]], Any],
) -> ParoscientificGauge:
    return ParoscientificGauge(
        str(device.get("port") or "COM30"),
        int(device.get("baud") or 9600),
        timeout=float(device.get("timeout_s") or 1.0),
        dest_id=str(device.get("dest_id") or "01"),
        response_timeout_s=float(device.get("response_timeout_s") or 2.2),
        serial_factory=_pressure_serial_factory_adapter(device, serial_factory),
    )


def _attempt_paroscientific_read(
    device: Mapping[str, Any],
    *,
    serial_factory: Callable[[Mapping[str, Any]], Any],
    method: str,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Attempt a Paroscientific P3 pressure read using the hardened retry helper."""
    trace: list[dict[str, Any]] = []
    gauge = _new_pressure_gauge(device, serial_factory)
    opened = False
    closed = False
    value: Optional[float] = None
    error = ""
    continuous_cancel_sent = False
    try:
        gauge.open()
        opened = True
        trace.append(_trace_row(device_name="pressure_gauge", device_type="pressure_gauge", port=str(device.get("port") or ""), action=f"{method}_open", result="ok"))

        # Use the hardened p3_read_with_retry which:
        #  1) sends P3 to cancel continuous mode (P4/P7)
        #  2) waits 300 ms for gauge to exit continuous mode
        #  3) drains buffer
        #  4) queries P3 with progressive retry (up to 3, +100 ms each)
        #  5) if all retries fail, performs full restart: cancel → wait → query
        value = float(
            gauge._p3_read_with_retry(
                cancel_wait_s=0.30,
                query_timeout_s=0.20,
                max_retries=3,
                retry_increment_s=0.10,
            )
            )
        continuous_cancel_sent = True
        trace.append(_trace_row(device_name="pressure_gauge", device_type="pressure_gauge", port=str(device.get("port") or ""), action=f"{method}_p3_read_with_retry", result="ok", details={"pressure_hpa": value}))
    except Exception as exc:
        error = str(exc)
        trace.append(
            _trace_row(
                device_name="pressure_gauge",
                device_type="pressure_gauge",
                port=str(device.get("port") or ""),
                action=method,
                result="unavailable",
                details={"error": error},
            )
        )
    finally:
        try:
            gauge.close()
            closed = True
            trace.append(_trace_row(device_name="pressure_gauge", device_type="pressure_gauge", port=str(device.get("port") or ""), action=f"{method}_close", result="ok"))
        except Exception as exc:
            trace.append(
                _trace_row(
                    device_name="pressure_gauge",
                    device_type="pressure_gauge",
                    port=str(device.get("port") or ""),
                    action=f"{method}_close",
                    result="error",
                    details={"error": str(exc)},
                )
            )
    return {
        "attempted": True,
        "succeeded": value is not None,
        "pressure_hpa": value,
        "error": error,
        "port_open_close_ok": bool(opened and closed),
        "continuous_cancel_sent": bool(continuous_cancel_sent),
        "pre_cancel_continuous_attempted": bool(continuous_cancel_sent),
        "p3_read_with_retry_used": True,
    }, trace


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
    generic_read_frame_failed = parser_status != "parse_ok"
    if raw:
        stream_assessment = "continuous_output"
    elif error:
        stream_assessment = "read_unavailable"
    else:
        stream_assessment = "no_continuous_output_seen_query_response_unresolved"

    p3_command_preview = _new_pressure_gauge(device, serial_factory)._cmd("P3")
    p3_result = {"attempted": False, "succeeded": False, "pressure_hpa": None, "error": "", "port_open_close_ok": False}
    fast_result = {"attempted": False, "succeeded": False, "pressure_hpa": None, "error": "", "port_open_close_ok": False}
    p3_result, p3_trace = _attempt_paroscientific_read(device, serial_factory=serial_factory, method="read_pressure")
    trace.extend(p3_trace)
    if not p3_result.get("succeeded"):
        fast_result, fast_trace = _attempt_paroscientific_read(device, serial_factory=serial_factory, method="read_pressure_fast")
        trace.extend(fast_trace)

    driver_pressure_hpa = p3_result.get("pressure_hpa") if p3_result.get("succeeded") else fast_result.get("pressure_hpa")
    driver_parse_status = "parse_ok" if driver_pressure_hpa is not None else parser_status
    pressure_gauge_unavailable = driver_pressure_hpa is None
    read_methods_attempted = ["generic_read_frame_raw_capture", "ParoscientificGauge.read_pressure"]
    if fast_result.get("attempted"):
        read_methods_attempted.append("ParoscientificGauge.read_pressure_fast")
    diagnostics = {
        **device,
        "driver_detected": True,
        "driver_name": "gas_calibrator.devices.paroscientific.ParoscientificGauge",
        "driver_read_only_methods": ["read_pressure", "read_pressure_fast"],
        "generic_read_frame_failed": bool(generic_read_frame_failed),
        "generic_frame_mode_unsupported_or_not_continuous": bool(generic_read_frame_failed),
        "known_v1_driver_readonly_attempted": True,
        "paroscientific_command_profile": "P3_single_read_query",
        "p3_command_preview": p3_command_preview,
        "read_methods_attempted": read_methods_attempted,
        "raw_capture_only": True,
        "read_window_s": read_window_s,
        "raw_bytes_len": len(raw),
        "raw_hex_preview": _hex_preview(raw),
        "raw_ascii_preview": _decode_ascii_preview(raw),
        "raw_lines_sample": _raw_lines_sample(raw),
        "parsed_pressure_hpa": driver_pressure_hpa,
        "parser_status": driver_parse_status,
        "parse_status": driver_parse_status,
        "pressure_hpa": driver_pressure_hpa,
        "raw_capture_pressure_hpa": pressure_hpa,
        "raw_capture_parser_status": parser_status,
        "stream_mode_assessment": stream_assessment,
        "query_response_supported_by_driver": True,
        "paroscientific_p3_read_attempted": bool(p3_result.get("attempted")),
        "paroscientific_p3_read_succeeded": bool(p3_result.get("succeeded")),
        "paroscientific_p3_read_failed": bool(p3_result.get("attempted") and not p3_result.get("succeeded")),
        "paroscientific_p3_error": str(p3_result.get("error") or ""),
        "paroscientific_fast_read_attempted": bool(fast_result.get("attempted")),
        "paroscientific_fast_read_succeeded": bool(fast_result.get("succeeded")),
        "paroscientific_fast_read_failed": bool(fast_result.get("attempted") and not fast_result.get("succeeded")),
        "paroscientific_fast_error": str(fast_result.get("error") or ""),
        "paroscientific_pre_cancel_continuous_sent": bool(p3_result.get("continuous_cancel_sent")
            or fast_result.get("continuous_cancel_sent")),
        "paroscientific_p3_clear_buffer": True,
        "paroscientific_p3_retries": 2,
        "dest_id_scan_attempted": False,
        "dest_id_scan_hits": [],
        "continuous_mode_supported_by_v1": True,
        "continuous_mode_not_used_in_r0_1": True,
        "continuous_mode_not_used_reason": "P4/P7 may alter output mode during diagnostics; R0.1 uses P3/read_pressure_fast only.",
        "pressure_gauge_probe_status": "readonly_available" if not pressure_gauge_unavailable else "known_v1_driver_readonly_failed",
        "pressure_gauge_unavailable": bool(pressure_gauge_unavailable),
        "pressure_gauge_blocks_r1": bool(pressure_gauge_unavailable),
        "state_changing_command_sent": False,
        "write_command_sent": False,
        "any_write_command_sent": False,
        "persistent_config_write_sent": False,
        "pressure_gauge_setting_write_sent": False,
        "control_command_sent": False,
        "port_open_close_ok": bool(opened and closed),
        "error": error,
    }
    return diagnostics, trace


def _read_chamber_registers(chamber: TemperatureChamber) -> tuple[dict[str, Any], list[str]]:
    read_methods = ["read_input_registers(7991,1)", "read_holding_registers(8100,1)"]
    current_temp_c = chamber.read_temp_c()
    set_temp_c = chamber.read_set_temp_c()
    values: dict[str, Any] = {
        "pv_current_temperature_c": float(current_temp_c),
        "pv_temperature_c": float(current_temp_c),
        "sv_set_temperature_c": float(set_temp_c),
        "sv_temperature_c": float(set_temp_c),
    }
    try:
        values["run_state"] = int(chamber.read_run_state())
        values["status_value"] = values["run_state"]
        read_methods.append("read_input_registers(7990,1)")
        values["run_state_read_status"] = "available"
    except Exception as exc:
        values["run_state_read_status"] = "unavailable"
        values["run_state_read_error"] = str(exc)
    return values, read_methods


def _chamber_readonly_diagnostics(
    device: Mapping[str, Any],
    *,
    chamber_readonly_driver_probe_status: str,
    read_only_registers_attempted: list[str],
    error: str = "",
    values: Optional[Mapping[str, Any]] = None,
    port_open_close_ok: bool = False,
) -> dict[str, Any]:
    readonly_succeeded = chamber_readonly_driver_probe_status == "known_driver_readonly_succeeded"
    readonly_failed = chamber_readonly_driver_probe_status == "known_driver_readonly_failed"
    diagnostics = {
        **dict(device),
        "driver_detected": True,
        "driver_name": "gas_calibrator.devices.temperature_chamber.TemperatureChamber",
        "driver_config_source": "devices.temperature_chamber",
        "read_only_methods": ["read_temp_c", "read_set_temp_c", "read_run_state"],
        "pv_current_temperature_register": 7991,
        "pv_register": 7991,
        "sv_set_temperature_read_register": 8100,
        "sv_register": 8100,
        "sv_set_temperature_write_register_identified_not_called": 8100,
        "run_status_register": 7990,
        "status_register": 7990,
        "protocol_candidate": "modbus_rtu",
        "protocol_status": "readonly_available" if readonly_succeeded else "temperature_chamber_protocol_unresolved",
        "generic_ascii_query_failed": True,
        "ascii_query_unsupported": True,
        "legacy_ascii_queries": ["PV?", "SV?"],
        "legacy_ascii_query_status": "unsupported_for_configured_modbus_driver",
        "chamber_driver_available": True,
        "chamber_readonly_driver_probe_status": chamber_readonly_driver_probe_status,
        "known_driver_readonly_succeeded": readonly_succeeded,
        "known_driver_readonly_failed": readonly_failed,
        "protocol_mismatch": True,
        "chamber_unavailable": readonly_failed,
        "temperature_chamber_unavailable": readonly_failed,
        "read_only_registers_attempted": list(read_only_registers_attempted),
        "write_register_sent": False,
        "write_coil_sent": False,
        "set_temperature_called": False,
        "set_temperature_command_sent": False,
        "start_command_sent": False,
        "stop_command_sent": False,
        "enable_command_sent": False,
        "disable_command_sent": False,
        "change_sv_command_sent": False,
        "control_command_sent": False,
        "port_open_close_ok": bool(port_open_close_ok),
        "error": error,
    }
    diagnostics.update(dict(values or {}))
    return diagnostics


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
        diagnostics = _chamber_readonly_diagnostics(
            device,
            chamber_readonly_driver_probe_status="driver_client_create_failed",
            read_only_registers_attempted=[],
            error=str(exc),
            port_open_close_ok=False,
        )
        diagnostics["chamber_driver_available"] = False
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
        values, read_methods = _read_chamber_registers(chamber)
        status = "known_driver_readonly_succeeded"
    except Exception as exc:
        error = str(exc)
        status = "known_driver_readonly_failed"
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
    diagnostics = _chamber_readonly_diagnostics(
        device,
        chamber_readonly_driver_probe_status=status,
        read_only_registers_attempted=read_methods,
        error=error,
        values=values,
        port_open_close_ok=bool(opened and closed),
    )
    return diagnostics, trace


def _default_chamber_client_factory(device: Mapping[str, Any]) -> Any:
    factory = TemperatureChamber._default_client_factory()
    return factory(
        port=str(device.get("port") or ""),
        baudrate=int(device.get("baud") or 9600),
        bytesize=int(device.get("bytesize") or 8),
        parity=str(device.get("parity") or "N"),
        stopbits=float(device.get("stopbits") or 1),
        timeout=float(device.get("timeout_s") or 1.0),
    )


def write_r0_1_reference_read_probe_artifacts(
    raw_cfg: Mapping[str, Any],
    *,
    output_dir: Optional[str | Path] = None,
    config_path: str | Path = "",
    operator_confirmation_path: Optional[str | Path] = None,
    branch: str = "",
    head: str = "",
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
    operator_payload: dict[str, Any] = {}
    operator_validation: dict[str, Any] = {
        "valid": False,
        "errors": ["operator_confirmation_not_provided"],
        "allowed_ports": sorted(R0_1_ALLOWED_PORTS),
        "utf8_no_bom": False,
        "only_com30_com27": False,
    }
    if operator_confirmation_path:
        try:
            operator_payload = load_json_mapping_utf8_no_bom(operator_confirmation_path)
            operator_validation = _operator_confirmation_validation(
                operator_payload,
                raw_cfg,
                config_path=config_path,
                branch=branch,
                head=head,
            )
        except Exception as exc:
            operator_validation = {
                "valid": False,
                "errors": [f"operator_confirmation_load_failed:{exc}"],
                "allowed_ports": sorted(R0_1_ALLOWED_PORTS),
                "utf8_no_bom": False,
                "only_com30_com27": False,
            }
    if execute_read_only and not operator_validation.get("valid"):
        rejection_reasons.extend(str(error) for error in operator_validation.get("errors", []))

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
            **_chamber_readonly_diagnostics(
                _chamber_device(raw_cfg),
                chamber_readonly_driver_probe_status="not_executed",
                read_only_registers_attempted=[],
            ),
            "protocol_status": "not_executed",
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

    executed = bool(execute_read_only and not rejection_reasons)
    pressure_unavailable = executed and bool(pressure_diag.get("pressure_gauge_unavailable"))
    chamber_probe_status = str(chamber_diag.get("chamber_readonly_driver_probe_status") or "not_executed")
    chamber_unavailable = executed and chamber_probe_status != "known_driver_readonly_succeeded"
    r1_block_reasons: list[str] = []
    if rejection_reasons:
        r1_block_reasons.extend(rejection_reasons)
    if pressure_unavailable:
        r1_block_reasons.append("pressure_gauge_reference_unavailable")
    if chamber_unavailable:
        if chamber_diag.get("temperature_chamber_unavailable"):
            r1_block_reasons.append("temperature_chamber_unavailable")
        else:
            r1_block_reasons.append(chamber_probe_status)
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
        "reference_read_diagnostics": str(run_dir / "reference_read_diagnostics.json"),
        "r0_1_reference_read_diagnostics": str(run_dir / "r0_1_reference_read_diagnostics.json"),
        "raw_capture_COM30": str(run_dir / "raw_capture_COM30.jsonl"),
        "raw_capture_COM27": str(run_dir / "raw_capture_COM27.jsonl"),
        "raw_capture_COM30_pressure_gauge": str(run_dir / "raw_capture_COM30_pressure_gauge.json"),
        "chamber_read_diagnostics_COM27": str(run_dir / "chamber_read_diagnostics_COM27.json"),
        "port_open_close_trace": str(run_dir / "port_open_close_trace.jsonl"),
        "operator_confirmation_record": str(run_dir / "operator_confirmation_record.json"),
        "safety_assertions": str(run_dir / "safety_assertions.json"),
    }
    opened_ports = sorted(
        {
            str(row.get("port") or "")
            for row in trace_rows
            if str(row.get("result") or "") == "ok"
            and (str(row.get("action") or "") == "open" or str(row.get("action") or "").endswith("_open"))
            and row.get("port")
        }
    )
    safety_assertions = {
        **R0_1_EVIDENCE_MARKERS,
        "only_ports_opened": opened_ports,
        "allowed_ports": sorted(R0_1_ALLOWED_PORTS),
        "opened_only_com30_com27": set(opened_ports).issubset(R0_1_ALLOWED_PORTS),
    }
    summary = {
        "schema_version": R0_1_SCHEMA_VERSION,
        **R0_1_EVIDENCE_MARKERS,
        "final_decision": final_decision,
        "execute_read_only": bool(execute_read_only and not rejection_reasons),
        "real_com_opened": any(row.get("action") == "open" and row.get("result") == "ok" for row in trace_rows),
        "real_probe_executed": bool(execute_read_only and not rejection_reasons),
        "operator_confirmation_valid": bool(operator_validation.get("valid")),
        "operator_confirmation_path": str(Path(operator_confirmation_path).expanduser().resolve()) if operator_confirmation_path else "",
        "opened_ports": opened_ports,
        "opened_only_com30_com27": set(opened_ports).issubset(R0_1_ALLOWED_PORTS),
        "r1_conditioning_allowed": False,
        "r1_allowed": False,
        "a1r_allowed": False,
        "a2_allowed": False,
        "a3_allowed": False,
        "r1_blocked": bool(r1_block_reasons),
        "r1_block_reasons": r1_block_reasons,
        "pressure_gauge_parser_status": pressure_diag.get("parser_status", "not_executed"),
        "pressure_gauge_probe_status": pressure_diag.get("pressure_gauge_probe_status", "not_executed"),
        "pressure_gauge_unavailable": bool(pressure_diag.get("pressure_gauge_unavailable")),
        "pressure_gauge_blocks_r1": bool(pressure_diag.get("pressure_gauge_blocks_r1")),
        "temperature_chamber_protocol_status": chamber_diag.get("protocol_status", "not_executed"),
        "temperature_chamber_readonly_driver_probe_status": chamber_probe_status,
        "temperature_chamber_unavailable": bool(chamber_diag.get("temperature_chamber_unavailable")),
        "artifact_paths": artifact_paths,
    }
    operator_record = {
        "schema_version": R0_1_SCHEMA_VERSION,
        "record_type": "r0_1_operator_confirmation_record",
        "operator_confirmation_path": str(Path(operator_confirmation_path).expanduser().resolve()) if operator_confirmation_path else "",
        "validation": operator_validation,
        "payload": operator_payload,
        "not_real_acceptance_evidence": True,
        "acceptance_level": "engineering_probe_only",
        "real_primary_latest_refresh": False,
    }
    raw_capture_com30_rows = [
        {
            "source": "generic_read_frame_raw_capture",
            "port": pressure_diag.get("port"),
            "raw_bytes_len": pressure_diag.get("raw_bytes_len", 0),
            "raw_hex_preview": pressure_diag.get("raw_hex_preview", ""),
            "raw_ascii_preview": pressure_diag.get("raw_ascii_preview", ""),
            "raw_lines_sample": pressure_diag.get("raw_lines_sample", []),
            "parse_status": pressure_diag.get("raw_capture_parser_status", pressure_diag.get("parse_status")),
        },
        {
            "source": "paroscientific_v1_readonly_driver",
            "port": pressure_diag.get("port"),
            "p3_command_preview": pressure_diag.get("p3_command_preview"),
            "read_methods_attempted": pressure_diag.get("read_methods_attempted", []),
            "paroscientific_p3_read_attempted": pressure_diag.get("paroscientific_p3_read_attempted", False),
            "paroscientific_p3_read_succeeded": pressure_diag.get("paroscientific_p3_read_succeeded", False),
            "paroscientific_fast_read_attempted": pressure_diag.get("paroscientific_fast_read_attempted", False),
            "paroscientific_fast_read_succeeded": pressure_diag.get("paroscientific_fast_read_succeeded", False),
            "parsed_pressure_hpa": pressure_diag.get("parsed_pressure_hpa"),
            "parse_status": pressure_diag.get("parse_status"),
        },
    ]
    raw_capture_com27_rows = [
        {
            "source": "generic_ascii_query_assessment",
            "port": chamber_diag.get("port"),
            "generic_ascii_query_failed": chamber_diag.get("generic_ascii_query_failed", False),
            "ascii_query_unsupported": chamber_diag.get("ascii_query_unsupported", False),
            "legacy_ascii_queries": chamber_diag.get("legacy_ascii_queries", []),
        },
        {
            "source": "temperature_chamber_modbus_readonly_driver",
            "port": chamber_diag.get("port"),
            "read_only_registers_attempted": chamber_diag.get("read_only_registers_attempted", []),
            "pv_temperature_c": chamber_diag.get("pv_temperature_c", chamber_diag.get("pv_current_temperature_c")),
            "sv_temperature_c": chamber_diag.get("sv_temperature_c", chamber_diag.get("sv_set_temperature_c")),
            "status_value": chamber_diag.get("status_value", chamber_diag.get("run_state")),
            "protocol_status": chamber_diag.get("protocol_status"),
            "chamber_readonly_driver_probe_status": chamber_diag.get("chamber_readonly_driver_probe_status"),
        },
    ]
    _json_dump(run_dir / "summary.json", summary)
    _json_dump(run_dir / "reference_read_diagnostics.json", diagnostics)
    _json_dump(run_dir / "r0_1_reference_read_diagnostics.json", diagnostics)
    _json_dump(run_dir / "raw_capture_COM30_pressure_gauge.json", pressure_diag)
    _jsonl_dump(run_dir / "raw_capture_COM30.jsonl", raw_capture_com30_rows)
    _json_dump(run_dir / "chamber_read_diagnostics_COM27.json", chamber_diag)
    _jsonl_dump(run_dir / "raw_capture_COM27.jsonl", raw_capture_com27_rows)
    _jsonl_dump(run_dir / "port_open_close_trace.jsonl", trace_rows)
    _json_dump(run_dir / "operator_confirmation_record.json", operator_record)
    _json_dump(run_dir / "safety_assertions.json", safety_assertions)
    return summary


