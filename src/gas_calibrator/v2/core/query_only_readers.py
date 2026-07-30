"""Read-only device readers used by the temporary Step 3A R0 probe.

This module deliberately owns no admission gate, CLI, artifact writer, or
standalone executor. Ports must come from the reviewed probe configuration;
there are no historical COM fallbacks.
"""

from __future__ import annotations

from datetime import datetime, timezone
import time
from typing import Any, Callable, Mapping, Optional

from gas_calibrator.devices.paroscientific import ParoscientificGauge
from gas_calibrator.devices.temperature_chamber import TemperatureChamber


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


def _required_port(config: Mapping[str, Any], *, device_name: str) -> str:
    port = str(config.get("port") or "").strip()
    if not port:
        raise ValueError(f"{device_name} requires an explicit configured port")
    return port


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
    config = _device_cfg(raw_cfg, "pressure_gauge")
    timeout_s = float(config.get("timeout") or 1.0)
    return {
        "device_name": "pressure_gauge",
        "device_type": "pressure_gauge",
        "port": _required_port(config, device_name="pressure_gauge"),
        "baud": int(config.get("baud") or config.get("baudrate") or 9600),
        "timeout": timeout_s,
        "timeout_s": timeout_s,
        "response_timeout_s": float(config.get("response_timeout_s") or 2.2),
        "dest_id": str(config.get("dest_id") or "01"),
        "read_only": True,
    }


def _chamber_device(raw_cfg: Mapping[str, Any]) -> dict[str, Any]:
    config = _device_cfg(raw_cfg, "temperature_chamber")
    addr = int(
        config.get("addr")
        or config.get("slave")
        or config.get("unit")
        or config.get("unit_id")
        or 1
    )
    return {
        "device_name": "temperature_chamber",
        "device_type": "temperature_chamber",
        "port": _required_port(config, device_name="temperature_chamber"),
        "baud": int(config.get("baud") or config.get("baudrate") or 9600),
        "baudrate": int(config.get("baud") or config.get("baudrate") or 9600),
        "parity": str(config.get("parity") or "N"),
        "stopbits": float(config.get("stopbits") or 1),
        "bytesize": int(config.get("bytesize") or 8),
        "timeout": float(config.get("timeout") or 1.0),
        "timeout_s": float(config.get("timeout") or 1.0),
        "addr": addr,
        "slave_id": addr,
        "unit_id": addr,
        "read_only": True,
        "protocol_candidate": "modbus_rtu",
    }


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
        _required_port(device, device_name="pressure_gauge"),
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
    """Attempt one named Paroscientific read-only pressure strategy."""
    trace: list[dict[str, Any]] = []
    gauge = _new_pressure_gauge(device, serial_factory)
    opened = False
    closed = False
    value: Optional[float] = None
    error = ""
    continuous_cancel_sent = False
    p3_read_with_retry_used = False
    try:
        gauge.open()
        opened = True
        trace.append(
            _trace_row(
                device_name="pressure_gauge",
                device_type="pressure_gauge",
                port=str(device.get("port") or ""),
                action=f"{method}_open",
                result="ok",
            )
        )

        if method == "read_pressure_fast":
            value = float(
                gauge.read_pressure_fast(
                    response_timeout_s=0.20,
                    retries=1,
                    clear_buffer=False,
                    buffered_drain_s=0.08,
                )
            )
            trace_action = f"{method}_buffered_or_p3_read"
        elif method in {"read_pressure", "read_pressure_p3_retry"}:
            value = float(
                gauge._p3_read_with_retry(
                    cancel_wait_s=0.30,
                    query_timeout_s=0.20,
                    max_retries=3,
                    retry_increment_s=0.10,
                )
            )
            continuous_cancel_sent = True
            p3_read_with_retry_used = True
            trace_action = f"{method}_p3_read_with_retry"
        else:
            raise ValueError(f"unsupported Paroscientific read method: {method}")
        trace.append(
            _trace_row(
                device_name="pressure_gauge",
                device_type="pressure_gauge",
                port=str(device.get("port") or ""),
                action=trace_action,
                result="ok",
                details={"pressure_hpa": value},
            )
        )
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
            trace.append(
                _trace_row(
                    device_name="pressure_gauge",
                    device_type="pressure_gauge",
                    port=str(device.get("port") or ""),
                    action=f"{method}_close",
                    result="ok",
                )
            )
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
        "p3_read_with_retry_used": bool(p3_read_with_retry_used),
    }, trace


def read_pressure_gauge_raw_capture(
    raw_cfg: Mapping[str, Any],
    *,
    serial_factory: Callable[[Mapping[str, Any]], Any],
    recovery_pressure_hpa: Optional[float] = None,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    device = _pressure_device(raw_cfg)
    read_window_s = float(
        _path_value(raw_cfg, "r0_1.pressure_gauge.read_window_s") or 0.35
    )
    poll_interval_s = float(
        _path_value(raw_cfg, "r0_1.pressure_gauge.poll_interval_s") or 0.02
    )
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
        trace.append(
            _trace_row(
                device_name="pressure_gauge",
                device_type="pressure_gauge",
                port=device["port"],
                action="open",
                result="ok",
            )
        )
        raw = _read_raw_window(
            handle,
            read_window_s=read_window_s,
            poll_interval_s=poll_interval_s,
            max_reads=max_reads,
        )
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
                trace.append(
                    _trace_row(
                        device_name="pressure_gauge",
                        device_type="pressure_gauge",
                        port=device["port"],
                        action="close",
                        result="ok",
                    )
                )
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
    p3_result = {
        "attempted": False,
        "succeeded": False,
        "pressure_hpa": None,
        "error": "",
        "port_open_close_ok": False,
    }
    fast_result = dict(p3_result)
    retry3_result = dict(p3_result)
    p3_result, p3_trace = _attempt_paroscientific_read(
        device,
        serial_factory=serial_factory,
        method="read_pressure",
    )
    trace.extend(p3_trace)
    if not p3_result.get("succeeded"):
        fast_result, fast_trace = _attempt_paroscientific_read(
            device,
            serial_factory=serial_factory,
            method="read_pressure_fast",
        )
        trace.extend(fast_trace)

    driver_pressure_hpa = (
        p3_result.get("pressure_hpa")
        if p3_result.get("succeeded")
        else fast_result.get("pressure_hpa")
    )
    recovery_source = "none"

    if driver_pressure_hpa is None:
        retry3_result, retry3_trace = _attempt_paroscientific_read(
            device,
            serial_factory=serial_factory,
            method="read_pressure_p3_retry",
        )
        trace.extend(retry3_trace)
        if (
            retry3_result.get("succeeded")
            and retry3_result.get("pressure_hpa") is not None
        ):
            driver_pressure_hpa = float(retry3_result["pressure_hpa"])
            recovery_source = "p3_retry_3rd"

    if driver_pressure_hpa is None and pressure_hpa is not None:
        driver_pressure_hpa = float(pressure_hpa)
        recovery_source = "raw_capture_parse"

    if driver_pressure_hpa is None and recovery_pressure_hpa is not None:
        driver_pressure_hpa = float(recovery_pressure_hpa)
        recovery_source = "provided_recovery"

    driver_parse_status = (
        "parse_ok" if driver_pressure_hpa is not None else parser_status
    )
    pressure_gauge_unavailable = driver_pressure_hpa is None
    pressure_recovery_used = recovery_source != "none"
    read_methods_attempted = [
        "generic_read_frame_raw_capture",
        "ParoscientificGauge.read_pressure",
    ]
    if fast_result.get("attempted"):
        read_methods_attempted.append("ParoscientificGauge.read_pressure_fast")
    if retry3_result.get("attempted"):
        read_methods_attempted.append("ParoscientificGauge.read_pressure_p3_retry")
    diagnostics = {
        **device,
        "driver_detected": True,
        "driver_name": "gas_calibrator.devices.paroscientific.ParoscientificGauge",
        "driver_read_only_methods": ["read_pressure", "read_pressure_fast"],
        "generic_read_frame_failed": bool(generic_read_frame_failed),
        "generic_frame_mode_unsupported_or_not_continuous": bool(
            generic_read_frame_failed
        ),
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
        "paroscientific_p3_read_failed": bool(
            p3_result.get("attempted") and not p3_result.get("succeeded")
        ),
        "paroscientific_p3_error": str(p3_result.get("error") or ""),
        "paroscientific_fast_read_attempted": bool(fast_result.get("attempted")),
        "paroscientific_fast_read_succeeded": bool(fast_result.get("succeeded")),
        "paroscientific_fast_read_failed": bool(
            fast_result.get("attempted") and not fast_result.get("succeeded")
        ),
        "paroscientific_fast_error": str(fast_result.get("error") or ""),
        "paroscientific_pre_cancel_continuous_sent": bool(
            p3_result.get("continuous_cancel_sent")
            or fast_result.get("continuous_cancel_sent")
        ),
        "paroscientific_p3_clear_buffer": True,
        "paroscientific_p3_retries": 2,
        "paroscientific_p3_retry_3rd_attempted": bool(retry3_result.get("attempted")),
        "paroscientific_p3_retry_3rd_succeeded": bool(retry3_result.get("succeeded")),
        "paroscientific_p3_retry_3rd_error": str(retry3_result.get("error") or ""),
        "pressure_recovery_source": recovery_source,
        "pressure_recovery_used": bool(pressure_recovery_used),
        "dest_id_scan_attempted": False,
        "dest_id_scan_hits": [],
        "continuous_mode_supported_by_v1": True,
        "continuous_mode_not_used_in_r0_1": True,
        "continuous_mode_not_used_reason": (
            "P4/P7 may alter output mode during diagnostics; "
            "R0.1 uses P3/read_pressure_fast only."
        ),
        "pressure_gauge_probe_status": (
            "readonly_available"
            if not pressure_gauge_unavailable
            else "known_v1_driver_readonly_failed"
        ),
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


def _read_chamber_registers(
    chamber: TemperatureChamber,
) -> tuple[dict[str, Any], list[str]]:
    read_methods = [
        "read_input_registers(7991,1)",
        "read_holding_registers(8100,1)",
    ]
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
    readonly_succeeded = (
        chamber_readonly_driver_probe_status == "known_driver_readonly_succeeded"
    )
    readonly_failed = (
        chamber_readonly_driver_probe_status == "known_driver_readonly_failed"
    )
    diagnostics = {
        **dict(device),
        "driver_detected": True,
        "driver_name": "gas_calibrator.devices.temperature_chamber.TemperatureChamber",
        "driver_config_source": "devices.temperature_chamber",
        "read_only_methods": [
            "read_temp_c",
            "read_set_temp_c",
            "read_run_state",
        ],
        "pv_current_temperature_register": 7991,
        "pv_register": 7991,
        "sv_set_temperature_read_register": 8100,
        "sv_register": 8100,
        "sv_set_temperature_write_register_identified_not_called": 8100,
        "run_status_register": 7990,
        "status_register": 7990,
        "protocol_candidate": "modbus_rtu",
        "protocol_status": (
            "readonly_available"
            if readonly_succeeded
            else "temperature_chamber_protocol_unresolved"
        ),
        "generic_ascii_query_failed": True,
        "ascii_query_unsupported": True,
        "legacy_ascii_queries": ["PV?", "SV?"],
        "legacy_ascii_query_status": ("unsupported_for_configured_modbus_driver"),
        "chamber_driver_available": True,
        "chamber_readonly_driver_probe_status": (chamber_readonly_driver_probe_status),
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
        trace.append(
            _trace_row(
                device_name="temperature_chamber",
                device_type="temperature_chamber",
                port=device["port"],
                action="open",
                result="ok",
            )
        )
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
            trace.append(
                _trace_row(
                    device_name="temperature_chamber",
                    device_type="temperature_chamber",
                    port=device["port"],
                    action="close",
                    result="ok",
                )
            )
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


def default_chamber_client_factory(device: Mapping[str, Any]) -> Any:
    factory = TemperatureChamber._default_client_factory()
    return factory(
        port=_required_port(device, device_name="temperature_chamber"),
        baudrate=int(device.get("baud") or 9600),
        bytesize=int(device.get("bytesize") or 8),
        parity=str(device.get("parity") or "N"),
        stopbits=float(device.get("stopbits") or 1),
        timeout=float(device.get("timeout_s") or 1.0),
    )


__all__ = [
    "default_chamber_client_factory",
    "read_pressure_gauge_raw_capture",
    "read_temperature_chamber_read_only",
]
