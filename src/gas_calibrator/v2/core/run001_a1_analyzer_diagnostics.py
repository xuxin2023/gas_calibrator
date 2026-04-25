from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any, Callable, Mapping, Optional


READ_ONLY_QUERY_COMMANDS = ("READDATA",)
FORBIDDEN_PERSISTENT_COMMAND_TOKENS = (
    "SETCOMWAY",
    "SET_COMM_WAY",
    "SETMODE",
    "SET_MODE",
    "SENCO",
    "COEFF",
    "ZERO",
    "SPAN",
    "CALIBRATION",
    "CALIBRATE",
    "SAVE",
    "COMMIT",
    "WRITEBACK",
    "WRITE_BACK",
    "EEPROM",
    "FLASH",
    "NVM",
    "PARAM",
)


def contains_persistent_write_token(command: Any) -> bool:
    text = str(command or "").upper().replace("-", "_").replace(" ", "").replace("\t", "")
    return any(token in text for token in FORBIDDEN_PERSISTENT_COMMAND_TOKENS)


def _as_bool(value: Any, *, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def run001_a1_analyzer_configs(raw_cfg: Mapping[str, Any]) -> list[dict[str, Any]]:
    devices = raw_cfg.get("devices") if isinstance(raw_cfg, Mapping) else {}
    analyzers = devices.get("gas_analyzers") if isinstance(devices, Mapping) else []
    configs: list[dict[str, Any]] = []
    for index, item in enumerate(list(analyzers or [])):
        if not isinstance(item, Mapping):
            continue
        enabled = _as_bool(item.get("enabled"), default=True)
        connected = _as_bool(item.get("connected"), default=True)
        if not enabled or not connected:
            continue
        name = str(item.get("name") or f"ga{index + 1:02d}").strip()
        expected_device_id = str(item.get("device_id") or "").strip()
        configs.append(
            {
                "logical_id": f"gas_analyzer_{index}",
                "zero_based_index": index,
                "one_based_position": index + 1,
                "physical_label": name.upper() if name else f"GA{index + 1:02d}",
                "configured_name": name,
                "port": str(item.get("port") or "").strip(),
                "configured_port": str(item.get("port") or "").strip(),
                "baudrate": int(item.get("baud", item.get("baudrate", 115200)) or 115200),
                "expected_device_id": expected_device_id,
                "configured_device_id": expected_device_id,
                "protocol": "YGAS",
                "expected_mode": int(item.get("mode", 2) or 2),
                "active_send_expected": bool(item.get("active_send", False)),
                "enabled": enabled,
                "connected": connected,
            }
        )
    return configs


def _selected_analyzer_configs(
    raw_cfg: Mapping[str, Any],
    only_failed: Optional[list[str]],
    analyzers: Optional[list[str]] = None,
) -> list[dict[str, Any]]:
    configs = run001_a1_analyzer_configs(raw_cfg)
    selected = {str(item).strip() for item in list(analyzers or only_failed or []) if str(item).strip()}
    if not selected:
        return configs
    return [cfg for cfg in configs if cfg["logical_id"] in selected or cfg["physical_label"] in selected]


def _normalize_port(value: Any) -> str:
    return str(value or "").strip().upper()


def _configured_analyzer_lookup(raw_cfg: Mapping[str, Any]) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
    by_port: dict[str, dict[str, Any]] = {}
    by_device_id: dict[str, dict[str, Any]] = {}
    for cfg in run001_a1_analyzer_configs(raw_cfg):
        port = _normalize_port(cfg.get("configured_port"))
        device_id = str(cfg.get("configured_device_id") or "").strip()
        if port:
            by_port[port] = dict(cfg)
        if device_id:
            by_device_id[device_id] = dict(cfg)
    return by_port, by_device_id


def _port_discovery_configs(raw_cfg: Mapping[str, Any], ports: list[str]) -> list[dict[str, Any]]:
    by_port, _by_device_id = _configured_analyzer_lookup(raw_cfg)
    configs: list[dict[str, Any]] = []
    for index, port_value in enumerate(list(ports or [])):
        port = _normalize_port(port_value)
        if not port:
            continue
        planned = dict(by_port.get(port) or {})
        formal_configured_device_id = str(planned.get("configured_device_id") or "").strip()
        configured_label = str(planned.get("physical_label") or "").strip()
        configs.append(
            {
                "logical_id": f"port_discovery_{port}",
                "zero_based_index": index,
                "one_based_position": index + 1,
                "physical_label": configured_label or "",
                "configured_name": str(planned.get("configured_name") or "").strip(),
                "port": port,
                "configured_port": port,
                "baudrate": int(planned.get("baudrate") or 115200),
                "expected_device_id": "",
                "configured_device_id": "",
                "formal_configured_device_id_for_port": formal_configured_device_id,
                "protocol": "YGAS",
                "expected_mode": int(planned.get("expected_mode") or 2),
                "active_send_expected": True,
                "enabled": True,
                "connected": True,
                "port_discovery": True,
                "configured_physical_label_for_port": configured_label,
            }
        )
    return configs


def _make_default_analyzer(cfg: Mapping[str, Any], timeout_s: float) -> Any:
    from gas_calibrator.devices.gas_analyzer import GasAnalyzer

    return GasAnalyzer(
        str(cfg.get("configured_port") or ""),
        baudrate=int(cfg.get("baudrate") or 115200),
        timeout=max(0.1, min(float(timeout_s or 20.0), 2.0)),
        device_id=str(cfg.get("configured_device_id") or "000"),
    )


def _open_analyzer(analyzer: Any) -> None:
    for target in (analyzer, getattr(analyzer, "ser", None)):
        if target is None:
            continue
        for method_name in ("connect", "open"):
            method = getattr(target, method_name, None)
            if callable(method):
                method()
                return


def _close_analyzer(analyzer: Any) -> None:
    for target in (analyzer, getattr(analyzer, "ser", None)):
        if target is None:
            continue
        close = getattr(target, "close", None)
        if callable(close):
            try:
                close()
            except Exception:
                pass
            return


def _read_active_line(analyzer: Any, timeout_s: float) -> str:
    read_latest = getattr(analyzer, "read_latest_data", None)
    if callable(read_latest):
        try:
            timeout_value = max(0.1, min(float(timeout_s or 20.0), 60.0))
            attempts = 1 + max(0, int(getattr(analyzer, "ACTIVE_READ_RETRY_COUNT", 4) or 4))
            per_attempt_drain_s = max(0.05, timeout_value / max(1, attempts))
            return str(
                read_latest(
                    prefer_stream=True,
                    drain_s=per_attempt_drain_s,
                    read_timeout_s=0.05,
                    allow_passive_fallback=False,
                )
                or ""
            )
        except TypeError:
            return str(read_latest() or "")
    read_active = getattr(analyzer, "read_data_active", None)
    if callable(read_active):
        return str(read_active() or "")
    return ""


def _read_query_line(analyzer: Any) -> str:
    command = READ_ONLY_QUERY_COMMANDS[0]
    if contains_persistent_write_token(command):
        raise RuntimeError(f"read-only query command rejected by safety policy: {command}")
    passive = getattr(analyzer, "read_data_passive", None)
    if callable(passive):
        return str(passive() or "")
    read_latest = getattr(analyzer, "read_latest_data", None)
    if callable(read_latest):
        try:
            return str(read_latest(prefer_stream=False, allow_passive_fallback=True) or "")
        except TypeError:
            return str(read_latest() or "")
    return ""


def _parse_line(analyzer: Any, line: str) -> tuple[Optional[dict[str, Any]], Optional[dict[str, Any]]]:
    mode2_payload: Optional[dict[str, Any]] = None
    parsed_payload: Optional[dict[str, Any]] = None
    parse_mode2 = getattr(analyzer, "parse_line_mode2", None)
    if callable(parse_mode2):
        candidate = parse_mode2(line)
        if isinstance(candidate, Mapping):
            mode2_payload = dict(candidate)
    parse_line = getattr(analyzer, "parse_line", None)
    if callable(parse_line):
        candidate = parse_line(line)
        if isinstance(candidate, Mapping):
            parsed_payload = dict(candidate)
    if parsed_payload is None:
        parsed_payload = mode2_payload
    return mode2_payload, parsed_payload


def _detected_format(*, mode2: Optional[dict[str, Any]], parsed: Optional[dict[str, Any]], line: str) -> str:
    if mode2:
        return "mode2"
    if parsed:
        mode = parsed.get("mode")
        if mode == 1:
            return "mode1"
        if mode:
            return f"mode{mode}"
        return "ygas_unknown_mode"
    if str(line or "").strip():
        return "unparsed"
    return ""


def _infer_parse_error(*, line: str, parsed: Optional[dict[str, Any]], error_type: str, exc_message: str = "") -> str:
    if exc_message:
        return exc_message
    if line and not parsed:
        return "YGAS frame could not be parsed"
    if error_type in {"parse_fail", "read_or_parse_error"}:
        return error_type
    return ""


def _port_discovery_mapping_fields(
    result: Mapping[str, Any],
    *,
    raw_cfg: Mapping[str, Any],
) -> dict[str, str]:
    by_port, by_device_id = _configured_analyzer_lookup(raw_cfg)
    port = _normalize_port(result.get("configured_port") or result.get("port"))
    observed_id = str(result.get("observed_device_id") or "").strip()
    configured_for_port = by_port.get(port)
    configured_for_id = by_device_id.get(observed_id) if observed_id else None
    possible_label = str((configured_for_id or {}).get("physical_label") or "").strip()
    configured_label = str((configured_for_port or {}).get("physical_label") or "").strip()
    error_type = str(result.get("error_type") or "")
    mode2 = bool(result.get("mode2_detected"))
    active_send = bool(result.get("active_send_detected"))

    if error_type == "device_id_mismatch" and observed_id:
        suggestion = (
            f"do_not_map_to_{configured_label or port}_without_site_confirmation;"
            f" detected_device_id_{observed_id}"
        )
    elif mode2 and active_send and observed_id and configured_for_id:
        suggestion = f"suggest_map_to_{possible_label}_after_site_confirmation"
    elif mode2 and active_send and observed_id:
        suggestion = f"detected_unmapped_device_id_{observed_id}_requires_site_confirmation"
    elif error_type == "mode_mismatch":
        suggestion = "mode_mismatch_requires_mode2_active_send_setup"
    elif error_type == "no_data":
        suggestion = "no_data_check_power_wiring_active_send_and_windows_com_mapping"
    elif error_type == "port_open_fail":
        suggestion = "port_open_fail_check_windows_com_mapping_or_port_busy"
    elif error_type == "parse_fail":
        suggestion = "parse_fail_check_protocol_baudrate_or_non_analyzer_output"
    else:
        suggestion = "requires_site_confirmation"

    return {
        "possible_physical_label": possible_label,
        "mapping_suggestion": suggestion,
    }


def _error_suggestion(error_type: str, cfg: Mapping[str, Any]) -> str:
    label = cfg.get("physical_label") or cfg.get("logical_id")
    port = cfg.get("configured_port") or "configured COM port"
    if error_type == "port_open_fail":
        return f"Check {label} power, cable, Windows COM mapping, and whether {port} is occupied."
    if error_type == "no_data":
        return f"Check {label} active-send setting, TX/RX wiring, baudrate, and instrument power on {port}."
    if error_type == "parse_fail":
        return f"Check {label} protocol output format, baudrate, and whether frames are YGAS-compatible."
    if error_type == "mode_mismatch":
        return f"Check {label} mode setting; expected MODE2 active-send, and this diagnostic must not switch modes."
    if error_type == "device_id_mismatch":
        return f"Check {label} device-id mapping; expected {cfg.get('configured_device_id')} on {port}."
    return "No onsite communication issue detected by this read-only diagnostic."


def diagnose_analyzer_config(
    cfg: Mapping[str, Any],
    *,
    raw_cfg: Optional[Mapping[str, Any]] = None,
    analyzer_factory: Optional[Callable[[Mapping[str, Any]], Any]] = None,
    allow_read_query: bool = False,
    timeout_s: float = 20.0,
) -> dict[str, Any]:
    result = {
        **dict(cfg),
        "port_exists": bool(str(cfg.get("configured_port") or "").strip()),
        "port_open_success": False,
        "port_open": False,
        "bytes_received": 0,
        "frame_parse_success": False,
        "frame_parse": False,
        "mode2_frame_detected": False,
        "mode2_detected": False,
        "mode1_detected": False,
        "detected_format": "",
        "latest_data_age_s": None,
        "observed_device_id": "",
        "device_id": "",
        "after_device_id": "",
        "device_id_correct": False,
        "observed_serial": "",
        "active_send_detected": False,
        "parse_error": "",
        "read_query_command_used": False,
        "read_query_command": "",
        "commands_sent": [],
        "error_type": "",
        "error_message": "",
        "possible_physical_label": "",
        "mapping_suggestion": "",
        "suggested_onsite_check": "",
    }
    analyzer: Any = None
    try:
        factory = analyzer_factory or (lambda item: _make_default_analyzer(item, timeout_s))
        analyzer = factory(cfg)
        try:
            setattr(analyzer, "active_send", bool(cfg.get("active_send_expected")))
        except Exception:
            pass
        _open_analyzer(analyzer)
        result["port_open_success"] = True
        result["port_open"] = True
        line = _read_active_line(analyzer, timeout_s)
        if not line and allow_read_query:
            result["read_query_command_used"] = True
            result["read_query_command"] = READ_ONLY_QUERY_COMMANDS[0]
            result["commands_sent"] = [READ_ONLY_QUERY_COMMANDS[0]]
            line = _read_query_line(analyzer)
        result["bytes_received"] = len(str(line or "").encode("utf-8"))
        result["active_send_detected"] = bool(line) and not bool(result["read_query_command_used"])
        mode2, parsed = _parse_line(analyzer, line)
        result["mode2_frame_detected"] = bool(mode2)
        result["mode2_detected"] = bool(mode2)
        result["frame_parse_success"] = bool(parsed)
        result["frame_parse"] = bool(parsed)
        result["detected_format"] = _detected_format(mode2=mode2, parsed=parsed, line=line)
        result["mode1_detected"] = bool(parsed and not mode2 and int(parsed.get("mode") or 0) == 1)
        if parsed:
            result["latest_data_age_s"] = 0.0
            result["observed_device_id"] = str(parsed.get("id") or "")
            result["device_id"] = result["observed_device_id"]
            result["after_device_id"] = result["observed_device_id"]
            result["observed_serial"] = str(parsed.get("serial") or parsed.get("sn") or "")
        expected_device_id = str(cfg.get("configured_device_id") or "").strip()
        observed_device_id = str(result.get("observed_device_id") or "").strip()
        result["device_id_correct"] = bool(
            expected_device_id and observed_device_id and expected_device_id == observed_device_id
        )
        expected_mode = int(cfg.get("expected_mode") or 0)
        observed_mode = int(parsed.get("mode") or 0) if parsed else 0
        result["observed_mode"] = observed_mode if observed_mode else None
        if not line:
            result["error_type"] = "no_data"
        elif not parsed:
            result["error_type"] = "parse_fail"
        elif expected_mode and observed_mode and expected_mode != observed_mode:
            result["error_type"] = "mode_mismatch"
        elif expected_mode == 2 and not mode2:
            result["error_type"] = "mode_mismatch"
        elif expected_device_id and observed_device_id != expected_device_id:
            result["error_type"] = "device_id_mismatch"
        else:
            result["error_type"] = "ok"
        result["parse_error"] = _infer_parse_error(
            line=line,
            parsed=parsed,
            error_type=str(result["error_type"]),
        )
    except Exception as exc:
        result["error_message"] = str(exc)
        result["error_type"] = "port_open_fail" if not result["port_open_success"] else "read_or_parse_error"
        result["parse_error"] = _infer_parse_error(
            line="",
            parsed=None,
            error_type=str(result["error_type"]),
            exc_message=str(exc),
        )
    finally:
        if analyzer is not None:
            _close_analyzer(analyzer)
    result["suggested_onsite_check"] = _error_suggestion(str(result["error_type"]), cfg)
    result["final_status"] = "ready" if result["error_type"] == "ok" else "not_ready"
    if cfg.get("port_discovery") and raw_cfg is not None:
        result.update(_port_discovery_mapping_fields(result, raw_cfg=raw_cfg))
    return result


def build_analyzer_precheck_diagnostics(
    raw_cfg: Mapping[str, Any],
    *,
    only_failed: Optional[list[str]] = None,
    analyzers: Optional[list[str]] = None,
    ports: Optional[list[str]] = None,
    read_only: bool = True,
    allow_read_query: bool = False,
    timeout_s: float = 20.0,
    analyzer_factory: Optional[Callable[[Mapping[str, Any]], Any]] = None,
) -> dict[str, Any]:
    if not read_only:
        raise ValueError("Run-001/A1 analyzer diagnostics is read-only only")
    requested = list(ports or analyzers or only_failed or [])
    port_discovery = bool(ports)
    selected = _port_discovery_configs(raw_cfg, list(ports or [])) if port_discovery else _selected_analyzer_configs(raw_cfg, only_failed, analyzers)
    analyzers = [
        diagnose_analyzer_config(
            cfg,
            raw_cfg=raw_cfg if port_discovery else None,
            analyzer_factory=analyzer_factory,
            allow_read_query=allow_read_query,
            timeout_s=timeout_s,
        )
        for cfg in selected
    ]
    failed = [item for item in analyzers if item.get("error_type") != "ok"]
    detected = [
        item
        for item in analyzers
        if item.get("active_send_detected")
        and item.get("frame_parse_success")
        and str(item.get("observed_device_id") or "")
        and str(item.get("observed_mode") or "") in {"1", "2"}
    ]
    ready_detected = [item for item in detected if item.get("mode2_detected")]
    return {
        "schema_version": "run001_a1.analyzer_precheck_diagnostics.1",
        "artifact_type": "run001_a1_analyzer_precheck_diagnostics",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "run_id": "Run-001/A1",
        "read_only": True,
        "port_discovery": port_discovery,
        "default_mode": "active_send_listen",
        "allow_read_query": bool(allow_read_query),
        "read_only_query_commands": list(READ_ONLY_QUERY_COMMANDS),
        "forbidden_persistent_command_tokens": list(FORBIDDEN_PERSISTENT_COMMAND_TOKENS),
        "only_failed": list(only_failed or []),
        "requested_analyzers": requested,
        "requested_ports": list(ports or []),
        "zero_based_one_based_note": (
            "gas_analyzer_0/1/2/3 are zero-based logical ids and map to physical GA01/GA02/GA03/GA04."
        ),
        "analyzers": analyzers,
        "summary": {
            "total": len(analyzers),
            "ok": len(analyzers) - len(failed),
            "failed": len(failed),
            "failed_logical_ids": [str(item.get("logical_id") or "") for item in failed],
            "detected_analyzer_count": len(detected),
            "detected_ports": [str(item.get("configured_port") or "") for item in detected],
            "ready_mode2_detected_count": len(ready_detected),
            "ready_mode2_detected_ports": [str(item.get("configured_port") or "") for item in ready_detected],
            "port_discovery_does_not_require_all_ports_ready": port_discovery,
            "a1_no_write_rerun_allowed": (not port_discovery) and len(analyzers) == 4 and not failed,
            "persistent_write_command_sent": any(
                contains_persistent_write_token(command)
                for item in analyzers
                for command in list(item.get("commands_sent") or [])
            ),
        },
    }


def render_analyzer_precheck_report(payload: Mapping[str, Any]) -> str:
    lines = [
        "# Run-001/A1 analyzer precheck diagnostics",
        "",
        f"- read_only: {payload.get('read_only')}",
        f"- default_mode: {payload.get('default_mode')}",
        f"- allow_read_query: {payload.get('allow_read_query')}",
        f"- zero_based_one_based_note: {payload.get('zero_based_one_based_note')}",
        f"- persistent_write_command_sent: {payload.get('summary', {}).get('persistent_write_command_sent')}",
        "",
        "## Analyzer results",
    ]
    for item in list(payload.get("analyzers") or []):
        lines.extend(
            [
                "",
                f"### {item.get('logical_id')} / {item.get('physical_label')}",
                f"- port: {item.get('configured_port')} @ {item.get('baudrate')}",
                f"- expected_mode: {item.get('expected_mode')}",
                f"- active_send_expected: {item.get('active_send_expected')}",
                f"- port_open_success: {item.get('port_open_success')}",
                f"- bytes_received: {item.get('bytes_received')}",
                f"- frame_parse_success: {item.get('frame_parse_success')}",
                f"- detected_format: {item.get('detected_format') or '-'}",
                f"- mode1_detected: {item.get('mode1_detected')}",
                f"- mode2_frame_detected: {item.get('mode2_frame_detected')}",
                f"- observed_device_id: {item.get('observed_device_id') or '-'}",
                f"- read_query_command_used: {item.get('read_query_command_used')}",
                f"- error_type: {item.get('error_type')}",
                f"- possible_physical_label: {item.get('possible_physical_label') or '-'}",
                f"- mapping_suggestion: {item.get('mapping_suggestion') or '-'}",
                f"- suggested_onsite_check: {item.get('suggested_onsite_check')}",
            ]
        )
    lines.extend(
        [
            "",
            "This diagnostic is read-only. It must not send SETCOMWAY, MODE switches, SENCO, coefficient, zero, span, calibration, save, writeback, EEPROM, Flash, NVM, or parameter-write commands.",
            "",
        ]
    )
    return "\n".join(lines)


def write_analyzer_precheck_diagnostics(output_dir: str | Path, payload: Mapping[str, Any]) -> dict[str, str]:
    directory = Path(output_dir)
    directory.mkdir(parents=True, exist_ok=True)
    json_path = directory / "analyzer_precheck_diagnostics.json"
    report_path = directory / "analyzer_precheck_diagnostics.md"
    json_path.write_text(json.dumps(dict(payload), ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    report_path.write_text(render_analyzer_precheck_report(payload), encoding="utf-8")
    return {"json": str(json_path), "report": str(report_path)}
