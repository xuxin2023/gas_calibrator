from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any, Callable, Mapping, Optional

from .run001_a1_analyzer_diagnostics import (
    _close_analyzer,
    _make_default_analyzer,
    _open_analyzer,
    _parse_line,
    _port_discovery_configs,
    contains_persistent_write_token,
)
from .run001_a1_analyzer_mode2_setup import (
    MODE2_SETUP_ALLOWED_COMMANDS,
    MODE2_SETUP_COMMAND_TARGET_ID,
    MODE2_SETUP_FORBIDDEN_COMMAND_TOKENS,
    READ_ONLY_QUERY_COMMANDS,
    _active_upload_noise_count,
    _extract_success_ack,
    _is_success_ack,
    send_mode2_setup_commands,
)


SERIAL_ASSISTANT_BASELINE_PORTS = ("COM35", "COM37", "COM41", "COM42")
SERIAL_ASSISTANT_KNOWN_DETECTED_IDS = {
    "COM35": "091",
    "COM37": "003",
    "COM41": "023",
    "COM42": "012",
}
SERIAL_ASSISTANT_SENSOR_UPLOAD_RATE_HZ = 10
SERIAL_ASSISTANT_LINE_ENDING = "CRLF"
SERIAL_ASSISTANT_LINE_ENDING_BYTES = "\\r\\n"
SERIAL_ASSISTANT_READ_TIMEOUT_S = 1.0


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _normalize_port(value: Any) -> str:
    return str(value or "").strip().upper()


def _known_ids_for_ports(ports: list[str]) -> dict[str, str]:
    return {
        port: SERIAL_ASSISTANT_KNOWN_DETECTED_IDS.get(port, "")
        for port in [_normalize_port(item) for item in ports]
        if port
    }


def _cfg_by_port(raw_cfg: Mapping[str, Any]) -> dict[str, Mapping[str, Any]]:
    devices = raw_cfg.get("devices") if isinstance(raw_cfg, Mapping) else {}
    analyzers = devices.get("gas_analyzers") if isinstance(devices, Mapping) else []
    result: dict[str, Mapping[str, Any]] = {}
    for item in list(analyzers or []):
        if not isinstance(item, Mapping):
            continue
        port = _normalize_port(item.get("port"))
        if port:
            result[port] = item
    return result


def _serial_params_for_ports(raw_cfg: Mapping[str, Any], ports: list[str]) -> dict[str, dict[str, Any]]:
    configured = _cfg_by_port(raw_cfg)
    params: dict[str, dict[str, Any]] = {}
    for port in [_normalize_port(item) for item in ports]:
        item = configured.get(port, {})
        params[port] = {
            "baudrate": int(item.get("baud", item.get("baudrate", 115200)) or 115200),
            "data_bits": int(item.get("bytesize", 8) or 8),
            "parity": str(item.get("parity", "N") or "N"),
            "stop_bits": item.get("stopbits", 1),
            "flow_control": "unknown_from_serial_assistant; V1/V2 SerialDevice does not enable flow control",
            "line_ending": SERIAL_ASSISTANT_LINE_ENDING,
            "line_ending_bytes": SERIAL_ASSISTANT_LINE_ENDING_BYTES,
            "read_timeout_s": float(item.get("timeout", SERIAL_ASSISTANT_READ_TIMEOUT_S) or SERIAL_ASSISTANT_READ_TIMEOUT_S),
            "source": "run001_config_or_v1_serial_defaults",
        }
    return params


def build_serial_assistant_baseline_payload(
    raw_cfg: Mapping[str, Any],
    *,
    ports: Optional[list[str]] = None,
) -> dict[str, Any]:
    requested_ports = [_normalize_port(item) for item in list(ports or SERIAL_ASSISTANT_BASELINE_PORTS) if _normalize_port(item)]
    known_ids = _known_ids_for_ports(requested_ports)
    return {
        "schema_version": "run001_a1.serial_assistant_baseline.1",
        "artifact_type": "run001_a1_serial_assistant_baseline",
        "generated_at": _utc_now(),
        "run_id": "Run-001/A1",
        "tested_by_site_operator": True,
        "serial_assistant_can_read_write": True,
        "ports_under_test": requested_ports,
        "known_detected_ids": known_ids,
        "sensor_upload_rate_hz": SERIAL_ASSISTANT_SENSOR_UPLOAD_RATE_HZ,
        "command_target_id": MODE2_SETUP_COMMAND_TARGET_ID,
        "known_issue": "ACK may be buried in 10Hz active-send frames",
        "v1_already_supports_this_behavior": True,
        "serial_parameters": _serial_params_for_ports(raw_cfg, requested_ports),
        "command_examples": [
            "READDATA,YGAS,FFF",
            "MODE,YGAS,FFF,2",
            "SETCOMWAY,YGAS,FFF,1",
        ],
        "response_examples": [
            "YGAS,<id>,T",
            "YGAS,<id>,<active-send data frame>",
        ],
        "missing_serial_assistant_details": [
            "exact serial assistant software name/version",
            "exact serial assistant line-ending setting as displayed onsite",
            "exact serial assistant read timeout/display mode",
            "raw request/response transcript from onsite assistant",
        ],
        "not_a1_pass_evidence": True,
        "not_real_acceptance_evidence": True,
    }


def _split_stream_lines(raw: Any) -> list[str]:
    if raw is None:
        return []
    if isinstance(raw, (list, tuple)):
        lines: list[str] = []
        for item in raw:
            lines.extend(_split_stream_lines(item))
        return lines
    text = str(raw or "").replace("\r", "\n")
    return [line.strip() for line in text.split("\n") if line.strip()]


def _flush_input(analyzer: Any) -> bool:
    transport = getattr(analyzer, "ser", None)
    for target in (transport, analyzer):
        flush_input = getattr(target, "flush_input", None)
        if callable(flush_input):
            try:
                flush_input()
                return True
            except Exception:
                return False
    return False


def _listen_stream_lines(analyzer: Any, timeout_s: float) -> list[str]:
    drain_stream = getattr(analyzer, "_drain_stream_lines", None)
    if callable(drain_stream):
        return _split_stream_lines(drain_stream(drain_s=max(0.05, float(timeout_s or 0.05)), read_timeout_s=0.05))

    transport = getattr(analyzer, "ser", None)
    drain = getattr(transport, "drain_input_nonblock", None)
    if callable(drain):
        return _split_stream_lines(drain(drain_s=max(0.05, float(timeout_s or 0.05)), read_timeout_s=0.05))

    read_latest = getattr(analyzer, "read_latest_data", None)
    if callable(read_latest):
        try:
            return _split_stream_lines(
                read_latest(
                    prefer_stream=True,
                    drain_s=max(0.05, float(timeout_s or 0.05)),
                    read_timeout_s=0.05,
                    allow_passive_fallback=False,
                )
            )
        except TypeError:
            return _split_stream_lines(read_latest())
    return []


def _send_read_query(analyzer: Any) -> tuple[str, list[str]]:
    command = "READDATA,YGAS,FFF"
    if contains_persistent_write_token(command):
        raise RuntimeError(f"read-only query command rejected by safety policy: {command}")
    passive = getattr(analyzer, "read_data_passive", None)
    if callable(passive):
        return command, _split_stream_lines(passive())
    transport = getattr(analyzer, "ser", None)
    write = getattr(transport, "write", None)
    if callable(write):
        write(command + "\r\n")
        drain = getattr(transport, "drain_input_nonblock", None)
        if callable(drain):
            return command, _split_stream_lines(drain(drain_s=0.5, read_timeout_s=0.05))
    return command, []


def _detected_mode(mode2_payload: Optional[Mapping[str, Any]], parsed_payload: Optional[Mapping[str, Any]]) -> str:
    if mode2_payload:
        return "MODE2"
    if parsed_payload:
        mode = parsed_payload.get("mode")
        if str(mode) in {"1", "1.0"}:
            return "MODE1"
        if str(mode) in {"2", "2.0"}:
            return "MODE2"
        if mode is not None:
            return f"MODE{mode}"
    return ""


def _probe_status(*, port_open: bool, bytes_received: int, frame_parse: bool) -> str:
    if not port_open:
        return "port_open_fail"
    if bytes_received <= 0:
        return "no_data"
    if not frame_parse:
        return "parse_fail"
    return "ok"


def _difference_from_serial_assistant(status: str) -> str:
    if status == "ok":
        return "none_detected"
    return f"v2_probe_not_matching_serial_assistant_{status}"


def _is_communication_setup_or_read_query_command(command: Any) -> bool:
    normalized = str(command or "").strip().upper().replace("\r", "").replace("\n", "")
    allowed = {str(item).upper() for item in MODE2_SETUP_ALLOWED_COMMANDS}
    return normalized in allowed or normalized.startswith("READDATA")


def _persistent_write_command_sent(commands: list[str]) -> bool:
    return any(
        contains_persistent_write_token(command) and not _is_communication_setup_or_read_query_command(command)
        for command in commands
    )


def _summarize_lines(analyzer: Any, lines: list[str]) -> tuple[Optional[dict[str, Any]], Optional[dict[str, Any]], str, str]:
    first_mode2: Optional[dict[str, Any]] = None
    first_parsed: Optional[dict[str, Any]] = None
    first_parseable_line = ""
    for line in lines:
        mode2_payload, parsed_payload = _parse_line(analyzer, line)
        if parsed_payload and first_parsed is None:
            first_parsed = dict(parsed_payload)
            first_parseable_line = str(line or "")
        if mode2_payload and first_mode2 is None:
            first_mode2 = dict(mode2_payload)
            if not first_parseable_line:
                first_parseable_line = str(line or "")
    observed_id = str((first_parsed or first_mode2 or {}).get("id") or "")
    return first_mode2, first_parsed, first_parseable_line, observed_id


def _empty_port_result(cfg: Mapping[str, Any]) -> dict[str, Any]:
    port = _normalize_port(cfg.get("configured_port") or cfg.get("port"))
    return {
        **dict(cfg),
        "port": port,
        "configured_port": port,
        "known_detected_device_id": SERIAL_ASSISTANT_KNOWN_DETECTED_IDS.get(port, ""),
        "port_open": False,
        "flush_before_listen": False,
        "bytes_received": 0,
        "observed_response_count": 0,
        "observed_response_sample": [],
        "frame_parse": False,
        "detected_mode": "",
        "active_send": False,
        "active_send_detected": False,
        "device_id": "",
        "observed_device_id": "",
        "read_query_sent": False,
        "read_query_command": "",
        "mode_setup_command_sent": False,
        "commands_sent": [],
        "command_events": [],
        "ack_candidates": [],
        "ignored_active_frame_count": 0,
        "status": "planned",
        "difference_from_serial_assistant": "",
        "error": "",
    }


def probe_serial_assistant_equivalent_port(
    cfg: Mapping[str, Any],
    *,
    analyzer_factory: Optional[Callable[[Mapping[str, Any]], Any]] = None,
    timeout_s: float = 30.0,
    allow_read_query: bool = False,
    send_mode2_active_send: bool = False,
    confirm_communication_setup: bool = False,
    command_timeout_s: float = 5.0,
) -> dict[str, Any]:
    result = _empty_port_result(cfg)
    analyzer: Any = None
    lines: list[str] = []
    try:
        factory = analyzer_factory or (lambda item: _make_default_analyzer(item, timeout_s))
        analyzer = factory(cfg)
        try:
            setattr(analyzer, "active_send", True)
        except Exception:
            pass
        _open_analyzer(analyzer)
        result["port_open"] = True
        result["flush_before_listen"] = _flush_input(analyzer)
        lines = _listen_stream_lines(analyzer, timeout_s)
        if not lines and allow_read_query:
            command, query_lines = _send_read_query(analyzer)
            result["read_query_sent"] = True
            result["read_query_command"] = command
            result["commands_sent"] = [command]
            lines = query_lines
        mode2_payload, parsed_payload, _parseable_line, observed_id = _summarize_lines(analyzer, lines)
        ack_candidates = [_extract_success_ack(line) for line in lines if _is_success_ack(line)]
        result.update(
            {
                "bytes_received": sum(len(str(line or "").encode("utf-8")) for line in lines),
                "observed_response_count": len(lines),
                "observed_response_sample": lines[:5],
                "frame_parse": bool(parsed_payload),
                "detected_mode": _detected_mode(mode2_payload, parsed_payload),
                "active_send": bool(lines) and not result["read_query_sent"],
                "active_send_detected": bool(lines) and not result["read_query_sent"],
                "device_id": observed_id,
                "observed_device_id": observed_id,
                "ack_candidates": [item for item in ack_candidates if item],
                "ignored_active_frame_count": _active_upload_noise_count(lines),
            }
        )
        if send_mode2_active_send:
            if not confirm_communication_setup:
                raise RuntimeError("--confirm-communication-setup is required before MODE/SETCOMWAY commands")
            sent, command_events = send_mode2_setup_commands(
                analyzer,
                list(MODE2_SETUP_ALLOWED_COMMANDS),
                command_timeout_s=command_timeout_s,
            )
            result["commands_sent"] = list(result.get("commands_sent") or []) + sent
            result["command_events"] = command_events
            result["mode_setup_command_sent"] = bool(sent)
            event_ack_candidates = [
                str(event.get("ack_payload") or "")
                for event in command_events
                if str(event.get("ack_payload") or "").strip()
            ]
            result["ack_candidates"] = list(result.get("ack_candidates") or []) + event_ack_candidates
            result["ignored_active_frame_count"] = int(result["ignored_active_frame_count"]) + sum(
                int(event.get("ignored_active_frame_count") or 0) for event in command_events
            )
        result["status"] = _probe_status(
            port_open=bool(result["port_open"]),
            bytes_received=int(result["bytes_received"] or 0),
            frame_parse=bool(result["frame_parse"]),
        )
    except Exception as exc:
        result["error"] = str(exc)
        result["status"] = "port_open_fail" if not result["port_open"] else "error"
    finally:
        if analyzer is not None:
            _close_analyzer(analyzer)
    result["difference_from_serial_assistant"] = _difference_from_serial_assistant(str(result["status"] or ""))
    return result


def build_serial_assistant_equivalent_probe_payload(
    raw_cfg: Mapping[str, Any],
    *,
    ports: Optional[list[str]] = None,
    read_only: bool = True,
    allow_read_query: bool = False,
    send_mode2_active_send: bool = False,
    confirm_communication_setup: bool = False,
    timeout_s: float = 30.0,
    command_timeout_s: float = 5.0,
    analyzer_factory: Optional[Callable[[Mapping[str, Any]], Any]] = None,
) -> dict[str, Any]:
    requested_ports = [_normalize_port(item) for item in list(ports or SERIAL_ASSISTANT_BASELINE_PORTS) if _normalize_port(item)]
    if not read_only and not send_mode2_active_send:
        raise ValueError("Probe must be --read-only unless explicit communication setup is requested")
    selected = _port_discovery_configs(raw_cfg, requested_ports)
    results = [
        probe_serial_assistant_equivalent_port(
            cfg,
            analyzer_factory=analyzer_factory,
            timeout_s=timeout_s,
            allow_read_query=allow_read_query,
            send_mode2_active_send=send_mode2_active_send,
            confirm_communication_setup=confirm_communication_setup,
            command_timeout_s=command_timeout_s,
        )
        for cfg in selected
    ]
    commands_sent = [command for item in results for command in list(item.get("commands_sent") or [])]
    forbidden_token_detected = any(
        token in str(command or "").upper()
        for command in commands_sent
        for token in MODE2_SETUP_FORBIDDEN_COMMAND_TOKENS
    )
    persistent_write_sent = _persistent_write_command_sent(commands_sent)
    ok_count = sum(1 for item in results if item.get("status") == "ok")
    return {
        "schema_version": "run001_a1.serial_assistant_equivalent_probe.1",
        "artifact_type": "run001_a1_serial_assistant_equivalent_probe",
        "generated_at": _utc_now(),
        "run_id": "Run-001/A1",
        "read_only": bool(read_only),
        "allow_read_query": bool(allow_read_query),
        "send_mode2_active_send": bool(send_mode2_active_send),
        "confirm_communication_setup": bool(confirm_communication_setup),
        "requested_ports": requested_ports,
        "command_target_id": MODE2_SETUP_COMMAND_TARGET_ID,
        "line_ending": SERIAL_ASSISTANT_LINE_ENDING,
        "line_ending_bytes": SERIAL_ASSISTANT_LINE_ENDING_BYTES,
        "ack_search_policy": "scan_active_stream_for_YGAS_id_T",
        "allowed_communication_commands": list(MODE2_SETUP_ALLOWED_COMMANDS),
        "read_only_query_commands": list(READ_ONLY_QUERY_COMMANDS),
        "forbidden_tokens": list(MODE2_SETUP_FORBIDDEN_COMMAND_TOKENS),
        "commands_sent": commands_sent,
        "flush_before_send": True,
        "persistent_write_command_sent": bool(persistent_write_sent),
        "calibration_write_command_sent": False,
        "forbidden_token_detected": forbidden_token_detected,
        "v1_production_flow_touched": False,
        "run_app_touched": False,
        "a1_execute_invoked": False,
        "a2_invoked": False,
        "h2o_invoked": False,
        "full_group_invoked": False,
        "analyzers": results,
        "summary": {
            "total": len(results),
            "ok": ok_count,
            "failed": len(results) - ok_count,
            "serial_assistant_success_reproduced_by_v2": len(results) > 0 and ok_count == len(results),
            "mode_setup_command_sent": any(bool(item.get("mode_setup_command_sent")) for item in results),
            "persistent_write_command_sent": bool(persistent_write_sent),
            "calibration_write_command_sent": False,
            "not_a1_pass_evidence": True,
            "not_real_acceptance_evidence": True,
        },
        "not_a1_pass_evidence": True,
        "not_real_acceptance_evidence": True,
    }


def render_serial_assistant_baseline_report(payload: Mapping[str, Any]) -> str:
    lines = [
        "# Run-001/A1 serial assistant baseline",
        "",
        f"- tested_by_site_operator: {payload.get('tested_by_site_operator')}",
        f"- serial_assistant_can_read_write: {payload.get('serial_assistant_can_read_write')}",
        f"- ports_under_test: {', '.join(payload.get('ports_under_test') or [])}",
        f"- known_detected_ids: {payload.get('known_detected_ids')}",
        f"- sensor_upload_rate_hz: {payload.get('sensor_upload_rate_hz')}",
        f"- command_target_id: {payload.get('command_target_id')}",
        f"- known_issue: {payload.get('known_issue')}",
        f"- v1_already_supports_this_behavior: {payload.get('v1_already_supports_this_behavior')}",
        "",
        "## Serial parameters",
    ]
    for port, params in dict(payload.get("serial_parameters") or {}).items():
        lines.extend(
            [
                "",
                f"### {port}",
                f"- baudrate: {params.get('baudrate')}",
                f"- data_bits: {params.get('data_bits')}",
                f"- parity: {params.get('parity')}",
                f"- stop_bits: {params.get('stop_bits')}",
                f"- flow_control: {params.get('flow_control')}",
                f"- line_ending: {params.get('line_ending')} ({params.get('line_ending_bytes')})",
                f"- read_timeout_s: {params.get('read_timeout_s')}",
                f"- source: {params.get('source')}",
            ]
        )
    lines.extend(["", "## Missing onsite details"])
    for item in list(payload.get("missing_serial_assistant_details") or []):
        lines.append(f"- {item}")
    lines.extend(["", "This baseline is not A1 PASS evidence and not real acceptance evidence.", ""])
    return "\n".join(lines)


def render_serial_assistant_equivalent_probe_report(payload: Mapping[str, Any]) -> str:
    lines = [
        "# Run-001/A1 serial assistant equivalent probe",
        "",
        f"- read_only: {payload.get('read_only')}",
        f"- allow_read_query: {payload.get('allow_read_query')}",
        f"- send_mode2_active_send: {payload.get('send_mode2_active_send')}",
        f"- command_target_id: {payload.get('command_target_id')}",
        f"- ack_search_policy: {payload.get('ack_search_policy')}",
        f"- commands_sent: {payload.get('commands_sent')}",
        f"- persistent_write_command_sent: {payload.get('persistent_write_command_sent')}",
        f"- calibration_write_command_sent: {payload.get('calibration_write_command_sent')}",
        f"- forbidden_token_detected: {payload.get('forbidden_token_detected')}",
        f"- serial_assistant_success_reproduced_by_v2: {payload.get('summary', {}).get('serial_assistant_success_reproduced_by_v2')}",
        "",
        "## Analyzer results",
    ]
    for item in list(payload.get("analyzers") or []):
        lines.extend(
            [
                "",
                f"### {item.get('configured_port') or item.get('port')}",
                f"- known_detected_device_id: {item.get('known_detected_device_id') or '-'}",
                f"- port_open: {item.get('port_open')}",
                f"- flush_before_listen: {item.get('flush_before_listen')}",
                f"- bytes_received: {item.get('bytes_received')}",
                f"- observed_response_count: {item.get('observed_response_count')}",
                f"- frame_parse: {item.get('frame_parse')}",
                f"- detected_mode: {item.get('detected_mode') or '-'}",
                f"- active_send: {item.get('active_send')}",
                f"- observed_device_id: {item.get('observed_device_id') or '-'}",
                f"- read_query_sent: {item.get('read_query_sent')}",
                f"- mode_setup_command_sent: {item.get('mode_setup_command_sent')}",
                f"- ack_candidates: {item.get('ack_candidates')}",
                f"- ignored_active_frame_count: {item.get('ignored_active_frame_count')}",
                f"- status: {item.get('status')}",
                f"- difference_from_serial_assistant: {item.get('difference_from_serial_assistant')}",
                f"- error: {item.get('error') or '-'}",
            ]
        )
    lines.extend(["", "This probe does not execute A1, A2, H2O, full group, or calibration writes.", ""])
    return "\n".join(lines)


def write_serial_assistant_artifacts(
    output_dir: str | Path,
    baseline_payload: Mapping[str, Any],
    probe_payload: Mapping[str, Any],
) -> dict[str, str]:
    directory = Path(output_dir)
    directory.mkdir(parents=True, exist_ok=True)
    baseline_json = directory / "serial_assistant_baseline.json"
    baseline_md = directory / "serial_assistant_baseline.md"
    probe_json = directory / "serial_assistant_equivalent_probe.json"
    probe_md = directory / "serial_assistant_equivalent_probe.md"
    baseline_json.write_text(json.dumps(dict(baseline_payload), ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    baseline_md.write_text(render_serial_assistant_baseline_report(baseline_payload), encoding="utf-8")
    probe_json.write_text(json.dumps(dict(probe_payload), ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    probe_md.write_text(render_serial_assistant_equivalent_probe_report(probe_payload), encoding="utf-8")
    return {
        "serial_assistant_baseline_json": str(baseline_json),
        "serial_assistant_baseline_md": str(baseline_md),
        "serial_assistant_equivalent_probe_json": str(probe_json),
        "serial_assistant_equivalent_probe_md": str(probe_md),
    }
