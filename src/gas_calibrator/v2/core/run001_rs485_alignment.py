from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Any, Mapping, Optional


SCHEMA_VERSION = "v2.run001.a2_15.rs485_v1_v2_alignment_matrix.1"

ALIGNMENT_FIELDS = (
    "com_port",
    "baud",
    "parity",
    "stopbits",
    "bytesize",
    "timeout",
    "physical_layer",
    "protocol_profile",
    "rs485_address",
    "dest_id",
    "slave_id",
    "unit_id",
    "terminator",
    "command",
    "raw_request_hex",
    "raw_response_hex",
    "post_write_read_delay_ms",
    "drain_required",
    "continuous_mode_handling",
    "parse_rule",
)


def load_json_mapping(path: str | Path) -> dict[str, Any]:
    payload = json.loads(Path(path).read_text(encoding="utf-8-sig"))
    if not isinstance(payload, Mapping):
        raise ValueError(f"JSON payload must be an object: {path}")
    return dict(payload)


def _json_dump(path: Path, payload: Mapping[str, Any]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _normalize(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return f"{float(value):g}"
    return str(value).strip()


def _wire_text(command: str, terminator: str = "LF") -> str:
    text = str(command or "")
    if not text:
        return ""
    if "\\r" in text or "\\n" in text:
        return text.replace("\\r", "\r").replace("\\n", "\n")
    if text.endswith(("\r", "\n")):
        return text
    term = str(terminator or "LF").upper()
    if term == "CR":
        return text + "\r"
    if term == "CRLF":
        return text + "\r\n"
    return text + "\n"


def _hex(text: str) -> str:
    if not text:
        return ""
    return text.encode("ascii", errors="replace").hex().upper()


def _device_cfg(raw_cfg: Mapping[str, Any], name: str) -> dict[str, Any]:
    devices = raw_cfg.get("devices")
    if not isinstance(devices, Mapping):
        return {}
    value = devices.get(name)
    return dict(value) if isinstance(value, Mapping) else {}


def _relay_map(raw_cfg: Mapping[str, Any]) -> dict[str, Any]:
    valves = raw_cfg.get("valves")
    if not isinstance(valves, Mapping):
        return {}
    mapping = valves.get("relay_map")
    return dict(mapping) if isinstance(mapping, Mapping) else {}


def _serial_common(cfg: Mapping[str, Any], *, physical_layer: str = "RS485") -> dict[str, Any]:
    return {
        "com_port": str(cfg.get("port") or ""),
        "baud": cfg.get("baud", cfg.get("baudrate")),
        "parity": cfg.get("parity", "N"),
        "stopbits": cfg.get("stopbits", 1),
        "bytesize": cfg.get("bytesize", 8),
        "timeout": cfg.get("timeout", cfg.get("timeout_s", cfg.get("response_timeout_s"))),
        "physical_layer": physical_layer,
    }


def _pace_rows(path: Optional[str | Path]) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    if not path:
        return {}, []
    p = Path(path)
    if not p.exists():
        return {}, []
    data = load_json_mapping(p)
    rows = data.get("rows")
    if isinstance(rows, list):
        return data, list(rows)
    derived_rows = [
        {"command": str(key), "response": value, "duration_ms": None, "error": ""}
        for key, value in data.items()
        if str(key).endswith("?") and isinstance(value, str)
    ]
    return data, derived_rows


def _find_pace_row(rows: list[dict[str, Any]], command: str) -> dict[str, Any]:
    for row in rows:
        if str(row.get("command") or "").strip().upper() == command.upper():
            return dict(row)
    return {}


def _jsonl_rows(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8-sig").splitlines():
        if not line.strip():
            continue
        try:
            value = json.loads(line)
        except Exception:
            continue
        if isinstance(value, Mapping):
            rows.append(dict(value))
    return rows


def _current_trace_row(rows: list[dict[str, Any]], command: str) -> dict[str, Any]:
    for row in rows:
        if str(row.get("command") or "").strip().upper() == command.upper():
            return dict(row)
    return {}


def _first_p3_success_from_io(path: Optional[str | Path]) -> dict[str, Any]:
    if not path:
        return {}
    p = Path(path)
    if not p.exists():
        return {}
    rows: list[dict[str, Any]] = []
    with p.open("r", encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle):
            rows.append(dict(row))
    for idx, row in enumerate(rows):
        command = str(row.get("command") or "")
        if str(row.get("port") or "").upper() != "COM30":
            continue
        if str(row.get("direction") or "").upper() != "TX":
            continue
        if "P3" not in command:
            continue
        for later in rows[idx + 1 : idx + 8]:
            if str(later.get("port") or "").upper() != "COM30":
                continue
            if str(later.get("direction") or "").upper() != "RX":
                continue
            response = str(later.get("response") or "").strip()
            if response:
                return {
                    "command": command,
                    "response": response,
                    "path": str(p),
                }
    return {}


def _alignment_status(
    historical: Mapping[str, Any],
    current: Mapping[str, Any],
    *,
    compare_fields: tuple[str, ...] = ALIGNMENT_FIELDS,
) -> tuple[str, list[str]]:
    if not historical:
        return "historical_success_not_found", ["historical_success_not_found"]
    reasons: list[str] = []
    for field in compare_fields:
        old = historical.get(field)
        new = current.get(field)
        if field == "protocol_profile":
            old_text = _normalize(old)
            new_text = _normalize(new)
            if old_text and new_text and old_text in new_text:
                continue
            if old_text == "OLD_PACE5000" and "pace5000" in new_text.lower() and "v1_aligned" in new_text.lower():
                continue
        if _normalize(old) != _normalize(new):
            if field == "raw_response_hex" and _normalize(old) and not _normalize(new):
                reasons.append(f"{field}:historical_nonempty_current_empty")
            elif field == "command" and not _normalize(new):
                reasons.append(f"{field}:current_not_attempted")
            else:
                reasons.append(f"{field}:{_normalize(old) or '<missing>'}!={_normalize(new) or '<missing>'}")
    return ("aligned" if not reasons else "mismatch"), reasons


def _matrix_row(
    *,
    device_name: str,
    device_role: str,
    historical: Mapping[str, Any],
    current: Mapping[str, Any],
    evidence_path: str = "",
    compare_fields: tuple[str, ...] = ALIGNMENT_FIELDS,
) -> dict[str, Any]:
    status, reasons = _alignment_status(historical, current, compare_fields=compare_fields)
    row: dict[str, Any] = {
        "device_name": device_name,
        "device_role": device_role,
        "historical_success_or_v1": dict(historical),
        "current_v2_a2_14": dict(current),
        "alignment_status": status,
        "mismatch_reason": ";".join(reasons),
        "evidence_path": evidence_path,
    }
    for field in ALIGNMENT_FIELDS:
        row[field] = {
            "historical_success_or_v1": historical.get(field),
            "current_v2_a2_14": current.get(field),
        }
    return row


def build_rs485_v1_v2_alignment_matrix(
    *,
    current_a2_14_dir: str | Path,
    v1_config_path: str | Path,
    historical_pace_identity_path: Optional[str | Path] = None,
    historical_pace_readback_path: Optional[str | Path] = None,
    historical_pressure_gauge_io_path: Optional[str | Path] = None,
) -> dict[str, Any]:
    current_dir = Path(current_a2_14_dir)
    current_summary = load_json_mapping(current_dir / "summary.json")
    current_config = load_json_mapping(current_dir / "rs485_command_diagnostic_config.json")
    v1_config = load_json_mapping(v1_config_path)
    current_pc_trace = _jsonl_rows(current_dir / "pressure_controller_command_trace.jsonl")
    current_pm_trace = _jsonl_rows(current_dir / "pressure_meter_command_trace.jsonl")

    pace_identity_payload, pace_identity_rows = _pace_rows(historical_pace_identity_path)
    pace_readback_payload, pace_readback_rows = _pace_rows(historical_pace_readback_path)
    p3_success = _first_p3_success_from_io(historical_pressure_gauge_io_path)

    v1_pc = _device_cfg(v1_config, "pressure_controller")
    v2_pc = _device_cfg(current_config, "pressure_controller")
    v1_pm = _device_cfg(v1_config, "pressure_gauge")
    v2_pm = _device_cfg(current_config, "pressure_gauge")
    v1_relay = _device_cfg(v1_config, "relay")
    v1_relay_b = _device_cfg(v1_config, "relay_8")
    v2_relay = _device_cfg(current_config, "relay")
    v2_relay_b = _device_cfg(current_config, "relay_8")

    pace_profile = str(
        pace_identity_payload.get("profile")
        or pace_readback_payload.get("profile")
        or "OLD_PACE5000"
    )

    rows: list[dict[str, Any]] = []
    idn_hist = _find_pace_row(pace_identity_rows, "*IDN?")
    idn_current = _current_trace_row(current_pc_trace, "*IDN?")
    rows.append(
        _matrix_row(
            device_name="pressure_controller",
            device_role="identity_query",
            evidence_path=str(historical_pace_identity_path or ""),
            historical={
                **_serial_common(v1_pc),
                "protocol_profile": pace_profile,
                "terminator": v1_pc.get("line_ending", "LF"),
                "command": "*IDN?",
                "raw_request_hex": _hex(_wire_text("*IDN?", str(v1_pc.get("line_ending") or "LF"))),
                "raw_response_hex": _hex(str(idn_hist.get("response") or "")),
                "post_write_read_delay_ms": idn_hist.get("duration_ms"),
                "drain_required": False,
                "continuous_mode_handling": "not_applicable",
                "parse_rule": "identity_text_not_required_for_online",
            },
            current={
                **_serial_common(v2_pc),
                "protocol_profile": current_summary.get("pressure_controller_protocol_profile"),
                "terminator": current_summary.get("pressure_controller_command_terminator"),
                "command": current_summary.get("pressure_controller_identity_query_command") or "*IDN?",
                "raw_request_hex": idn_current.get("raw_request_hex") or _hex(_wire_text("*IDN?")),
                "raw_response_hex": idn_current.get("raw_response_hex") or "",
                "post_write_read_delay_ms": None,
                "drain_required": False,
                "continuous_mode_handling": "not_applicable",
                "parse_rule": "unsupported_identity_query_does_not_alone_mark_offline",
            },
        )
    )

    outp_hist = _find_pace_row(pace_identity_rows + pace_readback_rows, ":OUTP:STAT?")
    outp_current = _current_trace_row(current_pc_trace, ":OUTP:STAT?")
    rows.append(
        _matrix_row(
            device_name="pressure_controller",
            device_role="readonly_status_ping",
            evidence_path=str(historical_pace_readback_path or historical_pace_identity_path or ""),
            historical={
                **_serial_common(v1_pc),
                "protocol_profile": pace_profile,
                "terminator": v1_pc.get("line_ending", "LF"),
                "command": ":OUTP:STAT?",
                "raw_request_hex": _hex(_wire_text(":OUTP:STAT?", str(v1_pc.get("line_ending") or "LF"))),
                "raw_response_hex": _hex(str(outp_hist.get("response") or "")),
                "post_write_read_delay_ms": outp_hist.get("duration_ms"),
                "drain_required": False,
                "continuous_mode_handling": "not_applicable",
                "parse_rule": "parse_first_int",
            },
            current={
                **_serial_common(v2_pc),
                "protocol_profile": current_summary.get("pressure_controller_protocol_profile"),
                "terminator": current_summary.get("pressure_controller_command_terminator"),
                "command": ":OUTP:STAT?",
                "raw_request_hex": outp_current.get("raw_request_hex") or _hex(_wire_text(":OUTP:STAT?")),
                "raw_response_hex": outp_current.get("raw_response_hex") or "",
                "post_write_read_delay_ms": None,
                "drain_required": False,
                "continuous_mode_handling": "not_applicable",
                "parse_rule": "parse_first_int",
            },
        )
    )

    pressure_hist = _find_pace_row(pace_readback_rows, ":SENS:PRES?") or _find_pace_row(
        pace_identity_rows, ":SENS:PRES:INL?"
    )
    rows.append(
        _matrix_row(
            device_name="pressure_controller",
            device_role="readonly_pressure_read",
            evidence_path=str(historical_pace_readback_path or historical_pace_identity_path or ""),
            historical={
                **_serial_common(v1_pc),
                "protocol_profile": pace_profile,
                "terminator": v1_pc.get("line_ending", "LF"),
                "command": str(pressure_hist.get("command") or ":SENS:PRES?"),
                "raw_request_hex": _hex(_wire_text(str(pressure_hist.get("command") or ":SENS:PRES?"))),
                "raw_response_hex": _hex(str(pressure_hist.get("response") or "")),
                "post_write_read_delay_ms": pressure_hist.get("duration_ms"),
                "drain_required": False,
                "continuous_mode_handling": "fallback_from_cont_query_to_sens_pres_or_in_limits",
                "parse_rule": "parse_first_float",
            },
            current={
                **_serial_common(v2_pc),
                "protocol_profile": current_summary.get("pressure_controller_protocol_profile"),
                "terminator": current_summary.get("pressure_controller_command_terminator"),
                "command": "",
                "raw_request_hex": "",
                "raw_response_hex": "",
                "post_write_read_delay_ms": None,
                "drain_required": False,
                "continuous_mode_handling": "not_attempted_in_a2_14_query_only",
                "parse_rule": "parse_first_float",
            },
        )
    )

    p3_command = str(p3_success.get("command") or f"*{v1_pm.get('dest_id', '01')}00P3\\r\\n")
    current_pm = current_pm_trace[0] if current_pm_trace else {}
    rows.append(
        _matrix_row(
            device_name="pressure_meter",
            device_role="p3_single_read",
            evidence_path=str(historical_pressure_gauge_io_path or ""),
            historical={
                **_serial_common(v1_pm),
                "timeout": v1_pm.get("response_timeout_s", v1_pm.get("timeout")),
                "protocol_profile": "paroscientific_p3_readonly",
                "rs485_address": "",
                "dest_id": str(v1_pm.get("dest_id") or "01"),
                "slave_id": "",
                "unit_id": "",
                "terminator": "CRLF",
                "command": p3_command,
                "raw_request_hex": _hex(_wire_text(p3_command, "CRLF")),
                "raw_response_hex": _hex(str(p3_success.get("response") or "")),
                "post_write_read_delay_ms": None,
                "drain_required": False,
                "continuous_mode_handling": "P3 single-read; continuous P4/P7 optional outside query-only",
                "parse_rule": "strip_star_prefix_parse_float_after_address",
            },
            current={
                **_serial_common(v2_pm),
                "timeout": v2_pm.get("response_timeout_s", v2_pm.get("timeout")),
                "protocol_profile": current_summary.get("pressure_meter_protocol_profile"),
                "rs485_address": "",
                "dest_id": current_summary.get("pressure_meter_dest_id"),
                "slave_id": "",
                "unit_id": "",
                "terminator": "CRLF",
                "command": current_pm.get("command") or f"*{current_summary.get('pressure_meter_dest_id') or '01'}00P3\\r\\n",
                "raw_request_hex": current_pm.get("raw_request_hex") or _hex(_wire_text(f"*{current_summary.get('pressure_meter_dest_id') or '01'}00P3\\r\\n")),
                "raw_response_hex": current_pm.get("raw_response_hex") or "",
                "post_write_read_delay_ms": None,
                "drain_required": bool(current_summary.get("pressure_meter_drain_attempted")),
                "continuous_mode_handling": str(current_summary.get("pressure_meter_mode") or ""),
                "parse_rule": "strip_star_prefix_parse_float_after_address",
            },
        )
    )

    relay_compare_fields = tuple(
        field
        for field in ALIGNMENT_FIELDS
        if field
        not in {
            "protocol_profile",
            "command",
            "raw_request_hex",
            "raw_response_hex",
            "parse_rule",
        }
    )
    rows.append(
        _matrix_row(
            device_name="relay",
            device_role="channel_mapping",
            evidence_path=str(v1_config_path),
            compare_fields=relay_compare_fields,
            historical={
                **_serial_common(v1_relay),
                "protocol_profile": "modbus_rtu_relay",
                "rs485_address": v1_relay.get("addr"),
                "slave_id": v1_relay.get("addr"),
                "unit_id": v1_relay.get("addr"),
                "terminator": "modbus_rtu_crc",
                "command": "write_coil/channel_map",
                "raw_request_hex": "",
                "raw_response_hex": "",
                "post_write_read_delay_ms": None,
                "drain_required": False,
                "continuous_mode_handling": "not_applicable",
                "parse_rule": "modbus_response_status",
                "relay_map": _relay_map(v1_config),
                "relay_b_com_port": v1_relay_b.get("port"),
                "relay_b_slave_id": v1_relay_b.get("addr"),
            },
            current={
                **_serial_common(v2_relay),
                "protocol_profile": "modbus_relay_open_close_only_no_output",
                "rs485_address": v2_relay.get("addr"),
                "slave_id": v2_relay.get("addr"),
                "unit_id": v2_relay.get("addr"),
                "terminator": "modbus_rtu_crc",
                "command": "<open_close_only>",
                "raw_request_hex": "",
                "raw_response_hex": "",
                "post_write_read_delay_ms": None,
                "drain_required": False,
                "continuous_mode_handling": "not_applicable",
                "parse_rule": "open_close_only",
                "relay_map": _relay_map(current_config),
                "relay_b_com_port": v2_relay_b.get("port"),
                "relay_b_slave_id": v2_relay_b.get("addr"),
            },
        )
    )

    # Relay map is important enough to compare explicitly outside the generic field set.
    if rows[-1]["historical_success_or_v1"].get("relay_map") != rows[-1]["current_v2_a2_14"].get("relay_map"):
        rows[-1]["alignment_status"] = "mismatch"
        extra = "relay_map:historical_v1!=current_v2"
        existing = str(rows[-1].get("mismatch_reason") or "")
        rows[-1]["mismatch_reason"] = f"{existing};{extra}" if existing else extra

    mismatch_reasons = [
        f"{row['device_name']}.{row['device_role']}:{row['mismatch_reason']}"
        for row in rows
        if row.get("alignment_status") != "aligned"
    ]
    evidence_search = {
        "historical_pace_identity_path": str(historical_pace_identity_path or ""),
        "historical_pace_identity_found": bool(pace_identity_rows),
        "historical_pace_readback_path": str(historical_pace_readback_path or ""),
        "historical_pace_readback_found": bool(pace_readback_rows),
        "historical_pressure_gauge_io_path": str(historical_pressure_gauge_io_path or ""),
        "historical_pressure_gauge_success_found": bool(p3_success),
        "v1_config_path": str(v1_config_path),
        "current_a2_14_dir": str(current_dir),
    }
    if not any(
        (
            evidence_search["historical_pace_identity_found"],
            evidence_search["historical_pace_readback_found"],
            evidence_search["historical_pressure_gauge_success_found"],
        )
    ):
        mismatch_reasons.append("historical_success_not_found")

    return {
        "schema_version": SCHEMA_VERSION,
        "evidence_source": "a2_15_rs485_v1_v2_alignment_offline",
        "acceptance_level": "engineering_probe_only",
        "not_real_acceptance_evidence": True,
        "real_primary_latest_refresh": False,
        "no_write": True,
        "a3_allowed": False,
        "evidence_search": evidence_search,
        "historical_success_not_found": "historical_success_not_found" in mismatch_reasons,
        "command_profile_mismatch": bool(mismatch_reasons),
        "command_profile_mismatch_reason": ";".join(mismatch_reasons),
        "rows": rows,
    }


def write_rs485_v1_v2_alignment_matrix(
    *,
    output_path: str | Path,
    current_a2_14_dir: str | Path,
    v1_config_path: str | Path,
    historical_pace_identity_path: Optional[str | Path] = None,
    historical_pace_readback_path: Optional[str | Path] = None,
    historical_pressure_gauge_io_path: Optional[str | Path] = None,
) -> dict[str, Any]:
    payload = build_rs485_v1_v2_alignment_matrix(
        current_a2_14_dir=current_a2_14_dir,
        v1_config_path=v1_config_path,
        historical_pace_identity_path=historical_pace_identity_path,
        historical_pace_readback_path=historical_pace_readback_path,
        historical_pressure_gauge_io_path=historical_pressure_gauge_io_path,
    )
    target = Path(output_path)
    target.parent.mkdir(parents=True, exist_ok=True)
    _json_dump(target, payload)
    return payload
