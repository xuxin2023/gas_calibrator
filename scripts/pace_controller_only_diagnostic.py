from __future__ import annotations

import argparse
import csv
import json
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Sequence

from gas_calibrator.devices.pace5000 import Pace5000


CSV_FIELDS = [
    "timestamp",
    "idn",
    "inst_vers",
    "pressure_hpa",
    "outp_state",
    "isol_state",
    "mode",
    "vent_status",
    "vent_completed_latched",
    "effort",
    "comp1",
    "comp2",
    "barometric_pressure_hpa",
    "in_limits_pressure_hpa",
    "in_limits_state",
    "in_limits_time_s",
    "measured_slew_hpa_s",
    "oper_cond",
    "oper_pres_cond",
    "oper_pres_even",
    "oper_pres_vent_complete_bit",
    "oper_pres_in_limits_bit",
    "syst_err",
]

MATRIX_CSV_FIELDS = [
    "step",
    "actions",
    "action_responses",
    "idn",
    "inst_vers",
    "outp_stat",
    "outp_isol_stat",
    "outp_mode",
    "vent",
    "eff",
    "comp1",
    "comp2",
    "sens_pres_bar",
    "sens_pres_inl",
    "sens_pres_inl_time",
    "sens_pres_slew",
    "stat_oper_pres_cond",
    "stat_oper_pres_even",
    "syst_err",
    "parsed_vent",
    "parsed_eff",
    "parsed_comp1",
    "parsed_comp2",
    "parsed_stat_oper_pres_cond",
    "parsed_stat_oper_pres_even",
    "cond_vent_complete_bit0",
    "cond_in_limits_bit2",
    "even_vent_complete_bit0",
    "even_in_limits_bit2",
    "vent_status_is_3",
    "pace_legacy_vent_state_3_suspect",
    "pace_atmosphere_connected_latched_state_suspect",
    "legacy_vent3_control_ready_used",
    "legacy_vent3_accept_scope",
    "vent_status_3_count",
    "vent3_hard_blocked",
    "vent3_watchlist_only",
    "vent3_control_ready_attempted",
    "vent3_control_ready_prevented",
    "vent3_block_scope",
    "ack_callback_invoked",
    "vent3_post_window_status",
    "clear_attempt_sequence",
    "clear_result",
    "vent_complete_bit_before",
    "vent_complete_bit_after",
]

UI_ACK_CSV_FIELDS = [
    "phase",
    "sample_index",
    "timestamp",
    "idn",
    "inst_vers",
    "outp_stat",
    "outp_isol_stat",
    "outp_mode",
    "vent",
    "eff",
    "comp1",
    "comp2",
    "sens_pres_bar",
    "sens_pres_inl",
    "sens_pres_inl_time",
    "sens_pres_slew",
    "stat_oper_pres_cond",
    "stat_oper_pres_even",
    "syst_err",
    "parsed_vent",
    "parsed_eff",
    "parsed_comp1",
    "parsed_comp2",
    "parsed_stat_oper_pres_cond",
    "parsed_stat_oper_pres_even",
    "cond_vent_complete_bit0",
    "cond_in_limits_bit2",
    "even_vent_complete_bit0",
    "even_in_limits_bit2",
    "vent_status_is_3",
    "pace_legacy_vent_state_3_suspect",
    "pace_atmosphere_connected_latched_state_suspect",
    "legacy_vent3_control_ready_used",
    "legacy_vent3_accept_scope",
    "vent_status_3_count",
    "vent3_hard_blocked",
    "vent3_watchlist_only",
    "vent3_control_ready_attempted",
    "vent3_control_ready_prevented",
    "vent3_block_scope",
    "ack_callback_invoked",
    "vent3_post_window_status",
]

MATRIX_QUERY_COMMANDS = [
    ("idn", "*IDN?"),
    ("inst_vers", ":INST:VERS?"),
    ("outp_stat", ":OUTP:STAT?"),
    ("outp_isol_stat", ":OUTP:ISOL:STAT?"),
    ("outp_mode", ":OUTP:MODE?"),
    ("vent", ":SOUR:PRES:LEV:IMM:AMPL:VENT?"),
    ("eff", ":SOUR:PRES:EFF?"),
    ("comp1", ":SOUR:PRES:COMP1?"),
    ("comp2", ":SOUR:PRES:COMP2?"),
    ("sens_pres_bar", ":SENS:PRES:BAR?"),
    ("sens_pres_inl", ":SENS:PRES:INL?"),
    ("sens_pres_inl_time", ":SENS:PRES:INL:TIME?"),
    ("sens_pres_slew", ":SENS:PRES:SLEW?"),
    ("stat_oper_pres_cond", ":STAT:OPER:PRES:COND?"),
    ("stat_oper_pres_even", ":STAT:OPER:PRES:EVEN?"),
    ("syst_err", ":SYST:ERR?"),
]

MATRIX_STEPS = [
    ("A_read_only", []),
    ("B_cls_only", ["*CLS"]),
    ("C_even_read_only", [":STAT:OPER:PRES:EVEN?"]),
    ("D_vent0_only", [":SOUR:PRES:LEV:IMM:AMPL:VENT 0"]),
    ("E_even_then_vent0", [":STAT:OPER:PRES:EVEN?", ":SOUR:PRES:LEV:IMM:AMPL:VENT 0"]),
    ("F_cls_then_vent0", ["*CLS", ":SOUR:PRES:LEV:IMM:AMPL:VENT 0"]),
    ("G_cls_even_vent0", ["*CLS", ":STAT:OPER:PRES:EVEN?", ":SOUR:PRES:LEV:IMM:AMPL:VENT 0"]),
]


def _timestamp() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _snapshot_row(status: Dict[str, Any], *, timestamp: Optional[str] = None) -> Dict[str, Any]:
    return {
        "timestamp": timestamp or _timestamp(),
        "idn": status.get("idn", status.get("device_identity", "")),
        "inst_vers": status.get("inst_vers", status.get("instrument_version", "")),
        "pressure_hpa": status.get("pressure_hpa", ""),
        "outp_state": status.get("output_state", ""),
        "isol_state": status.get("isolation_state", ""),
        "mode": status.get("output_mode", ""),
        "vent_status": status.get("vent_status", ""),
        "vent_completed_latched": status.get("vent_completed_latched", ""),
        "effort": status.get("effort", ""),
        "comp1": status.get("comp1", ""),
        "comp2": status.get("comp2", ""),
        "barometric_pressure_hpa": status.get("barometric_pressure_hpa", ""),
        "in_limits_pressure_hpa": status.get("in_limits_pressure_hpa", ""),
        "in_limits_state": status.get("in_limits_state", ""),
        "in_limits_time_s": status.get("in_limits_time_s", ""),
        "measured_slew_hpa_s": status.get("measured_slew_hpa_s", ""),
        "oper_cond": status.get("oper_condition", ""),
        "oper_pres_cond": status.get("oper_pressure_condition", ""),
        "oper_pres_even": status.get("oper_pressure_event", ""),
        "oper_pres_vent_complete_bit": status.get("oper_pressure_vent_complete_bit", ""),
        "oper_pres_in_limits_bit": status.get("oper_pressure_in_limits_bit", ""),
        "syst_err": status.get("syst_err", ""),
    }


def _query_allowed_snapshot(pace: Pace5000) -> Dict[str, Any]:
    snapshot: Dict[str, Any] = {}
    for key, command in MATRIX_QUERY_COMMANDS:
        try:
            snapshot[key] = str(pace.query(command) or "").strip()
        except Exception as exc:
            snapshot[key] = f"ERROR:{exc}"
    return snapshot


def _diagnostic_status_from_allowed_snapshot(pace: Pace5000) -> Dict[str, Any]:
    snapshot = _query_allowed_snapshot(pace)
    parsed = _parsed_allowed_snapshot(snapshot)
    cond_value = parsed.get("stat_oper_pres_cond")
    even_value = parsed.get("stat_oper_pres_even")
    vent_value = parsed.get("vent")
    inl_text = snapshot.get("sens_pres_inl")
    in_limits_pressure = _parse_first_float(inl_text)
    in_limits_state = None
    if inl_text:
        parts = _response_payload(inl_text).split(",")
        if len(parts) >= 2:
            in_limits_state = _parse_first_int(parts[1])
    cond_bits = parsed.get("cond_bits", {})
    even_bits = parsed.get("even_bits", {})
    return {
        "idn": snapshot.get("idn", ""),
        "inst_vers": snapshot.get("inst_vers", ""),
        "pressure_hpa": "",
        "output_state": _parse_first_int(snapshot.get("outp_stat")),
        "isolation_state": _parse_first_int(snapshot.get("outp_isol_stat")),
        "output_mode": _response_payload(snapshot.get("outp_mode")),
        "vent_status": vent_value,
        "vent_completed_latched": vent_value == Pace5000.VENT_STATUS_COMPLETED,
        "effort": _parse_first_float(snapshot.get("eff")),
        "comp1": _parse_first_float(snapshot.get("comp1")),
        "comp2": _parse_first_float(snapshot.get("comp2")),
        "barometric_pressure_hpa": _parse_first_float(snapshot.get("sens_pres_bar")),
        "in_limits_pressure_hpa": in_limits_pressure,
        "in_limits_state": in_limits_state,
        "in_limits_time_s": _parse_first_float(snapshot.get("sens_pres_inl_time")),
        "measured_slew_hpa_s": _parse_first_float(snapshot.get("sens_pres_slew")),
        "oper_condition": "",
        "oper_pressure_condition": cond_value,
        "oper_pressure_event": even_value,
        "oper_pressure_vent_complete_bit": (
            True
            if cond_bits.get("vent_complete_bit0") is True or even_bits.get("vent_complete_bit0") is True
            else False
            if cond_bits.get("vent_complete_bit0") is not None or even_bits.get("vent_complete_bit0") is not None
            else ""
        ),
        "oper_pressure_in_limits_bit": (
            True
            if cond_bits.get("in_limits_bit2") is True or even_bits.get("in_limits_bit2") is True
            else False
            if cond_bits.get("in_limits_bit2") is not None or even_bits.get("in_limits_bit2") is not None
            else ""
        ),
        "syst_err": snapshot.get("syst_err", ""),
    }


def _sanitize_completed_vent_latch(pace: Pace5000) -> Dict[str, Any]:
    before = _diagnostic_status_from_allowed_snapshot(pace)
    clear_result = pace.clear_completed_vent_latch_if_present()
    after = _diagnostic_status_from_allowed_snapshot(pace)
    return {
        "performed": bool(clear_result.get("clear_attempted")),
        "command": clear_result.get("command", ""),
        "before_status": clear_result.get("before_status"),
        "after_status": clear_result.get("after_status"),
        "cleared": bool(clear_result.get("cleared", False)),
        "before": _snapshot_row(before),
        "after": _snapshot_row(after),
    }


def _write_csv(
    path: Path,
    rows: Sequence[Dict[str, Any]],
    *,
    fieldnames: Optional[Sequence[str]] = None,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(fieldnames or CSV_FIELDS))
        writer.writeheader()
        writer.writerows(rows)


def _response_payload(text: Any) -> str:
    raw = str(text or "").strip()
    if not raw:
        return ""
    parts = raw.split(None, 1)
    return parts[1].strip() if len(parts) > 1 else raw


def _parse_first_float(text: Any) -> Optional[float]:
    payload = _response_payload(text)
    if not payload:
        return None
    import re

    match = re.search(r"[-+]?\d+(?:\.\d+)?(?:[eE][-+]?\d+)?", payload)
    if not match:
        return None
    try:
        return float(match.group(0))
    except Exception:
        return None


def _parse_first_int(text: Any) -> Optional[int]:
    value = _parse_first_float(text)
    if value is None:
        return None
    try:
        return int(value)
    except Exception:
        return None


def _decode_oper_bits(value: Optional[int]) -> Dict[str, Optional[bool]]:
    if value is None:
        return {
            "vent_complete_bit0": None,
            "in_limits_bit2": None,
        }
    return {
        "vent_complete_bit0": bool(value & 1),
        "in_limits_bit2": bool(value & 4),
    }


def _controller_only_vent3_fields(
    vent_value: Optional[int],
    *,
    ack_callback_invoked: bool = False,
    post_window_status: Any = None,
) -> Dict[str, Any]:
    is_vent3 = vent_value == Pace5000.VENT_STATUS_TRAPPED_PRESSURE
    return {
        "vent_status_is_3": is_vent3,
        "pace_legacy_vent_state_3_suspect": is_vent3,
        "pace_atmosphere_connected_latched_state_suspect": is_vent3,
        "legacy_vent3_control_ready_used": False,
        "legacy_vent3_accept_scope": "none",
        "vent_status_3_count": 1 if is_vent3 else 0,
        "vent3_hard_blocked": False,
        "vent3_watchlist_only": bool(is_vent3),
        "vent3_control_ready_attempted": False,
        "vent3_control_ready_prevented": False,
        "vent3_block_scope": "none",
        "ack_callback_invoked": bool(ack_callback_invoked),
        "vent3_post_window_status": "" if post_window_status in ("", None) else post_window_status,
    }


def _parsed_allowed_snapshot(snapshot: Dict[str, Any]) -> Dict[str, Any]:
    cond_value = _parse_first_int(snapshot.get("stat_oper_pres_cond"))
    even_value = _parse_first_int(snapshot.get("stat_oper_pres_even"))
    vent_value = _parse_first_int(snapshot.get("vent"))
    parsed = {
        "vent": vent_value,
        "eff": _parse_first_float(snapshot.get("eff")),
        "comp1": _parse_first_float(snapshot.get("comp1")),
        "comp2": _parse_first_float(snapshot.get("comp2")),
        "stat_oper_pres_cond": cond_value,
        "stat_oper_pres_even": even_value,
        "cond_bits": _decode_oper_bits(cond_value),
        "even_bits": _decode_oper_bits(even_value),
    }
    parsed.update(_controller_only_vent3_fields(vent_value))
    return parsed


def _vent_complete_bit_from_step(step: Dict[str, Any]) -> Optional[bool]:
    parsed = step.get("snapshot", {}).get("parsed", {})
    cond_bits = parsed.get("cond_bits", {})
    even_bits = parsed.get("even_bits", {})
    values = [
        cond_bits.get("vent_complete_bit0"),
        even_bits.get("vent_complete_bit0"),
    ]
    if True in values:
        return True
    if False in values:
        return False
    return None


def _clear_attempt_sequence(step: Dict[str, Any]) -> str:
    return " -> ".join(action.get("command", "") for action in step.get("actions", []))


def _clear_result_from_step(step: Dict[str, Any]) -> str:
    sequence = _clear_attempt_sequence(step)
    if ":SOUR:PRES:LEV:IMM:AMPL:VENT 0" not in sequence:
        return "no_clear_attempt"

    vent_value = step.get("snapshot", {}).get("parsed", {}).get("vent")
    if vent_value == Pace5000.VENT_STATUS_IDLE:
        return "cleared_to_0"
    if vent_value == Pace5000.VENT_STATUS_TRAPPED_PRESSURE:
        return "persistent_3"
    if vent_value == Pace5000.VENT_STATUS_COMPLETED:
        return "still_completed_2"
    if vent_value is None:
        return "unknown_after_status"
    return f"after_status_{vent_value}"


def _is_zero_system_error(text: Any) -> bool:
    payload = _response_payload(text).replace(" ", "").lower()
    return payload.startswith("0,") or payload == "0" or "noerror" in payload


def _build_matrix_analysis(rows: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    steps_with_vent_status_3: List[str] = []
    clear_attempts: List[Dict[str, Any]] = []
    syst_err_all_zero = True
    eff_all_zero = True
    observation_only_steps: List[str] = []

    for index, row in enumerate(rows):
        parsed = row.get("snapshot", {}).get("parsed", {})
        if parsed.get("vent") == Pace5000.VENT_STATUS_TRAPPED_PRESSURE:
            steps_with_vent_status_3.append(str(row.get("step", "")))

        eff_value = parsed.get("eff")
        if eff_value is not None and float(eff_value) != 0.0:
            eff_all_zero = False

        if not _is_zero_system_error(row.get("snapshot", {}).get("syst_err")):
            syst_err_all_zero = False

        sequence = _clear_attempt_sequence(row)
        clear_result = row.get("clear_result", "")
        if sequence and clear_result == "no_clear_attempt":
            observation_only_steps.append(str(row.get("step", "")))

        if ":SOUR:PRES:LEV:IMM:AMPL:VENT 0" not in sequence:
            continue

        previous = rows[index - 1] if index > 0 else row
        clear_attempts.append(
            {
                "step": row.get("step", ""),
                "clear_attempt_sequence": sequence,
                "clear_result": clear_result,
                "vent_complete_bit_before": row.get("vent_complete_bit_before"),
                "vent_complete_bit_after": row.get("vent_complete_bit_after"),
                "vent_status_before": previous.get("snapshot", {}).get("parsed", {}).get("vent"),
                "vent_status_after": parsed.get("vent"),
            }
        )

    conclusion_codes: List[str] = []
    if steps_with_vent_status_3:
        conclusion_codes.append("legacy_vent_state_problem")
    if clear_attempts and all(item.get("vent_status_after") == Pace5000.VENT_STATUS_TRAPPED_PRESSURE for item in clear_attempts):
        conclusion_codes.append("firmware_state_persistent")
    if observation_only_steps:
        conclusion_codes.append("clear_sequence_observation_only")

    return {
        "vent_status_3_count": len(steps_with_vent_status_3),
        "steps_with_vent_status_3": steps_with_vent_status_3,
        "pace_legacy_vent_state_3_suspect": bool(steps_with_vent_status_3),
        "pace_atmosphere_connected_latched_state_suspect": bool(steps_with_vent_status_3),
        "legacy_vent3_control_ready_used": False,
        "legacy_vent3_accept_scope": "none",
        "vent3_hard_blocked": False,
        "vent3_watchlist_only": bool(steps_with_vent_status_3),
        "vent3_control_ready_attempted": False,
        "vent3_control_ready_prevented": False,
        "vent3_block_scope": "none",
        "ack_callback_invoked": False,
        "vent3_post_window_status": "",
        "clear_attempts": clear_attempts,
        "observation_only_steps": observation_only_steps,
        "eff_all_zero": eff_all_zero,
        "syst_err_all_zero": syst_err_all_zero,
        "conclusion_codes": conclusion_codes,
    }


def _matrix_snapshot_row(step: Dict[str, Any]) -> Dict[str, Any]:
    snapshot = step.get("snapshot", {})
    parsed = snapshot.get("parsed", {})
    cond_bits = parsed.get("cond_bits", {})
    even_bits = parsed.get("even_bits", {})
    return {
        "step": step.get("step", ""),
        "actions": " | ".join(action.get("command", "") for action in step.get("actions", [])),
        "action_responses": " | ".join(str(action.get("response", "") or "") for action in step.get("actions", [])),
        "idn": snapshot.get("idn", ""),
        "inst_vers": snapshot.get("inst_vers", ""),
        "outp_stat": snapshot.get("outp_stat", ""),
        "outp_isol_stat": snapshot.get("outp_isol_stat", ""),
        "outp_mode": snapshot.get("outp_mode", ""),
        "vent": snapshot.get("vent", ""),
        "eff": snapshot.get("eff", ""),
        "comp1": snapshot.get("comp1", ""),
        "comp2": snapshot.get("comp2", ""),
        "sens_pres_bar": snapshot.get("sens_pres_bar", ""),
        "sens_pres_inl": snapshot.get("sens_pres_inl", ""),
        "sens_pres_inl_time": snapshot.get("sens_pres_inl_time", ""),
        "sens_pres_slew": snapshot.get("sens_pres_slew", ""),
        "stat_oper_pres_cond": snapshot.get("stat_oper_pres_cond", ""),
        "stat_oper_pres_even": snapshot.get("stat_oper_pres_even", ""),
        "syst_err": snapshot.get("syst_err", ""),
        "parsed_vent": parsed.get("vent", ""),
        "parsed_eff": parsed.get("eff", ""),
        "parsed_comp1": parsed.get("comp1", ""),
        "parsed_comp2": parsed.get("comp2", ""),
        "parsed_stat_oper_pres_cond": parsed.get("stat_oper_pres_cond", ""),
        "parsed_stat_oper_pres_even": parsed.get("stat_oper_pres_even", ""),
        "cond_vent_complete_bit0": cond_bits.get("vent_complete_bit0", ""),
        "cond_in_limits_bit2": cond_bits.get("in_limits_bit2", ""),
        "even_vent_complete_bit0": even_bits.get("vent_complete_bit0", ""),
        "even_in_limits_bit2": even_bits.get("in_limits_bit2", ""),
        "vent_status_is_3": parsed.get("vent_status_is_3", False),
        "pace_legacy_vent_state_3_suspect": parsed.get("pace_legacy_vent_state_3_suspect", False),
        "pace_atmosphere_connected_latched_state_suspect": parsed.get(
            "pace_atmosphere_connected_latched_state_suspect",
            False,
        ),
        "legacy_vent3_control_ready_used": step.get("legacy_vent3_control_ready_used", False),
        "legacy_vent3_accept_scope": step.get("legacy_vent3_accept_scope", "none"),
        "vent_status_3_count": step.get("vent_status_3_count", 0),
        "vent3_hard_blocked": step.get("vent3_hard_blocked", False),
        "vent3_watchlist_only": step.get("vent3_watchlist_only", False),
        "vent3_control_ready_attempted": step.get("vent3_control_ready_attempted", False),
        "vent3_control_ready_prevented": step.get("vent3_control_ready_prevented", False),
        "vent3_block_scope": step.get("vent3_block_scope", "none"),
        "ack_callback_invoked": step.get("ack_callback_invoked", False),
        "vent3_post_window_status": step.get("vent3_post_window_status", ""),
        "clear_attempt_sequence": step.get("clear_attempt_sequence", ""),
        "clear_result": step.get("clear_result", ""),
        "vent_complete_bit_before": step.get("vent_complete_bit_before", ""),
        "vent_complete_bit_after": step.get("vent_complete_bit_after", ""),
    }


def run_controller_only_diagnostic(
    *,
    port: str,
    baudrate: int = 9600,
    timeout: float = 1.0,
    samples: int = 120,
    interval_s: float = 0.5,
    output_dir: Path | str,
    allow_write_sanitize: bool = False,
    pace_factory: Optional[Callable[..., Pace5000]] = None,
) -> Dict[str, Any]:
    output_path = Path(output_dir).resolve()
    output_path.mkdir(parents=True, exist_ok=True)
    factory = pace_factory or Pace5000
    pace = factory(port, baudrate=baudrate, timeout=timeout)
    rows: List[Dict[str, Any]] = []
    sanitize_summary: Dict[str, Any] = {"performed": False}
    pace.open()
    try:
        if allow_write_sanitize:
            sanitize_summary = _sanitize_completed_vent_latch(pace)
        for index in range(max(1, int(samples))):
            rows.append(_snapshot_row(_diagnostic_status_from_allowed_snapshot(pace)))
            if index + 1 < max(1, int(samples)):
                time.sleep(max(0.0, float(interval_s)))
    finally:
        pace.close()

    csv_path = output_path / "pace_controller_only_diagnostic.csv"
    json_path = output_path / "pace_controller_only_diagnostic.json"
    _write_csv(csv_path, rows)
    summary = {
        "port": port,
        "baudrate": baudrate,
        "timeout": timeout,
        "samples": max(1, int(samples)),
        "interval_s": max(0.0, float(interval_s)),
        "allow_write_sanitize": bool(allow_write_sanitize),
        "sanitize_summary": sanitize_summary,
        "csv_path": str(csv_path),
        "json_path": str(json_path),
        "rows": rows,
        "notes": [
            "read-only by default",
            "allow-write-sanitize only sends VENT 0 when a completed vent latch is present",
            "no setpoint, output enable, vent-on, main gas path, or non-standard extension writes are performed",
        ],
    }
    json_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    return summary


def run_controller_only_matrix(
    *,
    port: str,
    baudrate: int = 9600,
    timeout: float = 1.0,
    output_dir: Path | str,
    action_settle_s: float = 0.2,
    query_interval_s: float = 0.1,
    pace_factory: Optional[Callable[..., Pace5000]] = None,
) -> Dict[str, Any]:
    output_path = Path(output_dir).resolve()
    output_path.mkdir(parents=True, exist_ok=True)
    factory = pace_factory or Pace5000
    pace = factory(port, baudrate=baudrate, timeout=timeout)
    rows: List[Dict[str, Any]] = []
    pace.open()
    try:
        for step_name, actions in MATRIX_STEPS:
            action_rows: List[Dict[str, Any]] = []
            for command in actions:
                if command.endswith("?"):
                    response = str(pace.query(command) or "").strip()
                    action_rows.append({"command": command, "response": response})
                else:
                    pace.write(command)
                    action_rows.append({"command": command, "response": ""})
                time.sleep(max(0.0, float(action_settle_s)))

            snapshot: Dict[str, Any] = {}
            for key, command in MATRIX_QUERY_COMMANDS:
                try:
                    snapshot[key] = str(pace.query(command) or "").strip()
                except Exception as exc:
                    snapshot[key] = f"ERROR:{exc}"
                time.sleep(max(0.0, float(query_interval_s)))

            snapshot["parsed"] = _parsed_allowed_snapshot(snapshot)
            current_step = {
                "step": step_name,
                "actions": action_rows,
                "snapshot": snapshot,
                **_controller_only_vent3_fields(snapshot["parsed"].get("vent")),
            }
            previous_step = rows[-1] if rows else current_step
            current_step["clear_attempt_sequence"] = _clear_attempt_sequence(current_step)
            current_step["clear_result"] = _clear_result_from_step(current_step)
            current_step["vent_complete_bit_before"] = _vent_complete_bit_from_step(previous_step)
            current_step["vent_complete_bit_after"] = _vent_complete_bit_from_step(current_step)
            rows.append(
                current_step
            )
    finally:
        pace.close()

    csv_path = output_path / "pace_controller_only_matrix.csv"
    json_path = output_path / "pace_controller_only_matrix.json"
    _write_csv(csv_path, [_matrix_snapshot_row(row) for row in rows], fieldnames=MATRIX_CSV_FIELDS)
    summary = {
        "port": port,
        "baudrate": baudrate,
        "timeout": timeout,
        "action_settle_s": max(0.0, float(action_settle_s)),
        "query_interval_s": max(0.0, float(query_interval_s)),
        "csv_path": str(csv_path),
        "json_path": str(json_path),
        "steps": rows,
        "analysis": _build_matrix_analysis(rows),
        "notes": [
            "controller-only A-G matrix",
            "no setpoint, output enable, vent-on, or gas-path writes are performed",
            "matrix only uses *CLS, :STAT:OPER:PRES:EVEN?, and :SOUR:PRES:LEV:IMM:AMPL:VENT 0",
            "VENT=3 is treated as watchlist-only; popup visibility and SCPI VENT values are not assumed to be one-to-one",
            "controller-only matrix intentionally excludes :SENS:PRES:CONT? because 02.00.07 can contaminate :SYST:ERR? with -113",
        ],
    }
    json_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    return summary


def _take_allowed_snapshot(
    pace: Pace5000,
    *,
    ack_callback_invoked: bool = False,
) -> Dict[str, Any]:
    snapshot = _query_allowed_snapshot(pace)
    parsed = _parsed_allowed_snapshot(snapshot)
    parsed.update(
        _controller_only_vent3_fields(
            parsed.get("vent"),
            ack_callback_invoked=ack_callback_invoked,
            post_window_status=parsed.get("vent") if ack_callback_invoked else None,
        )
    )
    snapshot["parsed"] = parsed
    return snapshot


def _ui_ack_snapshot_row(sample: Dict[str, Any]) -> Dict[str, Any]:
    snapshot = sample.get("snapshot", {})
    parsed = snapshot.get("parsed", {})
    cond_bits = parsed.get("cond_bits", {})
    even_bits = parsed.get("even_bits", {})
    return {
        "phase": sample.get("phase", ""),
        "sample_index": sample.get("sample_index", ""),
        "timestamp": sample.get("timestamp", ""),
        "idn": snapshot.get("idn", ""),
        "inst_vers": snapshot.get("inst_vers", ""),
        "outp_stat": snapshot.get("outp_stat", ""),
        "outp_isol_stat": snapshot.get("outp_isol_stat", ""),
        "outp_mode": snapshot.get("outp_mode", ""),
        "vent": snapshot.get("vent", ""),
        "eff": snapshot.get("eff", ""),
        "comp1": snapshot.get("comp1", ""),
        "comp2": snapshot.get("comp2", ""),
        "sens_pres_bar": snapshot.get("sens_pres_bar", ""),
        "sens_pres_inl": snapshot.get("sens_pres_inl", ""),
        "sens_pres_inl_time": snapshot.get("sens_pres_inl_time", ""),
        "sens_pres_slew": snapshot.get("sens_pres_slew", ""),
        "stat_oper_pres_cond": snapshot.get("stat_oper_pres_cond", ""),
        "stat_oper_pres_even": snapshot.get("stat_oper_pres_even", ""),
        "syst_err": snapshot.get("syst_err", ""),
        "parsed_vent": parsed.get("vent", ""),
        "parsed_eff": parsed.get("eff", ""),
        "parsed_comp1": parsed.get("comp1", ""),
        "parsed_comp2": parsed.get("comp2", ""),
        "parsed_stat_oper_pres_cond": parsed.get("stat_oper_pres_cond", ""),
        "parsed_stat_oper_pres_even": parsed.get("stat_oper_pres_even", ""),
        "cond_vent_complete_bit0": cond_bits.get("vent_complete_bit0", ""),
        "cond_in_limits_bit2": cond_bits.get("in_limits_bit2", ""),
        "even_vent_complete_bit0": even_bits.get("vent_complete_bit0", ""),
        "even_in_limits_bit2": even_bits.get("in_limits_bit2", ""),
        "vent_status_is_3": parsed.get("vent_status_is_3", False),
        "pace_legacy_vent_state_3_suspect": parsed.get("pace_legacy_vent_state_3_suspect", False),
        "pace_atmosphere_connected_latched_state_suspect": parsed.get(
            "pace_atmosphere_connected_latched_state_suspect",
            False,
        ),
        "legacy_vent3_control_ready_used": parsed.get("legacy_vent3_control_ready_used", False),
        "legacy_vent3_accept_scope": parsed.get("legacy_vent3_accept_scope", "none"),
        "vent_status_3_count": parsed.get("vent_status_3_count", 0),
        "vent3_hard_blocked": parsed.get("vent3_hard_blocked", False),
        "vent3_watchlist_only": parsed.get("vent3_watchlist_only", False),
        "vent3_control_ready_attempted": parsed.get("vent3_control_ready_attempted", False),
        "vent3_control_ready_prevented": parsed.get("vent3_control_ready_prevented", False),
        "vent3_block_scope": parsed.get("vent3_block_scope", "none"),
        "ack_callback_invoked": parsed.get("ack_callback_invoked", False),
        "vent3_post_window_status": parsed.get("vent3_post_window_status", ""),
    }


def _build_ui_ack_analysis(samples: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    pre_samples = [sample for sample in samples if sample.get("phase") != "phase3_post_window"]
    post_samples = [sample for sample in samples if sample.get("phase") == "phase3_post_window"]
    pre_vents = [sample.get("snapshot", {}).get("parsed", {}).get("vent") for sample in pre_samples]
    post_vents = [sample.get("snapshot", {}).get("parsed", {}).get("vent") for sample in post_samples]
    pre_cond = [
        sample.get("snapshot", {}).get("parsed", {}).get("cond_bits", {}).get("vent_complete_bit0")
        for sample in pre_samples
    ]
    post_cond = [
        sample.get("snapshot", {}).get("parsed", {}).get("cond_bits", {}).get("vent_complete_bit0")
        for sample in post_samples
    ]
    pre_even = [
        sample.get("snapshot", {}).get("parsed", {}).get("even_bits", {}).get("vent_complete_bit0")
        for sample in pre_samples
    ]
    post_even = [
        sample.get("snapshot", {}).get("parsed", {}).get("even_bits", {}).get("vent_complete_bit0")
        for sample in post_samples
    ]
    conclusion_codes: List[str] = []
    pre_window_had_vent3 = any(value == Pace5000.VENT_STATUS_TRAPPED_PRESSURE for value in pre_vents if value is not None)
    pre_window_had_cond_bit0 = any(value is True for value in pre_cond if value is not None)
    pre_window_had_even_bit0 = any(value is True for value in pre_even if value is not None)
    callback_invoked_in_samples = any(
        sample.get("snapshot", {}).get("parsed", {}).get("ack_callback_invoked")
        for sample in post_samples
    )
    vent3_cleared_after_window = bool(post_vents) and all(
        value != Pace5000.VENT_STATUS_TRAPPED_PRESSURE for value in post_vents if value is not None
    )
    vent3_cleared_after_window = bool(pre_window_had_vent3 and vent3_cleared_after_window)
    cond_bit0_cleared_after_window = bool(
        pre_window_had_cond_bit0 and post_cond and all(value is False for value in post_cond if value is not None)
    )
    even_bit0_cleared_after_window = bool(
        pre_window_had_even_bit0 and post_even and all(value is False for value in post_even if value is not None)
    )
    ack_callback_invoked = bool(callback_invoked_in_samples)
    if pre_vents and all(value == Pace5000.VENT_STATUS_TRAPPED_PRESSURE for value in pre_vents if value is not None):
        conclusion_codes.append("pre_window_vent3_persistent")
    if vent3_cleared_after_window:
        conclusion_codes.append("vent_status_changed_after_window")
    if cond_bit0_cleared_after_window:
        conclusion_codes.append("cond_bit0_changed_after_window")
    return {
        "vent_status_3_count": sum(
            1 for sample in samples if sample.get("snapshot", {}).get("parsed", {}).get("vent") == Pace5000.VENT_STATUS_TRAPPED_PRESSURE
        ),
        "pace_legacy_vent_state_3_suspect": any(
            sample.get("snapshot", {}).get("parsed", {}).get("pace_legacy_vent_state_3_suspect") for sample in samples
        ),
        "pace_atmosphere_connected_latched_state_suspect": any(
            sample.get("snapshot", {}).get("parsed", {}).get("pace_atmosphere_connected_latched_state_suspect")
            for sample in samples
        ),
        "legacy_vent3_control_ready_used": False,
        "legacy_vent3_accept_scope": "none",
        "vent3_hard_blocked": False,
        "vent3_watchlist_only": any(
            sample.get("snapshot", {}).get("parsed", {}).get("vent3_watchlist_only") for sample in samples
        ),
        "vent3_control_ready_attempted": False,
        "vent3_control_ready_prevented": False,
        "vent3_block_scope": "none",
        "ack_callback_invoked": ack_callback_invoked,
        "vent3_post_window_status": post_vents[0] if post_vents else "",
        "pre_window_vent_values": pre_vents,
        "post_window_vent_values": post_vents,
        "pre_window_cond_bit0_values": pre_cond,
        "post_window_cond_bit0_values": post_cond,
        "pre_window_even_bit0_values": pre_even,
        "post_window_even_bit0_values": post_even,
        "vent3_persisted_before_window": bool(pre_vents) and all(
            value == Pace5000.VENT_STATUS_TRAPPED_PRESSURE for value in pre_vents if value is not None
        ),
        "vent3_cleared_after_window": vent3_cleared_after_window,
        "cond_bit0_cleared_after_window": cond_bit0_cleared_after_window,
        "even_bit0_cleared_after_window": even_bit0_cleared_after_window,
        "conclusion_codes": conclusion_codes,
    }


def run_controller_only_ui_ack_experiment(
    *,
    port: str,
    baudrate: int = 9600,
    timeout: float = 1.0,
    output_dir: Path | str,
    interval_s: float = 0.5,
    hold_samples: int = 5,
    post_ack_samples: int = 5,
    ack_wait_s: float = 15.0,
    ack_callback: Optional[Callable[[], None]] = None,
    pace_factory: Optional[Callable[..., Pace5000]] = None,
) -> Dict[str, Any]:
    output_path = Path(output_dir).resolve()
    output_path.mkdir(parents=True, exist_ok=True)
    factory = pace_factory or Pace5000
    pace = factory(port, baudrate=baudrate, timeout=timeout)
    samples: List[Dict[str, Any]] = []
    callback_invoked = False

    def _record_phase(phase: str, count: int, *, ack_callback_invoked: bool) -> None:
        total = max(1, int(count))
        for index in range(total):
            samples.append(
                {
                    "phase": phase,
                    "sample_index": index,
                    "timestamp": _timestamp(),
                    "snapshot": _take_allowed_snapshot(
                        pace,
                        ack_callback_invoked=ack_callback_invoked,
                    ),
                }
            )
            if index + 1 < total:
                time.sleep(max(0.0, float(interval_s)))

    pace.open()
    try:
        _record_phase("phase1_window_initial", 1, ack_callback_invoked=False)
        _record_phase("phase2_window_hold", hold_samples, ack_callback_invoked=False)
        if callable(ack_callback):
            ack_callback()
            callback_invoked = True
        elif ack_wait_s > 0:
            print(
                f"Wait {ack_wait_s:.1f}s for the manual observation window, "
                "then post-window sampling continues."
            )
            time.sleep(max(0.0, float(ack_wait_s)))
        _record_phase("phase3_post_window", post_ack_samples, ack_callback_invoked=callback_invoked)
    finally:
        pace.close()

    csv_path = output_path / "pace_controller_only_ui_ack_experiment.csv"
    json_path = output_path / "pace_controller_only_ui_ack_experiment.json"
    _write_csv(csv_path, [_ui_ack_snapshot_row(sample) for sample in samples], fieldnames=UI_ACK_CSV_FIELDS)
    summary = {
        "port": port,
        "baudrate": baudrate,
        "timeout": timeout,
        "interval_s": max(0.0, float(interval_s)),
        "hold_samples": max(1, int(hold_samples)),
        "post_window_samples": max(1, int(post_ack_samples)),
        "ack_wait_s": max(0.0, float(ack_wait_s)),
        "csv_path": str(csv_path),
        "json_path": str(json_path),
        "samples": samples,
        "analysis": _build_ui_ack_analysis(samples),
        "notes": [
            "controller-only UI observation-window experiment",
            "phase1/phase2 are read-only snapshots during the operator observation window before phase3",
            "phase3 starts only after the optional callback step or the configured observation wait window",
            "this experiment never sends VENT 0 and does not treat SCPI VENT 0 as equivalent to a front-panel popup or manual acknowledgement",
            "front-panel popup visibility and SCPI VENT values are not assumed to be one-to-one; real read-only runs have observed popup windows while SCPI still returned VENT=2",
            "controller-only experiment intentionally excludes :SENS:PRES:CONT? because 02.00.07 can contaminate :SYST:ERR? with -113",
        ],
    }
    json_path.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    return summary


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="PACE controller-only diagnostic poller.")
    parser.add_argument("--port", required=True, help="PACE serial port, for example COM7.")
    parser.add_argument("--baudrate", type=int, default=9600)
    parser.add_argument("--timeout", type=float, default=1.0)
    parser.add_argument("--samples", type=int, default=120, help="Number of read-only snapshots to collect.")
    parser.add_argument("--interval-s", type=float, default=0.5, help="Polling interval in seconds.")
    parser.add_argument("--output-dir", required=True, help="Directory for CSV/JSON output.")
    parser.add_argument(
        "--matrix-ag",
        action="store_true",
        help="Run the controller-only A-G matrix using only *CLS, EVEN?, and VENT 0.",
    )
    parser.add_argument(
        "--allow-write-sanitize",
        action="store_true",
        help="Allow exactly one sanitize write: VENT 0 to clear a completed vent latch.",
    )
    parser.add_argument(
        "--ui-ack-experiment",
        action="store_true",
        help="Run the controller-only front-panel observation-window experiment using only allowed read-only queries.",
    )
    parser.add_argument("--ui-ack-hold-samples", type=int, default=5, help="Number of read-only samples during the on-screen observation window.")
    parser.add_argument("--ui-ack-post-samples", type=int, default=5, help="Number of read-only samples in the post-window phase.")
    parser.add_argument("--ui-ack-wait-s", type=float, default=15.0, help="Observation wait window before post-window sampling begins.")
    args = parser.parse_args(argv)
    if args.ui_ack_experiment:
        summary = run_controller_only_ui_ack_experiment(
            port=args.port,
            baudrate=args.baudrate,
            timeout=args.timeout,
            output_dir=args.output_dir,
            interval_s=args.interval_s,
            hold_samples=args.ui_ack_hold_samples,
            post_ack_samples=args.ui_ack_post_samples,
            ack_wait_s=args.ui_ack_wait_s,
        )
    elif args.matrix_ag:
        summary = run_controller_only_matrix(
            port=args.port,
            baudrate=args.baudrate,
            timeout=args.timeout,
            output_dir=args.output_dir,
            action_settle_s=args.interval_s,
            query_interval_s=max(0.05, float(args.interval_s) / 2.0),
        )
    else:
        summary = run_controller_only_diagnostic(
            port=args.port,
            baudrate=args.baudrate,
            timeout=args.timeout,
            samples=args.samples,
            interval_s=args.interval_s,
            output_dir=args.output_dir,
            allow_write_sanitize=args.allow_write_sanitize,
        )
    print(f"saved csv: {summary['csv_path']}")
    print(f"saved json: {summary['json_path']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
