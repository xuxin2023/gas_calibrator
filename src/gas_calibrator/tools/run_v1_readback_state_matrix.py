from __future__ import annotations

import argparse
import csv
import json
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Sequence

from ..devices.gas_analyzer import GasAnalyzer
from ..devices.serial_base import serial as pyserial
from .run_v1_safe_readback_session import (
    _ListIoLogger,
    _capture_stream_snapshot,
    _record_step,
    _restore_session,
)

_GROUPS = (1, 3, 7, 8)
_EXPLICIT = GasAnalyzer.READBACK_SOURCE_EXPLICIT_C0
_AMBIGUOUS = GasAnalyzer.READBACK_SOURCE_AMBIGUOUS
_NO_VALID = GasAnalyzer.READBACK_SOURCE_NONE


def _normalize_device_id(value: Any) -> str:
    return GasAnalyzer.normalize_device_id(value)


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")


def _write_csv(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    header: list[str] = []
    for row in rows:
        for key in row.keys():
            if key not in header:
                header.append(str(key))
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=header)
        writer.writeheader()
        for row in rows:
            writer.writerow(dict(row))


def _timing_profiles() -> list[dict[str, Any]]:
    return [
        {
            "name": "default",
            "read_timeout_s": 0.6,
            "read_retries": 0,
            "command_delay_s": 0.05,
            "inter_command_gap_s": 0.05,
            "drain_window_s": 0.12,
            "quiet_window_s": 0.15,
            "active_settle_s": 0.35,
        },
        {
            "name": "relaxed",
            "read_timeout_s": 1.4,
            "read_retries": 1,
            "command_delay_s": 0.12,
            "inter_command_gap_s": 0.2,
            "drain_window_s": 0.25,
            "quiet_window_s": 0.3,
            "active_settle_s": 0.5,
        },
    ]


def _base_session_specs() -> list[dict[str, Any]]:
    return [
        {"name": "baseline", "label": "baseline"},
        {"name": "scan_like_passive", "label": "scan_like_passive"},
        {"name": "historical_prime", "label": "historical_prime"},
    ]


def _local_action_specs() -> list[dict[str, Any]]:
    return [
        {"name": "direct", "prepare_io": False},
        {"name": "drain_before", "prepare_io": False},
        {"name": "quiet_window", "prepare_io": False},
        {"name": "driver_prepare", "prepare_io": True},
    ]


def _target_variants(device_id: str) -> list[dict[str, str]]:
    return [
        {"name": "actual_device_id", "target_id": device_id},
        {"name": "broadcast_fff", "target_id": "FFF"},
    ]


def _command_styles() -> list[str]:
    return ["parameterized", "compact"]


def _apply_scan_like_session(
    ga: GasAnalyzer,
    *,
    steps: list[dict[str, Any]],
    timing: Mapping[str, Any],
) -> None:
    try:
        value = ga.set_comm_way_with_ack(False, require_ack=False)
    except Exception as exc:
        _record_step(steps, phase="prepare", action="set_comm_way_false_noack", ok=False, error=str(exc))
        raise
    _record_step(steps, phase="prepare", action="set_comm_way_false_noack", ok=True, value=value)
    time.sleep(max(0.0, float(timing.get("quiet_window_s") or 0.0)))
    _record_step(
        steps,
        phase="prepare",
        action="scan_like_quiet_window",
        ok=True,
        value={"seconds": float(timing.get("quiet_window_s") or 0.0)},
    )
    try:
        ga.ser.flush_input()
        _record_step(steps, phase="prepare", action="scan_like_flush_input", ok=True)
    except Exception as exc:
        _record_step(steps, phase="prepare", action="scan_like_flush_input", ok=False, error=str(exc))
        raise
    snapshot = _capture_stream_snapshot(ga, attempts=2)
    _record_step(steps, phase="prepare", action="scan_like_passive_probe", ok=True, value=snapshot)


def _apply_historical_prime_session(
    ga: GasAnalyzer,
    *,
    steps: list[dict[str, Any]],
    timing: Mapping[str, Any],
    ftd_hz: int,
    average_filter: int,
) -> None:
    config_steps = (
        ("set_mode_2", lambda require_ack: ga.set_mode_with_ack(2, require_ack=require_ack)),
        ("set_ftd", lambda require_ack: ga.set_active_freq_with_ack(int(ftd_hz), require_ack=require_ack)),
        (
            "set_average_filter",
            lambda require_ack: ga.set_average_filter_with_ack(int(average_filter), require_ack=require_ack),
        ),
        ("set_comm_way_true", lambda require_ack: ga.set_comm_way_with_ack(True, require_ack=require_ack)),
    )
    for action, fn in config_steps:
        ack_value = False
        try:
            ack_value = bool(fn(True))
        except Exception as exc:
            _record_step(steps, phase="prepare", action=f"{action}_ack", ok=False, error=str(exc))
        else:
            _record_step(steps, phase="prepare", action=f"{action}_ack", ok=True, value=ack_value)
        if not ack_value:
            fallback_value = fn(False)
            _record_step(steps, phase="prepare", action=f"{action}_noack_fallback", ok=True, value=fallback_value)
        time.sleep(max(0.0, float(timing.get("inter_command_gap_s") or 0.0)))
        _record_step(
            steps,
            phase="prepare",
            action=f"{action}_gap",
            ok=True,
            value={"seconds": float(timing.get("inter_command_gap_s") or 0.0)},
        )
    time.sleep(max(0.0, float(timing.get("active_settle_s") or 0.0)))
    _record_step(
        steps,
        phase="prepare",
        action="historical_active_settle",
        ok=True,
        value={"seconds": float(timing.get("active_settle_s") or 0.0)},
    )
    snapshot = _capture_stream_snapshot(ga, attempts=2)
    _record_step(steps, phase="prepare", action="historical_active_probe", ok=True, value=snapshot)
    fallback_value = ga.set_comm_way_with_ack(False, require_ack=False)
    _record_step(steps, phase="prepare", action="historical_set_comm_way_false", ok=True, value=fallback_value)
    time.sleep(max(0.0, float(timing.get("quiet_window_s") or 0.0)))
    _record_step(
        steps,
        phase="prepare",
        action="historical_quiet_window",
        ok=True,
        value={"seconds": float(timing.get("quiet_window_s") or 0.0)},
    )
    ga.ser.flush_input()
    _record_step(steps, phase="prepare", action="historical_flush_input", ok=True)


def _apply_base_session(
    ga: GasAnalyzer,
    *,
    base_session: str,
    steps: list[dict[str, Any]],
    timing: Mapping[str, Any],
    ftd_hz: int,
    average_filter: int,
) -> None:
    if base_session == "baseline":
        _record_step(steps, phase="prepare", action="baseline_session", ok=True)
        return
    if base_session == "scan_like_passive":
        _apply_scan_like_session(ga, steps=steps, timing=timing)
        return
    if base_session == "historical_prime":
        _apply_historical_prime_session(
            ga,
            steps=steps,
            timing=timing,
            ftd_hz=ftd_hz,
            average_filter=average_filter,
        )
        return
    raise ValueError(f"unsupported base_session: {base_session}")


def _apply_local_action(
    ga: GasAnalyzer,
    *,
    local_action: str,
    steps: list[dict[str, Any]],
    timing: Mapping[str, Any],
) -> None:
    if local_action == "direct":
        _record_step(steps, phase="command_prepare", action="direct", ok=True)
        return
    if local_action == "drain_before":
        drained = ga.ser.drain_input_nonblock(
            drain_s=float(timing.get("drain_window_s") or 0.0),
            read_timeout_s=0.05,
        )
        _record_step(steps, phase="command_prepare", action="drain_before", ok=True, value={"lines": list(drained or [])})
        return
    if local_action == "quiet_window":
        time.sleep(max(0.0, float(timing.get("quiet_window_s") or 0.0)))
        _record_step(
            steps,
            phase="command_prepare",
            action="quiet_window",
            ok=True,
            value={"seconds": float(timing.get("quiet_window_s") or 0.0)},
        )
        ga.ser.flush_input()
        _record_step(steps, phase="command_prepare", action="quiet_window_flush_input", ok=True)
        return
    if local_action == "driver_prepare":
        _record_step(steps, phase="command_prepare", action="driver_prepare_builtin", ok=True)
        return
    raise ValueError(f"unsupported local_action: {local_action}")


def _classify_capture(capture: Mapping[str, Any]) -> dict[str, Any]:
    source = str(capture.get("source") or _NO_VALID)
    lines = [str(item or "") for item in list(capture.get("raw_transcript_lines") or []) if str(item or "").strip()]
    only_legacy = bool(lines) and all(line.strip().upper().startswith("YGAS,") for line in lines)
    return {
        "source": source,
        "explicit_c0": source == _EXPLICIT,
        "ambiguous_line": source == _AMBIGUOUS,
        "no_valid": source == _NO_VALID,
        "only_legacy_stream": only_legacy,
        "failure_reason": str(capture.get("error") or ""),
        "source_line": str(capture.get("source_line") or ""),
        "source_line_has_explicit_c0": bool(capture.get("source_line_has_explicit_c0", False)),
        "raw_transcript_lines": lines,
        "attempt_transcripts": list(capture.get("attempt_transcripts") or []),
        "coefficients": dict(capture.get("coefficients") or {}),
        "command": str(capture.get("command") or ""),
        "target_id": str(capture.get("target_id") or ""),
    }


def _combo_name(
    *,
    base_session: str,
    local_action: str,
    command_style: str,
    target_variant: str,
    timing_profile: str,
) -> str:
    return "|".join([base_session, local_action, command_style, target_variant, timing_profile])


def _run_combo(
    *,
    port: str,
    device_id: str,
    base_session: str,
    local_action: str,
    command_style: str,
    target_variant: Mapping[str, str],
    timing: Mapping[str, Any],
    repeat_passes: int,
    ftd_hz: int,
    average_filter: int,
    baudrate: int,
    timeout: float,
    serial_factory: Any,
) -> dict[str, Any]:
    combo_id = _combo_name(
        base_session=base_session,
        local_action=local_action,
        command_style=command_style,
        target_variant=str(target_variant["name"]),
        timing_profile=str(timing["name"]),
    )
    io_logger = _ListIoLogger()
    steps: list[dict[str, Any]] = []
    ga = GasAnalyzer(
        str(port),
        int(baudrate),
        timeout=float(timeout),
        device_id=str(device_id),
        io_logger=io_logger,
        serial_factory=serial_factory,
    )
    baseline_snapshot: dict[str, Any] = {}
    restore_summary: dict[str, Any] = {}
    group_results: dict[str, Any] = {}
    fatal_error = ""
    try:
        ga.open()
        _record_step(steps, phase="session", action="open", ok=True)
        baseline_snapshot = _capture_stream_snapshot(ga, attempts=4)
        _record_step(steps, phase="baseline", action="capture_stream_snapshot", ok=True, value=baseline_snapshot)
        restore_mode = int(baseline_snapshot.get("mode") or 1)
        restore_active_send = bool(baseline_snapshot.get("stream_visible"))
        _apply_base_session(
            ga,
            base_session=base_session,
            steps=steps,
            timing=timing,
            ftd_hz=ftd_hz,
            average_filter=average_filter,
        )
        for group in _GROUPS:
            passes: list[dict[str, Any]] = []
            for pass_index in range(1, max(1, int(repeat_passes)) + 1):
                _apply_local_action(ga, local_action=local_action, steps=steps, timing=timing)
                capture = ga.capture_getco_command(
                    int(group),
                    target_id=str(target_variant["target_id"]),
                    command_style=str(command_style),
                    delay_s=float(timing.get("command_delay_s") or 0.0),
                    timeout_s=float(timing.get("read_timeout_s") or 0.0),
                    retries=int(timing.get("read_retries") or 0),
                    prepare_io=bool(local_action == "driver_prepare"),
                )
                classified = _classify_capture(capture)
                passes.append(
                    {
                        "pass_index": int(pass_index),
                        **classified,
                    }
                )
                _record_step(
                    steps,
                    phase="readback",
                    action=f"group_{int(group)}_pass_{int(pass_index)}",
                    ok=bool(classified["explicit_c0"]),
                    value={
                        "source": classified["source"],
                        "source_line": classified["source_line"],
                        "coefficients": classified["coefficients"],
                    },
                    error="" if classified["explicit_c0"] else classified["failure_reason"],
                )
                if pass_index < max(1, int(repeat_passes)):
                    time.sleep(max(0.0, float(timing.get("inter_command_gap_s") or 0.0)))
            explicit_passes = [row for row in passes if bool(row["explicit_c0"])]
            ambiguous_passes = [row for row in passes if bool(row["ambiguous_line"])]
            stable_explicit = False
            stable_coefficients: dict[str, float] = {}
            if len(explicit_passes) >= max(1, int(repeat_passes)):
                first = explicit_passes[0]
                stable_explicit = all(dict(row["coefficients"]) == dict(first["coefficients"]) for row in explicit_passes)
                if stable_explicit:
                    stable_coefficients = dict(first["coefficients"])
            group_results[str(group)] = {
                "group": int(group),
                "stable_explicit": bool(stable_explicit),
                "stable_coefficients": stable_coefficients,
                "passes": passes,
                "explicit_count": len(explicit_passes),
                "ambiguous_count": len(ambiguous_passes),
                "legacy_only_count": sum(1 for row in passes if bool(row["only_legacy_stream"])),
                "no_valid_count": sum(1 for row in passes if bool(row["no_valid"])),
            }
        restore_summary = _restore_session(
            ga,
            steps=steps,
            restore_mode=restore_mode,
            ftd_hz=ftd_hz,
            average_filter=average_filter,
            restore_active_send=restore_active_send,
        )
    except Exception as exc:
        fatal_error = str(exc)
        _record_step(steps, phase="session", action="fatal_error", ok=False, error=fatal_error)
        try:
            restore_summary = _restore_session(
                ga,
                steps=steps,
                restore_mode=1,
                ftd_hz=ftd_hz,
                average_filter=average_filter,
                restore_active_send=True,
            )
        except Exception as restore_exc:
            restore_summary = {
                "restore_mode": 1,
                "restore_active_send": True,
                "errors": [f"restore_failure:{restore_exc}"],
                "post_restore_snapshot": {},
                "post_restore_stream_ok": False,
            }
    finally:
        try:
            ga.close()
            _record_step(steps, phase="session", action="close", ok=True)
        except Exception as exc:
            _record_step(steps, phase="session", action="close", ok=False, error=str(exc))

    explicit_groups = [key for key, value in group_results.items() if bool(value.get("stable_explicit"))]
    return {
        "combo_id": combo_id,
        "base_session": base_session,
        "local_action": local_action,
        "command_style": command_style,
        "target_variant": str(target_variant["name"]),
        "target_id": str(target_variant["target_id"]),
        "timing_profile": str(timing["name"]),
        "baseline_snapshot": baseline_snapshot,
        "steps": steps,
        "io_rows": io_logger.rows,
        "group_results": group_results,
        "explicit_groups": explicit_groups,
        "minimum_success": "1" in explicit_groups and "7" in explicit_groups,
        "all_success": all(bool(group_results.get(str(group), {}).get("stable_explicit")) for group in _GROUPS),
        "restore_summary": restore_summary,
        "fatal_error": fatal_error,
    }


def _search_combos(device_id: str) -> list[dict[str, Any]]:
    combos: list[dict[str, Any]] = []
    default_timing = next(profile for profile in _timing_profiles() if str(profile["name"]) == "default")
    for base_session in _base_session_specs():
        for local_action in _local_action_specs():
            for command_style in _command_styles():
                for target_variant in _target_variants(device_id):
                    combos.append(
                        {
                            "base_session": str(base_session["name"]),
                            "local_action": str(local_action["name"]),
                            "command_style": str(command_style),
                            "target_variant": dict(target_variant),
                            "timing": dict(default_timing),
                        }
                    )
    return combos


def _rank_combo(result: Mapping[str, Any]) -> tuple[int, int, int, int]:
    group_results = dict(result.get("group_results") or {})
    explicit_count = sum(1 for payload in group_results.values() if bool(payload.get("stable_explicit")))
    ambiguous_count = sum(int(payload.get("ambiguous_count") or 0) for payload in group_results.values())
    restore_ok = 1 if bool(dict(result.get("restore_summary") or {}).get("post_restore_stream_ok")) else 0
    minimum_success = 1 if bool(result.get("minimum_success")) else 0
    base_priority = {
        "historical_prime": 2,
        "scan_like_passive": 1,
        "baseline": 0,
    }.get(str(result.get("base_session") or ""), 0)
    local_priority = {
        "driver_prepare": 3,
        "quiet_window": 2,
        "drain_before": 1,
        "direct": 0,
    }.get(str(result.get("local_action") or ""), 0)
    command_priority = 1 if str(result.get("command_style") or "") == "parameterized" else 0
    target_priority = 1 if str(result.get("target_variant") or "") == "actual_device_id" else 0
    return (minimum_success, explicit_count, ambiguous_count, restore_ok, base_priority, local_priority, command_priority, target_priority)


def _select_confirmation_candidates(
    search_results: Sequence[Mapping[str, Any]],
    *,
    limit: int,
) -> list[dict[str, Any]]:
    ranked = sorted((dict(item) for item in search_results), key=_rank_combo, reverse=True)
    selected: list[dict[str, Any]] = []
    seen: set[str] = set()
    for item in ranked:
        combo_id = str(item.get("combo_id") or "")
        if not combo_id or combo_id in seen:
            continue
        selected.append(item)
        seen.add(combo_id)
        if len(selected) >= max(1, int(limit)):
            break
    return selected


def _flatten_combo_rows(
    result: Mapping[str, Any],
    *,
    phase: str,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    restore_summary = dict(result.get("restore_summary") or {})
    for group in _GROUPS:
        payload = dict(dict(result.get("group_results") or {}).get(str(group)) or {})
        passes = list(payload.get("passes") or [])
        for pass_row in passes:
            rows.append(
                {
                    "phase": phase,
                    "combo_id": str(result.get("combo_id") or ""),
                    "base_session": str(result.get("base_session") or ""),
                    "local_action": str(result.get("local_action") or ""),
                    "command_style": str(result.get("command_style") or ""),
                    "target_variant": str(result.get("target_variant") or ""),
                    "target_id": str(result.get("target_id") or ""),
                    "timing_profile": str(result.get("timing_profile") or ""),
                    "group": int(group),
                    "pass_index": int(pass_row.get("pass_index") or 0),
                    "source": str(pass_row.get("source") or ""),
                    "explicit_c0": bool(pass_row.get("explicit_c0")),
                    "ambiguous_line": bool(pass_row.get("ambiguous_line")),
                    "no_valid": bool(pass_row.get("no_valid")),
                    "only_legacy_stream": bool(pass_row.get("only_legacy_stream")),
                    "failure_reason": str(pass_row.get("failure_reason") or ""),
                    "source_line": str(pass_row.get("source_line") or ""),
                    "source_line_has_explicit_c0": bool(pass_row.get("source_line_has_explicit_c0", False)),
                    "coefficients": json.dumps(pass_row.get("coefficients") or {}, ensure_ascii=False, sort_keys=True),
                    "raw_transcript_lines": json.dumps(pass_row.get("raw_transcript_lines") or [], ensure_ascii=False),
                    "attempt_transcripts": json.dumps(pass_row.get("attempt_transcripts") or [], ensure_ascii=False),
                    "stable_explicit_group": bool(payload.get("stable_explicit")),
                    "post_restore_stream_ok": bool(restore_summary.get("post_restore_stream_ok", False)),
                    "fatal_error": str(result.get("fatal_error") or ""),
                }
            )
    return rows


def run_readback_state_matrix(
    *,
    port: str,
    device_id: str,
    output_dir: str | Path,
    baudrate: int = 115200,
    timeout: float = 0.6,
    ftd_hz: int = 10,
    average_filter: int = 49,
    confirmation_limit: int = 6,
    confirmation_passes: int = 2,
    serial_factory: Any = None,
) -> dict[str, Any]:
    if serial_factory is None:
        if pyserial is None:
            raise ModuleNotFoundError("pyserial is required to run the readback state matrix against a real COM port")
        serial_factory = pyserial.Serial

    target_dir = Path(output_dir).resolve()
    target_dir.mkdir(parents=True, exist_ok=True)
    device_id_norm = _normalize_device_id(device_id)
    search_results: list[dict[str, Any]] = []
    confirmation_results: list[dict[str, Any]] = []
    raw_lines: list[str] = []

    for combo in _search_combos(device_id_norm):
        result = _run_combo(
            port=port,
            device_id=device_id_norm,
            base_session=str(combo["base_session"]),
            local_action=str(combo["local_action"]),
            command_style=str(combo["command_style"]),
            target_variant=dict(combo["target_variant"]),
            timing=dict(combo["timing"]),
            repeat_passes=1,
            ftd_hz=int(ftd_hz),
            average_filter=int(average_filter),
            baudrate=int(baudrate),
            timeout=float(timeout),
            serial_factory=serial_factory,
        )
        search_results.append(result)
        for row in list(result.get("steps") or []):
            raw_lines.append(json.dumps({"phase": "search", "kind": "step", "combo_id": result["combo_id"], **dict(row)}, ensure_ascii=False))
        for row in list(result.get("io_rows") or []):
            raw_lines.append(json.dumps({"phase": "search", "kind": "io", "combo_id": result["combo_id"], **dict(row)}, ensure_ascii=False))

    relaxed_timing = next(profile for profile in _timing_profiles() if str(profile["name"]) == "relaxed")
    candidates = _select_confirmation_candidates(search_results, limit=int(confirmation_limit))
    for candidate in candidates:
        result = _run_combo(
            port=port,
            device_id=device_id_norm,
            base_session=str(candidate["base_session"]),
            local_action=str(candidate["local_action"]),
            command_style=str(candidate["command_style"]),
            target_variant={"name": str(candidate["target_variant"]), "target_id": str(candidate["target_id"])},
            timing=dict(relaxed_timing),
            repeat_passes=max(1, int(confirmation_passes)),
            ftd_hz=int(ftd_hz),
            average_filter=int(average_filter),
            baudrate=int(baudrate),
            timeout=float(timeout),
            serial_factory=serial_factory,
        )
        confirmation_results.append(result)
        for row in list(result.get("steps") or []):
            raw_lines.append(json.dumps({"phase": "confirmation", "kind": "step", "combo_id": result["combo_id"], **dict(row)}, ensure_ascii=False))
        for row in list(result.get("io_rows") or []):
            raw_lines.append(json.dumps({"phase": "confirmation", "kind": "io", "combo_id": result["combo_id"], **dict(row)}, ensure_ascii=False))

    search_rows = []
    for item in search_results:
        group_results = dict(item.get("group_results") or {})
        search_rows.append(
            {
                "combo_id": str(item.get("combo_id") or ""),
                "base_session": str(item.get("base_session") or ""),
                "local_action": str(item.get("local_action") or ""),
                "command_style": str(item.get("command_style") or ""),
                "target_variant": str(item.get("target_variant") or ""),
                "target_id": str(item.get("target_id") or ""),
                "timing_profile": str(item.get("timing_profile") or ""),
                "explicit_groups": list(item.get("explicit_groups") or []),
                "explicit_count": sum(1 for payload in group_results.values() if bool(payload.get("stable_explicit"))),
                "ambiguous_count": sum(int(payload.get("ambiguous_count") or 0) for payload in group_results.values()),
                "legacy_only_count": sum(int(payload.get("legacy_only_count") or 0) for payload in group_results.values()),
                "minimum_success": bool(item.get("minimum_success")),
                "all_success": bool(item.get("all_success")),
                "post_restore_stream_ok": bool(dict(item.get("restore_summary") or {}).get("post_restore_stream_ok", False)),
                "fatal_error": str(item.get("fatal_error") or ""),
            }
        )

    confirmed_rows = []
    for item in confirmation_results:
        group_results = dict(item.get("group_results") or {})
        confirmed_rows.append(
            {
                "combo_id": str(item.get("combo_id") or ""),
                "base_session": str(item.get("base_session") or ""),
                "local_action": str(item.get("local_action") or ""),
                "command_style": str(item.get("command_style") or ""),
                "target_variant": str(item.get("target_variant") or ""),
                "target_id": str(item.get("target_id") or ""),
                "timing_profile": str(item.get("timing_profile") or ""),
                "explicit_groups": list(item.get("explicit_groups") or []),
                "explicit_count": sum(1 for payload in group_results.values() if bool(payload.get("stable_explicit"))),
                "minimum_success": bool(item.get("minimum_success")),
                "all_success": bool(item.get("all_success")),
                "post_restore_stream_ok": bool(dict(item.get("restore_summary") or {}).get("post_restore_stream_ok", False)),
                "fatal_error": str(item.get("fatal_error") or ""),
            }
        )

    best_confirmed = sorted(confirmation_results, key=_rank_combo, reverse=True)[0] if confirmation_results else {}
    best_search = sorted(search_results, key=_rank_combo, reverse=True)[0] if search_results else {}
    best_result = best_confirmed if best_confirmed and bool(best_confirmed.get("minimum_success")) else best_search
    stable_groups = {
        str(group): dict(dict(best_result.get("group_results") or {}).get(str(group)) or {})
        for group in _GROUPS
    }
    explicit_c0_found = bool(best_result and best_result.get("minimum_success"))
    summary = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "port": str(port),
        "device_id": device_id_norm,
        "search_rows": search_rows,
        "confirmation_rows": confirmed_rows,
        "best_search_combo": {
            "combo_id": str(best_search.get("combo_id") or ""),
            "explicit_groups": list(best_search.get("explicit_groups") or []),
            "minimum_success": bool(best_search.get("minimum_success", False)),
            "all_success": bool(best_search.get("all_success", False)),
        }
        if best_search
        else {},
        "best_confirmed_combo": {
            "combo_id": str(best_confirmed.get("combo_id") or ""),
            "explicit_groups": list(best_confirmed.get("explicit_groups") or []),
            "minimum_success": bool(best_confirmed.get("minimum_success", False)),
            "all_success": bool(best_confirmed.get("all_success", False)),
        }
        if best_confirmed
        else {},
        "best_result": {
            "combo_id": str(best_result.get("combo_id") or ""),
            "base_session": str(best_result.get("base_session") or ""),
            "local_action": str(best_result.get("local_action") or ""),
            "command_style": str(best_result.get("command_style") or ""),
            "target_variant": str(best_result.get("target_variant") or ""),
            "target_id": str(best_result.get("target_id") or ""),
            "timing_profile": str(best_result.get("timing_profile") or ""),
            "explicit_groups": list(best_result.get("explicit_groups") or []),
            "minimum_success": bool(best_result.get("minimum_success", False)),
            "all_success": bool(best_result.get("all_success", False)),
            "post_restore_stream_ok": bool(dict(best_result.get("restore_summary") or {}).get("post_restore_stream_ok", False)),
        }
        if best_result
        else {},
        "stable_groups": stable_groups,
        "explicit_c0_found": explicit_c0_found,
    }

    matrix_rows = []
    for result in search_results:
        matrix_rows.extend(_flatten_combo_rows(result, phase="search"))
    for result in confirmation_results:
        matrix_rows.extend(_flatten_combo_rows(result, phase="confirmation"))

    (target_dir / "readback_state_matrix_raw.log").write_text("\n".join(raw_lines) + ("\n" if raw_lines else ""), encoding="utf-8")
    _write_json(target_dir / "readback_state_matrix_summary.json", summary)
    _write_csv(target_dir / "readback_state_matrix.csv", matrix_rows)

    return {
        "output_dir": str(target_dir),
        "summary_path": str(target_dir / "readback_state_matrix_summary.json"),
        "matrix_csv_path": str(target_dir / "readback_state_matrix.csv"),
        "raw_log_path": str(target_dir / "readback_state_matrix_raw.log"),
        "summary": summary,
        "search_results": search_results,
        "confirmation_results": confirmation_results,
    }


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run a read-only session-state matrix to recover explicit GETCO C0 lines.")
    parser.add_argument("--port", default="COM39")
    parser.add_argument("--device-id", default="079")
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--baudrate", type=int, default=115200)
    parser.add_argument("--timeout", type=float, default=0.6)
    parser.add_argument("--ftd-hz", type=int, default=10)
    parser.add_argument("--average-filter", type=int, default=49)
    parser.add_argument("--confirmation-limit", type=int, default=6)
    parser.add_argument("--confirmation-passes", type=int, default=2)
    return parser


def main(argv: Iterable[str] | None = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(list(argv) if argv is not None else None)
    result = run_readback_state_matrix(
        port=str(args.port),
        device_id=str(args.device_id),
        output_dir=str(args.output_dir),
        baudrate=int(args.baudrate),
        timeout=float(args.timeout),
        ftd_hz=int(args.ftd_hz),
        average_filter=int(args.average_filter),
        confirmation_limit=int(args.confirmation_limit),
        confirmation_passes=int(args.confirmation_passes),
    )
    print(json.dumps(result["summary"], ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
