from __future__ import annotations

import argparse
import csv
import json
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence

from ..devices.gas_analyzer import GasAnalyzer
from ..devices.serial_base import serial as pyserial

_GROUPS = (1, 3, 7, 8)
_EXPECTED_GROUP_LENGTHS = {1: 6, 3: 6, 7: 4, 8: 4}


class _ListIoLogger:
    def __init__(self) -> None:
        self.rows: list[dict[str, Any]] = []

    def log_io(
        self,
        *,
        port: str,
        device: str,
        direction: str,
        command: Any = None,
        response: Any = None,
        error: Any = None,
        duration_ms: Any = None,
        **_kwargs: Any,
    ) -> None:
        self.rows.append(
            {
                "ts": datetime.now().isoformat(timespec="milliseconds"),
                "port": str(port),
                "device": str(device),
                "direction": str(direction),
                "duration_ms": "" if duration_ms in (None, "") else str(duration_ms),
                "command": "" if command is None else str(command),
                "response": "" if response is None else str(response),
                "error": "" if error is None else str(error),
            }
        )


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


def _capture_stream_snapshot(ga: GasAnalyzer, *, attempts: int = 4) -> dict[str, Any]:
    raw_line = ""
    parsed: dict[str, Any] = {}
    for _ in range(max(1, int(attempts))):
        raw_line = str(
            ga.read_latest_data(
                prefer_stream=True,
                drain_s=0.25,
                read_timeout_s=0.05,
                allow_passive_fallback=True,
            )
            or ""
        ).strip()
        parsed = dict(ga.parse_line(raw_line) or {})
        if raw_line:
            break
        time.sleep(0.05)
    return {
        "raw_line": raw_line,
        "parsed": parsed,
        "live_id": _normalize_device_id(parsed.get("id")) if parsed.get("id") else "",
        "mode": parsed.get("mode"),
        "stream_visible": bool(raw_line),
    }


def _flush_input(ga: GasAnalyzer) -> None:
    try:
        ga.ser.flush_input()
    except Exception:
        pass


def _read_group_full(
    ga: GasAnalyzer,
    group: int,
    *,
    target_id: str,
    timeout_s: float,
    retries: int,
) -> dict[str, Any]:
    reader = getattr(ga, "read_coefficient_group_capture", None)
    if callable(reader):
        capture = dict(
            reader(
                int(group),
                timeout_s=float(timeout_s),
                retries=int(retries),
                target_id=target_id,
            )
        )
    else:
        parsed = ga.read_coefficient_group(
            int(group),
            timeout_s=float(timeout_s),
            retries=int(retries),
            target_id=target_id,
            require_explicit_c0=True,
        )
        capture = {
            "source": GasAnalyzer.READBACK_SOURCE_EXPLICIT_C0,
            "coefficients": dict(parsed or {}),
            "source_line": "",
            "source_line_has_explicit_c0": True,
            "raw_transcript_lines": [],
            "attempt_transcripts": [],
            "command": "",
            "target_id": str(target_id),
            "error": "",
        }
    source = str(capture.get("source") or GasAnalyzer.READBACK_SOURCE_NONE)
    if source != GasAnalyzer.READBACK_SOURCE_EXPLICIT_C0:
        raise RuntimeError(str(capture.get("error") or f"READBACK_SOURCE_UNTRUSTED:{source}"))
    parsed = dict(capture.get("coefficients") or {})
    expected_len = int(_EXPECTED_GROUP_LENGTHS[int(group)])
    values: list[float] = []
    missing_keys: list[str] = []
    coefficients: dict[str, float] = {}
    for idx in range(expected_len):
        key = f"C{idx}"
        if key not in parsed:
            missing_keys.append(key)
            continue
        value = float(parsed[key])
        coefficients[key] = value
        values.append(value)
    if missing_keys:
        raise RuntimeError("READBACK_PARSE_MISSING:" + ",".join(missing_keys))
    return {
        "coefficients": coefficients,
        "values": values,
        "expected_len": expected_len,
        "capture": capture,
    }


def _record_step(
    steps: list[dict[str, Any]],
    *,
    phase: str,
    action: str,
    ok: bool,
    value: Any = None,
    error: str = "",
) -> None:
    steps.append(
        {
            "ts": datetime.now().isoformat(timespec="milliseconds"),
            "phase": phase,
            "action": action,
            "ok": bool(ok),
            "value": value,
            "error": str(error or ""),
        }
    )


def _call_step(
    steps: list[dict[str, Any]],
    *,
    phase: str,
    action: str,
    fn: Any,
) -> Any:
    try:
        value = fn()
    except Exception as exc:
        _record_step(steps, phase=phase, action=action, ok=False, error=str(exc))
        raise
    _record_step(steps, phase=phase, action=action, ok=True, value=value)
    return value


def _sleep_step(steps: list[dict[str, Any]], *, phase: str, action: str, seconds: float) -> None:
    time.sleep(max(0.0, float(seconds)))
    _record_step(steps, phase=phase, action=action, ok=True, value={"seconds": float(seconds)})


def _prepare_quiet_only(
    ga: GasAnalyzer,
    *,
    steps: list[dict[str, Any]],
    quiet_window_s: float,
) -> None:
    _call_step(
        steps,
        phase="prepare",
        action="set_comm_way_false_noack",
        fn=lambda: ga.set_comm_way_with_ack(False, require_ack=False),
    )
    _sleep_step(steps, phase="prepare", action="quiet_window", seconds=quiet_window_s)
    _call_step(steps, phase="prepare", action="flush_input", fn=lambda: _flush_input(ga))


def _best_effort_config_step(
    ga: GasAnalyzer,
    *,
    steps: list[dict[str, Any]],
    action: str,
    ack_fn: Any,
    noack_fn: Any,
) -> None:
    ack_value = _call_step(steps, phase="prepare", action=f"{action}_ack", fn=ack_fn)
    if ack_value:
        return
    _call_step(steps, phase="prepare", action=f"{action}_noack_fallback", fn=noack_fn)


def _prepare_historical_prime(
    ga: GasAnalyzer,
    *,
    steps: list[dict[str, Any]],
    ftd_hz: int,
    average_filter: int,
    inter_command_gap_s: float,
    active_settle_s: float,
    quiet_window_s: float,
) -> None:
    _best_effort_config_step(
        ga,
        steps=steps,
        action="set_mode_2",
        ack_fn=lambda: ga.set_mode_with_ack(2, require_ack=True),
        noack_fn=lambda: ga.set_mode_with_ack(2, require_ack=False),
    )
    _sleep_step(steps, phase="prepare", action="post_mode_gap", seconds=inter_command_gap_s)
    _best_effort_config_step(
        ga,
        steps=steps,
        action="set_ftd",
        ack_fn=lambda: ga.set_active_freq_with_ack(int(ftd_hz), require_ack=True),
        noack_fn=lambda: ga.set_active_freq_with_ack(int(ftd_hz), require_ack=False),
    )
    _sleep_step(steps, phase="prepare", action="post_ftd_gap", seconds=inter_command_gap_s)
    _best_effort_config_step(
        ga,
        steps=steps,
        action="set_average_filter",
        ack_fn=lambda: ga.set_average_filter_with_ack(int(average_filter), require_ack=True),
        noack_fn=lambda: ga.set_average_filter_with_ack(int(average_filter), require_ack=False),
    )
    _sleep_step(steps, phase="prepare", action="post_average_gap", seconds=inter_command_gap_s)
    _best_effort_config_step(
        ga,
        steps=steps,
        action="set_comm_way_true",
        ack_fn=lambda: ga.set_comm_way_with_ack(True, require_ack=True),
        noack_fn=lambda: ga.set_comm_way_with_ack(True, require_ack=False),
    )
    _sleep_step(steps, phase="prepare", action="active_settle", seconds=active_settle_s)
    _call_step(
        steps,
        phase="prepare",
        action="active_stream_probe",
        fn=lambda: _capture_stream_snapshot(ga, attempts=2),
    )
    _best_effort_config_step(
        ga,
        steps=steps,
        action="set_comm_way_false",
        ack_fn=lambda: ga.set_comm_way_with_ack(False, require_ack=True),
        noack_fn=lambda: ga.set_comm_way_with_ack(False, require_ack=False),
    )
    _sleep_step(steps, phase="prepare", action="quiet_window", seconds=quiet_window_s)
    _call_step(steps, phase="prepare", action="flush_input", fn=lambda: _flush_input(ga))


def _strategy_specs(
    *,
    ftd_hz: int,
    average_filter: int,
    inter_command_gap_s: float,
    active_settle_s: float,
    quiet_window_s: float,
) -> list[dict[str, Any]]:
    return [
        {
            "name": "quiet_only",
            "prepare": lambda ga, steps: _prepare_quiet_only(
                ga,
                steps=steps,
                quiet_window_s=quiet_window_s,
            ),
        },
        {
            "name": "historical_prime",
            "prepare": lambda ga, steps: _prepare_historical_prime(
                ga,
                steps=steps,
                ftd_hz=ftd_hz,
                average_filter=average_filter,
                inter_command_gap_s=inter_command_gap_s,
                active_settle_s=active_settle_s,
                quiet_window_s=quiet_window_s,
            ),
        },
    ]


def _read_group_passes(
    ga: GasAnalyzer,
    *,
    device_id: str,
    steps: list[dict[str, Any]],
    repeat_passes: int,
    read_timeout_s: float,
    read_retries: int,
) -> dict[str, Any]:
    groups_summary: dict[str, Any] = {}
    target_variants = [
        {"name": "actual_device_id", "target_id": device_id},
        {"name": "broadcast_fff", "target_id": "FFF"},
    ]
    for group in _GROUPS:
        pass_rows: list[dict[str, Any]] = []
        for pass_index in range(1, max(1, int(repeat_passes)) + 1):
            pass_row = {
                "pass_index": int(pass_index),
                "success": False,
                "target_variant": "",
                "values": [],
                "coefficients": {},
                "capture": {},
                "errors": [],
            }
            for variant in target_variants:
                try:
                    payload = _read_group_full(
                        ga,
                        int(group),
                        target_id=str(variant["target_id"]),
                        timeout_s=float(read_timeout_s),
                        retries=int(read_retries),
                    )
                except Exception as exc:
                    error_text = str(exc)
                    pass_row["errors"].append(
                        {
                            "target_variant": str(variant["name"]),
                            "target_id": str(variant["target_id"]),
                            "error": error_text,
                        }
                    )
                    _record_step(
                        steps,
                        phase="readback",
                        action=f"group_{int(group)}_pass_{int(pass_index)}_{variant['name']}",
                        ok=False,
                        error=error_text,
                    )
                    continue
                pass_row["success"] = True
                pass_row["target_variant"] = str(variant["name"])
                pass_row["values"] = list(payload["values"])
                pass_row["coefficients"] = dict(payload["coefficients"])
                pass_row["capture"] = dict(payload.get("capture") or {})
                _record_step(
                    steps,
                    phase="readback",
                    action=f"group_{int(group)}_pass_{int(pass_index)}_{variant['name']}",
                    ok=True,
                    value=payload,
                )
                break
            pass_rows.append(pass_row)

        successful_passes = [row for row in pass_rows if row["success"]]
        stable = False
        stable_variant = ""
        stable_coefficients: dict[str, float] = {}
        stable_capture: dict[str, Any] = {}
        if len(successful_passes) >= max(1, int(repeat_passes)):
            first = successful_passes[0]
            stable = all(
                row["target_variant"] == first["target_variant"] and list(row["values"]) == list(first["values"])
                for row in successful_passes[: max(1, int(repeat_passes))]
            )
            if stable:
                stable_variant = str(first["target_variant"])
                stable_coefficients = dict(first["coefficients"])
                stable_capture = dict(first.get("capture") or {})

        groups_summary[str(group)] = {
            "group": int(group),
            "stable": bool(stable),
            "stable_variant": stable_variant,
            "stable_coefficients": stable_coefficients,
            "stable_capture": stable_capture,
            "passes": pass_rows,
        }
    return groups_summary


def _restore_session(
    ga: GasAnalyzer,
    *,
    steps: list[dict[str, Any]],
    restore_mode: int,
    ftd_hz: int,
    average_filter: int,
    restore_active_send: bool,
) -> dict[str, Any]:
    restore_errors: list[str] = []
    for action, fn in (
        ("restore_mode", lambda: ga.set_mode_with_ack(int(restore_mode), require_ack=False)),
        ("restore_ftd", lambda: ga.set_active_freq_with_ack(int(ftd_hz), require_ack=False)),
        ("restore_average_filter", lambda: ga.set_average_filter_with_ack(int(average_filter), require_ack=False)),
        ("restore_comm_way", lambda: ga.set_comm_way_with_ack(bool(restore_active_send), require_ack=False)),
    ):
        try:
            value = fn()
        except Exception as exc:
            restore_errors.append(f"{action}:{exc}")
            _record_step(steps, phase="restore", action=action, ok=False, error=str(exc))
            continue
        _record_step(steps, phase="restore", action=action, ok=True, value=value)

    _sleep_step(steps, phase="restore", action="post_restore_settle", seconds=0.35)
    snapshot = _capture_stream_snapshot(ga, attempts=4)
    _record_step(steps, phase="restore", action="post_restore_snapshot", ok=True, value=snapshot)
    stream_ok = bool(snapshot.get("stream_visible")) if restore_active_send else True
    return {
        "restore_mode": int(restore_mode),
        "restore_active_send": bool(restore_active_send),
        "errors": restore_errors,
        "post_restore_snapshot": snapshot,
        "post_restore_stream_ok": bool(stream_ok and not restore_errors),
    }


def _run_single_strategy(
    *,
    strategy: Mapping[str, Any],
    port: str,
    device_id: str,
    baudrate: int,
    timeout: float,
    ftd_hz: int,
    average_filter: int,
    read_timeout_s: float,
    read_retries: int,
    repeat_passes: int,
    serial_factory: Any,
) -> dict[str, Any]:
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
    groups_summary: dict[str, Any] = {}
    fatal_error = ""
    try:
        ga.open()
        _record_step(steps, phase="session", action="open", ok=True)
        baseline_snapshot = _capture_stream_snapshot(ga, attempts=4)
        _record_step(steps, phase="baseline", action="capture_stream_snapshot", ok=True, value=baseline_snapshot)
        restore_mode = int(baseline_snapshot.get("mode") or 1)
        restore_active_send = bool(baseline_snapshot.get("stream_visible"))
        prepare = strategy.get("prepare")
        if callable(prepare):
            prepare(ga, steps)
        groups_summary = _read_group_passes(
            ga,
            device_id=device_id,
            steps=steps,
            repeat_passes=repeat_passes,
            read_timeout_s=read_timeout_s,
            read_retries=read_retries,
        )
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

    stable_groups = {
        str(group): dict(groups_summary.get(str(group)) or {})
        for group in _GROUPS
    }
    stable_group_names = [name for name, payload in stable_groups.items() if bool(payload.get("stable"))]
    backup_ready = all(bool(stable_groups[str(group)].get("stable")) for group in _GROUPS)
    minimum_success = bool(stable_groups["1"].get("stable")) and bool(stable_groups["7"].get("stable"))
    return {
        "strategy": str(strategy.get("name") or ""),
        "baseline_snapshot": baseline_snapshot,
        "steps": steps,
        "io_rows": io_logger.rows,
        "groups": stable_groups,
        "stable_group_names": stable_group_names,
        "minimum_success": bool(minimum_success),
        "backup_ready": bool(backup_ready),
        "fatal_error": fatal_error,
        "restore_summary": restore_summary,
    }


def _best_strategy_result(strategy_results: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    if not strategy_results:
        return {}
    ranked = sorted(
        (dict(item) for item in strategy_results),
        key=lambda item: (
            1 if bool(item.get("backup_ready")) else 0,
            len(list(item.get("stable_group_names") or [])),
            1 if bool(item.get("minimum_success")) else 0,
        ),
        reverse=True,
    )
    return ranked[0]


def run_safe_readback_session(
    *,
    port: str,
    device_id: str,
    output_dir: str | Path,
    baudrate: int = 115200,
    timeout: float = 0.6,
    ftd_hz: int = 10,
    average_filter: int = 49,
    inter_command_gap_s: float = 0.15,
    active_settle_s: float = 0.4,
    quiet_window_s: float = 0.2,
    read_timeout_s: float = 1.2,
    read_retries: int = 4,
    repeat_passes: int = 2,
    strategy_names: Sequence[str] | None = None,
    serial_factory: Any = None,
) -> dict[str, Any]:
    if serial_factory is None:
        if pyserial is None:
            raise ModuleNotFoundError("pyserial is required to run the safe readback session against a real COM port")
        serial_factory = pyserial.Serial

    target_dir = Path(output_dir).resolve()
    target_dir.mkdir(parents=True, exist_ok=True)
    device_id_norm = _normalize_device_id(device_id)
    strategy_filter = {str(item).strip().lower() for item in list(strategy_names or []) if str(item).strip()}
    all_specs = _strategy_specs(
        ftd_hz=int(ftd_hz),
        average_filter=int(average_filter),
        inter_command_gap_s=float(inter_command_gap_s),
        active_settle_s=float(active_settle_s),
        quiet_window_s=float(quiet_window_s),
    )
    selected_specs = [
        spec for spec in all_specs if not strategy_filter or str(spec.get("name") or "").strip().lower() in strategy_filter
    ]

    strategy_results: list[dict[str, Any]] = []
    raw_lines: list[str] = []
    group_rows: list[dict[str, Any]] = []
    restore_rows: list[dict[str, Any]] = []

    for spec in selected_specs:
        result = _run_single_strategy(
            strategy=spec,
            port=port,
            device_id=device_id_norm,
            baudrate=int(baudrate),
            timeout=float(timeout),
            ftd_hz=int(ftd_hz),
            average_filter=int(average_filter),
            read_timeout_s=float(read_timeout_s),
            read_retries=int(read_retries),
            repeat_passes=int(repeat_passes),
            serial_factory=serial_factory,
        )
        strategy_results.append(result)
        strategy_name = str(result.get("strategy") or "")
        for row in list(result.get("steps") or []):
            raw_lines.append(json.dumps({"kind": "step", "strategy": strategy_name, **dict(row)}, ensure_ascii=False))
        for row in list(result.get("io_rows") or []):
            raw_lines.append(json.dumps({"kind": "io", "strategy": strategy_name, **dict(row)}, ensure_ascii=False))
        for group in _GROUPS:
            payload = dict(result.get("groups", {}).get(str(group)) or {})
            group_rows.append(
                {
                    "strategy": strategy_name,
                    "group": int(group),
                    "stable": bool(payload.get("stable")),
                    "stable_variant": payload.get("stable_variant", ""),
                    "stable_coefficients": json.dumps(payload.get("stable_coefficients") or {}, ensure_ascii=False),
                    "stable_source": str(dict(payload.get("stable_capture") or {}).get("source") or ""),
                    "stable_source_line": str(dict(payload.get("stable_capture") or {}).get("source_line") or ""),
                    "passes": json.dumps(payload.get("passes") or [], ensure_ascii=False),
                }
            )
        restore_summary = dict(result.get("restore_summary") or {})
        restore_rows.append(
            {
                "strategy": strategy_name,
                "restore_mode": restore_summary.get("restore_mode"),
                "restore_active_send": restore_summary.get("restore_active_send"),
                "post_restore_stream_ok": bool(restore_summary.get("post_restore_stream_ok", False)),
                "errors": ";".join(list(restore_summary.get("errors") or [])),
                "post_restore_line": str(
                    dict(restore_summary.get("post_restore_snapshot") or {}).get("raw_line") or ""
                ),
            }
        )

    best_result = _best_strategy_result(strategy_results)
    backup_payload = {
        str(group): dict(best_result.get("groups", {}).get(str(group), {}).get("stable_coefficients") or {})
        for group in _GROUPS
        if dict(best_result.get("groups", {}).get(str(group), {}).get("stable_coefficients") or {})
    }
    live_backup_path = ""
    if best_result and bool(best_result.get("backup_ready")):
        live_backup_path = str(target_dir / "live_coefficient_backup.json")
        _write_json(Path(live_backup_path), backup_payload)

    summary = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "port": str(port),
        "device_id": device_id_norm,
        "groups_requested": list(_GROUPS),
        "repeat_passes": int(repeat_passes),
        "strategy_results": strategy_results,
        "best_strategy": {
            "name": str(best_result.get("strategy") or ""),
            "stable_group_names": list(best_result.get("stable_group_names") or []),
            "minimum_success": bool(best_result.get("minimum_success", False)),
            "backup_ready": bool(best_result.get("backup_ready", False)),
            "fatal_error": str(best_result.get("fatal_error") or ""),
        }
        if best_result
        else {},
        "live_backup_ready": bool(best_result and best_result.get("backup_ready")),
        "live_backup_path": live_backup_path,
    }

    (target_dir / "safe_readback_raw.log").write_text("\n".join(raw_lines) + ("\n" if raw_lines else ""), encoding="utf-8")
    _write_json(target_dir / "safe_readback_summary.json", summary)
    _write_csv(target_dir / "safe_readback_groups.csv", group_rows)
    _write_json(target_dir / "safe_readback_restore_summary.json", {"rows": restore_rows})

    return {
        "output_dir": str(target_dir),
        "summary_path": str(target_dir / "safe_readback_summary.json"),
        "groups_csv_path": str(target_dir / "safe_readback_groups.csv"),
        "restore_summary_path": str(target_dir / "safe_readback_restore_summary.json"),
        "raw_log_path": str(target_dir / "safe_readback_raw.log"),
        "live_backup_path": live_backup_path,
        "summary": summary,
        "strategy_results": strategy_results,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Run a safe read-only GETCO readback session for one analyzer.")
    parser.add_argument("--port", default="COM39")
    parser.add_argument("--device-id", default="079")
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--baudrate", type=int, default=115200)
    parser.add_argument("--timeout", type=float, default=0.6)
    parser.add_argument("--ftd-hz", type=int, default=10)
    parser.add_argument("--average-filter", type=int, default=49)
    parser.add_argument("--inter-command-gap-s", type=float, default=0.15)
    parser.add_argument("--active-settle-s", type=float, default=0.4)
    parser.add_argument("--quiet-window-s", type=float, default=0.2)
    parser.add_argument("--read-timeout-s", type=float, default=1.2)
    parser.add_argument("--read-retries", type=int, default=4)
    parser.add_argument("--repeat-passes", type=int, default=2)
    parser.add_argument("--strategy", action="append", default=[])
    args = parser.parse_args()

    result = run_safe_readback_session(
        port=str(args.port),
        device_id=str(args.device_id),
        output_dir=str(args.output_dir),
        baudrate=int(args.baudrate),
        timeout=float(args.timeout),
        ftd_hz=int(args.ftd_hz),
        average_filter=int(args.average_filter),
        inter_command_gap_s=float(args.inter_command_gap_s),
        active_settle_s=float(args.active_settle_s),
        quiet_window_s=float(args.quiet_window_s),
        read_timeout_s=float(args.read_timeout_s),
        read_retries=int(args.read_retries),
        repeat_passes=int(args.repeat_passes),
        strategy_names=list(args.strategy or []),
    )
    print(json.dumps(result["summary"], ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
