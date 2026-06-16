"""Read-only V1.5 GETCO9 protocol probe.

This tool is intentionally separate from the SENCO9 writer. It never writes
SENCO, never changes analyzer IDs, and never controls PACE, valves, gas routes,
or water routes. Its job is to discover which safe GETCO command form returns
the current pressure coefficient snapshot before any controlled SENCO9 write.
"""

from __future__ import annotations

import argparse
import csv
import json
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence

from ..config import load_config
from ..devices import GasAnalyzer
from ..devices.gas_analyzer import GasAnalyzer as _ProtocolGasAnalyzer
from ..validation.reporting import ValidationMetadata, write_validation_report
from .v1_5_entrypoint_guards import (
    add_engineering_diagnostic_guard_args,
    require_engineering_diagnostic_guard,
)
from .v1_5_serial_safety import require_fragile_serial_timing


def _log(message: str) -> None:
    print(message, flush=True)


def _device_id(value: Any) -> str:
    text = str(value or "").strip()
    if text.isdigit():
        return f"{int(text):03d}"
    return text.upper()


def _enabled_analyzers(cfg: Mapping[str, Any]) -> List[Dict[str, Any]]:
    devices = cfg.get("devices", {}) if isinstance(cfg, Mapping) else {}
    source = devices.get("gas_analyzers") if isinstance(devices, Mapping) else None
    if isinstance(source, list) and source:
        analyzers = [dict(item) for item in source if isinstance(item, Mapping) and item.get("enabled", True)]
    else:
        single = devices.get("gas_analyzer") if isinstance(devices, Mapping) else None
        analyzers = [dict(single)] if isinstance(single, Mapping) and single.get("enabled", False) else []
    for idx, item in enumerate(analyzers, start=1):
        item.setdefault("name", f"ga{idx:02d}")
    return analyzers


def _select_analyzers(cfg: Mapping[str, Any], selected_device_ids: Sequence[str]) -> List[Dict[str, Any]]:
    analyzers = _enabled_analyzers(cfg)
    wanted = {_device_id(item) for item in selected_device_ids if str(item or "").strip()}
    if not wanted:
        return analyzers
    return [item for item in analyzers if _device_id(item.get("device_id")) in wanted]


def _sleep_gap(seconds: float) -> None:
    delay = max(0.0, float(seconds or 0.0))
    if delay > 0:
        time.sleep(delay)


def _restore_analyzer_runtime(
    ga: GasAnalyzer,
    analyzer_cfg: Mapping[str, Any],
    *,
    command_gap_s: float = 0.5,
) -> Dict[str, Any]:
    mode = int(analyzer_cfg.get("mode", 2) or 2)
    active_send = bool(analyzer_cfg.get("active_send", True))
    ftd_hz = int(analyzer_cfg.get("ftd_hz", 1) or 1)
    average_filter = int(analyzer_cfg.get("average_filter", 49) or 49)
    restore = {
        "mode": mode,
        "active_send": active_send,
        "ftd_hz": ftd_hz,
        "average_filter": average_filter,
        "status": "attempted",
        "error": "",
    }
    try:
        ga.set_mode_with_ack(mode, require_ack=False)
        _sleep_gap(command_gap_s)
        ga.set_active_freq_with_ack(ftd_hz, require_ack=False)
        _sleep_gap(command_gap_s)
        ga.set_average_filter_with_ack(average_filter, require_ack=False)
        _sleep_gap(command_gap_s)
        ga.set_comm_way_with_ack(active_send, require_ack=False)
        restore["status"] = "restored"
    except Exception as exc:
        restore["status"] = "restore_failed"
        restore["error"] = str(exc)
    return restore


def _read_identity_snapshot(ga: GasAnalyzer, *, prefer_stream: bool, timeout_s: float = 3.0) -> Dict[str, Any]:
    deadline = time.time() + max(0.2, float(timeout_s))
    last: Optional[Mapping[str, Any]] = None
    while time.time() < deadline:
        try:
            snapshot = ga.read_current_mode_snapshot(
                prefer_stream=prefer_stream,
                drain_s=0.35,
                read_timeout_s=0.05,
                allow_passive_fallback=True,
            )
        except Exception:
            snapshot = None
        if isinstance(snapshot, Mapping) and snapshot:
            last = snapshot
            if snapshot.get("id"):
                break
        time.sleep(0.1)
    if not last:
        return {"ok": False, "id": "", "mode": "", "raw": "", "reason": "no_mode_snapshot"}
    return {
        "ok": bool(last.get("id")),
        "id": _device_id(last.get("id")),
        "mode": last.get("mode", ""),
        "raw": last.get("raw", ""),
        "reason": "" if last.get("id") else "missing_frame_id",
    }


def _split_lines(raw: Any) -> List[str]:
    return _ProtocolGasAnalyzer._split_stream_lines(raw)


def _safe_exchange(
    ga: GasAnalyzer,
    command: str,
    *,
    response_timeout_s: float,
    clear_input: bool,
) -> tuple[List[str], str]:
    try:
        lines = ga.ser.exchange_readlines(
            command if command.endswith(("\r", "\n")) else command + "\r\n",
            response_timeout_s=max(0.05, float(response_timeout_s)),
            read_timeout_s=0.05,
            clear_input=clear_input,
        )
        out: List[str] = []
        for line in lines:
            out.extend(_split_lines(line))
        return out, ""
    except Exception as exc:
        return [], str(exc)


def _getco_commands(device_id: str) -> List[Dict[str, str]]:
    targets = ["FFF"]
    if device_id and device_id not in targets:
        targets.append(device_id)
    if "000" not in targets:
        targets.append("000")
    rows: List[Dict[str, str]] = []
    for target in targets:
        rows.append(
            {
                "command_family": "manual_getco_comma_index",
                "target": target,
                "command": f"GETCO,YGAS,{target},9",
            }
        )
        rows.append(
            {
                "command_family": "legacy_getco_index_prefix",
                "target": target,
                "command": f"GETCO9,YGAS,{target}",
            }
        )
    return rows


def _gentle_getco_commands(device_id: str) -> List[Dict[str, str]]:
    targets = ["FFF"]
    if device_id and device_id not in targets:
        targets.append(device_id)
    if "000" not in targets:
        targets.append("000")
    rows: List[Dict[str, str]] = [
        {
            "command_family": "manual_getco_comma_index",
            "target": target,
            "command": f"GETCO,YGAS,{target},9",
        }
        for target in targets
    ]
    if device_id:
        rows.append(
            {
                "command_family": "legacy_getco_index_prefix",
                "target": device_id,
                "command": f"GETCO9,YGAS,{device_id}",
            }
        )
    return rows


def _quiet_commands(device_id: str) -> List[Dict[str, str]]:
    commands = [
        {"quiet_variant": "none_active_stream", "command": ""},
        {"quiet_variant": "setcomway_fff_0", "command": "SETCOMWAY,YGAS,FFF,0"},
    ]
    if device_id and device_id != "FFF":
        commands.append({"quiet_variant": "setcomway_device_0", "command": f"SETCOMWAY,YGAS,{device_id},0"})
    return commands


def _mode_commands(device_id: str, *, include_mode_sweep: bool) -> List[Dict[str, str]]:
    commands = [{"mode_variant": "none", "command": ""}]
    targets = ["FFF"]
    if device_id and device_id not in targets:
        targets.append(device_id)
    for target in targets:
        commands.append({"mode_variant": f"mode2_{target.lower()}", "command": f"MODE,YGAS,{target},2"})
    if include_mode_sweep:
        for mode in (1, 3):
            for target in targets:
                commands.append(
                    {
                        "mode_variant": f"mode{mode}_{target.lower()}",
                        "command": f"MODE,YGAS,{target},{mode}",
                    }
                )
    return commands


def _first_coefficient(lines: Sequence[str]) -> Optional[Dict[str, float]]:
    for line in lines:
        parsed = _ProtocolGasAnalyzer.parse_coefficient_group_line(line)
        if parsed:
            return parsed
    return None


def _probe_one(
    ga: GasAnalyzer,
    analyzer_cfg: Mapping[str, Any],
    *,
    aggressive_variants: bool,
    include_mode_sweep: bool,
    response_timeout_s: float,
    command_gap_s: float,
    quiet_settle_s: float,
    gentle_mode2_command: bool,
    restore_command_gap_s: float,
) -> tuple[List[Dict[str, Any]], Dict[str, Any], Dict[str, Any]]:
    device_id = _device_id(analyzer_cfg.get("device_id"))
    identity_before = _read_identity_snapshot(
        ga,
        prefer_stream=bool(analyzer_cfg.get("active_send", True)),
        timeout_s=4.0,
    )
    rows: List[Dict[str, Any]] = []
    sequence_idx = 0
    if not aggressive_variants:
        quiet = {"quiet_variant": "setcomway_fff_0_once", "command": "SETCOMWAY,YGAS,FFF,0"}
        quiet_lines, quiet_error = _safe_exchange(
            ga,
            quiet["command"],
            response_timeout_s=0.8,
            clear_input=True,
        )
        _sleep_gap(quiet_settle_s)
        try:
            ga.ser.flush_input()
        except Exception:
            pass
        mode = {"mode_variant": "none", "command": ""}
        mode_lines: List[str] = []
        mode_error = ""
        if gentle_mode2_command:
            mode = {"mode_variant": "mode2_fff_once", "command": "MODE,YGAS,FFF,2"}
            mode_lines, mode_error = _safe_exchange(
                ga,
                mode["command"],
                response_timeout_s=0.8,
                clear_input=True,
            )
            _sleep_gap(command_gap_s)
            try:
                ga.ser.flush_input()
            except Exception:
                pass
        for getco in _gentle_getco_commands(device_id):
            sequence_idx += 1
            lines, error = _safe_exchange(
                ga,
                getco["command"],
                response_timeout_s=response_timeout_s,
                clear_input=True,
            )
            parsed = _first_coefficient(lines)
            rows.append(
                {
                    "probe_mode": "gentle",
                    "sequence_idx": sequence_idx,
                    "analyzer_name": analyzer_cfg.get("name", ""),
                    "analyzer_device_id": device_id,
                    "port": analyzer_cfg.get("port", ""),
                    "quiet_variant": quiet["quiet_variant"],
                    "quiet_command": quiet["command"],
                    "quiet_error": quiet_error,
                    "quiet_line_count": len(quiet_lines),
                    "quiet_sample": " | ".join(quiet_lines[:3]),
                    "mode_variant": mode["mode_variant"],
                    "mode_command": mode["command"],
                    "mode_error": mode_error,
                    "mode_line_count": len(mode_lines),
                    "mode_sample": " | ".join(mode_lines[:3]),
                    "getco_family": getco["command_family"],
                    "getco_target": getco["target"],
                    "getco_command": getco["command"],
                    "getco_error": error,
                    "response_line_count": len(lines),
                    "response_sample": " | ".join(lines[:6]),
                    "coefficient_found": bool(parsed),
                    "parsed_coefficients_json": json.dumps(parsed or {}, ensure_ascii=True),
                    "writes_senco": False,
                    "writes_device_id": False,
                    "controls_water_or_gas_routes": False,
                }
            )
            if parsed:
                break
            _sleep_gap(command_gap_s)
        restore = _restore_analyzer_runtime(
            ga,
            analyzer_cfg,
            command_gap_s=restore_command_gap_s,
        )
        time.sleep(0.5)
        identity_after = _read_identity_snapshot(
            ga,
            prefer_stream=bool(analyzer_cfg.get("active_send", True)),
            timeout_s=4.0,
        )
        return rows, identity_before, {"restore": restore, "identity_after": identity_after}

    for quiet in _quiet_commands(device_id):
        quiet_lines: List[str] = []
        quiet_error = ""
        if quiet["command"]:
            quiet_lines, quiet_error = _safe_exchange(
                ga,
                quiet["command"],
                response_timeout_s=0.6,
                clear_input=True,
            )
            time.sleep(0.2)
            try:
                ga.ser.flush_input()
            except Exception:
                pass
        for mode in _mode_commands(device_id, include_mode_sweep=include_mode_sweep):
            mode_lines: List[str] = []
            mode_error = ""
            if mode["command"]:
                mode_lines, mode_error = _safe_exchange(
                    ga,
                    mode["command"],
                    response_timeout_s=0.6,
                    clear_input=True,
                )
                time.sleep(0.1)
                try:
                    ga.ser.flush_input()
                except Exception:
                    pass
            for getco in _getco_commands(device_id):
                sequence_idx += 1
                lines, error = _safe_exchange(
                    ga,
                    getco["command"],
                    response_timeout_s=response_timeout_s,
                    clear_input=True,
                )
                parsed = _first_coefficient(lines)
                rows.append(
                    {
                        "probe_mode": "aggressive",
                        "sequence_idx": sequence_idx,
                        "analyzer_name": analyzer_cfg.get("name", ""),
                        "analyzer_device_id": device_id,
                        "port": analyzer_cfg.get("port", ""),
                        "quiet_variant": quiet["quiet_variant"],
                        "quiet_command": quiet["command"],
                        "quiet_error": quiet_error,
                        "quiet_line_count": len(quiet_lines),
                        "quiet_sample": " | ".join(quiet_lines[:3]),
                        "mode_variant": mode["mode_variant"],
                        "mode_command": mode["command"],
                        "mode_error": mode_error,
                        "mode_line_count": len(mode_lines),
                        "mode_sample": " | ".join(mode_lines[:3]),
                        "getco_family": getco["command_family"],
                        "getco_target": getco["target"],
                        "getco_command": getco["command"],
                        "getco_error": error,
                        "response_line_count": len(lines),
                        "response_sample": " | ".join(lines[:6]),
                        "coefficient_found": bool(parsed),
                        "parsed_coefficients_json": json.dumps(parsed or {}, ensure_ascii=True),
                        "writes_senco": False,
                        "writes_device_id": False,
                        "controls_water_or_gas_routes": False,
                    }
                )
                if parsed:
                    break
                _sleep_gap(command_gap_s)
            if rows and rows[-1].get("coefficient_found"):
                break
            _sleep_gap(command_gap_s)
        if rows and rows[-1].get("coefficient_found"):
            break
        _sleep_gap(command_gap_s)
    restore = _restore_analyzer_runtime(ga, analyzer_cfg, command_gap_s=restore_command_gap_s)
    time.sleep(0.5)
    identity_after = _read_identity_snapshot(
        ga,
        prefer_stream=bool(analyzer_cfg.get("active_send", True)),
        timeout_s=4.0,
    )
    return rows, identity_before, {"restore": restore, "identity_after": identity_after}


def _write_csv(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    header: List[str] = []
    for row in rows:
        for key in row:
            if key not in header:
                header.append(str(key))
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=header)
        writer.writeheader()
        writer.writerows(rows)


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Read-only V1.5 GETCO9 protocol probe.")
    parser.add_argument("--config", required=True, help="V1.5 hardware config JSON.")
    parser.add_argument("--output-dir", required=True, help="Output directory for read-only probe evidence.")
    parser.add_argument("--device-id", action="append", default=[], help="MODE2 device ID to probe.")
    parser.add_argument(
        "--include-mode-sweep",
        action="store_true",
        help="Also try MODE 1 and MODE 3 read-only GETCO variants before restoring configured runtime.",
    )
    parser.add_argument("--response-timeout-s", type=float, default=1.2)
    parser.add_argument(
        "--aggressive-variants",
        action="store_true",
        help="Try the older broad matrix of quiet/mode/GETCO variants. Default is a gentle low-command probe.",
    )
    parser.add_argument(
        "--gentle-mode2-command",
        action="store_true",
        help="In gentle mode, send one MODE=2 command before GETCO attempts.",
    )
    parser.add_argument("--command-gap-s", type=float, default=1.0)
    parser.add_argument("--quiet-settle-s", type=float, default=2.0)
    parser.add_argument("--restore-command-gap-s", type=float, default=1.0)
    add_engineering_diagnostic_guard_args(parser)
    args = parser.parse_args(list(argv) if argv is not None else None)
    require_engineering_diagnostic_guard(args, parser, context="GETCO9 protocol probe")
    return args


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    try:
        require_fragile_serial_timing(
            args,
            tool_name="probe_v1_5_getco9_protocol",
            fields=("command_gap_s", "restore_command_gap_s"),
        )
    except ValueError as exc:
        _log(str(exc))
        return 2
    cfg_path = Path(args.config).resolve()
    cfg = load_config(cfg_path)
    analyzers = _select_analyzers(cfg, args.device_id)
    if not analyzers:
        _log("No enabled analyzers selected for GETCO9 probe.")
        return 2

    destination = Path(args.output_dir).resolve()
    destination.mkdir(parents=True, exist_ok=True)
    rows: List[Dict[str, Any]] = []
    identity_rows: List[Dict[str, Any]] = []
    start_ts = datetime.now().isoformat(timespec="seconds")
    rc = 1

    for analyzer_cfg in analyzers:
        device_id = _device_id(analyzer_cfg.get("device_id"))
        _log(f"GETCO9 read-only probe begin: device_id={device_id} port={analyzer_cfg.get('port')}")
        ga = GasAnalyzer(
            str(analyzer_cfg["port"]),
            int(analyzer_cfg.get("baud", analyzer_cfg.get("baudrate", 115200)) or 115200),
            timeout=float(analyzer_cfg.get("timeout", 1.0) or 1.0),
            device_id=device_id,
        )
        identity_before: Dict[str, Any] = {}
        tail: Dict[str, Any] = {}
        error = ""
        try:
            ga.open()
            probe_rows, identity_before, tail = _probe_one(
                ga,
                analyzer_cfg,
                aggressive_variants=bool(args.aggressive_variants),
                include_mode_sweep=bool(args.include_mode_sweep),
                response_timeout_s=float(args.response_timeout_s),
                command_gap_s=float(args.command_gap_s),
                quiet_settle_s=float(args.quiet_settle_s),
                gentle_mode2_command=bool(args.gentle_mode2_command),
                restore_command_gap_s=float(args.restore_command_gap_s),
            )
            rows.extend(probe_rows)
        except Exception as exc:
            error = str(exc)
        finally:
            try:
                if error:
                    tail = {
                        "restore": _restore_analyzer_runtime(
                            ga,
                            analyzer_cfg,
                            command_gap_s=float(args.restore_command_gap_s),
                        ),
                        "identity_after": {},
                    }
                ga.close()
            except Exception:
                pass
        identity_after = dict(tail.get("identity_after") or {})
        restore = dict(tail.get("restore") or {})
        found_count = sum(
            1
            for row in rows
            if row.get("analyzer_device_id") == device_id and str(row.get("coefficient_found")) == "True"
        )
        identity_rows.append(
            {
                "analyzer_name": analyzer_cfg.get("name", ""),
                "analyzer_device_id": device_id,
                "port": analyzer_cfg.get("port", ""),
                "identity_before_ok": bool(identity_before.get("ok")),
                "identity_before_id": identity_before.get("id", ""),
                "identity_after_ok": bool(identity_after.get("ok")),
                "identity_after_id": identity_after.get("id", ""),
                "restore_status": restore.get("status", ""),
                "restore_error": restore.get("error", ""),
                "coefficient_found_count": found_count,
                "error": error,
                "writes_senco": False,
                "writes_device_id": False,
                "controls_water_or_gas_routes": False,
            }
        )

    found_any = any(row.get("coefficient_found") for row in rows)
    if found_any:
        rc = 0

    _write_csv(destination / "getco9_protocol_probe_rows.csv", rows)
    _write_csv(destination / "getco9_protocol_probe_identity.csv", identity_rows)
    conclusion = [
        {
            "status": "pass" if found_any else "blocked",
            "reason": ""
            if found_any
            else "GETCO9 coefficient line not found by read-only safe command variants",
            "config": str(cfg_path),
            "include_mode_sweep": bool(args.include_mode_sweep),
            "analyzer_count": len(analyzers),
            "coefficient_found_any": found_any,
            "writes_senco": False,
            "writes_device_id": False,
            "controls_water_or_gas_routes": False,
        }
    ]
    _write_csv(destination / "getco9_protocol_probe_conclusion.csv", conclusion)
    metadata = ValidationMetadata(
        tool_name="probe_v1_5_getco9_protocol",
        created_at=start_ts,
        analyzers=[
            f"{row.get('analyzer_name')}:{row.get('analyzer_device_id')}"
            for row in identity_rows
        ],
        input_paths=[str(cfg_path)],
        output_dir=str(destination),
        config_path=str(cfg_path),
        config_summary={
            "aggressive_variants": bool(args.aggressive_variants),
            "include_mode_sweep": bool(args.include_mode_sweep),
            "gentle_mode2_command": bool(args.gentle_mode2_command),
            "command_gap_s": float(args.command_gap_s),
            "quiet_settle_s": float(args.quiet_settle_s),
            "restore_command_gap_s": float(args.restore_command_gap_s),
        },
        notes=[
            "Read-only protocol probe: no SENCO writes, no identity writes, no gas/water/PACE control.",
            "Runtime restore is attempted for MODE, FTD, average filter, and active upload after each analyzer.",
        ],
    )
    write_validation_report(
        destination,
        prefix="v1_5_getco9_protocol_probe",
        metadata=metadata,
        tables={
            "probe_rows": rows,
            "identity": identity_rows,
            "conclusion": conclusion,
        },
    )
    _log(f"GETCO9 read-only probe complete: status={conclusion[0]['status']} output={destination}")
    return rc


if __name__ == "__main__":
    raise SystemExit(main())
