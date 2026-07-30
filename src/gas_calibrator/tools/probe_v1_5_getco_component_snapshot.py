"""Read-only V1.5 component GETCO snapshot probe.

This tool captures old CO2/H2O component coefficient groups before any human
write review. It never writes SENCO, never changes analyzer IDs, and never
controls PACE, valves, gas routes, or water routes.
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


def _parse_groups(value: str) -> List[int]:
    groups: List[int] = []
    for item in str(value or "").replace(";", ",").split(","):
        text = item.strip()
        if not text:
            continue
        group = int(text)
        if group <= 0:
            raise ValueError("GETCO group must be positive")
        if group not in groups:
            groups.append(group)
    if not groups:
        raise ValueError("At least one GETCO group is required")
    return groups


def _split_lines(raw: Any) -> List[str]:
    return _ProtocolGasAnalyzer._split_stream_lines(raw)


def _parse_mode2_line(text: str) -> Optional[Dict[str, Any]]:
    """Parse one MODE2 frame without opening a device.

    ``GasAnalyzer`` intentionally exposes its production parser as an instance
    method.  The snapshot probe only has raw drained lines here, so reuse the
    same tokenizer and strict MODE2 parser directly instead of relying on a
    non-existent compatibility method.
    """

    try:
        for candidate in _ProtocolGasAnalyzer._iter_frame_candidates(text):
            parts = _ProtocolGasAnalyzer._split_frame_parts(candidate)
            parsed = _ProtocolGasAnalyzer._parse_mode2(parts, text)
            if parsed is not None:
                return parsed
    except Exception:
        return None
    return None


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


def _drain_sample(ga: GasAnalyzer, *, drain_s: float) -> List[str]:
    try:
        drain = getattr(ga.ser, "drain_input_nonblock", None)
        if callable(drain):
            lines = drain(drain_s=max(0.0, float(drain_s)), read_timeout_s=0.05)
            out: List[str] = []
            for line in lines:
                out.extend(_split_lines(line))
            return out
    except Exception:
        return []
    return []


def _parse_identity_from_lines(lines: Sequence[str]) -> Dict[str, Any]:
    for raw in lines:
        for line in _split_lines(raw):
            text = str(line or "").strip()
            if not text:
                continue
            parsed = _parse_mode2_line(text)
            if isinstance(parsed, Mapping) and parsed.get("id"):
                return {
                    "ok": True,
                    "id": _device_id(parsed.get("id")),
                    "mode": parsed.get("mode", ""),
                    "raw": text,
                    "source": "mode2_parser",
                }
            tokens = [item.strip() for item in text.split(",")]
            if len(tokens) >= 2 and tokens[0].upper() == "YGAS":
                observed = _device_id(tokens[1])
                if observed:
                    return {
                        "ok": True,
                        "id": observed,
                        "mode": tokens[2] if len(tokens) > 2 else "",
                        "raw": text,
                        "source": "ygas_frame_prefix",
                    }
    return {"ok": False, "id": "", "mode": "", "raw": "", "source": "", "reason": "no_identity_frame"}


def _read_identity_snapshot(
    ga: GasAnalyzer,
    *,
    prefer_stream: bool,
    timeout_s: float,
    drain_s: float,
) -> Dict[str, Any]:
    deadline = time.time() + max(0.2, float(timeout_s))
    last: Dict[str, Any] = {"ok": False, "id": "", "mode": "", "raw": "", "source": "", "reason": "no_identity_frame"}
    while time.time() < deadline:
        reader = getattr(ga, "read_current_mode_snapshot", None)
        if callable(reader):
            try:
                snapshot = reader(
                    prefer_stream=prefer_stream,
                    drain_s=max(0.0, float(drain_s)),
                    read_timeout_s=0.05,
                    allow_passive_fallback=True,
                )
            except Exception:
                snapshot = None
            if isinstance(snapshot, Mapping) and snapshot:
                last = {
                    "ok": bool(snapshot.get("id")),
                    "id": _device_id(snapshot.get("id")),
                    "mode": snapshot.get("mode", ""),
                    "raw": snapshot.get("raw", ""),
                    "source": "read_current_mode_snapshot",
                    "reason": "" if snapshot.get("id") else "missing_frame_id",
                }
                if last["id"]:
                    return last

        parsed = _parse_identity_from_lines(_drain_sample(ga, drain_s=max(0.0, float(drain_s))))
        if parsed.get("id"):
            return parsed
        last = parsed
        time.sleep(0.1)
    return last


def _getco_commands(device_id: str, group: int, *, include_legacy: bool) -> List[Dict[str, str]]:
    targets = ["FFF"]
    if device_id and device_id not in targets:
        targets.append(device_id)
    if "000" not in targets:
        targets.append("000")

    rows: List[Dict[str, str]] = [
        {
            "command_family": "manual_getco_comma_index",
            "target": target,
            "command": f"GETCO,YGAS,{target},{int(group)}",
        }
        for target in targets
    ]
    if include_legacy and device_id:
        rows.append(
            {
                "command_family": "legacy_getco_index_prefix",
                "target": device_id,
                "command": f"GETCO{int(group)},YGAS,{device_id}",
            }
        )
    return rows


def _first_coefficient(lines: Sequence[str]) -> Optional[Dict[str, float]]:
    for line in lines:
        parsed = _ProtocolGasAnalyzer.parse_coefficient_group_line(line)
        if parsed:
            return parsed
    return None


def _coefficient_values(parsed: Optional[Mapping[str, Any]]) -> List[float]:
    if not parsed:
        return []
    indexes: List[int] = []
    for key in parsed:
        text = str(key or "").strip().upper()
        if text.startswith("C") and text[1:].isdigit():
            indexes.append(int(text[1:]))
    if not indexes:
        return []
    return [float(parsed.get(f"C{idx}", 0.0)) for idx in range(min(indexes), max(indexes) + 1)]


def _min_coefficients_for_group(group: int, default: int) -> int:
    # SENCO5/SENCO6 are final affine concentration trims: C0 + C1 only.
    if int(group) in {5, 6}:
        return 2
    return int(default)


def _set_comm_way(
    ga: GasAnalyzer,
    active: bool,
    *,
    response_timeout_s: float,
    command_gap_s: float,
) -> Dict[str, Any]:
    command = f"SETCOMWAY,YGAS,FFF,{1 if active else 0}"
    lines, error = _safe_exchange(
        ga,
        command,
        response_timeout_s=response_timeout_s,
        clear_input=True,
    )
    if command_gap_s > 0:
        time.sleep(command_gap_s)
    return {
        "command": command,
        "active": bool(active),
        "error": error,
        "line_count": len(lines),
        "sample": " | ".join(lines[:4]),
    }


def _probe_one(
    ga: GasAnalyzer,
    analyzer_cfg: Mapping[str, Any],
    *,
    groups: Sequence[int],
    response_timeout_s: float,
    command_gap_s: float,
    attempts_per_group: int,
    min_coefficients_per_group: int,
    include_legacy: bool,
    allow_quiet_setcomway: bool,
    pre_drain_s: float,
) -> tuple[List[Dict[str, Any]], Dict[str, Any]]:
    device_id = _device_id(analyzer_cfg.get("device_id"))
    configured_device_id = _device_id(analyzer_cfg.get("configured_device_id")) or device_id
    identity_rebound = bool(analyzer_cfg.get("runtime_identity_rebound"))
    rows: List[Dict[str, Any]] = []
    comm_rows: List[Dict[str, Any]] = []
    if pre_drain_s > 0:
        _drain_sample(ga, drain_s=pre_drain_s)
    if allow_quiet_setcomway:
        comm_rows.append(
            {
                "phase": "quiet_before_getco",
                **_set_comm_way(
                    ga,
                    False,
                    response_timeout_s=response_timeout_s,
                    command_gap_s=command_gap_s,
                ),
            }
        )
    sequence_idx = 0
    for group in groups:
        found_for_group = False
        for attempt in range(1, max(1, int(attempts_per_group)) + 1):
            for getco in _getco_commands(device_id, group, include_legacy=include_legacy):
                sequence_idx += 1
                lines, error = _safe_exchange(
                    ga,
                    getco["command"],
                    response_timeout_s=response_timeout_s,
                    clear_input=True,
                )
                if command_gap_s > 0:
                    time.sleep(command_gap_s)
                parsed = _first_coefficient(lines)
                parsed_values = _coefficient_values(parsed)
                required_coefficients = _min_coefficients_for_group(group, int(min_coefficients_per_group))
                coefficient_valid = bool(parsed) and len(parsed_values) >= required_coefficients
                rows.append(
                    {
                        "sequence_idx": sequence_idx,
                        "analyzer_name": analyzer_cfg.get("name", ""),
                        "analyzer_device_id": device_id,
                        "configured_device_id": configured_device_id,
                        "runtime_device_id": device_id,
                        "runtime_identity_rebound": identity_rebound,
                        "port": analyzer_cfg.get("port", ""),
                        "getco_group": int(group),
                        "attempt": attempt,
                        "getco_family": getco["command_family"],
                        "getco_target": getco["target"],
                        "getco_command": getco["command"],
                        "getco_error": error,
                        "response_line_count": len(lines),
                        "response_sample": " | ".join(lines[:8]),
                        "coefficient_found": bool(parsed),
                        "coefficient_valid": coefficient_valid,
                        "min_coefficients_per_group": required_coefficients,
                        "parsed_coefficients_json": json.dumps(parsed or {}, ensure_ascii=True),
                        "coefficient_values_json": json.dumps(parsed_values, ensure_ascii=True),
                        "writes_senco": False,
                        "writes_device_id": False,
                        "controls_water_or_gas_routes": False,
                        "controls_pace": False,
                        "sets_comm_way": bool(allow_quiet_setcomway),
                    }
                )
                if coefficient_valid:
                    found_for_group = True
                    break
            if found_for_group:
                break
    if allow_quiet_setcomway and bool(analyzer_cfg.get("active_send", True)):
        comm_rows.append(
            {
                "phase": "restore_active_upload",
                **_set_comm_way(
                    ga,
                    True,
                    response_timeout_s=response_timeout_s,
                    command_gap_s=command_gap_s,
                ),
            }
        )
    return rows, {"comm_way_events": comm_rows}


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
        writer.writerows(_sanitize_rows_for_artifacts(rows))


def _sanitize_cell(value: Any) -> Any:
    if not isinstance(value, str):
        return value
    return "".join(
        ch if ch in {"\t", "\n", "\r"} or ord(ch) >= 32 else "?"
        for ch in value
    )


def _sanitize_rows_for_artifacts(rows: Sequence[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    return [
        {str(key): _sanitize_cell(value) for key, value in row.items()}
        for row in rows
    ]


def _snapshot_payload(rows: Sequence[Mapping[str, Any]]) -> Dict[str, Any]:
    devices: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        if not row.get("coefficient_valid"):
            continue
        device_id = str(row.get("analyzer_device_id") or "").strip()
        group = int(row.get("getco_group") or 0)
        if not device_id or group <= 0:
            continue
        values = json.loads(str(row.get("coefficient_values_json") or "[]"))
        parsed = json.loads(str(row.get("parsed_coefficients_json") or "{}"))
        item = devices.setdefault(
            device_id,
            {
                "analyzer_prefix": row.get("analyzer_name", ""),
                "analyzer_device_id": device_id,
                "configured_device_id": row.get("configured_device_id", ""),
                "runtime_device_id": row.get("runtime_device_id", device_id),
                "runtime_identity_rebound": bool(row.get("runtime_identity_rebound")),
                "port": row.get("port", ""),
                "source": "read_only_getco_component_snapshot",
            },
        )
        item[f"GETCO{group}_before"] = values
        item[f"GETCO{group}_before_parsed"] = parsed
        item[f"GETCO{group}_before_command"] = row.get("getco_command", "")
    return devices


def _write_runtime_bound_config(
    *,
    cfg: Mapping[str, Any],
    cfg_path: Path,
    destination: Path,
    identity_rows: Sequence[Mapping[str, Any]],
    command_gap_s: float,
) -> Path:
    payload = json.loads(json.dumps(cfg, ensure_ascii=False, default=str))
    devices = payload.setdefault("devices", {})
    workflow = payload.setdefault("workflow", {})
    if isinstance(workflow, dict):
        analyzer_init = workflow.setdefault("analyzer_mode2_init", {})
        if isinstance(analyzer_init, dict):
            analyzer_init["command_gap_s"] = float(command_gap_s)
            analyzer_init["reapply_delay_s"] = max(
                float(command_gap_s),
                float(analyzer_init.get("reapply_delay_s") or 0.0),
            )
            analyzer_init["fragile_serial_contract"] = "minimum_1s_command_gap"
    by_port = {
        str(row.get("port") or ""): dict(row)
        for row in identity_rows
        if str(row.get("port") or "").strip() and row.get("identity_verified")
    }

    analyzers = devices.get("gas_analyzers")
    if isinstance(analyzers, list):
        for analyzer in analyzers:
            if not isinstance(analyzer, dict):
                continue
            row = by_port.get(str(analyzer.get("port") or ""))
            if not row:
                continue
            analyzer["configured_device_id"] = row.get("configured_device_id", analyzer.get("device_id", ""))
            analyzer["device_id"] = row.get("analyzer_device_id", analyzer.get("device_id", ""))
            analyzer["runtime_device_id"] = row.get("runtime_device_id", analyzer.get("device_id", ""))
            analyzer["runtime_identity_bound"] = True
            analyzer["identity_binding_source"] = "v1_5_getco_component_snapshot"
            analyzer["identity_binding_frozen"] = True

    single = devices.get("gas_analyzer")
    if isinstance(single, dict):
        row = by_port.get(str(single.get("port") or ""))
        if row:
            single["configured_device_id"] = row.get("configured_device_id", single.get("device_id", ""))
            single["device_id"] = row.get("analyzer_device_id", single.get("device_id", ""))
            single["runtime_device_id"] = row.get("runtime_device_id", single.get("device_id", ""))
            single["runtime_identity_bound"] = True
            single["identity_binding_source"] = "v1_5_getco_component_snapshot"
            single["identity_binding_frozen"] = True

    payload["v1_5_identity_binding"] = {
        "source_config": str(cfg_path),
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "frozen_for_run": True,
        "writes_device_id": False,
        "configured_ids_preserved": True,
        "analyzer_command_gap_s": float(command_gap_s),
        "identity_rows": list(identity_rows),
    }
    path = destination / "runtime_identity_bound_config.json"
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Read-only V1.5 component GETCO snapshot probe.")
    parser.add_argument("--config", required=True, help="V1.5 hardware config JSON.")
    parser.add_argument("--output-dir", required=True, help="Output directory for read-only snapshot evidence.")
    parser.add_argument("--device-id", action="append", default=[], help="MODE2 device ID to probe.")
    parser.add_argument("--groups", default="1,3", help="Comma-separated GETCO groups, e.g. 1,3 for CO2.")
    parser.add_argument("--response-timeout-s", type=float, default=1.5)
    parser.add_argument("--command-gap-s", type=float, default=1.0)
    parser.add_argument("--attempts-per-group", type=int, default=1)
    parser.add_argument("--min-coefficients-per-group", type=int, default=4)
    parser.add_argument("--pre-drain-s", type=float, default=0.25)
    parser.add_argument(
        "--identity-timeout-s",
        type=float,
        default=3.0,
        help="Read MODE/stream identity before GETCO; a mismatch blocks the snapshot for that analyzer.",
    )
    parser.add_argument("--identity-drain-s", type=float, default=0.35)
    parser.add_argument("--include-legacy", action="store_true", help="Also try GETCO1,YGAS,ID style commands.")
    parser.add_argument(
        "--allow-quiet-setcomway",
        action="store_true",
        help="Temporarily stop active upload with SETCOMWAY=0 and restore it after GETCO reads.",
    )
    parser.add_argument(
        "--allow-runtime-identity-rebind",
        action="store_true",
        help=(
            "If the MODE2 frame ID differs from the configured device_id, continue using the observed "
            "runtime ID as analyzer identity. This does not write the device ID or config."
        ),
    )
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    try:
        require_fragile_serial_timing(
            args,
            tool_name="probe_v1_5_getco_component_snapshot",
            fields=("command_gap_s",),
        )
    except ValueError as exc:
        _log(str(exc))
        return 2
    cfg_path = Path(args.config).resolve()
    groups = _parse_groups(args.groups)
    cfg = load_config(cfg_path)
    analyzers = _select_analyzers(cfg, args.device_id)
    if not analyzers:
        _log("No enabled analyzers selected for component GETCO snapshot.")
        return 2

    destination = Path(args.output_dir).resolve()
    destination.mkdir(parents=True, exist_ok=True)
    start_ts = datetime.now().isoformat(timespec="seconds")
    rows: List[Dict[str, Any]] = []
    identity_rows: List[Dict[str, Any]] = []
    comm_rows: List[Dict[str, Any]] = []

    for analyzer_cfg in analyzers:
        configured_device_id = _device_id(analyzer_cfg.get("device_id"))
        device_id = configured_device_id
        runtime_device_id = ""
        identity_rebound = False
        _log(f"Component GETCO read-only snapshot begin: device_id={device_id} port={analyzer_cfg.get('port')}")
        ga = GasAnalyzer(
            str(analyzer_cfg["port"]),
            int(analyzer_cfg.get("baud", analyzer_cfg.get("baudrate", 115200)) or 115200),
            timeout=float(analyzer_cfg.get("timeout", 1.0) or 1.0),
            device_id=device_id,
        )
        error = ""
        identity_before: Dict[str, Any] = {}
        identity_after: Dict[str, Any] = {}
        try:
            ga.open()
            identity_before = _read_identity_snapshot(
                ga,
                prefer_stream=bool(analyzer_cfg.get("active_send", True)),
                timeout_s=float(args.identity_timeout_s),
                drain_s=float(args.identity_drain_s),
            )
            runtime_device_id = _device_id(identity_before.get("id"))
            if runtime_device_id != device_id:
                if bool(args.allow_runtime_identity_rebind) and runtime_device_id:
                    identity_rebound = True
                    device_id = runtime_device_id
                    analyzer_cfg = {
                        **dict(analyzer_cfg),
                        "configured_device_id": configured_device_id,
                        "device_id": runtime_device_id,
                        "runtime_identity_rebound": True,
                    }
                    _log(
                        "Runtime identity rebound for read-only GETCO snapshot: "
                        f"configured={configured_device_id or '<missing>'} observed={runtime_device_id}"
                    )
                else:
                    raise RuntimeError(
                        f"identity_mismatch expected={device_id} observed={identity_before.get('id') or '<missing>'}"
                    )
            if _device_id(identity_before.get("id")) != device_id:
                raise RuntimeError(
                    f"identity_mismatch expected={device_id} observed={identity_before.get('id') or '<missing>'}"
                )
            probe_rows, tail = _probe_one(
                ga,
                analyzer_cfg,
                groups=groups,
                response_timeout_s=float(args.response_timeout_s),
                command_gap_s=float(args.command_gap_s),
                attempts_per_group=int(args.attempts_per_group),
                min_coefficients_per_group=int(args.min_coefficients_per_group),
                include_legacy=bool(args.include_legacy),
                allow_quiet_setcomway=bool(args.allow_quiet_setcomway),
                pre_drain_s=float(args.pre_drain_s),
            )
            identity_after = _read_identity_snapshot(
                ga,
                prefer_stream=bool(analyzer_cfg.get("active_send", True)),
                timeout_s=float(args.identity_timeout_s),
                drain_s=float(args.identity_drain_s),
            )
            if _device_id(identity_after.get("id")) != device_id:
                raise RuntimeError(
                    f"post_getco_identity_mismatch expected={device_id} observed={identity_after.get('id') or '<missing>'}"
                )
            for probe_row in probe_rows:
                probe_row["configured_device_id"] = configured_device_id
                probe_row["runtime_device_id"] = device_id
                probe_row["runtime_identity_rebound"] = identity_rebound
                probe_row["identity_before"] = identity_before.get("id", "")
                probe_row["identity_after"] = identity_after.get("id", "")
                probe_row["identity_verified"] = True
            rows.extend(probe_rows)
            for event in tail.get("comm_way_events", []):
                comm_rows.append(
                    {
                        "analyzer_name": analyzer_cfg.get("name", ""),
                        "analyzer_device_id": device_id,
                        "port": analyzer_cfg.get("port", ""),
                        **dict(event),
                    }
                )
        except Exception as exc:
            error = str(exc)
        finally:
            try:
                ga.close()
            except Exception:
                pass
        found_groups = sorted(
            {
                int(row.get("getco_group") or 0)
                for row in rows
                if row.get("analyzer_device_id") == device_id and row.get("coefficient_found")
                and row.get("coefficient_valid")
            }
        )
        identity_rows.append(
            {
                "analyzer_name": analyzer_cfg.get("name", ""),
                "configured_device_id": configured_device_id,
                "analyzer_device_id": device_id,
                "runtime_device_id": device_id,
                "runtime_identity_rebound": identity_rebound,
                "port": analyzer_cfg.get("port", ""),
                "requested_groups": ",".join(str(group) for group in groups),
                "found_groups": ",".join(str(group) for group in found_groups),
                "all_groups_found": set(found_groups) == set(groups),
                "identity_before": identity_before.get("id", ""),
                "identity_before_raw": identity_before.get("raw", ""),
                "identity_before_source": identity_before.get("source", ""),
                "identity_after": identity_after.get("id", ""),
                "identity_after_raw": identity_after.get("raw", ""),
                "identity_after_source": identity_after.get("source", ""),
                "identity_verified": _device_id(identity_before.get("id")) == device_id
                and (not identity_after or _device_id(identity_after.get("id")) == device_id),
                "error": error,
                "writes_senco": False,
                "writes_device_id": False,
                "controls_water_or_gas_routes": False,
                "controls_pace": False,
                "sets_comm_way": bool(args.allow_quiet_setcomway),
            }
        )

    devices_snapshot = _snapshot_payload(rows)
    all_devices_bound = bool(identity_rows) and all(bool(row.get("all_groups_found")) for row in identity_rows)
    all_identity_verified = bool(identity_rows) and all(bool(row.get("identity_verified")) for row in identity_rows)
    conclusion = [
        {
            "status": "pass" if all_devices_bound and all_identity_verified else "blocked",
            "reason": ""
            if all_devices_bound and all_identity_verified
            else ("identity_not_verified" if not all_identity_verified else "not_all_requested_getco_groups_found"),
            "config": str(cfg_path),
            "analyzer_count": len(analyzers),
            "groups": ",".join(str(group) for group in groups),
            "all_devices_bound": all_devices_bound,
            "all_identity_verified": all_identity_verified,
            "allow_quiet_setcomway": bool(args.allow_quiet_setcomway),
            "allow_runtime_identity_rebind": bool(args.allow_runtime_identity_rebind),
            "runtime_identity_rebound_count": sum(1 for row in identity_rows if row.get("runtime_identity_rebound")),
            "writes_senco": False,
            "writes_device_id": False,
            "controls_water_or_gas_routes": False,
            "controls_pace": False,
        }
    ]

    _write_csv(destination / "getco_component_snapshot_rows.csv", rows)
    _write_csv(destination / "getco_component_snapshot_identity.csv", identity_rows)
    _write_csv(destination / "getco_component_snapshot_comm_way_events.csv", comm_rows)
    _write_csv(destination / "getco_component_snapshot_conclusion.csv", conclusion)
    snapshot_path = destination / "old_component_coefficients_snapshot.json"
    snapshot_path.write_text(json.dumps(devices_snapshot, ensure_ascii=False, indent=2), encoding="utf-8")
    bound_config_path = _write_runtime_bound_config(
        cfg=cfg,
        cfg_path=cfg_path,
        destination=destination,
        identity_rows=identity_rows,
        command_gap_s=float(args.command_gap_s),
    )

    metadata = ValidationMetadata(
        tool_name="probe_v1_5_getco_component_snapshot",
        created_at=start_ts,
        analyzers=[
            f"{row.get('analyzer_name')}:{row.get('analyzer_device_id')}"
            for row in identity_rows
        ],
        input_paths=[str(cfg_path)],
        output_dir=str(destination),
        config_path=str(cfg_path),
        config_summary={
            "groups": groups,
            "include_legacy": bool(args.include_legacy),
            "allow_quiet_setcomway": bool(args.allow_quiet_setcomway),
            "allow_runtime_identity_rebind": bool(args.allow_runtime_identity_rebind),
            "runtime_identity_bound_config": str(bound_config_path),
            "identity_timeout_s": float(args.identity_timeout_s),
            "command_gap_s": float(args.command_gap_s),
            "attempts_per_group": int(args.attempts_per_group),
            "min_coefficients_per_group": int(args.min_coefficients_per_group),
        },
        notes=[
            "Read-only component coefficient snapshot: no SENCO writes, no identity writes, no gas/water/PACE control.",
            "Each selected analyzer must expose a matching MODE/stream device ID before GETCO is accepted; COM port labels are not treated as device identity.",
            "The optional SETCOMWAY quieting path changes only active-upload communication mode and is disabled by default.",
        ],
    )
    write_validation_report(
        destination,
        prefix="v1_5_getco_component_snapshot",
        metadata=metadata,
        tables={
            "snapshot_rows": _sanitize_rows_for_artifacts(rows),
            "identity": _sanitize_rows_for_artifacts(identity_rows),
            "comm_way_events": _sanitize_rows_for_artifacts(comm_rows),
            "conclusion": _sanitize_rows_for_artifacts(conclusion),
        },
    )
    _log(f"Component GETCO read-only snapshot complete: status={conclusion[0]['status']} output={destination}")
    return 0 if all_devices_bound and all_identity_verified else 1


if __name__ == "__main__":
    raise SystemExit(main())
