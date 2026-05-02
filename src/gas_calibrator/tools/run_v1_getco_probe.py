from __future__ import annotations

import argparse
import csv
import json
import time
from collections import Counter
from pathlib import Path
from typing import Any, Iterable, List, Mapping, Sequence

from ..devices.gas_analyzer import GasAnalyzer
from ..devices.serial_base import serial as pyserial

_MATRIX_COLUMNS = [
    "strategy",
    "attempt",
    "group",
    "command_style",
    "command",
    "status",
    "failure_reason",
    "raw_byte_count",
    "response_line_count",
    "parsed_value_count",
    "parsed_coefficients",
]


def _normalize_device_id(value: Any) -> str:
    return GasAnalyzer.normalize_device_id(value)


def _command_specs(device_id: str) -> list[dict[str, Any]]:
    groups = (1, 3, 7, 8)
    rows: list[dict[str, Any]] = []
    for group in groups:
        rows.append(
            {
                "group": int(group),
                "command_style": "compact",
                "command": f"GETCO{int(group)},YGAS,{device_id}\r\n",
            }
        )
        rows.append(
            {
                "group": int(group),
                "command_style": "parameterized",
                "command": f"GETCO,YGAS,{device_id},{int(group)}\r\n",
            }
        )
    return rows


def _strategy_specs(
    *,
    capture_seconds: float,
    drain_window_s: float,
    quiet_window_s: float,
    include_temporary_quiet: bool,
    device_id: str,
) -> list[dict[str, Any]]:
    strategies = [
        {
            "name": "direct",
            "drain_before": False,
            "drain_window_s": 0.0,
            "quiet_window_s": 0.0,
            "capture_seconds": max(0.2, float(capture_seconds)),
            "temporary_quiet": False,
        },
        {
            "name": "drain_before",
            "drain_before": True,
            "drain_window_s": max(0.05, float(drain_window_s)),
            "quiet_window_s": 0.0,
            "capture_seconds": max(0.2, float(capture_seconds)),
            "temporary_quiet": False,
        },
        {
            "name": "drain_then_quiet",
            "drain_before": True,
            "drain_window_s": max(0.05, float(drain_window_s)),
            "quiet_window_s": max(0.0, float(quiet_window_s)),
            "capture_seconds": max(0.2, float(capture_seconds)),
            "temporary_quiet": False,
        },
        {
            "name": "wide_timeout",
            "drain_before": True,
            "drain_window_s": max(0.05, float(drain_window_s)),
            "quiet_window_s": max(0.0, float(quiet_window_s)),
            "capture_seconds": max(2.0, float(capture_seconds)),
            "temporary_quiet": False,
        },
    ]
    if include_temporary_quiet:
        strategies.append(
            {
                "name": "temporary_passive",
                "drain_before": True,
                "drain_window_s": max(0.05, float(drain_window_s)),
                "quiet_window_s": max(0.0, float(quiet_window_s)),
                "capture_seconds": max(2.0, float(capture_seconds)),
                "temporary_quiet": True,
                "temporary_quiet_command": f"SETCOMWAY,YGAS,{device_id},0\r\n",
                "temporary_restore_command": f"SETCOMWAY,YGAS,{device_id},1\r\n",
            }
        )
    return strategies


def _split_text_lines(decoded_text: str) -> list[str]:
    return GasAnalyzer._split_stream_lines(decoded_text)


def classify_probe_capture(raw_bytes: bytes, decoded_text: str) -> dict[str, Any]:
    lines = _split_text_lines(decoded_text)
    parsed_hits: list[dict[str, Any]] = []
    seen: set[str] = set()

    candidates = list(lines)
    if decoded_text and decoded_text not in candidates:
        candidates.append(decoded_text)

    for candidate in candidates:
        parsed = GasAnalyzer.parse_coefficient_group_line(candidate)
        if not parsed:
            continue
        fingerprint = json.dumps(parsed, ensure_ascii=False, sort_keys=True)
        if fingerprint in seen:
            continue
        seen.add(fingerprint)
        parsed_hits.append(
            {
                "candidate_text": str(candidate),
                "parsed_coefficients": parsed,
            }
        )

    if parsed_hits:
        status = "parsed_success"
        failure_reason = ""
    elif raw_bytes:
        status = "raw_received_but_unparsed"
        failure_reason = "active_stream_only" if lines and all(line.upper().startswith("YGAS,") for line in lines) else "unparsed_payload"
    else:
        status = "no_response"
        failure_reason = "no_response"

    return {
        "status": status,
        "failure_reason": failure_reason,
        "response_line_count": len(lines),
        "response_lines": lines,
        "parsed_hits": parsed_hits,
        "parsed_value_count": max((len(hit["parsed_coefficients"]) for hit in parsed_hits), default=0),
        "parsed_coefficients": parsed_hits[0]["parsed_coefficients"] if parsed_hits else {},
    }


def summarize_probe_records(records: Sequence[Mapping[str, Any]], *, repeat: int) -> dict[str, Any]:
    aggregate_rows: list[dict[str, Any]] = []
    stable_groups: dict[str, Any] = {}

    by_pair: dict[tuple[int, str], list[Mapping[str, Any]]] = {}
    for record in records:
        key = (int(record.get("group") or 0), str(record.get("command_style") or ""))
        by_pair.setdefault(key, []).append(record)

    for (group, command_style), items in sorted(by_pair.items()):
        status_counter = Counter(str(item.get("status") or "") for item in items)
        parsed_payloads = [
            json.dumps(dict(item.get("parsed_coefficients") or {}), ensure_ascii=False, sort_keys=True)
            for item in items
            if str(item.get("status") or "") == "parsed_success" and dict(item.get("parsed_coefficients") or {})
        ]
        payload_counter = Counter(parsed_payloads)
        best_payload, best_count = payload_counter.most_common(1)[0] if payload_counter else ("", 0)
        aggregate_rows.append(
            {
                "group": int(group),
                "command_style": command_style,
                "total_attempts": len(items),
                "parsed_success_count": int(status_counter.get("parsed_success", 0)),
                "raw_received_but_unparsed_count": int(status_counter.get("raw_received_but_unparsed", 0)),
                "no_response_count": int(status_counter.get("no_response", 0)),
                "best_payload_match_count": int(best_count),
                "best_payload": json.loads(best_payload) if best_payload else {},
            }
        )

    for group in (1, 3, 7, 8):
        matching = [row for row in aggregate_rows if int(row.get("group") or 0) == int(group)]
        if not matching:
            stable_groups[str(group)] = {"stable": False, "reason": "not_tested", "coefficients": {}}
            continue
        best = max(
            matching,
            key=lambda row: (int(row.get("parsed_success_count") or 0), int(row.get("best_payload_match_count") or 0)),
        )
        stable = int(best.get("parsed_success_count") or 0) >= int(repeat) and bool(best.get("best_payload"))
        stable_groups[str(group)] = {
            "stable": bool(stable),
            "command_style": best.get("command_style"),
            "parsed_success_count": int(best.get("parsed_success_count") or 0),
            "best_payload_match_count": int(best.get("best_payload_match_count") or 0),
            "coefficients": dict(best.get("best_payload") or {}),
        }

    live_backup_ready = all(bool(stable_groups[str(group)]["stable"]) for group in (1, 3, 7, 8))
    return {
        "aggregate_rows": aggregate_rows,
        "stable_groups": stable_groups,
        "live_backup_ready": bool(live_backup_ready),
    }


def _read_available_bytes(ser: Any) -> bytes:
    waiting = int(getattr(ser, "in_waiting", 0) or 0)
    if waiting <= 0:
        return b""
    payload = ser.read(waiting)
    if isinstance(payload, bytes):
        return payload
    return str(payload or "").encode("ascii", errors="ignore")


def _drain_serial(ser: Any, duration_s: float) -> bytes:
    drained = bytearray()
    deadline = time.monotonic() + max(0.0, float(duration_s))
    while time.monotonic() < deadline:
        chunk = _read_available_bytes(ser)
        if chunk:
            drained.extend(chunk)
            continue
        time.sleep(0.005)
    return bytes(drained)


def _write_and_capture(ser: Any, command: str, capture_seconds: float) -> bytes:
    ser.write(command.encode("ascii", errors="ignore"))
    flush = getattr(ser, "flush", None)
    if callable(flush):
        flush()
    captured = bytearray()
    deadline = time.monotonic() + max(0.05, float(capture_seconds))
    while time.monotonic() < deadline:
        chunk = _read_available_bytes(ser)
        if chunk:
            captured.extend(chunk)
            continue
        time.sleep(0.005)
    return bytes(captured)


def run_getco_probe(
    *,
    port: str,
    device_id: str,
    output_dir: str | Path,
    baudrate: int = 115200,
    timeout: float = 0.05,
    repeat: int = 3,
    capture_seconds: float = 1.2,
    drain_window_s: float = 0.25,
    quiet_window_s: float = 0.15,
    include_temporary_quiet: bool = False,
    serial_factory: Any = None,
) -> dict[str, Any]:
    if serial_factory is None:
        if pyserial is None:
            raise ModuleNotFoundError("pyserial is required to run the GETCO probe against a real COM port")
        serial_factory = pyserial.Serial

    device_id_norm = _normalize_device_id(device_id)
    target_dir = Path(output_dir).resolve()
    target_dir.mkdir(parents=True, exist_ok=True)

    records: list[dict[str, Any]] = []
    raw_log_lines: list[str] = []
    command_specs = _command_specs(device_id_norm)
    strategy_specs = _strategy_specs(
        capture_seconds=float(capture_seconds),
        drain_window_s=float(drain_window_s),
        quiet_window_s=float(quiet_window_s),
        include_temporary_quiet=bool(include_temporary_quiet),
        device_id=device_id_norm,
    )

    for strategy in strategy_specs:
        for attempt in range(1, max(1, int(repeat)) + 1):
            for command_spec in command_specs:
                with serial_factory(port=str(port), baudrate=int(baudrate), timeout=float(timeout)) as ser:
                    drained_bytes = b""
                    quiet_bytes = b""
                    restore_bytes = b""

                    if strategy.get("drain_before"):
                        drained_bytes = _drain_serial(ser, float(strategy.get("drain_window_s") or 0.0))
                    if float(strategy.get("quiet_window_s") or 0.0) > 0:
                        time.sleep(float(strategy.get("quiet_window_s") or 0.0))
                    if strategy.get("temporary_quiet"):
                        quiet_bytes = _write_and_capture(
                            ser,
                            str(strategy.get("temporary_quiet_command") or ""),
                            capture_seconds=0.8,
                        )

                    response_bytes = _write_and_capture(
                        ser,
                        str(command_spec["command"]),
                        capture_seconds=float(strategy.get("capture_seconds") or capture_seconds),
                    )

                    if strategy.get("temporary_quiet"):
                        restore_bytes = _write_and_capture(
                            ser,
                            str(strategy.get("temporary_restore_command") or ""),
                            capture_seconds=0.8,
                        )

                response_text = response_bytes.decode("ascii", errors="ignore")
                capture_info = classify_probe_capture(response_bytes, response_text)
                record = {
                    "strategy": str(strategy["name"]),
                    "attempt": int(attempt),
                    "group": int(command_spec["group"]),
                    "command_style": str(command_spec["command_style"]),
                    "command": str(command_spec["command"]).strip(),
                    "status": capture_info["status"],
                    "failure_reason": capture_info["failure_reason"],
                    "raw_byte_count": len(response_bytes),
                    "response_line_count": capture_info["response_line_count"],
                    "parsed_value_count": capture_info["parsed_value_count"],
                    "parsed_coefficients": dict(capture_info["parsed_coefficients"] or {}),
                    "response_lines": list(capture_info["response_lines"] or []),
                    "drained_bytes_hex": drained_bytes.hex(),
                    "quiet_bytes_hex": quiet_bytes.hex(),
                    "response_bytes_hex": response_bytes.hex(),
                    "restore_bytes_hex": restore_bytes.hex(),
                    "response_text": response_text,
                }
                records.append(record)
                raw_log_lines.extend(
                    [
                        f"=== strategy={record['strategy']} attempt={record['attempt']} command={record['command']} ===",
                        f"status={record['status']} failure_reason={record['failure_reason']}",
                        f"drained_bytes_hex={record['drained_bytes_hex']}",
                        f"quiet_bytes_hex={record['quiet_bytes_hex']}",
                        f"response_bytes_hex={record['response_bytes_hex']}",
                        "response_text:",
                        record["response_text"],
                        f"restore_bytes_hex={record['restore_bytes_hex']}",
                        "",
                    ]
                )

    summary = summarize_probe_records(records, repeat=max(1, int(repeat)))
    payload = {
        "port": str(port),
        "device_id": device_id_norm,
        "repeat": max(1, int(repeat)),
        "records": records,
        "aggregate_rows": summary["aggregate_rows"],
        "stable_groups": summary["stable_groups"],
        "live_backup_ready": bool(summary["live_backup_ready"]),
    }

    (target_dir / "getco_probe_raw.log").write_text("\n".join(raw_log_lines), encoding="utf-8")
    (target_dir / "getco_probe_summary.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )
    with (target_dir / "getco_probe_matrix.csv").open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=_MATRIX_COLUMNS)
        writer.writeheader()
        for record in records:
            writer.writerow(
                {
                    "strategy": record["strategy"],
                    "attempt": record["attempt"],
                    "group": record["group"],
                    "command_style": record["command_style"],
                    "command": record["command"],
                    "status": record["status"],
                    "failure_reason": record["failure_reason"],
                    "raw_byte_count": record["raw_byte_count"],
                    "response_line_count": record["response_line_count"],
                    "parsed_value_count": record["parsed_value_count"],
                    "parsed_coefficients": json.dumps(record["parsed_coefficients"], ensure_ascii=False, sort_keys=True),
                }
            )

    if bool(summary["live_backup_ready"]):
        live_backup = {
            "device_id": device_id_norm,
            "port": str(port),
            "captured_at": time.strftime("%Y-%m-%dT%H:%M:%S"),
            "groups": {
                group: data["coefficients"]
                for group, data in summary["stable_groups"].items()
                if bool(data.get("stable"))
            },
        }
        (target_dir / "live_coefficient_backup.json").write_text(
            json.dumps(live_backup, ensure_ascii=False, indent=2, default=str),
            encoding="utf-8",
        )
        payload["live_backup_path"] = str(target_dir / "live_coefficient_backup.json")
    return payload


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run a read-only GETCO probe matrix for one V1 live analyzer.")
    parser.add_argument("--port", required=True, help="Analyzer COM port, e.g. COM39.")
    parser.add_argument("--device-id", required=True, help="Live device id, e.g. 079.")
    parser.add_argument("--output-dir", required=True, help="Directory for GETCO probe artifacts.")
    parser.add_argument("--repeat", type=int, default=3, help="How many attempts to run per strategy and command.")
    parser.add_argument("--capture-seconds", type=float, default=1.2, help="Response capture window per command.")
    parser.add_argument("--baudrate", type=int, default=115200, help="Serial baudrate.")
    parser.add_argument("--timeout", type=float, default=0.05, help="Serial read timeout.")
    parser.add_argument("--drain-window-s", type=float, default=0.25, help="Local drain window before a probe command.")
    parser.add_argument("--quiet-window-s", type=float, default=0.15, help="Idle wait after drain_before strategies.")
    parser.add_argument(
        "--include-temporary-quiet",
        action="store_true",
        help="Also test a temporary SETCOMWAY quiet strategy and restore immediately after each probe.",
    )
    return parser


def main(argv: Iterable[str] | None = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(list(argv) if argv is not None else None)
    result = run_getco_probe(
        port=args.port,
        device_id=args.device_id,
        output_dir=args.output_dir,
        baudrate=int(args.baudrate),
        timeout=float(args.timeout),
        repeat=int(args.repeat),
        capture_seconds=float(args.capture_seconds),
        drain_window_s=float(args.drain_window_s),
        quiet_window_s=float(args.quiet_window_s),
        include_temporary_quiet=bool(args.include_temporary_quiet),
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
