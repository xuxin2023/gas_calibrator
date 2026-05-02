from __future__ import annotations

import argparse
import csv
import json
import time
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Sequence

from ..devices.gas_analyzer import GasAnalyzer
from ..devices.serial_base import serial as pyserial

_LINE_ENDINGS: tuple[tuple[str, bytes], ...] = (
    ("lf", b"\n"),
    ("crlf", b"\r\n"),
    ("cr", b"\r"),
    ("none", b""),
)
_ALLOWED_COMMAND_TEMPLATES: tuple[tuple[str, Callable[[str], str]], ...] = (
    ("read_data", lambda target: f"READDATA,YGAS,{target}"),
    ("getco1_compact", lambda target: f"GETCO1,YGAS,{target}"),
    ("getco1_parameterized", lambda target: f"GETCO,YGAS,{target},1"),
    ("getco7_compact", lambda target: f"GETCO7,YGAS,{target}"),
    ("getco7_parameterized", lambda target: f"GETCO,YGAS,{target},7"),
)


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


def _write_json(path: Path, payload: Any) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")


def _normalize_case_spec(value: str) -> Dict[str, str]:
    text = str(value or "").strip()
    if ":" not in text:
        raise ValueError(f"invalid case spec: {value}")
    port, device_id = text.split(":", 1)
    return {"port": str(port).strip(), "device_id": GasAnalyzer.normalize_device_id(device_id)}


def _decode_bytes(data: bytes) -> str:
    return bytes(data or b"").decode("ascii", errors="ignore")


def _split_lines(decoded_text: str) -> List[str]:
    return GasAnalyzer._split_stream_lines(decoded_text)


def _capture_available_bytes(ser: Any) -> bytes:
    waiting = int(getattr(ser, "in_waiting", 0) or 0)
    if waiting <= 0:
        return b""
    payload = ser.read(waiting)
    if isinstance(payload, bytes):
        return payload
    return str(payload or "").encode("ascii", errors="ignore")


def _drain_for_window(ser: Any, duration_s: float) -> bytes:
    chunks = bytearray()
    deadline = time.monotonic() + max(0.0, float(duration_s))
    while time.monotonic() < deadline:
        chunk = _capture_available_bytes(ser)
        if chunk:
            chunks.extend(chunk)
            continue
        time.sleep(0.005)
    return bytes(chunks)


def _validate_read_only_command(command_text: str) -> None:
    normalized = str(command_text or "").strip().upper()
    if normalized.startswith(("SENCO", "MODE,", "SETCOMWAY", "FTD,", "AVERAGE", "ID,")):
        raise ValueError(f"write command not allowed in command channel matrix: {command_text}")
    if not normalized.startswith(("READDATA", "GETCO")):
        raise ValueError(f"unsupported command in command channel matrix: {command_text}")


def _classify_capture(raw_bytes: bytes) -> Dict[str, Any]:
    decoded_text = _decode_bytes(raw_bytes)
    lines = [str(line or "") for line in _split_lines(decoded_text) if str(line or "").strip()]
    explicit_lines: List[str] = []
    ambiguous_lines: List[str] = []
    ack_lines: List[str] = []
    non_ygas_lines: List[str] = []

    for line in lines:
        inspected = GasAnalyzer.inspect_coefficient_group_line(line)
        source = str(inspected.get("source") or GasAnalyzer.READBACK_SOURCE_NONE)
        if source == GasAnalyzer.READBACK_SOURCE_EXPLICIT_C0:
            explicit_lines.append(line)
        elif source == GasAnalyzer.READBACK_SOURCE_AMBIGUOUS:
            ambiguous_lines.append(line)
        if GasAnalyzer._is_success_ack(line):
            ack_lines.append(line)
        stripped = str(line or "").strip()
        if stripped and not stripped.upper().startswith("YGAS,") and stripped not in explicit_lines and stripped not in ambiguous_lines:
            non_ygas_lines.append(stripped)

    only_legacy_ygas_stream = bool(lines) and all(str(line).strip().upper().startswith("YGAS,") for line in lines)
    if explicit_lines:
        status = "explicit_c0"
    elif ambiguous_lines:
        status = "ambiguous"
    elif ack_lines:
        status = "ack"
    elif non_ygas_lines:
        status = "non_ygas_response"
    elif only_legacy_ygas_stream:
        status = "only_legacy_ygas_stream"
    elif raw_bytes:
        status = "unparsed_payload"
    else:
        status = "no_response"

    return {
        "status": status,
        "decoded_text": decoded_text,
        "lines": lines,
        "explicit_c0": bool(explicit_lines),
        "explicit_c0_lines": explicit_lines,
        "ambiguous": bool(ambiguous_lines),
        "ambiguous_lines": ambiguous_lines,
        "ack": bool(ack_lines),
        "ack_lines": ack_lines,
        "non_ygas_response": bool(non_ygas_lines),
        "non_ygas_lines": non_ygas_lines,
        "only_legacy_ygas_stream": bool(only_legacy_ygas_stream),
        "no_response": not raw_bytes,
    }


def _probe_once(
    *,
    serial_factory: Optional[Callable[..., Any]],
    port: str,
    baudrate: int,
    timeout: float,
    parity: str,
    stopbits: float,
    bytesize: int,
    dtr: Optional[bool],
    rts: Optional[bool],
    command_text: str,
    line_ending_name: str,
    line_ending_bytes: bytes,
    capture_seconds: float,
    pre_drain_s: float,
) -> Dict[str, Any]:
    if serial_factory is None:
        if pyserial is None:
            raise ModuleNotFoundError("pyserial is required for command channel matrix")
        serial_factory = pyserial.Serial

    started_at = datetime.now().isoformat(timespec="milliseconds")
    ser = serial_factory(
        port=port,
        baudrate=int(baudrate),
        timeout=float(timeout),
        parity=parity,
        stopbits=stopbits,
        bytesize=int(bytesize),
    )
    opened = False
    try:
        opened = True
        if dtr is not None and hasattr(ser, "dtr"):
            ser.dtr = bool(dtr)
        if rts is not None and hasattr(ser, "rts"):
            ser.rts = bool(rts)
        current_dtr = getattr(ser, "dtr", None)
        current_rts = getattr(ser, "rts", None)

        pre_drain_bytes = _drain_for_window(ser, float(pre_drain_s))
        command_bytes = command_text.encode("ascii", errors="ignore") + bytes(line_ending_bytes or b"")
        ser.write(command_bytes)
        flush = getattr(ser, "flush", None)
        if callable(flush):
            flush()
        response_bytes = _drain_for_window(ser, float(capture_seconds))
    finally:
        current_dtr = locals().get("current_dtr", None)
        current_rts = locals().get("current_rts", None)
        if opened and hasattr(ser, "close"):
            ser.close()

    return {
        "started_at": started_at,
        "command_text": command_text,
        "command_bytes": command_bytes,
        "command_bytes_hex": command_bytes.hex(),
        "line_ending": line_ending_name,
        "pre_drain_bytes": pre_drain_bytes,
        "pre_drain_bytes_hex": bytes(pre_drain_bytes).hex(),
        "pre_drain_text": _decode_bytes(pre_drain_bytes),
        "response_bytes": response_bytes,
        "response_bytes_hex": bytes(response_bytes).hex(),
        "response_text": _decode_bytes(response_bytes),
        "dtr": current_dtr,
        "rts": current_rts,
    }


def run_command_channel_matrix(
    *,
    output_dir: str | Path,
    cases: Sequence[Mapping[str, Any]],
    baudrate: int = 115200,
    timeout: float = 0.35,
    parity: str = "N",
    stopbits: float = 1,
    bytesize: int = 8,
    capture_seconds: float = 0.35,
    pre_drain_s: float = 0.12,
    serial_factory: Optional[Callable[..., Any]] = None,
) -> Dict[str, Any]:
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    rows: List[Dict[str, Any]] = []
    raw_lines: List[str] = []
    case_specs = [
        {
            "port": str(item.get("port") or "").strip(),
            "device_id": GasAnalyzer.normalize_device_id(item.get("device_id")),
        }
        for item in list(cases or [])
        if str(item.get("port") or "").strip() and str(item.get("device_id") or "").strip()
    ]
    if not case_specs:
        raise ValueError("at least one case is required")

    for case in case_specs:
        port = str(case["port"])
        case_device_id = str(case["device_id"])
        targets = (case_device_id, "FFF", "000")
        for target in targets:
            for command_name, builder in _ALLOWED_COMMAND_TEMPLATES:
                command_text = builder(str(target))
                _validate_read_only_command(command_text)
                for line_ending_name, line_ending_bytes in _LINE_ENDINGS:
                    probe = _probe_once(
                        serial_factory=serial_factory,
                        port=port,
                        baudrate=int(baudrate),
                        timeout=float(timeout),
                        parity=str(parity),
                        stopbits=float(stopbits),
                        bytesize=int(bytesize),
                        dtr=None,
                        rts=None,
                        command_text=command_text,
                        line_ending_name=line_ending_name,
                        line_ending_bytes=line_ending_bytes,
                        capture_seconds=float(capture_seconds),
                        pre_drain_s=float(pre_drain_s),
                    )
                    classified = _classify_capture(bytes(probe["response_bytes"]))
                    row = {
                        "CasePort": port,
                        "CaseDeviceId": case_device_id,
                        "Target": str(target),
                        "CommandName": command_name,
                        "CommandString": command_text,
                        "CommandBytesHex": probe["command_bytes_hex"],
                        "LineEnding": line_ending_name,
                        "Baudrate": int(baudrate),
                        "Bytesize": int(bytesize),
                        "Parity": str(parity),
                        "Stopbits": float(stopbits),
                        "Timeout": float(timeout),
                        "CaptureSeconds": float(capture_seconds),
                        "PreDrainSeconds": float(pre_drain_s),
                        "DTR": probe["dtr"],
                        "RTS": probe["rts"],
                        "PreDrainBytesHex": probe["pre_drain_bytes_hex"],
                        "PreDrainText": probe["pre_drain_text"],
                        "RawReceivedBytesHex": probe["response_bytes_hex"],
                        "DecodedReceivedText": classified["decoded_text"],
                        "ResponseLineCount": len(classified["lines"]),
                        "Status": classified["status"],
                        "ExplicitC0": bool(classified["explicit_c0"]),
                        "Ack": bool(classified["ack"]),
                        "NonYgasResponse": bool(classified["non_ygas_response"]),
                        "OnlyLegacyYgasStream": bool(classified["only_legacy_ygas_stream"]),
                        "NoResponse": bool(classified["no_response"]),
                        "ExplicitC0Line": str(classified["explicit_c0_lines"][0] if classified["explicit_c0_lines"] else ""),
                        "AckLine": str(classified["ack_lines"][0] if classified["ack_lines"] else ""),
                        "NonYgasLine": str(classified["non_ygas_lines"][0] if classified["non_ygas_lines"] else ""),
                        "AmbiguousLine": str(classified["ambiguous_lines"][0] if classified["ambiguous_lines"] else ""),
                    }
                    rows.append(row)
                    raw_lines.extend(
                        [
                            "=== case="
                            + f"{port}/{case_device_id} target={target} command={command_name} line_ending={line_ending_name} ===",
                            f"command_string={command_text}",
                            f"command_bytes_hex={probe['command_bytes_hex']}",
                            f"serial=baud:{int(baudrate)} bytesize:{int(bytesize)} parity:{parity} stopbits:{float(stopbits)} timeout:{float(timeout)} dtr:{probe['dtr']} rts:{probe['rts']}",
                            f"pre_drain_bytes_hex={probe['pre_drain_bytes_hex']}",
                            "pre_drain_text:",
                            str(probe["pre_drain_text"]),
                            f"status={classified['status']}",
                            f"response_bytes_hex={probe['response_bytes_hex']}",
                            "response_text:",
                            str(classified["decoded_text"]),
                            "",
                        ]
                    )

    matrix_csv_path = output_dir / "command_channel_matrix.csv"
    summary_json_path = output_dir / "command_channel_matrix_summary.json"
    raw_log_path = output_dir / "command_channel_matrix_raw.log"
    _write_csv(matrix_csv_path, rows)
    raw_log_path.write_text("\n".join(raw_lines), encoding="utf-8")

    per_case_summary: Dict[str, Any] = {}
    for case in case_specs:
        key = f"{case['port']}/{case['device_id']}"
        case_rows = [row for row in rows if row["CasePort"] == case["port"] and row["CaseDeviceId"] == case["device_id"]]
        status_counts = Counter(str(row["Status"]) for row in case_rows)
        per_case_summary[key] = {
            "row_count": len(case_rows),
            "status_counts": dict(status_counts),
            "explicit_c0_found": any(bool(row["ExplicitC0"]) for row in case_rows),
            "ack_found": any(bool(row["Ack"]) for row in case_rows),
            "non_ygas_response_found": any(bool(row["NonYgasResponse"]) for row in case_rows),
            "only_legacy_stream_count": int(sum(1 for row in case_rows if bool(row["OnlyLegacyYgasStream"]))),
            "no_response_count": int(sum(1 for row in case_rows if bool(row["NoResponse"]))),
            "best_rows": [
                {
                    "Target": row["Target"],
                    "CommandName": row["CommandName"],
                    "LineEnding": row["LineEnding"],
                    "Status": row["Status"],
                    "CommandBytesHex": row["CommandBytesHex"],
                }
                for row in case_rows
                if row["Status"] in {"explicit_c0", "ack", "non_ygas_response"}
            ][:10],
        }

    overall_status_counts = Counter(str(row["Status"]) for row in rows)
    summary_payload = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "cases": case_specs,
        "read_only_only": True,
        "allowed_command_names": [name for name, _builder in _ALLOWED_COMMAND_TEMPLATES],
        "line_endings_tested": [name for name, _ending in _LINE_ENDINGS],
        "targets_tested": ["actual_device_id", "FFF", "000"],
        "status_counts": dict(overall_status_counts),
        "per_case": per_case_summary,
        "artifacts": {
            "matrix_csv": str(matrix_csv_path),
            "summary_json": str(summary_json_path),
            "raw_log": str(raw_log_path),
        },
    }
    _write_json(summary_json_path, summary_payload)
    return {
        "rows": rows,
        "summary": summary_payload,
        "matrix_csv_path": str(matrix_csv_path),
        "summary_json_path": str(summary_json_path),
        "raw_log_path": str(raw_log_path),
    }


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run a read-only command channel framing matrix.")
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--case", action="append", dest="cases", default=[])
    parser.add_argument("--baudrate", type=int, default=115200)
    parser.add_argument("--timeout", type=float, default=0.35)
    parser.add_argument("--parity", default="N")
    parser.add_argument("--stopbits", type=float, default=1)
    parser.add_argument("--bytesize", type=int, default=8)
    parser.add_argument("--capture-seconds", type=float, default=0.35)
    parser.add_argument("--pre-drain-seconds", type=float, default=0.12)
    return parser


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    cases = [_normalize_case_spec(item) for item in list(args.cases or [])]
    if not cases:
        cases = [_normalize_case_spec("COM39:079")]
    result = run_command_channel_matrix(
        output_dir=args.output_dir,
        cases=cases,
        baudrate=int(args.baudrate),
        timeout=float(args.timeout),
        parity=str(args.parity),
        stopbits=float(args.stopbits),
        bytesize=int(args.bytesize),
        capture_seconds=float(args.capture_seconds),
        pre_drain_s=float(args.pre_drain_seconds),
    )
    print(json.dumps(result["summary"], ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
