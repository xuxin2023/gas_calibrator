from __future__ import annotations

import argparse
import csv
import json
import math
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Iterable, List, Mapping, Optional

from ..coefficients.write_readiness import build_write_readiness_decision, summarize_runtime_parity
from ..devices.gas_analyzer import GasAnalyzer

_CAPTURE_COLUMNS = [
    "timestamp",
    "protocol",
    "stream_format",
    "device_id",
    "co2_ppm",
    "h2o_mmol",
    "co2_density",
    "h2o_density",
    "co2_ratio_f",
    "co2_ratio_raw",
    "h2o_ratio_f",
    "h2o_ratio_raw",
    "ref_signal",
    "co2_signal",
    "h2o_signal",
    "co2_sig",
    "h2o_sig",
    "chamber_temp_c",
    "case_temp_c",
    "temp_c",
    "avg_temp_c",
    "pressure_kpa",
    "signal_ratio",
    "status",
    "raw",
]

_STAT_FIELDS = [
    "co2_ppm",
    "h2o_mmol",
    "co2_ratio_f",
    "co2_ratio_raw",
    "h2o_ratio_f",
    "h2o_ratio_raw",
    "ref_signal",
    "co2_signal",
    "h2o_signal",
    "co2_sig",
    "h2o_sig",
    "chamber_temp_c",
    "case_temp_c",
    "temp_c",
    "avg_temp_c",
    "pressure_kpa",
    "signal_ratio",
]


def _safe_float(value: Any) -> float | None:
    if value in (None, "", "null", "None"):
        return None
    try:
        number = float(value)
    except Exception:
        return None
    if not math.isfinite(number):
        return None
    return number


def _normalize_device_id(value: Any) -> str:
    text = str(value or "").strip().upper()
    if not text:
        return ""
    if text.isdigit():
        return f"{int(text):03d}"
    return text


def _write_csv(path: Path, rows: List[Mapping[str, Any]]) -> None:
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=_CAPTURE_COLUMNS)
        writer.writeheader()
        for row in rows:
            payload = {column: row.get(column) for column in _CAPTURE_COLUMNS}
            writer.writerow(payload)


def _compute_stats(values: List[float | None]) -> dict[str, Any] | None:
    usable = [value for value in values if value is not None and math.isfinite(value)]
    if not usable:
        return None
    mean = sum(usable) / len(usable)
    variance = sum((value - mean) ** 2 for value in usable) / len(usable)
    sorted_values = sorted(usable)
    midpoint = len(sorted_values) // 2
    if len(sorted_values) % 2:
        median = sorted_values[midpoint]
    else:
        median = (sorted_values[midpoint - 1] + sorted_values[midpoint]) / 2.0
    return {
        "count": len(usable),
        "mean": mean,
        "median": median,
        "min": min(usable),
        "max": max(usable),
        "span": max(usable) - min(usable),
        "std": math.sqrt(variance),
    }


def _build_capture_row(parsed: Mapping[str, Any], raw_line: str, timestamp: str) -> dict[str, Any]:
    mode = int(_safe_float(parsed.get("mode")) or 1)
    stream_format = "mode2" if mode == 2 else "legacy"
    ref_signal = _safe_float(parsed.get("ref_signal"))
    co2_signal = _safe_float(parsed.get("co2_signal"))
    signal_ratio = None
    if ref_signal is not None and ref_signal != 0.0 and co2_signal is not None:
        signal_ratio = co2_signal / ref_signal

    visible_temps = [
        value
        for value in (
            _safe_float(parsed.get("chamber_temp_c")),
            _safe_float(parsed.get("case_temp_c")),
            _safe_float(parsed.get("temp_c")),
        )
        if value is not None
    ]
    avg_temp_c = sum(visible_temps) / len(visible_temps) if visible_temps else None

    return {
        "timestamp": timestamp,
        "protocol": "YGAS",
        "stream_format": stream_format,
        "device_id": _normalize_device_id(parsed.get("id") or parsed.get("device_id")),
        "co2_ppm": _safe_float(parsed.get("co2_ppm")),
        "h2o_mmol": _safe_float(parsed.get("h2o_mmol")),
        "co2_density": _safe_float(parsed.get("co2_density")),
        "h2o_density": _safe_float(parsed.get("h2o_density")),
        "co2_ratio_f": _safe_float(parsed.get("co2_ratio_f")),
        "co2_ratio_raw": _safe_float(parsed.get("co2_ratio_raw")),
        "h2o_ratio_f": _safe_float(parsed.get("h2o_ratio_f")),
        "h2o_ratio_raw": _safe_float(parsed.get("h2o_ratio_raw")),
        "ref_signal": ref_signal,
        "co2_signal": co2_signal,
        "h2o_signal": _safe_float(parsed.get("h2o_signal")),
        "co2_sig": _safe_float(parsed.get("co2_sig")),
        "h2o_sig": _safe_float(parsed.get("h2o_sig")),
        "chamber_temp_c": _safe_float(parsed.get("chamber_temp_c")),
        "case_temp_c": _safe_float(parsed.get("case_temp_c")),
        "temp_c": _safe_float(parsed.get("temp_c")),
        "avg_temp_c": avg_temp_c,
        "pressure_kpa": _safe_float(parsed.get("pressure_kpa")),
        "signal_ratio": signal_ratio,
        "status": str(parsed.get("status") or "").strip(),
        "raw": raw_line,
    }


def _visible_runtime_inputs(rows: List[Mapping[str, Any]]) -> tuple[dict[str, Any], list[str], list[str]]:
    def _has_value(column: str) -> bool:
        return any(_safe_float(row.get(column)) is not None for row in rows)

    def _has_positive(column: str) -> bool:
        return any((_safe_float(row.get(column)) or 0.0) > 0.0 for row in rows)

    stream_formats_seen = sorted(
        {
            str(row.get("stream_format") or "").strip().lower()
            for row in rows
            if str(row.get("stream_format") or "").strip()
        }
    )
    payload = {
        "stream_formats_seen": stream_formats_seen,
        "legacy_stream_only": bool(rows) and stream_formats_seen == ["legacy"],
        "target_available": _has_value("co2_ppm"),
        "ratio_f_available": _has_positive("co2_ratio_f"),
        "ratio_raw_available": _has_positive("co2_ratio_raw"),
        "signal_available": _has_positive("co2_signal"),
        "ref_signal_available": _has_positive("ref_signal"),
        "signal_over_ref_available": _has_positive("co2_signal") and _has_positive("ref_signal"),
        "legacy_signal_available": _has_positive("co2_sig"),
        "temperature_available": _has_value("temp_c"),
        "chamber_temp_available": _has_value("chamber_temp_c"),
        "case_temp_available": _has_value("case_temp_c"),
        "direct_ratio_available": _has_positive("co2_ratio_f") or _has_positive("co2_ratio_raw"),
    }
    available = [key for key, value in payload.items() if key.endswith("_available") and value]
    missing = [key for key, value in payload.items() if key.endswith("_available") and not value]
    return payload, available, missing


def capture_baseline_ygas_stream(
    *,
    port: str,
    device_id: str,
    output_dir: str | Path,
    capture_seconds: float = 120.0,
    baudrate: int = 115200,
    timeout: float = 0.3,
    prefer_stream: bool = True,
    allow_passive_fallback: bool = True,
    drain_s: float = 0.2,
    read_timeout_s: float = 0.05,
    poll_interval_s: float = 0.01,
    max_frames: int | None = None,
    device_factory: Callable[..., Any] = GasAnalyzer,
) -> dict[str, Any]:
    target_dir = Path(output_dir).resolve()
    target_dir.mkdir(parents=True, exist_ok=True)
    device_id_norm = _normalize_device_id(device_id)
    device = device_factory(
        str(port),
        int(baudrate),
        timeout=float(timeout),
        device_id=device_id_norm,
    )

    rows: List[dict[str, Any]] = []
    deadline = time.monotonic() + max(0.05, float(capture_seconds))

    try:
        device.open()
        while time.monotonic() <= deadline:
            if max_frames is not None and len(rows) >= max_frames:
                break
            raw_line = str(
                device.read_latest_data(
                    prefer_stream=prefer_stream,
                    drain_s=float(drain_s),
                    read_timeout_s=float(read_timeout_s),
                    allow_passive_fallback=allow_passive_fallback,
                )
                or ""
            ).strip()
            if not raw_line:
                if poll_interval_s > 0:
                    time.sleep(float(poll_interval_s))
                continue
            parsed = device.parse_line(raw_line)
            if not isinstance(parsed, Mapping) or not parsed:
                if poll_interval_s > 0:
                    time.sleep(float(poll_interval_s))
                continue
            parsed_device_id = _normalize_device_id(parsed.get("id") or parsed.get("device_id"))
            if parsed_device_id and parsed_device_id != device_id_norm:
                if poll_interval_s > 0:
                    time.sleep(float(poll_interval_s))
                continue
            timestamp = datetime.now().astimezone().isoformat(timespec="milliseconds")
            rows.append(_build_capture_row(parsed, raw_line, timestamp))
            if poll_interval_s > 0:
                time.sleep(float(poll_interval_s))
    finally:
        try:
            device.close()
        except Exception:
            pass

    capture_path = target_dir / f"baseline_stream_{device_id_norm}.csv"
    _write_csv(capture_path, rows)

    visible_runtime_inputs, available, missing = _visible_runtime_inputs(rows)
    legacy_stream_only = bool(visible_runtime_inputs.get("legacy_stream_only"))
    if not rows:
        parity_verdict = "parity_inconclusive_missing_live_stream"
        parity_note = "runtime baseline probe did not capture any valid YGAS frame"
    elif legacy_stream_only:
        parity_verdict = "parity_inconclusive_missing_runtime_inputs"
        parity_note = "legacy runtime stream does not expose ratio or chamber/case inputs needed for parity"
    else:
        parity_verdict = "not_audited"
        parity_note = "runtime probe captured visible inputs but still requires a full runtime parity audit"

    parity_summary = summarize_runtime_parity(
        {
            "parity_verdict": parity_verdict,
            "legacy_stream_only": legacy_stream_only,
        }
    )
    readiness = build_write_readiness_decision(
        fit_quality="unknown",
        delivery_recommendation="unknown",
        coefficient_source="runtime_probe",
        writeback_status="not_requested",
        runtime_parity_verdict=parity_verdict,
        legacy_stream_only=legacy_stream_only,
    )

    summary_path = target_dir / "baseline_stream_summary.json"
    summary = {
        "probe_type": "baseline_ygas_stream",
        "port": str(port),
        "device_id": device_id_norm,
        "capture_seconds": float(capture_seconds),
        "valid_frame_count": len(rows),
        "stream_formats_seen": list(visible_runtime_inputs.get("stream_formats_seen") or []),
        "legacy_stream_only": legacy_stream_only,
        "first_timestamp": rows[0]["timestamp"] if rows else "",
        "last_timestamp": rows[-1]["timestamp"] if rows else "",
        "sample_raw_first": rows[0]["raw"] if rows else "",
        "visible_runtime_inputs": visible_runtime_inputs,
        "visible_runtime_inputs_available": available,
        "visible_runtime_inputs_missing": missing,
        "baseline_capture_path": str(capture_path),
        "runtime_parity_summary_path": str(summary_path),
        "parity_verdict": parity_verdict,
        "runtime_parity_quality": parity_summary["quality"],
        "final_write_ready": bool(readiness["final_write_ready"]),
        "readiness_code": readiness["readiness_code"],
        "readiness_reason": readiness["readiness_reason"],
        "readiness_summary": readiness["readiness_summary"],
        "parity_note": parity_note,
        "conclusion_hint": parity_note,
    }
    for field in _STAT_FIELDS:
        summary[f"{field}_stats"] = _compute_stats([_safe_float(row.get(field)) for row in rows])

    summary_path.write_text(
        json.dumps(summary, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )
    return summary


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Capture a baseline YGAS runtime stream for one live V1 analyzer.")
    parser.add_argument("--port", required=True, help="Analyzer COM port, e.g. COM39.")
    parser.add_argument("--device-id", required=True, help="Live device id, e.g. 079.")
    parser.add_argument("--output-dir", required=True, help="Directory for baseline stream artifacts.")
    parser.add_argument("--capture-seconds", type=float, default=120.0, help="Capture duration in seconds.")
    parser.add_argument("--baudrate", type=int, default=115200, help="Serial baudrate.")
    parser.add_argument("--timeout", type=float, default=0.3, help="Serial timeout in seconds.")
    parser.add_argument("--prefer-stream", dest="prefer_stream", action="store_true", help="Prefer active stream reads.")
    parser.add_argument("--no-prefer-stream", dest="prefer_stream", action="store_false", help="Use passive reads first.")
    parser.set_defaults(prefer_stream=True)
    parser.add_argument(
        "--allow-passive-fallback",
        dest="allow_passive_fallback",
        action="store_true",
        help="Allow fallback to passive READDATA if stream drain returns empty.",
    )
    parser.add_argument(
        "--no-passive-fallback",
        dest="allow_passive_fallback",
        action="store_false",
        help="Disable passive fallback.",
    )
    parser.set_defaults(allow_passive_fallback=True)
    return parser


def main(argv: Iterable[str] | None = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(list(argv) if argv is not None else None)
    summary = capture_baseline_ygas_stream(
        port=args.port,
        device_id=args.device_id,
        output_dir=args.output_dir,
        capture_seconds=float(args.capture_seconds),
        baudrate=int(args.baudrate),
        timeout=float(args.timeout),
        prefer_stream=bool(args.prefer_stream),
        allow_passive_fallback=bool(args.allow_passive_fallback),
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
