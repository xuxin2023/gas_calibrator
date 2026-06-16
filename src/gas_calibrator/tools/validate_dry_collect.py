"""Ambient-air dry collect validation.

This sidecar tool only verifies sampling, frame usability, summary export, and
fit-input preview under no-humidity-generator / no-gas-cylinder conditions.
It does not change the production V1 workflow entry or timing defaults.
"""

from __future__ import annotations

import argparse
import copy
import csv
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, Optional

from ..config import load_config
from ..logging_utils import RunLogger
from ..validation.common import analyze_sample_rows, build_validation_point
from ..validation.reporting import ValidationMetadata, write_validation_report
from ..workflow.runner import CalibrationRunner
from .run_headless import _build_devices, _close_devices


def _log(message: str) -> None:
    print(message, flush=True)


def _prepare_runtime_cfg(cfg: Dict[str, Any], *, include_pressure: bool, include_temperature: bool) -> Dict[str, Any]:
    runtime_cfg = copy.deepcopy(cfg)
    devices_cfg = runtime_cfg.setdefault("devices", {})
    for key in ("humidity_generator", "dewpoint_meter", "relay", "relay_8"):
        if isinstance(devices_cfg.get(key), dict):
            devices_cfg[key]["enabled"] = False
    if not include_pressure:
        for key in ("pressure_controller", "pressure_gauge"):
            if isinstance(devices_cfg.get(key), dict):
                devices_cfg[key]["enabled"] = False
    if not include_temperature:
        for key in ("temperature_chamber", "thermometer"):
            if isinstance(devices_cfg.get(key), dict):
                devices_cfg[key]["enabled"] = False
    else:
        for key in ("temperature_chamber", "thermometer"):
            device_cfg = devices_cfg.get(key)
            if isinstance(device_cfg, dict) and device_cfg.get("port"):
                device_cfg["enabled"] = True
    runtime_cfg.setdefault("workflow", {})["collect_only"] = True
    return runtime_cfg


def _apply_read_first_only_analyzer_guard(runtime_cfg: Dict[str, Any]) -> None:
    init_cfg = runtime_cfg.setdefault("workflow", {}).setdefault("analyzer_mode2_init", {})
    init_cfg["read_first_before_config"] = True
    init_cfg["sniff_stream_before_config"] = True
    init_cfg["skip_config_when_read_first_ready"] = True
    init_cfg["write_config_on_read_first_fail"] = False


def _parse_temperature_io_value(device: str, response: str) -> tuple[str, Optional[float]]:
    text = str(response or "").strip()
    device_key = str(device or "").strip().lower()
    if not text:
        return "", None
    if device_key == "temperature_chamber":
        if text.startswith("temp_c="):
            try:
                return "temperature_chamber_temp_c", float(text.split("=", 1)[1])
            except ValueError:
                return "temperature_chamber_temp_c", None
        if text.startswith("rh_pct="):
            try:
                return "temperature_chamber_rh_pct", float(text.split("=", 1)[1])
            except ValueError:
                return "temperature_chamber_rh_pct", None
    if device_key == "thermometer":
        value_text = text.upper().removesuffix("C").strip()
        try:
            return "digital_thermometer_temp_c", float(value_text)
        except ValueError:
            return "digital_thermometer_temp_c", None
    return "", None


def _write_temperature_evidence_from_io(run_dir: Path, io_path: Path) -> Dict[str, Path]:
    rows = []
    if io_path.exists():
        with io_path.open("r", encoding="utf-8-sig", newline="") as handle:
            for row in csv.DictReader(handle):
                device = str(row.get("device") or "")
                if device not in {"temperature_chamber", "thermometer"}:
                    continue
                if str(row.get("direction") or "").upper() != "RX":
                    continue
                metric, value = _parse_temperature_io_value(device, str(row.get("response") or ""))
                if not metric:
                    continue
                rows.append(
                    {
                        "timestamp": row.get("timestamp", ""),
                        "port": row.get("port", ""),
                        "device": device,
                        "metric": metric,
                        "value": "" if value is None else value,
                        "raw_response": row.get("response", ""),
                        "parse_status": "pass" if value is not None else "parse_error",
                    }
                )

    csv_path = run_dir / "temperature_evidence_from_io.csv"
    fieldnames = ["timestamp", "port", "device", "metric", "value", "raw_response", "parse_status"]
    with csv_path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    summary_rows = []
    by_metric: Dict[str, list[float]] = {}
    for row in rows:
        if row["parse_status"] != "pass":
            continue
        by_metric.setdefault(str(row["metric"]), []).append(float(row["value"]))
    for metric, values in sorted(by_metric.items()):
        if not values:
            continue
        summary_rows.append(
            {
                "metric": metric,
                "count": len(values),
                "min": min(values),
                "max": max(values),
                "mean": sum(values) / len(values),
                "last": values[-1],
            }
        )
    summary_path = run_dir / "temperature_evidence_from_io_summary.json"
    summary_path.write_text(
        json.dumps(
            {
                "source_io_path": str(io_path),
                "evidence_csv": str(csv_path),
                "total_temperature_io_rows": len(rows),
                "summary": summary_rows,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    return {"csv": csv_path, "summary": summary_path}


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ambient-air dry collect validation.")
    parser.add_argument("--config", default="configs/default_config.json")
    parser.add_argument("--output-dir", default=None)
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--include-pressure", action="store_true", help="Also read pressure devices if available.")
    parser.add_argument("--include-temperature", action="store_true", help="Also read chamber/thermometer if available.")
    parser.add_argument("--count", type=int, default=None, help="Override sample count for this sidecar validation only.")
    parser.add_argument("--interval-s", type=float, default=None, help="Override sample interval for this sidecar validation only.")
    parser.add_argument("--temp-set", type=float, default=20.0, help="Metadata-only temp setpoint stored in point row.")
    parser.add_argument("--pressure-target-hpa", type=float, default=None, help="Metadata-only pressure target stored in point row.")
    parser.add_argument(
        "--read-first-only-analyzers",
        action="store_true",
        help=(
            "Require existing MODE2 analyzer frames and fail instead of sending startup "
            "MODE/FTD/AVERAGE commands."
        ),
    )
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    cfg = load_config(args.config)
    runtime_cfg = _prepare_runtime_cfg(
        cfg,
        include_pressure=bool(args.include_pressure),
        include_temperature=bool(args.include_temperature),
    )
    if args.read_first_only_analyzers:
        _apply_read_first_only_analyzer_guard(runtime_cfg)
    if args.count is not None:
        runtime_cfg.setdefault("workflow", {}).setdefault("sampling", {})["stable_count"] = int(args.count)
        runtime_cfg["workflow"]["sampling"]["count"] = int(args.count)
    if args.interval_s is not None:
        runtime_cfg.setdefault("workflow", {}).setdefault("sampling", {})["interval_s"] = float(args.interval_s)

    output_dir = Path(args.output_dir).resolve() if args.output_dir else Path(runtime_cfg["paths"]["output_dir"]).resolve()
    run_id = args.run_id or f"dry_collect_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    logger = RunLogger(output_dir, run_id=run_id, cfg=runtime_cfg)
    devices: Dict[str, Any] = {}
    try:
        _log("Validation mode: ambient dry collect. No valve switching, no humidity-generator path, no gas-cylinder path.")
        devices = _build_devices(runtime_cfg, io_logger=logger)
        runner = CalibrationRunner(runtime_cfg, devices, logger, _log, lambda *_: None)
        runner._configure_devices()

        point = build_validation_point(
            index=1,
            temp_c=float(args.temp_set),
            pressure_hpa=args.pressure_target_hpa,
            point_tag="ambient_air_validation",
        )
        runner._sample_and_log(point, phase="co2", point_tag="ambient_air_validation")
        tables = analyze_sample_rows(
            runner._all_samples,
            cfg=runtime_cfg,
            gas="both",
            modes=("current",),
        )
        metadata = ValidationMetadata(
            tool_name="validate_dry_collect",
            analyzers=sorted({str(row.get("Analyzer") or "") for row in tables["frame_quality_summary"] if row.get("Analyzer")}),
            input_paths=[str(logger.samples_path), str(logger.points_path), str(logger.analyzer_summary_csv_path)],
            output_dir=str(logger.run_dir),
            config_path=str(Path(args.config).resolve()),
            config_summary={
                "include_pressure": bool(args.include_pressure),
                "include_temperature": bool(args.include_temperature),
                "read_first_only_analyzers": bool(args.read_first_only_analyzers),
                "sample_count": int(runtime_cfg["workflow"]["sampling"].get("stable_count", runtime_cfg["workflow"]["sampling"].get("count", 10))),
                "sample_interval_s": float(runtime_cfg["workflow"]["sampling"].get("interval_s", 1.0)),
            },
            notes=[
                "No coefficients are written by this tool.",
                "Auto-fit is preview-only through fit_input_overview; it does not write devices.",
            ],
        )
        outputs = write_validation_report(
            logger.run_dir,
            prefix="dry_collect_validation",
            metadata=metadata,
            tables=tables,
        )
        temp_outputs = _write_temperature_evidence_from_io(logger.run_dir, logger.io_path)
        _log(f"Dry collect validation saved: {outputs['workbook']}")
        if temp_outputs["csv"].exists():
            _log(f"Temperature IO evidence saved: {temp_outputs['csv']}")
        return 0
    except Exception as exc:
        _log(f"Dry collect validation failed: {exc}")
        return 1
    finally:
        _close_devices(devices)
        try:
            logger.close()
        except Exception:
            pass


if __name__ == "__main__":
    sys.exit(main())
