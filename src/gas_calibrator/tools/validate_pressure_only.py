"""Pressure-source validation without humidity generator or gas cylinders.

This tool only samples analyzers plus pressure devices. It does not switch
routes or actuate the V1 production workflow.
"""

from __future__ import annotations

import argparse
import copy
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from ..config import load_config
from ..logging_utils import RunLogger
from ..validation.common import analyze_sample_rows, build_validation_point
from ..validation.reporting import ValidationMetadata, write_validation_report
from ..workflow.runner import CalibrationRunner
from .run_headless import _build_devices, _close_devices


def _log(message: str) -> None:
    print(message, flush=True)


def _parse_pressure_points(raw: str | None) -> List[Optional[float]]:
    if not raw:
        return [None]
    out: List[Optional[float]] = []
    for part in str(raw).split(","):
        text = part.strip()
        if not text or text.lower() == "ambient":
            out.append(None)
            continue
        out.append(float(text))
    return out or [None]


def _prepare_runtime_cfg(cfg: Dict[str, Any]) -> Dict[str, Any]:
    runtime_cfg = copy.deepcopy(cfg)
    devices_cfg = runtime_cfg.setdefault("devices", {})
    for key in ("humidity_generator", "dewpoint_meter", "relay", "relay_8", "temperature_chamber", "thermometer"):
        if isinstance(devices_cfg.get(key), dict):
            devices_cfg[key]["enabled"] = False
    runtime_cfg.setdefault("workflow", {})["collect_only"] = True
    return runtime_cfg


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Pressure-only validation under ambient air.")
    parser.add_argument("--config", default="configs/default_config.json")
    parser.add_argument("--output-dir", default=None)
    parser.add_argument("--run-id", default=None)
    parser.add_argument(
        "--pressure-points",
        default="ambient",
        help="Comma-separated metadata pressure points, e.g. ambient,500,800,1100. The tool does not control pressure hardware by default.",
    )
    parser.add_argument("--count", type=int, default=None)
    parser.add_argument("--interval-s", type=float, default=None)
    parser.add_argument(
        "--no-prompt",
        action="store_true",
        help="Do not prompt between batches. Useful for a single ambient batch or tests.",
    )
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    cfg = load_config(args.config)
    runtime_cfg = _prepare_runtime_cfg(cfg)
    if args.count is not None:
        runtime_cfg.setdefault("workflow", {}).setdefault("sampling", {})["stable_count"] = int(args.count)
        runtime_cfg["workflow"]["sampling"]["count"] = int(args.count)
    if args.interval_s is not None:
        runtime_cfg.setdefault("workflow", {}).setdefault("sampling", {})["interval_s"] = float(args.interval_s)

    output_dir = Path(args.output_dir).resolve() if args.output_dir else Path(runtime_cfg["paths"]["output_dir"]).resolve()
    run_id = args.run_id or f"pressure_only_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    logger = RunLogger(output_dir, run_id=run_id, cfg=runtime_cfg)
    devices: Dict[str, Any] = {}

    try:
        pressure_points = _parse_pressure_points(args.pressure_points)
        _log("Validation mode: pressure-only ambient verification. No route switching. No gas-cylinder or humidity-generator dependency.")
        devices = _build_devices(runtime_cfg, io_logger=logger)
        if "pressure_gauge" not in devices and "pace" not in devices:
            _log("Pressure-only validation warning: no pressure_gauge / pace device available; report will only include analyzer BAR if present.")
        runner = CalibrationRunner(runtime_cfg, devices, logger, _log, lambda *_: None)
        runner._configure_devices()

        for index, target in enumerate(pressure_points, start=1):
            if not args.no_prompt:
                prompt = "ambient pressure" if target is None else f"{target:g} hPa"
                input(f"Prepare current pressure condition at {prompt}, then press Enter to sample...")
            point_tag = "pressure_only_ambient" if target is None else f"pressure_only_{target:g}hpa"
            point = build_validation_point(
                index=index,
                temp_c=20.0,
                pressure_hpa=target,
                point_tag=point_tag,
            )
            runner._sample_and_log(point, phase="co2", point_tag=point_tag)

        tables = analyze_sample_rows(
            runner._all_samples,
            cfg=runtime_cfg,
            gas="both",
            modes=("current",),
        )
        metadata = ValidationMetadata(
            tool_name="validate_pressure_only",
            analyzers=sorted({str(row.get("Analyzer") or "") for row in tables["frame_quality_summary"] if row.get("Analyzer")}),
            input_paths=[str(logger.samples_path), str(logger.points_path), str(logger.analyzer_summary_csv_path)],
            output_dir=str(logger.run_dir),
            config_path=str(Path(args.config).resolve()),
            config_summary={
                "pressure_points": ["ambient" if item is None else item for item in pressure_points],
                "sample_count": int(runtime_cfg["workflow"]["sampling"].get("stable_count", runtime_cfg["workflow"]["sampling"].get("count", 10))),
                "sample_interval_s": float(runtime_cfg["workflow"]["sampling"].get("interval_s", 1.0)),
                "prompt_between_batches": not bool(args.no_prompt),
            },
            notes=[
                "This tool does not control pressure hardware by default.",
                "Use it to compare external P and analyzer BAR under manually prepared pressure conditions.",
            ],
        )
        outputs = write_validation_report(
            logger.run_dir,
            prefix="pressure_only_validation",
            metadata=metadata,
            tables=tables,
        )
        _log(f"Pressure-only validation saved: {outputs['workbook']}")
        return 0
    except KeyboardInterrupt:
        _log("Pressure-only validation cancelled by user.")
        return 130
    except Exception as exc:
        _log(f"Pressure-only validation failed: {exc}")
        return 1
    finally:
        _close_devices(devices)
        try:
            logger.close()
        except Exception:
            pass


if __name__ == "__main__":
    sys.exit(main())
