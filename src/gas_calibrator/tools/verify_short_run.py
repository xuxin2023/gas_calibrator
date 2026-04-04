"""Run a standalone engineering verification workflow and audit its outputs."""

from __future__ import annotations

import argparse
import copy
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from ..config import load_config
from ..diagnostics import run_self_test
from ..logging_utils import RunLogger
from ..workflow.runner import CalibrationRunner
from .audit_run import _print_report, audit_run_dir
from .run_headless import _build_devices, _close_devices, _enabled_failures


def _log(msg: str) -> None:
    print(msg, flush=True)


def build_short_verification_config(
    cfg: Dict[str, Any],
    *,
    temp_c: float,
    skip_co2_ppm: List[int],
    enable_connect_check: bool,
    points_excel_override: Optional[str] = None,
    output_dir_override: Optional[str] = None,
) -> Dict[str, Any]:
    runtime_cfg = copy.deepcopy(cfg)
    workflow_cfg = runtime_cfg.setdefault("workflow", {})

    workflow_cfg["collect_only"] = True
    workflow_cfg["route_mode"] = "h2o_then_co2"
    workflow_cfg["skip_h2o"] = False
    workflow_cfg["selected_temps_c"] = [] if points_excel_override else [float(temp_c)]
    workflow_cfg["skip_co2_ppm"] = list(sorted({int(item) for item in skip_co2_ppm}))
    if points_excel_override:
        workflow_cfg["preserve_explicit_point_matrix"] = True

    paths_cfg = runtime_cfg.setdefault("paths", {})
    if points_excel_override:
        paths_cfg["points_excel"] = str(Path(points_excel_override).resolve())
    if output_dir_override:
        paths_cfg["output_dir"] = str(Path(output_dir_override).resolve())

    connect_cfg = workflow_cfg.setdefault("startup_connect_check", {})
    if not isinstance(connect_cfg, dict):
        connect_cfg = {}
        workflow_cfg["startup_connect_check"] = connect_cfg
    connect_cfg["enabled"] = bool(enable_connect_check)

    pressure_precheck_cfg = workflow_cfg.setdefault("startup_pressure_precheck", {})
    if not isinstance(pressure_precheck_cfg, dict):
        pressure_precheck_cfg = {}
        workflow_cfg["startup_pressure_precheck"] = pressure_precheck_cfg
    pressure_precheck_cfg["enabled"] = False

    stability_cfg = workflow_cfg.setdefault("stability", {})
    if not isinstance(stability_cfg, dict):
        stability_cfg = {}
        workflow_cfg["stability"] = stability_cfg
    temp_cfg = stability_cfg.setdefault("temperature", {})
    if not isinstance(temp_cfg, dict):
        temp_cfg = {}
        stability_cfg["temperature"] = temp_cfg
    temp_cfg["analyzer_chamber_temp_timeout_s"] = min(
        float(temp_cfg.get("analyzer_chamber_temp_timeout_s", 300.0) or 300.0),
        300.0,
    )
    temp_cfg["analyzer_chamber_temp_first_valid_timeout_s"] = min(
        float(temp_cfg.get("analyzer_chamber_temp_first_valid_timeout_s", 60.0) or 60.0),
        60.0,
    )

    return runtime_cfg


def _parse_skip_co2_ppm(raw: str) -> List[int]:
    values: List[int] = []
    for part in str(raw or "").split(","):
        text = part.strip()
        if not text:
            continue
        values.append(int(text))
    return values


def _write_runtime_snapshot(run_dir: Path, runtime_cfg: Dict[str, Any]) -> None:
    (run_dir / "runtime_config_snapshot.json").write_text(
        json.dumps(runtime_cfg, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a standalone engineering short verification workflow.")
    parser.add_argument(
        "--config",
        default="configs/default_config.json",
        help="Base config json (default: configs/default_config.json)",
    )
    parser.add_argument(
        "--temp",
        type=float,
        default=20.0,
        help="Single chamber temperature to verify (default: 20C).",
    )
    parser.add_argument(
        "--skip-co2-ppm",
        default="",
        help="Comma-separated CO2 ppm values to skip for the short run.",
    )
    parser.add_argument(
        "--enable-connect-check",
        action="store_true",
        help="Keep startup connectivity self-test enabled.",
    )
    parser.add_argument(
        "--run-id",
        default=None,
        help="Optional fixed run_id folder name under logs/.",
    )
    parser.add_argument(
        "--points-excel",
        default=None,
        help="Optional verification-only points workbook override. When provided, mixed temperatures are allowed.",
    )
    parser.add_argument(
        "--output-dir",
        default=None,
        help="Optional verification output root override.",
    )
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    cfg = load_config(args.config)
    runtime_cfg = build_short_verification_config(
        cfg,
        temp_c=float(args.temp),
        skip_co2_ppm=_parse_skip_co2_ppm(args.skip_co2_ppm),
        enable_connect_check=bool(args.enable_connect_check),
        points_excel_override=args.points_excel,
        output_dir_override=args.output_dir,
    )
    run_id = args.run_id or f"verify_short_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    logger = RunLogger(Path(runtime_cfg["paths"]["output_dir"]), run_id=run_id, cfg=runtime_cfg)
    devices: Dict[str, Any] = {}

    _write_runtime_snapshot(logger.run_dir, runtime_cfg)
    _log(f"Run folder: {logger.run_dir}")
    _log(
        "Short verify overrides: "
        f"temp={float(args.temp):g}C route_mode=h2o_then_co2 "
        f"skip_co2_ppm={runtime_cfg['workflow'].get('skip_co2_ppm', [])} "
        f"connect_check={runtime_cfg['workflow'].get('startup_connect_check', {}).get('enabled', False)} "
        f"points_excel={runtime_cfg.get('paths', {}).get('points_excel', '')}"
    )

    try:
        if runtime_cfg.get("workflow", {}).get("startup_connect_check", {}).get("enabled", False):
            _log("Connectivity check...")
            results = run_self_test(runtime_cfg, log_fn=_log, io_logger=logger)
            failures = _enabled_failures(runtime_cfg, results)
            if failures:
                _log("Connectivity check failed:")
                for name, err in failures:
                    _log(f"- {name}: {err}")
                return 2

        devices = _build_devices(runtime_cfg, io_logger=logger)
        runner = CalibrationRunner(runtime_cfg, devices, logger, _log, _log)
        runner.run()

        audit_result = audit_run_dir(logger.run_dir, runtime_cfg=runtime_cfg)
        _print_report(audit_result)
        return 0 if audit_result.ok else 1
    except Exception as exc:
        _log(f"Short verification aborted: {exc}")
        return 1
    finally:
        _close_devices(devices)
        try:
            logger.close()
        except Exception:
            pass


if __name__ == "__main__":
    sys.exit(main())
