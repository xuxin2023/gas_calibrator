"""Headless workflow launcher for bench automation tests."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

from ..config import (
    V1_CO2_ONLY_H2O_NOT_SUPPORTED_MESSAGE,
    load_config,
    require_v1_h2o_zero_span_supported,
    v1_h2o_zero_span_capability,
)
from ..devices import (
    DewpointMeter,
    GasAnalyzer,
    HumidityGenerator,
    Pace5000,
    ParoscientificGauge,
    RelayController,
    TemperatureChamber,
    Thermometer,
)
from ..diagnostics import run_self_test
from ..logging_utils import RunLogger
from ..workflow import runner as runner_mod
from ..workflow.runner import CalibrationRunner


def _log(msg: str) -> None:
    print(msg, flush=True)


def _close_devices(devices: Dict[str, Any]) -> None:
    seen = set()
    for dev in devices.values():
        if isinstance(dev, dict):
            candidates = list(dev.values())
        elif isinstance(dev, (list, tuple, set)):
            candidates = list(dev)
        else:
            candidates = [dev]

        for item in candidates:
            if not hasattr(item, "close"):
                continue
            obj_id = id(item)
            if obj_id in seen:
                continue
            seen.add(obj_id)
            try:
                item.close()
            except Exception:
                pass


def _enabled_failures(cfg: Dict[str, Any], results: Dict[str, Any]) -> List[Tuple[str, str]]:
    failures: List[Tuple[str, str]] = []
    dcfg = cfg.get("devices", {})
    for name, result in results.items():
        if name == "gas_analyzer":
            single_enabled = bool(dcfg.get("gas_analyzer", {}).get("enabled", False))
            multi_cfg = dcfg.get("gas_analyzers", [])
            multi_enabled = any(
                isinstance(item, dict) and item.get("enabled", True) for item in multi_cfg
            ) if isinstance(multi_cfg, list) else False
            enabled = single_enabled or multi_enabled
        else:
            enabled = bool(dcfg.get(name, {}).get("enabled", False))

        if not enabled:
            continue
        if isinstance(result, dict) and result.get("ok"):
            continue
        err = result.get("err", "UNKNOWN") if isinstance(result, dict) else "UNKNOWN"
        failures.append((name, str(err)))
    return failures


def _build_devices(cfg: Dict[str, Any], io_logger: Optional[RunLogger] = None) -> Dict[str, Any]:
    dcfg = cfg["devices"]
    built: Dict[str, Any] = {}

    try:
        if dcfg["pressure_controller"]["enabled"]:
            built["pace"] = Pace5000(
                dcfg["pressure_controller"]["port"],
                dcfg["pressure_controller"]["baud"],
                timeout=float(dcfg["pressure_controller"].get("timeout", 1.0)),
                line_ending=dcfg["pressure_controller"].get("line_ending"),
                query_line_endings=dcfg["pressure_controller"].get("query_line_endings"),
                pressure_queries=dcfg["pressure_controller"].get("pressure_queries"),
                io_logger=io_logger,
            )
            built["pace"].open()

        if dcfg["pressure_gauge"]["enabled"]:
            built["pressure_gauge"] = ParoscientificGauge(
                dcfg["pressure_gauge"]["port"],
                dcfg["pressure_gauge"]["baud"],
                timeout=float(dcfg["pressure_gauge"].get("timeout", 1.0)),
                dest_id=dcfg["pressure_gauge"]["dest_id"],
                response_timeout_s=dcfg["pressure_gauge"].get("response_timeout_s"),
                io_logger=io_logger,
            )
            built["pressure_gauge"].open()

        if dcfg["dewpoint_meter"]["enabled"]:
            built["dewpoint"] = DewpointMeter(
                dcfg["dewpoint_meter"]["port"],
                dcfg["dewpoint_meter"]["baud"],
                station=dcfg["dewpoint_meter"]["station"],
                io_logger=io_logger,
            )
            built["dewpoint"].open()

        if dcfg["humidity_generator"]["enabled"]:
            built["humidity_gen"] = HumidityGenerator(
                dcfg["humidity_generator"]["port"],
                dcfg["humidity_generator"]["baud"],
                io_logger=io_logger,
            )
            built["humidity_gen"].open()

        built_primary_ga = False
        gas_list_cfg = dcfg.get("gas_analyzers", [])
        if isinstance(gas_list_cfg, list) and gas_list_cfg:
            for idx, gcfg in enumerate(gas_list_cfg, start=1):
                if not isinstance(gcfg, dict) or not gcfg.get("enabled", True):
                    continue
                key = f"gas_analyzer_{idx:02d}"
                dev = GasAnalyzer(
                    gcfg["port"],
                    gcfg.get("baud", 115200),
                    device_id=gcfg.get("device_id", f"{idx:03d}"),
                    io_logger=io_logger,
                )
                dev.open()
                built[key] = dev
                if not built_primary_ga:
                    built["gas_analyzer"] = dev
                    built_primary_ga = True
        elif dcfg["gas_analyzer"]["enabled"]:
            built["gas_analyzer"] = GasAnalyzer(
                dcfg["gas_analyzer"]["port"],
                dcfg["gas_analyzer"]["baud"],
                device_id=dcfg["gas_analyzer"]["device_id"],
                io_logger=io_logger,
            )
            built["gas_analyzer"].open()

        if dcfg["temperature_chamber"]["enabled"]:
            built["temp_chamber"] = TemperatureChamber(
                dcfg["temperature_chamber"]["port"],
                dcfg["temperature_chamber"]["baud"],
                addr=dcfg["temperature_chamber"]["addr"],
                io_logger=io_logger,
            )
            built["temp_chamber"].open()

        if dcfg["thermometer"]["enabled"]:
            built["thermometer"] = Thermometer(
                dcfg["thermometer"]["port"],
                dcfg["thermometer"]["baud"],
                timeout=dcfg["thermometer"].get("timeout", 1.2),
                parity=dcfg["thermometer"].get("parity", "N"),
                stopbits=dcfg["thermometer"].get("stopbits", 1),
                bytesize=dcfg["thermometer"].get("bytesize", 8),
                io_logger=io_logger,
            )
            built["thermometer"].open()

        if dcfg["relay"]["enabled"]:
            built["relay"] = RelayController(
                dcfg["relay"]["port"],
                dcfg["relay"]["baud"],
                addr=dcfg["relay"]["addr"],
                io_logger=io_logger,
            )
            built["relay"].open()

        relay8_cfg = dcfg.get("relay_8", {})
        if relay8_cfg.get("enabled"):
            built["relay_8"] = RelayController(
                relay8_cfg["port"],
                relay8_cfg["baud"],
                addr=relay8_cfg["addr"],
                io_logger=io_logger,
            )
            built["relay_8"].open()
    except Exception:
        _close_devices(built)
        raise

    return built


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run calibration workflow headlessly.")
    parser.add_argument(
        "--config",
        default="configs/default_config.json",
        help="Path to config json (default: configs/default_config.json)",
    )
    parser.add_argument(
        "--temp",
        type=float,
        default=None,
        help="Only run points matching this chamber temperature (e.g. 20).",
    )
    parser.add_argument(
        "--run-id",
        default=None,
        help="Optional fixed run_id folder name under logs/",
    )
    parser.add_argument(
        "--skip-connect-check",
        action="store_true",
        help="Skip startup connectivity self-test gate.",
    )
    parser.add_argument(
        "--skip-h2o",
        action="store_true",
        help="Skip H2O route and start directly from CO2 route for the selected temperature group.",
    )
    return parser.parse_args(list(argv) if argv is not None else None)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    cfg = load_config(args.config)
    coeff_cfg = cfg.get("coefficients", {}) if isinstance(cfg.get("coefficients", {}), dict) else {}
    capability = v1_h2o_zero_span_capability(coeff_cfg)
    _log(
        "Capability boundary: "
        f"H2O zero/span status={capability['status']} note={capability['note']}"
    )
    try:
        require_v1_h2o_zero_span_supported(coeff_cfg, context="run_headless")
    except RuntimeError as exc:
        _log(str(exc))
        _log(V1_CO2_ONLY_H2O_NOT_SUPPORTED_MESSAGE)
        return 2
    logger = RunLogger(Path(cfg["paths"]["output_dir"]), run_id=args.run_id, cfg=cfg)
    _log(f"Run folder: {logger.run_dir}")

    patched_loader = False
    original_loader = runner_mod.load_points_from_excel
    devices: Dict[str, Any] = {}

    try:
        if args.skip_h2o:
            cfg.setdefault("workflow", {})["skip_h2o"] = True
            _log("Workflow override: skip_h2o=True")

        if args.temp is not None:
            target = float(args.temp)

            def _filtered_loader(path: str, missing_pressure_policy: str = "require", **kwargs):
                points = original_loader(
                    path,
                    missing_pressure_policy=missing_pressure_policy,
                    **kwargs,
                )
                filtered = [
                    p
                    for p in points
                    if p.temp_chamber_c is not None and abs(float(p.temp_chamber_c) - target) < 1e-9
                ]
                _log(f"Point filter: temp={target:g}C -> {len(filtered)}/{len(points)} points")
                return filtered

            runner_mod.load_points_from_excel = _filtered_loader
            patched_loader = True

        if not args.skip_connect_check and cfg.get("workflow", {}).get("startup_connect_check", {}).get("enabled", False):
            _log("Connectivity check...")
            results = run_self_test(cfg, log_fn=_log, io_logger=logger)
            failures = _enabled_failures(cfg, results)
            if failures:
                _log("Connectivity check failed:")
                for name, err in failures:
                    _log(f"- {name}: {err}")
                return 2

        devices = _build_devices(cfg, io_logger=logger)
        runner = CalibrationRunner(cfg, devices, logger, _log, _log)
        runner.run()
        return 0
    except Exception as exc:
        _log(f"Headless run aborted: {exc}")
        return 1
    finally:
        if patched_loader:
            runner_mod.load_points_from_excel = original_loader
        _close_devices(devices)
        try:
            logger.close()
        except Exception:
            pass


if __name__ == "__main__":
    sys.exit(main())
