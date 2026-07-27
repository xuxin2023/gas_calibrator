"""Run a V1.5 formal H2O open-flow queue across temperature groups.

This is a thin orchestrator around the already-proven single-point H2O
open-flow sidecar. It controls the temperature chamber once per temperature
group, then runs all H2O points at that temperature without sealed pressure
control and without any SENCO/ID writes.
"""

from __future__ import annotations

import argparse
import copy
import csv
import hashlib
import json
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence

from ..config import load_config
from ..data.points import CalibrationPoint
from ..logging_utils import RunLogger, _claim_immutable_run_dir
from ..validation.v1_5_h2o_queue_failure_audit import (
    audit_and_write as _audit_h2o_queue_failures,
    classify_point_failure_from_log as _classify_point_failure_from_log,
)
from ..validation.v1_5_open_flow_purge_contract import resolve_v1_5_open_flow_purge
from ..workflow.runner import CalibrationRunner
from .run_headless import _build_devices, _close_devices
from .run_v1_5_formal_h2o_open_flow_sampling import (
    _safe_stop_humidity_generator,
)
from .run_v1_5_formal_open_flow_sampling import (
    FORMAL_OPEN_FLOW_ANALYZER_GATE_PREFER_ALL_STABLE_GRACE_S,
    V1_5_ENGINEERING_PROBE_CONFIRMATION_TEXT,
    _apply_analyzer_acquisition_policy,
    _apply_v1_5_temperature_truth_contract,
    _defer_startup_mode2_disabled_analyzers,
    _engineering_probe_authorization_errors,
    _write_operator_confirmation_record,
)


def _log(message: str) -> None:
    print(message, flush=True)


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run V1.5 no-write H2O open-flow sampling queue across temperature groups."
    )
    parser.add_argument("--config", required=True, help="Runtime config JSON.")
    parser.add_argument("--queue-csv", required=True, help="Canonical h2o_runner_queue.csv.")
    parser.add_argument("--output-dir", required=True, help="Output directory for queue evidence.")
    parser.add_argument("--run-id", default=None, help="Optional queue run id.")
    parser.add_argument("--temps", default="all", help="Comma-separated temperatures to run, or all.")
    parser.add_argument(
        "--temperature-order",
        choices=("asc", "desc", "queue"),
        default="asc",
        help="Temperature group order. H2O formal default is low-to-high.",
    )
    parser.add_argument("--purge-s", type=float, default=None, help="Override purge seconds.")
    parser.add_argument("--sample-count", type=int, default=None, help="Override sample count.")
    parser.add_argument("--sample-interval-s", type=float, default=1.0)
    parser.add_argument("--sensor-read-interval-s", type=float, default=5.0)
    parser.add_argument(
        "--analyzer-acquisition",
        choices=("active_stream_10hz", "active_stream_1hz", "passive_query"),
        default="active_stream_1hz",
    )
    ftd_group = parser.add_mutually_exclusive_group()
    ftd_group.add_argument("--allow-ftd-write", dest="allow_ftd_write", action="store_true", default=False)
    ftd_group.add_argument("--no-ftd-write", dest="allow_ftd_write", action="store_false")
    parser.add_argument("--min-valid-analyzers", type=int, default=1)
    parser.add_argument(
        "--analyzer-gate-prefer-all-stable-grace-s",
        type=float,
        default=FORMAL_OPEN_FLOW_ANALYZER_GATE_PREFER_ALL_STABLE_GRACE_S,
        help=(
            "After min-valid analyzers become stable, wait this many seconds for the remaining "
            "analyzers to become stable before accepting the point with independent grading."
        ),
    )
    parser.add_argument(
        "--h2o-pressure-presample-policy",
        choices=("warn", "fail", "skip"),
        default="skip",
        help=(
            "Formal H2O open-flow default skips pre-sample pressure waiting; pressure remains "
            "diagnostic evidence. Opt in to warn/fail only for engineering review."
        ),
    )
    parser.add_argument(
        "--safe-stop-hgen-each-point",
        action="store_true",
        help=(
            "Opt in to the old point-level HGEN shutdown behavior. The formal queue default "
            "keeps HGEN running between humidity points and safe-stops it at queue end."
        ),
    )
    parser.add_argument("--hgen-flow-lpm", type=float, default=None)
    parser.add_argument("--strict-humidity-reference-match", action="store_true")
    parser.add_argument("--skip-humidity-generator-gate", action="store_true")
    parser.add_argument("--skip-dewpoint-gate", action="store_true")
    parser.add_argument(
        "--control-temperature",
        dest="control_temperature",
        action="store_true",
        default=True,
        help="Control and wait for the temperature chamber once per temperature group.",
    )
    parser.add_argument("--no-control-temperature", dest="control_temperature", action="store_false")
    parser.add_argument("--temperature-soak-after-reach-s", type=float, default=None)
    parser.add_argument("--temperature-tol-c", type=float, default=None)
    parser.add_argument("--temperature-timeout-s", type=float, default=None)
    parser.add_argument("--temperature-hard-max-wait-s", type=float, default=None)
    parser.add_argument("--temperature-analyzer-span-c", type=float, default=None)
    parser.add_argument("--temperature-analyzer-window-s", type=float, default=None)
    parser.add_argument("--temperature-analyzer-timeout-s", type=float, default=None)
    parser.add_argument("--max-points", type=int, default=None)
    parser.add_argument("--stop-on-point-fail", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--no-prompt", action="store_true")
    parser.add_argument("--engineering-probe-only", action="store_true")
    parser.add_argument(
        "--operator-confirmation",
        default="",
        help=(
            "Exact second-unlock text required for a real queue: "
            f"{V1_5_ENGINEERING_PROBE_CONFIRMATION_TEXT!r}."
        ),
    )
    return parser.parse_args(list(argv) if argv is not None else None)


def _safe_float(value: Any) -> Optional[float]:
    if value in (None, "", "None", "null"):
        return None
    try:
        numeric = float(value)
    except Exception:
        return None
    if numeric != numeric:
        return None
    return numeric


def _format_value(value: Any) -> str:
    numeric = _safe_float(value)
    if numeric is None:
        return str(value or "")
    if float(numeric).is_integer():
        return str(int(numeric))
    return f"{numeric:g}"


def _parse_float_filter(text: str) -> Optional[set[float]]:
    raw = str(text or "").strip()
    if not raw or raw.lower() == "all":
        return None
    out: set[float] = set()
    for item in raw.split(","):
        item = item.strip()
        if item:
            out.add(float(item))
    return out


def _load_queue_rows(path: str | Path) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    with Path(path).open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            if str(row.get("component") or "").strip().lower() != "h2o":
                continue
            temp = _safe_float(row.get("temp_c"))
            hgen_temp = _safe_float(row.get("hgen_temp_c"))
            hgen_rh = _safe_float(row.get("hgen_rh_pct"))
            if temp is None or hgen_temp is None or hgen_rh is None:
                continue
            rows.append(
                {
                    **row,
                    "temp_c": float(temp),
                    "hgen_temp_c": float(hgen_temp),
                    "hgen_rh_pct": float(hgen_rh),
                    "reference_dewpoint_c": _safe_float(row.get("reference_dewpoint_c")),
                    "reference_h2o_mmol": _safe_float(row.get("reference_h2o_mmol")),
                    "certificate_uncertainty_mmol": _safe_float(row.get("certificate_uncertainty_mmol")),
                    "sample_role": str(row.get("sample_role") or "fit").strip().lower() or "fit",
                    "purge_s": _safe_float(row.get("purge_s")),
                    "sample_count": int(_safe_float(row.get("sample_count")) or 10),
                    "analyzer_acquisition": str(row.get("analyzer_acquisition") or "").strip(),
                }
            )
    return rows


def _select_queue_rows(
    rows: Sequence[Mapping[str, Any]],
    *,
    temps: Optional[set[float]],
    max_points: Optional[int],
) -> List[Dict[str, Any]]:
    selected: List[Dict[str, Any]] = []
    for row in rows:
        temp = float(row["temp_c"])
        if temps is not None and not any(abs(temp - item) < 1e-9 for item in temps):
            continue
        selected.append(dict(row))
        if max_points is not None and len(selected) >= int(max_points):
            break
    return selected


def _ordered_temperature_groups(
    rows: Sequence[Mapping[str, Any]],
    *,
    order: str,
) -> List[tuple[float, List[Dict[str, Any]]]]:
    groups: Dict[float, List[Dict[str, Any]]] = {}
    sequence: List[float] = []
    for row in rows:
        temp = float(row["temp_c"])
        if temp not in groups:
            groups[temp] = []
            sequence.append(temp)
        groups[temp].append(dict(row))
    if order == "asc":
        temps = sorted(groups)
    elif order == "desc":
        temps = sorted(groups, reverse=True)
    else:
        temps = sequence
    return [(temp, groups[temp]) for temp in temps]


def _prepare_temperature_runtime_cfg(
    cfg: Dict[str, Any],
    *,
    output_dir: Path,
    analyzer_acquisition: str,
    allow_ftd_write: bool,
    sample_interval_s: float,
    sensor_read_interval_s: float,
    soak_after_reach_s: Optional[float],
    tol_c: Optional[float],
    timeout_s: Optional[float],
    hard_max_wait_s: Optional[float],
    analyzer_span_c: Optional[float],
    analyzer_window_s: Optional[float],
    analyzer_timeout_s: Optional[float],
) -> Dict[str, Any]:
    runtime_cfg = copy.deepcopy(cfg)
    runtime_cfg.setdefault("paths", {})["output_dir"] = str(output_dir.resolve())
    runtime_cfg.setdefault("metadata", {})["v1_5_h2o_queue_temperature_settle"] = True
    runtime_cfg["metadata"]["writes_senco"] = False
    runtime_cfg["metadata"]["writes_device_id"] = False
    workflow_cfg = runtime_cfg.setdefault("workflow", {})
    workflow_cfg["collect_only"] = True
    workflow_cfg["skip_h2o"] = True
    workflow_cfg["route_mode"] = "h2o_open_flow_temperature_settle"

    devices_cfg = runtime_cfg.setdefault("devices", {})
    for key in ("pressure_controller", "pressure_gauge", "dewpoint_meter", "humidity_generator", "relay", "relay_8"):
        if isinstance(devices_cfg.get(key), dict):
            devices_cfg[key]["enabled"] = False
    if not isinstance(devices_cfg.get("temperature_chamber"), dict):
        raise RuntimeError("temperature_chamber config is missing")
    devices_cfg["temperature_chamber"]["enabled"] = True
    temp_cfg = _apply_v1_5_temperature_truth_contract(runtime_cfg, require_device_config=True)

    _apply_analyzer_acquisition_policy(
        runtime_cfg,
        analyzer_acquisition=analyzer_acquisition,
        sensor_read_interval_s=sensor_read_interval_s,
        sample_interval_s=sample_interval_s,
        allow_ftd_write=allow_ftd_write,
    )

    temp_cfg["wait_for_target_before_continue"] = True
    temp_cfg["analyzer_chamber_temp_enabled"] = True
    if soak_after_reach_s is not None:
        temp_cfg["soak_after_reach_s"] = float(soak_after_reach_s)
    if tol_c is not None:
        temp_cfg["tol"] = float(tol_c)
    if timeout_s is not None:
        temp_cfg["timeout_s"] = float(timeout_s)
    if hard_max_wait_s is not None:
        temp_cfg["hard_max_wait_s"] = float(hard_max_wait_s)
    temp_cfg["analyzer_chamber_temp_span_c"] = (
        float(analyzer_span_c)
        if analyzer_span_c is not None
        else max(float(temp_cfg.get("analyzer_chamber_temp_span_c", 0.08) or 0.08), 0.08)
    )
    temp_cfg["analyzer_chamber_temp_window_s"] = (
        float(analyzer_window_s)
        if analyzer_window_s is not None
        else max(float(temp_cfg.get("analyzer_chamber_temp_window_s", 60.0) or 60.0), 60.0)
    )
    temp_cfg["analyzer_chamber_temp_timeout_s"] = (
        float(analyzer_timeout_s)
        if analyzer_timeout_s is not None
        else max(float(temp_cfg.get("analyzer_chamber_temp_timeout_s", 5400.0) or 5400.0), 5400.0)
    )
    return runtime_cfg


def _prepare_humidity_prewarm_runtime_cfg(
    cfg: Dict[str, Any],
    *,
    output_dir: Path,
) -> Dict[str, Any]:
    runtime_cfg = copy.deepcopy(cfg)
    runtime_cfg.setdefault("paths", {})["output_dir"] = str(output_dir.resolve())
    metadata = runtime_cfg.setdefault("metadata", {})
    metadata["v1_5_h2o_queue_humidity_generator_prewarm"] = True
    metadata["writes_senco"] = False
    metadata["writes_device_id"] = False
    metadata["opens_h2o_route"] = False
    metadata["controls_water_route"] = False
    metadata["physical_meaning"] = (
        "The humidity generator is preheated at the next H2O setpoint while the chamber "
        "settles. No H2O route valves are opened, so humid gas is not sent through the "
        "analyzer chain until the formal point starts."
    )

    workflow_cfg = runtime_cfg.setdefault("workflow", {})
    workflow_cfg["collect_only"] = True
    workflow_cfg["route_mode"] = "h2o_humidity_generator_prewarm_only"

    devices_cfg = runtime_cfg.setdefault("devices", {})
    for key in (
        "pressure_controller",
        "pressure_gauge",
        "dewpoint_meter",
        "temperature_chamber",
        "thermometer",
        "relay",
        "relay_8",
    ):
        if isinstance(devices_cfg.get(key), dict):
            devices_cfg[key]["enabled"] = False
    if isinstance(devices_cfg.get("gas_analyzer"), dict):
        devices_cfg["gas_analyzer"]["enabled"] = False
    gas_analyzers = devices_cfg.get("gas_analyzers")
    if isinstance(gas_analyzers, list):
        for item in gas_analyzers:
            if isinstance(item, dict):
                item["enabled"] = False

    hgen_cfg = devices_cfg.get("humidity_generator")
    if not isinstance(hgen_cfg, dict):
        raise RuntimeError("humidity_generator config is missing")
    hgen_cfg["enabled"] = True
    return runtime_cfg


def _read_humidity_generator_snapshot(device: Any) -> Dict[str, Any]:
    if device is None:
        return {}
    fetch_all = getattr(device, "fetch_all", None)
    if not callable(fetch_all):
        return {}
    try:
        snapshot = fetch_all()
    except Exception as exc:
        return {"error": str(exc)}
    return snapshot if isinstance(snapshot, dict) else {"raw": snapshot}


def _prewarm_humidity_generator_for_group(
    cfg: Dict[str, Any],
    *,
    temp_c: float,
    lead_row: Mapping[str, Any],
    output_dir: Path,
    run_id: str,
) -> bool:
    hgen_temp = _safe_float(lead_row.get("hgen_temp_c"))
    hgen_rh = _safe_float(lead_row.get("hgen_rh_pct"))
    if hgen_temp is None or hgen_rh is None:
        return True

    runtime_cfg = _prepare_humidity_prewarm_runtime_cfg(cfg, output_dir=output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    logger = RunLogger(
        output_dir,
        run_id=run_id,
        cfg=runtime_cfg,
        immutable_run_dir=True,
    )
    runtime_config_snapshot_error: Optional[str] = None
    runtime_config_snapshot_path = logger.run_dir / "humidity_prewarm_runtime_config_snapshot.json"
    try:
        runtime_config_snapshot_path.parent.mkdir(parents=True, exist_ok=True)
        runtime_config_snapshot_path.write_text(
            json.dumps(runtime_cfg, ensure_ascii=False, indent=2, default=str) + "\n",
            encoding="utf-8",
        )
    except Exception as exc:
        runtime_config_snapshot_error = str(exc)
    devices: Dict[str, Any] = {}
    summary: Dict[str, Any] = {
        "schema_version": "v1_5_h2o_queue_humidity_generator_prewarm_v0",
        "run_id": run_id,
        "timestamp": datetime.now().isoformat(timespec="seconds"),
        "temp_group_c": float(temp_c),
        "target_hgen_temp_c": float(hgen_temp),
        "target_hgen_rh_pct": float(hgen_rh),
        "ok": False,
        "route_opened": False,
        "open_valves": [],
        "sealed_pressure_control": False,
        "writes_senco": False,
        "writes_device_id": False,
        "runtime_config_snapshot_path": str(runtime_config_snapshot_path),
        "runtime_config_snapshot_error": runtime_config_snapshot_error,
        "physical_meaning": (
            "Prewarm the humidity generator during chamber stabilization. The H2O route stays "
            "closed, so the analyzer chain is not exposed to humid gas until the formal point "
            "opens the water route and passes humidity/dewpoint gates."
        ),
    }
    try:
        devices = _build_devices(runtime_cfg, io_logger=logger)
        runner = CalibrationRunner(runtime_cfg, devices, logger, _log, lambda *_: None)
        point = CalibrationPoint(
            index=0,
            temp_chamber_c=float(temp_c),
            co2_ppm=None,
            hgen_temp_c=float(hgen_temp),
            hgen_rh_pct=float(hgen_rh),
            target_pressure_hpa=None,
            dewpoint_c=_safe_float(lead_row.get("reference_dewpoint_c")),
            h2o_mmol=_safe_float(lead_row.get("reference_h2o_mmol")),
            raw_h2o=None,
        )
        summary["before_snapshot"] = _read_humidity_generator_snapshot(devices.get("humidity_gen"))
        runner._prepare_humidity_generator(point)
        summary["after_snapshot"] = _read_humidity_generator_snapshot(devices.get("humidity_gen"))
        summary["ok"] = True
        return True
    except Exception as exc:
        summary["error"] = str(exc)
        _log(f"Humidity generator prewarm failed at {temp_c:g}C: {exc}")
        return False
    finally:
        try:
            (logger.run_dir / "humidity_prewarm_summary.json").write_text(
                json.dumps(summary, ensure_ascii=False, indent=2, default=str) + "\n",
                encoding="utf-8",
            )
        except Exception:
            pass
        try:
            _close_devices(devices)
        except Exception:
            pass
        try:
            logger.close()
        except Exception:
            pass


def _safe_stop_humidity_generator_after_queue(
    cfg: Dict[str, Any],
    *,
    output_dir: Path,
    run_id: str,
) -> bool:
    runtime_cfg = _prepare_humidity_prewarm_runtime_cfg(cfg, output_dir=output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    logger = RunLogger(
        output_dir,
        run_id=run_id,
        cfg=runtime_cfg,
        immutable_run_dir=True,
    )
    (logger.run_dir / "hgen_final_safe_stop_runtime_config.json").write_text(
        json.dumps(runtime_cfg, ensure_ascii=False, indent=2, default=str) + "\n",
        encoding="utf-8",
    )
    devices: Dict[str, Any] = {}
    summary: Dict[str, Any] = {
        "schema_version": "v1_5_h2o_queue_humidity_generator_final_safe_stop_v0",
        "run_id": run_id,
        "timestamp": datetime.now().isoformat(timespec="seconds"),
        "ok": False,
        "route_opened": False,
        "open_valves": [],
        "sealed_pressure_control": False,
        "writes_senco": False,
        "writes_device_id": False,
        "physical_meaning": (
            "The H2O queue keeps the humidity generator thermally active between points. "
            "After the last point, the queue performs one explicit safe-stop while all water "
            "route valves remain closed."
        ),
    }
    try:
        devices = _build_devices(runtime_cfg, io_logger=logger)
        summary["before_snapshot"] = _read_humidity_generator_snapshot(devices.get("humidity_gen"))
        safe_stop_status = _safe_stop_humidity_generator(devices)
        summary["safe_stop_status"] = safe_stop_status
        if not isinstance(safe_stop_status, dict) or safe_stop_status.get("ok") is not True:
            raise RuntimeError(
                "HUMIDITY_GENERATOR_FINAL_SAFE_STOP_NOT_CONFIRMED:"
                + str(
                    safe_stop_status.get("error", "unreported")
                    if isinstance(safe_stop_status, dict)
                    else "unreported"
                )
            )
        summary["after_snapshot"] = _read_humidity_generator_snapshot(devices.get("humidity_gen"))
        summary["ok"] = True
        return True
    except Exception as exc:
        summary["error"] = str(exc)
        _log(f"Humidity generator final safe-stop failed: {exc}")
        return False
    finally:
        try:
            (logger.run_dir / "humidity_generator_queue_final_safe_stop.json").write_text(
                json.dumps(summary, ensure_ascii=False, indent=2, default=str) + "\n",
                encoding="utf-8",
            )
        except Exception:
            pass
        try:
            _close_devices(devices)
        except Exception:
            pass
        try:
            logger.close()
        except Exception:
            pass


def _settle_temperature_group(
    cfg: Dict[str, Any],
    *,
    temp_c: float,
    output_dir: Path,
    run_id: str,
    args: argparse.Namespace,
) -> bool:
    runtime_cfg = _prepare_temperature_runtime_cfg(
        cfg,
        output_dir=output_dir,
        analyzer_acquisition=args.analyzer_acquisition,
        allow_ftd_write=bool(args.allow_ftd_write),
        sample_interval_s=float(args.sample_interval_s),
        sensor_read_interval_s=float(args.sensor_read_interval_s),
        soak_after_reach_s=args.temperature_soak_after_reach_s,
        tol_c=args.temperature_tol_c,
        timeout_s=args.temperature_timeout_s,
        hard_max_wait_s=args.temperature_hard_max_wait_s,
        analyzer_span_c=args.temperature_analyzer_span_c,
        analyzer_window_s=args.temperature_analyzer_window_s,
        analyzer_timeout_s=args.temperature_analyzer_timeout_s,
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    logger = RunLogger(
        output_dir,
        run_id=run_id,
        cfg=runtime_cfg,
        immutable_run_dir=True,
    )
    (logger.run_dir / "temperature_settle_runtime_config_snapshot.json").write_text(
        json.dumps(runtime_cfg, ensure_ascii=False, indent=2, default=str) + "\n",
        encoding="utf-8",
    )
    devices: Dict[str, Any] = {}
    try:
        devices = _build_devices(runtime_cfg, io_logger=logger)
        if "temp_chamber" not in devices:
            raise RuntimeError("temperature_chamber did not open")
        runner = CalibrationRunner(runtime_cfg, devices, logger, _log, lambda *_: None)
        runner._configure_devices()
        runner._startup_preflight_reset()
        _defer_startup_mode2_disabled_analyzers(runner)
        ok = bool(runner._set_temperature(float(temp_c)))
        (logger.run_dir / "temperature_settle_summary.json").write_text(
            json.dumps(
                {
                    "schema_version": "v1_5_h2o_queue_temperature_settle_v0",
                    "run_id": run_id,
                    "temp_c": float(temp_c),
                    "ok": ok,
                    "physical_meaning": (
                        "The chamber and analyzer bodies are settled before opening the H2O route "
                        "at this temperature group. This separates body-temperature compensation "
                        "evidence from humidity-generator and dewpoint stabilization."
                    ),
                },
                ensure_ascii=False,
                indent=2,
                default=str,
            )
            + "\n",
            encoding="utf-8",
        )
        return ok
    except Exception as exc:
        _log(f"Temperature settle failed at {temp_c:g}C: {exc}")
        try:
            (logger.run_dir / "temperature_settle_summary.json").write_text(
                json.dumps(
                    {
                        "schema_version": "v1_5_h2o_queue_temperature_settle_v0",
                        "run_id": run_id,
                        "temp_c": float(temp_c),
                        "ok": False,
                        "error": str(exc),
                    },
                    ensure_ascii=False,
                    indent=2,
                    default=str,
                )
                + "\n",
                encoding="utf-8",
            )
        except Exception:
            pass
        return False
    finally:
        try:
            _close_devices(devices)
        except Exception:
            pass
        try:
            logger.close()
        except Exception:
            pass


def _build_point_command(
    *,
    config_path: str,
    output_dir: Path,
    row: Mapping[str, Any],
    run_id: str,
    args: argparse.Namespace,
) -> List[str]:
    purge_resolution = resolve_v1_5_open_flow_purge(
        component="h2o",
        row=row,
        explicit_purge_s=args.purge_s,
    )
    purge_s = purge_resolution.purge_s
    sample_count = int(args.sample_count if args.sample_count is not None else row.get("sample_count") or 10)
    acquisition = str(row.get("analyzer_acquisition") or args.analyzer_acquisition or "active_stream_1hz")
    cmd = [
        sys.executable,
        "-m",
        "gas_calibrator.tools.run_v1_5_formal_h2o_open_flow_sampling",
        "--config",
        str(config_path),
        "--output-dir",
        str(output_dir.resolve()),
        "--run-id",
        run_id,
        "--temp",
        _format_value(row["temp_c"]),
        "--hgen-temp",
        _format_value(row["hgen_temp_c"]),
        "--hgen-rh",
        _format_value(row["hgen_rh_pct"]),
        "--purge-s",
        _format_value(purge_s),
        "--minimum-purge-s",
        _format_value(purge_resolution.minimum_purge_s),
        "--sample-count",
        str(sample_count),
        "--sample-interval-s",
        _format_value(args.sample_interval_s),
        "--sensor-read-interval-s",
        _format_value(args.sensor_read_interval_s),
        "--analyzer-acquisition",
        acquisition,
        "--min-valid-analyzers",
        str(int(args.min_valid_analyzers)),
        "--analyzer-gate-prefer-all-stable-grace-s",
        _format_value(args.analyzer_gate_prefer_all_stable_grace_s),
        "--h2o-pressure-presample-policy",
        str(args.h2o_pressure_presample_policy),
        "--no-prompt",
    ]
    if not args.safe_stop_hgen_each_point:
        cmd.append("--keep-hgen-running-after-point")
    if row.get("reference_h2o_mmol") is not None:
        cmd.extend(["--certificate-h2o-mmol", _format_value(row["reference_h2o_mmol"])])
    if row.get("reference_dewpoint_c") is not None:
        cmd.extend(["--certificate-dewpoint-c", _format_value(row["reference_dewpoint_c"])])
    if row.get("certificate_uncertainty_mmol") is not None:
        cmd.extend(["--certificate-uncertainty-mmol", _format_value(row["certificate_uncertainty_mmol"])])
    if args.hgen_flow_lpm is not None:
        cmd.extend(["--hgen-flow-lpm", _format_value(args.hgen_flow_lpm)])
    cmd.append("--allow-ftd-write" if args.allow_ftd_write else "--no-ftd-write")
    if bool(getattr(args, "engineering_probe_only", False)):
        cmd.append("--engineering-probe-only")
    operator_confirmation = str(
        getattr(args, "operator_confirmation", "") or ""
    ).strip()
    if operator_confirmation:
        cmd.extend(["--operator-confirmation", operator_confirmation])
    if args.strict_humidity_reference_match:
        cmd.append("--strict-humidity-reference-match")
    if args.skip_humidity_generator_gate:
        cmd.append("--skip-humidity-generator-gate")
    if args.skip_dewpoint_gate:
        cmd.append("--skip-dewpoint-gate")
    return cmd


def _write_manifest_csv(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields: List[str] = []
    for row in rows:
        for key in row.keys():
            if key not in fields:
                fields.append(str(key))
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _write_queue_exclusion_evidence(
    queue_dir: Path,
    *,
    queue_summary: Mapping[str, Any],
    manifest_rows: Sequence[Mapping[str, Any]],
    reason: str,
    exclude_all_points: bool = False,
) -> None:
    """Record unsafe H2O queue rows as diagnostic-only evidence."""

    now = datetime.now().isoformat(timespec="seconds")
    exclusion_scope = (
        "queue_all_points" if exclude_all_points else "failed_or_aborted_points"
    )
    candidate_rows = [
        row
        for row in manifest_rows
        if (
            str(row.get("status") or "").lower() != "dry_run"
            and (
                exclude_all_points
                or str(row.get("status") or "").lower() != "ok"
            )
        )
    ]
    if not candidate_rows:
        candidate_rows = [
            {
                "point_run_id": "",
                "point_id": "",
                "temp_c": "",
                "hgen_temp_c": "",
                "hgen_rh_pct": "",
                "reference_dewpoint_c": "",
                "reference_h2o_mmol": "",
                "sample_role": "",
                "status": "aborted",
                "point_log": "",
            }
        ]

    rows: List[Dict[str, Any]] = []
    for row in candidate_rows:
        rows.append(
            {
                "created_at": now,
                "queue_run_id": queue_summary.get("queue_run_id", ""),
                "queue_dir": str(queue_dir),
                "point_run_id": row.get("point_run_id", ""),
                "point_id": row.get("point_id", ""),
                "temp_c": row.get("temp_c", ""),
                "hgen_temp_c": row.get("hgen_temp_c", ""),
                "hgen_rh_pct": row.get("hgen_rh_pct", ""),
                "reference_dewpoint_c": row.get("reference_dewpoint_c", ""),
                "reference_h2o_mmol": row.get("reference_h2o_mmol", ""),
                "sample_role": row.get("sample_role", ""),
                "source_status": row.get("status", ""),
                "point_log": row.get("point_log", ""),
                "exclude_from_fit": True,
                "exclude_from_acceptance": True,
                "exclude_from_senco_review": True,
                "exclusion_reason": reason,
                "exclusion_scope": exclusion_scope,
                "physical_meaning": (
                    (
                        "The queue-level humidity-generator final safe-stop failed, so no "
                        "sampled point in this queue retains a complete physical safety "
                        "closure. All queue points remain diagnostic evidence only."
                    )
                    if exclude_all_points
                    else (
                        "This H2O open-flow point did not complete under a continuous, stable "
                        "humidity-route sampling contract. Partial frames remain diagnostic "
                        "evidence only and must not enter H2O fitting, acceptance, or SENCO review."
                    )
                ),
            }
        )

    csv_path = queue_dir / "queue_abort_exclusion.csv"
    with csv_path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)

    (queue_dir / "queue_abort_exclusion.json").write_text(
        json.dumps(
            {
                "schema_version": "v1_5_h2o_queue_abort_exclusion_v1",
                "created_at": now,
                "queue_run_id": queue_summary.get("queue_run_id", ""),
                "reason": reason,
                "exclusion_scope": exclusion_scope,
                "exclude_from_fit": True,
                "exclude_from_acceptance": True,
                "exclude_from_senco_review": True,
                "rows": rows,
            },
            ensure_ascii=False,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )


def _write_queue_failure_audit(queue_dir: Path) -> Dict[str, Any]:
    """Write log-based H2O queue failure audit artifacts."""

    manifest_path = Path(queue_dir) / "queue_manifest.csv"
    output_dir = Path(queue_dir) / "queue_failure_audit"
    audit = _audit_h2o_queue_failures(manifest_path, output_dir)
    return {
        "status": "ok",
        "output_dir": str(output_dir),
        "total_points": audit.get("total_points"),
        "status_counts": dict(audit.get("status_counts") or {}),
        "failure_category_counts": dict(audit.get("failure_category_counts") or {}),
        "outputs": dict(audit.get("outputs") or {}),
    }


def _point_run_id(*, index: int, temp_c: float, hgen_temp_c: float, hgen_rh_pct: float) -> str:
    temp_token = _format_value(temp_c).replace("-", "m").replace(".", "p")
    hgen_token = _format_value(hgen_temp_c).replace("-", "m").replace(".", "p")
    rh_token = _format_value(hgen_rh_pct).replace("-", "m").replace(".", "p")
    return f"p{int(index):03d}_T{temp_token}_HG{hgen_token}C_{rh_token}RH_h2o"


def _temperature_settle_run_id(temp_c: float) -> str:
    temp_token = _format_value(temp_c).replace("-", "m").replace(".", "p")
    return f"T{temp_token}_temp_settle"


def _queue_output_dir(output_dir: Path, queue_run_id: str) -> Path:
    """Avoid duplicating run_id when a caller already passes a run-specific dir."""

    if output_dir.name == str(queue_run_id):
        return output_dir
    return output_dir / _queue_dir_name(output_dir, queue_run_id)


def _queue_dir_name(output_dir: Path, queue_run_id: str) -> str:
    """Keep queue evidence paths short enough for Windows while preserving traceability."""

    run_id = str(queue_run_id)
    candidate = output_dir / run_id
    if len(str(candidate)) <= 220 and len(run_id) <= 80:
        return run_id
    digest = hashlib.sha1(run_id.encode("utf-8")).hexdigest()[:10]
    return f"h2oq_{digest}"


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    authorization_errors = _engineering_probe_authorization_errors(
        args,
        dry_run=bool(args.dry_run),
    )
    if authorization_errors:
        _log(
            "Refusing V1.5 H2O queue engineering probe: "
            + ",".join(authorization_errors)
        )
        return 2

    cfg_path = str(Path(args.config).resolve())
    queue_path = Path(args.queue_csv).resolve()
    output_dir = Path(args.output_dir).resolve()
    queue_run_id = args.run_id or f"v1_5_h2o_open_flow_queue_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    queue_dir = _queue_output_dir(output_dir, queue_run_id)
    try:
        _claim_immutable_run_dir(queue_dir, run_id=queue_run_id)
    except FileExistsError as exc:
        _log(str(exc))
        return 2
    operator_confirmation_path: Optional[Path] = None
    if not args.dry_run:
        operator_confirmation_path = _write_operator_confirmation_record(
            queue_dir,
            run_id=queue_run_id,
            args=args,
            scope="v1_5_h2o_open_flow_queue_no_write_engineering_probe",
        )
    point_log_dir = queue_dir / "point_logs"
    point_log_dir.mkdir(parents=True, exist_ok=True)

    queue_rows = _load_queue_rows(queue_path)
    selected = _select_queue_rows(
        queue_rows,
        temps=_parse_float_filter(args.temps),
        max_points=args.max_points,
    )
    groups = _ordered_temperature_groups(selected, order=args.temperature_order)
    cfg = load_config(cfg_path)

    manifest_rows: List[Dict[str, Any]] = []
    queue_summary = {
        "schema_version": "v1_5_h2o_open_flow_queue_v0",
        "queue_run_id": queue_run_id,
        "queue_dir_name": queue_dir.name,
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "config_path": cfg_path,
        "queue_csv": str(queue_path),
        "output_dir": str(output_dir),
        "queue_dir": str(queue_dir),
        "selected_points": len(selected),
        "temperature_order": args.temperature_order,
        "control_temperature": bool(args.control_temperature),
        "dry_run": bool(args.dry_run),
        "no_write": not bool(args.allow_ftd_write),
        "no_write_scope": (
            "no_analyzer_persistent_configuration_no_senco_no_device_id_"
            "no_calibration_coefficient_write"
        ),
        "operator_confirmation_record": (
            str(operator_confirmation_path) if operator_confirmation_path else None
        ),
        "engineering_probe_only": bool(args.engineering_probe_only),
        "promotion_state": "blocked",
        "not_real_acceptance_evidence": True,
        "sealed_pressure_control": False,
        "writes_senco": False,
        "writes_device_id": False,
        "hgen_point_shutdown_policy": (
            "safe_stop_each_point"
            if args.safe_stop_hgen_each_point
            else "queue_managed_keep_running_between_points"
        ),
        "hgen_final_safe_stop_required": not bool(args.safe_stop_hgen_each_point),
        "physical_meaning": (
            "H2O standards are sampled in open flow after each temperature group is settled. "
            "The dewpoint meter is the humidity reference; pressure is recorded as an input "
            "quantity and legacy sealed pressure points are excluded from formal H2O fitting."
        ),
    }
    (queue_dir / "queue_summary.json").write_text(
        json.dumps(queue_summary, ensure_ascii=False, indent=2, default=str) + "\n",
        encoding="utf-8",
    )

    if not selected:
        _log("No H2O queue points selected.")
        _write_manifest_csv(queue_dir / "queue_manifest.csv", manifest_rows)
        return 1

    hard_failure = False
    queue_wide_exclusion = False
    hgen_final_safe_stop_ok: Optional[bool] = None
    # Keep this evidence run id short. Queue output directories can already be
    # deep on Windows, and final safe-stop must not be blocked by path length.
    hgen_final_safe_stop_run_id = "hgen_final_safe_stop"
    point_index = 0
    abort_reason = ""
    for temp_c, rows in groups:
        settle_run_id = _temperature_settle_run_id(float(temp_c))
        lead_row = rows[0] if rows else {}
        prewarm_run_id = f"{settle_run_id}_hgen_prewarm"
        if not args.dry_run:
            _log(
                "Humidity generator prewarm before chamber settle: "
                f"T={temp_c:g}C hgen={float(lead_row['hgen_temp_c']):g}C/"
                f"{float(lead_row['hgen_rh_pct']):g}%RH"
            )
            if not _prewarm_humidity_generator_for_group(
                cfg,
                temp_c=float(temp_c),
                lead_row=lead_row,
                output_dir=output_dir,
                run_id=prewarm_run_id,
            ):
                hard_failure = True
                abort_reason = "humidity_generator_prewarm_failed"
                _log(f"Temperature group {temp_c:g}C failed; humidity generator prewarm failed.")
                break
        if args.control_temperature:
            _log(f"Temperature group {temp_c:g}C: settle chamber before {len(rows)} H2O points")
            if args.dry_run:
                temp_ok = True
            else:
                temp_ok = _settle_temperature_group(
                    cfg,
                    temp_c=float(temp_c),
                    output_dir=output_dir,
                    run_id=settle_run_id,
                    args=args,
                )
            if not temp_ok:
                hard_failure = True
                abort_reason = "temperature_group_settle_failed"
                _log(f"Temperature group {temp_c:g}C failed; stop queue.")
                break

        for row in rows:
            point_index += 1
            purge_resolution = resolve_v1_5_open_flow_purge(
                component="h2o",
                row=row,
                explicit_purge_s=args.purge_s,
            )
            point_run_id = _point_run_id(
                index=point_index,
                temp_c=float(temp_c),
                hgen_temp_c=float(row["hgen_temp_c"]),
                hgen_rh_pct=float(row["hgen_rh_pct"]),
            )
            cmd = _build_point_command(
                config_path=cfg_path,
                output_dir=output_dir,
                row=row,
                run_id=point_run_id,
                args=args,
            )
            started = datetime.now().isoformat(timespec="seconds")
            point_log_path = point_log_dir / f"{point_run_id}.log"
            manifest_row: Dict[str, Any] = {
                "point_run_id": point_run_id,
                "point_id": row.get("point_id"),
                "temp_c": float(temp_c),
                "hgen_temp_c": row.get("hgen_temp_c"),
                "hgen_rh_pct": row.get("hgen_rh_pct"),
                "reference_dewpoint_c": row.get("reference_dewpoint_c"),
                "reference_h2o_mmol": row.get("reference_h2o_mmol"),
                "sample_role": row.get("sample_role"),
                "started_at": started,
                "ended_at": "",
                "returncode": "",
                "status": "dry_run" if args.dry_run else "running",
                "point_log": str(point_log_path),
                "command": " ".join(cmd),
                "resolved_purge_s": purge_resolution.purge_s,
                "minimum_purge_s": purge_resolution.minimum_purge_s,
                "purge_profile": purge_resolution.profile,
                "purge_explicit_override": purge_resolution.explicit_override,
                "purge_reasons": ";".join(purge_resolution.reasons),
            }
            manifest_rows.append(manifest_row)
            _write_manifest_csv(queue_dir / "queue_manifest.csv", manifest_rows)
            _log(
                "H2O point start: "
                f"T={temp_c:g}C hgen={float(row['hgen_temp_c']):g}C/{float(row['hgen_rh_pct']):g}%RH"
            )

            if args.dry_run:
                manifest_row["ended_at"] = datetime.now().isoformat(timespec="seconds")
                manifest_row["returncode"] = 0
                manifest_row["status"] = "dry_run"
                _write_manifest_csv(queue_dir / "queue_manifest.csv", manifest_rows)
                continue

            with point_log_path.open("w", encoding="utf-8", newline="") as log_handle:
                log_handle.write("COMMAND: " + " ".join(cmd) + "\n")
                log_handle.flush()
                try:
                    completed = subprocess.run(
                        cmd,
                        cwd=str(Path.cwd()),
                        stdout=log_handle,
                        stderr=subprocess.STDOUT,
                        text=True,
                        check=False,
                    )
                except KeyboardInterrupt:
                    manifest_row["ended_at"] = datetime.now().isoformat(timespec="seconds")
                    manifest_row["returncode"] = "interrupted"
                    manifest_row["status"] = "aborted"
                    _write_manifest_csv(queue_dir / "queue_manifest.csv", manifest_rows)
                    hard_failure = True
                    abort_reason = "operator_interrupted"
                    _log("H2O point interrupted by operator; point marked aborted.")
                    break
            if hard_failure and abort_reason == "operator_interrupted":
                break
            manifest_row["ended_at"] = datetime.now().isoformat(timespec="seconds")
            manifest_row["returncode"] = int(completed.returncode)
            manifest_row["status"] = "ok" if completed.returncode == 0 else "failed"
            if completed.returncode != 0:
                manifest_row.update(_classify_point_failure_from_log(point_log_path))
            _write_manifest_csv(queue_dir / "queue_manifest.csv", manifest_rows)

            if completed.returncode != 0:
                abort_reason = "h2o_point_failed"
                _log(
                    "H2O point failed: "
                    f"T={temp_c:g}C hgen={float(row['hgen_temp_c']):g}C/"
                    f"{float(row['hgen_rh_pct']):g}%RH rc={completed.returncode}; log={point_log_path}"
                )
                if args.stop_on_point_fail:
                    hard_failure = True
                    break
            else:
                _log(
                    "H2O point ok: "
                    f"T={temp_c:g}C hgen={float(row['hgen_temp_c']):g}C/{float(row['hgen_rh_pct']):g}%RH"
                )
            time.sleep(1.0)
        if hard_failure:
            break

    if not args.dry_run and not args.safe_stop_hgen_each_point:
        hgen_final_safe_stop_ok = _safe_stop_humidity_generator_after_queue(
            cfg,
            output_dir=output_dir,
            run_id=hgen_final_safe_stop_run_id,
        )
        if not hgen_final_safe_stop_ok:
            hard_failure = True
            queue_wide_exclusion = True
            abort_reason = "humidity_generator_final_safe_stop_failed"

    ok_count = sum(1 for row in manifest_rows if str(row.get("status")) == "ok")
    fail_count = sum(1 for row in manifest_rows if str(row.get("status")) == "failed")
    dry_count = sum(1 for row in manifest_rows if str(row.get("status")) == "dry_run")
    queue_summary.update(
        {
            "ended_at": datetime.now().isoformat(timespec="seconds"),
            "ok_points": ok_count,
            "failed_points": fail_count,
            "dry_run_points": dry_count,
            "hard_failure": hard_failure,
            "queue_wide_exclusion": queue_wide_exclusion,
            "hgen_final_safe_stop_ok": hgen_final_safe_stop_ok,
            "hgen_final_safe_stop_run_id": (
                hgen_final_safe_stop_run_id
                if hgen_final_safe_stop_ok is not None
                else None
            ),
        }
    )
    if hard_failure or fail_count:
        _write_queue_exclusion_evidence(
            queue_dir,
            queue_summary=queue_summary,
            manifest_rows=manifest_rows,
            reason=abort_reason or "h2o_queue_failed",
            exclude_all_points=queue_wide_exclusion,
        )
    try:
        queue_summary["failure_audit"] = _write_queue_failure_audit(queue_dir)
    except Exception as exc:  # pragma: no cover - audit must not mask route shutdown evidence.
        queue_summary["failure_audit"] = {
            "status": "error",
            "error": f"{type(exc).__name__}: {exc}",
        }
    (queue_dir / "queue_summary.json").write_text(
        json.dumps(queue_summary, ensure_ascii=False, indent=2, default=str) + "\n",
        encoding="utf-8",
    )
    _log(f"H2O queue summary: ok={ok_count} failed={fail_count} dry_run={dry_count} dir={queue_dir}")
    if hard_failure or fail_count:
        return 1
    if not args.dry_run and ok_count == 0:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
