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
import json
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence

from ..config import load_config
from ..logging_utils import RunLogger
from ..workflow.runner import CalibrationRunner
from .run_headless import _build_devices, _close_devices
from .run_v1_5_formal_h2o_open_flow_sampling import DEFAULT_H2O_OPEN_FLOW_PURGE_S
from .run_v1_5_formal_open_flow_sampling import (
    _apply_analyzer_acquisition_policy,
    _defer_startup_mode2_disabled_analyzers,
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
    ftd_group.add_argument("--allow-ftd-write", dest="allow_ftd_write", action="store_true", default=True)
    ftd_group.add_argument("--no-ftd-write", dest="allow_ftd_write", action="store_false")
    parser.add_argument("--min-valid-analyzers", type=int, default=1)
    parser.add_argument(
        "--h2o-pressure-presample-policy",
        choices=("warn", "fail", "skip"),
        default="warn",
        help="Keep H2O pressure instability as QC evidence by default instead of aborting the route.",
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

    _apply_analyzer_acquisition_policy(
        runtime_cfg,
        analyzer_acquisition=analyzer_acquisition,
        sensor_read_interval_s=sensor_read_interval_s,
        sample_interval_s=sample_interval_s,
        allow_ftd_write=allow_ftd_write,
    )

    temp_cfg = workflow_cfg.setdefault("stability", {}).setdefault("temperature", {})
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
    logger = RunLogger(output_dir, run_id=run_id, cfg=runtime_cfg)
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
    purge_s = float(args.purge_s if args.purge_s is not None else row.get("purge_s") or DEFAULT_H2O_OPEN_FLOW_PURGE_S)
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
        "--h2o-pressure-presample-policy",
        str(args.h2o_pressure_presample_policy),
        "--no-prompt",
    ]
    if row.get("reference_h2o_mmol") is not None:
        cmd.extend(["--certificate-h2o-mmol", _format_value(row["reference_h2o_mmol"])])
    if row.get("reference_dewpoint_c") is not None:
        cmd.extend(["--certificate-dewpoint-c", _format_value(row["reference_dewpoint_c"])])
    if row.get("certificate_uncertainty_mmol") is not None:
        cmd.extend(["--certificate-uncertainty-mmol", _format_value(row["certificate_uncertainty_mmol"])])
    if args.hgen_flow_lpm is not None:
        cmd.extend(["--hgen-flow-lpm", _format_value(args.hgen_flow_lpm)])
    if not args.allow_ftd_write:
        cmd.append("--no-ftd-write")
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


def _point_run_id(*, index: int, temp_c: float, hgen_temp_c: float, hgen_rh_pct: float) -> str:
    temp_token = _format_value(temp_c).replace("-", "m").replace(".", "p")
    hgen_token = _format_value(hgen_temp_c).replace("-", "m").replace(".", "p")
    rh_token = _format_value(hgen_rh_pct).replace("-", "m").replace(".", "p")
    return f"p{int(index):03d}_T{temp_token}_HG{hgen_token}C_{rh_token}RH_h2o"


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    if not args.no_prompt:
        _log("Refusing to run real H2O queue without --no-prompt.")
        return 2

    cfg_path = str(Path(args.config).resolve())
    queue_path = Path(args.queue_csv).resolve()
    output_dir = Path(args.output_dir).resolve()
    queue_run_id = args.run_id or f"v1_5_h2o_open_flow_queue_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    queue_dir = output_dir / queue_run_id
    queue_dir.mkdir(parents=True, exist_ok=True)
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
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "config_path": cfg_path,
        "queue_csv": str(queue_path),
        "output_dir": str(output_dir),
        "selected_points": len(selected),
        "temperature_order": args.temperature_order,
        "control_temperature": bool(args.control_temperature),
        "dry_run": bool(args.dry_run),
        "no_write": True,
        "sealed_pressure_control": False,
        "writes_senco": False,
        "writes_device_id": False,
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
    point_index = 0
    for temp_c, rows in groups:
        temp_token = _format_value(temp_c).replace("-", "m")
        settle_run_id = f"{queue_run_id}_T{temp_token}_temperature_settle"
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
                _log(f"Temperature group {temp_c:g}C failed; stop queue.")
                break

        for row in rows:
            point_index += 1
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
                completed = subprocess.run(
                    cmd,
                    cwd=str(Path.cwd()),
                    stdout=log_handle,
                    stderr=subprocess.STDOUT,
                    text=True,
                    check=False,
                )
            manifest_row["ended_at"] = datetime.now().isoformat(timespec="seconds")
            manifest_row["returncode"] = int(completed.returncode)
            manifest_row["status"] = "ok" if completed.returncode == 0 else "failed"
            _write_manifest_csv(queue_dir / "queue_manifest.csv", manifest_rows)

            if completed.returncode != 0:
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
        }
    )
    (queue_dir / "queue_summary.json").write_text(
        json.dumps(queue_summary, ensure_ascii=False, indent=2, default=str) + "\n",
        encoding="utf-8",
    )
    _log(f"H2O queue summary: ok={ok_count} failed={fail_count} dry_run={dry_count} dir={queue_dir}")
    if hard_failure:
        return 1
    if not args.dry_run and ok_count == 0:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
