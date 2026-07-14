"""Run a V1.5 formal CO2 open-flow queue across temperature groups.

This tool is intentionally a thin orchestrator around the already-proven
single-point open-flow sidecar. It controls the temperature chamber once per
temperature group, then runs all CO2 points at that temperature without sealed
pressure control and without any SENCO/ID writes.
"""

from __future__ import annotations

import argparse
import copy
import csv
import json
import subprocess
import sys
import time
from dataclasses import replace
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence

from ..config import load_config
from ..logging_utils import RunLogger
from ..validation.v1_5_co2_queue_failure_audit import (
    audit_and_write as _audit_co2_queue_failures,
    classify_point_failure_from_log as _classify_point_failure_from_log,
)
from ..validation.v1_5_open_flow_purge_contract import resolve_v1_5_open_flow_purge
from ..validation.v1_5_open_flow_purge_contract import CO2_NORMAL_PURGE_S
from ..workflow.runner import CalibrationRunner
from .run_headless import _build_devices, _close_devices
from .run_v1_5_formal_open_flow_sampling import (
    FORMAL_OPEN_FLOW_DEWPOINT_GATE_MAX_TOTAL_WAIT_S,
    FORMAL_OPEN_FLOW_ANALYZER_GATE_PREFER_ALL_STABLE_GRACE_S,
    _apply_analyzer_acquisition_policy,
    _defer_startup_mode2_disabled_analyzers,
    _formal_open_flow_dewpoint_gate_max_wait_s,
)


def _log(message: str) -> None:
    print(message, flush=True)


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run V1.5 no-write CO2 open-flow sampling queue across temperature groups."
    )
    parser.add_argument("--config", required=True, help="Runtime config JSON.")
    parser.add_argument("--queue-csv", required=True, help="Canonical co2_runner_queue.csv.")
    parser.add_argument("--output-dir", required=True, help="Output directory for queue evidence.")
    parser.add_argument("--run-id", default=None, help="Optional queue run id.")
    parser.add_argument(
        "--temps",
        default="all",
        help="Comma-separated temperatures to run, or all.",
    )
    parser.add_argument(
        "--temperature-order",
        choices=("asc", "desc", "queue"),
        default="desc",
        help="Temperature group order.",
    )
    parser.add_argument(
        "--roles",
        default="fit,verification",
        help="Comma-separated sample roles to run.",
    )
    parser.add_argument("--purge-s", type=float, default=None, help="Override purge seconds.")
    parser.add_argument(
        "--co2-adaptive-purge-after-first-point",
        action="store_true",
        help=(
            "For ordinary CO2 points after the first point in the same temperature group, "
            "shorten the fixed open-flow purge to --co2-subsequent-purge-s while keeping "
            "the dewpoint and ratio gates mandatory. Conservative/recovery points are not shortened."
        ),
    )
    parser.add_argument(
        "--co2-subsequent-purge-s",
        type=float,
        default=240.0,
        help=(
            "Fixed purge seconds for non-first ordinary CO2 points when "
            "--co2-adaptive-purge-after-first-point is enabled."
        ),
    )
    parser.add_argument(
        "--n2-prepurge-s",
        type=float,
        default=None,
        help=(
            "Optional nitrogen pre-purge seconds before the first CO2 dry anchor in each temperature group. "
            "When omitted the queue inherits workflow.nitrogen_purge.co2_prepurge_s "
            "from the runtime config; pass 0 to disable explicitly. This remains no-write."
        ),
    )
    parser.add_argument(
        "--n2-purge-source-valve",
        type=int,
        default=None,
        help=(
            "Optional logical nitrogen source valve. When omitted the sidecar reads "
            "valves.nitrogen_purge_source from the runtime config."
        ),
    )
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
    parser.add_argument(
        "--min-valid-analyzers",
        type=int,
        default=None,
        help=(
            "Minimum analyzers that must pass the point-level ratio gate. "
            "When omitted, the formal queue uses per-analyzer evidence mode with "
            "min_valid=1 so one invalid analyzer cannot block valid analyzers."
        ),
    )
    parser.add_argument(
        "--analyzer-gate-required-labels",
        default="",
        help="Comma-separated analyzer labels that must pass the CO2 ratio gate before each point.",
    )
    parser.add_argument(
        "--analyzer-gate-prefer-all-stable-grace-s",
        type=float,
        default=FORMAL_OPEN_FLOW_ANALYZER_GATE_PREFER_ALL_STABLE_GRACE_S,
        help=(
            "After min-valid analyzers become stable, wait this many seconds for the remaining "
            "analyzers to become stable before accepting the point with independent grading."
        ),
    )
    parser.add_argument("--co2-ratio-f-preseal-tol", type=float, default=None)
    parser.add_argument("--co2-ratio-f-preseal-window-s", type=float, default=None)
    parser.add_argument("--co2-ratio-f-preseal-timeout-s", type=float, default=None)
    parser.add_argument("--co2-ratio-f-preseal-min-samples", type=int, default=None)
    parser.add_argument(
        "--co2-ratio-f-preseal-policy",
        choices=("reject", "warn", "pass"),
        default=None,
        help="Forwarded to the single-point CO2 sidecar; default formal behavior is reject.",
    )
    dewpoint_gate = parser.add_mutually_exclusive_group()
    dewpoint_gate.add_argument(
        "--gas-route-dewpoint-gate-enabled",
        dest="gas_route_dewpoint_gate_enabled",
        action="store_true",
        default=True,
        help="Require dry/stable open-flow dewpoint evidence before each formal CO2 sample window.",
    )
    dewpoint_gate.add_argument(
        "--no-gas-route-dewpoint-gate",
        dest="gas_route_dewpoint_gate_enabled",
        action="store_false",
        help="Disable the formal CO2 route dewpoint gate for engineering recovery only.",
    )
    parser.add_argument(
        "--gas-route-dewpoint-gate-policy",
        choices=("reject", "warn", "pass"),
        default="reject",
        help=(
            "Policy when the CO2 route dewpoint gate has stable evidence but the absolute dry-enough "
            "threshold is not met. Formal CO2 calibration defaults to reject so insufficiently dry "
            "route evidence cannot enter the main coefficient fit."
        ),
    )
    dry_gate = parser.add_mutually_exclusive_group()
    dry_gate.add_argument(
        "--gas-route-dewpoint-require-dry-enough",
        dest="gas_route_dewpoint_require_dry_enough",
        action="store_true",
        default=True,
        help="Require the route dewpoint to be below --gas-route-dewpoint-dry-enough-c.",
    )
    dry_gate.add_argument(
        "--no-gas-route-dewpoint-require-dry-enough",
        dest="gas_route_dewpoint_require_dry_enough",
        action="store_false",
        help="Use dewpoint tail stability only; records the disabled dry-enough gate in evidence.",
    )
    parser.add_argument("--gas-route-dewpoint-dry-enough-c", type=float, default=-28.0)
    parser.add_argument(
        "--gas-route-dewpoint-gate-max-total-wait-s",
        type=float,
        default=FORMAL_OPEN_FLOW_DEWPOINT_GATE_MAX_TOTAL_WAIT_S,
        help=(
            "Maximum normal CO2 open-flow dewpoint wait before the point is downgraded/rejected. "
            "Formal automation caps this at 1800 s so an abnormal route does not consume gas for an hour."
        ),
    )
    parser.add_argument("--gas-route-dewpoint-gate-window-s", type=float, default=60.0)
    parser.add_argument("--gas-route-dewpoint-gate-tail-span-max-c", type=float, default=0.45)
    parser.add_argument(
        "--gas-route-dewpoint-gate-tail-slope-abs-max-c-per-s",
        type=float,
        default=0.005,
    )
    parser.add_argument(
        "--gas-route-dewpoint-gate-deep-dry-tail-relax-margin-c",
        type=float,
        default=4.0,
        help=(
            "CO2 dry-route only: once the whole dewpoint tail is this many degrees below "
            "the dry-enough threshold and still falling, do not keep waiting only for tail slope/span."
        ),
    )
    parser.add_argument(
        "--control-temperature",
        dest="control_temperature",
        action="store_true",
        default=True,
        help="Control and wait for the temperature chamber once per temperature group.",
    )
    parser.add_argument(
        "--no-control-temperature",
        dest="control_temperature",
        action="store_false",
        help="Do not control the temperature chamber; only run the queue metadata temperatures.",
    )
    parser.add_argument("--temperature-soak-after-reach-s", type=float, default=None)
    parser.add_argument("--temperature-tol-c", type=float, default=None)
    parser.add_argument("--temperature-timeout-s", type=float, default=None)
    parser.add_argument("--temperature-hard-max-wait-s", type=float, default=None)
    parser.add_argument("--temperature-analyzer-span-c", type=float, default=None)
    parser.add_argument("--temperature-analyzer-window-s", type=float, default=None)
    parser.add_argument("--temperature-analyzer-timeout-s", type=float, default=None)
    parser.add_argument("--max-points", type=int, default=None)
    parser.add_argument("--skip-stability-gate", action="store_true")
    parser.add_argument(
        "--stop-on-point-fail",
        dest="stop_on_point_fail",
        action="store_true",
        default=True,
        help=(
            "Stop the formal queue when a point sidecar fails a shared physical gate. "
            "Per-analyzer quality grading remains independent through min-valid-analyzers."
        ),
    )
    parser.add_argument(
        "--continue-on-point-fail",
        dest="stop_on_point_fail",
        action="store_false",
        help="Diagnostic-only override that continues after a failed point sidecar.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Write the queue manifest and commands without opening any COM port.",
    )
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
        if not item:
            continue
        out.add(float(item))
    return out


def _parse_text_filter(text: str) -> set[str]:
    return {item.strip().lower() for item in str(text or "").split(",") if item.strip()}


def _load_queue_rows(path: str | Path) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    with Path(path).open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            if str(row.get("component") or "").strip().lower() != "co2":
                continue
            temp = _safe_float(row.get("temp_c"))
            ppm = _safe_float(row.get("source_nominal_ppm"))
            if temp is None or ppm is None:
                continue
            rows.append(
                {
                    **row,
                    "temp_c": float(temp),
                    "source_nominal_ppm": float(ppm),
                    "co2_group": str(row.get("co2_group") or "A").strip().upper() or "A",
                    "sample_role": str(row.get("sample_role") or "").strip().lower(),
                    "purge_s": _safe_float(row.get("purge_s")),
                    "sample_count": int(_safe_float(row.get("sample_count")) or 10),
                    "analyzer_acquisition": str(row.get("analyzer_acquisition") or "").strip(),
                }
            )
    return rows


def _configured_enabled_analyzer_count(cfg: Mapping[str, Any]) -> int:
    devices_cfg = cfg.get("devices", {}) if isinstance(cfg.get("devices"), Mapping) else {}
    analyzers = devices_cfg.get("gas_analyzers")
    if isinstance(analyzers, Sequence) and not isinstance(analyzers, (str, bytes)):
        count = 0
        for item in analyzers:
            if not isinstance(item, Mapping):
                continue
            if item.get("enabled") is False:
                continue
            count += 1
        return count
    gas_analyzer = devices_cfg.get("gas_analyzer")
    if isinstance(gas_analyzer, Mapping):
        return 0 if gas_analyzer.get("enabled") is False else 1
    return 1


def _resolve_formal_min_valid_analyzers(
    cfg: Mapping[str, Any],
    explicit_min_valid: Optional[int],
) -> int:
    configured_count = max(1, _configured_enabled_analyzer_count(cfg))
    if explicit_min_valid is None:
        return 1
    return min(max(1, int(explicit_min_valid)), configured_count)


def _resolve_n2_prepurge_s(cfg: Mapping[str, Any], explicit_seconds: Optional[float]) -> float:
    """Resolve optional N2 pre-purge without inheriting it into formal CO2.

    Formal gas-route calibration now relies on the certified gas point itself
    reaching a dry-enough dewpoint and stable ratio. N2 pre-purge remains
    available only when explicitly requested as engineering conditioning.
    """

    if explicit_seconds is not None:
        return max(0.0, float(explicit_seconds))
    _ = cfg
    return 0.0


def _select_queue_rows(
    rows: Sequence[Mapping[str, Any]],
    *,
    temps: Optional[set[float]],
    roles: set[str],
    max_points: Optional[int],
) -> List[Dict[str, Any]]:
    selected: List[Dict[str, Any]] = []
    for row in rows:
        temp = float(row["temp_c"])
        role = str(row.get("sample_role") or "").lower()
        if temps is not None and not any(abs(temp - item) < 1e-9 for item in temps):
            continue
        if roles and role not in roles:
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
    return [
        (
            temp,
            sorted(groups[temp], key=lambda item: float(item["source_nominal_ppm"])),
        )
        for temp in temps
    ]


def _temperature_group_n2_prepurge_index(rows: Sequence[Mapping[str, Any]]) -> Optional[int]:
    """Return the row index that should receive the temperature-group N2 pre-purge.

    N2 pre-purge is a route-conditioning action. It should remove humidity-route
    residue and dead-volume gas before the dry CO2 evidence for a temperature
    group begins, not interrupt every certified CO2 point. Prefer the 0 ppm dry
    anchor when present; otherwise use the first point in the temperature group.
    """

    if not rows:
        return None
    for index, row in enumerate(rows):
        ppm = _safe_float(row.get("source_nominal_ppm"))
        if ppm is not None and abs(float(ppm)) <= 1e-9:
            return index
    return 0


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
    runtime_cfg.setdefault("metadata", {})["v1_5_co2_queue_temperature_settle"] = True
    runtime_cfg["metadata"]["writes_senco"] = False
    runtime_cfg["metadata"]["writes_device_id"] = False
    runtime_cfg.setdefault("workflow", {})["collect_only"] = True
    runtime_cfg["workflow"]["skip_h2o"] = True
    runtime_cfg["workflow"]["route_mode"] = "co2_open_flow_temperature_settle"

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

    temp_cfg = runtime_cfg["workflow"].setdefault("stability", {}).setdefault("temperature", {})
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
    if analyzer_span_c is not None:
        temp_cfg["analyzer_chamber_temp_span_c"] = float(analyzer_span_c)
    else:
        temp_cfg["analyzer_chamber_temp_span_c"] = max(
            float(temp_cfg.get("analyzer_chamber_temp_span_c", 0.08) or 0.08),
            0.08,
        )
    if analyzer_window_s is not None:
        temp_cfg["analyzer_chamber_temp_window_s"] = float(analyzer_window_s)
    else:
        temp_cfg["analyzer_chamber_temp_window_s"] = max(
            float(temp_cfg.get("analyzer_chamber_temp_window_s", 60.0) or 60.0),
            60.0,
        )
    if analyzer_timeout_s is not None:
        temp_cfg["analyzer_chamber_temp_timeout_s"] = float(analyzer_timeout_s)
    else:
        temp_cfg["analyzer_chamber_temp_timeout_s"] = max(
            float(temp_cfg.get("analyzer_chamber_temp_timeout_s", 5400.0) or 5400.0),
            5400.0,
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
    output_dir.mkdir(parents=True, exist_ok=True)
    logger = RunLogger(output_dir, run_id=run_id, cfg=runtime_cfg)
    snapshot_path = logger.run_dir / "temperature_settle_runtime_config_snapshot.json"
    snapshot_path.write_text(
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
                    "schema_version": "v1_5_co2_queue_temperature_settle_v0",
                    "run_id": run_id,
                    "temp_c": float(temp_c),
                    "ok": ok,
                    "physical_meaning": (
                        "The chamber and analyzer bodies are settled before opening any CO2 source "
                        "at this temperature group. This avoids treating gas-transition drift as "
                        "temperature compensation evidence."
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
                        "schema_version": "v1_5_co2_queue_temperature_settle_v0",
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
        _close_devices(devices)
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
    n2_prepurge_s_for_point: Optional[float] = None,
    row_index_in_temperature_group: int = 0,
) -> List[str]:
    purge_resolution = _resolve_co2_point_purge(
        row=row,
        args=args,
        row_index_in_temperature_group=row_index_in_temperature_group,
    )
    purge_s = purge_resolution.purge_s
    sample_count = int(args.sample_count if args.sample_count is not None else row.get("sample_count") or 10)
    acquisition = str(row.get("analyzer_acquisition") or args.analyzer_acquisition or "active_stream_1hz")
    min_valid_analyzers = 1 if args.min_valid_analyzers is None else int(args.min_valid_analyzers)
    cmd = [
        sys.executable,
        "-m",
        "gas_calibrator.tools.run_v1_5_formal_open_flow_sampling",
        "--config",
        str(config_path),
        "--output-dir",
        str(output_dir.resolve()),
        "--run-id",
        run_id,
        "--temp",
        _format_value(row["temp_c"]),
        "--co2-source-ppm",
        _format_value(row["source_nominal_ppm"]),
        "--co2-group",
        str(row.get("co2_group") or "A"),
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
        str(min_valid_analyzers),
        "--no-prompt",
    ]
    if not args.allow_ftd_write:
        cmd.append("--no-ftd-write")
    point_n2_prepurge_source = (
        args.n2_prepurge_s if n2_prepurge_s_for_point is None else n2_prepurge_s_for_point
    )
    point_n2_prepurge_s = float(point_n2_prepurge_source or 0.0)
    if point_n2_prepurge_s > 0.0:
        cmd.extend(["--n2-prepurge-s", _format_value(point_n2_prepurge_s)])
        if args.n2_purge_source_valve is not None:
            cmd.extend(["--n2-purge-source-valve", str(int(args.n2_purge_source_valve))])
    if str(args.analyzer_gate_required_labels or "").strip():
        cmd.extend(["--analyzer-gate-required-labels", str(args.analyzer_gate_required_labels).strip()])
    cmd.extend(
        [
            "--analyzer-gate-prefer-all-stable-grace-s",
            _format_value(args.analyzer_gate_prefer_all_stable_grace_s),
        ]
    )
    if args.co2_ratio_f_preseal_tol is not None:
        cmd.extend(["--co2-ratio-f-preseal-tol", _format_value(args.co2_ratio_f_preseal_tol)])
    if args.co2_ratio_f_preseal_window_s is not None:
        cmd.extend(["--co2-ratio-f-preseal-window-s", _format_value(args.co2_ratio_f_preseal_window_s)])
    if args.co2_ratio_f_preseal_timeout_s is not None:
        cmd.extend(["--co2-ratio-f-preseal-timeout-s", _format_value(args.co2_ratio_f_preseal_timeout_s)])
    if args.co2_ratio_f_preseal_min_samples is not None:
        cmd.extend(["--co2-ratio-f-preseal-min-samples", str(int(args.co2_ratio_f_preseal_min_samples))])
    if args.co2_ratio_f_preseal_policy is not None:
        cmd.extend(["--co2-ratio-f-preseal-policy", str(args.co2_ratio_f_preseal_policy)])
    if bool(args.gas_route_dewpoint_gate_enabled):
        cmd.append("--gas-route-dewpoint-gate-enabled")
    else:
        cmd.append("--no-gas-route-dewpoint-gate")
    cmd.extend(["--gas-route-dewpoint-gate-policy", str(args.gas_route_dewpoint_gate_policy)])
    if bool(args.gas_route_dewpoint_require_dry_enough):
        cmd.append("--gas-route-dewpoint-require-dry-enough")
    else:
        cmd.append("--no-gas-route-dewpoint-require-dry-enough")
    cmd.extend(["--gas-route-dewpoint-dry-enough-c", _format_value(args.gas_route_dewpoint_dry_enough_c)])
    cmd.extend(
        [
            "--gas-route-dewpoint-gate-max-total-wait-s",
            _format_value(args.gas_route_dewpoint_gate_max_total_wait_s),
        ]
    )
    cmd.extend(["--gas-route-dewpoint-gate-window-s", _format_value(args.gas_route_dewpoint_gate_window_s)])
    cmd.extend(
        [
            "--gas-route-dewpoint-gate-tail-span-max-c",
            _format_value(args.gas_route_dewpoint_gate_tail_span_max_c),
        ]
    )
    cmd.extend(
        [
            "--gas-route-dewpoint-gate-tail-slope-abs-max-c-per-s",
            _format_value(args.gas_route_dewpoint_gate_tail_slope_abs_max_c_per_s),
        ]
    )
    cmd.extend(
        [
            "--gas-route-dewpoint-gate-deep-dry-tail-relax-margin-c",
            _format_value(args.gas_route_dewpoint_gate_deep_dry_tail_relax_margin_c),
        ]
    )
    certificate_co2_ppm = _safe_float(
        row.get("certificate_co2_ppm")
        or row.get("standard_gas_certificate_value_ppm")
        or row.get("certificate_value_ppm")
    )
    if certificate_co2_ppm is not None:
        cmd.extend(["--certificate-co2-ppm", _format_value(certificate_co2_ppm)])
    certificate_uncertainty_ppm = _safe_float(
        row.get("certificate_uncertainty_ppm")
        or row.get("standard_gas_certificate_uncertainty_ppm")
        or row.get("certificate_uncertainty_abs_ppm")
    )
    if certificate_uncertainty_ppm is not None:
        cmd.extend(["--certificate-uncertainty-ppm", _format_value(certificate_uncertainty_ppm)])
    if args.skip_stability_gate:
        cmd.append("--skip-stability-gate")
    return cmd


def _resolve_co2_point_purge(
    *,
    row: Mapping[str, Any],
    args: argparse.Namespace,
    row_index_in_temperature_group: int,
):
    base_resolution = resolve_v1_5_open_flow_purge(
        component="co2",
        row=row,
        explicit_purge_s=args.purge_s,
    )
    if not bool(getattr(args, "co2_adaptive_purge_after_first_point", False)):
        return base_resolution
    if int(row_index_in_temperature_group) <= 0:
        return base_resolution

    subsequent_purge_s = _safe_float(getattr(args, "co2_subsequent_purge_s", None))
    if subsequent_purge_s is None or subsequent_purge_s <= 0.0:
        return base_resolution

    # Only shorten ordinary, already-known CO2 route points. If the row asks for
    # conservative/recovery handling, keep that longer physical conditioning time.
    if (
        float(base_resolution.purge_s) > CO2_NORMAL_PURGE_S
        or float(base_resolution.minimum_purge_s) > CO2_NORMAL_PURGE_S
    ):
        return base_resolution

    shortened = min(float(subsequent_purge_s), CO2_NORMAL_PURGE_S)
    return replace(
        base_resolution,
        purge_s=shortened,
        minimum_purge_s=shortened,
        profile="adaptive_after_first_point",
        explicit_override=True,
        reasons=tuple(base_resolution.reasons)
        + (
            "same_temperature_group_after_first_point",
            "dewpoint_and_ratio_gates_remain_mandatory",
        ),
    )


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


def _write_queue_failure_audit(queue_dir: Path) -> Dict[str, Any]:
    """Write an offline queue failure audit from the current manifest.

    This is deliberately post-run and file-only. It must never reopen COM ports
    or influence whether the already-finished queue is considered complete.
    """

    manifest_path = Path(queue_dir) / "queue_manifest.csv"
    output_dir = Path(queue_dir) / "queue_failure_audit"
    audit = _audit_co2_queue_failures(manifest_path, output_dir)
    return {
        "status": "ok",
        "output_dir": str(output_dir),
        "total_points": audit.get("total_points"),
        "status_counts": dict(audit.get("status_counts") or {}),
        "failure_category_counts": dict(audit.get("failure_category_counts") or {}),
        "outputs": dict(audit.get("outputs") or {}),
    }


def _point_run_id(*, index: int, temp_c: float, ppm: float, role: Any) -> str:
    temp_token = _format_value(temp_c).replace("-", "m").replace(".", "p")
    ppm_token = _format_value(ppm).replace("-", "m").replace(".", "p")
    role_token = str(role or "sample").strip().lower() or "sample"
    return f"p{int(index):03d}_T{temp_token}_{ppm_token}ppm_{role_token}"


def _temperature_settle_run_id(temp_c: float) -> str:
    temp_token = _format_value(temp_c).replace("-", "m").replace(".", "p")
    return f"T{temp_token}_temp_settle"


def _temperature_settle_failure_manifest_row(
    *,
    temp_c: float,
    settle_run_id: str,
    output_dir: Path,
    row_count: int,
) -> Dict[str, Any]:
    return {
        "point_run_id": settle_run_id,
        "temp_c": float(temp_c),
        "source_nominal_ppm": "",
        "co2_group": "temperature_settle",
        "sample_role": "temperature_settle",
        "started_at": "",
        "ended_at": datetime.now().isoformat(timespec="seconds"),
        "returncode": "",
        "status": "failed",
        "point_log": "",
        "command": "",
        "failure_category": "temperature_settle_failed",
        "failure_reason": f"Temperature group {temp_c:g}C failed before {int(row_count)} CO2 point(s).",
        "temperature_settle_run_id": settle_run_id,
        "temperature_settle_output_dir": str(Path(output_dir) / settle_run_id),
    }


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    args.gas_route_dewpoint_gate_max_total_wait_s = _formal_open_flow_dewpoint_gate_max_wait_s(
        args.gas_route_dewpoint_gate_max_total_wait_s
    )
    if not args.no_prompt:
        _log("Refusing to run real CO2 queue without --no-prompt.")
        return 2

    cfg_path = str(Path(args.config).resolve())
    queue_path = Path(args.queue_csv).resolve()
    output_dir = Path(args.output_dir).resolve()
    queue_run_id = args.run_id or f"v1_5_co2_open_flow_queue_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    queue_dir = output_dir / queue_run_id
    queue_dir.mkdir(parents=True, exist_ok=True)
    point_log_dir = queue_dir / "point_logs"
    point_log_dir.mkdir(parents=True, exist_ok=True)

    queue_rows = _load_queue_rows(queue_path)
    selected = _select_queue_rows(
        queue_rows,
        temps=_parse_float_filter(args.temps),
        roles=_parse_text_filter(args.roles),
        max_points=args.max_points,
    )
    groups = _ordered_temperature_groups(selected, order=args.temperature_order)
    cfg = load_config(cfg_path)
    args.min_valid_analyzers = _resolve_formal_min_valid_analyzers(cfg, args.min_valid_analyzers)
    args.n2_prepurge_s = _resolve_n2_prepurge_s(cfg, args.n2_prepurge_s)

    manifest_rows: List[Dict[str, Any]] = []
    n2_policy = (
        "explicit_engineering_conditioning_once_per_temperature_group"
        if float(args.n2_prepurge_s or 0.0) > 0.0
        else "disabled_by_default_certified_gas_dewpoint_ratio_gate_only"
    )
    queue_summary = {
        "schema_version": "v1_5_co2_open_flow_queue_v0",
        "queue_run_id": queue_run_id,
        "created_at": datetime.now().isoformat(timespec="seconds"),
        "config_path": cfg_path,
        "queue_csv": str(queue_path),
        "output_dir": str(output_dir),
        "selected_points": len(selected),
        "temperature_order": args.temperature_order,
        "roles": sorted(_parse_text_filter(args.roles)),
        "control_temperature": bool(args.control_temperature),
        "dry_run": bool(args.dry_run),
        "no_write": True,
        "sealed_pressure_control": False,
        "writes_senco": False,
        "writes_device_id": False,
        "min_valid_analyzers": int(args.min_valid_analyzers),
        "gas_route_dewpoint_gate_enabled": bool(args.gas_route_dewpoint_gate_enabled),
        "gas_route_dewpoint_gate_policy": args.gas_route_dewpoint_gate_policy,
        "gas_route_dewpoint_require_dry_enough": bool(args.gas_route_dewpoint_require_dry_enough),
        "gas_route_dewpoint_dry_enough_c": float(args.gas_route_dewpoint_dry_enough_c),
        "gas_route_dewpoint_gate_max_total_wait_s": float(args.gas_route_dewpoint_gate_max_total_wait_s),
        "gas_route_dewpoint_gate_deep_dry_tail_relax_margin_c": float(
            args.gas_route_dewpoint_gate_deep_dry_tail_relax_margin_c
        ),
        "co2_adaptive_purge_after_first_point": bool(args.co2_adaptive_purge_after_first_point),
        "co2_subsequent_purge_s": float(args.co2_subsequent_purge_s),
        "n2_prepurge_s": max(0.0, float(args.n2_prepurge_s or 0.0)),
        "n2_prepurge_policy": n2_policy,
        "n2_purge_source_valve": (
            None if args.n2_purge_source_valve is None else int(args.n2_purge_source_valve)
        ),
        "physical_meaning": (
            "CO2 standards are sampled in open flow after each temperature group is settled. "
            "Pressure is recorded as an input quantity and sealed pressure points are excluded "
            "from formal CO2 fitting. Nitrogen pre-purge is disabled by default; every certified "
            "gas point must prove its own dry-enough dewpoint state and per-analyzer ratio stability "
            "before sampling. Explicit N2 pre-purge is engineering conditioning evidence only. "
            "Analyzer ratio gates are per-device evidence gates by default: an invalid analyzer "
            "is downgraded in QC instead of blocking valid analyzers in the same gas state."
        ),
    }
    (queue_dir / "queue_summary.json").write_text(
        json.dumps(queue_summary, ensure_ascii=False, indent=2, default=str) + "\n",
        encoding="utf-8",
    )

    if not selected:
        _log("No CO2 queue points selected.")
        _write_manifest_csv(queue_dir / "queue_manifest.csv", manifest_rows)
        return 1

    hard_failure = False
    point_index = 0
    for temp_c, rows in groups:
        settle_run_id = _temperature_settle_run_id(float(temp_c))
        if args.control_temperature:
            _log(f"Temperature group {temp_c:g}C: settle chamber before {len(rows)} CO2 points")
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
                manifest_rows.append(
                    _temperature_settle_failure_manifest_row(
                        temp_c=float(temp_c),
                        settle_run_id=settle_run_id,
                        output_dir=output_dir,
                        row_count=len(rows),
                    )
                )
                _write_manifest_csv(queue_dir / "queue_manifest.csv", manifest_rows)
                hard_failure = True
                _log(f"Temperature group {temp_c:g}C failed; stop queue.")
                break

        n2_prepurge_index = _temperature_group_n2_prepurge_index(rows)
        for row_index, row in enumerate(rows):
            point_index += 1
            ppm = float(row["source_nominal_ppm"])
            point_n2_prepurge_s = (
                max(0.0, float(args.n2_prepurge_s or 0.0))
                if n2_prepurge_index is not None and row_index == n2_prepurge_index
                else 0.0
            )
            purge_resolution = _resolve_co2_point_purge(
                row=row,
                args=args,
                row_index_in_temperature_group=row_index,
            )
            point_run_id = _point_run_id(
                index=point_index,
                temp_c=float(temp_c),
                ppm=ppm,
                role=row.get("sample_role"),
            )
            cmd = _build_point_command(
                config_path=cfg_path,
                output_dir=output_dir,
                row=row,
                run_id=point_run_id,
                args=args,
                n2_prepurge_s_for_point=point_n2_prepurge_s,
                row_index_in_temperature_group=row_index,
            )
            started = datetime.now().isoformat(timespec="seconds")
            point_log_path = point_log_dir / f"{point_run_id}.log"
            manifest_row: Dict[str, Any] = {
                "point_run_id": point_run_id,
                "temp_c": float(temp_c),
                "source_nominal_ppm": ppm,
                "co2_group": row.get("co2_group"),
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
                "n2_prepurge_s": point_n2_prepurge_s,
                "n2_prepurge_policy": n2_policy,
                "n2_purge_source_valve": (
                    None
                    if args.n2_purge_source_valve is None
                    else int(args.n2_purge_source_valve)
                ),
                "min_valid_analyzers": int(args.min_valid_analyzers),
                "gas_route_dewpoint_gate_enabled": bool(args.gas_route_dewpoint_gate_enabled),
                "gas_route_dewpoint_dry_enough_c": float(args.gas_route_dewpoint_dry_enough_c),
                "gas_route_dewpoint_gate_deep_dry_tail_relax_margin_c": float(
                    args.gas_route_dewpoint_gate_deep_dry_tail_relax_margin_c
                ),
                "failure_category": "",
                "failure_reason": "",
            }
            manifest_rows.append(manifest_row)
            _write_manifest_csv(queue_dir / "queue_manifest.csv", manifest_rows)
            _log(f"CO2 point start: T={temp_c:g}C source={ppm:g}ppm role={row.get('sample_role')}")

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
            if completed.returncode != 0:
                manifest_row.update(_classify_point_failure_from_log(point_log_path))
            _write_manifest_csv(queue_dir / "queue_manifest.csv", manifest_rows)

            if completed.returncode != 0:
                _log(
                    f"CO2 point failed: T={temp_c:g}C source={ppm:g}ppm "
                    f"rc={completed.returncode}; log={point_log_path}"
                )
                if args.stop_on_point_fail:
                    hard_failure = True
                    break
            else:
                _log(f"CO2 point ok: T={temp_c:g}C source={ppm:g}ppm")
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
    try:
        queue_summary["failure_audit"] = _write_queue_failure_audit(queue_dir)
    except Exception as exc:
        queue_summary["failure_audit"] = {
            "status": "error",
            "error": f"{type(exc).__name__}: {exc}",
        }
    (queue_dir / "queue_summary.json").write_text(
        json.dumps(queue_summary, ensure_ascii=False, indent=2, default=str) + "\n",
        encoding="utf-8",
    )
    _log(f"Queue summary: ok={ok_count} failed={fail_count} dry_run={dry_count} dir={queue_dir}")
    if hard_failure:
        return 1
    if not args.dry_run and ok_count == 0:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
