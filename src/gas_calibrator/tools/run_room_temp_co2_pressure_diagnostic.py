"""Independent metrology-grade seal/pressure qualification diagnostic V2 CLI."""

from __future__ import annotations

import argparse
import copy
import csv
import json
import math
import sys
import threading
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from ..config import load_config
from ..data.points import CalibrationPoint
from ..devices import DewpointMeter, GasAnalyzer, Pace5000, ParoscientificGauge, RelayController
from ..logging_utils import RunLogger
from ..validation.room_temp_co2_pressure_diagnostic import (
    DEFAULT_ANALYZER_ID,
    DEFAULT_DEWPOINT_REBOUND_MIN_RISE_C,
    DEFAULT_DEWPOINT_REBOUND_WINDOW_S,
    DEFAULT_FULL_GAS_POINTS_PPM,
    DEFAULT_FULL_PRESSURE_POINTS_HPA,
    DEFAULT_GAS_SWITCH_DEADTIME_S,
    DEFAULT_LAYER_SEQUENCE,
    DEFAULT_MAX_FLUSH_S,
    DEFAULT_MIN_FLUSH_S,
    DEFAULT_PRECONDITION_GAS_PPM,
    DEFAULT_PRECONDITION_MAX_FLUSH_S,
    DEFAULT_PRECONDITION_MIN_FLUSH_S,
    DEFAULT_PRECONDITION_WINDOW_S,
    DEFAULT_PRESSURE_SETTLE_TIMEOUT_S,
    DEFAULT_RESTORE_VENT_OBSERVE_S,
    DEFAULT_SAMPLE_POLL_S,
    DEFAULT_SCREENING_FLUSH_S,
    DEFAULT_SCREENING_GAS_POINTS_PPM,
    DEFAULT_SCREENING_PRESSURE_POINTS_HPA,
    DEFAULT_SEALED_HOLD_S,
    DEFAULT_STABLE_WINDOW_S,
    DEFAULT_THRESHOLDS,
    DEFAULT_VARIANTS,
    analyze_room_temp_diagnostic,
    build_analyzer_chain_isolation_comparison,
    build_analyzer_chain_compare_vs_baseline,
    build_analyzer_chain_compare_vs_8ch,
    build_analyzer_chain_pace_contribution_comparison,
    build_analyzer_chain_isolation_summary,
    build_aligned_rows,
    build_flush_summary,
    build_phase_gate_row,
    build_pressure_point_summary,
    build_seal_hold_summary,
    evaluate_flush_gate,
    export_analyzer_chain_isolation_results,
    export_room_temp_diagnostic_results,
)
from ..workflow.runner import CalibrationRunner
from .safe_stop import perform_safe_stop_with_retries
from .run_gas_route_ratio_leak_check import (
    _close_devices,
    _configure_analyzer_stream,
    _normalize_group,
    _select_analyzer_cfg,
)


@dataclass(frozen=True)
class VariantSpec:
    name: str
    description: str
    seal_trigger_hpa: Optional[float]
    ramp_down_rate_hpa_per_s: Optional[float]
    stable_gate_mode: str
    comparison_factor: str


def _log(message: str) -> None:
    print(message, flush=True)


def _safe_float(value: Any) -> Optional[float]:
    try:
        if value in (None, ""):
            return None
        return float(value)
    except Exception:
        return None


def _safe_int(value: Any) -> Optional[int]:
    numeric = _safe_float(value)
    if numeric is None:
        return None
    try:
        return int(round(numeric))
    except Exception:
        return None


def _parse_bool_text(value: Any) -> bool:
    text = str(value or "").strip().lower()
    if text in {"1", "true", "on", "yes", "y"}:
        return True
    if text in {"0", "false", "off", "no", "n"}:
        return False
    raise argparse.ArgumentTypeError("expected one of: true/false/on/off")


def _parse_int_list(raw: str | None, default_values: Sequence[int]) -> List[int]:
    if raw in (None, ""):
        return [int(value) for value in default_values]
    out: List[int] = []
    for part in str(raw).split(","):
        text = part.strip()
        if not text:
            continue
        out.append(int(float(text)))
    if not out:
        raise argparse.ArgumentTypeError("list must not be empty")
    return out


def _parse_str_list(raw: str | None, default_values: Sequence[str]) -> List[str]:
    if raw in (None, ""):
        return [str(value) for value in default_values]
    out = [part.strip().upper() for part in str(raw).split(",") if part.strip()]
    if not out:
        raise argparse.ArgumentTypeError("list must not be empty")
    return out


def _deep_merge_dict(base: Mapping[str, Any], overlay: Mapping[str, Any]) -> Dict[str, Any]:
    merged: Dict[str, Any] = copy.deepcopy(dict(base))
    for key, value in overlay.items():
        if isinstance(value, Mapping) and isinstance(merged.get(key), dict):
            merged[key] = _deep_merge_dict(merged[key], value)
        else:
            merged[key] = copy.deepcopy(value)
    return merged


def _load_cli_config(path: str | Path) -> Dict[str, Any]:
    config_path = Path(path).resolve()
    payload = json.loads(config_path.read_text(encoding="utf-8-sig"))
    if not isinstance(payload, Mapping) or "base_config" not in payload:
        return load_config(config_path)

    base_raw = payload.get("base_config")
    if not str(base_raw or "").strip():
        raise RuntimeError(f"overlay config missing base_config: {config_path}")

    base_path = Path(str(base_raw))
    if not base_path.is_absolute():
        base_path = (config_path.parent / base_path).resolve()
    else:
        base_path = base_path.resolve()

    overlay = {str(key): copy.deepcopy(value) for key, value in payload.items() if str(key) != "base_config"}
    merged = _deep_merge_dict(load_config(base_path), overlay)
    merged["_base_dir"] = str(base_path.parent.parent)
    return merged


def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Independent metrology-grade seal/pressure qualification diagnostic V2. Does not write coefficients.",
    )
    parser.add_argument("--config", default="configs/default_config.json")
    parser.add_argument("--allow-live-hardware", action="store_true")
    parser.add_argument("--analyzer", default=DEFAULT_ANALYZER_ID)
    parser.add_argument("--co2-group", choices=("auto", "A", "B", "a", "b"), default="auto")
    parser.add_argument("--variants", default="A,B,C", help="Process variants to run (default: A,B,C).")
    parser.add_argument("--layers", default="1,2,3", help="Layers to run (default: 1,2,3).")
    parser.add_argument("--repeats", type=int, default=3, help="Repeat count for each requested layer (default: 3).")
    parser.add_argument("--gas-points", default=None, help="Optional full-matrix gas points override, e.g. 0,200,400,600,800,1000.")
    parser.add_argument("--pressure-points", default=None, help="Optional full-matrix pressure points override, e.g. 1100,1000,900,800,700,600,500.")
    parser.add_argument("--gas-switch-deadtime-s", type=float, default=DEFAULT_GAS_SWITCH_DEADTIME_S)
    parser.add_argument("--min-flush-s", type=float, default=DEFAULT_MIN_FLUSH_S)
    parser.add_argument("--screening-flush-s-default", type=float, default=DEFAULT_SCREENING_FLUSH_S)
    parser.add_argument("--max-flush-s", type=float, default=DEFAULT_MAX_FLUSH_S)
    parser.add_argument("--flush-gate-window-s", type=float, default=DEFAULT_THRESHOLDS.flush_gate_window_s)
    parser.add_argument(
        "--flush-vent-refresh-interval-s",
        type=float,
        default=0.0,
        help=(
            "Diagnostic-only: for analyzer-chain-isolation + --skip-gas-analyzer, "
            "re-run the legacy safe vent action during flush every N seconds; 0 disables refresh."
        ),
    )
    parser.add_argument("--enable-precondition", type=_parse_bool_text, default=True)
    parser.add_argument("--precondition-only", type=_parse_bool_text, default=False)
    parser.add_argument("--precondition-gas-ppm", type=int, default=DEFAULT_PRECONDITION_GAS_PPM)
    parser.add_argument("--precondition-min-flush-s", type=float, default=DEFAULT_PRECONDITION_MIN_FLUSH_S)
    parser.add_argument("--precondition-max-flush-s", type=float, default=DEFAULT_PRECONDITION_MAX_FLUSH_S)
    parser.add_argument("--precondition-window-s", type=float, default=DEFAULT_PRECONDITION_WINDOW_S)
    parser.add_argument("--rebound-window-s", type=float, default=DEFAULT_DEWPOINT_REBOUND_WINDOW_S)
    parser.add_argument("--rebound-min-rise-c", type=float, default=DEFAULT_DEWPOINT_REBOUND_MIN_RISE_C)
    parser.add_argument("--pressure-settle-timeout-s", type=float, default=DEFAULT_PRESSURE_SETTLE_TIMEOUT_S)
    parser.add_argument("--stable-window-s", type=float, default=DEFAULT_STABLE_WINDOW_S)
    parser.add_argument("--sealed-hold-s", type=float, default=DEFAULT_SEALED_HOLD_S)
    parser.add_argument("--sample-poll-s", type=float, default=DEFAULT_SAMPLE_POLL_S)
    parser.add_argument("--restore-vent-observe-s", type=float, default=DEFAULT_RESTORE_VENT_OBSERVE_S)
    parser.add_argument("--print-every-s", type=float, default=10.0)
    parser.add_argument("--configure-analyzer-stream", action="store_true")
    parser.add_argument(
        "--skip-gas-analyzer",
        action="store_true",
        help="Diagnostic-only: do not build/open/start/configure any gas analyzer; keep focus on PACE/gauge/dewpoint/relay/route switching.",
    )
    parser.add_argument("--early-stop", type=_parse_bool_text, default=True, help="Stop deeper layers for a variant after fail; default true.")
    parser.add_argument("--treat-insufficient-as-stop", type=_parse_bool_text, default=True, help="Treat insufficient_evidence as a stage stop; default true.")
    parser.add_argument("--variant-b-seal-trigger-hpa", type=float, default=1110.0)
    parser.add_argument("--variant-c-factor", choices=("seal_trigger_hpa", "ramp_down_rate"), default="seal_trigger_hpa")
    parser.add_argument("--variant-c-value", type=float, default=1105.0)
    parser.add_argument("--single-cycle-smoke", action="store_true", help="Shortcut for the smallest first live smoke: B + Layer 1 + repeats 1.")
    parser.add_argument(
        "--smoke-level",
        choices=("s1", "s1-recheck", "s2", "s3", "screen", "layer4", "analyzer-chain-isolation"),
        default=None,
    )
    parser.add_argument(
        "--chain-mode",
        choices=(
            "analyzer_out_keep_rest",
            "analyzer_out_pace_out_keep_rest",
            "analyzer_in_keep_rest",
            "analyzer_in_pace_out_keep_rest",
            "compare_pair",
        ),
        default="analyzer_out_keep_rest",
    )
    parser.add_argument("--setup-note", default="")
    parser.add_argument("--operator-note", default="")
    parser.add_argument("--analyzer-count-in-path", type=int, choices=(0, 8), default=None)
    parser.add_argument("--output-dir", default=None)
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--no-restore-baseline", action="store_true")
    parser.add_argument("--no-export-png", action="store_true")
    parser.add_argument("--no-export-xlsx", action="store_true")
    parser.add_argument("--no-export-csv", action="store_true")
    args = parser.parse_args(list(argv) if argv is not None else None)
    if args.smoke_level == "s1":
        args.variants = "B"
        args.layers = "1"
        args.repeats = 1
        args.early_stop = True
        args.treat_insufficient_as_stop = True
        args.enable_precondition = False
    elif args.smoke_level == "s1-recheck":
        args.variants = "B"
        args.layers = "1"
        args.repeats = 1
        args.early_stop = True
        args.treat_insufficient_as_stop = True
        args.enable_precondition = True
    elif args.smoke_level == "s2":
        args.variants = "B"
        args.layers = "1,2"
        args.repeats = 1
        args.early_stop = True
        args.treat_insufficient_as_stop = True
    elif args.smoke_level == "s3":
        args.variants = "B"
        args.layers = "1,2,3"
        args.repeats = 1
        args.early_stop = True
        args.treat_insufficient_as_stop = True
    elif args.smoke_level == "screen":
        args.variants = "A,B,C"
        args.layers = "1,2,3"
        args.repeats = 3
        args.early_stop = True
        args.treat_insufficient_as_stop = True
    elif args.smoke_level == "layer4":
        if str(args.variants or "A,B,C") == "A,B,C":
            args.variants = "B"
        args.layers = "4"
        args.repeats = 1
        args.early_stop = True
    elif args.smoke_level == "analyzer-chain-isolation":
        args.variants = "B"
        args.layers = "1"
        args.repeats = 1
        args.early_stop = True
        args.treat_insufficient_as_stop = True
        args.enable_precondition = True
        args.precondition_only = True
    if args.single_cycle_smoke and args.smoke_level is None:
        args.variants = "B"
        args.layers = "1"
        args.repeats = 1
        args.early_stop = True
        args.treat_insufficient_as_stop = True
        args.enable_precondition = False
    if args.precondition_only:
        args.enable_precondition = True
    if float(args.flush_vent_refresh_interval_s) < 0.0:
        parser.error("--flush-vent-refresh-interval-s must be >= 0")
    if float(args.flush_vent_refresh_interval_s) > 0.0 and not (
        args.smoke_level == "analyzer-chain-isolation" and bool(args.skip_gas_analyzer)
    ):
        parser.error(
            "--flush-vent-refresh-interval-s only applies to "
            "--smoke-level analyzer-chain-isolation with --skip-gas-analyzer"
        )
    if args.chain_mode in {"analyzer_out_keep_rest", "analyzer_out_pace_out_keep_rest"} and args.analyzer_count_in_path is None:
        args.analyzer_count_in_path = 0
    elif args.chain_mode in {"analyzer_in_keep_rest", "analyzer_in_pace_out_keep_rest"} and args.analyzer_count_in_path is None:
        args.analyzer_count_in_path = 8
    return args


def _make_logger(
    cfg: Mapping[str, Any],
    output_dir: Optional[str],
    run_id: Optional[str],
    *,
    smoke_level: Optional[str] = None,
) -> RunLogger:
    base_dir = Path(str(cfg.get("_base_dir") or Path.cwd()))
    default_leaf = "analyzer_chain_isolation" if smoke_level == "analyzer-chain-isolation" else "metrology_seal_pressure_v2"
    default_root = base_dir / "results" / "diagnostics" / default_leaf
    root = Path(output_dir).resolve() if output_dir else default_root.resolve()
    timestamp = run_id or datetime.now().strftime("%Y%m%d_%H%M%S")
    return RunLogger(root, run_id=timestamp, cfg=dict(cfg))


def _prepare_runtime_cfg(
    cfg: Mapping[str, Any],
    analyzer_cfg: Mapping[str, Any],
    args: argparse.Namespace,
    variant: VariantSpec,
    *,
    skip_gas_analyzer: bool = False,
) -> Dict[str, Any]:
    runtime_cfg = copy.deepcopy(dict(cfg))
    runtime_devices = runtime_cfg.setdefault("devices", {})
    runtime_devices["gas_analyzer"] = dict(analyzer_cfg) if isinstance(analyzer_cfg, Mapping) else {}
    runtime_devices["gas_analyzer"]["enabled"] = not bool(skip_gas_analyzer)
    runtime_devices["gas_analyzers"] = []
    pressure_cfg = runtime_cfg.setdefault("workflow", {}).setdefault("pressure", {})
    pressure_cfg["stabilize_timeout_s"] = float(args.pressure_settle_timeout_s)
    if variant.seal_trigger_hpa is not None:
        pressure_cfg["co2_preseal_pressure_gauge_trigger_hpa"] = float(variant.seal_trigger_hpa)
    if variant.ramp_down_rate_hpa_per_s is not None:
        pressure_cfg["soft_control_enabled"] = True
        pressure_cfg["soft_control_linear_slew_hpa_per_s"] = float(variant.ramp_down_rate_hpa_per_s)
    return runtime_cfg


def _build_devices(
    runtime_cfg: Mapping[str, Any],
    analyzer_cfg: Mapping[str, Any],
    io_logger: RunLogger,
    *,
    skip_gas_analyzer: bool = False,
) -> Dict[str, Any]:
    dcfg = runtime_cfg.get("devices", {}) if isinstance(runtime_cfg, Mapping) else {}
    built: Dict[str, Any] = {}
    try:
        if not skip_gas_analyzer:
            built["gas_analyzer"] = GasAnalyzer(
                str(analyzer_cfg["port"]),
                int(analyzer_cfg.get("baud", 115200)),
                device_id=str(analyzer_cfg.get("device_id") or "000"),
                io_logger=io_logger,
            )
            built["gas_analyzer"].open()

        pcfg = dcfg.get("pressure_controller", {})
        if not isinstance(pcfg, dict) or not pcfg.get("enabled"):
            raise RuntimeError("pressure_controller is required for this diagnostic")
        built["pace"] = Pace5000(
            str(pcfg["port"]),
            int(pcfg.get("baud", 9600)),
            timeout=float(pcfg.get("timeout", 1.0)),
            line_ending=pcfg.get("line_ending"),
            query_line_endings=pcfg.get("query_line_endings"),
            pressure_queries=pcfg.get("pressure_queries"),
            io_logger=io_logger,
        )
        built["pace"].open()

        gcfg = dcfg.get("pressure_gauge", {})
        if not isinstance(gcfg, dict) or not gcfg.get("enabled"):
            raise RuntimeError("pressure_gauge is required for this diagnostic")
        built["pressure_gauge"] = ParoscientificGauge(
            str(gcfg["port"]),
            int(gcfg.get("baud", 9600)),
            timeout=float(gcfg.get("timeout", 1.0)),
            dest_id=str(gcfg.get("dest_id", "01")),
            response_timeout_s=gcfg.get("response_timeout_s"),
            io_logger=io_logger,
        )
        built["pressure_gauge"].open()

        dpcfg = dcfg.get("dewpoint_meter", {})
        if not isinstance(dpcfg, dict) or not dpcfg.get("enabled"):
            raise RuntimeError("dewpoint_meter is required for this diagnostic")
        built["dewpoint"] = DewpointMeter(
            str(dpcfg["port"]),
            int(dpcfg.get("baud", 9600)),
            station=str(dpcfg.get("station", "001")),
            io_logger=io_logger,
        )
        built["dewpoint"].open()

        relay_cfg = dcfg.get("relay", {})
        if isinstance(relay_cfg, dict) and relay_cfg.get("enabled"):
            built["relay"] = RelayController(
                str(relay_cfg["port"]),
                int(relay_cfg.get("baud", 38400)),
                addr=int(relay_cfg.get("addr", 1)),
                io_logger=io_logger,
            )
            built["relay"].open()

        relay8_cfg = dcfg.get("relay_8", {})
        if isinstance(relay8_cfg, dict) and relay8_cfg.get("enabled"):
            built["relay_8"] = RelayController(
                str(relay8_cfg["port"]),
                int(relay8_cfg.get("baud", 38400)),
                addr=int(relay8_cfg.get("addr", 1)),
                io_logger=io_logger,
            )
            built["relay_8"].open()
    except Exception:
        _close_devices(built)
        raise
    return built


def _resolve_selected_analyzer_cfg(
    cfg: Mapping[str, Any],
    args: argparse.Namespace,
) -> Tuple[str, Dict[str, Any]]:
    try:
        return _select_analyzer_cfg(cfg, args.analyzer)
    except Exception:
        if bool(getattr(args, "skip_gas_analyzer", False)) and str(getattr(args, "smoke_level", "") or "").strip() == "analyzer-chain-isolation":
            requested = str(getattr(args, "analyzer", "") or "").strip() or DEFAULT_ANALYZER_ID
            return requested, {}
        raise


def _build_source_point(gas_ppm: int, *, index: int, co2_group: str) -> CalibrationPoint:
    return CalibrationPoint(
        index=index,
        temp_chamber_c=20.0,
        co2_ppm=float(gas_ppm),
        hgen_temp_c=None,
        hgen_rh_pct=None,
        target_pressure_hpa=None,
        dewpoint_c=None,
        h2o_mmol=None,
        raw_h2o=None,
        co2_group=co2_group,
    )


def _build_pressure_point(source_point: CalibrationPoint, pressure_target_hpa: int, *, index: int) -> CalibrationPoint:
    return CalibrationPoint(
        index=index,
        temp_chamber_c=float(source_point.temp_chamber_c),
        co2_ppm=float(source_point.co2_ppm or 0.0),
        hgen_temp_c=None,
        hgen_rh_pct=None,
        target_pressure_hpa=float(pressure_target_hpa),
        dewpoint_c=None,
        h2o_mmol=None,
        raw_h2o=None,
        co2_group=source_point.co2_group,
    )


def _route_for_point(runner: CalibrationRunner, point: CalibrationPoint) -> Dict[str, Any]:
    source_valve = runner._source_valve_for_point(point)
    path_valve = runner._co2_path_for_point(point)
    open_logical_valves = runner._co2_open_valves(point, include_total_valve=True)
    if source_valve is None or path_valve is None or not open_logical_valves:
        raise RuntimeError(f"CO2 route mapping not found for {point.co2_ppm} ppm")
    group = str(getattr(point, "co2_group", "") or "").strip().upper() or "A"
    return {
        "group": group,
        "source_valve": source_valve,
        "path_valve": path_valve,
        "open_logical_valves": open_logical_valves,
    }


def _non_source_open_valves(route: Mapping[str, Any]) -> List[int]:
    source_valve = route.get("source_valve")
    ordered: List[int] = []
    seen: set[int] = set()
    for value in route.get("open_logical_valves", []) or []:
        try:
            numeric = int(value)
        except Exception:
            continue
        if source_valve is not None and int(source_valve) == numeric:
            continue
        if numeric in seen:
            continue
        seen.add(numeric)
        ordered.append(numeric)
    return ordered


def _closed_swing_feed_valves(runner: CalibrationRunner, route: Mapping[str, Any]) -> List[int]:
    valves_cfg = getattr(runner, "cfg", {}).get("valves", {}) if getattr(runner, "cfg", None) else {}
    ordered: List[int] = []
    seen: set[int] = set()
    for key in ("h2o_path", "gas_main"):
        try:
            numeric = int(valves_cfg.get(key))
        except Exception:
            continue
        if numeric in seen:
            continue
        seen.add(numeric)
        ordered.append(numeric)
    source_valve = route.get("source_valve")
    if source_valve is not None:
        try:
            numeric = int(source_valve)
        except Exception:
            numeric = None
        if numeric is not None and numeric not in seen:
            seen.add(numeric)
            ordered.append(numeric)
    return ordered


def _closed_swing_open_valves(runner: CalibrationRunner, route: Mapping[str, Any]) -> List[int]:
    upstream_feed_valves = set(_closed_swing_feed_valves(runner, route))

    ordered: List[int] = []
    seen: set[int] = set()
    preferred_keep = route.get("path_valve")
    for value in route.get("open_logical_valves", []) or []:
        try:
            numeric = int(value)
        except Exception:
            continue
        if numeric in upstream_feed_valves:
            continue
        if numeric in seen:
            continue
        seen.add(numeric)
        ordered.append(numeric)

    if preferred_keep is not None:
        try:
            preferred_numeric = int(preferred_keep)
        except Exception:
            preferred_numeric = None
        if preferred_numeric is not None and preferred_numeric not in seen:
            ordered.insert(0, preferred_numeric)
    return ordered


def _commanded_valve_state(runner: CalibrationRunner, valve: int) -> Optional[bool]:
    resolve = getattr(runner, "_resolve_valve_target", None)
    cache = getattr(runner, "_relay_state_cache", None)
    if not callable(resolve) or not isinstance(cache, dict):
        return None
    try:
        relay_name, channel = resolve(int(valve))
    except Exception:
        return None
    return cache.get((str(relay_name), int(channel)))


def _closed_swing_feed_close_failures(runner: CalibrationRunner, route: Mapping[str, Any]) -> List[str]:
    failures: List[str] = []
    for valve in _closed_swing_feed_valves(runner, route):
        state = _commanded_valve_state(runner, valve)
        if state is None:
            failures.append(f"feed_valve_state_unknown:{valve}")
        elif bool(state):
            failures.append(f"feed_valve_still_open:{valve}")
    path_valve = route.get("path_valve")
    if path_valve is not None:
        try:
            path_numeric = int(path_valve)
        except Exception:
            path_numeric = None
        if path_numeric is not None:
            state = _commanded_valve_state(runner, path_numeric)
            if state is None:
                failures.append(f"path_valve_state_unknown:{path_numeric}")
            elif not bool(state):
                failures.append(f"path_valve_not_open:{path_numeric}")
    return failures


def _closed_swing_supply_open_state(runner: CalibrationRunner, route: Mapping[str, Any]) -> Optional[bool]:
    feed_valves = _closed_swing_feed_valves(runner, route)
    states: List[bool] = []
    for valve in feed_valves:
        state = _commanded_valve_state(runner, valve)
        if state is None:
            return None
        states.append(bool(state))
    return any(states) if states else None


def _closed_swing_manifold_state(runner: CalibrationRunner, route: Mapping[str, Any]) -> str:
    valves_cfg = getattr(runner, "cfg", {}).get("valves", {}) if getattr(runner, "cfg", None) else {}
    parts: List[str] = []
    for key in ("h2o_path", "gas_main"):
        numeric = _safe_int(valves_cfg.get(key))
        if numeric is None:
            continue
        state = _commanded_valve_state(runner, numeric)
        state_text = "unknown" if state is None else ("open" if state else "closed")
        parts.append(f"{key}:{numeric}={state_text}")
    return ";".join(parts)


def _latest_pressure_snapshot(rows: Sequence[Mapping[str, Any]]) -> Tuple[Optional[float], str, Optional[float]]:
    latest = dict(rows[-1]) if rows else {}
    gauge = _safe_float(latest.get("gauge_pressure_hpa"))
    controller = _safe_float(latest.get("controller_pressure_hpa"))
    dewpoint = _safe_float(latest.get("dewpoint_c"))
    if gauge is not None:
        return gauge, "gauge_pressure_hpa", dewpoint
    return controller, "controller_pressure_hpa", dewpoint


def _intermediate_open_valves(previous_route: Mapping[str, Any], next_route: Mapping[str, Any]) -> List[int]:
    ordered: List[int] = []
    seen: set[int] = set()
    for route in (previous_route, next_route):
        for value in _non_source_open_valves(route):
            if value in seen:
                continue
            seen.add(value)
            ordered.append(value)
    return ordered


def _trace_row_count(path: Path) -> int:
    if not path.exists():
        return 0
    with path.open("r", encoding="utf-8", newline="") as handle:
        return sum(1 for _ in csv.DictReader(handle))


def _map_trace_phase(trace_stage: str) -> str:
    stage = str(trace_stage or "").strip().lower()
    if stage in {"preseal_vent_off_begin", "preseal_wait", "preseal_trigger_reached", "route_sealed"}:
        return "seal_prepare"
    if stage.startswith("control_") or stage in {"pressure_control_wait", "pressure_in_limits", "pressure_in_limits_ready_check"}:
        return "pressure_sweep"
    if stage.startswith("dewpoint_gate") or stage.startswith("sampling_") or stage == "sampling_begin":
        return "stable_sampling"
    if stage.startswith("atmosphere_"):
        return "restore_vent_on"
    return "pressure_sweep"


def _trace_tail_rows(
    trace_path: Path,
    cursor: int,
    *,
    context: Mapping[str, Any],
    gas_start_ts: datetime,
    default_pressure_target_hpa: Optional[int],
    controller_vent_state: str,
) -> Tuple[List[Dict[str, Any]], int]:
    if not trace_path.exists():
        return [], cursor
    with trace_path.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    new_rows = rows[cursor:]
    normalized: List[Dict[str, Any]] = []
    for row in new_rows:
        ts = row.get("ts")
        dt = datetime.fromisoformat(str(ts)) if ts else None
        pressure_target = row.get("pressure_target_hpa")
        normalized.append(
            {
                "timestamp": ts,
                "elapsed_s": max(0.0, (dt - gas_start_ts).total_seconds()) if dt is not None else None,
                "phase_elapsed_s": None,
                "process_variant": context.get("process_variant"),
                "layer": context.get("layer"),
                "repeat_index": context.get("repeat_index"),
                "phase": _map_trace_phase(str(row.get("trace_stage") or "")),
                "current_phase": _map_trace_phase(str(row.get("trace_stage") or "")),
                "gas_ppm": context.get("gas_ppm"),
                "chain_mode": context.get("chain_mode"),
                "pressure_target_hpa": int(float(pressure_target)) if str(pressure_target or "").strip() else default_pressure_target_hpa,
                "route_group": context.get("route_group"),
                "source_valve": context.get("source_valve"),
                "path_valve": context.get("path_valve"),
                "row_source": "runner_pressure_trace",
                "trace_stage": row.get("trace_stage"),
                "phase_note": row.get("trigger_reason") or row.get("note"),
                "analyzer2_co2_ratio": None,
                "co2_ratio_raw": None,
                "co2_ratio_f": None,
                "co2_density": None,
                "co2_ppm": None,
                "chamber_temp_c": None,
                "shell_temp_c": None,
                "analyzer2_pressure_hpa": None,
                "gauge_pressure_hpa": row.get("pressure_gauge_hpa"),
                "dewpoint_c": row.get("dewpoint_c") or row.get("dewpoint_live_c"),
                "dewpoint_temp_c": row.get("dew_temp_c") or row.get("dew_temp_live_c"),
                "dewpoint_rh_percent": row.get("dew_rh_pct") or row.get("dew_rh_live_pct"),
                "controller_pressure_hpa": row.get("pace_pressure_hpa"),
                "controller_vent_state": controller_vent_state,
                "controller_vent_status_code": row.get("pace_vent_status"),
                "controller_output_state": row.get("pace_output_state"),
                "controller_isolation_state": row.get("pace_isolation_state"),
                "actual_deadtime_s": context.get("actual_deadtime_s"),
                "gate_pass": context.get("gate_pass"),
                "gate_fail_reason": context.get("gate_fail_reason"),
            }
        )
    return normalized, len(rows)


def _capture_phase_rows(
    analyzer: Optional[GasAnalyzer],
    devices: Mapping[str, Any],
    *,
    context: Mapping[str, Any],
    gas_start_mono: float,
    duration_s: float,
    sample_poll_s: float,
    print_every_s: float,
    controller_vent_state: str,
    pressure_target_hpa: Optional[int] = None,
    phase_elapsed_offset_s: float = 0.0,
) -> List[Dict[str, Any]]:
    prefer_stream = bool(analyzer.active_send) if analyzer is not None else False
    rows: List[Dict[str, Any]] = []
    phase_start = time.monotonic()
    last_print = -1.0
    while True:
        now = time.monotonic()
        phase_elapsed = (now - phase_start) + float(phase_elapsed_offset_s)
        gas_elapsed = now - gas_start_mono
        if phase_elapsed > float(phase_elapsed_offset_s) + float(duration_s) and rows:
            break

        frame_start = time.monotonic()
        raw = None
        parsed = None
        if analyzer is not None:
            raw = analyzer.read_latest_data(
                prefer_stream=prefer_stream,
                drain_s=max(0.05, min(0.25, sample_poll_s * 1.5)),
                read_timeout_s=0.05,
                allow_passive_fallback=True,
            )
            parsed = analyzer.parse_line(raw) if raw else None
        analyzer_pressure_hpa = None
        if isinstance(parsed, dict):
            pressure_kpa = parsed.get("pressure_kpa")
            if pressure_kpa not in (None, ""):
                try:
                    analyzer_pressure_hpa = float(pressure_kpa) * 10.0
                except Exception:
                    analyzer_pressure_hpa = None

        gauge_value = None
        controller_pressure = None
        controller_status_code = None
        controller_output_state = None
        controller_isolation_state = None
        dewpoint_value = None
        dew_temp_value = None
        dew_rh_value = None
        try:
            gauge_value = devices["pressure_gauge"].read_pressure_fast()
        except Exception:
            gauge_value = None
        try:
            controller_pressure = devices["pace"].read_pressure()
        except Exception:
            controller_pressure = None
        try:
            controller_status_code = devices["pace"].get_vent_status()
        except Exception:
            controller_status_code = None
        try:
            controller_output_state = devices["pace"].get_output_state()
        except Exception:
            controller_output_state = None
        try:
            controller_isolation_state = devices["pace"].get_isolation_state()
        except Exception:
            controller_isolation_state = None
        try:
            dew_data = devices["dewpoint"].get_current_fast(timeout_s=0.35)
        except Exception:
            dew_data = {}
        if isinstance(dew_data, dict):
            dewpoint_value = dew_data.get("dewpoint_c")
            dew_temp_value = dew_data.get("temp_c")
            dew_rh_value = dew_data.get("rh_pct")

        phase_name = str(context.get("phase") or "")
        row = {
            "timestamp": datetime.now().isoformat(timespec="milliseconds"),
            "elapsed_s": round(gas_elapsed, 6),
            "phase_elapsed_s": round(phase_elapsed, 6),
            "process_variant": context.get("process_variant"),
            "layer": context.get("layer"),
            "repeat_index": context.get("repeat_index"),
            "phase": phase_name,
            "current_phase": phase_name,
            "gas_ppm": context.get("gas_ppm"),
            "chain_mode": context.get("chain_mode"),
            "pressure_target_hpa": pressure_target_hpa,
            "route_group": context.get("route_group"),
            "source_valve": context.get("source_valve"),
            "path_valve": context.get("path_valve"),
            "row_source": "poll",
            "trace_stage": "",
            "phase_note": "",
            "analyzer2_co2_ratio": parsed.get("co2_ratio_raw") if isinstance(parsed, dict) else None,
            "co2_ratio_raw": parsed.get("co2_ratio_raw") if isinstance(parsed, dict) else None,
            "co2_ratio_f": parsed.get("co2_ratio_f") if isinstance(parsed, dict) else None,
            "co2_density": parsed.get("co2_density") if isinstance(parsed, dict) else None,
            "co2_ppm": parsed.get("co2_ppm") if isinstance(parsed, dict) else None,
            "chamber_temp_c": parsed.get("chamber_temp_c") if isinstance(parsed, dict) else None,
            "shell_temp_c": parsed.get("case_temp_c") if isinstance(parsed, dict) else None,
            "analyzer2_pressure_hpa": analyzer_pressure_hpa,
            "gauge_pressure_hpa": gauge_value,
            "dewpoint_c": dewpoint_value,
            "dewpoint_temp_c": dew_temp_value,
            "dewpoint_rh_percent": dew_rh_value,
            "controller_pressure_hpa": controller_pressure,
            "controller_vent_state": controller_vent_state,
            "controller_vent_status_code": controller_status_code,
            "controller_output_state": controller_output_state,
            "controller_isolation_state": controller_isolation_state,
            "actual_deadtime_s": context.get("actual_deadtime_s"),
            "gate_pass": context.get("gate_pass"),
            "gate_fail_reason": context.get("gate_fail_reason"),
        }
        rows.append(row)

        if phase_elapsed - last_print >= max(0.2, float(print_every_s)):
            ratio_text = "--" if row["analyzer2_co2_ratio"] is None else f"{float(row['analyzer2_co2_ratio']):.6f}"
            pressure_text = "--" if gauge_value is None else f"{float(gauge_value):.3f}"
            dewpoint_text = "--" if dewpoint_value is None else f"{float(dewpoint_value):.3f}"
            vent_text = "--" if controller_status_code is None else str(controller_status_code)
            output_text = "--" if controller_output_state is None else str(controller_output_state)
            isolation_text = "--" if controller_isolation_state is None else str(controller_isolation_state)
            _log(
                f"[{context.get('process_variant')}] L{context.get('layer')} R{context.get('repeat_index')} "
                f"{context.get('gas_ppm')} ppm {phase_name} {phase_elapsed:6.1f}s | "
                f"ratio={ratio_text} dewpoint={dewpoint_text} gauge={pressure_text} "
                f"vent_code={vent_text} out={output_text} isol={isolation_text}"
            )
            last_print = phase_elapsed

        remaining_s = max(0.0, float(sample_poll_s) - (time.monotonic() - frame_start))
        if remaining_s > 0.0:
            time.sleep(remaining_s)
    return rows


def _phase_context(
    *,
    process_variant: str,
    layer: int,
    repeat_index: int,
    phase: str,
    gas_ppm: int,
    route: Mapping[str, Any],
    actual_deadtime_s: Optional[float] = None,
    gate_pass: Optional[bool] = None,
    gate_fail_reason: str = "",
    chain_mode: Optional[str] = None,
) -> Dict[str, Any]:
    return {
        "process_variant": process_variant,
        "layer": int(layer),
        "repeat_index": int(repeat_index),
        "phase": phase,
        "gas_ppm": int(gas_ppm),
        "chain_mode": chain_mode,
        "route_group": route.get("group"),
        "source_valve": route.get("source_valve"),
        "path_valve": route.get("path_valve"),
        "actual_deadtime_s": actual_deadtime_s,
        "gate_pass": gate_pass,
        "gate_fail_reason": gate_fail_reason,
    }


def _record_event(
    events: List[Dict[str, Any]],
    *,
    run_id: str,
    process_variant: str,
    layer: int,
    repeat_index: int,
    gas_ppm: Optional[int],
    pressure_target_hpa: Optional[int],
    event_type: str,
    event_value: Any = "",
    note: str = "",
    timestamp: Optional[str] = None,
    chain_mode: Optional[str] = None,
    extra_fields: Optional[Mapping[str, Any]] = None,
) -> None:
    row = {
        "timestamp": timestamp or datetime.now().isoformat(timespec="milliseconds"),
        "run_id": run_id,
        "process_variant": process_variant,
        "layer": layer,
        "repeat_index": repeat_index,
        "gas_ppm": gas_ppm,
        "chain_mode": chain_mode,
        "pressure_target_hpa": pressure_target_hpa,
        "event_type": event_type,
        "event_value": event_value,
        "note": note,
    }
    if extra_fields:
        row.update(dict(extra_fields))
    events.append(row)


def _record_trace_events(
    events: List[Dict[str, Any]],
    *,
    run_id: str,
    trace_rows: Sequence[Mapping[str, Any]],
) -> None:
    mapping = {
        "preseal_vent_off_begin": "vent_off",
        "preseal_trigger_reached": "seal_trigger_reached",
        "route_sealed": "seal_valve_close",
    }
    for row in trace_rows:
        trace_stage = str(row.get("trace_stage") or "")
        event_type = mapping.get(trace_stage)
        if not event_type:
            continue
        _record_event(
            events,
            run_id=run_id,
            process_variant=str(row.get("process_variant") or ""),
            layer=int(row.get("layer") or 0),
            repeat_index=int(row.get("repeat_index") or 0),
            gas_ppm=_safe_int(row.get("gas_ppm")),
            chain_mode=str(row.get("chain_mode") or "") or None,
            pressure_target_hpa=_safe_int(row.get("pressure_target_hpa")),
            event_type=event_type,
            event_value=trace_stage,
            note=str(row.get("phase_note") or ""),
            timestamp=str(row.get("timestamp") or ""),
        )


def _write_actuation_events_csv(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    if not rows:
        return
    fieldnames = [
        "timestamp",
        "run_id",
        "process_variant",
        "layer",
        "repeat_index",
        "gas_ppm",
        "chain_mode",
        "pressure_target_hpa",
        "event_type",
        "event_value",
        "note",
    ]
    for row in rows:
        for key in row.keys():
            if key not in fieldnames:
                fieldnames.append(str(key))
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: row.get(key) for key in fieldnames})


def _append_live_csv_row(path: Path, row: Mapping[str, Any]) -> None:
    normalized: Dict[str, Any] = {}
    for key, value in dict(row).items():
        if isinstance(value, list):
            normalized[str(key)] = ";".join(str(item) for item in value)
        elif isinstance(value, dict):
            normalized[str(key)] = json.dumps(value, ensure_ascii=False)
        else:
            normalized[str(key)] = value

    existing_rows: List[Dict[str, Any]] = []
    fieldnames: List[str] = []
    if path.exists() and path.stat().st_size > 0:
        with path.open("r", encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle)
            fieldnames = list(reader.fieldnames or [])
            existing_rows = [dict(item) for item in reader]
    for key in normalized.keys():
        if key not in fieldnames:
            fieldnames.append(key)
    existing_rows.append(normalized)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for item in existing_rows:
            writer.writerow({key: item.get(key) for key in fieldnames})


def _variant_specs(args: argparse.Namespace) -> List[VariantSpec]:
    selected = _parse_str_list(args.variants, DEFAULT_VARIANTS)
    specs: Dict[str, VariantSpec] = {
        "A": VariantSpec(
            name="A",
            description="基线版，沿用当前配置动作顺序",
            seal_trigger_hpa=None,
            ramp_down_rate_hpa_per_s=None,
            stable_gate_mode="baseline",
            comparison_factor="baseline",
        ),
        "B": VariantSpec(
            name="B",
            description="候选版，1110hPa 触发封路，单向降压",
            seal_trigger_hpa=float(args.variant_b_seal_trigger_hpa),
            ramp_down_rate_hpa_per_s=None,
            stable_gate_mode="pressure+dewpoint+sample_count",
            comparison_factor="seal_trigger_hpa",
        ),
    }
    if str(args.variant_c_factor) == "seal_trigger_hpa":
        specs["C"] = VariantSpec(
            name="C",
            description="单因素变体，仅修改 seal_trigger_hpa",
            seal_trigger_hpa=float(args.variant_c_value),
            ramp_down_rate_hpa_per_s=None,
            stable_gate_mode="pressure+dewpoint+sample_count",
            comparison_factor="seal_trigger_hpa",
        )
    else:
        specs["C"] = VariantSpec(
            name="C",
            description="单因素变体，仅修改 ramp_down_rate",
            seal_trigger_hpa=float(args.variant_b_seal_trigger_hpa),
            ramp_down_rate_hpa_per_s=float(args.variant_c_value),
            stable_gate_mode="pressure+dewpoint+sample_count",
            comparison_factor="ramp_down_rate",
        )
    return [specs[name] for name in selected if name in specs]


def _layer_sequence(args: argparse.Namespace) -> List[int]:
    values = _parse_int_list(args.layers, DEFAULT_LAYER_SEQUENCE)
    out = []
    for value in values:
        if value not in {1, 2, 3, 4}:
            raise argparse.ArgumentTypeError("layers must be in {1,2,3,4}")
        if value not in out:
            out.append(value)
    return out


def _layer_gas_sequence(layer: int, args: argparse.Namespace) -> List[int]:
    if layer == 1:
        return [0, 1000, 0]
    if layer in {2, 3}:
        return list(DEFAULT_SCREENING_GAS_POINTS_PPM)
    return _parse_int_list(args.gas_points, DEFAULT_FULL_GAS_POINTS_PPM)


def _layer_pressure_points(layer: int, args: argparse.Namespace) -> List[int]:
    if layer == 3:
        return list(DEFAULT_SCREENING_PRESSURE_POINTS_HPA)
    if layer == 4:
        return _parse_int_list(args.pressure_points, DEFAULT_FULL_PRESSURE_POINTS_HPA)
    return [1100]


def _apply_gate_to_rows(rows: Sequence[Dict[str, Any]], *, gate_pass: bool, gate_fail_reason: str) -> None:
    for row in rows:
        row["gate_pass"] = bool(gate_pass)
        row["gate_fail_reason"] = gate_fail_reason


def _phase_display_elapsed_s(rows: Sequence[Mapping[str, Any]]) -> float:
    values = [_safe_float(row.get("phase_elapsed_s")) for row in rows]
    numeric = [item for item in values if item is not None]
    return max(numeric) if numeric else 0.0


def _build_flush_gate_trace_row(
    rows: Sequence[Mapping[str, Any]],
    *,
    process_variant: str,
    layer: int,
    repeat_index: int,
    gas_ppm: int,
    phase: str,
    elapsed_real_s: float,
    gate_window_s: float,
    min_flush_s: float,
    target_flush_s: float,
    note: str,
    require_ratio: bool = True,
    chain_mode: Optional[str] = None,
) -> Dict[str, Any]:
    gate = evaluate_flush_gate(
        rows,
        min_flush_s=min_flush_s,
        target_flush_s=target_flush_s,
        gate_window_s=gate_window_s,
        require_ratio=require_ratio,
    )
    latest = rows[-1] if rows else {}
    return {
        "timestamp": str(latest.get("timestamp") or datetime.now().isoformat(timespec="milliseconds")),
        "elapsed_s_real": round(max(0.0, float(elapsed_real_s)), 3),
        "elapsed_s_display": round(_phase_display_elapsed_s(rows), 3),
        "process_variant": process_variant,
        "layer": int(layer),
        "repeat_index": int(repeat_index),
        "gas_ppm": int(gas_ppm),
        "chain_mode": latest.get("chain_mode") or chain_mode,
        "phase": phase,
        "dewpoint_c": _safe_float(latest.get("dewpoint_c")),
        "dewpoint_span_window_c": gate.get("flush_gate_dewpoint_span"),
        "dewpoint_slope_window_c_per_s": gate.get("flush_gate_dewpoint_slope"),
        "ratio_value": gate.get("flush_gate_ratio_value"),
        "ratio_span_window": gate.get("flush_gate_ratio_span"),
        "ratio_slope_window_per_s": gate.get("flush_gate_ratio_slope"),
        "gauge_hpa": gate.get("flush_gate_gauge_value"),
        "gauge_span_window_hpa": gate.get("flush_gate_pressure_span"),
        "gauge_slope_window_hpa_per_s": gate.get("flush_gate_pressure_slope"),
        "gate_pass": gate.get("gate_pass"),
        "failing_subgates": ";".join(gate.get("failing_subgates", []) or []),
        "note": note,
    }


def _log_flush_gate_snapshot(snapshot: Mapping[str, Any]) -> None:
    _log(
        "flush_gate_snapshot "
        f"variant={snapshot.get('process_variant')} "
        f"layer={snapshot.get('layer')} "
        f"repeat={snapshot.get('repeat_index')} "
        f"gas={snapshot.get('gas_ppm')} "
        f"phase={snapshot.get('phase')} "
        f"elapsed_real={snapshot.get('elapsed_s_real')}s "
        f"elapsed_display={snapshot.get('elapsed_s_display')}s "
        f"dewpoint={snapshot.get('dewpoint_c')} "
        f"dewpoint_span_60s={snapshot.get('dewpoint_span_window_c')} "
        f"dewpoint_slope_60s={snapshot.get('dewpoint_slope_window_c_per_s')} "
        f"ratio={snapshot.get('ratio_value')} "
        f"ratio_span_60s={snapshot.get('ratio_span_window')} "
        f"ratio_slope_60s={snapshot.get('ratio_slope_window_per_s')} "
        f"gauge={snapshot.get('gauge_hpa')} "
        f"gauge_span_60s={snapshot.get('gauge_span_window_hpa')} "
        f"gauge_slope_60s={snapshot.get('gauge_slope_window_hpa_per_s')} "
        f"gate_pass={snapshot.get('gate_pass')} "
        f"failing_subgates={snapshot.get('failing_subgates')} "
        f"note={snapshot.get('note')}"
    )


def _pace_has_legacy_ge_druck_identity(pace: Any) -> bool:
    checker = getattr(pace, "has_legacy_vent_status_model", None)
    if callable(checker):
        try:
            return bool(checker())
        except Exception:
            pass
    identity_getter = getattr(pace, "get_device_identity", None)
    if not callable(identity_getter):
        return False
    try:
        identity = str(identity_getter() or "").strip().upper()
    except Exception:
        return False
    return "GE DRUCK" in identity or "PACE5000 USER INTERFACE" in identity


def _analyzer_chain_legacy_safe_vent_policy(
    cfg: Mapping[str, Any],
    *,
    pace: Any,
    pace_in_path: bool,
) -> Dict[str, Any]:
    legacy_identity = bool(pace_in_path and pace is not None and _pace_has_legacy_ge_druck_identity(pace))
    closed_pressure_swing_enabled = bool(_closed_pressure_swing_defaults(cfg).get("closed_pressure_swing_enabled"))
    required = bool(pace_in_path and legacy_identity)
    active = bool(required and not closed_pressure_swing_enabled)
    fail_reason = "closed_pressure_swing_enabled" if required and closed_pressure_swing_enabled else ""
    return {
        "legacy_identity_detected": legacy_identity,
        "legacy_safe_vent_required": required,
        "legacy_safe_vent_active": active,
        "legacy_safe_vent_fail_reason": fail_reason,
        "legacy_safe_vent_mode": "diagnostic_only_no_abort" if active else "",
    }


def _legacy_safe_vent_timeout_s(cfg: Mapping[str, Any]) -> float:
    pressure_cfg = cfg.get("workflow", {}).get("pressure", {}) if isinstance(cfg, Mapping) else {}
    if not isinstance(pressure_cfg, Mapping):
        return 30.0
    return max(0.5, float(pressure_cfg.get("vent_transition_timeout_s", 30.0) or 30.0))


def _run_legacy_safe_vent_action(
    runner: CalibrationRunner,
    *,
    cfg: Mapping[str, Any],
    action: str,
    reason: str,
    run_id: str,
    actuation_events: List[Dict[str, Any]],
    process_variant: str,
    layer: int,
    repeat_index: int,
    gas_ppm: Optional[int],
    chain_mode: Optional[str],
) -> Dict[str, Any]:
    pace = runner.devices.get("pace")
    helper = getattr(pace, "enter_legacy_diagnostic_safe_vent_mode", None) if pace is not None else None
    if not callable(helper):
        raise RuntimeError(
            "legacy_safe_vent_blocked("
            f"action={action},step=helper_unavailable,last_status=unknown,output_state=unknown,"
            "isolation_state=unknown,recoverable=true)"
        )

    _record_event(
        actuation_events,
        run_id=run_id,
        process_variant=process_variant,
        layer=layer,
        repeat_index=repeat_index,
        gas_ppm=gas_ppm,
        chain_mode=chain_mode,
        pressure_target_hpa=None,
        event_type="legacy_safe_vent_begin",
        event_value=action,
        note=reason,
    )
    try:
        result = dict(
            helper(
                timeout_s=_legacy_safe_vent_timeout_s(cfg),
                poll_s=0.25,
                action=action,
            )
        )
    except Exception as exc:
        raise RuntimeError(
            "legacy_safe_vent_blocked("
            f"action={action},step=helper_exception,last_status=unknown,output_state=unknown,"
            f"isolation_state=unknown,recoverable=true,error={exc})"
        ) from exc

    event_type = "legacy_safe_vent_observed" if bool(result.get("ok")) else "legacy_safe_vent_blocked"
    _record_event(
        actuation_events,
        run_id=run_id,
        process_variant=process_variant,
        layer=layer,
        repeat_index=repeat_index,
        gas_ppm=gas_ppm,
        chain_mode=chain_mode,
        pressure_target_hpa=None,
        event_type=event_type,
        event_value=result.get("after_status"),
        note=str(result.get("reason") or reason),
    )
    if not bool(result.get("ok")):
        raise RuntimeError(str(result.get("reason") or "legacy_safe_vent_blocked"))

    _log(f"[CHAIN] legacy safe vent ok | action={action} reason={result.get('reason')}")
    return result


def _legacy_safe_vent_refresh_block_reason(
    pace: Any,
    *,
    action: str,
    last_status: Optional[int],
    output_state: Optional[int],
    isolation_state: Optional[int],
    step: str,
) -> str:
    builder = getattr(pace, "_legacy_safe_vent_block_error", None)
    if callable(builder):
        try:
            return str(
                builder(
                    action=action,
                    last_status=last_status,
                    output_state=output_state,
                    isolation_state=isolation_state,
                    step=step,
                )
            )
        except Exception:
            pass
    return (
        "legacy_safe_vent_blocked("
        f"action={action},step={step},"
        f"last_status={last_status if last_status is not None else 'unknown'},"
        f"output_state={output_state if output_state is not None else 'unknown'},"
        f"isolation_state={isolation_state if isolation_state is not None else 'unknown'},"
        "recoverable=true)"
    )


def _run_legacy_safe_vent_refresh_action(
    runner: CalibrationRunner,
    *,
    action: str,
) -> Dict[str, Any]:
    pace = runner.devices.get("pace")
    if pace is None:
        raise RuntimeError(
            "legacy_safe_vent_blocked("
            f"action={action},step=pace_missing,last_status=unknown,output_state=unknown,"
            "isolation_state=unknown,recoverable=true)"
        )
    lock = getattr(pace, "_cmd_lock", None)
    if lock is None or not hasattr(lock, "__enter__"):
        raise RuntimeError(
            "legacy_safe_vent_blocked("
            f"action={action},step=pace_command_lock_missing,last_status=unknown,output_state=unknown,"
            "isolation_state=unknown,recoverable=true)"
        )

    legacy_identity_checker = getattr(pace, "has_legacy_vent_status_model", None)
    legacy_identity = bool(legacy_identity_checker()) if callable(legacy_identity_checker) else False
    result = {
        "action": action,
        "legacy_identity": legacy_identity,
        "ok": False,
        "recoverable": True,
        "reason": "",
        "vent_command_sent": False,
        "before_status": None,
        "after_status": None,
        "output_state": 0,
        "isolation_state": 1,
    }
    if not legacy_identity:
        result["reason"] = "non_legacy_identity"
        return result

    with lock:
        stop_hold = getattr(pace, "stop_atmosphere_hold", None)
        if callable(stop_hold):
            stop_hold()
        try:
            pace.set_output(False)
            pace.set_isolation_open(True)
            before_status = pace.get_vent_status()
        except Exception as exc:
            result["reason"] = _legacy_safe_vent_refresh_block_reason(
                pace,
                action=action,
                last_status=result.get("before_status"),
                output_state=0,
                isolation_state=1,
                step=f"prepare:{exc}",
            )
            return result

        result["before_status"] = before_status
        result["after_status"] = before_status
        if before_status == Pace5000.VENT_STATUS_TRAPPED_PRESSURE:
            result["reason"] = _legacy_safe_vent_refresh_block_reason(
                pace,
                action=action,
                last_status=before_status,
                output_state=0,
                isolation_state=1,
                step="watchlist_status_3",
            )
            return result
        if before_status in {Pace5000.VENT_STATUS_IDLE, Pace5000.VENT_STATUS_COMPLETED}:
            try:
                pace.vent(True)
            except Exception as exc:
                result["reason"] = _legacy_safe_vent_refresh_block_reason(
                    pace,
                    action=action,
                    last_status=before_status,
                    output_state=0,
                    isolation_state=1,
                    step=f"vent_on:{exc}",
                )
                return result
            result["vent_command_sent"] = True
            result["after_status"] = Pace5000.VENT_STATUS_IN_PROGRESS
        elif before_status != Pace5000.VENT_STATUS_IN_PROGRESS:
            result["reason"] = _legacy_safe_vent_refresh_block_reason(
                pace,
                action=action,
                last_status=before_status,
                output_state=0,
                isolation_state=1,
                step="unsupported_status",
            )
            return result

    result["ok"] = True
    result["reason"] = (
        "legacy_safe_vent_refresh_scheduled("
        f"action={action},last_status={result.get('after_status')},"
        f"vent_command_sent={str(bool(result.get('vent_command_sent'))).lower()})"
    )
    return result


def _update_flush_vent_refresh_metadata(
    setup_metadata: Optional[Dict[str, Any]],
    *,
    requested_interval_s: float,
    actual_intervals_s: Sequence[float],
    refresh_count: int,
    thread_used: bool,
) -> None:
    if setup_metadata is None:
        return
    actual_mean = (
        round(sum(float(item) for item in actual_intervals_s) / len(actual_intervals_s), 6)
        if actual_intervals_s
        else None
    )
    actual_max = round(max(float(item) for item in actual_intervals_s), 6) if actual_intervals_s else None
    setup_metadata.update(
        {
            "flush_vent_refresh_interval_s": requested_interval_s,
            "flush_vent_refresh_interval_s_requested": requested_interval_s,
            "flush_vent_refresh_interval_s_actual_mean": actual_mean,
            "flush_vent_refresh_interval_s_actual_max": actual_max,
            "flush_vent_refresh_count": int(refresh_count),
            "flush_vent_refresh_thread_used": bool(thread_used),
        }
    )


def _flush_vent_refresh_extra_fields(payload: Mapping[str, Any]) -> Dict[str, Any]:
    return {
        "flush_vent_refresh_sequence": int(payload.get("sequence") or 0),
        "flush_vent_refresh_interval_s_requested": _safe_float(payload.get("requested_interval_s")),
        "flush_vent_refresh_interval_s_actual": _safe_float(payload.get("actual_interval_s")),
        "flush_vent_refresh_duration_s": _safe_float(payload.get("duration_s")),
        "flush_vent_refresh_thread_used": bool(payload.get("thread_used")),
        "flush_vent_refresh_before_status": _safe_int(payload.get("before_status")),
        "flush_vent_refresh_after_status": _safe_int(payload.get("after_status")),
        "flush_vent_refresh_vent_command_sent": payload.get("vent_command_sent"),
        "flush_vent_refresh_completed_timestamp": payload.get("completed_timestamp"),
    }


class _FlushVentRefreshHeartbeat:
    def __init__(
        self,
        *,
        interval_s: float,
        pace: Any,
        phase_label: str,
        phase_start_mono: float,
        action: str,
        handler: Callable[[str], Mapping[str, Any] | None],
        warning_failure_threshold: int = 3,
    ) -> None:
        self._interval_s = max(0.0, float(interval_s))
        self._pace = pace
        self._phase_label = str(phase_label or "flush")
        self._phase_start_mono = float(phase_start_mono)
        self._action = str(action)
        self._handler = handler
        self._warning_failure_threshold = max(1, int(warning_failure_threshold))
        self._pace_lock = getattr(pace, "_cmd_lock", None)
        self._pending_lock = threading.Lock()
        self._pending_events: List[Dict[str, Any]] = []
        self._stop_event = threading.Event()
        self._started_event = threading.Event()
        self._fatal_error: Optional[str] = None
        self._thread: Optional[threading.Thread] = None
        self._sequence = 0
        self._last_begin_mono: Optional[float] = None

    def start(self) -> None:
        if self._interval_s <= 0.0:
            return
        if not callable(self._handler):
            raise RuntimeError("flush_vent_refresh_heartbeat_start_failed(handler_unavailable)")
        if self._pace is None:
            raise RuntimeError("flush_vent_refresh_heartbeat_start_failed(pace_unavailable)")
        if self._pace_lock is None or not hasattr(self._pace_lock, "__enter__"):
            raise RuntimeError("flush_vent_refresh_heartbeat_start_failed(pace_command_lock_unavailable)")
        self._thread = threading.Thread(
            target=self._run,
            name=f"flush-vent-refresh-{self._action}",
            daemon=False,
        )
        self._thread.start()
        if not self._started_event.wait(timeout=max(1.0, self._interval_s * 2.0 + 0.5)):
            raise RuntimeError("flush_vent_refresh_heartbeat_start_failed(thread_not_started)")
        if self._fatal_error:
            raise RuntimeError(self._fatal_error)

    def drain_events(self) -> List[Dict[str, Any]]:
        with self._pending_lock:
            out = list(self._pending_events)
            self._pending_events.clear()
        return out

    def stop(self) -> List[Dict[str, Any]]:
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=max(2.0, self._interval_s * 2.0 + 1.0))
            if self._thread.is_alive():
                raise RuntimeError("flush_vent_refresh_heartbeat_stop_timeout")
        pending = self.drain_events()
        if self._fatal_error:
            raise RuntimeError(self._fatal_error)
        return pending

    def _append_pending(self, payload: Mapping[str, Any]) -> None:
        with self._pending_lock:
            self._pending_events.append(dict(payload))

    def _run(self) -> None:
        try:
            next_due_mono = self._phase_start_mono + self._interval_s
            consecutive_failures = 0
            self._started_event.set()
            while not self._stop_event.is_set():
                wait_s = max(0.0, next_due_mono - time.monotonic())
                if self._stop_event.wait(wait_s):
                    break

                begin_mono = time.monotonic()
                begin_ts = datetime.now().isoformat(timespec="milliseconds")
                elapsed_real_s = max(0.0, begin_mono - self._phase_start_mono)
                self._sequence += 1
                actual_interval_s = None if self._last_begin_mono is None else begin_mono - self._last_begin_mono
                self._last_begin_mono = begin_mono
                refresh_label = f"{elapsed_real_s:.1f}s"
                refresh_reason = f"metrology {self._phase_label} refresh @{refresh_label}"

                result: Dict[str, Any]
                ok = False
                try:
                    with self._pace_lock:
                        raw_result = self._handler(refresh_reason)
                    result = dict(raw_result) if isinstance(raw_result, Mapping) else {}
                    ok = bool(result.get("ok", True))
                except Exception as exc:
                    result = {
                        "action": self._action,
                        "ok": False,
                        "reason": str(exc),
                        "vent_command_sent": False,
                        "before_status": None,
                        "after_status": None,
                    }

                end_mono = time.monotonic()
                end_ts = datetime.now().isoformat(timespec="milliseconds")
                payload = {
                    "type": "refresh",
                    "action": str(result.get("action") or self._action),
                    "sequence": self._sequence,
                    "timestamp": begin_ts,
                    "completed_timestamp": end_ts,
                    "elapsed_real_s": elapsed_real_s,
                    "requested_interval_s": self._interval_s,
                    "actual_interval_s": actual_interval_s,
                    "duration_s": max(0.0, end_mono - begin_mono),
                    "refresh_label": refresh_label,
                    "refresh_reason": refresh_reason,
                    "thread_used": True,
                    "ok": ok,
                    "reason": str(result.get("reason") or refresh_reason),
                    "vent_command_sent": result.get("vent_command_sent"),
                    "before_status": result.get("before_status"),
                    "after_status": result.get("after_status"),
                }
                self._append_pending(payload)

                if ok:
                    consecutive_failures = 0
                else:
                    consecutive_failures += 1
                    if consecutive_failures >= self._warning_failure_threshold:
                        self._append_pending(
                            {
                                "type": "warning",
                                "timestamp": end_ts,
                                "elapsed_real_s": max(0.0, end_mono - self._phase_start_mono),
                                "requested_interval_s": self._interval_s,
                                "thread_used": True,
                                "consecutive_failures": consecutive_failures,
                                "reason": str(result.get("reason") or "flush_vent_refresh_failed"),
                            }
                        )

                next_due_mono += self._interval_s
                while next_due_mono <= time.monotonic():
                    next_due_mono += self._interval_s
        except Exception as exc:
            self._fatal_error = f"flush_vent_refresh_heartbeat_failed({exc})"
            self._append_pending(
                {
                    "type": "fatal",
                    "timestamp": datetime.now().isoformat(timespec="milliseconds"),
                    "reason": self._fatal_error,
                    "thread_used": True,
                }
            )
        finally:
            self._started_event.set()


def _drain_flush_vent_refresh_heartbeat_events(
    payloads: Sequence[Mapping[str, Any]],
    *,
    rows: Sequence[Mapping[str, Any]],
    actuation_events: List[Dict[str, Any]],
    flush_gate_trace_rows: List[Dict[str, Any]],
    run_id: str,
    process_variant: str,
    layer: int,
    repeat_index: int,
    gas_ppm: int,
    phase_name: str,
    phase_label: str,
    gate_window_s: float,
    min_flush_s: float,
    target_flush_s: float,
    require_ratio: bool,
    chain_mode: Optional[str],
    refresh_stats: Dict[str, Any],
    requested_interval_s: float,
    setup_metadata: Optional[Dict[str, Any]],
) -> None:
    for payload in payloads:
        payload_type = str(payload.get("type") or "")
        if payload_type == "fatal":
            raise RuntimeError(str(payload.get("reason") or "flush_vent_refresh_heartbeat_failed"))

        extra_fields = _flush_vent_refresh_extra_fields(payload)
        if payload_type == "refresh":
            action = str(payload.get("action") or "analyzer_chain_flush_vent_refresh")
            refresh_reason = str(payload.get("refresh_reason") or payload.get("reason") or "")
            begin_ts = str(payload.get("timestamp") or datetime.now().isoformat(timespec="milliseconds"))
            end_ts = str(payload.get("completed_timestamp") or begin_ts)
            elapsed_real_s = max(0.0, float(payload.get("elapsed_real_s") or 0.0))
            refresh_label = str(payload.get("refresh_label") or f"{elapsed_real_s:.1f}s")

            _record_event(
                actuation_events,
                run_id=run_id,
                process_variant=process_variant,
                layer=layer,
                repeat_index=repeat_index,
                gas_ppm=gas_ppm,
                chain_mode=chain_mode,
                pressure_target_hpa=None,
                event_type="legacy_safe_vent_begin",
                event_value=action,
                note=refresh_reason,
                timestamp=begin_ts,
                extra_fields=extra_fields,
            )
            if bool(payload.get("ok")):
                _record_event(
                    actuation_events,
                    run_id=run_id,
                    process_variant=process_variant,
                    layer=layer,
                    repeat_index=repeat_index,
                    gas_ppm=gas_ppm,
                    chain_mode=chain_mode,
                    pressure_target_hpa=None,
                    event_type="legacy_safe_vent_observed",
                    event_value=payload.get("after_status"),
                    note=str(payload.get("reason") or refresh_reason),
                    timestamp=end_ts,
                    extra_fields=extra_fields,
                )
                _record_event(
                    actuation_events,
                    run_id=run_id,
                    process_variant=process_variant,
                    layer=layer,
                    repeat_index=repeat_index,
                    gas_ppm=gas_ppm,
                    chain_mode=chain_mode,
                    pressure_target_hpa=None,
                    event_type="vent_refresh",
                    event_value="VENT_ON_REFRESH",
                    note=f"{phase_label} refresh @{refresh_label}",
                    timestamp=end_ts,
                    extra_fields=extra_fields,
                )
                trace_row = _build_flush_gate_trace_row(
                    rows,
                    process_variant=process_variant,
                    layer=layer,
                    repeat_index=repeat_index,
                    gas_ppm=gas_ppm,
                    phase=phase_name,
                    elapsed_real_s=elapsed_real_s,
                    gate_window_s=gate_window_s,
                    min_flush_s=min_flush_s,
                    target_flush_s=target_flush_s,
                    note=f"{phase_label}_vent_refresh_{refresh_label}",
                    require_ratio=require_ratio,
                    chain_mode=chain_mode,
                )
                trace_row["timestamp"] = begin_ts
                trace_row.update(extra_fields)
                flush_gate_trace_rows.append(trace_row)
                _log_flush_gate_snapshot(trace_row)

                refresh_stats["count"] = int(refresh_stats.get("count", 0)) + 1
                actual_interval_s = _safe_float(payload.get("actual_interval_s"))
                if actual_interval_s is not None:
                    refresh_stats.setdefault("actual_intervals_s", []).append(actual_interval_s)
            else:
                block_reason = str(payload.get("reason") or refresh_reason or "flush_vent_refresh_failed")
                _log(f"[WARN] {block_reason}")
                _record_event(
                    actuation_events,
                    run_id=run_id,
                    process_variant=process_variant,
                    layer=layer,
                    repeat_index=repeat_index,
                    gas_ppm=gas_ppm,
                    chain_mode=chain_mode,
                    pressure_target_hpa=None,
                    event_type="legacy_safe_vent_blocked",
                    event_value=payload.get("after_status"),
                    note=block_reason,
                    timestamp=end_ts,
                    extra_fields=extra_fields,
                )
                _record_event(
                    actuation_events,
                    run_id=run_id,
                    process_variant=process_variant,
                    layer=layer,
                    repeat_index=repeat_index,
                    gas_ppm=gas_ppm,
                    chain_mode=chain_mode,
                    pressure_target_hpa=None,
                    event_type="vent_refresh_warning",
                    event_value="heartbeat_refresh_failed",
                    note=block_reason,
                    timestamp=end_ts,
                    extra_fields=extra_fields,
                )
        elif payload_type == "warning":
            warning_note = (
                f"{phase_label}_refresh_consecutive_failures="
                f"{int(payload.get('consecutive_failures') or 0)} reason={payload.get('reason')}"
            )
            _log(f"[WARN] {warning_note}")
            _record_event(
                actuation_events,
                run_id=run_id,
                process_variant=process_variant,
                layer=layer,
                repeat_index=repeat_index,
                gas_ppm=gas_ppm,
                chain_mode=chain_mode,
                pressure_target_hpa=None,
                event_type="vent_refresh_warning",
                event_value="heartbeat_consecutive_failures",
                note=warning_note,
                timestamp=str(payload.get("timestamp") or datetime.now().isoformat(timespec="milliseconds")),
                extra_fields={
                    "flush_vent_refresh_interval_s_requested": _safe_float(payload.get("requested_interval_s")),
                    "flush_vent_refresh_thread_used": bool(payload.get("thread_used")),
                    "flush_vent_refresh_consecutive_failures": int(payload.get("consecutive_failures") or 0),
                },
            )

    _update_flush_vent_refresh_metadata(
        setup_metadata,
        requested_interval_s=requested_interval_s,
        actual_intervals_s=list(refresh_stats.get("actual_intervals_s") or []),
        refresh_count=int(refresh_stats.get("count", 0)),
        thread_used=bool(refresh_stats.get("thread_used")),
    )


def _apply_analyzer_chain_pressure_baseline(
    runner: CalibrationRunner,
    *,
    cfg: Mapping[str, Any],
    reason: str,
    legacy_safe_vent_active: bool,
    run_id: str,
    actuation_events: List[Dict[str, Any]],
    process_variant: str,
    layer: int,
    repeat_index: int,
    gas_ppm: Optional[int],
    chain_mode: Optional[str],
    action: str,
) -> Dict[str, Any]:
    if not legacy_safe_vent_active:
        runner._set_co2_route_baseline(reason=reason)
        return {"ok": True, "mode": "default", "reason": reason}

    result = _run_legacy_safe_vent_action(
        runner,
        cfg=cfg,
        action=action,
        reason=reason,
        run_id=run_id,
        actuation_events=actuation_events,
        process_variant=process_variant,
        layer=layer,
        repeat_index=repeat_index,
        gas_ppm=gas_ppm,
        chain_mode=chain_mode,
    )
    runner._apply_route_baseline_valves()
    _log("Managed relay/pressure baseline restored with legacy diagnostic-safe vent.")
    return result


def _configure_analyzer_chain_capture_startup(
    runner: CalibrationRunner,
    *,
    analyzer: Optional[GasAnalyzer],
    analyzer_name: str,
    analyzer_cfg: Mapping[str, Any],
    setup_metadata: Dict[str, Any],
) -> Dict[str, Any]:
    analyzers_in_path = list(setup_metadata.get("analyzers_in_path") or [])
    non_capture_names = [
        str(item.get("name") or "").strip()
        for item in analyzers_in_path
        if isinstance(item, Mapping) and str(item.get("name") or "").strip() and str(item.get("name") or "").strip() != str(analyzer_name)
    ]
    startup_summary = {
        "analyzer_startup_gate_policy": "capture_analyzer_only",
        "capture_analyzer_startup_label": str(analyzer_name or ""),
        "capture_analyzer_startup_status": "pending",
        "capture_analyzer_startup_fail_reason": "",
        "non_capture_analyzers_skipped": list(non_capture_names),
        "non_capture_analyzer_startup_policy": "not_required_for_diagnostic_only",
    }
    setup_metadata.update(startup_summary)

    if non_capture_names:
        _log(
            "[CHAIN] diagnostic-only startup gate uses capture analyzer only; "
            f"non-capture analyzers not startup-gated: {', '.join(non_capture_names)}"
        )

    if analyzer is None:
        reason = (
            "all_analyzers_unavailable("
            f"capture_analyzer={analyzer_name},reason=capture_analyzer_missing)"
        )
        startup_summary["capture_analyzer_startup_status"] = "fail"
        startup_summary["capture_analyzer_startup_fail_reason"] = reason
        setup_metadata.update(startup_summary)
        raise RuntimeError(reason)

    settings = runner._gas_analyzer_runtime_settings(dict(analyzer_cfg) if isinstance(analyzer_cfg, Mapping) else {})
    try:
        runner._configure_gas_analyzer(
            analyzer,
            label=str(analyzer_name or "gas_analyzer"),
            mode=int(settings["mode"]),
            active_send=bool(settings["active_send"]),
            ftd_hz=int(settings["ftd_hz"]),
            avg_co2=int(settings["avg_co2"]),
            avg_h2o=int(settings["avg_h2o"]),
            avg_filter=int(settings["avg_filter"]),
            warning_phase="startup",
        )
        runner._disabled_analyzers.discard(str(analyzer_name or ""))
        runner._disabled_analyzer_reasons.pop(str(analyzer_name or ""), None)
        runner._disabled_analyzer_last_reprobe_ts.pop(str(analyzer_name or ""), None)
    except Exception as exc:
        runner._disable_analyzers([str(analyzer_name or "gas_analyzer")], reason="capture_analyzer_startup_failed")
        runner._disabled_analyzer_last_reprobe_ts[str(analyzer_name or "gas_analyzer")] = time.time()
        reason = f"capture_analyzer_startup_failed(name={analyzer_name},reason={exc})"
        startup_summary["capture_analyzer_startup_status"] = "fail"
        startup_summary["capture_analyzer_startup_fail_reason"] = reason
        setup_metadata.update(startup_summary)
        raise RuntimeError(reason) from exc

    startup_summary["capture_analyzer_startup_status"] = "pass"
    setup_metadata.update(startup_summary)
    return startup_summary


def _perform_runtime_safe_stop(
    devices: Mapping[str, Any],
    *,
    runner: Optional[CalibrationRunner] = None,
    cfg: Mapping[str, Any],
    run_id: str,
    actuation_events: List[Dict[str, Any]],
    process_variant: str,
    layer: int,
    repeat_index: int,
    gas_ppm: Optional[int],
    note: str,
    chain_mode: Optional[str] = None,
    pace_in_path: bool = True,
    pace_mode: str = "default",
) -> Dict[str, Any]:
    _record_event(
        actuation_events,
        run_id=run_id,
        process_variant=process_variant,
        layer=layer,
        repeat_index=repeat_index,
        gas_ppm=gas_ppm,
        chain_mode=chain_mode,
        pressure_target_hpa=None,
        event_type="safe_stop_begin",
        event_value="safe_stop_begin",
        note=note,
    )
    result: Dict[str, Any] = {}
    if pace_in_path:
        result = perform_safe_stop_with_retries(
            dict(devices),
            cfg=dict(cfg),
            pace_mode=pace_mode,
            attempts=2,
            retry_delay_s=1.0,
            log_fn=_log,
        )
    elif runner is not None:
        runner._apply_route_baseline_valves()
        _log("PACE 不在气路中：safe stop 仅恢复气路阀位基线。")
    _record_event(
        actuation_events,
        run_id=run_id,
        process_variant=process_variant,
        layer=layer,
        repeat_index=repeat_index,
        gas_ppm=gas_ppm,
        chain_mode=chain_mode,
        pressure_target_hpa=None,
        event_type="safe_stop_end",
        event_value="safe_stop_end",
        note=note,
    )
    return result


def _switch_gas_route(
    runner: CalibrationRunner,
    analyzer: Optional[GasAnalyzer],
    devices: Mapping[str, Any],
    *,
    previous_route: Optional[Mapping[str, Any]],
    next_route: Mapping[str, Any],
    process_variant: str,
    layer: int,
    repeat_index: int,
    gas_ppm: int,
    gas_start_mono: float,
    sample_poll_s: float,
    print_every_s: float,
    expected_deadtime_s: float,
    actuation_events: List[Dict[str, Any]],
    run_id: str,
    chain_mode: Optional[str] = None,
    controller_vent_state_label: str = "VENT_ON",
    pace_in_path: bool = True,
    initial_baseline_handler: Optional[Callable[[], None]] = None,
) -> Tuple[List[Dict[str, Any]], float]:
    if previous_route is None:
        if pace_in_path:
            if callable(initial_baseline_handler):
                initial_baseline_handler()
            else:
                runner._set_co2_route_baseline(reason=f"{process_variant} layer{layer} repeat{repeat_index} first gas")
        else:
            runner._apply_route_baseline_valves()
        runner._apply_valve_states(next_route["open_logical_valves"])
        _record_event(
            actuation_events,
            run_id=run_id,
            process_variant=process_variant,
            layer=layer,
            repeat_index=repeat_index,
            gas_ppm=gas_ppm,
            chain_mode=chain_mode,
            pressure_target_hpa=None,
            event_type="gas_source_open",
            event_value=next_route.get("source_valve"),
            note="first gas route open",
        )
        return [], 0.0

    deadtime_open = _intermediate_open_valves(previous_route, next_route)
    _record_event(
        actuation_events,
        run_id=run_id,
        process_variant=process_variant,
        layer=layer,
        repeat_index=repeat_index,
        gas_ppm=previous_route.get("gas_ppm"),
        chain_mode=chain_mode,
        pressure_target_hpa=None,
        event_type="gas_source_close",
        event_value=previous_route.get("source_valve"),
        note=f"switch to {gas_ppm} ppm",
    )
    _record_event(
        actuation_events,
        run_id=run_id,
        process_variant=process_variant,
        layer=layer,
        repeat_index=repeat_index,
        gas_ppm=gas_ppm,
        chain_mode=chain_mode,
        pressure_target_hpa=None,
        event_type="gas_switch_deadtime_start",
        event_value=expected_deadtime_s,
        note=f"from {previous_route.get('gas_ppm')} ppm to {gas_ppm} ppm",
    )
    runner._apply_valve_states(deadtime_open)
    start = time.monotonic()
    context = _phase_context(
        process_variant=process_variant,
        layer=layer,
        repeat_index=repeat_index,
        phase="gas_switch_deadtime",
        gas_ppm=gas_ppm,
        route=next_route,
        actual_deadtime_s=expected_deadtime_s,
        chain_mode=chain_mode,
    )
    deadtime_rows = _capture_phase_rows(
        analyzer,
        devices,
        context=context,
        gas_start_mono=gas_start_mono,
        duration_s=float(expected_deadtime_s),
        sample_poll_s=sample_poll_s,
        print_every_s=max(1.0, print_every_s),
        controller_vent_state=controller_vent_state_label,
    )
    actual_deadtime_s = max(0.0, time.monotonic() - start)
    _apply_gate_to_rows(deadtime_rows, gate_pass=True, gate_fail_reason="")
    for row in deadtime_rows:
        row["actual_deadtime_s"] = actual_deadtime_s
    runner._apply_valve_states(next_route["open_logical_valves"])
    _record_event(
        actuation_events,
        run_id=run_id,
        process_variant=process_variant,
        layer=layer,
        repeat_index=repeat_index,
        gas_ppm=gas_ppm,
        chain_mode=chain_mode,
        pressure_target_hpa=None,
        event_type="gas_switch_deadtime_end",
        event_value=actual_deadtime_s,
        note=f"deadtime target={expected_deadtime_s}",
    )
    _record_event(
        actuation_events,
        run_id=run_id,
        process_variant=process_variant,
        layer=layer,
        repeat_index=repeat_index,
        gas_ppm=gas_ppm,
        chain_mode=chain_mode,
        pressure_target_hpa=None,
        event_type="gas_source_open",
        event_value=next_route.get("source_valve"),
        note=f"open {gas_ppm} ppm source",
    )
    return deadtime_rows, actual_deadtime_s


def _run_flush_phase(
    runner: CalibrationRunner,
    analyzer: Optional[GasAnalyzer],
    devices: Mapping[str, Any],
    *,
    process_variant: str,
    layer: int,
    repeat_index: int,
    gas_ppm: int,
    route: Mapping[str, Any],
    gas_start_mono: float,
    min_flush_s: float,
    target_flush_s: float,
    max_flush_s: float,
    gate_window_s: float,
    rebound_window_s: float,
    rebound_min_rise_c: float,
    sample_poll_s: float,
    print_every_s: float,
    actual_deadtime_s: Optional[float],
    actuation_events: List[Dict[str, Any]],
    flush_gate_trace_rows: List[Dict[str, Any]],
    run_id: str,
    require_ratio: bool = True,
    require_vent_on: bool = True,
    controller_vent_state_label: str = "VENT_ON",
    phase_name_override: Optional[str] = None,
    gate_name_override: Optional[str] = None,
    phase_label_override: Optional[str] = None,
    chain_mode: Optional[str] = None,
    vent_on_handler: Optional[Callable[[str], None]] = None,
    flush_vent_refresh_interval_s: float = 0.0,
    vent_refresh_handler: Optional[Callable[[str], Mapping[str, Any] | None]] = None,
    flush_vent_refresh_wall_clock_heartbeat: bool = False,
    setup_metadata: Optional[Dict[str, Any]] = None,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any], Dict[str, Any], str]:
    precondition_mode = layer == 0
    phase_name = phase_name_override or ("precondition_vent_on" if precondition_mode else "gas_flush_vent_on")
    gate_name = gate_name_override or ("precondition_gate" if precondition_mode else "flush_gate")
    phase_label = phase_label_override or ("precondition" if precondition_mode else "flush")
    if precondition_mode:
        _record_event(
            actuation_events,
            run_id=run_id,
            process_variant=process_variant,
            layer=layer,
            repeat_index=repeat_index,
            gas_ppm=gas_ppm,
            pressure_target_hpa=None,
            event_type="precondition_start",
            event_value="precondition_start",
            note="0 ppm precondition begin",
        )
    if require_vent_on:
        vent_reason = f"metrology {phase_label} gate"
        if callable(vent_on_handler):
            vent_on_handler(vent_reason)
        else:
            runner._set_pressure_controller_vent(True, reason=vent_reason)
        _record_event(
            actuation_events,
            run_id=run_id,
            process_variant=process_variant,
            layer=layer,
            repeat_index=repeat_index,
            gas_ppm=gas_ppm,
            pressure_target_hpa=None,
            event_type="vent_on",
            event_value="VENT_ON",
            note=f"{phase_label} phase start",
        )
    rows: List[Dict[str, Any]] = []
    phase_start_mono = time.monotonic()
    chunk_s = min(15.0, max(5.0, gate_window_s / 3.0))
    refresh_interval_s = max(0.0, float(flush_vent_refresh_interval_s or 0.0))
    use_threaded_refresh = bool(
        flush_vent_refresh_wall_clock_heartbeat
        and refresh_interval_s > 0.0
        and callable(vent_refresh_handler)
    )
    if refresh_interval_s > 0.0 and not use_threaded_refresh:
        chunk_s = min(chunk_s, max(0.5, refresh_interval_s))
    flush_vent_refresh_count = 0
    refresh_limit_warning_emitted = False
    stalled_sampling_warning_emitted = False
    identical_sample_streak = 0
    last_sample_signature: Optional[Tuple[Optional[float], Optional[float], Optional[float], Optional[float]]] = None
    max_refresh_count = (
        max(1, int(math.ceil(float(max_flush_s) / refresh_interval_s)) + 2)
        if refresh_interval_s > 0.0
        else 0
    )
    refresh_stats: Dict[str, Any] = {
        "count": 0,
        "actual_intervals_s": [],
        "thread_used": use_threaded_refresh,
    }
    _update_flush_vent_refresh_metadata(
        setup_metadata,
        requested_interval_s=refresh_interval_s,
        actual_intervals_s=[],
        refresh_count=flush_vent_refresh_count,
        thread_used=use_threaded_refresh,
    )
    next_trace_elapsed_s = 30.0
    next_refresh_elapsed_s = refresh_interval_s if refresh_interval_s > 0.0 and not use_threaded_refresh else None
    first_gate_eval_logged = False
    flush_gate_trace_rows.append(
        _build_flush_gate_trace_row(
            rows,
            process_variant=process_variant,
            layer=layer,
            repeat_index=repeat_index,
            gas_ppm=gas_ppm,
            phase=phase_name,
            elapsed_real_s=0.0,
            gate_window_s=gate_window_s,
            min_flush_s=min_flush_s,
            target_flush_s=target_flush_s,
            note=f"{phase_label}_start",
            require_ratio=require_ratio,
            chain_mode=chain_mode,
        )
    )
    _log_flush_gate_snapshot(flush_gate_trace_rows[-1])
    heartbeat: Optional[_FlushVentRefreshHeartbeat] = None
    if use_threaded_refresh:
        heartbeat = _FlushVentRefreshHeartbeat(
            interval_s=refresh_interval_s,
            pace=devices.get("pace"),
            phase_label=phase_label,
            phase_start_mono=phase_start_mono,
            action="analyzer_chain_flush_vent_refresh",
            handler=vent_refresh_handler,
        )
        heartbeat.start()

    summary: Dict[str, Any] = {}
    gate_row: Dict[str, Any] = {}
    flush_status = ""

    def _drain_pending_refresh_events() -> None:
        if heartbeat is None:
            return
        payloads = heartbeat.drain_events()
        if payloads:
            _drain_flush_vent_refresh_heartbeat_events(
                payloads,
                rows=rows,
                actuation_events=actuation_events,
                flush_gate_trace_rows=flush_gate_trace_rows,
                run_id=run_id,
                process_variant=process_variant,
                layer=layer,
                repeat_index=repeat_index,
                gas_ppm=gas_ppm,
                phase_name=phase_name,
                phase_label=phase_label,
                gate_window_s=gate_window_s,
                min_flush_s=min_flush_s,
                target_flush_s=target_flush_s,
                require_ratio=require_ratio,
                chain_mode=chain_mode,
                refresh_stats=refresh_stats,
                requested_interval_s=refresh_interval_s,
                setup_metadata=setup_metadata,
            )

    try:
        while True:
            elapsed_real_s = max(0.0, time.monotonic() - phase_start_mono)
            remaining_s = float(max_flush_s) - elapsed_real_s
            if remaining_s <= 0.0:
                break
            duration_s = min(chunk_s, remaining_s)
            context = _phase_context(
                process_variant=process_variant,
                layer=layer,
                repeat_index=repeat_index,
                phase=phase_name,
                gas_ppm=gas_ppm,
                route=route,
                actual_deadtime_s=actual_deadtime_s,
                chain_mode=chain_mode,
            )
            chunk_rows = _capture_phase_rows(
                analyzer,
                devices,
                context=context,
                gas_start_mono=gas_start_mono,
                duration_s=duration_s,
                sample_poll_s=sample_poll_s,
                print_every_s=print_every_s,
                controller_vent_state=controller_vent_state_label,
                phase_elapsed_offset_s=elapsed_real_s,
            )
            rows.extend(chunk_rows)
            elapsed_real_s = max(0.0, time.monotonic() - phase_start_mono)
            _drain_pending_refresh_events()
            if chunk_rows:
                latest_chunk_row = chunk_rows[-1]
                sample_signature = (
                    _safe_float(latest_chunk_row.get("phase_elapsed_s")),
                    _safe_float(latest_chunk_row.get("gauge_pressure_hpa")),
                    _safe_float(latest_chunk_row.get("controller_pressure_hpa")),
                    _safe_float(latest_chunk_row.get("dewpoint_c")),
                )
                if sample_signature == last_sample_signature:
                    identical_sample_streak += 1
                else:
                    identical_sample_streak = 1
                    last_sample_signature = sample_signature
                if (
                    refresh_interval_s > 0.0
                    and identical_sample_streak >= 3
                    and not stalled_sampling_warning_emitted
                ):
                    warning_note = (
                        f"{phase_label}_refresh_sampling_stalled "
                        f"elapsed_real={elapsed_real_s:.3f}s elapsed_display={sample_signature[0]}"
                    )
                    _log(f"[WARN] {warning_note}")
                    _record_event(
                        actuation_events,
                        run_id=run_id,
                        process_variant=process_variant,
                        layer=layer,
                        repeat_index=repeat_index,
                        gas_ppm=gas_ppm,
                        pressure_target_hpa=None,
                        event_type="vent_refresh_warning",
                        event_value="sampling_stalled",
                        note=warning_note,
                        chain_mode=chain_mode,
                    )
                    flush_gate_trace_rows.append(
                        _build_flush_gate_trace_row(
                            rows,
                            process_variant=process_variant,
                            layer=layer,
                            repeat_index=repeat_index,
                            gas_ppm=gas_ppm,
                            phase=phase_name,
                            elapsed_real_s=elapsed_real_s,
                            gate_window_s=gate_window_s,
                            min_flush_s=min_flush_s,
                            target_flush_s=target_flush_s,
                            note=warning_note,
                            require_ratio=require_ratio,
                            chain_mode=chain_mode,
                        )
                    )
                    _log_flush_gate_snapshot(flush_gate_trace_rows[-1])
                    stalled_sampling_warning_emitted = True
            if next_refresh_elapsed_s is not None and elapsed_real_s >= next_refresh_elapsed_s:
                if callable(vent_refresh_handler):
                    if flush_vent_refresh_count >= max_refresh_count:
                        if not refresh_limit_warning_emitted:
                            warning_note = (
                                f"{phase_label}_refresh_limit_reached "
                                f"count={flush_vent_refresh_count} max={max_refresh_count}"
                            )
                            _log(f"[WARN] {warning_note}")
                            _record_event(
                                actuation_events,
                                run_id=run_id,
                                process_variant=process_variant,
                                layer=layer,
                                repeat_index=repeat_index,
                                gas_ppm=gas_ppm,
                                pressure_target_hpa=None,
                                event_type="vent_refresh_warning",
                                event_value="refresh_limit_reached",
                                note=warning_note,
                                chain_mode=chain_mode,
                            )
                            flush_gate_trace_rows.append(
                                _build_flush_gate_trace_row(
                                    rows,
                                    process_variant=process_variant,
                                    layer=layer,
                                    repeat_index=repeat_index,
                                    gas_ppm=gas_ppm,
                                    phase=phase_name,
                                    elapsed_real_s=elapsed_real_s,
                                    gate_window_s=gate_window_s,
                                    min_flush_s=min_flush_s,
                                    target_flush_s=target_flush_s,
                                    note=warning_note,
                                    require_ratio=require_ratio,
                                    chain_mode=chain_mode,
                                )
                            )
                            _log_flush_gate_snapshot(flush_gate_trace_rows[-1])
                            refresh_limit_warning_emitted = True
                        next_refresh_elapsed_s = None
                    else:
                        refresh_label = f"{elapsed_real_s:.1f}s"
                        refresh_reason = f"metrology {phase_label} refresh @{refresh_label}"
                        vent_refresh_handler(refresh_reason)
                        flush_vent_refresh_count += 1
                        refresh_stats["count"] = flush_vent_refresh_count
                        _update_flush_vent_refresh_metadata(
                            setup_metadata,
                            requested_interval_s=refresh_interval_s,
                            actual_intervals_s=list(refresh_stats.get("actual_intervals_s") or []),
                            refresh_count=flush_vent_refresh_count,
                            thread_used=False,
                        )
                        _record_event(
                            actuation_events,
                            run_id=run_id,
                            process_variant=process_variant,
                            layer=layer,
                            repeat_index=repeat_index,
                            gas_ppm=gas_ppm,
                            pressure_target_hpa=None,
                            event_type="vent_refresh",
                            event_value="VENT_ON_REFRESH",
                            note=f"{phase_label} refresh @{refresh_label}",
                            chain_mode=chain_mode,
                            extra_fields={
                                "flush_vent_refresh_interval_s_requested": refresh_interval_s,
                                "flush_vent_refresh_thread_used": False,
                            },
                        )
                        flush_gate_trace_rows.append(
                            _build_flush_gate_trace_row(
                                rows,
                                process_variant=process_variant,
                                layer=layer,
                                repeat_index=repeat_index,
                                gas_ppm=gas_ppm,
                                phase=phase_name,
                                elapsed_real_s=elapsed_real_s,
                                gate_window_s=gate_window_s,
                                min_flush_s=min_flush_s,
                                target_flush_s=target_flush_s,
                                note=f"{phase_label}_vent_refresh_{refresh_label}",
                                require_ratio=require_ratio,
                                chain_mode=chain_mode,
                            )
                        )
                        flush_gate_trace_rows[-1].update(
                            {
                                "flush_vent_refresh_interval_s_requested": refresh_interval_s,
                                "flush_vent_refresh_thread_used": False,
                            }
                        )
                        _log_flush_gate_snapshot(flush_gate_trace_rows[-1])
                        next_refresh_elapsed_s = elapsed_real_s + refresh_interval_s
            while elapsed_real_s >= next_trace_elapsed_s:
                flush_gate_trace_rows.append(
                    _build_flush_gate_trace_row(
                        rows,
                        process_variant=process_variant,
                        layer=layer,
                        repeat_index=repeat_index,
                        gas_ppm=gas_ppm,
                        phase=phase_name,
                        elapsed_real_s=elapsed_real_s,
                        gate_window_s=gate_window_s,
                        min_flush_s=min_flush_s,
                        target_flush_s=target_flush_s,
                        note=f"{phase_label}_heartbeat_{int(next_trace_elapsed_s)}s",
                        require_ratio=require_ratio,
                        chain_mode=chain_mode,
                    )
                )
                _log_flush_gate_snapshot(flush_gate_trace_rows[-1])
                next_trace_elapsed_s += 30.0
            if elapsed_real_s < float(target_flush_s):
                continue
            summary = build_flush_summary(
                rows,
                process_variant=process_variant,
                layer=layer,
                repeat_index=repeat_index,
                gas_ppm=gas_ppm,
                actual_deadtime_s=actual_deadtime_s,
                actuation_events=actuation_events,
                min_flush_s=min_flush_s,
                target_flush_s=target_flush_s,
                gate_window_s=gate_window_s,
                require_ratio=require_ratio,
                require_vent_on=require_vent_on,
                rebound_window_s=rebound_window_s,
                rebound_min_rise_c=rebound_min_rise_c,
            )
            if not first_gate_eval_logged:
                flush_gate_trace_rows.append(
                    _build_flush_gate_trace_row(
                        rows,
                        process_variant=process_variant,
                        layer=layer,
                        repeat_index=repeat_index,
                        gas_ppm=gas_ppm,
                        phase=phase_name,
                        elapsed_real_s=elapsed_real_s,
                        gate_window_s=gate_window_s,
                        min_flush_s=min_flush_s,
                        target_flush_s=target_flush_s,
                        note=f"{phase_label}_first_gate_evaluation",
                        require_ratio=require_ratio,
                        chain_mode=chain_mode,
                    )
                )
                _log_flush_gate_snapshot(flush_gate_trace_rows[-1])
                first_gate_eval_logged = True
            if summary.get("flush_gate_pass"):
                gate_row = build_phase_gate_row(
                    process_variant=process_variant,
                    layer=layer,
                    repeat_index=repeat_index,
                    gas_ppm=gas_ppm,
                    pressure_target_hpa=None,
                    phase=phase_name,
                    gate_name=gate_name,
                    gate_status=str(summary.get("flush_gate_status")),
                    gate_pass=True,
                    gate_window_s=gate_window_s,
                    gate_value={
                        "flush_duration_s": summary.get("flush_duration_s"),
                        "flush_last60s_ratio_span": summary.get("flush_last60s_ratio_span"),
                        "flush_last60s_ratio_slope": summary.get("flush_last60s_ratio_slope"),
                        "flush_last60s_dewpoint_span": summary.get("flush_last60s_dewpoint_span"),
                        "flush_last60s_dewpoint_slope": summary.get("flush_last60s_dewpoint_slope"),
                        "flush_last60s_gauge_span_hpa": summary.get("flush_last60s_gauge_span_hpa"),
                        "flush_last60s_gauge_slope_hpa_per_s": summary.get("flush_last60s_gauge_slope_hpa_per_s"),
                        "analyzer_raw_sample_count": summary.get("analyzer_raw_sample_count"),
                        "gauge_raw_sample_count": summary.get("gauge_raw_sample_count"),
                        "dewpoint_raw_sample_count": summary.get("dewpoint_raw_sample_count"),
                        "aligned_sample_count": summary.get("aligned_sample_count"),
                    },
                    gate_threshold={
                        "target_flush_s": target_flush_s,
                        "min_flush_s": min_flush_s,
                    },
                )
                flush_gate_trace_rows.append(
                    _build_flush_gate_trace_row(
                        rows,
                        process_variant=process_variant,
                        layer=layer,
                        repeat_index=repeat_index,
                        gas_ppm=gas_ppm,
                        phase=phase_name,
                        elapsed_real_s=elapsed_real_s,
                        gate_window_s=gate_window_s,
                        min_flush_s=min_flush_s,
                        target_flush_s=target_flush_s,
                        note=f"{phase_label}_gate_pass",
                        require_ratio=require_ratio,
                        chain_mode=chain_mode,
                    )
                )
                _log_flush_gate_snapshot(flush_gate_trace_rows[-1])
                _apply_gate_to_rows(rows, gate_pass=True, gate_fail_reason="")
                if precondition_mode:
                    _record_event(
                        actuation_events,
                        run_id=run_id,
                        process_variant=process_variant,
                        layer=layer,
                        repeat_index=repeat_index,
                        gas_ppm=gas_ppm,
                        pressure_target_hpa=None,
                        event_type="precondition_pass",
                        event_value="precondition_pass",
                        note="precondition gate passed",
                    )
                flush_status = ""
                break

        if not summary:
            summary = build_flush_summary(
                rows,
                process_variant=process_variant,
                layer=layer,
                repeat_index=repeat_index,
                gas_ppm=gas_ppm,
                actual_deadtime_s=actual_deadtime_s,
                actuation_events=actuation_events,
                min_flush_s=min_flush_s,
                target_flush_s=target_flush_s,
                gate_window_s=gate_window_s,
                require_ratio=require_ratio,
                require_vent_on=require_vent_on,
                rebound_window_s=rebound_window_s,
                rebound_min_rise_c=rebound_min_rise_c,
            )
            fail_reason = str(summary.get("flush_gate_fail_reason") or "").strip()
            fail_reason = ";".join([item for item in ["max_flush_timeout", fail_reason] if item])
            summary["flush_gate_status"] = "fail"
            summary["flush_gate_pass"] = False
            summary["flush_gate_fail_reason"] = fail_reason
            summary["flush_gate_failing_subgates"] = [
                item for item in str(fail_reason).split(";") if item and item != "max_flush_timeout"
            ]
            gate_row = build_phase_gate_row(
                process_variant=process_variant,
                layer=layer,
                repeat_index=repeat_index,
                gas_ppm=gas_ppm,
                pressure_target_hpa=None,
                phase=phase_name,
                gate_name=gate_name,
                gate_status="fail",
                gate_pass=False,
                gate_window_s=gate_window_s,
                gate_value={
                    "flush_duration_s": summary.get("flush_duration_s"),
                    "flush_last60s_ratio_span": summary.get("flush_last60s_ratio_span"),
                    "flush_last60s_ratio_slope": summary.get("flush_last60s_ratio_slope"),
                    "flush_last60s_dewpoint_span": summary.get("flush_last60s_dewpoint_span"),
                    "flush_last60s_dewpoint_slope": summary.get("flush_last60s_dewpoint_slope"),
                    "flush_last60s_gauge_span_hpa": summary.get("flush_last60s_gauge_span_hpa"),
                    "flush_last60s_gauge_slope_hpa_per_s": summary.get("flush_last60s_gauge_slope_hpa_per_s"),
                    "analyzer_raw_sample_count": summary.get("analyzer_raw_sample_count"),
                    "gauge_raw_sample_count": summary.get("gauge_raw_sample_count"),
                    "dewpoint_raw_sample_count": summary.get("dewpoint_raw_sample_count"),
                    "aligned_sample_count": summary.get("aligned_sample_count"),
                },
                gate_threshold={"target_flush_s": target_flush_s, "max_flush_s": max_flush_s},
                gate_fail_reason=fail_reason,
            )
            flush_gate_trace_rows.append(
                _build_flush_gate_trace_row(
                    rows,
                    process_variant=process_variant,
                    layer=layer,
                    repeat_index=repeat_index,
                    gas_ppm=gas_ppm,
                    phase=phase_name,
                    elapsed_real_s=max(0.0, time.monotonic() - phase_start_mono),
                    gate_window_s=gate_window_s,
                    min_flush_s=min_flush_s,
                    target_flush_s=target_flush_s,
                    note=f"{phase_label}_max_flush_timeout",
                    require_ratio=require_ratio,
                    chain_mode=chain_mode,
                )
            )
            _log_flush_gate_snapshot(flush_gate_trace_rows[-1])
            _record_event(
                actuation_events,
                run_id=run_id,
                process_variant=process_variant,
                layer=layer,
                repeat_index=repeat_index,
                gas_ppm=gas_ppm,
                pressure_target_hpa=None,
                event_type="precondition_fail_max_timeout" if precondition_mode else "flush_gate_fail_max_timeout",
                event_value=max_flush_s,
                note=fail_reason,
            )
            _apply_gate_to_rows(rows, gate_pass=False, gate_fail_reason=fail_reason)
            flush_status = "precondition_max_flush_timeout" if precondition_mode else "max_flush_timeout"
    finally:
        if heartbeat is not None:
            pending = heartbeat.stop()
            if pending:
                _drain_flush_vent_refresh_heartbeat_events(
                    pending,
                    rows=rows,
                    actuation_events=actuation_events,
                    flush_gate_trace_rows=flush_gate_trace_rows,
                    run_id=run_id,
                    process_variant=process_variant,
                    layer=layer,
                    repeat_index=repeat_index,
                    gas_ppm=gas_ppm,
                    phase_name=phase_name,
                    phase_label=phase_label,
                    gate_window_s=gate_window_s,
                    min_flush_s=min_flush_s,
                    target_flush_s=target_flush_s,
                    require_ratio=require_ratio,
                    chain_mode=chain_mode,
                    refresh_stats=refresh_stats,
                    requested_interval_s=refresh_interval_s,
                    setup_metadata=setup_metadata,
                )
        else:
            _update_flush_vent_refresh_metadata(
                setup_metadata,
                requested_interval_s=refresh_interval_s,
                actual_intervals_s=list(refresh_stats.get("actual_intervals_s") or []),
                refresh_count=int(refresh_stats.get("count", 0)),
                thread_used=False,
            )

    return rows, summary, gate_row, flush_status


def _as_precondition_summary(summary: Mapping[str, Any]) -> Dict[str, Any]:
    out = dict(summary)
    out["precondition_status"] = "pass" if bool(summary.get("flush_gate_pass")) else "fail"
    out["precondition_fail_reason"] = str(summary.get("flush_gate_fail_reason") or "")
    out["precondition_window_s"] = summary.get("flush_gate_window_s")
    return out


def _run_sealed_hold_phase(
    runner: CalibrationRunner,
    analyzer: GasAnalyzer,
    devices: Mapping[str, Any],
    trace_path: Path,
    trace_cursor: int,
    *,
    process_variant: str,
    layer: int,
    repeat_index: int,
    gas_ppm: int,
    route: Mapping[str, Any],
    source_point: CalibrationPoint,
    gas_start_mono: float,
    gas_start_ts: datetime,
    hold_duration_s: float,
    sample_poll_s: float,
    print_every_s: float,
    point_index: int,
    actuation_events: List[Dict[str, Any]],
    run_id: str,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any], Dict[str, Any], int]:
    seal_point = _build_pressure_point(source_point, 1100, index=point_index)
    if not runner._pressurize_and_hold(seal_point, route="co2"):
        summary = build_seal_hold_summary(
            [],
            process_variant=process_variant,
            layer=layer,
            repeat_index=repeat_index,
            gas_ppm=gas_ppm,
            hold_duration_target_s=hold_duration_s,
        )
        gate_row = build_phase_gate_row(
            process_variant=process_variant,
            layer=layer,
            repeat_index=repeat_index,
            gas_ppm=gas_ppm,
            pressure_target_hpa=1100,
            phase="sealed_hold",
            gate_name="seal_prepare",
            gate_status="fail",
            gate_pass=False,
            gate_window_s=None,
            gate_value="pressurize_and_hold_failed",
            gate_threshold=None,
            gate_fail_reason="pressurize_and_hold_failed",
        )
        return [], summary, gate_row, trace_cursor

    trace_context = _phase_context(
        process_variant=process_variant,
        layer=layer,
        repeat_index=repeat_index,
        phase="seal_prepare",
        gas_ppm=gas_ppm,
        route=route,
    )
    trace_rows, trace_cursor = _trace_tail_rows(
        trace_path,
        trace_cursor,
        context=trace_context,
        gas_start_ts=gas_start_ts,
        default_pressure_target_hpa=1100,
        controller_vent_state="VENT_OFF",
    )
    _record_trace_events(actuation_events, run_id=run_id, trace_rows=trace_rows)
    hold_context = _phase_context(
        process_variant=process_variant,
        layer=layer,
        repeat_index=repeat_index,
        phase="sealed_hold",
        gas_ppm=gas_ppm,
        route=route,
    )
    _record_event(
        actuation_events,
        run_id=run_id,
        process_variant=process_variant,
        layer=layer,
        repeat_index=repeat_index,
        gas_ppm=gas_ppm,
        pressure_target_hpa=1100,
        event_type="seal_hold_start",
        event_value=hold_duration_s,
        note="sealed hold begin",
    )
    hold_rows = _capture_phase_rows(
        analyzer,
        devices,
        context=hold_context,
        gas_start_mono=gas_start_mono,
        duration_s=hold_duration_s,
        sample_poll_s=sample_poll_s,
        print_every_s=print_every_s,
        controller_vent_state="VENT_OFF",
    )
    rows = trace_rows + hold_rows
    summary = build_seal_hold_summary(
        hold_rows,
        process_variant=process_variant,
        layer=layer,
        repeat_index=repeat_index,
        gas_ppm=gas_ppm,
        hold_duration_target_s=hold_duration_s,
    )
    fail_reason = ";".join(summary.get("hold_warning_flags", []))
    gate_row = build_phase_gate_row(
        process_variant=process_variant,
        layer=layer,
        repeat_index=repeat_index,
        gas_ppm=gas_ppm,
        pressure_target_hpa=1100,
        phase="sealed_hold",
        gate_name="sealed_hold_gate",
        gate_status=str(summary.get("phase_status")),
        gate_pass=str(summary.get("phase_status")) == "pass",
        gate_window_s=hold_duration_s,
        gate_value={
            "hold_pressure_drift_hpa_per_min": summary.get("hold_pressure_drift_hpa_per_min"),
            "hold_ratio_drift_per_min": summary.get("hold_ratio_drift_per_min"),
            "hold_dewpoint_drift_c_per_min": summary.get("hold_dewpoint_drift_c_per_min"),
            "analyzer_raw_sample_count": summary.get("analyzer_raw_sample_count"),
            "gauge_raw_sample_count": summary.get("gauge_raw_sample_count"),
            "dewpoint_raw_sample_count": summary.get("dewpoint_raw_sample_count"),
            "aligned_sample_count": summary.get("aligned_sample_count"),
        },
        gate_threshold={"hold_duration_s": hold_duration_s},
        gate_fail_reason=fail_reason,
    )
    _record_event(
        actuation_events,
        run_id=run_id,
        process_variant=process_variant,
        layer=layer,
        repeat_index=repeat_index,
        gas_ppm=gas_ppm,
        pressure_target_hpa=1100,
        event_type="seal_hold_end",
        event_value=summary.get("hold_duration_s"),
        note=f"status={summary.get('phase_status')}",
    )
    _apply_gate_to_rows(hold_rows, gate_pass=str(summary.get("phase_status")) == "pass", gate_fail_reason=fail_reason)
    return rows, summary, gate_row, trace_cursor


def _run_pressure_sweep_phase(
    runner: CalibrationRunner,
    analyzer: GasAnalyzer,
    devices: Mapping[str, Any],
    trace_path: Path,
    trace_cursor: int,
    pressure_summary_live_path: Path,
    phase_gate_live_path: Path,
    *,
    process_variant: str,
    layer: int,
    repeat_index: int,
    gas_ppm: int,
    route: Mapping[str, Any],
    source_point: CalibrationPoint,
    gas_start_mono: float,
    gas_start_ts: datetime,
    pressure_points: Sequence[int],
    stable_window_s: float,
    sample_poll_s: float,
    print_every_s: float,
    point_index_start: int,
    actuation_events: List[Dict[str, Any]],
    run_id: str,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]], int, int]:
    all_rows: List[Dict[str, Any]] = []
    summaries: List[Dict[str, Any]] = []
    gate_rows: List[Dict[str, Any]] = []
    point_index = point_index_start

    seal_point = _build_pressure_point(source_point, int(pressure_points[0]), index=point_index)
    point_index += 1
    if not runner._pressurize_and_hold(seal_point, route="co2"):
        gate_rows.append(
            build_phase_gate_row(
                process_variant=process_variant,
                layer=layer,
                repeat_index=repeat_index,
                gas_ppm=gas_ppm,
                pressure_target_hpa=int(pressure_points[0]),
                phase="seal_prepare",
                gate_name="seal_prepare",
                gate_status="fail",
                gate_pass=False,
                gate_window_s=None,
                gate_value="pressurize_and_hold_failed",
                gate_threshold=None,
                gate_fail_reason="pressurize_and_hold_failed",
            )
        )
        return all_rows, summaries, gate_rows, trace_cursor, point_index

    trace_context = _phase_context(
        process_variant=process_variant,
        layer=layer,
        repeat_index=repeat_index,
        phase="seal_prepare",
        gas_ppm=gas_ppm,
        route=route,
    )
    trace_rows, trace_cursor = _trace_tail_rows(
        trace_path,
        trace_cursor,
        context=trace_context,
        gas_start_ts=gas_start_ts,
        default_pressure_target_hpa=int(pressure_points[0]),
        controller_vent_state="VENT_OFF",
    )
    all_rows.extend(trace_rows)
    _record_trace_events(actuation_events, run_id=run_id, trace_rows=trace_rows)
    _record_event(
        actuation_events,
        run_id=run_id,
        process_variant=process_variant,
        layer=layer,
        repeat_index=repeat_index,
        gas_ppm=gas_ppm,
        pressure_target_hpa=int(pressure_points[0]),
        event_type="pressure_sweep_start",
        event_value="pressure_sweep_start",
        note=f"targets={','.join(str(item) for item in pressure_points)}",
    )

    for pressure_target in pressure_points:
        _record_event(
            actuation_events,
            run_id=run_id,
            process_variant=process_variant,
            layer=layer,
            repeat_index=repeat_index,
            gas_ppm=gas_ppm,
            pressure_target_hpa=int(pressure_target),
            event_type="pressure_target_change",
            event_value=int(pressure_target),
            note="set new pressure target",
        )
        pressure_point = _build_pressure_point(source_point, int(pressure_target), index=point_index)
        point_index += 1
        settle_started = datetime.now()
        control_ok = runner._set_pressure_to_target(pressure_point)
        transition_rows, trace_cursor = _trace_tail_rows(
            trace_path,
            trace_cursor,
            context=_phase_context(
                process_variant=process_variant,
                layer=layer,
                repeat_index=repeat_index,
                phase="pressure_sweep",
                gas_ppm=gas_ppm,
                route=route,
            ),
            gas_start_ts=gas_start_ts,
            default_pressure_target_hpa=int(pressure_target),
            controller_vent_state="VENT_OFF",
        )
        all_rows.extend(transition_rows)
        _record_trace_events(actuation_events, run_id=run_id, trace_rows=transition_rows)
        if not control_ok:
            summary = build_pressure_point_summary(
                transition_rows,
                [],
                process_variant=process_variant,
                layer=layer,
                repeat_index=repeat_index,
                gas_ppm=gas_ppm,
                pressure_target_hpa=int(pressure_target),
                settle_started_time=settle_started,
            )
            summaries.append(summary)
            _append_live_csv_row(pressure_summary_live_path, summary)
            gate_row = build_phase_gate_row(
                process_variant=process_variant,
                layer=layer,
                repeat_index=repeat_index,
                gas_ppm=gas_ppm,
                pressure_target_hpa=int(pressure_target),
                phase="pressure_sweep",
                gate_name="pressure_control_gate",
                gate_status="fail",
                gate_pass=False,
                gate_window_s=None,
                gate_value="pressure_control_failed",
                gate_threshold=None,
                gate_fail_reason="pressure_control_failed",
            )
            gate_rows.append(gate_row)
            _append_live_csv_row(phase_gate_live_path, gate_row)
            continue

        settle_reached = datetime.now()
        ready_ok = runner._wait_after_pressure_stable_before_sampling(pressure_point)
        ready_rows, trace_cursor = _trace_tail_rows(
            trace_path,
            trace_cursor,
            context=_phase_context(
                process_variant=process_variant,
                layer=layer,
                repeat_index=repeat_index,
                phase="pressure_sweep",
                gas_ppm=gas_ppm,
                route=route,
            ),
            gas_start_ts=gas_start_ts,
            default_pressure_target_hpa=int(pressure_target),
            controller_vent_state="VENT_OFF",
        )
        all_rows.extend(ready_rows)
        transition_rows.extend(ready_rows)
        _record_trace_events(actuation_events, run_id=run_id, trace_rows=ready_rows)
        if not ready_ok:
            summary = build_pressure_point_summary(
                transition_rows,
                [],
                process_variant=process_variant,
                layer=layer,
                repeat_index=repeat_index,
                gas_ppm=gas_ppm,
                pressure_target_hpa=int(pressure_target),
                settle_started_time=settle_started,
                settle_reached_time=settle_reached,
            )
            summaries.append(summary)
            _append_live_csv_row(pressure_summary_live_path, summary)
            gate_row = build_phase_gate_row(
                process_variant=process_variant,
                layer=layer,
                repeat_index=repeat_index,
                gas_ppm=gas_ppm,
                pressure_target_hpa=int(pressure_target),
                phase="pressure_sweep",
                gate_name="pressure_ready_gate",
                gate_status="fail",
                gate_pass=False,
                gate_window_s=None,
                gate_value="pressure_ready_failed",
                gate_threshold=None,
                gate_fail_reason="pressure_ready_failed",
            )
            gate_rows.append(gate_row)
            _append_live_csv_row(phase_gate_live_path, gate_row)
            continue

        sample_start = datetime.now()
        _record_event(
            actuation_events,
            run_id=run_id,
            process_variant=process_variant,
            layer=layer,
            repeat_index=repeat_index,
            gas_ppm=gas_ppm,
            pressure_target_hpa=int(pressure_target),
            event_type="stable_sampling_start",
            event_value=stable_window_s,
            note="stable sampling begin",
        )
        stable_rows = _capture_phase_rows(
            analyzer,
            devices,
            context=_phase_context(
                process_variant=process_variant,
                layer=layer,
                repeat_index=repeat_index,
                phase="stable_sampling",
                gas_ppm=gas_ppm,
                route=route,
            ),
            gas_start_mono=gas_start_mono,
            duration_s=stable_window_s,
            sample_poll_s=sample_poll_s,
            print_every_s=max(1.0, print_every_s),
            controller_vent_state="VENT_OFF",
            pressure_target_hpa=int(pressure_target),
        )
        sample_end = datetime.now()
        all_rows.extend(stable_rows)
        summary = build_pressure_point_summary(
            transition_rows,
            stable_rows,
            process_variant=process_variant,
            layer=layer,
            repeat_index=repeat_index,
            gas_ppm=gas_ppm,
            pressure_target_hpa=int(pressure_target),
            settle_started_time=settle_started,
            settle_reached_time=settle_reached,
            sample_start_time=sample_start,
            sample_end_time=sample_end,
        )
        summaries.append(summary)
        _append_live_csv_row(pressure_summary_live_path, summary)
        fail_reason = ";".join(summary.get("warning_flags", []))
        gate_row = build_phase_gate_row(
            process_variant=process_variant,
            layer=layer,
            repeat_index=repeat_index,
            gas_ppm=gas_ppm,
            pressure_target_hpa=int(pressure_target),
            phase="stable_sampling",
            gate_name="stable_sampling_gate",
            gate_status=str(summary.get("phase_status")),
            gate_pass=str(summary.get("phase_status")) == "pass",
            gate_window_s=stable_window_s,
            gate_value={
                "stable_sample_count": summary.get("stable_sample_count"),
                "ratio_slope": summary.get("point_window_ratio_slope_per_s"),
                "dewpoint_slope": summary.get("point_window_dewpoint_slope_per_s"),
                "analyzer_raw_sample_count": summary.get("analyzer_raw_sample_count"),
                "gauge_raw_sample_count": summary.get("gauge_raw_sample_count"),
                "dewpoint_raw_sample_count": summary.get("dewpoint_raw_sample_count"),
                "aligned_sample_count": summary.get("aligned_sample_count"),
            },
            gate_threshold={"stable_window_s": stable_window_s},
            gate_fail_reason=fail_reason,
        )
        gate_rows.append(gate_row)
        _append_live_csv_row(phase_gate_live_path, gate_row)
        _record_event(
            actuation_events,
            run_id=run_id,
            process_variant=process_variant,
            layer=layer,
            repeat_index=repeat_index,
            gas_ppm=gas_ppm,
            pressure_target_hpa=int(pressure_target),
            event_type="stable_sampling_end",
            event_value=summary.get("stable_sample_count"),
            note=f"status={summary.get('phase_status')}",
        )
        _apply_gate_to_rows(stable_rows, gate_pass=str(summary.get("phase_status")) == "pass", gate_fail_reason=fail_reason)

    return all_rows, summaries, gate_rows, trace_cursor, point_index


def _variant_layer_status(
    *,
    variant: str,
    flush_summaries: Sequence[Mapping[str, Any]],
    seal_hold_summaries: Sequence[Mapping[str, Any]],
    pressure_summaries: Sequence[Mapping[str, Any]],
    phase_gate_rows: Sequence[Mapping[str, Any]],
    args: argparse.Namespace,
) -> str:
    analyzed = analyze_room_temp_diagnostic(
        flush_summaries,
        seal_hold_summaries,
        pressure_summaries,
        phase_gate_rows=phase_gate_rows,
        min_flush_s=float(args.min_flush_s),
        target_flush_s=float(args.screening_flush_s_default),
        expected_deadtime_s=float(args.gas_switch_deadtime_s),
    )
    for item in analyzed.get("variant_summaries", []) or []:
        if str(item.get("process_variant")) == str(variant):
            return str(item.get("classification") or "insufficient_evidence")
    return "insufficient_evidence"


def _should_stop_after_layer(status: str, *, early_stop: bool, treat_insufficient_as_stop: bool) -> bool:
    if not bool(early_stop):
        return False
    normalized = str(status or "").strip().lower()
    if normalized == "fail":
        return True
    if normalized == "insufficient_evidence" and bool(treat_insufficient_as_stop):
        return True
    return False


def _chain_mode_flags(chain_mode: str) -> Dict[str, Any]:
    normalized = str(chain_mode or "").strip()
    analyzer_connected = normalized in {"analyzer_in_keep_rest", "analyzer_in_pace_out_keep_rest"}
    analyzer_count = 8 if analyzer_connected else 0
    pace_in_path = normalized not in {"analyzer_in_pace_out_keep_rest", "analyzer_out_pace_out_keep_rest"}
    return {
        "chain_mode": normalized,
        "analyzer_count_in_path": analyzer_count,
        "analyzer_chain_connected": analyzer_connected,
        "pace_in_path": pace_in_path,
        "pace_expected_vent_on": pace_in_path,
        "controller_vent_expected": pace_in_path,
        "controller_vent_state": "VENT_ON" if pace_in_path else "NOT_APPLICABLE",
        "valve_block_in_path": True,
        "dewpoint_meter_in_path": True,
        "gauge_in_path": True,
    }


def _diagnostic_chain_cfg(cfg: Mapping[str, Any]) -> Dict[str, Any]:
    diagnostics_cfg = cfg.get("diagnostics", {}) if isinstance(cfg, Mapping) else {}
    if not isinstance(diagnostics_cfg, Mapping):
        return {}
    chain_cfg = diagnostics_cfg.get("analyzer_chain_isolation", {})
    return dict(chain_cfg) if isinstance(chain_cfg, Mapping) else {}


def _diagnostic_precondition_cfg(cfg: Mapping[str, Any]) -> Dict[str, Any]:
    diagnostics_cfg = cfg.get("diagnostics", {}) if isinstance(cfg, Mapping) else {}
    if not isinstance(diagnostics_cfg, Mapping):
        return {}
    precondition_cfg = diagnostics_cfg.get("precondition", {})
    return dict(precondition_cfg) if isinstance(precondition_cfg, Mapping) else {}


def _closed_pressure_swing_cfg(cfg: Mapping[str, Any]) -> Dict[str, Any]:
    precondition_cfg = _diagnostic_precondition_cfg(cfg)
    swing_cfg = precondition_cfg.get("closed_pressure_swing", {})
    return dict(swing_cfg) if isinstance(swing_cfg, Mapping) else {}


def _closed_pressure_swing_defaults(cfg: Mapping[str, Any]) -> Dict[str, Any]:
    swing_cfg = _closed_pressure_swing_cfg(cfg)
    enabled = bool(swing_cfg.get("enabled"))
    cycles_requested = int(_safe_int(swing_cfg.get("cycles")) or 1) if enabled else 0
    return {
        "closed_pressure_swing_enabled": enabled,
        "closed_pressure_swing_cycles_requested": cycles_requested,
        "closed_pressure_swing_cycles_completed": 0,
        "closed_pressure_swing_high_pressure_hpa": _safe_float(swing_cfg.get("high_pressure_hpa")),
        "closed_pressure_swing_low_pressure_hpa": _safe_float(swing_cfg.get("low_pressure_hpa")),
        "closed_pressure_swing_low_hold_s": _safe_float(swing_cfg.get("low_hold_s")),
        "closed_pressure_swing_linear_slew_hpa_per_s": _safe_float(swing_cfg.get("linear_slew_hpa_per_s")),
        "closed_pressure_swing_vent_closed_verified": False,
        "closed_pressure_swing_abort_reason": "",
        "closed_pressure_swing_total_extra_s": 0.0,
        "closed_pressure_swing_high_reached": False,
        "closed_pressure_swing_seal_command_issued": False,
        "closed_pressure_swing_seal_verified": False,
        "closed_pressure_swing_sealed_control_to_low_started": False,
        "extra_precondition_strategy_used": "closed_pressure_swing_predry" if enabled else "",
        "extra_precondition_time_cost_s": 0.0,
    }


def _resolve_chain_analyzers(cfg: Mapping[str, Any], chain_mode: str) -> List[Dict[str, Any]]:
    flags = _chain_mode_flags(chain_mode)
    if not bool(flags.get("analyzer_chain_connected")):
        return []
    devices_cfg = cfg.get("devices", {}) if isinstance(cfg, Mapping) else {}
    gas_list = devices_cfg.get("gas_analyzers", []) if isinstance(devices_cfg, Mapping) else []
    resolved: List[Dict[str, Any]] = []
    if isinstance(gas_list, list):
        for index, item in enumerate(gas_list, start=1):
            if not isinstance(item, Mapping) or not bool(item.get("enabled", True)):
                continue
            resolved.append(
                {
                    "order": len(resolved) + 1,
                    "name": str(item.get("name") or f"ga{index:02d}"),
                    "device_id": str(item.get("device_id") or f"{index:03d}"),
                    "port": str(item.get("port") or ""),
                }
            )
    if resolved:
        return resolved
    single_cfg = devices_cfg.get("gas_analyzer", {}) if isinstance(devices_cfg, Mapping) else {}
    if isinstance(single_cfg, Mapping) and bool(single_cfg.get("enabled")):
        return [
            {
                "order": 1,
                "name": "gas_analyzer",
                "device_id": str(single_cfg.get("device_id") or "000"),
                "port": str(single_cfg.get("port") or ""),
            }
        ]
    return []


def _format_chain_analyzers(analyzers_in_path: Sequence[Mapping[str, Any]]) -> str:
    parts: List[str] = []
    for item in analyzers_in_path:
        order = _safe_int(item.get("order"))
        name = str(item.get("name") or "")
        device_id = str(item.get("device_id") or "")
        port = str(item.get("port") or "")
        prefix = f"{order}:" if order is not None else ""
        parts.append(f"{prefix}{name}[{device_id}]@{port}")
    return " -> ".join(parts)


def _resolve_reference_dirs(cfg: Mapping[str, Any]) -> List[Path]:
    chain_cfg = _diagnostic_chain_cfg(cfg)
    raw_dirs = chain_cfg.get("compare_vs_8ch_reference_dirs", [])
    if not isinstance(raw_dirs, list):
        return []
    base_dir = Path(str(cfg.get("_base_dir") or Path.cwd()))
    resolved: List[Path] = []
    for raw_dir in raw_dirs:
        try:
            candidate = Path(str(raw_dir))
        except Exception:
            continue
        if not candidate.is_absolute():
            candidate = (base_dir / candidate).resolve()
        else:
            candidate = candidate.resolve()
        resolved.append(candidate)
    return resolved


def _resolve_compare_vs_baseline_reference_dir(cfg: Mapping[str, Any]) -> Optional[Path]:
    chain_cfg = _diagnostic_chain_cfg(cfg)
    raw_dir = chain_cfg.get("compare_vs_baseline_reference_dir")
    if raw_dir in (None, ""):
        return None
    try:
        candidate = Path(str(raw_dir))
    except Exception:
        return None
    base_dir = Path(str(cfg.get("_base_dir") or Path.cwd()))
    if not candidate.is_absolute():
        candidate = (base_dir / candidate).resolve()
    else:
        candidate = candidate.resolve()
    return candidate


def _chain_mode_case_name(chain_mode: str) -> Optional[str]:
    return {
        "analyzer_out_pace_out_keep_rest": "A0P0",
        "analyzer_out_keep_rest": "A0P1",
        "analyzer_in_pace_out_keep_rest": "A1P0",
        "analyzer_in_keep_rest": "A1P1",
    }.get(str(chain_mode or "").strip())


def _chain_mode_checklist(chain_mode: str) -> str:
    flags = _chain_mode_flags(chain_mode)
    if chain_mode == "analyzer_out_keep_rest":
        lines = [
            "当前模式：analyzer_out_keep_rest",
            "要求 8 台分析仪串路断开 / 不在路。",
            "零气源、减压阀、生产阀组、露点仪、数字压力计、PACE 其余连接保持不变。",
            "PACE 必须保持 VENT ON。",
            "本次只做 0 ppm + VENT ON + precondition/flush，不进入 seal / pressure sweep。",
        ]
    elif chain_mode == "analyzer_out_pace_out_keep_rest":
        lines = [
            "当前模式：analyzer_out_pace_out_keep_rest",
            "要求分析仪串路断开 / 不在路。",
            "PACE 从气路中断开，其他连接相对 analyzer_out_keep_rest 尽量保持不变。",
            "零气源、减压阀、生产阀组、露点仪、数字压力计其余连接不要动。",
            "本次只做 0 ppm + VENT/flush/precondition，不进入 seal / pressure sweep。",
        ]
    elif chain_mode == "analyzer_in_keep_rest":
        lines = [
            "当前模式：analyzer_in_keep_rest",
            "要求 8 台分析仪串路恢复在路。",
            "零气源、减压阀、生产阀组、露点仪、数字压力计、PACE 其余连接保持不变。",
            "PACE 必须保持 VENT ON。",
            "本次只做 0 ppm + VENT ON + precondition/flush，不进入 seal / pressure sweep。",
        ]
    else:
        lines = [
            "当前模式：compare_pair",
            "先运行 analyzer_out_keep_rest，再恢复 8 台分析仪串路运行 analyzer_in_keep_rest。",
            "若两组数据都已存在，本命令会直接输出对比报告，不执行新真机动作。",
            "PACE 必须保持 VENT ON，其他连接不要动。",
        ]
    lines.append(f"analyzer_chain_connected={flags['analyzer_chain_connected']}")
    lines.append(f"analyzer_count_in_path={flags['analyzer_count_in_path']}")
    case_name = _chain_mode_case_name(chain_mode)
    if case_name:
        lines.append(f"case_name={case_name}")
    return "\n".join(lines)


def _chain_mode_checklist_text(chain_mode: str) -> str:
    flags = _chain_mode_flags(chain_mode)
    if chain_mode == "analyzer_out_keep_rest":
        lines = [
            "当前模式：analyzer_out_keep_rest",
            "要求 8 台气体分析仪串路断开 / 不在路。",
            "零气源、减压阀、生产阀组、露点仪、数字压力计、PACE 其余连接保持不变。",
            "PACE 必须保持 VENT ON。",
            "本次只做 0 ppm + VENT ON + precondition/flush，不进入 seal / pressure sweep。",
        ]
    elif chain_mode == "analyzer_out_pace_out_keep_rest":
        lines = [
            "当前模式：analyzer_out_pace_out_keep_rest",
            "要求分析仪串路断开 / 不在路。",
            "压力控制器（PACE）从气路中断开；其他连接相对 analyzer_out_keep_rest 尽量保持不变。",
            "零气源、减压阀、生产阀组、露点仪、数字压力计其余连接不要动。",
            "本次只做 0 ppm + VENT/flush/precondition，不进入 seal / pressure sweep。",
            "CLI 不会自动驱动一个已经不在气路中的 PACE；若 PACE 仍在线，仅记录 metadata / telemetry。",
        ]
    elif chain_mode == "analyzer_in_keep_rest":
        lines = [
            "当前模式：analyzer_in_keep_rest",
            "要求 8 台气体分析仪串路恢复在路。",
            "零气源、减压阀、生产阀组、露点仪、数字压力计、PACE 其余连接保持不变。",
            "PACE 必须保持 VENT ON。",
            "本次只做 0 ppm + VENT ON + precondition/flush，不进入 seal / pressure sweep。",
        ]
    elif chain_mode == "analyzer_in_pace_out_keep_rest":
        lines = [
            "当前模式：analyzer_in_pace_out_keep_rest",
            "要求 8 台气体分析仪串路恢复在路。",
            "压力控制器（PACE）从气路中断开；其他连接相对 analyzer_in_keep_rest 尽量保持不变。",
            "零气源、减压阀、生产阀组、露点仪、数字压力计其余连接不要动。",
            "本次只做 0 ppm + VENT/flush/precondition，不进入 seal / pressure sweep。",
            "CLI 不会自动驱动一个已经不在气路中的 PACE；若 PACE 仍在线，仅记录 metadata / telemetry。",
        ]
    else:
        lines = [
            "当前模式：compare_pair",
            "先运行 analyzer_out_keep_rest，再恢复 8 台气体分析仪串路运行 analyzer_in_keep_rest。",
            "若两组数据都已存在，本命令直接输出对比报告，不执行新的真机动作。",
            "PACE 必须保持 VENT ON，其余连接不要动。",
        ]
    lines.append(f"analyzer_chain_connected={flags['analyzer_chain_connected']}")
    lines.append(f"analyzer_count_in_path={flags['analyzer_count_in_path']}")
    lines.append(f"pace_in_path={flags['pace_in_path']}")
    lines.append(f"controller_vent_expected={flags['controller_vent_expected']}")
    case_name = _chain_mode_case_name(chain_mode)
    if case_name:
        lines.append(f"case_name={case_name}")
    return "\n".join(lines)


def _log_operator_checklist(text: str) -> None:
    _log("=== Operator Checklist ===")
    for line in str(text or "").splitlines():
        _log(f"- {line}")


def _annotate_chain_mode_rows(
    rows: Sequence[Mapping[str, Any]],
    *,
    chain_mode: str,
    isolation_phase_name: Optional[str] = None,
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for raw_row in rows:
        row = dict(raw_row)
        row["chain_mode"] = chain_mode
        if isolation_phase_name and str(row.get("phase") or "") in {"precondition_vent_on", "gas_flush_vent_on"}:
            row["phase"] = isolation_phase_name
            row["current_phase"] = isolation_phase_name
        out.append(row)
    return out


def _annotate_chain_mode_events(rows: Sequence[Mapping[str, Any]], *, chain_mode: str) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for raw_row in rows:
        row = dict(raw_row)
        row["chain_mode"] = chain_mode
        out.append(row)
    return out


def _closed_pressure_swing_tolerance_hpa(cfg: Mapping[str, Any], target_hpa: float) -> float:
    devices_cfg = cfg.get("devices", {}) if isinstance(cfg, Mapping) else {}
    pressure_cfg = devices_cfg.get("pressure_controller", {}) if isinstance(devices_cfg, Mapping) else {}
    in_limits_pct = _safe_float(pressure_cfg.get("in_limits_pct")) or 0.02
    return max(3.0, abs(float(target_hpa)) * in_limits_pct)


def _closed_pressure_swing_state_snapshot(
    runner: CalibrationRunner,
    pace: Pace5000,
) -> Tuple[Dict[str, Any], List[str]]:
    snapshot = dict(runner._pressure_snapshot() or {})
    try:
        snapshot["pace_vent_status"] = pace.get_vent_status()
    except Exception as exc:
        snapshot["pace_vent_status_err"] = str(exc)
    try:
        snapshot["pace_output_state"] = pace.get_output_state()
    except Exception as exc:
        snapshot["pace_output_state_err"] = str(exc)
    try:
        snapshot["pace_isolation_state"] = pace.get_isolation_state()
    except Exception as exc:
        snapshot["pace_isolation_state_err"] = str(exc)
    snapshot["hold_thread_active"] = bool(getattr(runner, "_pressure_atmosphere_hold_enabled", False))
    snapshot["atmosphere_hold_strategy"] = str(getattr(runner, "_pressure_atmosphere_hold_strategy", "") or "")
    snapshot["vent_after_valve_open"] = getattr(runner, "_pace_vent_after_valve_open", None)
    failures: List[str] = []
    vent_status = _safe_int(snapshot.get("pace_vent_status"))
    output_state = _safe_int(snapshot.get("pace_output_state"))
    isolation_state = _safe_int(snapshot.get("pace_isolation_state"))
    trapped_pressure_status = _safe_int(getattr(pace, "VENT_STATUS_TRAPPED_PRESSURE", 3))
    if vent_status is None:
        failures.append("vent_status_unavailable")
    elif vent_status not in {0, trapped_pressure_status}:
        failures.append(f"vent_status={vent_status}")
    if output_state is None:
        failures.append("output_state_unavailable")
    elif output_state != 1:
        failures.append(f"output_state={output_state}")
    if isolation_state is None:
        failures.append("isolation_state_unavailable")
    elif isolation_state != 1:
        failures.append(f"isolation_state={isolation_state}")
    if snapshot.get("hold_thread_active"):
        failures.append("atmosphere_hold_active")
    if snapshot.get("atmosphere_hold_strategy") == "vent_valve_open_after_vent" and snapshot.get("vent_after_valve_open") is True:
        failures.append("vent_after_valve_open")
    return snapshot, failures


def _configure_closed_pressure_swing_slew(pace: Any, linear_slew_hpa_per_s: Optional[float]) -> Optional[str]:
    slew_value = _safe_float(linear_slew_hpa_per_s)
    if slew_value is None or slew_value <= 0.0:
        return None
    try:
        set_slew_mode_linear = getattr(pace, "set_slew_mode_linear", None)
        set_slew_rate = getattr(pace, "set_slew_rate", None)
        set_overshoot_allowed = getattr(pace, "set_overshoot_allowed", None)
        if callable(set_overshoot_allowed):
            set_overshoot_allowed(False)
        if callable(set_slew_mode_linear):
            set_slew_mode_linear()
        if callable(set_slew_rate):
            set_slew_rate(slew_value)
        return None
    except Exception as exc:
        return f"closed_pressure_swing_slew_config_failed:{exc}"


def _restore_closed_pressure_swing_slew(pace: Any) -> None:
    try:
        set_slew_mode_max = getattr(pace, "set_slew_mode_max", None)
        set_overshoot_allowed = getattr(pace, "set_overshoot_allowed", None)
        if callable(set_slew_mode_max):
            set_slew_mode_max()
        if callable(set_overshoot_allowed):
            set_overshoot_allowed(True)
    except Exception:
        return


def _capture_until_pressure_target(
    runner: CalibrationRunner,
    analyzer: Optional[GasAnalyzer],
    devices: Mapping[str, Any],
    *,
    context: Mapping[str, Any],
    gas_start_mono: float,
    controller_vent_state: str,
    sample_poll_s: float,
    print_every_s: float,
    target_hpa: float,
    timeout_s: float,
    tolerance_hpa: float,
    phase_elapsed_offset_s: float = 0.0,
    require_vent_closed_verified: bool = True,
    target_mode: str = "within",
) -> Tuple[List[Dict[str, Any]], float, Optional[str], bool, str]:
    pace = runner.devices.get("pace")
    rows: List[Dict[str, Any]] = []
    elapsed_offset = float(phase_elapsed_offset_s)
    deadline = time.monotonic() + max(0.1, float(timeout_s))
    while time.monotonic() < deadline:
        remaining_s = deadline - time.monotonic()
        chunk_duration_s = min(max(0.4, float(sample_poll_s)), max(0.05, remaining_s))
        chunk_rows = _capture_phase_rows(
            analyzer,
            devices,
            context=context,
            gas_start_mono=gas_start_mono,
            duration_s=chunk_duration_s,
            sample_poll_s=sample_poll_s,
            print_every_s=print_every_s,
            controller_vent_state=controller_vent_state,
            pressure_target_hpa=int(round(float(target_hpa))),
            phase_elapsed_offset_s=elapsed_offset,
        )
        rows.extend(chunk_rows)
        if chunk_rows:
            elapsed_offset = _safe_float(chunk_rows[-1].get("phase_elapsed_s")) or elapsed_offset
        if require_vent_closed_verified and pace is not None:
            _, failures = _closed_pressure_swing_state_snapshot(runner, pace)
            if failures:
                return rows, elapsed_offset, None, False, "vent_closed_not_verified:" + ",".join(failures)
        latest_row = chunk_rows[-1] if chunk_rows else (rows[-1] if rows else {})
        gauge_pressure = _safe_float(latest_row.get("gauge_pressure_hpa"))
        controller_pressure = _safe_float(latest_row.get("controller_pressure_hpa"))
        pressure_value = gauge_pressure if gauge_pressure is not None else controller_pressure
        if pressure_value is not None:
            mode = str(target_mode or "within").strip().lower()
            target_value = float(target_hpa)
            tolerance_value = float(tolerance_hpa)
            reached = False
            if mode == "at_or_above":
                reached = pressure_value >= (target_value - tolerance_value)
            elif mode == "at_or_below":
                reached = pressure_value <= (target_value + tolerance_value)
            else:
                reached = abs(pressure_value - target_value) <= tolerance_value
            if reached:
                return rows, elapsed_offset, str(latest_row.get("timestamp") or ""), True, ""
    return rows, elapsed_offset, None, False, f"pressure_target_not_reached:{target_hpa}"


def _run_closed_pressure_swing_predry(
    runner: CalibrationRunner,
    analyzer: Optional[GasAnalyzer],
    devices: Mapping[str, Any],
    *,
    cfg: Mapping[str, Any],
    process_variant: str,
    layer: int,
    repeat_index: int,
    gas_ppm: int,
    route: Mapping[str, Any],
    gas_start_mono: float,
    sample_poll_s: float,
    print_every_s: float,
    actuation_events: List[Dict[str, Any]],
    run_id: str,
    chain_mode: str,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], Dict[str, Any]]:
    strategy_state = _closed_pressure_swing_defaults(cfg)
    if not bool(strategy_state.get("closed_pressure_swing_enabled")):
        return [], [], strategy_state

    pace = runner.devices.get("pace")
    if chain_mode != "analyzer_in_keep_rest":
        strategy_state["closed_pressure_swing_abort_reason"] = "unsupported_chain_mode"
        return [], [], strategy_state
    required_pace_methods = (
        "get_vent_status",
        "get_output_state",
        "get_isolation_state",
        "set_setpoint",
    )
    if pace is None or any(not hasattr(pace, attr) for attr in required_pace_methods):
        strategy_state["closed_pressure_swing_abort_reason"] = "pace_missing"
        return [], [], strategy_state

    swing_cfg = _closed_pressure_swing_cfg(cfg)
    require_vent_closed_verified = bool(swing_cfg.get("require_vent_closed_verified", True))
    cycles_requested = max(1, int(_safe_int(swing_cfg.get("cycles")) or 1))
    high_pressure_hpa = float(_safe_float(swing_cfg.get("high_pressure_hpa")) or 1105.0)
    low_pressure_hpa = float(_safe_float(swing_cfg.get("low_pressure_hpa")) or 500.0)
    low_hold_s = float(_safe_float(swing_cfg.get("low_hold_s")) or 10.0)
    settle_after_repressurize_s = float(_safe_float(swing_cfg.get("settle_after_repressurize_s")) or 5.0)
    valve_settle_s = max(0.0, float(_safe_float(swing_cfg.get("valve_settle_s")) or 1.0))
    max_total_extra_s = float(_safe_float(swing_cfg.get("max_total_extra_s")) or 120.0)
    linear_slew_hpa_per_s = _safe_float(swing_cfg.get("linear_slew_hpa_per_s"))
    high_tolerance_hpa = _closed_pressure_swing_tolerance_hpa(cfg, high_pressure_hpa)
    low_tolerance_hpa = _closed_pressure_swing_tolerance_hpa(cfg, low_pressure_hpa)

    strategy_state.update(
        {
            "closed_pressure_swing_cycles_requested": cycles_requested,
            "closed_pressure_swing_high_pressure_hpa": high_pressure_hpa,
            "closed_pressure_swing_low_pressure_hpa": low_pressure_hpa,
            "closed_pressure_swing_low_hold_s": low_hold_s,
            "closed_pressure_swing_linear_slew_hpa_per_s": linear_slew_hpa_per_s,
            "closed_pressure_swing_valve_settle_s": valve_settle_s,
            "extra_precondition_strategy_used": "closed_pressure_swing_predry",
        }
    )

    raw_rows: List[Dict[str, Any]] = []
    trace_rows: List[Dict[str, Any]] = []
    phase_elapsed_offset_s = 0.0
    overall_start_mono = time.monotonic()
    abort_reason = ""
    vent_closed_verified = False
    source_closed_for_cycle = False

    _log(
        f"[CHAIN] closed_pressure_swing_predry enabled | cycles={cycles_requested} "
        f"high={high_pressure_hpa:.1f} low={low_pressure_hpa:.1f}"
    )
    try:
        _record_event(
            actuation_events,
            run_id=run_id,
            process_variant=process_variant,
            layer=layer,
            repeat_index=repeat_index,
            gas_ppm=gas_ppm,
            pressure_target_hpa=None,
            chain_mode=chain_mode,
            event_type="vent_off",
            event_value="VENT_OFF",
            note="closed_pressure_swing_predry_start",
        )
        runner._set_pressure_controller_vent(False, reason="closed pressure swing predry")
        if not runner._enable_pressure_controller_output(reason="closed pressure swing predry"):
            strategy_state["closed_pressure_swing_abort_reason"] = "pressure_controller_output_enable_failed"
            return raw_rows, trace_rows, strategy_state
        slew_abort_reason = _configure_closed_pressure_swing_slew(pace, linear_slew_hpa_per_s)
        if slew_abort_reason:
            strategy_state["closed_pressure_swing_abort_reason"] = slew_abort_reason
            return raw_rows, trace_rows, strategy_state
        snapshot, failures = _closed_pressure_swing_state_snapshot(runner, pace)
    except Exception as exc:
        strategy_state["closed_pressure_swing_abort_reason"] = f"closed_pressure_swing_setup_failed:{exc}"
        return raw_rows, trace_rows, strategy_state
    if require_vent_closed_verified and failures:
        strategy_state["closed_pressure_swing_abort_reason"] = "vent_closed_not_verified:" + ",".join(failures)
        return raw_rows, trace_rows, strategy_state
    vent_closed_verified = True
    strategy_state["closed_pressure_swing_vent_closed_verified"] = vent_closed_verified

    for cycle_index in range(1, cycles_requested + 1):
        remaining_total_s = max_total_extra_s - (time.monotonic() - overall_start_mono)
        if remaining_total_s <= 0.0:
            abort_reason = "max_total_extra_s_exhausted"
            break

        cycle_trace = {
            "timestamp": datetime.now().isoformat(timespec="milliseconds"),
            "phase_name": "closed_swing_cycle",
            "cycle_index": cycle_index,
            "start_ts": datetime.now().isoformat(timespec="milliseconds"),
            "target_high_hpa": high_pressure_hpa,
            "target_low_hpa": low_pressure_hpa,
            "reached_high_pressure_ts": None,
            "reached_low_pressure_ts": None,
            "repressurized_ts": None,
            "high_reached_boolean": False,
            "seal_command_issued": False,
            "seal_verified": False,
            "gas_supply_open_state": _closed_swing_supply_open_state(runner, route),
            "route_valve_state": _commanded_valve_state(runner, _safe_int(route.get("path_valve")) or -1),
            "manifold_valve_state": _closed_swing_manifold_state(runner, route),
            "vent_state_before": snapshot.get("pace_vent_status"),
            "output_state_before": snapshot.get("pace_output_state"),
            "isolation_state_before": snapshot.get("pace_isolation_state"),
            "pace_vent_state": snapshot.get("pace_vent_status"),
            "pace_output_state": snapshot.get("pace_output_state"),
            "control_mode": "closed_pressure_swing",
            "setpoint_hpa": high_pressure_hpa,
            "measured_pressure_hpa": None,
            "pressure_source_name": None,
            "vent_state_during_low": None,
            "output_state_during_low": None,
            "isolation_state_during_low": None,
            "vent_state_after": None,
            "output_state_after": None,
            "isolation_state_after": None,
            "dewpoint_before_cycle": _safe_float(raw_rows[-1].get("dewpoint_c")) if raw_rows else None,
            "dewpoint_after_cycle": None,
            "dewpoint_value": None,
            "abort_reason": "",
        }
        _record_event(
            actuation_events,
            run_id=run_id,
            process_variant=process_variant,
            layer=layer,
            repeat_index=repeat_index,
            gas_ppm=gas_ppm,
            pressure_target_hpa=None,
            chain_mode=chain_mode,
            event_type="closed_swing_cycle_start",
            event_value=cycle_index,
            note=f"closed_pressure_swing cycle {cycle_index} start",
        )
        high_context = _phase_context(
            process_variant=process_variant,
            layer=layer,
            repeat_index=repeat_index,
            phase="closed_pressure_swing_high_pressurize",
            gas_ppm=gas_ppm,
            route=route,
            chain_mode=chain_mode,
        )
        _record_event(
            actuation_events,
            run_id=run_id,
            process_variant=process_variant,
            layer=layer,
            repeat_index=repeat_index,
            gas_ppm=gas_ppm,
            pressure_target_hpa=int(round(high_pressure_hpa)),
            chain_mode=chain_mode,
            event_type="pressure_target_change",
            event_value=high_pressure_hpa,
            note=f"closed_pressure_swing cycle {cycle_index} high target",
        )
        try:
            pace.set_setpoint(high_pressure_hpa)
        except Exception as exc:
            abort_reason = f"set_high_pressure_failed:{exc}"
            cycle_trace["abort_reason"] = abort_reason
            trace_rows.append(cycle_trace)
            break
        high_rows, phase_elapsed_offset_s, reached_high_ts, reached_high, step_abort_reason = _capture_until_pressure_target(
            runner,
            analyzer,
            devices,
            context=high_context,
            gas_start_mono=gas_start_mono,
            controller_vent_state="VENT_CLOSED",
            sample_poll_s=sample_poll_s,
            print_every_s=print_every_s,
            target_hpa=high_pressure_hpa,
            timeout_s=remaining_total_s,
            tolerance_hpa=high_tolerance_hpa,
            phase_elapsed_offset_s=phase_elapsed_offset_s,
            require_vent_closed_verified=require_vent_closed_verified,
            target_mode="at_or_above",
        )
        raw_rows.extend(high_rows)
        cycle_trace["reached_high_pressure_ts"] = reached_high_ts
        if not reached_high:
            snapshot, _ = _closed_pressure_swing_state_snapshot(runner, pace)
            cycle_trace["vent_state_after"] = snapshot.get("pace_vent_status")
            cycle_trace["output_state_after"] = snapshot.get("pace_output_state")
            cycle_trace["isolation_state_after"] = snapshot.get("pace_isolation_state")
            abort_reason = step_abort_reason or f"high_pressure_not_reached:{high_pressure_hpa}"
            cycle_trace["abort_reason"] = abort_reason
            trace_rows.append(cycle_trace)
            break
        strategy_state["closed_pressure_swing_high_reached"] = True
        cycle_trace["high_reached_boolean"] = True
        measured_pressure, pressure_source_name, dewpoint_value = _latest_pressure_snapshot(raw_rows)
        cycle_trace["measured_pressure_hpa"] = measured_pressure
        cycle_trace["pressure_source_name"] = pressure_source_name
        cycle_trace["dewpoint_value"] = dewpoint_value
        _record_event(
            actuation_events,
            run_id=run_id,
            process_variant=process_variant,
            layer=layer,
            repeat_index=repeat_index,
            gas_ppm=gas_ppm,
            pressure_target_hpa=int(round(high_pressure_hpa)),
            chain_mode=chain_mode,
            event_type="high_pressure_reached",
            event_value=high_pressure_hpa,
            note=f"closed_pressure_swing cycle {cycle_index} reached high threshold",
        )

        # Once the line is charged with dry gas, close every upstream feed valve
        # (source + main/total feed path) and keep only the downstream analysis path
        # open so the low-pressure swing happens in a truly closed volume.
        _record_event(
            actuation_events,
            run_id=run_id,
            process_variant=process_variant,
            layer=layer,
            repeat_index=repeat_index,
            gas_ppm=gas_ppm,
            pressure_target_hpa=None,
            chain_mode=chain_mode,
            event_type="seal_route_begin",
            event_value="seal_route_begin",
            note=f"closed_pressure_swing cycle {cycle_index} seal route begin",
        )
        runner._apply_valve_states(_closed_swing_open_valves(runner, route))
        source_closed_for_cycle = True
        strategy_state["closed_pressure_swing_seal_command_issued"] = True
        cycle_trace["seal_command_issued"] = True
        _record_event(
            actuation_events,
            run_id=run_id,
            process_variant=process_variant,
            layer=layer,
            repeat_index=repeat_index,
            gas_ppm=gas_ppm,
            pressure_target_hpa=None,
            chain_mode=chain_mode,
            event_type="gas_source_close",
            event_value=route.get("source_valve"),
            note=f"closed_pressure_swing cycle {cycle_index} close upstream feed valves after high target",
        )
        if valve_settle_s > 0.0:
            settle_context = _phase_context(
                process_variant=process_variant,
                layer=layer,
                repeat_index=repeat_index,
                phase="closed_pressure_swing_post_close_settle",
                gas_ppm=gas_ppm,
                route=route,
                chain_mode=chain_mode,
            )
            settle_rows = _capture_phase_rows(
                analyzer,
                devices,
                context=settle_context,
                gas_start_mono=gas_start_mono,
                duration_s=min(valve_settle_s, max_total_extra_s),
                sample_poll_s=sample_poll_s,
                print_every_s=print_every_s,
                controller_vent_state="VENT_CLOSED",
                phase_elapsed_offset_s=phase_elapsed_offset_s,
            )
            raw_rows.extend(settle_rows)
            if settle_rows:
                phase_elapsed_offset_s = _safe_float(settle_rows[-1].get("phase_elapsed_s")) or phase_elapsed_offset_s
        valve_failures = _closed_swing_feed_close_failures(runner, route)
        if valve_failures:
            abort_reason = "closed_volume_not_verified:" + ",".join(valve_failures)
            cycle_trace["abort_reason"] = abort_reason
            snapshot, _ = _closed_pressure_swing_state_snapshot(runner, pace)
            cycle_trace["vent_state_after"] = snapshot.get("pace_vent_status")
            cycle_trace["output_state_after"] = snapshot.get("pace_output_state")
            cycle_trace["isolation_state_after"] = snapshot.get("pace_isolation_state")
            trace_rows.append(cycle_trace)
            break
        strategy_state["closed_pressure_swing_seal_verified"] = True
        cycle_trace["seal_verified"] = True
        cycle_trace["gas_supply_open_state"] = _closed_swing_supply_open_state(runner, route)
        cycle_trace["route_valve_state"] = _commanded_valve_state(runner, _safe_int(route.get("path_valve")) or -1)
        cycle_trace["manifold_valve_state"] = _closed_swing_manifold_state(runner, route)
        _record_event(
            actuation_events,
            run_id=run_id,
            process_variant=process_variant,
            layer=layer,
            repeat_index=repeat_index,
            gas_ppm=gas_ppm,
            pressure_target_hpa=None,
            chain_mode=chain_mode,
            event_type="seal_route_done",
            event_value="seal_route_done",
            note=f"closed_pressure_swing cycle {cycle_index} seal route verified",
        )

        remaining_total_s = max_total_extra_s - (time.monotonic() - overall_start_mono)
        if remaining_total_s <= 0.0:
            abort_reason = "max_total_extra_s_exhausted"
            cycle_trace["abort_reason"] = abort_reason
            trace_rows.append(cycle_trace)
            break

        low_context = _phase_context(
            process_variant=process_variant,
            layer=layer,
            repeat_index=repeat_index,
            phase="closed_pressure_swing_low_pressurize",
            gas_ppm=gas_ppm,
            route=route,
            chain_mode=chain_mode,
        )
        _record_event(
            actuation_events,
            run_id=run_id,
            process_variant=process_variant,
            layer=layer,
            repeat_index=repeat_index,
            gas_ppm=gas_ppm,
            pressure_target_hpa=int(round(low_pressure_hpa)),
            chain_mode=chain_mode,
            event_type="pressure_target_change",
            event_value=low_pressure_hpa,
            note=f"closed_pressure_swing cycle {cycle_index} low target",
        )
        _record_event(
            actuation_events,
            run_id=run_id,
            process_variant=process_variant,
            layer=layer,
            repeat_index=repeat_index,
            gas_ppm=gas_ppm,
            pressure_target_hpa=int(round(low_pressure_hpa)),
            chain_mode=chain_mode,
            event_type="sealed_control_to_low_begin",
            event_value=low_pressure_hpa,
            note=f"closed_pressure_swing cycle {cycle_index} sealed control to low begin",
        )
        strategy_state["closed_pressure_swing_sealed_control_to_low_started"] = True
        try:
            pace.set_setpoint(low_pressure_hpa)
        except Exception as exc:
            abort_reason = f"set_low_pressure_failed:{exc}"
            cycle_trace["abort_reason"] = abort_reason
            trace_rows.append(cycle_trace)
            break
        low_rows, phase_elapsed_offset_s, reached_low_ts, reached_low, step_abort_reason = _capture_until_pressure_target(
            runner,
            analyzer,
            devices,
            context=low_context,
            gas_start_mono=gas_start_mono,
            controller_vent_state="VENT_CLOSED",
            sample_poll_s=sample_poll_s,
            print_every_s=print_every_s,
            target_hpa=low_pressure_hpa,
            timeout_s=remaining_total_s,
            tolerance_hpa=low_tolerance_hpa,
            phase_elapsed_offset_s=phase_elapsed_offset_s,
            require_vent_closed_verified=require_vent_closed_verified,
        )
        raw_rows.extend(low_rows)
        cycle_trace["reached_low_pressure_ts"] = reached_low_ts
        snapshot, failures = _closed_pressure_swing_state_snapshot(runner, pace)
        cycle_trace["vent_state_during_low"] = snapshot.get("pace_vent_status")
        cycle_trace["output_state_during_low"] = snapshot.get("pace_output_state")
        cycle_trace["isolation_state_during_low"] = snapshot.get("pace_isolation_state")
        cycle_trace["pace_vent_state"] = snapshot.get("pace_vent_status")
        cycle_trace["pace_output_state"] = snapshot.get("pace_output_state")
        cycle_trace["setpoint_hpa"] = low_pressure_hpa
        if not reached_low:
            cycle_trace["vent_state_after"] = snapshot.get("pace_vent_status")
            cycle_trace["output_state_after"] = snapshot.get("pace_output_state")
            cycle_trace["isolation_state_after"] = snapshot.get("pace_isolation_state")
            abort_reason = step_abort_reason or f"low_pressure_not_reached:{low_pressure_hpa}"
            cycle_trace["abort_reason"] = abort_reason
            trace_rows.append(cycle_trace)
            break
        _record_event(
            actuation_events,
            run_id=run_id,
            process_variant=process_variant,
            layer=layer,
            repeat_index=repeat_index,
            gas_ppm=gas_ppm,
            pressure_target_hpa=int(round(low_pressure_hpa)),
            chain_mode=chain_mode,
            event_type="low_pressure_reached",
            event_value=low_pressure_hpa,
            note=f"closed_pressure_swing cycle {cycle_index} reached low target",
        )
        if require_vent_closed_verified and failures:
            abort_reason = "vent_closed_not_verified:" + ",".join(failures)
            cycle_trace["abort_reason"] = abort_reason
            trace_rows.append(cycle_trace)
            break

        remaining_total_s = max_total_extra_s - (time.monotonic() - overall_start_mono)
        if remaining_total_s <= 0.0:
            abort_reason = "max_total_extra_s_exhausted"
            cycle_trace["abort_reason"] = abort_reason
            trace_rows.append(cycle_trace)
            break

        low_hold_context = _phase_context(
            process_variant=process_variant,
            layer=layer,
            repeat_index=repeat_index,
            phase="closed_pressure_swing_low_hold",
            gas_ppm=gas_ppm,
            route=route,
            chain_mode=chain_mode,
        )
        low_hold_rows = _capture_phase_rows(
            analyzer,
            devices,
            context=low_hold_context,
            gas_start_mono=gas_start_mono,
            duration_s=min(low_hold_s, remaining_total_s),
            sample_poll_s=sample_poll_s,
            print_every_s=print_every_s,
            controller_vent_state="VENT_CLOSED",
            phase_elapsed_offset_s=phase_elapsed_offset_s,
        )
        raw_rows.extend(low_hold_rows)
        if low_hold_rows:
            phase_elapsed_offset_s = _safe_float(low_hold_rows[-1].get("phase_elapsed_s")) or phase_elapsed_offset_s
        snapshot, failures = _closed_pressure_swing_state_snapshot(runner, pace)
        cycle_trace["vent_state_during_low"] = snapshot.get("pace_vent_status")
        cycle_trace["output_state_during_low"] = snapshot.get("pace_output_state")
        cycle_trace["isolation_state_during_low"] = snapshot.get("pace_isolation_state")
        cycle_trace["pace_vent_state"] = snapshot.get("pace_vent_status")
        cycle_trace["pace_output_state"] = snapshot.get("pace_output_state")
        cycle_trace["setpoint_hpa"] = low_pressure_hpa
        _record_event(
            actuation_events,
            run_id=run_id,
            process_variant=process_variant,
            layer=layer,
            repeat_index=repeat_index,
            gas_ppm=gas_ppm,
            pressure_target_hpa=int(round(low_pressure_hpa)),
            chain_mode=chain_mode,
            event_type="low_hold_begin",
            event_value=low_hold_s,
            note=f"closed_pressure_swing cycle {cycle_index} low hold begin",
        )
        if require_vent_closed_verified and failures:
            abort_reason = "vent_closed_not_verified:" + ",".join(failures)
            cycle_trace["abort_reason"] = abort_reason
            cycle_trace["vent_state_after"] = snapshot.get("pace_vent_status")
            cycle_trace["output_state_after"] = snapshot.get("pace_output_state")
            cycle_trace["isolation_state_after"] = snapshot.get("pace_isolation_state")
            trace_rows.append(cycle_trace)
            break

        if cycle_index >= cycles_requested:
            measured_pressure, pressure_source_name, dewpoint_value = _latest_pressure_snapshot(raw_rows)
            cycle_trace["measured_pressure_hpa"] = measured_pressure
            cycle_trace["pressure_source_name"] = pressure_source_name
            cycle_trace["dewpoint_value"] = dewpoint_value
            cycle_trace["dewpoint_after_cycle"] = dewpoint_value
            cycle_trace["gas_supply_open_state"] = _closed_swing_supply_open_state(runner, route)
            cycle_trace["route_valve_state"] = _commanded_valve_state(runner, _safe_int(route.get("path_valve")) or -1)
            cycle_trace["manifold_valve_state"] = _closed_swing_manifold_state(runner, route)
            _record_event(
                actuation_events,
                run_id=run_id,
                process_variant=process_variant,
                layer=layer,
                repeat_index=repeat_index,
                gas_ppm=gas_ppm,
                pressure_target_hpa=None,
                chain_mode=chain_mode,
                event_type="cycle_complete",
                event_value=cycle_index,
                note=f"closed_pressure_swing cycle {cycle_index} complete after low hold",
            )
            trace_rows.append(cycle_trace)
            strategy_state["closed_pressure_swing_cycles_completed"] = cycle_index
            break

        remaining_total_s = max_total_extra_s - (time.monotonic() - overall_start_mono)
        if remaining_total_s <= 0.0:
            abort_reason = "max_total_extra_s_exhausted"
            cycle_trace["abort_reason"] = abort_reason
            trace_rows.append(cycle_trace)
            break

        repressurize_context = _phase_context(
            process_variant=process_variant,
            layer=layer,
            repeat_index=repeat_index,
            phase="closed_pressure_swing_repressurize",
            gas_ppm=gas_ppm,
            route=route,
            chain_mode=chain_mode,
        )
        _record_event(
            actuation_events,
            run_id=run_id,
            process_variant=process_variant,
            layer=layer,
            repeat_index=repeat_index,
            gas_ppm=gas_ppm,
            pressure_target_hpa=int(round(high_pressure_hpa)),
            chain_mode=chain_mode,
            event_type="pressure_target_change",
            event_value=high_pressure_hpa,
            note=f"closed_pressure_swing cycle {cycle_index} repressurize target",
        )
        _record_event(
            actuation_events,
            run_id=run_id,
            process_variant=process_variant,
            layer=layer,
            repeat_index=repeat_index,
            gas_ppm=gas_ppm,
            pressure_target_hpa=int(round(high_pressure_hpa)),
            chain_mode=chain_mode,
            event_type="repressurize_begin",
            event_value=high_pressure_hpa,
            note=f"closed_pressure_swing cycle {cycle_index} repressurize begin",
        )
        try:
            runner._apply_valve_states(route["open_logical_valves"])
            source_closed_for_cycle = False
            _record_event(
                actuation_events,
                run_id=run_id,
                process_variant=process_variant,
                layer=layer,
                repeat_index=repeat_index,
                gas_ppm=gas_ppm,
                pressure_target_hpa=None,
                chain_mode=chain_mode,
                event_type="gas_source_open",
                event_value=route.get("source_valve"),
                note=f"closed_pressure_swing cycle {cycle_index} reopen source for repressurize",
            )
            pace.set_setpoint(high_pressure_hpa)
        except Exception as exc:
            abort_reason = f"set_repressurize_failed:{exc}"
            cycle_trace["abort_reason"] = abort_reason
            trace_rows.append(cycle_trace)
            break
        repress_rows, phase_elapsed_offset_s, repressurized_ts, repressurized, step_abort_reason = _capture_until_pressure_target(
            runner,
            analyzer,
            devices,
            context=repressurize_context,
            gas_start_mono=gas_start_mono,
            controller_vent_state="VENT_CLOSED",
            sample_poll_s=sample_poll_s,
            print_every_s=print_every_s,
            target_hpa=high_pressure_hpa,
            timeout_s=remaining_total_s,
            tolerance_hpa=high_tolerance_hpa,
            phase_elapsed_offset_s=phase_elapsed_offset_s,
            require_vent_closed_verified=require_vent_closed_verified,
            target_mode="at_or_above",
        )
        raw_rows.extend(repress_rows)
        cycle_trace["repressurized_ts"] = repressurized_ts
        if not repressurized:
            snapshot, _ = _closed_pressure_swing_state_snapshot(runner, pace)
            cycle_trace["vent_state_after"] = snapshot.get("pace_vent_status")
            cycle_trace["output_state_after"] = snapshot.get("pace_output_state")
            cycle_trace["isolation_state_after"] = snapshot.get("pace_isolation_state")
            abort_reason = step_abort_reason or f"repressurize_not_reached:{high_pressure_hpa}"
            cycle_trace["abort_reason"] = abort_reason
            trace_rows.append(cycle_trace)
            break

        remaining_total_s = max_total_extra_s - (time.monotonic() - overall_start_mono)
        if remaining_total_s <= 0.0:
            abort_reason = "max_total_extra_s_exhausted"
            cycle_trace["abort_reason"] = abort_reason
            trace_rows.append(cycle_trace)
            break

        settle_context = _phase_context(
            process_variant=process_variant,
            layer=layer,
            repeat_index=repeat_index,
            phase="closed_pressure_swing_repressurize_settle",
            gas_ppm=gas_ppm,
            route=route,
            chain_mode=chain_mode,
        )
        settle_rows = _capture_phase_rows(
            analyzer,
            devices,
            context=settle_context,
            gas_start_mono=gas_start_mono,
            duration_s=min(settle_after_repressurize_s, remaining_total_s),
            sample_poll_s=sample_poll_s,
            print_every_s=print_every_s,
            controller_vent_state="VENT_CLOSED",
            phase_elapsed_offset_s=phase_elapsed_offset_s,
        )
        raw_rows.extend(settle_rows)
        if settle_rows:
            phase_elapsed_offset_s = _safe_float(settle_rows[-1].get("phase_elapsed_s")) or phase_elapsed_offset_s
        snapshot, failures = _closed_pressure_swing_state_snapshot(runner, pace)
        cycle_trace["vent_state_after"] = snapshot.get("pace_vent_status")
        cycle_trace["output_state_after"] = snapshot.get("pace_output_state")
        cycle_trace["isolation_state_after"] = snapshot.get("pace_isolation_state")
        cycle_trace["dewpoint_after_cycle"] = _safe_float(raw_rows[-1].get("dewpoint_c")) if raw_rows else None
        if require_vent_closed_verified and failures:
            abort_reason = "vent_closed_not_verified:" + ",".join(failures)
            cycle_trace["abort_reason"] = abort_reason
            trace_rows.append(cycle_trace)
            break
        trace_rows.append(cycle_trace)
        strategy_state["closed_pressure_swing_cycles_completed"] = cycle_index

    if source_closed_for_cycle:
        try:
            runner._apply_valve_states(route["open_logical_valves"])
            _record_event(
                actuation_events,
                run_id=run_id,
                process_variant=process_variant,
                layer=layer,
                repeat_index=repeat_index,
                gas_ppm=gas_ppm,
                pressure_target_hpa=None,
                chain_mode=chain_mode,
                event_type="gas_source_open",
                event_value=route.get("source_valve"),
                note="closed_pressure_swing restore source after cycle",
            )
        except Exception:
            pass

    strategy_state["closed_pressure_swing_vent_closed_verified"] = vent_closed_verified
    strategy_state["closed_pressure_swing_abort_reason"] = abort_reason
    strategy_state["closed_pressure_swing_total_extra_s"] = round(max(0.0, time.monotonic() - overall_start_mono), 3)
    strategy_state["extra_precondition_time_cost_s"] = strategy_state["closed_pressure_swing_total_extra_s"]
    _restore_closed_pressure_swing_slew(pace)
    return raw_rows, trace_rows, strategy_state


def _load_json_file(path: Path) -> Optional[Dict[str, Any]]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _load_csv_rows(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def _latest_chain_mode_artifacts(
    root_dir: Path,
    *,
    chain_mode: str,
    exclude_run_dir: Optional[Path] = None,
) -> Optional[Dict[str, Any]]:
    candidates: List[Tuple[float, Path, Dict[str, Any]]] = []
    if not root_dir.exists():
        return None
    for child in root_dir.iterdir():
        if not child.is_dir():
            continue
        if exclude_run_dir is not None and child.resolve() == exclude_run_dir.resolve():
            continue
        setup_path = child / "setup_metadata.json"
        if not setup_path.exists():
            continue
        metadata = _load_json_file(setup_path)
        if not isinstance(metadata, dict):
            continue
        if str(metadata.get("smoke_level") or "") != "analyzer-chain-isolation":
            continue
        if str(metadata.get("chain_mode") or "") != str(chain_mode):
            continue
        candidates.append((setup_path.stat().st_mtime, child, metadata))
    if not candidates:
        return None
    _, run_dir, metadata = sorted(candidates, key=lambda item: item[0], reverse=True)[0]
    summary_rows = _load_csv_rows(run_dir / "isolation_summary.csv")
    summary_row = next(
        (row for row in summary_rows if str(row.get("chain_mode") or "") == str(chain_mode)),
        summary_rows[0] if summary_rows else {},
    )
    return {
        "run_dir": run_dir,
        "setup_metadata": metadata,
        "raw_rows": _load_csv_rows(run_dir / "raw_timeseries.csv"),
        "flush_gate_trace_rows": _load_csv_rows(run_dir / "flush_gate_trace.csv"),
        "actuation_events": _load_csv_rows(run_dir / "actuation_events.csv"),
        "summary": dict(summary_row),
    }


def _chain_artifacts_from_run_dir(run_dir: Path, *, expected_chain_mode: Optional[str] = None) -> Optional[Dict[str, Any]]:
    setup_path = run_dir / "setup_metadata.json"
    if not setup_path.exists():
        return None
    metadata = _load_json_file(setup_path)
    if not isinstance(metadata, dict):
        return None
    if str(metadata.get("smoke_level") or "") != "analyzer-chain-isolation":
        return None
    if expected_chain_mode is not None and str(metadata.get("chain_mode") or "") != str(expected_chain_mode):
        return None
    summary_rows = _load_csv_rows(run_dir / "isolation_summary.csv")
    summary_row = next(
        (row for row in summary_rows if str(row.get("chain_mode") or "") == str(metadata.get("chain_mode") or "")),
        summary_rows[0] if summary_rows else {},
    )
    return {
        "run_dir": run_dir,
        "setup_metadata": metadata,
        "raw_rows": _load_csv_rows(run_dir / "raw_timeseries.csv"),
        "flush_gate_trace_rows": _load_csv_rows(run_dir / "flush_gate_trace.csv"),
        "actuation_events": _load_csv_rows(run_dir / "actuation_events.csv"),
        "summary": dict(summary_row),
    }


def _collect_4ch_chain_summaries(
    root_dir: Path,
    *,
    current_run_dir: Path,
    current_summary: Mapping[str, Any],
) -> List[Dict[str, Any]]:
    ordered_modes = (
        "analyzer_out_pace_out_keep_rest",
        "analyzer_in_pace_out_keep_rest",
        "analyzer_in_keep_rest",
        "analyzer_out_keep_rest",
    )
    collected: List[Dict[str, Any]] = []
    current_mode = str(current_summary.get("chain_mode") or "")
    for mode in ordered_modes:
        if mode == current_mode:
            summary = dict(current_summary)
        else:
            artifacts = _latest_chain_mode_artifacts(root_dir, chain_mode=mode, exclude_run_dir=current_run_dir)
            summary = dict(artifacts.get("summary") or {}) if artifacts is not None else {}
        if summary:
            collected.append(summary)
    return collected


def _load_reference_chain_summaries(reference_dirs: Sequence[Path]) -> List[Dict[str, Any]]:
    summaries: List[Dict[str, Any]] = []
    for run_dir in reference_dirs:
        artifacts = _chain_artifacts_from_run_dir(run_dir)
        if artifacts is None:
            continue
        summary_rows = artifacts.get("summary")
        if summary_rows:
            summaries.append(dict(summary_rows))
        comparison_path = run_dir / "isolation_comparison_summary.json"
        if comparison_path.exists():
            payload = _load_json_file(comparison_path)
            if isinstance(payload, dict):
                for row in payload.get("summaries", []) or []:
                    if isinstance(row, Mapping):
                        summaries.append(dict(row))
    deduped: Dict[str, Dict[str, Any]] = {}
    for row in summaries:
        case_name = _chain_mode_case_name(str(row.get("chain_mode") or ""))
        if case_name and case_name not in deduped:
            deduped[case_name] = row
    return list(deduped.values())


def _build_chain_setup_metadata(
    *,
    cfg: Mapping[str, Any],
    run_id: str,
    chain_mode: str,
    analyzer_count_in_path: Optional[int],
    analyzer_name: str,
    analyzer_cfg: Mapping[str, Any],
    analyzers_in_path: Sequence[Mapping[str, Any]],
    setup_note: str,
    operator_note: str,
    output_dir: Path,
) -> Dict[str, Any]:
    flags = _chain_mode_flags(chain_mode)
    resolved_count = len(list(analyzers_in_path))
    if resolved_count > 0:
        flags["analyzer_count_in_path"] = resolved_count
        flags["analyzer_chain_connected"] = True
    elif analyzer_count_in_path is not None:
        flags["analyzer_count_in_path"] = int(analyzer_count_in_path)
        flags["analyzer_chain_connected"] = int(analyzer_count_in_path) > 0
    chain_cfg = _diagnostic_chain_cfg(cfg)
    analyzer_text = _format_chain_analyzers(analyzers_in_path)
    return {
        "run_id": run_id,
        "smoke_level": "analyzer-chain-isolation",
        "chain_mode": chain_mode,
        "case_name": _chain_mode_case_name(chain_mode),
        "chain_label": str(chain_cfg.get("chain_label") or ""),
        "analyzer_count_in_path": flags["analyzer_count_in_path"],
        "analyzer_chain_connected": flags["analyzer_chain_connected"],
        "analyzers_in_path": [dict(item) for item in analyzers_in_path],
        "analyzers_in_path_text": analyzer_text,
        "capture_analyzer_name": analyzer_name,
        "capture_analyzer_device_id": str(analyzer_cfg.get("device_id") or ""),
        "capture_analyzer_port": str(analyzer_cfg.get("port") or ""),
        "pace_in_path": flags["pace_in_path"],
        "pace_expected_vent_on": flags["pace_expected_vent_on"],
        "controller_vent_expected": flags["controller_vent_expected"],
        "controller_vent_state": flags["controller_vent_state"],
        "valve_block_in_path": flags["valve_block_in_path"],
        "dewpoint_meter_in_path": flags["dewpoint_meter_in_path"],
        "gauge_in_path": flags["gauge_in_path"],
        "compare_vs_8ch_reference_dirs": [str(path) for path in _resolve_reference_dirs(cfg)],
        "compare_vs_baseline_reference_dir": str(_resolve_compare_vs_baseline_reference_dir(cfg) or ""),
        "setup_note": setup_note,
        "operator_note": operator_note,
        "output_dir": str(output_dir),
        "flush_vent_refresh_interval_s": 0.0,
        "flush_vent_refresh_interval_s_requested": 0.0,
        "flush_vent_refresh_interval_s_actual_mean": None,
        "flush_vent_refresh_interval_s_actual_max": None,
        "flush_vent_refresh_count": 0,
        "flush_vent_refresh_thread_used": False,
        **_closed_pressure_swing_defaults(cfg),
    }


def _export_analyzer_chain_isolation_results_with_fallback(
    output_dir: str | Path,
    **kwargs: Any,
) -> Dict[str, Path]:
    try:
        return export_analyzer_chain_isolation_results(output_dir, **kwargs)
    except ModuleNotFoundError as exc:
        if getattr(exc, "name", "") != "matplotlib" or not bool(kwargs.get("export_png", True)):
            raise
        _log("[CHAIN] matplotlib missing; retrying analyzer-chain export without PNG plots.")
        retry_kwargs = dict(kwargs)
        retry_kwargs["export_png"] = False
        return export_analyzer_chain_isolation_results(output_dir, **retry_kwargs)


def _run_analyzer_chain_isolation_capture(
    *,
    args: argparse.Namespace,
    cfg: Mapping[str, Any],
    logger: RunLogger,
    analyzer_name: str,
    analyzer_cfg: Mapping[str, Any],
    variant: VariantSpec,
    chain_mode: str,
) -> int:
    skip_gas_analyzer = bool(getattr(args, "skip_gas_analyzer", False))
    flush_vent_refresh_interval_s = max(0.0, float(getattr(args, "flush_vent_refresh_interval_s", 0.0) or 0.0))
    runtime_cfg = _prepare_runtime_cfg(
        cfg,
        analyzer_cfg,
        args,
        variant,
        skip_gas_analyzer=skip_gas_analyzer,
    )
    devices = _build_devices(
        runtime_cfg,
        analyzer_cfg,
        logger,
        skip_gas_analyzer=skip_gas_analyzer,
    )
    runner: Optional[CalibrationRunner] = None
    raw_rows: List[Dict[str, Any]] = []
    actuation_events: List[Dict[str, Any]] = []
    flush_gate_trace_rows: List[Dict[str, Any]] = []
    closed_pressure_swing_trace_rows: List[Dict[str, Any]] = []
    run_id = str(getattr(logger, "run_id", "") or logger.run_dir.name)
    analyzers_in_path = _resolve_chain_analyzers(cfg, chain_mode)
    setup_metadata = _build_chain_setup_metadata(
        cfg=cfg,
        run_id=run_id,
        chain_mode=chain_mode,
        analyzer_count_in_path=args.analyzer_count_in_path,
        analyzer_name=analyzer_name,
        analyzer_cfg=analyzer_cfg,
        analyzers_in_path=analyzers_in_path,
        setup_note=str(args.setup_note or ""),
        operator_note=str(args.operator_note or ""),
        output_dir=logger.run_dir,
    )
    analyzer_sampling_enabled = bool(setup_metadata.get("analyzer_chain_connected")) and not skip_gas_analyzer
    skipped_analyzers = [
        str(item.get("name") or "").strip()
        for item in list(setup_metadata.get("analyzers_in_path") or [])
        if isinstance(item, Mapping) and str(item.get("name") or "").strip()
    ]
    setup_metadata.update(
        {
            "gas_analyzer_skipped": skip_gas_analyzer,
            "gas_analyzer_skip_reason": "diagnostic_only_focus_non_analyzer" if skip_gas_analyzer else "",
            "analyzer_sampling_enabled": analyzer_sampling_enabled,
            "flush_vent_refresh_interval_s": flush_vent_refresh_interval_s,
            "flush_vent_refresh_interval_s_requested": flush_vent_refresh_interval_s,
            "flush_vent_refresh_interval_s_actual_mean": None,
            "flush_vent_refresh_interval_s_actual_max": None,
            "flush_vent_refresh_count": 0,
            "flush_vent_refresh_thread_used": False,
        }
    )
    if skip_gas_analyzer:
        setup_metadata.update(
            {
                "analyzer_startup_gate_policy": "skipped_diagnostic_only_non_analyzer_focus",
                "capture_analyzer_startup_label": str(analyzer_name or ""),
                "capture_analyzer_startup_status": "skipped",
                "capture_analyzer_startup_fail_reason": "",
                "non_capture_analyzers_skipped": list(skipped_analyzers),
                "non_capture_analyzer_startup_policy": "skipped_with_gas_analyzer_skip",
            }
        )
    operator_checklist = _chain_mode_checklist_text(chain_mode)
    _log_operator_checklist(operator_checklist)
    _log(
        f"[CHAIN] resolved analyzers in path ({setup_metadata.get('analyzer_count_in_path')}): "
        f"{setup_metadata.get('analyzers_in_path_text') or '--'}"
    )
    _log(
        f"[CHAIN] capture analyzer: {setup_metadata.get('capture_analyzer_name')} "
        f"[{setup_metadata.get('capture_analyzer_device_id')}]@{setup_metadata.get('capture_analyzer_port')}"
    )
    if skip_gas_analyzer:
        _log(
            "[CHAIN] gas analyzer startup/sampling skipped | "
            "reason=diagnostic_only_focus_non_analyzer | physical chain may remain in path."
        )
    analyzer_for_capture = devices.get("gas_analyzer") if analyzer_sampling_enabled else None
    pace_in_path = bool(setup_metadata.get("pace_in_path"))
    controller_vent_expected = bool(setup_metadata.get("controller_vent_expected"))
    controller_vent_state_label = str(setup_metadata.get("controller_vent_state") or ("VENT_ON" if pace_in_path else "NOT_APPLICABLE"))
    legacy_safe_vent_active = False
    safe_stop_result: Dict[str, Any] = {}
    abort_reason = ""
    runtime_safe_stop_attempted = False
    try:
        legacy_safe_vent_policy = _analyzer_chain_legacy_safe_vent_policy(
            runtime_cfg or cfg,
            pace=devices.get("pace"),
            pace_in_path=pace_in_path,
        )
        setup_metadata.update(legacy_safe_vent_policy)
        legacy_safe_vent_active = bool(legacy_safe_vent_policy.get("legacy_safe_vent_active"))
        if bool(legacy_safe_vent_policy.get("legacy_safe_vent_required")) and not legacy_safe_vent_active:
            fail_reason = str(legacy_safe_vent_policy.get("legacy_safe_vent_fail_reason") or "policy_blocked")
            raise RuntimeError(
                "legacy_safe_vent_blocked("
                f"action=analyzer_chain_isolation,step=policy,last_status=unknown,output_state=unknown,"
                f"isolation_state=unknown,recoverable=true,reason={fail_reason})"
            )

        runner = CalibrationRunner(runtime_cfg, devices, logger, _log, lambda *_: None)
        if pace_in_path:
            runner._configure_devices(configure_gas_analyzers=False)
        else:
            detached_pace = runner.devices.pop("pace", None)
            try:
                runner._configure_devices(configure_gas_analyzers=False)
            finally:
                if detached_pace is not None:
                    runner.devices["pace"] = detached_pace

        if analyzer_sampling_enabled:
            _configure_analyzer_chain_capture_startup(
                runner,
                analyzer=analyzer_for_capture,
                analyzer_name=analyzer_name,
                analyzer_cfg=analyzer_cfg,
                setup_metadata=setup_metadata,
            )
        group_pref = _normalize_group(args.co2_group)
        gas_ppm = 0
        source_point = _build_source_point(gas_ppm, index=0, co2_group=group_pref)
        route = _route_for_point(runner, source_point)
        route["gas_ppm"] = gas_ppm
        gas_start_mono = time.monotonic()
        _log(
            f"[CHAIN] {chain_mode} -> 0 ppm | analyzer_connected={setup_metadata.get('analyzer_chain_connected')} "
            f"count={setup_metadata.get('analyzer_count_in_path')} source={route['source_valve']} path={route['path_valve']}"
        )
        deadtime_rows, actual_deadtime_s = _switch_gas_route(
            runner,
            analyzer_for_capture,
            devices,
            previous_route=None,
            next_route=route,
            process_variant=variant.name,
            layer=0,
            repeat_index=1,
            gas_ppm=gas_ppm,
            gas_start_mono=gas_start_mono,
            sample_poll_s=float(args.sample_poll_s),
            print_every_s=float(args.print_every_s),
            expected_deadtime_s=float(args.gas_switch_deadtime_s),
            actuation_events=actuation_events,
            run_id=run_id,
            chain_mode=chain_mode,
            controller_vent_state_label=controller_vent_state_label,
            pace_in_path=pace_in_path,
            initial_baseline_handler=(
                lambda: _apply_analyzer_chain_pressure_baseline(
                    runner,
                    cfg=runtime_cfg or cfg,
                    reason=f"{variant.name} layer0 repeat1 first gas",
                    legacy_safe_vent_active=legacy_safe_vent_active,
                    run_id=run_id,
                    actuation_events=actuation_events,
                    process_variant=variant.name,
                    layer=0,
                    repeat_index=1,
                    gas_ppm=gas_ppm,
                    chain_mode=chain_mode,
                    action="analyzer_chain_initial_baseline",
                )
            )
            if pace_in_path
            else None,
        )
        raw_rows.extend(_annotate_chain_mode_rows(deadtime_rows, chain_mode=chain_mode))
        predry_rows, closed_pressure_swing_trace_rows, closed_pressure_swing_state = _run_closed_pressure_swing_predry(
            runner,
            analyzer_for_capture,
            devices,
            cfg=cfg,
            process_variant=variant.name,
            layer=0,
            repeat_index=1,
            gas_ppm=gas_ppm,
            route=route,
            gas_start_mono=gas_start_mono,
            sample_poll_s=float(args.sample_poll_s),
            print_every_s=float(args.print_every_s),
            actuation_events=actuation_events,
            run_id=run_id,
            chain_mode=chain_mode,
        )
        setup_metadata.update(closed_pressure_swing_state)
        raw_rows.extend(_annotate_chain_mode_rows(predry_rows, chain_mode=chain_mode))
        flush_stop_reason = str(closed_pressure_swing_state.get("closed_pressure_swing_abort_reason") or "")
        if flush_stop_reason:
            flush_summary = build_flush_summary(
                predry_rows or deadtime_rows,
                process_variant=variant.name,
                layer=0,
                repeat_index=1,
                gas_ppm=gas_ppm,
                actual_deadtime_s=actual_deadtime_s,
                actuation_events=actuation_events,
                min_flush_s=float(args.precondition_min_flush_s),
                target_flush_s=float(args.precondition_min_flush_s),
                gate_window_s=float(args.precondition_window_s),
                require_ratio=analyzer_sampling_enabled,
                require_vent_on=False,
                rebound_window_s=float(args.rebound_window_s),
                rebound_min_rise_c=float(args.rebound_min_rise_c),
            )
            flush_summary["flush_gate_status"] = "fail"
            flush_summary["flush_gate_pass"] = False
            flush_summary["flush_gate_fail_reason"] = f"closed_pressure_swing_abort;{flush_stop_reason}"
            flush_summary["flush_gate_reason"] = flush_summary["flush_gate_fail_reason"]
            flush_summary["flush_duration_s"] = _safe_float(closed_pressure_swing_state.get("closed_pressure_swing_total_extra_s"))
            flush_rows: List[Dict[str, Any]] = []
            flush_gate_row = {}
        else:
            flush_rows, flush_summary, flush_gate_row, flush_stop_reason = _run_flush_phase(
                runner,
                analyzer_for_capture,
                devices,
                process_variant=variant.name,
                layer=0,
                repeat_index=1,
                gas_ppm=gas_ppm,
                route=route,
                gas_start_mono=gas_start_mono,
                min_flush_s=float(args.precondition_min_flush_s),
                target_flush_s=float(args.precondition_min_flush_s),
                max_flush_s=float(args.precondition_max_flush_s),
                gate_window_s=float(args.precondition_window_s),
                rebound_window_s=float(args.rebound_window_s),
                rebound_min_rise_c=float(args.rebound_min_rise_c),
                sample_poll_s=float(args.sample_poll_s),
                print_every_s=float(args.print_every_s),
                actual_deadtime_s=actual_deadtime_s,
                actuation_events=actuation_events,
                flush_gate_trace_rows=flush_gate_trace_rows,
                run_id=run_id,
                require_ratio=analyzer_sampling_enabled,
                require_vent_on=controller_vent_expected,
                controller_vent_state_label=controller_vent_state_label,
                phase_name_override="isolation_flush_vent_on",
                gate_name_override="isolation_flush_gate",
                phase_label_override="isolation_flush",
                chain_mode=chain_mode,
                vent_on_handler=(
                    lambda vent_reason: _run_legacy_safe_vent_action(
                        runner,
                        cfg=runtime_cfg or cfg,
                        action="analyzer_chain_flush_vent",
                        reason=vent_reason,
                        run_id=run_id,
                        actuation_events=actuation_events,
                        process_variant=variant.name,
                        layer=0,
                        repeat_index=1,
                        gas_ppm=gas_ppm,
                        chain_mode=chain_mode,
                    )
                )
                if legacy_safe_vent_active
                else None,
                flush_vent_refresh_interval_s=flush_vent_refresh_interval_s,
                vent_refresh_handler=(
                    lambda vent_reason: _run_legacy_safe_vent_refresh_action(
                        runner,
                        action="analyzer_chain_flush_vent_refresh",
                    )
                )
                if legacy_safe_vent_active and flush_vent_refresh_interval_s > 0.0
                else None,
                flush_vent_refresh_wall_clock_heartbeat=bool(skip_gas_analyzer),
                setup_metadata=setup_metadata,
            )
        safe_stop_result = _perform_runtime_safe_stop(
            devices,
            runner=runner,
            cfg=runtime_cfg or cfg,
            run_id=run_id,
            actuation_events=actuation_events,
            process_variant=variant.name,
            layer=0,
            repeat_index=1,
            gas_ppm=gas_ppm,
            note=flush_stop_reason or str(flush_summary.get("flush_gate_status") or "isolation_complete"),
            chain_mode=chain_mode,
            pace_in_path=pace_in_path,
            pace_mode="diagnostic_safe_vent" if legacy_safe_vent_active else "default",
        )
        runtime_safe_stop_attempted = True
        if safe_stop_result and not bool(safe_stop_result.get("safe_stop_verified", True)):
            safe_stop_issues = [str(item) for item in list(safe_stop_result.get("safe_stop_issues") or []) if str(item)]
            raise RuntimeError(safe_stop_issues[0] if safe_stop_issues else "safe_stop_not_verified")
        raw_rows.extend(_annotate_chain_mode_rows(flush_rows, chain_mode=chain_mode, isolation_phase_name="isolation_flush_vent_on"))
        flush_gate_trace_rows = _annotate_chain_mode_rows(flush_gate_trace_rows, chain_mode=chain_mode, isolation_phase_name="isolation_flush_vent_on")
        actuation_events = _annotate_chain_mode_events(actuation_events, chain_mode=chain_mode)
        isolation_summary = build_analyzer_chain_isolation_summary(
            flush_summary,
            run_id=run_id,
            smoke_level="analyzer-chain-isolation",
            chain_mode=chain_mode,
            setup_metadata=setup_metadata,
        )
        if chain_mode == "analyzer_out_keep_rest":
            counterpart_mode = "analyzer_in_keep_rest"
        elif chain_mode == "analyzer_out_pace_out_keep_rest":
            counterpart_mode = None
        elif chain_mode == "analyzer_in_pace_out_keep_rest":
            counterpart_mode = "analyzer_in_keep_rest"
        else:
            counterpart_mode = "analyzer_out_keep_rest"
        counterpart = (
            _latest_chain_mode_artifacts(logger.run_dir.parent, chain_mode=counterpart_mode, exclude_run_dir=logger.run_dir)
            if counterpart_mode
            else None
        )
        isolation_summaries = [isolation_summary]
        combined_raw_rows = list(raw_rows)
        combined_trace_rows = list(flush_gate_trace_rows)
        combined_events = list(actuation_events)
        if counterpart is not None:
            isolation_summaries.append(dict(counterpart.get("summary") or {}))
            combined_raw_rows.extend(counterpart.get("raw_rows") or [])
            combined_trace_rows.extend(counterpart.get("flush_gate_trace_rows") or [])
            combined_events.extend(counterpart.get("actuation_events") or [])
        comparison_summary = build_analyzer_chain_isolation_comparison(isolation_summaries)
        comparison_summary["pace_vs_standard_in_comparison"] = build_analyzer_chain_pace_contribution_comparison(isolation_summaries)
        compare_vs_8ch_summary = build_analyzer_chain_compare_vs_8ch(
            _collect_4ch_chain_summaries(logger.run_dir.parent, current_run_dir=logger.run_dir, current_summary=isolation_summary),
            _load_reference_chain_summaries(_resolve_reference_dirs(cfg)),
        )
        baseline_reference_dir = _resolve_compare_vs_baseline_reference_dir(cfg)
        compare_vs_baseline_summary = None
        if baseline_reference_dir is not None:
            baseline_artifacts = _chain_artifacts_from_run_dir(baseline_reference_dir, expected_chain_mode="analyzer_in_keep_rest")
            if baseline_artifacts is not None:
                compare_vs_baseline_summary = build_analyzer_chain_compare_vs_baseline(
                    [isolation_summary],
                    [dict(baseline_artifacts.get("summary") or {})],
                )
        outputs = _export_analyzer_chain_isolation_results_with_fallback(
            logger.run_dir,
            raw_rows=combined_raw_rows,
            flush_gate_trace_rows=combined_trace_rows,
            actuation_events=combined_events,
            closed_pressure_swing_trace_rows=closed_pressure_swing_trace_rows,
            setup_metadata=setup_metadata,
            isolation_summaries=isolation_summaries,
            comparison_summary=comparison_summary,
            operator_checklist=operator_checklist,
            compare_vs_8ch_rows=compare_vs_8ch_summary.get("rows", []),
            compare_vs_8ch_summary=compare_vs_8ch_summary,
            compare_vs_baseline_rows=(compare_vs_baseline_summary or {}).get("rows", []),
            compare_vs_baseline_summary=compare_vs_baseline_summary,
            export_csv=not bool(args.no_export_csv),
            export_xlsx=not bool(args.no_export_xlsx),
            export_png=not bool(args.no_export_png),
        )
        _log(
            f"[CHAIN] result | chain_mode={chain_mode} classification={isolation_summary.get('classification')} "
            f"dominant={comparison_summary.get('dominant_isolation_conclusion')}"
        )
        for key in (
            "raw_timeseries",
            "flush_gate_trace",
            "actuation_events",
            "closed_pressure_swing_trace",
            "setup_metadata",
            "isolation_summary",
            "isolation_comparison_summary",
            "summary",
            "compare_vs_baseline_csv",
            "compare_vs_baseline_md",
            "compare_vs_8ch_csv",
            "compare_vs_8ch_md",
            "readable_report",
            "diagnostic_workbook",
        ):
            _log(f"{key} -> {outputs.get(key)}")
        return 0
    except Exception as exc:
        abort_reason = abort_reason or str(exc)
        raise
    finally:
        if devices and not args.no_restore_baseline:
            if not runtime_safe_stop_attempted:
                try:
                    early_abort_safe_stop = _perform_runtime_safe_stop(
                        devices,
                        runner=runner,
                        cfg=runtime_cfg or cfg,
                        run_id=run_id,
                        actuation_events=actuation_events,
                        process_variant=variant.name,
                        layer=0,
                        repeat_index=1,
                        gas_ppm=gas_ppm if "gas_ppm" in locals() else None,
                        note=abort_reason or "analyzer_chain_startup_abort",
                        chain_mode=chain_mode,
                        pace_in_path=pace_in_path,
                        pace_mode="diagnostic_safe_vent" if legacy_safe_vent_active else "default",
                    )
                    if early_abort_safe_stop and not bool(early_abort_safe_stop.get("safe_stop_verified", True)):
                        issues = [str(item) for item in list(early_abort_safe_stop.get("safe_stop_issues") or []) if str(item)]
                        _log(f"Early-abort safe stop incomplete: {', '.join(issues) if issues else 'safe_stop_not_verified'}")
                except Exception as exc:
                    _log(f"Early-abort safe stop failed: {exc}")
            try:
                restore_runner = runner or CalibrationRunner(runtime_cfg or cfg, devices, logger, _log, lambda *_: None)
                if pace_in_path:
                    _apply_analyzer_chain_pressure_baseline(
                        restore_runner,
                        cfg=runtime_cfg or cfg,
                        reason="after analyzer chain isolation diagnostic finish",
                        legacy_safe_vent_active=legacy_safe_vent_active,
                        run_id=run_id,
                        actuation_events=actuation_events,
                        process_variant=variant.name,
                        layer=0,
                        repeat_index=1,
                        gas_ppm=gas_ppm if "gas_ppm" in locals() else None,
                        chain_mode=chain_mode,
                        action="analyzer_chain_final_restore",
                    )
                    if legacy_safe_vent_active:
                        _log("Managed relay/pressure baseline restored with legacy diagnostic-safe vent.")
                    else:
                        _log("Managed relay/pressure baseline restored.")
                else:
                    restore_runner._apply_route_baseline_valves()
                    _log("PACE not in path: restored valve baseline only.")
            except Exception as exc:
                _log(f"Baseline restore failed: {exc}")
        _close_devices(devices)


def _run_analyzer_chain_isolation_compare_pair(
    *,
    args: argparse.Namespace,
    cfg: Mapping[str, Any],
    logger: RunLogger,
    analyzer_name: str,
    analyzer_cfg: Mapping[str, Any],
) -> int:
    setup_metadata = _build_chain_setup_metadata(
        cfg=cfg,
        run_id=str(getattr(logger, "run_id", "") or logger.run_dir.name),
        chain_mode="compare_pair",
        analyzer_count_in_path=None,
        analyzer_name=analyzer_name,
        analyzer_cfg=analyzer_cfg,
        analyzers_in_path=[],
        setup_note=str(args.setup_note or ""),
        operator_note=str(args.operator_note or ""),
        output_dir=logger.run_dir,
    )
    operator_checklist = _chain_mode_checklist_text("compare_pair")
    _log_operator_checklist(operator_checklist)
    root_dir = logger.run_dir.parent
    out_artifacts = _latest_chain_mode_artifacts(root_dir, chain_mode="analyzer_out_keep_rest", exclude_run_dir=logger.run_dir)
    in_artifacts = _latest_chain_mode_artifacts(root_dir, chain_mode="analyzer_in_keep_rest", exclude_run_dir=logger.run_dir)
    isolation_summaries: List[Dict[str, Any]] = []
    raw_rows: List[Dict[str, Any]] = []
    flush_gate_trace_rows: List[Dict[str, Any]] = []
    actuation_events: List[Dict[str, Any]] = []
    for artifacts in (out_artifacts, in_artifacts):
        if artifacts is None:
            continue
        isolation_summaries.append(dict(artifacts.get("summary") or {}))
        raw_rows.extend(artifacts.get("raw_rows") or [])
        flush_gate_trace_rows.extend(artifacts.get("flush_gate_trace_rows") or [])
        actuation_events.extend(artifacts.get("actuation_events") or [])
    comparison_summary = build_analyzer_chain_isolation_comparison(isolation_summaries)
    outputs = _export_analyzer_chain_isolation_results_with_fallback(
        logger.run_dir,
        raw_rows=raw_rows,
        flush_gate_trace_rows=flush_gate_trace_rows,
        actuation_events=actuation_events,
        setup_metadata=setup_metadata,
        isolation_summaries=isolation_summaries,
        comparison_summary=comparison_summary,
        operator_checklist=operator_checklist,
        export_csv=not bool(args.no_export_csv),
        export_xlsx=not bool(args.no_export_xlsx),
        export_png=not bool(args.no_export_png),
    )
    _log(
        f"[CHAIN] compare_pair | available={comparison_summary.get('comparison_available')} "
        f"dominant={comparison_summary.get('dominant_isolation_conclusion')}"
    )
    for key in (
        "raw_timeseries",
        "flush_gate_trace",
        "actuation_events",
        "setup_metadata",
        "isolation_summary",
        "isolation_comparison_summary",
        "readable_report",
        "diagnostic_workbook",
    ):
        _log(f"{key} -> {outputs.get(key)}")
    return 0


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = parse_args(argv)
    if not args.allow_live_hardware:
        _log("Safety gate: pass --allow-live-hardware to run this independent metrology diagnostic.")
        return 2
    if bool(args.skip_gas_analyzer) and args.smoke_level != "analyzer-chain-isolation":
        _log("--skip-gas-analyzer is limited to --smoke-level analyzer-chain-isolation.")
        return 2
    if float(args.min_flush_s) < 120.0:
        _log("min_flush_s must stay >= 120s.")
        return 2
    if float(args.screening_flush_s_default) < float(args.min_flush_s):
        _log("screening_flush_s_default must be >= min_flush_s.")
        return 2
    if float(args.max_flush_s) < float(args.screening_flush_s_default):
        _log("max_flush_s must be >= screening_flush_s_default.")
        return 2
    if float(args.precondition_min_flush_s) < 120.0:
        _log("precondition_min_flush_s must stay >= 120s.")
        return 2
    if float(args.precondition_max_flush_s) < float(args.precondition_min_flush_s):
        _log("precondition_max_flush_s must be >= precondition_min_flush_s.")
        return 2

    cfg = _load_cli_config(args.config)
    analyzer_name, analyzer_cfg = _resolve_selected_analyzer_cfg(cfg, args)
    logger = _make_logger(cfg, args.output_dir, args.run_id, smoke_level=args.smoke_level)
    if args.smoke_level == "analyzer-chain-isolation":
        try:
            _log(f"Analyzer-chain isolation output dir: {logger.run_dir}")
            if bool(args.skip_gas_analyzer) and not analyzer_cfg:
                _log(
                    "[CHAIN] selected analyzer config unavailable; continuing in skip mode with "
                    f"metadata-only capture label={analyzer_name}"
                )
            if args.chain_mode == "compare_pair":
                return _run_analyzer_chain_isolation_compare_pair(
                    args=args,
                    cfg=cfg,
                    logger=logger,
                    analyzer_name=analyzer_name,
                    analyzer_cfg=analyzer_cfg,
                )
            variant = _variant_specs(args)[0]
            return _run_analyzer_chain_isolation_capture(
                args=args,
                cfg=cfg,
                logger=logger,
                analyzer_name=analyzer_name,
                analyzer_cfg=analyzer_cfg,
                variant=variant,
                chain_mode=str(args.chain_mode),
            )
        finally:
            try:
                logger.close()
            except Exception:
                pass
    devices: Dict[str, Any] = {}
    runtime_cfg: Dict[str, Any] = {}
    raw_rows: List[Dict[str, Any]] = []
    actuation_events: List[Dict[str, Any]] = []
    flush_gate_trace_rows: List[Dict[str, Any]] = []
    precondition_summaries: List[Dict[str, Any]] = []
    flush_summaries: List[Dict[str, Any]] = []
    seal_hold_summaries: List[Dict[str, Any]] = []
    pressure_summaries: List[Dict[str, Any]] = []
    phase_gate_rows: List[Dict[str, Any]] = []
    variant_execution_meta: Dict[str, Dict[str, Any]] = {}
    run_fail_reason = ""

    try:
        _log(f"Metrology diagnostic output dir: {logger.run_dir}")
        _log(f"Selected analyzer: {analyzer_name}")
        logger.log_io(
            port="LOG",
            device="metrology_seal_pressure_v2",
            direction="EVENT",
            command="diagnostic-start",
            response="diagnostic_only=true writes_coefficients=false writes_senco=false changes_device_id=false v1_main_flow_unchanged=true v2_untouched=true",
        )

        variants = _variant_specs(args)
        layers = _layer_sequence(args)
        if 4 in layers and len(variants) != 1:
            _log("Layer 4 should be run with a single selected variant; use --variants B or similar.")
            return 2

        base_runtime_cfg = _prepare_runtime_cfg(cfg, analyzer_cfg, args, variants[0])
        devices = _build_devices(base_runtime_cfg, analyzer_cfg, logger)
        analyzer = devices["gas_analyzer"]
        if args.configure_analyzer_stream:
            _configure_analyzer_stream(analyzer, analyzer_cfg)
        else:
            analyzer.active_send = bool(analyzer_cfg.get("active_send", False))
        trace_path = logger.run_dir / "pressure_transition_trace.csv"
        pressure_summary_live_path = logger.run_dir / "pressure_point_summary.csv"
        phase_gate_live_path = logger.run_dir / "phase_gate_summary.csv"
        trace_cursor = _trace_row_count(trace_path)
        group_pref = _normalize_group(args.co2_group)
        point_index = 1000
        run_id = str(getattr(logger, "run_id", "") or logger.run_dir.name)
        stop_requested = False

        if bool(args.enable_precondition) and (bool(args.precondition_only) or 1 in layers):
            pre_variant = variants[0]
            runtime_cfg = _prepare_runtime_cfg(cfg, analyzer_cfg, args, pre_variant)
            pre_runner = CalibrationRunner(runtime_cfg, devices, logger, _log, lambda *_: None)
            pre_runner._configure_devices()
            pre_gas_ppm = int(args.precondition_gas_ppm)
            pre_point = _build_source_point(pre_gas_ppm, index=0, co2_group=group_pref)
            pre_route = _route_for_point(pre_runner, pre_point)
            pre_route["gas_ppm"] = pre_gas_ppm
            pre_gas_start_mono = time.monotonic()
            _log(
                f"[PRE] Layer 0 预调理 -> {pre_gas_ppm} ppm | "
                f"group={pre_route['group']} source={pre_route['source_valve']} path={pre_route['path_valve']}"
            )
            deadtime_rows, pre_deadtime_s = _switch_gas_route(
                pre_runner,
                analyzer,
                devices,
                previous_route=None,
                next_route=pre_route,
                process_variant=pre_variant.name,
                layer=0,
                repeat_index=0,
                gas_ppm=pre_gas_ppm,
                gas_start_mono=pre_gas_start_mono,
                sample_poll_s=float(args.sample_poll_s),
                print_every_s=float(args.print_every_s),
                expected_deadtime_s=float(args.gas_switch_deadtime_s),
                actuation_events=actuation_events,
                run_id=run_id,
            )
            raw_rows.extend(deadtime_rows)
            pre_rows, pre_summary, pre_gate_row, pre_stop_reason = _run_flush_phase(
                pre_runner,
                analyzer,
                devices,
                process_variant=pre_variant.name,
                layer=0,
                repeat_index=0,
                gas_ppm=pre_gas_ppm,
                route=pre_route,
                gas_start_mono=pre_gas_start_mono,
                min_flush_s=float(args.precondition_min_flush_s),
                target_flush_s=float(args.precondition_min_flush_s),
                max_flush_s=float(args.precondition_max_flush_s),
                gate_window_s=float(args.precondition_window_s),
                rebound_window_s=float(args.rebound_window_s),
                rebound_min_rise_c=float(args.rebound_min_rise_c),
                sample_poll_s=float(args.sample_poll_s),
                print_every_s=float(args.print_every_s),
                actual_deadtime_s=pre_deadtime_s,
                actuation_events=actuation_events,
                flush_gate_trace_rows=flush_gate_trace_rows,
                run_id=run_id,
            )
            raw_rows.extend(pre_rows)
            phase_gate_rows.append(pre_gate_row)
            precondition_summaries.append(_as_precondition_summary(pre_summary))
            _log(
                f"[PRE] Layer 0 预调理 | duration={pre_summary.get('flush_duration_s')} "
                f"gate={pre_summary.get('flush_gate_status')} rebound={pre_summary.get('dewpoint_rebound_detected')}"
            )
            if not pre_summary.get("flush_gate_pass"):
                run_fail_reason = pre_stop_reason or "precondition_failed"
                stop_requested = True
                _perform_runtime_safe_stop(
                    devices,
                    cfg=runtime_cfg or cfg,
                    run_id=run_id,
                    actuation_events=actuation_events,
                    process_variant=pre_variant.name,
                    layer=0,
                    repeat_index=0,
                    gas_ppm=pre_gas_ppm,
                    note=run_fail_reason,
                )
            elif bool(args.precondition_only):
                run_fail_reason = "precondition_only_complete"
                stop_requested = True
                _perform_runtime_safe_stop(
                    devices,
                    cfg=runtime_cfg or cfg,
                    run_id=run_id,
                    actuation_events=actuation_events,
                    process_variant=pre_variant.name,
                    layer=0,
                    repeat_index=0,
                    gas_ppm=pre_gas_ppm,
                    note=run_fail_reason,
                )

        for variant in ([] if stop_requested else variants):
            variant_execution_meta.setdefault(
                variant.name,
                {
                    "skipped_layers": [],
                    "skipped_due_to_previous_layer_failure": False,
                },
            )
            runtime_cfg = _prepare_runtime_cfg(cfg, analyzer_cfg, args, variant)
            runner = CalibrationRunner(runtime_cfg, devices, logger, _log, lambda *_: None)
            runner._configure_devices()
            _log(f"=== 工艺 {variant.name} 开始 | {variant.description} ===")

            for layer in layers:
                gas_sequence = _layer_gas_sequence(layer, args)
                pressure_points = _layer_pressure_points(layer, args)
                _log(f"--- Layer {layer} | variant={variant.name} | gas={gas_sequence} | pressure={pressure_points} ---")
                if layer in {2, 3, 4}:
                    prior_status = _variant_layer_status(
                        variant=variant.name,
                        flush_summaries=flush_summaries,
                        seal_hold_summaries=seal_hold_summaries,
                        pressure_summaries=pressure_summaries,
                        phase_gate_rows=phase_gate_rows,
                        args=args,
                    )
                    if layer > 1 and _should_stop_after_layer(
                        prior_status,
                        early_stop=bool(args.early_stop),
                        treat_insufficient_as_stop=bool(args.treat_insufficient_as_stop),
                    ):
                        skipped_layers = [one for one in layers if one >= layer]
                        variant_execution_meta[variant.name]["skipped_due_to_previous_layer_failure"] = True
                        variant_execution_meta[variant.name]["skipped_layers"] = sorted(
                            set(variant_execution_meta[variant.name]["skipped_layers"]) | set(skipped_layers)
                        )
                        phase_gate_rows.append(
                            build_phase_gate_row(
                                process_variant=variant.name,
                                layer=layer,
                                repeat_index=0,
                                gas_ppm=None,
                                pressure_target_hpa=None,
                                phase="layer_gate",
                                gate_name=f"layer_{layer}_entry",
                                gate_status="fail" if prior_status == "fail" else "insufficient_evidence",
                                gate_pass=False,
                                gate_window_s=None,
                                gate_value=prior_status,
                                gate_threshold="previous layers must pass",
                                gate_fail_reason="skipped_due_to_previous_layer_failure",
                            )
                        )
                        _record_event(
                            actuation_events,
                            run_id=run_id,
                            process_variant=variant.name,
                            layer=layer,
                            repeat_index=0,
                            gas_ppm=None,
                            pressure_target_hpa=None,
                            event_type="layer_skip",
                            event_value=prior_status,
                            note=f"skipped_due_to_previous_layer_failure; skipped_layers={skipped_layers}",
                        )
                        _log(f"Layer {layer} skipped for variant {variant.name}: previous layers status={prior_status}.")
                        break

                for repeat_index in range(1, int(args.repeats) + 1):
                    previous_route: Optional[Dict[str, Any]] = None
                    runner._set_co2_route_baseline(reason=f"{variant.name} layer{layer} repeat{repeat_index} begin")

                    for gas_order, gas_ppm in enumerate(gas_sequence, start=1):
                        source_point = _build_source_point(int(gas_ppm), index=gas_order, co2_group=group_pref)
                        route = _route_for_point(runner, source_point)
                        gas_start_mono = time.monotonic()
                        gas_start_ts = datetime.now()
                        _log(
                            f"[{variant.name}] L{layer} R{repeat_index} 切到 {gas_ppm} ppm | "
                            f"group={route['group']} source={route['source_valve']} path={route['path_valve']}"
                        )

                        deadtime_rows, actual_deadtime_s = _switch_gas_route(
                            runner,
                            analyzer,
                            devices,
                            previous_route=previous_route,
                            next_route=route,
                            process_variant=variant.name,
                            layer=layer,
                            repeat_index=repeat_index,
                            gas_ppm=int(gas_ppm),
                            gas_start_mono=gas_start_mono,
                            sample_poll_s=float(args.sample_poll_s),
                            print_every_s=float(args.print_every_s),
                            expected_deadtime_s=float(args.gas_switch_deadtime_s),
                            actuation_events=actuation_events,
                            run_id=run_id,
                        )
                        raw_rows.extend(deadtime_rows)
                        route["gas_ppm"] = gas_ppm

                        flush_rows, flush_summary, flush_gate_row, flush_stop_reason = _run_flush_phase(
                            runner,
                            analyzer,
                            devices,
                            process_variant=variant.name,
                            layer=layer,
                            repeat_index=repeat_index,
                            gas_ppm=int(gas_ppm),
                            route=route,
                            gas_start_mono=gas_start_mono,
                            min_flush_s=float(args.min_flush_s),
                            target_flush_s=float(args.screening_flush_s_default),
                            max_flush_s=float(args.max_flush_s),
                            gate_window_s=float(args.flush_gate_window_s),
                            rebound_window_s=float(args.rebound_window_s),
                            rebound_min_rise_c=float(args.rebound_min_rise_c),
                            sample_poll_s=float(args.sample_poll_s),
                            print_every_s=float(args.print_every_s),
                            actual_deadtime_s=actual_deadtime_s,
                            actuation_events=actuation_events,
                            flush_gate_trace_rows=flush_gate_trace_rows,
                            run_id=run_id,
                        )
                        raw_rows.extend(flush_rows)
                        flush_summaries.append(flush_summary)
                        phase_gate_rows.append(flush_gate_row)
                        _log(
                            f"[{variant.name}] L{layer} R{repeat_index} {gas_ppm} ppm flush | "
                            f"duration={flush_summary.get('flush_duration_s')} gate={flush_summary.get('flush_gate_status')} "
                            f"t95={flush_summary.get('flush_ratio_t95_s')} fail_reason={flush_summary.get('flush_gate_fail_reason')}"
                        )
                        if not flush_summary.get("flush_gate_pass"):
                            run_fail_reason = flush_stop_reason or str(flush_summary.get("flush_gate_fail_reason") or "flush_gate_failed")
                            stop_requested = True
                            _perform_runtime_safe_stop(
                                devices,
                                cfg=runtime_cfg or cfg,
                                run_id=run_id,
                                actuation_events=actuation_events,
                                process_variant=variant.name,
                                layer=layer,
                                repeat_index=repeat_index,
                                gas_ppm=int(gas_ppm),
                                note=run_fail_reason,
                            )
                            previous_route = route
                            break

                        if layer == 1:
                            previous_route = route
                            continue

                        if layer == 2:
                            hold_rows, hold_summary, hold_gate_row, trace_cursor = _run_sealed_hold_phase(
                                runner,
                                analyzer,
                                devices,
                                trace_path,
                                trace_cursor,
                                process_variant=variant.name,
                                layer=layer,
                                repeat_index=repeat_index,
                                gas_ppm=int(gas_ppm),
                                route=route,
                                source_point=source_point,
                                gas_start_mono=gas_start_mono,
                                gas_start_ts=gas_start_ts,
                                hold_duration_s=float(args.sealed_hold_s),
                                sample_poll_s=float(args.sample_poll_s),
                                print_every_s=float(args.print_every_s),
                                point_index=point_index,
                                actuation_events=actuation_events,
                                run_id=run_id,
                            )
                            point_index += 1
                            raw_rows.extend(hold_rows)
                            seal_hold_summaries.append(hold_summary)
                            phase_gate_rows.append(hold_gate_row)
                        else:
                            sweep_rows, sweep_summaries, sweep_gate_rows, trace_cursor, point_index = _run_pressure_sweep_phase(
                                runner,
                                analyzer,
                                devices,
                                trace_path,
                                trace_cursor,
                                pressure_summary_live_path,
                                phase_gate_live_path,
                                process_variant=variant.name,
                                layer=layer,
                                repeat_index=repeat_index,
                                gas_ppm=int(gas_ppm),
                                route=route,
                                source_point=source_point,
                                gas_start_mono=gas_start_mono,
                                gas_start_ts=gas_start_ts,
                                pressure_points=pressure_points,
                                stable_window_s=float(args.stable_window_s),
                                sample_poll_s=float(args.sample_poll_s),
                                print_every_s=float(args.print_every_s),
                                point_index_start=point_index,
                                actuation_events=actuation_events,
                                run_id=run_id,
                            )
                            raw_rows.extend(sweep_rows)
                            pressure_summaries.extend(sweep_summaries)
                            phase_gate_rows.extend(sweep_gate_rows)

                        runner._set_pressure_controller_vent(True, reason="after metrology diagnostic gas")
                        _record_event(
                            actuation_events,
                            run_id=run_id,
                            process_variant=variant.name,
                            layer=layer,
                            repeat_index=repeat_index,
                            gas_ppm=int(gas_ppm),
                            pressure_target_hpa=None,
                            event_type="restore_vent_on",
                            event_value="VENT_ON",
                            note="after gas diagnostic stage",
                        )
                        restore_rows = _capture_phase_rows(
                            analyzer,
                            devices,
                            context=_phase_context(
                                process_variant=variant.name,
                                layer=layer,
                                repeat_index=repeat_index,
                                phase="restore_vent_on",
                                gas_ppm=int(gas_ppm),
                                route=route,
                            ),
                            gas_start_mono=gas_start_mono,
                            duration_s=float(args.restore_vent_observe_s),
                            sample_poll_s=float(args.sample_poll_s),
                            print_every_s=max(1.0, float(args.print_every_s)),
                            controller_vent_state="VENT_ON",
                        )
                        raw_rows.extend(restore_rows)
                        previous_route = route

                    if stop_requested:
                        break

                if stop_requested:
                    break

                current_status = _variant_layer_status(
                    variant=variant.name,
                    flush_summaries=flush_summaries,
                    seal_hold_summaries=seal_hold_summaries,
                    pressure_summaries=pressure_summaries,
                    phase_gate_rows=phase_gate_rows,
                    args=args,
                )
                if stop_requested:
                    break
                _log(f"Layer {layer} for variant {variant.name} current_status={current_status}")
                if _should_stop_after_layer(
                    current_status,
                    early_stop=bool(args.early_stop),
                    treat_insufficient_as_stop=bool(args.treat_insufficient_as_stop),
                ):
                    skipped_layers = [one for one in layers if one > layer]
                    if skipped_layers:
                        variant_execution_meta[variant.name]["skipped_due_to_previous_layer_failure"] = True
                        variant_execution_meta[variant.name]["skipped_layers"] = sorted(
                            set(variant_execution_meta[variant.name]["skipped_layers"]) | set(skipped_layers)
                        )
                        for skipped_layer in skipped_layers:
                            phase_gate_rows.append(
                                build_phase_gate_row(
                                    process_variant=variant.name,
                                    layer=skipped_layer,
                                    repeat_index=0,
                                    gas_ppm=None,
                                    pressure_target_hpa=None,
                                    phase="layer_gate",
                                    gate_name=f"layer_{skipped_layer}_entry",
                                    gate_status="fail" if current_status == "fail" else "insufficient_evidence",
                                    gate_pass=False,
                                    gate_window_s=None,
                                    gate_value=current_status,
                                    gate_threshold="previous layers must pass",
                                    gate_fail_reason="skipped_due_to_previous_layer_failure",
                                    note=f"stopped after layer {layer}",
                                )
                            )
                        _record_event(
                            actuation_events,
                            run_id=run_id,
                            process_variant=variant.name,
                            layer=layer,
                            repeat_index=0,
                            gas_ppm=None,
                            pressure_target_hpa=None,
                            event_type="layer_skip",
                            event_value=current_status,
                            note=f"skipped_due_to_previous_layer_failure; skipped_layers={skipped_layers}",
                        )
                    break

            if stop_requested:
                break

        aligned_rows = build_aligned_rows(raw_rows, interval_s=float(args.sample_poll_s))
        if bool(args.precondition_only):
            pre_status = (
                "pass"
                if precondition_summaries and all(str(item.get("precondition_status")) == "pass" for item in precondition_summaries)
                else "fail"
            )
            diagnostic_summary = {
                "classification": pre_status,
                "recommended_variant": None,
                "recommendation_confidence": "low",
                "recommendation_reason": "precondition-only run",
                "recommendation_basis": ["precondition_gate"],
                "eligible_for_layer4": False,
                "eligible_variants_for_layer4": [],
                "dominant_error_source": (
                    "source gas moisture suspicion"
                    if any(bool(item.get("dewpoint_rebound_detected")) for item in precondition_summaries)
                    else "insufficient_stability"
                ),
                "evidence_density_ok": True,
                "evidence_density_reason": "",
                "variant_summaries": [
                    {
                        "process_variant": variants[0].name if variants else "B",
                        "classification": pre_status,
                        "layer_statuses": {0: pre_status},
                        "can_enter_layer4": False,
                        "dominant_error_source": (
                            "source gas moisture suspicion"
                            if any(bool(item.get("dewpoint_rebound_detected")) for item in precondition_summaries)
                            else "insufficient_stability"
                        ),
                        "next_best_change": "flush gate",
                        "evidence_density_ok": True,
                        "evidence_density_reason": "",
                        "metrics": [],
                        "summary_messages": [
                            "仅执行 Layer 0 预调理，不进入正式 Layer 1~4 评分。"
                        ],
                        "return_to_zero": [],
                        "skipped_layers": [1, 2, 3, 4],
                        "skipped_due_to_previous_layer_failure": False,
                    }
                ],
                "flush_summaries": [],
                "seal_hold_summaries": [],
                "pressure_summaries": [],
                "phase_gate_summary": phase_gate_rows,
                "thresholds": {},
                "min_flush_s": float(args.min_flush_s),
                "target_flush_s": float(args.screening_flush_s_default),
                "expected_deadtime_s": float(args.gas_switch_deadtime_s),
                "missing_evidence": ["未运行正式 Layer 1~4。"],
                "final_report_answers": {
                    "best_variant": None,
                    "recommendation_confidence": "low",
                    "recommendation_reason": "precondition-only run",
                    "recommendation_basis": ["precondition_gate"],
                    "dominant_error_source": (
                        "source gas moisture suspicion"
                        if any(bool(item.get("dewpoint_rebound_detected")) for item in precondition_summaries)
                        else "insufficient_stability"
                    ),
                    "eligible_for_layer4": False,
                    "eligible_variants_for_layer4": [],
                    "next_best_change": "flush gate",
                    "missing_evidence": ["未运行正式 Layer 1~4。"],
                },
            }
        else:
            diagnostic_summary = analyze_room_temp_diagnostic(
                flush_summaries,
                seal_hold_summaries,
                pressure_summaries,
                phase_gate_rows=phase_gate_rows,
                min_flush_s=float(args.min_flush_s),
                target_flush_s=float(args.screening_flush_s_default),
                expected_deadtime_s=float(args.gas_switch_deadtime_s),
            )
            diagnostic_summary = dict(diagnostic_summary)
        if precondition_summaries and any(str(item.get("precondition_status")) != "pass" for item in precondition_summaries):
            diagnostic_summary["classification"] = "fail"
            diagnostic_summary["dominant_error_source"] = (
                "source gas moisture suspicion"
                if any(bool(item.get("dewpoint_rebound_detected")) for item in precondition_summaries)
                else "insufficient_stability"
            )
            if diagnostic_summary.get("variant_summaries"):
                for item in diagnostic_summary.get("variant_summaries", []) or []:
                    item["classification"] = "fail"
                    layer_statuses = dict(item.get("layer_statuses") or {})
                    layer_statuses[0] = "fail"
                    item["layer_statuses"] = layer_statuses
                    messages = list(item.get("summary_messages") or [])
                    messages.append("Layer 0 预调理在 max flush 前仍未过 gate，正式 Layer 1 未获准启动。")
                    item["summary_messages"] = messages
            if diagnostic_summary.get("final_report_answers"):
                diagnostic_summary["final_report_answers"]["dominant_error_source"] = diagnostic_summary["dominant_error_source"]
                diagnostic_summary["final_report_answers"]["eligible_for_layer4"] = False
        for item in diagnostic_summary.get("variant_summaries", []) or []:
            meta = variant_execution_meta.get(str(item.get("process_variant")), {})
            item["skipped_layers"] = meta.get("skipped_layers", item.get("skipped_layers", []))
            item["skipped_due_to_previous_layer_failure"] = bool(
                meta.get("skipped_due_to_previous_layer_failure", item.get("skipped_due_to_previous_layer_failure", False))
            )
        diagnostic_summary.update(
            {
                "created_at": datetime.now().isoformat(timespec="seconds"),
                "analyzer": analyzer_name,
                "variants": [variant.name for variant in variants],
                "layers": layers,
                "repeats": int(args.repeats),
                "stable_window_s": float(args.stable_window_s),
                "sample_poll_s": float(args.sample_poll_s),
                "writes_coefficients": False,
                "writes_senco": False,
                "changes_device_id": False,
                "v2_touched": False,
                "output_dir": str(logger.run_dir),
                "pressure_transition_trace_csv": str(trace_path) if trace_path.exists() else None,
                "early_stop": bool(args.early_stop),
                "treat_insufficient_as_stop": bool(args.treat_insufficient_as_stop),
                "smoke_level": args.smoke_level,
                "precondition_enabled": bool(args.enable_precondition),
                "precondition_only": bool(args.precondition_only),
                "precondition_summaries": precondition_summaries,
                "run_fail_reason": run_fail_reason,
            }
        )
        outputs = export_room_temp_diagnostic_results(
            logger.run_dir,
            raw_rows=raw_rows,
            aligned_rows=aligned_rows,
            actuation_events=actuation_events,
            flush_gate_trace_rows=flush_gate_trace_rows,
            precondition_summaries=precondition_summaries,
            flush_summaries=flush_summaries,
            seal_hold_summaries=seal_hold_summaries,
            pressure_summaries=pressure_summaries,
            phase_gate_rows=phase_gate_rows,
            diagnostic_summary=diagnostic_summary,
            export_csv=not bool(args.no_export_csv),
            export_xlsx=not bool(args.no_export_xlsx),
            export_png=not bool(args.no_export_png),
        )
        _log(
            f"最终结果 | classification={str(diagnostic_summary.get('classification') or '').upper()} "
            f"recommended_variant={diagnostic_summary.get('recommended_variant')} "
            f"eligible_for_layer4={diagnostic_summary.get('eligible_for_layer4')}"
        )
        for key in (
            "raw_timeseries",
            "aligned_timeseries",
            "actuation_events",
            "flush_gate_trace",
            "flush_summary",
            "seal_hold_summary",
            "pressure_point_summary",
            "phase_gate_summary",
            "diagnostic_summary",
            "readable_report",
            "diagnostic_workbook",
        ):
            _log(f"{key} -> {outputs.get(key)}")
        return 0
    except Exception as exc:
        try:
            _record_event(
                actuation_events,
                run_id=str(getattr(logger, "run_id", "") or getattr(getattr(logger, "run_dir", None), "name", "")),
                process_variant="",
                layer=0,
                repeat_index=0,
                gas_ppm=None,
                pressure_target_hpa=None,
                event_type="run_abort",
                event_value="abort",
                note=str(exc),
            )
        except Exception:
            pass
        try:
            run_dir = getattr(logger, "run_dir", None)
            if run_dir is not None:
                _write_actuation_events_csv(Path(run_dir) / "actuation_events.csv", actuation_events)
        except Exception:
            pass
        _log(f"Metrology seal/pressure diagnostic aborted: {exc}")
        return 1
    finally:
        if devices and not args.no_restore_baseline:
            try:
                runner = CalibrationRunner(runtime_cfg or cfg, devices, logger, _log, lambda *_: None)
                runner._set_pressure_controller_vent(True, reason="after metrology diagnostic finish")
                runner._set_co2_route_baseline(reason="after metrology diagnostic finish")
                _log("Managed relay/pressure baseline restored.")
            except Exception as exc:
                _log(f"Baseline restore failed: {exc}")
        _close_devices(devices)
        try:
            logger.close()
        except Exception:
            pass


if __name__ == "__main__":
    sys.exit(main())
