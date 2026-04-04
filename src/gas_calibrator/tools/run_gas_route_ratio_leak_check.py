"""Independent CO2 raw-ratio gas-route leak-check sidecar."""

from __future__ import annotations

import argparse
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from ..config import load_config
from ..devices import GasAnalyzer, RelayController
from ..logging_utils import RunLogger
from ..validation.gas_route_ratio_leak_check import (
    DEFAULT_GAS_PPM_SEQUENCE,
    DEFAULT_POINT_DURATION_S,
    DEFAULT_STABLE_WINDOW_S,
    DEFAULT_TAIL_WINDOW_S,
    analyze_point_summaries,
    export_leak_check_results,
    summarize_point_rows,
)


LogFn = Callable[[str], None]


def _log(message: str) -> None:
    print(message, flush=True)


def _as_int(value: Any) -> Optional[int]:
    try:
        return int(value)
    except Exception:
        return None


def _parse_ppm_list(raw: str | None) -> List[int]:
    if raw in (None, ""):
        return list(DEFAULT_GAS_PPM_SEQUENCE)
    out: List[int] = []
    for part in str(raw).split(","):
        text = part.strip()
        if not text:
            continue
        out.append(int(float(text)))
    if not out:
        raise argparse.ArgumentTypeError("ppm list must not be empty")
    return out


def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Independent CO2 raw-ratio gas-route leak check. Does not write calibration coefficients.",
    )
    parser.add_argument(
        "--config",
        default="configs/default_config.json",
        help="Path to config json (default: configs/default_config.json).",
    )
    parser.add_argument(
        "--allow-live-hardware",
        action="store_true",
        help="Required safety flag before opening real COM/relay devices.",
    )
    parser.add_argument(
        "--analyzer",
        default="ga02",
        help="Analyzer label or device_id to read from (default: ga02).",
    )
    parser.add_argument(
        "--gas-ppm",
        default="0,200,400,600,800,1000",
        help="Comma-separated CO2 gas ladder (default: 0,200,400,600,800,1000).",
    )
    parser.add_argument(
        "--co2-group",
        choices=("auto", "A", "B", "a", "b"),
        default="auto",
        help="Prefer CO2 source group A/B when both groups are configured.",
    )
    parser.add_argument(
        "--point-duration-s",
        type=float,
        default=DEFAULT_POINT_DURATION_S,
        help="Per-point acquisition duration in seconds (default: 120).",
    )
    parser.add_argument(
        "--stable-window-s",
        type=float,
        default=DEFAULT_STABLE_WINDOW_S,
        help="Stable-window duration in seconds (default: 30).",
    )
    parser.add_argument(
        "--tail-window-s",
        type=float,
        default=DEFAULT_TAIL_WINDOW_S,
        help="Tail-window duration for delta calculation in seconds (default: 10).",
    )
    parser.add_argument(
        "--sample-poll-s",
        type=float,
        default=0.1,
        help="Sample polling interval in seconds (default: 0.1).",
    )
    parser.add_argument(
        "--print-every-s",
        type=float,
        default=1.0,
        help="Console progress print interval in seconds (default: 1.0).",
    )
    parser.add_argument(
        "--configure-analyzer-stream",
        action="store_true",
        help="Optionally re-apply mode/stream commands before sampling. Disabled by default to minimize live changes.",
    )
    parser.add_argument(
        "--output-dir",
        default=None,
        help="Optional output root. Default: <repo>/results/gas_route_ratio_leak_check",
    )
    parser.add_argument(
        "--run-id",
        default=None,
        help="Optional output folder name under the leak-check results root.",
    )
    parser.add_argument(
        "--no-restore-baseline",
        action="store_true",
        help="Do not restore managed relay valves to baseline closed state after the diagnostic.",
    )
    parser.add_argument(
        "--source-close-first-delay-s",
        type=float,
        default=0.0,
        help=(
            "Optional point-switch strategy: close the previous source valve first, keep the "
            "non-source route valves open, wait N seconds, then open the next source valve."
        ),
    )
    return parser.parse_args(list(argv) if argv is not None else None)


def _make_logger(cfg: Mapping[str, Any], output_dir: Optional[str], run_id: Optional[str]) -> RunLogger:
    base_dir = Path(str(cfg.get("_base_dir") or Path.cwd()))
    default_root = base_dir / "results" / "gas_route_ratio_leak_check"
    root = Path(output_dir).resolve() if output_dir else default_root.resolve()
    timestamp = run_id or datetime.now().strftime("%Y%m%d_%H%M%S")
    return RunLogger(root, run_id=timestamp, cfg=dict(cfg))


def _close_devices(devices: Mapping[str, Any]) -> None:
    seen: set[int] = set()
    for item in devices.values():
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


def _normalize_group(value: str) -> str:
    text = str(value or "auto").strip().upper()
    if text in {"A", "B"}:
        return text
    return "AUTO"


def _select_analyzer_cfg(cfg: Mapping[str, Any], analyzer: str) -> Tuple[str, Dict[str, Any]]:
    wanted = str(analyzer or "").strip().lower()
    devices_cfg = cfg.get("devices", {}) if isinstance(cfg, Mapping) else {}
    gas_list = devices_cfg.get("gas_analyzers", []) if isinstance(devices_cfg, Mapping) else []
    if isinstance(gas_list, list):
        for index, item in enumerate(gas_list, start=1):
            if not isinstance(item, dict) or not item.get("enabled", True):
                continue
            name = str(item.get("name") or f"ga{index:02d}")
            aliases = {
                name.lower(),
                f"ga{index:02d}",
                str(item.get("device_id") or "").strip().lower(),
                str(index),
                f"{index:02d}",
            }
            if wanted in aliases:
                return name, dict(item)
    single_cfg = devices_cfg.get("gas_analyzer", {}) if isinstance(devices_cfg, Mapping) else {}
    if isinstance(single_cfg, dict) and single_cfg.get("enabled") and wanted in {"gas_analyzer", "primary", "ga01", "1", "01"}:
        return "gas_analyzer", dict(single_cfg)
    raise RuntimeError(f"Analyzer not found or not enabled: {analyzer}")


def _build_devices(cfg: Mapping[str, Any], analyzer_name: str, analyzer_cfg: Mapping[str, Any], io_logger: RunLogger) -> Dict[str, Any]:
    devices_cfg = cfg.get("devices", {}) if isinstance(cfg, Mapping) else {}
    built: Dict[str, Any] = {}
    try:
        built["analyzer"] = GasAnalyzer(
            str(analyzer_cfg["port"]),
            int(analyzer_cfg.get("baud", 115200)),
            device_id=str(analyzer_cfg.get("device_id") or "000"),
            io_logger=io_logger,
        )
        built["analyzer"].open()
        built["analyzer_name"] = analyzer_name

        relay_cfg = devices_cfg.get("relay", {}) if isinstance(devices_cfg, Mapping) else {}
        if isinstance(relay_cfg, dict) and relay_cfg.get("enabled"):
            built["relay"] = RelayController(
                str(relay_cfg["port"]),
                int(relay_cfg.get("baud", 38400)),
                addr=int(relay_cfg.get("addr", 1)),
                io_logger=io_logger,
            )
            built["relay"].open()

        relay8_cfg = devices_cfg.get("relay_8", {}) if isinstance(devices_cfg, Mapping) else {}
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


def _managed_logical_valves(cfg: Mapping[str, Any]) -> List[int]:
    valves_cfg = cfg.get("valves", {}) if isinstance(cfg, Mapping) else {}
    managed: set[int] = set()
    for key in ("co2_path", "co2_path_group2", "gas_main", "h2o_path", "hold", "flow_switch"):
        value = _as_int(valves_cfg.get(key))
        if value is not None:
            managed.add(value)
    for key in ("co2_map", "co2_map_group2"):
        one_map = valves_cfg.get(key, {})
        if isinstance(one_map, dict):
            for value in one_map.values():
                numeric = _as_int(value)
                if numeric is not None:
                    managed.add(numeric)
    return sorted(managed)


def _resolve_valve_target(cfg: Mapping[str, Any], logical_valve: int) -> Tuple[str, int]:
    valves_cfg = cfg.get("valves", {}) if isinstance(cfg, Mapping) else {}
    relay_map = valves_cfg.get("relay_map", {}) if isinstance(valves_cfg, Mapping) else {}
    entry = relay_map.get(str(logical_valve)) if isinstance(relay_map, dict) else None

    relay_name = "relay"
    channel = logical_valve
    if isinstance(entry, dict):
        relay_name = str(entry.get("device") or "relay")
        mapped = _as_int(entry.get("channel"))
        if mapped is not None:
            channel = mapped
    return relay_name, channel


def _apply_logical_valves(cfg: Mapping[str, Any], devices: Mapping[str, Any], open_logical_valves: Sequence[int]) -> None:
    open_set = {int(value) for value in open_logical_valves}
    grouped: Dict[str, List[Tuple[int, bool]]] = {}
    for logical_valve in _managed_logical_valves(cfg):
        relay_name, channel = _resolve_valve_target(cfg, logical_valve)
        grouped.setdefault(relay_name, []).append((channel, logical_valve in open_set))

    for relay_name, updates in grouped.items():
        relay = devices.get(relay_name)
        if relay is None:
            raise RuntimeError(f"Relay '{relay_name}' is required but unavailable")
        bulk = getattr(relay, "set_valves_bulk", None)
        if callable(bulk):
            bulk(updates)
            continue
        for channel, state in updates:
            relay.set_valve(channel, state)


def _co2_group_candidates(cfg: Mapping[str, Any], preferred_group: str) -> List[Tuple[str, Dict[str, Any], Optional[int]]]:
    valves_cfg = cfg.get("valves", {}) if isinstance(cfg, Mapping) else {}
    map_a = valves_cfg.get("co2_map", {}) if isinstance(valves_cfg.get("co2_map", {}), dict) else {}
    map_b = valves_cfg.get("co2_map_group2", {}) if isinstance(valves_cfg.get("co2_map_group2", {}), dict) else {}
    path_a = _as_int(valves_cfg.get("co2_path"))
    path_b = _as_int(valves_cfg.get("co2_path_group2", valves_cfg.get("co2_path")))
    normalized = _normalize_group(preferred_group)
    if normalized == "A":
        return [("A", map_a, path_a)]
    if normalized == "B":
        return [("B", map_b, path_b)]
    return [("A", map_a, path_a), ("B", map_b, path_b)]


def _route_for_gas_ppm(cfg: Mapping[str, Any], gas_ppm: int, preferred_group: str) -> Dict[str, Any]:
    valves_cfg = cfg.get("valves", {}) if isinstance(cfg, Mapping) else {}
    ppm_key = str(int(gas_ppm))
    for group_name, one_map, path_valve in _co2_group_candidates(cfg, preferred_group):
        if ppm_key in one_map:
            source_valve = _as_int(one_map.get(ppm_key))
            if source_valve is None:
                raise RuntimeError(f"Invalid source valve mapping for {gas_ppm} ppm in group {group_name}")
            return {
                "group": group_name,
                "path_valve": path_valve,
                "source_valve": source_valve,
                "open_logical_valves": [
                    value
                    for value in (
                        _as_int(valves_cfg.get("h2o_path")),
                        _as_int(valves_cfg.get("gas_main")),
                        path_valve,
                        source_valve,
                    )
                    if value is not None
                ],
            }
    raise RuntimeError(f"CO2 valve mapping not found for {gas_ppm} ppm")


def _non_source_open_valves(route: Mapping[str, Any]) -> List[int]:
    source_valve = _as_int(route.get("source_valve"))
    ordered: List[int] = []
    seen: set[int] = set()
    for value in route.get("open_logical_valves", []) or []:
        numeric = _as_int(value)
        if numeric is None or numeric == source_valve or numeric in seen:
            continue
        seen.add(numeric)
        ordered.append(numeric)
    return ordered


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


def _switch_route(
    cfg: Mapping[str, Any],
    devices: Mapping[str, Any],
    *,
    previous_route: Optional[Mapping[str, Any]],
    next_route: Mapping[str, Any],
    source_close_first_delay_s: float,
    emit: LogFn = _log,
) -> None:
    delay_s = max(0.0, float(source_close_first_delay_s))
    if previous_route and delay_s > 0.0:
        intermediate_open = _intermediate_open_valves(previous_route, next_route)
        _apply_logical_valves(cfg, devices, intermediate_open)
        emit(
            "点间切换：已先关闭上一点源阀，保持总阀/总气阀通路，"
            f"等待 {delay_s:.1f}s 后打开下一路源阀"
        )
        time.sleep(delay_s)
    _apply_logical_valves(cfg, devices, next_route["open_logical_valves"])


def _configure_analyzer_stream(dev: GasAnalyzer, analyzer_cfg: Mapping[str, Any]) -> None:
    command_gap_s = 0.15
    dev.set_comm_way_with_ack(False, require_ack=False)
    time.sleep(command_gap_s)
    dev.set_mode_with_ack(int(analyzer_cfg.get("mode", 2)), require_ack=False)
    time.sleep(command_gap_s)
    average_filter = _as_int(analyzer_cfg.get("average_filter"))
    if average_filter is not None:
        dev.set_average_filter_with_ack(average_filter, require_ack=False)
        time.sleep(command_gap_s)
    else:
        co2_n = _as_int(analyzer_cfg.get("average_co2"))
        h2o_n = _as_int(analyzer_cfg.get("average_h2o"))
        if co2_n is not None and h2o_n is not None:
            dev.set_average_with_ack(co2_n=co2_n, h2o_n=h2o_n, require_ack=False)
            time.sleep(command_gap_s)
    active_send = bool(analyzer_cfg.get("active_send", False))
    if active_send:
        ftd_hz = _as_int(analyzer_cfg.get("ftd_hz")) or 10
        dev.set_active_freq_with_ack(ftd_hz, require_ack=False)
        time.sleep(command_gap_s)
        dev.set_comm_way_with_ack(True, require_ack=False)
        time.sleep(1.0)
    dev.active_send = active_send


def _capture_point_rows(
    analyzer: GasAnalyzer,
    *,
    analyzer_name: str,
    gas_ppm: int,
    route_group: str,
    source_valve: int,
    path_valve: Optional[int],
    duration_s: float,
    sample_poll_s: float,
    print_every_s: float,
) -> List[Dict[str, Any]]:
    prefer_stream = bool(analyzer.active_send)
    rows: List[Dict[str, Any]] = []
    point_start = time.monotonic()
    last_print_at = -1.0
    sample_index = 0

    while True:
        now = time.monotonic()
        elapsed_s = now - point_start
        if elapsed_s > duration_s and rows:
            break

        frame_start = time.monotonic()
        raw = analyzer.read_latest_data(
            prefer_stream=prefer_stream,
            drain_s=max(0.05, min(0.25, sample_poll_s * 1.5)),
            read_timeout_s=0.05,
            allow_passive_fallback=True,
        )
        parsed = analyzer.parse_line(raw) if raw else None
        ratio_raw = parsed.get("co2_ratio_raw") if isinstance(parsed, dict) else None
        timestamp = datetime.now().isoformat(timespec="milliseconds")
        row = {
            "timestamp": timestamp,
            "elapsed_s": round(elapsed_s, 6),
            "sample_index": sample_index,
            "gas_ppm": int(gas_ppm),
            "route_group": route_group,
            "source_valve": source_valve,
            "path_valve": path_valve,
            "analyzer": analyzer_name,
            "frame_parse_ok": bool(parsed),
            "mode": parsed.get("mode") if isinstance(parsed, dict) else None,
            "status": parsed.get("status") if isinstance(parsed, dict) else None,
            "co2_ppm_live": parsed.get("co2_ppm") if isinstance(parsed, dict) else None,
            "co2_ratio_f": parsed.get("co2_ratio_f") if isinstance(parsed, dict) else None,
            "co2_ratio_raw": ratio_raw,
            "pressure_kpa": parsed.get("pressure_kpa") if isinstance(parsed, dict) else None,
            "raw": raw,
        }
        rows.append(row)
        sample_index += 1

        if elapsed_s - last_print_at >= max(0.1, float(print_every_s)):
            ratio_text = "--" if ratio_raw is None else f"{float(ratio_raw):.6f}"
            _log(f"[{gas_ppm:>4} ppm] 已采样 {elapsed_s:6.1f}s | {analyzer_name} ratio={ratio_text}")
            last_print_at = elapsed_s

        remaining_s = max(0.0, float(sample_poll_s) - (time.monotonic() - frame_start))
        if remaining_s > 0.0:
            time.sleep(remaining_s)

    return rows


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = parse_args(argv)
    if not args.allow_live_hardware:
        _log("Safety gate: pass --allow-live-hardware to run this independent live diagnostic.")
        return 2

    gas_ppm_list = _parse_ppm_list(args.gas_ppm)
    cfg = load_config(args.config)
    logger = _make_logger(cfg, args.output_dir, args.run_id)
    devices: Dict[str, Any] = {}
    raw_rows: List[Dict[str, Any]] = []
    point_summaries: List[Dict[str, Any]] = []
    try:
        analyzer_name, analyzer_cfg = _select_analyzer_cfg(cfg, args.analyzer)
        _log(f"Leak check output dir: {logger.run_dir}")
        _log(f"Selected analyzer: {analyzer_name}")
        _log(f"Gas ladder: {gas_ppm_list}")
        logger.log_io(
            port="LOG",
            device="gas_route_ratio_leak_check",
            direction="EVENT",
            command="diagnostic-start",
            response=(
                "diagnostic_only=true writes_coefficients=false writes_senco=false "
                "changes_device_id=false pressure_control=false"
            ),
        )

        devices = _build_devices(cfg, analyzer_name, analyzer_cfg, logger)
        analyzer = devices["analyzer"]
        analyzer.active_send = bool(analyzer_cfg.get("active_send", False))
        if args.configure_analyzer_stream:
            _log("Applying optional analyzer stream configuration...")
            _configure_analyzer_stream(analyzer, analyzer_cfg)
        elif analyzer.active_send:
            _log("Using configured active stream mode for sampling.")
        else:
            _log("Configured active stream is off; using passive READDATA polling.")

        previous_route: Optional[Dict[str, Any]] = None
        for gas_ppm in gas_ppm_list:
            route = _route_for_gas_ppm(cfg, gas_ppm, args.co2_group)
            _log(
                f"切换气点 {gas_ppm} ppm | group={route['group']} source_valve={route['source_valve']} path_valve={route['path_valve']}"
            )
            _switch_route(
                cfg,
                devices,
                previous_route=previous_route,
                next_route=route,
                source_close_first_delay_s=float(args.source_close_first_delay_s),
            )

            point_rows = _capture_point_rows(
                analyzer,
                analyzer_name=analyzer_name,
                gas_ppm=gas_ppm,
                route_group=route["group"],
                source_valve=int(route["source_valve"]),
                path_valve=_as_int(route["path_valve"]),
                duration_s=float(args.point_duration_s),
                sample_poll_s=float(args.sample_poll_s),
                print_every_s=float(args.print_every_s),
            )
            raw_rows.extend(point_rows)

            point_summary = summarize_point_rows(
                point_rows,
                gas_ppm=gas_ppm,
                stable_window_s=float(args.stable_window_s),
                tail_window_s=float(args.tail_window_s),
            )
            point_summary["analyzer"] = analyzer_name
            point_summary["route_group"] = route["group"]
            point_summary["source_valve"] = route["source_valve"]
            point_summary["path_valve"] = route["path_valve"]
            point_summaries.append(point_summary)

            _log(
                f"[{gas_ppm:>4} ppm] point done | "
                f"stable_mean={point_summary.get('stable_mean_ratio')} "
                f"stable_std={point_summary.get('stable_std_ratio')} "
                f"tail_delta={point_summary.get('tail_delta_ratio')}"
            )
            previous_route = route

        fit_summary = analyze_point_summaries(point_summaries)
        fit_summary = dict(fit_summary)
        fit_summary.update(
            {
                "created_at": datetime.now().isoformat(timespec="seconds"),
                "analyzer": analyzer_name,
                "gas_ppm_sequence": gas_ppm_list,
                "point_duration_s": float(args.point_duration_s),
                "stable_window_s": float(args.stable_window_s),
                "tail_window_s": float(args.tail_window_s),
                "writes_coefficients": False,
                "writes_senco": False,
                "changes_device_id": False,
                "pressure_control_used": False,
                "output_dir": str(logger.run_dir),
            }
        )

        outputs = export_leak_check_results(
            logger.run_dir,
            raw_rows=raw_rows,
            point_summaries=point_summaries,
            fit_summary=fit_summary,
        )

        _log(
            "最终结果 | "
            f"monotonic={fit_summary.get('monotonic_ok')} "
            f"r2={fit_summary.get('linear_r2')} "
            f"max_residual={fit_summary.get('max_abs_normalized_residual')} "
            f"span_compression={fit_summary.get('span_compression_ratio')} "
            f"classification={str(fit_summary.get('classification') or '').upper()}"
        )
        _log(f"raw_timeseries.csv -> {outputs['raw_timeseries']}")
        _log(f"point_summary.csv -> {outputs['point_summary']}")
        _log(f"fit_summary.json -> {outputs['fit_summary']}")
        _log(f"readable_report.txt -> {outputs['readable_report']}")
        _log(f"ratio_overview.png -> {outputs['ratio_overview_plot']}")
        _log(f"stable_mean_fit.png -> {outputs['stable_mean_fit_plot']}")
        _log(f"transition_windows.png -> {outputs['transition_windows_plot']}")
        detail_plots = outputs.get("transition_detail_plots", {})
        if isinstance(detail_plots, Mapping):
            for _, path in detail_plots.items():
                _log(f"transition_detail.png -> {path}")
        return 0
    except Exception as exc:
        _log(f"Gas-route ratio leak check aborted: {exc}")
        return 1
    finally:
        if devices and not args.no_restore_baseline:
            try:
                _apply_logical_valves(cfg, devices, [])
                _log("Managed relay valves restored to baseline.")
            except Exception as exc:
                _log(f"Relay baseline restore failed: {exc}")
        _close_devices(devices)
        try:
            logger.close()
        except Exception:
            pass


if __name__ == "__main__":
    sys.exit(main())
