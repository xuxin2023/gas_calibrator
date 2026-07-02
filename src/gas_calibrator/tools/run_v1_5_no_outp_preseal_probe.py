"""Minimal V1.5 no‑OUTP preseal physical probe.

Validates whether omitting :OUTP 0 (/OUTP1) during preseal vent‑off allows
the CO2 open route to pressurize, answering whether OUTP cycling was the root
cause of the "atmosphere still visible after VENT0" symptom.

THIS IS A RESEARCH PROBE — NOT A CALIBRATION TOOL.
DO NOT use on production hardware without explicit operator authorization.
DO NOT refresh real_primary_latest from this probe.
DO NOT merge this into the V1.5 production branch without full review.

Usage (offline only — no real hardware unless explicitly authorized):
    python -m gas_calibrator.tools.run_v1_5_no_outp_preseal_probe \
        --config site_v1_5_no_write_current_hardware_co2_20c_1000ppm_full_pressure_no_tempwait.json \
        --co2-ppm 1000 --temp 20 --observe-s 10 --min-rise-hpa 5 --no-write
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from ..config import load_config
from ..devices import (
    DewpointMeter,
    Pace5000,
    ParoscientificGauge,
    RelayController,
)
from .v1_5_entrypoint_guards import (
    add_engineering_diagnostic_guard_args,
    require_engineering_diagnostic_guard,
)


def _log(msg: str) -> None:
    print(msg, flush=True)


def _now_ts() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def _write_json(path: Path, obj: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, ensure_ascii=False, default=str)


def _csv_row(path: Path, header: List[str], row: List[Any]) -> None:
    write_header = not path.exists()
    with open(path, "a", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        if write_header:
            w.writerow(header)
        w.writerow(row)


def _as_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(value)
    except (ValueError, TypeError):
        return None


class ProbeIoLogger:
    """Minimal CSV IO logger for probe-level device IO capture.

    Matches the V1 RunLogger.log_io signature so devices can write through it."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._handle = self.path.open("w", newline="", encoding="utf-8")
        self._writer = csv.DictWriter(
            self._handle,
            fieldnames=["timestamp", "port", "device", "direction", "command", "response", "error"],
        )
        self._writer.writeheader()
        self._handle.flush()

    def log_io(
        self,
        *args: Any,
        port: str = "",
        device: str = "",
        direction: str = "",
        command: Any = None,
        response: Any = None,
        error: Any = None,
    ) -> None:
        if args:
            if len(args) >= 1:
                device = args[0]
            if len(args) >= 2:
                direction = args[1]
            if len(args) >= 3:
                command = args[2]
            if len(args) >= 4:
                response = args[3]
            if len(args) >= 5:
                error = args[4]
        self._writer.writerow({
            "timestamp": datetime.now().isoformat(timespec="milliseconds"),
            "port": str(port or ""),
            "device": str(device or ""),
            "direction": str(direction or ""),
            "command": "" if command is None else str(command),
            "response": "" if response is None else str(response),
            "error": "" if error is None else str(error),
        })
        self._handle.flush()

    def close(self) -> None:
        try:
            self._handle.close()
        except Exception:
            pass


def _managed_logical_valves(cfg: Mapping[str, Any]) -> List[int]:
    valves_cfg = cfg.get("valves", {}) if isinstance(cfg.get("valves"), dict) else {}
    managed: set = set()
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
    valves_cfg = cfg.get("valves", {}) if isinstance(cfg.get("valves"), dict) else {}
    relay_map = valves_cfg.get("relay_map", {}) if isinstance(valves_cfg, dict) else {}
    entry = relay_map.get(str(logical_valve)) if isinstance(relay_map, dict) else None
    relay_name = "relay"
    channel = logical_valve
    if isinstance(entry, dict):
        relay_name = str(entry.get("device") or "relay")
        mapped = _as_int(entry.get("channel"))
        if mapped is not None:
            channel = mapped
    return relay_name, channel


def _apply_logical_valves(
    cfg: Mapping[str, Any],
    devices: Mapping[str, Any],
    open_logical_valves: Sequence[int],
) -> None:
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


def _close_all_relay_valves(cfg: Mapping[str, Any], devices: Mapping[str, Any]) -> None:
    for logical_valve in _managed_logical_valves(cfg):
        relay_name, channel = _resolve_valve_target(cfg, logical_valve)
        relay = devices.get(relay_name)
        if relay is not None:
            try:
                relay.set_valve(channel, False)
            except Exception:
                pass


class NoOutpProbe:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.config_path = args.config

        raw = load_config(args.config)
        self._overlay_effective(raw)
        self.cfg = raw
        self.devices: Dict[str, Any] = {}
        self.io_log_path: Optional[Path] = None
        self.trace_path: Optional[Path] = None
        self.summary_path: Optional[Path] = None

        self._started_at = time.time()
        self._outp0_count: int = 0
        self._outp1_count: int = 0
        self._vent1_count: int = 0
        self._vent0_count: int = 0
        self._observations: List[Dict[str, Any]] = []
        self.io_logger: Optional[ProbeIoLogger] = None

        self.com22_baseline: Optional[float] = None
        self.com22_max: Optional[float] = None
        self.pace_baseline: Optional[float] = None
        self.pace_max: Optional[float] = None
        self.dewpoint_baseline: Optional[float] = None
        self.dewpoint_max_delta: Optional[float] = None
        self.valid_dewpoint_data: bool = False
        self.dewpoint_read_error: Optional[str] = None
        self.dewpoint_samples_count: int = 0

    # ── config ────────────────────────────────────────────────────

    @staticmethod
    def _deep_set(d: Dict[str, Any], dotted: str, value: Any) -> None:
        parts = dotted.split(".")
        for p in parts[:-1]:
            d = d.setdefault(p, {})
        d[parts[-1]] = value

    @staticmethod
    def _deep_get(d: Dict[str, Any], dotted: str, default: Any = None) -> Any:
        parts = dotted.split(".")
        for p in parts:
            if isinstance(d, dict) and p in d:
                d = d[p]
            else:
                return default
        return d

    def _overlay_effective(self, raw: Dict[str, Any]) -> None:
        self._deep_set(raw, "workflow.pressure.no_outp_transition_mode", True)
        self._deep_set(raw, "workflow.pressure.no_outp_pressure_rise_timeout_s",
                       float(self.args.observe_s) if self.args.observe_s else 10.0)
        self._deep_set(raw, "workflow.pressure.no_outp_pressure_rise_min_hpa",
                       float(self.args.min_rise_hpa) if self.args.min_rise_hpa else 5.0)

    # ── no-write preflight ─────────────────────────────────────────

    _WRITE_RISK_KEYS: List[str] = [
        "coefficients.enabled",
        "coefficients.sencos",
        "postrun_corrected_delivery.enabled",
        "postrun_corrected_delivery.write_devices",
        "postrun_corrected_delivery.write_pressure_coefficients",
        "workflow.postrun_corrected_delivery.enabled",
        "workflow.postrun_corrected_delivery.write_devices",
        "workflow.postrun_corrected_delivery.write_pressure_coefficients",
        "startup_pressure_sensor_calibration.enabled",
        "startup_pressure_sensor_calibration.apply_write",
        "workflow.startup_pressure_sensor_calibration.enabled",
        "workflow.startup_pressure_sensor_calibration.apply_write",
        "postrun.write_devices",
        "postrun.write_pressure_coefficients",
        "workflow.postrun.write_devices",
        "workflow.postrun.write_pressure_coefficients",
        "spc.enabled",
        "spc.apply_write",
        "workflow.spc.enabled",
        "workflow.spc.apply_write",
        "workflow.startup_pressure_precheck.enabled",
    ]

    def _no_write_preflight(self) -> Optional[str]:
        failures: List[str] = []
        for key in self._WRITE_RISK_KEYS:
            val = self._deep_get(self.cfg, key)
            is_bad = (isinstance(val, bool) and val is True)
            if is_bad:
                failures.append(f"{key}={val}")
        for key, label in (
            ("coefficients.sencos", "coefficients.sencos_non_empty"),
            ("workflow.coefficients.sencos", "workflow.coefficients.sencos_non_empty"),
        ):
            val = self._deep_get(self.cfg, key)
            if isinstance(val, (dict, list, tuple, set)) and bool(val):
                failures.append(label)
        if failures:
            return f"NO_WRITE_PREFLIGHT_FAIL: {', '.join(failures)}"
        return None

    # ── devices ────────────────────────────────────────────────────

    def _build_devices(self) -> None:
        dcfg = self.cfg.get("devices", {})
        log_dir = Path(self.cfg.get("paths", {}).get("output_dir", "logs"))
        run_tag = f"no_outp_preseal_probe/run_{_now_ts()}"
        out_dir = log_dir / run_tag
        out_dir.mkdir(parents=True, exist_ok=True)

        self.output_dir = out_dir
        self.io_log_path = out_dir / "io_log.csv"
        self.trace_path = out_dir / "probe_trace.csv"
        self.summary_path = out_dir / "probe_summary.json"

        self.io_logger = ProbeIoLogger(self.io_log_path)

        pc = dcfg.get("pressure_controller", {})
        if pc.get("enabled"):
            self.devices["pace"] = Pace5000(
                pc["port"], pc.get("baud", 115200),
                timeout=float(pc.get("timeout", 1.0)),
                line_ending=pc.get("line_ending"),
                query_line_endings=pc.get("query_line_endings"),
                pressure_queries=pc.get("pressure_queries"),
                io_logger=self.io_logger,
            )
            self.devices["pace"].open()
            _log("PACE opened")

        pg = dcfg.get("pressure_gauge", {})
        if pg.get("enabled"):
            self.devices["pressure_gauge"] = ParoscientificGauge(
                pg["port"], pg.get("baud", 115200),
                timeout=float(pg.get("timeout", 1.0)),
                dest_id=pg.get("dest_id", 1),
                response_timeout_s=pg.get("response_timeout_s"),
            )
            self.devices["pressure_gauge"].open()
            _log("COM22 pressure gauge opened")

        dm = dcfg.get("dewpoint_meter", {})
        if dm.get("enabled"):
            self.devices["dewpoint"] = DewpointMeter(
                dm["port"], dm.get("baud", 115200),
                station=dm.get("station", 1),
                io_logger=self.io_logger,
            )
            self.devices["dewpoint"].open()
            _log("Dewpoint meter opened")

        relay_cfg = dcfg.get("relay", {}) if isinstance(dcfg, dict) else {}
        if relay_cfg.get("enabled"):
            self.devices["relay"] = RelayController(
                relay_cfg["port"],
                relay_cfg.get("baud", 38400),
                addr=relay_cfg.get("addr", 1),
                io_logger=self.io_logger,
            )
            self.devices["relay"].open()
            _log("Relay opened")

        relay8_cfg = dcfg.get("relay_8", {}) if isinstance(dcfg, dict) else {}
        if relay8_cfg.get("enabled"):
            self.devices["relay_8"] = RelayController(
                relay8_cfg["port"],
                relay8_cfg.get("baud", 38400),
                addr=relay8_cfg.get("addr", 1),
                io_logger=self.io_logger,
            )
            self.devices["relay_8"].open()
            _log("Relay_8 opened")

        if not self.devices:
            raise RuntimeError("No devices enabled — cannot probe")

    def _valve_configs(self) -> Dict[str, Optional[int]]:
        v = self.cfg.get("valves", {}) if isinstance(self.cfg.get("valves"), dict) else {}
        volts: Dict[str, Optional[int]] = {}
        for key in ("h2o_path", "gas_main", "co2_path", "co2_path_group2"):
            val = v.get(key)
            volts[key] = int(val) if val is not None else None
        return volts

    def _source_valve_for_ppm(self, ppm: float) -> Tuple[Optional[int], Optional[str]]:
        v = self.cfg.get("valves", {}) if isinstance(self.cfg.get("valves"), dict) else {}
        map_a = v.get("co2_map", {}) if isinstance(v.get("co2_map"), dict) else {}
        map_b = v.get("co2_map_group2", {}) if isinstance(v.get("co2_map_group2"), dict) else {}
        key = str(int(ppm))
        if key in map_a:
            return int(map_a[key]), "A"
        if key in map_b:
            return int(map_b[key]), "B"
        return None, None

    def _co2_open_valves(self) -> List[int]:
        vc = self._valve_configs()
        source, group = self._source_valve_for_ppm(float(self.args.co2_ppm))
        self._co2_group = group
        if group == "B":
            raise RuntimeError("BLOCKED_GROUP2_UNSUPPORTED")
        open_list: List[int] = []
        for v in (vc.get("h2o_path"), vc.get("gas_main"), vc.get("co2_path"), source):
            if v is not None:
                open_list.append(int(v))
        return open_list

    def _close_valves(self) -> None:
        _log("Closing CO2 route valves")
        _close_all_relay_valves(self.cfg, self.devices)

    def _log_probe_io(self, device: str, direction: str, payload: str) -> None:
        try:
            self.io_logger.log_io(
                port="PROBE",
                device=device,
                direction=direction,
                command=payload,
            )
        except Exception:
            pass

    # ── probe phases ───────────────────────────────────────────────

    def _read_pace_pressure(self) -> Optional[float]:
        try:
            val = float(self.devices["pace"].read_pressure())
            return val if val == val else None
        except Exception:
            return None

    def _read_gauge_pressure(self) -> Optional[float]:
        try:
            val = float(self.devices["pressure_gauge"].read_pressure())
            return val if val == val else None
        except Exception:
            return None

    @staticmethod
    def _extract_dewpoint_value(data: Any) -> Optional[float]:
        if data is None:
            return None
        if isinstance(data, (int, float)):
            val = float(data)
            return val if val == val else None
        if isinstance(data, dict):
            for key in ("dewpoint_c", "dewpoint", "dew_point", "dp", "td", "value"):
                if key not in data:
                    continue
                try:
                    val = float(data.get(key))
                except Exception:
                    continue
                return val if val == val else None
        return None

    def _read_dewpoint(self) -> Optional[float]:
        dew = self.devices.get("dewpoint")
        if dew is None:
            self.dewpoint_read_error = "dewpoint_device_missing"
            return None

        attempts = 4
        last_error: Optional[str] = None
        readers = []
        fast_reader = getattr(dew, "get_current_fast", None)
        if callable(fast_reader):
            def _fast_read(reader=fast_reader):
                try:
                    return reader(timeout_s=0.35)
                except TypeError:
                    return reader()
            readers.append(_fast_read)
        current_reader = getattr(dew, "get_current", None)
        if callable(current_reader):
            def _current_read(reader=current_reader):
                try:
                    return reader(timeout_s=0.5, attempts=1)
                except TypeError:
                    return reader()
            readers.append(_current_read)
        for name in ("read_dewpoint", "read", "status"):
            reader = getattr(dew, name, None)
            if callable(reader):
                readers.append(reader)

        if not readers:
            self.dewpoint_read_error = "dewpoint_reader_missing"
            return None

        for attempt in range(attempts):
            for reader in readers:
                try:
                    value = self._extract_dewpoint_value(reader())
                except Exception as exc:
                    last_error = f"{type(exc).__name__}: {exc}"
                    continue
                if value is not None:
                    self.dewpoint_read_error = None
                    return value
                last_error = "dewpoint_value_missing"
            if attempt < attempts - 1:
                time.sleep(0.05)

        self.dewpoint_read_error = last_error or "dewpoint_value_missing"
        return None

    def _record_obs(self, phase: str) -> None:
        pp = self._read_pace_pressure()
        gp = self._read_gauge_pressure()
        dp = self._read_dewpoint()
        row: Dict[str, Any] = {
            "ts": f"{time.time()-self._started_at:.3f}",
            "phase": phase,
            "pace_hpa": pp,
            "com22_hpa": gp,
            "dewpoint_c": dp,
        }
        self._observations.append(row)

        if gp is not None:
            if self.com22_baseline is None:
                self.com22_baseline = gp
            self.com22_max = max(self.com22_max or gp, gp)
        if pp is not None:
            if self.pace_baseline is None:
                self.pace_baseline = pp
            self.pace_max = max(self.pace_max or pp, pp)
        if dp is not None:
            self.valid_dewpoint_data = True
            self.dewpoint_samples_count += 1
            if self.dewpoint_baseline is None:
                self.dewpoint_baseline = dp
            if self.dewpoint_max_delta is None:
                self.dewpoint_max_delta = 0.0
            delta = abs(dp - (self.dewpoint_baseline or dp))
            self.dewpoint_max_delta = max(self.dewpoint_max_delta or 0.0, delta)

        _csv_row(
            self.trace_path or Path("probe_trace.csv"),
            ["ts_s", "phase", "pace_hpa", "com22_hpa", "dewpoint_c"],
            [row["ts"], phase, pp, gp, dp],
        )

    def _safe_stop_before(self) -> None:
        if not self.args.safe_stop_before:
            return
        _log("--- safe_stop before probe ---")
        try:
            from ..tools.safe_stop import SafeStopTool
            SafeStopTool.run_immediate(self.cfg)
        except Exception as exc:
            _log(f"safe_stop before failed (continuing): {exc}")

    def _safe_stop_after(self) -> None:
        if not self.args.safe_stop_after:
            return
        _log("--- safe_stop after probe ---")
        try:
            from ..tools.safe_stop import SafeStopTool
            SafeStopTool.run_immediate(self.cfg)
        except Exception as exc:
            _log(f"safe_stop after failed: {exc}")

    # ── main probe ─────────────────────────────────────────────────

    def run(self) -> int:
        _log("=== V1.5 no-OUTP preseal physical probe ===")
        _log(f"  branch: codex/v1.5-pace-no-outp-transition-research")
        _log(f"  safe_stop_before={self.args.safe_stop_before}")
        _log(f"  safe_stop_after={self.args.safe_stop_after}")
        _log("  operator must decide safe_stop")

        # 1. no-write preflight
        err = self._no_write_preflight()
        if err:
            _log(f"FATAL: {err}")
            _write_json(Path("logs/no_outp_preseal_probe") / "preflight_fail.json",
                        {"error": err, "exit": "PREFLIGHT_FAIL"})
            return 2

        # 2. startup hold check
        if self._deep_get(self.cfg, "workflow.startup_pressure_precheck.enabled"):
            _log("FATAL: startup_pressure_precheck.enabled=True → BLOCKED")
            return 3

        # 3. build devices
        _log("Initializing devices...")
        self._safe_stop_before()
        self._build_devices()
        _log(f"Output dir: {self.output_dir}")

        # 3a. relay presence check for real mode
        if not self.devices.get("relay") and not self.devices.get("relay_8"):
            _log("BLOCKED: no relay device built — cannot open CO2 route")
            self._final_decision = "BLOCKED_RELAY_MISSING"
            self._write_summary()
            self._close_devices_safe()
            return 4

        try:
            result = self._probe()
            return result
        finally:
            self._safe_stop_after()
            self._close_devices_safe()
            self._write_summary()
            self._write_io_log_evidence()
            _log(f"Probe complete. Results: {self.summary_path}")

    def _probe(self) -> int:
        try:
            open_valves = self._co2_open_valves()
        except RuntimeError as exc:
            if "BLOCKED_GROUP2_UNSUPPORTED" in str(exc):
                _log("BLOCKED: Group B / co2_map_group2 is not supported by this minimal probe")
                self._final_decision = "BLOCKED_GROUP2_UNSUPPORTED"
                return 7
            raise
        _log(f"CO2 route open valves: {open_valves}")

        # ── Open CO2 route ──
        relay = self.devices.get("relay")
        relay_8 = self.devices.get("relay_8")
        if relay is None and relay_8 is None:
            _log("BLOCKED: no relay device — cannot open CO2 route")
            self._final_decision = "BLOCKED_RELAY_MISSING"
            return 4

        try:
            _apply_logical_valves(self.cfg, self.devices, open_valves)
            self._log_probe_io("relay", "PROBE", f"open_valves={open_valves}")
            _log("CO2 route valves opened via RelayController.set_valve/set_valves_bulk")
        except Exception as exc:
            _log(f"BLOCKED: CO2 route open failed: {exc}")
            self._final_decision = f"BLOCKED_ROUTE_OPEN_FAIL: {exc}"
            return 5

        # ── Phase 1: open-flow VENT1 (no OUTP0) ──
        pace = self.devices["pace"]
        _log("Phase 1: enter atmosphere hold (VENT1, no OUTP0)")

        try:
            stop_hold = getattr(pace, "stop_atmosphere_hold", None)
            if callable(stop_hold):
                stop_hold()
        except Exception as exc:
            _log(f"stop_atmosphere_hold: {exc}")

        pace.vent(True)
        self._vent1_count += 1
        self._log_probe_io("pace", "PROBE", "VENT1 open-flow start")
        try:
            start_hold = getattr(pace, "start_atmosphere_hold", None)
            if callable(start_hold):
                start_hold(interval_s=2.0)
        except Exception:
            pass

        _log("VENT1 open-flow: 5s warm-up")
        for _ in range(5):
            time.sleep(1.0)
            self._record_obs("openflow")

        # ── Phase 2: close atmosphere (VENT0, no OUTP0) ──
        _log("Phase 2: close atmosphere (VENT0, no OUTP0)")

        try:
            stop_hold2 = getattr(pace, "stop_atmosphere_hold", None)
            if callable(stop_hold2):
                stop_hold2()
        except Exception as exc:
            _log(f"stop_atmosphere_hold before VENT0: {exc}")

        pace.vent(False)
        self._vent0_count += 1
        self._log_probe_io("pace", "PROBE", "VENT0 close")

        iso = getattr(pace, "set_isolation_open", None)
        if callable(iso):
            iso(True)
            self._log_probe_io("pace", "PROBE", "isolation_open=True")

        _log(f"VENT0 sent. Observing for {self.args.observe_s}s...")

        observe_s = float(self.args.observe_s)
        observe_start = time.time()
        observe_deadline = observe_start + observe_s

        while time.time() < observe_deadline:
            time.sleep(0.25)
            self._record_obs("observe")

        # ── Phase 3: restore atmosphere (cleanup) ──
        _log("Phase 3: restore atmosphere")

        try:
            stop_hold3 = getattr(pace, "stop_atmosphere_hold", None)
            if callable(stop_hold3):
                stop_hold3()
        except Exception:
            pass

        pace.vent(True)
        self._vent1_count += 1
        self._log_probe_io("pace", "PROBE", "VENT1 restore atmosphere")
        try:
            start_hold3 = getattr(pace, "start_atmosphere_hold", None)
            if callable(start_hold3):
                start_hold3(interval_s=2.0)
        except Exception:
            pass

        time.sleep(2.0)
        self._close_valves()
        _log("Atmosphere restored, valves closed")

        # ── Decision ──
        return self._decide()

    def _decide(self) -> int:
        com22_rise = None
        if self.com22_baseline is not None and self.com22_max is not None:
            com22_rise = self.com22_max - self.com22_baseline

        min_rise = float(self.args.min_rise_hpa) if self.args.min_rise_hpa else 5.0

        io_outp0, io_outp1, io_vent0, io_vent1 = self._count_outp_from_io_log()

        failures: List[str] = []
        if io_outp0 is not None and io_outp0 > 0:
            failures.append(f"OUTP0_from_io_log={io_outp0}")
            self._outp0_count = io_outp0
        if io_outp1 is not None and io_outp1 > 0:
            failures.append(f"OUTP1_from_io_log={io_outp1}")
            self._outp1_count = io_outp1
        if com22_rise is None or com22_rise < min_rise:
            failures.append(f"COM22_rise={com22_rise:.2f} < {min_rise}" if com22_rise is not None
                            else "COM22_rise=no_data")

        if io_outp0 is None and io_outp1 is None:
            self._final_decision = "BLOCKED_IO_LOG_MISSING"
            _log("BLOCKED: no io_log.csv available — cannot verify OUTP/VENT")
            return 6

        self._final_decision = "PASS" if not failures else f"FAIL: {'; '.join(failures)}"

        _log("")
        _log(f"OUTP0 (from io_log):       {io_outp0}")
        _log(f"OUTP1 (from io_log):       {io_outp1}")
        _log(f"VENT0 (from io_log):       {io_vent0}")
        _log(f"VENT1 count (openflow):    {self._vent1_count}")
        _log(f"VENT0 count (close):      {self._vent0_count}")
        _log(f"COM22 baseline:           {self.com22_baseline}")
        _log(f"COM22 max:                {self.com22_max}")
        _log(f"COM22 rise:               {com22_rise}")
        _log(f"PACE baseline:            {self.pace_baseline}")
        _log(f"PACE max:                 {self.pace_max}")
        _log(f"Dewpoint baseline:        {self.dewpoint_baseline}")
        _log(f"Dewpoint max delta:       {self.dewpoint_max_delta}")
        _log(f"Dewpoint valid:           {self.valid_dewpoint_data}")
        _log(f"Dewpoint samples:         {self.dewpoint_samples_count}")
        _log(f"Dewpoint read error:      {self.dewpoint_read_error}")
        _log(f"Decision:                 {self._final_decision}")

        if failures:
            return 1
        return 0

    def _count_outp_from_io_log(self) -> Tuple[Optional[int], Optional[int], Optional[int], Optional[int]]:
        path = self.io_log_path
        if not path or not path.exists():
            return None, None, None, None
        outp0 = 0
        outp1 = 0
        vent0 = 0
        vent1 = 0
        try:
            with path.open("r", encoding="utf-8", newline="") as f:
                for row in csv.DictReader(f):
                    cmd = str(row.get("command", "") or "")
                    if ":OUTP 0" in cmd or ":OUTP0" in cmd.replace(" ", ""):
                        outp0 += 1
                    if ":OUTP 1" in cmd or ":OUTP1" in cmd.replace(" ", ""):
                        outp1 += 1
                    if "VENT 0" in cmd or "VENT0" in cmd.replace(" ", ""):
                        vent0 += 1
                    if "VENT 1" in cmd or "VENT1" in cmd.replace(" ", ""):
                        vent1 += 1
        except Exception:
            return None, None, None, None
        return outp0, outp1, vent0, vent1

    def _write_io_log_evidence(self) -> None:
        if not self.io_log_path or not self.io_log_path.exists():
            _log("WARNING: io_log.csv not generated — BLOCKED_IO_LOG_MISSING")
            return
        io_outp0, io_outp1, io_vent0, io_vent1 = self._count_outp_from_io_log()
        _log(f"IO log evidence: OUTP0={io_outp0} OUTP1={io_outp1} VENT0={io_vent0} VENT1={io_vent1}")

    def _write_summary(self) -> None:
        if not self.summary_path:
            return
        com22_rise = None
        if self.com22_baseline is not None and self.com22_max is not None:
            com22_rise = self.com22_max - self.com22_baseline

        io_outp0, io_outp1, io_vent0, io_vent1 = self._count_outp_from_io_log()

        io_log_exists = bool(self.io_log_path and self.io_log_path.exists())

        summary: Dict[str, Any] = {
            "final_decision": getattr(self, "_final_decision", "UNKNOWN"),
            "branch": "codex/v1.5-pace-no-outp-transition-research",
            "config_path": str(self.config_path),
            "no_outp_transition_mode": True,
            "no_write_preflight": "PASS" if self._no_write_preflight() is None else "FAIL",
            "startup_hold_check_disabled": True,
            "co2_group": getattr(self, "_co2_group", "A") or "A",
            "group2_supported": False,
            "safe_stop_before": bool(self.args.safe_stop_before),
            "safe_stop_after": bool(self.args.safe_stop_after),
            "outp0_count_probe_phase": self._outp0_count,
            "outp1_count_probe_phase": self._outp1_count,
            "outp0_from_io_log": io_outp0,
            "outp1_from_io_log": io_outp1,
            "vent0_from_io_log": io_vent0,
            "vent1_from_io_log": io_vent1,
            "vent1_count_openflow": self._vent1_count,
            "vent0_count_close": self._vent0_count,
            "io_log_exists": io_log_exists,
            "outp_counting_source": "io_log.csv" if io_log_exists else "internal_counter_only",
            "com22_pressure_baseline_hpa": self.com22_baseline,
            "com22_pressure_max_hpa": self.com22_max,
            "com22_pressure_rise_hpa": com22_rise,
            "pace_pressure_baseline_hpa": self.pace_baseline,
            "pace_pressure_max_hpa": self.pace_max,
            "dewpoint_baseline": self.dewpoint_baseline,
            "dewpoint_max_delta": self.dewpoint_max_delta,
            "valid_dewpoint_data": self.valid_dewpoint_data,
            "dewpoint_read_error": self.dewpoint_read_error,
            "dewpoint_samples_count": self.dewpoint_samples_count,
            "operator_pace_vent_popup_observed": None,
            "operator_dewpoint_air_ingress_observed": None,
            "cleanup_completed": True,
            "not_real_acceptance_evidence": True,
            "engineering_probe_only": True,
            "promotion_state": "blocked",
        }
        _write_json(self.summary_path, summary)

        if self.trace_path:
            tp = str(self.trace_path)
        else:
            tp = "N/A"
        op = self.output_dir / "operator_observation_template.md"
        op.parent.mkdir(parents=True, exist_ok=True)
        op.write_text(
            f"# No-OUTP Preseal Probe — Operator Observation\n\n"
            f"Probe: {self.output_dir}\n\n"
            f"1. PACE 屏幕通大气小窗口？\n"
            f"   - VENT1 open-flow 期间：___ (expected: yes)\n"
            f"   - VENT0 关闭后：___ (expected: no/消失)\n\n"
            f"2. 露点是否发生空气混入型突变？\n"
            f"   - VENT0 后露点变化：___ (expected: 稳定，无突跳)\n\n"
            f"3. COM22 压力是否在 VENT0 后上升？\n"
            f"   - baseline={self.com22_baseline} max={self.com22_max} rise={com22_rise}\n\n"
            f"4. safe_stop 决策：\n"
            f"   - safe_stop_before={self.args.safe_stop_before}\n"
            f"   - safe_stop_after={self.args.safe_stop_after}\n\n"
            f"5. OUTP 统计来源：\n"
            f"   - io_log_exists={io_log_exists}\n"
            f"   - OUTP0(io_log)={io_outp0} OUTP1(io_log)={io_outp1}\n\n"
            f"Trace: {tp}\n",
            encoding="utf-8",
        )

    def _close_devices_safe(self) -> None:
        for name, dev in list(self.devices.items()):
            try:
                if hasattr(dev, "close"):
                    dev.close()
            except Exception:
                pass


# ── entry point ────────────────────────────────────────────────────

def main(argv: Optional[List[str]] = None) -> NoOutpProbe:
    parser = argparse.ArgumentParser(
        description="V1.5 no-OUTP preseal physical probe (RESEARCH ONLY)",
    )
    parser.add_argument("--config", required=True, help="Path to site config JSON")
    parser.add_argument("--co2-ppm", type=float, default=1000.0, help="CO2 ppm for source valve (default: 1000)")
    parser.add_argument("--temp", type=float, default=20.0, help="Target temperature (informational)")
    parser.add_argument("--observe-s", type=float, default=10.0, help="Observe window after VENT0 (seconds)")
    parser.add_argument("--min-rise-hpa", type=float, default=5.0, help="Minimum COM22 pressure rise for pass")
    parser.add_argument("--no-write", action="store_true", default=True)
    parser.add_argument("--safe-stop-before", action="store_true", default=False, help="Run safe_stop before probe")
    parser.add_argument("--safe-stop-after", action="store_true", default=False, help="Run safe_stop after probe")
    add_engineering_diagnostic_guard_args(parser)
    args = parser.parse_args(argv)
    require_engineering_diagnostic_guard(args, parser, context="no-OUTP preseal physical probe")

    probe = NoOutpProbe(args)
    return probe


if __name__ == "__main__":
    probe = main()
    code = probe.run()
    sys.exit(code)
