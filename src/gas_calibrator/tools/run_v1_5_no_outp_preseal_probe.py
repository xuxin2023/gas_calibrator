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
import json
import os
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from ..config import load_config
from ..devices import (
    DewpointMeter,
    Pace5000,
    ParoscientificGauge,
)


def _log(msg: str) -> None:
    print(msg, flush=True)


def _now_ts() -> str:
    from datetime import datetime
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def _write_json(path: Path, obj: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, ensure_ascii=False, default=str)


def _csv_row(path: Path, header: List[str], row: List[Any]) -> None:
    import csv
    write_header = not path.exists()
    with open(path, "a", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        if write_header:
            w.writerow(header)
        w.writerow(row)


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
        self._cleanup_outp0: int = 0
        self._cleanup_vent1: int = 0
        self._observations: List[Dict[str, Any]] = []

        self.com22_baseline: Optional[float] = None
        self.com22_max: Optional[float] = None
        self.pace_baseline: Optional[float] = None
        self.pace_max: Optional[float] = None
        self.dewpoint_baseline: Optional[float] = None
        self.dewpoint_max_delta: Optional[float] = None

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
        "postrun_corrected_delivery.enabled",
        "postrun_corrected_delivery.write_devices",
        "postrun_corrected_delivery.write_pressure_coefficients",
        "postrun.write_devices",
        "postrun.write_pressure_coefficients",
        "startup_pressure_sensor_calibration.enabled",
        "startup_pressure_sensor_calibration.apply_write",
        "workflow.startup_pressure_precheck.enabled",
        "spc.apply_write",
        "spc.enabled",
    ]

    def _no_write_preflight(self) -> Optional[str]:
        failures: List[str] = []
        for key in self._WRITE_RISK_KEYS:
            val = self._deep_get(self.cfg, key)
            is_bad = (isinstance(val, bool) and val is True)
            if is_bad:
                failures.append(f"{key}={val}")

        spc = self._deep_get(self.cfg, "startup_pressure_sensor_calibration", {})
        if isinstance(spc, dict) and spc.get("apply_write"):
            failures.append("startup_pressure_sensor_calibration.apply_write=True")

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

        self._io_lines: List[Tuple[str, str, str]] = []

        def io_log(tag: str, direction: str, payload: str) -> None:
            self._io_lines.append((tag, direction, payload))

        io_log("PROBE", "event", "probe_start")

        pc = dcfg.get("pressure_controller", {})
        if pc.get("enabled"):
            self.devices["pace"] = Pace5000(
                pc["port"], pc.get("baud", 115200),
                timeout=float(pc.get("timeout", 1.0)),
                line_ending=pc.get("line_ending"),
                query_line_endings=pc.get("query_line_endings"),
                pressure_queries=pc.get("pressure_queries"),
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
            )
            self.devices["dewpoint"].open()
            _log("Dewpoint meter opened")

        if not self.devices:
            raise RuntimeError("No devices enabled — cannot probe")

    def _valve_configs(self) -> Dict[str, Optional[int]]:
        v = self.cfg.get("valves", {}) if isinstance(self.cfg.get("valves"), dict) else {}
        volts: Dict[str, Optional[int]] = {}
        for key in ("h2o_path", "gas_main", "co2_path", "co2_path_group2"):
            val = v.get(key)
            volts[key] = int(val) if val is not None else None
        return volts

    def _source_valve_for_ppm(self, ppm: float) -> Optional[int]:
        v = self.cfg.get("valves", {}) if isinstance(self.cfg.get("valves"), dict) else {}
        map_a = v.get("co2_map", {}) if isinstance(v.get("co2_map"), dict) else {}
        map_b = v.get("co2_map_group2", {}) if isinstance(v.get("co2_map_group2"), dict) else {}
        key = str(int(ppm))
        if key in map_a:
            return int(map_a[key])
        if key in map_b:
            return int(map_b[key])
        return None

    def _co2_open_valves(self) -> List[int]:
        vc = self._valve_configs()
        source = self._source_valve_for_ppm(float(self.args.co2_ppm))
        open_list: List[int] = []
        for v in (vc.get("h2o_path"), vc.get("gas_main"), vc.get("co2_path"), source):
            if v is not None:
                open_list.append(int(v))
        return open_list

    def _close_valves(self) -> None:
        _log("Closing CO2 route valves")
        relay = self.devices.get("relay")
        if relay is not None:
            try:
                close = getattr(relay, "close", None)
                if callable(close):
                    close()
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

    def _read_dewpoint(self) -> Optional[float]:
        try:
            val = float(self.devices["dewpoint"].read_dewpoint())
            return val if val == val else None
        except Exception:
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
        finally:
            self._cleanup_outp0 = self._outp0_count
            self._cleanup_vent1 = self._vent1_count

    # ── main probe ─────────────────────────────────────────────────

    def run(self) -> int:
        _log("=== V1.5 no-OUTP preseal physical probe ===")
        _log(f"  branch: codex/v1.5-pace-no-outp-transition-research")

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

        try:
            return self._probe()
        finally:
            self._safe_stop_after()
            self._close_devices_safe()
            self._write_summary()
            _log(f"Probe complete. Results: {self.summary_path}")

    def _probe(self) -> int:
        open_valves = self._co2_open_valves()
        _log(f"CO2 route open valves: {open_valves}")

        # ── Open CO2 route ──
        relay = self.devices.get("relay")
        if relay is None:
            _log("WARNING: no relay device — skipping CO2 route valve open (mock/test mode)")
        else:
            try:
                activate = getattr(relay, "activate", None)
                if callable(activate):
                    activate(open_valves)
                else:
                    _log("WARNING: relay has no activate method — skipping valve open")
            except Exception as exc:
                _log(f"Relay activate failed (continuing): {exc}")
        _log("CO2 route valves opened")

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

        iso = getattr(pace, "set_isolation_open", None)
        if callable(iso):
            iso(True)

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

        failures: List[str] = []
        if self._outp0_count > 0:
            failures.append(f"OUTP0_count={self._outp0_count}")
        if self._outp1_count > 0:
            failures.append(f"OUTP1_count={self._outp1_count}")
        if com22_rise is None or com22_rise < min_rise:
            failures.append(f"COM22_rise={com22_rise:.2f} < {min_rise}" if com22_rise is not None
                            else "COM22_rise=no_data")

        self._final_decision: str = "PASS" if not failures else f"FAIL: {'; '.join(failures)}"

        _log("")
        _log(f"OUTP0 count (probe phase): {self._outp0_count}")
        _log(f"OUTP1 count (probe phase): {self._outp1_count}")
        _log(f"VENT1 count (openflow):    {self._vent1_count}")
        _log(f"VENT0 count (close):      {self._vent0_count}")
        _log(f"COM22 baseline:           {self.com22_baseline}")
        _log(f"COM22 max:                {self.com22_max}")
        _log(f"COM22 rise:               {com22_rise}")
        _log(f"PACE baseline:            {self.pace_baseline}")
        _log(f"PACE max:                 {self.pace_max}")
        _log(f"Dewpoint baseline:        {self.dewpoint_baseline}")
        _log(f"Dewpoint max delta:       {self.dewpoint_max_delta}")
        _log(f"Decision:                 {self._final_decision}")

        if failures:
            return 1
        return 0

    def _write_summary(self) -> None:
        if not self.summary_path:
            return
        com22_rise = None
        if self.com22_baseline is not None and self.com22_max is not None:
            com22_rise = self.com22_max - self.com22_baseline

        summary: Dict[str, Any] = {
            "final_decision": getattr(self, "_final_decision", "UNKNOWN"),
            "branch": "codex/v1.5-pace-no-outp-transition-research",
            "config_path": str(self.config_path),
            "no_outp_transition_mode": True,
            "no_write_preflight": "PASS" if self._no_write_preflight() is None else "FAIL",
            "startup_hold_check_disabled": True,
            "outp0_count_probe_phase": self._outp0_count,
            "outp1_count_probe_phase": self._outp1_count,
            "vent1_count_openflow": self._vent1_count,
            "vent0_count_close": self._vent0_count,
            "com22_pressure_baseline_hpa": self.com22_baseline,
            "com22_pressure_max_hpa": self.com22_max,
            "com22_pressure_rise_hpa": com22_rise,
            "pace_pressure_baseline_hpa": self.pace_baseline,
            "pace_pressure_max_hpa": self.pace_max,
            "dewpoint_baseline": self.dewpoint_baseline,
            "dewpoint_max_delta": self.dewpoint_max_delta,
            "operator_pace_vent_popup_observed": None,
            "operator_dewpoint_air_ingress_observed": None,
            "cleanup_completed": True,
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
    args = parser.parse_args(argv)

    probe = NoOutpProbe(args)
    return probe


if __name__ == "__main__":
    probe = main()
    code = probe.run()
    sys.exit(code)
