"""V1.5 pressure-only sealed tuning harness.

This tool is intentionally isolated from the V1 production workflow.  It does
not open gas source valves, does not start HGEN, and does not write calibration
parameters.  It is for bounded no-write engineering tuning only.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import re
import time
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from ..config import load_config
from ..devices import DewpointMeter, Pace5000, ParoscientificGauge, RelayController


FORBIDDEN_CAL_WRITE_RE = re.compile(
    r"(?:\bSENCO\b|\bZERO\b|\bSPAN\b|\bCAL(?:IBRATION)?\b|\bCOEF(?:FICIENT)?\b)",
    re.IGNORECASE,
)


def _now_iso() -> str:
    return datetime.now().isoformat(timespec="milliseconds")


def _stamp() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def _as_float(value: Any) -> Optional[float]:
    try:
        out = float(value)
    except Exception:
        return None
    if not math.isfinite(out):
        return None
    return out


def _as_int(value: Any) -> Optional[int]:
    try:
        return int(float(value))
    except Exception:
        return None


def _deep_get(data: Mapping[str, Any], dotted: str, default: Any = None) -> Any:
    cur: Any = data
    for part in dotted.split("."):
        if isinstance(cur, Mapping) and part in cur:
            cur = cur[part]
        else:
            return default
    return cur


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False, default=str), encoding="utf-8")


def _append_csv(path: Path, fieldnames: Sequence[str], row: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    write_header = not path.exists()
    with path.open("a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(fieldnames), extrasaction="ignore")
        if write_header:
            writer.writeheader()
        writer.writerow({key: row.get(key, "") for key in fieldnames})


class PressureTuneIoLogger:
    """Small IO logger compatible with existing device log_io calls."""

    fieldnames = ["timestamp", "port", "device", "direction", "command", "response", "error"]

    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._handle = self.path.open("w", newline="", encoding="utf-8")
        self._writer = csv.DictWriter(self._handle, fieldnames=self.fieldnames)
        self._writer.writeheader()
        self._handle.flush()

    @staticmethod
    def _text(value: Any) -> str:
        return "" if value is None else str(value)

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
        self._writer.writerow(
            {
                "timestamp": _now_iso(),
                "port": self._text(port),
                "device": self._text(device),
                "direction": self._text(direction),
                "command": self._text(command),
                "response": self._text(response),
                "error": self._text(error),
            }
        )
        self._handle.flush()

    def close(self) -> None:
        try:
            self._handle.close()
        except Exception:
            pass


def _managed_logical_valves(cfg: Mapping[str, Any]) -> List[int]:
    valves_cfg = cfg.get("valves", {}) if isinstance(cfg.get("valves"), Mapping) else {}
    managed: set[int] = set()
    for key in ("co2_path", "co2_path_group2", "gas_main", "h2o_path", "hold", "flow_switch"):
        value = _as_int(valves_cfg.get(key))
        if value is not None:
            managed.add(value)
    for key in ("co2_map", "co2_map_group2"):
        mapping = valves_cfg.get(key, {}) if isinstance(valves_cfg.get(key), Mapping) else {}
        for value in mapping.values():
            numeric = _as_int(value)
            if numeric is not None:
                managed.add(numeric)
    return sorted(managed)


def _resolve_valve_target(cfg: Mapping[str, Any], logical_valve: int) -> Tuple[str, int]:
    valves_cfg = cfg.get("valves", {}) if isinstance(cfg.get("valves"), Mapping) else {}
    relay_map = valves_cfg.get("relay_map", {}) if isinstance(valves_cfg.get("relay_map"), Mapping) else {}
    entry = relay_map.get(str(logical_valve))
    relay_name = "relay"
    channel = int(logical_valve)
    if isinstance(entry, Mapping):
        relay_name = str(entry.get("device") or "relay")
        mapped = _as_int(entry.get("channel"))
        if mapped is not None:
            channel = mapped
    return relay_name, channel


def _apply_logical_valves_closed(cfg: Mapping[str, Any], devices: Mapping[str, Any]) -> List[int]:
    managed = _managed_logical_valves(cfg)
    grouped: Dict[str, List[Tuple[int, bool]]] = {}
    for logical_valve in managed:
        relay_name, channel = _resolve_valve_target(cfg, logical_valve)
        grouped.setdefault(relay_name, []).append((channel, False))
    for relay_name, updates in grouped.items():
        relay = devices.get(relay_name)
        if relay is None:
            raise RuntimeError(f"Relay '{relay_name}' required to close logical valves")
        bulk = getattr(relay, "set_valves_bulk", None)
        if callable(bulk):
            bulk(updates)
            continue
        for channel, state in updates:
            relay.set_valve(channel, state)
    return managed


@dataclass(frozen=True)
class TrialParams:
    trial_id: int
    slew_rate_hpa_per_s: float
    approach_slow_zone_hpa: float
    slow_slew_rate_hpa_per_s: float
    fast_monitor_interval_s: float
    upper_window_hpa: float
    burst_rows: int
    burst_interval_s: float
    max_monitor_s: float = 45.0


DEFAULT_TRIALS: Tuple[TrialParams, ...] = (
    TrialParams(1, 5.0, 20.0, 1.0, 0.2, 1.0, 3, 0.2),
    TrialParams(2, 3.0, 20.0, 1.0, 0.2, 1.0, 3, 0.2),
    TrialParams(3, 3.0, 20.0, 0.5, 0.2, 0.5, 3, 0.2),
    TrialParams(4, 2.0, 20.0, 0.5, 0.1, 0.5, 3, 0.2),
    TrialParams(5, 1.0, 20.0, 0.5, 0.1, 1.0, 5, 0.2),
    TrialParams(6, 3.0, 20.0, 0.5, 0.1, 1.0, 3, 0.2),
)


QUERY_LATENCY_FIELDS = [
    "query_type",
    "command",
    "count",
    "min_ms",
    "p50_ms",
    "p95_ms",
    "max_ms",
    "timeout_count",
    "parse_error_count",
    "selected_for_fast_monitor",
    "reason",
]


TRIAL_SUMMARY_FIELDS = [
    "trial_id",
    "parameter_set",
    "start_pressure_pace",
    "start_pressure_com22",
    "route_close_ts",
    "setpoint_ts",
    "OUTP1_ts",
    "route_close_to_outp1_s",
    "target_hpa",
    "expected_direction",
    "pressure_source_used_for_trigger",
    "candidate_detected",
    "candidate_ts",
    "candidate_pressure_hpa",
    "candidate_offset_hpa",
    "candidate_to_first_sample_s",
    "sample_rows",
    "sample_valid_for_acceptance",
    "sample_invalidated_reason",
    "target_crossing_ts",
    "target_crossing_pressure",
    "positive_effort_max_pct",
    "VENT1_count_during_active_sealed",
    "VENT0_count_during_active_sealed",
    "VENT3_count",
    "dewpoint_local_rise",
    "dewpoint_abnormal",
    "safe_stop_status",
    "score",
    "final_decision",
]


SAMPLE_FIELDS = [
    "trial_id",
    "row_index",
    "sample_ts",
    "sample_trigger",
    "candidate_pressure_hpa",
    "candidate_pressure_offset_hpa",
    "sample_snapshot_pressure_hpa",
    "actual_pressure_used_for_sample",
    "actual_pressure_source_for_sample",
    "nominal_target_hpa",
    "effort_pct",
    "vent_status",
    "dewpoint_c",
    "com22_pressure_hpa",
    "sample_valid_for_acceptance",
    "sample_invalidated",
    "sample_invalidated_reason",
    "sample_invalidated_by_target_crossing",
    "sample_invalidated_by_positive_effort",
    "sample_invalidated_by_dewpoint",
    "sample_invalidated_by_vent",
    "per_device_age_ms",
    "per_device_source",
    "alignment_ok",
    "alignment_failure_reason",
    "line_contaminated_by_pressure_tuning",
    "not_real_acceptance_evidence",
]


class V15SealedPressureTune900:
    def __init__(
        self,
        cfg: Mapping[str, Any],
        *,
        devices: Optional[Mapping[str, Any]] = None,
        output_dir: Optional[Path] = None,
        config_path: Optional[Path] = None,
        max_trials: int = 6,
        target_hpa: float = 900.0,
        no_write: bool = True,
        confirm_pressure_only_tuning: bool = False,
        trials: Sequence[TrialParams] = DEFAULT_TRIALS,
    ) -> None:
        self.cfg = dict(cfg)
        self.devices: Dict[str, Any] = dict(devices or {})
        self.config_path = config_path
        self.max_trials = max(1, min(int(max_trials), 8))
        self.target_hpa = float(target_hpa)
        self.no_write = bool(no_write)
        self.confirm_pressure_only_tuning = bool(confirm_pressure_only_tuning)
        self.trials = list(trials)[: self.max_trials]
        root = Path(str(_deep_get(self.cfg, "paths.output_dir", "logs") or "logs"))
        self.output_dir = output_dir or root / "v1_5_pressure_only_tuning_900" / f"run_{_stamp()}"
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.io_log_path = self.output_dir / "io_log.csv"
        self.query_latency_path = self.output_dir / "query_latency_audit.csv"
        self.summary_path = self.output_dir / "sealed_pressure_tuning_900_summary.csv"
        self.samples_path = self.output_dir / "sealed_pressure_tuning_900_samples.csv"
        self.report_path = self.output_dir / "sealed_pressure_tuning_900_report.md"
        self.json_summary_path = self.output_dir / "sealed_pressure_tuning_900_summary.json"
        self.io_logger: Optional[PressureTuneIoLogger] = None
        self.selected_pressure_query: str = "read_pressure"
        self.query_latency_rows: List[Dict[str, Any]] = []
        self.trial_rows: List[Dict[str, Any]] = []
        self.sample_rows: List[Dict[str, Any]] = []
        self.line_contaminated = False
        self.cleanup_vent_after_abort_or_trial_end = False
        self.rerun_requires_full_open_flow_flush = False
        self.live_dewpoint_enabled = bool(
            _deep_get(self.cfg, "workflow.pressure_tuning.live_dewpoint_enabled", False)
        )

    def preflight(self) -> List[str]:
        issues: List[str] = []
        workflow = self.cfg.get("workflow", {}) if isinstance(self.cfg.get("workflow"), Mapping) else {}
        if not self.no_write:
            issues.append("no_write flag is required")
        if not self.confirm_pressure_only_tuning:
            issues.append("confirm_pressure_only_tuning is required")
        if workflow.get("collect_only") is not True:
            issues.append("workflow.collect_only must be true")
        if workflow.get("production") is True:
            issues.append("workflow.production must be false")
        if workflow.get("controlled_write") is True:
            issues.append("workflow.controlled_write must be false")
        for key in (
            "coefficients.enabled",
            "workflow.startup_pressure_sensor_calibration.apply_write",
            "workflow.postrun_corrected_delivery.write_devices",
            "workflow.postrun_corrected_delivery.write_pressure_coefficients",
            "workflow.spc.apply_write",
        ):
            if _deep_get(self.cfg, key) is True:
                issues.append(f"{key}=true")
        for key in ("coefficients.sencos", "workflow.coefficients.sencos"):
            value = _deep_get(self.cfg, key)
            if isinstance(value, (Mapping, list, tuple, set)) and value:
                issues.append(f"{key} is not empty")
        if not _managed_logical_valves(self.cfg):
            issues.append("no managed relay valves found; cannot confirm route-close mapping")
        if not self.trials:
            issues.append("no bounded trial matrix")
        if self.max_trials > 8:
            issues.append("max_trials must be <= 8")
        return issues

    def build_devices(self) -> None:
        dcfg = self.cfg.get("devices", {}) if isinstance(self.cfg.get("devices"), Mapping) else {}
        self.io_logger = PressureTuneIoLogger(self.io_log_path)
        pc = dcfg.get("pressure_controller", {}) if isinstance(dcfg.get("pressure_controller"), Mapping) else {}
        if pc.get("enabled"):
            self.devices["pace"] = Pace5000(
                pc["port"],
                int(pc.get("baud", 115200)),
                timeout=float(pc.get("timeout", 1.0)),
                line_ending=pc.get("line_ending"),
                query_line_endings=pc.get("query_line_endings"),
                pressure_queries=pc.get("pressure_queries"),
                io_logger=self.io_logger,
            )
            self.devices["pace"].open()
        pg = dcfg.get("pressure_gauge", {}) if isinstance(dcfg.get("pressure_gauge"), Mapping) else {}
        if pg.get("enabled"):
            self.devices["pressure_gauge"] = ParoscientificGauge(
                pg["port"],
                int(pg.get("baud", 115200)),
                timeout=float(pg.get("timeout", 1.0)),
                dest_id=str(pg.get("dest_id", "01")),
                response_timeout_s=pg.get("response_timeout_s"),
                io_logger=self.io_logger,
            )
            self.devices["pressure_gauge"].open()
        dew = dcfg.get("dewpoint_meter", {}) if isinstance(dcfg.get("dewpoint_meter"), Mapping) else {}
        if dew.get("enabled"):
            self.devices["dewpoint"] = DewpointMeter(
                dew["port"],
                int(dew.get("baud", 115200)),
                station=int(dew.get("station", 1)),
                io_logger=self.io_logger,
            )
            self.devices["dewpoint"].open()
        for name in ("relay", "relay_8"):
            rcfg = dcfg.get(name, {}) if isinstance(dcfg.get(name), Mapping) else {}
            if rcfg.get("enabled"):
                self.devices[name] = RelayController(
                    rcfg["port"],
                    int(rcfg.get("baud", 38400)),
                    addr=int(rcfg.get("addr", 1)),
                    io_logger=self.io_logger,
                )
                self.devices[name].open()

    def close_devices(self) -> None:
        for dev in list(self.devices.values()):
            try:
                close = getattr(dev, "close", None)
                if callable(close):
                    close()
            except Exception:
                pass
        if self.io_logger:
            self.io_logger.close()

    def _pace(self) -> Any:
        pace = self.devices.get("pace")
        if pace is None:
            raise RuntimeError("PACE pressure controller is required")
        return pace

    def _read_com22(self) -> Tuple[Optional[float], Optional[float]]:
        gauge = self.devices.get("pressure_gauge")
        if gauge is None:
            return None, None
        ts = time.time()
        try:
            value = _as_float(gauge.read_pressure())
        except Exception:
            return None, None
        return value, ts

    def _read_dewpoint(self) -> Tuple[Optional[float], Optional[float]]:
        if not self.live_dewpoint_enabled:
            return None, None
        dew = self.devices.get("dewpoint")
        if dew is None:
            return None, None
        readers = []
        for name in ("get_current_fast", "get_current", "read_dewpoint", "read"):
            fn = getattr(dew, name, None)
            if callable(fn):
                readers.append(fn)
        for reader in readers:
            ts = time.time()
            try:
                raw = reader()
            except TypeError:
                try:
                    raw = reader(timeout_s=0.35)
                except Exception:
                    continue
            except Exception:
                continue
            value = raw
            if isinstance(raw, Mapping):
                for key in ("dewpoint_c", "dewpoint", "dp", "td", "value"):
                    if key in raw:
                        value = raw.get(key)
                        break
            numeric = _as_float(value)
            if numeric is not None:
                return numeric, ts
        return None, None

    def _parse_pressure_response(self, text: Any) -> Optional[float]:
        match = re.search(r"[-+]?\d+(?:\.\d+)?(?:[eE][-+]?\d+)?", str(text or ""))
        return _as_float(match.group(0)) if match else None

    def _query_pace_pressure(self, command: str) -> Tuple[Optional[float], str, float, Optional[str]]:
        pace = self._pace()
        begin = time.perf_counter()
        raw = ""
        err: Optional[str] = None
        value: Optional[float] = None
        try:
            if command == "read_pressure":
                value = _as_float(pace.read_pressure())
                raw = "" if value is None else str(value)
            else:
                raw = str(pace.query(command))
                value = self._parse_pressure_response(raw)
        except Exception as exc:
            err = str(exc)
        elapsed_ms = (time.perf_counter() - begin) * 1000.0
        return value, raw, elapsed_ms, err

    def audit_query_latency(self, *, repeats: int = 3) -> str:
        commands = [
            ("pace_cont_pressure", ":SENS:PRES:CONT?"),
            ("pace_inlimit_pressure", ":SENS:PRES:INL?"),
            ("pace_read_pressure", "read_pressure"),
        ]
        candidates: List[Tuple[str, float]] = []
        rows: List[Dict[str, Any]] = []
        for query_type, command in commands:
            latencies: List[float] = []
            timeout_count = 0
            parse_error_count = 0
            for _ in range(max(1, int(repeats))):
                value, _raw, elapsed_ms, err = self._query_pace_pressure(command)
                if err:
                    timeout_count += 1
                    continue
                if value is None:
                    parse_error_count += 1
                    continue
                latencies.append(elapsed_ms)
            sorted_lat = sorted(latencies)
            p50 = sorted_lat[len(sorted_lat) // 2] if sorted_lat else ""
            p95 = sorted_lat[min(len(sorted_lat) - 1, int(math.ceil(len(sorted_lat) * 0.95)) - 1)] if sorted_lat else ""
            row = {
                "query_type": query_type,
                "command": command,
                "count": len(latencies),
                "min_ms": min(sorted_lat) if sorted_lat else "",
                "p50_ms": p50,
                "p95_ms": p95,
                "max_ms": max(sorted_lat) if sorted_lat else "",
                "timeout_count": timeout_count,
                "parse_error_count": parse_error_count,
                "selected_for_fast_monitor": False,
                "reason": "",
            }
            rows.append(row)
            if latencies:
                candidates.append((command, float(p50)))
        if candidates:
            preferred_order = {":SENS:PRES:CONT?": 0, "read_pressure": 1, ":SENS:PRES:INL?": 2}
            self.selected_pressure_query = sorted(
                candidates,
                key=lambda item: (preferred_order.get(item[0], 10), item[1]),
            )[0][0]
        else:
            self.selected_pressure_query = "read_pressure"
        for row in rows:
            selected = row["command"] == self.selected_pressure_query
            row["selected_for_fast_monitor"] = selected
            row["reason"] = "fastest parseable PACE pressure command" if selected else "secondary evidence only"
            _append_csv(self.query_latency_path, QUERY_LATENCY_FIELDS, row)
        self.query_latency_rows = rows
        return self.selected_pressure_query

    def _configure_slew(self, params: TrialParams) -> Dict[str, Any]:
        pace = self._pace()
        fields: Dict[str, Any] = {
            "slew_mode_set": False,
            "slew_rate_set": "",
            "overshoot_not_allowed_set": False,
            "syst_err_after_commands": "",
            "configured_before_OUTP1": True,
            "changed_during_approach": False,
        }
        set_mode = getattr(pace, "set_slew_mode_linear", None)
        if callable(set_mode):
            set_mode()
            fields["slew_mode_set"] = True
        set_rate = getattr(pace, "set_slew_rate", None)
        if callable(set_rate):
            set_rate(float(params.slew_rate_hpa_per_s))
            fields["slew_rate_set"] = float(params.slew_rate_hpa_per_s)
        set_over = getattr(pace, "set_overshoot_allowed", None)
        if callable(set_over):
            set_over(False)
            fields["overshoot_not_allowed_set"] = True
        query = getattr(pace, "query", None)
        if callable(query):
            try:
                fields["syst_err_after_commands"] = str(query(":SYST:ERR?"))
            except Exception as exc:
                fields["syst_err_after_commands"] = f"ERROR:{exc}"
        return fields

    def _count_io_commands(self, *, active_start_ts: Optional[str] = None, active_end_ts: Optional[str] = None) -> Dict[str, int]:
        counts = {
            "vent1": 0,
            "vent0": 0,
            "vent3": 0,
            "outp1": 0,
            "forbidden_calibration_writes": 0,
        }
        path = self.io_log_path
        if not path.exists():
            return counts
        start = datetime.fromisoformat(active_start_ts) if active_start_ts else None
        end = datetime.fromisoformat(active_end_ts) if active_end_ts else None
        with path.open("r", newline="", encoding="utf-8") as handle:
            for row in csv.DictReader(handle):
                ts_text = str(row.get("timestamp") or "")
                try:
                    ts = datetime.fromisoformat(ts_text)
                except Exception:
                    ts = None
                if start is not None and ts is not None and ts < start:
                    continue
                if end is not None and ts is not None and ts > end:
                    continue
                if str(row.get("direction", "")).upper() != "TX":
                    continue
                command = str(row.get("command", "") or "").upper()
                compact = command.replace(" ", "")
                if "VENT1" in compact or ":SOUR:PRES:LEV:IMM:AMPL:VENT1" in compact:
                    counts["vent1"] += 1
                if "VENT0" in compact or ":SOUR:PRES:LEV:IMM:AMPL:VENT0" in compact:
                    counts["vent0"] += 1
                if ":OUTP1" in compact:
                    counts["outp1"] += 1
                if FORBIDDEN_CAL_WRITE_RE.search(command):
                    counts["forbidden_calibration_writes"] += 1
        return counts

    def _read_effort_pct(self) -> Tuple[Optional[float], str]:
        pace = self._pace()
        query = getattr(pace, "query", None)
        if not callable(query):
            return None, "effort_query_unavailable"
        try:
            raw = str(query(":SOUR:PRES:EFF?"))
        except Exception as exc:
            return None, str(exc)
        value = self._parse_pressure_response(raw)
        return value, raw

    def _read_vent_status(self) -> Optional[int]:
        pace = self._pace()
        getter = getattr(pace, "get_vent_status", None)
        if callable(getter):
            try:
                return _as_int(getter())
            except Exception:
                return None
        return None

    def _enable_output(self) -> None:
        pace = self._pace()
        enable = getattr(pace, "enable_control_output", None)
        if callable(enable):
            enable()
            return
        iso = getattr(pace, "set_isolation_open", None)
        if callable(iso):
            iso(True)
        mode = getattr(pace, "set_output_mode_active", None)
        if callable(mode):
            mode()
        pace.set_output(True)

    def _wait_return_to_atmosphere(
        self,
        *,
        reference_pressure_hpa: Optional[float],
        timeout_s: float = 20.0,
        tolerance_hpa: float = 15.0,
    ) -> Dict[str, Any]:
        begin = time.time()
        reference = reference_pressure_hpa if reference_pressure_hpa is not None else 1007.0
        last_pace: Optional[float] = None
        last_com22: Optional[float] = None
        ok = False
        while time.time() - begin <= max(0.0, float(timeout_s)):
            try:
                last_pace = _as_float(self._pace().read_pressure())
            except Exception:
                last_pace = None
            last_com22, _ts = self._read_com22()
            candidates = [value for value in (last_pace, last_com22) if value is not None]
            if candidates and any(abs(float(value) - float(reference)) <= float(tolerance_hpa) for value in candidates):
                ok = True
                break
            time.sleep(0.5)
        return {
            "return_to_atmosphere_ok": ok,
            "return_to_atmosphere_reference_hpa": reference,
            "return_to_atmosphere_tolerance_hpa": float(tolerance_hpa),
            "return_to_atmosphere_elapsed_s": max(0.0, time.time() - begin),
            "return_to_atmosphere_pace_pressure_hpa": last_pace if last_pace is not None else "",
            "return_to_atmosphere_com22_pressure_hpa": last_com22 if last_com22 is not None else "",
        }

    def _safe_stop_trial(self, *, reference_pressure_hpa: Optional[float] = None) -> Dict[str, Any]:
        status: Dict[str, Any] = {"safe_stop_ts": _now_iso(), "ok": True}
        pace = self.devices.get("pace")
        if pace is not None:
            try:
                set_output = getattr(pace, "set_output", None)
                if callable(set_output):
                    set_output(False)
                iso = getattr(pace, "set_isolation_open", None)
                if callable(iso):
                    iso(True)
                enter = getattr(pace, "enter_atmosphere_mode", None)
                if callable(enter):
                    enter()
                else:
                    vent = getattr(pace, "vent", None)
                    if callable(vent):
                        vent(True)
                self.cleanup_vent_after_abort_or_trial_end = True
            except Exception as exc:
                status["ok"] = False
                status["pace_error"] = str(exc)
            try:
                status["final_pace_pressure_hpa"] = pace.read_pressure()
            except Exception as exc:
                status["final_pace_pressure_error"] = str(exc)
            for key, method_name in (
                ("final_pace_outp", "get_output_state"),
                ("final_pace_isol", "get_isolation_state"),
                ("final_pace_vent", "get_vent_status"),
            ):
                method = getattr(pace, method_name, None)
                if callable(method):
                    try:
                        status[key] = method()
                    except Exception as exc:
                        status[f"{key}_error"] = str(exc)
        try:
            _apply_logical_valves_closed(self.cfg, self.devices)
            status["relay_reset"] = True
        except Exception as exc:
            status["relay_reset"] = False
            status["relay_reset_error"] = str(exc)
        com22, _ts = self._read_com22()
        status["final_com22_pressure_hpa"] = com22 if com22 is not None else ""
        status.update(self._wait_return_to_atmosphere(reference_pressure_hpa=reference_pressure_hpa))
        self.line_contaminated = True
        self.rerun_requires_full_open_flow_flush = True
        return status

    def run_trial(self, params: TrialParams) -> Dict[str, Any]:
        target = float(self.target_hpa)
        pace = self._pace()
        route_close_ts = _now_iso()
        active_start = route_close_ts
        start_pace, _raw, _lat, _err = self._query_pace_pressure(self.selected_pressure_query)
        start_com22, _com22_ts = self._read_com22()
        _apply_logical_valves_closed(self.cfg, self.devices)
        slew_fields = self._configure_slew(params)
        slow_slew_applied = False
        setpoint_ts = _now_iso()
        pace.set_setpoint(target)
        outp_begin = time.time()
        self._enable_output()
        outp1_ts = _now_iso()
        route_close_to_outp1_s = (
            datetime.fromisoformat(outp1_ts) - datetime.fromisoformat(route_close_ts)
        ).total_seconds()
        candidate_detected = False
        candidate_ts = ""
        candidate_pressure: Optional[float] = None
        candidate_to_first_sample_s: Optional[float] = None
        target_crossing_ts = ""
        target_crossing_pressure: Optional[float] = None
        positive_effort_max = 0.0
        vent3_count = 0
        sample_invalidated_reason = ""
        sample_count = 0
        final_decision = "NO_CANDIDATE"
        pressure_series: List[Dict[str, Any]] = []
        adaptive_monitor_s = float(params.max_monitor_s)
        if start_pace is not None and start_pace > target and params.slew_rate_hpa_per_s > 0:
            adaptive_monitor_s = max(
                adaptive_monitor_s,
                (float(start_pace) - target) / max(0.1, float(params.slew_rate_hpa_per_s)) + 15.0,
            )
        deadline = time.time() + max(0.5, adaptive_monitor_s)
        last_poll = time.time()
        dewpoint_baseline, _dew_ts = self._read_dewpoint()
        dewpoint_local_rise = 0.0
        dewpoint_abnormal = False
        while time.time() < deadline:
            pressure, raw_pressure, latency_ms, pressure_err = self._query_pace_pressure(self.selected_pressure_query)
            now = time.time()
            interval_s = max(0.0, now - last_poll)
            last_poll = now
            effort, raw_effort = self._read_effort_pct()
            if effort is not None:
                positive_effort_max = max(positive_effort_max, float(effort))
            vent_status = self._read_vent_status()
            if vent_status == 3:
                vent3_count += 1
            dewpoint_c, dew_ts = self._read_dewpoint()
            if dewpoint_baseline is not None and dewpoint_c is not None:
                dewpoint_local_rise = max(dewpoint_local_rise, float(dewpoint_c) - float(dewpoint_baseline))
                dewpoint_abnormal = dewpoint_abnormal or dewpoint_local_rise > 3.0
            pressure_series.append(
                {
                    "ts": _now_iso(),
                    "pressure_hpa": pressure if pressure is not None else "",
                    "raw_pressure": raw_pressure,
                    "latency_ms": latency_ms,
                    "error": pressure_err or "",
                    "interval_s": interval_s,
                    "effort_pct": effort if effort is not None else "",
                    "vent_status": vent_status if vent_status is not None else "",
                    "dewpoint_c": dewpoint_c if dewpoint_c is not None else "",
                }
            )
            if pressure is None:
                time.sleep(max(0.0, float(params.fast_monitor_interval_s)))
                continue
            if (
                not slow_slew_applied
                and params.slow_slew_rate_hpa_per_s > 0
                and params.slow_slew_rate_hpa_per_s < params.slew_rate_hpa_per_s
                and pressure <= target + max(0.0, float(params.approach_slow_zone_hpa))
            ):
                set_rate = getattr(pace, "set_slew_rate", None)
                if callable(set_rate):
                    set_rate(float(params.slow_slew_rate_hpa_per_s))
                    slow_slew_applied = True
            if vent_status in {1, 3}:
                final_decision = "FAIL_CLOSED_VENT_STATUS_ACTIVE_OR_TRAPPED"
                sample_invalidated_reason = "vent"
                break
            if effort is not None and effort >= 0.3:
                final_decision = "FAIL_CLOSED_POSITIVE_EFFORT_BEFORE_CANDIDATE"
                sample_invalidated_reason = "positive_effort"
                break
            if dewpoint_abnormal:
                final_decision = "FAIL_CLOSED_DEWPOINT_ABNORMAL_BEFORE_CANDIDATE"
                sample_invalidated_reason = "dewpoint"
                break
            if pressure < target:
                target_crossing_ts = _now_iso()
                target_crossing_pressure = pressure
                final_decision = "FAIL_CLOSED_TARGET_CROSSING_BEFORE_SAMPLE"
                sample_invalidated_reason = "target_crossing"
                break
            if target <= pressure <= target + float(params.upper_window_hpa):
                candidate_detected = True
                candidate_ts = _now_iso()
                candidate_pressure = pressure
                rows = self._collect_short_burst(
                    params=params,
                    target_hpa=target,
                    candidate_ts=candidate_ts,
                    candidate_pressure_hpa=pressure,
                    candidate_effort_pct=effort,
                    candidate_vent_status=vent_status,
                    candidate_dewpoint_c=dewpoint_c,
                )
                sample_count = len(rows)
                first_sample_ts = rows[0]["sample_ts"] if rows else candidate_ts
                candidate_to_first_sample_s = (
                    datetime.fromisoformat(first_sample_ts) - datetime.fromisoformat(candidate_ts)
                ).total_seconds()
                invalid_reasons = [
                    str(row.get("sample_invalidated_reason") or "")
                    for row in rows
                    if str(row.get("sample_invalidated_reason") or "")
                ]
                sample_invalidated_reason = invalid_reasons[0] if invalid_reasons else ""
                final_decision = "SAMPLED_INVALIDATED" if sample_invalidated_reason else "SAMPLED_NO_WRITE"
                break
            time.sleep(max(0.0, float(params.fast_monitor_interval_s)))
        active_end = _now_iso()
        io_counts = self._count_io_commands(active_start_ts=active_start, active_end_ts=active_end)
        atmosphere_reference = start_com22 if start_com22 is not None else start_pace
        safe_stop_status = self._safe_stop_trial(reference_pressure_hpa=atmosphere_reference)
        if io_counts["vent1"] or io_counts["vent0"]:
            final_decision = "FAIL_CLOSED_ACTIVE_SEALED_VENT_WRITE"
            sample_invalidated_reason = sample_invalidated_reason or "vent"
        score = self._score_trial(
            candidate_detected=candidate_detected,
            sample_rows=sample_count,
            candidate_to_first_sample_s=candidate_to_first_sample_s,
            target_crossing_before_sample=bool(target_crossing_ts and not candidate_detected),
            positive_effort_before_sample=final_decision == "FAIL_CLOSED_POSITIVE_EFFORT_BEFORE_CANDIDATE",
            vent_fail=bool(io_counts["vent1"] or io_counts["vent0"] or vent3_count),
            dewpoint_abnormal=dewpoint_abnormal,
        )
        row = {
            "trial_id": params.trial_id,
            "parameter_set": json.dumps(asdict(params), ensure_ascii=False),
            "start_pressure_pace": start_pace if start_pace is not None else "",
            "start_pressure_com22": start_com22 if start_com22 is not None else "",
            "route_close_ts": route_close_ts,
            "setpoint_ts": setpoint_ts,
            "OUTP1_ts": outp1_ts,
            "route_close_to_outp1_s": route_close_to_outp1_s,
            "target_hpa": target,
            "expected_direction": "exhaust_only",
            "pressure_source_used_for_trigger": f"PACE:{self.selected_pressure_query}",
            "candidate_detected": candidate_detected,
            "candidate_ts": candidate_ts,
            "candidate_pressure_hpa": candidate_pressure if candidate_pressure is not None else "",
            "candidate_offset_hpa": (
                candidate_pressure - target if candidate_pressure is not None else ""
            ),
            "candidate_to_first_sample_s": (
                candidate_to_first_sample_s if candidate_to_first_sample_s is not None else ""
            ),
            "sample_rows": sample_count,
            "sample_valid_for_acceptance": False,
            "sample_invalidated_reason": sample_invalidated_reason,
            "target_crossing_ts": target_crossing_ts,
            "target_crossing_pressure": target_crossing_pressure if target_crossing_pressure is not None else "",
            "positive_effort_max_pct": positive_effort_max,
            "VENT1_count_during_active_sealed": io_counts["vent1"],
            "VENT0_count_during_active_sealed": io_counts["vent0"],
            "VENT3_count": vent3_count,
            "dewpoint_local_rise": dewpoint_local_rise,
            "dewpoint_abnormal": dewpoint_abnormal,
            "safe_stop_status": json.dumps(safe_stop_status, ensure_ascii=False, default=str),
            "score": score,
            "final_decision": final_decision,
            **slew_fields,
            "changed_during_approach": bool(slow_slew_applied),
            "fast_candidate_monitor_enabled": True,
            "fast_candidate_monitor_interval_s": params.fast_monitor_interval_s,
            "fast_candidate_monitor_sample_count": len(pressure_series),
            "fast_candidate_monitor_max_s": adaptive_monitor_s,
            "fast_candidate_monitor_pressures": json.dumps(pressure_series, ensure_ascii=False, default=str),
            "fast_candidate_monitor_max_gap_s": max(
                [float(item.get("interval_s") or 0.0) for item in pressure_series] or [0.0]
            ),
            "line_contaminated_by_pressure_tuning": True,
            "rerun_calibration_requires_full_open_flow_flush": True,
            "not_real_acceptance_evidence": True,
        }
        self.trial_rows.append(row)
        _append_csv(self.summary_path, list(dict.fromkeys([*TRIAL_SUMMARY_FIELDS, *row.keys()])), row)
        return row

    def _collect_short_burst(
        self,
        *,
        params: TrialParams,
        target_hpa: float,
        candidate_ts: str,
        candidate_pressure_hpa: float,
        candidate_effort_pct: Optional[float],
        candidate_vent_status: Optional[int],
        candidate_dewpoint_c: Optional[float],
    ) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        for row_index in range(1, max(1, int(params.burst_rows)) + 1):
            if row_index > 1:
                time.sleep(max(0.0, float(params.burst_interval_s)))
            sample_ts = _now_iso()
            pressure, _raw, _lat_ms, _err = self._query_pace_pressure(self.selected_pressure_query)
            effort, _eff_raw = self._read_effort_pct()
            vent_status = self._read_vent_status()
            com22, com22_ts = self._read_com22()
            dewpoint_c, dew_ts = self._read_dewpoint()
            invalid_reasons: List[str] = []
            if pressure is not None and pressure < target_hpa:
                invalid_reasons.append("target_crossing")
            if effort is not None and effort >= 0.3:
                invalid_reasons.append("positive_effort")
            if vent_status in {1, 3}:
                invalid_reasons.append("vent")
            if (
                candidate_dewpoint_c is not None
                and dewpoint_c is not None
                and float(dewpoint_c) - float(candidate_dewpoint_c) > 3.0
            ):
                invalid_reasons.append("dewpoint")
            reason = ",".join(invalid_reasons)
            now = time.time()
            ages = {
                "pace_ms": 0.0,
                "com22_ms": max(0.0, (now - com22_ts) * 1000.0) if com22_ts else "",
                "dewpoint_ms": max(0.0, (now - dew_ts) * 1000.0) if dew_ts else "",
            }
            source = {
                "pace": "live_query",
                "com22": "secondary_cache_or_live",
                "dewpoint": "optional_cache_or_live",
            }
            row = {
                "trial_id": params.trial_id,
                "row_index": row_index,
                "sample_ts": sample_ts,
                "sample_trigger": "fast_above_target_candidate",
                "candidate_pressure_hpa": candidate_pressure_hpa,
                "candidate_pressure_offset_hpa": candidate_pressure_hpa - target_hpa,
                "sample_snapshot_pressure_hpa": pressure if pressure is not None else "",
                "actual_pressure_used_for_sample": candidate_pressure_hpa if pressure is None else pressure,
                "actual_pressure_source_for_sample": "snapshot" if pressure is not None else "candidate",
                "nominal_target_hpa": target_hpa,
                "effort_pct": effort if effort is not None else candidate_effort_pct if candidate_effort_pct is not None else "",
                "vent_status": vent_status if vent_status is not None else candidate_vent_status if candidate_vent_status is not None else "",
                "dewpoint_c": dewpoint_c if dewpoint_c is not None else candidate_dewpoint_c if candidate_dewpoint_c is not None else "",
                "com22_pressure_hpa": com22 if com22 is not None else "",
                "sample_valid_for_acceptance": False,
                "sample_invalidated": bool(reason),
                "sample_invalidated_reason": reason,
                "sample_invalidated_by_target_crossing": "target_crossing" in invalid_reasons,
                "sample_invalidated_by_positive_effort": "positive_effort" in invalid_reasons,
                "sample_invalidated_by_dewpoint": "dewpoint" in invalid_reasons,
                "sample_invalidated_by_vent": "vent" in invalid_reasons,
                "per_device_age_ms": json.dumps(ages, ensure_ascii=False),
                "per_device_source": json.dumps(source, ensure_ascii=False),
                "alignment_ok": False,
                "alignment_failure_reason": "pressure_only_tuning_not_acceptance;alignment_is_evidence_only",
                "line_contaminated_by_pressure_tuning": True,
                "not_real_acceptance_evidence": True,
            }
            self.sample_rows.append(row)
            rows.append(row)
            _append_csv(self.samples_path, SAMPLE_FIELDS, row)
        return rows

    @staticmethod
    def _score_trial(
        *,
        candidate_detected: bool,
        sample_rows: int,
        candidate_to_first_sample_s: Optional[float],
        target_crossing_before_sample: bool,
        positive_effort_before_sample: bool,
        vent_fail: bool,
        dewpoint_abnormal: bool,
    ) -> int:
        if vent_fail or target_crossing_before_sample or positive_effort_before_sample:
            return 0
        score = 0
        if candidate_detected:
            score += 20
        if sample_rows > 0:
            score += 30
        if candidate_to_first_sample_s is not None and candidate_to_first_sample_s <= 1.0:
            score += 20
        if not target_crossing_before_sample:
            score += 20
        if not positive_effort_before_sample:
            score += 10
        if not vent_fail:
            score += 20
        if not dewpoint_abnormal:
            score += 10
        score += 10
        score += 10
        return score

    def run(self, *, build_devices: bool = True) -> int:
        issues = self.preflight()
        if issues:
            _write_json(self.json_summary_path, {"final_decision": "PREFLIGHT_BLOCKED", "issues": issues})
            return 2
        if build_devices and not self.devices:
            self.build_devices()
        try:
            self.audit_query_latency()
            consecutive_serious_fail = 0
            success_count = 0
            for idx, params in enumerate(self.trials, start=1):
                row = self.run_trial(params)
                decision = str(row.get("final_decision") or "")
                if decision.startswith("SAMPLED"):
                    success_count += 1
                    consecutive_serious_fail = 0
                elif "VENT" in decision or "POSITIVE_EFFORT" in decision or "HARDWARE" in decision:
                    consecutive_serious_fail += 1
                else:
                    consecutive_serious_fail = 0
                if idx >= 3 and success_count >= 3:
                    break
                if consecutive_serious_fail >= 3:
                    break
            self.write_report()
            return 0
        finally:
            self.close_devices()

    def write_report(self) -> None:
        best = max(self.trial_rows, key=lambda row: int(row.get("score") or 0), default={})
        payload = {
            "final_decision": "PRESSURE_ONLY_TUNING_COMPLETE",
            "config_path": str(self.config_path or ""),
            "run_dir": str(self.output_dir),
            "trial_count": len(self.trial_rows),
            "best_trial_id": best.get("trial_id", ""),
            "best_slew_rate": json.loads(best.get("parameter_set", "{}")).get("slew_rate_hpa_per_s", "")
            if best
            else "",
            "best_slow_slew_rate": json.loads(best.get("parameter_set", "{}")).get("slow_slew_rate_hpa_per_s", "")
            if best
            else "",
            "best_fast_monitor_interval": json.loads(best.get("parameter_set", "{}")).get(
                "fast_monitor_interval_s", ""
            )
            if best
            else "",
            "best_sampling_window": json.loads(best.get("parameter_set", "{}")).get("upper_window_hpa", "")
            if best
            else "",
            "best_burst_rows": json.loads(best.get("parameter_set", "{}")).get("burst_rows", "") if best else "",
            "best_burst_interval": json.loads(best.get("parameter_set", "{}")).get("burst_interval_s", "")
            if best
            else "",
            "selected_pressure_query": self.selected_pressure_query,
            "com22_secondary_only": True,
            "ten_by_one_second_sampling_unsuitable_for_fast_window": True,
            "line_contaminated_by_pressure_tuning": self.line_contaminated,
            "samples_after_cleanup_allowed": False,
            "rerun_calibration_requires_full_open_flow_flush": self.rerun_requires_full_open_flow_flush,
            "not_real_acceptance_evidence": True,
            "promotion_state": "blocked",
        }
        _write_json(self.json_summary_path, payload)
        lines = [
            "# V1.5 pressure-only sealed tuning 900 hPa",
            "",
            f"Run dir: {self.output_dir}",
            "",
            "This is not real acceptance evidence.",
            "",
            f"Selected PACE pressure query: `{self.selected_pressure_query}`",
            f"Trial count: {len(self.trial_rows)}",
            f"Best trial: {payload['best_trial_id']}",
            f"Best slew rate: {payload['best_slew_rate']}",
            f"Best slow slew rate: {payload['best_slow_slew_rate']}",
            f"Best fast monitor interval: {payload['best_fast_monitor_interval']}",
            f"Best sampling window upper hPa: {payload['best_sampling_window']}",
            f"Best burst rows: {payload['best_burst_rows']}",
            f"Best burst interval: {payload['best_burst_interval']}",
            "",
            "- COM22 is secondary evidence only; PACE pressure is the trigger source.",
            "- 10x1s sampling is unsuitable for a sub-second above-target window.",
            "- Cleanup vents to atmosphere only after trial end; line is contaminated.",
            "- Next calibration run requires full open-flow flush.",
        ]
        self.report_path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="V1.5 pressure-only sealed tuning harness for 900 hPa.")
    parser.add_argument(
        "--config",
        required=True,
        help="No-write site config. HGEN/analyzers are ignored by this tool.",
    )
    parser.add_argument("--target-hpa", type=float, default=900.0)
    parser.add_argument("--max-trials", type=int, default=6)
    parser.add_argument("--no-write", action="store_true", default=True)
    parser.add_argument("--confirm-pressure-only-tuning", action="store_true", default=False)
    parser.add_argument("--output-dir", default="")
    args = parser.parse_args(argv)
    cfg_path = Path(args.config)
    cfg = load_config(str(cfg_path))
    output_dir = Path(args.output_dir) if args.output_dir else None
    harness = V15SealedPressureTune900(
        cfg,
        output_dir=output_dir,
        config_path=cfg_path,
        max_trials=args.max_trials,
        target_hpa=args.target_hpa,
        no_write=args.no_write,
        confirm_pressure_only_tuning=args.confirm_pressure_only_tuning,
    )
    code = harness.run(build_devices=True)
    print(f"run_dir={harness.output_dir}", flush=True)
    print(f"summary={harness.json_summary_path}", flush=True)
    return code


if __name__ == "__main__":
    raise SystemExit(main())
