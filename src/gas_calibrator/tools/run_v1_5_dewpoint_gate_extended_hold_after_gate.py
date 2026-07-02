"""V1.5 engineering diagnostic: extended hold inside the CO2 dewpoint gate.

This tool is intentionally outside the normal V1.5 runner entry. It reuses the
existing no-write runner setup, then extends the already-passed dewpoint gate
before the analyzer gate can begin. The route stays open with VENT1 while the
same dewpoint gate reader and cadence continue for a fixed diagnostic hold.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import re
import statistics
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence

from ..config import (
    V1_CO2_ONLY_H2O_NOT_SUPPORTED_MESSAGE,
    load_config,
    require_v1_h2o_zero_span_supported,
    v1_h2o_zero_span_capability,
)
from ..diagnostics import run_self_test
from ..logging_utils import RunLogger
from ..pace_audit import (
    PressureControllerComLockExists,
    is_pace_query,
    is_state_changing_pace_command,
    is_vent1_command,
    normalize_pace_command,
)
from ..workflow import runner as runner_mod
from ..workflow.runner import CalibrationRunner
from . import run_headless
from .v1_5_entrypoint_guards import (
    add_engineering_diagnostic_guard_args,
    require_engineering_diagnostic_guard,
)


DEWPOINT_GATE_EXTENDED_HOLD_REBOUND_OBSERVED = "DEWPOINT_GATE_EXTENDED_HOLD_REBOUND_OBSERVED"
DEWPOINT_GATE_EXTENDED_HOLD_NO_REBOUND = "DEWPOINT_GATE_EXTENDED_HOLD_NO_REBOUND"
FAIL_CLOSED_UNEXPECTED_COMMAND_DURING_DEWPOINT_GATE_EXTENDED_HOLD = (
    "FAIL_CLOSED_UNEXPECTED_COMMAND_DURING_DEWPOINT_GATE_EXTENDED_HOLD"
)
FAIL_CLOSED_DEWPOINT_GATE_EXTENDED_HOLD_SAMPLE_GAP_EXCEEDED = (
    "FAIL_CLOSED_DEWPOINT_GATE_EXTENDED_HOLD_SAMPLE_GAP_EXCEEDED"
)
FAIL_CLOSED_DEWPOINT_GATE_EXTENDED_HOLD_RUNTIME_ERROR = (
    "FAIL_CLOSED_DEWPOINT_GATE_EXTENDED_HOLD_RUNTIME_ERROR"
)

_CALIBRATION_WRITE_PATTERNS = (
    "ID",
    "SENCO",
    "ZERO",
    "SPAN",
    "CALIB",
    "COEFF",
    "COEFFICIENT",
)


def _log(message: str) -> None:
    print(message, flush=True)


def _iso_from_wall(value: Any) -> str:
    try:
        parsed = float(value)
    except Exception:
        return ""
    if parsed <= 0:
        return ""
    return datetime.fromtimestamp(parsed).isoformat(timespec="milliseconds")


def _parse_iso(value: Any) -> Optional[datetime]:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def _iso_to_wall_s(value: Any) -> Optional[float]:
    parsed = _parse_iso(value)
    if parsed is None:
        return None
    try:
        return parsed.timestamp()
    except Exception:
        return None


def _float_or_none(value: Any) -> Optional[float]:
    try:
        return float(value)
    except Exception:
        return None


def _parse_dewpoint_response(response: Any) -> Optional[float]:
    text = str(response or "")
    if "GetCurData_" not in text:
        return None
    match = re.search(r"GetCurData_([-+]?\d+(?:\.\d+)?)_", text)
    if not match:
        return None
    return _float_or_none(match.group(1))


def _read_csv(path: Path) -> List[Dict[str, Any]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def _write_csv(path: Path, rows: Sequence[Mapping[str, Any]], fieldnames: Sequence[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(fieldnames), extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow(dict(row))


def _vent1_gap_stats(events: Sequence[Mapping[str, Any]]) -> Dict[str, Any]:
    wall_values = [
        float(value)
        for value in (_float_or_none(row.get("wall_s")) for row in events)
        if value is not None
    ]
    wall_values.sort()
    gaps = [wall_values[index] - wall_values[index - 1] for index in range(1, len(wall_values))]
    if not gaps:
        return {
            "vent1_gap_min_s": None,
            "vent1_gap_avg_s": None,
            "vent1_gap_max_s": None,
        }
    return {
        "vent1_gap_min_s": min(gaps),
        "vent1_gap_avg_s": statistics.mean(gaps),
        "vent1_gap_max_s": max(gaps),
    }


def _latest_io_path(run_dir: Path) -> Optional[Path]:
    candidates = sorted(run_dir.glob("io_*.csv"), key=lambda path: path.stat().st_mtime)
    return candidates[-1] if candidates else None


def _summarize_values(samples: Sequence[Mapping[str, Any]], *, tail_reference_c: Optional[float]) -> Dict[str, Any]:
    valid = [
        row
        for row in samples
        if bool(row.get("read_ok")) and _float_or_none(row.get("dewpoint_c")) is not None
    ]
    values = [float(_float_or_none(row.get("dewpoint_c"))) for row in valid]
    wall_values = [
        (float(_float_or_none(row.get("sample_wall_ts"))), float(_float_or_none(row.get("dewpoint_c"))))
        for row in valid
        if _float_or_none(row.get("sample_wall_ts")) is not None
    ]
    wall_values.sort(key=lambda item: item[0])
    summary: Dict[str, Any] = {
        "dewpoint_gate_extended_hold_sample_count": len(valid),
        "dewpoint_gate_extended_hold_read_error_count": sum(1 for row in samples if not bool(row.get("read_ok"))),
        "dewpoint_gate_tail_reference_c": tail_reference_c,
    }
    if not wall_values:
        summary.update(
            {
                "dewpoint_gate_extended_hold_first_ts": "",
                "dewpoint_gate_extended_hold_first_value_c": None,
                "dewpoint_gate_extended_hold_last_ts": "",
                "dewpoint_gate_extended_hold_last_value_c": None,
                "dewpoint_gate_extended_hold_min_c": None,
                "dewpoint_gate_extended_hold_max_c": None,
                "dewpoint_gate_extended_hold_mean_c": None,
                "dewpoint_gate_extended_hold_median_c": None,
                "dewpoint_gate_extended_hold_slope_c_per_min": None,
                "dewpoint_gate_extended_hold_trend": "unavailable",
                "dewpoint_gate_extended_hold_max_gap_s": None,
                "dewpoint_gate_extended_hold_delta_vs_tail_reference_c": None,
                "dewpoint_gate_extended_hold_first_rebound_ts": "",
                "dewpoint_gate_extended_hold_first_rebound_value_c": None,
                "dewpoint_gate_extended_hold_rebound_exceeded": False,
            }
        )
        return summary

    first_ts, first_value = wall_values[0]
    last_ts, last_value = wall_values[-1]
    gaps = [max(0.0, curr[0] - prev[0]) for prev, curr in zip(wall_values, wall_values[1:])]
    duration_s = max(0.0, last_ts - first_ts)
    slope_c_per_min = ((last_value - first_value) / duration_s * 60.0) if duration_s > 0 else 0.0
    change = last_value - first_value
    if len(wall_values) == 1:
        trend = "single_sample"
    elif change > 0.05:
        trend = "rising"
    elif change < -0.05:
        trend = "falling"
    else:
        trend = "stable"

    first_rebound_ts = ""
    first_rebound_value_c = None
    rebound_exceeded = False
    if tail_reference_c is not None:
        for sample_ts, value_c in wall_values:
            if value_c - float(tail_reference_c) > 0.20:
                first_rebound_ts = _iso_from_wall(sample_ts)
                first_rebound_value_c = value_c
                rebound_exceeded = True
                break

    delta_vs_tail = None
    if tail_reference_c is not None:
        delta_vs_tail = last_value - float(tail_reference_c)

    summary.update(
        {
            "dewpoint_gate_extended_hold_first_ts": _iso_from_wall(first_ts),
            "dewpoint_gate_extended_hold_first_value_c": first_value,
            "dewpoint_gate_extended_hold_last_ts": _iso_from_wall(last_ts),
            "dewpoint_gate_extended_hold_last_value_c": last_value,
            "dewpoint_gate_extended_hold_min_c": min(values),
            "dewpoint_gate_extended_hold_max_c": max(values),
            "dewpoint_gate_extended_hold_mean_c": statistics.mean(values),
            "dewpoint_gate_extended_hold_median_c": statistics.median(values),
            "dewpoint_gate_extended_hold_slope_c_per_min": slope_c_per_min,
            "dewpoint_gate_extended_hold_trend": trend,
            "dewpoint_gate_extended_hold_max_gap_s": max(gaps) if gaps else 0.0,
            "dewpoint_gate_extended_hold_delta_vs_tail_reference_c": delta_vs_tail,
            "dewpoint_gate_extended_hold_first_rebound_ts": first_rebound_ts,
            "dewpoint_gate_extended_hold_first_rebound_value_c": first_rebound_value_c,
            "dewpoint_gate_extended_hold_rebound_exceeded": rebound_exceeded,
        }
    )
    return summary


def _count_io_between(io_path: Optional[Path], begin_ts: str, end_ts: str) -> Dict[str, Any]:
    rows = _read_csv(io_path) if io_path else []
    analyzer_tx = 0
    analyzer_rx = 0
    relay_write = 0
    analyzer_disabled = 0
    analyzer_timeout_reject = 0
    calibration_tx = 0
    for row in rows:
        timestamp = str(row.get("timestamp") or "")
        if timestamp < begin_ts or timestamp > end_ts:
            continue
        device = str(row.get("device") or "")
        direction = str(row.get("direction") or "").upper()
        command = str(row.get("command") or "")
        response = str(row.get("response") or "")
        if device == "gas_analyzer" and direction == "TX":
            analyzer_tx += 1
        if device == "gas_analyzer" and direction == "RX" and response and response != "<flush_input>":
            analyzer_rx += 1
        if device == "relay_controller" and direction == "TX" and command.startswith("write_"):
            relay_write += 1
        if device == "runner" and command == "analyzers-disabled":
            analyzer_disabled += 1
        if device == "runner" and command in {"sensor-read-reject", "sensor-read-reject-summary"}:
            analyzer_timeout_reject += 1
        if direction == "TX" and any(pattern in command.upper() for pattern in _CALIBRATION_WRITE_PATTERNS):
            calibration_tx += 1
    return {
        "analyzer_tx_count": analyzer_tx,
        "analyzer_rx_count": analyzer_rx,
        "analyzer_active_list_changes": analyzer_disabled,
        "analyzers_disabled_count": analyzer_disabled,
        "analyzer_timeout_reject_count": analyzer_timeout_reject,
        "relay_write_count": relay_write,
        "valve_change_count": relay_write,
        "calibration_command_tx_count": calibration_tx,
    }


def _is_unexpected_pace_command(command: str) -> bool:
    normalized = command.strip().upper()
    if "?" in normalized:
        return False
    if "VENT 1" in normalized and "VENT?" not in normalized:
        return False
    return bool(normalized)


def _nearest_count(rows: Sequence[Mapping[str, Any]], center: datetime, seconds: float, predicate) -> int:
    begin = center - timedelta(seconds=seconds)
    end = center + timedelta(seconds=seconds)
    count = 0
    for row in rows:
        timestamp = _parse_iso(row.get("timestamp") or row.get("wall_ts"))
        if timestamp is None or timestamp < begin or timestamp > end:
            continue
        if predicate(row):
            count += 1
    return count


def write_analyzer_gate_dewpoint_vs_analyzer_events(run_dir: Path) -> Path:
    """Write a compact offline audit CSV for an existing run directory."""
    run_dir = Path(run_dir)
    trace_rows = _read_csv(run_dir / "pressure_transition_trace.csv")
    io_path = _latest_io_path(run_dir)
    io_rows = _read_csv(io_path) if io_path else []
    raw_rows = _read_csv(run_dir / "pace_raw_serial_tap.csv")

    analyzer_end = next(
        (row for row in trace_rows if row.get("trace_stage") == "co2_precondition_analyzer_gate_end"),
        {},
    )
    begin_ts = _parse_iso(analyzer_end.get("analyzer_gate_begin_ts"))
    end_ts = _parse_iso(analyzer_end.get("analyzer_gate_end_ts"))
    tail_reference = _float_or_none(analyzer_end.get("dewpoint_gate_tail_reference_c"))
    if tail_reference is None:
        gate_end = next(
            (row for row in trace_rows if row.get("trace_stage") == "co2_precondition_dewpoint_gate_end"),
            {},
        )
        tail_reference = _float_or_none(gate_end.get("dewpoint_gate_tail_reference_c"))
    if begin_ts is None or end_ts is None:
        path = run_dir / "analyzer_gate_dewpoint_vs_analyzer_events.csv"
        _write_csv(
            path,
            [],
            [
                "ts",
                "event_type",
                "dewpoint_c",
                "delta_vs_tail_reference_c",
                "analyzer_rx_count_nearby",
                "analyzer_reject_event_nearby",
                "pace_vent1_gap_nearby",
                "pace_unexpected_write_nearby",
                "valve_write_nearby",
                "notes",
            ],
        )
        return path

    dewpoint_samples: List[Dict[str, Any]] = []
    pending_tx: Optional[datetime] = None
    for row in io_rows:
        if row.get("device") != "dewpoint_meter":
            continue
        timestamp = _parse_iso(row.get("timestamp"))
        if timestamp is None:
            continue
        if row.get("direction") == "TX":
            pending_tx = timestamp
            continue
        value = _parse_dewpoint_response(row.get("response"))
        if value is None or timestamp < begin_ts or timestamp > end_ts:
            continue
        delta = value - tail_reference if tail_reference is not None else None
        analyzer_rx_near = _nearest_count(
            io_rows,
            timestamp,
            1.0,
            lambda item: item.get("device") == "gas_analyzer"
            and item.get("direction") == "RX"
            and bool(item.get("response"))
            and item.get("response") != "<flush_input>",
        )
        reject_near = _nearest_count(
            io_rows,
            timestamp,
            5.0,
            lambda item: item.get("device") == "runner"
            and item.get("command") in {"sensor-read-reject", "sensor-read-reject-summary"},
        )
        valve_near = _nearest_count(
            io_rows,
            timestamp,
            1.0,
            lambda item: item.get("device") == "relay_controller"
            and item.get("direction") == "TX"
            and str(item.get("command") or "").startswith("write_"),
        )
        raw_near = [
            item
            for item in raw_rows
            if (raw_ts := _parse_iso(item.get("wall_ts"))) is not None
            and timestamp - timedelta(seconds=1.0) <= raw_ts <= timestamp + timedelta(seconds=1.0)
            and item.get("direction") == "WRITE"
        ]
        unexpected_near = sum(
            1
            for item in raw_near
            if _is_unexpected_pace_command(str(item.get("decoded_command") or ""))
        )
        vent_times = [
            _parse_iso(item.get("wall_ts"))
            for item in raw_rows
            if item.get("direction") == "WRITE"
            and "VENT 1" in str(item.get("decoded_command") or "").upper()
            and _parse_iso(item.get("wall_ts")) is not None
            and _parse_iso(item.get("wall_ts")) <= timestamp
        ]
        vent_gap = None
        if vent_times:
            vent_gap = max(0.0, (timestamp - max(vent_times)).total_seconds())
        dewpoint_samples.append(
            {
                "ts": timestamp.isoformat(timespec="milliseconds"),
                "event_type": "dewpoint_sample",
                "dewpoint_c": value,
                "delta_vs_tail_reference_c": delta,
                "analyzer_rx_count_nearby": analyzer_rx_near,
                "analyzer_reject_event_nearby": reject_near,
                "pace_vent1_gap_nearby": vent_gap,
                "pace_unexpected_write_nearby": unexpected_near,
                "valve_write_nearby": valve_near,
                "notes": "pending_tx=" + (pending_tx.isoformat(timespec="milliseconds") if pending_tx else ""),
            }
        )

    path = run_dir / "analyzer_gate_dewpoint_vs_analyzer_events.csv"
    _write_csv(
        path,
        dewpoint_samples,
        [
            "ts",
            "event_type",
            "dewpoint_c",
            "delta_vs_tail_reference_c",
            "analyzer_rx_count_nearby",
            "analyzer_reject_event_nearby",
            "pace_vent1_gap_nearby",
            "pace_unexpected_write_nearby",
            "valve_write_nearby",
            "notes",
        ],
    )
    return path


class DewpointGateExtendedHoldDiagnosticRunner(CalibrationRunner):
    """Runner subclass used only by the standalone diagnostic tool."""

    def _dewpoint_gate_extended_hold_duration_s(self) -> float:
        return max(
            0.01,
            float(self._wf("workflow.diagnostics.dewpoint_gate_extended_hold.duration_s", 180.0) or 180.0),
        )

    def _dewpoint_gate_extended_hold_sample_interval_s(self) -> float:
        return max(0.2, float(self._gas_route_dewpoint_gate_cfg().get("poll_s") or 2.0))

    def _dewpoint_gate_extended_hold_max_gap_s(self) -> float:
        return max(
            self._dewpoint_gate_extended_hold_sample_interval_s(),
            float(
                self._wf(
                    "workflow.diagnostics.dewpoint_gate_extended_hold.max_gap_s",
                    self._wf("workflow.stability.analyzer_gate_dewpoint_monitor_max_gap_s", 15.0),
                )
                or 15.0
            ),
        )

    def _controlled_exit_failure_is_route_terminal(self) -> bool:
        decision = str(getattr(self, "_controlled_exit_final_decision", "") or "").strip()
        if decision in {
            FAIL_CLOSED_UNEXPECTED_COMMAND_DURING_DEWPOINT_GATE_EXTENDED_HOLD,
            FAIL_CLOSED_DEWPOINT_GATE_EXTENDED_HOLD_SAMPLE_GAP_EXCEEDED,
            FAIL_CLOSED_DEWPOINT_GATE_EXTENDED_HOLD_RUNTIME_ERROR,
        }:
            return True
        return super()._controlled_exit_failure_is_route_terminal()

    def _wait_co2_route_dewpoint_gate_before_seal(self, point, **kwargs) -> bool:  # type: ignore[override]
        gate_passed = super()._wait_co2_route_dewpoint_gate_before_seal(point, **kwargs)
        if not gate_passed:
            return False
        self.log(
            "Dewpoint gate extended hold diagnostic active: remain in dewpoint gate; "
            "skip analyzer stability wait"
        )
        return self._run_dewpoint_gate_extended_hold_after_gate(point)

    def _hold_timeline_path(self) -> Path:
        run_dir = Path(getattr(self.logger, "run_dir", Path("logs")))
        return run_dir / "dewpoint_gate_extended_hold_timeline.csv"

    def _hold_summary_path(self) -> Path:
        run_dir = Path(getattr(self.logger, "run_dir", Path("logs")))
        return run_dir / "dewpoint_gate_extended_hold_summary.json"

    def _hold_rebound_alignment_path(self) -> Path:
        run_dir = Path(getattr(self.logger, "run_dir", Path("logs")))
        return run_dir / "dewpoint_gate_extended_hold_first_rebound_alignment.csv"

    def _read_pace_int_status(self, pace: Any, method_name: str) -> Any:
        getter = getattr(pace, method_name, None)
        if not callable(getter):
            return ""
        try:
            return getter()
        except Exception as exc:
            return f"error:{exc}"

    def _read_dewpoint_gate_extended_hold_sample(
        self,
        *,
        hold_begin_wall_s: float,
        tail_reference_c: Optional[float],
    ) -> Dict[str, Any]:
        sample_wall_ts = time.time()
        row: Dict[str, Any] = {
            "ts": _iso_from_wall(sample_wall_ts),
            "sample_wall_ts": sample_wall_ts,
            "elapsed_since_hold_begin_s": max(0.0, sample_wall_ts - hold_begin_wall_s),
            "dewpoint_c": None,
            "delta_vs_tail_reference_c": None,
            "read_ok": False,
            "error": "",
            "nearest_vent1_age_s": None,
            "nearest_pace_pressure_hpa": None,
            "nearest_com22_pressure_hpa": None,
            "nearest_vent_status": "",
            "nearest_outp_state": "",
            "nearest_isol_state": "",
        }
        try:
            snapshot = self._read_precondition_dewpoint_gate_snapshot()
            value_c = self._as_float(snapshot.get("dewpoint_c"))
            row["dewpoint_c"] = value_c
            row["read_ok"] = value_c is not None
            if value_c is not None and tail_reference_c is not None:
                row["delta_vs_tail_reference_c"] = float(value_c) - float(tail_reference_c)
        except Exception as exc:
            row["error"] = str(exc) or "dewpoint_read_failed"
        pace = self.devices.get("pace")
        try:
            freshness = self._route_open_vent1_freshness_evidence(
                pace,
                now_wall_s=time.time(),
                now_monotonic_s=time.monotonic(),
            )
            row["nearest_vent1_age_s"] = self._as_float(freshness.get("vent1_last_refresh_age_s"))
        except Exception:
            row["nearest_vent1_age_s"] = None
        try:
            row["nearest_pace_pressure_hpa"] = self._read_pace_pressure_now(pace) if pace is not None else None
        except Exception:
            row["nearest_pace_pressure_hpa"] = None
        try:
            row["nearest_com22_pressure_hpa"] = self._read_com22_pressure_now()
        except Exception:
            row["nearest_com22_pressure_hpa"] = None
        try:
            row["nearest_vent_status"] = self._read_pace_int_status(pace, "get_vent_status") if pace is not None else ""
        except Exception:
            row["nearest_vent_status"] = ""
        try:
            row["nearest_outp_state"] = self._read_pace_int_status(pace, "get_output_state") if pace is not None else ""
        except Exception:
            row["nearest_outp_state"] = ""
        try:
            row["nearest_isol_state"] = self._read_pace_int_status(pace, "get_isolation_state") if pace is not None else ""
        except Exception:
            row["nearest_isol_state"] = ""
        return row

    def _pace_raw_tap_rows_between(self, begin_wall_s: float, end_wall_s: float) -> List[Dict[str, Any]]:
        path = getattr(self.logger, "raw_serial_tap_csv_path", None)
        if path is None:
            return []
        raw_path = Path(path)
        raw_file = getattr(self.logger, "_raw_tap_file", None)
        try:
            if raw_file is not None:
                raw_file.flush()
        except Exception:
            pass
        if not raw_path.exists():
            return []
        rows: List[Dict[str, Any]] = []
        try:
            with raw_path.open("r", encoding="utf-8-sig", newline="") as handle:
                for row in csv.DictReader(handle):
                    wall_s = _iso_to_wall_s(row.get("wall_ts"))
                    if wall_s is None or wall_s < begin_wall_s or wall_s > end_wall_s:
                        continue
                    decoded = normalize_pace_command(
                        row.get("decoded_command") or row.get("raw_text_decoded") or ""
                    )
                    event = dict(row)
                    event["wall_s"] = wall_s
                    event["decoded_command"] = decoded
                    event["is_vent1"] = is_vent1_command(decoded)
                    event["is_query"] = is_pace_query(decoded)
                    event["is_unexpected_state_changing"] = (
                        str(row.get("direction") or "").strip().upper() == "WRITE"
                        and is_state_changing_pace_command(decoded)
                        and not is_vent1_command(decoded)
                    )
                    rows.append(event)
        except Exception:
            return []
        rows.sort(key=lambda item: float(item.get("wall_s") or 0.0))
        return rows

    def _enrich_hold_timeline_rows(
        self,
        rows: Sequence[Mapping[str, Any]],
        *,
        begin_wall_s: float,
        end_wall_s: float,
    ) -> List[Dict[str, Any]]:
        raw_rows = self._pace_raw_tap_rows_between(begin_wall_s, end_wall_s)
        vent1_rows = [
            row
            for row in raw_rows
            if str(row.get("direction") or "").strip().upper() == "WRITE" and bool(row.get("is_vent1"))
        ]
        unexpected_rows = [row for row in raw_rows if bool(row.get("is_unexpected_state_changing"))]
        enriched: List[Dict[str, Any]] = []
        for source_row in rows:
            row = dict(source_row)
            sample_wall_s = _float_or_none(row.get("sample_wall_ts"))
            if sample_wall_s is None:
                sample_wall_s = _iso_to_wall_s(row.get("ts"))
            previous = [event for event in vent1_rows if float(event.get("wall_s") or 0.0) <= float(sample_wall_s or 0.0)]
            upcoming = [event for event in vent1_rows if float(event.get("wall_s") or 0.0) >= float(sample_wall_s or 0.0)]
            prev_event = previous[-1] if previous else None
            next_event = upcoming[0] if upcoming else None
            prev_wall_s = _float_or_none(prev_event.get("wall_s")) if prev_event else None
            next_wall_s = _float_or_none(next_event.get("wall_s")) if next_event else None
            row["nearest_prev_vent1_ts"] = prev_event.get("wall_ts", "") if prev_event else ""
            row["age_since_prev_vent1_s"] = (
                max(0.0, float(sample_wall_s) - float(prev_wall_s))
                if sample_wall_s is not None and prev_wall_s is not None
                else None
            )
            row["nearest_next_vent1_ts"] = next_event.get("wall_ts", "") if next_event else ""
            row["time_to_next_vent1_s"] = (
                max(0.0, float(next_wall_s) - float(sample_wall_s))
                if sample_wall_s is not None and next_wall_s is not None
                else None
            )
            row["vent1_gap_s"] = (
                max(0.0, float(next_wall_s) - float(prev_wall_s))
                if prev_wall_s is not None and next_wall_s is not None
                else None
            )
            nearby_window_s = max(2.0, self._dewpoint_gate_extended_hold_sample_interval_s())
            if sample_wall_s is None:
                nearby_unexpected = []
            else:
                nearby_unexpected = [
                    event
                    for event in unexpected_rows
                    if abs(float(event.get("wall_s") or 0.0) - float(sample_wall_s)) <= nearby_window_s
                ]
            row["raw_tap_unexpected_write_nearby"] = len(nearby_unexpected)
            notes: List[str] = []
            if nearby_unexpected:
                notes.append(f"unexpected_pace_write={nearby_unexpected[0].get('decoded_command', '')}")
            if row.get("nearest_vent1_age_s") in {None, ""} and row.get("age_since_prev_vent1_s") not in {None, ""}:
                row["nearest_vent1_age_s"] = row.get("age_since_prev_vent1_s")
            row["notes"] = "; ".join(notes)
            enriched.append(row)
        return enriched

    def _write_rebound_alignment_csv(self, rows: Sequence[Mapping[str, Any]], first_rebound_ts: Any) -> str:
        path = self._hold_rebound_alignment_path()
        center_s = _iso_to_wall_s(first_rebound_ts)
        if center_s is None:
            selected: Sequence[Mapping[str, Any]] = []
        else:
            selected = [
                row
                for row in rows
                if (_iso_to_wall_s(row.get("ts")) is not None)
                and abs(float(_iso_to_wall_s(row.get("ts"))) - center_s) <= 30.0
            ]
        self._write_hold_timeline(selected, path_override=path)
        return str(path)

    def _write_hold_timeline(
        self,
        rows: Sequence[Mapping[str, Any]],
        *,
        path_override: Optional[Path] = None,
    ) -> str:
        path = path_override or self._hold_timeline_path()
        _write_csv(
            path,
            rows,
            [
                "ts",
                "dewpoint_c",
                "delta_vs_tail_reference_c",
                "nearest_prev_vent1_ts",
                "age_since_prev_vent1_s",
                "nearest_next_vent1_ts",
                "time_to_next_vent1_s",
                "vent1_gap_s",
                "read_ok",
                "error",
                "elapsed_since_hold_begin_s",
                "nearest_vent1_age_s",
                "nearest_pace_pressure_hpa",
                "nearest_com22_pressure_hpa",
                "nearest_vent_status",
                "nearest_outp_state",
                "nearest_isol_state",
                "raw_tap_unexpected_write_nearby",
                "notes",
            ],
        )
        return str(path)

    def _build_hold_summary(
        self,
        point,
        *,
        begin_wall_s: float,
        end_wall_s: float,
        rows: Sequence[Mapping[str, Any]],
        tail_reference_c: Optional[float],
        runtime_error: str = "",
        previous_stage: str = "co2_precondition_dewpoint_gate",
    ) -> Dict[str, Any]:
        begin_ts = _iso_from_wall(begin_wall_s)
        end_ts = _iso_from_wall(end_wall_s)
        enriched_rows = self._enrich_hold_timeline_rows(
            rows,
            begin_wall_s=begin_wall_s,
            end_wall_s=end_wall_s,
        )
        timeline_csv = self._write_hold_timeline(enriched_rows)
        summary = {
            "dewpoint_gate_extended_hold_enabled": True,
            "dewpoint_gate_extended_hold_begin_ts": begin_ts,
            "dewpoint_gate_extended_hold_end_ts": end_ts,
            "dewpoint_gate_extended_hold_duration_s": max(0.0, end_wall_s - begin_wall_s),
            "dewpoint_gate_extended_hold_sample_interval_s": self._dewpoint_gate_extended_hold_sample_interval_s(),
            "dewpoint_gate_extended_hold_timeline_csv": timeline_csv,
            "dewpoint_gate_extended_hold_same_reader": True,
            "dewpoint_gate_extended_hold_reader_function": "_read_precondition_dewpoint_gate_snapshot",
            "dewpoint_gate_extended_hold_same_sampling_interval": True,
            "dewpoint_gate_extended_hold_previous_stage": previous_stage or "co2_precondition_dewpoint_gate",
            "dewpoint_gate_extended_hold_stage": "co2_precondition_dewpoint_gate_extended_hold",
            "analyzer_gate_entered": False,
            "wait_primary_sensor_stable_called": False,
            "analyzer_rx_stability_loop_started": False,
            "dewpoint_gate_tail_reference_c": tail_reference_c,
            "runtime_error": runtime_error,
            "point_row": getattr(point, "index", ""),
            "pressure_target_hpa": getattr(point, "target_pressure_hpa", ""),
        }
        summary.update(_summarize_values(enriched_rows, tail_reference_c=tail_reference_c))
        max_gap_s = self._as_float(summary.get("dewpoint_gate_extended_hold_max_gap_s"))
        raw_summary_fn = getattr(self.logger, "summarize_pace_raw_tap_window", None)
        raw_summary = dict(raw_summary_fn(begin_ts, end_ts) or {}) if callable(raw_summary_fn) else {}
        summary.update(raw_summary)
        raw_rows = self._pace_raw_tap_rows_between(begin_wall_s, end_wall_s)
        vent1_events = [
            row
            for row in raw_rows
            if str(row.get("direction") or "").strip().upper() == "WRITE" and bool(row.get("is_vent1"))
        ]
        summary.update(_vent1_gap_stats(vent1_events))
        io_summary = _count_io_between(
            Path(getattr(self.logger, "io_path", "")) if getattr(self.logger, "io_path", None) else None,
            begin_ts,
            end_ts,
        )
        summary.update(io_summary)
        summary["analyzer_timeout_count"] = summary.get("analyzer_timeout_reject_count", 0)
        try:
            summary["actual_open_valves"] = ",".join(str(value) for value in self._cached_actual_open_valves())
        except Exception:
            summary["actual_open_valves"] = ""
        summary["dewpoint_gate_extended_hold_first_rebound_alignment_csv"] = self._write_rebound_alignment_csv(
            enriched_rows,
            summary.get("dewpoint_gate_extended_hold_first_rebound_ts"),
        )

        if runtime_error:
            decision = FAIL_CLOSED_DEWPOINT_GATE_EXTENDED_HOLD_RUNTIME_ERROR
        elif int(summary.get("unexpected_state_changing_write_count") or 0) > 0:
            decision = FAIL_CLOSED_UNEXPECTED_COMMAND_DURING_DEWPOINT_GATE_EXTENDED_HOLD
        elif max_gap_s is not None and max_gap_s > self._dewpoint_gate_extended_hold_max_gap_s():
            decision = FAIL_CLOSED_DEWPOINT_GATE_EXTENDED_HOLD_SAMPLE_GAP_EXCEEDED
        elif bool(summary.get("dewpoint_gate_extended_hold_rebound_exceeded")):
            decision = DEWPOINT_GATE_EXTENDED_HOLD_REBOUND_OBSERVED
        else:
            decision = DEWPOINT_GATE_EXTENDED_HOLD_NO_REBOUND
        summary["final_decision"] = decision
        summary["vent0_count"] = summary.get("vent0_count", 0)
        summary["outp0_count"] = summary.get("outp0_count", 0)
        summary["outp1_count"] = summary.get("outp1_count", 0)
        summary["isol_command_count"] = summary.get("isol_command_count", 0)
        summary["setpoint_sour_pres_count"] = summary.get("setpoint_sour_pres_count", 0)
        return summary

    def _persist_hold_summary(self, summary: Mapping[str, Any]) -> str:
        path = self._hold_summary_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(dict(summary), ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        return str(path)

    def _run_dewpoint_gate_extended_hold_after_gate(self, point) -> bool:
        runtime_state = dict(self._point_runtime_state(point, phase="co2") or {})
        tail_reference_c = self._as_float(runtime_state.get("dewpoint_gate_tail_reference_c"))
        duration_s = self._dewpoint_gate_extended_hold_duration_s()
        interval_s = self._dewpoint_gate_extended_hold_sample_interval_s()
        begin_wall_s = time.time()
        previous_stage = "co2_precondition_dewpoint_gate"
        previous_logger_stage = self._set_logger_workflow_stage("co2_precondition_dewpoint_gate_extended_hold")
        rows: List[Dict[str, Any]] = []
        runtime_error = ""
        try:
            while not self.stop_event.is_set() and (time.time() - begin_wall_s) < duration_s:
                loop_started = time.time()
                self._check_pause()
                rows.append(
                    self._read_dewpoint_gate_extended_hold_sample(
                        hold_begin_wall_s=begin_wall_s,
                        tail_reference_c=tail_reference_c,
                    )
                )
                elapsed = time.time() - loop_started
                remaining_s = max(0.0, duration_s - (time.time() - begin_wall_s))
                sleep_s = min(max(0.0, interval_s - elapsed), remaining_s)
                if sleep_s > 0:
                    time.sleep(sleep_s)
        except Exception as exc:
            runtime_error = str(exc) or "dewpoint_gate_extended_hold_runtime_error"
        finally:
            end_wall_s = time.time()
            self._restore_logger_workflow_stage(previous_logger_stage)

        summary = self._build_hold_summary(
            point,
            begin_wall_s=begin_wall_s,
            end_wall_s=end_wall_s,
            rows=rows,
            tail_reference_c=tail_reference_c,
            runtime_error=runtime_error,
            previous_stage=previous_stage,
        )
        summary_path = self._persist_hold_summary(summary)
        self._set_point_runtime_fields(point, phase="co2", **summary)
        self._append_pressure_trace_row(
            point=point,
            route="co2",
            point_phase="co2",
            trace_stage="co2_precondition_dewpoint_gate_extended_hold",
            pressure_target_hpa=getattr(point, "target_pressure_hpa", None),
            refresh_pace_state=False,
            extra_fields={
                "final_decision": summary["final_decision"],
                "dewpoint_preseal_decision": summary["final_decision"],
            },
            note=f"summary={summary_path} decision={summary['final_decision']}",
        )
        self._mark_co2_route_terminal_failure(
            final_decision=str(summary["final_decision"]),
            reason="dewpoint gate extended hold diagnostic completed before analyzer gate",
            point=point,
            phase="co2",
        )
        self.log(
            "Dewpoint gate extended hold diagnostic completed: "
            f"decision={summary['final_decision']} summary={summary_path}"
        )
        return False


def _parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run V1.5 dewpoint gate extended hold diagnostic after CO2 gate.")
    parser.add_argument("--config", default="configs/default_config.json", help="V1.5 config path.")
    parser.add_argument("--run-id", default=None, help="Optional run id under logs/.")
    parser.add_argument("--temp", type=float, default=None, help="Only run points matching this chamber temperature.")
    parser.add_argument("--skip-connect-check", action="store_true", help="Skip startup connectivity self-test.")
    parser.add_argument("--skip-h2o", action="store_true", help="Force skip_h2o=True.")
    parser.add_argument("--hold-duration-s", type=float, default=None, help="Override dewpoint gate extended hold duration.")
    parser.add_argument("--sample-interval-s", type=float, default=None, help="Override dewpoint-only sample interval.")
    parser.add_argument("--audit-run-dir", default=None, help="Only write offline analyzer/dewpoint event CSV for a run dir.")
    add_engineering_diagnostic_guard_args(parser)
    args = parser.parse_args(list(argv) if argv is not None else None)
    if not args.audit_run_dir:
        require_engineering_diagnostic_guard(args, parser, context="dewpoint gate extended-hold diagnostic")
    return args


def _apply_cli_overrides(cfg: Dict[str, Any], args: argparse.Namespace) -> None:
    diagnostics = cfg.setdefault("workflow", {}).setdefault("diagnostics", {}).setdefault(
        "dewpoint_gate_extended_hold",
        {},
    )
    diagnostics["enabled"] = True
    if args.hold_duration_s is not None:
        diagnostics["duration_s"] = float(args.hold_duration_s)
    if args.sample_interval_s is not None:
        cfg.setdefault("workflow", {}).setdefault("stability", {})[
            "gas_route_dewpoint_gate_poll_s"
        ] = float(args.sample_interval_s)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = _parse_args(argv)
    if args.audit_run_dir:
        path = write_analyzer_gate_dewpoint_vs_analyzer_events(Path(args.audit_run_dir))
        _log(f"Analyzer/dewpoint event audit CSV: {path}")
        return 0

    cfg = load_config(args.config)
    cfg["_runtime_config_path"] = str(args.config)
    _apply_cli_overrides(cfg, args)
    coeff_cfg = cfg.get("coefficients", {}) if isinstance(cfg.get("coefficients", {}), dict) else {}
    capability = v1_h2o_zero_span_capability(coeff_cfg)
    _log(
        "Capability boundary: "
        f"H2O zero/span status={capability['status']} note={capability['note']}"
    )
    try:
        require_v1_h2o_zero_span_supported(coeff_cfg, context="dewpoint_gate_extended_hold_diagnostic")
    except RuntimeError as exc:
        _log(str(exc))
        _log(V1_CO2_ONLY_H2O_NOT_SUPPORTED_MESSAGE)
        return 2

    logger = RunLogger(Path(cfg["paths"]["output_dir"]), run_id=args.run_id, cfg=cfg)
    _log(f"Run folder: {logger.run_dir}")
    devices: Dict[str, Any] = {}
    patched_loader = False
    original_loader = runner_mod.load_points_from_excel
    try:
        if args.skip_h2o:
            cfg.setdefault("workflow", {})["skip_h2o"] = True
            _log("Workflow override: skip_h2o=True")

        if args.temp is not None:
            target = float(args.temp)

            def _filtered_loader(path: str, missing_pressure_policy: str = "require", **kwargs):
                points = original_loader(path, missing_pressure_policy=missing_pressure_policy, **kwargs)
                filtered = [
                    point
                    for point in points
                    if point.temp_chamber_c is not None and math.isclose(float(point.temp_chamber_c), target)
                ]
                _log(f"Point filter: temp={target:g}C -> {len(filtered)}/{len(points)} points")
                return filtered

            runner_mod.load_points_from_excel = _filtered_loader
            patched_loader = True

        if not args.skip_connect_check and cfg.get("workflow", {}).get("startup_connect_check", {}).get("enabled", False):
            _log("Connectivity check...")
            results = run_self_test(cfg, log_fn=_log, io_logger=logger)
            failures = run_headless._enabled_failures(cfg, results)
            if failures:
                _log("Connectivity check failed:")
                for name, err in failures:
                    _log(f"- {name}: {err}")
                return 2

        devices = run_headless._build_devices(cfg, io_logger=logger)
        runner = DewpointGateExtendedHoldDiagnosticRunner(cfg, devices, logger, _log, _log)
        runner.run()
        return 0
    except PressureControllerComLockExists as exc:
        decision = "BLOCKED_PRESSURE_CONTROLLER_COM_LOCK_EXISTS"
        _log(f"{decision}: {exc.lock_path}")
        _log(f"Existing lock: {exc.existing}")
        try:
            (logger.run_dir / "precheck_final_decision.json").write_text(
                json.dumps(
                    {
                        "final_decision": decision,
                        "lock_path": str(exc.lock_path),
                        "existing_lock": exc.existing,
                    },
                    ensure_ascii=False,
                    indent=2,
                    sort_keys=True,
                )
                + "\n",
                encoding="utf-8",
            )
        except Exception:
            pass
        return 2
    except Exception as exc:
        _log(f"Dewpoint gate extended hold diagnostic aborted: {exc}")
        return 1
    finally:
        if patched_loader:
            runner_mod.load_points_from_excel = original_loader
        run_headless._close_devices(devices)
        try:
            logger.close()
        except Exception:
            pass
if __name__ == "__main__":
    sys.exit(main())
