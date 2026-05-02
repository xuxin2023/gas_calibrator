"""Analysis/export helpers for metrology-grade seal/pressure qualification diagnostic V2."""

from __future__ import annotations

import csv
import json
import math
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from pathlib import Path
from statistics import mean, median, pstdev
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence

from .dewpoint_flush_gate import (
    DEFAULT_DEWPOINT_GATE_WINDOW_S,
    DEFAULT_DEWPOINT_REBOUND_MIN_RISE_C as SHARED_DEFAULT_DEWPOINT_REBOUND_MIN_RISE_C,
    DEFAULT_DEWPOINT_REBOUND_WINDOW_S as SHARED_DEFAULT_DEWPOINT_REBOUND_WINDOW_S,
    detect_dewpoint_rebound as shared_detect_dewpoint_rebound,
    evaluate_dewpoint_flush_gate,
)


DEFAULT_ANALYZER_ID = "ga02"
DEFAULT_VARIANTS: tuple[str, ...] = ("A", "B", "C")
DEFAULT_LAYER_SEQUENCE: tuple[int, ...] = (1, 2, 3)
DEFAULT_SCREENING_GAS_POINTS_PPM: tuple[int, ...] = (0, 1000)
DEFAULT_FULL_GAS_POINTS_PPM: tuple[int, ...] = (0, 200, 400, 600, 800, 1000)
DEFAULT_REPEATABILITY_GAS_POINTS_PPM: tuple[int, ...] = (0, 600, 1000)
DEFAULT_SCREENING_PRESSURE_POINTS_HPA: tuple[int, ...] = (1100, 800, 500)
DEFAULT_FULL_PRESSURE_POINTS_HPA: tuple[int, ...] = (1100, 1000, 900, 800, 700, 600, 500)
DEFAULT_GAS_SWITCH_DEADTIME_S = 2.0
DEFAULT_MIN_FLUSH_S = 120.0
DEFAULT_SCREENING_FLUSH_S = 180.0
DEFAULT_MAX_FLUSH_S = 300.0
DEFAULT_FLUSH_GATE_WINDOW_S = DEFAULT_DEWPOINT_GATE_WINDOW_S
DEFAULT_PRECONDITION_GAS_PPM = 0
DEFAULT_PRECONDITION_MIN_FLUSH_S = 180.0
DEFAULT_PRECONDITION_MAX_FLUSH_S = 900.0
DEFAULT_PRECONDITION_WINDOW_S = 60.0
DEFAULT_PRESSURE_SETTLE_TIMEOUT_S = 180.0
DEFAULT_STABLE_WINDOW_S = 20.0
DEFAULT_SAMPLE_POLL_S = 1.0
DEFAULT_RESTORE_VENT_OBSERVE_S = 10.0
DEFAULT_SEALED_HOLD_S = 180.0
DEFAULT_STABLE_SAMPLE_COUNT_MIN = 10
DEFAULT_STABLE_SAMPLE_COUNT_TARGET = 20
DEFAULT_DEWPOINT_REBOUND_WINDOW_S = SHARED_DEFAULT_DEWPOINT_REBOUND_WINDOW_S
DEFAULT_DEWPOINT_REBOUND_MIN_RISE_C = SHARED_DEFAULT_DEWPOINT_REBOUND_MIN_RISE_C


@dataclass(frozen=True)
class MetrologyDiagnosticThresholds:
    flush_gate_window_s: float = DEFAULT_FLUSH_GATE_WINDOW_S
    flush_ratio_slope_abs_max_per_s: float = 0.0008
    flush_ratio_span_max: float = 0.01
    flush_dewpoint_slope_abs_max_per_s: float = 0.003
    flush_dewpoint_span_max_c: float = 0.35
    flush_pressure_std_max_hpa: float = 1.5
    flush_pressure_slope_abs_max_hpa_per_s: float = 0.03
    flush_pressure_span_max_hpa: float = 2.5
    flush_pressure_span_warn_hpa: float = 8.0
    flush_ratio_t95_warn_max_s: float = 150.0
    flush_ratio_t95_fail_max_s: float = 220.0
    return_to_zero_warn_delta: float = 0.01
    return_to_zero_fail_delta: float = 0.02
    sealed_hold_pressure_drift_warn_hpa_per_min: float = 0.8
    sealed_hold_pressure_drift_fail_hpa_per_min: float = 1.5
    sealed_hold_ratio_drift_warn_per_min: float = 0.003
    sealed_hold_ratio_drift_fail_per_min: float = 0.006
    sealed_hold_dewpoint_drift_warn_c_per_min: float = 0.08
    sealed_hold_dewpoint_drift_fail_c_per_min: float = 0.15
    point_window_ratio_slope_warn_per_s: float = 0.0008
    point_window_ratio_slope_fail_per_s: float = 0.0015
    point_window_dewpoint_slope_warn_per_s: float = 0.003
    point_window_dewpoint_slope_fail_per_s: float = 0.006
    point_window_pressure_std_warn_hpa: float = 0.5
    point_window_pressure_std_fail_hpa: float = 1.0
    normalized_endpoint_span_retention_warn_min: float = 0.85
    normalized_endpoint_span_retention_fail_min: float = 0.70
    analyzer_pressure_bias_warn_hpa: float = 5.0
    analyzer_pressure_bias_fail_hpa: float = 10.0
    analyzer_pressure_bias_slope_warn_abs: float = 0.02
    analyzer_pressure_bias_slope_fail_abs: float = 0.05
    repeatability_warn_std: float = 0.01
    repeatability_fail_std: float = 0.02
    deadtime_warn_abs_s: float = 0.75
    deadtime_fail_abs_s: float = 1.5
    deadtime_warn_std_s: float = 0.5
    deadtime_fail_std_s: float = 1.0
    stable_sample_count_min: int = DEFAULT_STABLE_SAMPLE_COUNT_MIN
    stable_sample_count_target: int = DEFAULT_STABLE_SAMPLE_COUNT_TARGET


DEFAULT_THRESHOLDS = MetrologyDiagnosticThresholds()


def _safe_float(value: Any) -> Optional[float]:
    if value in (None, ""):
        return None
    try:
        numeric = float(value)
    except Exception:
        return None
    if not math.isfinite(numeric):
        return None
    return numeric


def _safe_int(value: Any) -> Optional[int]:
    numeric = _safe_float(value)
    if numeric is None:
        return None
    try:
        return int(round(numeric))
    except Exception:
        return None


def _safe_mean(values: Sequence[float]) -> Optional[float]:
    return mean(values) if values else None


def _safe_median(values: Sequence[float]) -> Optional[float]:
    return median(values) if values else None


def _safe_std(values: Sequence[float]) -> Optional[float]:
    if not values:
        return None
    if len(values) == 1:
        return 0.0
    return pstdev(values)


def _safe_iso(ts: Optional[datetime]) -> Optional[str]:
    return ts.isoformat(timespec="milliseconds") if ts is not None else None


def _parse_timestamp(value: Any) -> Optional[datetime]:
    text = str(value or "").strip()
    if not text:
        return None
    normalized = text.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(normalized)
    except Exception:
        return None


def _series(rows: Sequence[Mapping[str, Any]], key: str) -> List[float]:
    values: List[float] = []
    for row in rows:
        numeric = _safe_float(row.get(key))
        if numeric is not None:
            values.append(numeric)
    return values


def _coerce_flag_list(values: Iterable[str]) -> List[str]:
    out: List[str] = []
    for item in values:
        text = str(item or "").strip()
        if text and text not in out:
            out.append(text)
    return out


def _ordered_rows(rows: Sequence[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    return sorted(
        [dict(row) for row in rows],
        key=lambda row: _parse_timestamp(row.get("timestamp")) or datetime.min,
    )


def _tail_rows(rows: Sequence[Mapping[str, Any]], window_s: float) -> List[Dict[str, Any]]:
    ordered = _ordered_rows(rows)
    if not ordered:
        return []
    timestamps = [_parse_timestamp(row.get("timestamp")) for row in ordered]
    valid = [item for item in timestamps if item is not None]
    if len(valid) < 2:
        return ordered
    cutoff = valid[-1] - timedelta(seconds=max(0.0, float(window_s)))
    return [
        row
        for row in ordered
        if (_parse_timestamp(row.get("timestamp")) or datetime.min) >= cutoff
    ]


def _duration_seconds(rows: Sequence[Mapping[str, Any]]) -> float:
    timestamps = [_parse_timestamp(row.get("timestamp")) for row in rows]
    timestamps = [item for item in timestamps if item is not None]
    if len(timestamps) >= 2:
        return max(0.0, (timestamps[-1] - timestamps[0]).total_seconds())
    elapsed_values = [_safe_float(row.get("elapsed_s")) for row in rows]
    elapsed_values = [item for item in elapsed_values if item is not None]
    if elapsed_values:
        return max(0.0, max(elapsed_values) - min(elapsed_values))
    return 0.0


def _phase_duration_s(rows: Sequence[Mapping[str, Any]]) -> float:
    values = [_safe_float(row.get("phase_elapsed_s")) for row in rows]
    values = [item for item in values if item is not None]
    if values:
        return max(0.0, max(values))
    return _duration_seconds(rows)


def _linear_regression(xs: Sequence[float], ys: Sequence[float]) -> Dict[str, Optional[float]]:
    if len(xs) != len(ys) or len(xs) < 2:
        return {"slope": None, "intercept": None, "r2": None}
    x_mean = sum(xs) / len(xs)
    y_mean = sum(ys) / len(ys)
    sxx = sum((x - x_mean) ** 2 for x in xs)
    syy = sum((y - y_mean) ** 2 for y in ys)
    sxy = sum((x - x_mean) * (y - y_mean) for x, y in zip(xs, ys))
    if sxx <= 0:
        return {"slope": None, "intercept": y_mean, "r2": None}
    slope = sxy / sxx
    intercept = y_mean - slope * x_mean
    r2 = None
    if syy > 0:
        r2 = max(0.0, min(1.0, (sxy * sxy) / (sxx * syy)))
    return {"slope": slope, "intercept": intercept, "r2": r2}


def _time_value_pairs(rows: Sequence[Mapping[str, Any]], key: str) -> List[tuple[float, float]]:
    ordered = _ordered_rows(rows)
    if not ordered:
        return []
    base_ts = _parse_timestamp(ordered[0].get("timestamp"))
    if base_ts is None:
        return []
    pairs: List[tuple[float, float]] = []
    for row in ordered:
        ts = _parse_timestamp(row.get("timestamp"))
        value = _safe_float(row.get(key))
        if ts is None or value is None:
            continue
        pairs.append((max(0.0, (ts - base_ts).total_seconds()), value))
    return pairs


def _slope_per_s(rows: Sequence[Mapping[str, Any]], key: str) -> Optional[float]:
    pairs = _time_value_pairs(rows, key)
    if len(pairs) < 2:
        return None
    fit = _linear_regression([x for x, _ in pairs], [y for _, y in pairs])
    return fit["slope"]


def _quantile(values: Sequence[float], q: float) -> Optional[float]:
    if not values:
        return None
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    q_clamped = max(0.0, min(1.0, float(q)))
    index = (len(ordered) - 1) * q_clamped
    lower = int(math.floor(index))
    upper = int(math.ceil(index))
    if lower == upper:
        return ordered[lower]
    ratio = index - lower
    return ordered[lower] * (1.0 - ratio) + ordered[upper] * ratio


def _status_low_good(
    value: Optional[float],
    *,
    pass_max: float,
    warn_max: float,
    missing: str = "insufficient_evidence",
) -> str:
    if value is None:
        return missing
    numeric = abs(float(value))
    if numeric <= pass_max:
        return "pass"
    if numeric <= warn_max:
        return "warn"
    return "fail"


def _status_high_good(
    value: Optional[float],
    *,
    pass_min: float,
    warn_min: float,
    missing: str = "insufficient_evidence",
) -> str:
    if value is None:
        return missing
    numeric = float(value)
    if numeric >= pass_min:
        return "pass"
    if numeric >= warn_min:
        return "warn"
    return "fail"


def _merge_statuses(statuses: Iterable[str]) -> str:
    statuses = [str(item or "").strip() for item in statuses if str(item or "").strip()]
    if not statuses:
        return "insufficient_evidence"
    if any(item == "fail" for item in statuses):
        return "fail"
    if any(item == "insufficient_evidence" for item in statuses):
        return "insufficient_evidence"
    if any(item == "warn" for item in statuses):
        return "warn"
    return "pass"


def _preferred_pressure_series(rows: Sequence[Mapping[str, Any]]) -> List[float]:
    out: List[float] = []
    for row in rows:
        for key in ("gauge_pressure_hpa", "controller_pressure_hpa", "analyzer2_pressure_hpa"):
            numeric = _safe_float(row.get(key))
            if numeric is not None:
                out.append(numeric)
                break
    return out


def _raw_sample_count(rows: Sequence[Mapping[str, Any]], key: str) -> int:
    count = 0
    for row in rows:
        if _safe_float(row.get(key)) is not None:
            count += 1
    return count


def _aligned_like_sample_count(rows: Sequence[Mapping[str, Any]]) -> int:
    buckets: set[str] = set()
    for row in rows:
        ts = _parse_timestamp(row.get("timestamp"))
        if ts is None:
            continue
        buckets.add(ts.replace(microsecond=0).isoformat(timespec="seconds"))
    return len(buckets)


def _pressure_spike_stats(values: Sequence[float]) -> Dict[str, Optional[float]]:
    if not values:
        return {
            "flush_pressure_median": None,
            "flush_pressure_p95": None,
            "flush_pressure_p99": None,
            "flush_spike_count": None,
            "flush_spike_max": None,
            "flush_spike_duration_s": None,
        }
    med = _safe_median(values)
    p95 = _quantile(values, 0.95)
    p99 = _quantile(values, 0.99)
    spike_threshold = None
    if med is not None and p99 is not None:
        spike_threshold = med + max(1.0, p99 - med)
    spike_count = None
    spike_max = None
    spike_duration = None
    if spike_threshold is not None:
        spikes = [value for value in values if value >= spike_threshold]
        spike_count = len(spikes)
        spike_max = max(spikes) if spikes else None
        spike_duration = float(spike_count) if spike_count is not None else None
    return {
        "flush_pressure_median": med,
        "flush_pressure_p95": p95,
        "flush_pressure_p99": p99,
        "flush_spike_count": spike_count,
        "flush_spike_max": spike_max,
        "flush_spike_duration_s": spike_duration,
    }


def _response_txx_s(rows: Sequence[Mapping[str, Any]], *, key: str, fraction: float) -> Optional[float]:
    ordered = _ordered_rows(rows)
    if len(ordered) < 2:
        return None
    values = [_safe_float(row.get(key)) for row in ordered]
    values = [item for item in values if item is not None]
    if len(values) < 2:
        return None
    first = values[0]
    last = _safe_mean(values[-min(len(values), 5) :])
    if first is None or last is None:
        return None
    amplitude = last - first
    if abs(amplitude) <= 1e-12:
        return 0.0
    target = first + amplitude * float(fraction)
    base_ts = _parse_timestamp(ordered[0].get("timestamp"))
    if base_ts is None:
        return None
    direction = 1.0 if amplitude >= 0 else -1.0
    for row in ordered:
        ts = _parse_timestamp(row.get("timestamp"))
        value = _safe_float(row.get(key))
        if ts is None or value is None:
            continue
        if direction * (value - target) >= 0:
            return max(0.0, (ts - base_ts).total_seconds())
    return None


def detect_dewpoint_rebound(
    rows: Sequence[Mapping[str, Any]],
    *,
    actuation_events: Optional[Sequence[Mapping[str, Any]]] = None,
    process_variant: Optional[str] = None,
    layer: Optional[int] = None,
    repeat_index: Optional[int] = None,
    gas_ppm: Optional[int] = None,
    rebound_window_s: float = DEFAULT_DEWPOINT_REBOUND_WINDOW_S,
    rebound_min_rise_c: float = DEFAULT_DEWPOINT_REBOUND_MIN_RISE_C,
    rebound_require_no_new_actuation: bool = True,
) -> Dict[str, Any]:
    return shared_detect_dewpoint_rebound(
        rows,
        actuation_events=actuation_events,
        process_variant=process_variant,
        layer=layer,
        repeat_index=repeat_index,
        gas_ppm=gas_ppm,
        rebound_window_s=rebound_window_s,
        rebound_min_rise_c=rebound_min_rise_c,
        rebound_require_no_new_actuation=rebound_require_no_new_actuation,
    )


def build_phase_gate_row(
    *,
    process_variant: str,
    layer: int,
    repeat_index: int,
    gas_ppm: Optional[int],
    pressure_target_hpa: Optional[int],
    phase: str,
    gate_name: str,
    gate_status: str,
    gate_pass: bool,
    gate_window_s: Optional[float],
    gate_value: Any,
    gate_threshold: Any,
    gate_fail_reason: str = "",
    note: str = "",
) -> Dict[str, Any]:
    return {
        "timestamp": datetime.now().isoformat(timespec="seconds"),
        "process_variant": process_variant,
        "layer": int(layer),
        "repeat_index": int(repeat_index),
        "phase": phase,
        "gas_ppm": gas_ppm,
        "pressure_target_hpa": pressure_target_hpa,
        "gate_name": gate_name,
        "gate_status": gate_status,
        "gate_pass": bool(gate_pass),
        "gate_window_s": gate_window_s,
        "gate_value": gate_value,
        "gate_threshold": gate_threshold,
        "gate_fail_reason": gate_fail_reason,
        "note": note,
    }


def evaluate_flush_gate(
    rows: Sequence[Mapping[str, Any]],
    *,
    min_flush_s: float,
    target_flush_s: float,
    gate_window_s: float = DEFAULT_FLUSH_GATE_WINDOW_S,
    require_ratio: bool = True,
    require_vent_on: bool = True,
    thresholds: MetrologyDiagnosticThresholds = DEFAULT_THRESHOLDS,
) -> Dict[str, Any]:
    ordered = _ordered_rows(rows)
    duration_s = _phase_duration_s(ordered)
    tail = _tail_rows(ordered, gate_window_s)
    dewpoint_gate = evaluate_dewpoint_flush_gate(
        ordered,
        min_flush_s=min_flush_s,
        gate_window_s=gate_window_s,
        max_tail_span_c=thresholds.flush_dewpoint_span_max_c,
        max_abs_tail_slope_c_per_s=thresholds.flush_dewpoint_slope_abs_max_per_s,
        include_rebound_in_gate=False,
        require_vent_on=require_vent_on,
    )
    ratio_values = _series(tail, "analyzer2_co2_ratio")
    ratio_slope = _slope_per_s(tail, "analyzer2_co2_ratio")
    ratio_span = (max(ratio_values) - min(ratio_values)) if len(ratio_values) >= 2 else None
    dewpoint_slope = _safe_float(dewpoint_gate.get("dewpoint_tail_slope_60s"))
    dewpoint_span = _safe_float(dewpoint_gate.get("dewpoint_tail_span_60s"))
    pressure_values = _preferred_pressure_series(tail)
    pressure_std = _safe_std(pressure_values)
    pressure_span = (max(pressure_values) - min(pressure_values)) if len(pressure_values) >= 2 else None
    gauge_slope = _slope_per_s(tail, "gauge_pressure_hpa")
    latest = ordered[-1] if ordered else {}
    vent_on = str(dewpoint_gate.get("vent_state_during_flush") or "") == "VENT_ON"

    reasons: List[str] = []
    if require_vent_on and not vent_on:
        reasons.append("flush_not_all_vent_on")
    if duration_s < float(min_flush_s):
        reasons.append("flush_duration_below_min")
    if duration_s < float(target_flush_s):
        reasons.append("flush_duration_below_target")
    if require_ratio:
        if ratio_slope is None:
            reasons.append("ratio_tail_window_missing")
        elif abs(ratio_slope) > thresholds.flush_ratio_slope_abs_max_per_s:
            reasons.append("ratio_tail_slope_too_large")
        if ratio_span is None:
            reasons.append("ratio_tail_span_missing")
        elif ratio_span > thresholds.flush_ratio_span_max:
            reasons.append("ratio_tail_span_too_large")
    if dewpoint_slope is None:
        reasons.append("dewpoint_tail_window_missing")
    elif abs(dewpoint_slope) > thresholds.flush_dewpoint_slope_abs_max_per_s:
        reasons.append("dewpoint_tail_slope_too_large")
    if dewpoint_span is None:
        reasons.append("dewpoint_tail_span_missing")
    elif dewpoint_span > thresholds.flush_dewpoint_span_max_c:
        reasons.append("dewpoint_tail_span_too_large")
    if pressure_std is None:
        reasons.append("pressure_tail_window_missing")
    elif pressure_std > thresholds.flush_pressure_std_max_hpa:
        reasons.append("pressure_tail_std_too_large")
    if gauge_slope is None:
        reasons.append("gauge_tail_window_missing")
    elif abs(gauge_slope) > thresholds.flush_pressure_slope_abs_max_hpa_per_s:
        reasons.append("gauge_tail_slope_too_large")
    if pressure_span is None:
        reasons.append("gauge_tail_span_missing")
    elif pressure_span > thresholds.flush_pressure_span_max_hpa:
        reasons.append("gauge_tail_span_too_large")

    if "flush_not_all_vent_on" in reasons or "flush_duration_below_min" in reasons:
        status = "fail"
    elif reasons:
        status = "warn"
    else:
        status = "pass"
    gate_pass = status == "pass"
    return {
        "gate_pass": gate_pass,
        "gate_status": status,
        "gate_window_s": float(gate_window_s),
        "gate_fail_reason": ";".join(reasons),
        "failing_subgates": reasons,
        "flush_duration_s": duration_s,
        "dewpoint_time_to_gate": duration_s if gate_pass else None,
        "dewpoint_tail_span_60s": dewpoint_span,
        "dewpoint_tail_slope_60s": dewpoint_slope,
        "flush_gate_reason": ";".join(reasons),
        "flush_gate_ratio_value": _safe_float(latest.get("analyzer2_co2_ratio")),
        "flush_gate_ratio_slope": ratio_slope,
        "flush_gate_ratio_span": ratio_span,
        "flush_gate_dewpoint_value": _safe_float(latest.get("dewpoint_c")),
        "flush_gate_dewpoint_slope": dewpoint_slope,
        "flush_gate_dewpoint_span": dewpoint_span,
        "flush_gate_gauge_value": _safe_float(latest.get("gauge_pressure_hpa")),
        "flush_gate_pressure_std": pressure_std,
        "flush_gate_pressure_span": pressure_span,
        "flush_gate_pressure_slope": gauge_slope,
        "vent_state_during_flush": "VENT_ON" if vent_on else "NOT_ALL_VENT_ON",
    }


def build_flush_summary(
    rows: Sequence[Mapping[str, Any]],
    *,
    process_variant: str,
    layer: int,
    repeat_index: int,
    gas_ppm: int,
    actual_deadtime_s: Optional[float],
    actuation_events: Optional[Sequence[Mapping[str, Any]]] = None,
    min_flush_s: float = DEFAULT_MIN_FLUSH_S,
    target_flush_s: float = DEFAULT_SCREENING_FLUSH_S,
    gate_window_s: float = DEFAULT_FLUSH_GATE_WINDOW_S,
    require_ratio: bool = True,
    require_vent_on: bool = True,
    rebound_window_s: float = DEFAULT_DEWPOINT_REBOUND_WINDOW_S,
    rebound_min_rise_c: float = DEFAULT_DEWPOINT_REBOUND_MIN_RISE_C,
    rebound_require_no_new_actuation: bool = True,
    thresholds: MetrologyDiagnosticThresholds = DEFAULT_THRESHOLDS,
) -> Dict[str, Any]:
    ordered = _ordered_rows(rows)
    timestamps = [_parse_timestamp(row.get("timestamp")) for row in ordered]
    ratio_values = _series(ordered, "analyzer2_co2_ratio")
    dewpoint_values = _series(ordered, "dewpoint_c")
    rh_values = _series(ordered, "dewpoint_rh_percent")
    pressure_values = _preferred_pressure_series(ordered)
    gate = evaluate_flush_gate(
        ordered,
        min_flush_s=min_flush_s,
        target_flush_s=target_flush_s,
        gate_window_s=gate_window_s,
        require_ratio=require_ratio,
        require_vent_on=require_vent_on,
        thresholds=thresholds,
    )
    last30 = _tail_rows(ordered, 30.0)
    last60 = _tail_rows(ordered, 60.0)
    pressure_stats = _pressure_spike_stats(pressure_values)
    analyzer_count = _raw_sample_count(ordered, "analyzer2_co2_ratio")
    gauge_count = _raw_sample_count(ordered, "gauge_pressure_hpa")
    dewpoint_count = _raw_sample_count(ordered, "dewpoint_c")
    aligned_count = _aligned_like_sample_count(ordered)
    start_pressure = pressure_values[0] if pressure_values else None
    peak_pressure = max(pressure_values) if pressure_values else None
    end_pressure = pressure_values[-1] if pressure_values else None
    warning_flags = []
    if gate["gate_status"] != "pass":
        warning_flags.extend(str(gate["gate_fail_reason"] or "").split(";"))
    pressure_rise = (
        float(peak_pressure) - float(start_pressure)
        if peak_pressure is not None and start_pressure is not None
        else None
    )
    if pressure_rise is not None and pressure_stats["flush_pressure_p99"] is not None and pressure_stats["flush_pressure_median"] is not None:
        if pressure_stats["flush_pressure_p99"] - pressure_stats["flush_pressure_median"] > thresholds.flush_pressure_span_warn_hpa:
            warning_flags.append("flush_pressure_spike")
    rebound = detect_dewpoint_rebound(
        ordered,
        actuation_events=actuation_events,
        process_variant=process_variant,
        layer=layer,
        repeat_index=repeat_index,
        gas_ppm=gas_ppm,
        rebound_window_s=rebound_window_s,
        rebound_min_rise_c=rebound_min_rise_c,
        rebound_require_no_new_actuation=rebound_require_no_new_actuation,
    )
    if rebound.get("dewpoint_rebound_detected"):
        warning_flags.append("dewpoint_rebound_detected")
    return {
        "process_variant": process_variant,
        "layer": int(layer),
        "repeat_index": int(repeat_index),
        "gas_ppm": int(gas_ppm),
        "flush_start_time": _safe_iso(timestamps[0]) if timestamps else None,
        "flush_end_time": _safe_iso(timestamps[-1]) if timestamps else None,
        "actual_deadtime_s": actual_deadtime_s,
        "flush_duration_s": gate["flush_duration_s"],
        "vent_state_during_flush": gate["vent_state_during_flush"],
        "flush_gate_window_s": gate["gate_window_s"],
        "flush_gate_status": gate["gate_status"],
        "flush_gate_pass": gate["gate_pass"],
        "flush_gate_fail_reason": gate["gate_fail_reason"],
        "flush_gate_reason": gate["gate_fail_reason"],
        "flush_gate_require_vent_on": require_vent_on,
        "flush_gate_failing_subgates": _coerce_flag_list(gate.get("failing_subgates", [])),
        "flush_gate_require_ratio": bool(require_ratio),
        "flush_ratio_start": ratio_values[0] if ratio_values else None,
        "flush_ratio_end": ratio_values[-1] if ratio_values else None,
        "flush_ratio_t90_s": _response_txx_s(ordered, key="analyzer2_co2_ratio", fraction=0.90),
        "flush_ratio_t95_s": _response_txx_s(ordered, key="analyzer2_co2_ratio", fraction=0.95),
        "flush_last30s_mean": _safe_mean(_series(last30, "analyzer2_co2_ratio")),
        "flush_last30s_std": _safe_std(_series(last30, "analyzer2_co2_ratio")),
        "flush_last30s_ratio_slope": _slope_per_s(last30, "analyzer2_co2_ratio"),
        "flush_last60s_mean": _safe_mean(_series(last60, "analyzer2_co2_ratio")),
        "flush_last60s_std": _safe_std(_series(last60, "analyzer2_co2_ratio")),
        "flush_last60s_ratio_span": (
            max(_series(last60, "analyzer2_co2_ratio")) - min(_series(last60, "analyzer2_co2_ratio"))
            if len(_series(last60, "analyzer2_co2_ratio")) >= 2
            else None
        ),
        "flush_last60s_ratio_slope": _slope_per_s(last60, "analyzer2_co2_ratio"),
        "flush_last60s_dewpoint_span": gate.get("dewpoint_tail_span_60s"),
        "flush_last60s_dewpoint_slope": gate.get("dewpoint_tail_slope_60s"),
        "dewpoint_tail_span_60s": gate.get("dewpoint_tail_span_60s"),
        "dewpoint_tail_slope_60s": gate.get("dewpoint_tail_slope_60s"),
        "dewpoint_time_to_gate": _safe_float(gate.get("dewpoint_time_to_gate")),
        "flush_last60s_gauge_span_hpa": (
            max(_series(last60, "gauge_pressure_hpa")) - min(_series(last60, "gauge_pressure_hpa"))
            if len(_series(last60, "gauge_pressure_hpa")) >= 2
            else None
        ),
        "flush_last60s_gauge_slope_hpa_per_s": _slope_per_s(last60, "gauge_pressure_hpa"),
        "flush_pressure_start_hpa": start_pressure,
        "flush_pressure_peak_hpa": peak_pressure,
        "flush_pressure_end_hpa": end_pressure,
        "flush_pressure_rise_hpa": pressure_rise,
        "flush_dewpoint_start": dewpoint_values[0] if dewpoint_values else None,
        "flush_dewpoint_end": dewpoint_values[-1] if dewpoint_values else None,
        "flush_rh_start": rh_values[0] if rh_values else None,
        "flush_rh_end": rh_values[-1] if rh_values else None,
        "flush_warning_flags": _coerce_flag_list(warning_flags),
        "analyzer_raw_sample_count": analyzer_count,
        "gauge_raw_sample_count": gauge_count,
        "dewpoint_raw_sample_count": dewpoint_count,
        "aligned_sample_count": aligned_count,
        **rebound,
        **pressure_stats,
    }


def build_return_to_zero_summary(
    *,
    process_variant: str,
    cycle_index: int,
    first_zero: Mapping[str, Any],
    second_zero: Mapping[str, Any],
    thresholds: MetrologyDiagnosticThresholds = DEFAULT_THRESHOLDS,
) -> Dict[str, Any]:
    ref = _safe_float(first_zero.get("flush_last30s_mean"))
    returned = _safe_float(second_zero.get("flush_last30s_mean"))
    delta = None if ref is None or returned is None else returned - ref
    status = _status_low_good(
        delta,
        pass_max=thresholds.return_to_zero_warn_delta,
        warn_max=thresholds.return_to_zero_fail_delta,
    )
    return {
        "process_variant": process_variant,
        "cycle_index": int(cycle_index),
        "first_zero_repeat_index": first_zero.get("repeat_index"),
        "second_zero_repeat_index": second_zero.get("repeat_index"),
        "return_to_zero_reference_ratio": ref,
        "return_to_zero_ratio": returned,
        "return_to_zero_delta": delta,
        "return_to_zero_status": status,
    }


def build_seal_hold_summary(
    rows: Sequence[Mapping[str, Any]],
    *,
    process_variant: str,
    layer: int,
    repeat_index: int,
    gas_ppm: int,
    hold_duration_target_s: float,
    thresholds: MetrologyDiagnosticThresholds = DEFAULT_THRESHOLDS,
) -> Dict[str, Any]:
    ordered = _ordered_rows(rows)
    timestamps = [_parse_timestamp(row.get("timestamp")) for row in ordered]
    ratio_values = _series(ordered, "analyzer2_co2_ratio")
    gauge_values = _series(ordered, "gauge_pressure_hpa")
    dewpoint_values = _series(ordered, "dewpoint_c")
    rh_values = _series(ordered, "dewpoint_rh_percent")
    analyzer_count = _raw_sample_count(ordered, "analyzer2_co2_ratio")
    gauge_count = _raw_sample_count(ordered, "gauge_pressure_hpa")
    dewpoint_count = _raw_sample_count(ordered, "dewpoint_c")
    aligned_count = _aligned_like_sample_count(ordered)
    duration_s = _phase_duration_s(ordered)
    pressure_drift = None if len(gauge_values) < 2 else gauge_values[-1] - gauge_values[0]
    ratio_drift = None if len(ratio_values) < 2 else ratio_values[-1] - ratio_values[0]
    dewpoint_drift = None if len(dewpoint_values) < 2 else dewpoint_values[-1] - dewpoint_values[0]
    rh_drift = None if len(rh_values) < 2 else rh_values[-1] - rh_values[0]
    duration_min = duration_s / 60.0 if duration_s > 0 else None
    pressure_drift_per_min = (
        pressure_drift / duration_min if pressure_drift is not None and duration_min not in (None, 0.0) else None
    )
    ratio_drift_per_min = (
        ratio_drift / duration_min if ratio_drift is not None and duration_min not in (None, 0.0) else None
    )
    dewpoint_drift_per_min = (
        dewpoint_drift / duration_min if dewpoint_drift is not None and duration_min not in (None, 0.0) else None
    )
    statuses = [
        _status_low_good(
            pressure_drift_per_min,
            pass_max=thresholds.sealed_hold_pressure_drift_warn_hpa_per_min,
            warn_max=thresholds.sealed_hold_pressure_drift_fail_hpa_per_min,
        ),
        _status_low_good(
            ratio_drift_per_min,
            pass_max=thresholds.sealed_hold_ratio_drift_warn_per_min,
            warn_max=thresholds.sealed_hold_ratio_drift_fail_per_min,
        ),
        _status_low_good(
            dewpoint_drift_per_min,
            pass_max=thresholds.sealed_hold_dewpoint_drift_warn_c_per_min,
            warn_max=thresholds.sealed_hold_dewpoint_drift_fail_c_per_min,
        ),
    ]
    warning_flags = []
    if duration_s < float(hold_duration_target_s):
        statuses.append("warn")
        warning_flags.append("sealed_hold_shorter_than_target")
    phase_status = _merge_statuses(statuses)
    if gas_ppm == 0 and ratio_drift is not None and ratio_drift > thresholds.sealed_hold_ratio_drift_warn_per_min * max(1.0, duration_min or 1.0):
        warning_flags.append("zero_gas_ratio_up_drift")
    if gas_ppm == 1000 and ratio_drift is not None and ratio_drift < -thresholds.sealed_hold_ratio_drift_warn_per_min * max(1.0, duration_min or 1.0):
        warning_flags.append("span_gas_ratio_down_drift")
    return {
        "process_variant": process_variant,
        "layer": int(layer),
        "repeat_index": int(repeat_index),
        "gas_ppm": int(gas_ppm),
        "hold_start_time": _safe_iso(timestamps[0]) if timestamps else None,
        "hold_end_time": _safe_iso(timestamps[-1]) if timestamps else None,
        "hold_duration_s": duration_s,
        "hold_pressure_start_hpa": gauge_values[0] if gauge_values else None,
        "hold_pressure_end_hpa": gauge_values[-1] if gauge_values else None,
        "hold_pressure_drift_hpa": pressure_drift,
        "hold_pressure_drift_hpa_per_min": pressure_drift_per_min,
        "hold_ratio_start": ratio_values[0] if ratio_values else None,
        "hold_ratio_end": ratio_values[-1] if ratio_values else None,
        "hold_ratio_drift": ratio_drift,
        "hold_ratio_drift_per_min": ratio_drift_per_min,
        "hold_dewpoint_start_c": dewpoint_values[0] if dewpoint_values else None,
        "hold_dewpoint_end_c": dewpoint_values[-1] if dewpoint_values else None,
        "hold_dewpoint_drift_c": dewpoint_drift,
        "hold_dewpoint_drift_c_per_min": dewpoint_drift_per_min,
        "hold_rh_start": rh_values[0] if rh_values else None,
        "hold_rh_end": rh_values[-1] if rh_values else None,
        "hold_rh_drift": rh_drift,
        "hold_warning_flags": _coerce_flag_list(warning_flags),
        "analyzer_raw_sample_count": analyzer_count,
        "gauge_raw_sample_count": gauge_count,
        "dewpoint_raw_sample_count": dewpoint_count,
        "aligned_sample_count": aligned_count,
        "phase_status": phase_status,
    }


def _trace_stage_timestamp(rows: Sequence[Mapping[str, Any]], trace_stage: str) -> Optional[datetime]:
    target = str(trace_stage or "").strip()
    if not target:
        return None
    for row in reversed(_ordered_rows(rows)):
        if str(row.get("trace_stage") or "").strip() != target:
            continue
        ts = _parse_timestamp(row.get("timestamp"))
        if ts is not None:
            return ts
    return None


def _timing_ms(start: Optional[datetime], end: Optional[datetime]) -> Optional[float]:
    if start is None or end is None:
        return None
    return round(max(0.0, (end - start).total_seconds()) * 1000.0, 3)


def _trace_row_for_stage(rows: Sequence[Mapping[str, Any]], *trace_stages: str) -> Dict[str, Any]:
    targets = {str(item or "").strip() for item in trace_stages if str(item or "").strip()}
    if not targets:
        return {}
    for row in reversed(_ordered_rows(rows)):
        if str(row.get("trace_stage") or "").strip() in targets:
            return dict(row)
    return {}


def build_pressure_point_summary(
    transition_rows: Sequence[Mapping[str, Any]],
    stable_rows: Sequence[Mapping[str, Any]],
    *,
    process_variant: str,
    layer: int,
    repeat_index: int,
    gas_ppm: int,
    pressure_target_hpa: int,
    settle_started_time: Optional[datetime] = None,
    settle_reached_time: Optional[datetime] = None,
    sample_start_time: Optional[datetime] = None,
    sample_end_time: Optional[datetime] = None,
    thresholds: MetrologyDiagnosticThresholds = DEFAULT_THRESHOLDS,
) -> Dict[str, Any]:
    ordered_transition = _ordered_rows(transition_rows)
    ordered_stable = _ordered_rows(stable_rows)
    ratio_values = _series(ordered_stable, "analyzer2_co2_ratio")
    analyzer_pressure_values = _series(ordered_stable, "analyzer2_pressure_hpa")
    gauge_values = _series(ordered_stable, "gauge_pressure_hpa")
    dewpoint_values = _series(ordered_stable, "dewpoint_c")
    dew_temp_values = _series(ordered_stable, "dewpoint_temp_c")
    dew_rh_values = _series(ordered_stable, "dewpoint_rh_percent")
    pressure_in_limits_ts = _trace_stage_timestamp(ordered_transition, "pressure_in_limits")
    dewpoint_gate_begin_ts = _trace_stage_timestamp(ordered_transition, "dewpoint_gate_begin")
    dewpoint_gate_end_ts = _trace_stage_timestamp(ordered_transition, "dewpoint_gate_end")
    sampling_begin_ts = _trace_stage_timestamp(ordered_transition, "sampling_begin")
    first_effective_sample_ts = _parse_timestamp(ordered_stable[0].get("timestamp")) if ordered_stable else None
    gate_metrics_row = _trace_row_for_stage(
        ordered_transition,
        "dewpoint_gate_pass",
        "dewpoint_gate_timeout",
        "dewpoint_gate_end",
    )
    dewpoint_gate_result = "skipped"
    if gate_metrics_row:
        gate_stage = str(gate_metrics_row.get("trace_stage") or "").strip().lower()
        if gate_stage == "dewpoint_gate_pass":
            dewpoint_gate_result = "stable"
        elif gate_stage == "dewpoint_gate_timeout":
            dewpoint_gate_result = "timeout"
        elif gate_stage == "dewpoint_gate_end":
            note = str(gate_metrics_row.get("phase_note") or "").strip().lower()
            if "result=stable" in note:
                dewpoint_gate_result = "stable"
            elif "result=timeout" in note:
                dewpoint_gate_result = "timeout"
    combined_pressure = _preferred_pressure_series(ordered_transition + ordered_stable)
    gauge_mean = _safe_mean(gauge_values)
    analyzer_mean = _safe_mean(analyzer_pressure_values)
    tracking_error = None if gauge_mean is None else gauge_mean - float(pressure_target_hpa)
    gauge_minus_analyzer = None if gauge_mean is None or analyzer_mean is None else gauge_mean - analyzer_mean
    overshoot = None
    rebound = None
    pressure_monotonicity_score = None
    if combined_pressure:
        overshoot = max(0.0, max(combined_pressure) - float(pressure_target_hpa))
        rebound = max(0.0, max(combined_pressure) - min(combined_pressure))
        if len(combined_pressure) >= 2:
            monotonic_steps = 0
            comparable_steps = 0
            for prev, curr in zip(combined_pressure, combined_pressure[1:]):
                if curr <= prev + 0.3:
                    monotonic_steps += 1
                comparable_steps += 1
            pressure_monotonicity_score = monotonic_steps / comparable_steps if comparable_steps else None
    ratio_slope = _slope_per_s(ordered_stable, "analyzer2_co2_ratio")
    dewpoint_slope = _slope_per_s(ordered_stable, "dewpoint_c")
    pressure_std = _safe_std(gauge_values)
    analyzer_count = _raw_sample_count(ordered_stable, "analyzer2_co2_ratio")
    gauge_count = _raw_sample_count(ordered_stable, "gauge_pressure_hpa")
    dewpoint_count = _raw_sample_count(ordered_stable, "dewpoint_c")
    aligned_count = _aligned_like_sample_count(ordered_stable)
    statuses: List[str] = []
    warning_flags: List[str] = []
    stable_count = len(ordered_stable)
    if stable_count < int(thresholds.stable_sample_count_min):
        statuses.append("insufficient_evidence")
        warning_flags.append("stable_sample_count_below_min")
    elif stable_count < int(thresholds.stable_sample_count_target):
        statuses.append("warn")
        warning_flags.append("stable_sample_count_below_target")
    if analyzer_count < int(thresholds.stable_sample_count_min):
        statuses.append("insufficient_evidence")
        warning_flags.append("insufficient_analyzer_samples")
    if gauge_count < int(thresholds.stable_sample_count_min):
        statuses.append("insufficient_evidence")
        warning_flags.append("insufficient_gauge_samples")
    if dewpoint_count < int(thresholds.stable_sample_count_min):
        statuses.append("insufficient_evidence")
        warning_flags.append("insufficient_dewpoint_samples")
    if aligned_count < int(thresholds.stable_sample_count_min):
        statuses.append("insufficient_evidence")
        warning_flags.append("insufficient_aligned_samples")

    point_statuses = [
        _status_low_good(
            ratio_slope,
            pass_max=thresholds.point_window_ratio_slope_warn_per_s,
            warn_max=thresholds.point_window_ratio_slope_fail_per_s,
        ),
        _status_low_good(
            dewpoint_slope,
            pass_max=thresholds.point_window_dewpoint_slope_warn_per_s,
            warn_max=thresholds.point_window_dewpoint_slope_fail_per_s,
        ),
        _status_low_good(
            pressure_std,
            pass_max=thresholds.point_window_pressure_std_warn_hpa,
            warn_max=thresholds.point_window_pressure_std_fail_hpa,
        ),
    ]
    statuses.extend(point_statuses)
    if tracking_error is None:
        statuses.append("insufficient_evidence")
        warning_flags.append("gauge_pressure_missing")
    if point_statuses[0] != "pass":
        warning_flags.append("ratio_window_not_flat")
    if point_statuses[1] != "pass":
        warning_flags.append("dewpoint_window_not_flat")
    if point_statuses[2] != "pass":
        warning_flags.append("pressure_window_not_stable")
    phase_status = _merge_statuses(statuses)
    bias_series = [
        gauge - analyzer
        for gauge, analyzer in zip(gauge_values, analyzer_pressure_values)
    ]
    return {
        "process_variant": process_variant,
        "layer": int(layer),
        "repeat_index": int(repeat_index),
        "gas_ppm": int(gas_ppm),
        "pressure_target_hpa": int(pressure_target_hpa),
        "start_time": _safe_iso(settle_started_time),
        "settle_reached_time": _safe_iso(settle_reached_time),
        "sample_start_time": _safe_iso(sample_start_time),
        "sample_end_time": _safe_iso(sample_end_time),
        "stable_sample_count": stable_count,
        "stable_sample_count_min": int(thresholds.stable_sample_count_min),
        "stable_sample_count_target": int(thresholds.stable_sample_count_target),
        "analyzer_raw_sample_count": analyzer_count,
        "gauge_raw_sample_count": gauge_count,
        "dewpoint_raw_sample_count": dewpoint_count,
        "aligned_sample_count": aligned_count,
        "analyzer2_co2_ratio_mean": _safe_mean(ratio_values),
        "analyzer2_co2_ratio_std": _safe_std(ratio_values),
        "analyzer2_pressure_mean": analyzer_mean,
        "analyzer2_pressure_std": _safe_std(analyzer_pressure_values),
        "gauge_pressure_mean": gauge_mean,
        "gauge_pressure_std": pressure_std,
        "gauge_minus_analyzer_pressure_mean": gauge_minus_analyzer,
        "gauge_minus_analyzer_pressure_std": _safe_std(bias_series),
        "dewpoint_mean": _safe_mean(dewpoint_values),
        "dewpoint_std": _safe_std(dewpoint_values),
        "dewpoint_temp_mean": _safe_mean(dew_temp_values),
        "dewpoint_rh_mean": _safe_mean(dew_rh_values),
        "pressure_tracking_error": tracking_error,
        "settle_time_s": (
            max(0.0, (settle_reached_time - settle_started_time).total_seconds())
            if settle_started_time is not None and settle_reached_time is not None
            else None
        ),
        "dwell_to_stable_s": (
            max(0.0, (sample_start_time - settle_reached_time).total_seconds())
            if settle_reached_time is not None and sample_start_time is not None
            else None
        ),
        "pressure_in_limits_to_dewpoint_gate_begin_ms": _timing_ms(pressure_in_limits_ts, dewpoint_gate_begin_ts),
        "dewpoint_gate_begin_to_dewpoint_gate_end_ms": _timing_ms(dewpoint_gate_begin_ts, dewpoint_gate_end_ts),
        "dewpoint_gate_end_to_sampling_begin_ms": _timing_ms(dewpoint_gate_end_ts, sampling_begin_ts),
        "sampling_begin_to_first_effective_sample_ms": _timing_ms(sampling_begin_ts, first_effective_sample_ts),
        "dewpoint_gate_result": dewpoint_gate_result,
        "dewpoint_gate_elapsed_s": _safe_float(gate_metrics_row.get("dewpoint_gate_elapsed_s")),
        "dewpoint_gate_count": _safe_int(gate_metrics_row.get("dewpoint_gate_count")),
        "dewpoint_gate_span_c": _safe_float(gate_metrics_row.get("dewpoint_gate_span_c")),
        "dewpoint_gate_slope_c_per_s": _safe_float(gate_metrics_row.get("dewpoint_gate_slope_c_per_s")),
        "overshoot_hpa": overshoot,
        "rebound_hpa": rebound,
        "pressure_monotonicity_score": pressure_monotonicity_score,
        "point_window_ratio_slope_per_s": ratio_slope,
        "point_window_dewpoint_slope_per_s": dewpoint_slope,
        "phase_status": phase_status,
        "warning_flags": _coerce_flag_list(warning_flags),
    }


def build_aligned_rows(
    raw_rows: Sequence[Mapping[str, Any]],
    *,
    interval_s: float = DEFAULT_SAMPLE_POLL_S,
) -> List[Dict[str, Any]]:
    if not raw_rows:
        return []
    step_s = max(0.2, float(interval_s))
    ordered_rows = _ordered_rows(raw_rows)
    timestamps = [_parse_timestamp(row.get("timestamp")) for row in ordered_rows]
    timestamps = [item for item in timestamps if item is not None]
    if not timestamps:
        return []
    start = timestamps[0].replace(microsecond=0)
    end = timestamps[-1]
    current_snapshot: Dict[str, Any] = {}
    cursor = 0
    fields_to_keep = [
        "process_variant",
        "layer",
        "repeat_index",
        "phase",
        "gas_ppm",
        "pressure_target_hpa",
        "analyzer2_co2_ratio",
        "analyzer2_pressure_hpa",
        "gauge_pressure_hpa",
        "dewpoint_c",
        "dewpoint_temp_c",
        "dewpoint_rh_percent",
        "controller_pressure_hpa",
        "controller_vent_state",
        "controller_vent_status_code",
        "controller_output_state",
        "controller_isolation_state",
        "actual_deadtime_s",
        "gate_pass",
        "gate_fail_reason",
    ]
    aligned: List[Dict[str, Any]] = []
    moment = start
    while moment <= end + timedelta(seconds=step_s / 2.0):
        bucket_end = moment + timedelta(seconds=step_s)
        saw_any = False
        while cursor < len(ordered_rows):
            row = ordered_rows[cursor]
            ts = _parse_timestamp(row.get("timestamp"))
            if ts is None or ts >= bucket_end:
                break
            saw_any = True
            for key in fields_to_keep:
                value = row.get(key)
                if value not in (None, ""):
                    current_snapshot[key] = value
            cursor += 1
        if saw_any or current_snapshot:
            aligned.append(
                {
                    "timestamp": moment.isoformat(timespec="seconds"),
                    **{key: current_snapshot.get(key) for key in fields_to_keep},
                }
            )
        moment = bucket_end
    return aligned


def _group_rows_by_keys(rows: Sequence[Mapping[str, Any]], *keys: str) -> Dict[tuple[Any, ...], List[Dict[str, Any]]]:
    grouped: Dict[tuple[Any, ...], List[Dict[str, Any]]] = {}
    for row in rows:
        key = tuple(row.get(name) for name in keys)
        grouped.setdefault(key, []).append(dict(row))
    return grouped


def _variant_rank(value: str) -> tuple[int, str]:
    status_order = {"pass": 0, "warn": 1, "insufficient_evidence": 2, "fail": 3}
    return (status_order.get(str(value or ""), 9), str(value or ""))


def analyze_room_temp_diagnostic(
    flush_summaries: Sequence[Mapping[str, Any]],
    seal_hold_summaries: Sequence[Mapping[str, Any]],
    pressure_summaries: Sequence[Mapping[str, Any]],
    *,
    phase_gate_rows: Optional[Sequence[Mapping[str, Any]]] = None,
    min_flush_s: float = DEFAULT_MIN_FLUSH_S,
    target_flush_s: float = DEFAULT_SCREENING_FLUSH_S,
    expected_deadtime_s: float = DEFAULT_GAS_SWITCH_DEADTIME_S,
    thresholds: MetrologyDiagnosticThresholds = DEFAULT_THRESHOLDS,
) -> Dict[str, Any]:
    flush_rows = _ordered_rows(flush_summaries)
    hold_rows = _ordered_rows(seal_hold_summaries)
    point_rows = _ordered_rows(pressure_summaries)
    gate_rows = _ordered_rows(phase_gate_rows or [])
    variants = sorted(
        {
            str(row.get("process_variant"))
            for row in flush_rows + hold_rows + point_rows + gate_rows
            if str(row.get("process_variant") or "").strip()
        }
    )
    variant_summaries: List[Dict[str, Any]] = []
    best_variant: Optional[str] = None
    best_rank = (99, "")
    overall_statuses: List[str] = []
    biggest_error_votes: Dict[str, int] = {}
    layer4_eligible_variants: List[str] = []

    for variant in variants:
        variant_flush = [row for row in flush_rows if row.get("process_variant") == variant]
        variant_holds = [row for row in hold_rows if row.get("process_variant") == variant]
        variant_points = [row for row in point_rows if row.get("process_variant") == variant]

        metric_rows: List[Dict[str, Any]] = []
        summary_messages: List[str] = []
        dominant_candidates: List[str] = []

        flush_vent_status = "pass" if all(str(row.get("vent_state_during_flush") or "").upper() == "VENT_ON" for row in variant_flush) else "fail"
        metric_rows.append({"name": "flush_vent_state_check", "status": flush_vent_status, "value": None})
        if flush_vent_status == "fail":
            summary_messages.append("通气阶段存在 VENT OFF，直接不满足资格确认。")
            dominant_candidates.append("切气串扰")

        flush_duration_status = "pass"
        if any((_safe_float(row.get("flush_duration_s")) or 0.0) < float(min_flush_s) for row in variant_flush):
            flush_duration_status = "fail"
        metric_rows.append({"name": "flush_duration_check", "status": flush_duration_status, "value": min_flush_s})
        if flush_duration_status == "fail":
            summary_messages.append("存在 flush_duration_s 小于 min_flush_s。")

        flush_gate_status = _merge_statuses(row.get("flush_gate_status") for row in variant_flush)
        metric_rows.append(
            {
                "name": "flush_gate_check",
                "status": flush_gate_status,
                "value": [row.get("flush_gate_fail_reason") for row in variant_flush],
            }
        )
        if flush_gate_status != "pass":
            summary_messages.append("存在 flush gate 未通过或达到 max_flush_s 仍不过 gate 的气点。")
            dominant_candidates.append("切气串扰")

        deadtimes = [_safe_float(row.get("actual_deadtime_s")) for row in variant_flush if row.get("actual_deadtime_s") not in (None, "")]
        deadtime_mean = _safe_mean([item for item in deadtimes if item is not None])
        deadtime_std = _safe_std([item for item in deadtimes if item is not None])
        deadtime_status = _merge_statuses(
            [
                _status_low_good(
                    None if deadtime_mean is None else deadtime_mean - float(expected_deadtime_s),
                    pass_max=thresholds.deadtime_warn_abs_s,
                    warn_max=thresholds.deadtime_fail_abs_s,
                ),
                _status_low_good(
                    deadtime_std,
                    pass_max=thresholds.deadtime_warn_std_s,
                    warn_max=thresholds.deadtime_fail_std_s,
                ),
            ]
        )
        if deadtime_status == "fail":
            deadtime_status = "warn"
        metric_rows.append(
            {
                "name": "gas_switch_deadtime_check",
                "status": deadtime_status,
                "value": {
                    "expected_deadtime_s": expected_deadtime_s,
                    "actual_deadtime_mean_s": deadtime_mean,
                    "actual_deadtime_std_s": deadtime_std,
                },
            }
        )

        return_zero_rows: List[Dict[str, Any]] = []
        layer1_zeros = [row for row in variant_flush if _safe_int(row.get("layer")) == 1 and _safe_int(row.get("gas_ppm")) == 0]
        zero_groups = _group_rows_by_keys(layer1_zeros, "repeat_index")
        for repeat_key in sorted(zero_groups.keys(), key=lambda item: _safe_int(item[0]) or 0):
            rows_same_repeat = zero_groups[repeat_key]
            if len(rows_same_repeat) >= 2:
                ordered = sorted(rows_same_repeat, key=lambda row: _parse_timestamp(row.get("flush_start_time")) or datetime.min)
                return_zero_rows.append(
                    build_return_to_zero_summary(
                        process_variant=str(variant),
                        cycle_index=len(return_zero_rows) + 1,
                        first_zero=ordered[0],
                        second_zero=ordered[-1],
                        thresholds=thresholds,
                    )
                )
        return_zero_status = _merge_statuses(item.get("return_to_zero_status") for item in return_zero_rows)
        if return_zero_rows:
            metric_rows.append({"name": "return_to_zero_check", "status": return_zero_status, "value": return_zero_rows})
            if return_zero_status != "pass":
                summary_messages.append("0->1000->0 回零偏差偏大。")
                dominant_candidates.append("切气串扰")

        hold_status = _merge_statuses(row.get("phase_status") for row in variant_holds) if variant_holds else "insufficient_evidence"
        metric_rows.append({"name": "sealed_hold_check", "status": hold_status, "value": None})
        if hold_status == "fail":
            summary_messages.append("sealed hold 漂移异常，更像封路不严或封路后仍有扰动。")
            dominant_candidates.append("封路不严")

        endpoint_map: Dict[int, Dict[int, List[Dict[str, Any]]]] = {}
        for row in variant_points:
            gas = _safe_int(row.get("gas_ppm"))
            pressure = _safe_int(row.get("pressure_target_hpa"))
            if gas is None or pressure is None:
                continue
            endpoint_map.setdefault(pressure, {}).setdefault(gas, []).append(dict(row))
        endpoint_rows: List[Dict[str, Any]] = []
        baseline_norm = None
        for pressure in sorted(endpoint_map.keys(), reverse=True):
            gas_rows = endpoint_map.get(pressure, {})
            if 0 not in gas_rows or 1000 not in gas_rows:
                continue
            zero_ratio = _safe_mean([
                item
                for item in (_safe_float(row.get("analyzer2_co2_ratio_mean")) for row in gas_rows[0])
                if item is not None
            ])
            span_ratio = _safe_mean([
                item
                for item in (_safe_float(row.get("analyzer2_co2_ratio_mean")) for row in gas_rows[1000])
                if item is not None
            ])
            gauge_mean = _safe_mean([
                item
                for item in (
                    _safe_float(row.get("gauge_pressure_mean"))
                    for row in gas_rows[0] + gas_rows[1000]
                )
                if item is not None
            ])
            raw_span = None if zero_ratio is None or span_ratio is None else abs(span_ratio - zero_ratio)
            norm_span = None if raw_span is None or gauge_mean in (None, 0.0) else raw_span / gauge_mean
            if pressure == 1100 and norm_span not in (None, 0.0):
                baseline_norm = norm_span
            retention = None if norm_span is None or baseline_norm in (None, 0.0) else norm_span / baseline_norm
            status = "insufficient_evidence"
            if pressure == 1100 and retention is None:
                status = "pass"
                retention = 1.0 if norm_span is not None else None
            elif retention is not None:
                status = _status_high_good(
                    retention,
                    pass_min=thresholds.normalized_endpoint_span_retention_warn_min,
                    warn_min=thresholds.normalized_endpoint_span_retention_fail_min,
                )
            endpoint_rows.append(
                {
                    "process_variant": variant,
                    "pressure_target_hpa": pressure,
                    "endpoint_span_raw": raw_span,
                    "endpoint_span_norm": norm_span,
                    "endpoint_span_retention_norm": retention,
                    "status": status,
                }
            )
        endpoint_status = _merge_statuses(item.get("status") for item in endpoint_rows)
        metric_rows.append({"name": "normalized_endpoint_span_retention_check", "status": endpoint_status, "value": endpoint_rows})

        repeat_groups = _group_rows_by_keys(variant_points, "layer", "gas_ppm", "pressure_target_hpa")
        repeatability_rows: List[Dict[str, Any]] = []
        for (layer, gas_ppm, pressure_hpa), grouped_rows in sorted(repeat_groups.items(), key=lambda item: (item[0][0], item[0][1], item[0][2])):
            ratios = [
                item
                for item in (
                    _safe_float(row.get("analyzer2_co2_ratio_mean"))
                    for row in grouped_rows
                )
                if item is not None
            ]
            if len(ratios) < 2:
                status = "insufficient_evidence"
                ratio_std = None
            else:
                ratio_std = _safe_std(ratios)
                status = _status_low_good(
                    ratio_std,
                    pass_max=thresholds.repeatability_warn_std,
                    warn_max=thresholds.repeatability_fail_std,
                )
            repeatability_rows.append(
                {
                    "layer": layer,
                    "gas_ppm": gas_ppm,
                    "pressure_target_hpa": pressure_hpa,
                    "repeatability_std": ratio_std,
                    "repeat_count": len(ratios),
                    "status": status,
                }
            )
        repeatability_status = _merge_statuses(item.get("status") for item in repeatability_rows)
        metric_rows.append({"name": "repeatability_check", "status": repeatability_status, "value": repeatability_rows})

        density_reasons: List[str] = []
        insufficient_analyzer = any(
            "insufficient_analyzer_samples" in [str(flag) for flag in row.get("warning_flags", [])]
            for row in variant_points
        ) or any(
            count is not None and count < int(thresholds.stable_sample_count_min)
            for count in (_safe_int(row.get("analyzer_raw_sample_count")) for row in variant_holds + variant_flush)
        )
        insufficient_gauge = any(
            "insufficient_gauge_samples" in [str(flag) for flag in row.get("warning_flags", [])]
            for row in variant_points
        ) or any(
            count is not None and count < int(thresholds.stable_sample_count_min)
            for count in (_safe_int(row.get("gauge_raw_sample_count")) for row in variant_holds + variant_flush)
        )
        insufficient_dewpoint = any(
            "insufficient_dewpoint_samples" in [str(flag) for flag in row.get("warning_flags", [])]
            for row in variant_points
        ) or any(
            count is not None and count < int(thresholds.stable_sample_count_min)
            for count in (_safe_int(row.get("dewpoint_raw_sample_count")) for row in variant_holds + variant_flush)
        )
        if insufficient_analyzer:
            density_reasons.append("insufficient_analyzer_samples")
        if insufficient_gauge:
            density_reasons.append("insufficient_gauge_samples")
        if insufficient_dewpoint:
            density_reasons.append("insufficient_dewpoint_samples")
        evidence_density_ok = not density_reasons
        evidence_density_status = "pass" if evidence_density_ok else "insufficient_evidence"
        metric_rows.append(
            {
                "name": "evidence_density_check",
                "status": evidence_density_status,
                "value": {
                    "evidence_density_ok": evidence_density_ok,
                    "evidence_density_reason": ";".join(density_reasons),
                },
            }
        )
        if not evidence_density_ok:
            summary_messages.append("关键设备原始样本数不足，当前证据密度不够。")
            dominant_candidates.append("证据不足（采样密度不够）")

        pressure_targets = [
            item
            for item in (_safe_float(row.get("pressure_target_hpa")) for row in variant_points)
            if item is not None
        ]
        bias_targets = [
            item
            for item in (_safe_float(row.get("gauge_minus_analyzer_pressure_mean")) for row in variant_points)
            if item is not None
        ]
        bias_mean = _safe_mean(bias_targets)
        bias_std = _safe_std(bias_targets)
        fit = _linear_regression(pressure_targets[: len(bias_targets)], bias_targets)
        bias_status = _merge_statuses(
            [
                _status_low_good(
                    bias_mean,
                    pass_max=thresholds.analyzer_pressure_bias_warn_hpa,
                    warn_max=thresholds.analyzer_pressure_bias_fail_hpa,
                ),
                _status_low_good(
                    fit.get("slope"),
                    pass_max=thresholds.analyzer_pressure_bias_slope_warn_abs,
                    warn_max=thresholds.analyzer_pressure_bias_slope_fail_abs,
                ),
            ]
        )
        metric_rows.append(
            {
                "name": "analyzer_pressure_bias_check",
                "status": bias_status,
                "value": {
                    "analyzer_gauge_pressure_bias_mean": bias_mean,
                    "analyzer_gauge_pressure_bias_std": bias_std,
                    "analyzer_gauge_pressure_bias_vs_pressure_slope": fit.get("slope"),
                },
            }
        )
        if bias_status != "pass":
            summary_messages.append("analyzer pressure 与数字压力计存在稳定偏移，更像 analyzer pressure bias。")
            dominant_candidates.append("analyzer pressure bias")

        point_window_status = _merge_statuses(row.get("phase_status") for row in variant_points) if variant_points else "insufficient_evidence"
        metric_rows.append({"name": "pressure_point_window_check", "status": point_window_status, "value": None})
        if point_window_status == "fail":
            summary_messages.append("压力点稳态窗口仍有 ratio/dewpoint/pressure 漂移，动态控压阶段不够干净。")
            dominant_candidates.append("动态控压混气")

        if endpoint_status != "pass":
            summary_messages.append("pressure-normalized endpoint span retention 存在压缩。")
            dominant_candidates.append("动态控压混气")
        if repeatability_status != "pass":
            summary_messages.append("重复性不足。")
            dominant_candidates.append("证据不足（采样密度不够）")
        if hold_status != "pass":
            dominant_candidates.append("封路不严")

        layer_statuses: Dict[int, str] = {}
        all_layers = sorted({
            _safe_int(row.get("layer"))
            for row in variant_flush + variant_holds + variant_points
            if _safe_int(row.get("layer")) is not None
        })
        for layer in all_layers:
            one_statuses: List[str] = []
            if layer == 1:
                one_statuses.extend([
                    flush_vent_status,
                    flush_duration_status,
                    flush_gate_status,
                    return_zero_status if return_zero_rows else "insufficient_evidence",
                ])
            elif layer == 2:
                one_statuses.extend([flush_gate_status, hold_status])
            elif layer in {3, 4}:
                one_statuses.extend([
                    flush_gate_status,
                    hold_status if layer == 3 else "pass",
                    point_window_status,
                    endpoint_status,
                    repeatability_status,
                    bias_status,
                ])
            layer_statuses[int(layer)] = _merge_statuses(one_statuses)

        can_enter_layer4 = all(layer_statuses.get(layer) == "pass" for layer in (1, 2, 3) if layer in layer_statuses)
        if can_enter_layer4:
            layer4_eligible_variants.append(str(variant))
        classification = _merge_statuses(metric.get("status") for metric in metric_rows)
        if classification == "pass" and any(layer in layer_statuses for layer in (1, 2, 3)) and not can_enter_layer4:
            classification = "warn"

        if not dominant_candidates:
            dominant_candidates.append("证据不足（采样密度不够）" if classification == "insufficient_evidence" else "切气串扰")
        dominant_error = max(sorted(set(dominant_candidates)), key=dominant_candidates.count)
        biggest_error_votes[dominant_error] = biggest_error_votes.get(dominant_error, 0) + 1

        next_best_change = "analyzer 高频采样接入"
        if flush_gate_status != "pass":
            next_best_change = "flush gate"
        elif hold_status != "pass":
            next_best_change = "seal_trigger_hpa"
        elif point_window_status != "pass":
            next_best_change = "ramp_down_rate"
        elif repeatability_status != "pass":
            next_best_change = "stable window"

        variant_summary = {
            "process_variant": variant,
            "classification": classification,
            "layer_statuses": layer_statuses,
            "can_enter_layer4": can_enter_layer4,
            "dominant_error_source": dominant_error,
            "next_best_change": next_best_change,
            "evidence_density_ok": evidence_density_ok,
            "evidence_density_reason": ";".join(density_reasons),
            "metrics": metric_rows,
            "summary_messages": summary_messages,
            "return_to_zero": return_zero_rows,
        }
        variant_summaries.append(variant_summary)
        overall_statuses.append(classification)
        rank = _variant_rank(classification)
        if rank < best_rank:
            best_rank = rank
            best_variant = str(variant)

    overall_classification = _merge_statuses(overall_statuses)
    dominant_error_source = None
    if biggest_error_votes:
        dominant_error_source = max(sorted(biggest_error_votes), key=lambda key: biggest_error_votes[key])
    ranked_variants = sorted(
        variant_summaries,
        key=lambda item: (
            _variant_rank(str(item.get("classification") or "")),
            0 if bool(item.get("evidence_density_ok")) else 1,
            0 if bool(item.get("can_enter_layer4")) else 1,
            str(item.get("process_variant") or ""),
        ),
    )
    recommended_variant = None
    recommendation_confidence = "low"
    recommendation_reason = "insufficient evidence or no qualified variant"
    recommendation_basis = [
        "layer_status",
        "repeatability",
        "sealed_hold_drift",
        "normalized_endpoint_span_retention",
        "analyzer_pressure_bias",
        "evidence_density",
    ]
    eligible_for_layer4 = any(item.get("can_enter_layer4") for item in variant_summaries)
    if ranked_variants:
        top = ranked_variants[0]
        second = ranked_variants[1] if len(ranked_variants) > 1 else None
        top_class = str(top.get("classification") or "")
        top_evidence_ok = bool(top.get("evidence_density_ok"))
        clearly_better = second is None or _variant_rank(top_class) < _variant_rank(str(second.get("classification") or ""))
        if top_class == "pass":
            recommended_variant = str(top.get("process_variant"))
            recommendation_confidence = "high" if bool(top.get("can_enter_layer4")) and top_evidence_ok else "medium"
            recommendation_reason = "qualified variant passed current layers"
        elif top_class == "warn" and top_evidence_ok and clearly_better:
            recommended_variant = str(top.get("process_variant"))
            recommendation_confidence = "medium"
            recommendation_reason = "warn but evidence is dense and variant is clearly better than peers"
        elif top_class == "warn" and not top_evidence_ok:
            recommendation_reason = "top warn variant still lacks enough evidence density"
        elif top_class in {"fail", "insufficient_evidence"}:
            recommendation_reason = "insufficient evidence or no qualified variant"

    evidence_density_ok = bool(ranked_variants) and bool(ranked_variants[0].get("evidence_density_ok")) if recommended_variant else False
    evidence_density_reason = (
        next(
            (
                str(item.get("evidence_density_reason") or "")
                for item in variant_summaries
                if str(item.get("process_variant")) == str(recommended_variant)
            ),
            "",
        )
        if recommended_variant is not None
        else "insufficient evidence or no qualified variant"
    )

    missing_evidence: List[str] = []
    if any(metric.get("status") == "insufficient_evidence" for variant in variant_summaries for metric in variant.get("metrics", [])):
        missing_evidence.append("关键阶段存在 insufficient_evidence，需补足稳定窗口样本数或高频 analyzer 采样。")
    if not any(_safe_int(row.get("layer")) == 4 for row in point_rows):
        missing_evidence.append("尚未完成 Layer 4 全矩阵确认。")
    if not any(_safe_int(row.get("repeat_index")) == 3 for row in point_rows):
        missing_evidence.append("代表点 3 次重复性证据仍不完整。")

    return {
        "classification": overall_classification,
        "recommended_variant": recommended_variant,
        "recommendation_confidence": recommendation_confidence,
        "recommendation_reason": recommendation_reason,
        "recommendation_basis": recommendation_basis,
        "eligible_for_layer4": eligible_for_layer4,
        "eligible_variants_for_layer4": layer4_eligible_variants,
        "dominant_error_source": dominant_error_source,
        "evidence_density_ok": evidence_density_ok,
        "evidence_density_reason": evidence_density_reason,
        "variant_summaries": variant_summaries,
        "flush_summaries": flush_rows,
        "seal_hold_summaries": hold_rows,
        "pressure_summaries": point_rows,
        "phase_gate_summary": gate_rows,
        "thresholds": asdict(thresholds),
        "min_flush_s": float(min_flush_s),
        "target_flush_s": float(target_flush_s),
        "expected_deadtime_s": float(expected_deadtime_s),
        "missing_evidence": missing_evidence,
        "final_report_answers": {
            "best_variant": recommended_variant,
            "recommendation_confidence": recommendation_confidence,
            "recommendation_reason": recommendation_reason,
            "recommendation_basis": recommendation_basis,
            "dominant_error_source": dominant_error_source,
            "eligible_for_layer4": eligible_for_layer4,
            "eligible_variants_for_layer4": layer4_eligible_variants,
            "next_best_change": (
                next(
                    (
                        item.get("next_best_change")
                        for item in variant_summaries
                        if item.get("process_variant") == recommended_variant
                    ),
                    "analyzer 高频采样接入",
                )
                if recommended_variant is not None
                else "analyzer 高频采样接入"
            ),
            "missing_evidence": missing_evidence,
        },
    }


def build_readable_report(
    *,
    diagnostic_summary: Mapping[str, Any],
    flush_summaries: Sequence[Mapping[str, Any]],
    seal_hold_summaries: Sequence[Mapping[str, Any]],
    pressure_summaries: Sequence[Mapping[str, Any]],
    phase_gate_rows: Sequence[Mapping[str, Any]],
    plot_files: Optional[Mapping[str, Any]] = None,
) -> str:
    lines: List[str] = []
    lines.append("# 封路控压计量级资格确认诊断 V2 报告")
    lines.append(f"- 生成时间: {datetime.now().isoformat(timespec='seconds')}")
    lines.append(f"- 总体结论: {str(diagnostic_summary.get('classification') or 'unknown').upper()}")
    lines.append(f"- 推荐继续验证的候选工艺: {diagnostic_summary.get('recommended_variant')}")
    lines.append(f"- recommendation_confidence: {diagnostic_summary.get('recommendation_confidence')}")
    lines.append(f"- recommendation_reason: {diagnostic_summary.get('recommendation_reason')}")
    lines.append(f"- 具备进入 Layer 4 资格的工艺: {diagnostic_summary.get('eligible_variants_for_layer4')}")
    lines.append(f"- 是否满足进入 Layer 4: {diagnostic_summary.get('eligible_for_layer4')}")
    lines.append(f"- 当前更像的最大误差源: {diagnostic_summary.get('dominant_error_source')}")
    lines.append(f"- evidence_density_ok: {diagnostic_summary.get('evidence_density_ok')}")
    lines.append(f"- evidence_density_reason: {diagnostic_summary.get('evidence_density_reason')}")
    lines.append("- 若出现露点突变、ratio 折断、压力回弹，请先对照 actuation_events.csv 排查动作时序。")
    lines.append("")
    lines.append("## 分层结论")
    for item in diagnostic_summary.get("variant_summaries", []) or []:
        lines.append(
            f"- 工艺 {item.get('process_variant')}: "
            f"classification={item.get('classification')} "
            f"layers={item.get('layer_statuses')} "
            f"can_enter_layer4={item.get('can_enter_layer4')} "
            f"dominant_error_source={item.get('dominant_error_source')} "
            f"skipped_layers={item.get('skipped_layers')} "
            f"skipped_due_to_previous_layer_failure={item.get('skipped_due_to_previous_layer_failure')} "
            f"evidence_density_ok={item.get('evidence_density_ok')}"
        )
        for text in item.get("summary_messages", []) or []:
            lines.append(f"  说明: {text}")
    lines.append("")
    lines.append("## 最终报告回答")
    answers = diagnostic_summary.get("final_report_answers", {}) or {}
    lines.append(f"1. 最适合继续逼近计量校准级别的工艺版本: {answers.get('best_variant')}")
    lines.append(f"   recommendation_confidence: {answers.get('recommendation_confidence')}")
    lines.append(f"   recommendation_reason: {answers.get('recommendation_reason')}")
    lines.append(f"   recommendation_basis: {answers.get('recommendation_basis')}")
    lines.append(f"2. 当前最大的误差源更像是: {answers.get('dominant_error_source')}")
    lines.append(f"3. 当前是否满足进入 Layer 4: {answers.get('eligible_for_layer4')}")
    lines.append(f"4. 下一步最值钱的改动: {answers.get('next_best_change')}")
    lines.append("5. 仍缺的证据:")
    for item in answers.get("missing_evidence", []) or []:
        lines.append(f"   - {item}")
    lines.append("")
    lines.append("## Flush Summary")
    for row in flush_summaries:
        lines.append(
            f"- variant={row.get('process_variant')} layer={row.get('layer')} repeat={row.get('repeat_index')} gas={row.get('gas_ppm')} "
            f"flush={row.get('flush_duration_s')}s gate={row.get('flush_gate_status')} "
            f"t95={row.get('flush_ratio_t95_s')} pressure_rise={row.get('flush_pressure_rise_hpa')} "
            f"flags={';'.join(row.get('flush_warning_flags', []))}"
        )
    lines.append("")
    lines.append("## Sealed Hold Summary")
    for row in seal_hold_summaries:
        lines.append(
            f"- variant={row.get('process_variant')} repeat={row.get('repeat_index')} gas={row.get('gas_ppm')} "
            f"hold={row.get('hold_duration_s')}s status={row.get('phase_status')} "
            f"pressure_drift_per_min={row.get('hold_pressure_drift_hpa_per_min')} "
            f"dewpoint_drift_per_min={row.get('hold_dewpoint_drift_c_per_min')}"
        )
    lines.append("")
    lines.append("## Pressure Point Summary")
    for row in pressure_summaries:
        lines.append(
            f"- variant={row.get('process_variant')} layer={row.get('layer')} repeat={row.get('repeat_index')} "
            f"gas={row.get('gas_ppm')} pressure={row.get('pressure_target_hpa')} "
            f"status={row.get('phase_status')} sample_count={row.get('stable_sample_count')} "
            f"ratio_mean={row.get('analyzer2_co2_ratio_mean')} ratio_slope={row.get('point_window_ratio_slope_per_s')} "
            f"dewpoint_slope={row.get('point_window_dewpoint_slope_per_s')}"
        )
    lines.append("")
    lines.append("## Phase Gate Summary")
    for row in phase_gate_rows:
        lines.append(
            f"- variant={row.get('process_variant')} layer={row.get('layer')} repeat={row.get('repeat_index')} "
            f"phase={row.get('phase')} gate={row.get('gate_name')} status={row.get('gate_status')} "
            f"fail_reason={row.get('gate_fail_reason')}"
        )
    lines.append("")
    lines.append("## 推荐真机启动顺序")
    lines.append("- Phase S1: python run_room_temp_co2_pressure_diagnostic.py --allow-live-hardware --smoke-level s1")
    lines.append("- Phase S2: python run_room_temp_co2_pressure_diagnostic.py --allow-live-hardware --smoke-level s2")
    lines.append("- Phase S3: python run_room_temp_co2_pressure_diagnostic.py --allow-live-hardware --smoke-level s3")
    lines.append("- Phase S4: python run_room_temp_co2_pressure_diagnostic.py --allow-live-hardware --smoke-level screen")
    lines.append("- Phase S5: 只有 eligible_for_layer4 = true 时，再跑 --smoke-level layer4 或等价 Layer 4 命令。")
    if plot_files:
        lines.append("")
        lines.append("## 图文件")
        for key, value in plot_files.items():
            if isinstance(value, Mapping):
                for inner_key, inner_value in value.items():
                    lines.append(f"- {key}.{inner_key}: {inner_value}")
            else:
                lines.append(f"- {key}: {value}")
    return "\n".join(lines) + "\n"


def build_readable_report_v2(
    *,
    diagnostic_summary: Mapping[str, Any],
    precondition_summaries: Sequence[Mapping[str, Any]],
    flush_summaries: Sequence[Mapping[str, Any]],
    seal_hold_summaries: Sequence[Mapping[str, Any]],
    pressure_summaries: Sequence[Mapping[str, Any]],
    phase_gate_rows: Sequence[Mapping[str, Any]],
    plot_files: Optional[Mapping[str, Any]] = None,
) -> str:
    lines: List[str] = []
    lines.append("# 封路控压计量级资格确认诊断 V2 报告")
    lines.append(f"- 生成时间: {datetime.now().isoformat(timespec='seconds')}")
    lines.append(f"- 总体结论: {str(diagnostic_summary.get('classification') or 'unknown').upper()}")
    lines.append(f"- 推荐继续验证的候选工艺: {diagnostic_summary.get('recommended_variant')}")
    lines.append(f"- recommendation_confidence: {diagnostic_summary.get('recommendation_confidence')}")
    lines.append(f"- recommendation_reason: {diagnostic_summary.get('recommendation_reason')}")
    lines.append(f"- 具备进入 Layer 4 资格的工艺: {diagnostic_summary.get('eligible_variants_for_layer4')}")
    lines.append(f"- 是否满足进入 Layer 4: {diagnostic_summary.get('eligible_for_layer4')}")
    lines.append(f"- 当前更像的最大误差源: {diagnostic_summary.get('dominant_error_source')}")
    lines.append(f"- evidence_density_ok: {diagnostic_summary.get('evidence_density_ok')}")
    lines.append(f"- evidence_density_reason: {diagnostic_summary.get('evidence_density_reason')}")
    lines.append(f"- run_fail_reason: {diagnostic_summary.get('run_fail_reason')}")
    lines.append("- 若出现露点突变、ratio 折断、压力回弹，请先对照 actuation_events.csv 和 flush_gate_trace.csv 排查动作时序与 gate 子项。")
    lines.append(
        "- 首档 0 ppm 预调理不计入 Layer 1 正式评分；若 ratio/gauge 已稳而 dewpoint 在 VENT ON 且无新动作下仍明显反弹，"
        "会优先标记为 source gas moisture / line moisture memory / dewpoint meter dynamic suspicion，而不是直接判成 leak。"
    )
    lines.append("")
    if precondition_summaries:
        lines.append("## Precondition Summary")
        for row in precondition_summaries:
            lines.append(
                f"- gas={row.get('gas_ppm')} duration={row.get('flush_duration_s')}s "
                f"status={row.get('precondition_status')} fail_reason={row.get('precondition_fail_reason')} "
                f"rebound={row.get('dewpoint_rebound_detected')} rebound_note={row.get('rebound_note')}"
            )
        lines.append("")
    lines.append("## 分层结论")
    for item in diagnostic_summary.get("variant_summaries", []) or []:
        lines.append(
            f"- 工艺 {item.get('process_variant')}: "
            f"classification={item.get('classification')} "
            f"layers={item.get('layer_statuses')} "
            f"can_enter_layer4={item.get('can_enter_layer4')} "
            f"dominant_error_source={item.get('dominant_error_source')} "
            f"skipped_layers={item.get('skipped_layers')} "
            f"skipped_due_to_previous_layer_failure={item.get('skipped_due_to_previous_layer_failure')} "
            f"evidence_density_ok={item.get('evidence_density_ok')}"
        )
        for text in item.get("summary_messages", []) or []:
            lines.append(f"  说明: {text}")
    lines.append("")
    lines.append("## 最终报告回答")
    answers = diagnostic_summary.get("final_report_answers", {}) or {}
    lines.append(f"1. 最适合继续逼近计量校准级别的工艺版本: {answers.get('best_variant')}")
    lines.append(f"   recommendation_confidence: {answers.get('recommendation_confidence')}")
    lines.append(f"   recommendation_reason: {answers.get('recommendation_reason')}")
    lines.append(f"   recommendation_basis: {answers.get('recommendation_basis')}")
    lines.append(f"2. 当前最大的误差源更像是: {answers.get('dominant_error_source')}")
    lines.append(f"3. 当前是否满足进入 Layer 4: {answers.get('eligible_for_layer4')}")
    lines.append(f"4. 下一步最值钱的改动: {answers.get('next_best_change')}")
    lines.append("5. 仍缺的证据:")
    for item in answers.get("missing_evidence", []) or []:
        lines.append(f"   - {item}")
    lines.append("")
    lines.append("## Flush Summary")
    for row in flush_summaries:
        lines.append(
            f"- variant={row.get('process_variant')} layer={row.get('layer')} repeat={row.get('repeat_index')} gas={row.get('gas_ppm')} "
            f"flush={row.get('flush_duration_s')}s gate={row.get('flush_gate_status')} "
            f"t95={row.get('flush_ratio_t95_s')} pressure_rise={row.get('flush_pressure_rise_hpa')} "
            f"flags={';'.join(row.get('flush_warning_flags', []))}"
        )
    lines.append("")
    lines.append("## Sealed Hold Summary")
    for row in seal_hold_summaries:
        lines.append(
            f"- variant={row.get('process_variant')} repeat={row.get('repeat_index')} gas={row.get('gas_ppm')} "
            f"hold={row.get('hold_duration_s')}s status={row.get('phase_status')} "
            f"pressure_drift_per_min={row.get('hold_pressure_drift_hpa_per_min')} "
            f"dewpoint_drift_per_min={row.get('hold_dewpoint_drift_c_per_min')}"
        )
    lines.append("")
    lines.append("## Pressure Point Summary")
    for row in pressure_summaries:
        lines.append(
            f"- variant={row.get('process_variant')} layer={row.get('layer')} repeat={row.get('repeat_index')} "
            f"gas={row.get('gas_ppm')} pressure={row.get('pressure_target_hpa')} "
            f"status={row.get('phase_status')} sample_count={row.get('stable_sample_count')} "
            f"ratio_mean={row.get('analyzer2_co2_ratio_mean')} ratio_slope={row.get('point_window_ratio_slope_per_s')} "
            f"dewpoint_slope={row.get('point_window_dewpoint_slope_per_s')}"
        )
    lines.append("")
    lines.append("## Phase Gate Summary")
    for row in phase_gate_rows:
        lines.append(
            f"- variant={row.get('process_variant')} layer={row.get('layer')} repeat={row.get('repeat_index')} "
            f"phase={row.get('phase')} gate={row.get('gate_name')} status={row.get('gate_status')} "
            f"fail_reason={row.get('gate_fail_reason')}"
        )
    lines.append("")
    lines.append("## 推荐真机启动顺序")
    lines.append("- Phase S1: python run_room_temp_co2_pressure_diagnostic.py --allow-live-hardware --smoke-level s1")
    lines.append("- Phase S2: python run_room_temp_co2_pressure_diagnostic.py --allow-live-hardware --smoke-level s2")
    lines.append("- Phase S3: python run_room_temp_co2_pressure_diagnostic.py --allow-live-hardware --smoke-level s3")
    lines.append("- Phase S4: python run_room_temp_co2_pressure_diagnostic.py --allow-live-hardware --smoke-level screen")
    lines.append("- Phase S5: 只有 eligible_for_layer4 = true 时，再跑 --smoke-level layer4 或等价 Layer 4 命令。")
    if plot_files:
        lines.append("")
        lines.append("## 图文件")
        for key, value in plot_files.items():
            if isinstance(value, Mapping):
                for inner_key, inner_value in value.items():
                    lines.append(f"- {key}.{inner_key}: {inner_value}")
            else:
                lines.append(f"- {key}: {value}")
    return "\n".join(lines) + "\n"


def _csv_rows(rows: Sequence[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    normalized: List[Dict[str, Any]] = []
    for row in rows:
        converted: Dict[str, Any] = {}
        for key, value in dict(row).items():
            if isinstance(value, list):
                converted[str(key)] = ";".join(str(item) for item in value)
            elif isinstance(value, dict):
                converted[str(key)] = json.dumps(value, ensure_ascii=False)
            else:
                converted[str(key)] = value
        normalized.append(converted)
    return normalized


def _write_csv(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    normalized = _csv_rows(rows)
    header: List[str] = []
    for row in normalized:
        for key in row.keys():
            if key not in header:
                header.append(str(key))
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=header)
        writer.writeheader()
        for row in normalized:
            writer.writerow(row)


def _write_workbook(
    path: Path,
    *,
    raw_rows: Sequence[Mapping[str, Any]],
    aligned_rows: Sequence[Mapping[str, Any]],
    actuation_events: Sequence[Mapping[str, Any]],
    flush_summaries: Sequence[Mapping[str, Any]],
    seal_hold_summaries: Sequence[Mapping[str, Any]],
    pressure_summaries: Sequence[Mapping[str, Any]],
    phase_gate_rows: Sequence[Mapping[str, Any]],
    diagnostic_summary: Mapping[str, Any],
) -> None:
    import pandas as pd

    path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(path) as writer:
        pd.DataFrame(_csv_rows(raw_rows)).to_excel(writer, sheet_name="raw_timeseries", index=False)
        pd.DataFrame(_csv_rows(aligned_rows)).to_excel(writer, sheet_name="aligned_timeseries", index=False)
        pd.DataFrame(_csv_rows(actuation_events)).to_excel(writer, sheet_name="actuation_events", index=False)
        pd.DataFrame(_csv_rows(flush_summaries)).to_excel(writer, sheet_name="flush_summary", index=False)
        pd.DataFrame(_csv_rows(seal_hold_summaries)).to_excel(writer, sheet_name="seal_hold_summary", index=False)
        pd.DataFrame(_csv_rows(pressure_summaries)).to_excel(writer, sheet_name="pressure_summary", index=False)
        pd.DataFrame(_csv_rows(phase_gate_rows)).to_excel(writer, sheet_name="phase_gate_summary", index=False)
        pd.DataFrame(_csv_rows(diagnostic_summary.get("variant_summaries", []) or [])).to_excel(
            writer,
            sheet_name="variant_summary",
            index=False,
        )


def _write_workbook_v2(
    path: Path,
    *,
    raw_rows: Sequence[Mapping[str, Any]],
    aligned_rows: Sequence[Mapping[str, Any]],
    actuation_events: Sequence[Mapping[str, Any]],
    flush_gate_trace_rows: Sequence[Mapping[str, Any]],
    precondition_summaries: Sequence[Mapping[str, Any]],
    flush_summaries: Sequence[Mapping[str, Any]],
    seal_hold_summaries: Sequence[Mapping[str, Any]],
    pressure_summaries: Sequence[Mapping[str, Any]],
    phase_gate_rows: Sequence[Mapping[str, Any]],
    diagnostic_summary: Mapping[str, Any],
) -> None:
    import pandas as pd

    path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(path) as writer:
        pd.DataFrame(_csv_rows(raw_rows)).to_excel(writer, sheet_name="raw_timeseries", index=False)
        pd.DataFrame(_csv_rows(aligned_rows)).to_excel(writer, sheet_name="aligned_timeseries", index=False)
        pd.DataFrame(_csv_rows(actuation_events)).to_excel(writer, sheet_name="actuation_events", index=False)
        pd.DataFrame(_csv_rows(flush_gate_trace_rows)).to_excel(writer, sheet_name="flush_gate_trace", index=False)
        pd.DataFrame(_csv_rows(precondition_summaries)).to_excel(writer, sheet_name="precondition_summary", index=False)
        pd.DataFrame(_csv_rows(flush_summaries)).to_excel(writer, sheet_name="flush_summary", index=False)
        pd.DataFrame(_csv_rows(seal_hold_summaries)).to_excel(writer, sheet_name="seal_hold_summary", index=False)
        pd.DataFrame(_csv_rows(pressure_summaries)).to_excel(writer, sheet_name="pressure_summary", index=False)
        pd.DataFrame(_csv_rows(phase_gate_rows)).to_excel(writer, sheet_name="phase_gate_summary", index=False)
        pd.DataFrame(_csv_rows(diagnostic_summary.get("variant_summaries", []) or [])).to_excel(
            writer,
            sheet_name="variant_summary",
            index=False,
        )


def export_room_temp_diagnostic_results(
    output_dir: str | Path,
    *,
    raw_rows: Sequence[Mapping[str, Any]],
    aligned_rows: Sequence[Mapping[str, Any]],
    actuation_events: Sequence[Mapping[str, Any]],
    flush_gate_trace_rows: Sequence[Mapping[str, Any]],
    precondition_summaries: Sequence[Mapping[str, Any]],
    flush_summaries: Sequence[Mapping[str, Any]],
    seal_hold_summaries: Sequence[Mapping[str, Any]],
    pressure_summaries: Sequence[Mapping[str, Any]],
    phase_gate_rows: Sequence[Mapping[str, Any]],
    diagnostic_summary: Mapping[str, Any],
    export_csv: bool = True,
    export_xlsx: bool = True,
    export_png: bool = True,
) -> Dict[str, Path]:
    root = Path(output_dir).resolve()
    root.mkdir(parents=True, exist_ok=True)

    raw_path = root / "raw_timeseries.csv"
    aligned_path = root / "aligned_timeseries.csv"
    actuation_path = root / "actuation_events.csv"
    flush_gate_trace_path = root / "flush_gate_trace.csv"
    flush_path = root / "flush_summary.csv"
    hold_path = root / "seal_hold_summary.csv"
    pressure_path = root / "pressure_point_summary.csv"
    gate_path = root / "phase_gate_summary.csv"
    summary_path = root / "diagnostic_summary.json"
    report_path = root / "readable_report.md"
    workbook_path = root / "diagnostic_workbook.xlsx"

    if export_csv:
        _write_csv(raw_path, raw_rows)
        _write_csv(aligned_path, aligned_rows)
        _write_csv(actuation_path, actuation_events)
        _write_csv(flush_gate_trace_path, flush_gate_trace_rows)
        _write_csv(flush_path, flush_summaries)
        _write_csv(hold_path, seal_hold_summaries)
        _write_csv(pressure_path, pressure_summaries)
        _write_csv(gate_path, phase_gate_rows)

    plot_outputs: Dict[str, Any] = {}
    if export_png:
        from .room_temp_co2_pressure_plots import generate_room_temp_diagnostic_plots

        plot_outputs = generate_room_temp_diagnostic_plots(
            root,
            raw_rows=raw_rows,
            flush_summaries=flush_summaries,
            seal_hold_summaries=seal_hold_summaries,
            pressure_summaries=pressure_summaries,
            diagnostic_summary=diagnostic_summary,
        )

    summary_payload = dict(diagnostic_summary)
    summary_payload["plot_files"] = {
        key: (
            {inner_key: str(inner_value) for inner_key, inner_value in value.items()}
            if isinstance(value, Mapping)
            else str(value)
        )
        for key, value in plot_outputs.items()
    }
    summary_path.write_text(json.dumps(summary_payload, ensure_ascii=False, indent=2), encoding="utf-8")
    report_path.write_text(
        build_readable_report_v2(
            diagnostic_summary=summary_payload,
            precondition_summaries=precondition_summaries,
            flush_summaries=flush_summaries,
            seal_hold_summaries=seal_hold_summaries,
            pressure_summaries=pressure_summaries,
            phase_gate_rows=phase_gate_rows,
            plot_files=summary_payload["plot_files"],
        ),
        encoding="utf-8",
    )

    if export_xlsx:
        _write_workbook_v2(
            workbook_path,
            raw_rows=raw_rows,
            aligned_rows=aligned_rows,
            actuation_events=actuation_events,
            flush_gate_trace_rows=flush_gate_trace_rows,
            precondition_summaries=precondition_summaries,
            flush_summaries=flush_summaries,
            seal_hold_summaries=seal_hold_summaries,
            pressure_summaries=pressure_summaries,
            phase_gate_rows=phase_gate_rows,
            diagnostic_summary=summary_payload,
        )

    outputs: Dict[str, Path] = {
        "output_dir": root,
        "raw_timeseries": raw_path,
        "aligned_timeseries": aligned_path,
        "actuation_events": actuation_path,
        "flush_gate_trace": flush_gate_trace_path,
        "flush_summary": flush_path,
        "seal_hold_summary": hold_path,
        "pressure_point_summary": pressure_path,
        "phase_gate_summary": gate_path,
        "diagnostic_summary": summary_path,
        "readable_report": report_path,
        "diagnostic_workbook": workbook_path,
    }
    outputs.update(plot_outputs)
    return outputs


def _chain_mode_rank(value: Any) -> int:
    status = str(value or "").strip().lower()
    order = {"pass": 0, "warn": 1, "fail": 2, "insufficient_evidence": 3}
    return order.get(status, 9)


def _chain_mode_flags(chain_mode: str) -> Dict[str, bool]:
    normalized = str(chain_mode or "").strip()
    return {
        "analyzer_chain_connected": normalized in {"analyzer_in_keep_rest", "analyzer_in_pace_out_keep_rest"},
        "pace_in_path": normalized not in {"analyzer_in_pace_out_keep_rest", "analyzer_out_pace_out_keep_rest"},
        "pace_expected_vent_on": normalized not in {"analyzer_in_pace_out_keep_rest", "analyzer_out_pace_out_keep_rest"},
        "valve_block_in_path": True,
        "dewpoint_meter_in_path": True,
        "gauge_in_path": True,
    }


def build_analyzer_chain_isolation_summary(
    flush_summary: Mapping[str, Any],
    *,
    run_id: str,
    smoke_level: str,
    chain_mode: str,
    setup_metadata: Mapping[str, Any],
    thresholds: MetrologyDiagnosticThresholds = DEFAULT_THRESHOLDS,
) -> Dict[str, Any]:
    analyzer_connected = bool(setup_metadata.get("analyzer_chain_connected"))
    gas_analyzer_skipped = bool(setup_metadata.get("gas_analyzer_skipped"))
    analyzer_sampling_enabled = bool(
        setup_metadata.get("analyzer_sampling_enabled", analyzer_connected and not gas_analyzer_skipped)
    )
    analyzer_count_in_path = _safe_int(setup_metadata.get("analyzer_count_in_path")) or (8 if analyzer_connected else 0)
    evidence_reasons: List[str] = []
    if (_safe_int(flush_summary.get("dewpoint_raw_sample_count")) or 0) < int(thresholds.stable_sample_count_min):
        evidence_reasons.append("insufficient_dewpoint_samples")
    if (_safe_int(flush_summary.get("gauge_raw_sample_count")) or 0) < int(thresholds.stable_sample_count_min):
        evidence_reasons.append("insufficient_gauge_samples")
    if analyzer_sampling_enabled and (_safe_int(flush_summary.get("analyzer_raw_sample_count")) or 0) < int(thresholds.stable_sample_count_min):
        evidence_reasons.append("insufficient_analyzer_samples")

    if evidence_reasons:
        classification = "insufficient_evidence"
    else:
        classification = str(flush_summary.get("flush_gate_status") or "insufficient_evidence")

    time_to_gate = _safe_float(flush_summary.get("flush_duration_s")) if bool(flush_summary.get("flush_gate_pass")) else None
    ratio_tail_span = _safe_float(flush_summary.get("flush_last60s_ratio_span")) if analyzer_sampling_enabled else None
    ratio_tail_slope = _safe_float(flush_summary.get("flush_last60s_ratio_slope")) if analyzer_sampling_enabled else None

    return {
        "run_id": run_id,
        "smoke_level": smoke_level,
        "process_variant": flush_summary.get("process_variant"),
        "chain_mode": chain_mode,
        "case_name": _chain_mode_case_name(chain_mode),
        "chain_label": setup_metadata.get("chain_label"),
        "analyzer_count_in_path": analyzer_count_in_path,
        "analyzer_chain_connected": analyzer_connected,
        "analyzer_sampling_enabled": analyzer_sampling_enabled,
        "gas_analyzer_skipped": gas_analyzer_skipped,
        "gas_analyzer_skip_reason": str(setup_metadata.get("gas_analyzer_skip_reason") or ""),
        "analyzers_in_path_text": setup_metadata.get("analyzers_in_path_text"),
        "capture_analyzer_name": setup_metadata.get("capture_analyzer_name"),
        "capture_analyzer_port": setup_metadata.get("capture_analyzer_port"),
        "pace_in_path": bool(setup_metadata.get("pace_in_path")),
        "controller_vent_expected": bool(setup_metadata.get("controller_vent_expected", setup_metadata.get("pace_expected_vent_on"))),
        "controller_vent_state": setup_metadata.get("controller_vent_state"),
        "flush_vent_refresh_interval_s": _safe_float(setup_metadata.get("flush_vent_refresh_interval_s")),
        "flush_vent_refresh_interval_s_requested": _safe_float(setup_metadata.get("flush_vent_refresh_interval_s_requested")),
        "flush_vent_refresh_interval_s_actual_mean": _safe_float(setup_metadata.get("flush_vent_refresh_interval_s_actual_mean")),
        "flush_vent_refresh_interval_s_actual_max": _safe_float(setup_metadata.get("flush_vent_refresh_interval_s_actual_max")),
        "flush_vent_refresh_count": _safe_int(setup_metadata.get("flush_vent_refresh_count")),
        "flush_vent_refresh_thread_used": bool(setup_metadata.get("flush_vent_refresh_thread_used")),
        "classification": classification,
        "flush_gate_status": flush_summary.get("flush_gate_status"),
        "flush_gate_pass": bool(flush_summary.get("flush_gate_pass")),
        "flush_gate_fail_reason": flush_summary.get("flush_gate_fail_reason"),
        "vent_state_during_flush": flush_summary.get("vent_state_during_flush"),
        "flush_duration_s": _safe_float(flush_summary.get("flush_duration_s")),
        "dewpoint_tail_span_60s": _safe_float(flush_summary.get("flush_last60s_dewpoint_span")),
        "dewpoint_tail_slope_60s": _safe_float(flush_summary.get("flush_last60s_dewpoint_slope")),
        "dewpoint_time_to_gate": time_to_gate,
        "dewpoint_rebound_detected": bool(flush_summary.get("dewpoint_rebound_detected")),
        "rebound_rise_c": _safe_float(flush_summary.get("rebound_rise_c")),
        "gauge_tail_span_60s": _safe_float(flush_summary.get("flush_last60s_gauge_span_hpa")),
        "gauge_tail_slope_60s": _safe_float(flush_summary.get("flush_last60s_gauge_slope_hpa_per_s")),
        "ratio_tail_span_60s": ratio_tail_span,
        "ratio_tail_slope_60s": ratio_tail_slope,
        "analyzer_raw_sample_count": _safe_int(flush_summary.get("analyzer_raw_sample_count")),
        "gauge_raw_sample_count": _safe_int(flush_summary.get("gauge_raw_sample_count")),
        "dewpoint_raw_sample_count": _safe_int(flush_summary.get("dewpoint_raw_sample_count")),
        "aligned_sample_count": _safe_int(flush_summary.get("aligned_sample_count")),
        "evidence_density_ok": not evidence_reasons,
        "evidence_density_reason": ";".join(evidence_reasons),
        "flush_warning_flags": _coerce_flag_list(flush_summary.get("flush_warning_flags", [])),
        "closed_pressure_swing_enabled": bool(setup_metadata.get("closed_pressure_swing_enabled")),
        "closed_pressure_swing_cycles_requested": _safe_int(setup_metadata.get("closed_pressure_swing_cycles_requested")),
        "closed_pressure_swing_cycles_completed": _safe_int(setup_metadata.get("closed_pressure_swing_cycles_completed")),
        "closed_pressure_swing_high_pressure_hpa": _safe_float(setup_metadata.get("closed_pressure_swing_high_pressure_hpa")),
        "closed_pressure_swing_low_pressure_hpa": _safe_float(setup_metadata.get("closed_pressure_swing_low_pressure_hpa")),
        "closed_pressure_swing_low_hold_s": _safe_float(setup_metadata.get("closed_pressure_swing_low_hold_s")),
        "closed_pressure_swing_linear_slew_hpa_per_s": _safe_float(setup_metadata.get("closed_pressure_swing_linear_slew_hpa_per_s")),
        "closed_pressure_swing_vent_closed_verified": bool(setup_metadata.get("closed_pressure_swing_vent_closed_verified")),
        "closed_pressure_swing_abort_reason": str(setup_metadata.get("closed_pressure_swing_abort_reason") or ""),
        "closed_pressure_swing_total_extra_s": _safe_float(setup_metadata.get("closed_pressure_swing_total_extra_s")),
        "extra_precondition_strategy_used": str(setup_metadata.get("extra_precondition_strategy_used") or ""),
        "extra_precondition_time_cost_s": _safe_float(setup_metadata.get("extra_precondition_time_cost_s")),
        "setup_note": setup_metadata.get("setup_note"),
        "operator_note": setup_metadata.get("operator_note"),
        "output_dir": setup_metadata.get("output_dir"),
    }


def _smaller_is_better_mode(
    left_name: str,
    left_value: Optional[float],
    right_name: str,
    right_value: Optional[float],
    *,
    abs_compare: bool = False,
) -> Optional[str]:
    if left_value is None and right_value is None:
        return None
    if left_value is None:
        return right_name
    if right_value is None:
        return left_name
    left_numeric = abs(left_value) if abs_compare else left_value
    right_numeric = abs(right_value) if abs_compare else right_value
    if left_numeric < right_numeric:
        return left_name
    if right_numeric < left_numeric:
        return right_name
    return "tie"


def build_analyzer_chain_isolation_comparison(
    isolation_summaries: Sequence[Mapping[str, Any]],
) -> Dict[str, Any]:
    ordered = [dict(row) for row in isolation_summaries if str(row.get("chain_mode") or "").strip()]
    by_mode = {str(row.get("chain_mode")): row for row in ordered}
    out_row = by_mode.get("analyzer_out_keep_rest")
    in_row = by_mode.get("analyzer_in_keep_rest")
    comparison_available = out_row is not None and in_row is not None

    faster_mode = None
    worse_rebound_mode = None
    larger_slope_mode = None
    larger_span_mode = None
    dominant = "insufficient_evidence"
    should_continue_s1 = False
    reason_not_to_continue = "isolation pair is incomplete"
    next_check = "先补齐 analyzer_out_keep_rest 和 analyzer_in_keep_rest 两个模式，再比较 8 台分析仪串路影响。"

    if comparison_available:
        out_status = str(out_row.get("classification") or "insufficient_evidence")
        in_status = str(in_row.get("classification") or "insufficient_evidence")
        faster_mode = _smaller_is_better_mode(
            "analyzer_out_keep_rest",
            _safe_float(out_row.get("dewpoint_time_to_gate")),
            "analyzer_in_keep_rest",
            _safe_float(in_row.get("dewpoint_time_to_gate")),
        )
        worse_rebound_mode = _smaller_is_better_mode(
            "analyzer_out_keep_rest",
            _safe_float(out_row.get("rebound_rise_c")) or 0.0,
            "analyzer_in_keep_rest",
            _safe_float(in_row.get("rebound_rise_c")) or 0.0,
        )
        if worse_rebound_mode == "analyzer_out_keep_rest":
            worse_rebound_mode = "analyzer_in_keep_rest"
        elif worse_rebound_mode == "analyzer_in_keep_rest":
            worse_rebound_mode = "analyzer_out_keep_rest"
        larger_slope_mode = _smaller_is_better_mode(
            "analyzer_out_keep_rest",
            _safe_float(out_row.get("dewpoint_tail_slope_60s")),
            "analyzer_in_keep_rest",
            _safe_float(in_row.get("dewpoint_tail_slope_60s")),
            abs_compare=True,
        )
        if larger_slope_mode == "analyzer_out_keep_rest":
            larger_slope_mode = "analyzer_in_keep_rest"
        elif larger_slope_mode == "analyzer_in_keep_rest":
            larger_slope_mode = "analyzer_out_keep_rest"
        larger_span_mode = _smaller_is_better_mode(
            "analyzer_out_keep_rest",
            _safe_float(out_row.get("dewpoint_tail_span_60s")),
            "analyzer_in_keep_rest",
            _safe_float(in_row.get("dewpoint_tail_span_60s")),
        )
        if larger_span_mode == "analyzer_out_keep_rest":
            larger_span_mode = "analyzer_in_keep_rest"
        elif larger_span_mode == "analyzer_in_keep_rest":
            larger_span_mode = "analyzer_out_keep_rest"

        out_better_count = 0
        metric_pairs = [
            (_safe_float(out_row.get("dewpoint_tail_span_60s")), _safe_float(in_row.get("dewpoint_tail_span_60s")), False),
            (_safe_float(out_row.get("dewpoint_tail_slope_60s")), _safe_float(in_row.get("dewpoint_tail_slope_60s")), True),
            (_safe_float(out_row.get("rebound_rise_c")) or 0.0, _safe_float(in_row.get("rebound_rise_c")) or 0.0, False),
            (_safe_float(out_row.get("dewpoint_time_to_gate")), _safe_float(in_row.get("dewpoint_time_to_gate")), False),
        ]
        similar_metric_count = 0
        for out_value, in_value, abs_compare in metric_pairs:
            if out_value is None or in_value is None:
                continue
            left = abs(out_value) if abs_compare else out_value
            right = abs(in_value) if abs_compare else in_value
            scale = max(abs(right), 1e-9)
            improvement = (right - left) / scale
            if improvement >= 0.20:
                out_better_count += 1
            if abs(left - right) / scale <= 0.15:
                similar_metric_count += 1

        if out_status == "pass" and _chain_mode_rank(in_status) >= _chain_mode_rank("warn"):
            dominant = "analyzer_chain_moisture_memory_suspicion"
            reason_not_to_continue = "8 台分析仪串路恢复在路后显著变差，当前不建议继续正式 S1。"
            next_check = "优先检查 8 台分析仪串路体积、材料吸附和 purge 条件，再复测 analyzer_in_keep_rest。"
        elif out_status == "pass" and in_status == "pass":
            dominant = "no_strong_chain_effect_detected"
            should_continue_s1 = True
            reason_not_to_continue = ""
            next_check = "两种模式都可收敛，可回到正式 S1 继续验证 shared path 是否仍稳定。"
        elif _chain_mode_rank(out_status) < _chain_mode_rank(in_status) and out_better_count >= 2:
            dominant = "analyzer_chain_amplifies_but_not_root_cause"
            reason_not_to_continue = "8 台分析仪串路会放大湿度拖尾，但 analyzer_out_keep_rest 也未完全通过。"
            next_check = "先在 analyzer_out_keep_rest 下继续确认 shared path 基线，再回头处理 8 台分析仪串路放大效应。"
        elif out_status == "fail" and in_status == "fail" and similar_metric_count >= 2:
            dominant = "shared_upstream_or_downstream_path_suspicion"
            reason_not_to_continue = "去掉 8 台分析仪串路后 dewpoint 特征仍近似，当前不建议继续正式 S1。"
            next_check = "优先检查零气源/减压阀、阀组共享段和 PACE 末端排气路径。"
        elif out_status == "pass" and in_status == "warn":
            dominant = "analyzer_chain_moisture_memory_suspicion"
            reason_not_to_continue = "8 台分析仪串路在路时已出现额外湿度拖尾。"
            next_check = "保持其余链路不变，重点检查 8 台分析仪串路 dead volume 与材料记忆。"
        else:
            dominant = "insufficient_evidence"
            reason_not_to_continue = "两种模式的优劣还不够明确，建议补齐同条件复测。"
            next_check = "按相同 0 ppm + VENT ON 条件各复测 1 次，确认 tail slope / rebound 是否可重复。"

    return {
        "comparison_available": comparison_available,
        "chain_modes_present": [str(row.get("chain_mode")) for row in ordered],
        "faster_time_to_gate_mode": faster_mode,
        "worse_rebound_mode": worse_rebound_mode,
        "larger_dewpoint_tail_slope_mode": larger_slope_mode,
        "larger_dewpoint_tail_span_mode": larger_span_mode,
        "dominant_isolation_conclusion": dominant,
        "should_continue_s1": should_continue_s1,
        "reason_not_to_continue_s1": reason_not_to_continue,
        "recommended_next_physical_check": next_check,
        "summaries": ordered,
    }


def build_analyzer_chain_pace_contribution_comparison(
    isolation_summaries: Sequence[Mapping[str, Any]],
) -> Dict[str, Any]:
    ordered = [dict(row) for row in isolation_summaries if str(row.get("chain_mode") or "").strip()]
    by_mode = {str(row.get("chain_mode")): row for row in ordered}
    a1p0 = by_mode.get("analyzer_in_pace_out_keep_rest")
    a1p1 = by_mode.get("analyzer_in_keep_rest")
    if a1p0 is None or a1p1 is None:
        return {
            "comparison_available": False,
            "reference_mode": "analyzer_in_keep_rest",
            "candidate_mode": "analyzer_in_pace_out_keep_rest",
            "pace_contribution_assessment": "insufficient_evidence",
            "reason": "missing_a1p0_or_a1p1",
        }

    metrics = (
        ("dewpoint_time_to_gate", False),
        ("dewpoint_tail_span_60s", False),
        ("dewpoint_tail_slope_60s", True),
        ("rebound_rise_c", False),
    )
    strong_improvement_count = 0
    mild_improvement_count = 0
    similar_count = 0
    for key, abs_compare in metrics:
        left_value = _safe_float(a1p0.get(key))
        right_value = _safe_float(a1p1.get(key))
        if left_value is None or right_value is None:
            continue
        left = abs(left_value) if abs_compare else left_value
        right = abs(right_value) if abs_compare else right_value
        scale = max(abs(right), 1e-9)
        improvement = (right - left) / scale
        if improvement >= 0.20:
            strong_improvement_count += 1
        elif improvement >= 0.05:
            mild_improvement_count += 1
        if abs(left - right) / scale <= 0.15:
            similar_count += 1

    a1p0_rank = _chain_mode_rank(a1p0.get("classification"))
    a1p1_rank = _chain_mode_rank(a1p1.get("classification"))
    if str(a1p0.get("classification") or "") == "fail" and str(a1p1.get("classification") or "") == "fail" and strong_improvement_count == 0 and similar_count >= 2:
        assessment = "analyzer_chain_dominant_even_without_pace"
    elif a1p0_rank < a1p1_rank and strong_improvement_count >= 1:
        assessment = "significant_additional_contributor"
    elif a1p0_rank <= a1p1_rank and (strong_improvement_count >= 1 or mild_improvement_count >= 2):
        assessment = "minor_additional_contributor"
    elif str(a1p0.get("classification") or "") == "fail" and str(a1p1.get("classification") or "") == "fail":
        assessment = "analyzer_chain_dominant_even_without_pace"
    else:
        assessment = "insufficient_evidence"

    return {
        "comparison_available": True,
        "reference_mode": "analyzer_in_keep_rest",
        "candidate_mode": "analyzer_in_pace_out_keep_rest",
        "pace_contribution_assessment": assessment,
        "classification_candidate": a1p0.get("classification"),
        "classification_reference": a1p1.get("classification"),
        "flush_gate_status_candidate": a1p0.get("flush_gate_status"),
        "flush_gate_status_reference": a1p1.get("flush_gate_status"),
        "dewpoint_time_to_gate_candidate": _safe_float(a1p0.get("dewpoint_time_to_gate")),
        "dewpoint_time_to_gate_reference": _safe_float(a1p1.get("dewpoint_time_to_gate")),
        "dewpoint_tail_span_60s_candidate": _safe_float(a1p0.get("dewpoint_tail_span_60s")),
        "dewpoint_tail_span_60s_reference": _safe_float(a1p1.get("dewpoint_tail_span_60s")),
        "dewpoint_tail_slope_60s_candidate": _safe_float(a1p0.get("dewpoint_tail_slope_60s")),
        "dewpoint_tail_slope_60s_reference": _safe_float(a1p1.get("dewpoint_tail_slope_60s")),
        "dewpoint_rebound_detected_candidate": bool(a1p0.get("dewpoint_rebound_detected")),
        "dewpoint_rebound_detected_reference": bool(a1p1.get("dewpoint_rebound_detected")),
        "rebound_rise_c_candidate": _safe_float(a1p0.get("rebound_rise_c")),
        "rebound_rise_c_reference": _safe_float(a1p1.get("rebound_rise_c")),
        "strong_improvement_count": strong_improvement_count,
        "mild_improvement_count": mild_improvement_count,
        "similar_metric_count": similar_count,
    }


def _chain_mode_case_name(chain_mode: str) -> Optional[str]:
    return {
        "analyzer_out_pace_out_keep_rest": "A0P0",
        "analyzer_out_keep_rest": "A0P1",
        "analyzer_in_pace_out_keep_rest": "A1P0",
        "analyzer_in_keep_rest": "A1P1",
    }.get(str(chain_mode or "").strip())


def _compare_case_row(row: Mapping[str, Any]) -> Dict[str, Any]:
    fail_reason = str(row.get("flush_gate_fail_reason") or "")
    classification = str(row.get("classification") or "insufficient_evidence")
    timeout = "timeout" in fail_reason.lower()
    time_to_gate = _safe_float(row.get("dewpoint_time_to_gate"))
    if time_to_gate is None and timeout:
        time_to_gate = _safe_float(row.get("flush_duration_s"))
    return {
        "case": _chain_mode_case_name(str(row.get("chain_mode") or "")),
        "chain_mode": str(row.get("chain_mode") or ""),
        "outcome": classification,
        "classification": classification,
        "flush_gate_status": str(row.get("flush_gate_status") or ""),
        "time_to_gate_s": time_to_gate,
        "rebound_detected": bool(row.get("dewpoint_rebound_detected")),
        "rebound_rise_c": _safe_float(row.get("rebound_rise_c")),
        "dewpoint_tail_slope_60s": _safe_float(row.get("dewpoint_tail_slope_60s")),
        "dewpoint_tail_span_60s": _safe_float(row.get("dewpoint_tail_span_60s")),
        "timeout": timeout,
        "flush_duration_s": _safe_float(row.get("flush_duration_s")),
        "fail_reason": fail_reason,
        "analyzer_count_in_path": _safe_int(row.get("analyzer_count_in_path")),
    }


def build_analyzer_chain_compare_vs_8ch(
    current_summaries: Sequence[Mapping[str, Any]],
    baseline_summaries: Sequence[Mapping[str, Any]],
) -> Dict[str, Any]:
    current_by_case = {
        str(case_row["case"]): case_row
        for case_row in (_compare_case_row(row) for row in current_summaries)
        if case_row.get("case")
    }
    baseline_by_case = {
        str(case_row["case"]): case_row
        for case_row in (_compare_case_row(row) for row in baseline_summaries)
        if case_row.get("case")
    }

    ordered_cases = ("A0P0", "A1P0", "A1P1", "A0P1")
    rows: List[Dict[str, Any]] = []
    for case_name in ordered_cases:
        current_row = current_by_case.get(case_name)
        baseline_row = baseline_by_case.get(case_name)
        if current_row is None and baseline_row is None:
            continue
        current_time = _safe_float(current_row.get("time_to_gate_s")) if current_row else None
        baseline_time = _safe_float(baseline_row.get("time_to_gate_s")) if baseline_row else None
        time_delta = None
        time_improvement_pct = None
        if current_time is not None and baseline_time is not None:
            time_delta = baseline_time - current_time
            if abs(baseline_time) > 1e-9:
                time_improvement_pct = time_delta / abs(baseline_time)
        rows.append(
            {
                "case": case_name,
                "current_outcome": current_row.get("outcome") if current_row else None,
                "current_time_to_gate_s": current_time,
                "current_rebound_detected": current_row.get("rebound_detected") if current_row else None,
                "current_dewpoint_tail_slope_60s": current_row.get("dewpoint_tail_slope_60s") if current_row else None,
                "current_dewpoint_tail_span_60s": current_row.get("dewpoint_tail_span_60s") if current_row else None,
                "current_timeout": current_row.get("timeout") if current_row else None,
                "baseline_outcome_8ch": baseline_row.get("outcome") if baseline_row else None,
                "baseline_time_to_gate_s_8ch": baseline_time,
                "baseline_rebound_detected_8ch": baseline_row.get("rebound_detected") if baseline_row else None,
                "baseline_dewpoint_tail_slope_60s_8ch": baseline_row.get("dewpoint_tail_slope_60s") if baseline_row else None,
                "baseline_dewpoint_tail_span_60s_8ch": baseline_row.get("dewpoint_tail_span_60s") if baseline_row else None,
                "baseline_timeout_8ch": baseline_row.get("timeout") if baseline_row else None,
                "time_to_gate_improvement_s": time_delta,
                "time_to_gate_improvement_pct": time_improvement_pct,
            }
        )

    a1p0_current = current_by_case.get("A1P0")
    a1p0_baseline = baseline_by_case.get("A1P0")
    a1p1_current = current_by_case.get("A1P1")
    a1p1_baseline = baseline_by_case.get("A1P1")

    a1p0_improvement_s = None
    a1p0_improvement_pct = None
    if a1p0_current and a1p0_baseline:
        current_time = _safe_float(a1p0_current.get("time_to_gate_s"))
        baseline_time = _safe_float(a1p0_baseline.get("time_to_gate_s"))
        if current_time is not None and baseline_time is not None:
            a1p0_improvement_s = baseline_time - current_time
            if abs(baseline_time) > 1e-9:
                a1p0_improvement_pct = a1p0_improvement_s / abs(baseline_time)

    a1p1_timeout_cleared = bool(a1p1_current and a1p1_baseline and bool(a1p1_baseline.get("timeout")) and not bool(a1p1_current.get("timeout")))
    a1p1_slope_improvement_pct = None
    if a1p1_current and a1p1_baseline:
        current_slope = _safe_float(a1p1_current.get("dewpoint_tail_slope_60s"))
        baseline_slope = _safe_float(a1p1_baseline.get("dewpoint_tail_slope_60s"))
        if current_slope is not None and baseline_slope is not None and abs(baseline_slope) > 1e-9:
            a1p1_slope_improvement_pct = (abs(baseline_slope) - abs(current_slope)) / abs(baseline_slope)

    overall = "改善不明显"
    conclusion = "4 台串路相对 8 台基线暂未体现稳定且显著的改善。"
    if (
        a1p0_current
        and a1p0_baseline
        and str(a1p0_current.get("classification") or "") == "pass"
        and str(a1p0_baseline.get("classification") or "") == "pass"
        and (a1p0_improvement_pct or 0.0) >= 0.20
        and not bool(a1p0_current.get("rebound_detected"))
        and a1p1_current
        and not bool(a1p1_current.get("timeout"))
    ):
        overall = "明显改善"
        conclusion = "4 台串路已明显优于 8 台基线，湿度拖尾显著缩短，建议继续沿 4 台路线整改与回归。"
    elif (
        (a1p0_improvement_pct or 0.0) >= 0.10
        or a1p1_timeout_cleared
        or (a1p1_slope_improvement_pct or 0.0) >= 0.15
    ):
        overall = "部分改善"
        conclusion = "4 台串路相对 8 台基线已有改善，但 PACE residual contribution 或残余死体积/材料记忆仍明显。"

    return {
        "current_cases_present": sorted(current_by_case.keys()),
        "baseline_cases_present": sorted(baseline_by_case.keys()),
        "overall_assessment": overall,
        "conclusion": conclusion,
        "a1p0_improvement_seconds_vs_8ch": a1p0_improvement_s,
        "a1p0_improvement_pct_vs_8ch": a1p0_improvement_pct,
        "a1p1_timeout_cleared_vs_8ch": a1p1_timeout_cleared,
        "a1p1_slope_improvement_pct_vs_8ch": a1p1_slope_improvement_pct,
        "rows": rows,
    }


def build_analyzer_chain_compare_vs_baseline(
    current_summaries: Sequence[Mapping[str, Any]],
    baseline_summaries: Sequence[Mapping[str, Any]],
) -> Dict[str, Any]:
    current_row = next(
        (row for row in (_compare_case_row(item) for item in current_summaries) if str(row.get("case") or "") == "A1P1"),
        {},
    )
    baseline_row = next(
        (row for row in (_compare_case_row(item) for item in baseline_summaries) if str(row.get("case") or "") == "A1P1"),
        {},
    )

    if not current_row or not baseline_row:
        return {
            "comparison_available": False,
            "worth_continuing": False,
            "recommendation": "baseline_missing",
            "conclusion": "未找到完整的 4ch A1P1 基线，无法判断闭式压力摆动预吹扫是否值得继续。",
            "rows": [],
        }

    current_time = _safe_float(current_row.get("time_to_gate_s"))
    baseline_time = _safe_float(baseline_row.get("time_to_gate_s"))
    current_slope = _safe_float(current_row.get("dewpoint_tail_slope_60s"))
    baseline_slope = _safe_float(baseline_row.get("dewpoint_tail_slope_60s"))
    current_span = _safe_float(current_row.get("dewpoint_tail_span_60s"))
    baseline_span = _safe_float(baseline_row.get("dewpoint_tail_span_60s"))
    current_rebound = bool(current_row.get("rebound_detected"))
    baseline_rebound = bool(baseline_row.get("rebound_detected"))
    extra_strategy = str(
        next((row.get("extra_precondition_strategy_used") for row in current_summaries if row.get("extra_precondition_strategy_used")), "")
        or ""
    )
    extra_time_cost_s = _safe_float(
        next((row.get("extra_precondition_time_cost_s") for row in current_summaries if row.get("extra_precondition_time_cost_s") not in (None, "")), None)
    )
    vent_closed_verified = bool(
        next((row.get("closed_pressure_swing_vent_closed_verified") for row in current_summaries if row.get("closed_pressure_swing_enabled")), False)
    )
    abort_reason = str(
        next((row.get("closed_pressure_swing_abort_reason") for row in current_summaries if row.get("closed_pressure_swing_abort_reason")), "")
        or ""
    )

    time_improvement_s = None
    time_improvement_pct = None
    if current_time is not None and baseline_time is not None:
        time_improvement_s = baseline_time - current_time
        if abs(baseline_time) > 1e-9:
            time_improvement_pct = time_improvement_s / abs(baseline_time)

    slope_not_worse = (
        current_slope is not None
        and baseline_slope is not None
        and abs(current_slope) <= abs(baseline_slope) + 1e-9
    )
    span_not_worse = (
        current_span is not None
        and baseline_span is not None
        and current_span <= baseline_span + 1e-9
    )
    improvement_clear = bool(
        time_improvement_s is not None
        and ((time_improvement_s >= 30.0) or ((time_improvement_pct or 0.0) >= 0.15))
    )
    still_pass = str(current_row.get("classification") or "") == "pass"
    no_vent_warning = vent_closed_verified and not abort_reason
    worth_continuing = bool(
        still_pass
        and not current_rebound
        and improvement_clear
        and slope_not_worse
        and span_not_worse
        and no_vent_warning
    )
    conclusion = "不建议纳入流程。"
    recommendation = "do_not_continue"
    if worth_continuing:
        conclusion = "1 轮闭式压力摆动预吹扫相对当前 4ch A1P1 基线显示明确正向收益，建议再做 2 轮验证。"
        recommendation = "continue_to_2cycles"
    elif still_pass and no_vent_warning:
        conclusion = "1 轮闭式压力摆动预吹扫已安全完成，但相对当前 4ch A1P1 基线改善不明显或指标变差，不建议纳入流程。"

    row = {
        "case": "A1P1",
        "classification": current_row.get("classification"),
        "flush_gate_status": current_row.get("flush_gate_status"),
        "dewpoint_time_to_gate": current_time,
        "dewpoint_tail_span_60s": current_span,
        "dewpoint_tail_slope_60s": current_slope,
        "dewpoint_rebound_detected": current_rebound,
        "timeout": bool(current_row.get("timeout")),
        "baseline_classification": baseline_row.get("classification"),
        "baseline_flush_gate_status": baseline_row.get("flush_gate_status"),
        "baseline_dewpoint_time_to_gate": baseline_time,
        "baseline_dewpoint_tail_span_60s": baseline_span,
        "baseline_dewpoint_tail_slope_60s": baseline_slope,
        "baseline_dewpoint_rebound_detected": baseline_rebound,
        "baseline_timeout": bool(baseline_row.get("timeout")),
        "extra_precondition_strategy_used": extra_strategy,
        "extra_precondition_time_cost_s": extra_time_cost_s,
        "closed_pressure_swing_vent_closed_verified": vent_closed_verified,
        "closed_pressure_swing_abort_reason": abort_reason,
        "time_to_gate_improvement_s": time_improvement_s,
        "time_to_gate_improvement_pct": time_improvement_pct,
        "tail_span_not_worse": span_not_worse,
        "tail_slope_not_worse": slope_not_worse,
        "worth_continuing": worth_continuing,
    }
    return {
        "comparison_available": True,
        "worth_continuing": worth_continuing,
        "recommendation": recommendation,
        "conclusion": conclusion,
        "time_to_gate_improvement_s": time_improvement_s,
        "time_to_gate_improvement_pct": time_improvement_pct,
        "tail_span_not_worse": span_not_worse,
        "tail_slope_not_worse": slope_not_worse,
        "rebound_still_false": not current_rebound,
        "closed_pressure_swing_vent_closed_verified": vent_closed_verified,
        "closed_pressure_swing_abort_reason": abort_reason,
        "rows": [row],
    }


def build_analyzer_chain_compare_vs_8ch_report(compare_summary: Mapping[str, Any]) -> str:
    rows = compare_summary.get("rows", []) or []
    lines = [
        "# 4 台串路相对 8 台基线对比",
        f"- 总体判定: {compare_summary.get('overall_assessment')}",
        f"- 结论: {compare_summary.get('conclusion')}",
        f"- 当前工况: {compare_summary.get('current_cases_present')}",
        f"- 8 台基线工况: {compare_summary.get('baseline_cases_present')}",
        "",
        "## 核心对比",
    ]
    for row in rows:
        lines.append(
            f"- {row.get('case')}: 当前={row.get('current_outcome')} time_to_gate={row.get('current_time_to_gate_s')}s "
            f"tail_slope={row.get('current_dewpoint_tail_slope_60s')} rebound={row.get('current_rebound_detected')} "
            f"| 8ch={row.get('baseline_outcome_8ch')} time_to_gate={row.get('baseline_time_to_gate_s_8ch')}s "
            f"tail_slope={row.get('baseline_dewpoint_tail_slope_60s_8ch')} rebound={row.get('baseline_rebound_detected_8ch')}"
        )
    lines.extend(
        [
            "",
            f"- A1P0 相对 8ch 改善秒数: {compare_summary.get('a1p0_improvement_seconds_vs_8ch')}",
            f"- A1P0 相对 8ch 改善比例: {compare_summary.get('a1p0_improvement_pct_vs_8ch')}",
            f"- A1P1 是否清除 8ch timeout: {compare_summary.get('a1p1_timeout_cleared_vs_8ch')}",
            f"- A1P1 tail_slope 改善比例: {compare_summary.get('a1p1_slope_improvement_pct_vs_8ch')}",
            "",
        ]
    )
    return "\n".join(lines) + "\n"


def build_analyzer_chain_compare_vs_baseline_report(compare_summary: Mapping[str, Any]) -> str:
    lines = [
        "# 闭式压力摆动预吹扫 vs 当前 4ch A1P1 基线",
        f"- comparison_available: {compare_summary.get('comparison_available')}",
        f"- worth_continuing: {compare_summary.get('worth_continuing')}",
        f"- recommendation: {compare_summary.get('recommendation')}",
        f"- conclusion: {compare_summary.get('conclusion')}",
        f"- time_to_gate_improvement_s: {compare_summary.get('time_to_gate_improvement_s')}",
        f"- time_to_gate_improvement_pct: {compare_summary.get('time_to_gate_improvement_pct')}",
        f"- tail_span_not_worse: {compare_summary.get('tail_span_not_worse')}",
        f"- tail_slope_not_worse: {compare_summary.get('tail_slope_not_worse')}",
        f"- rebound_still_false: {compare_summary.get('rebound_still_false')}",
        f"- closed_pressure_swing_vent_closed_verified: {compare_summary.get('closed_pressure_swing_vent_closed_verified')}",
        f"- closed_pressure_swing_abort_reason: {compare_summary.get('closed_pressure_swing_abort_reason')}",
        "",
        "## 核心对比",
    ]
    for row in compare_summary.get("rows", []) or []:
        lines.append(
            f"- {row.get('case')}: current={row.get('classification')} gate={row.get('flush_gate_status')} "
            f"time_to_gate={row.get('dewpoint_time_to_gate')} span={row.get('dewpoint_tail_span_60s')} "
            f"slope={row.get('dewpoint_tail_slope_60s')} rebound={row.get('dewpoint_rebound_detected')} "
            f"| baseline={row.get('baseline_classification')} gate={row.get('baseline_flush_gate_status')} "
            f"time_to_gate={row.get('baseline_dewpoint_time_to_gate')} span={row.get('baseline_dewpoint_tail_span_60s')} "
            f"slope={row.get('baseline_dewpoint_tail_slope_60s')} rebound={row.get('baseline_dewpoint_rebound_detected')} "
            f"| strategy={row.get('extra_precondition_strategy_used')} extra_time_cost_s={row.get('extra_precondition_time_cost_s')}"
        )
    lines.append("")
    return "\n".join(lines) + "\n"


def build_analyzer_chain_isolation_report(
    *,
    setup_metadata: Mapping[str, Any],
    operator_checklist: str,
    isolation_summaries: Sequence[Mapping[str, Any]],
    comparison_summary: Mapping[str, Any],
    compare_vs_8ch_summary: Optional[Mapping[str, Any]] = None,
    compare_vs_baseline_summary: Optional[Mapping[str, Any]] = None,
    plot_files: Optional[Mapping[str, Any]] = None,
) -> str:
    lines: List[str] = []
    lines.append("# 8 台分析仪串路在路 / 不在路对比诊断报告")
    lines.append(f"- 生成时间: {datetime.now().isoformat(timespec='seconds')}")
    lines.append(f"- run_id: {setup_metadata.get('run_id')}")
    lines.append(f"- smoke_level: {setup_metadata.get('smoke_level')}")
    lines.append(f"- chain_mode: {setup_metadata.get('chain_mode')}")
    lines.append(f"- case_name: {setup_metadata.get('case_name')}")
    lines.append(f"- chain_label: {setup_metadata.get('chain_label')}")
    lines.append(f"- analyzer_chain_connected: {setup_metadata.get('analyzer_chain_connected')}")
    lines.append(f"- analyzer_count_in_path: {setup_metadata.get('analyzer_count_in_path')}")
    lines.append(f"- analyzers_in_path: {setup_metadata.get('analyzers_in_path_text')}")
    lines.append(
        f"- capture_analyzer: {setup_metadata.get('capture_analyzer_name')} "
        f"[{setup_metadata.get('capture_analyzer_device_id')}]@{setup_metadata.get('capture_analyzer_port')}"
    )
    lines.append(f"- dominant_isolation_conclusion: {comparison_summary.get('dominant_isolation_conclusion')}")
    lines.append(f"- should_continue_s1: {comparison_summary.get('should_continue_s1')}")
    lines.append(f"- reason_not_to_continue_s1: {comparison_summary.get('reason_not_to_continue_s1')}")
    lines.append(f"- recommended_next_physical_check: {comparison_summary.get('recommended_next_physical_check')}")
    lines.append(f"- closed_pressure_swing_enabled: {setup_metadata.get('closed_pressure_swing_enabled')}")
    lines.append(f"- closed_pressure_swing_cycles_requested: {setup_metadata.get('closed_pressure_swing_cycles_requested')}")
    lines.append(f"- closed_pressure_swing_cycles_completed: {setup_metadata.get('closed_pressure_swing_cycles_completed')}")
    lines.append(f"- closed_pressure_swing_vent_closed_verified: {setup_metadata.get('closed_pressure_swing_vent_closed_verified')}")
    lines.append(f"- closed_pressure_swing_abort_reason: {setup_metadata.get('closed_pressure_swing_abort_reason')}")
    lines.append(f"- closed_pressure_swing_total_extra_s: {setup_metadata.get('closed_pressure_swing_total_extra_s')}")
    lines.append(f"- closed_pressure_swing_linear_slew_hpa_per_s: {setup_metadata.get('closed_pressure_swing_linear_slew_hpa_per_s')}")
    lines.append("")
    lines.append("## Operator Checklist")
    for line in operator_checklist.splitlines():
        lines.append(f"- {line}")
    lines.append("")
    lines.append("## Isolation Summary")
    for row in isolation_summaries:
        lines.append(
            f"- mode={row.get('chain_mode')} classification={row.get('classification')} "
            f"dewpoint_tail_span_60s={row.get('dewpoint_tail_span_60s')} "
            f"dewpoint_tail_slope_60s={row.get('dewpoint_tail_slope_60s')} "
            f"rebound={row.get('dewpoint_rebound_detected')} rebound_rise_c={row.get('rebound_rise_c')} "
            f"dewpoint_time_to_gate={row.get('dewpoint_time_to_gate')} fail_reason={row.get('flush_gate_fail_reason')}"
        )
    lines.append("")
    lines.append("## Comparison")
    lines.append(f"- faster_time_to_gate_mode: {comparison_summary.get('faster_time_to_gate_mode')}")
    lines.append(f"- worse_rebound_mode: {comparison_summary.get('worse_rebound_mode')}")
    lines.append(f"- larger_dewpoint_tail_slope_mode: {comparison_summary.get('larger_dewpoint_tail_slope_mode')}")
    lines.append(f"- larger_dewpoint_tail_span_mode: {comparison_summary.get('larger_dewpoint_tail_span_mode')}")
    pace_compare = comparison_summary.get("pace_vs_standard_in_comparison")
    if isinstance(pace_compare, Mapping):
        lines.append("")
        lines.append("## A1P0 vs A1P1")
        lines.append(
            f"- pace_contribution_assessment: {pace_compare.get('pace_contribution_assessment')}"
        )
        lines.append(
            f"- classification: A1P0={pace_compare.get('classification_candidate')} "
            f"A1P1={pace_compare.get('classification_reference')}"
        )
        lines.append(
            f"- flush_gate_status: A1P0={pace_compare.get('flush_gate_status_candidate')} "
            f"A1P1={pace_compare.get('flush_gate_status_reference')}"
        )
        lines.append(
            f"- dewpoint_time_to_gate: A1P0={pace_compare.get('dewpoint_time_to_gate_candidate')} "
            f"A1P1={pace_compare.get('dewpoint_time_to_gate_reference')}"
        )
        lines.append(
            f"- dewpoint_tail_span_60s: A1P0={pace_compare.get('dewpoint_tail_span_60s_candidate')} "
            f"A1P1={pace_compare.get('dewpoint_tail_span_60s_reference')}"
        )
        lines.append(
            f"- dewpoint_tail_slope_60s: A1P0={pace_compare.get('dewpoint_tail_slope_60s_candidate')} "
            f"A1P1={pace_compare.get('dewpoint_tail_slope_60s_reference')}"
        )
        lines.append(
            f"- dewpoint_rebound_detected: A1P0={pace_compare.get('dewpoint_rebound_detected_candidate')} "
            f"A1P1={pace_compare.get('dewpoint_rebound_detected_reference')}"
        )
        lines.append(
            f"- rebound_rise_c: A1P0={pace_compare.get('rebound_rise_c_candidate')} "
            f"A1P1={pace_compare.get('rebound_rise_c_reference')}"
        )
    if isinstance(compare_vs_8ch_summary, Mapping):
        lines.append("")
        lines.append("## 4 台串路 vs 8 台基线")
        lines.append(f"- overall_assessment: {compare_vs_8ch_summary.get('overall_assessment')}")
        lines.append(f"- conclusion: {compare_vs_8ch_summary.get('conclusion')}")
        lines.append(
            f"- A1P0 improvement vs 8ch: seconds={compare_vs_8ch_summary.get('a1p0_improvement_seconds_vs_8ch')} "
            f"pct={compare_vs_8ch_summary.get('a1p0_improvement_pct_vs_8ch')}"
        )
        lines.append(
            f"- A1P1 timeout cleared vs 8ch: {compare_vs_8ch_summary.get('a1p1_timeout_cleared_vs_8ch')}"
        )
        lines.append(
            f"- A1P1 tail_slope improvement vs 8ch: {compare_vs_8ch_summary.get('a1p1_slope_improvement_pct_vs_8ch')}"
        )
        for row in compare_vs_8ch_summary.get("rows", []) or []:
            lines.append(
                f"- {row.get('case')}: current={row.get('current_outcome')} "
                f"time_to_gate={row.get('current_time_to_gate_s')} rebound={row.get('current_rebound_detected')} "
                f"tail_slope={row.get('current_dewpoint_tail_slope_60s')} "
                f"| baseline_8ch={row.get('baseline_outcome_8ch')} time_to_gate={row.get('baseline_time_to_gate_s_8ch')} "
                f"rebound={row.get('baseline_rebound_detected_8ch')} tail_slope={row.get('baseline_dewpoint_tail_slope_60s_8ch')}"
            )
    if isinstance(compare_vs_baseline_summary, Mapping):
        lines.append("")
        lines.append("## 闭式压力摆动预吹扫 vs 当前 4ch A1P1 基线")
        lines.append(f"- worth_continuing: {compare_vs_baseline_summary.get('worth_continuing')}")
        lines.append(f"- recommendation: {compare_vs_baseline_summary.get('recommendation')}")
        lines.append(f"- conclusion: {compare_vs_baseline_summary.get('conclusion')}")
        lines.append(
            f"- time_to_gate_improvement: seconds={compare_vs_baseline_summary.get('time_to_gate_improvement_s')} "
            f"pct={compare_vs_baseline_summary.get('time_to_gate_improvement_pct')}"
        )
        lines.append(f"- tail_span_not_worse: {compare_vs_baseline_summary.get('tail_span_not_worse')}")
        lines.append(f"- tail_slope_not_worse: {compare_vs_baseline_summary.get('tail_slope_not_worse')}")
        lines.append(f"- rebound_still_false: {compare_vs_baseline_summary.get('rebound_still_false')}")
        lines.append(
            f"- closed_pressure_swing_vent_closed_verified: {compare_vs_baseline_summary.get('closed_pressure_swing_vent_closed_verified')}"
        )
        lines.append(
            f"- closed_pressure_swing_abort_reason: {compare_vs_baseline_summary.get('closed_pressure_swing_abort_reason')}"
        )
    lines.append("")
    lines.append("若出现 dewpoint 突变或 gate 长时间不过，请先对照 actuation_events.csv 与 flush_gate_trace.csv。")
    if plot_files:
        lines.append("")
        lines.append("## 图文件")
        for key, value in plot_files.items():
            lines.append(f"- {key}: {value}")
    return "\n".join(lines) + "\n"


def _write_chain_isolation_workbook(
    path: Path,
    *,
    raw_rows: Sequence[Mapping[str, Any]],
    flush_gate_trace_rows: Sequence[Mapping[str, Any]],
    actuation_events: Sequence[Mapping[str, Any]],
    closed_pressure_swing_trace_rows: Sequence[Mapping[str, Any]],
    setup_metadata: Mapping[str, Any],
    isolation_summaries: Sequence[Mapping[str, Any]],
    comparison_summary: Mapping[str, Any],
    compare_vs_8ch_rows: Sequence[Mapping[str, Any]] = (),
    compare_vs_8ch_summary: Optional[Mapping[str, Any]] = None,
    compare_vs_baseline_rows: Sequence[Mapping[str, Any]] = (),
    compare_vs_baseline_summary: Optional[Mapping[str, Any]] = None,
) -> None:
    import pandas as pd

    path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(path) as writer:
        pd.DataFrame(_csv_rows(raw_rows)).to_excel(writer, sheet_name="raw_timeseries", index=False)
        pd.DataFrame(_csv_rows(flush_gate_trace_rows)).to_excel(writer, sheet_name="flush_gate_trace", index=False)
        pd.DataFrame(_csv_rows(actuation_events)).to_excel(writer, sheet_name="actuation_events", index=False)
        if closed_pressure_swing_trace_rows:
            pd.DataFrame(_csv_rows(closed_pressure_swing_trace_rows)).to_excel(
                writer,
                sheet_name="closed_swing_trace",
                index=False,
            )
        pd.DataFrame(_csv_rows([setup_metadata])).to_excel(writer, sheet_name="setup_metadata", index=False)
        pd.DataFrame(_csv_rows(isolation_summaries)).to_excel(writer, sheet_name="isolation_summary", index=False)
        pd.DataFrame(_csv_rows([comparison_summary])).to_excel(writer, sheet_name="comparison_summary", index=False)
        if compare_vs_8ch_rows:
            pd.DataFrame(_csv_rows(compare_vs_8ch_rows)).to_excel(writer, sheet_name="compare_vs_8ch", index=False)
        if compare_vs_8ch_summary:
            pd.DataFrame(_csv_rows([compare_vs_8ch_summary])).to_excel(writer, sheet_name="compare_vs_8ch_summary", index=False)
        if compare_vs_baseline_rows:
            pd.DataFrame(_csv_rows(compare_vs_baseline_rows)).to_excel(writer, sheet_name="compare_vs_baseline", index=False)
        if compare_vs_baseline_summary:
            pd.DataFrame(_csv_rows([compare_vs_baseline_summary])).to_excel(
                writer,
                sheet_name="compare_vs_baseline_summary",
                index=False,
            )


def export_analyzer_chain_isolation_results(
    output_dir: str | Path,
    *,
    raw_rows: Sequence[Mapping[str, Any]],
    flush_gate_trace_rows: Sequence[Mapping[str, Any]],
    actuation_events: Sequence[Mapping[str, Any]],
    closed_pressure_swing_trace_rows: Sequence[Mapping[str, Any]] = (),
    setup_metadata: Mapping[str, Any],
    isolation_summaries: Sequence[Mapping[str, Any]],
    comparison_summary: Mapping[str, Any],
    operator_checklist: str,
    compare_vs_8ch_rows: Sequence[Mapping[str, Any]] = (),
    compare_vs_8ch_summary: Optional[Mapping[str, Any]] = None,
    compare_vs_baseline_rows: Sequence[Mapping[str, Any]] = (),
    compare_vs_baseline_summary: Optional[Mapping[str, Any]] = None,
    export_csv: bool = True,
    export_xlsx: bool = True,
    export_png: bool = True,
) -> Dict[str, Path]:
    root = Path(output_dir).resolve()
    root.mkdir(parents=True, exist_ok=True)

    raw_path = root / "raw_timeseries.csv"
    trace_path = root / "flush_gate_trace.csv"
    actuation_path = root / "actuation_events.csv"
    closed_swing_trace_path = root / "closed_pressure_swing_trace.csv"
    setup_path = root / "setup_metadata.json"
    summary_path = root / "isolation_summary.csv"
    comparison_path = root / "isolation_comparison_summary.json"
    rollup_summary_path = root / "summary.json"
    compare_csv_path = root / "compare_vs_8ch.csv"
    compare_md_path = root / "compare_vs_8ch.md"
    compare_baseline_csv_path = root / "compare_vs_baseline.csv"
    compare_baseline_md_path = root / "compare_vs_baseline.md"
    report_path = root / "readable_report.md"
    workbook_path = root / "diagnostic_workbook.xlsx"
    checklist_path = root / "operator_checklist.md"

    if export_csv:
        _write_csv(raw_path, raw_rows)
        _write_csv(trace_path, flush_gate_trace_rows)
        _write_csv(actuation_path, actuation_events)
        if closed_pressure_swing_trace_rows:
            _write_csv(closed_swing_trace_path, closed_pressure_swing_trace_rows)
        _write_csv(summary_path, isolation_summaries)
        if compare_vs_8ch_rows:
            _write_csv(compare_csv_path, compare_vs_8ch_rows)
        if compare_vs_baseline_rows:
            _write_csv(compare_baseline_csv_path, compare_vs_baseline_rows)

    plot_outputs: Dict[str, Any] = {}
    if export_png:
        from .room_temp_co2_pressure_plots import generate_analyzer_chain_isolation_plots

        plot_outputs = generate_analyzer_chain_isolation_plots(
            root,
            raw_rows=raw_rows,
            flush_gate_trace_rows=flush_gate_trace_rows,
            isolation_summaries=isolation_summaries,
            comparison_summary=comparison_summary,
        )
        if compare_vs_8ch_rows:
            from .room_temp_co2_pressure_plots import generate_compare_vs_8ch_time_to_gate_plot

            compare_plot = generate_compare_vs_8ch_time_to_gate_plot(root, compare_vs_8ch_rows=compare_vs_8ch_rows)
            if compare_plot is not None:
                plot_outputs["compare_vs_8ch_time_to_gate"] = compare_plot

    setup_path.write_text(json.dumps(dict(setup_metadata), ensure_ascii=False, indent=2), encoding="utf-8")
    comparison_payload = dict(comparison_summary)
    comparison_payload["plot_files"] = {key: str(value) for key, value in plot_outputs.items()}
    comparison_path.write_text(json.dumps(comparison_payload, ensure_ascii=False, indent=2), encoding="utf-8")
    checklist_path.write_text(operator_checklist, encoding="utf-8")
    rollup_payload = {
        "setup_metadata": dict(setup_metadata),
        "isolation_summaries": [dict(row) for row in isolation_summaries],
        "isolation_comparison_summary": comparison_payload,
        "compare_vs_8ch": dict(compare_vs_8ch_summary or {}),
        "compare_vs_baseline": dict(compare_vs_baseline_summary or {}),
    }
    rollup_summary_path.write_text(json.dumps(rollup_payload, ensure_ascii=False, indent=2), encoding="utf-8")
    if compare_vs_8ch_summary:
        compare_md_path.write_text(build_analyzer_chain_compare_vs_8ch_report(compare_vs_8ch_summary), encoding="utf-8")
    if compare_vs_baseline_summary:
        compare_baseline_md_path.write_text(
            build_analyzer_chain_compare_vs_baseline_report(compare_vs_baseline_summary),
            encoding="utf-8",
        )
    report_path.write_text(
        build_analyzer_chain_isolation_report(
            setup_metadata=setup_metadata,
            operator_checklist=operator_checklist,
            isolation_summaries=isolation_summaries,
            comparison_summary=comparison_payload,
            compare_vs_8ch_summary=compare_vs_8ch_summary,
            compare_vs_baseline_summary=compare_vs_baseline_summary,
            plot_files=comparison_payload.get("plot_files"),
        ),
        encoding="utf-8",
    )

    if export_xlsx:
        _write_chain_isolation_workbook(
            workbook_path,
            raw_rows=raw_rows,
            flush_gate_trace_rows=flush_gate_trace_rows,
            actuation_events=actuation_events,
            closed_pressure_swing_trace_rows=closed_pressure_swing_trace_rows,
            setup_metadata=setup_metadata,
            isolation_summaries=isolation_summaries,
            comparison_summary=comparison_payload,
            compare_vs_8ch_rows=compare_vs_8ch_rows,
            compare_vs_8ch_summary=compare_vs_8ch_summary,
            compare_vs_baseline_rows=compare_vs_baseline_rows,
            compare_vs_baseline_summary=compare_vs_baseline_summary,
        )

    outputs: Dict[str, Path] = {
        "output_dir": root,
        "raw_timeseries": raw_path,
        "flush_gate_trace": trace_path,
        "actuation_events": actuation_path,
        "closed_pressure_swing_trace": closed_swing_trace_path,
        "setup_metadata": setup_path,
        "isolation_summary": summary_path,
        "isolation_comparison_summary": comparison_path,
        "summary": rollup_summary_path,
        "compare_vs_baseline_csv": compare_baseline_csv_path,
        "compare_vs_baseline_md": compare_baseline_md_path,
        "compare_vs_8ch_csv": compare_csv_path,
        "compare_vs_8ch_md": compare_md_path,
        "readable_report": report_path,
        "diagnostic_workbook": workbook_path,
        "operator_checklist": checklist_path,
    }
    outputs.update(plot_outputs)
    return outputs
