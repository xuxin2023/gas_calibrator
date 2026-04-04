from __future__ import annotations

import math
from datetime import datetime, timedelta
from statistics import pstdev
from typing import Any, Dict, Mapping, Optional, Sequence


DEFAULT_DEWPOINT_GATE_WINDOW_S = 60.0
DEFAULT_DEWPOINT_REBOUND_WINDOW_S = 180.0
DEFAULT_DEWPOINT_REBOUND_MIN_RISE_C = 1.0


def safe_float(value: Any) -> Optional[float]:
    if value in (None, ""):
        return None
    try:
        numeric = float(value)
    except Exception:
        return None
    if not math.isfinite(numeric):
        return None
    return numeric


def safe_int(value: Any) -> Optional[int]:
    numeric = safe_float(value)
    if numeric is None:
        return None
    try:
        return int(round(numeric))
    except Exception:
        return None


def safe_iso(ts: Optional[datetime]) -> Optional[str]:
    return ts.isoformat(timespec="milliseconds") if ts is not None else None


def dewpoint_saturation_pressure_hpa(dewpoint_c: Any) -> Optional[float]:
    numeric = safe_float(dewpoint_c)
    if numeric is None:
        return None
    if numeric >= 0.0:
        return 6.1121 * math.exp((18.678 - numeric / 234.5) * (numeric / (257.14 + numeric)))
    return 6.1115 * math.exp((23.036 - numeric / 333.7) * (numeric / (279.82 + numeric)))


def dewpoint_to_h2o_mmol_per_mol(dewpoint_c: Any, pressure_hpa: Any) -> Optional[float]:
    dewpoint = safe_float(dewpoint_c)
    pressure = safe_float(pressure_hpa)
    if dewpoint is None or pressure is None or pressure <= 0.0:
        return None
    vapor_pressure = dewpoint_saturation_pressure_hpa(dewpoint)
    if vapor_pressure is None:
        return None
    return round(1000.0 * vapor_pressure / pressure, 6)


def vapor_pressure_hpa_to_dewpoint_c(vapor_pressure_hpa: Any) -> Optional[float]:
    vapor_pressure = safe_float(vapor_pressure_hpa)
    if vapor_pressure is None or vapor_pressure <= 0.0:
        return None

    low = -120.0
    high = 80.0
    low_pressure = dewpoint_saturation_pressure_hpa(low)
    high_pressure = dewpoint_saturation_pressure_hpa(high)
    if low_pressure is None or high_pressure is None:
        return None
    if vapor_pressure < low_pressure or vapor_pressure > high_pressure:
        return None

    for _ in range(80):
        mid = (low + high) / 2.0
        mid_pressure = dewpoint_saturation_pressure_hpa(mid)
        if mid_pressure is None:
            return None
        if mid_pressure < vapor_pressure:
            low = mid
        else:
            high = mid
    return round((low + high) / 2.0, 6)


def predict_pressure_scaled_dewpoint_c(
    preseal_dewpoint_c: Any,
    preseal_pressure_hpa: Any,
    target_pressure_hpa: Any,
) -> Optional[float]:
    preseal_pressure = safe_float(preseal_pressure_hpa)
    target_pressure = safe_float(target_pressure_hpa)
    if preseal_pressure is None or target_pressure is None or preseal_pressure <= 0.0 or target_pressure <= 0.0:
        return None
    preseal_vapor_pressure = dewpoint_saturation_pressure_hpa(preseal_dewpoint_c)
    if preseal_vapor_pressure is None:
        return None
    target_vapor_pressure = preseal_vapor_pressure * target_pressure / preseal_pressure
    return vapor_pressure_hpa_to_dewpoint_c(target_vapor_pressure)


def parse_timestamp(value: Any) -> Optional[datetime]:
    text = str(value or "").strip()
    if not text:
        return None
    normalized = text.replace("Z", "+00:00")
    try:
        return datetime.fromisoformat(normalized)
    except Exception:
        return None


def ordered_rows(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    return sorted(
        [dict(row) for row in rows],
        key=lambda row: parse_timestamp(row.get("timestamp")) or datetime.min,
    )


def series(rows: Sequence[Mapping[str, Any]], key: str) -> list[float]:
    values: list[float] = []
    for row in rows:
        numeric = safe_float(row.get(key))
        if numeric is not None:
            values.append(numeric)
    return values


def tail_rows(rows: Sequence[Mapping[str, Any]], window_s: float) -> list[dict[str, Any]]:
    ordered = ordered_rows(rows)
    if not ordered:
        return []
    timestamps = [parse_timestamp(row.get("timestamp")) for row in ordered]
    valid = [item for item in timestamps if item is not None]
    if len(valid) < 2:
        return ordered
    cutoff = valid[-1] - timedelta(seconds=max(0.0, float(window_s)))
    return [
        row
        for row in ordered
        if (parse_timestamp(row.get("timestamp")) or datetime.min) >= cutoff
    ]


def duration_seconds(rows: Sequence[Mapping[str, Any]]) -> float:
    timestamps = [parse_timestamp(row.get("timestamp")) for row in rows]
    timestamps = [item for item in timestamps if item is not None]
    if len(timestamps) >= 2:
        return max(0.0, (timestamps[-1] - timestamps[0]).total_seconds())
    elapsed_values = [safe_float(row.get("elapsed_s")) for row in rows]
    elapsed_values = [item for item in elapsed_values if item is not None]
    if elapsed_values:
        return max(0.0, max(elapsed_values) - min(elapsed_values))
    return 0.0


def phase_duration_s(rows: Sequence[Mapping[str, Any]]) -> float:
    values = [safe_float(row.get("phase_elapsed_s")) for row in rows]
    values = [item for item in values if item is not None]
    if values:
        return max(0.0, max(values))
    return duration_seconds(rows)


def linear_regression(xs: Sequence[float], ys: Sequence[float]) -> Dict[str, Optional[float]]:
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


def time_value_pairs(rows: Sequence[Mapping[str, Any]], key: str) -> list[tuple[float, float]]:
    ordered = ordered_rows(rows)
    if not ordered:
        return []
    base_ts = parse_timestamp(ordered[0].get("timestamp"))
    if base_ts is None:
        return []
    pairs: list[tuple[float, float]] = []
    for row in ordered:
        ts = parse_timestamp(row.get("timestamp"))
        value = safe_float(row.get(key))
        if ts is None or value is None:
            continue
        pairs.append((max(0.0, (ts - base_ts).total_seconds()), value))
    return pairs


def slope_per_s(rows: Sequence[Mapping[str, Any]], key: str) -> Optional[float]:
    pairs = time_value_pairs(rows, key)
    if len(pairs) < 2:
        return None
    fit = linear_regression([x for x, _ in pairs], [y for _, y in pairs])
    return fit["slope"]


def preferred_pressure_series(rows: Sequence[Mapping[str, Any]]) -> list[float]:
    out: list[float] = []
    for row in rows:
        for key in ("gauge_pressure_hpa", "controller_pressure_hpa", "analyzer2_pressure_hpa"):
            numeric = safe_float(row.get(key))
            if numeric is not None:
                out.append(numeric)
                break
    return out


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
    ordered = ordered_rows(rows)
    candidates: list[tuple[datetime, float, Dict[str, Any]]] = []
    for row in ordered:
        ts = parse_timestamp(row.get("timestamp"))
        dew = safe_float(row.get("dewpoint_c"))
        if ts is None or dew is None:
            continue
        candidates.append((ts, dew, row))
    if len(candidates) < 3:
        return {
            "dewpoint_rebound_detected": False,
            "rebound_start_time": None,
            "rebound_min_time": None,
            "rebound_peak_time": None,
            "rebound_rise_c": None,
            "rebound_window_s": float(rebound_window_s),
            "rebound_gauge_delta_hpa": None,
            "rebound_temp_delta_c": None,
            "rebound_rh_delta_pct": None,
            "rebound_note": "",
        }

    matching_events: list[tuple[datetime, Mapping[str, Any]]] = []
    for event in actuation_events or []:
        ts = parse_timestamp(event.get("timestamp"))
        if ts is None:
            continue
        if process_variant is not None and str(event.get("process_variant") or "") != str(process_variant):
            continue
        if layer is not None and safe_int(event.get("layer")) != int(layer):
            continue
        if repeat_index is not None and safe_int(event.get("repeat_index")) != int(repeat_index):
            continue
        event_gas = safe_int(event.get("gas_ppm"))
        if gas_ppm is not None and event_gas not in (None, int(gas_ppm)):
            continue
        matching_events.append((ts, event))

    window_s = max(1.0, float(rebound_window_s))
    min_rise = max(0.0, float(rebound_min_rise_c))
    min_idx = 0
    min_ts, min_value, min_row = candidates[min_idx]
    for idx in range(1, len(candidates)):
        ts, value, row = candidates[idx]
        if value < min_value:
            min_idx = idx
            min_ts, min_value, min_row = ts, value, row
            continue
        elapsed = (ts - min_ts).total_seconds()
        if elapsed > window_s:
            min_idx = idx
            min_ts, min_value, min_row = ts, value, row
            continue
        rise = value - min_value
        if rise < min_rise:
            continue
        if rebound_require_no_new_actuation:
            conflicting = [
                event
                for event_ts, event in matching_events
                if event_ts > min_ts and event_ts <= ts
            ]
            if conflicting:
                continue
        gauge_min = safe_float(min_row.get("gauge_pressure_hpa"))
        gauge_peak = safe_float(row.get("gauge_pressure_hpa"))
        temp_min = safe_float(min_row.get("dewpoint_temp_c", min_row.get("dew_temp_c")))
        temp_peak = safe_float(row.get("dewpoint_temp_c", row.get("dew_temp_c")))
        rh_min = safe_float(min_row.get("dewpoint_rh_percent", min_row.get("dew_rh_pct")))
        rh_peak = safe_float(row.get("dewpoint_rh_percent", row.get("dew_rh_pct")))
        return {
            "dewpoint_rebound_detected": True,
            "rebound_start_time": safe_iso(min_ts),
            "rebound_min_time": safe_iso(min_ts),
            "rebound_peak_time": safe_iso(ts),
            "rebound_rise_c": rise,
            "rebound_window_s": elapsed,
            "rebound_gauge_delta_hpa": (
                gauge_peak - gauge_min
                if gauge_peak is not None and gauge_min is not None
                else None
            ),
            "rebound_temp_delta_c": (
                temp_peak - temp_min
                if temp_peak is not None and temp_min is not None
                else None
            ),
            "rebound_rh_delta_pct": (
                rh_peak - rh_min
                if rh_peak is not None and rh_min is not None
                else None
            ),
            "rebound_note": (
                "source gas moisture suspicion / line moisture memory suspicion / "
                "dewpoint meter dynamic suspicion"
            ),
        }
    return {
        "dewpoint_rebound_detected": False,
        "rebound_start_time": None,
        "rebound_min_time": None,
        "rebound_peak_time": None,
        "rebound_rise_c": None,
        "rebound_window_s": float(rebound_window_s),
        "rebound_gauge_delta_hpa": None,
        "rebound_temp_delta_c": None,
        "rebound_rh_delta_pct": None,
        "rebound_note": "",
    }


def evaluate_dewpoint_flush_gate(
    rows: Sequence[Mapping[str, Any]],
    *,
    min_flush_s: float,
    gate_window_s: float = DEFAULT_DEWPOINT_GATE_WINDOW_S,
    max_tail_span_c: float = 0.35,
    max_abs_tail_slope_c_per_s: float = 0.003,
    rebound_window_s: float = DEFAULT_DEWPOINT_REBOUND_WINDOW_S,
    rebound_min_rise_c: float = DEFAULT_DEWPOINT_REBOUND_MIN_RISE_C,
    rebound_require_no_new_actuation: bool = True,
    include_rebound_in_gate: bool = True,
    require_vent_on: bool = False,
) -> Dict[str, Any]:
    ordered = ordered_rows(rows)
    duration_s = phase_duration_s(ordered)
    tail = tail_rows(ordered, gate_window_s)
    dewpoint_values = series(tail, "dewpoint_c")
    dewpoint_slope = slope_per_s(tail, "dewpoint_c")
    dewpoint_span = (max(dewpoint_values) - min(dewpoint_values)) if len(dewpoint_values) >= 2 else None
    latest = ordered[-1] if ordered else {}
    vent_states = [str(row.get("controller_vent_state") or "").strip().upper() for row in ordered]
    vent_on = bool(vent_states) and all(state == "VENT_ON" for state in vent_states)
    if not require_vent_on:
        vent_on = True
    rebound = detect_dewpoint_rebound(
        ordered,
        rebound_window_s=rebound_window_s,
        rebound_min_rise_c=rebound_min_rise_c,
        rebound_require_no_new_actuation=rebound_require_no_new_actuation,
    )
    reasons: list[str] = []
    if require_vent_on and not vent_on:
        reasons.append("flush_not_all_vent_on")
    if duration_s < float(min_flush_s):
        reasons.append("flush_duration_below_min")
    if dewpoint_slope is None:
        reasons.append("dewpoint_tail_window_missing")
    elif abs(dewpoint_slope) > float(max_abs_tail_slope_c_per_s):
        reasons.append("dewpoint_tail_slope_too_large")
    if dewpoint_span is None:
        reasons.append("dewpoint_tail_span_missing")
    elif dewpoint_span > float(max_tail_span_c):
        reasons.append("dewpoint_tail_span_too_large")
    if include_rebound_in_gate and bool(rebound.get("dewpoint_rebound_detected")):
        reasons.append("dewpoint_rebound_detected")
    pressure_values = preferred_pressure_series(tail)
    pressure_std = pstdev(pressure_values) if len(pressure_values) >= 2 else None
    pressure_span = (max(pressure_values) - min(pressure_values)) if len(pressure_values) >= 2 else None
    gate_pass = not reasons
    return {
        "gate_pass": gate_pass,
        "gate_status": "pass" if gate_pass else "waiting",
        "gate_reason": ";".join(reasons),
        "failing_subgates": reasons,
        "flush_duration_s": duration_s,
        "dewpoint_time_to_gate": duration_s,
        "dewpoint_tail_count_60s": len(dewpoint_values),
        "dewpoint_tail_value_60s": safe_float(latest.get("dewpoint_c")),
        "dewpoint_tail_span_60s": dewpoint_span,
        "dewpoint_tail_slope_60s": dewpoint_slope,
        "pressure_tail_std_60s": pressure_std,
        "pressure_tail_span_60s": pressure_span,
        "vent_state_during_flush": "VENT_ON" if vent_on else "NOT_ALL_VENT_ON",
        **rebound,
    }
