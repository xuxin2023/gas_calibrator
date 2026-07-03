"""Automatic steady-state window selection for V1.5 open-flow evidence."""

from __future__ import annotations

from typing import Any, Dict, List, Mapping, Sequence

from ._math import data_range, slope, stddev, values


DEFAULT_SIGNAL_KEYS = (
    "co2_ppm",
    "h2o_mmol",
    "dewpoint_c",
    "com22_pressure_hpa",
    "analyzer_pressure_hpa",
    "co2_ratio",
    "h2o_ratio",
    "ref_signal",
    "chamber_temp_c",
    "case_temp_c",
)


def _window_metrics(rows: Sequence[Mapping[str, Any]], signal_keys: Sequence[str]) -> Dict[str, Dict[str, Any]]:
    metrics: Dict[str, Dict[str, Any]] = {}
    for key in signal_keys:
        data = values(rows, key)
        if not data:
            continue
        metrics[key] = {
            "count": len(data),
            "std": stddev(data),
            "slope": slope(data),
            "range": data_range(data),
        }
    return metrics


def _score(metrics: Mapping[str, Mapping[str, Any]]) -> float:
    score = 0.0
    for item in metrics.values():
        score += abs(float(item.get("slope") or 0.0))
        score += abs(float(item.get("std") or 0.0))
        score += abs(float(item.get("range") or 0.0)) * 0.25
    return score


def select_steady_state_window(
    rows: Sequence[Mapping[str, Any]],
    *,
    signal_keys: Sequence[str] = DEFAULT_SIGNAL_KEYS,
    window_size: int = 10,
) -> Dict[str, Any]:
    """Select the lowest-variation window from already-recorded rows."""

    if window_size <= 1:
        raise ValueError("window_size must be > 1")
    if len(rows) < window_size:
        return {
            "status": "fail",
            "reason": "insufficient_rows_for_steady_state_window",
            "window_size": window_size,
            "row_count": len(rows),
        }

    best: Dict[str, Any] | None = None
    for start in range(0, len(rows) - window_size + 1):
        window = rows[start : start + window_size]
        metrics = _window_metrics(window, signal_keys)
        score = _score(metrics)
        status_bad = any(str(row.get("status_register_qc") or "").lower() == "fail" for row in window)
        if status_bad:
            score += 1_000_000.0
        candidate = {
            "status": "pass" if metrics and not status_bad else "fail",
            "start_index": start,
            "end_index": start + window_size - 1,
            "sample_count": window_size,
            "score": score,
            "metrics": metrics,
            "reason": "status_register_fail_in_window" if status_bad else "",
        }
        if best is None or candidate["score"] < best["score"]:
            best = candidate

    return best or {"status": "fail", "reason": "no_numeric_metrics"}
