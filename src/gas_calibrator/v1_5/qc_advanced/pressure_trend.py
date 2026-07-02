"""Pressure-channel history and trend diagnostics."""

from __future__ import annotations

from typing import Any, Dict, Mapping, Sequence

from ._math import mean, slope, values


def evaluate_pressure_trend(
    rows: Sequence[Mapping[str, Any]],
    *,
    analyzer_delta_key: str = "analyzer_minus_com22_hpa",
    pace_delta_key: str = "pace_minus_com22_hpa",
    mean_abs_limit_hpa: float = 1.0,
    slope_abs_limit_hpa_per_point: float = 0.05,
) -> Dict[str, Any]:
    analyzer_delta = values(rows, analyzer_delta_key)
    pace_delta = values(rows, pace_delta_key)
    analyzer_mean = mean(analyzer_delta)
    pace_mean = mean(pace_delta)
    analyzer_slope = slope(analyzer_delta)
    pace_slope = slope(pace_delta)
    reasons = []
    if analyzer_mean is None:
        reasons.append("analyzer_pressure_delta_missing")
    elif abs(analyzer_mean) > mean_abs_limit_hpa:
        reasons.append("analyzer_pressure_mean_bias_exceeds_limit")
    if analyzer_slope is not None and abs(analyzer_slope) > slope_abs_limit_hpa_per_point:
        reasons.append("analyzer_pressure_trend_drift")
    if pace_mean is not None and abs(pace_mean) > mean_abs_limit_hpa:
        reasons.append("pace_pressure_mean_bias_exceeds_limit")
    if pace_slope is not None and abs(pace_slope) > slope_abs_limit_hpa_per_point:
        reasons.append("pace_pressure_trend_drift")

    return {
        "status": "pass" if not reasons else "review",
        "reasons": reasons,
        "analyzer_delta_mean_hpa": analyzer_mean,
        "analyzer_delta_slope_hpa_per_point": analyzer_slope,
        "pace_delta_mean_hpa": pace_mean,
        "pace_delta_slope_hpa_per_point": pace_slope,
        "sample_count": len(rows),
    }
