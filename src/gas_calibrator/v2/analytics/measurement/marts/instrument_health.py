from __future__ import annotations

from statistics import mean
from typing import Any


def _average(values: list[float | int | None]) -> float | None:
    numbers = [float(item) for item in values if item is not None]
    if not numbers:
        return None
    return mean(numbers)


def _delta(current: float | None, baseline: float | None) -> float | None:
    if current is None or baseline is None:
        return None
    return float(current) - float(baseline)


def _drift_penalty(history: list[dict[str, Any]]) -> float:
    if len(history) < 2:
        return 0.0
    baseline = history[0]
    latest = history[-1]
    ratio_deltas = [
        abs(value)
        for value in (
            _delta(latest.get("mean_co2_ratio_f"), baseline.get("mean_co2_ratio_f")),
            _delta(latest.get("mean_h2o_ratio_f"), baseline.get("mean_h2o_ratio_f")),
        )
        if value is not None
    ]
    ratio_delta = max(ratio_deltas, default=0.0)
    rmse_delta = abs(float(_delta(latest.get("mean_rmse"), baseline.get("mean_rmse")) or 0.0))
    normalized = min(1.0, (ratio_delta * 20.0) + (rmse_delta * 20.0))
    return normalized


def _health_band(score: float) -> str:
    if score >= 90.0:
        return "excellent"
    if score >= 75.0:
        return "good"
    if score >= 60.0:
        return "watch"
    return "poor"


def build_instrument_health(features: dict[str, Any], **_: Any) -> dict[str, Any]:
    analyzers_output: list[dict[str, Any]] = []
    for item in features.get("analyzer_features", []):
        frame_count = int(item.get("frame_count") or 0)
        point_count = int(item.get("point_count") or 0)
        abnormal_rate = 0.0 if frame_count == 0 else float(item.get("abnormal_status_count") or 0) / frame_count
        qc_fail_rate = 0.0 if point_count == 0 else float(item.get("qc_fail_count") or 0) / point_count
        drift_penalty = _drift_penalty(list(item.get("history", [])))
        usable_rate = float(item.get("usable_rate") or 0.0)
        status_clean_rate = max(0.0, 1.0 - abnormal_rate)
        qc_clean_rate = max(0.0, 1.0 - min(1.0, qc_fail_rate))
        drift_clean_rate = max(0.0, 1.0 - drift_penalty)
        health_score = round(
            100.0
            * (
                (0.50 * usable_rate)
                + (0.20 * status_clean_rate)
                + (0.20 * qc_clean_rate)
                + (0.10 * drift_clean_rate)
            ),
            2,
        )
        analyzers_output.append(
            {
                "analyzer_label": item.get("analyzer_label"),
                "frame_count": frame_count,
                "run_count": item.get("run_count", 0),
                "usable_rate": usable_rate,
                "abnormal_status_rate": abnormal_rate,
                "qc_fail_rate": qc_fail_rate,
                "drift_penalty": drift_penalty,
                "health_score": health_score,
                "health_band": _health_band(health_score),
            }
        )

    return {
        "analyzer_count": len(analyzers_output),
        "average_health_score": _average([item.get("health_score") for item in analyzers_output]),
        "analyzers": analyzers_output,
    }
