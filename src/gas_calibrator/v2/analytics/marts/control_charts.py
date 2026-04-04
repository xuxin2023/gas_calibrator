from __future__ import annotations

from math import sqrt
from statistics import mean
from typing import Any


def _control_limits(values: list[float]) -> tuple[float | None, float | None, float | None]:
    if not values:
        return None, None, None
    center = mean(values)
    if len(values) < 2:
        return center, center, center
    variance = sum((value - center) ** 2 for value in values) / len(values)
    sigma = sqrt(max(variance, 0.0))
    return center, center + (3.0 * sigma), center - (3.0 * sigma)


def _series(history: list[dict[str, Any]], metric: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for item in history:
        value = item.get(metric)
        if value is None:
            continue
        rows.append(
            {
                "run_id": item.get("run_id"),
                "start_time": item.get("start_time"),
                "value": float(value),
            }
        )
    return rows


def build_control_charts(features: dict[str, Any], **_: Any) -> dict[str, Any]:
    charts: list[dict[str, Any]] = []
    for analyzer in features.get("analyzers", []):
        analyzer_id = analyzer.get("analyzer_id")
        history = list(analyzer.get("history") or [])
        metric_charts: list[dict[str, Any]] = []
        for metric in ("mean_co2_ppm", "mean_h2o_mmol", "mean_rmse"):
            series = _series(history, metric)
            values = [row["value"] for row in series]
            center, ucl, lcl = _control_limits(values)
            metric_charts.append(
                {
                    "metric": metric,
                    "sample_count": len(series),
                    "center_line": center,
                    "ucl": ucl,
                    "lcl": lcl,
                    "series": series,
                    "status": "ok" if len(series) >= 2 else "insufficient_history",
                }
            )
        charts.append(
            {
                "analyzer_id": analyzer_id,
                "analyzer_serial": analyzer.get("analyzer_serial"),
                "charts": metric_charts,
            }
        )
    return {
        "analyzer_count": len(charts),
        "charts": charts,
    }
