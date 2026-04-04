from __future__ import annotations

from typing import Any


def _delta(latest: float | None, baseline: float | None) -> float | None:
    if latest is None or baseline is None:
        return None
    return round(float(latest) - float(baseline), 6)


def build_drift_metrics(features: dict[str, Any], **_: Any) -> dict[str, Any]:
    analyzers = []
    for analyzer in features.get("analyzers", []):
        history = list(analyzer.get("history") or [])
        if len(history) < 2:
            analyzers.append(
                {
                    "analyzer_id": analyzer.get("analyzer_id"),
                    "history_points": len(history),
                    "status": "insufficient_history",
                }
            )
            continue
        baseline = history[0]
        latest = history[-1]
        co2_delta = _delta(latest.get("mean_co2_ppm"), baseline.get("mean_co2_ppm"))
        h2o_delta = _delta(latest.get("mean_h2o_mmol"), baseline.get("mean_h2o_mmol"))
        rmse_delta = _delta(latest.get("mean_rmse"), baseline.get("mean_rmse"))
        drift_score = sum(abs(value) for value in (co2_delta, h2o_delta, rmse_delta) if value is not None)
        analyzers.append(
            {
                "analyzer_id": analyzer.get("analyzer_id"),
                "history_points": len(history),
                "baseline_run_id": baseline.get("run_id"),
                "latest_run_id": latest.get("run_id"),
                "mean_co2_ppm_delta": co2_delta,
                "mean_h2o_mmol_delta": h2o_delta,
                "rmse_delta": rmse_delta,
                "drift_score": drift_score,
                "status": "ok",
            }
        )
    return {
        "analyzer_count": len(analyzers),
        "analyzers": analyzers,
    }
