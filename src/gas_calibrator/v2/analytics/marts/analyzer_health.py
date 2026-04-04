from __future__ import annotations

from typing import Any


def _score_analyzer(analyzer: dict[str, Any]) -> float:
    sample_count = max(int(analyzer.get("sample_count") or 0), 1)
    qc_fail_count = float(analyzer.get("qc_fail_count") or 0.0)
    fit_result_count = max(int(analyzer.get("fit_result_count") or 0), 1)
    alarm_count = float(analyzer.get("alarm_count") or 0.0)
    mean_rmse = float(analyzer.get("mean_rmse") or 0.0)

    qc_fail_rate = qc_fail_count / sample_count
    alarm_density = alarm_count / max(int(analyzer.get("run_count") or 0), 1)
    rmse_penalty = min(mean_rmse * 500.0, 20.0)
    qc_penalty = min(qc_fail_rate * 60.0, 60.0)
    alarm_penalty = min(alarm_density * 12.0, 15.0)
    fit_penalty = 0.0 if fit_result_count > 0 else 10.0
    return max(0.0, round(100.0 - rmse_penalty - qc_penalty - alarm_penalty - fit_penalty, 2))


def _health_status(score: float) -> str:
    if score >= 85.0:
        return "healthy"
    if score >= 65.0:
        return "watch"
    return "critical"


def build_analyzer_health(features: dict[str, Any], **_: Any) -> dict[str, Any]:
    analyzers: list[dict[str, Any]] = []
    for analyzer in features.get("analyzers", []):
        score = _score_analyzer(analyzer)
        sample_count = max(int(analyzer.get("sample_count") or 0), 1)
        analyzers.append(
            {
                "analyzer_id": analyzer.get("analyzer_id"),
                "analyzer_serial": analyzer.get("analyzer_serial"),
                "run_count": analyzer.get("run_count"),
                "sample_count": analyzer.get("sample_count"),
                "fit_result_count": analyzer.get("fit_result_count"),
                "mean_rmse": analyzer.get("mean_rmse"),
                "mean_r_squared": analyzer.get("mean_r_squared"),
                "qc_fail_rate": round(float(analyzer.get("qc_fail_count") or 0.0) / sample_count, 4),
                "alarm_density": round(
                    float(analyzer.get("alarm_count") or 0.0) / max(int(analyzer.get("run_count") or 0), 1),
                    4,
                ),
                "health_score": score,
                "status": _health_status(score),
            }
        )
    analyzers.sort(key=lambda item: (item["health_score"], item["analyzer_id"] or ""))
    return {
        "analyzer_count": len(analyzers),
        "analyzers": analyzers,
    }
