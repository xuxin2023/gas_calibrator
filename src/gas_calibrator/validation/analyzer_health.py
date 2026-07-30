"""Pure offline analyzer-health summaries for simulation and replay evidence."""

from __future__ import annotations

from statistics import mean
from typing import Any


def _bounded_rate(numerator: float, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return max(0.0, min(1.0, float(numerator) / denominator))


def _health_status(score: float | None) -> str:
    if score is None:
        return "not_evaluated"
    if score >= 85.0:
        return "healthy"
    if score >= 65.0:
        return "watch"
    return "critical"


def _score_analyzer(analyzer: dict[str, Any]) -> float | None:
    sample_count = max(int(analyzer.get("sample_count") or 0), 0)
    run_count = max(int(analyzer.get("run_count") or 0), 0)
    if sample_count <= 0 or run_count <= 0:
        return None
    fit_result_count = max(int(analyzer.get("fit_result_count") or 0), 0)
    qc_fail_rate = _bounded_rate(
        float(analyzer.get("qc_fail_count") or 0.0),
        sample_count,
    )
    alarm_density = max(
        0.0,
        float(analyzer.get("alarm_count") or 0.0) / run_count,
    )
    mean_rmse = abs(float(analyzer.get("mean_rmse") or 0.0))
    rmse_penalty = min(mean_rmse * 500.0, 20.0)
    qc_penalty = min(qc_fail_rate * 60.0, 60.0)
    alarm_penalty = min(alarm_density * 12.0, 15.0)
    fit_penalty = 0.0 if fit_result_count > 0 else 10.0
    return max(
        0.0,
        round(
            100.0
            - rmse_penalty
            - qc_penalty
            - alarm_penalty
            - fit_penalty,
            2,
        ),
    )


def build_analyzer_health(
    features: dict[str, Any],
    **_: Any,
) -> dict[str, Any]:
    """Score aggregate run/QC features without claiming live device health."""

    analyzers: list[dict[str, Any]] = []
    for analyzer in features.get("analyzers", []):
        sample_count = max(int(analyzer.get("sample_count") or 0), 0)
        run_count = max(int(analyzer.get("run_count") or 0), 0)
        score = _score_analyzer(analyzer)
        analyzers.append(
            {
                "analyzer_id": analyzer.get("analyzer_id"),
                "analyzer_serial": analyzer.get("analyzer_serial"),
                "run_count": run_count,
                "sample_count": sample_count,
                "fit_result_count": max(
                    int(analyzer.get("fit_result_count") or 0),
                    0,
                ),
                "mean_rmse": analyzer.get("mean_rmse"),
                "mean_r_squared": analyzer.get("mean_r_squared"),
                "qc_fail_rate": (
                    _bounded_rate(
                        float(analyzer.get("qc_fail_count") or 0.0),
                        sample_count,
                    )
                    if sample_count > 0
                    else None
                ),
                "alarm_density": (
                    round(
                        max(
                            0.0,
                            float(analyzer.get("alarm_count") or 0.0)
                            / run_count,
                        ),
                        4,
                    )
                    if run_count > 0
                    else None
                ),
                "health_score": score,
                "status": _health_status(score),
            }
        )
    analyzers.sort(
        key=lambda item: (
            item["health_score"] is not None,
            item["health_score"] or 0.0,
            item["analyzer_id"] or "",
        )
    )
    evaluated_count = sum(
        row["health_score"] is not None for row in analyzers
    )
    return {
        "analyzer_count": len(analyzers),
        "evaluated_count": evaluated_count,
        "not_evaluated_count": len(analyzers) - evaluated_count,
        "analyzers": analyzers,
        "evaluation_scope": "offline_features_only",
        "not_real_acceptance_evidence": True,
    }


def _average(values: list[float | int | None]) -> float | None:
    numbers = [float(item) for item in values if item is not None]
    return mean(numbers) if numbers else None


def _delta(
    current: float | None,
    baseline: float | None,
) -> float | None:
    if current is None or baseline is None:
        return None
    return float(current) - float(baseline)


def _drift_penalty(history: list[dict[str, Any]]) -> float | None:
    if len(history) < 2:
        return None
    baseline = history[0]
    latest = history[-1]
    ratio_deltas = [
        abs(value)
        for value in (
            _delta(
                latest.get("mean_co2_ratio_f"),
                baseline.get("mean_co2_ratio_f"),
            ),
            _delta(
                latest.get("mean_h2o_ratio_f"),
                baseline.get("mean_h2o_ratio_f"),
            ),
        )
        if value is not None
    ]
    ratio_delta = max(ratio_deltas, default=0.0)
    rmse_delta = abs(
        float(
            _delta(
                latest.get("mean_rmse"),
                baseline.get("mean_rmse"),
            )
            or 0.0
        )
    )
    return min(1.0, (ratio_delta * 20.0) + (rmse_delta * 20.0))


def _health_band(score: float | None) -> str:
    if score is None:
        return "not_evaluated"
    if score >= 90.0:
        return "excellent"
    if score >= 75.0:
        return "good"
    if score >= 60.0:
        return "watch"
    return "poor"


def build_instrument_health(
    features: dict[str, Any],
    **_: Any,
) -> dict[str, Any]:
    """Score frame/QC features while preserving missing-evidence states."""

    analyzers_output: list[dict[str, Any]] = []
    for item in features.get("analyzer_features", []):
        frame_count = max(int(item.get("frame_count") or 0), 0)
        point_count = max(int(item.get("point_count") or 0), 0)
        run_count = max(int(item.get("run_count") or 0), 0)
        evidence_sufficient = (
            frame_count > 0 and point_count > 0 and run_count > 0
        )
        abnormal_rate = (
            _bounded_rate(
                float(item.get("abnormal_status_count") or 0.0),
                frame_count,
            )
            if frame_count > 0
            else None
        )
        qc_fail_rate = (
            _bounded_rate(
                float(item.get("qc_fail_count") or 0.0),
                point_count,
            )
            if point_count > 0
            else None
        )
        history = list(item.get("history", []))
        drift_penalty = _drift_penalty(history)
        usable_rate = max(
            0.0,
            min(1.0, float(item.get("usable_rate") or 0.0)),
        )
        health_score: float | None = None
        if evidence_sufficient:
            weighted_score = (
                (0.50 * usable_rate)
                + (0.20 * (1.0 - float(abnormal_rate or 0.0)))
                + (0.20 * (1.0 - float(qc_fail_rate or 0.0)))
            )
            available_weight = 0.90
            if drift_penalty is not None:
                weighted_score += 0.10 * (1.0 - drift_penalty)
                available_weight += 0.10
            health_score = round(
                100.0 * weighted_score / available_weight,
                2,
            )
        analyzers_output.append(
            {
                "analyzer_label": item.get("analyzer_label"),
                "frame_count": frame_count,
                "run_count": run_count,
                "usable_rate": usable_rate,
                "abnormal_status_rate": abnormal_rate,
                "qc_fail_rate": qc_fail_rate,
                "drift_penalty": drift_penalty,
                "drift_status": (
                    "evaluated"
                    if drift_penalty is not None
                    else "not_evaluated"
                ),
                "health_score": health_score,
                "health_band": _health_band(health_score),
            }
        )

    evaluated_count = sum(
        row["health_score"] is not None for row in analyzers_output
    )
    return {
        "analyzer_count": len(analyzers_output),
        "evaluated_count": evaluated_count,
        "not_evaluated_count": len(analyzers_output) - evaluated_count,
        "average_health_score": _average(
            [item.get("health_score") for item in analyzers_output]
        ),
        "analyzers": analyzers_output,
        "evaluation_scope": "offline_frames_and_qc_only",
        "not_real_acceptance_evidence": True,
    }


__all__ = ["build_analyzer_health", "build_instrument_health"]
