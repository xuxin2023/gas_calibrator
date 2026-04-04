from __future__ import annotations

from typing import Any


def _delta(current: float | None, baseline: float | None) -> float | None:
    if current is None or baseline is None:
        return None
    return float(current) - float(baseline)


def _relative_change(current: float | None, baseline: float | None) -> float | None:
    if current is None or baseline in (None, 0):
        return None
    return abs(float(current) - float(baseline)) / abs(float(baseline))


def _severity(max_ratio_delta: float | None, max_signal_change: float | None, rmse_delta: float | None) -> str:
    ratio_delta = float(max_ratio_delta or 0.0)
    signal_change = float(max_signal_change or 0.0)
    rmse = abs(float(rmse_delta or 0.0))
    if ratio_delta >= 0.02 or signal_change >= 0.15 or rmse >= 0.02:
        return "alert"
    if ratio_delta >= 0.01 or signal_change >= 0.08 or rmse >= 0.01:
        return "watch"
    return "stable"


def build_measurement_drift(features: dict[str, Any], **_: Any) -> dict[str, Any]:
    output: list[dict[str, Any]] = []
    severity_counts = {"stable": 0, "watch": 0, "alert": 0, "insufficient_history": 0}
    for analyzer in features.get("analyzer_features", []):
        history = list(analyzer.get("history", []))
        if len(history) < 2:
            severity_counts["insufficient_history"] += 1
            output.append(
                {
                    "analyzer_label": analyzer.get("analyzer_label"),
                    "run_count": analyzer.get("run_count", 0),
                    "status": "insufficient_history",
                    "baseline_run_id": history[0]["run_id"] if history else None,
                    "latest_run_id": history[-1]["run_id"] if history else None,
                }
            )
            continue

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
        signal_changes = [
            value
            for value in (
                _relative_change(latest.get("mean_ref_signal"), baseline.get("mean_ref_signal")),
                _relative_change(latest.get("mean_co2_signal"), baseline.get("mean_co2_signal")),
                _relative_change(latest.get("mean_h2o_signal"), baseline.get("mean_h2o_signal")),
            )
            if value is not None
        ]
        max_ratio_delta = max(ratio_deltas, default=0.0)
        max_signal_change = max(signal_changes, default=0.0)
        rmse_delta = _delta(latest.get("mean_rmse"), baseline.get("mean_rmse"))
        status = _severity(max_ratio_delta, max_signal_change, rmse_delta)
        severity_counts[status] += 1
        output.append(
            {
                "analyzer_label": analyzer.get("analyzer_label"),
                "run_count": analyzer.get("run_count", 0),
                "status": status,
                "baseline_run_id": baseline.get("run_id"),
                "latest_run_id": latest.get("run_id"),
                "co2_ppm_delta": _delta(latest.get("mean_co2_ppm"), baseline.get("mean_co2_ppm")),
                "h2o_mmol_delta": _delta(latest.get("mean_h2o_mmol"), baseline.get("mean_h2o_mmol")),
                "co2_ratio_f_delta": _delta(latest.get("mean_co2_ratio_f"), baseline.get("mean_co2_ratio_f")),
                "h2o_ratio_f_delta": _delta(latest.get("mean_h2o_ratio_f"), baseline.get("mean_h2o_ratio_f")),
                "ref_signal_relative_change": _relative_change(
                    latest.get("mean_ref_signal"),
                    baseline.get("mean_ref_signal"),
                ),
                "co2_signal_relative_change": _relative_change(
                    latest.get("mean_co2_signal"),
                    baseline.get("mean_co2_signal"),
                ),
                "h2o_signal_relative_change": _relative_change(
                    latest.get("mean_h2o_signal"),
                    baseline.get("mean_h2o_signal"),
                ),
                "rmse_delta": rmse_delta,
                "max_ratio_delta": max_ratio_delta,
                "max_signal_relative_change": max_signal_change,
            }
        )

    return {
        "analyzer_count": len(output),
        "severity_counts": severity_counts,
        "analyzers": output,
    }
