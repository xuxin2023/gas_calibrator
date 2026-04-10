"""Old-chain vs new-chain comparison helpers."""

from __future__ import annotations

import math
from typing import Any

import numpy as np
import pandas as pd

from .fits import fit_linear


CONCENTRATION_BUCKETS: tuple[tuple[str, float | None, float | None], ...] = (
    ("0 ppm", 0.0, 0.0),
    ("0~200 ppm", 0.0, 200.0),
    ("200~500 ppm", 200.0, 500.0),
    ("500~800 ppm", 500.0, 800.0),
    ("800~1000 ppm", 800.0, 1000.0),
)


def _metrics(errors: pd.Series) -> dict[str, float]:
    clean = pd.to_numeric(errors, errors="coerce").dropna()
    if clean.empty:
        return {
            "rmse": math.nan,
            "mae": math.nan,
            "max_abs_error": math.nan,
            "bias": math.nan,
            "std": math.nan,
        }
    values = clean.to_numpy(dtype=float)
    abs_values = np.abs(values)
    return {
        "rmse": float(np.sqrt(np.mean(np.square(values)))),
        "mae": float(np.mean(abs_values)),
        "max_abs_error": float(np.max(abs_values)),
        "bias": float(np.mean(values)),
        "std": float(np.std(values)),
    }


def _winner(old_value: float, new_value: float) -> str:
    if math.isnan(old_value) and math.isnan(new_value):
        return "tie"
    if math.isnan(old_value):
        return "new_chain"
    if math.isnan(new_value):
        return "old_chain"
    if abs(old_value - new_value) <= 1.0e-12:
        return "tie"
    return "old_chain" if old_value < new_value else "new_chain"


def _winner_from_metric_rows(old_metrics: dict[str, float], new_metrics: dict[str, float]) -> str:
    for key in ("rmse", "mae", "max_abs_error", "std"):
        winner = _winner(old_metrics[key], new_metrics[key])
        if winner != "tie":
            return winner
    return "tie"


def _stability_score(by_temp_subset: pd.DataFrame, column_name: str) -> float:
    values = pd.to_numeric(by_temp_subset[column_name], errors="coerce").dropna()
    if values.empty:
        return math.nan
    return float(np.std(values.to_numpy(dtype=float)))


def _range_mask(series: pd.Series, lower: float | None, upper: float | None) -> pd.Series:
    if lower == 0.0 and upper == 0.0:
        return series == 0.0
    mask = pd.Series(True, index=series.index)
    if lower is not None:
        mask &= series > lower
    if upper is not None:
        mask &= series <= upper
    return mask


def _regression_rows(frame: pd.DataFrame, analyzer_id: str, scope: str, temp_c: float | None) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for chain_name, pred_col in (("old_chain", "old_pred_ppm"), ("new_chain", "new_pred_ppm")):
        subset = frame.dropna(subset=["target_ppm", pred_col])
        if len(subset) < 2:
            continue
        fit = fit_linear(subset["target_ppm"], subset[pred_col])
        rows.append(
            {
                "analyzer_id": analyzer_id,
                "chain_name": chain_name,
                "scope": scope,
                "temp_c": temp_c,
                "sample_count": fit.sample_count,
                "slope": fit.slope,
                "intercept": fit.intercept,
                "r2": fit.r2,
                "formula": fit.formula(x_name="target_ppm"),
            }
        )
    return rows


def build_comparison_outputs(point_reconciliation: pd.DataFrame) -> dict[str, pd.DataFrame | list[str]]:
    """Build step-08 comparison tables and automatic conclusions."""

    compare = point_reconciliation.copy()
    compare["target_ppm"] = pd.to_numeric(compare["target_ppm"], errors="coerce")
    compare["temp_c"] = pd.to_numeric(compare["temp_c"], errors="coerce")
    compare["old_error"] = pd.to_numeric(compare["old_error"], errors="coerce")
    compare["new_error"] = pd.to_numeric(compare["new_error"], errors="coerce")

    overview_rows: list[dict[str, Any]] = []
    by_temp_rows: list[dict[str, Any]] = []
    by_range_rows: list[dict[str, Any]] = []
    zero_rows: list[dict[str, Any]] = []
    regression_overall_rows: list[dict[str, Any]] = []
    regression_by_temp_rows: list[dict[str, Any]] = []
    conclusion_lines: list[str] = []
    auto_conclusion_rows: list[dict[str, Any]] = []

    for analyzer_id, analyzer_df in compare.groupby("analyzer_id"):
        old_metrics = _metrics(analyzer_df["old_error"])
        new_metrics = _metrics(analyzer_df["new_error"])
        winner_overall = _winner_from_metric_rows(old_metrics, new_metrics)

        zero_df = analyzer_df[analyzer_df["target_ppm"] == 0].copy()
        old_zero_metrics = _metrics(zero_df["old_error"])
        new_zero_metrics = _metrics(zero_df["new_error"])
        winner_zero = _winner_from_metric_rows(old_zero_metrics, new_zero_metrics)

        for temp_c, temp_df in analyzer_df.groupby("temp_c"):
            old_temp_metrics = _metrics(temp_df["old_error"])
            new_temp_metrics = _metrics(temp_df["new_error"])
            by_temp_rows.append(
                {
                    "analyzer_id": analyzer_id,
                    "temp_c": temp_c,
                    "old_rmse": old_temp_metrics["rmse"],
                    "new_rmse": new_temp_metrics["rmse"],
                    "old_bias": old_temp_metrics["bias"],
                    "new_bias": new_temp_metrics["bias"],
                    "old_max_abs_error": old_temp_metrics["max_abs_error"],
                    "new_max_abs_error": new_temp_metrics["max_abs_error"],
                    "winner_at_temp": _winner_from_metric_rows(old_temp_metrics, new_temp_metrics),
                }
            )
            temp_zero_df = temp_df[temp_df["target_ppm"] == 0].copy()
            if not temp_zero_df.empty:
                old_temp_zero = _metrics(temp_zero_df["old_error"])
                new_temp_zero = _metrics(temp_zero_df["new_error"])
                zero_rows.append(
                    {
                        "analyzer_id": analyzer_id,
                        "temp_c": temp_c,
                        "old_zero_mean_error": old_temp_zero["bias"],
                        "new_zero_mean_error": new_temp_zero["bias"],
                        "old_zero_std": old_temp_zero["std"],
                        "new_zero_std": new_temp_zero["std"],
                        "old_zero_max_abs_error": old_temp_zero["max_abs_error"],
                        "new_zero_max_abs_error": new_temp_zero["max_abs_error"],
                    }
                )
            regression_by_temp_rows.extend(_regression_rows(temp_df, analyzer_id, "per_temp", temp_c))

        by_temp_df = pd.DataFrame([row for row in by_temp_rows if row["analyzer_id"] == analyzer_id])
        old_temp_stability_score = _stability_score(by_temp_df, "old_bias")
        new_temp_stability_score = _stability_score(by_temp_df, "new_bias")
        winner_temp_stability = _winner(old_temp_stability_score, new_temp_stability_score)

        for bucket_name, lower, upper in CONCENTRATION_BUCKETS:
            bucket_df = analyzer_df[_range_mask(analyzer_df["target_ppm"], lower, upper)].copy()
            old_bucket = _metrics(bucket_df["old_error"])
            new_bucket = _metrics(bucket_df["new_error"])
            by_range_rows.append(
                {
                    "analyzer_id": analyzer_id,
                    "concentration_range": bucket_name,
                    "old_rmse": old_bucket["rmse"],
                    "new_rmse": new_bucket["rmse"],
                    "old_bias": old_bucket["bias"],
                    "new_bias": new_bucket["bias"],
                    "old_max_abs_error": old_bucket["max_abs_error"],
                    "new_max_abs_error": new_bucket["max_abs_error"],
                    "winner_in_range": _winner_from_metric_rows(old_bucket, new_bucket),
                }
            )

        high_df = pd.DataFrame(
            [
                row
                for row in by_range_rows
                if row["analyzer_id"] == analyzer_id and row["concentration_range"] == "800~1000 ppm"
            ]
        )
        winner_high = "tie"
        if not high_df.empty:
            winner_high = str(high_df.iloc[0]["winner_in_range"])

        old_wins = sum(item == "old_chain" for item in (winner_overall, winner_zero, winner_temp_stability, winner_high))
        new_wins = sum(item == "new_chain" for item in (winner_overall, winner_zero, winner_temp_stability, winner_high))
        if old_wins >= 3:
            recommendation = "Keep old chain as the release baseline; use new chain for diagnostics and model development."
        elif new_wins >= 3:
            recommendation = "New chain is consistently stronger; continue validation toward a dedicated absorbance calibration model."
        else:
            recommendation = "Mixed result; keep old chain for release and use new chain for transparent troubleshooting only."

        overview_rows.append(
            {
                "analyzer_id": analyzer_id,
                "old_chain_rmse": old_metrics["rmse"],
                "new_chain_rmse": new_metrics["rmse"],
                "old_chain_mae": old_metrics["mae"],
                "new_chain_mae": new_metrics["mae"],
                "old_chain_max_abs_error": old_metrics["max_abs_error"],
                "new_chain_max_abs_error": new_metrics["max_abs_error"],
                "old_chain_bias": old_metrics["bias"],
                "new_chain_bias": new_metrics["bias"],
                "winner_overall": winner_overall,
                "winner_zero": winner_zero,
                "winner_temp_stability": winner_temp_stability,
                "recommendation": recommendation,
            }
        )
        regression_overall_rows.extend(_regression_rows(analyzer_df, analyzer_id, "overall", None))
        conclusion_lines.append(
            f"{analyzer_id}: overall={winner_overall}, zero={winner_zero}, temp stability={winner_temp_stability}, "
            f"high concentration={winner_high}. {recommendation}"
        )

    overview_df = pd.DataFrame(overview_rows)
    by_temp_df = pd.DataFrame(by_temp_rows).sort_values(["analyzer_id", "temp_c"], ignore_index=True)
    by_range_df = pd.DataFrame(by_range_rows).sort_values(["analyzer_id", "concentration_range"], ignore_index=True)
    zero_df = pd.DataFrame(zero_rows).sort_values(["analyzer_id", "temp_c"], ignore_index=True)
    regression_overall_df = pd.DataFrame(regression_overall_rows).sort_values(["analyzer_id", "chain_name"], ignore_index=True)
    regression_by_temp_df = pd.DataFrame(regression_by_temp_rows).sort_values(["analyzer_id", "temp_c", "chain_name"], ignore_index=True)

    def _category_winner(column_name: str) -> str:
        values = overview_df[column_name].astype(str).tolist() if not overview_df.empty else []
        old_count = values.count("old_chain")
        new_count = values.count("new_chain")
        if old_count == new_count:
            return "tie"
        return "old_chain" if old_count > new_count else "new_chain"

    high_bucket = by_range_df[by_range_df["concentration_range"] == "800~1000 ppm"].copy()
    high_winners = high_bucket["winner_in_range"].astype(str).tolist() if not high_bucket.empty else []
    old_high = high_winners.count("old_chain")
    new_high = high_winners.count("new_chain")
    high_conc_winner = "tie" if old_high == new_high else ("old_chain" if old_high > new_high else "new_chain")

    overall_winner = _category_winner("winner_overall")
    zero_winner = _category_winner("winner_zero")
    temp_winner = _category_winner("winner_temp_stability")
    if [overall_winner, zero_winner, temp_winner, high_conc_winner].count("old_chain") >= 3:
        top_recommendation = "Keep the old ratio-based chain as the benchmark; treat the new absorbance chain as a debug surface."
    elif [overall_winner, zero_winner, temp_winner, high_conc_winner].count("new_chain") >= 3:
        top_recommendation = "The new absorbance chain is trending stronger overall; expand validation and consider a dedicated absorbance prediction model."
    else:
        top_recommendation = "Results are mixed. Keep the old chain for release decisions and use the new chain to explain failure modes."

    auto_conclusion_rows.extend(
        [
            {"category": "overall", "winner": overall_winner, "summary": f"Overall winner across analyzers: {overall_winner}"},
            {"category": "0 ppm", "winner": zero_winner, "summary": f"Zero-point winner across analyzers: {zero_winner}"},
            {"category": "temp stability", "winner": temp_winner, "summary": f"Temperature-stability winner across analyzers: {temp_winner}"},
            {"category": "high concentration", "winner": high_conc_winner, "summary": f"High-concentration winner across analyzers: {high_conc_winner}"},
            {"category": "recommendation", "winner": "mixed", "summary": top_recommendation},
        ]
    )

    return {
        "overview_summary": overview_df,
        "by_temperature": by_temp_df,
        "by_concentration_range": by_range_df,
        "zero_special": zero_df,
        "regression_overall": regression_overall_df,
        "regression_by_temperature": regression_by_temp_df,
        "point_reconciliation": compare,
        "auto_conclusions": pd.DataFrame(auto_conclusion_rows),
        "conclusion_lines": conclusion_lines,
    }
