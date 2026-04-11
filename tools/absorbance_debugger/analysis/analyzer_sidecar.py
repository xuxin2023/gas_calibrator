"""Analyzer-specific sidecar diagnostics for laggard analyzers."""

from __future__ import annotations

import math
from typing import Any

import numpy as np
import pandas as pd

from .absorbance_models import _fit_one_candidate, active_model_specs
from .remaining_gap import remaining_gap_total_from_frame


def _metrics(errors: pd.Series | np.ndarray) -> dict[str, float]:
    clean = pd.to_numeric(pd.Series(errors), errors="coerce").dropna()
    if clean.empty:
        return {"rmse": math.nan, "mae": math.nan}
    values = clean.to_numpy(dtype=float)
    return {
        "rmse": float(np.sqrt(np.mean(np.square(values)))),
        "mae": float(np.mean(np.abs(values))),
    }


def _segment_mask(frame: pd.DataFrame, segment_tag: str) -> pd.Series:
    target = pd.to_numeric(frame["target_ppm"], errors="coerce")
    if segment_tag == "zero":
        return target == 0.0
    if segment_tag == "low":
        return (target > 0.0) & (target <= 200.0)
    if segment_tag == "main":
        return (target > 200.0) & (target <= 1000.0)
    raise ValueError(f"Unsupported segment_tag: {segment_tag}")


def _segment_rmse(frame: pd.DataFrame, error_column: str, segment_tag: str) -> float:
    subset = frame[_segment_mask(frame, segment_tag)].copy()
    return _metrics(subset[error_column])["rmse"]


def _temp_bias_spread(frame: pd.DataFrame, error_column: str) -> float:
    grouped = (
        frame.dropna(subset=["temp_c", error_column])
        .groupby("temp_c", dropna=False)[error_column]
        .mean()
    )
    if grouped.empty:
        return math.nan
    return float(np.std(grouped.to_numpy(dtype=float)))


def _score_metric_name(config: Any) -> str:
    return "composite_score" if bool(getattr(config, "enable_composite_score", True)) else "validation_rmse"


def _ratio_source_from_pair(source_pair: str) -> str:
    return "ratio_co2_raw" if str(source_pair) == "raw/raw" else "ratio_co2_filt"


def _point_feature_summary(filtered_samples: pd.DataFrame, analyzer_id: str, selected_source_pair: str) -> pd.DataFrame:
    subset = filtered_samples[filtered_samples["analyzer"].astype(str) == analyzer_id].copy()
    if subset.empty:
        return pd.DataFrame()
    summary = (
        subset.groupby(["analyzer", "point_title", "point_row"], dropna=False)
        .agg(
            target_ppm=("target_co2_ppm", "mean"),
            temp_set_c=("temp_set_c", "mean"),
            temp_cavity_c_mean=("temp_cavity_c", "mean"),
            temp_shell_c_mean=("temp_shell_c", "mean"),
            ratio_h2o_raw_mean=("ratio_h2o_raw", "mean"),
            ratio_h2o_filt_mean=("ratio_h2o_filt", "mean"),
            h2o_density_mean=("h2o_density", "mean"),
            ratio_co2_raw_mean=("ratio_co2_raw", "mean"),
            ratio_co2_filt_mean=("ratio_co2_filt", "mean"),
        )
        .reset_index()
        .rename(columns={"analyzer": "analyzer_id"})
    )
    if selected_source_pair == "raw/raw":
        summary["humidity_ratio_selected"] = pd.to_numeric(summary["ratio_h2o_raw_mean"], errors="coerce").combine_first(
            pd.to_numeric(summary["ratio_h2o_filt_mean"], errors="coerce")
        )
        summary["co2_ratio_selected"] = pd.to_numeric(summary["ratio_co2_raw_mean"], errors="coerce").combine_first(
            pd.to_numeric(summary["ratio_co2_filt_mean"], errors="coerce")
        )
    else:
        summary["humidity_ratio_selected"] = pd.to_numeric(summary["ratio_h2o_filt_mean"], errors="coerce").combine_first(
            pd.to_numeric(summary["ratio_h2o_raw_mean"], errors="coerce")
        )
        summary["co2_ratio_selected"] = pd.to_numeric(summary["ratio_co2_filt_mean"], errors="coerce").combine_first(
            pd.to_numeric(summary["ratio_co2_raw_mean"], errors="coerce")
        )
    summary["temp_feature_c"] = pd.to_numeric(summary["temp_cavity_c_mean"], errors="coerce").combine_first(
        pd.to_numeric(summary["temp_shell_c_mean"], errors="coerce")
    ).combine_first(pd.to_numeric(summary["temp_set_c"], errors="coerce"))
    return summary


def _prepare_base_frame(
    point_reconciliation: pd.DataFrame,
    filtered_samples: pd.DataFrame,
    *,
    analyzer_id: str,
    selected_source_pair: str,
    fixed_model_family: str,
    fixed_zero_residual_mode: str,
    fixed_prediction_scope: str,
) -> pd.DataFrame:
    base = point_reconciliation[point_reconciliation["analyzer_id"].astype(str) == analyzer_id].copy()
    if base.empty:
        return base
    base["target_ppm"] = pd.to_numeric(base["target_ppm"], errors="coerce")
    base["temp_c"] = pd.to_numeric(base["temp_c"], errors="coerce")
    base["old_error"] = pd.to_numeric(base["old_error"], errors="coerce")
    base["new_error"] = pd.to_numeric(base["new_error"], errors="coerce")
    base["old_pred_ppm"] = pd.to_numeric(base["old_pred_ppm"], errors="coerce")
    base["new_pred_ppm"] = pd.to_numeric(base["new_pred_ppm"], errors="coerce")
    point_features = _point_feature_summary(filtered_samples, analyzer_id, selected_source_pair)
    base = base.merge(
        point_features,
        on=["analyzer_id", "point_title", "point_row"],
        how="left",
        suffixes=("", "_feature"),
    )
    base["selected_source_pair"] = selected_source_pair
    base["fixed_model_family"] = fixed_model_family
    base["fixed_zero_residual_mode"] = fixed_zero_residual_mode
    base["fixed_prediction_scope"] = fixed_prediction_scope
    base["A_mean"] = pd.to_numeric(base["A_mean"], errors="coerce")
    base["temp_feature_c"] = pd.to_numeric(base["temp_feature_c"], errors="coerce").combine_first(base["temp_c"])
    base["humidity_ratio_selected"] = pd.to_numeric(base["humidity_ratio_selected"], errors="coerce")
    base["h2o_density_mean"] = pd.to_numeric(base["h2o_density_mean"], errors="coerce")
    a_mean = pd.to_numeric(base["A_mean"], errors="coerce")
    humidity = pd.to_numeric(base["humidity_ratio_selected"], errors="coerce")
    density = pd.to_numeric(base["h2o_density_mean"], errors="coerce")
    temp_feature = pd.to_numeric(base["temp_feature_c"], errors="coerce")
    base["A_centered"] = a_mean - float(a_mean.mean())
    base["humidity_ratio_selected_centered"] = humidity - float(humidity.mean())
    base["h2o_density_centered"] = density - float(density.mean())
    base["temp_centered"] = temp_feature - float(temp_feature.mean())
    temp_break = 20.0
    base["temp_hot_hinge"] = np.maximum(temp_feature - temp_break, 0.0)
    base["temp_cold_hinge"] = np.maximum(temp_break - temp_feature, 0.0)
    base["A_times_humidity"] = base["A_centered"] * base["humidity_ratio_selected_centered"]
    base["A_times_temp"] = base["A_centered"] * base["temp_centered"]
    return base


def _prediction_frame_for_scope(residual_df: pd.DataFrame, requested_scope: str) -> tuple[pd.DataFrame, str]:
    if residual_df.empty:
        return pd.DataFrame(), requested_scope
    scope_frame = residual_df[residual_df["prediction_scope"].astype(str) == requested_scope].copy()
    actual_scope = requested_scope
    if scope_frame.empty and requested_scope == "validation_oof":
        scope_frame = residual_df[residual_df["prediction_scope"].astype(str) == "overall_fit"].copy()
        actual_scope = "overall_fit_fallback"
    return scope_frame, actual_scope
