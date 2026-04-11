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


def _same_family_refit_frame(
    absorbance_point_variants: pd.DataFrame,
    *,
    analyzer_id: str,
    selected_source_pair: str,
    fixed_model_family: str,
    fixed_zero_residual_mode: str,
    fixed_prediction_scope: str,
    config: Any,
) -> tuple[pd.DataFrame, str]:
    ratio_source = _ratio_source_from_pair(selected_source_pair)
    subset = absorbance_point_variants[
        (absorbance_point_variants["analyzer"].astype(str) == analyzer_id)
        & (absorbance_point_variants["ratio_source"].astype(str) == ratio_source)
        & (absorbance_point_variants["zero_residual_mode"].astype(str) == fixed_zero_residual_mode)
    ].copy()
    if subset.empty:
        return pd.DataFrame(), ""
    subset["target_ppm"] = pd.to_numeric(subset["target_co2_ppm"], errors="coerce")
    subset["temp_c"] = pd.to_numeric(subset["temp_set_c"], errors="coerce")
    subset["temp_model_c"] = pd.to_numeric(subset["temp_use_mean_c"], errors="coerce").combine_first(subset["temp_c"])
    subset["group_key"] = (
        subset["temp_c"].map(lambda value: "nan" if pd.isna(value) else f"{float(value):g}")
        + "|"
        + subset["target_ppm"].map(lambda value: "nan" if pd.isna(value) else f"{float(value):g}")
        + "|"
        + subset["point_title"].fillna("").astype(str)
    )
    subset["selected_source_pair"] = selected_source_pair
    subset["selected_ratio_source"] = ratio_source

    score_metric = _score_metric_name(config)
    candidate_rows: list[dict[str, Any]] = []
    residual_rows: list[dict[str, Any]] = []
    for spec in active_model_specs(config):
        if spec.model_family != fixed_model_family:
            continue
        try:
            score_row, _coeff_rows, spec_residual_rows = _fit_one_candidate(
                analyzer_df=subset,
                spec=spec,
                strategy=config.model_selection_strategy,
                score_weights=config.composite_weight_map(),
                legacy_score_weights=config.legacy_composite_weight_map(),
                enable_composite_score=config.enable_composite_score,
                absorbance_column="A_mean",
            )
        except Exception:
            continue
        candidate_rows.append(score_row)
        residual_rows.extend(spec_residual_rows)
    if not candidate_rows:
        return pd.DataFrame(), ""
    scores = pd.DataFrame(candidate_rows).sort_values(
        [score_metric, "validation_rmse", "overall_rmse", "model_id"],
        ignore_index=True,
    )
    chosen_model = str(scores.iloc[0]["model_id"])
    residual_df = pd.DataFrame(residual_rows)
    residual_df = residual_df[residual_df["model_id"].astype(str) == chosen_model].copy()
    scope_frame, _actual_scope = _prediction_frame_for_scope(residual_df, fixed_prediction_scope)
    if scope_frame.empty:
        return pd.DataFrame(), chosen_model
    scope_frame = scope_frame.rename(columns={"predicted_ppm": "sidecar_pred_ppm", "error_ppm": "sidecar_error_ppm"})
    return scope_frame[["point_title", "point_row", "target_ppm", "sidecar_pred_ppm", "sidecar_error_ppm"]].copy(), chosen_model


def _fit_linear_residual_correction(
    frame: pd.DataFrame,
    *,
    feature_columns: list[str],
    target_column: str,
    use_leave_one_out: bool,
) -> tuple[pd.Series, str]:
    valid = frame.dropna(subset=[*feature_columns, target_column]).copy()
    predictions = pd.Series(0.0, index=frame.index, dtype=float)
    if valid.empty:
        return predictions, "no_usable_feature_rows"

    def _solve(train_df: pd.DataFrame, predict_df: pd.DataFrame) -> np.ndarray:
        x_train = np.column_stack(
            [np.ones(len(train_df), dtype=float), *[pd.to_numeric(train_df[column], errors="coerce").to_numpy(dtype=float) for column in feature_columns]]
        )
        y_train = pd.to_numeric(train_df[target_column], errors="coerce").to_numpy(dtype=float)
        coeffs, _, _, _ = np.linalg.lstsq(x_train, y_train, rcond=None)
        x_predict = np.column_stack(
            [np.ones(len(predict_df), dtype=float), *[pd.to_numeric(predict_df[column], errors="coerce").to_numpy(dtype=float) for column in feature_columns]]
        )
        raw = x_predict @ coeffs
        cap = float(np.nanmax(np.abs(y_train))) if len(y_train) else math.nan
        return np.clip(raw, -cap, cap) if math.isfinite(cap) and cap > 0.0 else raw

    if use_leave_one_out and len(valid) > len(feature_columns) + 1:
        oof = pd.Series(np.nan, index=valid.index, dtype=float)
        for row_index in valid.index:
            train_df = valid.drop(index=row_index)
            if len(train_df) <= len(feature_columns):
                continue
            oof.loc[row_index] = float(_solve(train_df, valid.loc[[row_index]])[0])
        if oof.notna().sum() == len(valid):
            predictions.loc[valid.index] = oof
            return predictions, "leave_one_out"
    predictions.loc[valid.index] = _solve(valid, valid)
    return predictions, "full_fit"


def _mode_frame(
    base_frame: pd.DataFrame,
    *,
    sidecar_mode: str,
    pred_column: str,
    error_column: str,
) -> pd.DataFrame:
    frame = base_frame.copy()
    frame["sidecar_mode"] = sidecar_mode
    frame["sidecar_pred_ppm"] = pd.to_numeric(frame[pred_column], errors="coerce")
    frame["sidecar_error_ppm"] = pd.to_numeric(frame[error_column], errors="coerce")
    frame["abs_error_old"] = pd.to_numeric(frame["old_error"], errors="coerce").abs()
    frame["abs_error_current"] = pd.to_numeric(frame["new_error"], errors="coerce").abs()
    frame["abs_error_sidecar"] = pd.to_numeric(frame["sidecar_error_ppm"], errors="coerce").abs()
    return frame


def _detail_row(
    mode_frame: pd.DataFrame,
    *,
    analyzer_id: str,
    sidecar_mode: str,
    selected_source_pair: str,
    fixed_model_family: str,
    fixed_zero_residual_mode: str,
    fixed_prediction_scope: str,
    current_frame: pd.DataFrame,
) -> dict[str, Any]:
    old_overall = _metrics(mode_frame["old_error"])["rmse"]
    current_overall = _metrics(current_frame["sidecar_error_ppm"])["rmse"]
    current_zero = _segment_rmse(current_frame, "sidecar_error_ppm", "zero")
    current_low = _segment_rmse(current_frame, "sidecar_error_ppm", "low")
    current_main = _segment_rmse(current_frame, "sidecar_error_ppm", "main")
    overall = _metrics(mode_frame["sidecar_error_ppm"])["rmse"]
    zero = _segment_rmse(mode_frame, "sidecar_error_ppm", "zero")
    low = _segment_rmse(mode_frame, "sidecar_error_ppm", "low")
    main = _segment_rmse(mode_frame, "sidecar_error_ppm", "main")
    old_zero = _segment_rmse(mode_frame, "old_error", "zero")
    return {
        "analyzer_id": analyzer_id,
        "sidecar_mode": sidecar_mode,
        "selected_source_pair": selected_source_pair,
        "fixed_model_family": fixed_model_family,
        "fixed_zero_residual_mode": fixed_zero_residual_mode,
        "fixed_prediction_scope": fixed_prediction_scope,
        "zero_rmse": zero,
        "low_range_rmse": low,
        "main_range_rmse": main,
        "overall_rmse": overall,
        "temp_bias_spread": _temp_bias_spread(mode_frame, "sidecar_error_ppm"),
        "gap_to_old_overall": overall - old_overall if pd.notna(overall) and pd.notna(old_overall) else math.nan,
        "gap_to_old_zero": zero - old_zero if pd.notna(zero) and pd.notna(old_zero) else math.nan,
        "delta_vs_current_ga01_overall": current_overall - overall if pd.notna(current_overall) and pd.notna(overall) else math.nan,
        "delta_vs_current_ga01_zero": current_zero - zero if pd.notna(current_zero) and pd.notna(zero) else math.nan,
        "delta_vs_current_ga01_low": current_low - low if pd.notna(current_low) and pd.notna(low) else math.nan,
        "delta_vs_current_ga01_main": current_main - main if pd.notna(current_main) and pd.notna(main) else math.nan,
        "pointwise_win_count_vs_old": int((mode_frame["abs_error_sidecar"] < mode_frame["abs_error_old"] - 1.0e-12).fillna(False).sum()),
        "pointwise_win_count_vs_current_ga01": int((mode_frame["abs_error_sidecar"] < mode_frame["abs_error_current"] - 1.0e-12).fillna(False).sum()),
    }
