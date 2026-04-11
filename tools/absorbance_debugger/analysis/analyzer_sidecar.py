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


def _format_point_label(row: pd.Series) -> str:
    temp_value = pd.to_numeric(pd.Series([row.get("temp_c", row.get("temp_set_c"))]), errors="coerce").iloc[0]
    target_value = pd.to_numeric(pd.Series([row.get("target_ppm")]), errors="coerce").iloc[0]
    segment = str(row.get("segment_tag") or "")
    temp_text = "nanC" if pd.isna(temp_value) else f"{float(temp_value):g}C"
    target_text = "nanppm" if pd.isna(target_value) else f"{float(target_value):g}ppm"
    return f"{temp_text}/{target_text}/{segment}"


def _format_example_series(frame: pd.DataFrame, value_column: str) -> str:
    if frame.empty or value_column not in frame.columns:
        return ""
    ranked = frame.dropna(subset=[value_column]).copy()
    if ranked.empty:
        return ""
    ranked = ranked.sort_values([value_column], ascending=[False], ignore_index=True).head(3)
    return "|".join(_format_point_label(row) for _, row in ranked.iterrows())


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
    base["reconciliation_row_id"] = base.index.to_numpy()
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
    output = scope_frame[["point_title", "point_row", "target_ppm", "sidecar_pred_ppm", "sidecar_error_ppm"]].copy()
    output["analyzer_id"] = analyzer_id
    return output[["analyzer_id", "point_title", "point_row", "target_ppm", "sidecar_pred_ppm", "sidecar_error_ppm"]], chosen_model


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
    sidecar_vs_current = mode_frame["abs_error_current"] - mode_frame["abs_error_sidecar"]
    sidecar_vs_current_loss = mode_frame["abs_error_sidecar"] - mode_frame["abs_error_current"]
    win_rows = mode_frame[sidecar_vs_current > 1.0e-12].copy()
    win_rows["local_gain"] = sidecar_vs_current.loc[win_rows.index]
    loss_rows = mode_frame[sidecar_vs_current_loss > 1.0e-12].copy()
    loss_rows["local_loss"] = sidecar_vs_current_loss.loc[loss_rows.index]
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
        "local_win_examples": _format_example_series(win_rows, "local_gain"),
        "local_loss_examples": _format_example_series(loss_rows, "local_loss"),
    }


def build_analyzer_sidecar_challenge(
    *,
    point_reconciliation: pd.DataFrame,
    filtered_samples: pd.DataFrame,
    absorbance_point_variants: pd.DataFrame,
    selection_table: pd.DataFrame,
    config: Any,
    laggard_analyzer_id: str = "GA01",
) -> dict[str, pd.DataFrame]:
    """Run analyzer-specific sidecar diagnostics without touching the global deployable winner."""

    point_reconciliation = point_reconciliation.copy()
    filtered_samples = filtered_samples.copy()
    absorbance_point_variants = absorbance_point_variants.copy()
    selection = selection_table.copy()
    if point_reconciliation.empty or selection.empty:
        empty = pd.DataFrame()
        return {"detail": empty, "summary": empty, "conclusions": empty}

    if "selected_source_pair" not in point_reconciliation.columns and "ratio_source_selected" in point_reconciliation.columns:
        point_reconciliation["selected_source_pair"] = np.where(
            point_reconciliation["ratio_source_selected"].astype(str).str.contains("raw", case=False, na=False),
            "raw/raw",
            "filt/filt",
        )
    if "best_model_family" not in point_reconciliation.columns:
        point_reconciliation["best_model_family"] = ""
    if "selected_prediction_scope" not in point_reconciliation.columns:
        point_reconciliation["selected_prediction_scope"] = "validation_oof"
    if "zero_residual_mode" not in point_reconciliation.columns:
        point_reconciliation["zero_residual_mode"] = "none"

    point_reconciliation["old_error"] = pd.to_numeric(point_reconciliation["old_error"], errors="coerce")
    point_reconciliation["new_error"] = pd.to_numeric(point_reconciliation["new_error"], errors="coerce")
    point_reconciliation["old_pred_ppm"] = pd.to_numeric(point_reconciliation["old_pred_ppm"], errors="coerce")
    point_reconciliation["new_pred_ppm"] = pd.to_numeric(point_reconciliation["new_pred_ppm"], errors="coerce")
    point_reconciliation["target_ppm"] = pd.to_numeric(point_reconciliation["target_ppm"], errors="coerce")
    point_reconciliation["temp_c"] = pd.to_numeric(point_reconciliation["temp_c"], errors="coerce")
    point_reconciliation["point_row"] = pd.to_numeric(point_reconciliation["point_row"], errors="coerce")
    point_reconciliation["positive_gap_sq"] = (
        np.square(point_reconciliation["new_error"]) - np.square(point_reconciliation["old_error"])
    ).clip(lower=0.0)

    by_analyzer_gap = (
        point_reconciliation.groupby("analyzer_id", dropna=False)["positive_gap_sq"].sum(min_count=1).sort_values(ascending=False)
    )
    current_remaining_gap_total = remaining_gap_total_from_frame(point_reconciliation, error_column="new_error")
    laggard_primary = str(laggard_analyzer_id or "")
    if laggard_primary == "" or laggard_primary not in set(point_reconciliation["analyzer_id"].astype(str)):
        laggard_primary = str(by_analyzer_gap.index[0]) if not by_analyzer_gap.empty else ""
    if laggard_primary == "":
        empty = pd.DataFrame()
        return {"detail": empty, "summary": empty, "conclusions": empty}

    fixed_row_df = selection[selection["analyzer_id"].astype(str) == laggard_primary].copy()
    if fixed_row_df.empty:
        empty = pd.DataFrame()
        return {"detail": empty, "summary": empty, "conclusions": empty}
    fixed_row = fixed_row_df.iloc[0]
    selected_source_pair = str(fixed_row.get("selected_source_pair") or "raw/raw")
    fixed_model_family = str(fixed_row.get("best_model_family") or "")
    fixed_zero_residual_mode = str(fixed_row.get("zero_residual_mode") or "none")
    fixed_prediction_scope = str(fixed_row.get("selected_prediction_scope") or "validation_oof")
    mode2_semantic_profile = str(
        point_reconciliation.loc[point_reconciliation["analyzer_id"].astype(str) == laggard_primary, "mode2_semantic_profile"]
        .dropna()
        .astype(str)
        .iloc[0]
        if not point_reconciliation.loc[
            point_reconciliation["analyzer_id"].astype(str) == laggard_primary, "mode2_semantic_profile"
        ].dropna().empty
        else "mode2_semantics_unknown"
    )

    base_frame = _prepare_base_frame(
        point_reconciliation,
        filtered_samples,
        analyzer_id=laggard_primary,
        selected_source_pair=selected_source_pair,
        fixed_model_family=fixed_model_family,
        fixed_zero_residual_mode=fixed_zero_residual_mode,
        fixed_prediction_scope=fixed_prediction_scope,
    )
    if base_frame.empty:
        empty = pd.DataFrame()
        return {"detail": empty, "summary": empty, "conclusions": empty}

    segment_target = pd.to_numeric(base_frame["target_ppm"], errors="coerce")
    base_frame["segment_tag"] = np.select(
        [
            segment_target == 0.0,
            (segment_target > 0.0) & (segment_target <= 200.0),
            (segment_target > 200.0) & (segment_target <= 1000.0),
        ],
        ["zero", "low", "main"],
        default="other",
    )
    keep_keys = ["analyzer_id", "point_title", "point_row", "target_ppm"]
    use_leave_one_out = fixed_prediction_scope == "validation_oof"

    mode_frames: dict[str, pd.DataFrame] = {}
    mode_notes: dict[str, dict[str, str]] = {}

    current_frame = _mode_frame(base_frame, sidecar_mode="current_global_fixed", pred_column="new_pred_ppm", error_column="new_error")
    mode_frames["current_global_fixed"] = current_frame
    mode_notes["current_global_fixed"] = {
        "chosen_model_id_under_sidecar": str(fixed_row.get("best_absorbance_model") or ""),
        "sidecar_fit_note": "uses current global deployable prediction without modification",
    }

    refit_predictions, chosen_model_id = _same_family_refit_frame(
        absorbance_point_variants,
        analyzer_id=laggard_primary,
        selected_source_pair=selected_source_pair,
        fixed_model_family=fixed_model_family,
        fixed_zero_residual_mode=fixed_zero_residual_mode,
        fixed_prediction_scope=fixed_prediction_scope,
        config=config,
    )
    refit_merged = base_frame.merge(refit_predictions, on=keep_keys, how="left")
    refit_frame = _mode_frame(
        refit_merged.assign(
            sidecar_pred_ppm=pd.to_numeric(refit_merged.get("sidecar_pred_ppm"), errors="coerce").combine_first(
                pd.to_numeric(refit_merged["new_pred_ppm"], errors="coerce")
            ),
            sidecar_error_ppm=pd.to_numeric(refit_merged.get("sidecar_error_ppm"), errors="coerce").combine_first(
                pd.to_numeric(refit_merged["new_error"], errors="coerce")
            ),
        ),
        sidecar_mode="ga01_same_family_refit",
        pred_column="sidecar_pred_ppm",
        error_column="sidecar_error_ppm",
    )
    mode_frames["ga01_same_family_refit"] = refit_frame
    mode_notes["ga01_same_family_refit"] = {
        "chosen_model_id_under_sidecar": chosen_model_id,
        "sidecar_fit_note": "same selected source pair and fixed family refit",
    }

    correction_specs = {
        "ga01_humidity_cross_residual": ["humidity_ratio_selected_centered", "A_times_humidity", "h2o_density_centered"],
        "ga01_temp_piecewise_residual": ["temp_centered", "temp_hot_hinge", "temp_cold_hinge", "A_times_temp"],
        "ga01_humidity_plus_temp_residual": [
            "humidity_ratio_selected_centered",
            "A_times_humidity",
            "h2o_density_centered",
            "temp_centered",
            "temp_hot_hinge",
            "temp_cold_hinge",
            "A_times_temp",
        ],
    }
    for mode_name, feature_columns in correction_specs.items():
        correction, fit_note = _fit_linear_residual_correction(
            base_frame,
            feature_columns=feature_columns,
            target_column="new_error",
            use_leave_one_out=use_leave_one_out,
        )
        corrected = base_frame.copy()
        corrected["residual_correction"] = pd.to_numeric(correction, errors="coerce")
        corrected["corrected_error_ppm"] = pd.to_numeric(corrected["new_error"], errors="coerce") - corrected["residual_correction"]
        corrected["corrected_pred_ppm"] = pd.to_numeric(corrected["target_ppm"], errors="coerce") + corrected["corrected_error_ppm"]
        mode_frames[mode_name] = _mode_frame(
            corrected,
            sidecar_mode=mode_name,
            pred_column="corrected_pred_ppm",
            error_column="corrected_error_ppm",
        )
        mode_notes[mode_name] = {
            "chosen_model_id_under_sidecar": str(fixed_row.get("best_absorbance_model") or ""),
            "sidecar_fit_note": fit_note,
        }

    detail_rows: list[dict[str, Any]] = []
    global_reconciliations: dict[str, pd.DataFrame] = {}
    for mode_name, frame in mode_frames.items():
        global_frame = point_reconciliation.copy()
        replacement = frame[["reconciliation_row_id", "sidecar_pred_ppm", "sidecar_error_ppm"]].copy()
        replacement["reconciliation_row_id"] = pd.to_numeric(replacement["reconciliation_row_id"], errors="coerce").astype("Int64")
        replacement = replacement.dropna(subset=["reconciliation_row_id"]).drop_duplicates(subset=["reconciliation_row_id"], keep="last")
        error_map = pd.Series(
            pd.to_numeric(replacement["sidecar_error_ppm"], errors="coerce").to_numpy(),
            index=replacement["reconciliation_row_id"].astype(int).to_numpy(),
        )
        pred_map = pd.Series(
            pd.to_numeric(replacement["sidecar_pred_ppm"], errors="coerce").to_numpy(),
            index=replacement["reconciliation_row_id"].astype(int).to_numpy(),
        )
        sidecar_error_series = global_frame.index.to_series().map(error_map)
        sidecar_pred_series = global_frame.index.to_series().map(pred_map)
        global_frame["sidecar_error_used"] = sidecar_error_series.combine_first(
            pd.to_numeric(global_frame["new_error"], errors="coerce")
        )
        global_frame["sidecar_pred_used"] = sidecar_pred_series.combine_first(
            pd.to_numeric(global_frame["new_pred_ppm"], errors="coerce")
        )
        global_reconciliations[mode_name] = global_frame

        detail_row = _detail_row(
            frame,
            analyzer_id=laggard_primary,
            sidecar_mode=mode_name,
            selected_source_pair=selected_source_pair,
            fixed_model_family=fixed_model_family,
            fixed_zero_residual_mode=fixed_zero_residual_mode,
            fixed_prediction_scope=fixed_prediction_scope,
            current_frame=current_frame,
        )
        detail_row.update(
            {
                "mode2_semantic_profile": mode2_semantic_profile,
                "laggard_analyzer_primary": laggard_primary,
                "current_remaining_gap_total": current_remaining_gap_total,
                "global_remaining_gap_if_applied_sidecar": remaining_gap_total_from_frame(global_frame, error_column="sidecar_error_used"),
                "score_metric_used": _score_metric_name(config),
                "chosen_model_id_under_sidecar": mode_notes[mode_name]["chosen_model_id_under_sidecar"],
                "sidecar_fit_note": mode_notes[mode_name]["sidecar_fit_note"],
            }
        )
        detail_row["global_remaining_gap_delta_vs_current"] = (
            detail_row["current_remaining_gap_total"] - detail_row["global_remaining_gap_if_applied_sidecar"]
            if pd.notna(detail_row["current_remaining_gap_total"]) and pd.notna(detail_row["global_remaining_gap_if_applied_sidecar"])
            else math.nan
        )
        detail_rows.append(detail_row)

    detail_df = pd.DataFrame(detail_rows).sort_values(["sidecar_mode"], ignore_index=True)
    if detail_df.empty:
        empty = pd.DataFrame()
        return {"detail": empty, "summary": empty, "conclusions": empty}

    candidate_rows = detail_df[detail_df["sidecar_mode"] != "current_global_fixed"].copy()
    if candidate_rows.empty:
        best_sidecar_mode = "current_global_fixed"
    else:
        candidate_rows = candidate_rows.sort_values(
            ["global_remaining_gap_delta_vs_current", "delta_vs_current_ga01_overall", "sidecar_mode"],
            ascending=[False, False, True],
            ignore_index=True,
        )
        best_sidecar_mode = str(candidate_rows.iloc[0]["sidecar_mode"])

    detail_df["best_sidecar_mode"] = best_sidecar_mode
    detail_df["best_sidecar_flag"] = detail_df["sidecar_mode"].astype(str) == best_sidecar_mode
    detail_df["future_candidate_flag"] = (
        detail_df["sidecar_mode"].astype(str).ne("current_global_fixed")
        & (pd.to_numeric(detail_df["global_remaining_gap_delta_vs_current"], errors="coerce") > 1.0e-12)
    )

    summary_df = detail_df[
        [
            "laggard_analyzer_primary",
            "analyzer_id",
            "mode2_semantic_profile",
            "sidecar_mode",
            "selected_source_pair",
            "fixed_model_family",
            "fixed_zero_residual_mode",
            "fixed_prediction_scope",
            "zero_rmse",
            "low_range_rmse",
            "main_range_rmse",
            "overall_rmse",
            "temp_bias_spread",
            "gap_to_old_overall",
            "gap_to_old_zero",
            "delta_vs_current_ga01_overall",
            "delta_vs_current_ga01_zero",
            "delta_vs_current_ga01_low",
            "delta_vs_current_ga01_main",
            "pointwise_win_count_vs_old",
            "pointwise_win_count_vs_current_ga01",
            "local_win_examples",
            "local_loss_examples",
            "current_remaining_gap_total",
            "global_remaining_gap_if_applied_sidecar",
            "global_remaining_gap_delta_vs_current",
            "score_metric_used",
            "chosen_model_id_under_sidecar",
            "sidecar_fit_note",
            "best_sidecar_mode",
            "best_sidecar_flag",
            "future_candidate_flag",
        ]
    ].copy()
    summary_df.insert(0, "summary_scope", "ga01_sidecar_mode")

    improvement_lookup = {
        row["sidecar_mode"]: float(pd.to_numeric(pd.Series([row["delta_vs_current_ga01_overall"]]), errors="coerce").iloc[0] or 0.0)
        for _, row in detail_df.iterrows()
        if row["sidecar_mode"] != "current_global_fixed"
    }
    best_row = detail_df[detail_df["sidecar_mode"].astype(str) == best_sidecar_mode].iloc[0]
    ga01_gap_share = (
        float(by_analyzer_gap.get(laggard_primary, 0.0)) / float(current_remaining_gap_total)
        if pd.notna(current_remaining_gap_total) and current_remaining_gap_total > 0.0
        else math.nan
    )
    if not improvement_lookup or max(improvement_lookup.values()) <= 1.0e-12:
        weakness_type = "no_effective_model_sidecar_gain"
    else:
        best_improvement_mode = max(improvement_lookup, key=improvement_lookup.get)
        weakness_type = {
            "ga01_same_family_refit": "ppm_family_weakness",
            "ga01_humidity_cross_residual": "humidity_residual_weakness",
            "ga01_temp_piecewise_residual": "temp_residual_weakness",
            "ga01_humidity_plus_temp_residual": "mixed_humidity_plus_temp_residual",
        }.get(best_improvement_mode, "mixed_or_unclear")

    conclusions_df = pd.DataFrame(
        [
            {
                "question_id": "ga01_analyzer_specific_weakness",
                "question": "Does GA01 behave like an analyzer-specific weakness?",
                "answer": "yes_ga01_primary_laggard" if laggard_primary == "GA01" else "laggard_is_not_ga01",
                "evidence": f"laggard_analyzer_primary={laggard_primary}; ga01_remaining_gap_share={ga01_gap_share:.4f}" if pd.notna(ga01_gap_share) else f"laggard_analyzer_primary={laggard_primary}",
                "laggard_analyzer_primary": laggard_primary,
                "best_sidecar_mode": best_sidecar_mode,
            },
            {
                "question_id": "ga01_weakness_type",
                "question": "Does GA01 look more like ppm family, humidity residual, or temperature residual weakness?",
                "answer": weakness_type,
                "evidence": (
                    " | ".join(
                        f"{mode}={float(value):.6g}" for mode, value in sorted(improvement_lookup.items(), key=lambda item: item[0])
                    )
                    or "no candidate sidecar improvements available"
                ),
                "laggard_analyzer_primary": laggard_primary,
                "best_sidecar_mode": best_sidecar_mode,
            },
            {
                "question_id": "ga01_sidecar_global_effect",
                "question": "Can a GA01-only sidecar shrink remaining gap without touching GA02/GA03?",
                "answer": (
                    "yes_sidecar_reduces_remaining_gap_without_touching_other_analyzers"
                    if float(pd.to_numeric(pd.Series([best_row["global_remaining_gap_delta_vs_current"]]), errors="coerce").iloc[0] or 0.0) > 1.0e-12
                    else "no_material_global_gap_reduction"
                ),
                "evidence": (
                    f"best_sidecar_mode={best_sidecar_mode}; global_remaining_gap_delta_vs_current={float(pd.to_numeric(pd.Series([best_row['global_remaining_gap_delta_vs_current']]), errors='coerce').iloc[0] or 0.0):.6g}; "
                    "GA02/GA03 predictions remain unchanged because only laggard analyzer rows are replaced diagnostically"
                ),
                "laggard_analyzer_primary": laggard_primary,
                "best_sidecar_mode": best_sidecar_mode,
            },
            {
                "question_id": "future_candidate_status",
                "question": "Is the best GA01 sidecar worth retaining as a future analyzer-specific candidate?",
                "answer": (
                    "retain_as_future_analyzer_specific_candidate"
                    if best_sidecar_mode != "current_global_fixed"
                    and float(pd.to_numeric(pd.Series([best_row["delta_vs_current_ga01_overall"]]), errors="coerce").iloc[0] or 0.0) > 1.0e-12
                    else "not_enough_sidecar_gain"
                ),
                "evidence": (
                    f"best_sidecar_mode={best_sidecar_mode}; delta_vs_current_ga01_overall={float(pd.to_numeric(pd.Series([best_row['delta_vs_current_ga01_overall']]), errors='coerce').iloc[0] or 0.0):.6g}; "
                    f"future_candidate_flag={bool(best_row['future_candidate_flag'])}"
                ),
                "laggard_analyzer_primary": laggard_primary,
                "best_sidecar_mode": best_sidecar_mode,
            },
            {
                "question_id": "ga01_data_quality_review",
                "question": "Should GA01 fall back to data-quality or coverage review instead of more model tuning?",
                "answer": (
                    "yes_review_data_quality_or_coverage"
                    if best_sidecar_mode == "current_global_fixed"
                    or float(pd.to_numeric(pd.Series([best_row["delta_vs_current_ga01_overall"]]), errors="coerce").iloc[0] or 0.0) <= 1.0e-12
                    else "not_primary_next_step"
                ),
                "evidence": (
                    f"best_sidecar_mode={best_sidecar_mode}; delta_vs_current_ga01_overall={float(pd.to_numeric(pd.Series([best_row['delta_vs_current_ga01_overall']]), errors='coerce').iloc[0] or 0.0):.6g}; "
                    f"global_remaining_gap_delta_vs_current={float(pd.to_numeric(pd.Series([best_row['global_remaining_gap_delta_vs_current']]), errors='coerce').iloc[0] or 0.0):.6g}"
                ),
                "laggard_analyzer_primary": laggard_primary,
                "best_sidecar_mode": best_sidecar_mode,
            },
        ]
    )

    return {
        "detail": detail_df,
        "summary": summary_df,
        "conclusions": conclusions_df,
    }
