"""Batch-level candidate tournament diagnostics."""

from __future__ import annotations

import math
from typing import Any

import numpy as np
import pandas as pd

from .absorbance_models import _fit_one_candidate, active_model_specs
from .ppm_family_challenge import (
    _choose_humidity_delta,
    _choose_humidity_proxy,
    _family_specs_for_analyzer,
)


SOURCE_MODES: tuple[str, ...] = (
    "raw_only_strict",
    "filt_only_strict",
    "raw_first_with_fallback",
    "filt_first_with_fallback",
)

MODEL_FAMILIES: tuple[str, ...] = (
    "current_ratio_poly_rt_p",
    "v5_abs_k_minimal_proxy",
    "legacy_humidity_cross_D",
    "legacy_humidity_cross_E",
)

RESIDUAL_HEADS: tuple[str, ...] = (
    "none",
    "global_temp_residual",
    "global_humidity_residual",
    "global_humidity_plus_temp_residual",
    "analyzer_specific_humidity_plus_temp_residual",
)

SCOPE_A_NAME = "historical_ga02_ga03_candidate_tournament"
SCOPE_B_NAME = "run_20260410_132440_all_analyzers_candidate_tournament"
SCOPE_A_DESCRIPTION = "historical packages excluding run_20260410_132440; only GA02 and GA03 are included"
SCOPE_B_DESCRIPTION = "run_20260410_132440 only; include every analyzer that appears in that run"
PRIMARY_SURFACE = "run_native_old_vs_new"
SECONDARY_SURFACE = "debugger_reconstructed_old_vs_new"

DETAIL_COLUMNS: tuple[str, ...] = (
    "run_id",
    "comparison_scope",
    "comparison_scope_label",
    "comparison_scope_description",
    "comparison_surface",
    "analyzer_id",
    "candidate_id",
    "source_mode",
    "model_family",
    "residual_head",
    "diagnostic_only_flag",
    "diagnostic_only_reason",
    "ratio_source_mode",
    "mode2_semantic_profile",
    "selected_prediction_scope",
    "selected_prediction_scope_majority",
    "overall_rmse_old",
    "overall_rmse_new",
    "zero_rmse_old",
    "zero_rmse_new",
    "low_rmse_old",
    "low_rmse_new",
    "main_rmse_old",
    "main_rmse_new",
    "pointwise_win_count_vs_old",
    "pointwise_loss_count_vs_old",
    "improvement_pct_overall",
    "improvement_pct_zero",
    "improvement_pct_low",
    "improvement_pct_main",
    "promotion_score",
    "promotion_rank",
    "raw_filt_divergence_mean",
    "raw_filt_divergence_max",
    "residual_vs_temp_corr",
    "residual_vs_humidity_corr",
    "residual_vs_signal_corr",
    "whether_gain_comes_from_main_segment",
    "whether_gain_is_analyzer_concentrated",
    "likely_root_cause_bucket",
    "point_count",
)

SUMMARY_COLUMNS: tuple[str, ...] = (
    "summary_scope",
    "comparison_scope",
    "comparison_scope_label",
    "comparison_scope_description",
    "comparison_surface",
    "candidate_id",
    "source_mode",
    "model_family",
    "residual_head",
    "diagnostic_only_flag",
    "diagnostic_only_reason",
    "selected_prediction_scope_majority",
    "overall_rmse_old",
    "overall_rmse_new",
    "zero_rmse_old",
    "zero_rmse_new",
    "low_rmse_old",
    "low_rmse_new",
    "main_rmse_old",
    "main_rmse_new",
    "improvement_pct_overall",
    "improvement_pct_zero",
    "improvement_pct_low",
    "improvement_pct_main",
    "pointwise_win_count_vs_old",
    "pointwise_loss_count_vs_old",
    "promotion_score",
    "promotion_rank",
    "coverage_ratio",
    "expected_analyzer_count",
    "observed_analyzer_count",
    "run_count",
    "raw_filt_divergence_mean",
    "raw_filt_divergence_max",
    "residual_vs_temp_corr",
    "residual_vs_humidity_corr",
    "residual_vs_signal_corr",
    "whether_gain_comes_from_main_segment",
    "whether_gain_is_analyzer_concentrated",
    "likely_root_cause_bucket",
    "future_deployable_candidate_flag",
)


def _empty_frame(columns: tuple[str, ...]) -> pd.DataFrame:
    return pd.DataFrame(columns=list(columns))


def _first_text(values: pd.Series, default: str = "") -> str:
    clean = values.dropna().astype(str).str.strip()
    clean = clean[clean != ""]
    if clean.empty:
        return default
    return str(clean.iloc[0])


def _majority_text(values: pd.Series, default: str = "") -> str:
    clean = values.dropna().astype(str).str.strip()
    clean = clean[clean != ""]
    if clean.empty:
        return default
    counts = clean.value_counts()
    if len(counts) > 1 and int(counts.iloc[0]) == int(counts.iloc[1]):
        return "mixed"
    return str(counts.index[0])


def _metrics(errors: pd.Series | np.ndarray) -> dict[str, float]:
    clean = pd.to_numeric(pd.Series(errors), errors="coerce").dropna()
    if clean.empty:
        return {"rmse": math.nan}
    values = clean.to_numpy(dtype=float)
    return {"rmse": float(np.sqrt(np.mean(np.square(values))))}


def _improvement_pct(old_value: float, new_value: float) -> float:
    if pd.isna(old_value) or pd.isna(new_value) or abs(float(old_value)) <= 1.0e-12:
        return math.nan
    return float((float(old_value) - float(new_value)) / float(old_value) * 100.0)


def _segment_mask(frame: pd.DataFrame, segment_tag: str) -> pd.Series:
    target = pd.to_numeric(frame.get("target_ppm"), errors="coerce")
    if segment_tag == "overall":
        return pd.Series(True, index=frame.index)
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


def _candidate_id(source_mode: str, model_family: str, residual_head: str) -> str:
    return f"{source_mode}|{model_family}|{residual_head}"


def _scope_name_for_run(run_id: str, scope_b_run_id: str) -> str:
    return SCOPE_B_NAME if str(run_id) == str(scope_b_run_id) else SCOPE_A_NAME


def _scope_meta(scope_name: str) -> tuple[str, str]:
    if scope_name == SCOPE_B_NAME:
        return "Scope B: run_20260410_132440 all analyzers", SCOPE_B_DESCRIPTION
    return "Scope A: historical GA02/GA03", SCOPE_A_DESCRIPTION


def _expected_analyzers_by_scope(
    run_results: list[dict[str, Any]],
    scope_b_run_id: str,
) -> dict[str, set[str]]:
    expected: dict[str, set[str]] = {SCOPE_A_NAME: set(), SCOPE_B_NAME: set()}
    for result in run_results:
        run_id = str(result.get("run_name", "") or "")
        compare = result.get("point_reconciliation", pd.DataFrame())
        compare = compare.copy() if isinstance(compare, pd.DataFrame) else pd.DataFrame()
        if not run_id or compare.empty:
            continue
        analyzers = {
            str(value)
            for value in compare.get("analyzer_id", pd.Series(dtype=object)).dropna().astype(str).tolist()
            if str(value)
        }
        if run_id == scope_b_run_id:
            expected[SCOPE_B_NAME].update(analyzers)
        else:
            expected[SCOPE_A_NAME].update(analyzers.intersection({"GA02", "GA03"}))
    return expected


def _point_feature_summary(filtered: pd.DataFrame) -> pd.DataFrame:
    if filtered.empty:
        return pd.DataFrame()
    aggregation_map = {
        "temp_c": "temp_set_c",
        "target_ppm": "target_co2_ppm",
        "ratio_co2_raw_mean": "ratio_co2_raw",
        "ratio_co2_filt_mean": "ratio_co2_filt",
        "h2o_ratio_raw_mean": "ratio_h2o_raw",
        "h2o_ratio_filt_mean": "ratio_h2o_filt",
        "h2o_density_mean": "h2o_density",
        "co2_signal_mean": "co2_signal",
        "temp_cavity_c_mean": "temp_cavity_c",
        "temp_shell_c_mean": "temp_shell_c",
    }
    available_aggregations = {
        output_name: (input_name, "mean")
        for output_name, input_name in aggregation_map.items()
        if input_name in filtered.columns
    }
    if not available_aggregations:
        return pd.DataFrame()
    grouped = (
        filtered.groupby(["analyzer", "point_title", "point_row"], dropna=False)
        .agg(**available_aggregations)
        .reset_index()
        .rename(columns={"analyzer": "analyzer_id"})
    )
    for output_name in aggregation_map:
        if output_name not in grouped.columns:
            grouped[output_name] = math.nan
    grouped["raw_filt_divergence"] = (
        pd.to_numeric(grouped["ratio_co2_raw_mean"], errors="coerce")
        - pd.to_numeric(grouped["ratio_co2_filt_mean"], errors="coerce")
    ).abs()
    grouped["temp_feature_c"] = pd.to_numeric(grouped["temp_cavity_c_mean"], errors="coerce").combine_first(
        pd.to_numeric(grouped["temp_shell_c_mean"], errors="coerce")
    ).combine_first(pd.to_numeric(grouped["temp_c"], errors="coerce"))
    grouped["humidity_feature"] = pd.to_numeric(grouped["h2o_ratio_filt_mean"], errors="coerce").combine_first(
        pd.to_numeric(grouped["h2o_ratio_raw_mean"], errors="coerce")
    )
    return grouped


def _normalize_surface_frame(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame.copy()
    compare = frame.copy()
    compare["target_ppm"] = pd.to_numeric(compare.get("target_ppm"), errors="coerce")
    compare["temp_c"] = pd.to_numeric(compare.get("temp_c"), errors="coerce")
    compare["old_pred_ppm"] = pd.to_numeric(compare.get("old_pred_ppm"), errors="coerce")
    compare["new_pred_ppm"] = pd.to_numeric(compare.get("new_pred_ppm"), errors="coerce")
    compare["old_error"] = compare["old_pred_ppm"] - compare["target_ppm"]
    compare["new_error"] = compare["new_pred_ppm"] - compare["target_ppm"]
    old_abs = compare["old_error"].abs()
    new_abs = compare["new_error"].abs()
    compare["winner_for_point"] = np.select(
        [old_abs.lt(new_abs), new_abs.lt(old_abs)],
        ["old_chain", "new_chain"],
        default="tie",
    )
    compare.loc[compare["old_error"].isna() & compare["new_error"].notna(), "winner_for_point"] = "new_chain"
    compare.loc[compare["old_error"].notna() & compare["new_error"].isna(), "winner_for_point"] = "old_chain"
    return compare


def _native_surface_frame(
    run_id: str,
    point_reconciliation: pd.DataFrame,
    filtered: pd.DataFrame,
    candidate_frame: pd.DataFrame,
) -> pd.DataFrame:
    if point_reconciliation.empty or candidate_frame.empty:
        return pd.DataFrame()
    native = (
        filtered.groupby(["analyzer", "point_title", "point_row"], dropna=False)
        .agg(run_native_new_pred_ppm=("co2_ppm", "mean"))
        .reset_index()
        .rename(columns={"analyzer": "analyzer_id"})
    )
    keep_cols = [
        "analyzer_id",
        "point_title",
        "point_row",
        "target_ppm",
        "temp_c",
        "old_pred_ppm",
        "selected_source_pair",
        "selected_prediction_scope",
        "mode2_semantic_profile",
        "raw_filt_divergence",
        "humidity_feature",
        "co2_signal_mean",
        "temp_feature_c",
    ]
    available_cols = [column for column in keep_cols if column in candidate_frame.columns]
    native_frame = point_reconciliation[
        ["analyzer_id", "point_title", "point_row", "target_ppm", "temp_c", "old_pred_ppm"]
    ].copy()
    native_frame = native_frame.merge(
        candidate_frame[available_cols],
        on=["analyzer_id", "point_title", "point_row"],
        how="inner",
        suffixes=("", "_candidate"),
    )
    native_frame = native_frame.merge(
        native[["analyzer_id", "point_title", "point_row", "run_native_new_pred_ppm"]],
        on=["analyzer_id", "point_title", "point_row"],
        how="left",
    )
    native_frame["run_id"] = run_id
    native_frame["comparison_surface"] = PRIMARY_SURFACE
    native_frame["old_value_source"] = "old_residual_csv_prediction_simplified"
    native_frame["new_value_source"] = "analyzer_sheet_mean_co2_ppm"
    native_frame["selected_prediction_scope"] = "run_native_point_mean"
    native_frame["new_pred_ppm"] = pd.to_numeric(native_frame["run_native_new_pred_ppm"], errors="coerce")
    return _normalize_surface_frame(native_frame)


def _prediction_table_from_residuals(
    residual_df: pd.DataFrame,
    requested_scope: str,
) -> pd.DataFrame:
    if residual_df.empty:
        return pd.DataFrame()
    overall = residual_df[residual_df["prediction_scope"].astype(str) == "overall_fit"][
        ["point_title", "point_row", "predicted_ppm", "error_ppm"]
    ].rename(columns={"predicted_ppm": "overall_pred_ppm", "error_ppm": "overall_error_ppm"})
    validation = residual_df[residual_df["prediction_scope"].astype(str) == "validation_oof"][
        ["point_title", "point_row", "predicted_ppm", "error_ppm"]
    ].rename(columns={"predicted_ppm": "validation_pred_ppm", "error_ppm": "validation_error_ppm"})
    merged = overall.merge(validation, on=["point_title", "point_row"], how="outer")
    if str(requested_scope) == "validation_oof":
        merged["selected_pred_ppm"] = merged["validation_pred_ppm"].combine_first(merged["overall_pred_ppm"])
        merged["selected_error_ppm"] = merged["validation_error_ppm"].combine_first(merged["overall_error_ppm"])
        merged["selected_prediction_scope"] = np.where(
            merged["validation_pred_ppm"].notna(),
            "validation_oof",
            "overall_fit_fallback",
        )
    else:
        merged["selected_pred_ppm"] = merged["overall_pred_ppm"]
        merged["selected_error_ppm"] = merged["overall_error_ppm"]
        merged["selected_prediction_scope"] = "overall_fit"
    return merged


def _score_sort_columns() -> list[str]:
    return ["composite_score", "validation_rmse", "overall_rmse", "model_id"]


def _best_source_rows(scores: pd.DataFrame) -> dict[tuple[str, str], pd.Series]:
    lookup: dict[tuple[str, str], pd.Series] = {}
    if scores.empty:
        return lookup
    for (analyzer_id, source_pair), subset in scores.groupby(["analyzer_id", "selected_source_pair"], dropna=False):
        ordered = subset.sort_values(_score_sort_columns(), ignore_index=True)
        if ordered.empty:
            continue
        lookup[(str(analyzer_id), str(source_pair))] = ordered.iloc[0]
    return lookup


def _pick_source_pair(source_mode: str, available_pairs: set[str]) -> str:
    if source_mode == "raw_only_strict":
        return "raw/raw" if "raw/raw" in available_pairs else ""
    if source_mode == "filt_only_strict":
        return "filt/filt" if "filt/filt" in available_pairs else ""
    if source_mode == "raw_first_with_fallback":
        if "raw/raw" in available_pairs:
            return "raw/raw"
        if "filt/filt" in available_pairs:
            return "filt/filt"
        return ""
    if source_mode == "filt_first_with_fallback":
        if "filt/filt" in available_pairs:
            return "filt/filt"
        if "raw/raw" in available_pairs:
            return "raw/raw"
        return ""
    raise ValueError(f"Unsupported source_mode: {source_mode}")


def _ratio_source_from_pair(source_pair: str) -> str:
    return "ratio_co2_raw" if str(source_pair) == "raw/raw" else "ratio_co2_filt"


def _candidate_bucket(source_mode: str, model_family: str, residual_head: str) -> str:
    if residual_head == "analyzer_specific_humidity_plus_temp_residual":
        return "analyzer_specific"
    if residual_head == "global_temp_residual":
        return "temp_residual"
    if residual_head in {"global_humidity_residual", "global_humidity_plus_temp_residual"}:
        return "mixed" if model_family != "current_ratio_poly_rt_p" else "humidity_feature"
    if model_family in {"v5_abs_k_minimal_proxy", "legacy_humidity_cross_D", "legacy_humidity_cross_E"}:
        return "humidity_feature"
    if source_mode != "raw_first_with_fallback":
        return "source_policy"
    return "mixed"


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
            [
                np.ones(len(train_df), dtype=float),
                *[
                    pd.to_numeric(train_df[column], errors="coerce").to_numpy(dtype=float)
                    for column in feature_columns
                ],
            ]
        )
        y_train = pd.to_numeric(train_df[target_column], errors="coerce").to_numpy(dtype=float)
        coeffs, _, _, _ = np.linalg.lstsq(x_train, y_train, rcond=None)
        x_predict = np.column_stack(
            [
                np.ones(len(predict_df), dtype=float),
                *[
                    pd.to_numeric(predict_df[column], errors="coerce").to_numpy(dtype=float)
                    for column in feature_columns
                ],
            ]
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


def _safe_corr(x: pd.Series, y: pd.Series) -> float:
    work = pd.DataFrame({"x": pd.to_numeric(x, errors="coerce"), "y": pd.to_numeric(y, errors="coerce")}).dropna()
    if len(work) < 2:
        return math.nan
    x_values = work["x"].to_numpy(dtype=float)
    y_values = work["y"].to_numpy(dtype=float)
    if float(np.std(x_values)) <= 1.0e-12 or float(np.std(y_values)) <= 1.0e-12:
        return math.nan
    return float(np.corrcoef(x_values, y_values)[0, 1])


def _gain_flags(frame: pd.DataFrame) -> tuple[bool, bool, float]:
    work = frame.copy()
    work["gain_sq_vs_old"] = (
        np.square(pd.to_numeric(work["old_error"], errors="coerce"))
        - np.square(pd.to_numeric(work["new_error"], errors="coerce"))
    ).clip(lower=0.0)
    total_gain = float(pd.to_numeric(work["gain_sq_vs_old"], errors="coerce").sum())
    if total_gain <= 0.0:
        return False, False, 0.0
    main_gain = float(
        pd.to_numeric(work.loc[_segment_mask(work, "main"), "gain_sq_vs_old"], errors="coerce").sum()
    )
    by_analyzer = (
        work.groupby("analyzer_id", dropna=False)["gain_sq_vs_old"]
        .sum(min_count=1)
        .sort_values(ascending=False)
    )
    top_share = float(by_analyzer.iloc[0] / total_gain) if not by_analyzer.empty else 0.0
    return main_gain > 0.5 * total_gain, top_share >= 0.65, top_share


def _candidate_scope_frame(
    point_frame: pd.DataFrame,
    *,
    run_id: str,
    comparison_scope: str,
    comparison_surface: str,
    candidate_id: str,
    source_mode: str,
    model_family: str,
    residual_head: str,
    diagnostic_only_flag: bool,
    diagnostic_only_reason: str,
) -> pd.DataFrame:
    if point_frame.empty:
        return point_frame.copy()
    label, description = _scope_meta(comparison_scope)
    scoped = point_frame.copy()
    scoped["run_id"] = run_id
    scoped["comparison_scope"] = comparison_scope
    scoped["comparison_scope_label"] = label
    scoped["comparison_scope_description"] = description
    scoped["comparison_surface"] = comparison_surface
    scoped["candidate_id"] = candidate_id
    scoped["source_mode"] = source_mode
    scoped["model_family"] = model_family
    scoped["residual_head"] = residual_head
    scoped["diagnostic_only_flag"] = diagnostic_only_flag
    scoped["diagnostic_only_reason"] = diagnostic_only_reason
    scoped["ratio_source_mode"] = source_mode
    return scoped


def _variant_frame(
    result: dict[str, Any],
    analyzer_id: str,
    source_pair: str,
    zero_residual_mode: str,
    water_zero_anchor_mode: str,
) -> pd.DataFrame:
    variants = result.get("water_point_variants", pd.DataFrame())
    variants = variants.copy() if isinstance(variants, pd.DataFrame) else pd.DataFrame()
    if variants.empty:
        variants = result.get("water_zero_anchor_features", pd.DataFrame())
        variants = variants.copy() if isinstance(variants, pd.DataFrame) else pd.DataFrame()
    if variants.empty:
        variants = result.get("absorbance_point_variants", pd.DataFrame())
        variants = variants.copy() if isinstance(variants, pd.DataFrame) else pd.DataFrame()
    if variants.empty:
        return pd.DataFrame()
    ratio_source = _ratio_source_from_pair(source_pair)
    work = variants.copy()
    analyzer_column = "analyzer_id" if "analyzer_id" in work.columns else "analyzer"
    work = work[work[analyzer_column].astype(str) == analyzer_id].copy()
    work = work[work["ratio_source"].astype(str) == ratio_source].copy()
    if "zero_residual_mode" in work.columns:
        work = work[work["zero_residual_mode"].astype(str) == str(zero_residual_mode)].copy()
    if "water_zero_anchor_mode" in work.columns and str(water_zero_anchor_mode):
        work = work[work["water_zero_anchor_mode"].astype(str) == str(water_zero_anchor_mode)].copy()
    if work.empty:
        return work
    work["analyzer_id"] = work[analyzer_column].astype(str)
    compare = result.get("point_reconciliation", pd.DataFrame())
    compare = compare.copy() if isinstance(compare, pd.DataFrame) else pd.DataFrame()
    if not compare.empty:
        keys = compare[compare["analyzer_id"].astype(str) == analyzer_id][["point_title", "point_row"]].drop_duplicates()
        work = work.merge(keys, on=["point_title", "point_row"], how="inner")
    work["target_ppm"] = pd.to_numeric(work.get("target_co2_ppm"), errors="coerce")
    work["temp_c"] = pd.to_numeric(work.get("temp_set_c"), errors="coerce")
    work["temp_model_c"] = pd.to_numeric(work.get("temp_use_mean_c"), errors="coerce").combine_first(
        pd.to_numeric(work.get("temp_c"), errors="coerce")
    )
    work["group_key"] = (
        work["temp_c"].map(lambda value: "nan" if pd.isna(value) else f"{float(value):g}")
        + "|"
        + work["target_ppm"].map(lambda value: "nan" if pd.isna(value) else f"{float(value):g}")
        + "|"
        + work["point_title"].fillna("").astype(str)
    )
    work["selected_source_pair"] = source_pair
    work["selected_ratio_source"] = ratio_source
    return work


def _current_family_frame(
    result: dict[str, Any],
    point_features: pd.DataFrame,
    source_row_lookup: dict[tuple[str, str], pd.Series],
    analyzer_id: str,
    source_pair: str,
) -> pd.DataFrame:
    base = result.get("point_reconciliation", pd.DataFrame())
    base = base.copy() if isinstance(base, pd.DataFrame) else pd.DataFrame()
    if base.empty:
        return pd.DataFrame()
    key = (analyzer_id, source_pair)
    if key not in source_row_lookup:
        return pd.DataFrame()
    source_row = source_row_lookup[key]
    zero_mode = str(source_row.get("zero_residual_mode") or "none")
    water_mode = str(source_row.get("water_zero_anchor_mode") or "none")
    variant = _variant_frame(result, analyzer_id, source_pair, zero_mode, water_mode)
    residuals = result.get("model_results", {}).get("residuals", pd.DataFrame())
    residuals = residuals.copy() if isinstance(residuals, pd.DataFrame) else pd.DataFrame()
    if residuals.empty:
        return pd.DataFrame()
    residuals = residuals[
        (residuals["analyzer_id"].astype(str) == analyzer_id)
        & (residuals["selected_source_pair"].astype(str) == source_pair)
        & (residuals["model_id"].astype(str) == str(source_row.get("model_id") or ""))
    ].copy()
    if "zero_residual_mode" in residuals.columns:
        residuals = residuals[residuals["zero_residual_mode"].astype(str) == zero_mode].copy()
    if "water_zero_anchor_mode" in residuals.columns and str(water_mode):
        residuals = residuals[residuals["water_zero_anchor_mode"].astype(str) == water_mode].copy()
    prediction_table = _prediction_table_from_residuals(residuals, str(source_row.get("score_source") or "validation_oof"))
    analyzer_base = base[base["analyzer_id"].astype(str) == analyzer_id].copy()
    analyzer_base = analyzer_base[["analyzer_id", "point_title", "point_row", "target_ppm", "temp_c", "old_pred_ppm"]]
    feature_cols = [
        "analyzer_id",
        "point_title",
        "point_row",
        "A_mean",
        "temp_model_c",
        "h2o_ratio_raw_mean",
        "h2o_ratio_filt_mean",
        "h2o_density_mean",
        "water_ratio_mean",
        "delta_h2o_ratio_vs_subzero_anchor",
        "delta_h2o_ratio_vs_zeroC_anchor",
        "mode2_semantic_profile",
        "mode2_legacy_raw_compare_safe",
        "mode2_is_baseline_bearing_profile",
    ]
    variant_cols = [column for column in feature_cols if column in variant.columns]
    merged = analyzer_base.merge(
        variant[variant_cols].drop_duplicates(subset=["analyzer_id", "point_title", "point_row"], keep="last"),
        on=["analyzer_id", "point_title", "point_row"],
        how="left",
    )
    merged = merged.merge(
        point_features,
        on=["analyzer_id", "point_title", "point_row"],
        how="left",
        suffixes=("", "_feature"),
    )
    merged = merged.merge(prediction_table, on=["point_title", "point_row"], how="left")
    merged["selected_source_pair"] = source_pair
    merged["selected_prediction_scope"] = merged["selected_prediction_scope"].fillna("overall_fit_fallback")
    merged["new_pred_ppm"] = pd.to_numeric(merged["selected_pred_ppm"], errors="coerce")
    merged["new_error"] = pd.to_numeric(merged["selected_error_ppm"], errors="coerce").combine_first(
        merged["new_pred_ppm"] - pd.to_numeric(merged["target_ppm"], errors="coerce")
    )
    return merged


def _custom_family_frame(
    result: dict[str, Any],
    point_features: pd.DataFrame,
    source_row_lookup: dict[tuple[str, str], pd.Series],
    analyzer_id: str,
    source_pair: str,
    model_family: str,
) -> pd.DataFrame:
    key = (analyzer_id, source_pair)
    if key not in source_row_lookup:
        return pd.DataFrame()
    source_row = source_row_lookup[key]
    zero_mode = str(source_row.get("zero_residual_mode") or "none")
    water_mode = str(source_row.get("water_zero_anchor_mode") or "none")
    analyzer_frame = _variant_frame(result, analyzer_id, source_pair, zero_mode, water_mode)
    if analyzer_frame.empty:
        return pd.DataFrame()
    requested_scope = str(source_row.get("score_source") or "validation_oof")
    current_model_id = str(source_row.get("model_id") or "")
    for column_name in (
        "delta_h2o_ratio_vs_legacy_summary_anchor",
        "delta_h2o_ratio_vs_legacy_zero_ppm_anchor",
        "delta_h2o_ratio_vs_subzero_anchor",
        "delta_h2o_ratio_vs_zeroC_anchor",
    ):
        if column_name not in analyzer_frame.columns:
            analyzer_frame[column_name] = math.nan
    active_lookup = {spec.model_id: spec for spec in active_model_specs(result.get("config"))}
    family_specs = {spec.ppm_family_mode: spec for spec in _family_specs_for_analyzer(current_model_id, active_lookup)}
    family_key = {
        "v5_abs_k_minimal_proxy": "v5_abs_k_minimal",
        "legacy_humidity_cross_D": "legacy_humidity_cross_D",
        "legacy_humidity_cross_E": "legacy_humidity_cross_E",
    }.get(model_family, "")
    if family_key not in family_specs:
        return pd.DataFrame()
    family_spec = family_specs[family_key]
    legacy_safe = bool(analyzer_frame.get("mode2_legacy_raw_compare_safe", pd.Series([False])).fillna(False).any())
    baseline_bearing = bool(analyzer_frame.get("mode2_is_baseline_bearing_profile", pd.Series([False])).fillna(False).any())
    humidity_proxy, _humidity_proxy_label = _choose_humidity_proxy(
        analyzer_frame,
        selected_source_pair=source_pair,
        legacy_safe=legacy_safe,
        baseline_bearing=baseline_bearing,
    )
    humidity_delta, _humidity_delta_label = _choose_humidity_delta(analyzer_frame)
    analyzer_frame["K_feature"] = humidity_proxy
    analyzer_frame["ratio_feature"] = pd.to_numeric(analyzer_frame.get("ratio_in_mean"), errors="coerce")
    analyzer_frame["pressure_feature"] = (
        pd.to_numeric(analyzer_frame.get("pressure_use_mean_hpa"), errors="coerce") - float(result.get("config").p_ref_hpa)
    ) / float(result.get("config").p_ref_hpa)
    analyzer_frame["humidity_delta_feature"] = humidity_delta
    analyzer_frame["humidity_relative_feature"] = analyzer_frame["ratio_feature"] * analyzer_frame["K_feature"]
    absorbance_column = str(source_row.get("absorbance_column") or "A_mean")
    try:
        _score_row, _coeffs, residual_rows = _fit_one_candidate(
            analyzer_df=analyzer_frame,
            spec=family_spec.to_absorbance_spec(),
            strategy=result.get("config").model_selection_strategy,
            score_weights=result.get("config").composite_weight_map(),
            legacy_score_weights=result.get("config").legacy_composite_weight_map(),
            enable_composite_score=result.get("config").enable_composite_score,
            absorbance_column=absorbance_column,
        )
    except Exception:
        return pd.DataFrame()
    prediction_table = _prediction_table_from_residuals(pd.DataFrame(residual_rows), requested_scope)
    base = result.get("point_reconciliation", pd.DataFrame())
    base = base.copy() if isinstance(base, pd.DataFrame) else pd.DataFrame()
    analyzer_base = base[base["analyzer_id"].astype(str) == analyzer_id].copy()
    analyzer_base = analyzer_base[["analyzer_id", "point_title", "point_row", "target_ppm", "temp_c", "old_pred_ppm"]]
    feature_cols = [
        "analyzer_id",
        "point_title",
        "point_row",
        "A_mean",
        "temp_model_c",
        "h2o_ratio_raw_mean",
        "h2o_ratio_filt_mean",
        "h2o_density_mean",
        "water_ratio_mean",
        "delta_h2o_ratio_vs_subzero_anchor",
        "delta_h2o_ratio_vs_zeroC_anchor",
        "mode2_semantic_profile",
        "mode2_legacy_raw_compare_safe",
        "mode2_is_baseline_bearing_profile",
    ]
    variant_cols = [column for column in feature_cols if column in analyzer_frame.columns]
    merged = analyzer_base.merge(
        analyzer_frame[variant_cols].drop_duplicates(subset=["analyzer_id", "point_title", "point_row"], keep="last"),
        on=["analyzer_id", "point_title", "point_row"],
        how="left",
    )
    merged = merged.merge(
        point_features,
        on=["analyzer_id", "point_title", "point_row"],
        how="left",
        suffixes=("", "_feature"),
    )
    merged = merged.merge(prediction_table, on=["point_title", "point_row"], how="left")
    merged["selected_source_pair"] = source_pair
    merged["selected_prediction_scope"] = merged["selected_prediction_scope"].fillna("overall_fit_fallback")
    merged["new_pred_ppm"] = pd.to_numeric(merged["selected_pred_ppm"], errors="coerce")
    merged["new_error"] = pd.to_numeric(merged["selected_error_ppm"], errors="coerce").combine_first(
        merged["new_pred_ppm"] - pd.to_numeric(merged["target_ppm"], errors="coerce")
    )
    return merged


def _base_candidate_frame(
    result: dict[str, Any],
    point_features: pd.DataFrame,
    source_row_lookup: dict[tuple[str, str], pd.Series],
    source_mode: str,
    model_family: str,
) -> pd.DataFrame:
    compare = result.get("point_reconciliation", pd.DataFrame())
    compare = compare.copy() if isinstance(compare, pd.DataFrame) else pd.DataFrame()
    if compare.empty:
        return pd.DataFrame()
    analyzers = sorted({str(value) for value in compare["analyzer_id"].dropna().astype(str).tolist() if str(value)})
    frames: list[pd.DataFrame] = []
    for analyzer_id in analyzers:
        available_pairs = {
            source_pair
            for one_analyzer, source_pair in source_row_lookup
            if one_analyzer == analyzer_id
        }
        source_pair = _pick_source_pair(source_mode, available_pairs)
        if not source_pair:
            continue
        if model_family == "current_ratio_poly_rt_p":
            frame = _current_family_frame(result, point_features, source_row_lookup, analyzer_id, source_pair)
        else:
            frame = _custom_family_frame(result, point_features, source_row_lookup, analyzer_id, source_pair, model_family)
        if frame.empty:
            continue
        frames.append(frame)
    if not frames:
        return pd.DataFrame()
    combined = pd.concat(frames, ignore_index=True)
    combined["base_error"] = pd.to_numeric(combined["new_error"], errors="coerce")
    combined["base_pred_ppm"] = pd.to_numeric(combined["new_pred_ppm"], errors="coerce")
    return combined


def _apply_residual_head(base_frame: pd.DataFrame, residual_head: str) -> tuple[pd.DataFrame, bool, str]:
    if base_frame.empty:
        return pd.DataFrame(), False, ""
    frame = base_frame.copy()
    frame["new_pred_ppm"] = pd.to_numeric(frame["base_pred_ppm"], errors="coerce")
    frame["new_error"] = pd.to_numeric(frame["base_error"], errors="coerce")
    diagnostic_only_flag = residual_head == "analyzer_specific_humidity_plus_temp_residual"
    diagnostic_only_reason = (
        "diagnostic-only: analyzer-specific residual head is not eligible as a deployable default"
        if diagnostic_only_flag
        else ""
    )
    feature_map = {
        "global_temp_residual": ["temp_feature_c", "temp_c"],
        "global_humidity_residual": ["humidity_feature", "h2o_density_mean"],
        "global_humidity_plus_temp_residual": ["humidity_feature", "h2o_density_mean", "temp_feature_c"],
        "analyzer_specific_humidity_plus_temp_residual": ["humidity_feature", "h2o_density_mean", "temp_feature_c"],
    }
    if residual_head == "none":
        return frame, diagnostic_only_flag, diagnostic_only_reason
    features = [column for column in feature_map[residual_head] if column in frame.columns]
    if not features:
        return frame, diagnostic_only_flag, diagnostic_only_reason
    if residual_head == "analyzer_specific_humidity_plus_temp_residual":
        correction = pd.Series(0.0, index=frame.index, dtype=float)
        for analyzer_id, subset in frame.groupby("analyzer_id", dropna=False):
            series, _fit_note = _fit_linear_residual_correction(
                subset,
                feature_columns=features,
                target_column="base_error",
                use_leave_one_out=True,
            )
            correction.loc[subset.index] = pd.to_numeric(series, errors="coerce").reindex(subset.index).fillna(0.0)
    else:
        correction, _fit_note = _fit_linear_residual_correction(
            frame,
            feature_columns=features,
            target_column="base_error",
            use_leave_one_out=True,
        )
    frame["residual_correction"] = pd.to_numeric(correction, errors="coerce")
    frame["new_error"] = pd.to_numeric(frame["base_error"], errors="coerce") - frame["residual_correction"]
    frame["new_pred_ppm"] = pd.to_numeric(frame["target_ppm"], errors="coerce") + frame["new_error"]
    return frame, diagnostic_only_flag, diagnostic_only_reason


def _build_run_candidate_frames(
    result: dict[str, Any],
    scope_b_run_id: str,
) -> list[pd.DataFrame]:
    run_id = str(result.get("run_name", "") or "")
    compare = result.get("point_reconciliation", pd.DataFrame())
    filtered = result.get("filtered", pd.DataFrame())
    model_results = result.get("model_results", {})
    scores = model_results.get("scores", pd.DataFrame()) if isinstance(model_results, dict) else pd.DataFrame()
    residuals = model_results.get("residuals", pd.DataFrame()) if isinstance(model_results, dict) else pd.DataFrame()
    compare = compare.copy() if isinstance(compare, pd.DataFrame) else pd.DataFrame()
    filtered = filtered.copy() if isinstance(filtered, pd.DataFrame) else pd.DataFrame()
    scores = scores.copy() if isinstance(scores, pd.DataFrame) else pd.DataFrame()
    residuals = residuals.copy() if isinstance(residuals, pd.DataFrame) else pd.DataFrame()
    if not run_id or compare.empty or filtered.empty:
        return []
    comparison_scope = _scope_name_for_run(run_id, scope_b_run_id)
    if comparison_scope == SCOPE_A_NAME:
        allowed_analyzers = {"GA02", "GA03"}
        compare = compare[compare["analyzer_id"].astype(str).isin(allowed_analyzers)].copy()
        filtered = filtered[filtered["analyzer"].astype(str).isin(allowed_analyzers)].copy()
        if not scores.empty and "analyzer_id" in scores.columns:
            scores = scores[scores["analyzer_id"].astype(str).isin(allowed_analyzers)].copy()
        if not residuals.empty and "analyzer_id" in residuals.columns:
            residuals = residuals[residuals["analyzer_id"].astype(str).isin(allowed_analyzers)].copy()
    if compare.empty or filtered.empty or scores.empty:
        return []
    scoped_model_results = dict(model_results) if isinstance(model_results, dict) else {}
    scoped_model_results["scores"] = scores
    scoped_model_results["residuals"] = residuals
    scoped_result = dict(result)
    scoped_result["point_reconciliation"] = compare
    scoped_result["filtered"] = filtered
    scoped_result["model_results"] = scoped_model_results
    point_features = _point_feature_summary(filtered)
    source_row_lookup = _best_source_rows(scores)
    if not source_row_lookup:
        return []
    outputs: list[pd.DataFrame] = []
    for source_mode in SOURCE_MODES:
        for model_family in MODEL_FAMILIES:
            base_frame = _base_candidate_frame(scoped_result, point_features, source_row_lookup, source_mode, model_family)
            if base_frame.empty:
                continue
            for residual_head in RESIDUAL_HEADS:
                candidate_frame, diagnostic_only_flag, diagnostic_only_reason = _apply_residual_head(base_frame, residual_head)
                if candidate_frame.empty:
                    continue
                candidate_id = _candidate_id(source_mode, model_family, residual_head)
                surface2 = candidate_frame.copy()
                surface2["run_id"] = run_id
                surface2["comparison_surface"] = SECONDARY_SURFACE
                surface2["old_value_source"] = "old_residual_csv_prediction_simplified"
                surface2["new_value_source"] = "selected_pred_ppm_from_debugger"
                surface2 = _normalize_surface_frame(surface2)
                surface2 = _candidate_scope_frame(
                    surface2,
                    run_id=run_id,
                    comparison_scope=comparison_scope,
                    comparison_surface=SECONDARY_SURFACE,
                    candidate_id=candidate_id,
                    source_mode=source_mode,
                    model_family=model_family,
                    residual_head=residual_head,
                    diagnostic_only_flag=diagnostic_only_flag,
                    diagnostic_only_reason=diagnostic_only_reason,
                )
                surface1 = _native_surface_frame(run_id, compare, filtered, candidate_frame)
                if not surface1.empty:
                    surface1 = _candidate_scope_frame(
                        surface1,
                        run_id=run_id,
                        comparison_scope=comparison_scope,
                        comparison_surface=PRIMARY_SURFACE,
                        candidate_id=candidate_id,
                        source_mode=source_mode,
                        model_family=model_family,
                        residual_head=residual_head,
                        diagnostic_only_flag=diagnostic_only_flag,
                        diagnostic_only_reason=diagnostic_only_reason,
                    )
                    outputs.append(surface1)
                outputs.append(surface2)
    return outputs


def _detail_rows(point_surfaces: pd.DataFrame) -> pd.DataFrame:
    if point_surfaces.empty:
        return _empty_frame(DETAIL_COLUMNS)
    detail_rows: list[dict[str, Any]] = []
    group_keys = [
        "run_id",
        "comparison_scope",
        "comparison_surface",
        "candidate_id",
        "source_mode",
        "model_family",
        "residual_head",
    ]
    group_flags: dict[tuple[str, ...], tuple[bool, bool, float]] = {}
    for keys, subset in point_surfaces.groupby(group_keys, dropna=False):
        group_flags[tuple(str(item) for item in keys)] = _gain_flags(subset)
    for keys, subset in point_surfaces.groupby(group_keys + ["analyzer_id"], dropna=False):
        (
            run_id,
            comparison_scope,
            comparison_surface,
            candidate_id,
            source_mode,
            model_family,
            residual_head,
            analyzer_id,
        ) = [str(item) for item in keys]
        overall_old = _metrics(subset["old_error"])["rmse"]
        overall_new = _metrics(subset["new_error"])["rmse"]
        zero_old = _segment_rmse(subset, "old_error", "zero")
        zero_new = _segment_rmse(subset, "new_error", "zero")
        low_old = _segment_rmse(subset, "old_error", "low")
        low_new = _segment_rmse(subset, "new_error", "low")
        main_old = _segment_rmse(subset, "old_error", "main")
        main_new = _segment_rmse(subset, "new_error", "main")
        winners = subset.get("winner_for_point", pd.Series(dtype=object)).fillna("tie").astype(str)
        selected_scope_series = subset.get("selected_prediction_scope", pd.Series(dtype=object))
        main_gain, analyzer_concentrated, _top_share = group_flags[
            (run_id, comparison_scope, comparison_surface, candidate_id, source_mode, model_family, residual_head)
        ]
        label, description = _scope_meta(comparison_scope)
        detail_rows.append(
            {
                "run_id": run_id,
                "comparison_scope": comparison_scope,
                "comparison_scope_label": label,
                "comparison_scope_description": description,
                "comparison_surface": comparison_surface,
                "analyzer_id": analyzer_id,
                "candidate_id": candidate_id,
                "source_mode": source_mode,
                "model_family": model_family,
                "residual_head": residual_head,
                "diagnostic_only_flag": bool(subset["diagnostic_only_flag"].fillna(False).any()),
                "diagnostic_only_reason": _first_text(subset["diagnostic_only_reason"]),
                "ratio_source_mode": source_mode,
                "mode2_semantic_profile": _majority_text(
                    subset.get("mode2_semantic_profile", pd.Series(dtype=object)),
                    default="mode2_semantics_unknown",
                ),
                "selected_prediction_scope": "|".join(
                    sorted(
                        {
                            str(value).strip()
                            for value in selected_scope_series.dropna().astype(str).tolist()
                            if str(value).strip()
                        }
                    )
                ),
                "selected_prediction_scope_majority": _majority_text(selected_scope_series, default="unknown"),
                "overall_rmse_old": overall_old,
                "overall_rmse_new": overall_new,
                "zero_rmse_old": zero_old,
                "zero_rmse_new": zero_new,
                "low_rmse_old": low_old,
                "low_rmse_new": low_new,
                "main_rmse_old": main_old,
                "main_rmse_new": main_new,
                "pointwise_win_count_vs_old": int((winners == "new_chain").sum()),
                "pointwise_loss_count_vs_old": int((winners == "old_chain").sum()),
                "improvement_pct_overall": _improvement_pct(overall_old, overall_new),
                "improvement_pct_zero": _improvement_pct(zero_old, zero_new),
                "improvement_pct_low": _improvement_pct(low_old, low_new),
                "improvement_pct_main": _improvement_pct(main_old, main_new),
                "promotion_score": math.nan,
                "promotion_rank": math.nan,
                "raw_filt_divergence_mean": float(pd.to_numeric(subset.get("raw_filt_divergence"), errors="coerce").mean()),
                "raw_filt_divergence_max": float(pd.to_numeric(subset.get("raw_filt_divergence"), errors="coerce").max()),
                "residual_vs_temp_corr": _safe_corr(
                    subset.get("new_error", pd.Series(dtype=float)),
                    subset.get("temp_feature_c", subset.get("temp_c", pd.Series(dtype=float))),
                ),
                "residual_vs_humidity_corr": _safe_corr(
                    subset.get("new_error", pd.Series(dtype=float)),
                    subset.get("humidity_feature", pd.Series(dtype=float)),
                ),
                "residual_vs_signal_corr": _safe_corr(
                    subset.get("new_error", pd.Series(dtype=float)),
                    subset.get("co2_signal_mean", pd.Series(dtype=float)),
                ),
                "whether_gain_comes_from_main_segment": bool(main_gain),
                "whether_gain_is_analyzer_concentrated": bool(analyzer_concentrated),
                "likely_root_cause_bucket": _candidate_bucket(source_mode, model_family, residual_head),
                "point_count": int(len(subset)),
            }
        )
    detail = pd.DataFrame(detail_rows).sort_values(
        ["comparison_scope", "comparison_surface", "candidate_id", "run_id", "analyzer_id"],
        ignore_index=True,
    )
    return detail.reindex(columns=list(DETAIL_COLUMNS) + [column for column in detail.columns if column not in DETAIL_COLUMNS])


def _summary_scope_surface_rows(
    point_surfaces: pd.DataFrame,
    expected_analyzers: dict[str, set[str]],
) -> pd.DataFrame:
    if point_surfaces.empty:
        return _empty_frame(SUMMARY_COLUMNS)
    rows: list[dict[str, Any]] = []
    group_keys = [
        "comparison_scope",
        "comparison_surface",
        "candidate_id",
        "source_mode",
        "model_family",
        "residual_head",
    ]
    for keys, subset in point_surfaces.groupby(group_keys, dropna=False):
        comparison_scope, comparison_surface, candidate_id, source_mode, model_family, residual_head = [str(item) for item in keys]
        label, description = _scope_meta(comparison_scope)
        overall_old = _metrics(subset["old_error"])["rmse"]
        overall_new = _metrics(subset["new_error"])["rmse"]
        zero_old = _segment_rmse(subset, "old_error", "zero")
        zero_new = _segment_rmse(subset, "new_error", "zero")
        low_old = _segment_rmse(subset, "old_error", "low")
        low_new = _segment_rmse(subset, "new_error", "low")
        main_old = _segment_rmse(subset, "old_error", "main")
        main_new = _segment_rmse(subset, "new_error", "main")
        winners = subset.get("winner_for_point", pd.Series(dtype=object)).fillna("tie").astype(str)
        main_gain, analyzer_concentrated, _top_share = _gain_flags(subset)
        observed_analyzers = int(subset.get("analyzer_id", pd.Series(dtype=object)).astype(str).nunique())
        expected_count = int(len(expected_analyzers.get(comparison_scope, set())))
        coverage_ratio = float(observed_analyzers) / float(expected_count) if expected_count > 0 else math.nan
        rows.append(
            {
                "summary_scope": "candidate_scope_surface",
                "comparison_scope": comparison_scope,
                "comparison_scope_label": label,
                "comparison_scope_description": description,
                "comparison_surface": comparison_surface,
                "candidate_id": candidate_id,
                "source_mode": source_mode,
                "model_family": model_family,
                "residual_head": residual_head,
                "diagnostic_only_flag": bool(subset["diagnostic_only_flag"].fillna(False).any()),
                "diagnostic_only_reason": _first_text(subset["diagnostic_only_reason"]),
                "selected_prediction_scope_majority": _majority_text(
                    subset.get("selected_prediction_scope", pd.Series(dtype=object)),
                    default="unknown",
                ),
                "overall_rmse_old": overall_old,
                "overall_rmse_new": overall_new,
                "zero_rmse_old": zero_old,
                "zero_rmse_new": zero_new,
                "low_rmse_old": low_old,
                "low_rmse_new": low_new,
                "main_rmse_old": main_old,
                "main_rmse_new": main_new,
                "improvement_pct_overall": _improvement_pct(overall_old, overall_new),
                "improvement_pct_zero": _improvement_pct(zero_old, zero_new),
                "improvement_pct_low": _improvement_pct(low_old, low_new),
                "improvement_pct_main": _improvement_pct(main_old, main_new),
                "pointwise_win_count_vs_old": int((winners == "new_chain").sum()),
                "pointwise_loss_count_vs_old": int((winners == "old_chain").sum()),
                "promotion_score": math.nan,
                "promotion_rank": math.nan,
                "coverage_ratio": coverage_ratio,
                "expected_analyzer_count": expected_count,
                "observed_analyzer_count": observed_analyzers,
                "run_count": int(subset.get("run_id", pd.Series(dtype=object)).astype(str).nunique()),
                "raw_filt_divergence_mean": float(pd.to_numeric(subset.get("raw_filt_divergence"), errors="coerce").mean()),
                "raw_filt_divergence_max": float(pd.to_numeric(subset.get("raw_filt_divergence"), errors="coerce").max()),
                "residual_vs_temp_corr": _safe_corr(
                    subset.get("new_error", pd.Series(dtype=float)),
                    subset.get("temp_feature_c", subset.get("temp_c", pd.Series(dtype=float))),
                ),
                "residual_vs_humidity_corr": _safe_corr(
                    subset.get("new_error", pd.Series(dtype=float)),
                    subset.get("humidity_feature", pd.Series(dtype=float)),
                ),
                "residual_vs_signal_corr": _safe_corr(
                    subset.get("new_error", pd.Series(dtype=float)),
                    subset.get("co2_signal_mean", pd.Series(dtype=float)),
                ),
                "whether_gain_comes_from_main_segment": bool(main_gain),
                "whether_gain_is_analyzer_concentrated": bool(analyzer_concentrated),
                "likely_root_cause_bucket": _candidate_bucket(source_mode, model_family, residual_head),
                "future_deployable_candidate_flag": False,
            }
        )
    summary = pd.DataFrame(rows).sort_values(
        ["comparison_scope", "comparison_surface", "candidate_id"],
        ignore_index=True,
    )
    return summary.reindex(columns=list(SUMMARY_COLUMNS))


def _score_component(value: float, *, weight: float, invert_negative: bool = False) -> float:
    if pd.isna(value):
        return 0.0
    numeric = float(np.clip(value, -100.0, 100.0))
    if invert_negative:
        numeric = -max(0.0, -numeric)
    return weight * numeric


def _promotion_rows(summary_scope_surface: pd.DataFrame) -> pd.DataFrame:
    if summary_scope_surface.empty:
        return _empty_frame(SUMMARY_COLUMNS)
    rows: list[dict[str, Any]] = []
    for candidate_id, subset in summary_scope_surface.groupby("candidate_id", dropna=False):
        lookup = subset.set_index(["comparison_scope", "comparison_surface"])

        def _row(scope_name: str, surface_name: str) -> pd.Series:
            if (scope_name, surface_name) in lookup.index:
                one = lookup.loc[(scope_name, surface_name)]
                return one.iloc[0] if isinstance(one, pd.DataFrame) else one
            return pd.Series(dtype=object)

        scope_b_surface1 = _row(SCOPE_B_NAME, PRIMARY_SURFACE)
        scope_b_surface2 = _row(SCOPE_B_NAME, SECONDARY_SURFACE)
        scope_a_surface1 = _row(SCOPE_A_NAME, PRIMARY_SURFACE)
        b1_overall = pd.to_numeric(pd.Series([scope_b_surface1.get("improvement_pct_overall")]), errors="coerce").iloc[0]
        b1_main = pd.to_numeric(pd.Series([scope_b_surface1.get("improvement_pct_main")]), errors="coerce").iloc[0]
        b1_zero = pd.to_numeric(pd.Series([scope_b_surface1.get("improvement_pct_zero")]), errors="coerce").iloc[0]
        b2_overall = pd.to_numeric(pd.Series([scope_b_surface2.get("improvement_pct_overall")]), errors="coerce").iloc[0]
        b2_main = pd.to_numeric(pd.Series([scope_b_surface2.get("improvement_pct_main")]), errors="coerce").iloc[0]
        b2_zero = pd.to_numeric(pd.Series([scope_b_surface2.get("improvement_pct_zero")]), errors="coerce").iloc[0]
        a1_overall = pd.to_numeric(pd.Series([scope_a_surface1.get("improvement_pct_overall")]), errors="coerce").iloc[0]
        a1_main = pd.to_numeric(pd.Series([scope_a_surface1.get("improvement_pct_main")]), errors="coerce").iloc[0]
        score = 0.0
        score += _score_component(b1_overall, weight=0.34)
        score += _score_component(b1_main, weight=0.26)
        score += _score_component(b1_zero, weight=0.12, invert_negative=True)
        score += _score_component(a1_overall, weight=0.10, invert_negative=True)
        score += _score_component(a1_main, weight=0.08, invert_negative=True)
        score += _score_component(b2_overall, weight=0.06, invert_negative=True)
        score += _score_component(b2_main, weight=0.04, invert_negative=True)
        score += _score_component(b2_zero, weight=0.03, invert_negative=True)
        score += _score_component(
            -abs(
                float(b2_overall) if pd.notna(b2_overall) else 0.0
                - (float(b1_overall) if pd.notna(b1_overall) else 0.0)
            ),
            weight=0.02,
        )
        score += _score_component(
            -abs(
                float(b2_main) if pd.notna(b2_main) else 0.0
                - (float(b1_main) if pd.notna(b1_main) else 0.0)
            ),
            weight=0.02,
        )
        diagnostic_only_flag = bool(subset["diagnostic_only_flag"].fillna(False).any())
        coverage_penalty = 0.0
        coverage_ratio = pd.to_numeric(pd.Series([scope_b_surface1.get("coverage_ratio")]), errors="coerce").iloc[0]
        if pd.notna(coverage_ratio):
            coverage_penalty = max(0.0, 1.0 - float(coverage_ratio)) * 20.0
        concentration_penalty = 10.0 if bool(scope_b_surface1.get("whether_gain_is_analyzer_concentrated", False)) else 0.0
        if not concentration_penalty and bool(scope_b_surface2.get("whether_gain_is_analyzer_concentrated", False)):
            concentration_penalty = 5.0
        diagnostic_penalty = 8.0 if diagnostic_only_flag else 0.0
        final_score = float(score - coverage_penalty - concentration_penalty - diagnostic_penalty)
        likely_bucket = _first_text(subset["likely_root_cause_bucket"], default="mixed")
        future_candidate_flag = bool(
            not diagnostic_only_flag
            and float(b1_overall or 0.0) > 0.0
            and float(b1_main or 0.0) > 0.0
            and float(b1_zero or 0.0) > -25.0
            and float(a1_overall or 0.0) > -25.0
            and float(a1_main or 0.0) > -25.0
            and float(b2_overall or 0.0) > -25.0
            and float(b2_main or 0.0) > -25.0
        )
        rows.append(
            {
                "summary_scope": "candidate_overall",
                "comparison_scope": "all_scopes",
                "comparison_scope_label": "All scopes (scored separately, not merged into one headline)",
                "comparison_scope_description": "promotion score combines fixed primary acceptance context with secondary diagnostic performance",
                "comparison_surface": "all_surfaces",
                "candidate_id": str(candidate_id),
                "source_mode": _first_text(subset["source_mode"]),
                "model_family": _first_text(subset["model_family"]),
                "residual_head": _first_text(subset["residual_head"]),
                "diagnostic_only_flag": diagnostic_only_flag,
                "diagnostic_only_reason": _first_text(subset["diagnostic_only_reason"]),
                "selected_prediction_scope_majority": _first_text(
                    subset[subset["comparison_surface"] == SECONDARY_SURFACE]["selected_prediction_scope_majority"],
                    default="unknown",
                ),
                "overall_rmse_old": pd.to_numeric(pd.Series([scope_b_surface1.get("overall_rmse_old")]), errors="coerce").iloc[0],
                "overall_rmse_new": pd.to_numeric(pd.Series([scope_b_surface1.get("overall_rmse_new")]), errors="coerce").iloc[0],
                "zero_rmse_old": pd.to_numeric(pd.Series([scope_b_surface1.get("zero_rmse_old")]), errors="coerce").iloc[0],
                "zero_rmse_new": pd.to_numeric(pd.Series([scope_b_surface1.get("zero_rmse_new")]), errors="coerce").iloc[0],
                "low_rmse_old": pd.to_numeric(pd.Series([scope_b_surface1.get("low_rmse_old")]), errors="coerce").iloc[0],
                "low_rmse_new": pd.to_numeric(pd.Series([scope_b_surface1.get("low_rmse_new")]), errors="coerce").iloc[0],
                "main_rmse_old": pd.to_numeric(pd.Series([scope_b_surface1.get("main_rmse_old")]), errors="coerce").iloc[0],
                "main_rmse_new": pd.to_numeric(pd.Series([scope_b_surface1.get("main_rmse_new")]), errors="coerce").iloc[0],
                "improvement_pct_overall": b1_overall,
                "improvement_pct_zero": b1_zero,
                "improvement_pct_low": pd.to_numeric(pd.Series([scope_b_surface1.get("improvement_pct_low")]), errors="coerce").iloc[0],
                "improvement_pct_main": b1_main,
                "pointwise_win_count_vs_old": int(pd.to_numeric(pd.Series([scope_b_surface1.get("pointwise_win_count_vs_old")]), errors="coerce").fillna(0).iloc[0]),
                "pointwise_loss_count_vs_old": int(pd.to_numeric(pd.Series([scope_b_surface1.get("pointwise_loss_count_vs_old")]), errors="coerce").fillna(0).iloc[0]),
                "promotion_score": final_score,
                "promotion_rank": math.nan,
                "coverage_ratio": coverage_ratio,
                "expected_analyzer_count": pd.to_numeric(pd.Series([scope_b_surface1.get("expected_analyzer_count")]), errors="coerce").iloc[0],
                "observed_analyzer_count": pd.to_numeric(pd.Series([scope_b_surface1.get("observed_analyzer_count")]), errors="coerce").iloc[0],
                "run_count": pd.to_numeric(pd.Series([scope_b_surface1.get("run_count")]), errors="coerce").iloc[0],
                "raw_filt_divergence_mean": pd.to_numeric(pd.Series([scope_b_surface1.get("raw_filt_divergence_mean")]), errors="coerce").iloc[0],
                "raw_filt_divergence_max": pd.to_numeric(pd.Series([scope_b_surface1.get("raw_filt_divergence_max")]), errors="coerce").iloc[0],
                "residual_vs_temp_corr": pd.to_numeric(pd.Series([scope_b_surface1.get("residual_vs_temp_corr")]), errors="coerce").iloc[0],
                "residual_vs_humidity_corr": pd.to_numeric(pd.Series([scope_b_surface1.get("residual_vs_humidity_corr")]), errors="coerce").iloc[0],
                "residual_vs_signal_corr": pd.to_numeric(pd.Series([scope_b_surface1.get("residual_vs_signal_corr")]), errors="coerce").iloc[0],
                "whether_gain_comes_from_main_segment": bool(scope_b_surface1.get("whether_gain_comes_from_main_segment", False)),
                "whether_gain_is_analyzer_concentrated": bool(scope_b_surface1.get("whether_gain_is_analyzer_concentrated", False)),
                "likely_root_cause_bucket": likely_bucket,
                "future_deployable_candidate_flag": future_candidate_flag,
            }
        )
    overall = pd.DataFrame(rows).sort_values(["promotion_score", "candidate_id"], ascending=[False, True], ignore_index=True)
    if not overall.empty:
        overall["promotion_rank"] = np.arange(1, len(overall) + 1, dtype=int)
    return overall.reindex(columns=list(SUMMARY_COLUMNS))


def _apply_promotion_scores(
    detail: pd.DataFrame,
    summary_scope_surface: pd.DataFrame,
    overall: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if overall.empty:
        return detail, summary_scope_surface
    score_map = overall.set_index("candidate_id")["promotion_score"].to_dict()
    rank_map = overall.set_index("candidate_id")["promotion_rank"].to_dict()
    future_map = overall.set_index("candidate_id")["future_deployable_candidate_flag"].to_dict()
    detail = detail.copy()
    summary_scope_surface = summary_scope_surface.copy()
    detail["promotion_score"] = detail["candidate_id"].map(score_map)
    detail["promotion_rank"] = detail["candidate_id"].map(rank_map)
    summary_scope_surface["promotion_score"] = summary_scope_surface["candidate_id"].map(score_map)
    summary_scope_surface["promotion_rank"] = summary_scope_surface["candidate_id"].map(rank_map)
    summary_scope_surface["future_deployable_candidate_flag"] = summary_scope_surface["candidate_id"].map(future_map)
    return detail, summary_scope_surface


def build_candidate_tournament_outputs(
    run_results: list[dict[str, Any]],
    *,
    scope_b_run_id: str = "run_20260410_132440",
) -> dict[str, pd.DataFrame]:
    """Build batch-level candidate tournament outputs."""

    point_frames: list[pd.DataFrame] = []
    for result in run_results:
        point_frames.extend(_build_run_candidate_frames(result, scope_b_run_id))
    point_surfaces = (
        pd.concat([frame for frame in point_frames if not frame.empty], ignore_index=True)
        if any(not frame.empty for frame in point_frames)
        else pd.DataFrame()
    )
    if point_surfaces.empty:
        empty_detail = _empty_frame(DETAIL_COLUMNS)
        empty_summary = _empty_frame(SUMMARY_COLUMNS)
        return {
            "detail": empty_detail,
            "summary": empty_summary,
            "candidate_overall": empty_summary,
            "point_surfaces": point_surfaces,
        }
    expected_analyzers = _expected_analyzers_by_scope(run_results, scope_b_run_id)
    detail = _detail_rows(point_surfaces)
    summary_scope_surface = _summary_scope_surface_rows(point_surfaces, expected_analyzers)
    overall = _promotion_rows(summary_scope_surface)
    detail, summary_scope_surface = _apply_promotion_scores(detail, summary_scope_surface, overall)
    summary = pd.concat([summary_scope_surface, overall], ignore_index=True, sort=False)
    summary = summary.sort_values(
        ["summary_scope", "promotion_rank", "comparison_scope", "comparison_surface", "candidate_id"],
        ignore_index=True,
        na_position="last",
    )
    summary = summary.reindex(columns=list(SUMMARY_COLUMNS))
    detail = detail.reindex(columns=list(DETAIL_COLUMNS))
    return {
        "detail": detail,
        "summary": summary,
        "candidate_overall": overall.reindex(columns=list(SUMMARY_COLUMNS)),
        "point_surfaces": point_surfaces,
    }
