"""Diagnostic helpers for explaining why the new chain loses."""

from __future__ import annotations

import math
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from .absorbance_models import evaluate_absorbance_models


def _metrics(errors: pd.Series | np.ndarray) -> dict[str, float]:
    clean = pd.to_numeric(pd.Series(errors), errors="coerce").dropna()
    if clean.empty:
        return {
            "rmse": math.nan,
            "mae": math.nan,
            "max_abs_error": math.nan,
            "bias": math.nan,
            "std": math.nan,
        }
    values = clean.to_numpy(dtype=float)
    return {
        "rmse": float(np.sqrt(np.mean(np.square(values)))),
        "mae": float(np.mean(np.abs(values))),
        "max_abs_error": float(np.max(np.abs(values))),
        "bias": float(np.mean(values)),
        "std": float(np.std(values)),
    }


def _winner(old_value: float, new_value: float) -> str:
    if not math.isfinite(old_value) and not math.isfinite(new_value):
        return "tie"
    if not math.isfinite(old_value):
        return "new_chain"
    if not math.isfinite(new_value):
        return "old_chain"
    if abs(old_value - new_value) <= 1.0e-12:
        return "tie"
    return "old_chain" if old_value < new_value else "new_chain"


def _temp_bias_spread(frame: pd.DataFrame, error_column: str) -> float:
    grouped = (
        frame.dropna(subset=["temp_c", error_column])
        .groupby("temp_c", dropna=False)[error_column]
        .mean()
    )
    if grouped.empty:
        return math.nan
    return float(np.std(grouped.to_numpy(dtype=float)))


def _consensus_text(series: pd.Series, default: str) -> str:
    values = series.dropna().astype(str).str.strip()
    values = values[values != ""]
    unique = values.drop_duplicates().tolist()
    if not unique:
        return default
    if len(unique) == 1:
        return str(unique[0])
    return default


def _consensus_bool(series: pd.Series, *, default: bool = False) -> bool:
    if series.empty:
        return default
    mapped = series.map(lambda value: bool(value) if pd.notna(value) else np.nan)
    values = mapped.dropna().astype(bool).drop_duplicates().tolist()
    if not values:
        return default
    if len(values) == 1:
        return bool(values[0])
    return default


def build_analyzer_mode2_summary(filtered: pd.DataFrame) -> pd.DataFrame:
    """Collapse MODE2 semantic metadata to one guarded summary per analyzer."""

    if filtered.empty or "analyzer" not in filtered.columns:
        return pd.DataFrame(
            columns=[
                "analyzer",
                "mode2_semantic_profile",
                "mode2_legacy_raw_compare_safe",
                "mode2_is_baseline_bearing_profile",
            ]
        )

    rows: list[dict[str, Any]] = []
    for analyzer_id, subset in filtered.groupby("analyzer", dropna=False):
        profile_values = (
            subset.get("mode2_semantic_profile", pd.Series(dtype=str))
            .dropna()
            .astype(str)
            .str.strip()
        )
        unique_profiles = profile_values[profile_values != ""].drop_duplicates().tolist()
        if not unique_profiles:
            profile = "mode2_semantics_unknown"
        elif len(unique_profiles) == 1:
            profile = str(unique_profiles[0])
        else:
            profile = "mixed_profile"
        rows.append(
            {
                "analyzer": analyzer_id,
                "mode2_semantic_profile": profile,
                "mode2_legacy_raw_compare_safe": _consensus_bool(
                    subset.get("mode2_legacy_raw_compare_safe", pd.Series(dtype=object)),
                    default=False,
                ),
                "mode2_is_baseline_bearing_profile": (
                    subset.get("mode2_is_baseline_bearing_profile", pd.Series(dtype=object))
                    .dropna()
                    .map(bool)
                    .any()
                    if "mode2_is_baseline_bearing_profile" in subset.columns
                    else False
                ),
            }
        )
    return pd.DataFrame(rows).sort_values(["analyzer"], ignore_index=True)


def build_point_raw_summary(filtered: pd.DataFrame) -> pd.DataFrame:
    """Aggregate stable point-level raw fields once for later comparisons."""

    point_summary = (
        filtered.groupby(
            ["analyzer", "point_title", "point_row", "point_tag", "temp_set_c", "target_co2_ppm"],
            dropna=False,
        )
        .agg(
            ratio_co2_raw_mean=("ratio_co2_raw", "mean"),
            ratio_co2_filt_mean=("ratio_co2_filt", "mean"),
            pressure_std_hpa_mean=("pressure_std_hpa", "mean"),
            pressure_corr_hpa_mean=("pressure_corr_hpa", "mean"),
            temp_std_c_mean=("temp_std_c", "mean"),
            temp_corr_c_mean=("temp_corr_c", "mean"),
        )
        .reset_index()
    )
    mode2_summary = build_analyzer_mode2_summary(filtered)
    if mode2_summary.empty:
        point_summary["mode2_semantic_profile"] = "mode2_semantics_unknown"
        point_summary["mode2_legacy_raw_compare_safe"] = False
        point_summary["mode2_is_baseline_bearing_profile"] = False
        return point_summary
    return point_summary.merge(mode2_summary, on="analyzer", how="left")


def _pressure_branch_specs(config: Any) -> tuple[tuple[str, str | None, str], ...]:
    return (
        ("no_pressure_norm", None, "A = -ln(R / R0(T))"),
        ("pressure_std", "pressure_std_hpa", "A = -ln(R / R0(T)) * (P_ref / P_std)"),
        ("pressure_corr", "pressure_corr_hpa", "A = -ln(R / R0(T)) * (P_ref / P_corr)"),
        ("alt_divide_only_std", "pressure_std_hpa", "A_alt = -ln(R / R0(T)) / P_std"),
        ("alt_divide_only_corr", "pressure_corr_hpa", "A_alt = -ln(R / R0(T)) / P_corr"),
    )


def _chain_context(temp_source: str, pressure_branch: str) -> str:
    if temp_source == "temp_std_c" and pressure_branch == "pressure_std":
        return "physics_upper_bound"
    if temp_source == "temp_corr_c" and pressure_branch == "pressure_corr":
        return "deployable_chain"
    return "diagnostic_only"


def _short_ratio_label(source: str) -> str:
    return "raw" if source == "ratio_co2_raw" else "filt"


def build_diagnostic_absorbance_points(
    filtered: pd.DataFrame,
    r0_lookup: dict[tuple[str, str, str, str], Any],
    config: Any,
) -> pd.DataFrame:
    """Create a point-level absorbance matrix for order/source/pressure diagnostics."""

    point_keys = ["analyzer", "point_title", "point_row", "point_tag", "temp_set_c", "target_co2_ppm"]
    rows: list[dict[str, Any]] = []

    for analyzer, subset in filtered.groupby("analyzer"):
        for ratio_in_source in config.ratio_sources:
            for r0_fit_source in config.ratio_sources:
                for temp_source in config.temp_sources:
                    fit_key = (analyzer, r0_fit_source, temp_source, config.default_r0_model)
                    if fit_key not in r0_lookup:
                        continue
                    fit = r0_lookup[fit_key]
                    work = subset[point_keys + ["sample_index", ratio_in_source, temp_source, "pressure_std_hpa", "pressure_corr_hpa"]].copy()
                    work = work.rename(columns={ratio_in_source: "ratio_in", temp_source: "temp_use_c"})
                    work["R0_T"] = fit.evaluate(work["temp_use_c"].to_numpy(dtype=float))
                    ratio_clamped = np.clip(work["ratio_in"].to_numpy(dtype=float), config.eps, None)
                    r0_clamped = np.clip(work["R0_T"].to_numpy(dtype=float), config.eps, None)
                    work["log_term"] = -np.log(ratio_clamped / r0_clamped)

                    for pressure_branch, pressure_column, branch_formula in _pressure_branch_specs(config):
                        branch_work = work.copy()
                        if pressure_column is None:
                            branch_work["pressure_use_hpa"] = np.nan
                            branch_work["A_sample"] = branch_work["log_term"]
                            pressure_label = "none"
                        else:
                            pressure_values = np.clip(branch_work[pressure_column].to_numpy(dtype=float), config.p_min_hpa, None)
                            branch_work["pressure_use_hpa"] = pressure_values
                            pressure_label = "P_std" if pressure_column == "pressure_std_hpa" else "P_corr"
                            if pressure_branch.startswith("alt_divide_only"):
                                branch_work["A_sample"] = branch_work["log_term"] / pressure_values
                            else:
                                branch_work["A_sample"] = branch_work["log_term"] * (config.p_ref_hpa / pressure_values)

                        grouped = (
                            branch_work.groupby(point_keys, dropna=False)
                            .agg(
                                sample_count=("sample_index", "count"),
                                ratio_in_mean=("ratio_in", "mean"),
                                temp_use_mean_c=("temp_use_c", "mean"),
                                pressure_use_mean_hpa=("pressure_use_hpa", "mean"),
                                R0_T_mean=("R0_T", "mean"),
                                A_samplewise_mean=("A_sample", "mean"),
                                A_samplewise_std=("A_sample", "std"),
                                A_samplewise_min=("A_sample", "min"),
                                A_samplewise_max=("A_sample", "max"),
                            )
                            .reset_index()
                        )
                        grouped["A_samplewise_std"] = grouped["A_samplewise_std"].fillna(0.0)
                        r0_at_mean = fit.evaluate(grouped["temp_use_mean_c"].to_numpy(dtype=float))
                        ratio_mean_clamped = np.clip(grouped["ratio_in_mean"].to_numpy(dtype=float), config.eps, None)
                        r0_mean_clamped = np.clip(np.asarray(r0_at_mean, dtype=float), config.eps, None)
                        log_term_mean = -np.log(ratio_mean_clamped / r0_mean_clamped)
                        if pressure_column is None:
                            a_from_mean = log_term_mean
                        else:
                            pressure_mean = np.clip(grouped["pressure_use_mean_hpa"].to_numpy(dtype=float), config.p_min_hpa, None)
                            if pressure_branch.startswith("alt_divide_only"):
                                a_from_mean = log_term_mean / pressure_mean
                            else:
                                a_from_mean = log_term_mean * (config.p_ref_hpa / pressure_mean)

                        base_columns = grouped.copy()
                        base_columns["ratio_in_source"] = ratio_in_source
                        base_columns["r0_fit_source"] = r0_fit_source
                        base_columns["temp_source"] = temp_source
                        base_columns["pressure_branch"] = pressure_branch
                        base_columns["pressure_source_used"] = pressure_label
                        base_columns["chain_context"] = _chain_context(temp_source, pressure_branch)
                        base_columns["branch_formula"] = branch_formula
                        base_columns["r0_model"] = config.default_r0_model
                        base_columns["R0_T_from_mean"] = r0_at_mean
                        base_columns["A_from_mean"] = a_from_mean
                        base_columns["source_pair_label"] = f"{_short_ratio_label(ratio_in_source)}/{_short_ratio_label(r0_fit_source)}"
                        base_columns["source_pair_kind"] = "matched" if ratio_in_source == r0_fit_source else "mixed"
                        base_columns["pressure_branch_label"] = (
                            "alt_divide_only" if pressure_branch.startswith("alt_divide_only") else pressure_branch
                        )

                        samplewise_df = base_columns.copy()
                        samplewise_df["order_mode"] = "samplewise_log_first"
                        samplewise_df["A_mean"] = samplewise_df["A_samplewise_mean"]
                        samplewise_df["A_std"] = samplewise_df["A_samplewise_std"]

                        meanfirst_df = base_columns.copy()
                        meanfirst_df["order_mode"] = "mean_first_log"
                        meanfirst_df["A_mean"] = meanfirst_df["A_from_mean"]
                        meanfirst_df["A_std"] = 0.0

                        rows.extend(samplewise_df.to_dict(orient="records"))
                        rows.extend(meanfirst_df.to_dict(orient="records"))

    return pd.DataFrame(rows)


def _load_compare_frame(
    point_raw: pd.DataFrame,
    old_df: pd.DataFrame,
    best_predictions: pd.DataFrame,
    selection: pd.DataFrame,
    scenario_meta: dict[str, Any],
) -> pd.DataFrame:
    compare = point_raw.copy()
    compare = compare.merge(
        old_df[["analyzer", "point_row", "point_title", "old_prediction_ppm", "old_residual_ppm", "old_ratio_value"]],
        on=["analyzer", "point_row", "point_title"],
        how="left",
    )
    selected = best_predictions.rename(columns={"analyzer_id": "analyzer"}).copy()
    compare = compare.merge(
        selected[
            [
                "analyzer",
                "point_row",
                "point_title",
                "A_mean",
                "A_std",
                "selected_pred_ppm",
                "selected_error_ppm",
                "best_absorbance_model",
                "best_absorbance_model_label",
                "selection_reason",
                "selected_prediction_scope",
                "composite_score",
            ]
        ],
        on=["analyzer", "point_row", "point_title"],
        how="left",
    )
    compare["old_pred_ppm"] = compare["old_prediction_ppm"]
    compare["old_error"] = compare["old_pred_ppm"] - compare["target_co2_ppm"]
    compare["new_pred_ppm"] = compare["selected_pred_ppm"]
    compare["new_error"] = compare["selected_error_ppm"].combine_first(compare["new_pred_ppm"] - compare["target_co2_ppm"])
    for key, value in scenario_meta.items():
        compare[key] = value
    return compare.rename(
        columns={
            "analyzer": "analyzer_id",
            "temp_set_c": "temp_c",
            "target_co2_ppm": "target_ppm",
        }
    )


def _summarize_compare_frame(compare: pd.DataFrame) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for analyzer_id, subset in compare.groupby("analyzer_id"):
        old_overall = _metrics(subset["old_error"])
        new_overall = _metrics(subset["new_error"])
        zero_subset = subset[subset["target_ppm"] == 0].copy()
        old_zero = _metrics(zero_subset["old_error"])
        new_zero = _metrics(zero_subset["new_error"])
        high_subset = subset[(subset["target_ppm"] > 800.0) & (subset["target_ppm"] <= 1000.0)].copy()
        old_high = _metrics(high_subset["old_error"])
        new_high = _metrics(high_subset["new_error"])
        row = {
            "analyzer_id": analyzer_id,
            "old_chain_rmse": old_overall["rmse"],
            "new_chain_rmse": new_overall["rmse"],
            "old_chain_mae": old_overall["mae"],
            "new_chain_mae": new_overall["mae"],
            "old_chain_max_abs_error": old_overall["max_abs_error"],
            "new_chain_max_abs_error": new_overall["max_abs_error"],
            "old_chain_bias": old_overall["bias"],
            "new_chain_bias": new_overall["bias"],
            "old_zero_rmse": old_zero["rmse"],
            "new_zero_rmse": new_zero["rmse"],
            "old_zero_bias": old_zero["bias"],
            "new_zero_bias": new_zero["bias"],
            "old_temp_bias_spread": _temp_bias_spread(subset, "old_error"),
            "new_temp_bias_spread": _temp_bias_spread(subset, "new_error"),
            "old_high_rmse": old_high["rmse"],
            "new_high_rmse": new_high["rmse"],
            "winner_overall": _winner(old_overall["rmse"], new_overall["rmse"]),
            "winner_zero": _winner(old_zero["rmse"], new_zero["rmse"]),
            "winner_temp_stability": _winner(
                _temp_bias_spread(subset, "old_error"),
                _temp_bias_spread(subset, "new_error"),
            ),
            "winner_high_concentration": _winner(old_high["rmse"], new_high["rmse"]),
        }
        for column in (
            "order_mode",
            "ratio_in_source",
            "r0_fit_source",
            "source_pair_label",
            "source_pair_kind",
            "temp_source",
            "pressure_branch",
            "pressure_branch_label",
            "pressure_source_used",
            "chain_context",
        ):
            if column in subset.columns:
                row[column] = subset[column].iloc[0]
        for column in (
            "best_absorbance_model",
            "best_absorbance_model_label",
            "selection_reason",
            "selected_prediction_scope",
            "composite_score",
        ):
            if column in subset.columns:
                row[column] = subset[column].dropna().iloc[0] if subset[column].notna().any() else ""
        rows.append(row)
    return rows


def evaluate_scenario_groups(
    branch_points: pd.DataFrame,
    scenario_columns: list[str],
    config: Any,
    point_raw: pd.DataFrame,
    old_df: pd.DataFrame,
) -> pd.DataFrame:
    """Fit best absorbance models across diagnostic scenario groups."""

    rows: list[dict[str, Any]] = []
    for scenario_key, subset in branch_points.groupby(scenario_columns, dropna=False):
        scenario_values = scenario_key if isinstance(scenario_key, tuple) else (scenario_key,)
        scenario_meta = dict(zip(scenario_columns, scenario_values, strict=False))
        model_results = evaluate_absorbance_models(subset, config, absorbance_column="A_mean")
        if model_results["best_predictions"].empty:
            continue
        compare = _load_compare_frame(point_raw, old_df, model_results["best_predictions"], model_results["selection"], scenario_meta)
        rows.extend(_summarize_compare_frame(compare))
    return pd.DataFrame(rows)


def _selected_source_subset(branch_points: pd.DataFrame, selected_source_map: dict[str, str] | None) -> pd.DataFrame:
    if not selected_source_map:
        return branch_points.copy()
    subset = branch_points.copy()
    subset["selected_source_pair"] = subset["analyzer"].map(selected_source_map)
    return subset[subset["selected_source_pair"] == subset["source_pair_label"]].copy()


def build_order_compare(
    branch_points: pd.DataFrame,
    config: Any,
    point_raw: pd.DataFrame,
    old_df: pd.DataFrame,
    selected_source_map: dict[str, str] | None = None,
) -> pd.DataFrame:
    subset = _selected_source_subset(branch_points, selected_source_map)
    subset = subset[
        (subset["source_pair_kind"] == "matched")
        & (subset["temp_source"] == config.default_temp_source)
        & (subset["pressure_branch"] == config.default_pressure_branch_label())
    ].copy()
    summary = evaluate_scenario_groups(subset, ["order_mode"], config, point_raw, old_df)
    if summary.empty:
        return summary
    summary["samplewise_log_first_is_better"] = False
    summary["significant_order_gain"] = False
    summary["improvement_vs_meanfirst_rmse"] = 0.0
    summary["improvement_vs_meanfirst_pct"] = 0.0
    summary["recommended_default_order"] = ""
    for analyzer_id, analyzer_df in summary.groupby("analyzer_id"):
        sample = analyzer_df[analyzer_df["order_mode"] == "samplewise_log_first"]
        mean = analyzer_df[analyzer_df["order_mode"] == "mean_first_log"]
        if sample.empty or mean.empty:
            continue
        sample_rmse = float(sample.iloc[0]["new_chain_rmse"])
        mean_rmse = float(mean.iloc[0]["new_chain_rmse"])
        improvement = mean_rmse - sample_rmse
        improvement_pct = (improvement / mean_rmse * 100.0) if math.isfinite(mean_rmse) and mean_rmse else 0.0
        significant = improvement > max(0.5, mean_rmse * 0.05)
        mask = summary["analyzer_id"] == analyzer_id
        summary.loc[mask, "samplewise_log_first_is_better"] = sample_rmse < mean_rmse
        summary.loc[mask, "significant_order_gain"] = significant
        summary.loc[mask, "improvement_vs_meanfirst_rmse"] = improvement
        summary.loc[mask, "improvement_vs_meanfirst_pct"] = improvement_pct
        summary.loc[mask, "recommended_default_order"] = (
            "samplewise_log_first" if sample_rmse <= mean_rmse else "mean_first_log"
        )
    return summary.drop_duplicates(subset=["analyzer_id", "order_mode"]).sort_values(["analyzer_id", "order_mode"], ignore_index=True)


def build_r0_source_consistency_compare(
    branch_points: pd.DataFrame,
    config: Any,
    point_raw: pd.DataFrame,
    old_df: pd.DataFrame,
) -> pd.DataFrame:
    subset = branch_points[
        (branch_points["order_mode"] == "samplewise_log_first")
        & (branch_points["temp_source"] == config.default_temp_source)
        & (branch_points["pressure_branch"] == config.default_pressure_branch_label())
    ].copy()
    summary = evaluate_scenario_groups(subset, ["ratio_in_source", "r0_fit_source", "source_pair_label", "source_pair_kind"], config, point_raw, old_df)
    if summary.empty:
        return summary
    summary["mixed_source_invalid_for_production_default"] = False
    summary["recommended_default_source_pair"] = ""
    for analyzer_id, analyzer_df in summary.groupby("analyzer_id"):
        matched_best = analyzer_df[analyzer_df["source_pair_kind"] == "matched"]["new_chain_rmse"].min()
        mixed_best = analyzer_df[analyzer_df["source_pair_kind"] == "mixed"]["new_chain_rmse"].min()
        invalid = math.isfinite(matched_best) and math.isfinite(mixed_best) and mixed_best > matched_best * 1.05
        recommended_row = analyzer_df[analyzer_df["source_pair_kind"] == "matched"].sort_values("new_chain_rmse").head(1)
        recommended = recommended_row.iloc[0]["source_pair_label"] if not recommended_row.empty else ""
        mask = summary["analyzer_id"] == analyzer_id
        summary.loc[mask, "mixed_source_invalid_for_production_default"] = invalid
        summary.loc[mask, "recommended_default_source_pair"] = recommended
    return summary.sort_values(["analyzer_id", "source_pair_label"], ignore_index=True)


def build_pressure_branch_compare(
    branch_points: pd.DataFrame,
    config: Any,
    point_raw: pd.DataFrame,
    old_df: pd.DataFrame,
    selected_source_map: dict[str, str] | None = None,
) -> pd.DataFrame:
    alt_branch = config.default_alt_pressure_branch_label()
    subset = _selected_source_subset(branch_points, selected_source_map)
    subset = subset[
        (subset["order_mode"] == "samplewise_log_first")
        & (subset["source_pair_kind"] == "matched")
        & (subset["temp_source"] == config.default_temp_source)
        & (subset["pressure_branch"].isin(["no_pressure_norm", "pressure_std", "pressure_corr", alt_branch]))
    ].copy()
    summary = evaluate_scenario_groups(subset, ["pressure_branch", "pressure_branch_label", "pressure_source_used"], config, point_raw, old_df)
    if summary.empty:
        return summary
    summary["pressure_branch_report"] = summary["pressure_branch_label"]
    summary["branch_rank"] = 0
    summary["recommended_pressure_branch"] = ""
    summary["pressure_branch_note"] = ""
    for analyzer_id, analyzer_df in summary.groupby("analyzer_id"):
        ordered = analyzer_df.sort_values("new_chain_rmse").reset_index()
        for rank, idx in enumerate(ordered["index"].tolist(), start=1):
            summary.loc[idx, "branch_rank"] = rank
        summary.loc[summary["analyzer_id"] == analyzer_id, "recommended_pressure_branch"] = ordered.iloc[0]["pressure_branch"]
        note = ""
        if ordered.iloc[0]["pressure_branch"] == "no_pressure_norm":
            note = "no_pressure_norm is the current diagnostic best branch on this run."
        elif ordered.iloc[0]["pressure_branch"] == "pressure_std":
            note = "pressure_std is steadier than pressure_corr on this run."
        elif ordered.iloc[0]["pressure_branch"] == "pressure_corr":
            note = "pressure_corr remains the best deployable pressure branch on this run."
        summary.loc[summary["analyzer_id"] == analyzer_id, "pressure_branch_note"] = note
    return summary.sort_values(["analyzer_id", "branch_rank"], ignore_index=True)


def build_upper_bound_vs_deployable_compare(
    branch_points: pd.DataFrame,
    config: Any,
    point_raw: pd.DataFrame,
    old_df: pd.DataFrame,
    selected_source_map: dict[str, str] | None = None,
) -> pd.DataFrame:
    subset = _selected_source_subset(branch_points, selected_source_map)
    subset = subset[
        (subset["order_mode"] == "samplewise_log_first")
        & (subset["source_pair_kind"] == "matched")
        & (subset["chain_context"].isin(["physics_upper_bound", "deployable_chain"]))
    ].copy()
    summary = evaluate_scenario_groups(subset, ["chain_context", "temp_source", "pressure_branch", "pressure_source_used"], config, point_raw, old_df)
    if summary.empty:
        return summary
    summary["best_model_upper_bound"] = ""
    summary["best_model_deployable"] = ""
    summary["best_model_consistent"] = False
    summary["upper_bound_beats_deployable"] = False
    summary["context_diagnosis"] = ""
    for analyzer_id, analyzer_df in summary.groupby("analyzer_id"):
        upper = analyzer_df[analyzer_df["chain_context"] == "physics_upper_bound"]
        deploy = analyzer_df[analyzer_df["chain_context"] == "deployable_chain"]
        upper_model = upper.iloc[0]["best_absorbance_model"] if not upper.empty else ""
        deploy_model = deploy.iloc[0]["best_absorbance_model"] if not deploy.empty else ""
        consistent = bool(upper_model and deploy_model and upper_model == deploy_model)
        upper_rmse = float(upper.iloc[0]["new_chain_rmse"]) if not upper.empty else math.nan
        deploy_rmse = float(deploy.iloc[0]["new_chain_rmse"]) if not deploy.empty else math.nan
        upper_better = math.isfinite(upper_rmse) and math.isfinite(deploy_rmse) and upper_rmse + 0.5 < deploy_rmse
        mask = summary["analyzer_id"] == analyzer_id
        summary.loc[mask, "best_model_upper_bound"] = upper_model
        summary.loc[mask, "best_model_deployable"] = deploy_model
        summary.loc[mask, "best_model_consistent"] = consistent
        summary.loc[mask, "upper_bound_beats_deployable"] = upper_better
        diagnosis = (
            "deployable_temperature_pressure_gap"
            if upper_better
            else "absorbance_ppm_model_gap"
            if math.isfinite(upper_rmse) and math.isfinite(float(upper.iloc[0]['old_chain_rmse'])) and upper_rmse > float(upper.iloc[0]["old_chain_rmse"])
            else "no_clear_context_gap"
        )
        summary.loc[mask, "context_diagnosis"] = diagnosis
    return summary.sort_values(["analyzer_id", "chain_context"], ignore_index=True)


def build_root_cause_ranking(
    order_compare: pd.DataFrame,
    source_compare: pd.DataFrame,
    pressure_compare: pd.DataFrame,
    context_compare: pd.DataFrame,
    validation_table: pd.DataFrame,
) -> tuple[pd.DataFrame, list[str], str]:
    """Rank the main reasons the new chain currently loses."""

    def avg(df: pd.DataFrame, mask: pd.Series, column: str) -> float:
        values = pd.to_numeric(df.loc[mask, column], errors="coerce").dropna()
        return float(values.mean()) if not values.empty else math.nan

    avg_sample = avg(order_compare, order_compare["order_mode"] == "samplewise_log_first", "new_chain_rmse") if not order_compare.empty else math.nan
    avg_mean = avg(order_compare, order_compare["order_mode"] == "mean_first_log", "new_chain_rmse") if not order_compare.empty else math.nan
    avg_matched = avg(source_compare, source_compare["source_pair_kind"] == "matched", "new_chain_rmse") if not source_compare.empty else math.nan
    avg_mixed = avg(source_compare, source_compare["source_pair_kind"] == "mixed", "new_chain_rmse") if not source_compare.empty else math.nan
    avg_no_pressure = avg(pressure_compare, pressure_compare["pressure_branch"] == "no_pressure_norm", "new_chain_rmse") if not pressure_compare.empty else math.nan
    avg_pressure_norm = avg(
        pressure_compare,
        pressure_compare["pressure_branch"].isin(["pressure_std", "pressure_corr"]),
        "new_chain_rmse",
    ) if not pressure_compare.empty else math.nan
    avg_upper_new = avg(context_compare, context_compare["chain_context"] == "physics_upper_bound", "new_chain_rmse") if not context_compare.empty else math.nan
    avg_upper_old = avg(context_compare, context_compare["chain_context"] == "physics_upper_bound", "old_chain_rmse") if not context_compare.empty else math.nan
    avg_deploy_new = avg(context_compare, context_compare["chain_context"] == "deployable_chain", "new_chain_rmse") if not context_compare.empty else math.nan
    avg_deploy_old = avg(context_compare, context_compare["chain_context"] == "deployable_chain", "old_chain_rmse") if not context_compare.empty else math.nan

    missing_40_zero = False
    if not validation_table.empty:
        match = validation_table[validation_table["check_name"] == "identified_missing_40c_zero_ppm"]
        if not match.empty:
            missing_40_zero = bool(match.iloc[0]["passed"])

    issues: list[dict[str, Any]] = []

    def add_issue(issue_name: str, severity_score: float, evidence: str, recommended_action: str) -> None:
        if severity_score >= 6.0:
            severity = "high"
        elif severity_score >= 2.0:
            severity = "medium"
        elif severity_score > 0.0:
            severity = "low"
        else:
            severity = "not_detected"
        issues.append(
            {
                "issue_name": issue_name,
                "_severity_score": severity_score,
                "severity": severity,
                "evidence": evidence,
                "recommended_action": recommended_action,
            }
        )

    weak_model_gap = max((avg_upper_new - avg_upper_old), 0.0) if math.isfinite(avg_upper_new) and math.isfinite(avg_upper_old) else 0.0
    add_issue(
        "weak_absorbance_ppm_model",
        weak_model_gap,
        (
            f"physics_upper_bound avg RMSE old={avg_upper_old:.3f}, new={avg_upper_new:.3f}."
            if math.isfinite(avg_upper_new) and math.isfinite(avg_upper_old)
            else "physics_upper_bound comparison did not produce enough data."
        ),
        "Strengthen the absorbance ppm calibration model first, then validate on multiple independent runs.",
    )

    order_gap = max((avg_mean - avg_sample), 0.0) if math.isfinite(avg_mean) and math.isfinite(avg_sample) else 0.0
    add_issue(
        "mean_before_log_order_issue",
        order_gap,
        (
            f"avg new-chain RMSE samplewise={avg_sample:.3f}, mean-first={avg_mean:.3f}."
            if math.isfinite(avg_mean) and math.isfinite(avg_sample)
            else "order-mode comparison did not produce enough data."
        ),
        "Keep samplewise_log_first as the default implementation if it remains consistently lower-error.",
    )

    source_gap = max((avg_mixed - avg_matched), 0.0) if math.isfinite(avg_mixed) and math.isfinite(avg_matched) else 0.0
    add_issue(
        "r0_source_mismatch",
        source_gap,
        (
            f"avg new-chain RMSE matched={avg_matched:.3f}, mixed={avg_mixed:.3f}."
            if math.isfinite(avg_mixed) and math.isfinite(avg_matched)
            else "source-consistency comparison did not produce enough data."
        ),
        "Do not allow mixed ratio/R0 source pairs as the production default if matched pairs remain stronger.",
    )

    pressure_gap = max((avg_pressure_norm - avg_no_pressure), 0.0) if math.isfinite(avg_pressure_norm) and math.isfinite(avg_no_pressure) else 0.0
    add_issue(
        "pressure_norm_not_helpful_on_current_run",
        pressure_gap,
        (
            f"avg new-chain RMSE no-pressure={avg_no_pressure:.3f}, pressure-norm={avg_pressure_norm:.3f}."
            if math.isfinite(avg_pressure_norm) and math.isfinite(avg_no_pressure)
            else "pressure-branch comparison did not produce enough data."
        ),
        "Keep no_pressure_norm only as a diagnostic branch on this run if it is steadier; do not generalize that result to multi-pressure campaigns.",
    )

    add_issue(
        "incomplete_r0_anchor_at_40C",
        3.0 if missing_40_zero else 0.0,
        "40 C has no 0 ppm anchor in this run, so R0(T) must extrapolate there." if missing_40_zero else "40 C zero anchor is present.",
        "Add a 40 C / 0 ppm CO2 point before drawing strong conclusions about high-temperature absorbance behavior.",
    )

    deploy_gap = max((avg_deploy_new - avg_upper_new), 0.0) if math.isfinite(avg_deploy_new) and math.isfinite(avg_upper_new) else 0.0
    add_issue(
        "deployable_temperature_pressure_gap",
        deploy_gap,
        (
            f"avg new-chain RMSE upper_bound={avg_upper_new:.3f}, deployable={avg_deploy_new:.3f}."
            if math.isfinite(avg_deploy_new) and math.isfinite(avg_upper_new)
            else "upper-bound vs deployable comparison did not produce enough data."
        ),
        "Prioritize temperature/pressure deployment corrections if the physics upper bound is meaningfully better than the deployable chain.",
    )

    add_issue(
        "base_final_not_primary_bottleneck",
        0.05,
        "The main old-vs-new comparison is driven by static point fits; base/final remains a secondary diagnostic branch here.",
        "Do not spend the next cycle on FIR or continuous smoothing before the static absorbance model and branch definitions are fixed.",
    )

    ranking = pd.DataFrame(issues).sort_values(["_severity_score", "issue_name"], ascending=[False, True], ignore_index=True)
    ranking["rank"] = np.arange(1, len(ranking) + 1, dtype=int)
    ranking = ranking[["rank", "issue_name", "severity", "evidence", "recommended_action", "_severity_score"]]

    top_detected = ranking[ranking["severity"] != "not_detected"].head(3)
    top_lines = [
        f"Top {idx + 1}: {row.issue_name} ({row.severity}) - {row.evidence}"
        for idx, row in enumerate(top_detected.itertuples(index=False))
    ]
    if top_detected.empty:
        top_lines.append("No high-confidence root cause stood out from this single run.")

    implementation_issue = "No clear active implementation bug was detected; the main gaps are branch quality and model strength."
    mean_issue = ranking[ranking["issue_name"] == "mean_before_log_order_issue"]
    source_issue = ranking[ranking["issue_name"] == "r0_source_mismatch"]
    if not mean_issue.empty and mean_issue.iloc[0]["severity"] in {"high", "medium"}:
        implementation_issue = "A meaningful order-of-operations issue was detected: samplewise_log_first should remain the default path."
    elif not source_issue.empty and source_issue.iloc[0]["severity"] in {"high", "medium"}:
        implementation_issue = "A meaningful source-consistency issue was detected: mixed ratio/R0 source pairs should not be used as a default."

    return ranking.drop(columns="_severity_score"), top_lines, implementation_issue


def _best_variant_rows(frame: pd.DataFrame, analyzer_id: str, group_column: str) -> pd.DataFrame:
    if frame.empty or group_column not in frame.columns:
        return pd.DataFrame()
    subset = frame[frame["analyzer_id"] == analyzer_id].copy()
    if subset.empty:
        return subset
    rows: list[pd.Series] = []
    for _, variant_df in subset.groupby(group_column, dropna=False):
        rows.append(
            variant_df.sort_values(
                ["composite_score", "validation_rmse", "overall_rmse", "model_id"],
                ignore_index=True,
            ).iloc[0]
        )
    return pd.DataFrame(rows)


def build_ga01_residual_profile(
    point_reconciliation: pd.DataFrame,
    model_results: dict[str, pd.DataFrame],
) -> tuple[pd.DataFrame, str]:
    """Build a focused GA01 residual profile and a short diagnosis note."""

    analyzer_id = "GA01"
    points = point_reconciliation[point_reconciliation["analyzer_id"] == analyzer_id].copy()
    if points.empty:
        return pd.DataFrame(), "GA01 data was not available in the current run."

    rows: list[dict[str, Any]] = []
    for temp_c, subset in points.groupby("temp_c", dropna=False):
        old_metrics = _metrics(subset["old_error"])
        new_metrics = _metrics(subset["new_error"])
        rows.append(
            {
                "profile_section": "by_temperature",
                "analyzer_id": analyzer_id,
                "temp_c": temp_c,
                "sample_count": int(len(subset)),
                "old_rmse": old_metrics["rmse"],
                "new_rmse": new_metrics["rmse"],
                "old_bias": old_metrics["bias"],
                "new_bias": new_metrics["bias"],
                "target_ppm": math.nan,
                "variant_label": "",
                "old_chain_rmse": old_metrics["rmse"],
                "new_chain_rmse": new_metrics["rmse"],
            }
        )
    for target_ppm, subset in points.groupby("target_ppm", dropna=False):
        old_metrics = _metrics(subset["old_error"])
        new_metrics = _metrics(subset["new_error"])
        rows.append(
            {
                "profile_section": "by_target_ppm",
                "analyzer_id": analyzer_id,
                "temp_c": math.nan,
                "sample_count": int(len(subset)),
                "old_rmse": old_metrics["rmse"],
                "new_rmse": new_metrics["rmse"],
                "old_bias": old_metrics["bias"],
                "new_bias": new_metrics["bias"],
                "target_ppm": target_ppm,
                "variant_label": "",
                "old_chain_rmse": old_metrics["rmse"],
                "new_chain_rmse": new_metrics["rmse"],
            }
        )

    zero_points = points[points["target_ppm"] == 0].copy()
    for temp_c, subset in zero_points.groupby("temp_c", dropna=False):
        old_metrics = _metrics(subset["old_error"])
        new_metrics = _metrics(subset["new_error"])
        rows.append(
            {
                "profile_section": "zero_bias",
                "analyzer_id": analyzer_id,
                "temp_c": temp_c,
                "sample_count": int(len(subset)),
                "old_rmse": old_metrics["rmse"],
                "new_rmse": new_metrics["rmse"],
                "old_bias": old_metrics["bias"],
                "new_bias": new_metrics["bias"],
                "target_ppm": 0.0,
                "variant_label": f"zero@{temp_c:g}C",
                "old_chain_rmse": old_metrics["rmse"],
                "new_chain_rmse": new_metrics["rmse"],
            }
        )

    score_table = model_results.get("scores", pd.DataFrame()).copy()
    overall_old_rmse = _metrics(points["old_error"])["rmse"]
    for section_name, group_column in (
        ("source_pair_compare", "selected_source_pair"),
        ("zero_residual_compare", "zero_residual_mode"),
        ("model_family_compare", "model_family"),
    ):
        summary = _best_variant_rows(score_table, analyzer_id, group_column)
        for _, row in summary.iterrows():
            rows.append(
                {
                    "profile_section": section_name,
                    "analyzer_id": analyzer_id,
                    "temp_c": math.nan,
                    "sample_count": int(row.get("sample_count", 0)),
                    "old_rmse": overall_old_rmse,
                    "new_rmse": float(row.get("overall_rmse", math.nan)),
                    "old_bias": math.nan,
                    "new_bias": float(row.get("overall_bias", math.nan)),
                    "target_ppm": math.nan,
                    "variant_label": str(row.get(group_column, "n/a")),
                    "old_chain_rmse": overall_old_rmse,
                    "new_chain_rmse": float(row.get("overall_rmse", math.nan)),
                    "zero_rmse": float(row.get("zero_rmse", math.nan)),
                    "temp_bias_spread": float(row.get("temp_bias_spread", math.nan)),
                    "selected_model_id": str(row.get("model_id", "")),
                }
            )

    zero_compare = _best_variant_rows(score_table, analyzer_id, "zero_residual_mode")
    family_compare = _best_variant_rows(score_table, analyzer_id, "model_family")
    source_compare = _best_variant_rows(score_table, analyzer_id, "selected_source_pair")
    zero_gain = 0.0
    source_gain = 0.0
    family_gain = 0.0
    if not zero_compare.empty and (zero_compare["zero_residual_mode"] == "none").any():
        none_row = zero_compare[zero_compare["zero_residual_mode"] == "none"].iloc[0]
        best_corr = zero_compare.sort_values("overall_rmse").iloc[0]
        zero_gain = float(none_row["overall_rmse"]) - float(best_corr["overall_rmse"])
    if not source_compare.empty:
        source_gain = float(source_compare["overall_rmse"].max()) - float(source_compare["overall_rmse"].min())
    if not family_compare.empty and {"single_range", "piecewise_range"} <= set(family_compare["model_family"].astype(str)):
        single_row = family_compare[family_compare["model_family"] == "single_range"].iloc[0]
        piecewise_row = family_compare[family_compare["model_family"] == "piecewise_range"].iloc[0]
        family_gain = float(single_row["overall_rmse"]) - float(piecewise_row["overall_rmse"])

    high_temp_zero_bias = 0.0
    if not zero_points.empty:
        hottest_zero = zero_points.sort_values("temp_c").tail(1)
        if not hottest_zero.empty:
            high_temp_zero_bias = abs(float(hottest_zero.iloc[0]["new_error"]))

    if high_temp_zero_bias > 5.0 and zero_gain >= max(source_gain, family_gain):
        primary_issue = "high_temp_zero_anchor_missing"
        diagnosis_note = (
            "GA01 is still dominated by high-temperature zero anchoring risk: the hottest 0 ppm residual remains large, "
            "and ΔA0(T) helps more than source switching or family switching."
        )
    elif family_gain > max(0.5, source_gain, zero_gain):
        primary_issue = "low_range_model_gap"
        diagnosis_note = (
            "GA01 looks more like a low-range model problem: the piecewise family improves more than the source-pair or ΔA0(T) variants."
        )
    elif source_gain > max(0.5, family_gain, zero_gain):
        primary_issue = "source_selection_gap"
        diagnosis_note = (
            "GA01 looks more like a source-pair issue: raw/raw vs filt/filt changes matter more than the zero-correction and model-family changes."
        )
    else:
        primary_issue = "other_or_mixed"
        diagnosis_note = (
            "GA01 remains mixed: high-temperature zero anchoring, low-range model fit, and source choice all contribute, "
            "with no single dominant fix on this run."
        )

    rows.append(
        {
            "profile_section": "diagnosis",
            "analyzer_id": analyzer_id,
            "temp_c": math.nan,
            "sample_count": int(len(points)),
            "old_rmse": overall_old_rmse,
            "new_rmse": _metrics(points["new_error"])["rmse"],
            "old_bias": _metrics(points["old_error"])["bias"],
            "new_bias": _metrics(points["new_error"])["bias"],
            "target_ppm": math.nan,
            "variant_label": "",
            "old_chain_rmse": overall_old_rmse,
            "new_chain_rmse": _metrics(points["new_error"])["rmse"],
            "primary_issue": primary_issue,
            "diagnosis_note": diagnosis_note,
        }
    )

    return pd.DataFrame(rows), diagnosis_note
