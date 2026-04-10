"""Diagnostic-only fixed-chain ppm family challenge."""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd

from .absorbance_models import AbsorbanceModelSpec, _fit_one_candidate, active_model_specs
from .diagnostics import build_analyzer_mode2_summary

LOW_RANGE_NAME = "0~200 ppm"
MAIN_RANGE_NAME = "200~1000 ppm"


@dataclass(frozen=True)
class PpmFamilyChallengeSpec:
    """One fixed-chain diagnostic ppm family."""

    ppm_family_mode: str
    model_id: str
    model_label: str
    formula: str
    terms: tuple[str, ...]
    uses_humidity_cross_terms: bool

    def to_absorbance_spec(self) -> AbsorbanceModelSpec:
        return AbsorbanceModelSpec(
            model_id=self.model_id,
            model_label=self.model_label,
            formula=self.formula,
            terms=self.terms,
            model_family="fixed_family_challenge",
        )


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


def _temp_bias_spread(frame: pd.DataFrame, error_column: str) -> float:
    grouped = (
        frame.dropna(subset=["temp_c", error_column])
        .groupby("temp_c", dropna=False)[error_column]
        .mean()
    )
    if grouped.empty:
        return math.nan
    return float(np.std(grouped.to_numpy(dtype=float)))


def _range_mask(series: pd.Series, lower: float | None, upper: float | None) -> pd.Series:
    numeric = pd.to_numeric(series, errors="coerce")
    if lower == 0.0 and upper == 0.0:
        return numeric == 0.0
    mask = pd.Series(True, index=series.index)
    if lower is not None:
        mask &= numeric > lower
    if upper is not None:
        mask &= numeric <= upper
    return mask


def _clip_ratio(value: Any) -> float:
    numeric = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(numeric):
        return math.nan
    return float(np.clip(float(numeric), 0.0, 1.0))


def _first_text(frame: pd.DataFrame, column_name: str) -> str:
    if column_name not in frame.columns:
        return ""
    values = frame[column_name].dropna()
    if values.empty:
        return ""
    return str(values.iloc[0])


def _selected_legacy_none_points(
    legacy_feature_frame: pd.DataFrame,
    fixed_selection: pd.DataFrame,
) -> pd.DataFrame:
    if legacy_feature_frame.empty or fixed_selection.empty:
        return pd.DataFrame()
    fixed = fixed_selection[
        [
            "analyzer_id",
            "selected_ratio_source",
            "selected_source_pair",
            "best_absorbance_model",
            "best_model_family",
            "zero_residual_mode",
            "selected_prediction_scope",
            "absorbance_column",
        ]
    ].copy()
    fixed = fixed.rename(
        columns={
            "analyzer_id": "analyzer",
            "selected_ratio_source": "fixed_ratio_source",
            "best_absorbance_model": "fixed_best_model",
            "best_model_family": "fixed_model_family",
            "zero_residual_mode": "fixed_zero_residual_mode",
            "selected_prediction_scope": "fixed_prediction_scope",
            "absorbance_column": "fixed_absorbance_proxy",
        }
    )
    merged = legacy_feature_frame[legacy_feature_frame["water_lineage_mode"] == "none"].copy()
    merged = merged.merge(fixed, on="analyzer", how="inner")
    mask = (
        (merged["ratio_source"] == merged["fixed_ratio_source"])
        & (merged["zero_residual_mode"] == merged["fixed_zero_residual_mode"])
    )
    return merged.loc[mask].copy()


def _active_spec_lookup(config: Any) -> dict[str, AbsorbanceModelSpec]:
    return {spec.model_id: spec for spec in active_model_specs(config)}


def _choose_humidity_proxy(
    frame: pd.DataFrame,
    *,
    selected_source_pair: str,
    legacy_safe: bool,
    baseline_bearing: bool,
) -> tuple[pd.Series, str]:
    raw = pd.to_numeric(frame.get("h2o_ratio_raw_mean"), errors="coerce")
    filt = pd.to_numeric(frame.get("h2o_ratio_filt_mean"), errors="coerce")
    if baseline_bearing and filt.notna().any():
        return filt, "baseline_bearing_h2o_proxy:ratio_h2o_filt_mean"
    if selected_source_pair == "raw/raw" and legacy_safe and raw.notna().any():
        return raw, "ratio_h2o_raw_mean"
    if filt.notna().any():
        return filt, "ratio_h2o_filt_mean"
    return raw, "ratio_h2o_raw_mean_fallback"


def _choose_humidity_delta(frame: pd.DataFrame) -> tuple[pd.Series, str]:
    for column_name in (
        "delta_h2o_ratio_vs_legacy_summary_anchor",
        "delta_h2o_ratio_vs_legacy_zero_ppm_anchor",
        "delta_h2o_ratio_vs_subzero_anchor",
        "delta_h2o_ratio_vs_zeroC_anchor",
    ):
        values = pd.to_numeric(frame.get(column_name), errors="coerce")
        if values.notna().any():
            return values, column_name
    return pd.to_numeric(frame.get("water_ratio_mean"), errors="coerce"), "water_ratio_mean_fallback"


def _family_specs_for_analyzer(
    fixed_model_id: str,
    active_lookup: dict[str, AbsorbanceModelSpec],
) -> list[PpmFamilyChallengeSpec]:
    current_spec = active_lookup.get(str(fixed_model_id))
    if current_spec is None:
        current_spec = AbsorbanceModelSpec(
            model_id=str(fixed_model_id or "current_fixed_model"),
            model_label=str(fixed_model_id or "Current fixed family"),
            formula="ppm = current fixed-chain family",
            terms=("intercept", "A"),
        )
    return [
        PpmFamilyChallengeSpec(
            ppm_family_mode="current_fixed_family",
            model_id=current_spec.model_id,
            model_label=f"Current fixed family: {current_spec.model_label}",
            formula=current_spec.formula,
            terms=current_spec.terms,
            uses_humidity_cross_terms=False,
        ),
        PpmFamilyChallengeSpec(
            ppm_family_mode="v5_abs_k_minimal",
            model_id="v5_abs_k_minimal",
            model_label="V5 Abs+K minimal",
            formula="ppm = a0 + a1*A + a2*A^2 + a3*A^3 + a4*K + a5*K^2 + a6*A*K",
            terms=("intercept", "A", "A^2", "A^3", "K", "K^2", "A*K"),
            uses_humidity_cross_terms=True,
        ),
        PpmFamilyChallengeSpec(
            ppm_family_mode="legacy_humidity_cross_D",
            model_id="legacy_humidity_cross_D",
            model_label="Legacy humidity cross D",
            formula="ppm = d0 + d1*A + d2*A^2 + d3*R + d4*K + d5*P + d6*H + d7*H^2",
            terms=("intercept", "A", "A^2", "R", "K", "P", "H", "H^2"),
            uses_humidity_cross_terms=True,
        ),
        PpmFamilyChallengeSpec(
            ppm_family_mode="legacy_humidity_cross_E",
            model_id="legacy_humidity_cross_E",
            model_label="Legacy humidity cross E",
            formula="ppm = e0 + e1*A + e2*A^2 + e3*A^3 + e4*R + e5*R^2 + e6*K + e7*K^2 + e8*A*K + e9*P + e10*H + e11*H^2 + e12*R*H",
            terms=("intercept", "A", "A^2", "A^3", "R", "R^2", "K", "K^2", "A*K", "P", "H", "H^2", "R*H"),
            uses_humidity_cross_terms=True,
        ),
    ]


def _prediction_tables(
    residual_rows: list[dict[str, Any]],
    requested_scope: str,
) -> tuple[pd.DataFrame, str]:
    residual_df = pd.DataFrame(residual_rows)
    if residual_df.empty:
        return pd.DataFrame(), requested_scope
    overall = residual_df[residual_df["prediction_scope"] == "overall_fit"][
        ["analyzer_id", "point_title", "point_row", "predicted_ppm", "error_ppm"]
    ].rename(columns={"predicted_ppm": "overall_pred_ppm", "error_ppm": "overall_error_ppm"})
    validation = residual_df[residual_df["prediction_scope"] == "validation_oof"][
        ["analyzer_id", "point_title", "point_row", "predicted_ppm", "error_ppm"]
    ].rename(columns={"predicted_ppm": "validation_pred_ppm", "error_ppm": "validation_error_ppm"})
    merged = overall.merge(validation, on=["analyzer_id", "point_title", "point_row"], how="left")
    actual_scope = requested_scope
    if requested_scope == "validation_oof" and merged["validation_pred_ppm"].notna().any():
        merged["selected_pred_ppm"] = merged["validation_pred_ppm"]
        merged["selected_error_ppm"] = merged["validation_error_ppm"]
    else:
        merged["selected_pred_ppm"] = merged["overall_pred_ppm"]
        merged["selected_error_ppm"] = merged["overall_error_ppm"]
        if requested_scope == "validation_oof":
            actual_scope = "overall_fit_fallback"
    return merged, actual_scope


def _detail_from_compare(
    compare: pd.DataFrame,
    *,
    analyzer_id: str,
    fixed_row: pd.Series,
    ppm_family_mode: str,
    uses_humidity_cross_terms: bool,
    humidity_feature_set: str,
    requested_scope: str,
    actual_scope: str,
    formula: str,
) -> dict[str, Any]:
    zero_subset = compare[_range_mask(compare["target_ppm"], 0.0, 0.0)].copy()
    low_subset = compare[_range_mask(compare["target_ppm"], 0.0, 200.0)].copy()
    main_subset = compare[_range_mask(compare["target_ppm"], 200.0, 1000.0)].copy()
    new_metrics = _metrics(compare["new_error"])
    old_metrics = _metrics(compare["old_error"])
    new_zero = _metrics(zero_subset["new_error"])
    old_zero = _metrics(zero_subset["old_error"])
    return {
        "analyzer_id": analyzer_id,
        "mode2_semantic_profile": str(fixed_row.get("mode2_semantic_profile") or "mode2_semantics_unknown"),
        "mode2_legacy_raw_compare_safe": bool(fixed_row.get("mode2_legacy_raw_compare_safe", False)),
        "mode2_is_baseline_bearing_profile": bool(fixed_row.get("mode2_is_baseline_bearing_profile", False)),
        "selected_source_pair": str(fixed_row.get("selected_source_pair") or ""),
        "fixed_absorbance_proxy": str(fixed_row.get("fixed_absorbance_proxy") or "A_mean"),
        "fixed_zero_residual_mode": str(fixed_row.get("fixed_zero_residual_mode") or "none"),
        "fixed_prediction_scope": requested_scope,
        "fixed_prediction_scope_used": actual_scope,
        "fixed_water_lineage_mode": "none",
        "fixed_best_model": str(fixed_row.get("fixed_best_model") or ""),
        "fixed_model_family": str(fixed_row.get("fixed_model_family") or ""),
        "ppm_family_mode": ppm_family_mode,
        "uses_humidity_cross_terms": uses_humidity_cross_terms,
        "humidity_feature_set": humidity_feature_set,
        "overall_rmse": new_metrics["rmse"],
        "zero_rmse": new_zero["rmse"],
        "low_range_rmse": _metrics(low_subset["new_error"])["rmse"],
        "main_range_rmse": _metrics(main_subset["new_error"])["rmse"],
        "temp_bias_spread": _temp_bias_spread(compare, "new_error"),
        "old_chain_overall_rmse": old_metrics["rmse"],
        "old_chain_zero_rmse": old_zero["rmse"],
        "gap_to_old_overall": new_metrics["rmse"] - old_metrics["rmse"] if pd.notna(new_metrics["rmse"]) and pd.notna(old_metrics["rmse"]) else math.nan,
        "gap_to_old_zero": new_zero["rmse"] - old_zero["rmse"] if pd.notna(new_zero["rmse"]) and pd.notna(old_zero["rmse"]) else math.nan,
        "challenge_formula": formula,
        "challenge_sample_count": int(compare["new_error"].notna().sum()),
    }


def _aggregate_laggard_gap(subset: pd.DataFrame) -> tuple[float, int]:
    laggards = subset[
        pd.to_numeric(subset["current_fixed_family_gap_to_old_overall"], errors="coerce").gt(0.0)
    ].copy()
    if laggards.empty:
        return math.nan, 0
    weights = pd.to_numeric(laggards["current_fixed_family_gap_to_old_overall"], errors="coerce")
    ratios = pd.to_numeric(laggards["gap_closed_ratio_vs_current_fixed_family_capped"], errors="coerce")
    usable = weights.notna() & ratios.notna() & (weights > 0.0)
    if not usable.any():
        return math.nan, int(laggards["analyzer_id"].nunique())
    weighted = float(np.average(ratios[usable].to_numpy(dtype=float), weights=weights[usable].to_numpy(dtype=float)))
    return weighted, int(laggards["analyzer_id"].nunique())


def _summary_rows(detail_df: pd.DataFrame) -> pd.DataFrame:
    if detail_df.empty:
        return pd.DataFrame()
    profile_cols = [
        "mode2_semantic_profile",
        "mode2_legacy_raw_compare_safe",
        "mode2_is_baseline_bearing_profile",
    ]
    copy_columns = [
        "analyzer_id",
        *profile_cols,
        "selected_source_pair",
        "fixed_absorbance_proxy",
        "fixed_zero_residual_mode",
        "fixed_prediction_scope",
        "fixed_water_lineage_mode",
        "fixed_best_model",
        "fixed_model_family",
        "ppm_family_mode",
        "uses_humidity_cross_terms",
        "humidity_feature_set",
        "overall_rmse",
        "zero_rmse",
        "low_range_rmse",
        "main_range_rmse",
        "temp_bias_spread",
        "old_chain_overall_rmse",
        "gap_to_old_overall",
        "gap_to_old_zero",
        "delta_vs_current_fixed_family_overall",
        "delta_vs_current_fixed_family_zero",
        "delta_vs_current_fixed_family_low",
        "delta_vs_current_fixed_family_main",
        "gap_closed_ratio_vs_current_fixed_family_raw",
        "gap_closed_ratio_vs_current_fixed_family_capped",
        "crossed_old_chain_flag",
        "current_fixed_family_gap_to_old_overall",
    ]
    rows: list[dict[str, Any]] = []

    for analyzer_id, subset in detail_df.groupby("analyzer_id", dropna=False):
        best = subset.sort_values(
            [
                "gap_closed_ratio_vs_current_fixed_family_capped",
                "delta_vs_current_fixed_family_overall",
                "delta_vs_current_fixed_family_zero",
                "ppm_family_mode",
            ],
            ascending=[False, False, False, True],
            na_position="last",
            ignore_index=True,
        ).iloc[0]
        row = {"summary_scope": "per_analyzer_best_family"}
        row.update({column: best[column] for column in copy_columns if column in best.index})
        rows.append(row)

    metric_columns = [
        "overall_rmse",
        "zero_rmse",
        "low_range_rmse",
        "main_range_rmse",
        "temp_bias_spread",
        "old_chain_overall_rmse",
        "gap_to_old_overall",
        "gap_to_old_zero",
        "delta_vs_current_fixed_family_overall",
        "delta_vs_current_fixed_family_zero",
        "delta_vs_current_fixed_family_low",
        "delta_vs_current_fixed_family_main",
        "gap_closed_ratio_vs_current_fixed_family_raw",
        "gap_closed_ratio_vs_current_fixed_family_capped",
        "current_fixed_family_gap_to_old_overall",
    ]
    group_columns = [*profile_cols, "ppm_family_mode"]
    for group_key, subset in detail_df.groupby(group_columns, dropna=False):
        values = group_key if isinstance(group_key, tuple) else (group_key,)
        row = {"summary_scope": "aggregate_profile_family_mean", "analyzer_id": "ALL"}
        row.update(dict(zip(group_columns, values, strict=False)))
        laggard_gap, laggard_count = _aggregate_laggard_gap(subset)
        row.update(
            {
                "selected_source_pair": _first_text(subset, "selected_source_pair"),
                "fixed_absorbance_proxy": _first_text(subset, "fixed_absorbance_proxy"),
                "fixed_zero_residual_mode": _first_text(subset, "fixed_zero_residual_mode"),
                "fixed_prediction_scope": _first_text(subset, "fixed_prediction_scope"),
                "fixed_water_lineage_mode": _first_text(subset, "fixed_water_lineage_mode"),
                "fixed_best_model": _first_text(subset, "fixed_best_model"),
                "fixed_model_family": _first_text(subset, "fixed_model_family"),
                "uses_humidity_cross_terms": bool(subset["uses_humidity_cross_terms"].fillna(False).any()),
                "humidity_feature_set": _first_text(subset, "humidity_feature_set"),
                "crossed_old_chain_flag": bool(subset["crossed_old_chain_flag"].fillna(False).any()),
                "crossed_old_chain_analyzer_count": int(subset["crossed_old_chain_flag"].fillna(False).sum()),
                "laggard_only_weighted_gap_closed_ratio_capped": laggard_gap,
                "laggard_only_analyzer_count": laggard_count,
            }
        )
        row.update(subset[metric_columns].mean(numeric_only=True).to_dict())
        rows.append(row)

    aggregate = pd.DataFrame([row for row in rows if row["summary_scope"] == "aggregate_profile_family_mean"])
    if not aggregate.empty:
        for _, subset in aggregate.groupby(profile_cols, dropna=False):
            best = subset.sort_values(
                [
                    "laggard_only_weighted_gap_closed_ratio_capped",
                    "gap_closed_ratio_vs_current_fixed_family_capped",
                    "delta_vs_current_fixed_family_overall",
                    "ppm_family_mode",
                ],
                ascending=[False, False, False, True],
                na_position="last",
                ignore_index=True,
            ).iloc[0]
            row = best.to_dict()
            row["summary_scope"] = "aggregate_profile_best_family"
            rows.append(row)

    return pd.DataFrame(rows).sort_values(
        ["summary_scope", "mode2_semantic_profile", "analyzer_id", "ppm_family_mode"],
        ignore_index=True,
    )


def _profile_legacy_gap_lookup(legacy_summary: pd.DataFrame) -> dict[tuple[str, bool, bool], dict[str, Any]]:
    if legacy_summary.empty:
        return {}
    subset = legacy_summary[
        legacy_summary["summary_scope"].astype(str).isin({"aggregate_profile_best_mode", "aggregate_mode_mean"})
    ].copy()
    lookup: dict[tuple[str, bool, bool], dict[str, Any]] = {}
    for row in subset.itertuples(index=False):
        key = (
            str(getattr(row, "mode2_semantic_profile", "mode2_semantics_unknown")),
            bool(getattr(row, "mode2_legacy_raw_compare_safe", False)),
            bool(getattr(row, "mode2_is_baseline_bearing_profile", False)),
        )
        current = lookup.get(key)
        score = pd.to_numeric(pd.Series([getattr(row, "laggard_only_weighted_gap_closed_ratio_capped", math.nan)]), errors="coerce").iloc[0]
        if current is None:
            lookup[key] = row._asdict()
            continue
        current_score = pd.to_numeric(
            pd.Series([current.get("laggard_only_weighted_gap_closed_ratio_capped", math.nan)]),
            errors="coerce",
        ).iloc[0]
        if pd.notna(score) and (pd.isna(current_score) or score > current_score):
            lookup[key] = row._asdict()
    return lookup


def _cross_term_gains(detail_df: pd.DataFrame) -> pd.DataFrame:
    if detail_df.empty:
        return pd.DataFrame()
    cross = detail_df[detail_df["uses_humidity_cross_terms"].fillna(False)].copy()
    if cross.empty:
        return pd.DataFrame()
    rows: list[dict[str, Any]] = []
    for analyzer_id, subset in cross.groupby("analyzer_id", dropna=False):
        best = subset.sort_values(
            ["delta_vs_current_fixed_family_overall", "delta_vs_current_fixed_family_zero"],
            ascending=[False, False],
            na_position="last",
            ignore_index=True,
        ).iloc[0]
        rows.append(
            {
                "analyzer_id": analyzer_id,
                "best_cross_family_mode": best["ppm_family_mode"],
                "best_cross_delta_overall": best["delta_vs_current_fixed_family_overall"],
                "best_cross_delta_zero": best["delta_vs_current_fixed_family_zero"],
                "mode2_semantic_profile": best["mode2_semantic_profile"],
            }
        )
    return pd.DataFrame(rows)
