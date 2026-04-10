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


def _profile_rows(
    detail_df: pd.DataFrame,
    summary_df: pd.DataFrame,
    legacy_summary: pd.DataFrame,
) -> list[dict[str, Any]]:
    profile_cols = [
        "mode2_semantic_profile",
        "mode2_legacy_raw_compare_safe",
        "mode2_is_baseline_bearing_profile",
    ]
    legacy_lookup = _profile_legacy_gap_lookup(legacy_summary)
    cross_gains = _cross_term_gains(detail_df)
    rows: list[dict[str, Any]] = []
    best_profile_rows = summary_df[summary_df["summary_scope"] == "aggregate_profile_best_family"].copy()

    for best in best_profile_rows.itertuples(index=False):
        key = (
            str(best.mode2_semantic_profile),
            bool(best.mode2_legacy_raw_compare_safe),
            bool(best.mode2_is_baseline_bearing_profile),
        )
        legacy_best = legacy_lookup.get(key, {})
        legacy_gap = pd.to_numeric(pd.Series([legacy_best.get("laggard_only_weighted_gap_closed_ratio_capped", math.nan)]), errors="coerce").iloc[0]
        ppm_gap = pd.to_numeric(pd.Series([getattr(best, "laggard_only_weighted_gap_closed_ratio_capped", math.nan)]), errors="coerce").iloc[0]
        ppm_delta = pd.to_numeric(pd.Series([getattr(best, "delta_vs_current_fixed_family_overall", math.nan)]), errors="coerce").iloc[0]
        remaining_zero_gap = pd.to_numeric(pd.Series([getattr(best, "gap_to_old_zero", math.nan)]), errors="coerce").iloc[0]
        remaining_temp_gap = pd.to_numeric(pd.Series([getattr(best, "temp_bias_spread", math.nan)]), errors="coerce").iloc[0]
        profile_detail = detail_df[
            (detail_df["mode2_semantic_profile"] == best.mode2_semantic_profile)
            & (detail_df["mode2_legacy_raw_compare_safe"] == bool(best.mode2_legacy_raw_compare_safe))
            & (detail_df["mode2_is_baseline_bearing_profile"] == bool(best.mode2_is_baseline_bearing_profile))
        ].copy()
        profile_cross = cross_gains.merge(
            profile_detail[["analyzer_id"] + profile_cols].drop_duplicates(),
            on="analyzer_id",
            how="inner",
        )
        analyzer_gain_text = "none"
        if not profile_cross.empty:
            analyzer_gain_text = "; ".join(
                f"{row.analyzer_id}:{row.best_cross_family_mode}:{float(row.best_cross_delta_overall):.4f}"
                for row in profile_cross.sort_values("analyzer_id").itertuples(index=False)
            )
        ga01_gain = pd.to_numeric(
            profile_cross.loc[profile_cross["analyzer_id"] == "GA01", "best_cross_delta_overall"],
            errors="coerce",
        )
        other_gain = pd.to_numeric(
            profile_cross.loc[profile_cross["analyzer_id"].isin(["GA02", "GA03"]), "best_cross_delta_overall"],
            errors="coerce",
        )
        ga01_more_important = (
            ga01_gain.notna().any()
            and (
                other_gain.dropna().empty
                or float(ga01_gain.dropna().iloc[0]) > float(other_gain.dropna().mean()) + 0.5
            )
        )
        profile_values = dict(zip(profile_cols, key, strict=False))
        rows.extend(
            [
                {
                    "conclusion_scope": "per_profile",
                    **profile_values,
                    "question_id": "weak_ppm_vs_legacy_water",
                    "question": "Does weak ppm family behavior look more like the unified main cause than missing legacy water-lineage replay?",
                    "answer": "yes_more_likely" if pd.notna(ppm_gap) and (pd.isna(legacy_gap) or ppm_gap > legacy_gap + 0.05) else "not_clear_or_profile_limited",
                    "evidence": (
                        f"best_family={best.ppm_family_mode}; laggard_gap_closed_capped={ppm_gap:.4f}; legacy_replay_best_capped={legacy_gap:.4f}"
                        if pd.notna(ppm_gap)
                        else "insufficient_data"
                    ),
                    "recommended_ppm_family_mode": best.ppm_family_mode,
                },
                {
                    "conclusion_scope": "per_profile",
                    **profile_values,
                    "question_id": "humidity_cross_importance",
                    "question": "Are humidity cross terms more important for GA01 than for GA02/GA03?",
                    "answer": "yes_ga01_stronger" if ga01_more_important else "no_clear_ga01_dominance",
                    "evidence": f"best_cross_family_gains={analyzer_gain_text}",
                    "recommended_ppm_family_mode": best.ppm_family_mode,
                },
                {
                    "conclusion_scope": "per_profile",
                    **profile_values,
                    "question_id": "v5_abs_k_minimal_doc_alignment",
                    "question": "Is v5_abs_k_minimal the closest debugger-accessible mapping to the documented V5 Abs+K algorithm?",
                    "answer": "yes_approximate_mapping",
                    "evidence": "Uses fixed A proxy plus semantic humidity proxy K with A/A2/A3/K/K2/AK terms; debugger has no exact AbsFinal column.",
                    "recommended_ppm_family_mode": "v5_abs_k_minimal",
                },
                {
                    "conclusion_scope": "per_profile",
                    **profile_values,
                    "question_id": "remaining_zero_temp_gap",
                    "question": "If ppm family gains remain incomplete, does the evidence still support missing 40C / 0 ppm zero anchor as the next main suspect?",
                    "answer": (
                        "yes_still_primary_suspect"
                        if pd.notna(ppm_delta) and ppm_delta > 0.0 and (
                            (pd.notna(remaining_zero_gap) and remaining_zero_gap > 0.0)
                            or (pd.notna(remaining_temp_gap) and remaining_temp_gap > 0.0)
                        )
                        else "not_promoted_by_this_profile"
                    ),
                    "evidence": (
                        f"best_family={best.ppm_family_mode}; delta_vs_current_overall={ppm_delta:.4f}; remaining_gap_to_old_zero={remaining_zero_gap:.4f}; temp_bias_spread={remaining_temp_gap:.4f}"
                        if pd.notna(ppm_delta)
                        else "insufficient_data"
                    ),
                    "recommended_ppm_family_mode": best.ppm_family_mode,
                },
                {
                    "conclusion_scope": "per_profile",
                    **profile_values,
                    "question_id": "headline",
                    "question": "headline",
                    "answer": (
                        f"{best.ppm_family_mode} closed {float(best.gap_closed_ratio_vs_current_fixed_family_capped):.2%} of the current fixed-family gap (capped)"
                        if pd.notna(getattr(best, "gap_closed_ratio_vs_current_fixed_family_capped"))
                        else "insufficient_data"
                    ),
                    "evidence": (
                        f"laggard_weighted_gap_closed_capped={ppm_gap:.4f}; crossed_old_chain={int(getattr(best, 'crossed_old_chain_analyzer_count', 0))}"
                        if pd.notna(ppm_gap)
                        else "insufficient_data"
                    ),
                    "recommended_ppm_family_mode": best.ppm_family_mode,
                },
            ]
        )
    return rows


def _synthesis_rows(conclusion_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    profile_rows = [row for row in conclusion_rows if row.get("conclusion_scope") == "per_profile"]
    profile_ids = sorted({str(row.get("mode2_semantic_profile") or "") for row in profile_rows})
    if len(profile_ids) <= 1:
        return []
    question_map = {
        "weak_ppm_vs_legacy_water": "Does weak ppm family behavior look more like the unified main cause than missing legacy water-lineage replay?",
        "humidity_cross_importance": "Are humidity cross terms more important for GA01 than for GA02/GA03?",
        "remaining_zero_temp_gap": "If ppm family gains remain incomplete, does the evidence still support missing 40C / 0 ppm zero anchor as the next main suspect?",
    }
    synthesis: list[dict[str, Any]] = []
    for question_id, question in question_map.items():
        scoped = [row for row in profile_rows if row.get("question_id") == question_id]
        if not scoped:
            continue
        synthesis.append(
            {
                "conclusion_scope": "cross_profile_synthesis",
                "mode2_semantic_profile": "ALL_SEPARATED_PROFILES",
                "mode2_legacy_raw_compare_safe": False,
                "mode2_is_baseline_bearing_profile": any(bool(row.get("mode2_is_baseline_bearing_profile", False)) for row in scoped),
                "question_id": question_id,
                "question": question,
                "answer": "see_separated_profile_rows",
                "evidence": "; ".join(f"{row['mode2_semantic_profile']}={row['answer']}" for row in scoped),
                "recommended_ppm_family_mode": "|".join(
                    sorted({str(row.get("recommended_ppm_family_mode") or "") for row in scoped if str(row.get("recommended_ppm_family_mode") or "")})
                ),
            }
        )
    return synthesis


def _conclusion_rows(
    detail_df: pd.DataFrame,
    summary_df: pd.DataFrame,
    legacy_summary: pd.DataFrame,
) -> pd.DataFrame:
    if detail_df.empty or summary_df.empty:
        return pd.DataFrame()
    rows = _profile_rows(detail_df, summary_df, legacy_summary)
    rows.extend(_synthesis_rows(rows))
    return pd.DataFrame(rows)


def run_fixed_chain_ppm_family_challenge(
    *,
    filtered_samples: pd.DataFrame,
    legacy_water_feature_frame: pd.DataFrame,
    fixed_selection: pd.DataFrame,
    old_ratio_residuals: pd.DataFrame,
    legacy_water_summary: pd.DataFrame,
    config: Any,
) -> dict[str, pd.DataFrame]:
    """Compare fixed-chain ppm families without changing the deployable winner."""

    fixed_points = _selected_legacy_none_points(legacy_water_feature_frame, fixed_selection)
    if fixed_points.empty or fixed_selection.empty:
        empty = pd.DataFrame()
        return {"detail": empty, "summary": empty, "conclusions": empty}

    mode2_summary = build_analyzer_mode2_summary(filtered_samples)
    work = fixed_points.merge(mode2_summary, on="analyzer", how="left")
    work = work.merge(
        old_ratio_residuals[["analyzer", "point_row", "point_title", "old_prediction_ppm", "old_residual_ppm"]],
        on=["analyzer", "point_row", "point_title"],
        how="left",
    )
    work["analyzer_id"] = work["analyzer"]
    work["target_ppm"] = pd.to_numeric(work["target_co2_ppm"], errors="coerce")
    work["temp_c"] = pd.to_numeric(work["temp_set_c"], errors="coerce")
    work["temp_model_c"] = pd.to_numeric(work["temp_use_mean_c"], errors="coerce")
    work["group_key"] = (
        work["temp_c"].map(lambda value: "nan" if pd.isna(value) else f"{float(value):g}")
        + "|"
        + work["target_ppm"].map(lambda value: "nan" if pd.isna(value) else f"{float(value):g}")
        + "|"
        + work["point_title"].fillna("").astype(str)
    )

    selection_lookup = fixed_selection.set_index("analyzer_id")
    active_lookup = _active_spec_lookup(config)
    detail_rows: list[dict[str, Any]] = []
    for analyzer_id, subset in work.groupby("analyzer_id", dropna=False):
        if analyzer_id not in selection_lookup.index:
            continue
        fixed_row = selection_lookup.loc[analyzer_id]
        if isinstance(fixed_row, pd.DataFrame):
            fixed_row = fixed_row.iloc[0]
        requested_scope = str(fixed_row.get("selected_prediction_scope") or "overall_fit")
        absorbance_proxy = str(fixed_row.get("absorbance_column") or "A_mean")
        analyzer_frame = subset.copy()
        analyzer_frame["fixed_absorbance_proxy"] = absorbance_proxy
        analyzer_frame["fixed_best_model"] = str(fixed_row.get("best_absorbance_model") or "")
        analyzer_frame["fixed_model_family"] = str(fixed_row.get("best_model_family") or "")
        analyzer_frame["fixed_zero_residual_mode"] = str(fixed_row.get("zero_residual_mode") or "none")
        analyzer_frame["fixed_prediction_scope"] = requested_scope
        selected_source_pair = str(fixed_row.get("selected_source_pair") or "")
        humidity_proxy, humidity_proxy_label = _choose_humidity_proxy(
            analyzer_frame,
            selected_source_pair=selected_source_pair,
            legacy_safe=bool(analyzer_frame["mode2_legacy_raw_compare_safe"].fillna(False).iloc[0]),
            baseline_bearing=bool(analyzer_frame["mode2_is_baseline_bearing_profile"].fillna(False).iloc[0]),
        )
        humidity_delta, humidity_delta_label = _choose_humidity_delta(analyzer_frame)
        analyzer_frame["K_feature"] = humidity_proxy
        analyzer_frame["ratio_feature"] = pd.to_numeric(analyzer_frame.get("ratio_in_mean"), errors="coerce")
        analyzer_frame["pressure_feature"] = (
            pd.to_numeric(analyzer_frame.get("pressure_use_mean_hpa"), errors="coerce") - float(config.p_ref_hpa)
        ) / float(config.p_ref_hpa)
        analyzer_frame["humidity_delta_feature"] = humidity_delta
        analyzer_frame["humidity_relative_feature"] = analyzer_frame["ratio_feature"] * analyzer_frame["K_feature"]
        family_specs = _family_specs_for_analyzer(str(fixed_row.get("best_absorbance_model") or ""), active_lookup)
        for family_spec in family_specs:
            spec = family_spec.to_absorbance_spec()
            try:
                _score_row, _coeff_rows, residual_rows = _fit_one_candidate(
                    analyzer_df=analyzer_frame,
                    spec=spec,
                    strategy=config.model_selection_strategy,
                    score_weights=config.composite_weight_map(),
                    legacy_score_weights=config.legacy_composite_weight_map(),
                    enable_composite_score=config.enable_composite_score,
                    absorbance_column=absorbance_proxy,
                )
            except Exception:
                continue
            prediction_table, actual_scope = _prediction_tables(residual_rows, requested_scope)
            compare = analyzer_frame.merge(
                prediction_table,
                on=["analyzer_id", "point_title", "point_row"],
                how="left",
            )
            compare["old_error"] = pd.to_numeric(compare["old_residual_ppm"], errors="coerce").combine_first(
                pd.to_numeric(compare["old_prediction_ppm"], errors="coerce") - pd.to_numeric(compare["target_ppm"], errors="coerce")
            )
            compare["new_error"] = pd.to_numeric(compare["selected_error_ppm"], errors="coerce")
            humidity_feature_set = (
                "none"
                if family_spec.ppm_family_mode == "current_fixed_family"
                else "|".join(
                    [
                        humidity_proxy_label,
                        humidity_delta_label,
                        "pressure_feature:(P_use-P_ref)/P_ref",
                        "ratio_feature:ratio_in_mean",
                        "humidity_relative_feature:ratio_in_mean*K",
                    ]
                )
            )
            detail_rows.append(
                _detail_from_compare(
                    compare,
                    analyzer_id=str(analyzer_id),
                    fixed_row=pd.Series(
                        {
                            **fixed_row.to_dict(),
                            "selected_source_pair": selected_source_pair,
                            "fixed_absorbance_proxy": absorbance_proxy,
                            "fixed_best_model": str(fixed_row.get("best_absorbance_model") or ""),
                            "fixed_model_family": str(fixed_row.get("best_model_family") or ""),
                            "fixed_zero_residual_mode": str(fixed_row.get("zero_residual_mode") or "none"),
                            "mode2_semantic_profile": analyzer_frame["mode2_semantic_profile"].fillna("mode2_semantics_unknown").iloc[0],
                            "mode2_legacy_raw_compare_safe": bool(analyzer_frame["mode2_legacy_raw_compare_safe"].fillna(False).iloc[0]),
                            "mode2_is_baseline_bearing_profile": bool(analyzer_frame["mode2_is_baseline_bearing_profile"].fillna(False).iloc[0]),
                        }
                    ),
                    ppm_family_mode=family_spec.ppm_family_mode,
                    uses_humidity_cross_terms=family_spec.uses_humidity_cross_terms,
                    humidity_feature_set=humidity_feature_set,
                    requested_scope=requested_scope,
                    actual_scope=actual_scope,
                    formula=family_spec.formula,
                )
            )

    detail_df = pd.DataFrame(detail_rows).sort_values(["analyzer_id", "ppm_family_mode"], ignore_index=True) if detail_rows else pd.DataFrame()
    if detail_df.empty:
        empty = pd.DataFrame()
        return {"detail": empty, "summary": empty, "conclusions": empty}

    baseline = detail_df[detail_df["ppm_family_mode"] == "current_fixed_family"].set_index("analyzer_id")
    delta_rows: list[dict[str, Any]] = []
    for row in detail_df.itertuples(index=False):
        base_row = baseline.loc[row.analyzer_id] if row.analyzer_id in baseline.index else pd.Series(dtype=object)
        if isinstance(base_row, pd.DataFrame):
            base_row = base_row.iloc[0]
        baseline_gap = pd.to_numeric(pd.Series([base_row.get("gap_to_old_overall", math.nan)]), errors="coerce").iloc[0]
        current_gap = pd.to_numeric(pd.Series([row.gap_to_old_overall]), errors="coerce").iloc[0]
        raw_gap_ratio = (
            float((baseline_gap - current_gap) / baseline_gap)
            if pd.notna(baseline_gap) and abs(float(baseline_gap)) > 1.0e-12 and pd.notna(current_gap)
            else math.nan
        )
        delta_rows.append(
            {
                "analyzer_id": row.analyzer_id,
                "ppm_family_mode": row.ppm_family_mode,
                "delta_vs_current_fixed_family_overall": (
                    float(base_row.get("overall_rmse")) - float(row.overall_rmse)
                    if not base_row.empty and pd.notna(base_row.get("overall_rmse")) and pd.notna(row.overall_rmse)
                    else math.nan
                ),
                "delta_vs_current_fixed_family_zero": (
                    float(base_row.get("zero_rmse")) - float(row.zero_rmse)
                    if not base_row.empty and pd.notna(base_row.get("zero_rmse")) and pd.notna(row.zero_rmse)
                    else math.nan
                ),
                "delta_vs_current_fixed_family_low": (
                    float(base_row.get("low_range_rmse")) - float(row.low_range_rmse)
                    if not base_row.empty and pd.notna(base_row.get("low_range_rmse")) and pd.notna(row.low_range_rmse)
                    else math.nan
                ),
                "delta_vs_current_fixed_family_main": (
                    float(base_row.get("main_range_rmse")) - float(row.main_range_rmse)
                    if not base_row.empty and pd.notna(base_row.get("main_range_rmse")) and pd.notna(row.main_range_rmse)
                    else math.nan
                ),
                "current_fixed_family_gap_to_old_overall": baseline_gap,
                "gap_closed_ratio_vs_current_fixed_family_raw": raw_gap_ratio,
                "gap_closed_ratio_vs_current_fixed_family_capped": _clip_ratio(raw_gap_ratio),
                "crossed_old_chain_flag": bool(pd.notna(row.gap_to_old_overall) and float(row.gap_to_old_overall) <= 0.0),
            }
        )
    detail_df = detail_df.merge(pd.DataFrame(delta_rows), on=["analyzer_id", "ppm_family_mode"], how="left")
    summary_df = _summary_rows(detail_df)
    conclusions_df = _conclusion_rows(detail_df, summary_df, legacy_water_summary)
    return {"detail": detail_df, "summary": summary_df, "conclusions": conclusions_df}
