"""Diagnostic-only source selection audit and source policy challenge helpers."""

from __future__ import annotations

import math
from typing import Any

import numpy as np
import pandas as pd

from .diagnostics import build_analyzer_mode2_summary


def _metrics(errors: pd.Series | np.ndarray) -> dict[str, float]:
    clean = pd.to_numeric(pd.Series(errors), errors="coerce").dropna()
    if clean.empty:
        return {
            "rmse": math.nan,
            "mae": math.nan,
        }
    values = clean.to_numpy(dtype=float)
    return {
        "rmse": float(np.sqrt(np.mean(np.square(values)))),
        "mae": float(np.mean(np.abs(values))),
    }


def _range_mask(series: pd.Series, segment_tag: str) -> pd.Series:
    target = pd.to_numeric(series, errors="coerce")
    if segment_tag == "overall":
        return pd.Series(True, index=series.index)
    if segment_tag == "zero":
        return target == 0.0
    if segment_tag == "low":
        return (target > 0.0) & (target <= 200.0)
    if segment_tag == "main":
        return (target > 200.0) & (target <= 1000.0)
    raise ValueError(f"Unsupported segment_tag: {segment_tag}")


def _segment_rmse(frame: pd.DataFrame, error_column: str, segment_tag: str) -> float:
    subset = frame[_range_mask(frame["target_ppm"], segment_tag)].copy()
    return _metrics(subset[error_column])["rmse"]


def _score_metric_name(config: Any) -> str:
    return "composite_score" if bool(getattr(config, "enable_composite_score", True)) else "validation_rmse"


def _source_pair_label(ratio_source: str) -> str:
    return "raw/raw" if str(ratio_source) == "ratio_co2_raw" else "filt/filt"


def _source_short(source_value: str) -> str:
    source_text = str(source_value or "")
    if "raw" in source_text:
        return "raw"
    if "filt" in source_text:
        return "filt"
    return "unknown"


def _first_text(frame: pd.DataFrame, column_name: str, default: str = "") -> str:
    if column_name not in frame.columns:
        return default
    values = frame[column_name].dropna().astype(str).str.strip()
    values = values[values != ""]
    if values.empty:
        return default
    return str(values.iloc[0])


def _first_bool(frame: pd.DataFrame, column_name: str, default: bool = False) -> bool:
    if column_name not in frame.columns:
        return default
    values = frame[column_name].dropna()
    if values.empty:
        return default
    return bool(values.map(bool).iloc[0])


def _policy_sort_columns(score_metric: str) -> list[str]:
    return [score_metric, "validation_rmse", "overall_rmse", "model_id"]


def _format_point_label(row: pd.Series) -> str:
    temp_value = pd.to_numeric(pd.Series([row.get("temp_c", row.get("temp_set_c"))]), errors="coerce").iloc[0]
    target_value = pd.to_numeric(pd.Series([row.get("target_ppm")]), errors="coerce").iloc[0]
    segment = str(row.get("segment_tag") or "")
    temp_text = "nanC" if pd.isna(temp_value) else f"{float(temp_value):g}C"
    target_text = "nanppm" if pd.isna(target_value) else f"{float(target_value):g}ppm"
    return f"{temp_text}/{target_text}/{segment}"


def _format_example_series(frame: pd.DataFrame, improvement_column: str) -> str:
    if frame.empty or improvement_column not in frame.columns:
        return ""
    ranked = frame.dropna(subset=[improvement_column]).copy()
    if ranked.empty:
        return ""
    ranked = ranked.sort_values([improvement_column], ascending=[False], ignore_index=True).head(3)
    return "|".join(_format_point_label(row) for _, row in ranked.iterrows())


def _selection_reason_primary(
    selected_source_pair: str,
    raw_available_flag: bool,
    filt_available_flag: bool,
    raw_required_keys_pass: bool,
    filt_required_keys_pass: bool,
    raw_quality_gate_pass: bool,
    filt_quality_gate_pass: bool,
    raw_score: float,
    filt_score: float,
) -> str:
    selected_short = _source_short(selected_source_pair)
    if selected_short == "filt":
        if not raw_available_flag:
            return "raw_unavailable"
        if not raw_required_keys_pass:
            return "raw_required_keys_failed"
        if not raw_quality_gate_pass:
            return "raw_quality_gate_failed"
        if pd.notna(filt_score) and pd.notna(raw_score) and float(filt_score) < float(raw_score) - 1.0e-12:
            return "filt_score_better"
        return "filt_selected_without_raw_advantage"
    if selected_short == "raw":
        if not filt_available_flag:
            return "filt_unavailable"
        if not filt_required_keys_pass:
            return "filt_required_keys_failed"
        if not filt_quality_gate_pass:
            return "filt_quality_gate_failed"
        if pd.notna(raw_score) and pd.notna(filt_score) and float(raw_score) < float(filt_score) - 1.0e-12:
            return "raw_score_better"
        if pd.notna(raw_score) and pd.notna(filt_score) and abs(float(raw_score) - float(filt_score)) <= 1.0e-12:
            return "tie_break_or_default_prefers_raw"
        return "raw_selected_without_score_gain"
    return "selection_unknown"


def _selection_reason_secondary(
    mode2_legacy_raw_compare_safe: bool,
    signal_priority_prefers_filt_flag: bool,
    raw_score: float,
    filt_score: float,
    score_metric: str,
) -> str:
    if not mode2_legacy_raw_compare_safe:
        return "mode2 profile marks legacy raw compare unsafe, but current source selector does not hard-gate on this flag"
    if signal_priority_prefers_filt_flag:
        return "config/default preference points to filt"
    if pd.notna(raw_score) and pd.notna(filt_score):
        return f"{score_metric}: raw={float(raw_score):.6g}, filt={float(filt_score):.6g}"
    return "no secondary reason available"


def build_source_selection_audit(
    filtered: pd.DataFrame,
    absorbance_points: pd.DataFrame,
    model_results: dict[str, pd.DataFrame],
    config: Any,
) -> dict[str, pd.DataFrame]:
    """Explain why current source selection became raw/raw or filt/filt per analyzer."""

    filtered = filtered.copy()
    absorbance_points = absorbance_points.copy()
    scores = model_results.get("scores", pd.DataFrame()).copy()
    selection = model_results.get("selection", pd.DataFrame()).copy()
    mode2_summary = build_analyzer_mode2_summary(filtered)
    score_metric = _score_metric_name(config)
    signal_priority_prefers_filt_flag = bool(getattr(config, "default_ratio_source", "") == "ratio_co2_filt")

    detail_rows: list[dict[str, Any]] = []
    analyzer_ids = sorted(
        set(filtered.get("analyzer", pd.Series(dtype=str)).dropna().astype(str).tolist())
        | set(selection.get("analyzer_id", pd.Series(dtype=str)).dropna().astype(str).tolist())
    )

    for analyzer_id in analyzer_ids:
        analyzer_samples = filtered[filtered["analyzer"].astype(str) == analyzer_id].copy()
        mode2_row = mode2_summary[mode2_summary["analyzer"].astype(str) == analyzer_id]
        profile = _first_text(mode2_row, "mode2_semantic_profile", default="mode2_semantics_unknown")
        legacy_safe = _first_bool(mode2_row, "mode2_legacy_raw_compare_safe", default=False)
        selection_row = (
            selection[selection["analyzer_id"].astype(str) == analyzer_id].iloc[0]
            if not selection.empty and (selection["analyzer_id"].astype(str) == analyzer_id).any()
            else pd.Series(dtype=object)
        )
        selected_source_pair = str(selection_row.get("selected_source_pair") or "")
        actual_ratio_source_used = _source_short(selected_source_pair)

        source_values: dict[str, dict[str, Any]] = {}
        for ratio_source in ("ratio_co2_raw", "ratio_co2_filt"):
            source_short = _source_short(ratio_source)
            source_pair = _source_pair_label(ratio_source)
            available_flag = bool(analyzer_samples[ratio_source].notna().any()) if ratio_source in analyzer_samples.columns else False
            required_keys_pass = bool(
                ratio_source in analyzer_samples.columns
                and getattr(config, "default_temp_source", "") in analyzer_samples.columns
                and getattr(config, "default_pressure_source", "") in analyzer_samples.columns
                and analyzer_samples[[ratio_source, config.default_temp_source, config.default_pressure_source]].notna().all(axis=1).any()
            )
            source_points = absorbance_points[
                (absorbance_points["analyzer"].astype(str) == analyzer_id)
                & (absorbance_points["ratio_source"].astype(str) == ratio_source)
                & (absorbance_points["temp_source"].astype(str) == str(config.default_temp_source))
                & (absorbance_points["pressure_source"].astype(str) == str(config.default_pressure_source))
                & (absorbance_points["r0_model"].astype(str) == str(config.default_r0_model))
            ].copy()
            source_scores = scores[
                (scores["analyzer_id"].astype(str) == analyzer_id)
                & (scores["selected_source_pair"].astype(str) == source_pair)
            ].copy()
            source_scores = source_scores.sort_values(_policy_sort_columns(score_metric), ignore_index=True)
            forced_score = (
                float(pd.to_numeric(source_scores.iloc[0][score_metric], errors="coerce"))
                if not source_scores.empty and score_metric in source_scores.columns
                else math.nan
            )
            quality_gate_pass = bool(not source_scores.empty and source_points["A_mean"].notna().any())
            source_values[source_short] = {
                "available_flag": available_flag,
                "required_keys_pass": required_keys_pass,
                "quality_gate_pass": quality_gate_pass,
                "nonpositive_ratio_count": int(
                    (pd.to_numeric(analyzer_samples.get(ratio_source, pd.Series(dtype=float)), errors="coerce") <= 0.0).fillna(False).sum()
                ),
                "missing_count": int(pd.to_numeric(analyzer_samples.get(ratio_source, pd.Series(dtype=float)), errors="coerce").isna().sum()),
                "forced_score": forced_score,
            }

        detail_rows.append(
            {
                "analyzer_id": analyzer_id,
                "mode2_semantic_profile": profile,
                "mode2_legacy_raw_compare_safe": legacy_safe,
                "raw_available_flag": bool(source_values["raw"]["available_flag"]),
                "filt_available_flag": bool(source_values["filt"]["available_flag"]),
                "raw_required_keys_pass": bool(source_values["raw"]["required_keys_pass"]),
                "filt_required_keys_pass": bool(source_values["filt"]["required_keys_pass"]),
                "raw_quality_gate_pass": bool(source_values["raw"]["quality_gate_pass"]),
                "filt_quality_gate_pass": bool(source_values["filt"]["quality_gate_pass"]),
                "raw_nonpositive_ratio_count": int(source_values["raw"]["nonpositive_ratio_count"]),
                "filt_nonpositive_ratio_count": int(source_values["filt"]["nonpositive_ratio_count"]),
                "raw_missing_count": int(source_values["raw"]["missing_count"]),
                "filt_missing_count": int(source_values["filt"]["missing_count"]),
                "signal_priority_prefers_filt_flag": signal_priority_prefers_filt_flag,
                "raw_score_if_forced": source_values["raw"]["forced_score"],
                "filt_score_if_forced": source_values["filt"]["forced_score"],
                "selected_source_pair": selected_source_pair,
                "selection_reason_primary": _selection_reason_primary(
                    selected_source_pair=selected_source_pair,
                    raw_available_flag=bool(source_values["raw"]["available_flag"]),
                    filt_available_flag=bool(source_values["filt"]["available_flag"]),
                    raw_required_keys_pass=bool(source_values["raw"]["required_keys_pass"]),
                    filt_required_keys_pass=bool(source_values["filt"]["required_keys_pass"]),
                    raw_quality_gate_pass=bool(source_values["raw"]["quality_gate_pass"]),
                    filt_quality_gate_pass=bool(source_values["filt"]["quality_gate_pass"]),
                    raw_score=float(source_values["raw"]["forced_score"]),
                    filt_score=float(source_values["filt"]["forced_score"]),
                ),
                "selection_reason_secondary": _selection_reason_secondary(
                    mode2_legacy_raw_compare_safe=legacy_safe,
                    signal_priority_prefers_filt_flag=signal_priority_prefers_filt_flag,
                    raw_score=float(source_values["raw"]["forced_score"]),
                    filt_score=float(source_values["filt"]["forced_score"]),
                    score_metric=score_metric,
                ),
                "actual_ratio_source_used": actual_ratio_source_used,
            }
        )

    detail_df = pd.DataFrame(detail_rows).sort_values(["analyzer_id"], ignore_index=True) if detail_rows else pd.DataFrame()
    if detail_df.empty:
        empty = pd.DataFrame()
        return {"detail": empty, "summary": empty, "conclusions": empty}

    used_pairs = detail_df["selected_source_pair"].fillna("").astype(str).tolist()
    used_sources = detail_df["actual_ratio_source_used"].fillna("unknown").astype(str).tolist()
    majority = "raw" if used_sources.count("raw") > used_sources.count("filt") else "filt" if used_sources.count("filt") > used_sources.count("raw") else "mixed"
    why_mixed = "; ".join(
        f"{row.analyzer_id}={row.selected_source_pair}:{row.selection_reason_primary}"
        for row in detail_df.itertuples(index=False)
    )

    summary_rows = detail_df.copy()
    summary_rows.insert(0, "summary_scope", "per_analyzer")
    summary_rows["designed_v5_ratio_source_intent"] = "raw_or_instantaneous"
    summary_rows["actual_ratio_source_used_in_this_run"] = "mixed" if len(set(used_pairs)) > 1 else majority
    summary_rows["why_selected_source_pair_became_mixed"] = why_mixed
    summary_rows["score_metric_used"] = score_metric

    overall_row = pd.DataFrame(
        [
            {
                "summary_scope": "overall",
                "analyzer_id": "",
                "mode2_semantic_profile": "multiple",
                "mode2_legacy_raw_compare_safe": bool(detail_df["mode2_legacy_raw_compare_safe"].all()),
                "raw_available_flag": bool(detail_df["raw_available_flag"].all()),
                "filt_available_flag": bool(detail_df["filt_available_flag"].all()),
                "raw_required_keys_pass": bool(detail_df["raw_required_keys_pass"].all()),
                "filt_required_keys_pass": bool(detail_df["filt_required_keys_pass"].all()),
                "raw_quality_gate_pass": bool(detail_df["raw_quality_gate_pass"].all()),
                "filt_quality_gate_pass": bool(detail_df["filt_quality_gate_pass"].all()),
                "raw_nonpositive_ratio_count": int(detail_df["raw_nonpositive_ratio_count"].sum()),
                "filt_nonpositive_ratio_count": int(detail_df["filt_nonpositive_ratio_count"].sum()),
                "raw_missing_count": int(detail_df["raw_missing_count"].sum()),
                "filt_missing_count": int(detail_df["filt_missing_count"].sum()),
                "signal_priority_prefers_filt_flag": signal_priority_prefers_filt_flag,
                "raw_score_if_forced": float(pd.to_numeric(detail_df["raw_score_if_forced"], errors="coerce").mean()),
                "filt_score_if_forced": float(pd.to_numeric(detail_df["filt_score_if_forced"], errors="coerce").mean()),
                "selected_source_pair": "|".join(used_pairs),
                "selection_reason_primary": "mixed_selection_across_analyzers",
                "selection_reason_secondary": "selector compares matched raw/raw and filt/filt by score; no hard filt preference is configured",
                "actual_ratio_source_used": "mixed" if len(set(used_pairs)) > 1 else majority,
                "designed_v5_ratio_source_intent": "raw_or_instantaneous",
                "actual_ratio_source_used_in_this_run": "mixed" if len(set(used_pairs)) > 1 else majority,
                "why_selected_source_pair_became_mixed": why_mixed,
                "score_metric_used": score_metric,
            }
        ]
    )
    summary_df = pd.concat([summary_rows, overall_row], ignore_index=True, sort=False)

    selected_filt = detail_df[detail_df["actual_ratio_source_used"] == "filt"].copy()
    if selected_filt.empty:
        why_answer = "current run did not fall to filt selection on any analyzer"
    else:
        raw_fail_rows = selected_filt[
            (~selected_filt["raw_available_flag"])
            | (~selected_filt["raw_required_keys_pass"])
            | (~selected_filt["raw_quality_gate_pass"])
        ]
        if not raw_fail_rows.empty:
            why_answer = (
                "mixed source happened because at least one analyzer had raw unavailable or raw gate failure before scoring: "
                + ", ".join(raw_fail_rows["analyzer_id"].astype(str).tolist())
            )
        else:
            why_answer = (
                "mixed source happened because raw/raw and filt/filt were both available, "
                "and filt had the better forced score on: "
                + ", ".join(selected_filt["analyzer_id"].astype(str).tolist())
            )

    conclusion_rows = [
        {
            "question_id": "why_selected_source_pair_became_mixed",
            "question": "Why did the current run become mixed across analyzers?",
            "answer": why_answer,
            "evidence": why_mixed,
        },
        {
            "question_id": "whether_filt_was_config_forced",
            "question": "Was filt selected because of config preference or signal priority?",
            "answer": "No hard filt preference is configured in the current deployable selector." if not signal_priority_prefers_filt_flag else "Yes, config default points to filt.",
            "evidence": f"default_ratio_source={getattr(config, 'default_ratio_source', '')}; signal_priority_prefers_filt_flag={signal_priority_prefers_filt_flag}",
        },
    ]
    for row in detail_df.itertuples(index=False):
        conclusion_rows.append(
            {
                "question_id": f"selection_reason_{row.analyzer_id}",
                "question": f"Why was {row.selected_source_pair} selected for {row.analyzer_id}?",
                "answer": str(row.selection_reason_primary),
                "evidence": f"{row.selection_reason_secondary}; raw_score={row.raw_score_if_forced}; filt_score={row.filt_score_if_forced}",
            }
        )
    conclusions_df = pd.DataFrame(conclusion_rows)

    return {
        "detail": detail_df,
        "summary": summary_df,
        "conclusions": conclusions_df,
    }


def _policy_candidate_row(
    scores: pd.DataFrame,
    analyzer_id: str,
    source_pair: str,
    fixed_model_family: str,
    fixed_zero_residual_mode: str,
    fixed_prediction_scope: str,
    score_metric: str,
) -> pd.Series | None:
    subset = scores[
        (scores["analyzer_id"].astype(str) == analyzer_id)
        & (scores["selected_source_pair"].astype(str) == source_pair)
        & (scores["model_family"].astype(str) == fixed_model_family)
        & (scores["zero_residual_mode"].astype(str) == fixed_zero_residual_mode)
        & (scores["score_source"].astype(str) == fixed_prediction_scope)
    ].copy()
    if "water_zero_anchor_mode" in subset.columns:
        subset = subset[subset["water_zero_anchor_mode"].astype(str) == "none"].copy()
    if "with_water_zero_anchor_correction" in subset.columns:
        subset = subset[~subset["with_water_zero_anchor_correction"].fillna(False).map(bool)].copy()
    if subset.empty:
        return None
    subset = subset.sort_values(_policy_sort_columns(score_metric), ignore_index=True)
    return subset.iloc[0]


def _policy_prediction_frame(
    residuals: pd.DataFrame,
    analyzer_id: str,
    candidate_row: pd.Series,
    fixed_prediction_scope: str,
) -> pd.DataFrame:
    subset = residuals[
        (residuals["analyzer_id"].astype(str) == analyzer_id)
        & (residuals["selected_source_pair"].astype(str) == str(candidate_row.get("selected_source_pair") or ""))
        & (residuals["model_id"].astype(str) == str(candidate_row.get("model_id") or ""))
        & (residuals["model_family"].astype(str) == str(candidate_row.get("model_family") or ""))
        & (residuals["zero_residual_mode"].astype(str) == str(candidate_row.get("zero_residual_mode") or ""))
        & (residuals["prediction_scope"].astype(str) == fixed_prediction_scope)
    ].copy()
    if subset.empty:
        return subset
    subset = subset.rename(columns={"predicted_ppm": "policy_pred_ppm", "error_ppm": "policy_error_ppm"})
    subset["target_ppm"] = pd.to_numeric(subset["target_ppm"], errors="coerce")
    return subset[
        [
            "analyzer_id",
            "point_title",
            "point_row",
            "temp_c",
            "target_ppm",
            "policy_pred_ppm",
            "policy_error_ppm",
            "selected_source_pair",
            "model_id",
            "model_family",
            "zero_residual_mode",
            "prediction_scope",
        ]
    ].copy()


def _policy_choice_for_mode(
    policy_mode: str,
    fixed_row: pd.Series,
    candidate_lookup: dict[str, pd.Series | None],
) -> tuple[str, pd.Series | None]:
    current_pair = str(fixed_row.get("selected_source_pair") or "")
    raw_candidate = candidate_lookup.get("raw")
    filt_candidate = candidate_lookup.get("filt")
    if policy_mode == "current_deployable_mixed":
        return current_pair, raw_candidate if current_pair == "raw/raw" else filt_candidate
    if policy_mode == "raw_first_with_fallback":
        if raw_candidate is not None:
            return "raw/raw", raw_candidate
        if filt_candidate is not None:
            return "filt/filt", filt_candidate
        return "", None
    if policy_mode == "raw_only_strict":
        return ("raw/raw", raw_candidate) if raw_candidate is not None else ("", None)
    if policy_mode == "filt_only_strict":
        return ("filt/filt", filt_candidate) if filt_candidate is not None else ("", None)
    raise ValueError(f"Unsupported policy_mode: {policy_mode}")


def build_source_policy_challenge(
    point_reconciliation: pd.DataFrame,
    model_results: dict[str, pd.DataFrame],
    config: Any,
    source_selection_audit_detail: pd.DataFrame | None = None,
) -> dict[str, pd.DataFrame]:
    """Compare current mixed vs raw-first/raw-only/filt-only under fixed family and zero-mode."""

    point_reconciliation = point_reconciliation.copy()
    scores = model_results.get("scores", pd.DataFrame()).copy()
    residuals = model_results.get("residuals", pd.DataFrame()).copy()
    selection = model_results.get("selection", pd.DataFrame()).copy()
    source_selection_audit_detail = (
        source_selection_audit_detail.copy() if source_selection_audit_detail is not None else pd.DataFrame()
    )
    score_metric = _score_metric_name(config)
    if point_reconciliation.empty or scores.empty or residuals.empty or selection.empty:
        empty = pd.DataFrame()
        return {"detail": empty, "summary": empty, "conclusions": empty}

    point_reconciliation["target_ppm"] = pd.to_numeric(point_reconciliation["target_ppm"], errors="coerce")
    point_reconciliation["old_error"] = pd.to_numeric(point_reconciliation["old_error"], errors="coerce")
    point_reconciliation["new_error"] = pd.to_numeric(point_reconciliation["new_error"], errors="coerce")

    detail_rows: list[dict[str, Any]] = []
    policy_frames: dict[str, list[pd.DataFrame]] = {
        "current_deployable_mixed": [],
        "raw_first_with_fallback": [],
        "raw_only_strict": [],
        "filt_only_strict": [],
    }

    for fixed_row in selection.sort_values(["analyzer_id"], ignore_index=True).to_dict(orient="records"):
        analyzer_id = str(fixed_row.get("analyzer_id") or "")
        analyzer_points = point_reconciliation[point_reconciliation["analyzer_id"].astype(str) == analyzer_id].copy()
        if analyzer_points.empty:
            continue
        fixed_model_family = str(fixed_row.get("best_model_family") or "")
        fixed_zero_residual_mode = str(fixed_row.get("zero_residual_mode") or "none")
        fixed_prediction_scope = str(fixed_row.get("selected_prediction_scope") or "validation_oof")
        mode2_semantic_profile = _first_text(analyzer_points, "mode2_semantic_profile", default="mode2_semantics_unknown")

        candidate_lookup = {
            "raw": _policy_candidate_row(scores, analyzer_id, "raw/raw", fixed_model_family, fixed_zero_residual_mode, fixed_prediction_scope, score_metric),
            "filt": _policy_candidate_row(scores, analyzer_id, "filt/filt", fixed_model_family, fixed_zero_residual_mode, fixed_prediction_scope, score_metric),
        }

        for policy_mode in ("current_deployable_mixed", "raw_first_with_fallback", "raw_only_strict", "filt_only_strict"):
            selected_source_pair_under_policy, candidate_row = _policy_choice_for_mode(policy_mode, pd.Series(fixed_row), candidate_lookup)
            if policy_mode == "current_deployable_mixed":
                merged = analyzer_points.copy()
                merged["policy_pred_ppm"] = pd.to_numeric(merged["new_pred_ppm"], errors="coerce")
                merged["policy_error_ppm"] = pd.to_numeric(merged["new_error"], errors="coerce")
                chosen_model_id = str(fixed_row.get("best_absorbance_model") or "")
            elif candidate_row is not None:
                prediction_frame = _policy_prediction_frame(residuals, analyzer_id, candidate_row, fixed_prediction_scope)
                merged = analyzer_points.merge(
                    prediction_frame,
                    on=["analyzer_id", "point_title", "point_row", "target_ppm"],
                    how="inner",
                )
                chosen_model_id = str(candidate_row.get("model_id") or "")
            else:
                merged = analyzer_points.iloc[0:0].copy()
                chosen_model_id = ""

            if not merged.empty:
                merged["segment_tag"] = np.select(
                    [
                        pd.to_numeric(merged["target_ppm"], errors="coerce") == 0.0,
                        (pd.to_numeric(merged["target_ppm"], errors="coerce") > 0.0) & (pd.to_numeric(merged["target_ppm"], errors="coerce") <= 200.0),
                        (pd.to_numeric(merged["target_ppm"], errors="coerce") > 200.0) & (pd.to_numeric(merged["target_ppm"], errors="coerce") <= 1000.0),
                    ],
                    ["zero", "low", "main"],
                    default="other",
                )
                merged["abs_error_old"] = pd.to_numeric(merged["old_error"], errors="coerce").abs()
                merged["abs_error_current_mixed"] = pd.to_numeric(merged["new_error"], errors="coerce").abs()
                merged["abs_error_policy"] = pd.to_numeric(merged["policy_error_ppm"], errors="coerce").abs()
                merged["policy_vs_current_improvement"] = merged["abs_error_current_mixed"] - merged["abs_error_policy"]
                policy_frames[policy_mode].append(
                    merged[
                        [
                            "analyzer_id",
                            "point_title",
                            "point_row",
                            "temp_c",
                            "target_ppm",
                            "old_error",
                            "new_error",
                            "policy_error_ppm",
                            "policy_pred_ppm",
                            "segment_tag",
                        ]
                    ].assign(source_policy_mode=policy_mode, selected_source_pair_under_policy=selected_source_pair_under_policy)
                )

            overall_rmse = _metrics(merged["policy_error_ppm"])["rmse"] if not merged.empty else math.nan
            zero_rmse = _segment_rmse(merged, "policy_error_ppm", "zero") if not merged.empty else math.nan
            low_rmse = _segment_rmse(merged, "policy_error_ppm", "low") if not merged.empty else math.nan
            main_rmse = _segment_rmse(merged, "policy_error_ppm", "main") if not merged.empty else math.nan
            current_overall_rmse = _metrics(merged["new_error"])["rmse"] if not merged.empty else _metrics(analyzer_points["new_error"])["rmse"]
            current_zero_rmse = _segment_rmse(merged if not merged.empty else analyzer_points, "new_error", "zero")
            current_low_rmse = _segment_rmse(merged if not merged.empty else analyzer_points, "new_error", "low")
            current_main_rmse = _segment_rmse(merged if not merged.empty else analyzer_points, "new_error", "main")
            old_overall_rmse = _metrics(merged["old_error"])["rmse"] if not merged.empty else _metrics(analyzer_points["old_error"])["rmse"]

            local_win_examples = ""
            local_loss_examples = ""
            if not merged.empty:
                local_win_examples = _format_example_series(
                    merged[merged["policy_vs_current_improvement"] > 1.0e-12],
                    "policy_vs_current_improvement",
                )
                local_loss_examples = _format_example_series(
                    merged[merged["policy_vs_current_improvement"] < -1.0e-12].assign(policy_vs_current_loss=lambda df: -df["policy_vs_current_improvement"]),
                    "policy_vs_current_loss",
                )

            detail_rows.append(
                {
                    "analyzer_id": analyzer_id,
                    "mode2_semantic_profile": mode2_semantic_profile,
                    "fixed_model_family": fixed_model_family,
                    "fixed_zero_residual_mode": fixed_zero_residual_mode,
                    "fixed_prediction_scope": fixed_prediction_scope,
                    "fixed_water_lineage_mode": "none",
                    "source_policy_mode": policy_mode,
                    "selected_source_pair_under_policy": selected_source_pair_under_policy,
                    "overall_rmse": overall_rmse,
                    "zero_rmse": zero_rmse,
                    "low_range_rmse": low_rmse,
                    "main_range_rmse": main_rmse,
                    "old_chain_overall_rmse": old_overall_rmse,
                    "gap_to_old_overall": overall_rmse - old_overall_rmse if pd.notna(overall_rmse) and pd.notna(old_overall_rmse) else math.nan,
                    "delta_vs_current_mixed_overall": current_overall_rmse - overall_rmse if pd.notna(current_overall_rmse) and pd.notna(overall_rmse) else math.nan,
                    "delta_vs_current_mixed_zero": current_zero_rmse - zero_rmse if pd.notna(current_zero_rmse) and pd.notna(zero_rmse) else math.nan,
                    "delta_vs_current_mixed_low": current_low_rmse - low_rmse if pd.notna(current_low_rmse) and pd.notna(low_rmse) else math.nan,
                    "delta_vs_current_mixed_main": current_main_rmse - main_rmse if pd.notna(current_main_rmse) and pd.notna(main_rmse) else math.nan,
                    "pointwise_win_count_vs_old": int((merged.get("abs_error_policy", pd.Series(dtype=float)) < merged.get("abs_error_old", pd.Series(dtype=float)) - 1.0e-12).fillna(False).sum()) if not merged.empty else 0,
                    "pointwise_loss_count_vs_old": int((merged.get("abs_error_policy", pd.Series(dtype=float)) > merged.get("abs_error_old", pd.Series(dtype=float)) + 1.0e-12).fillna(False).sum()) if not merged.empty else 0,
                    "pointwise_win_count_vs_current_mixed": int((merged.get("abs_error_policy", pd.Series(dtype=float)) < merged.get("abs_error_current_mixed", pd.Series(dtype=float)) - 1.0e-12).fillna(False).sum()) if not merged.empty else 0,
                    "pointwise_loss_count_vs_current_mixed": int((merged.get("abs_error_policy", pd.Series(dtype=float)) > merged.get("abs_error_current_mixed", pd.Series(dtype=float)) + 1.0e-12).fillna(False).sum()) if not merged.empty else 0,
                    "local_win_examples": local_win_examples,
                    "local_loss_examples": local_loss_examples,
                    "chosen_model_id_under_policy": chosen_model_id,
                    "score_metric_used": score_metric,
                }
            )

    detail_df = pd.DataFrame(detail_rows).sort_values(["analyzer_id", "source_policy_mode"], ignore_index=True) if detail_rows else pd.DataFrame()
    if detail_df.empty:
        empty = pd.DataFrame()
        return {"detail": empty, "summary": empty, "conclusions": empty}

    summary_rows: list[dict[str, Any]] = []
    current_frame = pd.concat(policy_frames["current_deployable_mixed"], ignore_index=True) if policy_frames["current_deployable_mixed"] else pd.DataFrame()
    current_overall_rmse = _metrics(current_frame["policy_error_ppm"])["rmse"] if not current_frame.empty else math.nan
    current_zero_rmse = _segment_rmse(current_frame, "policy_error_ppm", "zero") if not current_frame.empty else math.nan
    current_low_rmse = _segment_rmse(current_frame, "policy_error_ppm", "low") if not current_frame.empty else math.nan
    current_main_rmse = _segment_rmse(current_frame, "policy_error_ppm", "main") if not current_frame.empty else math.nan

    for policy_mode, frames in policy_frames.items():
        combined = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
        overall_rmse = _metrics(combined["policy_error_ppm"])["rmse"] if not combined.empty else math.nan
        zero_rmse = _segment_rmse(combined, "policy_error_ppm", "zero") if not combined.empty else math.nan
        low_rmse = _segment_rmse(combined, "policy_error_ppm", "low") if not combined.empty else math.nan
        main_rmse = _segment_rmse(combined, "policy_error_ppm", "main") if not combined.empty else math.nan
        old_overall_rmse = _metrics(combined["old_error"])["rmse"] if not combined.empty else math.nan
        detail_subset = detail_df[detail_df["source_policy_mode"] == policy_mode]
        summary_rows.append(
            {
                "source_policy_mode": policy_mode,
                "fixed_water_lineage_mode": "none",
                "overall_rmse": overall_rmse,
                "zero_rmse": zero_rmse,
                "low_range_rmse": low_rmse,
                "main_range_rmse": main_rmse,
                "old_chain_overall_rmse": old_overall_rmse,
                "delta_vs_current_mixed_overall": current_overall_rmse - overall_rmse if pd.notna(current_overall_rmse) and pd.notna(overall_rmse) else math.nan,
                "delta_vs_current_mixed_zero": current_zero_rmse - zero_rmse if pd.notna(current_zero_rmse) and pd.notna(zero_rmse) else math.nan,
                "delta_vs_current_mixed_low": current_low_rmse - low_rmse if pd.notna(current_low_rmse) and pd.notna(low_rmse) else math.nan,
                "delta_vs_current_mixed_main": current_main_rmse - main_rmse if pd.notna(current_main_rmse) and pd.notna(main_rmse) else math.nan,
                "analyzers_covered_count": int(detail_subset["selected_source_pair_under_policy"].astype(str).ne("").sum()),
                "analyzers_beating_current_mixed_count": int((pd.to_numeric(detail_subset["delta_vs_current_mixed_overall"], errors="coerce") > 1.0e-12).fillna(False).sum()),
                "analyzers_beating_old_count": int((pd.to_numeric(detail_subset["gap_to_old_overall"], errors="coerce") < -1.0e-12).fillna(False).sum()),
                "whether_improves_current_deployable_result": bool(pd.notna(overall_rmse) and pd.notna(current_overall_rmse) and overall_rmse < current_overall_rmse - 1.0e-12),
                "score_metric_used": score_metric,
            }
        )
    summary_df = pd.DataFrame(summary_rows).sort_values(["source_policy_mode"], ignore_index=True)

    raw_first_row = summary_df[summary_df["source_policy_mode"] == "raw_first_with_fallback"].iloc[0]
    current_row = summary_df[summary_df["source_policy_mode"] == "current_deployable_mixed"].iloc[0]
    raw_first_improves = bool(raw_first_row["whether_improves_current_deployable_result"])

    audit_lookup = (
        source_selection_audit_detail.set_index("analyzer_id").to_dict(orient="index")
        if not source_selection_audit_detail.empty and "analyzer_id" in source_selection_audit_detail.columns
        else {}
    )
    current_reason_parts: list[str] = []
    current_reason_evidence: list[str] = []
    analyzer_alignment_parts: list[str] = []
    for analyzer_id, analyzer_df in detail_df.groupby("analyzer_id", dropna=False):
        current_choice = analyzer_df[analyzer_df["source_policy_mode"] == "current_deployable_mixed"].iloc[0]
        raw_first_choice = analyzer_df[analyzer_df["source_policy_mode"] == "raw_first_with_fallback"].iloc[0]
        raw_only_choice = analyzer_df[analyzer_df["source_policy_mode"] == "raw_only_strict"].iloc[0]
        filt_only_choice = analyzer_df[analyzer_df["source_policy_mode"] == "filt_only_strict"].iloc[0]
        if str(current_choice["selected_source_pair_under_policy"]) == "raw/raw":
            analyzer_alignment_parts.append(f"{analyzer_id}=closest_to_raw_first")
        elif str(raw_first_choice["selected_source_pair_under_policy"]) == "raw/raw":
            analyzer_alignment_parts.append(f"{analyzer_id}=can_return_to_raw_with_fallback")
        else:
            analyzer_alignment_parts.append(f"{analyzer_id}=still_depends_on_filt")

        if str(current_choice["selected_source_pair_under_policy"]) != "filt/filt":
            continue

        audit_row = audit_lookup.get(str(analyzer_id), {})
        raw_blocked = any(
            [
                not bool(audit_row.get("raw_available_flag", True)),
                not bool(audit_row.get("raw_required_keys_pass", True)),
                not bool(audit_row.get("raw_quality_gate_pass", True)),
            ]
        )
        raw_only_rmse = pd.to_numeric(pd.Series([raw_only_choice.get("overall_rmse")]), errors="coerce").iloc[0]
        filt_only_rmse = pd.to_numeric(pd.Series([filt_only_choice.get("overall_rmse")]), errors="coerce").iloc[0]
        if raw_blocked or pd.isna(raw_only_rmse) or str(raw_only_choice.get("selected_source_pair_under_policy") or "") == "":
            current_reason_parts.append(f"{analyzer_id}=raw_unavailable_or_invalid")
            current_reason_evidence.append(
                f"{analyzer_id}: raw_available={audit_row.get('raw_available_flag')}, "
                f"raw_required_keys_pass={audit_row.get('raw_required_keys_pass')}, "
                f"raw_quality_gate_pass={audit_row.get('raw_quality_gate_pass')}"
            )
        elif pd.notna(filt_only_rmse) and float(filt_only_rmse) < float(raw_only_rmse) - 1.0e-12:
            current_reason_parts.append(f"{analyzer_id}=filt_better_in_fixed_family")
            current_reason_evidence.append(
                f"{analyzer_id}: raw_only_rmse={float(raw_only_rmse):.6g}, filt_only_rmse={float(filt_only_rmse):.6g}"
            )
        else:
            current_reason_parts.append(f"{analyzer_id}=mixed_not_fully_explained_by_fixed_family_advantage")
            current_reason_evidence.append(
                f"{analyzer_id}: raw_only_rmse={raw_only_rmse}, filt_only_rmse={filt_only_rmse}"
            )

    current_mixed_reason_answer = (
        "; ".join(current_reason_parts)
        if current_reason_parts
        else "current deployable selection does not rely on filt/filt under the fixed-family source policy challenge"
    )
    current_mixed_reason_evidence = (
        "; ".join(current_reason_evidence)
        if current_reason_evidence
        else "no filt/filt analyzers were present in current_deployable_mixed"
    )

    support_statement_answer = (
        "Yes. Current new chain is inconsistent with V5 raw-first intent, and whether that mismatch explains the old-chain gap must be checked with the fixed-chain source policy challenge."
    )
    sufficiency_answer = (
        "source policy mismatch is not sufficient to explain the remaining gap"
        if not raw_first_improves
        else "source policy mismatch contributes materially and raw-first improves the current mixed result"
    )

    conclusions_df = pd.DataFrame(
        [
            {
                "question_id": "current_mixed_reason_category",
                "question": "Did current mixed happen because raw was unavailable/invalid or because filt scored better?",
                "answer": current_mixed_reason_answer,
                "evidence": current_mixed_reason_evidence,
            },
            {
                "question_id": "whether_raw_first_improves_current_deployable_result",
                "question": "Does raw_first_with_fallback improve current_deployable_mixed overall?",
                "answer": "raw_first_with_fallback improves current_deployable_mixed overall" if raw_first_improves else "raw_first_with_fallback degrades or does not improve current_deployable_mixed overall",
                "evidence": (
                    f"current_mixed overall_rmse={current_row['overall_rmse']}, "
                    f"raw_first_with_fallback overall_rmse={raw_first_row['overall_rmse']}, "
                    f"delta_vs_current_mixed_overall={raw_first_row['delta_vs_current_mixed_overall']}"
                ),
            },
            {
                "question_id": "raw_first_design_intent_alignment",
                "question": "Which analyzers align with raw-first intent and which depend on filt?",
                "answer": "; ".join(analyzer_alignment_parts),
                "evidence": "; ".join(
                    f"{row.analyzer_id}:current={row.selected_source_pair_under_policy}"
                    for row in detail_df[detail_df["source_policy_mode"] == "current_deployable_mixed"].itertuples(index=False)
                ),
            },
            {
                "question_id": "statement_support_on_source_policy_mismatch",
                "question": "Is the source-policy mismatch statement supported?",
                "answer": support_statement_answer,
                "evidence": (
                    "designed_v5_ratio_source_intent=raw_or_instantaneous; "
                    "actual_ratio_source_used_in_this_run comes from current_deployable_mixed selections; "
                    "fixed-chain source policy challenge compares current mixed vs raw-first."
                ),
            },
            {
                "question_id": "whether_source_policy_mismatch_is_sufficient_explanation",
                "question": "Is source policy mismatch sufficient to explain the remaining gap to old_chain?",
                "answer": sufficiency_answer,
                "evidence": (
                    f"raw_first_with_fallback overall_rmse={raw_first_row['overall_rmse']}, "
                    f"old_chain_overall_rmse={raw_first_row['old_chain_overall_rmse']}"
                ),
            },
        ]
    )

    return {
        "detail": detail_df,
        "summary": summary_df,
        "conclusions": conclusions_df,
    }
