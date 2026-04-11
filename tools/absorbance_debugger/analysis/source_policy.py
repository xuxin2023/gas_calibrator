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
