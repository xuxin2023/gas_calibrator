"""Remaining-gap decomposition for current deployable new chain vs old_chain."""

from __future__ import annotations

import json
import math
from typing import Any

import numpy as np
import pandas as pd


def _metrics(errors: pd.Series | np.ndarray) -> dict[str, float]:
    clean = pd.to_numeric(pd.Series(errors), errors="coerce").dropna()
    if clean.empty:
        return {"rmse": math.nan, "mae": math.nan}
    values = clean.to_numpy(dtype=float)
    return {
        "rmse": float(np.sqrt(np.mean(np.square(values)))),
        "mae": float(np.mean(np.abs(values))),
    }


def _segment_tag(series: pd.Series) -> pd.Series:
    target = pd.to_numeric(series, errors="coerce")
    return pd.Series(
        np.select(
            [
                target == 0.0,
                (target > 0.0) & (target <= 200.0),
                (target > 200.0) & (target <= 1000.0),
            ],
            ["zero", "low", "main"],
            default="other",
        ),
        index=series.index,
    )


def _segment_mask(frame: pd.DataFrame, segment_tag: str) -> pd.Series:
    target = pd.to_numeric(frame["target_ppm"], errors="coerce")
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


def _positive_gap(series: pd.Series) -> pd.Series:
    numeric = pd.to_numeric(series, errors="coerce").fillna(0.0)
    return numeric.clip(lower=0.0)


def remaining_gap_total_from_frame(frame: pd.DataFrame, *, error_column: str = "new_error") -> float:
    """Return the positive squared-error excess total vs old_chain."""

    work = frame.copy()
    work["old_error"] = pd.to_numeric(work["old_error"], errors="coerce")
    work[error_column] = pd.to_numeric(work[error_column], errors="coerce")
    excess = np.square(work[error_column]) - np.square(work["old_error"])
    return float(_positive_gap(pd.Series(excess, index=work.index)).sum())


def _json_map(series: pd.Series) -> str:
    clean = pd.to_numeric(series, errors="coerce").dropna()
    payload = {str(index): float(value) for index, value in clean.items()}
    return json.dumps(payload, ensure_ascii=True, sort_keys=True)


def _share_json(frame: pd.DataFrame, group_column: str, value_column: str, total_value: float) -> str:
    if frame.empty or total_value <= 0.0:
        return json.dumps({}, ensure_ascii=True)
    grouped = (
        frame.groupby(group_column, dropna=False)[value_column]
        .sum(min_count=1)
        .sort_values(ascending=False)
    )
    shares = grouped.map(lambda value: float(value) / float(total_value) if pd.notna(value) else math.nan)
    return _json_map(shares)


def _point_label(row: pd.Series) -> str:
    analyzer = str(row.get("analyzer_id") or "")
    temp_value = pd.to_numeric(pd.Series([row.get("temp_set_c")]), errors="coerce").iloc[0]
    target_value = pd.to_numeric(pd.Series([row.get("target_ppm")]), errors="coerce").iloc[0]
    segment = str(row.get("segment_tag") or "")
    temp_text = "nanC" if pd.isna(temp_value) else f"{float(temp_value):g}C"
    target_text = "nanppm" if pd.isna(target_value) else f"{float(target_value):g}ppm"
    return f"{analyzer}@{temp_text}/{target_text}/{segment}"


def _top_contributors_text(frame: pd.DataFrame, limit: int = 10) -> str:
    if frame.empty:
        return ""
    ranked = frame[frame["contributes_to_remaining_gap_flag"]].copy()
    if ranked.empty:
        return ""
    ranked = ranked.sort_values(
        ["excess_squared_error_vs_old", "analyzer_id", "temp_set_c", "target_ppm"],
        ascending=[False, True, True, True],
        ignore_index=True,
    ).head(limit)
    return "; ".join(_point_label(row) for _, row in ranked.iterrows())


def _ga03_freeze_answer(frame: pd.DataFrame) -> tuple[str, str]:
    ga03 = frame[frame["analyzer_id"].astype(str) == "GA03"].copy()
    if ga03.empty:
        return "ga03_not_present", "GA03 rows were not present in the current run."
    overall_old = _metrics(ga03["old_error"])["rmse"]
    overall_new = _metrics(ga03["new_error"])["rmse"]
    low_old = _segment_rmse(ga03, "old_error", "low")
    low_new = _segment_rmse(ga03, "new_error", "low")
    main_old = _segment_rmse(ga03, "old_error", "main")
    main_new = _segment_rmse(ga03, "new_error", "main")
    if (
        pd.notna(overall_old)
        and pd.notna(overall_new)
        and overall_new < overall_old - 1.0e-12
        and pd.notna(low_old)
        and pd.notna(low_new)
        and low_new < low_old - 1.0e-12
        and pd.notna(main_old)
        and pd.notna(main_new)
        and main_new < main_old - 1.0e-12
    ):
        return (
            "yes_freeze_as_local_winner",
            f"GA03 overall/low/main RMSE={overall_new:.6g}/{low_new:.6g}/{main_new:.6g} vs old={overall_old:.6g}/{low_old:.6g}/{main_old:.6g}",
        )
    return (
        "not_yet_freeze",
        f"GA03 overall/low/main RMSE={overall_new:.6g}/{low_new:.6g}/{main_new:.6g} vs old={overall_old:.6g}/{low_old:.6g}/{main_old:.6g}",
    )


def build_remaining_gap_decomposition(point_reconciliation: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """Decompose where current deployable new_chain still lags old_chain."""

    compare = point_reconciliation.copy()
    if compare.empty:
        empty = pd.DataFrame()
        return {"detail": empty, "summary": empty, "conclusions": empty}

    compare["target_ppm"] = pd.to_numeric(compare["target_ppm"], errors="coerce")
    compare["temp_set_c"] = pd.to_numeric(compare.get("temp_c"), errors="coerce")
    compare["old_error"] = pd.to_numeric(compare["old_error"], errors="coerce")
    compare["new_error"] = pd.to_numeric(compare["new_error"], errors="coerce")
    compare["old_abs_error"] = compare["old_error"].abs()
    compare["new_abs_error"] = compare["new_error"].abs()
    compare["excess_abs_error_vs_old"] = compare["new_abs_error"] - compare["old_abs_error"]
    compare["excess_squared_error_vs_old"] = np.square(compare["new_error"]) - np.square(compare["old_error"])
    compare["contributes_to_remaining_gap_flag"] = compare["excess_squared_error_vs_old"] > 1.0e-12
    compare["segment_tag"] = _segment_tag(compare["target_ppm"])
    compare["fixed_model_family"] = compare.get("best_model_family", pd.Series("", index=compare.index)).fillna("").astype(str)
    compare["fixed_zero_residual_mode"] = compare.get("zero_residual_mode", pd.Series("", index=compare.index)).fillna("").astype(str)
    compare["fixed_prediction_scope"] = compare.get("selected_prediction_scope", pd.Series("", index=compare.index)).fillna("").astype(str)
    compare["selected_source_pair"] = compare.get("selected_source_pair", pd.Series("", index=compare.index)).fillna("").astype(str)

    detail_df = compare[
        [
            "analyzer_id",
            "mode2_semantic_profile",
            "temp_set_c",
            "target_ppm",
            "segment_tag",
            "old_abs_error",
            "new_abs_error",
            "excess_abs_error_vs_old",
            "excess_squared_error_vs_old",
            "contributes_to_remaining_gap_flag",
            "selected_source_pair",
            "fixed_model_family",
            "fixed_zero_residual_mode",
            "fixed_prediction_scope",
            "point_title",
            "point_row",
            "old_error",
            "new_error",
        ]
    ].copy()
    detail_df["contribution_rank_global"] = math.nan
    detail_df["contribution_rank_within_analyzer"] = math.nan

    contributors = detail_df[detail_df["contributes_to_remaining_gap_flag"]].copy()
    if not contributors.empty:
        contributors = contributors.sort_values(
            ["excess_squared_error_vs_old", "analyzer_id", "temp_set_c", "target_ppm"],
            ascending=[False, True, True, True],
            ignore_index=False,
        )
        detail_df.loc[contributors.index, "contribution_rank_global"] = np.arange(1, len(contributors) + 1, dtype=float)
        for _, subset in contributors.groupby("analyzer_id", dropna=False):
            detail_df.loc[subset.index, "contribution_rank_within_analyzer"] = np.arange(1, len(subset) + 1, dtype=float)

    positive_gap = detail_df[detail_df["contributes_to_remaining_gap_flag"]].copy()
    remaining_gap_total = float(_positive_gap(positive_gap["excess_squared_error_vs_old"]).sum())
    by_analyzer = (
        positive_gap.groupby("analyzer_id", dropna=False)["excess_squared_error_vs_old"]
        .sum(min_count=1)
        .sort_values(ascending=False)
    )
    by_segment = (
        positive_gap.groupby("segment_tag", dropna=False)["excess_squared_error_vs_old"]
        .sum(min_count=1)
        .sort_values(ascending=False)
    )
    by_temp = (
        positive_gap.groupby("temp_set_c", dropna=False)["excess_squared_error_vs_old"]
        .sum(min_count=1)
        .sort_values(ascending=False)
    )
    by_target = (
        positive_gap.groupby("target_ppm", dropna=False)["excess_squared_error_vs_old"]
        .sum(min_count=1)
        .sort_values(ascending=False)
    )

    laggard_analyzer_primary = str(by_analyzer.index[0]) if not by_analyzer.empty else ""
    laggard_segment_primary = str(by_segment.index[0]) if not by_segment.empty else ""
    ga01_gap = float(by_analyzer.get("GA01", 0.0))
    ga02_gap = float(by_analyzer.get("GA02", 0.0))
    ga03_gap = float(by_analyzer.get("GA03", 0.0))
    ga02_subset = positive_gap[positive_gap["analyzer_id"].astype(str) == "GA02"].copy()
    ga02_zero_gap = float(ga02_subset.loc[ga02_subset["segment_tag"] == "zero", "excess_squared_error_vs_old"].sum())
    ga02_low_main_gap = float(ga02_subset.loc[ga02_subset["segment_tag"].isin(["low", "main"]), "excess_squared_error_vs_old"].sum())
    ga03_answer, ga03_evidence = _ga03_freeze_answer(compare)

    summary_df = pd.DataFrame(
        [
            {
                "summary_scope": "overall",
                "remaining_gap_metric": "positive_squared_error_excess_vs_old",
                "remaining_gap_total": remaining_gap_total,
                "remaining_gap_from_ga01": ga01_gap,
                "remaining_gap_from_ga02": ga02_gap,
                "remaining_gap_from_ga03": ga03_gap,
                "remaining_gap_share_by_analyzer": _share_json(positive_gap, "analyzer_id", "excess_squared_error_vs_old", remaining_gap_total),
                "remaining_gap_share_by_segment": _share_json(positive_gap, "segment_tag", "excess_squared_error_vs_old", remaining_gap_total),
                "remaining_gap_share_by_temp": _share_json(positive_gap, "temp_set_c", "excess_squared_error_vs_old", remaining_gap_total),
                "remaining_gap_share_by_target_ppm": _share_json(positive_gap, "target_ppm", "excess_squared_error_vs_old", remaining_gap_total),
                "top_10_gap_contributor_points": _top_contributors_text(detail_df, limit=10),
                "laggard_analyzer_primary": laggard_analyzer_primary,
                "laggard_segment_primary": laggard_segment_primary,
            }
        ]
    )

    ga01_share = (ga01_gap / remaining_gap_total) if remaining_gap_total > 0.0 else math.nan
    switch_to_sidecar = bool(
        laggard_analyzer_primary == "GA01"
        and pd.notna(ga01_share)
        and float(ga01_share) >= 0.5
    )
    conclusions_df = pd.DataFrame(
        [
            {
                "question_id": "remaining_gap_primary_contributor",
                "question": "Is GA01 the primary remaining-gap contributor?",
                "answer": "yes_ga01_primary" if laggard_analyzer_primary == "GA01" else "no_ga01_not_primary",
                "evidence": f"laggard_analyzer_primary={laggard_analyzer_primary}; share_by_analyzer={summary_df.iloc[0]['remaining_gap_share_by_analyzer']}",
                "laggard_analyzer_primary": laggard_analyzer_primary,
                "laggard_segment_primary": laggard_segment_primary,
            },
            {
                "question_id": "ga02_gap_shape",
                "question": "Does GA02 look more like a zero/overall issue or a low/main issue?",
                "answer": "low_main" if ga02_low_main_gap > ga02_zero_gap + 1.0e-12 else "zero_overall",
                "evidence": f"GA02 zero_gap={ga02_zero_gap:.6g}; GA02 low_main_gap={ga02_low_main_gap:.6g}",
                "laggard_analyzer_primary": laggard_analyzer_primary,
                "laggard_segment_primary": laggard_segment_primary,
            },
            {
                "question_id": "ga03_freeze_status",
                "question": "Should GA03 be treated as a frozen local winner?",
                "answer": ga03_answer,
                "evidence": ga03_evidence,
                "laggard_analyzer_primary": laggard_analyzer_primary,
                "laggard_segment_primary": laggard_segment_primary,
            },
            {
                "question_id": "shift_to_analyzer_specific_sidecar",
                "question": "Should follow-up work shift from global challenge to analyzer-specific sidecar work?",
                "answer": "yes_shift_to_analyzer_specific_sidecar" if switch_to_sidecar else "not_yet_shifted",
                "evidence": f"GA01 remaining_gap_share={ga01_share:.4f}" if pd.notna(ga01_share) else "remaining_gap_total_not_positive",
                "laggard_analyzer_primary": laggard_analyzer_primary,
                "laggard_segment_primary": laggard_segment_primary,
            },
        ]
    )

    return {
        "detail": detail_df.drop(columns=["point_title", "point_row", "old_error", "new_error"]),
        "summary": summary_df,
        "conclusions": conclusions_df,
    }
