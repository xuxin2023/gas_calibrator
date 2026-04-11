"""Old-chain vs new-chain comparison helpers."""

from __future__ import annotations

import math
from typing import Any

import numpy as np
import pandas as pd

from .fits import fit_linear


CONCENTRATION_BUCKETS: tuple[tuple[str, float | None, float | None], ...] = (
    ("0 ppm", 0.0, 0.0),
    ("0~200 ppm", 0.0, 200.0),
    ("200~1000 ppm", 200.0, 1000.0),
    ("200~500 ppm", 200.0, 500.0),
    ("500~800 ppm", 500.0, 800.0),
    ("800~1000 ppm", 800.0, 1000.0),
)

SCOPED_DETAIL_COLUMNS: tuple[str, ...] = (
    "run_id",
    "comparison_scope",
    "analyzer_id",
    "selected_source_pair",
    "actual_ratio_source_used",
    "old_chain_overall_rmse",
    "new_chain_overall_rmse",
    "delta_overall_rmse",
    "improvement_pct_overall",
    "old_chain_zero_rmse",
    "new_chain_zero_rmse",
    "delta_zero_rmse",
    "improvement_pct_zero",
    "old_chain_low_rmse",
    "new_chain_low_rmse",
    "delta_low_rmse",
    "improvement_pct_low",
    "old_chain_main_rmse",
    "new_chain_main_rmse",
    "delta_main_rmse",
    "improvement_pct_main",
    "point_count",
    "pointwise_win_count",
    "pointwise_loss_count",
    "pointwise_tie_count",
    "overall_win_flag",
    "zero_win_flag",
    "low_win_flag",
    "main_win_flag",
)

SCOPED_LOCAL_WINS_COLUMNS: tuple[str, ...] = (
    "run_id",
    "comparison_scope",
    "analyzer_id",
    "point_title",
    "point_row",
    "temp_set_c",
    "target_ppm",
    "target_value",
    "old_value",
    "new_value",
    "abs_error_old",
    "abs_error_new",
    "improvement_abs_error",
    "local_win_flag",
    "local_win_rank_within_analyzer",
    "local_loss_rank_within_analyzer",
    "segment_tag",
)

SCOPED_SUMMARY_COLUMNS: tuple[str, ...] = (
    "comparison_scope",
    "run_scope_description",
    "analyzer_set",
    "overall_verdict_scoped",
    "overall_rmse_old_scoped",
    "overall_rmse_new_scoped",
    "overall_improvement_pct_scoped",
    "zero_improvement_pct_scoped",
    "low_improvement_pct_scoped",
    "main_improvement_pct_scoped",
    "analyzer_overall_wins",
    "analyzer_zero_wins",
    "analyzer_low_wins",
    "analyzer_main_wins",
    "total_pointwise_wins",
    "total_pointwise_losses",
    "analyzers_fully_beating_old_count",
    "analyzers_partially_beating_old_count",
    "analyzers_still_lagging_count",
    "headline_safe_statement",
    "scope_limitation_statement",
)

SCOPED_SEGMENT_COLUMNS: tuple[str, ...] = (
    "comparison_scope",
    "segment_tag",
    "old_chain_rmse",
    "new_chain_rmse",
    "delta_rmse",
    "improvement_pct",
    "point_count",
    "win_flag",
)

COMPARISON_RECONCILIATION_COLUMNS: tuple[str, ...] = (
    "comparison_scope",
    "scope_label",
    "run_scope_description",
    "benchmark_chain",
    "old_value_source",
    "new_value_source",
    "selected_source_pair",
    "matched_only_filter_applied",
    "valid_only_filter_applied",
    "hard_exclude_500hpa_applied",
    "analyzer_inclusion_scope",
    "point_count_used",
    "whether_point_table_mean_was_used",
    "whether_deployable_chain_output_was_used",
    "run_ids_in_scope",
    "analyzer_set",
    "overall_verdict_scoped",
    "why_single_package_quick_calc_can_flip_direction",
    "future_external_reporting_rule",
)


def _metrics(errors: pd.Series) -> dict[str, float]:
    clean = pd.to_numeric(errors, errors="coerce").dropna()
    if clean.empty:
        return {
            "rmse": math.nan,
            "mae": math.nan,
            "max_abs_error": math.nan,
            "bias": math.nan,
            "std": math.nan,
        }
    values = clean.to_numpy(dtype=float)
    abs_values = np.abs(values)
    return {
        "rmse": float(np.sqrt(np.mean(np.square(values)))),
        "mae": float(np.mean(abs_values)),
        "max_abs_error": float(np.max(abs_values)),
        "bias": float(np.mean(values)),
        "std": float(np.std(values)),
    }


def _winner(old_value: float, new_value: float) -> str:
    if math.isnan(old_value) and math.isnan(new_value):
        return "tie"
    if math.isnan(old_value):
        return "new_chain"
    if math.isnan(new_value):
        return "old_chain"
    if abs(old_value - new_value) <= 1.0e-12:
        return "tie"
    return "old_chain" if old_value < new_value else "new_chain"


def _winner_from_metric_rows(old_metrics: dict[str, float], new_metrics: dict[str, float]) -> str:
    for key in ("rmse", "mae", "max_abs_error", "std"):
        winner = _winner(old_metrics[key], new_metrics[key])
        if winner != "tie":
            return winner
    return "tie"


def _stability_score(by_temp_subset: pd.DataFrame, column_name: str) -> float:
    values = pd.to_numeric(by_temp_subset[column_name], errors="coerce").dropna()
    if values.empty:
        return math.nan
    return float(np.std(values.to_numpy(dtype=float)))


def _range_mask(series: pd.Series, lower: float | None, upper: float | None) -> pd.Series:
    if lower == 0.0 and upper == 0.0:
        return series == 0.0
    mask = pd.Series(True, index=series.index)
    if lower is not None:
        mask &= series > lower
    if upper is not None:
        mask &= series <= upper
    return mask


def _regression_rows(frame: pd.DataFrame, analyzer_id: str, scope: str, temp_c: float | None) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for chain_name, pred_col in (("old_chain", "old_pred_ppm"), ("new_chain", "new_pred_ppm")):
        subset = frame.dropna(subset=["target_ppm", pred_col])
        if len(subset) < 2:
            continue
        fit = fit_linear(subset["target_ppm"], subset[pred_col])
        rows.append(
            {
                "analyzer_id": analyzer_id,
                "chain_name": chain_name,
                "scope": scope,
                "temp_c": temp_c,
                "sample_count": fit.sample_count,
                "slope": fit.slope,
                "intercept": fit.intercept,
                "r2": fit.r2,
                "formula": fit.formula(x_name="target_ppm"),
            }
        )
    return rows


def build_comparison_outputs(
    point_reconciliation: pd.DataFrame,
    selection_table: pd.DataFrame | None = None,
) -> dict[str, pd.DataFrame | list[str]]:
    """Build step-08 comparison tables and automatic conclusions."""

    compare = point_reconciliation.copy()
    compare["target_ppm"] = pd.to_numeric(compare["target_ppm"], errors="coerce")
    compare["temp_c"] = pd.to_numeric(compare["temp_c"], errors="coerce")
    compare["old_error"] = pd.to_numeric(compare["old_error"], errors="coerce")
    compare["new_error"] = pd.to_numeric(compare["new_error"], errors="coerce")

    overview_rows: list[dict[str, Any]] = []
    by_temp_rows: list[dict[str, Any]] = []
    by_range_rows: list[dict[str, Any]] = []
    zero_rows: list[dict[str, Any]] = []
    regression_overall_rows: list[dict[str, Any]] = []
    regression_by_temp_rows: list[dict[str, Any]] = []
    conclusion_lines: list[str] = []
    auto_conclusion_rows: list[dict[str, Any]] = []
    selection_table = selection_table.copy() if selection_table is not None else pd.DataFrame()

    for analyzer_id, analyzer_df in compare.groupby("analyzer_id"):
        selection_row = (
            selection_table[selection_table["analyzer_id"] == analyzer_id].iloc[0]
            if not selection_table.empty and (selection_table["analyzer_id"] == analyzer_id).any()
            else None
        )
        old_metrics = _metrics(analyzer_df["old_error"])
        new_metrics = _metrics(analyzer_df["new_error"])
        winner_overall = _winner_from_metric_rows(old_metrics, new_metrics)

        zero_df = analyzer_df[analyzer_df["target_ppm"] == 0].copy()
        old_zero_metrics = _metrics(zero_df["old_error"])
        new_zero_metrics = _metrics(zero_df["new_error"])
        winner_zero = _winner_from_metric_rows(old_zero_metrics, new_zero_metrics)

        for temp_c, temp_df in analyzer_df.groupby("temp_c"):
            old_temp_metrics = _metrics(temp_df["old_error"])
            new_temp_metrics = _metrics(temp_df["new_error"])
            by_temp_rows.append(
                {
                    "analyzer_id": analyzer_id,
                    "temp_c": temp_c,
                    "old_rmse": old_temp_metrics["rmse"],
                    "new_rmse": new_temp_metrics["rmse"],
                    "old_bias": old_temp_metrics["bias"],
                    "new_bias": new_temp_metrics["bias"],
                    "old_max_abs_error": old_temp_metrics["max_abs_error"],
                    "new_max_abs_error": new_temp_metrics["max_abs_error"],
                    "winner_at_temp": _winner_from_metric_rows(old_temp_metrics, new_temp_metrics),
                }
            )
            temp_zero_df = temp_df[temp_df["target_ppm"] == 0].copy()
            if not temp_zero_df.empty:
                old_temp_zero = _metrics(temp_zero_df["old_error"])
                new_temp_zero = _metrics(temp_zero_df["new_error"])
                zero_rows.append(
                    {
                        "analyzer_id": analyzer_id,
                        "temp_c": temp_c,
                        "old_zero_mean_error": old_temp_zero["bias"],
                        "new_zero_mean_error": new_temp_zero["bias"],
                        "old_zero_std": old_temp_zero["std"],
                        "new_zero_std": new_temp_zero["std"],
                        "old_zero_max_abs_error": old_temp_zero["max_abs_error"],
                        "new_zero_max_abs_error": new_temp_zero["max_abs_error"],
                    }
                )
            regression_by_temp_rows.extend(_regression_rows(temp_df, analyzer_id, "per_temp", temp_c))

        by_temp_df = pd.DataFrame([row for row in by_temp_rows if row["analyzer_id"] == analyzer_id])
        old_temp_stability_score = _stability_score(by_temp_df, "old_bias")
        new_temp_stability_score = _stability_score(by_temp_df, "new_bias")
        winner_temp_stability = _winner(old_temp_stability_score, new_temp_stability_score)

        for bucket_name, lower, upper in CONCENTRATION_BUCKETS:
            bucket_df = analyzer_df[_range_mask(analyzer_df["target_ppm"], lower, upper)].copy()
            old_bucket = _metrics(bucket_df["old_error"])
            new_bucket = _metrics(bucket_df["new_error"])
            by_range_rows.append(
                {
                    "analyzer_id": analyzer_id,
                    "concentration_range": bucket_name,
                    "old_rmse": old_bucket["rmse"],
                    "new_rmse": new_bucket["rmse"],
                    "old_bias": old_bucket["bias"],
                    "new_bias": new_bucket["bias"],
                    "old_max_abs_error": old_bucket["max_abs_error"],
                    "new_max_abs_error": new_bucket["max_abs_error"],
                    "winner_in_range": _winner_from_metric_rows(old_bucket, new_bucket),
                }
            )

        low_df = pd.DataFrame(
            [
                row
                for row in by_range_rows
                if row["analyzer_id"] == analyzer_id and row["concentration_range"] == "0~200 ppm"
            ]
        )
        winner_low_range = "tie"
        if not low_df.empty:
            winner_low_range = str(low_df.iloc[0]["winner_in_range"])

        main_df = pd.DataFrame(
            [
                row
                for row in by_range_rows
                if row["analyzer_id"] == analyzer_id and row["concentration_range"] == "200~1000 ppm"
            ]
        )
        winner_main_range = "tie"
        if not main_df.empty:
            winner_main_range = str(main_df.iloc[0]["winner_in_range"])

        comparison_winners = (winner_overall, winner_zero, winner_temp_stability, winner_low_range, winner_main_range)
        old_wins = sum(item == "old_chain" for item in comparison_winners)
        new_wins = sum(item == "new_chain" for item in comparison_winners)
        if new_wins >= 3:
            recommendation = "Absorbance model is now the stronger offline candidate; move to next-stage offline validation before any production decision."
        elif winner_zero == "new_chain" and winner_overall != "new_chain":
            recommendation = "Absorbance model improved zero drift, but old_chain still wins the broader accuracy benchmark."
        elif winner_temp_stability == "new_chain" and winner_overall != "new_chain":
            recommendation = "Absorbance model improved temperature stability, but old_chain remains the better overall benchmark."
        elif old_wins >= 3:
            recommendation = "old_chain remains production benchmark; keep absorbance model in offline challenge mode."
        else:
            recommendation = "Mixed result; keep old_chain for release decisions and continue absorbance model iteration offline."

        overview_rows.append(
            {
                "analyzer_id": analyzer_id,
                "old_chain_rmse": old_metrics["rmse"],
                "new_chain_rmse": new_metrics["rmse"],
                "old_zero_rmse": old_zero_metrics["rmse"],
                "new_zero_rmse": new_zero_metrics["rmse"],
                "old_temp_stability_metric": old_temp_stability_score,
                "new_temp_stability_metric": new_temp_stability_score,
                "old_chain_mae": old_metrics["mae"],
                "new_chain_mae": new_metrics["mae"],
                "old_chain_max_abs_error": old_metrics["max_abs_error"],
                "new_chain_max_abs_error": new_metrics["max_abs_error"],
                "old_chain_bias": old_metrics["bias"],
                "new_chain_bias": new_metrics["bias"],
                "winner_overall": winner_overall,
                "winner_zero": winner_zero,
                "winner_temp_stability": winner_temp_stability,
                "winner_low_range": winner_low_range,
                "winner_main_range": winner_main_range,
                "best_absorbance_model": selection_row["best_absorbance_model"] if selection_row is not None else "",
                "best_model_family": selection_row["best_model_family"] if selection_row is not None and "best_model_family" in selection_row else "",
                "zero_residual_mode": selection_row["zero_residual_mode"] if selection_row is not None and "zero_residual_mode" in selection_row else "",
                "recommendation": recommendation,
            }
        )
        regression_overall_rows.extend(_regression_rows(analyzer_df, analyzer_id, "overall", None))
        conclusion_lines.append(
            f"{analyzer_id}: best_model={selection_row['best_absorbance_model'] if selection_row is not None else 'n/a'}, "
            f"overall={winner_overall}, zero={winner_zero}, temp stability={winner_temp_stability}, "
            f"low range={winner_low_range}, main range={winner_main_range}. {recommendation}"
        )
        if selection_row is not None:
            auto_conclusion_rows.append(
                {
                    "category": f"best_absorbance_model:{analyzer_id}",
                    "winner": str(selection_row["best_absorbance_model"]),
                    "summary": (
                        f"{analyzer_id} selected {selection_row['best_absorbance_model']} "
                        f"({selection_row['best_absorbance_model_label']}). {selection_row['selection_reason']}"
                    ),
                }
            )

    def _sorted_frame(rows: list[dict[str, Any]], sort_keys: list[str]) -> pd.DataFrame:
        frame = pd.DataFrame(rows)
        return frame.sort_values(sort_keys, ignore_index=True) if not frame.empty else frame

    overview_df = pd.DataFrame(overview_rows)
    by_temp_df = _sorted_frame(by_temp_rows, ["analyzer_id", "temp_c"])
    by_range_df = _sorted_frame(by_range_rows, ["analyzer_id", "concentration_range"])
    zero_df = _sorted_frame(zero_rows, ["analyzer_id", "temp_c"])
    regression_overall_df = _sorted_frame(regression_overall_rows, ["analyzer_id", "chain_name"])
    regression_by_temp_df = _sorted_frame(regression_by_temp_rows, ["analyzer_id", "temp_c", "chain_name"])

    def _category_winner(column_name: str) -> str:
        values = overview_df[column_name].astype(str).tolist() if not overview_df.empty else []
        old_count = values.count("old_chain")
        new_count = values.count("new_chain")
        if old_count == new_count:
            return "tie"
        return "old_chain" if old_count > new_count else "new_chain"

    overall_winner = _category_winner("winner_overall")
    zero_winner = _category_winner("winner_zero")
    temp_winner = _category_winner("winner_temp_stability")
    low_range_winner = _category_winner("winner_low_range")
    main_range_winner = _category_winner("winner_main_range")
    overall_votes = [overall_winner, zero_winner, temp_winner, low_range_winner, main_range_winner]
    if overall_votes.count("new_chain") >= 3:
        top_recommendation = "absorbance model is ready for next-stage offline validation."
    elif zero_winner == "new_chain" and overall_winner != "new_chain":
        top_recommendation = "absorbance model improved but still not production-ready; it only beats old_chain on zero drift."
    elif temp_winner == "new_chain" and overall_winner != "new_chain":
        top_recommendation = "absorbance model now beats old_chain on temperature stability, but not overall."
    elif overall_votes.count("old_chain") >= 3:
        top_recommendation = "old_chain remains production benchmark."
    else:
        top_recommendation = "Results are mixed. Keep old_chain for release decisions and continue offline absorbance validation."

    auto_conclusion_rows.extend(
        [
            {"category": "overall", "winner": overall_winner, "summary": f"Overall winner across analyzers: {overall_winner}"},
            {"category": "0 ppm", "winner": zero_winner, "summary": f"Zero-point winner across analyzers: {zero_winner}"},
            {"category": "temp stability", "winner": temp_winner, "summary": f"Temperature-stability winner across analyzers: {temp_winner}"},
            {"category": "low range", "winner": low_range_winner, "summary": f"Low-range winner across analyzers: {low_range_winner}"},
            {"category": "main range", "winner": main_range_winner, "summary": f"Main-range winner across analyzers: {main_range_winner}"},
            {"category": "recommendation", "winner": "mixed", "summary": top_recommendation},
        ]
    )

    return {
        "overview_summary": overview_df,
        "by_temperature": by_temp_df,
        "by_concentration_range": by_range_df,
        "zero_special": zero_df,
        "regression_overall": regression_overall_df,
        "regression_by_temperature": regression_by_temp_df,
        "point_reconciliation": compare,
        "auto_conclusions": pd.DataFrame(auto_conclusion_rows),
        "conclusion_lines": conclusion_lines,
    }


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


def _delta_new_minus_old(old_value: float, new_value: float) -> float:
    if pd.isna(old_value) or pd.isna(new_value):
        return math.nan
    return float(new_value - old_value)


def _improvement_pct(old_value: float, new_value: float) -> float:
    if pd.isna(old_value) or pd.isna(new_value) or abs(float(old_value)) <= 1.0e-12:
        return math.nan
    return float((float(old_value) - float(new_value)) / float(old_value) * 100.0)


def _win_flag(old_value: float, new_value: float) -> bool:
    if pd.isna(old_value) or pd.isna(new_value):
        return False
    return bool(float(new_value) < float(old_value) - 1.0e-12)


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


def _segment_metrics(frame: pd.DataFrame, segment_tag: str) -> dict[str, float]:
    subset = frame[_segment_mask(frame, segment_tag)].copy()
    old_metrics = _metrics(subset["old_error"])
    new_metrics = _metrics(subset["new_error"])
    return {
        "old_rmse": old_metrics["rmse"],
        "new_rmse": new_metrics["rmse"],
        "delta_rmse": _delta_new_minus_old(old_metrics["rmse"], new_metrics["rmse"]),
        "improvement_pct": _improvement_pct(old_metrics["rmse"], new_metrics["rmse"]),
        "point_count": int(len(subset)),
        "win_flag": _win_flag(old_metrics["rmse"], new_metrics["rmse"]),
    }


def _normalize_actual_ratio_source(value: Any) -> str:
    text = str(value or "").strip().lower()
    if text in {"ratio_co2_raw", "raw/raw", "raw", "r_in=raw", "raw_ratio"}:
        return "raw"
    if text in {"ratio_co2_filt", "filt/filt", "filt", "filtered", "filtered_ratio"}:
        return "filt"
    return "unknown"


def _resolve_analyzer_actual_ratio_source(
    analyzer_id: str,
    analyzer_df: pd.DataFrame,
    selection_table: pd.DataFrame,
    new_chain_input_audit: pd.DataFrame,
) -> tuple[str, str, str]:
    evidence_parts: list[str] = []
    observed_sources: list[str] = []

    selection_row = (
        selection_table[selection_table["analyzer_id"] == analyzer_id].iloc[0]
        if not selection_table.empty and (selection_table["analyzer_id"] == analyzer_id).any()
        else pd.Series(dtype=object)
    )
    if not selection_row.empty:
        source_pair = str(selection_row.get("selected_source_pair") or "")
        selected_ratio_source = str(selection_row.get("selected_ratio_source") or "")
        if source_pair:
            evidence_parts.append(f"selection.selected_source_pair={source_pair}")
            observed_sources.append(_normalize_actual_ratio_source(source_pair))
        if selected_ratio_source:
            evidence_parts.append(f"selection.selected_ratio_source={selected_ratio_source}")
            observed_sources.append(_normalize_actual_ratio_source(selected_ratio_source))

    ratio_source_selected = _first_text(analyzer_df, "ratio_source_selected", default="")
    if ratio_source_selected:
        evidence_parts.append(f"point_reconciliation.ratio_source_selected={ratio_source_selected}")
        observed_sources.append(_normalize_actual_ratio_source(ratio_source_selected))

    selected_source_pair = _first_text(analyzer_df, "selected_source_pair", default="")
    if selected_source_pair and all("selection.selected_source_pair=" not in part for part in evidence_parts):
        evidence_parts.append(f"point_reconciliation.selected_source_pair={selected_source_pair}")
        observed_sources.append(_normalize_actual_ratio_source(selected_source_pair))

    audit_rows = new_chain_input_audit.copy() if isinstance(new_chain_input_audit, pd.DataFrame) else pd.DataFrame()
    if not audit_rows.empty and {"audit_scope", "analyzer_id"} <= set(audit_rows.columns):
        audit_rows = audit_rows[
            (audit_rows["audit_scope"] == "per_analyzer_selected_main_chain")
            & (audit_rows["analyzer_id"].astype(str) == analyzer_id)
        ].copy()
        if not audit_rows.empty:
            audit_row = audit_rows.iloc[0]
            r_in_source = str(audit_row.get("R_in_source") or "")
            r0_fit_source = str(audit_row.get("R0_fit_source") or "")
            if r_in_source:
                evidence_parts.append(f"input_audit.R_in_source={r_in_source}")
                observed_sources.append(_normalize_actual_ratio_source(r_in_source))
            if r0_fit_source:
                evidence_parts.append(f"input_audit.R0_fit_source={r0_fit_source}")
                observed_sources.append(_normalize_actual_ratio_source(r0_fit_source))

    cleaned_sources = sorted({item for item in observed_sources if item in {"raw", "filt"}})
    if not cleaned_sources:
        actual_ratio_source = "unknown"
    elif len(cleaned_sources) == 1:
        actual_ratio_source = cleaned_sources[0]
    else:
        actual_ratio_source = "mixed"

    selected_pair_value = str(selection_row.get("selected_source_pair") or "") if not selection_row.empty else selected_source_pair
    evidence = "; ".join(evidence_parts) if evidence_parts else "no deployable ratio-source evidence found"
    return selected_pair_value, actual_ratio_source, evidence


def _overall_ratio_source_used(detail_df: pd.DataFrame) -> tuple[str, str, str]:
    if detail_df.empty or "actual_ratio_source_used" not in detail_df.columns:
        return "unknown", "unknown", "no per-analyzer ratio-source summary available"

    values = detail_df["actual_ratio_source_used"].fillna("unknown").astype(str).str.strip().tolist()
    raw_count = values.count("raw")
    filt_count = values.count("filt")
    if raw_count == 0 and filt_count == 0:
        actual = "unknown"
        majority = "unknown"
    else:
        used = {value for value in values if value in {"raw", "filt"}}
        actual = next(iter(used)) if len(used) == 1 else "mixed"
        if raw_count > filt_count:
            majority = "raw"
        elif filt_count > raw_count:
            majority = "filt"
        else:
            majority = "mixed"
    evidence = "; ".join(
        f"{row.analyzer_id}={row.actual_ratio_source_used} ({row.selected_source_pair})"
        for row in detail_df.itertuples(index=False)
    )
    return actual, majority, evidence or "no analyzer-level ratio evidence available"


def _count_true(series: pd.Series) -> int:
    if series.empty:
        return 0
    return int(series.fillna(False).map(bool).sum())


def _pick_best_candidate_rows(
    detail_df: pd.DataFrame,
    *,
    candidate_group: str,
    mode_column: str,
    baseline_mode: str,
    label_column: str,
) -> list[dict[str, Any]]:
    if detail_df.empty or mode_column not in detail_df.columns:
        return []

    baseline = detail_df[detail_df[mode_column].astype(str) == baseline_mode][
        ["analyzer_id", "overall_rmse", "old_chain_overall_rmse"]
    ].rename(
        columns={
            "overall_rmse": "deployable_overall_rmse",
            "old_chain_overall_rmse": "baseline_old_chain_overall_rmse",
        }
    )
    rows: list[dict[str, Any]] = []
    for mode_value, subset in detail_df.groupby(mode_column, dropna=False):
        mode_text = str(mode_value)
        if mode_text == baseline_mode:
            continue
        merged = subset.merge(baseline, on="analyzer_id", how="left")
        candidate_mean = float(pd.to_numeric(merged["overall_rmse"], errors="coerce").mean())
        deployable_mean = float(pd.to_numeric(merged["deployable_overall_rmse"], errors="coerce").mean())
        old_column = "old_chain_overall_rmse" if "old_chain_overall_rmse" in merged.columns else "baseline_old_chain_overall_rmse"
        old_mean = float(pd.to_numeric(merged[old_column], errors="coerce").mean())
        rows.append(
            {
                "candidate_group": candidate_group,
                "candidate_id": mode_text,
                "candidate_label": _first_text(subset, label_column, default=mode_text),
                "candidate_scope": "diagnostic_only",
                "candidate_mean_overall_rmse": candidate_mean,
                "deployable_mean_overall_rmse": deployable_mean,
                "old_chain_mean_overall_rmse": old_mean,
                "delta_vs_deployable_mean_rmse": _delta_new_minus_old(deployable_mean, candidate_mean),
                "improvement_vs_deployable_pct": _improvement_pct(deployable_mean, candidate_mean),
                "delta_vs_old_chain_mean_rmse": _delta_new_minus_old(old_mean, candidate_mean),
                "improvement_vs_old_chain_pct": _improvement_pct(old_mean, candidate_mean),
                "analyzers_beating_deployable_count": int(
                    (
                        pd.to_numeric(merged["overall_rmse"], errors="coerce")
                        < pd.to_numeric(merged["deployable_overall_rmse"], errors="coerce")
                    ).fillna(False).sum()
                ),
                "analyzers_beating_old_count": int(
                    (
                        pd.to_numeric(merged["overall_rmse"], errors="coerce")
                        < pd.to_numeric(merged[old_column], errors="coerce")
                    ).fillna(False).sum()
                ),
                "beats_current_deployable": bool(candidate_mean < deployable_mean) if pd.notna(candidate_mean) and pd.notna(deployable_mean) else False,
                "beats_old_chain": bool(candidate_mean < old_mean) if pd.notna(candidate_mean) and pd.notna(old_mean) else False,
                "headline_eligible": False,
                "evidence": "; ".join(
                    f"{row.analyzer_id}={row.overall_rmse:.3f}"
                    for row in merged.itertuples(index=False)
                    if pd.notna(row.overall_rmse)
                ),
            }
        )
    if not rows:
        return []
    ordered = pd.DataFrame(rows).sort_values(
        ["candidate_mean_overall_rmse", "candidate_id"],
        ignore_index=True,
    )
    return ordered.head(1).to_dict(orient="records")


def _water_anchor_candidate_rows(water_anchor_compare: pd.DataFrame) -> list[dict[str, Any]]:
    if water_anchor_compare.empty:
        return []
    candidate_mean = float(pd.to_numeric(water_anchor_compare["water_anchor_overall_rmse"], errors="coerce").mean())
    deployable_mean = float(pd.to_numeric(water_anchor_compare["baseline_overall_rmse"], errors="coerce").mean())
    old_mean = float(pd.to_numeric(water_anchor_compare["old_chain_rmse"], errors="coerce").mean())
    mode_values = sorted(
        {
            str(value)
            for value in water_anchor_compare.get("water_zero_anchor_mode", pd.Series(dtype=str)).dropna().astype(str).tolist()
            if str(value)
        }
    )
    return [
        {
            "candidate_group": "other_diagnostic_only_candidate",
            "candidate_id": "selected_water_zero_anchor_branch",
            "candidate_label": ",".join(mode_values) if mode_values else "selected_water_zero_anchor_branch",
            "candidate_scope": "diagnostic_only",
            "candidate_mean_overall_rmse": candidate_mean,
            "deployable_mean_overall_rmse": deployable_mean,
            "old_chain_mean_overall_rmse": old_mean,
            "delta_vs_deployable_mean_rmse": _delta_new_minus_old(deployable_mean, candidate_mean),
            "improvement_vs_deployable_pct": _improvement_pct(deployable_mean, candidate_mean),
            "delta_vs_old_chain_mean_rmse": _delta_new_minus_old(old_mean, candidate_mean),
            "improvement_vs_old_chain_pct": _improvement_pct(old_mean, candidate_mean),
            "analyzers_beating_deployable_count": int(
                (
                    pd.to_numeric(water_anchor_compare["water_anchor_overall_rmse"], errors="coerce")
                    < pd.to_numeric(water_anchor_compare["baseline_overall_rmse"], errors="coerce")
                ).fillna(False).sum()
            ),
            "analyzers_beating_old_count": int(
                (
                    pd.to_numeric(water_anchor_compare["water_anchor_overall_rmse"], errors="coerce")
                    < pd.to_numeric(water_anchor_compare["old_chain_rmse"], errors="coerce")
                ).fillna(False).sum()
            ),
            "beats_current_deployable": bool(candidate_mean < deployable_mean) if pd.notna(candidate_mean) and pd.notna(deployable_mean) else False,
            "beats_old_chain": bool(candidate_mean < old_mean) if pd.notna(candidate_mean) and pd.notna(old_mean) else False,
            "headline_eligible": False,
            "evidence": "; ".join(
                f"{row.analyzer_id}={row.water_anchor_overall_rmse:.3f}"
                for row in water_anchor_compare.itertuples(index=False)
                if pd.notna(row.water_anchor_overall_rmse)
            ),
        }
    ]


def build_old_vs_new_comparison_outputs(
    point_reconciliation: pd.DataFrame,
    selection_table: pd.DataFrame | None = None,
    new_chain_input_audit: pd.DataFrame | None = None,
    legacy_water_replay_detail: pd.DataFrame | None = None,
    ppm_family_challenge_detail: pd.DataFrame | None = None,
    water_anchor_compare: pd.DataFrame | None = None,
) -> dict[str, pd.DataFrame]:
    """Build deployable old-vs-new step-09/10 outputs."""

    compare = point_reconciliation.copy()
    selection_table = selection_table.copy() if selection_table is not None else pd.DataFrame()
    new_chain_input_audit = new_chain_input_audit.copy() if new_chain_input_audit is not None else pd.DataFrame()
    legacy_water_replay_detail = legacy_water_replay_detail.copy() if legacy_water_replay_detail is not None else pd.DataFrame()
    ppm_family_challenge_detail = ppm_family_challenge_detail.copy() if ppm_family_challenge_detail is not None else pd.DataFrame()
    water_anchor_compare = water_anchor_compare.copy() if water_anchor_compare is not None else pd.DataFrame()

    if compare.empty:
        empty = pd.DataFrame()
        return {
            "detail": empty,
            "summary": empty,
            "local_wins": empty,
            "aggregate_segments": empty,
            "ratio_source_audit": empty,
            "diagnostic_candidates": empty,
        }

    compare["target_ppm"] = pd.to_numeric(compare["target_ppm"], errors="coerce")
    compare["temp_c"] = pd.to_numeric(compare["temp_c"], errors="coerce")
    compare["old_error"] = pd.to_numeric(compare["old_error"], errors="coerce")
    compare["new_error"] = pd.to_numeric(compare["new_error"], errors="coerce")
    compare["old_pred_ppm"] = pd.to_numeric(compare["old_pred_ppm"], errors="coerce")
    compare["new_pred_ppm"] = pd.to_numeric(compare["new_pred_ppm"], errors="coerce")

    detail_rows: list[dict[str, Any]] = []
    for analyzer_id, analyzer_df in compare.groupby("analyzer_id", dropna=False):
        selected_source_pair, actual_ratio_source, evidence = _resolve_analyzer_actual_ratio_source(
            str(analyzer_id),
            analyzer_df,
            selection_table,
            new_chain_input_audit,
        )
        overall = _segment_metrics(analyzer_df, "overall")
        zero = _segment_metrics(analyzer_df, "zero")
        low = _segment_metrics(analyzer_df, "low")
        main = _segment_metrics(analyzer_df, "main")
        old_overall = _metrics(analyzer_df["old_error"])
        new_overall = _metrics(analyzer_df["new_error"])
        winner_counts = analyzer_df.get("winner_for_point", pd.Series(dtype=str)).fillna("tie").astype(str)
        detail_rows.append(
            {
                "analyzer_id": str(analyzer_id),
                "mode2_semantic_profile": _first_text(analyzer_df, "mode2_semantic_profile", default="mode2_semantics_unknown"),
                "mode2_legacy_raw_compare_safe": _first_bool(analyzer_df, "mode2_legacy_raw_compare_safe", default=False),
                "selected_source_pair": selected_source_pair or _first_text(analyzer_df, "selected_source_pair", default=""),
                "actual_ratio_source_used": actual_ratio_source,
                "actual_ratio_source_evidence": evidence,
                "old_chain_overall_rmse": overall["old_rmse"],
                "new_chain_overall_rmse": overall["new_rmse"],
                "delta_overall_rmse": overall["delta_rmse"],
                "improvement_pct_overall": overall["improvement_pct"],
                "old_chain_zero_rmse": zero["old_rmse"],
                "new_chain_zero_rmse": zero["new_rmse"],
                "delta_zero_rmse": zero["delta_rmse"],
                "improvement_pct_zero": zero["improvement_pct"],
                "old_chain_low_rmse": low["old_rmse"],
                "new_chain_low_rmse": low["new_rmse"],
                "delta_low_rmse": low["delta_rmse"],
                "improvement_pct_low": low["improvement_pct"],
                "old_chain_main_rmse": main["old_rmse"],
                "new_chain_main_rmse": main["new_rmse"],
                "delta_main_rmse": main["delta_rmse"],
                "improvement_pct_main": main["improvement_pct"],
                "old_chain_mae": old_overall["mae"],
                "new_chain_mae": new_overall["mae"],
                "point_count": int(len(analyzer_df)),
                "pointwise_win_count": int((winner_counts == "new_chain").sum()),
                "pointwise_loss_count": int((winner_counts == "old_chain").sum()),
                "pointwise_tie_count": int((winner_counts == "tie").sum()),
                "overall_win_flag": overall["win_flag"],
                "zero_win_flag": zero["win_flag"],
                "low_win_flag": low["win_flag"],
                "main_win_flag": main["win_flag"],
            }
        )

    detail_df = pd.DataFrame(detail_rows).sort_values(["analyzer_id"], ignore_index=True)

    aggregate_segment_rows: list[dict[str, Any]] = []
    for segment_tag in ("overall", "zero", "low", "main"):
        metrics = _segment_metrics(compare, segment_tag)
        aggregate_segment_rows.append(
            {
                "segment_tag": segment_tag,
                "old_chain_rmse": metrics["old_rmse"],
                "new_chain_rmse": metrics["new_rmse"],
                "delta_rmse": metrics["delta_rmse"],
                "improvement_pct": metrics["improvement_pct"],
                "point_count": metrics["point_count"],
                "win_flag": metrics["win_flag"],
            }
        )
    aggregate_segments = pd.DataFrame(aggregate_segment_rows)
    segment_lookup = aggregate_segments.set_index("segment_tag") if not aggregate_segments.empty else pd.DataFrame()

    actual_ratio_source_used_in_this_run, actual_ratio_source_used_majority, ratio_evidence = _overall_ratio_source_used(detail_df)
    designed_v5_ratio_source_intent = "raw_or_instantaneous_ratio"

    overall_win = bool(segment_lookup.loc["overall", "win_flag"]) if not segment_lookup.empty and "overall" in segment_lookup.index else False
    zero_win = bool(segment_lookup.loc["zero", "win_flag"]) if not segment_lookup.empty and "zero" in segment_lookup.index else False
    low_win = bool(segment_lookup.loc["low", "win_flag"]) if not segment_lookup.empty and "low" in segment_lookup.index else False
    main_win = bool(segment_lookup.loc["main", "win_flag"]) if not segment_lookup.empty and "main" in segment_lookup.index else False

    if overall_win and zero_win and low_win and main_win:
        overall_verdict = "current_deployable_new_chain_beats_old_chain_on_overall_zero_low_main"
    elif overall_win:
        overall_verdict = "current_deployable_new_chain_beats_old_chain_on_overall_but_not_all_segments"
    else:
        overall_verdict = "current_deployable_new_chain_does_not_yet_beat_old_chain_overall"

    all_segment_wins = (
        detail_df["overall_win_flag"].fillna(False)
        & detail_df["zero_win_flag"].fillna(False)
        & detail_df["low_win_flag"].fillna(False)
        & detail_df["main_win_flag"].fillna(False)
    )
    partial_segment_wins = (
        detail_df["overall_win_flag"].fillna(False)
        | detail_df["zero_win_flag"].fillna(False)
        | detail_df["low_win_flag"].fillna(False)
        | detail_df["main_win_flag"].fillna(False)
    )
    analyzers_fully_beating_old_count = int(all_segment_wins.sum())
    analyzers_partially_beating_old_count = int((partial_segment_wins & ~all_segment_wins).sum())
    analyzers_still_lagging_count = int(len(detail_df) - analyzers_fully_beating_old_count - analyzers_partially_beating_old_count)

    ratio_caveat = (
        f"actual offline run used {actual_ratio_source_used_in_this_run} ratio while designed_v5_ratio_source_intent is {designed_v5_ratio_source_intent}"
        if actual_ratio_source_used_in_this_run in {"filt", "mixed"}
        else "headline stays locked to old_chain vs current_deployable_new_chain; diagnostic candidates remain appendix-only"
    )

    summary_row = {
        "comparison_scope": "current_deployable_new_vs_old",
        "overall_verdict": overall_verdict,
        "overall_rmse_old": float(segment_lookup.loc["overall", "old_chain_rmse"]) if "overall" in segment_lookup.index else math.nan,
        "overall_rmse_new": float(segment_lookup.loc["overall", "new_chain_rmse"]) if "overall" in segment_lookup.index else math.nan,
        "overall_improvement_pct": float(segment_lookup.loc["overall", "improvement_pct"]) if "overall" in segment_lookup.index else math.nan,
        "zero_rmse_old": float(segment_lookup.loc["zero", "old_chain_rmse"]) if "zero" in segment_lookup.index else math.nan,
        "zero_rmse_new": float(segment_lookup.loc["zero", "new_chain_rmse"]) if "zero" in segment_lookup.index else math.nan,
        "zero_improvement_pct": float(segment_lookup.loc["zero", "improvement_pct"]) if "zero" in segment_lookup.index else math.nan,
        "low_rmse_old": float(segment_lookup.loc["low", "old_chain_rmse"]) if "low" in segment_lookup.index else math.nan,
        "low_rmse_new": float(segment_lookup.loc["low", "new_chain_rmse"]) if "low" in segment_lookup.index else math.nan,
        "low_improvement_pct": float(segment_lookup.loc["low", "improvement_pct"]) if "low" in segment_lookup.index else math.nan,
        "main_rmse_old": float(segment_lookup.loc["main", "old_chain_rmse"]) if "main" in segment_lookup.index else math.nan,
        "main_rmse_new": float(segment_lookup.loc["main", "new_chain_rmse"]) if "main" in segment_lookup.index else math.nan,
        "main_improvement_pct": float(segment_lookup.loc["main", "improvement_pct"]) if "main" in segment_lookup.index else math.nan,
        "analyzer_overall_wins": _count_true(detail_df["overall_win_flag"]),
        "analyzer_zero_wins": _count_true(detail_df["zero_win_flag"]),
        "analyzer_low_wins": _count_true(detail_df["low_win_flag"]),
        "analyzer_main_wins": _count_true(detail_df["main_win_flag"]),
        "total_pointwise_wins": int(detail_df["pointwise_win_count"].fillna(0).sum()),
        "total_pointwise_losses": int(detail_df["pointwise_loss_count"].fillna(0).sum()),
        "analyzers_fully_beating_old_count": analyzers_fully_beating_old_count,
        "analyzers_partially_beating_old_count": analyzers_partially_beating_old_count,
        "analyzers_still_lagging_count": analyzers_still_lagging_count,
        "actual_ratio_source_used_in_this_run": actual_ratio_source_used_in_this_run,
        "actual_ratio_source_used_majority": actual_ratio_source_used_majority,
        "designed_v5_ratio_source_intent": designed_v5_ratio_source_intent,
        "supporting_evidence_for_actual_ratio_source": ratio_evidence,
        "main_caveat": ratio_caveat,
        "whether_new_chain_has_overall_evidence_to_surpass_old": overall_win,
    }
    summary_df = pd.DataFrame([summary_row])

    local = compare.copy()
    local["temp_set_c"] = local["temp_c"]
    local["target_value"] = local["target_ppm"]
    local["old_value"] = local["old_pred_ppm"]
    local["new_value"] = local["new_pred_ppm"]
    local["abs_error_old"] = local["old_error"].abs()
    local["abs_error_new"] = local["new_error"].abs()
    local["improvement_abs_error"] = local["abs_error_old"] - local["abs_error_new"]
    local["local_win_flag"] = local["improvement_abs_error"] > 1.0e-12
    local["segment_tag"] = np.select(
        [
            local["target_ppm"] == 0.0,
            (local["target_ppm"] > 0.0) & (local["target_ppm"] <= 200.0),
            (local["target_ppm"] > 200.0) & (local["target_ppm"] <= 1000.0),
        ],
        ["zero", "low", "main"],
        default="other",
    )

    local_rows: list[dict[str, Any]] = []
    for analyzer_id, subset in local.groupby("analyzer_id", dropna=False):
        wins = subset[subset["improvement_abs_error"] > 1.0e-12].sort_values(
            ["improvement_abs_error", "abs_error_old", "target_ppm"],
            ascending=[False, False, True],
            ignore_index=True,
        ).head(5)
        losses = subset[subset["improvement_abs_error"] < -1.0e-12].sort_values(
            ["improvement_abs_error", "abs_error_new", "target_ppm"],
            ascending=[True, False, True],
            ignore_index=True,
        ).head(5)
        for rank, row in enumerate(wins.itertuples(index=False), start=1):
            local_rows.append(
                {
                    "analyzer_id": row.analyzer_id,
                    "point_title": row.point_title,
                    "point_row": row.point_row,
                    "temp_set_c": row.temp_set_c,
                    "target_ppm": row.target_ppm,
                    "old_value": row.old_value,
                    "new_value": row.new_value,
                    "target_value": row.target_value,
                    "abs_error_old": row.abs_error_old,
                    "abs_error_new": row.abs_error_new,
                    "improvement_abs_error": row.improvement_abs_error,
                    "local_win_flag": True,
                    "local_win_rank_within_analyzer": rank,
                    "segment_tag": row.segment_tag,
                }
            )
        for rank, row in enumerate(losses.itertuples(index=False), start=1):
            local_rows.append(
                {
                    "analyzer_id": row.analyzer_id,
                    "point_title": row.point_title,
                    "point_row": row.point_row,
                    "temp_set_c": row.temp_set_c,
                    "target_ppm": row.target_ppm,
                    "old_value": row.old_value,
                    "new_value": row.new_value,
                    "target_value": row.target_value,
                    "abs_error_old": row.abs_error_old,
                    "abs_error_new": row.abs_error_new,
                    "improvement_abs_error": row.improvement_abs_error,
                    "local_win_flag": False,
                    "local_win_rank_within_analyzer": rank,
                    "segment_tag": row.segment_tag,
                }
            )
    local_wins_df = pd.DataFrame(local_rows).sort_values(
        ["analyzer_id", "local_win_flag", "local_win_rank_within_analyzer"],
        ascending=[True, False, True],
        ignore_index=True,
    ) if local_rows else pd.DataFrame(
        columns=[
            "analyzer_id",
            "point_title",
            "point_row",
            "temp_set_c",
            "target_ppm",
            "old_value",
            "new_value",
            "target_value",
            "abs_error_old",
            "abs_error_new",
            "improvement_abs_error",
            "local_win_flag",
            "local_win_rank_within_analyzer",
            "segment_tag",
        ]
    )

    diagnostic_candidate_rows: list[dict[str, Any]] = []
    diagnostic_candidate_rows.extend(
        _pick_best_candidate_rows(
            legacy_water_replay_detail,
            candidate_group="best_legacy_water_replay_candidate",
            mode_column="water_lineage_mode",
            baseline_mode="none",
            label_column="water_lineage_mode",
        )
    )
    diagnostic_candidate_rows.extend(
        _pick_best_candidate_rows(
            ppm_family_challenge_detail,
            candidate_group="best_ppm_family_challenge_candidate",
            mode_column="ppm_family_mode",
            baseline_mode="current_fixed_family",
            label_column="ppm_family_mode",
        )
    )
    diagnostic_candidate_rows.extend(_water_anchor_candidate_rows(water_anchor_compare))
    diagnostic_candidates_df = pd.DataFrame(diagnostic_candidate_rows).sort_values(
        ["candidate_group", "candidate_mean_overall_rmse"],
        ignore_index=True,
    ) if diagnostic_candidate_rows else pd.DataFrame()

    ratio_source_audit = pd.DataFrame(
        [
            {
                "designed_v5_ratio_source_intent": designed_v5_ratio_source_intent,
                "actual_ratio_source_used_in_this_run": actual_ratio_source_used_in_this_run,
                "actual_ratio_source_used_majority": actual_ratio_source_used_majority,
                "supporting_evidence_for_actual_ratio_source": ratio_evidence,
            }
        ]
    )

    return {
        "detail": detail_df,
        "summary": summary_df,
        "local_wins": local_wins_df,
        "aggregate_segments": aggregate_segments,
        "ratio_source_audit": ratio_source_audit,
        "diagnostic_candidates": diagnostic_candidates_df,
    }


def _empty_frame(columns: tuple[str, ...]) -> pd.DataFrame:
    return pd.DataFrame(columns=list(columns))


def _normalize_actual_ratio_source_series(values: pd.Series) -> str:
    normalized = {
        _normalize_actual_ratio_source(value)
        for value in values.dropna().astype(str).tolist()
    }
    normalized.discard("unknown")
    if "mixed" in normalized:
        return "mixed"
    if len(normalized) > 1:
        return "mixed"
    if len(normalized) == 1:
        return next(iter(normalized))
    return "unknown"


def _collapse_text_values(values: pd.Series, default: str = "") -> str:
    unique_values = sorted(
        {
            str(value).strip()
            for value in values.dropna().astype(str).tolist()
            if str(value).strip()
        }
    )
    return "|".join(unique_values) if unique_values else default


def _scoped_overall_verdict(overall_win: bool, zero_win: bool, low_win: bool, main_win: bool) -> str:
    if overall_win and zero_win and low_win and main_win:
        return "current_deployable_new_chain_beats_old_chain_on_overall_zero_low_main_within_scope"
    if overall_win:
        return "current_deployable_new_chain_beats_old_chain_overall_within_scope_but_not_all_segments"
    return "current_deployable_new_chain_does_not_yet_beat_old_chain_overall_within_scope"


def _normalize_scoped_compare_frame(compare: pd.DataFrame, run_id: str, comparison_scope: str) -> pd.DataFrame:
    if compare.empty:
        return compare.copy()
    scoped = compare.copy()
    scoped["run_id"] = str(run_id)
    scoped["comparison_scope"] = str(comparison_scope)
    scoped["analyzer_id"] = scoped["analyzer_id"].astype(str)
    scoped["target_ppm"] = pd.to_numeric(scoped["target_ppm"], errors="coerce")
    scoped["temp_c"] = pd.to_numeric(scoped["temp_c"], errors="coerce")
    scoped["old_error"] = pd.to_numeric(scoped["old_error"], errors="coerce")
    scoped["new_error"] = pd.to_numeric(scoped["new_error"], errors="coerce")
    scoped["old_pred_ppm"] = pd.to_numeric(scoped["old_pred_ppm"], errors="coerce")
    scoped["new_pred_ppm"] = pd.to_numeric(scoped["new_pred_ppm"], errors="coerce")
    return scoped


def _build_scoped_detail_from_compare(compare: pd.DataFrame, comparison_scope: str) -> pd.DataFrame:
    if compare.empty:
        return _empty_frame(SCOPED_DETAIL_COLUMNS)

    rows: list[dict[str, Any]] = []
    for (run_id, analyzer_id), subset in compare.groupby(["run_id", "analyzer_id"], dropna=False):
        overall = _segment_metrics(subset, "overall")
        zero = _segment_metrics(subset, "zero")
        low = _segment_metrics(subset, "low")
        main = _segment_metrics(subset, "main")
        winner_counts = subset.get("winner_for_point", pd.Series(dtype=str)).fillna("tie").astype(str)
        selected_pair = _collapse_text_values(subset.get("selected_source_pair", pd.Series(dtype=object)))
        actual_ratio = _normalize_actual_ratio_source_series(
            pd.concat(
                [
                    subset.get("ratio_source_selected", pd.Series(dtype=object)),
                    subset.get("selected_source_pair", pd.Series(dtype=object)),
                ],
                ignore_index=True,
            )
        )
        rows.append(
            {
                "run_id": str(run_id),
                "comparison_scope": str(comparison_scope),
                "analyzer_id": str(analyzer_id),
                "selected_source_pair": selected_pair,
                "actual_ratio_source_used": actual_ratio,
                "old_chain_overall_rmse": overall["old_rmse"],
                "new_chain_overall_rmse": overall["new_rmse"],
                "delta_overall_rmse": overall["delta_rmse"],
                "improvement_pct_overall": overall["improvement_pct"],
                "old_chain_zero_rmse": zero["old_rmse"],
                "new_chain_zero_rmse": zero["new_rmse"],
                "delta_zero_rmse": zero["delta_rmse"],
                "improvement_pct_zero": zero["improvement_pct"],
                "old_chain_low_rmse": low["old_rmse"],
                "new_chain_low_rmse": low["new_rmse"],
                "delta_low_rmse": low["delta_rmse"],
                "improvement_pct_low": low["improvement_pct"],
                "old_chain_main_rmse": main["old_rmse"],
                "new_chain_main_rmse": main["new_rmse"],
                "delta_main_rmse": main["delta_rmse"],
                "improvement_pct_main": main["improvement_pct"],
                "point_count": int(len(subset)),
                "pointwise_win_count": int((winner_counts == "new_chain").sum()),
                "pointwise_loss_count": int((winner_counts == "old_chain").sum()),
                "pointwise_tie_count": int((winner_counts == "tie").sum()),
                "overall_win_flag": overall["win_flag"],
                "zero_win_flag": zero["win_flag"],
                "low_win_flag": low["win_flag"],
                "main_win_flag": main["win_flag"],
            }
        )
    detail_df = pd.DataFrame(rows).sort_values(["run_id", "analyzer_id"], ignore_index=True)
    ordered_columns = list(SCOPED_DETAIL_COLUMNS) + [column for column in detail_df.columns if column not in SCOPED_DETAIL_COLUMNS]
    return detail_df.reindex(columns=ordered_columns)


def _build_scoped_local_wins(compare: pd.DataFrame, comparison_scope: str, top_n: int = 10) -> pd.DataFrame:
    if compare.empty:
        return _empty_frame(SCOPED_LOCAL_WINS_COLUMNS)

    local = compare.copy()
    local["temp_set_c"] = pd.to_numeric(local["temp_c"], errors="coerce")
    local["target_ppm"] = pd.to_numeric(local["target_ppm"], errors="coerce")
    local["target_value"] = local["target_ppm"]
    local["old_value"] = pd.to_numeric(local["old_pred_ppm"], errors="coerce")
    local["new_value"] = pd.to_numeric(local["new_pred_ppm"], errors="coerce")
    local["abs_error_old"] = pd.to_numeric(local["old_error"], errors="coerce").abs()
    local["abs_error_new"] = pd.to_numeric(local["new_error"], errors="coerce").abs()
    local["improvement_abs_error"] = local["abs_error_old"] - local["abs_error_new"]
    local["local_win_flag"] = local["improvement_abs_error"] > 1.0e-12
    local["segment_tag"] = np.select(
        [
            local["target_ppm"] == 0.0,
            (local["target_ppm"] > 0.0) & (local["target_ppm"] <= 200.0),
            (local["target_ppm"] > 200.0) & (local["target_ppm"] <= 1000.0),
        ],
        ["zero", "low", "main"],
        default="other",
    )

    rows: list[dict[str, Any]] = []
    for analyzer_id, subset in local.groupby("analyzer_id", dropna=False):
        wins = subset[subset["improvement_abs_error"] > 1.0e-12].sort_values(
            ["improvement_abs_error", "abs_error_old", "run_id", "target_ppm"],
            ascending=[False, False, True, True],
            ignore_index=True,
        ).head(top_n)
        losses = subset[subset["improvement_abs_error"] < -1.0e-12].sort_values(
            ["improvement_abs_error", "abs_error_new", "run_id", "target_ppm"],
            ascending=[True, False, True, True],
            ignore_index=True,
        ).head(top_n)
        for rank, row in enumerate(wins.itertuples(index=False), start=1):
            rows.append(
                {
                    "run_id": str(row.run_id),
                    "comparison_scope": str(comparison_scope),
                    "analyzer_id": str(row.analyzer_id),
                    "point_title": row.point_title,
                    "point_row": row.point_row,
                    "temp_set_c": row.temp_set_c,
                    "target_ppm": row.target_ppm,
                    "target_value": row.target_value,
                    "old_value": row.old_value,
                    "new_value": row.new_value,
                    "abs_error_old": row.abs_error_old,
                    "abs_error_new": row.abs_error_new,
                    "improvement_abs_error": row.improvement_abs_error,
                    "local_win_flag": True,
                    "local_win_rank_within_analyzer": rank,
                    "local_loss_rank_within_analyzer": np.nan,
                    "segment_tag": row.segment_tag,
                }
            )
        for rank, row in enumerate(losses.itertuples(index=False), start=1):
            rows.append(
                {
                    "run_id": str(row.run_id),
                    "comparison_scope": str(comparison_scope),
                    "analyzer_id": str(row.analyzer_id),
                    "point_title": row.point_title,
                    "point_row": row.point_row,
                    "temp_set_c": row.temp_set_c,
                    "target_ppm": row.target_ppm,
                    "target_value": row.target_value,
                    "old_value": row.old_value,
                    "new_value": row.new_value,
                    "abs_error_old": row.abs_error_old,
                    "abs_error_new": row.abs_error_new,
                    "improvement_abs_error": row.improvement_abs_error,
                    "local_win_flag": False,
                    "local_win_rank_within_analyzer": np.nan,
                    "local_loss_rank_within_analyzer": rank,
                    "segment_tag": row.segment_tag,
                }
            )
    if not rows:
        return _empty_frame(SCOPED_LOCAL_WINS_COLUMNS)
    local_df = pd.DataFrame(rows).sort_values(
        ["analyzer_id", "local_win_flag", "run_id", "local_win_rank_within_analyzer", "local_loss_rank_within_analyzer"],
        ascending=[True, False, True, True, True],
        ignore_index=True,
    )
    ordered_columns = list(SCOPED_LOCAL_WINS_COLUMNS) + [column for column in local_df.columns if column not in SCOPED_LOCAL_WINS_COLUMNS]
    return local_df.reindex(columns=ordered_columns)


def _build_scoped_analyzer_aggregate(
    compare: pd.DataFrame,
    detail_df: pd.DataFrame,
    comparison_scope: str,
) -> pd.DataFrame:
    if compare.empty:
        return _empty_frame(SCOPED_DETAIL_COLUMNS)

    rows: list[dict[str, Any]] = []
    for analyzer_id, subset in compare.groupby("analyzer_id", dropna=False):
        overall = _segment_metrics(subset, "overall")
        zero = _segment_metrics(subset, "zero")
        low = _segment_metrics(subset, "low")
        main = _segment_metrics(subset, "main")
        winner_counts = subset.get("winner_for_point", pd.Series(dtype=str)).fillna("tie").astype(str)
        detail_subset = detail_df[detail_df["analyzer_id"].astype(str) == str(analyzer_id)].copy() if not detail_df.empty else pd.DataFrame()
        selected_pair = _collapse_text_values(detail_subset.get("selected_source_pair", pd.Series(dtype=object)))
        actual_ratio = _normalize_actual_ratio_source_series(detail_subset.get("actual_ratio_source_used", pd.Series(dtype=object)))
        rows.append(
            {
                "run_id": "",
                "comparison_scope": str(comparison_scope),
                "analyzer_id": str(analyzer_id),
                "selected_source_pair": selected_pair,
                "actual_ratio_source_used": actual_ratio,
                "old_chain_overall_rmse": overall["old_rmse"],
                "new_chain_overall_rmse": overall["new_rmse"],
                "delta_overall_rmse": overall["delta_rmse"],
                "improvement_pct_overall": overall["improvement_pct"],
                "old_chain_zero_rmse": zero["old_rmse"],
                "new_chain_zero_rmse": zero["new_rmse"],
                "delta_zero_rmse": zero["delta_rmse"],
                "improvement_pct_zero": zero["improvement_pct"],
                "old_chain_low_rmse": low["old_rmse"],
                "new_chain_low_rmse": low["new_rmse"],
                "delta_low_rmse": low["delta_rmse"],
                "improvement_pct_low": low["improvement_pct"],
                "old_chain_main_rmse": main["old_rmse"],
                "new_chain_main_rmse": main["new_rmse"],
                "delta_main_rmse": main["delta_rmse"],
                "improvement_pct_main": main["improvement_pct"],
                "point_count": int(len(subset)),
                "pointwise_win_count": int((winner_counts == "new_chain").sum()),
                "pointwise_loss_count": int((winner_counts == "old_chain").sum()),
                "pointwise_tie_count": int((winner_counts == "tie").sum()),
                "overall_win_flag": overall["win_flag"],
                "zero_win_flag": zero["win_flag"],
                "low_win_flag": low["win_flag"],
                "main_win_flag": main["win_flag"],
            }
        )
    analyzer_df = pd.DataFrame(rows).sort_values(["analyzer_id"], ignore_index=True)
    ordered_columns = list(SCOPED_DETAIL_COLUMNS) + [column for column in analyzer_df.columns if column not in SCOPED_DETAIL_COLUMNS]
    return analyzer_df.reindex(columns=ordered_columns)


def _build_scoped_summary(
    *,
    comparison_scope: str,
    run_scope_description: str,
    headline_safe_statement: str,
    scope_limitation_statement: str,
    compare: pd.DataFrame,
    analyzer_aggregate: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if compare.empty:
        summary_df = pd.DataFrame(
            [
                {
                    "comparison_scope": comparison_scope,
                    "run_scope_description": run_scope_description,
                    "analyzer_set": _collapse_text_values(analyzer_aggregate.get("analyzer_id", pd.Series(dtype=object))),
                    "overall_verdict_scoped": "no_successful_old_vs_new_rows_in_scope",
                    "overall_rmse_old_scoped": math.nan,
                    "overall_rmse_new_scoped": math.nan,
                    "overall_improvement_pct_scoped": math.nan,
                    "zero_improvement_pct_scoped": math.nan,
                    "low_improvement_pct_scoped": math.nan,
                    "main_improvement_pct_scoped": math.nan,
                    "analyzer_overall_wins": 0,
                    "analyzer_zero_wins": 0,
                    "analyzer_low_wins": 0,
                    "analyzer_main_wins": 0,
                    "total_pointwise_wins": 0,
                    "total_pointwise_losses": 0,
                    "analyzers_fully_beating_old_count": 0,
                    "analyzers_partially_beating_old_count": 0,
                    "analyzers_still_lagging_count": 0,
                    "headline_safe_statement": headline_safe_statement,
                    "scope_limitation_statement": scope_limitation_statement,
                    "zero_rmse_old_scoped": math.nan,
                    "zero_rmse_new_scoped": math.nan,
                    "low_rmse_old_scoped": math.nan,
                    "low_rmse_new_scoped": math.nan,
                    "main_rmse_old_scoped": math.nan,
                    "main_rmse_new_scoped": math.nan,
                }
            ]
        )
        return summary_df.reindex(columns=list(summary_df.columns)), _empty_frame(SCOPED_SEGMENT_COLUMNS)

    aggregate_rows: list[dict[str, Any]] = []
    for segment_tag in ("overall", "zero", "low", "main"):
        metrics = _segment_metrics(compare, segment_tag)
        aggregate_rows.append(
            {
                "comparison_scope": comparison_scope,
                "segment_tag": segment_tag,
                "old_chain_rmse": metrics["old_rmse"],
                "new_chain_rmse": metrics["new_rmse"],
                "delta_rmse": metrics["delta_rmse"],
                "improvement_pct": metrics["improvement_pct"],
                "point_count": metrics["point_count"],
                "win_flag": metrics["win_flag"],
            }
        )
    aggregate_segments = pd.DataFrame(aggregate_rows).reindex(columns=list(SCOPED_SEGMENT_COLUMNS))
    segment_lookup = aggregate_segments.set_index("segment_tag")
    overall_win = bool(segment_lookup.loc["overall", "win_flag"])
    zero_win = bool(segment_lookup.loc["zero", "win_flag"])
    low_win = bool(segment_lookup.loc["low", "win_flag"])
    main_win = bool(segment_lookup.loc["main", "win_flag"])
    all_segment_wins = (
        analyzer_aggregate["overall_win_flag"].fillna(False).map(bool)
        & analyzer_aggregate["zero_win_flag"].fillna(False).map(bool)
        & analyzer_aggregate["low_win_flag"].fillna(False).map(bool)
        & analyzer_aggregate["main_win_flag"].fillna(False).map(bool)
    )
    any_segment_wins = (
        analyzer_aggregate["overall_win_flag"].fillna(False).map(bool)
        | analyzer_aggregate["zero_win_flag"].fillna(False).map(bool)
        | analyzer_aggregate["low_win_flag"].fillna(False).map(bool)
        | analyzer_aggregate["main_win_flag"].fillna(False).map(bool)
    )
    summary_row = {
        "comparison_scope": comparison_scope,
        "run_scope_description": run_scope_description,
        "analyzer_set": ",".join(analyzer_aggregate["analyzer_id"].astype(str).tolist()),
        "overall_verdict_scoped": _scoped_overall_verdict(overall_win, zero_win, low_win, main_win),
        "overall_rmse_old_scoped": float(segment_lookup.loc["overall", "old_chain_rmse"]),
        "overall_rmse_new_scoped": float(segment_lookup.loc["overall", "new_chain_rmse"]),
        "overall_improvement_pct_scoped": float(segment_lookup.loc["overall", "improvement_pct"]),
        "zero_improvement_pct_scoped": float(segment_lookup.loc["zero", "improvement_pct"]),
        "low_improvement_pct_scoped": float(segment_lookup.loc["low", "improvement_pct"]),
        "main_improvement_pct_scoped": float(segment_lookup.loc["main", "improvement_pct"]),
        "analyzer_overall_wins": _count_true(analyzer_aggregate["overall_win_flag"]),
        "analyzer_zero_wins": _count_true(analyzer_aggregate["zero_win_flag"]),
        "analyzer_low_wins": _count_true(analyzer_aggregate["low_win_flag"]),
        "analyzer_main_wins": _count_true(analyzer_aggregate["main_win_flag"]),
        "total_pointwise_wins": int(compare.get("winner_for_point", pd.Series(dtype=str)).fillna("").astype(str).eq("new_chain").sum()),
        "total_pointwise_losses": int(compare.get("winner_for_point", pd.Series(dtype=str)).fillna("").astype(str).eq("old_chain").sum()),
        "analyzers_fully_beating_old_count": int(all_segment_wins.sum()),
        "analyzers_partially_beating_old_count": int((any_segment_wins & ~all_segment_wins).sum()),
        "analyzers_still_lagging_count": int(len(analyzer_aggregate) - any_segment_wins.sum()),
        "headline_safe_statement": headline_safe_statement,
        "scope_limitation_statement": scope_limitation_statement,
        "zero_rmse_old_scoped": float(segment_lookup.loc["zero", "old_chain_rmse"]),
        "zero_rmse_new_scoped": float(segment_lookup.loc["zero", "new_chain_rmse"]),
        "low_rmse_old_scoped": float(segment_lookup.loc["low", "old_chain_rmse"]),
        "low_rmse_new_scoped": float(segment_lookup.loc["low", "new_chain_rmse"]),
        "main_rmse_old_scoped": float(segment_lookup.loc["main", "old_chain_rmse"]),
        "main_rmse_new_scoped": float(segment_lookup.loc["main", "new_chain_rmse"]),
    }
    summary_df = pd.DataFrame([summary_row])
    ordered_columns = list(SCOPED_SUMMARY_COLUMNS) + [column for column in summary_df.columns if column not in SCOPED_SUMMARY_COLUMNS]
    return summary_df.reindex(columns=ordered_columns), aggregate_segments


def _build_scoped_scope_outputs(
    *,
    comparison_scope: str,
    run_scope_description: str,
    headline_safe_statement: str,
    scope_limitation_statement: str,
    selected_runs: list[dict[str, Any]],
    analyzer_filter: tuple[str, ...] | None,
) -> dict[str, pd.DataFrame | str]:
    detail_frames: list[pd.DataFrame] = []
    point_frames: list[pd.DataFrame] = []

    for item in selected_runs:
        run_id = str(item["run_id"])
        detail = item["detail"].copy()
        compare = item["point_reconciliation"].copy()
        if analyzer_filter is not None:
            allowed = {str(value) for value in analyzer_filter}
            if not detail.empty and "analyzer_id" in detail.columns:
                detail = detail[detail["analyzer_id"].astype(str).isin(allowed)].copy()
            if not compare.empty and "analyzer_id" in compare.columns:
                compare = compare[compare["analyzer_id"].astype(str).isin(allowed)].copy()
        if not detail.empty:
            detail["run_id"] = run_id
            detail["comparison_scope"] = comparison_scope
            ordered_columns = list(SCOPED_DETAIL_COLUMNS) + [column for column in detail.columns if column not in SCOPED_DETAIL_COLUMNS]
            detail_frames.append(detail.reindex(columns=ordered_columns))
        if not compare.empty:
            point_frames.append(_normalize_scoped_compare_frame(compare, run_id, comparison_scope))

    compare_df = pd.concat(point_frames, ignore_index=True) if point_frames else pd.DataFrame()
    detail_df = pd.concat(detail_frames, ignore_index=True) if detail_frames else pd.DataFrame()
    if detail_df.empty and not compare_df.empty:
        detail_df = _build_scoped_detail_from_compare(compare_df, comparison_scope)
    elif not detail_df.empty:
        ordered_columns = list(SCOPED_DETAIL_COLUMNS) + [column for column in detail_df.columns if column not in SCOPED_DETAIL_COLUMNS]
        detail_df = detail_df.reindex(columns=ordered_columns).sort_values(["run_id", "analyzer_id"], ignore_index=True)
    else:
        detail_df = _empty_frame(SCOPED_DETAIL_COLUMNS)

    local_wins = _build_scoped_local_wins(compare_df, comparison_scope, top_n=10)
    analyzer_aggregate = _build_scoped_analyzer_aggregate(compare_df, detail_df, comparison_scope)
    summary_df, aggregate_segments = _build_scoped_summary(
        comparison_scope=comparison_scope,
        run_scope_description=run_scope_description,
        headline_safe_statement=headline_safe_statement,
        scope_limitation_statement=scope_limitation_statement,
        compare=compare_df,
        analyzer_aggregate=analyzer_aggregate,
    )
    return {
        "comparison_scope": comparison_scope,
        "run_scope_description": run_scope_description,
        "headline_safe_statement": headline_safe_statement,
        "scope_limitation_statement": scope_limitation_statement,
        "detail": detail_df,
        "summary": summary_df,
        "local_wins": local_wins,
        "aggregate_segments": aggregate_segments,
        "analyzer_aggregate": analyzer_aggregate,
        "point_reconciliation": compare_df,
    }


def build_scoped_old_vs_new_outputs(
    run_results: list[dict[str, Any]],
    *,
    scope_b_run_id: str = "run_20260410_132440",
) -> dict[str, dict[str, pd.DataFrame | str]]:
    """Build two separate scoped old-vs-new outputs for historical vs 2026-04-10."""

    prepared_runs: list[dict[str, Any]] = []
    for result in run_results:
        run_id = str(result.get("run_name", "") or "")
        if not run_id:
            continue
        detail = result.get("old_vs_new_outputs", {}).get("detail", pd.DataFrame())
        point_reconciliation = result.get("point_reconciliation", pd.DataFrame())
        detail = detail.copy() if isinstance(detail, pd.DataFrame) else pd.DataFrame()
        point_reconciliation = (
            point_reconciliation.copy()
            if isinstance(point_reconciliation, pd.DataFrame)
            else pd.DataFrame()
        )
        if detail.empty and point_reconciliation.empty:
            continue
        prepared_runs.append(
            {
                "run_id": run_id,
                "detail": detail,
                "point_reconciliation": point_reconciliation,
            }
        )

    historical_runs = [item for item in prepared_runs if item["run_id"] != scope_b_run_id]
    scope_b_runs = [item for item in prepared_runs if item["run_id"] == scope_b_run_id]

    scope_a = _build_scoped_scope_outputs(
        comparison_scope="historical_ga02_ga03_old_vs_new",
        run_scope_description="historical packages excluding run_20260410_132440; only GA02 and GA03 are included",
        headline_safe_statement="仅针对 historical packages 中的 GA02/GA03",
        scope_limitation_statement="两个 scope 不能合并解读为统一全局结论",
        selected_runs=historical_runs,
        analyzer_filter=("GA02", "GA03"),
    )
    scope_b = _build_scoped_scope_outputs(
        comparison_scope="run_20260410_132440_all_analyzers_old_vs_new",
        run_scope_description="run_20260410_132440 only; include every analyzer that appears in that run",
        headline_safe_statement="仅针对 run_20260410_132440 的全 analyzers",
        scope_limitation_statement="两个 scope 不能合并解读为统一全局结论",
        selected_runs=scope_b_runs,
        analyzer_filter=None,
    )
    return {
        "scope_a": scope_a,
        "scope_b": scope_b,
    }


def _sorted_unique_text(values: pd.Series) -> list[str]:
    if values.empty:
        return []
    return sorted(
        {
            str(value).strip()
            for value in values.dropna().astype(str).tolist()
            if str(value).strip()
        }
    )


def _scope_selected_source_pair_summary(detail: pd.DataFrame) -> str:
    if detail.empty:
        return ""
    parts: list[str] = []
    for row in detail.sort_values(["run_id", "analyzer_id"]).itertuples(index=False):
        parts.append(f"{row.run_id}/{row.analyzer_id}={row.selected_source_pair}")
    return "; ".join(parts)


def _scope_run_ids(detail: pd.DataFrame) -> str:
    return ",".join(_sorted_unique_text(detail.get("run_id", pd.Series(dtype=object))))


def _scope_analyzer_set(detail: pd.DataFrame, summary: pd.DataFrame) -> str:
    if not summary.empty and "analyzer_set" in summary.columns:
        value = str(summary.iloc[0].get("analyzer_set") or "").strip()
        if value:
            return value
    return ",".join(_sorted_unique_text(detail.get("analyzer_id", pd.Series(dtype=object))))


def _scope_point_count(scope_output: dict[str, pd.DataFrame | str]) -> int:
    aggregate_segments = scope_output.get("aggregate_segments", pd.DataFrame())
    if isinstance(aggregate_segments, pd.DataFrame) and not aggregate_segments.empty:
        overall_row = aggregate_segments[aggregate_segments["segment_tag"].astype(str) == "overall"]
        if not overall_row.empty:
            return int(pd.to_numeric(overall_row.iloc[0].get("point_count"), errors="coerce") or 0)
    compare = scope_output.get("point_reconciliation", pd.DataFrame())
    if isinstance(compare, pd.DataFrame):
        return int(len(compare))
    return 0


def _scope_config_facts(run_results: list[dict[str, Any]], run_ids: set[str]) -> tuple[bool, bool, bool]:
    matched_only = False
    valid_only = False
    hard_exclude = False
    for result in run_results:
        run_id = str(result.get("run_name", "") or "")
        if run_id not in run_ids:
            continue
        config = result.get("config")
        matched_only = matched_only or bool(getattr(config, "default_source_policy", "") == "matched_only")
        valid_only = valid_only or bool(getattr(config, "use_valid_only_main_conclusion", False))
        hard_exclude = hard_exclude or bool(getattr(config, "invalid_pressure_mode", "") == "hard_exclude")
    return matched_only, valid_only, hard_exclude


def build_comparison_reconciliation_table(
    *,
    run_results: list[dict[str, Any]],
    scoped_outputs: dict[str, dict[str, pd.DataFrame | str]],
) -> pd.DataFrame:
    """Build a scope-by-scope reconciliation table for the final report layer."""

    fixed_rule = "以后对外一律以 step_09c / step_09d / step_10c 的 scoped debugger 结果为准"
    rows: list[dict[str, Any]] = []
    for scope_key, scope_label in (("scope_a", "historical GA02/GA03"), ("scope_b", "2026-04-10 all analyzers")):
        scope_output = scoped_outputs[scope_key]
        detail = scope_output.get("detail", pd.DataFrame())
        summary = scope_output.get("summary", pd.DataFrame())
        detail = detail if isinstance(detail, pd.DataFrame) else pd.DataFrame()
        summary = summary if isinstance(summary, pd.DataFrame) else pd.DataFrame()
        summary_row = summary.iloc[0] if not summary.empty else pd.Series(dtype=object)
        run_ids = set(_sorted_unique_text(detail.get("run_id", pd.Series(dtype=object))))
        matched_only, valid_only, hard_exclude = _scope_config_facts(run_results, run_ids)
        if scope_key == "scope_a":
            analyzer_scope = "historical packages excluding run_20260410_132440; analyzer inclusion fixed to GA02 and GA03 only"
            quick_calc_reason = (
                "A single-package quick calculation can point in the opposite direction if it only looks at one historical run, "
                "drops one of GA02/GA03, or uses a different point count than the scoped historical aggregation."
            )
        else:
            analyzer_scope = (
                f"run_20260410_132440 only; analyzer inclusion is every analyzer present in that run ({_scope_analyzer_set(detail, summary)})"
            )
            quick_calc_reason = (
                "A quick calculation can point in the opposite direction if it omits some analyzers from run_20260410_132440, "
                "or if it does not use the same matched-only, valid-only, hard-excluded deployable debugger point table."
            )
        rows.append(
            {
                "comparison_scope": str(scope_output.get("comparison_scope", "")),
                "scope_label": scope_label,
                "run_scope_description": str(summary_row.get("run_scope_description", scope_output.get("run_scope_description", ""))),
                "benchmark_chain": "old_chain",
                "old_value_source": "point_reconciliation.old_pred_ppm",
                "new_value_source": "point_reconciliation.new_pred_ppm from current_deployable_new_chain",
                "selected_source_pair": _scope_selected_source_pair_summary(detail),
                "matched_only_filter_applied": matched_only,
                "valid_only_filter_applied": valid_only,
                "hard_exclude_500hpa_applied": hard_exclude,
                "analyzer_inclusion_scope": analyzer_scope,
                "point_count_used": _scope_point_count(scope_output),
                "whether_point_table_mean_was_used": True,
                "whether_deployable_chain_output_was_used": True,
                "run_ids_in_scope": _scope_run_ids(detail),
                "analyzer_set": _scope_analyzer_set(detail, summary),
                "overall_verdict_scoped": str(summary_row.get("overall_verdict_scoped", "")),
                "why_single_package_quick_calc_can_flip_direction": quick_calc_reason,
                "future_external_reporting_rule": fixed_rule,
            }
        )
    reconciliation = pd.DataFrame(rows)
    ordered_columns = list(COMPARISON_RECONCILIATION_COLUMNS) + [
        column for column in reconciliation.columns if column not in COMPARISON_RECONCILIATION_COLUMNS
    ]
    return reconciliation.reindex(columns=ordered_columns)
