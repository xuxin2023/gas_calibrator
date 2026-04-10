"""Cross-run summary helpers for the absorbance debugger."""

from __future__ import annotations

from typing import Any

import pandas as pd


def build_cross_run_summary(
    run_results: list[dict[str, Any]],
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, str]:
    """Build cross-run summary tables and a short reproducibility note."""

    rows: list[dict[str, Any]] = []
    for result in run_results:
        run_name = str(result.get("run_name", ""))
        overview = result.get("comparison_outputs", {}).get("overview_summary", pd.DataFrame())
        selection = result.get("model_results", {}).get("selection", pd.DataFrame())
        ga01_note = str(result.get("ga01_special_note", ""))
        invalid_pressure_summary = result.get("invalid_pressure_summary", pd.DataFrame())
        run_role_assessment = result.get("run_role_assessment", pd.DataFrame())
        config = result.get("config")
        if overview is None or getattr(overview, "empty", True):
            continue

        selection = selection.copy() if selection is not None else pd.DataFrame()
        invalid_point_count = 0
        invalid_sample_count = 0
        if invalid_pressure_summary is not None and not getattr(invalid_pressure_summary, "empty", True):
            overall_invalid = invalid_pressure_summary[invalid_pressure_summary["summary_scope"] == "overall"]
            if not overall_invalid.empty:
                invalid_point_count = int(pd.to_numeric(overall_invalid.iloc[0].get("invalid_point_count"), errors="coerce") or 0)
                invalid_sample_count = int(pd.to_numeric(overall_invalid.iloc[0].get("invalid_sample_count"), errors="coerce") or 0)

        run_role = ""
        has_high_temp_zero_anchor_candidate = False
        if run_role_assessment is not None and not getattr(run_role_assessment, "empty", True):
            run_row = run_role_assessment[run_role_assessment["assessment_scope"] == "run_summary"]
            if not run_row.empty:
                run_role = str(run_row.iloc[0].get("recommended_role", ""))
                has_high_temp_zero_anchor_candidate = bool(run_row.iloc[0].get("has_high_temp_zero_anchor_candidate", False))

        for _, row in overview.iterrows():
            analyzer_id = str(row["analyzer_id"])
            selection_row = (
                selection[selection["analyzer_id"] == analyzer_id].iloc[0]
                if not selection.empty and (selection["analyzer_id"] == analyzer_id).any()
                else None
            )
            rows.append(
                {
                    "run_id": run_name,
                    "run_name": run_name,
                    "analyzer_id": analyzer_id,
                    "old_overall_rmse": row.get("old_chain_rmse"),
                    "new_overall_rmse": row.get("new_chain_rmse"),
                    "old_zero_rmse": row.get("old_zero_rmse"),
                    "new_zero_rmse": row.get("new_zero_rmse"),
                    "old_temp_stability_metric": row.get("old_temp_stability_metric"),
                    "new_temp_stability_metric": row.get("new_temp_stability_metric"),
                    "rmse_gap_new_minus_old": row.get("new_chain_rmse") - row.get("old_chain_rmse"),
                    "winner_overall": row.get("winner_overall"),
                    "winner_zero": row.get("winner_zero"),
                    "winner_temp_stability": row.get("winner_temp_stability"),
                    "winner_low_range": row.get("winner_low_range"),
                    "winner_main_range": row.get("winner_main_range"),
                    "best_absorbance_model": selection_row["best_absorbance_model"] if selection_row is not None else "",
                    "best_model_family": selection_row["best_model_family"] if selection_row is not None and "best_model_family" in selection_row else "",
                    "zero_residual_mode": selection_row["zero_residual_mode"] if selection_row is not None and "zero_residual_mode" in selection_row else "",
                    "selected_source_policy": getattr(config, "matched_selection_policy", ""),
                    "selected_source_pair": selection_row["selected_source_pair"] if selection_row is not None and "selected_source_pair" in selection_row else "",
                    "invalid_pressure_excluded_count": invalid_point_count,
                    "invalid_pressure_excluded_sample_count": invalid_sample_count,
                    "run_role": run_role,
                    "has_high_temp_zero_anchor_candidate": has_high_temp_zero_anchor_candidate,
                    "ga01_special_note": ga01_note if analyzer_id == "GA01" else "",
                }
            )

    summary = pd.DataFrame(rows).sort_values(["analyzer_id", "run_name"], ignore_index=True) if rows else pd.DataFrame()
    if summary.empty:
        empty = pd.DataFrame()
        return empty, empty, empty, "Cross-run reproducibility could not be evaluated because no successful run results were collected."

    note_parts: list[str] = []
    by_analyzer_rows: list[dict[str, Any]] = []
    auto_rows: list[dict[str, Any]] = []

    for analyzer_id in ("GA02", "GA03"):
        subset = summary[summary["analyzer_id"] == analyzer_id].copy()
        if subset.empty:
            note_parts.append(f"{analyzer_id}: no successful batch result.")
            continue
        improved = int((subset["new_overall_rmse"] < subset["old_overall_rmse"]).sum())
        total = int(len(subset))
        note_parts.append(
            f"{analyzer_id}: new_chain improved on {improved}/{total} runs."
            if improved > 0
            else f"{analyzer_id}: new_chain did not improve on any of {total} runs."
        )

    ga01_subset = summary[summary["analyzer_id"] == "GA01"].copy()
    if not ga01_subset.empty:
        lag_count = int((ga01_subset["new_overall_rmse"] > ga01_subset["old_overall_rmse"]).sum())
        note_parts.append(f"GA01: new_chain still lags old_chain on {lag_count}/{int(len(ga01_subset))} runs.")

    for analyzer_id, subset in summary.groupby("analyzer_id"):
        subset = subset.sort_values("run_name").reset_index(drop=True)
        total = int(len(subset))
        old_wins = int((subset["winner_overall"] == "old_chain").sum())
        new_wins = int((subset["winner_overall"] == "new_chain").sum())
        near_old_mask = (
            pd.to_numeric(subset["new_overall_rmse"], errors="coerce")
            <= pd.to_numeric(subset["old_overall_rmse"], errors="coerce") + 0.5
        )
        near_old_count = int(near_old_mask.fillna(False).sum())
        invalid_total = int(pd.to_numeric(subset["invalid_pressure_excluded_count"], errors="coerce").fillna(0).sum())
        has_anchor = bool(subset["has_high_temp_zero_anchor_candidate"].fillna(False).any())
        included_in_all_runs = total == len(run_results)

        if analyzer_id == "GA01":
            conclusion = (
                "GA01 still lags old_chain on every available run."
                if old_wins == total and total > 0
                else "GA01 is mixed across runs and still needs dedicated diagnosis."
            )
        elif analyzer_id == "GA02":
            conclusion = (
                f"GA02 is near old_chain on {near_old_count}/{total} runs (<=0.5 ppm gap heuristic)."
                if total > 0
                else "GA02 has no successful run."
            )
        elif analyzer_id == "GA03":
            conclusion = (
                f"GA03 locally overtook old_chain on {new_wins}/{total} runs."
                if total > 0
                else "GA03 has no successful run."
            )
        elif analyzer_id == "GA04":
            conclusion = (
                "GA04 is now included in the main comparison set."
                if included_in_all_runs
                else "GA04 is only partially available across runs."
            )
        else:
            conclusion = f"{analyzer_id} has {new_wins}/{total} overall wins for new_chain."

        by_analyzer_rows.append(
            {
                "analyzer_id": analyzer_id,
                "run_count": total,
                "overall_old_chain_win_count": old_wins,
                "overall_new_chain_win_count": new_wins,
                "zero_new_chain_win_count": int((subset["winner_zero"] == "new_chain").sum()),
                "temp_new_chain_win_count": int((subset["winner_temp_stability"] == "new_chain").sum()),
                "near_old_count_within_0p5ppm": near_old_count,
                "invalid_pressure_excluded_count_total": invalid_total,
                "has_high_temp_zero_anchor_candidate_in_any_run": has_anchor,
                "included_in_all_runs": included_in_all_runs,
                "cross_run_conclusion": conclusion,
            }
        )
        auto_rows.append(
            {
                "category": analyzer_id,
                "winner": "mixed" if old_wins and new_wins else ("old_chain" if old_wins else "new_chain"),
                "summary": conclusion,
            }
        )

    auto_rows.append(
        {
            "category": "reproducibility",
            "winner": "diagnostic",
            "summary": " ".join(note_parts),
        }
    )

    by_analyzer = pd.DataFrame(by_analyzer_rows).sort_values(["analyzer_id"], ignore_index=True)
    auto_conclusions = pd.DataFrame(auto_rows)
    return summary, by_analyzer, auto_conclusions, " ".join(note_parts)
