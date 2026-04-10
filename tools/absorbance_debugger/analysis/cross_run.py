"""Cross-run summary helpers for the absorbance debugger."""

from __future__ import annotations

from typing import Any

import pandas as pd


def build_cross_run_summary(
    run_results: list[dict[str, Any]],
) -> tuple[pd.DataFrame, str]:
    """Build a cross-run summary table and a short reproducibility note."""

    rows: list[dict[str, Any]] = []
    for result in run_results:
        run_name = str(result.get("run_name", ""))
        overview = result.get("comparison_outputs", {}).get("overview_summary", pd.DataFrame())
        selection = result.get("model_results", {}).get("selection", pd.DataFrame())
        ga01_note = str(result.get("ga01_special_note", ""))
        if overview is None or getattr(overview, "empty", True):
            continue
        selection = selection.copy() if selection is not None else pd.DataFrame()
        for _, row in overview.iterrows():
            analyzer_id = str(row["analyzer_id"])
            selection_row = (
                selection[selection["analyzer_id"] == analyzer_id].iloc[0]
                if not selection.empty and (selection["analyzer_id"] == analyzer_id).any()
                else None
            )
            rows.append(
                {
                    "run_name": run_name,
                    "analyzer_id": analyzer_id,
                    "old_chain_rmse": row.get("old_chain_rmse"),
                    "new_chain_rmse": row.get("new_chain_rmse"),
                    "rmse_gap_new_minus_old": row.get("new_chain_rmse") - row.get("old_chain_rmse"),
                    "winner_overall": row.get("winner_overall"),
                    "winner_zero": row.get("winner_zero"),
                    "winner_temp_stability": row.get("winner_temp_stability"),
                    "winner_low_range": row.get("winner_low_range"),
                    "winner_main_range": row.get("winner_main_range"),
                    "best_absorbance_model": selection_row["best_absorbance_model"] if selection_row is not None else "",
                    "best_model_family": selection_row["best_model_family"] if selection_row is not None and "best_model_family" in selection_row else "",
                    "zero_residual_mode": selection_row["zero_residual_mode"] if selection_row is not None and "zero_residual_mode" in selection_row else "",
                    "selected_source_pair": selection_row["selected_source_pair"] if selection_row is not None and "selected_source_pair" in selection_row else "",
                    "ga01_special_note": ga01_note if analyzer_id == "GA01" else "",
                }
            )

    summary = pd.DataFrame(rows).sort_values(["analyzer_id", "run_name"], ignore_index=True) if rows else pd.DataFrame()
    if summary.empty:
        return summary, "Cross-run reproducibility could not be evaluated because no successful run results were collected."

    note_parts: list[str] = []
    for analyzer_id in ("GA02", "GA03"):
        subset = summary[summary["analyzer_id"] == analyzer_id].copy()
        if subset.empty:
            note_parts.append(f"{analyzer_id}: no successful batch result.")
            continue
        improved = int((subset["new_chain_rmse"] < subset["old_chain_rmse"]).sum())
        total = int(len(subset))
        if improved == total:
            note_parts.append(f"{analyzer_id}: new_chain improved on {improved}/{total} runs.")
        elif improved == 0:
            note_parts.append(f"{analyzer_id}: new_chain did not improve on any of {total} runs.")
        else:
            note_parts.append(f"{analyzer_id}: new_chain improved on {improved}/{total} runs.")
    ga01_subset = summary[summary["analyzer_id"] == "GA01"].copy()
    if not ga01_subset.empty:
        lag_count = int((ga01_subset["new_chain_rmse"] > ga01_subset["old_chain_rmse"]).sum())
        note_parts.append(f"GA01: new_chain still lags old_chain on {lag_count}/{int(len(ga01_subset))} runs.")

    return summary, " ".join(note_parts)
