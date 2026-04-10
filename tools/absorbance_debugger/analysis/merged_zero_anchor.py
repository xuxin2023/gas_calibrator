"""Merged zero-anchor diagnostics for historical cross-run evaluation."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from .pipeline import _comparison_tables, _compute_absorbance, _fit_absorbance_models, _fit_r0
from .zero_residual import build_zero_residual_point_variants, fit_zero_residual_models
from ..io.run_bundle import RunBundle
from ..plots.charts import plot_merged_zero_anchor_compare


def _metric_at_high_temp(point_reconciliation: pd.DataFrame) -> float:
    subset = point_reconciliation[
        (pd.to_numeric(point_reconciliation["temp_c"], errors="coerce") == 40.0)
        & (pd.to_numeric(point_reconciliation["target_ppm"], errors="coerce") == 0.0)
    ].copy()
    if subset.empty:
        return float("nan")
    values = pd.to_numeric(subset["new_error"], errors="coerce").dropna().to_numpy(dtype=float)
    if values.size == 0:
        return float("nan")
    return float(np.sqrt(np.mean(np.square(values))))


def build_merged_zero_anchor_compare(
    *,
    base_result: dict[str, Any],
    anchor_result: dict[str, Any],
    output_dir: Path,
) -> pd.DataFrame:
    """Re-fit R0(T) on merged zero points and compare against the base run."""

    anchor_role = anchor_result.get("run_role_assessment", pd.DataFrame())
    if getattr(anchor_role, "empty", True):
        return pd.DataFrame()
    anchor_run_row = anchor_role[anchor_role["assessment_scope"] == "run_summary"]
    if anchor_run_row.empty or not bool(anchor_run_row.iloc[0].get("has_high_temp_zero_anchor_candidate", False)):
        return pd.DataFrame()

    base_filtered = base_result["filtered"].copy()
    anchor_filtered = anchor_result["filtered"].copy()
    zero_merged = pd.concat(
        [
            base_filtered[pd.to_numeric(base_filtered["target_co2_ppm"], errors="coerce") == 0.0],
            anchor_filtered[pd.to_numeric(anchor_filtered["target_co2_ppm"], errors="coerce") == 0.0],
        ],
        ignore_index=True,
    )
    if zero_merged.empty:
        return pd.DataFrame()

    base_config = base_result["config"]
    _, _, _, merged_r0_lookup = _fit_r0(zero_merged, base_config)
    merged_absorbance_samples, merged_absorbance_points = _compute_absorbance(base_filtered, base_config, merged_r0_lookup)
    _, _, _, zero_residual_lookup = fit_zero_residual_models(merged_absorbance_points, base_config)
    merged_point_variants = build_zero_residual_point_variants(merged_absorbance_points, zero_residual_lookup, base_config)
    merged_model_results = _fit_absorbance_models(merged_point_variants, base_config, output_dir, write_outputs=False)
    merged_point_reconciliation, merged_comparison_outputs = _comparison_tables(
        RunBundle(base_config.input_path),
        base_filtered,
        merged_absorbance_samples,
        merged_absorbance_points,
        merged_model_results,
        base_config,
    )

    baseline_overview = base_result.get("comparison_outputs", {}).get("overview_summary", pd.DataFrame()).copy()
    merged_overview = merged_comparison_outputs.get("overview_summary", pd.DataFrame()).copy()
    rows: list[dict[str, Any]] = []
    analyzer_ids = sorted(
        set(baseline_overview.get("analyzer_id", pd.Series(dtype=str)).astype(str).tolist())
        | set(merged_overview.get("analyzer_id", pd.Series(dtype=str)).astype(str).tolist())
    )
    for analyzer_id in analyzer_ids:
        base_row_df = baseline_overview[baseline_overview["analyzer_id"] == analyzer_id]
        merged_row_df = merged_overview[merged_overview["analyzer_id"] == analyzer_id]
        if base_row_df.empty and merged_row_df.empty:
            continue
        base_row = base_row_df.iloc[0] if not base_row_df.empty else pd.Series(dtype=object)
        merged_row = merged_row_df.iloc[0] if not merged_row_df.empty else pd.Series(dtype=object)
        old_rmse = base_row.get("old_chain_rmse", np.nan)
        baseline_new_rmse = base_row.get("new_chain_rmse", np.nan)
        merged_new_rmse = merged_row.get("new_chain_rmse", np.nan)
        gap_baseline = baseline_new_rmse - old_rmse if pd.notna(baseline_new_rmse) and pd.notna(old_rmse) else np.nan
        gap_merged = merged_new_rmse - old_rmse if pd.notna(merged_new_rmse) and pd.notna(old_rmse) else np.nan
        selection_df = merged_model_results.get("selection", pd.DataFrame())
        selection_row = selection_df[selection_df["analyzer_id"] == analyzer_id].iloc[0] if not selection_df.empty and (selection_df["analyzer_id"] == analyzer_id).any() else pd.Series(dtype=object)
        rows.append(
            {
                "comparison_scope": "analyzer",
                "base_run_id": str(base_result.get("run_name", "")),
                "anchor_run_id": str(anchor_result.get("run_name", "")),
                "analyzer_id": analyzer_id,
                "anchor_has_40c_zero_ppm": True,
                "diagnostic_mode": "diagnostic_merged_zero_anchor_experiment",
                "old_chain_rmse": old_rmse,
                "baseline_new_chain_rmse": baseline_new_rmse,
                "merged_anchor_new_chain_rmse": merged_new_rmse,
                "baseline_zero_rmse": base_row.get("new_zero_rmse", np.nan),
                "merged_zero_rmse": merged_row.get("new_zero_rmse", np.nan),
                "baseline_temp_stability_metric": base_row.get("new_temp_stability_metric", np.nan),
                "merged_temp_stability_metric": merged_row.get("new_temp_stability_metric", np.nan),
                "baseline_high_temp_zero_rmse": _metric_at_high_temp(
                    base_result.get("point_reconciliation", pd.DataFrame()).query("analyzer_id == @analyzer_id")
                ),
                "merged_high_temp_zero_rmse": _metric_at_high_temp(
                    merged_point_reconciliation.query("analyzer_id == @analyzer_id")
                ),
                "gap_to_old_baseline": gap_baseline,
                "gap_to_old_merged": gap_merged,
                "gap_shrink_vs_old": gap_baseline - gap_merged if pd.notna(gap_baseline) and pd.notna(gap_merged) else np.nan,
                "merged_selection_source_pair": selection_row.get("selected_source_pair", ""),
            }
        )

    summary = pd.DataFrame(rows).sort_values(["analyzer_id"], ignore_index=True) if rows else pd.DataFrame()
    if not summary.empty:
        plot_merged_zero_anchor_compare(summary, output_dir / "step_05z_merged_zero_anchor_plot.png")
    return summary
