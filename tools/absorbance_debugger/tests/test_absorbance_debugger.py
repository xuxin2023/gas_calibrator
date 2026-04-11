from __future__ import annotations

import ast
import zipfile
from dataclasses import replace
from pathlib import Path

import numpy as np
import pandas as pd

from tools.absorbance_debugger import gui as gui_module
from tools.absorbance_debugger.analysis.absorbance_models import (
    PIECEWISE_MODEL_SPECS,
    _design_matrix,
    active_model_specs,
    evaluate_absorbance_models,
)
from tools.absorbance_debugger.analysis.comparison import (
    build_comparison_reconciliation_table,
    build_dual_surface_reconciliation_outputs,
    build_old_vs_new_comparison_outputs,
    build_scoped_old_vs_new_outputs,
)
from tools.absorbance_debugger.analysis.remaining_gap import build_remaining_gap_decomposition
from tools.absorbance_debugger.analysis.analyzer_sidecar import build_analyzer_sidecar_challenge
from tools.absorbance_debugger.analysis.lineage_audit import (
    build_new_chain_input_audit,
    build_old_water_correction_audit,
)
from tools.absorbance_debugger.analysis.source_policy import (
    build_source_policy_challenge,
    build_source_selection_audit,
)
from tools.absorbance_debugger.analysis.legacy_water_replay import (
    build_legacy_water_replay_features,
    load_legacy_water_replay_rules,
)
from tools.absorbance_debugger.analysis.ppm_family_challenge import (
    run_fixed_chain_ppm_family_challenge,
)
from tools.absorbance_debugger.analysis.pipeline import _identify_invalid_pressure_points
from tools.absorbance_debugger.analysis.water_zero_anchor import (
    build_water_anchor_compare,
    build_water_zero_anchor_features,
    build_water_zero_anchor_point_variants,
    fit_water_zero_anchor_models,
)
from tools.absorbance_debugger.analysis.zero_residual import (
    build_zero_residual_point_variants,
    fit_zero_residual_models,
)
from tools.absorbance_debugger.app import run_debugger, run_debugger_batch
from tools.absorbance_debugger.io.run_bundle import RunBundle, discover_run_artifacts
from tools.absorbance_debugger.models.config import DebuggerConfig
from tools.absorbance_debugger.options import (
    normalize_absorbance_order_mode,
    normalize_invalid_pressure_mode,
    normalize_model_selection_strategy,
    normalize_pressure_source,
    normalize_ratio_source,
    normalize_temp_source,
    parse_path_list,
)
from tools.absorbance_debugger.parsers.schema import classify_mode2_semantics
from tools.absorbance_debugger.reports.renderers import (
    render_comparison_reconciliation_markdown,
    render_dual_surface_reconciliation_markdown,
    render_executive_summary_markdown,
    render_old_vs_new_report_markdown,
    render_scoped_old_vs_new_report_markdown,
)


REPO_ROOT = Path(__file__).resolve().parents[3]
REFERENCE_RUN_ZIP = REPO_ROOT / "logs" / "run_20260407_185002.zip"
HISTORICAL_RUN_ZIP = REPO_ROOT / "logs" / "run_20260403_014845.zip"


def test_reference_run_generates_expected_outputs(tmp_path: Path) -> None:
    output_dir = tmp_path / "absorbance_debug"

    result = run_debugger(
        REFERENCE_RUN_ZIP,
        output_dir=output_dir,
        ratio_source="raw",
        temperature_source="corr",
        pressure_source="corr",
    )

    assert result["validation_table"]["passed"].all()
    assert (output_dir / "step_01_samples_core.csv").exists()
    assert (output_dir / "step_05_r0_fit_coefficients.csv").exists()
    assert (output_dir / "step_05y_zero_residual_observations.csv").exists()
    assert (output_dir / "step_05y_zero_residual_models.csv").exists()
    assert (output_dir / "step_05y_zero_residual_selection.csv").exists()
    assert (output_dir / "step_05y_zero_residual_plot.png").exists()
    assert (output_dir / "step_05y_legacy_water_replay_detail.csv").exists()
    assert (output_dir / "step_05y_legacy_water_replay_summary.csv").exists()
    assert (output_dir / "step_05y_legacy_water_replay_stage_metrics.csv").exists()
    assert (output_dir / "step_05y_legacy_water_replay_plot.png").exists()
    assert (output_dir / "step_00y_old_water_correction_audit.md").exists()
    assert (output_dir / "step_00y_old_water_correction_sources.csv").exists()
    assert (output_dir / "step_00z_new_chain_input_audit.csv").exists()
    assert (output_dir / "step_05w_water_zero_anchor_features.csv").exists()
    assert (output_dir / "step_05w_water_zero_anchor_models.csv").exists()
    assert (output_dir / "step_05w_water_zero_anchor_selection.csv").exists()
    assert (output_dir / "step_05w_water_zero_anchor_plot.png").exists()
    assert (output_dir / "step_06_absorbance_model_candidates.csv").exists()
    assert (output_dir / "step_06_absorbance_model_scores.csv").exists()
    assert (output_dir / "step_06_absorbance_model_selection.csv").exists()
    assert (output_dir / "step_06_absorbance_model_coefficients.csv").exists()
    assert (output_dir / "step_06_absorbance_model_residuals.csv").exists()
    assert (output_dir / "step_06y_piecewise_model_candidates.csv").exists()
    assert (output_dir / "step_06y_piecewise_model_scores.csv").exists()
    assert (output_dir / "step_06y_piecewise_model_selection.csv").exists()
    assert (output_dir / "step_06y_piecewise_model_coefficients.csv").exists()
    assert (output_dir / "step_06y_piecewise_model_plots.png").exists()
    assert (output_dir / "step_06y_weight_sensitivity_compare.csv").exists()
    assert (output_dir / "step_06y_ppm_family_challenge_detail.csv").exists()
    assert (output_dir / "step_06y_ppm_family_challenge_summary.csv").exists()
    assert (output_dir / "step_06y_ppm_family_challenge_plot.png").exists()
    assert (output_dir / "step_02x_invalid_pressure_points.csv").exists()
    assert (output_dir / "step_02x_invalid_pressure_summary.csv").exists()
    assert (output_dir / "step_02x_invalid_pressure_plots.png").exists()
    assert (output_dir / "step_06x_absorbance_order_compare.csv").exists()
    assert (output_dir / "step_05x_r0_source_consistency.csv").exists()
    assert (output_dir / "step_04x_pressure_branch_compare.csv").exists()
    assert (output_dir / "step_08x_upper_bound_vs_deployable.csv").exists()
    assert (output_dir / "step_08x_root_cause_ranking.csv").exists()
    assert (output_dir / "step_08x_valid_only_overview_summary.csv").exists()
    assert (output_dir / "step_08x_valid_only_by_temperature.csv").exists()
    assert (output_dir / "step_08x_valid_only_zero_special.csv").exists()
    assert (output_dir / "step_08x_valid_only_auto_conclusions.csv").exists()
    assert (output_dir / "step_08x_default_chain_before_after.csv").exists()
    assert (output_dir / "step_08w_water_anchor_compare.csv").exists()
    assert (output_dir / "step_08w_water_anchor_compare_plots.png").exists()
    assert (output_dir / "step_04w_pressure_data_assessment.csv").exists()
    assert (output_dir / "step_08y_ga01_residual_profile.csv").exists()
    assert (output_dir / "step_08y_ga01_residual_profile_plots.png").exists()
    assert (output_dir / "step_08_old_vs_new_compare.xlsx").exists()
    assert (output_dir / "step_08_overview_summary.csv").exists()
    assert (output_dir / "step_08_by_temperature.csv").exists()
    assert (output_dir / "step_08_by_concentration_range.csv").exists()
    assert (output_dir / "step_08_zero_special.csv").exists()
    assert (output_dir / "step_08_regression_overall.csv").exists()
    assert (output_dir / "step_08_regression_by_temperature.csv").exists()
    assert (output_dir / "step_08_point_reconciliation.csv").exists()
    assert (output_dir / "step_08_auto_conclusions.csv").exists()
    assert (output_dir / "step_08y_legacy_water_replay_conclusions.csv").exists()
    assert (output_dir / "step_08y_ppm_family_challenge_conclusions.csv").exists()
    assert (output_dir / "step_06x_source_selection_audit_detail.csv").exists()
    assert (output_dir / "step_06x_source_selection_audit_summary.csv").exists()
    assert (output_dir / "step_08x_source_selection_audit_conclusions.csv").exists()
    assert (output_dir / "step_06x_source_policy_challenge_detail.csv").exists()
    assert (output_dir / "step_06x_source_policy_challenge_summary.csv").exists()
    assert (output_dir / "step_06x_source_policy_challenge_plot.png").exists()
    assert (output_dir / "step_08x_source_policy_challenge_conclusions.csv").exists()
    assert (output_dir / "step_07x_remaining_gap_decomposition_detail.csv").exists()
    assert (output_dir / "step_07x_remaining_gap_decomposition_summary.csv").exists()
    assert (output_dir / "step_07x_remaining_gap_decomposition_plot.png").exists()
    assert (output_dir / "step_08x_remaining_gap_decomposition_conclusions.csv").exists()
    assert (output_dir / "step_07x_ga01_sidecar_detail.csv").exists()
    assert (output_dir / "step_07x_ga01_sidecar_summary.csv").exists()
    assert (output_dir / "step_07x_ga01_sidecar_plot.png").exists()
    assert (output_dir / "step_08x_ga01_sidecar_conclusions.csv").exists()
    assert (output_dir / "step_09_old_vs_new_comparison_detail.csv").exists()
    assert (output_dir / "step_09_old_vs_new_comparison_summary.csv").exists()
    assert (output_dir / "step_09_old_vs_new_local_wins.csv").exists()
    assert (output_dir / "step_09_old_vs_new_plot.png").exists()
    assert (output_dir / "step_10_old_vs_new_report.md").exists()
    assert (output_dir / "report.md").exists()
    assert (output_dir / "report.html").exists()
    assert (output_dir / "report.xlsx").exists()

    filtered = pd.read_csv(output_dir / "step_02_samples_filtered.csv")
    excluded = pd.read_csv(output_dir / "step_02_excluded_rows.csv")
    assert sorted(filtered["analyzer"].unique().tolist()) == ["GA01", "GA02", "GA03"]
    assert excluded["exclude_reason"].str.contains("warning_only_analyzer").any()

    overview = pd.read_csv(output_dir / "step_08_overview_summary.csv")
    assert set(overview["analyzer_id"]) == {"GA01", "GA02", "GA03"}
    assert {"winner_overall", "winner_zero", "winner_temp_stability", "winner_low_range", "winner_main_range", "recommendation"} <= set(overview.columns)

    selection = pd.read_csv(output_dir / "step_06_absorbance_model_selection.csv")
    assert set(selection["analyzer_id"]) == {"GA01", "GA02", "GA03"}
    assert {"best_absorbance_model", "best_model_family", "zero_residual_mode", "selection_reason", "selected_prediction_scope", "selected_source_pair", "default_absorbance_order"} <= set(selection.columns)
    assert set(selection["selected_source_pair"]) <= {"raw/raw", "filt/filt"}
    assert set(selection["default_absorbance_order"]) == {"samplewise_log_first"}

    scores = pd.read_csv(output_dir / "step_06_absorbance_model_scores.csv")
    assert {"model_id", "model_family", "validation_rmse", "overall_rmse", "composite_score", "composite_score_old_weights", "composite_score_new_weights", "model_rank", "selected_source_pair"} <= set(scores.columns)
    assert scores["model_rank"].notna().all()
    assert not scores["selected_source_pair"].isin(["raw/filt", "filt/raw"]).any()

    order_compare = pd.read_csv(output_dir / "step_06x_absorbance_order_compare.csv")
    assert {"order_mode", "samplewise_log_first_is_better", "significant_order_gain"} <= set(order_compare.columns)
    assert set(order_compare["order_mode"]) == {"samplewise_log_first", "mean_first_log"}

    source_compare = pd.read_csv(output_dir / "step_05x_r0_source_consistency.csv")
    assert {"source_pair_label", "mixed_source_invalid_for_production_default"} <= set(source_compare.columns)
    assert {"raw/raw", "filt/filt", "raw/filt", "filt/raw"} <= set(source_compare["source_pair_label"])

    pressure_branch = pd.read_csv(output_dir / "step_04x_pressure_branch_compare.csv")
    assert {"pressure_branch", "branch_rank", "recommended_pressure_branch"} <= set(pressure_branch.columns)
    assert {"no_pressure_norm", "pressure_std", "pressure_corr"} <= set(pressure_branch["pressure_branch"])

    upper_vs_deployable = pd.read_csv(output_dir / "step_08x_upper_bound_vs_deployable.csv")
    assert {"chain_context", "best_model_upper_bound", "best_model_deployable", "best_model_consistent"} <= set(upper_vs_deployable.columns)
    assert {"physics_upper_bound", "deployable_chain"} <= set(upper_vs_deployable["chain_context"])

    root_causes = pd.read_csv(output_dir / "step_08x_root_cause_ranking.csv")
    assert {"rank", "issue_name", "severity", "evidence", "recommended_action"} <= set(root_causes.columns)
    assert "weak_absorbance_ppm_model" in set(root_causes["issue_name"])

    point_reconciliation = pd.read_csv(output_dir / "step_08_point_reconciliation.csv")
    assert {"old_pred_ppm", "new_pred_ppm", "old_error", "new_error", "winner_for_point", "selected_source_pair"} <= set(point_reconciliation.columns)
    assert point_reconciliation["pressure_source"].isin(["P_std", "P_corr"]).all()
    assert point_reconciliation["temperature_source"].isin(["T_std", "T_corr"]).all()
    assert point_reconciliation["best_absorbance_model"].notna().any()
    assert point_reconciliation["selected_prediction_scope"].isin(["validation_oof", "overall_fit"]).all()
    assert set(point_reconciliation["absorbance_order_mode_selected"]) == {"samplewise_log_first"}
    assert set(point_reconciliation["selected_source_pair"].dropna().unique().tolist()) <= {"raw/raw", "filt/filt"}

    selected_models = selection.set_index("analyzer_id")["best_absorbance_model"].to_dict()
    point_models = (
        point_reconciliation.dropna(subset=["best_absorbance_model"])
        .groupby("analyzer_id")["best_absorbance_model"]
        .agg(lambda values: values.mode().iloc[0])
        .to_dict()
    )
    assert point_models == selected_models

    legacy_detail = pd.read_csv(output_dir / "step_05y_legacy_water_replay_detail.csv")
    legacy_stage = pd.read_csv(output_dir / "step_05y_legacy_water_replay_stage_metrics.csv")
    legacy_conclusions = pd.read_csv(output_dir / "step_08y_legacy_water_replay_conclusions.csv")
    legacy_summary = pd.read_csv(output_dir / "step_05y_legacy_water_replay_summary.csv")
    assert {
        "analyzer_id",
        "mode2_semantic_profile",
        "mode2_legacy_raw_compare_safe",
        "mode2_is_baseline_bearing_profile",
        "selected_source_pair",
        "fixed_best_model",
        "fixed_model_family",
        "fixed_zero_residual_mode",
        "fixed_prediction_scope",
        "water_lineage_mode",
        "uses_co2_temp_groups",
        "uses_co2_zero_ppm_rows",
        "uses_subzero_zero_water_anchor",
        "subzero_anchor_row_count",
        "zero_ppm_anchor_row_count",
        "overall_rmse",
        "zero_rmse",
        "low_range_rmse",
        "main_range_rmse",
        "temp_bias_spread",
        "old_chain_overall_rmse",
        "gap_to_old_overall",
        "gap_to_old_zero",
        "delta_vs_none_overall",
        "delta_vs_none_zero",
        "delta_vs_none_low",
        "delta_vs_none_main",
        "gap_closed_ratio_vs_current_new_chain",
        "gap_closed_ratio_raw",
        "gap_closed_ratio_capped_0_100",
        "crossed_old_chain_flag",
        "overclosure_ratio",
        "laggard_only_weighted_gap_closed_ratio_capped",
        "laggard_only_analyzer_count",
    } <= set(legacy_detail.columns)
    assert set(legacy_detail["water_lineage_mode"]) == {
        "none",
        "simplified_subzero_anchor",
        "legacy_h2o_summary_selection",
        "legacy_h2o_summary_selection_plus_zero_ppm_rows",
    }
    assert legacy_detail.groupby("analyzer_id")["water_lineage_mode"].nunique().eq(4).all()

    fixed_check = legacy_detail.merge(
        selection[
            [
                "analyzer_id",
                "selected_source_pair",
                "best_absorbance_model",
                "best_model_family",
                "zero_residual_mode",
                "selected_prediction_scope",
            ]
        ],
        on="analyzer_id",
        how="left",
        suffixes=("_legacy", "_selection"),
    )
    assert (fixed_check["selected_source_pair_legacy"] == fixed_check["selected_source_pair_selection"]).all()
    assert (fixed_check["fixed_best_model"] == fixed_check["best_absorbance_model"]).all()
    assert (fixed_check["fixed_model_family"] == fixed_check["best_model_family"]).all()
    assert (fixed_check["fixed_zero_residual_mode"] == fixed_check["zero_residual_mode"]).all()
    assert (fixed_check["fixed_prediction_scope"] == fixed_check["selected_prediction_scope"]).all()

    assert {"layer", "metric_name", "metric_value", "gain_vs_none"} <= set(legacy_stage.columns)
    assert {"zero_temp", "absorbance", "final_ppm"} <= set(legacy_stage["layer"])
    assert {"residual_spread_before_replay", "residual_spread_after_replay", "gap_closed_ratio_vs_current_new_chain", "gap_closed_ratio_capped_0_100"} <= set(
        legacy_stage["metric_name"]
    )

    assert {
        "summary_scope",
        "mode2_semantic_profile",
        "mode2_legacy_raw_compare_safe",
        "mode2_is_baseline_bearing_profile",
        "gap_closed_ratio_capped_0_100",
        "laggard_only_weighted_gap_closed_ratio_capped",
        "laggard_only_analyzer_count",
    } <= set(legacy_summary.columns)
    assert {"question_id", "question", "answer", "evidence", "recommended_mode", "mode2_semantic_profile"} <= set(legacy_conclusions.columns)
    assert {
        "legacy_gap_closure",
        "dominant_layer",
        "most_dependent_analyzer",
        "statement_support",
        "remaining_main_causes",
    } <= set(legacy_conclusions["question_id"])

    ppm_detail = pd.read_csv(output_dir / "step_06y_ppm_family_challenge_detail.csv")
    ppm_summary = pd.read_csv(output_dir / "step_06y_ppm_family_challenge_summary.csv")
    ppm_conclusions = pd.read_csv(output_dir / "step_08y_ppm_family_challenge_conclusions.csv")
    assert {
        "analyzer_id",
        "mode2_semantic_profile",
        "mode2_legacy_raw_compare_safe",
        "mode2_is_baseline_bearing_profile",
        "selected_source_pair",
        "fixed_absorbance_proxy",
        "fixed_zero_residual_mode",
        "fixed_prediction_scope",
        "fixed_water_lineage_mode",
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
    } <= set(ppm_detail.columns)
    assert {
        "current_fixed_family",
        "v5_abs_k_minimal",
        "legacy_humidity_cross_D",
        "legacy_humidity_cross_E",
    } <= set(ppm_detail["ppm_family_mode"])
    current_fixed = ppm_detail[ppm_detail["ppm_family_mode"] == "current_fixed_family"]
    fixed_ppm_check = current_fixed.merge(
        selection[["analyzer_id", "best_absorbance_model", "best_model_family", "selected_source_pair", "zero_residual_mode", "selected_prediction_scope"]],
        on="analyzer_id",
        how="left",
    )
    assert (fixed_ppm_check["fixed_best_model"] == fixed_ppm_check["best_absorbance_model"]).all()
    assert (fixed_ppm_check["fixed_model_family"] == fixed_ppm_check["best_model_family"]).all()
    assert (fixed_ppm_check["selected_source_pair_x"] == fixed_ppm_check["selected_source_pair_y"]).all()
    assert (fixed_ppm_check["fixed_zero_residual_mode"] == fixed_ppm_check["zero_residual_mode"]).all()
    assert (fixed_ppm_check["fixed_prediction_scope"] == fixed_ppm_check["selected_prediction_scope"]).all()
    assert {"summary_scope", "mode2_semantic_profile", "ppm_family_mode", "laggard_only_weighted_gap_closed_ratio_capped"} <= set(ppm_summary.columns)
    assert {"question_id", "question", "answer", "evidence", "recommended_ppm_family_mode", "mode2_semantic_profile"} <= set(ppm_conclusions.columns)

    source_selection_audit_detail = pd.read_csv(output_dir / "step_06x_source_selection_audit_detail.csv")
    source_selection_audit_summary = pd.read_csv(output_dir / "step_06x_source_selection_audit_summary.csv")
    source_selection_audit_conclusions = pd.read_csv(output_dir / "step_08x_source_selection_audit_conclusions.csv")
    assert {
        "analyzer_id",
        "mode2_semantic_profile",
        "raw_available_flag",
        "filt_available_flag",
        "raw_required_keys_pass",
        "filt_required_keys_pass",
        "raw_quality_gate_pass",
        "filt_quality_gate_pass",
        "raw_nonpositive_ratio_count",
        "filt_nonpositive_ratio_count",
        "raw_missing_count",
        "filt_missing_count",
        "signal_priority_prefers_filt_flag",
        "raw_score_if_forced",
        "filt_score_if_forced",
        "selected_source_pair",
        "selection_reason_primary",
        "selection_reason_secondary",
        "actual_ratio_source_used",
    } <= set(source_selection_audit_detail.columns)
    assert {
        "summary_scope",
        "designed_v5_ratio_source_intent",
        "actual_ratio_source_used_in_this_run",
        "why_selected_source_pair_became_mixed",
    } <= set(source_selection_audit_summary.columns)
    assert {"question_id", "question", "answer", "evidence"} <= set(source_selection_audit_conclusions.columns)

    source_policy_challenge_detail = pd.read_csv(output_dir / "step_06x_source_policy_challenge_detail.csv")
    source_policy_challenge_summary = pd.read_csv(output_dir / "step_06x_source_policy_challenge_summary.csv")
    source_policy_challenge_conclusions = pd.read_csv(output_dir / "step_08x_source_policy_challenge_conclusions.csv")
    assert {
        "analyzer_id",
        "mode2_semantic_profile",
        "fixed_model_family",
        "fixed_zero_residual_mode",
        "fixed_prediction_scope",
        "source_policy_mode",
        "selected_source_pair_under_policy",
        "overall_rmse",
        "zero_rmse",
        "low_range_rmse",
        "main_range_rmse",
        "old_chain_overall_rmse",
        "gap_to_old_overall",
        "delta_vs_current_mixed_overall",
        "delta_vs_current_mixed_zero",
        "delta_vs_current_mixed_low",
        "delta_vs_current_mixed_main",
        "pointwise_win_count_vs_old",
        "pointwise_loss_count_vs_old",
        "pointwise_win_count_vs_current_mixed",
        "pointwise_loss_count_vs_current_mixed",
        "local_win_examples",
        "local_loss_examples",
    } <= set(source_policy_challenge_detail.columns)
    assert {
        "source_policy_mode",
        "overall_rmse",
        "delta_vs_current_mixed_overall",
        "whether_improves_current_deployable_result",
    } <= set(source_policy_challenge_summary.columns)
    assert {"question_id", "question", "answer", "evidence"} <= set(source_policy_challenge_conclusions.columns)
    assert {
        "current_deployable_mixed",
        "raw_first_with_fallback",
        "raw_only_strict",
        "filt_only_strict",
    } <= set(source_policy_challenge_detail["source_policy_mode"])

    remaining_gap_detail = pd.read_csv(output_dir / "step_07x_remaining_gap_decomposition_detail.csv")
    remaining_gap_summary = pd.read_csv(output_dir / "step_07x_remaining_gap_decomposition_summary.csv")
    remaining_gap_conclusions = pd.read_csv(output_dir / "step_08x_remaining_gap_decomposition_conclusions.csv")
    assert {
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
        "contribution_rank_global",
        "contribution_rank_within_analyzer",
        "selected_source_pair",
        "fixed_model_family",
        "fixed_zero_residual_mode",
        "fixed_prediction_scope",
    } <= set(remaining_gap_detail.columns)
    assert {
        "remaining_gap_total",
        "remaining_gap_from_ga01",
        "remaining_gap_from_ga02",
        "remaining_gap_from_ga03",
        "remaining_gap_share_by_analyzer",
        "remaining_gap_share_by_segment",
        "remaining_gap_share_by_temp",
        "remaining_gap_share_by_target_ppm",
        "top_10_gap_contributor_points",
        "laggard_analyzer_primary",
        "laggard_segment_primary",
    } <= set(remaining_gap_summary.columns)
    assert {"question_id", "question", "answer", "evidence", "laggard_analyzer_primary", "laggard_segment_primary"} <= set(remaining_gap_conclusions.columns)
    assert remaining_gap_summary["laggard_analyzer_primary"].notna().all()

    ga01_sidecar_detail = pd.read_csv(output_dir / "step_07x_ga01_sidecar_detail.csv")
    ga01_sidecar_summary = pd.read_csv(output_dir / "step_07x_ga01_sidecar_summary.csv")
    ga01_sidecar_conclusions = pd.read_csv(output_dir / "step_08x_ga01_sidecar_conclusions.csv")
    assert {
        "analyzer_id",
        "mode2_semantic_profile",
        "sidecar_mode",
        "selected_source_pair",
        "fixed_model_family",
        "fixed_zero_residual_mode",
        "fixed_prediction_scope",
        "zero_rmse",
        "low_range_rmse",
        "main_range_rmse",
        "overall_rmse",
        "temp_bias_spread",
        "gap_to_old_overall",
        "gap_to_old_zero",
        "delta_vs_current_ga01_overall",
        "delta_vs_current_ga01_zero",
        "delta_vs_current_ga01_low",
        "delta_vs_current_ga01_main",
        "pointwise_win_count_vs_old",
        "pointwise_win_count_vs_current_ga01",
        "local_win_examples",
        "local_loss_examples",
    } <= set(ga01_sidecar_detail.columns)
    assert {
        "summary_scope",
        "laggard_analyzer_primary",
        "sidecar_mode",
        "best_sidecar_mode",
        "global_remaining_gap_if_applied_sidecar",
        "global_remaining_gap_delta_vs_current",
    } <= set(ga01_sidecar_summary.columns)
    assert {"question_id", "question", "answer", "evidence", "laggard_analyzer_primary", "best_sidecar_mode"} <= set(ga01_sidecar_conclusions.columns)
    assert set(ga01_sidecar_summary["sidecar_mode"]) >= {
        "current_global_fixed",
        "ga01_same_family_refit",
        "ga01_humidity_cross_residual",
        "ga01_temp_piecewise_residual",
        "ga01_humidity_plus_temp_residual",
    }

    old_vs_new_detail = pd.read_csv(output_dir / "step_09_old_vs_new_comparison_detail.csv")
    old_vs_new_summary = pd.read_csv(output_dir / "step_09_old_vs_new_comparison_summary.csv")
    old_vs_new_local_wins = pd.read_csv(output_dir / "step_09_old_vs_new_local_wins.csv")
    assert {
        "analyzer_id",
        "mode2_semantic_profile",
        "mode2_legacy_raw_compare_safe",
        "selected_source_pair",
        "actual_ratio_source_used",
        "actual_ratio_source_evidence",
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
        "old_chain_mae",
        "new_chain_mae",
        "point_count",
        "pointwise_win_count",
        "pointwise_loss_count",
        "pointwise_tie_count",
        "overall_win_flag",
        "zero_win_flag",
        "low_win_flag",
        "main_win_flag",
    } <= set(old_vs_new_detail.columns)
    assert {
        "comparison_scope",
        "overall_verdict",
        "overall_rmse_old",
        "overall_rmse_new",
        "overall_improvement_pct",
        "analyzer_overall_wins",
        "analyzer_zero_wins",
        "analyzer_low_wins",
        "analyzer_main_wins",
        "total_pointwise_wins",
        "total_pointwise_losses",
        "analyzers_fully_beating_old_count",
        "analyzers_partially_beating_old_count",
        "analyzers_still_lagging_count",
        "actual_ratio_source_used_majority",
        "designed_v5_ratio_source_intent",
        "main_caveat",
        "whether_new_chain_has_overall_evidence_to_surpass_old",
    } <= set(old_vs_new_summary.columns)
    assert {
        "analyzer_id",
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
    } <= set(old_vs_new_local_wins.columns)
    assert {"zero", "low", "main"} <= set(old_vs_new_local_wins["segment_tag"])

    old_vs_new_report = (output_dir / "step_10_old_vs_new_report.md").read_text(encoding="utf-8")
    assert "old_chain" in old_vs_new_report
    assert "current_deployable_new_chain" in old_vs_new_report
    assert "actual_ratio_source_used_in_this_run" in old_vs_new_report
    assert "designed_v5_ratio_source_intent = raw_or_instantaneous" in old_vs_new_report
    assert "selection_reason_primary" in old_vs_new_report
    assert "whether_raw_first_improves_current_deployable_result" in old_vs_new_report
    headline_block = old_vs_new_report.split("## 2. overall comparison", 1)[0]
    assert "best_legacy_water_replay_candidate" not in headline_block
    assert "best_ppm_family_challenge_candidate" not in headline_block


def test_old_water_correction_audit_and_new_chain_input_audit_can_run(tmp_path: Path) -> None:
    markdown, sources, summary = build_old_water_correction_audit()

    assert "Old Water Correction Audit" in markdown
    assert isinstance(summary, list)
    assert isinstance(sources, pd.DataFrame)

    config = DebuggerConfig(input_path=tmp_path, output_dir=tmp_path)
    selected_source_summary = pd.DataFrame(
        [
            {
                "analyzer_id": "GA01",
                "selected_ratio_source": "ratio_co2_raw",
                "selected_source_pair": "raw/raw",
                "zero_residual_mode": "linear",
            }
        ]
    )
    samples_core = pd.DataFrame(
        [
            {
                "analyzer": "GA01",
                "ratio_h2o_raw": 0.11,
                "ratio_h2o_filt": 0.10,
                "h2o_signal": 1.0,
                "h2o_density": 2.0,
                "h2o_mmol": 3.0,
            }
        ]
    )
    zero_residual_selection = pd.DataFrame(
        [
            {
                "analyzer_id": "GA01",
                "ratio_source": "ratio_co2_raw",
                "selected_zero_residual_model": "linear",
            }
        ]
    )

    audit = build_new_chain_input_audit(
        config=config,
        selected_source_summary=selected_source_summary,
        samples_core=samples_core,
        zero_residual_selection=zero_residual_selection,
    )

    assert not audit.empty
    assert audit["uses_h2o_ratio"].eq(False).all()
    assert audit["uses_water_baseline_or_anchor"].eq(False).all()
    assert "ratio_h2o_raw" in audit.iloc[0]["available_h2o_fields_in_samples"]


def test_old_vs_new_final_comparison_report_keeps_deployable_headline() -> None:
    point_reconciliation = pd.DataFrame(
        [
            {
                "analyzer_id": "GA01",
                "temp_c": 0.0,
                "target_ppm": 0.0,
                "old_pred_ppm": 8.0,
                "new_pred_ppm": 3.0,
                "old_error": 8.0,
                "new_error": 3.0,
                "winner_for_point": "new_chain",
                "ratio_source_selected": "raw",
                "selected_source_pair": "raw/raw",
                "mode2_semantic_profile": "legacy_ratio_raw_profile",
                "mode2_legacy_raw_compare_safe": True,
                "point_title": "p1",
                "point_row": 1,
            },
            {
                "analyzer_id": "GA01",
                "temp_c": 20.0,
                "target_ppm": 100.0,
                "old_pred_ppm": 112.0,
                "new_pred_ppm": 104.0,
                "old_error": 12.0,
                "new_error": 4.0,
                "winner_for_point": "new_chain",
                "ratio_source_selected": "raw",
                "selected_source_pair": "raw/raw",
                "mode2_semantic_profile": "legacy_ratio_raw_profile",
                "mode2_legacy_raw_compare_safe": True,
                "point_title": "p2",
                "point_row": 2,
            },
            {
                "analyzer_id": "GA01",
                "temp_c": 20.0,
                "target_ppm": 400.0,
                "old_pred_ppm": 418.0,
                "new_pred_ppm": 406.0,
                "old_error": 18.0,
                "new_error": 6.0,
                "winner_for_point": "new_chain",
                "ratio_source_selected": "raw",
                "selected_source_pair": "raw/raw",
                "mode2_semantic_profile": "legacy_ratio_raw_profile",
                "mode2_legacy_raw_compare_safe": True,
                "point_title": "p3",
                "point_row": 3,
            },
            {
                "analyzer_id": "GA02",
                "temp_c": 0.0,
                "target_ppm": 0.0,
                "old_pred_ppm": 2.0,
                "new_pred_ppm": 5.0,
                "old_error": 2.0,
                "new_error": 5.0,
                "winner_for_point": "old_chain",
                "ratio_source_selected": "filt",
                "selected_source_pair": "filt/filt",
                "mode2_semantic_profile": "baseline_bearing_profile",
                "mode2_legacy_raw_compare_safe": False,
                "point_title": "p1",
                "point_row": 1,
            },
            {
                "analyzer_id": "GA02",
                "temp_c": 20.0,
                "target_ppm": 100.0,
                "old_pred_ppm": 104.0,
                "new_pred_ppm": 109.0,
                "old_error": 4.0,
                "new_error": 9.0,
                "winner_for_point": "old_chain",
                "ratio_source_selected": "filt",
                "selected_source_pair": "filt/filt",
                "mode2_semantic_profile": "baseline_bearing_profile",
                "mode2_legacy_raw_compare_safe": False,
                "point_title": "p2",
                "point_row": 2,
            },
            {
                "analyzer_id": "GA02",
                "temp_c": 20.0,
                "target_ppm": 400.0,
                "old_pred_ppm": 407.0,
                "new_pred_ppm": 413.0,
                "old_error": 7.0,
                "new_error": 13.0,
                "winner_for_point": "old_chain",
                "ratio_source_selected": "filt",
                "selected_source_pair": "filt/filt",
                "mode2_semantic_profile": "baseline_bearing_profile",
                "mode2_legacy_raw_compare_safe": False,
                "point_title": "p3",
                "point_row": 3,
            },
        ]
    )
    selection = pd.DataFrame(
        [
            {"analyzer_id": "GA01", "selected_source_pair": "raw/raw", "selected_ratio_source": "ratio_co2_raw"},
            {"analyzer_id": "GA02", "selected_source_pair": "filt/filt", "selected_ratio_source": "ratio_co2_filt"},
        ]
    )
    new_chain_input_audit = pd.DataFrame(
        [
            {"audit_scope": "per_analyzer_selected_main_chain", "analyzer_id": "GA01", "R_in_source": "ratio_co2_raw", "R0_fit_source": "ratio_co2_raw"},
            {"audit_scope": "per_analyzer_selected_main_chain", "analyzer_id": "GA02", "R_in_source": "ratio_co2_filt", "R0_fit_source": "ratio_co2_filt"},
        ]
    )
    legacy_water_replay_detail = pd.DataFrame(
        [
            {"analyzer_id": "GA01", "water_lineage_mode": "none", "overall_rmse": 5.0, "old_chain_overall_rmse": 9.0},
            {"analyzer_id": "GA02", "water_lineage_mode": "none", "overall_rmse": 10.0, "old_chain_overall_rmse": 6.0},
            {"analyzer_id": "GA01", "water_lineage_mode": "simplified_subzero_anchor", "overall_rmse": 4.0, "old_chain_overall_rmse": 9.0},
            {"analyzer_id": "GA02", "water_lineage_mode": "simplified_subzero_anchor", "overall_rmse": 8.0, "old_chain_overall_rmse": 6.0},
        ]
    )
    ppm_family_detail = pd.DataFrame(
        [
            {"analyzer_id": "GA01", "ppm_family_mode": "current_fixed_family", "overall_rmse": 5.0, "old_chain_overall_rmse": 9.0},
            {"analyzer_id": "GA02", "ppm_family_mode": "current_fixed_family", "overall_rmse": 10.0, "old_chain_overall_rmse": 6.0},
            {"analyzer_id": "GA01", "ppm_family_mode": "legacy_humidity_cross_D", "overall_rmse": 4.5, "old_chain_overall_rmse": 9.0},
            {"analyzer_id": "GA02", "ppm_family_mode": "legacy_humidity_cross_D", "overall_rmse": 8.5, "old_chain_overall_rmse": 6.0},
        ]
    )
    water_anchor_compare = pd.DataFrame(
        [
            {"analyzer_id": "GA01", "baseline_overall_rmse": 5.0, "water_anchor_overall_rmse": 4.4, "old_chain_rmse": 9.0, "water_zero_anchor_mode": "linear"},
            {"analyzer_id": "GA02", "baseline_overall_rmse": 10.0, "water_anchor_overall_rmse": 8.2, "old_chain_rmse": 6.0, "water_zero_anchor_mode": "linear"},
        ]
    )

    outputs = build_old_vs_new_comparison_outputs(
        point_reconciliation=point_reconciliation,
        selection_table=selection,
        new_chain_input_audit=new_chain_input_audit,
        legacy_water_replay_detail=legacy_water_replay_detail,
        ppm_family_challenge_detail=ppm_family_detail,
        water_anchor_compare=water_anchor_compare,
    )

    assert {"comparison_scope", "overall_verdict", "actual_ratio_source_used_majority"} <= set(outputs["summary"].columns)
    assert {"actual_ratio_source_used", "actual_ratio_source_evidence"} <= set(outputs["detail"].columns)
    assert {"local_win_flag", "local_win_rank_within_analyzer", "segment_tag"} <= set(outputs["local_wins"].columns)

    report = render_old_vs_new_report_markdown(
        {
            "run_name": "synthetic_run",
            "summary": outputs["summary"],
            "detail": outputs["detail"],
            "aggregate_segments": outputs["aggregate_segments"],
            "local_wins": outputs["local_wins"],
            "ratio_source_audit": outputs["ratio_source_audit"],
            "diagnostic_candidates": outputs["diagnostic_candidates"],
        }
    )

    assert "headline_comparison_scope = old_chain vs current_deployable_new_chain" in report
    assert "actual_ratio_source_used_in_this_run = mixed" in report
    headline_block = report.split("## 2. overall comparison", 1)[0]
    assert "best_legacy_water_replay_candidate" not in headline_block
    assert "best_ppm_family_challenge_candidate" not in headline_block


def _synthetic_scoped_run_result(run_id: str, output_dir: Path) -> dict[str, object]:
    analyzer_specs: dict[str, dict[str, tuple[float, float, float]]] = {
        "run_20260403_014845": {
            "GA01": {"zero": (4.0, 6.0, 0.0), "low": (8.0, 10.0, 100.0), "main": (10.0, 13.0, 400.0)},
            "GA02": {"zero": (3.0, 1.0, 0.0), "low": (7.0, 3.0, 100.0), "main": (9.0, 4.0, 400.0)},
            "GA03": {"zero": (2.5, 1.5, 0.0), "low": (6.0, 3.0, 100.0), "main": (7.5, 4.5, 400.0)},
        },
        "run_20260407_185002": {
            "GA01": {"zero": (5.0, 6.5, 0.0), "low": (7.5, 9.0, 100.0), "main": (9.0, 12.0, 400.0)},
            "GA02": {"zero": (2.8, 0.9, 0.0), "low": (6.2, 2.6, 100.0), "main": (8.4, 3.8, 400.0)},
            "GA03": {"zero": (2.0, 1.0, 0.0), "low": (5.4, 2.8, 100.0), "main": (7.0, 4.0, 400.0)},
        },
        "run_20260410_132440": {
            "GA01": {"zero": (3.0, 4.5, 0.0), "low": (6.0, 7.5, 100.0), "main": (8.0, 9.5, 400.0)},
            "GA02": {"zero": (2.5, 1.2, 0.0), "low": (5.5, 2.5, 100.0), "main": (7.0, 3.5, 400.0)},
            "GA03": {"zero": (2.0, 1.1, 0.0), "low": (4.8, 2.2, 100.0), "main": (6.4, 3.0, 400.0)},
            "GA04": {"zero": (2.2, 2.4, 0.0), "low": (5.0, 4.5, 100.0), "main": (6.2, 6.7, 400.0)},
        },
    }
    source_pairs = {
        "GA01": ("raw/raw", "ratio_co2_raw"),
        "GA02": ("filt/filt", "ratio_co2_filt"),
        "GA03": ("raw/raw", "ratio_co2_raw"),
        "GA04": ("filt/filt", "ratio_co2_filt"),
    }

    rows: list[dict[str, object]] = []
    filtered_rows: list[dict[str, object]] = []
    selection_rows: list[dict[str, object]] = []
    audit_rows: list[dict[str, object]] = []
    for analyzer_id, segments in analyzer_specs[run_id].items():
        selected_source_pair, selected_ratio_source = source_pairs[analyzer_id]
        selected_prediction_scope = "overall_fit" if analyzer_id == "GA04" else "validation_oof"
        selection_rows.append(
            {
                "analyzer_id": analyzer_id,
                "selected_source_pair": selected_source_pair,
                "selected_ratio_source": selected_ratio_source,
                "best_absorbance_model": f"{analyzer_id.lower()}_model",
                "best_model_family": "single_range",
                "zero_residual_mode": "linear",
                "selected_prediction_scope": selected_prediction_scope,
            }
        )
        audit_rows.append(
            {
                "audit_scope": "per_analyzer_selected_main_chain",
                "analyzer_id": analyzer_id,
                "R_in_source": selected_ratio_source,
                "R0_fit_source": selected_ratio_source,
            }
        )
        for idx, (segment_tag, (old_error, new_error, target_ppm)) in enumerate(segments.items(), start=1):
            temp_c = 0.0 if segment_tag == "zero" else 20.0
            winner = "new_chain" if abs(new_error) < abs(old_error) else "old_chain"
            native_error = new_error * 0.5
            rows.append(
                {
                    "analyzer_id": analyzer_id,
                    "temp_c": temp_c,
                    "target_ppm": target_ppm,
                    "old_pred_ppm": target_ppm + old_error,
                    "new_pred_ppm": target_ppm + new_error,
                    "old_error": old_error,
                    "new_error": new_error,
                    "winner_for_point": winner,
                    "ratio_source_selected": "raw" if "raw" in selected_source_pair else "filt",
                    "selected_source_pair": selected_source_pair,
                    "selected_prediction_scope": selected_prediction_scope,
                    "mode2_semantic_profile": "baseline_bearing_profile",
                    "mode2_legacy_raw_compare_safe": analyzer_id in {"GA01", "GA03"},
                    "point_title": f"{run_id}_{analyzer_id}_{segment_tag}",
                    "point_row": idx,
                }
            )
            for sample_index, delta in enumerate((-0.1, 0.1), start=1):
                filtered_rows.append(
                    {
                        "analyzer": analyzer_id,
                        "point_title": f"{run_id}_{analyzer_id}_{segment_tag}",
                        "point_row": idx,
                        "temp_set_c": temp_c,
                        "target_co2_ppm": target_ppm,
                        "co2_ppm": target_ppm + native_error + delta,
                        "sample_index": sample_index,
                    }
                )

    point_reconciliation = pd.DataFrame(rows)
    filtered = pd.DataFrame(filtered_rows)
    selection = pd.DataFrame(selection_rows)
    new_chain_input_audit = pd.DataFrame(audit_rows)
    old_vs_new_outputs = build_old_vs_new_comparison_outputs(
        point_reconciliation=point_reconciliation,
        selection_table=selection,
        new_chain_input_audit=new_chain_input_audit,
    )
    detail = old_vs_new_outputs["detail"].copy()
    overview = pd.DataFrame(
        [
            {
                "analyzer_id": row.analyzer_id,
                "old_chain_rmse": row.old_chain_overall_rmse,
                "new_chain_rmse": row.new_chain_overall_rmse,
                "old_zero_rmse": row.old_chain_zero_rmse,
                "new_zero_rmse": row.new_chain_zero_rmse,
                "old_temp_stability_metric": 0.0,
                "new_temp_stability_metric": 0.0,
                "winner_overall": "new_chain" if bool(row.overall_win_flag) else "old_chain",
                "winner_zero": "new_chain" if bool(row.zero_win_flag) else "old_chain",
                "winner_temp_stability": "new_chain" if bool(row.overall_win_flag) else "old_chain",
                "winner_low_range": "new_chain" if bool(row.low_win_flag) else "old_chain",
                "winner_main_range": "new_chain" if bool(row.main_win_flag) else "old_chain",
            }
            for row in detail.itertuples(index=False)
        ]
    )
    return {
        "run_name": run_id,
        "output_dir": output_dir,
        "validation_table": pd.DataFrame(),
        "comparison_outputs": {"overview_summary": overview},
        "model_results": {"selection": selection},
        "invalid_pressure_summary": pd.DataFrame([{"summary_scope": "overall", "invalid_point_count": 1, "invalid_sample_count": 3}]),
        "run_role_assessment": pd.DataFrame([{"assessment_scope": "run_summary", "has_high_temp_zero_anchor_candidate": False, "recommended_role": "mixed role"}]),
        "config": DebuggerConfig(input_path=output_dir, output_dir=output_dir),
        "ga01_special_note": "GA01 needs separate tracking.",
        "filtered": filtered,
        "point_reconciliation": point_reconciliation,
        "old_vs_new_outputs": old_vs_new_outputs,
    }


def test_build_scoped_old_vs_new_outputs_separates_scopes_and_report() -> None:
    run_results = [
        _synthetic_scoped_run_result("run_20260403_014845", Path("D:/tmp/a")),
        _synthetic_scoped_run_result("run_20260407_185002", Path("D:/tmp/b")),
        _synthetic_scoped_run_result("run_20260410_132440", Path("D:/tmp/c")),
    ]

    outputs = build_scoped_old_vs_new_outputs(run_results)
    scope_a = outputs["scope_a"]
    scope_b = outputs["scope_b"]

    assert {
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
    } <= set(scope_a["detail"].columns)
    assert {
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
    } <= set(scope_a["summary"].columns)
    assert {
        "run_id",
        "analyzer_id",
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
    } <= set(scope_a["local_wins"].columns)
    assert set(scope_a["detail"]["analyzer_id"]) == {"GA02", "GA03"}
    assert "GA01" not in str(scope_a["summary"].iloc[0]["analyzer_set"])
    assert set(scope_b["detail"]["analyzer_id"]) == {"GA01", "GA02", "GA03", "GA04"}
    assert scope_a["summary"].iloc[0]["headline_safe_statement"] == "仅针对 historical packages 中的 GA02/GA03"
    assert scope_b["summary"].iloc[0]["headline_safe_statement"] == "仅针对 run_20260410_132440 的全 analyzers"
    assert scope_a["summary"].iloc[0]["scope_limitation_statement"] == "两个 scope 不能合并解读为统一全局结论"
    assert scope_b["summary"].iloc[0]["scope_limitation_statement"] == "两个 scope 不能合并解读为统一全局结论"

    report = render_scoped_old_vs_new_report_markdown(outputs)
    assert "Scope A: historical GA02/GA03 comparison" in report
    assert "Scope B: 2026-04-10 all-analyzers comparison" in report
    assert "在历史数据包上，仅看 GA02/GA03，新算法相对旧算法" in report
    assert "在 2026-04-10 这包完整 run 上，全 analyzers 视角下新算法相对旧算法的真实状态是" in report
    assert "这两个 scope 用途不同，不应混成单一全局 headline" in report


def test_executive_summary_and_reconciliation_render_required_sections() -> None:
    run_results = [
        _synthetic_scoped_run_result("run_20260403_014845", Path("D:/tmp/a")),
        _synthetic_scoped_run_result("run_20260407_185002", Path("D:/tmp/b")),
        _synthetic_scoped_run_result("run_20260410_132440", Path("D:/tmp/c")),
    ]
    scoped_outputs = build_scoped_old_vs_new_outputs(run_results)
    reconciliation = build_comparison_reconciliation_table(
        run_results=run_results,
        scoped_outputs=scoped_outputs,
    )

    assert {
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
    } <= set(reconciliation.columns)

    executive_md = render_executive_summary_markdown(scoped_outputs)
    assert "historical GA02/GA03" in executive_md
    assert "2026-04-10 all analyzers" in executive_md
    assert "唯一正式 benchmark 是 old_chain" in executive_md

    reconciliation_md = render_comparison_reconciliation_markdown(
        reconciliation,
        scope_a=scoped_outputs["scope_a"],
        scope_b=scoped_outputs["scope_b"],
    )
    assert "old_value_source" in reconciliation_md
    assert "new_value_source" in reconciliation_md
    assert "matched_only_filter_applied" in reconciliation_md
    assert "valid_only_filter_applied" in reconciliation_md
    assert "analyzer_inclusion_scope" in reconciliation_md
    assert "以后对外一律以 step_09c / step_09d / step_10c 的 scoped debugger 结果为准" in reconciliation_md


def test_dual_surface_reconciliation_outputs_and_report() -> None:
    run_results = [
        _synthetic_scoped_run_result("run_20260403_014845", Path("D:/tmp/a")),
        _synthetic_scoped_run_result("run_20260407_185002", Path("D:/tmp/b")),
        _synthetic_scoped_run_result("run_20260410_132440", Path("D:/tmp/c")),
    ]
    outputs = build_dual_surface_reconciliation_outputs(run_results)

    assert {
        "run_id",
        "analyzer_id",
        "comparison_surface",
        "old_value_source",
        "new_value_source",
        "selected_prediction_scope",
        "selected_prediction_scope_majority",
        "old_chain_overall_rmse",
        "new_chain_overall_rmse",
        "delta_overall_rmse",
        "old_chain_zero_rmse",
        "new_chain_zero_rmse",
        "old_chain_low_rmse",
        "new_chain_low_rmse",
        "old_chain_main_rmse",
        "new_chain_main_rmse",
        "point_count",
        "pointwise_win_count",
        "pointwise_loss_count",
    } <= set(outputs["detail"].columns)
    assert {
        "comparison_surface",
        "old_value_source",
        "new_value_source",
        "selected_prediction_scope",
        "selected_prediction_scope_majority",
    } <= set(outputs["summary"].columns)

    summary = outputs["summary"].set_index("comparison_surface")
    assert summary.loc["run_native_old_vs_new", "new_value_source"] == "analyzer_sheet_mean_co2_ppm"
    assert summary.loc["debugger_reconstructed_old_vs_new", "new_value_source"] == "selected_pred_ppm_from_debugger"

    report_md = render_dual_surface_reconciliation_markdown(outputs)
    assert "what quick calc actually used" in report_md
    assert "what current debugger comparison actually used" in report_md
    assert "no evidence of old/new flip bug" in report_md


def test_source_policy_challenge_stays_diagnostic_only_and_report_surfaces_audit(tmp_path: Path) -> None:
    config = DebuggerConfig(input_path=tmp_path, output_dir=tmp_path)
    filtered = pd.DataFrame(
        [
            {
                "analyzer": "GA01",
                "point_title": f"p{idx}",
                "point_row": idx,
                "ratio_co2_raw": raw_ratio,
                "ratio_co2_filt": filt_ratio,
                "temp_corr_c": 20.0,
                "pressure_corr_hpa": 1013.25,
                "mode2_semantic_profile": "baseline_bearing_profile",
                "mode2_legacy_raw_compare_safe": False,
                "mode2_is_baseline_bearing_profile": True,
            }
            for idx, (raw_ratio, filt_ratio) in enumerate(((1.01, 1.00), (1.12, 1.08), (1.26, 1.18)), start=1)
        ]
    )
    absorbance_points = pd.DataFrame(
        [
            {
                "analyzer": "GA01",
                "point_title": f"p{idx}",
                "point_row": idx,
                "ratio_source": ratio_source,
                "temp_source": config.default_temp_source,
                "pressure_source": config.default_pressure_source,
                "r0_model": config.default_r0_model,
                "A_mean": a_mean,
            }
            for idx, (raw_a, filt_a) in enumerate(((0.02, 0.01), (0.11, 0.08), (0.24, 0.18)), start=1)
            for ratio_source, a_mean in (("ratio_co2_raw", raw_a), ("ratio_co2_filt", filt_a))
        ]
    )
    point_reconciliation = pd.DataFrame(
        [
            {
                "analyzer_id": "GA01",
                "temp_c": temp_c,
                "target_ppm": target_ppm,
                "old_pred_ppm": target_ppm + old_error,
                "new_pred_ppm": target_ppm + new_error,
                "old_error": old_error,
                "new_error": new_error,
                "winner_for_point": "new_chain",
                "ratio_source_selected": "filt",
                "selected_source_pair": "filt/filt",
                "mode2_semantic_profile": "baseline_bearing_profile",
                "mode2_legacy_raw_compare_safe": False,
                "point_title": f"p{idx}",
                "point_row": idx,
            }
            for idx, (temp_c, target_ppm, old_error, new_error) in enumerate(
                ((0.0, 0.0, 6.0, 1.0), (20.0, 100.0, 8.0, 2.0), (20.0, 400.0, 9.0, 3.0)),
                start=1,
            )
        ]
    )
    selection = pd.DataFrame(
        [
            {
                "analyzer_id": "GA01",
                "best_absorbance_model": "model_filt",
                "best_model_family": "single_range",
                "zero_residual_mode": "none",
                "selected_prediction_scope": "validation_oof",
                "selected_source_pair": "filt/filt",
            }
        ]
    )
    scores = pd.DataFrame(
        [
            {
                "analyzer_id": "GA01",
                "model_id": "model_raw",
                "model_family": "single_range",
                "zero_residual_mode": "none",
                "score_source": "validation_oof",
                "selected_source_pair": "raw/raw",
                "validation_rmse": 4.0,
                "overall_rmse": 4.1,
                "composite_score": 4.0,
                "water_zero_anchor_mode": "none",
                "with_water_zero_anchor_correction": False,
            },
            {
                "analyzer_id": "GA01",
                "model_id": "model_filt",
                "model_family": "single_range",
                "zero_residual_mode": "none",
                "score_source": "validation_oof",
                "selected_source_pair": "filt/filt",
                "validation_rmse": 2.0,
                "overall_rmse": 2.2,
                "composite_score": 2.0,
                "water_zero_anchor_mode": "none",
                "with_water_zero_anchor_correction": False,
            },
        ]
    )
    residuals = pd.DataFrame(
        [
            {
                "analyzer_id": "GA01",
                "model_id": model_id,
                "model_family": "single_range",
                "zero_residual_mode": "none",
                "prediction_scope": "validation_oof",
                "selected_source_pair": source_pair,
                "point_title": f"p{idx}",
                "point_row": idx,
                "temp_c": temp_c,
                "target_ppm": target_ppm,
                "predicted_ppm": target_ppm + error_ppm,
                "error_ppm": error_ppm,
            }
            for model_id, source_pair, errors in (
                ("model_raw", "raw/raw", (3.0, 4.0, 5.0)),
                ("model_filt", "filt/filt", (1.0, 2.0, 3.0)),
            )
            for idx, ((temp_c, target_ppm), error_ppm) in enumerate(zip(((0.0, 0.0), (20.0, 100.0), (20.0, 400.0)), errors, strict=False), start=1)
        ]
    )
    model_results = {
        "scores": scores,
        "residuals": residuals,
        "selection": selection,
    }

    selection_before = selection.copy(deep=True)
    source_selection_audit = build_source_selection_audit(filtered, absorbance_points, model_results, config)
    source_policy_outputs = build_source_policy_challenge(
        point_reconciliation=point_reconciliation,
        model_results=model_results,
        config=config,
        source_selection_audit_detail=source_selection_audit["detail"],
    )

    pd.testing.assert_frame_equal(selection, selection_before)
    current_row = source_policy_outputs["detail"][source_policy_outputs["detail"]["source_policy_mode"] == "current_deployable_mixed"].iloc[0]
    raw_first_row = source_policy_outputs["detail"][source_policy_outputs["detail"]["source_policy_mode"] == "raw_first_with_fallback"].iloc[0]
    raw_first_summary = source_policy_outputs["summary"][source_policy_outputs["summary"]["source_policy_mode"] == "raw_first_with_fallback"].iloc[0]
    current_reason = source_policy_outputs["conclusions"][source_policy_outputs["conclusions"]["question_id"] == "current_mixed_reason_category"].iloc[0]
    assert current_row["selected_source_pair_under_policy"] == "filt/filt"
    assert raw_first_row["selected_source_pair_under_policy"] == "raw/raw"
    assert bool(raw_first_summary["whether_improves_current_deployable_result"]) is False
    assert current_reason["answer"] == "GA01=filt_better_in_fixed_family"

    report = render_old_vs_new_report_markdown(
        {
            "run_name": "synthetic_source_policy",
            "summary": pd.DataFrame(
                [
                    {
                        "overall_verdict": "current_deployable_new_chain_does_not_yet_beat_old_chain_overall",
                        "overall_rmse_old": 7.7,
                        "overall_rmse_new": 2.2,
                        "overall_improvement_pct": 71.4,
                        "actual_ratio_source_used_in_this_run": "filt",
                        "actual_ratio_source_used_majority": "filt",
                        "whether_new_chain_has_overall_evidence_to_surpass_old": False,
                    }
                ]
            ),
            "detail": pd.DataFrame(
                [
                    {
                        "analyzer_id": "GA01",
                        "mode2_semantic_profile": "baseline_bearing_profile",
                        "selected_source_pair": "filt/filt",
                        "actual_ratio_source_used": "filt",
                        "improvement_pct_overall": 71.4,
                        "improvement_pct_zero": 70.0,
                        "improvement_pct_low": 68.0,
                        "improvement_pct_main": 66.0,
                        "overall_win_flag": True,
                        "zero_win_flag": True,
                        "low_win_flag": True,
                        "main_win_flag": True,
                    }
                ]
            ),
            "aggregate_segments": pd.DataFrame(
                [
                    {"segment_tag": "overall", "old_chain_rmse": 7.7, "new_chain_rmse": 2.2},
                    {"segment_tag": "zero", "old_chain_rmse": 6.0, "new_chain_rmse": 1.0},
                    {"segment_tag": "low", "old_chain_rmse": 8.0, "new_chain_rmse": 2.0},
                    {"segment_tag": "main", "old_chain_rmse": 9.0, "new_chain_rmse": 3.0},
                ]
            ),
            "local_wins": pd.DataFrame(),
            "ratio_source_audit": pd.DataFrame(
                [
                    {
                        "actual_ratio_source_used_in_this_run": "filt",
                        "actual_ratio_source_used_majority": "filt",
                        "supporting_evidence_for_actual_ratio_source": "GA01=filt (filt/filt)",
                    }
                ]
            ),
            "diagnostic_candidates": pd.DataFrame(),
            "source_selection_audit_summary": source_selection_audit["summary"],
            "source_selection_audit_conclusions": source_selection_audit["conclusions"],
            "source_policy_challenge_summary": source_policy_outputs["summary"],
            "source_policy_challenge_conclusions": source_policy_outputs["conclusions"],
        }
    )

    assert "designed_v5_ratio_source_intent = raw_or_instantaneous" in report
    assert "actual_ratio_source_used_in_this_run" in report
    assert "selection_reason_primary" in report
    assert "whether_raw_first_improves_current_deployable_result = False" in report


def _synthetic_remaining_gap_sidecar_inputs() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    point_rows: list[dict[str, object]] = []
    filtered_rows: list[dict[str, object]] = []
    absorbance_rows: list[dict[str, object]] = []
    selection_rows: list[dict[str, object]] = []
    point_row = 1
    analyzer_profiles = {
        "GA01": {
            "source_pair": "raw/raw",
            "old_errors": [1.0, 1.0, 1.5, 1.5, 1.2, 1.0],
            "new_errors": [5.0, 4.5, 4.0, 3.6, 3.0, 2.8],
            "mode2_profile": "legacy_ratio_raw_profile",
        },
        "GA02": {
            "source_pair": "filt/filt",
            "old_errors": [2.2, 2.0, 1.8, 1.7, 1.8, 2.0],
            "new_errors": [1.9, 1.7, 1.5, 1.4, 1.6, 1.8],
            "mode2_profile": "baseline_bearing_profile",
        },
        "GA03": {
            "source_pair": "filt/filt",
            "old_errors": [2.0, 1.8, 1.6, 1.5, 1.4, 1.3],
            "new_errors": [1.2, 1.1, 1.0, 0.9, 0.9, 0.8],
            "mode2_profile": "baseline_bearing_profile",
        },
    }
    points = [
        (0.0, 0.0),
        (10.0, 50.0),
        (20.0, 100.0),
        (20.0, 400.0),
        (30.0, 800.0),
        (30.0, 1000.0),
    ]

    for analyzer_id, profile in analyzer_profiles.items():
        selection_rows.append(
            {
                "analyzer_id": analyzer_id,
                "best_absorbance_model": f"{analyzer_id.lower()}_current_model",
                "best_model_family": "single_range",
                "zero_residual_mode": "none",
                "selected_prediction_scope": "validation_oof",
                "selected_source_pair": profile["source_pair"],
            }
        )
        for idx, ((temp_c, target_ppm), old_error, new_error) in enumerate(
            zip(points, profile["old_errors"], profile["new_errors"], strict=False),
            start=1,
        ):
            point_title = f"{analyzer_id.lower()}_p{idx}"
            absorbance = (target_ppm / 100.0) + (temp_c / 50.0)
            point_rows.append(
                {
                    "analyzer_id": analyzer_id,
                    "point_title": point_title,
                    "point_row": point_row,
                    "temp_c": temp_c,
                    "target_ppm": target_ppm,
                    "A_mean": absorbance,
                    "old_pred_ppm": target_ppm + old_error,
                    "new_pred_ppm": target_ppm + new_error,
                    "old_error": old_error,
                    "new_error": new_error,
                    "selected_source_pair": profile["source_pair"],
                    "best_model_family": "single_range",
                    "zero_residual_mode": "none",
                    "selected_prediction_scope": "validation_oof",
                    "mode2_semantic_profile": profile["mode2_profile"],
                    "mode2_legacy_raw_compare_safe": analyzer_id == "GA01",
                }
            )
            filtered_rows.append(
                {
                    "analyzer": analyzer_id,
                    "point_title": point_title,
                    "point_row": point_row,
                    "target_co2_ppm": target_ppm,
                    "temp_set_c": temp_c,
                    "temp_cavity_c": temp_c + 0.2,
                    "temp_shell_c": temp_c,
                    "ratio_h2o_raw": 0.10 + (new_error / 10.0),
                    "ratio_h2o_filt": 0.09 + (new_error / 12.0),
                    "h2o_density": 0.20 + (new_error / 8.0),
                    "ratio_co2_raw": 1.0 + absorbance / 20.0,
                    "ratio_co2_filt": 1.0 + absorbance / 22.0,
                }
            )
            absorbance_rows.append(
                {
                    "analyzer": analyzer_id,
                    "point_title": point_title,
                    "point_row": point_row,
                    "temp_set_c": temp_c,
                    "target_co2_ppm": target_ppm,
                    "temp_use_mean_c": temp_c,
                    "ratio_source": "ratio_co2_raw" if profile["source_pair"] == "raw/raw" else "ratio_co2_filt",
                    "zero_residual_mode": "none",
                    "A_mean": absorbance,
                    "A_std": 0.01,
                }
            )
            point_row += 1
    return (
        pd.DataFrame(point_rows),
        pd.DataFrame(filtered_rows),
        pd.DataFrame(absorbance_rows),
        pd.DataFrame(selection_rows),
    )


def test_remaining_gap_and_ga01_sidecar_stay_diagnostic_only(tmp_path: Path) -> None:
    config = DebuggerConfig(input_path=tmp_path, output_dir=tmp_path)
    point_reconciliation, filtered_samples, absorbance_point_variants, selection = _synthetic_remaining_gap_sidecar_inputs()

    remaining_gap_outputs = build_remaining_gap_decomposition(point_reconciliation)
    selection_before = selection.copy(deep=True)
    sidecar_outputs = build_analyzer_sidecar_challenge(
        point_reconciliation=point_reconciliation,
        filtered_samples=filtered_samples,
        absorbance_point_variants=absorbance_point_variants,
        selection_table=selection,
        config=config,
        laggard_analyzer_id=str(remaining_gap_outputs["summary"].iloc[0]["laggard_analyzer_primary"]),
    )

    assert str(remaining_gap_outputs["summary"].iloc[0]["laggard_analyzer_primary"]) == "GA01"
    assert {"laggard_analyzer_primary", "laggard_segment_primary"} <= set(remaining_gap_outputs["summary"].columns)
    pd.testing.assert_frame_equal(selection, selection_before)

    current_row = sidecar_outputs["summary"][sidecar_outputs["summary"]["sidecar_mode"] == "current_global_fixed"].iloc[0]
    best_mode = str(sidecar_outputs["summary"].iloc[0]["best_sidecar_mode"])
    assert {"laggard_analyzer_primary", "best_sidecar_mode", "global_remaining_gap_delta_vs_current"} <= set(sidecar_outputs["summary"].columns)
    assert {"laggard_analyzer_primary", "best_sidecar_mode"} <= set(sidecar_outputs["conclusions"].columns)
    assert current_row["laggard_analyzer_primary"] == "GA01"
    assert best_mode in set(sidecar_outputs["summary"]["sidecar_mode"])
    assert float(pd.to_numeric(sidecar_outputs["summary"]["global_remaining_gap_delta_vs_current"], errors="coerce").max()) >= 0.0


def test_zero_residual_fit_and_compare_can_run(tmp_path: Path) -> None:
    config = DebuggerConfig(input_path=tmp_path, output_dir=tmp_path)
    points = pd.DataFrame(
        [
            {
                "analyzer": "GA01",
                "analyzer_slot": 1,
                "point_title": f"p{idx}",
                "point_row": idx,
                "point_tag": f"p{idx}",
                "temp_set_c": temp_c,
                "target_co2_ppm": target_ppm,
                "ratio_source": "ratio_co2_raw",
                "temp_source": "temp_corr_c",
                "pressure_source": "pressure_corr_hpa",
                "r0_model": "quadratic",
                "branch_id": config.default_branch_id(),
                "is_default_branch": True,
                "sample_count": 4,
                "ratio_in_mean": 1.0,
                "temp_use_mean_c": temp_c,
                "pressure_use_mean_hpa": 1013.25,
                "R0_T_mean": 1.0,
                "A_mean": absorbance,
                "A_std": 0.01,
                "A_min": absorbance - 0.01,
                "A_max": absorbance + 0.01,
                "A_alt_mean": absorbance / config.p_ref_hpa,
                "A_from_mean": absorbance,
            }
            for idx, (temp_c, target_ppm, absorbance) in enumerate(
                [
                    (-10.0, 0.0, 2.0),
                    (0.0, 0.0, 1.0),
                    (20.0, 0.0, 0.0),
                    (30.0, 0.0, -0.5),
                    (0.0, 100.0, 12.0),
                    (20.0, 200.0, 20.0),
                    (30.0, 500.0, 48.0),
                ],
                start=1,
            )
        ]
    )
    observations, models, selection, lookup = fit_zero_residual_models(points, config)
    variants = build_zero_residual_point_variants(points, lookup, config)

    assert not observations.empty
    assert not models.empty
    assert not selection.empty
    assert {"none", "linear", "quadratic"} <= set(variants["zero_residual_mode"])
    none_zero = variants[(variants["zero_residual_mode"] == "none") & (variants["target_co2_ppm"] == 0)]["A_mean"].to_numpy()
    quad_zero = variants[(variants["zero_residual_mode"] == "quadratic") & (variants["target_co2_ppm"] == 0)]["A_mean"].to_numpy()
    assert np.sqrt(np.mean(np.square(quad_zero))) < np.sqrt(np.mean(np.square(none_zero)))


def test_water_zero_anchor_feature_extraction_and_compare_can_run(tmp_path: Path) -> None:
    config = DebuggerConfig(input_path=tmp_path, output_dir=tmp_path)
    base_points = pd.DataFrame(
        [
            {
                "analyzer": "GA01",
                "point_title": f"p{idx}",
                "point_row": idx,
                "point_tag": f"p{idx}",
                "temp_set_c": temp_c,
                "target_co2_ppm": target_ppm,
                "ratio_source": "ratio_co2_raw",
                "temp_source": "temp_corr_c",
                "pressure_source": "pressure_corr_hpa",
                "r0_model": "quadratic",
                "temp_use_mean_c": temp_c,
                "A_mean": absorbance,
                "A_std": 0.01,
                "A_from_mean": absorbance,
                "A_alt_mean": absorbance / config.p_ref_hpa,
            }
            for idx, (temp_c, target_ppm, absorbance) in enumerate(
                [
                    (-20.0, 0.0, 0.6),
                    (-10.0, 0.0, 0.4),
                    (0.0, 0.0, 0.3),
                    (20.0, 0.0, 0.15),
                    (20.0, 100.0, 5.0),
                    (30.0, 300.0, 11.0),
                ],
                start=1,
            )
        ]
    )
    zero_variants = build_zero_residual_point_variants(base_points, {}, replace(config, enable_zero_residual_correction=False))
    filtered_samples = pd.DataFrame(
        [
            {
                "analyzer": "GA01",
                "point_title": f"p{idx}",
                "point_row": idx,
                "temp_set_c": temp_c,
                "target_co2_ppm": target_ppm,
                "ratio_h2o_raw": h2o_ratio,
                "ratio_h2o_filt": h2o_ratio * 0.98,
                "h2o_signal": 1.0 + idx,
                "h2o_density": 2.0 + idx,
                "sample_index": 1,
            }
            for idx, (temp_c, target_ppm, h2o_ratio) in enumerate(
                [
                    (-20.0, 0.0, 0.21),
                    (-10.0, 0.0, 0.22),
                    (0.0, 0.0, 0.25),
                    (20.0, 0.0, 0.31),
                    (20.0, 100.0, 0.34),
                    (30.0, 300.0, 0.39),
                ],
                start=1,
            )
        ]
    )

    features = build_water_zero_anchor_features(zero_variants, filtered_samples)
    models, selection, lookup = fit_water_zero_anchor_models(features, config)
    variants = build_water_zero_anchor_point_variants(features, lookup, config)

    assert not features.empty
    assert not models.empty
    assert not selection.empty
    assert {"none", "linear", "quadratic"} & set(variants["water_zero_anchor_mode"])
    assert "delta_h2o_ratio_vs_subzero_anchor" in features.columns
    assert features["feature_status"].ne("feature_unavailable").any()

    baseline_outputs = {
        "overview_summary": pd.DataFrame(
            [
                {
                    "analyzer_id": "GA01",
                    "old_chain_rmse": 1.0,
                    "new_chain_rmse": 2.0,
                    "old_zero_rmse": 0.5,
                    "new_zero_rmse": 0.9,
                    "old_temp_stability_metric": 0.3,
                    "new_temp_stability_metric": 0.8,
                    "new_chain_max_abs_error": 3.0,
                }
            ]
        ),
        "by_concentration_range": pd.DataFrame(
            [
                {"analyzer_id": "GA01", "concentration_range": "0~200 ppm", "new_rmse": 1.2},
                {"analyzer_id": "GA01", "concentration_range": "200~1000 ppm", "new_rmse": 2.4},
            ]
        ),
        "by_temperature": pd.DataFrame(
            [
                {"analyzer_id": "GA01", "temp_c": 40.0, "new_rmse": 4.0},
            ]
        ),
    }
    water_outputs = {
        "overview_summary": pd.DataFrame(
            [
                {
                    "analyzer_id": "GA01",
                    "old_chain_rmse": 1.0,
                    "new_chain_rmse": 1.8,
                    "old_zero_rmse": 0.5,
                    "new_zero_rmse": 0.6,
                    "old_temp_stability_metric": 0.3,
                    "new_temp_stability_metric": 0.5,
                    "new_chain_max_abs_error": 2.5,
                }
            ]
        ),
        "by_concentration_range": pd.DataFrame(
            [
                {"analyzer_id": "GA01", "concentration_range": "0~200 ppm", "new_rmse": 1.0},
                {"analyzer_id": "GA01", "concentration_range": "200~1000 ppm", "new_rmse": 2.1},
            ]
        ),
        "by_temperature": pd.DataFrame(
            [
                {"analyzer_id": "GA01", "temp_c": 40.0, "new_rmse": 3.6},
            ]
        ),
    }
    baseline_selection = pd.DataFrame(
        [
            {
                "analyzer_id": "GA01",
                "selected_source_pair": "raw/raw",
                "zero_residual_mode": "none",
                "water_zero_anchor_mode": "none",
                "water_zero_anchor_model_label": "No water zero-anchor correction",
                "water_feature_status": "subzero_and_zeroC_available",
            }
        ]
    )
    water_selection = pd.DataFrame(
        [
            {
                "analyzer_id": "GA01",
                "selected_source_pair": "raw/raw",
                "zero_residual_mode": "none",
                "water_zero_anchor_mode": "linear",
                "water_zero_anchor_model_label": "Linear water zero-anchor correction",
                "water_feature_status": "subzero_and_zeroC_available",
                "selection_reason": "Selected linear.",
            }
        ]
    )
    compare = build_water_anchor_compare(baseline_outputs, water_outputs, baseline_selection, water_selection)
    assert not compare.empty
    assert compare.iloc[0]["winner_zero"] == "water_zero_anchor_chain"


def test_legacy_h2o_summary_selection_includes_repo_selected_co2_rows() -> None:
    rules = load_legacy_water_replay_rules(REPO_ROOT)
    fixed_points = pd.DataFrame(
        [
            {
                "analyzer": "GA01",
                "point_title": "co2_main",
                "point_row": 1,
                "temp_set_c": 10.0,
                "target_co2_ppm": 300.0,
                "ratio_source": "ratio_co2_raw",
                "fixed_zero_residual_mode": "none",
                "temp_use_mean_c": 10.0,
                "A_mean": 1.25,
            }
        ]
    )
    water_lineage_samples = pd.DataFrame(
        [
            {
                "analyzer": "GA01",
                "point_title": "co2_main",
                "point_row": 1,
                "temp_set_c": 10.0,
                "target_co2_ppm": 300.0,
                "route": "co2",
                "stage": "co2_phase",
                "point_tag": "co2_main",
                "ratio_h2o_raw": 0.50,
                "ratio_h2o_filt": 0.49,
                "h2o_signal": 2.0,
                "h2o_density": 3.0,
                "sample_index": 1,
            },
            {
                "analyzer": "GA01",
                "point_title": "h2o_phase",
                "point_row": 2,
                "temp_set_c": 20.0,
                "target_co2_ppm": 300.0,
                "route": "h2o",
                "stage": "h2o_phase",
                "point_tag": "h2o_phase",
                "ratio_h2o_raw": 0.30,
                "ratio_h2o_filt": 0.29,
                "h2o_signal": 2.1,
                "h2o_density": 3.1,
                "sample_index": 1,
            },
            {
                "analyzer": "GA01",
                "point_title": "co2_m20",
                "point_row": 3,
                "temp_set_c": -20.0,
                "target_co2_ppm": 50.0,
                "route": "co2",
                "stage": "co2_phase",
                "point_tag": "co2_m20",
                "ratio_h2o_raw": 0.10,
                "ratio_h2o_filt": 0.09,
                "h2o_signal": 2.2,
                "h2o_density": 3.2,
                "sample_index": 1,
            },
            {
                "analyzer": "GA01",
                "point_title": "co2_m10",
                "point_row": 4,
                "temp_set_c": -10.0,
                "target_co2_ppm": 80.0,
                "route": "co2",
                "stage": "co2_phase",
                "point_tag": "co2_m10",
                "ratio_h2o_raw": 0.20,
                "ratio_h2o_filt": 0.19,
                "h2o_signal": 2.3,
                "h2o_density": 3.3,
                "sample_index": 1,
            },
            {
                "analyzer": "GA01",
                "point_title": "co2_0",
                "point_row": 5,
                "temp_set_c": 0.0,
                "target_co2_ppm": 120.0,
                "route": "co2",
                "stage": "co2_phase",
                "point_tag": "co2_0",
                "ratio_h2o_raw": 0.40,
                "ratio_h2o_filt": 0.39,
                "h2o_signal": 2.4,
                "h2o_density": 3.4,
                "sample_index": 1,
            },
            {
                "analyzer": "GA01",
                "point_title": "co2_zero_10",
                "point_row": 6,
                "temp_set_c": 10.0,
                "target_co2_ppm": 0.0,
                "route": "co2",
                "stage": "co2_phase",
                "point_tag": "co2_zero_10",
                "ratio_h2o_raw": 0.60,
                "ratio_h2o_filt": 0.59,
                "h2o_signal": 2.5,
                "h2o_density": 3.5,
                "sample_index": 1,
            },
        ]
    )

    features = build_legacy_water_replay_features(fixed_points, water_lineage_samples, rules)

    assert set(features["water_lineage_mode"]) == {
        "none",
        "simplified_subzero_anchor",
        "legacy_h2o_summary_selection",
        "legacy_h2o_summary_selection_plus_zero_ppm_rows",
    }
    legacy_row = features[features["water_lineage_mode"] == "legacy_h2o_summary_selection"].iloc[0]
    plus_zero_row = features[features["water_lineage_mode"] == "legacy_h2o_summary_selection_plus_zero_ppm_rows"].iloc[0]

    assert bool(legacy_row["uses_co2_temp_groups"]) is True
    assert bool(legacy_row["uses_co2_zero_ppm_rows"]) is False
    assert int(legacy_row["legacy_summary_anchor_row_count"]) == 4
    assert int(legacy_row["zero_ppm_anchor_row_count"]) == 1
    assert abs(float(legacy_row["delta_h2o_ratio_vs_legacy_summary_anchor"]) - 0.25) < 1.0e-9

    assert bool(plus_zero_row["uses_co2_temp_groups"]) is True
    assert bool(plus_zero_row["uses_co2_zero_ppm_rows"]) is True
    assert int(plus_zero_row["zero_ppm_anchor_row_count"]) == 1
    assert abs(float(plus_zero_row["delta_h2o_ratio_vs_legacy_summary_anchor"]) + 0.10) < 1.0e-9


def _synthetic_ppm_family_inputs(config: DebuggerConfig) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    analyzers = (
        ("GA01", "legacy_ratio_raw_profile", True, False, "raw/raw"),
        ("GA02", "baseline_bearing_profile", False, True, "filt/filt"),
    )
    filtered_rows: list[dict[str, object]] = []
    legacy_rows: list[dict[str, object]] = []
    selection_rows: list[dict[str, object]] = []
    old_rows: list[dict[str, object]] = []
    point_row = 1
    for analyzer_id, profile, legacy_safe, baseline_bearing, source_pair in analyzers:
        selection_rows.append(
            {
                "analyzer_id": analyzer_id,
                "selected_ratio_source": "ratio_co2_raw" if source_pair == "raw/raw" else "ratio_co2_filt",
                "selected_source_pair": source_pair,
                "best_absorbance_model": "model_a_linear",
                "best_model_family": "single_range",
                "zero_residual_mode": "none",
                "selected_prediction_scope": "overall_fit",
                "absorbance_column": "A_mean",
            }
        )
        for temp_c in (-20.0, 0.0, 20.0, 40.0):
            for step, base_a in enumerate((0.4, 0.9, 1.5, 2.4), start=1):
                humidity = (temp_c + 20.0) / 60.0 + step * 0.04
                a_value = base_a + (0.05 if analyzer_id == "GA02" else 0.0)
                target_ppm = 60.0 * a_value + 140.0 * humidity + 90.0 * a_value * humidity + 18.0 * humidity * humidity
                point_title = f"{analyzer_id}_p{point_row}"
                legacy_rows.append(
                    {
                        "analyzer": analyzer_id,
                        "water_lineage_mode": "none",
                        "point_title": point_title,
                        "point_row": point_row,
                        "temp_set_c": temp_c,
                        "target_co2_ppm": target_ppm,
                        "temp_use_mean_c": temp_c,
                        "pressure_use_mean_hpa": 1013.25,
                        "ratio_source": "ratio_co2_raw" if source_pair == "raw/raw" else "ratio_co2_filt",
                        "zero_residual_mode": "none",
                        "ratio_in_mean": 0.55 - a_value * 0.02,
                        "A_mean": a_value,
                        "A_from_mean": a_value,
                        "A_alt_mean": a_value / config.p_ref_hpa,
                        "h2o_ratio_raw_mean": humidity,
                        "h2o_ratio_filt_mean": humidity * 0.97,
                        "water_ratio_mean": humidity,
                        "delta_h2o_ratio_vs_subzero_anchor": humidity - 0.10,
                        "delta_h2o_ratio_vs_zeroC_anchor": humidity - 0.16,
                        "delta_h2o_ratio_vs_legacy_summary_anchor": humidity - 0.14,
                        "delta_h2o_ratio_vs_legacy_zero_ppm_anchor": humidity - 0.12,
                    }
                )
                filtered_rows.append(
                    {
                        "analyzer": analyzer_id,
                        "mode2_semantic_profile": profile,
                        "mode2_legacy_raw_compare_safe": legacy_safe,
                        "mode2_is_baseline_bearing_profile": baseline_bearing,
                    }
                )
                old_prediction = target_ppm + (4.0 if analyzer_id == "GA01" else 2.0)
                old_rows.append(
                    {
                        "analyzer": analyzer_id,
                        "point_row": point_row,
                        "point_title": point_title,
                        "old_prediction_ppm": old_prediction,
                        "old_residual_ppm": old_prediction - target_ppm,
                    }
                )
                point_row += 1
    return (
        pd.DataFrame(filtered_rows),
        pd.DataFrame(legacy_rows),
        pd.DataFrame(selection_rows),
        pd.DataFrame(old_rows),
    )


def test_fixed_chain_ppm_family_challenge_prefers_humidity_cross_terms_on_humid_data(tmp_path: Path) -> None:
    config = DebuggerConfig(input_path=tmp_path, output_dir=tmp_path)
    filtered_samples, legacy_feature_frame, fixed_selection, old_ratio_residuals = _synthetic_ppm_family_inputs(config)

    result = run_fixed_chain_ppm_family_challenge(
        filtered_samples=filtered_samples,
        legacy_water_feature_frame=legacy_feature_frame,
        fixed_selection=fixed_selection,
        old_ratio_residuals=old_ratio_residuals,
        legacy_water_summary=pd.DataFrame(),
        config=config,
    )

    detail = result["detail"]
    ga01 = detail[detail["analyzer_id"] == "GA01"].set_index("ppm_family_mode")
    assert float(ga01.loc["legacy_humidity_cross_D", "delta_vs_current_fixed_family_overall"]) > 0.0
    assert float(ga01.loc["v5_abs_k_minimal", "delta_vs_current_fixed_family_overall"]) > 0.0
    assert bool(ga01.loc["legacy_humidity_cross_D", "uses_humidity_cross_terms"]) is True


def test_fixed_chain_ppm_family_challenge_keeps_deployable_winner_and_layers_profiles(tmp_path: Path) -> None:
    config = DebuggerConfig(input_path=tmp_path, output_dir=tmp_path)
    filtered_samples, legacy_feature_frame, fixed_selection, old_ratio_residuals = _synthetic_ppm_family_inputs(config)
    fixed_selection_before = fixed_selection.copy(deep=True)

    result = run_fixed_chain_ppm_family_challenge(
        filtered_samples=filtered_samples,
        legacy_water_feature_frame=legacy_feature_frame,
        fixed_selection=fixed_selection,
        old_ratio_residuals=old_ratio_residuals,
        legacy_water_summary=pd.DataFrame(),
        config=config,
    )

    pd.testing.assert_frame_equal(fixed_selection, fixed_selection_before)
    current_rows = result["detail"][result["detail"]["ppm_family_mode"] == "current_fixed_family"]
    merged = current_rows.merge(
        fixed_selection_before[["analyzer_id", "best_absorbance_model", "best_model_family", "selected_source_pair", "zero_residual_mode", "selected_prediction_scope"]],
        on="analyzer_id",
        how="left",
    )
    assert (merged["fixed_best_model"] == merged["best_absorbance_model"]).all()
    assert (merged["fixed_model_family"] == merged["best_model_family"]).all()
    assert (merged["selected_source_pair_x"] == merged["selected_source_pair_y"]).all()
    assert (merged["fixed_zero_residual_mode"] == merged["zero_residual_mode"]).all()
    assert (merged["fixed_prediction_scope"] == merged["selected_prediction_scope"]).all()
    assert {"legacy_ratio_raw_profile", "baseline_bearing_profile"} <= set(result["summary"]["mode2_semantic_profile"])


def test_v5_mode2_semantics_are_marked_as_baseline_bearing_profile() -> None:
    result = classify_mode2_semantics(
        raw_message="YGAS,014,0000.000,04.632,-479.741,04.173,1.5318,1.5306,0.7231,0.7231,03718,05683,02687,-17.61,-17.11,106.32",
        mode2_field_count=16,
        software_version="v5_plus",
    )

    assert result["mode2_semantic_profile"] == "baseline_bearing_profile"
    assert result["mode2_profile_source"] == "software_version"
    assert bool(result["mode2_is_baseline_bearing_profile"]) is True
    assert bool(result["mode2_legacy_raw_compare_safe"]) is False


def test_piecewise_model_continuity_and_selection_can_run(tmp_path: Path) -> None:
    config = DebuggerConfig(input_path=tmp_path, output_dir=tmp_path)
    points = []
    for idx, (temp_c, target_ppm, absorbance) in enumerate(
        [
            (0.0, 0.0, 0.0),
            (0.0, 100.0, 8.0),
            (0.0, 200.0, 16.0),
            (0.0, 400.0, 26.0),
            (0.0, 800.0, 42.0),
            (20.0, 0.0, 0.3),
            (20.0, 100.0, 8.4),
            (20.0, 200.0, 16.3),
            (20.0, 400.0, 26.2),
            (20.0, 800.0, 41.7),
        ],
        start=1,
    ):
        points.append(
            {
                "analyzer": "GA01",
                "point_title": f"p{idx}",
                "point_row": idx,
                "temp_set_c": temp_c,
                "target_co2_ppm": target_ppm,
                "temp_use_mean_c": temp_c,
                "A_mean": absorbance,
                "A_std": 0.01,
                "zero_residual_mode": "none",
                "zero_residual_model_label": "No zero residual correction",
                "with_zero_residual_correction": False,
            }
        )
    frame = pd.DataFrame(points)
    results = evaluate_absorbance_models(frame, config)
    assert "piecewise_range" in set(results["scores"]["model_family"])
    coeffs = results["coefficients"]
    piecewise_row = results["scores"][results["scores"]["model_id"] == "piecewise_linear"].iloc[0]
    coeff_vector = coeffs[coeffs["model_id"] == "piecewise_linear"].sort_values("term_order")["coefficient"].to_numpy(dtype=float)
    breakpoint_absorbance = float(piecewise_row["piecewise_boundary_absorbance"])
    spec = next(spec for spec in active_model_specs(config) if spec.model_id == "piecewise_linear")
    left = pd.DataFrame({"absorbance_input": [breakpoint_absorbance], "temp_model_c": [20.0]})
    right = pd.DataFrame({"absorbance_input": [breakpoint_absorbance + 1.0e-9], "temp_model_c": [20.0]})
    pred_left = float((_design_matrix(left, spec, breakpoint_absorbance) @ coeff_vector)[0])
    pred_right = float((_design_matrix(right, spec, breakpoint_absorbance) @ coeff_vector)[0])
    assert abs(pred_left - pred_right) < 1.0e-4


def test_cross_run_batch_interface_can_run(monkeypatch, tmp_path: Path) -> None:
    run_a = tmp_path / "run_a.zip"
    run_b = tmp_path / "run_b.zip"
    run_a.write_text("a", encoding="utf-8")
    run_b.write_text("b", encoding="utf-8")

    def fake_run_debugger(input_path, **kwargs):
        stem = Path(input_path).stem
        output_dir = Path(kwargs["output_dir"])
        output_dir.mkdir(parents=True, exist_ok=True)
        overview = pd.DataFrame(
            [
                {"analyzer_id": "GA01", "old_chain_rmse": 5.0, "new_chain_rmse": 7.0, "old_zero_rmse": 2.0, "new_zero_rmse": 3.0, "old_temp_stability_metric": 1.0, "new_temp_stability_metric": 2.0, "winner_overall": "old_chain", "winner_zero": "old_chain", "winner_temp_stability": "old_chain", "winner_low_range": "old_chain", "winner_main_range": "old_chain"},
                {"analyzer_id": "GA02", "old_chain_rmse": 6.0, "new_chain_rmse": 4.0 if stem == "run_a" else 5.0, "old_zero_rmse": 2.5, "new_zero_rmse": 1.7, "old_temp_stability_metric": 1.4, "new_temp_stability_metric": 1.0, "winner_overall": "new_chain", "winner_zero": "new_chain", "winner_temp_stability": "new_chain", "winner_low_range": "new_chain", "winner_main_range": "new_chain"},
                {"analyzer_id": "GA03", "old_chain_rmse": 5.5, "new_chain_rmse": 4.5, "old_zero_rmse": 1.8, "new_zero_rmse": 1.5, "old_temp_stability_metric": 1.2, "new_temp_stability_metric": 0.9, "winner_overall": "new_chain", "winner_zero": "new_chain", "winner_temp_stability": "new_chain", "winner_low_range": "new_chain", "winner_main_range": "new_chain"},
            ]
        )
        selection = pd.DataFrame(
            [
                {"analyzer_id": "GA01", "best_absorbance_model": "model_a_linear", "best_model_family": "single_range", "zero_residual_mode": "none", "selected_source_pair": "raw/raw"},
                {"analyzer_id": "GA02", "best_absorbance_model": "piecewise_linear", "best_model_family": "piecewise_range", "zero_residual_mode": "linear", "selected_source_pair": "filt/filt"},
                {"analyzer_id": "GA03", "best_absorbance_model": "piecewise_quadratic", "best_model_family": "piecewise_range", "zero_residual_mode": "quadratic", "selected_source_pair": "raw/raw"},
            ]
        )
        return {
            "output_dir": output_dir,
            "validation_table": pd.DataFrame(),
            "comparison_outputs": {"overview_summary": overview},
            "model_results": {"selection": selection},
            "invalid_pressure_summary": pd.DataFrame([{"summary_scope": "overall", "invalid_point_count": 3, "invalid_sample_count": 30}]),
            "run_role_assessment": pd.DataFrame([{"assessment_scope": "run_summary", "has_high_temp_zero_anchor_candidate": stem == "run_b", "recommended_role": "mixed role"}]),
            "config": DebuggerConfig(input_path=output_dir, output_dir=output_dir),
            "ga01_special_note": "GA01 still looks like a high-temp zero-anchor issue.",
        }

    monkeypatch.setattr("tools.absorbance_debugger.app.run_debugger", fake_run_debugger)
    result = run_debugger_batch((run_a, run_b), output_dir=tmp_path / "batch")

    assert (Path(result["output_dir"]) / "step_09_cross_run_summary.csv").exists()
    assert (Path(result["output_dir"]) / "step_09_cross_run_by_analyzer.csv").exists()
    assert (Path(result["output_dir"]) / "step_09_cross_run_auto_conclusions.csv").exists()
    assert (Path(result["output_dir"]) / "step_09_cross_run_plots.png").exists()
    assert "GA02" in result["reproducibility_note"]
    assert set(result["cross_run_summary"]["run_name"]) == {"run_a", "run_b"}
    assert "invalid_pressure_excluded_count" in result["cross_run_summary"].columns
    assert set(result["cross_run_by_analyzer"]["analyzer_id"]) == {"GA01", "GA02", "GA03"}


def test_cross_run_batch_writes_scoped_old_vs_new_outputs(monkeypatch, tmp_path: Path) -> None:
    historical_a = tmp_path / "run_20260403_014845.zip"
    historical_b = tmp_path / "run_20260407_185002.zip"
    scope_b = tmp_path / "run_20260410_132440.zip"
    for path in (historical_a, historical_b, scope_b):
        path.write_text("placeholder", encoding="utf-8")

    def fake_run_debugger(input_path, **kwargs):
        stem = Path(input_path).stem
        output_dir = Path(kwargs["output_dir"])
        output_dir.mkdir(parents=True, exist_ok=True)
        return _synthetic_scoped_run_result(stem, output_dir)

    monkeypatch.setattr("tools.absorbance_debugger.app.run_debugger", fake_run_debugger)
    result = run_debugger_batch((historical_a, historical_b, scope_b), output_dir=tmp_path / "batch_scoped")
    output_dir = Path(result["output_dir"])

    assert (output_dir / "step_09c_historical_ga02_ga03_old_vs_new_detail.csv").exists()
    assert (output_dir / "step_09c_historical_ga02_ga03_old_vs_new_summary.csv").exists()
    assert (output_dir / "step_09c_historical_ga02_ga03_local_wins.csv").exists()
    assert (output_dir / "step_09c_historical_ga02_ga03_plot.png").exists()
    assert (output_dir / "step_09d_20260410_all_analyzers_old_vs_new_detail.csv").exists()
    assert (output_dir / "step_09d_20260410_all_analyzers_old_vs_new_summary.csv").exists()
    assert (output_dir / "step_09d_20260410_all_analyzers_local_wins.csv").exists()
    assert (output_dir / "step_09d_20260410_all_analyzers_plot.png").exists()
    assert (output_dir / "step_10c_scoped_old_vs_new_report.md").exists()
    assert (output_dir / "step_10d_executive_summary.md").exists()
    assert (output_dir / "step_10d_executive_summary.png").exists()
    assert (output_dir / "step_10e_comparison_reconciliation.md").exists()
    assert (output_dir / "step_10e_comparison_reconciliation.csv").exists()
    assert (output_dir / "step_09f_dual_surface_detail.csv").exists()
    assert (output_dir / "step_09f_dual_surface_summary.csv").exists()
    assert (output_dir / "step_09f_dual_surface_plot.png").exists()
    assert (output_dir / "step_10f_dual_surface_reconciliation.md").exists()

    scope_a_detail = pd.read_csv(output_dir / "step_09c_historical_ga02_ga03_old_vs_new_detail.csv")
    scope_a_summary = pd.read_csv(output_dir / "step_09c_historical_ga02_ga03_old_vs_new_summary.csv")
    scope_a_local = pd.read_csv(output_dir / "step_09c_historical_ga02_ga03_local_wins.csv")
    scope_b_detail = pd.read_csv(output_dir / "step_09d_20260410_all_analyzers_old_vs_new_detail.csv")
    scope_b_summary = pd.read_csv(output_dir / "step_09d_20260410_all_analyzers_old_vs_new_summary.csv")
    scope_b_local = pd.read_csv(output_dir / "step_09d_20260410_all_analyzers_local_wins.csv")
    dual_detail = pd.read_csv(output_dir / "step_09f_dual_surface_detail.csv")
    dual_summary = pd.read_csv(output_dir / "step_09f_dual_surface_summary.csv")

    assert {"run_id", "comparison_scope", "analyzer_id", "selected_source_pair", "actual_ratio_source_used"} <= set(scope_a_detail.columns)
    assert {"comparison_scope", "run_scope_description", "analyzer_set", "headline_safe_statement", "scope_limitation_statement"} <= set(scope_a_summary.columns)
    assert {"run_id", "analyzer_id", "local_win_flag", "local_win_rank_within_analyzer", "local_loss_rank_within_analyzer", "segment_tag"} <= set(scope_a_local.columns)
    assert {"run_id", "comparison_scope", "analyzer_id", "selected_source_pair", "actual_ratio_source_used"} <= set(scope_b_detail.columns)
    assert {"comparison_scope", "run_scope_description", "analyzer_set", "headline_safe_statement", "scope_limitation_statement"} <= set(scope_b_summary.columns)
    assert {"run_id", "analyzer_id", "local_win_flag", "local_win_rank_within_analyzer", "local_loss_rank_within_analyzer", "segment_tag"} <= set(scope_b_local.columns)

    assert set(scope_a_detail["analyzer_id"]) == {"GA02", "GA03"}
    assert "GA01" not in str(scope_a_summary.iloc[0]["analyzer_set"])
    assert set(scope_b_detail["analyzer_id"]) == {"GA01", "GA02", "GA03", "GA04"}
    assert scope_a_summary.iloc[0]["headline_safe_statement"] == "仅针对 historical packages 中的 GA02/GA03"
    assert scope_b_summary.iloc[0]["headline_safe_statement"] == "仅针对 run_20260410_132440 的全 analyzers"
    assert scope_a_summary.iloc[0]["scope_limitation_statement"] == "两个 scope 不能合并解读为统一全局结论"
    assert scope_b_summary.iloc[0]["scope_limitation_statement"] == "两个 scope 不能合并解读为统一全局结论"
    assert {
        "comparison_surface",
        "old_value_source",
        "new_value_source",
        "selected_prediction_scope",
        "selected_prediction_scope_majority",
    } <= set(dual_detail.columns)
    assert {
        "comparison_surface",
        "old_value_source",
        "new_value_source",
        "selected_prediction_scope",
        "selected_prediction_scope_majority",
    } <= set(dual_summary.columns)
    dual_summary_index = dual_summary.set_index("comparison_surface")
    assert dual_summary_index.loc["run_native_old_vs_new", "new_value_source"] == "analyzer_sheet_mean_co2_ppm"
    assert dual_summary_index.loc["debugger_reconstructed_old_vs_new", "new_value_source"] == "selected_pred_ppm_from_debugger"

    report = (output_dir / "step_10c_scoped_old_vs_new_report.md").read_text(encoding="utf-8")
    assert "historical GA02/GA03 comparison" in report
    assert "2026-04-10 all-analyzers comparison" in report
    assert "在历史数据包上，仅看 GA02/GA03，新算法相对旧算法" in report
    assert "在 2026-04-10 这包完整 run 上，全 analyzers 视角下新算法相对旧算法的真实状态是" in report

    executive = (output_dir / "step_10d_executive_summary.md").read_text(encoding="utf-8")
    assert "historical GA02/GA03" in executive
    assert "2026-04-10 all analyzers" in executive

    reconciliation = pd.read_csv(output_dir / "step_10e_comparison_reconciliation.csv")
    assert {
        "old_value_source",
        "new_value_source",
        "matched_only_filter_applied",
        "valid_only_filter_applied",
        "analyzer_inclusion_scope",
    } <= set(reconciliation.columns)
    reconciliation_md = (output_dir / "step_10e_comparison_reconciliation.md").read_text(encoding="utf-8")
    assert "old_value_source" in reconciliation_md
    assert "new_value_source" in reconciliation_md
    assert "matched_only_filter_applied" in reconciliation_md
    assert "valid_only_filter_applied" in reconciliation_md
    assert "analyzer_inclusion_scope" in reconciliation_md
    dual_report = (output_dir / "step_10f_dual_surface_reconciliation.md").read_text(encoding="utf-8")
    assert "what quick calc actually used" in dual_report
    assert "what current debugger comparison actually used" in dual_report
    assert "no evidence of old/new flip bug" in dual_report


def test_historical_run_detects_high_temp_anchor_and_invalid_500hpa(tmp_path: Path) -> None:
    output_dir = tmp_path / "historical_absorbance_debug"

    result = run_debugger(
        HISTORICAL_RUN_ZIP,
        output_dir=output_dir,
        ratio_source="raw",
        temperature_source="corr",
        pressure_source="corr",
    )

    assert (output_dir / "step_08z_run_role_assessment.csv").exists()
    role = pd.read_csv(output_dir / "step_08z_run_role_assessment.csv")
    run_row = role[role["assessment_scope"] == "run_summary"].iloc[0]
    assert bool(run_row["has_high_temp_zero_anchor_candidate"]) is True
    assert "high-temperature zero anchor candidate" in str(run_row["high_temp_zero_anchor_note"])

    invalid_summary = pd.read_csv(output_dir / "step_02x_invalid_pressure_summary.csv")
    overall_invalid = invalid_summary[invalid_summary["summary_scope"] == "overall"].iloc[0]
    assert int(overall_invalid["invalid_point_count"]) > 0

    overview = pd.read_csv(output_dir / "step_08_overview_summary.csv")
    assert "GA04" in set(overview["analyzer_id"])

    report_md = (output_dir / "report.md").read_text(encoding="utf-8")
    assert "this run provides a high-temperature zero anchor candidate" in report_md


def test_historical_run_bundle_contains_40c_zero_candidate() -> None:
    bundle = RunBundle(HISTORICAL_RUN_ZIP)
    artifacts = discover_run_artifacts(bundle)
    points_raw = bundle.read_csv(artifacts.files["points_readable"])
    temp_col = "温箱目标温度C"
    ppm_col = "目标二氧化碳浓度ppm"
    pressure_col = "目标压力hPa"
    points = points_raw.copy()
    mask = (
        pd.to_numeric(points[temp_col], errors="coerce").eq(40.0)
        & pd.to_numeric(points[ppm_col], errors="coerce").eq(0.0)
        & pd.to_numeric(points[pressure_col], errors="coerce").isna()
    )
    assert bool(mask.any()) is True


def test_run_bundle_discovers_zip_and_extracted_directory(tmp_path: Path) -> None:
    zip_bundle = RunBundle(REFERENCE_RUN_ZIP)
    zip_artifacts = discover_run_artifacts(zip_bundle)
    assert zip_artifacts.files["samples"] is not None
    assert zip_artifacts.files["points_readable"] is not None

    extracted_root = tmp_path / "extracted"
    with zipfile.ZipFile(REFERENCE_RUN_ZIP) as archive:
        archive.extractall(extracted_root)

    dir_bundle = RunBundle(extracted_root)
    dir_artifacts = discover_run_artifacts(dir_bundle)
    points = dir_bundle.read_csv(dir_artifacts.files["points_readable"])

    assert dir_artifacts.files["runtime_config"] is not None
    assert len(points) == 48


def test_option_normalizers_accept_gui_and_cli_tokens() -> None:
    assert normalize_ratio_source("raw") == "ratio_co2_raw"
    assert normalize_ratio_source("filt") == "ratio_co2_filt"
    assert normalize_temp_source("T_std") == "temp_std_c"
    assert normalize_temp_source("corr") == "temp_corr_c"
    assert normalize_pressure_source("P_std") == "pressure_std_hpa"
    assert normalize_pressure_source("corr") == "pressure_corr_hpa"
    assert normalize_absorbance_order_mode("samplewise") == "samplewise_log_first"
    assert normalize_absorbance_order_mode("compare_both") == "compare_both"
    assert normalize_invalid_pressure_mode("hard") == "hard_exclude"
    assert normalize_invalid_pressure_mode("diagnostic") == "diagnostic_only"
    assert normalize_model_selection_strategy("auto") == "auto_grouped"
    assert normalize_model_selection_strategy("grouped_loo") == "grouped_loo"


def test_invalid_pressure_filter_hard_excludes_500_hpa_bin(tmp_path: Path) -> None:
    config = DebuggerConfig(input_path=tmp_path, output_dir=tmp_path)
    filtered = pd.DataFrame(
        [
            {
                "analyzer": "GA01",
                "point_title": "p500",
                "point_row": 1,
                "route": "co2",
                "temp_set_c": 20.0,
                "target_co2_ppm": 400.0,
                "target_pressure_hpa": 500.0,
                "pressure_std_hpa": 503.0,
                "pressure_corr_hpa": 501.0,
                "sample_index": 1,
            },
            {
                "analyzer": "GA01",
                "point_title": "p500",
                "point_row": 1,
                "route": "co2",
                "temp_set_c": 20.0,
                "target_co2_ppm": 400.0,
                "target_pressure_hpa": 500.0,
                "pressure_std_hpa": 504.0,
                "pressure_corr_hpa": 502.0,
                "sample_index": 2,
            },
            {
                "analyzer": "GA01",
                "point_title": "p1013",
                "point_row": 2,
                "route": "co2",
                "temp_set_c": 20.0,
                "target_co2_ppm": 400.0,
                "target_pressure_hpa": 1013.25,
                "pressure_std_hpa": 1012.0,
                "pressure_corr_hpa": 1013.0,
                "sample_index": 1,
            },
        ]
    )
    invalid_points, invalid_summary, filtered_valid, excluded_invalid = _identify_invalid_pressure_points(filtered, config)

    assert len(invalid_points) == 1
    assert bool(invalid_points.iloc[0]["excluded_from_main_analysis"]) is True
    assert invalid_points.iloc[0]["invalid_reason"] == "legacy_invalid_pressure_target_500hpa"
    assert len(filtered_valid) == 1
    assert len(excluded_invalid) == 2
    assert int(invalid_summary.iloc[0]["invalid_point_count"]) == 1


def test_gui_passes_selection_parameters(monkeypatch, tmp_path: Path) -> None:
    captured: dict[str, object] = {}

    def fake_run_debugger(input_path, **kwargs):
        captured["input_path"] = str(input_path)
        captured.update(kwargs)
        return {"output_dir": str(tmp_path)}

    class ImmediateThread:
        def __init__(self, target, daemon) -> None:
            self._target = target

        def start(self) -> None:
            self._target()

    root = gui_module.Tk()
    root.withdraw()
    gui = gui_module.AbsorbanceDebuggerGui(root)
    gui.root.after = lambda _delay, callback: callback()
    gui.input_path.set(str(REFERENCE_RUN_ZIP))
    gui.output_dir.set(str(tmp_path))
    gui.p_ref.set("1009.5")
    gui.ratio_source.set("filt")
    gui.temperature_source.set("T_std")
    gui.pressure_source.set("P_std")
    gui.absorbance_order_mode.set("mean_first_log")
    gui.model_selection_strategy.set("grouped_loo")
    gui.enable_zero_residual_correction.set("1")
    gui.zero_residual_models.set("quadratic")
    gui.enable_water_zero_anchor_correction.set("1")
    gui.water_zero_anchor_models.set("linear")
    gui.enable_piecewise_model.set("1")
    gui.piecewise_boundary_ppm.set("180")
    gui.invalid_pressure_targets_hpa.set("500,530")
    gui.invalid_pressure_tolerance_hpa.set("25")
    gui.enable_composite_score.set("0")
    gui.run_source_consistency_compare.set("0")
    gui.run_pressure_branch_compare.set("1")
    gui.run_upper_bound_compare.set("0")
    gui.hard_invalid_pressure_exclude.set("1")
    gui.use_valid_only_main_conclusion.set("1")
    gui.auto_open_report.set("0")

    monkeypatch.setattr(gui_module, "run_debugger", fake_run_debugger)
    monkeypatch.setattr(gui_module.threading, "Thread", lambda target, daemon=True: ImmediateThread(target, daemon))

    gui._start_analysis()
    root.destroy()

    assert captured["input_path"] == str(REFERENCE_RUN_ZIP)
    assert captured["ratio_source"] == "ratio_co2_filt"
    assert captured["temperature_source"] == "temp_std_c"
    assert captured["pressure_source"] == "pressure_std_hpa"
    assert captured["absorbance_order_mode"] == "mean_first_log"
    assert captured["model_selection_strategy"] == "grouped_loo"
    assert captured["enable_composite_score"] is False
    assert captured["enable_zero_residual_correction"] is True
    assert captured["zero_residual_models"] == "quadratic"
    assert captured["enable_water_zero_anchor_correction"] is True
    assert captured["water_zero_anchor_models"] == "linear"
    assert captured["enable_piecewise_model"] is True
    assert captured["piecewise_boundary_ppm"] == 180.0
    assert captured["run_r0_source_consistency_compare"] is False
    assert captured["run_pressure_branch_compare"] is True
    assert captured["run_upper_bound_compare"] is False
    assert captured["invalid_pressure_targets_hpa"] == "500,530"
    assert captured["invalid_pressure_tolerance_hpa"] == 25.0
    assert captured["invalid_pressure_mode"] == "hard_exclude"
    assert captured["use_valid_only_main_conclusion"] is True
    assert captured["p_ref_hpa"] == 1009.5


def test_tool_has_no_runtime_import_to_v1() -> None:
    tool_root = REPO_ROOT / "tools" / "absorbance_debugger"
    offenders: list[str] = []
    for path in tool_root.rglob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                names = [alias.name for alias in node.names]
            elif isinstance(node, ast.ImportFrom):
                module = node.module or ""
                names = [module]
            else:
                continue
            if any(name == "run_app" or name.startswith("src.gas_calibrator") for name in names):
                offenders.append(str(path))
                break
    assert offenders == []


def test_parse_path_list_supports_batch_gui_entry() -> None:
    assert parse_path_list("a.zip;b.zip\nc") == ("a.zip", "b.zip", "c")
