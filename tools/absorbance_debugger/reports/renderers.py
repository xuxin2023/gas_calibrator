"""Markdown/HTML/Excel report helpers."""

from __future__ import annotations

import html
from pathlib import Path
from typing import Mapping

import pandas as pd


def _table_to_markdown(frame: pd.DataFrame, max_rows: int = 20) -> str:
    if frame.empty:
        return "_No rows._"
    clipped = frame.head(max_rows).copy()
    columns = [str(column) for column in clipped.columns]
    header = "| " + " | ".join(columns) + " |"
    sep = "| " + " | ".join("---" for _ in columns) + " |"
    rows = []
    for values in clipped.astype(object).where(pd.notna(clipped), "").itertuples(index=False, name=None):
        rows.append("| " + " | ".join(str(value) for value in values) + " |")
    return "\n".join([header, sep, *rows])


def _format_number(value: object, digits: int = 3) -> str:
    number = pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]
    if pd.isna(number):
        return "n/a"
    return f"{float(number):.{digits}f}"


def render_report_markdown(report: Mapping[str, object]) -> str:
    """Render a human-readable markdown report."""

    lines: list[str] = []
    lines.append(f"# Offline Absorbance Debug Report: {report['run_name']}")
    lines.append("")
    lines.append("## 1. Data inventory")
    lines.append(f"- Input source: `{report['input_path']}`")
    lines.append(f"- Output directory: `{report['output_dir']}`")
    lines.append(f"- Total points identified: `{report['point_count']}`")
    lines.append(f"- CO2 points identified: `{report['co2_point_count']}`")
    lines.append(f"- H2O points identified: `{report['h2o_point_count']}`")
    lines.append("")
    lines.append("## 2. Available analyzers")
    lines.append(f"- Main analyzers: `{', '.join(report['main_analyzers'])}`")
    lines.append(f"- Warning-only analyzers: `{', '.join(report['warning_only_analyzers'])}`")
    lines.append(f"- Analyzers seen in data: `{', '.join(report['detected_analyzers'])}`")
    lines.append("")
    lines.append("### Analyzer scope")
    lines.append(_table_to_markdown(report["analyzer_scope"], max_rows=12))
    lines.append("")
    lines.append("## 3. Temperature coverage")
    lines.append(f"- CO2 temperatures: `{report['co2_temperatures']}`")
    lines.append(f"- Zero-gas temperatures: `{report['zero_temperatures']}`")
    lines.append(f"- Missing zero-gas temperatures: `{report['missing_zero_temperatures']}`")
    run_role = report["run_role_assessment"]
    run_role_row = run_role[run_role["assessment_scope"] == "run_summary"].iloc[0] if not run_role.empty else {}
    if bool(run_role_row.get("has_high_temp_zero_anchor_candidate", False)):
        lines.append("- this run provides a high-temperature zero anchor candidate")
    else:
        lines.append("- 40 C is excluded from R0(T) fitting because the run has no ambient 0 ppm point there.")
    lines.append("")
    lines.append("## 4. Formula summary")
    for label, formula in report["formulas"].items():
        lines.append(f"- {label}: `{formula}`")
    lines.append("")
    lines.append("## 5. Rule Freeze for Fair Absorbance Challenge")
    for item in report["rule_freeze"]:
        lines.append(f"- {item}")
    lines.append("")
    lines.append("## 6. Validation checks")
    lines.append(_table_to_markdown(report["validation_table"], max_rows=20))
    lines.append("")
    lines.append("## 7. Invalid pressure exclusion")
    lines.append("### Selected matched sources")
    lines.append(_table_to_markdown(report["selected_source_summary"], max_rows=12))
    lines.append("")
    lines.append("### Invalid pressure summary")
    lines.append(_table_to_markdown(report["invalid_pressure_summary"], max_rows=12))
    lines.append("")
    lines.append("### Invalid pressure points")
    lines.append(_table_to_markdown(report["invalid_pressure_points"], max_rows=20))
    lines.append("")
    lines.append("## Water/Pressure Data Lineage Assessment")
    for item in report.get("old_water_audit_summary", []):
        lines.append(f"- {item}")
    lines.append("")
    lines.append("### Old water correction audit sources")
    lines.append(_table_to_markdown(report.get("old_water_audit_sources", pd.DataFrame()), max_rows=20))
    lines.append("")
    lines.append("### New-chain input audit")
    lines.append(_table_to_markdown(report.get("new_chain_input_audit", pd.DataFrame()), max_rows=12))
    lines.append("")
    lines.append("### Pressure data assessment")
    lines.append(_table_to_markdown(report.get("pressure_data_assessment", pd.DataFrame()), max_rows=12))
    lines.append("")
    lines.append("### Water zero-anchor selection")
    lines.append(_table_to_markdown(report.get("water_zero_anchor_selection", pd.DataFrame()), max_rows=12))
    lines.append("")
    lines.append("### Water zero-anchor compare")
    lines.append(_table_to_markdown(report.get("water_anchor_compare", pd.DataFrame()), max_rows=12))
    lines.append("")
    lines.append("### Legacy water replay summary")
    lines.append(_table_to_markdown(report.get("legacy_water_replay_summary", pd.DataFrame()), max_rows=12))
    lines.append("")
    lines.append("### Legacy water replay stage metrics")
    lines.append(_table_to_markdown(report.get("legacy_water_replay_stage_metrics", pd.DataFrame()), max_rows=18))
    lines.append("")
    lines.append("### Legacy water replay conclusions")
    lines.append(_table_to_markdown(report.get("legacy_water_replay_conclusions", pd.DataFrame()), max_rows=12))
    lines.append("")
    lines.append("## 8. Key coefficients")
    lines.append("### Temperature correction")
    lines.append(_table_to_markdown(report["temperature_coefficients"], max_rows=12))
    lines.append("")
    lines.append("### Pressure correction")
    lines.append(_table_to_markdown(report["pressure_coefficients"], max_rows=12))
    lines.append("")
    lines.append("### R0(T)")
    lines.append(_table_to_markdown(report["r0_coefficients"], max_rows=18))
    lines.append("")
    lines.append("### Zero residual ΔA0(T)")
    lines.append(_table_to_markdown(report["zero_residual_models"], max_rows=18))
    lines.append("")
    lines.append("### Zero residual selection")
    lines.append(_table_to_markdown(report["zero_residual_selection"], max_rows=12))
    lines.append("")
    lines.append("### Water zero-anchor models")
    lines.append(_table_to_markdown(report.get("water_zero_anchor_models", pd.DataFrame()), max_rows=18))
    lines.append("")
    lines.append("## 9. Absorbance model selection")
    lines.append("### Selected models")
    lines.append(_table_to_markdown(report["absorbance_model_selection"], max_rows=12))
    lines.append("")
    lines.append("### Candidate scores")
    lines.append(_table_to_markdown(report["absorbance_model_scores"], max_rows=24))
    lines.append("")
    lines.append("### Piecewise challenge summary")
    lines.append(_table_to_markdown(report["piecewise_model_selection"], max_rows=12))
    lines.append("")
    lines.append("### Weight sensitivity")
    lines.append(_table_to_markdown(report["weight_sensitivity_compare"], max_rows=12))
    lines.append("")
    lines.append("## 10. Why New Chain Loses")
    if report["diagnostic_top_lines"]:
        for item in report["diagnostic_top_lines"]:
            lines.append(f"- {item}")
    lines.append(f"- Implementation assessment: `{report['implementation_issue']}`")
    lines.append("")
    lines.append("### Absorbance order compare")
    lines.append(_table_to_markdown(report["order_compare"], max_rows=12))
    lines.append("")
    lines.append("### R0 source consistency")
    lines.append(_table_to_markdown(report["source_consistency"], max_rows=12))
    lines.append("")
    lines.append("### Pressure branch compare")
    lines.append(_table_to_markdown(report["pressure_branch_compare"], max_rows=12))
    lines.append("")
    lines.append("### Upper bound vs deployable")
    lines.append(_table_to_markdown(report["upper_bound_vs_deployable"], max_rows=12))
    lines.append("")
    lines.append("### Root cause ranking")
    lines.append(_table_to_markdown(report["root_cause_ranking"], max_rows=12))
    lines.append("")
    lines.append("## 11. Old vs new comparison")
    lines.append("- Main conclusion uses the valid-only chain after invalid-pressure exclusion.")
    lines.append("- New-chain comparison uses the selected absorbance model with grouped validation predictions when available.")
    lines.append("")
    lines.append("### Overview summary")
    lines.append(_table_to_markdown(report["overview_summary"], max_rows=20))
    lines.append("")
    lines.append("### By temperature")
    lines.append(_table_to_markdown(report["by_temperature"], max_rows=24))
    lines.append("")
    lines.append("### By concentration range")
    lines.append(_table_to_markdown(report["by_concentration_range"], max_rows=24))
    lines.append("")
    lines.append("### Zero-point special")
    lines.append(_table_to_markdown(report["zero_special"], max_rows=24))
    lines.append("")
    lines.append("### Regression overall")
    lines.append(_table_to_markdown(report["regression_overall"], max_rows=24))
    if report["comparison_conclusions"]:
        lines.append("")
        lines.append("### Analyzer conclusions")
        for item in report["comparison_conclusions"]:
            lines.append(f"- {item}")
    lines.append("")
    lines.append("## 12. Before/after tightening")
    lines.append(_table_to_markdown(report["before_after_summary"], max_rows=12))
    lines.append("")
    lines.append("## 13. Run Role Assessment")
    lines.append(f"- {report['run_role_note']}")
    lines.append("")
    lines.append(_table_to_markdown(report["run_role_assessment"], max_rows=16))
    lines.append("")
    lines.append("## 14. GA01 Special Profile")
    lines.append(f"- {report['ga01_special_note']}")
    lines.append("")
    lines.append(_table_to_markdown(report["ga01_profile"], max_rows=24))
    lines.append("")
    lines.append("## 15. Full-data appendix")
    lines.append("### Full-data overview summary")
    lines.append(_table_to_markdown(report["appendix_overview_summary"], max_rows=20))
    lines.append("")
    lines.append("### Full-data automatic conclusion")
    lines.append(_table_to_markdown(report["appendix_auto_conclusions"], max_rows=10))
    lines.append("")
    lines.append("## 16. Automatic conclusion page")
    lines.append(_table_to_markdown(report["auto_conclusions"], max_rows=20))
    lines.append("")
    lines.append("## 17. Base/final mode")
    lines.append(f"- Enabled: `{report['base_final_enabled']}`")
    lines.append(f"- Source: `{report['base_final_source']}`")
    lines.append("")
    lines.append("## 18. Limitations")
    for item in report["limitations"]:
        lines.append(f"- {item}")
    lines.append("")
    lines.append("## 19. Suggested next experiments")
    for item in report["next_steps"]:
        lines.append(f"- {item}")
    lines.append("")
    return "\n".join(lines)


def render_old_vs_new_report_markdown(report: Mapping[str, object]) -> str:
    """Render the step-10 deployable old-vs-new markdown report."""

    lines: list[str] = []
    summary = report["summary"]
    detail = report["detail"]
    aggregate_segments = report["aggregate_segments"]
    local_wins = report["local_wins"]
    ratio_source_audit = report["ratio_source_audit"]
    diagnostic_candidates = report["diagnostic_candidates"]
    source_selection_audit_summary = report.get("source_selection_audit_summary", pd.DataFrame())
    source_selection_audit_conclusions = report.get("source_selection_audit_conclusions", pd.DataFrame())
    source_policy_challenge_summary = report.get("source_policy_challenge_summary", pd.DataFrame())
    source_policy_challenge_conclusions = report.get("source_policy_challenge_conclusions", pd.DataFrame())
    summary_row = summary.iloc[0] if not summary.empty else pd.Series(dtype=object)
    ratio_row = ratio_source_audit.iloc[0] if not ratio_source_audit.empty else pd.Series(dtype=object)

    lines.append(f"# Old vs New Report: {report['run_name']}")
    lines.append("")
    lines.append("## 1. headline")
    lines.append("- headline_comparison_scope = old_chain vs current_deployable_new_chain")
    lines.append(
        "- headline_verdict = "
        + (
            f"{summary_row.get('overall_verdict', 'unknown')} "
            f"(whether_new_chain_has_overall_evidence_to_surpass_old={summary_row.get('whether_new_chain_has_overall_evidence_to_surpass_old', False)})"
            if not summary.empty
            else "unknown"
        )
    )
    if not summary.empty:
        lines.append(
            "- headline_readout = "
            f"overall old={_format_number(summary_row.get('overall_rmse_old'))}, "
            f"new={_format_number(summary_row.get('overall_rmse_new'))}, "
            f"improvement_pct={_format_number(summary_row.get('overall_improvement_pct'), 2)}%"
        )
    lines.append("")
    lines.append("## 2. overall comparison")
    lines.append("- comparison_scope = current_deployable_new_chain vs old_chain")
    lines.append(_table_to_markdown(aggregate_segments, max_rows=8))
    lines.append("")
    lines.append("## 3. analyzer-by-analyzer comparison")
    analyzer_columns = [
        "analyzer_id",
        "mode2_semantic_profile",
        "selected_source_pair",
        "actual_ratio_source_used",
        "improvement_pct_overall",
        "improvement_pct_zero",
        "improvement_pct_low",
        "improvement_pct_main",
        "overall_win_flag",
        "zero_win_flag",
        "low_win_flag",
        "main_win_flag",
    ]
    analyzer_view = detail[analyzer_columns].copy() if not detail.empty else pd.DataFrame(columns=analyzer_columns)
    lines.append(_table_to_markdown(analyzer_view, max_rows=20))
    lines.append("")
    lines.append("## 4. local wins and losses")
    if local_wins.empty:
        lines.append("_No local win/loss rows._")
    else:
        for analyzer_id in detail["analyzer_id"].astype(str).tolist():
            lines.append(f"### {analyzer_id}")
            lines.append("Typical local wins")
            win_rows = local_wins[
                (local_wins["analyzer_id"].astype(str) == analyzer_id)
                & (local_wins["local_win_flag"].fillna(False))
            ][
                [
                    "temp_set_c",
                    "target_ppm",
                    "old_value",
                    "new_value",
                    "abs_error_old",
                    "abs_error_new",
                    "improvement_abs_error",
                    "segment_tag",
                ]
            ]
            lines.append(_table_to_markdown(win_rows, max_rows=5))
            lines.append("")
            lines.append("Typical local losses")
            loss_rows = local_wins[
                (local_wins["analyzer_id"].astype(str) == analyzer_id)
                & (~local_wins["local_win_flag"].fillna(False))
            ][
                [
                    "temp_set_c",
                    "target_ppm",
                    "old_value",
                    "new_value",
                    "abs_error_old",
                    "abs_error_new",
                    "improvement_abs_error",
                    "segment_tag",
                ]
            ]
            lines.append(_table_to_markdown(loss_rows, max_rows=5))
            lines.append("")
    lines.append("## 5. ratio source audit")
    lines.append("- designed_v5_ratio_source_intent = raw_or_instantaneous")
    lines.append(
        "- actual_ratio_source_used_in_this_run = "
        + str(ratio_row.get("actual_ratio_source_used_in_this_run", summary_row.get("actual_ratio_source_used_in_this_run", "unknown")))
    )
    lines.append(
        "- actual_ratio_source_used_majority = "
        + str(ratio_row.get("actual_ratio_source_used_majority", summary_row.get("actual_ratio_source_used_majority", "unknown")))
    )
    lines.append(
        "- supporting_evidence_for_actual_ratio_source = "
        + str(ratio_row.get("supporting_evidence_for_actual_ratio_source", summary_row.get("supporting_evidence_for_actual_ratio_source", "")))
    )
    lines.append(
        "- why_intent_and_execution_can_differ = "
        "V5 design intent takes instantaneous R first and filters later in baseline/turbulence handling, "
        "but this offline run still selects matched raw/raw or filt/filt branches per analyzer during fitting."
    )
    lines.append("")
    lines.append("## 6. source selection audit")
    if not source_selection_audit_summary.empty:
        overall_audit = source_selection_audit_summary[source_selection_audit_summary["summary_scope"] == "overall"]
        overall_row = overall_audit.iloc[0] if not overall_audit.empty else pd.Series(dtype=object)
        lines.append("- why_selected_source_pair_became_mixed = " + str(overall_row.get("why_selected_source_pair_became_mixed", "")))
        lines.append("- selection_reason_primary = " + "; ".join(
            f"{row.analyzer_id}={row.selection_reason_primary}"
            for row in source_selection_audit_summary[source_selection_audit_summary["summary_scope"] == "per_analyzer"].itertuples(index=False)
        ))
        audit_view = source_selection_audit_summary[
            source_selection_audit_summary["summary_scope"] == "per_analyzer"
        ][
            [
                "analyzer_id",
                "selected_source_pair",
                "selection_reason_primary",
                "selection_reason_secondary",
                "raw_score_if_forced",
                "filt_score_if_forced",
            ]
        ]
        lines.append(_table_to_markdown(audit_view, max_rows=12))
    else:
        lines.append("_No source selection audit summary available._")
    if not source_selection_audit_conclusions.empty:
        lines.append("")
        lines.append(_table_to_markdown(source_selection_audit_conclusions, max_rows=12))
    lines.append("")
    lines.append("## 7. source policy challenge")
    if not source_policy_challenge_summary.empty:
        raw_first_row = source_policy_challenge_summary[
            source_policy_challenge_summary["source_policy_mode"] == "raw_first_with_fallback"
        ]
        raw_first_row = raw_first_row.iloc[0] if not raw_first_row.empty else pd.Series(dtype=object)
        lines.append(
            "- whether_raw_first_improves_current_deployable_result = "
            + str(raw_first_row.get("whether_improves_current_deployable_result", False))
        )
        lines.append(_table_to_markdown(source_policy_challenge_summary, max_rows=12))
    else:
        lines.append("_No source policy challenge summary available._")
    if not source_policy_challenge_conclusions.empty:
        lines.append("")
        lines.append(_table_to_markdown(source_policy_challenge_conclusions, max_rows=12))
    lines.append("")
    lines.append("## 8. diagnostic candidates appendix")
    lines.append("- The headline above remains locked to old_chain vs current_deployable_new_chain. Everything below is diagnostic-only and not a deployable conclusion.")
    if diagnostic_candidates.empty:
        lines.append("_No diagnostic-only candidate summary available._")
    else:
        lines.append(_table_to_markdown(diagnostic_candidates, max_rows=12))
    lines.append("")
    return "\n".join(lines)


def render_candidate_tournament_report_markdown(report: Mapping[str, object]) -> str:
    """Render the step-12 candidate tournament markdown report."""

    summary = report.get("summary", pd.DataFrame())
    summary = summary.copy() if isinstance(summary, pd.DataFrame) else pd.DataFrame()
    overall = summary[summary["summary_scope"].astype(str) == "candidate_overall"].copy() if not summary.empty else pd.DataFrame()
    scoped = summary[summary["summary_scope"].astype(str) == "candidate_scope_surface"].copy() if not summary.empty else pd.DataFrame()

    def _scope_row(candidate_id: str, scope_name: str, surface_name: str) -> pd.Series:
        if scoped.empty:
            return pd.Series(dtype=object)
        subset = scoped[
            (scoped["candidate_id"].astype(str) == str(candidate_id))
            & (scoped["comparison_scope"].astype(str) == scope_name)
            & (scoped["comparison_surface"].astype(str) == surface_name)
        ]
        return subset.iloc[0] if not subset.empty else pd.Series(dtype=object)

    top_row = overall.iloc[0] if not overall.empty else pd.Series(dtype=object)
    top_candidate_id = str(top_row.get("candidate_id") or "")
    top_b1 = _scope_row(top_candidate_id, "run_20260410_132440_all_analyzers_candidate_tournament", "run_native_old_vs_new")
    top_b2 = _scope_row(top_candidate_id, "run_20260410_132440_all_analyzers_candidate_tournament", "debugger_reconstructed_old_vs_new")
    top_a1 = _scope_row(top_candidate_id, "historical_ga02_ga03_candidate_tournament", "run_native_old_vs_new")

    source_compare = (
        overall.groupby("source_mode", dropna=False)["promotion_score"].mean().sort_values(ascending=False)
        if not overall.empty and "source_mode" in overall.columns
        else pd.Series(dtype=float)
    )
    branch_answer = str(source_compare.index[0]) if not source_compare.empty else "unknown"

    category_map = {
        "humidity cross": overall[overall["model_family"].astype(str).isin(["v5_abs_k_minimal_proxy", "legacy_humidity_cross_D", "legacy_humidity_cross_E"])],
        "temp residual": overall[overall["residual_head"].astype(str) == "global_temp_residual"],
        "analyzer-specific residual": overall[overall["residual_head"].astype(str) == "analyzer_specific_humidity_plus_temp_residual"],
    } if not overall.empty else {}
    category_scores = {
        label: float(pd.to_numeric(frame["promotion_score"], errors="coerce").max())
        for label, frame in category_map.items()
        if frame is not None and not frame.empty
    }
    category_answer = max(category_scores, key=category_scores.get) if category_scores else "unknown"

    gap_rows = pd.DataFrame()
    if not report.get("detail", pd.DataFrame()).empty and top_candidate_id:
        detail = report["detail"].copy()
        gap_rows = detail[
            (detail["candidate_id"].astype(str) == top_candidate_id)
            & (detail["comparison_scope"].astype(str) == "run_20260410_132440_all_analyzers_candidate_tournament")
            & (detail["comparison_surface"].astype(str) == "debugger_reconstructed_old_vs_new")
        ][
            [
                "run_id",
                "analyzer_id",
                "improvement_pct_overall",
                "improvement_pct_zero",
                "improvement_pct_main",
            ]
        ].sort_values(["improvement_pct_overall", "analyzer_id"], ascending=[True, True], ignore_index=True)

    lines: list[str] = []
    lines.append("# Candidate Tournament Report")
    lines.append("")
    lines.append("## 1. surfaces")
    lines.append("- primary acceptance surface = run_native_old_vs_new")
    lines.append("- secondary diagnostic surface = debugger_reconstructed_old_vs_new")
    lines.append("- Surface 1 keeps old = old_residual_csv_prediction_simplified and new = analyzer_sheet_mean_co2_ppm.")
    lines.append("- Surface 2 keeps old = old_residual_csv_prediction_simplified and new = selected_pred_ppm_from_debugger.")
    lines.append("")
    lines.append("## 2. promotion score")
    lines.append("- promotion score favors Scope B + Surface 1 overall/main context, then checks Scope B + Surface 2 diagnostic performance, Scope A + Surface 1 stability, zero guardrails, and robustness penalties.")
    lines.append("- analyzer_specific_humidity_plus_temp_residual is always marked diagnostic-only and is penalized for future deployable ranking.")
    lines.append("")
    lines.append("## 3. top candidate")
    if overall.empty:
        lines.append("_No candidate rows were generated._")
    else:
        lines.append(_table_to_markdown(overall.head(10), max_rows=10))
    lines.append("")
    lines.append("## 4. required answers")
    lines.append(
        "1. Which candidate is closest to or above old_chain on Scope B + Surface 1? "
        + (
            f"`{top_candidate_id}` with Scope B Surface 1 overall={_format_number(top_b1.get('improvement_pct_overall'), 2)}%, "
            f"main={_format_number(top_b1.get('improvement_pct_main'), 2)}%."
            if top_candidate_id
            else "n/a"
        )
    )
    lines.append(
        "2. Does it also satisfy the Scope A stability guardrail? "
        + (
            f"Scope A Surface 1 overall={_format_number(top_a1.get('improvement_pct_overall'), 2)}%, "
            f"main={_format_number(top_a1.get('improvement_pct_main'), 2)}%."
            if top_candidate_id
            else "n/a"
        )
    )
    lines.append(f"3. Which raw vs filt branch looks more promising? `{branch_answer}`.")
    lines.append(f"4. Which family of changes looks most effective? `{category_answer}`.")
    lines.append(
        "5. What does the current most likely main cause look like? "
        + (f"`{top_row.get('likely_root_cause_bucket', 'mixed')}`." if top_candidate_id else "n/a")
    )
    lines.append(
        "6. Is there a future deployable candidate already? "
        + (
            f"`{bool(top_row.get('future_deployable_candidate_flag', False))}` for `{top_candidate_id}`."
            if top_candidate_id
            else "n/a"
        )
    )
    if gap_rows.empty:
        lines.append("7. If it still does not beat old_chain, which analyzers or segments remain behind? n/a")
    else:
        laggards = gap_rows[
            pd.to_numeric(gap_rows["improvement_pct_overall"], errors="coerce").fillna(0.0) <= 0.0
        ].head(6)
        laggard_text = "; ".join(
            f"{row.analyzer_id}:overall={_format_number(row.improvement_pct_overall, 2)}%, zero={_format_number(row.improvement_pct_zero, 2)}%, main={_format_number(row.improvement_pct_main, 2)}%"
            for row in laggards.itertuples(index=False)
        ) or "no clear laggard rows"
        lines.append(f"7. If it still does not beat old_chain, which analyzers or segments remain behind? {laggard_text}")
    lines.append("")
    lines.append("## 5. scope-by-scope summary")
    if scoped.empty:
        lines.append("_No scope/surface summary rows._")
    else:
        lines.append(_table_to_markdown(scoped.head(24), max_rows=24))
    lines.append("")
    lines.append("## 6. notes")
    lines.append("- Scope A and Scope B stay separate and are not merged into a single headline.")
    lines.append("- primary acceptance surface remains the authoritative acceptance benchmark.")
    lines.append("- secondary diagnostic surface is used to rank debugger-side candidate chains.")
    return "\n".join(lines)


def _scoped_summary_readout(summary_row: pd.Series) -> str:
    return (
        f"overall old={_format_number(summary_row.get('overall_rmse_old_scoped'))}, "
        f"new={_format_number(summary_row.get('overall_rmse_new_scoped'))}, "
        f"improvement={_format_number(summary_row.get('overall_improvement_pct_scoped'), 2)}%; "
        f"zero improvement={_format_number(summary_row.get('zero_improvement_pct_scoped'), 2)}%; "
        f"low improvement={_format_number(summary_row.get('low_improvement_pct_scoped'), 2)}%; "
        f"main improvement={_format_number(summary_row.get('main_improvement_pct_scoped'), 2)}%"
    )


def _scoped_local_example_lines(local_wins: pd.DataFrame, analyzer_id: str, *, win_flag: bool, limit: int = 3) -> list[str]:
    if local_wins.empty:
        return []
    rank_column = "local_win_rank_within_analyzer" if win_flag else "local_loss_rank_within_analyzer"
    subset = local_wins[
        (local_wins["analyzer_id"].astype(str) == str(analyzer_id))
        & (local_wins["local_win_flag"].fillna(False) == bool(win_flag))
    ].copy()
    if subset.empty:
        return []
    subset = subset.sort_values([rank_column, "run_id"], ignore_index=True).head(limit)
    lines: list[str] = []
    for row in subset.itertuples(index=False):
        lines.append(
            f"{row.run_id} @ { _format_number(row.temp_set_c, 1) }C / { _format_number(row.target_ppm, 1) } ppm: "
            f"old abs error { _format_number(row.abs_error_old) } -> new abs error { _format_number(row.abs_error_new) }, "
            f"improvement { _format_number(row.improvement_abs_error) } ppm ({row.segment_tag})"
        )
    return lines


def _scoped_source_note_lines(detail: pd.DataFrame) -> list[str]:
    if detail.empty:
        return ["No analyzer-level source note available."]
    lines: list[str] = []
    for analyzer_id, subset in detail.groupby("analyzer_id", dropna=False):
        run_bits = [
            f"{row.run_id}={row.actual_ratio_source_used} ({row.selected_source_pair})"
            for row in subset.sort_values("run_id").itertuples(index=False)
        ]
        lines.append(f"{analyzer_id}: " + "; ".join(run_bits))
    return lines


def _historical_scoped_conclusion(summary_row: pd.Series) -> str:
    overall_gain = pd.to_numeric(pd.Series([summary_row.get("overall_improvement_pct_scoped")]), errors="coerce").iloc[0]
    zero_gain = pd.to_numeric(pd.Series([summary_row.get("zero_improvement_pct_scoped")]), errors="coerce").iloc[0]
    low_gain = pd.to_numeric(pd.Series([summary_row.get("low_improvement_pct_scoped")]), errors="coerce").iloc[0]
    main_gain = pd.to_numeric(pd.Series([summary_row.get("main_improvement_pct_scoped")]), errors="coerce").iloc[0]
    all_positive = all(pd.notna(value) and float(value) > 0.0 for value in (overall_gain, zero_gain, low_gain, main_gain))
    if all_positive:
        return "在历史数据包上，仅看 GA02/GA03，新算法相对旧算法已表现出持续优势。"
    return "在历史数据包上，仅看 GA02/GA03，新算法相对旧算法尚未表现出持续优势。"


def _scope_b_scoped_conclusion(summary_row: pd.Series) -> str:
    overall_gain = pd.to_numeric(pd.Series([summary_row.get("overall_improvement_pct_scoped")]), errors="coerce").iloc[0]
    if pd.notna(overall_gain) and float(overall_gain) > 0.0:
        return "在 2026-04-10 这包完整 run 上，全 analyzers 视角下新算法相对旧算法的真实状态是整体已领先，但仍需逐台看局部赢输点。"
    return "在 2026-04-10 这包完整 run 上，全 analyzers 视角下新算法相对旧算法的真实状态是整体仍未领先，且需要逐台看局部赢输点。"


def render_scoped_old_vs_new_report_markdown(report: Mapping[str, object]) -> str:
    """Render the step-10c scoped old-vs-new markdown report."""

    scope_a = report["scope_a"]
    scope_b = report["scope_b"]
    scope_a_summary = scope_a["summary"]
    scope_b_summary = scope_b["summary"]
    scope_a_row = scope_a_summary.iloc[0] if not scope_a_summary.empty else pd.Series(dtype=object)
    scope_b_row = scope_b_summary.iloc[0] if not scope_b_summary.empty else pd.Series(dtype=object)
    scope_a_detail = scope_a["detail"]
    scope_b_detail = scope_b["detail"]
    scope_a_analyzers = scope_a["analyzer_aggregate"]
    scope_b_analyzers = scope_b["analyzer_aggregate"]
    scope_a_local = scope_a["local_wins"]
    scope_b_local = scope_b["local_wins"]

    lines: list[str] = []
    lines.append("# Scoped Old vs New Report")
    lines.append("")
    lines.append("## 1. headline")
    lines.append("- 本报告包含两个 scope：historical GA02/GA03 comparison，以及 2026-04-10 all-analyzers comparison。")
    lines.append("- 两个 scope 不能合并解读为统一全局结论。")
    lines.append("- 主比较对象固定为 old_chain vs current_deployable_new_chain。")
    lines.append("")
    lines.append("## 2. Scope A: historical GA02/GA03 comparison")
    lines.append(f"- headline_safe_statement = {scope_a_row.get('headline_safe_statement', '')}")
    lines.append(f"- overall / zero / low / main = {_scoped_summary_readout(scope_a_row)}")
    lines.append("")
    lines.append(_table_to_markdown(scope_a["aggregate_segments"], max_rows=8))
    lines.append("")
    if scope_a_analyzers.empty:
        lines.append("_No Scope A analyzer rows._")
    else:
        for analyzer_id in ("GA02", "GA03"):
            analyzer_rows = scope_a_analyzers[scope_a_analyzers["analyzer_id"].astype(str) == analyzer_id].copy()
            if analyzer_rows.empty:
                continue
            analyzer_row = analyzer_rows.iloc[0]
            per_run = scope_a_detail[scope_a_detail["analyzer_id"].astype(str) == analyzer_id].copy()
            lines.append(f"### {analyzer_id}")
            lines.append(
                f"- pooled overall / zero / low / main improvement = "
                f"{_format_number(analyzer_row.get('improvement_pct_overall'), 2)}% / "
                f"{_format_number(analyzer_row.get('improvement_pct_zero'), 2)}% / "
                f"{_format_number(analyzer_row.get('improvement_pct_low'), 2)}% / "
                f"{_format_number(analyzer_row.get('improvement_pct_main'), 2)}%"
            )
            lines.append(
                f"- run-by-run wins = overall {int(per_run['overall_win_flag'].fillna(False).map(bool).sum())}/{len(per_run)}, "
                f"zero {int(per_run['zero_win_flag'].fillna(False).map(bool).sum())}/{len(per_run)}, "
                f"low {int(per_run['low_win_flag'].fillna(False).map(bool).sum())}/{len(per_run)}, "
                f"main {int(per_run['main_win_flag'].fillna(False).map(bool).sum())}/{len(per_run)}"
            )
            lines.append(_table_to_markdown(
                per_run[
                    [
                        "run_id",
                        "selected_source_pair",
                        "actual_ratio_source_used",
                        "improvement_pct_overall",
                        "improvement_pct_zero",
                        "improvement_pct_low",
                        "improvement_pct_main",
                    ]
                ],
                max_rows=20,
            ))
            win_lines = _scoped_local_example_lines(scope_a_local, analyzer_id, win_flag=True, limit=3)
            loss_lines = _scoped_local_example_lines(scope_a_local, analyzer_id, win_flag=False, limit=3)
            lines.append("- 典型局部赢点：")
            if win_lines:
                for item in win_lines:
                    lines.append(f"  - {item}")
            else:
                lines.append("  - No local wins recorded.")
            lines.append("- 典型局部输点：")
            if loss_lines:
                for item in loss_lines:
                    lines.append(f"  - {item}")
            else:
                lines.append("  - No local losses recorded.")
            lines.append("")
    lines.append(f"- scoped_conclusion = {_historical_scoped_conclusion(scope_a_row)}")
    lines.append("")
    lines.append("## 3. Scope B: 2026-04-10 all-analyzers comparison")
    lines.append(f"- headline_safe_statement = {scope_b_row.get('headline_safe_statement', '')}")
    lines.append(f"- overall / zero / low / main = {_scoped_summary_readout(scope_b_row)}")
    lines.append("")
    lines.append(_table_to_markdown(scope_b["aggregate_segments"], max_rows=8))
    lines.append("")
    if scope_b_analyzers.empty:
        lines.append("_No Scope B analyzer rows._")
    else:
        for analyzer_row in scope_b_analyzers.itertuples(index=False):
            analyzer_id = str(analyzer_row.analyzer_id)
            per_run = scope_b_detail[scope_b_detail["analyzer_id"].astype(str) == analyzer_id].copy()
            lines.append(f"### {analyzer_id}")
            lines.append(
                f"- overall / zero / low / main improvement = "
                f"{_format_number(analyzer_row.improvement_pct_overall, 2)}% / "
                f"{_format_number(analyzer_row.improvement_pct_zero, 2)}% / "
                f"{_format_number(analyzer_row.improvement_pct_low, 2)}% / "
                f"{_format_number(analyzer_row.improvement_pct_main, 2)}%"
            )
            lines.append(_table_to_markdown(
                per_run[
                    [
                        "run_id",
                        "selected_source_pair",
                        "actual_ratio_source_used",
                        "improvement_pct_overall",
                        "improvement_pct_zero",
                        "improvement_pct_low",
                        "improvement_pct_main",
                    ]
                ],
                max_rows=8,
            ))
            win_lines = _scoped_local_example_lines(scope_b_local, analyzer_id, win_flag=True, limit=3)
            loss_lines = _scoped_local_example_lines(scope_b_local, analyzer_id, win_flag=False, limit=3)
            lines.append("- 典型局部赢点：")
            if win_lines:
                for item in win_lines:
                    lines.append(f"  - {item}")
            else:
                lines.append("  - No local wins recorded.")
            lines.append("- 典型局部输点：")
            if loss_lines:
                for item in loss_lines:
                    lines.append(f"  - {item}")
            else:
                lines.append("  - No local losses recorded.")
            lines.append("")
    lines.append(f"- scoped_conclusion = {_scope_b_scoped_conclusion(scope_b_row)}")
    lines.append("")
    lines.append("## 4. actual source used note")
    lines.append("- Scope A facts only:")
    for item in _scoped_source_note_lines(scope_a_detail):
        lines.append(f"  - {item}")
    lines.append("- Scope B facts only:")
    for item in _scoped_source_note_lines(scope_b_detail):
        lines.append(f"  - {item}")
    lines.append("")
    lines.append("## 5. final wording")
    lines.append(
        "- historical GA02/GA03 的可汇报结论："
        + _historical_scoped_conclusion(scope_a_row)
        + " "
        + _scoped_summary_readout(scope_a_row)
    )
    lines.append(
        "- 2026-04-10 全 analyzers 的可汇报结论："
        + _scope_b_scoped_conclusion(scope_b_row)
        + " "
        + _scoped_summary_readout(scope_b_row)
    )
    lines.append("- 这两个 scope 用途不同，不应混成单一全局 headline")
    lines.append("")
    return "\n".join(lines)


def _scope_yes_no_from_summary(summary_row: pd.Series, *, require_all_segments: bool) -> str:
    overall = pd.to_numeric(pd.Series([summary_row.get("overall_improvement_pct_scoped")]), errors="coerce").iloc[0]
    zero = pd.to_numeric(pd.Series([summary_row.get("zero_improvement_pct_scoped")]), errors="coerce").iloc[0]
    low = pd.to_numeric(pd.Series([summary_row.get("low_improvement_pct_scoped")]), errors="coerce").iloc[0]
    main = pd.to_numeric(pd.Series([summary_row.get("main_improvement_pct_scoped")]), errors="coerce").iloc[0]
    if require_all_segments:
        passed = all(pd.notna(value) and float(value) > 0.0 for value in (overall, zero, low, main))
    else:
        passed = pd.notna(overall) and float(overall) > 0.0
    return "是" if passed else "否"


def _scope_segment_readout(summary_row: pd.Series) -> str:
    return (
        f"overall={_format_number(summary_row.get('overall_improvement_pct_scoped'), 2)}%, "
        f"zero={_format_number(summary_row.get('zero_improvement_pct_scoped'), 2)}%, "
        f"low={_format_number(summary_row.get('low_improvement_pct_scoped'), 2)}%, "
        f"main={_format_number(summary_row.get('main_improvement_pct_scoped'), 2)}%"
    )


def _analyzer_segment_wins(scope_output: Mapping[str, object]) -> list[str]:
    analyzer_aggregate = scope_output.get("analyzer_aggregate", pd.DataFrame())
    if not isinstance(analyzer_aggregate, pd.DataFrame) or analyzer_aggregate.empty:
        return []
    segments = (
        ("overall", "overall_win_flag"),
        ("zero", "zero_win_flag"),
        ("low", "low_win_flag"),
        ("main", "main_win_flag"),
    )
    lines: list[str] = []
    for row in analyzer_aggregate.itertuples(index=False):
        won_segments = [label for label, column in segments if bool(getattr(row, column, False))]
        if won_segments:
            lines.append(f"{row.analyzer_id}: {', '.join(won_segments)}")
    return lines


def _top_local_point_lines(local_wins: pd.DataFrame, *, limit: int = 8) -> list[str]:
    if local_wins.empty:
        return []
    subset = local_wins[local_wins["local_win_flag"].fillna(False)].copy()
    if subset.empty:
        return []
    subset = subset.sort_values(
        ["improvement_abs_error", "run_id", "analyzer_id"],
        ascending=[False, True, True],
        ignore_index=True,
    ).head(limit)
    lines: list[str] = []
    for row in subset.itertuples(index=False):
        lines.append(
            f"{row.run_id}/{row.analyzer_id} @ {_format_number(row.temp_set_c, 1)}C / {_format_number(row.target_ppm, 1)} ppm: "
            f"old={_format_number(row.old_value)}, new={_format_number(row.new_value)}, "
            f"abs error improvement={_format_number(row.improvement_abs_error)} ppm ({row.segment_tag})"
        )
    return lines


def render_executive_summary_markdown(report: Mapping[str, object]) -> str:
    """Render the step-10d executive summary markdown."""

    scope_a = report["scope_a"]
    scope_b = report["scope_b"]
    scope_a_row = scope_a["summary"].iloc[0] if not scope_a["summary"].empty else pd.Series(dtype=object)
    scope_b_row = scope_b["summary"].iloc[0] if not scope_b["summary"].empty else pd.Series(dtype=object)

    lines: list[str] = []
    lines.append("# Executive Summary")
    lines.append("")
    lines.append("## 1. historical GA02/GA03")
    lines.append(
        "- historical GA02/GA03：新算法相对旧算法是否已有持续优势 = "
        + _scope_yes_no_from_summary(scope_a_row, require_all_segments=True)
    )
    lines.append("- scoped readout = " + _scope_segment_readout(scope_a_row))
    lines.append("")
    lines.append("## 2. 2026-04-10 all analyzers")
    lines.append(
        "- 2026-04-10 all analyzers：新算法相对旧算法是否已整体超过旧算法 = "
        + _scope_yes_no_from_summary(scope_b_row, require_all_segments=False)
    )
    lines.append("- scoped readout = " + _scope_segment_readout(scope_b_row))
    lines.append("")
    lines.append("## 3. Clear Wins")
    lines.append("- already clear analyzer / segment wins in historical GA02/GA03:")
    scope_a_wins = _analyzer_segment_wins(scope_a)
    if scope_a_wins:
        for item in scope_a_wins:
            lines.append(f"  - {item}")
    else:
        lines.append("  - none")
    lines.append("- already clear analyzer / segment wins in 2026-04-10 all analyzers:")
    scope_b_wins = _analyzer_segment_wins(scope_b)
    if scope_b_wins:
        for item in scope_b_wins:
            lines.append(f"  - {item}")
    else:
        lines.append("  - none")
    lines.append("- already clear local winning points:")
    point_lines = _top_local_point_lines(scope_a["local_wins"], limit=4) + _top_local_point_lines(scope_b["local_wins"], limit=4)
    if point_lines:
        for item in point_lines:
            lines.append(f"  - {item}")
    else:
        lines.append("  - none")
    lines.append("")
    lines.append("## 4. Official Benchmark")
    lines.append("- 当前项目的唯一正式 benchmark 是 old_chain。")
    lines.append("- 主 headline 不允许被 water replay / ppm family / source policy / GA01 sidecar 覆盖。")
    lines.append("")
    return "\n".join(lines)


def render_comparison_reconciliation_markdown(
    reconciliation: pd.DataFrame,
    *,
    scope_a: Mapping[str, object],
    scope_b: Mapping[str, object],
) -> str:
    """Render the step-10e comparison reconciliation markdown."""

    fixed_rule = "以后对外一律以 step_09c / step_09d / step_10c 的 scoped debugger 结果为准"
    scope_a_row = scope_a["summary"].iloc[0] if not scope_a["summary"].empty else pd.Series(dtype=object)
    scope_b_row = scope_b["summary"].iloc[0] if not scope_b["summary"].empty else pd.Series(dtype=object)

    lines: list[str] = []
    lines.append("# Comparison Reconciliation")
    lines.append("")
    lines.append("## 1. Why the two scopes cannot be merged into one headline")
    lines.append(
        "- Scope A is a historical-packages view that only includes GA02/GA03 and answers whether those representative analyzers show sustained performance."
    )
    lines.append(
        "- Scope B is the full run_20260410_132440 view and answers the real all-analyzers status inside that one complete run."
    )
    lines.append(
        f"- Scope A verdict = {scope_a_row.get('overall_verdict_scoped', '')}; Scope B verdict = {scope_b_row.get('overall_verdict_scoped', '')}."
    )
    lines.append("- Because the run set and analyzer inclusion scope are different, the two scopes cannot be merged into one global headline.")
    lines.append("")
    lines.append("## 2. Why a previous quick single-package calculation could point in the opposite direction")
    if not reconciliation.empty:
        for row in reconciliation.itertuples(index=False):
            lines.append(f"- {row.scope_label}: {row.why_single_package_quick_calc_can_flip_direction}")
    else:
        lines.append("- No reconciliation rows available.")
    lines.append("")
    lines.append("## 3. Reconciliation Checklist")
    lines.append(_table_to_markdown(reconciliation, max_rows=10))
    lines.append("")
    lines.append("## 4. Fixed Conclusion")
    lines.append(f"- {fixed_rule}")
    lines.append("")
    return "\n".join(lines)


def render_dual_surface_reconciliation_markdown(report: Mapping[str, object]) -> str:
    """Render the step-10f dual-surface reconciliation markdown."""

    summary = report["summary"]
    detail = report["detail"]
    aggregate_segments = report.get("aggregate_segments", pd.DataFrame())
    native_row = (
        summary[summary["comparison_surface"].astype(str) == "run_native_old_vs_new"].iloc[0]
        if not summary.empty and (summary["comparison_surface"].astype(str) == "run_native_old_vs_new").any()
        else pd.Series(dtype=object)
    )
    debugger_row = (
        summary[summary["comparison_surface"].astype(str) == "debugger_reconstructed_old_vs_new"].iloc[0]
        if not summary.empty and (summary["comparison_surface"].astype(str) == "debugger_reconstructed_old_vs_new").any()
        else pd.Series(dtype=object)
    )

    def _surface_readout(row: pd.Series) -> str:
        if row.empty:
            return "n/a"
        return (
            f"overall old={_format_number(row.get('overall_rmse_old'))}, new={_format_number(row.get('overall_rmse_new'))}; "
            f"zero old={_format_number(row.get('zero_rmse_old'))}, new={_format_number(row.get('zero_rmse_new'))}; "
            f"low old={_format_number(row.get('low_rmse_old'))}, new={_format_number(row.get('low_rmse_new'))}; "
            f"main old={_format_number(row.get('main_rmse_old'))}, new={_format_number(row.get('main_rmse_new'))}"
        )

    lines: list[str] = []
    lines.append("# Dual-Surface Reconciliation")
    lines.append("")
    lines.append("## 1. what quick calc actually used")
    lines.append("- old = old residual csv `prediction_simplified`")
    lines.append("- new = analyzer-sheet point-mean `二氧化碳浓度ppm`")
    lines.append("- surface = `run_native_old_vs_new`")
    lines.append(f"- readout = {_surface_readout(native_row)}")
    lines.append("")
    lines.append("## 2. what current debugger comparison actually used")
    lines.append("- old = old residual csv `prediction_simplified`")
    lines.append("- new = debugger reconstructed `selected_pred_ppm`")
    lines.append("- surface = `debugger_reconstructed_old_vs_new`")
    lines.append(
        "- selected_prediction_scope_majority = "
        + str(debugger_row.get("selected_prediction_scope_majority", "unknown"))
    )
    lines.append(f"- readout = {_surface_readout(debugger_row)}")
    lines.append("")
    lines.append("## 3. whether there is evidence of an old/new flip bug")
    lines.append("- no evidence of old/new flip bug")
    lines.append(
        "- old stays on the old residual source, and the only thing that changes between the two surfaces is which new value definition is used."
    )
    lines.append("")
    lines.append("## 4. which result should be used for")
    lines.append("- run 原生新旧算法对比: use `run_native_old_vs_new`")
    lines.append("- debugger 离线重建验证: use `debugger_reconstructed_old_vs_new`")
    lines.append("")
    lines.append("## 5. side-by-side surface table")
    lines.append(_table_to_markdown(summary, max_rows=10))
    lines.append("")
    lines.append("## 6. segment table")
    lines.append(_table_to_markdown(aggregate_segments, max_rows=12))
    lines.append("")
    lines.append("## 7. analyzer detail")
    lines.append(_table_to_markdown(detail, max_rows=20))
    lines.append("")
    lines.append("## 8. final wording")
    lines.append(
        "- 快算为什么错：快算把 `old residual csv prediction_simplified` 对到了 `analyzer_sheet_mean_co2_ppm` 这套 run-native 新值，"
        "而 step_09/step_10 对的是 `selected_pred_ppm_from_debugger` 这套 debugger reconstructed 新值，所以两个方向不一定相同。"
    )
    lines.append(
        "- 程序哪里不是 bug，而是口径不同：当前程序没有把 old/new 翻转，差异主要来自 new 的定义不同。"
        "run-native surface 回答的是运行包原生输出，debugger surface 回答的是离线重建验证。"
    )
    lines.append("")
    return "\n".join(lines)


def render_report_html(report: Mapping[str, object]) -> str:
    """Render a compact standalone HTML report."""

    md = render_report_markdown(report)
    paragraphs = []
    for block in md.split("\n\n"):
        escaped = html.escape(block)
        if block.startswith("#"):
            level = len(block.split(" ", 1)[0])
            text = html.escape(block[level + 1 :])
            paragraphs.append(f"<h{level}>{text}</h{level}>")
        else:
            paragraphs.append(f"<pre>{escaped}</pre>")
    body = "\n".join(paragraphs)
    return (
        "<!doctype html><html><head><meta charset='utf-8'>"
        "<title>Offline Absorbance Debug Report</title>"
        "<style>body{font-family:Segoe UI,Arial,sans-serif;margin:24px;line-height:1.45;}"
        "pre{white-space:pre-wrap;background:#f7f7f7;padding:12px;border-radius:8px;}"
        "h1,h2,h3{color:#14324a;} table{border-collapse:collapse;} </style>"
        "</head><body>"
        f"{body}"
        "</body></html>"
    )


def write_workbook(path: Path, sheets: Mapping[str, pd.DataFrame]) -> None:
    """Write multiple dataframes into one Excel workbook."""

    path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        for name, frame in sheets.items():
            safe_name = str(name)[:31] or "Sheet1"
            frame.to_excel(writer, sheet_name=safe_name, index=False)
