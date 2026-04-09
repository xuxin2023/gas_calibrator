from __future__ import annotations

from pathlib import Path
from typing import Any


KNOWN_ARTIFACT_ROLES = frozenset(
    {
        "execution_rows",
        "execution_summary",
        "diagnostic_analysis",
        "formal_analysis",
        "unclassified",
    }
)

DEFAULT_ROLE_CATALOG: dict[str, list[str]] = {
    "execution_rows": [
        "runtime_points",
        "io_log",
        "samples_csv",
        "samples_excel",
        "results_json",
        "point_summaries",
    ],
    "execution_summary": [
        "manifest",
        "run_summary",
        "points_readable",
        "acceptance_plan",
        "lineage_summary",
        "evidence_registry",
        "simulation_evidence_sidecar_bundle",
        "measurement_phase_coverage_report",
        "scope_definition_pack",
        "decision_rule_profile",
        "reference_asset_registry",
        "metrology_traceability_stub",
        "uncertainty_budget_stub",
        "method_confirmation_protocol",
        "method_confirmation_matrix",
        "software_validation_traceability_matrix",
        "release_validation_manifest",
        "suite_summary",
        "suite_summary_markdown",
        "suite_acceptance_plan",
        "suite_evidence_registry",
        "stage_admission_review_pack",
        "engineering_isolation_admission_checklist",
        "stage3_real_validation_plan",
        "stage3_standards_alignment_matrix",
    ],
    "diagnostic_analysis": [
        "qc_report",
        "qc_summary",
        "qc_manifest",
        "qc_reviewer_digest",
        "temperature_snapshots",
        "analytics_summary",
        "spectral_quality_summary",
        "trend_registry",
        "multi_source_stability_evidence",
        "multi_source_stability_evidence_markdown",
        "state_transition_evidence",
        "state_transition_evidence_markdown",
        "measurement_phase_coverage_report",
        "measurement_phase_coverage_report_markdown",
        "scope_readiness_summary",
        "scope_readiness_summary_markdown",
        "certificate_readiness_summary",
        "certificate_readiness_summary_markdown",
        "uncertainty_method_readiness_summary",
        "uncertainty_method_readiness_summary_markdown",
        "audit_readiness_digest",
        "audit_readiness_digest_markdown",
        "suite_analytics_summary",
        "summary_parity_report",
        "summary_parity_report_markdown",
        "export_resilience_report",
        "export_resilience_report_markdown",
        "workbench_action_report_json",
        "workbench_action_report_markdown",
        "workbench_action_snapshot",
        "room_temp_diagnostic_summary",
        "room_temp_diagnostic_report",
        "room_temp_diagnostic_workbook",
        "room_temp_diagnostic_plot",
        "analyzer_chain_isolation_comparison",
        "analyzer_chain_isolation_rollup",
        "analyzer_chain_diagnostic_report",
        "analyzer_chain_diagnostic_workbook",
        "analyzer_chain_diagnostic_plot",
        "analyzer_chain_compare_vs_8ch_report",
        "analyzer_chain_compare_vs_baseline_report",
        "analyzer_chain_operator_checklist",
    ],
    "formal_analysis": [
        "coefficient_report",
        "coefficient_registry",
        "phase_transition_bridge_reviewer_artifact",
        "stage_admission_review_pack_reviewer_artifact",
        "engineering_isolation_admission_checklist_reviewer_artifact",
        "stage3_real_validation_plan_reviewer_artifact",
        "stage3_standards_alignment_matrix_reviewer_artifact",
        "scope_definition_pack_markdown",
        "decision_rule_profile_markdown",
        "reference_asset_registry_markdown",
        "metrology_traceability_stub_markdown",
        "uncertainty_budget_stub_markdown",
        "method_confirmation_protocol_markdown",
        "method_confirmation_matrix_markdown",
        "software_validation_traceability_matrix_markdown",
        "release_validation_manifest_markdown",
        "scope_readiness_summary_markdown",
        "certificate_readiness_summary_markdown",
        "uncertainty_method_readiness_summary_markdown",
        "audit_readiness_digest_markdown",
    ],
}

KNOWN_ARTIFACT_KEYS_BY_FILENAME: dict[str, str] = {
    "summary.json": "run_summary",
    "manifest.json": "manifest",
    "results.json": "results_json",
    "point_summaries.json": "point_summaries",
    "points.csv": "runtime_points",
    "points_readable.csv": "points_readable",
    "io_log.csv": "io_log",
    "samples.csv": "samples_csv",
    "samples.xlsx": "samples_excel",
    "acceptance_plan.json": "acceptance_plan",
    "analytics_summary.json": "analytics_summary",
    "spectral_quality_summary.json": "spectral_quality_summary",
    "trend_registry.json": "trend_registry",
    "lineage_summary.json": "lineage_summary",
    "evidence_registry.json": "evidence_registry",
    "coefficient_registry.json": "coefficient_registry",
    "suite_summary.json": "suite_summary",
    "suite_summary.md": "suite_summary_markdown",
    "suite_analytics_summary.json": "suite_analytics_summary",
    "suite_acceptance_plan.json": "suite_acceptance_plan",
    "suite_evidence_registry.json": "suite_evidence_registry",
    "summary_parity_report.json": "summary_parity_report",
    "summary_parity_report.md": "summary_parity_report_markdown",
    "export_resilience_report.json": "export_resilience_report",
    "export_resilience_report.md": "export_resilience_report_markdown",
    "qc_report.json": "qc_report",
    "qc_report.csv": "qc_report",
    "qc_summary.json": "qc_summary",
    "qc_manifest.json": "qc_manifest",
    "qc_reviewer_digest.md": "qc_reviewer_digest",
    "temperature_snapshots.json": "temperature_snapshots",
    "calibration_coefficients.xlsx": "coefficient_report",
    "phase_transition_bridge_reviewer.md": "phase_transition_bridge_reviewer_artifact",
    "stage_admission_review_pack.json": "stage_admission_review_pack",
    "stage_admission_review_pack.md": "stage_admission_review_pack_reviewer_artifact",
    "engineering_isolation_admission_checklist.json": "engineering_isolation_admission_checklist",
    "engineering_isolation_admission_checklist.md": "engineering_isolation_admission_checklist_reviewer_artifact",
    "stage3_real_validation_plan.json": "stage3_real_validation_plan",
    "stage3_real_validation_plan.md": "stage3_real_validation_plan_reviewer_artifact",
    "stage3_standards_alignment_matrix.json": "stage3_standards_alignment_matrix",
    "stage3_standards_alignment_matrix.md": "stage3_standards_alignment_matrix_reviewer_artifact",
    "multi_source_stability_evidence.json": "multi_source_stability_evidence",
    "multi_source_stability_evidence.md": "multi_source_stability_evidence_markdown",
    "state_transition_evidence.json": "state_transition_evidence",
    "state_transition_evidence.md": "state_transition_evidence_markdown",
    "simulation_evidence_sidecar_bundle.json": "simulation_evidence_sidecar_bundle",
    "measurement_phase_coverage_report.json": "measurement_phase_coverage_report",
    "measurement_phase_coverage_report.md": "measurement_phase_coverage_report_markdown",
    "scope_definition_pack.json": "scope_definition_pack",
    "scope_definition_pack.md": "scope_definition_pack_markdown",
    "decision_rule_profile.json": "decision_rule_profile",
    "decision_rule_profile.md": "decision_rule_profile_markdown",
    "scope_readiness_summary.json": "scope_readiness_summary",
    "scope_readiness_summary.md": "scope_readiness_summary_markdown",
    "reference_asset_registry.json": "reference_asset_registry",
    "reference_asset_registry.md": "reference_asset_registry_markdown",
    "certificate_readiness_summary.json": "certificate_readiness_summary",
    "certificate_readiness_summary.md": "certificate_readiness_summary_markdown",
    "metrology_traceability_stub.json": "metrology_traceability_stub",
    "metrology_traceability_stub.md": "metrology_traceability_stub_markdown",
    "uncertainty_budget_stub.json": "uncertainty_budget_stub",
    "uncertainty_budget_stub.md": "uncertainty_budget_stub_markdown",
    "method_confirmation_protocol.json": "method_confirmation_protocol",
    "method_confirmation_protocol.md": "method_confirmation_protocol_markdown",
    "method_confirmation_matrix.json": "method_confirmation_matrix",
    "method_confirmation_matrix.md": "method_confirmation_matrix_markdown",
    "uncertainty_method_readiness_summary.json": "uncertainty_method_readiness_summary",
    "uncertainty_method_readiness_summary.md": "uncertainty_method_readiness_summary_markdown",
    "software_validation_traceability_matrix.json": "software_validation_traceability_matrix",
    "software_validation_traceability_matrix.md": "software_validation_traceability_matrix_markdown",
    "release_validation_manifest.json": "release_validation_manifest",
    "release_validation_manifest.md": "release_validation_manifest_markdown",
    "audit_readiness_digest.json": "audit_readiness_digest",
    "audit_readiness_digest.md": "audit_readiness_digest_markdown",
    "workbench_action_report.json": "workbench_action_report_json",
    "workbench_action_report.md": "workbench_action_report_markdown",
    "workbench_action_snapshot.json": "workbench_action_snapshot",
    "ai_run_summary.md": "ai_run_summary_markdown",
    "run_summary.txt": "run_summary_text",
    "route_trace.jsonl": "route_trace",
    "run.log": "run_log",
    "samples_runtime.csv": "samples_runtime",
}

KNOWN_REPORT_ARTIFACTS = [
    "summary.json",
    "manifest.json",
    "results.json",
    "point_summaries.json",
    "points.csv",
    "points_readable.csv",
    "io_log.csv",
    "samples.csv",
    "samples.xlsx",
    "acceptance_plan.json",
    "analytics_summary.json",
    "trend_registry.json",
    "lineage_summary.json",
    "evidence_registry.json",
    "coefficient_registry.json",
    "suite_summary.json",
    "suite_summary.md",
    "suite_analytics_summary.json",
    "suite_acceptance_plan.json",
    "suite_evidence_registry.json",
    "summary_parity_report.json",
    "summary_parity_report.md",
    "export_resilience_report.json",
    "export_resilience_report.md",
    "qc_report.json",
    "qc_report.csv",
    "qc_summary.json",
    "qc_manifest.json",
    "qc_reviewer_digest.md",
    "temperature_snapshots.json",
    "ai_run_summary.md",
    "run_summary.txt",
    "calibration_coefficients.xlsx",
    "phase_transition_bridge_reviewer.md",
    "stage_admission_review_pack.json",
    "stage_admission_review_pack.md",
    "engineering_isolation_admission_checklist.json",
    "engineering_isolation_admission_checklist.md",
    "stage3_real_validation_plan.json",
    "stage3_real_validation_plan.md",
    "stage3_standards_alignment_matrix.json",
    "stage3_standards_alignment_matrix.md",
    "multi_source_stability_evidence.json",
    "multi_source_stability_evidence.md",
    "state_transition_evidence.json",
    "state_transition_evidence.md",
    "simulation_evidence_sidecar_bundle.json",
    "measurement_phase_coverage_report.json",
    "measurement_phase_coverage_report.md",
    "scope_definition_pack.json",
    "scope_definition_pack.md",
    "decision_rule_profile.json",
    "decision_rule_profile.md",
    "scope_readiness_summary.json",
    "scope_readiness_summary.md",
    "reference_asset_registry.json",
    "reference_asset_registry.md",
    "certificate_readiness_summary.json",
    "certificate_readiness_summary.md",
    "metrology_traceability_stub.json",
    "metrology_traceability_stub.md",
    "uncertainty_budget_stub.json",
    "uncertainty_budget_stub.md",
    "method_confirmation_protocol.json",
    "method_confirmation_protocol.md",
    "method_confirmation_matrix.json",
    "method_confirmation_matrix.md",
    "uncertainty_method_readiness_summary.json",
    "uncertainty_method_readiness_summary.md",
    "software_validation_traceability_matrix.json",
    "software_validation_traceability_matrix.md",
    "release_validation_manifest.json",
    "release_validation_manifest.md",
    "audit_readiness_digest.json",
    "audit_readiness_digest.md",
    "workbench_action_report.json",
    "workbench_action_report.md",
    "workbench_action_snapshot.json",
]


def normalize_artifact_role(value: Any) -> str:
    role = str(value or "").strip().lower()
    return role if role in KNOWN_ARTIFACT_ROLES else "unclassified"


def build_default_role_catalog() -> dict[str, list[str]]:
    return {
        str(role): [str(item) for item in list(items or []) if str(item or "").strip()]
        for role, items in DEFAULT_ROLE_CATALOG.items()
    }


def merge_role_catalog(role_catalog: dict[str, Any] | None = None) -> dict[str, list[str]]:
    merged = build_default_role_catalog()
    seen_by_role: dict[str, set[str]] = {
        role: {str(item).strip() for item in items if str(item).strip()}
        for role, items in merged.items()
    }
    for role, items in dict(role_catalog or {}).items():
        normalized_role = normalize_artifact_role(role)
        if normalized_role == "unclassified" or normalized_role not in merged:
            continue
        role_items = items if isinstance(items, (list, tuple, set)) else [items]
        for item in role_items:
            key = str(item or "").strip()
            if not key or key in seen_by_role[normalized_role]:
                continue
            merged[normalized_role].append(key)
            seen_by_role[normalized_role].add(key)
    return merged


def build_role_by_key(role_catalog: dict[str, Any] | None = None) -> dict[str, str]:
    merged: dict[str, str] = {}
    for role, items in merge_role_catalog(role_catalog).items():
        normalized_role = normalize_artifact_role(role)
        for item in list(items or []):
            key = str(item or "").strip()
            if key:
                merged.setdefault(key, normalized_role)
    return merged


def infer_artifact_identity(
    path_or_name: Any,
    *,
    role_catalog: dict[str, Any] | None = None,
) -> dict[str, str]:
    text = str(path_or_name or "").strip()
    path = Path(text) if text else Path()
    filename = path.name.lower()
    artifact_key = _infer_diagnostic_artifact_key(path, text=text)
    if not artifact_key:
        artifact_key = KNOWN_ARTIFACT_KEYS_BY_FILENAME.get(filename, "")
    artifact_role = normalize_artifact_role(build_role_by_key(role_catalog).get(artifact_key))
    return {
        "artifact_key": artifact_key,
        "artifact_role": artifact_role,
    }


def _infer_diagnostic_artifact_key(path: Path, *, text: str) -> str:
    filename = path.name.lower()
    bundle_kind = _diagnostic_bundle_kind(path, text=text)
    if bundle_kind == "room_temp":
        if filename == "diagnostic_summary.json":
            return "room_temp_diagnostic_summary"
        if filename == "readable_report.md":
            return "room_temp_diagnostic_report"
        if filename == "diagnostic_workbook.xlsx":
            return "room_temp_diagnostic_workbook"
        if filename.endswith(".png"):
            return "room_temp_diagnostic_plot"
    if bundle_kind == "analyzer_chain":
        if filename == "isolation_comparison_summary.json":
            return "analyzer_chain_isolation_comparison"
        if filename == "summary.json":
            return "analyzer_chain_isolation_rollup"
        if filename == "readable_report.md":
            return "analyzer_chain_diagnostic_report"
        if filename == "diagnostic_workbook.xlsx":
            return "analyzer_chain_diagnostic_workbook"
        if filename == "compare_vs_8ch.md":
            return "analyzer_chain_compare_vs_8ch_report"
        if filename == "compare_vs_baseline.md":
            return "analyzer_chain_compare_vs_baseline_report"
        if filename == "operator_checklist.md":
            return "analyzer_chain_operator_checklist"
        if filename.endswith(".png"):
            return "analyzer_chain_diagnostic_plot"
    if filename == "diagnostic_summary.json":
        return "room_temp_diagnostic_summary"
    if filename == "isolation_comparison_summary.json":
        return "analyzer_chain_isolation_comparison"
    if filename == "compare_vs_8ch.md":
        return "analyzer_chain_compare_vs_8ch_report"
    if filename == "compare_vs_baseline.md":
        return "analyzer_chain_compare_vs_baseline_report"
    if filename == "operator_checklist.md":
        return "analyzer_chain_operator_checklist"
    return ""


def _diagnostic_bundle_kind(path: Path, *, text: str) -> str:
    normalized = text.replace("\\", "/").strip().lower()
    filename = path.name.lower()
    parent = path.parent if str(path) else Path()
    try:
        if parent and (parent / "diagnostic_summary.json").exists():
            return "room_temp"
        if parent and (parent / "isolation_comparison_summary.json").exists():
            return "analyzer_chain"
    except Exception:
        pass
    if "analyzer_chain" in normalized or "chain_isolation" in normalized:
        return "analyzer_chain"
    if "room_temp" in normalized or "pressure_diagnostic" in normalized:
        return "room_temp"
    if filename == "diagnostic_summary.json":
        return "room_temp"
    if filename == "isolation_comparison_summary.json":
        return "analyzer_chain"
    return ""
