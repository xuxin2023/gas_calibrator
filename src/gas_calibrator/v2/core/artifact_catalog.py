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
        "suite_summary",
        "suite_summary_markdown",
        "suite_acceptance_plan",
        "suite_evidence_registry",
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
        "suite_analytics_summary",
        "summary_parity_report",
        "summary_parity_report_markdown",
        "export_resilience_report",
        "export_resilience_report_markdown",
        "workbench_action_report_json",
        "workbench_action_report_markdown",
        "workbench_action_snapshot",
    ],
    "formal_analysis": [
        "coefficient_report",
        "coefficient_registry",
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
    filename = Path(text).name.lower()
    artifact_key = KNOWN_ARTIFACT_KEYS_BY_FILENAME.get(filename, "")
    artifact_role = normalize_artifact_role(build_role_by_key(role_catalog).get(artifact_key))
    return {
        "artifact_key": artifact_key,
        "artifact_role": artifact_role,
    }
