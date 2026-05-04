from __future__ import annotations

import hashlib
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .golden_dataset_registry import (
    GOLDEN_DATASET_REGISTRY_FILENAME,
    build_golden_dataset_registry,
    write_golden_dataset_registry,
)


REGRESSION_SCOREBOARD_SCHEMA_VERSION = "regression-scoreboard-v1"
REGRESSION_SCOREBOARD_FILENAME = "regression_scoreboard.json"
REGRESSION_SCOREBOARD_MARKDOWN_FILENAME = "regression_scoreboard.md"
BUNDLE_DIFF_SUMMARY_FILENAME = "bundle_diff_summary.json"
ARTIFACT_SCHEMA_DIFF_FILENAME = "artifact_schema_diff.json"

_BOUNDARY_FIELDS: dict[str, Any] = {
    "evidence_source": "simulated",
    "reviewer_only": True,
    "readiness_mapping_only": True,
    "not_real_acceptance_evidence": True,
    "not_formal_metrology_conclusion": True,
}

_SURFACE_SPECS: tuple[dict[str, Any], ...] = (
    {
        "surface_id": "parser_results_reports",
        "title": "Parser / Results / Reports",
        "required_all": [
            "summary.json",
            "manifest.json",
            "results.json",
            "acceptance_plan.json",
            "analytics_summary.json",
        ],
        "required_any": ["suite_summary.json", "run_artifact_index.json"],
        "status_files": [
            "suite_summary.json",
            "summary_parity_report.json",
            "export_resilience_report.json",
            "analytics_summary.json",
        ],
    },
    {
        "surface_id": "review_center_surface",
        "title": "Review Center Surface",
        "required_all": [
            "step2_closeout_bundle.json",
            "step2_closeout_digest.json",
            "evidence_coverage_matrix.json",
            "result_traceability_tree.json",
            "reviewer_anchor_navigation.json",
            "stage_admission_review_pack.json",
            "engineering_isolation_gate_result.json",
        ],
        "required_any": ["step2_closeout_evidence_index.json", "stage3_standards_alignment_matrix.json"],
        "status_files": [],
        "validator": "review_center_surface",
    },
    {
        "surface_id": "workbench_evidence_surface",
        "title": "Workbench Evidence Surface",
        "required_all": [
            "multi_source_stability_evidence.json",
            "state_transition_evidence.json",
            "measurement_phase_coverage_report.json",
            "step2_closeout_bundle.json",
            "engineering_isolation_gate_result.json",
            "result_traceability_tree.json",
            "run_metadata_profile.json",
            "ai_run_summary.md",
        ],
        "required_any": [
            "simulation_evidence_sidecar_bundle.json",
            "compatibility_scan_summary.json",
        ],
        "status_files": [],
        "validator": "workbench_evidence_surface",
    },
    {
        "surface_id": "scope_decision_surface",
        "title": "Scope / Decision Surface",
        "required_all": [
            "scope_definition_pack.json",
            "decision_rule_profile.json",
            "scope_readiness_summary.json",
            "reference_asset_registry.json",
            "certificate_lifecycle_summary.json",
            "pre_run_readiness_gate.json",
            "step2_closeout_bundle.json",
            "engineering_isolation_gate_result.json",
        ],
        "required_any": [],
        "status_files": [],
        "validator": "scope_decision_surface",
    },
    {
        "surface_id": "uncertainty_method_surface",
        "title": "Uncertainty / Method Surface",
        "required_all": [
            "uncertainty_report_pack.json",
            "uncertainty_rollup.json",
            "uncertainty_method_readiness_summary.json",
            "method_confirmation_protocol.json",
            "verification_rollup.json",
            "step2_closeout_bundle.json",
        ],
        "required_any": [
            "uncertainty_golden_cases.json",
            "route_specific_validation_matrix.json",
        ],
        "status_files": [],
        "validator": "uncertainty_method_surface",
    },
    {
        "surface_id": "software_validation_comparison_surface",
        "title": "Software Validation / Comparison Linkage",
        "required_all": [
            "software_validation_traceability_matrix.json",
            "requirement_design_code_test_links.json",
            "validation_evidence_index.json",
            "comparison_evidence_pack.json",
            "scope_comparison_view.json",
            "comparison_digest.json",
            "comparison_rollup.json",
        ],
        "required_any": [
            "release_manifest.json",
            "audit_readiness_digest.json",
        ],
        "status_files": [],
    },
    {
        "surface_id": "sidecar_analytics_ai_surface",
        "title": "Sidecar Analytics / AI Surface",
        "required_all": ["ai_run_summary.md"],
        "required_any": [
            "spectral_quality_summary.json",
            "trend_registry.json",
            "sidecar_index_summary.json",
            "review_copilot_payload.json",
            "model_governance_summary.json",
        ],
        "status_files": [],
        "validator": "sidecar_analytics_ai_surface",
    },
)


def generate_regression_scoreboard(
    *,
    current_bundle_dir: str | Path,
    baseline_bundle_dir: str | Path | None = None,
    output_dir: str | Path,
    current_label: str = "current_branch_result",
    baseline_label: str = "previous_baseline",
    golden_registry: dict[str, Any] | None = None,
) -> dict[str, Any]:
    current_dir = Path(current_bundle_dir).resolve()
    baseline_dir = Path(baseline_bundle_dir).resolve() if baseline_bundle_dir else None
    target_dir = Path(output_dir).resolve()
    target_dir.mkdir(parents=True, exist_ok=True)

    registry = dict(golden_registry or build_golden_dataset_registry())
    golden_registry_path = write_golden_dataset_registry(target_dir, registry)

    current_files = _scan_bundle_dir(current_dir)
    baseline_files = _scan_bundle_dir(baseline_dir) if baseline_dir else {}
    current_context = _collect_bundle_context(current_files)
    current_validation = _extract_validation_counts(current_files)
    current_surfaces = _evaluate_surfaces(current_files, context=current_context)
    bundle_diff_summary = _build_bundle_diff_summary(
        current_files=current_files,
        baseline_files=baseline_files,
        current_label=current_label,
        baseline_label=baseline_label,
        current_dir=current_dir,
        baseline_dir=baseline_dir,
    )
    artifact_schema_diff = _build_artifact_schema_diff(
        current_files=current_files,
        baseline_files=baseline_files,
        current_label=current_label,
        baseline_label=baseline_label,
    )
    artifact_regressions = _build_artifact_regressions(
        current_surfaces=current_surfaces,
        baseline_surfaces=_surface_index(
            _evaluate_surfaces(
                baseline_files,
                context=_collect_bundle_context(baseline_files),
            )
        )
        if baseline_files
        else {},
        bundle_diff_summary=bundle_diff_summary,
        artifact_schema_diff=artifact_schema_diff,
    )
    missing_surfaces = [surface["surface_id"] for surface in current_surfaces if surface["status"] == "missing"]
    degraded_areas = [
        {
            "surface_id": surface["surface_id"],
            "title": surface["title"],
            "reason": surface["reason"],
        }
        for surface in current_surfaces
        if surface["status"] == "degraded"
    ]
    changed_schema_fields = list(artifact_schema_diff.get("changed_schema_fields") or [])
    surface_counter = Counter(str(surface["status"]) for surface in current_surfaces)
    executed_case_names = current_validation["case_names"]
    golden_coverage = _build_golden_coverage(registry=registry, executed_case_names=executed_case_names)
    remaining_blockers = list(current_context.get("remaining_blockers") or [])
    remaining_warnings = list(current_context.get("remaining_warnings") or [])
    remaining_formal_gaps = list(current_context.get("remaining_formal_gaps") or [])
    remaining_gaps = list(current_context.get("remaining_gaps") or [])
    recommendation = _recommend_bundle(
        current_label=current_label,
        baseline_label=baseline_label,
        has_baseline=baseline_dir is not None,
        validation=current_validation,
        current_surfaces=current_surfaces,
        artifact_regressions=artifact_regressions,
        remaining_blockers=remaining_blockers,
        remaining_warnings=remaining_warnings,
        remaining_formal_gaps=remaining_formal_gaps,
    )

    scoreboard = {
        "schema_version": REGRESSION_SCOREBOARD_SCHEMA_VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "current_bundle": {
            "label": current_label,
            "bundle_dir": str(current_dir),
            "file_count": len(current_files),
        },
        "baseline_bundle": {
            "label": baseline_label,
            "bundle_dir": str(baseline_dir) if baseline_dir else "",
            "file_count": len(baseline_files),
            "available": baseline_dir is not None,
        },
        "validation_counts": {
            "total": current_validation["total"],
            "passed": current_validation["passed"],
            "failed": current_validation["failed"],
            "skipped": current_validation["skipped"],
            "degraded": current_validation["degraded"],
        },
        "surface_counts": {
            "total": len(current_surfaces),
            "passed": surface_counter.get("pass", 0),
            "degraded": surface_counter.get("degraded", 0),
            "missing": surface_counter.get("missing", 0),
        },
        "degraded_areas": degraded_areas,
        "artifact_regressions": artifact_regressions,
        "missing_surfaces": missing_surfaces,
        "changed_schema_fields": changed_schema_fields,
        "golden_registry_summary": dict(registry.get("summary") or {}),
        "golden_registry_coverage": golden_coverage,
        "surface_results": current_surfaces,
        "scope_id": str(current_context.get("scope_id") or ""),
        "decision_rule_id": str(current_context.get("decision_rule_id") or ""),
        "limitation_note": str(current_context.get("limitation_note") or ""),
        "non_claim_note": str(current_context.get("non_claim_note") or ""),
        "uncertainty_case_id": str(current_context.get("uncertainty_case_id") or ""),
        "method_confirmation_protocol_id": str(current_context.get("method_confirmation_protocol_id") or ""),
        "verification_rollup_id": str(current_context.get("verification_rollup_id") or ""),
        "engineering_isolation_gate_level": str(current_context.get("engineering_isolation_gate_level") or ""),
        "remaining_blockers": remaining_blockers,
        "remaining_warnings": remaining_warnings,
        "remaining_formal_gaps": remaining_formal_gaps,
        "remaining_gaps": remaining_gaps,
        "bundle_diff_summary_path": str(target_dir / BUNDLE_DIFF_SUMMARY_FILENAME),
        "artifact_schema_diff_path": str(target_dir / ARTIFACT_SCHEMA_DIFF_FILENAME),
        "golden_dataset_registry_path": str(golden_registry_path),
        "recommendation": recommendation,
        **_BOUNDARY_FIELDS,
    }

    _write_json(target_dir / REGRESSION_SCOREBOARD_FILENAME, scoreboard)
    _write_json(target_dir / BUNDLE_DIFF_SUMMARY_FILENAME, bundle_diff_summary)
    _write_json(target_dir / ARTIFACT_SCHEMA_DIFF_FILENAME, artifact_schema_diff)
    (target_dir / REGRESSION_SCOREBOARD_MARKDOWN_FILENAME).write_text(
        _render_scoreboard_markdown(scoreboard),
        encoding="utf-8",
    )
    return scoreboard


def _scan_bundle_dir(bundle_dir: Path | None) -> dict[str, dict[str, Any]]:
    if bundle_dir is None or not bundle_dir.exists():
        return {}
    scanned: dict[str, dict[str, Any]] = {}
    for path in sorted(item for item in bundle_dir.rglob("*") if item.is_file()):
        relative_path = path.relative_to(bundle_dir).as_posix()
        scanned[relative_path] = _file_info(path=path, bundle_dir=bundle_dir)
    return scanned


def _file_info(*, path: Path, bundle_dir: Path) -> dict[str, Any]:
    payload: dict[str, Any] | None = None
    kind = "binary"
    if path.suffix.lower() in {".json", ".md", ".txt", ".csv"}:
        kind = "text"
    if path.suffix.lower() == ".json":
        payload = _load_json(path)
        kind = "json"
    content = path.read_bytes()
    return {
        "relative_path": path.relative_to(bundle_dir).as_posix(),
        "path": str(path),
        "filename": path.name,
        "size": len(content),
        "sha1": hashlib.sha1(content).hexdigest(),
        "kind": kind,
        "payload": payload if isinstance(payload, dict) else None,
    }


def _extract_validation_counts(files: dict[str, dict[str, Any]]) -> dict[str, Any]:
    default_payload = {
        "total": 0,
        "passed": 0,
        "failed": 0,
        "skipped": 0,
        "degraded": 0,
        "case_names": [],
    }
    suite_payload = _first_payload(files, "suite_summary.json")
    if not suite_payload:
        return default_payload
    counts = dict(suite_payload.get("counts") or {})
    cases = [dict(item) for item in list(suite_payload.get("cases") or []) if isinstance(item, dict)]
    passed = int(counts.get("passed") or 0)
    failed = int(counts.get("failed") or 0)
    total = int(counts.get("total") or len(cases))
    degraded = sum(1 for case in cases if not bool(case.get("ok", False)) and str(case.get("status") or "").upper() in {"MISMATCH", "SNAPSHOT_ONLY"})
    skipped = sum(
        1
        for case in cases
        if str(case.get("status") or "").upper() in {"NOT_EXECUTED", "SNAPSHOT_ONLY"}
    )
    return {
        "total": total,
        "passed": passed,
        "failed": failed,
        "skipped": skipped,
        "degraded": degraded,
        "case_names": [str(case.get("name") or "") for case in cases if str(case.get("name") or "").strip()],
    }


def _evaluate_surfaces(
    files: dict[str, dict[str, Any]],
    *,
    context: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    bundle_context = dict(context or _collect_bundle_context(files))
    results: list[dict[str, Any]] = []
    for spec in _SURFACE_SPECS:
        required_all = list(spec.get("required_all") or [])
        required_any = list(spec.get("required_any") or [])
        missing_required = [filename for filename in required_all if filename not in files]
        present_required_any = [filename for filename in required_any if filename in files]
        present_files = [filename for filename in required_all + required_any if filename in files]
        status_level = "pass"
        reasons: list[str] = []

        if missing_required and present_files:
            status_level = "degraded"
            reasons.append("required artifacts missing")
        elif missing_required and not present_files:
            status_level = "missing"
            reasons.append("surface absent")
        elif required_any and not present_required_any:
            status_level = "degraded"
            reasons.append("expected optional surface entry missing")

        boundary_issues = _surface_boundary_issues(files, present_files)
        if boundary_issues and status_level == "pass":
            status_level = "degraded"
        reasons.extend(boundary_issues)

        status_issues = _surface_status_issues(
            files=files,
            filenames=list(spec.get("status_files") or []),
        )
        if status_issues and status_level == "pass":
            status_level = "degraded"
        reasons.extend(status_issues)
        validator_issues = _surface_validator_issues(
            validator_name=str(spec.get("validator") or "").strip(),
            files=files,
            context=bundle_context,
        )
        if validator_issues and status_level == "pass":
            status_level = "degraded"
        reasons.extend(validator_issues)

        results.append(
            {
                "surface_id": str(spec.get("surface_id") or ""),
                "title": str(spec.get("title") or spec.get("surface_id") or ""),
                "status": status_level,
                "present_files": sorted(present_files),
                "missing_required_files": missing_required,
                "present_required_any_files": sorted(present_required_any),
                "reason": "; ".join(dict.fromkeys(reasons)) if reasons else "surface complete",
            }
        )
    return results


def _collect_bundle_context(files: dict[str, dict[str, Any]]) -> dict[str, Any]:
    summary_payload = _first_payload(files, "summary.json")
    scope_payload = _first_payload(files, "scope_definition_pack.json")
    decision_payload = _first_payload(files, "decision_rule_profile.json")
    closeout_bundle = _first_payload(files, "step2_closeout_bundle.json")
    closeout_digest = _first_payload(files, "step2_closeout_digest.json")
    gate_result = _first_payload(files, "engineering_isolation_gate_result.json")
    uncertainty_rollup = _first_payload(files, "uncertainty_rollup.json")
    uncertainty_report_pack = _first_payload(files, "uncertainty_report_pack.json")
    method_protocol = _first_payload(files, "method_confirmation_protocol.json")
    verification_rollup = _first_payload(files, "verification_rollup.json")
    evidence_coverage_matrix = _first_payload(files, "evidence_coverage_matrix.json")
    result_traceability_tree = _first_payload(files, "result_traceability_tree.json")
    evidence_lineage_index = _first_payload(files, "evidence_lineage_index.json")
    reviewer_anchor_navigation = _first_payload(files, "reviewer_anchor_navigation.json")
    ai_run_summary_payload = _first_payload(files, "ai_run_summary.json")
    ai_run_summary_text = _first_text(files, "ai_run_summary.md")
    recognition_binding = dict(
        summary_payload.get("recognition_binding")
        or dict(summary_payload.get("stats") or {}).get("recognition_binding")
        or {}
    )
    uncertainty_binding = dict(
        summary_payload.get("uncertainty_binding")
        or dict(summary_payload.get("stats") or {}).get("uncertainty_binding")
        or {}
    )
    remaining_blockers = _issue_texts(gate_result.get("blockers") or closeout_bundle.get("blocker_items") or [])
    remaining_warnings = _issue_texts(gate_result.get("warnings") or closeout_bundle.get("warning_items") or [])
    remaining_formal_gaps = _issue_texts(
        gate_result.get("unresolved_gaps")
        or closeout_bundle.get("formal_gap_items")
        or []
    )
    remaining_gaps = _dedupe_text(
        remaining_blockers
        + remaining_warnings
        + remaining_formal_gaps
        + [
            str(reason).strip()
            for reason in list(closeout_bundle.get("missing_evidence_categories") or [])
            if str(reason).strip()
        ]
    )
    return {
        "summary": summary_payload,
        "scope_definition_pack": scope_payload,
        "decision_rule_profile": decision_payload,
        "step2_closeout_bundle": closeout_bundle,
        "step2_closeout_digest": closeout_digest,
        "engineering_isolation_gate_result": gate_result,
        "uncertainty_report_pack": uncertainty_report_pack,
        "uncertainty_rollup": uncertainty_rollup,
        "method_confirmation_protocol": method_protocol,
        "verification_rollup": verification_rollup,
        "evidence_coverage_matrix": evidence_coverage_matrix,
        "result_traceability_tree": result_traceability_tree,
        "evidence_lineage_index": evidence_lineage_index,
        "reviewer_anchor_navigation": reviewer_anchor_navigation,
        "ai_run_summary_payload": ai_run_summary_payload,
        "ai_run_summary_text": ai_run_summary_text,
        "scope_id": str(
            closeout_bundle.get("scope_id")
            or closeout_digest.get("scope_id")
            or recognition_binding.get("scope_id")
            or scope_payload.get("scope_id")
            or dict(scope_payload.get("scope_export_pack") or {}).get("scope_id")
            or ""
        ).strip(),
        "decision_rule_id": str(
            closeout_bundle.get("decision_rule_id")
            or closeout_digest.get("decision_rule_id")
            or recognition_binding.get("decision_rule_id")
            or decision_payload.get("decision_rule_id")
            or ""
        ).strip(),
        "limitation_note": str(
            closeout_bundle.get("limitation_note")
            or closeout_digest.get("limitation_note")
            or recognition_binding.get("limitation_note")
            or decision_payload.get("limitation_note")
            or ""
        ).strip(),
        "non_claim_note": str(
            closeout_bundle.get("non_claim_note")
            or closeout_digest.get("non_claim_note")
            or recognition_binding.get("non_claim_note")
            or decision_payload.get("non_claim_note")
            or ""
        ).strip(),
        "uncertainty_case_id": str(
            closeout_bundle.get("uncertainty_case_id")
            or uncertainty_binding.get("uncertainty_case_id")
            or uncertainty_rollup.get("uncertainty_case_id")
            or uncertainty_report_pack.get("uncertainty_case_id")
            or ""
        ).strip(),
        "method_confirmation_protocol_id": str(
            closeout_bundle.get("method_confirmation_protocol_id")
            or uncertainty_binding.get("method_confirmation_protocol_id")
            or method_protocol.get("protocol_id")
            or method_protocol.get("method_confirmation_protocol_id")
            or ""
        ).strip(),
        "verification_rollup_id": str(
            closeout_bundle.get("verification_rollup_id")
            or verification_rollup.get("verification_rollup_id")
            or verification_rollup.get("verification_digest_id")
            or ""
        ).strip(),
        "engineering_isolation_gate_level": str(gate_result.get("gate_level") or ""),
        "remaining_blockers": remaining_blockers,
        "remaining_warnings": remaining_warnings,
        "remaining_formal_gaps": remaining_formal_gaps,
        "remaining_gaps": remaining_gaps,
    }


def _surface_validator_issues(
    *,
    validator_name: str,
    files: dict[str, dict[str, Any]],
    context: dict[str, Any],
) -> list[str]:
    if not validator_name:
        return []
    validators = {
        "review_center_surface": _validate_review_center_surface,
        "scope_decision_surface": _validate_scope_decision_surface,
        "uncertainty_method_surface": _validate_uncertainty_method_surface,
        "workbench_evidence_surface": _validate_workbench_evidence_surface,
        "sidecar_analytics_ai_surface": _validate_sidecar_analytics_ai_surface,
    }
    validator = validators.get(validator_name)
    if validator is None:
        return []
    return list(validator(files=files, context=context) or [])


def _validate_review_center_surface(*, files: dict[str, dict[str, Any]], context: dict[str, Any]) -> list[str]:
    issues: list[str] = []
    closeout_bundle = dict(context.get("step2_closeout_bundle") or {})
    coverage_matrix = dict(context.get("evidence_coverage_matrix") or {})
    traceability_tree = dict(context.get("result_traceability_tree") or {})
    reviewer_anchor_navigation = dict(context.get("reviewer_anchor_navigation") or {})
    gate_result = dict(context.get("engineering_isolation_gate_result") or {})
    if not str(closeout_bundle.get("summary_line") or "").strip():
        issues.append("step2_closeout_bundle missing summary_line")
    if not list(coverage_matrix.get("rows") or []):
        issues.append("evidence_coverage_matrix rows missing")
    if not list(traceability_tree.get("nodes") or []):
        issues.append("result_traceability_tree nodes missing")
    if not list(reviewer_anchor_navigation.get("anchors") or []):
        issues.append("reviewer_anchor_navigation anchors missing")
    if not str(dict(gate_result.get("review_surface") or {}).get("summary_text") or "").strip():
        issues.append("engineering_isolation_gate_result review_surface missing")
    return issues


def _validate_scope_decision_surface(*, files: dict[str, dict[str, Any]], context: dict[str, Any]) -> list[str]:
    issues: list[str] = []
    scope_id = str(context.get("scope_id") or "").strip()
    decision_rule_id = str(context.get("decision_rule_id") or "").strip()
    limitation_note = str(context.get("limitation_note") or "").strip()
    non_claim_note = str(context.get("non_claim_note") or "").strip()
    summary_binding = dict(
        dict(context.get("summary") or {}).get("recognition_binding")
        or dict(dict(context.get("summary") or {}).get("stats") or {}).get("recognition_binding")
        or {}
    )
    closeout_bundle = dict(context.get("step2_closeout_bundle") or {})
    gate_result = dict(context.get("engineering_isolation_gate_result") or {})
    if not scope_id:
        issues.append("scope_id missing")
    if not decision_rule_id:
        issues.append("decision_rule_id missing")
    if not limitation_note:
        issues.append("limitation_note missing")
    if not non_claim_note:
        issues.append("non_claim_note missing")
    if scope_id and str(summary_binding.get("scope_id") or "").strip() != scope_id:
        issues.append("summary recognition_binding scope_id missing")
    if decision_rule_id and str(summary_binding.get("decision_rule_id") or "").strip() != decision_rule_id:
        issues.append("summary recognition_binding decision_rule_id missing")
    if scope_id and str(closeout_bundle.get("scope_id") or "").strip() != scope_id:
        issues.append("step2_closeout_bundle scope_id missing")
    if decision_rule_id and str(closeout_bundle.get("decision_rule_id") or "").strip() != decision_rule_id:
        issues.append("step2_closeout_bundle decision_rule_id missing")
    if limitation_note and str(gate_result.get("limitation_note") or "").strip() != limitation_note:
        issues.append("engineering_isolation_gate_result limitation_note missing")
    if non_claim_note and str(gate_result.get("non_claim_note") or "").strip() != non_claim_note:
        issues.append("engineering_isolation_gate_result non_claim_note missing")
    return issues


def _validate_uncertainty_method_surface(*, files: dict[str, dict[str, Any]], context: dict[str, Any]) -> list[str]:
    issues: list[str] = []
    uncertainty_case_id = str(context.get("uncertainty_case_id") or "").strip()
    method_confirmation_protocol_id = str(context.get("method_confirmation_protocol_id") or "").strip()
    verification_rollup_id = str(context.get("verification_rollup_id") or "").strip()
    closeout_bundle = dict(context.get("step2_closeout_bundle") or {})
    uncertainty_rollup = dict(context.get("uncertainty_rollup") or {})
    method_protocol = dict(context.get("method_confirmation_protocol") or {})
    verification_rollup = dict(context.get("verification_rollup") or {})
    if not uncertainty_case_id:
        issues.append("uncertainty_case_id missing")
    if not method_confirmation_protocol_id:
        issues.append("method_confirmation_protocol_id missing")
    if not verification_rollup_id:
        issues.append("verification_rollup_id missing")
    if uncertainty_case_id and str(uncertainty_rollup.get("uncertainty_case_id") or "").strip() != uncertainty_case_id:
        issues.append("uncertainty_rollup uncertainty_case_id missing")
    if method_confirmation_protocol_id and str(
        method_protocol.get("protocol_id") or method_protocol.get("method_confirmation_protocol_id") or ""
    ).strip() != method_confirmation_protocol_id:
        issues.append("method_confirmation_protocol id missing")
    if verification_rollup_id and str(
        verification_rollup.get("verification_rollup_id") or verification_rollup.get("verification_digest_id") or ""
    ).strip() != verification_rollup_id:
        issues.append("verification_rollup id missing")
    if uncertainty_case_id and str(closeout_bundle.get("uncertainty_case_id") or "").strip() != uncertainty_case_id:
        issues.append("step2_closeout_bundle uncertainty_case_id missing")
    return issues


def _validate_workbench_evidence_surface(*, files: dict[str, dict[str, Any]], context: dict[str, Any]) -> list[str]:
    issues: list[str] = []
    closeout_bundle = dict(context.get("step2_closeout_bundle") or {})
    gate_result = dict(context.get("engineering_isolation_gate_result") or {})
    traceability_tree = dict(context.get("result_traceability_tree") or {})
    run_metadata_profile = _first_payload(files, "run_metadata_profile.json")
    ai_run_summary_text = str(context.get("ai_run_summary_text") or "").strip()
    if not str(closeout_bundle.get("summary_line") or "").strip():
        issues.append("closeout compact surface missing")
    if not str(dict(gate_result.get("review_surface") or {}).get("summary_text") or "").strip():
        issues.append("gate compact surface missing")
    if not str(dict(traceability_tree.get("digest") or {}).get("summary") or "").strip():
        issues.append("traceability compact surface missing")
    if not str(dict(run_metadata_profile.get("digest") or {}).get("summary") or "").strip():
        issues.append("human governance compact surface missing")
    if "reviewer_only" not in ai_run_summary_text.lower():
        issues.append("ai_run_summary reviewer boundary missing")
    return issues


def _validate_sidecar_analytics_ai_surface(*, files: dict[str, dict[str, Any]], context: dict[str, Any]) -> list[str]:
    content = str(context.get("ai_run_summary_text") or "").lower()
    issues: list[str] = []
    for token in (
        "advisory_only = true",
        "reviewer_only = true",
        "not_formal_metrology_conclusion = true",
    ):
        if token not in content:
            issues.append(f"ai_run_summary.md missing '{token}'")
    return issues


def _surface_boundary_issues(files: dict[str, dict[str, Any]], filenames: list[str]) -> list[str]:
    issues: list[str] = []
    for filename in filenames:
        payload = dict(files.get(filename, {}).get("payload") or {})
        if not payload:
            continue
        if "not_real_acceptance_evidence" in payload and payload.get("not_real_acceptance_evidence") is False:
            issues.append(f"{filename}: boundary flag not_real_acceptance_evidence=false")
        if "reviewer_only" in payload and payload.get("reviewer_only") is False:
            issues.append(f"{filename}: boundary flag reviewer_only=false")
        if "readiness_mapping_only" in payload and payload.get("readiness_mapping_only") is False:
            issues.append(f"{filename}: boundary flag readiness_mapping_only=false")
    return issues


def _surface_status_issues(*, files: dict[str, dict[str, Any]], filenames: list[str]) -> list[str]:
    issues: list[str] = []
    for filename in filenames:
        payload = dict(files.get(filename, {}).get("payload") or {})
        if not payload:
            continue
        classification = _classify_payload_status(payload)
        if classification == "degraded":
            issues.append(f"{filename}: degraded status detected")
        elif classification == "fail":
            issues.append(f"{filename}: failure status detected")
    return issues


def _build_bundle_diff_summary(
    *,
    current_files: dict[str, dict[str, Any]],
    baseline_files: dict[str, dict[str, Any]],
    current_label: str,
    baseline_label: str,
    current_dir: Path,
    baseline_dir: Path | None,
) -> dict[str, Any]:
    current_keys = set(current_files)
    baseline_keys = set(baseline_files)
    added = sorted(current_keys - baseline_keys)
    removed = sorted(baseline_keys - current_keys)
    changed = sorted(
        key
        for key in sorted(current_keys & baseline_keys)
        if str(current_files[key].get("sha1")) != str(baseline_files[key].get("sha1"))
    )
    unchanged = sorted(current_keys & baseline_keys - set(changed))
    return {
        "schema_version": "bundle-diff-summary-v1",
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "current_bundle": {"label": current_label, "bundle_dir": str(current_dir)},
        "baseline_bundle": {
            "label": baseline_label,
            "bundle_dir": str(baseline_dir) if baseline_dir else "",
            "available": baseline_dir is not None,
        },
        "summary": {
            "current_files": len(current_files),
            "baseline_files": len(baseline_files),
            "added_artifacts": len(added),
            "removed_artifacts": len(removed),
            "changed_artifacts": len(changed),
            "unchanged_artifacts": len(unchanged),
        },
        "added_artifacts": added,
        "removed_artifacts": removed,
        "changed_artifacts": [
            {
                "artifact": key,
                "baseline_sha1": str(baseline_files[key].get("sha1") or ""),
                "current_sha1": str(current_files[key].get("sha1") or ""),
            }
            for key in changed
        ],
        **_BOUNDARY_FIELDS,
    }


def _build_artifact_schema_diff(
    *,
    current_files: dict[str, dict[str, Any]],
    baseline_files: dict[str, dict[str, Any]],
    current_label: str,
    baseline_label: str,
) -> dict[str, Any]:
    changed_schema_fields: list[dict[str, Any]] = []
    artifact_rows: list[dict[str, Any]] = []
    shared_json_files = sorted(
        key
        for key in set(current_files) & set(baseline_files)
        if current_files[key].get("kind") == "json" and baseline_files[key].get("kind") == "json"
    )

    for key in shared_json_files:
        baseline_payload = dict(baseline_files[key].get("payload") or {})
        current_payload = dict(current_files[key].get("payload") or {})
        baseline_fields = _flatten_schema_fields(baseline_payload)
        current_fields = _flatten_schema_fields(current_payload)
        added_fields = sorted(current_fields.keys() - baseline_fields.keys())
        removed_fields = sorted(baseline_fields.keys() - current_fields.keys())
        type_changed_fields = sorted(
            field
            for field in baseline_fields.keys() & current_fields.keys()
            if baseline_fields[field] != current_fields[field]
        )
        if not (added_fields or removed_fields or type_changed_fields):
            continue
        for field in added_fields:
            changed_schema_fields.append(
                {
                    "artifact": key,
                    "field_path": field,
                    "change": "added",
                    "baseline_types": [],
                    "current_types": sorted(current_fields[field]),
                }
            )
        for field in removed_fields:
            changed_schema_fields.append(
                {
                    "artifact": key,
                    "field_path": field,
                    "change": "removed",
                    "baseline_types": sorted(baseline_fields[field]),
                    "current_types": [],
                }
            )
        for field in type_changed_fields:
            changed_schema_fields.append(
                {
                    "artifact": key,
                    "field_path": field,
                    "change": "type_changed",
                    "baseline_types": sorted(baseline_fields[field]),
                    "current_types": sorted(current_fields[field]),
                }
            )
        artifact_rows.append(
            {
                "artifact": key,
                "added_fields": added_fields,
                "removed_fields": removed_fields,
                "type_changed_fields": type_changed_fields,
            }
        )

    return {
        "schema_version": "artifact-schema-diff-v1",
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "current_label": current_label,
        "baseline_label": baseline_label,
        "summary": {
            "artifacts_compared": len(shared_json_files),
            "artifacts_changed": len(artifact_rows),
            "changed_field_count": len(changed_schema_fields),
        },
        "changed_schema_fields": changed_schema_fields,
        "artifacts": artifact_rows,
        **_BOUNDARY_FIELDS,
    }


def _build_artifact_regressions(
    *,
    current_surfaces: list[dict[str, Any]],
    baseline_surfaces: dict[str, dict[str, Any]],
    bundle_diff_summary: dict[str, Any],
    artifact_schema_diff: dict[str, Any],
) -> list[dict[str, Any]]:
    regressions: list[dict[str, Any]] = []
    severity_order = {"missing": 0, "degraded": 1, "pass": 2}
    for surface in current_surfaces:
        baseline_surface = baseline_surfaces.get(str(surface.get("surface_id") or ""))
        if not baseline_surface:
            continue
        current_status = str(surface.get("status") or "missing")
        baseline_status = str(baseline_surface.get("status") or "missing")
        if severity_order.get(current_status, -1) < severity_order.get(baseline_status, -1):
            regressions.append(
                {
                    "kind": "surface_regression",
                    "surface_id": surface.get("surface_id"),
                    "from": baseline_status,
                    "to": current_status,
                    "reason": surface.get("reason"),
                }
            )

    for artifact in list(bundle_diff_summary.get("removed_artifacts") or []):
        regressions.append(
            {
                "kind": "artifact_removed",
                "artifact": artifact,
                "reason": "artifact missing from current bundle",
            }
        )

    for field in list(artifact_schema_diff.get("changed_schema_fields") or [])[:20]:
        if str(field.get("change") or "") in {"removed", "type_changed"}:
            regressions.append(
                {
                    "kind": "schema_regression",
                    "artifact": field.get("artifact"),
                    "field_path": field.get("field_path"),
                    "change": field.get("change"),
                }
            )
    return regressions


def _surface_index(rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    return {
        str(row.get("surface_id") or ""): dict(row)
        for row in rows
        if str(row.get("surface_id") or "").strip()
    }


def _build_golden_coverage(*, registry: dict[str, Any], executed_case_names: list[str]) -> dict[str, Any]:
    cases = [dict(item) for item in list(registry.get("cases") or []) if isinstance(item, dict)]
    case_index = {str(case.get("case_id") or ""): case for case in cases}
    executed = [case_index[name] for name in executed_case_names if name in case_index]
    return {
        "executed_case_count": len(executed),
        "registry_case_count": len(cases),
        "executed_case_names": [str(case.get("case_id") or "") for case in executed],
        "gas_family_counts": _count_rows(executed, "gas_families"),
        "path_category_counts": _count_rows(executed, "path_categories"),
        "temperature_point_category_counts": _count_rows(executed, "temperature_point_categories"),
        "pressure_point_category_counts": _count_rows(executed, "pressure_point_categories"),
        "anomaly_counts": _count_rows(executed, "anomaly_scenarios"),
    }


def _recommend_bundle(
    *,
    current_label: str,
    baseline_label: str,
    has_baseline: bool,
    validation: dict[str, Any],
    current_surfaces: list[dict[str, Any]],
    artifact_regressions: list[dict[str, Any]],
    remaining_blockers: list[str],
    remaining_warnings: list[str],
    remaining_formal_gaps: list[str],
) -> dict[str, Any]:
    missing_surfaces = [surface for surface in current_surfaces if surface["status"] == "missing"]
    degraded_surfaces = [surface for surface in current_surfaces if surface["status"] == "degraded"]
    if artifact_regressions and has_baseline:
        return {
            "recommended_bundle_label": baseline_label,
            "recommendation_state": "baseline_preferred",
            "reason": "current bundle regressed against previous baseline on required offline surfaces",
        }
    if (
        validation["failed"] == 0
        and not missing_surfaces
        and not degraded_surfaces
        and not remaining_blockers
        and not remaining_warnings
        and not remaining_formal_gaps
    ):
        return {
            "recommended_bundle_label": current_label,
            "recommendation_state": "step2_reviewer_candidate",
            "reason": "current bundle has no failed suite cases and no missing required reviewer/readiness surfaces",
        }
    return {
        "recommended_bundle_label": current_label,
        "recommendation_state": "candidate_with_gaps",
        "reason": (
            f"current bundle remains the best offline candidate, "
            f"but still has {len(degraded_surfaces)} degraded surfaces, {len(missing_surfaces)} missing surfaces, "
            f"{len(remaining_blockers)} blockers, {len(remaining_warnings)} warnings, "
            f"and {len(remaining_formal_gaps)} formal gaps"
        ),
    }


def _render_scoreboard_markdown(scoreboard: dict[str, Any]) -> str:
    validation = dict(scoreboard.get("validation_counts") or {})
    surfaces = dict(scoreboard.get("surface_counts") or {})
    recommendation = dict(scoreboard.get("recommendation") or {})
    degraded_areas = list(scoreboard.get("degraded_areas") or [])
    missing_surfaces = list(scoreboard.get("missing_surfaces") or [])
    artifact_regressions = list(scoreboard.get("artifact_regressions") or [])
    changed_schema_fields = list(scoreboard.get("changed_schema_fields") or [])
    golden_coverage = dict(scoreboard.get("golden_registry_coverage") or {})
    remaining_blockers = list(scoreboard.get("remaining_blockers") or [])
    remaining_warnings = list(scoreboard.get("remaining_warnings") or [])
    remaining_formal_gaps = list(scoreboard.get("remaining_formal_gaps") or [])
    lines = [
        "# Regression Scoreboard",
        "",
        f"- current bundle: {dict(scoreboard.get('current_bundle') or {}).get('bundle_dir', '--')}",
        f"- baseline bundle: {dict(scoreboard.get('baseline_bundle') or {}).get('bundle_dir', '--') or '--'}",
        f"- suite pass/fail: {validation.get('passed', 0)}/{validation.get('failed', 0)}",
        f"- suite skipped/degraded: {validation.get('skipped', 0)}/{validation.get('degraded', 0)}",
        f"- surface pass/degraded/missing: {surfaces.get('passed', 0)}/{surfaces.get('degraded', 0)}/{surfaces.get('missing', 0)}",
        f"- executed golden cases: {golden_coverage.get('executed_case_count', 0)}/{golden_coverage.get('registry_case_count', 0)}",
        f"- scope / decision: {scoreboard.get('scope_id', '--') or '--'} / {scoreboard.get('decision_rule_id', '--') or '--'}",
        f"- limitation note: {scoreboard.get('limitation_note', '--') or '--'}",
        f"- non-claim note: {scoreboard.get('non_claim_note', '--') or '--'}",
        f"- uncertainty / method: {scoreboard.get('uncertainty_case_id', '--') or '--'} / {scoreboard.get('method_confirmation_protocol_id', '--') or '--'} / {scoreboard.get('verification_rollup_id', '--') or '--'}",
        f"- engineering isolation gate: {scoreboard.get('engineering_isolation_gate_level', '--') or '--'}",
        f"- recommendation: {recommendation.get('recommended_bundle_label', '--')} ({recommendation.get('recommendation_state', '--')})",
        "",
        "## Degraded Areas",
    ]
    if degraded_areas:
        lines.extend(
            f"- {item.get('surface_id', '--')}: {item.get('reason', '--')}"
            for item in degraded_areas
        )
    else:
        lines.append("- none")
    lines.extend(["", "## Missing Surfaces"])
    if missing_surfaces:
        lines.extend(f"- {item}" for item in missing_surfaces)
    else:
        lines.append("- none")
    lines.extend(["", "## Remaining Blockers"])
    if remaining_blockers:
        lines.extend(f"- {item}" for item in remaining_blockers)
    else:
        lines.append("- none")
    lines.extend(["", "## Remaining Warnings"])
    if remaining_warnings:
        lines.extend(f"- {item}" for item in remaining_warnings)
    else:
        lines.append("- none")
    lines.extend(["", "## Remaining Formal Gaps"])
    if remaining_formal_gaps:
        lines.extend(f"- {item}" for item in remaining_formal_gaps)
    else:
        lines.append("- none")
    lines.extend(["", "## Artifact Regressions"])
    if artifact_regressions:
        lines.extend(
            f"- {item.get('kind', '--')}: {item.get('surface_id') or item.get('artifact') or item.get('field_path')}"
            for item in artifact_regressions
        )
    else:
        lines.append("- none")
    lines.extend(["", "## Changed Schema Fields"])
    if changed_schema_fields:
        lines.extend(
            f"- {item.get('artifact', '--')} :: {item.get('field_path', '--')} ({item.get('change', '--')})"
            for item in changed_schema_fields[:20]
        )
    else:
        lines.append("- none")
    lines.append("")
    return "\n".join(lines)


def _flatten_schema_fields(payload: Any, *, path: str = "") -> dict[str, set[str]]:
    field_map: dict[str, set[str]] = {}
    field_map.setdefault(path or "$", set()).add(_type_name(payload))
    if isinstance(payload, dict):
        for key, value in payload.items():
            child_path = f"{path}.{key}" if path else key
            child_map = _flatten_schema_fields(value, path=child_path)
            for child_key, child_types in child_map.items():
                field_map.setdefault(child_key, set()).update(child_types)
    elif isinstance(payload, list):
        child_path = f"{path}[]" if path else "[]"
        for item in payload:
            child_map = _flatten_schema_fields(item, path=child_path)
            for child_key, child_types in child_map.items():
                field_map.setdefault(child_key, set()).update(child_types)
    return field_map


def _type_name(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int) and not isinstance(value, bool):
        return "integer"
    if isinstance(value, float):
        return "number"
    if isinstance(value, str):
        return "string"
    if isinstance(value, list):
        return "array"
    if isinstance(value, dict):
        return "object"
    return type(value).__name__


def _first_payload(files: dict[str, dict[str, Any]], filename: str) -> dict[str, Any]:
    for info in files.values():
        if info.get("filename") == filename and isinstance(info.get("payload"), dict):
            return dict(info.get("payload") or {})
    return {}


def _first_text(files: dict[str, dict[str, Any]], filename: str) -> str:
    for info in files.values():
        if info.get("filename") != filename:
            continue
        path = Path(str(info.get("path") or ""))
        if not path.exists():
            continue
        try:
            return path.read_text(encoding="utf-8")
        except Exception:
            return ""
    return ""


def _issue_texts(values: Any) -> list[str]:
    rows: list[str] = []
    for item in list(values or []):
        if isinstance(item, dict):
            text = str(item.get("summary") or item.get("title") or item.get("reason") or "").strip()
        else:
            text = str(item or "").strip()
        if text and text not in rows:
            rows.append(text)
    return rows


def _dedupe_text(values: list[str]) -> list[str]:
    rows: list[str] = []
    for value in values:
        text = str(value or "").strip()
        if text and text not in rows:
            rows.append(text)
    return rows


def _classify_payload_status(payload: dict[str, Any]) -> str:
    suite_counts = dict(payload.get("counts") or {})
    if suite_counts:
        if int(suite_counts.get("failed") or 0) > 0:
            return "fail"
        return "pass"
    for candidate in _status_candidates(payload):
        normalized = str(candidate or "").strip().lower()
        if normalized in {"fail", "failed", "error", "blocked", "blocker"}:
            return "fail"
        if normalized in {
            "warn",
            "warning",
            "degraded",
            "mismatch",
            "snapshot_only",
            "reviewer_only",
            "attention",
        }:
            return "degraded"
        if normalized in {
            "match",
            "pass",
            "passed",
            "ok",
            "ready",
            "ready_for_engineering_isolation",
            "completed",
            "collected",
            "closeout_candidate",
            "dry_run_only",
        }:
            return "pass"
    return "pass"


def _status_candidates(payload: dict[str, Any]) -> list[Any]:
    candidates = [
        payload.get("status"),
        payload.get("compare_status"),
        payload.get("overall_status"),
        payload.get("verification_status"),
        payload.get("closeout_status"),
        payload.get("package_status"),
        payload.get("audit_status"),
        payload.get("dossier_status"),
        payload.get("gate_level"),
        payload.get("promotion_state"),
        dict(payload.get("summary") or {}).get("status"),
        dict(payload.get("export_resilience_status") or {}).get("overall_status"),
        dict(payload.get("reference_quality_statistics") or {}).get("reference_quality"),
    ]
    return [item for item in candidates if item not in (None, "")]


def _count_rows(rows: list[dict[str, Any]], key: str) -> dict[str, int]:
    counter: Counter[str] = Counter()
    for row in rows:
        for item in list(row.get(key) or []):
            text = str(item or "").strip()
            if text:
                counter[text] += 1
    return dict(sorted(counter.items()))


def _load_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return dict(payload) if isinstance(payload, dict) else {}


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
