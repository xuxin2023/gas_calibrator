from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .stage3_real_validation_plan_artifact_entry import _VALIDATION_CATEGORY_LABELS


ENGINEERING_ISOLATION_GATE_RESULT_FILENAME = "engineering_isolation_gate_result.json"
ENGINEERING_ISOLATION_GATE_DIGEST_FILENAME = "engineering_isolation_gate_digest.md"
ENGINEERING_ISOLATION_BLOCKERS_FILENAME = "engineering_isolation_blockers.json"
ENGINEERING_ISOLATION_WARNINGS_FILENAME = "engineering_isolation_warnings.json"

ENGINEERING_ISOLATION_GATE_ARTIFACT_KEY = "engineering_isolation_gate_result"
ENGINEERING_ISOLATION_GATE_REVIEWER_ARTIFACT_KEY = "engineering_isolation_gate_digest"
ENGINEERING_ISOLATION_BLOCKERS_ARTIFACT_KEY = "engineering_isolation_blockers"
ENGINEERING_ISOLATION_WARNINGS_ARTIFACT_KEY = "engineering_isolation_warnings"

PASS_FOR_REVIEWER_BRIDGE = "pass_for_reviewer_bridge"
BLOCKED_FOR_ENGINEERING_ISOLATION = "blocked_for_engineering_isolation"
MISSING_FORMAL_EVIDENCE = "missing_formal_evidence"
ADVISORY_ONLY_OK = "advisory_only_ok"

_TITLE_TEXT = "Engineering Isolation Reviewer Bridge Gate / 工程隔离准入桥接总闸"
_BRIDGE_NOTE = (
    "This gate is reviewer/admission bridge only. "
    "It is not formal admission approval, not real acceptance, and does not change the default execution chain."
)
_BOUNDARY_STATEMENTS = [
    "reviewer / admission bridge only",
    "simulation / offline / headless only",
    "not formal admission approval",
    "not real acceptance",
    "default execution chain unchanged",
    "no real device / no real COM",
    "sidecar / AI must remain off main chain",
]

_GATE_LEVEL_DISPLAY = {
    PASS_FOR_REVIEWER_BRIDGE: "准入桥结论：可进入 reviewer bridge",
    BLOCKED_FOR_ENGINEERING_ISOLATION: "准入桥结论：仍阻塞 engineering-isolation",
    MISSING_FORMAL_EVIDENCE: "准入桥结论：仍缺 formal evidence",
    ADVISORY_ONLY_OK: "准入桥结论：仅 advisory 通过",
}

_DEFAULT_BUNDLE_SUGGESTIONS = {
    "scope_decision_bundle": {
        "bundle_id": "scope_decision_bundle",
        "title": "Scope / Decision Rule Bundle",
        "summary": "Bind scope definition, decision rule, and conformity boundary before any later-stage validation draft.",
        "required_evidence_categories": ["scope / decision rule binding"],
    },
    "asset_certificate_bundle": {
        "bundle_id": "asset_certificate_bundle",
        "title": "Asset / Certificate / Pre-run Bundle",
        "summary": "Close reference assets, certificate lifecycle, intermediate checks, and advisory pre-run gaps in one bridge package.",
        "required_evidence_categories": [
            "reference assets / certificates / intermediate checks",
            "traceability review",
        ],
    },
    "uncertainty_method_bundle": {
        "bundle_id": "uncertainty_method_bundle",
        "title": "Uncertainty / Method Confirmation Bundle",
        "summary": "Pair uncertainty objects, example cases, method confirmation, and verification rollups into a reviewer-complete bundle.",
        "required_evidence_categories": [
            "uncertainty objects / examples",
            "method confirmation",
            "real run uncertainty result",
            "real-world repeatability",
        ],
    },
    "software_validation_audit_bundle": {
        "bundle_id": "software_validation_audit_bundle",
        "title": "Software Validation / Audit Bundle",
        "summary": "Close traceability, evidence index, release governance, and audit-readiness sidecars without promoting them into real approval.",
        "required_evidence_categories": [
            "software validation / audit readiness",
            "traceability review",
        ],
    },
    "comparison_pt_ilc_bundle": {
        "bundle_id": "comparison_pt_ilc_bundle",
        "title": "Comparison / PT-ILC Bundle",
        "summary": "Prepare comparison evidence, scope comparison, and PT/ILC readiness as a Stage 3 draft input set.",
        "required_evidence_categories": [
            "comparison readiness",
            "PT / ILC readiness",
        ],
    },
    "device_acceptance_bundle": {
        "bundle_id": "device_acceptance_bundle",
        "title": "Stage 3 Device Acceptance Bundle",
        "summary": "Draft the real-device acceptance, writeback, pass/fail, and anomaly-retest bundle as future-stage input only.",
        "required_evidence_categories": [
            "device acceptance",
            "pass / fail contract",
            "anomaly retest",
        ],
    },
}

_KNOWN_ARTIFACT_FILENAMES = {
    "stage_admission_review_pack": "stage_admission_review_pack.json",
    "engineering_isolation_admission_checklist": "engineering_isolation_admission_checklist.json",
    "pre_run_readiness_gate": "pre_run_readiness_gate.json",
    "step2_closeout_bundle": "step2_closeout_bundle.json",
    "stage3_standards_alignment_matrix": "stage3_standards_alignment_matrix.json",
    "stage3_real_validation_plan": "stage3_real_validation_plan.json",
    "scope_definition_pack": "scope_definition_pack.json",
    "decision_rule_profile": "decision_rule_profile.json",
    "reference_asset_registry": "reference_asset_registry.json",
    "certificate_lifecycle_summary": "certificate_lifecycle_summary.json",
    "uncertainty_report_pack": "uncertainty_report_pack.json",
    "uncertainty_rollup": "uncertainty_rollup.json",
    "uncertainty_method_readiness_summary": "uncertainty_method_readiness_summary.json",
    "method_confirmation_protocol": "method_confirmation_protocol.json",
    "verification_rollup": "verification_rollup.json",
    "software_validation_traceability_matrix": "software_validation_traceability_matrix.json",
    "requirement_design_code_test_links": "requirement_design_code_test_links.json",
    "validation_evidence_index": "validation_evidence_index.json",
    "software_validation_rollup": "software_validation_rollup.json",
    "audit_readiness_digest": "audit_readiness_digest.json",
    "comparison_evidence_pack": "comparison_evidence_pack.json",
    "scope_comparison_view": "scope_comparison_view.json",
    "comparison_digest": "comparison_digest.json",
    "comparison_rollup": "comparison_rollup.json",
    "pt_ilc_registry": "pt_ilc_registry.json",
    "sidecar_index_summary": "sidecar_index_summary.json",
    "review_copilot_payload": "review_copilot_payload.json",
    "model_governance_summary": "model_governance_summary.json",
}


def build_engineering_isolation_gate_evaluator(
    *,
    run_id: str = "",
    run_dir: str | Path | None = None,
    stage_admission_review_pack: dict[str, Any] | None = None,
    engineering_isolation_admission_checklist: dict[str, Any] | None = None,
    pre_run_readiness_gate: dict[str, Any] | None = None,
    step2_closeout_bundle: dict[str, Any] | None = None,
    step2_closeout_compact_section: dict[str, Any] | None = None,
    stage3_standards_alignment_matrix: dict[str, Any] | None = None,
    stage3_real_validation_plan: dict[str, Any] | None = None,
    scope_definition_pack: dict[str, Any] | None = None,
    decision_rule_profile: dict[str, Any] | None = None,
    conformity_statement_profile: dict[str, Any] | None = None,
    reference_asset_registry: dict[str, Any] | None = None,
    certificate_lifecycle_summary: dict[str, Any] | None = None,
    uncertainty_report_pack: dict[str, Any] | None = None,
    uncertainty_rollup: dict[str, Any] | None = None,
    uncertainty_method_readiness_summary: dict[str, Any] | None = None,
    method_confirmation_protocol: dict[str, Any] | None = None,
    verification_rollup: dict[str, Any] | None = None,
    software_validation_traceability_matrix: dict[str, Any] | None = None,
    requirement_design_code_test_links: dict[str, Any] | None = None,
    validation_evidence_index: dict[str, Any] | None = None,
    software_validation_rollup: dict[str, Any] | None = None,
    audit_readiness_digest: dict[str, Any] | None = None,
    pt_ilc_registry: dict[str, Any] | None = None,
    comparison_evidence_pack: dict[str, Any] | None = None,
    scope_comparison_view: dict[str, Any] | None = None,
    comparison_digest: dict[str, Any] | None = None,
    comparison_rollup: dict[str, Any] | None = None,
    sidecar_index_summary: dict[str, Any] | None = None,
    review_copilot_payload: dict[str, Any] | None = None,
    model_governance_summary: dict[str, Any] | None = None,
) -> dict[str, Any]:
    normalized_run_id = str(run_id or "").strip()
    normalized_run_dir = str(Path(run_dir)) if run_dir is not None else ""

    review_pack = dict(stage_admission_review_pack or {})
    checklist = dict(engineering_isolation_admission_checklist or {})
    pre_run_gate = dict(pre_run_readiness_gate or {})
    closeout_bundle = dict(step2_closeout_bundle or {})
    closeout_compact = dict(step2_closeout_compact_section or {})
    standards_matrix = dict(stage3_standards_alignment_matrix or {})
    stage3_plan = dict(stage3_real_validation_plan or {})
    scope_pack = dict(scope_definition_pack or {})
    decision_profile = dict(decision_rule_profile or {})
    conformity_profile = dict(
        conformity_statement_profile
        or dict(decision_profile.get("conformity_statement_profile") or {})
        or {}
    )
    reference_registry = dict(reference_asset_registry or {})
    certificate_summary = dict(certificate_lifecycle_summary or {})
    uncertainty_pack = dict(uncertainty_report_pack or {})
    uncertainty_rollup_payload = dict(uncertainty_rollup or {})
    uncertainty_summary = dict(uncertainty_method_readiness_summary or {})
    method_protocol = dict(method_confirmation_protocol or {})
    verification_rollup_payload = dict(verification_rollup or {})
    software_traceability = dict(software_validation_traceability_matrix or {})
    requirement_links = dict(requirement_design_code_test_links or {})
    validation_index = dict(validation_evidence_index or {})
    software_rollup = dict(software_validation_rollup or {})
    audit_digest = dict(audit_readiness_digest or {})
    pt_ilc = dict(pt_ilc_registry or {})
    comparison_pack = dict(comparison_evidence_pack or {})
    comparison_scope = dict(scope_comparison_view or {})
    comparison_digest_payload = dict(comparison_digest or {})
    comparison_rollup_payload = dict(comparison_rollup or {})
    sidecar_index = dict(sidecar_index_summary or {})
    review_copilot = dict(review_copilot_payload or {})
    model_governance = dict(model_governance_summary or {})

    source_artifact_refs = _build_source_artifact_refs(
        run_dir=normalized_run_dir,
        payloads={
            "stage_admission_review_pack": review_pack,
            "engineering_isolation_admission_checklist": checklist,
            "pre_run_readiness_gate": pre_run_gate,
            "step2_closeout_bundle": closeout_bundle,
            "stage3_standards_alignment_matrix": standards_matrix,
            "stage3_real_validation_plan": stage3_plan,
            "scope_definition_pack": scope_pack,
            "decision_rule_profile": decision_profile,
            "reference_asset_registry": reference_registry,
            "certificate_lifecycle_summary": certificate_summary,
            "uncertainty_report_pack": uncertainty_pack,
            "uncertainty_rollup": uncertainty_rollup_payload,
            "uncertainty_method_readiness_summary": uncertainty_summary,
            "method_confirmation_protocol": method_protocol,
            "verification_rollup": verification_rollup_payload,
            "software_validation_traceability_matrix": software_traceability,
            "requirement_design_code_test_links": requirement_links,
            "validation_evidence_index": validation_index,
            "software_validation_rollup": software_rollup,
            "audit_readiness_digest": audit_digest,
            "comparison_evidence_pack": comparison_pack,
            "scope_comparison_view": comparison_scope,
            "comparison_digest": comparison_digest_payload,
            "comparison_rollup": comparison_rollup_payload,
            "pt_ilc_registry": pt_ilc,
            "sidecar_index_summary": sidecar_index,
            "review_copilot_payload": review_copilot,
            "model_governance_summary": model_governance,
        },
    )

    blockers: list[dict[str, Any]] = []
    warnings: list[dict[str, Any]] = []
    formal_gaps: list[dict[str, Any]] = []
    next_actions: list[str] = []
    bundle_suggestion_ids: list[str] = []
    checks: list[dict[str, Any]] = []

    review_bridge_required = {
        "review_pack": bool(review_pack),
        "checklist": bool(checklist),
        "closeout_bundle": bool(closeout_bundle or closeout_compact),
    }
    review_bridge_missing = [
        label
        for label, present in (
            ("stage_admission_review_pack", review_bridge_required["review_pack"]),
            (
                "engineering_isolation_admission_checklist",
                review_bridge_required["checklist"],
            ),
            ("step2_closeout_bundle", review_bridge_required["closeout_bundle"]),
        )
        if not present
    ]
    closeout_blocker_items = _text_list(closeout_bundle.get("blocker_items") or [])
    closeout_warning_items = _text_list(closeout_bundle.get("warning_items") or [])
    closeout_missing_categories = _text_list(
        closeout_bundle.get("missing_evidence_categories")
        or closeout_compact.get("missing_evidence_categories")
        or []
    )
    review_bridge_check_issues: list[dict[str, Any]] = []
    if review_bridge_missing:
        review_bridge_check_issues.extend(
            _make_issue_rows(
                check_id="reviewer_bridge_bundle",
                severity="blocker",
                title="reviewer / closeout bridge artifacts missing",
                items=[f"missing artifact: {item}" for item in review_bridge_missing],
                source_artifacts=[
                    "stage_admission_review_pack",
                    "engineering_isolation_admission_checklist",
                    "step2_closeout_bundle",
                ],
                category="reviewer_bridge",
            )
        )
    if closeout_missing_categories:
        review_bridge_check_issues.extend(
            _make_issue_rows(
                check_id="reviewer_bridge_bundle",
                severity="blocker",
                title="step2 closeout categories missing",
                items=[f"missing closeout category: {item}" for item in closeout_missing_categories],
                source_artifacts=["step2_closeout_bundle"],
                category="reviewer_bridge",
            )
        )
    if closeout_blocker_items:
        review_bridge_check_issues.extend(
            _make_issue_rows(
                check_id="reviewer_bridge_bundle",
                severity="blocker",
                title="step2 closeout blockers",
                items=closeout_blocker_items,
                source_artifacts=["step2_closeout_bundle"],
                category="reviewer_bridge",
            )
        )
    if closeout_warning_items:
        warnings.extend(
            _make_issue_rows(
                check_id="reviewer_bridge_bundle",
                severity="warning",
                title="step2 closeout warnings",
                items=closeout_warning_items,
                source_artifacts=["step2_closeout_bundle"],
                category="reviewer_bridge",
            )
        )
    checks.append(
        _build_check_result(
            check_id="reviewer_bridge_bundle",
            title="reviewer 工件 / closeout bundle bridge",
            issues=review_bridge_check_issues,
            fallback_status="pass",
            source_artifacts=[
                "stage_admission_review_pack",
                "engineering_isolation_admission_checklist",
                "step2_closeout_bundle",
            ],
            summary=_first_non_empty(
                closeout_compact.get("summary_line"),
                closeout_bundle.get("summary_line"),
                _summary_text(review_pack),
            ),
        )
    )
    blockers.extend(review_bridge_check_issues)
    next_actions.extend(_text_list(closeout_bundle.get("bridge_to_stage3_candidates") or []))
    next_actions.extend(_text_list(review_pack.get("execute_now_in_step2_tail") or []))

    scope_binding_issues: list[dict[str, Any]] = []
    missing_scope_bindings = [
        label
        for label, present in (
            ("scope_definition_pack", bool(scope_pack)),
            ("decision_rule_profile", bool(decision_profile)),
            (
                "conformity_statement_profile",
                bool(conformity_profile) or bool(decision_profile.get("conformity_statement_profile")),
            ),
        )
        if not present
    ]
    if missing_scope_bindings:
        scope_binding_issues.extend(
            _make_issue_rows(
                check_id="scope_decision_binding",
                severity="blocker",
                title="scope / decision rule binding incomplete",
                items=[f"missing binding: {item}" for item in missing_scope_bindings],
                source_artifacts=["scope_definition_pack", "decision_rule_profile"],
                category="scope_binding",
            )
        )
    checks.append(
        _build_check_result(
            check_id="scope_decision_binding",
            title="scope / decision rule binding",
            issues=scope_binding_issues,
            fallback_status="pass",
            source_artifacts=["scope_definition_pack", "decision_rule_profile"],
            summary=_merge_text(
                _summary_text(scope_pack),
                _summary_text(decision_profile),
            ),
        )
    )
    blockers.extend(scope_binding_issues)
    if scope_binding_issues:
        bundle_suggestion_ids.append("scope_decision_bundle")

    pre_run_blocking_items = _text_list(pre_run_gate.get("blocking_items") or [])
    pre_run_warning_items = _text_list(pre_run_gate.get("warning_items") or [])
    assets_issues: list[dict[str, Any]] = []
    missing_asset_gate_inputs = [
        label
        for label, present in (
            ("reference_asset_registry", bool(reference_registry)),
            ("certificate_lifecycle_summary", bool(certificate_summary)),
            ("pre_run_readiness_gate", bool(pre_run_gate)),
        )
        if not present
    ]
    if missing_asset_gate_inputs:
        assets_issues.extend(
            _make_issue_rows(
                check_id="asset_certificate_intermediate_checks",
                severity="blocker",
                title="asset / certificate / pre-run inputs missing",
                items=[f"missing reviewer readiness input: {item}" for item in missing_asset_gate_inputs],
                source_artifacts=[
                    "reference_asset_registry",
                    "certificate_lifecycle_summary",
                    "pre_run_readiness_gate",
                ],
                category="asset_readiness",
            )
        )
    if pre_run_blocking_items:
        assets_issues.extend(
            _make_issue_rows(
                check_id="asset_certificate_intermediate_checks",
                severity="blocker",
                title="asset / certificate / intermediate checks blocked",
                items=pre_run_blocking_items,
                source_artifacts=["pre_run_readiness_gate"],
                category="asset_readiness",
            )
        )
    if pre_run_warning_items:
        warnings.extend(
            _make_issue_rows(
                check_id="asset_certificate_intermediate_checks",
                severity="warning",
                title="asset / certificate warnings",
                items=pre_run_warning_items,
                source_artifacts=["pre_run_readiness_gate"],
                category="asset_readiness",
            )
        )
    pre_run_status = str(
        dict(pre_run_gate.get("digest") or {}).get("pre_run_gate_status")
        or pre_run_gate.get("gate_status")
        or ""
    ).strip()
    if (
        not assets_issues
        and not pre_run_warning_items
        and pre_run_status
        and pre_run_status not in {"pass", "ready", "ok"}
    ):
        warnings.extend(
            _make_issue_rows(
                check_id="asset_certificate_intermediate_checks",
                severity="warning",
                title="pre-run readiness remains advisory",
                items=[f"pre-run gate status: {pre_run_status}"],
                source_artifacts=["pre_run_readiness_gate"],
                category="asset_readiness",
            )
        )
    checks.append(
        _build_check_result(
            check_id="asset_certificate_intermediate_checks",
            title="assets / certificates / intermediate checks",
            issues=assets_issues,
            fallback_status="warning" if pre_run_warning_items or pre_run_status else "pass",
            source_artifacts=[
                "reference_asset_registry",
                "certificate_lifecycle_summary",
                "pre_run_readiness_gate",
            ],
            summary=_merge_text(
                _summary_text(reference_registry),
                dict(pre_run_gate.get("digest") or {}).get("asset_readiness_overview"),
                dict(pre_run_gate.get("digest") or {}).get("certificate_lifecycle_overview"),
            ),
        )
    )
    blockers.extend(assets_issues)
    if assets_issues or pre_run_warning_items:
        bundle_suggestion_ids.append("asset_certificate_bundle")

    uncertainty_method_issues: list[dict[str, Any]] = []
    missing_uncertainty_method = [
        label
        for label, present in (
            (
                "uncertainty",
                bool(uncertainty_pack or uncertainty_rollup_payload or uncertainty_summary),
            ),
            (
                "method_confirmation",
                bool(method_protocol or verification_rollup_payload),
            ),
        )
        if not present
    ]
    if missing_uncertainty_method:
        uncertainty_method_issues.extend(
            _make_issue_rows(
                check_id="uncertainty_method_confirmation",
                severity="blocker",
                title="uncertainty / method confirmation inputs missing",
                items=[f"missing bridge input: {item}" for item in missing_uncertainty_method],
                source_artifacts=[
                    "uncertainty_report_pack",
                    "uncertainty_rollup",
                    "uncertainty_method_readiness_summary",
                    "method_confirmation_protocol",
                    "verification_rollup",
                ],
                category="uncertainty_method",
            )
        )
    uncertainty_examples_ready = _payload_has_examples(uncertainty_pack) or _payload_has_examples(
        uncertainty_rollup_payload
    )
    method_examples_ready = _payload_has_examples(method_protocol) or _payload_has_examples(
        verification_rollup_payload
    )
    if not missing_uncertainty_method and (not uncertainty_examples_ready or not method_examples_ready):
        warnings.extend(
            _make_issue_rows(
                check_id="uncertainty_method_confirmation",
                severity="warning",
                title="uncertainty / method examples remain thin",
                items=[
                    item
                    for item in (
                        "uncertainty examples not detected" if not uncertainty_examples_ready else "",
                        "method confirmation examples not detected" if not method_examples_ready else "",
                    )
                    if item
                ],
                source_artifacts=[
                    "uncertainty_report_pack",
                    "method_confirmation_protocol",
                ],
                category="uncertainty_method",
            )
        )
    uncertainty_formal_lines = _text_list(
        [
            dict(uncertainty_pack.get("digest") or {}).get("missing_evidence_summary"),
            dict(uncertainty_pack.get("digest") or {}).get("reviewer_next_step_digest"),
            dict(uncertainty_summary.get("digest") or {}).get("missing_evidence_summary"),
            dict(uncertainty_summary.get("digest") or {}).get("reviewer_next_step_digest"),
            dict(method_protocol.get("digest") or {}).get("missing_evidence_summary"),
            dict(method_protocol.get("digest") or {}).get("reviewer_next_step_digest"),
        ]
    )
    if not missing_uncertainty_method and uncertainty_formal_lines:
        formal_gaps.extend(
            _make_issue_rows(
                check_id="uncertainty_method_confirmation",
                severity="missing_formal",
                title="uncertainty / method formal evidence still missing",
                items=uncertainty_formal_lines[:4],
                source_artifacts=[
                    "uncertainty_report_pack",
                    "uncertainty_method_readiness_summary",
                    "method_confirmation_protocol",
                    "verification_rollup",
                ],
                category="uncertainty_method",
            )
        )
    checks.append(
        _build_check_result(
            check_id="uncertainty_method_confirmation",
            title="uncertainty / method confirmation objects and examples",
            issues=uncertainty_method_issues,
            fallback_status="missing_formal" if uncertainty_formal_lines else "pass",
            source_artifacts=[
                "uncertainty_report_pack",
                "uncertainty_rollup",
                "uncertainty_method_readiness_summary",
                "method_confirmation_protocol",
                "verification_rollup",
            ],
            summary=_merge_text(
                _summary_text(uncertainty_summary),
                _summary_text(method_protocol),
            ),
        )
    )
    blockers.extend(uncertainty_method_issues)
    if uncertainty_method_issues or uncertainty_formal_lines:
        bundle_suggestion_ids.append("uncertainty_method_bundle")

    software_issues: list[dict[str, Any]] = []
    missing_software_inputs = [
        label
        for label, present in (
            ("software_validation_traceability_matrix", bool(software_traceability)),
            ("requirement_design_code_test_links", bool(requirement_links)),
            ("validation_evidence_index", bool(validation_index)),
            ("audit_readiness_digest", bool(audit_digest)),
        )
        if not present
    ]
    if missing_software_inputs:
        software_issues.extend(
            _make_issue_rows(
                check_id="software_validation_audit_readiness",
                severity="blocker",
                title="software validation / audit inputs missing",
                items=[f"missing software audit input: {item}" for item in missing_software_inputs],
                source_artifacts=[
                    "software_validation_traceability_matrix",
                    "requirement_design_code_test_links",
                    "validation_evidence_index",
                    "audit_readiness_digest",
                ],
                category="software_validation",
            )
        )
    software_formal_lines = _text_list(
        [
            dict(audit_digest.get("digest") or {}).get("missing_evidence_summary"),
            dict(audit_digest.get("digest") or {}).get("reviewer_next_step_digest"),
            dict(software_rollup.get("digest") or {}).get("missing_evidence_summary"),
        ]
    )
    if not missing_software_inputs and software_formal_lines:
        formal_gaps.extend(
            _make_issue_rows(
                check_id="software_validation_audit_readiness",
                severity="missing_formal",
                title="software validation / audit formal closure still missing",
                items=software_formal_lines[:3],
                source_artifacts=[
                    "software_validation_traceability_matrix",
                    "validation_evidence_index",
                    "audit_readiness_digest",
                    "software_validation_rollup",
                ],
                category="software_validation",
            )
        )
    checks.append(
        _build_check_result(
            check_id="software_validation_audit_readiness",
            title="software validation / audit readiness closure",
            issues=software_issues,
            fallback_status="missing_formal" if software_formal_lines else "pass",
            source_artifacts=[
                "software_validation_traceability_matrix",
                "requirement_design_code_test_links",
                "validation_evidence_index",
                "audit_readiness_digest",
                "software_validation_rollup",
            ],
            summary=_merge_text(
                _summary_text(audit_digest),
                _summary_text(software_rollup),
            ),
        )
    )
    blockers.extend(software_issues)
    if software_issues or software_formal_lines:
        bundle_suggestion_ids.append("software_validation_audit_bundle")

    comparison_issues: list[dict[str, Any]] = []
    comparison_present = bool(
        comparison_pack or comparison_scope or comparison_digest_payload or comparison_rollup_payload
    )
    if not comparison_present:
        comparison_issues.extend(
            _make_issue_rows(
                check_id="comparison_pt_ilc_readiness",
                severity="blocker",
                title="comparison readiness missing",
                items=["comparison_evidence_pack / comparison_rollup not available"],
                source_artifacts=[
                    "comparison_evidence_pack",
                    "comparison_rollup",
                    "scope_comparison_view",
                ],
                category="comparison",
            )
        )
    if comparison_present and not pt_ilc:
        formal_gaps.extend(
            _make_issue_rows(
                check_id="comparison_pt_ilc_readiness",
                severity="missing_formal",
                title="PT / ILC readiness missing",
                items=["pt_ilc_registry not available"],
                source_artifacts=["comparison_rollup", "pt_ilc_registry"],
                category="comparison",
            )
        )
    comparison_formal_lines = _text_list(
        [
            dict(comparison_rollup_payload.get("digest") or {}).get("missing_evidence_summary"),
            dict(comparison_rollup_payload.get("digest") or {}).get("reviewer_next_step_digest"),
            dict(comparison_digest_payload.get("digest") or {}).get("missing_evidence_summary"),
        ]
    )
    if comparison_present and comparison_formal_lines:
        formal_gaps.extend(
            _make_issue_rows(
                check_id="comparison_pt_ilc_readiness",
                severity="missing_formal",
                title="comparison / PT-ILC future-stage evidence still missing",
                items=comparison_formal_lines[:3],
                source_artifacts=[
                    "comparison_rollup",
                    "comparison_digest",
                    "pt_ilc_registry",
                ],
                category="comparison",
            )
        )
    checks.append(
        _build_check_result(
            check_id="comparison_pt_ilc_readiness",
            title="comparison / PT / ILC readiness",
            issues=comparison_issues,
            fallback_status="missing_formal" if (not pt_ilc or comparison_formal_lines) else "pass",
            source_artifacts=[
                "comparison_evidence_pack",
                "comparison_rollup",
                "scope_comparison_view",
                "pt_ilc_registry",
            ],
            summary=_merge_text(
                _summary_text(comparison_rollup_payload),
                _summary_text(pt_ilc),
            ),
        )
    )
    blockers.extend(comparison_issues)
    if comparison_issues or not pt_ilc or comparison_formal_lines:
        bundle_suggestion_ids.append("comparison_pt_ilc_bundle")

    standards_rows = [
        dict(item)
        for item in list(standards_matrix.get("rows") or [])
        if isinstance(item, dict)
    ]
    standards_issues: list[dict[str, Any]] = []
    if not standards_matrix:
        standards_issues.extend(
            _make_issue_rows(
                check_id="standards_coverage_gaps",
                severity="blocker",
                title="standards coverage matrix missing",
                items=["stage3_standards_alignment_matrix not available"],
                source_artifacts=["stage3_standards_alignment_matrix"],
                category="standards_coverage",
            )
        )
    standards_gap_lines = _build_standards_gap_lines(standards_rows)
    if standards_gap_lines:
        formal_gaps.extend(
            _make_issue_rows(
                check_id="standards_coverage_gaps",
                severity="missing_formal",
                title="standards coverage gaps remain open",
                items=standards_gap_lines[:12],
                source_artifacts=["stage3_standards_alignment_matrix"],
                category="standards_coverage",
            )
        )
    checks.append(
        _build_check_result(
            check_id="standards_coverage_gaps",
            title="standards coverage gaps",
            issues=standards_issues,
            fallback_status="missing_formal" if standards_gap_lines else "pass",
            source_artifacts=["stage3_standards_alignment_matrix"],
            summary=_summary_text(standards_matrix),
        )
    )
    blockers.extend(standards_issues)

    sidecar_issues: list[dict[str, Any]] = []
    sidecar_payloads = {
        "sidecar_index_summary": sidecar_index,
        "review_copilot_payload": review_copilot,
        "model_governance_summary": model_governance,
    }
    sidecar_boundary_violations = [
        key
        for key, payload in sidecar_payloads.items()
        if payload
        and (
            bool(payload.get("main_chain_dependency", False))
            or bool(payload.get("primary_evidence_rewritten", False))
            or not bool(payload.get("not_real_acceptance_evidence", True))
        )
    ]
    if sidecar_boundary_violations:
        sidecar_issues.extend(
            _make_issue_rows(
                check_id="sidecar_ai_governance_boundary",
                severity="blocker",
                title="sidecar / AI governance boundary violated",
                items=[f"sidecar boundary violation: {item}" for item in sidecar_boundary_violations],
                source_artifacts=list(sidecar_boundary_violations),
                category="sidecar_boundary",
            )
        )
    elif any(sidecar_payloads.values()):
        warnings.extend(
            _make_issue_rows(
                check_id="sidecar_ai_governance_boundary",
                severity="warning",
                title="sidecar / AI remains reviewer-only",
                items=["sidecar / AI governance stays off main chain and cannot become primary evidence"],
                source_artifacts=[key for key, payload in sidecar_payloads.items() if payload],
                category="sidecar_boundary",
            )
        )
    checks.append(
        _build_check_result(
            check_id="sidecar_ai_governance_boundary",
            title="sidecar / AI governance off-main-chain boundary",
            issues=sidecar_issues,
            fallback_status="advisory" if any(sidecar_payloads.values()) else "pass",
            source_artifacts=[key for key, payload in sidecar_payloads.items() if payload],
            summary=_merge_text(
                _summary_text(sidecar_index),
                _summary_text(model_governance),
            ),
        )
    )
    blockers.extend(sidecar_issues)

    next_actions.extend(_split_digest_lines(dict(pre_run_gate.get("digest") or {}).get("reviewer_next_step_digest")))
    next_actions.extend(_split_digest_lines(dict(uncertainty_summary.get("digest") or {}).get("reviewer_next_step_digest")))
    next_actions.extend(_split_digest_lines(dict(method_protocol.get("digest") or {}).get("reviewer_next_step_digest")))
    next_actions.extend(_split_digest_lines(dict(audit_digest.get("digest") or {}).get("reviewer_next_step_digest")))
    next_actions.extend(_text_list(checklist.get("defer_to_stage3_real_validation") or []))
    next_actions.extend(_text_list(stage3_plan.get("validation_items") or [], field="title_text"))
    next_actions = _dedupe_text(next_actions)

    blockers = _dedupe_issue_rows(blockers)
    warnings = _dedupe_issue_rows(warnings)
    formal_gaps = _dedupe_issue_rows(formal_gaps)

    gate_level = _resolve_gate_level(
        blockers=blockers,
        formal_gaps=formal_gaps,
        warnings=warnings,
    )
    gate_level_display = _GATE_LEVEL_DISPLAY.get(gate_level, gate_level)

    required_evidence_categories = _dedupe_text(
        list(standards_matrix.get("required_evidence_categories") or [])
        + [
            label
            for label in (
                _VALIDATION_CATEGORY_LABELS.get(str(item.get("category") or ""))
                for item in list(stage3_plan.get("validation_items") or [])
                if isinstance(item, dict)
            )
            if str(label or "").strip()
        ]
    )
    missing_prerequisites = _dedupe_text([str(item.get("summary") or "").strip() for item in blockers + formal_gaps])

    suggested_validation_bundles = _build_suggested_validation_bundles(
        bundle_suggestion_ids=bundle_suggestion_ids,
        checks=checks,
    )
    stage3_real_validation_plan_draft_input = {
        "required_evidence_categories": list(required_evidence_categories),
        "missing_prerequisites": list(missing_prerequisites),
        "suggested_validation_bundles": suggested_validation_bundles,
    }

    review_surface = {
        "title_text": _TITLE_TEXT,
        "status_line": gate_level_display,
        "summary_text": _build_summary_line(
            gate_level_display=gate_level_display,
            blockers=blockers,
            warnings=warnings,
            formal_gaps=formal_gaps,
        ),
        "blocker_lines": [str(item.get("summary") or "").strip() for item in blockers],
        "warning_lines": [str(item.get("summary") or "").strip() for item in warnings],
        "unresolved_gap_lines": [str(item.get("summary") or "").strip() for item in formal_gaps],
        "suggested_next_action_lines": list(next_actions),
        "bridge_note_text": _BRIDGE_NOTE,
        "artifact_paths": {
            "engineering_isolation_gate_result": _artifact_output_path(
                normalized_run_dir,
                ENGINEERING_ISOLATION_GATE_RESULT_FILENAME,
            ),
            "engineering_isolation_gate_digest": _artifact_output_path(
                normalized_run_dir,
                ENGINEERING_ISOLATION_GATE_DIGEST_FILENAME,
            ),
            "engineering_isolation_blockers": _artifact_output_path(
                normalized_run_dir,
                ENGINEERING_ISOLATION_BLOCKERS_FILENAME,
            ),
            "engineering_isolation_warnings": _artifact_output_path(
                normalized_run_dir,
                ENGINEERING_ISOLATION_WARNINGS_FILENAME,
            ),
        },
        "compact_lines": [
            gate_level_display,
            f"blockers: {len(blockers)}",
            f"warnings: {len(warnings)}",
            f"unresolved gaps: {len(formal_gaps)}",
            f"suggested next actions: {len(next_actions)}",
            "reviewer/admission bridge only",
        ],
    }

    compact_panel = {
        "title_text": _TITLE_TEXT,
        "gate_level": gate_level,
        "gate_level_display": gate_level_display,
        "summary_line": review_surface["summary_text"],
        "status_line": gate_level_display,
        "blocker_count": len(blockers),
        "warning_count": len(warnings),
        "unresolved_gap_count": len(formal_gaps),
        "next_action_count": len(next_actions),
        "bridge_note_text": _BRIDGE_NOTE,
        "suggested_next_action_lines": list(next_actions[:5]),
        "boundary_summary": " | ".join(_BOUNDARY_STATEMENTS),
    }

    generated_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    artifact_paths = dict(review_surface.get("artifact_paths") or {})
    result_payload = {
        "schema_version": "engineering-isolation-gate-v1",
        "artifact_type": ENGINEERING_ISOLATION_GATE_ARTIFACT_KEY,
        "generated_at": generated_at,
        "run_id": normalized_run_id,
        "run_dir": normalized_run_dir,
        "phase": "step2_engineering_isolation_bridge",
        "mode": "simulation_only",
        "overall_status": gate_level,
        "gate_level": gate_level,
        "gate_level_display": gate_level_display,
        "reviewer_bridge_only": True,
        "not_formal_admission_approval": True,
        "not_real_acceptance_evidence": True,
        "default_execution_chain_unchanged": True,
        "real_device_touched": False,
        "real_acceptance_output": False,
        "boundary_statements": list(_BOUNDARY_STATEMENTS),
        "checks": checks,
        "blocker_count": len(blockers),
        "warning_count": len(warnings),
        "formal_gap_count": len(formal_gaps),
        "blockers": blockers,
        "warnings": warnings,
        "unresolved_gaps": formal_gaps,
        "suggested_next_actions": list(next_actions),
        "evidence_coverage": {
            "total_checks": len(checks),
            "counts": _count_check_statuses(checks),
            "required_evidence_categories": list(required_evidence_categories),
            "standards_gap_count": len(standards_gap_lines),
        },
        "stage3_real_validation_plan_draft_input": stage3_real_validation_plan_draft_input,
        "source_artifact_refs": source_artifact_refs,
        "artifact_paths": artifact_paths,
        "review_surface": review_surface,
        "compact_panel": compact_panel,
        "note": _BRIDGE_NOTE,
    }

    blockers_payload = {
        "schema_version": "engineering-isolation-gate-v1",
        "artifact_type": ENGINEERING_ISOLATION_BLOCKERS_ARTIFACT_KEY,
        "generated_at": generated_at,
        "run_id": normalized_run_id,
        "gate_level": gate_level,
        "gate_level_display": gate_level_display,
        "items": blockers,
        "count": len(blockers),
        "reviewer_bridge_only": True,
        "not_formal_admission_approval": True,
        "not_real_acceptance_evidence": True,
        "artifact_paths": artifact_paths,
    }
    warnings_payload = {
        "schema_version": "engineering-isolation-gate-v1",
        "artifact_type": ENGINEERING_ISOLATION_WARNINGS_ARTIFACT_KEY,
        "generated_at": generated_at,
        "run_id": normalized_run_id,
        "gate_level": gate_level,
        "gate_level_display": gate_level_display,
        "items": warnings,
        "count": len(warnings),
        "reviewer_bridge_only": True,
        "not_formal_admission_approval": True,
        "not_real_acceptance_evidence": True,
        "artifact_paths": artifact_paths,
    }
    digest_markdown = _render_digest_markdown(
        result_payload=result_payload,
        blockers=blockers,
        warnings=warnings,
        formal_gaps=formal_gaps,
        stage3_draft_input=stage3_real_validation_plan_draft_input,
    )

    return {
        "engineering_isolation_gate_result": result_payload,
        "engineering_isolation_blockers": blockers_payload,
        "engineering_isolation_warnings": warnings_payload,
        "engineering_isolation_gate_digest_markdown": digest_markdown,
        "engineering_isolation_gate_compact_panel": compact_panel,
    }


def _build_source_artifact_refs(
    *,
    run_dir: str,
    payloads: dict[str, dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    rows: dict[str, dict[str, Any]] = {}
    for key, payload in payloads.items():
        normalized = dict(payload or {})
        artifact_paths = dict(normalized.get("artifact_paths") or {})
        path_text = str(
            artifact_paths.get(key)
            or artifact_paths.get(f"{key}_json")
            or artifact_paths.get(f"{key}_artifact")
            or _artifact_output_path(run_dir, _KNOWN_ARTIFACT_FILENAMES.get(key, f"{key}.json"))
        ).strip()
        if not normalized and not path_text:
            continue
        rows[key] = {
            "artifact_type": str(normalized.get("artifact_type") or key),
            "path": path_text,
            "summary_text": _summary_text(normalized),
            "overall_status": str(
                normalized.get("overall_status")
                or normalized.get("gate_level")
                or dict(normalized.get("digest") or {}).get("readiness_status")
                or ""
            ).strip(),
        }
    return rows


def _build_check_result(
    *,
    check_id: str,
    title: str,
    issues: list[dict[str, Any]],
    fallback_status: str,
    source_artifacts: list[str],
    summary: str,
) -> dict[str, Any]:
    status = fallback_status
    if any(str(item.get("severity") or "") == "blocker" for item in issues):
        status = "blocker"
    elif any(str(item.get("severity") or "") == "missing_formal" for item in issues):
        status = "missing_formal"
    elif any(str(item.get("severity") or "") == "warning" for item in issues):
        status = "warning"
    elif fallback_status == "advisory":
        status = "advisory"
    return {
        "check_id": check_id,
        "title": title,
        "status": status,
        "summary": str(summary or title).strip(),
        "source_artifacts": list(source_artifacts),
    }


def _make_issue_rows(
    *,
    check_id: str,
    severity: str,
    title: str,
    items: list[str],
    source_artifacts: list[str],
    category: str,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for index, item in enumerate(_dedupe_text(items), start=1):
        rows.append(
            {
                "issue_id": f"{check_id}:{severity}:{index}",
                "check_id": check_id,
                "severity": severity,
                "category": category,
                "title": title,
                "summary": str(item).strip(),
                "source_artifacts": list(source_artifacts),
            }
        )
    return rows


def _dedupe_issue_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    deduped: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str]] = set()
    for item in rows:
        payload = dict(item or {})
        key = (
            str(payload.get("check_id") or ""),
            str(payload.get("severity") or ""),
            str(payload.get("summary") or ""),
        )
        if key in seen:
            continue
        seen.add(key)
        deduped.append(payload)
    return deduped


def _build_standards_gap_lines(rows: list[dict[str, Any]]) -> list[str]:
    gap_lines: list[str] = []
    for row in rows:
        readiness_status = str(row.get("readiness_status") or "").strip()
        gap_note = str(row.get("gap_note") or "").strip()
        standard_text = str(
            row.get("standard_id_or_family") or row.get("standard_family") or row.get("mapping_id") or "--"
        ).strip()
        required_categories = _dedupe_text(list(row.get("required_evidence_categories") or []))
        if gap_note:
            gap_lines.append(f"{standard_text}: {gap_note}")
        elif readiness_status and readiness_status not in {"mapping_ready", "ready", "ok"}:
            if required_categories:
                gap_lines.append(
                    f"{standard_text}: {readiness_status} | required evidence: {', '.join(required_categories)}"
                )
            else:
                gap_lines.append(f"{standard_text}: {readiness_status}")
    return _dedupe_text(gap_lines)


def _build_suggested_validation_bundles(
    *,
    bundle_suggestion_ids: list[str],
    checks: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    selected_ids = _dedupe_text(bundle_suggestion_ids)
    if not selected_ids:
        selected_ids = [
            "scope_decision_bundle",
            "asset_certificate_bundle",
            "uncertainty_method_bundle",
            "software_validation_audit_bundle",
            "comparison_pt_ilc_bundle",
            "device_acceptance_bundle",
        ]
    rows: list[dict[str, Any]] = []
    active_check_map = {str(item.get("check_id") or ""): dict(item) for item in checks if isinstance(item, dict)}
    for bundle_id in selected_ids:
        payload = dict(_DEFAULT_BUNDLE_SUGGESTIONS.get(bundle_id) or {})
        if not payload:
            continue
        linked_checks = [
            check_id
            for check_id in (
                "scope_decision_binding" if bundle_id == "scope_decision_bundle" else "",
                "asset_certificate_intermediate_checks" if bundle_id == "asset_certificate_bundle" else "",
                "uncertainty_method_confirmation" if bundle_id == "uncertainty_method_bundle" else "",
                "software_validation_audit_readiness" if bundle_id == "software_validation_audit_bundle" else "",
                "comparison_pt_ilc_readiness" if bundle_id == "comparison_pt_ilc_bundle" else "",
                "reviewer_bridge_bundle" if bundle_id == "device_acceptance_bundle" else "",
            )
            if check_id and check_id in active_check_map
        ]
        payload["linked_checks"] = linked_checks
        rows.append(payload)
    return rows


def _resolve_gate_level(
    *,
    blockers: list[dict[str, Any]],
    formal_gaps: list[dict[str, Any]],
    warnings: list[dict[str, Any]],
) -> str:
    if blockers:
        return BLOCKED_FOR_ENGINEERING_ISOLATION
    if formal_gaps:
        return MISSING_FORMAL_EVIDENCE
    if warnings:
        return ADVISORY_ONLY_OK
    return PASS_FOR_REVIEWER_BRIDGE


def _count_check_statuses(checks: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for item in checks:
        status = str(item.get("status") or "").strip()
        if not status:
            continue
        counts[status] = counts.get(status, 0) + 1
    return counts


def _summary_text(payload: dict[str, Any]) -> str:
    normalized = dict(payload or {})
    digest = dict(normalized.get("digest") or {})
    review_surface = dict(normalized.get("review_surface") or {})
    return str(
        digest.get("summary")
        or review_surface.get("summary_text")
        or normalized.get("summary_line")
        or normalized.get("summary")
        or ""
    ).strip()


def _payload_has_examples(payload: dict[str, Any]) -> bool:
    normalized = dict(payload or {})
    for key in (
        "cases",
        "case_rows",
        "sample_cases",
        "golden_cases",
        "validation_items",
        "checklist_items",
        "rows",
        "entries",
        "summary_lines",
        "detail_lines",
    ):
        value = normalized.get(key)
        if isinstance(value, (list, tuple)) and value:
            return True
        if isinstance(value, dict) and value:
            return True
    review_surface = dict(normalized.get("review_surface") or {})
    if list(review_surface.get("summary_lines") or []) or list(review_surface.get("detail_lines") or []):
        return True
    digest = dict(normalized.get("digest") or {})
    for key in (
        "current_coverage_summary",
        "matrix_completeness_summary",
        "protocol_overview_summary",
        "uncertainty_overview_summary",
    ):
        if str(digest.get(key) or "").strip():
            return True
    return False


def _split_digest_lines(value: Any) -> list[str]:
    text = str(value or "").strip()
    if not text:
        return []
    rows: list[str] = []
    for item in text.split(" | "):
        normalized = str(item or "").strip()
        if normalized:
            rows.append(normalized)
    return rows


def _dedupe_text(values: list[Any], field: str = "") -> list[str]:
    rows: list[str] = []
    for item in list(values or []):
        text = ""
        if field and isinstance(item, dict):
            text = str(dict(item or {}).get(field) or "").strip()
        else:
            text = str(item or "").strip()
        if text and text not in rows:
            rows.append(text)
    return rows


def _text_list(values: Any, field: str = "") -> list[str]:
    if isinstance(values, str):
        return _split_digest_lines(values)
    if isinstance(values, dict) and field:
        return _dedupe_text([values], field=field)
    if isinstance(values, (list, tuple)):
        return _dedupe_text(list(values), field=field)
    return []


def _merge_text(*parts: Any) -> str:
    rows = [str(item or "").strip() for item in parts if str(item or "").strip()]
    return " | ".join(_dedupe_text(rows))


def _first_non_empty(*values: Any) -> str:
    for item in values:
        text = str(item or "").strip()
        if text:
            return text
    return ""


def _artifact_output_path(run_dir: str, filename: str) -> str:
    if not run_dir:
        return filename
    return str(Path(run_dir) / filename)


def _build_summary_line(
    *,
    gate_level_display: str,
    blockers: list[dict[str, Any]],
    warnings: list[dict[str, Any]],
    formal_gaps: list[dict[str, Any]],
) -> str:
    return (
        f"{gate_level_display} | blockers {len(blockers)} | warnings {len(warnings)} | "
        f"unresolved gaps {len(formal_gaps)} | reviewer/admission bridge only"
    )


def _render_digest_markdown(
    *,
    result_payload: dict[str, Any],
    blockers: list[dict[str, Any]],
    warnings: list[dict[str, Any]],
    formal_gaps: list[dict[str, Any]],
    stage3_draft_input: dict[str, Any],
) -> str:
    review_surface = dict(result_payload.get("review_surface") or {})
    artifact_paths = dict(result_payload.get("artifact_paths") or {})
    lines = [
        f"# {_TITLE_TEXT}",
        "",
        f"> {_BRIDGE_NOTE}",
        "",
        "## Current Conclusion",
        f"- {str(review_surface.get('status_line') or result_payload.get('gate_level_display') or '--')}",
        f"- {str(review_surface.get('summary_text') or '--')}",
        "",
        "## Blockers",
    ]
    blocker_lines = [str(item.get("summary") or "").strip() for item in blockers]
    warning_lines = [str(item.get("summary") or "").strip() for item in warnings]
    gap_lines = [str(item.get("summary") or "").strip() for item in formal_gaps]
    next_action_lines = _text_list(review_surface.get("suggested_next_action_lines") or [])
    if blocker_lines:
        lines.extend(f"- {item}" for item in blocker_lines)
    else:
        lines.append("- none")
    lines.extend(
        [
            "",
            "## Warnings",
        ]
    )
    if warning_lines:
        lines.extend(f"- {item}" for item in warning_lines)
    else:
        lines.append("- none")
    lines.extend(
        [
            "",
            "## Unresolved Gaps",
        ]
    )
    if gap_lines:
        lines.extend(f"- {item}" for item in gap_lines)
    else:
        lines.append("- none")
    lines.extend(
        [
            "",
            "## Suggested Next Actions",
        ]
    )
    if next_action_lines:
        lines.extend(f"- {item}" for item in next_action_lines)
    else:
        lines.append("- none")
    lines.extend(
        [
            "",
            "## Stage 3 Draft Input",
            "- required evidence categories:",
        ]
    )
    required_categories = _text_list(stage3_draft_input.get("required_evidence_categories") or [])
    if required_categories:
        lines.extend(f"  - {item}" for item in required_categories)
    else:
        lines.append("  - none")
    lines.append("- missing prerequisites:")
    missing_prerequisites = _text_list(stage3_draft_input.get("missing_prerequisites") or [])
    if missing_prerequisites:
        lines.extend(f"  - {item}" for item in missing_prerequisites)
    else:
        lines.append("  - none")
    lines.append("- suggested validation bundles:")
    bundles = [
        dict(item)
        for item in list(stage3_draft_input.get("suggested_validation_bundles") or [])
        if isinstance(item, dict)
    ]
    if bundles:
        lines.extend(
            f"  - {str(item.get('title') or item.get('bundle_id') or '--')}: {str(item.get('summary') or '--')}"
            for item in bundles
        )
    else:
        lines.append("  - none")
    lines.extend(
        [
            "",
            "## Boundary",
            *[f"- {item}" for item in _BOUNDARY_STATEMENTS],
            "",
            "## Artifacts",
            f"- result: {str(artifact_paths.get('engineering_isolation_gate_result') or ENGINEERING_ISOLATION_GATE_RESULT_FILENAME)}",
            f"- digest: {str(artifact_paths.get('engineering_isolation_gate_digest') or ENGINEERING_ISOLATION_GATE_DIGEST_FILENAME)}",
            f"- blockers: {str(artifact_paths.get('engineering_isolation_blockers') or ENGINEERING_ISOLATION_BLOCKERS_FILENAME)}",
            f"- warnings: {str(artifact_paths.get('engineering_isolation_warnings') or ENGINEERING_ISOLATION_WARNINGS_FILENAME)}",
        ]
    )
    return "\n".join(lines).rstrip() + "\n"
