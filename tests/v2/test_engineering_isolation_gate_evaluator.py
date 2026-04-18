from pathlib import Path

from gas_calibrator.v2.core.engineering_isolation_gate_evaluator import (
    ADVISORY_ONLY_OK,
    BLOCKED_FOR_ENGINEERING_ISOLATION,
    ENGINEERING_ISOLATION_BLOCKERS_FILENAME,
    ENGINEERING_ISOLATION_GATE_DIGEST_FILENAME,
    ENGINEERING_ISOLATION_GATE_RESULT_FILENAME,
    ENGINEERING_ISOLATION_WARNINGS_FILENAME,
    MISSING_FORMAL_EVIDENCE,
    PASS_FOR_REVIEWER_BRIDGE,
    build_engineering_isolation_gate_evaluator,
)


def _base_kwargs(run_dir: Path) -> dict:
    return {
        "run_id": "run-gate-001",
        "run_dir": run_dir,
        "stage_admission_review_pack": {
            "summary": "stage admission review pack ready",
            "execute_now_in_step2_tail": ["keep stage admission digest aligned"],
        },
        "engineering_isolation_admission_checklist": {
            "summary": "engineering isolation checklist ready",
            "defer_to_stage3_real_validation": ["draft stage3 validation bundle"],
        },
        "pre_run_readiness_gate": {
            "summary": "pre-run readiness ready",
            "digest": {"reviewer_next_step_digest": "keep intermediate checks current"},
        },
        "step2_closeout_bundle": {
            "summary_line": "step2 closeout bundle ready",
            "bridge_to_stage3_candidates": ["carry closeout bundle into reviewer bridge"],
        },
        "step2_closeout_compact_section": {
            "summary_line": "step2 closeout compact ready",
        },
        "stage3_standards_alignment_matrix": {
            "summary": "standards matrix ready",
            "required_evidence_categories": [
                "scope / decision rule binding",
                "software validation / audit readiness",
            ],
            "rows": [
                {
                    "standard_family": "ISO/IEC 17025",
                    "readiness_status": "mapping_ready",
                    "required_evidence_categories": ["scope / decision rule binding"],
                }
            ],
        },
        "stage3_real_validation_plan": {
            "validation_items": [
                {"title_text": "future device acceptance bundle"},
            ]
        },
        "scope_definition_pack": {"summary": "scope definition ready"},
        "decision_rule_profile": {
            "summary": "decision rule ready",
            "conformity_statement_profile": {"summary": "conformity profile ready"},
        },
        "reference_asset_registry": {"summary": "reference assets ready"},
        "certificate_lifecycle_summary": {"summary": "certificate lifecycle ready"},
        "uncertainty_report_pack": {
            "summary": "uncertainty pack ready",
            "cases": [{"case_id": "u-1"}],
        },
        "uncertainty_rollup": {
            "summary": "uncertainty rollup ready",
            "rows": [{"row_id": "u-row-1"}],
        },
        "uncertainty_method_readiness_summary": {"summary": "uncertainty readiness ready"},
        "method_confirmation_protocol": {
            "summary": "method confirmation ready",
            "rows": [{"row_id": "m-row-1"}],
        },
        "verification_rollup": {
            "summary": "verification rollup ready",
            "rows": [{"row_id": "v-row-1"}],
        },
        "software_validation_traceability_matrix": {
            "summary": "software traceability ready",
        },
        "requirement_design_code_test_links": {
            "summary": "requirement/design/code/test linkage ready",
        },
        "validation_evidence_index": {"summary": "validation evidence index ready"},
        "software_validation_rollup": {"summary": "software validation rollup ready"},
        "audit_readiness_digest": {"summary": "audit readiness ready"},
        "pt_ilc_registry": {"summary": "pt ilc readiness ready"},
        "comparison_evidence_pack": {"summary": "comparison evidence ready"},
        "scope_comparison_view": {"summary": "scope comparison ready"},
        "comparison_digest": {"summary": "comparison digest ready"},
        "comparison_rollup": {"summary": "comparison rollup ready"},
    }


def test_engineering_isolation_gate_evaluator_passes_reviewer_bridge_with_offline_boundaries(
    tmp_path: Path,
) -> None:
    payload = build_engineering_isolation_gate_evaluator(**_base_kwargs(tmp_path))

    result = payload["engineering_isolation_gate_result"]
    draft_input = result["stage3_real_validation_plan_draft_input"]

    assert result["gate_level"] == PASS_FOR_REVIEWER_BRIDGE
    assert result["blocker_count"] == 0
    assert result["warning_count"] == 0
    assert result["formal_gap_count"] == 0
    assert result["reviewer_bridge_only"] is True
    assert result["not_formal_admission_approval"] is True
    assert result["not_real_acceptance_evidence"] is True
    assert result["default_execution_chain_unchanged"] is True
    assert result["real_device_touched"] is False
    assert result["real_acceptance_output"] is False
    assert result["review_surface"]["artifact_paths"]["engineering_isolation_gate_result"].endswith(
        ENGINEERING_ISOLATION_GATE_RESULT_FILENAME
    )
    assert result["review_surface"]["artifact_paths"]["engineering_isolation_gate_digest"].endswith(
        ENGINEERING_ISOLATION_GATE_DIGEST_FILENAME
    )
    assert result["review_surface"]["artifact_paths"]["engineering_isolation_blockers"].endswith(
        ENGINEERING_ISOLATION_BLOCKERS_FILENAME
    )
    assert result["review_surface"]["artifact_paths"]["engineering_isolation_warnings"].endswith(
        ENGINEERING_ISOLATION_WARNINGS_FILENAME
    )
    assert draft_input["required_evidence_categories"]
    assert draft_input["missing_prerequisites"] == []
    assert draft_input["suggested_validation_bundles"]


def test_engineering_isolation_gate_evaluator_classifies_missing_scope_binding_as_blocker(
    tmp_path: Path,
) -> None:
    kwargs = _base_kwargs(tmp_path)
    kwargs.pop("scope_definition_pack")

    payload = build_engineering_isolation_gate_evaluator(**kwargs)
    result = payload["engineering_isolation_gate_result"]

    assert result["gate_level"] == BLOCKED_FOR_ENGINEERING_ISOLATION
    assert result["blocker_count"] >= 1
    assert any(
        "missing binding: scope_definition_pack" in str(item.get("summary") or "")
        for item in list(result["blockers"] or [])
    )
    assert any(
        str(item.get("check_id") or "") == "scope_decision_binding"
        for item in list(result["blockers"] or [])
    )


def test_engineering_isolation_gate_evaluator_classifies_standards_gap_as_missing_formal_evidence(
    tmp_path: Path,
) -> None:
    kwargs = _base_kwargs(tmp_path)
    kwargs["stage3_standards_alignment_matrix"] = {
        "summary": "standards matrix needs formal closure",
        "required_evidence_categories": ["scope / decision rule binding"],
        "rows": [
            {
                "standard_family": "ISO/IEC 17025",
                "readiness_status": "gap_present",
                "gap_note": "formal device acceptance evidence still missing",
                "required_evidence_categories": ["device acceptance"],
            }
        ],
    }

    payload = build_engineering_isolation_gate_evaluator(**kwargs)
    result = payload["engineering_isolation_gate_result"]

    assert result["gate_level"] == MISSING_FORMAL_EVIDENCE
    assert result["blocker_count"] == 0
    assert result["formal_gap_count"] >= 1
    assert any(
        "formal device acceptance evidence still missing" in str(item.get("summary") or "")
        for item in list(result["unresolved_gaps"] or [])
    )


def test_engineering_isolation_gate_evaluator_classifies_sidecar_boundary_as_advisory_only(
    tmp_path: Path,
) -> None:
    kwargs = _base_kwargs(tmp_path)
    kwargs["sidecar_index_summary"] = {
        "summary": "sidecar summary ready",
        "not_real_acceptance_evidence": True,
        "main_chain_dependency": False,
        "primary_evidence_rewritten": False,
    }

    payload = build_engineering_isolation_gate_evaluator(**kwargs)
    result = payload["engineering_isolation_gate_result"]
    warnings = payload["engineering_isolation_warnings"]

    assert result["gate_level"] == ADVISORY_ONLY_OK
    assert result["blocker_count"] == 0
    assert result["formal_gap_count"] == 0
    assert result["warning_count"] >= 1
    assert warnings["count"] >= 1
    assert any(
        str(item.get("check_id") or "") == "sidecar_ai_governance_boundary"
        for item in list(result["warnings"] or [])
    )
