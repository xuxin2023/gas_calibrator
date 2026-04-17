from __future__ import annotations

from gas_calibrator.v2.core.step2_closeout_bundle_builder import (
    STEP2_CLOSEOUT_BUNDLE_ARTIFACT_TYPE,
    STEP2_CLOSEOUT_BUNDLE_FILENAME,
    STEP2_CLOSEOUT_EVIDENCE_INDEX_FILENAME,
    STEP2_CLOSEOUT_SUMMARY_FILENAME,
    STEP2_CLOSEOUT_TITLE,
    build_step2_closeout_bundle,
)


def _payload(key: str, *, path: str | None = None) -> dict[str, object]:
    artifact_path = path or f"D:/tmp/{key}.json"
    return {
        "artifact_type": key,
        "summary_line": f"{key} summary",
        "evidence_source": "simulated",
        "reviewer_only": True,
        "readiness_mapping_only": True,
        "not_real_acceptance_evidence": True,
        "not_ready_for_formal_claim": True,
        "artifact_paths": {key: artifact_path},
        "digest": {"summary": f"{key} digest"},
    }


def _bundle() -> dict[str, object]:
    return build_step2_closeout_bundle(
        run_id="closeout-run",
        run_dir="D:/tmp/closeout-run",
        scope_definition_pack=_payload("scope_definition_pack"),
        decision_rule_profile={
            **_payload("decision_rule_profile"),
            "conformity_statement_profile": _payload("conformity_statement_profile"),
        },
        reference_asset_registry=_payload("reference_asset_registry"),
        certificate_lifecycle_summary=_payload("certificate_lifecycle_summary"),
        pre_run_readiness_gate=_payload("pre_run_readiness_gate"),
        uncertainty_report_pack=_payload("uncertainty_report_pack"),
        uncertainty_rollup=_payload("uncertainty_rollup"),
        method_confirmation_protocol=_payload("method_confirmation_protocol"),
        verification_rollup=_payload("verification_rollup"),
        software_validation_traceability_matrix=_payload("software_validation_traceability_matrix"),
        requirement_design_code_test_links=_payload("requirement_design_code_test_links"),
        validation_evidence_index=_payload("validation_evidence_index"),
        change_impact_summary=_payload("change_impact_summary"),
        rollback_readiness_summary=_payload("rollback_readiness_summary"),
        release_manifest=_payload("release_manifest"),
        release_scope_summary=_payload("release_scope_summary"),
        release_boundary_digest=_payload("release_boundary_digest"),
        release_evidence_pack_index=_payload("release_evidence_pack_index"),
        release_validation_manifest=_payload("release_validation_manifest"),
        software_validation_rollup=_payload("software_validation_rollup"),
        audit_readiness_digest=_payload("audit_readiness_digest"),
        comparison_evidence_pack=_payload("comparison_evidence_pack"),
        scope_comparison_view=_payload("scope_comparison_view"),
        comparison_digest=_payload("comparison_digest"),
        comparison_rollup=_payload("comparison_rollup"),
        step2_closeout_digest=_payload("step2_closeout_digest"),
    )


def test_step2_closeout_bundle_outputs_required_surfaces() -> None:
    snapshot = _bundle()

    bundle = dict(snapshot["step2_closeout_bundle"])
    evidence_index = dict(snapshot["step2_closeout_evidence_index"])
    summary_markdown = str(snapshot["step2_closeout_summary_markdown"])
    compact = dict(snapshot["step2_closeout_compact_section"])

    assert bundle["artifact_type"] == STEP2_CLOSEOUT_BUNDLE_ARTIFACT_TYPE
    assert bundle["artifact_paths"]["step2_closeout_bundle"].endswith(STEP2_CLOSEOUT_BUNDLE_FILENAME)
    assert bundle["artifact_paths"]["step2_closeout_evidence_index"].endswith(STEP2_CLOSEOUT_EVIDENCE_INDEX_FILENAME)
    assert bundle["artifact_paths"]["step2_closeout_summary_markdown"].endswith(STEP2_CLOSEOUT_SUMMARY_FILENAME)
    assert evidence_index["artifact_type"] == "step2_closeout_evidence_index"
    assert summary_markdown.startswith(f"# {STEP2_CLOSEOUT_TITLE}")
    assert compact["summary_key"] == "step2_closeout"


def test_step2_closeout_bundle_enforces_reviewer_readiness_non_claim_boundary() -> None:
    bundle = dict(_bundle()["step2_closeout_bundle"])

    assert bundle["reviewer_only"] is True
    assert bundle["readiness_mapping_only"] is True
    assert bundle["not_real_acceptance_evidence"] is True
    assert bundle["not_ready_for_formal_claim"] is True
    assert bundle["file_artifact_first_preserved"] is True
    assert bundle["main_chain_dependency"] is False
    assert bundle["primary_evidence_rewritten"] is False


def test_step2_closeout_bundle_builds_stage3_bridge_candidates_when_required_inputs_present() -> None:
    bundle = dict(_bundle()["step2_closeout_bundle"])

    assert bundle["missing_evidence_categories"] == []
    assert "engineering_isolation_admission_bridge" in list(bundle["bridge_to_stage3_candidates"])
    assert "reviewer/readiness/non-claim" in str(bundle["summary_line"])


def test_step2_closeout_bundle_keeps_sidecar_optional_and_stable_when_not_injected() -> None:
    snapshot = _bundle()
    bundle = dict(snapshot["step2_closeout_bundle"])
    compact = dict(snapshot["step2_closeout_compact_section"])
    evidence_index = dict(snapshot["step2_closeout_evidence_index"])

    assert bundle["sidecar_injected"] is False
    assert "sidecar" not in list(bundle["missing_evidence_categories"])
    assert bundle["main_chain_dependency"] is False
    assert bundle["file_artifact_first_preserved"] is True
    assert any("sidecar \u672a\u6ce8\u5165" in str(item) for item in list(bundle["info_items"]))
    assert compact["main_chain_dependency"] is False
    assert compact["file_artifact_first_preserved"] is True
    assert any(
        str(item.get("category_id") or "") == "sidecar" and not bool(item.get("available"))
        for item in list(evidence_index["entries"])
    )
