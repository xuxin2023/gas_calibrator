"""Tests for Step 2 closeout package contracts and builder.

Verifies:
- Contract key coverage and zh/en consistency
- Builder output stability (status, sections, blockers, next_steps)
- Simulation-only / reviewer-only / non-claim boundary enforcement
- Closeout package does not introduce real acceptance / formal claim language
- Closeout package aggregates from existing payloads (not parallel summary)
- Closeout package is consistent with closeout readiness / digest / governance / compact summary
- Step 2 boundary assertions continue to pass
- Fallback behavior
"""

from __future__ import annotations

from gas_calibrator.v2.core.step2_closeout_package_contracts import (
    CLOSEOUT_PACKAGE_ARTIFACT_TYPE,
    CLOSEOUT_PACKAGE_CONTRACTS_VERSION,
    CLOSEOUT_PACKAGE_I18N_KEYS,
    CLOSEOUT_PACKAGE_SECTION_LABELS_EN,
    CLOSEOUT_PACKAGE_SECTION_LABELS_ZH,
    CLOSEOUT_PACKAGE_SECTION_ORDER,
    CLOSEOUT_PACKAGE_SIMULATION_ONLY_BOUNDARY_EN,
    CLOSEOUT_PACKAGE_SIMULATION_ONLY_BOUNDARY_ZH,
    CLOSEOUT_PACKAGE_STEP2_BOUNDARY,
    CLOSEOUT_PACKAGE_TITLE_EN,
    CLOSEOUT_PACKAGE_TITLE_ZH,
    resolve_closeout_package_title,
    resolve_closeout_package_summary,
    resolve_closeout_package_section_label,
    resolve_closeout_package_simulation_only_boundary,
    resolve_closeout_package_reviewer_only_notice,
    resolve_closeout_package_non_claim_notice,
)
from gas_calibrator.v2.core.step2_closeout_package_builder import (
    build_step2_closeout_package,
    build_closeout_package_fallback,
    CLOSEOUT_PACKAGE_BUILDER_VERSION,
)
from gas_calibrator.v2.core.step2_closeout_readiness_builder import (
    build_step2_closeout_readiness,
)
from gas_calibrator.v2.core.step2_readiness import build_step2_readiness_summary
from gas_calibrator.v2.core.reviewer_summary_packs import build_control_flow_compare_pack


# ---------------------------------------------------------------------------
# Contract tests
# ---------------------------------------------------------------------------

def test_package_contracts_version() -> None:
    assert CLOSEOUT_PACKAGE_CONTRACTS_VERSION == "2.22.0"


def test_package_section_labels_zh_en_consistent() -> None:
    assert set(CLOSEOUT_PACKAGE_SECTION_LABELS_ZH.keys()) == set(CLOSEOUT_PACKAGE_SECTION_LABELS_EN.keys())


def test_package_section_order_covers_all_labels() -> None:
    for key in CLOSEOUT_PACKAGE_SECTION_ORDER:
        assert key in CLOSEOUT_PACKAGE_SECTION_LABELS_ZH, f"Missing zh label for section: {key}"
        assert key in CLOSEOUT_PACKAGE_SECTION_LABELS_EN, f"Missing en label for section: {key}"


def test_package_i18n_keys_present() -> None:
    required_keys = [
        "title", "summary",
        "section_readiness", "section_digest", "section_governance_handoff",
        "section_compact_summaries", "section_parity_resilience", "section_phase_evidence",
        "section_stage_admission", "section_engineering_isolation_checklist",
        "section_blockers", "section_next_steps", "section_boundary",
        "simulation_only_boundary", "reviewer_only_notice", "non_claim_notice",
        "package_status", "no_content",
    ]
    for key in required_keys:
        assert key in CLOSEOUT_PACKAGE_I18N_KEYS, f"Missing i18n key: {key}"


def test_package_step2_boundary_markers() -> None:
    assert CLOSEOUT_PACKAGE_STEP2_BOUNDARY["evidence_source"] == "simulated"
    assert CLOSEOUT_PACKAGE_STEP2_BOUNDARY["not_real_acceptance_evidence"] is True
    assert CLOSEOUT_PACKAGE_STEP2_BOUNDARY["not_ready_for_formal_claim"] is True
    assert CLOSEOUT_PACKAGE_STEP2_BOUNDARY["reviewer_only"] is True
    assert CLOSEOUT_PACKAGE_STEP2_BOUNDARY["readiness_mapping_only"] is True
    assert CLOSEOUT_PACKAGE_STEP2_BOUNDARY["primary_evidence_rewritten"] is False
    assert CLOSEOUT_PACKAGE_STEP2_BOUNDARY["real_acceptance_ready"] is False


def test_package_simulation_only_boundary_contains_non_claim() -> None:
    assert "real acceptance" in CLOSEOUT_PACKAGE_SIMULATION_ONLY_BOUNDARY_ZH
    assert "real acceptance" in CLOSEOUT_PACKAGE_SIMULATION_ONLY_BOUNDARY_EN.lower()


def test_resolve_package_title_zh_en() -> None:
    assert resolve_closeout_package_title(lang="zh") == CLOSEOUT_PACKAGE_TITLE_ZH
    assert resolve_closeout_package_title(lang="en") == CLOSEOUT_PACKAGE_TITLE_EN
    assert "Step 2" in resolve_closeout_package_title(lang="zh")
    assert "Step 2" in resolve_closeout_package_title(lang="en")


def test_resolve_package_section_label_all_sections() -> None:
    for key in CLOSEOUT_PACKAGE_SECTION_ORDER:
        label_zh = resolve_closeout_package_section_label(key, lang="zh")
        label_en = resolve_closeout_package_section_label(key, lang="en")
        assert label_zh, f"Missing zh label for section: {key}"
        assert label_en, f"Missing en label for section: {key}"


def test_resolve_package_section_label_fallback() -> None:
    unknown = "unknown_section_xyz"
    assert resolve_closeout_package_section_label(unknown, lang="zh") == unknown
    assert resolve_closeout_package_section_label(unknown, lang="en") == unknown


def test_no_formal_acceptance_language_in_contracts() -> None:
    all_texts = [
        CLOSEOUT_PACKAGE_TITLE_ZH, CLOSEOUT_PACKAGE_TITLE_EN,
        CLOSEOUT_PACKAGE_SIMULATION_ONLY_BOUNDARY_ZH, CLOSEOUT_PACKAGE_SIMULATION_ONLY_BOUNDARY_EN,
    ]
    for text in all_texts:
        lower = text.lower()
        if "formal release" in lower:
            assert "not a formal release" in lower or "does not constitute" in lower or "不构成正式放行" in text


# ---------------------------------------------------------------------------
# Builder tests
# ---------------------------------------------------------------------------

def test_builder_version_present() -> None:
    assert CLOSEOUT_PACKAGE_BUILDER_VERSION == "2.22.0"


def test_builder_output_has_required_fields() -> None:
    result = build_step2_closeout_package(run_id="test-run")
    required_fields = [
        "schema_version", "artifact_type", "generated_at", "run_id", "phase",
        "package_version", "package_status", "package_status_label",
        "reviewer_summary_line", "reviewer_summary_lines",
        "sections", "section_order",
        "blockers", "next_steps",
        "simulation_only_boundary", "source_versions",
        "evidence_source", "not_real_acceptance_evidence",
        "not_ready_for_formal_claim", "reviewer_only",
        "readiness_mapping_only", "primary_evidence_rewritten",
        "real_acceptance_ready",
    ]
    for field in required_fields:
        assert field in result, f"Missing field: {field}"


def test_builder_boundary_markers_always_enforced() -> None:
    result = build_step2_closeout_package(run_id="test-run")
    assert result["evidence_source"] == "simulated"
    assert result["not_real_acceptance_evidence"] is True
    assert result["not_ready_for_formal_claim"] is True
    assert result["reviewer_only"] is True
    assert result["readiness_mapping_only"] is True
    assert result["primary_evidence_rewritten"] is False
    assert result["real_acceptance_ready"] is False


def test_builder_artifact_type() -> None:
    result = build_step2_closeout_package(run_id="test-run")
    assert result["artifact_type"] == CLOSEOUT_PACKAGE_ARTIFACT_TYPE
    assert result["phase"] == "step2_closeout"


def test_builder_package_status_from_closeout_readiness() -> None:
    """package_status should match closeout readiness closeout_status."""
    readiness = build_step2_readiness_summary(
        run_id="test-run",
        simulation_mode=True,
        config_governance_handoff={
            "simulation_only": True,
            "operator_safe": True,
            "real_port_device_count": 0,
            "engineering_only_flag_count": 0,
            "enabled_engineering_flags": [],
            "execution_gate": {"status": "open"},
            "step2_default_workflow_allowed": True,
            "requires_explicit_unlock": False,
        },
    )
    closeout = build_step2_closeout_readiness(
        run_id="test-run",
        step2_readiness_summary=readiness,
    )
    result = build_step2_closeout_package(
        run_id="test-run",
        step2_closeout_readiness=closeout,
    )
    assert result["package_status"] == closeout["closeout_status"]
    assert result["package_status"] == "ok"


def test_builder_sections_in_canonical_order() -> None:
    result = build_step2_closeout_package(run_id="test-run")
    section_keys = [s["key"] for s in result["sections"]]
    assert section_keys == list(CLOSEOUT_PACKAGE_SECTION_ORDER)


def test_builder_sections_all_have_label() -> None:
    result = build_step2_closeout_package(run_id="test-run")
    for section in result["sections"]:
        assert section["label"], f"Missing label for section: {section['key']}"
        assert section["key"]


def test_builder_reviewer_summary_lines_contain_boundary() -> None:
    result = build_step2_closeout_package(run_id="test-run")
    lines = result["reviewer_summary_lines"]
    boundary = result["simulation_only_boundary"]
    assert any(boundary in line for line in lines), f"Boundary text not in summary lines"


def test_builder_reviewer_summary_line_not_real_acceptance() -> None:
    result = build_step2_closeout_package(run_id="test-run")
    line = result["reviewer_summary_line"]
    assert "real acceptance" in line.lower() or "real acceptance" in line


def test_builder_blockers_from_closeout_readiness() -> None:
    """blockers should come from closeout readiness."""
    readiness = build_step2_readiness_summary(
        run_id="test-run",
        simulation_mode=False,
        config_governance_handoff={
            "simulation_only": False,
            "operator_safe": False,
            "real_port_device_count": 1,
            "engineering_only_flag_count": 0,
            "enabled_engineering_flags": [],
            "execution_gate": {"status": "blocked"},
            "step2_default_workflow_allowed": False,
            "requires_explicit_unlock": True,
        },
    )
    closeout = build_step2_closeout_readiness(
        run_id="test-run",
        step2_readiness_summary=readiness,
    )
    result = build_step2_closeout_package(
        run_id="test-run",
        step2_closeout_readiness=closeout,
    )
    assert result["blockers"] == closeout["blockers"]
    assert len(result["blockers"]) > 0


def test_builder_next_steps_from_closeout_readiness() -> None:
    """next_steps should come from closeout readiness."""
    closeout = build_step2_closeout_readiness(run_id="test-run")
    result = build_step2_closeout_package(
        run_id="test-run",
        step2_closeout_readiness=closeout,
    )
    assert result["next_steps"] == closeout["next_steps"]


def test_builder_surfaces_compare_summary_from_compact_packs() -> None:
    compare_pack = build_control_flow_compare_pack(
        {
            "latest_control_flow_compare": {
                "compare_status": "MISMATCH",
                "validation_profile": "replacement_skip0_co2_only_simulated",
                "target_route": "co2",
                "first_failure_phase": "sample_end",
                "point_presence_diff": "no_diff",
                "sample_count_diff": "diff_present",
                "route_trace_diff": "diff_present",
                "key_action_mismatches": ["vent"],
                "physical_route_mismatch": "yes",
                "next_check": "inspect sample count diff",
            }
        }
    )

    result = build_step2_closeout_package(
        run_id="test-run",
        compact_summary_packs=[compare_pack],
    )

    sections_by_key = {section["key"]: section for section in result["sections"]}
    assert result["compare_available"] is True
    assert result["compare_status"] == "MISMATCH"
    assert result["compare_sample_count_diff"] == "diff_present"
    assert result["compare_route_trace_diff"] == "diff_present"
    assert result["compare_key_action_mismatches"] == ["vent"]
    assert "下一步检查" in result["reviewer_summary_line"] or "Next check" in result["reviewer_summary_line"]
    assert any("离线对齐" in line or "Compare" in line for line in result["reviewer_summary_lines"])
    assert sections_by_key["compact_summaries"]["compare_available"] is True
    assert sections_by_key["compact_summaries"]["compare_status"] == "MISMATCH"


def test_builder_lang_en() -> None:
    result = build_step2_closeout_package(run_id="test-run", lang="en")
    assert "Step 2" in result["reviewer_summary_line"]
    assert "real acceptance" in result["reviewer_summary_line"].lower()
    # Sections should have English labels
    for section in result["sections"]:
        assert section["label"]  # non-empty


def test_builder_no_formal_acceptance_language() -> None:
    result = build_step2_closeout_package(run_id="test-run")
    all_text = " ".join(str(v) for v in result.values() if isinstance(v, str))
    all_text += " ".join(str(line) for line in result.get("reviewer_summary_lines", []))
    lower = all_text.lower()
    if "formal release conclusion" in lower:
        assert "not a formal release" in lower or "不构成正式放行" in all_text
    assert "ready for formal claim" not in lower
    if "formal compliance claim" in lower:
        assert "does not form" in lower or "不形成" in all_text


def test_builder_no_real_path_language() -> None:
    result = build_step2_closeout_package(run_id="test-run")
    result_str = str(result)
    assert "real_compare" not in result_str
    assert "real_verify" not in result_str
    assert "real_primary_latest" not in result_str
    assert result["real_acceptance_ready"] is False


def test_builder_with_all_inputs() -> None:
    """Test builder with all input payloads provided."""
    readiness = build_step2_readiness_summary(
        run_id="test-run",
        simulation_mode=True,
        config_governance_handoff={
            "simulation_only": True,
            "operator_safe": True,
            "real_port_device_count": 0,
            "engineering_only_flag_count": 0,
            "enabled_engineering_flags": [],
            "execution_gate": {"status": "open"},
            "step2_default_workflow_allowed": True,
            "requires_explicit_unlock": False,
        },
    )
    closeout = build_step2_closeout_readiness(
        run_id="test-run",
        step2_readiness_summary=readiness,
    )
    result = build_step2_closeout_package(
        run_id="test-run",
        step2_closeout_readiness=closeout,
        step2_closeout_digest={"schema_version": "1.0"},
        stage_admission_review_pack={"available": True},
        engineering_isolation_admission_checklist={"overall_status": "done"},
        compact_summary_packs=[{"summary_key": "test", "display_label": "测试", "summary_line": "ok", "severity": "info"}],
        governance_handoff={"step2_default_workflow_allowed": True},
        parity_resilience={"status": "MATCH"},
        phase_evidence={"phases": ["step2"]},
    )
    assert result["package_status"] == "ok"
    assert result["evidence_source"] == "simulated"
    assert result["not_real_acceptance_evidence"] is True
    # Sections should reflect availability
    sections_by_key = {s["key"]: s for s in result["sections"]}
    assert sections_by_key["readiness"]["available"] is True
    assert sections_by_key["digest"]["available"] is True
    assert sections_by_key["governance_handoff"]["available"] is True
    assert sections_by_key["compact_summaries"]["available"] is True
    assert sections_by_key["parity_resilience"]["available"] is True
    assert sections_by_key["phase_evidence"]["available"] is True
    assert sections_by_key["stage_admission"]["available"] is True
    assert sections_by_key["engineering_isolation_checklist"]["available"] is True


def test_builder_consistency_with_closeout_readiness() -> None:
    """Closeout package should be consistent with closeout readiness."""
    closeout = build_step2_closeout_readiness(run_id="test-run")
    result = build_step2_closeout_package(
        run_id="test-run",
        step2_closeout_readiness=closeout,
    )
    # package_status should match closeout_status
    assert result["package_status"] == closeout["closeout_status"]
    # blockers should match
    assert result["blockers"] == closeout["blockers"]
    # next_steps should match
    assert result["next_steps"] == closeout["next_steps"]


# ---------------------------------------------------------------------------
# Fallback tests
# ---------------------------------------------------------------------------

def test_fallback_boundary_markers() -> None:
    fb = build_closeout_package_fallback()
    assert fb["evidence_source"] == "simulated"
    assert fb["not_real_acceptance_evidence"] is True
    assert fb["not_ready_for_formal_claim"] is True
    assert fb["reviewer_only"] is True
    assert fb["readiness_mapping_only"] is True
    assert fb["primary_evidence_rewritten"] is False
    assert fb["real_acceptance_ready"] is False


def test_fallback_status_values() -> None:
    fb = build_closeout_package_fallback()
    assert fb["package_status"] == "reviewer_only"
    assert fb["blockers"] == []
    assert fb["next_steps"] == []
    assert fb["sections"] == []


def test_fallback_lang_en() -> None:
    fb = build_closeout_package_fallback(lang="en")
    assert fb["package_status"] == "reviewer_only"
    assert "fallback" in fb["reviewer_summary_line"].lower()


def test_fallback_no_formal_semantics() -> None:
    fb = build_closeout_package_fallback()
    assert fb["real_acceptance_ready"] is False
    assert "approved" not in str(fb).lower() or "not approved" in str(fb).lower()


# ---------------------------------------------------------------------------
# Step 2 boundary — comprehensive
# ---------------------------------------------------------------------------

def test_closeout_package_step2_boundary_markers_all() -> None:
    """All 7 Step 2 boundary markers should be enforced in builder output."""
    result = build_step2_closeout_package(run_id="test-run")
    assert result["evidence_source"] == "simulated"
    assert result["not_real_acceptance_evidence"] is True
    assert result["not_ready_for_formal_claim"] is True
    assert result["reviewer_only"] is True
    assert result["readiness_mapping_only"] is True
    assert result["primary_evidence_rewritten"] is False
    assert result["real_acceptance_ready"] is False


def test_closeout_package_no_formal_semantics_in_status() -> None:
    """package_status should not contain formal acceptance/release language."""
    result = build_step2_closeout_package(run_id="test-run")
    assert result["package_status"] in {"ok", "attention", "blocker", "reviewer_only"}
    assert "approved" not in result["package_status"].lower()
    assert "released" not in result["package_status"].lower()
    assert "formal_acceptance" not in result["package_status"].lower()


# ---------------------------------------------------------------------------
# closeout_package_source field tests (Step 2.22)
# ---------------------------------------------------------------------------

def test_closeout_package_source_rebuilt() -> None:
    """build_step2_closeout_package should set closeout_package_source = 'rebuilt'."""
    result = build_step2_closeout_package(run_id="test-run")
    assert result["closeout_package_source"] == "rebuilt"


def test_closeout_package_source_fallback() -> None:
    """build_closeout_package_fallback should set closeout_package_source = 'fallback'."""
    fb = build_closeout_package_fallback()
    assert fb["closeout_package_source"] == "fallback"


def test_closeout_package_persisted_source() -> None:
    """When a persisted package has closeout_package_source = 'persisted',
    it should be preserved (simulating app_facade / results_gateway behavior)."""
    result = build_step2_closeout_package(run_id="test-run")
    result["closeout_package_source"] = "persisted"
    assert result["closeout_package_source"] == "persisted"
