"""Tests for Step 2 closeout readiness contracts and builder.

Verifies:
- Contract key coverage and zh/en consistency
- Builder output stability (status, blockers, next_steps)
- Simulation-only / reviewer-only / non-claim boundary enforcement
- Closeout readiness does not introduce real acceptance / formal claim language
- Closeout readiness aggregates from existing payloads (not parallel summary)
- Step 2 boundary assertions continue to pass
"""

from __future__ import annotations

from gas_calibrator.v2.core.step2_closeout_readiness_contracts import (
    CLOSEOUT_BLOCKER_LABELS_EN,
    CLOSEOUT_BLOCKER_LABELS_ZH,
    CLOSEOUT_CONTRIBUTING_SECTIONS,
    CLOSEOUT_CONTRIBUTING_SECTION_LABELS_EN,
    CLOSEOUT_CONTRIBUTING_SECTION_LABELS_ZH,
    CLOSEOUT_I18N_KEYS,
    CLOSEOUT_NEXT_STEPS_EN,
    CLOSEOUT_NEXT_STEPS_ZH,
    CLOSEOUT_STATUS_BUCKETS,
    CLOSEOUT_STEP2_BOUNDARY,
    CLOSEOUT_SIMULATION_ONLY_BOUNDARY_EN,
    CLOSEOUT_SIMULATION_ONLY_BOUNDARY_ZH,
    CLOSEOUT_TITLE_EN,
    CLOSEOUT_TITLE_ZH,
    CLOSEOUT_STATUS_OK,
    CLOSEOUT_STATUS_ATTENTION,
    CLOSEOUT_STATUS_BLOCKER,
    CLOSEOUT_STATUS_REVIEWER_ONLY,
    GATE_STATUS_LABEL_ZH,
    GATE_STATUS_LABEL_EN,
    CLOSEOUT_FALLBACK_NOTE_ZH,
    CLOSEOUT_FALLBACK_NOTE_EN,
    resolve_closeout_title,
    resolve_closeout_summary_line,
    resolve_closeout_status_label,
    resolve_closeout_blocker_label,
    resolve_closeout_next_step_label,
    resolve_closeout_simulation_only_boundary,
    resolve_closeout_reviewer_only_notice,
    resolve_closeout_non_claim_notice,
    resolve_closeout_contributing_section_label,
    resolve_gate_status_label,
    build_closeout_readiness_fallback,
)
from gas_calibrator.v2.core.step2_closeout_readiness_builder import (
    build_step2_closeout_readiness,
    CLOSEOUT_READINESS_BUILDER_VERSION,
)
from gas_calibrator.v2.core.step2_readiness import build_step2_readiness_summary


# ---------------------------------------------------------------------------
# Contract tests
# ---------------------------------------------------------------------------

def test_closeout_status_buckets_are_complete() -> None:
    assert CLOSEOUT_STATUS_OK in CLOSEOUT_STATUS_BUCKETS
    assert CLOSEOUT_STATUS_ATTENTION in CLOSEOUT_STATUS_BUCKETS
    assert CLOSEOUT_STATUS_BLOCKER in CLOSEOUT_STATUS_BUCKETS
    assert CLOSEOUT_STATUS_REVIEWER_ONLY in CLOSEOUT_STATUS_BUCKETS
    assert len(CLOSEOUT_STATUS_BUCKETS) == 4


def test_closeout_blocker_labels_zh_en_consistent() -> None:
    assert set(CLOSEOUT_BLOCKER_LABELS_ZH.keys()) == set(CLOSEOUT_BLOCKER_LABELS_EN.keys())


def test_closeout_next_steps_zh_en_consistent() -> None:
    assert set(CLOSEOUT_NEXT_STEPS_ZH.keys()) == set(CLOSEOUT_NEXT_STEPS_EN.keys())


def test_closeout_contributing_sections_zh_en_consistent() -> None:
    assert set(CLOSEOUT_CONTRIBUTING_SECTION_LABELS_ZH.keys()) == set(CLOSEOUT_CONTRIBUTING_SECTION_LABELS_EN.keys())
    for key in CLOSEOUT_CONTRIBUTING_SECTIONS:
        assert key in CLOSEOUT_CONTRIBUTING_SECTION_LABELS_ZH
        assert key in CLOSEOUT_CONTRIBUTING_SECTION_LABELS_EN


def test_closeout_i18n_keys_present() -> None:
    required_keys = [
        "title", "summary_line", "status_ok", "status_attention",
        "status_blocker", "status_reviewer_only",
        "simulation_only_boundary", "reviewer_only_notice", "non_claim_notice",
        "blockers_label", "next_steps_label", "contributing_sections_label",
    ]
    for key in required_keys:
        assert key in CLOSEOUT_I18N_KEYS, f"Missing i18n key: {key}"


def test_closeout_step2_boundary_markers() -> None:
    assert CLOSEOUT_STEP2_BOUNDARY["evidence_source"] == "simulated"
    assert CLOSEOUT_STEP2_BOUNDARY["not_real_acceptance_evidence"] is True
    assert CLOSEOUT_STEP2_BOUNDARY["not_ready_for_formal_claim"] is True
    assert CLOSEOUT_STEP2_BOUNDARY["reviewer_only"] is True
    assert CLOSEOUT_STEP2_BOUNDARY["readiness_mapping_only"] is True
    assert CLOSEOUT_STEP2_BOUNDARY["primary_evidence_rewritten"] is False


def test_closeout_simulation_only_boundary_contains_non_claim() -> None:
    assert "real acceptance" in CLOSEOUT_SIMULATION_ONLY_BOUNDARY_ZH.lower() or "real acceptance" in CLOSEOUT_SIMULATION_ONLY_BOUNDARY_ZH
    assert "real acceptance" in CLOSEOUT_SIMULATION_ONLY_BOUNDARY_EN.lower()


def test_resolve_closeout_title_zh_en() -> None:
    assert resolve_closeout_title(lang="zh") == CLOSEOUT_TITLE_ZH
    assert resolve_closeout_title(lang="en") == CLOSEOUT_TITLE_EN
    assert "Step 2" in resolve_closeout_title(lang="zh")
    assert "Step 2" in resolve_closeout_title(lang="en")


def test_resolve_closeout_status_label_all_buckets() -> None:
    for status in CLOSEOUT_STATUS_BUCKETS:
        label_zh = resolve_closeout_status_label(status, lang="zh")
        label_en = resolve_closeout_status_label(status, lang="en")
        assert label_zh, f"Missing zh label for status: {status}"
        assert label_en, f"Missing en label for status: {status}"


def test_resolve_closeout_blocker_label_fallback() -> None:
    unknown = "unknown_blocker_xyz"
    assert resolve_closeout_blocker_label(unknown, lang="zh") == unknown
    assert resolve_closeout_blocker_label(unknown, lang="en") == unknown


def test_resolve_closeout_next_step_label_fallback() -> None:
    unknown = "unknown_step_xyz"
    assert resolve_closeout_next_step_label(unknown, lang="zh") == unknown
    assert resolve_closeout_next_step_label(unknown, lang="en") == unknown


def test_no_formal_acceptance_language_in_contracts() -> None:
    """Verify no formal acceptance / formal claim / accreditation language in contracts."""
    all_texts = [
        CLOSEOUT_TITLE_ZH, CLOSEOUT_TITLE_EN,
        CLOSEOUT_SIMULATION_ONLY_BOUNDARY_ZH, CLOSEOUT_SIMULATION_ONLY_BOUNDARY_EN,
    ]
    for text in all_texts:
        lower = text.lower()
        # "formal release conclusion" is OK only in negation context
        if "formal release" in lower:
            assert "not a formal release" in lower or "does not constitute" in lower or "不构成正式放行" in text


# ---------------------------------------------------------------------------
# Builder tests
# ---------------------------------------------------------------------------

def test_builder_version_present() -> None:
    assert CLOSEOUT_READINESS_BUILDER_VERSION
    assert CLOSEOUT_READINESS_BUILDER_VERSION.startswith("2.")


def test_builder_output_has_required_fields() -> None:
    result = build_step2_closeout_readiness(run_id="test-run")
    required_fields = [
        "schema_version", "artifact_type", "generated_at", "run_id", "phase",
        "closeout_status", "closeout_status_label",
        "reviewer_summary_line", "reviewer_summary_lines",
        "blockers", "next_steps", "contributing_sections",
        "simulation_only_boundary", "rendered_compact_sections",
        "gate_status", "gate_summary", "closeout_gate_alignment",
        "evidence_source", "not_real_acceptance_evidence",
        "not_ready_for_formal_claim", "reviewer_only",
        "readiness_mapping_only", "primary_evidence_rewritten",
        "real_acceptance_ready",
        "source_readiness_status", "source_blocking_items", "source_warning_items",
    ]
    for field in required_fields:
        assert field in result, f"Missing field: {field}"


def test_builder_boundary_markers_always_enforced() -> None:
    result = build_step2_closeout_readiness(run_id="test-run")
    assert result["not_real_acceptance_evidence"] is True
    assert result["not_ready_for_formal_claim"] is True
    assert result["reviewer_only"] is True
    assert result["readiness_mapping_only"] is True
    assert result["primary_evidence_rewritten"] is False
    assert result["real_acceptance_ready"] is False
    assert result["evidence_source"] in {"simulated", "simulated_protocol"}


def test_builder_status_ok_when_readiness_ready() -> None:
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
    result = build_step2_closeout_readiness(
        run_id="test-run",
        step2_readiness_summary=readiness,
    )
    assert result["closeout_status"] == CLOSEOUT_STATUS_OK
    assert result["blockers"] == []
    assert "就绪" in result["closeout_status_label"] or "ready" in result["closeout_status_label"].lower()


def test_builder_status_blocker_when_readiness_blocked() -> None:
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
    result = build_step2_closeout_readiness(
        run_id="test-run",
        step2_readiness_summary=readiness,
    )
    assert result["closeout_status"] == CLOSEOUT_STATUS_BLOCKER
    assert len(result["blockers"]) > 0


def test_builder_reviewer_summary_lines_contain_boundary() -> None:
    result = build_step2_closeout_readiness(run_id="test-run")
    lines = result["reviewer_summary_lines"]
    # Should contain simulation-only boundary text
    boundary = result["simulation_only_boundary"]
    assert any(boundary in line for line in lines), f"Boundary text not in summary lines: {boundary}"


def test_builder_reviewer_summary_line_not_real_acceptance() -> None:
    result = build_step2_closeout_readiness(run_id="test-run")
    line = result["reviewer_summary_line"]
    assert "real acceptance" in line.lower() or "real acceptance" in line


def test_builder_contributing_sections_all_present() -> None:
    result = build_step2_closeout_readiness(run_id="test-run")
    sections = result["contributing_sections"]
    section_keys = [s["key"] for s in sections]
    for key in CLOSEOUT_CONTRIBUTING_SECTIONS:
        assert key in section_keys, f"Missing contributing section: {key}"


def test_builder_contributing_sections_with_packs() -> None:
    packs = [
        {"summary_key": "test_pack", "display_label": "测试包", "summary_line": "测试", "severity": "info"},
    ]
    result = build_step2_closeout_readiness(
        run_id="test-run",
        compact_summary_packs=packs,
    )
    sections = result["contributing_sections"]
    compact_section = next(s for s in sections if s["key"] == "compact_summary")
    assert compact_section["available"] is True
    assert compact_section["pack_count"] == 1


def test_builder_next_steps_always_include_await_real_acceptance() -> None:
    result = build_step2_closeout_readiness(run_id="test-run")
    step_keys = [s["key"] for s in result["next_steps"]]
    assert "await_real_acceptance" in step_keys


def test_builder_next_steps_ok_includes_proceed() -> None:
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
    result = build_step2_closeout_readiness(
        run_id="test-run",
        step2_readiness_summary=readiness,
    )
    step_keys = [s["key"] for s in result["next_steps"]]
    assert "proceed_to_engineering_isolation" in step_keys


def test_builder_lang_en() -> None:
    result = build_step2_closeout_readiness(run_id="test-run", lang="en")
    assert "Step 2" in result["reviewer_summary_line"]
    assert "real acceptance" in result["reviewer_summary_line"].lower()
    # Contributing sections should have English labels
    sections = result["contributing_sections"]
    for section in sections:
        assert section["label"]  # non-empty


def test_builder_no_formal_acceptance_language() -> None:
    """Verify builder output does not contain formal acceptance / formal claim language."""
    result = build_step2_closeout_readiness(run_id="test-run")
    all_text = " ".join(str(v) for v in result.values() if isinstance(v, str))
    all_text += " ".join(str(line) for line in result.get("reviewer_summary_lines", []))
    lower = all_text.lower()
    # "formal release conclusion" is OK only in negation context
    if "formal release conclusion" in lower:
        assert "not a formal release" in lower or "不构成正式放行" in all_text
    # Should never claim "ready for formal claim"
    assert "ready for formal claim" not in lower
    # "formal compliance claim" is OK only in negation context
    if "formal compliance claim" in lower:
        assert "does not form" in lower or "不形成" in all_text


def test_builder_rendered_compact_sections_from_packs() -> None:
    packs = [
        {"summary_key": "phase_evidence", "display_label": "阶段证据", "summary_line": "3 phases", "severity": "info", "priority": 10},
        {"summary_key": "parity_resilience", "display_label": "一致性", "summary_line": "MATCH", "severity": "ok", "priority": 60},
    ]
    result = build_step2_closeout_readiness(
        run_id="test-run",
        compact_summary_packs=packs,
    )
    rendered = result["rendered_compact_sections"]
    assert len(rendered) == 2
    assert rendered[0]["summary_key"] == "phase_evidence"
    assert rendered[1]["summary_key"] == "parity_resilience"


def test_builder_artifact_type() -> None:
    result = build_step2_closeout_readiness(run_id="test-run")
    assert result["artifact_type"] == "step2_closeout_readiness"
    assert result["phase"] == "step2_closeout"


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
    result = build_step2_closeout_readiness(
        run_id="test-run",
        step2_readiness_summary=readiness,
        compact_summary_packs=[{"summary_key": "test", "display_label": "测试", "summary_line": "ok", "severity": "info"}],
        governance_handoff={"step2_default_workflow_allowed": True},
        parity_resilience={"status": "MATCH"},
        acceptance_governance={"acceptance_level": "offline_regression", "readiness_mapping_only": True},
        phase_evidence={"phases": ["step2"]},
    )
    assert result["closeout_status"] == CLOSEOUT_STATUS_OK
    assert result["evidence_source"] in {"simulated", "simulated_protocol"}
    assert result["not_real_acceptance_evidence"] is True
    # Contributing sections should all be available
    sections = result["contributing_sections"]
    for section in sections:
        assert section["available"] is True


def test_builder_consistency_with_step2_readiness() -> None:
    """Closeout readiness should be consistent with step2_readiness_summary output."""
    readiness = build_step2_readiness_summary(
        run_id="test-run",
        simulation_mode=True,
    )
    result = build_step2_closeout_readiness(
        run_id="test-run",
        step2_readiness_summary=readiness,
    )
    # Source fields should match
    assert result["source_readiness_status"] == readiness["overall_status"]
    assert result["source_blocking_items"] == readiness["blocking_items"]
    assert result["source_warning_items"] == readiness["warning_items"]


# ---------------------------------------------------------------------------
# Step 2.19 tests — gate fields, fallback, boundary, naming
# ---------------------------------------------------------------------------

def test_builder_includes_gate_status() -> None:
    """Builder output should include gate_status field (Step 2.19)."""
    result = build_step2_closeout_readiness(run_id="test-run")
    assert "gate_status" in result
    assert result["gate_status"] in {"ready_for_engineering_isolation", "not_ready"}


def test_builder_includes_gate_summary() -> None:
    """Builder output should include gate_summary field (Step 2.19)."""
    result = build_step2_closeout_readiness(run_id="test-run")
    assert "gate_summary" in result
    gs = result["gate_summary"]
    assert "pass_count" in gs
    assert "total_count" in gs
    assert "blocked_count" in gs
    assert "blocked_gate_ids" in gs
    assert isinstance(gs["pass_count"], int)
    assert isinstance(gs["total_count"], int)
    assert isinstance(gs["blocked_count"], int)
    assert isinstance(gs["blocked_gate_ids"], list)


def test_builder_includes_closeout_gate_alignment() -> None:
    """Builder output should include closeout_gate_alignment field (Step 2.19)."""
    result = build_step2_closeout_readiness(run_id="test-run")
    assert "closeout_gate_alignment" in result
    alignment = result["closeout_gate_alignment"]
    assert "closeout_status" in alignment
    assert "gate_status" in alignment
    assert "aligned" in alignment
    assert isinstance(alignment["aligned"], bool)


def test_gate_status_aligns_with_overall_status() -> None:
    """gate_status should match step2_readiness_summary.overall_status."""
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
    result = build_step2_closeout_readiness(
        run_id="test-run",
        step2_readiness_summary=readiness,
    )
    assert result["gate_status"] == readiness["overall_status"]
    assert result["gate_status"] == "ready_for_engineering_isolation"


def test_gate_status_not_ready_when_blocked() -> None:
    """gate_status should be not_ready when readiness has blockers."""
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
    result = build_step2_closeout_readiness(
        run_id="test-run",
        step2_readiness_summary=readiness,
    )
    assert result["gate_status"] == "not_ready"


def test_gate_summary_counts_consistent() -> None:
    """gate_summary counts should be consistent."""
    result = build_step2_closeout_readiness(run_id="test-run")
    gs = result["gate_summary"]
    assert gs["pass_count"] + gs["blocked_count"] <= gs["total_count"]
    assert gs["blocked_count"] == len(gs["blocked_gate_ids"])


def test_closeout_gate_alignment_ok_ready() -> None:
    """closeout_gate_alignment.aligned should be True when ok + ready_for_engineering_isolation."""
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
    result = build_step2_closeout_readiness(
        run_id="test-run",
        step2_readiness_summary=readiness,
    )
    assert result["closeout_gate_alignment"]["aligned"] is True
    assert result["closeout_gate_alignment"]["closeout_status"] == "ok"
    assert result["closeout_gate_alignment"]["gate_status"] == "ready_for_engineering_isolation"


def test_closeout_gate_alignment_blocker_not_ready() -> None:
    """closeout_gate_alignment.aligned should be True when blocker + not_ready."""
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
    result = build_step2_closeout_readiness(
        run_id="test-run",
        step2_readiness_summary=readiness,
    )
    assert result["closeout_gate_alignment"]["aligned"] is True
    assert result["closeout_gate_alignment"]["closeout_status"] == "blocker"
    assert result["closeout_gate_alignment"]["gate_status"] == "not_ready"


def test_closeout_gate_alignment_reviewer_only_not_ready() -> None:
    """closeout_gate_alignment.aligned should be True when reviewer_only + not_ready."""
    result = build_step2_closeout_readiness(run_id="test-run")
    # Default (no readiness summary) → reviewer_only + not_ready
    if result["closeout_status"] == "reviewer_only" and result["gate_status"] == "not_ready":
        assert result["closeout_gate_alignment"]["aligned"] is True


def test_build_closeout_readiness_fallback_boundary_markers() -> None:
    """Fallback should guarantee all Step 2 boundary markers."""
    fb = build_closeout_readiness_fallback()
    assert fb["evidence_source"] == "simulated"
    assert fb["not_real_acceptance_evidence"] is True
    assert fb["not_ready_for_formal_claim"] is True
    assert fb["reviewer_only"] is True
    assert fb["readiness_mapping_only"] is True
    assert fb["primary_evidence_rewritten"] is False
    assert fb["real_acceptance_ready"] is False


def test_build_closeout_readiness_fallback_status_values() -> None:
    """Fallback should have correct status values."""
    fb = build_closeout_readiness_fallback()
    assert fb["closeout_status"] == "reviewer_only"
    assert fb["gate_status"] == "not_ready"
    assert fb["blockers"] == []
    assert fb["next_steps"] == []
    assert fb["contributing_sections"] == []


def test_build_closeout_readiness_fallback_gate_summary() -> None:
    """Fallback gate_summary should be all zeros."""
    fb = build_closeout_readiness_fallback()
    gs = fb["gate_summary"]
    assert gs["pass_count"] == 0
    assert gs["total_count"] == 0
    assert gs["blocked_count"] == 0
    assert gs["blocked_gate_ids"] == []


def test_build_closeout_readiness_fallback_gate_alignment() -> None:
    """Fallback closeout_gate_alignment should be aligned."""
    fb = build_closeout_readiness_fallback()
    alignment = fb["closeout_gate_alignment"]
    assert alignment["closeout_status"] == "reviewer_only"
    assert alignment["gate_status"] == "not_ready"
    assert alignment["aligned"] is True


def test_build_closeout_readiness_fallback_compatibility_note() -> None:
    """Fallback should include compatibility_note by default."""
    fb = build_closeout_readiness_fallback()
    assert "compatibility_note" in fb
    assert fb["compatibility_note"]  # non-empty
    # Without note
    fb_no_note = build_closeout_readiness_fallback(include_compatibility_note=False)
    assert "compatibility_note" not in fb_no_note


def test_build_closeout_readiness_fallback_lang_en() -> None:
    """Fallback should support English."""
    fb = build_closeout_readiness_fallback(lang="en")
    assert fb["closeout_status"] == "reviewer_only"
    assert "compatibility_note" in fb
    assert "fallback" in fb["compatibility_note"].lower()


def test_gate_status_labels_zh_en_consistent() -> None:
    """Gate status labels should have consistent zh/en keys."""
    assert set(GATE_STATUS_LABEL_ZH.keys()) == set(GATE_STATUS_LABEL_EN.keys())


def test_resolve_gate_status_label() -> None:
    """resolve_gate_status_label should return correct labels."""
    zh_ready = resolve_gate_status_label("ready_for_engineering_isolation", lang="zh")
    en_ready = resolve_gate_status_label("ready_for_engineering_isolation", lang="en")
    assert zh_ready  # non-empty
    assert en_ready  # non-empty
    # Fallback for unknown
    unknown = resolve_gate_status_label("unknown_status", lang="zh")
    assert unknown == "unknown_status"


def test_closeout_i18n_keys_include_gate_fields() -> None:
    """i18n keys should include gate_status, gate_summary, closeout_gate_alignment."""
    assert "gate_status" in CLOSEOUT_I18N_KEYS
    assert "gate_summary" in CLOSEOUT_I18N_KEYS
    assert "closeout_gate_alignment" in CLOSEOUT_I18N_KEYS


def test_closeout_readiness_no_formal_semantics_in_gate_fields() -> None:
    """Gate fields should not contain formal acceptance/release language."""
    result = build_step2_closeout_readiness(run_id="test-run")
    # gate_status value domain check
    assert result["gate_status"] in {"ready_for_engineering_isolation", "not_ready"}
    # No "approved" / "released" / "formal_acceptance" in gate_status
    assert "approved" not in result["gate_status"].lower()
    assert "released" not in result["gate_status"].lower()
    assert "formal_acceptance" not in result["gate_status"].lower()


def test_closeout_readiness_no_real_path_language() -> None:
    """Closeout readiness should not contain real path language."""
    result = build_step2_closeout_readiness(run_id="test-run")
    result_str = str(result)
    assert "real_compare" not in result_str
    assert "real_verify" not in result_str
    assert "real_primary_latest" not in result_str
    # real_acceptance_ready must be False
    assert result["real_acceptance_ready"] is False


def test_closeout_readiness_step2_boundary_markers_all() -> None:
    """All 7 Step 2 boundary markers should be enforced in builder output."""
    result = build_step2_closeout_readiness(run_id="test-run")
    assert result["evidence_source"] in {"simulated", "simulated_protocol"}
    assert result["not_real_acceptance_evidence"] is True
    assert result["not_ready_for_formal_claim"] is True
    assert result["reviewer_only"] is True
    assert result["readiness_mapping_only"] is True
    assert result["primary_evidence_rewritten"] is False
    assert result["real_acceptance_ready"] is False


def test_fallback_no_formal_semantics() -> None:
    """Fallback should not contain formal acceptance/release language."""
    fb = build_closeout_readiness_fallback()
    assert fb["gate_status"] == "not_ready"
    assert "approved" not in str(fb).lower() or "not approved" in str(fb).lower()
    # real_acceptance_ready must be False
    assert fb["real_acceptance_ready"] is False


def test_builder_version_220() -> None:
    """Builder version should be 2.20.0."""
    assert CLOSEOUT_READINESS_BUILDER_VERSION == "2.20.0"


def test_contracts_version_220() -> None:
    """Contracts version should be 2.20.0."""
    from gas_calibrator.v2.core.step2_closeout_readiness_contracts import CLOSEOUT_READINESS_CONTRACTS_VERSION
    assert CLOSEOUT_READINESS_CONTRACTS_VERSION == "2.20.0"


# ---------------------------------------------------------------------------
# Step 2.20: resolve_gate_status_label fallback for unknown status
# ---------------------------------------------------------------------------

def test_resolve_gate_status_label_unknown_status_fallback() -> None:
    """resolve_gate_status_label should return the raw status for unknown values."""
    assert resolve_gate_status_label("unknown_gate_value") == "unknown_gate_value"
    assert resolve_gate_status_label("unknown_gate_value", lang="en") == "unknown_gate_value"


def test_resolve_gate_status_label_known_values() -> None:
    """resolve_gate_status_label should resolve known gate status values."""
    assert resolve_gate_status_label("ready_for_engineering_isolation") == "门禁已就绪"
    assert resolve_gate_status_label("not_ready") == "门禁未就绪"
    assert resolve_gate_status_label("ready_for_engineering_isolation", lang="en") == "Gates ready"
    assert resolve_gate_status_label("not_ready", lang="en") == "Gates not ready"
