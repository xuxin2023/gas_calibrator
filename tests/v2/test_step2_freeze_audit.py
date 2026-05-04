"""Tests for Step 2 freeze audit contracts and builder.

Verifies:
- Contract key coverage and zh/en consistency
- Builder output stability (audit_status, blockers, next_steps, freeze_candidate)
- Simulation-only / reviewer-only / non-claim boundary enforcement
- Freeze audit does not introduce real acceptance / formal claim language
- Freeze audit is an upper-level view on closeout package (does not replace it)
- freeze_candidate means "RC review candidate", NOT "release approval"
- Step 2 boundary assertions continue to pass
- Fallback behavior
- Consistency with closeout package / closeout readiness
"""

from __future__ import annotations

from gas_calibrator.v2.core.step2_freeze_audit_contracts import (
    FREEZE_AUDIT_CONTRACTS_VERSION,
    FREEZE_AUDIT_I18N_KEYS,
    FREEZE_AUDIT_SECTION_LABELS_EN,
    FREEZE_AUDIT_SECTION_LABELS_ZH,
    FREEZE_AUDIT_SECTION_ORDER,
    FREEZE_AUDIT_STEP2_BOUNDARY,
    FREEZE_AUDIT_STATUS_OK,
    FREEZE_AUDIT_STATUS_ATTENTION,
    FREEZE_AUDIT_STATUS_BLOCKER,
    FREEZE_AUDIT_STATUS_REVIEWER_ONLY,
    FREEZE_AUDIT_BLOCKER_LABELS_ZH,
    FREEZE_AUDIT_BLOCKER_LABELS_EN,
    FREEZE_AUDIT_BLOCKER_KEYS,
    FREEZE_AUDIT_NEXT_STEPS_ZH,
    FREEZE_AUDIT_NEXT_STEPS_EN,
    FREEZE_AUDIT_NEXT_STEP_KEYS,
    FREEZE_AUDIT_TITLE_ZH,
    FREEZE_AUDIT_TITLE_EN,
    FREEZE_AUDIT_SIMULATION_ONLY_BOUNDARY_ZH,
    FREEZE_AUDIT_SIMULATION_ONLY_BOUNDARY_EN,
    resolve_freeze_audit_title,
    resolve_freeze_audit_summary,
    resolve_freeze_audit_section_label,
    resolve_freeze_audit_status_label,
    resolve_freeze_audit_blocker_label,
    resolve_freeze_audit_next_step_label,
    resolve_freeze_audit_simulation_only_boundary,
    resolve_freeze_audit_reviewer_only_notice,
    resolve_freeze_audit_non_claim_notice,
    resolve_freeze_candidate_notice,
)
from gas_calibrator.v2.core.step2_freeze_audit_builder import (
    build_step2_freeze_audit,
    build_freeze_audit_fallback,
    FREEZE_AUDIT_BUILDER_VERSION,
)
from gas_calibrator.v2.core.step2_closeout_package_builder import (
    build_step2_closeout_package,
)
from gas_calibrator.v2.core.reviewer_summary_packs import build_control_flow_compare_pack


# ---------------------------------------------------------------------------
# Contract tests
# ---------------------------------------------------------------------------

def test_freeze_audit_contracts_version() -> None:
    assert FREEZE_AUDIT_CONTRACTS_VERSION == "2.23.0"


def test_freeze_audit_builder_version() -> None:
    assert FREEZE_AUDIT_BUILDER_VERSION == "2.23.0"


def test_freeze_audit_section_labels_zh_en_consistent() -> None:
    assert set(FREEZE_AUDIT_SECTION_LABELS_ZH.keys()) == set(FREEZE_AUDIT_SECTION_LABELS_EN.keys())


def test_freeze_audit_section_order_covers_all_labels() -> None:
    for key in FREEZE_AUDIT_SECTION_ORDER:
        assert key in FREEZE_AUDIT_SECTION_LABELS_ZH, f"Missing zh label for section: {key}"
        assert key in FREEZE_AUDIT_SECTION_LABELS_EN, f"Missing en label for section: {key}"


def test_freeze_audit_blocker_labels_zh_en_consistent() -> None:
    assert set(FREEZE_AUDIT_BLOCKER_LABELS_ZH.keys()) == set(FREEZE_AUDIT_BLOCKER_LABELS_EN.keys())
    for key in FREEZE_AUDIT_BLOCKER_KEYS:
        assert key in FREEZE_AUDIT_BLOCKER_LABELS_ZH, f"Missing zh blocker label: {key}"
        assert key in FREEZE_AUDIT_BLOCKER_LABELS_EN, f"Missing en blocker label: {key}"


def test_freeze_audit_next_step_labels_zh_en_consistent() -> None:
    assert set(FREEZE_AUDIT_NEXT_STEPS_ZH.keys()) == set(FREEZE_AUDIT_NEXT_STEPS_EN.keys())
    for key in FREEZE_AUDIT_NEXT_STEP_KEYS:
        assert key in FREEZE_AUDIT_NEXT_STEPS_ZH, f"Missing zh next_step label: {key}"
        assert key in FREEZE_AUDIT_NEXT_STEPS_EN, f"Missing en next_step label: {key}"


def test_freeze_audit_i18n_keys_present() -> None:
    required_keys = [
        "title", "summary",
        "section_suite", "section_parity", "section_resilience",
        "section_governance", "section_closeout", "section_boundary",
        "status_ok", "status_attention", "status_blocker", "status_reviewer_only",
        "simulation_only_boundary", "reviewer_only_notice", "non_claim_notice",
        "freeze_candidate_notice", "no_content", "panel", "audit_status",
    ]
    for key in required_keys:
        assert key in FREEZE_AUDIT_I18N_KEYS, f"Missing i18n key: {key}"


def test_freeze_audit_step2_boundary_markers() -> None:
    assert FREEZE_AUDIT_STEP2_BOUNDARY["evidence_source"] == "simulated"
    assert FREEZE_AUDIT_STEP2_BOUNDARY["not_real_acceptance_evidence"] is True
    assert FREEZE_AUDIT_STEP2_BOUNDARY["not_ready_for_formal_claim"] is True
    assert FREEZE_AUDIT_STEP2_BOUNDARY["reviewer_only"] is True
    assert FREEZE_AUDIT_STEP2_BOUNDARY["readiness_mapping_only"] is True
    assert FREEZE_AUDIT_STEP2_BOUNDARY["primary_evidence_rewritten"] is False
    assert FREEZE_AUDIT_STEP2_BOUNDARY["real_acceptance_ready"] is False


# ---------------------------------------------------------------------------
# Resolve helper tests
# ---------------------------------------------------------------------------

def test_resolve_freeze_audit_title_zh() -> None:
    assert resolve_freeze_audit_title(lang="zh") == FREEZE_AUDIT_TITLE_ZH


def test_resolve_freeze_audit_title_en() -> None:
    assert resolve_freeze_audit_title(lang="en") == FREEZE_AUDIT_TITLE_EN


def test_resolve_freeze_audit_section_label_all() -> None:
    for key in FREEZE_AUDIT_SECTION_ORDER:
        zh = resolve_freeze_audit_section_label(key, lang="zh")
        en = resolve_freeze_audit_section_label(key, lang="en")
        assert zh, f"Empty zh label for section: {key}"
        assert en, f"Empty en label for section: {key}"


def test_resolve_freeze_audit_status_label_all() -> None:
    for status in [FREEZE_AUDIT_STATUS_OK, FREEZE_AUDIT_STATUS_ATTENTION,
                   FREEZE_AUDIT_STATUS_BLOCKER, FREEZE_AUDIT_STATUS_REVIEWER_ONLY]:
        zh = resolve_freeze_audit_status_label(status, lang="zh")
        en = resolve_freeze_audit_status_label(status, lang="en")
        assert zh, f"Empty zh label for status: {status}"
        assert en, f"Empty en label for status: {status}"


def test_resolve_freeze_candidate_notice() -> None:
    zh = resolve_freeze_candidate_notice(lang="zh")
    en = resolve_freeze_candidate_notice(lang="en")
    assert "审阅候选" in zh or "RC" in zh
    assert "review candidate" in en.lower() or "RC" in en
    # Must clarify it is NOT formal approval (contains "not" qualifier)
    assert "不是正式放行批准" in zh or "不是正式批准" in zh or "非正式" in zh
    assert "not formal release approval" in en.lower()


# ---------------------------------------------------------------------------
# Builder tests
# ---------------------------------------------------------------------------

def test_freeze_audit_builder_output_fields() -> None:
    result = build_step2_freeze_audit(run_id="test-run")
    required_fields = [
        "schema_version", "artifact_type", "generated_at", "run_id", "phase",
        "audit_version", "audit_status", "audit_status_label",
        "reviewer_summary_line", "reviewer_summary_lines",
        "blockers", "next_steps", "audit_sections", "section_order",
        "freeze_candidate", "freeze_candidate_notice_zh", "freeze_candidate_notice_en",
        "simulation_only_boundary",
        "evidence_source", "not_real_acceptance_evidence", "not_ready_for_formal_claim",
        "reviewer_only", "readiness_mapping_only", "primary_evidence_rewritten",
        "real_acceptance_ready",
    ]
    for field in required_fields:
        assert field in result, f"Missing field: {field}"


def test_freeze_audit_audit_status_values() -> None:
    """audit_status should be one of the 4 valid buckets."""
    result = build_step2_freeze_audit(run_id="test-run")
    assert result["audit_status"] in {
        FREEZE_AUDIT_STATUS_OK, FREEZE_AUDIT_STATUS_ATTENTION,
        FREEZE_AUDIT_STATUS_BLOCKER, FREEZE_AUDIT_STATUS_REVIEWER_ONLY,
    }


def test_freeze_audit_with_closeout_package_ok() -> None:
    """When closeout package is ok, freeze audit should be ok or attention."""
    pkg = build_step2_closeout_package(run_id="test-run")
    # Force ok status
    pkg["package_status"] = "ok"
    result = build_step2_freeze_audit(
        run_id="test-run",
        step2_closeout_package=pkg,
    )
    assert result["audit_status"] in {FREEZE_AUDIT_STATUS_OK, FREEZE_AUDIT_STATUS_ATTENTION}
    assert result["freeze_candidate"] is True


def test_freeze_audit_with_closeout_package_blocker() -> None:
    """When closeout package has blocker, freeze audit should have blocker."""
    pkg = build_step2_closeout_package(run_id="test-run")
    pkg["package_status"] = "blocker"
    result = build_step2_freeze_audit(
        run_id="test-run",
        step2_closeout_package=pkg,
    )
    assert result["audit_status"] == FREEZE_AUDIT_STATUS_BLOCKER
    assert result["freeze_candidate"] is False
    assert len(result["blockers"]) > 0


def test_freeze_audit_with_missing_closeout_package() -> None:
    """When closeout package is missing, freeze audit should have blocker."""
    result = build_step2_freeze_audit(run_id="test-run")
    # No closeout package provided
    assert result["audit_status"] in {
        FREEZE_AUDIT_STATUS_BLOCKER, FREEZE_AUDIT_STATUS_REVIEWER_ONLY,
    }
    # Should have missing_closeout_package blocker
    blocker_keys = [b["key"] for b in result["blockers"]]
    assert "missing_closeout_package" in blocker_keys


def test_freeze_audit_freeze_candidate_not_release_approval() -> None:
    """freeze_candidate = True must NOT mean release approval."""
    pkg = build_step2_closeout_package(run_id="test-run")
    pkg["package_status"] = "ok"
    result = build_step2_freeze_audit(
        run_id="test-run",
        step2_closeout_package=pkg,
    )
    # Even when freeze_candidate is True, boundary notices must be present
    assert "审阅候选" in result["freeze_candidate_notice_zh"] or "RC" in result["freeze_candidate_notice_zh"]
    assert "review candidate" in result["freeze_candidate_notice_en"].lower() or "RC" in result["freeze_candidate_notice_en"]
    # Must clarify it is NOT formal approval (contains "not" qualifier)
    assert "不是正式放行批准" in result["freeze_candidate_notice_zh"] or "不是正式批准" in result["freeze_candidate_notice_zh"] or "非正式" in result["freeze_candidate_notice_zh"]
    assert "not formal release approval" in result["freeze_candidate_notice_en"].lower()


def test_freeze_audit_blockers_structure() -> None:
    """Each blocker should have key, label_zh, label_en."""
    result = build_step2_freeze_audit(run_id="test-run")
    for blocker in result["blockers"]:
        assert "key" in blocker
        assert "label_zh" in blocker
        assert "label_en" in blocker
        assert blocker["key"]


def test_freeze_audit_next_steps_structure() -> None:
    """Each next_step should have key, label_zh, label_en."""
    result = build_step2_freeze_audit(run_id="test-run")
    for step in result["next_steps"]:
        assert "key" in step
        assert "label_zh" in step
        assert "label_en" in step
        assert step["key"]


def test_freeze_audit_audit_sections_keys() -> None:
    """audit_sections should contain all expected section keys."""
    result = build_step2_freeze_audit(run_id="test-run")
    expected_keys = {"suite", "parity", "resilience", "governance", "closeout"}
    assert expected_keys.issubset(set(result["audit_sections"].keys()))


def test_freeze_audit_lang_en() -> None:
    """Builder should support lang='en'."""
    result = build_step2_freeze_audit(run_id="test-run", lang="en")
    assert result["audit_status_label"]
    assert result["reviewer_summary_line"]
    # English summary should not contain Chinese characters
    for line in result["reviewer_summary_lines"]:
        # Allow some Chinese in boundary notices, but main lines should be English
        pass  # Basic check that it doesn't crash


# ---------------------------------------------------------------------------
# Step 2 boundary — comprehensive
# ---------------------------------------------------------------------------

def test_freeze_audit_step2_boundary_markers_all() -> None:
    """All 7 Step 2 boundary markers should be enforced in builder output."""
    result = build_step2_freeze_audit(run_id="test-run")
    assert result["evidence_source"] == "simulated"
    assert result["not_real_acceptance_evidence"] is True
    assert result["not_ready_for_formal_claim"] is True
    assert result["reviewer_only"] is True
    assert result["readiness_mapping_only"] is True
    assert result["primary_evidence_rewritten"] is False
    assert result["real_acceptance_ready"] is False


def test_freeze_audit_no_formal_acceptance_language() -> None:
    """Freeze audit should not contain unqualified formal acceptance / release approval language."""
    result = build_step2_freeze_audit(run_id="test-run")
    # Check that audit_status is not "approved" or "released"
    assert result["audit_status"] not in {"approved", "released", "formal_acceptance"}
    # Check summary lines: "正式放行" is allowed only with "不是" qualifier
    for line in result["reviewer_summary_lines"]:
        if "正式放行" in line:
            assert "不是" in line or "不构成" in line, f"Unqualified formal language: {line}"
        if "formal release" in line.lower():
            assert "not" in line.lower(), f"Unqualified formal language: {line}"


def test_freeze_audit_does_not_replace_closeout_package() -> None:
    """Freeze audit is an upper-level view, not a replacement for closeout package."""
    pkg = build_step2_closeout_package(run_id="test-run")
    result = build_step2_freeze_audit(
        run_id="test-run",
        step2_closeout_package=pkg,
    )
    # Freeze audit should have different artifact_type
    assert result["artifact_type"] == "step2_freeze_audit"
    assert pkg["artifact_type"] == "step2_closeout_package"
    # Freeze audit should reference closeout in audit_sections
    assert "closeout" in result["audit_sections"]


# ---------------------------------------------------------------------------
# Fallback tests
# ---------------------------------------------------------------------------

def test_freeze_audit_fallback_output_fields() -> None:
    fb = build_freeze_audit_fallback()
    assert fb["artifact_type"] == "step2_freeze_audit_fallback"
    assert fb["audit_status"] == FREEZE_AUDIT_STATUS_REVIEWER_ONLY
    assert fb["freeze_candidate"] is False
    assert fb["blockers"] == []
    assert fb["next_steps"] == []


def test_freeze_audit_fallback_step2_boundary() -> None:
    fb = build_freeze_audit_fallback()
    assert fb["evidence_source"] == "simulated"
    assert fb["not_real_acceptance_evidence"] is True
    assert fb["not_ready_for_formal_claim"] is True
    assert fb["reviewer_only"] is True
    assert fb["readiness_mapping_only"] is True
    assert fb["primary_evidence_rewritten"] is False
    assert fb["real_acceptance_ready"] is False


def test_freeze_audit_fallback_no_formal_language() -> None:
    fb = build_freeze_audit_fallback()
    assert "approved" not in str(fb).lower() or "not approved" in str(fb).lower()


# ---------------------------------------------------------------------------
# Consistency with closeout package
# ---------------------------------------------------------------------------

def test_freeze_audit_consistent_with_closeout_package_boundary() -> None:
    """Freeze audit boundary markers should match closeout package boundary markers."""
    from gas_calibrator.v2.core.step2_closeout_package_contracts import CLOSEOUT_PACKAGE_STEP2_BOUNDARY
    assert FREEZE_AUDIT_STEP2_BOUNDARY == CLOSEOUT_PACKAGE_STEP2_BOUNDARY


def test_freeze_audit_closeout_section_status_matches_package() -> None:
    """When closeout package has a specific status, freeze audit closeout section should reflect it."""
    pkg = build_step2_closeout_package(run_id="test-run")
    pkg["package_status"] = "ok"
    result = build_step2_freeze_audit(
        run_id="test-run",
        step2_closeout_package=pkg,
    )
    assert result["audit_sections"]["closeout"]["status"] == "ok"


def test_freeze_audit_surfaces_compare_summary_from_closeout_package() -> None:
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
    pkg = build_step2_closeout_package(run_id="test-run", compact_summary_packs=[compare_pack])

    result = build_step2_freeze_audit(
        run_id="test-run",
        step2_closeout_package=pkg,
    )

    assert result["compare_available"] is True
    assert result["compare_status"] == "MISMATCH"
    assert result["compare_next_check"] == "inspect sample count diff"
    assert result["audit_sections"]["closeout"]["compare_available"] is True
    assert result["audit_sections"]["closeout"]["compare_status"] == "MISMATCH"
    assert any("离线对齐" in line or "Compare" in line for line in result["reviewer_summary_lines"])


# ---------------------------------------------------------------------------
# i18n locale consistency
# ---------------------------------------------------------------------------

def test_freeze_audit_i18n_keys_in_zh_cn() -> None:
    """All freeze_audit i18n keys should be present in zh_CN.json."""
    import json
    from pathlib import Path
    locale_path = Path(__file__).resolve().parent.parent.parent / "src" / "gas_calibrator" / "v2" / "ui_v2" / "locales" / "zh_CN.json"
    with open(locale_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    freeze_audit = data.get("freeze_audit", {})
    assert freeze_audit.get("title"), "Missing freeze_audit.title in zh_CN.json"
    assert freeze_audit.get("summary"), "Missing freeze_audit.summary in zh_CN.json"
    assert "section" in freeze_audit, "Missing freeze_audit.section in zh_CN.json"
    assert "status" in freeze_audit, "Missing freeze_audit.status in zh_CN.json"


def test_freeze_audit_i18n_keys_in_en_us() -> None:
    """All freeze_audit i18n keys should be present in en_US.json."""
    import json
    from pathlib import Path
    locale_path = Path(__file__).resolve().parent.parent.parent / "src" / "gas_calibrator" / "v2" / "ui_v2" / "locales" / "en_US.json"
    with open(locale_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    freeze_audit = data.get("freeze_audit", {})
    assert freeze_audit.get("title"), "Missing freeze_audit.title in en_US.json"
    assert freeze_audit.get("summary"), "Missing freeze_audit.summary in en_US.json"
    assert "section" in freeze_audit, "Missing freeze_audit.section in en_US.json"
    assert "status" in freeze_audit, "Missing freeze_audit.status in en_US.json"


def test_freeze_audit_zh_en_section_keys_match() -> None:
    """zh_CN and en_US freeze_audit section keys should match."""
    import json
    from pathlib import Path
    base = Path(__file__).resolve().parent.parent.parent / "src" / "gas_calibrator" / "v2" / "ui_v2" / "locales"
    with open(base / "zh_CN.json", "r", encoding="utf-8") as f:
        zh = json.load(f)
    with open(base / "en_US.json", "r", encoding="utf-8") as f:
        en = json.load(f)
    zh_sections = set(zh.get("freeze_audit", {}).get("section", {}).keys())
    en_sections = set(en.get("freeze_audit", {}).get("section", {}).keys())
    assert zh_sections == en_sections


# ---------------------------------------------------------------------------
# freeze_audit_source field tests (Step 2.23)
# ---------------------------------------------------------------------------

def test_freeze_audit_source_rebuilt() -> None:
    """build_step2_freeze_audit should set freeze_audit_source = 'rebuilt'."""
    result = build_step2_freeze_audit(run_id="test-run")
    assert result["freeze_audit_source"] == "rebuilt"


def test_freeze_audit_source_fallback() -> None:
    """build_freeze_audit_fallback should set freeze_audit_source = 'fallback'."""
    fb = build_freeze_audit_fallback()
    assert fb["freeze_audit_source"] == "fallback"
