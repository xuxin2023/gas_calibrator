"""Tests for Step 3 admission dossier contracts and builder.

Verifies:
- Contract key coverage and zh/en consistency
- Builder output stability (dossier_status, blockers, next_steps, admission_candidate)
- Simulation-only / reviewer-only / non-claim boundary enforcement
- admission_candidate means "review candidate material ready", NOT "Step 3 approval"
- Step 2 boundary assertions continue to pass
- Fallback behavior
- Consistency with freeze_audit / closeout_package
"""

from __future__ import annotations

import pytest

from gas_calibrator.v2.adapters.results_gateway import ResultsGateway
from gas_calibrator.v2.core.step3_admission_dossier_contracts import (
    ADMISSION_DOSSIER_CONTRACTS_VERSION,
    ADMISSION_DOSSIER_I18N_KEYS,
    ADMISSION_DOSSIER_SECTION_LABELS_EN,
    ADMISSION_DOSSIER_SECTION_LABELS_ZH,
    ADMISSION_DOSSIER_SECTION_ORDER,
    ADMISSION_DOSSIER_STEP2_BOUNDARY,
    ADMISSION_DOSSIER_STATUS_OK,
    ADMISSION_DOSSIER_STATUS_ATTENTION,
    ADMISSION_DOSSIER_STATUS_BLOCKER,
    ADMISSION_DOSSIER_STATUS_REVIEWER_ONLY,
    ADMISSION_DOSSIER_BLOCKER_LABELS_ZH,
    ADMISSION_DOSSIER_BLOCKER_LABELS_EN,
    ADMISSION_DOSSIER_BLOCKER_KEYS,
    ADMISSION_DOSSIER_NEXT_STEPS_ZH,
    ADMISSION_DOSSIER_NEXT_STEPS_EN,
    ADMISSION_DOSSIER_NEXT_STEP_KEYS,
    ADMISSION_DOSSIER_TITLE_ZH,
    ADMISSION_DOSSIER_TITLE_EN,
    resolve_admission_dossier_title,
    resolve_admission_dossier_section_label,
    resolve_admission_dossier_status_label,
    resolve_admission_candidate_notice,
    resolve_admission_dossier_simulation_only_boundary,
)
from gas_calibrator.v2.core.step3_admission_dossier_builder import (
    build_step3_admission_dossier,
    build_admission_dossier_fallback,
    ADMISSION_DOSSIER_BUILDER_VERSION,
)
from gas_calibrator.v2.core.step2_freeze_audit_builder import build_step2_freeze_audit
from gas_calibrator.v2.core.step2_closeout_package_builder import build_step2_closeout_package


# ---------------------------------------------------------------------------
# Contract tests
# ---------------------------------------------------------------------------

def test_dossier_contracts_version() -> None:
    assert ADMISSION_DOSSIER_CONTRACTS_VERSION == "2.24.0"


def test_dossier_builder_version() -> None:
    assert ADMISSION_DOSSIER_BUILDER_VERSION == "2.24.0"


def test_dossier_section_labels_zh_en_consistent() -> None:
    assert set(ADMISSION_DOSSIER_SECTION_LABELS_ZH.keys()) == set(ADMISSION_DOSSIER_SECTION_LABELS_EN.keys())


def test_dossier_section_order_covers_all_labels() -> None:
    for key in ADMISSION_DOSSIER_SECTION_ORDER:
        assert key in ADMISSION_DOSSIER_SECTION_LABELS_ZH
        assert key in ADMISSION_DOSSIER_SECTION_LABELS_EN


def test_dossier_blocker_labels_zh_en_consistent() -> None:
    assert set(ADMISSION_DOSSIER_BLOCKER_LABELS_ZH.keys()) == set(ADMISSION_DOSSIER_BLOCKER_LABELS_EN.keys())
    for key in ADMISSION_DOSSIER_BLOCKER_KEYS:
        assert key in ADMISSION_DOSSIER_BLOCKER_LABELS_ZH
        assert key in ADMISSION_DOSSIER_BLOCKER_LABELS_EN


def test_dossier_next_step_labels_zh_en_consistent() -> None:
    assert set(ADMISSION_DOSSIER_NEXT_STEPS_ZH.keys()) == set(ADMISSION_DOSSIER_NEXT_STEPS_EN.keys())
    for key in ADMISSION_DOSSIER_NEXT_STEP_KEYS:
        assert key in ADMISSION_DOSSIER_NEXT_STEPS_ZH
        assert key in ADMISSION_DOSSIER_NEXT_STEPS_EN


def test_dossier_i18n_keys_present() -> None:
    required_keys = [
        "title", "summary",
        "section_freeze_audit", "section_closeout_package", "section_closeout_readiness",
        "section_governance_handoff", "section_parity_resilience", "section_phase_evidence",
        "section_blockers", "section_next_steps", "section_boundary",
        "status_ok", "status_attention", "status_blocker", "status_reviewer_only",
        "simulation_only_boundary", "reviewer_only_notice", "non_claim_notice",
        "admission_candidate_notice", "no_content", "panel", "dossier_status",
        "admission_dossier_source", "dossier_sections", "admission_candidate",
    ]
    for key in required_keys:
        assert key in ADMISSION_DOSSIER_I18N_KEYS, f"Missing i18n key: {key}"


def test_dossier_step2_boundary_markers() -> None:
    assert ADMISSION_DOSSIER_STEP2_BOUNDARY["evidence_source"] == "simulated"
    assert ADMISSION_DOSSIER_STEP2_BOUNDARY["not_real_acceptance_evidence"] is True
    assert ADMISSION_DOSSIER_STEP2_BOUNDARY["not_ready_for_formal_claim"] is True
    assert ADMISSION_DOSSIER_STEP2_BOUNDARY["reviewer_only"] is True
    assert ADMISSION_DOSSIER_STEP2_BOUNDARY["readiness_mapping_only"] is True
    assert ADMISSION_DOSSIER_STEP2_BOUNDARY["primary_evidence_rewritten"] is False
    assert ADMISSION_DOSSIER_STEP2_BOUNDARY["real_acceptance_ready"] is False


# ---------------------------------------------------------------------------
# Resolve helper tests
# ---------------------------------------------------------------------------

def test_resolve_dossier_title() -> None:
    assert resolve_admission_dossier_title(lang="zh") == ADMISSION_DOSSIER_TITLE_ZH
    assert resolve_admission_dossier_title(lang="en") == ADMISSION_DOSSIER_TITLE_EN


def test_resolve_admission_candidate_notice() -> None:
    zh = resolve_admission_candidate_notice(lang="zh")
    en = resolve_admission_candidate_notice(lang="en")
    # Must contain "审阅候选" or "候选材料"
    assert "候选" in zh
    assert "candidate" in en.lower()
    # Must NOT be unqualified formal approval
    assert "不是" in zh or "非" in zh
    assert "not" in en.lower()


# ---------------------------------------------------------------------------
# Builder tests
# ---------------------------------------------------------------------------

def test_dossier_builder_output_fields() -> None:
    result = build_step3_admission_dossier(run_id="test-run")
    required_fields = [
        "schema_version", "artifact_type", "generated_at", "run_id", "phase",
        "dossier_version", "dossier_status", "dossier_status_label",
        "reviewer_summary_line", "reviewer_summary_lines",
        "blockers", "next_steps", "dossier_sections", "section_order",
        "admission_candidate", "admission_candidate_notice_zh", "admission_candidate_notice_en",
        "simulation_only_boundary", "source_versions", "admission_dossier_source",
        "evidence_source", "not_real_acceptance_evidence", "not_ready_for_formal_claim",
        "reviewer_only", "readiness_mapping_only", "primary_evidence_rewritten",
        "real_acceptance_ready",
    ]
    for field in required_fields:
        assert field in result, f"Missing field: {field}"


def test_dossier_status_values() -> None:
    result = build_step3_admission_dossier(run_id="test-run")
    assert result["dossier_status"] in {
        ADMISSION_DOSSIER_STATUS_OK, ADMISSION_DOSSIER_STATUS_ATTENTION,
        ADMISSION_DOSSIER_STATUS_BLOCKER, ADMISSION_DOSSIER_STATUS_REVIEWER_ONLY,
    }


def test_dossier_always_has_real_acceptance_not_ready_blocker() -> None:
    """In Step 2, real_acceptance_not_ready is always a blocker."""
    result = build_step3_admission_dossier(run_id="test-run")
    blocker_keys = [b["key"] for b in result["blockers"]]
    assert "real_acceptance_not_ready" in blocker_keys


def test_dossier_admission_candidate_always_false_in_step2() -> None:
    """In Step 2, admission_candidate should always be False because
    real_acceptance_not_ready is always a blocker."""
    result = build_step3_admission_dossier(run_id="test-run")
    assert result["admission_candidate"] is False


def test_dossier_with_all_inputs() -> None:
    """When all inputs are provided, dossier should have complete sections."""
    pkg = build_step2_closeout_package(run_id="test-run")
    pkg["package_status"] = "ok"
    audit = build_step2_freeze_audit(run_id="test-run", step2_closeout_package=pkg)
    result = build_step3_admission_dossier(
        run_id="test-run",
        step2_freeze_audit=audit,
        step2_closeout_package=pkg,
    )
    assert "freeze_audit" in result["dossier_sections"]
    assert "closeout_package" in result["dossier_sections"]
    # Still has real_acceptance_not_ready blocker
    blocker_keys = [b["key"] for b in result["blockers"]]
    assert "real_acceptance_not_ready" in blocker_keys


def test_dossier_blockers_structure() -> None:
    result = build_step3_admission_dossier(run_id="test-run")
    for blocker in result["blockers"]:
        assert "key" in blocker
        assert "label_zh" in blocker
        assert "label_en" in blocker


def test_dossier_next_steps_structure() -> None:
    result = build_step3_admission_dossier(run_id="test-run")
    for step in result["next_steps"]:
        assert "key" in step
        assert "label_zh" in step
        assert "label_en" in step


def test_dossier_dossier_sections_keys() -> None:
    result = build_step3_admission_dossier(run_id="test-run")
    expected_keys = {"freeze_audit", "closeout_package", "closeout_readiness",
                     "governance_handoff", "parity_resilience", "phase_evidence"}
    assert expected_keys.issubset(set(result["dossier_sections"].keys()))


def test_dossier_lang_en() -> None:
    result = build_step3_admission_dossier(run_id="test-run", lang="en")
    assert result["dossier_status_label"]
    assert result["reviewer_summary_line"]


# ---------------------------------------------------------------------------
# Step 2 boundary — comprehensive
# ---------------------------------------------------------------------------

def test_dossier_step2_boundary_markers_all() -> None:
    result = build_step3_admission_dossier(run_id="test-run")
    assert result["evidence_source"] == "simulated"
    assert result["not_real_acceptance_evidence"] is True
    assert result["not_ready_for_formal_claim"] is True
    assert result["reviewer_only"] is True
    assert result["readiness_mapping_only"] is True
    assert result["primary_evidence_rewritten"] is False
    assert result["real_acceptance_ready"] is False


def test_dossier_no_formal_acceptance_language() -> None:
    result = build_step3_admission_dossier(run_id="test-run")
    assert result["dossier_status"] not in {"approved", "released", "formal_acceptance"}
    for line in result["reviewer_summary_lines"]:
        if "正式放行" in line:
            assert "不是" in line or "不构成" in line
        if "formal release" in line.lower():
            assert "not" in line.lower()


def test_dossier_admission_candidate_not_step3_approval() -> None:
    """admission_candidate notice must clarify it is NOT Step 3 approval."""
    result = build_step3_admission_dossier(run_id="test-run")
    zh_notice = result["admission_candidate_notice_zh"]
    en_notice = result["admission_candidate_notice_en"]
    assert "不是" in zh_notice or "非" in zh_notice
    assert "not" in en_notice.lower()


# ---------------------------------------------------------------------------
# Fallback tests
# ---------------------------------------------------------------------------

def test_dossier_fallback_output_fields() -> None:
    fb = build_admission_dossier_fallback()
    assert fb["artifact_type"] == "step3_admission_dossier_fallback"
    assert fb["dossier_status"] == ADMISSION_DOSSIER_STATUS_REVIEWER_ONLY
    assert fb["admission_candidate"] is False


def test_results_gateway_exposes_final_closure_matrix_surface_guardrails(tmp_path) -> None:
    payload = ResultsGateway(tmp_path).read_results_payload()

    matrix = dict(payload.get("step2_final_closure_matrix") or {})
    assert matrix["artifact_type"] == "step2_final_closure_matrix"
    assert "results" in matrix["audited_surfaces"]
    assert "reports" in matrix["audited_surfaces"]
    assert "historical" in matrix["audited_surfaces"]
    assert "review_index" not in matrix["audited_surfaces"]
    assert matrix["missing_surfaces"] == []
    assert matrix["not_real_acceptance_evidence"] is True
    assert matrix["not_ready_for_formal_claim"] is True
    assert matrix["real_acceptance_ready"] is False


def test_results_gateway_does_not_swallow_non_compat_closeout_verification_errors(
    tmp_path,
    monkeypatch,
) -> None:
    import gas_calibrator.v2.adapters.results_gateway as gateway_module

    def _boom(**kwargs):
        raise RuntimeError("closeout verification exploded")

    monkeypatch.setattr(
        gateway_module,
        "build_step2_closeout_verification_surface_payload",
        _boom,
    )

    with pytest.raises(RuntimeError, match="closeout verification exploded"):
        ResultsGateway(tmp_path).read_results_payload()


def test_dossier_fallback_step2_boundary() -> None:
    fb = build_admission_dossier_fallback()
    assert fb["evidence_source"] == "simulated"
    assert fb["not_real_acceptance_evidence"] is True
    assert fb["real_acceptance_ready"] is False


# ---------------------------------------------------------------------------
# Consistency with freeze_audit / closeout_package
# ---------------------------------------------------------------------------

def test_dossier_boundary_matches_closeout_package() -> None:
    from gas_calibrator.v2.core.step2_closeout_package_contracts import CLOSEOUT_PACKAGE_STEP2_BOUNDARY
    assert ADMISSION_DOSSIER_STEP2_BOUNDARY == CLOSEOUT_PACKAGE_STEP2_BOUNDARY


def test_dossier_boundary_matches_freeze_audit() -> None:
    from gas_calibrator.v2.core.step2_freeze_audit_contracts import FREEZE_AUDIT_STEP2_BOUNDARY
    assert ADMISSION_DOSSIER_STEP2_BOUNDARY == FREEZE_AUDIT_STEP2_BOUNDARY


# ---------------------------------------------------------------------------
# i18n locale consistency
# ---------------------------------------------------------------------------

def test_dossier_i18n_keys_in_zh_cn() -> None:
    import json
    from pathlib import Path
    locale_path = Path(__file__).resolve().parent.parent.parent / "src" / "gas_calibrator" / "v2" / "ui_v2" / "locales" / "zh_CN.json"
    with open(locale_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    dossier = data.get("admission_dossier", {})
    assert dossier.get("title"), "Missing admission_dossier.title in zh_CN.json"
    assert "section" in dossier
    assert "status" in dossier


def test_dossier_i18n_keys_in_en_us() -> None:
    import json
    from pathlib import Path
    locale_path = Path(__file__).resolve().parent.parent.parent / "src" / "gas_calibrator" / "v2" / "ui_v2" / "locales" / "en_US.json"
    with open(locale_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    dossier = data.get("admission_dossier", {})
    assert dossier.get("title"), "Missing admission_dossier.title in en_US.json"
    assert "section" in dossier
    assert "status" in dossier


def test_dossier_zh_en_section_keys_match() -> None:
    import json
    from pathlib import Path
    base = Path(__file__).resolve().parent.parent.parent / "src" / "gas_calibrator" / "v2" / "ui_v2" / "locales"
    with open(base / "zh_CN.json", "r", encoding="utf-8") as f:
        zh = json.load(f)
    with open(base / "en_US.json", "r", encoding="utf-8") as f:
        en = json.load(f)
    zh_sections = set(zh.get("admission_dossier", {}).get("section", {}).keys())
    en_sections = set(en.get("admission_dossier", {}).get("section", {}).keys())
    assert zh_sections == en_sections


# ---------------------------------------------------------------------------
# Step 2.24: admission_dossier_source and persistence tests
# ---------------------------------------------------------------------------

def test_dossier_admission_dossier_source_rebuilt() -> None:
    """Builder default should set admission_dossier_source = 'rebuilt'."""
    result = build_step3_admission_dossier(run_id="test-run")
    assert result["admission_dossier_source"] == "rebuilt"


def test_dossier_fallback_admission_dossier_source() -> None:
    """Fallback should set admission_dossier_source = 'fallback'."""
    fb = build_admission_dossier_fallback()
    assert fb["admission_dossier_source"] == "fallback"


def test_dossier_persisted_source_can_be_overridden() -> None:
    """When results_gateway sets admission_dossier_source = 'persisted',
    the field should be overridable."""
    result = build_step3_admission_dossier(run_id="test-run")
    result["admission_dossier_source"] = "persisted"
    assert result["admission_dossier_source"] == "persisted"


def test_dossier_consumable_fields_present() -> None:
    """All consumable fields should be present in the dossier output."""
    result = build_step3_admission_dossier(run_id="test-run")
    consumable_fields = [
        "step3_admission_dossier",  # key name in results payload (not in dossier itself)
        "admission_dossier_source",
        "dossier_status",
        "reviewer_summary_line",
        "reviewer_summary_lines",
        "blockers",
        "next_steps",
        "dossier_sections",
        "admission_candidate",
    ]
    # dossier itself has all except the payload key
    for field in consumable_fields:
        if field == "step3_admission_dossier":
            continue  # this is the key in the outer payload, not in the dossier
        assert field in result, f"Missing consumable field: {field}"


def test_dossier_results_gateway_includes_dossier() -> None:
    """results_gateway read_results_payload should include step3_admission_dossier."""
    from gas_calibrator.v2.adapters.results_gateway import ResultsGateway
    from pathlib import Path
    # Use a minimal temp dir to verify the key exists in the output
    import tempfile
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir_path = Path(tmpdir)
        gw = ResultsGateway(tmpdir_path)
        payload = gw.read_results_payload()
        assert "step3_admission_dossier" in payload
        dossier = dict(payload["step3_admission_dossier"] or {})
        assert dossier.get("artifact_type") in ("step3_admission_dossier", "step3_admission_dossier_fallback")
        assert dossier.get("admission_dossier_source") in ("persisted", "rebuilt", "fallback")


def test_dossier_results_gateway_reports_payload_includes_dossier() -> None:
    """results_gateway read_reports_payload should include step3_admission_dossier."""
    from gas_calibrator.v2.adapters.results_gateway import ResultsGateway
    from pathlib import Path
    import tempfile
    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir_path = Path(tmpdir)
        gw = ResultsGateway(tmpdir_path)
        payload = gw.read_reports_payload()
        assert "step3_admission_dossier" in payload
