"""Tests for Step 2 final closeout verification.

Verifies:
- Builder output stability (verification_status, blockers, next_steps, missing_for_step3)
- Simulation-only / reviewer-only / non-claim boundary enforcement
- verification_status only expresses Step 2 closeout candidate state
- missing_for_step3 explicitly lists real acceptance gaps
- Fallback behavior
- Step 2 boundary assertions
"""

from __future__ import annotations

from gas_calibrator.v2.core.step2_closeout_verification import (
    CLOSEOUT_VERIFICATION_VERSION,
    VERIFICATION_STATUS_CANDIDATE,
    VERIFICATION_STATUS_BLOCKER,
    VERIFICATION_STATUS_REVIEWER_ONLY,
    build_step2_closeout_verification,
    build_closeout_verification_fallback,
)


# ---------------------------------------------------------------------------
# Version
# ---------------------------------------------------------------------------

def test_closeout_verification_version() -> None:
    assert CLOSEOUT_VERIFICATION_VERSION == "2.24.0"


# ---------------------------------------------------------------------------
# Builder output
# ---------------------------------------------------------------------------

def test_verification_output_fields() -> None:
    result = build_step2_closeout_verification(run_id="test-run")
    required_fields = [
        "schema_version", "artifact_type", "generated_at", "run_id", "phase",
        "verification_version", "verification_status", "reviewer_summary_line",
        "blockers", "next_steps", "missing_for_step3", "simulation_only_boundary",
        "closeout_readiness_status", "closeout_package_status",
        "freeze_audit_status", "dossier_status",
        "evidence_source", "not_real_acceptance_evidence", "not_ready_for_formal_claim",
        "reviewer_only", "readiness_mapping_only", "primary_evidence_rewritten",
        "real_acceptance_ready",
    ]
    for field in required_fields:
        assert field in result, f"Missing field: {field}"


def test_verification_status_values() -> None:
    result = build_step2_closeout_verification(run_id="test-run")
    assert result["verification_status"] in {
        VERIFICATION_STATUS_CANDIDATE, VERIFICATION_STATUS_BLOCKER,
        VERIFICATION_STATUS_REVIEWER_ONLY,
    }


def test_verification_always_has_real_acceptance_not_ready_blocker() -> None:
    result = build_step2_closeout_verification(run_id="test-run")
    blocker_keys = [b["key"] for b in result["blockers"]]
    assert "real_acceptance_not_ready" in blocker_keys


def test_verification_status_is_candidate_when_only_real_acceptance_blocker() -> None:
    """When only real_acceptance_not_ready is a blocker, status should be candidate."""
    from gas_calibrator.v2.core.step2_closeout_readiness_builder import build_step2_closeout_readiness
    from gas_calibrator.v2.core.step2_closeout_package_builder import build_step2_closeout_package
    from gas_calibrator.v2.core.step2_freeze_audit_builder import build_step2_freeze_audit
    from gas_calibrator.v2.core.step3_admission_dossier_builder import build_step3_admission_dossier

    readiness = build_step2_closeout_readiness(run_id="test-run")
    readiness["closeout_status"] = "ok"
    pkg = build_step2_closeout_package(run_id="test-run", step2_closeout_readiness=readiness)
    pkg["package_status"] = "ok"
    audit = build_step2_freeze_audit(run_id="test-run", step2_closeout_package=pkg, step2_closeout_readiness=readiness)
    audit["audit_status"] = "ok"
    dossier = build_step3_admission_dossier(
        run_id="test-run",
        step2_freeze_audit=audit,
        step2_closeout_package=pkg,
        step2_closeout_readiness=readiness,
    )

    result = build_step2_closeout_verification(
        run_id="test-run",
        step2_closeout_readiness=readiness,
        step2_closeout_package=pkg,
        step2_freeze_audit=audit,
        step3_admission_dossier=dossier,
    )
    # With all sub-artifacts ok, only real_acceptance_not_ready remains
    assert result["verification_status"] == VERIFICATION_STATUS_CANDIDATE


def test_verification_status_is_blocker_when_sub_artifact_has_blocker() -> None:
    """When a sub-artifact has a blocker, verification_status should be blocker."""
    result = build_step2_closeout_verification(
        run_id="test-run",
        step2_closeout_readiness={"closeout_status": "blocker"},
    )
    assert result["verification_status"] == VERIFICATION_STATUS_BLOCKER


# ---------------------------------------------------------------------------
# missing_for_step3
# ---------------------------------------------------------------------------

def test_missing_for_step3_zh() -> None:
    result = build_step2_closeout_verification(run_id="test-run", lang="zh")
    missing = list(result["missing_for_step3"] or [])
    assert len(missing) >= 3
    # Must mention real device / real acceptance
    all_text = " ".join(missing)
    assert "真实" in all_text or "real" in all_text.lower()


def test_missing_for_step3_en() -> None:
    result = build_step2_closeout_verification(run_id="test-run", lang="en")
    missing = list(result["missing_for_step3"] or [])
    assert len(missing) >= 3
    all_text = " ".join(missing)
    assert "real" in all_text.lower()


# ---------------------------------------------------------------------------
# Step 2 boundary
# ---------------------------------------------------------------------------

def test_verification_step2_boundary_markers() -> None:
    result = build_step2_closeout_verification(run_id="test-run")
    assert result["evidence_source"] == "simulated"
    assert result["not_real_acceptance_evidence"] is True
    assert result["not_ready_for_formal_claim"] is True
    assert result["reviewer_only"] is True
    assert result["readiness_mapping_only"] is True
    assert result["primary_evidence_rewritten"] is False
    assert result["real_acceptance_ready"] is False


def test_verification_no_formal_acceptance_language() -> None:
    result = build_step2_closeout_verification(run_id="test-run")
    assert result["verification_status"] not in {"approved", "released", "formal_acceptance"}
    summary = result["reviewer_summary_line"]
    if "正式放行" in summary:
        assert "不是" in summary or "不构成" in summary
    if "formal release" in summary.lower():
        assert "not" in summary.lower()


def test_verification_simulation_only_boundary() -> None:
    result = build_step2_closeout_verification(run_id="test-run")
    boundary = result["simulation_only_boundary"]
    assert boundary
    assert "仿真" in boundary or "simulation" in boundary.lower()


# ---------------------------------------------------------------------------
# Blockers and next_steps structure
# ---------------------------------------------------------------------------

def test_verification_blockers_structure() -> None:
    result = build_step2_closeout_verification(run_id="test-run")
    for blocker in result["blockers"]:
        assert "key" in blocker
        assert "label_zh" in blocker
        assert "label_en" in blocker


def test_verification_next_steps_structure() -> None:
    result = build_step2_closeout_verification(run_id="test-run")
    for step in result["next_steps"]:
        assert "key" in step
        assert "label_zh" in step
        assert "label_en" in step


def test_verification_next_steps_always_include_real_device() -> None:
    result = build_step2_closeout_verification(run_id="test-run")
    step_keys = [s["key"] for s in result["next_steps"]]
    assert "obtain_real_device_access" in step_keys
    assert "run_step3_real_validation" in step_keys


# ---------------------------------------------------------------------------
# Fallback
# ---------------------------------------------------------------------------

def test_verification_fallback_output_fields() -> None:
    fb = build_closeout_verification_fallback()
    assert fb["artifact_type"] == "step2_closeout_verification_fallback"
    assert fb["verification_status"] == VERIFICATION_STATUS_REVIEWER_ONLY


def test_verification_fallback_step2_boundary() -> None:
    fb = build_closeout_verification_fallback()
    assert fb["evidence_source"] == "simulated"
    assert fb["not_real_acceptance_evidence"] is True
    assert fb["real_acceptance_ready"] is False


# ---------------------------------------------------------------------------
# Consistency with admission_dossier / closeout_package / freeze_audit
# ---------------------------------------------------------------------------

def test_verification_boundary_matches_admission_dossier() -> None:
    from gas_calibrator.v2.core.step3_admission_dossier_contracts import ADMISSION_DOSSIER_STEP2_BOUNDARY
    result = build_step2_closeout_verification(run_id="test-run")
    assert result["evidence_source"] == ADMISSION_DOSSIER_STEP2_BOUNDARY["evidence_source"]
    assert result["not_real_acceptance_evidence"] == ADMISSION_DOSSIER_STEP2_BOUNDARY["not_real_acceptance_evidence"]
    assert result["real_acceptance_ready"] == ADMISSION_DOSSIER_STEP2_BOUNDARY["real_acceptance_ready"]


def test_verification_source_status_snapshots() -> None:
    """When sub-artifacts are provided, their status should be captured."""
    result = build_step2_closeout_verification(
        run_id="test-run",
        step2_closeout_readiness={"closeout_status": "ok"},
        step2_closeout_package={"package_status": "ok"},
        step2_freeze_audit={"audit_status": "ok"},
        step3_admission_dossier={"dossier_status": "blocker"},
    )
    assert result["closeout_readiness_status"] == "ok"
    assert result["closeout_package_status"] == "ok"
    assert result["freeze_audit_status"] == "ok"
    assert result["dossier_status"] == "blocker"
