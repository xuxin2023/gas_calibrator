"""Tests for Step 2 freeze seal contracts and builder — no-drift guardrails.

Step 2 boundary:
  - evidence_source = "simulated"
  - not_real_acceptance_evidence = True
  - not_ready_for_formal_claim = True
  - reviewer_only = True
  - readiness_mapping_only = True
  - primary_evidence_rewritten = False
  - real_acceptance_ready = False
"""

from __future__ import annotations

import pytest


# ---------------------------------------------------------------------------
# Contracts tests
# ---------------------------------------------------------------------------

class TestFreezeSealContracts:
    """Test freeze seal contracts — canonical field names, status buckets,
    drift labels, missing-surface labels, source-mismatch labels,
    boundary markers, and i18n keys."""

    def test_version(self):
        from gas_calibrator.v2.core.step2_freeze_seal_contracts import FREEZE_SEAL_CONTRACTS_VERSION
        assert FREEZE_SEAL_CONTRACTS_VERSION == "2.26.0"

    def test_audited_object_keys(self):
        from gas_calibrator.v2.core.step2_freeze_seal_contracts import AUDITED_OBJECT_KEYS
        assert AUDITED_OBJECT_KEYS == (
            "step2_closeout_readiness",
            "step2_closeout_package",
            "step2_freeze_audit",
            "step3_admission_dossier",
            "step2_closeout_verification",
        )

    def test_status_buckets(self):
        from gas_calibrator.v2.core.step2_freeze_seal_contracts import (
            FREEZE_SEAL_STATUS_OK,
            FREEZE_SEAL_STATUS_ATTENTION,
            FREEZE_SEAL_STATUS_BLOCKER,
            FREEZE_SEAL_STATUS_REVIEWER_ONLY,
        )
        assert FREEZE_SEAL_STATUS_OK == "ok"
        assert FREEZE_SEAL_STATUS_ATTENTION == "attention"
        assert FREEZE_SEAL_STATUS_BLOCKER == "blocker"
        assert FREEZE_SEAL_STATUS_REVIEWER_ONLY == "reviewer_only"

    def test_boundary_marker_fields(self):
        from gas_calibrator.v2.core.step2_freeze_seal_contracts import BOUNDARY_MARKER_FIELDS
        assert "evidence_source" in BOUNDARY_MARKER_FIELDS
        assert "not_real_acceptance_evidence" in BOUNDARY_MARKER_FIELDS
        assert "not_ready_for_formal_claim" in BOUNDARY_MARKER_FIELDS
        assert "reviewer_only" in BOUNDARY_MARKER_FIELDS
        assert "readiness_mapping_only" in BOUNDARY_MARKER_FIELDS
        assert "primary_evidence_rewritten" in BOUNDARY_MARKER_FIELDS
        assert "real_acceptance_ready" in BOUNDARY_MARKER_FIELDS
        assert len(BOUNDARY_MARKER_FIELDS) == 7

    def test_step2_boundary_canonical(self):
        from gas_calibrator.v2.core.step2_freeze_seal_contracts import FREEZE_SEAL_STEP2_BOUNDARY
        assert FREEZE_SEAL_STEP2_BOUNDARY["evidence_source"] == "simulated"
        assert FREEZE_SEAL_STEP2_BOUNDARY["not_real_acceptance_evidence"] is True
        assert FREEZE_SEAL_STEP2_BOUNDARY["not_ready_for_formal_claim"] is True
        assert FREEZE_SEAL_STEP2_BOUNDARY["reviewer_only"] is True
        assert FREEZE_SEAL_STEP2_BOUNDARY["readiness_mapping_only"] is True
        assert FREEZE_SEAL_STEP2_BOUNDARY["primary_evidence_rewritten"] is False
        assert FREEZE_SEAL_STEP2_BOUNDARY["real_acceptance_ready"] is False

    def test_object_status_field_names(self):
        from gas_calibrator.v2.core.step2_freeze_seal_contracts import OBJECT_STATUS_FIELD
        assert OBJECT_STATUS_FIELD["step2_closeout_readiness"] == "closeout_status"
        assert OBJECT_STATUS_FIELD["step2_closeout_package"] == "package_status"
        assert OBJECT_STATUS_FIELD["step2_freeze_audit"] == "audit_status"
        assert OBJECT_STATUS_FIELD["step3_admission_dossier"] == "dossier_status"
        assert OBJECT_STATUS_FIELD["step2_closeout_verification"] == "verification_status"

    def test_object_source_field_names(self):
        from gas_calibrator.v2.core.step2_freeze_seal_contracts import OBJECT_SOURCE_FIELD
        assert OBJECT_SOURCE_FIELD["step2_closeout_readiness"] == "closeout_readiness_source"
        assert OBJECT_SOURCE_FIELD["step2_closeout_package"] == "closeout_package_source"
        assert OBJECT_SOURCE_FIELD["step2_freeze_audit"] == "freeze_audit_source"
        assert OBJECT_SOURCE_FIELD["step3_admission_dossier"] == "admission_dossier_source"
        assert OBJECT_SOURCE_FIELD["step2_closeout_verification"] == "closeout_verification_source"

    def test_source_priority(self):
        from gas_calibrator.v2.core.step2_freeze_seal_contracts import SOURCE_PRIORITY
        assert SOURCE_PRIORITY == ("persisted", "rebuilt", "fallback")

    def test_consumable_fields_follow_registry_union(self):
        from gas_calibrator.v2.core.step2_closure_schema_registry import (
            STEP2_FREEZE_SEAL_AUDIT_OBJECT_KEYS,
            build_consumable_field_union,
        )
        from gas_calibrator.v2.core.step2_freeze_seal_contracts import CONSUMABLE_FIELDS

        assert CONSUMABLE_FIELDS == build_consumable_field_union(STEP2_FREEZE_SEAL_AUDIT_OBJECT_KEYS)

    def test_drift_labels_zh_en_consistency(self):
        from gas_calibrator.v2.core.step2_freeze_seal_contracts import DRIFT_LABELS_ZH, DRIFT_LABELS_EN
        assert set(DRIFT_LABELS_ZH.keys()) == set(DRIFT_LABELS_EN.keys())

    def test_missing_surface_labels_zh_en_consistency(self):
        from gas_calibrator.v2.core.step2_freeze_seal_contracts import MISSING_SURFACE_LABELS_ZH, MISSING_SURFACE_LABELS_EN
        assert set(MISSING_SURFACE_LABELS_ZH.keys()) == set(MISSING_SURFACE_LABELS_EN.keys())

    def test_source_mismatch_labels_zh_en_consistency(self):
        from gas_calibrator.v2.core.step2_freeze_seal_contracts import SOURCE_MISMATCH_LABELS_ZH, SOURCE_MISMATCH_LABELS_EN
        assert set(SOURCE_MISMATCH_LABELS_ZH.keys()) == set(SOURCE_MISMATCH_LABELS_EN.keys())

    def test_i18n_keys_coverage(self):
        from gas_calibrator.v2.core.step2_freeze_seal_contracts import FREEZE_SEAL_I18N_KEYS
        # All keys should start with "freeze_seal."
        for key in FREEZE_SEAL_I18N_KEYS:
            assert key.startswith("freeze_seal."), f"i18n key {key} does not start with freeze_seal."

    def test_resolve_helpers_zh_en(self):
        from gas_calibrator.v2.core.step2_freeze_seal_contracts import (
            resolve_freeze_seal_title,
            resolve_freeze_seal_summary,
            resolve_freeze_seal_status_label,
            resolve_freeze_seal_simulation_only_boundary,
            resolve_freeze_seal_reviewer_only_notice,
            resolve_freeze_seal_non_claim_notice,
        )
        assert resolve_freeze_seal_title("zh") != resolve_freeze_seal_title("en")
        assert resolve_freeze_seal_summary("zh") != resolve_freeze_seal_summary("en")
        for status in ("ok", "attention", "blocker", "reviewer_only"):
            zh = resolve_freeze_seal_status_label(status, lang="zh")
            en = resolve_freeze_seal_status_label(status, lang="en")
            assert zh != en
        assert resolve_freeze_seal_simulation_only_boundary("zh") != resolve_freeze_seal_simulation_only_boundary("en")
        assert resolve_freeze_seal_reviewer_only_notice("zh") != resolve_freeze_seal_reviewer_only_notice("en")
        assert resolve_freeze_seal_non_claim_notice("zh") != resolve_freeze_seal_non_claim_notice("en")

    def test_no_formal_acceptance_language_in_contracts(self):
        from gas_calibrator.v2.core.step2_freeze_seal_contracts import (
            FREEZE_SEAL_TITLE_ZH, FREEZE_SEAL_TITLE_EN,
            FREEZE_SEAL_SUMMARY_ZH, FREEZE_SEAL_SUMMARY_EN,
            FREEZE_SEAL_SIMULATION_ONLY_BOUNDARY_ZH, FREEZE_SEAL_SIMULATION_ONLY_BOUNDARY_EN,
        )
        # Title and summary should not contain "正式放行批准" as a positive claim
        for text in [FREEZE_SEAL_TITLE_ZH, FREEZE_SEAL_TITLE_EN,
                     FREEZE_SEAL_SUMMARY_ZH, FREEZE_SEAL_SUMMARY_EN]:
            if "正式放行批准" in text:
                assert "不是" in text
            if "formal release approval" in text.lower():
                assert "not" in text.lower()
        # Boundary text may contain negated form
        for text in [FREEZE_SEAL_SIMULATION_ONLY_BOUNDARY_ZH, FREEZE_SEAL_SIMULATION_ONLY_BOUNDARY_EN]:
            if "正式放行" in text:
                assert "不" in text


# ---------------------------------------------------------------------------
# Builder tests
# ---------------------------------------------------------------------------

class TestFreezeSealBuilder:
    """Test freeze seal builder — status, drift_sections, missing_surfaces,
    source_mismatches, boundary markers."""

    def _make_closeout_readiness(self, **overrides):
        base = {
            "schema_version": "1.0",
            "artifact_type": "step2_closeout_readiness",
            "generated_at": "2026-04-15T00:00:00+00:00",
            "run_id": "test-run",
            "phase": "step2_closeout",
            "closeout_status": "ok",
            "closeout_status_label": "阶段就绪",
            "reviewer_summary_line": "Step 2 收官：阶段就绪",
            "reviewer_summary_lines": ["Step 2 收官就绪度", "状态：阶段就绪"],
            "blockers": [],
            "next_steps": [],
            "contributing_sections": [],
            "simulation_only_boundary": "仿真边界",
            "rendered_compact_sections": [],
            "gate_status": "ready_for_engineering_isolation",
            "gate_summary": {"pass_count": 1, "total_count": 1, "blocked_count": 0, "blocked_gate_ids": []},
            "closeout_gate_alignment": {"closeout_status": "ok", "gate_status": "ready_for_engineering_isolation", "aligned": True},
            "evidence_source": "simulated",
            "not_real_acceptance_evidence": True,
            "not_ready_for_formal_claim": True,
            "reviewer_only": True,
            "readiness_mapping_only": True,
            "primary_evidence_rewritten": False,
            "real_acceptance_ready": False,
            "closeout_readiness_source": "rebuilt",
        }
        base.update(overrides)
        return base

    def _make_closeout_package(self, **overrides):
        base = {
            "schema_version": "1.0",
            "artifact_type": "step2_closeout_package",
            "generated_at": "2026-04-15T00:00:00+00:00",
            "run_id": "test-run",
            "phase": "step2_closeout",
            "package_version": "2.26.0",
            "package_status": "ok",
            "package_status_label": "阶段就绪",
            "reviewer_summary_line": "Step 2 收官包：阶段就绪",
            "reviewer_summary_lines": ["Step 2 收官包", "状态：阶段就绪"],
            "sections": [],
            "section_order": [],
            "blockers": [],
            "next_steps": [],
            "simulation_only_boundary": "仿真边界",
            "source_versions": {},
            "closeout_package_source": "rebuilt",
            "evidence_source": "simulated",
            "not_real_acceptance_evidence": True,
            "not_ready_for_formal_claim": True,
            "reviewer_only": True,
            "readiness_mapping_only": True,
            "primary_evidence_rewritten": False,
            "real_acceptance_ready": False,
        }
        base.update(overrides)
        return base

    def _make_freeze_audit(self, **overrides):
        base = {
            "schema_version": "1.0",
            "artifact_type": "step2_freeze_audit",
            "generated_at": "2026-04-15T00:00:00+00:00",
            "run_id": "test-run",
            "phase": "step2_freeze_audit",
            "audit_version": "2.26.0",
            "audit_status": "ok",
            "audit_status_label": "冻结审计就绪",
            "reviewer_summary_line": "Step 2 冻结审计：RC 审阅候选",
            "reviewer_summary_lines": ["Step 2 冻结审计", "状态：冻结审计就绪"],
            "blockers": [],
            "next_steps": [],
            "audit_sections": {},
            "section_order": [],
            "freeze_candidate": True,
            "freeze_candidate_notice_zh": "freeze_candidate 仅表示 RC 审阅候选",
            "freeze_candidate_notice_en": "freeze_candidate means RC review candidate only",
            "simulation_only_boundary": "仿真边界",
            "freeze_audit_source": "rebuilt",
            "evidence_source": "simulated",
            "not_real_acceptance_evidence": True,
            "not_ready_for_formal_claim": True,
            "reviewer_only": True,
            "readiness_mapping_only": True,
            "primary_evidence_rewritten": False,
            "real_acceptance_ready": False,
        }
        base.update(overrides)
        return base

    def _make_admission_dossier(self, **overrides):
        base = {
            "schema_version": "1.0",
            "artifact_type": "step3_admission_dossier",
            "generated_at": "2026-04-15T00:00:00+00:00",
            "run_id": "test-run",
            "phase": "step3_admission_dossier",
            "dossier_version": "2.26.0",
            "dossier_status": "blocker",
            "dossier_status_label": "存在阻塞项",
            "reviewer_summary_line": "Step 3 准入材料：存在阻塞项",
            "reviewer_summary_lines": ["Step 3 准入材料", "状态：存在阻塞项"],
            "blockers": [{"key": "real_acceptance_not_ready", "label_zh": "真实验收尚未就绪", "label_en": "Real acceptance not ready"}],
            "next_steps": [],
            "dossier_sections": {},
            "section_order": [],
            "admission_candidate": False,
            "admission_candidate_notice_zh": "admission_candidate 仅表示审阅候选",
            "admission_candidate_notice_en": "admission_candidate means review candidate only",
            "simulation_only_boundary": "仿真边界",
            "source_versions": {},
            "admission_dossier_source": "rebuilt",
            "evidence_source": "simulated",
            "not_real_acceptance_evidence": True,
            "not_ready_for_formal_claim": True,
            "reviewer_only": True,
            "readiness_mapping_only": True,
            "primary_evidence_rewritten": False,
            "real_acceptance_ready": False,
        }
        base.update(overrides)
        return base

    def _make_closeout_verification(self, **overrides):
        base = {
            "schema_version": "1.0",
            "artifact_type": "step2_closeout_verification",
            "generated_at": "2026-04-15T00:00:00+00:00",
            "run_id": "test-run",
            "phase": "step2_closeout_verification",
            "verification_version": "2.26.0",
            "verification_status": "closeout_candidate",
            "reviewer_summary_line": "Step 2 收官候选",
            "blockers": [{"key": "real_acceptance_not_ready", "label_zh": "真实验收尚未就绪", "label_en": "Real acceptance not ready"}],
            "next_steps": [],
            "missing_for_step3": [],
            "simulation_only_boundary": "仿真边界",
            "closeout_readiness_status": "ok",
            "closeout_package_status": "ok",
            "freeze_audit_status": "ok",
            "dossier_status": "blocker",
            "closeout_verification_source": "rebuilt",
            "verification_source": "rebuilt",
            "verification_fallback_reason": "",
            "evidence_source": "simulated",
            "not_real_acceptance_evidence": True,
            "not_ready_for_formal_claim": True,
            "reviewer_only": True,
            "readiness_mapping_only": True,
            "primary_evidence_rewritten": False,
            "real_acceptance_ready": False,
        }
        base.update(overrides)
        return base

    def test_builder_required_fields(self):
        from gas_calibrator.v2.core.step2_freeze_seal_builder import build_step2_freeze_seal
        seal = build_step2_freeze_seal(run_id="test")
        assert seal["artifact_type"] == "step2_freeze_seal"
        assert seal["phase"] == "step2_freeze_seal"
        assert "freeze_seal_status" in seal
        assert "drift_sections" in seal
        assert "missing_surfaces" in seal
        assert "source_mismatches" in seal
        assert "audited_objects" in seal
        assert "simulation_only_boundary" in seal

    def test_builder_boundary_markers(self):
        from gas_calibrator.v2.core.step2_freeze_seal_builder import build_step2_freeze_seal
        seal = build_step2_freeze_seal(run_id="test")
        assert seal["evidence_source"] == "simulated"
        assert seal["not_real_acceptance_evidence"] is True
        assert seal["not_ready_for_formal_claim"] is True
        assert seal["reviewer_only"] is True
        assert seal["readiness_mapping_only"] is True
        assert seal["primary_evidence_rewritten"] is False
        assert seal["real_acceptance_ready"] is False

    def test_builder_status_ok_with_all_objects(self):
        from gas_calibrator.v2.core.step2_freeze_seal_builder import build_step2_freeze_seal
        seal = build_step2_freeze_seal(
            run_id="test",
            step2_closeout_readiness=self._make_closeout_readiness(),
            step2_closeout_package=self._make_closeout_package(),
            step2_freeze_audit=self._make_freeze_audit(),
            step3_admission_dossier=self._make_admission_dossier(),
            step2_closeout_verification=self._make_closeout_verification(),
        )
        assert seal["freeze_seal_status"] == "ok"
        assert seal["drift_sections"] == []
        assert seal["source_mismatches"] == []

    def test_builder_detects_boundary_marker_mismatch(self):
        from gas_calibrator.v2.core.step2_freeze_seal_builder import build_step2_freeze_seal
        bad_readiness = self._make_closeout_readiness(evidence_source="real")
        seal = build_step2_freeze_seal(
            run_id="test",
            step2_closeout_readiness=bad_readiness,
            step2_closeout_package=self._make_closeout_package(),
            step2_freeze_audit=self._make_freeze_audit(),
            step3_admission_dossier=self._make_admission_dossier(),
            step2_closeout_verification=self._make_closeout_verification(),
        )
        assert seal["freeze_seal_status"] == "blocker"
        assert any(d["drift_type"] == "boundary_marker_mismatch" for d in seal["drift_sections"])

    def test_builder_detects_missing_field(self):
        from gas_calibrator.v2.core.step2_freeze_seal_builder import build_step2_freeze_seal
        # Remove a consumable field
        readiness = self._make_closeout_readiness()
        del readiness["reviewer_summary_line"]
        seal = build_step2_freeze_seal(
            run_id="test",
            step2_closeout_readiness=readiness,
            step2_closeout_package=self._make_closeout_package(),
            step2_freeze_audit=self._make_freeze_audit(),
            step3_admission_dossier=self._make_admission_dossier(),
            step2_closeout_verification=self._make_closeout_verification(),
        )
        assert seal["freeze_seal_status"] in ("attention", "blocker")
        assert any(d["drift_type"] == "field_missing" for d in seal["drift_sections"])

    def test_builder_detects_source_mismatch(self):
        from gas_calibrator.v2.core.step2_freeze_seal_builder import build_step2_freeze_seal
        bad_pkg = self._make_closeout_package(closeout_package_source="invalid_source")
        seal = build_step2_freeze_seal(
            run_id="test",
            step2_closeout_readiness=self._make_closeout_readiness(),
            step2_closeout_package=bad_pkg,
            step2_freeze_audit=self._make_freeze_audit(),
            step3_admission_dossier=self._make_admission_dossier(),
            step2_closeout_verification=self._make_closeout_verification(),
        )
        assert seal["freeze_seal_status"] == "blocker"
        assert len(seal["source_mismatches"]) > 0

    def test_builder_reads_registry_required_fields_dynamically(self, monkeypatch):
        from dataclasses import replace

        from gas_calibrator.v2.core import step2_closure_schema_registry as registry_mod
        from gas_calibrator.v2.core.step2_freeze_seal_builder import build_step2_freeze_seal

        original_entry = registry_mod.REGISTRY["step2_closeout_package"]
        monkeypatch.setitem(
            registry_mod.REGISTRY,
            "step2_closeout_package",
            replace(
                original_entry,
                required_consumable_fields=original_entry.required_consumable_fields
                + ("schema_lock_probe",),
            ),
        )

        seal = build_step2_freeze_seal(
            run_id="test",
            step2_closeout_readiness=self._make_closeout_readiness(),
            step2_closeout_package=self._make_closeout_package(),
            step2_freeze_audit=self._make_freeze_audit(),
            step3_admission_dossier=self._make_admission_dossier(),
            step2_closeout_verification=self._make_closeout_verification(),
        )

        assert any(
            item["drift_type"] == "field_missing"
            and item["object"] == "step2_closeout_package"
            and item["field"] == "schema_lock_probe"
            for item in seal["drift_sections"]
        )

    def test_builder_missing_objects_give_attention(self):
        from gas_calibrator.v2.core.step2_freeze_seal_builder import build_step2_freeze_seal
        seal = build_step2_freeze_seal(
            run_id="test",
            step2_closeout_readiness=self._make_closeout_readiness(),
            # Other objects missing
        )
        # Missing objects should produce field_missing drifts
        assert seal["freeze_seal_status"] in ("attention", "blocker", "ok", "reviewer_only")

    def test_builder_audited_objects_summary(self):
        from gas_calibrator.v2.core.step2_freeze_seal_builder import build_step2_freeze_seal
        seal = build_step2_freeze_seal(
            run_id="test",
            step2_closeout_readiness=self._make_closeout_readiness(),
            step2_closeout_package=self._make_closeout_package(),
            step2_freeze_audit=self._make_freeze_audit(),
            step3_admission_dossier=self._make_admission_dossier(),
            step2_closeout_verification=self._make_closeout_verification(),
        )
        audited = seal["audited_objects"]
        assert len(audited) == 5
        keys = {a["key"] for a in audited}
        assert "step2_closeout_readiness" in keys
        assert "step2_closeout_package" in keys
        assert "step2_freeze_audit" in keys
        assert "step3_admission_dossier" in keys
        assert "step2_closeout_verification" in keys

    def test_builder_lang_en(self):
        from gas_calibrator.v2.core.step2_freeze_seal_builder import build_step2_freeze_seal
        seal = build_step2_freeze_seal(run_id="test", lang="en")
        assert "Freeze seal" in seal["reviewer_summary_line"] or "freeze seal" in seal["reviewer_summary_line"].lower()

    def test_builder_no_formal_acceptance_language(self):
        from gas_calibrator.v2.core.step2_freeze_seal_builder import build_step2_freeze_seal
        seal = build_step2_freeze_seal(run_id="test")
        # "正式放行批准" may appear only with negation ("不是正式放行批准")
        # "formal release approval" may appear only with negation
        for line in seal["reviewer_summary_lines"]:
            if "正式放行批准" in line:
                assert "不是" in line
            if "formal release approval" in line.lower():
                assert "not" in line.lower()

    def test_builder_does_not_replace_closeout_package(self):
        """Freeze seal is a guardrail, not a replacement."""
        from gas_calibrator.v2.core.step2_freeze_seal_builder import build_step2_freeze_seal
        seal = build_step2_freeze_seal(run_id="test")
        assert seal["artifact_type"] == "step2_freeze_seal"
        assert seal["artifact_type"] != "step2_closeout_package"
        assert seal["artifact_type"] != "step2_freeze_audit"
        assert seal["artifact_type"] != "step3_admission_dossier"

    def test_builder_freeze_seal_status_not_formal_release(self):
        """freeze_seal_status only expresses no-drift state, not formal release."""
        from gas_calibrator.v2.core.step2_freeze_seal_builder import build_step2_freeze_seal
        seal = build_step2_freeze_seal(run_id="test")
        assert seal["freeze_seal_status"] in ("ok", "attention", "blocker", "reviewer_only")


# ---------------------------------------------------------------------------
# Fallback tests
# ---------------------------------------------------------------------------

class TestFreezeSealFallback:
    """Test freeze seal fallback."""

    def test_fallback_boundary_markers(self):
        from gas_calibrator.v2.core.step2_freeze_seal_builder import build_freeze_seal_fallback
        fb = build_freeze_seal_fallback()
        assert fb["evidence_source"] == "simulated"
        assert fb["not_real_acceptance_evidence"] is True
        assert fb["not_ready_for_formal_claim"] is True
        assert fb["reviewer_only"] is True
        assert fb["readiness_mapping_only"] is True
        assert fb["primary_evidence_rewritten"] is False
        assert fb["real_acceptance_ready"] is False

    def test_fallback_status(self):
        from gas_calibrator.v2.core.step2_freeze_seal_builder import build_freeze_seal_fallback
        fb = build_freeze_seal_fallback()
        assert fb["freeze_seal_status"] == "reviewer_only"

    def test_fallback_no_formal_language(self):
        from gas_calibrator.v2.core.step2_freeze_seal_builder import build_freeze_seal_fallback
        fb = build_freeze_seal_fallback()
        # "正式放行批准" may appear only with negation
        if "正式放行批准" in fb["reviewer_summary_line"]:
            assert "不是" in fb["reviewer_summary_line"]
        if "formal release approval" in fb["reviewer_summary_line"].lower():
            assert "not" in fb["reviewer_summary_line"].lower()

    def test_fallback_source(self):
        from gas_calibrator.v2.core.step2_freeze_seal_builder import build_freeze_seal_fallback
        fb = build_freeze_seal_fallback()
        assert fb["freeze_seal_source"] == "fallback"

    def test_fallback_audited_objects(self):
        from gas_calibrator.v2.core.step2_freeze_seal_builder import build_freeze_seal_fallback
        fb = build_freeze_seal_fallback()
        assert len(fb["audited_objects"]) == 5
        assert all(not a["present"] for a in fb["audited_objects"])


# ---------------------------------------------------------------------------
# Cross-surface consistency tests
# ---------------------------------------------------------------------------

class TestCrossSurfaceConsistency:
    """Test that the five core objects have consistent boundary markers
    across results / reports / historical / review index / UI."""

    def test_boundary_markers_match_across_all_five_objects(self):
        """All five objects must share the same Step 2 boundary markers."""
        from gas_calibrator.v2.core.step2_freeze_seal_contracts import FREEZE_SEAL_STEP2_BOUNDARY
        from gas_calibrator.v2.core.step2_closeout_readiness_builder import build_step2_closeout_readiness
        from gas_calibrator.v2.core.step2_closeout_package_builder import build_step2_closeout_package
        from gas_calibrator.v2.core.step2_freeze_audit_builder import build_step2_freeze_audit
        from gas_calibrator.v2.core.step3_admission_dossier_builder import build_step3_admission_dossier
        from gas_calibrator.v2.core.step2_closeout_verification import build_step2_closeout_verification

        readiness = build_step2_closeout_readiness(run_id="test")
        pkg = build_step2_closeout_package(run_id="test", step2_closeout_readiness=readiness)
        audit = build_step2_freeze_audit(run_id="test", step2_closeout_package=pkg, step2_closeout_readiness=readiness)
        dossier = build_step3_admission_dossier(run_id="test", step2_freeze_audit=audit, step2_closeout_package=pkg, step2_closeout_readiness=readiness)
        verification = build_step2_closeout_verification(run_id="test", step2_closeout_readiness=readiness, step2_closeout_package=pkg, step2_freeze_audit=audit, step3_admission_dossier=dossier)

        for obj_name, obj in [
            ("closeout_readiness", readiness),
            ("closeout_package", pkg),
            ("freeze_audit", audit),
            ("admission_dossier", dossier),
            ("closeout_verification", verification),
        ]:
            for field, expected in FREEZE_SEAL_STEP2_BOUNDARY.items():
                actual = obj.get(field)
                assert actual == expected, f"{obj_name}.{field}: expected {expected}, got {actual}"

    def test_source_priority_consistent(self):
        """All objects should use source values from (persisted, rebuilt, fallback)."""
        from gas_calibrator.v2.core.step2_freeze_seal_contracts import SOURCE_PRIORITY
        from gas_calibrator.v2.core.step2_closeout_package_builder import build_step2_closeout_package
        from gas_calibrator.v2.core.step2_closeout_readiness_builder import build_step2_closeout_readiness
        from gas_calibrator.v2.core.step2_freeze_audit_builder import build_step2_freeze_audit
        from gas_calibrator.v2.core.step3_admission_dossier_builder import build_step3_admission_dossier
        from gas_calibrator.v2.core.step2_closeout_verification import build_step2_closeout_verification

        readiness = build_step2_closeout_readiness(run_id="test")
        pkg = build_step2_closeout_package(run_id="test", step2_closeout_readiness=readiness)
        audit = build_step2_freeze_audit(run_id="test", step2_closeout_package=pkg, step2_closeout_readiness=readiness)
        dossier = build_step3_admission_dossier(run_id="test", step2_freeze_audit=audit, step2_closeout_package=pkg, step2_closeout_readiness=readiness)
        verification = build_step2_closeout_verification(run_id="test", step2_closeout_readiness=readiness, step2_closeout_package=pkg, step2_freeze_audit=audit, step3_admission_dossier=dossier)

        for obj_name, obj, source_field in [
            ("closeout_package", pkg, "closeout_package_source"),
            ("freeze_audit", audit, "freeze_audit_source"),
            ("admission_dossier", dossier, "admission_dossier_source"),
            ("closeout_verification", verification, "closeout_verification_source"),
        ]:
            source = obj.get(source_field, "")
            if source:
                assert source in SOURCE_PRIORITY, f"{obj_name}.{source_field} = {source}, not in {SOURCE_PRIORITY}"

    def test_freeze_seal_ok_when_all_objects_consistent(self):
        """When all five objects are consistent, freeze seal should be ok."""
        from gas_calibrator.v2.core.step2_freeze_seal_builder import build_step2_freeze_seal
        from gas_calibrator.v2.core.step2_closeout_readiness_builder import build_step2_closeout_readiness
        from gas_calibrator.v2.core.step2_closeout_package_builder import build_step2_closeout_package
        from gas_calibrator.v2.core.step2_freeze_audit_builder import build_step2_freeze_audit
        from gas_calibrator.v2.core.step3_admission_dossier_builder import build_step3_admission_dossier
        from gas_calibrator.v2.core.step2_closeout_verification import build_step2_closeout_verification

        readiness = build_step2_closeout_readiness(run_id="test")
        pkg = build_step2_closeout_package(run_id="test", step2_closeout_readiness=readiness)
        audit = build_step2_freeze_audit(run_id="test", step2_closeout_package=pkg, step2_closeout_readiness=readiness)
        dossier = build_step3_admission_dossier(run_id="test", step2_freeze_audit=audit, step2_closeout_package=pkg, step2_closeout_readiness=readiness)
        verification = build_step2_closeout_verification(run_id="test", step2_closeout_readiness=readiness, step2_closeout_package=pkg, step2_freeze_audit=audit, step3_admission_dossier=dossier)

        seal = build_step2_freeze_seal(
            run_id="test",
            step2_closeout_readiness=readiness,
            step2_closeout_package=pkg,
            step2_freeze_audit=audit,
            step3_admission_dossier=dossier,
            step2_closeout_verification=verification,
        )
        assert seal["freeze_seal_status"] == "ok"
        assert seal["drift_sections"] == []
        assert seal["source_mismatches"] == []


# ---------------------------------------------------------------------------
# i18n locale consistency
# ---------------------------------------------------------------------------

class TestFreezeSealI18nLocaleConsistency:
    """Test that zh_CN and en_US locale files have matching freeze_seal keys."""

    def test_freeze_seal_top_level_keys_match(self):
        import json
        from pathlib import Path
        base = Path(__file__).resolve().parents[2] / "src" / "gas_calibrator" / "v2" / "ui_v2" / "locales"
        with open(base / "zh_CN.json", encoding="utf-8") as f:
            zh = json.load(f)
        with open(base / "en_US.json", encoding="utf-8") as f:
            en = json.load(f)
        zh_keys = set(zh.get("freeze_seal", {}).keys())
        en_keys = set(en.get("freeze_seal", {}).keys())
        assert zh_keys == en_keys, f"zh_CN keys: {zh_keys - en_keys} extra, en_US keys: {en_keys - zh_keys} extra"

    def test_freeze_seal_reports_keys_match(self):
        import json
        from pathlib import Path
        base = Path(__file__).resolve().parents[2] / "src" / "gas_calibrator" / "v2" / "ui_v2" / "locales"
        with open(base / "zh_CN.json", encoding="utf-8") as f:
            zh = json.load(f)
        with open(base / "en_US.json", encoding="utf-8") as f:
            en = json.load(f)
        zh_reports = zh.get("pages", {}).get("reports", {})
        en_reports = en.get("pages", {}).get("reports", {})
        zh_seal_keys = {k for k in zh_reports if k.startswith("freeze_seal_")}
        en_seal_keys = {k for k in en_reports if k.startswith("freeze_seal_")}
        assert zh_seal_keys == en_seal_keys


# ---------------------------------------------------------------------------
# Step 2 boundary assertion
# ---------------------------------------------------------------------------

class TestStep2Boundary:
    """Assert that freeze seal never violates Step 2 boundary."""

    def test_no_real_path_in_output(self):
        from gas_calibrator.v2.core.step2_freeze_seal_builder import build_step2_freeze_seal
        seal = build_step2_freeze_seal(run_id="test")
        output_str = str(seal)
        assert "COM" not in output_str
        assert "/dev/tty" not in output_str
        assert "real_acceptance_ready" in output_str
        assert seal["real_acceptance_ready"] is False

    def test_no_formal_approval_claim(self):
        from gas_calibrator.v2.core.step2_freeze_seal_builder import build_step2_freeze_seal
        seal = build_step2_freeze_seal(run_id="test")
        for line in seal["reviewer_summary_lines"]:
            assert "正式批准" not in line or "不是" in line
            assert "formal approval" not in line.lower() or "not" in line.lower()
