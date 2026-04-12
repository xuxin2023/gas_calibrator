"""Tests for Step 2.6 governance handoff pack convergence.

Validates:
- governance_handoff_contracts completeness and ordering
- label / role / filename / i18n key consistency
- stage_admission_review_pack / engineering_isolation_admission_checklist surface parity
- Chinese default display, English fallback
- Step 2 boundary markers
- No real paths / real device / real acceptance semantics
"""

from __future__ import annotations

from pathlib import Path

import pytest


# ---------------------------------------------------------------------------
# 1. Contract completeness and ordering
# ---------------------------------------------------------------------------


class TestGovernanceHandoffContractsCompleteness:
    """governance_handoff_contracts must define all 8 artifact keys."""

    def test_eight_artifact_keys(self) -> None:
        from gas_calibrator.v2.core.governance_handoff_contracts import (
            GOVERNANCE_HANDOFF_ARTIFACT_KEYS,
        )
        assert len(GOVERNANCE_HANDOFF_ARTIFACT_KEYS) == 8

    def test_canonical_order(self) -> None:
        from gas_calibrator.v2.core.governance_handoff_contracts import (
            GOVERNANCE_HANDOFF_ARTIFACT_KEYS,
        )
        assert GOVERNANCE_HANDOFF_ARTIFACT_KEYS == (
            "step2_readiness_summary",
            "metrology_calibration_contract",
            "phase_transition_bridge",
            "phase_transition_bridge_reviewer_artifact",
            "stage_admission_review_pack",
            "stage_admission_review_pack_reviewer_artifact",
            "engineering_isolation_admission_checklist",
            "engineering_isolation_admission_checklist_reviewer_artifact",
        )

    def test_no_duplicate_keys(self) -> None:
        from gas_calibrator.v2.core.governance_handoff_contracts import (
            GOVERNANCE_HANDOFF_ARTIFACT_KEYS,
        )
        assert len(set(GOVERNANCE_HANDOFF_ARTIFACT_KEYS)) == len(GOVERNANCE_HANDOFF_ARTIFACT_KEYS)


class TestGovernanceHandoffContractsDictionaries:
    """All contract dictionaries must cover every artifact key."""

    @pytest.fixture()
    def contracts(self) -> dict:
        from gas_calibrator.v2.core import governance_handoff_contracts as gc
        return {
            "keys": gc.GOVERNANCE_HANDOFF_ARTIFACT_KEYS,
            "filenames": gc.GOVERNANCE_HANDOFF_FILENAMES,
            "roles": gc.GOVERNANCE_HANDOFF_ROLES,
            "labels": gc.GOVERNANCE_HANDOFF_DISPLAY_LABELS,
            "labels_en": gc.GOVERNANCE_HANDOFF_DISPLAY_LABELS_EN,
            "i18n_keys": gc.GOVERNANCE_HANDOFF_I18N_KEYS,
            "visibility": gc.GOVERNANCE_HANDOFF_SURFACE_VISIBILITY,
            "phases": gc.GOVERNANCE_HANDOFF_PHASES,
            "boundary": gc.GOVERNANCE_HANDOFF_STEP2_BOUNDARY,
        }

    def test_filenames_cover_all_keys(self, contracts: dict) -> None:
        keys = contracts["keys"]
        filenames = contracts["filenames"]
        for key in keys:
            assert key in filenames, f"Missing filename for {key}"
            assert filenames[key].endswith(".json") or filenames[key].endswith(".md")

    def test_roles_cover_all_keys(self, contracts: dict) -> None:
        valid_roles = {"execution_summary", "formal_analysis"}
        for key in contracts["keys"]:
            role = contracts["roles"][key]
            assert role in valid_roles, f"Invalid role '{role}' for {key}"

    def test_labels_cover_all_keys(self, contracts: dict) -> None:
        for key in contracts["keys"]:
            assert key in contracts["labels"], f"Missing Chinese label for {key}"
            assert key in contracts["labels_en"], f"Missing English label for {key}"

    def test_i18n_keys_cover_all_keys(self, contracts: dict) -> None:
        for key in contracts["keys"]:
            i18n_key = contracts["i18n_keys"][key]
            assert i18n_key.startswith("governance_handoff."), f"Bad i18n key prefix for {key}"

    def test_boundary_covers_all_keys(self, contracts: dict) -> None:
        for key in contracts["keys"]:
            boundary = contracts["boundary"][key]
            assert boundary["evidence_source"] == "simulated"
            assert boundary["not_real_acceptance_evidence"] is True
            assert boundary["not_ready_for_formal_claim"] is True


# ---------------------------------------------------------------------------
# 2. Label / role / filename / i18n key consistency
# ---------------------------------------------------------------------------


class TestGovernanceHandoffLabelConsistency:
    """Chinese labels must be default, English only fallback."""

    def test_chinese_labels_contain_cjk(self) -> None:
        from gas_calibrator.v2.core.governance_handoff_contracts import (
            GOVERNANCE_HANDOFF_DISPLAY_LABELS,
            GOVERNANCE_HANDOFF_ARTIFACT_KEYS,
        )
        for key in GOVERNANCE_HANDOFF_ARTIFACT_KEYS:
            label = GOVERNANCE_HANDOFF_DISPLAY_LABELS[key]
            assert any('\u4e00' <= c <= '\u9fff' for c in label), \
                f"Chinese label for {key} has no CJK characters: {label}"

    def test_english_labels_no_cjk(self) -> None:
        from gas_calibrator.v2.core.governance_handoff_contracts import (
            GOVERNANCE_HANDOFF_DISPLAY_LABELS_EN,
            GOVERNANCE_HANDOFF_ARTIFACT_KEYS,
        )
        for key in GOVERNANCE_HANDOFF_ARTIFACT_KEYS:
            label = GOVERNANCE_HANDOFF_DISPLAY_LABELS_EN[key]
            assert not any('\u4e00' <= c <= '\u9fff' for c in label), \
                f"English label for {key} has CJK characters: {label}"

    def test_resolve_label_zh_default(self) -> None:
        from gas_calibrator.v2.core.governance_handoff_contracts import (
            resolve_governance_handoff_display_label,
            GOVERNANCE_HANDOFF_DISPLAY_LABELS,
            GOVERNANCE_HANDOFF_ARTIFACT_KEYS,
        )
        for key in GOVERNANCE_HANDOFF_ARTIFACT_KEYS:
            assert resolve_governance_handoff_display_label(key) == GOVERNANCE_HANDOFF_DISPLAY_LABELS[key]

    def test_resolve_label_en_fallback(self) -> None:
        from gas_calibrator.v2.core.governance_handoff_contracts import (
            resolve_governance_handoff_display_label,
            GOVERNANCE_HANDOFF_DISPLAY_LABELS_EN,
            GOVERNANCE_HANDOFF_ARTIFACT_KEYS,
        )
        for key in GOVERNANCE_HANDOFF_ARTIFACT_KEYS:
            assert resolve_governance_handoff_display_label(key, lang="en") == GOVERNANCE_HANDOFF_DISPLAY_LABELS_EN[key]


# ---------------------------------------------------------------------------
# 3. stage_admission_review_pack / engineering_isolation_admission_checklist parity
# ---------------------------------------------------------------------------


class TestStageAdmissionAndChecklistParity:
    """stage_admission_review_pack and engineering_isolation_admission_checklist must have consistent surface."""

    def test_both_have_json_filename(self) -> None:
        from gas_calibrator.v2.core.stage_admission_review_pack import STAGE_ADMISSION_REVIEW_PACK_FILENAME
        from gas_calibrator.v2.core.engineering_isolation_admission_checklist import ENGINEERING_ISOLATION_ADMISSION_CHECKLIST_FILENAME
        assert STAGE_ADMISSION_REVIEW_PACK_FILENAME.endswith(".json")
        assert ENGINEERING_ISOLATION_ADMISSION_CHECKLIST_FILENAME.endswith(".json")

    def test_both_have_reviewer_filename(self) -> None:
        from gas_calibrator.v2.core.stage_admission_review_pack import STAGE_ADMISSION_REVIEW_PACK_REVIEWER_FILENAME
        from gas_calibrator.v2.core.engineering_isolation_admission_checklist import ENGINEERING_ISOLATION_ADMISSION_CHECKLIST_REVIEWER_FILENAME
        assert STAGE_ADMISSION_REVIEW_PACK_REVIEWER_FILENAME.endswith(".md")
        assert ENGINEERING_ISOLATION_ADMISSION_CHECKLIST_REVIEWER_FILENAME.endswith(".md")

    def test_both_have_same_role(self) -> None:
        from gas_calibrator.v2.core.governance_handoff_contracts import GOVERNANCE_HANDOFF_ROLES
        assert GOVERNANCE_HANDOFF_ROLES["stage_admission_review_pack"] == \
               GOVERNANCE_HANDOFF_ROLES["engineering_isolation_admission_checklist"]
        assert GOVERNANCE_HANDOFF_ROLES["stage_admission_review_pack_reviewer_artifact"] == \
               GOVERNANCE_HANDOFF_ROLES["engineering_isolation_admission_checklist_reviewer_artifact"]

    def test_both_have_same_phase(self) -> None:
        from gas_calibrator.v2.core.governance_handoff_contracts import GOVERNANCE_HANDOFF_PHASES
        assert GOVERNANCE_HANDOFF_PHASES["stage_admission_review_pack"] == \
               GOVERNANCE_HANDOFF_PHASES["engineering_isolation_admission_checklist"]

    def test_checklist_title_chinese_first(self) -> None:
        """Checklist title must be Chinese-first (matching stage_admission convention)."""
        from gas_calibrator.v2.core.engineering_isolation_admission_checklist_artifact_entry import (
            build_engineering_isolation_admission_checklist_artifact_entry,
        )
        entry = build_engineering_isolation_admission_checklist_artifact_entry(
            artifact_path="/tmp/test.json",
        )
        title = entry.get("title_text", "")
        assert "工程隔离准入清单" in title
        # Chinese must come before English
        zh_pos = title.index("工程隔离准入清单")
        en_pos = title.index("Engineering")
        assert zh_pos < en_pos, f"Chinese should come before English in title: {title}"


# ---------------------------------------------------------------------------
# 4. Filenames match module constants
# ---------------------------------------------------------------------------


class TestFilenamesMatchModuleConstants:
    """Shared contract filenames must match the actual module-level constants."""

    def test_step2_readiness_filename(self) -> None:
        from gas_calibrator.v2.core.step2_readiness import STEP2_READINESS_SUMMARY_FILENAME
        from gas_calibrator.v2.core.governance_handoff_contracts import GOVERNANCE_HANDOFF_FILENAMES
        assert STEP2_READINESS_SUMMARY_FILENAME == GOVERNANCE_HANDOFF_FILENAMES["step2_readiness_summary"]

    def test_metrology_contract_filename(self) -> None:
        from gas_calibrator.v2.core.metrology_calibration_contract import METROLOGY_CALIBRATION_CONTRACT_FILENAME
        from gas_calibrator.v2.core.governance_handoff_contracts import GOVERNANCE_HANDOFF_FILENAMES
        assert METROLOGY_CALIBRATION_CONTRACT_FILENAME == GOVERNANCE_HANDOFF_FILENAMES["metrology_calibration_contract"]

    def test_phase_bridge_filename(self) -> None:
        from gas_calibrator.v2.core.phase_transition_bridge import PHASE_TRANSITION_BRIDGE_FILENAME
        from gas_calibrator.v2.core.governance_handoff_contracts import GOVERNANCE_HANDOFF_FILENAMES
        assert PHASE_TRANSITION_BRIDGE_FILENAME == GOVERNANCE_HANDOFF_FILENAMES["phase_transition_bridge"]

    def test_phase_bridge_reviewer_filename(self) -> None:
        from gas_calibrator.v2.core.phase_transition_bridge_reviewer_artifact import PHASE_TRANSITION_BRIDGE_REVIEWER_FILENAME
        from gas_calibrator.v2.core.governance_handoff_contracts import GOVERNANCE_HANDOFF_FILENAMES
        assert PHASE_TRANSITION_BRIDGE_REVIEWER_FILENAME == GOVERNANCE_HANDOFF_FILENAMES["phase_transition_bridge_reviewer_artifact"]

    def test_stage_admission_filename(self) -> None:
        from gas_calibrator.v2.core.stage_admission_review_pack import STAGE_ADMISSION_REVIEW_PACK_FILENAME
        from gas_calibrator.v2.core.governance_handoff_contracts import GOVERNANCE_HANDOFF_FILENAMES
        assert STAGE_ADMISSION_REVIEW_PACK_FILENAME == GOVERNANCE_HANDOFF_FILENAMES["stage_admission_review_pack"]

    def test_stage_admission_reviewer_filename(self) -> None:
        from gas_calibrator.v2.core.stage_admission_review_pack import STAGE_ADMISSION_REVIEW_PACK_REVIEWER_FILENAME
        from gas_calibrator.v2.core.governance_handoff_contracts import GOVERNANCE_HANDOFF_FILENAMES
        assert STAGE_ADMISSION_REVIEW_PACK_REVIEWER_FILENAME == GOVERNANCE_HANDOFF_FILENAMES["stage_admission_review_pack_reviewer_artifact"]

    def test_checklist_filename(self) -> None:
        from gas_calibrator.v2.core.engineering_isolation_admission_checklist import ENGINEERING_ISOLATION_ADMISSION_CHECKLIST_FILENAME
        from gas_calibrator.v2.core.governance_handoff_contracts import GOVERNANCE_HANDOFF_FILENAMES
        assert ENGINEERING_ISOLATION_ADMISSION_CHECKLIST_FILENAME == GOVERNANCE_HANDOFF_FILENAMES["engineering_isolation_admission_checklist"]

    def test_checklist_reviewer_filename(self) -> None:
        from gas_calibrator.v2.core.engineering_isolation_admission_checklist import ENGINEERING_ISOLATION_ADMISSION_CHECKLIST_REVIEWER_FILENAME
        from gas_calibrator.v2.core.governance_handoff_contracts import GOVERNANCE_HANDOFF_FILENAMES
        assert ENGINEERING_ISOLATION_ADMISSION_CHECKLIST_REVIEWER_FILENAME == GOVERNANCE_HANDOFF_FILENAMES["engineering_isolation_admission_checklist_reviewer_artifact"]


# ---------------------------------------------------------------------------
# 5. i18n locale consistency
# ---------------------------------------------------------------------------


class TestI18nLocaleConsistency:
    """Locale files must have governance_handoff keys matching contracts."""

    def test_zh_cn_has_governance_handoff(self) -> None:
        import json
        with open(Path("src/gas_calibrator/v2/ui_v2/locales/zh_CN.json"), encoding="utf-8") as f:
            zh = json.load(f)
        assert "governance_handoff" in zh
        gov = zh["governance_handoff"]
        from gas_calibrator.v2.core.governance_handoff_contracts import GOVERNANCE_HANDOFF_I18N_KEYS
        for key, i18n_key in GOVERNANCE_HANDOFF_I18N_KEYS.items():
            leaf = i18n_key.split(".")[-1]
            assert leaf in gov, f"zh_CN missing i18n key '{leaf}' for artifact {key}"

    def test_en_us_has_governance_handoff(self) -> None:
        import json
        with open(Path("src/gas_calibrator/v2/ui_v2/locales/en_US.json"), encoding="utf-8") as f:
            en = json.load(f)
        assert "governance_handoff" in en
        gov = en["governance_handoff"]
        from gas_calibrator.v2.core.governance_handoff_contracts import GOVERNANCE_HANDOFF_I18N_KEYS
        for key, i18n_key in GOVERNANCE_HANDOFF_I18N_KEYS.items():
            leaf = i18n_key.split(".")[-1]
            assert leaf in gov, f"en_US missing i18n key '{leaf}' for artifact {key}"

    def test_zh_cn_labels_contain_cjk(self) -> None:
        import json
        with open(Path("src/gas_calibrator/v2/ui_v2/locales/zh_CN.json"), encoding="utf-8") as f:
            zh = json.load(f)
        gov = zh["governance_handoff"]
        for key, label in gov.items():
            assert any('\u4e00' <= c <= '\u9fff' for c in label), \
                f"zh_CN governance_handoff.{key} has no CJK: {label}"

    def test_en_us_labels_no_cjk(self) -> None:
        import json
        with open(Path("src/gas_calibrator/v2/ui_v2/locales/en_US.json"), encoding="utf-8") as f:
            en = json.load(f)
        gov = en["governance_handoff"]
        for key, label in gov.items():
            assert not any('\u4e00' <= c <= '\u9fff' for c in label), \
                f"en_US governance_handoff.{key} has CJK: {label}"


# ---------------------------------------------------------------------------
# 6. Step 2 boundary
# ---------------------------------------------------------------------------


class TestGovernanceHandoffStep2Boundary:
    """Governance handoff artifacts must maintain Step 2 boundary."""

    def test_no_real_paths_in_contracts(self) -> None:
        import gas_calibrator.v2.core.governance_handoff_contracts as gc
        import inspect
        import re
        source = inspect.getsource(gc)
        # Check for real device / serial port references, not variable names containing substrings
        assert not re.search(r'\bCOM\d+\b', source), "Real COM port reference found"
        assert not re.search(r'["\']serial["\']', source), "Serial port reference found"
        assert not re.search(r'["\']real_device["\']', source), "Real device reference found"

    def test_no_formal_approval_language(self) -> None:
        from gas_calibrator.v2.core.governance_handoff_contracts import (
            GOVERNANCE_HANDOFF_DISPLAY_LABELS,
            GOVERNANCE_HANDOFF_DISPLAY_LABELS_EN,
            GOVERNANCE_HANDOFF_ARTIFACT_KEYS,
        )
        forbidden = ["formal approval", "real acceptance", "accredited", "certified"]
        for key in GOVERNANCE_HANDOFF_ARTIFACT_KEYS:
            zh = GOVERNANCE_HANDOFF_DISPLAY_LABELS[key].lower()
            en = GOVERNANCE_HANDOFF_DISPLAY_LABELS_EN[key].lower()
            for word in forbidden:
                assert word not in zh, f"Chinese label for {key} contains '{word}'"
                assert word not in en, f"English label for {key} contains '{word}'"

    def test_all_boundary_markers_simulated(self) -> None:
        from gas_calibrator.v2.core.governance_handoff_contracts import (
            GOVERNANCE_HANDOFF_STEP2_BOUNDARY,
            GOVERNANCE_HANDOFF_ARTIFACT_KEYS,
        )
        for key in GOVERNANCE_HANDOFF_ARTIFACT_KEYS:
            b = GOVERNANCE_HANDOFF_STEP2_BOUNDARY[key]
            assert b["evidence_source"] == "simulated"
            assert b["not_real_acceptance_evidence"] is True
            assert b["not_ready_for_formal_claim"] is True
            assert b["reviewer_only"] is True
            assert b["readiness_mapping_only"] is True

    def test_reviewer_pairing_primary_has_reviewer(self) -> None:
        from gas_calibrator.v2.core.governance_handoff_contracts import (
            GOVERNANCE_HANDOFF_REVIEWER_PAIRING,
            GOVERNANCE_HANDOFF_ARTIFACT_KEYS,
        )
        for primary, reviewer in GOVERNANCE_HANDOFF_REVIEWER_PAIRING.items():
            assert primary in GOVERNANCE_HANDOFF_ARTIFACT_KEYS
            assert reviewer in GOVERNANCE_HANDOFF_ARTIFACT_KEYS
