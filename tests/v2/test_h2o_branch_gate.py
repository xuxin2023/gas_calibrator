from __future__ import annotations

import pytest
from gas_calibrator.v2.core.run001_h2o_only_1_point_no_write_probe import (
    evaluate_h2o_1_point_no_write_gate,
    load_json_mapping,
)


def _minimal_config(**overrides):
    base = {
        "h2o_only": True,
        "single_route": True,
        "single_temperature_group": True,
        "no_write": True,
        "not_real_acceptance_evidence": True,
        "default_cutover_to_v2": False,
        "disable_v1": False,
        "scope": "run001_h2o_1_point",
    }
    base.update(overrides)
    return base


def _perform_gate(cfg, operator_confirmation_path=None, branch="codex/run001-a1-no-write-dry-run", head="abc1234"):
    return evaluate_h2o_1_point_no_write_gate(
        cfg,
        cli_allow=True,
        operator_confirmation_path=operator_confirmation_path,
        branch=branch,
        head=head,
        config_path="",
        run_app_py_untouched=True,
    )


class TestH2OBranchGate:
    """Tests for H2O admission branch gate configurability."""

    def test_default_allowed_branch_passes(self):
        cfg = _minimal_config()
        admission = _perform_gate(cfg, branch="codex/run001-a1-no-write-dry-run")
        reasons = [r for r in admission.reasons if "branch" in r.lower()]
        assert not reasons, f"default branch should pass, got {reasons}"

    def test_unknown_branch_rejected_by_default(self):
        cfg = _minimal_config()
        admission = _perform_gate(cfg, branch="some/other-branch")
        assert "current_branch_not_allowed_for_h2o_1_point_no_write" in admission.reasons

    def test_configured_allowed_branch_passes(self):
        cfg = _minimal_config(allowed_branches=["codex/v2-golden-recovery-v210-h2o"])
        admission = _perform_gate(cfg, branch="codex/v2-golden-recovery-v210-h2o")
        branch_reasons = [r for r in admission.reasons if "branch" in r.lower()]
        assert not branch_reasons, f"configured branch should pass, got {branch_reasons}"

    def test_configured_allowed_branches_multi(self):
        cfg = _minimal_config(allowed_branches=["codex/run001-a1-no-write-dry-run", "codex/v2-golden-recovery-cdb82111"])
        admission = _perform_gate(cfg, branch="codex/v2-golden-recovery-cdb82111")
        branch_reasons = [r for r in admission.reasons if "branch" in r.lower()]
        assert not branch_reasons

    def test_allowed_branches_evidence_written(self):
        cfg = _minimal_config(allowed_branches=["codex/v2-golden-recovery-cdb82111"])
        admission = _perform_gate(cfg, branch="codex/v2-golden-recovery-cdb82111")
        evidence = dict(admission.evidence)
        assert evidence.get("branch_allowed") is True
        assert evidence.get("current_branch") == "codex/v2-golden-recovery-cdb82111"
        assert evidence.get("allowed_branches") == ["codex/v2-golden-recovery-cdb82111"]

    def test_disallowed_branch_evidence_written(self):
        cfg = _minimal_config(allowed_branches=["codex/run001-a1-no-write-dry-run"])
        admission = _perform_gate(cfg, branch="codex/other-branch")
        evidence = dict(admission.evidence)
        assert evidence.get("branch_allowed") is False
        assert evidence.get("allowed_branches") == ["codex/run001-a1-no-write-dry-run"]

    def test_no_write_not_relaxed_by_branch(self):
        cfg = _minimal_config(no_write=False, allowed_branches=["any-branch"])
        admission = _perform_gate(cfg)
        assert "config_no_write_not_true" in admission.reasons

    def test_head_mismatch_still_fails_with_allowed_branch(self):
        cfg = _minimal_config(allowed_branches=["codex/run001-a1-no-write-dry-run"])
        admission = evaluate_h2o_1_point_no_write_gate(
            cfg,
            cli_allow=True,
            operator_confirmation_path=None,
            branch="codex/run001-a1-no-write-dry-run",
            head="",
            config_path="",
            run_app_py_untouched=True,
        )
        assert "current_head_missing" in admission.reasons

    def test_empty_branch_does_not_reject(self):
        cfg = _minimal_config()
        admission = _perform_gate(cfg, branch="")
        branch_reasons = [r for r in admission.reasons if "branch" in r.lower()]
        assert not branch_reasons
