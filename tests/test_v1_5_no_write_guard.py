"""Governance tests for the V1/V1.5 global no-write policy.

The accepted policy deliberately avoids installing an implicit global guard in
the production runner. No-write remains mandatory for offline/V2 engineering
probe scopes, while V1/V1.5 writes retain operation-specific authorization.
"""

from __future__ import annotations

import json
from pathlib import Path

from gas_calibrator.workflow.runner import CalibrationRunner


POLICY_PATH = (
    Path(__file__).resolve().parents[1]
    / "docs"
    / "architecture"
    / "v1_5_global_no_write_policy.json"
)


def _load_policy() -> dict:
    return json.loads(POLICY_PATH.read_text(encoding="utf-8"))


def _make_runner() -> CalibrationRunner:
    class FakeLogger:
        run_dir = None

    cfg = {"paths": {"output_dir": "logs"}}
    return CalibrationRunner(cfg, {}, FakeLogger(), lambda _message: None, lambda _message: None)


def test_policy_rejects_an_implicit_production_global_guard() -> None:
    policy = _load_policy()

    assert policy["decision"] == "no_implicit_global_no_write_guard"
    assert policy["status"] == "accepted"
    assert policy["production_default"]["global_no_write_guard_enabled"] is False


def test_runner_does_not_install_hypothetical_global_guard_state() -> None:
    runner = _make_runner()

    assert not hasattr(runner, "_no_write_guard_enabled")
    assert not hasattr(runner, "_no_write_guard_blocked_count")
    assert not hasattr(runner, "_no_write_guard_blocked_reasons")


def test_policy_preserves_v1_and_default_entry_invariants() -> None:
    invariants = _load_policy()["protected_invariants"]

    assert invariants["v1_fallback_preserved"] is True
    assert invariants["run_app_default_entry_unchanged"] is True
    assert invariants["v2_real_com_default_allowed"] is False
    assert invariants["v2_real_acceptance_claim_allowed"] is False


def test_offline_and_step3a_scopes_remain_no_write() -> None:
    scopes = _load_policy()["scope_policy"]
    step3a = scopes["v2_step3a_engineering_probe"]

    assert scopes["simulation_replay_offline"]["no_write_required"] is True
    assert step3a["no_write_required"] is True
    assert step3a["dual_unlock_required"] is True
    assert step3a["operator_confirmation_record_required"] is True
    assert step3a["engineering_probe_only"] is True
    assert step3a["promotion_state"] == "blocked"
    assert step3a["not_real_acceptance_evidence"] is True


def test_v1_v1_5_writes_require_operation_specific_controls() -> None:
    controlled = _load_policy()["scope_policy"]["v1_v1_5_controlled_write"]

    assert controlled["implicit_global_block"] is False
    assert controlled["operation_specific_authorization_required"] is True
    assert controlled["prewrite_snapshot_required"] is True
    assert controlled["postwrite_readback_required"] is True
    assert controlled["rollback_plan_required"] is True
    assert controlled["postwrite_reverification_required"] is True


def test_future_global_guard_change_has_an_explicit_gate() -> None:
    gate = _load_policy()["future_change_gate"]

    assert gate["separate_adr_required"] is True
    assert gate["v1_production_impact_review_required"] is True
    assert gate["parity_required"] is True
    assert gate["writeback_fault_injection_required"] is True
    assert gate["explicit_user_authorization_required"] is True
