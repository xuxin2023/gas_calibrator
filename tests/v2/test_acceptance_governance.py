from gas_calibrator.v2.core.acceptance_model import (
    build_user_visible_evidence_boundary,
    build_run_acceptance_plan,
    build_suite_acceptance_plan,
    build_validation_acceptance_snapshot,
)
from gas_calibrator.v2.core.step2_readiness import build_step2_readiness_summary


def test_validation_acceptance_snapshot_does_not_allow_simulated_promotion() -> None:
    payload = {
        "evidence_source": "simulated_protocol",
        "evidence_state": "simulated_protocol",
        "compare_status": "MATCH",
        "diagnostic_only": False,
        "acceptance_evidence": False,
        "not_real_acceptance_evidence": True,
        "reference_quality": {"reference_quality": "healthy"},
        "route_execution_summary": {
            "valid_for_route_diff": True,
            "relay_physical_mismatch": {"v1": False, "v2": False},
        },
    }

    snapshot = build_validation_acceptance_snapshot(payload)

    assert snapshot["acceptance_level"] == "offline_regression"
    assert snapshot["promotion_state"] == "dry_run_only"
    assert snapshot["ready_for_promotion"] is False
    assert "real acceptance evidence present" in snapshot["missing_conditions"]
    assert snapshot["promotion_plan"]["publish_primary_latest_allowed"] is False


def test_run_acceptance_plan_stays_dry_run_without_real_acceptance() -> None:
    plan = build_run_acceptance_plan(
        run_id="run_test",
        simulation_mode=True,
        reference_quality_ok_flag=True,
        export_error_count=0,
        parity_status="MATCH",
    )

    assert plan["evidence_source"] == "simulated_protocol"
    assert plan["not_real_acceptance_evidence"] is True
    assert plan["promotion_state"] == "dry_run_only"
    assert plan["ready_for_promotion"] is False


def test_suite_acceptance_plan_reports_missing_real_acceptance() -> None:
    plan = build_suite_acceptance_plan(
        suite_name="nightly",
        offline_green=True,
        parity_green=True,
        resilience_green=True,
        evidence_sources_present=["simulated", "replay", "simulated_protocol"],
    )

    assert plan["acceptance_scope"] == "suite"
    assert plan["evidence_source"] == "simulated_protocol"
    assert plan["evidence_sources_present"] == ["simulated_protocol", "replay"]
    assert plan["promotion_state"] == "dry_run_only"
    assert "real acceptance evidence present" in plan["missing_conditions"]


def test_user_visible_evidence_boundary_defaults_to_step2_dry_run_fields() -> None:
    payload = build_user_visible_evidence_boundary(simulation_mode=True)

    assert payload["evidence_source"] == "simulated_protocol"
    assert payload["not_real_acceptance_evidence"] is True
    assert payload["acceptance_level"] == "offline_regression"
    assert payload["promotion_state"] == "dry_run_only"


def test_step2_readiness_summary_reports_engineering_isolation_preparation_without_claiming_real_acceptance() -> None:
    readiness = build_step2_readiness_summary(
        run_id="run_ready",
        simulation_mode=True,
        config_governance_handoff={
            "simulation_only": True,
            "operator_safe": True,
            "real_port_device_count": 0,
            "engineering_only_flag_count": 0,
            "enabled_engineering_flags": [],
            "risk_markers": [],
            "execution_gate": {"status": "open"},
            "step2_default_workflow_allowed": True,
            "requires_explicit_unlock": False,
        },
    )

    gate_ids = {str(item["gate_id"]) for item in readiness["gates"]}

    assert readiness["phase"] == "step2_readiness_bridge"
    assert readiness["mode"] == "simulation_only"
    assert readiness["overall_status"] == "ready_for_engineering_isolation"
    assert readiness["evidence_mode"] == "simulation_offline_headless"
    assert readiness["not_real_acceptance_evidence"] is True
    assert readiness["blocking_items"] == []
    assert {
        "simulation_only_boundary",
        "real_bench_locked_by_default",
        "shared_experiment_flags_default_off",
        "offline_only_adapters_not_in_default_path",
        "reviewer_surface_hydration_chain_ready",
        "headless_smoke_path_available",
        "step2_gate_status",
    } <= gate_ids
    assert readiness["reviewer_display"]["status_line"].startswith("阶段状态：")
    assert "不是 real acceptance" in readiness["reviewer_display"]["summary_text"]
    assert all(str(item["reason_code"]).strip() for item in readiness["gates"])


def test_step2_readiness_summary_blocks_enabled_engineering_flags_but_keeps_raw_contract_machine_readable() -> None:
    readiness = build_step2_readiness_summary(
        run_id="run_blocked",
        simulation_mode=True,
        config_governance_handoff={
            "simulation_only": True,
            "operator_safe": False,
            "real_port_device_count": 0,
            "engineering_only_flag_count": 1,
            "enabled_engineering_flags": ["workflow.pressure.capture_then_hold_enabled"],
            "risk_markers": ["engineering_only_flags_enabled"],
            "execution_gate": {"status": "blocked"},
            "step2_default_workflow_allowed": False,
            "requires_explicit_unlock": True,
        },
    )

    step2_gate = next(item for item in readiness["gates"] if item["gate_id"] == "step2_gate_status")
    flag_gate = next(item for item in readiness["gates"] if item["gate_id"] == "shared_experiment_flags_default_off")

    assert readiness["overall_status"] == "not_ready"
    assert "shared_experiment_flags_default_off" in readiness["blocking_items"]
    assert step2_gate["status"] == "not_ready"
    assert step2_gate["reason_code"] == "execution_gate_blocked"
    assert flag_gate["status"] == "blocked"
    assert flag_gate["details"]["enabled_engineering_flags"] == ["workflow.pressure.capture_then_hold_enabled"]
    assert "不是 real acceptance" in readiness["reviewer_display"]["summary_text"]
