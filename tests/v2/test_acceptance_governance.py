from gas_calibrator.v2.core.acceptance_model import (
    build_user_visible_evidence_boundary,
    build_run_acceptance_plan,
    build_suite_acceptance_plan,
    build_validation_acceptance_snapshot,
)


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
