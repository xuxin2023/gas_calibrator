from __future__ import annotations

import json

from gas_calibrator.v1_5.workstation_snapshot import (
    ARTIFACT_ROLES,
    EXPORT_STATUSES,
    REPORT_AUTHORITY,
    SCHEMA_VERSION,
    build_workstation_snapshot,
)


def test_snapshot_preserves_45_13_and_distinct_physical_anchors(tmp_path) -> None:
    snapshot = build_workstation_snapshot(output_dir=tmp_path)

    assert snapshot["schema_version"] == SCHEMA_VERSION
    assert snapshot["product_version"] == "V1.5"
    assert snapshot["point_counts"] == {"co2": 45, "h2o": 13}
    assert snapshot["results"]["anchors"] == {
        "co2": {
            "kind": "co2_zero_gas",
            "independent_from_h2o": True,
        },
        "h2o": {
            "kind": "h2o_dry_gas",
            "independent_from_co2": True,
        },
    }
    assert snapshot["evidence_source"] == "simulated"
    assert snapshot["not_real_acceptance_evidence"] is True
    assert snapshot["plan"]["total_points"] == 58
    assert snapshot["plan"]["routes"] == [
        {
            "route_kind": "co2",
            "point_count": 45,
            "execution_mode": "mature_runner_dry_run",
            "status": "planned",
        },
        {
            "route_kind": "h2o",
            "point_count": 13,
            "execution_mode": "mature_runner_dry_run",
            "status": "planned",
        },
    ]
    assert snapshot["plan"]["editable"] is False
    assert snapshot["plan"]["point_table_edit_allowed"] is False
    assert snapshot["qc"]["overall_status"] == "pending"
    assert {
        row["check_id"]: row["status"] for row in snapshot["qc"]["checks"]
    }["sample_stability"] == "not_evaluated"
    assert {
        row["check_id"]: row["status"] for row in snapshot["qc"]["checks"]
    }["real_device_readback"] == "not_evaluated"
    assert snapshot["qc"]["point_evidence_contract"]["status"] == (
        "not_evaluated"
    )
    assert snapshot["qc"]["point_evidence_contract"]["authority"] == (
        "mature_v1_5_runner_artifacts"
    )
    assert snapshot["qc"]["point_evidence_contract"]["artifact_roles"] == [
        "execution_rows",
        "execution_summary",
        "formal_analysis",
    ]
    assert "threshold_profile_hash" in snapshot["qc"][
        "point_evidence_contract"
    ]["required_fields"]
    assert snapshot["qc"]["rule_threshold_governance"] == {
        "status": "runner_owned_read_only",
        "source": "reviewed_runtime_config_and_mature_runner_qc",
        "ui_edit_allowed": False,
        "policy_version_required": True,
        "threshold_profile_hash_required": True,
    }
    assert snapshot["qc"]["reject_reason_summary"]["status"] == (
        "not_evaluated"
    )
    assert snapshot["devices"]["overall_status"] == "simulation_only"
    assert snapshot["devices"]["ui_mode"] == "read_only_configured_slots"
    assert snapshot["devices"]["runtime_state_authority"] == (
        "mature_v1_5_runner_only"
    )
    assert snapshot["devices"]["real_device_state"] == "not_evaluated"
    assert snapshot["devices"]["connection_policy"] == "no_com_no_scan"
    assert snapshot["devices"]["configured_channel_count"] == 6
    assert snapshot["devices"]["connected_count"] == 0
    assert snapshot["devices"]["unknown_health_count"] == 6
    assert snapshot["devices"]["initialization_contract"] == {
        "owner": "mature_v1_5_initialization_flow",
        "runtime_mode": "MODE2",
        "upload_rate_hz": 1,
        "temperature_coefficients": "SENCO7_SENCO8_neutral",
        "neutralization_evidence_required": True,
        "readback_verification_required": True,
        "performed_by_read_only_workstation": False,
    }
    assert snapshot["devices"]["simulation_preset_actions_available"] is False
    assert snapshot["devices"]["fault_injection_actions_available"] is False
    assert snapshot["devices"]["route_control_actions_available"] is False
    assert snapshot["devices"]["device_configuration_actions_available"] is False
    assert snapshot["devices"]["contains_runtime_device_data"] is False
    assert all(
        row["identity_status"] == "not_evaluated"
        and row["health_status"] == "not_evaluated"
        and row["connection_status"] == "not_connected"
        for row in snapshot["devices"]["channels"]
    )
    assert snapshot["algorithm"]["overall_status"] == (
        "locked_production_default"
    )
    assert snapshot["algorithm"]["production_profile"]["profile_id"] == (
        "legacy_ratio_production"
    )
    assert snapshot["algorithm"]["shadow_candidates"][0][
        "promotion_state"
    ] == "blocked"
    assert snapshot["algorithm"]["auto_select"] is False
    assert snapshot["reports"]["allowed_roles"] == list(ARTIFACT_ROLES)
    assert snapshot["reports"]["allowed_export_statuses"] == list(
        EXPORT_STATUSES
    )
    assert snapshot["reports"]["authority"] == REPORT_AUTHORITY
    assert snapshot["reports"]["ui_mode"] == "read_only_inventory"
    assert snapshot["reports"]["export_actions_available"] is False
    assert snapshot["reports"]["formal_release_status"] == "not_evaluated"
    assert snapshot["reports"][
        "formal_release_requires_independent_review"
    ] is True
    assert snapshot["reports"]["formal_certificate_signing_available"] is False
    assert snapshot["reports"]["not_real_acceptance_evidence"] is True


def test_plan_preview_redacts_runner_paths_commands_and_ports() -> None:
    snapshot = build_workstation_snapshot(
        execution={
            "routes": [
                {
                    "route_kind": "co2",
                    "expected_point_count": 45,
                    "queue_csv": r"D:\private\co2.csv",
                    "argv": ["--port", "COM35"],
                    "command_preview": "python secret_runner.py",
                },
                {
                    "route_kind": "h2o",
                    "expected_point_count": 13,
                    "output_dir": r"D:\private\h2o",
                },
            ]
        }
    )

    plan_text = json.dumps(snapshot["plan"], ensure_ascii=False)
    assert snapshot["plan"]["contains_paths"] is False
    assert "D:\\private" not in plan_text
    assert "COM35" not in plan_text
    assert "secret_runner.py" not in plan_text


def test_device_summary_ignores_untrusted_live_identity_payload() -> None:
    snapshot = build_workstation_snapshot(
        execution={
            "device_slots": [
                {
                    "port": "COM35",
                    "serial_number": "REAL-DEVICE-001",
                    "health_status": "healthy",
                }
            ]
        }
    )

    device_text = json.dumps(snapshot["devices"], ensure_ascii=False)
    assert "COM35" not in device_text
    assert "REAL-DEVICE-001" not in device_text
    assert "healthy" not in device_text
    assert snapshot["devices"]["contains_ports"] is False
    assert snapshot["devices"]["contains_serial_numbers"] is False
    assert snapshot["devices"]["device_control_actions_available"] is False
    assert snapshot["devices"]["hardware_refresh_actions_available"] is False
    assert snapshot["devices"]["simulation_preset_actions_available"] is False
    assert snapshot["devices"]["fault_injection_actions_available"] is False
    assert snapshot["devices"]["route_control_actions_available"] is False
    assert snapshot["devices"]["contains_runtime_device_data"] is False


def test_algorithm_summary_blocks_implicit_shadow_profile_switch() -> None:
    snapshot = build_workstation_snapshot(
        execution={"profile_id": "absorption_ratio_shadow"}
    )

    assert snapshot["algorithm"]["overall_status"] == "blocked"
    assert snapshot["algorithm"]["observed_profile_id"] == (
        "absorption_ratio_shadow"
    )
    assert snapshot["algorithm"]["production_profile"]["profile_id"] == (
        "legacy_ratio_production"
    )
    assert snapshot["algorithm"]["profile_selection_actions_available"] is False
    assert snapshot["algorithm"]["coefficient_write_actions_available"] is False
    assert snapshot["algorithm"]["blockers"] == [
        "observed_profile_is_not_locked_production_default",
        "implicit_profile_switch_forbidden",
    ]


def test_snapshot_normalizes_dry_run_results_and_bounded_artifacts(
    tmp_path,
) -> None:
    (tmp_path / "v1_5_operator_workstation_dry_run.json").write_text(
        "{}",
        encoding="utf-8",
    )
    snapshot = build_workstation_snapshot(
        execution={
            "overall_status": "pass",
            "run_id": "snapshot-pass",
            "point_counts": {"co2": 45, "h2o": 13},
            "route_results": [
                {
                    "route_kind": "co2",
                    "status": "pass",
                    "dry_run_points": 45,
                },
                {
                    "route_kind": "h2o",
                    "status": "pass",
                    "dry_run_points": 13,
                },
            ],
            "not_real_acceptance_evidence": True,
            "opens_com_ports": False,
            "writes_coefficients": False,
        },
        output_dir=tmp_path,
    )

    assert snapshot["run"]["status"] == "pass"
    assert snapshot["run"]["route_results"] == [
        {
            "route_kind": "co2",
            "status": "pass",
            "point_count": 45,
            "blockers": [],
        },
        {
            "route_kind": "h2o",
            "status": "pass",
            "point_count": 13,
            "blockers": [],
        },
    ]
    assert snapshot["review"]["overall_status"] == "dry_run_review_ready"
    assert snapshot["plan"]["status"] == "executed_dry_run"
    assert snapshot["qc"]["overall_status"] == "dry_run_pass"
    qc_statuses = {
        row["check_id"]: row["status"] for row in snapshot["qc"]["checks"]
    }
    assert qc_statuses["point_count_contract"] == "pass"
    assert qc_statuses["anchor_separation"] == "pass"
    assert qc_statuses["no_write_safety"] == "pass"
    assert qc_statuses["route_dry_run_closure"] == "pass"
    assert qc_statuses["sample_stability"] == "not_evaluated"
    assert qc_statuses["real_device_readback"] == "not_evaluated"
    assert snapshot["qc"]["point_evidence_contract"][
        "available_row_count"
    ] == 0
    assert snapshot["qc"]["rule_threshold_governance"][
        "ui_edit_allowed"
    ] is False
    assert snapshot["reports"]["present_count"] == 1
    assert {
        row["export_status"] for row in snapshot["reports"]["artifacts"]
    } == {"ok", "missing"}
    assert snapshot["reports"]["contains_paths"] is False
    assert all(
        row["role"] == "execution_summary"
        for row in snapshot["reports"]["artifacts"]
    )


def test_snapshot_keeps_certificate_advisory_and_has_no_control_actions() -> None:
    snapshot = build_workstation_snapshot(
        certificate_records=[
            {"record_id": "a", "review_state": "draft"},
            {"record_id": "b", "review_state": "pending_review"},
        ]
    )

    assert snapshot["certificate"]["record_count"] == 2
    assert snapshot["certificate"]["review_state_counts"] == {
        "draft": 1,
        "pending_review": 1,
    }
    assert snapshot["certificate"]["start_gate"] == "non_blocking"
    assert snapshot["certificate"]["connected_to_calibration"] is False
    assert snapshot["review"]["approval_actions_available"] is False
    assert snapshot["review"]["coefficient_write_actions_available"] is False
    assert snapshot["safety"]["status"] == "pass"
    assert snapshot["opens_com_ports"] is False
    assert snapshot["controls_water_or_gas_routes"] is False
    assert snapshot["writes_coefficients"] is False
    assert snapshot["writes_device_id"] is False


def test_snapshot_blocks_any_upstream_safety_drift() -> None:
    snapshot = build_workstation_snapshot(
        execution={
            "overall_status": "pass",
            "opens_com_ports": True,
            "writes_coefficients": True,
            "not_real_acceptance_evidence": False,
        }
    )

    assert snapshot["safety"]["status"] == "blocked"
    assert snapshot["review"]["overall_status"] == "blocked"
    assert snapshot["plan"]["status"] == "blocked"
    assert snapshot["qc"]["overall_status"] == "blocked"
    assert snapshot["safety"]["violations"] == [
        "opens_com_ports_true",
        "writes_coefficients_true",
        "dry_run_claimed_as_real_acceptance",
    ]


def test_qc_blocks_explicit_invalid_or_mismatched_point_counts() -> None:
    invalid = build_workstation_snapshot(
        execution={"point_counts": {"co2": "invalid", "h2o": 13}}
    )
    mismatched = build_workstation_snapshot(
        execution={"point_counts": {"co2": 44, "h2o": 13}}
    )

    assert invalid["point_counts"] == {"co2": 0, "h2o": 13}
    assert invalid["qc"]["overall_status"] == "blocked"
    assert invalid["algorithm"]["overall_status"] == "blocked"
    assert "point_count_contract_failed" in invalid["qc"]["blockers"]
    assert mismatched["qc"]["overall_status"] == "blocked"
    assert "point_count_contract_failed" in mismatched["qc"]["blockers"]
