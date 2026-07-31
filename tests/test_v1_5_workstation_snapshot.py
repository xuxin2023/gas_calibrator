from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

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
        "mature_v1_v1_5_artifacts_only"
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
        "upload_rate_scope": "calibration_upload_timebase",
        "average1": None,
        "average2": None,
        "averages_are_independent": True,
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
    assert snapshot["runtime"]["freshness_status"] == "unknown"
    assert snapshot["physical_reference"]["temperature_truth"] == {
        "source": "digital_platinum_resistance_thermometer_in_chamber",
        "chamber_controller_display_is_truth": False,
    }
    assert snapshot["physical_reference"]["flow"][
        "used_for_concentration_fit"
    ] is False
    assert snapshot["physical_reference"]["sampling"] == {
        "calibration_upload_timebase_hz": 1,
        "raw_device_internal_acquisition_rate_claimed": False,
        "average1_average2_are_independent": True,
    }
    assert snapshot["physical_reference"]["anchors"][
        "h2o_dry_anchor_is_additional"
    ] is True


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


def _write_mature_runtime_artifacts(tmp_path) -> tuple[datetime, dict]:
    run_dir = tmp_path / "run_20260731_120000"
    run_dir.mkdir()
    io_path = run_dir / "io_20260731_120000.csv"
    io_path.write_text(
        "\n".join(
            (
                "timestamp,port,device,direction,command,response,error",
                (
                    '2026-07-31T12:00:00.000,RUN,runner,EVENT,stage,'
                    '"{""current"": ""CO2 400 ppm 1000 hPa"", '
                    '""route_group"": ""CO2 group A""}",'
                ),
                (
                    "2026-07-31T12:00:01.000,COM35,gas_analyzer,RX,"
                    'read,"YGAS,001,400.1,2.1",'
                ),
                (
                    "2026-07-31T12:00:02.000,COM36,gas_analyzer,RX,"
                    'read,"YGAS,002,399.9,2.0",'
                ),
                (
                    '2026-07-31T12:00:03.000,RUN,runner,EVENT,sample-progress,'
                    '"{""text"": ""采样进度：4/10""}",'
                ),
            )
        )
        + "\n",
        encoding="utf-8",
    )
    (run_dir / "runtime_config_snapshot.json").write_text(
        json.dumps(
            {
                "devices": {
                    "gas_analyzers": [
                        {
                            "name": "ga01",
                            "enabled": True,
                            "port": "COM35",
                            "ftd_hz": 1,
                            "average1": 49,
                            "average2": 50,
                        },
                        {
                            "name": "ga02",
                            "enabled": True,
                            "port": "COM36",
                            "ftd_hz": 1,
                            "average1": 49,
                            "average2": 50,
                        },
                    ]
                }
            }
        ),
        encoding="utf-8",
    )
    (run_dir / "samples_20260731_120000.csv").write_text(
        "\n".join(
            (
                (
                    "sample_ts,thermometer_sample_ts,thermometer_temp_c,"
                    "pressure_gauge_sample_ts,pressure_gauge_hpa,"
                    "dewpoint_sample_ts,dewpoint_c,flow_lpm"
                ),
                (
                    "2026-07-31T12:00:02.500,2026-07-31T12:00:02.000,"
                    "24.93,2026-07-31T12:00:02.100,1000.2,"
                    "2026-07-31T12:00:02.200,-44.8,1.25"
                ),
            )
        )
        + "\n",
        encoding="utf-8",
    )
    mtime = datetime.fromtimestamp(io_path.stat().st_mtime, timezone.utc)
    site_profile = {
        "reported_connected_count": 4,
        "reported_powered_count": 4,
        "candidate_analyzers": [
            {
                "port": f"COM{35 + index}",
                "ga_label": f"GA{index + 1:02d}",
                "connected": index < 4,
                "powered": index < 4,
                "operator_confirmed": index < 4,
                "protocol_device_id": f"{index + 1:03d}",
                "sn_code": f"012607{index + 1:02d}",
                "runtime_evidence": {
                    "ftd_hz": 1,
                    "average1": 49,
                    "average2": 50,
                },
            }
            for index in range(8)
        ],
    }
    return mtime, site_profile


def test_snapshot_reads_only_fresh_mature_runtime_artifacts(tmp_path) -> None:
    mtime, site_profile = _write_mature_runtime_artifacts(tmp_path)
    snapshot = build_workstation_snapshot(
        runtime_output_dir=tmp_path,
        site_profile=site_profile,
        now_utc=mtime + timedelta(seconds=5),
    )

    assert snapshot["display_mode"] == "mature_runtime_artifact_read_only"
    assert snapshot["evidence_source"] == "mature_runner_artifact_read_only"
    assert snapshot["runtime"]["freshness_status"] == "fresh"
    assert snapshot["runtime"]["current_stage"] == "CO2 400 ppm 1000 hPa"
    assert snapshot["runtime"]["sample_progress"] == "采样进度：4/10"
    assert snapshot["runtime"]["contains_paths"] is False
    assert snapshot["runtime"]["source_files"] == [
        "io_20260731_120000.csv",
        "runtime_config_snapshot.json",
        "samples_20260731_120000.csv",
    ]
    assert snapshot["devices"]["overall_status"] == "runtime_artifact_fresh"
    assert snapshot["devices"]["configured_channel_count"] == 8
    assert snapshot["devices"]["reported_connected_count"] == 4
    assert snapshot["devices"]["reported_powered_count"] == 4
    assert snapshot["devices"]["mapped_connected_count"] == 4
    assert snapshot["devices"]["powered_count"] == 4
    assert snapshot["devices"]["connected_count"] == 2
    assert snapshot["devices"]["initialization_contract"]["upload_rate_hz"] == 1
    assert snapshot["devices"]["initialization_contract"]["average1"] == 49
    assert snapshot["devices"]["initialization_contract"]["average2"] == 50
    assert snapshot["physical_reference"]["observations"]["temperature"][
        "value"
    ] == 24.93
    assert snapshot["physical_reference"]["observations"]["flow"] == {
        "value": 1.25,
        "unit": "L/min",
        "source": "dewpoint_meter_output",
        "channel": "dewpoint_meter_flow_output",
        "sample_timestamp": "2026-07-31T12:00:02.200",
        "freshness_status": "fresh",
        "role": "existence_and_stability_monitoring_only",
        "used_for_concentration_fit": False,
    }
    assert snapshot["physical_reference"]["pressure_chain"]["status"] == (
        "controller_feedback_missing"
    )
    assert snapshot["physical_reference"]["pressure_chain"][
        "calibration_ready"
    ] is False
    serialized = json.dumps(snapshot, ensure_ascii=False)
    assert "COM35" not in serialized
    assert str(tmp_path) not in serialized
    assert snapshot["opens_com_ports"] is False
    assert snapshot["not_real_acceptance_evidence"] is True


def test_snapshot_normalizes_bilingual_pressure_chain_fields(
    tmp_path,
) -> None:
    _, site_profile = _write_mature_runtime_artifacts(tmp_path)
    run_dir = tmp_path / "run_20260731_120000"
    sample_path = run_dir / "samples_20260731_120000.csv"
    sample_path.write_text(
        "\n".join(
            (
                (
                    "采样时间,数字温度计缓存采样时间,数字温度计温度C,"
                    "数字压力计采样时间,数字压力计压力hPa,"
                    "露点仪实时采样时间,露点仪实时露点C,"
                    "露点仪实时流量(L/min),压力控制器采样时间,"
                    "压力控制器压力hPa,压力控制器输出状态,"
                    "压力控制器隔离状态,压力控制器通大气状态"
                ),
                (
                    "2026-07-31T12:00:02.500,"
                    "2026-07-31T12:00:02.000,0.0,"
                    "2026-07-31T12:00:02.100,1000.2,"
                    "2026-07-31T12:00:02.200,-44.8,1.25,"
                    "2026-07-31T12:00:02.500,1000.7,1,0,0"
                ),
            )
        )
        + "\n",
        encoding="utf-8",
    )
    config_path = run_dir / "runtime_config_snapshot.json"
    config = json.loads(config_path.read_text(encoding="utf-8"))
    config["devices"].update(
        {
            "pressure_controller": {"enabled": True, "port": "COM23"},
            "pressure_gauge": {"enabled": True, "port": "COM22"},
        }
    )
    config_path.write_text(json.dumps(config), encoding="utf-8")
    io_path = run_dir / "io_20260731_120000.csv"
    mtime = datetime.fromtimestamp(io_path.stat().st_mtime, timezone.utc)

    snapshot = build_workstation_snapshot(
        runtime_output_dir=tmp_path,
        site_profile=site_profile,
        now_utc=mtime + timedelta(seconds=5),
    )

    observations = snapshot["physical_reference"]["observations"]
    pressure_chain = snapshot["physical_reference"]["pressure_chain"]
    assert observations["temperature"]["value"] == 0.0
    assert observations["pressure"]["value"] == 1000.2
    assert observations["pressure"]["role"] == (
        "metrological_truth_for_pressure_calibration"
    )
    assert observations["pressure_controller"]["value"] == 1000.7
    assert observations["pressure_controller"]["used_as_pressure_truth"] is False
    assert observations["dewpoint"]["value"] == -44.8
    assert observations["flow"]["value"] == 1.25
    assert pressure_chain["status"] == "fresh_coincident_observation"
    assert pressure_chain["calibration_ready"] is True
    assert pressure_chain["controller_configured"] is True
    assert pressure_chain["reference_configured"] is True
    assert pressure_chain["pair_timestamp_delta_ms"] == 400.0
    assert pressure_chain["pair_age_seconds"] == 0.5
    assert pressure_chain["pair_is_recent"] is True
    assert round(
        pressure_chain["controller_minus_reference_hpa"],
        3,
    ) == 0.5
    assert pressure_chain["delta_role"] == (
        "control_tracking_only_not_SENCO9_fit_residual"
    )

    sample_path.write_text(
        sample_path.read_text(encoding="utf-8").replace(
            ",2026-07-31T12:00:02.500,1000.7,",
            ",2026-07-31T12:00:05.500,1000.7,",
        ),
        encoding="utf-8",
    )
    noncoincident = build_workstation_snapshot(
        runtime_output_dir=tmp_path,
        site_profile=site_profile,
        now_utc=mtime + timedelta(seconds=5),
    )
    noncoincident_chain = noncoincident["physical_reference"][
        "pressure_chain"
    ]
    assert noncoincident_chain["status"] == "pair_not_coincident"
    assert noncoincident_chain["pair_is_coincident"] is False
    assert noncoincident_chain["calibration_ready"] is False

    sample_path.write_text(
        sample_path.read_text(encoding="utf-8")
        .replace(
            "2026-07-31T12:00:02.500",
            "2026-07-31T11:59:30.500",
        )
        .replace(
            "2026-07-31T12:00:02.100",
            "2026-07-31T11:59:30.100",
        )
        .replace(
            "2026-07-31T12:00:02.000",
            "2026-07-31T11:59:30.000",
        )
        .replace(
            "2026-07-31T12:00:02.200",
            "2026-07-31T11:59:30.200",
        )
        .replace(
            "2026-07-31T12:00:05.500",
            "2026-07-31T11:59:30.500",
        ),
        encoding="utf-8",
    )
    old_pair = build_workstation_snapshot(
        runtime_output_dir=tmp_path,
        site_profile=site_profile,
        now_utc=mtime + timedelta(seconds=5),
    )
    old_pair_chain = old_pair["physical_reference"]["pressure_chain"]
    assert old_pair_chain["status"] == "pair_stale"
    assert old_pair_chain["pair_is_coincident"] is True
    assert old_pair_chain["pair_is_recent"] is False
    assert old_pair_chain["calibration_ready"] is False
    assert old_pair["physical_reference"]["observations"]["pressure"][
        "freshness_status"
    ] == "stale"


def test_snapshot_requires_configured_pressure_roles_for_calibration_ready(
    tmp_path,
) -> None:
    mtime, site_profile = _write_mature_runtime_artifacts(tmp_path)
    run_dir = tmp_path / "run_20260731_120000"
    sample_path = run_dir / "samples_20260731_120000.csv"
    sample_path.write_text(
        "\n".join(
            (
                (
                    "sample_ts,pressure_gauge_sample_ts,pressure_gauge_hpa,"
                    "pace_sample_ts,pace_pressure_hpa"
                ),
                (
                    "2026-07-31T12:00:02.500,2026-07-31T12:00:02.100,"
                    "1000.2,2026-07-31T12:00:02.200,1000.7"
                ),
            )
        )
        + "\n",
        encoding="utf-8",
    )

    snapshot = build_workstation_snapshot(
        runtime_output_dir=tmp_path,
        site_profile=site_profile,
        now_utc=mtime + timedelta(seconds=5),
    )
    pressure_chain = snapshot["physical_reference"]["pressure_chain"]

    assert pressure_chain["status"] == "reference_not_configured"
    assert pressure_chain["readback_ready"] is True
    assert pressure_chain["configuration_ready"] is False
    assert pressure_chain["reference_configured"] is False
    assert pressure_chain["controller_configured"] is False
    assert pressure_chain["calibration_ready"] is False


def test_snapshot_rejects_nonfinite_pressure_readbacks(tmp_path) -> None:
    mtime, site_profile = _write_mature_runtime_artifacts(tmp_path)
    run_dir = tmp_path / "run_20260731_120000"
    sample_path = run_dir / "samples_20260731_120000.csv"
    sample_path.write_text(
        "\n".join(
            (
                (
                    "sample_ts,pressure_gauge_sample_ts,pressure_gauge_hpa,"
                    "pace_sample_ts,pace_pressure_hpa"
                ),
                (
                    "2026-07-31T12:00:02.500,2026-07-31T12:00:02.100,"
                    "NaN,2026-07-31T12:00:02.200,Infinity"
                ),
            )
        )
        + "\n",
        encoding="utf-8",
    )
    config_path = run_dir / "runtime_config_snapshot.json"
    config = json.loads(config_path.read_text(encoding="utf-8"))
    config["devices"].update(
        {
            "pressure_controller": {
                "enabled": True,
                "port": "COM23",
            },
            "pressure_gauge": {
                "enabled": True,
                "port": "COM22",
            },
        }
    )
    config_path.write_text(json.dumps(config), encoding="utf-8")

    snapshot = build_workstation_snapshot(
        runtime_output_dir=tmp_path,
        site_profile=site_profile,
        now_utc=mtime + timedelta(seconds=5),
    )
    pressure_chain = snapshot["physical_reference"]["pressure_chain"]

    assert pressure_chain["status"] == "reference_missing"
    assert pressure_chain["readback_ready"] is False
    assert pressure_chain["configuration_ready"] is True
    assert pressure_chain["controller_minus_reference_hpa"] is None
    assert pressure_chain["calibration_ready"] is False


def test_snapshot_rejects_old_channel_frame_inside_fresh_artifact(
    tmp_path,
) -> None:
    _, site_profile = _write_mature_runtime_artifacts(tmp_path)
    io_path = (
        tmp_path
        / "run_20260731_120000"
        / "io_20260731_120000.csv"
    )
    io_path.write_text(
        "\n".join(
            (
                "timestamp,port,device,direction,command,response,error",
                (
                    "2026-07-31T12:00:00.000,COM35,gas_analyzer,RX,"
                    'read,"YGAS,001,400.1,2.1",'
                ),
                (
                    '2026-07-31T12:00:30.000,RUN,runner,EVENT,stage,'
                    '"{""current"": ""CO2 400 ppm 1000 hPa""}",'
                ),
            )
        )
        + "\n",
        encoding="utf-8",
    )
    mtime = datetime.fromtimestamp(io_path.stat().st_mtime, timezone.utc)

    snapshot = build_workstation_snapshot(
        runtime_output_dir=tmp_path,
        site_profile=site_profile,
        now_utc=mtime + timedelta(seconds=5),
    )

    first_channel = snapshot["devices"]["channels"][0]
    assert snapshot["runtime"]["freshness_status"] == "fresh"
    assert snapshot["devices"]["connected_count"] == 0
    assert first_channel["last_frame_age_seconds"] == 30
    assert first_channel["last_frame_status"] == "stale"
    assert first_channel["connection_status"] == "stale_frame"


def test_snapshot_marks_old_runtime_artifacts_stale_and_excludes_v2_names(
    tmp_path,
) -> None:
    mtime, site_profile = _write_mature_runtime_artifacts(tmp_path)
    stale = build_workstation_snapshot(
        runtime_output_dir=tmp_path,
        site_profile=site_profile,
        now_utc=mtime + timedelta(seconds=182),
    )
    assert stale["runtime"]["freshness_status"] == "stale"
    assert stale["devices"]["overall_status"] == "runtime_artifact_stale"
    assert stale["devices"]["connected_count"] == 0
    assert all(
        row["connection_status"] != "recent_frame"
        for row in stale["devices"]["channels"]
    )

    v2_only = tmp_path / "v2_only"
    run_dir = v2_only / "run_20260731_130000"
    run_dir.mkdir(parents=True)
    (run_dir / "io_log.csv").write_text(
        "timestamp,device,direction,data\n",
        encoding="utf-8",
    )
    (run_dir / "samples.csv").write_text("sample_ts\n", encoding="utf-8")
    excluded = build_workstation_snapshot(runtime_output_dir=v2_only)
    assert excluded["runtime"]["freshness_status"] == "unknown"
    assert excluded["display_mode"] == "simulated_read_only"


def test_snapshot_reads_dewpoint_flow_from_formal_reference_record(
    tmp_path,
) -> None:
    mtime, site_profile = _write_mature_runtime_artifacts(tmp_path)
    run_dir = tmp_path / "run_20260731_120000"
    (run_dir / "samples_20260731_120000.csv").write_text(
        "\n".join(
            (
                (
                    "sample_ts,thermometer_sample_ts,thermometer_temp_c,"
                    "pressure_gauge_sample_ts,pressure_gauge_hpa,"
                    "dewpoint_sample_ts,dewpoint_c"
                ),
                (
                    "2026-07-31T12:00:02.500,2026-07-31T12:00:02.000,"
                    "24.93,2026-07-31T12:00:02.100,1000.2,"
                    "2026-07-31T12:00:02.200,-44.8"
                ),
            )
        )
        + "\n",
        encoding="utf-8",
    )
    (run_dir / "formal_reference_source_record.json").write_text(
        json.dumps(
            {
                "schema_version": "v1_5_formal_reference_source_record_v1",
                "route_flow_evidence": {
                    "role": "route_and_process_evidence_only",
                    "source": "dewpoint_meter_output",
                    "observed_flow_lpm": 1.31,
                    "dewpoint_meter_output_flow_lpm": 1.31,
                },
            }
        ),
        encoding="utf-8",
    )

    snapshot = build_workstation_snapshot(
        runtime_output_dir=tmp_path,
        site_profile=site_profile,
        now_utc=mtime + timedelta(seconds=5),
    )
    flow = snapshot["physical_reference"]["observations"]["flow"]
    assert flow["value"] == 1.31
    assert flow["source"] == "dewpoint_meter_output"
    assert flow["channel"] == "dewpoint_meter_flow_output"
    assert flow["unit"] == "L/min"
    assert flow["sample_timestamp"]
    assert flow["role"] == "existence_and_stability_monitoring_only"
    assert flow["used_for_concentration_fit"] is False
    assert "formal_reference_source_record.json" in snapshot["runtime"][
        "source_files"
    ]
