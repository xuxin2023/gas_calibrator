from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping

from gas_calibrator.v2.core.run001_a2_co2_only_7_pressure_no_write_probe import (
    A2_ALLOWED_PRESSURE_POINTS_HPA,
    A2_ENV_VAR,
    evaluate_a2_co2_7_pressure_no_write_gate,
    write_a2_co2_7_pressure_no_write_probe_artifacts,
)


BRANCH = "codex/run001-a1-no-write-dry-run"
HEAD = "134a68188e9a3c53d720efccdb1b6de3cc0703e2"


def _write_a1r_prereq(tmp_path: Path, extra: dict[str, Any] | None = None) -> Path:
    run_dir = tmp_path / "a1r"
    run_dir.mkdir()
    payload = {
        "final_decision": "PASS",
        "evidence_source": "real_probe_a1r_minimal_no_write_sampling",
        "not_real_acceptance_evidence": True,
        "attempted_write_count": 0,
        "any_write_command_sent": False,
        "r0_1_reference_readonly_prereq_pass": True,
        "r0_full_query_only_prereq_pass": True,
        "r1_conditioning_only_prereq_pass": True,
        "a1r_minimal_sampling_executed": True,
    }
    payload.update(extra or {})
    (run_dir / "summary.json").write_text(json.dumps(payload), encoding="utf-8")
    return run_dir


def _base_config(tmp_path: Path) -> dict[str, Any]:
    a1r_dir = _write_a1r_prereq(tmp_path)
    return {
        "scope": "a2_co2_7_pressure_no_write",
        "a1r_output_dir": str(a1r_dir),
        "co2_only": True,
        "skip0": True,
        "single_route": True,
        "single_temperature": True,
        "no_write": True,
        "v1_fallback_required": True,
        "pressure_points_hpa": list(A2_ALLOWED_PRESSURE_POINTS_HPA),
        "sample_min_count_per_pressure": 4,
        "a3_enabled": False,
        "h2o_enabled": False,
        "full_group_enabled": False,
        "multi_temperature_enabled": False,
        "mode_switch_enabled": False,
        "analyzer_id_write_enabled": False,
        "senco_write_enabled": False,
        "calibration_write_enabled": False,
        "chamber_set_temperature_enabled": False,
        "chamber_start_enabled": False,
        "chamber_stop_enabled": False,
        "real_primary_latest_refresh": False,
        "a2_co2_7_pressure_no_write_probe": {
            "scope": "a2_co2_7_pressure_no_write",
            "a1r_output_dir": str(a1r_dir),
            "co2_only": True,
            "skip0": True,
            "single_route": True,
            "single_temperature": True,
            "no_write": True,
            "v1_fallback_required": True,
            "pressure_points_hpa": list(A2_ALLOWED_PRESSURE_POINTS_HPA),
            "sample_min_count_per_pressure": 4,
            "pressure_cache_max_age_ms": 2000,
            "a3_enabled": False,
            "h2o_enabled": False,
            "full_group_enabled": False,
            "multi_temperature_enabled": False,
            "mode_switch_enabled": False,
            "analyzer_id_write_enabled": False,
            "senco_write_enabled": False,
            "calibration_write_enabled": False,
            "chamber_set_temperature_enabled": False,
            "chamber_start_enabled": False,
            "chamber_stop_enabled": False,
            "real_primary_latest_refresh": False,
        },
    }


def _operator_confirmation(tmp_path: Path, config_path: Path, config: Mapping[str, Any]) -> Path:
    payload = {
        "operator_name": "pytest",
        "timestamp": "2026-04-27T00:00:00+08:00",
        "branch": BRANCH,
        "HEAD": HEAD,
        "config_path": str(config_path),
        "a1r_output_dir": config["a1r_output_dir"],
        "pressure_points_hpa": list(A2_ALLOWED_PRESSURE_POINTS_HPA),
        "port_manifest": {
            "pressure_controller": "COM31",
            "pressure_gauge": "COM30",
            "temperature_chamber": "COM27",
            "thermometer": "COM26",
            "gas_analyzers": ["COM35", "COM37", "COM41", "COM42"],
        },
        "explicit_acknowledgement": {
            "only_a2_co2_7_pressure_no_write": True,
            "co2_only": True,
            "skip0": True,
            "single_route": True,
            "single_temperature": True,
            "seven_pressure_points": True,
            "no_write": True,
            "no_id_write": True,
            "no_senco_write": True,
            "no_calibration_write": True,
            "no_chamber_sv_write": True,
            "no_chamber_set_temperature": True,
            "no_chamber_start": True,
            "no_chamber_stop": True,
            "no_mode_switch": True,
            "not_real_acceptance": True,
            "engineering_probe_only": True,
            "v1_fallback_required": True,
            "authorized_pressure_control_scope_acknowledged": True,
            "do_not_refresh_real_primary_latest": True,
            "a3_enabled": False,
            "h2o_enabled": False,
            "full_group_enabled": False,
            "multi_temperature_enabled": False,
            "real_primary_latest_refresh": False,
        },
    }
    path = tmp_path / "operator_confirmation.json"
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8-sig")
    return path


def _config_and_operator(tmp_path: Path) -> tuple[dict[str, Any], Path, Path]:
    config = _base_config(tmp_path)
    config_path = tmp_path / "config.json"
    config_path.write_text(json.dumps(config), encoding="utf-8")
    op_path = _operator_confirmation(tmp_path, config_path, config)
    return config, config_path, op_path


def _passing_executor(_config_path: str | Path) -> dict[str, Any]:
    point_results = []
    sample_rows = []
    pressure_rows = []
    route_rows = [
        {
            "timestamp": "2026-04-27T00:00:00+00:00",
            "action": "high_pressure_first_point_mode_enabled",
            "result": "ok",
            "point_index": 1,
            "target": {"pressure_hpa": 1100.0},
        },
        {
            "timestamp": "2026-04-27T00:00:01+00:00",
            "action": "set_vent",
            "target": {"vent_on": False},
            "result": "ok",
        },
        {
            "timestamp": "2026-04-27T00:00:02+00:00",
            "action": "seal_route",
            "result": "ok",
        },
    ]
    for index, pressure in enumerate(A2_ALLOWED_PRESSURE_POINTS_HPA, start=1):
        point_results.append(
            {
                "target_pressure_hpa": pressure,
                "pressure_point_index": index,
                "pressure_setpoint_command_sent": True,
                "pressure_setpoint_scope": "authorized_a2_pressure_control_scope",
                "vent_state_before_point": 0,
                "seal_state_before_point": "verified_closed",
                "pressure_gauge_hpa_before_ready": pressure,
                "pressure_gauge_hpa_before_sample": pressure + 0.1,
                "pressure_age_ms_before_sample": 250.0,
                "pressure_gauge_freshness_ok_before_sample": True,
                "pressure_ready_gate_result": "PASS",
                "pressure_ready_gate_latency_ms": 120.0,
                "heartbeat_ready_before_sample": True,
                "heartbeat_gap_observed_ms": 1200.0,
                "heartbeat_emission_gap_ms": 20.0,
                "blocking_operation_duration_ms": 1180.0,
                "route_conditioning_ready_before_sample": True,
                "sample_count": 4,
                "valid_frame_count": 4,
                "frame_has_data": True,
                "frame_usable": True,
                "frame_status": "frames_seen",
                "analyzer_ids_seen": ["001", "029"],
                "point_completed": True,
                "point_final_decision": "PASS",
            }
        )
        pressure_rows.append({"event": "pressure_ready", "target_pressure_hpa": pressure, "point_index": index})
        for sample_index in range(4):
            sample_rows.append(
                {
                    "target_pressure_hpa": pressure,
                    "pressure_point_index": index,
                    "sample_index": sample_index + 1,
                    "device_id": "001",
                    "frame_usable": True,
                }
            )
    return {
        "execution_run_dir": "D:/fake_a2_execution",
        "point_results": point_results,
        "route_trace_rows": route_rows,
        "pressure_trace_rows": pressure_rows,
        "sample_rows": sample_rows,
    }


def test_a2_gate_requires_triple_unlock(tmp_path: Path) -> None:
    config, config_path, op_path = _config_and_operator(tmp_path)

    missing_cli = evaluate_a2_co2_7_pressure_no_write_gate(
        config,
        cli_allow=False,
        env={A2_ENV_VAR: "1"},
        operator_confirmation_path=op_path,
        branch=BRANCH,
        head=HEAD,
        config_path=str(config_path),
    )
    assert missing_cli.approved is False
    assert "missing_cli_flag_allow_v2_a2_co2_7_pressure_no_write_real_com" in missing_cli.reasons

    missing_env = evaluate_a2_co2_7_pressure_no_write_gate(
        config,
        cli_allow=True,
        env={},
        operator_confirmation_path=op_path,
        branch=BRANCH,
        head=HEAD,
        config_path=str(config_path),
    )
    assert missing_env.approved is False
    assert "missing_env_gas_cal_v2_a2_co2_7_pressure_no_write_real_com" in missing_env.reasons

    approved = evaluate_a2_co2_7_pressure_no_write_gate(
        config,
        cli_allow=True,
        env={A2_ENV_VAR: "1"},
        operator_confirmation_path=op_path,
        branch=BRANCH,
        head=HEAD,
        config_path=str(config_path),
    )
    assert approved.approved is True


def test_a2_probe_writes_required_artifacts_and_passes_with_complete_points(tmp_path: Path) -> None:
    config, config_path, op_path = _config_and_operator(tmp_path)

    summary = write_a2_co2_7_pressure_no_write_probe_artifacts(
        config,
        output_dir=tmp_path / "a2",
        config_path=config_path,
        operator_confirmation_path=op_path,
        branch=BRANCH,
        head=HEAD,
        cli_allow=True,
        env={A2_ENV_VAR: "1"},
        execute_probe=True,
        executor=_passing_executor,
    )

    assert summary["final_decision"] == "PASS"
    assert summary["a2_pressure_sweep_executed"] is True
    assert summary["pressure_points_expected"] == 7
    assert summary["pressure_points_completed"] == 7
    assert summary["points_completed"] == 7
    assert summary["sample_count_total"] == 28
    assert summary["all_pressure_points_have_fresh_pressure_before_sample"] is True
    assert summary["all_pressure_points_have_samples"] is True
    assert summary["a2_1_heartbeat_gap_accounting_fix_present"] is True
    assert summary["a3_allowed"] is False
    assert summary["attempted_write_count"] == 0
    assert summary["any_write_command_sent"] is False
    assert summary["identity_write_command_sent"] is False
    assert summary["mode_switch_command_sent"] is False
    assert summary["senco_write_command_sent"] is False
    assert summary["calibration_write_command_sent"] is False
    assert summary["chamber_set_temperature_command_sent"] is False
    assert summary["chamber_start_command_sent"] is False
    assert summary["chamber_stop_command_sent"] is False
    assert summary["real_primary_latest_refresh"] is False

    for path in summary["artifact_paths"].values():
        assert Path(path).exists()
    points = json.loads(Path(summary["artifact_paths"]["point_results"]).read_text(encoding="utf-8"))["points"]
    assert len(points) == 7
    assert all(point["pressure_ready_gate_result"] == "PASS" for point in points)
    assert all(point["pressure_gauge_freshness_ok_before_sample"] is True for point in points)
    assert all(point["heartbeat_gap_observed_ms"] == 1200.0 for point in points)
    assert all(point["heartbeat_emission_gap_ms"] == 20.0 for point in points)
    assert all(point["blocking_operation_duration_ms"] == 1180.0 for point in points)


def test_a2_probe_fails_closed_on_stale_pressure_and_downstream_execution(tmp_path: Path) -> None:
    config, config_path, op_path = _config_and_operator(tmp_path)

    def bad_executor(_config_path: str | Path) -> dict[str, Any]:
        point_results = _passing_executor(_config_path)["point_results"]
        point_results[0]["pressure_gauge_freshness_ok_before_sample"] = False
        point_results[0]["point_completed"] = False
        point_results[0]["point_final_decision"] = "FAIL_CLOSED"
        return {"point_results": point_results, "route_trace_rows": [], "pressure_trace_rows": [], "sample_rows": []}

    summary = write_a2_co2_7_pressure_no_write_probe_artifacts(
        config,
        output_dir=tmp_path / "a2_bad",
        config_path=config_path,
        operator_confirmation_path=op_path,
        branch=BRANCH,
        head=HEAD,
        cli_allow=True,
        env={A2_ENV_VAR: "1"},
        execute_probe=True,
        executor=bad_executor,
    )

    assert summary["final_decision"] == "FAIL_CLOSED"
    assert summary["a3_allowed"] is False
    assert "downstream_point_executed_after_fail_closed_point" in summary["rejection_reasons"]
    assert summary["all_pressure_points_have_fresh_pressure_before_sample"] is False


def test_a2_gate_rejects_pressure_point_drift(tmp_path: Path) -> None:
    config, config_path, op_path = _config_and_operator(tmp_path)
    config["pressure_points_hpa"] = list(A2_ALLOWED_PRESSURE_POINTS_HPA) + [400.0]
    config["a2_co2_7_pressure_no_write_probe"]["pressure_points_hpa"] = list(A2_ALLOWED_PRESSURE_POINTS_HPA) + [400.0]

    admission = evaluate_a2_co2_7_pressure_no_write_gate(
        config,
        cli_allow=True,
        env={A2_ENV_VAR: "1"},
        operator_confirmation_path=op_path,
        branch=BRANCH,
        head=HEAD,
        config_path=str(config_path),
    )

    assert admission.approved is False
    assert "config_pressure_points_not_exact_a2_set" in admission.reasons
