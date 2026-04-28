from __future__ import annotations

import json
from pathlib import Path
import subprocess
from typing import Any, Callable, Mapping

import pytest

from gas_calibrator.v2.core.run001_a2_co2_only_7_pressure_no_write_probe import (
    A2_ALLOWED_PRESSURE_POINTS_HPA,
    A2_ENV_VAR,
    evaluate_a2_co2_7_pressure_no_write_gate,
    write_a2_co2_7_pressure_no_write_probe_artifacts,
)
from gas_calibrator.v2.core.run001_a1_dry_run import load_point_rows
from gas_calibrator.v2.core.run001_a2_no_write import evaluate_run001_a2_readiness
from gas_calibrator.v2.core.run001_r1_conditioning_only_probe import load_json_mapping
from gas_calibrator.v2.scripts.run001_a2_co2_only_7_pressure_no_write_probe import main as a2_probe_main


BRANCH = "codex/run001-a1-no-write-dry-run"
HEAD = "134a68188e9a3c53d720efccdb1b6de3cc0703e2"
REPO_ROOT = Path(__file__).resolve().parents[4]


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
        "run001_a2": {
            "scope": "run001_a2_co2_no_write_pressure_sweep",
            "mode": "real_machine_dry_run",
            "no_write": True,
            "co2_only": True,
            "skip_co2_ppm": [0],
            "single_route": True,
            "single_temperature_group": True,
            "default_cutover_to_v2": False,
            "disable_v1": False,
            "full_h2o_co2_group": False,
            "authorized_pressure_points_hpa": list(A2_ALLOWED_PRESSURE_POINTS_HPA),
            "allow_write_coefficients": False,
            "allow_write_zero": False,
            "allow_write_span": False,
            "allow_write_calibration_parameters": False,
        },
        "workflow": {
            "route_mode": "co2_only",
            "selected_temps_c": [20.0],
            "skip_co2_ppm": [0],
        },
        "devices": {
            "dewpoint_meter": {"enabled": False},
            "humidity_generator": {"enabled": False},
        },
        "paths": {
            "output_dir": str(tmp_path / "downstream_output"),
        },
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


def test_load_json_mapping_accepts_utf8_json_without_bom(tmp_path: Path) -> None:
    path = tmp_path / "config.json"
    path.write_text(json.dumps({"ok": True}), encoding="utf-8")

    assert load_json_mapping(path) == {"ok": True}


def test_load_json_mapping_accepts_utf8_bom_json(tmp_path: Path) -> None:
    path = tmp_path / "config_bom.json"
    path.write_text(json.dumps({"ok": True}), encoding="utf-8-sig")

    assert load_json_mapping(path) == {"ok": True}


def test_load_json_mapping_rejects_non_object_json(tmp_path: Path) -> None:
    path = tmp_path / "list.json"
    path.write_text(json.dumps([{"ok": True}]), encoding="utf-8-sig")

    with pytest.raises(ValueError, match="JSON payload must be an object"):
        load_json_mapping(path)


def test_load_json_mapping_does_not_swallow_syntax_errors(tmp_path: Path) -> None:
    path = tmp_path / "broken.json"
    path.write_text('{"missing": ', encoding="utf-8-sig")

    with pytest.raises(json.JSONDecodeError):
        load_json_mapping(path)


def test_a2_script_accepts_bom_config_before_admission_gate(tmp_path: Path) -> None:
    config = _base_config(tmp_path)
    config_path = tmp_path / "config_bom.json"
    config_path.write_text(json.dumps(config), encoding="utf-8-sig")
    op_path = _operator_confirmation(tmp_path, config_path, config)
    output_dir = tmp_path / "a2_bom_config"

    result = a2_probe_main(
        [
            "--config",
            str(config_path),
            "--operator-confirmation",
            str(op_path),
            "--output-dir",
            str(output_dir),
            "--branch",
            BRANCH,
            "--head",
            HEAD,
        ]
    )

    assert result == 2
    summary = json.loads((output_dir / "summary.json").read_text(encoding="utf-8"))
    assert summary["final_decision"] == "FAIL_CLOSED"
    assert summary["operator_confirmation_valid"] is True
    assert summary["a2_pressure_sweep_executed"] is False
    assert summary["execution_error"] == ""
    assert not any("invalid_operator_confirmation_json" in reason for reason in summary["rejection_reasons"])
    assert not any("JSONDecodeError" in reason for reason in summary["rejection_reasons"])


def test_a2_wrapper_generates_downstream_co2_points_json_and_passes_points_gate(tmp_path: Path) -> None:
    config, config_path, op_path = _config_and_operator(tmp_path)

    def executor(aligned_config_path: str | Path) -> dict[str, Any]:
        aligned_cfg = load_json_mapping(aligned_config_path)
        rows = load_point_rows(aligned_config_path, aligned_cfg)
        readiness = evaluate_run001_a2_readiness(aligned_cfg, config_path=aligned_config_path, point_rows=rows)
        assert len(rows) == 7
        assert [row["route"] for row in rows] == ["co2"] * 7
        assert [row["pressure_hpa"] for row in rows] == list(A2_ALLOWED_PRESSURE_POINTS_HPA)
        assert [row["target_pressure_hpa"] for row in rows] == list(A2_ALLOWED_PRESSURE_POINTS_HPA)
        assert "a2_point_pressure_list_mismatch" not in readiness["hard_stop_reasons"]
        assert "a2_points_not_co2_only" not in readiness["hard_stop_reasons"]
        payload = _passing_executor(aligned_config_path)
        payload["downstream_readiness"] = readiness
        return payload

    summary = write_a2_co2_7_pressure_no_write_probe_artifacts(
        config,
        output_dir=tmp_path / "a2_generated_points",
        config_path=config_path,
        operator_confirmation_path=op_path,
        branch=BRANCH,
        head=HEAD,
        cli_allow=True,
        env={A2_ENV_VAR: "1"},
        execute_probe=True,
        executor=executor,
    )

    points_path = Path(summary["generated_points_json_path"])
    assert summary["final_decision"] == "PASS"
    assert summary["points_config_alignment_ready"] is True
    assert summary["downstream_points_generated"] is True
    assert points_path.name == "a2_3_v1_aligned_points.json"
    assert points_path.exists()
    assert len(json.loads(points_path.read_text(encoding="utf-8"))) == 7
    assert summary["generated_points_json_sha256"]
    assert summary["generated_points_json_sha256"] == summary["effective_points_json_sha256"]
    assert summary["downstream_points_row_count"] == 7
    assert summary["downstream_point_routes"] == ["co2"] * 7
    assert summary["downstream_point_pressures_hpa"] == list(A2_ALLOWED_PRESSURE_POINTS_HPA)
    assert summary["downstream_points_gate_reasons"] == []
    assert summary["a3_allowed"] is False
    assert summary["attempted_write_count"] == 0
    assert summary["any_write_command_sent"] is False
    assert summary["chamber_write_register_command_sent"] is False


def _write_existing_points(path: Path, rows: list[Mapping[str, Any]]) -> None:
    path.write_text(json.dumps([dict(row) for row in rows], ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _valid_existing_rows() -> list[dict[str, Any]]:
    return [
        {
            "index": index,
            "route": "co2",
            "pressure_hpa": pressure,
            "target_pressure_hpa": pressure,
            "co2_ppm": 100.0,
            "temperature_c": 20.0,
            "temp_chamber_c": 20.0,
        }
        for index, pressure in enumerate(A2_ALLOWED_PRESSURE_POINTS_HPA, start=1)
    ]


@pytest.mark.parametrize(
    ("mutator", "expected_reason"),
    [
        (lambda rows: rows[0].pop("route"), "a2_point_route_missing"),
        (
            lambda rows: (rows[0].pop("pressure_hpa"), rows[0].pop("target_pressure_hpa")),
            "a2_point_pressure_missing",
        ),
        (lambda rows: rows.reverse(), "a2_point_pressure_list_mismatch"),
    ],
)
def test_a2_wrapper_fails_closed_for_invalid_existing_points_json(
    tmp_path: Path,
    mutator: Callable[[list[dict[str, Any]]], object],
    expected_reason: str,
) -> None:
    config, config_path, op_path = _config_and_operator(tmp_path)
    rows = _valid_existing_rows()
    mutator(rows)
    points_path = tmp_path / "existing_points.json"
    _write_existing_points(points_path, rows)
    config["paths"]["points_excel"] = str(points_path)
    config_path.write_text(json.dumps(config), encoding="utf-8")

    def executor(_aligned_config_path: str | Path) -> dict[str, Any]:
        raise AssertionError("invalid points must fail before downstream execution")

    summary = write_a2_co2_7_pressure_no_write_probe_artifacts(
        config,
        output_dir=tmp_path / "a2_invalid_points",
        config_path=config_path,
        operator_confirmation_path=op_path,
        branch=BRANCH,
        head=HEAD,
        cli_allow=True,
        env={A2_ENV_VAR: "1"},
        execute_probe=True,
        executor=executor,
    )

    assert summary["final_decision"] == "FAIL_CLOSED"
    assert summary["points_config_alignment_ready"] is False
    assert any(expected_reason in reason for reason in summary["rejection_reasons"])
    assert summary["attempted_write_count"] == 0
    assert summary["any_write_command_sent"] is False
    assert summary["a3_allowed"] is False


def test_a2_wrapper_fails_closed_for_non_json_points_suffix(tmp_path: Path) -> None:
    config, config_path, op_path = _config_and_operator(tmp_path)
    points_path = tmp_path / "existing_points.txt"
    _write_existing_points(points_path, _valid_existing_rows())
    config["paths"]["points_excel"] = str(points_path)
    config_path.write_text(json.dumps(config), encoding="utf-8")

    summary = write_a2_co2_7_pressure_no_write_probe_artifacts(
        config,
        output_dir=tmp_path / "a2_bad_suffix",
        config_path=config_path,
        operator_confirmation_path=op_path,
        branch=BRANCH,
        head=HEAD,
        cli_allow=True,
        env={A2_ENV_VAR: "1"},
        execute_probe=True,
        executor=lambda _path: _passing_executor(_path),
    )

    assert summary["final_decision"] == "FAIL_CLOSED"
    assert summary["points_config_alignment_ready"] is False
    assert any("a2_points_json_suffix_not_json" in reason for reason in summary["rejection_reasons"])


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
    assert summary["operator_confirmation_valid"] is True
    assert summary["a3_allowed"] is False
    assert summary["attempted_write_count"] == 0
    assert summary["any_write_command_sent"] is False
    assert summary["identity_write_command_sent"] is False
    assert summary["mode_switch_command_sent"] is False
    assert summary["senco_write_command_sent"] is False
    assert summary["calibration_write_command_sent"] is False
    assert summary["chamber_write_register_command_sent"] is False
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


def test_a2_probe_summary_records_a2_3_v1_aligned_pressure_source_fields(tmp_path: Path) -> None:
    config, config_path, op_path = _config_and_operator(tmp_path)
    config["workflow"] = {"pressure": {"a2_conditioning_pressure_source": "v1_aligned"}}

    def executor(config_path: str | Path) -> dict[str, Any]:
        payload = _passing_executor(config_path)
        payload["route_trace_rows"].append(
            {
                "timestamp": "2026-04-27T00:00:03+00:00",
                "action": "co2_route_conditioning_pressure_sample",
                "point_index": 1,
                "actual": {
                    "pressure_source_selected": "digital_pressure_gauge_p3",
                    "pressure_source_selection_reason": "continuous_stale_fallback_to_p3_fast",
                    "critical_window_uses_latest_frame": False,
                    "critical_window_uses_query": True,
                    "p3_fast_fallback_attempted": True,
                    "p3_fast_fallback_result": "success",
                    "normal_p3_fallback_attempted": False,
                    "normal_p3_fallback_result": "",
                    "digital_gauge_stream_stale": False,
                    "continuous_restart_attempted": True,
                    "continuous_restart_result": "recovered",
                },
                "result": "ok",
            }
        )
        return payload

    summary = write_a2_co2_7_pressure_no_write_probe_artifacts(
        config,
        output_dir=tmp_path / "a2_v1_aligned",
        config_path=config_path,
        operator_confirmation_path=op_path,
        branch=BRANCH,
        head=HEAD,
        cli_allow=True,
        env={A2_ENV_VAR: "1"},
        execute_probe=True,
        executor=executor,
    )

    assert summary["a2_3_v1_pressure_gauge_read_policy_present"] is True
    assert summary["a2_3_pressure_source_strategy"] == "v1_aligned"
    assert summary["pressure_source_selected"] == "digital_pressure_gauge_p3"
    assert summary["pressure_source_selection_reason"] == "continuous_stale_fallback_to_p3_fast"
    assert summary["critical_window_uses_latest_frame"] is False
    assert summary["critical_window_uses_query"] is True
    assert summary["p3_fast_fallback_attempted"] is True
    assert summary["p3_fast_fallback_result"] == "success"
    assert summary["normal_p3_fallback_attempted"] is False
    assert summary["digital_gauge_stream_stale"] is False
    assert summary["continuous_restart_attempted"] is True
    assert summary["continuous_restart_result"] == "recovered"
    assert summary["a3_allowed"] is False


@pytest.mark.parametrize(
    ("field", "reason"),
    [
        ("a3_enabled", "config_a3_enabled_not_disabled"),
        ("h2o_enabled", "config_h2o_enabled_not_disabled"),
        ("full_group_enabled", "config_full_group_enabled_not_disabled"),
        ("multi_temperature_enabled", "config_multi_temperature_enabled_not_disabled"),
    ],
)
def test_a2_gate_still_rejects_stage_expansion_after_bom_safe_loader(
    tmp_path: Path,
    field: str,
    reason: str,
) -> None:
    config, config_path, op_path = _config_and_operator(tmp_path)
    config[field] = True
    config["a2_co2_7_pressure_no_write_probe"][field] = True

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
    assert reason in admission.reasons


def test_run_app_py_untouched_by_a2_bom_safe_loader_change() -> None:
    result = subprocess.run(
        ["git", "diff", "--quiet", "--", "run_app.py"],
        cwd=REPO_ROOT,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stdout + result.stderr


def test_v1_untouched_by_a2_bom_safe_loader_change() -> None:
    result = subprocess.run(
        ["git", "diff", "--quiet", "--", "src/gas_calibrator/v1"],
        cwd=REPO_ROOT,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stdout + result.stderr


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
