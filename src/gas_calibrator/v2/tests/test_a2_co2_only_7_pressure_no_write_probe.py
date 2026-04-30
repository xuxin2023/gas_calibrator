from __future__ import annotations

import json
from pathlib import Path
import subprocess
from types import SimpleNamespace
from typing import Any, Callable, Mapping

import pytest

from gas_calibrator.v2.core.run001_a2_co2_only_7_pressure_no_write_probe import (
    A2_ALLOWED_PRESSURE_POINTS_HPA,
    A2_ENV_VAR,
    A2_INTERRUPTED_FAIL_CLOSED_REASON,
    evaluate_a2_co2_7_pressure_no_write_gate,
    _artifact_completeness,
    _jsonl_dump,
    _load_jsonl,
    write_a2_co2_7_pressure_no_write_probe_artifacts,
)
from gas_calibrator.v2.core.services import trace_size_guard as trace_guard_module
from gas_calibrator.v2.core.services.status_service import StatusService
from gas_calibrator.v2.core.services.trace_size_guard import MAX_TRACE_EVENT_JSON_BYTES
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
        "pressure_source": "v1_aligned",
        "temperature_stabilization_wait_skipped": True,
        "temperature_gate_mode": "current_pv_engineering_probe",
        "temperature_not_part_of_acceptance": True,
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
            "pressure": {"a2_conditioning_pressure_source": "v1_aligned"},
            "stability": {
                "temperature": {
                    "skip_temperature_stabilization_wait": True,
                    "temperature_stabilization_wait_skipped": True,
                    "temperature_gate_mode": "current_pv_engineering_probe",
                    "temperature_not_part_of_acceptance": True,
                },
            },
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
            "pressure_source": "v1_aligned",
            "temperature_stabilization_wait_skipped": True,
            "temperature_gate_mode": "current_pv_engineering_probe",
            "temperature_not_part_of_acceptance": True,
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
        "pressure_source": "v1_aligned",
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
            "skip_temperature_stabilization_wait": True,
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


def _expected_a2_artifact_paths(run_dir: Path) -> dict[str, str]:
    return {
        "probe_admission_record": str(run_dir / "probe_admission_record.json"),
        "summary": str(run_dir / "summary.json"),
        "a2_pressure_sweep_trace": str(run_dir / "a2_pressure_sweep_trace.jsonl"),
        "route_trace": str(run_dir / "route_trace.jsonl"),
        "pressure_trace": str(run_dir / "pressure_trace.jsonl"),
        "pressure_ready_trace": str(run_dir / "pressure_ready_trace.jsonl"),
        "heartbeat_trace": str(run_dir / "heartbeat_trace.jsonl"),
        "analyzer_sampling_rows": str(run_dir / "analyzer_sampling_rows.jsonl"),
        "point_results": str(run_dir / "point_results.json"),
        "point_results_csv": str(run_dir / "point_results.csv"),
        "safety_assertions": str(run_dir / "safety_assertions.json"),
        "operator_confirmation_record": str(run_dir / "operator_confirmation_record.json"),
        "process_exit_record": str(run_dir / "process_exit_record.json"),
    }


def _large_route_state() -> dict[str, Any]:
    return {
        "current_route": "co2",
        "route_state": {f"large_key_{index}": "x" * 4096 for index in range(100)},
        "pressure_samples": [
            {"pressure_hpa": 1000.0 + index * 0.01, "raw_response": "p" * 2048}
            for index in range(600)
        ],
        "vent_ticks": [
            {"phase": "conditioning_hold", "tick": index, "raw": "v" * 1024}
            for index in range(600)
        ],
        "diagnostic_deferred_events": [
            {"source": "diagnostic_blocked_vent_scheduler", "payload": "d" * 1024}
            for _index in range(200)
        ],
    }


def test_status_service_route_trace_compacts_large_route_state_before_write(tmp_path: Path) -> None:
    context = SimpleNamespace(
        result_store=SimpleNamespace(run_dir=str(tmp_path)),
        session=SimpleNamespace(run_id="run-route-trace-guard"),
        data_writer=SimpleNamespace(write_log=lambda *_args, **_kwargs: None),
    )
    run_state = SimpleNamespace(artifacts=SimpleNamespace(output_files=[]))
    service = StatusService(context, run_state, host=SimpleNamespace(route_context=None))

    service.record_route_trace(
        action="pressure_control_ready_gate",
        route="co2",
        point_index=1,
        actual=_large_route_state(),
    )

    raw_line = (tmp_path / "route_trace.jsonl").read_bytes().strip()
    record = json.loads(raw_line.decode("utf-8"))
    assert len(raw_line) <= MAX_TRACE_EVENT_JSON_BYTES
    assert record["trace_guard_applied_to_route_trace"] is True
    assert record["trace_event_truncated"] is True
    assert record["trace_event_original_size_bytes"] > record["trace_event_truncated_size_bytes"]


def test_a2_route_trace_dump_compacts_large_route_state(tmp_path: Path) -> None:
    path = tmp_path / "route_trace.jsonl"
    stats = _jsonl_dump(
        path,
        [
            {
                "action": "pressure_control_ready_gate",
                "point_index": 1,
                "actual": _large_route_state(),
            }
        ],
        trace_name="route_trace",
    )

    raw_line = path.read_bytes().strip()
    record = json.loads(raw_line.decode("utf-8"))
    assert len(raw_line) <= MAX_TRACE_EVENT_JSON_BYTES
    assert stats["trace_event_truncated_count"] == 1
    assert record["trace_guard_applied_to_route_trace"] is True
    assert record["trace_event_truncated"] is True


def test_a2_pressure_trace_dump_summarizes_large_pressure_samples(tmp_path: Path) -> None:
    path = tmp_path / "pressure_trace.jsonl"
    pressure_samples = [
        {"pressure_hpa": 900.0 + index * 0.1, "raw_response": "sample" * 256}
        for index in range(800)
    ]

    _jsonl_dump(
        path,
        [
            {
                "target_pressure_hpa": 900.0,
                "actual": {
                    "pressure_samples": pressure_samples,
                    "selected_pressure_source": "digital_pressure_gauge",
                },
            }
        ],
        trace_name="pressure_trace",
    )

    raw_line = path.read_bytes().strip()
    record = json.loads(raw_line.decode("utf-8"))
    assert len(raw_line) <= MAX_TRACE_EVENT_JSON_BYTES
    assert record["trace_guard_applied_to_pressure_trace"] is True
    assert record["trace_event_truncated"] is True
    assert record["actual"]["pressure_samples"]["_truncated"] is True
    assert record["actual"]["pressure_samples"]["_length"] == len(pressure_samples)


def test_a2_wrapper_load_jsonl_skips_large_line_with_warning(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(trace_guard_module, "MAX_TRACE_LINE_INLINE_LOAD_BYTES", 32)
    path = tmp_path / "route_trace.jsonl"
    path.write_text(json.dumps({"action": "tick", "payload": "x" * 128}) + "\n", encoding="utf-8")

    rows = _load_jsonl(path, trace_name="route_trace")

    assert rows == [
        {
            "event": "trace_large_line_skipped",
            "path": str(path),
            "line_bytes": len(path.read_bytes()),
            "max_line_bytes": 32,
            "reason": "trace_line_too_large_for_inline_load",
            "trace_large_line_skipped": True,
            "trace_large_line_warning_count": 1,
            "trace_streaming_read_used": True,
            "trace_inline_load_blocked": False,
            "trace_file_size_guard_triggered": False,
            "trace_guard_schema_version": "v2.trace_size_guard.1",
            "trace_guard_applied_to_route_trace": True,
        }
    ]


def test_a2_wrapper_load_jsonl_blocks_inline_load_for_large_trace(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(trace_guard_module, "MAX_TRACE_FILE_INLINE_LOAD_BYTES", 8)
    path = tmp_path / "route_trace.jsonl"
    path.write_text(json.dumps({"action": "tick"}) + "\n", encoding="utf-8")

    rows = _load_jsonl(path, trace_name="route_trace")

    assert rows[0]["event"] == "trace_inline_load_blocked"
    assert rows[0]["trace_file_size_guard_triggered"] is True
    assert rows[0]["trace_inline_load_blocked"] is True
    assert rows[0]["trace_streaming_read_used"] is True
    assert rows[0]["trace_guard_applied_to_route_trace"] is True


def test_a2_wrapper_load_jsonl_uses_streaming_read(monkeypatch, tmp_path: Path) -> None:
    path = tmp_path / "route_trace.jsonl"
    path.write_text(json.dumps({"action": "tick"}) + "\n", encoding="utf-8")

    def fail_read_text(*_args, **_kwargs):
        raise AssertionError("read_text must not be used for JSONL traces")

    monkeypatch.setattr(Path, "read_text", fail_read_text)

    rows = _load_jsonl(path, trace_name="route_trace")

    assert rows[0]["action"] == "tick"
    assert rows[0]["trace_streaming_read_used"] is True


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
                "pressure_source_selected": "digital_pressure_gauge_p3",
                "pressure_source_selection_reason": "v1_aligned_pressure_gate",
                "selected_pressure_source": "digital_pressure_gauge_p3",
                "selected_pressure_sample_age_s": 0.05,
                "selected_pressure_sample_is_stale": False,
                "selected_pressure_parse_ok": True,
                "selected_pressure_freshness_ok": True,
                "pressure_freshness_decision_source": "digital_pressure_gauge_p3",
                "selected_pressure_fail_closed_reason": "",
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
    assert summary["artifact_completeness_pass"] is True
    assert summary["required_artifacts_missing"] == []
    assert summary["no_write_assertion_status"] == "pass"
    assert summary["safety_assertions_complete"] is True

    for path in summary["artifact_paths"].values():
        assert Path(path).exists()
    points = json.loads(Path(summary["artifact_paths"]["point_results"]).read_text(encoding="utf-8"))["points"]
    assert len(points) == 7
    assert all(point["pressure_ready_gate_result"] == "PASS" for point in points)
    assert all(point["pressure_gauge_freshness_ok_before_sample"] is True for point in points)
    assert all(point["heartbeat_gap_observed_ms"] == 1200.0 for point in points)
    assert all(point["heartbeat_emission_gap_ms"] == 20.0 for point in points)
    assert all(point["blocking_operation_duration_ms"] == 1180.0 for point in points)


def test_a2_interrupted_before_real_com_writes_fail_closed_guard_artifacts(tmp_path: Path) -> None:
    config, config_path, op_path = _config_and_operator(tmp_path)

    def executor(_aligned_config_path: str | Path) -> dict[str, Any]:
        exc = KeyboardInterrupt()
        exc.partial_execution = {
            "interruption_audit": {
                "real_com_opened": False,
                "any_device_command_sent": False,
                "any_write_command_sent": False,
                "device_command_audit_complete": True,
                "safe_stop_triggered": False,
            }
        }
        raise exc

    output_dir = tmp_path / "a2_interrupted_pre_com"
    summary = write_a2_co2_7_pressure_no_write_probe_artifacts(
        config,
        output_dir=output_dir,
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
    assert summary["fail_closed_reason"] == A2_INTERRUPTED_FAIL_CLOSED_REASON
    assert summary["interrupted_execution"] is True
    assert summary["interruption_source"] == "KeyboardInterrupt"
    assert summary["real_com_opened"] is False
    assert summary["any_device_command_sent"] is False
    assert summary["any_write_command_sent"] is False
    assert summary["safe_stop_triggered"] is False
    assert summary["no_write_assertion_status"] == "pass_pre_com"
    assert summary["safety_assertions_complete"] is False
    assert summary["a3_allowed"] is False
    assert summary["evidence_source"] == "real_probe_a2_12r_co2_7_pressure_no_write"
    for name in (
        "probe_admission_record.json",
        "operator_confirmation_record.json",
        "summary.json",
        "safety_assertions.json",
        "process_exit_record.json",
    ):
        assert (output_dir / name).exists()


def test_a2_interrupted_after_real_com_keeps_no_write_unknown_without_audit(tmp_path: Path) -> None:
    config, config_path, op_path = _config_and_operator(tmp_path)

    def executor(_aligned_config_path: str | Path) -> dict[str, Any]:
        exc = RuntimeError("downstream executor interrupted")
        exc.partial_execution = {
            "interruption_audit": {
                "real_com_opened": True,
                "any_device_command_sent": True,
                "device_command_audit_complete": False,
                "safe_stop_triggered": "unknown",
            }
        }
        raise exc

    output_dir = tmp_path / "a2_interrupted_after_com"
    summary = write_a2_co2_7_pressure_no_write_probe_artifacts(
        config,
        output_dir=output_dir,
        config_path=config_path,
        operator_confirmation_path=op_path,
        branch=BRANCH,
        head=HEAD,
        cli_allow=True,
        env={A2_ENV_VAR: "1"},
        execute_probe=True,
        executor=executor,
    )
    safety = json.loads((output_dir / "safety_assertions.json").read_text(encoding="utf-8"))

    assert summary["final_decision"] == "FAIL_CLOSED"
    assert summary["fail_closed_reason"] == A2_INTERRUPTED_FAIL_CLOSED_REASON
    assert summary["interrupted_execution"] is True
    assert summary["interruption_source"] == "RuntimeError"
    assert summary["real_com_opened"] is True
    assert summary["any_device_command_sent"] is True
    assert summary["any_write_command_sent"] == "unknown"
    assert summary["no_write_assertion_status"] == "unknown"
    assert summary["must_not_claim_no_write_pass"] is True
    assert summary["device_command_audit_complete"] is False
    assert summary["safety_assertions_complete"] is False
    assert safety["no_write"] is False
    assert safety["no_write_assertion_status"] == "unknown"
    assert safety["must_not_claim_no_write_pass"] is True
    assert summary["a3_allowed"] is False


def test_a2_artifact_completeness_gate_fails_for_previous_interrupted_dir_shape(tmp_path: Path) -> None:
    output_dir = tmp_path / "previous_interrupted"
    output_dir.mkdir()
    (output_dir / "operator_confirmation_input.json").write_text("{}", encoding="utf-8")
    (output_dir / "a2_3_v1_aligned_downstream_config.json").write_text("{}", encoding="utf-8")

    completeness = _artifact_completeness(_expected_a2_artifact_paths(output_dir))

    assert completeness["artifact_completeness_pass"] is False
    assert completeness["artifact_completeness_fail_reason"] == "required_artifacts_missing"
    assert "summary.json" in completeness["required_artifacts_missing"]
    assert "safety_assertions.json" in completeness["required_artifacts_missing"]
    assert "operator_confirmation_record.json" in completeness["required_artifacts_missing"]
    assert "route_trace.jsonl" in completeness["required_artifacts_missing"]
    assert "point_results.json" in completeness["required_artifacts_missing"]


def test_a2_wrapper_recovers_partial_output_dir_with_fail_closed_summary(tmp_path: Path) -> None:
    config, config_path, op_path = _config_and_operator(tmp_path)
    output_dir = tmp_path / "previous_interrupted_recovery"
    output_dir.mkdir()
    (output_dir / "operator_confirmation_input.json").write_text("{}", encoding="utf-8")
    (output_dir / "a2_3_v1_aligned_downstream_config.json").write_text("{}", encoding="utf-8")

    summary = write_a2_co2_7_pressure_no_write_probe_artifacts(
        config,
        output_dir=output_dir,
        config_path=config_path,
        operator_confirmation_path=op_path,
        branch=BRANCH,
        head=HEAD,
        cli_allow=True,
        env={A2_ENV_VAR: "1"},
        execute_probe=False,
    )

    assert summary["final_decision"] == "FAIL_CLOSED"
    assert "execute_probe_not_requested" in summary["rejection_reasons"]
    assert summary["a3_allowed"] is False
    assert summary["evidence_source"] == "real_probe_a2_12r_co2_7_pressure_no_write"
    assert (output_dir / "summary.json").exists()
    assert (output_dir / "safety_assertions.json").exists()
    assert (output_dir / "operator_confirmation_record.json").exists()
    assert (output_dir / "probe_admission_record.json").exists()
    assert (output_dir / "process_exit_record.json").exists()


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
                    "continuous_stream_stale": True,
                    "digital_gauge_stream_stale": True,
                    "selected_pressure_source": "digital_pressure_gauge_p3",
                    "selected_pressure_sample_age_s": 0.05,
                    "selected_pressure_sample_is_stale": False,
                    "selected_pressure_parse_ok": True,
                    "selected_pressure_freshness_ok": True,
                    "pressure_freshness_decision_source": "digital_pressure_gauge_p3",
                    "selected_pressure_fail_closed_reason": "",
                    "selected_pressure_source_for_conditioning_monitor": "digital_pressure_gauge_continuous",
                    "selected_pressure_source_for_pressure_gate": "digital_pressure_gauge_p3",
                    "a2_conditioning_pressure_source_strategy": "v1_aligned",
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
    assert summary["probe_identity"] == "A2.12R CO2-only seven-pressure no-write engineering probe"
    assert summary["probe_version"] == "A2.12R"
    assert summary["evidence_source"] == "real_probe_a2_12r_co2_7_pressure_no_write"
    assert summary["legacy_evidence_source"] == "real_probe_a2_12_co2_7_pressure_no_write"
    assert summary["legacy_evidence_sources"] == [
        "real_probe_a2_12_co2_7_pressure_no_write",
        "real_probe_a2_10_co2_7_pressure_no_write",
    ]
    safety_assertions = json.loads((tmp_path / "a2_v1_aligned" / "safety_assertions.json").read_text(encoding="utf-8"))
    operator_record = json.loads(
        (tmp_path / "a2_v1_aligned" / "operator_confirmation_record.json").read_text(encoding="utf-8")
    )
    assert safety_assertions["evidence_source"] == "real_probe_a2_12r_co2_7_pressure_no_write"
    assert safety_assertions["probe_version"] == "A2.12R"
    assert operator_record["evidence_source"] == "real_probe_a2_12r_co2_7_pressure_no_write"
    assert operator_record["probe_identity"] == "A2.12R CO2-only seven-pressure no-write engineering probe"
    assert summary["a2_3_pressure_source_strategy"] == "v1_aligned"
    assert summary["a2_4_v1_pressure_gauge_read_policy_present"] is True
    assert summary["a2_4_pressure_source_strategy"] == "v1_aligned"
    assert summary["temperature_stabilization_wait_skipped"] is True
    assert summary["temperature_gate_mode"] == "current_pv_engineering_probe"
    assert summary["temperature_not_part_of_acceptance"] is True
    assert summary["pressure_source_selected"] == "digital_pressure_gauge_p3"
    assert summary["pressure_source_selection_reason"] == "continuous_stale_fallback_to_p3_fast"
    assert summary["critical_window_uses_latest_frame"] is False
    assert summary["critical_window_uses_query"] is True
    assert summary["p3_fast_fallback_attempted"] is True
    assert summary["p3_fast_fallback_result"] == "success"
    assert summary["normal_p3_fallback_attempted"] is False
    assert summary["continuous_stream_stale"] is True
    assert summary["digital_gauge_stream_stale"] is True
    assert summary["selected_pressure_source"] == "digital_pressure_gauge_p3"
    assert summary["selected_pressure_sample_age_s"] == 0.05
    assert summary["selected_pressure_sample_is_stale"] is False
    assert summary["selected_pressure_parse_ok"] is True
    assert summary["selected_pressure_freshness_ok"] is True
    assert summary["pressure_freshness_decision_source"] == "digital_pressure_gauge_p3"
    assert summary["selected_pressure_fail_closed_reason"] == ""
    assert summary["selected_pressure_source_for_conditioning_monitor"] == "digital_pressure_gauge_continuous"
    assert summary["selected_pressure_source_for_pressure_gate"] == "digital_pressure_gauge_p3"
    assert summary["conditioning_monitor_pressure_source_allowed"] is True
    assert summary["pressure_gate_reached"] is True
    assert summary["pressure_gate_source_required"] == "v1_aligned"
    assert summary["pressure_gate_source_observed"] == "digital_pressure_gauge_p3"
    assert summary["pressure_gate_source_alignment_ready"] is True
    assert summary["pressure_gate_source_alignment_reasons"] == []
    assert summary["a2_conditioning_pressure_source_strategy"] == "v1_aligned"
    assert summary["continuous_restart_attempted"] is True
    assert summary["continuous_restart_result"] == "recovered"
    assert summary["a3_allowed"] is False


def test_a2_probe_summary_records_optional_temperature_context_policy(tmp_path: Path) -> None:
    config, config_path, op_path = _config_and_operator(tmp_path)

    def executor(config_path: str | Path) -> dict[str, Any]:
        payload = _passing_executor(config_path)
        payload["service_summary"] = {
            "stats": {
                "temperature_chamber_required_for_a2": False,
                "temperature_chamber_init_attempted": True,
                "temperature_chamber_init_ok": False,
                "temperature_chamber_init_failed": True,
                "temperature_chamber_init_failure_blocks_a2": False,
                "temperature_chamber_optional_in_skip_temp_wait": True,
                "temperature_context_available": False,
                "temperature_context_source": "unavailable",
                "temperature_context_unavailable_reason": "temperature_chamber_init_failed",
                "temperature_chamber_readonly_probe_attempted": True,
                "temperature_chamber_readonly_probe_result": "unavailable",
                "temperature_not_part_of_acceptance": True,
                "temperature_stabilization_wait_skipped": True,
                "temperature_gate_mode": "current_pv_engineering_probe",
                "critical_devices_required": [
                    "gas_analyzer_0",
                    "pressure_controller",
                    "pressure_meter",
                    "relay_a",
                    "relay_b",
                ],
                "critical_devices_failed": [],
                "optional_context_devices": ["temperature_chamber"],
                "optional_context_devices_failed": ["temperature_chamber"],
                "critical_device_init_failure_blocks_probe": False,
                "optional_context_failure_blocks_probe": False,
            }
        }
        return payload

    summary = write_a2_co2_7_pressure_no_write_probe_artifacts(
        config,
        output_dir=tmp_path / "a2_optional_temperature_context",
        config_path=config_path,
        operator_confirmation_path=op_path,
        branch=BRANCH,
        head=HEAD,
        cli_allow=True,
        env={A2_ENV_VAR: "1"},
        execute_probe=True,
        executor=executor,
    )

    assert summary["temperature_chamber_required_for_a2"] is False
    assert summary["temperature_chamber_init_attempted"] is True
    assert summary["temperature_chamber_init_ok"] is False
    assert summary["temperature_chamber_init_failed"] is True
    assert summary["temperature_chamber_init_failure_blocks_a2"] is False
    assert summary["temperature_chamber_optional_in_skip_temp_wait"] is True
    assert summary["temperature_context_available"] is False
    assert summary["temperature_context_unavailable_reason"] == "temperature_chamber_init_failed"
    assert summary["temperature_chamber_readonly_probe_attempted"] is True
    assert summary["temperature_chamber_readonly_probe_result"] == "unavailable"
    assert summary["optional_context_devices_failed"] == ["temperature_chamber"]
    assert summary["critical_devices_failed"] == []
    assert summary["critical_device_init_failure_blocks_probe"] is False
    assert summary["optional_context_failure_blocks_probe"] is False
    assert summary["chamber_set_temperature_command_sent"] is False
    assert summary["chamber_start_command_sent"] is False
    assert summary["chamber_stop_command_sent"] is False


def test_a2_probe_summary_records_a2_14_command_diagnostics(tmp_path: Path) -> None:
    config, config_path, op_path = _config_and_operator(tmp_path)
    config["devices"].update(
        {
            "pressure_controller": {
                "enabled": True,
                "port": "COM31",
                "baud": 9600,
                "timeout": 1.0,
                "line_ending": "LF",
                "protocol_profile": "vendor_unknown_ascii",
            },
            "pressure_gauge": {
                "enabled": True,
                "port": "COM30",
                "baud": 9600,
                "timeout": 1.0,
                "response_timeout_s": 2.2,
                "dest_id": "01",
            },
            "relay": {"enabled": True, "port": "COM28", "baud": 38400, "addr": 1},
            "relay_8": {"enabled": True, "port": "COM29", "baud": 38400, "addr": 1},
        }
    )
    config["valves"] = {
        "relay_map": {
            "7": {"device": "relay", "channel": 15},
            "8": {"device": "relay_8", "channel": 8},
        }
    }
    config_path.write_text(json.dumps(config), encoding="utf-8")

    def executor(config_path: str | Path) -> dict[str, Any]:
        payload = _passing_executor(config_path)
        payload["route_trace_rows"] = [
            {
                "action": "set_output",
                "target": {"enabled": False},
                "result": "fail",
                "message": "PACE_COMMAND_ERROR(command=:OUTP:STAT 0, error=)",
            },
            {
                "action": "set_vent",
                "target": {"vent_on": True},
                "result": "fail",
                "message": "PACE_COMMAND_ERROR(command=:OUTP:STAT 0, error=)",
            },
        ]
        return payload

    summary = write_a2_co2_7_pressure_no_write_probe_artifacts(
        config,
        output_dir=tmp_path / "a2_14_command_diagnostics",
        config_path=config_path,
        operator_confirmation_path=op_path,
        branch=BRANCH,
        head=HEAD,
        cli_allow=True,
        env={A2_ENV_VAR: "1"},
        execute_probe=True,
        executor=executor,
    )

    assert summary["pressure_controller_driver_profile"] == "gas_calibrator.devices.pace5000.Pace5000"
    assert summary["pressure_controller_configured_port"] == "COM31"
    assert summary["pressure_controller_serial_settings"]["baud"] == 9600
    assert summary["pressure_controller_protocol_profile"] == "vendor_unknown_ascii"
    assert summary["pressure_controller_command_terminator"] == "LF"
    assert summary["pressure_controller_identity_query_command"] == "*IDN?"
    assert summary["pressure_controller_identity_query_result"] == "unsupported_identity_query_not_offline_decision"
    assert summary["pressure_controller_identity_query_error"] == "unsupported_identity_query"
    assert summary["pressure_controller_output_command"] == ":OUTP:STAT 0"
    assert summary["pressure_controller_output_command_result"] == "fail"
    assert "PACE_COMMAND_ERROR" in summary["pressure_controller_output_command_error"]
    assert summary["pressure_controller_vent_command"] == ":SOUR:PRES:LEV:IMM:AMPL:VENT 1"
    assert summary["pressure_controller_vent_command_result"] == "fail"
    assert "PACE_COMMAND_ERROR" in summary["pressure_controller_vent_command_error"]
    assert summary["pressure_controller_pace_command_error_raw"].startswith("PACE_COMMAND_ERROR")
    assert summary["v1_v2_pressure_controller_command_alignment"].startswith("profile_mismatch")

    assert summary["pressure_meter_alias_resolved"] is True
    assert summary["pressure_meter_selected_device_key"] == "pressure_gauge"
    assert summary["pressure_meter_port"] == "COM30"
    assert summary["pressure_meter_dest_id"] == "01"
    assert summary["pressure_meter_protocol_profile"] == "paroscientific_p3_readonly"
    assert summary["pressure_meter_first_read_attempted"] is True
    assert summary["pressure_meter_first_read_result"] == "PASS"
    assert summary["pressure_meter_parse_ok"] is True
    assert summary["v1_v2_pressure_meter_read_alignment"] == "aligned_paroscientific_p3_with_pressure_gauge_alias"

    assert summary["relay_a_configured_port"] == "COM28"
    assert summary["relay_b_configured_port"] == "COM29"
    assert summary["relay_driver_profile"] == "gas_calibrator.devices.relay.RelayController"
    assert summary["relay_channel_mapping"]["7"]["channel"] == 15
    assert summary["relay_output_command_allowed_in_probe"] is False
    assert summary["relay_output_command_sent"] is False


def test_a2_probe_skip_temp_policy_from_operator_ack_when_raw_config_unaligned(tmp_path: Path) -> None:
    config, config_path, op_path = _config_and_operator(tmp_path)
    config.pop("temperature_stabilization_wait_skipped", None)
    config.pop("temperature_gate_mode", None)
    config.pop("temperature_not_part_of_acceptance", None)
    config["a2_co2_7_pressure_no_write_probe"].pop("temperature_stabilization_wait_skipped", None)
    config["a2_co2_7_pressure_no_write_probe"].pop("temperature_gate_mode", None)
    config["a2_co2_7_pressure_no_write_probe"].pop("temperature_not_part_of_acceptance", None)
    config["workflow"]["stability"]["temperature"] = {}
    config_path.write_text(json.dumps(config), encoding="utf-8")

    summary = write_a2_co2_7_pressure_no_write_probe_artifacts(
        config,
        output_dir=tmp_path / "a2_skip_temp_operator_ack",
        config_path=config_path,
        operator_confirmation_path=op_path,
        branch=BRANCH,
        head=HEAD,
        cli_allow=True,
        env={A2_ENV_VAR: "1"},
        execute_probe=True,
        executor=_passing_executor,
    )

    assert summary["temperature_stabilization_wait_skipped"] is True
    assert summary["temperature_gate_mode"] == "current_pv_engineering_probe"
    assert summary["temperature_not_part_of_acceptance"] is True
    assert summary["temperature_chamber_required_for_a2"] is False
    assert summary["temperature_chamber_optional_in_skip_temp_wait"] is True


def test_a2_probe_pressure_source_strategy_uses_downstream_aligned_config_without_runtime_metric(
    tmp_path: Path,
) -> None:
    config, config_path, op_path = _config_and_operator(tmp_path)
    config["workflow"]["pressure"]["a2_conditioning_pressure_source"] = "continuous"
    config_path.write_text(json.dumps(config), encoding="utf-8")

    def executor(config_path: str | Path) -> dict[str, Any]:
        payload = _passing_executor(config_path)
        payload["service_summary"] = {
            "a2_conditioning_pressure_source_strategy": "continuous",
        }
        return payload

    summary = write_a2_co2_7_pressure_no_write_probe_artifacts(
        config,
        output_dir=tmp_path / "a2_downstream_aligned_strategy",
        config_path=config_path,
        operator_confirmation_path=op_path,
        branch=BRANCH,
        head=HEAD,
        cli_allow=True,
        env={A2_ENV_VAR: "1"},
        execute_probe=True,
        executor=executor,
    )

    assert summary["raw_config_pressure_source_strategy"] == "continuous"
    assert summary["downstream_aligned_pressure_source_strategy"] == "v1_aligned"
    assert summary["runtime_pressure_source_strategy_observed"] == ""
    assert summary["a2_conditioning_pressure_source_strategy"] == "v1_aligned"
    assert summary["a2_conditioning_pressure_source_strategy_source"] == "downstream_aligned_config"
    assert summary["pressure_source_strategy_aggregation_mismatch"] is False
    assert summary["pressure_source_strategy_aggregation_mismatch_reason"] == ""


def test_a2_probe_summary_records_positive_preseal_abort_and_emergency_relief(
    tmp_path: Path,
) -> None:
    config, config_path, op_path = _config_and_operator(tmp_path)

    def executor(config_path: str | Path) -> dict[str, Any]:
        payload = _passing_executor(config_path)
        payload["point_results"] = []
        payload["pressure_trace_rows"] = []
        payload["sample_rows"] = []
        payload["route_trace_rows"] = [
            {
                "timestamp": "2026-04-29T03:43:00+00:00",
                "action": "positive_preseal_abort",
                "result": "fail",
                "actual": {
                    "positive_preseal_phase_started": True,
                    "positive_preseal_phase_started_at": "2026-04-29T03:43:00+00:00",
                    "positive_preseal_pressure_guard_checked": True,
                    "positive_preseal_pressure_hpa": 1280.989,
                    "positive_preseal_pressure_source": "digital_pressure_gauge_p3",
                    "positive_preseal_pressure_sample_age_s": 0.05,
                    "positive_preseal_abort_pressure_hpa": 1150.0,
                    "positive_preseal_pressure_overlimit": True,
                    "positive_preseal_abort_reason": "preseal_abort_pressure_exceeded",
                    "positive_preseal_setpoint_sent": False,
                    "positive_preseal_setpoint_hpa": None,
                    "positive_preseal_output_enabled": False,
                    "positive_preseal_route_open": True,
                    "positive_preseal_seal_command_sent": False,
                    "positive_preseal_pressure_setpoint_command_sent": False,
                    "positive_preseal_sample_started": False,
                    "positive_preseal_overlimit_fail_closed": True,
                    "pressure_hpa": 1280.989,
                    "abort_reason": "preseal_abort_pressure_exceeded",
                },
            },
            {
                "timestamp": "2026-04-29T03:43:01+00:00",
                "action": "set_vent",
                "target": {"vent_on": True},
                "result": "ok",
                "actual": {
                    "emergency_abort_relief_vent_required": True,
                    "emergency_abort_relief_vent_allowed": True,
                    "emergency_abort_relief_vent_blocked_reason": "",
                    "emergency_abort_relief_vent_command_sent": True,
                    "emergency_abort_relief_vent_phase": "positive_preseal_pressurization",
                    "emergency_abort_relief_reason": "positive_preseal_abort_pressure_exceeded",
                    "emergency_abort_relief_pressure_hpa": 1280.989,
                    "emergency_abort_relief_route_open": True,
                    "emergency_abort_relief_seal_command_sent": False,
                    "emergency_abort_relief_pressure_setpoint_command_sent": False,
                    "emergency_abort_relief_sample_started": False,
                    "emergency_abort_relief_may_mix_air": False,
                    "normal_maintenance_vent_blocked_after_flush_phase": False,
                    "cleanup_vent_classification": "emergency_abort_relief",
                    "safe_stop_pressure_relief_result": "command_sent",
                },
            },
        ]
        return payload

    summary = write_a2_co2_7_pressure_no_write_probe_artifacts(
        config,
        output_dir=tmp_path / "a2_positive_preseal_abort",
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
    assert "a2_positive_preseal_pressure_overlimit" in summary["rejection_reasons"]
    assert "a2_route_conditioning_vent_blocked_after_flush_phase" not in summary["rejection_reasons"]
    assert summary["positive_preseal_pressure_guard_checked"] is True
    assert summary["positive_preseal_pressure_hpa"] == 1280.989
    assert summary["positive_preseal_abort_pressure_hpa"] == 1150.0
    assert summary["positive_preseal_pressure_overlimit"] is True
    assert summary["positive_preseal_setpoint_sent"] is False
    assert summary["positive_preseal_seal_command_sent"] is False
    assert summary["positive_preseal_sample_started"] is False
    assert summary["emergency_abort_relief_vent_allowed"] is True
    assert summary["emergency_abort_relief_vent_command_sent"] is True
    assert summary["cleanup_vent_classification"] == "emergency_abort_relief"
    assert summary["normal_maintenance_vent_blocked_after_flush_phase"] is False
    assert summary["safe_stop_pressure_relief_result"] == "command_sent"
    assert summary["attempted_write_count"] == 0
    assert summary["any_write_command_sent"] is False


def test_a2_probe_does_not_reject_pressure_gate_source_before_gate_reached(tmp_path: Path) -> None:
    config, config_path, op_path = _config_and_operator(tmp_path)

    def executor(config_path: str | Path) -> dict[str, Any]:
        payload = _passing_executor(config_path)
        payload["point_results"] = []
        payload["pressure_trace_rows"] = []
        payload["sample_rows"] = []
        payload["route_trace_rows"] = [
            {
                "timestamp": "2026-04-27T00:00:03+00:00",
                "action": "co2_route_conditioning_vent_heartbeat_gap",
                "actual": {
                    "route_conditioning_phase": "route_conditioning_flush_phase",
                    "route_conditioning_vent_gap_exceeded": True,
                    "route_conditioning_vent_gap_exceeded_source": "defer_path_no_reschedule",
                    "terminal_gap_source": "defer_path_no_reschedule",
                    "terminal_vent_write_age_ms_at_gap_gate": 4917.728,
                    "max_vent_pulse_write_gap_ms_including_terminal_gap": 4917.728,
                    "max_vent_pulse_gap_limit_ms": 1000.0,
                    "selected_pressure_source_for_conditioning_monitor": "digital_pressure_gauge_continuous",
                    "selected_pressure_source_for_pressure_gate": "",
                    "pressure_monitor_nonblocking": True,
                    "conditioning_monitor_pressure_deferred": True,
                    "fail_closed_reason": "route_conditioning_vent_gap_exceeded",
                },
                "result": "fail",
            }
        ]
        payload["service_summary"] = {
            "route_conditioning_vent_gap_exceeded": True,
            "route_conditioning_vent_gap_exceeded_source": "defer_path_no_reschedule",
            "terminal_gap_source": "defer_path_no_reschedule",
            "terminal_vent_write_age_ms_at_gap_gate": 4917.728,
            "max_vent_pulse_write_gap_ms_including_terminal_gap": 4917.728,
            "max_vent_pulse_gap_limit_ms": 1000.0,
            "selected_pressure_source_for_conditioning_monitor": "digital_pressure_gauge_continuous",
            "selected_pressure_source_for_pressure_gate": "",
            "pressure_monitor_nonblocking": True,
            "conditioning_monitor_pressure_deferred": True,
        }
        return payload

    summary = write_a2_co2_7_pressure_no_write_probe_artifacts(
        config,
        output_dir=tmp_path / "a2_gate_not_reached",
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
    assert "a2_route_conditioning_vent_gap_exceeded" in summary["rejection_reasons"]
    assert "a2_4_pressure_source_not_v1_aligned" not in summary["rejection_reasons"]
    assert "a2_pressure_gate_source_not_v1_aligned" not in summary["rejection_reasons"]
    assert summary["conditioning_monitor_pressure_source_allowed"] is True
    assert summary["pressure_gate_reached"] is False
    assert summary["pressure_gate_not_reached_reason"] == "route_conditioning_fail_closed"
    assert summary["pressure_gate_source_required"] == "v1_aligned"
    assert summary["pressure_gate_source_observed"] == ""
    assert summary["pressure_gate_source_alignment_ready"] is False
    assert summary["pressure_gate_source_alignment_reasons"] == []


def test_a2_probe_propagates_route_trace_atmosphere_and_transient_interruption(tmp_path: Path) -> None:
    config, config_path, op_path = _config_and_operator(tmp_path)

    def executor(config_path: str | Path) -> dict[str, Any]:
        payload = _passing_executor(config_path)
        payload["point_results"] = []
        payload["pressure_trace_rows"] = []
        payload["sample_rows"] = []
        payload["route_trace_rows"] = [
            {
                "timestamp": "2026-04-29T13:02:17+00:00",
                "action": "set_vent",
                "target": {"vent_on": True},
                "message": "Vent atmosphere before CO2 route conditioning",
                "actual": {
                    "pressure_hpa": 1014.361,
                    "atmosphere_ready": True,
                    "vent_status_raw": 1,
                    "vent_status_interpreted": "in_progress",
                },
                "result": "ok",
            },
            {
                "timestamp": "2026-04-29T13:06:22+00:00",
                "action": "co2_route_conditioning_vent_heartbeat_gap",
                "actual": {
                    "route_conditioning_vent_gap_exceeded": True,
                    "route_conditioning_vent_gap_exceeded_source": "defer_path_no_reschedule",
                    "terminal_gap_source": "defer_path_no_reschedule",
                    "terminal_vent_write_age_ms_at_gap_gate": 2113.793,
                    "max_vent_pulse_write_gap_ms_including_terminal_gap": 2113.793,
                    "max_vent_pulse_gap_limit_ms": 2000.0,
                    "route_conditioning_hard_abort_exceeded": False,
                    "route_conditioning_pressure_overlimit": False,
                    "route_conditioning_peak_pressure_hpa": 1195.639,
                    "defer_path_no_reschedule": True,
                    "terminal_gap_after_defer": True,
                    "pressure_monitor_nonblocking": True,
                    "fail_closed_reason": "route_conditioning_vent_gap_exceeded",
                },
                "result": "fail",
            },
        ]
        payload["service_summary"] = {
            "route_conditioning_vent_gap_exceeded": True,
            "route_conditioning_vent_gap_exceeded_source": "defer_path_no_reschedule",
            "terminal_gap_source": "defer_path_no_reschedule",
            "terminal_vent_write_age_ms_at_gap_gate": 2113.793,
            "max_vent_pulse_write_gap_ms_including_terminal_gap": 2113.793,
            "max_vent_pulse_gap_limit_ms": 2000.0,
            "route_conditioning_hard_abort_pressure_hpa": 1250.0,
            "route_conditioning_hard_abort_exceeded": False,
            "route_conditioning_pressure_overlimit": False,
            "route_conditioning_peak_pressure_hpa": 1195.639,
        }
        return payload

    summary = write_a2_co2_7_pressure_no_write_probe_artifacts(
        config,
        output_dir=tmp_path / "a2_atmosphere_fallback",
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
    assert summary["evidence_source"] == "real_probe_a2_12r_co2_7_pressure_no_write"
    assert summary["measured_atmospheric_pressure_hpa"] == 1014.361
    assert summary["measured_atmospheric_pressure_source"] == "route_trace_pre_route_vent_pressure"
    assert summary["route_conditioning_pressure_before_route_open_hpa"] == 1014.361
    assert summary["route_open_transient_recovery_target_hpa"] == 1014.361
    assert summary["route_open_transient_evaluation_state"] == "interrupted_by_vent_gap"
    assert summary["route_open_transient_interrupted_by_vent_gap"] is True
    assert summary["route_open_transient_rejection_reason"] == (
        "vent_gap_exceeded_before_recovery_evaluation"
    )
    assert summary["route_open_transient_interrupted_reason"] == (
        "vent_gap_exceeded_before_recovery_evaluation"
    )
    assert summary["route_open_transient_summary_source"] == "route_conditioning_vent_gap"


def test_a2_probe_treats_defer_latency_warning_as_non_vent_gap_when_actual_gap_ok(tmp_path: Path) -> None:
    config, config_path, op_path = _config_and_operator(tmp_path)

    def executor(config_path: str | Path) -> dict[str, Any]:
        payload = _passing_executor(config_path)
        legacy_false_gap = {
            "route_conditioning_vent_gap_exceeded": True,
            "route_conditioning_vent_gap_exceeded_source": "defer_path_no_reschedule",
            "terminal_gap_source": "defer_path_no_reschedule",
            "defer_source": "pressure_monitor",
            "defer_operation": "selected_pressure_sample_stale",
            "defer_returned_to_vent_loop": False,
            "defer_to_next_vent_loop_ms": 505.139,
            "defer_path_no_reschedule": True,
            "defer_path_no_reschedule_reason": "defer_to_next_vent_loop_exceeded_200ms",
            "terminal_gap_after_defer": True,
            "terminal_gap_after_defer_ms": 505.139,
            "fast_vent_after_defer_sent": True,
            "fast_vent_after_defer_write_ms": 0.147,
            "max_vent_pulse_write_gap_ms_including_terminal_gap": 556.693,
            "max_vent_pulse_gap_limit_ms": 1000.0,
            "measured_atmospheric_pressure_hpa": 1012.46,
            "measured_atmospheric_pressure_source": "route_trace_pre_route_vent_pressure",
            "route_conditioning_pressure_before_route_open_hpa": 1012.46,
            "route_open_transient_recovery_target_hpa": 1012.46,
            "route_open_transient_evaluation_state": "interrupted_by_vent_gap",
            "route_open_transient_interrupted_by_vent_gap": True,
            "route_open_transient_rejection_reason": "vent_gap_exceeded_before_recovery_evaluation",
            "route_open_transient_interrupted_reason": "vent_gap_exceeded_before_recovery_evaluation",
            "route_open_transient_summary_source": "route_conditioning_vent_gap",
            "route_conditioning_hard_abort_pressure_hpa": 1250.0,
            "route_conditioning_hard_abort_exceeded": False,
            "route_conditioning_pressure_overlimit": False,
            "route_conditioning_peak_pressure_hpa": 1160.66,
            "pressure_rise_despite_valid_vent_scheduler": False,
            "sustained_pressure_rise_after_route_open": False,
        }
        payload["route_trace_rows"].insert(
            0,
            {
                "timestamp": "2026-04-30T00:17:43+00:00",
                "action": "set_vent",
                "actual": {"pressure_hpa": 1012.46, "atmosphere_ready": True},
                "result": "ok",
            },
        )
        payload["route_trace_rows"].append(
            {
                "timestamp": "2026-04-30T00:17:52+00:00",
                "action": "co2_route_conditioning_defer_no_reschedule",
                "actual": legacy_false_gap,
                "result": "fail",
            }
        )
        payload["service_summary"] = legacy_false_gap
        return payload

    summary = write_a2_co2_7_pressure_no_write_probe_artifacts(
        config,
        output_dir=tmp_path / "a2_defer_latency_warning",
        config_path=config_path,
        operator_confirmation_path=op_path,
        branch=BRANCH,
        head=HEAD,
        cli_allow=True,
        env={A2_ENV_VAR: "1"},
        execute_probe=True,
        executor=executor,
    )

    assert "a2_route_conditioning_vent_gap_exceeded" not in summary["rejection_reasons"]
    assert summary["route_conditioning_vent_gap_exceeded"] is False
    assert summary["defer_reschedule_latency_ms"] == 505.139
    assert summary["defer_reschedule_latency_budget_ms"] == 200.0
    assert summary["defer_reschedule_latency_exceeded"] is True
    assert summary["defer_reschedule_latency_warning"] is True
    assert summary["defer_reschedule_caused_vent_gap_exceeded"] is False
    assert summary["defer_returned_to_vent_loop"] is True
    assert summary["defer_reschedule_completed"] is True
    assert summary["defer_path_no_reschedule"] is False
    assert summary["terminal_gap_after_defer"] is False
    assert summary["vent_gap_exceeded_after_defer"] is False
    assert summary["vent_gap_after_defer_threshold_ms"] == 1000.0
    assert summary["max_vent_pulse_write_gap_ms_including_terminal_gap"] == 556.693
    assert summary["route_open_transient_evaluation_state"] == "continuing_after_defer_warning"
    assert summary["route_open_transient_interrupted_by_vent_gap"] is False
    assert summary["route_open_transient_rejection_reason"] == ""
    assert summary["route_open_transient_interrupted_reason"] == ""
    assert summary["route_open_transient_summary_source"] == "route_conditioning_defer_latency_warning"


def test_a2_probe_rejects_non_v1_aligned_source_only_after_pressure_gate_reached(tmp_path: Path) -> None:
    config, config_path, op_path = _config_and_operator(tmp_path)

    def executor(config_path: str | Path) -> dict[str, Any]:
        payload = _passing_executor(config_path)
        payload["service_summary"] = {
            "selected_pressure_source_for_conditioning_monitor": "digital_pressure_gauge_continuous",
            "selected_pressure_source_for_pressure_gate": "digital_pressure_gauge_continuous",
        }
        return payload

    summary = write_a2_co2_7_pressure_no_write_probe_artifacts(
        config,
        output_dir=tmp_path / "a2_bad_gate_source",
        config_path=config_path,
        operator_confirmation_path=op_path,
        branch=BRANCH,
        head=HEAD,
        cli_allow=True,
        env={A2_ENV_VAR: "1"},
        execute_probe=True,
        executor=executor,
    )

    assert summary["pressure_gate_reached"] is True
    assert summary["conditioning_monitor_pressure_source_allowed"] is True
    assert summary["pressure_gate_source_observed"] == "digital_pressure_gauge_continuous"
    assert summary["pressure_gate_source_alignment_ready"] is False
    assert summary["pressure_gate_source_alignment_reasons"] == ["pressure_gate_source_not_v1_aligned"]
    assert "a2_pressure_gate_source_not_v1_aligned" in summary["rejection_reasons"]
    assert "a2_4_pressure_source_not_v1_aligned" not in summary["rejection_reasons"]


def test_a2_probe_fails_closed_on_route_conditioning_vent_gap_gate(tmp_path: Path) -> None:
    config, config_path, op_path = _config_and_operator(tmp_path)

    def executor(config_path: str | Path) -> dict[str, Any]:
        payload = _passing_executor(config_path)
        payload["service_summary"] = {
            "route_conditioning_vent_maintenance_active": True,
            "vent_maintenance_started_at": "2026-04-27T00:00:00+00:00",
            "vent_maintenance_started_monotonic_s": 100.0,
            "route_open_to_first_vent_ms": 998.0,
            "route_open_to_first_pressure_read_ms": 1.0,
            "vent_pulse_count": 4,
            "vent_pulse_interval_ms": [500.0, 11438.41],
            "max_vent_pulse_gap_ms": 11438.41,
            "terminal_vent_write_age_ms_at_gap_gate": 2218.567,
            "max_vent_pulse_write_gap_ms_including_terminal_gap": 11438.41,
            "route_conditioning_vent_gap_exceeded_source": "defer_path_no_reschedule",
            "terminal_gap_source": "defer_path_no_reschedule",
            "terminal_gap_operation": "selected_pressure_sample_stale",
            "terminal_gap_duration_ms": 2218.567,
            "defer_returned_to_vent_loop": False,
            "defer_to_next_vent_loop_ms": 2218.567,
            "terminal_gap_after_defer": True,
            "terminal_gap_after_defer_ms": 2218.567,
            "defer_path_no_reschedule": True,
            "max_vent_pulse_gap_limit_ms": 1000.0,
            "vent_scheduler_tick_count": 20,
            "vent_scheduler_loop_gap_ms": [100.0, 100.0],
            "max_vent_scheduler_loop_gap_ms": 100.0,
            "pressure_drop_after_vent_hpa": [None, -5.884],
            "route_conditioning_pressure_before_route_open_hpa": 1009.0,
            "route_conditioning_pressure_after_route_open_hpa": 1010.0,
            "route_conditioning_peak_pressure_hpa": 1010.0,
            "route_conditioning_pressure_rise_rate_hpa_per_s": 0.5,
            "route_conditioning_pressure_overlimit": False,
            "route_conditioning_vent_gap_exceeded": True,
            "vent_pulse_blocked_after_flush_phase": False,
            "unsafe_vent_after_seal_or_pressure_control_command_sent": False,
            "vent_off_blocked_during_flush": True,
            "seal_blocked_during_flush": True,
            "pressure_setpoint_blocked_during_flush": True,
            "sample_blocked_during_flush": True,
        }
        return payload

    summary = write_a2_co2_7_pressure_no_write_probe_artifacts(
        config,
        output_dir=tmp_path / "a2_gap",
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
    assert "a2_route_conditioning_vent_gap_exceeded" in summary["rejection_reasons"]
    assert summary["route_conditioning_vent_maintenance_active"] is True
    assert summary["max_vent_pulse_gap_ms"] == 11438.41
    assert summary["terminal_vent_write_age_ms_at_gap_gate"] == 2218.567
    assert summary["max_vent_pulse_write_gap_ms_including_terminal_gap"] == 11438.41
    assert summary["route_conditioning_vent_gap_exceeded_source"] == "defer_path_no_reschedule"
    assert summary["terminal_gap_source"] == "defer_path_no_reschedule"
    assert summary["terminal_gap_operation"] == "selected_pressure_sample_stale"
    assert summary["terminal_gap_after_defer"] is True
    assert summary["defer_path_no_reschedule"] is True
    assert summary["max_vent_pulse_gap_limit_ms"] == 1000.0
    assert summary["vent_scheduler_tick_count"] == 20
    assert summary["route_open_to_first_vent_ms"] == 998.0
    assert summary["unsafe_vent_after_seal_or_pressure_control_command_sent"] is False
    assert summary["a3_allowed"] is False


@pytest.mark.parametrize(
    ("service_update", "expected_reason"),
    [
        (
            {
                "pre_route_vent_phase_started": True,
                "pre_route_fast_vent_required": True,
                "pre_route_fast_vent_sent": False,
                "pre_route_fast_vent_duration_ms": 800.0,
                "pre_route_fast_vent_timeout": True,
                "fast_vent_reassert_supported": True,
                "fast_vent_reassert_used": True,
                "route_conditioning_fast_vent_command_timeout": True,
                "route_conditioning_fast_vent_not_supported": False,
                "route_conditioning_diagnostic_blocked_vent_scheduler": False,
                "vent_command_write_duration_ms": 800.0,
                "vent_command_total_duration_ms": 800.0,
                "max_vent_command_total_duration_ms": 800.0,
            },
            "a2_route_conditioning_fast_vent_command_timeout",
        ),
        (
            {
                "pre_route_vent_phase_started": True,
                "pre_route_fast_vent_required": True,
                "pre_route_fast_vent_sent": False,
                "fast_vent_reassert_supported": False,
                "fast_vent_reassert_used": False,
                "route_conditioning_fast_vent_command_timeout": False,
                "route_conditioning_fast_vent_not_supported": True,
                "route_conditioning_diagnostic_blocked_vent_scheduler": False,
            },
            "a2_route_conditioning_fast_vent_not_supported",
        ),
        (
            {
                "fast_vent_reassert_supported": True,
                "fast_vent_reassert_used": True,
                "route_conditioning_fast_vent_command_timeout": False,
                "route_conditioning_fast_vent_not_supported": False,
                "route_conditioning_diagnostic_blocked_vent_scheduler": True,
                "diagnostic_duration_ms": 2400.0,
                "route_conditioning_vent_gap_exceeded_source": "pressure_monitor",
                "vent_scheduler_priority_mode": True,
                "vent_scheduler_checked_before_diagnostic": True,
                "diagnostic_budget_ms": 100.0,
                "diagnostic_budget_exceeded": True,
                "diagnostic_blocking_component": "pressure_monitor",
                "diagnostic_blocking_operation": "stream_snapshot",
                "diagnostic_blocking_duration_ms": 2400.0,
                "pressure_monitor_nonblocking": True,
                "pressure_monitor_budget_ms": 100.0,
                "pressure_monitor_duration_ms": 2400.0,
                "pressure_monitor_blocked_vent_scheduler": True,
                "conditioning_monitor_pressure_deferred": False,
                "trace_write_budget_ms": 50.0,
                "trace_write_blocked_vent_scheduler": False,
            },
            "a2_route_conditioning_diagnostic_blocked_vent_scheduler",
        ),
        (
            {
                "fast_vent_reassert_supported": True,
                "fast_vent_reassert_used": True,
                "route_open_transition_started": True,
                "route_open_command_write_duration_ms": 1200.0,
                "route_open_transition_blocked_vent_scheduler": True,
                "route_conditioning_vent_gap_exceeded_source": "route_open_transition",
                "terminal_vent_write_age_ms_at_gap_gate": 0.0,
                "max_vent_pulse_write_gap_ms_including_terminal_gap": 500.0,
            },
            "a2_route_open_transition_blocked_vent_scheduler",
        ),
    ],
)
def test_a2_probe_fails_closed_on_a2_5_fast_vent_gates(
    tmp_path: Path,
    service_update: dict[str, Any],
    expected_reason: str,
) -> None:
    config, config_path, op_path = _config_and_operator(tmp_path)

    def executor(config_path: str | Path) -> dict[str, Any]:
        payload = _passing_executor(config_path)
        payload["service_summary"] = {
            "route_conditioning_vent_maintenance_active": True,
            "vent_maintenance_started_at": "2026-04-27T00:00:00+00:00",
            "vent_maintenance_started_monotonic_s": 100.0,
            "vent_command_write_started_at": "2026-04-27T00:00:00+00:00",
            "vent_command_write_sent_at": "2026-04-27T00:00:00+00:00",
            "vent_command_write_completed_at": "2026-04-27T00:00:00.800000+00:00",
            "vent_command_wait_after_command_s": 0.0,
            "vent_command_capture_pressure_enabled": False,
            "vent_command_query_state_enabled": False,
            "vent_command_confirm_transition_enabled": False,
            "vent_command_blocking_phase": "fast_vent_write",
            "route_open_high_frequency_vent_phase_started": False,
            "route_open_to_first_vent_write_ms": None,
            "max_vent_pulse_write_gap_ms": None,
            "selected_pressure_source_for_conditioning_monitor": "digital_pressure_gauge_continuous",
            "selected_pressure_source_for_pressure_gate": "digital_pressure_gauge_p3",
            "a2_conditioning_pressure_source_strategy": "v1_aligned",
            **service_update,
        }
        return payload

    summary = write_a2_co2_7_pressure_no_write_probe_artifacts(
        config,
        output_dir=tmp_path / "a2_5_fast_vent_gate",
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
    assert expected_reason in summary["rejection_reasons"]
    assert summary["vent_command_wait_after_command_s"] == 0.0
    assert summary["vent_command_capture_pressure_enabled"] is False
    assert summary["vent_command_query_state_enabled"] is False
    assert summary["vent_command_confirm_transition_enabled"] is False
    assert summary["selected_pressure_source_for_pressure_gate"] == "digital_pressure_gauge_p3"
    assert summary["a2_conditioning_pressure_source_strategy"] == "v1_aligned"
    if expected_reason == "a2_route_conditioning_diagnostic_blocked_vent_scheduler":
        assert summary["route_conditioning_vent_gap_exceeded_source"] == "pressure_monitor"
        assert summary["diagnostic_blocking_component"] == "pressure_monitor"
        assert summary["diagnostic_blocking_operation"] == "stream_snapshot"
        assert summary["pressure_monitor_blocked_vent_scheduler"] is True
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
