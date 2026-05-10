from __future__ import annotations

import json
from pathlib import Path

from gas_calibrator.v2.core import run001_h2o_only_1_point_no_write_probe as probe


def _minimal_cfg() -> dict:
    return {
        "h2o_only": True,
        "single_route": True,
        "single_temperature_group": True,
        "no_write": True,
        "not_real_acceptance_evidence": True,
        "default_cutover_to_v2": False,
        "disable_v1": False,
        "v1_fallback_required": True,
        "skip0": True,
        "scope": "run001_h2o_1_point",
        "pressure_points_hpa": [1100, 1000, 900, 800, 700, 600, 500],
        "a3_enabled": False,
        "co2_enabled": False,
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
    }


def _approved_admission() -> probe.H2OAdmission:
    return probe.H2OAdmission(
        approved=True,
        reasons=(),
        evidence={**probe.H2O_EVIDENCE_MARKERS, "admission_approved": True},
        operator_confirmation={"operator_name": "fake"},
        operator_validation={"valid": True, "errors": []},
    )


def _read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _make_downstream_run(tmp_path: Path, *, final_decision: str = "PASS", before_sampling: bool = False) -> Path:
    run_dir = tmp_path / "downstream" / "run_20260510_012103"
    run_dir.mkdir(parents=True)
    failure_reason = "H2O route readiness failed" if final_decision != "PASS" else ""
    _write_json(
        run_dir / "summary.json",
        {
            "run_id": "run_20260510_012103",
            "final_decision": final_decision,
            "failure_reason": failure_reason,
            "a1_fail_reason": failure_reason,
            "service_status_error": failure_reason,
            "points_completed": 0 if before_sampling else 1,
            "sample_count": 0 if before_sampling else 1,
            "pressure_completed": False if before_sampling else True,
            "sample_completed": False if before_sampling else True,
            "no_write": True,
        },
    )
    (run_dir / "run.log").write_text("start\nroute readiness failed\nfinal safe stop\n", encoding="utf-8")
    (run_dir / "route_trace.jsonl").write_text(
        '{"action":"wait_route_ready","result":"timeout","message":"H2O route open/ready check; failure_step=set_h2o_path_open reason=relay_physical_mismatch"}\n',
        encoding="utf-8",
    )
    if not before_sampling:
        _write_json(run_dir / "results.json", {"points": []})
        (run_dir / "samples.csv").write_text("ts,value\n1,2\n", encoding="utf-8")
    (run_dir / "points.csv").write_text("point,result\n1,fail\n", encoding="utf-8")
    _write_json(run_dir / "route_pressure_sample_trace.json", {"samples": []})
    _write_json(
        run_dir / "no_write_guard.json",
        {
            "no_write": True,
            "attempted_write_count": 0,
            "blocked_write_events": [],
            "identity_write_command_sent": False,
            "persistent_write_command_sent": False,
        },
    )
    return run_dir


def _patch_common(monkeypatch, downstream_run_dir: Path, *, final_decision: str = "PASS") -> None:
    monkeypatch.setattr(probe, "evaluate_h2o_1_point_no_write_gate", lambda *args, **kwargs: _approved_admission())
    monkeypatch.setattr(
        probe,
        "prepare_h2o_downstream_points_config",
        lambda raw_cfg, *, config_path, output_dir: (Path(output_dir) / "fake_downstream_config.json", {"points_config_alignment_ready": True}),
    )

    def fake_execute(config_path):
        return {
            "execution_run_dir": str(downstream_run_dir),
            "underlying_config_path": str(config_path),
            "service_summary": _read_json(downstream_run_dir / "summary.json"),
            "no_write_guard": _read_json(downstream_run_dir / "no_write_guard.json"),
            "route_trace_rows": [json.loads(line) for line in (downstream_run_dir / "route_trace.jsonl").read_text(encoding="utf-8").splitlines() if line.strip()],
            "run_log_tail": (downstream_run_dir / "run.log").read_text(encoding="utf-8")[-20000:],
        }

    monkeypatch.setattr(probe, "execute_h2o_single_point_probe", fake_execute)


def _run_probe(tmp_path: Path, monkeypatch, downstream_run_dir: Path, *, final_decision: str = "PASS") -> tuple[dict, dict, Path]:
    _patch_common(monkeypatch, downstream_run_dir, final_decision=final_decision)
    handoff = tmp_path / "handoff"
    summary = probe.write_h2o_1_point_no_write_probe_artifacts(
        _minimal_cfg(),
        output_dir=handoff,
        config_path=tmp_path / "config.json",
        operator_confirmation_path=tmp_path / "operator.json",
        branch="codex/v2-golden-recovery-cdb82111",
        head="fake-head",
        cli_allow=True,
        env={probe.H2O_ENV_VAR: probe.H2O_ENV_VALUE},
        execute_probe=True,
        run_app_py_untouched=True,
    )
    process_exit = _read_json(handoff / "process_exit_record.json")
    return summary, process_exit, handoff


def test_summary_and_process_exit_record_downstream_run_dir(tmp_path, monkeypatch):
    downstream = _make_downstream_run(tmp_path)
    summary, process_exit, _ = _run_probe(tmp_path, monkeypatch, downstream)

    assert summary["downstream_execution_run_dir"] == str(downstream)
    assert summary["execution_run_dir"] == str(downstream)
    assert process_exit["downstream_execution_run_dir"] == str(downstream)
    assert process_exit["execution_run_dir"] == str(downstream)


def test_records_downstream_run_log_path_and_tail(tmp_path, monkeypatch):
    downstream = _make_downstream_run(tmp_path)
    summary, process_exit, _ = _run_probe(tmp_path, monkeypatch, downstream)

    assert summary["downstream_run_log_path"] == str(downstream / "run.log")
    assert "route readiness failed" in summary["downstream_run_log_tail"]
    assert process_exit["downstream_run_log_path"] == str(downstream / "run.log")
    assert "final safe stop" in process_exit["downstream_run_log_tail"]


def test_artifact_map_links_downstream_route_trace(tmp_path, monkeypatch):
    downstream = _make_downstream_run(tmp_path)
    summary, _, _ = _run_probe(tmp_path, monkeypatch, downstream)

    route_trace = summary["downstream_artifact_map"]["route_trace"]
    assert route_trace["source_path"] == str(downstream / "route_trace.jsonl")
    assert route_trace["exists"] is True
    assert route_trace["status"] == "linked"


def test_results_and_points_csv_are_mapped_to_point_results(tmp_path, monkeypatch):
    downstream = _make_downstream_run(tmp_path)
    summary, _, _ = _run_probe(tmp_path, monkeypatch, downstream)

    artifact_map = summary["downstream_artifact_map"]
    assert artifact_map["point_results_json"]["source_path"] == str(downstream / "results.json")
    assert artifact_map["point_results_csv"]["source_path"] == str(downstream / "points.csv")


def test_handoff_missing_route_trace_satisfied_by_downstream_link(tmp_path, monkeypatch):
    downstream = _make_downstream_run(tmp_path)
    summary, process_exit, handoff = _run_probe(tmp_path, monkeypatch, downstream)

    assert not (handoff / "route_trace.jsonl").exists()
    assert "route_trace" in summary["required_artifact_keys_present"]
    assert "route_trace" not in summary["required_artifact_keys_missing"]
    assert summary["artifact_completeness_fail_reason"] == ""
    assert "route_trace" in process_exit["downstream_linked_artifact_keys_present"]


def test_missing_heartbeat_and_analyzer_jsonl_do_not_create_empty_files(tmp_path, monkeypatch):
    downstream = _make_downstream_run(tmp_path)
    summary, _, handoff = _run_probe(tmp_path, monkeypatch, downstream)

    assert not (handoff / "heartbeat_trace.jsonl").exists()
    assert not (handoff / "analyzer_sampling_rows.jsonl").exists()
    assert summary["downstream_artifact_map"]["heartbeat_trace_like"]["status"] == "not_available_or_not_generated"
    assert summary["downstream_artifact_map"]["heartbeat_trace_like"]["hard_required"] is False
    assert summary["downstream_artifact_map"]["analyzer_sampling_rows_like"]["source_path"] == str(downstream / "samples.csv")


def test_downstream_business_failure_preserves_diagnostics(tmp_path, monkeypatch):
    downstream = _make_downstream_run(tmp_path, final_decision="FAIL")
    summary, process_exit, _ = _run_probe(tmp_path, monkeypatch, downstream, final_decision="FAIL")

    assert summary["final_decision"] == "FAIL_CLOSED"
    assert "downstream_business_failed" in summary["rejection_reasons"]
    assert summary["downstream_service_summary"]["failure_reason"] == "H2O route readiness failed"
    assert summary["downstream_failure_reason"] == "H2O route readiness failed"
    assert process_exit["fail_closed_reason"] == "downstream_business_failed"
    assert process_exit["downstream_run_log_path"] == str(downstream / "run.log")


def test_process_exit_record_contains_downstream_diagnostics_and_empty_exception_fields(tmp_path, monkeypatch):
    downstream = _make_downstream_run(tmp_path)
    _, process_exit, _ = _run_probe(tmp_path, monkeypatch, downstream)

    assert process_exit["downstream_execution_started"] is True
    assert process_exit["downstream_execution_completed"] is True
    assert process_exit["downstream_service_summary"]["run_id"] == "run_20260510_012103"
    assert process_exit["downstream_artifact_map"]["downstream_summary"]["exists"] is True
    assert process_exit["downstream_exception_class"] == ""
    assert process_exit["downstream_exception_message"] == ""
    assert process_exit["downstream_exception_traceback"] == ""


def test_fake_execution_exception_records_traceback_without_com(tmp_path, monkeypatch):
    monkeypatch.setattr(probe, "evaluate_h2o_1_point_no_write_gate", lambda *args, **kwargs: _approved_admission())
    monkeypatch.setattr(
        probe,
        "prepare_h2o_downstream_points_config",
        lambda raw_cfg, *, config_path, output_dir: (Path(output_dir) / "fake_downstream_config.json", {"points_config_alignment_ready": True}),
    )

    def fake_execute(config_path):
        raise RuntimeError("fake downstream boom")

    monkeypatch.setattr(probe, "execute_h2o_single_point_probe", fake_execute)
    handoff = tmp_path / "handoff_exception"
    summary = probe.write_h2o_1_point_no_write_probe_artifacts(
        _minimal_cfg(),
        output_dir=handoff,
        config_path=tmp_path / "config.json",
        operator_confirmation_path=tmp_path / "operator.json",
        branch="codex/v2-golden-recovery-cdb82111",
        head="fake-head",
        cli_allow=True,
        env={probe.H2O_ENV_VAR: probe.H2O_ENV_VALUE},
        execute_probe=True,
        run_app_py_untouched=True,
    )
    process_exit = _read_json(handoff / "process_exit_record.json")

    assert summary["final_decision"] == "FAIL_CLOSED"
    assert summary["downstream_exception_class"] == "RuntimeError"
    assert summary["downstream_exception_message"] == "fake downstream boom"
    assert "RuntimeError: fake downstream boom" in summary["downstream_exception_traceback"]
    assert process_exit["real_com_opened"] == "unknown"
    assert process_exit["any_write_command_sent"] == "unknown"


def test_safety_assertions_finalize_from_downstream_no_write_guard(tmp_path, monkeypatch):
    downstream = _make_downstream_run(tmp_path)
    summary, process_exit, handoff = _run_probe(tmp_path, monkeypatch, downstream)
    safety = _read_json(handoff / "safety_assertions.json")

    assert safety["record_type"] == "h2o_safety_assertions_final"
    assert safety["attempted_write_count"] == 0
    assert safety["any_write_command_sent"] is False
    assert safety["device_command_audit_complete"] is True
    assert safety["no_write_assertion_status"] == "pass_engineering_evidence"
    assert safety["must_not_claim_no_write_pass"] is False
    assert summary["no_write_assertion_status"] == "pass_engineering_evidence"
    assert process_exit["no_write_assertion_status"] == "pass_engineering_evidence"


def test_route_ready_fail_before_sampling_does_not_require_samples_as_hard_artifacts(tmp_path, monkeypatch):
    downstream = _make_downstream_run(tmp_path, final_decision="FAIL", before_sampling=True)
    summary, process_exit, _ = _run_probe(tmp_path, monkeypatch, downstream, final_decision="FAIL")

    assert summary["final_decision"] == "FAIL_CLOSED"
    assert summary["missing_artifacts_expected_due_to_before_sampling"] is True
    assert summary["artifact_completeness_fail_reason"] == ""
    assert "samples_runtime" in summary["required_artifact_keys_present"]
    assert "point_results_json" in summary["required_artifact_keys_present"]
    assert "samples_runtime" not in summary["required_artifact_keys_missing"]
    assert "point_results_json" not in summary["required_artifact_keys_missing"]
    assert process_exit["missing_artifacts_expected_due_to_before_sampling"] is True


def test_probe_summary_preserves_downstream_business_failure_before_sampling(tmp_path, monkeypatch):
    downstream = _make_downstream_run(tmp_path, final_decision="FAIL", before_sampling=True)
    summary, _, _ = _run_probe(tmp_path, monkeypatch, downstream, final_decision="FAIL")

    assert summary["final_decision"] == "FAIL_CLOSED"
    assert "downstream_business_failed" in summary["rejection_reasons"]
    assert summary["downstream_failure_reason"] == "H2O route readiness failed"
    assert summary["downstream_final_decision"] == "FAIL"


def test_process_exit_records_route_ready_failure_step_and_reason(tmp_path, monkeypatch):
    downstream = _make_downstream_run(tmp_path, final_decision="FAIL", before_sampling=True)
    _, process_exit, _ = _run_probe(tmp_path, monkeypatch, downstream, final_decision="FAIL")

    assert process_exit["route_ready_failure_step"] == "set_h2o_path_open"
    assert process_exit["route_ready_failure_reason"] == "relay_physical_mismatch"


def test_no_write_finalization_still_blocks_real_acceptance(tmp_path, monkeypatch):
    downstream = _make_downstream_run(tmp_path)
    summary, process_exit, handoff = _run_probe(tmp_path, monkeypatch, downstream)
    safety = _read_json(handoff / "safety_assertions.json")

    assert safety["not_real_acceptance_evidence"] is True
    assert safety["promotion_state"] == "blocked"
    assert safety["real_primary_latest_refresh"] is False
    assert safety["engineering_probe_only"] is True
    assert summary["any_write_command_sent"] is False
    assert summary["no_write_assertion_status"] == "pass_engineering_evidence"
    assert summary["not_real_acceptance_evidence"] is True
    assert summary["promotion_state"] == "blocked"
    assert summary["real_primary_latest_refresh"] is False
    assert process_exit["not_real_acceptance_evidence"] is True
    assert process_exit["promotion_state"] == "blocked"
    assert process_exit["real_primary_latest_refresh"] is False
