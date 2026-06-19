import json
from types import SimpleNamespace

from gas_calibrator.tools.run_v1_5_formal_initialization_runner import (
    build_formal_initialization_plan,
    execute_formal_initialization_plan,
    main as initialization_cli,
    write_formal_initialization_database_bundle,
    write_formal_initialization_plan,
)


def _write_config(path):
    path.write_text(
        json.dumps(
            {
                "devices": {
                    "gas_analyzers": [
                        {"name": "GA01", "port": "COM35", "device_id": "001", "enabled": True},
                        {"name": "GA02", "port": "COM36", "device_id": "090", "enabled": True},
                    ]
                }
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    return path


def test_formal_initialization_plan_freezes_getco_and_keeps_planner_safe(tmp_path):
    config = _write_config(tmp_path / "runtime.json")

    plan = build_formal_initialization_plan(config_path=config, output_dir=tmp_path / "out", run_id="demo")

    assert plan.schema == "v1_5_formal_initialization_plan_v0"
    assert plan.dry_run_only is True
    assert plan.expected_device_ids == ("001", "090")
    assert plan.safety_contract["planner_opens_com_ports"] is False
    assert plan.safety_contract["planner_writes_coefficients"] is False
    assert plan.safety_contract["planner_controls_gas_route"] is False
    assert plan.safety_contract["planner_controls_water_route"] is False
    assert plan.safety_contract["does_not_write_device_id"] is True
    assert plan.tool_ownership["formal_initialization_runner"]["role"] == "single_formal_initialization_entrypoint"
    assert plan.tool_ownership["getco_snapshot_probe"]["role"] == (
        "subordinate_read_only_identity_and_getco_snapshot"
    )
    assert plan.tool_ownership["controlled_writers"]["role"] == "subordinate_authorized_write_tools"
    assert plan.tool_ownership["formal_route_readiness_probe"]["role"] == (
        "subordinate_initialization_route_readiness_probe"
    )

    getco = next(step for step in plan.steps if step.step_id == "identity_and_getco_epoch0_snapshot")
    command = list(getco.command)
    assert getco.opens_com_ports is True
    assert getco.writes_coefficients is False
    assert command[command.index("--groups") + 1] == "1,2,3,4,5,6,7,8,9"
    assert command[command.index("--command-gap-s") + 1] == "1.2"
    assert "--allow-runtime-identity-rebind" in command


def test_formal_initialization_rejects_subsecond_analyzer_command_gap(tmp_path):
    config = _write_config(tmp_path / "runtime.json")

    rc = initialization_cli(
        [
            "--config",
            str(config),
            "--output-dir",
            str(tmp_path / "out"),
            "--command-gap-s",
            "0.5",
        ]
    )

    assert rc == 1


def test_formal_initialization_records_s7_s8_and_s9_physical_policies(tmp_path):
    config = _write_config(tmp_path / "runtime.json")

    plan = build_formal_initialization_plan(
        config_path=config,
        output_dir=tmp_path / "out",
        senco78_policy="review_then_single_point_repair_if_abnormal",
        senco9_policy="direct_pressure_calibration",
    )

    assert plan.coefficient_policy["senco78"] == "review_then_single_point_repair_if_abnormal"
    assert plan.coefficient_policy["senco78_epoch0_snapshot"] == "old_GETCO7_GETCO8_required_before_any_temperature_write"
    assert "reverse_or_compose_old_senco78" in plan.coefficient_policy["senco78_if_not_neutralized"]
    assert plan.coefficient_policy["senco9"] == "direct_pressure_calibration"
    assert plan.physical_contract["pressure_before_components"] == "direct_multi_point_pressure_calibration_and_verification"
    assert (
        plan.physical_contract["temperature_before_components"]
        == "current_temperature_and_subzero_projection_review_after_senco78_repair"
    )
    assert "epoch0_GETCO7_8" in plan.physical_contract["s7_s8_old_coefficient_handling"]
    assert "subzero" in plan.physical_contract["s7_s8_subzero_failure_guard"]

    current_temp_gate = next(step for step in plan.steps if step.step_id == "temperature_current_point_review_gate")
    pressure_gate = next(step for step in plan.steps if step.step_id == "senco9_pressure_policy_gate")
    route_gate = next(step for step in plan.steps if step.step_id == "formal_route_readiness_probe")
    assert "sub-zero" in current_temp_gate.physical_meaning
    assert current_temp_gate.writes_coefficients is False
    assert "old_component_coefficients_snapshot.json" in " ".join(current_temp_gate.command or ())
    assert "multi-pressure calibration" in pressure_gate.physical_meaning
    assert pressure_gate.execution_mode == "clear_only_when_pressure_channel_is_fixed_or_prior_s9_is_untrustworthy"
    assert "chamber soak" in route_gate.physical_meaning
    assert route_gate.opens_com_ports is True
    assert route_gate.controls_gas_route is False
    assert route_gate.controls_water_route is False
    assert "run_v1_5_formal_route_readiness_probe" in " ".join(route_gate.command or ())


def test_formal_initialization_writer_outputs_reviewable_artifacts(tmp_path):
    config = _write_config(tmp_path / "runtime.json")
    out = tmp_path / "out"
    plan = build_formal_initialization_plan(config_path=config, output_dir=out, run_id="demo")

    outputs = write_formal_initialization_plan(plan)

    assert outputs["json"].exists()
    assert outputs["markdown"].exists()
    assert outputs["powershell"].exists()
    assert outputs["contract_json"].exists()
    assert outputs["database_bundle_json"].exists()

    contract = json.loads(outputs["contract_json"].read_text(encoding="utf-8"))
    assert contract["tool_ownership"]["formal_initialization_runner"]["role"] == (
        "single_formal_initialization_entrypoint"
    )
    assert contract["tool_ownership"]["getco_snapshot_probe"]["forbidden_use"].startswith(
        "Not a top-level formal initialization entrypoint"
    )
    assert contract["required_before_open_flow"] == [
        "identity_and_getco_epoch0_snapshot",
        "senco5_neutralization_gate",
        "senco6_neutralization_gate",
        "temperature_current_point_review_gate",
        "temperature_current_point_single_point_repair_gate",
        "senco9_pressure_policy_gate",
        "pressure_channel_completion_audit",
        "mode2_1hz_filter_startup_contract",
        "formal_route_readiness_probe",
        "initialization_readiness_audit",
    ]

    ps1 = outputs["powershell"].read_text(encoding="utf-8")
    md = outputs["markdown"].read_text(encoding="utf-8")
    assert "## Tool ownership" in md
    assert "single_formal_initialization_entrypoint" in md
    assert "probe_v1_5_getco_component_snapshot" in ps1
    assert "# CONTROLLED WRITE GATE" in ps1
    assert "# python -m gas_calibrator.tools.run_v1_5_co2_senco5_neutral_controlled_write" in ps1
    assert "export_v1_5_initialization_readiness" in ps1
    assert "run_v1_5_formal_route_readiness_probe" in ps1
    assert "pressure_channel_completion_audit" in ps1
    assert "--readback-retry-delay-s 1.2" in ps1
    assert "--coefficient-read-delay-s 1.2" in ps1

    bundle = json.loads(outputs["database_bundle_json"].read_text(encoding="utf-8"))
    assert bundle["schema"] == "v1_5_formal_initialization_db_bundle_v0"
    assert bundle["tables"]["runs"][0]["plan_id"] == "v1_5_formal_initialization"
    assert bundle["tables"]["runs"][0]["analyzer_id"] == "001,090"
    assert {row["serial_number"] for row in bundle["tables"]["devices"]} == {"001", "090"}
    assert {
        row["artifact_role"] for row in bundle["tables"]["sample_files"]
    } >= {"initialization_plan_snapshot", "initialization_contract", "initialization_command_plan"}
    assert any(
        row["check_name"] == "formal_initialization_planner_no_real_com" and row["status"] == "pass"
        for row in bundle["tables"]["evidence_integrity_checks"]
    )


def test_formal_initialization_database_bundle_indexes_epoch0_getco_by_device_id(tmp_path):
    config = _write_config(tmp_path / "runtime.json")
    out = tmp_path / "out"
    plan = build_formal_initialization_plan(config_path=config, output_dir=out, run_id="demo")
    outputs = write_formal_initialization_plan(plan)

    snapshot_dir = out / "coefficient_epoch_0_getco_snapshot"
    snapshot_dir.mkdir(parents=True, exist_ok=True)
    (snapshot_dir / "old_component_coefficients_snapshot.json").write_text(
        json.dumps(
            {
                "001": {
                    "analyzer_device_id": "001",
                    "GETCO1_before": [1.0, 2.0],
                    "GETCO2_before": [3.0, 4.0],
                    "GETCO3_before": [5.0, 6.0],
                    "GETCO4_before": [7.0, 8.0],
                    "GETCO5_before": [0.0, 1.0],
                    "GETCO6_before": [0.0, 1.0],
                    "GETCO7_before": [0.0, 1.0],
                    "GETCO8_before": [0.0, 1.0],
                    "GETCO9_before": [0.0, 1.0],
                },
                "090": {
                    "analyzer_device_id": "090",
                    "GETCO1_before": [9.0, 10.0],
                    "GETCO5_before": [0.0, 1.0],
                    "GETCO9_before": [0.0, 1.0],
                },
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    (snapshot_dir / "getco_component_snapshot_identity.csv").write_text(
        "analyzer_device_id,port,status\n001,COM35,ok\n090,COM36,ok\n",
        encoding="utf-8",
    )
    (snapshot_dir / "runtime_identity_bound_config.json").write_text(
        json.dumps({"devices": {"gas_analyzers": []}}, ensure_ascii=False),
        encoding="utf-8",
    )

    bundle_path = write_formal_initialization_database_bundle(plan, outputs)
    bundle = json.loads(bundle_path.read_text(encoding="utf-8"))

    snapshots = bundle["tables"]["coefficient_snapshots"]
    assert {row["analyzer_id"] for row in snapshots} == {"001", "090"}
    by_id = {row["analyzer_id"]: row for row in snapshots}
    assert by_id["001"]["snapshot_type"] == "initialization_epoch0_getco1_9"
    assert by_id["001"]["metadata"]["missing_getco_groups"] == []
    assert by_id["090"]["metadata"]["missing_getco_groups"] == [
        "GETCO2",
        "GETCO3",
        "GETCO4",
        "GETCO6",
        "GETCO7",
        "GETCO8",
    ]
    assert any(
        row["check_name"] == "epoch0_getco1_9_snapshot_indexed" and row["status"] == "pass"
        for row in bundle["tables"]["evidence_integrity_checks"]
    )


def test_formal_initialization_cli_writes_plan(tmp_path):
    config = _write_config(tmp_path / "runtime.json")
    out = tmp_path / "cli"

    rc = initialization_cli(["--config", str(config), "--output-dir", str(out), "--run-id", "demo"])

    assert rc == 0
    payload = json.loads((out / "v1_5_formal_initialization_plan.json").read_text(encoding="utf-8"))
    assert payload["run_id"] == "demo"
    assert payload["safety_contract"]["minimum_analyzer_command_gap_s"] == 1.0
    assert payload["coefficient_policy"]["senco5"] == "neutralize_or_explicitly_model_before_co2_main_fit"
    assert (out / "v1_5_formal_initialization_db_bundle.json").exists()


def test_formal_initialization_executor_runs_only_offline_without_unlocks(tmp_path):
    config = _write_config(tmp_path / "runtime.json")
    out = tmp_path / "exec"
    plan = build_formal_initialization_plan(config_path=config, output_dir=out, run_id="demo")
    outputs = write_formal_initialization_plan(plan)
    calls = []

    def fake_runner(command, **_kwargs):
        calls.append(tuple(command))
        return SimpleNamespace(returncode=0, stdout="ok", stderr="")

    report, execution_outputs = execute_formal_initialization_plan(
        plan,
        outputs=outputs,
        command_runner=fake_runner,
        stop_on_failure=False,
    )

    assert report.status == "partial"
    assert len(calls) == 1
    assert any("export_v1_5_initialization_readiness" in part for part in calls[0])
    by_step = {row.step_id: row for row in report.step_results}
    assert by_step["identity_and_getco_epoch0_snapshot"].reason == "skipped_read_only_real_com_locked"
    assert by_step["senco5_neutralization_gate"].reason == "skipped_controlled_write_locked"
    assert by_step["initialization_readiness_audit"].status == "passed"
    assert execution_outputs["execution_json"].exists()
    assert execution_outputs["execution_csv"].exists()


def test_formal_initialization_executor_exports_pressure_completion_before_readiness_when_paths_are_supplied(tmp_path):
    config = _write_config(tmp_path / "runtime.json")
    out = tmp_path / "exec_pressure_completion"
    pressure_inputs = tmp_path / "pressure_inputs"
    pressure_inputs.mkdir()
    senco9_write = pressure_inputs / "senco9_write_summary.csv"
    post_write = pressure_inputs / "pressure_fit_summary.csv"
    reference = pressure_inputs / "com22_reference.json"
    traceability = pressure_inputs / "com22_traceability.json"
    for path in (senco9_write, post_write):
        path.write_text("analyzer_id,status\n001,ready\n", encoding="utf-8")
    reference.write_text(json.dumps({"device_id": "COM22"}, ensure_ascii=False), encoding="utf-8")
    traceability.write_text(json.dumps({"certificate_id": "demo"}, ensure_ascii=False), encoding="utf-8")
    plan = build_formal_initialization_plan(
        config_path=config,
        output_dir=out,
        run_id="demo",
        pressure_completion_senco9_write_summary=senco9_write,
        pressure_completion_post_write_fit_summary=post_write,
        pressure_completion_reference_json=reference,
        pressure_completion_reference_traceability=traceability,
        pressure_completion_device_ids=("001",),
        pressure_completion_known_limitations=("090_command_chain_blocked|excluded|observe only|diagnostic"),
    )
    outputs = write_formal_initialization_plan(plan)
    calls = []

    def fake_runner(command, **_kwargs):
        calls.append(tuple(command))
        return SimpleNamespace(returncode=0, stdout="ok", stderr="")

    report, _execution_outputs = execute_formal_initialization_plan(
        plan,
        outputs=outputs,
        command_runner=fake_runner,
        selected_steps=("pressure_channel_completion_audit", "initialization_readiness_audit"),
        stop_on_failure=False,
    )

    assert report.status == "passed"
    joined = [" ".join(command) for command in calls]
    assert len(calls) == 2
    assert "export_v1_5_pressure_channel_completion" in joined[0]
    assert "export_v1_5_initialization_readiness" in joined[1]
    assert "--device-id 001" in joined[0]
    by_step = {row.step_id: row for row in report.step_results}
    assert by_step["pressure_channel_completion_audit"].status == "passed"
    assert by_step["pressure_channel_completion_audit"].opens_com_ports is False
    assert by_step["pressure_channel_completion_audit"].writes_coefficients is False


def test_formal_initialization_executor_read_only_unlock_does_not_run_writers(tmp_path):
    config = _write_config(tmp_path / "runtime.json")
    out = tmp_path / "exec_readonly"
    plan = build_formal_initialization_plan(config_path=config, output_dir=out, run_id="demo")
    outputs = write_formal_initialization_plan(plan)
    calls = []

    def fake_runner(command, **_kwargs):
        calls.append(tuple(command))
        return SimpleNamespace(returncode=0, stdout="ok", stderr="")

    report, _outputs = execute_formal_initialization_plan(
        plan,
        outputs=outputs,
        allow_read_only_real_com=True,
        command_runner=fake_runner,
        stop_on_failure=False,
    )

    assert report.status == "partial"
    joined = [" ".join(command) for command in calls]
    assert any("probe_v1_5_getco_component_snapshot" in command for command in joined)
    assert any("run_v1_5_temperature_current_point_review" in command for command in joined)
    assert any("run_v1_5_formal_route_readiness_probe" in command for command in joined)
    assert any("export_v1_5_initialization_readiness" in command for command in joined)
    assert not any("controlled_write" in command for command in joined)
    by_step = {row.step_id: row for row in report.step_results}
    assert by_step["identity_and_getco_epoch0_snapshot"].status == "passed"
    assert by_step["temperature_current_point_review_gate"].status == "passed"
    assert by_step["senco6_neutralization_gate"].reason == "skipped_controlled_write_locked"


def test_formal_initialization_executor_blocks_writes_without_reviewer_approver(tmp_path):
    config = _write_config(tmp_path / "runtime.json")
    out = tmp_path / "exec_blocked"
    plan = build_formal_initialization_plan(config_path=config, output_dir=out, run_id="demo")
    outputs = write_formal_initialization_plan(plan)
    calls = []

    def fake_runner(command, **_kwargs):
        calls.append(tuple(command))
        return SimpleNamespace(returncode=0, stdout="ok", stderr="")

    report, _outputs = execute_formal_initialization_plan(
        plan,
        outputs=outputs,
        allow_read_only_real_com=True,
        allow_controlled_writes=True,
        command_runner=fake_runner,
    )

    assert report.status == "blocked"
    assert len(calls) == 1
    by_step = {row.step_id: row for row in report.step_results}
    assert by_step["identity_and_getco_epoch0_snapshot"].status == "passed"
    assert by_step["senco5_neutralization_gate"].status == "blocked"
    assert by_step["senco5_neutralization_gate"].reason == "blocked_missing_reviewer_or_approver"


def test_formal_initialization_executor_runs_controlled_writes_when_approved(tmp_path):
    config = _write_config(tmp_path / "runtime.json")
    out = tmp_path / "exec_write"
    plan = build_formal_initialization_plan(
        config_path=config,
        output_dir=out,
        run_id="demo",
        reviewer="reviewer_a",
        approver="approver_b",
    )
    outputs = write_formal_initialization_plan(plan)
    calls = []

    def fake_runner(command, **_kwargs):
        calls.append(tuple(command))
        return SimpleNamespace(returncode=0, stdout="ok", stderr="")

    report, execution_outputs = execute_formal_initialization_plan(
        plan,
        outputs=outputs,
        allow_read_only_real_com=True,
        allow_controlled_writes=True,
        command_runner=fake_runner,
    )

    assert report.status == "passed"
    joined = [" ".join(command) for command in calls]
    assert len(calls) == 8
    assert any("run_v1_5_co2_senco5_neutral_controlled_write" in command for command in joined)
    assert any("run_v1_5_h2o_senco6_neutral_controlled_write" in command for command in joined)
    assert any("run_v1_5_temperature_current_point_review" in command for command in joined)
    assert any("run_v1_5_pressure_senco9_clear_controlled_write" in command for command in joined)

    refreshed_bundle = write_formal_initialization_database_bundle(plan, execution_outputs)
    bundle = json.loads(refreshed_bundle.read_text(encoding="utf-8"))
    assert any(
        row["artifact_role"] == "initialization_execution_log"
        for row in bundle["tables"]["sample_files"]
    )
