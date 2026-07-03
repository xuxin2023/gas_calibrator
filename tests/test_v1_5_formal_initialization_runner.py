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


def _write_top_level_sn_config(path):
    path.write_text(
        json.dumps(
            {
                "analyzers": [
                    {
                        "slot": "GA01",
                        "port": "COM36",
                        "protocol_device_id": "047",
                        "sn_code": "01260601",
                        "device_code": "01260601",
                        "enabled": True,
                    },
                    {
                        "slot": "GA02",
                        "port": "COM37",
                        "protocol_device_id": "054",
                        "sn_code": "01260602",
                        "device_code": "01260602",
                        "enabled": True,
                    },
                ]
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
    assert plan.safety_contract["production_database_backend"] == "postgresql"
    assert plan.safety_contract["production_database_required_major"] == 18
    assert plan.tool_ownership["formal_initialization_runner"]["role"] == "single_formal_initialization_entrypoint"
    assert plan.tool_ownership["getco_snapshot_probe"]["role"] == (
        "subordinate_read_only_identity_and_getco_snapshot"
    )
    assert plan.tool_ownership["sn_identity_initialization"]["role"] == (
        "subordinate_first_discovery_sn_device_code_planner"
    )
    assert plan.tool_ownership["initialization_db_preflight"]["role"] == (
        "subordinate_postgresql18_identity_and_evidence_preflight"
    )
    assert plan.tool_ownership["analyzer_check_monitor"]["role"] == (
        "downstream_read_only_chamber_stable_monitor_record"
    )
    assert plan.tool_ownership["controlled_writers"]["role"] == "subordinate_authorized_write_tools"
    assert plan.tool_ownership["formal_route_readiness_probe"]["role"] == (
        "subordinate_initialization_route_readiness_probe"
    )

    sn_step = next(step for step in plan.steps if step.step_id == "sn_identity_initialization_plan")
    sn_command = " ".join(sn_step.command)
    assert sn_step.opens_com_ports is False
    assert sn_step.writes_device_id is False
    assert sn_step.execution_mode == "offline_sn_identity_plan_only"
    assert "run_v1_5_sn_identity_initialization" in sn_command
    assert "--execute" not in sn_command
    assert "I_AUTHORIZE_V1_5_SN_IDENTITY_WRITE" not in sn_command
    assert "SN/device_code" in sn_step.physical_meaning

    getco = next(step for step in plan.steps if step.step_id == "identity_and_getco_epoch0_snapshot")
    command = list(getco.command)
    assert getco.opens_com_ports is True
    assert getco.writes_coefficients is False
    assert command[command.index("--groups") + 1] == "1,2,3,4,5,6,7,8,9"
    assert command[command.index("--command-gap-s") + 1] == "1.2"
    assert "--allow-runtime-identity-rebind" in command


def test_formal_initialization_accepts_current_batch_sn_identity_shape(tmp_path):
    config = _write_top_level_sn_config(tmp_path / "current6_like.json")
    out = tmp_path / "out"

    plan = build_formal_initialization_plan(config_path=config, output_dir=out, run_id="demo")
    outputs = write_formal_initialization_plan(plan)
    bundle = json.loads(outputs["database_bundle_json"].read_text(encoding="utf-8"))

    assert plan.expected_device_ids == ("047", "054")
    assert [row["sn_code"] for row in plan.analyzer_identities] == ["01260601", "01260602"]
    assert bundle["tables"]["runs"][0]["analyzer_id"] == "047,054"
    assert [row["sn_code"] for row in bundle["tables"]["devices"]] == ["01260601", "01260602"]
    assert [row["device_code"] for row in bundle["tables"]["devices"]] == ["01260601", "01260602"]
    assert [row["protocol_device_id_at_run"] for row in bundle["tables"]["run_devices"]] == ["047", "054"]
    assert bundle["tables"]["run_devices"][0]["metadata"]["identity_key"] == "sn_code/device_code"


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

    assert plan.coefficient_policy["senco78"] == "neutralize_in_initialization"
    assert plan.coefficient_policy["senco78_requested_policy"] == "review_then_single_point_repair_if_abnormal"
    assert plan.coefficient_policy["senco78_epoch0_snapshot"] == "old_GETCO7_GETCO8_required_before_any_temperature_write"
    assert plan.coefficient_policy["senco78_neutralization"] == (
        "required_after_epoch0_for_classic_and_new_algorithm_analyzers"
    )
    assert "disabled" in plan.coefficient_policy["senco78_temperature_calibration"]
    assert plan.coefficient_policy["senco78_if_not_neutralized"] == "blocked_before_open_flow_sampling"
    assert plan.coefficient_policy["senco9"] == "direct_pressure_calibration"
    assert plan.physical_contract["pressure_before_components"] == "direct_multi_point_pressure_calibration_and_verification"
    assert (
        plan.physical_contract["temperature_before_components"]
        == "SENCO7/SENCO8_neutralized; use analyzer runtime chamber/cell temperature directly"
    )
    assert plan.physical_contract["runtime_acquisition"] == "MODE2_1Hz_active_upload_with_AVERAGE1_2_filter_before_sampling"
    assert "PostgreSQL 18" in plan.physical_contract["database_identity_lookup"]
    assert "--require-postgresql-18" in plan.physical_contract["database_preflight_before_routes"]
    assert "CHECK,YGAS,FFF" in plan.physical_contract["check_monitor_after_chamber_temp_stable"]
    assert "--execute-controlled-writes" in plan.physical_contract["device_control_authorization"]
    assert "must_be_neutralized" in plan.physical_contract["s7_s8_old_coefficient_handling"]
    assert "neutralization" in plan.physical_contract["s7_s8_subzero_failure_guard"]

    temperature_gate = next(step for step in plan.steps if step.step_id == "senco78_neutralization_gate")
    pressure_gate = next(step for step in plan.steps if step.step_id == "senco9_pressure_policy_gate")
    pressure_preflight = next(step for step in plan.steps if step.step_id == "pressure_senco9_no_write_preflight")
    db_preflight = next(step for step in plan.steps if step.step_id == "initialization_db_preflight_postgresql18_gate")
    check_monitor = next(
        step for step in plan.steps if step.step_id == "analyzer_check_monitor_after_chamber_temp_stable_contract"
    )
    route_gate = next(step for step in plan.steps if step.step_id == "formal_route_readiness_probe")
    assert "no longer performs temperature calibration" in temperature_gate.physical_meaning
    assert temperature_gate.writes_coefficients is True
    temperature_command = " ".join(temperature_gate.command or ())
    assert "run_v1_5_temperature_senco78_neutral_controlled_write" in temperature_command
    assert "--enable-senco78-write" in temperature_command
    assert "WRITE_SENCO78_NEUTRAL_V1_5_TEMPERATURE_INPUTS" in temperature_command
    assert "--write-all-nonneutral" in temperature_command
    assert "senco78_neutral_write_events.csv" in " ".join(temperature_gate.expected_outputs)
    assert "multi-pressure calibration" in pressure_gate.physical_meaning
    assert pressure_gate.execution_mode == "clear_only_when_pressure_channel_is_fixed_or_prior_s9_is_untrustworthy"
    assert pressure_preflight.opens_com_ports is False
    assert pressure_preflight.writes_coefficients is False
    assert "export_v1_5_pressure_senco9_no_write_preflight" in " ".join(pressure_preflight.command or ())
    assert "--pressure-reference-json" in pressure_preflight.command
    assert "FRGsz25038057" in " ".join(pressure_preflight.command or ())
    assert db_preflight.command == ()
    assert db_preflight.opens_com_ports is False
    assert db_preflight.writes_coefficients is False
    assert "PostgreSQL 18" in db_preflight.physical_meaning
    assert "--require-postgresql-18" in " ".join(db_preflight.safety_notes)
    assert check_monitor.command == ()
    assert check_monitor.opens_com_ports is True
    assert check_monitor.writes_coefficients is False
    assert check_monitor.gate == "required_before_point_sampling_after_chamber_temperature_stable"
    assert "CHECK,YGAS,FFF" in check_monitor.physical_meaning
    assert "analyzer_check_monitor.csv" in check_monitor.expected_outputs
    assert "chamber soak" in route_gate.physical_meaning
    assert route_gate.opens_com_ports is True
    assert route_gate.controls_gas_route is False
    assert route_gate.controls_water_route is False
    assert "run_v1_5_formal_route_readiness_probe" in " ".join(route_gate.command or ())
    assert "temperature_current_point_review_gate" not in {step.step_id for step in plan.steps}
    assert "temperature_current_point_single_point_repair_gate" not in {step.step_id for step in plan.steps}
    step_order = {step.step_id: index for index, step in enumerate(plan.steps)}
    assert step_order["pressure_channel_completion_audit"] < step_order["initialization_db_preflight_postgresql18_gate"]
    assert step_order["initialization_db_preflight_postgresql18_gate"] < step_order["formal_route_readiness_probe"]
    assert step_order["formal_route_readiness_probe"] < step_order["analyzer_check_monitor_after_chamber_temp_stable_contract"]
    assert step_order["analyzer_check_monitor_after_chamber_temp_stable_contract"] < step_order["initialization_readiness_audit"]


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
        "sn_identity_initialization_plan",
        "identity_and_getco_epoch0_snapshot",
        "senco5_neutralization_gate",
        "senco6_neutralization_gate",
        "senco78_neutralization_gate",
        "senco9_pressure_policy_gate",
        "pressure_senco9_no_write_preflight",
        "pressure_channel_completion_audit",
        "mode2_1hz_filter_startup_contract",
        "initialization_db_preflight_postgresql18_gate",
        "formal_route_readiness_probe",
        "initialization_readiness_audit",
    ]
    assert contract["required_before_point_sampling_after_chamber_temperature_stable"] == [
        "analyzer_check_monitor_after_chamber_temp_stable_contract",
    ]

    ps1 = outputs["powershell"].read_text(encoding="utf-8")
    md = outputs["markdown"].read_text(encoding="utf-8")
    assert "## Tool ownership" in md
    assert "single_formal_initialization_entrypoint" in md
    assert "subordinate_first_discovery_sn_device_code_planner" in md
    assert "PostgreSQL 18" in md
    assert "CHECK,YGAS,FFF" in md
    assert "run_v1_5_sn_identity_initialization" in ps1
    assert "--execute" not in ps1
    assert "I_AUTHORIZE_V1_5_SN_IDENTITY_WRITE" not in ps1
    assert "probe_v1_5_getco_component_snapshot" in ps1
    assert "# PRODUCTION DEVICE WRITE" in ps1
    assert "# python -m gas_calibrator.tools.run_v1_5_co2_senco5_neutral_controlled_write" in ps1
    assert "production_operator" in ps1
    assert "v1_5_device_control_authorized" in ps1
    assert "run_v1_5_temperature_senco78_neutral_controlled_write" in ps1
    assert "WRITE_SENCO78_NEUTRAL_V1_5_TEMPERATURE_INPUTS" in ps1
    assert "run_v1_5_temperature_current_point_review" not in ps1
    assert "export_v1_5_initialization_readiness" in ps1
    assert "run_v1_5_formal_route_readiness_probe" in ps1
    assert "export_v1_5_pressure_senco9_no_write_preflight" in ps1
    assert "--pressure-reference-json" in ps1
    assert "pressure_channel_completion_audit" in ps1
    assert "initialization_db_preflight_postgresql18_gate" in ps1
    assert "analyzer_check_monitor_after_chamber_temp_stable_contract" in ps1
    assert "run_v1_5_initialization_db_preflight" not in ps1
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
    integrity_by_name = {row["check_name"]: row for row in bundle["tables"]["evidence_integrity_checks"]}
    assert integrity_by_name["postgresql18_initialization_db_preflight_required"]["status"] == "pass"
    assert integrity_by_name["postgresql18_initialization_db_preflight_required"]["details"][
        "production_database_required_major"
    ] == 18
    assert integrity_by_name["sn_device_code_primary_identity_contract"]["status"] == "pass"
    assert integrity_by_name["runtime_mode2_1hz_filter_contract"]["status"] == "pass"
    assert integrity_by_name["senco78_temperature_calibration_disabled"]["status"] == "pass"
    assert integrity_by_name["check_monitor_after_chamber_temp_stable_contract"]["status"] == "pass"
    assert (
        integrity_by_name["check_monitor_after_chamber_temp_stable_contract"]["details"]["artifact"]
        == "analyzer_check_monitor.csv"
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
    assert payload["coefficient_policy"]["senco78"] == "neutralize_in_initialization"
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
    assert len(calls) == 3
    joined = [" ".join(command) for command in calls]
    assert any("run_v1_5_sn_identity_initialization" in command for command in joined)
    assert any("export_v1_5_pressure_senco9_no_write_preflight" in command for command in joined)
    assert any("export_v1_5_initialization_readiness" in command for command in joined)
    by_step = {row.step_id: row for row in report.step_results}
    assert by_step["sn_identity_initialization_plan"].status == "passed"
    assert by_step["sn_identity_initialization_plan"].writes_device_id is False
    assert by_step["identity_and_getco_epoch0_snapshot"].reason == "skipped_read_only_real_com_locked"
    assert by_step["senco5_neutralization_gate"].reason == "skipped_controlled_write_locked"
    assert by_step["pressure_senco9_no_write_preflight"].status == "passed"
    assert by_step["initialization_readiness_audit"].status == "passed"
    assert execution_outputs["execution_json"].exists()
    assert execution_outputs["execution_csv"].exists()


def test_formal_initialization_db_and_check_contracts_do_not_execute_hardware_or_database(tmp_path):
    config = _write_config(tmp_path / "runtime.json")
    out = tmp_path / "exec_contracts"
    plan = build_formal_initialization_plan(config_path=config, output_dir=out, run_id="demo")
    outputs = write_formal_initialization_plan(plan)
    calls = []

    def fake_runner(command, **_kwargs):
        calls.append(tuple(command))
        raise AssertionError("contract-only initialization gates must not spawn subprocesses")

    report, _execution_outputs = execute_formal_initialization_plan(
        plan,
        outputs=outputs,
        selected_steps=(
            "initialization_db_preflight_postgresql18_gate",
            "analyzer_check_monitor_after_chamber_temp_stable_contract",
        ),
        command_runner=fake_runner,
        stop_on_failure=False,
    )

    assert report.status == "passed"
    assert calls == []
    by_step = {row.step_id: row for row in report.step_results}
    db_gate = by_step["initialization_db_preflight_postgresql18_gate"]
    check_gate = by_step["analyzer_check_monitor_after_chamber_temp_stable_contract"]
    assert db_gate.status == "not_applicable"
    assert db_gate.reason == "no_standalone_command"
    assert db_gate.opens_com_ports is False
    assert db_gate.writes_coefficients is False
    assert check_gate.status == "not_applicable"
    assert check_gate.reason == "no_standalone_command"
    assert check_gate.opens_com_ports is True
    assert check_gate.writes_coefficients is False
    check_step = next(step for step in plan.steps if step.step_id == check_gate.step_id)
    assert "Legacy analyzers that do not support CHECK" in " ".join(check_step.safety_notes)
    assert "new-algorithm or CHECK-capable analyzer protocol" in check_step.required_inputs


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
    assert any("run_v1_5_sn_identity_initialization" in command for command in joined)
    assert any("probe_v1_5_getco_component_snapshot" in command for command in joined)
    assert any("run_v1_5_formal_route_readiness_probe" in command for command in joined)
    assert any("export_v1_5_initialization_readiness" in command for command in joined)
    assert not any("controlled_write" in command for command in joined)
    assert not any("run_v1_5_temperature_senco78_neutral_controlled_write" in command for command in joined)
    by_step = {row.step_id: row for row in report.step_results}
    assert by_step["identity_and_getco_epoch0_snapshot"].status == "passed"
    assert by_step["senco6_neutralization_gate"].reason == "skipped_controlled_write_locked"
    assert by_step["senco78_neutralization_gate"].reason == "skipped_controlled_write_locked"


def test_formal_initialization_executor_blocks_controlled_writes_without_real_com_unlock(tmp_path):
    config = _write_config(tmp_path / "runtime.json")
    out = tmp_path / "exec_write_without_real_com"
    plan = build_formal_initialization_plan(config_path=config, output_dir=out, run_id="demo")
    outputs = write_formal_initialization_plan(plan)
    calls = []

    def fake_runner(command, **_kwargs):
        calls.append(tuple(command))
        return SimpleNamespace(returncode=0, stdout="ok", stderr="")

    report, _outputs = execute_formal_initialization_plan(
        plan,
        outputs=outputs,
        allow_controlled_writes=True,
        command_runner=fake_runner,
        stop_on_failure=False,
    )

    joined = [" ".join(command) for command in calls]
    assert not any("controlled_write" in command for command in joined)
    by_step = {row.step_id: row for row in report.step_results}
    assert by_step["senco5_neutralization_gate"].status == "blocked"
    assert by_step["senco5_neutralization_gate"].reason == (
        "blocked_controlled_write_requires_read_only_real_com_unlock"
    )
    assert by_step["senco6_neutralization_gate"].reason == (
        "blocked_controlled_write_requires_read_only_real_com_unlock"
    )
    assert by_step["senco78_neutralization_gate"].reason == (
        "blocked_controlled_write_requires_read_only_real_com_unlock"
    )
    assert by_step["senco9_pressure_policy_gate"].reason == (
        "blocked_controlled_write_requires_read_only_real_com_unlock"
    )


def test_formal_initialization_executor_runs_device_writes_with_operator_authorization_labels(tmp_path):
    config = _write_config(tmp_path / "runtime.json")
    out = tmp_path / "exec_authorized_labels"
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

    assert report.status == "passed"
    assert len(calls) == 9
    joined = [" ".join(command) for command in calls]
    assert "run_v1_5_sn_identity_initialization" in joined[0]
    assert "probe_v1_5_getco_component_snapshot" in joined[1]
    assert any("run_v1_5_co2_senco5_neutral_controlled_write" in command for command in joined)
    assert any("--reviewer production_operator" in command for command in joined)
    assert any("--approver v1_5_device_control_authorized" in command for command in joined)
    by_step = {row.step_id: row for row in report.step_results}
    assert by_step["sn_identity_initialization_plan"].status == "passed"
    assert by_step["identity_and_getco_epoch0_snapshot"].status == "passed"
    assert by_step["senco5_neutralization_gate"].status == "passed"
    assert by_step["senco78_neutralization_gate"].status == "passed"


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
    assert len(calls) == 9
    assert any("run_v1_5_sn_identity_initialization" in command for command in joined)
    assert any("run_v1_5_co2_senco5_neutral_controlled_write" in command for command in joined)
    assert any("run_v1_5_h2o_senco6_neutral_controlled_write" in command for command in joined)
    assert any("run_v1_5_temperature_senco78_neutral_controlled_write" in command for command in joined)
    assert any("run_v1_5_pressure_senco9_clear_controlled_write" in command for command in joined)
    assert any("export_v1_5_pressure_senco9_no_write_preflight" in command for command in joined)

    refreshed_bundle = write_formal_initialization_database_bundle(plan, execution_outputs)
    bundle = json.loads(refreshed_bundle.read_text(encoding="utf-8"))
    assert any(
        row["artifact_role"] == "initialization_execution_log"
        for row in bundle["tables"]["sample_files"]
    )
