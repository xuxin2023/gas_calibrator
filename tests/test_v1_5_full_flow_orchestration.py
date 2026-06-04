import json
import sys
from dataclasses import replace

from gas_calibrator.tools.run_v1_5_full_calibration_chain import main as cli_main
from gas_calibrator.v1_5.orchestration.full_flow import (
    build_full_flow_plan,
    build_full_flow_state,
    run_supervised_full_flow,
    write_full_flow_plan,
    write_full_flow_state,
    write_full_flow_supervised_run,
)


def test_full_flow_plan_keeps_pressure_and_temperature_before_components(tmp_path):
    config = tmp_path / "config.json"
    config.write_text("{}", encoding="utf-8")
    plan = build_full_flow_plan(
        config_path=config,
        output_dir=tmp_path / "plan",
        run_id="demo",
        operator="operator-a",
        analyzer_id="multi-device",
    )

    step_ids = [step.step_id for step in plan.steps]
    assert step_ids.index("pressure_quick_check") < step_ids.index("co2_open_flow_sampling")
    assert step_ids.index("pressure_quick_check") < step_ids.index("h2o_open_flow_sampling")
    assert step_ids.index("temperature_channel_fast_review") < step_ids.index("co2_open_flow_sampling")
    assert step_ids.index("temperature_channel_fast_review") < step_ids.index("h2o_open_flow_sampling")
    assert step_ids.index("controlled_component_write_placeholder") < step_ids.index(
        "post_write_reverification_placeholder"
    )
    assert step_ids.index("post_write_reverification_placeholder") < step_ids.index("formal_evidence_sidecar")
    assert plan.coefficient_epoch_contract["do_not_clear_existing_coefficients_on_startup"] is True
    assert plan.coefficient_epoch_contract["identity_key"] == "analyzer_device_id_not_com_port_or_ga_alias"


def test_full_flow_plan_uses_v1_5_validated_entries_and_blocks_auto_writes(tmp_path):
    config = tmp_path / "config.json"
    config.write_text("{}", encoding="utf-8")
    plan = build_full_flow_plan(
        config_path=config,
        output_dir=tmp_path / "plan",
        run_id="demo",
        operator="operator-a",
        analyzer_id="multi-device",
    )

    assert plan.safety_contract["planner_opens_com_ports"] is False
    assert plan.safety_contract["planner_writes_coefficients"] is False
    assert plan.safety_contract["uses_validated_v1_5_behavior_not_folder_name_only"] is True
    assert plan.safety_contract["reference_serial_bank_shift_default_enabled"] is False
    assert plan.safety_contract["reference_serial_bank_shift_allowed_scope"] == "COM24-COM31_between_COM16-COM23_only"
    assert plan.safety_contract["gas_analyzer_serial_ports_protected"] == "COM35-COM42_use_MODE2_identity_binding"
    assert (
        plan.metadata["optional_reference_serial_port_binding_tool"]
        == "gas_calibrator.tools.prepare_v1_5_runtime_serial_port_binding"
    )
    assert all(not step.writes_device_id for step in plan.steps)
    assert not any(".v2" in (step.tool_module or "").lower() for step in plan.steps)

    write_steps = [step for step in plan.steps if step.writes_coefficients]
    assert [step.step_id for step in write_steps] == ["controlled_component_write_placeholder"]
    assert write_steps[0].execution_mode == "blocked_pending_explicit_authorization"
    assert write_steps[0].command == ()


def test_full_flow_plan_freezes_getco_1_to_9_before_sampling(tmp_path):
    config = tmp_path / "config.json"
    config.write_text("{}", encoding="utf-8")
    plan = build_full_flow_plan(
        config_path=config,
        output_dir=tmp_path / "plan",
        run_id="demo",
        operator="operator-a",
        analyzer_id="multi-device",
    )

    getco = next(step for step in plan.steps if step.step_id == "device_identity_and_getco_snapshot")
    assert getco.opens_com_ports is True
    assert getco.writes_coefficients is False
    assert getco.writes_device_id is False
    command = list(getco.command)
    assert command[command.index("--groups") + 1] == "1,2,3,4,5,6,7,8,9"
    assert "--allow-runtime-identity-rebind" in command
    assert command[command.index("--attempts-per-group") + 1] == "3"
    assert command[command.index("--response-timeout-s") + 1] == "2.5"
    assert "--include-legacy" in command
    assert "--allow-quiet-setcomway" not in command
    assert getco.coefficient_epoch_event == "start_epoch_0"
    assert "runtime_identity_bound_config.json" in getco.expected_outputs


def test_full_flow_physical_stages_use_runtime_bound_config_after_identity_snapshot(tmp_path):
    config = tmp_path / "config.json"
    config.write_text("{}", encoding="utf-8")
    plan = build_full_flow_plan(config_path=config, output_dir=tmp_path / "plan", run_id="demo")

    runtime_bound = str(tmp_path / "plan" / "coefficient_epoch_0_getco_snapshot" / "runtime_identity_bound_config.json")
    for step_id in ("pressure_quick_check", "co2_open_flow_sampling", "h2o_open_flow_sampling"):
        step = next(item for item in plan.steps if item.step_id == step_id)
        command = list(step.command)
        assert command[command.index("--config") + 1] == runtime_bound


def test_full_flow_cli_writes_json_markdown_and_command_list(tmp_path):
    config = tmp_path / "config.json"
    config.write_text("{}", encoding="utf-8")
    out = tmp_path / "flow"

    rc = cli_main(["--config", str(config), "--output-dir", str(out), "--run-id", "demo"])

    assert rc == 0
    plan_json = out / "v1_5_full_flow_plan.json"
    assert plan_json.exists()
    assert (out / "v1_5_full_flow_plan.md").exists()
    assert (out / "v1_5_full_flow_commands.ps1").exists()
    payload = json.loads(plan_json.read_text(encoding="utf-8"))
    assert payload["schema"] == "v1_5_full_calibration_flow_plan_v0"
    assert payload["dry_run_only"] is True
    assert payload["safety_contract"]["does_not_modify_run_app"] is True
    assert (out / "v1_5_full_flow_state.json").exists()
    assert (out / "v1_5_full_flow_state.md").exists()


def test_write_full_flow_plan_contains_physical_contract(tmp_path):
    config = tmp_path / "config.json"
    config.write_text("{}", encoding="utf-8")
    plan = build_full_flow_plan(config_path=config, output_dir=tmp_path / "plan", run_id="demo")
    outputs = write_full_flow_plan(plan)

    text = outputs["markdown"].read_text(encoding="utf-8")
    assert "Existing internal coefficients affect displayed CO2/H2O" in text
    assert "CO2 fitting uses factory ratio evidence" in text
    assert "H2O fitting must use dewpoint/reference-backed water evidence" in text
    assert "updated model must be checked against independent open-flow verification points" in text


def test_command_list_quotes_placeholders_for_powershell(tmp_path):
    config = tmp_path / "config.json"
    config.write_text("{}", encoding="utf-8")
    plan = build_full_flow_plan(config_path=config, output_dir=tmp_path / "plan", run_id="demo")
    outputs = write_full_flow_plan(plan)

    commands = outputs["powershell"].read_text(encoding="utf-8")
    assert '"<co2_runner_queue.csv>"' in commands
    assert '"<h2o_runner_queue.csv>"' in commands


def test_empty_reviewer_and_approver_are_not_rendered_as_bare_flags(tmp_path):
    config = tmp_path / "config.json"
    config.write_text("{}", encoding="utf-8")
    plan = build_full_flow_plan(config_path=config, output_dir=tmp_path / "plan", run_id="demo")
    report_step = next(step for step in plan.steps if step.step_id == "zh_calibration_reports")

    command = list(report_step.command)
    assert "--reviewer" not in command
    assert "--approver" not in command


def test_initial_state_only_allows_first_offline_stage(tmp_path):
    config = tmp_path / "config.json"
    config.write_text("{}", encoding="utf-8")
    plan = build_full_flow_plan(
        config_path=config,
        output_dir=tmp_path / "plan",
        run_id="demo",
        operator="operator-a",
        analyzer_id="multi-device",
    )
    state = build_full_flow_state(plan)

    assert state.current_step_id == "load_plan_and_traceability"
    assert state.current_status == "ready"
    assert state.ready_step_ids == ("load_plan_and_traceability",)
    assert "device_identity_and_getco_snapshot" not in state.ready_step_ids
    second = next(stage for stage in state.stage_states if stage.step_id == "device_identity_and_getco_snapshot")
    assert second.status == "pending_previous_stage"


def test_resume_state_blocks_real_com_until_authorized(tmp_path):
    config = tmp_path / "config.json"
    config.write_text("{}", encoding="utf-8")
    plan = build_full_flow_plan(config_path=config, output_dir=tmp_path / "plan", run_id="demo")
    state = build_full_flow_state(plan, completed_steps=["load_plan_and_traceability"])

    assert state.current_step_id == "device_identity_and_getco_snapshot"
    assert state.current_status == "blocked_real_com_authorization"
    assert state.blocked_step_ids == ("device_identity_and_getco_snapshot",)
    stage = next(item for item in state.stage_states if item.step_id == "device_identity_and_getco_snapshot")
    assert stage.requires_real_com_authorization is True
    assert stage.opens_com_ports is True
    assert stage.writes_coefficients is False


def test_route_stage_remains_blocked_without_route_authorization(tmp_path):
    config = tmp_path / "config.json"
    config.write_text("{}", encoding="utf-8")
    co2_queue = tmp_path / "co2_queue.csv"
    co2_queue.write_text("component,temp_c\nco2,20\n", encoding="utf-8")
    plan = build_full_flow_plan(
        config_path=config,
        output_dir=tmp_path / "plan",
        run_id="demo",
        co2_queue_csv=co2_queue,
    )
    state = build_full_flow_state(
        plan,
        completed_steps=[
            "load_plan_and_traceability",
            "device_identity_and_getco_snapshot",
            "pressure_quick_check",
            "pressure_senco9_no_write_review",
            "temperature_channel_fast_review",
        ],
        allow_real_com=True,
        allow_pressure_control=True,
        allow_route_control=False,
    )

    assert state.current_step_id == "co2_open_flow_sampling"
    assert state.current_status == "blocked_route_authorization"
    co2_stage = next(item for item in state.stage_states if item.step_id == "co2_open_flow_sampling")
    assert co2_stage.requires_route_authorization is True
    assert co2_stage.controls_gas_route is True


def test_write_stage_is_never_auto_executable_from_state(tmp_path):
    config = tmp_path / "config.json"
    config.write_text("{}", encoding="utf-8")
    plan = build_full_flow_plan(config_path=config, output_dir=tmp_path / "plan", run_id="demo")
    completed = [
        step.step_id
        for step in plan.steps
        if step.step_id != "controlled_component_write_placeholder"
    ]
    state = build_full_flow_state(
        plan,
        completed_steps=completed,
        allow_real_com=True,
        allow_pressure_control=True,
        allow_route_control=True,
        allow_writes=True,
    )
    write_stage = next(item for item in state.stage_states if item.step_id == "controlled_component_write_placeholder")

    assert state.current_step_id == "controlled_component_write_placeholder"
    assert write_stage.status == "manual_review"
    assert write_stage.can_execute_now is False
    assert write_stage.writes_coefficients is True


def test_write_full_flow_state_outputs_resume_summary(tmp_path):
    config = tmp_path / "config.json"
    config.write_text("{}", encoding="utf-8")
    plan = build_full_flow_plan(config_path=config, output_dir=tmp_path / "plan", run_id="demo")
    state = build_full_flow_state(plan, completed_steps=["load_plan_and_traceability"])

    outputs = write_full_flow_state(state, tmp_path / "state")

    payload = json.loads(outputs["state_json"].read_text(encoding="utf-8"))
    assert payload["schema"] == "v1_5_full_calibration_flow_state_v0"
    assert payload["current_step_id"] == "device_identity_and_getco_snapshot"
    text = outputs["state_markdown"].read_text(encoding="utf-8")
    assert "real_COM_stage_requires_explicit_operator_authorization" in text


def test_supervised_run_is_planned_only_without_execute_flag(tmp_path):
    config = tmp_path / "config.json"
    config.write_text("{}", encoding="utf-8")
    plan = build_full_flow_plan(
        config_path=config,
        output_dir=tmp_path / "plan",
        run_id="demo",
        operator="operator-a",
        analyzer_id="multi-device",
    )

    result = run_supervised_full_flow(plan, execute_commands=False, output_dir=tmp_path / "exec")

    assert result.events[0].step_id == "load_plan_and_traceability"
    assert result.events[0].status == "planned_only"
    assert result.final_state.current_step_id == "load_plan_and_traceability"
    assert result.final_state.completed_step_ids == ()


def test_supervised_run_executes_ready_offline_step_and_stops_before_com(tmp_path):
    config = tmp_path / "config.json"
    config.write_text("{}", encoding="utf-8")
    plan = build_full_flow_plan(
        config_path=config,
        output_dir=tmp_path / "plan",
        run_id="demo",
        operator="operator-a",
        analyzer_id="multi-device",
    )
    first = replace(plan.steps[0], command=(sys.executable, "-c", "print('offline ok')"))
    plan = replace(plan, steps=(first, *plan.steps[1:]))

    result = run_supervised_full_flow(
        plan,
        execute_commands=True,
        max_steps=2,
        output_dir=tmp_path / "exec",
        cwd=tmp_path,
    )

    assert [event.status for event in result.events] == ["completed", "stopped"]
    assert result.final_state.completed_step_ids == ("load_plan_and_traceability",)
    assert result.final_state.current_step_id == "device_identity_and_getco_snapshot"
    assert result.final_state.current_status == "blocked_real_com_authorization"
    stdout = result.events[0].stdout_path
    assert stdout
    assert "offline ok" in open(stdout, encoding="utf-8").read()


def test_write_final_state_after_supervised_offline_success(tmp_path):
    config = tmp_path / "config.json"
    config.write_text("{}", encoding="utf-8")
    plan = build_full_flow_plan(
        config_path=config,
        output_dir=tmp_path / "plan",
        run_id="demo",
        operator="operator-a",
        analyzer_id="multi-device",
    )
    first = replace(plan.steps[0], command=(sys.executable, "-c", "print('offline ok')"))
    plan = replace(plan, steps=(first, *plan.steps[1:]))
    result = run_supervised_full_flow(
        plan,
        execute_commands=True,
        max_steps=1,
        output_dir=tmp_path / "exec",
        cwd=tmp_path,
    )

    outputs = write_full_flow_state(result.final_state, tmp_path / "state")
    payload = json.loads(outputs["state_json"].read_text(encoding="utf-8"))

    assert payload["completed_step_ids"] == ["load_plan_and_traceability"]
    assert payload["current_step_id"] == "device_identity_and_getco_snapshot"
    assert payload["current_status"] == "blocked_real_com_authorization"


def test_supervised_run_refuses_hazard_stage_even_when_state_authorized(tmp_path):
    config = tmp_path / "config.json"
    config.write_text("{}", encoding="utf-8")
    plan = build_full_flow_plan(config_path=config, output_dir=tmp_path / "plan", run_id="demo")

    result = run_supervised_full_flow(
        plan,
        completed_steps=["load_plan_and_traceability", "device_identity_and_getco_snapshot"],
        allow_real_com=True,
        allow_pressure_control=True,
        execute_commands=True,
        max_steps=1,
        output_dir=tmp_path / "exec",
    )

    assert result.events[0].step_id == "pressure_quick_check"
    assert result.events[0].status == "blocked_non_offline_stage"
    assert result.final_state.current_step_id == "pressure_quick_check"
    assert result.final_state.completed_step_ids == (
        "load_plan_and_traceability",
        "device_identity_and_getco_snapshot",
    )


def test_supervised_run_can_execute_read_only_com_stage_when_allowed(tmp_path):
    config = tmp_path / "config.json"
    config.write_text("{}", encoding="utf-8")
    plan = build_full_flow_plan(config_path=config, output_dir=tmp_path / "plan", run_id="demo")
    getco_index = [step.step_id for step in plan.steps].index("device_identity_and_getco_snapshot")
    fake_getco = replace(plan.steps[getco_index], command=(sys.executable, "-c", "print('read-only getco')"))
    plan = replace(
        plan,
        steps=(
            *plan.steps[:getco_index],
            fake_getco,
            *plan.steps[getco_index + 1 :],
        ),
    )

    result = run_supervised_full_flow(
        plan,
        completed_steps=["load_plan_and_traceability"],
        allow_real_com=True,
        execute_commands=True,
        max_steps=1,
        output_dir=tmp_path / "exec",
        cwd=tmp_path,
    )

    assert result.events[0].step_id == "device_identity_and_getco_snapshot"
    assert result.events[0].status == "completed"
    assert result.final_state.current_step_id == "pressure_quick_check"
    assert result.final_state.current_status == "blocked_pressure_authorization"
    assert result.final_state.completed_step_ids == (
        "load_plan_and_traceability",
        "device_identity_and_getco_snapshot",
    )


def test_write_full_flow_supervised_run_outputs_safety_summary(tmp_path):
    config = tmp_path / "config.json"
    config.write_text("{}", encoding="utf-8")
    plan = build_full_flow_plan(
        config_path=config,
        output_dir=tmp_path / "plan",
        run_id="demo",
        operator="operator-a",
        analyzer_id="multi-device",
    )
    result = run_supervised_full_flow(plan, execute_commands=False, output_dir=tmp_path / "exec")

    outputs = write_full_flow_supervised_run(result, tmp_path / "exec")

    payload = json.loads(outputs["supervised_json"].read_text(encoding="utf-8"))
    assert payload["schema"] == "v1_5_full_calibration_supervised_run_v0"
    text = outputs["supervised_markdown"].read_text(encoding="utf-8")
    assert "no gas/water route control" in text
    assert "planned_only" in text


def test_full_flow_cli_can_write_supervised_planned_only_report(tmp_path):
    config = tmp_path / "config.json"
    config.write_text("{}", encoding="utf-8")
    out = tmp_path / "flow"

    rc = cli_main(
        [
            "--config",
            str(config),
            "--output-dir",
            str(out),
            "--run-id",
            "demo",
            "--operator",
            "operator-a",
            "--supervised-run-ready-offline",
        ]
    )

    assert rc == 0
    supervised_json = out / "v1_5_full_flow_supervised_run.json"
    assert supervised_json.exists()
    payload = json.loads(supervised_json.read_text(encoding="utf-8"))
    assert payload["events"][0]["status"] == "planned_only"
