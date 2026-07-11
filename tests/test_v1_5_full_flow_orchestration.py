import json
import sys
from dataclasses import replace

from gas_calibrator.tools.run_v1_5_full_calibration_chain import main as cli_main
from gas_calibrator.validation.v1_5_canonical_evidence import write_canonical_v1_5_evidence_package
from gas_calibrator.v1_5.orchestration.full_flow import (
    LIVE_RUNNER_READINESS_SCHEMA,
    STAGE_MANIFEST_SCHEMA,
    build_full_flow_live_runner_readiness,
    build_full_flow_plan,
    build_full_flow_stage_manifest,
    build_full_flow_state,
    run_supervised_full_flow,
    write_full_flow_plan,
    write_full_flow_state,
    write_full_flow_supervised_run,
)


def _flag_value(command, flag):
    values = [str(part) for part in command]
    return values[values.index(flag) + 1]


def _pre_identity_offline_steps():
    return [
        "load_plan_and_traceability",
        "formal_initialization_contract_plan",
        "formal_initialization_executor_dry_run_snapshot",
        "formal_initialization_blocked_executor_snapshot",
        "formal_initialization_controlled_executor_design_snapshot",
        "formal_initialization_readonly_com_preflight_design_snapshot",
        "formal_initialization_readonly_com_preflight_blocked_executor_snapshot",
        "formal_initialization_readonly_com_preflight_controlled_executor_design_snapshot",
        "formal_initialization_readonly_com_preflight_controlled_blocked_executor_snapshot",
        "formal_readonly_com_execution_contract_snapshot",
        "formal_readonly_com_execution_blocked_executor_snapshot",
        "formal_readonly_com_execution_packet_validator_snapshot",
        "formal_readonly_com_execution_plan_preview_snapshot",
        "formal_readonly_com_minimal_executor_review_snapshot",
        "formal_readonly_com_minimal_executor_stub_snapshot",
        "initialization_readiness_snapshot",
        "pre_gas_readiness_snapshot",
    ]


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
    assert step_ids.index("load_plan_and_traceability") < step_ids.index("formal_initialization_contract_plan")
    assert step_ids.index("formal_initialization_contract_plan") < step_ids.index(
        "formal_initialization_executor_dry_run_snapshot"
    )
    assert step_ids.index("formal_initialization_executor_dry_run_snapshot") < step_ids.index(
        "formal_initialization_blocked_executor_snapshot"
    )
    assert step_ids.index("formal_initialization_blocked_executor_snapshot") < step_ids.index(
        "formal_initialization_controlled_executor_design_snapshot"
    )
    assert step_ids.index("formal_initialization_controlled_executor_design_snapshot") < step_ids.index(
        "formal_initialization_readonly_com_preflight_design_snapshot"
    )
    assert step_ids.index("formal_initialization_readonly_com_preflight_design_snapshot") < step_ids.index(
        "formal_initialization_readonly_com_preflight_blocked_executor_snapshot"
    )
    assert step_ids.index("formal_initialization_readonly_com_preflight_blocked_executor_snapshot") < step_ids.index(
        "formal_initialization_readonly_com_preflight_controlled_executor_design_snapshot"
    )
    assert step_ids.index(
        "formal_initialization_readonly_com_preflight_controlled_executor_design_snapshot"
    ) < step_ids.index(
        "formal_initialization_readonly_com_preflight_controlled_blocked_executor_snapshot"
    )
    assert step_ids.index(
        "formal_initialization_readonly_com_preflight_controlled_blocked_executor_snapshot"
    ) < step_ids.index(
        "formal_readonly_com_execution_contract_snapshot"
    )
    assert step_ids.index(
        "formal_readonly_com_execution_contract_snapshot"
    ) < step_ids.index(
        "formal_readonly_com_execution_blocked_executor_snapshot"
    )
    assert step_ids.index(
        "formal_readonly_com_execution_blocked_executor_snapshot"
    ) < step_ids.index(
        "formal_readonly_com_execution_packet_validator_snapshot"
    )
    assert step_ids.index(
        "formal_readonly_com_execution_packet_validator_snapshot"
    ) < step_ids.index(
        "formal_readonly_com_execution_plan_preview_snapshot"
    )
    assert step_ids.index(
        "formal_readonly_com_execution_plan_preview_snapshot"
    ) < step_ids.index(
        "formal_readonly_com_minimal_executor_review_snapshot"
    )
    assert step_ids.index(
        "formal_readonly_com_minimal_executor_review_snapshot"
    ) < step_ids.index(
        "formal_readonly_com_minimal_executor_stub_snapshot"
    )
    assert step_ids.index(
        "formal_readonly_com_minimal_executor_stub_snapshot"
    ) < step_ids.index(
        "initialization_readiness_snapshot"
    )
    assert step_ids.index("initialization_readiness_snapshot") < step_ids.index("pre_gas_readiness_snapshot")
    assert step_ids.index("pre_gas_readiness_snapshot") < step_ids.index("device_identity_and_getco_snapshot")
    assert step_ids.index("device_identity_and_getco_snapshot") < step_ids.index(
        "identity_getco_readiness_snapshot"
    )
    assert step_ids.index("identity_getco_readiness_snapshot") < step_ids.index(
        "auxiliary_senco56789_neutralization_gate"
    )
    assert step_ids.index("auxiliary_senco56789_neutralization_gate") < step_ids.index("pressure_quick_check")
    assert step_ids.index("pressure_quick_check") < step_ids.index("co2_open_flow_sampling")
    assert step_ids.index("pressure_quick_check") < step_ids.index("h2o_open_flow_sampling")
    assert step_ids.index("pressure_quick_check") < step_ids.index("pressure_senco9_no_write_acquisition")
    assert step_ids.index("pressure_senco9_no_write_acquisition") < step_ids.index("pressure_senco9_no_write_review")
    assert step_ids.index("pressure_senco9_no_write_review") < step_ids.index("pressure_channel_completion_audit")
    assert step_ids.index("pressure_channel_completion_audit") < step_ids.index(
        "batch_initialization_closeout_index"
    )
    assert step_ids.index("batch_initialization_closeout_index") < step_ids.index(
        "post_closeout_resume_gate_snapshot"
    )
    assert step_ids.index("post_closeout_resume_gate_snapshot") < step_ids.index(
        "post_closeout_resume_prefix_application_review"
    )
    assert step_ids.index("post_closeout_resume_prefix_application_review") < step_ids.index(
        "temperature_channel_fast_review"
    )
    assert step_ids.index("post_closeout_resume_gate_snapshot") < step_ids.index("co2_open_flow_sampling")
    assert step_ids.index("post_closeout_resume_gate_snapshot") < step_ids.index("h2o_open_flow_sampling")
    assert step_ids.index("temperature_channel_fast_review") < step_ids.index("co2_open_flow_sampling")
    assert step_ids.index("temperature_channel_fast_review") < step_ids.index("h2o_open_flow_sampling")
    assert step_ids.index("co2_open_flow_sampling") < step_ids.index("factory_signal_health_review")
    assert step_ids.index("h2o_open_flow_sampling") < step_ids.index("factory_signal_health_review")
    assert step_ids.index("factory_signal_health_review") < step_ids.index("fit_input_quality_review")
    assert step_ids.index("fit_input_quality_review") < step_ids.index("post_run_coefficient_executor")
    assert step_ids.index("h2o_open_flow_sampling") < step_ids.index("post_run_coefficient_executor")
    assert step_ids.index("post_run_coefficient_executor") < step_ids.index("full_flow_closure_readiness")
    assert step_ids.index("full_flow_closure_readiness") < step_ids.index("co2_candidate_write_review")
    assert step_ids.index("post_run_coefficient_executor") < step_ids.index("co2_candidate_write_review")
    assert step_ids.index("factory_signal_health_review") < step_ids.index("co2_candidate_write_review")
    assert step_ids.index("co2_candidate_write_review") < step_ids.index(
        "main_senco_write_precheck_authorization_gate"
    )
    assert step_ids.index("main_senco_write_precheck_authorization_gate") < step_ids.index(
        "controlled_component_write_placeholder"
    )
    assert step_ids.index("controlled_component_write_placeholder") < step_ids.index(
        "post_write_reverification_placeholder"
    )
    assert step_ids.index("post_write_reverification_placeholder") < step_ids.index("formal_evidence_sidecar")
    assert step_ids.index("formal_evidence_sidecar") < step_ids.index("formal_database_dry_run_snapshot")
    assert step_ids.index("formal_database_dry_run_snapshot") < step_ids.index(
        "formal_database_import_preflight_snapshot"
    )
    assert step_ids.index("formal_database_import_preflight_snapshot") < step_ids.index(
        "formal_database_import_authorization_snapshot"
    )
    assert step_ids.index("formal_database_import_authorization_snapshot") < step_ids.index(
        "formal_database_import_command_contract_snapshot"
    )
    assert step_ids.index("formal_database_import_command_contract_snapshot") < step_ids.index("database_import")
    assert step_ids.index("zh_calibration_reports") < step_ids.index("final_evidence_status_refresh")
    assert step_ids.index("final_evidence_status_refresh") < step_ids.index(
        "automation_control_contract_snapshot"
    )
    assert step_ids.index("automation_control_contract_snapshot") < step_ids.index(
        "algorithm_profile_runner_dry_run_snapshot"
    )
    assert step_ids.index("algorithm_profile_runner_dry_run_snapshot") < step_ids.index(
        "formal_run_status_snapshot"
    )
    assert plan.coefficient_epoch_contract["do_not_clear_existing_coefficients_on_startup"] is False
    assert (
        plan.coefficient_epoch_contract["clear_or_neutralize_auxiliary_groups_after_epoch0_snapshot"]
        == "SENCO5,SENCO6,SENCO7,SENCO8,SENCO9"
    )
    assert plan.coefficient_epoch_contract["identity_key"] == "analyzer_device_id_not_com_port_or_ga_alias"
    assert plan.physical_order[0] == "formal_initialization_contract_readiness_and_pre_gas_sidecar"


def test_full_flow_initialization_contract_stage_is_offline_only(tmp_path):
    config = tmp_path / "config.json"
    config.write_text("{}", encoding="utf-8")
    plan = build_full_flow_plan(
        config_path=config,
        output_dir=tmp_path / "plan",
        run_id="demo",
        operator="operator-a",
        analyzer_id="multi-device",
    )

    init_plan = next(step for step in plan.steps if step.step_id == "formal_initialization_contract_plan")
    init_executor = next(step for step in plan.steps if step.step_id == "formal_initialization_executor_dry_run_snapshot")
    init_blocked_executor = next(
        step for step in plan.steps if step.step_id == "formal_initialization_blocked_executor_snapshot"
    )
    init_controlled_design = next(
        step for step in plan.steps if step.step_id == "formal_initialization_controlled_executor_design_snapshot"
    )
    init_readonly_com_design = next(
        step
        for step in plan.steps
        if step.step_id == "formal_initialization_readonly_com_preflight_design_snapshot"
    )
    init_readonly_com_blocked_executor = next(
        step
        for step in plan.steps
        if step.step_id == "formal_initialization_readonly_com_preflight_blocked_executor_snapshot"
    )
    init_readonly_com_controlled_design = next(
        step
        for step in plan.steps
        if step.step_id == "formal_initialization_readonly_com_preflight_controlled_executor_design_snapshot"
    )
    init_readonly_com_controlled_blocked_executor = next(
        step
        for step in plan.steps
        if step.step_id == "formal_initialization_readonly_com_preflight_controlled_blocked_executor_snapshot"
    )
    readonly_com_execution_contract = next(
        step for step in plan.steps if step.step_id == "formal_readonly_com_execution_contract_snapshot"
    )
    readonly_com_execution_blocked_executor = next(
        step for step in plan.steps if step.step_id == "formal_readonly_com_execution_blocked_executor_snapshot"
    )
    readonly_com_execution_packet_validator = next(
        step for step in plan.steps if step.step_id == "formal_readonly_com_execution_packet_validator_snapshot"
    )
    readonly_com_execution_plan_preview = next(
        step for step in plan.steps if step.step_id == "formal_readonly_com_execution_plan_preview_snapshot"
    )
    readonly_com_minimal_executor_review = next(
        step for step in plan.steps if step.step_id == "formal_readonly_com_minimal_executor_review_snapshot"
    )
    readonly_com_minimal_executor_stub = next(
        step for step in plan.steps if step.step_id == "formal_readonly_com_minimal_executor_stub_snapshot"
    )
    readiness = next(step for step in plan.steps if step.step_id == "initialization_readiness_snapshot")
    pre_gas = next(step for step in plan.steps if step.step_id == "pre_gas_readiness_snapshot")
    init_command = list(init_plan.command)
    init_executor_command = list(init_executor.command)
    init_blocked_executor_command = list(init_blocked_executor.command)
    init_controlled_design_command = list(init_controlled_design.command)
    init_readonly_com_design_command = list(init_readonly_com_design.command)
    init_readonly_com_blocked_executor_command = list(init_readonly_com_blocked_executor.command)
    init_readonly_com_controlled_design_command = list(init_readonly_com_controlled_design.command)
    init_readonly_com_controlled_blocked_executor_command = list(
        init_readonly_com_controlled_blocked_executor.command
    )
    readonly_com_execution_contract_command = list(readonly_com_execution_contract.command)
    readonly_com_execution_blocked_executor_command = list(readonly_com_execution_blocked_executor.command)
    readonly_com_execution_packet_validator_command = list(readonly_com_execution_packet_validator.command)
    readonly_com_execution_plan_preview_command = list(readonly_com_execution_plan_preview.command)
    readonly_com_minimal_executor_review_command = list(readonly_com_minimal_executor_review.command)
    readonly_com_minimal_executor_stub_command = list(readonly_com_minimal_executor_stub.command)
    readiness_command = list(readiness.command)
    pre_gas_command = list(pre_gas.command)

    assert init_plan.execution_mode == "offline_sidecar"
    assert init_plan.opens_com_ports is False
    assert init_plan.writes_coefficients is False
    assert init_plan.writes_device_id is False
    assert init_plan.controls_gas_route is False
    assert init_plan.controls_water_route is False
    assert init_plan.tool_module == "gas_calibrator.tools.run_v1_5_formal_initialization_runner"
    assert "v1_5_formal_initialization_db_bundle.json" in " ".join(init_plan.expected_outputs)
    assert _flag_value(init_command, "--config") == str(config.resolve())
    assert _flag_value(init_command, "--output-dir") == str((tmp_path / "plan" / "formal_initialization").resolve())
    assert _flag_value(init_command, "--run-id") == "demo_initialization"
    assert _flag_value(init_command, "--operator") == "operator-a"
    assert "--execute" not in init_command
    assert "--execute-read-only-real-com" not in init_command
    assert "--execute-controlled-writes" not in init_command

    assert init_executor.execution_mode == "offline_sidecar"
    assert init_executor.opens_com_ports is False
    assert init_executor.writes_coefficients is False
    assert init_executor.writes_device_id is False
    assert init_executor.controls_gas_route is False
    assert init_executor.controls_water_route is False
    assert init_executor.tool_module == "gas_calibrator.tools.export_v1_5_formal_initialization_executor_dry_run"
    assert _flag_value(init_executor_command, "--formal-initialization-plan-json").endswith(
        "formal_initialization\\v1_5_formal_initialization_plan.json"
    )
    assert _flag_value(init_executor_command, "--output-dir") == str(
        (tmp_path / "plan" / "formal_initialization_executor_dry_run").resolve()
    )
    assert "v1_5_formal_initialization_executor_dry_run.json" in " ".join(init_executor.expected_outputs)
    assert "--execute" not in init_executor_command

    assert init_blocked_executor.execution_mode == "offline_sidecar"
    assert init_blocked_executor.opens_com_ports is False
    assert init_blocked_executor.writes_coefficients is False
    assert init_blocked_executor.writes_device_id is False
    assert init_blocked_executor.controls_pressure is False
    assert init_blocked_executor.controls_gas_route is False
    assert init_blocked_executor.controls_water_route is False
    assert init_blocked_executor.tool_module == "gas_calibrator.tools.run_v1_5_formal_initialization_blocked_executor"
    assert _flag_value(init_blocked_executor_command, "--formal-initialization-executor-dry-run-json").endswith(
        "formal_initialization_executor_dry_run\\v1_5_formal_initialization_executor_dry_run.json"
    )
    assert _flag_value(init_blocked_executor_command, "--formal-initialization-plan-json").endswith(
        "formal_initialization\\v1_5_formal_initialization_plan.json"
    )
    assert _flag_value(init_blocked_executor_command, "--output-dir") == str(
        (tmp_path / "plan" / "formal_initialization_blocked_executor").resolve()
    )
    assert "--fail-on-blocked" in init_blocked_executor_command
    assert "--execute" not in init_blocked_executor_command
    assert "--execute-read-only-real-com" not in init_blocked_executor_command
    assert "--execute-controlled-writes" not in init_blocked_executor_command
    assert "v1_5_formal_initialization_blocked_executor.json" in " ".join(
        init_blocked_executor.expected_outputs
    )

    assert init_controlled_design.execution_mode == "offline_sidecar"
    assert init_controlled_design.opens_com_ports is False
    assert init_controlled_design.writes_coefficients is False
    assert init_controlled_design.writes_device_id is False
    assert init_controlled_design.controls_pressure is False
    assert init_controlled_design.controls_gas_route is False
    assert init_controlled_design.controls_water_route is False
    assert init_controlled_design.tool_module == (
        "gas_calibrator.tools.export_v1_5_formal_initialization_controlled_executor_design"
    )
    assert _flag_value(init_controlled_design_command, "--formal-initialization-blocked-executor-json").endswith(
        "formal_initialization_blocked_executor\\v1_5_formal_initialization_blocked_executor.json"
    )
    assert _flag_value(init_controlled_design_command, "--output-dir") == str(
        (tmp_path / "plan" / "formal_initialization_controlled_executor_design").resolve()
    )
    assert "--execute-controlled-initialization" not in init_controlled_design_command
    assert "--execute-read-only-real-com" not in init_controlled_design_command
    assert "--execute-controlled-writes" not in init_controlled_design_command
    assert "v1_5_formal_initialization_controlled_executor_design.json" in " ".join(
        init_controlled_design.expected_outputs
    )

    assert init_readonly_com_design.execution_mode == "offline_sidecar"
    assert init_readonly_com_design.opens_com_ports is False
    assert init_readonly_com_design.writes_coefficients is False
    assert init_readonly_com_design.writes_device_id is False
    assert init_readonly_com_design.controls_pressure is False
    assert init_readonly_com_design.controls_gas_route is False
    assert init_readonly_com_design.controls_water_route is False
    assert init_readonly_com_design.tool_module == (
        "gas_calibrator.tools.export_v1_5_formal_initialization_readonly_com_preflight_design"
    )
    assert _flag_value(
        init_readonly_com_design_command,
        "--formal-initialization-controlled-executor-design-json",
    ).endswith(
        "formal_initialization_controlled_executor_design\\v1_5_formal_initialization_controlled_executor_design.json"
    )
    assert _flag_value(init_readonly_com_design_command, "--output-dir") == str(
        (tmp_path / "plan" / "formal_initialization_readonly_com_preflight_design").resolve()
    )
    assert "--execute-read-only-real-com" not in init_readonly_com_design_command
    assert "--execute-controlled-writes" not in init_readonly_com_design_command
    assert "v1_5_formal_initialization_readonly_com_preflight_design.json" in " ".join(
        init_readonly_com_design.expected_outputs
    )

    assert init_readonly_com_blocked_executor.execution_mode == "offline_sidecar"
    assert init_readonly_com_blocked_executor.opens_com_ports is False
    assert init_readonly_com_blocked_executor.writes_coefficients is False
    assert init_readonly_com_blocked_executor.writes_device_id is False
    assert init_readonly_com_blocked_executor.controls_pressure is False
    assert init_readonly_com_blocked_executor.controls_gas_route is False
    assert init_readonly_com_blocked_executor.controls_water_route is False
    assert init_readonly_com_blocked_executor.tool_module == (
        "gas_calibrator.tools.run_v1_5_formal_initialization_readonly_com_preflight_blocked_executor"
    )
    assert _flag_value(
        init_readonly_com_blocked_executor_command,
        "--formal-initialization-readonly-com-preflight-design-json",
    ).endswith(
        "formal_initialization_readonly_com_preflight_design\\v1_5_formal_initialization_readonly_com_preflight_design.json"
    )
    assert _flag_value(init_readonly_com_blocked_executor_command, "--output-dir") == str(
        (tmp_path / "plan" / "formal_initialization_readonly_com_preflight_blocked_executor").resolve()
    )
    assert "--fail-on-blocked" in init_readonly_com_blocked_executor_command
    assert "--execute" not in init_readonly_com_blocked_executor_command
    assert "--execute-read-only-real-com" not in init_readonly_com_blocked_executor_command
    assert "--allow-real-com" not in init_readonly_com_blocked_executor_command
    assert "--execute-controlled-writes" not in init_readonly_com_blocked_executor_command
    assert "v1_5_formal_initialization_readonly_com_preflight_blocked_executor.json" in " ".join(
        init_readonly_com_blocked_executor.expected_outputs
    )

    assert init_readonly_com_controlled_design.execution_mode == "offline_sidecar"
    assert init_readonly_com_controlled_design.opens_com_ports is False
    assert init_readonly_com_controlled_design.writes_coefficients is False
    assert init_readonly_com_controlled_design.writes_device_id is False
    assert init_readonly_com_controlled_design.controls_pressure is False
    assert init_readonly_com_controlled_design.controls_gas_route is False
    assert init_readonly_com_controlled_design.controls_water_route is False
    assert init_readonly_com_controlled_design.tool_module == (
        "gas_calibrator.tools.export_v1_5_formal_initialization_readonly_com_preflight_controlled_executor_design"
    )
    assert _flag_value(
        init_readonly_com_controlled_design_command,
        "--formal-initialization-readonly-com-preflight-blocked-executor-json",
    ).endswith(
        "formal_initialization_readonly_com_preflight_blocked_executor\\v1_5_formal_initialization_readonly_com_preflight_blocked_executor.json"
    )
    assert _flag_value(init_readonly_com_controlled_design_command, "--output-dir") == str(
        (tmp_path / "plan" / "formal_initialization_readonly_com_preflight_controlled_executor_design").resolve()
    )
    assert "--execute-read-only-real-com" not in init_readonly_com_controlled_design_command
    assert "--execute-controlled-writes" not in init_readonly_com_controlled_design_command
    assert "v1_5_formal_initialization_readonly_com_preflight_controlled_executor_design.json" in " ".join(
        init_readonly_com_controlled_design.expected_outputs
    )

    assert init_readonly_com_controlled_blocked_executor.execution_mode == "offline_sidecar"
    assert init_readonly_com_controlled_blocked_executor.opens_com_ports is False
    assert init_readonly_com_controlled_blocked_executor.writes_coefficients is False
    assert init_readonly_com_controlled_blocked_executor.writes_device_id is False
    assert init_readonly_com_controlled_blocked_executor.controls_pressure is False
    assert init_readonly_com_controlled_blocked_executor.controls_gas_route is False
    assert init_readonly_com_controlled_blocked_executor.controls_water_route is False
    assert init_readonly_com_controlled_blocked_executor.tool_module == (
        "gas_calibrator.tools.run_v1_5_formal_initialization_readonly_com_preflight_controlled_blocked_executor"
    )
    assert _flag_value(
        init_readonly_com_controlled_blocked_executor_command,
        "--formal-initialization-readonly-com-preflight-controlled-executor-design-json",
    ).endswith(
        "formal_initialization_readonly_com_preflight_controlled_executor_design\\v1_5_formal_initialization_readonly_com_preflight_controlled_executor_design.json"
    )
    assert _flag_value(init_readonly_com_controlled_blocked_executor_command, "--output-dir") == str(
        (tmp_path / "plan" / "formal_initialization_readonly_com_preflight_controlled_blocked_executor").resolve()
    )
    assert "--fail-on-blocked" in init_readonly_com_controlled_blocked_executor_command
    assert "--execute" not in init_readonly_com_controlled_blocked_executor_command
    assert "--execute-read-only-real-com" not in init_readonly_com_controlled_blocked_executor_command
    assert "--allow-real-com" not in init_readonly_com_controlled_blocked_executor_command
    assert "--execute-controlled-writes" not in init_readonly_com_controlled_blocked_executor_command
    assert "v1_5_formal_initialization_readonly_com_preflight_controlled_blocked_executor.json" in " ".join(
        init_readonly_com_controlled_blocked_executor.expected_outputs
    )

    assert readonly_com_execution_contract.execution_mode == "offline_sidecar"
    assert readonly_com_execution_contract.opens_com_ports is False
    assert readonly_com_execution_contract.writes_coefficients is False
    assert readonly_com_execution_contract.writes_device_id is False
    assert readonly_com_execution_contract.controls_pressure is False
    assert readonly_com_execution_contract.controls_gas_route is False
    assert readonly_com_execution_contract.controls_water_route is False
    assert readonly_com_execution_contract.tool_module == (
        "gas_calibrator.tools.export_v1_5_formal_readonly_com_execution_contract"
    )
    assert _flag_value(
        readonly_com_execution_contract_command,
        "--formal-initialization-readonly-com-preflight-controlled-blocked-executor-json",
    ).endswith(
        "formal_initialization_readonly_com_preflight_controlled_blocked_executor\\v1_5_formal_initialization_readonly_com_preflight_controlled_blocked_executor.json"
    )
    assert _flag_value(readonly_com_execution_contract_command, "--output-dir") == str(
        (tmp_path / "plan" / "formal_readonly_com_execution_contract").resolve()
    )
    assert "--fail-on-review-required" in readonly_com_execution_contract_command
    assert "--execute" not in readonly_com_execution_contract_command
    assert "--execute-read-only-real-com" not in readonly_com_execution_contract_command
    assert "--allow-real-com" not in readonly_com_execution_contract_command
    assert "--execute-controlled-writes" not in readonly_com_execution_contract_command
    assert "--operator-confirmation-text" not in readonly_com_execution_contract_command
    assert "--authorization-id" not in readonly_com_execution_contract_command
    assert "v1_5_formal_readonly_com_execution_contract.json" in " ".join(
        readonly_com_execution_contract.expected_outputs
    )

    assert readonly_com_execution_blocked_executor.execution_mode == "offline_sidecar"
    assert readonly_com_execution_blocked_executor.opens_com_ports is False
    assert readonly_com_execution_blocked_executor.writes_coefficients is False
    assert readonly_com_execution_blocked_executor.writes_device_id is False
    assert readonly_com_execution_blocked_executor.controls_pressure is False
    assert readonly_com_execution_blocked_executor.controls_gas_route is False
    assert readonly_com_execution_blocked_executor.controls_water_route is False
    assert readonly_com_execution_blocked_executor.tool_module == (
        "gas_calibrator.tools.run_v1_5_formal_readonly_com_execution_blocked_executor"
    )
    assert _flag_value(
        readonly_com_execution_blocked_executor_command,
        "--formal-readonly-com-execution-contract-json",
    ).endswith("formal_readonly_com_execution_contract\\v1_5_formal_readonly_com_execution_contract.json")
    assert _flag_value(readonly_com_execution_blocked_executor_command, "--output-dir") == str(
        (tmp_path / "plan" / "formal_readonly_com_execution_blocked_executor").resolve()
    )
    assert "--fail-on-blocked" in readonly_com_execution_blocked_executor_command
    assert "--execute" not in readonly_com_execution_blocked_executor_command
    assert "--execute-read-only-real-com" not in readonly_com_execution_blocked_executor_command
    assert "--allow-real-com" not in readonly_com_execution_blocked_executor_command
    assert "--execute-controlled-writes" not in readonly_com_execution_blocked_executor_command
    assert "--operator-confirmation-text" not in readonly_com_execution_blocked_executor_command
    assert "--authorization-id" not in readonly_com_execution_blocked_executor_command
    assert "v1_5_formal_readonly_com_execution_blocked_executor.json" in " ".join(
        readonly_com_execution_blocked_executor.expected_outputs
    )

    assert readonly_com_execution_packet_validator.execution_mode == "offline_sidecar"
    assert readonly_com_execution_packet_validator.opens_com_ports is False
    assert readonly_com_execution_packet_validator.writes_coefficients is False
    assert readonly_com_execution_packet_validator.writes_device_id is False
    assert readonly_com_execution_packet_validator.controls_pressure is False
    assert readonly_com_execution_packet_validator.controls_gas_route is False
    assert readonly_com_execution_packet_validator.controls_water_route is False
    assert readonly_com_execution_packet_validator.tool_module == (
        "gas_calibrator.tools.export_v1_5_formal_readonly_com_execution_packet_validator"
    )
    assert _flag_value(
        readonly_com_execution_packet_validator_command,
        "--formal-readonly-com-execution-blocked-executor-json",
    ).endswith(
        "formal_readonly_com_execution_blocked_executor\\v1_5_formal_readonly_com_execution_blocked_executor.json"
    )
    assert _flag_value(readonly_com_execution_packet_validator_command, "--output-dir") == str(
        (tmp_path / "plan" / "formal_readonly_com_execution_packet_validator").resolve()
    )
    assert "--execute" not in readonly_com_execution_packet_validator_command
    assert "--execute-read-only-real-com" not in readonly_com_execution_packet_validator_command
    assert "--allow-real-com" not in readonly_com_execution_packet_validator_command
    assert "--execute-controlled-writes" not in readonly_com_execution_packet_validator_command
    assert "--operator-confirmation-text" not in readonly_com_execution_packet_validator_command
    assert "--authorization-id" not in readonly_com_execution_packet_validator_command
    assert "--authorization-packet-json" not in readonly_com_execution_packet_validator_command
    assert "--reviewed-port-inventory-json" not in readonly_com_execution_packet_validator_command
    assert "--active-analyzer-list-json" not in readonly_com_execution_packet_validator_command
    assert "v1_5_formal_readonly_com_execution_packet_validator.json" in " ".join(
        readonly_com_execution_packet_validator.expected_outputs
    )

    assert readonly_com_execution_plan_preview.execution_mode == "offline_sidecar"
    assert readonly_com_execution_plan_preview.opens_com_ports is False
    assert readonly_com_execution_plan_preview.writes_coefficients is False
    assert readonly_com_execution_plan_preview.writes_device_id is False
    assert readonly_com_execution_plan_preview.controls_pressure is False
    assert readonly_com_execution_plan_preview.controls_gas_route is False
    assert readonly_com_execution_plan_preview.controls_water_route is False
    assert readonly_com_execution_plan_preview.tool_module == (
        "gas_calibrator.tools.export_v1_5_formal_readonly_com_execution_plan_preview"
    )
    assert _flag_value(
        readonly_com_execution_plan_preview_command,
        "--formal-readonly-com-execution-packet-validator-json",
    ).endswith(
        "formal_readonly_com_execution_packet_validator\\v1_5_formal_readonly_com_execution_packet_validator.json"
    )
    assert _flag_value(readonly_com_execution_plan_preview_command, "--output-dir") == str(
        (tmp_path / "plan" / "formal_readonly_com_execution_plan_preview").resolve()
    )
    assert "--execute" not in readonly_com_execution_plan_preview_command
    assert "--execute-read-only-real-com" not in readonly_com_execution_plan_preview_command
    assert "--allow-real-com" not in readonly_com_execution_plan_preview_command
    assert "--execute-controlled-writes" not in readonly_com_execution_plan_preview_command
    assert "--operator-confirmation-text" not in readonly_com_execution_plan_preview_command
    assert "--authorization-id" not in readonly_com_execution_plan_preview_command
    assert "--authorization-packet-json" not in readonly_com_execution_plan_preview_command
    assert "--reviewed-port-inventory-json" not in readonly_com_execution_plan_preview_command
    assert "--active-analyzer-list-json" not in readonly_com_execution_plan_preview_command
    assert "v1_5_formal_readonly_com_execution_plan_preview.json" in " ".join(
        readonly_com_execution_plan_preview.expected_outputs
    )

    assert readonly_com_minimal_executor_review.execution_mode == "offline_sidecar"
    assert readonly_com_minimal_executor_review.opens_com_ports is False
    assert readonly_com_minimal_executor_review.writes_coefficients is False
    assert readonly_com_minimal_executor_review.writes_device_id is False
    assert readonly_com_minimal_executor_review.controls_pressure is False
    assert readonly_com_minimal_executor_review.controls_gas_route is False
    assert readonly_com_minimal_executor_review.controls_water_route is False
    assert readonly_com_minimal_executor_review.tool_module == (
        "gas_calibrator.tools.export_v1_5_formal_readonly_com_minimal_executor_review"
    )
    assert _flag_value(
        readonly_com_minimal_executor_review_command,
        "--formal-readonly-com-execution-plan-preview-json",
    ).endswith(
        "formal_readonly_com_execution_plan_preview\\v1_5_formal_readonly_com_execution_plan_preview.json"
    )
    assert "--execute" not in readonly_com_minimal_executor_review_command
    assert "--execute-read-only-real-com" not in readonly_com_minimal_executor_review_command
    assert "--allow-real-com" not in readonly_com_minimal_executor_review_command
    assert "v1_5_formal_readonly_com_minimal_executor_review.json" in " ".join(
        readonly_com_minimal_executor_review.expected_outputs
    )

    assert readonly_com_minimal_executor_stub.execution_mode == "offline_sidecar"
    assert readonly_com_minimal_executor_stub.opens_com_ports is False
    assert readonly_com_minimal_executor_stub.writes_coefficients is False
    assert readonly_com_minimal_executor_stub.writes_device_id is False
    assert readonly_com_minimal_executor_stub.controls_pressure is False
    assert readonly_com_minimal_executor_stub.controls_gas_route is False
    assert readonly_com_minimal_executor_stub.controls_water_route is False
    assert readonly_com_minimal_executor_stub.tool_module == (
        "gas_calibrator.tools.run_v1_5_formal_readonly_com_minimal_executor_stub"
    )
    assert _flag_value(
        readonly_com_minimal_executor_stub_command,
        "--formal-readonly-com-minimal-executor-review-json",
    ).endswith(
        "formal_readonly_com_minimal_executor_review\\v1_5_formal_readonly_com_minimal_executor_review.json"
    )
    assert _flag_value(readonly_com_minimal_executor_stub_command, "--output-dir") == str(
        (tmp_path / "plan" / "formal_readonly_com_minimal_executor_stub").resolve()
    )
    assert "--fail-on-blocked" in readonly_com_minimal_executor_stub_command
    assert "--execute" not in readonly_com_minimal_executor_stub_command
    assert "--execute-read-only-real-com" not in readonly_com_minimal_executor_stub_command
    assert "--allow-real-com" not in readonly_com_minimal_executor_stub_command
    assert "--operator-confirmation-text" not in readonly_com_minimal_executor_stub_command
    assert "--authorization-id" not in readonly_com_minimal_executor_stub_command
    assert "--reviewed-port-inventory-json" not in readonly_com_minimal_executor_stub_command
    assert "--active-analyzer-list-json" not in readonly_com_minimal_executor_stub_command
    assert "v1_5_formal_readonly_com_minimal_executor_stub.json" in " ".join(
        readonly_com_minimal_executor_stub.expected_outputs
    )

    assert readiness.execution_mode == "offline_sidecar"
    assert readiness.opens_com_ports is False
    assert readiness.writes_coefficients is False
    assert readiness.tool_module == "gas_calibrator.tools.export_v1_5_initialization_readiness"
    assert _flag_value(readiness_command, "--run-dir") == str(
        (tmp_path / "plan" / "formal_initialization").resolve()
    )
    assert _flag_value(readiness_command, "--config") == str(config.resolve())
    assert "v1_5_initialization_database_sidecar.json" in " ".join(readiness.expected_outputs)

    assert pre_gas.execution_mode == "offline_sidecar"
    assert pre_gas.opens_com_ports is False
    assert pre_gas.controls_gas_route is False
    assert pre_gas.controls_water_route is False
    assert pre_gas.writes_coefficients is False
    assert pre_gas.tool_module == "gas_calibrator.tools.export_v1_5_pre_gas_readiness"
    assert _flag_value(pre_gas_command, "--run-dir") == str((tmp_path / "plan").resolve())
    assert _flag_value(pre_gas_command, "--initialization-dir") == str(
        (tmp_path / "plan" / "formal_initialization").resolve()
    )
    assert _flag_value(pre_gas_command, "--output-dir") == str((tmp_path / "plan" / "pre_gas_readiness").resolve())
    assert "v1_5_pre_gas_readiness_checks.csv" in " ".join(pre_gas.expected_outputs)
    assert "SENCO9 pressure completion" in pre_gas.physical_meaning


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
    assert [step.step_id for step in write_steps] == [
        "auxiliary_senco56789_neutralization_gate",
        "controlled_component_write_placeholder",
    ]
    assert all(step.execution_mode == "blocked_pending_explicit_authorization" for step in write_steps)
    assert all(step.command == () for step in write_steps)


def test_full_flow_stage_manifest_makes_automation_boundaries_explicit(tmp_path):
    config = tmp_path / "config.json"
    config.write_text("{}", encoding="utf-8")
    plan = build_full_flow_plan(
        config_path=config,
        output_dir=tmp_path / "plan",
        run_id="demo",
        operator="operator-a",
        analyzer_id="multi-device",
    )

    manifest = build_full_flow_stage_manifest(plan)
    by_step = {stage.step_id: stage for stage in manifest.stages}

    assert manifest.schema == STAGE_MANIFEST_SCHEMA
    assert manifest.one_button_live_runner_ready is False
    assert manifest.safety_summary["does_not_modify_run_app"] is True
    assert manifest.safety_summary["planner_opens_com_ports"] is False
    assert manifest.safety_summary["planner_writes_coefficients"] is False
    assert manifest.safety_summary["identity_key"] == "analyzer_device_id_not_com_port_or_ga_alias"
    assert manifest.automation_summary["blocked_controlled_write"] == 2

    init_plan = by_step["formal_initialization_contract_plan"]
    init_readiness = by_step["initialization_readiness_snapshot"]
    assert init_plan.automation_state == "offline_review_auto_candidate"
    assert init_readiness.automation_state == "offline_review_auto_candidate"
    assert init_plan.authorization_required["real_com"] is False
    assert init_readiness.authorization_required["real_com"] is False
    assert init_plan.authorization_required["coefficient_write"] is False
    assert init_readiness.authorization_required["coefficient_write"] is False
    assert "v1_5_formal_initialization_db_bundle.json" in " ".join(init_plan.expected_outputs)
    assert "v1_5_initialization_database_sidecar.json" in " ".join(init_readiness.expected_outputs)

    identity = by_step["device_identity_and_getco_snapshot"]
    assert identity.automation_state == "read_only_real_com_requires_authorization"
    assert identity.authorization_required["real_com"] is True
    assert identity.evidence_contract["readback_required"] is True
    getco_readiness = by_step["identity_getco_readiness_snapshot"]
    assert getco_readiness.automation_state == "offline_review_auto_candidate"
    assert getco_readiness.authorization_required["real_com"] is False
    assert getco_readiness.authorization_required["coefficient_write"] is False
    assert "v1_5_getco_identity_readiness.json" in " ".join(getco_readiness.expected_outputs)

    co2 = by_step["co2_open_flow_sampling"]
    h2o = by_step["h2o_open_flow_sampling"]
    assert co2.automation_state == "dedicated_open_flow_runner_requires_authorization"
    assert h2o.automation_state == "dedicated_open_flow_runner_requires_authorization"
    assert co2.authorization_required["route_control"] is True
    assert h2o.authorization_required["route_control"] is True
    assert co2.evidence_contract["raw_frames_preserved"] is True
    assert h2o.evidence_contract["raw_frames_preserved"] is True
    assert co2.evidence_contract["reject_reasons_required"] is True
    assert h2o.evidence_contract["reject_reasons_required"] is True

    pressure = by_step["pressure_senco9_no_write_acquisition"]
    assert pressure.automation_state == "dedicated_pressure_runner_requires_authorization"
    assert pressure.authorization_required["pressure_control"] is True
    assert pressure.authorization_required["coefficient_write"] is False

    pressure_completion = by_step["pressure_channel_completion_audit"]
    assert pressure_completion.automation_state == "offline_review_waiting_for_run_artifacts"
    assert pressure_completion.authorization_required["real_com"] is False
    assert pressure_completion.authorization_required["coefficient_write"] is False
    assert "pressure_channel_completion_summary.csv" in " ".join(pressure_completion.expected_outputs)

    batch_closeout = by_step["batch_initialization_closeout_index"]
    assert batch_closeout.automation_state == "offline_review_auto_candidate"
    assert batch_closeout.authorization_required["real_com"] is False
    assert batch_closeout.authorization_required["pressure_control"] is False
    assert batch_closeout.authorization_required["route_control"] is False
    assert batch_closeout.authorization_required["coefficient_write"] is False
    assert batch_closeout.safety_boundaries["opens_com_ports"] is False
    assert batch_closeout.safety_boundaries["writes_device_id"] is False
    assert batch_closeout.safety_boundaries["writes_coefficients"] is False
    assert "v1_5_batch_initialization_closeout_index.json" in " ".join(batch_closeout.expected_outputs)

    resume_gate = by_step["post_closeout_resume_gate_snapshot"]
    assert resume_gate.automation_state == "offline_review_auto_candidate"
    assert resume_gate.authorization_required["real_com"] is False
    assert resume_gate.authorization_required["pressure_control"] is False
    assert resume_gate.authorization_required["route_control"] is False
    assert resume_gate.authorization_required["coefficient_write"] is False
    assert resume_gate.safety_boundaries["opens_com_ports"] is False
    assert resume_gate.safety_boundaries["writes_device_id"] is False
    assert resume_gate.safety_boundaries["writes_coefficients"] is False
    assert "v1_5_post_closeout_resume_gate.json" in " ".join(resume_gate.expected_outputs)

    artifact_authorization = by_step["main_senco_write_precheck_authorization_gate"]
    authorization_command = list(artifact_authorization.command)
    assert artifact_authorization.automation_state == "offline_review_waiting_for_run_artifacts"
    assert artifact_authorization.authorization_required["coefficient_write"] is False
    assert artifact_authorization.safety_boundaries["opens_com_ports"] is False
    assert artifact_authorization.safety_boundaries["writes_coefficients"] is False
    assert "--execute" not in authorization_command
    assert "--authorized-writer-scope" in authorization_command
    assert "<authorized_writer_scope>" in authorization_command
    assert "co2_senco5_linear" not in authorization_command
    assert "h2o_senco6_linear" not in authorization_command
    assert "--authorized-device-id" in authorization_command
    assert "<authorized_device_id>" in authorization_command
    assert "main_senco_artifact_authorization.json" in " ".join(
        artifact_authorization.expected_outputs
    )

    write = by_step["controlled_component_write_placeholder"]
    assert write.automation_state == "blocked_controlled_write"
    assert write.authorization_required["coefficient_write"] is True
    assert write.evidence_contract["post_write_reverify_required"] is True


def test_full_flow_live_runner_readiness_lists_controlled_live_gates(tmp_path):
    config = tmp_path / "config.json"
    config.write_text("{}", encoding="utf-8")
    plan = build_full_flow_plan(
        config_path=config,
        output_dir=tmp_path / "plan",
        run_id="demo",
        operator="operator-a",
        analyzer_id="multi-device",
    )

    readiness = build_full_flow_live_runner_readiness(plan)
    domains = {domain.domain: domain for domain in readiness.domains}

    assert readiness.schema == LIVE_RUNNER_READINESS_SCHEMA
    assert readiness.one_button_live_runner_ready is False
    assert readiness.current_automation_level == "supervised_tool_chain_with_controlled_live_gates"
    assert readiness.ready_domains == ("offline_planning", "initialization_contract")
    assert set(readiness.required_authorizations) == {
        "real_com",
        "pressure_control",
        "route_control",
        "coefficient_write",
    }
    assert {"identity_and_epoch0", "pressure_channel", "co2_open_flow", "h2o_open_flow"}.issubset(
        set(readiness.blocked_domains)
    )
    assert domains["initialization_contract"].status == "ready_offline_supervised"
    assert domains["initialization_contract"].stage_ids == (
        "formal_initialization_contract_plan",
        "formal_initialization_executor_dry_run_snapshot",
        "formal_initialization_blocked_executor_snapshot",
        "formal_initialization_controlled_executor_design_snapshot",
        "formal_initialization_readonly_com_preflight_design_snapshot",
        "formal_initialization_readonly_com_preflight_blocked_executor_snapshot",
        "formal_initialization_readonly_com_preflight_controlled_executor_design_snapshot",
        "formal_initialization_readonly_com_preflight_controlled_blocked_executor_snapshot",
        "formal_readonly_com_execution_contract_snapshot",
        "formal_readonly_com_execution_blocked_executor_snapshot",
        "formal_readonly_com_execution_packet_validator_snapshot",
        "formal_readonly_com_execution_plan_preview_snapshot",
        "formal_readonly_com_minimal_executor_review_snapshot",
        "formal_readonly_com_minimal_executor_stub_snapshot",
        "initialization_readiness_snapshot",
        "pre_gas_readiness_snapshot",
    )
    assert domains["identity_and_epoch0"].stage_ids == (
        "device_identity_and_getco_snapshot",
        "identity_getco_readiness_snapshot",
    )
    assert "PostgreSQL 18" in domains["initialization_contract"].reason
    assert "verify the no-write epoch-0 evidence offline" in domains["identity_and_epoch0"].next_action
    assert domains["pressure_channel"].status == "requires_pressure_authorization"
    assert domains["pressure_channel"].required_authorizations == ("real_com", "pressure_control")
    assert "pressure P" in domains["pressure_channel"].reason
    assert domains["co2_open_flow"].required_authorizations == ("real_com", "route_control")
    assert "gas route remains open" in domains["co2_open_flow"].next_action
    assert domains["h2o_open_flow"].required_authorizations == ("real_com", "route_control")
    assert "continuous HGEN" in domains["h2o_open_flow"].next_action
    assert domains["candidate_fit_and_qc"].status == "offline_review_waiting_for_run_artifacts"
    assert "CO2 zero gas and H2O dry-gas anchors" in domains["candidate_fit_and_qc"].next_action
    assert domains["controlled_write_and_reverify"].status == "blocked_controlled_write"
    assert "coefficient_write" in domains["controlled_write_and_reverify"].required_authorizations


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
    getco_readiness = next(step for step in plan.steps if step.step_id == "identity_getco_readiness_snapshot")
    readiness_command = list(getco_readiness.command)
    assert getco_readiness.execution_mode == "offline_sidecar"
    assert getco_readiness.opens_com_ports is False
    assert getco_readiness.writes_coefficients is False
    assert readiness_command[readiness_command.index("--getco-dir") + 1] == str(
        (tmp_path / "plan" / "coefficient_epoch_0_getco_snapshot").resolve()
    )
    assert readiness_command[readiness_command.index("--output-dir") + 1] == str(
        (tmp_path / "plan" / "identity_getco_readiness").resolve()
    )
    assert "--fail-on-not-ready" in readiness_command
    aux = next(step for step in plan.steps if step.step_id == "auxiliary_senco56789_neutralization_gate")
    assert aux.opens_com_ports is True
    assert aux.writes_coefficients is True
    assert aux.command == ()
    assert aux.coefficient_epoch_event == "start_epoch_auxiliary_neutralized_after_epoch_0"
    assert "run_v1_5_temperature_senco78_neutral_controlled_write" in " ".join(aux.notes)


def test_full_flow_plan_preserves_validated_co2_h2o_route_contracts(tmp_path):
    config = tmp_path / "config.json"
    config.write_text("{}", encoding="utf-8")
    plan = build_full_flow_plan(config_path=config, output_dir=tmp_path / "plan", run_id="demo")

    co2 = next(item for item in plan.steps if item.step_id == "co2_open_flow_sampling")
    h2o = next(item for item in plan.steps if item.step_id == "h2o_open_flow_sampling")
    co2_command = list(co2.command)
    h2o_command = list(h2o.command)

    assert _flag_value(co2_command, "--temperature-order") == "desc"
    assert _flag_value(h2o_command, "--temperature-order") == "asc"
    assert _flag_value(co2_command, "--analyzer-acquisition") == "active_stream_1hz"
    assert _flag_value(h2o_command, "--analyzer-acquisition") == "active_stream_1hz"
    assert _flag_value(h2o_command, "--h2o-pressure-presample-policy") == "skip"
    assert "--skip-stability-gate" not in co2_command
    assert "--co2-ratio-f-preseal-policy" not in co2_command
    assert "--skip-dewpoint-gate" not in h2o_command
    assert "--skip-humidity-generator-gate" not in h2o_command
    assert "--pressure-diagnostic-only" not in h2o_command
    assert "--no-control-temperature" not in co2_command
    assert "--no-control-temperature" not in h2o_command


def test_full_flow_plan_requires_factory_signal_health_before_fit_review(tmp_path):
    config = tmp_path / "config.json"
    config.write_text("{}", encoding="utf-8")
    plan = build_full_flow_plan(config_path=config, output_dir=tmp_path / "plan", run_id="demo")

    step = next(item for item in plan.steps if item.step_id == "factory_signal_health_review")
    command = list(step.command)

    assert step.tool_module == "gas_calibrator.tools.export_v1_5_factory_signal_health_review"
    assert step.execution_mode == "offline_review"
    assert step.gate == "required_before_component_write_review"
    assert _flag_value(command, "--point-means-csv") == "<offline_fit_point_means.csv>"
    assert _flag_value(command, "--residuals-csv") == "<candidate_fit_residuals.csv>"
    assert "factory_signal_health_summary.csv" in step.expected_outputs
    assert "pass_factory_signal_health" in " ".join(step.notes)
    assert "SETILLUM no-argument readback is not treated as numeric evidence" in " ".join(step.notes)

    fit_review = next(item for item in plan.steps if item.step_id == "fit_input_quality_review")
    assert fit_review.gate == "requires_factory_signal_health_review"
    assert "v1_5_fit_input_quality_summary.csv" in fit_review.expected_outputs
    assert "v1_5_fit_input_quality_devices.csv" in fit_review.expected_outputs


def test_full_flow_plan_binds_final_batch_closeout_before_mature_open_flow(tmp_path):
    config = tmp_path / "config.json"
    config.write_text("{}", encoding="utf-8")
    plan = build_full_flow_plan(config_path=config, output_dir=tmp_path / "plan", run_id="demo")

    step = next(item for item in plan.steps if item.step_id == "batch_initialization_closeout_index")
    command = list(step.command)

    assert step.tool_module == "gas_calibrator.tools.export_v1_5_batch_initialization_closeout_index"
    assert step.execution_mode == "offline_sidecar"
    assert step.gate == "required_after_pressure_completion_before_mature_open_flow"
    assert step.opens_com_ports is False
    assert step.controls_pressure is False
    assert step.controls_gas_route is False
    assert step.controls_water_route is False
    assert step.writes_device_id is False
    assert step.writes_coefficients is False
    assert _flag_value(command, "--readonly-com-executor-json") == str(
        (
            tmp_path
            / "plan"
            / "formal_readonly_com_minimal_executor"
            / "v1_5_formal_readonly_com_minimal_executor.json"
        ).resolve()
    )
    assert _flag_value(command, "--readonly-identity-getco-snapshot-json") == str(
        (tmp_path / "plan" / "coefficient_epoch_0_getco_snapshot" / "old_component_coefficients_snapshot.json").resolve()
    )
    assert _flag_value(command, "--pressure-device-readiness-csv") == str(
        (
            tmp_path
            / "plan"
            / "pressure_channel"
            / "pressure_channel_completion"
            / "pressure_channel_device_readiness.csv"
        ).resolve()
    )
    assert _flag_value(command, "--route-readiness-json") == str(
        (tmp_path / "plan" / "formal_initialization" / "formal_route_readiness.json").resolve()
    )
    assert _flag_value(command, "--pre-gas-readiness-json") == str(
        (tmp_path / "plan" / "pre_gas_readiness" / "v1_5_pre_gas_readiness.json").resolve()
    )
    assert "--fail-on-review-required" in command
    assert "batch_initialization_closeout_index/v1_5_batch_initialization_closeout_index.json" in step.expected_outputs

    resume = next(item for item in plan.steps if item.step_id == "post_closeout_resume_gate_snapshot")
    resume_command = list(resume.command)
    assert resume.tool_module == "gas_calibrator.tools.export_v1_5_post_closeout_resume_gate"
    assert resume.execution_mode == "offline_sidecar"
    assert resume.gate == "required_after_batch_closeout_before_resume_state_application"
    assert resume.opens_com_ports is False
    assert resume.controls_pressure is False
    assert resume.controls_gas_route is False
    assert resume.controls_water_route is False
    assert resume.writes_device_id is False
    assert resume.writes_coefficients is False
    assert _flag_value(resume_command, "--full-flow-plan-json") == str(
        (tmp_path / "plan" / "v1_5_full_flow_plan.json").resolve()
    )
    assert _flag_value(resume_command, "--batch-initialization-closeout-json") == str(
        (
            tmp_path
            / "plan"
            / "batch_initialization_closeout_index"
            / "v1_5_batch_initialization_closeout_index.json"
        ).resolve()
    )
    assert "--fail-on-blocked" in resume_command
    assert "post_closeout_resume_gate/v1_5_post_closeout_resume_gate.json" in resume.expected_outputs

    application_review = next(
        item for item in plan.steps if item.step_id == "post_closeout_resume_prefix_application_review"
    )
    application_command = list(application_review.command)
    assert application_review.tool_module == "gas_calibrator.tools.export_v1_5_resume_prefix_application_review"
    assert application_review.execution_mode == "offline_sidecar"
    assert application_review.gate == "required_before_authoritative_resume_state_application"
    assert application_review.opens_com_ports is False
    assert application_review.controls_pressure is False
    assert application_review.controls_gas_route is False
    assert application_review.controls_water_route is False
    assert application_review.writes_device_id is False
    assert application_review.writes_coefficients is False
    assert _flag_value(application_command, "--full-flow-plan-json") == str(
        (tmp_path / "plan" / "v1_5_full_flow_plan.json").resolve()
    )
    assert _flag_value(application_command, "--post-closeout-resume-gate-json") == str(
        (
            tmp_path
            / "plan"
            / "post_closeout_resume_gate"
            / "v1_5_post_closeout_resume_gate.json"
        ).resolve()
    )
    assert "--fail-on-blocked" in application_command
    assert (
        "resume_prefix_application_review/v1_5_resume_prefix_application_review.json"
        in application_review.expected_outputs
    )

    writer_design = next(
        item for item in plan.steps if item.step_id == "authoritative_resume_state_writer_design"
    )
    writer_command = list(writer_design.command)
    assert writer_design.tool_module == (
        "gas_calibrator.tools.export_v1_5_authoritative_resume_state_writer_design"
    )
    assert writer_design.execution_mode == "offline_sidecar"
    assert writer_design.gate == "required_before_authoritative_resume_state_writer_implementation"
    assert writer_design.opens_com_ports is False
    assert writer_design.controls_pressure is False
    assert writer_design.controls_gas_route is False
    assert writer_design.controls_water_route is False
    assert writer_design.writes_device_id is False
    assert writer_design.writes_coefficients is False
    assert _flag_value(writer_command, "--full-flow-plan-json") == str(
        (tmp_path / "plan" / "v1_5_full_flow_plan.json").resolve()
    )
    assert _flag_value(writer_command, "--resume-prefix-application-review-json") == str(
        (
            tmp_path
            / "plan"
            / "resume_prefix_application_review"
            / "v1_5_resume_prefix_application_review.json"
        ).resolve()
    )
    assert "--fail-on-blocked" in writer_command
    assert (
        "authoritative_resume_state_writer_design/v1_5_authoritative_resume_state_writer_design.json"
        in writer_design.expected_outputs
    )

    blocked_writer = next(
        item
        for item in plan.steps
        if item.step_id == "authoritative_resume_state_writer_blocked_executor"
    )
    blocked_command = list(blocked_writer.command)
    assert blocked_writer.tool_module == (
        "gas_calibrator.tools.run_v1_5_authoritative_resume_state_writer_blocked_executor"
    )
    assert blocked_writer.execution_mode == "offline_blocked_stub"
    assert blocked_writer.opens_com_ports is False
    assert blocked_writer.controls_pressure is False
    assert blocked_writer.controls_gas_route is False
    assert blocked_writer.controls_water_route is False
    assert blocked_writer.writes_device_id is False
    assert blocked_writer.writes_coefficients is False
    assert _flag_value(blocked_command, "--full-flow-plan-json") == str(
        (tmp_path / "plan" / "v1_5_full_flow_plan.json").resolve()
    )
    assert _flag_value(
        blocked_command, "--authoritative-resume-state-writer-design-json"
    ) == str(
        (
            tmp_path
            / "plan"
            / "authoritative_resume_state_writer_design"
            / "v1_5_authoritative_resume_state_writer_design.json"
        ).resolve()
    )
    assert "--fail-on-blocked" in blocked_command
    assert (
        "authoritative_resume_state_writer_blocked_executor/v1_5_authoritative_resume_state_writer_blocked_executor.json"
        in blocked_writer.expected_outputs
    )


def test_full_flow_plan_adds_no_write_post_run_coefficient_executor(tmp_path):
    config = tmp_path / "config.json"
    config.write_text("{}", encoding="utf-8")
    plan = build_full_flow_plan(
        config_path=config,
        output_dir=tmp_path / "plan",
        run_id="demo",
        reviewed_run_dir=tmp_path / "reviewed_run",
    )

    step = next(item for item in plan.steps if item.step_id == "post_run_coefficient_executor")
    command = list(step.command)

    assert step.tool_module == "gas_calibrator.tools.export_v1_5_post_run_coefficient_executor"
    assert step.execution_mode == "offline_review"
    assert step.gate == "required_after_component_acquisition_before_controlled_write"
    assert step.opens_com_ports is False
    assert step.controls_gas_route is False
    assert step.controls_water_route is False
    assert step.writes_coefficients is False
    assert _flag_value(command, "--run-dir") == str((tmp_path / "reviewed_run").resolve())
    assert _flag_value(command, "--output-dir") == str(
        (tmp_path / "plan" / "post_run_coefficient_executor").resolve()
    )
    assert _flag_value(command, "--pressure-completion-summary-csv") == str(
        (
            tmp_path
            / "plan"
            / "pressure_channel"
            / "pressure_channel_completion"
            / "pressure_channel_completion_summary.csv"
        ).resolve()
    )
    assert _flag_value(command, "--pressure-device-readiness-csv") == str(
        (
            tmp_path
            / "plan"
            / "pressure_channel"
            / "pressure_channel_completion"
            / "pressure_channel_device_readiness.csv"
        ).resolve()
    )
    assert _flag_value(command, "--fit-input-quality-summary-csv") == str(
        (tmp_path / "plan" / "fit_input_quality" / "v1_5_fit_input_quality_summary.csv").resolve()
    )
    assert _flag_value(command, "--fit-input-quality-devices-csv") == str(
        (tmp_path / "plan" / "fit_input_quality" / "v1_5_fit_input_quality_devices.csv").resolve()
    )
    assert "post_run_coefficient_executor/executor_manifest.json" in step.expected_outputs
    assert "post_run_coefficient_executor/device_eligibility.csv" in step.expected_outputs
    assert "post_run_coefficient_executor/controlled_write_package.csv" in step.expected_outputs
    assert "post_run_coefficient_executor/post_write_reverification_plan.csv" in step.expected_outputs
    assert "post_run_coefficient_executor/archive_gap_list.csv" in step.expected_outputs
    assert "Missing H2O post-write reverification blocks final acceptance" in " ".join(step.notes)


def test_full_flow_plan_adds_offline_closure_readiness_gate(tmp_path):
    config = tmp_path / "config.json"
    config.write_text("{}", encoding="utf-8")
    plan = build_full_flow_plan(
        config_path=config,
        output_dir=tmp_path / "plan",
        run_id="demo",
        reviewed_run_dir=tmp_path / "reviewed_run",
    )

    step = next(item for item in plan.steps if item.step_id == "full_flow_closure_readiness")
    command = list(step.command)

    assert step.tool_module == "gas_calibrator.tools.export_v1_5_full_flow_closure_readiness"
    assert step.execution_mode == "offline_review"
    assert step.gate == "required_before_controlled_write_review"
    assert step.opens_com_ports is False
    assert step.controls_gas_route is False
    assert step.controls_water_route is False
    assert step.writes_coefficients is False
    assert _flag_value(command, "--run-dir") == str((tmp_path / "plan").resolve())
    assert _flag_value(command, "--output-dir") == str(
        (tmp_path / "plan" / "full_flow_closure_readiness").resolve()
    )
    assert "full_flow_closure_readiness/v1_5_full_flow_closure_readiness.json" in step.expected_outputs
    assert "full_flow_closure_readiness/v1_5_full_flow_device_closure.csv" in step.expected_outputs
    assert "full_flow_closure_readiness/v1_5_full_flow_release_domains.csv" in step.expected_outputs
    assert "Fit/verification labels do not exclude" in " ".join(step.notes)


def test_full_flow_physical_stages_use_runtime_bound_config_after_identity_snapshot(tmp_path):
    config = tmp_path / "config.json"
    config.write_text("{}", encoding="utf-8")
    plan = build_full_flow_plan(config_path=config, output_dir=tmp_path / "plan", run_id="demo")

    runtime_bound = str(tmp_path / "plan" / "coefficient_epoch_0_getco_snapshot" / "runtime_identity_bound_config.json")
    for step_id in (
        "pressure_quick_check",
        "pressure_senco9_no_write_acquisition",
        "co2_open_flow_sampling",
        "h2o_open_flow_sampling",
    ):
        step = next(item for item in plan.steps if item.step_id == step_id)
        command = list(step.command)
        assert command[command.index("--config") + 1] == runtime_bound


def test_full_flow_pressure_senco9_acquisition_uses_full_v1_5_transition_contract(tmp_path):
    config = tmp_path / "config.json"
    config.write_text("{}", encoding="utf-8")
    plan = build_full_flow_plan(config_path=config, output_dir=tmp_path / "plan", run_id="demo")

    step = next(item for item in plan.steps if item.step_id == "pressure_senco9_no_write_acquisition")
    command = list(step.command)

    assert step.tool_module == "gas_calibrator.tools.validate_pressure_only"
    assert step.opens_com_ports is True
    assert step.controls_pressure is True
    assert step.writes_coefficients is False
    assert command[command.index("--pressure-points") + 1] == "1100,1000,900,800,700,600,500"
    assert "--control-pressure-points" in command
    assert "--continuous-atmosphere-hold" in command
    assert "--require-continuous-atmosphere-hold" in command
    assert command[command.index("--pressure-control-slew-mode") + 1] == "max"
    assert "--pressure-control-slew-hpa-per-s" not in command
    assert command[command.index("--pressure-control-atmosphere-release-wait-s") + 1] == "1.5"
    assert command[command.index("--pressure-control-post-stable-wait-s") + 1] == "8.0"
    assert "pressure_transition_trace.csv" in step.expected_outputs
    assert "dynamic_pressure_diagnostic" not in " ".join(command).lower()


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
    assert (out / "v1_5_full_flow_stage_manifest.json").exists()
    assert (out / "v1_5_full_flow_stage_manifest.md").exists()
    assert (out / "v1_5_full_flow_live_runner_readiness.json").exists()
    assert (out / "v1_5_full_flow_live_runner_readiness.md").exists()
    assert (out / "v1_5_formal_flow_contract.json").exists()
    assert (out / "v1_5_formal_flow_contract.md").exists()
    assert (out / "v1_5_run_evidence_status.json").exists()
    assert (out / "v1_5_run_evidence_status.md").exists()
    assert (out / "formal_run_status" / "v1_5_formal_run_status.json").exists()
    assert (out / "formal_run_status" / "v1_5_formal_run_status.md").exists()
    assert (out / "formal_run_status" / "v1_5_formal_run_status_gates.csv").exists()
    assert (out / "automation_control_contract" / "v1_5_automation_control_contract.json").exists()
    assert (out / "automation_control_contract" / "V1_5_AUTOMATION_CONTROL_CONTRACT.md").exists()
    assert (out / "algorithm_profile_runner_dry_run" / "v1_5_algorithm_profile_runner_dry_run.json").exists()
    assert (out / "algorithm_profile_runner_dry_run" / "V1_5_ALGORITHM_PROFILE_RUNNER_DRY_RUN.md").exists()
    assert (out / "formal_database_dry_run" / "v1_5_formal_database_dry_run.json").exists()
    assert (out / "formal_database_dry_run" / "V1_5_FORMAL_DATABASE_DRY_RUN.md").exists()
    assert (
        out / "formal_database_import_preflight" / "v1_5_formal_database_import_preflight.json"
    ).exists()
    assert (
        out / "formal_database_import_preflight" / "V1_5_FORMAL_DATABASE_IMPORT_PREFLIGHT.md"
    ).exists()
    assert (
        out / "formal_database_import_authorization" / "v1_5_formal_database_import_authorization.json"
    ).exists()
    assert (
        out / "formal_database_import_authorization" / "V1_5_FORMAL_DATABASE_IMPORT_AUTHORIZATION.md"
    ).exists()
    assert (
        out
        / "formal_database_import_command_contract"
        / "v1_5_formal_database_import_command_contract.json"
    ).exists()
    assert (
        out
        / "formal_database_import_command_contract"
        / "V1_5_FORMAL_DATABASE_IMPORT_COMMAND_CONTRACT.md"
    ).exists()
    operation_console_json = out / "operation_console" / "v1_5_operation_console.json"
    operation_console_html = out / "operation_console" / "v1_5_operation_console.html"
    assert operation_console_json.exists()
    assert operation_console_html.exists()
    payload = json.loads(plan_json.read_text(encoding="utf-8"))
    assert payload["schema"] == "v1_5_full_calibration_flow_plan_v0"
    assert payload["dry_run_only"] is True
    assert payload["safety_contract"]["does_not_modify_run_app"] is True
    stage_manifest = json.loads((out / "v1_5_full_flow_stage_manifest.json").read_text(encoding="utf-8"))
    assert stage_manifest["schema"] == STAGE_MANIFEST_SCHEMA
    assert stage_manifest["one_button_live_runner_ready"] is False
    assert stage_manifest["safety_summary"]["planner_writes_coefficients"] is False
    assert (
        stage_manifest["safety_summary"]["live_runner_readiness_artifact"]
        == "v1_5_full_flow_live_runner_readiness.json"
    )
    readiness = json.loads((out / "v1_5_full_flow_live_runner_readiness.json").read_text(encoding="utf-8"))
    assert readiness["schema"] == LIVE_RUNNER_READINESS_SCHEMA
    assert readiness["one_button_live_runner_ready"] is False
    assert "pressure_channel" in readiness["blocked_domains"]
    assert "coefficient_write" in readiness["required_authorizations"]
    contract = json.loads((out / "v1_5_formal_flow_contract.json").read_text(encoding="utf-8"))
    assert contract["status"] == "pass"
    assert contract["physical_boundaries"]["not_real_acceptance_evidence"] is True
    evidence_status = json.loads((out / "v1_5_run_evidence_status.json").read_text(encoding="utf-8"))
    assert evidence_status["physical_boundaries"]["opens_com_ports"] is False
    assert evidence_status["physical_boundaries"]["writes_coefficients"] is False
    assert evidence_status["contract_status"] == "pass"
    assert evidence_status["full_flow_live_runner_readiness"]["status"] == "present"
    assert evidence_status["full_flow_live_runner_readiness"]["one_button_live_runner_ready"] is False
    formal_status = json.loads((out / "formal_run_status" / "v1_5_formal_run_status.json").read_text(encoding="utf-8"))
    assert formal_status["physical_boundaries"]["opens_com_ports"] is False
    assert formal_status["physical_boundaries"]["writes_coefficients"] is False
    assert formal_status["linked_inputs"]["algorithm_profile_runner_dry_run_json"] == str(
        (out / "algorithm_profile_runner_dry_run" / "v1_5_algorithm_profile_runner_dry_run.json").resolve()
    )
    assert formal_status["linked_inputs"]["formal_database_dry_run_json"] == str(
        (out / "formal_database_dry_run" / "v1_5_formal_database_dry_run.json").resolve()
    )
    assert formal_status["linked_inputs"]["formal_database_import_preflight_json"] == str(
        (
            out / "formal_database_import_preflight" / "v1_5_formal_database_import_preflight.json"
        ).resolve()
    )
    assert formal_status["linked_inputs"]["formal_database_import_authorization_json"] == str(
        (
            out / "formal_database_import_authorization" / "v1_5_formal_database_import_authorization.json"
        ).resolve()
    )
    assert formal_status["linked_inputs"]["formal_database_import_command_contract_json"] == str(
        (
            out
            / "formal_database_import_command_contract"
            / "v1_5_formal_database_import_command_contract.json"
        ).resolve()
    )
    formal_gates = {row["gate_id"]: row for row in formal_status["gates"]}
    assert formal_gates["algorithm_profile_runner_dry_run"]["status"] == "ready"
    assert formal_gates["algorithm_profile_runner_dry_run"]["blocks_release"] is False
    assert formal_gates["formal_database_dry_run"]["status"] == "ready"
    assert formal_gates["formal_database_dry_run"]["blocks_release"] is False
    assert formal_gates["formal_database_import_preflight"]["status"] == "review_required"
    assert formal_gates["formal_database_import_preflight"]["blocks_release"] is False
    assert formal_gates["formal_database_import_preflight"]["blocks_physical_flow"] is False
    assert formal_gates["formal_database_import_authorization"]["status"] == "blocked"
    assert formal_gates["formal_database_import_authorization"]["blocks_release"] is False
    assert formal_gates["formal_database_import_authorization"]["blocks_physical_flow"] is False
    assert formal_gates["formal_database_import_command_contract"]["status"] == "blocked"
    assert formal_gates["formal_database_import_command_contract"]["blocks_release"] is False
    assert formal_gates["formal_database_import_command_contract"]["blocks_physical_flow"] is False
    database_dry_run = json.loads(
        (out / "formal_database_dry_run" / "v1_5_formal_database_dry_run.json").read_text(encoding="utf-8-sig")
    )
    assert database_dry_run["production_backend"] == "postgresql"
    assert database_dry_run["production_postgresql_major"] == 18
    assert database_dry_run["database_import_allowed"] is False
    assert database_dry_run["connects_postgresql"] is False
    database_import_preflight = json.loads(
        (
            out / "formal_database_import_preflight" / "v1_5_formal_database_import_preflight.json"
        ).read_text(encoding="utf-8-sig")
    )
    assert database_import_preflight["overall_status"] == "review_required"
    assert database_import_preflight["dsn_configured"] is False
    assert database_import_preflight["connects_postgresql"] is False
    assert database_import_preflight["database_import_allowed"] is False
    database_import_authorization = json.loads(
        (
            out / "formal_database_import_authorization" / "v1_5_formal_database_import_authorization.json"
        ).read_text(encoding="utf-8-sig")
    )
    assert database_import_authorization["overall_status"] == "blocked"
    assert database_import_authorization["preflight_ready"] is False
    assert database_import_authorization["archive_release_ready"] is False
    assert database_import_authorization["senco_authorization_archive_binding_ready"] is False
    assert database_import_authorization["manual_authorization_ready"] is False
    assert database_import_authorization["connects_postgresql"] is False
    assert database_import_authorization["database_import_allowed"] is False
    database_import_command_contract = json.loads(
        (
            out
            / "formal_database_import_command_contract"
            / "v1_5_formal_database_import_command_contract.json"
        ).read_text(encoding="utf-8-sig")
    )
    assert database_import_command_contract["overall_status"] == "blocked"
    assert database_import_command_contract["authorization_ready"] is False
    assert database_import_command_contract["preflight_ready"] is False
    assert database_import_command_contract["senco_authorization_archive_binding_ready"] is False
    assert database_import_command_contract["connects_postgresql"] is False
    assert database_import_command_contract["real_import_execution_allowed"] is False
    assert database_import_command_contract["database_import_allowed"] is False
    assert formal_status["linked_inputs"]["run_evidence_status_json"] == str(
        (out / "v1_5_run_evidence_status.json").resolve()
    )
    operation_console = json.loads(operation_console_json.read_text(encoding="utf-8"))
    assert operation_console["source_evidence"]["has_full_flow_stage_manifest"] is True
    assert operation_console["source_evidence"]["has_formal_run_status"] is True
    assert operation_console["opens_com_ports"] is False
    assert operation_console["controls_water_or_gas_routes"] is False
    assert operation_console["writes_coefficients"] is False
    assert operation_console["cannot_write_senco"] is True
    assert operation_console["stage_manifest_panel"]["status"] != "not_found"
    assert operation_console["stage_manifest_panel"]["one_button_live_runner_ready"] is False
    assert operation_console["formal_run_status_panel"]["current_stage"] == formal_status["current_stage"]
    assert (out / "v1_5_full_flow_state.json").exists()
    assert (out / "v1_5_full_flow_state.md").exists()


def test_write_full_flow_plan_contains_physical_contract(tmp_path):
    config = tmp_path / "config.json"
    config.write_text("{}", encoding="utf-8")
    plan = build_full_flow_plan(config_path=config, output_dir=tmp_path / "plan", run_id="demo")
    outputs = write_full_flow_plan(plan)

    text = outputs["markdown"].read_text(encoding="utf-8")
    assert "Existing internal coefficients affect displayed CO2/H2O" in text
    assert "SENCO5/SENCO6 are final CO2/H2O displayed-concentration affine trims" in text
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
    assert "per_device_certificate_manifest.json" in report_step.expected_outputs
    assert "per_device_certificate_artifact_hashes.csv" in report_step.expected_outputs
    refresh_step = next(step for step in plan.steps if step.step_id == "final_evidence_status_refresh")
    assert refresh_step.tool_module == "gas_calibrator.tools.export_v1_5_run_evidence_status"
    assert str(refresh_step.command[refresh_step.command.index("--run-dir") + 1]).endswith("plan")
    automation_step = next(step for step in plan.steps if step.step_id == "automation_control_contract_snapshot")
    automation_command = list(automation_step.command)
    assert automation_step.tool_module == "gas_calibrator.tools.export_v1_5_automation_control_contract"
    assert automation_step.execution_mode == "offline_sidecar"
    assert automation_step.opens_com_ports is False
    assert automation_step.controls_gas_route is False
    assert automation_step.controls_water_route is False
    assert automation_step.writes_coefficients is False
    assert _flag_value(automation_command, "--output-dir").endswith("automation_control_contract")
    assert "0620/0621 physical route core" in automation_step.physical_meaning
    algorithm_step = next(step for step in plan.steps if step.step_id == "algorithm_profile_runner_dry_run_snapshot")
    algorithm_command = list(algorithm_step.command)
    assert algorithm_step.tool_module == "gas_calibrator.tools.export_v1_5_algorithm_profile_runner_dry_run"
    assert algorithm_step.execution_mode == "offline_sidecar"
    assert algorithm_step.opens_com_ports is False
    assert algorithm_step.controls_gas_route is False
    assert algorithm_step.controls_water_route is False
    assert algorithm_step.writes_coefficients is False
    assert _flag_value(algorithm_command, "--output-dir").endswith("algorithm_profile_runner_dry_run")
    assert _flag_value(algorithm_command, "--profile-path").endswith("configs\\v1_5_algorithm_route_profiles.json")
    database_step = next(step for step in plan.steps if step.step_id == "formal_database_dry_run_snapshot")
    database_command = list(database_step.command)
    assert database_step.tool_module == "gas_calibrator.tools.export_v1_5_formal_database_dry_run"
    assert database_step.execution_mode == "offline_sidecar"
    assert database_step.opens_com_ports is False
    assert database_step.controls_gas_route is False
    assert database_step.controls_water_route is False
    assert database_step.writes_coefficients is False
    assert database_step.writes_device_id is False
    assert _flag_value(database_command, "--output-dir").endswith("formal_database_dry_run")
    assert "--fail-on-blocker" in database_command
    import_preflight_step = next(
        step for step in plan.steps if step.step_id == "formal_database_import_preflight_snapshot"
    )
    import_preflight_command = list(import_preflight_step.command)
    assert (
        import_preflight_step.tool_module
        == "gas_calibrator.tools.export_v1_5_formal_database_import_preflight"
    )
    assert import_preflight_step.execution_mode == "offline_sidecar"
    assert import_preflight_step.opens_com_ports is False
    assert import_preflight_step.controls_gas_route is False
    assert import_preflight_step.controls_water_route is False
    assert import_preflight_step.writes_coefficients is False
    assert import_preflight_step.writes_device_id is False
    assert _flag_value(import_preflight_command, "--formal-database-dry-run-json").endswith(
        "formal_database_dry_run\\v1_5_formal_database_dry_run.json"
    )
    assert _flag_value(import_preflight_command, "--dsn-env") == "V1_5_POSTGRES_DSN"
    assert _flag_value(import_preflight_command, "--output-dir").endswith("formal_database_import_preflight")
    assert "--fail-on-blocker" in import_preflight_command
    import_authorization_step = next(
        step for step in plan.steps if step.step_id == "formal_database_import_authorization_snapshot"
    )
    import_authorization_command = list(import_authorization_step.command)
    assert (
        import_authorization_step.tool_module
        == "gas_calibrator.tools.export_v1_5_formal_database_import_authorization"
    )
    assert import_authorization_step.execution_mode == "offline_sidecar"
    assert import_authorization_step.opens_com_ports is False
    assert import_authorization_step.controls_gas_route is False
    assert import_authorization_step.controls_water_route is False
    assert import_authorization_step.writes_coefficients is False
    assert import_authorization_step.writes_device_id is False
    assert _flag_value(import_authorization_command, "--formal-database-import-preflight-json").endswith(
        "formal_database_import_preflight\\v1_5_formal_database_import_preflight.json"
    )
    assert _flag_value(import_authorization_command, "--archive-closure-json").endswith(
        "formal_archive_closure_from_full_chain\\v1_5_formal_archive_closure_index.json"
    )
    assert _flag_value(import_authorization_command, "--authorization-id") == "<database_import_authorization_id>"
    assert _flag_value(import_authorization_command, "--output-dir").endswith(
        "formal_database_import_authorization"
    )
    assert "--fail-on-blocker" in import_authorization_command
    command_contract_step = next(
        step for step in plan.steps if step.step_id == "formal_database_import_command_contract_snapshot"
    )
    command_contract_command = list(command_contract_step.command)
    assert (
        command_contract_step.tool_module
        == "gas_calibrator.tools.export_v1_5_formal_database_import_command_contract"
    )
    assert command_contract_step.execution_mode == "offline_sidecar"
    assert command_contract_step.opens_com_ports is False
    assert command_contract_step.controls_gas_route is False
    assert command_contract_step.controls_water_route is False
    assert command_contract_step.writes_coefficients is False
    assert command_contract_step.writes_device_id is False
    assert _flag_value(command_contract_command, "--formal-database-import-authorization-json").endswith(
        "formal_database_import_authorization\\v1_5_formal_database_import_authorization.json"
    )
    assert _flag_value(command_contract_command, "--formal-database-import-preflight-json").endswith(
        "formal_database_import_preflight\\v1_5_formal_database_import_preflight.json"
    )
    assert _flag_value(command_contract_command, "--archive-closure-json").endswith(
        "formal_archive_closure_from_full_chain\\v1_5_formal_archive_closure_index.json"
    )
    assert _flag_value(command_contract_command, "--evidence-bundle-json").endswith(
        "formal_archive_closure_from_full_chain\\evidence_bundle.json"
    )
    assert _flag_value(command_contract_command, "--dsn-env") == "V1_5_POSTGRES_DSN"
    assert (
        _flag_value(command_contract_command, "--requested-command-module")
        == "gas_calibrator.tools.import_v1_5_evidence_package"
    )
    assert _flag_value(command_contract_command, "--output-dir").endswith(
        "formal_database_import_command_contract"
    )
    assert "--fail-on-blocker" in command_contract_command
    blocked_executor_step = next(
        step for step in plan.steps if step.step_id == "formal_database_import_blocked_executor_snapshot"
    )
    blocked_executor_command = list(blocked_executor_step.command)
    assert blocked_executor_step.tool_module == "gas_calibrator.tools.import_v1_5_evidence_package"
    assert blocked_executor_step.execution_mode == "offline_sidecar"
    assert blocked_executor_step.opens_com_ports is False
    assert blocked_executor_step.controls_gas_route is False
    assert blocked_executor_step.controls_water_route is False
    assert blocked_executor_step.writes_coefficients is False
    assert blocked_executor_step.writes_device_id is False
    assert _flag_value(blocked_executor_command, "--formal-database-import-command-contract-json").endswith(
        "formal_database_import_command_contract\\v1_5_formal_database_import_command_contract.json"
    )
    assert _flag_value(blocked_executor_command, "--formal-database-import-authorization-json").endswith(
        "formal_database_import_authorization\\v1_5_formal_database_import_authorization.json"
    )
    assert _flag_value(blocked_executor_command, "--formal-database-import-preflight-json").endswith(
        "formal_database_import_preflight\\v1_5_formal_database_import_preflight.json"
    )
    assert _flag_value(blocked_executor_command, "--archive-closure-json").endswith(
        "formal_archive_closure_from_full_chain\\v1_5_formal_archive_closure_index.json"
    )
    assert _flag_value(blocked_executor_command, "--evidence-bundle-json").endswith(
        "formal_archive_closure_from_full_chain\\evidence_bundle.json"
    )
    assert _flag_value(blocked_executor_command, "--dsn-env") == "V1_5_POSTGRES_DSN"
    assert _flag_value(blocked_executor_command, "--output-dir").endswith(
        "formal_database_import_blocked_executor"
    )
    assert "--fail-on-blocked" in blocked_executor_command
    design_step = next(
        step for step in plan.steps if step.step_id == "formal_database_import_controlled_executor_design_snapshot"
    )
    design_command = list(design_step.command)
    assert (
        design_step.tool_module
        == "gas_calibrator.tools.export_v1_5_formal_database_import_controlled_executor_design"
    )
    assert design_step.execution_mode == "offline_sidecar"
    assert design_step.opens_com_ports is False
    assert design_step.controls_gas_route is False
    assert design_step.controls_water_route is False
    assert design_step.writes_coefficients is False
    assert design_step.writes_device_id is False
    assert _flag_value(design_command, "--formal-database-import-blocked-executor-json").endswith(
        "formal_database_import_blocked_executor\\v1_5_formal_database_import_blocked_executor.json"
    )
    assert _flag_value(design_command, "--dsn-env") == "V1_5_POSTGRES_DSN"
    assert _flag_value(design_command, "--output-dir").endswith(
        "formal_database_import_controlled_executor_design"
    )
    status_step = next(step for step in plan.steps if step.step_id == "formal_run_status_snapshot")
    status_command = list(status_step.command)
    assert status_step.tool_module == "gas_calibrator.tools.export_v1_5_formal_run_status"
    assert status_step.execution_mode == "offline_sidecar"
    assert status_step.opens_com_ports is False
    assert status_step.writes_coefficients is False
    assert _flag_value(status_command, "--initialization-readiness-json").endswith(
        "formal_initialization\\v1_5_initialization_readiness.json"
    )
    assert _flag_value(status_command, "--formal-initialization-controlled-executor-design-json").endswith(
        "formal_initialization_controlled_executor_design\\v1_5_formal_initialization_controlled_executor_design.json"
    )
    assert _flag_value(status_command, "--formal-initialization-readonly-com-preflight-design-json").endswith(
        "formal_initialization_readonly_com_preflight_design\\v1_5_formal_initialization_readonly_com_preflight_design.json"
    )
    assert _flag_value(
        status_command,
        "--formal-initialization-readonly-com-preflight-blocked-executor-json",
    ).endswith(
        "formal_initialization_readonly_com_preflight_blocked_executor\\v1_5_formal_initialization_readonly_com_preflight_blocked_executor.json"
    )
    assert _flag_value(
        status_command,
        "--formal-initialization-readonly-com-preflight-controlled-executor-design-json",
    ).endswith(
        "formal_initialization_readonly_com_preflight_controlled_executor_design\\v1_5_formal_initialization_readonly_com_preflight_controlled_executor_design.json"
    )
    assert _flag_value(
        status_command,
        "--formal-initialization-readonly-com-preflight-controlled-blocked-executor-json",
    ).endswith(
        "formal_initialization_readonly_com_preflight_controlled_blocked_executor\\v1_5_formal_initialization_readonly_com_preflight_controlled_blocked_executor.json"
    )
    assert _flag_value(status_command, "--formal-readonly-com-execution-contract-json").endswith(
        "formal_readonly_com_execution_contract\\v1_5_formal_readonly_com_execution_contract.json"
    )
    assert _flag_value(status_command, "--formal-readonly-com-execution-packet-validator-json").endswith(
        "formal_readonly_com_execution_packet_validator\\v1_5_formal_readonly_com_execution_packet_validator.json"
    )
    assert _flag_value(status_command, "--formal-readonly-com-execution-plan-preview-json").endswith(
        "formal_readonly_com_execution_plan_preview\\v1_5_formal_readonly_com_execution_plan_preview.json"
    )
    assert _flag_value(status_command, "--formal-readonly-com-minimal-executor-review-json").endswith(
        "formal_readonly_com_minimal_executor_review\\v1_5_formal_readonly_com_minimal_executor_review.json"
    )
    assert _flag_value(status_command, "--formal-readonly-com-minimal-executor-stub-json").endswith(
        "formal_readonly_com_minimal_executor_stub\\v1_5_formal_readonly_com_minimal_executor_stub.json"
    )
    assert _flag_value(status_command, "--formal-readonly-com-minimal-executor-json").endswith(
        "formal_readonly_com_minimal_executor\\v1_5_formal_readonly_com_minimal_executor.json"
    )
    assert _flag_value(status_command, "--pre-gas-readiness-json").endswith(
        "pre_gas_readiness\\v1_5_pre_gas_readiness.json"
    )
    assert _flag_value(status_command, "--batch-initialization-closeout-json").endswith(
        "batch_initialization_closeout_index\\v1_5_batch_initialization_closeout_index.json"
    )
    assert _flag_value(status_command, "--post-closeout-resume-gate-json").endswith(
        "post_closeout_resume_gate\\v1_5_post_closeout_resume_gate.json"
    )
    assert _flag_value(status_command, "--run-evidence-status-json").endswith("v1_5_run_evidence_status.json")
    assert _flag_value(status_command, "--algorithm-profile-runner-dry-run-json").endswith(
        "algorithm_profile_runner_dry_run\\v1_5_algorithm_profile_runner_dry_run.json"
    )
    assert _flag_value(status_command, "--formal-database-dry-run-json").endswith(
        "formal_database_dry_run\\v1_5_formal_database_dry_run.json"
    )
    assert _flag_value(status_command, "--formal-database-import-preflight-json").endswith(
        "formal_database_import_preflight\\v1_5_formal_database_import_preflight.json"
    )
    assert _flag_value(status_command, "--formal-database-import-authorization-json").endswith(
        "formal_database_import_authorization\\v1_5_formal_database_import_authorization.json"
    )
    assert _flag_value(status_command, "--formal-database-import-command-contract-json").endswith(
        "formal_database_import_command_contract\\v1_5_formal_database_import_command_contract.json"
    )
    assert _flag_value(status_command, "--formal-database-import-blocked-executor-json").endswith(
        "formal_database_import_blocked_executor\\v1_5_formal_database_import_blocked_executor.json"
    )
    assert _flag_value(status_command, "--formal-database-import-controlled-executor-design-json").endswith(
        "formal_database_import_controlled_executor_design\\v1_5_formal_database_import_controlled_executor_design.json"
    )
    assert _flag_value(status_command, "--senco-artifact-authorization-json").endswith(
        "main_senco_write_precheck\\main_senco_artifact_authorization.json"
    )
    assert "v1_5_formal_run_status_gates.csv" in " ".join(status_step.expected_outputs)


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
    second = next(stage for stage in state.stage_states if stage.step_id == "formal_initialization_contract_plan")
    assert second.status == "pending_previous_stage"
    identity = next(stage for stage in state.stage_states if stage.step_id == "device_identity_and_getco_snapshot")
    assert identity.status == "pending_previous_stage"


def test_resume_state_blocks_real_com_until_authorized(tmp_path):
    config = tmp_path / "config.json"
    config.write_text("{}", encoding="utf-8")
    plan = build_full_flow_plan(config_path=config, output_dir=tmp_path / "plan", run_id="demo")
    state = build_full_flow_state(plan, completed_steps=["load_plan_and_traceability"])

    assert state.current_step_id == "formal_initialization_contract_plan"
    assert state.current_status == "ready"

    state = build_full_flow_state(plan, completed_steps=_pre_identity_offline_steps())

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
            *_pre_identity_offline_steps(),
            "device_identity_and_getco_snapshot",
            "identity_getco_readiness_snapshot",
            "auxiliary_senco56789_neutralization_gate",
            "pressure_quick_check",
            "pressure_senco9_no_write_acquisition",
            "pressure_senco9_no_write_review",
            "pressure_channel_completion_audit",
            "batch_initialization_closeout_index",
            "post_closeout_resume_gate_snapshot",
            "post_closeout_resume_prefix_application_review",
            "authoritative_resume_state_writer_design",
            "authoritative_resume_state_writer_blocked_executor",
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
    state = build_full_flow_state(plan, completed_steps=_pre_identity_offline_steps())

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
    offline = tuple(
        replace(step, command=(sys.executable, "-c", f"print('{step.step_id} ok')"))
        if step.step_id in _pre_identity_offline_steps()
        else step
        for step in plan.steps
    )
    plan = replace(plan, steps=offline)

    result = run_supervised_full_flow(
        plan,
        execute_commands=True,
        max_steps=len(_pre_identity_offline_steps()) + 1,
        output_dir=tmp_path / "exec",
        cwd=tmp_path,
    )

    assert [event.status for event in result.events] == [
        *("completed" for _ in _pre_identity_offline_steps()),
        "stopped",
    ]
    assert result.final_state.completed_step_ids == tuple(_pre_identity_offline_steps())
    assert result.final_state.current_step_id == "device_identity_and_getco_snapshot"
    assert result.final_state.current_status == "blocked_real_com_authorization"
    stdout = result.events[0].stdout_path
    assert stdout
    assert "load_plan_and_traceability ok" in open(stdout, encoding="utf-8").read()


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
    offline = tuple(
        replace(step, command=(sys.executable, "-c", f"print('{step.step_id} ok')"))
        if step.step_id in _pre_identity_offline_steps()
        else step
        for step in plan.steps
    )
    plan = replace(plan, steps=offline)
    result = run_supervised_full_flow(
        plan,
        execute_commands=True,
        max_steps=len(_pre_identity_offline_steps()),
        output_dir=tmp_path / "exec",
        cwd=tmp_path,
    )

    outputs = write_full_flow_state(result.final_state, tmp_path / "state")
    payload = json.loads(outputs["state_json"].read_text(encoding="utf-8"))

    assert payload["completed_step_ids"] == _pre_identity_offline_steps()
    assert payload["current_step_id"] == "device_identity_and_getco_snapshot"
    assert payload["current_status"] == "blocked_real_com_authorization"


def test_supervised_run_refuses_hazard_stage_even_when_state_authorized(tmp_path):
    config = tmp_path / "config.json"
    config.write_text("{}", encoding="utf-8")
    plan = build_full_flow_plan(config_path=config, output_dir=tmp_path / "plan", run_id="demo")

    result = run_supervised_full_flow(
        plan,
        completed_steps=[
            *_pre_identity_offline_steps(),
            "device_identity_and_getco_snapshot",
            "identity_getco_readiness_snapshot",
        ],
        failed_steps=[],
        allow_real_com=True,
        allow_pressure_control=True,
        execute_commands=True,
        max_steps=1,
        output_dir=tmp_path / "exec",
    )

    assert result.events[0].step_id == "auxiliary_senco56789_neutralization_gate"
    assert result.events[0].status == "stopped"
    assert result.final_state.current_step_id == "auxiliary_senco56789_neutralization_gate"
    assert result.final_state.completed_step_ids == (
        *_pre_identity_offline_steps(),
        "device_identity_and_getco_snapshot",
        "identity_getco_readiness_snapshot",
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
        completed_steps=_pre_identity_offline_steps(),
        allow_real_com=True,
        execute_commands=True,
        max_steps=1,
        output_dir=tmp_path / "exec",
        cwd=tmp_path,
    )

    assert result.events[0].step_id == "device_identity_and_getco_snapshot"
    assert result.events[0].status == "completed"
    assert result.final_state.current_step_id == "identity_getco_readiness_snapshot"
    assert result.final_state.current_status == "ready"
    assert result.final_state.completed_step_ids == (
        *_pre_identity_offline_steps(),
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


def test_full_flow_cli_can_generate_offline_archive_closure_for_reviewed_run(tmp_path):
    canonical = write_canonical_v1_5_evidence_package(
        tmp_path / "canonical",
        include_reports=False,
    )
    config = tmp_path / "config.json"
    config.write_text("{}", encoding="utf-8")
    out = tmp_path / "flow"
    run_dir = canonical["root"] / "run"
    archive_dir = run_dir / "formal_archive_from_full_chain"
    reviewed_standard_gases = tmp_path / "reviewed_standard_gases.json"
    reviewed_standard_gases.write_text(
        json.dumps(
            {
                "standard_gases": [
                    {
                        "component": "CO2",
                        "cylinder_id": "CO2-REVIEWED-FULL-FLOW",
                        "certificate_value": 897.04,
                        "unit": "ppm",
                        "uncertainty": 1.0,
                        "uncertainty_k": 2,
                        "certificate_id": "CERT-CO2-FULL-FLOW",
                        "certificate_hash": "reviewed-co2-full-flow-hash",
                        "valid_until": "2027-03-03",
                    },
                    {
                        "component": "H2O",
                        "cylinder_id": "H2O-REVIEWED-FULL-FLOW",
                        "certificate_value": 5.0,
                        "unit": "mmol/mol",
                        "uncertainty": 2.0,
                        "uncertainty_k": 2,
                        "certificate_id": "CERT-H2O-FULL-FLOW",
                        "certificate_hash": "reviewed-h2o-full-flow-hash",
                        "valid_until": "2027-03-03",
                    },
                ]
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )

    rc = cli_main(
        [
            "--config",
            str(config),
            "--output-dir",
            str(out),
            "--run-id",
            "archive-demo",
            "--reviewed-run-dir",
            str(run_dir),
            "--pressure-reference-json",
            str(canonical["pressure_reference"]),
            "--archive-closure",
            "--archive-plan-json",
            str(canonical["plan"]),
            "--archive-standard-gases-json",
            str(reviewed_standard_gases),
            "--archive-output-dir",
            str(archive_dir),
            "--archive-db-mode",
            "dry-run",
            "--archive-report-no",
            "RPT-FULL-CHAIN-ARCHIVE",
            "--reviewer",
            "reviewer-a",
            "--approver",
            "approver-a",
            "--archive-location",
            "lab-a",
            "--archive-calibration-date",
            "2026-05-24",
        ]
    )

    assert rc == 0
    archive_index_path = archive_dir / "v1_5_formal_archive_closure_index.json"
    archive_bundle_path = archive_dir / "evidence_bundle.json"
    archive_traceability_path = archive_dir / "traceability_summary.json"
    archive_database_path = archive_dir / "database_import_summary.json"
    assert archive_index_path.exists()
    assert archive_bundle_path.exists()
    assert archive_traceability_path.exists()
    assert archive_database_path.exists()

    archive_index = json.loads(archive_index_path.read_text(encoding="utf-8"))
    assert archive_index["database"]["mode"] == "dry_run"
    assert archive_index["database"]["database_imported"] is False
    assert archive_index["standard_gases_json"] == str(reviewed_standard_gases.resolve())
    assert (archive_dir / "standard_gases_reviewed_snapshot.json").exists()
    assert (archive_dir / "formal_plan_snapshot_with_standard_gases.json").exists()
    assert archive_index["physical_boundaries"]["opens_com_ports"] is False
    assert archive_index["physical_boundaries"]["controls_water_or_gas_routes"] is False
    assert archive_index["physical_boundaries"]["writes_coefficients"] is False
    assert archive_index["traceability_checks"]["has_raw_samples"] is True
    assert "formal_calibration_report_markdown" in archive_index["reports"]

    bundle = json.loads(archive_bundle_path.read_text(encoding="utf-8"))
    report_types = {row["report_type"] for row in bundle["tables"]["reports"]}
    assert {"run_report", "technical_report", "formal_calibration_report", "report_model"}.issubset(
        report_types
    )
    gas_rows = bundle["tables"]["standard_gases"]
    assert {row["cylinder_id"] for row in gas_rows} == {
        "CO2-REVIEWED-FULL-FLOW",
        "H2O-REVIEWED-FULL-FLOW",
    }
    assert {row["certificate_hash"] for row in gas_rows} == {
        "reviewed-co2-full-flow-hash",
        "reviewed-h2o-full-flow-hash",
    }
    evidence_status = json.loads((out / "v1_5_run_evidence_status.json").read_text(encoding="utf-8"))
    assert evidence_status["physical_boundaries"]["opens_com_ports"] is False
    assert evidence_status["physical_boundaries"]["writes_coefficients"] is False
    assert evidence_status["linked_inputs"]["evidence_bundle_json"] == str(archive_bundle_path.resolve())
    stage_map = {stage["stage_id"]: stage for stage in evidence_status["stage_statuses"]}
    assert stage_map["reports"]["status"] == "pass"
    assert evidence_status["artifact_count"] > 0


def test_full_flow_cli_can_generate_post_run_coefficient_executor_gap_list(tmp_path):
    config = tmp_path / "config.json"
    config.write_text("{}", encoding="utf-8")
    run_dir = tmp_path / "reviewed_run"
    run_dir.mkdir()
    plan_json = run_dir / "formal_plan_snapshot.json"
    plan_json.write_text(
        json.dumps(
            {"devices": {"gas_analyzers": [{"runtime_device_id": "077", "serial_port": "COM35"}]}},
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    pressure_reference_json = run_dir / "pressure_reference.json"
    pressure_reference_json.write_text(json.dumps({"device_id": "COM22"}), encoding="utf-8")
    out = tmp_path / "flow"

    rc = cli_main(
        [
            "--config",
            str(config),
            "--output-dir",
            str(out),
            "--run-id",
            "executor-demo",
            "--reviewed-run-dir",
            str(run_dir),
            "--archive-plan-json",
            str(plan_json),
            "--pressure-reference-json",
            str(pressure_reference_json),
            "--post-run-coefficient-executor",
        ]
    )

    assert rc == 0
    manifest_path = out / "post_run_coefficient_executor" / "executor_manifest.json"
    summary_path = out / "post_run_coefficient_executor" / "executor_summary.md"
    devices_path = out / "post_run_coefficient_executor" / "device_eligibility.csv"
    assert manifest_path.exists()
    assert summary_path.exists()
    assert devices_path.exists()
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest["overall_status"] == "blocked"
    assert manifest["physical_boundaries"]["opens_com_ports"] is False
    assert manifest["physical_boundaries"]["writes_coefficients"] is False
    stages = {row["stage_id"]: row for row in manifest["stages"]}
    assert stages["post_write_reverification"]["status"] == "not_attempted"
    assert "V1.5 采集后系数闭环执行计划" in summary_path.read_text(encoding="utf-8-sig")


def test_full_flow_closure_readiness_auto_generates_post_run_executor(tmp_path, capsys):
    config = tmp_path / "config.json"
    config.write_text("{}", encoding="utf-8")
    run_dir = tmp_path / "reviewed_run"
    run_dir.mkdir()
    plan_json = run_dir / "formal_plan_snapshot.json"
    plan_json.write_text(
        json.dumps(
            {"devices": {"gas_analyzers": [{"runtime_device_id": "077", "serial_port": "COM35"}]}},
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    pressure_reference_json = run_dir / "pressure_reference.json"
    pressure_reference_json.write_text(json.dumps({"device_id": "COM22"}), encoding="utf-8")
    out = tmp_path / "flow"

    rc = cli_main(
        [
            "--config",
            str(config),
            "--output-dir",
            str(out),
            "--run-id",
            "closure-demo",
            "--reviewed-run-dir",
            str(run_dir),
            "--archive-plan-json",
            str(plan_json),
            "--pressure-reference-json",
            str(pressure_reference_json),
            "--full-flow-closure-readiness",
        ]
    )
    cli_result = json.loads(capsys.readouterr().out)

    assert rc == 0
    executor_manifest = out / "post_run_coefficient_executor" / "executor_manifest.json"
    closure_json = out / "full_flow_closure_readiness" / "v1_5_full_flow_closure_readiness.json"
    closure_stages = out / "full_flow_closure_readiness" / "v1_5_full_flow_closure_stages.csv"
    closure_release_domains = out / "full_flow_closure_readiness" / "v1_5_full_flow_release_domains.csv"
    assert executor_manifest.exists()
    assert closure_json.exists()
    assert closure_stages.exists()
    assert closure_release_domains.exists()
    assert cli_result["full_flow_closure_readiness_stages"] == str(closure_stages.resolve())
    assert cli_result["full_flow_closure_readiness_release_domains"] == str(closure_release_domains.resolve())
    executor = json.loads(executor_manifest.read_text(encoding="utf-8"))
    closure = json.loads(closure_json.read_text(encoding="utf-8"))
    assert executor["physical_boundaries"]["opens_com_ports"] is False
    assert executor["physical_boundaries"]["writes_coefficients"] is False
    stages = {row["stage_id"]: row for row in closure["stage_statuses"]}
    assert stages["post_run_coefficient_executor"]["status"] in {"ready", "blocked"}
    assert closure["linked_inputs"]["post_run_executor_json"] == str(executor_manifest.resolve())
    evidence_status = json.loads((out / "v1_5_run_evidence_status.json").read_text(encoding="utf-8"))
    evidence_stages = {row["stage_id"]: row for row in evidence_status["stage_statuses"]}
    assert evidence_stages["post_run_coefficient_executor"]["status"] == "pass"
    assert evidence_stages["full_flow_closure_readiness"]["status"] == "pass"
    roles = {row["role"] for row in evidence_status["artifacts"]}
    assert {
        "post_run_coefficient_executor",
        "post_run_device_eligibility",
        "post_run_controlled_write_package",
        "post_run_reverification_plan",
        "post_run_archive_gap_list",
    }.issubset(roles)
    assert {
        "full_flow_closure_readiness",
        "full_flow_closure_gaps",
        "full_flow_device_closure",
        "full_flow_release_domains",
    }.issubset(roles)
    assert evidence_status["linked_inputs"]["evidence_bundle_json"] == ""


def test_full_flow_cli_post_acquisition_closure_generates_offline_closure_chain(tmp_path, capsys):
    config = tmp_path / "config.json"
    config.write_text("{}", encoding="utf-8")
    run_dir = tmp_path / "reviewed_run"
    run_dir.mkdir()
    plan_json = run_dir / "formal_plan_snapshot.json"
    plan_json.write_text(
        json.dumps(
            {"devices": {"gas_analyzers": [{"runtime_device_id": "084", "serial_port": "COM36"}]}},
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    pressure_reference_json = run_dir / "pressure_reference.json"
    pressure_reference_json.write_text(json.dumps({"device_id": "COM22"}), encoding="utf-8")
    out = tmp_path / "flow"

    rc = cli_main(
        [
            "--config",
            str(config),
            "--output-dir",
            str(out),
            "--run-id",
            "post-acquisition-demo",
            "--reviewed-run-dir",
            str(run_dir),
            "--archive-plan-json",
            str(plan_json),
            "--pressure-reference-json",
            str(pressure_reference_json),
            "--post-acquisition-closure",
        ]
    )
    cli_result = json.loads(capsys.readouterr().out)

    assert rc == 0
    executor_manifest = out / "post_run_coefficient_executor" / "executor_manifest.json"
    readiness_json = out / "full_flow_closure_readiness" / "v1_5_full_flow_closure_readiness.json"
    status_json = out / "v1_5_run_evidence_status.json"
    assert executor_manifest.exists()
    assert readiness_json.exists()
    assert status_json.exists()
    assert cli_result["post_run_coefficient_executor_manifest"] == str(executor_manifest.resolve())
    assert cli_result["full_flow_closure_readiness_json"] == str(readiness_json.resolve())
    assert cli_result["run_evidence_status_final_json"] == str(status_json.resolve())
    formal_status_json = out / "formal_run_status" / "v1_5_formal_run_status.json"
    assert cli_result["formal_run_status_json"] == str(formal_status_json.resolve())
    assert cli_result["formal_run_status_refreshed_after_closure"] == str(formal_status_json.resolve())

    executor = json.loads(executor_manifest.read_text(encoding="utf-8"))
    readiness = json.loads(readiness_json.read_text(encoding="utf-8"))
    evidence_status = json.loads(status_json.read_text(encoding="utf-8"))
    formal_status = json.loads(formal_status_json.read_text(encoding="utf-8"))
    assert executor["physical_boundaries"]["opens_com_ports"] is False
    assert executor["physical_boundaries"]["writes_coefficients"] is False
    assert readiness["linked_inputs"]["post_run_executor_json"] == str(executor_manifest.resolve())
    evidence_stages = {row["stage_id"]: row for row in evidence_status["stage_statuses"]}
    assert evidence_stages["post_run_coefficient_executor"]["status"] == "pass"
    assert evidence_stages["full_flow_closure_readiness"]["status"] == "pass"
    roles = {row["role"] for row in evidence_status["artifacts"]}
    assert "post_run_controlled_write_package" in roles
    assert "post_run_reverification_plan" in roles
    assert "full_flow_closure_readiness" in roles
    assert formal_status["linked_inputs"]["full_flow_closure_readiness_json"] == str(readiness_json.resolve())
    assert formal_status["physical_boundaries"]["controls_water_or_gas_routes"] is False
