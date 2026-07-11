import json
from dataclasses import replace
from pathlib import Path

import pytest

from gas_calibrator.tools.export_v1_5_formal_flow_contract import main as export_main
from gas_calibrator.v1_5.orchestration.full_flow import build_full_flow_plan, write_full_flow_plan
from gas_calibrator.validation.v1_5_formal_flow_contract import (
    discover_current_v1_5_inventory,
    validate_v1_5_formal_flow_contract,
)


pytestmark = pytest.mark.v1_5_formal_gate


def _config(tmp_path):
    path = tmp_path / "config.json"
    path.write_text("{}", encoding="utf-8")
    return path


def _inventory_for_plan():
    return discover_current_v1_5_inventory(anchor_paths=(Path.cwd(),))


def test_formal_flow_contract_passes_for_generated_plan(tmp_path):
    plan = build_full_flow_plan(config_path=_config(tmp_path), output_dir=tmp_path / "flow", run_id="demo")

    report = validate_v1_5_formal_flow_contract(plan, inventory_entries=_inventory_for_plan())

    assert report.status == "pass"
    assert report.issues == ()
    assert report.warnings == ()
    assert report.physical_boundaries["not_real_acceptance_evidence"] is True
    assert report.physical_boundaries["opens_com_ports"] is False
    assert report.formal_runner_steps == ("co2_open_flow_sampling", "h2o_open_flow_sampling")
    assert report.step_sequence.index("formal_initialization_contract_plan") < report.step_sequence.index(
        "formal_initialization_executor_dry_run_snapshot"
    )
    assert report.step_sequence.index("formal_initialization_executor_dry_run_snapshot") < report.step_sequence.index(
        "formal_initialization_blocked_executor_snapshot"
    )
    assert report.step_sequence.index("formal_initialization_blocked_executor_snapshot") < report.step_sequence.index(
        "formal_initialization_controlled_executor_design_snapshot"
    )
    assert report.step_sequence.index(
        "formal_initialization_controlled_executor_design_snapshot"
    ) < report.step_sequence.index(
        "formal_initialization_readonly_com_preflight_design_snapshot"
    )
    assert report.step_sequence.index(
        "formal_initialization_readonly_com_preflight_design_snapshot"
    ) < report.step_sequence.index(
        "formal_initialization_readonly_com_preflight_blocked_executor_snapshot"
    )
    assert report.step_sequence.index(
        "formal_initialization_readonly_com_preflight_blocked_executor_snapshot"
    ) < report.step_sequence.index(
        "formal_initialization_readonly_com_preflight_controlled_executor_design_snapshot"
    )
    assert report.step_sequence.index(
        "formal_initialization_readonly_com_preflight_controlled_executor_design_snapshot"
    ) < report.step_sequence.index(
        "formal_initialization_readonly_com_preflight_controlled_blocked_executor_snapshot"
    )
    assert report.step_sequence.index(
        "formal_initialization_readonly_com_preflight_controlled_blocked_executor_snapshot"
    ) < report.step_sequence.index(
        "formal_readonly_com_execution_contract_snapshot"
    )
    assert report.step_sequence.index(
        "formal_readonly_com_execution_contract_snapshot"
    ) < report.step_sequence.index(
        "formal_readonly_com_execution_blocked_executor_snapshot"
    )
    assert report.step_sequence.index(
        "formal_readonly_com_execution_blocked_executor_snapshot"
    ) < report.step_sequence.index(
        "formal_readonly_com_execution_packet_validator_snapshot"
    )
    assert report.step_sequence.index(
        "formal_readonly_com_execution_packet_validator_snapshot"
    ) < report.step_sequence.index(
        "formal_readonly_com_execution_plan_preview_snapshot"
    )
    assert report.step_sequence.index(
        "formal_readonly_com_execution_plan_preview_snapshot"
    ) < report.step_sequence.index(
        "formal_readonly_com_minimal_executor_review_snapshot"
    )
    assert report.step_sequence.index(
        "formal_readonly_com_minimal_executor_review_snapshot"
    ) < report.step_sequence.index(
        "formal_readonly_com_minimal_executor_stub_snapshot"
    )
    assert report.step_sequence.index(
        "formal_readonly_com_minimal_executor_stub_snapshot"
    ) < report.step_sequence.index(
        "initialization_readiness_snapshot"
    )
    assert report.step_sequence.index("initialization_readiness_snapshot") < report.step_sequence.index(
        "pre_gas_readiness_snapshot"
    )
    assert report.step_sequence.index("pre_gas_readiness_snapshot") < report.step_sequence.index(
        "device_identity_and_getco_snapshot"
    )
    assert report.step_sequence.index("device_identity_and_getco_snapshot") < report.step_sequence.index(
        "identity_getco_readiness_snapshot"
    )
    assert report.step_sequence.index("identity_getco_readiness_snapshot") < report.step_sequence.index(
        "auxiliary_senco56789_neutralization_gate"
    )
    assert "PRE_GAS_READINESS" in "\n".join(report.physical_flow)
    assert "INITIALIZATION_EXECUTOR_DRY_RUN" in "\n".join(report.physical_flow)
    assert "INITIALIZATION_BLOCKED_EXECUTOR" in "\n".join(report.physical_flow)
    assert "INITIALIZATION_CONTROLLED_EXECUTOR_DESIGN" in "\n".join(report.physical_flow)
    assert "INITIALIZATION_READONLY_COM_PREFLIGHT_DESIGN" in "\n".join(report.physical_flow)
    assert "INITIALIZATION_READONLY_COM_PREFLIGHT_BLOCKED_EXECUTOR" in "\n".join(report.physical_flow)
    assert "INITIALIZATION_READONLY_COM_PREFLIGHT_CONTROLLED_EXECUTOR_DESIGN" in "\n".join(
        report.physical_flow
    )
    assert "INITIALIZATION_READONLY_COM_PREFLIGHT_CONTROLLED_BLOCKED_EXECUTOR" in "\n".join(
        report.physical_flow
    )
    assert "FORMAL_READONLY_COM_EXECUTION_CONTRACT" in "\n".join(report.physical_flow)
    assert "FORMAL_READONLY_COM_EXECUTION_BLOCKED_EXECUTOR" in "\n".join(report.physical_flow)
    assert "FORMAL_READONLY_COM_EXECUTION_PACKET_VALIDATOR" in "\n".join(report.physical_flow)
    assert "FORMAL_READONLY_COM_EXECUTION_PLAN_PREVIEW" in "\n".join(report.physical_flow)
    assert "FORMAL_READONLY_COM_MINIMAL_EXECUTOR_STUB" in "\n".join(report.physical_flow)
    assert "IDENTITY_GETCO_READINESS" in "\n".join(report.physical_flow)
    assert report.step_sequence.index("pressure_senco9_no_write_acquisition") < report.step_sequence.index(
        "pressure_senco9_no_write_review"
    )
    assert report.step_sequence.index("pressure_senco9_no_write_review") < report.step_sequence.index(
        "pressure_channel_completion_audit"
    )
    assert report.step_sequence.index("pressure_channel_completion_audit") < report.step_sequence.index(
        "temperature_channel_fast_review"
    )
    assert report.step_sequence.index("co2_candidate_write_review") < report.step_sequence.index(
        "main_senco_write_precheck_authorization_gate"
    )
    assert report.step_sequence.index("main_senco_write_precheck_authorization_gate") < report.step_sequence.index(
        "controlled_component_write_placeholder"
    )
    assert report.step_sequence.index("controlled_component_write_placeholder") < report.step_sequence.index(
        "post_write_reverification_placeholder"
    )
    assert report.step_sequence.index("post_write_reverification_placeholder") < report.step_sequence.index(
        "formal_evidence_sidecar"
    )
    assert report.step_sequence.index("formal_evidence_sidecar") < report.step_sequence.index(
        "formal_database_dry_run_snapshot"
    )
    assert report.step_sequence.index("formal_database_dry_run_snapshot") < report.step_sequence.index(
        "formal_database_import_preflight_snapshot"
    )
    assert report.step_sequence.index("formal_database_import_preflight_snapshot") < report.step_sequence.index(
        "formal_database_import_authorization_snapshot"
    )
    assert report.step_sequence.index("formal_database_import_authorization_snapshot") < report.step_sequence.index(
        "formal_database_import_command_contract_snapshot"
    )
    assert report.step_sequence.index("formal_database_import_command_contract_snapshot") < report.step_sequence.index(
        "formal_database_import_blocked_executor_snapshot"
    )
    assert report.step_sequence.index("formal_database_import_blocked_executor_snapshot") < report.step_sequence.index(
        "formal_database_import_controlled_executor_design_snapshot"
    )
    assert report.step_sequence.index(
        "formal_database_import_controlled_executor_design_snapshot"
    ) < report.step_sequence.index(
        "database_import"
    )
    assert report.step_sequence.index("final_evidence_status_refresh") < report.step_sequence.index(
        "automation_control_contract_snapshot"
    )
    assert report.step_sequence.index("automation_control_contract_snapshot") < report.step_sequence.index(
        "formal_run_status_snapshot"
    )
    if "algorithm_profile_runner_dry_run_snapshot" in report.step_sequence:
        assert report.step_sequence.index("automation_control_contract_snapshot") < report.step_sequence.index(
            "algorithm_profile_runner_dry_run_snapshot"
        )
    assert "FORMAL_DATABASE_DRY_RUN" in "\n".join(report.physical_flow)
    assert "FORMAL_DATABASE_IMPORT_PREFLIGHT" in "\n".join(report.physical_flow)
    assert "FORMAL_DATABASE_IMPORT_AUTHORIZATION" in "\n".join(report.physical_flow)
    assert "FORMAL_DATABASE_IMPORT_COMMAND_CONTRACT" in "\n".join(report.physical_flow)
    assert "FORMAL_DATABASE_IMPORT_BLOCKED_EXECUTOR" in "\n".join(report.physical_flow)
    assert "FORMAL_DATABASE_IMPORT_CONTROLLED_EXECUTOR_DESIGN" in "\n".join(report.physical_flow)
    assert "AUTOMATION_CONTROL_CONTRACT" in "\n".join(report.physical_flow)
    assert "SENCO_ARTIFACT_AUTHORIZATION" in "\n".join(report.physical_flow)
    assert "FORMAL_RUN_STATUS" in "\n".join(report.physical_flow)


def test_formal_flow_contract_blocks_component_sampling_before_pressure(tmp_path):
    plan = build_full_flow_plan(config_path=_config(tmp_path), output_dir=tmp_path / "flow", run_id="demo")
    steps = list(plan.steps)
    pressure_index = [step.step_id for step in steps].index("pressure_quick_check")
    co2_index = [step.step_id for step in steps].index("co2_open_flow_sampling")
    steps[pressure_index], steps[co2_index] = steps[co2_index], steps[pressure_index]

    report = validate_v1_5_formal_flow_contract(replace(plan, steps=tuple(steps)), inventory_entries=_inventory_for_plan())

    assert report.status == "blocked"
    assert any(issue.code == "physical_order_violation" for issue in report.issues)


@pytest.mark.parametrize(
    "missing_flag",
    ("--fit-input-quality-summary-csv", "--fit-input-quality-devices-csv"),
)
def test_formal_flow_contract_blocks_post_run_executor_without_fit_input_gate(tmp_path, missing_flag):
    plan = build_full_flow_plan(config_path=_config(tmp_path), output_dir=tmp_path / "flow", run_id="demo")
    step_index = [step.step_id for step in plan.steps].index("post_run_coefficient_executor")
    command = list(plan.steps[step_index].command)
    flag_index = command.index(missing_flag)
    del command[flag_index : flag_index + 2]
    broken_step = replace(plan.steps[step_index], command=tuple(command))
    plan = replace(plan, steps=(*plan.steps[:step_index], broken_step, *plan.steps[step_index + 1 :]))

    report = validate_v1_5_formal_flow_contract(plan, inventory_entries=_inventory_for_plan())

    assert report.status == "blocked"
    assert any(issue.code == "post_run_coefficient_executor_missing_fit_input_gate" for issue in report.issues)


def test_formal_flow_contract_blocks_physical_stage_without_runtime_identity_bound_config(tmp_path):
    config = _config(tmp_path)
    plan = build_full_flow_plan(config_path=config, output_dir=tmp_path / "flow", run_id="demo")
    co2_index = [step.step_id for step in plan.steps].index("pressure_senco9_no_write_acquisition")
    command = list(plan.steps[co2_index].command)
    command[command.index("--config") + 1] = str(config)
    broken_co2 = replace(plan.steps[co2_index], command=tuple(command))
    plan = replace(plan, steps=(*plan.steps[:co2_index], broken_co2, *plan.steps[co2_index + 1 :]))

    report = validate_v1_5_formal_flow_contract(plan, inventory_entries=_inventory_for_plan())

    assert report.status == "blocked"
    assert any(issue.code == "physical_stage_not_runtime_identity_bound" for issue in report.issues)


def test_formal_flow_contract_blocks_initialization_blocked_executor_that_is_not_stubbed(tmp_path):
    plan = build_full_flow_plan(config_path=_config(tmp_path), output_dir=tmp_path / "flow", run_id="demo")
    steps = list(plan.steps)
    index = [step.step_id for step in steps].index("formal_initialization_blocked_executor_snapshot")
    command = tuple(
        part
        for part in steps[index].command
        if part
        not in {
            "--formal-initialization-executor-dry-run-json",
            "--formal-initialization-plan-json",
            "--output-dir",
            "--fail-on-blocked",
        }
    )
    broken = replace(
        steps[index],
        tool_module="gas_calibrator.tools.run_v1_5_formal_initialization_runner",
        execution_mode="read_only_real_com_requires_authorization",
        opens_com_ports=True,
        writes_coefficients=True,
        writes_device_id=True,
        command=command,
    )
    steps[index] = broken

    report = validate_v1_5_formal_flow_contract(
        replace(plan, steps=tuple(steps)),
        inventory_entries=_inventory_for_plan(),
    )

    assert report.status == "blocked"
    codes = {issue.code for issue in report.issues}
    assert "formal_initialization_blocked_executor_wrong_tool" in codes
    assert "formal_initialization_blocked_executor_must_be_offline_no_write" in codes
    assert "formal_initialization_blocked_executor_must_not_write_device_id" in codes
    assert "formal_initialization_blocked_executor_must_be_offline" in codes
    assert "formal_initialization_blocked_executor_missing_required_flag" in codes


def test_formal_flow_contract_blocks_initialization_controlled_design_that_is_not_offline(tmp_path):
    plan = build_full_flow_plan(config_path=_config(tmp_path), output_dir=tmp_path / "flow", run_id="demo")
    steps = list(plan.steps)
    index = [step.step_id for step in steps].index("formal_initialization_controlled_executor_design_snapshot")
    command = tuple(
        part
        for part in steps[index].command
        if part
        not in {
            "--formal-initialization-blocked-executor-json",
            "--output-dir",
        }
    )
    steps[index] = replace(
        steps[index],
        tool_module="gas_calibrator.tools.run_v1_5_formal_initialization_runner",
        execution_mode="read_only_real_com_requires_authorization",
        opens_com_ports=True,
        writes_coefficients=True,
        writes_device_id=True,
        command=command,
    )

    report = validate_v1_5_formal_flow_contract(
        replace(plan, steps=tuple(steps)),
        inventory_entries=_inventory_for_plan(),
    )

    assert report.status == "blocked"
    codes = {issue.code for issue in report.issues}
    assert "formal_initialization_controlled_executor_design_wrong_tool" in codes
    assert "formal_initialization_controlled_executor_design_must_be_offline_no_write" in codes
    assert "formal_initialization_controlled_executor_design_must_not_write_device_id" in codes
    assert "formal_initialization_controlled_executor_design_must_be_offline" in codes
    assert "formal_initialization_controlled_executor_design_missing_required_flag" in codes


def test_formal_flow_contract_blocks_readonly_com_preflight_design_that_is_not_offline(tmp_path):
    plan = build_full_flow_plan(config_path=_config(tmp_path), output_dir=tmp_path / "flow", run_id="demo")
    steps = list(plan.steps)
    index = [
        step.step_id for step in steps
    ].index("formal_initialization_readonly_com_preflight_design_snapshot")
    command = tuple(
        part
        for part in steps[index].command
        if part
        not in {
            "--formal-initialization-controlled-executor-design-json",
            "--output-dir",
        }
    )
    steps[index] = replace(
        steps[index],
        tool_module="gas_calibrator.tools.run_v1_5_formal_initialization_runner",
        execution_mode="read_only_real_com_requires_authorization",
        opens_com_ports=True,
        writes_coefficients=True,
        writes_device_id=True,
        command=command,
    )

    report = validate_v1_5_formal_flow_contract(
        replace(plan, steps=tuple(steps)),
        inventory_entries=_inventory_for_plan(),
    )

    assert report.status == "blocked"
    codes = {issue.code for issue in report.issues}
    assert "formal_initialization_readonly_com_preflight_design_wrong_tool" in codes
    assert "formal_initialization_readonly_com_preflight_design_must_be_offline_no_write" in codes
    assert "formal_initialization_readonly_com_preflight_design_must_not_write_device_id" in codes
    assert "formal_initialization_readonly_com_preflight_design_must_be_offline" in codes
    assert "formal_initialization_readonly_com_preflight_design_missing_required_flag" in codes


def test_formal_flow_contract_blocks_readonly_com_preflight_blocked_executor_that_is_not_stubbed(tmp_path):
    plan = build_full_flow_plan(config_path=_config(tmp_path), output_dir=tmp_path / "flow", run_id="demo")
    steps = list(plan.steps)
    index = [
        step.step_id for step in steps
    ].index("formal_initialization_readonly_com_preflight_blocked_executor_snapshot")
    command = tuple(
        part
        for part in steps[index].command
        if part
        not in {
            "--formal-initialization-readonly-com-preflight-design-json",
            "--output-dir",
            "--fail-on-blocked",
        }
    )
    steps[index] = replace(
        steps[index],
        tool_module="gas_calibrator.tools.run_v1_5_formal_initialization_runner",
        execution_mode="read_only_real_com_requires_authorization",
        opens_com_ports=True,
        writes_coefficients=True,
        writes_device_id=True,
        command=command,
    )

    report = validate_v1_5_formal_flow_contract(
        replace(plan, steps=tuple(steps)),
        inventory_entries=_inventory_for_plan(),
    )

    assert report.status == "blocked"
    codes = {issue.code for issue in report.issues}
    assert "formal_initialization_readonly_com_preflight_blocked_executor_wrong_tool" in codes
    assert "formal_initialization_readonly_com_preflight_blocked_executor_must_be_offline_no_write" in codes
    assert "formal_initialization_readonly_com_preflight_blocked_executor_must_not_write_device_id" in codes
    assert "formal_initialization_readonly_com_preflight_blocked_executor_must_be_offline" in codes
    assert "formal_initialization_readonly_com_preflight_blocked_executor_missing_required_flag" in codes


def test_formal_flow_contract_blocks_readonly_com_preflight_controlled_executor_design_that_is_not_offline(
    tmp_path,
):
    plan = build_full_flow_plan(config_path=_config(tmp_path), output_dir=tmp_path / "flow", run_id="demo")
    steps = list(plan.steps)
    index = [
        step.step_id for step in steps
    ].index("formal_initialization_readonly_com_preflight_controlled_executor_design_snapshot")
    command = tuple(
        part
        for part in steps[index].command
        if part
        not in {
            "gas_calibrator.tools.export_v1_5_formal_initialization_readonly_com_preflight_controlled_executor_design",
            "--formal-initialization-readonly-com-preflight-blocked-executor-json",
            "--output-dir",
        }
    )
    steps[index] = replace(
        steps[index],
        tool_module="gas_calibrator.tools.run_v1_5_formal_co2_open_flow_queue",
        command=command,
        execution_mode="real_com",
        opens_com_ports=True,
        writes_device_id=True,
        writes_coefficients=True,
        controls_pressure=True,
        controls_gas_route=True,
    )
    tampered = replace(plan, steps=tuple(steps))

    report = validate_v1_5_formal_flow_contract(tampered)

    assert report.status == "blocked"
    codes = {issue.code for issue in report.issues}
    assert "formal_initialization_readonly_com_preflight_controlled_executor_design_wrong_tool" in codes
    assert (
        "formal_initialization_readonly_com_preflight_controlled_executor_design_must_be_offline_no_write"
        in codes
    )
    assert (
        "formal_initialization_readonly_com_preflight_controlled_executor_design_must_not_write_device_id"
        in codes
    )
    assert "formal_initialization_readonly_com_preflight_controlled_executor_design_must_be_offline" in codes
    assert (
        "formal_initialization_readonly_com_preflight_controlled_executor_design_missing_required_flag"
        in codes
    )


def test_formal_flow_contract_blocks_readonly_com_preflight_controlled_blocked_executor_that_is_not_offline(
    tmp_path,
):
    plan = build_full_flow_plan(config_path=_config(tmp_path), output_dir=tmp_path / "flow", run_id="demo")
    steps = list(plan.steps)
    index = [
        step.step_id for step in steps
    ].index("formal_initialization_readonly_com_preflight_controlled_blocked_executor_snapshot")
    command = tuple(
        part
        for part in steps[index].command
        if part
        not in {
            "gas_calibrator.tools.run_v1_5_formal_initialization_readonly_com_preflight_controlled_blocked_executor",
            "--formal-initialization-readonly-com-preflight-controlled-executor-design-json",
            "--output-dir",
            "--fail-on-blocked",
        }
    ) + ("--execute-read-only-real-com",)
    steps[index] = replace(
        steps[index],
        tool_module="gas_calibrator.tools.run_v1_5_formal_co2_open_flow_queue",
        command=command,
        execution_mode="real_com",
        opens_com_ports=True,
        writes_device_id=True,
        writes_coefficients=True,
        controls_pressure=True,
        controls_gas_route=True,
    )
    tampered = replace(plan, steps=tuple(steps))

    report = validate_v1_5_formal_flow_contract(tampered)

    assert report.status == "blocked"
    codes = {issue.code for issue in report.issues}
    assert "formal_initialization_readonly_com_preflight_controlled_blocked_executor_wrong_tool" in codes
    assert (
        "formal_initialization_readonly_com_preflight_controlled_blocked_executor_must_be_offline_no_write"
        in codes
    )
    assert (
        "formal_initialization_readonly_com_preflight_controlled_blocked_executor_must_not_write_device_id"
        in codes
    )
    assert "formal_initialization_readonly_com_preflight_controlled_blocked_executor_must_be_offline" in codes
    assert (
        "formal_initialization_readonly_com_preflight_controlled_blocked_executor_missing_required_flag"
        in codes
    )
    assert (
        "formal_initialization_readonly_com_preflight_controlled_blocked_executor_forbidden_unlock"
        in codes
    )


def test_formal_flow_contract_blocks_readonly_com_execution_contract_that_is_not_offline(tmp_path):
    plan = build_full_flow_plan(config_path=_config(tmp_path), output_dir=tmp_path / "flow", run_id="demo")
    steps = list(plan.steps)
    index = [step.step_id for step in steps].index("formal_readonly_com_execution_contract_snapshot")
    command = tuple(
        part
        for part in steps[index].command
        if part
        not in {
            "gas_calibrator.tools.export_v1_5_formal_readonly_com_execution_contract",
            "--formal-initialization-readonly-com-preflight-controlled-blocked-executor-json",
            "--output-dir",
            "--fail-on-review-required",
        }
    ) + ("--execute-read-only-real-com", "--authorization-id", "AUTH-1")
    steps[index] = replace(
        steps[index],
        tool_module="gas_calibrator.tools.probe_v1_5_getco_component_snapshot",
        command=command,
        execution_mode="read_only_real_com_requires_authorization",
        opens_com_ports=True,
        writes_device_id=True,
    )

    report = validate_v1_5_formal_flow_contract(
        replace(plan, steps=tuple(steps)),
        inventory_entries=_inventory_for_plan(),
    )

    assert report.status == "blocked"
    codes = {issue.code for issue in report.issues}
    assert "formal_readonly_com_execution_contract_wrong_tool" in codes
    assert "formal_readonly_com_execution_contract_must_be_offline_no_write" in codes
    assert "formal_readonly_com_execution_contract_must_not_write_device_id" in codes
    assert "formal_readonly_com_execution_contract_must_be_offline" in codes
    assert "formal_readonly_com_execution_contract_missing_required_flag" in codes
    assert "formal_readonly_com_execution_contract_forbidden_unlock" in codes


def test_formal_flow_contract_blocks_readonly_com_execution_packet_validator_with_packet_inputs(tmp_path):
    plan = build_full_flow_plan(config_path=_config(tmp_path), output_dir=tmp_path / "flow", run_id="demo")
    steps = list(plan.steps)
    index = [step.step_id for step in steps].index("formal_readonly_com_execution_packet_validator_snapshot")
    command = tuple(
        part
        for part in steps[index].command
        if part
        not in {
            "--formal-readonly-com-execution-blocked-executor-json",
            "--output-dir",
        }
    ) + (
        "--execute-read-only-real-com",
        "--authorization-packet-json",
        "authorization.json",
        "--reviewed-port-inventory-json",
        "ports.json",
    )
    steps[index] = replace(
        steps[index],
        tool_module="gas_calibrator.tools.probe_v1_5_getco_component_snapshot",
        command=command,
        execution_mode="read_only_real_com_requires_authorization",
        opens_com_ports=True,
        writes_coefficients=True,
        writes_device_id=True,
    )

    report = validate_v1_5_formal_flow_contract(
        replace(plan, steps=tuple(steps)),
        inventory_entries=_inventory_for_plan(),
    )

    assert report.status == "blocked"
    codes = {issue.code for issue in report.issues}
    assert "formal_readonly_com_execution_packet_validator_wrong_tool" in codes
    assert "formal_readonly_com_execution_packet_validator_must_be_offline_no_write" in codes
    assert "formal_readonly_com_execution_packet_validator_must_not_write_device_id" in codes
    assert "formal_readonly_com_execution_packet_validator_must_be_offline" in codes
    assert "formal_readonly_com_execution_packet_validator_missing_required_flag" in codes
    assert "formal_readonly_com_execution_packet_validator_forbidden_unlock_or_packet_input" in codes


def test_formal_flow_contract_blocks_readonly_com_execution_plan_preview_with_packet_inputs(tmp_path):
    plan = build_full_flow_plan(config_path=_config(tmp_path), output_dir=tmp_path / "flow", run_id="demo")
    steps = list(plan.steps)
    index = [step.step_id for step in steps].index("formal_readonly_com_execution_plan_preview_snapshot")
    command = tuple(
        part
        for part in steps[index].command
        if part
        not in {
            "--formal-readonly-com-execution-packet-validator-json",
            "--output-dir",
        }
    ) + (
        "--execute-read-only-real-com",
        "--reviewed-port-inventory-json",
        "ports.json",
        "--active-analyzer-list-json",
        "active.json",
    )
    steps[index] = replace(
        steps[index],
        tool_module="gas_calibrator.tools.probe_v1_5_getco_component_snapshot",
        command=command,
        execution_mode="read_only_real_com_requires_authorization",
        opens_com_ports=True,
        writes_coefficients=True,
        writes_device_id=True,
    )

    report = validate_v1_5_formal_flow_contract(
        replace(plan, steps=tuple(steps)),
        inventory_entries=_inventory_for_plan(),
    )

    assert report.status == "blocked"
    codes = {issue.code for issue in report.issues}
    assert "formal_readonly_com_execution_plan_preview_wrong_tool" in codes
    assert "formal_readonly_com_execution_plan_preview_must_be_offline_no_write" in codes
    assert "formal_readonly_com_execution_plan_preview_must_not_write_device_id" in codes
    assert "formal_readonly_com_execution_plan_preview_must_be_offline" in codes
    assert "formal_readonly_com_execution_plan_preview_missing_required_flag" in codes
    assert "formal_readonly_com_execution_plan_preview_forbidden_unlock_or_packet_input" in codes


def test_formal_flow_contract_blocks_readonly_com_minimal_executor_stub_with_live_inputs(tmp_path):
    plan = build_full_flow_plan(config_path=_config(tmp_path), output_dir=tmp_path / "flow", run_id="demo")
    steps = list(plan.steps)
    index = [step.step_id for step in steps].index("formal_readonly_com_minimal_executor_stub_snapshot")
    command = tuple(
        part
        for part in steps[index].command
        if part
        not in {
            "--formal-readonly-com-minimal-executor-review-json",
            "--output-dir",
        }
    ) + (
        "--execute-read-only-real-com",
        "--reviewed-port-inventory-json",
        "ports.json",
        "--active-analyzer-list-json",
        "active.json",
    )
    steps[index] = replace(
        steps[index],
        tool_module="gas_calibrator.tools.probe_v1_5_getco_component_snapshot",
        command=command,
        execution_mode="read_only_real_com_requires_authorization",
        opens_com_ports=True,
        writes_coefficients=True,
        writes_device_id=True,
    )

    report = validate_v1_5_formal_flow_contract(
        replace(plan, steps=tuple(steps)),
        inventory_entries=_inventory_for_plan(),
    )

    assert report.status == "blocked"
    codes = {issue.code for issue in report.issues}
    assert "formal_readonly_com_minimal_executor_stub_wrong_tool" in codes
    assert "formal_readonly_com_minimal_executor_stub_must_be_offline_no_write" in codes
    assert "formal_readonly_com_minimal_executor_stub_must_not_write_device_id" in codes
    assert "formal_readonly_com_minimal_executor_stub_must_be_offline" in codes
    assert "formal_readonly_com_minimal_executor_stub_missing_required_flag" in codes
    assert "formal_readonly_com_minimal_executor_stub_forbidden_live_or_context_input" in codes


def test_formal_flow_contract_blocks_identity_getco_readiness_that_is_not_offline(tmp_path):
    plan = build_full_flow_plan(config_path=_config(tmp_path), output_dir=tmp_path / "flow", run_id="demo")
    steps = list(plan.steps)
    index = [step.step_id for step in steps].index("identity_getco_readiness_snapshot")
    command = tuple(part for part in steps[index].command if part != "--fail-on-not-ready")
    steps[index] = replace(
        steps[index],
        tool_module="gas_calibrator.tools.probe_v1_5_getco_component_snapshot",
        command=command,
        opens_com_ports=True,
        writes_coefficients=True,
    )

    report = validate_v1_5_formal_flow_contract(replace(plan, steps=tuple(steps)), inventory_entries=_inventory_for_plan())

    assert report.status == "blocked"
    codes = {issue.code for issue in report.issues}
    assert "identity_getco_readiness_wrong_tool" in codes
    assert "identity_getco_readiness_must_be_offline_no_write" in codes
    assert "identity_getco_readiness_must_fail_when_not_ready" in codes


def test_formal_flow_contract_blocks_senco9_acquisition_without_transition_contract(tmp_path):
    plan = build_full_flow_plan(config_path=_config(tmp_path), output_dir=tmp_path / "flow", run_id="demo")
    steps = list(plan.steps)
    index = [step.step_id for step in steps].index("pressure_senco9_no_write_acquisition")
    command = list(steps[index].command)
    command.remove("--control-pressure-points")
    command[command.index("--pressure-points") + 1] = "ambient,1100,1000,900,800,700"
    command[command.index("--pressure-control-slew-mode") + 1] = "linear"
    broken = replace(steps[index], command=tuple(command), expected_outputs=("pressure_only_validation_meta.json",))
    steps[index] = broken

    report = validate_v1_5_formal_flow_contract(replace(plan, steps=tuple(steps)), inventory_entries=_inventory_for_plan())

    assert report.status == "blocked"
    codes = {issue.code for issue in report.issues}
    assert "pressure_senco9_acquisition_contract_violation" in codes
    assert "pressure_senco9_point_matrix_incomplete" in codes
    assert "pressure_senco9_sealed_matrix_must_not_include_ambient" in codes
    assert "pressure_senco9_slew_contract_violation" in codes
    assert "pressure_senco9_trace_missing" in codes


def test_formal_flow_contract_blocks_senco9_review_that_is_only_preflight(tmp_path):
    plan = build_full_flow_plan(config_path=_config(tmp_path), output_dir=tmp_path / "flow", run_id="demo")
    steps = list(plan.steps)
    index = [step.step_id for step in steps].index("pressure_senco9_no_write_review")
    broken = replace(
        steps[index],
        tool_module="gas_calibrator.tools.export_v1_5_pressure_senco9_no_write_preflight",
    )
    steps[index] = broken

    report = validate_v1_5_formal_flow_contract(replace(plan, steps=tuple(steps)), inventory_entries=_inventory_for_plan())

    assert report.status == "blocked"
    assert any(issue.code == "pressure_senco9_review_wrong_tool" for issue in report.issues)


def test_formal_flow_contract_blocks_pressure_completion_that_is_not_offline_completion_export(tmp_path):
    plan = build_full_flow_plan(config_path=_config(tmp_path), output_dir=tmp_path / "flow", run_id="demo")
    steps = list(plan.steps)
    index = [step.step_id for step in steps].index("pressure_channel_completion_audit")
    broken = replace(
        steps[index],
        tool_module="gas_calibrator.tools.validate_pressure_only",
        opens_com_ports=True,
    )
    steps[index] = broken

    report = validate_v1_5_formal_flow_contract(replace(plan, steps=tuple(steps)), inventory_entries=_inventory_for_plan())

    assert report.status == "blocked"
    codes = {issue.code for issue in report.issues}
    assert "pressure_channel_completion_wrong_tool" in codes
    assert "pressure_channel_completion_must_be_offline" in codes


def test_formal_flow_contract_blocks_diagnostic_tool_in_formal_route(tmp_path):
    plan = build_full_flow_plan(config_path=_config(tmp_path), output_dir=tmp_path / "flow", run_id="demo")
    co2_index = [step.step_id for step in plan.steps].index("co2_open_flow_sampling")
    diagnostic_co2 = replace(
        plan.steps[co2_index],
        tool_module="gas_calibrator.tools.run_v1_5_open_flow_dynamic_pressure_diagnostic",
    )
    plan = replace(plan, steps=(*plan.steps[:co2_index], diagnostic_co2, *plan.steps[co2_index + 1 :]))
    inventory = _inventory_for_plan()
    inventory["entries"].append(
        {
            "path": "src/gas_calibrator/tools/run_v1_5_open_flow_dynamic_pressure_diagnostic.py",
            "category": "diagnostic_only",
        }
    )

    report = validate_v1_5_formal_flow_contract(plan, inventory_entries=inventory)

    assert report.status == "blocked"
    assert any(issue.code == "diagnostic_tool_in_formal_flow" for issue in report.issues)


def test_formal_flow_contract_blocks_wrong_component_temperature_order(tmp_path):
    plan = build_full_flow_plan(config_path=_config(tmp_path), output_dir=tmp_path / "flow", run_id="demo")
    co2_index = [step.step_id for step in plan.steps].index("co2_open_flow_sampling")
    command = list(plan.steps[co2_index].command)
    command[command.index("--temperature-order") + 1] = "asc"
    broken_co2 = replace(plan.steps[co2_index], command=tuple(command))
    plan = replace(plan, steps=(*plan.steps[:co2_index], broken_co2, *plan.steps[co2_index + 1 :]))

    report = validate_v1_5_formal_flow_contract(plan, inventory_entries=_inventory_for_plan())

    assert report.status == "blocked"
    assert any(issue.code == "formal_temperature_order_violation" for issue in report.issues)


def test_formal_flow_contract_blocks_formal_open_flow_skip_and_diagnostic_flags(tmp_path):
    plan = build_full_flow_plan(config_path=_config(tmp_path), output_dir=tmp_path / "flow", run_id="demo")
    steps = list(plan.steps)
    co2_index = [step.step_id for step in steps].index("co2_open_flow_sampling")
    h2o_index = [step.step_id for step in steps].index("h2o_open_flow_sampling")
    co2 = replace(steps[co2_index], command=(*steps[co2_index].command, "--skip-stability-gate"))
    h2o = replace(steps[h2o_index], command=(*steps[h2o_index].command, "--skip-dewpoint-gate"))
    steps[co2_index] = co2
    steps[h2o_index] = h2o

    report = validate_v1_5_formal_flow_contract(replace(plan, steps=tuple(steps)), inventory_entries=_inventory_for_plan())

    assert report.status == "blocked"
    assert sum(issue.code == "formal_open_flow_forbidden_flag" for issue in report.issues) == 2


def test_formal_flow_contract_blocks_relaxed_co2_ratio_gate_policy(tmp_path):
    plan = build_full_flow_plan(config_path=_config(tmp_path), output_dir=tmp_path / "flow", run_id="demo")
    co2_index = [step.step_id for step in plan.steps].index("co2_open_flow_sampling")
    command = (*plan.steps[co2_index].command, "--co2-ratio-f-preseal-policy", "warn")
    broken_co2 = replace(plan.steps[co2_index], command=command)
    plan = replace(plan, steps=(*plan.steps[:co2_index], broken_co2, *plan.steps[co2_index + 1 :]))

    report = validate_v1_5_formal_flow_contract(plan, inventory_entries=_inventory_for_plan())

    assert report.status == "blocked"
    assert any(issue.code == "formal_co2_ratio_gate_policy_violation" for issue in report.issues)


def test_formal_flow_contract_requires_post_write_reverification(tmp_path):
    plan = build_full_flow_plan(config_path=_config(tmp_path), output_dir=tmp_path / "flow", run_id="demo")
    steps = tuple(step for step in plan.steps if step.step_id != "post_write_reverification_placeholder")

    report = validate_v1_5_formal_flow_contract(replace(plan, steps=steps), inventory_entries=_inventory_for_plan())

    assert report.status == "blocked"
    assert any(issue.code == "missing_required_step" for issue in report.issues)


def test_formal_flow_contract_blocks_formal_status_that_is_not_offline(tmp_path):
    plan = build_full_flow_plan(config_path=_config(tmp_path), output_dir=tmp_path / "flow", run_id="demo")
    steps = list(plan.steps)
    index = [step.step_id for step in steps].index("formal_run_status_snapshot")
    broken = replace(
        steps[index],
        tool_module="gas_calibrator.tools.run_v1_5_formal_co2_open_flow_queue",
        execution_mode="real_route_runner_when_authorized",
        opens_com_ports=True,
        controls_gas_route=True,
    )
    steps[index] = broken

    report = validate_v1_5_formal_flow_contract(replace(plan, steps=tuple(steps)), inventory_entries=_inventory_for_plan())

    assert report.status == "blocked"
    codes = {issue.code for issue in report.issues}
    assert "formal_run_status_wrong_tool" in codes
    assert "formal_run_status_must_be_offline_no_write" in codes
    assert "formal_run_status_must_be_offline" in codes


def test_formal_flow_contract_blocks_automation_contract_that_is_not_offline(tmp_path):
    plan = build_full_flow_plan(config_path=_config(tmp_path), output_dir=tmp_path / "flow", run_id="demo")
    steps = list(plan.steps)
    index = [step.step_id for step in steps].index("automation_control_contract_snapshot")
    broken = replace(
        steps[index],
        tool_module="gas_calibrator.tools.run_v1_5_formal_co2_open_flow_queue",
        execution_mode="real_route_runner_when_authorized",
        opens_com_ports=True,
        controls_gas_route=True,
        writes_coefficients=True,
        command=tuple(part for part in steps[index].command if part != "--output-dir"),
    )
    steps[index] = broken

    report = validate_v1_5_formal_flow_contract(replace(plan, steps=tuple(steps)), inventory_entries=_inventory_for_plan())

    assert report.status == "blocked"
    codes = {issue.code for issue in report.issues}
    assert "automation_control_contract_wrong_tool" in codes
    assert "automation_control_contract_must_be_offline_no_write" in codes
    assert "automation_control_contract_must_be_offline" in codes
    assert "automation_control_contract_missing_required_flag" in codes


def test_formal_flow_contract_blocks_database_dry_run_that_is_not_offline(tmp_path):
    plan = build_full_flow_plan(config_path=_config(tmp_path), output_dir=tmp_path / "flow", run_id="demo")
    steps = list(plan.steps)
    index = [step.step_id for step in steps].index("formal_database_dry_run_snapshot")
    broken = replace(
        steps[index],
        tool_module="gas_calibrator.tools.import_v1_5_evidence_package",
        execution_mode="offline_database_requires_configured_dsn",
        opens_com_ports=True,
        writes_coefficients=True,
        command=tuple(part for part in steps[index].command if part != "--fail-on-blocker"),
    )
    steps[index] = broken

    report = validate_v1_5_formal_flow_contract(replace(plan, steps=tuple(steps)), inventory_entries=_inventory_for_plan())

    assert report.status == "blocked"
    codes = {issue.code for issue in report.issues}
    assert "formal_database_dry_run_wrong_tool" in codes
    assert "formal_database_dry_run_must_be_offline_no_write" in codes
    assert "formal_database_dry_run_must_fail_on_blocker" in codes


def test_formal_flow_contract_blocks_database_import_preflight_that_is_not_offline(tmp_path):
    plan = build_full_flow_plan(config_path=_config(tmp_path), output_dir=tmp_path / "flow", run_id="demo")
    steps = list(plan.steps)
    index = [step.step_id for step in steps].index("formal_database_import_preflight_snapshot")
    command = tuple(
        part
        for part in steps[index].command
        if part not in {"--formal-database-dry-run-json", "--dsn-env", "--fail-on-blocker"}
    )
    broken = replace(
        steps[index],
        tool_module="gas_calibrator.tools.import_v1_5_evidence_package",
        execution_mode="offline_database_requires_configured_dsn",
        opens_com_ports=True,
        writes_coefficients=True,
        command=command,
    )
    steps[index] = broken

    report = validate_v1_5_formal_flow_contract(
        replace(plan, steps=tuple(steps)),
        inventory_entries=_inventory_for_plan(),
    )

    assert report.status == "blocked"
    codes = {issue.code for issue in report.issues}
    assert "formal_database_import_preflight_wrong_tool" in codes
    assert "formal_database_import_preflight_must_be_offline_no_write" in codes
    assert "formal_database_import_preflight_missing_required_flag" in codes


def test_formal_flow_contract_blocks_database_import_authorization_that_is_not_offline(tmp_path):
    plan = build_full_flow_plan(config_path=_config(tmp_path), output_dir=tmp_path / "flow", run_id="demo")
    steps = list(plan.steps)
    index = [step.step_id for step in steps].index("formal_database_import_authorization_snapshot")
    command = tuple(
        part
        for part in steps[index].command
        if part
        not in {
            "--formal-database-import-preflight-json",
            "--archive-closure-json",
            "--authorization-id",
            "--fail-on-blocker",
        }
    )
    broken = replace(
        steps[index],
        tool_module="gas_calibrator.tools.import_v1_5_evidence_package",
        execution_mode="offline_database_requires_configured_dsn",
        opens_com_ports=True,
        writes_coefficients=True,
        command=command,
    )
    steps[index] = broken

    report = validate_v1_5_formal_flow_contract(
        replace(plan, steps=tuple(steps)),
        inventory_entries=_inventory_for_plan(),
    )

    assert report.status == "blocked"
    codes = {issue.code for issue in report.issues}
    assert "formal_database_import_authorization_wrong_tool" in codes
    assert "formal_database_import_authorization_must_be_offline_no_write" in codes
    assert "formal_database_import_authorization_missing_required_flag" in codes


def test_formal_flow_contract_blocks_database_import_command_contract_that_is_not_offline(tmp_path):
    plan = build_full_flow_plan(config_path=_config(tmp_path), output_dir=tmp_path / "flow", run_id="demo")
    steps = list(plan.steps)
    index = [step.step_id for step in steps].index("formal_database_import_command_contract_snapshot")
    command = tuple(
        part
        for part in steps[index].command
        if part
        not in {
            "--formal-database-import-authorization-json",
            "--formal-database-import-preflight-json",
            "--archive-closure-json",
            "--evidence-bundle-json",
            "--dsn-env",
            "--requested-command-module",
            "--fail-on-blocker",
        }
    )
    broken = replace(
        steps[index],
        tool_module="gas_calibrator.tools.import_v1_5_evidence_package",
        execution_mode="offline_database_requires_configured_dsn",
        opens_com_ports=True,
        writes_coefficients=True,
        command=command,
    )
    steps[index] = broken

    report = validate_v1_5_formal_flow_contract(
        replace(plan, steps=tuple(steps)),
        inventory_entries=_inventory_for_plan(),
    )

    assert report.status == "blocked"
    codes = {issue.code for issue in report.issues}
    assert "formal_database_import_command_contract_wrong_tool" in codes
    assert "formal_database_import_command_contract_must_be_offline_no_write" in codes
    assert "formal_database_import_command_contract_missing_required_flag" in codes


def test_formal_flow_contract_blocks_database_import_blocked_executor_that_is_not_stubbed(tmp_path):
    plan = build_full_flow_plan(config_path=_config(tmp_path), output_dir=tmp_path / "flow", run_id="demo")
    steps = list(plan.steps)
    index = [step.step_id for step in steps].index("formal_database_import_blocked_executor_snapshot")
    command = tuple(
        part
        for part in steps[index].command
        if part
        not in {
            "--formal-database-import-command-contract-json",
            "--formal-database-import-authorization-json",
            "--formal-database-import-preflight-json",
            "--archive-closure-json",
            "--evidence-bundle-json",
            "--dsn-env",
            "--output-dir",
            "--fail-on-blocked",
        }
    )
    broken = replace(
        steps[index],
        tool_module="gas_calibrator.tools.export_v1_5_formal_database_import_command_contract",
        execution_mode="offline_database_requires_configured_dsn",
        opens_com_ports=True,
        writes_coefficients=True,
        command=command,
    )
    steps[index] = broken

    report = validate_v1_5_formal_flow_contract(
        replace(plan, steps=tuple(steps)),
        inventory_entries=_inventory_for_plan(),
    )

    assert report.status == "blocked"
    codes = {issue.code for issue in report.issues}
    assert "formal_database_import_blocked_executor_wrong_tool" in codes
    assert "formal_database_import_blocked_executor_must_be_offline_no_write" in codes
    assert "formal_database_import_blocked_executor_missing_required_flag" in codes


def test_formal_flow_contract_blocks_database_import_controlled_executor_design_that_is_not_offline(tmp_path):
    plan = build_full_flow_plan(config_path=_config(tmp_path), output_dir=tmp_path / "flow", run_id="demo")
    steps = list(plan.steps)
    index = [step.step_id for step in steps].index("formal_database_import_controlled_executor_design_snapshot")
    command = tuple(
        part
        for part in steps[index].command
        if part
        not in {
            "--formal-database-import-blocked-executor-json",
            "--dsn-env",
            "--output-dir",
        }
    )
    broken = replace(
        steps[index],
        tool_module="gas_calibrator.tools.import_v1_5_evidence_package",
        execution_mode="offline_database_requires_configured_dsn",
        opens_com_ports=True,
        writes_coefficients=True,
        command=command,
    )
    steps[index] = broken

    report = validate_v1_5_formal_flow_contract(
        replace(plan, steps=tuple(steps)),
        inventory_entries=_inventory_for_plan(),
    )

    assert report.status == "blocked"
    codes = {issue.code for issue in report.issues}
    assert "formal_database_import_controlled_executor_design_wrong_tool" in codes
    assert "formal_database_import_controlled_executor_design_must_be_offline_no_write" in codes
    assert "formal_database_import_controlled_executor_design_missing_required_flag" in codes


def test_export_formal_flow_contract_writes_json_and_markdown(tmp_path):
    plan = build_full_flow_plan(config_path=_config(tmp_path), output_dir=tmp_path / "flow", run_id="demo")
    plan_outputs = write_full_flow_plan(plan)
    out = tmp_path / "contract"

    rc = export_main(
        [
            "--plan-json",
            str(plan_outputs["json"]),
            "--output-dir",
            str(out),
            "--fail-on-blocked",
        ]
    )

    assert rc == 0
    payload = json.loads((out / "v1_5_formal_flow_contract.json").read_text(encoding="utf-8"))
    assert payload["status"] == "pass"
    assert payload["warnings"] == []
    assert payload["physical_boundaries"]["not_real_acceptance_evidence"] is True
    text = (out / "v1_5_formal_flow_contract.md").read_text(encoding="utf-8")
    assert "POST_WRITE_REVERIFY" in text
    assert "not_real_acceptance_evidence" in text
