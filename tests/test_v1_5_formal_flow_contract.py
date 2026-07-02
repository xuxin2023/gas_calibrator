import json
from dataclasses import replace

import pytest

from gas_calibrator.tools.export_v1_5_formal_flow_contract import main as export_main
from gas_calibrator.v1_5.orchestration.full_flow import build_full_flow_plan, write_full_flow_plan
from gas_calibrator.validation.v1_5_formal_flow_contract import validate_v1_5_formal_flow_contract


pytestmark = pytest.mark.v1_5_formal_gate


def _config(tmp_path):
    path = tmp_path / "config.json"
    path.write_text("{}", encoding="utf-8")
    return path


def _inventory_for_plan():
    return {
        "entries": [
            {
                "path": "src/gas_calibrator/tools/prepare_v1_5_formal_run_package.py",
                "category": "formal_review_evidence",
            },
            {
                "path": "src/gas_calibrator/tools/run_v1_5_formal_initialization_runner.py",
                "category": "formal_review_evidence",
            },
            {
                "path": "src/gas_calibrator/tools/export_v1_5_initialization_readiness.py",
                "category": "formal_review_evidence",
            },
            {
                "path": "src/gas_calibrator/tools/export_v1_5_pre_gas_readiness.py",
                "category": "formal_review_evidence",
            },
            {
                "path": "src/gas_calibrator/tools/probe_v1_5_getco_component_snapshot.py",
                "category": "formal_review_evidence",
            },
            {
                "path": "src/gas_calibrator/tools/export_v1_5_getco_identity_readiness.py",
                "category": "formal_review_evidence",
            },
            {
                "path": "src/gas_calibrator/tools/export_v1_5_pressure_senco9_evaluation.py",
                "category": "formal_review_evidence",
            },
            {
                "path": "src/gas_calibrator/tools/export_v1_5_pressure_channel_completion.py",
                "category": "formal_review_evidence",
            },
            {
                "path": "src/gas_calibrator/tools/export_v1_5_temperature_channel_review.py",
                "category": "formal_review_evidence",
            },
            {
                "path": "src/gas_calibrator/tools/run_v1_5_formal_co2_open_flow_queue.py",
                "category": "formal_runner",
            },
            {
                "path": "src/gas_calibrator/tools/run_v1_5_formal_h2o_open_flow_queue.py",
                "category": "formal_runner",
            },
            {
                "path": "src/gas_calibrator/tools/export_v1_5_fit_input_quality.py",
                "category": "formal_review_evidence",
            },
            {
                "path": "src/gas_calibrator/tools/export_v1_5_co2_senco_pair_model_scope.py",
                "category": "formal_review_evidence",
            },
            {
                "path": "src/gas_calibrator/tools/export_v1_5_post_write_reverification.py",
                "category": "formal_review_evidence",
            },
            {
                "path": "src/gas_calibrator/tools/run_v1_5_formal_evidence_sidecar.py",
                "category": "formal_review_evidence",
            },
            {
                "path": "src/gas_calibrator/tools/export_v1_5_formal_database_dry_run.py",
                "category": "formal_review_evidence",
            },
            {
                "path": "src/gas_calibrator/tools/export_v1_5_formal_database_import_preflight.py",
                "category": "formal_review_evidence",
            },
            {
                "path": "src/gas_calibrator/tools/export_v1_5_formal_database_import_authorization.py",
                "category": "formal_review_evidence",
            },
            {
                "path": "src/gas_calibrator/tools/import_v1_5_evidence_package.py",
                "category": "formal_review_evidence",
            },
            {
                "path": "src/gas_calibrator/tools/export_v1_5_calibration_reports.py",
                "category": "formal_review_evidence",
            },
            {
                "path": "src/gas_calibrator/tools/export_v1_5_run_evidence_status.py",
                "category": "formal_review_evidence",
            },
            {
                "path": "src/gas_calibrator/tools/export_v1_5_formal_run_status.py",
                "category": "formal_review_evidence",
            },
        ]
    }


def test_formal_flow_contract_passes_for_generated_plan(tmp_path):
    plan = build_full_flow_plan(config_path=_config(tmp_path), output_dir=tmp_path / "flow", run_id="demo")

    report = validate_v1_5_formal_flow_contract(plan, inventory_entries=_inventory_for_plan())

    assert report.status == "pass"
    assert report.issues == ()
    assert report.physical_boundaries["not_real_acceptance_evidence"] is True
    assert report.physical_boundaries["opens_com_ports"] is False
    assert report.formal_runner_steps == ("co2_open_flow_sampling", "h2o_open_flow_sampling")
    assert report.step_sequence.index("formal_initialization_contract_plan") < report.step_sequence.index(
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
        "database_import"
    )
    assert report.step_sequence.index("final_evidence_status_refresh") < report.step_sequence.index(
        "formal_run_status_snapshot"
    )
    assert "FORMAL_DATABASE_DRY_RUN" in "\n".join(report.physical_flow)
    assert "FORMAL_DATABASE_IMPORT_PREFLIGHT" in "\n".join(report.physical_flow)
    assert "FORMAL_DATABASE_IMPORT_AUTHORIZATION" in "\n".join(report.physical_flow)
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


def test_export_formal_flow_contract_writes_json_and_markdown(tmp_path):
    plan = build_full_flow_plan(config_path=_config(tmp_path), output_dir=tmp_path / "flow", run_id="demo")
    plan_outputs = write_full_flow_plan(plan)
    inventory_path = tmp_path / "inventory.json"
    inventory_path.write_text(json.dumps(_inventory_for_plan(), ensure_ascii=False), encoding="utf-8")
    out = tmp_path / "contract"

    rc = export_main(
        [
            "--plan-json",
            str(plan_outputs["json"]),
            "--inventory-json",
            str(inventory_path),
            "--output-dir",
            str(out),
            "--fail-on-blocked",
        ]
    )

    assert rc == 0
    payload = json.loads((out / "v1_5_formal_flow_contract.json").read_text(encoding="utf-8"))
    assert payload["status"] == "pass"
    assert payload["physical_boundaries"]["not_real_acceptance_evidence"] is True
    text = (out / "v1_5_formal_flow_contract.md").read_text(encoding="utf-8")
    assert "POST_WRITE_REVERIFY" in text
    assert "not_real_acceptance_evidence" in text
