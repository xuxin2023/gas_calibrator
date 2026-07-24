from pathlib import Path

import pytest

from gas_calibrator.tools.export_v1_5_entrypoint_inventory import main as export_main
from gas_calibrator.validation import v1_5_entrypoint_inventory as inventory_validation
from gas_calibrator.validation.v1_5_entrypoint_inventory import (
    CANONICAL_FORMAL_PATH,
    audit_v1_5_isolated_reference_integrity,
    build_v1_5_workspace_surface_rows,
    classify_v1_5_entrypoint,
    discover_v1_5_entrypoints,
    guardrailed_entrypoint_rows,
    summarize_entrypoints,
    validate_v1_5_active_surface_policy,
    validate_v1_5_canonical_formal_path_contract,
)


pytestmark = pytest.mark.v1_5_formal_gate


def test_entrypoint_classifier_separates_formal_runner_diagnostic_and_write(tmp_path: Path) -> None:
    root = tmp_path
    formal = root / "src/gas_calibrator/tools/run_v1_5_formal_co2_open_flow_queue.py"
    diagnostic = root / "src/gas_calibrator/tools/run_v1_5_open_flow_dynamic_pressure_diagnostic.py"
    write = root / "src/gas_calibrator/tools/run_v1_5_co2_senco13_controlled_write.py"
    for path in (formal, diagnostic, write):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("", encoding="utf-8")

    formal_entry = classify_v1_5_entrypoint(formal, root=root)
    diagnostic_entry = classify_v1_5_entrypoint(diagnostic, root=root)
    write_entry = classify_v1_5_entrypoint(write, root=root)

    assert formal_entry.category == "formal_runner"
    assert formal_entry.controls_routes is True
    assert diagnostic_entry.category == "diagnostic_only"
    assert diagnostic_entry.formal_status == "diagnostic_only"
    assert write_entry.category == "controlled_write"
    assert write_entry.writes_coefficients is True


def test_entrypoint_classifier_marks_open_flow_sampling_as_canonical_worker(tmp_path: Path) -> None:
    root = tmp_path
    co2_worker = root / "src/gas_calibrator/tools/run_v1_5_formal_open_flow_sampling.py"
    h2o_worker = root / "src/gas_calibrator/tools/run_v1_5_formal_h2o_open_flow_sampling.py"
    for path in (co2_worker, h2o_worker):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("", encoding="utf-8")

    co2_entry = classify_v1_5_entrypoint(co2_worker, root=root)
    h2o_entry = classify_v1_5_entrypoint(h2o_worker, root=root)

    for entry in (co2_entry, h2o_entry):
        assert entry.category == "formal_sampling_worker"
        assert entry.formal_status == "canonical_queue_worker"
        assert entry.opens_com_ports is True
        assert entry.controls_routes is True
        assert entry.writes_coefficients is False
        assert "canonical per-point sampling worker" in entry.notes[0]


def test_entrypoint_classifier_marks_minimal_readonly_com_executor_as_manual_support(
    tmp_path: Path,
) -> None:
    root = tmp_path
    executor = root / "src/gas_calibrator/tools/run_v1_5_formal_readonly_com_minimal_executor.py"
    executor.parent.mkdir(parents=True, exist_ok=True)
    executor.write_text("", encoding="utf-8")

    entry = classify_v1_5_entrypoint(executor, root=root)

    assert entry.category == "formal_review_evidence"
    assert entry.formal_status == "manual_authorized_read_only_com_support"
    assert entry.risk_level == "real_com_read_only_no_write_risk"
    assert entry.opens_com_ports is True
    assert entry.controls_routes is False
    assert entry.writes_coefficients is False
    assert "manual-authorized minimal read-only COM executor" in entry.notes[0]


def test_entrypoint_classifier_marks_pressure_runner_and_legacy_v1_reference(tmp_path: Path) -> None:
    root = tmp_path
    pressure_runner = root / "src/gas_calibrator/tools/validate_pressure_only.py"
    legacy = root / "src/gas_calibrator/tools/run_v1_corrected_autodelivery.py"
    for path in (pressure_runner, legacy):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("", encoding="utf-8")

    pressure_entry = classify_v1_5_entrypoint(pressure_runner, root=root)
    legacy_entry = classify_v1_5_entrypoint(legacy, root=root)

    assert pressure_entry.category == "formal_pressure_no_write_runner"
    assert pressure_entry.formal_status == "formal_pressure_no_write_when_authorized"
    assert pressure_entry.opens_com_ports is True
    assert pressure_entry.writes_coefficients is False
    assert legacy_entry.category == "legacy_v1_reference"
    assert legacy_entry.formal_status == "legacy_v1_reference_only"
    assert legacy_entry.writes_coefficients is True
    assert "legacy V1 reference only" in legacy_entry.notes[0]


def test_entrypoint_classifier_keeps_offline_sidecars_out_of_real_com_risk(tmp_path: Path) -> None:
    root = tmp_path
    sidecar = root / "src/gas_calibrator/tools/run_v1_5_formal_evidence_sidecar.py"
    offline_chain = root / "src/gas_calibrator/tools/run_v1_5_formal_offline_review_chain.py"
    full_chain = root / "src/gas_calibrator/tools/run_v1_5_full_calibration_chain.py"
    archive_closure = root / "src/gas_calibrator/tools/run_v1_5_formal_archive_closure.py"
    for path in (sidecar, offline_chain, full_chain, archive_closure):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("", encoding="utf-8")

    sidecar_entry = classify_v1_5_entrypoint(sidecar, root=root)
    offline_entry = classify_v1_5_entrypoint(offline_chain, root=root)
    full_chain_entry = classify_v1_5_entrypoint(full_chain, root=root)
    archive_entry = classify_v1_5_entrypoint(archive_closure, root=root)

    assert sidecar_entry.category == "formal_review_evidence"
    assert sidecar_entry.opens_com_ports is False
    assert offline_entry.category == "formal_review_evidence"
    assert offline_entry.risk_level == "offline"
    assert full_chain_entry.category == "full_flow_orchestration"
    assert full_chain_entry.controls_routes is False
    assert archive_entry.category == "formal_review_evidence"
    assert archive_entry.risk_level == "offline"
    assert archive_entry.opens_com_ports is False
    assert archive_entry.controls_routes is False
    assert archive_entry.notes == ("offline archive closure; does not open COM ports or control routes",)


def test_entrypoint_classifier_marks_web_console_as_offline_ui_review(tmp_path: Path) -> None:
    root = tmp_path
    web_console = root / "src/gas_calibrator/tools/run_v1_5_web_console.py"
    web_console.parent.mkdir(parents=True, exist_ok=True)
    web_console.write_text("", encoding="utf-8")

    entry = classify_v1_5_entrypoint(web_console, root=root)

    assert entry.category == "ui_review"
    assert entry.formal_status == "local_read_only_web_console"
    assert entry.risk_level == "offline"
    assert entry.opens_com_ports is False
    assert entry.controls_routes is False
    assert entry.writes_coefficients is False


def test_entrypoint_classifier_treats_getco_snapshot_as_formal_precheck(tmp_path: Path) -> None:
    root = tmp_path
    getco = root / "src/gas_calibrator/tools/probe_v1_5_getco_component_snapshot.py"
    dynamic_probe = root / "src/gas_calibrator/tools/probe_v1_5_open_flow_dynamic_pressure.py"
    for path in (getco, dynamic_probe):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("", encoding="utf-8")

    getco_entry = classify_v1_5_entrypoint(getco, root=root)
    dynamic_entry = classify_v1_5_entrypoint(dynamic_probe, root=root)

    assert getco_entry.category == "formal_review_evidence"
    assert getco_entry.formal_status == "formal_support"
    assert getco_entry.opens_com_ports is True
    assert "subordinate initialization evidence tool" in getco_entry.notes[0]
    assert dynamic_entry.category == "diagnostic_only"


def test_entrypoint_classifier_promotes_formal_initialization_runner_as_single_owner(tmp_path: Path) -> None:
    root = tmp_path
    initializer = root / "src/gas_calibrator/tools/run_v1_5_formal_initialization_runner.py"
    getco = root / "src/gas_calibrator/tools/probe_v1_5_getco_component_snapshot.py"
    for path in (initializer, getco):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("", encoding="utf-8")

    init_entry = classify_v1_5_entrypoint(initializer, root=root)
    getco_entry = classify_v1_5_entrypoint(getco, root=root)
    canonical = {row["stage"]: row["entrypoint"] for row in CANONICAL_FORMAL_PATH}

    assert canonical["01_formal_initialization"] == (
        "src/gas_calibrator/tools/run_v1_5_formal_initialization_runner.py"
    )
    assert "probe_v1_5_getco_component_snapshot.py" not in canonical.values()
    assert init_entry.category == "full_flow_orchestration"
    assert init_entry.formal_status == "canonical_initialization_planner"
    assert init_entry.opens_com_ports is False
    assert init_entry.controls_routes is False
    assert init_entry.writes_coefficients is False
    assert "canonical initialization owner" in init_entry.notes[0]
    assert getco_entry.category == "formal_review_evidence"
    assert getco_entry.opens_com_ports is True


def test_entrypoint_classifier_marks_initialization_support_tools(tmp_path: Path) -> None:
    root = tmp_path
    runtime_setup = root / "src/gas_calibrator/tools/run_v1_5_analyzer_runtime_setup.py"
    db_preflight = root / "src/gas_calibrator/tools/run_v1_5_initialization_db_preflight.py"
    sn_identity = root / "src/gas_calibrator/tools/run_v1_5_sn_identity_initialization.py"
    for path in (runtime_setup, db_preflight, sn_identity):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("", encoding="utf-8")

    runtime_entry = classify_v1_5_entrypoint(runtime_setup, root=root)
    db_entry = classify_v1_5_entrypoint(db_preflight, root=root)
    sn_entry = classify_v1_5_entrypoint(sn_identity, root=root)

    for entry in (runtime_entry, db_entry, sn_entry):
        assert entry.category == "identity_and_serial_binding"
        assert entry.formal_status == "formal_initialization_support"
        assert entry.controls_routes is False
        assert entry.writes_coefficients is False
        assert "formal initialization support" in entry.notes[0]
    assert runtime_entry.opens_com_ports is True
    assert sn_entry.opens_com_ports is True
    assert db_entry.opens_com_ports is False
    assert db_entry.risk_level == "offline"


def test_entrypoint_classifier_marks_route_readiness_as_formal_preflight_support(tmp_path: Path) -> None:
    root = tmp_path
    readiness = root / "src/gas_calibrator/tools/run_v1_5_formal_route_readiness_probe.py"
    initializer = root / "src/gas_calibrator/tools/run_v1_5_formal_initialization_runner.py"
    for path in (readiness, initializer):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("", encoding="utf-8")
    initializer.write_text(
        "from gas_calibrator.tools.run_v1_5_formal_route_readiness_probe import main\n",
        encoding="utf-8",
    )

    readiness_entry = classify_v1_5_entrypoint(readiness, root=root)
    issues = audit_v1_5_isolated_reference_integrity(root)

    assert readiness_entry.category == "formal_review_evidence"
    assert readiness_entry.formal_status == "formal_preflight_support"
    assert readiness_entry.opens_com_ports is True
    assert readiness_entry.controls_routes is True
    assert "formal route-readiness preflight support" in readiness_entry.notes[0]
    assert [
        issue.to_json()
        for issue in issues
        if issue.isolated_path == "src/gas_calibrator/tools/run_v1_5_formal_route_readiness_probe.py"
    ] == []


def test_entrypoint_classifier_marks_pre_gas_readiness_as_offline_sidecar(tmp_path: Path) -> None:
    root = tmp_path
    pre_gas = root / "src/gas_calibrator/tools/export_v1_5_pre_gas_readiness.py"
    init_executor_dry_run = root / "src/gas_calibrator/tools/export_v1_5_formal_initialization_executor_dry_run.py"
    init_blocked_executor = root / "src/gas_calibrator/tools/run_v1_5_formal_initialization_blocked_executor.py"
    init_controlled_executor_design = (
        root / "src/gas_calibrator/tools/export_v1_5_formal_initialization_controlled_executor_design.py"
    )
    init_readonly_com_preflight_design = (
        root
        / "src/gas_calibrator/tools/export_v1_5_formal_initialization_readonly_com_preflight_design.py"
    )
    init_readonly_com_preflight_blocked_executor = (
        root
        / "src/gas_calibrator/tools/run_v1_5_formal_initialization_readonly_com_preflight_blocked_executor.py"
    )
    init_readonly_com_preflight_controlled_executor_design = (
        root
        / "src/gas_calibrator/tools/export_v1_5_formal_initialization_readonly_com_preflight_controlled_executor_design.py"
    )
    init_readonly_com_preflight_controlled_blocked_executor = (
        root
        / "src/gas_calibrator/tools/run_v1_5_formal_initialization_readonly_com_preflight_controlled_blocked_executor.py"
    )
    readonly_com_execution_contract = (
        root / "src/gas_calibrator/tools/export_v1_5_formal_readonly_com_execution_contract.py"
    )
    readonly_com_execution_blocked_executor = (
        root / "src/gas_calibrator/tools/run_v1_5_formal_readonly_com_execution_blocked_executor.py"
    )
    readonly_com_execution_packet_validator = (
        root / "src/gas_calibrator/tools/export_v1_5_formal_readonly_com_execution_packet_validator.py"
    )
    readonly_com_execution_plan_preview = (
        root / "src/gas_calibrator/tools/export_v1_5_formal_readonly_com_execution_plan_preview.py"
    )
    readonly_com_minimal_executor_stub = (
        root / "src/gas_calibrator/tools/run_v1_5_formal_readonly_com_minimal_executor_stub.py"
    )
    getco_readiness = root / "src/gas_calibrator/tools/export_v1_5_getco_identity_readiness.py"
    formal_status = root / "src/gas_calibrator/tools/export_v1_5_formal_run_status.py"
    formal_database_dry_run = root / "src/gas_calibrator/tools/export_v1_5_formal_database_dry_run.py"
    formal_database_import_preflight = root / "src/gas_calibrator/tools/export_v1_5_formal_database_import_preflight.py"
    formal_database_import_authorization = root / "src/gas_calibrator/tools/export_v1_5_formal_database_import_authorization.py"
    formal_database_import_command_contract = (
        root / "src/gas_calibrator/tools/export_v1_5_formal_database_import_command_contract.py"
    )
    formal_database_import_blocked_executor = root / "src/gas_calibrator/tools/import_v1_5_evidence_package.py"
    formal_database_import_controlled_executor_design = (
        root / "src/gas_calibrator/tools/export_v1_5_formal_database_import_controlled_executor_design.py"
    )
    historical_replay = root / "src/gas_calibrator/tools/export_v1_5_historical_replay_contract.py"
    historical_replay_evidence = root / "src/gas_calibrator/tools/export_v1_5_historical_replay_evidence.py"
    historical_replay_missing_point = root / "src/gas_calibrator/tools/export_v1_5_historical_replay_missing_point_audit.py"
    historical_replay_qc_gap = root / "src/gas_calibrator/tools/export_v1_5_historical_replay_qc_gap_audit.py"
    algorithm_formal_point_plan = root / "src/gas_calibrator/tools/export_v1_5_algorithm_formal_point_plan_guard.py"
    algorithm_formal_runlist = root / "src/gas_calibrator/tools/export_v1_5_algorithm_formal_runlist_preview.py"
    algorithm_runlist_readiness = root / "src/gas_calibrator/tools/export_v1_5_algorithm_runlist_readiness.py"
    algorithm_runner_dry_run = root / "src/gas_calibrator/tools/export_v1_5_algorithm_runner_integration_dry_run.py"
    algorithm_profile_runner_dry_run = root / "src/gas_calibrator/tools/export_v1_5_algorithm_profile_runner_dry_run.py"
    algorithm_queue_handoff_preflight = root / "src/gas_calibrator/tools/export_v1_5_algorithm_queue_handoff_preflight.py"
    for path in (
        pre_gas,
        init_executor_dry_run,
        init_blocked_executor,
        init_controlled_executor_design,
        init_readonly_com_preflight_design,
        init_readonly_com_preflight_blocked_executor,
        init_readonly_com_preflight_controlled_executor_design,
        init_readonly_com_preflight_controlled_blocked_executor,
        readonly_com_execution_contract,
        readonly_com_execution_blocked_executor,
        readonly_com_execution_packet_validator,
        readonly_com_execution_plan_preview,
        readonly_com_minimal_executor_stub,
        getco_readiness,
        formal_status,
        formal_database_dry_run,
        formal_database_import_preflight,
        formal_database_import_authorization,
        formal_database_import_command_contract,
        formal_database_import_blocked_executor,
        formal_database_import_controlled_executor_design,
        historical_replay,
        historical_replay_evidence,
        historical_replay_missing_point,
        historical_replay_qc_gap,
        algorithm_formal_point_plan,
        algorithm_formal_runlist,
        algorithm_runlist_readiness,
        algorithm_runner_dry_run,
        algorithm_profile_runner_dry_run,
        algorithm_queue_handoff_preflight,
    ):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("", encoding="utf-8")

    entry = classify_v1_5_entrypoint(pre_gas, root=root)
    init_executor_dry_run_entry = classify_v1_5_entrypoint(init_executor_dry_run, root=root)
    init_blocked_executor_entry = classify_v1_5_entrypoint(init_blocked_executor, root=root)
    init_controlled_executor_design_entry = classify_v1_5_entrypoint(
        init_controlled_executor_design,
        root=root,
    )
    init_readonly_com_preflight_design_entry = classify_v1_5_entrypoint(
        init_readonly_com_preflight_design,
        root=root,
    )
    init_readonly_com_preflight_blocked_executor_entry = classify_v1_5_entrypoint(
        init_readonly_com_preflight_blocked_executor,
        root=root,
    )
    init_readonly_com_preflight_controlled_executor_design_entry = classify_v1_5_entrypoint(
        init_readonly_com_preflight_controlled_executor_design,
        root=root,
    )
    init_readonly_com_preflight_controlled_blocked_executor_entry = classify_v1_5_entrypoint(
        init_readonly_com_preflight_controlled_blocked_executor,
        root=root,
    )
    readonly_com_execution_contract_entry = classify_v1_5_entrypoint(
        readonly_com_execution_contract,
        root=root,
    )
    readonly_com_execution_blocked_executor_entry = classify_v1_5_entrypoint(
        readonly_com_execution_blocked_executor,
        root=root,
    )
    readonly_com_execution_packet_validator_entry = classify_v1_5_entrypoint(
        readonly_com_execution_packet_validator,
        root=root,
    )
    readonly_com_execution_plan_preview_entry = classify_v1_5_entrypoint(
        readonly_com_execution_plan_preview,
        root=root,
    )
    readonly_com_minimal_executor_stub_entry = classify_v1_5_entrypoint(
        readonly_com_minimal_executor_stub,
        root=root,
    )
    getco_entry = classify_v1_5_entrypoint(getco_readiness, root=root)
    formal_status_entry = classify_v1_5_entrypoint(formal_status, root=root)
    formal_database_dry_run_entry = classify_v1_5_entrypoint(formal_database_dry_run, root=root)
    formal_database_import_preflight_entry = classify_v1_5_entrypoint(formal_database_import_preflight, root=root)
    formal_database_import_authorization_entry = classify_v1_5_entrypoint(
        formal_database_import_authorization,
        root=root,
    )
    formal_database_import_command_contract_entry = classify_v1_5_entrypoint(
        formal_database_import_command_contract,
        root=root,
    )
    formal_database_import_blocked_executor_entry = classify_v1_5_entrypoint(
        formal_database_import_blocked_executor,
        root=root,
    )
    formal_database_import_controlled_executor_design_entry = classify_v1_5_entrypoint(
        formal_database_import_controlled_executor_design,
        root=root,
    )
    historical_replay_entry = classify_v1_5_entrypoint(historical_replay, root=root)
    historical_replay_evidence_entry = classify_v1_5_entrypoint(historical_replay_evidence, root=root)
    historical_replay_missing_point_entry = classify_v1_5_entrypoint(historical_replay_missing_point, root=root)
    historical_replay_qc_gap_entry = classify_v1_5_entrypoint(historical_replay_qc_gap, root=root)
    algorithm_formal_point_plan_entry = classify_v1_5_entrypoint(algorithm_formal_point_plan, root=root)
    algorithm_formal_runlist_entry = classify_v1_5_entrypoint(algorithm_formal_runlist, root=root)
    algorithm_runlist_readiness_entry = classify_v1_5_entrypoint(algorithm_runlist_readiness, root=root)
    algorithm_runner_dry_run_entry = classify_v1_5_entrypoint(algorithm_runner_dry_run, root=root)
    algorithm_profile_runner_dry_run_entry = classify_v1_5_entrypoint(algorithm_profile_runner_dry_run, root=root)
    algorithm_queue_handoff_preflight_entry = classify_v1_5_entrypoint(algorithm_queue_handoff_preflight, root=root)

    assert entry.category == "formal_review_evidence"
    assert entry.formal_status == "formal_support"
    assert entry.risk_level == "offline"
    assert entry.opens_com_ports is False
    assert entry.controls_routes is False
    assert entry.writes_coefficients is False
    assert "offline pre-gas readiness sidecar" in entry.notes[0]
    assert init_executor_dry_run_entry.category == "formal_review_evidence"
    assert init_executor_dry_run_entry.formal_status == "formal_support"
    assert init_executor_dry_run_entry.risk_level == "offline"
    assert init_executor_dry_run_entry.opens_com_ports is False
    assert init_executor_dry_run_entry.controls_routes is False
    assert init_executor_dry_run_entry.writes_coefficients is False
    assert "offline initialization executor dry-run review" in init_executor_dry_run_entry.notes[0]
    assert init_blocked_executor_entry.category == "formal_review_evidence"
    assert init_blocked_executor_entry.formal_status == "formal_support"
    assert init_blocked_executor_entry.risk_level == "offline"
    assert init_blocked_executor_entry.opens_com_ports is False
    assert init_blocked_executor_entry.controls_routes is False
    assert init_blocked_executor_entry.writes_coefficients is False
    assert "offline initialization blocked executor stub" in init_blocked_executor_entry.notes[0]
    assert init_controlled_executor_design_entry.category == "formal_review_evidence"
    assert init_controlled_executor_design_entry.formal_status == "formal_support"
    assert init_controlled_executor_design_entry.risk_level == "offline"
    assert init_controlled_executor_design_entry.opens_com_ports is False
    assert init_controlled_executor_design_entry.controls_routes is False
    assert init_controlled_executor_design_entry.writes_coefficients is False
    assert "offline initialization controlled executor design" in init_controlled_executor_design_entry.notes[0]
    assert init_readonly_com_preflight_design_entry.category == "formal_review_evidence"
    assert init_readonly_com_preflight_design_entry.formal_status == "formal_support"
    assert init_readonly_com_preflight_design_entry.risk_level == "offline"
    assert init_readonly_com_preflight_design_entry.opens_com_ports is False
    assert init_readonly_com_preflight_design_entry.controls_routes is False
    assert init_readonly_com_preflight_design_entry.writes_coefficients is False
    assert (
        "offline initialization read-only real-COM preflight design"
        in init_readonly_com_preflight_design_entry.notes[0]
    )
    assert init_readonly_com_preflight_blocked_executor_entry.category == "formal_review_evidence"
    assert init_readonly_com_preflight_blocked_executor_entry.formal_status == "formal_support"
    assert init_readonly_com_preflight_blocked_executor_entry.risk_level == "offline"
    assert init_readonly_com_preflight_blocked_executor_entry.opens_com_ports is False
    assert init_readonly_com_preflight_blocked_executor_entry.controls_routes is False
    assert init_readonly_com_preflight_blocked_executor_entry.writes_coefficients is False
    assert (
        "offline initialization read-only real-COM preflight blocked executor stub"
        in init_readonly_com_preflight_blocked_executor_entry.notes[0]
    )
    assert init_readonly_com_preflight_controlled_executor_design_entry.category == "formal_review_evidence"
    assert init_readonly_com_preflight_controlled_executor_design_entry.formal_status == "formal_support"
    assert init_readonly_com_preflight_controlled_executor_design_entry.risk_level == "offline"
    assert init_readonly_com_preflight_controlled_executor_design_entry.opens_com_ports is False
    assert init_readonly_com_preflight_controlled_executor_design_entry.controls_routes is False
    assert init_readonly_com_preflight_controlled_executor_design_entry.writes_coefficients is False
    assert (
        "offline initialization read-only real-COM preflight controlled executor design"
        in init_readonly_com_preflight_controlled_executor_design_entry.notes[0]
    )
    assert init_readonly_com_preflight_controlled_blocked_executor_entry.category == "formal_review_evidence"
    assert init_readonly_com_preflight_controlled_blocked_executor_entry.formal_status == "formal_support"
    assert init_readonly_com_preflight_controlled_blocked_executor_entry.risk_level == "offline"
    assert init_readonly_com_preflight_controlled_blocked_executor_entry.opens_com_ports is False
    assert init_readonly_com_preflight_controlled_blocked_executor_entry.controls_routes is False
    assert init_readonly_com_preflight_controlled_blocked_executor_entry.writes_coefficients is False
    assert (
        "offline initialization read-only real-COM preflight controlled blocked executor stub"
        in init_readonly_com_preflight_controlled_blocked_executor_entry.notes[0]
    )
    assert readonly_com_execution_contract_entry.category == "formal_review_evidence"
    assert readonly_com_execution_contract_entry.formal_status == "formal_support"
    assert readonly_com_execution_contract_entry.risk_level == "offline"
    assert readonly_com_execution_contract_entry.opens_com_ports is False
    assert readonly_com_execution_contract_entry.controls_routes is False
    assert readonly_com_execution_contract_entry.writes_coefficients is False
    assert "offline read-only COM execution packet contract" in readonly_com_execution_contract_entry.notes[0]
    assert readonly_com_execution_blocked_executor_entry.category == "formal_review_evidence"
    assert readonly_com_execution_blocked_executor_entry.formal_status == "formal_support"
    assert readonly_com_execution_blocked_executor_entry.risk_level == "offline"
    assert readonly_com_execution_blocked_executor_entry.opens_com_ports is False
    assert readonly_com_execution_blocked_executor_entry.controls_routes is False
    assert readonly_com_execution_blocked_executor_entry.writes_coefficients is False
    assert (
        "offline read-only COM execution blocked executor stub"
        in readonly_com_execution_blocked_executor_entry.notes[0]
    )
    assert readonly_com_execution_packet_validator_entry.category == "formal_review_evidence"
    assert readonly_com_execution_packet_validator_entry.formal_status == "formal_support"
    assert readonly_com_execution_packet_validator_entry.risk_level == "offline"
    assert readonly_com_execution_packet_validator_entry.opens_com_ports is False
    assert readonly_com_execution_packet_validator_entry.controls_routes is False
    assert readonly_com_execution_packet_validator_entry.writes_coefficients is False
    assert (
        "offline read-only COM execution packet validator"
        in readonly_com_execution_packet_validator_entry.notes[0]
    )
    assert readonly_com_execution_plan_preview_entry.category == "formal_review_evidence"
    assert readonly_com_execution_plan_preview_entry.formal_status == "formal_support"
    assert readonly_com_execution_plan_preview_entry.risk_level == "offline"
    assert readonly_com_execution_plan_preview_entry.opens_com_ports is False
    assert readonly_com_execution_plan_preview_entry.controls_routes is False
    assert readonly_com_execution_plan_preview_entry.writes_coefficients is False
    assert (
        "offline read-only COM execution plan preview"
        in readonly_com_execution_plan_preview_entry.notes[0]
    )
    assert readonly_com_minimal_executor_stub_entry.category == "formal_review_evidence"
    assert readonly_com_minimal_executor_stub_entry.formal_status == "formal_support"
    assert readonly_com_minimal_executor_stub_entry.risk_level == "offline"
    assert readonly_com_minimal_executor_stub_entry.opens_com_ports is False
    assert readonly_com_minimal_executor_stub_entry.controls_routes is False
    assert readonly_com_minimal_executor_stub_entry.writes_coefficients is False
    assert (
        "offline read-only COM minimal executor stub"
        in readonly_com_minimal_executor_stub_entry.notes[0]
    )
    assert getco_entry.category == "formal_review_evidence"
    assert getco_entry.formal_status == "formal_support"
    assert getco_entry.risk_level == "offline"
    assert getco_entry.opens_com_ports is False
    assert getco_entry.controls_routes is False
    assert getco_entry.writes_coefficients is False
    assert "offline identity/GETCO readiness sidecar" in getco_entry.notes[0]
    assert formal_status_entry.category == "formal_review_evidence"
    assert formal_status_entry.formal_status == "formal_support"
    assert formal_status_entry.risk_level == "offline"
    assert formal_status_entry.opens_com_ports is False
    assert formal_status_entry.controls_routes is False
    assert formal_status_entry.writes_coefficients is False
    assert "offline formal run status rollup" in formal_status_entry.notes[0]
    assert formal_database_dry_run_entry.category == "formal_review_evidence"
    assert formal_database_dry_run_entry.formal_status == "formal_support"
    assert formal_database_dry_run_entry.risk_level == "offline"
    assert formal_database_dry_run_entry.opens_com_ports is False
    assert formal_database_dry_run_entry.controls_routes is False
    assert formal_database_dry_run_entry.writes_coefficients is False
    assert "offline PostgreSQL 18 database dry-run contract" in formal_database_dry_run_entry.notes[0]
    assert formal_database_import_preflight_entry.category == "formal_review_evidence"
    assert formal_database_import_preflight_entry.formal_status == "formal_support"
    assert formal_database_import_preflight_entry.risk_level == "offline"
    assert formal_database_import_preflight_entry.opens_com_ports is False
    assert formal_database_import_preflight_entry.controls_routes is False
    assert formal_database_import_preflight_entry.writes_coefficients is False
    assert "offline PostgreSQL 18 database import preflight" in formal_database_import_preflight_entry.notes[0]
    assert formal_database_import_authorization_entry.category == "formal_review_evidence"
    assert formal_database_import_authorization_entry.formal_status == "formal_support"
    assert formal_database_import_authorization_entry.risk_level == "offline"
    assert formal_database_import_authorization_entry.opens_com_ports is False
    assert formal_database_import_authorization_entry.controls_routes is False
    assert formal_database_import_authorization_entry.writes_coefficients is False
    assert (
        "offline PostgreSQL 18 database import authorization guard"
        in formal_database_import_authorization_entry.notes[0]
    )
    assert formal_database_import_command_contract_entry.category == "formal_review_evidence"
    assert formal_database_import_command_contract_entry.formal_status == "formal_support"
    assert formal_database_import_command_contract_entry.risk_level == "offline"
    assert formal_database_import_command_contract_entry.opens_com_ports is False
    assert formal_database_import_command_contract_entry.controls_routes is False
    assert formal_database_import_command_contract_entry.writes_coefficients is False
    assert (
        "offline PostgreSQL 18 database import command contract"
        in formal_database_import_command_contract_entry.notes[0]
    )
    assert formal_database_import_blocked_executor_entry.category == "formal_review_evidence"
    assert formal_database_import_blocked_executor_entry.formal_status == "formal_support"
    assert formal_database_import_blocked_executor_entry.risk_level == "offline"
    assert formal_database_import_blocked_executor_entry.opens_com_ports is False
    assert formal_database_import_blocked_executor_entry.controls_routes is False
    assert formal_database_import_blocked_executor_entry.writes_coefficients is False
    assert (
        "offline PostgreSQL 18 blocked import executor stub"
        in formal_database_import_blocked_executor_entry.notes[0]
    )
    assert formal_database_import_controlled_executor_design_entry.category == "formal_review_evidence"
    assert formal_database_import_controlled_executor_design_entry.formal_status == "formal_support"
    assert formal_database_import_controlled_executor_design_entry.risk_level == "offline"
    assert formal_database_import_controlled_executor_design_entry.opens_com_ports is False
    assert formal_database_import_controlled_executor_design_entry.controls_routes is False
    assert formal_database_import_controlled_executor_design_entry.writes_coefficients is False
    assert (
        "offline PostgreSQL 18 controlled import executor design"
        in formal_database_import_controlled_executor_design_entry.notes[0]
    )
    assert historical_replay_entry.category == "formal_review_evidence"
    assert historical_replay_entry.formal_status == "formal_support"
    assert historical_replay_entry.risk_level == "offline"
    assert historical_replay_entry.opens_com_ports is False
    assert historical_replay_entry.controls_routes is False
    assert historical_replay_entry.writes_coefficients is False
    assert "offline historical replay contract" in historical_replay_entry.notes[0]
    assert historical_replay_evidence_entry.category == "formal_review_evidence"
    assert historical_replay_evidence_entry.formal_status == "formal_support"
    assert historical_replay_evidence_entry.risk_level == "offline"
    assert historical_replay_evidence_entry.opens_com_ports is False
    assert historical_replay_evidence_entry.controls_routes is False
    assert historical_replay_evidence_entry.writes_coefficients is False
    assert "offline historical replay evidence binder" in historical_replay_evidence_entry.notes[0]
    assert historical_replay_missing_point_entry.category == "formal_review_evidence"
    assert historical_replay_missing_point_entry.formal_status == "formal_support"
    assert historical_replay_missing_point_entry.risk_level == "offline"
    assert historical_replay_missing_point_entry.opens_com_ports is False
    assert historical_replay_missing_point_entry.controls_routes is False
    assert historical_replay_missing_point_entry.writes_coefficients is False
    assert "offline historical replay missing-point audit" in historical_replay_missing_point_entry.notes[0]
    assert historical_replay_qc_gap_entry.category == "formal_review_evidence"
    assert historical_replay_qc_gap_entry.formal_status == "formal_support"
    assert historical_replay_qc_gap_entry.risk_level == "offline"
    assert historical_replay_qc_gap_entry.opens_com_ports is False
    assert historical_replay_qc_gap_entry.controls_routes is False
    assert historical_replay_qc_gap_entry.writes_coefficients is False
    assert "offline historical replay QC gap audit" in historical_replay_qc_gap_entry.notes[0]
    assert algorithm_formal_point_plan_entry.category == "formal_review_evidence"
    assert algorithm_formal_point_plan_entry.formal_status == "formal_support"
    assert algorithm_formal_point_plan_entry.risk_level == "offline"
    assert algorithm_formal_point_plan_entry.opens_com_ports is False
    assert algorithm_formal_point_plan_entry.controls_routes is False
    assert algorithm_formal_point_plan_entry.writes_coefficients is False
    assert "offline algorithm formal point-plan guard" in algorithm_formal_point_plan_entry.notes[0]
    assert algorithm_formal_runlist_entry.category == "formal_review_evidence"
    assert algorithm_formal_runlist_entry.formal_status == "formal_support"
    assert algorithm_formal_runlist_entry.risk_level == "offline"
    assert algorithm_formal_runlist_entry.opens_com_ports is False
    assert algorithm_formal_runlist_entry.controls_routes is False
    assert algorithm_formal_runlist_entry.writes_coefficients is False
    assert "offline algorithm formal runlist preview" in algorithm_formal_runlist_entry.notes[0]
    assert algorithm_runlist_readiness_entry.category == "formal_review_evidence"
    assert algorithm_runlist_readiness_entry.formal_status == "formal_support"
    assert algorithm_runlist_readiness_entry.risk_level == "offline"
    assert algorithm_runlist_readiness_entry.opens_com_ports is False
    assert algorithm_runlist_readiness_entry.controls_routes is False
    assert algorithm_runlist_readiness_entry.writes_coefficients is False
    assert "offline algorithm runlist readiness gate" in algorithm_runlist_readiness_entry.notes[0]
    assert algorithm_runner_dry_run_entry.category == "formal_review_evidence"
    assert algorithm_runner_dry_run_entry.formal_status == "formal_support"
    assert algorithm_runner_dry_run_entry.risk_level == "offline"
    assert algorithm_runner_dry_run_entry.opens_com_ports is False
    assert algorithm_runner_dry_run_entry.controls_routes is False
    assert algorithm_runner_dry_run_entry.writes_coefficients is False
    assert "offline algorithm runner integration dry-run" in algorithm_runner_dry_run_entry.notes[0]
    assert algorithm_profile_runner_dry_run_entry.category == "formal_review_evidence"
    assert algorithm_profile_runner_dry_run_entry.formal_status == "formal_support"
    assert algorithm_profile_runner_dry_run_entry.risk_level == "offline"
    assert algorithm_profile_runner_dry_run_entry.opens_com_ports is False
    assert algorithm_profile_runner_dry_run_entry.controls_routes is False
    assert algorithm_profile_runner_dry_run_entry.writes_coefficients is False
    assert "offline algorithm profile runner dry-run" in algorithm_profile_runner_dry_run_entry.notes[0]
    assert algorithm_queue_handoff_preflight_entry.category == "formal_review_evidence"
    assert algorithm_queue_handoff_preflight_entry.formal_status == "formal_support"
    assert algorithm_queue_handoff_preflight_entry.risk_level == "offline"
    assert algorithm_queue_handoff_preflight_entry.opens_com_ports is False
    assert algorithm_queue_handoff_preflight_entry.controls_routes is False
    assert algorithm_queue_handoff_preflight_entry.writes_coefficients is False
    assert "offline algorithm queue handoff preflight" in algorithm_queue_handoff_preflight_entry.notes[0]


def test_entrypoint_discovery_finds_v1_5_tools_libraries_and_tests(tmp_path: Path) -> None:
    paths = [
        "src/gas_calibrator/tools/export_v1_5_formal_readiness.py",
        "src/gas_calibrator/tools/run_v1_5_h2o_senco24_controlled_write.py",
        "src/gas_calibrator/tools/run_v1_5_formal_initialization_runner.py",
        "src/gas_calibrator/tools/verify_v1_5_evidence_bundle.py",
        "src/gas_calibrator/v1_5/orchestration/full_flow.py",
        "src/gas_calibrator/storage/v1_5_evidence/repository.py",
        "src/gas_calibrator/tools/validate_pressure_only.py",
        "src/gas_calibrator/tools/run_v1_online_acceptance.py",
        "tests/test_v1_5_formal_readiness.py",
        "tests/test_unrelated.py",
    ]
    for item in paths:
        path = tmp_path / item
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("", encoding="utf-8")

    entries = discover_v1_5_entrypoints(tmp_path)
    names = {entry.name for entry in entries}
    summary = summarize_entrypoints(entries)

    assert "export_v1_5_formal_readiness" in names
    assert "run_v1_5_h2o_senco24_controlled_write" in names
    assert "run_v1_5_formal_initialization_runner" in names
    assert "verify_v1_5_evidence_bundle" in names
    assert "full_flow" in names
    assert "repository" in names
    assert "validate_pressure_only" in names
    assert "run_v1_online_acceptance" in names
    assert "test_v1_5_formal_readiness" in names
    assert "test_unrelated" not in names
    assert summary["controlled_write"] == 1
    assert summary["formal_review_evidence"] == 2
    assert summary["formal_pressure_no_write_runner"] == 1
    assert summary["full_flow_orchestration"] == 2
    assert summary["legacy_v1_reference"] == 1
    assert summary["test_gate"] == 1


def test_resume_offline_post_execution_verifier_is_offline_support() -> None:
    root = Path(__file__).resolve().parents[1]
    entry = classify_v1_5_entrypoint(
        root
        / "src/gas_calibrator/tools/export_v1_5_authoritative_resume_offline_post_execution_verifier.py",
        root=root,
    )
    assert entry.category == "formal_review_evidence"
    assert entry.formal_status == "formal_support"
    assert entry.risk_level == "offline"
    assert entry.opens_com_ports is False
    assert entry.controls_routes is False
    assert entry.writes_coefficients is False
    assert "post-execution verifier" in entry.notes[0]


def test_resume_offline_state_advance_preflight_is_offline_support() -> None:
    root = Path(__file__).resolve().parents[1]
    entry = classify_v1_5_entrypoint(
        root
        / "src/gas_calibrator/tools/export_v1_5_authoritative_resume_offline_state_advance_preflight.py",
        root=root,
    )
    assert entry.category == "formal_review_evidence"
    assert entry.formal_status == "formal_support"
    assert entry.risk_level == "offline"
    assert entry.opens_com_ports is False
    assert entry.controls_routes is False
    assert entry.writes_coefficients is False
    assert "state-advance preflight" in entry.notes[0]


def test_resume_offline_state_advance_authorization_tools_are_offline_support() -> None:
    root = Path(__file__).resolve().parents[1]
    paths = (
        root
        / "src/gas_calibrator/tools/export_v1_5_authoritative_resume_offline_state_advance_authorization.py",
        root
        / "src/gas_calibrator/tools/run_v1_5_authoritative_resume_offline_state_advance_blocked_executor.py",
    )
    entries = [classify_v1_5_entrypoint(path, root=root) for path in paths]
    assert all(entry.category == "formal_review_evidence" for entry in entries)
    assert all(entry.formal_status == "formal_support" for entry in entries)
    assert all(entry.risk_level == "offline" for entry in entries)
    assert all(entry.opens_com_ports is False for entry in entries)
    assert all(entry.controls_routes is False for entry in entries)
    assert all(entry.writes_coefficients is False for entry in entries)
    assert "authorization validator" in entries[0].notes[0]
    assert "blocked executor" in entries[1].notes[0]


def test_resume_offline_state_advance_atomic_writer_is_manual_state_write_only() -> None:
    root = Path(__file__).resolve().parents[1]
    entry = classify_v1_5_entrypoint(
        root
        / "src/gas_calibrator/tools/run_v1_5_authoritative_resume_offline_state_advance_atomic_writer.py",
        root=root,
    )
    assert entry.category == "controlled_state_writer"
    assert entry.formal_status == "manual_authorized_only"
    assert entry.risk_level == "state_file_write_risk"
    assert entry.opens_com_ports is False
    assert entry.controls_routes is False
    assert entry.writes_coefficients is False
    assert "one-step offline resume state-advance writer" in entry.notes[0]


def test_resume_offline_state_advance_post_write_tools_are_offline_support() -> None:
    root = Path(__file__).resolve().parents[1]
    paths = (
        root
        / "src/gas_calibrator/tools/export_v1_5_authoritative_resume_offline_state_advance_post_write_verification.py",
        root
        / "src/gas_calibrator/tools/export_v1_5_authoritative_resume_offline_state_advance_consumer_readiness.py",
    )
    entries = [classify_v1_5_entrypoint(path, root=root) for path in paths]
    assert all(entry.category == "formal_review_evidence" for entry in entries)
    assert all(entry.formal_status == "formal_support" for entry in entries)
    assert all(entry.risk_level == "offline" for entry in entries)
    assert all(entry.opens_com_ports is False for entry in entries)
    assert all(entry.controls_routes is False for entry in entries)
    assert all(entry.writes_coefficients is False for entry in entries)
    assert "post-write verifier" in entries[0].notes[0]
    assert "consumer readiness gate" in entries[1].notes[0]


def test_resume_offline_state_advance_next_step_plan_is_offline_support() -> None:
    root = Path(__file__).resolve().parents[1]
    entry = classify_v1_5_entrypoint(
        root
        / "src/gas_calibrator/tools/export_v1_5_authoritative_resume_offline_state_advance_next_step_plan.py",
        root=root,
    )
    assert entry.category == "formal_review_evidence"
    assert entry.formal_status == "formal_support"
    assert entry.risk_level == "offline"
    assert entry.opens_com_ports is False
    assert entry.controls_routes is False
    assert entry.writes_coefficients is False
    assert "next-step preview" in entry.notes[0]


def test_resume_offline_state_advance_next_step_authorization_is_offline_support() -> None:
    root = Path(__file__).resolve().parents[1]
    entry = classify_v1_5_entrypoint(
        root
        / "src/gas_calibrator/tools/export_v1_5_authoritative_resume_offline_state_advance_next_step_authorization_preflight.py",
        root=root,
    )
    assert entry.category == "formal_review_evidence"
    assert entry.formal_status == "formal_support"
    assert entry.risk_level == "offline"
    assert entry.opens_com_ports is False
    assert entry.controls_routes is False
    assert entry.writes_coefficients is False
    assert "next-step review authorization preflight" in entry.notes[0]


def test_resume_offline_state_advance_next_step_blocked_executor_is_offline_support() -> None:
    root = Path(__file__).resolve().parents[1]
    entry = classify_v1_5_entrypoint(
        root
        / "src/gas_calibrator/tools/run_v1_5_authoritative_resume_offline_state_advance_next_step_blocked_executor.py",
        root=root,
    )
    assert entry.category == "formal_review_evidence"
    assert entry.formal_status == "formal_support"
    assert entry.risk_level == "offline"
    assert entry.opens_com_ports is False
    assert entry.controls_routes is False
    assert entry.writes_coefficients is False
    assert "next-step blocked executor" in entry.notes[0]


def test_resume_offline_state_advance_next_step_controlled_design_is_offline_support() -> None:
    root = Path(__file__).resolve().parents[1]
    entry = classify_v1_5_entrypoint(
        root
        / "src/gas_calibrator/tools/export_v1_5_authoritative_resume_offline_state_advance_next_step_controlled_executor_design.py",
        root=root,
    )
    assert entry.category == "formal_review_evidence"
    assert entry.formal_status == "formal_support"
    assert entry.risk_level == "offline"
    assert entry.opens_com_ports is False
    assert entry.controls_routes is False
    assert entry.writes_coefficients is False
    assert "controlled executor design" in entry.notes[0]


def test_export_entrypoint_inventory_writes_review_artifacts(tmp_path: Path) -> None:
    tool = tmp_path / "src/gas_calibrator/tools/run_v1_5_formal_open_flow_sampling.py"
    test_file = tmp_path / "tests/test_v1_5_formal_open_flow_sampling_runner.py"
    v2_file = tmp_path / "src/gas_calibrator/v2/legacy_runner.py"
    observed_config = tmp_path / "configs/site_v1_5_current_observed_6ch.json"
    for path in (tool, test_file, v2_file, observed_config):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("", encoding="utf-8")

    out = tmp_path / "out"
    rc = export_main(["--repo-root", str(tmp_path), "--output-dir", str(out)])

    assert rc == 0
    assert (out / "v1_5_entrypoint_inventory.json").exists()
    assert (out / "v1_5_entrypoint_inventory.csv").exists()
    assert (out / "v1_5_file_convergence_report.md").exists()
    assert (out / "v1_5_active_surface_report.md").exists()
    assert (out / "v1_5_isolation_reference_audit.md").exists()
    payload = (out / "v1_5_entrypoint_inventory.json").read_text(encoding="utf-8")
    assert "active_surface_boundaries" in payload
    assert "active_surface_policy" in payload
    assert "canonical_formal_path_policy" in payload
    assert "isolation_reference_audit" in payload
    assert '"canonical_formal_path_policy": {\n    "status": "blocked"' in payload
    assert "canonical_entrypoint_missing" in payload
    active_surface = (out / "v1_5_active_surface_report.md").read_text(encoding="utf-8-sig")
    assert "V1.5 活跃工作面与隔离清单" in active_surface
    assert "默认入口策略校验" in active_surface
    assert "legacy_v2_source_tree" in active_surface
    assert "temporary_or_observed_v1_5_configs" in active_surface
    md = (out / "v1_5_formal_entrypoints.md").read_text(encoding="utf-8")
    assert "V1.5 formal entrypoint inventory" in md
    assert "Canonical V1.5 Formal Path" in md
    assert "Completion Matrix" in md
    assert "Do Not Start Here" in md
    assert "`formal_runner`" in md
    assert "`diagnostic_only` entries must not be used as formal acceptance inputs by default." in md
    convergence = (out / "v1_5_file_convergence_report.md").read_text(encoding="utf-8")
    assert "V1.5 文件收敛报告" in convergence
    assert "它不是第二套入口清单" in convergence
    assert "不要从这里启动正式流程" in convergence
    assert "CO2 零气锚点与 H2O 干气低水锚点不是同一个物理概念" in convergence
    assert (out / "v1_5_file_convergence_report.md").read_bytes().startswith(b"\xef\xbb\xbf")
    isolation_reference = (out / "v1_5_isolation_reference_audit.md").read_text(encoding="utf-8")
    assert "V1.5 isolation reference audit" in isolation_reference
    assert "offline source audit only" in isolation_reference


def test_workspace_surface_marks_legacy_and_temporary_surfaces(tmp_path: Path) -> None:
    paths = [
        "src/gas_calibrator/v2/runner.py",
        "src/gas_calibrator/v2/__pycache__/runner.cpython-313.pyc",
        "tests/v2/test_runner.py",
        "docs/architecture/v2_cutover_checklist.md",
        "src/gas_calibrator/tools/run_v1_corrected_autodelivery.py",
        "src/gas_calibrator/tools/run_v1_5_open_flow_dynamic_pressure_diagnostic.py",
        "configs/site_v1_5_formal_current_observed_6ch.json",
        "root_probe.stdout.log",
    ]
    for item in paths:
        path = tmp_path / item
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("", encoding="utf-8")

    rows = build_v1_5_workspace_surface_rows(tmp_path)
    by_surface = {row.surface: row for row in rows}

    assert by_surface["legacy_v2_source_tree"].status == "legacy_reference_only"
    assert by_surface["legacy_v2_source_tree"].action == "exclude_from_v1_5_active_surface"
    assert by_surface["legacy_v2_tests"].action == "exclude_from_v1_5_active_surface"
    assert by_surface["legacy_v2_docs"].action == "exclude_from_v1_5_active_surface"
    assert by_surface["legacy_v1_reference_tools"].action == "do_not_start_v1_5_here"
    assert by_surface["v1_5_diagnostic_tools"].action == "guarded_engineering_use_only"
    assert by_surface["temporary_or_observed_v1_5_configs"].action == "do_not_use_as_default_config"
    assert by_surface["root_temporary_run_artifacts"].action == "move_to_logs_or_archive_after_review"
    assert "src/gas_calibrator/v2/runner.py" in by_surface["legacy_v2_source_tree"].examples
    assert "src/gas_calibrator/v2/__pycache__/runner.cpython-313.pyc" not in by_surface[
        "legacy_v2_source_tree"
    ].examples
    assert by_surface["legacy_v2_source_tree"].file_count == 1
    assert by_surface["temporary_or_observed_v1_5_configs"].file_count == 1


def test_active_surface_policy_has_no_repository_blockers() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    issues = validate_v1_5_active_surface_policy(repo_root)

    assert [issue.to_json() for issue in issues if issue.severity == "blocker"] == []


def test_canonical_formal_path_contract_has_no_repository_blockers() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    issues = validate_v1_5_canonical_formal_path_contract(repo_root)

    assert [issue.to_json() for issue in issues if issue.severity == "blocker"] == []


def test_canonical_formal_path_contract_blocks_stage_order_drift(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    mutated_path = tuple(reversed(CANONICAL_FORMAL_PATH))
    monkeypatch.setattr(inventory_validation, "CANONICAL_FORMAL_PATH", mutated_path)

    issues = inventory_validation.validate_v1_5_canonical_formal_path_contract(tmp_path, entries=[])
    blocker_rules = {issue.rule for issue in issues if issue.severity == "blocker"}

    assert "canonical_stage_order_changed" in blocker_rules


def test_canonical_formal_path_contract_blocks_support_tool_as_stage_owner(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    support = tmp_path / "src/gas_calibrator/tools/run_v1_5_analyzer_runtime_setup.py"
    support.parent.mkdir(parents=True, exist_ok=True)
    support.write_text("", encoding="utf-8")
    monkeypatch.setattr(
        inventory_validation,
        "CANONICAL_FORMAL_PATH",
        (
            {
                "stage": "01_formal_initialization",
                "entrypoint": "src/gas_calibrator/tools/run_v1_5_analyzer_runtime_setup.py",
                "category": "identity_and_serial_binding",
                "status": "bad",
                "physical_meaning": "bad",
                "safety_boundary": "bad",
            },
        ),
    )

    issues = inventory_validation.validate_v1_5_canonical_formal_path_contract(
        tmp_path,
        entries=discover_v1_5_entrypoints(tmp_path),
    )
    blocker_rules = {issue.rule for issue in issues if issue.severity == "blocker"}

    assert "canonical_stage_order_changed" in blocker_rules
    assert "canonical_uses_support_tool_as_stage_owner" in blocker_rules


def test_isolation_reference_audit_blocks_formal_runtime_reference_to_diagnostic(tmp_path: Path) -> None:
    root = tmp_path
    formal_runner = root / "src/gas_calibrator/tools/run_v1_5_formal_co2_open_flow_queue.py"
    diagnostic = root / "src/gas_calibrator/tools/run_v1_5_open_flow_dynamic_pressure_diagnostic.py"
    for path in (formal_runner, diagnostic):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("", encoding="utf-8")
    formal_runner.write_text(
        "from gas_calibrator.tools.run_v1_5_open_flow_dynamic_pressure_diagnostic import main\n",
        encoding="utf-8",
    )

    issues = audit_v1_5_isolated_reference_integrity(root)

    assert [
        issue.to_json()
        for issue in issues
        if issue.severity == "blocker"
        and issue.isolated_path == "src/gas_calibrator/tools/run_v1_5_open_flow_dynamic_pressure_diagnostic.py"
    ] != []


def test_isolation_reference_audit_reviews_validation_reference_to_legacy_v1(tmp_path: Path) -> None:
    root = tmp_path
    legacy = root / "src/gas_calibrator/tools/run_v1_corrected_autodelivery.py"
    audit = root / "src/gas_calibrator/validation/v1_ratio_poly_algorithm_audit.py"
    for path in (legacy, audit):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("", encoding="utf-8")
    audit.write_text(
        "LEGACY_SOURCE = 'gas_calibrator.tools.run_v1_corrected_autodelivery'\n",
        encoding="utf-8",
    )

    issues = audit_v1_5_isolated_reference_integrity(root)
    legacy_issues = [
        issue for issue in issues if issue.isolated_path == "src/gas_calibrator/tools/run_v1_corrected_autodelivery.py"
    ]

    assert legacy_issues
    assert {issue.severity for issue in legacy_issues} == {"review"}
    assert {issue.reference_category for issue in legacy_issues} == {"validation_or_audit_support"}


def test_isolation_reference_audit_has_no_repository_blockers() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    issues = audit_v1_5_isolated_reference_integrity(repo_root)

    assert [issue.to_json() for issue in issues if issue.severity == "blocker"] == []


def test_active_surface_policy_blocks_diagnostic_as_canonical_path(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    diagnostic = tmp_path / "src/gas_calibrator/tools/run_v1_5_open_flow_dynamic_pressure_diagnostic.py"
    diagnostic.parent.mkdir(parents=True, exist_ok=True)
    diagnostic.write_text("", encoding="utf-8")
    monkeypatch.setattr(
        inventory_validation,
        "CANONICAL_FORMAL_PATH",
        (
            {
                "stage": "bad_diagnostic_as_formal",
                "entrypoint": "src/gas_calibrator/tools/run_v1_5_open_flow_dynamic_pressure_diagnostic.py",
                "category": "formal_runner",
                "status": "bad",
                "physical_meaning": "bad",
                "safety_boundary": "bad",
            },
        ),
    )

    issues = inventory_validation.validate_v1_5_active_surface_policy(tmp_path)
    blocker_rules = {issue.rule for issue in issues if issue.severity == "blocker"}

    assert "canonical_category_mismatch" in blocker_rules
    assert "canonical_entrypoint_blocked_category" in blocker_rules


def test_canonical_formal_path_entries_exist_in_repository() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    entries = discover_v1_5_entrypoints(repo_root)
    discovered_paths = {entry.path for entry in entries}

    missing = [
        item["entrypoint"]
        for item in CANONICAL_FORMAL_PATH
        if item["entrypoint"] not in discovered_paths
    ]

    assert missing == []


def test_canonical_formal_path_flags_writes_and_route_runners() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    by_path = {entry.path: entry for entry in discover_v1_5_entrypoints(repo_root)}

    co2_runner = by_path["src/gas_calibrator/tools/run_v1_5_formal_co2_open_flow_queue.py"]
    h2o_runner = by_path["src/gas_calibrator/tools/run_v1_5_formal_h2o_open_flow_queue.py"]
    write_tool = by_path["src/gas_calibrator/tools/run_v1_5_co2_senco13_controlled_write.py"]
    full_flow = by_path["src/gas_calibrator/tools/run_v1_5_full_calibration_chain.py"]

    assert co2_runner.category == "formal_runner"
    assert co2_runner.controls_routes is True
    assert h2o_runner.category == "formal_runner"
    assert h2o_runner.controls_routes is True
    co2_worker = by_path["src/gas_calibrator/tools/run_v1_5_formal_open_flow_sampling.py"]
    h2o_worker = by_path["src/gas_calibrator/tools/run_v1_5_formal_h2o_open_flow_sampling.py"]
    assert co2_worker.category == "formal_sampling_worker"
    assert co2_worker.formal_status == "canonical_queue_worker"
    assert h2o_worker.category == "formal_sampling_worker"
    assert h2o_worker.formal_status == "canonical_queue_worker"
    assert write_tool.category == "controlled_write"
    assert write_tool.writes_coefficients is True
    assert full_flow.category == "full_flow_orchestration"
    assert full_flow.opens_com_ports is False


def test_guardrailed_entrypoints_collect_diagnostics_writes_and_queue_workers(tmp_path: Path) -> None:
    root = tmp_path
    paths = [
        "src/gas_calibrator/tools/run_v1_5_formal_co2_open_flow_queue.py",
        "src/gas_calibrator/tools/run_v1_5_formal_open_flow_sampling.py",
        "src/gas_calibrator/tools/run_v1_5_open_flow_dynamic_pressure_diagnostic.py",
        "src/gas_calibrator/tools/run_v1_5_co2_senco13_controlled_write.py",
        "src/gas_calibrator/tools/archive_v1_5_current_stage.py",
    ]
    for item in paths:
        path = root / item
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("", encoding="utf-8")

    rows = guardrailed_entrypoint_rows(discover_v1_5_entrypoints(root))
    by_path = {row["path"]: row for row in rows}

    assert "src/gas_calibrator/tools/run_v1_5_formal_co2_open_flow_queue.py" not in by_path
    assert (
        by_path["src/gas_calibrator/tools/run_v1_5_formal_open_flow_sampling.py"]["guardrail"]
        == "use_via_canonical_queue_only"
    )
    assert by_path["src/gas_calibrator/tools/run_v1_5_open_flow_dynamic_pressure_diagnostic.py"]["guardrail"] == "diagnostic_not_acceptance"
    assert by_path["src/gas_calibrator/tools/run_v1_5_co2_senco13_controlled_write.py"]["guardrail"] == "authorized_write_only"
    assert by_path["src/gas_calibrator/tools/archive_v1_5_current_stage.py"]["guardrail"] == "archive_housekeeping_only"


def test_active_surface_policy_does_not_review_canonical_queue_workers(tmp_path: Path) -> None:
    root = tmp_path
    paths = [
        "src/gas_calibrator/tools/run_v1_5_formal_open_flow_sampling.py",
        "src/gas_calibrator/tools/run_v1_5_formal_h2o_open_flow_sampling.py",
    ]
    for item in paths:
        path = root / item
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("", encoding="utf-8")

    issues = validate_v1_5_active_surface_policy(root, entries=discover_v1_5_entrypoints(root))

    reviewed_paths = {issue.path for issue in issues if issue.rule == "noncanonical_formal_runner"}
    assert reviewed_paths.isdisjoint(paths)


def test_guardrailed_entrypoints_collect_pressure_runner_and_legacy_v1(tmp_path: Path) -> None:
    root = tmp_path
    paths = [
        "src/gas_calibrator/tools/validate_pressure_only.py",
        "src/gas_calibrator/tools/run_v1_corrected_autodelivery.py",
    ]
    for item in paths:
        path = root / item
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("", encoding="utf-8")

    rows = guardrailed_entrypoint_rows(discover_v1_5_entrypoints(root))
    by_path = {row["path"]: row for row in rows}

    assert by_path["src/gas_calibrator/tools/validate_pressure_only.py"]["guardrail"] == "pressure_no_write_only"
    assert by_path["src/gas_calibrator/tools/run_v1_corrected_autodelivery.py"]["guardrail"] == "legacy_v1_reference_only"


def test_final_structure_doc_records_canonical_entrypoint_boundaries() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    doc = repo_root / "docs/v1_5_flow_contract/V1_5_FINAL_STRUCTURE_AND_FLOW.md"
    text = doc.read_text(encoding="utf-8")

    assert "run_v1_5_formal_initialization_runner.py" in text
    assert "run_v1_5_formal_co2_open_flow_queue.py" in text
    assert "run_v1_5_formal_h2o_open_flow_queue.py" in text
    assert "COM 端口只作为 transport" in text
    assert "CO2 zero gas" in text
    assert "H2O dry-gas" in text
    assert "A=-ln(R/R0(T))/(P_kPa/100)" in text
    assert "formal_run_status" in text
    assert "0620 成熟" in text
    assert "configs/default_config.json" in text
    assert "根目录 `D:\\gas_calibrator` 冻结为污染区" in text
    assert "final_acceptance_status" in text
    assert "export_v1_5_mature_route_contract.py" in text
    assert "export_v1_5_historical_replay_contract.py" in text
    assert "export_v1_5_historical_replay_evidence.py" in text
    assert "export_v1_5_historical_replay_missing_point_audit.py" in text
    assert "export_v1_5_historical_replay_qc_gap_audit.py" in text
    assert "export_v1_5_algorithm_formal_point_plan_guard.py" in text
    assert "export_v1_5_algorithm_formal_runlist_preview.py" in text
    assert "export_v1_5_algorithm_runlist_readiness.py" in text
    assert "export_v1_5_algorithm_runner_integration_dry_run.py" in text
    assert "export_v1_5_algorithm_profile_runner_dry_run.py" in text
    assert "legacy CO2 45 点" in text
    assert "legacy H2O 13 湿点" in text
