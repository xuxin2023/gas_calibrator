from __future__ import annotations

from copy import deepcopy
import json
from pathlib import Path

import pytest

from gas_calibrator.v2.core.offline_artifacts import build_suite_case_metadata
from gas_calibrator.v2.domain.services.gas_analyzer_bench_readiness import (
    analyze_gas_analyzer_bench_readiness,
    build_gas_analyzer_bench_readiness_acceptance,
)
from gas_calibrator.v2.sim.gas_analyzer_bench_readiness import (
    build_gas_analyzer_bench_readiness_offline_report,
    generate_gas_analyzer_bench_readiness_fixture,
    load_gas_analyzer_bench_readiness_contract,
)
from gas_calibrator.v2.sim.scenarios.suites import get_simulation_suite
from gas_calibrator.v2.ui_v2.i18n import display_suite_failure_type


def _clean_inputs() -> tuple[
    dict[str, object],
    dict[str, object],
    list[dict[str, object]],
    list[dict[str, object]],
]:
    contract = load_gas_analyzer_bench_readiness_contract()
    protocol, assets, budgets = generate_gas_analyzer_bench_readiness_fixture(contract)
    return contract, protocol, assets, budgets


def _evaluate(
    contract: dict[str, object],
    protocol: dict[str, object],
    assets: list[dict[str, object]],
    budgets: list[dict[str, object]],
) -> tuple[dict[str, object], dict[str, object]]:
    readiness = analyze_gas_analyzer_bench_readiness(
        protocol,
        assets,
        budgets,
        contract=contract,
    )
    acceptance = build_gas_analyzer_bench_readiness_acceptance(
        readiness,
        contract=contract,
    )
    return readiness, acceptance


def test_clean_bench_protocol_is_design_ready_but_not_execution_authorized() -> None:
    contract, protocol, assets, budgets = _clean_inputs()
    readiness, acceptance = _evaluate(contract, protocol, assets, budgets)

    assert readiness["status"] == "ok"
    assert readiness["traceability"]["asset_count"] == 8
    assert readiness["traceability"]["roles_complete"] is True
    assert acceptance["all_readiness_gates_passed"] is True
    assert acceptance["protocol_design_ready"] is True
    assert acceptance["ready_for_real_execution"] is False
    assert acceptance["execution_authorization_status"] == "not_requested"
    assert acceptance["real_acceptance_status"] == "blocked"
    assert acceptance["device_io_status"] == "not_attempted"
    assert acceptance["write_status"] == "not_attempted"
    assert acceptance["failed_gate_names"] == []
    for channel in readiness["channels"]:
        assert channel["environment_order"]["complete"] is True
        assert channel["environment_order"]["randomization_valid"] is True
        assert channel["environment_order"]["reproducible_from_seed"] is True
        assert channel["planned_measurement_row_count"] == 1296
        assert channel["sampling_axes_valid"] is True
        assert channel["uncertainty_budget"]["within_limit"] is True


def test_co2_zero_and_h2o_dry_routes_remain_physically_distinct() -> None:
    contract, protocol, assets, budgets = _clean_inputs()
    readiness, acceptance = _evaluate(contract, protocol, assets, budgets)
    channels = {channel["gas"]: channel for channel in readiness["channels"]}

    assert channels["co2"]["anchor_role"] == "co2_zero_gas"
    assert channels["co2"]["anchor_target"] == 0.0
    assert channels["co2"]["route_mode"] == "co2_zero_span_dry_path"
    assert channels["h2o"]["anchor_role"] == "h2o_dry_gas"
    assert channels["h2o"]["anchor_target"] == 0.2
    assert channels["h2o"]["route_mode"] == "h2o_dry_wet_conditioned_path"
    assert acceptance["all_readiness_gates_passed"] is True

    protocol["gas_plans"][1]["anchor_role"] = "co2_zero_gas"
    protocol["gas_plans"][1]["route_mode"] = "co2_zero_span_dry_path"
    _readiness, rejected = _evaluate(contract, protocol, assets, budgets)
    assert "h2o:GA01:anchor_role_and_target" in rejected["failed_gate_names"]
    assert "h2o:GA01:route_semantics" in rejected["failed_gate_names"]


def test_missing_duplicate_or_expired_traceability_assets_are_rejected() -> None:
    contract, protocol, assets, budgets = _clean_inputs()
    assets.pop(0)
    assets.append(deepcopy(assets[0]))
    assets[1]["due_date"] = "2026-07-24"
    assets[1]["asset_id"] = assets[0]["asset_id"]
    readiness, acceptance = _evaluate(contract, protocol, assets, budgets)

    assert readiness["traceability"]["missing_roles"] == ["co2_zero_gas_standard"]
    assert readiness["traceability"]["duplicate_roles"] == ["co2_span_gas_standard"]
    assert readiness["traceability"]["invalid_certificate_row_count"] == 1
    assert "global:all:traceability_roles_complete" in acceptance["failed_gate_names"]
    assert (
        "global:all:traceability_asset_identity_unique"
        in acceptance["failed_gate_names"]
    )
    assert (
        "global:all:traceability_certificates_valid" in acceptance["failed_gate_names"]
    )


def test_traceability_fields_and_uncertainties_are_hard_gates() -> None:
    contract, protocol, assets, budgets = _clean_inputs()
    assets[0]["certificate_id"] = ""
    assets[1]["standard_uncertainty"] = float("nan")
    readiness, acceptance = _evaluate(contract, protocol, assets, budgets)

    assert "certificate_id" in readiness["traceability"]["missing_fields"]
    assert readiness["traceability"]["invalid_uncertainty_row_count"] == 1
    assert "global:all:traceability_fields_complete" in acceptance["failed_gate_names"]
    assert (
        "global:all:traceability_uncertainties_valid" in acceptance["failed_gate_names"]
    )


def test_environment_order_must_be_complete_randomized_and_reproducible() -> None:
    contract, protocol, assets, budgets = _clean_inputs()
    co2_plan = protocol["gas_plans"][0]
    co2_plan["environment_cell_order"].pop(0)
    co2_plan["environment_cell_order"].append(
        deepcopy(co2_plan["environment_cell_order"][0])
    )
    co2_plan["randomization_seed"] = 1
    readiness, acceptance = _evaluate(contract, protocol, assets, budgets)
    co2 = next(channel for channel in readiness["channels"] if channel["gas"] == "co2")

    assert co2["environment_order"]["missing_cell_count"] == 1
    assert co2["environment_order"]["duplicate_cell_count"] == 1
    assert co2["environment_order"]["reproducible_from_seed"] is False
    assert (
        "co2:GA01:complete_rectangular_environment_order"
        in acceptance["failed_gate_names"]
    )
    assert (
        "co2:GA01:environment_order_randomized_and_reproducible"
        in acceptance["failed_gate_names"]
    )


def test_malformed_environment_cells_are_rejected_without_crashing() -> None:
    contract, protocol, assets, budgets = _clean_inputs()
    co2_plan = protocol["gas_plans"][0]
    co2_plan["environment_cell_order"][0]["temperature_c"] = None
    co2_plan["environment_cell_order"][1]["pressure_hpa"] = "not-a-number"
    readiness, acceptance = _evaluate(contract, protocol, assets, budgets)
    co2 = next(channel for channel in readiness["channels"] if channel["gas"] == "co2")

    assert co2["environment_order"]["unexpected_cell_count"] == 2
    assert co2["environment_order"]["complete"] is False
    assert (
        "co2:GA01:complete_rectangular_environment_order"
        in acceptance["failed_gate_names"]
    )


def test_dut_identity_and_physical_bench_controls_are_required() -> None:
    contract, protocol, assets, budgets = _clean_inputs()
    protocol["analyzer_model"] = ""
    protocol["bench_controls"]["pressure_decay_hpa_per_min"] = 0.5
    protocol["bench_controls"]["h2o_wetted_path_material"] = "PVC"
    protocol["bench_controls"]["h2o_wetted_path_temperature_controlled"] = False
    protocol["bench_controls"]["shared_timebase"] = False
    protocol["bench_controls"]["safe_exhaust"] = False
    readiness, acceptance = _evaluate(contract, protocol, assets, budgets)

    assert readiness["missing_dut_identity_fields"] == ["analyzer_model"]
    assert readiness["bench_controls"]["valid"] is False
    assert "global:all:dut_identity_complete" in acceptance["failed_gate_names"]
    assert "global:all:bench_physical_controls_ready" in acceptance["failed_gate_names"]


def test_sampling_axes_and_planned_count_cannot_be_weakened() -> None:
    contract, protocol, assets, budgets = _clean_inputs()
    protocol["gas_plans"][0]["target_levels"].pop()
    protocol["gas_plans"][0]["planned_measurement_row_count"] = 972
    _readiness, acceptance = _evaluate(contract, protocol, assets, budgets)

    assert "co2:GA01:sampling_axes_match_contract" in acceptance["failed_gate_names"]
    assert "co2:GA01:planned_measurement_count" in acceptance["failed_gate_names"]


def test_h2o_recovery_and_channel_stability_cannot_be_relaxed() -> None:
    contract, protocol, assets, budgets = _clean_inputs()
    protocol["gas_plans"][0]["conditioning"]["max_slope_span_fraction_per_min"] = 0.001
    protocol["gas_plans"][1]["conditioning"]["wet_to_dry_recovery_fraction"] = 0.02
    protocol["gas_plans"][1]["conditioning"]["recovery_hold_s"] = 30.0
    readiness, acceptance = _evaluate(contract, protocol, assets, budgets)
    by_gas = {channel["gas"]: channel for channel in readiness["channels"]}

    assert by_gas["co2"]["conditioning"]["base_valid"] is False
    assert by_gas["h2o"]["conditioning"]["recovery_valid"] is False
    assert "co2:GA01:conditioning_criteria" in acceptance["failed_gate_names"]
    assert "h2o:GA01:conditioning_criteria" in acceptance["failed_gate_names"]


def test_uncertainty_budget_requires_all_components_and_margin() -> None:
    contract, protocol, assets, budgets = _clean_inputs()
    budgets[0]["components"].pop()
    budgets[1]["components"][0]["standard_uncertainty_span_fraction"] = 0.01
    readiness, acceptance = _evaluate(contract, protocol, assets, budgets)
    by_gas = {channel["gas"]: channel for channel in readiness["channels"]}

    assert by_gas["co2"]["uncertainty_budget"]["components_complete"] is False
    assert by_gas["h2o"]["uncertainty_budget"]["within_limit"] is False
    assert "co2:GA01:uncertainty_components_complete" in acceptance["failed_gate_names"]
    assert (
        "h2o:GA01:expanded_uncertainty_within_limit" in acceptance["failed_gate_names"]
    )


def test_any_io_write_or_promotion_intent_is_rejected() -> None:
    contract, protocol, assets, budgets = _clean_inputs()
    protocol["device_io_requested"] = True
    protocol["automatic_execution_requested"] = True
    protocol["coefficient_fit_requested"] = True
    protocol["coefficient_write_requested"] = True
    protocol["database_write_requested"] = True
    protocol["real_primary_latest_refresh_requested"] = True
    _readiness, acceptance = _evaluate(contract, protocol, assets, budgets)

    assert "global:all:offline_no_io_no_write_scope" in acceptance["failed_gate_names"]
    assert acceptance["ready_for_real_execution"] is False
    assert acceptance["execution_authorization_status"] == "not_requested"


def test_stage_or_evidence_governance_gaps_are_rejected() -> None:
    contract, protocol, assets, budgets = _clean_inputs()
    protocol["stages"].pop(2)
    protocol["evidence_plan"]["raw_data_immutable"] = False
    protocol["evidence_plan"]["required_artifact_roles"].remove("formal_analysis")
    protocol["evidence_plan"]["formal_analysis_state"] = "ready"
    _readiness, acceptance = _evaluate(contract, protocol, assets, budgets)

    assert "global:all:protocol_stage_order_complete" in acceptance["failed_gate_names"]
    assert "global:all:evidence_roles_complete" in acceptance["failed_gate_names"]
    assert (
        "global:all:evidence_immutability_and_lineage"
        in acceptance["failed_gate_names"]
    )
    assert (
        "global:all:formal_analysis_remains_blocked" in acceptance["failed_gate_names"]
    )


def test_contract_rejects_unsafe_boundaries_or_scope(tmp_path: Path) -> None:
    contract = load_gas_analyzer_bench_readiness_contract()
    unsafe = tmp_path / "unsafe_ga_d4_contract.json"
    contract["evidence_boundary"]["device_io_allowed"] = True
    unsafe.write_text(json.dumps(contract), encoding="utf-8")
    with pytest.raises(ValueError, match="device_io_allowed=false"):
        load_gas_analyzer_bench_readiness_contract(unsafe)

    contract = load_gas_analyzer_bench_readiness_contract()
    contract["interpretation"]["protocol_readiness_is_execution_authorization"] = True
    unsafe.write_text(json.dumps(contract), encoding="utf-8")
    with pytest.raises(
        ValueError,
        match="protocol_readiness_is_execution_authorization=false",
    ):
        load_gas_analyzer_bench_readiness_contract(unsafe)

    contract = load_gas_analyzer_bench_readiness_contract()
    contract["environment_grid"]["temperatures_c"] = [25.0]
    unsafe.write_text(json.dumps(contract), encoding="utf-8")
    with pytest.raises(ValueError, match="27-cell environment grid"):
        load_gas_analyzer_bench_readiness_contract(unsafe)

    contract = load_gas_analyzer_bench_readiness_contract()
    contract["gas_protocols"]["co2"]["target_levels"].pop()
    unsafe.write_text(json.dumps(contract), encoding="utf-8")
    with pytest.raises(ValueError, match="target levels must retain GA-D3"):
        load_gas_analyzer_bench_readiness_contract(unsafe)

    contract = load_gas_analyzer_bench_readiness_contract()
    contract["required_uncertainty_components"]["h2o"].remove("sorption_and_memory")
    unsafe.write_text(json.dumps(contract), encoding="utf-8")
    with pytest.raises(ValueError, match="retain all uncertainty components"):
        load_gas_analyzer_bench_readiness_contract(unsafe)

    contract = load_gas_analyzer_bench_readiness_contract()
    contract["required_artifact_roles"].remove("formal_analysis")
    unsafe.write_text(json.dumps(contract), encoding="utf-8")
    with pytest.raises(ValueError, match="retain all evidence artifact roles"):
        load_gas_analyzer_bench_readiness_contract(unsafe)

    contract = load_gas_analyzer_bench_readiness_contract()
    contract["uncertainty_coverage_factor"] = 1.0
    unsafe.write_text(json.dumps(contract), encoding="utf-8")
    with pytest.raises(ValueError, match="must remain k=2"):
        load_gas_analyzer_bench_readiness_contract(unsafe)


def test_protocol_schema_version_is_a_readiness_gate() -> None:
    contract, protocol, assets, budgets = _clean_inputs()
    protocol["schema_version"] = "gas_analyzer_bench_protocol_plan_v0"
    readiness, acceptance = _evaluate(contract, protocol, assets, budgets)

    assert readiness["protocol_schema_version"] == "gas_analyzer_bench_protocol_plan_v0"
    assert "global:all:protocol_schema_version" in acceptance["failed_gate_names"]


def test_offline_report_preserves_no_execution_boundary(tmp_path: Path) -> None:
    result = build_gas_analyzer_bench_readiness_offline_report(
        report_root=tmp_path,
        run_name="ga_d4_contract",
    )
    report = json.loads(Path(result["report_json"]).read_text(encoding="utf-8"))
    inputs = json.loads(Path(result["execution_summary"]).read_text(encoding="utf-8"))

    assert result["status"] == "MATCH"
    assert inputs["artifact_role"] == "execution_summary"
    assert report["artifact_role"] == "diagnostic_analysis"
    assert report["evidence_source"] == "simulated"
    assert report["not_real_acceptance_evidence"] is True
    assert report["promotion_state"] == "blocked"
    assert report["protocol_design_ready"] is True
    assert report["ready_for_real_execution"] is False
    assert report["execution_authorization_status"] == "not_requested"
    assert report["real_acceptance_status"] == "blocked"
    assert report["device_io_status"] == "not_attempted"
    assert report["coefficient_writeback_status"] == "not_attempted"
    assert report["database_write_status"] == "not_attempted"
    assert report["real_primary_latest_refresh_status"] == "not_attempted"
    assert report["ec_flux_status"] == "not_in_scope"
    markdown = Path(result["report_markdown"]).read_text(encoding="utf-8")
    assert "不启动真实台架、不连接 COM、不产生执行许可" in markdown
    assert "CO2 零气与 H2O 干气点保持独立" in markdown


def test_regression_and_nightly_include_ga_d4_and_metadata_is_chinese() -> None:
    for suite_name in ("regression", "nightly"):
        matching = [
            case.name
            for case in get_simulation_suite(suite_name).cases
            if case.kind == "ga_bench_readiness"
        ]
        assert matching == ["gas_analyzer_bench_readiness_contract"]
    assert not any(
        case.kind == "ga_bench_readiness"
        for case in get_simulation_suite("smoke").cases
    )
    metadata = build_suite_case_metadata(
        {
            "name": "gas_analyzer_bench_readiness_contract",
            "kind": "ga_bench_readiness",
            "status": "MATCH",
            "ok": True,
            "artifact_dir": "",
            "details": {},
        },
        suite_name="regression",
    )
    assert metadata["evidence_source"] == "simulated"
    assert metadata["failure_type"] == "gas_analyzer_bench_readiness"
    assert (
        display_suite_failure_type(metadata["failure_type"], locale="zh_CN")
        == "气体分析仪台架协议准备度"
    )
