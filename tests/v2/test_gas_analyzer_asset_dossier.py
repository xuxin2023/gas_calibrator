from __future__ import annotations

from copy import deepcopy
import json
from pathlib import Path

import pytest

from gas_calibrator.validation.metrology.gas_analyzer_asset_dossier import (
    analyze_gas_analyzer_asset_dossier,
    build_gas_analyzer_asset_dossier_acceptance,
)
from gas_calibrator.v2.core.offline_artifacts import build_suite_case_metadata
from gas_calibrator.v2.sim.gas_analyzer_asset_dossier import (
    build_gas_analyzer_asset_dossier_offline_report,
    generate_complete_asset_dossier_fixture,
    load_ga_d5_observed_gap_fixture,
    load_gas_analyzer_asset_dossier_contract,
)
from gas_calibrator.v2.sim.scenarios.suites import get_simulation_suite
from gas_calibrator.v2.ui_v2.i18n import display_suite_failure_type


def _observed_inputs() -> tuple[dict[str, object], dict[str, object]]:
    contract = load_gas_analyzer_asset_dossier_contract()
    snapshot = load_ga_d5_observed_gap_fixture(contract=contract)
    return contract, snapshot


def _evaluate(
    contract: dict[str, object],
    snapshot: dict[str, object],
) -> tuple[dict[str, object], dict[str, object]]:
    readiness = analyze_gas_analyzer_asset_dossier(snapshot, contract=contract)
    acceptance = build_gas_analyzer_asset_dossier_acceptance(
        readiness,
        contract=contract,
    )
    return readiness, acceptance


def test_observed_0620_0621_baseline_is_consistent_but_expected_blocked() -> None:
    assert (
        analyze_gas_analyzer_asset_dossier.__module__
        == "gas_calibrator.validation.metrology.gas_analyzer_asset_dossier"
    )
    old_owner = (
        Path(__file__).resolve().parents[2]
        / "src/gas_calibrator/v2/domain/services/gas_analyzer_asset_dossier.py"
    )
    assert not old_owner.exists()

    contract, snapshot = _observed_inputs()
    readiness, acceptance = _evaluate(contract, snapshot)

    assert readiness["status"] == "ok"
    assert readiness["observed_state_matches_locked_baseline"] is True
    assert readiness["recovered_co2_values_match_locked_baseline"] is True
    assert acceptance["historical_baseline_consistent"] is True
    assert acceptance["asset_documentary_ready"] is False
    assert acceptance["current_prerequisites_ready"] is False
    assert acceptance["expected_gaps_observed"] is True
    assert acceptance["ready_for_real_execution"] is False
    assert acceptance["real_acceptance_status"] == "blocked"
    assert acceptance["device_io_status"] == "not_attempted"
    assert acceptance["missing_expected_blockers"] == []


def test_observed_facts_preserve_actual_0620_0621_distinctions() -> None:
    contract, snapshot = _observed_inputs()
    readiness, _acceptance = _evaluate(contract, snapshot)
    facts = readiness["historical_facts"]

    assert facts["co2_0620"] == {
        "expected_point_count": 45,
        "accepted_point_count": 45,
        "warning_point_count": 2,
        "missing_or_reject_count": 0,
    }
    assert facts["co2_0621"]["completed_entry_points"] == [
        {"temperature_c": 40.0, "target_ppm": 0.0},
        {"temperature_c": 40.0, "target_ppm": 400.0},
    ]
    assert facts["co2_0621"]["incomplete_zero_attempt_count"] == 3
    assert facts["h2o_0620"] == {
        "historical_device_count": 6,
        "historical_blocked_device_count": 3,
    }
    assert facts["recovered_co2"]["formal_fit_matched_rows"] == 228
    assert facts["recovered_co2"]["certificate_documents_linked"] == 0


def test_recovered_values_do_not_masquerade_as_certificate_documents() -> None:
    contract, snapshot = _observed_inputs()
    readiness, acceptance = _evaluate(contract, snapshot)
    dossier = readiness["asset_dossier"]

    assert dossier["role_counts"]["co2_standard_gas_series"] == 10
    assert (
        "certificate_document_sha256"
        in dossier["missing_common_fields_by_role"]["co2_standard_gas_series"]
    )
    assert (
        "cylinder_serial_number"
        in dossier["missing_gas_fields_by_role"]["co2_standard_gas_series"]
    )
    assert (
        "co2_standard_gas_series:certificate_documentary_fields_incomplete"
        in acceptance["blocking_reasons"]
    )
    assert (
        "co2_standard_gas_series:gas_identity_fields_incomplete"
        in acceptance["blocking_reasons"]
    )


def test_complete_reference_dossier_can_pass_documentary_gates_only() -> None:
    contract, observed = _observed_inputs()
    complete = generate_complete_asset_dossier_fixture(observed)
    readiness, acceptance = _evaluate(contract, complete)

    assert readiness["asset_dossier"]["asset_count"] == 16
    assert readiness["asset_dossier"]["roles_complete"] is True
    assert acceptance["historical_baseline_consistent"] is True
    assert acceptance["asset_documentary_ready"] is True
    assert acceptance["current_prerequisites_ready"] is False
    assert acceptance["ready_for_real_execution"] is False
    assert acceptance["execution_authorization_status"] == "not_requested"


def test_missing_role_and_wrong_cardinality_are_rejected() -> None:
    contract, observed = _observed_inputs()
    complete = generate_complete_asset_dossier_fixture(observed)
    complete["asset_records"] = [
        item
        for item in complete["asset_records"]
        if item["asset_role"] != "flow_reference"
    ]
    complete["asset_records"].append(deepcopy(complete["asset_records"][0]))
    readiness, acceptance = _evaluate(contract, complete)

    assert "flow_reference" in readiness["asset_dossier"]["missing_roles"]
    assert "co2_zero_gas" in readiness["asset_dossier"]["invalid_cardinality_roles"]
    assert acceptance["asset_documentary_ready"] is False


def test_certificate_dates_uncertainty_and_scope_are_hard_gates() -> None:
    contract, observed = _observed_inputs()
    complete = generate_complete_asset_dossier_fixture(observed)
    row = complete["asset_records"][0]
    row["certificate_valid_until"] = "2026-07-24"
    row["expanded_uncertainty"] = 0.1
    row["coverage_factor"] = 2.0
    row["plan_coverage_complete"] = False
    readiness, acceptance = _evaluate(contract, complete)

    asset_id = row["asset_id"]
    dossier = readiness["asset_dossier"]
    assert asset_id in dossier["invalid_lifecycle_asset_ids"]
    assert asset_id in dossier["invalid_uncertainty_asset_ids"]
    assert asset_id in dossier["invalid_scope_asset_ids"]
    assert acceptance["asset_documentary_ready"] is False


def test_shared_physical_asset_requires_explicit_covariance() -> None:
    contract, observed = _observed_inputs()
    complete = generate_complete_asset_dossier_fixture(observed)
    complete["asset_records"][11]["serial_number"] = complete["asset_records"][12][
        "serial_number"
    ]
    readiness, acceptance = _evaluate(contract, complete)

    assert readiness["asset_dossier"]["covariance_treatment_valid"] is False
    assert acceptance["asset_documentary_ready"] is False

    for index in (11, 12):
        complete["asset_records"][index]["correlation_group_id"] = "REF-GROUP-1"
        complete["asset_records"][index]["covariance_treatment"] = "covariance_included"
    readiness, acceptance = _evaluate(contract, complete)
    assert readiness["asset_dossier"]["covariance_treatment_valid"] is True
    assert acceptance["asset_documentary_ready"] is True


def test_any_execution_or_write_intent_breaks_historical_baseline() -> None:
    contract, snapshot = _observed_inputs()
    snapshot["execution_boundary"]["device_io_requested"] = True
    _readiness, acceptance = _evaluate(contract, snapshot)

    failed = [
        gate["name"] for gate in acceptance["baseline_gates"] if not gate["passed"]
    ]
    assert failed == ["offline_no_io_no_write_scope"]
    assert acceptance["historical_baseline_consistent"] is False
    assert acceptance["ready_for_real_execution"] is False


def test_contract_rejects_scope_method_or_baseline_weakening(tmp_path: Path) -> None:
    contract = load_gas_analyzer_asset_dossier_contract()
    path = tmp_path / "unsafe_ga_d5_contract.json"

    contract["evidence_boundary"]["device_io_allowed"] = True
    path.write_text(json.dumps(contract), encoding="utf-8")
    with pytest.raises(ValueError, match="device_io_allowed=False"):
        load_gas_analyzer_asset_dossier_contract(path)

    contract = load_gas_analyzer_asset_dossier_contract()
    contract["method_contract"]["pressure_first_senco9_required"] = False
    path.write_text(json.dumps(contract), encoding="utf-8")
    with pytest.raises(ValueError, match="mature 0613/0620/0621"):
        load_gas_analyzer_asset_dossier_contract(path)

    contract = load_gas_analyzer_asset_dossier_contract()
    contract["observed_baseline"]["co2_0620_accepted_point_count"] = 44
    path.write_text(json.dumps(contract), encoding="utf-8")
    with pytest.raises(ValueError, match="co2_0620_accepted_point_count"):
        load_gas_analyzer_asset_dossier_contract(path)

    contract = load_gas_analyzer_asset_dossier_contract()
    contract["required_common_fields"].remove("certificate_document_sha256")
    path.write_text(json.dumps(contract), encoding="utf-8")
    with pytest.raises(ValueError, match="certificate documentary fields"):
        load_gas_analyzer_asset_dossier_contract(path)


def test_fixture_rejects_source_hash_or_observed_fact_drift(tmp_path: Path) -> None:
    contract, snapshot = _observed_inputs()
    path = tmp_path / "drifted_ga_d5_fixture.json"
    snapshot["source_artifacts"][0]["sha256"] = "0" * 64
    path.write_text(json.dumps(snapshot), encoding="utf-8")
    with pytest.raises(ValueError, match="source hashes drifted"):
        load_ga_d5_observed_gap_fixture(path, contract=contract)

    contract, snapshot = _observed_inputs()
    snapshot["observed_state"]["co2_0621_incomplete_zero_attempt_count"] = 2
    path.write_text(json.dumps(snapshot), encoding="utf-8")
    with pytest.raises(ValueError, match="observed baseline drifted"):
        load_ga_d5_observed_gap_fixture(path, contract=contract)


def test_offline_report_is_expected_gaps_and_preserves_boundary(
    tmp_path: Path,
) -> None:
    result = build_gas_analyzer_asset_dossier_offline_report(
        report_root=tmp_path,
    )
    report = json.loads(Path(result["report_json"]).read_text(encoding="utf-8"))
    inputs = json.loads(Path(result["execution_summary"]).read_text(encoding="utf-8"))

    assert result["status"] == "EXPECTED_GAPS"
    assert inputs["artifact_role"] == "execution_summary"
    assert report["artifact_role"] == "diagnostic_analysis"
    assert report["evidence_source"] == "replay"
    assert report["not_real_acceptance_evidence"] is True
    assert report["promotion_state"] == "blocked"
    assert report["historical_baseline_consistent"] is True
    assert report["asset_documentary_ready"] is False
    assert report["current_prerequisites_ready"] is False
    assert report["ready_for_real_execution"] is False
    assert report["device_io_status"] == "not_attempted"
    assert report["coefficient_writeback_status"] == "not_attempted"
    assert report["database_write_status"] == "not_attempted"
    assert report["real_primary_latest_refresh_status"] == "not_attempted"
    markdown = Path(result["report_markdown"]).read_text(encoding="utf-8")
    assert "历史测量存在不等于计量证书资料完整" in markdown
    assert "CO2 零气与 H2O 干气/露点参考保持独立" in markdown


def test_regression_and_nightly_include_ga_d5_with_chinese_metadata() -> None:
    for suite_name in ("regression", "nightly"):
        matching = [
            case.name
            for case in get_simulation_suite(suite_name).cases
            if case.kind == "ga_asset_dossier"
        ]
        assert matching == ["ga_d5_0620_0621_asset_dossier_gaps"]
    assert not any(
        case.kind == "ga_asset_dossier" for case in get_simulation_suite("smoke").cases
    )
    metadata = build_suite_case_metadata(
        {
            "name": "ga_d5_0620_0621_asset_dossier_gaps",
            "kind": "ga_asset_dossier",
            "status": "EXPECTED_GAPS",
            "ok": True,
            "artifact_dir": "",
            "details": {},
        },
        suite_name="regression",
    )
    assert metadata["evidence_source"] == "replay"
    assert metadata["failure_type"] == "gas_analyzer_asset_dossier_readiness"
    assert (
        display_suite_failure_type(metadata["failure_type"], locale="zh_CN")
        == "气体分析仪计量资产资料包准备度"
    )
