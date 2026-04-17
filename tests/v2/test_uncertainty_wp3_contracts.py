from pathlib import Path
import json
import sys

from gas_calibrator.v2.adapters.results_gateway import ResultsGateway
from gas_calibrator.v2.adapters.uncertainty_gateway import UncertaintyGateway
from gas_calibrator.v2.core.uncertainty_repository import (
    DatabaseReadyUncertaintyRepositoryStub,
    FileBackedUncertaintyRepository,
)
from gas_calibrator.v2.scripts.build_offline_governance_artifacts import rebuild_run

SUPPORT_DIR = Path(__file__).resolve().parent
if str(SUPPORT_DIR) not in sys.path:
    sys.path.insert(0, str(SUPPORT_DIR))

from ui_v2_support import build_fake_facade


def _write_legacy_run(run_dir: Path) -> None:
    run_dir.mkdir(parents=True, exist_ok=True)
    for name in ("summary.json", "manifest.json", "results.json"):
        (run_dir / name).write_text(
            json.dumps({"run_id": run_dir.name, "stats": {"point_summaries": []}}, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )


def test_uncertainty_wp3_object_model_golden_cases_and_results_contract(tmp_path: Path) -> None:
    facade = build_fake_facade(tmp_path)
    run_dir = Path(facade.result_store.run_dir)
    rebuild_run(run_dir)

    repository = FileBackedUncertaintyRepository(run_dir)
    snapshot = repository.load_snapshot()
    payload = UncertaintyGateway(run_dir).read_payload()
    results_payload = ResultsGateway(
        run_dir,
        output_files_provider=facade.service.get_output_files,
    ).read_results_payload()

    budget_cases = list(payload["budget_case"].get("budget_case") or [])
    golden_cases = list(payload["uncertainty_golden_cases"].get("golden_cases") or [])
    input_rows = list(payload["uncertainty_input_set"].get("input_quantity_set") or [])
    coefficient_rows = list(payload["sensitivity_coefficient_set"].get("sensitivity_coefficients") or [])
    budget_levels = {str(item.get("budget_level") or "") for item in budget_cases}
    input_quantity_keys = {str(item.get("quantity_key") or "") for item in input_rows}
    fixture_paths = dict(payload["uncertainty_report_pack"].get("artifact_paths") or {})
    uncertainty_binding = dict(results_payload.get("uncertainty_binding") or {})
    result_budget_case = next(
        item for item in budget_cases if str(item.get("budget_level") or "") == "result"
    )
    required_case_fields = {
        "uncertainty_case_id",
        "scope_id",
        "decision_rule_id",
        "method_confirmation_protocol_id",
        "budget_level",
        "route_type",
        "measurand",
        "point_context",
        "input_quantity_set",
        "distribution_type",
        "sensitivity_coefficients",
        "repeatability_component",
        "reference_component",
        "fit_residual_component",
        "environmental_component",
        "pressure_handoff_component",
        "seal_ingress_risk_component",
        "coefficient_rounding_component",
        "writeback_verification_component",
        "combined_standard_uncertainty",
        "expected_combined_standard_uncertainty",
        "coverage_factor",
        "expanded_uncertainty",
        "expected_expanded_uncertainty",
        "golden_case_status",
        "report_rule",
        "evidence_source",
        "ready_for_readiness_mapping",
        "not_ready_for_formal_claim",
        "not_real_acceptance_evidence",
        "limitation_note",
        "non_claim_note",
        "reviewer_note",
    }

    assert payload["uncertainty_model"]["artifact_type"] == "uncertainty_model"
    assert payload["uncertainty_report_pack"]["artifact_type"] == "uncertainty_report_pack"
    assert payload["uncertainty_digest"]["artifact_type"] == "uncertainty_digest"
    assert payload["uncertainty_rollup"]["artifact_type"] == "uncertainty_rollup"
    assert len(budget_cases) >= 5
    assert len(golden_cases) >= 5
    assert len(input_rows) >= 20
    assert len(coefficient_rows) >= 20
    assert {"point", "route", "result"} <= budget_levels
    assert {
        "reference_setpoint",
        "dew_point_reference",
        "pressure_reference",
        "temperature_reference",
        "repeatability_sigma",
        "fit_residual",
        "rounding_resolution",
        "writeback_echo",
    } <= input_quantity_keys
    assert all(required_case_fields <= set(dict(item)) for item in budget_cases)
    assert all(dict(item).get("evidence_source") == "simulated" for item in budget_cases)
    assert all(bool(dict(item).get("ready_for_readiness_mapping")) for item in budget_cases)
    assert all(bool(dict(item).get("not_ready_for_formal_claim")) for item in budget_cases)
    assert all(bool(dict(item).get("not_real_acceptance_evidence")) for item in budget_cases)
    assert any(
        str(item.get("route_type") or "") == "gas" and str(item.get("measurand") or "") == "CO2"
        for item in budget_cases
    )
    assert any(
        str(item.get("route_type") or "") == "water" and str(item.get("measurand") or "") == "H2O"
        for item in budget_cases
    )
    assert any(
        str(item.get("route_type") or "") == "ambient" and str(item.get("measurand") or "") == "ambient_diagnostic"
        for item in budget_cases
    )
    assert any("writeback-rounding" in str(item.get("uncertainty_case_id") or "") for item in budget_cases)
    assert any("pressure-handoff-seal-ingress" in str(item.get("uncertainty_case_id") or "") for item in budget_cases)
    assert any("offline-result-rollup" in str(item.get("uncertainty_case_id") or "") for item in budget_cases)
    assert all(dict(item).get("traceability_summary") for item in golden_cases)
    assert all(dict(item).get("reviewer_only") is True for item in golden_cases)
    assert all(dict(item).get("readiness_mapping_only") is True for item in golden_cases)
    assert all(dict(item).get("non_claim") is True for item in golden_cases)
    assert all(str(dict(item).get("golden_case_status") or "") == "match" for item in golden_cases)
    assert all(dict(item).get("method_confirmation_protocol_id") for item in golden_cases)
    assert payload["uncertainty_report_pack"]["top_contributors"]
    assert payload["uncertainty_report_pack"]["data_completeness"]["placeholder_only"] is True
    assert payload["uncertainty_report_pack"]["budget_level_summary"]
    assert payload["uncertainty_report_pack"]["binding_summary"]
    assert payload["uncertainty_report_pack"]["calculation_chain_summary"]
    assert payload["uncertainty_report_pack"]["fixture_summary"]
    assert payload["uncertainty_report_pack"]["golden_case_summary"]
    assert payload["uncertainty_report_pack"]["uncertainty_case_id"] == result_budget_case["uncertainty_case_id"]
    assert payload["uncertainty_report_pack"]["method_confirmation_protocol_id"] == result_budget_case[
        "method_confirmation_protocol_id"
    ]
    assert payload["uncertainty_rollup"]["repository_mode"] == "file_artifact_first"
    assert payload["uncertainty_rollup"]["gateway_mode"] == "file_backed_default"
    assert payload["uncertainty_rollup"]["db_ready_stub"]["not_in_default_chain"] is True
    assert payload["uncertainty_rollup"]["primary_evidence_rewritten"] is False
    assert payload["uncertainty_rollup"]["not_real_acceptance_evidence"] is True
    assert payload["uncertainty_rollup"]["not_ready_for_formal_claim"] is True
    assert payload["uncertainty_rollup"]["report_pack_available"] is True
    assert payload["uncertainty_rollup"]["report_pack_available_on_disk"] is True
    assert payload["uncertainty_digest"]["uncertainty_case_id"] == result_budget_case["uncertainty_case_id"]
    assert payload["uncertainty_rollup"]["uncertainty_case_id"] == result_budget_case["uncertainty_case_id"]
    assert payload["uncertainty_rollup"]["method_confirmation_protocol_id"] == result_budget_case[
        "method_confirmation_protocol_id"
    ]
    assert payload["uncertainty_rollup"]["budget_level_summary"]
    assert payload["uncertainty_rollup"]["binding_summary"]
    assert payload["uncertainty_rollup"]["calculation_chain_summary"]
    assert payload["uncertainty_rollup"]["fixture_summary"]
    assert payload["uncertainty_rollup"]["rollup_summary_display"]
    assert any("uncertainty_budget_inputs.json" in str(value) for value in fixture_paths.values())
    assert snapshot["uncertainty_rollup"]["rollup_summary_display"]
    assert uncertainty_binding["scope_id"] == results_payload["recognition_binding"]["scope_id"]
    assert uncertainty_binding["decision_rule_id"] == results_payload["recognition_binding"]["decision_rule_id"]
    assert uncertainty_binding["uncertainty_case_id"] == result_budget_case["uncertainty_case_id"]
    assert uncertainty_binding["method_confirmation_protocol_id"] == result_budget_case[
        "method_confirmation_protocol_id"
    ]
    assert uncertainty_binding["not_real_acceptance_evidence"] is True
    assert uncertainty_binding["not_ready_for_formal_claim"] is True
    assert (
        "不确定度概览" in results_payload["result_summary_text"]
        or "Uncertainty overview" in results_payload["result_summary_text"]
    )
    assert (
        "预算完整度" in results_payload["result_summary_text"]
        or "Budget completeness" in results_payload["result_summary_text"]
    )
    assert (
        "主要不确定度贡献" in results_payload["result_summary_text"]
        or "Top uncertainty contributors" in results_payload["result_summary_text"]
    )
    assert "budget levels" in results_payload["result_summary_text"].lower()
    assert "uncertainty binding" in results_payload["result_summary_text"].lower()
    assert "calculation chain" in results_payload["result_summary_text"].lower()
    assert "fixture" in results_payload["result_summary_text"].lower()
    assert results_payload["uncertainty_report_pack"]["artifact_type"] == "uncertainty_report_pack"
    assert results_payload["uncertainty_digest"]["artifact_type"] == "uncertainty_digest"
    assert results_payload["uncertainty_rollup"]["artifact_type"] == "uncertainty_rollup"


def test_uncertainty_wp3_db_stub_and_placeholder_fallback(tmp_path: Path) -> None:
    run_dir = tmp_path / "legacy_run"
    _write_legacy_run(run_dir)

    stub_payload = DatabaseReadyUncertaintyRepositoryStub(run_dir).load_snapshot()
    repository = FileBackedUncertaintyRepository(run_dir)
    snapshot = repository.load_snapshot()
    payload = UncertaintyGateway(run_dir).read_payload()

    assert stub_payload["uncertainty_rollup"]["repository_mode"] == "db_ready_stub"
    assert stub_payload["uncertainty_rollup"]["db_ready_stub"]["not_in_default_chain"] is True
    assert stub_payload["uncertainty_rollup"]["primary_evidence_rewritten"] is False
    assert payload["uncertainty_report_pack"]["reviewer_placeholder"] is True
    assert payload["uncertainty_digest"]["reviewer_placeholder"] is True
    assert payload["uncertainty_rollup"]["legacy_placeholder_used"] is True
    assert payload["uncertainty_rollup"]["report_pack_available_on_disk"] is False
    assert payload["uncertainty_rollup"]["ready_for_readiness_mapping"] is True
    assert payload["uncertainty_rollup"]["not_ready_for_formal_claim"] is True
    assert payload["uncertainty_rollup"]["not_real_acceptance_evidence"] is True
    assert payload["uncertainty_rollup"]["primary_evidence_rewritten"] is False
    assert payload["uncertainty_rollup"]["db_ready_stub"]["not_in_default_chain"] is True
    assert payload["uncertainty_rollup"]["missing_artifact_types"]
    assert payload["uncertainty_rollup"]["non_claim_note"]
    assert payload["uncertainty_report_pack"]["artifact_present_on_disk"] is False
    assert payload["uncertainty_rollup"]["overview_display"] or payload["uncertainty_rollup"]["rollup_summary_display"]
    assert snapshot["uncertainty_report_pack"]["reviewer_placeholder"] is True
