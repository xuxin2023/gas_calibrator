from __future__ import annotations

from copy import deepcopy
import json
from pathlib import Path

import pytest

from gas_calibrator.validation.metrology.gas_analyzer_operating_envelope import (
    analyze_gas_analyzer_operating_envelope,
    build_gas_analyzer_operating_envelope_acceptance,
)
from gas_calibrator.v2.core.offline_artifacts import build_suite_case_metadata
from gas_calibrator.v2.sim.gas_analyzer_operating_envelope import (
    build_gas_analyzer_operating_envelope_offline_report,
    generate_gas_analyzer_static_envelope_fixture,
    load_gas_analyzer_operating_envelope_contract,
)
from gas_calibrator.v2.sim.scenarios.suites import get_simulation_suite
from gas_calibrator.v2.ui_v2.i18n import display_suite_failure_type


def _dynamic_performances() -> list[dict[str, object]]:
    return [
        {
            "gas": "co2",
            "analyzer_id": "GA01",
            "status": "ok",
            "usable_bandwidth_hz": 0.316,
            "low_frequency_effective_phase_delay_s": 0.46,
            "bandwidths": {"ten_percent_attenuation": {"decision_grade": "qualified"}},
        },
        {
            "gas": "h2o",
            "analyzer_id": "GA01",
            "status": "ok",
            "usable_bandwidth_hz": 0.137,
            "low_frequency_effective_phase_delay_s": 0.91,
            "bandwidths": {"ten_percent_attenuation": {"decision_grade": "qualified"}},
        },
    ]


def _clean_inputs() -> tuple[
    dict[str, object],
    list[dict[str, object]],
    list[dict[str, object]],
]:
    contract = load_gas_analyzer_operating_envelope_contract()
    measurements, interference = generate_gas_analyzer_static_envelope_fixture(contract)
    return contract, measurements, interference


def _evaluate(
    contract: dict[str, object],
    measurements: list[dict[str, object]],
    interference: list[dict[str, object]],
    dynamic: list[dict[str, object]] | None = None,
) -> tuple[dict[str, object], dict[str, object]]:
    envelope = analyze_gas_analyzer_operating_envelope(
        measurements,
        interference,
        _dynamic_performances() if dynamic is None else dynamic,
        contract=contract,
    )
    acceptance = build_gas_analyzer_operating_envelope_acceptance(
        envelope,
        contract=contract,
        protocol_id="test_ga_d3",
    )
    return envelope, acceptance


def test_clean_integrated_envelope_qualifies_all_environment_cells() -> None:
    assert (
        analyze_gas_analyzer_operating_envelope.__module__
        == "gas_calibrator.validation.metrology.gas_analyzer_operating_envelope"
    )
    old_owner = (
        Path(__file__).resolve().parents[2]
        / "src/gas_calibrator/v2/domain/services/gas_analyzer_operating_envelope.py"
    )
    assert not old_owner.exists()

    contract, measurements, interference = _clean_inputs()
    envelope, acceptance = _evaluate(contract, measurements, interference)

    assert len(measurements) == 2592
    assert len(interference) == 18
    assert envelope["status"] == "ok"
    assert acceptance["all_fixture_gates_passed"] is True
    assert acceptance["failed_gate_names"] == []
    assert envelope["co2_zero_gas_and_h2o_dry_gas_separated"] is True
    assert envelope["ec_flux_in_scope"] is False
    assert envelope["coefficient_fitting_in_scope"] is False
    assert envelope["automatic_dynamic_correction_applied"] is False
    for channel in envelope["channels"]:
        assert channel["coverage"] == {
            "expected_row_count": 1296,
            "observed_unique_row_count": 1296,
            "missing_row_count": 0,
            "duplicate_row_count": 0,
            "unexpected_row_count": 0,
            "missing_key_preview": [],
        }
        assert channel["interference_coverage"] == {
            "expected_row_count": 9,
            "observed_unique_row_count": 9,
            "missing_row_count": 0,
            "duplicate_row_count": 0,
            "unexpected_row_count": 0,
            "missing_key_preview": [],
        }
        qualified = channel["qualified_operating_envelope"]
        assert qualified["total_cell_count"] == 27
        assert qualified["failed_cell_count"] == 0
        assert qualified["rectangular_grid_complete"] is True


def test_co2_zero_and_h2o_dry_anchors_are_not_collapsed() -> None:
    contract, measurements, interference = _clean_inputs()
    envelope, acceptance = _evaluate(contract, measurements, interference)
    by_gas = {channel["gas"]: channel for channel in envelope["channels"]}

    assert by_gas["co2"]["anchor_role"] == "co2_zero_gas"
    assert by_gas["co2"]["anchor_target"] == 0.0
    assert by_gas["h2o"]["anchor_role"] == "h2o_dry_gas"
    assert by_gas["h2o"]["anchor_target"] == 0.2
    assert acceptance["all_fixture_gates_passed"] is True

    first_h2o_anchor = next(
        row
        for row in measurements
        if row["gas"] == "h2o" and row["target_value"] == 0.2
    )
    first_h2o_anchor["anchor_role"] = "co2_zero_gas"
    _envelope, rejected = _evaluate(contract, measurements, interference)
    assert any(
        "h2o:GA01:anchor_role_and_target" == item
        for item in rejected["failed_gate_names"]
    )


def test_missing_duplicate_and_unexpected_grid_rows_are_rejected() -> None:
    contract, measurements, interference = _clean_inputs()
    removed = measurements.pop(0)
    measurements.append(deepcopy(measurements[0]))
    measurements[1]["target_value"] = 777.0

    envelope, acceptance = _evaluate(contract, measurements, interference)
    co2 = next(channel for channel in envelope["channels"] if channel["gas"] == "co2")

    assert co2["coverage"]["missing_row_count"] >= 2
    assert co2["coverage"]["duplicate_row_count"] == 1
    assert co2["coverage"]["unexpected_row_count"] == 1
    assert co2["qualified_operating_envelope"]["rectangular_grid_complete"] is False
    assert acceptance["all_fixture_gates_passed"] is False
    assert "co2:GA01:complete_rectangular_grid" in acceptance["failed_gate_names"]
    assert removed["gas"] == "co2"


def test_bad_environment_corner_cannot_hide_inside_min_max_bounds() -> None:
    contract, measurements, interference = _clean_inputs()
    corner = next(
        row
        for row in measurements
        if row["gas"] == "co2"
        and row["temperature_c"] == 45.0
        and row["pressure_hpa"] == 1100.0
        and row["flow_slpm"] == 16.0
        and row["target_value"] == 1200.0
    )
    corner["measured_value"] = float(corner["measured_value"]) + 10.0
    envelope, acceptance = _evaluate(contract, measurements, interference)
    co2 = next(channel for channel in envelope["channels"] if channel["gas"] == "co2")

    failed_cells = [
        cell for cell in co2["environment_cells"] if cell["status"] == "unqualified"
    ]
    assert len(failed_cells) == 1
    assert failed_cells[0]["temperature_c"] == 45.0
    assert "co2:GA01:all_environment_cells_qualified" in acceptance["failed_gate_names"]
    assert "co2:GA01:span_normalized_error" in acceptance["failed_gate_names"]


def test_incomplete_interference_sweep_is_rejected() -> None:
    contract, measurements, interference = _clean_inputs()
    interference.pop(0)
    interference.append(deepcopy(interference[0]))
    interference[1]["target_value"] = 801.0
    envelope, acceptance = _evaluate(contract, measurements, interference)
    co2 = next(channel for channel in envelope["channels"] if channel["gas"] == "co2")

    assert co2["interference_coverage"]["missing_row_count"] >= 2
    assert co2["interference_coverage"]["duplicate_row_count"] == 1
    assert co2["interference_coverage"]["unexpected_row_count"] == 1
    assert "co2:GA01:complete_interference_sweep" in acceptance["failed_gate_names"]


def test_hysteresis_and_drift_regressions_are_rejected() -> None:
    contract, measurements, interference = _clean_inputs()
    for row in measurements:
        if row["gas"] == "co2" and row["sequence_direction"] == "descending":
            row["measured_value"] = float(row["measured_value"]) + 3.0
        if row["gas"] == "h2o" and row["session"] == "end":
            row["measured_value"] = float(row["measured_value"]) + 0.05
    _envelope, acceptance = _evaluate(contract, measurements, interference)

    assert "co2:GA01:hysteresis" in acceptance["failed_gate_names"]
    assert "h2o:GA01:drift" in acceptance["failed_gate_names"]


def test_reference_quality_and_cross_interference_are_hard_gates() -> None:
    contract, measurements, interference = _clean_inputs()
    measurements[0]["reference_quality"] = "degraded"
    measurements[1]["frame_usable"] = False
    measurements[2]["measured_value"] = float("nan")
    for row in interference:
        if row["gas"] == "co2" and row["interferent_value"] == 20.0:
            row["measured_value"] = float(row["measured_value"]) + 5.0
    interference[0]["reference_quality"] = "degraded"
    interference[1]["frame_usable"] = False
    interference[2]["reference_value"] = float("nan")
    _envelope, acceptance = _evaluate(contract, measurements, interference)

    assert "co2:GA01:reference_quality_healthy" in acceptance["failed_gate_names"]
    assert "co2:GA01:measurement_frames_usable" in acceptance["failed_gate_names"]
    assert (
        "co2:GA01:measurement_numeric_values_finite" in acceptance["failed_gate_names"]
    )
    assert "co2:GA01:interference_effect" in acceptance["failed_gate_names"]
    assert (
        "co2:GA01:interference_reference_quality_healthy"
        in acceptance["failed_gate_names"]
    )
    assert "co2:GA01:interference_frames_usable" in acceptance["failed_gate_names"]
    assert (
        "co2:GA01:interference_numeric_values_finite" in acceptance["failed_gate_names"]
    )
    assert acceptance["static_calibration_status"] == "simulation_static_envelope_fail"
    assert (
        acceptance["gas_analyzer_dynamic_status"]
        == "simulation_dynamic_dependency_pass"
    )


def test_dynamic_dependency_must_meet_bandwidth_and_delay_limits() -> None:
    contract, measurements, interference = _clean_inputs()
    dynamic = _dynamic_performances()
    dynamic[1]["usable_bandwidth_hz"] = 0.02
    dynamic[1]["low_frequency_effective_phase_delay_s"] = 2.5
    _envelope, acceptance = _evaluate(
        contract,
        measurements,
        interference,
        dynamic,
    )

    assert "h2o:GA01:ten_percent_bandwidth_hz" in acceptance["failed_gate_names"]
    assert (
        "h2o:GA01:low_frequency_effective_phase_delay_s"
        in acceptance["failed_gate_names"]
    )
    assert acceptance["static_calibration_status"] == "simulation_static_envelope_pass"
    assert (
        acceptance["gas_analyzer_dynamic_status"]
        == "simulation_dynamic_dependency_fail"
    )
    assert (
        acceptance["operating_envelope_status"] == "simulation_operating_envelope_fail"
    )


def test_analyzer_identity_must_match_static_interference_and_dynamic_data() -> None:
    contract, measurements, interference = _clean_inputs()
    interference[0]["analyzer_id"] = "GA02"
    unsupported_measurement = deepcopy(measurements[0])
    unsupported_measurement["gas"] = "ch4"
    measurements.append(unsupported_measurement)
    unsupported_interference = deepcopy(interference[0])
    unsupported_interference["gas"] = "ch4"
    interference.append(unsupported_interference)
    dynamic = _dynamic_performances()
    dynamic[1]["analyzer_id"] = "GA02"
    dynamic.append(deepcopy(dynamic[0]))
    dynamic.append(
        {
            "gas": "ch4",
            "analyzer_id": "GA01",
            "status": "ok",
        }
    )
    _envelope, acceptance = _evaluate(
        contract,
        measurements,
        interference,
        dynamic,
    )

    assert "co2:GA01:analyzer_identity_unambiguous" in acceptance["failed_gate_names"]
    assert "h2o:GA01:dynamic_analyzer_identity_match" in acceptance["failed_gate_names"]
    assert "global:all:supported_gas_scope_only" in acceptance["failed_gate_names"]


def test_contract_rejects_io_ec_scope_and_anchor_collapse(tmp_path: Path) -> None:
    contract = load_gas_analyzer_operating_envelope_contract()
    contract["evidence_boundary"]["device_io_allowed"] = True
    unsafe = tmp_path / "unsafe_ga_d3_contract.json"
    unsafe.write_text(json.dumps(contract), encoding="utf-8")
    with pytest.raises(ValueError, match="device_io_allowed=false"):
        load_gas_analyzer_operating_envelope_contract(unsafe)

    contract = load_gas_analyzer_operating_envelope_contract()
    contract["interpretation"]["ec_flux_or_cospectral_correction_in_scope"] = True
    unsafe.write_text(json.dumps(contract), encoding="utf-8")
    with pytest.raises(
        ValueError,
        match="ec_flux_or_cospectral_correction_in_scope=false",
    ):
        load_gas_analyzer_operating_envelope_contract(unsafe)

    contract = load_gas_analyzer_operating_envelope_contract()
    contract["gas_contracts"]["h2o"]["anchor_role"] = "co2_zero_gas"
    unsafe.write_text(json.dumps(contract), encoding="utf-8")
    with pytest.raises(ValueError, match="h2o anchor role must be h2o_dry_gas"):
        load_gas_analyzer_operating_envelope_contract(unsafe)


def test_offline_report_preserves_roles_lineage_and_no_write_boundary(
    tmp_path: Path,
) -> None:
    result = build_gas_analyzer_operating_envelope_offline_report(
        report_root=tmp_path,
        run_name="ga_d3_contract",
    )
    report = json.loads(Path(result["report_json"]).read_text(encoding="utf-8"))
    inputs = json.loads(Path(result["execution_rows"]).read_text(encoding="utf-8"))

    assert result["status"] == "MATCH"
    assert report["artifact_role"] == "diagnostic_analysis"
    assert inputs["artifact_role"] == "execution_rows"
    assert report["evidence_source"] == "simulated"
    assert report["not_real_acceptance_evidence"] is True
    assert report["promotion_state"] == "blocked"
    assert report["static_calibration_status"] == "simulation_static_envelope_pass"
    assert report["gas_analyzer_dynamic_status"] == "simulation_dynamic_dependency_pass"
    assert report["operating_envelope_status"] == "simulation_operating_envelope_pass"
    assert report["ec_flux_status"] == "not_in_scope"
    assert report["real_acceptance_status"] == "blocked"
    assert report["coefficient_fit_status"] == "not_applied"
    assert report["coefficient_writeback_status"] == "not_applied"
    assert report["dynamic_correction_status"] == "not_applied"
    assert Path(result["ga_d2_dynamic_dependency_report"]).exists()
    assert Path(result["report_markdown"]).exists()
    markdown = Path(result["report_markdown"]).read_text(encoding="utf-8")
    assert "CO2 零气锚点与 H2O 干气点分别评价" in markdown
    assert "不包含涡动协方差、协谱或通量闭合" in markdown


def test_regression_and_nightly_include_ga_d3_and_metadata_is_chinese() -> None:
    for suite_name in ("regression", "nightly"):
        matching = [
            case.name
            for case in get_simulation_suite(suite_name).cases
            if case.kind == "ga_operating_envelope"
        ]
        assert matching == ["gas_analyzer_operating_envelope_contract"]
    assert not any(
        case.kind == "ga_operating_envelope"
        for case in get_simulation_suite("smoke").cases
    )

    metadata = build_suite_case_metadata(
        {
            "name": "gas_analyzer_operating_envelope_contract",
            "kind": "ga_operating_envelope",
            "status": "MATCH",
            "ok": True,
            "artifact_dir": "",
            "details": {},
        },
        suite_name="regression",
    )
    assert metadata["evidence_source"] == "simulated"
    assert metadata["failure_type"] == "gas_analyzer_operating_envelope"
    assert (
        display_suite_failure_type(metadata["failure_type"], locale="zh_CN")
        == "气体分析仪综合工作包络"
    )
