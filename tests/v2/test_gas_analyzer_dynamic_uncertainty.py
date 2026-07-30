from __future__ import annotations

from dataclasses import replace
import json
from math import sqrt
from pathlib import Path

import pytest

from gas_calibrator.validation.metrology.ec_system_identification import (
    identify_empirical_transfer,
)
from gas_calibrator.validation.metrology.gas_analyzer_dynamic_uncertainty import (
    analyze_gas_analyzer_dynamic_performance,
    build_gas_analyzer_dynamic_uncertainty_acceptance,
)
from gas_calibrator.v2.core.offline_artifacts import build_suite_case_metadata
from gas_calibrator.v2.sim.ec_system_identification import (
    default_system_identification_fixtures,
    simulate_system_identification,
)
from gas_calibrator.v2.sim.gas_analyzer_dynamic_uncertainty import (
    build_gas_analyzer_dynamic_uncertainty_offline_report,
    load_gas_analyzer_dynamic_uncertainty_contract,
)
from gas_calibrator.v2.sim.scenarios.suites import get_simulation_suite
from gas_calibrator.v2.ui_v2.i18n import display_suite_failure_type


def _frequencies(contract: dict[str, object]) -> list[float]:
    grid = dict(contract["evaluation_grid"])
    return [
        index * float(grid["sample_rate_hz"]) / int(grid["segment_size"])
        for index in range(
            int(grid["first_positive_bin"]),
            int(grid["last_positive_bin"]) + 1,
        )
    ]


def _performance(
    gas: str,
    *,
    protocol_override=None,
    path_override=None,
) -> tuple[dict[str, object], dict[str, object], dict[str, object]]:
    contract = load_gas_analyzer_dynamic_uncertainty_contract()
    protocol, path = next(
        item
        for item in default_system_identification_fixtures()
        if item[0].gas == gas
    )
    protocol = protocol if protocol_override is None else protocol_override
    path = path if path_override is None else path_override
    series = simulate_system_identification(
        protocol,
        path,
        target_frequencies_hz=_frequencies(contract),
    )
    grid = dict(contract["evaluation_grid"])
    identification = identify_empirical_transfer(
        series,
        target_frequencies_hz=_frequencies(contract),
        warmup_s=float(grid["warmup_s"]),
        segment_size=int(grid["segment_size"]),
    )
    performance = analyze_gas_analyzer_dynamic_performance(
        identification,
        contract=contract,
    )
    return performance, identification, contract


def test_clean_co2_and_h2o_bandwidths_are_physically_ordered() -> None:
    assert (
        analyze_gas_analyzer_dynamic_performance.__module__
        == "gas_calibrator.validation.metrology.gas_analyzer_dynamic_uncertainty"
    )
    old_owner = (
        Path(__file__).resolve().parents[2]
        / "src/gas_calibrator/v2/domain/services/gas_analyzer_dynamic_uncertainty.py"
    )
    assert not old_owner.exists()

    co2, _co2_id, _contract = _performance("co2")
    h2o, _h2o_id, _contract = _performance("h2o")

    assert co2["status"] == "ok"
    assert h2o["status"] == "ok"
    assert float(co2["usable_bandwidth_hz"]) > float(h2o["usable_bandwidth_hz"])
    assert float(co2["usable_bandwidth_hz"]) == pytest.approx(0.316, abs=0.03)
    assert float(h2o["usable_bandwidth_hz"]) == pytest.approx(0.137, abs=0.03)
    assert (
        float(co2["bandwidths"]["minus_3db"]["frequency_hz"])
        > float(h2o["bandwidths"]["minus_3db"]["frequency_hz"])
    )
    assert float(h2o["low_frequency_effective_phase_delay_s"]) > float(
        co2["low_frequency_effective_phase_delay_s"]
    )


def test_five_percent_bandwidth_is_diagnostic_but_decision_bands_are_qualified() -> None:
    for gas in ("co2", "h2o"):
        performance, _identification, _contract = _performance(gas)
        bandwidths = performance["bandwidths"]

        assert bandwidths["five_percent_attenuation"]["decision_grade"] == "diagnostic_only"
        assert bandwidths["ten_percent_attenuation"]["decision_grade"] == "qualified"
        assert bandwidths["minus_3db"]["decision_grade"] == "qualified"
        assert (
            float(
                bandwidths["five_percent_attenuation"][
                    "expanded_relative_uncertainty"
                ]
            )
            > 0.35
        )


def test_uncertainty_budget_uses_rss_and_keeps_dynamic_bias_separate() -> None:
    performance, _identification, _contract = _performance("co2")
    budget = performance["uncertainty_budget"]
    point = budget["points"][0]
    amplitude_components = [
        float(item["amplitude_relative_standard_uncertainty"])
        for item in point["components"]
    ]
    phase_components = [
        float(item["phase_standard_uncertainty_deg"])
        for item in point["components"]
    ]

    assert float(point["combined_amplitude_relative_standard_uncertainty"]) == pytest.approx(
        sqrt(sum(value**2 for value in amplitude_components)),
        abs=1e-8,
    )
    assert float(point["combined_phase_standard_uncertainty_deg"]) == pytest.approx(
        sqrt(sum(value**2 for value in phase_components)),
        abs=1e-8,
    )
    assert float(point["expanded_amplitude_relative_uncertainty"]) == pytest.approx(
        2.0 * float(point["combined_amplitude_relative_standard_uncertainty"]),
        abs=1e-8,
    )
    assert budget["dynamic_attenuation_included_as_uncertainty"] is False
    assert performance["automatic_dynamic_correction_applied"] is False
    assert performance["correction_factor_output"] is None


def test_high_noise_cannot_pass_coherence_or_uncertainty_gates() -> None:
    protocol, _path = next(
        item
        for item in default_system_identification_fixtures()
        if item[0].gas == "co2"
    )
    noisy_protocol = replace(
        protocol,
        reference_noise_std=150.0,
        dut_noise_std=150.0,
    )
    performance, _identification, contract = _performance(
        "co2",
        protocol_override=noisy_protocol,
    )
    acceptance = build_gas_analyzer_dynamic_uncertainty_acceptance(
        [performance],
        contract=contract,
        protocol_id="test_high_noise",
    )

    assert acceptance["gas_analyzer_dynamic_status"] == "simulation_dynamic_uncertainty_fail"
    assert any("minimum_coherence" in item for item in acceptance["failed_gate_names"])
    assert any(
        "expanded_amplitude_relative_uncertainty" in item
        for item in acceptance["failed_gate_names"]
    )


def test_missing_clock_or_physical_metadata_cannot_pass() -> None:
    _performance_row, identification, contract = _performance("co2")
    identification["time_alignment_verified"] = False
    identification["metadata"].pop("flow_slpm")
    performance = analyze_gas_analyzer_dynamic_performance(
        identification,
        contract=contract,
    )
    acceptance = build_gas_analyzer_dynamic_uncertainty_acceptance(
        [performance],
        contract=contract,
        protocol_id="test_bad_context",
    )

    assert any("shared_clock_alignment" in item for item in acceptance["failed_gate_names"])
    assert any("required_path_metadata" in item for item in acceptance["failed_gate_names"])


def test_incomplete_prbs_period_cannot_pass() -> None:
    _performance_row, identification, contract = _performance("co2")
    identification["prbs_period_count_after_warmup"] = 0.5
    performance = analyze_gas_analyzer_dynamic_performance(
        identification,
        contract=contract,
    )
    acceptance = build_gas_analyzer_dynamic_uncertainty_acceptance(
        [performance],
        contract=contract,
        protocol_id="test_short_prbs",
    )

    assert any(
        "prbs_period_coverage_after_warmup" in item
        for item in acceptance["failed_gate_names"]
    )


def test_severe_h2o_memory_is_rejected_as_insufficient_bandwidth() -> None:
    _protocol, path = next(
        item
        for item in default_system_identification_fixtures()
        if item[0].gas == "h2o"
    )
    severe_memory_path = replace(
        path,
        memory_fraction=0.65,
        memory_rise_tau_s=8.0,
        memory_fall_tau_s=8.0,
    )
    performance, _identification, contract = _performance(
        "h2o",
        path_override=severe_memory_path,
    )
    acceptance = build_gas_analyzer_dynamic_uncertainty_acceptance(
        [performance],
        contract=contract,
        protocol_id="test_severe_h2o_memory",
    )

    assert float(performance["usable_bandwidth_hz"]) < 0.02
    assert acceptance["gas_analyzer_dynamic_status"] == "simulation_dynamic_uncertainty_fail"
    assert any("ten_percent_bandwidth_hz" in item for item in acceptance["failed_gate_names"])


def test_contract_rejects_ec_scope_or_automatic_correction(tmp_path: Path) -> None:
    contract = load_gas_analyzer_dynamic_uncertainty_contract()
    contract["evidence_boundary"]["automatic_dynamic_correction_allowed"] = True
    unsafe = tmp_path / "unsafe_ga_d2_contract.json"
    unsafe.write_text(json.dumps(contract), encoding="utf-8")

    with pytest.raises(ValueError, match="automatic_dynamic_correction_allowed=false"):
        load_gas_analyzer_dynamic_uncertainty_contract(unsafe)

    contract = load_gas_analyzer_dynamic_uncertainty_contract()
    contract["interpretation"]["ec_flux_or_cospectral_correction_in_scope"] = True
    unsafe.write_text(json.dumps(contract), encoding="utf-8")
    with pytest.raises(
        ValueError,
        match="ec_flux_or_cospectral_correction_in_scope=false",
    ):
        load_gas_analyzer_dynamic_uncertainty_contract(unsafe)


def test_offline_report_preserves_analyzer_only_evidence_boundary(
    tmp_path: Path,
) -> None:
    result = build_gas_analyzer_dynamic_uncertainty_offline_report(
        report_root=tmp_path,
        run_name="ga_d2_contract",
    )
    report = json.loads(Path(result["report_json"]).read_text(encoding="utf-8"))

    assert result["status"] == "MATCH"
    assert report["artifact_role"] == "diagnostic_analysis"
    assert report["evidence_source"] == "simulated"
    assert report["not_real_acceptance_evidence"] is True
    assert report["promotion_state"] == "blocked"
    assert report["gas_analyzer_dynamic_status"] == "simulation_dynamic_uncertainty_pass"
    assert report["ec_flux_status"] == "not_in_scope"
    assert report["real_acceptance_status"] == "blocked"
    assert report["dynamic_correction_status"] == "not_applied"
    assert report["performance_count"] == 2
    assert Path(result["report_markdown"]).exists()
    assert Path(result["system_identification_inputs"]).exists()


def test_regression_and_nightly_include_ga_d2_but_smoke_does_not() -> None:
    for suite_name in ("regression", "nightly"):
        matching = [
            case.name
            for case in get_simulation_suite(suite_name).cases
            if case.kind == "ga_dynamic_uncertainty"
        ]
        assert matching == ["gas_analyzer_dynamic_uncertainty_contract"]
    assert not any(
        case.kind == "ga_dynamic_uncertainty"
        for case in get_simulation_suite("smoke").cases
    )


def test_suite_metadata_and_chinese_label_use_analyzer_scope() -> None:
    metadata = build_suite_case_metadata(
        {
            "name": "gas_analyzer_dynamic_uncertainty_contract",
            "kind": "ga_dynamic_uncertainty",
            "status": "MATCH",
            "ok": True,
            "artifact_dir": "",
            "details": {},
        },
        suite_name="regression",
    )

    assert metadata["evidence_source"] == "simulated"
    assert metadata["failure_type"] == "gas_analyzer_dynamic_uncertainty"
    assert (
        display_suite_failure_type(metadata["failure_type"], locale="zh_CN")
        == "气体分析仪动态不确定度"
    )
