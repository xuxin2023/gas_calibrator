from __future__ import annotations

from dataclasses import replace
import json
from pathlib import Path

import numpy as np
import pytest

from gas_calibrator.v2.core.offline_artifacts import build_suite_case_metadata
from gas_calibrator.v2.domain.services.ec_dynamic_metrology import DynamicPathMetadata
from gas_calibrator.v2.domain.services.ec_system_identification import (
    build_system_identification_acceptance,
    identify_empirical_transfer,
)
from gas_calibrator.v2.sim.ec_system_identification import (
    SystemIdentificationProtocol,
    build_ec_system_identification_offline_report,
    generate_prbs,
    load_system_identification_contract,
    simulate_system_identification,
)
from gas_calibrator.v2.sim.scenarios.suites import get_simulation_suite
from gas_calibrator.v2.ui_v2.i18n import display_suite_failure_type


def _protocol(
    *,
    include_upstream_reference: bool = True,
    reference_noise_std: float = 0.01,
    dut_noise_std: float = 0.01,
) -> SystemIdentificationProtocol:
    return SystemIdentificationProtocol(
        protocol_id="test_co2_prbs",
        gas="co2",
        sample_rate_hz=20.0,
        duration_s=280.0,
        chip_rate_hz=2.0,
        baseline_value=600.0,
        amplitude=200.0,
        source_delay_s=0.10,
        source_tau_s=0.30,
        reference_delay_s=0.05,
        reference_tau_s=0.04,
        reference_noise_std=reference_noise_std,
        dut_noise_std=dut_noise_std,
        random_seed=42,
        include_upstream_reference=include_upstream_reference,
    )


def _path() -> DynamicPathMetadata:
    return DynamicPathMetadata(
        analyzer_id="GA01",
        gas="co2",
        serial_position=1,
        sample_rate_hz=20.0,
        tube_length_m=1.5,
        tube_inner_diameter_mm=4.0,
        tube_material="PTFE",
        flow_slpm=12.0,
        cell_pressure_hpa=1000.0,
        cell_temperature_c=25.0,
        relative_humidity_pct=30.0,
        heated_tube=False,
        filter_id="SIM-FILTER-D1",
        transport_delay_s=0.30,
        fast_rise_tau_s=0.25,
        fast_fall_tau_s=0.25,
    )


def _analyze(
    protocol: SystemIdentificationProtocol,
) -> tuple[dict[str, object], dict[str, object]]:
    contract = load_system_identification_contract()
    targets = list(contract["target_frequencies_hz"])
    series = simulate_system_identification(
        protocol,
        _path(),
        target_frequencies_hz=targets,
    )
    analysis = identify_empirical_transfer(
        series,
        target_frequencies_hz=targets,
        warmup_s=float(contract["warmup_s"]),
        segment_size=int(contract["segment_size"]),
    )
    return analysis, contract


def test_prbs_is_deterministic_balanced_maximal_length_sequence() -> None:
    sequence = generate_prbs(length=511)
    repeated = generate_prbs(length=1022)
    bipolar = 2.0 * sequence - 1.0

    assert np.array_equal(repeated[:511], sequence)
    assert np.array_equal(repeated[511:], sequence)
    assert int(np.sum(sequence)) == 256
    assert set(sequence) == {0.0, 1.0}
    assert all(
        float(np.dot(bipolar, np.roll(bipolar, lag))) == pytest.approx(-1.0)
        for lag in range(1, 511)
    )


def test_clean_prbs_recovers_reference_to_dut_transfer() -> None:
    analysis, contract = _analyze(_protocol())
    acceptance = build_system_identification_acceptance(
        [analysis],
        contract=contract,
        protocol_id="test_clean_prbs",
    )

    assert analysis["status"] == "ok"
    assert analysis["input_source"] == "upstream_reference"
    assert analysis["target_frequency_count"] == 4
    assert float(analysis["prbs_period_count_after_warmup"]) >= 1.0
    for point in analysis["relative_transfer_points"]:
        assert float(point["coherence"]) > 0.98
        assert float(point["amplitude_relative_error"]) < 0.03
        assert float(point["phase_absolute_error_deg"]) < 2.0
        assert point["amplitude_ci95_db"]["method"] == "welch_segment_percentile_interval"
    assert acceptance["ec_dynamic_status"] == "simulation_system_id_pass"


def test_command_source_dynamics_are_not_attributed_to_dut() -> None:
    analysis, _contract = _analyze(_protocol())
    separations = list(analysis["source_separation"])

    assert analysis["source_transfer_separated"] is True
    assert max(float(item["difference_ratio"]) for item in separations) > 0.30
    assert any(
        abs(
            float(item["command_to_dut_amplitude_ratio"])
            - float(item["reference_to_dut_amplitude_ratio"])
        )
        > 0.10
        for item in separations
    )


def test_missing_upstream_reference_cannot_pass() -> None:
    analysis, contract = _analyze(_protocol(include_upstream_reference=False))
    acceptance = build_system_identification_acceptance(
        [analysis],
        contract=contract,
        protocol_id="test_missing_reference",
    )

    assert analysis["status"] == "invalid"
    assert analysis["flags"] == ["upstream_reference_missing"]
    assert acceptance["ec_dynamic_status"] == "simulation_system_id_fail"
    assert any("upstream_reference_used" in item for item in acceptance["failed_gate_names"])


def test_missing_path_metadata_or_clock_alignment_cannot_pass() -> None:
    analysis, contract = _analyze(_protocol())
    analysis["metadata"].pop("flow_slpm")
    analysis["time_alignment_verified"] = False
    acceptance = build_system_identification_acceptance(
        [analysis],
        contract=contract,
        protocol_id="test_missing_physical_context",
    )

    assert acceptance["ec_dynamic_status"] == "simulation_system_id_fail"
    assert any("required_path_metadata" in item for item in acceptance["failed_gate_names"])
    assert any("shared_clock_alignment" in item for item in acceptance["failed_gate_names"])


def test_low_coherence_high_noise_fixture_cannot_pass() -> None:
    noisy = replace(
        _protocol(),
        reference_noise_std=150.0,
        dut_noise_std=150.0,
    )
    analysis, contract = _analyze(noisy)
    acceptance = build_system_identification_acceptance(
        [analysis],
        contract=contract,
        protocol_id="test_high_noise",
    )

    assert acceptance["ec_dynamic_status"] == "simulation_system_id_fail"
    assert any("coherence@" in item for item in acceptance["failed_gate_names"])


def test_contract_rejects_device_io_unlock(tmp_path: Path) -> None:
    contract = load_system_identification_contract()
    contract["evidence_boundary"]["device_io_allowed"] = True
    unsafe = tmp_path / "unsafe_ec_d1_contract.json"
    unsafe.write_text(json.dumps(contract), encoding="utf-8")

    with pytest.raises(ValueError, match="device_io_allowed=false"):
        load_system_identification_contract(unsafe)


def test_offline_report_preserves_simulation_evidence_boundary(tmp_path: Path) -> None:
    result = build_ec_system_identification_offline_report(
        report_root=tmp_path,
        run_name="ec_d1_contract",
    )
    report = json.loads(Path(result["report_json"]).read_text(encoding="utf-8"))

    assert result["status"] == "MATCH"
    assert report["artifact_role"] == "diagnostic_analysis"
    assert report["evidence_source"] == "simulated"
    assert report["not_real_acceptance_evidence"] is True
    assert report["promotion_state"] == "blocked"
    assert report["static_calibration_status"] == "not_evaluated"
    assert report["ec_dynamic_status"] == "simulation_system_id_pass"
    assert report["real_acceptance_status"] == "blocked"
    assert report["analysis_count"] == 2
    assert Path(result["report_markdown"]).exists()
    assert Path(result["simulated_series"]).exists()


def test_regression_and_nightly_include_system_identification_only() -> None:
    for suite_name in ("regression", "nightly"):
        matching = [
            case.name
            for case in get_simulation_suite(suite_name).cases
            if case.kind == "ec_system_id"
        ]
        assert matching == ["ec_dynamic_system_identification_contract"]
    assert not any(
        case.kind == "ec_system_id"
        for case in get_simulation_suite("smoke").cases
    )


def test_suite_metadata_classifies_system_identification_as_simulated() -> None:
    metadata = build_suite_case_metadata(
        {
            "name": "ec_dynamic_system_identification_contract",
            "kind": "ec_system_id",
            "status": "MATCH",
            "ok": True,
            "artifact_dir": "",
            "details": {},
        },
        suite_name="regression",
    )

    assert metadata["evidence_source"] == "simulated"
    assert metadata["failure_type"] == "ec_dynamic_system_identification"
    assert (
        display_suite_failure_type(
            metadata["failure_type"],
            locale="zh_CN",
        )
        == "EC 动态系统辨识"
    )
