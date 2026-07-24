from __future__ import annotations

import json
from pathlib import Path

import pytest

from gas_calibrator.v2.domain.services.ec_dynamic_metrology import (
    DynamicPathMetadata,
    analyze_dynamic_channel,
    build_dynamic_acceptance,
)
from gas_calibrator.v2.sim.ec_dynamic import (
    DynamicProtocolDefinition,
    build_ec_dynamic_offline_report,
    load_ec_dynamic_contract,
    simulate_dynamic_protocol,
)
from gas_calibrator.v2.sim.scenarios.suites import get_simulation_suite


def _path(
    *,
    analyzer_id: str = "GA01",
    gas: str = "co2",
    serial_position: int = 1,
    delay_s: float = 0.4,
    rise_tau_s: float = 0.3,
    fall_tau_s: float = 0.3,
    memory_fraction: float = 0.0,
    memory_rise_tau_s: float = 1.0,
    memory_fall_tau_s: float = 1.0,
) -> DynamicPathMetadata:
    return DynamicPathMetadata(
        analyzer_id=analyzer_id,
        gas=gas,
        serial_position=serial_position,
        sample_rate_hz=20.0,
        tube_length_m=float(serial_position),
        tube_inner_diameter_mm=4.0,
        tube_material="PTFE",
        flow_slpm=12.0,
        cell_pressure_hpa=1000.0,
        cell_temperature_c=30.0 if gas == "h2o" else 25.0,
        relative_humidity_pct=65.0 if gas == "h2o" else 30.0,
        heated_tube=gas == "h2o",
        filter_id="SIM-FILTER",
        transport_delay_s=delay_s,
        fast_rise_tau_s=rise_tau_s,
        fast_fall_tau_s=fall_tau_s,
        memory_fraction=memory_fraction,
        memory_rise_tau_s=memory_rise_tau_s,
        memory_fall_tau_s=memory_fall_tau_s,
    )


def _protocol(
    *,
    gas: str = "co2",
    jitter_s: float = 0.0,
    dropout_fraction: float = 0.0,
) -> DynamicProtocolDefinition:
    return DynamicProtocolDefinition(
        protocol_id=f"test_{gas}_step",
        gas=gas,
        duration_s=50.0 if gas == "co2" else 70.0,
        step_up_s=5.0,
        step_down_s=25.0 if gas == "co2" else 35.0,
        baseline_value=400.0 if gas == "co2" else 2.0,
        step_value=800.0 if gas == "co2" else 20.0,
        timestamp_jitter_std_s=jitter_s,
        dropout_fraction=dropout_fraction,
        random_seed=42,
    )


def _analysis(
    protocol: DynamicProtocolDefinition,
    path: DynamicPathMetadata,
) -> dict[str, object]:
    series = simulate_dynamic_protocol(protocol, [path])
    return analyze_dynamic_channel(series["channels"][0], protocol=series["protocol"])


def test_dynamic_path_metadata_rejects_nonphysical_values() -> None:
    with pytest.raises(ValueError, match="flow_slpm"):
        DynamicPathMetadata(
            **{
                **_path().to_dict(),
                "flow_slpm": 0.0,
            }
        ).validate()

    with pytest.raises(ValueError, match="CO2 fixture"):
        _path(memory_fraction=0.1).validate()


def test_contract_rejects_any_device_io_unlock(tmp_path: Path) -> None:
    contract = load_ec_dynamic_contract()
    contract["evidence_boundary"]["device_io_allowed"] = True
    path = tmp_path / "unsafe_contract.json"
    path.write_text(json.dumps(contract), encoding="utf-8")

    with pytest.raises(ValueError, match="device_io_allowed=false"):
        load_ec_dynamic_contract(path)


def test_co2_step_analysis_recovers_known_delay_and_time_constant() -> None:
    path = _path(delay_s=0.4, rise_tau_s=0.3, fall_tau_s=0.3)
    analysis = _analysis(_protocol(), path)

    assert analysis["status"] == "ok"
    assert float(analysis["effective_rise_delay_s"]) == pytest.approx(0.35, abs=0.08)
    assert float(analysis["effective_rise_tau_s"]) == pytest.approx(0.3, abs=0.04)
    assert float(dict(analysis["rise"])["t90_s"]) == pytest.approx(0.4 + 2.3026 * 0.3, abs=0.10)
    assert float(analysis["gain"]) == pytest.approx(1.0, abs=0.002)
    assert len(list(analysis["effective_transfer_function"])) >= 2
    assert len(list(analysis["allan_deviation"])) >= 1


def test_serial_positions_preserve_increasing_transport_delay() -> None:
    protocol = _protocol()
    series = simulate_dynamic_protocol(
        protocol,
        [
            _path(analyzer_id="GA01", serial_position=1, delay_s=0.2),
            _path(analyzer_id="GA02", serial_position=2, delay_s=0.6),
        ],
    )
    analyses = [
        analyze_dynamic_channel(channel, protocol=series["protocol"])
        for channel in series["channels"]
    ]
    acceptance = build_dynamic_acceptance(
        analyses,
        contract=load_ec_dynamic_contract(),
        protocol_id=protocol.protocol_id,
    )

    assert float(analyses[1]["effective_rise_delay_s"]) > float(analyses[0]["effective_rise_delay_s"])
    serial_gate = next(
        item
        for item in acceptance["required_gates"]
        if item["name"] == "serial_position_delay_order"
    )
    assert serial_gate["passed"] is True


def test_timestamp_jitter_and_dropout_cannot_false_pass() -> None:
    contract = load_ec_dynamic_contract()
    jitter_protocol = _protocol(jitter_s=0.012)
    jitter_analysis = _analysis(jitter_protocol, _path())
    jitter_acceptance = build_dynamic_acceptance(
        [jitter_analysis],
        contract=contract,
        protocol_id=jitter_protocol.protocol_id,
    )
    assert jitter_acceptance["ec_dynamic_status"] == "simulation_contract_fail"
    assert any("timestamp_jitter_ratio" in item for item in jitter_acceptance["failed_gate_names"])

    dropout_protocol = _protocol(dropout_fraction=0.05)
    dropout_analysis = _analysis(dropout_protocol, _path())
    dropout_acceptance = build_dynamic_acceptance(
        [dropout_analysis],
        contract=contract,
        protocol_id=dropout_protocol.protocol_id,
    )
    assert dropout_acceptance["ec_dynamic_status"] == "simulation_contract_fail"
    assert any("dropout_fraction" in item for item in dropout_acceptance["failed_gate_names"])


def test_missing_physical_path_metadata_cannot_pass() -> None:
    protocol = _protocol()
    analysis = _analysis(protocol, _path())
    analysis["metadata"].pop("flow_slpm")

    acceptance = build_dynamic_acceptance(
        [analysis],
        contract=load_ec_dynamic_contract(),
        protocol_id=protocol.protocol_id,
    )

    assert acceptance["ec_dynamic_status"] == "simulation_contract_fail"
    metadata_gate = next(
        item
        for item in acceptance["required_gates"]
        if item["name"] == "required_path_metadata"
    )
    assert metadata_gate["passed"] is False
    assert metadata_gate["value"]["missing"] == ["flow_slpm"]


def test_h2o_sorption_hysteresis_exceeding_contract_is_rejected() -> None:
    protocol = _protocol(gas="h2o")
    path = _path(
        gas="h2o",
        delay_s=0.4,
        rise_tau_s=0.3,
        fall_tau_s=2.5,
        memory_fraction=0.45,
        memory_rise_tau_s=1.0,
        memory_fall_tau_s=8.0,
    )
    analysis = _analysis(protocol, path)
    acceptance = build_dynamic_acceptance(
        [analysis],
        contract=load_ec_dynamic_contract(),
        protocol_id=protocol.protocol_id,
    )

    assert float(analysis["rise_fall_tau_ratio"]) > 2.5
    assert acceptance["ec_dynamic_status"] == "simulation_contract_fail"
    assert any("rise_fall_tau_asymmetry" in item for item in acceptance["failed_gate_names"])


def test_offline_report_keeps_static_dynamic_and_real_status_separate(tmp_path: Path) -> None:
    result = build_ec_dynamic_offline_report(
        report_root=tmp_path,
        run_name="ec_dynamic_contract",
    )
    report = json.loads(Path(result["report_json"]).read_text(encoding="utf-8"))

    assert result["status"] == "MATCH"
    assert report["evidence_source"] == "simulated"
    assert report["artifact_role"] == "diagnostic_analysis"
    assert report["not_real_acceptance_evidence"] is True
    assert report["promotion_state"] == "blocked"
    assert report["static_calibration_status"] == "not_evaluated"
    assert report["ec_dynamic_status"] == "simulation_contract_pass"
    assert report["real_acceptance_status"] == "blocked"
    assert report["channel_count"] == 4
    assert Path(result["report_markdown"]).exists()
    assert Path(result["simulated_series"]).exists()


def test_simulation_suites_include_ec_dynamic_contract() -> None:
    for suite_name in ("smoke", "regression", "nightly"):
        suite = get_simulation_suite(suite_name)
        matching = [case for case in suite.cases if case.kind == "ec_dynamic"]
        assert [case.name for case in matching] == ["ec_dynamic_offline_contract"]
