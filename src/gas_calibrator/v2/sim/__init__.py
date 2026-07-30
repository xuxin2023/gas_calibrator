from __future__ import annotations

from importlib import import_module
from pathlib import Path
from typing import Any


_EXPORTS = {
    "DEFAULT_REPLAY_FIXTURE_ROOT": ("gas_calibrator.v2.sim.replay", "DEFAULT_REPLAY_FIXTURE_ROOT"),
    "DynamicProtocolDefinition": ("gas_calibrator.v2.sim.ec_dynamic", "DynamicProtocolDefinition"),
    "SystemIdentificationProtocol": (
        "gas_calibrator.v2.sim.ec_system_identification",
        "SystemIdentificationProtocol",
    ),
    "SimulatedScenarioDefinition": ("gas_calibrator.v2.sim.scenarios", "SimulatedScenarioDefinition"),
    "SimulationSuiteCase": ("gas_calibrator.v2.sim.scenarios", "SimulationSuiteCase"),
    "SimulationSuiteDefinition": ("gas_calibrator.v2.sim.scenarios", "SimulationSuiteDefinition"),
    "build_ec_dynamic_offline_report": (
        "gas_calibrator.v2.sim.ec_dynamic",
        "build_ec_dynamic_offline_report",
    ),
    "build_ec_system_identification_offline_report": (
        "gas_calibrator.v2.sim.ec_system_identification",
        "build_ec_system_identification_offline_report",
    ),
    "build_gas_analyzer_dynamic_uncertainty_offline_report": (
        "gas_calibrator.v2.sim.gas_analyzer_dynamic_uncertainty",
        "build_gas_analyzer_dynamic_uncertainty_offline_report",
    ),
    "build_gas_analyzer_asset_dossier_offline_report": (
        "gas_calibrator.v2.sim.gas_analyzer_asset_dossier",
        "build_gas_analyzer_asset_dossier_offline_report",
    ),
    "build_gas_analyzer_bench_readiness_offline_report": (
        "gas_calibrator.v2.sim.gas_analyzer_bench_readiness",
        "build_gas_analyzer_bench_readiness_offline_report",
    ),
    "build_gas_analyzer_operating_envelope_offline_report": (
        "gas_calibrator.v2.sim.gas_analyzer_operating_envelope",
        "build_gas_analyzer_operating_envelope_offline_report",
    ),
    "build_protocol_simulated_compare_result": (
        "gas_calibrator.v2.sim.protocol",
        "build_protocol_simulated_compare_result",
    ),
    "build_export_resilience_report": ("gas_calibrator.v2.sim.resilience", "build_export_resilience_report"),
    "build_summary_parity_report": ("gas_calibrator.v2.sim.parity", "build_summary_parity_report"),
    "get_simulated_scenario": ("gas_calibrator.v2.sim.scenarios", "get_simulated_scenario"),
    "get_simulation_suite": ("gas_calibrator.v2.sim.scenarios", "get_simulation_suite"),
    "list_replay_scenarios": ("gas_calibrator.v2.sim.replay", "list_replay_scenarios"),
    "list_simulated_profiles": ("gas_calibrator.v2.sim.scenarios", "list_simulated_profiles"),
    "list_simulated_scenarios": ("gas_calibrator.v2.sim.scenarios", "list_simulated_scenarios"),
    "list_simulation_suites": ("gas_calibrator.v2.sim.scenarios", "list_simulation_suites"),
    "load_replay_fixture": ("gas_calibrator.v2.sim.replay", "load_replay_fixture"),
    "materialize_replay_fixture": ("gas_calibrator.v2.sim.replay", "materialize_replay_fixture"),
    "generate_prbs": ("gas_calibrator.v2.sim.ec_system_identification", "generate_prbs"),
    "default_system_identification_fixtures": (
        "gas_calibrator.v2.sim.ec_system_identification",
        "default_system_identification_fixtures",
    ),
    "simulate_dynamic_protocol": ("gas_calibrator.v2.sim.ec_dynamic", "simulate_dynamic_protocol"),
    "simulate_system_identification": (
        "gas_calibrator.v2.sim.ec_system_identification",
        "simulate_system_identification",
    ),
    "simulated_profile_defaults": ("gas_calibrator.v2.sim.scenarios", "simulated_profile_defaults"),
}


def build_certificate_operational_admission_offline_report(
    *,
    report_root: str | Path,
    run_name: str = "ga_d6b_certificate_operational_admission",
) -> dict[str, Any]:
    """Build the deterministic GA-D6B certificate-admission suite report."""
    from gas_calibrator.validation.certificate_operational_admission import (
        build_locked_fixture_verification,
        evaluate_certificate_operational_admission,
        load_certificate_operational_admission_contract,
        load_owner_attested_certificate_evidence,
        write_certificate_operational_admission_artifacts,
    )

    contract = load_certificate_operational_admission_contract()
    evidence = load_owner_attested_certificate_evidence()
    verification = build_locked_fixture_verification(evidence)
    result = evaluate_certificate_operational_admission(
        evidence,
        contract=contract,
        source_verification=verification,
    )
    report_dir = Path(report_root).resolve() / run_name
    artifacts = write_certificate_operational_admission_artifacts(result, output_dir=report_dir)
    contract_match = (
        result.get("status") == "PASSED_WITH_OWNER_ATTESTATION"
        and result.get("operational_certificate_gate_passed") is True
        and result.get("strict_original_certificate_gate_passed") is False
        and result.get("ready_for_real_execution") is False
    )
    return {
        "status": "MATCH" if contract_match else "MISMATCH",
        "compare_status": "MATCH" if contract_match else "MISMATCH",
        "report_dir": str(report_dir),
        "report_json": artifacts["diagnostic_analysis"],
        "report_markdown": artifacts["diagnostic_markdown"],
        "execution_rows": artifacts["execution_rows"],
        "execution_summary": artifacts["execution_summary"],
        "formal_analysis": artifacts["formal_analysis"],
        "report": result,
    }


__all__ = sorted([*_EXPORTS, "build_certificate_operational_admission_offline_report"])


def __getattr__(name: str) -> Any:
    try:
        module_name, attribute_name = _EXPORTS[name]
    except KeyError as exc:
        raise AttributeError(name) from exc
    module = import_module(module_name)
    value = getattr(module, attribute_name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted(set(globals()) | set(__all__))
