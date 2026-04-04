from __future__ import annotations

from importlib import import_module
from typing import Any


_EXPORTS = {
    "DEFAULT_REPLAY_FIXTURE_ROOT": ("gas_calibrator.v2.sim.replay", "DEFAULT_REPLAY_FIXTURE_ROOT"),
    "SimulatedScenarioDefinition": ("gas_calibrator.v2.sim.scenarios", "SimulatedScenarioDefinition"),
    "SimulationSuiteCase": ("gas_calibrator.v2.sim.scenarios", "SimulationSuiteCase"),
    "SimulationSuiteDefinition": ("gas_calibrator.v2.sim.scenarios", "SimulationSuiteDefinition"),
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
    "simulated_profile_defaults": ("gas_calibrator.v2.sim.scenarios", "simulated_profile_defaults"),
}

__all__ = sorted(_EXPORTS)


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
