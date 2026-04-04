from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from .catalog import get_simulated_scenario


@dataclass(frozen=True)
class SimulationSuiteCase:
    name: str
    kind: str
    scenario: Optional[str] = None
    profile: Optional[str] = None
    expected_statuses: tuple[str, ...] = ("MATCH",)


@dataclass(frozen=True)
class SimulationSuiteDefinition:
    name: str
    description: str
    cases: tuple[SimulationSuiteCase, ...]


def _scenario_case(name: str, *, expected_statuses: tuple[str, ...]) -> SimulationSuiteCase:
    scenario = get_simulated_scenario(name)
    return SimulationSuiteCase(
        name=name,
        kind="scenario",
        scenario=name,
        profile=scenario.validation_profile,
        expected_statuses=expected_statuses,
    )


SUITES: dict[str, SimulationSuiteDefinition] = {
    "smoke": SimulationSuiteDefinition(
        name="smoke",
        description="核心离线烟测：主成功路径、路由门禁、参考仪器门禁。",
        cases=(
            _scenario_case("full_route_success_with_relay_and_thermometer", expected_statuses=("MATCH",)),
            _scenario_case("relay_stuck_channel_causes_route_mismatch", expected_statuses=("MISMATCH",)),
            _scenario_case("thermometer_stale_reference", expected_statuses=("MATCH",)),
            _scenario_case("pressure_reference_degraded", expected_statuses=("MATCH",)),
            SimulationSuiteCase(name="summary_parity", kind="parity", expected_statuses=("MATCH",)),
        ),
    ),
    "regression": SimulationSuiteDefinition(
        name="regression",
        description="日常离线回归：主路径、关键故障、参考质量和证据状态。",
        cases=(
            _scenario_case("full_route_success_with_relay_and_thermometer", expected_statuses=("MATCH",)),
            _scenario_case("relay_route_switch_h2o_success", expected_statuses=("MATCH",)),
            _scenario_case("co2_only_skip0_success_eight_analyzers_with_relay", expected_statuses=("MATCH",)),
            _scenario_case("analyzer_mode2_partial_frame_protocol", expected_statuses=("NOT_EXECUTED",)),
            _scenario_case("pace_no_response_cleanup", expected_statuses=("MISMATCH",)),
            _scenario_case("pace_unsupported_header", expected_statuses=("MISMATCH",)),
            _scenario_case("humidity_generator_timeout", expected_statuses=("MISMATCH",)),
            _scenario_case("temperature_chamber_stalled", expected_statuses=("MISMATCH",)),
            _scenario_case("relay_stuck_channel_causes_route_mismatch", expected_statuses=("MISMATCH",)),
            _scenario_case("thermometer_stale_reference", expected_statuses=("MATCH",)),
            _scenario_case("thermometer_no_response", expected_statuses=("MATCH",)),
            _scenario_case("pressure_reference_degraded", expected_statuses=("MATCH",)),
            _scenario_case("pressure_gauge_wrong_unit_configuration", expected_statuses=("MATCH",)),
            SimulationSuiteCase(name="primary_latest_missing", kind="replay", expected_statuses=("SNAPSHOT_ONLY",)),
            SimulationSuiteCase(
                name="stale_h2o_latest_present_but_not_primary",
                kind="replay",
                expected_statuses=("SNAPSHOT_ONLY",),
            ),
        ),
    ),
    "nightly": SimulationSuiteDefinition(
        name="nightly",
        description="夜间扩展覆盖：回归矩阵 + parity + resilience + 参考仪器异常。",
        cases=(
            _scenario_case("full_route_success_with_relay_and_thermometer", expected_statuses=("MATCH",)),
            _scenario_case("relay_route_switch_h2o_success", expected_statuses=("MATCH",)),
            _scenario_case("co2_only_skip0_success_eight_analyzers_with_relay", expected_statuses=("MATCH",)),
            _scenario_case("relay_stuck_channel_causes_route_mismatch", expected_statuses=("MISMATCH",)),
            _scenario_case("thermometer_stale_reference", expected_statuses=("MATCH",)),
            _scenario_case("thermometer_no_response", expected_statuses=("MATCH",)),
            _scenario_case("pressure_reference_degraded", expected_statuses=("MATCH",)),
            _scenario_case("pressure_gauge_wrong_unit_configuration", expected_statuses=("MATCH",)),
            _scenario_case("analyzer_mode2_partial_frame_protocol", expected_statuses=("NOT_EXECUTED",)),
            _scenario_case("pace_no_response_cleanup", expected_statuses=("MISMATCH",)),
            _scenario_case("pace_unsupported_header", expected_statuses=("MISMATCH",)),
            _scenario_case("humidity_generator_timeout", expected_statuses=("MISMATCH",)),
            _scenario_case("temperature_chamber_stalled", expected_statuses=("MISMATCH",)),
            SimulationSuiteCase(name="primary_latest_missing", kind="replay", expected_statuses=("SNAPSHOT_ONLY",)),
            SimulationSuiteCase(
                name="stale_h2o_latest_present_but_not_primary",
                kind="replay",
                expected_statuses=("SNAPSHOT_ONLY",),
            ),
            SimulationSuiteCase(name="export_resilience", kind="resilience", expected_statuses=("MATCH",)),
            SimulationSuiteCase(name="summary_parity", kind="parity", expected_statuses=("MATCH",)),
        ),
    ),
    "parity": SimulationSuiteDefinition(
        name="parity",
        description="只检查 V1/V2 summary/export 口径一致性。",
        cases=(SimulationSuiteCase(name="summary_parity", kind="parity", expected_statuses=("MATCH",)),),
    ),
}


def list_simulation_suites() -> list[str]:
    return sorted(SUITES)


def get_simulation_suite(name: str) -> SimulationSuiteDefinition:
    try:
        return SUITES[str(name)]
    except KeyError as exc:
        raise KeyError(f"unknown simulation suite: {name}") from exc
