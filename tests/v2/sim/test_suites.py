from __future__ import annotations

from gas_calibrator.v2.sim import get_simulation_suite, list_simulation_suites


def test_simulation_suites_expose_expected_matrix_groups() -> None:
    suites = set(list_simulation_suites())

    assert {"smoke", "regression", "nightly", "parity"}.issubset(suites)


def test_smoke_and_nightly_suites_include_expected_cases() -> None:
    smoke = get_simulation_suite("smoke")
    nightly = get_simulation_suite("nightly")

    assert [case.name for case in smoke.cases] == [
        "full_route_success_with_relay_and_thermometer",
        "relay_stuck_channel_causes_route_mismatch",
        "thermometer_stale_reference",
        "pressure_reference_degraded",
        "summary_parity",
    ]
    assert {case.name for case in nightly.cases}.issuperset(
        {
            "full_route_success_with_relay_and_thermometer",
            "co2_only_skip0_success_eight_analyzers_with_relay",
            "relay_stuck_channel_causes_route_mismatch",
            "pressure_reference_degraded",
            "pressure_gauge_wrong_unit_configuration",
            "export_resilience",
            "summary_parity",
        }
    )
    assert nightly.description
