from __future__ import annotations

from copy import deepcopy

from gas_calibrator.v2.core.route_state_shadow import ShadowState, build_shadow_event, build_shadow_trace


GOLDEN_PRESSURES = [1100.0, 1000.0, 900.0, 800.0, 700.0, 600.0, 500.0]


def _co2_replay_calls() -> list[str]:
    calls = [
        "route_baseline:before CO2 route conditioning",
        "open_conditioning:vent_on",
        "open_co2_route:10",
        "wait_route_soak",
        "seal_transition:vent_off",
        "seal_transition:route_close",
    ]
    for pressure in GOLDEN_PRESSURES:
        calls.extend(
            [
                f"set_pressure_to_target:{pressure:.0f}",
                f"wait_pressure_stable:{pressure:.0f}",
                f"sample:{pressure:.0f}",
            ]
        )
    calls.append("cleanup:after CO2 source complete")
    return calls


def _co2_replay_sources() -> list[dict[str, object]]:
    sources: list[dict[str, object]] = [
        {
            "route": "co2",
            "source_action": "route_context_enter",
            "source_function": "Co2RouteRunner.execute",
            "point_index": 10,
        },
        {
            "route": "co2",
            "source_action": "open_conditioning",
            "source_function": "Co2RouteRunner.execute",
            "vent_state_observed": "ON",
            "route_open": True,
        },
        {
            "route": "co2",
            "source_action": "wait_route_soak",
            "source_trace_action": "wait_route_soak",
            "source_function": "Co2RouteRunner.execute",
            "route_open": True,
        },
        {
            "route": "co2",
            "source_action": "seal_transition",
            "source_function": "PressureControlService.pressurize_and_hold",
            "vent_state_observed": "OFF",
            "route_valve_state_observed": "sealed",
        },
    ]
    for pressure in GOLDEN_PRESSURES:
        sources.extend(
            [
                {
                    "route": "co2",
                    "source_action": "set_pressure_to_target",
                    "source_trace_action": "set_pressure_to_target",
                    "source_function": "PressureControlService.set_pressure_to_target",
                    "target": {"pressure_hpa": pressure},
                    "actual": {"pressure_stable": True, "vent_on": False},
                    "route_valve_state_observed": "sealed",
                    "point_tag": f"co2_groupa_800ppm_{pressure:.0f}hpa",
                },
                {
                    "route": "co2",
                    "source_action": "sample_start",
                    "source_trace_action": "sample_start",
                    "source_function": "Co2RouteRunner.execute",
                    "target": {"pressure_hpa": pressure},
                    "actual": {"vent_on": False},
                    "route_valve_state_observed": "sealed",
                    "sample_schema_version": "runtime",
                    "point_tag": f"co2_groupa_800ppm_{pressure:.0f}hpa",
                },
            ]
        )
    sources.append({"route": "co2", "source_action": "cleanup", "source_function": "ValveRoutingService.cleanup_co2_route"})
    return sources


def test_shadow_state_does_not_change_co2_golden_call_order() -> None:
    calls_before = _co2_replay_calls()
    calls_after = deepcopy(calls_before)
    _ = build_shadow_trace(_co2_replay_sources())

    assert calls_after == calls_before
    assert [item for item in calls_after if item.startswith("set_pressure_to_target:")] == [
        f"set_pressure_to_target:{pressure:.0f}" for pressure in GOLDEN_PRESSURES
    ]


def test_co2_sealed_only_shadow_never_enters_ambient_state_by_default() -> None:
    trace = build_shadow_trace(_co2_replay_sources())

    assert ShadowState.AMBIENT_OPEN_SAMPLING.value not in [event["shadow_state"] for event in trace]


def test_co2_shadow_records_open_conditioning_before_seal() -> None:
    trace = build_shadow_trace(_co2_replay_sources())
    states = [event["shadow_state"] for event in trace]

    assert states.index(ShadowState.OPEN_CONDITIONING.value) < states.index(ShadowState.SEAL_TRANSITION.value)


def test_co2_shadow_records_sealed_pressure_control_for_all_7_points() -> None:
    trace = build_shadow_trace(_co2_replay_sources())
    sealed = [
        event
        for event in trace
        if event["shadow_state"] == ShadowState.SEALED_PRESSURE_CONTROL.value and event["set_pressure_seen"]
    ]

    assert [event["target_pressure_hpa"] for event in sealed] == GOLDEN_PRESSURES
    assert len(sealed) == 7


def test_co2_shadow_records_cleanup_after_route() -> None:
    trace = build_shadow_trace(_co2_replay_sources())
    states = [event["shadow_state"] for event in trace]

    assert states[-1] == ShadowState.CLEANUP.value
    assert states.index(ShadowState.CLEANUP.value) > max(
        index for index, state in enumerate(states) if state == ShadowState.SEALED_PRESSURE_CONTROL.value
    )


def test_shadow_trace_does_not_emit_vent_command() -> None:
    trace = build_shadow_trace(_co2_replay_sources())

    assert all("vent_command" not in event for event in trace)
    assert all(event["behavior_changed"] is False for event in trace)
    assert all(event["observation_only"] is True for event in trace)


def test_shadow_trace_does_not_call_pressure_controller() -> None:
    class PressureControllerBomb:
        def __getattr__(self, name: str):
            raise AssertionError(f"pressure controller should not be accessed: {name}")

    event = build_shadow_event(
        {
            "route": "co2",
            "source_action": "set_pressure_to_target",
            "target": {"pressure_hpa": 1000.0},
            "pressure_controller": PressureControllerBomb(),
        }
    )

    assert event["shadow_state"] == ShadowState.SEALED_PRESSURE_CONTROL.value
    assert event["target_pressure_hpa"] == 1000.0


def test_shadow_trace_does_not_modify_sample_schema() -> None:
    sample_row = {"point_tag": "co2_groupa_800ppm_1100hpa", "co2_ppm": 800.0, "sample_index": 1}
    before = deepcopy(sample_row)
    event = build_shadow_event(
        {
            "route": "co2",
            "source_action": "sample_start",
            "target": {"pressure_hpa": 1100.0},
            "sample_schema_version": "runtime",
        }
    )

    assert sample_row == before
    assert event["sample_seen"] is True
    assert event["sample_schema_version"] == "runtime"
