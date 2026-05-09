from __future__ import annotations

from gas_calibrator.v2.core.route_state_shadow import ShadowState, build_shadow_event, build_shadow_trace


def test_h2o_shadow_can_represent_open_conditioning() -> None:
    event = build_shadow_event(
        {
            "route": "h2o",
            "source_action": "open_h2o_route_and_wait_ready",
            "source_trace_action": "wait_route_ready",
            "source_function": "DewpointAlignmentService.open_h2o_route_and_wait_ready",
            "vent_state_observed": "ON",
            "route_open": True,
        }
    )

    assert event["shadow_state"] == ShadowState.OPEN_CONDITIONING.value
    assert event["vent_state_observed"] == "ON"
    assert event["route_valve_state_observed"] == "open"


def test_h2o_shadow_can_represent_ambient_open_sampling() -> None:
    event = build_shadow_event(
        {
            "route": "h2o",
            "source_action": "pressure_skip ambient_open sample_start",
            "source_trace_action": "pressure_skip",
            "target": {"pressure_hpa": None, "vent_on": True},
            "is_ambient_pressure_point": True,
            "point_tag": "h2o_ambient_open",
            "sample_schema_version": "runtime",
        }
    )

    assert event["shadow_state"] == ShadowState.AMBIENT_OPEN_SAMPLING.value
    assert event["target_pressure_hpa"] is None
    assert event["vent_state_observed"] == "ON"
    assert event["sample_seen"] is True


def test_h2o_shadow_can_represent_ambient_to_sealed_transition_without_replacing_vent_calls() -> None:
    sources = [
        {
            "route": "h2o",
            "source_action": "stop_keepalive",
            "source_function": "H2oRouteRunner._stop_h2o_vent_keepalive",
            "keepalive_state": "stopped",
        },
        {
            "route": "h2o",
            "source_action": "bare_controller_vent_false",
            "source_function": "H2oRouteRunner.execute",
            "vent_state_observed": "OFF",
            "direct_vent_call_observed": True,
            "architecture_debt_observed": True,
        },
        {
            "route": "h2o",
            "source_action": "settle",
            "source_function": "H2oRouteRunner.execute",
            "settle_s": 1.5,
        },
        {
            "route": "h2o",
            "source_action": "gauge_read",
            "source_function": "H2oRouteRunner.execute",
            "pressure_after_settle_hpa": 1012.5,
        },
        {
            "route": "h2o",
            "source_action": "close_h2o_path",
            "source_function": "ValveRoutingService.set_h2o_path",
            "route_valve_state_observed": "closed",
        },
    ]
    trace = build_shadow_trace(sources)

    assert all(event["shadow_state"] == ShadowState.SEAL_TRANSITION.value for event in trace)
    assert all(event["behavior_changed"] is False for event in trace)
    assert any(event.get("direct_vent_call_observed") is True for event in trace)
    assert any(event.get("settle_s") == 1.5 for event in trace)
    assert any(event.get("pressure_after_settle_hpa") == 1012.5 for event in trace)


def test_h2o_bare_vent_is_recorded_as_architecture_debt_only() -> None:
    event = build_shadow_event(
        {
            "route": "h2o",
            "source_action": "bare_controller_vent_false",
            "source_function": "H2oRouteRunner.execute",
            "vent_state_observed": "OFF",
            "direct_vent_call_observed": True,
            "architecture_debt_observed": True,
        }
    )

    assert event["shadow_state"] == ShadowState.SEAL_TRANSITION.value
    assert event["direct_vent_call_observed"] is True
    assert event["architecture_debt_observed"] is True
    assert event["behavior_changed"] is False
    assert event["gate_applied"] is False


def test_h2o_shadow_can_represent_sealed_pressure_sweep() -> None:
    event = build_shadow_event(
        {
            "route": "h2o",
            "source_action": "set_pressure_to_target",
            "source_function": "PressureControlService.set_pressure_to_target",
            "target": {"pressure_hpa": 800.0},
            "actual": {"pressure_stable": True, "vent_on": False},
            "route_valve_state_observed": "sealed",
        }
    )

    assert event["shadow_state"] == ShadowState.SEALED_PRESSURE_CONTROL.value
    assert event["target_pressure_hpa"] == 800.0
    assert event["vent_state_observed"] == "OFF"


def test_h2o_shadow_can_record_dry_air_correction_evidence() -> None:
    event = build_shadow_event(
        {
            "route": "h2o",
            "source_action": "dry_air_correction",
            "source_function": "H2oRouteRunner.execute",
            "target": {"pressure_hpa": 800.0},
            "actual": {"vent_on": False},
            "dry_air_corrected_h2o_ppm": 1234.5,
        }
    )

    assert event["shadow_state"] == ShadowState.SEALED_PRESSURE_CONTROL.value
    assert event["dry_air_corrected_h2o_ppm"] == 1234.5
    assert event["behavior_changed"] is False
    assert event["gate_applied"] is False
