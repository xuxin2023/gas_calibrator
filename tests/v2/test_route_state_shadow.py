from __future__ import annotations

from gas_calibrator.v2.core.route_state_shadow import CONTRACT_SHADOW_STATES, ShadowState, build_shadow_event, infer_shadow_state


def test_shadow_event_defaults_are_observation_only() -> None:
    event = build_shadow_event({"route": "co2", "source_action": "route_context_enter"})

    assert event["observation_only"] is True
    assert event["behavior_changed"] is False
    assert event["gate_applied"] is False
    assert event["fail_closed_applied"] is False
    assert event["not_real_acceptance_evidence"] is True


def test_unknown_state_is_recorded_not_failed() -> None:
    event = build_shadow_event({"route": "co2", "source_action": "unmapped_event"})

    assert event["shadow_state"] == ShadowState.UNKNOWN.value
    assert event["inference_confidence"] == "unknown"
    assert event["fail_closed_applied"] is False


def test_missing_vent_evidence_stays_unknown() -> None:
    event = build_shadow_event(
        {"route": "co2", "source_action": "set_pressure_to_target", "target": {"pressure_hpa": 1000.0}}
    )

    assert event["shadow_state"] == ShadowState.SEALED_PRESSURE_CONTROL.value
    assert event["vent_state_observed"] == "unknown"
    assert "vent_state_observed" in event["unknown_fields"]
    assert "vent_evidence" in event["missing_source"]


def test_missing_pressure_evidence_stays_unknown() -> None:
    event = build_shadow_event(
        {"route": "h2o", "source_action": "gauge_read_after_settle", "source_function": "H2oRouteRunner.execute"}
    )

    assert event["shadow_state"] == ShadowState.SEAL_TRANSITION.value
    assert event["actual_pressure_hpa"] is None
    assert "actual_pressure_hpa" in event["unknown_fields"]
    assert "actual_pressure_evidence" in event["missing_source"]


def test_shadow_event_never_sets_gate_or_fail_closed() -> None:
    event = build_shadow_event(
        {
            "route": "co2",
            "source_action": "unexpected_ambient_open_sampling",
            "gate_applied": True,
            "fail_closed_applied": True,
            "behavior_changed": True,
        }
    )

    assert event["observation_only"] is True
    assert event["behavior_changed"] is False
    assert event["gate_applied"] is False
    assert event["fail_closed_applied"] is False


def test_constant_states_match_architecture_contract() -> None:
    assert CONTRACT_SHADOW_STATES == (
        "BASELINE",
        "OPEN_CONDITIONING",
        "AMBIENT_OPEN_SAMPLING",
        "SEAL_TRANSITION",
        "SEALED_PRESSURE_CONTROL",
        "CLEANUP",
        "EMERGENCY_SAFE_STOP",
        "UNKNOWN",
    )


def test_ambient_pressure_is_not_zero_hpa_target() -> None:
    event = build_shadow_event(
        {
            "route": "h2o",
            "source_action": "pressure_skip",
            "target": {"pressure_hpa": None, "vent_on": True},
            "is_ambient_pressure_point": True,
            "point_tag": "h2o_ambient_open",
        }
    )

    assert event["shadow_state"] == ShadowState.AMBIENT_OPEN_SAMPLING.value
    assert event["target_pressure_hpa"] is None
    assert infer_shadow_state(event) == ShadowState.AMBIENT_OPEN_SAMPLING.value
