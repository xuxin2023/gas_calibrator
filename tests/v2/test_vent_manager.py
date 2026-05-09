from __future__ import annotations

from gas_calibrator.v2.core.route_state_shadow import ShadowState
from gas_calibrator.v2.core.vent_manager import VentManager, VentRoute


def _manager() -> VentManager:
    return VentManager()


def test_h2o_open_conditioning_allows_vent_on() -> None:
    result = _manager().assert_vent_allowed(VentRoute.H2O, ShadowState.OPEN_CONDITIONING, True, reason="route open")

    assert result.allowed is True
    assert result.route == "h2o"
    assert result.state == ShadowState.OPEN_CONDITIONING.value
    assert result.vent_on_requested is True
    assert result.hardware_command_sent is False


def test_h2o_ambient_sampling_allows_vent_on() -> None:
    result = _manager().assert_vent_allowed("h2o", ShadowState.AMBIENT_OPEN_SAMPLING, True, reason="ambient sample")

    assert result.allowed is True
    assert result.state == ShadowState.AMBIENT_OPEN_SAMPLING.value
    assert result.blocked_reason == ""


def test_h2o_seal_transition_allows_vent_off() -> None:
    result = _manager().assert_vent_allowed("h2o", ShadowState.SEAL_TRANSITION, False, reason="before sealed sweep")

    assert result.allowed is True
    assert result.vent_on_requested is False
    assert result.severity == "ok"


def test_h2o_sealed_pressure_control_blocks_vent_on() -> None:
    result = _manager().assert_vent_allowed("h2o", ShadowState.SEALED_PRESSURE_CONTROL, True, reason="sealed")

    assert result.allowed is False
    assert result.blocked_reason == "sealed_pressure_control_vent_on_blocked"
    assert result.severity == "blocked"


def test_co2_open_conditioning_allows_vent_on() -> None:
    result = _manager().assert_vent_allowed("co2", ShadowState.OPEN_CONDITIONING, True, reason="route soak")

    assert result.allowed is True
    assert result.route == "co2"
    assert result.vent_on_requested is True


def test_co2_explicit_ambient_block_requires_caller_enablement() -> None:
    default_result = _manager().assert_vent_allowed(
        "co2",
        ShadowState.AMBIENT_OPEN_SAMPLING,
        True,
        reason="co2 ambient without route policy",
    )
    disabled_result = _manager().assert_vent_allowed(
        "co2",
        ShadowState.AMBIENT_OPEN_SAMPLING,
        True,
        reason="co2 ambient explicitly disabled",
        ambient_block_enabled=False,
    )
    enabled_result = _manager().assert_vent_allowed(
        "co2",
        ShadowState.AMBIENT_OPEN_SAMPLING,
        True,
        reason="co2 ambient route policy enabled",
        ambient_block_enabled=True,
    )
    h2o_result = _manager().assert_vent_allowed(
        "h2o",
        ShadowState.AMBIENT_OPEN_SAMPLING,
        True,
        reason="h2o ambient remains scoped",
    )

    assert default_result.allowed is False
    assert default_result.blocked_reason == "co2_ambient_block_not_explicitly_enabled"
    assert disabled_result.allowed is False
    assert disabled_result.blocked_reason == "co2_ambient_block_not_explicitly_enabled"
    assert enabled_result.allowed is True
    assert enabled_result.blocked_reason == ""
    assert h2o_result.allowed is True


def test_co2_sealed_pressure_control_blocks_vent_on() -> None:
    result = _manager().assert_vent_allowed("co2", ShadowState.SEALED_PRESSURE_CONTROL, True, reason="sealed")

    assert result.allowed is False
    assert result.blocked_reason == "sealed_pressure_control_vent_on_blocked"
    assert result.hardware_command_sent is False


def test_h2o_keepalive_allowed_only_open_or_ambient() -> None:
    open_result = _manager().start_vent_keepalive("h2o", ShadowState.OPEN_CONDITIONING, 1.0, reason="legacy h2o")
    ambient_result = _manager().start_vent_keepalive("h2o", ShadowState.AMBIENT_OPEN_SAMPLING, 1.0, reason="ambient")
    co2_result = _manager().start_vent_keepalive("co2", ShadowState.OPEN_CONDITIONING, 2.0, reason="co2 conditioning")

    assert open_result.allowed is True
    assert ambient_result.allowed is True
    assert co2_result.allowed is False
    assert co2_result.blocked_reason == "keepalive_route_state_not_allowed"


def test_h2o_keepalive_blocked_in_sealed_pressure_control() -> None:
    result = _manager().start_vent_keepalive("h2o", ShadowState.SEALED_PRESSURE_CONTROL, 1.0, reason="sealed")

    assert result.allowed is False
    assert result.blocked_reason == "sealed_pressure_control_vent_on_blocked"
    assert result.keepalive_started is False
    assert result.thread_created is False


def test_h2o_direct_vent_close_is_route_and_state_scoped() -> None:
    h2o_result = _manager().request_vent("h2o", ShadowState.SEAL_TRANSITION, False, reason="direct close before seal")
    h2o_wrong_state = _manager().request_vent("h2o", ShadowState.OPEN_CONDITIONING, False, reason="wrong state")
    co2_result = _manager().request_vent("co2", ShadowState.SEAL_TRANSITION, False, reason="co2 direct close")

    assert h2o_result.allowed is True
    assert h2o_wrong_state.allowed is False
    assert co2_result.allowed is False
    assert co2_result.blocked_reason == "route_state_vent_off_not_allowed"


def test_h2o_exception_does_not_allow_co2_sealed_vent_on() -> None:
    h2o_open = _manager().assert_vent_allowed("h2o", ShadowState.OPEN_CONDITIONING, True, reason="h2o exception")
    co2_sealed = _manager().assert_vent_allowed("co2", ShadowState.SEALED_PRESSURE_CONTROL, True, reason="must stay sealed")

    assert h2o_open.allowed is True
    assert co2_sealed.allowed is False
    assert co2_sealed.blocked_reason == "sealed_pressure_control_vent_on_blocked"


def test_co2_sealed_pressure_control_blocks_h2o_keepalive_vent_on() -> None:
    result = _manager().request_vent(
        "co2",
        ShadowState.SEALED_PRESSURE_CONTROL,
        True,
        reason="h2o residual keepalive tick attempted after route switch",
        source="h2o-vent-keepalive residual",
    )
    payload = result.as_dict()

    assert result.allowed is False
    assert result.blocked_reason == "sealed_pressure_control_vent_on_blocked"
    assert payload["hardware_command_sent"] is False
    assert payload["behavior_changed"] is False
    assert payload["fail_closed_applied"] is False
    assert payload["not_real_acceptance_evidence"] is True


def test_request_vent_is_policy_only_and_sends_no_hardware_command() -> None:
    result = _manager().request_vent("h2o", ShadowState.OPEN_CONDITIONING, True, reason="policy only", source="test")
    payload = result.as_dict()

    assert result.allowed is True
    assert payload["hardware_command_sent"] is False
    assert payload["behavior_changed"] is False
    assert payload["gate_applied"] is False
    assert payload["fail_closed_applied"] is False
    assert payload["not_real_acceptance_evidence"] is True
    assert payload["source"] == "test"


def test_start_keepalive_is_policy_only_and_creates_no_thread() -> None:
    result = _manager().start_vent_keepalive("h2o", ShadowState.OPEN_CONDITIONING, 1.0, reason="policy only")

    assert result.allowed is True
    assert result.keepalive_requested is True
    assert result.keepalive_started is False
    assert result.thread_created is False
    assert result.hardware_command_sent is False


def test_stop_keepalive_is_policy_only_and_joins_no_thread() -> None:
    result = _manager().stop_vent_keepalive("h2o", ShadowState.SEAL_TRANSITION, reason="before vent off")

    assert result.allowed is True
    assert result.keepalive_requested is True
    assert result.keepalive_stopped is False
    assert result.thread_joined is False
    assert result.hardware_command_sent is False


def test_record_vent_observation_is_observation_only() -> None:
    result = _manager().record_vent_observation(
        "h2o",
        ShadowState.SEAL_TRANSITION,
        False,
        reason="bare controller.vent false observed",
        source="H2oRouteRunner.execute",
        architecture_debt_observed=True,
    )
    payload = result.as_dict()

    assert result.allowed is True
    assert payload["observation_only"] is True
    assert payload["observed_on"] is False
    assert payload["architecture_debt_observed"] is True
    assert payload["hardware_command_sent"] is False
    assert payload["behavior_changed"] is False
    assert payload["gate_applied"] is False
    assert payload["fail_closed_applied"] is False
    assert payload["not_real_acceptance_evidence"] is True


def test_keepalive_interval_can_express_legacy_1s_and_future_2s_policy() -> None:
    legacy = _manager().start_vent_keepalive("h2o", ShadowState.OPEN_CONDITIONING, 1.0, reason="legacy")
    future = _manager().start_vent_keepalive("h2o", ShadowState.OPEN_CONDITIONING, 2.0, reason="future common default")
    invalid = _manager().start_vent_keepalive("h2o", ShadowState.OPEN_CONDITIONING, 0.0, reason="invalid")

    assert legacy.allowed is True
    assert legacy.interval_s == VentManager.legacy_h2o_keepalive_interval_s
    assert future.allowed is True
    assert future.interval_s == VentManager.future_common_default_interval_s
    assert invalid.allowed is False
    assert invalid.blocked_reason == "invalid_keepalive_interval"


def test_invalid_or_unknown_route_state_is_not_silent_allowed() -> None:
    unknown_route = _manager().assert_vent_allowed("argon", ShadowState.OPEN_CONDITIONING, True, reason="unknown route")
    unknown_state = _manager().assert_vent_allowed("h2o", "NOT_A_STATE", True, reason="unknown state")
    unknown_off = _manager().assert_vent_allowed("unknown", ShadowState.UNKNOWN, False, reason="unknown off")

    assert unknown_route.allowed is False
    assert unknown_route.blocked_reason == "unknown_route"
    assert unknown_state.allowed is False
    assert unknown_state.blocked_reason == "unknown_state"
    assert unknown_off.allowed is False
    assert unknown_off.severity == "warning"
