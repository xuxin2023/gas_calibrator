from __future__ import annotations

from gas_calibrator.v2.core.route_state_shadow import ShadowState
from gas_calibrator.v2.core.vent_manager import VentManager, VentPolicyResult


class FakeH2OVentAdapter:
    def __init__(self, manager: VentManager | None = None) -> None:
        self.manager = manager or VentManager()
        self.events: list[dict] = []
        self.controller_vent_calls: list[bool] = []
        self.thread_create_count = 0
        self.hardware_command_count = 0

    def start_keepalive(
        self,
        route: str,
        state: ShadowState,
        interval_s: float,
        reason: str,
        source: str = "FakeH2OVentAdapter.start_keepalive",
    ) -> VentPolicyResult:
        result = self.manager.start_vent_keepalive(route, state, interval_s, reason=reason, source=source)
        self._record("start_keepalive", result)
        return result

    def stop_keepalive(
        self,
        route: str,
        state: ShadowState,
        reason: str,
        source: str = "FakeH2OVentAdapter.stop_keepalive",
    ) -> VentPolicyResult:
        result = self.manager.stop_vent_keepalive(route, state, reason=reason, source=source)
        self._record("stop_keepalive", result)
        return result

    def request_vent(
        self,
        route: str,
        state: ShadowState,
        on: bool,
        reason: str,
        source: str = "FakeH2OVentAdapter.request_vent",
    ) -> VentPolicyResult:
        result = self.manager.request_vent(route, state, on, reason=reason, source=source)
        self._record("vent_on" if on else "vent_off", result)
        return result

    def _record(self, event: str, result: VentPolicyResult) -> None:
        payload = result.as_dict()
        payload["event"] = event
        self.events.append(payload)
        if payload["hardware_command_sent"]:
            self.hardware_command_count += 1
        if payload["thread_created"]:
            self.thread_create_count += 1


def _event_index(events: list[dict], event: str) -> int:
    for index, payload in enumerate(events):
        if payload["event"] == event:
            return index
    raise AssertionError(f"missing event {event!r}")


def test_fake_h2o_open_conditioning_keepalive_uses_vent_manager() -> None:
    adapter = FakeH2OVentAdapter()

    result = adapter.start_keepalive("h2o", ShadowState.OPEN_CONDITIONING, 1.0, reason="open conditioning")

    assert result.route == "h2o"
    assert result.state == ShadowState.OPEN_CONDITIONING.value
    assert result.interval_s == 1.0
    assert result.allowed is True
    assert result.hardware_command_sent is False
    assert result.thread_created is False
    assert adapter.thread_create_count == 0
    assert adapter.hardware_command_count == 0


def test_fake_h2o_ambient_open_keepalive_uses_vent_manager() -> None:
    adapter = FakeH2OVentAdapter()

    result = adapter.start_keepalive("h2o", ShadowState.AMBIENT_OPEN_SAMPLING, 1.0, reason="ambient open")

    assert result.route == "h2o"
    assert result.state == ShadowState.AMBIENT_OPEN_SAMPLING.value
    assert result.vent_on_requested is True
    assert result.keepalive_requested is True
    assert result.allowed is True
    assert adapter.controller_vent_calls == []
    assert adapter.hardware_command_count == 0


def test_fake_h2o_seal_transition_order_stop_keepalive_before_vent_off() -> None:
    adapter = FakeH2OVentAdapter()

    adapter.start_keepalive("h2o", ShadowState.OPEN_CONDITIONING, 1.0, reason="open conditioning")
    stop = adapter.stop_keepalive("h2o", ShadowState.SEAL_TRANSITION, reason="before seal")
    vent_off = adapter.request_vent("h2o", ShadowState.SEAL_TRANSITION, False, reason="direct close before seal")

    assert stop.allowed is True
    assert vent_off.allowed is True
    assert _event_index(adapter.events, "stop_keepalive") < _event_index(adapter.events, "vent_off")
    assert adapter.events[_event_index(adapter.events, "vent_off")]["vent_on_requested"] is False


def test_fake_h2o_sealed_pressure_control_blocks_keepalive() -> None:
    adapter = FakeH2OVentAdapter()

    result = adapter.start_keepalive("h2o", ShadowState.SEALED_PRESSURE_CONTROL, 1.0, reason="sealed sweep")

    assert result.route == "h2o"
    assert result.state == ShadowState.SEALED_PRESSURE_CONTROL.value
    assert result.allowed is False
    assert result.blocked_reason == "sealed_pressure_control_vent_on_blocked"
    assert result.hardware_command_sent is False
    assert result.thread_created is False


def test_fake_h2o_residual_keepalive_cannot_pollute_co2_sealed() -> None:
    adapter = FakeH2OVentAdapter()

    result = adapter.request_vent(
        "co2",
        ShadowState.SEALED_PRESSURE_CONTROL,
        True,
        reason="h2o residual keepalive tick after switch to co2 sealed",
        source="h2o residual keepalive fake replay",
    )

    assert result.route == "co2"
    assert result.state == ShadowState.SEALED_PRESSURE_CONTROL.value
    assert result.allowed is False
    assert result.blocked_reason == "sealed_pressure_control_vent_on_blocked"
    assert result.hardware_command_sent is False


def test_fake_h2o_adapter_does_not_create_threads_or_hardware_commands() -> None:
    adapter = FakeH2OVentAdapter()

    adapter.start_keepalive("h2o", ShadowState.OPEN_CONDITIONING, 1.0, reason="open conditioning")
    adapter.start_keepalive("h2o", ShadowState.AMBIENT_OPEN_SAMPLING, 1.0, reason="ambient open")
    adapter.stop_keepalive("h2o", ShadowState.SEAL_TRANSITION, reason="before seal")
    adapter.request_vent("h2o", ShadowState.SEAL_TRANSITION, False, reason="direct close before seal")
    adapter.start_keepalive("h2o", ShadowState.SEALED_PRESSURE_CONTROL, 1.0, reason="blocked sealed replay")

    assert adapter.thread_create_count == 0
    assert adapter.hardware_command_count == 0
    assert adapter.controller_vent_calls == []
    for payload in adapter.events:
        assert payload["hardware_command_sent"] is False
        assert payload["behavior_changed"] is False
        assert payload["fail_closed_applied"] is False
        assert payload["not_real_acceptance_evidence"] is True


def test_fake_h2o_interval_is_passed_by_caller_not_defaulted() -> None:
    adapter = FakeH2OVentAdapter()

    legacy = adapter.start_keepalive("h2o", ShadowState.OPEN_CONDITIONING, 1.0, reason="caller selected legacy")
    future = adapter.start_keepalive("h2o", ShadowState.OPEN_CONDITIONING, 2.0, reason="caller selected future")
    invalid = adapter.start_keepalive("h2o", ShadowState.OPEN_CONDITIONING, 0.0, reason="caller invalid")

    assert legacy.allowed is True
    assert legacy.interval_s == 1.0
    assert future.allowed is True
    assert future.interval_s == 2.0
    assert invalid.allowed is False
    assert invalid.blocked_reason == "invalid_keepalive_interval"
    assert [payload["interval_s"] for payload in adapter.events] == [1.0, 2.0, None]


def test_fake_h2o_adapter_can_replay_current_ambient_to_sealed_sequence() -> None:
    adapter = FakeH2OVentAdapter()

    open_keepalive = adapter.start_keepalive("h2o", ShadowState.OPEN_CONDITIONING, 1.0, reason="open conditioning")
    ambient_keepalive = adapter.start_keepalive("h2o", ShadowState.AMBIENT_OPEN_SAMPLING, 1.0, reason="ambient sample")
    stop = adapter.stop_keepalive("h2o", ShadowState.SEAL_TRANSITION, reason="transition to sealed")
    vent_off = adapter.request_vent("h2o", ShadowState.SEAL_TRANSITION, False, reason="seal before pressure control")
    sealed_block = adapter.start_keepalive("h2o", ShadowState.SEALED_PRESSURE_CONTROL, 1.0, reason="sealed sweep begins")

    assert open_keepalive.allowed is True
    assert ambient_keepalive.allowed is True
    assert stop.allowed is True
    assert vent_off.allowed is True
    assert sealed_block.allowed is False
    assert [payload["state"] for payload in adapter.events] == [
        ShadowState.OPEN_CONDITIONING.value,
        ShadowState.AMBIENT_OPEN_SAMPLING.value,
        ShadowState.SEAL_TRANSITION.value,
        ShadowState.SEAL_TRANSITION.value,
        ShadowState.SEALED_PRESSURE_CONTROL.value,
    ]
    assert [payload["event"] for payload in adapter.events] == [
        "start_keepalive",
        "start_keepalive",
        "stop_keepalive",
        "vent_off",
        "start_keepalive",
    ]
    assert _event_index(adapter.events, "stop_keepalive") < _event_index(adapter.events, "vent_off")
    assert sealed_block.blocked_reason == "sealed_pressure_control_vent_on_blocked"
