from __future__ import annotations

import inspect
import sys

from gas_calibrator.v2.core.h2o_vent_adapter import H2OVentAdapter
from gas_calibrator.v2.core.route_state_shadow import ShadowState
from gas_calibrator.v2.core.vent_manager import VentManager


class RecordingVentManager(VentManager):
    def __init__(self, calls: list[str]) -> None:
        super().__init__()
        self.calls = calls

    def request_vent(self, *args, **kwargs):
        self.calls.append("policy.request_vent")
        return super().request_vent(*args, **kwargs)

    def start_vent_keepalive(self, *args, **kwargs):
        self.calls.append("policy.start_keepalive")
        return super().start_vent_keepalive(*args, **kwargs)

    def stop_vent_keepalive(self, *args, **kwargs):
        self.calls.append("policy.stop_keepalive")
        return super().stop_vent_keepalive(*args, **kwargs)


def _event_index(events: list[dict], event: str) -> int:
    for index, payload in enumerate(events):
        if payload["event"] == event:
            return index
    raise AssertionError(f"missing event {event!r}")


def test_adapter_request_vent_calls_policy_before_fake_controller() -> None:
    calls: list[str] = []

    def fake_vent_command(on: bool) -> None:
        calls.append(f"fake_controller.vent:{on}")

    adapter = H2OVentAdapter(vent_command=fake_vent_command, manager=RecordingVentManager(calls))

    result = adapter.request_vent("h2o", ShadowState.SEAL_TRANSITION, False, reason="before seal")

    assert result.allowed is True
    assert result.hardware_command_sent is True
    assert calls == ["policy.request_vent", "fake_controller.vent:False"]


def test_adapter_does_not_call_controller_when_policy_blocked() -> None:
    vent_calls: list[bool] = []
    adapter = H2OVentAdapter(vent_command=lambda on: vent_calls.append(bool(on)))

    result = adapter.request_vent("h2o", ShadowState.SEALED_PRESSURE_CONTROL, True, reason="sealed")

    assert result.allowed is False
    assert result.blocked_reason == "sealed_pressure_control_vent_on_blocked"
    assert result.hardware_command_sent is False
    assert vent_calls == []


def test_adapter_start_keepalive_records_legacy_1s_interval_without_thread() -> None:
    adapter = H2OVentAdapter(vent_command=lambda on: None)

    result = adapter.start_keepalive("h2o", ShadowState.OPEN_CONDITIONING, 1.0, reason="legacy interval")

    assert result.allowed is True
    assert result.interval_s == 1.0
    assert result.keepalive_requested is True
    assert result.hardware_command_sent is False
    assert result.thread_created is False
    assert adapter.thread_created is False


def test_adapter_can_express_future_2s_interval_without_runtime_change() -> None:
    adapter = H2OVentAdapter(vent_command=lambda on: None)

    result = adapter.start_keepalive("h2o", ShadowState.OPEN_CONDITIONING, 2.0, reason="future interval")

    assert result.allowed is True
    assert result.interval_s == 2.0
    assert result.thread_created is False
    assert result.hardware_command_sent is False
    assert adapter.thread_created is False


def test_adapter_stop_keepalive_is_idempotent() -> None:
    adapter = H2OVentAdapter()

    first = adapter.stop_keepalive("h2o", ShadowState.SEAL_TRANSITION, reason="first stop")
    second = adapter.stop_keepalive("h2o", ShadowState.SEAL_TRANSITION, reason="second stop")

    assert first.allowed is True
    assert second.allowed is True
    assert first.already_stopped is True
    assert second.already_stopped is True
    assert first.stop_result == "already_stopped"
    assert second.stop_result == "already_stopped"
    assert adapter.stop_count == 2


def test_adapter_stop_keepalive_before_vent_off_sequence() -> None:
    adapter = H2OVentAdapter(vent_command=lambda on: None)

    adapter.start_keepalive("h2o", ShadowState.OPEN_CONDITIONING, 1.0, reason="open")
    stop = adapter.stop_keepalive("h2o", ShadowState.SEAL_TRANSITION, reason="before vent off")
    vent_off = adapter.request_vent("h2o", ShadowState.SEAL_TRANSITION, False, reason="seal")

    assert stop.allowed is True
    assert vent_off.allowed is True
    assert _event_index(adapter.events, "stop_keepalive") < _event_index(adapter.events, "vent_off")


def test_adapter_h2o_seal_transition_vent_off_allowed() -> None:
    vent_calls: list[bool] = []
    adapter = H2OVentAdapter(vent_command=lambda on: vent_calls.append(bool(on)))

    result = adapter.request_vent("h2o", ShadowState.SEAL_TRANSITION, False, reason="seal transition")

    assert result.allowed is True
    assert result.requested_on is False
    assert result.hardware_command_sent is True
    assert vent_calls == [False]


def test_adapter_h2o_sealed_pressure_blocks_vent_on() -> None:
    vent_calls: list[bool] = []
    adapter = H2OVentAdapter(vent_command=lambda on: vent_calls.append(bool(on)))

    result = adapter.request_vent("h2o", ShadowState.SEALED_PRESSURE_CONTROL, True, reason="sealed pressure")

    assert result.allowed is False
    assert result.blocked_reason == "sealed_pressure_control_vent_on_blocked"
    assert result.hardware_command_sent is False
    assert vent_calls == []


def test_adapter_co2_sealed_blocks_residual_h2o_keepalive() -> None:
    vent_calls: list[bool] = []
    adapter = H2OVentAdapter(vent_command=lambda on: vent_calls.append(bool(on)))

    result = adapter.request_vent(
        "co2",
        ShadowState.SEALED_PRESSURE_CONTROL,
        True,
        reason="h2o residual keepalive tick attempted after route switch",
        source="h2o-vent-keepalive residual",
    )

    assert result.route == "co2"
    assert result.allowed is False
    assert result.blocked_reason == "sealed_pressure_control_vent_on_blocked"
    assert result.hardware_command_sent is False
    assert vent_calls == []


def test_adapter_cleanup_does_not_emit_hardware_command() -> None:
    vent_calls: list[bool] = []
    adapter = H2OVentAdapter(vent_command=lambda on: vent_calls.append(bool(on)))

    first = adapter.cleanup(reason="first cleanup")
    second = adapter.cleanup(reason="second cleanup")

    assert first.allowed is True
    assert second.allowed is True
    assert first.event == "cleanup"
    assert second.event == "cleanup"
    assert first.hardware_command_sent is False
    assert second.hardware_command_sent is False
    assert second.already_stopped is True
    assert vent_calls == []


def test_adapter_results_are_not_real_acceptance_evidence() -> None:
    adapter = H2OVentAdapter(vent_command=lambda on: None)

    results = [
        adapter.start_keepalive("h2o", ShadowState.OPEN_CONDITIONING, 1.0, reason="start"),
        adapter.stop_keepalive("h2o", ShadowState.SEAL_TRANSITION, reason="stop"),
        adapter.request_vent("h2o", ShadowState.SEAL_TRANSITION, False, reason="vent off"),
        adapter.cleanup(reason="cleanup"),
    ]

    for result in results:
        payload = result.as_dict()
        assert payload["not_real_acceptance_evidence"] is True
        assert payload["behavior_changed"] is False
        assert payload["gate_applied"] is False
        assert payload["fail_closed_applied"] is False


def test_adapter_does_not_import_or_require_real_hardware() -> None:
    adapter = H2OVentAdapter()

    result = adapter.start_keepalive("h2o", ShadowState.OPEN_CONDITIONING, 1.0, reason="no hardware")
    source = inspect.getsource(sys.modules[H2OVentAdapter.__module__])

    assert result.allowed is True
    assert result.hardware_command_sent is False
    assert "device_manager" not in source
    assert "serial" not in source.lower()
    assert "COM" not in source
    assert adapter.vent_command is None
