from __future__ import annotations

import inspect
import sys

import pytest

from gas_calibrator.v2.core.h2o_vent_adapter import H2OVentAdapter
from gas_calibrator.v2.core.route_state_shadow import ShadowState


class FakeH2ORunner:
    def __init__(self, keepalive_interval_s: float = 1.0) -> None:
        self.keepalive_interval_s = keepalive_interval_s
        self.events: list[str] = []
        self.vent_calls: list[bool] = []
        self.pressure_targets: list[float] = []
        self.adapter = H2OVentAdapter(vent_command=self._fake_vent_command)

    def run_ambient_to_sealed_sequence(self, fail_at: str | None = None) -> None:
        try:
            self.start_open_conditioning_keepalive()
            self.start_ambient_open_keepalive()
            self.ambient_open_sample(fail_at=fail_at)
            self.stop_keepalive_before_seal()
            self.request_vent_off_before_seal()
            self.sleep_1p5()
            self.read_pressure_gauge()
            self.close_h2o_path()
            self.sealed_sweep_begin(fail_at=fail_at)
        finally:
            self.cleanup()

    def start_open_conditioning_keepalive(self):
        result = self.adapter.start_keepalive(
            "h2o",
            ShadowState.OPEN_CONDITIONING,
            self.keepalive_interval_s,
            reason="open conditioning",
            source="FakeH2ORunner.open_conditioning",
        )
        self.events.append("start_keepalive(open_conditioning)")
        return result

    def start_ambient_open_keepalive(self):
        result = self.adapter.start_keepalive(
            "h2o",
            ShadowState.AMBIENT_OPEN_SAMPLING,
            self.keepalive_interval_s,
            reason="ambient open sampling",
            source="FakeH2ORunner.ambient_open",
        )
        self.events.append("start_keepalive(ambient_open)")
        return result

    def ambient_open_sample(self, fail_at: str | None = None) -> None:
        self.events.append("ambient_open_sample")
        if fail_at == "ambient_sample":
            raise RuntimeError("ambient sample failed")

    def stop_keepalive_before_seal(self):
        result = self.adapter.stop_keepalive(
            "h2o",
            ShadowState.SEAL_TRANSITION,
            reason="before vent off",
            source="FakeH2ORunner.stop_before_seal",
        )
        self.events.append("stop_keepalive")
        return result

    def request_vent_off_before_seal(self):
        result = self.adapter.request_vent(
            "h2o",
            ShadowState.SEAL_TRANSITION,
            False,
            reason="seal transition vent off",
            source="FakeH2ORunner.vent_off_before_seal",
        )
        self.events.append("vent_off")
        return result

    def sleep_1p5(self) -> None:
        self.events.append("sleep_1p5")

    def read_pressure_gauge(self) -> float:
        self.events.append("read_pressure_gauge")
        return 1013.25

    def close_h2o_path(self) -> None:
        self.events.append("close_h2o_path")

    def sealed_sweep_begin(self, fail_at: str | None = None) -> None:
        self.events.append("sealed_sweep_begin")
        if fail_at == "sealed_sweep":
            raise RuntimeError("sealed sweep failed")

    def request_blocked_sealed_vent_on(self, route: str = "h2o"):
        return self.adapter.request_vent(
            route,
            ShadowState.SEALED_PRESSURE_CONTROL,
            True,
            reason="h2o residual keepalive tick attempted during sealed pressure control",
            source="h2o residual keepalive fake runner",
        )

    def set_pressure_to_target(self, pressure_hpa: float) -> None:
        self.pressure_targets.append(float(pressure_hpa))
        self.events.append(f"set_pressure_to_target:{float(pressure_hpa)}")

    def cleanup(self):
        result = self.adapter.cleanup(reason="fake runner finally cleanup", source="FakeH2ORunner.cleanup")
        self.events.append("cleanup_stop_keepalive")
        return result

    def _fake_vent_command(self, on: bool) -> None:
        self.vent_calls.append(bool(on))
        self.events.append(f"fake_controller.vent:{bool(on)}")


def _event_index(events: list[str], event: str) -> int:
    for index, item in enumerate(events):
        if item == event:
            return index
    raise AssertionError(f"missing event {event!r}")


def test_fake_runner_replays_h2o_ambient_to_sealed_sequence() -> None:
    runner = FakeH2ORunner()

    runner.run_ambient_to_sealed_sequence()

    expected = [
        "start_keepalive(open_conditioning)",
        "start_keepalive(ambient_open)",
        "ambient_open_sample",
        "stop_keepalive",
        "fake_controller.vent:False",
        "vent_off",
        "sleep_1p5",
        "read_pressure_gauge",
        "close_h2o_path",
        "sealed_sweep_begin",
        "cleanup_stop_keepalive",
    ]
    assert runner.events == expected


def test_fake_runner_stop_keepalive_before_vent_off() -> None:
    runner = FakeH2ORunner()

    runner.run_ambient_to_sealed_sequence()

    assert _event_index(runner.events, "stop_keepalive") < _event_index(runner.events, "vent_off")


def test_fake_runner_vent_off_before_close_h2o_path() -> None:
    runner = FakeH2ORunner()

    runner.run_ambient_to_sealed_sequence()

    assert _event_index(runner.events, "vent_off") < _event_index(runner.events, "close_h2o_path")


def test_fake_runner_ambient_open_is_not_zero_hpa_target() -> None:
    runner = FakeH2ORunner()

    runner.run_ambient_to_sealed_sequence()

    assert 0.0 not in runner.pressure_targets
    assert not any(event == "set_pressure_to_target:0.0" for event in runner.events)
    assert "ambient_open_sample" in runner.events


def test_fake_runner_keeps_vent_on_during_open_and_ambient() -> None:
    runner = FakeH2ORunner()

    open_result = runner.start_open_conditioning_keepalive()
    ambient_result = runner.start_ambient_open_keepalive()

    assert open_result.allowed is True
    assert open_result.route == "h2o"
    assert open_result.state == ShadowState.OPEN_CONDITIONING.value
    assert ambient_result.allowed is True
    assert ambient_result.route == "h2o"
    assert ambient_result.state == ShadowState.AMBIENT_OPEN_SAMPLING.value


def test_fake_runner_blocks_vent_on_in_sealed_pressure_control() -> None:
    runner = FakeH2ORunner()

    result = runner.request_blocked_sealed_vent_on("h2o")

    assert result.allowed is False
    assert result.blocked_reason == "sealed_pressure_control_vent_on_blocked"
    assert result.hardware_command_sent is False
    assert runner.vent_calls == []


def test_fake_runner_cleanup_is_idempotent_and_stops_keepalive() -> None:
    runner = FakeH2ORunner()

    runner.start_open_conditioning_keepalive()
    first = runner.cleanup()
    second = runner.cleanup()

    assert first.keepalive_stopped is True
    assert second.keepalive_stopped is True
    assert first.stop_result == "stopped"
    assert second.stop_result == "already_stopped"
    assert runner.adapter.keepalive_active is False


@pytest.mark.parametrize("fail_at", ["ambient_sample", "sealed_sweep"])
def test_fake_runner_cleanup_runs_after_failure(fail_at: str) -> None:
    runner = FakeH2ORunner()

    with pytest.raises(RuntimeError):
        runner.run_ambient_to_sealed_sequence(fail_at=fail_at)

    assert runner.events[-1] == "cleanup_stop_keepalive"
    assert runner.adapter.keepalive_active is False
    assert runner.adapter.events[-1]["event"] == "cleanup"


def test_fake_runner_preserves_legacy_1s_keepalive_interval() -> None:
    runner = FakeH2ORunner(keepalive_interval_s=1.0)

    open_result = runner.start_open_conditioning_keepalive()
    ambient_result = runner.start_ambient_open_keepalive()

    assert open_result.interval_s == 1.0
    assert ambient_result.interval_s == 1.0
    assert open_result.thread_created is False
    assert ambient_result.thread_created is False


def test_fake_runner_can_express_future_2s_policy_without_runtime_change() -> None:
    runner = FakeH2ORunner(keepalive_interval_s=2.0)

    runner.run_ambient_to_sealed_sequence()

    start_events = [event for event in runner.adapter.events if event["event"] == "start_keepalive"]
    assert [event["interval_s"] for event in start_events] == [2.0, 2.0]
    assert runner.events[:2] == ["start_keepalive(open_conditioning)", "start_keepalive(ambient_open)"]
    assert not any(event["thread_created"] for event in start_events)


def test_fake_runner_residual_h2o_keepalive_cannot_pollute_co2_sealed() -> None:
    runner = FakeH2ORunner()

    result = runner.request_blocked_sealed_vent_on("co2")

    assert result.route == "co2"
    assert result.allowed is False
    assert result.blocked_reason == "sealed_pressure_control_vent_on_blocked"
    assert result.hardware_command_sent is False
    assert runner.vent_calls == []


def test_fake_runner_no_real_hardware_or_thread_created() -> None:
    runner = FakeH2ORunner()

    runner.run_ambient_to_sealed_sequence()
    source = inspect.getsource(sys.modules[__name__])
    forbidden_tokens = ["device" + "_manager", "ser" + "ial", "C" + "OM", "thread" + "ing"]

    assert runner.adapter.thread_created is False
    assert all(event["thread_created"] is False for event in runner.adapter.events)
    assert all(token not in source for token in forbidden_tokens)
