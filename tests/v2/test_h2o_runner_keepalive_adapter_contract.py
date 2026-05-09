from __future__ import annotations

import ast
import inspect
import sys
import textwrap
from types import SimpleNamespace

import pytest

from gas_calibrator.v2.core.h2o_vent_adapter import H2OVentAdapter
from gas_calibrator.v2.core.runners.h2o_route_runner import H2oRouteRunner


def _start_keepalive_source() -> str:
    return inspect.getsource(H2oRouteRunner._start_h2o_vent_keepalive)


def _execute_source() -> str:
    return inspect.getsource(H2oRouteRunner.execute)


def _assert_order(source: str, tokens: list[str]) -> None:
    position = -1
    for token in tokens:
        next_position = source.find(token, position + 1)
        assert next_position >= 0, f"missing token {token!r}"
        assert next_position > position, f"token {token!r} is out of order"
        position = next_position


class BlockingH2OVentAdapter(H2OVentAdapter):
    def request_vent(self, *args, **kwargs):
        return self.manager.request_vent(
            "h2o",
            "SEALED_PRESSURE_CONTROL",
            True,
            reason="forced blocked policy for D2.1 contract",
            source="BlockingH2OVentAdapter",
        )


class OneTickStop:
    def __init__(self) -> None:
        self.calls = 0

    def clear(self) -> None:
        return None

    def wait(self, interval_s: float) -> bool:
        self.calls += 1
        return self.calls > 1

    def set(self) -> None:
        return None


class FakeThread:
    def __init__(self, target, daemon: bool, name: str) -> None:
        self.target = target
        self.daemon = daemon
        self.name = name
        self.started = False

    def start(self) -> None:
        self.started = True
        self.target()

    def is_alive(self) -> bool:
        return False

    def join(self, timeout: float | None = None) -> None:
        return None


class RecordingController:
    def __init__(self) -> None:
        self.calls: list[bool] = []

    def vent(self, on: bool) -> None:
        self.calls.append(bool(on))


class RecordingStatusService:
    def __init__(self) -> None:
        self.logs: list[str] = []

    def log(self, message: str) -> None:
        self.logs.append(str(message))


def _runner_for_one_keepalive_tick(controller: RecordingController) -> tuple[H2oRouteRunner, RecordingStatusService]:
    status = RecordingStatusService()
    service = SimpleNamespace(
        status_service=status,
        device_manager=SimpleNamespace(get_device=lambda name: controller if name == "pressure_controller" else None),
    )
    runner = H2oRouteRunner(service, [], [])
    runner._vent_keepalive_stop = OneTickStop()
    return runner, status


def test_h2o_runner_keepalive_tick_still_contains_current_legacy_vent_true_path() -> None:
    source = _start_keepalive_source()

    assert "interval_s = 1.0" in source
    assert "self._vent_keepalive_stop.wait(interval_s)" in source
    assert "controller.vent(True)" not in source
    assert "H2OVentAdapter" in source
    assert ".request_vent(" in source


def test_h2o_runner_keepalive_requires_adapter_before_controller_vent_on() -> None:
    source = _start_keepalive_source()

    adapter_index = source.index("request_vent")
    adapter_init_index = source.index("H2OVentAdapter(vent_command=controller.vent)")

    assert adapter_init_index < adapter_index
    assert "route=\"h2o\"" in source or "route='h2o'" in source
    assert "state=ShadowState.OPEN_CONDITIONING" in source
    assert "on=True" in source
    assert "source=\"H2oRouteRunner._start_h2o_vent_keepalive\"" in source
    assert "controller.vent(True)" not in source


def test_h2o_runner_keepalive_policy_allowed_calls_controller_vent(monkeypatch: pytest.MonkeyPatch) -> None:
    controller = RecordingController()
    runner, status = _runner_for_one_keepalive_tick(controller)

    monkeypatch.setattr("gas_calibrator.v2.core.runners.h2o_route_runner.threading.Thread", FakeThread)

    runner._start_h2o_vent_keepalive()

    assert controller.calls == [True]
    assert any("adapter_policy result=ok" in message for message in status.logs)


def test_h2o_runner_keepalive_policy_blocked_does_not_call_controller_vent(monkeypatch: pytest.MonkeyPatch) -> None:
    source = _start_keepalive_source()
    controller = RecordingController()
    runner, status = _runner_for_one_keepalive_tick(controller)

    monkeypatch.setattr("gas_calibrator.v2.core.runners.h2o_route_runner.threading.Thread", FakeThread)
    monkeypatch.setattr("gas_calibrator.v2.core.runners.h2o_route_runner.H2OVentAdapter", BlockingH2OVentAdapter)

    runner._start_h2o_vent_keepalive()

    assert "blocked_reason" in source
    assert "hardware_command_sent" in source
    assert "controller.vent(True)" not in source
    assert "result.hardware_command_sent" in source
    assert "command_result = f\"blocked:{result.blocked_reason or 'policy_denied'}\"" in source
    assert "failure_count += 1" in source
    assert controller.calls == []
    assert any("blocked:sealed_pressure_control_vent_on_blocked" in message for message in status.logs)


def test_h2o_runner_keepalive_preserves_legacy_1s_interval() -> None:
    source = _start_keepalive_source()
    tree = ast.parse(textwrap.dedent(source))
    interval_assignments = [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Assign)
        and any(isinstance(target, ast.Name) and target.id == "interval_s" for target in node.targets)
    ]

    assert len(interval_assignments) == 1
    value = interval_assignments[0].value
    assert isinstance(value, ast.Constant)
    assert value.value == 1.0
    assert "interval_s = 2.0" not in source
    assert "future_common_default_interval_s" not in source


def test_h2o_runner_vent_off_path_not_modified_in_D2_scope() -> None:
    source = _execute_source()
    vent_off_index = source.index("controller.vent(False)")
    stop_index = source.index("self._stop_h2o_vent_keepalive()")
    settle_index = source.index("time.sleep(1.5)")

    assert stop_index < vent_off_index < settle_index
    assert "controller.vent(False)" in source
    assert "request_vent(\n                            \"h2o\"" not in source
    assert "request_vent(\"h2o\"" not in source
    assert "on=False" not in source


def test_h2o_runner_ambient_to_sealed_order_contract() -> None:
    source = _execute_source()

    _assert_order(
        source,
        [
            "self._stop_h2o_vent_keepalive()",
            "controller.vent(False)",
            "time.sleep(1.5)",
            "read_pressure",
            "self.service.valve_routing_service.set_h2o_path(False, lead)",
            "pressurize_and_hold(",
            "prefer_direct_vent_close=True",
        ],
    )


def test_h2o_runner_cleanup_stops_keepalive_contract() -> None:
    source = _execute_source()

    assert "except WorkflowInterruptedError" in source
    assert "finally:" in source
    finally_index = source.index("finally:")
    stop_index = source.index("self._stop_h2o_vent_keepalive()", finally_index)
    clear_index = source.index("route_context.clear()", finally_index)

    assert finally_index < stop_index < clear_index


def test_h2o_runner_keepalive_records_blocked_or_failure_evidence() -> None:
    source = _start_keepalive_source()

    assert "failure_count" in source
    assert "last_result" in source
    assert "heartbeat_count" in source
    assert "blocked:" in source
    assert "error:" in source


def test_co2_golden_guard_not_in_scope_for_h2o_d2() -> None:
    module_globals = set(globals())
    loaded_gas_modules = [name for name in sys.modules if name.startswith("gas_calibrator.v2.core.runners.co2")]

    assert "Co2RouteRunner" not in module_globals
    assert not loaded_gas_modules
