from __future__ import annotations

import ast
import inspect
import sys
import textwrap

import pytest

from gas_calibrator.v2.core.runners.h2o_route_runner import H2oRouteRunner


D2_1_REASON = "D2.1 runtime adapter wiring not implemented"


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


def test_h2o_runner_keepalive_tick_still_contains_current_legacy_vent_true_path() -> None:
    source = _start_keepalive_source()

    assert "interval_s = 1.0" in source
    assert "self._vent_keepalive_stop.wait(interval_s)" in source
    assert "controller.vent(True)" in source
    assert "H2OVentAdapter" not in source
    assert ".request_vent(" not in source


@pytest.mark.xfail(strict=True, reason=D2_1_REASON)
def test_h2o_runner_keepalive_future_requires_adapter_before_controller_vent_on() -> None:
    source = _start_keepalive_source()

    adapter_index = source.index("request_vent")
    controller_index = source.index("controller.vent(True)")

    assert adapter_index < controller_index
    assert "route=\"h2o\"" in source or "route='h2o'" in source
    assert "on=True" in source


@pytest.mark.xfail(strict=True, reason=D2_1_REASON)
def test_h2o_runner_keepalive_policy_blocked_future_does_not_call_controller_vent() -> None:
    source = _start_keepalive_source()

    assert "blocked_reason" in source
    assert "hardware_command_sent" in source or "allowed" in source
    assert "controller.vent(True)" in source
    assert "if" in source and ".allowed" in source


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


@pytest.mark.xfail(strict=True, reason=D2_1_REASON)
def test_h2o_runner_keepalive_future_records_blocked_or_failure_evidence() -> None:
    source = _start_keepalive_source()

    assert "blocked_count" in source or "failure_count" in source or "_vent_keepalive_failure_count" in source
    assert "last_result" in source or "_vent_keepalive_last_result" in source
    assert "last_tick" in source or "_vent_keepalive_last_tick" in source


def test_co2_golden_guard_not_in_scope_for_h2o_d2() -> None:
    module_globals = set(globals())
    loaded_gas_modules = [name for name in sys.modules if name.startswith("gas_calibrator.v2.core.runners.co2")]

    assert "Co2RouteRunner" not in module_globals
    assert not loaded_gas_modules
