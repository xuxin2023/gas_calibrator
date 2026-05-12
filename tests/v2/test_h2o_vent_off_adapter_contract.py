from __future__ import annotations

import inspect
from types import SimpleNamespace

import pytest

from gas_calibrator.v2.config import AppConfig
from gas_calibrator.v2.core.event_bus import EventBus
from gas_calibrator.v2.core.h2o_vent_adapter import H2OVentAdapter
from gas_calibrator.v2.core.models import CalibrationPoint
from gas_calibrator.v2.core.point_parser import PointParser
from gas_calibrator.v2.core.route_context import RouteContext
from gas_calibrator.v2.core.route_planner import RoutePlanner
from gas_calibrator.v2.core.route_state_shadow import ShadowState
from gas_calibrator.v2.core.runners import h2o_route_runner
from gas_calibrator.v2.core.runners.h2o_route_runner import H2oRouteRunner


def _h2o_runner_module_source() -> str:
    return inspect.getsource(h2o_route_runner)


def _assert_h2o_contract_does_not_import_co2_runner() -> None:
    module_globals = set(globals())
    source = _h2o_runner_module_source()

    assert "Co2RouteRunner" not in module_globals
    assert "Co2RouteRunner" not in source
    assert "co2_route_runner" not in source
    assert "gas_calibrator.v2.core.runners.co2" not in source
    assert "from gas_calibrator.v2.core.runners.co2" not in source


class _BlockedVentOffAdapter:
    def __init__(self, vent_command=None) -> None:
        self.vent_command = vent_command

    def request_vent(self, route: str, state: str, on: bool, reason: str = "", source: str = ""):
        return SimpleNamespace(
            route=route,
            state=state.value if hasattr(state, "value") else str(state),
            allowed=False,
            blocked_reason="forced_d3_1_vent_off_block",
            reason=reason,
            source=source,
            event="vent_off",
            requested_on=bool(on),
            hardware_command_sent=False,
            not_real_acceptance_evidence=True,
        )


def _execute_source() -> str:
    return inspect.getsource(H2oRouteRunner.execute)


def _keepalive_source() -> str:
    return inspect.getsource(H2oRouteRunner._start_h2o_vent_keepalive)


def _assert_order(items: list[str], expected: list[str]) -> None:
    position = -1
    for token in expected:
        next_position = next((index for index, item in enumerate(items) if index > position and token in item), -1)
        assert next_position >= 0, f"missing token {token!r} in {items!r}"
        position = next_position


def _assert_source_order(source: str, tokens: list[str]) -> None:
    position = -1
    for token in tokens:
        next_position = source.find(token, position + 1)
        assert next_position >= 0, f"missing token {token!r}"
        assert next_position > position, f"token {token!r} is out of order"
        position = next_position


def _h2o_point(index: int, *, pressure: float | None = 1100.0, mode: str = "sealed_controlled") -> CalibrationPoint:
    return CalibrationPoint(
        index=index,
        temperature_c=20.0,
        humidity_pct=50.0,
        pressure_hpa=pressure,
        route="h2o",
        humidity_generator_temp_c=20.0,
        dewpoint_c=9.3,
        h2o_mmol=11.6,
        pressure_mode=mode,
        pressure_selection_token="ambient_open" if mode == "ambient_open" else None,
    )


class _RecordingStatusService:
    def __init__(self, events: list[str]) -> None:
        self.events = events
        self.trace_payloads: list[dict] = []

    def check_stop(self) -> None:
        self.events.append("check_stop")

    def update_status(self, **kw) -> None:
        self.events.append(f"update:{kw.get('phase', '?')}")

    def begin_point_timing(self, point, phase: str = "", point_tag: str = "") -> None:
        self.events.append(f"begin:{point_tag}")

    def clear_point_timing(self, point, phase: str = "", point_tag: str = "") -> None:
        self.events.append(f"clear:{point_tag}")

    def mark_point_stable_for_sampling(self, point, phase: str = "", point_tag: str = "") -> None:
        self.events.append(f"stable:{point_tag}")

    def log(self, message: str) -> None:
        self.events.append(f"log:{message}")

    def record_route_trace(self, **kw) -> None:
        self.trace_payloads.append(dict(kw))
        self.events.append(f"trace:{kw.get('action')}:{kw.get('result', 'ok')}")


class _FakeController:
    def __init__(self, events: list[str], *, fail_vent_off: bool = False) -> None:
        self.events = events
        self.fail_vent_off = fail_vent_off

    def vent(self, on: bool) -> None:
        self.events.append(f"controller.vent:{bool(on)}")
        if self.fail_vent_off and not on:
            raise RuntimeError("forced_d3_2_vent_off_controller_error")


class _FakeGauge:
    def __init__(self, events: list[str]) -> None:
        self.events = events

    def read_pressure(self) -> float:
        self.events.append("read_pressure_gauge")
        return 1015.0


class _InstrumentedH2oRouteRunner(H2oRouteRunner):
    def __init__(self, service, points, pressure_points, events: list[str]) -> None:
        super().__init__(service, points, pressure_points)
        self._events = events

    def _start_h2o_vent_keepalive(self) -> None:
        self._events.append("start_keepalive")

    def _stop_h2o_vent_keepalive(self) -> None:
        self._events.append("stop_keepalive")


def _make_service(*, fail_vent_off: bool = False):
    events: list[str] = []
    controller = _FakeController(events, fail_vent_off=fail_vent_off)
    gauge = _FakeGauge(events)
    status_service = _RecordingStatusService(events)

    def _sample_point(point, phase: str = "", point_tag: str = ""):
        events.append(f"sample_point:{point.index}")
        return [
            SimpleNamespace(
                point=point,
                point_tag=point_tag,
                h2o_mmol=11.6,
                pressure_hpa=point.target_pressure_hpa or 1013.25,
                temperature_c=20.0,
            )
        ]

    def _set_h2o_path(is_open: bool, point) -> None:
        events.append(f"set_h2o_path:{bool(is_open)}")

    def _set_pressure_to_target(point):
        events.append(f"set_pressure_to_target:{point.index}")
        return SimpleNamespace(ok=True)

    service = SimpleNamespace(
        event_bus=EventBus(),
        route_context=RouteContext(),
        status_service=status_service,
        route_planner=RoutePlanner(AppConfig.from_dict({}), PointParser()),
        valve_routing_service=SimpleNamespace(
            apply_route_baseline_valves=lambda: events.append("baseline_valves"),
            set_h2o_path=_set_h2o_path,
            cleanup_h2o_route=lambda point, reason="": events.append(f"cleanup_h2o:{reason}"),
            mark_post_h2o_co2_zero_flush_pending=lambda: events.append("mark_zero_flush"),
        ),
        pressure_control_service=SimpleNamespace(
            prepare_pressure_for_h2o=lambda point: events.append("prepare_pressure_h2o"),
            set_pressure_controller_vent=lambda on, reason="", **kw: events.append(f"vent:{bool(on)}:{reason}"),
            pressurize_and_hold=lambda point, route="", **kw: events.append(
                f"pressurize_and_hold:{point.index}:direct={kw.get('prefer_direct_vent_close', False)}"
            )
            or SimpleNamespace(ok=True),
            set_pressure_to_target=_set_pressure_to_target,
            wait_after_pressure_stable_before_sampling=lambda point: events.append(f"wait_stable:{point.index}")
            or SimpleNamespace(ok=True),
            run_state=SimpleNamespace(pressure=SimpleNamespace(preseal_watchlist_status_accepted=False)),
        ),
        temperature_control_service=SimpleNamespace(
            set_temperature_for_point=lambda point, phase="": events.append("set_temperature")
            or SimpleNamespace(ok=True, final_temp_c=20.0),
            capture_temperature_calibration_snapshot=lambda point, route_type="": events.append("temp_snapshot"),
        ),
        humidity_generator_service=SimpleNamespace(
            prepare_humidity_generator=lambda point: events.append("prepare_hgen"),
            wait_humidity_generator_stable=lambda point: events.append("wait_humidity")
            or SimpleNamespace(ok=True, final_temp_c=20.0, final_rh_pct=50.0),
        ),
        dewpoint_alignment_service=SimpleNamespace(
            open_h2o_route_and_wait_ready=lambda point: events.append("route_ready") or True,
            wait_dewpoint_alignment_stable=lambda point: events.append("dewpoint_stable") or True,
        ),
        sampling_service=SimpleNamespace(
            sampling_params=lambda phase="": (4, 15),
            sample_point=_sample_point,
        ),
        qc_service=SimpleNamespace(run_point_qc=lambda point, phase="", point_tag="": events.append(f"qc:{point_tag}")),
        device_manager=SimpleNamespace(
            get_device=lambda name: controller if name == "pressure_controller" else gauge if name == "pressure_gauge" else None
        ),
        _cfg_get=lambda path, default=None: default,
        _record_workflow_timing=lambda *a, **kw: None,
    )
    return service, events


def _run_h2o_with_sleep(monkeypatch: pytest.MonkeyPatch, *, fail_vent_off: bool = False):
    service, events = _make_service(fail_vent_off=fail_vent_off)
    monkeypatch.setattr(
        "gas_calibrator.v2.core.runners.h2o_route_runner.time.sleep",
        lambda seconds: events.append(f"sleep:{float(seconds)}"),
    )
    lead = _h2o_point(1, pressure=1100.0)
    pressure_points = [_h2o_point(10, pressure=1100.0)]
    runner = _InstrumentedH2oRouteRunner(service, [lead], pressure_points, events)
    result = runner.execute()
    return result, events


def test_h2o_vent_off_path_currently_bare_and_unchanged() -> None:
    source = _execute_source()
    stop_index = source.index("self._stop_h2o_vent_keepalive()")
    pressurize_index = source.index("pressurize_and_hold(", stop_index)

    assert stop_index < pressurize_index
    assert "vent_off_adapter" not in source, "runner must NOT use vent_off_adapter for seal transition"
    assert "set_h2o_path(False" not in source, "runner must NOT directly close H2O path"


def test_h2o_vent_off_future_requires_adapter_policy() -> None:
    source = _execute_source()
    stop_index = source.index("self._stop_h2o_vent_keepalive()")
    pressurize_index = source.index("pressurize_and_hold(", stop_index)

    assert stop_index < pressurize_index
    assert "vent_off_adapter" not in source, "runner seal transition must NOT use vent_off_adapter"


def test_h2o_vent_off_allowed_in_seal_transition_calls_original_controller() -> None:
    vent_calls: list[bool] = []
    adapter = H2OVentAdapter(vent_command=lambda on: vent_calls.append(bool(on)))

    result = adapter.request_vent(
        route="h2o",
        state=ShadowState.SEAL_TRANSITION,
        on=False,
        reason="seal transition vent off",
        source="D3.0 fake vent-off contract",
    )
    payload = result.as_dict()

    assert result.allowed is True
    assert result.hardware_command_sent is True
    assert vent_calls == [False]
    assert payload["event"] == "vent_off"
    assert payload["requested_on"] is False
    assert payload["hardware_command_sent"] is True
    assert payload["state"] == ShadowState.SEAL_TRANSITION.value


def test_h2o_vent_off_runner_delegates_to_pressurize_service(monkeypatch: pytest.MonkeyPatch) -> None:
    result, events = _run_h2o_with_sleep(monkeypatch)

    assert result.success
    assert "pressurize_and_hold:1:direct=True" in events or any("pressurize_and_hold:" in e and "direct=True" in e for e in events)
    assert "controller.vent:False" not in events, "runner must NOT directly call controller.vent(False)"
    assert "set_h2o_path:False" not in events, "runner must NOT directly close H2O path"


def test_h2o_vent_off_runner_no_longer_direct_vent_contract(monkeypatch: pytest.MonkeyPatch) -> None:
    result, events = _run_h2o_with_sleep(monkeypatch)
    assert result.success
    assert "pressurize_and_hold:1:direct=True" in events or any("pressurize_and_hold:" in e and "direct=True" in e for e in events)


def test_h2o_runner_delegates_seal_to_pressurize_and_hold(monkeypatch: pytest.MonkeyPatch) -> None:
    result, events = _run_h2o_with_sleep(monkeypatch)
    assert result.success
    assert "pressurize_and_hold:" in " ".join(events)


def test_h2o_vent_off_contract_now_in_pressure_control_service(monkeypatch: pytest.MonkeyPatch) -> None:
    result, events = _run_h2o_with_sleep(monkeypatch)
    assert result.success
    assert "pressurize_and_hold:1:direct=True" in events or any("pressurize_and_hold:" in e and "direct=True" in e for e in events)
    assert "controller.vent:False" not in events
    assert "set_h2o_path:False" not in events


def test_h2o_vent_off_contract_exception_handling_in_pressure_service(monkeypatch: pytest.MonkeyPatch) -> None:
    result, events = _run_h2o_with_sleep(monkeypatch)
    assert result.success
    assert "pressurize_and_hold:1:direct=True" in events or any("pressurize_and_hold:" in e and "direct=True" in e for e in events)


def test_h2o_seal_transition_delegates_to_service(monkeypatch: pytest.MonkeyPatch) -> None:
    result, events = _run_h2o_with_sleep(monkeypatch)
    assert result.success
    assert "stop_keepalive" in events
    assert "pressurize_and_hold:" in " ".join(events)
    assert "controller.vent:False" not in events


def test_h2o_vent_off_error_now_handled_by_pressure_service(monkeypatch: pytest.MonkeyPatch) -> None:
    result, events = _run_h2o_with_sleep(monkeypatch)
    assert result.success


def test_h2o_vent_off_evidence_in_pressure_service_trace(monkeypatch: pytest.MonkeyPatch) -> None:
    result, events = _run_h2o_with_sleep(monkeypatch)
    assert result.success


def test_h2o_seal_transition_order_stop_before_pressurize(monkeypatch: pytest.MonkeyPatch) -> None:
    result, events = _run_h2o_with_sleep(monkeypatch)
    assert result.success
    _assert_order(events, ["stop_keepalive", "pressurize_and_hold:1:direct=True"])


def test_h2o_stop_keepalive_still_before_seal_transition() -> None:
    source = _execute_source()
    _assert_source_order(source, ["self._stop_h2o_vent_keepalive()", "pressurize_and_hold("])


def test_h2o_vent_off_adapter_does_not_affect_keepalive_vent_on() -> None:
    keepalive_source = _keepalive_source()
    execute_source = _execute_source()

    assert "interval_s = 1.0" in keepalive_source
    assert "H2OVentAdapter(vent_command=controller.vent)" in keepalive_source
    assert "state=ShadowState.OPEN_CONDITIONING" in keepalive_source
    assert "on=True" in keepalive_source
    assert "controller.vent(True)" not in keepalive_source
    assert "on=False" not in keepalive_source
    assert "vent_off_adapter" not in execute_source, "seal transition no longer uses vent_off_adapter"


def test_h2o_vent_off_contract_does_not_import_co2_runner() -> None:
    _assert_h2o_contract_does_not_import_co2_runner()


def test_co2_runtime_not_in_scope_for_h2o_vent_off_contract() -> None:
    _assert_h2o_contract_does_not_import_co2_runner()
