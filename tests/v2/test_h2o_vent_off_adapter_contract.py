from __future__ import annotations

import inspect
import sys
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
from gas_calibrator.v2.core.runners.h2o_route_runner import H2oRouteRunner


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
        self.events.append(f"log:{message[:80]}")

    def record_route_trace(self, **kw) -> None:
        self.trace_payloads.append(dict(kw))
        self.events.append(f"trace:{kw.get('action')}:{kw.get('result', 'ok')}")


class _FakeController:
    def __init__(self, events: list[str]) -> None:
        self.events = events

    def vent(self, on: bool) -> None:
        self.events.append(f"controller.vent:{bool(on)}")


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


def _make_service():
    events: list[str] = []
    controller = _FakeController(events)
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


def _run_h2o_with_sleep(monkeypatch: pytest.MonkeyPatch):
    service, events = _make_service()
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
    vent_off_index = source.index("controller.vent(False)", stop_index)
    settle_index = source.index("time.sleep(1.5)", vent_off_index)
    pre_settle_segment = source[stop_index:settle_index]

    assert "controller.vent(False)" in source
    assert "request_vent" not in pre_settle_segment
    assert "on=False" not in pre_settle_segment
    _assert_source_order(
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


@pytest.mark.xfail(strict=True, reason="D3.1 runtime vent-off adapter wiring not implemented")
def test_h2o_vent_off_future_requires_adapter_policy() -> None:
    source = _execute_source()
    stop_index = source.index("self._stop_h2o_vent_keepalive()")
    settle_index = source.index("time.sleep(1.5)", stop_index)
    pre_settle_segment = source[stop_index:settle_index]

    assert "H2OVentAdapter" in pre_settle_segment
    assert "request_vent" in pre_settle_segment
    assert "route=\"h2o\"" in pre_settle_segment or "route='h2o'" in pre_settle_segment
    assert "ShadowState.SEAL_TRANSITION" in pre_settle_segment
    assert "on=False" in pre_settle_segment
    assert "H2oRouteRunner" in pre_settle_segment
    assert "seal" in pre_settle_segment.lower()
    assert pre_settle_segment.index("request_vent") < source.index("time.sleep(1.5)", stop_index)


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


@pytest.mark.xfail(strict=True, reason="D3.1 runtime vent-off adapter wiring not implemented")
def test_h2o_vent_off_blocked_does_not_close_h2o_path() -> None:
    source = _execute_source()
    blocked_index = source.index("vent_off_result.allowed")
    close_index = source.index("set_h2o_path(False, lead)")

    assert "hardware_command_sent" in source
    assert "blocked_reason" in source
    assert "cleanup_h2o_route" in source[blocked_index:close_index]
    assert "return RouteRunResult(" in source[blocked_index:close_index]
    assert blocked_index < close_index


@pytest.mark.xfail(strict=True, reason="D3.1 runtime vent-off adapter wiring not implemented")
def test_h2o_vent_off_blocked_does_not_read_pressure_or_pressurize() -> None:
    source = _execute_source()
    blocked_index = source.index("vent_off_result.allowed")
    read_index = source.index("read_pressure", blocked_index)
    pressurize_index = source.index("pressurize_and_hold(", blocked_index)
    blocked_segment = source[blocked_index:read_index]

    assert "return RouteRunResult(" in blocked_segment
    assert blocked_index < read_index < pressurize_index


@pytest.mark.xfail(strict=True, reason="D3.1 runtime vent-off adapter wiring not implemented")
def test_h2o_vent_off_blocked_records_evidence() -> None:
    source = _execute_source()
    blocked_index = source.index("vent_off_result")
    evidence_segment = source[blocked_index: source.index("time.sleep(1.5)", blocked_index)]

    assert "vent_off" in evidence_segment
    assert "h2o" in evidence_segment
    assert "SEAL_TRANSITION" in evidence_segment
    assert "blocked_reason" in evidence_segment
    assert "hardware_command_sent" in evidence_segment
    assert "not_real_acceptance_evidence" in evidence_segment
    assert "source" in evidence_segment


def test_h2o_vent_off_success_preserves_1p5s_read_gauge_close_path_order(monkeypatch: pytest.MonkeyPatch) -> None:
    result, events = _run_h2o_with_sleep(monkeypatch)

    assert result.success is True
    _assert_order(
        events,
        [
            "stop_keepalive",
            "controller.vent:False",
            "sleep:1.5",
            "read_pressure_gauge",
            "set_h2o_path:False",
            "pressurize_and_hold:1:direct=True",
        ],
    )


def test_h2o_stop_keepalive_still_before_vent_off() -> None:
    source = _execute_source()

    _assert_source_order(
        source,
        [
            "self._stop_h2o_vent_keepalive()",
            "controller.vent(False)",
        ],
    )


def test_h2o_vent_off_adapter_does_not_affect_keepalive_vent_on() -> None:
    keepalive_source = _keepalive_source()
    execute_source = _execute_source()

    assert "interval_s = 1.0" in keepalive_source
    assert "H2OVentAdapter(vent_command=controller.vent)" in keepalive_source
    assert "state=ShadowState.OPEN_CONDITIONING" in keepalive_source
    assert "on=True" in keepalive_source
    assert "controller.vent(True)" not in keepalive_source
    assert "on=False" not in keepalive_source
    assert "controller.vent(False)" in execute_source


def test_co2_runtime_not_in_scope_for_h2o_vent_off_contract() -> None:
    module_globals = set(globals())
    loaded_co2_modules = [name for name in sys.modules if name.startswith("gas_calibrator.v2.core.runners.co2")]

    assert "Co2RouteRunner" not in module_globals
    assert not loaded_co2_modules
