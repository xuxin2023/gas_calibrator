from __future__ import annotations

import inspect
from types import SimpleNamespace

import pytest

from gas_calibrator.v2.config import AppConfig
from gas_calibrator.v2.core.event_bus import EventBus
from gas_calibrator.v2.core.models import CalibrationPoint
from gas_calibrator.v2.core.point_parser import PointParser
from gas_calibrator.v2.core.route_context import RouteContext
from gas_calibrator.v2.core.route_planner import RoutePlanner
from gas_calibrator.v2.core.route_state_shadow import ShadowState, build_shadow_event
from gas_calibrator.v2.core.runners.h2o_route_runner import H2oRouteRunner
from gas_calibrator.v2.core.services.dewpoint_alignment_service import DewpointAlignmentService


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
        self.keepalive_active = False
        self.stop_count = 0

    def _start_h2o_vent_keepalive(self) -> None:
        self.keepalive_active = True
        self._events.append("start_keepalive")

    def _stop_h2o_vent_keepalive(self) -> None:
        self.stop_count += 1
        self._events.append("stop_keepalive")
        self.keepalive_active = False


def _make_service(*, sample_results: list | None = None, pressurize_ok: bool = True):
    events: list[str] = []
    controller = _FakeController(events)
    gauge = _FakeGauge(events)
    status_service = _RecordingStatusService(events)

    def _sample_point(point, phase: str = "", point_tag: str = ""):
        events.append(f"sample_point:{point.index}")
        if sample_results is not None:
            return sample_results
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
        events.append(f"set_pressure_to_target:{point.index}:{point.target_pressure_hpa}")
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
            or SimpleNamespace(ok=pressurize_ok),
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
    return service, events, status_service


def _run_h2o(pressure_points: list[CalibrationPoint], **service_kwargs):
    service, events, status_service = _make_service(**service_kwargs)
    lead = _h2o_point(1, pressure=pressure_points[0].target_pressure_hpa if pressure_points else 1100.0)
    runner = _InstrumentedH2oRouteRunner(service, [lead], pressure_points, events)
    result = runner.execute()
    return result, events, status_service, runner


def _index(events: list[str], prefix: str) -> int:
    return next(i for i, item in enumerate(events) if item.startswith(prefix))


def test_h2o_open_route_requires_vent_on_before_h2o_path_open() -> None:
    source = inspect.getsource(DewpointAlignmentService.open_h2o_route_and_wait_ready)
    assert source.index("_set_pressure_controller_vent(True") < source.index("_set_h2o_path(True")

    event = build_shadow_event(
        {
            "route": "h2o",
            "source_action": "open_h2o_route_and_wait_ready",
            "source_function": "DewpointAlignmentService.open_h2o_route_and_wait_ready",
            "vent_state_observed": "ON",
        }
    )

    assert event["route"] == "h2o"
    assert event["shadow_state"] == ShadowState.OPEN_CONDITIONING.value
    assert event["vent_state_observed"] == "ON"
    assert event["observation_only"] is True


def test_h2o_keepalive_current_interval_is_legacy_1s_behavior() -> None:
    source = inspect.getsource(H2oRouteRunner._start_h2o_vent_keepalive)
    assert "interval_s = 1.0" in source
    assert "controller.vent(True)" in source


def test_h2o_keepalive_allowed_only_in_open_or_ambient_state() -> None:
    allowed = {ShadowState.OPEN_CONDITIONING.value, ShadowState.AMBIENT_OPEN_SAMPLING.value}
    open_event = build_shadow_event({"route": "h2o", "source_action": "start_h2o_vent_keepalive", "vent_state_observed": "ON"})
    ambient_event = build_shadow_event({"route": "h2o", "source_action": "pressure_skip", "target": {"vent_on": True}})
    sealed_event = build_shadow_event({"route": "h2o", "source_action": "set_pressure_to_target", "vent_state_observed": "ON"})

    assert open_event["shadow_state"] in allowed
    assert ambient_event["shadow_state"] in allowed
    assert sealed_event["shadow_state"] == ShadowState.SEALED_PRESSURE_CONTROL.value
    assert sealed_event["shadow_state"] not in allowed


def test_h2o_ambient_open_is_not_zero_hpa_pressure_target() -> None:
    result, events, status_service, _ = _run_h2o([_h2o_point(10, pressure=1013.25, mode="ambient_open")])

    assert result.success
    pressure_skip = [payload for payload in status_service.trace_payloads if payload.get("action") == "pressure_skip"]
    assert pressure_skip
    assert all(payload.get("route") == "h2o" for payload in pressure_skip)
    assert all(payload.get("target", {}).get("pressure_hpa") is None for payload in pressure_skip)
    assert all(payload.get("target", {}).get("vent_on") is True for payload in pressure_skip)
    assert not any(item.startswith("set_pressure_to_target:") for item in events)
    assert not any("set_pressure_to_target:0" in item for item in events)
    assert _index(events, "start_keepalive") < _index(events, "sample_point:")


def test_h2o_seal_transition_order_stop_keepalive_vent_off_settle_read_pressure_close_path(monkeypatch: pytest.MonkeyPatch) -> None:
    sleep_calls: list[float] = []
    monkeypatch.setattr("gas_calibrator.v2.core.runners.h2o_route_runner.time.sleep", lambda seconds: sleep_calls.append(seconds))

    result, events, _, _ = _run_h2o([_h2o_point(10, pressure=1100.0)])

    assert result.success
    assert _index(events, "stop_keepalive") < _index(events, "controller.vent:False")
    assert _index(events, "controller.vent:False") < _index(events, "read_pressure_gauge")
    assert _index(events, "read_pressure_gauge") < _index(events, "set_h2o_path:False")
    assert _index(events, "set_h2o_path:False") < _index(events, "pressurize_and_hold:")
    assert sleep_calls == [1.5]
    assert "pressurize_and_hold:1:direct=True" in events


def test_h2o_sealed_pressure_control_has_no_keepalive_vent_on(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("gas_calibrator.v2.core.runners.h2o_route_runner.time.sleep", lambda seconds: None)

    result, events, _, _ = _run_h2o([_h2o_point(10, pressure=1100.0), _h2o_point(11, pressure=1000.0)])

    assert result.success
    first_sealed_setpoint = _index(events, "set_pressure_to_target:")
    assert _index(events, "stop_keepalive") < first_sealed_setpoint
    assert not any(item == "controller.vent:True" for item in events[first_sealed_setpoint:])
    assert not any(item == "start_keepalive" for item in events[first_sealed_setpoint:])
    assert all(_index(events, "set_h2o_path:False") < i for i, item in enumerate(events) if item.startswith("set_pressure_to_target:"))


def test_h2o_cleanup_always_stops_keepalive() -> None:
    result, events, _, runner = _run_h2o([_h2o_point(10, pressure=1100.0)], sample_results=[])

    assert not result.success
    assert runner.stop_count >= 1
    assert "stop_keepalive" in events
    assert any(item.startswith("sample_point:") for item in events)


def test_h2o_keepalive_cannot_pollute_co2_sealed_no_vent_guard(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("gas_calibrator.v2.core.runners.h2o_route_runner.time.sleep", lambda seconds: None)

    result, events, _, runner = _run_h2o([_h2o_point(10, pressure=1100.0)])

    assert result.success
    assert runner.keepalive_active is False
    assert events[-1] == "stop_keepalive"
    sealed_start = _index(events, "pressurize_and_hold:")
    assert not any(item == "controller.vent:True" for item in events[sealed_start:])


@pytest.mark.xfail(reason="future policy spec: keepalive interval is not yet route/state policy driven", strict=False)
def test_future_keepalive_policy_can_express_common_2s_default_without_runtime_change() -> None:
    source = inspect.getsource(H2oRouteRunner._start_h2o_vent_keepalive)
    assert "interval_s = 2.0" in source or "keepalive_policy" in source
    assert "SEALED_PRESSURE_CONTROL" in source


@pytest.mark.xfail(reason="future policy spec: H2O keepalive interval is still hardcoded legacy 1s", strict=False)
def test_future_h2o_keepalive_interval_is_configurable_not_hardcoded() -> None:
    source = inspect.getsource(H2oRouteRunner._start_h2o_vent_keepalive)
    assert "interval_s = 1.0" not in source
    assert "_cfg_get" in source or "keepalive_policy" in source
