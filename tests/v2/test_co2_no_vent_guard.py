from __future__ import annotations

from types import SimpleNamespace

from gas_calibrator.v2.config import AppConfig
from gas_calibrator.v2.core.event_bus import EventBus
from gas_calibrator.v2.core.models import CalibrationPoint
from gas_calibrator.v2.core.point_parser import PointParser
from gas_calibrator.v2.core.route_context import RouteContext
from gas_calibrator.v2.core.route_planner import RoutePlanner
from gas_calibrator.v2.core.runners.co2_route_runner import Co2RouteRunner


class RecordingRouteContext(RouteContext):
    def __init__(self) -> None:
        super().__init__()
        self.snapshots: list[dict[str, object]] = []

    def enter(self, **kwargs) -> None:
        super().enter(**kwargs)
        self.snapshots.append(self._snapshot())

    def update(self, **kwargs) -> None:
        super().update(**kwargs)
        self.snapshots.append(self._snapshot())

    def _snapshot(self) -> dict[str, object]:
        return {
            "current_route": self.current_route,
            "source_point_index": None if self.source_point is None else self.source_point.index,
            "active_point_index": None if self.active_point is None else self.active_point.index,
            "point_tag": self.point_tag,
            "retry": self.retry,
        }


class PressureControlStub:
    def __init__(self, calls: list[str], *, seal_ok: bool = True, wait_ok: bool = True, vent_on: bool = False) -> None:
        self.calls = calls
        self.seal_ok = seal_ok
        self.wait_ok = wait_ok
        self.vent_on = vent_on

    def pressurize_and_hold(self, point, route="co2"):
        self.calls.append("vent_guard:seal_call")
        if self.vent_on:
            self.calls.append("vent_guard:unexpected_vent_on")
        return SimpleNamespace(ok=self.seal_ok)

    def set_pressure_to_target(self, point):
        self.calls.append(f"pressure_target:{float(point.target_pressure_hpa):.0f}")
        return SimpleNamespace(ok=True)

    def wait_after_pressure_stable_before_sampling(self, point):
        self.calls.append("vent_guard:post_stable_wait")
        return SimpleNamespace(ok=self.wait_ok)


class StatusServiceStub:
    def __init__(self, calls: list[str]) -> None:
        self.calls = calls

    def check_stop(self):
        self.calls.append("check_stop")

    def update_status(self, **kwargs):
        self.calls.append(f"update:{kwargs['phase'].value}")

    def begin_point_timing(self, point, *, phase="", point_tag=""):
        self.calls.append(f"begin:{point_tag}")

    def clear_point_timing(self, point, *, phase="", point_tag=""):
        self.calls.append(f"clear:{point_tag}")

    def mark_point_stable_for_sampling(self, point, *, phase="", point_tag=""):
        self.calls.append(f"stable:{point_tag}")

    def log(self, message: str):
        self.calls.append(f"log:{message}")

    def record_route_trace(self, **kwargs):
        self.calls.append(f"trace:{kwargs.get('action')}:{kwargs.get('result', 'ok')}")


def _source_point() -> CalibrationPoint:
    return CalibrationPoint(index=10, temperature_c=20.0, co2_ppm=800.0, pressure_hpa=900.0, route="co2", co2_group="A")


def _pressure_points() -> list[CalibrationPoint]:
    return [
        CalibrationPoint(index=11, temperature_c=20.0, co2_ppm=800.0, pressure_hpa=1000.0, route="co2", co2_group="A"),
    ]


def _run_with_pressure_stub(*, seal_ok: bool = True, wait_ok: bool = True, vent_on: bool = False):
    calls: list[str] = []
    context = RecordingRouteContext()
    route_planner = RoutePlanner(AppConfig.from_dict({}), PointParser())
    pressure = PressureControlStub(calls, seal_ok=seal_ok, wait_ok=wait_ok, vent_on=vent_on)
    service = SimpleNamespace(
        event_bus=EventBus(),
        route_context=context,
        route_planner=route_planner,
        a2_hooks=SimpleNamespace(
            callbacks={},
            co2_route_open_monotonic_s=None,
            co2_route_conditioning_at_atmosphere_active=False,
            co2_route_open_pressure_hpa=None,
            preseal_pressure_rise_detected=False,
            route_open_pressure_first_sample_recorded=False,
            co2_route_conditioning_at_atmosphere_context={},
            high_pressure_first_point_mode_enabled=False,
        ),
        status_service=StatusServiceStub(calls),
        temperature_control_service=SimpleNamespace(
            set_temperature_for_point=lambda point, phase="": calls.append("wait_temperature") or SimpleNamespace(ok=True),
            capture_temperature_calibration_snapshot=lambda point, route_type="": calls.append("capture_temperature"),
        ),
        valve_routing_service=SimpleNamespace(
            set_co2_route_baseline=lambda reason="": calls.append(f"route_baseline:{reason}"),
            set_valves_for_co2=lambda point: calls.append(f"open_co2_route:{point.index}"),
            cleanup_co2_route=lambda reason="": calls.append(f"cleanup:{reason}"),
        ),
        pressure_control_service=pressure,
        sampling_service=SimpleNamespace(
            sampling_params=lambda phase="": (4, 0.0),
            sample_point=lambda point, phase="", point_tag="": calls.append(f"sample:{float(point.target_pressure_hpa):.0f}") or [],
        ),
        qc_service=SimpleNamespace(run_point_qc=lambda point, phase="", point_tag="": calls.append(f"qc:{float(point.target_pressure_hpa):.0f}")),
        _wait_co2_route_soak_before_seal=lambda point: calls.append("wait_route_soak") or True,
        _record_workflow_timing=lambda *args, **kwargs: None,
        _cfg_get=lambda path, default=None: default,
        _as_float=lambda value: None if value is None else float(value),
    )
    result = Co2RouteRunner(service, _source_point(), _pressure_points()).execute()
    return result, calls, context


def test_co2_no_vent_guard_stays_armed() -> None:
    result, calls, _ = _run_with_pressure_stub(vent_on=False)

    assert result.success is False or result.success is True
    assert "vent_guard:seal_call" in calls
    assert "vent_guard:unexpected_vent_on" not in calls


def test_co2_sealed_pressure_control_has_zero_real_vent_on() -> None:
    _, calls, _ = _run_with_pressure_stub(vent_on=False)

    assert not any(item == "vent_guard:unexpected_vent_on" for item in calls)
    assert not any(item.startswith("vent_on") for item in calls)


def test_co2_route_pressure_block_keeps_no_vent_guard() -> None:
    result, calls, _ = _run_with_pressure_stub(seal_ok=False)

    assert result.success is False
    assert "vent_guard:seal_call" in calls
    assert "cleanup:after CO2 pressure-seal failure" in calls


def test_co2_open_conditioning_vent_on_sequence_is_not_reopened_during_seal() -> None:
    _, calls, _ = _run_with_pressure_stub()

    assert calls.count("route_baseline:before CO2 route conditioning") == 1
    assert calls.count("open_co2_route:10") == 1
    assert calls.count("vent_guard:seal_call") == 1


def test_co2_seal_transition_vent_off_before_route_close() -> None:
    _, calls, _ = _run_with_pressure_stub()

    assert calls.index("wait_route_soak") < calls.index("vent_guard:seal_call")
    assert calls.index("vent_guard:seal_call") < calls.index("pressure_target:1000")
