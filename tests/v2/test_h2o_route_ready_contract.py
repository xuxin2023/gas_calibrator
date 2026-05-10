from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

from gas_calibrator.v2.config import AppConfig
from gas_calibrator.v2.core.event_bus import EventBus
from gas_calibrator.v2.core.models import CalibrationPoint
from gas_calibrator.v2.core.point_parser import PointParser
from gas_calibrator.v2.core.route_context import RouteContext
from gas_calibrator.v2.core.route_planner import RoutePlanner
from gas_calibrator.v2.core.runners.h2o_route_runner import H2oRouteRunner
from gas_calibrator.v2.core.services.dewpoint_alignment_service import DewpointAlignmentService


def _point(index: int = 1, *, pressure_hpa: float | None = 900.0, pressure_mode: str = "sealed_controlled") -> CalibrationPoint:
    return CalibrationPoint(
        index=index,
        temperature_c=25.0,
        humidity_pct=50.0,
        pressure_hpa=pressure_hpa,
        route="h2o",
        humidity_generator_temp_c=20.0,
        dewpoint_c=9.3,
        h2o_mmol=11.6,
        pressure_mode=pressure_mode,
        pressure_selection_token="ambient_open" if pressure_mode == "ambient_open" else None,
    )


class _FakeKeepaliveRunner(H2oRouteRunner):
    def __init__(self, service: Any, points: list[CalibrationPoint], pressure_points: list[CalibrationPoint], events: list[str]) -> None:
        super().__init__(service, points, pressure_points)
        self.events = events
        self.fake_keepalive_started = False

    def _start_h2o_vent_keepalive(self) -> None:
        if self.fake_keepalive_started:
            self.events.append("start_keepalive_duplicate")
            return
        self.fake_keepalive_started = True
        self.events.append("start_keepalive")

    def _stop_h2o_vent_keepalive(self) -> None:
        self.events.append("stop_keepalive")
        self.fake_keepalive_started = False


def _runner_service(events: list[str], *, route_ready: bool = True, route_ready_evidence: list[dict[str, Any]] | None = None) -> Any:
    class StatusService:
        def check_stop(self) -> None:
            events.append("check_stop")

        def update_status(self, **kwargs: Any) -> None:
            events.append(f"update:{getattr(kwargs.get('phase'), 'value', kwargs.get('phase'))}")

        def begin_point_timing(self, point: CalibrationPoint, *, phase: str = "", point_tag: str = "") -> None:
            events.append(f"begin:{point_tag}")

        def clear_point_timing(self, point: CalibrationPoint, *, phase: str = "", point_tag: str = "") -> None:
            events.append(f"clear:{point_tag}")

        def mark_point_stable_for_sampling(self, point: CalibrationPoint, *, phase: str = "", point_tag: str = "") -> None:
            events.append(f"stable:{point_tag}")

        def log(self, message: str) -> None:
            events.append(f"log:{message}")

        def record_route_trace(self, **kwargs: Any) -> None:
            events.append(f"trace:{kwargs.get('action')}:{kwargs.get('result', 'ok')}:{kwargs.get('message', '')}")

    class DewpointAlignment:
        last_h2o_route_ready_evidence = route_ready_evidence or []

        def open_h2o_route_and_wait_ready(self, point: CalibrationPoint) -> bool:
            events.append("open_h2o_route_ready")
            return route_ready

        def wait_dewpoint_alignment_stable(self, point: CalibrationPoint) -> bool:
            events.append("dewpoint_align")
            return True

    controller = SimpleNamespace(vent=lambda on: events.append(f"controller_vent:{bool(on)}"))
    gauge = SimpleNamespace(read_pressure=lambda: events.append("read_pressure") or 1015.0)
    return SimpleNamespace(
        event_bus=EventBus(),
        route_context=RouteContext(),
        route_planner=RoutePlanner(AppConfig.from_dict({}), PointParser()),
        status_service=StatusService(),
        valve_routing_service=SimpleNamespace(
            apply_route_baseline_valves=lambda: events.append("baseline_valves"),
            set_h2o_path=lambda is_open, point=None: events.append(f"set_h2o_path:{bool(is_open)}"),
            mark_post_h2o_co2_zero_flush_pending=lambda: events.append("mark_pending"),
            cleanup_h2o_route=lambda point, reason="": events.append(f"cleanup:{reason}"),
        ),
        pressure_control_service=SimpleNamespace(
            prepare_pressure_for_h2o=lambda point: events.append("prepare_pressure"),
            set_pressure_controller_vent=lambda on, reason="": events.append(f"vent:{bool(on)}:{reason}"),
            pressurize_and_hold=lambda point, route="h2o", **kwargs: events.append(
                f"pressurize_and_hold:direct={kwargs.get('prefer_direct_vent_close', False)}"
            )
            or SimpleNamespace(ok=True),
            set_pressure_to_target=lambda point: events.append(f"set_pressure_to_target:{point.index}") or SimpleNamespace(ok=True),
            wait_after_pressure_stable_before_sampling=lambda point: events.append(f"sample_hold:{point.index}") or SimpleNamespace(ok=True),
            run_state=SimpleNamespace(pressure=SimpleNamespace(preseal_watchlist_status_accepted=False)),
        ),
        humidity_generator_service=SimpleNamespace(
            prepare_humidity_generator=lambda point: events.append("prepare_humidity"),
            wait_humidity_generator_stable=lambda point: events.append("humidity_wait") or SimpleNamespace(ok=True, timed_out=False),
        ),
        temperature_control_service=SimpleNamespace(
            set_temperature_for_point=lambda point, phase="": events.append("temperature_wait") or SimpleNamespace(ok=True),
            capture_temperature_calibration_snapshot=lambda point, route_type="": events.append("capture_temp"),
        ),
        dewpoint_alignment_service=DewpointAlignment(),
        sampling_service=SimpleNamespace(
            sample_point=lambda point, phase="", point_tag="": events.append(f"sample:{point_tag}")
            or [SimpleNamespace(point=point, point_tag=point_tag, h2o_mmol=11.6, pressure_hpa=point.target_pressure_hpa or 1013.25, temperature_c=20.0)],
        ),
        qc_service=SimpleNamespace(run_point_qc=lambda point, phase="", point_tag="": events.append(f"qc:{point_tag}")),
        device_manager=SimpleNamespace(get_device=lambda name: controller if name == "pressure_controller" else gauge if name == "pressure_gauge" else None),
        _cfg_get=lambda path, default=None: default,
    )


def _assert_order(events: list[str], expected: list[str]) -> None:
    position = -1
    for token in expected:
        next_position = next((index for index, item in enumerate(events) if index > position and token in item), -1)
        assert next_position >= 0, f"missing {token!r} in {events!r}"
        position = next_position


class _StatusRecorder:
    def __init__(self) -> None:
        self.payloads: list[dict[str, Any]] = []

    def record_route_trace(self, **kwargs: Any) -> None:
        self.payloads.append(dict(kwargs))


def _dewpoint_service(
    *,
    dewpoint: Any = None,
    collect_only: bool = False,
    h2o_path_ok: bool = True,
) -> tuple[DewpointAlignmentService, _StatusRecorder, list[str]]:
    events: list[str] = []
    status = _StatusRecorder()
    host = SimpleNamespace(
        status_service=status,
        _device=lambda name: dewpoint if name == "dewpoint_meter" else None,
        _collect_only_fast_path_enabled=lambda: collect_only,
        _log=lambda message: events.append(f"log:{message}"),
        _set_pressure_controller_vent=lambda on, reason="": events.append(f"vent:{bool(on)}:{reason}"),
        _set_h2o_path=lambda is_open, point: events.append(f"h2o_path:{bool(is_open)}") or h2o_path_ok,
        _last_h2o_path_evidence={
            "relay_command_sent": True,
            "relay_command_result": "sent",
            "h2o_path_return_value": h2o_path_ok,
            "route_physical_state_match": h2o_path_ok,
            "relay_physical_mismatch": not h2o_path_ok,
            "mismatched_channels": [] if h2o_path_ok else [{"logical_valve": 8, "relay": "relay_b", "channel": 8, "target": True, "actual": False}],
            "h2o_path_open_verified": h2o_path_ok,
            "h2o_path_open_failure_reason": "" if h2o_path_ok else "relay_physical_mismatch",
        },
        _cfg_get=lambda path, default=None: 0.0 if path == "workflow.stability.h2o_route.preseal_soak_s" else default,
        _check_stop=lambda: events.append("check_stop"),
        _normalize_snapshot=lambda value: dict(value),
    )
    service = DewpointAlignmentService(SimpleNamespace(), SimpleNamespace(), host=host)
    return service, status, events


def _trace_steps(status: _StatusRecorder) -> list[dict[str, Any]]:
    return [payload for payload in status.payloads if payload.get("action") == "h2o_route_ready_step"]


def test_h2o_keepalive_starts_before_open_route_ready() -> None:
    events: list[str] = []
    service = _runner_service(events)

    result = _FakeKeepaliveRunner(service, [_point()], [_point(2, pressure_hpa=1000.0)], events).execute()

    assert result.success is True
    _assert_order(events, ["humidity_wait", "capture_temp", "start_keepalive", "open_h2o_route_ready"])


def test_h2o_keepalive_stops_when_route_ready_fails() -> None:
    events: list[str] = []
    service = _runner_service(events, route_ready=False)

    result = _FakeKeepaliveRunner(service, [_point()], [_point(2, pressure_hpa=1000.0)], events).execute()

    assert result.success is False
    assert "dewpoint_align" not in events
    assert not any(item.startswith("sample:") for item in events)
    _assert_order(events, ["start_keepalive", "open_h2o_route_ready", "cleanup:after H2O route timeout", "stop_keepalive"])


def test_h2o_keepalive_not_started_twice_after_route_ready_success() -> None:
    events: list[str] = []
    service = _runner_service(events)

    result = _FakeKeepaliveRunner(service, [_point()], [_point(2, pressure_hpa=1000.0)], events).execute()

    assert result.success is True
    assert events.count("start_keepalive") == 1
    assert "start_keepalive_duplicate" not in events


def test_h2o_route_ready_records_vent_on_step() -> None:
    service, status, _ = _dewpoint_service(dewpoint=SimpleNamespace())
    service._read_dewpoint_snapshot = lambda *args, **kwargs: {"dewpoint_c": 9.3, "temp_c": 20.0}

    assert service.open_h2o_route_and_wait_ready(_point()) is True

    steps = _trace_steps(status)
    assert any(payload["actual"]["step"] == "vent_on_before_h2o_path" and payload["result"] == "ok" for payload in steps)


def test_h2o_route_ready_records_h2o_path_open_step() -> None:
    service, status, _ = _dewpoint_service(dewpoint=SimpleNamespace(), h2o_path_ok=False)

    assert service.open_h2o_route_and_wait_ready(_point()) is False

    steps = _trace_steps(status)
    failure = [payload for payload in steps if payload["actual"]["step"] == "set_h2o_path_open"][-1]
    assert failure["result"] == "fail"
    assert failure["actual"]["relay_command_sent"] is True
    assert failure["actual"]["relay_command_result"] == "sent"
    assert failure["actual"]["h2o_path_return_value"] is False
    assert failure["actual"]["route_physical_state_match"] is False
    assert failure["actual"]["relay_physical_mismatch"] is True
    assert failure["actual"]["mismatched_channels"]
    assert failure["actual"]["h2o_path_open_verified"] is False
    assert failure["actual"]["h2o_path_open_failure_reason"] == "relay_physical_mismatch"


def test_h2o_route_ready_records_dewpoint_unavailable() -> None:
    service, status, _ = _dewpoint_service(dewpoint=None)

    assert service.open_h2o_route_and_wait_ready(_point()) is False

    failures = [payload for payload in _trace_steps(status) if payload["result"] == "fail"]
    assert failures[-1]["actual"]["step"] == "dewpoint_meter_available"
    assert failures[-1]["actual"]["reason"] == "dewpoint_meter_unavailable"


def test_h2o_route_ready_records_dewpoint_open_failed() -> None:
    class Dewpoint:
        def open(self) -> None:
            raise RuntimeError("forced open failure")

    service, status, _ = _dewpoint_service(dewpoint=Dewpoint())

    assert service.open_h2o_route_and_wait_ready(_point()) is False

    failures = [payload for payload in _trace_steps(status) if payload["result"] == "fail"]
    assert failures[-1]["actual"]["step"] == "dewpoint_meter_open"
    assert failures[-1]["actual"]["reason"] == "dewpoint_open_failed"
    assert failures[-1]["actual"]["error_message"] == "forced open failure"


def test_h2o_route_ready_records_dewpoint_initial_read_failed() -> None:
    service, status, _ = _dewpoint_service(dewpoint=SimpleNamespace())
    service._read_dewpoint_snapshot = lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("forced read failure"))

    assert service.open_h2o_route_and_wait_ready(_point()) is False

    failures = [payload for payload in _trace_steps(status) if payload["result"] == "fail"]
    assert failures[-1]["actual"]["step"] == "dewpoint_initial_read"
    assert failures[-1]["actual"]["reason"] == "dewpoint_initial_read_failed"
    assert failures[-1]["actual"]["error_message"] == "forced read failure"


def test_h2o_route_ready_records_preseal_soak_result() -> None:
    service, status, events = _dewpoint_service(dewpoint=SimpleNamespace())
    service._read_dewpoint_snapshot = lambda *args, **kwargs: {"dewpoint_c": 9.3, "temp_c": 20.0}

    assert service.open_h2o_route_and_wait_ready(_point()) is True

    assert "check_stop" not in events
    assert any(payload["actual"]["step"] == "preseal_soak" and payload["result"] == "ok" for payload in _trace_steps(status))


def test_h2o_route_ready_failure_message_contains_subreason() -> None:
    events: list[str] = []
    evidence = [{"step": "dewpoint_initial_read", "result": "fail", "reason": "dewpoint_initial_read_failed"}]
    service = _runner_service(events, route_ready=False, route_ready_evidence=evidence)

    result = _FakeKeepaliveRunner(service, [_point()], [_point(2, pressure_hpa=1000.0)], events).execute()

    assert result.success is False
    wait_trace = [item for item in events if item.startswith("trace:wait_route_ready:timeout:")][-1]
    assert "failure_step=dewpoint_initial_read" in wait_trace
    assert "reason=dewpoint_initial_read_failed" in wait_trace


def test_h2o_vent_off_chain_unchanged_after_route_ready_fix(monkeypatch: pytest.MonkeyPatch) -> None:
    events: list[str] = []
    service = _runner_service(events)
    monkeypatch.setattr("gas_calibrator.v2.core.runners.h2o_route_runner.time.sleep", lambda seconds: events.append(f"sleep:{seconds}"))

    result = _FakeKeepaliveRunner(service, [_point()], [_point(2, pressure_hpa=1000.0)], events).execute()

    assert result.success is True
    _assert_order(
        events,
        [
            "stop_keepalive",
            "controller_vent:False",
            "sleep:1.5",
            "read_pressure",
            "set_h2o_path:False",
            "pressurize_and_hold:direct=True",
        ],
    )


def test_co2_not_touched_by_h2o_route_ready_fix() -> None:
    events: list[str] = []
    service = _runner_service(events)

    result = _FakeKeepaliveRunner(service, [_point()], [_point(2, pressure_hpa=1000.0)], events).execute()

    assert result.success is True
    assert all("co2" not in item.lower() for item in events)
