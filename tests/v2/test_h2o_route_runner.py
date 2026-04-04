from __future__ import annotations

from types import SimpleNamespace

from gas_calibrator.v2.core.event_bus import EventBus, EventType
from gas_calibrator.v2.core.models import CalibrationPoint, CalibrationPhase
from gas_calibrator.v2.core.route_context import RouteContext
from gas_calibrator.v2.core.route_planner import RoutePlanner
from gas_calibrator.v2.core.point_parser import PointParser
from gas_calibrator.v2.config import AppConfig
from gas_calibrator.v2.core.runners.h2o_route_runner import H2oRouteRunner


def _assert_subsequence(calls: list[str], expected: list[str]) -> None:
    position = 0
    for expected_item in expected:
        while position < len(calls) and calls[position] != expected_item:
            position += 1
        assert position < len(calls), f"Missing expected call order item: {expected_item!r} in {calls!r}"
        position += 1


def _service(
    calls: list[str],
    *,
    humidity_ok: bool = True,
    collect_only: bool = False,
    humidity_timeout_policy: str = "abort_like_v1",
):
    event_bus = EventBus()
    samples: list[object] = []
    event_bus.subscribe(EventType.SAMPLE_COLLECTED, lambda event: samples.append(event.data))

    class StatusService:
        def check_stop(self): calls.append("check_stop")
        def update_status(self, **kwargs): calls.append(f"update:{kwargs['phase'].value}")
        def begin_point_timing(self, point, *, phase="", point_tag=""): calls.append(f"begin:{point_tag}")
        def clear_point_timing(self, point, *, phase="", point_tag=""): calls.append(f"clear:{point_tag}")
        def mark_point_stable_for_sampling(self, point, *, phase="", point_tag=""): calls.append(f"stable:{point_tag}")
        def log(self, message: str): calls.append(f"log:{message}")
        def record_route_trace(self, **kwargs): calls.append(f"trace:{kwargs.get('action')}:{kwargs.get('result', 'ok')}")

    service = SimpleNamespace(
        event_bus=event_bus,
        route_context=RouteContext(),
        route_planner=RoutePlanner(AppConfig.from_dict({}), PointParser()),
        status_service=StatusService(),
        valve_routing_service=SimpleNamespace(
            set_h2o_path=lambda is_open, point=None: calls.append(f"h2o_path:{is_open}"),
            mark_post_h2o_co2_zero_flush_pending=lambda: calls.append("mark_pending"),
            cleanup_h2o_route=lambda point, reason="": calls.append(f"cleanup:{reason}"),
        ),
        pressure_control_service=SimpleNamespace(
            prepare_pressure_for_h2o=lambda point: calls.append("prepare_pressure"),
            pressurize_and_hold=lambda point, route="h2o": calls.append("seal") or SimpleNamespace(ok=True),
            set_pressure_to_target=lambda point: calls.append(f"target_pressure:{point.index}") or SimpleNamespace(ok=True),
            wait_after_pressure_stable_before_sampling=lambda point: calls.append(f"sample_hold:{point.index}") or SimpleNamespace(ok=True),
        ),
        humidity_generator_service=SimpleNamespace(
            prepare_humidity_generator=lambda point: calls.append("prepare_humidity"),
            wait_humidity_generator_stable=lambda point: calls.append("humidity_wait") or SimpleNamespace(ok=humidity_ok, timed_out=not humidity_ok),
        ),
        temperature_control_service=SimpleNamespace(
            set_temperature_for_point=lambda point, phase="": calls.append("temperature_wait") or SimpleNamespace(ok=True),
            capture_temperature_calibration_snapshot=lambda point, route_type="": calls.append("capture_temp"),
        ),
        dewpoint_alignment_service=SimpleNamespace(
            open_h2o_route_and_wait_ready=lambda point: calls.append("open_h2o_route_ready") or True,
            wait_dewpoint_alignment_stable=lambda point: calls.append("dewpoint_align") or True,
        ),
        sampling_service=SimpleNamespace(
            sample_point=lambda point, phase="", point_tag="": calls.append(f"sample:{point_tag}") or [SimpleNamespace(point=point, point_tag=point_tag)],
        ),
        qc_service=SimpleNamespace(run_point_qc=lambda point, phase="", point_tag="": calls.append(f"qc:{point_tag}")),
        _collect_only_mode=lambda: collect_only,
        _cfg_get=lambda path, default=None: (
            humidity_timeout_policy if path == "workflow.stability.h2o_route.humidity_timeout_policy" else default
        ),
    )
    return service, samples


def test_h2o_route_runner_executes_happy_path() -> None:
    calls: list[str] = []
    service, samples = _service(calls)
    lead = CalibrationPoint(index=1, temperature_c=25.0, humidity_pct=50.0, pressure_hpa=900.0, route="h2o")
    pressure = CalibrationPoint(index=2, temperature_c=25.0, humidity_pct=50.0, pressure_hpa=1000.0, route="h2o")

    result = H2oRouteRunner(service, [lead], [pressure]).execute()

    assert service.route_context.current_route == ""
    assert result.success is True
    assert result.completed_point_indices == [2]
    assert result.sampled_point_indices == [2]
    assert result.skipped_point_indices == []
    assert "prepare_pressure" in calls
    assert "prepare_humidity" in calls
    assert "capture_temp" in calls
    assert any(item.startswith("begin:h2o_") for item in calls)
    assert any(item.startswith("stable:h2o_") for item in calls)
    assert any(item.startswith("qc:h2o_") for item in calls)
    assert "trace:wait_temperature:ok" in calls
    assert "trace:wait_humidity:ok" in calls
    assert "trace:sample_start:ok" in calls
    assert "trace:sample_end:ok" in calls
    assert "mark_pending" in calls
    assert samples


def test_h2o_route_runner_cleans_up_when_humidity_wait_fails() -> None:
    calls: list[str] = []
    service, samples = _service(calls, humidity_ok=False)
    lead = CalibrationPoint(index=1, temperature_c=25.0, humidity_pct=50.0, pressure_hpa=900.0, route="h2o")

    result = H2oRouteRunner(service, [lead], []).execute()

    assert result.success is False
    assert result.completed_point_indices == []
    assert result.sampled_point_indices == []
    assert result.skipped_point_indices == [1]
    assert any(item == "cleanup:after H2O humidity timeout" for item in calls)
    assert samples == []


def test_h2o_route_runner_collect_only_abort_like_v1_by_default() -> None:
    calls: list[str] = []
    service, samples = _service(calls, humidity_ok=False, collect_only=True)
    lead = CalibrationPoint(index=1, temperature_c=0.0, humidity_pct=50.0, pressure_hpa=1100.0, route="h2o")

    result = H2oRouteRunner(service, [lead], [lead]).execute()

    assert result.success is False
    assert result.completed_point_indices == []
    assert result.sampled_point_indices == []
    assert result.skipped_point_indices == [1]
    assert "cleanup:after H2O humidity timeout" in calls
    assert all("continue_after_timeout" not in item for item in calls)
    assert samples == []


def test_h2o_route_runner_collect_only_can_continue_after_humidity_timeout_when_policy_enables_it() -> None:
    calls: list[str] = []
    service, samples = _service(
        calls,
        humidity_ok=False,
        collect_only=True,
        humidity_timeout_policy="continue_after_timeout",
    )
    lead = CalibrationPoint(index=1, temperature_c=0.0, humidity_pct=50.0, pressure_hpa=1100.0, route="h2o")
    pressure_points = [
        CalibrationPoint(index=1, temperature_c=0.0, humidity_pct=50.0, pressure_hpa=1100.0, route="h2o"),
        CalibrationPoint(index=2, temperature_c=0.0, humidity_pct=50.0, pressure_hpa=800.0, route="h2o"),
        CalibrationPoint(index=3, temperature_c=0.0, humidity_pct=50.0, pressure_hpa=500.0, route="h2o"),
    ]

    result = H2oRouteRunner(service, [lead], pressure_points).execute()

    assert result.success is True
    assert result.completed_point_indices == [1, 2, 3]
    assert result.sampled_point_indices == [1, 2, 3]
    assert result.skipped_point_indices == []
    assert any(
        item
        == "log:Collect-only mode: humidity wait timed out; continue H2O sampling with current generator state "
        "(policy=continue_after_timeout)"
        for item in calls
    )
    assert "cleanup:after H2O humidity timeout" not in calls
    assert "mark_pending" in calls
    assert len(samples) == 3
    assert any(item.startswith("qc:h2o_0c_50rh_1100hpa") for item in calls)
    assert any(item.startswith("qc:h2o_0c_50rh_800hpa") for item in calls)
    assert any(item.startswith("qc:h2o_0c_50rh_500hpa") for item in calls)


def test_h2o_route_runner_preserves_v1_ordering_contract() -> None:
    calls: list[str] = []
    service, _ = _service(calls)
    lead = CalibrationPoint(index=1, temperature_c=25.0, humidity_pct=50.0, pressure_hpa=900.0, route="h2o")
    pressure = CalibrationPoint(index=2, temperature_c=25.0, humidity_pct=50.0, pressure_hpa=1000.0, route="h2o")

    result = H2oRouteRunner(service, [lead], [pressure]).execute()

    assert result.success is True
    point_tag = "h2o_25c_50rh_1000hpa"
    _assert_subsequence(
        calls,
        [
            "h2o_path:False",
            "prepare_pressure",
            "prepare_humidity",
            "temperature_wait",
            "humidity_wait",
            "capture_temp",
            "open_h2o_route_ready",
            "dewpoint_align",
            "mark_pending",
            "seal",
            f"begin:{point_tag}",
            "target_pressure:2",
            f"stable:{point_tag}",
            "update:sampling",
            f"sample:{point_tag}",
            f"qc:{point_tag}",
            "cleanup:after H2O group complete",
        ],
    )
