from __future__ import annotations

from types import SimpleNamespace

from gas_calibrator.v2.config import AppConfig
from gas_calibrator.v2.core.event_bus import EventBus, EventType
from gas_calibrator.v2.core.models import CalibrationPoint
from gas_calibrator.v2.core.point_parser import PointParser
from gas_calibrator.v2.core.route_context import RouteContext
from gas_calibrator.v2.core.route_planner import RoutePlanner
from gas_calibrator.v2.core.runners.co2_route_runner import Co2RouteRunner


def _assert_subsequence(calls: list[str], expected: list[str]) -> None:
    position = 0
    for expected_item in expected:
        while position < len(calls) and calls[position] != expected_item:
            position += 1
        assert position < len(calls), f"Missing expected call order item: {expected_item!r} in {calls!r}"
        position += 1


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


def test_co2_route_runner_executes_runner_mainline_and_tracks_route_context() -> None:
    calls: list[str] = []
    context = RecordingRouteContext()
    event_bus = EventBus()
    samples: list[object] = []
    event_bus.subscribe(EventType.SAMPLE_COLLECTED, lambda event: samples.append(event.data))
    retry_results = iter([False, True])

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
        route_context=context,
        route_planner=RoutePlanner(AppConfig.from_dict({}), PointParser()),
        status_service=StatusService(),
        temperature_control_service=SimpleNamespace(
            set_temperature_for_point=lambda point, phase="": calls.append("temperature_wait") or SimpleNamespace(ok=True),
            capture_temperature_calibration_snapshot=lambda point, route_type="": calls.append("capture_temp"),
        ),
        valve_routing_service=SimpleNamespace(
            set_co2_route_baseline=lambda reason="": calls.append(f"baseline:{reason}"),
            set_valves_for_co2=lambda point: calls.append(f"route:{point.index}"),
            cleanup_co2_route=lambda reason="": calls.append(f"cleanup:{reason}"),
        ),
        pressure_control_service=SimpleNamespace(
            pressurize_and_hold=lambda point, route="co2": calls.append("seal") or SimpleNamespace(ok=True),
            set_pressure_to_target=lambda point: calls.append(f"target_pressure:{point.index}") or SimpleNamespace(ok=next(retry_results)),
            wait_after_pressure_stable_before_sampling=lambda point: calls.append(f"sample_hold:{point.index}") or SimpleNamespace(ok=True),
        ),
        sampling_service=SimpleNamespace(
            sample_point=lambda point, phase="", point_tag="": calls.append(f"sample:{point_tag}") or [SimpleNamespace(point=point, point_tag=point_tag)],
        ),
        qc_service=SimpleNamespace(run_point_qc=lambda point, phase="", point_tag="": calls.append(f"qc:{point_tag}")),
        _wait_co2_route_soak_before_seal=lambda point: calls.append("route_soak") or True,
        _cfg_get=lambda path, default=None: {"workflow.pressure.co2_reseal_retry_count": 1}.get(path, default),
    )
    source = CalibrationPoint(index=10, temperature_c=25.0, co2_ppm=800.0, pressure_hpa=900.0, route="co2", co2_group="A")
    pressure = CalibrationPoint(index=11, temperature_c=25.0, co2_ppm=800.0, pressure_hpa=1000.0, route="co2", co2_group="A")

    result = Co2RouteRunner(service, source, [pressure]).execute()

    assert result.success is True
    assert result.completed_point_indices == [11]
    assert result.sampled_point_indices == [11]
    assert result.skipped_point_indices == []
    assert "capture_temp" in calls
    assert "baseline:before CO2 route conditioning" in calls
    assert "route:10" in calls
    assert any(item.startswith("begin:co2_") for item in calls)
    assert any(item.startswith("stable:co2_") for item in calls)
    assert any(item.startswith("qc:co2_") for item in calls)
    assert "trace:wait_temperature:ok" in calls
    assert "trace:wait_route_soak:ok" in calls
    assert "trace:sample_start:ok" in calls
    assert "trace:sample_end:ok" in calls
    assert any("retry within sealed route 1/1" in item for item in calls)
    assert "cleanup:after CO2 source complete" in calls
    assert samples
    assert context.current_route == ""
    assert context.source_point is None
    assert context.active_point is None
    assert any(item["source_point_index"] == 10 for item in context.snapshots)
    assert any(item["active_point_index"] == 11 for item in context.snapshots)
    assert any(str(item["point_tag"]).startswith("co2_") for item in context.snapshots)
    assert any(item["retry"] == 1 for item in context.snapshots)


def test_co2_route_runner_reasserts_route_after_post_h2o_zero_flush_and_clears_active_flag_on_seal_failure() -> None:
    calls: list[str] = []
    context = RecordingRouteContext()
    event_bus = EventBus()
    humidity_state = SimpleNamespace(active_post_h2o_co2_zero_flush=True)

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
        route_context=context,
        route_planner=RoutePlanner(AppConfig.from_dict({}), PointParser()),
        status_service=StatusService(),
        run_state=SimpleNamespace(humidity=humidity_state),
        _active_post_h2o_co2_zero_flush=True,
        temperature_control_service=SimpleNamespace(
            set_temperature_for_point=lambda point, phase="": calls.append("temperature_wait") or SimpleNamespace(ok=True),
            capture_temperature_calibration_snapshot=lambda point, route_type="": calls.append("capture_temp"),
        ),
        valve_routing_service=SimpleNamespace(
            set_co2_route_baseline=lambda reason="": calls.append(f"baseline:{reason}"),
            set_valves_for_co2=lambda point: calls.append(f"route:{point.index}"),
            cleanup_co2_route=lambda reason="": calls.append(f"cleanup:{reason}"),
        ),
        pressure_control_service=SimpleNamespace(
            pressurize_and_hold=lambda point, route="co2": calls.append("seal") or SimpleNamespace(ok=False),
            set_pressure_to_target=lambda point: calls.append(f"target_pressure:{point.index}") or SimpleNamespace(ok=True),
            wait_after_pressure_stable_before_sampling=lambda point: calls.append(f"sample_hold:{point.index}") or SimpleNamespace(ok=True),
        ),
        sampling_service=SimpleNamespace(sample_point=lambda point, phase="", point_tag="": calls.append(f"sample:{point_tag}") or []),
        qc_service=SimpleNamespace(run_point_qc=lambda point, phase="", point_tag="": None),
        _wait_co2_route_soak_before_seal=lambda point: calls.append("route_soak") or True,
        _has_special_co2_zero_flush_pending=lambda: True,
        _is_zero_co2_point=lambda point: True,
        _cfg_get=lambda path, default=None: default,
    )
    source = CalibrationPoint(index=12, temperature_c=25.0, co2_ppm=0.0, pressure_hpa=1100.0, route="co2", co2_group="A")

    result = Co2RouteRunner(service, source, [source]).execute()

    assert result.success is False
    assert result.completed_point_indices == []
    assert result.sampled_point_indices == []
    assert result.skipped_point_indices == [12]
    assert "baseline:before CO2 route conditioning" in calls
    assert "baseline:before CO2 pressure-seal recharge" in calls
    assert calls.count("route:12") == 2
    assert any("reassert route before pressure sealing" in item for item in calls)
    assert "cleanup:after CO2 pressure-seal failure" in calls
    assert humidity_state.active_post_h2o_co2_zero_flush is False
    assert service._active_post_h2o_co2_zero_flush is False


def test_co2_route_runner_preserves_v1_ordering_contract() -> None:
    calls: list[str] = []
    context = RecordingRouteContext()
    event_bus = EventBus()
    retry_results = iter([False, True])

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
        route_context=context,
        route_planner=RoutePlanner(AppConfig.from_dict({}), PointParser()),
        status_service=StatusService(),
        temperature_control_service=SimpleNamespace(
            set_temperature_for_point=lambda point, phase="": calls.append("temperature_wait") or SimpleNamespace(ok=True),
            capture_temperature_calibration_snapshot=lambda point, route_type="": calls.append("capture_temp"),
        ),
        valve_routing_service=SimpleNamespace(
            set_co2_route_baseline=lambda reason="": calls.append(f"baseline:{reason}"),
            set_valves_for_co2=lambda point: calls.append(f"route:{point.index}"),
            cleanup_co2_route=lambda reason="": calls.append(f"cleanup:{reason}"),
        ),
        pressure_control_service=SimpleNamespace(
            pressurize_and_hold=lambda point, route="co2": calls.append("seal") or SimpleNamespace(ok=True),
            set_pressure_to_target=lambda point: calls.append(f"target_pressure:{point.index}") or SimpleNamespace(ok=next(retry_results)),
            wait_after_pressure_stable_before_sampling=lambda point: calls.append(f"sample_hold:{point.index}") or SimpleNamespace(ok=True),
        ),
        sampling_service=SimpleNamespace(
            sample_point=lambda point, phase="", point_tag="": calls.append(f"sample:{point_tag}") or [SimpleNamespace(point=point, point_tag=point_tag)],
        ),
        qc_service=SimpleNamespace(run_point_qc=lambda point, phase="", point_tag="": calls.append(f"qc:{point_tag}")),
        _wait_co2_route_soak_before_seal=lambda point: calls.append("route_soak") or True,
        _cfg_get=lambda path, default=None: {"workflow.pressure.co2_reseal_retry_count": 1}.get(path, default),
    )
    source = CalibrationPoint(index=10, temperature_c=25.0, co2_ppm=800.0, pressure_hpa=900.0, route="co2", co2_group="A")
    pressure = CalibrationPoint(index=11, temperature_c=25.0, co2_ppm=800.0, pressure_hpa=1000.0, route="co2", co2_group="A")

    result = Co2RouteRunner(service, source, [pressure]).execute()

    assert result.success is True
    point_tag = "co2_groupa_800ppm_1000hpa"
    _assert_subsequence(
        calls,
        [
            "temperature_wait",
            "capture_temp",
            "baseline:before CO2 route conditioning",
            "route:10",
            "route_soak",
            "seal",
            f"begin:{point_tag}",
            "target_pressure:11",
            "log:CO2 800.0 ppm @ 1000.0 hPa timeout; retry within sealed route 1/1",
            "target_pressure:11",
            f"stable:{point_tag}",
            "update:sampling",
            f"sample:{point_tag}",
            f"qc:{point_tag}",
            "cleanup:after CO2 source complete",
        ],
    )


def test_co2_route_runner_records_shared_dewpoint_gate_fields_when_enabled() -> None:
    calls: list[str] = []
    trace_payloads: list[dict[str, object]] = []
    context = RecordingRouteContext()
    event_bus = EventBus()

    class StatusService:
        def check_stop(self): calls.append("check_stop")
        def update_status(self, **kwargs): calls.append(f"update:{kwargs['phase'].value}")
        def begin_point_timing(self, point, *, phase="", point_tag=""): calls.append(f"begin:{point_tag}")
        def clear_point_timing(self, point, *, phase="", point_tag=""): calls.append(f"clear:{point_tag}")
        def mark_point_stable_for_sampling(self, point, *, phase="", point_tag=""): calls.append(f"stable:{point_tag}")
        def log(self, message: str): calls.append(f"log:{message}")
        def record_route_trace(self, **kwargs):
            trace_payloads.append(dict(kwargs))
            calls.append(f"trace:{kwargs.get('action')}:{kwargs.get('result', 'ok')}")

    service = SimpleNamespace(
        event_bus=event_bus,
        route_context=context,
        route_planner=RoutePlanner(AppConfig.from_dict({}), PointParser()),
        status_service=StatusService(),
        temperature_control_service=SimpleNamespace(
            set_temperature_for_point=lambda point, phase="": SimpleNamespace(ok=True),
            capture_temperature_calibration_snapshot=lambda point, route_type="": None,
        ),
        valve_routing_service=SimpleNamespace(
            set_co2_route_baseline=lambda reason="": None,
            set_valves_for_co2=lambda point: None,
            cleanup_co2_route=lambda reason="": None,
        ),
        pressure_control_service=SimpleNamespace(
            pressurize_and_hold=lambda point, route="co2": SimpleNamespace(ok=True),
            set_pressure_to_target=lambda point: SimpleNamespace(ok=True),
            wait_after_pressure_stable_before_sampling=lambda point: SimpleNamespace(ok=True),
        ),
        sampling_service=SimpleNamespace(
            sample_point=lambda point, phase="", point_tag="": [SimpleNamespace(point=point, point_tag=point_tag)],
        ),
        qc_service=SimpleNamespace(run_point_qc=lambda point, phase="", point_tag="": None),
        _cfg_get=lambda path, default=None: default,
        _gas_route_dewpoint_gate_enabled=lambda: True,
    )

    def wait_route_soak(point):
        service._last_co2_route_dewpoint_gate_summary = {
            "dewpoint_time_to_gate": 205.0,
            "dewpoint_tail_span_60s": 0.08,
            "dewpoint_tail_slope_60s": 0.001,
            "dewpoint_rebound_detected": False,
            "flush_gate_status": "pass",
            "flush_gate_reason": "",
        }
        return True

    service._wait_co2_route_soak_before_seal = wait_route_soak

    source = CalibrationPoint(index=20, temperature_c=25.0, co2_ppm=0.0, pressure_hpa=900.0, route="co2", co2_group="A")
    pressure = CalibrationPoint(index=21, temperature_c=25.0, co2_ppm=0.0, pressure_hpa=1000.0, route="co2", co2_group="A")

    result = Co2RouteRunner(service, source, [pressure]).execute()

    assert result.success is True
    wait_route_trace = next(item for item in trace_payloads if item.get("action") == "wait_route_soak")
    assert wait_route_trace["result"] == "ok"
    assert wait_route_trace["actual"]["flush_gate_status"] == "pass"
    assert wait_route_trace["actual"]["dewpoint_time_to_gate"] == 205.0
    assert wait_route_trace["actual"]["dewpoint_tail_span_60s"] == 0.08
    assert wait_route_trace["actual"]["dewpoint_tail_slope_60s"] == 0.001
    assert wait_route_trace["actual"]["dewpoint_rebound_detected"] is False
    assert any("CO2 preseal dewpoint gate passed" in item for item in calls)
