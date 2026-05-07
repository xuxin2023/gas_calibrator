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


def _make_a2_hooks() -> SimpleNamespace:
    return SimpleNamespace(
        callbacks={},
        co2_route_conditioning_at_atmosphere_active=False,
        high_pressure_first_point_mode_enabled=False,
    )


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


class _StatusService:
    def __init__(self, calls: list[str]) -> None:
        self._calls = calls
    def check_stop(self) -> None: self._calls.append("check_stop")
    def update_status(self, **kwargs) -> None: self._calls.append(f"update:{kwargs['phase'].value}")
    def begin_point_timing(self, point, *, phase="", point_tag="") -> None: self._calls.append(f"begin:{point_tag}")
    def clear_point_timing(self, point, *, phase="", point_tag="") -> None: self._calls.append(f"clear:{point_tag}")
    def mark_point_stable_for_sampling(self, point, *, phase="", point_tag="") -> None: self._calls.append(f"stable:{point_tag}")
    def log(self, message: str) -> None: self._calls.append(f"log:{message}")
    def record_route_trace(self, **kwargs) -> None: self._calls.append(f"trace:{kwargs.get('action')}:{kwargs.get('result', 'ok')}")


class _TraceStatusService:
    def __init__(self, calls: list[str], payloads: list[dict[str, object]]) -> None:
        self._calls = calls; self._payloads = payloads
    def check_stop(self) -> None: self._calls.append("check_stop")
    def update_status(self, **kwargs) -> None: self._calls.append(f"update:{kwargs['phase'].value}")
    def begin_point_timing(self, point, *, phase="", point_tag="") -> None: self._calls.append(f"begin:{point_tag}")
    def clear_point_timing(self, point, *, phase="", point_tag="") -> None: self._calls.append(f"clear:{point_tag}")
    def mark_point_stable_for_sampling(self, point, *, phase="", point_tag="") -> None: self._calls.append(f"stable:{point_tag}")
    def log(self, message: str) -> None: self._calls.append(f"log:{message}")
    def record_route_trace(self, **kwargs) -> None:
        self._payloads.append(dict(kwargs))
        self._calls.append(f"trace:{kwargs.get('action')}:{kwargs.get('result', 'ok')}")


def test_co2_route_runner_executes_runner_mainline_and_tracks_route_context() -> None:
    calls: list[str] = []
    context = RecordingRouteContext()
    event_bus = EventBus()
    samples: list[object] = []
    event_bus.subscribe(EventType.SAMPLE_COLLECTED, lambda event: samples.append(event.data))
    retry_results = iter([False, True])

    service = SimpleNamespace(
        event_bus=event_bus,
        route_context=context,
        a2_hooks=_make_a2_hooks(),
        route_planner=RoutePlanner(AppConfig.from_dict({}), PointParser()),
        status_service=_StatusService(calls),
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
        sampling_service=SimpleNamespace(sampling_params=lambda phase="": (4, 15),
            sample_point=lambda point, phase="", point_tag="": calls.append(f"sample:{point_tag}") or [SimpleNamespace(point=point, point_tag=point_tag)],
        ),
        qc_service=SimpleNamespace(run_point_qc=lambda point, phase="", point_tag="": calls.append(f"qc:{point_tag}")),
        _wait_co2_route_soak_before_seal=lambda point: calls.append("route_soak") or True,
        _record_workflow_timing=lambda *a, **kw: None,
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
    assert any("pressure retry 1/1" in item for item in calls)
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

    service = SimpleNamespace(
        event_bus=event_bus,
        route_context=context,
        a2_hooks=_make_a2_hooks(),
        route_planner=RoutePlanner(AppConfig.from_dict({}), PointParser()),
        status_service=_StatusService(calls),
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
        sampling_service=SimpleNamespace(sampling_params=lambda phase="": (4, 15),sample_point=lambda point, phase="", point_tag="": calls.append(f"sample:{point_tag}") or []),
        qc_service=SimpleNamespace(run_point_qc=lambda point, phase="", point_tag="": None),
        _wait_co2_route_soak_before_seal=lambda point: calls.append("route_soak") or True,
        _has_special_co2_zero_flush_pending=lambda: True,
        _is_zero_co2_point=lambda point: True,
        _record_workflow_timing=lambda *a, **kw: None,
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

    service = SimpleNamespace(
        event_bus=event_bus,
        route_context=context,
        a2_hooks=_make_a2_hooks(),
        route_planner=RoutePlanner(AppConfig.from_dict({}), PointParser()),
        status_service=_StatusService(calls),
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
        sampling_service=SimpleNamespace(sampling_params=lambda phase="": (4, 15),
            sample_point=lambda point, phase="", point_tag="": calls.append(f"sample:{point_tag}") or [SimpleNamespace(point=point, point_tag=point_tag)],
        ),
        qc_service=SimpleNamespace(run_point_qc=lambda point, phase="", point_tag="": calls.append(f"qc:{point_tag}")),
        _wait_co2_route_soak_before_seal=lambda point: calls.append("route_soak") or True,
        _record_workflow_timing=lambda *a, **kw: None,
        _cfg_get=lambda path, default=None: {"workflow.pressure.co2_reseal_retry_count": 1}.get(path, default),
    )
    source = CalibrationPoint(index=10, temperature_c=25.0, co2_ppm=800.0, pressure_hpa=900.0, route="co2", co2_group="A")
    pressure = CalibrationPoint(index=11, temperature_c=25.0, co2_ppm=800.0, pressure_hpa=1000.0, route="co2", co2_group="A")

    result = Co2RouteRunner(service, source, [pressure]).execute()

    assert result.success is True
    point_tag = "co2_groupa_800ppm_1000hpa"
    _assert_subsequence(calls, [
        "temperature_wait", "capture_temp", "baseline:before CO2 route conditioning",
        "route:10", "route_soak", "seal", f"begin:{point_tag}",
        "target_pressure:11", "log:CO2 800.0 ppm @ 1000.0 hPa: pressure retry 1/1",
        "target_pressure:11", f"stable:{point_tag}", "update:sampling",
        f"sample:{point_tag}", f"qc:{point_tag}", "cleanup:after CO2 source complete",
    ])


def test_co2_route_runner_records_shared_dewpoint_gate_fields_when_enabled() -> None:
    calls: list[str] = []
    trace_payloads: list[dict[str, object]] = []
    context = RecordingRouteContext()
    event_bus = EventBus()

    service = SimpleNamespace(
        event_bus=event_bus,
        route_context=context,
        a2_hooks=_make_a2_hooks(),
        route_planner=RoutePlanner(AppConfig.from_dict({}), PointParser()),
        status_service=_TraceStatusService(calls, trace_payloads),
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
        sampling_service=SimpleNamespace(sampling_params=lambda phase="": (4, 15),
            sample_point=lambda point, phase="", point_tag="": [SimpleNamespace(point=point, point_tag=point_tag)],
        ),
        qc_service=SimpleNamespace(run_point_qc=lambda point, phase="", point_tag="": None),
        _record_workflow_timing=lambda *a, **kw: None,
        _cfg_get=lambda path, default=None: default,
        _gas_route_dewpoint_gate_enabled=lambda: True,
    )

    def wait_route_soak(point):
        service._last_co2_route_dewpoint_gate_summary = {
            "dewpoint_time_to_gate": 205.0, "dewpoint_tail_span_60s": 0.08,
            "dewpoint_tail_slope_60s": 0.001, "dewpoint_rebound_detected": False,
            "flush_gate_status": "pass", "flush_gate_reason": "",
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
    assert any("CO2 preseal dewpoint gate passed" in item for item in calls)


def test_co2_ambient_first_point_calls_set_pressure_controller_vent_on_and_logs_vent_true() -> None:
    calls: list[str] = []
    vent_calls: list[tuple[bool, str]] = []
    trace_payloads: list[dict[str, object]] = []

    def set_pressure_controller_vent(on: bool, *, reason: str = "") -> None:
        vent_calls.append((on, reason))

    service = SimpleNamespace(
        event_bus=EventBus(),
        route_context=RecordingRouteContext(),
        a2_hooks=_make_a2_hooks(),
        route_planner=RoutePlanner(AppConfig.from_dict({}), PointParser()),
        status_service=_TraceStatusService(calls, trace_payloads),
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
            set_pressure_to_target=lambda point: calls.append(f"set_pressure_to_target:{point.index}") or SimpleNamespace(ok=True),
            wait_after_pressure_stable_before_sampling=lambda point: SimpleNamespace(ok=True),
            set_pressure_controller_vent=set_pressure_controller_vent,
        ),
        sampling_service=SimpleNamespace(sampling_params=lambda phase="": (4, 15),
            sample_point=lambda point, phase="", point_tag="": [SimpleNamespace(point=point, point_tag=point_tag)],
        ),
        qc_service=SimpleNamespace(run_point_qc=lambda point, phase="", point_tag="": None),
        _wait_co2_route_soak_before_seal=lambda point: True,
        _record_workflow_timing=lambda *a, **kw: None,
        _cfg_get=lambda path, default=None: default,
    )

    source = CalibrationPoint(index=51, temperature_c=20.0, co2_ppm=0.0, pressure_hpa=800.0, route="co2", co2_group="A")
    ambient = CalibrationPoint(index=52, temperature_c=20.0, co2_ppm=0.0, pressure_hpa=None, pressure_mode="ambient_open", route="co2", co2_group="A")

    result = Co2RouteRunner(service, source, [ambient]).execute()

    assert result.success is True, "ambient-only route should succeed"
    assert len(vent_calls) >= 1, "seal_deferred must call set_pressure_controller_vent(True)"
    assert vent_calls[0] == (True, "CO2 first point ambient: keep atmosphere open")

    deferred_trace = next((t for t in trace_payloads if t.get("action") == "pressure_skip" and t.get("result") == "deferred"), None)
    assert deferred_trace is not None, "deferred pressure_skip trace required"
    assert deferred_trace["target"]["vent_on"] is True
    assert deferred_trace["target"]["pressure_hpa"] is None

    ambient_trace = next((t for t in trace_payloads if t.get("action") == "pressure_skip" and t.get("result") == "skipped"), None)
    assert ambient_trace is not None, "skipped pressure_skip trace for ambient point required"
    assert ambient_trace["target"]["vent_on"] is True

    assert "set_pressure_to_target" not in " ".join(calls), "ambient point must not set_pressure_to_target"
    assert "as_is" not in " ".join(calls), "no vent_on=as_is allowed"


def test_co2_route_runner_workflow_validation_error_fail_closed_with_skipped_indices() -> None:
    calls: list[str] = []
    context = RecordingRouteContext()
    from gas_calibrator.v2.core.runners.co2_route_runner import WorkflowValidationError

    def _validation_fail(point):
        raise WorkflowValidationError("simulated validation failure")

    service = SimpleNamespace(
        event_bus=EventBus(),
        route_context=context,
        a2_hooks=_make_a2_hooks(),
        route_planner=RoutePlanner(AppConfig.from_dict({}), PointParser()),
        status_service=_StatusService(calls),
        temperature_control_service=SimpleNamespace(
            set_temperature_for_point=lambda point, phase="": SimpleNamespace(ok=True),
            capture_temperature_calibration_snapshot=lambda point, route_type="": None,
        ),
        valve_routing_service=SimpleNamespace(
            set_co2_route_baseline=lambda reason="": None,
            set_valves_for_co2=_validation_fail,
            cleanup_co2_route=lambda reason="": calls.append(f"cleanup:{reason}"),
        ),
        pressure_control_service=SimpleNamespace(
            pressurize_and_hold=lambda point, route="co2": SimpleNamespace(ok=True),
            set_pressure_to_target=lambda point: SimpleNamespace(ok=True),
            wait_after_pressure_stable_before_sampling=lambda point: SimpleNamespace(ok=True),
        ),
        sampling_service=SimpleNamespace(sampling_params=lambda phase="": (4, 15),sample_point=lambda point, phase="", point_tag="": []),
        qc_service=SimpleNamespace(run_point_qc=lambda point, phase="", point_tag="": None),
        _wait_co2_route_soak_before_seal=lambda point: True,
        _record_workflow_timing=lambda *a, **kw: None,
        _cfg_get=lambda path, default=None: default,
    )

    source = CalibrationPoint(index=60, temperature_c=20.0, co2_ppm=500.0, pressure_hpa=800.0, route="co2", co2_group="A")

    result = Co2RouteRunner(service, source, [source]).execute()

    assert not result.success
    assert result.skipped_point_indices == [60], "expected_indices must be skipped on validation error"
    assert "CO2 route validation fail-closed" in " ".join(calls)
    assert context.current_route == "", "route_context must be cleared after WorkflowValidationError"


def test_co2_pressure_retry_fallback_only_retries_set_pressure_to_target_without_cleanup() -> None:
    calls: list[str] = []
    context = RecordingRouteContext()
    retry_results = iter([False, False, True])

    def fake_set_pressure_to_target(point):
        calls.append(f"set_pressure_to_target:{point.index}")
        return SimpleNamespace(ok=next(retry_results))

    service = SimpleNamespace(
        event_bus=EventBus(),
        route_context=context,
        a2_hooks=_make_a2_hooks(),
        route_planner=RoutePlanner(AppConfig.from_dict({}), PointParser()),
        status_service=_StatusService(calls),
        temperature_control_service=SimpleNamespace(
            set_temperature_for_point=lambda point, phase="": SimpleNamespace(ok=True),
            capture_temperature_calibration_snapshot=lambda point, route_type="": None,
        ),
        valve_routing_service=SimpleNamespace(
            set_co2_route_baseline=lambda reason="": None,
            set_valves_for_co2=lambda point: None,
            cleanup_co2_route=lambda reason="": calls.append(f"cleanup:{reason}"),
        ),
        pressure_control_service=SimpleNamespace(
            pressurize_and_hold=lambda point, route="co2": SimpleNamespace(ok=True),
            set_pressure_to_target=fake_set_pressure_to_target,
            wait_after_pressure_stable_before_sampling=lambda point: SimpleNamespace(ok=True),
        ),
        sampling_service=SimpleNamespace(sampling_params=lambda phase="": (4, 15),
            sample_point=lambda point, phase="", point_tag="": [SimpleNamespace(point=point, point_tag=point_tag)],
        ),
        qc_service=SimpleNamespace(run_point_qc=lambda point, phase="", point_tag="": None),
        _wait_co2_route_soak_before_seal=lambda point: True,
        _record_workflow_timing=lambda *a, **kw: None,
        _cfg_get=lambda path, default=None: {"workflow.pressure.co2_reseal_retry_count": 3}.get(path, default),
    )

    source = CalibrationPoint(index=70, temperature_c=25.0, co2_ppm=800.0, pressure_hpa=900.0, route="co2", co2_group="A")
    pressure = CalibrationPoint(index=71, temperature_c=25.0, co2_ppm=800.0, pressure_hpa=1000.0, route="co2", co2_group="A")

    result = Co2RouteRunner(service, source, [pressure]).execute()

    assert result.success
    target_calls = [c for c in calls if c.startswith("set_pressure_to_target")]
    assert len(target_calls) == 3, "should retry set_pressure_to_target exactly 3 times"
    cleanup_during_pressure = [c for c in calls if c.startswith("cleanup")]
    assert len(cleanup_during_pressure) == 1, "only final cleanup allowed, no cleanup during retry"
    assert "after CO2 source complete" in cleanup_during_pressure[0]


def test_co2_ambient_plus_800hpa_deferred_seal_uses_sample_point_not_source_point() -> None:
    calls: list[str] = []
    tracers: list[dict[str, object]] = []
    pressurize_and_hold_calls: list[tuple[object, str]] = []
    set_pressure_to_target_calls: list[object] = []

    def recording_pressurize_and_hold(pt, route="co2"):
        pressurize_and_hold_calls.append((pt, route))
        return SimpleNamespace(ok=True)

    def recording_set_pressure_to_target(pt):
        set_pressure_to_target_calls.append(pt)
        calls.append(f"set_pressure_to_target:{pt.index}")
        return SimpleNamespace(ok=True)

    service = SimpleNamespace(
        event_bus=EventBus(),
        route_context=RecordingRouteContext(),
        a2_hooks=_make_a2_hooks(),
        route_planner=RoutePlanner(AppConfig.from_dict({}), PointParser()),
        status_service=_TraceStatusService(calls, tracers),
        temperature_control_service=SimpleNamespace(
            set_temperature_for_point=lambda point, phase="": SimpleNamespace(ok=True),
            capture_temperature_calibration_snapshot=lambda point, route_type="": None,
        ),
        valve_routing_service=SimpleNamespace(
            set_co2_route_baseline=lambda reason="": None,
            set_valves_for_co2=lambda point: None,
            cleanup_co2_route=lambda reason="": calls.append(f"cleanup:{reason}"),
        ),
        pressure_control_service=SimpleNamespace(
            pressurize_and_hold=recording_pressurize_and_hold,
            set_pressure_to_target=recording_set_pressure_to_target,
            wait_after_pressure_stable_before_sampling=lambda point: SimpleNamespace(ok=True),
            set_pressure_controller_vent=lambda on, reason="": None,
        ),
        sampling_service=SimpleNamespace(
            sampling_params=lambda phase="": (4, 15),
            sample_point=lambda point, phase="", point_tag="": [
                SimpleNamespace(point=point, point_tag=point_tag)
            ],
        ),
        qc_service=SimpleNamespace(run_point_qc=lambda point, phase="", point_tag="": None),
        _wait_co2_route_soak_before_seal=lambda point: True,
        _record_workflow_timing=lambda *a, **kw: None,
        _cfg_get=lambda path, default=None: default,
    )

    source = CalibrationPoint(
        index=99, temperature_c=20.0, co2_ppm=1000.0, pressure_hpa=None,
        pressure_mode="ambient_open", route="co2", co2_group="A",
    )
    ambient = CalibrationPoint(
        index=100, temperature_c=20.0, co2_ppm=1000.0, pressure_hpa=None,
        pressure_mode="ambient_open", route="co2", co2_group="A",
    )
    hpa800 = CalibrationPoint(
        index=101, temperature_c=20.0, co2_ppm=1000.0, pressure_hpa=800.0,
        pressure_mode="sealed_controlled", route="co2", co2_group="A",
    )

    result = Co2RouteRunner(service, source, [ambient, hpa800]).execute()

    assert result.success is True, "ambient+800hPa route should succeed"
    assert set(result.completed_point_indices) == {99, 101}

    assert len(pressurize_and_hold_calls) == 1, (
        "ambient+800hPa should call pressurize_and_hold exactly once (deferred seal for 800hPa)"
    )
    seal_call_point, seal_call_route = pressurize_and_hold_calls[0]
    assert seal_call_route == "co2"
    assert seal_call_point is not source, (
        "pressurize_and_hold MUST receive sample_point (800hPa), NOT the CO2 source point"
    )
    assert seal_call_point.index == 101, (
        "pressurize_and_hold must receive the 800hPa sample_point, not the ambient or source index"
    )
    assert seal_call_point.target_pressure_hpa == 800.0, (
        "pressurize_and_hold sample_point must carry target_pressure_hpa=800"
    )

    assert len(set_pressure_to_target_calls) == 1, (
        "only the 800hPa point should call set_pressure_to_target"
    )
    assert set_pressure_to_target_calls[0].index == 101

    set_pressure_source_indices = {c.index for c in set_pressure_to_target_calls}
    assert 100 not in set_pressure_source_indices, (
        "ambient_open point must NOT call set_pressure_to_target"
    )


def test_co2_ambient_plus_800hpa_deferred_seal_logs_correctly() -> None:
    calls: list[str] = []
    tracers: list[dict[str, object]] = []

    service = SimpleNamespace(
        event_bus=EventBus(),
        route_context=RecordingRouteContext(),
        a2_hooks=_make_a2_hooks(),
        route_planner=RoutePlanner(AppConfig.from_dict({}), PointParser()),
        status_service=_TraceStatusService(calls, tracers),
        temperature_control_service=SimpleNamespace(
            set_temperature_for_point=lambda point, phase="": SimpleNamespace(ok=True),
            capture_temperature_calibration_snapshot=lambda point, route_type="": None,
        ),
        valve_routing_service=SimpleNamespace(
            set_co2_route_baseline=lambda reason="": None,
            set_valves_for_co2=lambda point: None,
            cleanup_co2_route=lambda reason="": calls.append(f"cleanup:{reason}"),
        ),
        pressure_control_service=SimpleNamespace(
            pressurize_and_hold=lambda point, route="co2": SimpleNamespace(ok=True),
            set_pressure_to_target=lambda point: SimpleNamespace(ok=True),
            wait_after_pressure_stable_before_sampling=lambda point: SimpleNamespace(ok=True),
            set_pressure_controller_vent=lambda on, reason="": None,
        ),
        sampling_service=SimpleNamespace(
            sampling_params=lambda phase="": (4, 15),
            sample_point=lambda point, phase="", point_tag="": [
                SimpleNamespace(point=point, point_tag=point_tag)
            ],
        ),
        qc_service=SimpleNamespace(run_point_qc=lambda point, phase="", point_tag="": None),
        _wait_co2_route_soak_before_seal=lambda point: True,
        _record_workflow_timing=lambda *a, **kw: None,
        _cfg_get=lambda path, default=None: default,
    )

    source = CalibrationPoint(
        index=110, temperature_c=20.0, co2_ppm=1000.0, pressure_hpa=None,
        pressure_mode="ambient_open", route="co2", co2_group="A",
    )
    ambient = CalibrationPoint(
        index=111, temperature_c=20.0, co2_ppm=1000.0, pressure_hpa=None,
        pressure_mode="ambient_open", route="co2", co2_group="A",
    )
    hpa800 = CalibrationPoint(
        index=112, temperature_c=20.0, co2_ppm=1000.0, pressure_hpa=800.0,
        pressure_mode="sealed_controlled", route="co2", co2_group="A",
    )

    result = Co2RouteRunner(service, source, [ambient, hpa800]).execute()

    assert result.success is True

    deferred_trace = next(
        (t for t in tracers if t.get("action") == "pressure_skip" and t.get("result") == "deferred"), None
    )
    assert deferred_trace is not None
    assert deferred_trace["target"]["vent_on"] is True
    assert "CO2 first point ambient" in str(deferred_trace.get("message", ""))

    skipped_trace = next(
        (t for t in tracers if t.get("action") == "pressure_skip" and t.get("result") == "skipped"), None
    )
    assert skipped_trace is not None
    assert skipped_trace["target"]["vent_on"] is True

    sampled_traces = [t for t in tracers if t.get("action") == "sample_end" and t.get("result") == "ok"]
    assert len(sampled_traces) == 2, "both ambient and 800hPa should be sampled"
