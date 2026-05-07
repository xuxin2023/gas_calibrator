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
    vent_off_calls: list[tuple[bool, str]] = []
    apply_valve_state_calls: list[list[int]] = []
    set_pressure_to_target_calls: list[object] = []

    def recording_set_pressure_controller_vent(on, reason="", **kw):
        vent_off_calls.append((on, reason))
        return {}

    def recording_apply_valve_states(open_valves):
        apply_valve_state_calls.append(list(open_valves))
        return {"relay_a": {"1": False}, "relay_b": {"1": False}}

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
            apply_valve_states=recording_apply_valve_states,
        ),
        pressure_control_service=SimpleNamespace(
            pressurize_and_hold=lambda point, route="co2": SimpleNamespace(ok=True),
            set_pressure_to_target=recording_set_pressure_to_target,
            wait_after_pressure_stable_before_sampling=lambda point: SimpleNamespace(ok=True),
            set_pressure_controller_vent=recording_set_pressure_controller_vent,
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
        _cfg_get=lambda path, default=None: (
            0.0 if path == "workflow.pressure.co2_ambient_to_sealed_vent_off_settle_s" else default
        ),
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

    vent_off_calls_before_close = [
        (on, reason) for on, reason in vent_off_calls
        if "vent off before route close" in reason
    ]
    assert len(vent_off_calls_before_close) == 1, (
        "ambient→800 transition must send exactly one VENT=OFF before route close"
    )
    assert vent_off_calls_before_close[0][0] is False, (
        "ambient→800 transition must send VENT=OFF (False)"
    )

    assert len(apply_valve_state_calls) == 1, (
        "ambient→800 transition must call apply_valve_states exactly once"
    )
    assert apply_valve_state_calls[0] == [], (
        "apply_valve_states must be called with empty list to close all valves"
    )

    transition_traces = [
        t for t in tracers
        if t.get("action") == "co2_ambient_to_sealed_transition"
    ]
    assert len(transition_traces) == 1
    actual = transition_traces[0]["actual"]
    assert actual["target_pressure_hpa"] == 800.0, (
        "transition evidence must record target_pressure_hpa=800"
    )
    assert actual["pressure_read_between_vent_off_and_route_close"] is False
    assert actual["vent_reassert_between_vent_off_and_route_close"] is False
    assert actual["preseal_atmosphere_hold_used"] is False
    assert actual["positive_preseal_used"] is False
    assert actual["sealed_no_vent_guard_active_before_set_pressure"] is True
    assert actual["vent_on_attempt_count_after_route_close"] == 0
    assert actual["vent_on_blocked_count_after_route_close"] == 0
    assert actual["vent_on_command_sent_after_route_close"] is False

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
            apply_valve_states=lambda open_valves: {"relay_a": {}, "relay_b": {}},
        ),
        pressure_control_service=SimpleNamespace(
            pressurize_and_hold=lambda point, route="co2": SimpleNamespace(ok=True),
            set_pressure_to_target=lambda point: SimpleNamespace(ok=True),
            wait_after_pressure_stable_before_sampling=lambda point: SimpleNamespace(ok=True),
            set_pressure_controller_vent=lambda on, reason="", **kw: None,
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
        _cfg_get=lambda path, default=None: (
            0.0 if path == "workflow.pressure.co2_ambient_to_sealed_vent_off_settle_s" else default
        ),
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

    transition_traces = [
        t for t in tracers
        if t.get("action") == "co2_ambient_to_sealed_transition"
    ]
    assert len(transition_traces) == 1, "ambient→800 transition must be recorded"
    actual = transition_traces[0]["actual"]
    assert actual["vent_off_to_route_close_s"] >= 0
    assert actual["target_pressure_hpa"] == 800.0
    assert actual["pressure_read_between_vent_off_and_route_close"] is False
    assert actual["vent_reassert_between_vent_off_and_route_close"] is False


def test_co2_ambient_to_sealed_transition_disables_conditioning_state() -> None:
    calls: list[str] = []
    tracers: list[dict[str, object]] = []
    a2_hooks = _make_a2_hooks()
    a2_hooks.co2_route_conditioning_at_atmosphere_active = True
    a2_hooks.co2_route_conditioning_at_atmosphere_context = {
        "route_conditioning_phase": "conditioning",
        "vent_ticks": 311,
    }

    service = SimpleNamespace(
        event_bus=EventBus(),
        route_context=RecordingRouteContext(),
        a2_hooks=a2_hooks,
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
            apply_valve_states=lambda open_valves: {"relay_a": {}, "relay_b": {}},
        ),
        pressure_control_service=SimpleNamespace(
            pressurize_and_hold=lambda point, route="co2": SimpleNamespace(ok=True),
            set_pressure_to_target=lambda point: SimpleNamespace(ok=True),
            wait_after_pressure_stable_before_sampling=lambda point: SimpleNamespace(ok=True),
            set_pressure_controller_vent=lambda on, reason="", **kw: None,
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
        _cfg_get=lambda path, default=None: (
            0.0 if path == "workflow.pressure.co2_ambient_to_sealed_vent_off_settle_s" else default
        ),
    )

    source = CalibrationPoint(
        index=120, temperature_c=20.0, co2_ppm=1000.0, pressure_hpa=None,
        pressure_mode="ambient_open", route="co2", co2_group="A",
    )
    ambient = CalibrationPoint(
        index=121, temperature_c=20.0, co2_ppm=1000.0, pressure_hpa=None,
        pressure_mode="ambient_open", route="co2", co2_group="A",
    )
    hpa800 = CalibrationPoint(
        index=122, temperature_c=20.0, co2_ppm=1000.0, pressure_hpa=800.0,
        pressure_mode="sealed_controlled", route="co2", co2_group="A",
    )

    result = Co2RouteRunner(service, source, [ambient, hpa800]).execute()

    assert result.success is True

    assert a2_hooks.co2_route_conditioning_at_atmosphere_active is False, (
        "conditioning atmosphere_active must be disabled during ambient→sealed transition"
    )
    ctx = a2_hooks.co2_route_conditioning_at_atmosphere_context
    assert isinstance(ctx, dict)
    assert ctx.get("route_conditioning_phase") == "ready_to_seal_phase", (
        "conditioning context phase must be set to ready_to_seal_phase"
    )

    transition_traces = [
        t for t in tracers
        if t.get("action") == "co2_ambient_to_sealed_transition"
    ]
    assert len(transition_traces) == 1
    actual = transition_traces[0]["actual"]
    assert actual["preseal_atmosphere_hold_used"] is False
    assert actual["positive_preseal_used"] is False


def test_co2_sealed_route_no_vent_guard_blocks_vent_on_after_route_close() -> None:
    calls: list[str] = []
    tracers: list[dict[str, object]] = []
    vent_call_log: list[dict] = []
    route_close_happened: bool = False
    guard_active: bool = False
    ambient_setup_vent_on_seen: bool = False

    class MockPressureControlService:
        def set_pressure_controller_vent(self, on, reason="", **kw):
            nonlocal route_close_happened, guard_active, ambient_setup_vent_on_seen
            phase = "after_route_close" if route_close_happened else "before_route_close"
            entry = {
                "vent_on": on,
                "reason": reason,
                "phase": phase,
                "guard_active": guard_active,
                "command_sent": False,
            }
            if on:
                if guard_active:
                    entry["command_sent"] = False
                    entry["blocked"] = True
                    vent_call_log.append(entry)
                    tracers.append({
                        "action": "sealed_route_vent_on_blocked",
                        "target": {"vent_on": True},
                        "actual": {
                            "vent_command_blocked": True,
                            "blocked_by": "co2_sealed_route_no_vent_guard",
                            "attempted_vent_on_after_route_close": True,
                            "vent_on_command_sent_after_route_close": False,
                            "caller_reason": reason,
                            "phase": phase,
                        },
                        "result": "blocked",
                    })
                    return {
                        "vent_command_blocked": True,
                        "blocked_by": "co2_sealed_route_no_vent_guard",
                        "attempted_vent_on_after_route_close": True,
                        "vent_on_command_sent_after_route_close": False,
                    }
                entry["command_sent"] = True
                if not route_close_happened:
                    ambient_setup_vent_on_seen = True
            vent_call_log.append(entry)
            return {}

        def set_pressure_to_target(self, point):
            nonlocal guard_active
            calls.append(f"set_pressure_to_target:{point.index}")
            self.set_pressure_controller_vent(
                True, reason="redundant vent open during setpoint control"
            )
            return SimpleNamespace(ok=True)

        def wait_after_pressure_stable_before_sampling(self, point):
            return SimpleNamespace(ok=True)

    a2_hooks = _make_a2_hooks()

    def recording_apply_valve_states(open_valves):
        nonlocal route_close_happened, guard_active
        route_close_happened = True
        guard_active = True
        return {"relay_a": {}, "relay_b": {}}

    service = SimpleNamespace(
        event_bus=EventBus(),
        route_context=RecordingRouteContext(),
        a2_hooks=a2_hooks,
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
            apply_valve_states=recording_apply_valve_states,
        ),
        pressure_control_service=MockPressureControlService(),
        sampling_service=SimpleNamespace(
            sampling_params=lambda phase="": (4, 15),
            sample_point=lambda point, phase="", point_tag="": [
                SimpleNamespace(point=point, point_tag=point_tag)
            ],
        ),
        qc_service=SimpleNamespace(run_point_qc=lambda point, phase="", point_tag="": None),
        _wait_co2_route_soak_before_seal=lambda point: True,
        _record_workflow_timing=lambda *a, **kw: None,
        _cfg_get=lambda path, default=None: (
            0.0 if path == "workflow.pressure.co2_ambient_to_sealed_vent_off_settle_s" else default
        ),
    )

    source = CalibrationPoint(
        index=130, temperature_c=20.0, co2_ppm=1000.0, pressure_hpa=None,
        pressure_mode="ambient_open", route="co2", co2_group="A",
    )
    ambient = CalibrationPoint(
        index=131, temperature_c=20.0, co2_ppm=1000.0, pressure_hpa=None,
        pressure_mode="ambient_open", route="co2", co2_group="A",
    )
    hpa800 = CalibrationPoint(
        index=132, temperature_c=20.0, co2_ppm=1000.0, pressure_hpa=800.0,
        pressure_mode="sealed_controlled", route="co2", co2_group="A",
    )

    result = Co2RouteRunner(service, source, [ambient, hpa800]).execute()

    assert result.success is True

    assert ambient_setup_vent_on_seen is True, (
        "ambient_open setup must include VENT=ON"
    )

    before_close_vent_on = [
        e for e in vent_call_log
        if e["vent_on"] is True and e["phase"] == "before_route_close"
    ]
    assert len(before_close_vent_on) == 1, (
        "exactly 1 VENT=ON before route close: ambient_open setup"
    )
    assert before_close_vent_on[0]["command_sent"] is True

    after_close_vent_on = [
        e for e in vent_call_log
        if e["vent_on"] is True and e["phase"] == "after_route_close"
    ]
    assert len(after_close_vent_on) >= 1, (
        "set_pressure_to_target must attempt VENT=ON after route close"
    )
    assert all(e["blocked"] is True for e in after_close_vent_on), (
        "all VENT=ON attempts after route close must be blocked"
    )
    assert all(e["command_sent"] is False for e in after_close_vent_on), (
        "no VENT=ON command must be sent after route close"
    )

    blocked_traces = [
        t for t in tracers
        if t.get("action") == "sealed_route_vent_on_blocked"
    ]
    assert len(blocked_traces) >= 1, (
        "at least 1 sealed_route_vent_on_blocked trace must be recorded"
    )
    assert blocked_traces[0]["result"] == "blocked"

    assert "cleanup:after CO2 source complete" in calls
    assert a2_hooks.co2_sealed_route_no_vent_active is False, (
        "guard must be disarmed after cleanup_co2_route"
    )

    transition_traces = [
        t for t in tracers
        if t.get("action") == "co2_ambient_to_sealed_transition"
    ]
    assert len(transition_traces) == 1
    actual = transition_traces[0]["actual"]
    assert actual["sealed_no_vent_guard_active_before_set_pressure"] is True
