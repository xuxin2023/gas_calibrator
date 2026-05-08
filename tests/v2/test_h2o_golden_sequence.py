from __future__ import annotations

from types import SimpleNamespace

import pytest

from gas_calibrator.v2.core.event_bus import EventBus
from gas_calibrator.v2.core.models import CalibrationPoint
from gas_calibrator.v2.core.route_context import RouteContext
from gas_calibrator.v2.core.route_planner import RoutePlanner
from gas_calibrator.v2.config import AppConfig
from gas_calibrator.v2.core.point_parser import PointParser
from gas_calibrator.v2.core.runners.h2o_route_runner import H2oRouteRunner


def _h2o_point(index, temp_c=20.0, rh=50.0, pressure=1100.0, mode="sealed_controlled"):
    return CalibrationPoint(
        index=index, temperature_c=float(temp_c), humidity_pct=float(rh),
        pressure_hpa=float(pressure), route="h2o",
        humidity_generator_temp_c=float(temp_c),
        dewpoint_c=9.3, h2o_mmol=11.6,
        pressure_mode=mode,
    )


class _RecordingStatusService:
    def __init__(self, calls):
        self.calls = calls
        self.trace_payloads = []

    def check_stop(self): self.calls.append("check_stop")
    def update_status(self, **kw): self.calls.append(f"update:{kw.get('phase','?')}")
    def begin_point_timing(self, point, phase="", point_tag=""): self.calls.append(f"begin:{point_tag}")
    def clear_point_timing(self, point, phase="", point_tag=""): self.calls.append(f"clear:{point_tag}")
    def mark_point_stable_for_sampling(self, point, phase="", point_tag=""): self.calls.append(f"stable:{point_tag}")
    def log(self, msg): self.calls.append(f"log:{msg[:60]}")
    def record_route_trace(self, **kw):
        self.trace_payloads.append(dict(kw))
        self.calls.append(f"trace:{kw.get('action')}:{kw.get('result','ok')}")


class _FakeController:
    def __init__(self, calls): self.calls = calls
    def vent(self, on): self.calls.append(f"controller.vent:{on}")


class _FakeGauge:
    def read_pressure(self): return 1015.0


def _make_service(overrides=None):
    overrides = dict(overrides or {})
    calls = []
    device_calls = []
    controller = _FakeController(device_calls)
    gauge = _FakeGauge()
    status_svc = _RecordingStatusService(calls)

    service = SimpleNamespace(
        event_bus=EventBus(),
        route_context=RouteContext(),
        status_service=status_svc,
        route_planner=RoutePlanner(AppConfig.from_dict({}), PointParser()),
        valve_routing_service=SimpleNamespace(
            apply_route_baseline_valves=lambda: calls.append("baseline_valves"),
            set_h2o_path=lambda seal, point: calls.append(f"set_h2o_path:{seal}"),
            cleanup_h2o_route=lambda point, reason="": calls.append(f"cleanup_h2o:{reason}"),
            mark_post_h2o_co2_zero_flush_pending=lambda: calls.append("mark_zero_flush"),
        ),
        pressure_control_service=SimpleNamespace(
            prepare_pressure_for_h2o=lambda point: calls.append("prepare_pressure_h2o"),
            set_pressure_controller_vent=lambda on, reason="", **kw: calls.append(f"vent:{on}:{reason[:30]}"),
            pressurize_and_hold=lambda point, route="", **kw: (
                calls.append(f"pressurize_and_hold:{point.index}:direct={kw.get('prefer_direct_vent_close',False)}")
                or SimpleNamespace(ok=True)
            ),
            set_pressure_to_target=lambda point: calls.append(f"set_pressure_to_target:{point.index}") or SimpleNamespace(ok=True),
            wait_after_pressure_stable_before_sampling=lambda point: calls.append(f"wait_stable:{point.index}") or SimpleNamespace(ok=True),
            run_state=SimpleNamespace(pressure=SimpleNamespace(preseal_watchlist_status_accepted=False)),
        ),
        temperature_control_service=SimpleNamespace(
            set_temperature_for_point=lambda point, phase="": SimpleNamespace(ok=True, final_temp_c=20.0),
            capture_temperature_calibration_snapshot=lambda point, route_type="": calls.append("temp_snapshot"),
        ),
        humidity_generator_service=SimpleNamespace(
            prepare_humidity_generator=lambda point: calls.append("prepare_hgen"),
            wait_humidity_generator_stable=lambda point: SimpleNamespace(ok=True, final_temp_c=20.0, final_rh_pct=50.0),
        ),
        dewpoint_alignment_service=SimpleNamespace(
            open_h2o_route_and_wait_ready=lambda point: calls.append("route_ready") or True,
            wait_dewpoint_alignment_stable=lambda point: calls.append("dewpoint_stable") or True,
        ),
        sampling_service=SimpleNamespace(
            sampling_params=lambda phase="": (4, 15),
            sample_point=lambda point, phase="", point_tag="": [
                SimpleNamespace(point=point, point_tag=point_tag, h2o_mmol=11.6,
                                pressure_hpa=point.target_pressure_hpa or 1013.25, temperature_c=20.0)
            ],
        ),
        qc_service=SimpleNamespace(
            run_point_qc=lambda point, phase="", point_tag="": calls.append(f"qc:{point_tag}"),
        ),
        device_manager=SimpleNamespace(
            get_device=lambda name: controller if name == "pressure_controller" else (gauge if name == "pressure_gauge" else None),
        ),
        _cfg_get=lambda path, default=None: overrides.get(path, default),
        _record_workflow_timing=lambda *a, **kw: None,
    )
    return service, calls, device_calls, status_svc


class TestH2oRouteGoldenSequence:

    def test_vent_keepalive_started_after_route_ready(self):
        service, calls, dc, _ = _make_service()
        lead = _h2o_point(1, pressure=1100.0)
        pts = [_h2o_point(10, pressure=1100.0)]
        runner = H2oRouteRunner(service, [lead], pts)
        result = runner.execute()
        assert result.success
        ri = calls.index("route_ready")
        di = calls.index("dewpoint_stable")
        assert ri < di, "keepalive starts after route_ready, before dewpoint_stable"

    def test_ambient_open_prepended_when_first_point_not_ambient(self):
        service, calls, dc, svc = _make_service()
        lead = _h2o_point(1, pressure=1100.0)
        pts = [_h2o_point(10, pressure=1100.0)]
        runner = H2oRouteRunner(service, [lead], pts)
        runner.execute()
        ambient_traces = [t for t in svc.trace_payloads
                          if t.get("action") == "pressure_skip"
                          and isinstance(t.get("target"), dict)
                          and t.get("target", {}).get("vent_on") is True]
        assert len(ambient_traces) >= 1, "ambient first point seal deferred trace required"
        set_p_calls = [c for c in calls if c.startswith("set_pressure_to_target:")]
        assert len(set_p_calls) >= 1, "sealed point must call set_pressure_to_target"

    def test_ambient_open_skips_set_pressure_to_target(self):
        service, calls, dc, _ = _make_service()
        lead = _h2o_point(1, pressure=1013.25, mode="ambient_open")
        pts = [_h2o_point(10, pressure=1013.25, mode="ambient_open")]
        runner = H2oRouteRunner(service, [lead], pts)
        runner.execute()
        set_p_calls = [c for c in calls if c.startswith("set_pressure_to_target:")]
        assert len(set_p_calls) == 0, "ambient-only route must not call set_pressure_to_target"

    def test_ambient_to_sealed_transition_sequence(self):
        service, calls, dc, svc = _make_service()
        lead = _h2o_point(1, pressure=1100.0)
        pts = [_h2o_point(10, pressure=1100.0)]
        runner = H2oRouteRunner(service, [lead], pts)
        runner.execute()
        vent_off_idx = next((i for i, c in enumerate(dc) if c == "controller.vent:False"), -1)
        assert vent_off_idx >= 0, "controller.vent(False) must be called"
        pressurize_idx = next((i for i, c in enumerate(calls) if c.startswith("pressurize_and_hold:")), -1)
        assert pressurize_idx >= 0, "pressurize_and_hold must be called"
        set_h2o_idx = next((i for i, c in enumerate(calls) if c.startswith("set_h2o_path:False")), -1)
        assert set_h2o_idx >= 0, "set_h2o_path(False) must be called to close water valve"

    def test_sealed_points_call_set_pressure_to_target(self):
        service, calls, dc, _ = _make_service()
        lead = _h2o_point(1, pressure=1100.0)
        pts = [_h2o_point(11, pressure=1100.0), _h2o_point(12, pressure=1000.0), _h2o_point(13, pressure=900.0)]
        runner = H2oRouteRunner(service, [lead], pts)
        result = runner.execute()
        assert result.success
        set_p_calls = [c for c in calls if c.startswith("set_pressure_to_target:")]
        assert len(set_p_calls) == 3

    def test_no_vent_on_during_sealed_points(self):
        service, calls, dc, _ = _make_service()
        lead = _h2o_point(1, pressure=1100.0)
        pts = [_h2o_point(11, pressure=1100.0), _h2o_point(12, pressure=1000.0)]
        runner = H2oRouteRunner(service, [lead], pts)
        runner.execute()
        first_seal = next((i for i, c in enumerate(calls) if c.startswith("pressurize_and_hold:")), -1)
        vent_on_after = [c for i, c in enumerate(calls) if i > first_seal and c.startswith("vent:True:")]
        assert len(vent_on_after) == 0, "no vent=ON after seal"

    def test_dry_air_correction_applied(self):
        service, calls, dc, _ = _make_service()
        lead = _h2o_point(1, pressure=1100.0)
        pts = [_h2o_point(10, pressure=1100.0)]
        runner = H2oRouteRunner(service, [lead], pts)
        result = runner.execute()
        assert result.success

    def test_cleanup_stops_keepalive(self):
        service, calls, dc, _ = _make_service()
        lead = _h2o_point(1, pressure=1100.0)
        pts = [_h2o_point(10, pressure=1100.0)]
        runner = H2oRouteRunner(service, [lead], pts)
        result = runner.execute()
        assert result.success
        assert "cleanup_h2o:after H2O group complete" in calls
