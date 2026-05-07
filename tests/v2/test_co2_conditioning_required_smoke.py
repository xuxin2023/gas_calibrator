from __future__ import annotations

from types import SimpleNamespace

import pytest

from gas_calibrator.v2.core.orchestrator import WorkflowOrchestrator
from gas_calibrator.v2.core.models import CalibrationPoint


def _ambient_point(idx=1, ppm=1000.0):
    point = CalibrationPoint(
        index=idx,
        temperature_c=20.0,
        co2_ppm=ppm,
        pressure_hpa=None,
        pressure_mode="ambient_open",
        pressure_selection_token="ambient_open",
        route="co2",
    )
    return point


def _sealed_point(idx=2, ppm=1000.0, pressure=800.0):
    return CalibrationPoint(
        index=idx,
        temperature_c=20.0,
        co2_ppm=ppm,
        pressure_hpa=pressure,
        pressure_mode="sealed_controlled",
        pressure_selection_token="800hPa",
        route="co2",
    )


def _make_raw_cfg(smoke_overrides=None):
    cfg = {
        "run001_a2": {
            "engineering_smoke_only": True,
            "not_for_production_readiness": True,
            "not_real_acceptance_evidence": True,
            "no_write": True,
            "default_cutover_to_v2": False,
            "disable_v1": False,
        },
        "workflow": {
            "route_mode": "co2_only",
            "pressure": {},
        },
    }
    if smoke_overrides:
        for key, value in (smoke_overrides or {}).items():
            parts = key.split(".")
            if parts[0] == "run001_a2" and len(parts) > 1:
                cfg["run001_a2"][parts[1]] = value
            elif parts[0] == "workflow" and len(parts) > 1:
                cfg["workflow"][parts[1]] = value
    return cfg


def _make_orchestrator(cfg=None, pressure_vals=None):
    orchestrator = WorkflowOrchestrator.__new__(WorkflowOrchestrator)
    raw_cfg = cfg or _make_raw_cfg()

    class FakeService:
        _raw_cfg = raw_cfg
        no_write_guard = True

    orchestrator.service = FakeService()
    orchestrator._cfg_get = lambda path, default=None: {
        "workflow.pressure.co2_route_conditioning_atmosphere_required": True,
        "workflow.route_mode": raw_cfg["workflow"]["route_mode"],
    }.get(path, default)
    orchestrator._cfg_root = lambda: raw_cfg
    orchestrator._workflow_timing_enabled = lambda: True
    orchestrator._workflow_no_write_guard_active = lambda: True
    vals = (pressure_vals or [])[:]
    orchestrator._a2_high_pressure_pressure_values = (
        lambda pt, pp: [float(v) for v in vals] if vals else [800.0]
    )
    orchestrator._as_float = lambda v: None if v in (None, "") else float(v)
    orchestrator._record_workflow_timing = lambda *a, **kw: None
    return orchestrator


def _make_orchestrator_with_timing(cfg=None, pressure_vals=None):
    orchestrator = WorkflowOrchestrator.__new__(WorkflowOrchestrator)
    raw_cfg = cfg or _make_raw_cfg()

    class FakeService:
        _raw_cfg = raw_cfg
        no_write_guard = True

    orchestrator.service = FakeService()
    orchestrator._cfg_get = lambda path, default=None: {
        "workflow.pressure.co2_route_conditioning_atmosphere_required": True,
        "workflow.route_mode": raw_cfg["workflow"]["route_mode"],
    }.get(path, default)
    orchestrator._cfg_root = lambda: raw_cfg
    orchestrator._workflow_timing_enabled = lambda: True
    orchestrator._workflow_no_write_guard_active = lambda: True
    vals = (pressure_vals or [])[:]
    orchestrator._a2_high_pressure_pressure_values = (
        lambda pt, pp: [float(v) for v in vals] if vals else [800.0]
    )
    orchestrator._as_float = lambda v: None if v in (None, "") else float(v)

    timing_records = []

    def record_timing(event_name, event_type="info", **kwargs):
        timing_records.append({"event_name": event_name, "event_type": event_type, **kwargs})

    orchestrator._record_workflow_timing = record_timing
    orchestrator.timing_records = timing_records
    return orchestrator


# ── smoke gate tests with ambient + 800 pressure_refs ──

def test_smoke_gate_flags_and_ambient_plus_800_returns_true():
    orchestrator = _make_orchestrator(pressure_vals=[800.0])
    ambient = _ambient_point()
    sealed = _sealed_point(pressure=800.0)
    result = orchestrator._a2_co2_smoke_conditioning_required(
        point=ambient, pressure_points=[ambient, sealed]
    )
    assert result is True


def test_smoke_gate_integrated_route_conditioning_required():
    orchestrator = _make_orchestrator(pressure_vals=[800.0])
    ambient = _ambient_point()
    sealed = _sealed_point(pressure=800.0)
    result = orchestrator._a2_co2_route_conditioning_required(
        point=ambient, pressure_points=[ambient, sealed]
    )
    assert result is True


def test_smoke_gate_without_ambient_returns_false():
    orchestrator = _make_orchestrator(pressure_vals=[800.0])
    sealed_only = [_sealed_point(pressure=800.0)]
    result = orchestrator._a2_co2_smoke_conditioning_required(
        point=sealed_only[0], pressure_points=sealed_only
    )
    assert result is False


def test_smoke_gate_without_800hpa_returns_false():
    orchestrator = _make_orchestrator(pressure_vals=[800.0])
    ambient = _ambient_point()
    result = orchestrator._a2_co2_smoke_conditioning_required(
        point=ambient, pressure_points=[ambient]
    )
    assert result is False


def test_smoke_gate_false_when_cutover_to_v2():
    cfg = _make_raw_cfg({"run001_a2.default_cutover_to_v2": True})
    orchestrator = _make_orchestrator(cfg=cfg)
    ambient = _ambient_point()
    sealed = _sealed_point(pressure=800.0)
    result = orchestrator._a2_co2_smoke_conditioning_required(
        point=ambient, pressure_points=[ambient, sealed]
    )
    assert result is False


def test_smoke_gate_false_when_disable_v1():
    cfg = _make_raw_cfg({"run001_a2.disable_v1": True})
    orchestrator = _make_orchestrator(cfg=cfg)
    ambient = _ambient_point()
    sealed = _sealed_point(pressure=800.0)
    result = orchestrator._a2_co2_smoke_conditioning_required(
        point=ambient, pressure_points=[ambient, sealed]
    )
    assert result is False


def test_smoke_gate_false_when_no_write_false():
    cfg = _make_raw_cfg({"run001_a2.no_write": False})
    orchestrator = _make_orchestrator(cfg=cfg)
    ambient = _ambient_point()
    sealed = _sealed_point(pressure=800.0)
    result = orchestrator._a2_co2_smoke_conditioning_required(
        point=ambient, pressure_points=[ambient, sealed]
    )
    assert result is False


def test_smoke_gate_false_when_route_mode_is_not_co2_only():
    cfg = _make_raw_cfg({"workflow.route_mode": "full"})
    orchestrator = _make_orchestrator(cfg=cfg)
    ambient = _ambient_point()
    sealed = _sealed_point(pressure=800.0)
    result = orchestrator._a2_co2_smoke_conditioning_required(
        point=ambient, pressure_points=[ambient, sealed]
    )
    assert result is False


# ── 1100hPa original path still works ──

def test_original_1100_path_works():
    orchestrator = WorkflowOrchestrator.__new__(WorkflowOrchestrator)
    raw_cfg = {
        "run001_a2": {"scope": "run001_a2_co2_no_write_pressure_sweep", "no_write": True},
    }

    class FakeSvc:
        _raw_cfg = raw_cfg
        no_write_guard = True

    orchestrator.service = FakeSvc()
    orchestrator._cfg_get = lambda path, default=None: {
        "workflow.pressure.co2_route_conditioning_atmosphere_required": True,
    }.get(path, default)
    orchestrator._cfg_root = lambda: raw_cfg
    orchestrator._workflow_timing_enabled = lambda: True
    orchestrator._workflow_no_write_guard_active = lambda: True
    orchestrator._a2_high_pressure_pressure_values = lambda pt, pp: [1100.0]
    orchestrator._as_float = lambda v: None if v in (None, "") else float(v)
    orchestrator._record_workflow_timing = lambda *a, **kw: None

    point = _sealed_point(pressure=1100.0)
    result = orchestrator._a2_co2_route_conditioning_required(point, [point])
    assert result is True


# ── timing record tests ──

def test_smoke_gate_records_timing_with_reason():
    orchestrator = _make_orchestrator_with_timing(pressure_vals=[800.0])
    ambient = _ambient_point()
    sealed = _sealed_point(pressure=800.0)
    result = orchestrator._a2_co2_smoke_conditioning_required(
        point=ambient, pressure_points=[ambient, sealed]
    )
    assert result is True
    gate_records = [r for r in orchestrator.timing_records if r["event_name"] == "co2_smoke_conditioning_gate"]
    assert len(gate_records) >= 1
    last = gate_records[-1]
    assert last["decision"] == "engineering_smoke_conditioning_required"
    rs = last.get("route_state") or {}
    assert rs.get("reason") == "engineering_smoke_conditioning_required"
    assert rs.get("smoke_pressure_refs_include_ambient_open") is True
    assert rs.get("smoke_pressure_refs_include_800hpa") is True


def test_smoke_gate_missing_ambient_records_insufficient():
    orchestrator = _make_orchestrator_with_timing(pressure_vals=[800.0])
    sealed_only = [_sealed_point(pressure=800.0)]
    result = orchestrator._a2_co2_smoke_conditioning_required(
        point=sealed_only[0], pressure_points=sealed_only
    )
    assert result is False
    gate_records = [r for r in orchestrator.timing_records if r["event_name"] == "co2_smoke_conditioning_gate"]
    assert len(gate_records) >= 1
    last = gate_records[-1]
    assert last["decision"] == "smoke_pressure_refs_insufficient"
    rs = last.get("route_state") or {}
    assert rs.get("reason") == "smoke_pressure_refs_insufficient"
    assert rs.get("smoke_pressure_refs_include_ambient_open") is False


# ── callbacks registration tests ──

def _make_orchestrator_with_a2_hooks():
    orchestrator = WorkflowOrchestrator.__new__(WorkflowOrchestrator)
    hooks = SimpleNamespace(callbacks={})
    orchestrator.a2_hooks = hooks
    orchestrator.conditioning_service = SimpleNamespace()

    def noop(*args, **kwargs):
        return {}

    orchestrator._mark_a2_co2_route_open_command_write_started = noop
    orchestrator._mark_a2_co2_route_open_command_write_completed = noop
    orchestrator._refresh_a2_co2_conditioning_after_route_open = noop
    orchestrator._fail_a2_route_open_transition_if_blocked = noop
    orchestrator._wait_a2_co2_route_open_settle_before_conditioning = noop
    orchestrator._complete_a2_co2_route_open_transition = noop
    orchestrator._record_a2_conditioning_workflow_timing = noop
    return orchestrator


def test_callbacks_registered_after_populate():
    orchestrator = _make_orchestrator_with_a2_hooks()
    orchestrator._populate_a2_hooks_callbacks()
    callbacks = orchestrator.a2_hooks.callbacks
    expected = [
        "mark_route_open_started",
        "mark_route_open_completed",
        "refresh_after_route_open",
        "fail_route_open_transition",
        "wait_route_open_settle",
        "complete_route_open_transition",
        "record_a2_conditioning_workflow_timing",
    ]
    for name in expected:
        assert name in callbacks, f"callback {name} not registered"
        assert callable(callbacks[name]), f"callback {name} is not callable"


def test_populate_then_begin_conditioning_callbacks_remain():
    cfg = _make_raw_cfg()
    raw_cfg = cfg

    class FakeSvc:
        _raw_cfg = raw_cfg
        no_write_guard = True

    orchestrator = WorkflowOrchestrator.__new__(WorkflowOrchestrator)
    orchestrator.service = FakeSvc()
    orchestrator.a2_hooks = SimpleNamespace(
        callbacks={},
        high_pressure_first_point_mode_enabled=False,
        high_pressure_first_point_context={},
        high_pressure_first_point_initial_decision="",
        high_pressure_first_point_vent_preclosed=False,
        co2_route_conditioning_completed=False,
        co2_route_conditioning_completed_at="",
        co2_route_conditioning_at_atmosphere_active=False,
        co2_route_conditioning_at_atmosphere_context={},
        preseal_last_pressure_hpa=None,
        preseal_pressure_rise_detected=False,
        co2_route_open_pressure_hpa=None,
        co2_route_open_monotonic_s=None,
        route_open_pressure_first_sample_recorded=False,
        preseal_analyzer_gate_passed=False,
    )
    orchestrator._cfg_get = lambda path, default=None: {
        "workflow.pressure.co2_route_conditioning_atmosphere_required": True,
        "workflow.route_mode": "co2_only",
    }.get(path, default)
    orchestrator._cfg_root = lambda: raw_cfg
    orchestrator._workflow_timing_enabled = lambda: True
    orchestrator._workflow_no_write_guard_active = lambda: True
    orchestrator._a2_high_pressure_pressure_values = lambda pt, pp: [800.0]
    orchestrator._record_workflow_timing = lambda *a, **kw: None
    orchestrator._as_float = lambda v: None if v in (None, "") else float(v)
    orchestrator._co2_conditioning_soak_s = lambda pt: 300.0

    orchestrator._a2_conditioning_pressure_source_mode = lambda: "continuous"

    vent_calls = []

    class FakePCS:
        @staticmethod
        def _start_a2_high_pressure_digital_gauge_stream(**kw):
            return {"stream_started": True}

        @staticmethod
        def set_pressure_controller_vent(vent_on, **kw):
            vent_calls.append({"vent_on": vent_on, **kw})
            return {"output_state": 0, "isolation_state": 1, "vent_status_raw": 1}

        @staticmethod
        def set_pressure_controller_vent_fast_reassert(vent_on, **kw):
            vent_calls.append({"vent_on": vent_on, "fast_reassert": True, **kw})
            return {
                "fast_vent_reassert_supported": True,
                "fast_vent_reassert_used": True,
                "vent_command_write_started_monotonic_s": 100.0,
                "vent_command_write_completed_monotonic_s": 100.001,
                "vent_command_write_duration_ms": 1.0,
                "vent_command_total_duration_ms": 1.0,
                "command_result": "ok",
                "command_error": "",
            }

    orchestrator.pressure_control_service = FakePCS()

    orchestrator._log = lambda msg: None
    orchestrator._co2_conditioning_soak_s = lambda pt: 300.0
    orchestrator._set_pressure_controller_vent = lambda vent_on, **kw: (
        vent_calls.append({"vent_on": vent_on, **kw})
        or {"output_state": 0, "isolation_state": 1, "vent_status_raw": 1}
    )

    from gas_calibrator.v2.core.services.conditioning_service import ConditioningService
    orchestrator.conditioning_service = ConditioningService(host=orchestrator)

    orchestrator._populate_a2_hooks_callbacks()

    callbacks = orchestrator.a2_hooks.callbacks
    expected = [
        "mark_route_open_started",
        "mark_route_open_completed",
        "refresh_after_route_open",
        "fail_route_open_transition",
        "wait_route_open_settle",
        "complete_route_open_transition",
        "record_a2_conditioning_workflow_timing",
    ]
    for name in expected:
        assert name in callbacks, f"callback {name} missing after populate"
        assert callable(callbacks[name]), f"callback {name} not callable"

    ambient = _ambient_point()
    sealed = _sealed_point(pressure=800.0)

    begin_result = orchestrator.conditioning_service._begin_a2_co2_route_conditioning_at_atmosphere(
        ambient, [ambient, sealed]
    )

    assert orchestrator.a2_hooks.co2_route_conditioning_at_atmosphere_active is True
    assert isinstance(orchestrator.a2_hooks.co2_route_conditioning_at_atmosphere_context, dict)
    ctx = orchestrator.a2_hooks.co2_route_conditioning_at_atmosphere_context
    assert ctx.get("vent_ticks") is not None

    for name in expected:
        assert name in callbacks, f"callback {name} missing after begin conditioning"
        assert callable(callbacks[name]), f"callback {name} not callable"


def test_refresh_after_route_open_callback_is_callable():
    orchestrator = _make_orchestrator_with_a2_hooks()
    orchestrator._populate_a2_hooks_callbacks()
    cb = orchestrator.a2_hooks.callbacks.get("refresh_after_route_open")
    assert callable(cb)
