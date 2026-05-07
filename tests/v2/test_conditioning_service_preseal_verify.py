from __future__ import annotations

from types import SimpleNamespace

import pytest

from gas_calibrator.v2.exceptions import WorkflowValidationError
from gas_calibrator.v2.core.orchestrator import WorkflowOrchestrator
from gas_calibrator.v2.core.services.conditioning_service import ConditioningService
from gas_calibrator.v2.core.models import CalibrationPoint


def _make_orchestrator_with_pressure(pressure_hpa, *, sample_age_s=0.1):
    orchestrator = WorkflowOrchestrator.__new__(WorkflowOrchestrator)
    orchestrator.conditioning_service = ConditioningService(host=orchestrator)
    orchestrator.a2_hooks = SimpleNamespace(
        preseal_last_pressure_hpa=None,
        preseal_pressure_rise_detected=False,
    )
    orchestrator._cfg_get = lambda path, default=None: {
        "workflow.pressure.preseal_ready_pressure_hpa": 1110.0,
        "workflow.pressure.preseal_capture_urgent_seal_threshold_hpa": 1150.0,
        "workflow.pressure.preseal_capture_hard_abort_pressure_hpa": 1250.0,
    }.get(path, default)
    orchestrator._as_float = lambda v: None if v in (None, "") else float(v)
    orchestrator._log = lambda msg: None

    timing_events = []

    def sample_reader(*, stage="", point_index=None):
        if pressure_hpa is None:
            return {}
        return {
            "pressure_hpa": pressure_hpa,
            "sample_age_s": sample_age_s,
            "pressure_sample_age_s": sample_age_s,
            "source": "digital_pressure_gauge",
            "pressure_source_used_for_decision": "digital_pressure_gauge_continuous",
        }

    orchestrator.pressure_control_service = SimpleNamespace(
        _current_high_pressure_first_point_sample=sample_reader,
    )
    orchestrator._record_workflow_timing = (
        lambda event_name, event_type="info", **kwargs: timing_events.append(
            {"event_name": event_name, "event_type": event_type, **kwargs}
        )
    )
    return orchestrator, timing_events


def test_method_exists_on_orchestrator():
    orchestrator = WorkflowOrchestrator.__new__(WorkflowOrchestrator)
    orchestrator.conditioning_service = ConditioningService(host=orchestrator)
    orchestrator.a2_hooks = SimpleNamespace()
    orchestrator._cfg_get = lambda path, default=None: {
        "workflow.pressure.preseal_ready_pressure_hpa": 1110.0,
        "workflow.pressure.preseal_capture_urgent_seal_threshold_hpa": 1150.0,
        "workflow.pressure.preseal_capture_hard_abort_pressure_hpa": 1250.0,
    }.get(path, default)
    orchestrator._as_float = lambda v: None if v in (None, "") else float(v)
    orchestrator._log = lambda msg: None

    def sample_reader(**kwargs):
        return {"pressure_hpa": 1009.0}

    orchestrator.pressure_control_service = SimpleNamespace(
        _current_high_pressure_first_point_sample=sample_reader,
    )
    orchestrator._record_workflow_timing = lambda *a, **kw: {}

    assert hasattr(orchestrator, "_verify_co2_preseal_atmosphere_hold_pressure")
    point = CalibrationPoint(index=1, temperature_c=20.0, co2_ppm=1000.0, pressure_hpa=800.0, route="co2")
    result = orchestrator._verify_co2_preseal_atmosphere_hold_pressure(point)
    assert result in {"ok", "positive_preseal_ready_handoff", "positive_preseal_arm_handoff"}


def test_pressure_below_ready_returns_ok():
    orchestrator, timing_events = _make_orchestrator_with_pressure(1009.0)
    point = CalibrationPoint(index=1, temperature_c=20.0, co2_ppm=1000.0, pressure_hpa=800.0, route="co2")
    result = orchestrator.conditioning_service._verify_co2_preseal_atmosphere_hold_pressure(point)
    assert result == "ok"
    assert timing_events
    assert timing_events[-1]["decision"] == "ok"
    assert timing_events[-1]["pressure_hpa"] == 1009.0


def test_pressure_above_ready_returns_handoff():
    orchestrator, timing_events = _make_orchestrator_with_pressure(1112.0)
    point = CalibrationPoint(index=1, temperature_c=20.0, co2_ppm=1000.0, pressure_hpa=800.0, route="co2")
    result = orchestrator.conditioning_service._verify_co2_preseal_atmosphere_hold_pressure(point)
    assert result == "positive_preseal_ready_handoff"
    assert timing_events[-1]["decision"] == "positive_preseal_ready_handoff"


def test_pressure_above_urgent_returns_arm_handoff():
    orchestrator, timing_events = _make_orchestrator_with_pressure(1160.0)
    point = CalibrationPoint(index=1, temperature_c=20.0, co2_ppm=1000.0, pressure_hpa=800.0, route="co2")
    result = orchestrator.conditioning_service._verify_co2_preseal_atmosphere_hold_pressure(point)
    assert result == "positive_preseal_arm_handoff"


def test_pressure_above_hard_abort_raises():
    orchestrator, timing_events = _make_orchestrator_with_pressure(1260.0)
    point = CalibrationPoint(index=1, temperature_c=20.0, co2_ppm=1000.0, pressure_hpa=800.0, route="co2")
    with pytest.raises(WorkflowValidationError):
        orchestrator.conditioning_service._verify_co2_preseal_atmosphere_hold_pressure(point)
    assert timing_events
    assert timing_events[-1]["decision"] == "hard_abort"
    assert timing_events[-1]["event_type"] == "fail"


def test_pressure_unavailable_does_not_attribute_error():
    orchestrator, timing_events = _make_orchestrator_with_pressure(None)
    point = CalibrationPoint(index=1, temperature_c=20.0, co2_ppm=1000.0, pressure_hpa=800.0, route="co2")
    result = orchestrator.conditioning_service._verify_co2_preseal_atmosphere_hold_pressure(point)
    assert result == "ok"
    assert timing_events
    assert timing_events[-1]["decision"] == "pressure_unavailable"


def test_no_vent_no_seal_no_set_pressure():
    orchestrator, _ = _make_orchestrator_with_pressure(1112.0)
    point = CalibrationPoint(index=1, temperature_c=20.0, co2_ppm=1000.0, pressure_hpa=800.0, route="co2")
    result = orchestrator.conditioning_service._verify_co2_preseal_atmosphere_hold_pressure(point)
    assert result == "positive_preseal_ready_handoff"


def _make_orchestrator_for_smoke_check(smoke_overrides=None):
    orchestrator = WorkflowOrchestrator.__new__(WorkflowOrchestrator)
    raw_cfg = {
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
                raw_cfg["run001_a2"][parts[1]] = value
            elif parts[0] == "workflow" and len(parts) > 1:
                raw_cfg["workflow"][parts[1]] = value

    class FakeService:
        def __init__(self):
            self._raw_cfg = raw_cfg

    orchestrator.service = FakeService()
    orchestrator._cfg_get = lambda path, default=None: {
        "workflow.route_mode": raw_cfg["workflow"]["route_mode"],
    }.get(path, default)
    orchestrator._cfg_root = lambda: raw_cfg
    return orchestrator


def _co2_point(idx=1, ppm=1000.0, pressure=800.0):
    return CalibrationPoint(
        index=idx,
        temperature_c=20.0,
        co2_ppm=ppm,
        pressure_hpa=pressure,
        route="co2",
    )


def test_smoke_conditioning_required_returns_true():
    orchestrator = _make_orchestrator_for_smoke_check()
    result = orchestrator._a2_co2_smoke_conditioning_required()
    assert result is True


def test_smoke_conditioning_required_false_without_smoke_flags():
    orchestrator = _make_orchestrator_for_smoke_check({
        "run001_a2.engineering_smoke_only": False,
    })
    result = orchestrator._a2_co2_smoke_conditioning_required()
    assert result is False


def test_smoke_conditioning_required_false_with_non_co2_only():
    orchestrator = _make_orchestrator_for_smoke_check({
        "workflow.route_mode": "full",
    })
    result = orchestrator._a2_co2_smoke_conditioning_required()
    assert result is False


def test_smoke_conditioning_required_false_when_cutover_to_v2():
    orchestrator = _make_orchestrator_for_smoke_check({
        "run001_a2.default_cutover_to_v2": True,
    })
    result = orchestrator._a2_co2_smoke_conditioning_required()
    assert result is False


def test_smoke_conditioning_required_false_when_disable_v1():
    orchestrator = _make_orchestrator_for_smoke_check({
        "run001_a2.disable_v1": True,
    })
    result = orchestrator._a2_co2_smoke_conditioning_required()
    assert result is False


def test_smoke_conditioning_required_false_when_no_write_disabled():
    orchestrator = _make_orchestrator_for_smoke_check({
        "run001_a2.no_write": False,
    })
    result = orchestrator._a2_co2_smoke_conditioning_required()
    assert result is False


def test_original_1100_path_still_works_without_smoke():
    orchestrator = WorkflowOrchestrator.__new__(WorkflowOrchestrator)
    raw_cfg = {
        "run001_a2": {
            "scope": "run001_a2_co2_no_write_pressure_sweep",
            "no_write": True,
        },
    }

    class FakeService:
        def __init__(self):
            self._raw_cfg = raw_cfg
        no_write_guard = True

    orchestrator.service = FakeService()
    orchestrator._cfg_get = lambda path, default=None: {
        "workflow.pressure.co2_route_conditioning_atmosphere_required": True,
    }.get(path, default)
    orchestrator._cfg_root = lambda: raw_cfg
    orchestrator._workflow_timing_enabled = lambda: True
    orchestrator._workflow_no_write_guard_active = lambda: True
    orchestrator._a2_high_pressure_pressure_values = lambda pt, pp: [1100.0]
    orchestrator._a2_co2_smoke_conditioning_required = lambda: False
    orchestrator._as_float = lambda v: None if v in (None, "") else float(v)

    point = _co2_point()
    pressure_points = [_co2_point(2, pressure=1100.0)]
    assert orchestrator._a2_co2_route_conditioning_required(point, pressure_points) is True


def test_smoke_bypass_contains_1100():
    orchestrator = WorkflowOrchestrator.__new__(WorkflowOrchestrator)
    raw_cfg = {
        "run001_a2": {
            "scope": "run001_a2_co2_no_write_pressure_sweep",
            "engineering_smoke_only": True,
            "not_for_production_readiness": True,
            "not_real_acceptance_evidence": True,
            "no_write": True,
            "default_cutover_to_v2": False,
            "disable_v1": False,
        },
        "workflow": {
            "route_mode": "co2_only",
        },
    }

    class FakeService:
        def __init__(self):
            self._raw_cfg = raw_cfg
        no_write_guard = True

    orchestrator.service = FakeService()
    orchestrator._cfg_get = lambda path, default=None: {
        "workflow.pressure.co2_route_conditioning_atmosphere_required": True,
        "workflow.route_mode": "co2_only",
    }.get(path, default)
    orchestrator._cfg_root = lambda: raw_cfg
    orchestrator._workflow_timing_enabled = lambda: True
    orchestrator._workflow_no_write_guard_active = lambda: True
    orchestrator._a2_high_pressure_pressure_values = lambda pt, pp: [800.0]
    orchestrator._as_float = lambda v: None if v in (None, "") else float(v)

    point = _co2_point(pressure=800.0)
    pressure_points = [_co2_point(2, pressure=800.0)]
    assert orchestrator._a2_co2_route_conditioning_required(point, pressure_points) is True
