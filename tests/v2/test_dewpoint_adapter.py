from __future__ import annotations

from types import SimpleNamespace

import pytest

from gas_calibrator.v2.core.models import CalibrationPoint
from gas_calibrator.v2.core.run_state import RunState
from gas_calibrator.v2.core.services.dewpoint_alignment_service import DewpointAlignmentService


def _make_host(legacy_vent=None, legacy_h2o_path=None, pcs_vent=None, vrs_h2o_path=None):
    logs = []
    host = SimpleNamespace()
    host._logs = logs
    host._log = lambda msg: logs.append(msg)

    if legacy_vent:
        host._set_pressure_controller_vent = legacy_vent
    if legacy_h2o_path:
        host._set_h2o_path = legacy_h2o_path

    if pcs_vent is not None or vrs_h2o_path is not None:
        host.pressure_control_service = SimpleNamespace()
        if pcs_vent:
            host.pressure_control_service.set_pressure_controller_vent = pcs_vent
        host.valve_routing_service = SimpleNamespace()
        if vrs_h2o_path:
            host.valve_routing_service.set_h2o_path = vrs_h2o_path

    host._device = lambda name: None
    host._check_stop = lambda: None
    host._cfg_get = lambda path, default=None: default
    host._collect_only_fast_path_enabled = lambda: False
    host._normalize_snapshot = lambda d: d
    return host


class _DummyContext:
    pass


def _pt(index=1):
    return CalibrationPoint(index=index, temperature_c=20.0, pressure_hpa=1000.0, route="h2o")


def test_legacy_set_h2o_path_called():
    called_with = {}
    def legacy(is_open, point):
        called_with["is_open"] = is_open
        called_with["point"] = point
        return True
    host = _make_host(legacy_h2o_path=legacy)
    svc = DewpointAlignmentService(_DummyContext(), RunState(), host=host)
    result = svc._set_h2o_path(True, _pt())
    assert result is True
    assert called_with.get("is_open") is True


def test_vrs_fallback_set_h2o_path():
    called_with = {}
    def vrs_set(is_open, point):
        called_with["is_open"] = is_open
        called_with["index"] = point.index
    host = _make_host(vrs_h2o_path=vrs_set)
    svc = DewpointAlignmentService(_DummyContext(), RunState(), host=host)
    result = svc._set_h2o_path(True, _pt())
    assert result is True
    assert called_with.get("is_open") is True


def test_no_adapter_returns_false():
    host = _make_host()
    svc = DewpointAlignmentService(_DummyContext(), RunState(), host=host)
    result = svc._set_h2o_path(True, _pt())
    assert result is False


def test_legacy_vent_called():
    called_with = {}
    def legacy(on, reason=""):
        called_with["on"] = on
        called_with["reason"] = reason
    host = _make_host(legacy_vent=legacy)
    svc = DewpointAlignmentService(_DummyContext(), RunState(), host=host)
    svc._set_pressure_controller_vent(True, reason="test")
    assert called_with.get("on") is True
    assert called_with.get("reason") == "test"


def test_pcs_fallback_vent_called():
    called_with = {}
    def pcs_set(on, reason=""):
        called_with["on"] = on
        called_with["reason"] = reason
    host = _make_host(pcs_vent=pcs_set)
    svc = DewpointAlignmentService(_DummyContext(), RunState(), host=host)
    svc._set_pressure_controller_vent(True, reason="test")
    assert called_with.get("on") is True


def test_legacy_has_priority_over_vrs():
    legacy_called = []
    vrs_called = []
    def legacy(is_open, point): legacy_called.append(is_open); return True
    def vrs_set(is_open, point): vrs_called.append(is_open)
    host = _make_host(legacy_h2o_path=legacy, vrs_h2o_path=vrs_set)
    svc = DewpointAlignmentService(_DummyContext(), RunState(), host=host)
    svc._set_h2o_path(True, _pt())
    assert len(legacy_called) == 1
    assert len(vrs_called) == 0
