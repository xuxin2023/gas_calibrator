from __future__ import annotations

from types import SimpleNamespace

import pytest

from gas_calibrator.v2.core.models import CalibrationPoint
from gas_calibrator.v2.core.services.pressure_control_service import PressureControlService


class TestH2oPressureDirectVent:

    def test_pressurize_and_hold_accepts_prefer_direct_vent_close(self):
        """v2.1.0 H2O runner calls pressurize_and_hold(prefer_direct_vent_close=True) — must not TypeError."""
        from gas_calibrator.v2.core.services.pressure_control_service import PressureControlService
        import inspect
        sig = inspect.signature(PressureControlService.pressurize_and_hold)
        params = list(sig.parameters.keys())
        assert "prefer_direct_vent_close" in params, "missing prefer_direct_vent_close in pressurize_and_hold"

    def test_set_pressure_controller_vent_has_prefer_direct_command(self):
        import inspect
        sig = inspect.signature(PressureControlService.set_pressure_controller_vent)
        params = list(sig.parameters.keys())
        assert "prefer_direct_command" in params, "missing prefer_direct_command in set_pressure_controller_vent"

    def test_prepare_pressure_for_h2o_isolates(self):
        """v2.1.0: prepare_pressure_for_h2o sets VENT=OFF with prefer_direct_command."""
        import inspect
        src = inspect.getsource(PressureControlService.prepare_pressure_for_h2o)
        assert "prefer_direct_command=True" in src
        assert "vent(False)" in src or "vent_on=False" in src or "set_pressure_controller_vent(False" in src


class TestH2oOrchestratorSetH2oPath:
    def test_orchestrator_has_set_h2o_path(self):
        from gas_calibrator.v2.core.orchestrator import WorkflowOrchestrator
        assert hasattr(WorkflowOrchestrator, "_set_h2o_path")
