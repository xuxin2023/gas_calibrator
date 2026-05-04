from __future__ import annotations

from types import SimpleNamespace

from gas_calibrator.v2.core.models import CalibrationPoint
from gas_calibrator.v2.core.run_state import RunState
from gas_calibrator.v2.core.services.pressure_control_service import PressureControlService


class _Controller:
    def __init__(self, *, output_state: int) -> None:
        self.output_state = output_state

    def get_vent_status(self) -> int:
        return 2

    def get_output_state(self) -> int:
        return self.output_state

    def get_isolation_state(self) -> int:
        return 1

    def vent_status_allows_control(self, status) -> bool:
        return int(status) in {0, 2}

    def describe_vent_status(self, status):
        return {
            "value": int(status),
            "classification": "completed_latched",
            "text": "completed",
            "profile": "OLD_PACE5000",
        }


class _Host:
    def __init__(self) -> None:
        self.trace: list[dict] = []

    def _cfg_get(self, _key, default=None):
        return default

    def _log(self, _message: str) -> None:
        return None

    def _record_route_trace(self, **payload) -> None:
        self.trace.append(dict(payload))


def _service() -> tuple[PressureControlService, RunState, _Host]:
    run_state = RunState()
    host = _Host()
    return PressureControlService(SimpleNamespace(), run_state, host=host), run_state, host


def _seal_context(*, control_started: bool) -> dict:
    return {
        "route": "co2",
        "sealed_source_point_index": 2,
        "final_vent_off_command_sent": True,
        "pressure_hpa": 1973.0,
        "sealed_pressure_hpa": 1973.0,
        "preseal_pressure_peak_hpa": 1979.0,
        "preseal_pressure_last_hpa": 1979.0,
        "preseal_trigger": "pressure_gauge_threshold",
        "preseal_trigger_pressure_hpa": 1979.0,
        "preseal_trigger_threshold_hpa": 1110.0,
        "preseal_final_atmosphere_exit_verified": True,
        "seal_transition_completed": True,
        "seal_transition_status": "verified_closed",
        "seal_open_channels": [],
        "sealed_route_pressure_control_started": control_started,
        "sealed_route_last_controlled_pressure_hpa": 1100.0 if control_started else None,
    }


def test_a1_pressure_gate_blocks_active_output_before_first_sealed_control() -> None:
    service, _run_state, _host = _service()
    point = CalibrationPoint(index=2, temperature_c=20.0, pressure_hpa=1000.0, route="co2")

    result = service._pressure_control_ready_gate(
        _Controller(output_state=1),
        point,
        seal_context=_seal_context(control_started=False),
    )

    assert result.ok is False
    assert "output_state=1(not_idle_before_control)" in result.diagnostics["hard_blockers"]


def test_a1_pressure_gate_allows_active_output_for_continued_sealed_route_control() -> None:
    service, _run_state, _host = _service()
    point = CalibrationPoint(index=2, temperature_c=20.0, pressure_hpa=1000.0, route="co2")

    result = service._pressure_control_ready_gate(
        _Controller(output_state=1),
        point,
        seal_context=_seal_context(control_started=True),
    )

    assert result.ok is True
    assert result.diagnostics["hard_blockers"] == []
    assert "output_state_active_for_continued_sealed_route_control" in result.diagnostics["decision_basis"]
    assert "output_state=1 accepted for continued sealed-route setpoint update" in result.diagnostics["warnings"]
