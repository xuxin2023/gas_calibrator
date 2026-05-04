from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
import subprocess

import pytest

from gas_calibrator.v2.core.models import CalibrationPoint
from gas_calibrator.v2.core.no_write_guard import NoWriteGuard, NoWriteViolation
from gas_calibrator.v2.core.run001_a1_dry_run import (
    RUN001_FAIL,
    build_run001_a1_evidence_payload,
    write_run001_a1_artifacts,
)
from gas_calibrator.v2.core.run_state import RunState
from gas_calibrator.v2.core.services.pressure_control_service import PressureControlService


class FakeController:
    def __init__(self) -> None:
        self.vent_status = 0
        self.output_state = 0
        self.isolation_state = 1
        self.target_hpa = 1000.0
        self.setpoint_readback_hpa = 1000.0
        self.accept_status_2 = True
        self.output_enable_effective = True
        self.in_limits = True
        self.calls: list[tuple[str, object]] = []

    def get_vent_status(self) -> int:
        return int(self.vent_status)

    def describe_vent_status(self, status: int) -> dict[str, object]:
        text = {
            0: "idle",
            1: "in_progress",
            2: "completed_latched",
            3: "trapped_pressure_or_watchlist",
        }.get(int(status), "unknown")
        return {"value": int(status), "classification": text, "text": text, "profile": "OLD_PACE5000"}

    def vent_status_allows_control(self, status: int) -> bool:
        return int(status) == 0 or (int(status) == 2 and self.accept_status_2)

    def get_output_state(self) -> int:
        return int(self.output_state)

    def get_isolation_state(self) -> int:
        return int(self.isolation_state)

    def set_setpoint(self, value: float) -> None:
        self.target_hpa = float(value)
        self.calls.append(("set_setpoint", float(value)))

    def get_setpoint(self) -> float:
        return float(self.setpoint_readback_hpa)

    def enable_control_output(self) -> None:
        if self.output_enable_effective:
            self.output_state = 1
        self.calls.append(("enable_control_output", True))

    def get_in_limits(self) -> tuple[float, int]:
        return float(self.target_hpa), 1 if self.in_limits else 0


class FakeAnalyzer:
    def write(self, data: str) -> str:
        return str(data)


def _point() -> CalibrationPoint:
    return CalibrationPoint(index=1, temperature_c=20.0, co2_ppm=100.0, pressure_hpa=1000.0, route="co2")


def _seal_context(*, include_pressure: bool = True) -> dict[str, object]:
    context: dict[str, object] = {
        "route": "co2",
        "sealed_source_point_index": 1,
        "final_vent_off_command_sent": True,
        "preseal_final_atmosphere_exit_required": True,
        "preseal_final_atmosphere_exit_started": True,
        "preseal_final_atmosphere_exit_verified": True,
        "preseal_final_atmosphere_exit_phase": "preseal_before_full_seal",
        "preseal_final_atmosphere_exit_reason": "vent_exit_verified_before_full_seal",
        "seal_transition_completed": True,
        "seal_transition_status": "verified_closed",
        "seal_transition_reason": "all reported route valves closed before pressure control",
        "seal_open_channels": [],
    }
    if include_pressure:
        context.update(
            {
                "pressure_hpa": 1110.2,
                "sealed_pressure_hpa": 1110.2,
                "preseal_pressure_peak_hpa": 1112.0,
                "preseal_pressure_last_hpa": 1111.0,
                "preseal_trigger": "pressure_gauge_threshold",
                "preseal_trigger_pressure_hpa": 1111.0,
                "preseal_trigger_threshold_hpa": 1110.0,
            }
        )
    return context


def _service(controller: FakeController) -> tuple[PressureControlService, list[dict[str, object]], RunState]:
    traces: list[dict[str, object]] = []
    logs: list[str] = []
    cfg = {
        "workflow.pressure.stabilize_timeout_s": 0.0,
        "workflow.pressure.restabilize_retries": 0,
        "workflow.pressure.restabilize_retry_interval_s": 0.0,
        "workflow.pressure.soft_recover_on_pressure_timeout": False,
        "workflow.pressure_control.setpoint_tolerance_hpa": 0.5,
    }

    class Host(SimpleNamespace):
        def _device(self, *names):
            if "pressure_controller" in names:
                return controller
            return None

        def _cfg_get(self, path: str, default=None):
            return cfg.get(path, default)

        def _call_first(self, device, method_names, *args):
            for name in method_names:
                method = getattr(device, name, None)
                if callable(method):
                    method(*args)
                    return True
            return False

        def _enable_pressure_controller_output(self, reason: str = ""):
            controller.enable_control_output()

        def _log(self, message: str):
            logs.append(message)

        def _as_float(self, value):
            return None if value is None else float(value)

        def _check_stop(self):
            return None

    run_state = RunState()
    host = Host(
        logs=logs,
        status_service=SimpleNamespace(record_route_trace=lambda **kwargs: traces.append(dict(kwargs))),
    )
    return PressureControlService(SimpleNamespace(), run_state, host=host), traces, run_state


def _base_raw_config() -> dict:
    return {
        "run001_a1": {
            "mode": "real_machine_dry_run",
            "no_write": True,
            "co2_only": True,
            "skip_co2_ppm": [0],
            "single_route": True,
            "single_temperature_group": True,
            "allow_real_route": True,
            "allow_real_pressure": True,
            "allow_real_wait": True,
            "allow_real_sample": True,
            "allow_artifact": True,
            "allow_write_coefficients": False,
            "allow_write_zero": False,
            "allow_write_span": False,
            "allow_write_calibration_parameters": False,
            "default_cutover_to_v2": False,
            "disable_v1": False,
            "full_h2o_co2_group": False,
        },
        "workflow": {
            "route_mode": "co2_only",
            "selected_temps_c": [20.0],
            "skip_co2_ppm": [0],
            "sampling": {"count": 10, "stable_count": 10, "interval_s": 1.0},
        },
        "paths": {},
        "features": {"use_v2": True, "simulation_mode": False},
    }


def test_vent_status_2_with_v1_compatible_evidence_warns_without_hard_block() -> None:
    controller = FakeController()
    controller.vent_status = 2
    service, traces, _ = _service(controller)

    result = service._pressure_control_ready_gate(controller, _point(), seal_context=_seal_context())

    assert result.ok is True
    assert result.diagnostics["vent_status_raw"] == 2
    assert result.diagnostics["vent_status_interpreted"]["classification"] == "completed_latched"
    assert result.diagnostics["gate_decision"] == "ready"
    assert result.diagnostics["hard_blockers"] == []
    assert "vent_status=2 observed" in result.diagnostics["warnings"][0]
    assert result.diagnostics["v1_semantic_compatibility"]["vent_status_2_allowed_by_controller"] is True
    assert traces[-1]["result"] == "ok"


def test_vent_status_2_without_pressure_evidence_still_fails() -> None:
    controller = FakeController()
    controller.vent_status = 2
    service, traces, _ = _service(controller)

    result = service._pressure_control_ready_gate(
        controller,
        _point(),
        seal_context=_seal_context(include_pressure=False),
    )

    assert result.ok is False
    assert "pressure_evidence_missing" in result.diagnostics["hard_blockers"]
    assert result.diagnostics["warnings"]
    assert traces[-1]["result"] == "fail"


@pytest.mark.parametrize(
    ("field", "value", "expected"),
    [
        ("vent_status", 1, "vent_status=1(in_progress_after_seal)"),
        ("isolation_state", 0, "isolation_state=0(not_open_before_control)"),
        ("output_state", 1, "output_state=1(not_idle_before_control)"),
    ],
)
def test_dangerous_or_missing_control_evidence_remains_hard_blocked(
    field: str,
    value: int,
    expected: str,
) -> None:
    controller = FakeController()
    setattr(controller, field, value)
    service, _, _ = _service(controller)

    result = service._pressure_control_ready_gate(controller, _point(), seal_context=_seal_context())

    assert result.ok is False
    assert expected in result.diagnostics["hard_blockers"]
    assert result.diagnostics["warnings"] == []


def test_setpoint_not_accepted_fails_after_ready_gate() -> None:
    controller = FakeController()
    controller.vent_status = 2
    controller.setpoint_readback_hpa = 900.0
    service, traces, run_state = _service(controller)
    run_state.pressure.sealed_route = "co2"
    run_state.pressure.sealed_source_point_index = 1
    run_state.pressure.final_vent_off_command_sent = True
    run_state.pressure.preseal_final_atmosphere_exit_verified = True
    run_state.pressure.seal_transition_completed = True
    run_state.pressure.seal_transition_status = "verified_closed"
    run_state.pressure.sealed_pressure_hpa = 1110.0

    result = service.set_pressure_to_target(_point())

    assert result.ok is False
    assert result.error == "Pressure setpoint not accepted"
    assert "setpoint_not_accepted" in result.diagnostics["hard_blockers"]
    assert traces[-1]["action"] == "set_pressure"
    assert traces[-1]["result"] == "fail"


def test_output_not_enabled_fails_after_setpoint() -> None:
    controller = FakeController()
    controller.output_enable_effective = False
    service, _, run_state = _service(controller)
    run_state.pressure.sealed_route = "co2"
    run_state.pressure.sealed_source_point_index = 1
    run_state.pressure.final_vent_off_command_sent = True
    run_state.pressure.preseal_final_atmosphere_exit_verified = True
    run_state.pressure.seal_transition_completed = True
    run_state.pressure.seal_transition_status = "verified_closed"
    run_state.pressure.sealed_pressure_hpa = 1110.0

    result = service.set_pressure_to_target(_point())

    assert result.ok is False
    assert result.error == "Pressure controller output not enabled"
    assert "output_not_enabled" in result.diagnostics["hard_blockers"]


def test_pressure_timeout_still_fails() -> None:
    controller = FakeController()
    controller.in_limits = False
    service, traces, run_state = _service(controller)
    run_state.pressure.sealed_route = "co2"
    run_state.pressure.sealed_source_point_index = 1
    run_state.pressure.final_vent_off_command_sent = True
    run_state.pressure.preseal_final_atmosphere_exit_verified = True
    run_state.pressure.seal_transition_completed = True
    run_state.pressure.seal_transition_status = "verified_closed"
    run_state.pressure.sealed_pressure_hpa = 1110.0

    result = service.set_pressure_to_target(_point())

    assert result.ok is False
    assert result.timed_out is True
    assert result.error == "Pressure stabilize timeout at target 1000.0 hPa"
    assert traces[-1]["result"] == "timeout"


def test_gate_artifact_records_decision_basis_warnings_and_hard_blockers_separately() -> None:
    controller = FakeController()
    controller.vent_status = 2
    service, traces, _ = _service(controller)

    service._pressure_control_ready_gate(controller, _point(), seal_context=_seal_context())
    actual = traces[-1]["actual"]

    assert actual["vent_status_raw"] == 2
    assert actual["gate_decision"] == "ready"
    assert "decision_basis" in actual
    assert actual["warnings"]
    assert actual["hard_blockers"] == []
    assert actual["pressure_gate_policy"]["policy_id"] == "run001_a1_v1_compatible_pressure_ready_gate"


def test_temperature_keep_running_intentional_is_recorded_not_failure(tmp_path: Path) -> None:
    payload = build_run001_a1_evidence_payload(
        _base_raw_config(),
        point_rows=[{"index": 1, "temperature_c": 20.0, "pressure_hpa": 1000.0, "route": "co2", "co2_ppm": 100.0}],
        run_dir=tmp_path,
    )
    written = write_run001_a1_artifacts(tmp_path / "run001", payload)
    summary = json.loads(Path(written["summary"]).read_text(encoding="utf-8"))
    report = Path(written["report"]).read_text(encoding="utf-8")

    assert summary["temperature_chamber_keep_running_intentional"] is True
    assert summary["temperature_chamber_not_closed_is_expected_for_this_run"] is True
    assert summary["temperature_chamber_not_a_failure_reason"] is True
    assert "temperature_chamber_keep_running_intentional: True" in report


def test_attempted_write_count_blocks_a1_payload() -> None:
    guard = NoWriteGuard()
    guard.record_blocked_write(
        device_name="ga01",
        device_type="gas_analyzer",
        method_name="write_coefficients",
        args=(),
        kwargs={},
    )

    payload = build_run001_a1_evidence_payload(
        _base_raw_config(),
        point_rows=[{"index": 1, "temperature_c": 20.0, "pressure_hpa": 1000.0, "route": "co2", "co2_ppm": 100.0}],
        guard=guard,
    )

    assert payload["attempted_write_count"] == 1
    assert payload["final_decision"] == RUN001_FAIL
    assert "attempted_write_count_gt_0" in payload["hard_stop_reasons"]


def test_id_ygas_is_intercepted_by_no_write_guard() -> None:
    guard = NoWriteGuard()
    analyzer = guard.guard_device(FakeAnalyzer(), device_name="ga01", device_type="gas_analyzer")

    with pytest.raises(NoWriteViolation):
        analyzer.write("ID,YGAS,001\r\n")

    assert guard.attempted_write_count == 1
    assert guard.blocked_events[0]["identity_write_command_sent"] is True


def test_run_app_and_v1_production_flow_are_not_touched() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    diff = subprocess.run(
        ["git", "diff", "--name-only"],
        cwd=repo_root,
        check=True,
        text=True,
        capture_output=True,
    ).stdout.splitlines()

    assert "run_app.py" not in diff
    assert "src/gas_calibrator/workflow/runner.py" not in diff
    assert "src/gas_calibrator/devices/pace5000.py" not in diff
