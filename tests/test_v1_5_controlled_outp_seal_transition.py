from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock

from gas_calibrator.data.points import CalibrationPoint
from gas_calibrator.workflow import runner as runner_module
from gas_calibrator.workflow.runner import CalibrationRunner


CONFIG_PATH = Path(
    "configs/site_v1_5_no_write_current_hardware_co2_20c_1000ppm_full_pressure_controlled_outp_skip_tempwait.json"
)


def _co2_point(index: int = 1, pressure: float = 1100.0, ppm: float = 1000.0) -> CalibrationPoint:
    return CalibrationPoint(
        index=index,
        temp_chamber_c=20.0,
        co2_ppm=ppm,
        hgen_temp_c=None,
        hgen_rh_pct=None,
        target_pressure_hpa=pressure,
        dewpoint_c=None,
        h2o_mmol=None,
        raw_h2o=None,
        co2_group=None,
    )


class FakePace:
    VENT_STATUS_IDLE = 0
    VENT_STATUS_COMPLETED = 2
    VENT_STATUS_TRAPPED_PRESSURE = 3

    def __init__(self) -> None:
        self.calls: list[tuple] = []
        self.output_state = 0
        self.isolation_state = 1
        self.vent_status = 0
        self.output_mode = "ACT"
        self.system_error = "0,No error"
        self.in_limits = [(1100.0, 1)]

    def stop_atmosphere_hold(self) -> bool:
        self.calls.append(("stop_hold",))
        return True

    def start_atmosphere_hold(self, interval_s: float = 0.0) -> None:
        self.calls.append(("start_hold", float(interval_s)))

    def is_atmosphere_hold_active(self) -> bool:
        return False

    def set_output(self, on: bool) -> None:
        self.calls.append(("output", bool(on)))
        self.output_state = 1 if on else 0

    def set_isolation_open(self, is_open: bool) -> None:
        self.calls.append(("isolation", bool(is_open)))
        self.isolation_state = 1 if is_open else 0

    def vent(self, on: bool = True) -> None:
        self.calls.append(("vent", bool(on)))
        self.vent_status = 1 if on else 2

    def wait_for_vent_idle(self, *, timeout_s: float = 30.0, poll_s: float = 0.25):
        self.calls.append(("wait_for_vent_idle", float(timeout_s), float(poll_s)))
        return self.vent_status

    def exit_atmosphere_mode(self, *, timeout_s: float = 30.0, poll_s: float = 0.25):
        self.calls.append(("exit_atmosphere_mode", float(timeout_s), float(poll_s)))
        self.output_state = 0
        self.vent_status = 2
        self.isolation_state = 1
        return self.vent_status

    def enable_control_output(self) -> None:
        self.calls.append(("enable_control_output",))
        self.output_state = 1
        self.vent_status = 2

    def set_setpoint(self, value: float) -> None:
        self.calls.append(("setpoint", float(value)))

    def get_output_state(self) -> int:
        return self.output_state

    def get_isolation_state(self) -> int:
        return self.isolation_state

    def get_vent_status(self) -> int:
        return self.vent_status

    def get_output_mode(self) -> str:
        return self.output_mode

    def query(self, command: str) -> str:
        self.calls.append(("query", str(command)))
        if str(command).strip().upper() == ":SYST:ERR?":
            return self.system_error
        return ""

    def vent_status_allows_control(self, status: int) -> bool:
        return int(status) in {0, 2, 3}

    def read_pressure(self) -> float:
        return 1013.0

    def get_in_limits(self):
        if self.in_limits:
            return self.in_limits.pop(0)
        return 1100.0, 1


class ActiveVentAfterOffPace(FakePace):
    def exit_atmosphere_mode(self, *, timeout_s: float = 30.0, poll_s: float = 0.25):
        self.calls.append(("exit_atmosphere_mode", float(timeout_s), float(poll_s)))
        self.output_state = 0
        self.vent_status = 1
        self.isolation_state = 1
        return self.vent_status


class LegacyTrappedVentAfterOffPace(FakePace):
    def exit_atmosphere_mode(self, *, timeout_s: float = 30.0, poll_s: float = 0.25):
        self.calls.append(("exit_atmosphere_mode", float(timeout_s), float(poll_s)))
        self.output_state = 0
        self.vent_status = 3
        self.isolation_state = 1
        return self.vent_status


class ManualFallbackPace(FakePace):
    exit_atmosphere_mode = None


class DriverNoPollPace(FakePace):
    def exit_atmosphere_mode(self, *, timeout_s: float = 30.0):
        self.calls.append(("exit_atmosphere_mode_timeout_only", float(timeout_s)))
        self.output_state = 0
        self.vent_status = 2
        self.isolation_state = 1
        return self.vent_status


class FakeStdin:
    def __init__(self, text: str = "", *, interactive: bool = True) -> None:
        self.text = text
        self.interactive = interactive
        self.read_count = 0

    def isatty(self) -> bool:
        return self.interactive

    def readline(self) -> str:
        self.read_count += 1
        if self.read_count == 1:
            return self.text
        return ""


class FakeGauge:
    def __init__(self, values=None) -> None:
        self.values = list(values or [1013.0])

    def read_pressure(self) -> float:
        if self.values:
            return float(self.values.pop(0))
        return 1013.0


class FakeLogger:
    run_dir = None


def _controlled_cfg(pressure_overrides: dict | None = None) -> dict:
    pressure = {
        "no_outp_transition_mode": True,
        "open_flow_output_off_mode": True,
        "controlled_outp_transition_mode": True,
        "vent0_to_seal_fixed_wait_s": 1.5,
        "controlled_verified_exit_atmosphere": True,
        "controlled_exit_wait_for_vent_idle_timeout_s": 3.0,
        "controlled_exit_wait_for_vent_idle_poll_s": 0.2,
        "require_operator_window_clear_after_vent0": True,
        "operator_window_confirm_mode": "test_config",
        "operator_window_clear_timeout_s": 30.0,
        "operator_window_cleared_after_vent0": True,
        "operator_window_note": "unit-test-confirmed",
        "pressure_rise_gate_blocks_seal": False,
        "vent_hold_interval_s": 2.0,
        "vent_transition_timeout_s": 5.0,
        "stabilize_timeout_s": 0.05,
        "restabilize_retries": 0,
        "controlled_output_confirm_timeout_s": 0.05,
        "controlled_output_confirm_poll_s": 0.01,
        "pressure_trace_poll_s": 0.01,
    }
    pressure.update(pressure_overrides or {})
    return {
        "paths": {"output_dir": "logs"},
        "workflow": {
            "collect_only": True,
            "pressure": pressure,
        },
        "valves": {
            "h2o_path": 8,
            "gas_main": 11,
            "co2_path": 7,
            "co2_map": {"1000": 6},
        },
    }


def _runner(
    pace: FakePace | None = None,
    gauge: FakeGauge | None = None,
    pressure_overrides: dict | None = None,
):
    logs: list[str] = []
    pace = pace or FakePace()
    gauge = gauge or FakeGauge()
    runner = CalibrationRunner(
        _controlled_cfg(pressure_overrides),
        {"pace": pace, "pressure_gauge": gauge},
        FakeLogger(),
        lambda message: logs.append(str(message)),
        lambda *_: None,
    )
    runner._append_pressure_trace_row = MagicMock()
    runner._emit_stage_event = MagicMock()
    runner._check_pause = MagicMock()
    runner._update_atmosphere_reference_hpa = MagicMock()
    runner._refresh_pressure_controller_aux_state = MagicMock()
    runner._capture_preseal_dewpoint_snapshot = MagicMock()
    runner._cached_ready_check_trace_values = MagicMock(return_value={})
    runner._start_pressure_transition_fast_signal_context = MagicMock()
    runner._stop_pressure_transition_fast_signal_context = MagicMock()
    runner._pressure_transition_fast_signal_context_active = MagicMock(return_value=None)
    runner._pressure_trace_poll_s = MagicMock(return_value=0.01)
    runner._pressure_control_wait_aux_interval_s = MagicMock(return_value=999.0)
    return runner, pace, gauge, logs


def _trace_stages(runner: CalibrationRunner) -> list[str]:
    return [
        str(call.kwargs.get("trace_stage"))
        for call in runner._append_pressure_trace_row.call_args_list
    ]


def _prepare_co2_group_runner_for_seal_failure_tests(runner: CalibrationRunner) -> None:
    runner._apply_idle_route_isolation = MagicMock()
    runner._set_temperature_for_point = MagicMock(return_value=True)
    runner._capture_temperature_calibration_snapshot = MagicMock()
    runner._open_co2_route_for_conditioning = MagicMock()
    runner._wait_co2_route_soak_before_seal = MagicMock(return_value=True)
    runner._gas_route_dewpoint_gate_enabled = MagicMock(return_value=False)
    runner._wait_co2_preseal_primary_sensor_gate = MagicMock(return_value=True)
    runner._wait_cold_co2_quality_gate = MagicMock(return_value=True)
    runner._sample_open_route_point = MagicMock()
    runner._cleanup_co2_route = MagicMock()
    runner._set_pressure_to_target = MagicMock()
    runner._set_pressure_to_target_in_active_co2_sealed_sweep = MagicMock()
    runner._sample_and_log = MagicMock()
    runner._wait_after_pressure_stable_before_sampling = MagicMock(return_value=True)
    runner._build_co2_pressure_point = MagicMock(side_effect=lambda _lead, ref: ref)
    runner._request_sample_export_deferral = MagicMock(return_value=False)
    runner._clear_requested_sample_export_deferral = MagicMock()
    runner._flush_deferred_sample_exports = MagicMock()
    runner._flush_deferred_point_exports = MagicMock()


def test_open_flow_sets_outp0_before_vent1() -> None:
    runner, pace, _, _ = _runner()

    runner._set_pressure_controller_vent(True, reason="before CO2 route conditioning")

    assert ("output", False) in pace.calls
    assert ("vent", True) in pace.calls
    assert pace.calls.index(("output", False)) < pace.calls.index(("vent", True))


def test_open_flow_vent1_still_allowed() -> None:
    runner, pace, _, _ = _runner()

    assert runner._set_pressure_controller_vent(True, reason="before CO2 route conditioning") is True

    assert pace.get_vent_status() == 1
    assert runner._pace_vent_status_classification(1, stage="open_flow") == "open_flow_venting_ok"


def test_run_start_allows_output_state_zero_in_controlled_mode() -> None:
    runner, pace, _, logs = _runner()
    pace.output_state = 0

    assert runner._check_pressure_output_preflight() is None
    assert any("[controlled-outp-preflight]" in message for message in logs)


def test_controlled_exit_uses_pace_driver_exit_atmosphere_mode(monkeypatch) -> None:
    runner, pace, _, _ = _runner(gauge=FakeGauge([1013.0, 1013.0]))
    runner._apply_valve_states = MagicMock()
    monkeypatch.setattr("time.sleep", lambda _seconds: None)

    assert runner._pressurize_route_for_sealed_points(_co2_point(), route="co2", sealed_control_refs=[_co2_point()])

    assert ("exit_atmosphere_mode", 3.0, 0.2) in pace.calls
    assert ("vent", False) not in pace.calls
    stages = _trace_stages(runner)
    assert "controlled_exit_atmosphere_begin" in stages
    assert "controlled_exit_atmosphere_driver_exit_begin" in stages
    assert "controlled_exit_atmosphere_driver_exit_done" in stages
    assert "controlled_exit_atmosphere_verify" in stages
    assert "operator_window_check_begin" in stages
    assert "operator_window_check_result" in stages
    assert "controlled_exit_atmosphere_pass" in stages


def test_driver_exit_does_not_send_outp1_before_seal() -> None:
    runner, pace, _, _ = _runner()

    assert runner._verified_exit_atmosphere_for_controlled_co2_preseal(
        _co2_point(),
        route="co2",
        reason="test",
    )

    assert ("exit_atmosphere_mode", 3.0, 0.2) in pace.calls
    assert ("output", True) not in pace.calls
    assert not any(call[0] in {"enable_control_output", "setpoint"} for call in pace.calls)


def test_manual_fallback_only_when_no_driver_exit() -> None:
    runner, pace, _, _ = _runner(pace=ManualFallbackPace())

    assert runner._verified_exit_atmosphere_for_controlled_co2_preseal(
        _co2_point(),
        route="co2",
        reason="test",
    )

    assert not any(call[0] == "exit_atmosphere_mode" for call in pace.calls)
    assert ("output", False) in pace.calls
    assert ("vent", False) in pace.calls
    assert ("isolation", True) in pace.calls
    assert ("wait_for_vent_idle", 3.0, 0.2) in pace.calls


def test_driver_exit_timeout_only_fallback() -> None:
    runner, pace, _, _ = _runner(pace=DriverNoPollPace())

    assert runner._verified_exit_atmosphere_for_controlled_co2_preseal(
        _co2_point(),
        route="co2",
        reason="test",
    )

    assert ("exit_atmosphere_mode_timeout_only", 3.0) in pace.calls


def test_verified_exit_records_pace_state_snapshot() -> None:
    runner, _, _, _ = _runner()

    assert runner._verified_exit_atmosphere_for_controlled_co2_preseal(
        _co2_point(),
        route="co2",
        reason="test",
    )

    verify_calls = [
        call.kwargs
        for call in runner._append_pressure_trace_row.call_args_list
        if call.kwargs.get("trace_stage") == "controlled_exit_atmosphere_verify"
    ]
    assert verify_calls
    verify = verify_calls[-1]
    assert verify["pace_vent_status"] == 2
    assert verify["pace_output_state"] == 0
    assert verify["pace_isolation_state"] == 1
    assert "exit_method=pace.exit_atmosphere_mode" in verify["note"]
    assert "OUTP:MODE=ACT" in verify["note"]
    assert "SYST:ERR?=0,No error" in verify["note"]
    assert ("query", ":SYST:ERR?") in runner.devices["pace"].calls


def test_system_error_zero_variants_allow_continue() -> None:
    assert CalibrationRunner._pressure_controller_system_error_allows_continue("0, No error") is True
    assert CalibrationRunner._pressure_controller_system_error_allows_continue(":SYST:ERR 0, No error") is True
    assert CalibrationRunner._pressure_controller_system_error_allows_continue('+0,"No error"') is True
    assert CalibrationRunner._pressure_controller_system_error_allows_continue("  :syst:err   0 , no error ") is True


def test_system_error_nonzero_fails() -> None:
    assert CalibrationRunner._pressure_controller_system_error_allows_continue("-113, Undefined header") is False
    assert CalibrationRunner._pressure_controller_system_error_allows_continue(":SYST:ERR 101, Bad state") is False


def test_verified_exit_active_vent_fails_before_close_valves(monkeypatch) -> None:
    runner, _, _, _ = _runner(pace=ActiveVentAfterOffPace(), gauge=FakeGauge([1013.0, 1013.0]))
    runner._apply_valve_states = MagicMock()
    monkeypatch.setattr("time.sleep", lambda _seconds: None)

    assert runner._pressurize_route_for_sealed_points(_co2_point(), route="co2", sealed_control_refs=[_co2_point()]) is False

    runner._apply_valve_states.assert_not_called()
    assert "controlled_exit_atmosphere_fail" in _trace_stages(runner)


def test_vent_status_3_not_ready_for_preseal_or_pressure_control(monkeypatch) -> None:
    runner, pace, _, _ = _runner(pace=LegacyTrappedVentAfterOffPace(), gauge=FakeGauge([1013.0, 1018.0]))
    runner._apply_valve_states = MagicMock()
    monkeypatch.setattr("time.sleep", lambda _seconds: None)

    assert runner._pressurize_route_for_sealed_points(_co2_point(), route="co2", sealed_control_refs=[_co2_point()]) is False

    stages = _trace_stages(runner)
    assert ("exit_atmosphere_mode", 3.0, 0.2) in pace.calls
    assert "vent0_state_evidence" in stages
    assert "controlled_exit_atmosphere_fail" in stages
    assert "controlled_exit_atmosphere_pass" not in stages
    assert "operator_window_check_result" not in stages
    runner._apply_valve_states.assert_not_called()


def test_operator_window_console_yes_allows_fixed_1p5_close(monkeypatch) -> None:
    runner, _, _, _ = _runner(
        gauge=FakeGauge([1013.0, 1013.0]),
        pressure_overrides={"operator_window_confirm_mode": "console"},
    )
    events: list[str] = []
    runner._apply_valve_states = MagicMock(side_effect=lambda _valves: events.append("close_valves"))
    monkeypatch.setattr("time.sleep", lambda seconds: events.append(f"sleep:{seconds:.1f}"))
    monkeypatch.setattr("sys.stdin", FakeStdin("YES\n", interactive=True))

    assert runner._pressurize_route_for_sealed_points(_co2_point(), route="co2", sealed_control_refs=[_co2_point()])

    assert runner._controlled_exit_final_decision == "ENGINEERING_EXIT_ATMOSPHERE_PASS"
    assert events[0].startswith("sleep:1.5")
    assert events[1] == "close_valves"


def test_operator_window_console_no_blocks_seal(monkeypatch) -> None:
    runner, _, _, _ = _runner(
        gauge=FakeGauge([1013.0, 1013.0]),
        pressure_overrides={"operator_window_confirm_mode": "console"},
    )
    runner._apply_valve_states = MagicMock()
    runner._cleanup_co2_route = MagicMock()
    monkeypatch.setattr("time.sleep", lambda _seconds: None)
    monkeypatch.setattr("sys.stdin", FakeStdin("NO\n", interactive=True))

    assert runner._pressurize_route_for_sealed_points(_co2_point(), route="co2", sealed_control_refs=[_co2_point()]) is False

    runner._apply_valve_states.assert_not_called()
    assert runner._controlled_exit_final_decision == "FAIL_CLOSED_ATMOSPHERE_WINDOW_NOT_CLEARED"
    assert "operator_window_check_result" in _trace_stages(runner)


def test_operator_window_console_unknown_blocks_pass(monkeypatch) -> None:
    runner, _, _, _ = _runner(
        gauge=FakeGauge([1013.0, 1013.0]),
        pressure_overrides={"operator_window_confirm_mode": "console"},
    )
    runner._apply_valve_states = MagicMock()
    runner._cleanup_co2_route = MagicMock()
    monkeypatch.setattr("time.sleep", lambda _seconds: None)
    monkeypatch.setattr("sys.stdin", FakeStdin("", interactive=False))

    assert runner._pressurize_route_for_sealed_points(_co2_point(), route="co2", sealed_control_refs=[_co2_point()]) is False

    runner._apply_valve_states.assert_not_called()
    assert runner._controlled_exit_final_decision == "FAIL_CLOSED_ATMOSPHERE_WINDOW_OBSERVATION_MISSING"
    stages = _trace_stages(runner)
    assert "controlled_exit_atmosphere_pass" in stages
    assert "operator_window_check_result" in stages
    assert "route_sealed" not in stages


def test_operator_window_config_true_not_used_for_engineering_bypass(monkeypatch) -> None:
    runner, _, _, _ = _runner(
        gauge=FakeGauge([1013.0, 1013.0]),
        pressure_overrides={
            "operator_window_confirm_mode": "console",
            "operator_window_cleared_after_vent0": True,
        },
    )
    runner._apply_valve_states = MagicMock()
    runner._cleanup_co2_route = MagicMock()
    monkeypatch.setattr("time.sleep", lambda _seconds: None)
    monkeypatch.setattr("sys.stdin", FakeStdin("", interactive=False))

    assert runner._pressurize_route_for_sealed_points(_co2_point(), route="co2", sealed_control_refs=[_co2_point()]) is False

    runner._apply_valve_states.assert_not_called()
    assert runner._controlled_exit_final_decision == "FAIL_CLOSED_ATMOSPHERE_WINDOW_OBSERVATION_MISSING"


def test_operator_window_prompt_after_fixed_wait_before_close(monkeypatch) -> None:
    runner, _, _, _ = _runner(
        gauge=FakeGauge([1013.0, 1013.0]),
        pressure_overrides={"operator_window_confirm_mode": "console"},
    )
    runner._apply_valve_states = MagicMock()
    monkeypatch.setattr("time.sleep", lambda _seconds: None)
    monkeypatch.setattr("sys.stdin", FakeStdin("YES\n", interactive=True))

    assert runner._pressurize_route_for_sealed_points(_co2_point(), route="co2", sealed_control_refs=[_co2_point()])

    stages = _trace_stages(runner)
    assert stages.index("controlled_exit_atmosphere_driver_exit_done") < stages.index("operator_window_prompt_printed")
    assert stages.index("controlled_outp_vent0_fixed_wait_before_seal") < stages.index("operator_window_prompt_printed")
    assert stages.index("operator_window_check_result") < stages.index("route_sealed")


def test_operator_window_trace_records_raw_response(monkeypatch) -> None:
    runner, _, _, _ = _runner(
        gauge=FakeGauge([1013.0, 1013.0]),
        pressure_overrides={"operator_window_confirm_mode": "console"},
    )
    runner._apply_valve_states = MagicMock()
    monkeypatch.setattr("time.sleep", lambda _seconds: None)
    monkeypatch.setattr("sys.stdin", FakeStdin("YES\n", interactive=True))

    assert runner._pressurize_route_for_sealed_points(_co2_point(), route="co2", sealed_control_refs=[_co2_point()])

    result_notes = [
        str(call.kwargs.get("note") or "")
        for call in runner._append_pressure_trace_row.call_args_list
        if call.kwargs.get("trace_stage") == "operator_window_check_result"
    ]
    assert result_notes
    assert "operator_window_response_raw=YES" in result_notes[-1]


def test_window_not_cleared_still_no_close_valves(monkeypatch) -> None:
    runner, _, _, _ = _runner(
        gauge=FakeGauge([1013.0, 1013.0]),
        pressure_overrides={"operator_window_confirm_mode": "console"},
    )
    runner._apply_valve_states = MagicMock()
    runner._cleanup_co2_route = MagicMock()
    monkeypatch.setattr("time.sleep", lambda _seconds: None)
    monkeypatch.setattr("sys.stdin", FakeStdin("NOT_CLEARED\n", interactive=True))

    assert runner._pressurize_route_for_sealed_points(_co2_point(), route="co2", sealed_control_refs=[_co2_point()]) is False

    runner._apply_valve_states.assert_not_called()


def test_window_ui_residual_suspected_removed() -> None:
    runner, _, _, _ = _runner()

    assert runner._verified_exit_atmosphere_for_controlled_co2_preseal(
        _co2_point(),
        route="co2",
        reason="test",
    )

    notes = " ".join(
        str(call.kwargs.get("note") or "")
        for call in runner._append_pressure_trace_row.call_args_list
    )
    assert "ENGINEERING_RUN_PARTIAL_WINDOW_UI_RESIDUAL_SUSPECTED" not in notes
    assert "window_state_unverified_by_software" not in notes


def test_vent0_fixed_1p5_then_close_valves(monkeypatch) -> None:
    runner, pace, _, _ = _runner(gauge=FakeGauge([1013.0, 1013.0]))
    point = _co2_point()
    events: list[tuple] = []
    runner._apply_valve_states = MagicMock(side_effect=lambda valves: events.append(("close_valves", list(valves))))
    original_guard = runner._activate_co2_sealed_no_vent_guard
    runner._activate_co2_sealed_no_vent_guard = MagicMock(
        side_effect=lambda *args, **kwargs: (events.append(("guard", None)), original_guard(*args, **kwargs))
    )
    monkeypatch.setattr("time.sleep", lambda seconds: events.append(("sleep", seconds)))

    assert runner._pressurize_route_for_sealed_points(point, route="co2", sealed_control_refs=[point]) is True

    assert ("exit_atmosphere_mode", 3.0, 0.2) in pace.calls
    assert any(event[0] == "sleep" and event[1] >= 1.49 for event in events)
    assert events.index(("close_valves", [])) > next(i for i, event in enumerate(events) if event[0] == "sleep")
    assert events.index(("close_valves", [])) < events.index(("guard", None))


def test_vent0_state_trace_recorded_before_route_close(monkeypatch) -> None:
    runner, _, _, _ = _runner(gauge=FakeGauge([1013.0, 1013.0, 1013.0]))
    point = _co2_point()
    runner._apply_valve_states = MagicMock()
    monkeypatch.setattr("time.sleep", lambda _seconds: None)

    assert runner._pressurize_route_for_sealed_points(point, route="co2", sealed_control_refs=[point]) is True

    evidence_calls = [
        call.kwargs
        for call in runner._append_pressure_trace_row.call_args_list
        if call.kwargs.get("trace_stage") == "vent0_state_evidence"
    ]
    close_calls = [
        call.kwargs
        for call in runner._append_pressure_trace_row.call_args_list
        if call.kwargs.get("trace_stage") == "route_valves_closed_after_vent0"
    ]
    assert evidence_calls
    assert close_calls
    fields = evidence_calls[-1]["extra_fields"]
    assert fields["vent0_command_ts"]
    assert fields["vent_status_after_vent0"] == 2
    assert fields["vent_status_after_vent0_classification"] == "vent_completed_ready"
    assert fields["outp_status_after_vent0"] == 0
    assert fields["isol_status_after_vent0"] == 1
    assert fields["syst_err_after_vent0"] == "0,No error"
    assert fields["pace_pressure_after_vent0_hpa"] == 1013.0
    assert fields["com22_pressure_after_vent0_hpa"] == 1013.0
    assert fields["route_valves_still_open_during_wait"] is True
    close_fields = close_calls[-1]["extra_fields"]
    assert close_fields["fixed_wait_elapsed_s"] is not None
    assert close_fields["route_valve_close_ts"]
    assert "actual_open_valves_after_close" in close_fields


def test_original_order_preserved(monkeypatch) -> None:
    runner, _, _, _ = _runner(gauge=FakeGauge([1013.0, 1013.0, 1013.0]))
    point = _co2_point()
    runner._apply_valve_states = MagicMock()
    monkeypatch.setattr("time.sleep", lambda _seconds: None)

    assert runner._pressurize_route_for_sealed_points(point, route="co2", sealed_control_refs=[point]) is True

    stages = _trace_stages(runner)
    assert stages.index("vent0_state_evidence") < stages.index("controlled_outp_vent0_fixed_wait_before_seal")
    assert stages.index("controlled_outp_vent0_fixed_wait_before_seal") < stages.index(
        "route_valves_closed_after_vent0"
    )
    assert stages.index("route_valves_closed_after_vent0") < stages.index("route_sealed")


def test_verified_exit_then_fixed_1p5_close_valves(monkeypatch) -> None:
    runner, _, _, _ = _runner(gauge=FakeGauge([1013.0, 1013.0]))
    events: list[str] = []
    runner._apply_valve_states = MagicMock(side_effect=lambda _valves: events.append("close_valves"))
    monkeypatch.setattr("time.sleep", lambda seconds: events.append(f"sleep:{seconds:.1f}"))

    assert runner._pressurize_route_for_sealed_points(_co2_point(), route="co2", sealed_control_refs=[_co2_point()])

    stages = _trace_stages(runner)
    assert stages.index("controlled_exit_atmosphere_pass") < stages.index("controlled_outp_vent0_fixed_wait_before_seal")
    assert stages.index("controlled_outp_vent0_fixed_wait_before_seal") < stages.index("operator_window_check_begin")
    assert stages.index("operator_window_check_result") < stages.index("route_sealed")
    assert events[0].startswith("sleep:1.5")
    assert events[1] == "close_valves"


def test_pressure_rise_gate_does_not_block_seal_in_controlled_mode(monkeypatch) -> None:
    runner, _, _, _ = _runner(gauge=FakeGauge([1013.0, 1013.0, 1013.0]))
    runner._cleanup_co2_route = MagicMock()
    runner._apply_valve_states = MagicMock()
    monkeypatch.setattr("time.sleep", lambda _seconds: None)

    assert runner._pressurize_route_for_sealed_points(_co2_point(), route="co2", sealed_control_refs=[_co2_point()]) is True

    runner._cleanup_co2_route.assert_not_called()
    assert "controlled_outp_pressure_rise_diagnostic" in _trace_stages(runner)


def test_pressure_rise_still_diagnostic_only(monkeypatch) -> None:
    runner, _, _, _ = _runner(gauge=FakeGauge([1013.0, 1013.0]))
    runner._cleanup_co2_route = MagicMock()
    runner._apply_valve_states = MagicMock()
    monkeypatch.setattr("time.sleep", lambda _seconds: None)

    assert runner._pressurize_route_for_sealed_points(_co2_point(), route="co2", sealed_control_refs=[_co2_point()])

    runner._cleanup_co2_route.assert_not_called()
    assert runner._apply_valve_states.called
    assert "controlled_outp_pressure_rise_diagnostic" in _trace_stages(runner)


def test_pressure_rise_cannot_replace_window_gate(monkeypatch) -> None:
    runner, _, _, _ = _runner(
        gauge=FakeGauge([1013.0, 1045.0]),
        pressure_overrides={"operator_window_confirm_mode": "console"},
    )
    runner._apply_valve_states = MagicMock()
    runner._cleanup_co2_route = MagicMock()
    monkeypatch.setattr("time.sleep", lambda _seconds: None)
    monkeypatch.setattr("sys.stdin", FakeStdin("", interactive=False))

    assert runner._pressurize_route_for_sealed_points(_co2_point(), route="co2", sealed_control_refs=[_co2_point()]) is False

    runner._apply_valve_states.assert_not_called()
    assert runner._controlled_exit_final_decision == "FAIL_CLOSED_ATMOSPHERE_WINDOW_OBSERVATION_MISSING"
    assert "controlled_outp_pressure_rise_diagnostic" in _trace_stages(runner)


def test_route_valves_remain_open_during_fixed_wait(monkeypatch) -> None:
    runner, _, _, _ = _runner(gauge=FakeGauge([1013.0, 1013.0]))
    events: list[str] = []
    runner._apply_valve_states = MagicMock(side_effect=lambda _valves: events.append("close_valves"))
    monkeypatch.setattr("time.sleep", lambda seconds: events.append(f"sleep:{seconds:.1f}"))

    runner._pressurize_route_for_sealed_points(_co2_point(), route="co2", sealed_control_refs=[_co2_point()])

    assert events[0].startswith("sleep:")
    assert events[1] == "close_valves"


def test_sealed_guard_activates_immediately_after_close(monkeypatch) -> None:
    runner, _, _, _ = _runner(gauge=FakeGauge([1013.0, 1013.0]))
    events: list[str] = []
    runner._apply_valve_states = MagicMock(side_effect=lambda _valves: events.append("close"))
    original_guard = runner._activate_co2_sealed_no_vent_guard
    runner._activate_co2_sealed_no_vent_guard = MagicMock(
        side_effect=lambda *args, **kwargs: (events.append("guard"), original_guard(*args, **kwargs))
    )
    monkeypatch.setattr("time.sleep", lambda _seconds: None)

    runner._pressurize_route_for_sealed_points(_co2_point(), route="co2", sealed_control_refs=[_co2_point()])

    assert events == ["close", "guard"]
    assert runner._co2_sealed_no_vent_guard_active is True


def test_sealed_sweep_first_point_enables_output_once() -> None:
    runner, pace, _, _ = _runner()
    point = _co2_point(pressure=900.0)
    runner._activate_co2_sealed_no_vent_guard(point, reason="test")
    runner._record_preseal_pressure_control_ready_state(point, phase="co2", defer_live_check=True)

    assert runner._set_pressure_to_target(point) is True

    assert pace.calls.count(("enable_control_output",)) == 1
    assert pace.calls.index(("enable_control_output",)) < pace.calls.index(("setpoint", 900.0))
    pass_calls = [
        call.kwargs
        for call in runner._append_pressure_trace_row.call_args_list
        if call.kwargs.get("trace_stage") == "sealed_control_output_enable_pass"
    ]
    assert pass_calls
    assert pass_calls[-1]["extra_fields"]["sealed_outp1_count"] == 1
    assert pass_calls[-1]["extra_fields"]["sealed_setpoint_count"] == 0


def test_sealed_sweep_first_point_reuses_existing_output_on() -> None:
    runner, pace, _, _ = _runner()
    point = _co2_point(pressure=900.0)
    runner._activate_co2_sealed_no_vent_guard(point, reason="test")
    runner._record_preseal_pressure_control_ready_state(point, phase="co2", defer_live_check=True)
    pace.output_state = 1

    assert runner._set_pressure_to_target(point) is True

    assert ("enable_control_output",) not in pace.calls
    assert ("setpoint", 900.0) in pace.calls
    stages = _trace_stages(runner)
    assert "sealed_control_output_already_on" in stages


def test_sealed_output_enable_after_verified_exit(monkeypatch) -> None:
    runner, pace, _, _ = _runner(gauge=FakeGauge([1013.0, 1013.0]))
    point = _co2_point(pressure=900.0)
    runner._apply_valve_states = MagicMock()
    monkeypatch.setattr("time.sleep", lambda _seconds: None)

    assert runner._pressurize_route_for_sealed_points(point, route="co2", sealed_control_refs=[point])
    assert runner._set_pressure_to_target(point) is True

    assert pace.calls.index(("exit_atmosphere_mode", 3.0, 0.2)) < pace.calls.index(("enable_control_output",))
    assert pace.calls.index(("enable_control_output",)) < pace.calls.index(("setpoint", 900.0))


def test_no_outp_after_first_sealed_control_enable() -> None:
    runner, pace, _, _ = _runner()
    first = _co2_point(pressure=1100.0)
    second = _co2_point(pressure=900.0)
    runner._activate_co2_sealed_no_vent_guard(first, reason="test")
    runner._begin_active_co2_sealed_sweep_context(first)
    runner._controlled_outp_sealed_output_enabled = True
    pace.calls.clear()

    assert runner._set_pressure_to_target_in_active_co2_sealed_sweep(second) is True

    assert ("setpoint", 900.0) in pace.calls
    assert not any(call[0] in {"output", "vent", "enable_control_output"} for call in pace.calls)


def test_sealed_sweep_subsequent_points_setpoint_only() -> None:
    runner, pace, _, _ = _runner()
    first = _co2_point(pressure=1100.0)
    later = _co2_point(pressure=700.0)
    runner._activate_co2_sealed_no_vent_guard(first, reason="test")
    runner._begin_active_co2_sealed_sweep_context(first)
    pace.calls.clear()

    assert runner._set_pressure_to_target_in_active_co2_sealed_sweep(later) is True
    assert pace.calls == [("setpoint", 700.0)]
    update_calls = [
        call.kwargs
        for call in runner._append_pressure_trace_row.call_args_list
        if call.kwargs.get("trace_stage") == "sealed_sweep_setpoint_update"
    ]
    assert update_calls
    assert update_calls[-1]["extra_fields"]["sealed_setpoint_count"] == 1
    assert update_calls[-1]["extra_fields"]["sealed_outp1_count"] == 0
    assert update_calls[-1]["extra_fields"]["sealed_vent1_count"] == 0


def test_sealed_sweep_context_clears_on_failure() -> None:
    runner, _, _, _ = _runner()
    first = _co2_point(pressure=1100.0)
    later = _co2_point(pressure=700.0)
    runner._activate_co2_sealed_no_vent_guard(first, reason="test")
    runner._begin_active_co2_sealed_sweep_context(first)
    runner._co2_sealed_no_vent_guard_active = False

    assert runner._set_pressure_to_target_in_active_co2_sealed_sweep(later) is False

    assert runner._active_co2_sealed_sweep_context is None
    assert runner._co2_sealed_no_vent_guard_active is False
    assert "sealed_sweep_context_end" in _trace_stages(runner)


def test_first_point_exit_fail_stops_co2_route() -> None:
    runner, _, _, _ = _runner()
    _prepare_co2_group_runner_for_seal_failure_tests(runner)

    def fail_once(*_args, **_kwargs):
        runner._controlled_exit_final_decision = "FAIL_CLOSED_ATMOSPHERE_EXIT_NOT_VERIFIED"
        return False

    runner._pressurize_route_for_sealed_points = MagicMock(side_effect=fail_once)

    runner._run_co2_point(
        _co2_point(index=3, pressure=1100.0),
        pressure_points=[
            _co2_point(index=3, pressure=1100.0),
            _co2_point(index=4, pressure=1000.0),
            _co2_point(index=5, pressure=900.0),
        ],
    )

    assert runner._pressurize_route_for_sealed_points.call_count == 1
    runner._cleanup_co2_route.assert_called_once_with(reason="FAIL_CLOSED_ATMOSPHERE_EXIT_NOT_VERIFIED")
    runner._set_pressure_to_target.assert_not_called()
    runner._sample_and_log.assert_not_called()


def test_first_point_window_no_stops_co2_route() -> None:
    runner, _, _, _ = _runner()
    _prepare_co2_group_runner_for_seal_failure_tests(runner)

    def fail_once(*_args, **_kwargs):
        runner._controlled_exit_final_decision = "FAIL_CLOSED_ATMOSPHERE_WINDOW_NOT_CLEARED"
        return False

    runner._pressurize_route_for_sealed_points = MagicMock(side_effect=fail_once)

    runner._run_co2_point(
        _co2_point(index=3, pressure=1100.0),
        pressure_points=[_co2_point(index=3, pressure=1100.0), _co2_point(index=4, pressure=1000.0)],
    )

    assert runner._pressurize_route_for_sealed_points.call_count == 1
    runner._cleanup_co2_route.assert_called_once_with(reason="FAIL_CLOSED_ATMOSPHERE_WINDOW_NOT_CLEARED")
    runner._set_pressure_to_target.assert_not_called()


def test_first_point_window_unknown_stops_co2_route() -> None:
    runner, _, _, _ = _runner()
    _prepare_co2_group_runner_for_seal_failure_tests(runner)

    def fail_once(*_args, **_kwargs):
        runner._controlled_exit_final_decision = "FAIL_CLOSED_ATMOSPHERE_WINDOW_OBSERVATION_MISSING"
        return False

    runner._pressurize_route_for_sealed_points = MagicMock(side_effect=fail_once)

    runner._run_co2_point(
        _co2_point(index=3, pressure=1100.0),
        pressure_points=[_co2_point(index=3, pressure=1100.0), _co2_point(index=4, pressure=1000.0)],
    )

    assert runner._pressurize_route_for_sealed_points.call_count == 1
    runner._cleanup_co2_route.assert_called_once_with(reason="FAIL_CLOSED_ATMOSPHERE_WINDOW_OBSERVATION_MISSING")
    runner._sample_and_log.assert_not_called()


def test_high_post_safe_stop_pressure_marks_relief_issue() -> None:
    runner, _, _, _ = _runner(
        pressure_overrides={
            "engineering_safe_stop_pressure_relief_check": True,
            "safe_stop_pressure_relief_threshold_hpa": 20.0,
            "safe_stop_pressure_relief_reference_hpa": 1013.25,
        }
    )

    issue = runner._engineering_safe_stop_pressure_relief_issue(
        {"pace_pressure_hpa": 1076.0, "gauge_pressure_hpa": 1015.0}
    )

    assert issue is not None
    assert issue.startswith("SAFE_STOP_PRESSURE_REMAINS_HIGH")
    assert "pace_pressure_hpa=1076.000" in issue


def test_near_ambient_post_safe_stop_pressure_passes_relief_check() -> None:
    runner, _, _, _ = _runner(
        pressure_overrides={
            "engineering_safe_stop_pressure_relief_check": True,
            "safe_stop_pressure_relief_threshold_hpa": 20.0,
            "safe_stop_pressure_relief_reference_hpa": 1013.25,
        }
    )

    assert runner._engineering_safe_stop_pressure_relief_issue(
        {"pace_pressure_hpa": 1025.0, "gauge_pressure_hpa": 1020.0}
    ) is None


def test_safe_stop_verified_independent_from_residual_pressure_cleared(monkeypatch) -> None:
    runner, _, _, _ = _runner(
        pressure_overrides={
            "engineering_safe_stop_pressure_relief_check": True,
            "safe_stop_pressure_relief_threshold_hpa": 20.0,
            "safe_stop_pressure_relief_reference_hpa": 1013.25,
        }
    )
    events: list[dict] = []

    class CaptureLogger:
        run_dir = None

        def log_io(self, **kwargs):
            events.append(dict(kwargs))

    runner.logger = CaptureLogger()
    monkeypatch.setattr(
        runner_module,
        "_perform_safe_stop",
        lambda *_args, **_kwargs: {
            "safe_stop_verified": True,
            "pace_pressure_hpa": 1076.0,
            "gauge_pressure_hpa": 1015.0,
            "safe_stop_issues": [],
        },
    )

    runner._restore_baseline_after_run()

    done = [event for event in events if event.get("command") == "baseline-restore-done"]
    assert done
    summary = json.loads(done[-1]["response"])
    assert summary["safe_stop_verified"] is True
    assert summary["residual_pressure_cleared"] is False
    assert summary["residual_pressure_requires_manual_clearance"] is True
    assert "SAFE_STOP_PRESSURE_REMAINS_HIGH" in summary["residual_pressure_high_reason"]
    assert summary["safe_stop_pace_pressure_hpa"] == 1076.0
    assert summary["safe_stop_com22_pressure_hpa"] == 1015.0


def test_open_flow_outp0_not_allowed_after_sealed() -> None:
    runner, pace, _, _ = _runner()
    runner._activate_co2_sealed_no_vent_guard(_co2_point(), reason="test")
    pace.calls.clear()

    assert runner._set_pressure_controller_vent(True, reason="before CO2 route conditioning") is False

    assert pace.calls == []
    blocked_calls = [
        call.kwargs
        for call in runner._append_pressure_trace_row.call_args_list
        if call.kwargs.get("trace_stage") == "sealed_no_vent_guard_blocked"
    ]
    assert blocked_calls
    assert blocked_calls[-1]["extra_fields"]["sealed_vent1_count"] == 1
    assert blocked_calls[-1]["extra_fields"]["sealed_pressure_control_vent_on_count"] == 1
    assert blocked_calls[-1]["extra_fields"]["no_vent_guard_blocked_count"] == 1


def test_h2o_unchanged() -> None:
    runner, _, _, _ = _runner()

    assert runner._controlled_outp_seal_transition_enabled("h2o") is False
    assert runner._controlled_verified_exit_atmosphere_enabled("h2o") is False


def test_h2o_path_unchanged() -> None:
    runner, pace, _, _ = _runner()

    assert runner._verified_exit_atmosphere_for_controlled_co2_preseal(
        _co2_point(),
        route="h2o",
        reason="test",
    )

    assert "controlled_exit_atmosphere_begin" not in _trace_stages(runner)
    assert ("vent", False) in pace.calls


def test_config_guard_controlled_outp_skip_tempwait() -> None:
    cfg = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    workflow = cfg["workflow"]
    pressure = workflow["pressure"]
    temp = workflow["stability"]["temperature"]

    assert pressure["no_outp_transition_mode"] is True
    assert pressure["controlled_outp_transition_mode"] is True
    assert pressure["open_flow_output_off_mode"] is True
    assert pressure["vent0_to_seal_fixed_wait_s"] == 1.5
    assert pressure["controlled_verified_exit_atmosphere"] is True
    assert pressure["controlled_exit_wait_for_vent_idle_timeout_s"] == 3.0
    assert pressure["controlled_exit_wait_for_vent_idle_poll_s"] == 0.2
    assert pressure["require_operator_window_clear_after_vent0"] is True
    assert pressure["operator_window_confirm_mode"] == "console"
    assert pressure["operator_window_clear_timeout_s"] == 30.0
    assert "operator_window_cleared_after_vent0" not in pressure
    assert pressure["pressure_rise_gate_blocks_seal"] is False
    assert pressure["engineering_safe_stop_pressure_relief_check"] is True
    assert pressure["safe_stop_pressure_relief_timeout_s"] == 30.0
    assert pressure["safe_stop_pressure_relief_threshold_hpa"] == 20.0
    assert workflow["collect_only"] is True
    assert cfg["coefficients"]["enabled"] is False
    assert cfg["coefficients"]["sencos"] == {}
    assert workflow["startup_pressure_sensor_calibration"]["enabled"] is False
    assert workflow["startup_pressure_sensor_calibration"]["apply_write"] is False
    assert workflow["postrun_corrected_delivery"]["enabled"] is False
    assert workflow["postrun_corrected_delivery"]["write_devices"] is False
    assert workflow["postrun_corrected_delivery"]["write_pressure_coefficients"] is False
    assert workflow["route_mode"] == "co2_only"
    assert workflow["selected_temps"] == [20]
    assert workflow["preserve_explicit_point_matrix"] is True
    assert temp["window_s"] == 0
    assert temp["timeout_s"] == 0
    assert temp["hard_max_wait_s"] == 0
    assert temp["analyzer_chamber_temp_enabled"] is False
    assert temp["analyzer_chamber_temp_timeout_s"] == 0
    assert temp["analyzer_chamber_temp_first_valid_timeout_s"] == 0
    assert cfg["paths"]["points_excel"].endswith("points_v1_5_co2_20c_1000ppm_full_pressure_nowait.xlsx")


def test_no_write_guard_still_holds() -> None:
    cfg = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    workflow = cfg["workflow"]

    assert cfg["coefficients"]["enabled"] is False
    assert cfg["coefficients"]["sencos"] == {}
    assert workflow["collect_only"] is True
    assert workflow["startup_pressure_sensor_calibration"]["apply_write"] is False
    assert workflow["postrun_corrected_delivery"]["write_devices"] is False
    assert workflow["postrun_corrected_delivery"]["write_pressure_coefficients"] is False


def test_config_enables_controlled_verified_exit() -> None:
    cfg = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    pressure = cfg["workflow"]["pressure"]
    temp = cfg["workflow"]["stability"]["temperature"]

    assert pressure["controlled_verified_exit_atmosphere"] is True
    assert pressure["controlled_exit_wait_for_vent_idle_timeout_s"] == 3.0
    assert pressure["controlled_exit_wait_for_vent_idle_poll_s"] == 0.2
    assert pressure["no_outp_transition_mode"] is True
    assert pressure["open_flow_output_off_mode"] is True
    assert cfg["workflow"]["collect_only"] is True
    assert temp["timeout_s"] == 0


def test_config_requires_operator_window_clear() -> None:
    cfg = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    pressure = cfg["workflow"]["pressure"]

    assert pressure["require_operator_window_clear_after_vent0"] is True
    assert pressure["operator_window_confirm_mode"] == "console"
    assert pressure["operator_window_clear_timeout_s"] == 30.0
    assert "operator_window_cleared_after_vent0" not in pressure
    assert pressure["engineering_safe_stop_pressure_relief_check"] is True
