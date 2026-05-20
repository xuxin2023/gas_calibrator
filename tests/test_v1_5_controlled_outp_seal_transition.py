from __future__ import annotations

import json
import time
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from gas_calibrator.data.points import CalibrationPoint
from gas_calibrator.workflow import runner as runner_module
from gas_calibrator.workflow.runner import CalibrationRunner


CONFIG_PATH = Path(
    "configs/site_v1_5_no_write_current_hardware_co2_20c_1000ppm_full_pressure_controlled_outp_skip_tempwait.json"
)


def _co2_point(index: int = 1, pressure: float = 900.0, ppm: float = 1000.0) -> CalibrationPoint:
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


def _ambient_co2_point(index: int = 3, ppm: float = 0.0) -> CalibrationPoint:
    point = _co2_point(index=index, pressure=None, ppm=ppm)
    setattr(point, "_pressure_mode", "ambient_open")
    setattr(point, "_pressure_target_label", "当前大气压")
    setattr(point, "_pressure_selection_token", "ambient")
    return point


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
        self.in_limits = []
        self.setpoint = None
        self.efforts = [-1.0]
        self.slew_mode = ""
        self.slew_rate = None
        self.overshoot_allowed = None

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

    def set_output_mode_active(self) -> None:
        self.calls.append(("set_output_mode_active",))
        self.output_mode = "ACT"

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
        self.setpoint = float(value)

    def set_slew_mode_linear(self) -> None:
        self.calls.append(("set_slew_mode_linear",))
        self.slew_mode = "LIN"

    def set_slew_rate(self, value: float) -> None:
        self.calls.append(("set_slew_rate", float(value)))
        self.slew_rate = float(value)

    def set_overshoot_allowed(self, enabled: bool) -> None:
        self.calls.append(("set_overshoot_allowed", bool(enabled)))
        self.overshoot_allowed = bool(enabled)

    def get_setpoint(self) -> float | None:
        return self.setpoint

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
        cmd = str(command).strip().upper()
        if cmd == ":SYST:ERR?":
            return self.system_error
        if cmd == ":SOUR:PRES:EFF?":
            if self.efforts:
                return str(self.efforts.pop(0))
            return "-1.0"
        return ""

    def vent_status_allows_control(self, status: int) -> bool:
        return int(status) in {0, 2, 3}

    def read_pressure(self) -> float:
        return 1013.0

    def get_in_limits(self):
        if self.in_limits:
            return self.in_limits.pop(0)
        return float(self.setpoint if self.setpoint is not None else 1100.0), 1


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


class Vent3ThenClearPace(FakePace):
    def __init__(self) -> None:
        super().__init__()
        self.vent_off_count = 0

    def vent(self, on: bool = True) -> None:
        self.calls.append(("vent", bool(on)))
        if on:
            self.vent_status = 1
            return
        self.vent_off_count += 1
        self.vent_status = 3 if self.vent_off_count == 1 else 0


class Vent3PersistPace(FakePace):
    def vent(self, on: bool = True) -> None:
        self.calls.append(("vent", bool(on)))
        self.vent_status = 1 if on else 3


class ManualFallbackPace(FakePace):
    exit_atmosphere_mode = None


class DriverNoPollPace(FakePace):
    def exit_atmosphere_mode(self, *, timeout_s: float = 30.0):
        self.calls.append(("exit_atmosphere_mode_timeout_only", float(timeout_s)))
        self.output_state = 0
        self.vent_status = 2
        self.isolation_state = 1
        return self.vent_status


class LingeringHoldPace(FakePace):
    def stop_atmosphere_hold(self) -> bool:
        self.calls.append(("stop_hold",))
        return True

    def is_atmosphere_hold_active(self) -> bool:
        return True


class ProbeFailOncePace(FakePace):
    def __init__(self) -> None:
        super().__init__()
        self.vent_status_read_count = 0

    def get_vent_status(self) -> int:
        self.vent_status_read_count += 1
        if self.vent_status_read_count == 1:
            raise TimeoutError("VENT? probe timeout")
        return self.vent_status


class SequenceVentStatusPace(FakePace):
    def __init__(self, statuses: list[int]) -> None:
        super().__init__()
        self._statuses = list(statuses)
        if statuses:
            self.vent_status = int(statuses[0])

    def get_vent_status(self) -> int:
        if self._statuses:
            self.vent_status = int(self._statuses.pop(0))
        return self.vent_status


class SlewConfigFailPace(FakePace):
    def set_slew_rate(self, value: float) -> None:
        self.calls.append(("set_slew_rate_failed", float(value)))
        raise RuntimeError("slew rate failed")


class SetpointPrearmFailPace(FakePace):
    def set_setpoint(self, value: float) -> None:
        self.calls.append(("setpoint_failed", float(value)))
        raise RuntimeError("setpoint prearm failed")


class BlockingStatePrimePace(FakePace):
    def get_output_state(self) -> int:
        raise AssertionError(":OUTP:STAT? must be deferred during open-flow keepalive")

    def get_isolation_state(self) -> int:
        raise AssertionError(":OUTP:ISOL:STAT? must be deferred during open-flow keepalive")


class Vent3AfterOutp1Pace(FakePace):
    def enable_control_output(self) -> None:
        self.calls.append(("enable_control_output",))
        self.output_state = 1
        self.vent_status = 3


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


class RawTapLogger(FakeLogger):
    def __init__(self, *, enabled: bool = True, vent1_age_s: float | None = None) -> None:
        self.enabled = enabled
        self.vent1_age_s = vent1_age_s

    def pace_raw_tap_enabled(self) -> bool:
        return self.enabled

    def latest_pace_raw_tap_vent1_evidence(self) -> dict:
        if self.vent1_age_s is None:
            return {"raw_tap_enabled": self.enabled}
        return {
            "raw_tap_enabled": self.enabled,
            "wall_ts": "2026-05-17T22:41:25.257",
            "monotonic_ts": f"{time.monotonic() - float(self.vent1_age_s):.9f}",
            "decoded_command": ":SOUR:PRES:LEV:IMM:AMPL:VENT 1",
            "thread_name": "pace5000-vent-hold-COM23",
        }


class PostVent0RawTapLogger(FakeLogger):
    def __init__(
        self,
        *,
        vent1_count: int = 0,
        vent0_wall_ts: float | None = None,
        outp1_count: int = 0,
        setpoint_count: int = 0,
    ) -> None:
        self.vent1_count = int(vent1_count)
        self.vent0_wall_ts = vent0_wall_ts
        self.outp1_count = int(outp1_count)
        self.setpoint_count = int(setpoint_count)

    def latest_pace_raw_tap_vent0_evidence(self) -> dict:
        wall_ts = (
            datetime.fromtimestamp(float(self.vent0_wall_ts)).isoformat(timespec="milliseconds")
            if self.vent0_wall_ts is not None
            else "2026-05-18T16:00:00.100"
        )
        return {
            "raw_tap_enabled": True,
            "wall_ts": wall_ts,
            "monotonic_ts": f"{time.monotonic():.9f}",
            "decoded_command": ":SOUR:PRES:LEV:IMM:AMPL:VENT 0",
            "thread_name": "MainThread",
        }

    def latest_pace_raw_tap_vent1_evidence(self) -> dict:
        return {
            "raw_tap_enabled": True,
            "wall_ts": "2026-05-18T15:59:59.500",
            "monotonic_ts": f"{time.monotonic() - 1.0:.9f}",
            "decoded_command": ":SOUR:PRES:LEV:IMM:AMPL:VENT 1",
            "thread_name": "pace5000-vent-hold-COM23",
        }

    def summarize_pace_raw_tap_window(self, _begin_ts, _end_ts) -> dict:
        return {
            "vent1_count": self.vent1_count,
            "vent0_count": 1,
            "outp1_count": self.outp1_count,
            "setpoint_sour_pres_count": self.setpoint_count,
            "unexpected_state_changing_write_count": 0,
        }


class QuietGapRawTapLogger(FakeLogger):
    def __init__(
        self,
        *,
        latest_wall_s: float,
        sequence_wall_s: list[float] | None = None,
        post_stop_count: int = 0,
        vent0_wall_ts: float | None = None,
    ) -> None:
        self.enabled = True
        self.latest_wall_s = float(latest_wall_s)
        self.sequence_wall_s = list(sequence_wall_s or [])
        self.post_stop_count = int(post_stop_count)
        self.vent0_wall_ts = vent0_wall_ts

    def pace_raw_tap_enabled(self) -> bool:
        return self.enabled

    def latest_pace_raw_tap_vent1_evidence(self) -> dict:
        if self.sequence_wall_s:
            self.latest_wall_s = float(self.sequence_wall_s.pop(0))
        return {
            "raw_tap_enabled": True,
            "wall_ts": datetime.fromtimestamp(self.latest_wall_s).isoformat(timespec="milliseconds"),
            "monotonic_ts": f"{time.monotonic():.9f}",
            "decoded_command": ":SOUR:PRES:LEV:IMM:AMPL:VENT 1",
            "thread_name": "pace5000-vent-hold-COM23",
        }

    def latest_pace_raw_tap_vent0_evidence(self) -> dict:
        wall_s = self.vent0_wall_ts if self.vent0_wall_ts is not None else time.time()
        return {
            "raw_tap_enabled": True,
            "wall_ts": datetime.fromtimestamp(float(wall_s)).isoformat(timespec="milliseconds"),
            "monotonic_ts": f"{time.monotonic():.9f}",
            "decoded_command": ":SOUR:PRES:LEV:IMM:AMPL:VENT 0",
            "thread_name": "MainThread",
        }

    def summarize_pace_raw_tap_window(self, _begin_ts, _end_ts) -> dict:
        latest_ts = datetime.fromtimestamp(self.latest_wall_s).isoformat(timespec="milliseconds")
        return {
            "vent1_count": self.post_stop_count,
            "vent1_times": [latest_ts] if self.post_stop_count else [],
            "vent0_count": 1,
            "outp1_count": 0,
            "setpoint_sour_pres_count": 0,
            "unexpected_state_changing_write_count": 0,
        }


class OpenFlowGapLogger(FakeLogger):
    def __init__(self, *, max_gap_s: float, blocking_operation: str = "PACE status query") -> None:
        self.max_gap_s = float(max_gap_s)
        self.blocking_operation = str(blocking_operation)

    def begin_pace_raw_tap_open_flow_until_preseal(self) -> dict:
        return {
            "open_flow_until_preseal_window_begin_ts": "2026-05-19T18:05:00.000",
        }

    def end_pace_raw_tap_open_flow_until_preseal(self) -> dict:
        return {
            "open_flow_until_preseal_window_begin_ts": "2026-05-19T18:05:00.000",
            "open_flow_until_preseal_window_end_ts": "2026-05-19T18:05:10.000",
            "open_flow_vent1_max_gap_s": self.max_gap_s,
            "open_flow_vent1_gap_violation_count": 1 if self.max_gap_s > 1.2 else 0,
            "open_flow_vent1_gap_violation_times": (
                "[{\"gap_s\": %.3f, \"from\": \"2026-05-19T18:05:01.000\", "
                "\"to\": \"2026-05-19T18:05:02.600\"}]"
            )
            % self.max_gap_s,
            "open_flow_vent_gap_stage": "pace_status_evidence_before_vent0",
            "open_flow_vent_gap_blocking_operation": self.blocking_operation,
            "unexpected_state_changing_write_count": 0,
        }


def _write_raw_tap_csv(path: Path, rows: list[tuple[str, str, str, str]]) -> None:
    path.write_text(
        "wall_ts,direction,decoded_command,workflow_stage\n"
        + "\n".join(",".join(row) for row in rows)
        + "\n",
        encoding="utf-8",
    )


def _controlled_cfg(pressure_overrides: dict | None = None) -> dict:
    pressure = {
        "no_outp_transition_mode": True,
        "open_flow_output_off_mode": True,
        "controlled_outp_transition_mode": True,
        "vent0_to_seal_fixed_wait_s": 1.5,
        "co2_preseal_pressure_build_max_wait_s": 1.5,
        "preseal_pressure_build_hard_limit_hpa": 1200.0,
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
        "pre_vent0_quiet_gap_s": 0.0,
        "pre_vent0_quiet_gap_max_s": 0.0,
        "pre_vent0_require_no_new_vent1": True,
        "pre_vent0_use_raw_tap_last_vent1": True,
        "pre_vent0_vent_status_probe_enabled": False,
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
    logger: FakeLogger | None = None,
):
    logs: list[str] = []
    pace = pace or FakePace()
    gauge = gauge or FakeGauge()
    runner = CalibrationRunner(
        _controlled_cfg(pressure_overrides),
        {"pace": pace, "pressure_gauge": gauge},
        logger or FakeLogger(),
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


def _last_route_open_gate_fields(runner: CalibrationRunner) -> dict:
    gate_calls = [
        call for call in runner._append_pressure_trace_row.call_args_list
        if call.kwargs.get("trace_stage") == "route_open_clean_atmosphere_gate"
    ]
    assert gate_calls
    return gate_calls[-1].kwargs.get("extra_fields", {})


def _last_stage_fields(runner: CalibrationRunner, trace_stage: str) -> dict:
    calls = [
        call for call in runner._append_pressure_trace_row.call_args_list
        if call.kwargs.get("trace_stage") == trace_stage
    ]
    assert calls
    return calls[-1].kwargs.get("extra_fields", {})


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


def test_preseal_failure_is_group_failure_not_1100_point_failure() -> None:
    runner, _pace, _, _ = _runner()
    _prepare_co2_group_runner_for_seal_failure_tests(runner)
    lead = _ambient_co2_point()
    ambient = _ambient_co2_point()
    p1100 = _co2_point(index=4, pressure=1100.0, ppm=0.0)
    p1000 = _co2_point(index=5, pressure=1000.0, ppm=0.0)

    def fail_preseal(*_args, **_kwargs):
        runner._controlled_exit_final_decision = "FAIL_CLOSED_PRESEAL_OPEN_FLOW_VENT1_GAP"
        runner._co2_route_terminal_failure_decision = "FAIL_CLOSED_PRESEAL_OPEN_FLOW_VENT1_GAP"
        runner._co2_route_terminal_failure_reason = "VENT1 gap before route close"
        return False

    runner._pressurize_route_for_sealed_points = MagicMock(side_effect=fail_preseal)

    runner._run_co2_point(lead, pressure_points=[ambient, p1100, p1000])

    stages = _trace_stages(runner)
    assert "sealed_group_preseal_begin" in stages
    assert "sealed_group_preseal_end" in stages
    group_fields = _last_stage_fields(runner, "sealed_group_preseal_end")
    assert group_fields["sealed_group_preseal_result"] == "fail"
    assert group_fields["sealed_group_preseal_failed_before_first_point"] is True
    assert group_fields["sealed_group_aborted_before_pressure_points"] is True
    assert group_fields["first_sealed_point_started"] is False
    assert group_fields["first_sealed_point_target_hpa"] == 1100.0
    assert "co2_route_terminal_failure" not in stages
    assert "sealed_pressure_point_started" not in stages
    runner._set_pressure_to_target.assert_not_called()


def test_preseal_failure_aborts_all_sealed_points() -> None:
    runner, _pace, _, _ = _runner()
    _prepare_co2_group_runner_for_seal_failure_tests(runner)
    lead = _ambient_co2_point()
    p1100 = _co2_point(index=4, pressure=1100.0, ppm=0.0)
    p1000 = _co2_point(index=5, pressure=1000.0, ppm=0.0)

    def fail_preseal(*_args, **_kwargs):
        runner._controlled_exit_final_decision = "FAIL_CLOSED_PRESEAL_OPEN_FLOW_VENT1_GAP"
        runner._co2_route_terminal_failure_decision = "FAIL_CLOSED_PRESEAL_OPEN_FLOW_VENT1_GAP"
        runner._co2_route_terminal_failure_reason = "VENT1 gap before route close"
        return False

    runner._pressurize_route_for_sealed_points = MagicMock(side_effect=fail_preseal)

    runner._run_co2_point(lead, pressure_points=[p1100, p1000])

    runner._pressurize_route_for_sealed_points.assert_called_once()
    skipped_calls = [
        call for call in runner._append_pressure_trace_row.call_args_list
        if call.kwargs.get("trace_stage") == "sealed_pressure_point_skipped_group_preseal_failed"
    ]
    assert [call.kwargs.get("pressure_target_hpa") for call in skipped_calls] == [1100.0, 1000.0]
    assert all(
        call.kwargs.get("extra_fields", {}).get("point_skipped_because_group_preseal_failed") is True
        for call in skipped_calls
    )
    assert skipped_calls[-1].kwargs.get("extra_fields", {}).get(
        "next_sealed_point_not_attempted_after_preseal_fail"
    ) is True
    runner._sample_and_log.assert_not_called()


def test_cleanup_vent_after_preseal_fail_is_not_active_sealed_violation() -> None:
    runner, _pace, _, _ = _runner()

    runner._cleanup_co2_route(reason="FAIL_CLOSED_PRESEAL_OPEN_FLOW_VENT1_GAP")

    fields = _last_stage_fields(runner, "co2_route_cleanup_restore_atmosphere")
    assert fields["vent_after_valve_close_classification"] == "cleanup_restore_atmosphere"
    assert fields["active_sealed_vent_violation"] is False
    assert fields["cleanup_vent_after_abort"] is True


def test_cleanup_vent_after_abort_marks_line_contaminated() -> None:
    runner, _pace, _, _ = _runner()

    runner._cleanup_co2_route(reason="FAIL_CLOSED_PRESEAL_OPEN_FLOW_VENT1_GAP")

    fields = _last_stage_fields(runner, "co2_route_cleanup_restore_atmosphere")
    assert fields["cleanup_may_contaminate_line"] is True
    assert fields["samples_after_cleanup_allowed"] is False
    assert fields["rerun_requires_full_open_flow_flush"] is True


def test_active_sealed_vent1_before_terminal_is_p0() -> None:
    runner, pace, _, _ = _runner()
    point = _co2_point(pressure=1100.0)
    runner._activate_co2_sealed_no_vent_guard(point, reason="unit")

    assert runner._set_pressure_controller_vent(True, reason="unexpected sealed VENT1") is False

    fields = _last_stage_fields(runner, "sealed_no_vent_guard_blocked")
    assert fields["active_sealed_vent_violation"] is True
    assert fields["vent_after_valve_close_classification"] == "active_sealed_vent_violation"
    assert fields["samples_after_cleanup_allowed"] is False
    assert not any(call[0] == "vent" and call[1] is True for call in pace.calls)


def test_safe_stop_vent1_after_terminal_not_counted_as_sealed_vent() -> None:
    runner, pace, _, _ = _runner()

    runner._cleanup_co2_route(reason="FAIL_CLOSED_PRESEAL_OPEN_FLOW_VENT1_GAP")

    stages = _trace_stages(runner)
    assert "sealed_no_vent_guard_blocked" not in stages
    fields = _last_stage_fields(runner, "co2_route_cleanup_restore_atmosphere")
    assert fields["active_sealed_vent_violation"] is False
    assert fields["vent_after_valve_close_classification"] == "cleanup_restore_atmosphere"
    assert any(call[0] in {"start_hold", "vent"} for call in pace.calls)


def test_vent_after_valve_close_timeline_records_relay_and_safe_stop_order() -> None:
    runner, _pace, _, _ = _runner()

    runner._cleanup_co2_route(reason="FAIL_CLOSED_PRESEAL_OPEN_FLOW_VENT1_GAP")

    fields = _last_stage_fields(runner, "co2_route_cleanup_restore_atmosphere")
    assert fields["cleanup_safe_stop_begin_ts"]
    assert fields["cleanup_relay_reset_ts"]
    assert fields["cleanup_pace_vent_on_ts"]


def test_pressure_point_failure_only_after_route_closed() -> None:
    runner, _pace, _, _ = _runner()
    _prepare_co2_group_runner_for_seal_failure_tests(runner)
    lead = _co2_point(index=3, pressure=1100.0, ppm=0.0)
    p1100 = _co2_point(index=4, pressure=1100.0, ppm=0.0)
    runner._pressurize_route_for_sealed_points = MagicMock(return_value=True)
    runner._set_pressure_to_target = MagicMock(return_value=False)

    runner._run_co2_point(lead, pressure_points=[p1100])

    stages = _trace_stages(runner)
    assert stages.index("sealed_group_preseal_end") < stages.index("sealed_pressure_point_started")
    fields = _last_stage_fields(runner, "sealed_pressure_point_started")
    assert fields["sealed_group_preseal_result"] == "pass"
    assert fields["sealed_point_started_after_route_closed"] is True
    assert fields["first_sealed_point_started"] is True
    assert not any(stage == "sealed_pressure_point_skipped_group_preseal_failed" for stage in stages)


def test_first_sealed_point_starts_after_route_closed() -> None:
    runner, _pace, _, _ = _runner()
    _prepare_co2_group_runner_for_seal_failure_tests(runner)
    lead = _co2_point(index=3, pressure=1100.0, ppm=0.0)
    p1100 = _co2_point(index=4, pressure=1100.0, ppm=0.0)
    runner._pressurize_route_for_sealed_points = MagicMock(return_value=True)
    runner._set_pressure_to_target = MagicMock(return_value=False)

    runner._run_co2_point(lead, pressure_points=[p1100])

    fields = _last_stage_fields(runner, "sealed_pressure_point_started")
    assert fields["first_sealed_point_target_hpa"] == 1100.0
    assert fields["sealed_point_started_after_route_closed"] is True


def test_ambient_open_sample_success_does_not_imply_sealed_preseal_success() -> None:
    runner, _pace, _, _ = _runner()
    _prepare_co2_group_runner_for_seal_failure_tests(runner)
    lead = _ambient_co2_point()
    ambient = _ambient_co2_point()
    p1100 = _co2_point(index=4, pressure=1100.0, ppm=0.0)

    def fail_preseal(*_args, **_kwargs):
        runner._controlled_exit_final_decision = "FAIL_CLOSED_PRESEAL_OPEN_FLOW_VENT1_GAP"
        runner._co2_route_terminal_failure_decision = "FAIL_CLOSED_PRESEAL_OPEN_FLOW_VENT1_GAP"
        runner._co2_route_terminal_failure_reason = "VENT1 gap before route close"
        return False

    runner._pressurize_route_for_sealed_points = MagicMock(side_effect=fail_preseal)

    runner._run_co2_point(lead, pressure_points=[ambient, p1100])

    runner._sample_open_route_point.assert_called_once()
    runner._sample_and_log.assert_not_called()
    fields = _last_stage_fields(runner, "sealed_group_preseal_end")
    assert fields["sealed_group_preseal_result"] == "fail"


def test_group_preseal_fail_reports_all_sealed_refs_skipped() -> None:
    runner, _pace, _, _ = _runner()
    _prepare_co2_group_runner_for_seal_failure_tests(runner)
    lead = _co2_point(index=3, pressure=1100.0, ppm=0.0)
    p1100 = _co2_point(index=4, pressure=1100.0, ppm=0.0)
    p1000 = _co2_point(index=5, pressure=1000.0, ppm=0.0)

    def fail_preseal(*_args, **_kwargs):
        runner._controlled_exit_final_decision = "FAIL_CLOSED_PRESEAL_OPEN_FLOW_VENT1_GAP"
        runner._co2_route_terminal_failure_decision = "FAIL_CLOSED_PRESEAL_OPEN_FLOW_VENT1_GAP"
        runner._co2_route_terminal_failure_reason = "VENT1 gap before route close"
        return False

    runner._pressurize_route_for_sealed_points = MagicMock(side_effect=fail_preseal)

    runner._run_co2_point(lead, pressure_points=[p1100, p1000])

    group_fields = _last_stage_fields(runner, "sealed_group_preseal_end")
    assert group_fields["remaining_selected_pressure_points_skipped"] == "1100.0,1000.0"
    skipped_fields = [
        call.kwargs.get("extra_fields", {})
        for call in runner._append_pressure_trace_row.call_args_list
        if call.kwargs.get("trace_stage") == "sealed_pressure_point_skipped_group_preseal_failed"
    ]
    assert [fields["skipped_reason"] for fields in skipped_fields] == [
        "group_preseal_failed",
        "group_preseal_failed",
    ]


def test_above_target_sampling_only_runs_after_preseal_success() -> None:
    runner, _pace, _, _ = _runner()
    _prepare_co2_group_runner_for_seal_failure_tests(runner)
    lead = _co2_point(index=3, pressure=1100.0, ppm=0.0)
    p1100 = _co2_point(index=4, pressure=1100.0, ppm=0.0)
    p1000 = _co2_point(index=5, pressure=1000.0, ppm=0.0)

    def fail_preseal(*_args, **_kwargs):
        runner._controlled_exit_final_decision = "FAIL_CLOSED_PRESEAL_OPEN_FLOW_VENT1_GAP"
        runner._co2_route_terminal_failure_decision = "FAIL_CLOSED_PRESEAL_OPEN_FLOW_VENT1_GAP"
        runner._co2_route_terminal_failure_reason = "VENT1 gap before route close"
        return False

    runner._pressurize_route_for_sealed_points = MagicMock(side_effect=fail_preseal)

    runner._run_co2_point(lead, pressure_points=[p1100, p1000])

    runner._set_pressure_to_target.assert_not_called()
    runner._set_pressure_to_target_in_active_co2_sealed_sweep.assert_not_called()
    runner._wait_after_pressure_stable_before_sampling.assert_not_called()
    runner._sample_and_log.assert_not_called()


def test_co2_route_baseline_skips_redundant_outp0_when_output_already_off() -> None:
    runner, pace, _, _ = _runner()

    runner._set_pressure_controller_vent(True, reason="before CO2 route conditioning")

    assert ("output", False) not in pace.calls
    assert ("vent", True) in pace.calls


def test_co2_route_baseline_sends_outp0_when_output_is_on() -> None:
    runner, pace, _, _ = _runner()
    pace.output_state = 1

    runner._set_pressure_controller_vent(True, reason="before CO2 route conditioning")

    assert ("output", False) in pace.calls
    assert pace.calls.index(("output", False)) < pace.calls.index(("vent", True))


def test_co2_route_baseline_skips_redundant_isol1_when_isol_already_open() -> None:
    runner, pace, _, _ = _runner()

    runner._set_pressure_controller_vent(True, reason="before CO2 route conditioning")

    assert ("isolation", True) not in pace.calls


def test_open_flow_vent1_still_allowed() -> None:
    runner, pace, _, _ = _runner()

    assert runner._set_pressure_controller_vent(True, reason="before CO2 route conditioning") is True

    assert pace.get_vent_status() == 1
    assert runner._pace_vent_status_classification(1, stage="open_flow") == "open_flow_venting_ok"


def test_vent3_is_diagnostic_only_all_stages() -> None:
    runner, _, _, _ = _runner()

    for stage in ("route_open", "preseal", "sealed", "recovery"):
        fields = runner._pace_vent_status_diagnostic_fields(3, stage=stage)
        assert fields["vent_status_raw"] == 3
        assert fields["vent_status_classification"] == "pressure_build_or_window_latched"
        assert fields["vent_status_watchlist"] is True
        assert fields["vent_status_diagnostic_only"] is True
        assert fields["vent_status_gate_effect"] == "none"
        assert fields["vent_status_terminal"] is False

    assert runner._pace_vent_status_is_ready_for_control(3) is False


def test_co2_pre_route_idle_isolation_does_not_send_vent0() -> None:
    runner, pace, _, _ = _runner()

    runner._apply_co2_pre_route_idle_baseline(reason="before CO2 chamber wait")

    assert ("vent", False) not in pace.calls
    assert any(
        call.kwargs.get("trace_stage") == "co2_pre_route_idle_isolation_skipped"
        and call.kwargs.get("extra_fields", {}).get("pace_vent0_before_route_open_suppressed") is True
        for call in runner._append_pressure_trace_row.call_args_list
    )


def test_h2o_path_unchanged_by_co2_pre_route_fix() -> None:
    runner, pace, _, _ = _runner()

    runner._apply_idle_route_isolation(reason="before H2O point conditioning")

    assert ("vent", False) in pace.calls


def test_co2_route_conditioning_keeps_vent1_before_route_open() -> None:
    runner, pace, _, _ = _runner()

    assert runner._set_co2_route_baseline(reason="before CO2 route conditioning") is True

    assert ("vent", True) in pace.calls
    assert ("start_hold", 1.0) in pace.calls
    assert runner._pressure_atmosphere_hold_enabled is True


def test_open_flow_vent_keepalive_gap_guard_records_gap() -> None:
    runner, _pace, _, _ = _runner(logger=OpenFlowGapLogger(max_gap_s=1.6))
    point = _co2_point()

    runner._begin_co2_open_flow_until_preseal_raw_tap_window(point, reason="unit")

    assert runner._end_co2_open_flow_until_preseal_raw_tap_window(point, reason="unit") is False

    fields = _last_stage_fields(runner, "open_flow_until_preseal_raw_tap_end")
    assert fields["open_flow_vent1_max_gap_s"] == pytest.approx(1.6)
    assert fields["open_flow_vent1_gap_violation_count"] == 1
    assert fields["open_flow_vent1_gap_fail_count"] == 1
    assert fields["open_flow_vent_gap_may_cause_flow_drop"] is True
    assert fields["pre_vent_exit_flow_drop_suspected"] is True
    assert fields["open_flow_vent1_gap_guard_passed"] is False
    assert fields["open_flow_vent1_gap_fail_reason"]
    assert runner._controlled_exit_final_decision == "FAIL_CLOSED_PRESEAL_OPEN_FLOW_VENT1_GAP"


def test_open_flow_vent1_keepalive_not_blocked_by_pressure_query() -> None:
    runner, _pace, _, _ = _runner(logger=OpenFlowGapLogger(max_gap_s=1.0))
    point = _co2_point(pressure=1100.0)
    runner._read_precondition_dewpoint_gate_snapshot = MagicMock(return_value={"dewpoint_c": -35.0})
    runner._read_pace_pressure_now = MagicMock(side_effect=AssertionError("PACE pressure query blocked VENT1"))

    runner._begin_co2_open_flow_until_preseal_raw_tap_window(point, reason="unit")
    row = runner._read_co2_route_base_soak_dewpoint_trace_sample(route_open_wall_s=time.time() - 5.0)

    assert row["dewpoint_c"] == pytest.approx(-35.0)
    assert row["pace_pressure_nearest_hpa"] is None
    assert runner._pace_query_deferred_for_keepalive_count == 1
    assert "base_soak_trace_pace_pressure" in runner._pace_query_deferred_for_keepalive_types
    runner._read_pace_pressure_now.assert_not_called()


def test_open_flow_inl_query_deferred_to_preserve_vent1_keepalive() -> None:
    runner, pace, _, _ = _runner(logger=OpenFlowGapLogger(max_gap_s=1.0))
    point = _co2_point(pressure=1100.0)
    pace.read_pressure = MagicMock(side_effect=AssertionError(":SENS:PRES:INL? blocked VENT1"))

    runner._begin_co2_open_flow_until_preseal_raw_tap_window(point, reason="unit")

    assert runner._read_pace_pressure_now(pace) is None
    pace.read_pressure.assert_not_called()
    fields = runner._open_flow_keepalive_scheduler_fields()
    assert fields["open_flow_inl_query_deferred_for_keepalive_count"] == 1
    assert fields["open_flow_pressure_query_deferred_for_keepalive_count"] == 1
    assert fields["open_flow_critical_window_inl_query_blocked"] is True
    assert fields["open_flow_critical_window_pressure_query_blocked"] is True


def test_open_flow_pressure_query_deferred_to_preserve_vent1_keepalive() -> None:
    runner, pace, _, _ = _runner(logger=OpenFlowGapLogger(max_gap_s=1.0))
    point = _co2_point(pressure=1100.0)
    pace.read_pressure = MagicMock(side_effect=AssertionError("PACE pressure query blocked VENT1"))

    runner._begin_co2_open_flow_until_preseal_raw_tap_window(point, reason="unit")
    pressures = runner._read_controlled_outp_preseal_pressures()

    assert pressures["pace_pressure_hpa"] is None
    pace.read_pressure.assert_not_called()
    assert runner._open_flow_pressure_query_deferred_for_keepalive_count == 1


def test_pre_stop_hold_inl_query_forbidden_in_open_flow_critical_window() -> None:
    runner, pace, _, _ = _runner(logger=OpenFlowGapLogger(max_gap_s=1.0))
    point = _co2_point(pressure=1100.0)
    pace.pressure_queries = [":SENS:PRES:INL?"]
    pace.query_line_endings = [None]
    pace._parse_first_float = lambda _resp: 1013.0
    pace.query = MagicMock(side_effect=AssertionError(":SENS:PRES:INL? must defer"))
    pace.read_pressure = MagicMock(side_effect=AssertionError("read_pressure must defer"))

    runner._begin_co2_open_flow_until_preseal_raw_tap_window(point, reason="unit")

    with pytest.raises(RuntimeError, match="PACE_PRESSURE_QUERY_DEFERRED"):
        runner._read_pace_pressure_value(fast=True)

    pace.query.assert_not_called()
    pace.read_pressure.assert_not_called()
    fields = runner._open_flow_keepalive_scheduler_fields()
    assert fields["open_flow_inl_query_deferred_for_keepalive_count"] == 1
    assert fields["open_flow_pressure_query_deferred_for_keepalive_count"] == 1
    assert "fast_signal_pace_pressure" in fields["open_flow_critical_pace_query_deferred_types"]


def test_pre_stop_hold_pressure_query_deferred_in_open_flow_critical_window() -> None:
    runner, pace, _, _ = _runner(logger=OpenFlowGapLogger(max_gap_s=1.0))
    point = _co2_point(pressure=1100.0)
    pace.read_pressure = MagicMock(side_effect=AssertionError("PACE pressure must defer"))

    runner._begin_co2_open_flow_until_preseal_raw_tap_window(point, reason="unit")

    with pytest.raises(RuntimeError, match="PACE_PRESSURE_QUERY_DEFERRED"):
        runner._read_pace_pressure_value(fast=True)

    pace.read_pressure.assert_not_called()
    assert runner._open_flow_pressure_query_deferred_for_keepalive_count == 1


def test_1100_not_failed_by_pre_stop_hold_inl_query_gap() -> None:
    runner, pace, _, _ = _runner(logger=OpenFlowGapLogger(max_gap_s=1.0))
    point = _co2_point(pressure=1100.0)
    pace.read_pressure = MagicMock(side_effect=AssertionError("pre-stop INL query should defer"))

    runner._begin_co2_open_flow_until_preseal_raw_tap_window(point, reason="unit")
    assert runner._read_pace_pressure_now(pace) is None

    assert runner._end_co2_open_flow_until_preseal_raw_tap_window(point, reason="unit") is True
    fields = _last_stage_fields(runner, "open_flow_until_preseal_raw_tap_end")
    assert fields["open_flow_vent1_gap_guard_passed"] is True
    assert fields["open_flow_inl_query_deferred_for_keepalive_count"] == 1
    assert getattr(runner, "_controlled_exit_final_decision", "") != "FAIL_CLOSED_OPEN_FLOW_VENT1_KEEPALIVE_GAP"


def test_open_flow_vent1_gap_guard_passes_when_scheduler_priority_works() -> None:
    runner, _pace, _, _ = _runner(logger=OpenFlowGapLogger(max_gap_s=1.0))
    point = _co2_point(pressure=1100.0)

    runner._begin_co2_open_flow_until_preseal_raw_tap_window(point, reason="unit")

    assert runner._end_co2_open_flow_until_preseal_raw_tap_window(point, reason="unit") is True
    fields = _last_stage_fields(runner, "open_flow_until_preseal_raw_tap_end")
    assert fields["open_flow_vent1_gap_fail_count"] == 0
    assert fields["open_flow_vent1_gap_guard_passed"] is True
    assert fields["vent1_scheduler_owner"] == "pace_atmosphere_hold_thread"


def test_open_flow_vent1_gap_still_fails_when_real_gap_exceeds_limit() -> None:
    test_open_flow_vent_keepalive_gap_guard_records_gap()


def test_open_flow_gap_guard_ends_at_stop_hold_begin(tmp_path: Path) -> None:
    runner, _pace, _, _ = _runner(logger=FakeLogger())
    raw_path = tmp_path / "pace_raw_serial_tap.csv"
    _write_raw_tap_csv(
        raw_path,
        [
            ("2026-05-19T18:05:00.000", "WRITE", ":SOUR:PRES:LEV:IMM:AMPL:VENT 1", "open_flow"),
            ("2026-05-19T18:05:01.000", "WRITE", ":SOUR:PRES:LEV:IMM:AMPL:VENT 1", "open_flow"),
            ("2026-05-19T18:05:02.000", "WRITE", ":SOUR:PRES:LEV:IMM:AMPL:VENT 1", "open_flow"),
            ("2026-05-19T18:05:03.000", "WRITE", ":SOUR:PRES:LEV:IMM:AMPL:VENT 1", "open_flow"),
            ("2026-05-19T18:05:04.000", "WRITE", ":SOUR:PRES:LEV:IMM:AMPL:VENT 1", "open_flow"),
            ("2026-05-19T18:05:05.000", "WRITE", ":SOUR:PRES:LEV:IMM:AMPL:VENT 1", "open_flow"),
            ("2026-05-19T18:05:06.000", "WRITE", ":SOUR:PRES:LEV:IMM:AMPL:VENT 1", "open_flow"),
            ("2026-05-19T18:05:08.250", "WRITE", ":SENS:PRES:INL?", "quiet_gap"),
        ],
    )
    runner.logger.raw_serial_tap_csv_path = raw_path

    fields = runner._open_flow_vent_keepalive_gap_fields(
        {
            "open_flow_until_preseal_window_begin_ts": "2026-05-19T18:05:00.000",
            "open_flow_until_preseal_window_end_ts": "2026-05-19T18:05:09.000",
            "open_flow_vent1_gap_guard_window_end_ts": "2026-05-19T18:05:06.000",
            "open_flow_vent1_max_gap_s": 2.25,
            "open_flow_vent1_gap_violation_count": 1,
        }
    )

    assert fields["open_flow_vent1_gap_guard_window_end_ts"] == "2026-05-19T18:05:06.000"
    assert fields["open_flow_vent1_gap_fail_count"] == 0
    assert fields["open_flow_vent1_gap_guard_passed"] is True
    assert fields["vent1_gap_after_keepalive_stop_ignored_for_open_flow_guard"] is True


def test_open_flow_gap_before_stop_hold_still_fails(tmp_path: Path) -> None:
    runner, _pace, _, _ = _runner(logger=FakeLogger())
    raw_path = tmp_path / "pace_raw_serial_tap.csv"
    _write_raw_tap_csv(
        raw_path,
        [
            ("2026-05-19T18:05:00.000", "WRITE", ":SOUR:PRES:LEV:IMM:AMPL:VENT 1", "open_flow"),
            ("2026-05-19T18:05:02.000", "WRITE", ":SOUR:PRES:LEV:IMM:AMPL:VENT 1", "open_flow"),
        ],
    )
    runner.logger.raw_serial_tap_csv_path = raw_path

    fields = runner._open_flow_vent_keepalive_gap_fields(
        {
            "open_flow_until_preseal_window_begin_ts": "2026-05-19T18:05:00.000",
            "open_flow_until_preseal_window_end_ts": "2026-05-19T18:05:02.000",
            "open_flow_vent1_gap_guard_window_end_ts": "2026-05-19T18:05:02.000",
        }
    )

    assert fields["open_flow_vent1_gap_fail_count"] == 1
    assert fields["open_flow_vent1_gap_guard_passed"] is False
    assert fields["pre_vent_exit_flow_drop_suspected"] is True


def test_quiet_gap_after_stop_hold_uses_no_new_vent1_not_keepalive_gap() -> None:
    runner, pace, _, _ = _runner(
        pressure_overrides={
            "pre_vent0_quiet_gap_s": 0.0,
            "pre_vent0_quiet_gap_max_s": 0.0,
            "pre_vent0_vent_status_probe_enabled": True,
        },
        logger=QuietGapRawTapLogger(latest_wall_s=time.time() - 2.0),
    )

    runner._last_atmosphere_hold_stop_request_ts = time.time()
    runner._wait_pre_vent0_raw_tap_quiet_gap(pace)

    fields = runner._last_pre_vent0_quiet_gap_fields
    assert fields["quiet_gap_window_uses_no_new_vent1_check"] is True
    assert fields["vent1_gap_after_keepalive_stop_ignored_for_open_flow_guard"] is True


def test_open_flow_vent_keepalive_not_blocked_by_status_probe(monkeypatch) -> None:
    runner, _pace, _, _ = _runner(
        gauge=FakeGauge([1112.0, 1112.0]),
        logger=OpenFlowGapLogger(max_gap_s=1.0, blocking_operation="deferred_pace_status_evidence_before_vent0"),
    )
    point = _co2_point(pressure=900.0)
    runner._apply_valve_states = MagicMock()
    runner._begin_co2_open_flow_until_preseal_raw_tap_window(point, reason="unit")
    monkeypatch.setattr("time.sleep", lambda _seconds: None)

    assert runner._pressurize_route_for_sealed_points(point, route="co2", sealed_control_refs=[point]) is True

    stages = _trace_stages(runner)
    assert "pace_status_evidence:before_vent0" not in stages
    assert "pace_status_evidence_before_vent0_deferred" in stages
    fields = _last_stage_fields(runner, "open_flow_until_preseal_raw_tap_end")
    assert fields["open_flow_vent1_max_gap_s"] == pytest.approx(1.0)
    assert fields["open_flow_vent1_gap_fail_count"] == 0
    assert fields["pre_vent_exit_flow_drop_suspected"] is False
    assert fields["open_flow_vent1_gap_guard_passed"] is True
    start_reasons = [
        str(call.kwargs.get("reason") or "")
        for call in runner._start_pressure_transition_fast_signal_context.call_args_list
    ]
    assert "before CO2 pressure seal" not in start_reasons
    assert "after CO2 atmosphere exit" in start_reasons


def test_pre_stop_hold_outp_stat_query_deferred_to_preserve_vent1_keepalive() -> None:
    runner, _pace, _, _ = _runner(pace=BlockingStatePrimePace(), logger=OpenFlowGapLogger(max_gap_s=1.0))
    point = _co2_point(pressure=1100.0)
    plan = {
        "fast_signal_enabled": False,
        "active_entries": [],
        "passive_entries": [],
        "skip_slow_aux_prime": True,
    }

    runner._begin_co2_open_flow_until_preseal_raw_tap_window(point, reason="unit")
    runner._prime_sampling_window_context({"worker_plan": plan}, worker_plan=plan, reason="sampling window start")

    fields = runner._open_flow_keepalive_scheduler_fields()
    assert fields["open_flow_query_blocking_keepalive_prevented"] is True
    assert fields["open_flow_cached_outp_state_used"] is True
    assert fields["open_flow_cached_isol_state_used"] is True
    assert "sampling_pace_state_prime" in fields["open_flow_critical_pace_query_deferred_types"]


def test_open_flow_critical_window_uses_cached_outp_isol() -> None:
    runner, _pace, _, _ = _runner(pace=BlockingStatePrimePace())
    point = _co2_point(pressure=1100.0)

    runner._begin_co2_open_flow_until_preseal_raw_tap_window(point, reason="unit")
    snapshot = runner._pace_state_snapshot(refresh=False)

    assert snapshot["pace_output_state"] == ""
    assert snapshot["pace_isolation_state"] == ""


def test_mainthread_vent1_not_sent_during_keepalive_owner_active() -> None:
    runner, pace, _, _ = _runner(logger=OpenFlowGapLogger(max_gap_s=1.0))
    point = _co2_point()
    runner._read_precondition_dewpoint_gate_snapshot = MagicMock(return_value={"dewpoint_c": -35.0})

    runner._begin_co2_open_flow_until_preseal_raw_tap_window(point, reason="unit")
    runner._read_co2_route_base_soak_dewpoint_trace_sample(route_open_wall_s=time.time() - 1.0)

    assert pace.calls.count(("vent", True)) == 0
    assert runner._open_flow_keepalive_owner == "pace_atmosphere_hold_thread"


def test_1100_point_not_failed_by_synthetic_query_blocking_gap() -> None:
    runner, _pace, _, _ = _runner(logger=OpenFlowGapLogger(max_gap_s=1.0))
    point = _co2_point(pressure=1100.0)
    runner._read_precondition_dewpoint_gate_snapshot = MagicMock(return_value={"dewpoint_c": -35.0})
    runner._read_pace_pressure_now = MagicMock(side_effect=AssertionError("should be deferred"))

    runner._begin_co2_open_flow_until_preseal_raw_tap_window(point, reason="unit")
    runner._read_co2_route_base_soak_dewpoint_trace_sample(route_open_wall_s=time.time() - 1.0)

    assert runner._end_co2_open_flow_until_preseal_raw_tap_window(point, reason="unit") is True
    fields = _last_stage_fields(runner, "open_flow_until_preseal_raw_tap_end")
    assert fields["open_flow_vent1_gap_guard_passed"] is True
    assert getattr(runner, "_controlled_exit_final_decision", "") != "FAIL_CLOSED_OPEN_FLOW_VENT1_KEEPALIVE_GAP"


def test_quiet_gap_only_starts_after_preseal_ready(monkeypatch) -> None:
    runner, _pace, _, _ = _runner(
        gauge=FakeGauge([1112.0, 1112.0]),
        logger=OpenFlowGapLogger(max_gap_s=1.0),
    )
    point = _co2_point(pressure=900.0)
    runner._apply_valve_states = MagicMock()
    runner._begin_co2_open_flow_until_preseal_raw_tap_window(point, reason="unit")
    monkeypatch.setattr("time.sleep", lambda _seconds: None)

    assert runner._pressurize_route_for_sealed_points(point, route="co2", sealed_control_refs=[point]) is True

    stages = _trace_stages(runner)
    assert stages.index("open_flow_until_preseal_raw_tap_end") < stages.index("preseal_vent_off_begin")
    assert stages.index("open_flow_until_preseal_raw_tap_end") < stages.index("controlled_exit_atmosphere_begin")
    fields = _last_stage_fields(runner, "open_flow_until_preseal_raw_tap_end")
    assert fields["quiet_gap_started_before_stop_hold_detected"] is False


def test_co2_route_open_requires_clean_atmosphere_state() -> None:
    runner, pace, _, _ = _runner()
    point = _co2_point()
    runner._complete_pending_route_handoff = MagicMock(return_value=False)
    runner._set_valves_for_co2 = MagicMock()

    assert runner._open_co2_route_for_conditioning(point, point_tag="co2-1000") is True

    runner._set_valves_for_co2.assert_called_once_with(point)
    gate_calls = [
        call for call in runner._append_pressure_trace_row.call_args_list
        if call.kwargs.get("trace_stage") == "route_open_clean_atmosphere_gate"
    ]
    assert gate_calls
    fields = gate_calls[-1].kwargs.get("extra_fields", {})
    assert fields["route_open_allowed"] is True
    assert fields["vent_status_before_route_open"] == 1
    assert fields["output_state_before_route_open"] == 0
    assert fields["isolation_state_before_route_open"] == 1
    assert ("vent", True) in pace.calls


class Vent3LatchPace(FakePace):
    def vent(self, on: bool = True) -> None:
        self.calls.append(("vent", bool(on)))
        self.vent_status = 3 if on else 2


class Vent3HighPressurePace(Vent3LatchPace):
    def read_pressure(self) -> float:
        return 1100.0


class HoldThreadEvidencePace(FakePace):
    def __init__(self, *, age_s: float = 0.1) -> None:
        super().__init__()
        self.last_successful_vent1_monotonic_ts = time.monotonic() - float(age_s)
        self.last_successful_vent1_ts = time.time() - float(age_s)

    def is_atmosphere_hold_active(self) -> bool:
        return True


def test_route_open_allows_vent3_when_pressure_ambient_and_vent1_fresh() -> None:
    runner, _pace, _, _ = _runner(pace=Vent3LatchPace())
    point = _co2_point()
    runner._complete_pending_route_handoff = MagicMock(return_value=False)
    runner._set_valves_for_co2 = MagicMock()

    assert runner._open_co2_route_for_conditioning(point, point_tag="co2-1000") is True

    runner._set_valves_for_co2.assert_called_once_with(point)
    gate_calls = [
        call for call in runner._append_pressure_trace_row.call_args_list
        if call.kwargs.get("trace_stage") == "route_open_clean_atmosphere_gate"
    ]
    fields = gate_calls[-1].kwargs.get("extra_fields", {})
    assert fields["vent_status_before_route_open"] == 3
    assert fields["vent_status_before_route_open_classification"] == "pressure_build_or_window_latched"
    assert fields["vent_status_watchlist"] is True
    assert fields["vent_status_gate_effect"] == "none"
    assert fields["route_open_allowed"] is True


def test_route_open_fails_by_pressure_not_vent3() -> None:
    runner, _pace, _, _ = _runner(pace=Vent3HighPressurePace())
    point = _co2_point()
    runner._complete_pending_route_handoff = MagicMock(return_value=False)
    runner._set_valves_for_co2 = MagicMock()

    assert runner._open_co2_route_for_conditioning(point, point_tag="co2-1000") is False

    runner._set_valves_for_co2.assert_not_called()
    gate_calls = [
        call for call in runner._append_pressure_trace_row.call_args_list
        if call.kwargs.get("trace_stage") == "route_open_clean_atmosphere_gate"
    ]
    fields = gate_calls[-1].kwargs.get("extra_fields", {})
    assert fields["vent_status_watchlist"] is True
    assert fields["route_open_allowed"] is False
    assert fields["route_open_block_reason"] == "ROUTE_OPEN_PRESSURE_NOT_AMBIENT"


def test_co2_route_open_blocks_when_vent1_stale() -> None:
    runner, pace, _, _ = _runner()
    point = _co2_point()
    runner._pressure_atmosphere_hold_enabled = True
    runner._last_pressure_atmosphere_refresh_ts = time.time() - 10.0
    pace.vent_status = 1

    assert runner._co2_route_open_clean_atmosphere_gate(point, point_tag="co2-1000") is False

    assert runner._controlled_exit_final_decision == "FAIL_CLOSED_ROUTE_OPEN_PACE_ATMOSPHERE_NOT_CLEAN"
    gate_calls = [
        call for call in runner._append_pressure_trace_row.call_args_list
        if call.kwargs.get("trace_stage") == "route_open_clean_atmosphere_gate"
    ]
    assert gate_calls[-1].kwargs.get("extra_fields", {})["route_open_block_reason"] == "ROUTE_OPEN_VENT1_HEARTBEAT_STALE"


def test_route_open_vent1_freshness_prefers_raw_tap() -> None:
    runner, pace, _, _ = _runner(logger=RawTapLogger(enabled=True, vent1_age_s=0.047))
    point = _co2_point()
    runner._pressure_atmosphere_hold_enabled = True
    runner._last_pressure_atmosphere_refresh_ts = time.time() - 11.0
    pace.vent_status = 1

    assert runner._co2_route_open_clean_atmosphere_gate(point, point_tag="co2-1000") is True

    fields = _last_route_open_gate_fields(runner)
    assert fields["route_open_allowed"] is True
    assert fields["vent1_last_refresh_source"] == "raw_tap"
    assert fields["vent1_freshness_decision"] == "fresh"
    assert fields["vent1_last_refresh_age_s"] < 1.0


def test_route_open_records_vent1_freshness_source_mismatch() -> None:
    runner, pace, _, _ = _runner(logger=RawTapLogger(enabled=True, vent1_age_s=0.047))
    point = _co2_point()
    runner._pressure_atmosphere_hold_enabled = True
    runner._last_pressure_atmosphere_refresh_ts = time.time() - 11.0
    pace.vent_status = 1

    assert runner._co2_route_open_clean_atmosphere_gate(point, point_tag="co2-1000") is True

    fields = _last_route_open_gate_fields(runner)
    assert fields["vent1_freshness_source_mismatch"] is True
    assert fields["vent1_freshness_source_mismatch_delta_s"] > 1.0
    assert fields["vent1_last_refresh_runner_ts"] is not None
    assert fields["vent1_last_refresh_raw_tap_ts"] == "2026-05-17T22:41:25.257"


def test_route_open_fails_when_raw_tap_vent1_really_stale() -> None:
    runner, pace, _, _ = _runner(logger=RawTapLogger(enabled=True, vent1_age_s=5.0))
    point = _co2_point()
    runner._pressure_atmosphere_hold_enabled = True
    runner._last_pressure_atmosphere_refresh_ts = time.time()
    pace.vent_status = 1

    assert runner._co2_route_open_clean_atmosphere_gate(point, point_tag="co2-1000") is False

    fields = _last_route_open_gate_fields(runner)
    assert fields["vent1_last_refresh_source"] == "raw_tap"
    assert fields["vent1_freshness_decision"] == "stale"
    assert fields["route_open_block_reason"] == "ROUTE_OPEN_VENT1_HEARTBEAT_STALE"


def test_route_open_uses_hold_thread_when_raw_tap_disabled() -> None:
    runner, pace, _, _ = _runner(
        pace=HoldThreadEvidencePace(age_s=0.1),
        logger=RawTapLogger(enabled=False, vent1_age_s=None),
    )
    point = _co2_point()
    runner._last_pressure_atmosphere_refresh_ts = time.time() - 11.0
    pace.vent_status = 1

    assert runner._co2_route_open_clean_atmosphere_gate(point, point_tag="co2-1000") is True

    fields = _last_route_open_gate_fields(runner)
    assert fields["vent1_last_refresh_source"] == "atmosphere_hold_thread"
    assert fields["vent1_freshness_decision"] == "fresh"
    assert fields["vent1_last_refresh_hold_thread_ts"] != ""


def test_route_open_fallback_runner_internal_marked() -> None:
    runner, pace, _, _ = _runner(logger=RawTapLogger(enabled=False, vent1_age_s=None))
    point = _co2_point()
    runner._pressure_atmosphere_hold_enabled = True
    runner._last_pressure_atmosphere_refresh_ts = time.time() - 0.1
    pace.vent_status = 1

    assert runner._co2_route_open_clean_atmosphere_gate(point, point_tag="co2-1000") is True

    fields = _last_route_open_gate_fields(runner)
    assert fields["vent1_last_refresh_source"] == "runner_internal"
    assert fields["vent1_freshness_decision"] == "fresh"


def test_raw_tap_route_open_precheck_records_clean_state() -> None:
    runner, _pace, _, _ = _runner()
    point = _co2_point()
    runner._complete_pending_route_handoff = MagicMock(return_value=False)
    runner._set_valves_for_co2 = MagicMock()

    assert runner._open_co2_route_for_conditioning(point, point_tag="co2-1000") is True

    gate_calls = [
        call for call in runner._append_pressure_trace_row.call_args_list
        if call.kwargs.get("trace_stage") == "route_open_clean_atmosphere_gate"
    ]
    assert gate_calls
    fields = gate_calls[-1].kwargs.get("extra_fields", {})
    assert set(
        [
            "vent_status_before_route_open",
            "vent_status_before_route_open_classification",
            "output_state_before_route_open",
            "isolation_state_before_route_open",
            "pace_pressure_before_route_open_hpa",
            "com22_pressure_before_route_open_hpa",
            "atmosphere_hold_active_before_route_open",
            "vent1_last_refresh_age_s",
            "actual_open_valves_before_route_open",
            "route_open_allowed",
        ]
    ).issubset(fields)


def test_sealed_control_blocks_vent3_before_sampling() -> None:
    runner, pace, _, _ = _runner()
    point = _co2_point(pressure=1100.0)
    pace.vent_status = 3
    pace.in_limits = [(1100.0, 1)]
    runner._activate_co2_sealed_no_vent_guard(point, reason="test")
    runner._begin_active_co2_sealed_sweep_context(point)

    assert runner._set_pressure_to_target_in_active_co2_sealed_sweep(point) is False

    stages = _trace_stages(runner)
    assert "sealed_vent_status_watchlist" in stages
    assert "sealed_sweep_live_check_fail" in stages
    assert ("setpoint", 1100.0) not in pace.calls


def test_recovery_does_not_fail_on_vent3_alone() -> None:
    runner, pace, gauge, _ = _runner(pressure_overrides={"no_outp_transition_mode": False})
    point = _co2_point(pressure=1100.0)
    pace.output_state = 0
    pace.isolation_state = 1
    pace.vent_status = 3
    gauge.values = [1013.0]

    assert runner._attempt_pressure_controller_output_on_recovery(
        point,
        phase="co2",
        pressure_target_hpa=1100.0,
        note="unit recovery",
    ) is True

    assert ("enable_control_output",) in pace.calls


def test_run_start_allows_output_state_zero_in_controlled_mode() -> None:
    runner, pace, _, logs = _runner()
    pace.output_state = 0

    assert runner._check_pressure_output_preflight() is None
    assert any("[controlled-outp-preflight]" in message for message in logs)


def test_controlled_exit_uses_direct_vent0_before_route_close(monkeypatch) -> None:
    runner, pace, _, _ = _runner(gauge=FakeGauge([1013.0, 1013.0]))
    runner._apply_valve_states = MagicMock()
    monkeypatch.setattr("time.sleep", lambda _seconds: None)

    assert runner._pressurize_route_for_sealed_points(_co2_point(), route="co2", sealed_control_refs=[_co2_point()])

    assert ("vent", False) in pace.calls
    assert ("exit_atmosphere_mode", 3.0, 0.2) not in pace.calls
    stages = _trace_stages(runner)
    assert "controlled_exit_atmosphere_begin" in stages
    assert "controlled_exit_atmosphere_driver_exit_begin" in stages
    assert "controlled_exit_atmosphere_driver_exit_done" in stages
    assert "controlled_exit_atmosphere_verify" not in stages
    assert "operator_window_check_begin" not in stages
    assert "operator_window_check_result" not in stages
    assert "post_seal_vent_abort_clear" in stages
    assert "route_sealed" in stages


def test_hold_stop_waits_for_thread_or_records_alive() -> None:
    now_s = time.time()
    runner, pace, _, _ = _runner(
        pace=LingeringHoldPace(),
        logger=QuietGapRawTapLogger(latest_wall_s=now_s - 0.1),
    )

    assert runner._stop_pressure_controller_atmosphere_hold(pace, reason="unit-test") is False

    assert runner._last_atmosphere_hold_stop_request_ts is not None
    assert runner._last_atmosphere_hold_stop_thread_join_begin_ts is not None
    assert runner._last_atmosphere_hold_stop_thread_join_return_ts is not None
    assert runner._last_atmosphere_hold_thread_alive_after_stop is True
    assert runner._last_atmosphere_hold_raw_last_vent1_before_stop_ts
    assert runner._last_atmosphere_hold_raw_last_vent1_after_stop_ts


def test_pre_vent0_uses_raw_tap_last_vent1() -> None:
    now_s = time.time()
    raw_wall_s = now_s - 0.01
    runner, pace, _, _ = _runner(
        pace=HoldThreadEvidencePace(age_s=30.0),
        pressure_overrides={"pre_vent0_quiet_gap_s": 0.0, "pre_vent0_quiet_gap_max_s": 0.0},
        logger=QuietGapRawTapLogger(latest_wall_s=raw_wall_s),
    )
    runner._last_atmosphere_hold_stop_request_ts = now_s - 0.5
    runner._last_atmosphere_hold_stop_thread_join_return_ts = now_s - 0.4
    runner._last_atmosphere_hold_stop_return_ts = now_s - 0.4
    runner._last_atmosphere_hold_raw_last_vent1_before_stop_ts = datetime.fromtimestamp(now_s - 30.0).isoformat(
        timespec="milliseconds"
    )
    runner._last_atmosphere_hold_raw_last_vent1_after_stop_ts = datetime.fromtimestamp(raw_wall_s).isoformat(
        timespec="milliseconds"
    )

    runner._wait_pre_vent0_raw_tap_quiet_gap(pace)

    fields = runner._last_pre_vent0_quiet_gap_fields
    assert fields["pre_vent0_quiet_gap_source"] == "raw_tap"
    assert fields["raw_last_vent1_used_for_vent0_gap_ts"] == datetime.fromtimestamp(raw_wall_s).isoformat(
        timespec="milliseconds"
    )
    assert fields["raw_last_vent1_used_for_vent0_gap_ts"] != fields["raw_last_vent1_before_stop_ts"]


def test_pre_vent0_waits_quiet_gap_after_raw_last_vent1() -> None:
    now_s = time.time()
    runner, pace, _, _ = _runner(
        pressure_overrides={"pre_vent0_quiet_gap_s": 0.03, "pre_vent0_quiet_gap_max_s": 0.05},
        logger=QuietGapRawTapLogger(latest_wall_s=now_s - 0.005),
    )
    runner._last_atmosphere_hold_stop_request_ts = now_s - 0.5
    runner._last_atmosphere_hold_stop_thread_join_return_ts = now_s - 0.4
    runner._last_atmosphere_hold_stop_return_ts = now_s - 0.4

    runner._wait_pre_vent0_raw_tap_quiet_gap(pace)

    fields = runner._last_pre_vent0_quiet_gap_fields
    assert fields["pre_vent0_quiet_gap_actual_s"] >= 0.025
    assert fields["pre_vent0_quiet_gap_satisfied"] is True


def test_pre_vent0_quiet_gap_resets_if_new_vent1_after_stop() -> None:
    now_s = time.time()
    later_wall_s = now_s + 0.005
    runner, pace, _, _ = _runner(
        pressure_overrides={"pre_vent0_quiet_gap_s": 0.02, "pre_vent0_quiet_gap_max_s": 0.05},
        logger=QuietGapRawTapLogger(
            latest_wall_s=now_s - 0.02,
            sequence_wall_s=[now_s - 0.02, later_wall_s],
            post_stop_count=1,
        ),
    )
    runner._last_atmosphere_hold_stop_request_ts = now_s - 0.5
    runner._last_atmosphere_hold_stop_thread_join_return_ts = now_s - 0.4
    runner._last_atmosphere_hold_stop_return_ts = now_s - 0.4

    runner._wait_pre_vent0_raw_tap_quiet_gap(pace)

    fields = runner._last_pre_vent0_quiet_gap_fields
    assert fields["raw_last_vent1_used_for_vent0_gap_ts"] == datetime.fromtimestamp(later_wall_s).isoformat(
        timespec="milliseconds"
    )
    assert fields["post_stop_new_vent1_count"] == 1
    assert fields["pre_vent0_quiet_gap_actual_s"] >= 0.015


def test_pre_vent0_quiet_gap_interrupted_by_pressure_safety(monkeypatch) -> None:
    now_s = time.time()
    runner, pace, _, _ = _runner(
        gauge=FakeGauge([1200.0, 1200.0, 1200.0]),
        pressure_overrides={"pre_vent0_quiet_gap_s": 2.0, "pre_vent0_quiet_gap_max_s": 3.0},
        logger=QuietGapRawTapLogger(latest_wall_s=now_s - 0.01),
    )
    runner._apply_valve_states = MagicMock()
    monkeypatch.setattr("time.sleep", lambda _seconds: None)

    assert runner._pressurize_route_for_sealed_points(
        _co2_point(),
        route="co2",
        sealed_control_refs=[_co2_point()],
    ) is False

    fields = _last_stage_fields(runner, "controlled_outp_vent0_fixed_wait_before_seal")
    assert fields["quiet_gap_interrupted_by_pressure"] is True
    assert fields["quiet_gap_pressure_hpa"] == pytest.approx(1200.0)
    assert ("vent", False) in pace.calls
    runner._apply_valve_states.assert_called_once_with([])
    assert runner._controlled_exit_final_decision == "FAIL_CLOSED_PRESEAL_PRESSURE_BUILD_HARD_LIMIT"


def test_pre_vent0_quiet_gap_does_not_cancel_open_flow_keepalive() -> None:
    now_s = time.time()
    runner, pace, _, _ = _runner(
        pressure_overrides={"pre_vent0_quiet_gap_s": 0.0, "pre_vent0_quiet_gap_max_s": 0.0},
        logger=QuietGapRawTapLogger(latest_wall_s=now_s - 0.1),
    )

    assert runner._set_co2_route_baseline(reason="unit open-flow") is True
    assert ("vent", True) in pace.calls
    assert ("start_hold", 1.0) in pace.calls

    assert runner._stop_pressure_controller_atmosphere_hold(pace, reason="unit pre-VENT0") is True
    runner._wait_pre_vent0_raw_tap_quiet_gap(pace)

    assert ("start_hold", 1.0) in pace.calls
    assert ("vent", False) not in pace.calls


def test_pre_vent0_optional_vent_status_probe_does_not_block_route_close() -> None:
    now_s = time.time()
    runner, pace, _, _ = _runner(
        pace=ProbeFailOncePace(),
        pressure_overrides={
            "pre_vent0_quiet_gap_s": 0.0,
            "pre_vent0_quiet_gap_max_s": 0.0,
            "pre_vent0_vent_status_probe_enabled": True,
        },
        logger=QuietGapRawTapLogger(latest_wall_s=now_s - 0.1),
    )

    method, _status, _error = runner._controlled_exit_atmosphere_command(
        pace,
        timeout_s=3.0,
        poll_s=0.2,
        fast_preseal_no_wait=True,
    )

    fields = runner._last_pre_vent0_quiet_gap_fields
    assert method == "probe_unavailable_abort_fallback"
    assert fields["pre_vent0_vent_status_probe_enabled"] is True
    assert "VENT? probe timeout" in fields["pre_vent0_vent_status_probe_error"]
    assert ("vent", False) in pace.calls


def test_pre_vent0_skips_abort_when_vent_status_2_completed() -> None:
    now_s = time.time()
    runner, pace, _, _ = _runner(
        pace=SequenceVentStatusPace([2]),
        pressure_overrides={
            "pre_vent0_quiet_gap_s": 0.0,
            "pre_vent0_quiet_gap_max_s": 0.0,
            "pre_vent0_vent_status_probe_enabled": True,
            "pre_vent0_status_probe_enabled": True,
        },
        logger=QuietGapRawTapLogger(latest_wall_s=now_s - 0.1),
    )

    method, status, error = runner._controlled_exit_atmosphere_command(
        pace,
        timeout_s=3.0,
        poll_s=0.2,
        fast_preseal_no_wait=True,
    )

    fields = runner._last_pre_vent0_quiet_gap_fields
    assert method == "completed_no_abort"
    assert status == 2
    assert error == ""
    assert ("vent", False) not in pace.calls
    assert fields["vent_abort_sent"] is False
    assert fields["vent_exit_method"] == "completed_no_abort"
    assert fields["vent_status_preseal"] == 2
    assert fields["vent2_transient_only"] is True
    assert fields["vent_exit_reference_ts"]


def test_pre_vent0_skips_abort_when_vent_status_0_ok() -> None:
    now_s = time.time()
    runner, pace, _, _ = _runner(
        pace=SequenceVentStatusPace([0]),
        pressure_overrides={
            "pre_vent0_quiet_gap_s": 0.0,
            "pre_vent0_quiet_gap_max_s": 0.0,
            "pre_vent0_vent_status_probe_enabled": True,
        },
        logger=QuietGapRawTapLogger(latest_wall_s=now_s - 0.1),
    )

    method, status, _error = runner._controlled_exit_atmosphere_command(
        pace,
        timeout_s=3.0,
        poll_s=0.2,
        fast_preseal_no_wait=True,
    )

    assert method == "already_ok_no_abort"
    assert status == 0
    assert ("vent", False) not in pace.calls


def test_pre_vent0_aborts_when_vent_status_1_still_in_progress_after_grace() -> None:
    now_s = time.time()
    runner, pace, _, _ = _runner(
        pace=SequenceVentStatusPace([1, 1]),
        pressure_overrides={
            "pre_vent0_quiet_gap_s": 0.0,
            "pre_vent0_quiet_gap_max_s": 0.0,
            "pre_vent0_vent_status_probe_enabled": True,
            "pre_vent0_in_progress_grace_s": 0.001,
        },
        logger=QuietGapRawTapLogger(latest_wall_s=now_s - 0.1),
    )

    method, _status, _error = runner._controlled_exit_atmosphere_command(
        pace,
        timeout_s=3.0,
        poll_s=0.2,
        fast_preseal_no_wait=True,
    )

    assert method == "abort_in_progress"
    assert ("vent", False) in pace.calls
    assert runner._last_pre_vent0_quiet_gap_fields["vent_abort_sent"] is True


def test_pre_vent0_waits_short_grace_when_vent_status_1_then_2() -> None:
    now_s = time.time()
    runner, pace, _, _ = _runner(
        pace=SequenceVentStatusPace([1, 2]),
        pressure_overrides={
            "pre_vent0_quiet_gap_s": 0.0,
            "pre_vent0_quiet_gap_max_s": 0.0,
            "pre_vent0_vent_status_probe_enabled": True,
            "pre_vent0_in_progress_grace_s": 0.001,
        },
        logger=QuietGapRawTapLogger(latest_wall_s=now_s - 0.1),
    )

    method, status, _error = runner._controlled_exit_atmosphere_command(
        pace,
        timeout_s=3.0,
        poll_s=0.2,
        fast_preseal_no_wait=True,
    )

    fields = runner._last_pre_vent0_quiet_gap_fields
    assert method == "completed_no_abort"
    assert status == 2
    assert ("vent", False) not in pace.calls
    assert fields["pre_vent0_status_probe_grace_used"] is True


def test_pre_vent0_vent3_preseal_routes_close_then_blocks_control() -> None:
    now_s = time.time()
    runner, pace, _, _ = _runner(
        pace=SequenceVentStatusPace([3]),
        pressure_overrides={
            "pre_vent0_quiet_gap_s": 0.0,
            "pre_vent0_quiet_gap_max_s": 0.0,
            "pre_vent0_vent_status_probe_enabled": True,
        },
        logger=QuietGapRawTapLogger(latest_wall_s=now_s - 0.1),
    )

    method, status, _error = runner._controlled_exit_atmosphere_command(
        pace,
        timeout_s=3.0,
        poll_s=0.2,
        fast_preseal_no_wait=True,
    )

    failures = runner._pressure_controller_ready_failures(
        {"pace_vent_status": 3, "pace_output_state": 0, "pace_isolation_state": 1},
        pace,
    )
    assert method == "preseal_vent3_unrecognized"
    assert status == 3
    assert ("vent", False) not in pace.calls
    assert "vent_window_latched" in failures


def test_control_ready_allows_vent_status_2_completed() -> None:
    runner, pace, _, _ = _runner()

    failures = runner._pressure_controller_ready_failures(
        {"pace_vent_status": 2, "pace_output_state": 0, "pace_isolation_state": 1},
        pace,
    )

    assert failures == []


def test_control_ready_blocks_vent_status_1_or_3() -> None:
    runner, pace, _, _ = _runner()

    failures_1 = runner._pressure_controller_ready_failures(
        {"pace_vent_status": 1, "pace_output_state": 0, "pace_isolation_state": 1},
        pace,
    )
    failures_3 = runner._pressure_controller_ready_failures(
        {"pace_vent_status": 3, "pace_output_state": 0, "pace_isolation_state": 1},
        pace,
    )

    assert "vent_status=1" in failures_1
    assert "vent_window_latched" in failures_3


def test_skip_abort_path_has_no_vent0_raw_tx() -> None:
    now_s = time.time()
    runner, pace, _, _ = _runner(
        pace=SequenceVentStatusPace([2]),
        pressure_overrides={
            "pre_vent0_quiet_gap_s": 0.0,
            "pre_vent0_quiet_gap_max_s": 0.0,
            "pre_vent0_vent_status_probe_enabled": True,
        },
        logger=QuietGapRawTapLogger(latest_wall_s=now_s - 0.1),
    )

    runner._controlled_exit_atmosphere_command(
        pace,
        timeout_s=3.0,
        poll_s=0.2,
        fast_preseal_no_wait=True,
    )
    fields = runner._collect_post_vent0_probe_fields(
        phase="co2",
        vent0_intent_ts=None,
        route_valves_still_open=True,
        allow_device_queries=False,
        vent_abort_sent=False,
        vent_exit_reference_ts=runner._last_vent_exit_reference_ts,
    )

    assert fields["vent_abort_sent"] is False
    assert fields["preseal_vent0_raw_tx_ts"] == ""


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


def test_driver_active_vent_status_no_longer_blocks_preseal_close(monkeypatch) -> None:
    runner, _, _, _ = _runner(pace=ActiveVentAfterOffPace(), gauge=FakeGauge([1013.0, 1013.0]))
    runner._apply_valve_states = MagicMock()
    monkeypatch.setattr("time.sleep", lambda _seconds: None)

    assert runner._pressurize_route_for_sealed_points(_co2_point(), route="co2", sealed_control_refs=[_co2_point()]) is True

    runner._apply_valve_states.assert_called_once_with([])
    assert "controlled_exit_atmosphere_fail" not in _trace_stages(runner)


def test_preseal_buildup_closes_route_then_blocks_vent3_before_outp1(monkeypatch) -> None:
    runner, pace, _, _ = _runner(
        pace=Vent3PersistPace(),
        gauge=FakeGauge([1013.0, 1018.0, 1022.0, 1024.0, 1026.0, 1028.0, 1030.0]),
        logger=PostVent0RawTapLogger(vent1_count=0),
    )
    runner._apply_valve_states = MagicMock()
    monkeypatch.setattr("time.sleep", lambda _seconds: None)

    assert runner._pressurize_route_for_sealed_points(_co2_point(), route="co2", sealed_control_refs=[_co2_point()]) is False

    stages = _trace_stages(runner)
    assert ("vent", False) in pace.calls
    assert ("exit_atmosphere_mode", 3.0, 0.2) not in pace.calls
    assert "route_valves_closed_after_vent0" in stages
    assert "post_seal_vent_abort_clear" in stages
    assert "controlled_exit_atmosphere_fail" not in stages
    runner._apply_valve_states.assert_called()
    fields = _last_stage_fields(runner, "controlled_outp_vent0_fixed_wait_before_seal")
    assert fields["post_vent0_vent3_watchlist_only"] is True
    assert _last_stage_fields(runner, "controlled_outp_vent0_fixed_wait_before_seal")[
        "post_vent0_new_vent1_count"
    ] == 0
    clear_fields = _last_stage_fields(runner, "post_seal_vent_abort_clear")
    assert clear_fields["sealed_control_ready_blocks_outp1"] is True
    assert clear_fields["outp1_blocked_reason"] == "FAIL_CLOSED_PRESSURE_CONTROLLER_VENT_WINDOW_LATCHED_BEFORE_CONTROL"
    assert ("enable_control_output",) not in pace.calls


def test_post_vent0_probe_records_vent_outp_isol_pressure_timeline() -> None:
    vent0_ts = time.time()
    runner, pace, _, _ = _runner(
        pace=LegacyTrappedVentAfterOffPace(),
        gauge=FakeGauge([1013.0, 1014.0, 1015.0, 1016.0, 1017.0, 1018.0, 1019.0]),
        logger=PostVent0RawTapLogger(vent1_count=0, vent0_wall_ts=vent0_ts),
    )
    pace.exit_atmosphere_mode(timeout_s=3.0, poll_s=0.2)

    fields = runner._collect_post_vent0_probe_fields(
        phase="co2",
        vent0_intent_ts=vent0_ts,
        route_valves_still_open=True,
        max_relative_s=3.0,
    )

    schedule = json.loads(fields["post_vent0_probe_schedule_s"])
    vent_timeline = json.loads(fields["post_vent0_vent_status_timeline"])
    outp_timeline = json.loads(fields["post_vent0_outp_status_timeline"])
    isol_timeline = json.loads(fields["post_vent0_isol_status_timeline"])
    pace_timeline = json.loads(fields["post_vent0_pace_pressure_timeline"])
    com22_timeline = json.loads(fields["post_vent0_com22_pressure_timeline"])
    assert schedule == [0.0, 0.2, 0.5, 1.0, 1.5, 2.0, 3.0]
    assert [row["relative_s"] for row in vent_timeline] == schedule
    assert {row["value"] for row in vent_timeline} == {3}
    assert {row["value"] for row in outp_timeline} == {0}
    assert {row["value"] for row in isol_timeline} == {1}
    assert len(pace_timeline) == len(schedule)
    assert len(com22_timeline) == len(schedule)
    assert fields["post_vent0_new_vent1_count"] == 0
    assert fields["post_vent0_vent3_count"] == len(schedule)
    assert fields["post_vent0_vent3_watchlist_only"] is True
    assert fields["post_vent0_pressure_build_observed"] is True
    assert fields["post_vent0_route_valves_still_open"] is True


def test_post_vent0_new_vent1_is_flagged(monkeypatch) -> None:
    runner, _pace, _, _ = _runner(
        gauge=FakeGauge([1013.0, 1013.0]),
        logger=PostVent0RawTapLogger(vent1_count=1),
    )
    runner._apply_valve_states = MagicMock()
    runner._cleanup_co2_route = MagicMock()
    monkeypatch.setattr("time.sleep", lambda _seconds: None)

    assert runner._pressurize_route_for_sealed_points(
        _co2_point(),
        route="co2",
        sealed_control_refs=[_co2_point()],
    ) is False

    fields = _last_stage_fields(runner, "controlled_outp_vent0_fixed_wait_before_seal")
    assert fields["post_vent0_new_vent1_count"] == 1
    assert runner._controlled_exit_final_decision == "FAIL_CLOSED_VENT1_AFTER_PRESEAL_VENT0"
    runner._apply_valve_states.assert_not_called()
    runner._cleanup_co2_route.assert_called_once()


def test_no_outp1_or_setpoint_before_route_close(monkeypatch) -> None:
    runner, _pace, _, _ = _runner(
        gauge=FakeGauge([1013.0, 1013.0]),
        logger=PostVent0RawTapLogger(vent1_count=0, outp1_count=1, setpoint_count=1),
    )
    runner._apply_valve_states = MagicMock()
    runner._cleanup_co2_route = MagicMock()
    monkeypatch.setattr("time.sleep", lambda _seconds: None)

    assert runner._pressurize_route_for_sealed_points(
        _co2_point(),
        route="co2",
        sealed_control_refs=[_co2_point()],
    ) is False

    fields = _last_stage_fields(runner, "controlled_outp_vent0_fixed_wait_before_seal")
    assert fields["post_vent0_outp1_count"] == 1
    assert fields["post_vent0_setpoint_count"] == 1
    assert runner._controlled_exit_final_decision == "FAIL_CLOSED_PRESSURE_CONTROL_BEFORE_ROUTE_CLOSE"
    runner._apply_valve_states.assert_not_called()
    runner._cleanup_co2_route.assert_called_once()


def test_route_close_waits_after_raw_vent0_and_valves_remain_open() -> None:
    vent0_ts = time.time()
    runner, _, _, _ = _runner(
        gauge=FakeGauge([1013.0, 1013.0]),
        logger=PostVent0RawTapLogger(vent1_count=0, vent0_wall_ts=vent0_ts + 0.05),
    )

    fields = runner._record_vent0_state_evidence(
        _co2_point(),
        phase="co2",
        vent0_command_ts=vent0_ts,
        vent0_raw_response=2,
        route_valves_still_open_during_wait=True,
        fixed_wait_elapsed_s=1.6,
        route_valve_close_ts=vent0_ts + 1.6,
        actual_open_valves_after_close="",
        trace_stage="route_valves_closed_after_vent0",
    )

    assert fields["route_valves_still_open_during_wait"] is True
    assert fields["post_vent0_route_valves_still_open"] is True
    assert fields["route_close_after_vent0_delay_s"] == pytest.approx(1.6)
    assert fields["route_close_after_vent0_raw_tx_s"] == pytest.approx(1.55, abs=0.01)
    assert fields["route_close_after_raw_vent0_s"] == pytest.approx(1.55, abs=0.01)


def test_route_close_deadline_uses_raw_vent0_timestamp() -> None:
    vent0_ts = time.time()
    raw_vent0_ts = vent0_ts + 0.25
    route_close_ts = raw_vent0_ts + 1.5
    runner, _, _, _ = _runner(
        gauge=FakeGauge([1013.0, 1013.0]),
        logger=PostVent0RawTapLogger(vent1_count=0, vent0_wall_ts=raw_vent0_ts),
    )

    fields = runner._record_vent0_state_evidence(
        _co2_point(),
        phase="co2",
        vent0_command_ts=vent0_ts,
        vent0_raw_response=2,
        route_valves_still_open_during_wait=True,
        fixed_wait_elapsed_s=1.5,
        route_valve_close_ts=route_close_ts,
        actual_open_valves_after_close="",
        trace_stage="route_valves_closed_after_vent0",
        route_close_deadline_ts=route_close_ts,
        route_close_deadline_source="raw_vent0_tx",
        route_valves_open_during_vent0_wait=True,
        route_closed_after_fixed_wait=True,
    )

    assert fields["route_close_deadline_source"] == "raw_vent0_tx"
    assert fields["route_close_after_raw_vent0_s"] == pytest.approx(1.5, abs=0.01)
    assert fields["route_close_after_vent0_delay_s"] == pytest.approx(1.75)
    assert fields["route_valves_open_during_vent0_wait"] is True
    assert fields["route_closed_after_fixed_wait"] is True


def test_post_vent0_probe_does_not_delay_route_close() -> None:
    vent0_ts = time.time()
    runner, _, _, _ = _runner(
        gauge=FakeGauge([1013.0, 1013.0]),
        logger=PostVent0RawTapLogger(vent1_count=0, vent0_wall_ts=vent0_ts),
    )

    fields = runner._collect_post_vent0_probe_fields(
        phase="co2",
        vent0_intent_ts=vent0_ts,
        route_valves_still_open=True,
        max_relative_s=1.5,
        deadline_wall_s=vent0_ts + 1.5,
        probe_phase="pre_route_close",
        allow_device_queries=False,
    )

    assert fields["pre_route_close_probe_count"] == 0
    assert fields["post_route_close_probe_count"] == 0
    assert fields["post_vent0_probe_skipped_due_to_deadline_count"] >= 1
    assert fields["post_vent0_new_vent1_count"] == 0


def test_preseal_pressure_build_waits_for_1110_before_route_close(monkeypatch) -> None:
    runner, _, _, _ = _runner(gauge=FakeGauge([1010.0, 1040.0, 1112.0, 1112.0]))
    events: list[str] = []
    runner._apply_valve_states = MagicMock(side_effect=lambda _valves: events.append("close_valves"))
    monkeypatch.setattr("time.sleep", lambda seconds: events.append(f"sleep:{seconds:.3f}"))

    assert runner._pressurize_route_for_sealed_points(
        _co2_point(),
        route="co2",
        sealed_control_refs=[_co2_point()],
    ) is True

    close_index = events.index("close_valves")
    assert any(event.startswith("sleep:") for event in events[:close_index])
    fields = _last_stage_fields(runner, "route_valves_closed_after_vent0")
    assert fields["preseal_route_close_trigger_source"] == "pressure_gauge_threshold"
    assert fields["route_close_pressure_at_close_hpa"] == pytest.approx(1112.0)
    assert fields["route_close_timeout_without_pressure_trigger"] is False


def test_preseal_pressure_build_safety_caps_configured_wait_and_hard_limit() -> None:
    runner, _, _, _ = _runner(
        pressure_overrides={
            "co2_preseal_pressure_build_max_wait_s": 5.0,
            "preseal_pressure_build_hard_limit_hpa": 1600.0,
        }
    )

    assert runner._preseal_route_close_max_wait_s() == pytest.approx(1.5)
    assert runner._preseal_pressure_build_hard_limit_hpa() == pytest.approx(1200.0)


def test_preseal_pressure_build_timeout_uses_1p5s_not_5s(monkeypatch) -> None:
    runner, _, _, _ = _runner(gauge=FakeGauge([1013.0, 1013.0]))
    events: list[str] = []
    runner._apply_valve_states = MagicMock(side_effect=lambda _valves: events.append("close_valves"))
    monkeypatch.setattr("time.sleep", lambda seconds: events.append(f"sleep:{seconds:.3f}"))

    assert runner._pressurize_route_for_sealed_points(
        _co2_point(),
        route="co2",
        sealed_control_refs=[_co2_point()],
    ) is True

    close_index = events.index("close_valves")
    slept_s = sum(float(event.split(":", 1)[1]) for event in events[:close_index] if event.startswith("sleep:"))
    assert slept_s == pytest.approx(1.5, abs=0.2)
    fields = _last_stage_fields(runner, "route_valves_closed_after_vent0")
    assert fields["preseal_route_close_trigger_source"] == "max_wait"
    assert fields["route_close_deadline_enforced"] is True
    assert fields["preseal_pressure_build_max_wait_s"] == pytest.approx(1.5)
    assert fields["route_close_timeout_without_pressure_trigger"] is True


def test_route_close_not_delayed_by_preseal_exit_evidence(monkeypatch) -> None:
    runner, _, _, _ = _runner(gauge=FakeGauge([1112.0, 1112.0]))
    events: list[str] = []
    runner._apply_valve_states = MagicMock(side_effect=lambda _valves: events.append("close_valves"))
    original_snapshot = runner._controlled_exit_atmosphere_snapshot

    def slow_snapshot(pace):
        events.append("slow_evidence")
        return original_snapshot(pace)

    runner._controlled_exit_atmosphere_snapshot = slow_snapshot
    monkeypatch.setattr("time.sleep", lambda seconds: events.append(f"sleep:{seconds:.3f}"))

    assert runner._pressurize_route_for_sealed_points(
        _co2_point(),
        route="co2",
        sealed_control_refs=[_co2_point()],
    ) is True

    assert "close_valves" in events
    assert "slow_evidence" not in events


def test_full_status_evidence_runs_only_after_route_close(monkeypatch) -> None:
    runner, _, _, _ = _runner(gauge=FakeGauge([1112.0, 1112.0]))
    runner._apply_valve_states = MagicMock()
    monkeypatch.setattr("time.sleep", lambda _seconds: None)

    assert runner._pressurize_route_for_sealed_points(
        _co2_point(),
        route="co2",
        sealed_control_refs=[_co2_point()],
    ) is True

    stages = _trace_stages(runner)
    assert "pace_status_evidence:after_vent0" not in stages
    assert "pace_status_evidence:after_route_close" not in stages
    assert stages.index("route_valves_closed_after_vent0") < stages.index("post_seal_vent_abort_clear")


def test_route_close_deadline_miss_is_recorded() -> None:
    vent0_ts = time.time()
    raw_vent0_ts = vent0_ts + 0.1
    deadline_ts = raw_vent0_ts + 1.5
    close_ts = deadline_ts + 0.25
    runner, _, _, _ = _runner(
        gauge=FakeGauge([1013.0, 1013.0]),
        logger=PostVent0RawTapLogger(vent1_count=0, vent0_wall_ts=raw_vent0_ts),
    )

    fields = runner._record_vent0_state_evidence(
        _co2_point(),
        phase="co2",
        vent0_command_ts=vent0_ts,
        vent0_raw_response=2,
        route_valves_still_open_during_wait=True,
        fixed_wait_elapsed_s=close_ts - raw_vent0_ts,
        route_valve_close_ts=close_ts,
        actual_open_valves_after_close="",
        trace_stage="route_valves_closed_after_vent0",
        route_close_deadline_ts=deadline_ts,
        route_close_deadline_source="raw_vent0_tx",
    )

    assert fields["route_close_deadline_missed"] is True
    assert fields["route_close_deadline_miss_s"] == pytest.approx(0.25, abs=0.01)


def test_preseal_pressure_hard_limit_emergency_closes_and_blocks_outp1(monkeypatch) -> None:
    runner, pace, _, _ = _runner(gauge=FakeGauge([1700.0, 1700.0]))
    runner._apply_valve_states = MagicMock()
    monkeypatch.setattr("time.sleep", lambda _seconds: None)

    assert runner._pressurize_route_for_sealed_points(
        _co2_point(),
        route="co2",
        sealed_control_refs=[_co2_point()],
    ) is False

    runner._apply_valve_states.assert_any_call([])
    assert ("enable_control_output",) not in pace.calls
    assert runner._controlled_exit_final_decision == "FAIL_CLOSED_PRESEAL_PRESSURE_BUILD_HARD_LIMIT"
    fields = _last_stage_fields(runner, "route_valves_closed_after_vent0")
    assert fields["preseal_pressure_build_hard_limit_hit"] is True
    assert fields["preseal_pressure_build_hard_limit_hpa"] == pytest.approx(1200.0)


def test_vent_status_3_after_vent0_does_not_block_route_close(monkeypatch) -> None:
    runner, pace, _, _ = _runner(
        pace=Vent3PersistPace(),
        gauge=FakeGauge([1112.0, 1112.0]),
        logger=PostVent0RawTapLogger(vent1_count=0),
    )
    runner._apply_valve_states = MagicMock()
    monkeypatch.setattr("time.sleep", lambda _seconds: None)

    assert runner._pressurize_route_for_sealed_points(
        _co2_point(),
        route="co2",
        sealed_control_refs=[_co2_point()],
    ) is False

    assert ("vent", False) in pace.calls
    assert ("exit_atmosphere_mode", 3.0, 0.2) not in pace.calls
    runner._apply_valve_states.assert_called_once_with([])
    fixed_fields = _last_stage_fields(runner, "controlled_outp_vent0_fixed_wait_before_seal")
    assert fixed_fields["post_vent0_new_vent1_count"] == 0
    assert fixed_fields["post_vent0_vent3_watchlist_only"] is True
    assert fixed_fields["post_vent0_route_valves_still_open"] is True
    clear_fields = _last_stage_fields(runner, "post_seal_vent_abort_clear")
    assert clear_fields["sealed_control_ready_blocks_outp1"] is True
    assert ("enable_control_output",) not in pace.calls


def test_post_seal_vent3_blocks_without_abort_clear(monkeypatch) -> None:
    runner, pace, _, _ = _runner(
        pace=Vent3ThenClearPace(),
        gauge=FakeGauge([1112.0, 1112.0]),
        logger=PostVent0RawTapLogger(vent1_count=0),
    )
    runner._apply_valve_states = MagicMock()
    monkeypatch.setattr("time.sleep", lambda _seconds: None)

    assert runner._pressurize_route_for_sealed_points(
        _co2_point(),
        route="co2",
        sealed_control_refs=[_co2_point()],
    ) is False

    fields = _last_stage_fields(runner, "post_seal_vent_abort_clear")
    assert fields["post_seal_vent_abort_clear_sent"] is False
    assert fields["post_seal_vent_status_before_clear"] == 3
    assert fields["post_seal_vent_status_after_clear"] == 3
    assert fields["post_seal_vent_window_cleared"] is False
    assert fields["sealed_control_ready_decision"] == (
        "blocked:FAIL_CLOSED_PRESSURE_CONTROLLER_VENT_WINDOW_LATCHED_BEFORE_CONTROL"
    )
    assert fields["sealed_control_ready_blocks_outp1"] is True
    assert ("setpoint", 1100.0) not in pace.calls
    assert ("enable_control_output",) not in pace.calls
    assert pace.calls.count(("vent", False)) == 1
    assert ("vent", True) not in pace.calls


def test_post_seal_vent_abort_clear_not_sent_if_vent_status_normal(monkeypatch) -> None:
    runner, _, _, _ = _runner(gauge=FakeGauge([1112.0, 1112.0]))
    runner._apply_valve_states = MagicMock()
    monkeypatch.setattr("time.sleep", lambda _seconds: None)

    assert runner._pressurize_route_for_sealed_points(
        _co2_point(),
        route="co2",
        sealed_control_refs=[_co2_point()],
    ) is True

    fields = _last_stage_fields(runner, "post_seal_vent_abort_clear")
    assert fields["post_seal_vent_abort_clear_sent"] is False
    assert fields["post_seal_vent_status_before_clear"] == 2


def test_control_ready_blocks_outp1_when_vent3_persists_after_clear(monkeypatch) -> None:
    runner, pace, _, _ = _runner(
        pace=Vent3PersistPace(),
        gauge=FakeGauge([1112.0, 1112.0]),
        logger=PostVent0RawTapLogger(vent1_count=0),
    )
    runner._apply_valve_states = MagicMock()
    monkeypatch.setattr("time.sleep", lambda _seconds: None)

    assert runner._pressurize_route_for_sealed_points(
        _co2_point(),
        route="co2",
        sealed_control_refs=[_co2_point()],
    ) is False

    fields = _last_stage_fields(runner, "post_seal_vent_abort_clear")
    assert fields["post_seal_vent_abort_clear_sent"] is False
    assert fields["post_seal_vent_window_persisted"] is True
    assert fields["sealed_control_ready_vent_clear_attempt_count"] == 0
    assert fields["sealed_control_ready_blocks_outp1"] is True
    assert fields["outp1_blocked_reason"] == "FAIL_CLOSED_PRESSURE_CONTROLLER_VENT_WINDOW_LATCHED_BEFORE_CONTROL"

    assert ("enable_control_output",) not in pace.calls
    assert ("setpoint", 1100.0) not in pace.calls
    assert runner._controlled_exit_final_decision == (
        "FAIL_CLOSED_PRESSURE_CONTROLLER_VENT_WINDOW_LATCHED_BEFORE_CONTROL"
    )
    assert ("vent", True) not in pace.calls


def test_vent3_after_clear_still_blocks_outp1(monkeypatch) -> None:
    runner, pace, _, _ = _runner(
        pace=Vent3PersistPace(),
        gauge=FakeGauge([1112.0, 1112.0]),
        logger=PostVent0RawTapLogger(vent1_count=0),
    )
    runner._apply_valve_states = MagicMock()
    monkeypatch.setattr("time.sleep", lambda _seconds: None)

    assert runner._pressurize_route_for_sealed_points(
        _co2_point(),
        route="co2",
        sealed_control_refs=[_co2_point()],
    ) is False

    fields = _last_stage_fields(runner, "post_seal_vent_abort_clear")
    assert fields["outp1_blocked_reason"] == "FAIL_CLOSED_PRESSURE_CONTROLLER_VENT_WINDOW_LATCHED_BEFORE_CONTROL"
    assert ("enable_control_output",) not in pace.calls
    assert ("setpoint", 1100.0) not in pace.calls
    assert ("vent", True) not in pace.calls


def test_control_ready_does_not_clear_vent3_to_allow_outp1(monkeypatch) -> None:
    runner, pace, _, _ = _runner(
        pace=Vent3ThenClearPace(),
        gauge=FakeGauge([1112.0, 1112.0]),
        logger=PostVent0RawTapLogger(vent1_count=0),
    )
    runner._apply_valve_states = MagicMock()
    monkeypatch.setattr("time.sleep", lambda _seconds: None)

    assert runner._pressurize_route_for_sealed_points(
        _co2_point(),
        route="co2",
        sealed_control_refs=[_co2_point()],
    ) is False

    fields = _last_stage_fields(runner, "post_seal_vent_abort_clear")
    assert fields["post_seal_vent_abort_clear_sent"] is False
    assert fields["post_seal_vent_status_after_clear"] == 3
    assert fields["sealed_control_ready_blocks_outp1"] is True
    assert ("enable_control_output",) not in pace.calls
    assert ("setpoint", 1100.0) not in pace.calls
    assert pace.calls.count(("vent", False)) == 1
    assert ("vent", True) not in pace.calls


def test_post_seal_clear_never_sends_vent1(monkeypatch) -> None:
    runner, pace, _, _ = _runner(
        pace=Vent3PersistPace(),
        gauge=FakeGauge([1112.0, 1112.0]),
        logger=PostVent0RawTapLogger(vent1_count=0),
    )
    runner._apply_valve_states = MagicMock()
    monkeypatch.setattr("time.sleep", lambda _seconds: None)

    assert runner._pressurize_route_for_sealed_points(
        _co2_point(),
        route="co2",
        sealed_control_refs=[_co2_point()],
    ) is False

    assert ("vent", True) not in pace.calls


def test_no_vent1_in_sealed_stage(monkeypatch) -> None:
    runner, pace, _, _ = _runner(
        pace=Vent3PersistPace(),
        gauge=FakeGauge([1112.0, 1112.0]),
        logger=PostVent0RawTapLogger(vent1_count=0),
    )
    runner._apply_valve_states = MagicMock()
    monkeypatch.setattr("time.sleep", lambda _seconds: None)

    assert runner._pressurize_route_for_sealed_points(
        _co2_point(),
        route="co2",
        sealed_control_refs=[_co2_point()],
    ) is False

    route_close_index = next(i for i, call in enumerate(pace.calls) if call == ("vent", False))
    assert ("vent", True) not in pace.calls[route_close_index + 1 :]


def test_sampling_not_started_when_control_ready_blocks(monkeypatch) -> None:
    runner, _, _, _ = _runner(
        pace=Vent3PersistPace(),
        gauge=FakeGauge([1112.0, 1112.0]),
        logger=PostVent0RawTapLogger(vent1_count=0),
    )
    runner._apply_valve_states = MagicMock()
    runner._sample_and_log = MagicMock()
    monkeypatch.setattr("time.sleep", lambda _seconds: None)

    assert runner._pressurize_route_for_sealed_points(
        _co2_point(),
        route="co2",
        sealed_control_refs=[_co2_point()],
    ) is False

    runner._sample_and_log.assert_not_called()


def test_vent_status_3_is_not_atmosphere_exit_failure(monkeypatch) -> None:
    runner, _, _, _ = _runner(
        pace=LegacyTrappedVentAfterOffPace(),
        gauge=FakeGauge([1013.0, 1013.0, 1013.0]),
        logger=PostVent0RawTapLogger(vent1_count=0),
    )
    runner._apply_valve_states = MagicMock()
    monkeypatch.setattr("time.sleep", lambda _seconds: None)

    assert runner._pressurize_route_for_sealed_points(
        _co2_point(),
        route="co2",
        sealed_control_refs=[_co2_point()],
    ) is True

    stages = _trace_stages(runner)
    assert "controlled_exit_atmosphere_fail" not in stages
    assert "post_seal_vent_abort_clear" in stages
    assert runner._controlled_exit_final_decision == "ENGINEERING_EXIT_ATMOSPHERE_PASS"


def test_vent_status_3_is_watchlist_not_terminal() -> None:
    runner, _, _, _ = _runner()

    fields = runner._pace_vent_status_diagnostic_fields(3, stage="preseal")

    assert fields["vent_status_watchlist"] is True
    assert fields["vent_status_classification"] == "pressure_build_or_window_latched"
    assert fields["vent_status_gate_effect"] == "none"
    assert fields["vent_status_terminal"] is False


def test_operator_window_console_yes_allows_pressure_build_timeout_close(monkeypatch) -> None:
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
    close_index = events.index("close_valves")
    slept_s = sum(float(event.split(":", 1)[1]) for event in events[:close_index] if event.startswith("sleep:"))
    assert slept_s == pytest.approx(1.5, abs=0.2)
    assert all(event.startswith("sleep:") for event in events[:close_index])


def test_operator_window_console_no_blocks_seal(monkeypatch) -> None:
    runner, _, _, _ = _runner(
        gauge=FakeGauge([1013.0, 1013.0]),
        pressure_overrides={"operator_window_confirm_mode": "console"},
    )
    runner._apply_valve_states = MagicMock()
    runner._cleanup_co2_route = MagicMock()
    monkeypatch.setattr("time.sleep", lambda _seconds: None)
    monkeypatch.setattr("sys.stdin", FakeStdin("NO\n", interactive=True))

    assert runner._pressurize_route_for_sealed_points(_co2_point(), route="co2", sealed_control_refs=[_co2_point()]) is True

    runner._apply_valve_states.assert_called_once_with([])
    runner._cleanup_co2_route.assert_not_called()
    assert runner._controlled_exit_final_decision == "ENGINEERING_EXIT_ATMOSPHERE_PASS"
    assert "operator_window_check_result" not in _trace_stages(runner)


def test_operator_window_noninteractive_warns_not_blocks_route_close(monkeypatch) -> None:
    runner, _, _, _ = _runner(
        gauge=FakeGauge([1013.0, 1013.0]),
        pressure_overrides={"operator_window_confirm_mode": "console"},
    )
    runner._apply_valve_states = MagicMock()
    runner._cleanup_co2_route = MagicMock()
    monkeypatch.setattr("time.sleep", lambda _seconds: None)
    monkeypatch.setattr("sys.stdin", FakeStdin("", interactive=False))

    assert runner._pressurize_route_for_sealed_points(_co2_point(), route="co2", sealed_control_refs=[_co2_point()]) is True

    runner._apply_valve_states.assert_called_once_with([])
    runner._cleanup_co2_route.assert_not_called()
    assert runner._controlled_exit_final_decision == "ENGINEERING_EXIT_ATMOSPHERE_PASS"
    stages = _trace_stages(runner)
    assert "post_seal_vent_abort_clear" in stages
    assert "operator_window_check_result" not in stages
    assert "route_sealed" in stages


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

    assert runner._pressurize_route_for_sealed_points(_co2_point(), route="co2", sealed_control_refs=[_co2_point()]) is True

    runner._apply_valve_states.assert_called_once_with([])
    runner._cleanup_co2_route.assert_not_called()
    assert runner._controlled_exit_final_decision == "ENGINEERING_EXIT_ATMOSPHERE_PASS"


def test_operator_window_prompt_after_fixed_wait_after_close(monkeypatch) -> None:
    runner, _, _, _ = _runner(
        gauge=FakeGauge([1013.0, 1013.0]),
        pressure_overrides={"operator_window_confirm_mode": "console"},
    )
    runner._apply_valve_states = MagicMock()
    monkeypatch.setattr("time.sleep", lambda _seconds: None)
    monkeypatch.setattr("sys.stdin", FakeStdin("YES\n", interactive=True))

    assert runner._pressurize_route_for_sealed_points(_co2_point(), route="co2", sealed_control_refs=[_co2_point()])

    stages = _trace_stages(runner)
    assert "operator_window_prompt_printed" not in stages
    assert "operator_window_check_result" not in stages
    assert stages.index("route_valves_closed_after_vent0") < stages.index("post_seal_vent_abort_clear")
    assert stages.index("post_seal_vent_abort_clear") < stages.index("route_sealed")


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
    assert result_notes == []


def test_window_not_cleared_warns_after_close_valves(monkeypatch) -> None:
    runner, _, _, _ = _runner(
        gauge=FakeGauge([1013.0, 1013.0]),
        pressure_overrides={"operator_window_confirm_mode": "console"},
    )
    runner._apply_valve_states = MagicMock()
    runner._cleanup_co2_route = MagicMock()
    monkeypatch.setattr("time.sleep", lambda _seconds: None)
    monkeypatch.setattr("sys.stdin", FakeStdin("NOT_CLEARED\n", interactive=True))

    assert runner._pressurize_route_for_sealed_points(_co2_point(), route="co2", sealed_control_refs=[_co2_point()]) is True

    runner._apply_valve_states.assert_called_once_with([])
    runner._cleanup_co2_route.assert_not_called()


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


def test_vent0_pressure_build_timeout_then_close_valves(monkeypatch) -> None:
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

    assert ("vent", False) in pace.calls
    assert ("exit_atmosphere_mode", 3.0, 0.2) not in pace.calls
    assert sum(event[1] for event in events if event[0] == "sleep") == pytest.approx(1.5, abs=0.2)
    assert events.index(("close_valves", [])) > next(i for i, event in enumerate(events) if event[0] == "sleep")
    assert events.index(("close_valves", [])) < events.index(("guard", None))


def test_vent0_state_trace_recorded_at_route_close(monkeypatch) -> None:
    runner, _, _, _ = _runner(gauge=FakeGauge([1013.0, 1013.0, 1013.0]))
    point = _co2_point()
    runner._apply_valve_states = MagicMock()
    monkeypatch.setattr("time.sleep", lambda _seconds: None)

    assert runner._pressurize_route_for_sealed_points(point, route="co2", sealed_control_refs=[point]) is True

    close_calls = [
        call.kwargs
        for call in runner._append_pressure_trace_row.call_args_list
        if call.kwargs.get("trace_stage") == "route_valves_closed_after_vent0"
    ]
    assert close_calls
    fields = close_calls[-1]["extra_fields"]
    assert fields["vent0_command_ts"]
    ready_fields = _last_stage_fields(runner, "post_seal_vent_abort_clear")
    assert ready_fields["sealed_control_ready_vent_status"] == 2
    assert ready_fields["sealed_control_ready_decision"] == "ready"
    assert fields["outp_status_after_vent0"] == 0
    assert fields["isol_status_after_vent0"] == 1
    assert fields["route_valves_still_open_during_wait"] is True
    assert fields["fixed_wait_elapsed_s"] is not None
    assert fields["route_valve_close_ts"]
    assert "actual_open_valves_after_close" in fields


def test_fast_control_order_preserved(monkeypatch) -> None:
    runner, _, _, _ = _runner(gauge=FakeGauge([1013.0, 1013.0, 1013.0]))
    point = _co2_point()
    runner._apply_valve_states = MagicMock()
    monkeypatch.setattr("time.sleep", lambda _seconds: None)

    assert runner._pressurize_route_for_sealed_points(point, route="co2", sealed_control_refs=[point]) is True

    stages = _trace_stages(runner)
    assert stages.index("controlled_outp_vent0_fixed_wait_before_seal") < stages.index(
        "route_valves_closed_after_vent0"
    )
    assert stages.index("route_valves_closed_after_vent0") < stages.index("post_seal_vent_abort_clear")
    assert stages.index("post_seal_vent_abort_clear") < stages.index("route_sealed")
    assert "operator_window_check_result" not in stages


def test_verified_exit_then_pressure_build_timeout_close_valves(monkeypatch) -> None:
    runner, _, _, _ = _runner(gauge=FakeGauge([1013.0, 1013.0]))
    events: list[str] = []
    runner._apply_valve_states = MagicMock(side_effect=lambda _valves: events.append("close_valves"))
    monkeypatch.setattr("time.sleep", lambda seconds: events.append(f"sleep:{seconds:.1f}"))

    assert runner._pressurize_route_for_sealed_points(_co2_point(), route="co2", sealed_control_refs=[_co2_point()])

    stages = _trace_stages(runner)
    assert stages.index("controlled_outp_vent0_fixed_wait_before_seal") < stages.index(
        "route_valves_closed_after_vent0"
    )
    assert stages.index("route_valves_closed_after_vent0") < stages.index("post_seal_vent_abort_clear")
    assert stages.index("post_seal_vent_abort_clear") < stages.index("route_sealed")
    assert "operator_window_check_begin" not in stages
    close_index = events.index("close_valves")
    slept_s = sum(float(event.split(":", 1)[1]) for event in events[:close_index] if event.startswith("sleep:"))
    assert slept_s == pytest.approx(1.5, abs=0.2)
    assert all(event.startswith("sleep:") for event in events[:close_index])


def test_pressure_rise_gate_does_not_block_seal_in_controlled_mode(monkeypatch) -> None:
    runner, _, _, _ = _runner(gauge=FakeGauge([1013.0, 1013.0, 1013.0]))
    runner._cleanup_co2_route = MagicMock()
    runner._apply_valve_states = MagicMock()
    monkeypatch.setattr("time.sleep", lambda _seconds: None)

    assert runner._pressurize_route_for_sealed_points(_co2_point(), route="co2", sealed_control_refs=[_co2_point()]) is True

    runner._cleanup_co2_route.assert_not_called()
    assert "controlled_outp_pressure_rise_diagnostic" not in _trace_stages(runner)


def test_pressure_rise_still_diagnostic_only(monkeypatch) -> None:
    runner, _, _, _ = _runner(gauge=FakeGauge([1013.0, 1013.0]))
    runner._cleanup_co2_route = MagicMock()
    runner._apply_valve_states = MagicMock()
    monkeypatch.setattr("time.sleep", lambda _seconds: None)

    assert runner._pressurize_route_for_sealed_points(_co2_point(), route="co2", sealed_control_refs=[_co2_point()])

    runner._cleanup_co2_route.assert_not_called()
    assert runner._apply_valve_states.called
    assert "controlled_outp_pressure_rise_diagnostic" not in _trace_stages(runner)


def test_pressure_rise_and_noninteractive_window_warn_not_block_route_close(monkeypatch) -> None:
    runner, _, _, _ = _runner(
        gauge=FakeGauge([1013.0, 1045.0]),
        pressure_overrides={"operator_window_confirm_mode": "console"},
    )
    runner._apply_valve_states = MagicMock()
    runner._cleanup_co2_route = MagicMock()
    monkeypatch.setattr("time.sleep", lambda _seconds: None)
    monkeypatch.setattr("sys.stdin", FakeStdin("", interactive=False))

    assert runner._pressurize_route_for_sealed_points(_co2_point(), route="co2", sealed_control_refs=[_co2_point()]) is True

    runner._apply_valve_states.assert_called_once_with([])
    runner._cleanup_co2_route.assert_not_called()
    assert runner._controlled_exit_final_decision == "ENGINEERING_EXIT_ATMOSPHERE_PASS"
    stages = _trace_stages(runner)
    assert "controlled_outp_pressure_rise_diagnostic" not in stages
    assert "operator_window_check_result" not in stages


def test_route_valves_remain_open_during_fixed_wait(monkeypatch) -> None:
    runner, _, _, _ = _runner(gauge=FakeGauge([1013.0, 1013.0]))
    events: list[str] = []
    runner._apply_valve_states = MagicMock(side_effect=lambda _valves: events.append("close_valves"))
    monkeypatch.setattr("time.sleep", lambda seconds: events.append(f"sleep:{seconds:.1f}"))

    runner._pressurize_route_for_sealed_points(_co2_point(), route="co2", sealed_control_refs=[_co2_point()])

    close_index = events.index("close_valves")
    assert close_index > 0
    assert all(event.startswith("sleep:") for event in events[:close_index])


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
    assert pace.calls.index(("setpoint", 900.0)) < pace.calls.index(("enable_control_output",))
    pass_calls = [
        call.kwargs
        for call in runner._append_pressure_trace_row.call_args_list
        if call.kwargs.get("trace_stage") == "sealed_control_output_enable_pass"
    ]
    assert pass_calls
    assert pass_calls[-1]["extra_fields"]["sealed_outp1_count"] == 1
    assert pass_calls[-1]["extra_fields"]["sealed_setpoint_count"] == 1


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
    pace.vent_status = 0
    assert runner._set_pressure_to_target(point) is True

    assert pace.calls.index(("vent", False)) < pace.calls.index(("setpoint", 900.0))
    assert pace.calls.index(("setpoint", 900.0)) < pace.calls.index(("enable_control_output",))


def test_route_close_to_outp1_has_short_deadline() -> None:
    runner, pace, _, _ = _runner(
        pressure_overrides={
            "route_close_to_setpoint_max_s": 10.0,
            "route_close_to_outp1_max_s": 0.001,
        }
    )
    point = _co2_point(pressure=900.0)
    runner._activate_co2_sealed_no_vent_guard(point, reason="test", route_close_ts=time.time() - 1.0)
    runner._record_preseal_pressure_control_ready_state(point, phase="co2", defer_live_check=False)

    assert runner._set_pressure_to_target(point) is False

    assert ("setpoint", 900.0) in pace.calls
    assert ("enable_control_output",) not in pace.calls
    fields = _last_stage_fields(runner, "sealed_passive_deadline_exceeded")
    assert fields["sealed_passive_exceeded"] is True
    assert fields["sealed_passive_blocking_stage"] == "before_outp1"


def test_sealed_passive_window_fails_if_exceeds_max() -> None:
    runner, pace, _, _ = _runner(
        pressure_overrides={
            "route_close_to_setpoint_max_s": 0.001,
            "sealed_passive_max_s": 0.001,
        }
    )
    point = _co2_point(pressure=900.0)
    runner._activate_co2_sealed_no_vent_guard(point, reason="test", route_close_ts=time.time() - 1.0)
    runner._record_preseal_pressure_control_ready_state(point, phase="co2", defer_live_check=False)

    assert runner._set_pressure_to_target(point) is False

    assert ("setpoint", 900.0) not in pace.calls
    assert ("enable_control_output",) not in pace.calls
    fields = _last_stage_fields(runner, "sealed_passive_deadline_exceeded")
    assert fields["sealed_passive_exceeded"] is True
    assert fields["sealed_passive_blocking_stage"] == "before_setpoint"


def test_vent2_completed_allowed_only_for_immediate_control() -> None:
    runner, pace, _, _ = _runner()
    point = _co2_point(pressure=900.0)
    pace.vent_status = 2
    runner._activate_co2_sealed_no_vent_guard(point, reason="test", route_close_ts=time.time())
    runner._record_preseal_pressure_control_ready_state(point, phase="co2", defer_live_check=False)

    assert runner._set_pressure_to_target(point) is True

    assert pace.calls.index(("setpoint", 900.0)) < pace.calls.index(("enable_control_output",))
    fields = _last_stage_fields(runner, "sealed_control_output_enable_pass")
    assert json.loads(fields["sealed_passive_state"])["VENT?"] == 2
    assert fields["sealed_passive_exceeded"] is False
    assert fields["vent2_transient_only"] is True


def test_outp1_after_vent3_fails_closed_before_pressure_ready() -> None:
    runner, pace, _, _ = _runner(pace=Vent3AfterOutp1Pace())
    point = _co2_point(pressure=900.0)
    runner._activate_co2_sealed_no_vent_guard(point, reason="test", route_close_ts=time.time())
    runner._record_preseal_pressure_control_ready_state(point, phase="co2", defer_live_check=False)

    assert runner._set_pressure_to_target(point) is False

    fields = _last_stage_fields(runner, "sealed_control_output_enable_fail")
    assert fields["vent_status_after_outp1"] == 3
    assert fields["pressure_controller_control_state_verified"] is False
    assert fields["pressure_controller_control_state_failure_reason"] == (
        "FAIL_CLOSED_PRESSURE_CONTROLLER_VENT_WINDOW_LATCHED_AFTER_OUTP1"
    )


def test_sampling_ready_treats_vent2_as_watchlist_with_control_evidence() -> None:
    runner, pace, _, _ = _runner()
    point = _co2_point(pressure=900.0)
    pace.output_state = 1
    pace.vent_status = 2
    pace.setpoint = 900.0
    runner._activate_co2_sealed_no_vent_guard(point, reason="test", route_close_ts=time.time())
    runner._sealed_first_setpoint_tx_ts = time.time()
    runner._sealed_first_outp1_tx_ts = time.time()
    runner._mark_sealed_pressure_ready(result="in_limits")

    assert runner._co2_sealed_sampling_ready(point, point_tag="unit") is True

    fields = _last_stage_fields(runner, "sealed_sampling_ready")
    assert fields["vent_status_before_sampling"] == 2
    assert fields["vent2_watchlist_before_sampling"] is True
    assert fields["sampling_blocked_by_vent_watchlist"] is False
    assert fields["pressure_controller_control_state_verified"] is True


def test_vent2_watchlist_not_sampling_ready_by_itself() -> None:
    runner, pace, _, _ = _runner()
    point = _co2_point(pressure=900.0)
    pace.output_state = 1
    pace.vent_status = 2
    pace.setpoint = 900.0
    runner._activate_co2_sealed_no_vent_guard(point, reason="test", route_close_ts=time.time())
    runner._sealed_first_setpoint_tx_ts = time.time()
    runner._sealed_first_outp1_tx_ts = time.time()

    assert runner._co2_sealed_sampling_ready(point, point_tag="vent2-only") is False

    fields = _last_stage_fields(runner, "sealed_sampling_ready_failed")
    assert fields["vent2_watchlist_before_sampling"] is True
    assert fields["sampling_blocked_by_pressure_not_ready"] is True
    assert fields["pressure_controller_control_state_verified"] is False


def test_sampling_ready_blocks_vent1_or_vent3() -> None:
    for status in (1, 3):
        runner, pace, _, _ = _runner()
        point = _co2_point(pressure=900.0)
        pace.output_state = 1
        pace.vent_status = status
        pace.setpoint = 900.0
        runner._activate_co2_sealed_no_vent_guard(point, reason="test", route_close_ts=time.time())
        runner._sealed_first_setpoint_tx_ts = time.time()
        runner._sealed_first_outp1_tx_ts = time.time()
        runner._mark_sealed_pressure_ready(result="in_limits")

        assert runner._co2_sealed_sampling_ready(point, point_tag=f"vent{status}") is False

        fields = _last_stage_fields(runner, "sealed_sampling_ready_failed")
        assert fields["vent_status_before_sampling"] == status
        assert fields["sampling_blocked_by_vent_watchlist"] is True


def test_sampling_ready_blocks_dewpoint_rise_after_outp1() -> None:
    runner, pace, _, _ = _runner()
    point = _co2_point(pressure=900.0)
    pace.output_state = 1
    pace.vent_status = 2
    pace.setpoint = 900.0
    runner._preseal_dewpoint_snapshot = {"dewpoint_c": -34.0}
    runner._cached_ready_check_trace_values = MagicMock(return_value={"dewpoint_c": -10.0})
    runner._activate_co2_sealed_no_vent_guard(point, reason="test", route_close_ts=time.time())
    runner._sealed_first_setpoint_tx_ts = time.time()
    runner._sealed_first_outp1_tx_ts = time.time()
    runner._mark_sealed_pressure_ready(result="in_limits")

    assert runner._co2_sealed_sampling_ready(point, point_tag="dew") is False

    fields = _last_stage_fields(runner, "sealed_sampling_ready_failed")
    assert fields["sampling_blocked_by_dewpoint_rise"] is True
    assert fields["dewpoint_rise_before_sampling_c"] == pytest.approx(24.0)


def test_sampling_ready_blocks_pressure_not_ready() -> None:
    runner, pace, _, _ = _runner()
    point = _co2_point(pressure=900.0)
    pace.output_state = 1
    pace.vent_status = 2
    pace.setpoint = 900.0
    runner._activate_co2_sealed_no_vent_guard(point, reason="test", route_close_ts=time.time())
    runner._sealed_first_setpoint_tx_ts = time.time()
    runner._sealed_first_outp1_tx_ts = time.time()

    assert runner._co2_sealed_sampling_ready(point, point_tag="pressure") is False

    fields = _last_stage_fields(runner, "sealed_sampling_ready_failed")
    assert fields["sampling_blocked_by_pressure_not_ready"] is True
    assert fields["pressure_controller_control_state_verified"] is False


def test_sampling_ready_blocks_sealed_vent0_or_vent1_counts() -> None:
    for counter in ("sealed_vent0_count", "sealed_vent1_count"):
        runner, pace, _, _ = _runner()
        point = _co2_point(pressure=900.0)
        pace.output_state = 1
        pace.vent_status = 2
        pace.setpoint = 900.0
        runner._activate_co2_sealed_no_vent_guard(point, reason="test", route_close_ts=time.time())
        assert runner._co2_sealed_no_vent_guard_context is not None
        runner._co2_sealed_no_vent_guard_context[counter] = 1
        runner._sealed_first_setpoint_tx_ts = time.time()
        runner._sealed_first_outp1_tx_ts = time.time()
        runner._mark_sealed_pressure_ready(result="in_limits")

        assert runner._co2_sealed_sampling_ready(point, point_tag=counter) is False

        fields = _last_stage_fields(runner, "sealed_sampling_ready_failed")
        assert fields["sampling_blocked_by_vent_watchlist"] is True


def test_v2_like_sequence_route_close_setpoint_outp1(monkeypatch) -> None:
    runner, pace, _, _ = _runner(gauge=FakeGauge([1112.0, 1112.0]))
    point = _co2_point(pressure=900.0)
    runner._apply_valve_states = MagicMock()
    monkeypatch.setattr("time.sleep", lambda _seconds: None)

    assert runner._pressurize_route_for_sealed_points(point, route="co2", sealed_control_refs=[point]) is True
    assert runner._set_pressure_to_target(point) is True

    stages = _trace_stages(runner)
    assert stages.index("route_valves_closed_after_vent0") < stages.index("sealed_fast_control_branch_entered")
    assert stages.index("sealed_fast_control_branch_entered") < stages.index("sealed_control_setpoint_command_sent")
    assert stages.index("sealed_control_setpoint_command_sent") < stages.index(
        "sealed_control_output_enable_command_sent"
    )
    assert stages.index("sealed_control_output_enable_command_sent") < stages.index("route_sealed")
    assert pace.calls.index(("setpoint", 900.0)) < pace.calls.index(("enable_control_output",))


def test_slew_config_prearmed_before_route_close(monkeypatch) -> None:
    runner, pace, _, _ = _runner(gauge=FakeGauge([1112.0, 1112.0]))
    point = _co2_point(pressure=900.0)
    runner._apply_valve_states = MagicMock()
    monkeypatch.setattr("time.sleep", lambda _seconds: None)

    assert runner._pressurize_route_for_sealed_points(point, route="co2", sealed_control_refs=[point]) is True

    stages = _trace_stages(runner)
    assert "sealed_control_prearm_config_before_route_close" in stages
    assert stages.index("sealed_control_prearm_config_before_route_close") < stages.index(
        "route_valves_closed_after_vent0"
    )
    assert ("set_slew_mode_linear",) in pace.calls
    assert ("set_slew_rate", 15.0) in pace.calls
    assert ("set_overshoot_allowed", False) in pace.calls
    assert "sealed_pressure_slew_configured" not in stages[stages.index("route_valves_closed_after_vent0") :]
    fields = _last_stage_fields(runner, "sealed_control_setpoint_command_sent")
    assert fields["slew_config_deferred_before_route_close"] is True
    assert fields["slew_config_after_route_close_count"] == 0


def test_slew_config_failure_before_route_close_fails_before_seal(monkeypatch) -> None:
    runner, _, _, _ = _runner(pace=SlewConfigFailPace(), gauge=FakeGauge([1112.0, 1112.0]))
    point = _co2_point(pressure=900.0)
    runner._apply_valve_states = MagicMock()
    monkeypatch.setattr("time.sleep", lambda _seconds: None)

    assert runner._pressurize_route_for_sealed_points(point, route="co2", sealed_control_refs=[point]) is False

    stages = _trace_stages(runner)
    assert "sealed_control_prearm_config_before_route_close_failed" in stages
    assert "route_valves_closed_after_vent0" not in stages


def test_minimal_ready_gate_fast_after_route_close() -> None:
    runner, _, _, _ = _runner()
    point = _co2_point(pressure=900.0)
    runner._activate_co2_sealed_no_vent_guard(point, reason="test", route_close_ts=time.time())
    runner._update_pace_state_cache(
        {"pace_vent_status": 2, "pace_output_state": 0, "pace_isolation_state": 1}
    )

    ready, fields = runner._post_route_close_minimal_control_ready(
        point,
        phase="co2",
        route_close_ts=time.time(),
        route_close_pressure_hpa=1125.0,
        pressure_hard_limit_hit=False,
    )

    assert ready is True
    assert fields["minimal_ready_uses_cached_vent_status"] is True
    assert fields["minimal_ready_realtime_query_count"] == 0
    assert fields["minimal_ready_gate_elapsed_s"] <= 1.0
    assert fields["minimal_ready_gate_exceeded"] is False


def test_route_close_to_setpoint_not_consumed_by_slew_config(monkeypatch) -> None:
    runner, pace, _, _ = _runner(gauge=FakeGauge([1112.0, 1112.0]))
    point = _co2_point(pressure=900.0)
    runner._apply_valve_states = MagicMock()
    monkeypatch.setattr("time.sleep", lambda _seconds: None)

    assert runner._pressurize_route_for_sealed_points(point, route="co2", sealed_control_refs=[point]) is True

    fields = _last_stage_fields(runner, "sealed_control_setpoint_command_sent")
    assert fields["route_close_to_setpoint_s"] <= 3.0
    assert fields["slew_config_after_route_close_count"] == 0
    assert pace.calls.index(("setpoint", 900.0)) < pace.calls.index(("enable_control_output",))


def test_route_close_to_outp1_not_consumed_by_slew_config(monkeypatch) -> None:
    runner, pace, _, _ = _runner(gauge=FakeGauge([1112.0, 1112.0]))
    point = _co2_point(pressure=900.0)
    runner._apply_valve_states = MagicMock()
    monkeypatch.setattr("time.sleep", lambda _seconds: None)

    assert runner._pressurize_route_for_sealed_points(point, route="co2", sealed_control_refs=[point]) is True

    fields = _last_stage_fields(runner, "sealed_control_output_enable_command_sent")
    assert fields["route_close_to_outp1_s"] <= 5.0
    assert fields["slew_config_after_route_close_count"] == 0
    assert pace.calls.index(("setpoint", 900.0)) < pace.calls.index(("enable_control_output",))


def test_route_close_to_outp1_fast_path_when_setpoint_prearmed(monkeypatch) -> None:
    runner, pace, _, _ = _runner(
        gauge=FakeGauge([1112.0, 1112.0]),
        pressure_overrides={"fast_outp1_after_route_close_enabled": True},
    )
    point = _co2_point(pressure=900.0)
    runner._apply_valve_states = MagicMock()
    monkeypatch.setattr("time.sleep", lambda _seconds: None)

    assert runner._pressurize_route_for_sealed_points(point, route="co2", sealed_control_refs=[point]) is True

    fields = _last_stage_fields(runner, "sealed_control_output_enable_command_sent")
    assert fields["outp1_fast_path_used"] is True
    assert fields["setpoint_prearmed_fast_outp1_path"] is True
    assert fields["route_close_to_outp1_s"] <= 2.0
    assert ("enable_control_output",) not in pace.calls
    assert ("output", True) in pace.calls
    assert pace.calls.index(("setpoint", 900.0)) < pace.calls.index(("output", True))


def test_outp1_delay_trace_records_blocking_steps(monkeypatch) -> None:
    runner, _pace, _, _ = _runner(
        gauge=FakeGauge([1112.0, 1112.0]),
        pressure_overrides={"fast_outp1_after_route_close_enabled": True},
    )
    point = _co2_point(pressure=900.0)
    runner._apply_valve_states = MagicMock()
    monkeypatch.setattr("time.sleep", lambda _seconds: None)

    assert runner._pressurize_route_for_sealed_points(point, route="co2", sealed_control_refs=[point]) is True

    fields = _last_stage_fields(runner, "sealed_control_output_enable_command_sent")
    assert fields["route_close_to_outp1_blocking_steps"]
    assert "set_output_true" in fields["route_close_to_outp1_blocking_steps"]
    assert fields["route_close_to_outp1_blocking_step_durations"]
    assert fields["outp1_delay_reason"] in {
        "fast_path",
        "fast_path_serial_write_elapsed_above_target",
    }


def test_fast_control_chain_still_setpoint_before_outp1(monkeypatch) -> None:
    runner, pace, _, _ = _runner(gauge=FakeGauge([1112.0, 1112.0]))
    point = _co2_point(pressure=900.0)
    runner._apply_valve_states = MagicMock()
    monkeypatch.setattr("time.sleep", lambda _seconds: None)

    assert runner._pressurize_route_for_sealed_points(point, route="co2", sealed_control_refs=[point]) is True

    stages = _trace_stages(runner)
    assert "sealed_fast_control_branch_entered" in stages
    assert stages.index("sealed_control_setpoint_command_sent") < stages.index(
        "sealed_control_output_enable_command_sent"
    )
    assert pace.calls.index(("setpoint", 900.0)) < pace.calls.index(("enable_control_output",))


def test_setpoint_prearmed_before_route_close_reduces_outp1_delay(monkeypatch) -> None:
    runner, pace, _, _ = _runner(gauge=FakeGauge([1112.0, 1112.0]))
    point = _co2_point(pressure=900.0)
    runner._apply_valve_states = MagicMock()
    monkeypatch.setattr("time.sleep", lambda _seconds: None)

    assert runner._pressurize_route_for_sealed_points(point, route="co2", sealed_control_refs=[point]) is True

    stages = _trace_stages(runner)
    assert stages.index("controlled_exit_atmosphere_driver_exit_done") < stages.index(
        "sealed_control_setpoint_prearm_before_route_close"
    )
    assert stages.index("sealed_control_setpoint_prearm_before_route_close") < stages.index(
        "route_valves_closed_after_vent0"
    )
    prearm_fields = _last_stage_fields(runner, "sealed_control_setpoint_prearm_before_route_close")
    assert prearm_fields["setpoint_prearm_before_vent_exit_detected"] is False
    fields = _last_stage_fields(runner, "sealed_control_output_enable_command_sent")
    assert fields["setpoint_prearmed_before_route_close"] is True
    assert fields["route_close_to_outp1_s"] <= 3.0
    assert fields["route_close_to_outp1_target_s"] == pytest.approx(2.0)
    assert fields["route_close_to_outp1_hard_max_s"] == pytest.approx(3.0)
    assert pace.calls.index(("setpoint", 900.0)) < pace.calls.index(("enable_control_output",))


def test_setpoint_prearm_failure_fails_before_route_close(monkeypatch) -> None:
    runner, pace, _, _ = _runner(pace=SetpointPrearmFailPace(), gauge=FakeGauge([1112.0, 1112.0]))
    point = _co2_point(pressure=900.0)
    runner._apply_valve_states = MagicMock()
    monkeypatch.setattr("time.sleep", lambda _seconds: None)

    assert runner._pressurize_route_for_sealed_points(point, route="co2", sealed_control_refs=[point]) is False

    stages = _trace_stages(runner)
    assert "sealed_control_setpoint_prearm_before_route_close_failed" in stages
    assert "route_valves_closed_after_vent0" not in stages
    assert ("enable_control_output",) not in pace.calls


def test_setpoint_prearm_not_before_vent_exit_completed() -> None:
    runner, pace, _, _ = _runner()
    point = _co2_point(pressure=900.0)

    assert runner._prearm_sealed_control_setpoint(
        point=point,
        phase="co2",
        pressure_target_hpa=900.0,
    ) is False

    fields = _last_stage_fields(runner, "sealed_control_setpoint_prearm_before_route_close_failed")
    assert fields["setpoint_prearm_before_vent_exit_detected"] is True
    assert ("setpoint", 900.0) not in pace.calls
    assert runner._controlled_exit_final_decision == "FAIL_CLOSED_SEALED_SETPOINT_PREARM_FAILED"


def test_descending_pressure_point_allows_exhaust_only_control() -> None:
    runner, pace, _, _ = _runner()
    point = _co2_point(pressure=900.0)
    runner._activate_co2_sealed_no_vent_guard(point, reason="test", route_close_ts=time.time())
    runner._record_preseal_pressure_control_ready_state(point, phase="co2", defer_live_check=False)

    assert runner._set_pressure_to_target(point) is True

    fields = _last_stage_fields(runner, "sealed_positive_supply_precheck")
    assert fields["pressure_control_direction_expected"] == "exhaust_only"
    assert fields["positive_supply_required"] is False
    assert fields["pressure_point_sequence_violation"] is False
    assert pace.calls.index(("setpoint", 900.0)) < pace.calls.index(("enable_control_output",))


def test_exhaust_only_ready_accepts_above_target_window() -> None:
    runner, pace, _, _ = _runner()
    point = _co2_point(pressure=1000.0)
    pace.in_limits = [(1001.0, 1)]
    pace.efforts = [-0.5]
    runner._activate_co2_sealed_no_vent_guard(point, reason="test", route_close_ts=time.time())
    runner._record_preseal_pressure_control_ready_state(point, phase="co2", defer_live_check=False)

    assert runner._set_pressure_to_target(point) is True

    fields = _last_stage_fields(runner, "pressure_in_limits")
    assert fields["exhaust_only_ready_gate_enabled"] is True
    assert fields["one_sided_pressure_ready"] is True
    assert fields["exhaust_only_ready_result"] == "pass"


def test_exhaust_only_above_target_candidate_detected() -> None:
    runner, _pace, _, _ = _runner(
        pressure_overrides={"exhaust_only_sample_above_target_enabled": True}
    )
    point = _co2_point(pressure=1000.0)
    state: dict = {}
    runner._exhaust_only_tracking_context = MagicMock(return_value=state)

    ok, _one_sided_ready, fields, _reason = runner._evaluate_exhaust_only_pressure_ready(
        target=1000.0,
        pressure_hpa=1000.8,
        in_limits=0,
        pressure_control_direction_expected="exhaust_only",
    )

    assert ok is True
    assert fields["exhaust_only_candidate_window_entered"] is True
    assert fields["exhaust_only_candidate_sampling_allowed"] is False
    assert fields["exhaust_only_candidate_pressure_hpa"] == pytest.approx(1000.8)
    assert state["actual_pressure_used_for_sample"] == pytest.approx(1000.8)


def test_exhaust_only_above_target_sampling_allowed_no_write() -> None:
    runner, pace, _, _ = _runner(
        pressure_overrides={
            "exhaust_only_sample_above_target_enabled": True,
            "exhaust_only_sample_above_target_allow_sampling": True,
        }
    )
    point = _co2_point(pressure=1000.0)
    pace.in_limits = [(1000.8, 0)]
    pace.efforts = [-0.5]
    runner._activate_co2_sealed_no_vent_guard(point, reason="test", route_close_ts=time.time())
    runner._record_preseal_pressure_control_ready_state(point, phase="co2", defer_live_check=False)

    assert runner._set_pressure_to_target(point) is True

    fields = _last_stage_fields(runner, "pressure_in_limits")
    assert fields["exhaust_only_candidate_window_entered"] is True
    assert fields["exhaust_only_candidate_sampling_allowed"] is True
    assert fields["pressure_in_limit"] is False
    assert fields["pressure_stable_evidence"] == "exhaust_only_above_target_window"
    assert fields["actual_pressure_used_for_sample"] == pytest.approx(1000.8)
    assert fields["nominal_target_hpa"] == pytest.approx(1000.0)
    assert fields["pressure_above_target_sample_offset_hpa"] == pytest.approx(0.8)
    assert fields["sampled_from_above_target_window"] is True
    assert ("vent", True) not in pace.calls


def test_above_target_sampling_allowed_in_limited_no_write() -> None:
    test_exhaust_only_above_target_sampling_allowed_no_write()


def test_above_target_sampling_not_enabled_by_default() -> None:
    runner, _pace, _, _ = _runner()

    _ok, _one_sided_ready, fields, _reason = runner._evaluate_exhaust_only_pressure_ready(
        target=1000.0,
        pressure_hpa=1000.736,
        in_limits=0,
        pressure_control_direction_expected="exhaust_only",
    )

    assert fields["exhaust_only_sample_above_target_enabled"] is False
    assert fields["exhaust_only_candidate_window_entered"] is False
    assert fields["exhaust_only_candidate_sampling_allowed"] is False


def test_actual_pressure_recorded_for_above_target_sample() -> None:
    runner, pace, _, _ = _runner()
    point = _co2_point(pressure=1000.0)
    pace.efforts = [-0.5]
    runner._activate_co2_sealed_no_vent_guard(point, reason="test", route_close_ts=time.time())
    context = runner._sealed_sweep_context_for_counters()
    assert context is not None
    context["actual_pressure_used_for_sample"] = 1000.8
    context["nominal_target_hpa"] = 1000.0
    context["sampled_from_above_target_window"] = True
    runner._all_gas_analyzers = MagicMock(return_value=[])
    runner._sampling_window_worker_plan = MagicMock(return_value={})
    runner._prime_sampling_window_context = MagicMock()
    runner._merge_fast_signal_cache_into_sample = MagicMock(
        side_effect=lambda data, *_args, **_kwargs: data.update(
            {
                "pressure_hpa": 1000.8,
                "pace_sample_ts": data["sample_start_ts"],
            }
        )
    )
    runner._sampling_row_pace_state_snapshot = MagicMock(
        return_value={
            "pace_output_state": 1,
            "pace_isolation_state": 1,
            "pace_vent_status": 2,
        }
    )
    runner._merge_analyzer_cache_into_sample = MagicMock(return_value={})
    runner._merge_slow_aux_cache_into_sample = MagicMock()

    samples = runner._collect_samples(point, 1, 0.0, phase="co2")

    assert samples is not None
    assert samples[0]["actual_pressure_used_for_sample"] == pytest.approx(1000.8)
    assert samples[0]["nominal_target_hpa"] == pytest.approx(1000.0)
    assert samples[0]["pressure_above_target_sample_offset_hpa"] == pytest.approx(0.8)
    assert samples[0]["sampled_from_above_target_window"] is True


def test_above_target_sample_records_actual_pressure() -> None:
    test_actual_pressure_recorded_for_above_target_sample()


def test_above_target_candidate_safe_at_detection_samples_immediately_no_write() -> None:
    runner, _pace, _, _ = _runner(
        pressure_overrides={
            "exhaust_only_sample_above_target_enabled": True,
            "exhaust_only_sample_above_target_allow_sampling": True,
        }
    )
    point = _co2_point(pressure=1000.0)
    runner._activate_co2_sealed_no_vent_guard(point, reason="test", route_close_ts=time.time())
    context = runner._sealed_sweep_context_for_counters()
    assert context is not None
    context.update(
        {
            "exhaust_only_candidate_sampling_allowed": True,
            "sampled_from_above_target_window": True,
            "actual_pressure_used_for_sample": 1000.007,
            "nominal_target_hpa": 1000.0,
            "dewpoint_abnormal_at_candidate": False,
        }
    )
    runner._wait_postseal_dewpoint_gate = MagicMock(
        side_effect=AssertionError("safe above-target candidate should not wait for later dewpoint gate")
    )
    runner._wait_co2_presample_long_guard = MagicMock(
        side_effect=AssertionError("safe above-target candidate should enter sampling immediately")
    )

    assert runner._wait_after_pressure_stable_before_sampling(point) is True

    begin_calls = [
        call
        for call in runner._append_pressure_trace_row.call_args_list
        if call.kwargs.get("trace_stage") == "sampling_begin"
    ]
    assert begin_calls
    assert begin_calls[-1].kwargs.get("trigger_reason") == "above_target_candidate_immediate_no_write"


def test_above_target_candidate_abnormal_at_detection_fails_before_sampling() -> None:
    runner, _pace, _, _ = _runner(
        pressure_overrides={
            "exhaust_only_sample_above_target_enabled": True,
            "exhaust_only_sample_above_target_allow_sampling": True,
        }
    )
    point = _co2_point(pressure=1000.0)
    runner._activate_co2_sealed_no_vent_guard(point, reason="test", route_close_ts=time.time())
    runner._preseal_dewpoint_snapshot = {
        "dewpoint_c": -40.0,
        "pressure_hpa": 1013.0,
        "sample_wall_ts": time.time() - 30.0,
    }
    runner._cached_ready_check_trace_values = MagicMock(return_value={"dewpoint_c": -30.0})

    ok, _one_sided_ready, fields, reason = runner._evaluate_exhaust_only_pressure_ready(
        target=1000.0,
        pressure_hpa=1000.007,
        in_limits=0,
        pressure_control_direction_expected="exhaust_only",
        point=point,
    )

    assert ok is False
    assert reason == "FAIL_CLOSED_DEWPOINT_RISE_AT_ABOVE_TARGET_CANDIDATE"
    assert fields["exhaust_only_candidate_window_entered"] is True
    assert fields["dewpoint_abnormal_at_candidate"] is True
    assert fields["exhaust_only_candidate_sampling_allowed"] is False


def test_above_target_sample_invalidated_by_dewpoint_rise_during_sample() -> None:
    runner, pace, _, _ = _runner()
    point = _co2_point(pressure=1000.0)
    pace.efforts = [-0.5]
    runner._activate_co2_sealed_no_vent_guard(point, reason="test", route_close_ts=time.time())
    context = runner._sealed_sweep_context_for_counters()
    assert context is not None
    context.update(
        {
            "actual_pressure_used_for_sample": 1000.007,
            "nominal_target_hpa": 1000.0,
            "sampled_from_above_target_window": True,
            "exhaust_only_candidate_sampling_allowed": True,
            "dewpoint_at_candidate": -36.0,
            "dewpoint_abnormal_at_candidate": False,
        }
    )
    runner._preseal_dewpoint_snapshot = {
        "dewpoint_c": -40.0,
        "pressure_hpa": 1013.0,
        "sample_wall_ts": time.time() - 30.0,
    }
    runner._all_gas_analyzers = MagicMock(return_value=[])
    runner._sampling_window_worker_plan = MagicMock(return_value={})
    runner._prime_sampling_window_context = MagicMock()
    runner._merge_fast_signal_cache_into_sample = MagicMock(
        side_effect=lambda data, *_args, **_kwargs: data.update(
            {
                "pressure_hpa": 1000.007,
                "dewpoint_live_c": -30.0,
                "dewpoint_live_sample_ts": data["sample_start_ts"],
                "pace_sample_ts": data["sample_start_ts"],
            }
        )
    )
    runner._sampling_row_pace_state_snapshot = MagicMock(
        return_value={"pace_output_state": 1, "pace_isolation_state": 1, "pace_vent_status": 2}
    )
    runner._merge_analyzer_cache_into_sample = MagicMock(return_value={})
    runner._merge_slow_aux_cache_into_sample = MagicMock()

    samples = runner._collect_samples(point, 1, 0.0, phase="co2")

    assert samples is not None
    assert samples[0]["sample_invalidated_by_dewpoint_rise"] is True
    assert samples[0]["dewpoint_abnormal_during_sample"] is True
    assert samples[0]["sample_valid_for_acceptance"] is False


def test_above_target_sample_invalidated_by_target_crossing_during_sample() -> None:
    runner, pace, _, _ = _runner()
    point = _co2_point(pressure=1000.0)
    pace.efforts = [-0.5]
    runner._activate_co2_sealed_no_vent_guard(point, reason="test", route_close_ts=time.time())
    context = runner._sealed_sweep_context_for_counters()
    assert context is not None
    context.update(
        {
            "actual_pressure_used_for_sample": 999.9,
            "nominal_target_hpa": 1000.0,
            "sampled_from_above_target_window": True,
            "exhaust_only_candidate_sampling_allowed": True,
            "dewpoint_at_candidate": -36.0,
            "dewpoint_abnormal_at_candidate": False,
        }
    )
    runner._all_gas_analyzers = MagicMock(return_value=[])
    runner._sampling_window_worker_plan = MagicMock(return_value={})
    runner._prime_sampling_window_context = MagicMock()
    runner._merge_fast_signal_cache_into_sample = MagicMock(
        side_effect=lambda data, *_args, **_kwargs: data.update(
            {"pressure_hpa": 999.9, "pace_sample_ts": data["sample_start_ts"]}
        )
    )
    runner._sampling_row_pace_state_snapshot = MagicMock(
        return_value={"pace_output_state": 1, "pace_isolation_state": 1, "pace_vent_status": 2}
    )
    runner._merge_analyzer_cache_into_sample = MagicMock(return_value={})
    runner._merge_slow_aux_cache_into_sample = MagicMock()

    samples = runner._collect_samples(point, 2, 0.0, phase="co2")

    assert samples is not None
    assert len(samples) == 1
    assert samples[0]["sample_invalidated_by_target_crossing"] is True
    assert samples[0]["sample_valid_for_acceptance"] is False


def test_above_target_sample_records_candidate_dewpoint_and_invalid_flags() -> None:
    runner, pace, _, _ = _runner()
    point = _co2_point(pressure=1000.0)
    pace.efforts = [-0.5]
    runner._activate_co2_sealed_no_vent_guard(point, reason="test", route_close_ts=time.time())
    context = runner._sealed_sweep_context_for_counters()
    assert context is not None
    context.update(
        {
            "actual_pressure_used_for_sample": 1000.8,
            "nominal_target_hpa": 1000.0,
            "sampled_from_above_target_window": True,
            "exhaust_only_candidate_sampling_allowed": True,
            "dewpoint_at_candidate": -35.26,
            "dewpoint_abnormal_at_candidate": False,
        }
    )
    runner._all_gas_analyzers = MagicMock(return_value=[])
    runner._sampling_window_worker_plan = MagicMock(return_value={})
    runner._prime_sampling_window_context = MagicMock()
    runner._merge_fast_signal_cache_into_sample = MagicMock(
        side_effect=lambda data, *_args, **_kwargs: data.update(
            {"pressure_hpa": 1000.8, "pace_sample_ts": data["sample_start_ts"]}
        )
    )
    runner._sampling_row_pace_state_snapshot = MagicMock(
        return_value={"pace_output_state": 1, "pace_isolation_state": 1, "pace_vent_status": 2}
    )
    runner._merge_analyzer_cache_into_sample = MagicMock(return_value={})
    runner._merge_slow_aux_cache_into_sample = MagicMock()

    samples = runner._collect_samples(point, 1, 0.0, phase="co2")

    assert samples is not None
    assert samples[0]["dewpoint_at_candidate"] == pytest.approx(-35.26)
    assert samples[0]["dewpoint_abnormal_at_candidate"] is False
    assert samples[0]["sampled_from_above_target_window"] is True


def test_exhaust_only_ready_blocks_below_target_undershoot() -> None:
    runner, pace, _, _ = _runner()
    point = _co2_point(pressure=1000.0)
    pace.in_limits = [(999.0, 0)]
    runner._activate_co2_sealed_no_vent_guard(point, reason="test", route_close_ts=time.time())
    runner._record_preseal_pressure_control_ready_state(point, phase="co2", defer_live_check=False)

    assert runner._set_pressure_to_target(point) is False

    fields = _last_stage_fields(runner, "sealed_pressure_control_state_fail")
    assert fields["pressure_undershoot_detected"] is True
    assert fields["exhaust_only_ready_failure_reason"] == "FAIL_CLOSED_PRESSURE_UNDERSHOOT_EXHAUST_ONLY"


def test_exhaust_only_ready_blocks_target_chatter(monkeypatch) -> None:
    runner, pace, _, _ = _runner(pressure_overrides={"stabilize_timeout_s": 0.2})
    point = _co2_point(pressure=1000.0)
    pace.in_limits = [(1001.0, 0), (999.9, 0), (1000.5, 0)]
    runner._activate_co2_sealed_no_vent_guard(point, reason="test", route_close_ts=time.time())
    runner._record_preseal_pressure_control_ready_state(point, phase="co2", defer_live_check=False)
    monkeypatch.setattr("time.sleep", lambda _seconds: None)

    assert runner._set_pressure_to_target(point) is False

    fields = _last_stage_fields(runner, "sealed_pressure_control_state_fail")
    assert fields["pressure_chatter_detected"] is True
    assert fields["pressure_target_crossing_count"] >= 1
    assert fields["target_crossing_count"] >= 1
    assert fields["pressure_chatter_fail_closed"] is True
    assert fields["sampling_blocked_by_control_chatter"] is True
    assert fields["exhaust_only_ready_failure_reason"] == "FAIL_CLOSED_PRESSURE_TARGET_CHATTER_EXHAUST_ONLY"


def test_exhaust_only_blocks_any_target_crossing(monkeypatch) -> None:
    runner, pace, _, _ = _runner(pressure_overrides={"stabilize_timeout_s": 0.2})
    point = _co2_point(pressure=1000.0)
    pace.in_limits = [(1001.0, 0), (999.9, 0)]
    runner._activate_co2_sealed_no_vent_guard(point, reason="test", route_close_ts=time.time())
    runner._record_preseal_pressure_control_ready_state(point, phase="co2", defer_live_check=False)
    monkeypatch.setattr("time.sleep", lambda _seconds: None)

    assert runner._set_pressure_to_target(point) is False

    fields = _last_stage_fields(runner, "sealed_pressure_control_state_fail")
    assert fields["target_crossing_count"] == 1
    assert fields["pressure_chatter_fail_closed"] is True
    assert fields["sampling_blocked_by_control_chatter"] is True
    assert fields["first_target_crossing_ts"]
    assert fields["pressure_wait_cycles_after_crossing"] == 0
    assert fields["fail_immediately_after_crossing"] is True


def test_target_crossing_fails_immediately_without_extra_wait_cycles(monkeypatch) -> None:
    test_exhaust_only_blocks_any_target_crossing(monkeypatch)


def test_target_crossing_still_fails_even_with_above_target_enabled(monkeypatch) -> None:
    runner, pace, _, _ = _runner(
        pressure_overrides={
            "stabilize_timeout_s": 0.2,
            "exhaust_only_sample_above_target_enabled": True,
            "exhaust_only_sample_above_target_allow_sampling": True,
        }
    )
    point = _co2_point(pressure=1000.0)
    pace.in_limits = [(1001.2, 0), (999.9, 0)]
    runner._activate_co2_sealed_no_vent_guard(point, reason="test", route_close_ts=time.time())
    runner._record_preseal_pressure_control_ready_state(point, phase="co2", defer_live_check=False)
    monkeypatch.setattr("time.sleep", lambda _seconds: None)

    assert runner._set_pressure_to_target(point) is False

    fields = _last_stage_fields(runner, "sealed_pressure_control_state_fail")
    assert fields["target_crossing_count"] == 1
    assert fields["pressure_chatter_fail_closed"] is True
    assert fields["exhaust_only_ready_failure_reason"] == "FAIL_CLOSED_PRESSURE_TARGET_CHATTER_EXHAUST_ONLY"


def test_target_crossing_records_first_crossing_to_fail_seconds(monkeypatch) -> None:
    runner, pace, _, _ = _runner(pressure_overrides={"stabilize_timeout_s": 0.2})
    point = _co2_point(pressure=1000.0)
    pace.in_limits = [(1001.0, 0), (999.9, 0)]
    runner._activate_co2_sealed_no_vent_guard(point, reason="test", route_close_ts=time.time())
    runner._record_preseal_pressure_control_ready_state(point, phase="co2", defer_live_check=False)
    monkeypatch.setattr("time.sleep", lambda _seconds: None)

    assert runner._set_pressure_to_target(point) is False

    fields = _last_stage_fields(runner, "sealed_pressure_control_state_fail")
    assert fields["first_target_crossing_ts"]
    assert fields["target_crossing_to_fail_s"] <= 1.0
    assert fields["target_crossing_to_safe_stop_s"] <= 1.0


def test_exhaust_only_blocks_below_target_duration() -> None:
    runner, pace, _, _ = _runner()
    point = _co2_point(pressure=1000.0)
    pace.in_limits = [(999.0, 0)]
    runner._activate_co2_sealed_no_vent_guard(point, reason="test", route_close_ts=time.time())
    runner._record_preseal_pressure_control_ready_state(point, phase="co2", defer_live_check=False)

    assert runner._set_pressure_to_target(point) is False

    fields = _last_stage_fields(runner, "sealed_pressure_control_state_fail")
    assert fields["pressure_crossed_below_target"] is True
    assert fields["pressure_monotonic_to_target"] is False
    assert fields["exhaust_only_ready_failure_reason"] == "FAIL_CLOSED_PRESSURE_UNDERSHOOT_EXHAUST_ONLY"


def test_exhaust_only_blocks_small_positive_effort() -> None:
    runner, pace, _, _ = _runner()
    point = _co2_point(pressure=1000.0)
    pace.in_limits = [(1001.0, 1)]
    pace.efforts = [0.5]
    runner._activate_co2_sealed_no_vent_guard(point, reason="test", route_close_ts=time.time())
    runner._record_preseal_pressure_control_ready_state(point, phase="co2", defer_live_check=False)

    assert runner._set_pressure_to_target(point) is False

    fields = _last_stage_fields(runner, "sealed_pressure_control_state_fail")
    assert fields["positive_supply_effort_fail_pct"] == pytest.approx(0.3)
    assert fields["positive_effort_strict_threshold_pct"] == pytest.approx(0.3)
    assert fields["positive_supply_effort_strict_fail"] is True
    assert fields["positive_supply_effort_detected"] is True
    assert fields["positive_effort_fail_closed"] is True


def test_positive_effort_0_3pct_blocks_sampling() -> None:
    runner, pace, _, _ = _runner()
    point = _co2_point(pressure=1000.0)
    pace.in_limits = [(1001.0, 1)]
    pace.efforts = [0.3]
    runner._activate_co2_sealed_no_vent_guard(point, reason="test", route_close_ts=time.time())
    runner._record_preseal_pressure_control_ready_state(point, phase="co2", defer_live_check=False)

    assert runner._set_pressure_to_target(point) is False

    fields = _last_stage_fields(runner, "sealed_pressure_control_state_fail")
    assert fields["positive_supply_effort_fail_pct"] == pytest.approx(0.3)
    assert fields["positive_effort_fail_closed"] is True
    assert fields["positive_supply_effort_detected"] is True


def test_positive_effort_warning_for_tiny_positive_noise() -> None:
    runner, pace, _, _ = _runner()
    point = _co2_point(pressure=1000.0)
    pace.in_limits = [(1001.0, 1)]
    pace.efforts = [0.1]
    runner._activate_co2_sealed_no_vent_guard(point, reason="test", route_close_ts=time.time())
    runner._record_preseal_pressure_control_ready_state(point, phase="co2", defer_live_check=False)

    assert runner._set_pressure_to_target(point) is True

    fields = _last_stage_fields(runner, "pressure_in_limits")
    assert fields["positive_supply_effort_warning_pct"] == pytest.approx(0.1)
    assert fields["positive_supply_effort_fail_pct"] == pytest.approx(0.3)
    assert fields["positive_effort_any_seen"] is True
    assert fields["positive_supply_effort_strict_fail"] is False
    assert fields["positive_supply_effort_detected"] is False


def test_inlimit_alone_does_not_allow_sampling_when_effort_positive() -> None:
    runner, pace, _, _ = _runner()
    point = _co2_point(pressure=1000.0)
    pace.output_state = 1
    pace.vent_status = 2
    pace.setpoint = 1000.0
    pace.efforts = [0.5]
    runner._activate_co2_sealed_no_vent_guard(point, reason="test", route_close_ts=time.time())
    runner._sealed_first_setpoint_tx_ts = time.time()
    runner._sealed_first_outp1_tx_ts = time.time()
    runner._mark_sealed_pressure_ready(result="in_limits")

    assert runner._co2_sealed_sampling_ready(point, point_tag="positive-effort-inlimit") is False

    fields = _last_stage_fields(runner, "sealed_sampling_ready_failed")
    assert fields["pressure_in_limit_before_sampling"] is True
    assert fields["positive_supply_effort_strict_fail"] is True
    assert fields["sample_blocked_by_positive_supply_effort"] is True
    assert fields["positive_effort_fail_closed"] is True


def test_inlimit_true_but_chatter_blocks_sampling(monkeypatch) -> None:
    runner, pace, _, _ = _runner(pressure_overrides={"stabilize_timeout_s": 0.2})
    point = _co2_point(pressure=1000.0)
    pace.in_limits = [(1001.0, 0), (999.9, 1)]
    runner._activate_co2_sealed_no_vent_guard(point, reason="test", route_close_ts=time.time())
    runner._record_preseal_pressure_control_ready_state(point, phase="co2", defer_live_check=False)
    monkeypatch.setattr("time.sleep", lambda _seconds: None)

    assert runner._set_pressure_to_target(point) is False

    fields = _last_stage_fields(runner, "sealed_pressure_control_state_fail")
    assert fields["pressure_chatter_detected"] is True
    assert fields["pressure_chatter_fail_closed"] is True
    assert fields["sampling_blocked_by_control_chatter"] is True


def test_dewpoint_lag_correlation_marks_pressure_control_mixing() -> None:
    runner, _, _, _ = _runner()
    point = _co2_point(pressure=1100.0)
    now = time.time()
    runner._preseal_dewpoint_snapshot = {
        "dewpoint_c": -36.0,
        "pressure_hpa": 1110.0,
        "sample_wall_ts": now - 90.0,
    }
    runner._last_route_close_ts = now - 80.0
    runner._sealed_first_outp1_tx_ts = now - 45.0
    runner._cached_ready_check_trace_values = MagicMock(
        return_value={"dewpoint_c": -28.0, "pace_pressure_hpa": 1100.0}
    )

    fields = runner._sealed_dewpoint_rise_trace_fields(point=point)

    assert fields["dewpoint_rise_observed_c"] == pytest.approx(8.0)
    assert fields["dewpoint_lag_best_s"] != ""
    assert fields["dewpoint_rise_likely_phase"] == "OUTP1_after_pressure_ready_wait"
    assert fields["likely_pressure_control_mixing"] is True


def test_dewpoint_rise_with_lag_after_outp1_marks_pressure_control_mixing() -> None:
    test_dewpoint_lag_correlation_marks_pressure_control_mixing()


def test_local_dewpoint_rise_distinguished_from_abnormal_rise() -> None:
    runner, _, _, _ = _runner()
    point = _co2_point(pressure=1280.0)
    now = time.time()
    runner._preseal_dewpoint_snapshot = {
        "dewpoint_c": -36.0,
        "pressure_hpa": 1000.0,
        "sample_wall_ts": now - 30.0,
    }
    runner._sealed_first_outp1_tx_ts = now - 20.0
    runner._cached_ready_check_trace_values = MagicMock(
        return_value={"dewpoint_c": -35.6, "pace_pressure_hpa": 1280.0}
    )

    fields = runner._sealed_dewpoint_rise_trace_fields(point=point)

    assert fields["dewpoint_rise_observed_c"] == pytest.approx(0.4)
    assert fields["dewpoint_local_rise_detected"] is True
    assert fields["dewpoint_abnormal_rise_detected"] is False
    assert fields["likely_pressure_control_mixing"] is False
    assert runner._sealed_dewpoint_rise_exceeded(point=point) is False


def test_dewpoint_local_rise_detected_even_when_sampling_blocked(monkeypatch) -> None:
    runner, pace, _, _ = _runner(
        pressure_overrides={
            "stabilize_timeout_s": 0.2,
            "exhaust_only_sample_above_target_enabled": False,
        }
    )
    point = _co2_point(pressure=1000.0)
    pace.in_limits = [(1001.0, 0), (999.9, 0)]
    now = time.time()
    runner._preseal_dewpoint_snapshot = {
        "dewpoint_c": -36.99,
        "pressure_hpa": 1280.0,
        "sample_wall_ts": now - 60.0,
    }
    runner._cached_ready_check_trace_values = MagicMock(
        return_value={"dewpoint_c": -33.79, "pace_pressure_hpa": 1000.0}
    )
    runner._activate_co2_sealed_no_vent_guard(point, reason="test", route_close_ts=now)
    runner._record_preseal_pressure_control_ready_state(point, phase="co2", defer_live_check=False)
    monkeypatch.setattr("time.sleep", lambda _seconds: None)

    assert runner._set_pressure_to_target(point) is False

    fields = _last_stage_fields(runner, "sealed_pressure_control_state_fail")
    assert fields["target_crossing_count"] == 1
    assert fields["dewpoint_local_rise_detected"] is True
    assert fields["dewpoint_local_rise_max_c"] == pytest.approx(3.2)
    assert "dewpoint_abnormal_rise_detected" in fields


def test_dewpoint_local_rise_detected_when_pressure_wait_rises(monkeypatch) -> None:
    test_dewpoint_local_rise_detected_even_when_sampling_blocked(monkeypatch)


def test_dewpoint_local_rise_not_equal_abnormal_rise() -> None:
    test_local_dewpoint_rise_distinguished_from_abnormal_rise()


def test_dewpoint_rise_still_fails_before_sampling() -> None:
    runner, pace, _, _ = _runner()
    point = _co2_point(pressure=1000.0)
    pace.output_state = 1
    pace.vent_status = 2
    pace.setpoint = 1000.0
    pace.efforts = [-1.0]
    now = time.time()
    runner._preseal_dewpoint_snapshot = {
        "dewpoint_c": -36.0,
        "pressure_hpa": 1110.0,
        "sample_wall_ts": now - 60.0,
    }
    runner._cached_ready_check_trace_values = MagicMock(
        return_value={"dewpoint_c": -28.0, "pace_pressure_hpa": 1100.0}
    )
    runner._activate_co2_sealed_no_vent_guard(point, reason="test", route_close_ts=now - 50.0)
    runner._sealed_first_setpoint_tx_ts = now - 49.0
    runner._sealed_first_outp1_tx_ts = now - 48.0
    runner._mark_sealed_pressure_ready(result="in_limits")

    assert runner._co2_sealed_sampling_ready(point, point_tag="dewpoint-rise") is False

    fields = _last_stage_fields(runner, "sealed_sampling_ready_failed")
    assert fields["sampling_blocked_by_dewpoint_rise"] is True
    assert fields["pressure_controller_control_state_failure_reason"] == "FAIL_CLOSED_DEWPOINT_RISE_BEFORE_SAMPLING"


def test_repeated_outp1_not_sent_when_already_on() -> None:
    runner, pace, _, _ = _runner()
    point = _co2_point(pressure=1000.0)
    pace.output_state = 1
    runner._activate_co2_sealed_no_vent_guard(point, reason="test", route_close_ts=time.time())

    assert runner._enable_controlled_outp_after_seal(point, phase="co2", pressure_target_hpa=1000.0) is True

    assert ("enable_control_output",) not in pace.calls
    assert ("output", True) not in pace.calls
    stages = _trace_stages(runner)
    assert "sealed_control_output_already_on" in stages


def test_ascending_pressure_point_blocks_when_clean_supply_not_confirmed() -> None:
    runner, pace, _, _ = _runner()
    point = _co2_point(pressure=1100.0)
    runner._activate_co2_sealed_no_vent_guard(point, reason="test", route_close_ts=time.time())
    runner._record_preseal_pressure_control_ready_state(point, phase="co2", defer_live_check=False)

    assert runner._set_pressure_to_target(point) is False

    fields = _last_stage_fields(runner, "sealed_positive_supply_precheck_failed")
    assert fields["positive_supply_required"] is True
    assert fields["positive_supply_forbidden"] is True
    assert fields["pressure_point_sequence_violation"] is True
    assert ("setpoint", 1100.0) not in pace.calls
    assert ("enable_control_output",) not in pace.calls


def test_clean_positive_supply_flag_allows_ascending_points_only_when_true() -> None:
    runner, pace, _, _ = _runner(
        pressure_overrides={"clean_positive_supply_confirmed": True}
    )
    point = _co2_point(pressure=1100.0)
    runner._activate_co2_sealed_no_vent_guard(point, reason="test", route_close_ts=time.time())
    runner._record_preseal_pressure_control_ready_state(point, phase="co2", defer_live_check=False)

    assert runner._set_pressure_to_target(point) is True

    fields = _last_stage_fields(runner, "sealed_positive_supply_precheck")
    assert fields["positive_supply_required"] is True
    assert fields["positive_supply_forbidden"] is False
    assert fields["clean_positive_supply_confirmed"] is True
    assert ("setpoint", 1100.0) in pace.calls


def test_positive_supply_guard_rejects_stale_open_flow_pressure() -> None:
    runner, pace, gauge, _ = _runner()
    point = _co2_point(pressure=1100.0)
    route_close_ts = time.time()
    pace.read_pressure = MagicMock(return_value=None)
    pace.get_in_limits = MagicMock(return_value=None)
    gauge.read_pressure = MagicMock(return_value=None)
    runner._activate_co2_sealed_no_vent_guard(point, reason="test", route_close_ts=route_close_ts)

    ok, fields = runner._post_route_close_minimal_control_ready(
        point,
        phase="co2",
        route_close_ts=route_close_ts,
        route_close_pressure_hpa=1008.0,
    )

    assert ok is False
    assert fields["minimal_ready_gate_blocked_by"] == "FAIL_CLOSED_STALE_PRESEAL_PRESSURE_EVIDENCE"
    assert fields["positive_supply_required_used_stale_pressure"] is False
    assert fields["positive_supply_guard_blocked_due_to_stale_pressure_evidence"] is True
    assert fields["stale_pressure_value_hpa"] == pytest.approx(1008.0)
    assert "sealed_positive_supply_precheck_fast_control_failed" not in _trace_stages(runner)


def test_positive_supply_guard_waits_for_fresh_post_close_pressure() -> None:
    runner, pace, gauge, _ = _runner()
    point = _co2_point(pressure=1100.0)
    route_close_ts = time.time()
    pace.read_pressure = MagicMock(return_value=1122.0)
    gauge.read_pressure = MagicMock(return_value=1121.5)
    runner._activate_co2_sealed_no_vent_guard(point, reason="test", route_close_ts=route_close_ts)

    ok, fields = runner._post_route_close_minimal_control_ready(
        point,
        phase="co2",
        route_close_ts=route_close_ts,
        route_close_pressure_hpa=1093.0,
    )

    assert ok is True
    pace.read_pressure.assert_called_once()
    gauge.read_pressure.assert_called_once()
    assert fields["positive_supply_guard_waited_for_fresh_post_close_pressure"] is True
    assert fields["post_route_close_pressure_fresh"] is True
    assert fields["positive_supply_direction_pressure_hpa"] == pytest.approx(1122.0)
    assert fields["positive_supply_direction_pressure_source"] == "pace"


def test_fresh_post_close_pressure_above_target_allows_exhaust_only() -> None:
    runner, pace, gauge, _ = _runner(
        pressure_overrides={"post_route_absolute_pressure_hard_limit_hpa": 1300.0}
    )
    point = _co2_point(pressure=1100.0)
    route_close_ts = time.time()
    pace.read_pressure = MagicMock(return_value=1300.0)
    gauge.read_pressure = MagicMock(return_value=1298.0)
    runner._activate_co2_sealed_no_vent_guard(point, reason="test", route_close_ts=route_close_ts)

    ready_ok, ready_fields = runner._post_route_close_minimal_control_ready(
        point,
        phase="co2",
        route_close_ts=route_close_ts,
        route_close_pressure_hpa=1008.0,
    )
    assert ready_ok is True

    assert runner._start_fast_control_after_route_close(
        point,
        phase="co2",
        pressure_target_hpa=1100.0,
        ready_fields=ready_fields,
    ) is True

    fields = _last_stage_fields(runner, "sealed_positive_supply_precheck_fast_control")
    assert fields["current_pressure_before_point_hpa"] == pytest.approx(1300.0)
    assert fields["pressure_control_direction_expected"] == "exhaust_only"
    assert fields["positive_supply_required"] is False
    assert fields["positive_supply_required_misclassified_due_to_stale_pressure"] is True
    assert ("enable_control_output",) in pace.calls


def test_missing_post_close_pressure_reports_stale_evidence_not_positive_supply() -> None:
    runner, pace, gauge, _ = _runner()
    point = _co2_point(pressure=1100.0)
    route_close_ts = time.time()
    pace.read_pressure = MagicMock(return_value=None)
    pace.get_in_limits = MagicMock(return_value=None)
    gauge.read_pressure = MagicMock(return_value=None)
    runner._activate_co2_sealed_no_vent_guard(point, reason="test", route_close_ts=route_close_ts)

    ok, fields = runner._post_route_close_minimal_control_ready(
        point,
        phase="co2",
        route_close_ts=route_close_ts,
        route_close_pressure_hpa=None,
    )

    assert ok is False
    assert fields["minimal_ready_gate_blocked_by"] == "FAIL_CLOSED_MISSING_POST_CLOSE_PRESSURE_EVIDENCE"
    assert fields["missing_fresh_post_close_pressure_evidence"] is True
    assert fields["positive_supply_check_ran_before_post_close_pressure_evidence"] is True
    assert "positive_supply_required_but_clean_supply_not_confirmed" not in fields["minimal_ready_gate_blocked_by"]


def test_fresh_post_close_pressure_below_target_blocks_positive_supply() -> None:
    runner, pace, gauge, _ = _runner()
    point = _co2_point(pressure=1100.0)
    route_close_ts = time.time()
    pace.read_pressure = MagicMock(return_value=1008.0)
    gauge.read_pressure = MagicMock(return_value=1007.5)
    runner._activate_co2_sealed_no_vent_guard(point, reason="test", route_close_ts=route_close_ts)

    ready_ok, ready_fields = runner._post_route_close_minimal_control_ready(
        point,
        phase="co2",
        route_close_ts=route_close_ts,
        route_close_pressure_hpa=1008.0,
    )
    assert ready_ok is True

    assert runner._start_fast_control_after_route_close(
        point,
        phase="co2",
        pressure_target_hpa=1100.0,
        ready_fields=ready_fields,
    ) is False

    fields = _last_stage_fields(runner, "sealed_positive_supply_precheck_fast_control_failed")
    assert fields["current_pressure_before_point_hpa"] == pytest.approx(1008.0)
    assert fields["positive_supply_required"] is True
    assert fields["positive_supply_forbidden"] is True
    assert fields["post_route_close_pressure_fresh"] is True
    assert runner._controlled_exit_final_decision == (
        "FAIL_CLOSED_POSITIVE_SUPPLY_REQUIRED_BUT_CLEAN_SUPPLY_NOT_CONFIRMED"
    )


def test_setpoint_prearm_uses_fresh_pressure_evidence() -> None:
    runner, pace, gauge, _ = _runner()
    point = _co2_point(pressure=1100.0)
    route_close_ts = time.time()
    pace.read_pressure = MagicMock(return_value=1125.0)
    gauge.read_pressure = MagicMock(return_value=1124.0)
    runner._activate_co2_sealed_no_vent_guard(point, reason="test", route_close_ts=route_close_ts)

    ready_ok, ready_fields = runner._post_route_close_minimal_control_ready(
        point,
        phase="co2",
        route_close_ts=route_close_ts,
        route_close_pressure_hpa=1008.0,
    )
    assert ready_ok is True

    assert runner._start_fast_control_after_route_close(
        point,
        phase="co2",
        pressure_target_hpa=1100.0,
        ready_fields=ready_fields,
    ) is True

    fields = _last_stage_fields(runner, "sealed_control_setpoint_command_sent")
    assert fields["positive_supply_direction_pressure_hpa"] == pytest.approx(1125.0)
    assert fields["positive_supply_direction_pressure_source"] == "pace"
    assert fields["current_pressure_before_point_hpa"] == pytest.approx(1125.0)
    assert ("setpoint", 1100.0) in pace.calls


def test_post_close_pressure_hard_limit_still_blocks() -> None:
    runner, pace, gauge, _ = _runner(
        pressure_overrides={"post_route_absolute_pressure_hard_limit_hpa": 1200.0}
    )
    point = _co2_point(pressure=1100.0)
    route_close_ts = time.time()
    pace.read_pressure = MagicMock(return_value=1201.0)
    gauge.read_pressure = MagicMock(return_value=1200.5)
    runner._activate_co2_sealed_no_vent_guard(point, reason="test", route_close_ts=route_close_ts)

    ok, fields = runner._post_route_close_minimal_control_ready(
        point,
        phase="co2",
        route_close_ts=route_close_ts,
        route_close_pressure_hpa=1008.0,
    )

    assert ok is False
    assert fields["minimal_ready_gate_blocked_by"] == "FAIL_CLOSED_POST_ROUTE_ABSOLUTE_PRESSURE_HARD_LIMIT"
    assert fields["post_route_absolute_pressure_hard_limit_hit"] is True
    assert ("enable_control_output",) not in pace.calls


def test_no_open_flow_critical_query_regression() -> None:
    runner, pace, _, _ = _runner()
    pace.read_pressure = MagicMock(side_effect=AssertionError("open-flow pressure query must be deferred"))
    runner._record_open_flow_pressure_query_deferred_for_keepalive = MagicMock(return_value=True)
    runner._cached_open_flow_pace_pressure_for_keepalive = MagicMock(return_value=1012.0)

    readings = runner._read_controlled_outp_preseal_pressures()

    assert readings["pace_pressure_hpa"] == pytest.approx(1012.0)
    pace.read_pressure.assert_not_called()


def test_positive_supply_effort_blocks_sampling() -> None:
    runner, pace, _, _ = _runner(
        pressure_overrides={"positive_supply_effort_max_duration_s": 0.0}
    )
    point = _co2_point(pressure=900.0)
    pace.output_state = 1
    pace.vent_status = 2
    pace.setpoint = 900.0
    pace.efforts = [5.0]
    runner._activate_co2_sealed_no_vent_guard(point, reason="test", route_close_ts=time.time())
    runner._sealed_first_setpoint_tx_ts = time.time()
    runner._sealed_first_outp1_tx_ts = time.time()
    runner._mark_sealed_pressure_ready(result="in_limits")

    assert runner._co2_sealed_sampling_ready(point, point_tag="positive-effort") is False

    fields = _last_stage_fields(runner, "sealed_sampling_ready_failed")
    assert fields["positive_supply_effort_detected"] is True
    assert fields["sample_blocked_by_positive_supply_effort"] is True
    assert fields["sampling_blocked_reason"] == "FAIL_CLOSED_POSITIVE_SUPPLY_EFFORT_DETECTED_BEFORE_SAMPLING"


def test_negative_effort_allows_exhaust_control() -> None:
    runner, pace, _, _ = _runner()
    point = _co2_point(pressure=900.0)
    pace.output_state = 1
    pace.vent_status = 2
    pace.setpoint = 900.0
    pace.efforts = [-8.0]
    runner._activate_co2_sealed_no_vent_guard(point, reason="test", route_close_ts=time.time())
    runner._sealed_first_setpoint_tx_ts = time.time()
    runner._sealed_first_outp1_tx_ts = time.time()
    runner._mark_sealed_pressure_ready(result="in_limits")

    assert runner._co2_sealed_sampling_ready(point, point_tag="negative-effort") is True

    fields = _last_stage_fields(runner, "sealed_sampling_ready")
    assert fields["positive_supply_effort_detected"] is False
    assert fields["sample_blocked_by_positive_supply_effort"] is False
    assert fields["effort_before_sampling"] == pytest.approx(-8.0)


def test_overshoot_disabled_before_sealed_control() -> None:
    runner, pace, _, _ = _runner()
    point = _co2_point(pressure=900.0)
    runner._activate_co2_sealed_no_vent_guard(point, reason="test", route_close_ts=time.time())
    runner._record_preseal_pressure_control_ready_state(point, phase="co2", defer_live_check=False)

    assert runner._set_pressure_to_target(point) is True

    assert ("set_overshoot_allowed", False) in pace.calls
    fields = _last_stage_fields(runner, "sealed_pressure_slew_configured")
    assert fields["overshoot_allowed_set"] is False


def test_slew_linear_mode_set_before_sealed_control() -> None:
    runner, pace, _, _ = _runner(
        pressure_overrides={"slew_rate_hpa_per_s": 12.5}
    )
    point = _co2_point(pressure=900.0)
    runner._activate_co2_sealed_no_vent_guard(point, reason="test", route_close_ts=time.time())
    runner._record_preseal_pressure_control_ready_state(point, phase="co2", defer_live_check=False)

    assert runner._set_pressure_to_target(point) is True

    assert ("set_slew_mode_linear",) in pace.calls
    assert ("set_slew_rate", 12.5) in pace.calls
    fields = _last_stage_fields(runner, "sealed_pressure_slew_configured")
    assert fields["slew_mode_set"] == "LIN"
    assert fields["slew_rate_set_hpa_per_s"] == pytest.approx(12.5)


def test_effort_guard_runs_before_and_during_sampling() -> None:
    runner, pace, _, _ = _runner()
    point = _co2_point(pressure=900.0)
    pace.output_state = 1
    pace.vent_status = 2
    pace.setpoint = 900.0
    pace.efforts = [-1.0, -2.0]
    runner._activate_co2_sealed_no_vent_guard(point, reason="test", route_close_ts=time.time())
    runner._sealed_first_setpoint_tx_ts = time.time()
    runner._sealed_first_outp1_tx_ts = time.time()
    runner._mark_sealed_pressure_ready(result="in_limits")

    assert runner._co2_sealed_sampling_ready(point, point_tag="effort-before") is True
    ok, fields, reason = runner._sealed_positive_supply_effort_guard(
        point,
        stage="during_sampling",
        pressure_target_hpa=900.0,
        fail_on_unsupported=True,
        during_sampling=True,
    )

    assert ok is True
    assert reason == ""
    assert fields["effort_before_sampling"] == pytest.approx(-1.0)
    assert fields["effort_during_sampling_min"] == pytest.approx(-2.0)
    stages = _trace_stages(runner)
    assert "sealed_positive_supply_effort_before_sampling" in stages
    assert "sealed_positive_supply_effort_during_sampling" in stages


def test_full_status_evidence_deferred_until_after_outp1(monkeypatch) -> None:
    runner, pace, _, _ = _runner(gauge=FakeGauge([1112.0, 1112.0]))
    point = _co2_point(pressure=900.0)
    runner._apply_valve_states = MagicMock()
    monkeypatch.setattr("time.sleep", lambda _seconds: None)

    assert runner._pressurize_route_for_sealed_points(point, route="co2", sealed_control_refs=[point]) is True

    stages = _trace_stages(runner)
    outp_stage_index = stages.index("sealed_control_output_enable_command_sent")
    before_outp1 = stages[:outp_stage_index]
    assert "controlled_outp_pressure_rise_diagnostic" not in before_outp1
    assert not any("operator_window" in stage for stage in before_outp1)
    fields = _last_stage_fields(runner, "sealed_control_setpoint_command_sent")
    assert fields["full_evidence_deferred_until_after_outp1"] is True
    assert fields["operator_window_deferred_until_after_outp1"] is True
    assert fields["dewpoint_evidence_deferred_until_after_outp1"] is True
    assert fields["pressure_diagnostic_deferred_until_after_outp1"] is True
    assert pace.calls.index(("setpoint", 900.0)) < pace.calls.index(("enable_control_output",))


def test_pressure_high_below_hard_limit_does_not_block_fast_control() -> None:
    runner, pace, _, _ = _runner(
        pressure_overrides={
            "post_route_pressure_warning_hpa": 1200.0,
            "post_route_absolute_pressure_hard_limit_hpa": 1300.0,
        }
    )
    point = _co2_point(pressure=900.0)
    pace.read_pressure = MagicMock(return_value=1201.0)
    runner._activate_co2_sealed_no_vent_guard(point, reason="test", route_close_ts=time.time())

    ready, fields = runner._post_route_close_minimal_control_ready(
        point,
        phase="co2",
        route_close_ts=time.time(),
        route_close_pressure_hpa=1201.0,
        pressure_hard_limit_hit=False,
    )

    assert ready is True
    assert fields["post_route_pressure_warning_hit"] is True
    assert fields["pressure_high_but_control_should_start"] is True
    assert fields["pressure_high_blocked_outp1"] is False
    assert runner._start_fast_control_after_route_close(
        point,
        phase="co2",
        pressure_target_hpa=900.0,
        ready_fields=fields,
    ) is True
    assert pace.calls.index(("setpoint", 900.0)) < pace.calls.index(("enable_control_output",))


def test_post_route_pressure_hard_limit_blocks_outp1() -> None:
    runner, pace, _, _ = _runner(
        pressure_overrides={"post_route_absolute_pressure_hard_limit_hpa": 1200.0}
    )
    point = _co2_point(pressure=900.0)
    pace.read_pressure = MagicMock(return_value=1201.0)
    runner._activate_co2_sealed_no_vent_guard(point, reason="test", route_close_ts=time.time())

    ready, fields = runner._post_route_close_minimal_control_ready(
        point,
        phase="co2",
        route_close_ts=time.time(),
        route_close_pressure_hpa=1201.0,
        pressure_hard_limit_hit=False,
    )

    assert ready is False
    assert fields["pressure_high_blocked_outp1"] is True
    assert fields["post_route_absolute_pressure_hard_limit_hit"] is True
    assert fields["outp1_blocked_reason"] == "FAIL_CLOSED_POST_ROUTE_ABSOLUTE_PRESSURE_HARD_LIMIT"
    assert ("setpoint", 900.0) not in pace.calls
    assert ("enable_control_output",) not in pace.calls


def test_positive_supply_effort_guard_starts_after_outp1(monkeypatch) -> None:
    runner, pace, _, _ = _runner(gauge=FakeGauge([1112.0, 1112.0]))
    point = _co2_point(pressure=900.0)
    pace.efforts = [-1.0]
    runner._apply_valve_states = MagicMock()
    monkeypatch.setattr("time.sleep", lambda _seconds: None)

    assert runner._pressurize_route_for_sealed_points(point, route="co2", sealed_control_refs=[point]) is True

    assert pace.calls.index(("enable_control_output",)) < pace.calls.index(("query", ":SOUR:PRES:EFF?"))
    setpoint_fields = _last_stage_fields(runner, "sealed_control_setpoint_command_sent")
    assert setpoint_fields["effort_query_before_outp1_skipped_or_zero_reason"]
    outp_fields = _last_stage_fields(runner, "sealed_control_output_enable_command_sent")
    assert outp_fields["effort_guard_active_after_outp1"] is True


def test_no_parameter_write_added_in_fast_control_path(monkeypatch) -> None:
    runner, pace, _, _ = _runner(gauge=FakeGauge([1112.0, 1112.0]))
    point = _co2_point(pressure=900.0)
    runner._apply_valve_states = MagicMock()
    monkeypatch.setattr("time.sleep", lambda _seconds: None)

    assert runner._pressurize_route_for_sealed_points(point, route="co2", sealed_control_refs=[point]) is True
    assert runner._set_pressure_to_target(point) is True

    forbidden = {"id", "senco", "zero", "span", "calibration", "coefficient"}
    flattened = " ".join(str(item).lower() for call in pace.calls for item in call)
    assert not any(token in flattened for token in forbidden)


def test_no_forbidden_writes_added(monkeypatch) -> None:
    test_no_parameter_write_added_in_fast_control_path(monkeypatch)


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
    assert ("set_slew_mode_linear",) in pace.calls
    assert ("set_slew_rate", 15.0) in pace.calls
    assert ("set_overshoot_allowed", False) in pace.calls
    assert ("setpoint", 700.0) in pace.calls
    assert pace.calls.index(("set_slew_mode_linear",)) < pace.calls.index(("setpoint", 700.0))
    assert not any(call[0] in {"output", "vent", "enable_control_output"} for call in pace.calls)
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
