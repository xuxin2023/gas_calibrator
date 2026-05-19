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
    assert ("start_hold", 2.0) in pace.calls
    assert runner._pressure_atmosphere_hold_enabled is True


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


def test_sealed_control_does_not_fail_on_vent3_alone() -> None:
    runner, pace, _, _ = _runner()
    point = _co2_point(pressure=1100.0)
    pace.vent_status = 3
    pace.in_limits = [(1100.0, 1)]
    runner._activate_co2_sealed_no_vent_guard(point, reason="test")
    runner._begin_active_co2_sealed_sweep_context(point)

    assert runner._set_pressure_to_target_in_active_co2_sealed_sweep(point) is True

    stages = _trace_stages(runner)
    assert "sealed_vent_status_watchlist" in stages
    assert "sealed_sweep_live_check_fail" not in stages
    assert ("setpoint", 1100.0) in pace.calls


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
    assert "controlled_exit_atmosphere_verify" in stages
    assert "operator_window_check_begin" in stages
    assert "operator_window_check_result" in stages
    assert "controlled_exit_atmosphere_pass" in stages


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
    assert ("start_hold", 2.0) in pace.calls

    assert runner._stop_pressure_controller_atmosphere_hold(pace, reason="unit pre-VENT0") is True
    runner._wait_pre_vent0_raw_tap_quiet_gap(pace)

    assert ("start_hold", 2.0) in pace.calls
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
    assert runner._stop_pressure_controller_atmosphere_hold(pace, reason="unit pre-VENT0") is True

    runner._wait_pre_vent0_raw_tap_quiet_gap(pace)
    vent0_ts = time.time()
    runner._mark_pre_vent0_command_sent(vent0_ts)
    pace.vent(False)

    fields = runner._last_pre_vent0_quiet_gap_fields
    assert fields["pre_vent0_vent_status_probe_enabled"] is True
    assert "VENT? probe timeout" in fields["pre_vent0_vent_status_probe_error"]
    assert ("vent", False) in pace.calls


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


def test_preseal_buildup_does_not_fail_on_vent3_alone(monkeypatch) -> None:
    runner, pace, _, _ = _runner(
        pace=Vent3PersistPace(),
        gauge=FakeGauge([1013.0, 1018.0, 1022.0, 1024.0, 1026.0, 1028.0, 1030.0]),
        logger=PostVent0RawTapLogger(vent1_count=0),
    )
    runner._apply_valve_states = MagicMock()
    monkeypatch.setattr("time.sleep", lambda _seconds: None)

    assert runner._pressurize_route_for_sealed_points(_co2_point(), route="co2", sealed_control_refs=[_co2_point()]) is True

    stages = _trace_stages(runner)
    assert ("vent", False) in pace.calls
    assert ("exit_atmosphere_mode", 3.0, 0.2) not in pace.calls
    assert "route_valves_closed_after_vent0" in stages
    assert "controlled_exit_atmosphere_pass" in stages
    assert "operator_window_check_result" in stages
    assert "controlled_exit_atmosphere_fail" not in stages
    runner._apply_valve_states.assert_called()
    fields = _last_stage_fields(runner, "route_valves_closed_after_vent0")
    assert fields["vent_status_watchlist"] is True
    assert fields["vent_status_terminal"] is False
    assert _last_stage_fields(runner, "controlled_outp_vent0_fixed_wait_before_seal")[
        "post_vent0_new_vent1_count"
    ] == 0


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

    assert events.index("close_valves") < events.index("slow_evidence")


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
    assert stages.index("route_valves_closed_after_vent0") < stages.index("pace_status_evidence:after_route_close")


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

    runner._apply_valve_states.assert_called_once_with([])
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
    ) is True

    assert ("vent", False) in pace.calls
    assert ("exit_atmosphere_mode", 3.0, 0.2) not in pace.calls
    runner._apply_valve_states.assert_called_once_with([])
    fixed_fields = _last_stage_fields(runner, "controlled_outp_vent0_fixed_wait_before_seal")
    assert fixed_fields["post_vent0_new_vent1_count"] == 0
    assert fixed_fields["post_vent0_vent3_watchlist_only"] is True
    assert fixed_fields["post_vent0_route_valves_still_open"] is True


def test_post_seal_vent_abort_clear_sent_when_vent3_clears(monkeypatch) -> None:
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
    ) is True

    fields = _last_stage_fields(runner, "post_seal_vent_abort_clear")
    assert fields["post_seal_vent_abort_clear_sent"] is True
    assert fields["post_seal_vent_status_before_clear"] == 3
    assert fields["post_seal_vent_status_after_clear"] == 0
    assert fields["post_seal_vent_window_cleared"] is True
    assert fields["sealed_control_ready_decision"] == "ready"
    assert fields["sealed_control_ready_blocks_outp1"] is False
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
    ) is True

    fields = _last_stage_fields(runner, "post_seal_vent_abort_clear")
    assert fields["post_seal_vent_abort_clear_sent"] is True
    assert fields["post_seal_vent_window_persisted"] is True
    assert fields["sealed_control_ready_vent_clear_attempt_count"] >= 1
    assert fields["sealed_control_ready_blocks_outp1"] is True
    assert fields["outp1_blocked_reason"] == "FAIL_CLOSED_PRESSURE_CONTROLLER_VENT_WINDOW_LATCHED_BEFORE_CONTROL"

    assert runner._set_pressure_to_target(_co2_point()) is False
    failed = _last_stage_fields(runner, "control_ready_failed")
    assert failed["sealed_control_ready_decision"] == (
        "FAIL_CLOSED_PRESSURE_CONTROLLER_VENT_WINDOW_LATCHED_BEFORE_CONTROL"
    )
    assert failed["outp1_blocked_reason"] == "FAIL_CLOSED_PRESSURE_CONTROLLER_VENT_WINDOW_LATCHED_BEFORE_CONTROL"
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
    ) is True
    assert runner._set_pressure_to_target(_co2_point()) is False

    fields = _last_stage_fields(runner, "control_ready_failed")
    assert fields["outp1_blocked_reason"] == "FAIL_CLOSED_PRESSURE_CONTROLLER_VENT_WINDOW_LATCHED_BEFORE_CONTROL"
    assert ("enable_control_output",) not in pace.calls
    assert ("setpoint", 1100.0) not in pace.calls
    assert ("vent", True) not in pace.calls


def test_control_ready_allows_outp1_when_vent_clear_returns_zero(monkeypatch) -> None:
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
    ) is True
    assert runner._set_pressure_to_target(_co2_point()) is True

    fields = _last_stage_fields(runner, "post_seal_vent_abort_clear")
    assert fields["post_seal_vent_status_after_clear"] == 0
    assert fields["sealed_control_ready_decision"] == "ready"
    assert ("enable_control_output",) in pace.calls
    assert ("setpoint", 1100.0) in pace.calls
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
    ) is True

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
    ) is True

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
    ) is True
    assert runner._set_pressure_to_target(_co2_point()) is False

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
    assert "controlled_exit_atmosphere_pass" in stages
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
    fields = _last_stage_fields(runner, "operator_window_check_result")
    assert fields["operator_window_gate_effect"] == "warning_only"
    assert fields["operator_window_terminal"] is False


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
    assert "controlled_exit_atmosphere_pass" in stages
    assert "operator_window_check_result" in stages
    assert stages.index("route_valves_closed_after_vent0") < stages.index("operator_window_check_result")
    assert "route_sealed" in stages
    fields = _last_stage_fields(runner, "operator_window_check_result")
    assert fields["operator_window_note"] == "stdin_non_interactive"
    assert fields["operator_window_gate_effect"] == "warning_only"
    assert fields["operator_window_terminal"] is False
    assert fields["route_close_blocked_by_operator_window"] is False


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
    assert stages.index("controlled_exit_atmosphere_driver_exit_done") < stages.index("operator_window_prompt_printed")
    assert stages.index("controlled_outp_vent0_fixed_wait_before_seal") < stages.index("operator_window_prompt_printed")
    assert stages.index("route_valves_closed_after_vent0") < stages.index("operator_window_prompt_printed")
    assert stages.index("route_valves_closed_after_vent0") < stages.index("operator_window_check_result")
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
    assert fields["vent_status_after_vent0"] == 2
    assert fields["vent_status_after_vent0_classification"] == "vent_completed_ready"
    assert fields["outp_status_after_vent0"] == 0
    assert fields["isol_status_after_vent0"] == 1
    assert fields["syst_err_after_vent0"] == "0,No error"
    assert fields["pace_pressure_after_vent0_hpa"] == 1013.0
    assert fields["com22_pressure_after_vent0_hpa"] == 1013.0
    assert fields["route_valves_still_open_during_wait"] is True
    assert fields["fixed_wait_elapsed_s"] is not None
    assert fields["route_valve_close_ts"]
    assert "actual_open_valves_after_close" in fields


def test_original_order_preserved(monkeypatch) -> None:
    runner, _, _, _ = _runner(gauge=FakeGauge([1013.0, 1013.0, 1013.0]))
    point = _co2_point()
    runner._apply_valve_states = MagicMock()
    monkeypatch.setattr("time.sleep", lambda _seconds: None)

    assert runner._pressurize_route_for_sealed_points(point, route="co2", sealed_control_refs=[point]) is True

    stages = _trace_stages(runner)
    assert stages.index("controlled_outp_vent0_fixed_wait_before_seal") < stages.index(
        "route_valves_closed_after_vent0"
    )
    assert stages.index("route_valves_closed_after_vent0") < stages.index("controlled_exit_atmosphere_pass")
    assert stages.index("route_valves_closed_after_vent0") < stages.index("operator_window_check_result")
    assert stages.index("route_valves_closed_after_vent0") < stages.index("route_sealed")


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
    assert stages.index("route_valves_closed_after_vent0") < stages.index("controlled_exit_atmosphere_pass")
    assert stages.index("route_valves_closed_after_vent0") < stages.index("operator_window_check_begin")
    assert stages.index("operator_window_check_result") < stages.index("route_sealed")
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
    assert "controlled_outp_pressure_rise_diagnostic" in _trace_stages(runner)


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
    pace.vent_status = 0
    assert runner._set_pressure_to_target(point) is True

    assert pace.calls.index(("vent", False)) < pace.calls.index(("enable_control_output",))
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
