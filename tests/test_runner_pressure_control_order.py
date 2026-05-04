from pathlib import Path

import pytest
import csv
import time

from gas_calibrator.data.points import CalibrationPoint
from gas_calibrator.logging_utils import RunLogger
from gas_calibrator.workflow.runner import CalibrationRunner


class _FakePace:
    def __init__(self):
        self.calls = []
        self._in_limits_sequence = [(1000.0, 1)]
        self.output_state = 0
        self.isolation_state = 1
        self.vent_status = 0
        self.vent_after_valve_open = False
        self.popup_ack_enabled = True
        self.current_pressure_hpa = 1012.4
        self.barometric_pressure_hpa = 1012.0
        self.system_error_text = '0,"No error"'
        self._pressure_reads = []

    def enter_atmosphere_mode_with_open_vent_valve(self, timeout_s=0.0, popup_ack_enabled=None):
        self.calls.append(("vent_on_open_valve", float(timeout_s), popup_ack_enabled))
        self.output_state = 0
        self.isolation_state = 1
        self.vent_after_valve_open = True
        self.vent_status = 0
        if popup_ack_enabled is not None:
            self.popup_ack_enabled = bool(popup_ack_enabled)

    def enter_atmosphere_mode(self, timeout_s=0.0, **kwargs):
        self.calls.append(
            ("vent_on", float(timeout_s), bool(kwargs.get("hold_open", False)), float(kwargs.get("hold_interval_s", 0.0)))
        )
        self.output_state = 0
        self.isolation_state = 1
        self.vent_after_valve_open = False
        self.vent_status = 0

    def exit_atmosphere_mode(self, timeout_s=0.0):
        self.calls.append(("vent_off", float(timeout_s)))
        self.output_state = 0
        self.isolation_state = 1
        self.vent_status = 0

    def set_vent_after_valve_open(self, is_open):
        self.calls.append(("vent_after_valve", bool(is_open)))
        self.vent_after_valve_open = bool(is_open)

    def get_vent_after_valve_open(self):
        return self.vent_after_valve_open

    def set_vent_popup_ack_enabled(self, enabled):
        self.calls.append(("popup_ack", bool(enabled)))
        self.popup_ack_enabled = bool(enabled)

    def get_vent_popup_ack_enabled(self):
        return self.popup_ack_enabled

    def set_setpoint(self, value):
        self.calls.append(("setpoint", float(value)))

    def enable_control_output(self):
        self.calls.append(("output_on",))
        self.output_state = 1
        self.vent_status = 0

    def set_output(self, on):
        self.calls.append(("output", bool(on)))
        self.output_state = 1 if on else 0

    def vent(self, on=True):
        self.calls.append(("vent", bool(on)))
        self.vent_status = 1 if on else 0

    def set_isolation_open(self, is_open):
        self.calls.append(("isol", bool(is_open)))
        self.isolation_state = 1 if is_open else 0

    def get_in_limits(self):
        if self._in_limits_sequence:
            return self._in_limits_sequence.pop(0)
        return 1000.0, 1

    def get_output_state(self):
        return self.output_state

    def get_isolation_state(self):
        return self.isolation_state

    def get_vent_status(self):
        return self.vent_status

    def read_pressure(self):
        if self._pressure_reads:
            self.current_pressure_hpa = float(self._pressure_reads.pop(0))
        return float(self.current_pressure_hpa)

    def get_barometric_pressure(self):
        return float(self.barometric_pressure_hpa)

    def get_system_error(self):
        return self.system_error_text

    def has_legacy_vent_state_3_compatibility(self):
        return False


class _FakePaceCompletedVentLatch(_FakePace):
    def __init__(self):
        super().__init__()
        self.vent_status = 2

    def clear_completed_vent_latch_if_present(self, **kwargs):
        self.calls.append(("clear_latch", dict(kwargs)))
        self.vent_status = 0
        return {
            "clear_attempted": True,
            "blocked": False,
            "cleared": True,
            "before_status": 2,
            "after_status": 0,
        }


class _FakePaceFallback:
    def __init__(self):
        self.calls = []
        self.output_state = 0
        self.isolation_state = 1
        self.vent_status = 0

    def set_output(self, on):
        self.calls.append(("output", bool(on)))
        self.output_state = 1 if on else 0

    def vent(self, on=True):
        self.calls.append(("vent", bool(on)))
        self.vent_status = 1 if on else 0

    def set_isolation_open(self, is_open):
        self.calls.append(("isol", bool(is_open)))
        self.isolation_state = 1 if is_open else 0

    def set_setpoint(self, value):
        self.calls.append(("setpoint", float(value)))

    def get_in_limits(self):
        return 1000.0, 1

    def get_output_state(self):
        return self.output_state

    def get_isolation_state(self):
        return self.isolation_state

    def get_vent_status(self):
        return self.vent_status


class _FakePaceSoftRecover(_FakePace):
    def __init__(self):
        super().__init__()
        self.phase = "first"
        self.first_reads = 0
        self.second_reads = 0
        self._single_cycle_active = False
        self._single_cycle_clear_requested = False
        self._single_cycle_query_count = 0

    def set_setpoint(self, value):
        self.calls.append(("setpoint", float(value), self.phase))

    def enable_control_output(self):
        self.calls.append(("output_on", self.phase))
        self.output_state = 1
        self.vent_status = 0

    def stop_atmosphere_hold(self):
        self.calls.append(("stop_hold",))

    def set_output(self, on):
        self.calls.append(("output", bool(on)))
        self.output_state = 1 if on else 0

    def vent(self, on=True):
        self.calls.append(("vent", bool(on)))
        if on:
            self._single_cycle_active = True
            self._single_cycle_clear_requested = False
            self._single_cycle_query_count = 0
            self.vent_status = 1
            return
        self._single_cycle_clear_requested = True
        self.vent_status = 0

    def close(self):
        self.calls.append(("close",))

    def open(self):
        self.calls.append(("open",))

    def set_units_hpa(self):
        self.calls.append(("units_hpa",))

    def set_in_limits(self, pct, time_s):
        self.calls.append(("set_in_limits", float(pct), float(time_s)))

    def get_vent_status(self):
        if not self._single_cycle_active:
            return self.vent_status
        self._single_cycle_query_count += 1
        if self._single_cycle_clear_requested:
            self.vent_status = 0
            self._single_cycle_active = False
            return 0
        if self._single_cycle_query_count <= 2:
            self.vent_status = 1
            return 1
        self.vent_status = 0
        self._single_cycle_active = False
        return 0

    def get_in_limits(self):
        if self.phase == "first":
            self.first_reads += 1
            return 550.0, 0
        self.second_reads += 1
        if self.second_reads < 2:
            return 549.9, 0
        return 550.0, 1


class _FakePaceVentOffFailure(_FakePace):
    def exit_atmosphere_mode(self, timeout_s=0.0):
        self.calls.append(("vent_off", float(timeout_s)))
        raise RuntimeError("EXIT_ATMOSPHERE_FAILED")


class _FakePaceLingeringHold(_FakePace):
    def stop_atmosphere_hold(self):
        self.calls.append(("stop_hold",))
        return False

    def is_atmosphere_hold_active(self):
        return True


class _FakePaceNotReady(_FakePace):
    def get_vent_status(self):
        return 1


class _FakePaceTrappedPressureReady(_FakePace):
    def get_vent_status(self):
        if self.output_state == 1:
            return 0
        return 3


class _FakePaceLegacyVentTrapped(_FakePace):
    VENT_STATUS_TRAPPED_PRESSURE = 3

    def get_vent_status(self):
        return 3

    def has_legacy_vent_state_3_compatibility(self):
        return True

    def vent_status_allows_control(self, status):
        return int(status) == 0

    def enable_control_output(self):
        self.calls.append(("output_on",))
        self.output_state = 1
        self.vent_status = 3


class _FakePaceOldK0472PresealWatchlist(_FakePace):
    VENT_STATUS_TRAPPED_PRESSURE = 3

    def __init__(self):
        super().__init__()
        self.vent_status = 1

    def detect_profile(self):
        return "OLD_PACE5000"

    def get_device_identity(self):
        return "GE DRUCK PACE5000 USER INTERFACE"

    def get_instrument_version(self):
        return "02.00.07"

    def has_legacy_vent_status_model(self):
        return True

    def has_legacy_vent_state_3_compatibility(self):
        return True

    def vent_status_allows_control(self, status):
        return int(status) == 0

    def exit_atmosphere_mode(self, timeout_s=0.0):
        self.calls.append(("vent_off", float(timeout_s)))
        self.output_state = 0
        self.isolation_state = 1
        self.vent_status = 3

    def vent(self, on=True):
        self.calls.append(("vent", bool(on)))
        self.output_state = 0
        self.isolation_state = 1
        self.vent_status = 1 if on else 3


class _FakePaceOldK0472OutputEnableWatchlist(_FakePaceOldK0472PresealWatchlist):
    def set_output_mode_active(self):
        self.calls.append(("mode_active",))

    def enable_control_output(self):
        self.calls.append(("enable_control_output_unexpected",))
        raise AssertionError("narrow old K0472 output path should use set_output")


class _FakePaceOutputOnTrappedThenReady(_FakePace):
    def __init__(self):
        super().__init__()
        self.phase = "first"

    def enable_control_output(self):
        self.calls.append(("output_on", self.phase))
        if self.phase == "first":
            # Simulate first attempt being ignored by controller when trapped.
            self.output_state = 0
            self.vent_status = 3
            self.phase = "second"
            return
        self.output_state = 1
        self.vent_status = 0


class _FakePaceLegacyOutputOnTrappedThenReady(_FakePace):
    VENT_STATUS_TRAPPED_PRESSURE = 3

    def __init__(self):
        super().__init__()
        self.phase = "first"
        self.vent_status = 3

    def get_vent_status(self):
        return self.vent_status

    def has_legacy_vent_state_3_compatibility(self):
        return True

    def vent_status_allows_control(self, status):
        return int(status) == 0

    def enable_control_output(self):
        self.calls.append(("output_on", self.phase))
        if self.phase == "first":
            self.output_state = 0
            self.vent_status = 3
            self.phase = "second"
            return
        self.output_state = 1
        self.vent_status = 3


class _FakePaceOpenValveUnsupported(_FakePace):
    def enter_atmosphere_mode_with_open_vent_valve(self, timeout_s=0.0, popup_ack_enabled=None):
        raise RuntimeError("VENT_AFTER_VALVE_UNSUPPORTED")


class _FakePaceSingleCycleVent(_FakePace):
    def __init__(self):
        super().__init__()
        self._single_cycle_active = False
        self._single_cycle_clear_requested = False
        self._single_cycle_query_count = 0
        self._terminal_vent_status = 0

    def vent(self, on=True):
        self.calls.append(("vent", bool(on)))
        if on:
            self._single_cycle_active = True
            self._single_cycle_clear_requested = False
            self._single_cycle_query_count = 0
            self.vent_status = 1
            return
        self._single_cycle_clear_requested = True
        self.vent_status = 0

    def get_vent_status(self):
        if not self._single_cycle_active:
            return self.vent_status
        self._single_cycle_query_count += 1
        if self._single_cycle_clear_requested:
            self.vent_status = 0
            self._single_cycle_active = False
            return 0
        if self._single_cycle_query_count <= 2:
            self.vent_status = 1
            return 1
        self.vent_status = int(self._terminal_vent_status)
        self._single_cycle_active = False
        return self.vent_status


class _FakePaceSingleCycleVentBlocked(_FakePaceSingleCycleVent):
    def __init__(self):
        super().__init__()
        self._terminal_vent_status = 2

    def has_legacy_vent_state_3_compatibility(self):
        return False

    def get_device_identity(self):
        return '*IDN GE Druck,Pace5000 User Interface,3213201,02.00.07'

    def get_instrument_version(self):
        return ':INST:VERS "02.00.07"'


class _FakePaceSingleCycleVentClearsToTrapped(_FakePaceSingleCycleVent):
    VENT_STATUS_TRAPPED_PRESSURE = 3

    def get_vent_status(self):
        if not self._single_cycle_active:
            return self.vent_status
        self._single_cycle_query_count += 1
        if self._single_cycle_clear_requested:
            self.vent_status = 3
            self._single_cycle_active = False
            return 3
        if self._single_cycle_query_count <= 2:
            self.vent_status = 1
            return 1
        self.vent_status = 2
        return 2

    def has_legacy_vent_state_3_compatibility(self):
        return True

    def vent_status_allows_control(self, status):
        return int(status) in {0, 3}


class _FakePaceOutputOnVentWindow(_FakePace):
    def enable_control_output(self):
        self.calls.append(("output_on",))
        self.output_state = 1
        self.vent_status = 1


class _FakePaceOutputOnNeverReady(_FakePace):
    def enable_control_output(self):
        self.calls.append(("output_on",))
        self.output_state = 0
        self.vent_status = 3


class _FakePaceLegacyVentCompleted(_FakePace):
    def exit_atmosphere_mode(self, timeout_s=0.0):
        self.calls.append(("vent_off", float(timeout_s)))
        self.output_state = 0
        self.isolation_state = 1
        self.vent_status = 2

    def enable_control_output(self):
        self.calls.append(("output_on",))
        self.output_state = 1
        self.vent_status = 2

    def vent_status_allows_control(self, status):
        return int(status) == 0


class _FakePaceCompletedVentLatchClears(_FakePace):
    def __init__(self):
        super().__init__()
        self.vent_status = 2

    def clear_status(self):
        self.calls.append(("clear_status",))

    def drain_system_errors(self):
        self.calls.append(("drain_system_errors",))
        return ['-222,"Data out of range"']

    def clear_completed_vent_latch_if_present(self, timeout_s=5.0, poll_s=0.25):
        self.calls.append(("clear_completed_vent_latch_if_present", float(timeout_s), float(poll_s)))
        before = self.vent_status
        self.vent_status = 0
        return {
            "before_status": before,
            "clear_attempted": True,
            "after_status": self.vent_status,
            "cleared": True,
            "command": ":SOUR:PRES:LEV:IMM:AMPL:VENT 0",
        }


class _FakePaceCompletedVentLatchBlocked(_FakePace):
    def __init__(self):
        super().__init__()
        self.vent_status = 2

    def clear_status(self):
        self.calls.append(("clear_status",))

    def drain_system_errors(self):
        self.calls.append(("drain_system_errors",))
        return []

    def clear_completed_vent_latch_if_present(self, timeout_s=5.0, poll_s=0.25):
        self.calls.append(("clear_completed_vent_latch_if_present", float(timeout_s), float(poll_s)))
        return {
            "before_status": 2,
            "clear_attempted": False,
            "after_status": 2,
            "cleared": False,
            "command": "",
            "skipped": True,
            "blocked": True,
            "reason": "legacy_completed_latch_auto_clear_blocked",
            "manual_intervention_required": True,
            "vent_command_sent": False,
        }

    def get_vent_status(self):
        return 2

    def has_legacy_vent_state_3_compatibility(self):
        return True

    def vent_status_allows_control(self, status):
        return False


class _FakePaceCompletedVentLatchBitOnly(_FakePace):
    def __init__(self):
        super().__init__()
        self.vent_status = 3

    def clear_status(self):
        self.calls.append(("clear_status",))

    def drain_system_errors(self):
        self.calls.append(("drain_system_errors",))
        return []

    def has_legacy_vent_state_3_compatibility(self):
        return True

    def vent_status_allows_control(self, status):
        return int(status) in {0, 3}


class _FakePaceSlowExitForPreseal:
    def __init__(self):
        self.calls = []

    def set_output(self, on):
        self.calls.append(("output", bool(on)))

    def vent(self, on=True):
        self.calls.append(("vent", bool(on)))

    def set_isolation_open(self, is_open):
        self.calls.append(("isol", bool(is_open)))

    def exit_atmosphere_mode(self, timeout_s=0.0):
        self.calls.append(("exit_atmosphere_mode", float(timeout_s)))
        raise AssertionError("fast preseal vent-off should not use exit_atmosphere_mode")


class _FakePaceLegacyBaselineReuse(_FakePace):
    def __init__(self):
        super().__init__()
        self.output_state = 0
        self.isolation_state = 1
        self.vent_status = 2

    def detect_profile(self):
        return "OLD_PACE5000"

    def vent_status_allows_control(self, status):
        return int(status) in {0, 2}


class _FakePaceOldCompletedBaselineRequiresFreshVent(_FakePaceSingleCycleVent):
    def __init__(self):
        super().__init__()
        self.output_state = 0
        self.isolation_state = 1
        self.vent_status = 2
        self._terminal_vent_status = 2

    def detect_profile(self):
        return "OLD_PACE5000"

    def has_legacy_vent_status_model(self):
        return True

    def get_device_identity(self):
        return '*IDN GE Druck,Pace5000 User Interface,3213201,02.00.07'

    def get_instrument_version(self):
        return ':INST:VERS "02.00.07"'


class _FakePaceOldCompletedBaselineLegacyCompatOnly(_FakePaceOldCompletedBaselineRequiresFreshVent):
    def detect_profile(self):
        return ""


class _FakePaceVentAfterValveGetterFailure(_FakePace):
    def get_vent_after_valve_open(self):
        raise RuntimeError("NO_RESPONSE")


class _FakeGaugeSequence:
    def __init__(self, reads=None):
        values = list(reads or [1012.0])
        self._reads = [float(value) for value in values]
        self._last = float(self._reads[-1]) if self._reads else 1012.0

    def read_pressure(self):
        if self._reads:
            self._last = float(self._reads.pop(0))
        return float(self._last)


class _FakeRelay:
    def __init__(self):
        self.calls = []
        self.states = {}

    def set_valve(self, channel, state):
        self.calls.append(("set_valve", int(channel), bool(state)))
        self.states[int(channel)] = bool(state)

    def set_valves_bulk(self, updates):
        values = [(int(channel), bool(state)) for channel, state in list(updates or [])]
        self.calls.append(("set_valves_bulk", tuple(values)))
        for channel, state in values:
            self.states[int(channel)] = bool(state)


def _load_pressure_trace_rows(logger: RunLogger):
    path = logger.run_dir / "pressure_transition_trace.csv"
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def _fresh_vent_cfg() -> dict:
    return {
        "workflow": {
            "pressure": {
                "vent_time_s": 0,
                "vent_transition_timeout_s": 12,
                "atmosphere_gate_monitor_s": 0.0,
                "atmosphere_gate_poll_s": 0.0,
                "atmosphere_gate_min_samples": 1,
            }
        }
    }


def _route_open_guard_cfg() -> dict:
    return {
        "valves": {
            "co2_path": 7,
            "co2_path_group2": 16,
            "gas_main": 11,
            "h2o_path": 8,
            "hold": 9,
            "flow_switch": 10,
            "co2_map": {
                "600": 4,
            },
            "co2_map_group2": {
                "500": 24,
            },
        },
        "workflow": {
            "pressure": {
                "vent_time_s": 0,
                "vent_transition_timeout_s": 12,
                "atmosphere_gate_monitor_s": 0.0,
                "atmosphere_gate_poll_s": 0.0,
                "atmosphere_gate_min_samples": 1,
                "atmosphere_gate_pressure_rising_grace_delta_hpa": 2.0,
                "atmosphere_gate_pressure_rising_fail_min_samples": 3,
                "atmosphere_gate_pressure_rising_fail_min_rise_hpa": 2.0,
                "atmosphere_gate_hard_fail_delta_hpa": 30.0,
                "route_open_guard_enabled": True,
                "route_open_guard_monitor_s": 0.02,
                "route_open_guard_poll_s": 0.01,
                "route_open_guard_pressure_tolerance_hpa": 30.0,
                "route_open_guard_pressure_rising_slope_max_hpa_s": 0.01,
                "route_open_guard_pressure_rising_min_delta_hpa": 0.5,
                "route_open_guard_pressure_rising_grace_delta_hpa": 2.0,
                "route_open_guard_pressure_rising_fail_min_samples": 3,
                "route_open_guard_pressure_rising_fail_min_rise_hpa": 2.0,
                "route_open_guard_recovery_trigger_delta_hpa": 50.0,
                "route_open_guard_recovery_tolerance_hpa": 30.0,
                "route_open_guard_vent_recovery_attempts": 1,
                "flush_guard_pressure_rising_grace_delta_hpa": 2.0,
                "flush_guard_pressure_rising_fail_min_samples": 3,
                "flush_guard_pressure_rising_fail_min_rise_hpa": 2.0,
            },
            "stability": {
                "gas_route_dewpoint_gate_enabled": False,
                "co2_route": {
                    "preseal_soak_s": 0.02,
                    "first_point_preseal_soak_s": 0.02,
                },
            },
        }
    }


def _co2_test_point(*, ppm: float, group: str | None = None, index: int = 1) -> CalibrationPoint:
    return CalibrationPoint(
        index=index,
        temp_chamber_c=20.0,
        co2_ppm=float(ppm),
        hgen_temp_c=None,
        hgen_rh_pct=None,
        target_pressure_hpa=1000.0,
        dewpoint_c=None,
        h2o_mmol=None,
        raw_h2o=None,
        co2_group=group,
    )


def _h2o_test_point(*, index: int = 1) -> CalibrationPoint:
    return CalibrationPoint(
        index=index,
        temp_chamber_c=20.0,
        co2_ppm=None,
        hgen_temp_c=20.0,
        hgen_rh_pct=30.0,
        target_pressure_hpa=1000.0,
        dewpoint_c=None,
        h2o_mmol=None,
        raw_h2o=None,
    )


def test_set_pressure_controller_vent_on_uses_fresh_vent_guarded_gate_by_default(tmp_path: Path) -> None:
    cfg = _fresh_vent_cfg()
    logger = RunLogger(tmp_path)
    pace = _FakePaceSingleCycleVent()
    runner = CalibrationRunner(cfg, {"pace": pace}, logger, lambda *_: None, lambda *_: None)

    runner._set_pressure_controller_vent(True, reason="test hold")
    logger.close()

    assert pace.calls == [
        ("output", False),
        ("isol", True),
        ("vent", True),
    ]
    trace_rows = _load_pressure_trace_rows(logger)
    assert any(
        row["trace_stage"] == "atmosphere_hold_strategy_selected"
        and row["atmosphere_hold_strategy"] == "single_cycle_query_clear"
        for row in trace_rows
    )
    assert any(row["trace_stage"] == "atmosphere_vent_in_progress" for row in trace_rows)
    completed_row = next(row for row in trace_rows if row["trace_stage"] == "atmosphere_vent_completed")
    assert completed_row["pace_vent_status_query"].strip() == "0"
    enter_row = next(row for row in trace_rows if row["trace_stage"] == "atmosphere_enter_verified")
    assert enter_row["fresh_vent_command_sent"].strip().lower() == "true"
    assert enter_row["vent_status_sequence"].strip() == "1,0"
    assert enter_row["atmosphere_ready"].strip().lower() == "true"
    assert enter_row["pace_syst_err_query"].strip().startswith("0")


def test_set_pressure_controller_vent_on_normalizes_legacy_open_valve_strategy_to_fresh_vent_guarded_gate(
    tmp_path: Path,
) -> None:
    cfg = _fresh_vent_cfg()
    cfg["workflow"]["pressure"]["atmosphere_hold_strategy"] = "vent_valve_open_after_vent"
    cfg["workflow"]["pressure"]["vent_after_valve_open"] = True
    logger = RunLogger(tmp_path)
    pace = _FakePaceSingleCycleVent()
    runner = CalibrationRunner(cfg, {"pace": pace}, logger, lambda *_: None, lambda *_: None)

    runner._set_pressure_controller_vent(True, reason="test hold")
    logger.close()

    assert pace.calls == [
        ("output", False),
        ("isol", True),
        ("vent", True),
    ]
    trace_rows = _load_pressure_trace_rows(logger)
    assert any(
        row["trace_stage"] == "atmosphere_hold_strategy_selected"
        and row["atmosphere_hold_strategy"] == "single_cycle_query_clear"
        for row in trace_rows
    )
    assert not any(row["trace_stage"] == "atmosphere_vent_clear_command" for row in trace_rows)
    assert any(row["trace_stage"] == "atmosphere_enter_verified" for row in trace_rows)


def test_set_pressure_controller_vent_on_does_not_depend_on_open_valve_extension_support(tmp_path: Path) -> None:
    cfg = _fresh_vent_cfg()
    cfg["workflow"]["pressure"]["atmosphere_hold_strategy"] = "vent_valve_open_after_vent"
    cfg["workflow"]["pressure"]["vent_after_valve_open"] = True
    messages = []
    logger = RunLogger(tmp_path)
    pace = _FakePaceSingleCycleVent()
    runner = CalibrationRunner(cfg, {"pace": pace}, logger, lambda message: messages.append(str(message)), lambda *_: None)

    runner._set_pressure_controller_vent(True, reason="test fallback")
    logger.close()

    assert pace.calls == [
        ("output", False),
        ("isol", True),
        ("vent", True),
    ]
    assert not any("fallback -> legacy hold thread" in message for message in messages)
    trace_rows = _load_pressure_trace_rows(logger)
    assert not any(row["trace_stage"] == "atmosphere_hold_legacy_fallback" for row in trace_rows)
    assert any(row["trace_stage"] == "atmosphere_enter_verified" for row in trace_rows)


def test_set_pressure_controller_vent_on_rejects_unscoped_completed_latch_status(
    tmp_path: Path,
) -> None:
    cfg = _fresh_vent_cfg()
    logger = RunLogger(tmp_path)
    pace = _FakePaceSingleCycleVentBlocked()
    runner = CalibrationRunner(cfg, {"pace": pace}, logger, lambda *_: None, lambda *_: None)

    with pytest.raises(RuntimeError, match="VENT_STATUS_2"):
        runner._set_pressure_controller_vent(True, reason="legacy completed latch")
    logger.close()

    assert pace.calls == [
        ("output", False),
        ("isol", True),
        ("vent", True),
    ]
    trace_rows = _load_pressure_trace_rows(logger)
    assert not any(row["trace_stage"] == "atmosphere_enter_verified" for row in trace_rows)


def test_set_pressure_controller_vent_on_reissues_fresh_vent_when_old_baseline_only_has_completed_latch(
    tmp_path: Path,
) -> None:
    cfg = _fresh_vent_cfg()
    logger = RunLogger(tmp_path)
    pace = _FakePaceOldCompletedBaselineRequiresFreshVent()
    runner = CalibrationRunner(cfg, {"pace": pace}, logger, lambda *_: None, lambda *_: None)

    assert runner._set_pressure_controller_vent(True, reason="startup preflight reset") is True
    logger.close()

    assert pace.calls == [
        ("output", False),
        ("isol", True),
        ("vent", True),
    ]
    trace_rows = _load_pressure_trace_rows(logger)
    completed_row = next(row for row in trace_rows if row["trace_stage"] == "atmosphere_vent_completed")
    assert completed_row["pace_vent_status_query"].strip() == "2"
    assert completed_row["fresh_vent_command_sent"].strip().lower() == "true"
    enter_row = next(row for row in trace_rows if row["trace_stage"] == "atmosphere_enter_verified")
    assert any(
        token in str(enter_row["note"])
        for token in ("pressure-confirmed AtmosphereGate complete", "ambient_hpa=", "pressure_hpa=")
    )
    assert enter_row["vent_status_sequence"].strip() == "1,2"
    assert enter_row["atmosphere_ready"].strip().lower() == "true"


def test_cleanup_co2_route_rejects_unscoped_completed_latch_status(tmp_path: Path) -> None:
    cfg = _fresh_vent_cfg()
    logger = RunLogger(tmp_path)
    pace = _FakePaceSingleCycleVentBlocked()
    runner = CalibrationRunner(cfg, {"pace": pace}, logger, lambda *_: None, lambda *_: None)

    with pytest.raises(RuntimeError, match="VENT_STATUS_2"):
        runner._cleanup_co2_route(reason="after startup pressure precheck")
    logger.close()

    assert pace.calls == [
        ("output", False),
        ("isol", True),
        ("vent", True),
    ]
    trace_rows = _load_pressure_trace_rows(logger)
    assert not any(row["trace_stage"] == "atmosphere_enter_verified" for row in trace_rows)


def test_wait_co2_route_dewpoint_gate_fails_fast_when_atmosphere_gate_missing(tmp_path: Path) -> None:
    cfg = {
        "workflow": {
            "stability": {
                "gas_route_dewpoint_gate_enabled": True,
                "gas_route_dewpoint_gate_policy": "reject",
            }
        }
    }
    point = CalibrationPoint(
        index=1,
        temp_chamber_c=20.0,
        co2_ppm=600.0,
        hgen_temp_c=20.0,
        hgen_rh_pct=30.0,
        target_pressure_hpa=1000.0,
        dewpoint_c=None,
        h2o_mmol=None,
        raw_h2o=None,
    )
    logger = RunLogger(tmp_path)
    pace = _FakePace()
    runner = CalibrationRunner(cfg, {"pace": pace}, logger, lambda *_: None, lambda *_: None)
    safe_stop_calls = []
    runner._best_effort_fail_fast_safe_stop = lambda **kwargs: safe_stop_calls.append(dict(kwargs)) or {"ok": True}

    assert runner._wait_co2_route_dewpoint_gate_before_seal(
        point,
        base_soak_s=0.0,
        log_context="unit test",
    ) is False
    logger.close()

    state = runner._point_runtime_state(point, phase="co2") or {}
    assert state.get("dewpoint_stabilization_started") is not True
    assert state["abort_reason"] == "ContinuousAtmosphereFlowthroughNotActive"
    assert safe_stop_calls == []
    trace_rows = _load_pressure_trace_rows(logger)
    end_row = next(row for row in trace_rows if row["trace_stage"] == "co2_precondition_dewpoint_gate_end")
    assert end_row["flush_gate_reason"].strip() == "ContinuousAtmosphereFlowthroughNotActive"
    assert end_row["dewpoint_stabilization_started"].strip().lower() != "true"
    assert end_row["abort_reason"].strip() == "ContinuousAtmosphereFlowthroughNotActive"


def test_check_flush_pressure_guard_aborts_on_rising_pressure(tmp_path: Path) -> None:
    cfg = {
        "workflow": {
            "pressure": {
                "flush_guard_pressure_tolerance_hpa": 100.0,
                "flush_guard_pressure_rising_slope_max_hpa_s": 0.01,
                "flush_guard_pressure_rising_min_delta_hpa": 0.5,
                "flush_guard_pressure_rising_fail_min_samples": 3,
                "flush_guard_pressure_rising_fail_min_rise_hpa": 2.0,
            }
        }
    }
    point = CalibrationPoint(
        index=1,
        temp_chamber_c=20.0,
        co2_ppm=600.0,
        hgen_temp_c=20.0,
        hgen_rh_pct=30.0,
        target_pressure_hpa=1000.0,
        dewpoint_c=None,
        h2o_mmol=None,
        raw_h2o=None,
    )
    logger = RunLogger(tmp_path)
    pace = _FakePace()
    pace._pressure_reads = [1012.0, 1012.8, 1014.2]
    runner = CalibrationRunner(cfg, {"pace": pace}, logger, lambda *_: None, lambda *_: None)
    runner._last_atmosphere_gate_summary = {
        "ambient_hpa": 1012.0,
        "atmosphere_ready": True,
    }
    safe_stop_calls = []
    runner._best_effort_fail_fast_safe_stop = lambda **kwargs: safe_stop_calls.append(dict(kwargs)) or {"ok": True}
    pressure_rows = []

    ok_first, state_first = runner._check_flush_pressure_guard(
        point,
        phase="co2",
        pressure_rows=pressure_rows,
        log_context="unit test flush guard",
    )
    time.sleep(0.02)
    ok_second, state_second = runner._check_flush_pressure_guard(
        point,
        phase="co2",
        pressure_rows=pressure_rows,
        log_context="unit test flush guard",
    )
    time.sleep(0.02)
    ok_third, state_third = runner._check_flush_pressure_guard(
        point,
        phase="co2",
        pressure_rows=pressure_rows,
        log_context="unit test flush guard",
    )
    logger.close()

    assert ok_first is True
    assert state_first["pressure_hpa"] == pytest.approx(1012.0)
    assert ok_second is True
    assert ok_third is False
    assert state_third["abort_reason"] == "PressureRisingDuringFlush"
    assert state_third["pressure_hpa"] == pytest.approx(1014.2)
    assert state_third["ambient_hpa"] == pytest.approx(1012.0)
    assert state_third["pressure_slope_hpa_s"] > 0.01
    assert safe_stop_calls[-1]["abort_reason"] == "PressureRisingDuringFlush"
    state = runner._point_runtime_state(point, phase="co2") or {}
    assert state["abort_reason"] == "PressureRisingDuringFlush"
    assert state["pressure_delta_from_ambient_hpa"] == pytest.approx(2.2)


def test_old_pace_vent2_requires_fresh_context(tmp_path: Path) -> None:
    cfg = _fresh_vent_cfg()
    logger = RunLogger(tmp_path)
    pace = _FakePaceOldCompletedBaselineRequiresFreshVent()
    runner = CalibrationRunner(cfg, {"pace": pace}, logger, lambda *_: None, lambda *_: None)

    samples = [(0.0, 1012.0), (1.0, 1012.1)]
    summary = runner._finalize_atmosphere_gate_summary(
        phase_start_ts=time.time(),
        reason="unit test missing fresh context",
        fresh_vent_command_sent=False,
        vent_status_sequence=[2],
        vent_completed=True,
        pace_syst_err_query='0,"No error"',
        pressure_window={
            "samples": samples,
            "metrics": runner._numeric_series_metrics(samples),
            "last_sample": {
                "pressure_hpa": 1012.1,
                "pressure_source": "pressure_gauge",
                "pressure_gauge_hpa": 1012.1,
                "pace_pressure_hpa": 0.0,
            },
        },
        phase="co2",
    )
    logger.close()

    assert summary["vent_status_sequence_text"] == "2"
    assert summary["fresh_vent_completed"] is True
    assert summary["atmosphere_ready"] is False
    assert summary["abort_reason"] == "FreshVentNotSent"


def test_old_pace_profile_falls_back_to_legacy_compat_when_detect_profile_blank(tmp_path: Path) -> None:
    cfg = _fresh_vent_cfg()
    logger = RunLogger(tmp_path)
    pace = _FakePaceOldCompletedBaselineLegacyCompatOnly()
    runner = CalibrationRunner(cfg, {"pace": pace}, logger, lambda *_: None, lambda *_: None)

    assert runner._set_pressure_controller_vent(True, reason="legacy compat fallback") is True
    logger.close()

    trace_rows = _load_pressure_trace_rows(logger)
    enter_row = next(row for row in trace_rows if row["trace_stage"] == "atmosphere_enter_verified")
    assert enter_row["vent_status_sequence"].strip() == "1,2"
    assert enter_row["atmosphere_ready"].strip().lower() == "true"


def test_valve8_is_total_valve_not_h2o_only(tmp_path: Path) -> None:
    cfg = _route_open_guard_cfg()
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(cfg, {}, logger, lambda *_: None, lambda *_: None)

    role_map = {item["valve_id"]: item for item in runner.valve_role_map_for_ids([8, 11])}
    logger.close()

    valve8 = role_map[8]
    assert valve8["role"] == "common_total_valve"
    assert valve8["legacy_config_key"] == "h2o_path"
    assert valve8["route_name"] == "common_total"
    assert valve8["meaning"] == "总阀门 / 总路阀"
    assert valve8["required_for_co2_route"] is True
    assert valve8["required_for_h2o_route"] is True
    assert valve8["can_introduce_pressure_source"] is False
    assert valve8["requires_active_atmosphere_flush"] is True
    assert "PACE vent is not synchronized" in valve8["risk_evidence"]
    valve11 = role_map[11]
    assert valve11["role"] == "gas_main"
    assert valve11["risk_amplifier"] is True
    assert "amplifies pressure rise" in valve11["risk_evidence"]


def test_co2_dry_route_includes_valve8(tmp_path: Path) -> None:
    cfg = _route_open_guard_cfg()
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(cfg, {}, logger, lambda *_: None, lambda *_: None)

    assert runner._co2_open_valves(_co2_test_point(ppm=600.0), include_total_valve=True)[0] == 8
    logger.close()


def test_co2_a_route_is_8_11_7_source(tmp_path: Path) -> None:
    cfg = _route_open_guard_cfg()
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(cfg, {}, logger, lambda *_: None, lambda *_: None)

    open_valves = runner._co2_open_valves(_co2_test_point(ppm=600.0, group="A"), include_total_valve=True)
    stages, unknown = runner._route_stage_groups_for_open_valves(open_valves)
    logger.close()

    assert open_valves == [8, 11, 7, 4]
    assert stages == [[8], [11], [7], [4]]
    assert unknown == []


def test_co2_b_route_is_8_11_16_source(tmp_path: Path) -> None:
    cfg = _route_open_guard_cfg()
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(cfg, {}, logger, lambda *_: None, lambda *_: None)

    open_valves = runner._co2_open_valves(_co2_test_point(ppm=500.0, group="B"), include_total_valve=True)
    stages, unknown = runner._route_stage_groups_for_open_valves(open_valves)
    logger.close()

    assert open_valves == [8, 11, 16, 24]
    assert stages == [[8], [11], [16], [24]]
    assert unknown == []


def test_h2o_route_is_8_9_10(tmp_path: Path) -> None:
    cfg = _route_open_guard_cfg()
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(cfg, {}, logger, lambda *_: None, lambda *_: None)

    open_valves = runner._h2o_open_valves(_h2o_test_point())
    stages, unknown = runner._route_stage_groups_for_open_valves(open_valves)
    logger.close()

    assert open_valves == [8, 9, 10]
    assert stages == [[8], [9], [10]]
    assert unknown == []


def test_route_open_guard_runs_before_soak(tmp_path: Path) -> None:
    cfg = _route_open_guard_cfg()
    logger = RunLogger(tmp_path)
    pace = _FakePaceOldCompletedBaselineRequiresFreshVent()
    gauge = _FakeGaugeSequence([1012.0] * 24)
    runner = CalibrationRunner(cfg, {"pace": pace, "pressure_gauge": gauge}, logger, lambda *_: None, lambda *_: None)
    point = CalibrationPoint(
        index=1,
        temp_chamber_c=20.0,
        co2_ppm=600.0,
        hgen_temp_c=20.0,
        hgen_rh_pct=30.0,
        target_pressure_hpa=1000.0,
        dewpoint_c=None,
        h2o_mmol=None,
        raw_h2o=None,
    )

    assert runner._open_co2_route_for_conditioning(point, point_tag="unit_route_guard") is True
    assert runner._wait_co2_route_soak_before_seal(point) is True
    logger.close()

    trace_rows = _load_pressure_trace_rows(logger)
    guard_end_index = next(
        idx for idx, row in enumerate(trace_rows) if row["trace_stage"] == "route_open_pressure_guard_end"
    )
    soak_begin_index = next(
        idx for idx, row in enumerate(trace_rows) if row["trace_stage"] == "soak_begin"
    )
    assert guard_end_index < soak_begin_index


def test_route_open_guard_aborts_before_dewpoint(tmp_path: Path) -> None:
    cfg = _route_open_guard_cfg()
    logger = RunLogger(tmp_path)
    pace = _FakePaceOldCompletedBaselineRequiresFreshVent()
    gauge = _FakeGaugeSequence([1012.0, 1012.0, 1045.0, 1045.0, 1045.0])
    runner = CalibrationRunner(cfg, {"pace": pace, "pressure_gauge": gauge}, logger, lambda *_: None, lambda *_: None)
    point = CalibrationPoint(
        index=1,
        temp_chamber_c=20.0,
        co2_ppm=600.0,
        hgen_temp_c=20.0,
        hgen_rh_pct=30.0,
        target_pressure_hpa=1000.0,
        dewpoint_c=None,
        h2o_mmol=None,
        raw_h2o=None,
    )

    assert runner._open_co2_route_for_conditioning(point, point_tag="unit_route_guard_abort") is False
    logger.close()

    state = runner._point_runtime_state(point, phase="co2") or {}
    assert state["abort_reason"] == "RouteVentPathNotEffective"
    trace_rows = _load_pressure_trace_rows(logger)
    assert any(row["trace_stage"] == "route_open_pressure_guard_end" for row in trace_rows)
    assert not any(row["trace_stage"] == "soak_begin" for row in trace_rows)
    assert not any(row["trace_stage"] == "co2_precondition_dewpoint_gate_begin" for row in trace_rows)


def test_route_valve_isolation_detects_offending_group(tmp_path: Path) -> None:
    cfg = _route_open_guard_cfg()
    logger = RunLogger(tmp_path)
    pace = _FakePaceOldCompletedBaselineRequiresFreshVent()
    gauge = _FakeGaugeSequence([1012.0, 1420.0, 1420.0])
    runner = CalibrationRunner(cfg, {"pace": pace, "pressure_gauge": gauge}, logger, lambda *_: None, lambda *_: None)
    point = CalibrationPoint(
        index=1,
        temp_chamber_c=20.0,
        co2_ppm=600.0,
        hgen_temp_c=20.0,
        hgen_rh_pct=30.0,
        target_pressure_hpa=1000.0,
        dewpoint_c=None,
        h2o_mmol=None,
        raw_h2o=None,
    )
    runner._last_atmosphere_gate_summary = {
        "ambient_hpa": 1012.0,
        "atmosphere_ready": True,
    }
    runner._apply_valve_states([4, 7, 8, 11])

    ok, summary = runner._run_route_open_pressure_guard(
        point,
        phase="co2",
        log_context="unit valve isolation",
        point_tag="unit_route_valve_isolation",
        stage_label="4|7|8|11",
    )
    logger.close()

    assert ok is False
    assert summary["abort_reason"] == "RouteVentPathNotEffective"
    assert summary["offending_route"] == "open:4|7|8|11"
    assert summary["offending_valve_or_group"] == "4|7|8|11"


def test_synchronized_atmosphere_flush_sends_fresh_vent_after_route_stage(tmp_path: Path) -> None:
    cfg = _route_open_guard_cfg()
    logger = RunLogger(tmp_path)
    pace = _FakePaceOldCompletedBaselineRequiresFreshVent()
    gauge = _FakeGaugeSequence([1012.0] * 64)
    runner = CalibrationRunner(cfg, {"pace": pace, "pressure_gauge": gauge}, logger, lambda *_: None, lambda *_: None)

    assert runner._open_co2_route_for_conditioning(_co2_test_point(ppm=600.0), point_tag="unit_sync_flush") is True
    logger.close()

    fresh_vent_calls = [call for call in pace.calls if call == ("vent", True)]
    assert len(fresh_vent_calls) >= 5
    trace_rows = _load_pressure_trace_rows(logger)
    assert sum(1 for row in trace_rows if row["trace_stage"] == "route_open_fresh_vent_begin") >= 4


def test_route_open_guard_repeats_old_pace_vent_keepalive_during_flowthrough(tmp_path: Path) -> None:
    cfg = _route_open_guard_cfg()
    cfg["workflow"]["pressure"]["route_open_guard_monitor_s"] = 0.28
    cfg["workflow"]["pressure"]["route_open_guard_poll_s"] = 0.02
    logger = RunLogger(tmp_path)
    pace = _FakePaceOldCompletedBaselineRequiresFreshVent()
    gauge = _FakeGaugeSequence([1012.0] * 256)
    runner = CalibrationRunner(cfg, {"pace": pace, "pressure_gauge": gauge}, logger, lambda *_: None, lambda *_: None)
    runner._continuous_atmosphere_keepalive_interval_s = lambda: 0.03
    point = _co2_test_point(ppm=600.0)

    runner.enter_continuous_atmosphere_flowthrough(
        "co2_a",
        point=point,
        phase="co2",
        point_tag="unit_repeated_keepalive",
        phase_name="SynchronizedAtmosphereFlush",
        reason="unit repeated keepalive begin",
    )
    runner._apply_valve_states([8, 11, 7, 4])
    ok, summary = runner._run_route_open_pressure_guard(
        point,
        phase="co2",
        log_context="unit repeated keepalive",
        point_tag="unit_repeated_keepalive",
        stage_label="8|11|7|4",
    )
    logger.close()

    assert ok is True
    assert summary["abort_reason"] == ""
    assert summary["continuous_keepalive_refresh_count"] >= 1
    assert sum(1 for call in pace.calls if call == ("vent", True)) >= 2
    assert ("vent", False) not in pace.calls
    state = runner._point_runtime_state(point, phase="co2") or {}
    assert state["continuous_atmosphere_active"] is True
    assert state["route_flow_active"] is True
    assert state["vent_keepalive_count"] >= 2
    assert state["pace_vent_command_sent"] == ":SOUR:PRES:LEV:IMM:AMPL:VENT 1"
    assert state["pace_vent_status_returned"] in {None, 1, 2}
    trace_rows = _load_pressure_trace_rows(logger)
    assert sum(1 for row in trace_rows if row["trace_stage"] == "route_open_fresh_vent_begin") >= 1
    assert sum(1 for row in trace_rows if row["trace_stage"] == "continuous_atmosphere_vent1_refresh") >= 1


def test_route_open_guard_event_trigger_sends_quick_keepalive_before_recovery_threshold(tmp_path: Path) -> None:
    cfg = _route_open_guard_cfg()
    pressure_cfg = cfg["workflow"]["pressure"]
    pressure_cfg["route_open_guard_monitor_s"] = 0.08
    pressure_cfg["route_open_guard_poll_s"] = 0.01
    pressure_cfg["route_open_guard_pressure_rising_min_delta_hpa"] = 25.0
    pressure_cfg["continuous_atmosphere_keepalive_interval_s"] = 99.0
    pressure_cfg["continuous_atmosphere_rise_trigger_delta_hpa"] = 2.0
    logger = RunLogger(tmp_path)
    pace = _FakePaceOldCompletedBaselineRequiresFreshVent()
    gauge = _FakeGaugeSequence([1012.0, 1014.5, 1014.4, 1014.3, 1014.2, 1014.1, 1014.0])
    runner = CalibrationRunner(cfg, {"pace": pace, "pressure_gauge": gauge}, logger, lambda *_: None, lambda *_: None)
    point = _co2_test_point(ppm=600.0)

    runner.enter_continuous_atmosphere_flowthrough(
        "co2_a",
        point=point,
        phase="co2",
        point_tag="unit_event_keepalive",
        phase_name="SynchronizedAtmosphereFlush",
        reason="unit event keepalive begin",
    )
    runner._apply_valve_states([8])
    ok, summary = runner._run_route_open_pressure_guard(
        point,
        phase="co2",
        log_context="unit event keepalive",
        point_tag="unit_event_keepalive",
        stage_label="8",
    )
    logger.close()

    assert ok is True
    assert summary["continuous_keepalive_refresh_count"] >= 1
    assert summary["continuous_event_keepalive_refresh_count"] >= 1
    assert summary["continuous_keepalive_trigger_reason"] == "pressure_delta_from_ambient_hpa"
    assert ("vent", False) not in pace.calls
    trace_rows = _load_pressure_trace_rows(logger)
    assert any(row["trace_stage"] == "continuous_atmosphere_vent1_refresh" for row in trace_rows)


def test_source_final_valve_open_runs_pre_and_post_quick_vent_bursts(tmp_path: Path) -> None:
    cfg = _route_open_guard_cfg()
    pressure_cfg = cfg["workflow"]["pressure"]
    pressure_cfg["route_open_guard_monitor_s"] = 0.02
    pressure_cfg["route_open_guard_poll_s"] = 0.01
    pressure_cfg["pre_source_final_vent_burst_count"] = 2
    pressure_cfg["pre_source_final_vent_burst_interval_s"] = 0.1
    pressure_cfg["post_source_final_vent_burst_window_s"] = 0.21
    pressure_cfg["post_source_final_vent_burst_interval_s"] = 0.1
    logger = RunLogger(tmp_path)
    pace = _FakePaceOldCompletedBaselineRequiresFreshVent()
    gauge = _FakeGaugeSequence([1012.0] * 512)
    runner = CalibrationRunner(cfg, {"pace": pace, "pressure_gauge": gauge}, logger, lambda *_: None, lambda *_: None)

    assert runner._open_co2_route_for_conditioning(_co2_test_point(ppm=600.0), point_tag="unit_source_final_burst")
    logger.close()

    trace_rows = _load_pressure_trace_rows(logger)
    stages = [row["trace_stage"] for row in trace_rows]
    final_stage_index = next(
        i
        for i, row in enumerate(trace_rows)
        if row["trace_stage"] == "route_open_stage" and row["offending_valve_or_group"] == "8|11|7|4"
    )
    assert stages.index("pre_source_final_vent_burst_begin") < final_stage_index
    assert final_stage_index < stages.index("post_source_final_vent_burst_begin")
    assert stages.index("post_source_final_vent_burst_begin") < stages.index("post_source_final_vent_burst_end")
    burst_refresh_rows = [
        row
        for row in trace_rows
        if row["trace_stage"] == "continuous_atmosphere_vent1_refresh"
        and "source_final_vent_burst" in row["note"]
    ]
    assert len(burst_refresh_rows) >= 4
    assert ("vent", False) not in pace.calls


def test_continuous_atmosphere_background_keepalive_runs_until_exit(tmp_path: Path) -> None:
    cfg = _fresh_vent_cfg()
    pressure_cfg = cfg["workflow"]["pressure"]
    pressure_cfg["continuous_atmosphere_background_keepalive_allow_fake"] = True
    pressure_cfg["continuous_atmosphere_keepalive_interval_s"] = 0.03
    logger = RunLogger(tmp_path)
    pace = _FakePaceOldCompletedBaselineRequiresFreshVent()
    runner = CalibrationRunner(cfg, {"pace": pace}, logger, lambda *_: None, lambda *_: None)
    runner._continuous_atmosphere_keepalive_interval_s = lambda: 0.03
    point = _co2_test_point(ppm=600.0)

    runner.enter_continuous_atmosphere_flowthrough(
        "co2_a",
        point=point,
        phase="co2",
        point_tag="unit_background_keepalive",
        phase_name="SynchronizedAtmosphereFlush",
        reason="unit background keepalive begin",
    )
    deadline = time.time() + 0.3
    while sum(1 for call in pace.calls if call == ("vent", True)) < 3 and time.time() < deadline:
        time.sleep(0.01)

    before_exit_count = sum(1 for call in pace.calls if call == ("vent", True))
    assert before_exit_count >= 3
    active_state = runner._continuous_atmosphere_state_snapshot()
    assert active_state["background_keepalive_active"] is True
    assert active_state["background_keepalive_count"] >= 3

    runner.exit_continuous_atmosphere_flowthrough(
        "co2_a",
        point=point,
        phase="co2",
        point_tag="unit_background_keepalive",
        reason="unit exit background keepalive",
    )
    stopped_count = sum(1 for call in pace.calls if call == ("vent", True))
    time.sleep(0.08)
    logger.close()

    assert sum(1 for call in pace.calls if call == ("vent", True)) == stopped_count
    assert ("vent", False) not in pace.calls
    stopped_state = runner._continuous_atmosphere_state_snapshot()
    assert stopped_state["active"] is False
    assert stopped_state["route_flow_active"] is False
    assert stopped_state["background_keepalive_active"] is False
    runtime_state = runner._point_runtime_state(point, phase="co2") or {}
    assert runtime_state["continuous_atmosphere_background_keepalive_active"] is False
    assert runtime_state["continuous_atmosphere_background_keepalive_count"] >= 3
    trace_rows = _load_pressure_trace_rows(logger)
    assert any(row["trace_stage"] == "continuous_atmosphere_background_keepalive_start" for row in trace_rows)
    assert any(row["trace_stage"] == "continuous_atmosphere_background_keepalive_stop" for row in trace_rows)


def test_continuous_atmosphere_background_keepalive_stops_before_sealed_guard(tmp_path: Path) -> None:
    cfg = _fresh_vent_cfg()
    pressure_cfg = cfg["workflow"]["pressure"]
    pressure_cfg["continuous_atmosphere_background_keepalive_allow_fake"] = True
    pressure_cfg["continuous_atmosphere_keepalive_interval_s"] = 0.03
    logger = RunLogger(tmp_path)
    pace = _FakePaceOldCompletedBaselineRequiresFreshVent()
    runner = CalibrationRunner(cfg, {"pace": pace}, logger, lambda *_: None, lambda *_: None)
    runner._continuous_atmosphere_keepalive_interval_s = lambda: 0.03
    point = _co2_test_point(ppm=600.0)

    runner.enter_continuous_atmosphere_flowthrough(
        "co2_a",
        point=point,
        phase="co2",
        point_tag="unit_background_sealed_guard",
        phase_name="SynchronizedAtmosphereFlush",
        reason="unit background before seal",
    )
    deadline = time.time() + 0.2
    while sum(1 for call in pace.calls if call == ("vent", True)) < 2 and time.time() < deadline:
        time.sleep(0.01)
    assert sum(1 for call in pace.calls if call == ("vent", True)) >= 2

    runner._activate_sealed_no_vent_guard(
        point=point,
        phase="co2",
        guard_phase="PressurePointSwitch",
        reason="unit sealed guard stops background",
    )
    stopped_count = sum(1 for call in pace.calls if call == ("vent", True))
    time.sleep(0.08)
    logger.close()

    assert sum(1 for call in pace.calls if call == ("vent", True)) == stopped_count
    assert ("vent", False) not in pace.calls
    state = runner._continuous_atmosphere_state_snapshot()
    assert state["background_keepalive_active"] is False
    assert runner._sealed_no_vent_guard_snapshot()["active"] is True


def test_route_open_guard_uses_quick_refresh_while_background_keepalive_active(tmp_path: Path) -> None:
    cfg = _route_open_guard_cfg()
    pressure_cfg = cfg["workflow"]["pressure"]
    pressure_cfg["continuous_atmosphere_background_keepalive_allow_fake"] = True
    pressure_cfg["route_open_guard_monitor_s"] = 0.04
    pressure_cfg["route_open_guard_poll_s"] = 0.01
    logger = RunLogger(tmp_path)
    pace = _FakePaceOldCompletedBaselineRequiresFreshVent()
    gauge = _FakeGaugeSequence([1012.0] * 128)
    runner = CalibrationRunner(cfg, {"pace": pace, "pressure_gauge": gauge}, logger, lambda *_: None, lambda *_: None)
    runner._continuous_atmosphere_keepalive_interval_s = lambda: 0.03
    runner._last_atmosphere_gate_summary = {"ambient_hpa": 1012.0, "atmosphere_ready": True}
    point = _co2_test_point(ppm=600.0)

    runner.enter_continuous_atmosphere_flowthrough(
        "co2_a",
        point=point,
        phase="co2",
        point_tag="unit_background_quick_guard",
        phase_name="SynchronizedAtmosphereFlush",
        reason="unit background quick guard begin",
    )
    deadline = time.time() + 0.2
    while not runner._continuous_atmosphere_state_snapshot().get("background_keepalive_active") and time.time() < deadline:
        time.sleep(0.01)
    runner._apply_valve_states([8])
    ok, summary = runner._run_route_open_pressure_guard(
        point,
        phase="co2",
        log_context="unit background quick guard",
        point_tag="unit_background_quick_guard",
        stage_label="8",
    )
    runner.exit_continuous_atmosphere_flowthrough(
        "co2_a",
        point=point,
        phase="co2",
        point_tag="unit_background_quick_guard",
        reason="unit background quick guard exit",
    )
    logger.close()

    assert ok is True
    assert summary["abort_reason"] == ""
    trace_rows = _load_pressure_trace_rows(logger)
    assert any(row["trace_stage"] == "continuous_atmosphere_vent1_refresh" for row in trace_rows)
    assert not any(row["trace_stage"] == "route_open_fresh_vent_begin" for row in trace_rows)
    assert ("vent", False) not in pace.calls


def test_sealed_control_blocks_when_any_required_solenoid_remains_open(tmp_path: Path) -> None:
    cfg = _route_open_guard_cfg()
    cfg["workflow"]["pressure"]["soft_recover_on_pressure_timeout"] = False
    logger = RunLogger(tmp_path)
    pace = _FakePace()
    runner = CalibrationRunner(cfg, {"pace": pace}, logger, lambda *_: None, lambda *_: None)
    runner._current_open_valves = (4, 7, 8, 11)
    point = _co2_test_point(ppm=600.0)
    point.target_pressure_hpa = 1100.0

    assert runner._set_pressure_to_target(point) is False
    logger.close()

    state = runner._point_runtime_state(point, phase="co2") or {}
    assert state["abort_reason"] == "SealTransitionIncomplete"
    assert state["seal_all_solenoids_closed"] is False
    assert state["seal_total_route_valve_closed"] is False
    assert state["seal_transition_completed"] is False
    assert set(state["seal_missing_closed_valves"]) >= {4, 7, 8, 11}
    assert ("setpoint", 1100.0) not in pace.calls
    assert ("output_on",) not in pace.calls
    trace_rows = _load_pressure_trace_rows(logger)
    assert any(row["trace_stage"] == "sealed_control_entry_blocked" for row in trace_rows)


def test_sealed_control_blocks_when_total_route_valve_remains_open(tmp_path: Path) -> None:
    cfg = _route_open_guard_cfg()
    logger = RunLogger(tmp_path)
    pace = _FakePace()
    runner = CalibrationRunner(cfg, {"pace": pace}, logger, lambda *_: None, lambda *_: None)
    runner._current_open_valves = (8,)
    point = _co2_test_point(ppm=600.0)
    point.target_pressure_hpa = 1100.0

    assert runner._set_pressure_to_target(point) is False
    logger.close()

    state = runner._point_runtime_state(point, phase="co2") or {}
    assert state["seal_all_solenoids_closed"] is False
    assert state["seal_total_route_valve_closed"] is False
    assert state["seal_transition_completed"] is False
    assert state["seal_missing_closed_valves"] == [8]
    assert ("setpoint", 1100.0) not in pace.calls


def test_pressurize_and_hold_records_total_route_valve_and_full_solenoid_seal(tmp_path: Path) -> None:
    cfg = _route_open_guard_cfg()
    pressure_cfg = cfg["workflow"]["pressure"]
    pressure_cfg["co2_no_topoff_vent_off_open_wait_s"] = 0.0
    pressure_cfg["pressurize_wait_after_vent_off_s"] = 0.0
    logger = RunLogger(tmp_path)
    pace = _FakePace()
    relay = _FakeRelay()
    relay8 = _FakeRelay()
    runner = CalibrationRunner(
        cfg,
        {"pace": pace, "relay": relay, "relay_8": relay8},
        logger,
        lambda *_: None,
        lambda *_: None,
    )
    runner._active_route_requires_preseal_topoff = False
    runner._current_open_valves = (4, 7, 8, 11)
    point = _co2_test_point(ppm=600.0)
    point.target_pressure_hpa = 1000.0

    assert runner._pressurize_and_hold(point, route="co2") is True
    logger.close()

    state = runner._point_runtime_state(point, phase="co2") or {}
    assert runner._current_open_valves == ()
    assert state["seal_all_solenoids_closed"] is True
    assert state["seal_total_route_valve_closed"] is True
    assert state["seal_transition_completed"] is True
    assert state["keepalive_stopped_before_seal"] is True
    assert state["preseal_final_atmosphere_exit_required"] is True
    assert state["preseal_final_atmosphere_exit_started"] is True
    assert state["preseal_final_atmosphere_exit_verified"] is True
    assert state["preseal_final_atmosphere_exit_phase"] == "preseal_before_full_seal"
    assert set(state["seal_required_valves_closed_list"]) >= {4, 7, 8, 11}
    trace_rows = _load_pressure_trace_rows(logger)
    stages = [row["trace_stage"] for row in trace_rows]
    assert "preseal_final_atmosphere_exit_started" in stages
    assert "pressure_vent0_command" in stages
    assert "preseal_final_atmosphere_exit_verified" in stages
    assert "seal_transition_started" in stages
    assert "seal_total_route_valve_closed" in stages
    assert "seal_all_required_solenoids_closed" in stages
    assert "seal_transition_completed" in stages
    assert (
        stages.index("preseal_final_atmosphere_exit_started")
        < stages.index("pressure_vent0_command")
        < stages.index("preseal_final_atmosphere_exit_verified")
        < stages.index("seal_transition_started")
        < stages.index("seal_transition_completed")
    )


def test_pressurize_and_hold_blocks_when_preseal_final_exit_is_not_verified(tmp_path: Path) -> None:
    cfg = _route_open_guard_cfg()
    pressure_cfg = cfg["workflow"]["pressure"]
    pressure_cfg["control_ready_wait_timeout_s"] = 0.0
    pressure_cfg["co2_no_topoff_vent_off_open_wait_s"] = 0.0
    pressure_cfg["pressurize_wait_after_vent_off_s"] = 0.0
    logger = RunLogger(tmp_path)
    pace = _FakePaceNotReady()
    runner = CalibrationRunner(cfg, {"pace": pace}, logger, lambda *_: None, lambda *_: None)
    runner._active_route_requires_preseal_topoff = False
    runner._current_open_valves = (4, 7, 8, 11)
    point = _co2_test_point(ppm=600.0)
    point.target_pressure_hpa = 1000.0

    assert runner._pressurize_and_hold(point, route="co2") is False
    logger.close()

    state = runner._point_runtime_state(point, phase="co2") or {}
    assert state["abort_reason"] == "PresealFinalAtmosphereExitNotVerified"
    assert state["preseal_final_atmosphere_exit_started"] is True
    assert state["preseal_final_atmosphere_exit_verified"] is False
    assert state["preseal_final_atmosphere_exit_phase"] == "preseal_before_full_seal"
    assert state["control_ready_failed_after_full_seal"] is False
    assert "vent_status=1" in state["preseal_final_atmosphere_exit_reason"]
    trace_rows = _load_pressure_trace_rows(logger)
    stages = [row["trace_stage"] for row in trace_rows]
    assert "preseal_final_atmosphere_exit_started" in stages
    assert "preseal_final_atmosphere_exit_failed" in stages
    assert "seal_transition_started" not in stages


def test_old_k0472_accepts_after_full_seal_watchlist_3_only_for_control_ready_entry(tmp_path: Path) -> None:
    cfg = _route_open_guard_cfg()
    pressure_cfg = cfg["workflow"]["pressure"]
    pressure_cfg["control_ready_wait_timeout_s"] = 0.0
    pressure_cfg["co2_no_topoff_vent_off_open_wait_s"] = 0.0
    pressure_cfg["pressurize_wait_after_vent_off_s"] = 0.0
    logger = RunLogger(tmp_path)
    pace = _FakePaceOldK0472OutputEnableWatchlist()
    runner = CalibrationRunner(cfg, {"pace": pace}, logger, lambda *_: None, lambda *_: None)
    runner._active_route_requires_preseal_topoff = False
    runner._current_open_valves = (4, 7, 8, 11)
    point = _co2_test_point(ppm=600.0)
    point.target_pressure_hpa = 1000.0

    assert runner._pressurize_and_hold(point, route="co2") is True
    calls_after_seal = list(pace.calls)
    state = runner._point_runtime_state(point, phase="co2") or {}
    assert state["seal_transition_completed"] is True
    assert state["preseal_final_atmosphere_exit_verified"] is True
    assert state["preseal_final_atmosphere_exit_phase"] == "preseal_before_full_seal"
    assert state["preseal_final_exit_watchlist_status_seen"] is True
    assert state["preseal_final_exit_watchlist_status_accepted"] is True
    assert "preseal_exit_watchlist_only_but_accepted" in state["preseal_final_exit_watchlist_status_reason"]
    assert state["legacy_v1_preseal_watchlist_evidence_found"] is True
    assert state["control_ready_watchlist_status_accepted"] is False

    assert runner._set_pressure_to_target(point) is True
    logger.close()

    new_calls = pace.calls[len(calls_after_seal):]
    assert not any(call[0] == "vent_off" for call in new_calls)
    assert ("vent", False) not in new_calls
    assert ("mode_active",) in new_calls
    assert ("output", True) in new_calls
    assert ("enable_control_output_unexpected",) not in new_calls
    state = runner._point_runtime_state(point, phase="co2") or {}
    assert state["control_ready_check_phase"] == "after_full_seal"
    assert state["control_ready_failed_with_watchlist_status_3"] is False
    assert state["control_ready_watchlist_status_accepted"] is True
    assert state["control_ready_watchlist_status_phase"] == "after_full_seal"
    assert state["control_ready_check_watchlist_status_seen"] is True
    assert state["control_ready_check_watchlist_status_accepted"] is True
    assert state["after_full_seal_watchlist_status_seen"] is True
    assert state["after_full_seal_watchlist_status_accepted"] is True
    assert "after_full_seal_watchlist_only_but_accepted" in state[
        "after_full_seal_watchlist_status_reason"
    ]
    assert state["legacy_v1_after_full_seal_watchlist_evidence_found"] is True
    assert state["legacy_v1_after_full_seal_watchlist_evidence_source"]
    assert state["after_full_seal_output_enable_watchlist_status_seen"] is True
    assert state["after_full_seal_output_enable_watchlist_status_accepted"] is True
    assert "after_full_seal_output_enable_watchlist_only_but_accepted" in state[
        "after_full_seal_output_enable_watchlist_status_reason"
    ]
    assert state["legacy_v1_after_full_seal_output_enable_watchlist_evidence_found"] is True
    assert state["legacy_v1_after_full_seal_output_enable_watchlist_evidence_source"]
    assert state["output_enable_watchlist_status_accepted"] is True
    assert state["output_enable_watchlist_status_phase"] == "after_full_seal"
    assert state["output_enable_failed_with_watchlist_status_3"] is False
    assert state["pace_control_started_after_full_seal"] is True
    trace_rows = _load_pressure_trace_rows(logger)
    accepted_rows = [
        row
        for row in trace_rows
        if row["trace_stage"] == "preseal_final_atmosphere_exit_verified"
    ]
    assert accepted_rows
    assert accepted_rows[-1]["preseal_final_exit_watchlist_status_seen"].strip() == "True"
    assert accepted_rows[-1]["preseal_final_exit_watchlist_status_accepted"].strip() == "True"
    assert accepted_rows[-1]["control_ready_watchlist_status_accepted"].strip() == "False"
    stages = [row["trace_stage"] for row in trace_rows]
    assert "control_vent_off_skipped_after_full_seal" in stages
    assert "control_ready_check_watchlist_status_seen" in stages
    assert "control_ready_check_watchlist_status_accepted" in stages
    assert "control_ready_verified" in stages
    assert "control_ready_check_failed_watchlist_status_3" not in stages
    assert "output_enable_started" in stages
    assert "output_enable_watchlist_status_seen" in stages
    assert "output_enable_watchlist_status_accepted" in stages
    assert "output_enable_failed_watchlist_status_3" not in stages
    assert "output_enable_verified" in stages
    assert "control_output_on_verified" in stages
    ready_rows = [row for row in trace_rows if row["trace_stage"] == "control_ready_verified"]
    assert ready_rows
    assert ready_rows[-1]["control_ready_watchlist_status_accepted"].strip() == "True"
    assert ready_rows[-1]["control_ready_check_watchlist_status_accepted"].strip() == "True"
    assert ready_rows[-1]["after_full_seal_watchlist_status_accepted"].strip() == "True"
    assert ready_rows[-1]["legacy_vent3_control_ready_used"].strip() == "True"
    assert (
        ready_rows[-1]["legacy_vent3_accept_scope"].strip()
        == "old_k0472_after_full_seal_control_ready_watchlist"
    )
    output_rows = [row for row in trace_rows if row["trace_stage"] == "output_enable_verified"]
    assert output_rows
    assert output_rows[-1]["output_enable_watchlist_status_accepted"].strip() == "True"
    assert output_rows[-1]["output_enable_watchlist_status_phase"].strip() == "after_full_seal"
    assert output_rows[-1]["after_full_seal_output_enable_watchlist_status_accepted"].strip() == "True"
    assert (
        output_rows[-1]["legacy_vent3_accept_scope"].strip()
        == "old_k0472_after_full_seal_output_enable_watchlist"
    )
    assert stages.index("preseal_final_atmosphere_exit_verified") < stages.index("seal_transition_started")


def test_set_pressure_skips_vent_latch_clear_after_full_seal(tmp_path: Path) -> None:
    cfg = _route_open_guard_cfg()
    cfg["workflow"]["pressure"]["control_ready_wait_timeout_s"] = 0.0
    logger = RunLogger(tmp_path)
    pace = _FakePaceCompletedVentLatch()
    runner = CalibrationRunner(cfg, {"pace": pace}, logger, lambda *_: None, lambda *_: None)
    runner._current_open_valves = ()
    point = _co2_test_point(ppm=600.0)
    point.target_pressure_hpa = 1100.0
    runner._set_point_runtime_fields(
        point,
        phase="co2",
        **runner._seal_transition_state(point, phase="co2"),
    )
    runner._record_preseal_pressure_control_ready_state(point, phase="co2", defer_live_check=True)
    runner._preseal_pressure_control_ready_state["recorded_wall_ts"] = time.time() - 999.0
    runner._activate_sealed_no_vent_guard(
        point=point,
        phase="co2",
        guard_phase="SealTransition",
        reason="unit full seal",
    )

    assert runner._set_pressure_to_target(point) is False
    logger.close()

    assert not any(call[0] == "clear_latch" for call in pace.calls)
    assert ("vent", False) not in pace.calls
    state = runner._point_runtime_state(point, phase="co2") or {}
    assert state["abort_reason"] == "PressureControllerNotReadyForControl"
    assert state["seal_transition_completed"] is True
    assert state["pace_control_started_after_full_seal"] is False
    assert state["pace_vent_clear_result"] == "skipped_after_full_seal"
    assert "control_ready_failed" in state["pressure_in_limits_timeout_reason_detail"]
    trace_rows = _load_pressure_trace_rows(logger)
    stages = [row["trace_stage"] for row in trace_rows]
    assert "pace_vent_clear_latch_skipped_after_full_seal" in stages
    assert "pressure_vent0_command" not in stages
    assert "pressure_vent0_blocked" not in stages


def test_control_ready_failure_records_watchlist_status_3_phase_after_full_seal(tmp_path: Path) -> None:
    cfg = _route_open_guard_cfg()
    cfg["workflow"]["pressure"]["control_ready_wait_timeout_s"] = 0.0
    logger = RunLogger(tmp_path)
    pace = _FakePaceLegacyVentTrapped()
    runner = CalibrationRunner(cfg, {"pace": pace}, logger, lambda *_: None, lambda *_: None)
    runner._current_open_valves = ()
    point = _co2_test_point(ppm=600.0)
    point.target_pressure_hpa = 1100.0
    runner._set_point_runtime_fields(
        point,
        phase="co2",
        **runner._seal_transition_state(point, phase="co2"),
    )
    runner._record_preseal_pressure_control_ready_state(point, phase="co2", defer_live_check=True)
    runner._preseal_pressure_control_ready_state["recorded_wall_ts"] = time.time() - 999.0
    runner._activate_sealed_no_vent_guard(
        point=point,
        phase="co2",
        guard_phase="SealTransition",
        reason="unit full seal",
    )

    assert runner._set_pressure_to_target(point) is False
    logger.close()

    state = runner._point_runtime_state(point, phase="co2") or {}
    assert state["abort_reason"] == "PressureControllerNotReadyForControl"
    assert state["control_ready_check_vent_status"] == 3
    assert state["control_ready_check_phase"] == "after_full_seal"
    assert state["control_ready_failed_after_full_seal"] is True
    assert state["control_ready_failed_with_watchlist_status_3"] is True
    assert state["control_ready_watchlist_status_accepted"] is False
    assert state["control_ready_watchlist_status_phase"] == "after_full_seal"
    assert state["control_ready_check_watchlist_status_seen"] is True
    assert state["control_ready_check_watchlist_status_accepted"] is False
    assert state["after_full_seal_watchlist_status_seen"] is True
    assert state["after_full_seal_watchlist_status_accepted"] is False
    assert "after_full_seal_watchlist_only_failure" in state["after_full_seal_watchlist_status_reason"]
    assert "phase=after_full_seal" in state["control_ready_failure_reason_detail"]
    assert "vent_status=3(watchlist_only)" in state["control_ready_failure_reason_detail"]
    trace_rows = _load_pressure_trace_rows(logger)
    stages = [row["trace_stage"] for row in trace_rows]
    assert "control_ready_check_started" in stages
    assert "control_ready_check_watchlist_status_seen" in stages
    assert "control_ready_check_failed_watchlist_status_3" in stages
    assert "control_ready_check_watchlist_status_accepted" not in stages
    assert "control_ready_failed" in stages
    assert "preseal_final_atmosphere_exit_started" not in stages
    assert "pressure_vent0_command" not in stages


def test_pressure_in_limits_timeout_reports_seal_verified_control_failure(tmp_path: Path) -> None:
    cfg = _route_open_guard_cfg()
    pressure_cfg = cfg["workflow"]["pressure"]
    pressure_cfg["stabilize_timeout_s"] = 0.05
    pressure_cfg["transition_trace_poll_s"] = 0.01
    pressure_cfg["restabilize_retries"] = 0
    pressure_cfg["soft_recover_on_pressure_timeout"] = False
    logger = RunLogger(tmp_path)
    pace = _FakePace()
    pace._in_limits_sequence = [(1200.0, 0)] * 20
    runner = CalibrationRunner(cfg, {"pace": pace}, logger, lambda *_: None, lambda *_: None)
    runner._current_open_valves = ()
    point = _co2_test_point(ppm=600.0)
    point.target_pressure_hpa = 1100.0

    assert runner._set_pressure_to_target(point) is False
    logger.close()

    state = runner._point_runtime_state(point, phase="co2") or {}
    assert state["seal_transition_completed"] is True
    assert state["pace_control_started_after_full_seal"] is True
    assert state["pressure_in_limits_timeout_phase"] == "sealed_control"
    assert "seal_verified_but_pace_not_in_limits" in state["pressure_in_limits_timeout_reason_detail"]
    trace_rows = _load_pressure_trace_rows(logger)
    stages = [row["trace_stage"] for row in trace_rows]
    assert "pressure_in_limits_wait_started" in stages
    assert "pressure_in_limits_wait_timeout" in stages


def test_continuous_atmosphere_keepalive_stops_when_sealed_guard_active(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    pace = _FakePaceOldCompletedBaselineRequiresFreshVent()
    runner = CalibrationRunner(_fresh_vent_cfg(), {"pace": pace}, logger, lambda *_: None, lambda *_: None)
    point = _co2_test_point(ppm=600.0)
    runner._continuous_atmosphere_state = {
        "active": True,
        "route_flow_active": True,
        "route_key": "co2_a",
        "phase_name": "ContinuousAtmosphereFlowThrough",
        "pressure_mode": CalibrationRunner._PRESSURE_MODE_ATMOSPHERE_FLUSH,
        "generation": 1,
        "keepalive_count": 1,
        "last_keepalive_ts": time.time() - 10.0,
        "last_keepalive_reason": "unit stale flowthrough",
        "last_keepalive_summary": {},
    }
    runner._activate_sealed_no_vent_guard(
        point=point,
        phase="co2",
        guard_phase="PressureSetpointHold",
        reason="unit sealed control",
    )

    ok, state = runner.maintain_continuous_atmosphere_flowthrough(
        "co2_a",
        point=point,
        phase="co2",
        point_tag="unit_sealed_guard",
        reason="unit sealed guard",
        force=True,
    )
    logger.close()

    assert ok is False
    assert state["abort_reason"] == "KeepaliveBlockedBySealedNoVentGuard"
    assert runner._continuous_atmosphere_state_snapshot()["active"] is False
    assert runner._continuous_atmosphere_state_snapshot()["route_flow_active"] is False
    assert ("vent", True) not in pace.calls


def test_source_final_vent_burst_stops_when_sealed_guard_active(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    pace = _FakePaceOldCompletedBaselineRequiresFreshVent()
    runner = CalibrationRunner(_fresh_vent_cfg(), {"pace": pace}, logger, lambda *_: None, lambda *_: None)
    point = _co2_test_point(ppm=600.0)
    runner._continuous_atmosphere_state = {
        "active": True,
        "route_flow_active": True,
        "route_key": "co2_a",
        "phase_name": "FinalStageAtmosphereSafetyGate",
        "pressure_mode": CalibrationRunner._PRESSURE_MODE_ATMOSPHERE_FLUSH,
        "generation": 1,
        "keepalive_count": 1,
        "last_keepalive_ts": time.time() - 10.0,
        "last_keepalive_reason": "unit stale burst",
        "last_keepalive_summary": {},
    }
    runner._activate_sealed_no_vent_guard(
        point=point,
        phase="co2",
        guard_phase="PressurePointSwitch",
        reason="unit sealed switch",
    )

    ok, summary = runner._run_continuous_atmosphere_vent_burst(
        point,
        phase="co2",
        point_tag="unit_burst_sealed_guard",
        route_key="co2_a",
        phase_name="FinalStageAtmosphereSafetyGate",
        stage_label="8|11|7|4",
        log_context="unit burst sealed guard",
        trace_prefix="post_source_final_vent_burst",
        count=2,
        interval_s=0.1,
    )
    logger.close()

    assert ok is False
    assert summary["burst_status"] == "fail"
    assert runner._continuous_atmosphere_state_snapshot()["active"] is False
    assert runner._continuous_atmosphere_state_snapshot()["route_flow_active"] is False
    assert ("vent", True) not in pace.calls


def test_continuous_atmosphere_keepalive_requires_open_route_pace_state(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    pace = _FakePaceOldCompletedBaselineRequiresFreshVent()
    pace.output_state = 1
    pace.isolation_state = 1
    runner = CalibrationRunner(_fresh_vent_cfg(), {"pace": pace}, logger, lambda *_: None, lambda *_: None)
    point = _co2_test_point(ppm=600.0)
    runner._last_atmosphere_gate_summary = {"ambient_hpa": 1012.0, "atmosphere_ready": True}
    runner._continuous_atmosphere_state = {
        "active": True,
        "route_flow_active": True,
        "route_key": "co2_a",
        "phase_name": "ContinuousAtmosphereFlowThrough",
        "pressure_mode": CalibrationRunner._PRESSURE_MODE_ATMOSPHERE_FLUSH,
        "generation": 1,
        "keepalive_count": 1,
        "last_keepalive_ts": time.time() - 10.0,
        "last_keepalive_reason": "unit unsafe route",
        "last_keepalive_summary": {},
    }

    ok, state = runner.maintain_continuous_atmosphere_flowthrough(
        "co2_a",
        point=point,
        phase="co2",
        point_tag="unit_open_route_required",
        reason="unit open route required",
        force=True,
    )
    logger.close()

    assert ok is False
    assert state["abort_reason"] == "ContinuousAtmosphereKeepaliveNotOpenRoute"
    assert state["pace_output_state"] == 1
    assert state["pace_isolation_state"] == 1
    assert ("vent", True) not in pace.calls
    trace_rows = _load_pressure_trace_rows(logger)
    assert any(row["trace_stage"] == "continuous_atmosphere_keepalive_blocked" for row in trace_rows)


def test_route_open_pressure_high_attempts_vent_recovery_before_abort(tmp_path: Path) -> None:
    cfg = _route_open_guard_cfg()
    logger = RunLogger(tmp_path)
    pace = _FakePaceOldCompletedBaselineRequiresFreshVent()
    gauge = _FakeGaugeSequence([1065.0, 1060.0, 1060.0])
    runner = CalibrationRunner(cfg, {"pace": pace, "pressure_gauge": gauge}, logger, lambda *_: None, lambda *_: None)
    point = _co2_test_point(ppm=600.0)
    runner._last_atmosphere_gate_summary = {"ambient_hpa": 1012.0, "atmosphere_ready": True}
    runner._apply_valve_states([8])

    ok, summary = runner._run_route_open_pressure_guard(
        point,
        phase="co2",
        log_context="unit route recovery abort",
        point_tag="unit_route_recovery_abort",
        stage_label="8",
    )
    logger.close()

    assert ok is False
    assert summary["vent_recovery_attempted"] is True
    assert summary["vent_recovery_count"] == 1
    assert sum(1 for call in pace.calls if call == ("vent", True)) == 2


def test_route_open_pressure_recovery_passes_when_revent_returns_to_ambient(tmp_path: Path) -> None:
    cfg = _route_open_guard_cfg()
    logger = RunLogger(tmp_path)
    pace = _FakePaceOldCompletedBaselineRequiresFreshVent()
    gauge = _FakeGaugeSequence([1065.0, 1012.2, 1012.1, 1012.1])
    runner = CalibrationRunner(cfg, {"pace": pace, "pressure_gauge": gauge}, logger, lambda *_: None, lambda *_: None)
    point = _co2_test_point(ppm=600.0)
    runner._last_atmosphere_gate_summary = {"ambient_hpa": 1012.0, "atmosphere_ready": True}
    runner._apply_valve_states([8])

    ok, summary = runner._run_route_open_pressure_guard(
        point,
        phase="co2",
        log_context="unit route recovery pass",
        point_tag="unit_route_recovery_pass",
        stage_label="8",
    )
    logger.close()

    assert ok is True
    assert summary["abort_reason"] == ""
    assert summary["vent_recovery_attempted"] is True
    assert summary["vent_recovery_count"] == 1
    assert summary["vent_recovery_result"] == "vent_refresh_recovered"


def test_route_open_pressure_recovery_fails_when_revent_cannot_reduce_pressure(tmp_path: Path) -> None:
    cfg = _route_open_guard_cfg()
    logger = RunLogger(tmp_path)
    pace = _FakePaceOldCompletedBaselineRequiresFreshVent()
    gauge = _FakeGaugeSequence([1065.0, 1055.0, 1055.0])
    runner = CalibrationRunner(cfg, {"pace": pace, "pressure_gauge": gauge}, logger, lambda *_: None, lambda *_: None)
    point = _co2_test_point(ppm=600.0)
    runner._last_atmosphere_gate_summary = {"ambient_hpa": 1012.0, "atmosphere_ready": True}
    runner._apply_valve_states([8])

    ok, summary = runner._run_route_open_pressure_guard(
        point,
        phase="co2",
        log_context="unit route recovery fail",
        point_tag="unit_route_recovery_fail",
        stage_label="8",
    )
    logger.close()

    assert ok is False
    assert summary["abort_reason"] == "RouteVentPathNotEffective"
    assert summary["vent_recovery_attempted"] is True
    assert summary["vent_recovery_result"] == "vent_refresh_failed"


def test_baseline_small_delta_slope_does_not_fail(tmp_path: Path) -> None:
    cfg = _route_open_guard_cfg()
    logger = RunLogger(tmp_path)
    pace = _FakePaceOldCompletedBaselineRequiresFreshVent()
    pace.barometric_pressure_hpa = 1013.3
    runner = CalibrationRunner(cfg, {"pace": pace}, logger, lambda *_: None, lambda *_: None)

    samples = [(0.0, 1012.1), (0.5, 1013.0), (1.0, 1013.5)]
    summary = runner._finalize_atmosphere_gate_summary(
        phase_start_ts=time.time(),
        reason="unit baseline slope grace",
        fresh_vent_command_sent=True,
        vent_status_sequence=[1, 2],
        vent_completed=True,
        pace_syst_err_query='0,"No error"',
        pressure_window={
            "samples": samples,
            "metrics": runner._numeric_series_metrics(samples),
            "last_sample": {
                "pressure_hpa": 1013.5,
                "pressure_source": "pressure_gauge",
                "pressure_gauge_hpa": 1013.5,
                "pace_pressure_hpa": 0.0,
            },
        },
        phase="co2",
    )
    logger.close()

    assert summary["pressure_delta_from_ambient_hpa"] == pytest.approx(0.2)
    assert summary["pressure_rising_suspicious"] is True
    assert summary["atmosphere_ready"] is True
    assert summary["abort_reason"] == ""


def test_dewpoint_stabilization_started_only_after_route_flush_gates_pass(tmp_path: Path) -> None:
    cfg = {
        "workflow": {
            "stability": {
                "gas_route_dewpoint_gate_enabled": True,
                "gas_route_dewpoint_gate_policy": "reject",
            }
        }
    }
    point = _co2_test_point(ppm=600.0)
    logger = RunLogger(tmp_path)
    pace = _FakePace()
    runner = CalibrationRunner(cfg, {"pace": pace}, logger, lambda *_: None, lambda *_: None)
    runner._best_effort_fail_fast_safe_stop = lambda **kwargs: {"ok": True}

    assert runner._wait_co2_route_dewpoint_gate_before_seal(
        point,
        base_soak_s=0.0,
        log_context="unit test post-gate start",
    ) is False
    logger.close()

    state = runner._point_runtime_state(point, phase="co2") or {}
    assert state.get("dewpoint_stabilization_started") is not True
    trace_rows = _load_pressure_trace_rows(logger)
    end_row = next(row for row in trace_rows if row["trace_stage"] == "co2_precondition_dewpoint_gate_end")
    assert end_row["dewpoint_stabilization_started"].strip().lower() != "true"


def test_pressure_setpoint_hold_does_not_use_atmosphere_guard(tmp_path: Path) -> None:
    cfg = {
        "workflow": {
            "pressure": {
                "vent_time_s": 0,
                "vent_transition_timeout_s": 12,
                "stabilize_timeout_s": 0.1,
            }
        }
    }
    point = CalibrationPoint(
        index=1,
        temp_chamber_c=20.0,
        co2_ppm=None,
        hgen_temp_c=20.0,
        hgen_rh_pct=30.0,
        target_pressure_hpa=1100.0,
        dewpoint_c=None,
        h2o_mmol=None,
        raw_h2o=None,
    )
    logger = RunLogger(tmp_path)
    pace = _FakePace()
    runner = CalibrationRunner(cfg, {"pace": pace}, logger, lambda *_: None, lambda *_: None)

    assert runner._set_pressure_to_target(point) is True
    logger.close()

    trace_rows = _load_pressure_trace_rows(logger)
    assert not any(row["trace_stage"].startswith("atmosphere_") for row in trace_rows)
    assert not any(row["trace_stage"].startswith("route_open_pressure_guard") for row in trace_rows)
    assert pace.calls == [
        ("vent_off", 12.0),
        ("setpoint", 1100.0),
        ("output_on",),
    ]


def test_pressure_setpoint_hold_has_no_vent_refresh(tmp_path: Path) -> None:
    cfg = {
        "workflow": {
            "pressure": {
                "vent_time_s": 0,
                "vent_transition_timeout_s": 12,
                "stabilize_timeout_s": 0.1,
            }
        }
    }
    point = CalibrationPoint(
        index=1,
        temp_chamber_c=20.0,
        co2_ppm=None,
        hgen_temp_c=None,
        hgen_rh_pct=None,
        target_pressure_hpa=1100.0,
        dewpoint_c=None,
        h2o_mmol=None,
        raw_h2o=None,
    )
    logger = RunLogger(tmp_path)
    pace = _FakePace()
    runner = CalibrationRunner(cfg, {"pace": pace}, logger, lambda *_: None, lambda *_: None)

    assert runner._set_pressure_to_target(point) is True
    logger.close()

    assert not any(call == ("vent", True) for call in pace.calls)
    trace_rows = _load_pressure_trace_rows(logger)
    assert not any(row["trace_stage"] == "route_open_fresh_vent_begin" for row in trace_rows)


def test_route_flush_does_not_use_pressure_setpoint_hold(tmp_path: Path) -> None:
    cfg = _route_open_guard_cfg()
    logger = RunLogger(tmp_path)
    pace = _FakePaceOldCompletedBaselineRequiresFreshVent()
    gauge = _FakeGaugeSequence([1012.0] * 24)
    runner = CalibrationRunner(cfg, {"pace": pace, "pressure_gauge": gauge}, logger, lambda *_: None, lambda *_: None)
    point = CalibrationPoint(
        index=1,
        temp_chamber_c=20.0,
        co2_ppm=600.0,
        hgen_temp_c=20.0,
        hgen_rh_pct=30.0,
        target_pressure_hpa=1100.0,
        dewpoint_c=None,
        h2o_mmol=None,
        raw_h2o=None,
    )

    assert runner._open_co2_route_for_conditioning(point, point_tag="unit_route_flush_only") is True
    logger.close()

    assert not any(call[0] == "setpoint" for call in pace.calls)
    assert not any(call[0] == "output_on" for call in pace.calls)


def test_set_pressure_to_target_closes_vent_before_setpoint_and_output(tmp_path: Path) -> None:
    cfg = {
        "workflow": {
            "pressure": {
                "vent_time_s": 0,
                "vent_transition_timeout_s": 12,
                "stabilize_timeout_s": 0.1,
            }
        }
    }
    point = CalibrationPoint(
        index=1,
        temp_chamber_c=20.0,
        co2_ppm=None,
        hgen_temp_c=20.0,
        hgen_rh_pct=30.0,
        target_pressure_hpa=1000.0,
        dewpoint_c=None,
        h2o_mmol=None,
        raw_h2o=None,
    )
    logger = RunLogger(tmp_path)
    pace = _FakePace()
    runner = CalibrationRunner(cfg, {"pace": pace}, logger, lambda *_: None, lambda *_: None)

    assert runner._set_pressure_to_target(point) is True
    logger.close()

    assert pace.calls == [
        ("vent_off", 12.0),
        ("setpoint", 1000.0),
        ("output_on",),
    ]


def test_set_pressure_to_target_ignores_aux_close_readback_failure_when_open_valve_strategy_not_used(
    tmp_path: Path,
) -> None:
    cfg = {
        "workflow": {
            "pressure": {
                "vent_time_s": 0,
                "vent_transition_timeout_s": 12,
                "stabilize_timeout_s": 0.1,
                "vent_after_valve_open": False,
            }
        }
    }
    point = CalibrationPoint(
        index=1,
        temp_chamber_c=20.0,
        co2_ppm=None,
        hgen_temp_c=20.0,
        hgen_rh_pct=30.0,
        target_pressure_hpa=1000.0,
        dewpoint_c=None,
        h2o_mmol=None,
        raw_h2o=None,
    )
    logger = RunLogger(tmp_path)
    pace = _FakePaceVentAfterValveGetterFailure()
    runner = CalibrationRunner(cfg, {"pace": pace}, logger, lambda *_: None, lambda *_: None)
    runner._pace_vent_after_valve_supported = True
    runner._pace_vent_after_valve_open = False
    runner._pressure_atmosphere_hold_strategy = "legacy_hold_thread"

    assert runner._set_pressure_to_target(point) is True
    logger.close()

    assert pace.calls == [
        ("vent_off", 12.0),
        ("setpoint", 1000.0),
        ("output_on",),
    ]
    trace_rows = _load_pressure_trace_rows(logger)
    assert not any(row["trace_stage"] == "control_vent_after_valve_closed" for row in trace_rows)
    assert not any(row["trace_stage"] == "control_vent_off_failed" for row in trace_rows)


def test_set_pressure_to_target_legacy_strategy_allows_stale_aux_close_readback_failure(
    tmp_path: Path,
) -> None:
    cfg = {
        "workflow": {
            "pressure": {
                "vent_time_s": 0,
                "vent_transition_timeout_s": 12,
                "stabilize_timeout_s": 0.1,
                "vent_after_valve_open": False,
            }
        }
    }
    point = CalibrationPoint(
        index=1,
        temp_chamber_c=20.0,
        co2_ppm=None,
        hgen_temp_c=20.0,
        hgen_rh_pct=30.0,
        target_pressure_hpa=1000.0,
        dewpoint_c=None,
        h2o_mmol=None,
        raw_h2o=None,
    )
    logger = RunLogger(tmp_path)
    pace = _FakePaceVentAfterValveGetterFailure()
    runner = CalibrationRunner(cfg, {"pace": pace}, logger, lambda *_: None, lambda *_: None)
    runner._pace_vent_after_valve_supported = True
    runner._pace_vent_after_valve_open = True
    runner._pressure_atmosphere_hold_strategy = "legacy_hold_thread"

    assert runner._set_pressure_to_target(point) is True
    logger.close()

    assert pace.calls == [
        ("vent_off", 12.0),
        ("setpoint", 1000.0),
        ("output_on",),
    ]
    trace_rows = _load_pressure_trace_rows(logger)
    assert not any(row["trace_stage"] == "control_vent_after_valve_closed" for row in trace_rows)
    assert not any(row["trace_stage"] == "control_vent_off_failed" for row in trace_rows)


def test_set_pressure_to_target_normalizes_open_valve_strategy_and_ignores_stale_close_readback_failure(
    tmp_path: Path,
) -> None:
    cfg = {
        "workflow": {
            "pressure": {
                "vent_time_s": 0,
                "vent_transition_timeout_s": 12,
                "stabilize_timeout_s": 0.1,
                "vent_after_valve_open": True,
            }
        }
    }
    point = CalibrationPoint(
        index=1,
        temp_chamber_c=20.0,
        co2_ppm=None,
        hgen_temp_c=20.0,
        hgen_rh_pct=30.0,
        target_pressure_hpa=1000.0,
        dewpoint_c=None,
        h2o_mmol=None,
        raw_h2o=None,
    )
    logger = RunLogger(tmp_path)
    pace = _FakePaceVentAfterValveGetterFailure()
    runner = CalibrationRunner(cfg, {"pace": pace}, logger, lambda *_: None, lambda *_: None)
    runner._pace_vent_after_valve_supported = True
    runner._pace_vent_after_valve_open = True
    runner._pressure_atmosphere_hold_strategy = "vent_valve_open_after_vent"

    assert runner._set_pressure_to_target(point) is True
    logger.close()

    assert pace.calls == [
        ("vent_off", 12.0),
        ("setpoint", 1000.0),
        ("output_on",),
    ]
    trace_rows = _load_pressure_trace_rows(logger)
    assert not any(row["trace_stage"] == "control_vent_after_valve_closed" for row in trace_rows)
    assert not any(row["trace_stage"] == "control_vent_off_failed" for row in trace_rows)


def test_set_pressure_to_target_aborts_when_exit_atmosphere_fails(tmp_path: Path) -> None:
    cfg = {
        "workflow": {
            "pressure": {
                "vent_time_s": 0,
                "vent_transition_timeout_s": 12,
                "stabilize_timeout_s": 0.1,
                "abort_on_vent_off_failure": True,
            }
        }
    }
    point = CalibrationPoint(
        index=1,
        temp_chamber_c=20.0,
        co2_ppm=None,
        hgen_temp_c=20.0,
        hgen_rh_pct=30.0,
        target_pressure_hpa=1000.0,
        dewpoint_c=None,
        h2o_mmol=None,
        raw_h2o=None,
    )
    logger = RunLogger(tmp_path)
    pace = _FakePaceVentOffFailure()
    runner = CalibrationRunner(cfg, {"pace": pace}, logger, lambda *_: None, lambda *_: None)

    assert runner._set_pressure_to_target(point) is False
    logger.close()

    assert pace.calls == [
        ("vent_off", 12.0),
    ]
    trace_rows = _load_pressure_trace_rows(logger)
    assert any(row["trace_stage"] == "control_vent_off_failed" for row in trace_rows)


def test_set_pressure_to_target_aborts_when_atmosphere_hold_thread_lingers(tmp_path: Path) -> None:
    cfg = {
        "workflow": {
            "pressure": {
                "vent_time_s": 0,
                "vent_transition_timeout_s": 12,
                "stabilize_timeout_s": 0.1,
            }
        }
    }
    point = CalibrationPoint(
        index=1,
        temp_chamber_c=20.0,
        co2_ppm=None,
        hgen_temp_c=20.0,
        hgen_rh_pct=30.0,
        target_pressure_hpa=1000.0,
        dewpoint_c=None,
        h2o_mmol=None,
        raw_h2o=None,
    )
    logger = RunLogger(tmp_path)
    pace = _FakePaceLingeringHold()
    runner = CalibrationRunner(cfg, {"pace": pace}, logger, lambda *_: None, lambda *_: None)

    assert runner._set_pressure_to_target(point) is False
    logger.close()

    assert pace.calls == [
        ("stop_hold",),
    ]
    trace_rows = _load_pressure_trace_rows(logger)
    assert any(row["trace_stage"] == "control_vent_off_failed" for row in trace_rows)


def test_set_pressure_to_target_aborts_when_controller_not_ready_after_vent_off(tmp_path: Path) -> None:
    cfg = {
        "workflow": {
            "pressure": {
                "vent_time_s": 0,
                "vent_transition_timeout_s": 12,
                "stabilize_timeout_s": 0.1,
                "strict_control_ready_check": True,
            }
        }
    }
    point = CalibrationPoint(
        index=1,
        temp_chamber_c=20.0,
        co2_ppm=None,
        hgen_temp_c=20.0,
        hgen_rh_pct=30.0,
        target_pressure_hpa=1000.0,
        dewpoint_c=None,
        h2o_mmol=None,
        raw_h2o=None,
    )
    logger = RunLogger(tmp_path)
    pace = _FakePaceNotReady()
    runner = CalibrationRunner(cfg, {"pace": pace}, logger, lambda *_: None, lambda *_: None)

    assert runner._set_pressure_to_target(point) is False
    logger.close()

    assert pace.calls == [
        ("vent_off", 12.0),
        ("vent_off", 12.0),
    ]
    trace_rows = _load_pressure_trace_rows(logger)
    assert any(row["trace_stage"] == "control_ready_failed" for row in trace_rows)


def test_set_pressure_to_target_rejects_trapped_pressure_before_setpoint_control(tmp_path: Path) -> None:
    cfg = {
        "workflow": {
            "pressure": {
                "vent_time_s": 0,
                "vent_transition_timeout_s": 12,
                "stabilize_timeout_s": 0.1,
                "strict_control_ready_check": True,
            }
        }
    }
    point = CalibrationPoint(
        index=1,
        temp_chamber_c=20.0,
        co2_ppm=None,
        hgen_temp_c=20.0,
        hgen_rh_pct=30.0,
        target_pressure_hpa=1000.0,
        dewpoint_c=None,
        h2o_mmol=None,
        raw_h2o=None,
    )
    logger = RunLogger(tmp_path)
    pace = _FakePaceTrappedPressureReady()
    runner = CalibrationRunner(cfg, {"pace": pace}, logger, lambda *_: None, lambda *_: None)

    assert runner._set_pressure_to_target(point) is False
    logger.close()

    assert pace.calls == [
        ("vent_off", 12.0),
        ("vent_off", 12.0),
    ]
    trace_rows = _load_pressure_trace_rows(logger)
    assert any(row["trace_stage"] == "control_ready_failed" for row in trace_rows)
    assert not any(row["trace_stage"] == "pressure_control_wait" for row in trace_rows)


def test_set_pressure_to_target_rejects_legacy_watchlist_status_3_before_setpoint_control(tmp_path: Path) -> None:
    cfg = {
        "workflow": {
            "pressure": {
                "vent_time_s": 0,
                "vent_transition_timeout_s": 12,
                "stabilize_timeout_s": 0.1,
                "strict_control_ready_check": True,
            }
        }
    }
    point = CalibrationPoint(
        index=1,
        temp_chamber_c=20.0,
        co2_ppm=0.0,
        hgen_temp_c=20.0,
        hgen_rh_pct=30.0,
        target_pressure_hpa=500.0,
        dewpoint_c=None,
        h2o_mmol=None,
        raw_h2o=None,
    )
    logger = RunLogger(tmp_path)
    pace = _FakePaceLegacyVentTrapped()
    runner = CalibrationRunner(cfg, {"pace": pace}, logger, lambda *_: None, lambda *_: None)

    assert runner._set_pressure_to_target(point) is False
    logger.close()

    assert pace.calls == [
        ("vent_off", 12.0),
        ("vent_off", 12.0),
    ]
    state = runner._point_runtime_state(point, phase="h2o") or {}
    assert state["control_ready_check_phase"] == "after_full_seal"
    assert state["control_ready_check_watchlist_status_seen"] is True
    assert state["control_ready_check_watchlist_status_accepted"] is False
    assert state["control_ready_watchlist_status_accepted"] is False
    assert state["after_full_seal_watchlist_status_seen"] is True
    assert state["after_full_seal_watchlist_status_accepted"] is False
    assert "post_seal_recovery_enabled" in state["after_full_seal_watchlist_status_reason"]
    trace_rows = _load_pressure_trace_rows(logger)
    assert any(
        row["trace_stage"] == "control_ready_failed"
        and row["pace_vent_status"].strip() == "3"
        and row["pace_legacy_vent_state_3_suspect"].strip().lower() == "true"
        and row["pace_atmosphere_connected_latched_state_suspect"].strip().lower() == "true"
        and row["legacy_vent3_control_ready_used"].strip().lower() != "true"
        and row["legacy_vent3_accept_scope"].strip() == "none"
        and row["vent3_hard_blocked"].strip().lower() == "true"
        and row["vent3_control_ready_attempted"].strip().lower() == "true"
        and row["vent3_control_ready_prevented"].strip().lower() == "true"
        and row["vent3_block_scope"].strip() == "pressure_control_ready"
        for row in trace_rows
    )
    assert not any(row["trace_stage"] == "control_ready_verified" for row in trace_rows)
    assert not any(row["trace_stage"] == "control_output_on_verified" for row in trace_rows)


def test_set_pressure_to_target_rejects_output_recovery_while_vent_status_3_remains_watchlist_only(tmp_path: Path) -> None:
    cfg = {
        "workflow": {
            "pressure": {
                "vent_time_s": 0,
                "vent_transition_timeout_s": 12,
                "stabilize_timeout_s": 0.1,
                "output_recovery_settle_s": 0.0,
            }
        }
    }
    point = CalibrationPoint(
        index=1,
        temp_chamber_c=20.0,
        co2_ppm=None,
        hgen_temp_c=20.0,
        hgen_rh_pct=30.0,
        target_pressure_hpa=1000.0,
        dewpoint_c=None,
        h2o_mmol=None,
        raw_h2o=None,
    )
    logger = RunLogger(tmp_path)
    pace = _FakePaceOutputOnTrappedThenReady()
    pace._in_limits_sequence = [(1000.0, 1)]
    runner = CalibrationRunner(cfg, {"pace": pace}, logger, lambda *_: None, lambda *_: None)

    assert runner._set_pressure_to_target(point) is False
    logger.close()

    assert pace.calls == [
        ("vent_off", 12.0),
        ("setpoint", 1000.0),
        ("output_on", "first"),
        ("output", False),
    ]
    trace_rows = _load_pressure_trace_rows(logger)
    assert any(row["trace_stage"] == "control_output_on_failed" for row in trace_rows)
    assert not any(row["trace_stage"] == "pressure_control_wait" for row in trace_rows)


def test_set_pressure_to_target_blocks_output_recovery_when_legacy_watchlist_status_3_persists(tmp_path: Path) -> None:
    cfg = {
        "workflow": {
            "pressure": {
                "vent_time_s": 0,
                "vent_transition_timeout_s": 12,
                "stabilize_timeout_s": 0.1,
                "output_recovery_settle_s": 0.0,
            }
        }
    }
    point = CalibrationPoint(
        index=1,
        temp_chamber_c=20.0,
        co2_ppm=0.0,
        hgen_temp_c=20.0,
        hgen_rh_pct=30.0,
        target_pressure_hpa=500.0,
        dewpoint_c=None,
        h2o_mmol=None,
        raw_h2o=None,
    )
    logger = RunLogger(tmp_path)
    pace = _FakePaceLegacyOutputOnTrappedThenReady()
    pace._in_limits_sequence = [(500.0, 1)]
    runner = CalibrationRunner(cfg, {"pace": pace}, logger, lambda *_: None, lambda *_: None)

    assert runner._set_pressure_to_target(point) is False
    logger.close()

    assert pace.calls == [
        ("vent_off", 12.0),
        ("setpoint", 500.0),
        ("output_on", "first"),
        ("output", False),
    ]
    trace_rows = _load_pressure_trace_rows(logger)
    assert any(
        row["trace_stage"] == "control_output_on_failed"
        and row["pace_vent_status"].strip() == "3"
        and row["legacy_vent3_control_ready_used"].strip().lower() != "true"
        and row["legacy_vent3_accept_scope"].strip() == "none"
        and row["vent3_control_ready_attempted"].strip().lower() == "true"
        and row["vent3_control_ready_prevented"].strip().lower() == "true"
        and row["vent3_block_scope"].strip() == "output_on_verify"
        for row in trace_rows
    )


def test_set_pressure_to_target_aborts_when_output_on_verification_detects_vent_window(tmp_path: Path) -> None:
    cfg = {
        "workflow": {
            "pressure": {
                "vent_time_s": 0,
                "vent_transition_timeout_s": 12,
                "stabilize_timeout_s": 0.1,
                "strict_control_ready_check": True,
            }
        }
    }
    point = CalibrationPoint(
        index=1,
        temp_chamber_c=20.0,
        co2_ppm=0.0,
        hgen_temp_c=20.0,
        hgen_rh_pct=30.0,
        target_pressure_hpa=1000.0,
        dewpoint_c=None,
        h2o_mmol=None,
        raw_h2o=None,
    )
    logger = RunLogger(tmp_path)
    pace = _FakePaceOutputOnVentWindow()
    runner = CalibrationRunner(cfg, {"pace": pace}, logger, lambda *_: None, lambda *_: None)

    assert runner._set_pressure_to_target(point) is False
    logger.close()

    assert pace.calls == [
        ("vent_off", 12.0),
        ("setpoint", 1000.0),
        ("output_on",),
        ("output", False),
    ]
    trace_rows = _load_pressure_trace_rows(logger)
    assert any(row["trace_stage"] == "control_output_on_failed" for row in trace_rows)
    assert not any(row["trace_stage"] == "pressure_control_wait" for row in trace_rows)


def test_set_pressure_to_target_aborts_when_output_state_stays_off(tmp_path: Path) -> None:
    cfg = {
        "workflow": {
            "pressure": {
                "vent_time_s": 0,
                "vent_transition_timeout_s": 12,
                "stabilize_timeout_s": 0.1,
                "output_on_verify_timeout_s": 0.0,
            }
        }
    }
    point = CalibrationPoint(
        index=1,
        temp_chamber_c=20.0,
        co2_ppm=600.0,
        hgen_temp_c=20.0,
        hgen_rh_pct=30.0,
        target_pressure_hpa=1100.0,
        dewpoint_c=None,
        h2o_mmol=None,
        raw_h2o=None,
    )
    logger = RunLogger(tmp_path)
    pace = _FakePaceOutputOnNeverReady()
    runner = CalibrationRunner(cfg, {"pace": pace}, logger, lambda *_: None, lambda *_: None)

    assert runner._set_pressure_to_target(point) is False
    logger.close()

    assert pace.calls == [
        ("vent_off", 12.0),
        ("setpoint", 1100.0),
        ("output_on",),
        ("output", False),
    ]
    trace_rows = _load_pressure_trace_rows(logger)
    assert any(row["trace_stage"] == "control_output_on_failed" for row in trace_rows)
    assert not any(row["trace_stage"] == "pressure_control_wait" for row in trace_rows)


def test_set_pressure_to_target_rejects_completed_vent_latch_before_control(tmp_path: Path) -> None:
    cfg = {
        "workflow": {
            "pressure": {
                "vent_time_s": 0,
                "vent_transition_timeout_s": 12,
                "stabilize_timeout_s": 0.1,
                "strict_control_ready_check": True,
            }
        }
    }
    point = CalibrationPoint(
        index=1,
        temp_chamber_c=20.0,
        co2_ppm=600.0,
        hgen_temp_c=20.0,
        hgen_rh_pct=30.0,
        target_pressure_hpa=1100.0,
        dewpoint_c=None,
        h2o_mmol=None,
        raw_h2o=None,
    )
    logger = RunLogger(tmp_path)
    pace = _FakePaceLegacyVentCompleted()
    runner = CalibrationRunner(cfg, {"pace": pace}, logger, lambda *_: None, lambda *_: None)

    assert runner._set_pressure_to_target(point) is False
    logger.close()

    assert pace.calls == [
        ("vent_off", 12.0),
        ("vent_off", 12.0),
    ]
    trace_rows = _load_pressure_trace_rows(logger)
    assert any(row["trace_stage"] == "control_ready_failed" for row in trace_rows)


def test_clear_pressure_sequence_completed_vent_latch_returns_blocked_summary_for_legacy_status_2(
    tmp_path: Path,
) -> None:
    logger = RunLogger(tmp_path)
    pace = _FakePaceCompletedVentLatchBlocked()
    runner = CalibrationRunner({}, {"pace": pace}, logger, lambda *_: None, lambda *_: None)
    point = CalibrationPoint(
        index=1,
        temp_chamber_c=20.0,
        co2_ppm=600.0,
        hgen_temp_c=None,
        hgen_rh_pct=None,
        target_pressure_hpa=800.0,
        dewpoint_c=None,
        h2o_mmol=None,
        raw_h2o=None,
    )

    summary = runner._clear_pressure_sequence_completed_vent_latch_if_present(
        point,
        phase="co2",
        reason="unit test legacy guard",
    )
    logger.close()

    assert summary["status"] == "blocked"
    assert summary["reason"] == "legacy_completed_latch_auto_clear_blocked(before=2,after=2)"
    assert pace.calls == [
        ("clear_status",),
        ("drain_system_errors",),
        ("clear_completed_vent_latch_if_present", 5.0, 0.25),
    ]
    trace_rows = _load_pressure_trace_rows(logger)
    assert any(
        row["trace_stage"] == "pace_vent_clear_latch_end"
        and row["pace_vent_clear_attempted"].strip().lower() == "false"
        and row["pace_vent_clear_result"].strip() == "legacy_completed_latch_auto_clear_blocked(before=2,after=2)"
        for row in trace_rows
    )


def test_set_pressure_to_target_clears_completed_vent_latch_at_sequence_start(tmp_path: Path) -> None:
    cfg = {
        "workflow": {
            "pressure": {
                "vent_time_s": 0,
                "vent_transition_timeout_s": 12,
                "stabilize_timeout_s": 0.1,
            }
        }
    }
    point = CalibrationPoint(
        index=1,
        temp_chamber_c=20.0,
        co2_ppm=600.0,
        hgen_temp_c=20.0,
        hgen_rh_pct=30.0,
        target_pressure_hpa=1100.0,
        dewpoint_c=None,
        h2o_mmol=None,
        raw_h2o=None,
    )
    logger = RunLogger(tmp_path)
    pace = _FakePaceCompletedVentLatchClears()
    runner = CalibrationRunner(cfg, {"pace": pace}, logger, lambda *_: None, lambda *_: None)

    assert runner._set_pressure_to_target(point) is True
    logger.close()

    assert pace.calls == [
        ("clear_status",),
        ("drain_system_errors",),
        ("clear_completed_vent_latch_if_present", 5.0, 0.25),
        ("vent_off", 12.0),
        ("setpoint", 1100.0),
        ("output_on",),
    ]
    trace_rows = _load_pressure_trace_rows(logger)
    assert any(
        row["trace_stage"] == "pace_vent_clear_latch_end"
        and row["pace_vent_clear_attempted"].strip().lower() == "true"
        for row in trace_rows
    )
    assert any(
        row["trace_stage"] == "pace_vent_clear_latch_begin"
        and "status_clear=*CLS sent" in str(row["note"])
        and "Data out of range" in str(row["note"])
        for row in trace_rows
    )


def test_set_pressure_to_target_stops_when_sequence_start_auto_clear_is_blocked(tmp_path: Path) -> None:
    cfg = {
        "workflow": {
            "pressure": {
                "vent_time_s": 0,
                "vent_transition_timeout_s": 12,
                "stabilize_timeout_s": 0.1,
            }
        }
    }
    point = CalibrationPoint(
        index=1,
        temp_chamber_c=20.0,
        co2_ppm=600.0,
        hgen_temp_c=20.0,
        hgen_rh_pct=30.0,
        target_pressure_hpa=1100.0,
        dewpoint_c=None,
        h2o_mmol=None,
        raw_h2o=None,
    )
    logger = RunLogger(tmp_path)
    pace = _FakePaceCompletedVentLatchBlocked()
    runner = CalibrationRunner(cfg, {"pace": pace}, logger, lambda *_: None, lambda *_: None)

    assert runner._set_pressure_to_target(point) is False
    logger.close()

    assert pace.calls == [
        ("clear_status",),
        ("drain_system_errors",),
        ("clear_completed_vent_latch_if_present", 5.0, 0.25),
    ]
    trace_rows = _load_pressure_trace_rows(logger)
    assert any(
        row["trace_stage"] == "pace_vent_clear_latch_end"
        and row["pace_vent_clear_result"].strip() == "legacy_completed_latch_auto_clear_blocked(before=2,after=2)"
        for row in trace_rows
    )
    assert not any(row["trace_stage"] == "control_vent_off_begin" for row in trace_rows)
    assert not any(row["trace_stage"] == "pressure_control_wait" for row in trace_rows)


def test_set_pressure_controller_vent_off_blocks_legacy_completed_latch_auto_clear(tmp_path: Path) -> None:
    cfg = {
        "workflow": {
            "pressure": {
                "vent_time_s": 0,
                "vent_transition_timeout_s": 12,
            }
        }
    }
    logger = RunLogger(tmp_path)
    pace = _FakePaceCompletedVentLatchBlocked()
    runner = CalibrationRunner(cfg, {"pace": pace}, logger, lambda *_: None, lambda *_: None)

    with pytest.raises(RuntimeError, match="VENT_COMPLETED_LATCH_AUTO_CLEAR_BLOCKED"):
        runner._set_pressure_controller_vent(False, reason="before setpoint control")
    logger.close()

    assert not any(call[0] in {"vent_off", "vent", "output", "isol"} for call in pace.calls)
    trace_rows = _load_pressure_trace_rows(logger)
    assert any(
        row["trace_stage"] == "control_vent_off_blocked"
        and row["pace_vent_status_query"].strip() == "2"
        and row["pace_vent_clear_result"].strip() == "legacy_completed_latch_auto_clear_blocked(before=2,after=2)"
        for row in trace_rows
    )


def test_set_pressure_controller_vent_off_uses_adapter_ready_semantics_for_legacy_completed_status(
    tmp_path: Path,
) -> None:
    cfg = {
        "workflow": {
            "pressure": {
                "vent_time_s": 0,
                "vent_transition_timeout_s": 12,
            }
        }
    }
    logger = RunLogger(tmp_path)
    pace = _FakePaceLegacyBaselineReuse()
    runner = CalibrationRunner(cfg, {"pace": pace}, logger, lambda *_: None, lambda *_: None)

    assert runner._set_pressure_controller_vent(False, reason="before setpoint control") is True
    logger.close()

    assert pace.calls == [("vent_off", 12.0)]
    trace_rows = _load_pressure_trace_rows(logger)
    assert not any(row["trace_stage"] == "control_vent_off_blocked" for row in trace_rows)


def test_force_clear_completed_vent_latch_is_blocked_when_status_is_trapped_but_oper_bit_is_still_set(
    tmp_path: Path,
) -> None:
    logger = RunLogger(tmp_path)
    pace = _FakePaceCompletedVentLatchBitOnly()
    runner = CalibrationRunner({}, {"pace": pace}, logger, lambda *_: None, lambda *_: None)
    point = CalibrationPoint(
        index=1,
        temp_chamber_c=20.0,
        co2_ppm=600.0,
        hgen_temp_c=None,
        hgen_rh_pct=None,
        target_pressure_hpa=800.0,
        dewpoint_c=None,
        h2o_mmol=None,
        raw_h2o=None,
    )
    snapshots = iter(
        [
            {
                "pace_outp_state_query": 0,
                "pace_isol_state_query": 1,
                "pace_mode_query": "ACT",
                "pace_vent_status_query": 3,
                "pace_oper_pres_cond_query": 1,
                "pace_oper_pres_even_query": 0,
                "pace_oper_pres_vent_complete_bit": True,
                "pace_oper_pres_in_limits_bit": False,
            },
            {
                "pace_outp_state_query": 0,
                "pace_isol_state_query": 1,
                "pace_mode_query": "ACT",
                "pace_vent_status_query": 3,
                "pace_oper_pres_cond_query": 0,
                "pace_oper_pres_even_query": 0,
                "pace_oper_pres_vent_complete_bit": False,
                "pace_oper_pres_in_limits_bit": False,
            },
            {
                "pace_outp_state_query": 0,
                "pace_isol_state_query": 1,
                "pace_mode_query": "ACT",
                "pace_vent_status_query": 3,
                "pace_oper_pres_cond_query": 0,
                "pace_oper_pres_even_query": 0,
                "pace_oper_pres_vent_complete_bit": False,
                "pace_oper_pres_in_limits_bit": False,
            },
        ]
    )

    runner._pace_diagnostic_state_snapshot = lambda *args, **kwargs: dict(next(snapshots))

    summary = runner._clear_pressure_sequence_completed_vent_latch_if_present(
        point,
        phase="co2",
        reason="unit test forced clear",
    )
    logger.close()

    assert summary["status"] == "blocked"
    assert summary["manual_intervention_required"] is True
    assert summary["reason"] == "legacy_completed_latch_bit_only_force_clear_blocked(before=3,cond=1,event=0)"
    assert summary["after"]["pace_vent_status_query"] == 3
    assert summary["after"]["pace_vent_completed_latched"] is True
    assert pace.calls == [
        ("clear_status",),
        ("drain_system_errors",),
    ]
    assert not any(call == ("vent", False) for call in pace.calls)


def test_set_pressure_to_target_stops_when_sequence_start_bit_only_force_clear_is_blocked(
    tmp_path: Path,
) -> None:
    cfg = {
        "workflow": {
            "pressure": {
                "vent_time_s": 0,
                "vent_transition_timeout_s": 12,
                "stabilize_timeout_s": 0.1,
            }
        }
    }
    logger = RunLogger(tmp_path)
    pace = _FakePaceCompletedVentLatchBitOnly()
    runner = CalibrationRunner(cfg, {"pace": pace}, logger, lambda *_: None, lambda *_: None)
    point = CalibrationPoint(
        index=1,
        temp_chamber_c=20.0,
        co2_ppm=600.0,
        hgen_temp_c=20.0,
        hgen_rh_pct=30.0,
        target_pressure_hpa=1100.0,
        dewpoint_c=None,
        h2o_mmol=None,
        raw_h2o=None,
    )
    blocked_snapshot = {
        # This blocked state is consumed by both the sampling handoff trace snapshot
        # and the sequence-start vent-latch clear diagnostic query.
        "pace_outp_state_query": 0,
        "pace_isol_state_query": 1,
        "pace_mode_query": "ACT",
        "pace_vent_status_query": 3,
        "pace_oper_pres_cond_query": 1,
        "pace_oper_pres_even_query": 0,
        "pace_oper_pres_vent_complete_bit": True,
        "pace_oper_pres_in_limits_bit": False,
    }
    snapshot_calls = []
    stage_events = []
    logged_messages = []

    def fake_snapshot(*args, **kwargs):
        snapshot_calls.append((args, kwargs))
        return dict(blocked_snapshot)

    runner._pace_diagnostic_state_snapshot = fake_snapshot
    runner._emit_stage_event = lambda **kwargs: stage_events.append(dict(kwargs))
    runner.log = lambda message: logged_messages.append(str(message))

    assert runner._set_pressure_to_target(point) is False
    logger.close()

    assert pace.calls == [
        ("clear_status",),
        ("drain_system_errors",),
    ]
    assert len(snapshot_calls) >= 2
    assert any(call_kwargs.get("refresh") is True for _, call_kwargs in snapshot_calls)
    assert not any(call[0] in {"vent_off", "vent", "setpoint", "output", "output_on", "isol"} for call in pace.calls)
    assert not any(event.get("wait_reason") == "控压中" for event in stage_events)
    assert any(
        "legacy_completed_latch_bit_only_force_clear_blocked(before=3,cond=1,event=0)" in message
        for message in logged_messages
    )
    assert any("manual intervention required" in message for message in logged_messages)


def test_set_pressure_controller_vent_off_for_preseal_skips_slow_exit_wait(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    pace = _FakePaceSlowExitForPreseal()
    runner = CalibrationRunner({}, {"pace": pace}, logger, lambda *_: None, lambda *_: None)

    assert runner._set_pressure_controller_vent(False, reason="before CO2 pressure seal") is True
    logger.close()

    assert pace.calls == [
        ("output", False),
        ("vent", False),
        ("isol", True),
    ]


def test_set_pressure_to_target_reapplies_setpoint_when_not_stable(tmp_path: Path) -> None:
    cfg = {
        "workflow": {
            "pressure": {
                "vent_time_s": 0,
                "vent_transition_timeout_s": 12,
                "stabilize_timeout_s": 2.5,
                "restabilize_retries": 2,
                "restabilize_retry_interval_s": 0.01,
            }
        }
    }
    point = CalibrationPoint(
        index=1,
        temp_chamber_c=20.0,
        co2_ppm=0.0,
        hgen_temp_c=20.0,
        hgen_rh_pct=30.0,
        target_pressure_hpa=1000.0,
        dewpoint_c=None,
        h2o_mmol=None,
        raw_h2o=None,
    )
    logger = RunLogger(tmp_path)
    pace = _FakePace()
    pace._in_limits_sequence = [
        (995.0, 0),
        (996.0, 0),
        (997.0, 0),
        (1000.0, 1),
    ]
    runner = CalibrationRunner(cfg, {"pace": pace}, logger, lambda *_: None, lambda *_: None)

    assert runner._set_pressure_to_target(point) is True
    logger.close()

    assert pace.calls == [
        ("vent_off", 12.0),
        ("setpoint", 1000.0),
        ("output_on",),
        ("setpoint", 1000.0),
        ("output_on",),
        ("setpoint", 1000.0),
        ("output_on",),
    ]


def test_set_pressure_to_target_writes_pressure_control_trace(tmp_path: Path) -> None:
    class _FakeGauge:
        def read_pressure(self):
            return 1000.6

    class _FakeDew:
        def get_current(self):
            return {"dewpoint_c": -10.5, "temp_c": 23.0, "rh_pct": 40.0}

    cfg = {
        "workflow": {
            "pressure": {
                "vent_time_s": 0,
                "vent_transition_timeout_s": 12,
                "stabilize_timeout_s": 2.5,
                "restabilize_retries": 0,
                "transition_trace_enabled": True,
                "transition_trace_poll_s": 0.5,
            }
        }
    }
    point = CalibrationPoint(
        index=1,
        temp_chamber_c=20.0,
        co2_ppm=0.0,
        hgen_temp_c=20.0,
        hgen_rh_pct=30.0,
        target_pressure_hpa=1000.0,
        dewpoint_c=None,
        h2o_mmol=None,
        raw_h2o=None,
    )
    logger = RunLogger(tmp_path)
    pace = _FakePace()
    pace._in_limits_sequence = [
        (998.0, 0),
        (999.5, 0),
        (1000.0, 1),
    ]
    runner = CalibrationRunner(
        cfg,
        {"pace": pace, "pressure_gauge": _FakeGauge(), "dewpoint": _FakeDew()},
        logger,
        lambda *_: None,
        lambda *_: None,
    )

    assert runner._set_pressure_to_target(point) is True
    logger.close()

    trace_rows = _load_pressure_trace_rows(logger)
    wait_rows = [row for row in trace_rows if row["trace_stage"] == "pressure_control_wait"]
    in_limit_rows = [row for row in trace_rows if row["trace_stage"] == "pressure_in_limits"]
    assert len(wait_rows) == 3
    assert len(in_limit_rows) == 1
    assert wait_rows[0]["note"] == "pace_in_limits=0"
    assert in_limit_rows[0]["note"] == "pace_in_limits=1"
    assert float(in_limit_rows[0]["pace_pressure_hpa"]) == 1000.0
    assert float(in_limit_rows[0]["pressure_gauge_hpa"]) == 1000.6


def test_set_pressure_to_target_prepared_h2o_does_not_reapply_setpoint(tmp_path: Path) -> None:
    cfg = {
        "workflow": {
            "pressure": {
                "vent_time_s": 0,
                "vent_transition_timeout_s": 12,
                "stabilize_timeout_s": 1.5,
                "restabilize_retries": 2,
                "restabilize_retry_interval_s": 0.01,
            }
        }
    }
    point = CalibrationPoint(
        index=1,
        temp_chamber_c=20.0,
        co2_ppm=None,
        hgen_temp_c=20.0,
        hgen_rh_pct=30.0,
        target_pressure_hpa=1000.0,
        dewpoint_c=None,
        h2o_mmol=None,
        raw_h2o=None,
    )
    logger = RunLogger(tmp_path)
    pace = _FakePace()
    pace._in_limits_sequence = [
        (999.0, 0),
        (1000.0, 1),
    ]
    runner = CalibrationRunner(cfg, {"pace": pace}, logger, lambda *_: None, lambda *_: None)
    runner._h2o_pressure_prepared_target = 1000.0

    assert runner._set_pressure_to_target(point) is True
    logger.close()

    assert pace.calls == [
        ("output_on",),
    ]


def test_set_pressure_to_target_fallback_keeps_output_path_open(tmp_path: Path) -> None:
    cfg = {
        "workflow": {
            "pressure": {
                "vent_time_s": 0,
                "vent_transition_timeout_s": 12,
                "stabilize_timeout_s": 0.1,
            }
        }
    }
    point = CalibrationPoint(
        index=1,
        temp_chamber_c=20.0,
        co2_ppm=None,
        hgen_temp_c=20.0,
        hgen_rh_pct=30.0,
        target_pressure_hpa=1000.0,
        dewpoint_c=None,
        h2o_mmol=None,
        raw_h2o=None,
    )
    logger = RunLogger(tmp_path)
    pace = _FakePaceFallback()
    runner = CalibrationRunner(cfg, {"pace": pace}, logger, lambda *_: None, lambda *_: None)

    assert runner._set_pressure_to_target(point) is True
    logger.close()

    assert pace.calls == [
        ("output", False),
        ("vent", False),
        ("isol", True),
        ("setpoint", 1000.0),
        ("output", True),
    ]


def test_atmosphere_refresh_is_disabled_after_vent_off(tmp_path: Path) -> None:
    cfg = _fresh_vent_cfg()
    cfg["workflow"]["pressure"]["vent_hold_interval_s"] = 0.0
    logger = RunLogger(tmp_path)
    pace = _FakePaceSingleCycleVent()
    runner = CalibrationRunner(cfg, {"pace": pace}, logger, lambda *_: None, lambda *_: None)

    runner._set_pressure_controller_vent(True, reason="enable hold")
    calls_after_on = list(pace.calls)
    runner._set_pressure_controller_vent(False, reason="disable hold")
    calls_after_off = list(pace.calls)
    runner._refresh_pressure_controller_atmosphere_hold(force=True, reason="should be ignored")
    logger.close()

    assert calls_after_on == [
        ("output", False),
        ("isol", True),
        ("vent", True),
    ]
    assert calls_after_off == [
        ("output", False),
        ("isol", True),
        ("vent", True),
        ("vent_off", 12.0),
    ]
    assert pace.calls == calls_after_off


def test_set_pressure_to_target_soft_recovers_and_retries_once(tmp_path: Path) -> None:
    cfg = {
        "devices": {
            "pressure_controller": {
                "in_limits_pct": 0.02,
                "in_limits_time_s": 10,
            }
        },
        "workflow": {
            "pressure": {
                "vent_time_s": 0,
                "vent_transition_timeout_s": 12,
                "stabilize_timeout_s": 0.6,
                "restabilize_retries": 0,
                "restabilize_retry_interval_s": 0.01,
                "soft_recover_on_pressure_timeout": True,
                "soft_recover_reopen_delay_s": 0.0,
            }
        }
    }
    point = CalibrationPoint(
        index=1,
        temp_chamber_c=20.0,
        co2_ppm=0.0,
        hgen_temp_c=None,
        hgen_rh_pct=None,
        target_pressure_hpa=550.0,
        dewpoint_c=None,
        h2o_mmol=None,
        raw_h2o=None,
    )
    logger = RunLogger(tmp_path)
    pace = _FakePaceSoftRecover()
    runner = CalibrationRunner(cfg, {"pace": pace}, logger, lambda *_: None, lambda *_: None)

    original_recover = runner._soft_recover_pressure_controller

    def wrapped_recover(**kwargs):
        result = original_recover(**kwargs)
        pace.phase = "second"
        return result

    runner._soft_recover_pressure_controller = wrapped_recover

    assert runner._set_pressure_to_target(point) is True
    logger.close()

    assert ("close",) in pace.calls
    assert ("open",) in pace.calls
    assert ("set_in_limits", 0.02, 10.0) in pace.calls
    assert ("setpoint", 550.0, "first") in pace.calls
    assert ("setpoint", 550.0, "second") in pace.calls


def test_set_pressure_to_target_same_route_follow_on_timeout_skips_atmosphere_reset_recovery(
    tmp_path: Path,
) -> None:
    cfg = {
        "workflow": {
            "pressure": {
                "vent_time_s": 0,
                "vent_transition_timeout_s": 12,
                "stabilize_timeout_s": 0.11,
                "restabilize_retries": 0,
                "restabilize_retry_interval_s": 0.01,
                "soft_recover_on_pressure_timeout": True,
            }
        }
    }
    previous_point = CalibrationPoint(
        index=1,
        temp_chamber_c=20.0,
        co2_ppm=800.0,
        hgen_temp_c=None,
        hgen_rh_pct=None,
        target_pressure_hpa=1000.0,
        dewpoint_c=None,
        h2o_mmol=None,
        raw_h2o=None,
    )
    follow_on_point = CalibrationPoint(
        index=2,
        temp_chamber_c=20.0,
        co2_ppm=800.0,
        hgen_temp_c=None,
        hgen_rh_pct=None,
        target_pressure_hpa=800.0,
        dewpoint_c=None,
        h2o_mmol=None,
        raw_h2o=None,
    )
    logger = RunLogger(tmp_path)
    pace = _FakePaceSoftRecover()
    runner = CalibrationRunner(cfg, {"pace": pace}, logger, lambda *_: None, lambda *_: None)
    runner._last_sealed_pressure_route_context = {
        "phase": "co2",
        "route_signature": runner._route_signature_for_point(previous_point, phase="co2"),
        "point_row": previous_point.index,
    }

    recover_called = False

    def forbidden_recover(**kwargs):
        nonlocal recover_called
        recover_called = True
        return False

    runner._soft_recover_pressure_controller = forbidden_recover

    assert runner._set_pressure_to_target(follow_on_point) is False
    logger.close()

    assert recover_called is False
    assert ("vent", True) not in pace.calls
    trace_rows = _load_pressure_trace_rows(logger)
    assert any(
        row["trace_stage"] == "control_timeout_recovery_skipped"
        and row["handoff_mode"] == "same_gas_pressure_step_handoff"
        and "skip atmosphere-reset soft recovery after timeout" in row["note"]
        for row in trace_rows
    )
