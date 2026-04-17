from pathlib import Path

import pytest
import csv

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

    def has_legacy_vent_state_3_compatibility(self):
        return False


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
        self.vent_status = 2
        return 2

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
        return int(status) in {0, 2, 3}

    def enable_control_output(self):
        self.calls.append(("output_on",))
        self.output_state = 1
        self.vent_status = 3


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
        return int(status) in {0, 2, 3}

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
        self.vent_status = 2
        return 2


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


class _FakePaceVentAfterValveGetterFailure(_FakePace):
    def get_vent_after_valve_open(self):
        raise RuntimeError("NO_RESPONSE")


def _load_pressure_trace_rows(logger: RunLogger):
    path = logger.run_dir / "pressure_transition_trace.csv"
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def test_set_pressure_controller_vent_on_uses_single_cycle_query_clear_by_default(tmp_path: Path) -> None:
    cfg = {
        "workflow": {
            "pressure": {
                "vent_time_s": 0,
                "vent_transition_timeout_s": 12,
            }
        }
    }
    logger = RunLogger(tmp_path)
    pace = _FakePaceSingleCycleVent()
    runner = CalibrationRunner(cfg, {"pace": pace}, logger, lambda *_: None, lambda *_: None)

    runner._set_pressure_controller_vent(True, reason="test hold")
    logger.close()

    assert pace.calls == [
        ("output", False),
        ("isol", True),
        ("vent", True),
        ("vent", False),
    ]
    trace_rows = _load_pressure_trace_rows(logger)
    assert any(
        row["trace_stage"] == "atmosphere_hold_strategy_selected"
        and row["atmosphere_hold_strategy"] == "single_cycle_query_clear"
        for row in trace_rows
    )
    assert any(row["trace_stage"] == "atmosphere_vent_in_progress" for row in trace_rows)
    assert any(row["trace_stage"] == "atmosphere_vent_completed" for row in trace_rows)
    assert any(row["trace_stage"] == "atmosphere_vent_clear" for row in trace_rows)


def test_set_pressure_controller_vent_on_normalizes_legacy_open_valve_strategy_to_single_cycle_query_clear(
    tmp_path: Path,
) -> None:
    cfg = {
        "workflow": {
            "pressure": {
                "vent_time_s": 0,
                "vent_transition_timeout_s": 12,
                "atmosphere_hold_strategy": "vent_valve_open_after_vent",
                "vent_after_valve_open": True,
            }
        }
    }
    logger = RunLogger(tmp_path)
    pace = _FakePaceSingleCycleVent()
    runner = CalibrationRunner(cfg, {"pace": pace}, logger, lambda *_: None, lambda *_: None)

    runner._set_pressure_controller_vent(True, reason="test hold")
    logger.close()

    assert pace.calls == [
        ("output", False),
        ("isol", True),
        ("vent", True),
        ("vent", False),
    ]
    trace_rows = _load_pressure_trace_rows(logger)
    assert any(
        row["trace_stage"] == "atmosphere_hold_strategy_selected"
        and row["atmosphere_hold_strategy"] == "single_cycle_query_clear"
        for row in trace_rows
    )
    assert any(row["trace_stage"] == "atmosphere_vent_clear_command" for row in trace_rows)


def test_set_pressure_controller_vent_on_does_not_depend_on_open_valve_extension_support(tmp_path: Path) -> None:
    cfg = {
        "workflow": {
            "pressure": {
                "vent_time_s": 0,
                "vent_transition_timeout_s": 12,
                "atmosphere_hold_strategy": "vent_valve_open_after_vent",
                "vent_after_valve_open": True,
            }
        }
    }
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
        ("vent", False),
    ]
    assert not any("fallback -> legacy hold thread" in message for message in messages)
    trace_rows = _load_pressure_trace_rows(logger)
    assert not any(row["trace_stage"] == "atmosphere_hold_legacy_fallback" for row in trace_rows)
    assert any(row["trace_stage"] == "atmosphere_enter_verified" for row in trace_rows)


def test_set_pressure_controller_vent_on_blocks_legacy_trapped_pressure_pending_ack(tmp_path: Path) -> None:
    cfg = {
        "workflow": {
            "pressure": {
                "vent_time_s": 0,
                "vent_transition_timeout_s": 12,
            }
        }
    }
    logger = RunLogger(tmp_path)
    pace = _FakePaceSingleCycleVentClearsToTrapped()
    runner = CalibrationRunner(cfg, {"pace": pace}, logger, lambda *_: None, lambda *_: None)

    with pytest.raises(RuntimeError, match="VENT_CLEAR_PENDING_ACK"):
        runner._set_pressure_controller_vent(True, reason="legacy trapped after clear")
    logger.close()

    assert pace.calls == [
        ("output", False),
        ("isol", True),
        ("vent", True),
        ("vent", False),
    ]
    trace_rows = _load_pressure_trace_rows(logger)
    assert any(
        row["trace_stage"] == "atmosphere_vent_clear"
        and row["pace_vent_status_query"].strip() == "3"
        and row["pace_vent_clear_result"].strip() == "pending_acknowledgement"
        and row["legacy_vent3_control_ready_used"].strip().lower() != "true"
        and row["legacy_vent3_accept_scope"].strip() == "none"
        and row["vent3_ui_ack_required"].strip().lower() == "true"
        and row["vent3_block_scope"].strip() == "vent_clear_pending_ack"
        for row in trace_rows
    )
    assert not any(row["trace_stage"] == "atmosphere_enter_verified" for row in trace_rows)


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


def test_set_pressure_to_target_rejects_legacy_trapped_pressure_before_setpoint_control(tmp_path: Path) -> None:
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


def test_set_pressure_to_target_rejects_output_recovery_while_still_trapped(tmp_path: Path) -> None:
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


def test_set_pressure_to_target_blocks_output_recovery_when_legacy_trapped_ready(tmp_path: Path) -> None:
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


def test_force_clear_completed_vent_latch_when_status_is_trapped_but_oper_bit_is_still_set(tmp_path: Path) -> None:
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

    assert summary["status"] == "applied"
    assert summary["reason"] == "clear_attempted_pending_ack(before=3,after=3)"
    assert pace.calls == [
        ("clear_status",),
        ("drain_system_errors",),
        ("vent", False),
    ]


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
    cfg = {
        "workflow": {
            "pressure": {
                "vent_time_s": 0,
                "vent_transition_timeout_s": 12,
                "vent_hold_interval_s": 0.0,
            }
        }
    }
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
        ("vent", False),
    ]
    assert calls_after_off == [
        ("output", False),
        ("isol", True),
        ("vent", True),
        ("vent", False),
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
    assert ("units_hpa",) in pace.calls
    assert ("set_in_limits", 0.02, 10.0) in pace.calls
    assert ("setpoint", 550.0, "first") in pace.calls
    assert ("setpoint", 550.0, "second") in pace.calls
