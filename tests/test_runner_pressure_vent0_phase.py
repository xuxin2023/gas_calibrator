import csv
import json
from pathlib import Path

import pytest

from gas_calibrator.data.points import CalibrationPoint
from gas_calibrator.logging_utils import RunLogger
from gas_calibrator.workflow.runner import CalibrationRunner


class _FakePaceVentContext:
    def __init__(
        self,
        *,
        output_state: int = 0,
        isolation_state: int = 1,
        vent_status: int = 1,
        in_limits_sequence=None,
    ) -> None:
        self.calls = []
        self.output_state = int(output_state)
        self.isolation_state = int(isolation_state)
        self.vent_status = int(vent_status)
        self._in_limits_sequence = list(in_limits_sequence or [])

    def stop_atmosphere_hold(self):
        self.calls.append(("stop_hold",))
        return True

    def clear_status(self):
        self.calls.append(("clear_status",))

    def drain_system_errors(self):
        self.calls.append(("drain_system_errors",))
        return []

    def clear_completed_vent_latch_if_present(self, timeout_s=5.0, poll_s=0.25):
        self.calls.append(("clear_completed_vent_latch_if_present", float(timeout_s), float(poll_s)))
        return {
            "before_status": self.vent_status,
            "clear_attempted": False,
            "after_status": self.vent_status,
            "cleared": self.vent_status == 0,
        }

    def get_output_state(self):
        return self.output_state

    def get_isolation_state(self):
        return self.isolation_state

    def get_vent_status(self):
        return self.vent_status

    def set_output(self, on):
        self.calls.append(("output", bool(on)))
        self.output_state = 1 if on else 0

    def set_isolation_open(self, is_open):
        self.calls.append(("isol", bool(is_open)))
        self.isolation_state = 1 if is_open else 0

    def vent(self, on=True):
        self.calls.append(("vent", bool(on)))
        self.vent_status = 1 if on else 0

    def exit_atmosphere_mode(self, timeout_s=0.0):
        self.calls.append(("vent_off", float(timeout_s)))
        self.output_state = 0
        self.isolation_state = 1
        self.vent_status = 0

    def set_setpoint(self, value):
        self.calls.append(("setpoint", float(value)))

    def enable_control_output(self):
        self.calls.append(("output_on",))
        self.output_state = 1

    def get_in_limits(self):
        if self._in_limits_sequence:
            return self._in_limits_sequence.pop(0)
        return 500.0, 1

    def has_legacy_vent_state_3_compatibility(self):
        return False


def _co2_point(target_pressure_hpa: float, *, index: int) -> CalibrationPoint:
    return CalibrationPoint(
        index=index,
        temp_chamber_c=20.0,
        co2_ppm=400.0,
        hgen_temp_c=None,
        hgen_rh_pct=None,
        target_pressure_hpa=float(target_pressure_hpa),
        dewpoint_c=None,
        h2o_mmol=None,
        raw_h2o=None,
    )


def _load_pressure_trace_rows(logger: RunLogger):
    path = logger.run_dir / "pressure_transition_trace.csv"
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def _vent_rows_with_context(logger: RunLogger):
    rows = []
    for row in _load_pressure_trace_rows(logger):
        payload = str(row.get("vent_context_json") or "").strip()
        if not payload:
            continue
        row = dict(row)
        row["vent_context"] = json.loads(payload)
        rows.append(row)
    return rows


def test_exit_atmosphere_allows_single_vent0_before_seal(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    pace = _FakePaceVentContext(output_state=0, isolation_state=1, vent_status=1)
    runner = CalibrationRunner(
        {"workflow": {"pressure": {"vent_time_s": 0, "vent_transition_timeout_s": 12}}},
        {"pace": pace},
        logger,
        lambda *_: None,
        lambda *_: None,
    )
    point = _co2_point(1000.0, index=1)

    assert runner._set_pressure_controller_vent(
        False,
        reason="before CO2 pressure seal",
        point=point,
        phase="co2",
        helper_name="_pressurize_and_hold",
        vent_phase_label="ExitAtmosphereBoundary",
        before_seal_transition_start=True,
        before_pressure_hold_start=True,
        before_sour_pres_command=True,
        before_outp_stat_1_command=True,
        vent_command_reason="ensure_vent_closed_before_seal",
    ) is True
    logger.close()

    vent_rows = [row for row in _vent_rows_with_context(logger) if row["trace_stage"] == "pressure_vent0_command"]
    assert len(vent_rows) == 1
    context = vent_rows[0]["vent_context"]
    assert context["command"] == ":SOUR:PRES:LEV:IMM:AMPL:VENT 0"
    assert context["reason"] == "ensure_vent_closed_before_seal"
    assert context["current_phase_before_command"] == "ExitAtmosphereBoundary"
    assert context["current_phase_after_command"] == "ExitAtmosphereBoundary"
    assert context["whether_before_seal_transition_start"] is True
    assert context["whether_before_pressure_hold_start"] is True
    assert context["whether_before_sour_pres"] is True
    assert context["whether_before_outp_stat_1"] is True
    assert context["route_flow_active"] is False
    assert context["continuous_atmosphere_active"] is False
    assert context["atmosphere_keepalive_enabled"] is False
    assert context["keepalive_generation_valid"] is True
    assert context["is_exit_atmosphere_boundary_abort"] is True
    assert context["is_pressure_hold_phase_leak"] is False
    assert ("vent", False) in pace.calls


def test_pressure_hold_rejects_vent0_after_hold_start(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    pace = _FakePaceVentContext(output_state=0, isolation_state=1, vent_status=1)
    runner = CalibrationRunner(
        {"workflow": {"pressure": {"vent_time_s": 0, "vent_transition_timeout_s": 12}}},
        {"pace": pace},
        logger,
        lambda *_: None,
        lambda *_: None,
    )
    point = _co2_point(500.0, index=2)
    runner._activate_sealed_no_vent_guard(
        point=point,
        phase="co2",
        guard_phase="PressureSetpointHold",
        reason="unit hold start",
    )

    with pytest.raises(RuntimeError, match="PressureHoldVentAbortLeak"):
        runner._set_pressure_controller_vent(
            False,
            reason="forbidden during pressure hold",
            point=point,
            phase="co2",
            helper_name="_set_pressure_to_target",
            vent_phase_label="PressureSetpointHold",
            before_seal_transition_start=False,
            before_pressure_hold_start=False,
            before_sour_pres_command=False,
            before_outp_stat_1_command=False,
            vent_command_reason="hold_phase_vent_off",
        )
    logger.close()

    blocked_rows = [row for row in _vent_rows_with_context(logger) if row["trace_stage"] == "pressure_vent0_blocked"]
    assert len(blocked_rows) == 1
    context = blocked_rows[0]["vent_context"]
    assert context["abort_reason"] == "PressureHoldVentAbortLeak"
    assert context["blocked"] is True
    assert context["command_sent"] is False
    assert context["sealed_no_vent_guard_phase"] == "PressureSetpointHold"
    assert context["is_pressure_hold_phase_leak"] is True
    assert not any(call in {("vent_off", 12.0), ("vent", False)} for call in pace.calls)


def test_pressure_point_switch_does_not_send_vent0_between_1000_and_500(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    pace = _FakePaceVentContext(
        output_state=0,
        isolation_state=0,
        vent_status=0,
        in_limits_sequence=[(500.0, 1)],
    )
    runner = CalibrationRunner(
        {"workflow": {"pressure": {"vent_time_s": 0, "vent_transition_timeout_s": 12, "stabilize_timeout_s": 0.1}}},
        {"pace": pace},
        logger,
        lambda *_: None,
        lambda *_: None,
    )
    previous_point = _co2_point(1000.0, index=1)
    next_point = _co2_point(500.0, index=2)
    runner._last_sealed_pressure_route_context = {
        "phase": "co2",
        "route_signature": runner._route_signature_for_point(previous_point, phase="co2"),
        "point_row": previous_point.index,
    }

    assert runner._set_pressure_to_target(next_point) is True
    logger.close()

    vent_rows = _vent_rows_with_context(logger)
    assert not any(row["trace_stage"] == "pressure_vent0_command" for row in vent_rows)
    noop_rows = [row for row in vent_rows if row["trace_stage"] == "pressure_vent0_not_needed"]
    assert len(noop_rows) == 1
    context = noop_rows[0]["vent_context"]
    assert context["current_phase_before_command"] == "PressurePointSwitch"
    assert context["command_sent"] is False
    assert context["vent_status_before_command"] == 0
    assert ("vent", False) not in pace.calls
    assert ("vent_off", 12.0) not in pace.calls
    assert ("isol", True) in pace.calls
    assert ("setpoint", 500.0) in pace.calls
    assert ("output_on",) in pace.calls


def test_post_exit_vent0_phase_attribution(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    pace = _FakePaceVentContext(
        output_state=0,
        isolation_state=1,
        vent_status=1,
        in_limits_sequence=[(500.0, 1)],
    )
    runner = CalibrationRunner(
        {"workflow": {"pressure": {"vent_time_s": 0, "vent_transition_timeout_s": 12, "stabilize_timeout_s": 0.1}}},
        {"pace": pace},
        logger,
        lambda *_: None,
        lambda *_: None,
    )
    first_point = _co2_point(1000.0, index=1)
    next_point = _co2_point(500.0, index=2)

    assert runner._set_pressure_controller_vent(
        False,
        reason="before CO2 pressure seal",
        point=first_point,
        phase="co2",
        helper_name="_pressurize_and_hold",
        vent_phase_label="ExitAtmosphereBoundary",
        before_seal_transition_start=True,
        before_pressure_hold_start=True,
        before_sour_pres_command=True,
        before_outp_stat_1_command=True,
        vent_command_reason="ensure_vent_closed_before_seal",
    ) is True

    pace.output_state = 0
    pace.isolation_state = 0
    pace.vent_status = 0
    runner._last_sealed_pressure_route_context = {
        "phase": "co2",
        "route_signature": runner._route_signature_for_point(first_point, phase="co2"),
        "point_row": first_point.index,
    }

    assert runner._set_pressure_to_target(next_point) is True
    logger.close()

    command_rows = [row for row in _vent_rows_with_context(logger) if row["trace_stage"] == "pressure_vent0_command"]
    assert len(command_rows) == 1
    assert all(
        row["vent_context"]["current_phase_before_command"] == "ExitAtmosphereBoundary"
        for row in command_rows
    )
    assert not any(
        row["vent_context"]["current_phase_before_command"] in {"PressureSetpointHold", "PressurePointSwitch"}
        for row in command_rows
    )
