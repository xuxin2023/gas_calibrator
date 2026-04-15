import csv
from pathlib import Path

import pytest

from gas_calibrator.data.points import CalibrationPoint
from gas_calibrator.logging_utils import RunLogger
from gas_calibrator.workflow import runner as runner_module
from gas_calibrator.workflow.runner import CalibrationRunner


class _GaugeSequence:
    def __init__(self, values):
        self.values = list(values)
        self.last = None

    def read_pressure(self):
        if self.values:
            self.last = float(self.values.pop(0))
        if self.last is None:
            self.last = 1006.0
        return float(self.last)


class _FakePaceForSuperambient:
    def __init__(self, in_limits_sequence=None):
        self.calls = []
        self.output_state = 0
        self.isolation_state = 0
        self.vent_status = 0
        self._in_limits_sequence = list(in_limits_sequence or [(1099.8, 0), (1100.0, 1)])

    def set_output_enabled_verified(self, enabled):
        self.calls.append(("output_verified", bool(enabled)))
        self.output_state = 1 if enabled else 0

    def set_output_isolated_verified(self, isolated):
        self.calls.append(("isolated_verified", bool(isolated)))
        self.isolation_state = 0 if isolated else 1

    def set_setpoint(self, value):
        self.calls.append(("setpoint", float(value)))

    def enable_control_output(self):
        self.calls.append(("output_on",))
        self.output_state = 1

    def get_in_limits(self):
        if self._in_limits_sequence:
            return self._in_limits_sequence.pop(0)
        return 1100.0, 1

    def get_output_state(self):
        return self.output_state

    def get_isolation_state(self):
        return self.isolation_state

    def get_vent_status(self):
        return self.vent_status


class _FakePaceForSuperambientWithBarometer(_FakePaceForSuperambient):
    def get_barometric_pressure(self):
        return 1006.5


def _co2_point(pressure_hpa: float = 1100.0, *, index: int = 1) -> CalibrationPoint:
    return CalibrationPoint(
        index=index,
        temp_chamber_c=20.0,
        co2_ppm=800.0,
        hgen_temp_c=None,
        hgen_rh_pct=None,
        target_pressure_hpa=pressure_hpa,
        dewpoint_c=None,
        h2o_mmol=None,
        raw_h2o=None,
    )


def _load_trace_rows(logger: RunLogger):
    path = logger.run_dir / "pressure_transition_trace.csv"
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def _load_timing_rows(logger: RunLogger):
    path = logger.run_dir / "point_timing_summary.csv"
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def _runner_cfg(include_hold: bool = True):
    valves = {
        "co2_path": 7,
        "gas_main": 11,
        "h2o_path": 8,
        "co2_map": {"800": 12},
    }
    if include_hold:
        valves["hold"] = 9
    return {
        "workflow": {
            "pressure": {
                "superambient_precharge_enabled": True,
                "superambient_trigger_margin_hpa": 5.0,
                "superambient_precharge_margin_hpa": 8.0,
                "superambient_precharge_timeout_s": 1.0,
                "superambient_precharge_same_gas_only": True,
                "superambient_reject_without_closed_path": True,
                "superambient_forbid_atmosphere_fallback": True,
                "stabilize_timeout_s": 1.0,
                "transition_trace_enabled": True,
                "transition_trace_poll_s": 0.05,
            }
        },
        "valves": valves,
    }


def _configure_superambient_runner(
    tmp_path: Path,
    *,
    gauge_values,
    include_hold: bool = True,
    ambient_reference_hpa=1006.0,
):
    logger = RunLogger(tmp_path)
    pace = _FakePaceForSuperambient()
    gauge = _GaugeSequence(gauge_values)
    runner = CalibrationRunner(
        _runner_cfg(include_hold=include_hold),
        {"pace": pace, "pressure_gauge": gauge},
        logger,
        lambda *_: None,
        lambda *_: None,
    )
    point = _co2_point()
    valve_calls = []
    vent_calls = []
    runner._apply_valve_states = lambda open_valves: valve_calls.append(list(open_valves))
    runner._set_pressure_controller_vent = lambda vent_on, reason="": vent_calls.append((bool(vent_on), str(reason))) or True
    runner._ensure_pressure_controller_ready_for_control = lambda *_args, **_kwargs: True
    runner._verify_pressure_controller_output_on = lambda *_args, **_kwargs: True
    runner._refresh_pressure_controller_atmosphere_hold = lambda **_kwargs: (_ for _ in ()).throw(
        AssertionError("continuous atmosphere hold refresh must not run for same-gas superambient precharge")
    )
    runner._atmosphere_reference_hpa = ambient_reference_hpa
    if ambient_reference_hpa is not None:
        runner._record_pressure_sequence_context(point, phase="co2", reason="unit test sequence")
    else:
        runner._active_pressure_sequence_context = {
            "phase": "co2",
            "route_signature": runner._route_signature_for_point(point, phase="co2"),
            "ambient_reference_hpa": None,
        }
        runner._pressure_sequence_ambient_reference_hpa = None
    return logger, runner, point, pace, valve_calls, vent_calls


def test_same_gas_superambient_target_uses_closed_precharge_then_fine_trim(tmp_path: Path) -> None:
    logger, runner, point, pace, valve_calls, vent_calls = _configure_superambient_runner(
        tmp_path,
        gauge_values=[1007.0, 1106.5, 1108.2],
    )

    assert runner._set_pressure_to_target(point) is True
    runner._write_point_timing_summary(point, phase="co2")
    logger.close()

    runtime_state = runner._point_runtime_state(point, phase="co2") or {}
    assert runtime_state["handoff_mode"] == "same_gas_superambient_precharge_handoff"
    assert runtime_state["ambient_reference_hpa"] == 1007.0
    assert runtime_state["superambient_target_hpa"] == 1100.0
    assert runtime_state["superambient_precharge_margin_hpa"] == 8.0
    assert runtime_state["superambient_precharge_peak_hpa"] == pytest.approx(1108.2)
    assert runtime_state["superambient_precharge_result"] == "pass"
    assert runtime_state["superambient_closed_path_verified"] is True
    assert valve_calls[0] == [7, 8, 9, 11, 12]
    assert valve_calls[1] == []
    assert vent_calls == [(False, "before same-gas superambient precharge")]
    assert ("output_verified", False) in pace.calls
    assert ("isolated_verified", True) in pace.calls
    assert ("setpoint", 1100.0) in pace.calls
    assert ("output_on",) in pace.calls

    trace_rows = _load_trace_rows(logger)
    stages = [row["trace_stage"] for row in trace_rows]
    assert "superambient_precharge_begin" in stages
    assert "superambient_precharge_end" in stages
    assert "superambient_fine_trim_begin" in stages
    assert "superambient_fine_trim_end" in stages
    assert "route_open" not in stages
    assert "atmosphere_enter_begin" not in stages
    assert "atmosphere_enter_verified" not in stages
    precharge_end = next(row for row in trace_rows if row["trace_stage"] == "superambient_precharge_end")
    assert precharge_end["handoff_mode"] == "same_gas_superambient_precharge_handoff"
    assert precharge_end["superambient_precharge_peak_hpa"] == "1108.2"
    assert precharge_end["superambient_precharge_result"] == "pass"
    assert precharge_end["superambient_closed_path_verified"] == "True"

    timing_rows = _load_timing_rows(logger)
    assert len(timing_rows) == 1
    assert timing_rows[0]["handoff_mode"] == "same_gas_superambient_precharge_handoff"
    assert timing_rows[0]["ambient_reference_hpa"] == "1007.0"
    assert timing_rows[0]["superambient_precharge_result"] == "pass"
    assert timing_rows[0]["superambient_precharge_begin_ts"] != ""
    assert timing_rows[0]["superambient_precharge_end_ts"] != ""
    assert timing_rows[0]["superambient_fine_trim_begin_ts"] != ""
    assert timing_rows[0]["superambient_fine_trim_end_ts"] != ""


def test_same_gas_superambient_rejects_without_closed_precharge_path(tmp_path: Path) -> None:
    logger, runner, point, _pace, _valve_calls, _vent_calls = _configure_superambient_runner(
        tmp_path,
        gauge_values=[1007.0, 1107.0],
        include_hold=False,
    )

    assert runner._set_pressure_to_target(point) is False
    logger.close()

    runtime_state = runner._point_runtime_state(point, phase="co2") or {}
    assert runtime_state["handoff_mode"] == "same_gas_superambient_precharge_handoff"
    assert runtime_state["root_cause_reject_reason"] == "superambient_precharge_unavailable"
    assert runtime_state["superambient_precharge_result"] == "superambient_precharge_unavailable"
    trace_rows = _load_trace_rows(logger)
    assert any(row["trace_stage"] == "superambient_precharge_end" for row in trace_rows)


def test_same_gas_superambient_rejects_when_ambient_reference_missing(tmp_path: Path) -> None:
    logger, runner, point, _pace, _valve_calls, _vent_calls = _configure_superambient_runner(
        tmp_path,
        gauge_values=[1107.0],
        ambient_reference_hpa=None,
    )

    assert runner._set_pressure_to_target(point) is False
    logger.close()

    runtime_state = runner._point_runtime_state(point, phase="co2") or {}
    assert runtime_state["root_cause_reject_reason"] == "ambient_reference_unavailable"
    assert runtime_state["superambient_precharge_result"] == "ambient_reference_unavailable"


def test_same_gas_superambient_rejects_when_precharge_times_out(monkeypatch, tmp_path: Path) -> None:
    logger, runner, point, _pace, valve_calls, _vent_calls = _configure_superambient_runner(
        tmp_path,
        gauge_values=[1007.0, 1007.2, 1007.1, 1007.3, 1007.2],
    )

    clock = {"now": 100.0}
    monkeypatch.setattr(runner_module.time, "time", lambda: clock["now"])
    monkeypatch.setattr(
        runner_module.time,
        "sleep",
        lambda seconds: clock.__setitem__("now", clock["now"] + max(0.05, float(seconds))),
    )

    assert runner._set_pressure_to_target(point) is False
    logger.close()

    runtime_state = runner._point_runtime_state(point, phase="co2") or {}
    assert runtime_state["root_cause_reject_reason"] == "superambient_precharge_timeout"
    assert runtime_state["superambient_precharge_result"] == "superambient_precharge_timeout"
    assert valve_calls[-1] == []


def test_same_gas_non_superambient_target_does_not_switch_to_superambient_handoff(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    pace = _FakePaceForSuperambient(in_limits_sequence=[(999.8, 0), (1000.0, 1)])
    runner = CalibrationRunner(
        _runner_cfg(include_hold=True),
        {"pace": pace, "pressure_gauge": _GaugeSequence([1000.0])},
        logger,
        lambda *_: None,
        lambda *_: None,
    )
    point = _co2_point(pressure_hpa=1000.0, index=1)
    follow_on_point = _co2_point(pressure_hpa=800.0, index=2)
    runner._set_pressure_controller_vent = lambda vent_on, reason="": True
    runner._ensure_pressure_controller_ready_for_control = lambda *_args, **_kwargs: True
    runner._verify_pressure_controller_output_on = lambda *_args, **_kwargs: True
    runner._atmosphere_reference_hpa = 1006.0
    runner._record_pressure_sequence_context(point, phase="co2", reason="unit test non-superambient")

    assert runner._set_pressure_to_target(point) is True
    runner._last_sealed_pressure_route_context = {
        "phase": "co2",
        "route_signature": runner._route_signature_for_point(point, phase="co2"),
        "point_row": point.index,
    }
    logger.close()

    runtime_state = runner._point_runtime_state(point, phase="co2") or {}
    assert runtime_state.get("handoff_mode") not in {"same_gas_superambient_precharge_handoff"}
    assert runner._prepare_sampling_handoff_mode(follow_on_point, phase="co2") == "same_gas_pressure_step_handoff"


def test_pressure_sequence_context_uses_pace_barometer_when_gauge_unavailable(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    pace = _FakePaceForSuperambientWithBarometer()
    runner = CalibrationRunner(
        _runner_cfg(include_hold=True),
        {"pace": pace},
        logger,
        lambda *_: None,
        lambda *_: None,
    )
    point = _co2_point()

    ambient_reference_hpa = runner._record_pressure_sequence_context(point, phase="co2", reason="barometer fallback")
    logger.close()

    assert ambient_reference_hpa == pytest.approx(1006.5)
    assert runner._pressure_sequence_ambient_reference_hpa == pytest.approx(1006.5)
