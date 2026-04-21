import csv
from pathlib import Path

import pytest

from gas_calibrator.data.points import CalibrationPoint
from gas_calibrator.logging_utils import RunLogger
from gas_calibrator.workflow.runner import CalibrationRunner


class _FakeRelay:
    def __init__(self):
        self.states = {}

    def set_valve(self, channel, open_):
        self.states[int(channel)] = bool(open_)

    def read_coils(self, _start=0, count=8):
        return [self.states.get(i + 1, False) for i in range(count)]

    def close(self):
        return None


class _FakePace:
    def __init__(self):
        self.vent_calls = []
        self.output_calls = []
        self._vent_status = 0
        self._output_state = 0
        self._isolation_state = 1
        self._vent_query_count = 0

    def vent(self, on=True):
        self.vent_calls.append(bool(on))
        self._vent_status = 1 if on else 0
        self._vent_query_count = 0

    def set_output(self, on):
        self.output_calls.append(bool(on))
        self._output_state = 1 if on else 0

    def set_isolation_open(self, is_open):
        self._isolation_state = 1 if is_open else 0

    def get_output_state(self):
        return self._output_state

    def get_isolation_state(self):
        return self._isolation_state

    def get_vent_status(self):
        if self._vent_status != 1:
            return self._vent_status
        self._vent_query_count += 1
        if self._vent_query_count >= 2:
            self._vent_status = 0
        return self._vent_status

    def read_pressure(self):
        return 1000.0

    def close(self):
        return None


class _FakePaceManual:
    def __init__(self):
        self.calls = []

    def enter_atmosphere_mode(self, timeout_s=0.0):
        self.calls.append(("vent_on", float(timeout_s)))

    def exit_atmosphere_mode(self, timeout_s=0.0):
        self.calls.append(("vent_off", float(timeout_s)))

    def read_pressure(self):
        return 1000.0

    def close(self):
        return None


class _FakePressureGauge:
    def __init__(self, values):
        self.values = list(values)

    def read_pressure(self):
        if self.values:
            value = float(self.values.pop(0))
            self._last = value
            return value
        return float(getattr(self, "_last", 1000.0))


class _FakePacePrecheck:
    def __init__(self):
        self.calls = []
        self.setpoints = []
        self._vent_status = 1
        self._output_state = 0
        self._isolation_state = 1

    def enter_atmosphere_mode(self, timeout_s=0.0, **kwargs):
        self._vent_status = 1
        self._output_state = 0
        self.calls.append(("vent_on", float(timeout_s), bool(kwargs.get("hold_open", False))))

    def exit_atmosphere_mode(self, timeout_s=0.0):
        self._vent_status = 0
        self.calls.append(("vent_off", float(timeout_s)))

    def set_output(self, on):
        self._output_state = 1 if on else 0
        self.calls.append(("output", bool(on)))

    def vent(self, on=True):
        self._vent_status = 1 if on else 0
        self.calls.append(("vent", bool(on)))

    def set_isolation_open(self, is_open):
        self._isolation_state = 1 if is_open else 0
        self.calls.append(("isolation", bool(is_open)))

    def enable_control_output(self):
        self._output_state = 1
        self.calls.append(("output_on",))

    def set_setpoint(self, value):
        self.setpoints.append(float(value))
        self.calls.append(("setpoint", float(value)))

    def get_in_limits(self):
        target = self.setpoints[-1] if self.setpoints else 1000.0
        return float(target), 1

    def read_pressure(self):
        return float(self.setpoints[-1] if self.setpoints else 1000.0)

    def get_output_state(self):
        return self._output_state

    def get_isolation_state(self):
        return self._isolation_state

    def get_vent_status(self):
        return self._vent_status

    def close(self):
        return None


class _FakePaceStartupSingleCycleBlocked:
    def __init__(self):
        self.calls = []
        self._output_state = 0
        self._isolation_state = 1
        self._vent_status = 0
        self._single_cycle_active = False
        self._single_cycle_clear_requested = False
        self._single_cycle_query_count = 0

    def set_output(self, on):
        self._output_state = 1 if on else 0
        self.calls.append(("output", bool(on)))

    def set_isolation_open(self, is_open):
        self._isolation_state = 1 if is_open else 0
        self.calls.append(("isol", bool(is_open)))

    def vent(self, on=True):
        self.calls.append(("vent", bool(on)))
        if on:
            self._single_cycle_active = True
            self._single_cycle_clear_requested = False
            self._single_cycle_query_count = 0
            self._vent_status = 1
            return
        self._single_cycle_clear_requested = True
        self._vent_status = 0

    def get_output_state(self):
        return self._output_state

    def get_isolation_state(self):
        return self._isolation_state

    def get_vent_status(self):
        if not self._single_cycle_active:
            return self._vent_status
        self._single_cycle_query_count += 1
        if self._single_cycle_clear_requested:
            self._vent_status = 0
            self._single_cycle_active = False
            return 0
        if self._single_cycle_query_count <= 2:
            self._vent_status = 1
            return 1
        self._vent_status = 2
        return 2

    def has_legacy_vent_state_3_compatibility(self):
        return False

    def detect_profile(self):
        return "OLD_PACE5000"

    def get_device_identity(self):
        return '*IDN GE Druck,Pace5000 User Interface,3213201,02.00.07'

    def get_instrument_version(self):
        return ':INST:VERS "02.00.07"'

    def read_pressure(self):
        return 1000.0

    def close(self):
        return None


def _load_pressure_trace_rows(logger: RunLogger):
    path = logger.run_dir / "pressure_transition_trace.csv"
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def test_startup_preflight_resets_valves_and_pressure(tmp_path: Path) -> None:
    cfg = {
        "valves": {
            "co2_path": 7,
            "co2_path_group2": 16,
            "gas_main": 11,
            "h2o_path": 8,
            "flow_switch": 10,
            "hold": 9,
            "relay_map": {
                "1": {"device": "relay", "channel": 7},
                "2": {"device": "relay", "channel": 8},
                "3": {"device": "relay", "channel": 9},
                "4": {"device": "relay", "channel": 10},
                "5": {"device": "relay", "channel": 11},
                "6": {"device": "relay", "channel": 12},
                "7": {"device": "relay", "channel": 15},
                "8": {"device": "relay_8", "channel": 8},
                "9": {"device": "relay_8", "channel": 1},
                "10": {"device": "relay_8", "channel": 2},
                "11": {"device": "relay_8", "channel": 3},
                "16": {"device": "relay", "channel": 16},
                "21": {"device": "relay", "channel": 6},
                "22": {"device": "relay", "channel": 5},
                "23": {"device": "relay", "channel": 4},
                "24": {"device": "relay", "channel": 3},
                "25": {"device": "relay", "channel": 2},
                "26": {"device": "relay", "channel": 1},
            },
            "co2_map": {"0": 1, "200": 2, "400": 3, "600": 4, "800": 5, "1000": 6},
            "co2_map_group2": {"0": 21, "100": 22, "300": 23, "500": 24, "700": 25, "900": 26},
        },
        "workflow": {"pressure": {"vent_time_s": 0}},
    }
    logger = RunLogger(tmp_path)
    relay = _FakeRelay()
    relay8 = _FakeRelay()
    pace = _FakePace()
    runner = CalibrationRunner(
        cfg,
        {"relay": relay, "relay_8": relay8, "pace": pace},
        logger,
        lambda *_: None,
        lambda *_: None,
    )

    runner._h2o_pressure_prepared_target = 1000.0
    runner._startup_preflight_reset()
    logger.close()

    assert all(v is False for v in relay.states.values())
    assert relay8.states.get(1) is False
    assert relay8.states.get(2) is False
    assert relay8.states.get(3) is False
    assert relay8.states.get(8) is False
    assert pace.vent_calls and pace.vent_calls[-1] is True
    assert pace.output_calls and pace.output_calls[-1] is False
    assert runner._h2o_pressure_prepared_target is None


def test_startup_preflight_reset_accepts_legacy_completed_status_without_clear_before_first_point(
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
    pace = _FakePaceStartupSingleCycleBlocked()
    runner = CalibrationRunner(
        cfg,
        {"pace": pace},
        logger,
        lambda *_: None,
        lambda *_: None,
    )

    runner._startup_preflight_reset()
    logger.close()

    assert pace.calls == [
        ("output", False),
        ("isol", True),
        ("vent", True),
    ]
    assert not any(call == ("vent", False) for call in pace.calls)
    trace_rows = _load_pressure_trace_rows(logger)
    assert any(
        row["trace_stage"] == "atmosphere_vent_completed"
        and row["pace_vent_status_query"].strip() == "2"
        and row["pace_vent_clear_result"].strip() == "legacy_completed_latch_observed_ready_without_clear"
        for row in trace_rows
    )
    assert not any(row["trace_stage"] == "atmosphere_vent_clear_command" for row in trace_rows)
    assert any(row["trace_stage"] == "atmosphere_enter_verified" for row in trace_rows)


def test_set_pressure_controller_vent_prefers_manual_driver_helpers(tmp_path: Path) -> None:
    cfg = {"workflow": {"pressure": {"vent_time_s": 0, "vent_transition_timeout_s": 12}}}
    logger = RunLogger(tmp_path)
    pace = _FakePaceManual()
    runner = CalibrationRunner(
        cfg,
        {"pace": pace},
        logger,
        lambda *_: None,
        lambda *_: None,
    )

    runner._set_pressure_controller_vent(True, reason="test on")
    runner._set_pressure_controller_vent(False, reason="test off")
    logger.close()

    assert pace.calls == [("vent_on", 12.0), ("vent_off", 12.0)]


def test_startup_pressure_precheck_passes_and_restores_baseline(tmp_path: Path) -> None:
    cfg = {
        "valves": {
            "co2_path": 7,
            "gas_main": 11,
            "h2o_path": 8,
            "flow_switch": 10,
            "hold": 9,
            "relay_map": {
                "1": {"device": "relay", "channel": 1},
                "7": {"device": "relay", "channel": 7},
                "8": {"device": "relay_8", "channel": 8},
                "9": {"device": "relay_8", "channel": 1},
                "10": {"device": "relay_8", "channel": 2},
                "11": {"device": "relay", "channel": 11},
            },
            "co2_map": {"0": 1},
        },
        "workflow": {
            "pressure": {
                "vent_time_s": 0,
                "vent_transition_timeout_s": 0,
                "pressurize_wait_after_vent_off_s": 0,
                "pressurize_high_hpa": 1000,
                "pressurize_timeout_s": 0.1,
                "stabilize_timeout_s": 0.1,
                "restabilize_retries": 0,
            },
            "startup_pressure_precheck": {
                "enabled": True,
                "route": "co2",
                "route_soak_s": 0,
                "hold_s": 0.02,
                "sample_interval_s": 0.01,
                "max_abs_drift_hpa": 1.0,
                "prefer_gauge": True,
                "strict": True,
            },
        },
    }
    logger = RunLogger(tmp_path)
    relay = _FakeRelay()
    relay8 = _FakeRelay()
    pace = _FakePacePrecheck()
    gauge = _FakePressureGauge([1000.0, 1000.3, 1000.5])
    runner = CalibrationRunner(
        cfg,
        {"relay": relay, "relay_8": relay8, "pace": pace, "pressure_gauge": gauge},
        logger,
        lambda *_: None,
        lambda *_: None,
    )
    runner._source_stage_safety["co2_a"] = True
    events = []

    point = CalibrationPoint(
        index=21,
        temp_chamber_c=20.0,
        co2_ppm=0.0,
        hgen_temp_c=None,
        hgen_rh_pct=None,
        target_pressure_hpa=1000.0,
        dewpoint_c=None,
        h2o_mmol=None,
        raw_h2o=None,
    )

    runner._set_co2_route_baseline = lambda reason="": events.append(("baseline", reason))  # type: ignore[method-assign]
    runner._open_route_with_pressure_guard = lambda *args, **kwargs: events.append((  # type: ignore[method-assign]
        "guarded_route_open",
        kwargs.get("phase"),
        kwargs.get("open_valves"),
    )) or True
    runner._pressurize_route_for_sealed_points = lambda one_point, route="co2", sealed_control_refs=None: events.append(  # type: ignore[method-assign]
        ("seal", route, one_point.target_pressure_hpa)
    ) or True
    runner._pressurize_and_hold = lambda one_point, route="co2": events.append(("pressurize", route, one_point.target_pressure_hpa)) or True  # type: ignore[method-assign]
    runner._set_pressure_to_target = lambda one_point: events.append(("stabilize", one_point.target_pressure_hpa)) or True  # type: ignore[method-assign]
    runner._observe_startup_pressure_hold = lambda _cfg: (  # type: ignore[method-assign]
        True,
        {
            "source": "pressure_gauge",
            "start_hpa": 1000.0,
            "end_hpa": 1000.5,
            "max_abs_drift_hpa": 0.5,
            "span_hpa": 0.5,
            "samples": 3,
            "limit_hpa": 1.0,
        },
    )
    runner._cleanup_co2_route = lambda reason="": events.append(("cleanup", reason))  # type: ignore[method-assign]

    runner._startup_pressure_precheck([point])
    logger.close()

    assert ("baseline", "before startup pressure precheck") in events
    assert any(event[0] == "guarded_route_open" and event[1] == "co2" for event in events)
    assert ("seal", "co2", 1000.0) in events
    assert ("stabilize", 1000.0) in events
    assert ("cleanup", "after startup pressure precheck") in events


def test_startup_pressure_precheck_raises_when_hold_drift_exceeds_limit(tmp_path: Path) -> None:
    cfg = {
        "valves": {
            "co2_path": 7,
            "gas_main": 11,
            "h2o_path": 8,
            "flow_switch": 10,
            "hold": 9,
            "relay_map": {
                "1": {"device": "relay", "channel": 1},
                "7": {"device": "relay", "channel": 7},
                "8": {"device": "relay_8", "channel": 8},
                "9": {"device": "relay_8", "channel": 1},
                "10": {"device": "relay_8", "channel": 2},
                "11": {"device": "relay", "channel": 11},
            },
            "co2_map": {"0": 1},
        },
        "workflow": {
            "pressure": {
                "vent_time_s": 0,
                "vent_transition_timeout_s": 0,
                "pressurize_wait_after_vent_off_s": 0,
                "stabilize_timeout_s": 0.1,
                "restabilize_retries": 0,
            },
            "startup_pressure_precheck": {
                "enabled": True,
                "route": "co2",
                "route_soak_s": 0,
                "hold_s": 0.02,
                "sample_interval_s": 0.01,
                "max_abs_drift_hpa": 1.0,
                "prefer_gauge": True,
                "strict": True,
            },
        },
    }
    logger = RunLogger(tmp_path)
    relay = _FakeRelay()
    relay8 = _FakeRelay()
    pace = _FakePacePrecheck()
    gauge = _FakePressureGauge([1000.0, 1002.2, 1003.1])
    runner = CalibrationRunner(
        cfg,
        {"relay": relay, "relay_8": relay8, "pace": pace, "pressure_gauge": gauge},
        logger,
        lambda *_: None,
        lambda *_: None,
    )
    runner._source_stage_safety["co2_a"] = True
    events = []

    point = CalibrationPoint(
        index=21,
        temp_chamber_c=20.0,
        co2_ppm=0.0,
        hgen_temp_c=None,
        hgen_rh_pct=None,
        target_pressure_hpa=1000.0,
        dewpoint_c=None,
        h2o_mmol=None,
        raw_h2o=None,
    )

    runner._set_co2_route_baseline = lambda reason="": events.append(("baseline", reason))  # type: ignore[method-assign]
    runner._open_route_with_pressure_guard = lambda *args, **kwargs: events.append((  # type: ignore[method-assign]
        "guarded_route_open",
        kwargs.get("phase"),
        kwargs.get("open_valves"),
    )) or True
    runner._pressurize_and_hold = lambda one_point, route="co2": events.append(("pressurize", route, one_point.target_pressure_hpa)) or True  # type: ignore[method-assign]
    runner._set_pressure_to_target = lambda one_point: events.append(("stabilize", one_point.target_pressure_hpa)) or True  # type: ignore[method-assign]
    runner._observe_startup_pressure_hold = lambda _cfg: (  # type: ignore[method-assign]
        False,
        {
            "source": "pressure_gauge",
            "start_hpa": 1000.0,
            "end_hpa": 1003.1,
            "max_abs_drift_hpa": 3.1,
            "span_hpa": 3.1,
            "samples": 3,
            "limit_hpa": 1.0,
        },
    )
    runner._cleanup_co2_route = lambda reason="": events.append(("cleanup", reason))  # type: ignore[method-assign]

    raised = False
    try:
        runner._startup_pressure_precheck([point])
    except RuntimeError:
        raised = True
    finally:
        logger.close()

    assert raised is True
    assert ("cleanup", "after startup pressure precheck") in events


def test_startup_pressure_precheck_point_skips_when_only_ambient_selected(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(
        {
            "workflow": {
                "selected_pressure_points": ["ambient"],
                "startup_pressure_precheck": {"enabled": True, "route": "co2"},
            }
        },
        {},
        logger,
        lambda *_: None,
        lambda *_: None,
    )
    point = CalibrationPoint(
        index=21,
        temp_chamber_c=20.0,
        co2_ppm=0.0,
        hgen_temp_c=None,
        hgen_rh_pct=None,
        target_pressure_hpa=1000.0,
        dewpoint_c=None,
        h2o_mmol=None,
        raw_h2o=None,
    )

    try:
        assert runner._startup_pressure_precheck_point([point], route="co2") is None
    finally:
        logger.close()


def test_startup_pressure_precheck_does_not_bypass_route_guard(tmp_path: Path) -> None:
    cfg = {
        "valves": {
            "co2_path": 7,
            "gas_main": 11,
            "h2o_path": 8,
            "relay_map": {},
            "co2_map": {"0": 4},
        },
        "workflow": {
            "pressure": {
                "route_open_guard_enabled": True,
            },
            "startup_pressure_precheck": {
                "enabled": True,
                "route": "co2",
                "route_soak_s": 0,
                "hold_s": 0,
                "strict": True,
            },
        },
    }
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(cfg, {}, logger, lambda *_: None, lambda *_: None)
    runner._source_stage_safety["co2_a"] = True
    point = CalibrationPoint(
        index=21,
        temp_chamber_c=20.0,
        co2_ppm=0.0,
        hgen_temp_c=None,
        hgen_rh_pct=None,
        target_pressure_hpa=1000.0,
        dewpoint_c=None,
        h2o_mmol=None,
        raw_h2o=None,
    )
    calls: list[tuple[str, object]] = []
    runner._set_valves_for_co2 = lambda _point: (_ for _ in ()).throw(AssertionError("direct CO2 route open must not be used"))  # type: ignore[method-assign]
    runner._open_route_with_pressure_guard = lambda *args, **kwargs: calls.append(("guard", kwargs.get("open_valves"))) or True  # type: ignore[method-assign]
    runner._pressurize_route_for_sealed_points = lambda *_args, **_kwargs: calls.append(("seal", None)) or True  # type: ignore[method-assign]
    runner._set_pressure_to_target = lambda *_args, **_kwargs: True  # type: ignore[method-assign]
    runner._observe_startup_pressure_hold = lambda _cfg: (True, {"source": "pressure_gauge", "start_hpa": 1000.0, "end_hpa": 1000.0, "max_abs_drift_hpa": 0.0, "span_hpa": 0.0, "samples": 1, "limit_hpa": 1.0})  # type: ignore[method-assign]
    runner._cleanup_co2_route = lambda reason="": calls.append(("cleanup", reason))  # type: ignore[method-assign]

    runner._startup_pressure_precheck([point])
    logger.close()

    assert any(call[0] == "guard" for call in calls)


def test_startup_pressure_sensor_calibration_does_not_bypass_route_guard(tmp_path: Path) -> None:
    cfg = {
        "valves": {
            "co2_path": 7,
            "gas_main": 11,
            "h2o_path": 8,
            "relay_map": {},
            "co2_map": {"0": 4},
        },
        "workflow": {
            "pressure": {
                "route_open_guard_enabled": True,
            },
            "startup_pressure_sensor_calibration": {
                "enabled": True,
                "target_hpa": 1000.0,
                "flush_soak_s": 0.0,
                "strict": True,
            },
        },
    }
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(cfg, {}, logger, lambda *_: None, lambda *_: None)
    point = CalibrationPoint(
        index=21,
        temp_chamber_c=20.0,
        co2_ppm=0.0,
        hgen_temp_c=None,
        hgen_rh_pct=None,
        target_pressure_hpa=1000.0,
        dewpoint_c=None,
        h2o_mmol=None,
        raw_h2o=None,
    )
    calls: list[list[int]] = []
    runner._startup_pressure_sensor_calibration_point = lambda _points: point  # type: ignore[method-assign]
    runner._active_gas_analyzers = lambda: [("GA01", object(), {})]  # type: ignore[method-assign]
    runner._set_valves_for_co2 = lambda _point: (_ for _ in ()).throw(AssertionError("direct CO2 route open must not be used"))  # type: ignore[method-assign]

    def _guarded_open(*_args, **kwargs):
        calls.append(list(kwargs.get("open_valves") or []))
        raise RuntimeError("guarded stop")

    runner._open_route_with_pressure_guard = _guarded_open  # type: ignore[method-assign]

    with pytest.raises(RuntimeError, match="guarded stop"):
        runner._startup_pressure_sensor_calibration([point])
    logger.close()

    assert calls == [[8, 11, 7]]
