from pathlib import Path

from gas_calibrator.data.points import CalibrationPoint
from gas_calibrator.logging_utils import RunLogger
from gas_calibrator.workflow.runner import CalibrationRunner


class _FakeRelay:
    def __init__(self):
        self.states = {}
        self.bulk_calls = []
        self.single_calls = []

    def set_valve(self, channel, open_):
        self.single_calls.append((int(channel), bool(open_)))
        self.states[int(channel)] = bool(open_)

    def set_valves_bulk(self, updates):
        normalized = list(updates)
        self.bulk_calls.append(normalized)
        for channel, state in normalized:
            self.states[int(channel)] = bool(state)

    def close(self):
        return None


class _FakePace:
    def __init__(self):
        self.setpoints = []

    def vent(self, _on=True):
        return None

    def set_output(self, _on):
        return None

    @staticmethod
    def read_pressure():
        return 1300.0

    def set_setpoint(self, value):
        self.setpoints.append(value)

    @staticmethod
    def get_in_limits():
        return 0.0, 1

    def close(self):
        return None


def _runner(tmp_path: Path) -> CalibrationRunner:
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
            "co2_map": {"0": 1, "400": 3},
            "co2_map_group2": {"0": 21, "500": 24},
        },
        "workflow": {
            "humidity_generator": {"ensure_run": False},
            "pressure": {
                "vent_time_s": 0.0,
                "pressurize_wait_after_vent_off_s": 0.0,
                "pressurize_high_hpa": 1200.0,
                "pressurize_timeout_s": 1.0,
                "stabilize_timeout_s": 1.0,
            },
        },
    }
    logger = RunLogger(tmp_path)
    devices = {
        "relay": _FakeRelay(),
        "relay_8": _FakeRelay(),
        "pace": _FakePace(),
    }
    return CalibrationRunner(cfg, devices, logger, lambda *_: None, lambda *_: None)


def _co2_point() -> CalibrationPoint:
    return CalibrationPoint(
        index=1,
        temp_chamber_c=20.0,
        co2_ppm=400.0,
        hgen_temp_c=None,
        hgen_rh_pct=None,
        target_pressure_hpa=1000.0,
        dewpoint_c=None,
        h2o_mmol=None,
        raw_h2o=None,
    )


def _h2o_point() -> CalibrationPoint:
    return CalibrationPoint(
        index=2,
        temp_chamber_c=20.0,
        co2_ppm=None,
        hgen_temp_c=20.0,
        hgen_rh_pct=50.0,
        target_pressure_hpa=1000.0,
        dewpoint_c=-10.0,
        h2o_mmol=2.0,
        raw_h2o="demo",
    )


def _h2o_with_co2_point() -> CalibrationPoint:
    return CalibrationPoint(
        index=6,
        temp_chamber_c=20.0,
        co2_ppm=400.0,
        hgen_temp_c=20.0,
        hgen_rh_pct=50.0,
        target_pressure_hpa=1000.0,
        dewpoint_c=-10.0,
        h2o_mmol=2.0,
        raw_h2o="demo",
    )


def test_dual_relay_routing_for_co2_and_h2o_paths(tmp_path: Path) -> None:
    runner = _runner(tmp_path)

    runner._set_valves_for_h2o(_h2o_point())
    assert runner.devices["relay_8"].states == {1: True, 2: True, 3: False, 8: True}

    runner._set_valves_for_co2(_co2_point())
    assert runner.devices["relay"].states[9] is True
    assert runner.devices["relay"].states[15] is True
    assert runner.devices["relay_8"].states == {1: False, 2: False, 3: True, 8: True}
    runner.logger.close()


def test_h2o_point_with_co2_target_keeps_co2_path_closed(tmp_path: Path) -> None:
    runner = _runner(tmp_path)

    runner._set_valves_for_h2o(_h2o_with_co2_point())

    assert runner.devices["relay_8"].states == {1: True, 2: True, 3: False, 8: True}
    assert runner.devices["relay"].states[15] is False
    assert runner.devices["relay"].states[9] is False
    runner.logger.close()


def test_pressurize_closes_hold_and_source_with_relay_map(tmp_path: Path) -> None:
    runner = _runner(tmp_path)
    point = _h2o_point()

    runner._set_h2o_path(True, point)
    runner._pressurize_and_hold(point, route="h2o")

    assert runner.devices["relay_8"].states.get(8) is False
    assert runner.devices["relay_8"].states.get(1) is False
    assert runner.devices["relay_8"].states.get(2) is False
    assert runner.devices["relay_8"].states.get(3) is False
    runner.logger.close()


def test_closing_h2o_path_closes_flow_switch(tmp_path: Path) -> None:
    runner = _runner(tmp_path)

    runner._set_h2o_path(True, _h2o_point())
    runner._set_h2o_path(False, _h2o_point())

    assert runner.devices["relay_8"].states == {1: False, 2: False, 3: False, 8: False}
    runner.logger.close()


def test_co2_route_baseline_reopens_gas_bypass(tmp_path: Path) -> None:
    runner = _runner(tmp_path)

    runner._set_co2_route_baseline(reason="test baseline")

    assert runner.devices["relay"].states[15] is False
    assert runner.devices["relay_8"].states == {1: False, 2: False, 3: False, 8: False}
    runner.logger.close()


def test_h2o_cleanup_restores_gas_bypass_baseline(tmp_path: Path) -> None:
    runner = _runner(tmp_path)

    point = _h2o_point()
    runner._set_h2o_path(True, point)
    runner._cleanup_h2o_route(point, reason="test cleanup")

    assert runner.devices["relay"].states[15] is False
    assert runner.devices["relay_8"].states == {1: False, 2: False, 3: False, 8: False}
    runner.logger.close()


def test_group2_route_and_zero_default_behavior(tmp_path: Path) -> None:
    runner = _runner(tmp_path)

    p0_default = CalibrationPoint(
        index=3,
        temp_chamber_c=20.0,
        co2_ppm=0.0,
        hgen_temp_c=None,
        hgen_rh_pct=None,
        target_pressure_hpa=1000.0,
        dewpoint_c=None,
        h2o_mmol=None,
        raw_h2o=None,
    )
    p0_group2 = CalibrationPoint(
        index=4,
        temp_chamber_c=20.0,
        co2_ppm=0.0,
        hgen_temp_c=None,
        hgen_rh_pct=None,
        target_pressure_hpa=1000.0,
        dewpoint_c=None,
        h2o_mmol=None,
        raw_h2o=None,
        co2_group="B",
    )
    p500_group2 = CalibrationPoint(
        index=5,
        temp_chamber_c=20.0,
        co2_ppm=500.0,
        hgen_temp_c=None,
        hgen_rh_pct=None,
        target_pressure_hpa=1000.0,
        dewpoint_c=None,
        h2o_mmol=None,
        raw_h2o=None,
    )

    assert runner._source_valve_for_point(p0_default) == 1
    assert runner._co2_path_for_point(p0_default) == 7

    assert runner._source_valve_for_point(p0_group2) == 21
    assert runner._co2_path_for_point(p0_group2) == 16

    runner._set_valves_for_co2(p500_group2)
    assert runner.devices["relay"].states[3] is True
    assert runner.devices["relay"].states[16] is True
    assert runner.devices["relay_8"].states[3] is True
    runner.logger.close()


def test_apply_valve_states_prefers_bulk_write_when_enabled(tmp_path: Path) -> None:
    runner = _runner(tmp_path)

    runner._apply_valve_states([7, 8, 9])

    assert runner.devices["relay"].bulk_calls
    assert runner.devices["relay_8"].bulk_calls
    runner.logger.close()


def test_apply_valve_states_falls_back_to_sequential_when_bulk_disabled(tmp_path: Path) -> None:
    runner = _runner(tmp_path)
    runner.cfg.setdefault("workflow", {}).setdefault("relay", {})["bulk_write_enabled"] = False

    runner._apply_valve_states([7, 8, 9])

    assert runner.devices["relay"].bulk_calls == []
    assert runner.devices["relay_8"].bulk_calls == []
    assert runner.devices["relay"].single_calls
    assert runner.devices["relay_8"].single_calls
    runner.logger.close()
