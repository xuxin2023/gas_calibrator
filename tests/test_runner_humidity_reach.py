import types
from pathlib import Path

from gas_calibrator.data.points import CalibrationPoint
from gas_calibrator.logging_utils import RunLogger
from gas_calibrator.workflow import runner as runner_mod


class _FakeClock:
    def __init__(self) -> None:
        self.now = 0.0

    def time(self) -> float:
        self.now += 0.1
        return self.now

    def sleep(self, _seconds: float) -> None:
        self.now += 0.1


class _StepClock:
    def __init__(self) -> None:
        self.now = 0.0

    def time(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.now += float(seconds)


class _FakeHumidityGen:
    def __init__(self, rows):
        self.rows = list(rows)
        self.calls = 0

    def fetch_all(self):
        self.calls += 1
        if self.rows:
            return {"data": self.rows.pop(0)}
        return {"data": {}}


class _FakeDewpointMeter:
    def __init__(self, values, *, temp_c: float = 20.0, rh_pct: float = 30.0):
        self.values = list(values)
        self.calls = 0
        self.last = None
        self.temp_c = temp_c
        self.rh_pct = rh_pct

    def get_current(self):
        self.calls += 1
        if self.values:
            self.last = self.values.pop(0)
        if isinstance(self.last, dict):
            return dict(self.last)
        return {"dewpoint_c": self.last, "temp_c": self.temp_c, "rh_pct": self.rh_pct}


def _point() -> CalibrationPoint:
    return CalibrationPoint(
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


def test_wait_humidity_reach_uses_rh_window_and_span(monkeypatch, tmp_path: Path) -> None:
    clock = _FakeClock()
    monkeypatch.setattr(runner_mod.time, "time", clock.time)
    monkeypatch.setattr(runner_mod.time, "sleep", clock.sleep)

    hgen = _FakeHumidityGen([{"Tc": 20.4, "Uw": 31.0}] + [{"Tc": 20.2, "Uw": 30.2} for _ in range(8)])
    cfg = {
        "workflow": {
            "stability": {
                "humidity_generator": {
                    "enabled": True,
                    "rh_tol_pct": 1.0,
                    "rh_stable_window_s": 1.0,
                    "rh_stable_span_pct": 0.3,
                    "timeout_s": 30.0,
                    "poll_s": 0.0,
                }
            }
        }
    }
    logs = []
    logger = RunLogger(tmp_path)
    try:
        runner = runner_mod.CalibrationRunner(
            cfg,
            {"humidity_gen": hgen},
            logger,
            logs.append,
            lambda *_: None,
        )
        runner._wait_humidity_generator_stable(_point())
    finally:
        logger.close()

    assert hgen.calls >= 3
    assert any("reached setpoint" in msg.lower() for msg in logs)
    assert any("span=" in msg.lower() for msg in logs)


def test_wait_humidity_reach_resets_when_rh_leaves_target_band(monkeypatch, tmp_path: Path) -> None:
    clock = _FakeClock()
    monkeypatch.setattr(runner_mod.time, "time", clock.time)
    monkeypatch.setattr(runner_mod.time, "sleep", clock.sleep)

    rows = (
        [{"Tc": 20.0, "Uw": 30.2} for _ in range(2)]
        + [{"Tc": 20.0, "Uw": 32.0}]
        + [{"Tc": 20.0, "Uw": 30.1} for _ in range(12)]
    )
    hgen = _FakeHumidityGen(rows)
    cfg = {
        "workflow": {
            "stability": {
                "humidity_generator": {
                    "enabled": True,
                    "rh_tol_pct": 1.0,
                    "rh_stable_window_s": 1.0,
                    "rh_stable_span_pct": 0.3,
                    "timeout_s": 30.0,
                    "poll_s": 0.0,
                }
            }
        }
    }
    logs = []
    logger = RunLogger(tmp_path)
    try:
        runner = runner_mod.CalibrationRunner(
            cfg,
            {"humidity_gen": hgen},
            logger,
            logs.append,
            lambda *_: None,
        )
        assert runner._wait_humidity_generator_stable(_point()) is True
    finally:
        logger.close()

    assert any("left target band" in msg.lower() for msg in logs)
    assert any("reached setpoint" in msg.lower() for msg in logs)
    assert hgen.calls >= 7


def test_wait_humidity_reach_allows_timeout_disable(monkeypatch, tmp_path: Path) -> None:
    clock = _FakeClock()
    monkeypatch.setattr(runner_mod.time, "time", clock.time)
    monkeypatch.setattr(runner_mod.time, "sleep", clock.sleep)

    hgen = _FakeHumidityGen([{"Tc": 20.0, "Uw": 30.0} for _ in range(12)])
    cfg = {
        "workflow": {
            "stability": {
                "humidity_generator": {
                    "enabled": True,
                    "rh_tol_pct": 1.0,
                    "rh_stable_window_s": 1.0,
                    "rh_stable_span_pct": 0.3,
                    "timeout_s": 0.0,
                    "poll_s": 0.0,
                }
            }
        }
    }
    logs = []
    logger = RunLogger(tmp_path)
    try:
        runner = runner_mod.CalibrationRunner(
            cfg,
            {"humidity_gen": hgen},
            logger,
            logs.append,
            lambda *_: None,
        )
        assert runner._wait_humidity_generator_stable(_point()) is True
    finally:
        logger.close()

    assert any("timeout disabled" in msg.lower() for msg in logs)
    assert any("reached setpoint" in msg.lower() for msg in logs)


def test_wait_humidity_reach_requires_temperature_within_tolerance(monkeypatch, tmp_path: Path) -> None:
    clock = _FakeClock()
    monkeypatch.setattr(runner_mod.time, "time", clock.time)
    monkeypatch.setattr(runner_mod.time, "sleep", clock.sleep)

    rows = (
        [{"Tc": 20.2, "Uw": 30.0} for _ in range(3)]
        + [{"Tc": 21.2, "Uw": 30.1}]
        + [{"Tc": 20.6, "Uw": 30.1} for _ in range(12)]
    )
    hgen = _FakeHumidityGen(rows)
    cfg = {
        "workflow": {
            "stability": {
                "humidity_generator": {
                    "enabled": True,
                    "temp_tol_c": 1.0,
                    "rh_tol_pct": 3.0,
                    "rh_stable_window_s": 1.0,
                    "rh_stable_span_pct": 0.5,
                    "timeout_s": 30.0,
                    "poll_s": 0.0,
                }
            }
        }
    }
    logs = []
    logger = RunLogger(tmp_path)
    try:
        runner = runner_mod.CalibrationRunner(
            cfg,
            {"humidity_gen": hgen},
            logger,
            logs.append,
            lambda *_: None,
        )
        assert runner._wait_humidity_generator_stable(_point()) is True
    finally:
        logger.close()

    assert any("left target band" in msg.lower() for msg in logs)
    assert any("tol=" in msg and "1.0" in msg for msg in logs)
    assert any("reached setpoint" in msg.lower() for msg in logs)


def test_wait_humidity_generator_dewpoint_stable(monkeypatch, tmp_path: Path) -> None:
    clock = _FakeClock()
    monkeypatch.setattr(runner_mod.time, "time", clock.time)
    monkeypatch.setattr(runner_mod.time, "sleep", clock.sleep)

    hgen = _FakeHumidityGen(
        [
            {"Td": 5.2},
            {"Td": 5.1},
            {"Td": 5.15},
            {"Td": 5.10},
            {"Td": 5.12},
        ]
    )
    cfg = {
        "workflow": {
            "stability": {
                "humidity_generator": {
                    "enabled": True,
                    "dewpoint_tol_c": 0.2,
                    "dewpoint_window_s": 30.0,
                    "dewpoint_timeout_s": 30.0,
                    "poll_s": 0.0,
                }
            }
        }
    }
    logs = []
    logger = RunLogger(tmp_path)
    try:
        runner = runner_mod.CalibrationRunner(
            cfg,
            {"humidity_gen": hgen},
            logger,
            logs.append,
            lambda *_: None,
        )
        runner._wait_humidity_generator_dewpoint_stable()
    finally:
        logger.close()

    assert any("dewpoint stable" in msg.lower() for msg in logs)


def test_wait_humidity_generator_dewpoint_allows_timeout_disable(monkeypatch, tmp_path: Path) -> None:
    clock = _FakeClock()
    monkeypatch.setattr(runner_mod.time, "time", clock.time)
    monkeypatch.setattr(runner_mod.time, "sleep", clock.sleep)

    hgen = _FakeHumidityGen([{"Td": 5.1} for _ in range(12)])
    cfg = {
        "workflow": {
            "stability": {
                "humidity_generator": {
                    "enabled": True,
                    "dewpoint_tol_c": 0.2,
                    "dewpoint_window_s": 1.0,
                    "dewpoint_timeout_s": 0.0,
                    "poll_s": 0.0,
                }
            }
        }
    }
    logs = []
    logger = RunLogger(tmp_path)
    try:
        runner = runner_mod.CalibrationRunner(
            cfg,
            {"humidity_gen": hgen},
            logger,
            logs.append,
            lambda *_: None,
        )
        assert runner._wait_humidity_generator_dewpoint_stable() is True
    finally:
        logger.close()

    assert any("timeout disabled" in msg.lower() for msg in logs)
    assert any("dewpoint stable" in msg.lower() for msg in logs)


def test_dewpoint_meter_window_stable_requires_same_rounded_tenth(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    try:
        runner = runner_mod.CalibrationRunner({}, {}, logger, lambda *_: None, lambda *_: None)
        assert runner._dewpoint_meter_window_stable([1.31, 1.32, 1.34], decimals=1) is True
        assert runner._dewpoint_meter_window_stable([1.31, 1.32, 1.41], decimals=1) is False
    finally:
        logger.close()


def test_wait_dewpoint_meter_stable_uses_meter_only(monkeypatch, tmp_path: Path) -> None:
    clock = _FakeClock()
    monkeypatch.setattr(runner_mod.time, "time", clock.time)
    monkeypatch.setattr(runner_mod.time, "sleep", clock.sleep)

    dew = _FakeDewpointMeter(
        [
            5.11,
            5.12,
            5.13,
            5.14,
            5.11,
            5.12,
            5.13,
            5.14,
            5.11,
            5.12,
            5.13,
            5.14,
        ]
    )
    cfg = {
        "workflow": {
            "stability": {
                "dewpoint": {
                    "window_s": 2.0,
                    "timeout_s": 10.0,
                    "poll_s": 0.0,
                    "stable_decimals": 1,
                }
            }
        }
    }
    logs = []
    logger = RunLogger(tmp_path)
    try:
        runner = runner_mod.CalibrationRunner(
            cfg,
            {"dewpoint": dew, "humidity_gen": _FakeHumidityGen([{"Tc": 20.0, "Uw": 30.0} for _ in range(16)])},
            logger,
            logs.append,
            lambda *_: None,
        )
        point = CalibrationPoint(
            index=1,
            temp_chamber_c=20.0,
            co2_ppm=0.0,
            hgen_temp_c=20.0,
            hgen_rh_pct=30.0,
            target_pressure_hpa=1000.0,
            dewpoint_c=5.12,
            h2o_mmol=None,
            raw_h2o=None,
        )
        assert runner._wait_dewpoint_alignment_stable(point) is True
    finally:
        logger.close()

    assert dew.calls > 5
    assert any("stability window started" in msg.lower() for msg in logs)
    assert any("dewpoint meter stable" in msg.lower() for msg in logs)


def test_wait_dewpoint_meter_resets_when_rounded_tenth_changes(monkeypatch, tmp_path: Path) -> None:
    clock = _FakeClock()
    monkeypatch.setattr(runner_mod.time, "time", clock.time)
    monkeypatch.setattr(runner_mod.time, "sleep", clock.sleep)

    dew = _FakeDewpointMeter(
        [
            5.11,
            5.12,
            5.13,
            5.14,
            5.21,
            5.21,
            5.22,
            5.23,
            5.24,
            5.21,
            5.22,
            5.23,
            5.24,
            5.21,
            5.22,
            5.23,
        ]
    )
    cfg = {
        "workflow": {
            "stability": {
                "dewpoint": {
                    "window_s": 2.0,
                    "timeout_s": 20.0,
                    "poll_s": 0.0,
                    "stable_decimals": 1,
                }
            }
        }
    }
    logs = []
    logger = RunLogger(tmp_path)
    try:
        runner = runner_mod.CalibrationRunner(
            cfg,
            {"dewpoint": dew, "humidity_gen": _FakeHumidityGen([{"Tc": 20.0, "Uw": 30.0} for _ in range(24)])},
            logger,
            logs.append,
            lambda *_: None,
        )
        point = CalibrationPoint(
            index=1,
            temp_chamber_c=20.0,
            co2_ppm=0.0,
            hgen_temp_c=20.0,
            hgen_rh_pct=30.0,
            target_pressure_hpa=1000.0,
            dewpoint_c=5.12,
            h2o_mmol=None,
            raw_h2o=None,
        )
        assert runner._wait_dewpoint_alignment_stable(point) is True
    finally:
        logger.close()

    assert any("stability window started" in msg.lower() for msg in logs)
    assert any("dewpoint meter stable" in msg.lower() for msg in logs)


def test_wait_dewpoint_meter_allows_timeout_disable(monkeypatch, tmp_path: Path) -> None:
    clock = _FakeClock()
    monkeypatch.setattr(runner_mod.time, "time", clock.time)
    monkeypatch.setattr(runner_mod.time, "sleep", clock.sleep)

    dew = _FakeDewpointMeter([1.34, 1.35, 1.36, 1.34, 1.35, 1.36, 1.34, 1.35, 1.36])
    cfg = {
        "workflow": {
            "stability": {
                "dewpoint": {
                    "window_s": 0.8,
                    "timeout_s": 0.0,
                    "poll_s": 0.0,
                    "stable_decimals": 1,
                }
            }
        }
    }
    logs = []
    logger = RunLogger(tmp_path)
    try:
        runner = runner_mod.CalibrationRunner(
            cfg,
            {"dewpoint": dew, "humidity_gen": _FakeHumidityGen([{"Tc": 20.0, "Uw": 30.0} for _ in range(16)])},
            logger,
            logs.append,
            lambda *_: None,
        )
        assert runner._wait_dewpoint_alignment_stable(_point()) is True
    finally:
        logger.close()

    assert any("timeout disabled" in msg.lower() for msg in logs)
    assert any("dewpoint meter stable" in msg.lower() for msg in logs)


def test_wait_dewpoint_meter_refreshes_atmosphere_hold(monkeypatch, tmp_path: Path) -> None:
    clock = _FakeClock()
    monkeypatch.setattr(runner_mod.time, "time", clock.time)
    monkeypatch.setattr(runner_mod.time, "sleep", clock.sleep)

    dew = _FakeDewpointMeter(
        [
            1.31,
            1.312,
            1.315,
            1.318,
            1.319,
            1.317,
            1.316,
            1.314,
            1.313,
            1.315,
            1.316,
            1.317,
        ]
    )
    cfg = {
        "workflow": {
            "pressure": {
                "vent_hold_interval_s": 2.0,
            },
            "stability": {
                "dewpoint": {
                    "window_s": 2.0,
                    "timeout_s": 10.0,
                    "poll_s": 0.0,
                    "stable_decimals": 1,
                }
            }
        }
    }
    logger = RunLogger(tmp_path)
    try:
        runner = runner_mod.CalibrationRunner(
            cfg,
            {"dewpoint": dew, "pace": object(), "humidity_gen": _FakeHumidityGen([{"Tc": 20.0, "Uw": 30.0} for _ in range(16)])},
            logger,
            lambda *_: None,
            lambda *_: None,
        )
        calls = []
        runner._refresh_pressure_controller_atmosphere_hold = types.MethodType(
            lambda self, force=False, reason="": calls.append((bool(force), reason)),
            runner,
        )
        assert runner._wait_dewpoint_alignment_stable(_point()) is True
    finally:
        logger.close()

    assert calls == []


def test_wait_dewpoint_meter_requires_temp_rh_match_before_stability(monkeypatch, tmp_path: Path) -> None:
    clock = _FakeClock()
    monkeypatch.setattr(runner_mod.time, "time", clock.time)
    monkeypatch.setattr(runner_mod.time, "sleep", clock.sleep)

    dew = _FakeDewpointMeter(
        [
            {"dewpoint_c": 5.1, "temp_c": 21.0, "rh_pct": 40.0},
            {"dewpoint_c": 5.1, "temp_c": 21.0, "rh_pct": 40.0},
            {"dewpoint_c": 5.1, "temp_c": 20.2, "rh_pct": 31.0},
            {"dewpoint_c": 5.1, "temp_c": 20.2, "rh_pct": 31.0},
            {"dewpoint_c": 5.1, "temp_c": 20.2, "rh_pct": 31.0},
            {"dewpoint_c": 5.1, "temp_c": 20.2, "rh_pct": 31.0},
            {"dewpoint_c": 5.1, "temp_c": 20.2, "rh_pct": 31.0},
            {"dewpoint_c": 5.1, "temp_c": 20.2, "rh_pct": 31.0},
        ]
    )
    cfg = {
        "workflow": {
            "stability": {
                "dewpoint": {
                    "window_s": 1.0,
                    "timeout_s": 10.0,
                    "poll_s": 0.0,
                    "stable_decimals": 1,
                    "temp_match_tol_c": 0.3,
                    "rh_match_tol_pct": 4.0,
                }
            }
        }
    }
    logs = []
    logger = RunLogger(tmp_path)
    try:
        runner = runner_mod.CalibrationRunner(
            cfg,
            {"dewpoint": dew, "humidity_gen": _FakeHumidityGen([{"Tc": 20.0, "Uw": 30.0} for _ in range(16)])},
            logger,
            logs.append,
            lambda *_: None,
        )
        assert runner._wait_dewpoint_alignment_stable(_point()) is True
    finally:
        logger.close()

    assert any("matched humidity generator" in msg.lower() for msg in logs)
    assert any("stability window started" in msg.lower() for msg in logs)
    assert any("dewpoint meter stable" in msg.lower() for msg in logs)


def test_wait_dewpoint_meter_uses_configured_rh_match_for_70rh_points(monkeypatch, tmp_path: Path) -> None:
    clock = _FakeClock()
    monkeypatch.setattr(runner_mod.time, "time", clock.time)
    monkeypatch.setattr(runner_mod.time, "sleep", clock.sleep)

    dew = _FakeDewpointMeter(
        [
            {"dewpoint_c": 13.60, "temp_c": 20.05, "rh_pct": 66.1},
            {"dewpoint_c": 13.62, "temp_c": 20.05, "rh_pct": 66.2},
            {"dewpoint_c": 13.61, "temp_c": 20.04, "rh_pct": 66.3},
            {"dewpoint_c": 13.63, "temp_c": 20.05, "rh_pct": 66.4},
            {"dewpoint_c": 13.62, "temp_c": 20.05, "rh_pct": 66.5},
            {"dewpoint_c": 13.61, "temp_c": 20.05, "rh_pct": 66.6},
            {"dewpoint_c": 13.62, "temp_c": 20.05, "rh_pct": 66.6},
            {"dewpoint_c": 13.61, "temp_c": 20.05, "rh_pct": 66.5},
        ]
    )
    cfg = {
        "workflow": {
            "stability": {
                "dewpoint": {
                    "window_s": 0.5,
                    "timeout_s": 10.0,
                    "poll_s": 0.0,
                    "temp_match_tol_c": 0.35,
                    "rh_match_tol_pct": 5.5,
                    "stability_tol_c": 0.05,
                }
            }
        }
    }
    logs = []
    logger = RunLogger(tmp_path)
    try:
        runner = runner_mod.CalibrationRunner(
            cfg,
            {"dewpoint": dew, "humidity_gen": _FakeHumidityGen([{"Tc": 20.0, "Uw": 69.8} for _ in range(16)])},
            logger,
            logs.append,
            lambda *_: None,
        )
        point = CalibrationPoint(
            index=1,
            temp_chamber_c=20.0,
            co2_ppm=0.0,
            hgen_temp_c=20.0,
            hgen_rh_pct=70.0,
            target_pressure_hpa=1000.0,
            dewpoint_c=13.6,
            h2o_mmol=None,
            raw_h2o=None,
        )
        assert runner._wait_dewpoint_alignment_stable(point) is True
    finally:
        logger.close()

    assert any("dewpoint meter stable" in msg.lower() for msg in logs)


def test_wait_dewpoint_meter_times_out_when_match_never_reached(monkeypatch, tmp_path: Path) -> None:
    clock = _FakeClock()
    monkeypatch.setattr(runner_mod.time, "time", clock.time)
    monkeypatch.setattr(runner_mod.time, "sleep", clock.sleep)

    dew = _FakeDewpointMeter([{"dewpoint_c": 5.1, "temp_c": 21.0, "rh_pct": 40.0}] * 20)
    cfg = {
        "workflow": {
            "stability": {
                "dewpoint": {
                    "window_s": 1.0,
                    "timeout_s": 1.0,
                    "poll_s": 0.0,
                    "temp_match_tol_c": 0.3,
                    "rh_match_tol_pct": 5.5,
                    "stability_tol_c": 0.05,
                }
            }
        }
    }
    logs = []
    logger = RunLogger(tmp_path)
    try:
        runner = runner_mod.CalibrationRunner(
            cfg,
            {"dewpoint": dew, "humidity_gen": _FakeHumidityGen([{"Tc": 20.0, "Uw": 30.0} for _ in range(20)])},
            logger,
            logs.append,
            lambda *_: None,
        )
        assert runner._wait_dewpoint_alignment_stable(_point()) is False
    finally:
        logger.close()

    assert any("dewpoint meter" in msg.lower() and "timeout" in msg.lower() for msg in logs)


def test_wait_dewpoint_meter_requires_multiple_window_samples_before_stable(monkeypatch, tmp_path: Path) -> None:
    clock = _StepClock()
    monkeypatch.setattr(runner_mod.time, "time", clock.time)
    monkeypatch.setattr(runner_mod.time, "sleep", clock.sleep)

    dew = _FakeDewpointMeter([{"dewpoint_c": 4.05, "temp_c": 10.09, "rh_pct": 66.07}] * 6)
    cfg = {
        "workflow": {
            "stability": {
                "dewpoint": {
                    "window_s": 1.0,
                    "timeout_s": 2.5,
                    "poll_s": 1.1,
                    "temp_match_tol_c": 0.45,
                    "rh_match_tol_pct": 3.5,
                    "stability_tol_c": 0.06,
                }
            }
        }
    }
    logs = []
    logger = RunLogger(tmp_path)
    try:
        runner = runner_mod.CalibrationRunner(
            cfg,
            {"dewpoint": dew, "humidity_gen": _FakeHumidityGen([{"Tc": 10.118, "Uw": 69.4} for _ in range(6)])},
            logger,
            logs.append,
            lambda *_: None,
        )
        point = CalibrationPoint(
            index=1,
            temp_chamber_c=10.0,
            co2_ppm=0.0,
            hgen_temp_c=10.118,
            hgen_rh_pct=69.4,
            target_pressure_hpa=1000.0,
            dewpoint_c=4.05,
            h2o_mmol=None,
            raw_h2o=None,
        )
        assert runner._wait_dewpoint_alignment_stable(point) is False
    finally:
        logger.close()

    assert not any("dewpoint meter stable" in msg.lower() for msg in logs)
    assert any("timeout" in msg.lower() for msg in logs)


def test_humidity_wait_skips_when_same_target_already_ready(monkeypatch, tmp_path: Path) -> None:
    clock = _FakeClock()
    monkeypatch.setattr(runner_mod.time, "time", clock.time)
    monkeypatch.setattr(runner_mod.time, "sleep", clock.sleep)

    hgen = _FakeHumidityGen([{"Tc": 20.0, "Uw": 30.0} for _ in range(12)])
    cfg = {
        "workflow": {
            "stability": {
                "humidity_generator": {
                    "enabled": True,
                    "rh_tol_pct": 1.0,
                    "rh_stable_window_s": 1.0,
                    "rh_stable_span_pct": 0.3,
                    "timeout_s": 30.0,
                    "poll_s": 0.0,
                }
            }
        }
    }
    logger = RunLogger(tmp_path)
    try:
        runner = runner_mod.CalibrationRunner(
            cfg,
            {"humidity_gen": hgen},
            logger,
            lambda *_: None,
            lambda *_: None,
        )
        runner._last_hgen_target = (20.0, 30.0)
        runner._wait_humidity_generator_stable(_point())
        first_calls = hgen.calls
        runner._wait_humidity_generator_stable(_point())
        second_calls = hgen.calls - first_calls
    finally:
        logger.close()

    assert first_calls >= 3
    assert second_calls == 0
