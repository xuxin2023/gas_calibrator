from pathlib import Path

from gas_calibrator.data.points import CalibrationPoint
from gas_calibrator.logging_utils import RunLogger
from gas_calibrator.workflow import runner as runner_mod


class _FakeClock:
    def __init__(self, step: float = 0.1) -> None:
        self.now = 0.0
        self.step = step

    def time(self) -> float:
        self.now += self.step
        return self.now

    def sleep(self, _seconds: float) -> None:
        self.now += self.step


class _ManualClock:
    def __init__(self) -> None:
        self.now = 0.0
        self.sleeps = []

    def time(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds


class _FakeAnalyzer:
    def __init__(self, rows):
        self.rows = list(rows)

    def read_data_passive(self) -> str:
        return "ROW"

    def read_latest_data(self, *args, **kwargs) -> str:
        return "ROW"

    def parse_line_mode2(self, _line: str):
        if self.rows:
            return self.rows.pop(0)
        return None


class _RetryParseAnalyzer:
    def __init__(self, lines, parsed_map):
        self.lines = list(lines)
        self.parsed_map = dict(parsed_map)

    def read_data_passive(self) -> str:
        if self.lines:
            return self.lines.pop(0)
        return ""

    def read_latest_data(self, *args, **kwargs) -> str:
        return self.read_data_passive()

    def parse_line_mode2(self, line: str):
        return self.parsed_map.get(line)


class _FakePace:
    def __init__(self, rows):
        self.rows = list(rows)

    def get_in_limits(self):
        if self.rows:
            return self.rows.pop(0)
        return 1000.0, 1


def _point_h2o() -> CalibrationPoint:
    return CalibrationPoint(
        index=1,
        temp_chamber_c=20.0,
        co2_ppm=None,
        hgen_temp_c=20.0,
        hgen_rh_pct=30.0,
        target_pressure_hpa=1000.0,
        dewpoint_c=-10.0,
        h2o_mmol=2.0,
        raw_h2o="demo",
    )


def test_h2o_sensor_stability_uses_h2o_ratio_filtered(monkeypatch, tmp_path: Path) -> None:
    clock = _FakeClock()
    monkeypatch.setattr(runner_mod.time, "time", clock.time)
    monkeypatch.setattr(runner_mod.time, "sleep", clock.sleep)

    ga = _FakeAnalyzer(
        [
            {"h2o_mmol": 10.0, "h2o_ratio_f": 0.5000},
            {"h2o_mmol": 11.0, "h2o_ratio_f": 0.5006},
            {"h2o_mmol": 9.6, "h2o_ratio_f": 0.5002},
        ]
    )
    cfg = {
        "workflow": {
            "stability": {
                "sensor": {
                    "enabled": True,
                    "h2o_ratio_f_tol": 0.001,
                    "h2o_ratio_raw_tol": 0.001,
                    "h2o_ratio_f_pressure_tol": 0.001,
                    "h2o_ratio_f_pressure_window_s": 30.0,
                    "h2o_ratio_f_pressure_min_samples": 3,
                    "h2o_ratio_f_pressure_fill_s": 0.0,
                    "window_s": 30.0,
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
            {"gas_analyzer": ga},
            logger,
            logs.append,
            lambda *_: None,
        )
        runner._wait_primary_sensor_stable(_point_h2o())
    finally:
        logger.close()

    assert any("h2o_ratio_f" in msg for msg in logs)


def test_h2o_sensor_wait_requires_pressure_in_limits(monkeypatch, tmp_path: Path) -> None:
    clock = _FakeClock()
    monkeypatch.setattr(runner_mod.time, "time", clock.time)
    monkeypatch.setattr(runner_mod.time, "sleep", clock.sleep)

    ga = _FakeAnalyzer(
        [
            {"h2o_ratio_f": 0.5000},
            {"h2o_ratio_f": 0.5005},
            {"h2o_ratio_f": 0.5003},
            {"h2o_ratio_f": 0.5004},
        ]
    )
    pace = _FakePace(
        [
            (990.0, 0),
            (995.0, 0),
            (999.9, 1),
            (1000.0, 1),
            (1000.1, 1),
            (1000.0, 1),
        ]
    )
    cfg = {
        "workflow": {
            "stability": {
                "sensor": {
                    "enabled": True,
                    "h2o_ratio_f_tol": 0.001,
                    "h2o_ratio_raw_tol": 0.001,
                    "h2o_ratio_f_pressure_tol": 0.001,
                    "h2o_ratio_f_pressure_window_s": 30.0,
                    "h2o_ratio_f_pressure_min_samples": 3,
                    "h2o_ratio_f_pressure_fill_s": 0.0,
                    "window_s": 30.0,
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
            {"gas_analyzer": ga, "pace": pace},
            logger,
            logs.append,
            lambda *_: None,
        )
        runner._wait_primary_sensor_stable(_point_h2o(), require_pressure_in_limits=True)
    finally:
        logger.close()

    assert any("Pressure not in-limits during sensor wait" in msg for msg in logs)
    assert any("Sensor stable" in msg for msg in logs)


def test_h2o_sensor_wait_under_pressure_uses_pressure_tol_and_10_samples(
    monkeypatch, tmp_path: Path
) -> None:
    clock = _FakeClock()
    monkeypatch.setattr(runner_mod.time, "time", clock.time)
    monkeypatch.setattr(runner_mod.time, "sleep", clock.sleep)

    ga = _FakeAnalyzer(
        [
            {"h2o_ratio_f": 0.5000},
            {"h2o_ratio_f": 0.5015},
            {"h2o_ratio_f": 0.4999},
            {"h2o_ratio_f": 0.5010},
            {"h2o_ratio_f": 0.5004},
            {"h2o_ratio_f": 0.4998},
            {"h2o_ratio_f": 0.5012},
            {"h2o_ratio_f": 0.5007},
            {"h2o_ratio_f": 0.5001},
            {"h2o_ratio_f": 0.5014},
        ]
    )
    pace = _FakePace([(1000.0, 1)] * 20)
    cfg = {
        "workflow": {
            "stability": {
                "sensor": {
                    "enabled": True,
                    "h2o_ratio_f_tol": 0.001,
                    "h2o_ratio_f_pressure_tol": 0.002,
                    "h2o_ratio_f_pressure_window_s": 10.0,
                    "h2o_ratio_f_pressure_min_samples": 10,
                    "h2o_ratio_f_pressure_fill_s": 0.0,
                    "window_s": 30.0,
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
            {"gas_analyzer": ga, "pace": pace},
            logger,
            logs.append,
            lambda *_: None,
        )
        assert runner._wait_primary_sensor_stable(_point_h2o(), require_pressure_in_limits=True) is True
    finally:
        logger.close()

    assert any("Sensor stable" in msg for msg in logs)


def test_h2o_sensor_wait_under_pressure_keeps_analyzer_window_after_in_limits_latched(
    monkeypatch, tmp_path: Path
) -> None:
    clock = _FakeClock()
    monkeypatch.setattr(runner_mod.time, "time", clock.time)
    monkeypatch.setattr(runner_mod.time, "sleep", clock.sleep)

    ga = _FakeAnalyzer(
        [
            {"h2o_ratio_f": 0.8040},
            {"h2o_ratio_f": 0.8042},
            {"h2o_ratio_f": 0.8044},
            {"h2o_ratio_f": 0.8043},
        ]
    )
    pace = _FakePace(
        [
            (1000.0, 1),
            (999.6, 0),
            (999.5, 0),
            (999.4, 0),
        ]
    )
    cfg = {
        "workflow": {
            "stability": {
                "sensor": {
                    "enabled": True,
                    "h2o_ratio_f_tol": 0.001,
                    "h2o_ratio_f_pressure_tol": 0.0006,
                    "h2o_ratio_f_pressure_window_s": 10.0,
                    "h2o_ratio_f_pressure_min_samples": 4,
                    "h2o_ratio_f_pressure_fill_s": 0.0,
                    "window_s": 30.0,
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
            {"gas_analyzer": ga, "pace": pace},
            logger,
            logs.append,
            lambda *_: None,
        )
        assert runner._wait_primary_sensor_stable(_point_h2o(), require_pressure_in_limits=True) is True
    finally:
        logger.close()

    assert any("Pressure drift detected after controller in-limits latched" in msg for msg in logs)
    assert any("Sensor stable" in msg for msg in logs)


def test_h2o_sensor_wait_under_pressure_waits_for_fill_before_stability_window(
    monkeypatch, tmp_path: Path
) -> None:
    clock = _FakeClock(step=1.0)
    monkeypatch.setattr(runner_mod.time, "time", clock.time)
    monkeypatch.setattr(runner_mod.time, "sleep", clock.sleep)

    ga = _FakeAnalyzer(
        [
            {"h2o_ratio_f": 0.8041},
            {"h2o_ratio_f": 0.8042},
            {"h2o_ratio_f": 0.8042},
            {"h2o_ratio_f": 0.8041},
        ]
    )
    pace = _FakePace([(1000.0, 1)] * 20)
    cfg = {
        "workflow": {
            "stability": {
                "sensor": {
                    "enabled": True,
                    "h2o_ratio_f_tol": 0.001,
                    "h2o_ratio_f_pressure_tol": 0.0006,
                    "h2o_ratio_f_pressure_window_s": 30.0,
                    "h2o_ratio_f_pressure_min_samples": 4,
                    "h2o_ratio_f_pressure_fill_s": 5.0,
                    "window_s": 30.0,
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
            {"gas_analyzer": ga, "pace": pace},
            logger,
            logs.append,
            lambda *_: None,
        )
        assert runner._wait_primary_sensor_stable(_point_h2o(), require_pressure_in_limits=True) is True
    finally:
        logger.close()

    assert any("waiting for analyzer cavity fill" in msg for msg in logs)
    assert any("Sensor stable" in msg for msg in logs)


def test_h2o_sensor_wait_requires_all_analyzers_to_stabilize(monkeypatch, tmp_path: Path) -> None:
    clock = _FakeClock(step=0.05)
    monkeypatch.setattr(runner_mod.time, "time", clock.time)
    monkeypatch.setattr(runner_mod.time, "sleep", clock.sleep)

    ga1 = _FakeAnalyzer([{"h2o_ratio_f": 0.8040}, {"h2o_ratio_f": 0.8041}] + [{"h2o_ratio_f": 0.8040}] * 20)
    ga2 = _FakeAnalyzer(
        [{"h2o_ratio_f": 0.8100}, {"h2o_ratio_f": 0.8120}] + [{"h2o_ratio_f": 0.8042}, {"h2o_ratio_f": 0.8043}] * 10
    )
    pace = _FakePace([(1000.0, 1)] * 60)
    cfg = {
        "devices": {
            "gas_analyzers": [{"name": "ga01"}, {"name": "ga02"}],
        },
        "workflow": {
            "stability": {
                "sensor": {
                    "enabled": True,
                    "h2o_ratio_f_pressure_tol": 0.0006,
                    "h2o_ratio_f_pressure_window_s": 1.0,
                    "h2o_ratio_f_pressure_min_samples": 2,
                    "h2o_ratio_f_pressure_fill_s": 0.0,
                    "timeout_s": 30.0,
                    "poll_s": 0.0,
                }
            }
        },
    }
    logs = []
    logger = RunLogger(tmp_path)
    try:
        runner = runner_mod.CalibrationRunner(
            cfg,
            {"gas_analyzer_1": ga1, "gas_analyzer_2": ga2, "pace": pace},
            logger,
            logs.append,
            lambda *_: None,
        )
        assert runner._wait_primary_sensor_stable(_point_h2o(), require_pressure_in_limits=True) is True
    finally:
        logger.close()

    stable_logs = [msg for msg in logs if "Sensor stable" in msg]
    assert stable_logs
    assert "ga01=" in stable_logs[-1]
    assert "ga02=" in stable_logs[-1]


def test_h2o_sensor_wait_drops_unstable_analyzer_after_timeout(monkeypatch, tmp_path: Path) -> None:
    clock = _FakeClock(step=0.05)
    monkeypatch.setattr(runner_mod.time, "time", clock.time)
    monkeypatch.setattr(runner_mod.time, "sleep", clock.sleep)

    ga1 = _FakeAnalyzer([{"h2o_ratio_f": 0.8040}, {"h2o_ratio_f": 0.8041}] + [{"h2o_ratio_f": 0.8040}] * 20)
    ga2 = _FakeAnalyzer([{"h2o_ratio_f": 0.8100}, {"h2o_ratio_f": 0.8120}] * 20)
    pace = _FakePace([(1000.0, 1)] * 100)
    cfg = {
        "devices": {
            "gas_analyzers": [{"name": "ga01"}, {"name": "ga02"}],
        },
        "workflow": {
            "stability": {
                "sensor": {
                    "enabled": True,
                    "h2o_ratio_f_pressure_tol": 0.0006,
                    "h2o_ratio_f_pressure_window_s": 1.0,
                    "h2o_ratio_f_pressure_min_samples": 2,
                    "h2o_ratio_f_pressure_fill_s": 0.0,
                    "timeout_s": 2.0,
                    "poll_s": 0.0,
                }
            }
        },
    }
    logs = []
    logger = RunLogger(tmp_path)
    try:
        runner = runner_mod.CalibrationRunner(
            cfg,
            {"gas_analyzer_1": ga1, "gas_analyzer_2": ga2, "pace": pace},
            logger,
            logs.append,
            lambda *_: None,
        )
        assert runner._wait_primary_sensor_stable(_point_h2o(), require_pressure_in_limits=True) is True
    finally:
        logger.close()

    assert "ga02" in runner._disabled_analyzers
    assert any("drop=ga02 keep=ga01" in msg for msg in logs)
    assert any("Analyzers dropped from active set: ga02" in msg for msg in logs)


def test_h2o_sensor_wait_retries_once_on_bad_frame(monkeypatch, tmp_path: Path) -> None:
    clock = _FakeClock()
    monkeypatch.setattr(runner_mod.time, "time", clock.time)
    monkeypatch.setattr(runner_mod.time, "sleep", clock.sleep)

    ga = _RetryParseAnalyzer(
        ["BAD", "GOOD1", "GOOD2", "GOOD3"],
        {
            "BAD": None,
            "GOOD1": {"h2o_ratio_f": 0.8040},
            "GOOD2": {"h2o_ratio_f": 0.8042},
            "GOOD3": {"h2o_ratio_f": 0.8041},
        },
    )
    pace = _FakePace([(1000.0, 1)] * 10)
    cfg = {
        "workflow": {
            "sensor_read_retry": {
                "retries": 1,
                "delay_s": 0.0,
            },
            "stability": {
                "sensor": {
                    "enabled": True,
                    "h2o_ratio_f_pressure_tol": 0.001,
                    "h2o_ratio_f_pressure_window_s": 30.0,
                    "h2o_ratio_f_pressure_min_samples": 2,
                    "h2o_ratio_f_pressure_fill_s": 0.0,
                    "timeout_s": 30.0,
                    "poll_s": 0.0,
                }
            },
        }
    }
    logs = []
    logger = RunLogger(tmp_path)
    try:
        runner = runner_mod.CalibrationRunner(
            cfg,
            {"gas_analyzer": ga, "pace": pace},
            logger,
            logs.append,
            lambda *_: None,
        )
        assert runner._wait_primary_sensor_stable(_point_h2o(), require_pressure_in_limits=True) is True
    finally:
        logger.close()

    assert any("Sensor stable" in msg for msg in logs)


def test_h2o_sensor_wait_under_pressure_uses_15s_sliding_window_and_1s_reads(
    monkeypatch, tmp_path: Path
) -> None:
    clock = _ManualClock()
    monkeypatch.setattr(runner_mod.time, "time", clock.time)
    monkeypatch.setattr(runner_mod.time, "sleep", clock.sleep)

    unstable = [
        {"h2o_ratio_f": 0.8100},
        {"h2o_ratio_f": 0.8050},
        {"h2o_ratio_f": 0.8000},
        {"h2o_ratio_f": 0.7950},
        {"h2o_ratio_f": 0.7900},
    ]
    stable = [{"h2o_ratio_f": 0.8040 + ((i % 3) * 0.0002)} for i in range(16)]
    ga = _FakeAnalyzer(unstable + stable)
    pace = _FakePace([(1000.0, 1)] * 40)
    cfg = {
        "workflow": {
            "stability": {
                "sensor": {
                    "enabled": True,
                    "h2o_ratio_f_pressure_tol": 0.001,
                    "h2o_ratio_f_pressure_window_s": 15.0,
                    "h2o_ratio_f_pressure_min_samples": 10,
                    "h2o_ratio_f_pressure_fill_s": 0.0,
                    "h2o_ratio_f_pressure_read_interval_s": 1.0,
                    "timeout_s": 60.0,
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
            {"gas_analyzer": ga, "pace": pace},
            logger,
            logs.append,
            lambda *_: None,
        )
        assert runner._wait_primary_sensor_stable(_point_h2o(), require_pressure_in_limits=True) is True
    finally:
        logger.close()

    assert any("Sensor stable" in msg for msg in logs)
    assert any(abs(s - 1.0) < 1e-9 for s in clock.sleeps)
