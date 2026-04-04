import csv
from pathlib import Path

from gas_calibrator.data.points import CalibrationPoint
from gas_calibrator.logging_utils import RunLogger
from gas_calibrator.workflow import runner as runner_mod


class _FakeChamber:
    def __init__(self, temps, run_state=0, set_temp_readbacks=None):
        self._temps = list(temps)
        self.set_calls = []
        self.started = False
        self.stop_calls = 0
        self.read_calls = 0
        self.run_state = int(run_state)
        self._set_temp_readbacks = list(set_temp_readbacks or [])

    def set_temp_c(self, value: float) -> None:
        self.set_calls.append(value)

    def start(self) -> None:
        self.started = True
        self.run_state = 1

    def read_temp_c(self):
        self.read_calls += 1
        if self._temps:
            return self._temps.pop(0)
        return None

    def stop(self) -> None:
        self.stop_calls += 1
        self.run_state = 0

    def read_run_state(self):
        return self.run_state

    def read_set_temp_c(self):
        if self._set_temp_readbacks:
            return self._set_temp_readbacks.pop(0)
        if self.set_calls:
            return self.set_calls[-1]
        return None


class _FakeClock:
    def __init__(self):
        self.now = 0.0

    def time(self) -> float:
        self.now += 0.1
        return self.now

    def sleep(self, _seconds: float) -> None:
        self.now += 0.1


class _FakeAnalyzer:
    pass


class _StartMismatchChamber(_FakeChamber):
    def start(self) -> None:
        self.started = True
        raise RuntimeError("START_STATE_MISMATCH")


class _NonStartingChamber(_FakeChamber):
    def start(self) -> None:
        self.started = True


def _co2_point() -> CalibrationPoint:
    return CalibrationPoint(
        index=3,
        temp_chamber_c=25.0,
        co2_ppm=400.0,
        hgen_temp_c=None,
        hgen_rh_pct=None,
        target_pressure_hpa=1100.0,
        dewpoint_c=None,
        h2o_mmol=None,
        raw_h2o=None,
    )


def test_set_temperature_with_soak_wait(monkeypatch, tmp_path: Path) -> None:
    clock = _FakeClock()
    monkeypatch.setattr(runner_mod.time, "time", clock.time)
    monkeypatch.setattr(runner_mod.time, "sleep", clock.sleep)

    chamber = _FakeChamber([25.0] * 20)
    cfg = {
        "workflow": {
            "stability": {
                "temperature": {
                    "tol": 0.2,
                    "timeout_s": 120,
                    "soak_after_reach_s": 1.0,
                    "reuse_running_in_tol_without_soak": False,
                }
            }
        }
    }
    logs = []
    logger = RunLogger(tmp_path)
    try:
        r = runner_mod.CalibrationRunner(cfg, {"temp_chamber": chamber}, logger, logs.append, lambda _: None)
        r._set_temperature(25.0)
    finally:
        logger.close()

    assert chamber.started is True
    assert chamber.set_calls == [25.0]
    assert chamber.read_calls > 1
    assert any("soak" in msg.lower() for msg in logs)


def test_set_temperature_waits_for_analyzer_chamber_temp_stability(monkeypatch, tmp_path: Path) -> None:
    clock = _FakeClock()
    monkeypatch.setattr(runner_mod.time, "time", clock.time)
    monkeypatch.setattr(runner_mod.time, "sleep", clock.sleep)

    chamber = _FakeChamber([25.0] * 40)
    analyzer = _FakeAnalyzer()
    analyzer_temps = iter([24.000, 24.005, 24.004, 24.006, 24.005, 24.004, 24.006, 24.005])
    logs = []
    cfg = {
        "workflow": {
            "stability": {
                "temperature": {
                    "tol": 0.2,
                    "timeout_s": 120,
                    "soak_after_reach_s": 1.0,
                    "reuse_running_in_tol_without_soak": False,
                    "analyzer_chamber_temp_window_s": 0.5,
                    "analyzer_chamber_temp_span_c": 0.02,
                    "analyzer_chamber_temp_timeout_s": 5.0,
                    "analyzer_chamber_temp_poll_s": 0.1,
                }
            }
        }
    }
    logger = RunLogger(tmp_path)
    try:
        r = runner_mod.CalibrationRunner(
            cfg,
            {"temp_chamber": chamber},
            logger,
            logs.append,
            lambda *_: None,
        )
        r._active_gas_analyzers = lambda: [("ga01", analyzer, {})]  # type: ignore[method-assign]
        r._all_gas_analyzers = lambda: [("ga01", analyzer, {})]  # type: ignore[method-assign]
        r._read_sensor_parsed = lambda *_args, **_kwargs: ("", {"chamber_temp_c": next(analyzer_temps)})  # type: ignore[method-assign]
        assert r._set_temperature(25.0) is True
    finally:
        logger.close()

    assert any("analyzer chamber temp stable" in msg.lower() for msg in logs)


def test_analyzer_chamber_temp_wait_emits_stage_events(monkeypatch, tmp_path: Path) -> None:
    clock = _FakeClock()
    monkeypatch.setattr(runner_mod.time, "time", clock.time)
    monkeypatch.setattr(runner_mod.time, "sleep", clock.sleep)

    analyzer = _FakeAnalyzer()
    analyzer_temps = iter([24.000, 24.005, 24.004, 24.006, 24.005, 24.004, 24.006, 24.005])
    logger = RunLogger(tmp_path)
    try:
        r = runner_mod.CalibrationRunner(
            {
                "workflow": {
                    "stability": {
                        "temperature": {
                            "analyzer_chamber_temp_window_s": 0.5,
                            "analyzer_chamber_temp_span_c": 0.02,
                            "analyzer_chamber_temp_timeout_s": 5.0,
                            "analyzer_chamber_temp_poll_s": 0.1,
                        }
                    }
                }
            },
            {},
            logger,
            lambda *_: None,
            lambda *_: None,
        )
        r._active_gas_analyzers = lambda: [("ga01", analyzer, {})]  # type: ignore[method-assign]
        r._all_gas_analyzers = lambda: [("ga01", analyzer, {})]  # type: ignore[method-assign]
        r._read_sensor_parsed = lambda *_args, **_kwargs: ("", {"chamber_temp_c": next(analyzer_temps)})  # type: ignore[method-assign]
        r._temperature_wait_context = {"point": _co2_point(), "phase": "co2", "point_tag": ""}

        assert r._wait_analyzer_chamber_temp_stable(25.0) is True
    finally:
        logger.close()

    with logger.io_path.open("r", encoding="utf-8", newline="") as f:
        rows = [row for row in csv.DictReader(f) if row["port"] == "RUN" and row["command"] == "stage"]

    assert any("腔温判稳" in row["response"] for row in rows)
    assert any("分析仪腔温判稳" in row["response"] for row in rows)


def test_temperature_reach_timeout_does_not_cut_off_soak_and_analyzer_wait(monkeypatch, tmp_path: Path) -> None:
    clock = _FakeClock()
    monkeypatch.setattr(runner_mod.time, "time", clock.time)
    monkeypatch.setattr(runner_mod.time, "sleep", clock.sleep)

    chamber = _FakeChamber([25.0] * 80, run_state=1)
    analyzer = _FakeAnalyzer()
    analyzer_temps = iter([24.000, 24.004, 24.006, 24.005, 24.004, 24.006, 24.005, 24.004, 24.006, 24.005])
    logs = []
    cfg = {
        "workflow": {
            "stability": {
                "temperature": {
                    "tol": 0.2,
                    "timeout_s": 1.0,
                    "soak_after_reach_s": 1.0,
                    "reuse_running_in_tol_without_soak": False,
                    "analyzer_chamber_temp_window_s": 0.5,
                    "analyzer_chamber_temp_span_c": 0.02,
                    "analyzer_chamber_temp_timeout_s": 2.0,
                    "analyzer_chamber_temp_poll_s": 0.1,
                }
            }
        }
    }
    logger = RunLogger(tmp_path)
    try:
        r = runner_mod.CalibrationRunner(
            cfg,
            {"temp_chamber": chamber},
            logger,
            logs.append,
            lambda *_: None,
        )
        r._active_gas_analyzers = lambda: [("ga01", analyzer, {})]  # type: ignore[method-assign]
        r._all_gas_analyzers = lambda: [("ga01", analyzer, {})]  # type: ignore[method-assign]
        r._read_sensor_parsed = lambda *_args, **_kwargs: ("", {"chamber_temp_c": next(analyzer_temps)})  # type: ignore[method-assign]
        assert r._set_temperature(25.0) is True
    finally:
        logger.close()

    assert any("chamber soak done" in msg.lower() for msg in logs)
    assert any("analyzer chamber temp stable" in msg.lower() for msg in logs)


def test_analyzer_chamber_temp_wait_times_out_when_no_valid_frame_arrives(monkeypatch, tmp_path: Path) -> None:
    clock = _FakeClock()
    monkeypatch.setattr(runner_mod.time, "time", clock.time)
    monkeypatch.setattr(runner_mod.time, "sleep", clock.sleep)

    analyzer = _FakeAnalyzer()
    logs = []
    logger = RunLogger(tmp_path)
    try:
        r = runner_mod.CalibrationRunner(
            {
                "workflow": {
                    "stability": {
                        "temperature": {
                            "analyzer_chamber_temp_window_s": 5.0,
                            "analyzer_chamber_temp_span_c": 0.02,
                            "analyzer_chamber_temp_timeout_s": 30.0,
                            "analyzer_chamber_temp_first_valid_timeout_s": 0.5,
                            "analyzer_chamber_temp_poll_s": 0.1,
                        }
                    }
                }
            },
            {},
            logger,
            logs.append,
            lambda *_: None,
        )
        r._active_gas_analyzers = lambda: [("ga01", analyzer, {})]  # type: ignore[method-assign]
        r._all_gas_analyzers = lambda: [("ga01", analyzer, {})]  # type: ignore[method-assign]
        r._read_sensor_parsed = lambda *_args, **_kwargs: ("", None)  # type: ignore[method-assign]

        assert r._wait_analyzer_chamber_temp_stable(25.0) is False
    finally:
        logger.close()

    assert any("first valid timeout" in msg.lower() for msg in logs)


def test_analyzer_chamber_temp_unstable_restarts_window(monkeypatch, tmp_path: Path) -> None:
    clock = _FakeClock()
    monkeypatch.setattr(runner_mod.time, "time", clock.time)
    monkeypatch.setattr(runner_mod.time, "sleep", clock.sleep)

    chamber = _FakeChamber([25.0] * 60)
    analyzer = _FakeAnalyzer()
    analyzer_temps = iter([
        24.000, 24.030, 24.000, 24.030, 24.000, 24.030,
        24.005, 24.006, 24.005, 24.006, 24.005, 24.006,
    ])
    logs = []
    cfg = {
        "workflow": {
            "stability": {
                "temperature": {
                    "tol": 0.2,
                    "timeout_s": 120,
                    "soak_after_reach_s": 1.0,
                    "reuse_running_in_tol_without_soak": False,
                    "analyzer_chamber_temp_window_s": 0.5,
                    "analyzer_chamber_temp_span_c": 0.02,
                    "analyzer_chamber_temp_timeout_s": 6.0,
                    "analyzer_chamber_temp_poll_s": 0.1,
                }
            }
        }
    }
    logger = RunLogger(tmp_path)
    try:
        r = runner_mod.CalibrationRunner(
            cfg,
            {"temp_chamber": chamber},
            logger,
            logs.append,
            lambda *_: None,
        )
        r._active_gas_analyzers = lambda: [("ga01", analyzer, {})]  # type: ignore[method-assign]
        r._all_gas_analyzers = lambda: [("ga01", analyzer, {})]  # type: ignore[method-assign]
        r._read_sensor_parsed = lambda *_args, **_kwargs: ("", {"chamber_temp_c": next(analyzer_temps)})  # type: ignore[method-assign]
        assert r._set_temperature(25.0) is True
    finally:
        logger.close()

    assert any("restart window" in msg.lower() for msg in logs)
    assert any("analyzer chamber temp stable" in msg.lower() for msg in logs)


def test_set_temperature_without_soak_returns_on_first_in_tol(monkeypatch, tmp_path: Path) -> None:
    clock = _FakeClock()
    monkeypatch.setattr(runner_mod.time, "time", clock.time)
    monkeypatch.setattr(runner_mod.time, "sleep", clock.sleep)

    chamber = _FakeChamber([25.0, 25.0, 25.0])
    cfg = {
        "workflow": {
            "stability": {
                "temperature": {
                    "tol": 0.2,
                    "timeout_s": 120,
                }
            }
        }
    }
    logger = RunLogger(tmp_path)
    try:
        r = runner_mod.CalibrationRunner(cfg, {"temp_chamber": chamber}, logger, lambda _: None, lambda _: None)
        r._set_temperature(25.0)
    finally:
        logger.close()

    assert chamber.read_calls == 1


def test_same_temperature_soak_only_once(monkeypatch, tmp_path: Path) -> None:
    clock = _FakeClock()
    monkeypatch.setattr(runner_mod.time, "time", clock.time)
    monkeypatch.setattr(runner_mod.time, "sleep", clock.sleep)

    chamber = _FakeChamber([25.0] * 40)
    cfg = {
        "workflow": {
            "stability": {
                "temperature": {
                    "tol": 0.2,
                    "timeout_s": 120,
                    "soak_after_reach_s": 1.0,
                    "reuse_running_in_tol_without_soak": False,
                }
            }
        }
    }
    logger = RunLogger(tmp_path)
    try:
        r = runner_mod.CalibrationRunner(cfg, {"temp_chamber": chamber}, logger, lambda *_: None, lambda *_: None)
        r._set_temperature(25.0)
        first_reads = chamber.read_calls
        r._set_temperature(25.0)
        second_reads = chamber.read_calls - first_reads
    finally:
        logger.close()

    assert first_reads > 1
    assert second_reads == 1
    assert chamber.set_calls == [25.0]


def test_same_temperature_does_not_reissue_command_when_running(monkeypatch, tmp_path: Path) -> None:
    clock = _FakeClock()
    monkeypatch.setattr(runner_mod.time, "time", clock.time)
    monkeypatch.setattr(runner_mod.time, "sleep", clock.sleep)

    chamber = _FakeChamber([25.0] * 40, run_state=1)
    logs = []
    cfg = {
        "workflow": {
            "stability": {
                "temperature": {
                    "tol": 0.2,
                    "timeout_s": 120,
                }
            }
        }
    }
    logger = RunLogger(tmp_path)
    try:
        r = runner_mod.CalibrationRunner(cfg, {"temp_chamber": chamber}, logger, logs.append, lambda *_: None)
        assert r._set_temperature(25.0) is True
        assert r._set_temperature(25.0) is True
    finally:
        logger.close()

    assert chamber.set_calls == [25.0]
    assert any("target unchanged; keep current command" in msg.lower() for msg in logs)


def test_same_temperature_skips_repeated_analyzer_stability_after_first_success(monkeypatch, tmp_path: Path) -> None:
    clock = _FakeClock()
    monkeypatch.setattr(runner_mod.time, "time", clock.time)
    monkeypatch.setattr(runner_mod.time, "sleep", clock.sleep)

    chamber = _FakeChamber([25.0] * 20, run_state=1)
    cfg = {
        "workflow": {
            "stability": {
                "temperature": {
                    "tol": 0.2,
                    "timeout_s": 120,
                }
            }
        }
    }
    logger = RunLogger(tmp_path)
    wait_calls = []
    try:
        r = runner_mod.CalibrationRunner(cfg, {"temp_chamber": chamber}, logger, lambda *_: None, lambda *_: None)
        r._wait_analyzer_chamber_temp_stable = lambda *_args, **_kwargs: wait_calls.append("wait") or True  # type: ignore[method-assign]
        assert r._set_temperature(25.0) is True
        assert r._set_temperature(25.0) is True
    finally:
        logger.close()

    assert wait_calls == ["wait"]
    assert chamber.set_calls == [25.0]


def test_soak_restarts_if_end_out_of_tolerance(monkeypatch, tmp_path: Path) -> None:
    clock = _FakeClock()
    monkeypatch.setattr(runner_mod.time, "time", clock.time)
    monkeypatch.setattr(runner_mod.time, "sleep", clock.sleep)

    # First soak window ends out of tolerance, then chamber re-enters tolerance and completes soak.
    chamber = _FakeChamber([25.0] + [24.0] * 30 + [25.0] * 30)
    cfg = {
        "workflow": {
            "stability": {
                "temperature": {
                    "tol": 0.2,
                    "timeout_s": 120,
                    "soak_after_reach_s": 1.0,
                    "reuse_running_in_tol_without_soak": False,
                }
            }
        }
    }
    logs = []
    logger = RunLogger(tmp_path)
    try:
        r = runner_mod.CalibrationRunner(cfg, {"temp_chamber": chamber}, logger, logs.append, lambda *_: None)
        r._set_temperature(25.0)
    finally:
        logger.close()

    assert chamber.read_calls > 20
    assert any("soak done" in msg.lower() for msg in logs)


def test_temperature_change_keeps_running_chamber_on_target_change(monkeypatch, tmp_path: Path) -> None:
    clock = _FakeClock()
    monkeypatch.setattr(runner_mod.time, "time", clock.time)
    monkeypatch.setattr(runner_mod.time, "sleep", clock.sleep)

    chamber = _FakeChamber([25.0] * 60)
    cfg = {
        "workflow": {
            "stability": {
                "temperature": {
                    "tol": 0.2,
                    "timeout_s": 120,
                    "soak_after_reach_s": 0.0,
                }
            }
        }
    }
    logger = RunLogger(tmp_path)
    try:
        r = runner_mod.CalibrationRunner(cfg, {"temp_chamber": chamber}, logger, lambda *_: None, lambda *_: None)
        r._set_temperature(25.0)
        r._set_temperature(26.0)
    finally:
        logger.close()

    assert chamber.stop_calls == 0
    assert chamber.set_calls == [25.0, 26.0]


def test_temperature_change_can_restart_when_explicitly_enabled(monkeypatch, tmp_path: Path) -> None:
    clock = _FakeClock()
    monkeypatch.setattr(runner_mod.time, "time", clock.time)
    monkeypatch.setattr(runner_mod.time, "sleep", clock.sleep)

    chamber = _FakeChamber([25.0] * 60)
    cfg = {
        "workflow": {
            "stability": {
                "temperature": {
                    "tol": 0.2,
                    "timeout_s": 120,
                    "soak_after_reach_s": 0.0,
                    "restart_on_target_change": True,
                }
            }
        }
    }
    logger = RunLogger(tmp_path)
    try:
        r = runner_mod.CalibrationRunner(cfg, {"temp_chamber": chamber}, logger, lambda *_: None, lambda *_: None)
        r._set_temperature(25.0)
        r._set_temperature(26.0)
    finally:
        logger.close()

    assert chamber.stop_calls >= 2


def test_reuse_running_in_tol_still_performs_post_reach_stability(monkeypatch, tmp_path: Path) -> None:
    clock = _FakeClock()
    monkeypatch.setattr(runner_mod.time, "time", clock.time)
    monkeypatch.setattr(runner_mod.time, "sleep", clock.sleep)

    chamber = _FakeChamber([25.0] * 40, run_state=1)
    analyzer = _FakeAnalyzer()
    analyzer_temps = iter([25.000, 25.005, 25.004, 25.006, 25.005, 25.004, 25.006, 25.005])
    cfg = {
        "workflow": {
            "stability": {
                "temperature": {
                    "tol": 0.2,
                    "timeout_s": 120,
                    "soak_after_reach_s": 1.0,
                    "reuse_running_in_tol_without_soak": True,
                    "analyzer_chamber_temp_window_s": 0.5,
                    "analyzer_chamber_temp_span_c": 0.02,
                    "analyzer_chamber_temp_timeout_s": 5.0,
                    "analyzer_chamber_temp_poll_s": 0.1,
                }
            }
        }
    }
    logs = []
    logger = RunLogger(tmp_path)
    try:
        r = runner_mod.CalibrationRunner(cfg, {"temp_chamber": chamber}, logger, logs.append, lambda *_: None)
        r._active_gas_analyzers = lambda: [("ga01", analyzer, {})]  # type: ignore[method-assign]
        r._all_gas_analyzers = lambda: [("ga01", analyzer, {})]  # type: ignore[method-assign]
        r._read_sensor_parsed = lambda *_args, **_kwargs: ("", {"chamber_temp_c": next(analyzer_temps)})  # type: ignore[method-assign]
        assert r._set_temperature(25.0) is True
    finally:
        logger.close()

    assert chamber.stop_calls == 0
    assert chamber.started is False
    assert chamber.set_calls == []
    assert any("for soak/stability wait" in msg.lower() for msg in logs)
    assert any("analyzer chamber temp stable" in msg.lower() for msg in logs)


def test_reuse_running_in_tol_does_not_skip_when_chamber_is_stopped(monkeypatch, tmp_path: Path) -> None:
    clock = _FakeClock()
    monkeypatch.setattr(runner_mod.time, "time", clock.time)
    monkeypatch.setattr(runner_mod.time, "sleep", clock.sleep)

    chamber = _FakeChamber([25.0] * 40, run_state=0)
    cfg = {
        "workflow": {
            "stability": {
                "temperature": {
                    "tol": 0.2,
                    "timeout_s": 120,
                    "soak_after_reach_s": 1.0,
                    "reuse_running_in_tol_without_soak": True,
                }
            }
        }
    }
    logs = []
    logger = RunLogger(tmp_path)
    try:
        r = runner_mod.CalibrationRunner(cfg, {"temp_chamber": chamber}, logger, logs.append, lambda *_: None)
        assert r._set_temperature(25.0) is True
    finally:
        logger.close()

    assert chamber.stop_calls == 0
    assert chamber.started is True
    assert chamber.set_calls == [25.0]
    assert any("controller is not running" in msg.lower() for msg in logs)


def test_temperature_change_fails_fast_when_transition_stalls(monkeypatch, tmp_path: Path) -> None:
    clock = _FakeClock()
    monkeypatch.setattr(runner_mod.time, "time", clock.time)
    monkeypatch.setattr(runner_mod.time, "sleep", clock.sleep)

    chamber = _FakeChamber([20.0] * 40)
    cfg = {
        "workflow": {
            "stability": {
                "temperature": {
                    "tol": 0.2,
                    "timeout_s": 120,
                    "transition_check_window_s": 1.0,
                    "transition_min_delta_c": 0.5,
                }
            }
        }
    }
    logs = []
    logger = RunLogger(tmp_path)
    try:
        r = runner_mod.CalibrationRunner(cfg, {"temp_chamber": chamber}, logger, logs.append, lambda *_: None)
        assert r._set_temperature(20.0) is True
        assert r._set_temperature(30.0) is False
    finally:
        logger.close()

    assert chamber.stop_calls == 0
    assert chamber.set_calls == [20.0, 30.0]
    assert any("stalled before reaching target" in msg.lower() for msg in logs)


def test_temperature_change_succeeds_when_new_target_moves_in_right_direction(monkeypatch, tmp_path: Path) -> None:
    clock = _FakeClock()
    monkeypatch.setattr(runner_mod.time, "time", clock.time)
    monkeypatch.setattr(runner_mod.time, "sleep", clock.sleep)

    chamber = _FakeChamber([20.0, 20.0, 20.0, 20.3, 20.6, 29.9, 30.0])
    cfg = {
        "workflow": {
            "stability": {
                "temperature": {
                    "tol": 0.2,
                    "timeout_s": 120,
                    "transition_check_window_s": 1.0,
                    "transition_min_delta_c": 0.5,
                }
            }
        }
    }
    logger = RunLogger(tmp_path)
    try:
        r = runner_mod.CalibrationRunner(cfg, {"temp_chamber": chamber}, logger, lambda *_: None, lambda *_: None)
        assert r._set_temperature(20.0) is True
        assert r._set_temperature(30.0) is True
    finally:
        logger.close()

    assert chamber.stop_calls == 0
    assert chamber.set_calls == [20.0, 30.0]


def test_temperature_change_succeeds_when_new_target_moves_downward(monkeypatch, tmp_path: Path) -> None:
    clock = _FakeClock()
    monkeypatch.setattr(runner_mod.time, "time", clock.time)
    monkeypatch.setattr(runner_mod.time, "sleep", clock.sleep)

    chamber = _FakeChamber([30.0, 30.0, 30.0, 29.6, 29.3, 20.2, 20.0], run_state=1)
    cfg = {
        "workflow": {
            "stability": {
                "temperature": {
                    "tol": 0.2,
                    "timeout_s": 120,
                    "transition_check_window_s": 1.0,
                    "transition_min_delta_c": 0.5,
                }
            }
        }
    }
    logger = RunLogger(tmp_path)
    try:
        r = runner_mod.CalibrationRunner(cfg, {"temp_chamber": chamber}, logger, lambda *_: None, lambda *_: None)
        assert r._set_temperature(30.0) is True
        assert r._set_temperature(20.0) is True
    finally:
        logger.close()

    assert chamber.stop_calls == 0
    assert chamber.set_calls == [30.0, 20.0]


def test_set_temperature_rewrites_setpoint_once_when_readback_mismatches(monkeypatch, tmp_path: Path) -> None:
    clock = _FakeClock()
    monkeypatch.setattr(runner_mod.time, "time", clock.time)
    monkeypatch.setattr(runner_mod.time, "sleep", clock.sleep)

    chamber = _FakeChamber([20.0, 20.0, 20.0], run_state=1, set_temp_readbacks=[18.0, 20.0])
    cfg = {
        "workflow": {
            "stability": {
                "temperature": {
                    "tol": 0.2,
                    "timeout_s": 120,
                    "soak_after_reach_s": 0.0,
                }
            }
        }
    }
    logs = []
    logger = RunLogger(tmp_path)
    try:
        r = runner_mod.CalibrationRunner(cfg, {"temp_chamber": chamber}, logger, logs.append, lambda *_: None)
        assert r._set_temperature(20.0) is True
    finally:
        logger.close()

    assert chamber.set_calls == [20.0, 20.0]
    assert any("setpoint readback mismatch" in msg.lower() for msg in logs)
    assert any("after rewrite" in msg.lower() for msg in logs)


def test_set_temperature_allows_start_state_mismatch_when_temperature_reaches_target(monkeypatch, tmp_path: Path) -> None:
    clock = _FakeClock()
    monkeypatch.setattr(runner_mod.time, "time", clock.time)
    monkeypatch.setattr(runner_mod.time, "sleep", clock.sleep)

    chamber = _StartMismatchChamber([20.0, 20.0, 20.0], run_state=0)
    logs = []
    cfg = {
        "workflow": {
            "stability": {
                "temperature": {
                    "tol": 0.2,
                    "timeout_s": 120,
                    "soak_after_reach_s": 0.0,
                }
            }
        }
    }
    logger = RunLogger(tmp_path)
    try:
        r = runner_mod.CalibrationRunner(cfg, {"temp_chamber": chamber}, logger, logs.append, lambda *_: None)
        assert r._set_temperature(20.0) is True
    finally:
        logger.close()

    assert chamber.set_calls == [20.0]
    assert any("start state mismatch" in msg.lower() for msg in logs)
    assert any("actual temperature trend" in msg.lower() for msg in logs)


def test_set_temperature_allows_stale_run_state_when_temperature_moves(monkeypatch, tmp_path: Path) -> None:
    clock = _FakeClock()
    monkeypatch.setattr(runner_mod.time, "time", clock.time)
    monkeypatch.setattr(runner_mod.time, "sleep", clock.sleep)

    chamber = _NonStartingChamber([19.0, 19.6, 20.0, 20.0], run_state=0)
    logs = []
    cfg = {
        "workflow": {
            "stability": {
                "temperature": {
                    "tol": 0.2,
                    "timeout_s": 120,
                    "soak_after_reach_s": 0.0,
                }
            }
        }
    }
    logger = RunLogger(tmp_path)
    try:
        r = runner_mod.CalibrationRunner(cfg, {"temp_chamber": chamber}, logger, logs.append, lambda *_: None)
        assert r._set_temperature(20.0) is True
    finally:
        logger.close()

    assert chamber.stop_calls >= 1
    assert any("still stop after retry start command" in msg.lower() for msg in logs)
    assert any("actual temperature trend" in msg.lower() for msg in logs)


def test_temperature_slow_cooling_continues_waiting_while_progressing(monkeypatch, tmp_path: Path) -> None:
    clock = _FakeClock()
    monkeypatch.setattr(runner_mod.time, "time", clock.time)
    monkeypatch.setattr(runner_mod.time, "sleep", clock.sleep)

    chamber = _FakeChamber([25.0, 24.9, 24.8, 24.7, 24.6, 24.5, 24.4, 20.2, 20.0], run_state=1)
    logs = []
    cfg = {
        "workflow": {
            "stability": {
                "temperature": {
                    "tol": 0.2,
                    "timeout_s": 0.5,
                    "continue_wait_while_progress": True,
                    "progress_window_s": 2.0,
                    "progress_min_delta_c": 0.05,
                    "hard_max_wait_s": 0.0,
                    "soak_after_reach_s": 0.0,
                    "analyzer_chamber_temp_enabled": False,
                }
            }
        }
    }
    logger = RunLogger(tmp_path)
    try:
        r = runner_mod.CalibrationRunner(cfg, {"temp_chamber": chamber}, logger, logs.append, lambda *_: None)
        assert r._set_temperature(20.0) is True
    finally:
        logger.close()

    assert any("continue waiting" in msg.lower() for msg in logs)
    assert not any("stalled before reaching target" in msg.lower() for msg in logs)


def test_temperature_slow_heating_continues_waiting_while_progressing(monkeypatch, tmp_path: Path) -> None:
    clock = _FakeClock()
    monkeypatch.setattr(runner_mod.time, "time", clock.time)
    monkeypatch.setattr(runner_mod.time, "sleep", clock.sleep)

    chamber = _FakeChamber([-20.0, -19.8, -19.6, -19.4, -19.2, -19.0, -18.8, -10.1, -10.0], run_state=1)
    logs = []
    cfg = {
        "workflow": {
            "stability": {
                "temperature": {
                    "tol": 0.2,
                    "timeout_s": 0.5,
                    "continue_wait_while_progress": True,
                    "progress_window_s": 2.0,
                    "progress_min_delta_c": 0.05,
                    "hard_max_wait_s": 0.0,
                    "soak_after_reach_s": 0.0,
                    "analyzer_chamber_temp_enabled": False,
                }
            }
        }
    }
    logger = RunLogger(tmp_path)
    try:
        r = runner_mod.CalibrationRunner(cfg, {"temp_chamber": chamber}, logger, logs.append, lambda *_: None)
        assert r._set_temperature(-10.0) is True
    finally:
        logger.close()

    assert any("continue waiting" in msg.lower() for msg in logs)
    assert not any("stalled before reaching target" in msg.lower() for msg in logs)


def test_set_temperature_can_skip_wait_for_target_when_enabled(monkeypatch, tmp_path: Path) -> None:
    clock = _FakeClock()
    monkeypatch.setattr(runner_mod.time, "time", clock.time)
    monkeypatch.setattr(runner_mod.time, "sleep", clock.sleep)

    chamber = _FakeChamber([20.0, 20.0, 20.0], run_state=1)
    logs = []
    cfg = {
        "workflow": {
            "stability": {
                "temperature": {
                    "tol": 0.2,
                    "timeout_s": 120,
                    "wait_for_target_before_continue": False,
                }
            }
        }
    }
    logger = RunLogger(tmp_path)
    try:
        r = runner_mod.CalibrationRunner(cfg, {"temp_chamber": chamber}, logger, logs.append, lambda *_: None)
        assert r._set_temperature(30.0) is True
    finally:
        logger.close()

    assert chamber.started is True
    assert chamber.set_calls == [30.0]
    assert chamber.read_calls == 0
    assert any("target wait skipped by configuration" in msg.lower() for msg in logs)


def test_temperature_wait_fails_only_when_no_meaningful_progress(monkeypatch, tmp_path: Path) -> None:
    clock = _FakeClock()
    monkeypatch.setattr(runner_mod.time, "time", clock.time)
    monkeypatch.setattr(runner_mod.time, "sleep", clock.sleep)

    chamber = _FakeChamber([20.0] * 40, run_state=1)
    logs = []
    cfg = {
        "workflow": {
            "stability": {
                "temperature": {
                    "tol": 0.2,
                    "timeout_s": 1.0,
                    "continue_wait_while_progress": True,
                    "progress_window_s": 0.8,
                    "progress_min_delta_c": 0.05,
                    "hard_max_wait_s": 0.0,
                    "soak_after_reach_s": 30.0,
                    "analyzer_chamber_temp_timeout_s": 60.0,
                }
            }
        }
    }
    logger = RunLogger(tmp_path)
    try:
        r = runner_mod.CalibrationRunner(cfg, {"temp_chamber": chamber}, logger, logs.append, lambda *_: None)
        assert r._set_temperature(25.0) is False
    finally:
        logger.close()

    assert any("stalled before reaching target" in msg.lower() for msg in logs)
