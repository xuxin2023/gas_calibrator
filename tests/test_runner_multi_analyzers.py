from pathlib import Path
import time

import pytest

from gas_calibrator.data.points import CalibrationPoint
from gas_calibrator.logging_utils import RunLogger
from gas_calibrator.workflow.runner import CalibrationRunner


class _FakeGasAnalyzer:
    def __init__(self, parsed):
        self.parsed = self._normalized_parsed(parsed)
        self.calls = []

    @staticmethod
    def _normalized_parsed(parsed):
        payload = {
            "co2_ratio_f": 1.0,
            "h2o_ratio_f": 1.0,
            "pressure_kpa": 101.3,
            "mode2_field_count": 16,
        }
        payload.update(dict(parsed))
        return payload

    def set_mode(self, mode):
        self.calls.append(("mode", mode, True))

    def set_mode_with_ack(self, mode, require_ack=True):
        self.calls.append(("mode", mode, bool(require_ack)))
        return True

    def set_comm_way(self, active):
        self.calls.append(("active", active, True))
        return True

    def set_comm_way_with_ack(self, active, require_ack=True):
        self.calls.append(("active", active, bool(require_ack)))
        return True

    def set_active_freq(self, hz):
        self.calls.append(("ftd", hz))
        return True

    def set_average(self, co2_n, h2o_n):
        self.calls.append(("avg", co2_n, h2o_n))
        return True

    def set_average_filter(self, window_n):
        self.calls.append(("avg_filter", window_n, True))
        return True

    def set_average_filter_with_ack(self, window_n, require_ack=True):
        self.calls.append(("avg_filter", window_n, bool(require_ack)))
        return True

    @staticmethod
    def read_data_passive():
        return "YGAS,001,500.0,2.0,1,1,1,1,1,1,1,1,25.0,25.0,101.3,OK"

    def read_latest_data(self, *args, **kwargs):
        return self.read_data_passive()

    def parse_line_mode2(self, _line):
        return dict(self.parsed)

    def close(self):
        return None


class _FakeRecoveringGasAnalyzer(_FakeGasAnalyzer):
    def __init__(self, parsed_sequence):
        super().__init__({})
        self._parsed_sequence = list(parsed_sequence)

    def parse_line_mode2(self, _line):
        if self._parsed_sequence:
            parsed = self._parsed_sequence.pop(0)
            return None if parsed is None else self._normalized_parsed(parsed)
        return None


class _FakeStreamingGasAnalyzer(_FakeGasAnalyzer):
    def __init__(self, parsed):
        super().__init__(parsed)
        self.read_latest_calls = 0
        self.read_passive_calls = 0

    def read_latest_data(self):
        self.read_latest_calls += 1
        return "YGAS,001,500.0,2.0,1,1,1,1,1,1,1,1,25.0,25.0,101.3,OK"

    def read_data_passive(self):
        self.read_passive_calls += 1
        raise AssertionError("passive read should not be used when stream reader is available")


class _FakeStreamingBatchGasAnalyzer(_FakeGasAnalyzer):
    def __init__(self, batches):
        super().__init__({})
        self._batches = list(batches)
        self.drain_calls = 0

    def _drain_stream_lines(self, drain_s=0.35, read_timeout_s=0.05):
        self.drain_calls += 1
        if self._batches:
            return list(self._batches.pop(0))
        return []

    def parse_line_mode2(self, line):
        if "500.0,2.0" in str(line):
            return self._normalized_parsed(
                {"co2_ppm": 500.0, "h2o_mmol": 2.0, "co2_ratio_f": 1.0, "h2o_ratio_f": 1.0}
            )
        return None


class _FakeLegacyStreamingGasAnalyzer(_FakeGasAnalyzer):
    def __init__(self):
        super().__init__({})
        self.read_latest_calls = 0
        self.read_passive_calls = 0

    def read_latest_data(self, *args, **kwargs):
        self.read_latest_calls += 1
        return "YGAS,001,500.0,2.0,0.99,0.99,25.0,101.3,0301,2798"

    def read_data_passive(self):
        self.read_passive_calls += 1
        return "YGAS,001,500.0,2.0,0.99,0.99,25.0,101.3,0301,2798"

    def parse_line_mode2(self, _line):
        return None


class _FakeAckFailingGasAnalyzer(_FakeGasAnalyzer):
    def set_average(self, co2_n, h2o_n):
        self.calls.append(("avg", co2_n, h2o_n))
        return False


def _point() -> CalibrationPoint:
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


def _active_frame(seq: int, recv_mono_s: float, co2_ppm: float, *, h2o_mmol: float = 2.0) -> dict:
    return {
        "recv_wall_ts": f"2026-03-30T12:00:{int(recv_mono_s * 10):02d}.000",
        "timestamp": 1000.0 + recv_mono_s,
        "recv_mono_s": recv_mono_s,
        "seq": seq,
        "line": f"FRAME-{seq}",
        "parsed": {
            "co2_ppm": co2_ppm,
            "h2o_mmol": h2o_mmol,
            "co2_ratio_f": 1.0,
            "h2o_ratio_f": 0.1,
            "pressure_kpa": 101.3,
            "mode2_field_count": 16,
        },
        "category": "parsed",
        "source": "active_stream",
        "is_live": True,
    }


def test_configure_devices_forces_mode2_for_all_analyzers(tmp_path: Path) -> None:
    cfg = {
        "devices": {
            "gas_analyzer": {"active_send": False, "ftd_hz": 1, "average_co2": 1, "average_h2o": 1},
            "gas_analyzers": [
                {"name": "ga01", "active_send": False, "ftd_hz": 1, "average_co2": 1, "average_h2o": 1},
                {"name": "ga02", "active_send": True, "ftd_hz": 2, "average_co2": 2, "average_h2o": 3},
            ],
            "pressure_controller": {"in_limits_pct": 0.02, "in_limits_time_s": 10},
        }
    }
    devices = {
        "gas_analyzer_01": _FakeGasAnalyzer({"co2_ppm": 500.0, "h2o_mmol": 2.0}),
        "gas_analyzer_02": _FakeGasAnalyzer({"co2_ppm": 600.0, "h2o_mmol": 3.0}),
    }
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(cfg, devices, logger, lambda *_: None, lambda *_: None)

    runner._configure_devices()

    assert ("mode", 2, False) in devices["gas_analyzer_01"].calls
    assert ("mode", 2, False) in devices["gas_analyzer_02"].calls
    assert ("active", False, False) in devices["gas_analyzer_01"].calls
    assert ("active", False, False) in devices["gas_analyzer_02"].calls
    assert ("active", True, False) not in devices["gas_analyzer_01"].calls
    assert ("active", True, False) in devices["gas_analyzer_02"].calls
    assert ("ftd", 1) in devices["gas_analyzer_01"].calls
    assert ("ftd", 2) in devices["gas_analyzer_02"].calls
    assert ("avg_filter", 49, False) in devices["gas_analyzer_01"].calls
    assert ("avg_filter", 49, False) in devices["gas_analyzer_02"].calls
    assert not any(call[0] == "avg" for call in devices["gas_analyzer_01"].calls)
    assert not any(call[0] == "avg" for call in devices["gas_analyzer_02"].calls)
    logger.close()


def test_configure_devices_disables_failing_analyzer_when_others_reach_mode2(tmp_path: Path) -> None:
    cfg = {
        "devices": {
            "gas_analyzer": {"active_send": True, "ftd_hz": 10, "average_co2": 1, "average_h2o": 1},
            "gas_analyzers": [
                {"name": "ga01", "active_send": False},
                {"name": "ga02", "active_send": True},
            ],
            "pressure_controller": {"in_limits_pct": 0.02, "in_limits_time_s": 10},
        }
    }
    devices = {
        "gas_analyzer_01": _FakeGasAnalyzer({"co2_ppm": 500.0, "h2o_mmol": 2.0}),
        "gas_analyzer_02": _FakeLegacyStreamingGasAnalyzer(),
    }
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(cfg, devices, logger, lambda *_: None, lambda *_: None)

    runner._configure_devices()

    assert devices["gas_analyzer_02"].read_latest_calls >= 1
    assert "ga02" in runner._disabled_analyzers
    assert runner._disabled_analyzer_reasons["ga02"] == "startup_mode2_verify_failed"
    assert "ga01" not in runner._disabled_analyzers
    assert "ga02" in runner._disabled_analyzer_last_reprobe_ts
    logger.close()


def test_configure_devices_raises_when_no_analyzer_reaches_mode2(tmp_path: Path) -> None:
    cfg = {
        "devices": {
            "gas_analyzer": {"active_send": True, "ftd_hz": 10, "average_co2": 1, "average_h2o": 1},
            "gas_analyzers": [{"name": "ga01", "active_send": True}],
            "pressure_controller": {"in_limits_pct": 0.02, "in_limits_time_s": 10},
        }
    }
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(
        cfg,
        {"gas_analyzer_01": _FakeLegacyStreamingGasAnalyzer()},
        logger,
        lambda *_: None,
        lambda *_: None,
    )

    with pytest.raises(RuntimeError, match="No gas analyzers available after startup configuration"):
        runner._configure_devices()

    assert "ga01" in runner._disabled_analyzers
    assert runner._disabled_analyzer_reasons["ga01"] == "startup_mode2_verify_failed"
    logger.close()


def test_configure_devices_accepts_startup_mode2_frame_outside_sampling_pressure_range(tmp_path: Path) -> None:
    cfg = {
        "devices": {
            "gas_analyzer": {"active_send": True, "ftd_hz": 10, "average_co2": 1, "average_h2o": 1},
            "gas_analyzers": [{"name": "ga01", "active_send": True}],
            "pressure_controller": {"in_limits_pct": 0.02, "in_limits_time_s": 10},
        }
    }
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(
        cfg,
        {
            "gas_analyzer_01": _FakeGasAnalyzer(
                {
                    "co2_ppm": 1.066,
                    "h2o_mmol": 0.762,
                    "co2_ratio_f": 1.0654,
                    "h2o_ratio_f": 0.7617,
                    "pressure_kpa": 190.57,
                    "mode2_field_count": 16,
                }
            )
        },
        logger,
        lambda *_: None,
        lambda *_: None,
    )

    runner._configure_devices()

    assert ("active", True, False) in runner.devices["gas_analyzer_01"].calls

    logger.close()


def test_configure_devices_raises_when_mode2_frame_missing_startup_required_key(tmp_path: Path) -> None:
    cfg = {
        "devices": {
            "gas_analyzer": {"active_send": True, "ftd_hz": 10, "average_co2": 1, "average_h2o": 1},
            "gas_analyzers": [{"name": "ga01", "active_send": True}],
            "pressure_controller": {"in_limits_pct": 0.02, "in_limits_time_s": 10},
        }
    }
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(
        cfg,
        {
            "gas_analyzer_01": _FakeGasAnalyzer(
                {
                    "co2_ppm": 500.0,
                    "h2o_mmol": 2.0,
                    "co2_ratio_f": None,
                    "h2o_ratio_f": 0.7,
                    "mode2_field_count": 16,
                }
            )
        },
        logger,
        lambda *_: None,
        lambda *_: None,
    )

    with pytest.raises(RuntimeError, match="No gas analyzers available after startup configuration"):
        runner._configure_devices()

    assert "ga01" in runner._disabled_analyzers
    assert runner._disabled_analyzer_reasons["ga01"] == "startup_mode2_verify_failed"
    logger.close()


def test_configure_gas_analyzer_reapplies_minimal_commands_until_mode2_frame_arrives(tmp_path: Path) -> None:
    cfg = {
        "devices": {
            "gas_analyzer": {"active_send": True, "ftd_hz": 10, "average_co2": 1, "average_h2o": 1},
            "gas_analyzers": [{"name": "ga01", "active_send": True}],
        },
        "workflow": {
            "analyzer_mode2_init": {
                "reapply_attempts": 3,
                "stream_attempts": 1,
                "retry_delay_s": 0.0,
                "reapply_delay_s": 0.0,
            }
        },
    }
    analyzer = _FakeRecoveringGasAnalyzer(
        [
            None,
            {"co2_ppm": 500.0, "h2o_mmol": 2.0, "co2_ratio_f": 1.0, "mode2_field_count": 16},
            {"co2_ppm": 500.0, "h2o_mmol": 2.0, "co2_ratio_f": 1.0, "mode2_field_count": 16},
            {"co2_ppm": 500.0, "h2o_mmol": 2.0, "co2_ratio_f": 1.0, "mode2_field_count": 16},
        ]
    )
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(cfg, {"gas_analyzer_01": analyzer}, logger, lambda *_: None, lambda *_: None)

    runner._configure_gas_analyzer(
        analyzer,
        label="ga01",
        mode=2,
        active_send=True,
        ftd_hz=10,
        avg_co2=1,
        avg_h2o=1,
        avg_filter=49,
        warning_phase="startup",
    )

    mode_calls = [call for call in analyzer.calls if call[0] == "mode"]
    assert len(mode_calls) == 2
    active_true_calls = [call for call in analyzer.calls if call[0] == "active" and call[1] is True]
    assert len(active_true_calls) == 2
    logger.close()


def test_configure_gas_analyzer_accepts_mode2_frame_after_success_ack_in_same_stream_window(tmp_path: Path) -> None:
    cfg = {
        "devices": {
            "gas_analyzer": {"active_send": True, "ftd_hz": 10, "average_co2": 1, "average_h2o": 1},
            "gas_analyzers": [{"name": "ga01", "active_send": True}],
        },
        "workflow": {
            "analyzer_mode2_init": {
                "reapply_attempts": 1,
                "stream_attempts": 1,
                "ready_consecutive_frames": 1,
                "retry_delay_s": 0.0,
                "reapply_delay_s": 0.0,
                "post_enable_stream_wait_s": 0.0,
            }
        },
    }
    analyzer = _FakeStreamingBatchGasAnalyzer(
        [
            [
                "YGAS,097,T",
                "YGAS,097,500.0,2.0,1,1,1,1,1,1,1,1,25.0,25.0,101.3,OK",
            ]
        ]
    )
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(cfg, {"gas_analyzer_01": analyzer}, logger, lambda *_: None, lambda *_: None)

    runner._configure_gas_analyzer(
        analyzer,
        label="ga01",
        mode=2,
        active_send=True,
        ftd_hz=10,
        avg_co2=1,
        avg_h2o=1,
        avg_filter=49,
        warning_phase="startup",
    )

    assert analyzer.drain_calls == 1
    assert ("active", True, False) in analyzer.calls
    logger.close()


def test_configure_gas_analyzer_waits_after_success_ack_before_reapplying(tmp_path: Path) -> None:
    cfg = {
        "devices": {
            "gas_analyzer": {"active_send": True, "ftd_hz": 10, "average_co2": 1, "average_h2o": 1},
            "gas_analyzers": [{"name": "ga01", "active_send": True}],
        },
        "workflow": {
            "analyzer_mode2_init": {
                "reapply_attempts": 2,
                "stream_attempts": 1,
                "ready_consecutive_frames": 1,
                "retry_delay_s": 0.0,
                "reapply_delay_s": 0.0,
                "command_gap_s": 0.0,
                "post_enable_stream_wait_s": 0.0,
                "post_enable_stream_ack_wait_s": 0.0,
            }
        },
    }
    analyzer = _FakeStreamingBatchGasAnalyzer(
        [
            ["YGAS,097,T"],
            ["YGAS,097,500.0,2.0,1,1,1,1,1,1,1,1,25.0,25.0,101.3,OK"],
        ]
    )
    analyzer._is_success_ack = lambda line: str(line).strip() == "YGAS,097,T"
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(cfg, {"gas_analyzer_01": analyzer}, logger, lambda *_: None, lambda *_: None)

    runner._configure_gas_analyzer(
        analyzer,
        label="ga01",
        mode=2,
        active_send=True,
        ftd_hz=10,
        avg_co2=1,
        avg_h2o=1,
        avg_filter=49,
        warning_phase="startup",
    )

    mode_calls = [call for call in analyzer.calls if call[0] == "mode"]
    assert len(mode_calls) == 1
    logger.close()


def test_read_mode2_frame_stream_branch_handles_empty_windows_without_crashing(tmp_path: Path) -> None:
    cfg = {
        "devices": {
            "gas_analyzer": {"active_send": True, "ftd_hz": 10, "average_co2": 1, "average_h2o": 1},
        }
    }
    analyzer = _FakeStreamingBatchGasAnalyzer([[], ["YGAS,097,T"]])
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(cfg, {"gas_analyzer_01": analyzer}, logger, lambda *_: None, lambda *_: None)

    line, parsed = runner._read_mode2_frame(
        analyzer,
        prefer_stream=True,
        ftd_hz=10,
        attempts=2,
        retry_delay_s=0.0,
    )

    assert parsed is None
    assert line == "YGAS,097,T"
    logger.close()


def test_configure_devices_keeps_analyzer_when_config_ack_is_missing_but_verify_passes(tmp_path: Path) -> None:
    cfg = {
        "devices": {
            "gas_analyzer": {"active_send": True, "ftd_hz": 1, "average_co2": 1, "average_h2o": 1},
            "gas_analyzers": [
                {"name": "ga01"},
                {"name": "ga02", "active_send": True},
            ],
            "pressure_controller": {"in_limits_pct": 0.02, "in_limits_time_s": 10},
        }
    }
    devices = {
        "gas_analyzer_01": _FakeGasAnalyzer({"co2_ppm": 500.0, "h2o_mmol": 2.0}),
        "gas_analyzer_02": _FakeAckFailingGasAnalyzer({"co2_ppm": 600.0, "h2o_mmol": 3.0}),
    }
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(cfg, devices, logger, lambda *_: None, lambda *_: None)

    runner._configure_devices()

    assert "ga02" not in runner._disabled_analyzers
    assert ("active", True, False) in devices["gas_analyzer_02"].calls
    logger.close()


def test_collect_samples_writes_prefixed_sensor_fields(tmp_path: Path) -> None:
    cfg = {
        "devices": {
            "gas_analyzer": {"active_send": False, "ftd_hz": 1, "average_co2": 1, "average_h2o": 1},
            "gas_analyzers": [
                {"name": "ga01"},
                {"name": "ga02"},
            ],
        },
        "workflow": {"sampling": {"count": 1, "stable_count": 1, "interval_s": 0.0}},
    }
    devices = {
        "gas_analyzer_01": _FakeGasAnalyzer({"co2_ppm": 501.0, "h2o_mmol": 2.1, "co2_signal": 123.0}),
        "gas_analyzer_02": _FakeGasAnalyzer({"co2_ppm": 601.0, "h2o_mmol": 3.1, "co2_signal": 223.0}),
    }
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(cfg, devices, logger, lambda *_: None, lambda *_: None)

    samples = runner._collect_samples(_point(), count=1, interval=0.0)
    assert samples is not None
    assert len(samples) == 1
    row = samples[0]

    # Primary analyzer keeps legacy unprefixed fields for coefficient fitting.
    assert row["co2_ppm"] == 501.0
    # All analyzers are additionally persisted with prefixed keys.
    assert row["ga01_co2_ppm"] == 501.0
    assert row["ga02_co2_ppm"] == 601.0
    assert row["ga01_frame_source"] == "passive_cache"
    assert row["ga02_frame_source"] == "passive_cache"
    assert row["ga01_frame_cache_ts"]
    assert row["ga02_frame_cache_ts"]
    assert row["ga01_frame_cache_age_ms"] >= 0.0
    assert row["ga02_frame_cache_age_ms"] >= 0.0
    logger.close()


def test_collect_samples_prefers_stream_reader_when_available(tmp_path: Path) -> None:
    cfg = {
        "devices": {
            "gas_analyzer": {"active_send": True, "ftd_hz": 10, "average_co2": 1, "average_h2o": 1},
            "gas_analyzers": [{"name": "ga01", "active_send": True}],
        },
        "workflow": {"sampling": {"count": 1, "stable_count": 1, "interval_s": 0.0}},
    }
    analyzer = _FakeStreamingGasAnalyzer({"co2_ppm": 501.0, "h2o_mmol": 2.1})
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(cfg, {"gas_analyzer_01": analyzer}, logger, lambda *_: None, lambda *_: None)

    samples = runner._collect_samples(_point(), count=1, interval=0.0)

    assert samples is not None
    assert analyzer.read_latest_calls == 1
    assert analyzer.read_passive_calls == 0
    assert samples[0]["ga01_frame_source"] == "active_stream"
    assert samples[0]["ga01_frame_is_live"] is True
    logger.close()


def test_active_analyzer_ring_buffer_keeps_multiple_stream_frames(tmp_path: Path) -> None:
    class _FakeDrainAnalyzer(_FakeGasAnalyzer):
        def __init__(self) -> None:
            super().__init__({})

        def _drain_stream_lines(self, drain_s=0.05, read_timeout_s=0.05):
            return ["FRAME-1", "FRAME-2", "FRAME-3"]

        def parse_line_mode2(self, line):
            ppm = {"FRAME-1": 501.0, "FRAME-2": 502.0, "FRAME-3": 503.0}[str(line)]
            return {
                "co2_ppm": ppm,
                "h2o_mmol": 2.0,
                "co2_ratio_f": 1.0,
                "h2o_ratio_f": 0.1,
                "pressure_kpa": 101.3,
                "mode2_field_count": 16,
            }

    cfg = {
        "devices": {
            "gas_analyzer": {"active_send": True, "ftd_hz": 10, "average_co2": 1, "average_h2o": 1},
            "gas_analyzers": [{"name": "ga01", "active_send": True, "ftd_hz": 10}],
        }
    }
    analyzer = _FakeDrainAnalyzer()
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(cfg, {"gas_analyzer_01": analyzer}, logger, lambda *_: None, lambda *_: None)
    context = runner._new_sampling_window_context(point=_point(), phase="co2", point_tag="demo")

    runner._refresh_sampling_analyzer_cache_entry(
        "ga01",
        analyzer,
        {"active_send": True, "ftd_hz": 10},
        context=context,
        reason="test",
    )

    frames = runner._sampling_window_active_analyzer_frames(context, analyzer, label="ga01")
    assert [frame["parsed"]["co2_ppm"] for frame in frames] == [501.0, 502.0, 503.0]
    assert len({frame["seq"] for frame in frames}) == 3
    logger.close()


def test_active_analyzer_anchor_match_prefers_left_frame_over_latest(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner({}, {}, logger, lambda *_: None, lambda *_: None)
    analyzer = object()
    context = runner._new_sampling_window_context(point=_point(), phase="co2", point_tag="demo")
    _runtime_key, buffer = runner._sampling_window_active_analyzer_buffer(context, analyzer, label="ga01")
    buffer.extend(
        [
            _active_frame(1, 10.10, 501.0),
            _active_frame(2, 10.70, 502.0),
            _active_frame(3, 10.90, 503.0),
        ]
    )

    match = runner._active_analyzer_anchor_match(
        context,
        analyzer,
        label="ga01",
        sample_anchor_mono=10.75,
    )

    assert match["entry"]["seq"] == 2
    assert match["side"] == "before_anchor"
    assert match["match_strategy"] == "left_match"
    assert match["delta_ms"] == pytest.approx(50.0)
    logger.close()


def test_active_analyzer_anchor_match_uses_small_right_match_when_left_missing(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner({}, {}, logger, lambda *_: None, lambda *_: None)
    analyzer = object()
    context = runner._new_sampling_window_context(point=_point(), phase="co2", point_tag="demo")
    _runtime_key, buffer = runner._sampling_window_active_analyzer_buffer(context, analyzer, label="ga01")
    buffer.append(_active_frame(1, 10.08, 501.0))

    match = runner._active_analyzer_anchor_match(
        context,
        analyzer,
        label="ga01",
        sample_anchor_mono=10.00,
    )

    assert match["entry"]["seq"] == 1
    assert match["side"] == "after_anchor"
    assert match["match_strategy"] == "right_match"
    assert match["stale"] is False
    assert match["delta_ms"] == pytest.approx(80.0)
    logger.close()


def test_active_analyzer_anchor_match_marks_stale_frames_explicitly(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner({}, {}, logger, lambda *_: None, lambda *_: None)
    analyzer = object()
    context = runner._new_sampling_window_context(point=_point(), phase="co2", point_tag="demo")
    _runtime_key, buffer = runner._sampling_window_active_analyzer_buffer(context, analyzer, label="ga01")
    buffer.extend([_active_frame(1, 9.74, 500.0), _active_frame(2, 9.30, 499.0)])

    near_stale = runner._active_analyzer_anchor_match(
        context,
        analyzer,
        label="ga01",
        sample_anchor_mono=10.00,
    )
    far_context = runner._new_sampling_window_context(point=_point(), phase="co2", point_tag="demo")
    _runtime_key, far_buffer = runner._sampling_window_active_analyzer_buffer(far_context, analyzer, label="ga01")
    far_buffer.append(_active_frame(3, 9.30, 498.0))
    far_stale = runner._active_analyzer_anchor_match(
        far_context,
        analyzer,
        label="ga01",
        sample_anchor_mono=10.00,
    )

    assert near_stale["side"] == "stale"
    assert near_stale["match_strategy"] == "stale_left_fallback"
    assert near_stale["stale"] is True
    assert near_stale["delta_ms"] == pytest.approx(260.0)
    assert far_stale["side"] == "stale"
    assert far_stale["match_strategy"] == "stale_left_far"
    assert far_stale["stale"] is True
    assert far_stale["delta_ms"] == pytest.approx(700.0)
    logger.close()


def test_collect_samples_keeps_soft_marked_extreme_frame_when_ratio_is_usable(tmp_path: Path) -> None:
    cfg = {
        "devices": {
            "gas_analyzer": {"active_send": True, "ftd_hz": 10, "average_co2": 1, "average_h2o": 1},
            "gas_analyzers": [{"name": "ga01", "active_send": True}],
        },
        "workflow": {"sampling": {"count": 1, "stable_count": 1, "interval_s": 0.0}},
    }
    analyzer = _FakeStreamingGasAnalyzer({"co2_ppm": 3000.0, "h2o_mmol": 72.0, "co2_ratio_f": 1.2})
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(cfg, {"gas_analyzer_01": analyzer}, logger, lambda *_: None, lambda *_: None)

    samples = runner._collect_samples(_point(), count=1, interval=0.0)

    assert samples is not None
    row = samples[0]
    assert row["ga01_frame_has_data"] is True
    assert row["ga01_frame_usable"] is True
    assert row["ga01_frame_status"] == "极值已标记"
    '''
    assert row["ga01_frame_status"] == "异常极值"
    '''
    assert row["co2_ppm"] == 3000.0
    logger.close()


def test_attempt_reenable_disabled_analyzers_restores_recovered_device(tmp_path: Path) -> None:
    cfg = {
        "devices": {
            "gas_analyzer": {"active_send": False, "ftd_hz": 1, "average_co2": 1, "average_h2o": 1},
            "gas_analyzers": [
                {"name": "ga01"},
                {"name": "ga06", "active_send": True, "ftd_hz": 2, "average_co2": 2, "average_h2o": 3},
            ],
        }
    }
    devices = {
        "gas_analyzer_01": _FakeGasAnalyzer({"co2_ppm": 501.0, "h2o_mmol": 2.1}),
        "gas_analyzer_02": _FakeRecoveringGasAnalyzer(
            [
                {"co2_ratio_f": 1.234, "co2_ppm": 400.0, "h2o_mmol": 1.5},
                {"co2_ratio_f": 1.234, "co2_ppm": 400.0, "h2o_mmol": 1.5},
                {"co2_ratio_f": 1.234, "co2_ppm": 400.0, "h2o_mmol": 1.5},
            ]
        ),
    }
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(cfg, devices, logger, lambda *_: None, lambda *_: None)
    runner._disabled_analyzers.add("ga06")
    runner._disabled_analyzer_reasons["ga06"] = "co2_ratio_f_timeout"

    runner._attempt_reenable_disabled_analyzers()

    assert "ga06" not in runner._disabled_analyzers
    assert "ga06" not in runner._disabled_analyzer_reasons
    assert ("mode", 2, False) in devices["gas_analyzer_02"].calls
    assert ("active", False, False) in devices["gas_analyzer_02"].calls
    assert ("active", True, False) in devices["gas_analyzer_02"].calls
    logger.close()


def test_attempt_reenable_disabled_analyzers_keeps_analyzer_disabled_when_probe_invalid(tmp_path: Path) -> None:
    cfg = {
        "devices": {
            "gas_analyzer": {"active_send": False, "ftd_hz": 1, "average_co2": 1, "average_h2o": 1},
            "gas_analyzers": [
                {"name": "ga01"},
                {"name": "ga06"},
            ],
        }
    }
    devices = {
        "gas_analyzer_01": _FakeGasAnalyzer({"co2_ppm": 501.0, "h2o_mmol": 2.1}),
        "gas_analyzer_02": _FakeRecoveringGasAnalyzer([None, {"co2_ratio_f": None, "co2_ppm": None}]),
    }
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(cfg, devices, logger, lambda *_: None, lambda *_: None)
    runner._disabled_analyzers.add("ga06")
    runner._disabled_analyzer_reasons["ga06"] = "co2_ratio_f_timeout"

    runner._attempt_reenable_disabled_analyzers()

    assert "ga06" in runner._disabled_analyzers
    assert runner._disabled_analyzer_reasons["ga06"] == "co2_ratio_f_timeout"
    assert ("mode", 2, False) in devices["gas_analyzer_02"].calls
    logger.close()


def test_attempt_reenable_disabled_analyzers_skips_retry_during_cooldown(tmp_path: Path) -> None:
    cfg = {
        "workflow": {"analyzer_reprobe": {"cooldown_s": 300}},
        "devices": {
            "gas_analyzer": {"active_send": False, "ftd_hz": 1, "average_co2": 1, "average_h2o": 1},
            "gas_analyzers": [
                {"name": "ga01"},
                {"name": "ga06"},
            ],
        },
    }
    devices = {
        "gas_analyzer_01": _FakeGasAnalyzer({"co2_ppm": 501.0, "h2o_mmol": 2.1}),
        "gas_analyzer_02": _FakeRecoveringGasAnalyzer(
            [
                {"co2_ratio_f": 1.234, "co2_ppm": 400.0, "h2o_mmol": 1.5},
                {"co2_ratio_f": 1.234, "co2_ppm": 400.0, "h2o_mmol": 1.5},
                {"co2_ratio_f": 1.234, "co2_ppm": 400.0, "h2o_mmol": 1.5},
            ]
        ),
    }
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(cfg, devices, logger, lambda *_: None, lambda *_: None)
    runner._disabled_analyzers.add("ga06")
    runner._disabled_analyzer_reasons["ga06"] = "co2_ratio_f_timeout"
    runner._disabled_analyzer_last_reprobe_ts["ga06"] = time.time()

    runner._attempt_reenable_disabled_analyzers()

    assert "ga06" in runner._disabled_analyzers
    assert devices["gas_analyzer_02"].calls == []
    logger.close()


def test_attempt_reenable_disabled_analyzers_retries_after_cooldown_expires(tmp_path: Path) -> None:
    cfg = {
        "workflow": {"analyzer_reprobe": {"cooldown_s": 10}},
        "devices": {
            "gas_analyzer": {"active_send": False, "ftd_hz": 1, "average_co2": 1, "average_h2o": 1},
            "gas_analyzers": [
                {"name": "ga01"},
                {"name": "ga06"},
            ],
        },
    }
    devices = {
        "gas_analyzer_01": _FakeGasAnalyzer({"co2_ppm": 501.0, "h2o_mmol": 2.1}),
        "gas_analyzer_02": _FakeRecoveringGasAnalyzer(
            [
                {"co2_ratio_f": 1.234, "co2_ppm": 400.0, "h2o_mmol": 1.5},
                {"co2_ratio_f": 1.234, "co2_ppm": 400.0, "h2o_mmol": 1.5},
                {"co2_ratio_f": 1.234, "co2_ppm": 400.0, "h2o_mmol": 1.5},
            ]
        ),
    }
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(cfg, devices, logger, lambda *_: None, lambda *_: None)
    runner._disabled_analyzers.add("ga06")
    runner._disabled_analyzer_reasons["ga06"] = "co2_ratio_f_timeout"
    runner._disabled_analyzer_last_reprobe_ts["ga06"] = time.time() - 11

    runner._attempt_reenable_disabled_analyzers()

    assert "ga06" not in runner._disabled_analyzers
    assert ("mode", 2, False) in devices["gas_analyzer_02"].calls
    logger.close()
