from pathlib import Path
import time
import json
import threading

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


class _FakeCyclingRatioGasAnalyzer(_FakeGasAnalyzer):
    def __init__(self, ratio_values, *, ratio_key: str = "co2_ratio_f"):
        super().__init__({})
        self._ratio_values = list(ratio_values)
        self._ratio_key = ratio_key
        self._read_count = 0

    def read_latest_data(self, *args, **kwargs):
        line = f"YGAS,001,FRAME,{self._read_count}"
        self._read_count += 1
        return line

    def parse_line_mode2(self, line):
        try:
            idx = int(str(line).rsplit(",", 1)[-1])
        except ValueError:
            idx = max(0, self._read_count - 1)
        if not self._ratio_values:
            return None
        return self._normalized_parsed({self._ratio_key: self._ratio_values[idx % len(self._ratio_values)]})


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


class _FakeFlushSensitiveGasAnalyzer(_FakeGasAnalyzer):
    def __init__(self):
        super().__init__({"co2_ratio_f": 1.0})
        self.flush_calls = 0
        self.flushed = False

    def flush_input(self):
        self.flush_calls += 1
        self.flushed = True

    def read_latest_data(self, *args, **kwargs):
        if self.flushed:
            return ""
        return "STALE-STABLE-FRAME"

    def parse_line_mode2(self, line):
        if line == "STALE-STABLE-FRAME":
            return self._normalized_parsed({"co2_ratio_f": 1.0})
        return None


class _FakeStreamingBatchGasAnalyzer(_FakeGasAnalyzer):
    def __init__(self, batches):
        super().__init__({})
        self._batches = list(batches)
        self.drain_calls = 0
        self.passive_calls = 0
        self.active_send = False

    def _drain_stream_lines(self, drain_s=0.35, read_timeout_s=0.05):
        self.drain_calls += 1
        if self._batches:
            return list(self._batches.pop(0))
        return []

    def read_data_passive(self):
        self.passive_calls += 1
        return super().read_data_passive()

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


def _preseal_gate_cfg(
    *,
    min_valid: int = 2,
    required_labels: list[str] | None = None,
    optional_labels: list[str] | None = None,
    max_wait_s: float = 1.0,
    invalid_frame_min_count: int = 2,
) -> dict:
    return {
        "devices": {
            "gas_analyzers": [
                {"name": "ga01"},
                {"name": "ga02"},
                {"name": "ga03"},
                {"name": "ga04"},
            ]
        },
        "workflow": {
            "stability": {
                "sensor": {
                    "enabled": True,
                    "co2_ratio_f_preseal_tol": 0.01,
                    "co2_ratio_f_preseal_window_s": 0.25,
                    "co2_ratio_f_preseal_timeout_s": max_wait_s,
                    "co2_ratio_f_preseal_min_samples": 2,
                    "co2_ratio_f_preseal_read_interval_s": 0.0,
                    "poll_s": 0.0,
                },
                "analyzer_gate_min_valid_analyzers": min_valid,
                "analyzer_gate_required_labels": required_labels or [],
                "analyzer_gate_optional_labels": optional_labels or ["ga01", "ga02", "ga03", "ga04"],
                "analyzer_gate_allow_pass_with_dropped_optional": True,
                "analyzer_gate_zero_value_policy": "drop_optional_not_block",
                "analyzer_gate_invalid_frame_drop_s": 30.0,
                "analyzer_gate_invalid_frame_min_count": invalid_frame_min_count,
                "analyzer_gate_silent_timeout_s": 0.2,
                "analyzer_gate_max_wait_s": max_wait_s,
                "analyzer_gate_stable_window_s": 0.25,
                "analyzer_gate_stable_min_samples": 2,
            }
        },
    }


def _runner_for_preseal_gate(tmp_path: Path, cfg: dict, analyzers: dict) -> tuple[CalibrationRunner, RunLogger]:
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(cfg, analyzers, logger, lambda *_: None, lambda *_: None)
    return runner, logger


def _runner_with_sampling_pressure_context(tmp_path: Path, cfg: dict) -> tuple[CalibrationRunner, RunLogger, dict]:
    runner, logger = _runner_for_preseal_gate(tmp_path, cfg, {})
    runner.devices["pace"] = object()
    runner.devices["pressure_gauge"] = object()
    runner.devices["dewpoint"] = object()
    context = runner._new_sampling_window_context(point=_point(), phase="co2", point_tag="open_flow_400ppm")
    runner._append_fast_signal_frame(
        context,
        "pressure_gauge",
        values={"pressure_gauge_hpa": 1010.2},
        source="test_com22_reference",
    )
    runner._append_fast_signal_frame(
        context,
        "dewpoint",
        values={"dewpoint_live_c": -45.0, "dew_temp_live_c": 20.0, "dew_rh_live_pct": 0.2},
        source="test_dewpoint_reference",
    )
    return runner, logger, context


def test_sampling_fast_signal_gate_does_not_require_pace_pressure_by_default(tmp_path: Path) -> None:
    cfg = {"workflow": {"sampling": {"pre_sample_signal_max_age_s": 5.0}}}
    runner, logger, context = _runner_with_sampling_pressure_context(tmp_path, cfg)
    try:
        assert runner._sampling_context_missing_fresh_fast_signals(context) == []
    finally:
        logger.close()


def test_sampling_fast_signal_gate_can_explicitly_require_pace_pressure(tmp_path: Path) -> None:
    cfg = {
        "workflow": {
            "sampling": {
                "pre_sample_signal_max_age_s": 5.0,
                "require_pace_pressure_signal": True,
            }
        }
    }
    runner, logger, context = _runner_with_sampling_pressure_context(tmp_path, cfg)
    try:
        assert runner._sampling_context_missing_fresh_fast_signals(context) == ["pace"]
        runner._append_fast_signal_frame(
            context,
            "pace",
            values={"pressure_hpa": 1010.1},
            source="test_explicit_pace_pressure",
        )
        assert runner._sampling_context_missing_fresh_fast_signals(context) == []
    finally:
        logger.close()


def test_analyzer_gate_passes_with_two_valid_stable_and_two_optional_none(tmp_path: Path) -> None:
    cfg = _preseal_gate_cfg()
    runner, logger = _runner_for_preseal_gate(
        tmp_path,
        cfg,
        {
            "gas_analyzer_01": _FakeGasAnalyzer({"co2_ratio_f": 1.0}),
            "gas_analyzer_02": _FakeRecoveringGasAnalyzer([None, None, None, None]),
            "gas_analyzer_03": _FakeRecoveringGasAnalyzer([None, None, None, None]),
            "gas_analyzer_04": _FakeGasAnalyzer({"co2_ratio_f": 1.1}),
        },
    )

    assert runner._wait_co2_preseal_primary_sensor_gate(_point()) is True
    logger.close()

    state = runner._point_runtime_state(_point(), phase="co2") or {}
    assert state["analyzer_gate_valid_stable_count"] == 2
    assert set(state["analyzer_gate_dropped_labels"].split(",")) == {"ga02", "ga03"}
    assert state["analyzer_gate_pass_with_dropped_optional"] is True


def test_co2_preseal_gate_flushes_active_stream_before_stability_window(tmp_path: Path) -> None:
    cfg = _preseal_gate_cfg(min_valid=1, max_wait_s=0.15)
    cfg["devices"]["gas_analyzers"] = [{"name": "ga01", "active_send": True}]
    stability = cfg["workflow"]["stability"]
    stability["analyzer_gate_optional_labels"] = ["ga01"]
    stability["analyzer_gate_silent_timeout_s"] = 0.03
    sensor_cfg = stability["sensor"]
    sensor_cfg["co2_ratio_f_preseal_window_s"] = 0.05
    sensor_cfg["co2_ratio_f_preseal_timeout_s"] = 0.15
    sensor_cfg["co2_ratio_f_preseal_min_samples"] = 2
    sensor_cfg["co2_ratio_f_preseal_read_interval_s"] = 0.0
    ga = _FakeFlushSensitiveGasAnalyzer()
    runner, logger = _runner_for_preseal_gate(
        tmp_path,
        cfg,
        {"gas_analyzer_01": ga},
    )
    runner._cache_live_analyzer_frame(
        ga,
        "cached-old-frame",
        ga._normalized_parsed({"co2_ratio_f": 1.0}),
        category="parsed",
        label="ga01",
        source="active_live_cache",
        is_live=True,
    )

    assert runner._get_live_analyzer_frame_cache(ga, label="ga01") is not None
    assert runner._wait_co2_preseal_primary_sensor_gate(_point()) is False
    logger.close()

    assert ga.flush_calls == 1
    assert runner._get_live_analyzer_frame_cache(ga, label="ga01") is None
    state = runner._point_runtime_state(_point(), phase="co2") or {}
    assert state["analyzer_gate_fresh_frame_reset"] is True
    assert state["analyzer_gate_fresh_frame_reset_flushed_labels"] == "ga01"
    assert state["analyzer_gate_live_cache_entries_cleared"] >= 1
    assert state["analyzer_gate_final_decision_reason"] == "min_valid_not_met"


def test_read_first_stream_marks_runtime_active_send_without_config_writes(tmp_path: Path) -> None:
    cfg = {
        "workflow": {
            "analyzer_mode2_init": {
                "read_first_before_config": True,
                "sniff_stream_before_config": True,
                "write_config_on_read_first_fail": False,
                "read_first_attempts": 1,
                "ready_consecutive_frames": 1,
            }
        }
    }
    runner, logger = _runner_for_preseal_gate(
        tmp_path,
        cfg,
        {"gas_analyzer_01": _FakeStreamingBatchGasAnalyzer([
            ["YGAS,001,500.0,2.0,101.0,1.1,1.0,1.0,1.0,1.0,1000,1001,1002,25.0,25.1,101.3"]
        ])},
    )
    ga = runner.devices["gas_analyzer_01"]

    runner._configure_gas_analyzer(
        ga,
        label="ga01",
        mode=2,
        active_send=True,
        ftd_hz=10,
        avg_co2=1,
        avg_h2o=1,
        avg_filter=49,
    )

    assert ga.active_send is True
    assert ga.calls == []
    assert ga.passive_calls == 0
    logger.close()


def test_analyzer_gate_passes_with_two_valid_stable_and_two_optional_zero(tmp_path: Path) -> None:
    cfg = _preseal_gate_cfg()
    runner, logger = _runner_for_preseal_gate(
        tmp_path,
        cfg,
        {
            "gas_analyzer_01": _FakeGasAnalyzer({"co2_ratio_f": 1.0}),
            "gas_analyzer_02": _FakeGasAnalyzer({"co2_ratio_f": 0.0}),
            "gas_analyzer_03": _FakeGasAnalyzer({"co2_ratio_f": 0.0}),
            "gas_analyzer_04": _FakeGasAnalyzer({"co2_ratio_f": 1.1}),
        },
    )

    assert runner._wait_co2_preseal_primary_sensor_gate(_point()) is True
    logger.close()

    state = runner._point_runtime_state(_point(), phase="co2") or {}
    assert state["analyzer_gate_valid_stable_count"] == 2
    assert "zero_or_suspect_optional" in state["analyzer_gate_dropped_reasons"]


def test_analyzer_gate_optional_invalid_dropped_after_threshold(tmp_path: Path) -> None:
    cfg = _preseal_gate_cfg(invalid_frame_min_count=2)
    runner, logger = _runner_for_preseal_gate(
        tmp_path,
        cfg,
        {
            "gas_analyzer_01": _FakeGasAnalyzer({"co2_ratio_f": 1.0}),
            "gas_analyzer_02": _FakeRecoveringGasAnalyzer([{"co2_ratio_f": None}, {"co2_ratio_f": None}]),
            "gas_analyzer_03": _FakeRecoveringGasAnalyzer([{"co2_ratio_f": None}, {"co2_ratio_f": None}]),
            "gas_analyzer_04": _FakeGasAnalyzer({"co2_ratio_f": 1.1}),
        },
    )

    assert runner._wait_co2_preseal_primary_sensor_gate(_point()) is True
    logger.close()

    state = runner._point_runtime_state(_point(), phase="co2") or {}
    assert set(state["analyzer_gate_dropped_labels"].split(",")) == {"ga02", "ga03"}


def test_analyzer_gate_can_drop_optional_without_disabling_for_sidecar_sampling(tmp_path: Path) -> None:
    cfg = _preseal_gate_cfg(invalid_frame_min_count=2)
    cfg["workflow"]["stability"]["analyzer_gate_disable_dropped_optional"] = False
    runner, logger = _runner_for_preseal_gate(
        tmp_path,
        cfg,
        {
            "gas_analyzer_01": _FakeGasAnalyzer({"co2_ratio_f": 1.0}),
            "gas_analyzer_02": _FakeRecoveringGasAnalyzer([None, None, None, None]),
            "gas_analyzer_03": _FakeRecoveringGasAnalyzer([None, None, None, None]),
            "gas_analyzer_04": _FakeGasAnalyzer({"co2_ratio_f": 1.1}),
        },
    )

    assert runner._wait_co2_preseal_primary_sensor_gate(_point()) is True
    logger.close()

    state = runner._point_runtime_state(_point(), phase="co2") or {}
    assert set(state["analyzer_gate_dropped_labels"].split(",")) == {"ga02", "ga03"}
    assert getattr(runner, "_disabled_analyzers", set()) == set()


def test_analyzer_gate_filters_single_ratio_spike_but_records_evidence(tmp_path: Path) -> None:
    cfg = _preseal_gate_cfg(min_valid=1, max_wait_s=2.0)
    stability = cfg["workflow"]["stability"]
    stability["analyzer_gate_spike_filter_enabled"] = True
    stability["analyzer_gate_spike_filter_max_count"] = 1
    sensor_cfg = stability["sensor"]
    sensor_cfg["co2_ratio_f_preseal_tol"] = 0.001
    sensor_cfg["co2_ratio_f_preseal_min_samples"] = 3
    runner, logger = _runner_for_preseal_gate(
        tmp_path,
        cfg,
        {
            "gas_analyzer_01": _FakeRecoveringGasAnalyzer(
                [
                    {"co2_ratio_f": 1.0000},
                    {"co2_ratio_f": 1.0100},
                    {"co2_ratio_f": 1.0004},
                ]
            ),
        },
    )

    assert runner._wait_co2_preseal_primary_sensor_gate(_point()) is True
    logger.close()

    state = runner._point_runtime_state(_point(), phase="co2") or {}
    statuses = json.loads(state["analyzer_gate_per_analyzer_status"])
    ga01 = statuses[0]
    assert ga01["stable"] is True
    assert ga01["stable_window_span"] > 0.001
    assert ga01["stable_window_span_after_spike_filter"] <= 0.001
    assert ga01["stability_spike_filtered_count"] == 1


def test_co2_preseal_gate_records_a_grade_ratio_target_when_passed(tmp_path: Path) -> None:
    cfg = _preseal_gate_cfg(min_valid=1, max_wait_s=0.5)
    cfg["devices"]["gas_analyzers"] = [{"name": "ga01"}]
    stability = cfg["workflow"]["stability"]
    stability["analyzer_gate_optional_labels"] = ["ga01"]
    stability["analyzer_gate_stable_window_s"] = 0.2
    stability["analyzer_gate_spike_filter_enabled"] = False
    sensor_cfg = stability["sensor"]
    sensor_cfg["co2_ratio_f_preseal_tol"] = 0.001
    sensor_cfg["co2_ratio_f_preseal_a_grade_tol"] = 0.0005
    sensor_cfg["co2_ratio_f_preseal_window_s"] = 0.2
    sensor_cfg["co2_ratio_f_preseal_timeout_s"] = 0.5
    sensor_cfg["co2_ratio_f_preseal_min_samples"] = 2
    sensor_cfg["co2_ratio_f_preseal_read_interval_s"] = 0.0
    runner, logger = _runner_for_preseal_gate(
        tmp_path,
        cfg,
        {
            "gas_analyzer_01": _FakeCyclingRatioGasAnalyzer([1.0000, 1.0004]),
        },
    )

    assert runner._wait_co2_preseal_primary_sensor_gate(_point()) is True
    logger.close()

    state = runner._point_runtime_state(_point(), phase="co2") or {}
    assert state["analyzer_gate_ratio_fit_quality_target"] == "A"
    assert state["analyzer_gate_ratio_a_grade_tol"] == 0.0005
    assert state["analyzer_gate_ratio_hard_tol"] == 0.001
    assert state["analyzer_gate_ratio_effective_tol"] == 0.0005
    assert state["analyzer_gate_fit_quality_grade"] == "A"
    assert state["analyzer_gate_a_grade_target_passed"] is True


def test_co2_preseal_gate_does_not_treat_hard_limit_as_a_grade(tmp_path: Path) -> None:
    cfg = _preseal_gate_cfg(min_valid=1, max_wait_s=0.12)
    cfg["devices"]["gas_analyzers"] = [{"name": "ga01"}]
    stability = cfg["workflow"]["stability"]
    stability["analyzer_gate_optional_labels"] = ["ga01"]
    stability["analyzer_gate_stable_window_s"] = 0.2
    stability["analyzer_gate_spike_filter_enabled"] = False
    sensor_cfg = stability["sensor"]
    sensor_cfg["co2_ratio_f_preseal_tol"] = 0.001
    sensor_cfg["co2_ratio_f_preseal_a_grade_tol"] = 0.0005
    sensor_cfg["co2_ratio_f_preseal_window_s"] = 0.2
    sensor_cfg["co2_ratio_f_preseal_timeout_s"] = 0.12
    sensor_cfg["co2_ratio_f_preseal_min_samples"] = 2
    sensor_cfg["co2_ratio_f_preseal_read_interval_s"] = 0.0
    runner, logger = _runner_for_preseal_gate(
        tmp_path,
        cfg,
        {
            "gas_analyzer_01": _FakeCyclingRatioGasAnalyzer([1.0000, 1.0008]),
        },
    )

    assert runner._wait_co2_preseal_primary_sensor_gate(_point()) is False
    logger.close()

    state = runner._point_runtime_state(_point(), phase="co2") or {}
    assert state["analyzer_gate_ratio_fit_quality_target"] == "A"
    assert state["analyzer_gate_ratio_a_grade_tol"] == 0.0005
    assert state["analyzer_gate_ratio_hard_tol"] == 0.001
    assert state["analyzer_gate_ratio_effective_tol"] == 0.0005
    assert state["analyzer_gate_fit_quality_grade"] == "not_a"
    assert state["analyzer_gate_a_grade_target_passed"] is False
    statuses = json.loads(state["analyzer_gate_per_analyzer_status"])
    assert statuses[0]["stable_window_span"] > 0.0005
    assert statuses[0]["stable_window_span"] <= 0.001


def test_analyzer_gate_can_wait_full_timeout_before_accepting_min_valid(tmp_path: Path) -> None:
    cfg = _preseal_gate_cfg(min_valid=1, max_wait_s=0.2)
    cfg["workflow"]["stability"]["analyzer_gate_wait_full_timeout_for_optional"] = True
    cfg["workflow"]["stability"]["analyzer_gate_spike_filter_enabled"] = False
    runner, logger = _runner_for_preseal_gate(
        tmp_path,
        cfg,
        {
            "gas_analyzer_01": _FakeGasAnalyzer({"co2_ratio_f": 1.0000}),
            "gas_analyzer_02": _FakeRecoveringGasAnalyzer(
                [{"co2_ratio_f": 1.0000}, {"co2_ratio_f": 1.1000}] * 100
            ),
            "gas_analyzer_03": _FakeRecoveringGasAnalyzer(
                [{"co2_ratio_f": 1.2000}, {"co2_ratio_f": 1.3000}] * 100
            ),
            "gas_analyzer_04": _FakeRecoveringGasAnalyzer(
                [{"co2_ratio_f": 1.4000}, {"co2_ratio_f": 1.5000}] * 100
            ),
        },
    )

    assert runner._wait_co2_preseal_primary_sensor_gate(_point()) is True
    logger.close()

    state = runner._point_runtime_state(_point(), phase="co2") or {}
    assert state["analyzer_gate_valid_stable_count"] == 1
    assert state["analyzer_gate_final_decision_reason"] == "max_wait_min_valid_met"
    statuses = json.loads(state["analyzer_gate_per_analyzer_status"])
    assert [item["stable"] for item in statuses] == [True, False, False, False]


def test_analyzer_gate_prefers_all_stable_with_bounded_grace_before_min_valid(tmp_path: Path) -> None:
    cfg = _preseal_gate_cfg(min_valid=1, max_wait_s=0.8)
    cfg["devices"]["gas_analyzers"] = [{"name": "ga01"}, {"name": "ga02"}]
    stability = cfg["workflow"]["stability"]
    stability["analyzer_gate_optional_labels"] = ["ga01", "ga02"]
    stability["analyzer_gate_prefer_all_stable_grace_s"] = 0.4
    stability["analyzer_gate_stable_window_s"] = 0.1
    stability["analyzer_gate_spike_filter_enabled"] = False
    sensor_cfg = stability["sensor"]
    sensor_cfg["co2_ratio_f_preseal_window_s"] = 0.1
    sensor_cfg["co2_ratio_f_preseal_timeout_s"] = 0.8
    runner, logger = _runner_for_preseal_gate(
        tmp_path,
        cfg,
        {
            "gas_analyzer_01": _FakeGasAnalyzer({"co2_ratio_f": 1.0000}),
            "gas_analyzer_02": _FakeRecoveringGasAnalyzer(
                [
                    {"co2_ratio_f": 1.0000},
                    {"co2_ratio_f": 1.1000},
                    {"co2_ratio_f": 1.0500},
                    {"co2_ratio_f": 1.0500},
                    {"co2_ratio_f": 1.0500},
                    {"co2_ratio_f": 1.0500},
                ]
                * 10
            ),
        },
    )

    assert runner._wait_co2_preseal_primary_sensor_gate(_point()) is True
    logger.close()

    state = runner._point_runtime_state(_point(), phase="co2") or {}
    assert state["analyzer_gate_valid_stable_count"] == 2
    assert state["analyzer_gate_final_decision_reason"] == "all_active_stable"
    assert state["analyzer_gate_prefer_all_stable_grace_s"] == 0.4
    statuses = json.loads(state["analyzer_gate_per_analyzer_status"])
    assert [item["stable"] for item in statuses] == [True, True]


def test_analyzer_gate_accepts_min_valid_after_bounded_all_stable_grace(tmp_path: Path) -> None:
    cfg = _preseal_gate_cfg(min_valid=1, max_wait_s=0.5)
    cfg["devices"]["gas_analyzers"] = [{"name": "ga01"}, {"name": "ga02"}]
    stability = cfg["workflow"]["stability"]
    stability["analyzer_gate_optional_labels"] = ["ga01", "ga02"]
    stability["analyzer_gate_prefer_all_stable_grace_s"] = 0.12
    stability["analyzer_gate_stable_window_s"] = 0.1
    stability["analyzer_gate_spike_filter_enabled"] = False
    sensor_cfg = stability["sensor"]
    sensor_cfg["co2_ratio_f_preseal_window_s"] = 0.1
    sensor_cfg["co2_ratio_f_preseal_timeout_s"] = 0.5
    runner, logger = _runner_for_preseal_gate(
        tmp_path,
        cfg,
        {
            "gas_analyzer_01": _FakeGasAnalyzer({"co2_ratio_f": 1.0000}),
            "gas_analyzer_02": _FakeRecoveringGasAnalyzer(
                [{"co2_ratio_f": 1.0000}, {"co2_ratio_f": 1.1000}] * 100
            ),
        },
    )

    assert runner._wait_co2_preseal_primary_sensor_gate(_point()) is True
    logger.close()

    state = runner._point_runtime_state(_point(), phase="co2") or {}
    assert state["analyzer_gate_valid_stable_count"] == 1
    assert state["analyzer_gate_final_decision_reason"] == "min_valid_met_after_prefer_all_grace"
    assert state["analyzer_gate_prefer_all_stable_grace_s"] == 0.12
    assert state["analyzer_gate_prefer_all_stable_grace_elapsed_s"] >= 0.12
    statuses = json.loads(state["analyzer_gate_per_analyzer_status"])
    assert [item["stable"] for item in statuses] == [True, False]


def test_h2o_analyzer_gate_can_drop_optional_without_disabling_for_sidecar_sampling(tmp_path: Path) -> None:
    cfg = _preseal_gate_cfg(invalid_frame_min_count=2)
    stability = cfg["workflow"]["stability"]
    stability["analyzer_gate_disable_dropped_optional"] = False
    sensor_cfg = stability["sensor"]
    sensor_cfg["h2o_ratio_f_preseal_tol"] = 0.01
    sensor_cfg["h2o_ratio_f_preseal_window_s"] = 0.25
    sensor_cfg["h2o_ratio_f_preseal_timeout_s"] = 1.0
    sensor_cfg["h2o_ratio_f_preseal_min_samples"] = 2
    sensor_cfg["h2o_ratio_f_preseal_read_interval_s"] = 0.0
    runner, logger = _runner_for_preseal_gate(
        tmp_path,
        cfg,
        {
            "gas_analyzer_01": _FakeGasAnalyzer({"h2o_ratio_f": 1.0}),
            "gas_analyzer_02": _FakeRecoveringGasAnalyzer([{"h2o_ratio_f": 0.0}, {"h2o_ratio_f": 0.0}]),
            "gas_analyzer_03": _FakeRecoveringGasAnalyzer([None, None, None, None]),
            "gas_analyzer_04": _FakeGasAnalyzer({"h2o_ratio_f": 1.1}),
        },
    )

    assert runner._wait_h2o_precondition_primary_sensor_gate(_point()) is True
    logger.close()

    state = runner._point_runtime_state(_point(), phase="h2o") or {}
    assert set(state["analyzer_gate_dropped_labels"].split(",")) == {"ga02", "ga03"}
    assert getattr(runner, "_disabled_analyzers", set()) == set()


def test_h2o_analyzer_gate_can_wait_full_timeout_before_accepting_min_valid(tmp_path: Path) -> None:
    cfg = _preseal_gate_cfg(min_valid=1, max_wait_s=0.2)
    stability = cfg["workflow"]["stability"]
    stability["analyzer_gate_wait_full_timeout_for_optional"] = True
    stability["analyzer_gate_spike_filter_enabled"] = False
    sensor_cfg = stability["sensor"]
    sensor_cfg["h2o_ratio_f_preseal_tol"] = 0.001
    sensor_cfg["h2o_ratio_f_preseal_window_s"] = 0.25
    sensor_cfg["h2o_ratio_f_preseal_timeout_s"] = 0.2
    sensor_cfg["h2o_ratio_f_preseal_min_samples"] = 2
    sensor_cfg["h2o_ratio_f_preseal_read_interval_s"] = 0.0
    runner, logger = _runner_for_preseal_gate(
        tmp_path,
        cfg,
        {
            "gas_analyzer_01": _FakeGasAnalyzer({"h2o_ratio_f": 0.7000}),
            "gas_analyzer_02": _FakeRecoveringGasAnalyzer(
                [{"h2o_ratio_f": 0.7000}, {"h2o_ratio_f": 0.7200}] * 100
            ),
            "gas_analyzer_03": _FakeRecoveringGasAnalyzer(
                [{"h2o_ratio_f": 0.7300}, {"h2o_ratio_f": 0.7600}] * 100
            ),
            "gas_analyzer_04": _FakeRecoveringGasAnalyzer(
                [{"h2o_ratio_f": 0.7800}, {"h2o_ratio_f": 0.8200}] * 100
            ),
        },
    )

    assert runner._wait_h2o_precondition_primary_sensor_gate(_point()) is True
    logger.close()

    state = runner._point_runtime_state(_point(), phase="h2o") or {}
    assert state["analyzer_gate_valid_stable_count"] == 1
    assert state["analyzer_gate_final_decision_reason"] == "max_wait_min_valid_met"
    statuses = json.loads(state["analyzer_gate_per_analyzer_status"])
    assert [item["stable"] for item in statuses] == [True, False, False, False]


def test_h2o_analyzer_gate_accepts_min_valid_after_bounded_all_stable_grace(tmp_path: Path) -> None:
    cfg = _preseal_gate_cfg(min_valid=1, max_wait_s=0.5)
    cfg["devices"]["gas_analyzers"] = [{"name": "ga01"}, {"name": "ga02"}]
    stability = cfg["workflow"]["stability"]
    stability["analyzer_gate_optional_labels"] = ["ga01", "ga02"]
    stability["analyzer_gate_prefer_all_stable_grace_s"] = 0.12
    stability["analyzer_gate_stable_window_s"] = 0.1
    stability["analyzer_gate_spike_filter_enabled"] = False
    sensor_cfg = stability["sensor"]
    sensor_cfg["h2o_ratio_f_preseal_tol"] = 0.001
    sensor_cfg["h2o_ratio_f_preseal_window_s"] = 0.1
    sensor_cfg["h2o_ratio_f_preseal_timeout_s"] = 0.5
    sensor_cfg["h2o_ratio_f_preseal_min_samples"] = 2
    sensor_cfg["h2o_ratio_f_preseal_read_interval_s"] = 0.0
    runner, logger = _runner_for_preseal_gate(
        tmp_path,
        cfg,
        {
            "gas_analyzer_01": _FakeGasAnalyzer({"h2o_ratio_f": 0.7000}),
            "gas_analyzer_02": _FakeRecoveringGasAnalyzer(
                [{"h2o_ratio_f": 0.7000}, {"h2o_ratio_f": 0.7200}] * 100
            ),
        },
    )

    assert runner._wait_h2o_precondition_primary_sensor_gate(_point()) is True
    logger.close()

    state = runner._point_runtime_state(_point(), phase="h2o") or {}
    assert state["analyzer_gate_valid_stable_count"] == 1
    assert state["analyzer_gate_final_decision_reason"] == "min_valid_met_after_prefer_all_grace"
    assert state["analyzer_gate_prefer_all_stable_grace_s"] == 0.12
    assert state["analyzer_gate_prefer_all_stable_grace_elapsed_s"] >= 0.12
    statuses = json.loads(state["analyzer_gate_per_analyzer_status"])
    assert [item["stable"] for item in statuses] == [True, False]


def test_h2o_analyzer_gate_filters_single_ratio_spike_but_records_evidence(tmp_path: Path) -> None:
    cfg = _preseal_gate_cfg(min_valid=1, max_wait_s=1.0)
    stability = cfg["workflow"]["stability"]
    stability["analyzer_gate_max_wait_s"] = 2.0
    stability["analyzer_gate_stable_window_s"] = 5.0
    stability["analyzer_gate_spike_filter_enabled"] = True
    stability["analyzer_gate_spike_filter_max_count"] = 1
    sensor_cfg = stability["sensor"]
    sensor_cfg["h2o_ratio_f_preseal_tol"] = 0.001
    sensor_cfg["h2o_ratio_f_preseal_window_s"] = 5.0
    sensor_cfg["h2o_ratio_f_preseal_timeout_s"] = 2.0
    sensor_cfg["h2o_ratio_f_preseal_min_samples"] = 3
    sensor_cfg["h2o_ratio_f_preseal_read_interval_s"] = 0.0
    runner, logger = _runner_for_preseal_gate(
        tmp_path,
        cfg,
        {
            "gas_analyzer_01": _FakeRecoveringGasAnalyzer(
                [
                    {"h2o_ratio_f": 0.7000},
                    {"h2o_ratio_f": 0.7110},
                    {"h2o_ratio_f": 0.7004},
                    {"h2o_ratio_f": 0.7005},
                    {"h2o_ratio_f": 0.7005},
                    {"h2o_ratio_f": 0.7005},
                ]
            ),
        },
    )

    assert runner._wait_h2o_precondition_primary_sensor_gate(_point()) is True
    logger.close()

    state = runner._point_runtime_state(_point(), phase="h2o") or {}
    statuses = json.loads(state["analyzer_gate_per_analyzer_status"])
    ga01 = statuses[0]
    assert ga01["stable"] is True
    assert ga01["stable_window_span"] > 0.001
    assert ga01["stable_window_span_after_spike_filter"] <= 0.001
    assert ga01["stability_spike_filtered_count"] == 1


def test_h2o_preseal_gate_records_a_grade_ratio_target_when_passed(tmp_path: Path) -> None:
    cfg = _preseal_gate_cfg(min_valid=1, max_wait_s=0.5)
    cfg["devices"]["gas_analyzers"] = [{"name": "ga01"}]
    stability = cfg["workflow"]["stability"]
    stability["analyzer_gate_optional_labels"] = ["ga01"]
    stability["analyzer_gate_stable_window_s"] = 0.2
    stability["analyzer_gate_spike_filter_enabled"] = False
    sensor_cfg = stability["sensor"]
    sensor_cfg["h2o_ratio_f_preseal_tol"] = 0.001
    sensor_cfg["h2o_ratio_f_preseal_a_grade_tol"] = 0.0005
    sensor_cfg["h2o_ratio_f_preseal_window_s"] = 0.2
    sensor_cfg["h2o_ratio_f_preseal_timeout_s"] = 0.5
    sensor_cfg["h2o_ratio_f_preseal_min_samples"] = 2
    sensor_cfg["h2o_ratio_f_preseal_read_interval_s"] = 0.0
    runner, logger = _runner_for_preseal_gate(
        tmp_path,
        cfg,
        {
            "gas_analyzer_01": _FakeCyclingRatioGasAnalyzer([0.7000, 0.7004], ratio_key="h2o_ratio_f"),
        },
    )

    assert runner._wait_h2o_precondition_primary_sensor_gate(_point()) is True
    logger.close()

    state = runner._point_runtime_state(_point(), phase="h2o") or {}
    assert state["analyzer_gate_ratio_fit_quality_target"] == "A"
    assert state["analyzer_gate_ratio_a_grade_tol"] == 0.0005
    assert state["analyzer_gate_ratio_hard_tol"] == 0.001
    assert state["analyzer_gate_ratio_effective_tol"] == 0.0005
    assert state["analyzer_gate_fit_quality_grade"] == "A"
    assert state["analyzer_gate_a_grade_target_passed"] is True


def test_sampling_freshness_ignores_analyzer_dropped_by_ratio_gate(tmp_path: Path) -> None:
    cfg = _preseal_gate_cfg()
    runner, logger = _runner_for_preseal_gate(
        tmp_path,
        cfg,
        {
            "gas_analyzer_01": _FakeGasAnalyzer({"co2_ratio_f": 1.0}),
            "gas_analyzer_02": _FakeGasAnalyzer({"co2_ratio_f": 1.2}),
        },
    )
    point = _point()
    context = runner._new_sampling_window_context(point=point, phase="co2", point_tag="demo")
    for label, ga, analyzer_cfg in runner._all_gas_analyzers():
        if label == "ga01":
            runner._refresh_sampling_analyzer_cache_entry(
                label,
                ga,
                analyzer_cfg,
                context=context,
                reason="test",
            )
    runner._set_point_runtime_fields(point, phase="co2", analyzer_gate_dropped_labels="ga02")

    assert runner._sampling_context_missing_fresh_analyzers(context) == []
    logger.close()


def test_analyzer_gate_fails_when_min_valid_not_met(tmp_path: Path) -> None:
    cfg = _preseal_gate_cfg(max_wait_s=0.4)
    runner, logger = _runner_for_preseal_gate(
        tmp_path,
        cfg,
        {
            "gas_analyzer_01": _FakeGasAnalyzer({"co2_ratio_f": 1.0}),
            "gas_analyzer_02": _FakeGasAnalyzer({"co2_ratio_f": 0.0}),
            "gas_analyzer_03": _FakeRecoveringGasAnalyzer([None, None, None]),
            "gas_analyzer_04": _FakeRecoveringGasAnalyzer([None, None, None]),
        },
    )

    assert runner._wait_co2_preseal_primary_sensor_gate(_point()) is False
    logger.close()

    state = runner._point_runtime_state(_point(), phase="co2") or {}
    assert state["analyzer_gate_valid_stable_count"] < 2
    assert state["analyzer_gate_final_decision_reason"] == "min_valid_not_met"


def test_analyzer_gate_required_label_failure_blocks(tmp_path: Path) -> None:
    cfg = _preseal_gate_cfg(min_valid=1, required_labels=["ga02"], max_wait_s=1.0)
    cfg["workflow"]["stability"]["analyzer_gate_stable_window_s"] = 1.0
    runner, logger = _runner_for_preseal_gate(
        tmp_path,
        cfg,
        {
            "gas_analyzer_01": _FakeGasAnalyzer({"co2_ratio_f": 1.0}),
            "gas_analyzer_02": _FakeGasAnalyzer({"co2_ratio_f": 0.0}),
            "gas_analyzer_03": _FakeGasAnalyzer({"co2_ratio_f": 0.0}),
            "gas_analyzer_04": _FakeGasAnalyzer({"co2_ratio_f": 1.1}),
        },
    )

    assert runner._wait_co2_preseal_primary_sensor_gate(_point()) is False
    logger.close()

    state = runner._point_runtime_state(_point(), phase="co2") or {}
    assert state["analyzer_gate_valid_stable_count"] >= 1
    assert state["analyzer_gate_final_decision_reason"] == "min_valid_not_met"


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


def test_configure_gas_analyzer_read_first_skips_startup_config_commands(tmp_path: Path) -> None:
    cfg = {
        "devices": {
            "gas_analyzer": {"active_send": False, "ftd_hz": 1, "average_co2": 1, "average_h2o": 1},
            "gas_analyzers": [{"name": "ga01", "active_send": False}],
        },
        "workflow": {
            "analyzer_mode2_init": {
                "read_first_before_config": True,
                "sniff_stream_before_config": True,
                "read_first_attempts": 1,
                "ready_consecutive_frames": 1,
                "read_first_retry_delay_s": 0.0,
            }
        },
    }
    analyzer = _FakeGasAnalyzer({"co2_ppm": 500.0, "h2o_mmol": 2.0, "mode2_field_count": 16})
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(cfg, {"gas_analyzer_01": analyzer}, logger, lambda *_: None, lambda *_: None)

    runner._configure_gas_analyzer(
        analyzer,
        label="ga01",
        mode=2,
        active_send=False,
        ftd_hz=1,
        avg_co2=1,
        avg_h2o=1,
        avg_filter=49,
        warning_phase="startup",
    )

    assert analyzer.calls == []
    logger.close()


def test_configure_gas_analyzer_read_first_can_still_apply_ftd_and_filter(tmp_path: Path) -> None:
    cfg = {
        "devices": {
            "gas_analyzer": {"active_send": True, "ftd_hz": 1, "average_co2": 1, "average_h2o": 1},
            "gas_analyzers": [{"name": "ga01", "active_send": True}],
        },
        "workflow": {
            "analyzer_mode2_init": {
                "read_first_before_config": True,
                "sniff_stream_before_config": True,
                "skip_config_when_read_first_ready": False,
                "read_first_attempts": 1,
                "ready_consecutive_frames": 1,
                "read_first_retry_delay_s": 0.0,
                "reapply_attempts": 1,
                "stream_attempts": 1,
                "post_enable_stream_wait_s": 0.0,
                "retry_delay_s": 0.0,
                "command_gap_s": 0.0,
            }
        },
    }
    analyzer = _FakeGasAnalyzer({"co2_ppm": 500.0, "h2o_mmol": 2.0, "mode2_field_count": 16})
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(cfg, {"gas_analyzer_01": analyzer}, logger, lambda *_: None, lambda *_: None)

    runner._configure_gas_analyzer(
        analyzer,
        label="ga01",
        mode=2,
        active_send=True,
        ftd_hz=1,
        avg_co2=1,
        avg_h2o=1,
        avg_filter=49,
        warning_phase="startup",
    )

    assert ("mode", 2, False) in analyzer.calls
    assert ("ftd", 1) in analyzer.calls
    assert ("avg_filter", 49, False) in analyzer.calls
    assert ("active", True, False) in analyzer.calls
    logger.close()


def test_configure_gas_analyzer_can_keep_active_stream_during_filter_reapply(tmp_path: Path) -> None:
    cfg = {
        "devices": {
            "gas_analyzer": {"active_send": True, "ftd_hz": 1, "average_co2": 1, "average_h2o": 1},
            "gas_analyzers": [{"name": "ga01", "active_send": True}],
        },
        "workflow": {
            "analyzer_mode2_init": {
                "read_first_before_config": True,
                "sniff_stream_before_config": True,
                "skip_config_when_read_first_ready": False,
                "quiet_active_before_config": False,
                "read_first_attempts": 1,
                "ready_consecutive_frames": 1,
                "read_first_retry_delay_s": 0.0,
                "reapply_attempts": 1,
                "stream_attempts": 1,
                "post_enable_stream_wait_s": 0.0,
                "retry_delay_s": 0.0,
                "command_gap_s": 0.0,
            }
        },
    }
    analyzer = _FakeGasAnalyzer({"co2_ppm": 500.0, "h2o_mmol": 2.0, "mode2_field_count": 16})
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(cfg, {"gas_analyzer_01": analyzer}, logger, lambda *_: None, lambda *_: None)

    runner._configure_gas_analyzer(
        analyzer,
        label="ga01",
        mode=2,
        active_send=True,
        ftd_hz=1,
        avg_co2=1,
        avg_h2o=1,
        avg_filter=49,
        warning_phase="startup",
    )

    assert ("active", False, False) not in analyzer.calls
    assert ("mode", 2, False) in analyzer.calls
    assert ("ftd", 1) in analyzer.calls
    assert ("avg_filter", 49, False) in analyzer.calls
    assert ("active", True, False) in analyzer.calls
    logger.close()


def test_configure_gas_analyzer_active_stream_read_first_skips_startup_config_commands(tmp_path: Path) -> None:
    cfg = {
        "devices": {
            "gas_analyzer": {"active_send": True, "ftd_hz": 10, "average_co2": 1, "average_h2o": 1},
            "gas_analyzers": [{"name": "ga01", "active_send": True, "ftd_hz": 10}],
        },
        "workflow": {
            "analyzer_mode2_init": {
                "read_first_before_config": True,
                "sniff_stream_before_config": True,
                "write_config_on_read_first_fail": False,
                "read_first_attempts": 1,
                "ready_consecutive_frames": 1,
                "read_first_retry_delay_s": 0.0,
                "send_active_freq": False,
            }
        },
    }
    analyzer = _FakeStreamingBatchGasAnalyzer(
        [["YGAS,001,500.0,2.0,1,1,1,1,1,1,1,1,25.0,25.0,101.3,OK"]]
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

    assert analyzer.calls == []
    assert analyzer.drain_calls == 1
    logger.close()


def test_configure_gas_analyzer_read_first_no_write_fails_without_hammering_commands(tmp_path: Path) -> None:
    cfg = {
        "devices": {
            "gas_analyzer": {"active_send": False, "ftd_hz": 1, "average_co2": 1, "average_h2o": 1},
            "gas_analyzers": [{"name": "ga01", "active_send": False}],
        },
        "workflow": {
            "analyzer_mode2_init": {
                "read_first_before_config": True,
                "sniff_stream_before_config": True,
                "write_config_on_read_first_fail": False,
                "read_first_attempts": 1,
                "ready_consecutive_frames": 1,
                "read_first_retry_delay_s": 0.0,
            }
        },
    }
    analyzer = _FakeLegacyStreamingGasAnalyzer()
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(cfg, {"gas_analyzer_01": analyzer}, logger, lambda *_: None, lambda *_: None)

    with pytest.raises(RuntimeError, match="startup config writes are disabled"):
        runner._configure_gas_analyzer(
            analyzer,
            label="ga01",
            mode=2,
            active_send=False,
            ftd_hz=1,
            avg_co2=1,
            avg_h2o=1,
            avg_filter=49,
            warning_phase="startup",
        )

    assert analyzer.calls == []
    logger.close()


def test_configure_gas_analyzer_can_skip_ftd_for_fragile_serial_devices(tmp_path: Path) -> None:
    cfg = {
        "devices": {
            "gas_analyzer": {"active_send": False, "ftd_hz": 1, "average_co2": 1, "average_h2o": 1},
            "gas_analyzers": [{"name": "ga01", "active_send": False}],
        },
        "workflow": {
            "analyzer_mode2_init": {
                "send_active_freq": False,
                "read_first_before_config": False,
                "reapply_attempts": 1,
                "passive_attempts": 1,
                "ready_consecutive_frames": 1,
                "retry_delay_s": 0.0,
                "command_gap_s": 0.0,
            }
        },
    }
    analyzer = _FakeGasAnalyzer({"co2_ppm": 500.0, "h2o_mmol": 2.0, "mode2_field_count": 16})
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(cfg, {"gas_analyzer_01": analyzer}, logger, lambda *_: None, lambda *_: None)

    runner._configure_gas_analyzer(
        analyzer,
        label="ga01",
        mode=2,
        active_send=False,
        ftd_hz=1,
        avg_co2=1,
        avg_h2o=1,
        avg_filter=49,
        warning_phase="startup",
    )

    assert ("mode", 2, False) in analyzer.calls
    assert ("active", False, False) in analyzer.calls
    assert not any(call[0] == "ftd" for call in analyzer.calls)
    assert ("avg_filter", 49, False) in analyzer.calls
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
    assert row["sample_begin_ts"]
    assert row["sample_snapshot_ts"]
    assert row["pressure_sample_age_ms"] in ("", None) or row["pressure_sample_age_ms"] >= 0.0
    analyzer_age = json.loads(row["analyzer_sample_age_ms_by_port"])
    assert set(analyzer_age) == {"ga01", "ga02"}
    per_device_age = json.loads(row["per_device_age_ms"])
    per_device_ts = json.loads(row["per_device_sample_ts"])
    per_device_source = json.loads(row["per_device_source"])
    assert per_device_age["ga01"] >= 0.0
    assert per_device_age["ga02"] >= 0.0
    assert per_device_ts["ga01"]
    assert per_device_source["ga01"] == "cache"
    assert row["sample_alignment_ok"] is True
    logger.close()


def test_sampling_timing_evidence_records_per_device_age(tmp_path: Path) -> None:
    test_collect_samples_writes_prefixed_sensor_fields(tmp_path)


def test_sample_count_zero_reports_sampling_not_verified(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(
        {"workflow": {"sampling": {"count": 1}}},
        {},
        logger,
        lambda *_: None,
        lambda *_: None,
    )

    status = runner._sampling_timing_audit_status([])

    assert status["sealed_sample_count"] == 0
    assert status["sample_count"] == 0
    assert status["sampling_fast_claim_allowed"] is False
    assert status["sampling_not_verified_reason"] == "sample_count_zero"
    assert status["sampling_missing_device_timestamp_fields"] == "no_sample_rows"
    logger.close()


def test_sampling_parallel_status_unknown_without_sample_rows(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(
        {"workflow": {"sampling": {"count": 1}}},
        {},
        logger,
        lambda *_: None,
        lambda *_: None,
    )

    status = runner._sampling_timing_audit_status([])

    assert status["sampling_parallel_status"] == "unknown"
    assert status["sampling_snapshot_based"] is False
    assert status["sampling_device_timestamp_complete"] is False
    logger.close()


def test_open_flow_sample_rows_do_not_mark_sealed_sampling_verified(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(
        {"workflow": {"sampling": {"count": 1}}},
        {},
        logger,
        lambda *_: None,
        lambda *_: None,
    )

    status = runner._sampling_timing_audit_status(
        [
            {
                "pressure_mode": "ambient_open",
                "sample_snapshot_ts": "2026-05-19T22:41:00.000",
                "per_device_sample_ts": json.dumps({"pace_pressure": "2026-05-19T22:41:00.000"}),
                "sampling_time_alignment_max_age_ms": 0.0,
                "sampling_time_alignment_p95_age_ms": 0.0,
            }
        ]
    )

    assert status["sample_count"] == 1
    assert status["sealed_sample_count"] == 0
    assert status["sampling_parallel_status"] == "partial"
    assert status["sampling_not_verified_reason"] == "no_sealed_sample_rows"
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


def test_active_analyzer_anchor_match_recovers_from_stale_frame(tmp_path: Path) -> None:
    cfg = {
        "workflow": {
            "analyzer_live_snapshot": {
                "active_frame_recovery_enabled": True,
                "active_frame_recovery_wait_s": 0.35,
                "active_frame_recovery_poll_s": 0.02,
            }
        }
    }
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(cfg, {}, logger, lambda *_: None, lambda *_: None)
    analyzer = object()
    context = runner._new_sampling_window_context(point=_point(), phase="co2", point_tag="demo")
    _runtime_key, buffer = runner._sampling_window_active_analyzer_buffer(context, analyzer, label="ga01")
    anchor = time.monotonic()
    buffer.append(_active_frame(1, anchor - 0.9, 500.0))

    def append_fresh_frame() -> None:
        time.sleep(0.06)
        buffer.append(_active_frame(2, time.monotonic(), 501.0))

    thread = threading.Thread(target=append_fresh_frame)
    thread.start()
    try:
        match = runner._active_analyzer_anchor_match_with_recovery(
            context,
            analyzer,
            label="ga01",
            sample_anchor_mono=anchor,
        )
    finally:
        thread.join(timeout=1.0)
        logger.close()

    assert match["entry"]["seq"] == 2
    assert match["stale"] is False
    assert match["recovered_from_match_strategy"] == "stale_left_far"
    assert str(match["match_strategy"]).endswith(":recovered")


def test_merge_analyzer_cache_rejects_unrecovered_stale_active_frame(tmp_path: Path) -> None:
    cfg = {
        "workflow": {
            "analyzer_live_snapshot": {
                "active_frame_recovery_enabled": False,
            }
        }
    }
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(cfg, {}, logger, lambda *_: None, lambda *_: None)
    analyzer = object()
    context = runner._new_sampling_window_context(point=_point(), phase="co2", point_tag="demo")
    _runtime_key, buffer = runner._sampling_window_active_analyzer_buffer(context, analyzer, label="ga01")
    buffer.append(_active_frame(1, 9.30, 500.0))
    data: dict = {}

    runner._merge_analyzer_cache_into_sample(
        data,
        [("ga01", analyzer, {"name": "ga01", "active_send": True})],
        context=context,
        sample_anchor_mono=10.00,
        row_time_s=time.time(),
    )

    assert data["ga01_frame_stale"] is True
    assert data["ga01_mode2_contract_status"] == "stale"
    assert data["ga01_mode2_qc_status"] == "stale"
    assert data["ga01_mode2_qc_reason"] == "stale_frame"
    assert data["ga01_status_register_qc"] == "missing"
    assert data["ga01_status_register_qc_reason"] == "stale_frame"
    assert data["ga01_frame_has_data"] is False
    logger.close()


def test_status_register_qc_distinguishes_missing_pass_and_fail(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner({}, {}, logger, lambda *_: None, lambda *_: None)

    assert runner._assess_status_register_qc({"status": ""}) == ("missing", "status_empty")
    assert runner._assess_status_register_qc({"status": "OK"}) == ("pass", "ok")
    assert runner._assess_status_register_qc({"status": "0001"}) == ("pass", "ok")
    status, reason = runner._assess_status_register_qc({"status": "0101"})
    assert status == "fail"
    assert "CO2信号超标" in reason
    status, reason = runner._assess_status_register_qc({"status": "CO2_SIGNAL_FAIL"})

    assert status == "fail"
    assert "CO2_SIGNAL_FAIL" in reason
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


def test_sampling_context_uses_per_device_workers_for_multiple_passive_analyzers(tmp_path: Path) -> None:
    cfg = {
        "workflow": {
            "analyzer_live_snapshot": {
                "sampling_worker_enabled": True,
                "passive_round_robin_enabled": True,
                "passive_per_device_workers_enabled": True,
            }
        },
        "devices": {
            "gas_analyzer": {"active_send": False, "ftd_hz": 1, "average_co2": 1, "average_h2o": 1},
            "gas_analyzers": [
                {"name": "ga01", "active_send": False},
                {"name": "ga02", "active_send": False},
            ],
        },
    }
    devices = {
        "gas_analyzer_01": _FakeGasAnalyzer({"co2_ppm": 501.0, "h2o_mmol": 2.1}),
        "gas_analyzer_02": _FakeGasAnalyzer({"co2_ppm": 502.0, "h2o_mmol": 2.2}),
    }
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(cfg, devices, logger, lambda *_: None, lambda *_: None)

    context = runner._start_sampling_window_context(point=_point(), phase="co2", point_tag="900ppm")
    try:
        worker_keys = {str(item.get("key") or "") for item in context.get("workers", []) if isinstance(item, dict)}
        assert "analyzer:passive:ga01" in worker_keys
        assert "analyzer:passive:ga02" in worker_keys
        assert "analyzer:passive" not in worker_keys
        assert context["worker_plan"]["active_entries"] == []
        assert len(context["worker_plan"]["passive_entries"]) == 2
    finally:
        runner._stop_sampling_window_context(context)
        logger.close()


def test_passive_per_device_workers_use_passive_interval_not_active_poll_rate(tmp_path: Path) -> None:
    class _CountingPassiveAnalyzer(_FakeGasAnalyzer):
        def __init__(self, parsed):
            super().__init__(parsed)
            self.read_latest_calls = 0

        def read_latest_data(self, *args, **kwargs):
            self.read_latest_calls += 1
            return self.read_data_passive()

    cfg = {
        "workflow": {
            "analyzer_live_snapshot": {
                "sampling_worker_enabled": True,
                "sampling_worker_interval_s": 0.05,
                "passive_round_robin_enabled": True,
                "passive_round_robin_interval_s": 1.0,
                "passive_per_device_workers_enabled": True,
            }
        },
        "devices": {
            "gas_analyzer": {"active_send": False, "ftd_hz": 1, "average_co2": 1, "average_h2o": 1},
            "gas_analyzers": [
                {"name": "ga01", "active_send": False},
                {"name": "ga02", "active_send": False},
            ],
        },
    }
    devices = {
        "gas_analyzer_01": _CountingPassiveAnalyzer({"co2_ppm": 501.0, "h2o_mmol": 2.1}),
        "gas_analyzer_02": _CountingPassiveAnalyzer({"co2_ppm": 502.0, "h2o_mmol": 2.2}),
    }
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(cfg, devices, logger, lambda *_: None, lambda *_: None)

    context = runner._start_sampling_window_context(point=_point(), phase="co2", point_tag="900ppm")
    try:
        time.sleep(0.35)
        assert devices["gas_analyzer_01"].read_latest_calls <= 1
        assert devices["gas_analyzer_02"].read_latest_calls <= 1
    finally:
        runner._stop_sampling_window_context(context)
        logger.close()
