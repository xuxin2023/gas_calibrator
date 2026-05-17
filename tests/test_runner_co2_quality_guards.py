from __future__ import annotations

import csv
import types
from datetime import datetime, timedelta
from pathlib import Path

import pytest

from gas_calibrator.data.points import CalibrationPoint
from gas_calibrator.logging_utils import RunLogger
from gas_calibrator.validation.dewpoint_flush_gate import evaluate_dewpoint_flush_gate, predict_pressure_scaled_dewpoint_c
from gas_calibrator.workflow import runner as runner_module
from gas_calibrator.workflow.runner import CalibrationRunner


def _point_co2_low_pressure() -> CalibrationPoint:
    return CalibrationPoint(
        index=1,
        temp_chamber_c=20.0,
        co2_ppm=1000.0,
        hgen_temp_c=None,
        hgen_rh_pct=None,
        target_pressure_hpa=700.0,
        dewpoint_c=None,
        h2o_mmol=None,
        raw_h2o=None,
        co2_group="A",
    )


def test_dewpoint_freshness_expired_blocks_or_regates(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(
        {
            "workflow": {
                "stability": {
                    "dewpoint_preseal_freshness_max_age_s": 60.0,
                    "dewpoint_preseal_freshness_max_delta_c": 0.20,
                }
            }
        },
        {},
        logger,
        lambda *_: None,
        lambda *_: None,
    )
    point = _point_co2_low_pressure()
    runner._set_point_runtime_fields(
        point,
        phase="co2",
        flush_gate_status="pass",
        dewpoint_gate_pass_ts=1000.0,
        dewpoint_gate_pass_value_c=-20.00,
        dewpoint_gate_tail_reference_c=-20.00,
    )
    runner._preseal_dewpoint_snapshot = {
        "sample_wall_ts": 1065.0,
        "dewpoint_c": -20.00,
        "temp_c": 20.0,
        "rh_pct": 5.0,
    }

    assert runner._check_preseal_dewpoint_freshness(point, phase="co2") is False
    logger.close()

    state = runner._point_runtime_state(point, phase="co2")
    assert state is not None
    assert state["dewpoint_freshness_expired"] is True
    assert state["dewpoint_freshness_decision"] == "fail_closed"
    assert state["dewpoint_rebound_exceeded"] is False
    trace_rows = []
    path = logger.run_dir / "pressure_transition_trace.csv"
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        import csv

        trace_rows = list(csv.DictReader(handle))
    freshness_rows = [row for row in trace_rows if row["trace_stage"] == "preseal_dewpoint_freshness_check"]
    assert freshness_rows
    assert freshness_rows[-1]["dewpoint_freshness_expired"] == "True"
    assert freshness_rows[-1]["dewpoint_freshness_decision"] == "fail_closed"


def _freshness_runner(tmp_path: Path) -> tuple[CalibrationRunner, RunLogger, CalibrationPoint]:
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(
        {
            "workflow": {
                "stability": {
                    "dewpoint_preseal_freshness_max_age_s": 60.0,
                    "dewpoint_preseal_freshness_max_delta_c": 0.20,
                    "analyzer_gate_dewpoint_monitor_max_gap_s": 15.0,
                    "analyzer_gate_dewpoint_monitor_max_read_errors": 0,
                }
            }
        },
        {},
        logger,
        lambda *_: None,
        lambda *_: None,
    )
    return runner, logger, _point_co2_low_pressure()


def _set_fresh_monitor_state(
    runner: CalibrationRunner,
    point: CalibrationPoint,
    *,
    gate_ts: float = 1000.0,
    gate_value_c: float = -24.89,
    tail_reference_c: float | None = -24.89,
    last_ts: float = 1005.0,
    sample_count: int = 10,
    max_gap_s: float = 5.0,
    read_error_count: int = 0,
) -> None:
    runner._set_point_runtime_fields(
        point,
        phase="co2",
        flush_gate_status="pass",
        dewpoint_gate_pass_ts=gate_ts,
        dewpoint_gate_pass_value_c=gate_value_c,
        dewpoint_gate_tail_reference_c=tail_reference_c,
        analyzer_gate_dewpoint_monitor_enabled=True,
        analyzer_gate_dewpoint_live_sample_count=sample_count,
        analyzer_gate_dewpoint_live_last_ts=(
            datetime.fromtimestamp(float(last_ts)).isoformat(timespec="milliseconds")
            if sample_count > 0
            else ""
        ),
        analyzer_gate_dewpoint_live_max_gap_s=max_gap_s,
        analyzer_gate_dewpoint_read_error_count=read_error_count,
    )


def test_dewpoint_falling_does_not_fail_freshness(tmp_path: Path) -> None:
    runner, logger, point = _freshness_runner(tmp_path)
    _set_fresh_monitor_state(runner, point, gate_value_c=-24.89)
    runner._preseal_dewpoint_snapshot = {
        "sample_wall_ts": 1010.0,
        "dewpoint_c": -25.45,
        "temp_c": 20.0,
        "rh_pct": 5.0,
    }

    assert runner._check_preseal_dewpoint_freshness(point, phase="co2") is True
    logger.close()

    state = runner._point_runtime_state(point, phase="co2") or {}
    assert state["dewpoint_delta_since_gate_c"] == pytest.approx(-0.56, abs=0.001)
    assert state["dewpoint_trend"] == "falling"
    assert state["dewpoint_rebound_exceeded"] is False
    assert state["dewpoint_freshness_sample_decision"] == "pass"
    assert state["dewpoint_preseal_decision"] == "pass"


def test_dewpoint_positive_rebound_fails(tmp_path: Path) -> None:
    runner, logger, point = _freshness_runner(tmp_path)
    _set_fresh_monitor_state(runner, point, gate_value_c=-31.30, tail_reference_c=-31.30)
    runner._preseal_dewpoint_snapshot = {
        "sample_wall_ts": 1010.0,
        "dewpoint_c": -24.89,
        "temp_c": 20.0,
        "rh_pct": 5.0,
    }

    assert runner._check_preseal_dewpoint_freshness(point, phase="co2") is False
    logger.close()

    state = runner._point_runtime_state(point, phase="co2") or {}
    assert state["dewpoint_rebound_delta_c"] == pytest.approx(6.41, abs=0.001)
    assert state["dewpoint_rebound_exceeded"] is True
    assert state["dewpoint_rebound_decision"] == "fail_closed"
    assert state["dewpoint_freshness_sample_decision"] == "pass"
    assert runner._controlled_exit_final_decision == "FAIL_CLOSED_DEWPOINT_REBOUND_DURING_PRESEAL"


def test_dewpoint_abs_delta_no_longer_used_for_falling(tmp_path: Path) -> None:
    runner, logger, point = _freshness_runner(tmp_path)
    _set_fresh_monitor_state(runner, point, gate_value_c=-24.89)
    runner._preseal_dewpoint_snapshot = {
        "sample_wall_ts": 1010.0,
        "dewpoint_c": -31.30,
        "temp_c": 20.0,
        "rh_pct": 5.0,
    }

    assert runner._check_preseal_dewpoint_freshness(point, phase="co2") is True
    logger.close()

    state = runner._point_runtime_state(point, phase="co2") or {}
    assert abs(state["dewpoint_delta_since_gate_c"]) > 0.20
    assert state["dewpoint_delta_since_gate_c"] < 0.0
    assert state["dewpoint_rebound_exceeded"] is False
    assert state["dewpoint_preseal_decision"] == "pass"


def test_dewpoint_live_sample_gap_still_fails(tmp_path: Path) -> None:
    runner, logger, point = _freshness_runner(tmp_path)
    _set_fresh_monitor_state(runner, point, max_gap_s=16.0)
    runner._preseal_dewpoint_snapshot = {
        "sample_wall_ts": 1010.0,
        "dewpoint_c": -24.90,
        "temp_c": 20.0,
        "rh_pct": 5.0,
    }

    assert runner._check_preseal_dewpoint_freshness(point, phase="co2") is False
    logger.close()

    state = runner._point_runtime_state(point, phase="co2") or {}
    assert state["dewpoint_freshness_sample_decision"] == "fail_closed"
    assert state["dewpoint_rebound_exceeded"] is False
    assert runner._controlled_exit_final_decision == "FAIL_CLOSED_DEWPOINT_LIVE_SAMPLE_GAP_EXCEEDED"


def test_dewpoint_no_live_sample_still_fails(tmp_path: Path) -> None:
    runner, logger, point = _freshness_runner(tmp_path)
    _set_fresh_monitor_state(runner, point, sample_count=0, max_gap_s=0.0)
    runner._preseal_dewpoint_snapshot = {
        "sample_wall_ts": 1010.0,
        "dewpoint_c": -24.90,
        "temp_c": 20.0,
        "rh_pct": 5.0,
    }

    assert runner._check_preseal_dewpoint_freshness(point, phase="co2") is False
    logger.close()

    state = runner._point_runtime_state(point, phase="co2") or {}
    assert state["dewpoint_freshness_sample_decision"] == "fail_closed"
    assert state["dewpoint_rebound_exceeded"] is False
    assert runner._controlled_exit_final_decision == "FAIL_CLOSED_DEWPOINT_FRESHNESS_EXPIRED"


def test_preseal_freshness_uses_positive_rebound_only(tmp_path: Path) -> None:
    runner, logger, point = _freshness_runner(tmp_path)
    _set_fresh_monitor_state(runner, point, gate_value_c=-24.89)
    runner._preseal_dewpoint_snapshot = {
        "sample_wall_ts": 1010.0,
        "dewpoint_c": -25.45,
        "temp_c": 20.0,
        "rh_pct": 5.0,
    }

    assert runner._check_preseal_dewpoint_freshness(point, phase="co2") is True
    logger.close()

    state = runner._point_runtime_state(point, phase="co2") or {}
    assert state["dewpoint_delta_since_gate_c"] == pytest.approx(-0.56, abs=0.001)
    assert state["dewpoint_rebound_decision"] == "pass"
    assert state["dewpoint_preseal_decision"] == "pass"


def test_base_soak_does_not_count_as_dewpoint_gate_coverage() -> None:
    start = datetime(2026, 5, 17, 23, 42, 0)
    rows = [
        {
            "timestamp": start.isoformat(timespec="milliseconds"),
            "phase_elapsed_s": 300.0,
            "controller_vent_state": "VENT_ON",
            "dewpoint_c": -24.88,
        },
        {
            "timestamp": (start + timedelta(seconds=2)).isoformat(timespec="milliseconds"),
            "phase_elapsed_s": 302.0,
            "controller_vent_state": "VENT_ON",
            "dewpoint_c": -24.89,
        },
    ]

    gate = evaluate_dewpoint_flush_gate(
        rows,
        min_flush_s=300.0,
        gate_window_s=30.0,
        min_tail_samples=3,
        min_tail_coverage_ratio=0.8,
        max_tail_gap_s=15.0,
    )

    assert gate["gate_pass"] is False
    assert "dewpoint_tail_sample_count_insufficient" in gate["gate_reason"]
    assert "dewpoint_tail_coverage_insufficient" in gate["gate_reason"]


def test_dewpoint_gate_requires_tail_window_after_soak() -> None:
    start = datetime(2026, 5, 17, 23, 42, 0)
    rows = [
        {
            "timestamp": (start + timedelta(seconds=idx * 5)).isoformat(timespec="milliseconds"),
            "phase_elapsed_s": idx * 5.0,
            "controller_vent_state": "VENT_ON",
            "dewpoint_c": -30.0,
        }
        for idx in range(7)
    ]

    gate = evaluate_dewpoint_flush_gate(
        rows,
        min_flush_s=24.0,
        gate_window_s=30.0,
        min_tail_samples=3,
        min_tail_coverage_ratio=0.8,
        max_tail_gap_s=15.0,
    )

    assert gate["gate_pass"] is True
    assert gate["dewpoint_gate_tail_coverage_s"] >= 24.0
    assert gate["dewpoint_time_to_gate"] == pytest.approx(30.0)


def test_dewpoint_gate_tail_reference_computed_from_gate_samples() -> None:
    start = datetime(2026, 5, 17, 23, 42, 0)
    rows = [
        {
            "timestamp": (start + timedelta(seconds=idx * 5)).isoformat(timespec="milliseconds"),
            "phase_elapsed_s": idx * 5.0,
            "controller_vent_state": "VENT_ON",
            "dewpoint_c": value,
        }
        for idx, value in enumerate([-30.04, -30.02, -30.01, -30.00, -30.00, -30.01, -30.00])
    ]

    gate = evaluate_dewpoint_flush_gate(
        rows,
        min_flush_s=24.0,
        gate_window_s=30.0,
        min_tail_samples=3,
        min_tail_coverage_ratio=0.8,
        max_tail_gap_s=15.0,
        tail_reference_method="median",
    )

    assert gate["gate_pass"] is True
    assert gate["dewpoint_gate_tail_sample_count"] == 7
    assert gate["dewpoint_gate_tail_coverage_s"] >= 24.0
    assert gate["dewpoint_gate_tail_last_c"] == pytest.approx(-30.00)
    assert gate["dewpoint_gate_tail_mean_c"] == pytest.approx(sum([-30.04, -30.02, -30.01, -30.00, -30.00, -30.01, -30.00]) / 7)
    assert gate["dewpoint_gate_tail_median_c"] == pytest.approx(-30.01)
    assert gate["dewpoint_gate_tail_min_c"] == pytest.approx(-30.04)
    assert gate["dewpoint_gate_tail_max_c"] == pytest.approx(-30.00)
    assert gate["dewpoint_gate_tail_span_c"] == pytest.approx(0.04)
    assert gate["dewpoint_gate_tail_reference_method"] == "median"
    assert gate["dewpoint_gate_tail_reference_c"] == pytest.approx(-30.01)


def test_preseal_rebound_uses_tail_reference_not_pass_snapshot(tmp_path: Path) -> None:
    runner, logger, point = _freshness_runner(tmp_path)
    _set_fresh_monitor_state(
        runner,
        point,
        gate_value_c=-24.89,
        tail_reference_c=-30.00,
    )
    runner._preseal_dewpoint_snapshot = {
        "sample_wall_ts": 1010.0,
        "dewpoint_c": -25.00,
        "temp_c": 20.0,
        "rh_pct": 5.0,
    }

    assert runner._check_preseal_dewpoint_freshness(point, phase="co2") is False
    logger.close()

    state = runner._point_runtime_state(point, phase="co2") or {}
    assert state["dewpoint_rebound_delta_c"] == pytest.approx(5.0, abs=0.001)
    assert runner._controlled_exit_final_decision == "FAIL_CLOSED_DEWPOINT_REBOUND_DURING_PRESEAL"


def test_tail_reference_minus30_analyzer_minus25_fails_rebound(tmp_path: Path) -> None:
    runner, logger, point = _freshness_runner(tmp_path)
    _set_fresh_monitor_state(runner, point, gate_value_c=-24.89, tail_reference_c=-30.0)
    runner._preseal_dewpoint_snapshot = {
        "sample_wall_ts": 1010.0,
        "dewpoint_c": -25.0,
        "temp_c": 20.0,
        "rh_pct": 5.0,
    }

    assert runner._check_preseal_dewpoint_freshness(point, phase="co2") is False
    logger.close()

    assert runner._controlled_exit_final_decision == "FAIL_CLOSED_DEWPOINT_REBOUND_DURING_PRESEAL"


def test_tail_reference_minus24_analyzer_minus25_passes_rebound(tmp_path: Path) -> None:
    runner, logger, point = _freshness_runner(tmp_path)
    _set_fresh_monitor_state(runner, point, gate_value_c=-24.89, tail_reference_c=-24.89)
    runner._preseal_dewpoint_snapshot = {
        "sample_wall_ts": 1010.0,
        "dewpoint_c": -25.45,
        "temp_c": 20.0,
        "rh_pct": 5.0,
    }

    assert runner._check_preseal_dewpoint_freshness(point, phase="co2") is True
    logger.close()

    state = runner._point_runtime_state(point, phase="co2") or {}
    assert state["dewpoint_rebound_delta_c"] == pytest.approx(-0.56, abs=0.001)
    assert state["dewpoint_rebound_decision"] == "pass"


def test_no_tail_reference_fails_closed(tmp_path: Path) -> None:
    runner, logger, point = _freshness_runner(tmp_path)
    _set_fresh_monitor_state(runner, point, gate_value_c=-24.89, tail_reference_c=None)
    runner._preseal_dewpoint_snapshot = {
        "sample_wall_ts": 1010.0,
        "dewpoint_c": -25.45,
        "temp_c": 20.0,
        "rh_pct": 5.0,
    }

    assert runner._check_preseal_dewpoint_freshness(point, phase="co2") is False
    logger.close()

    assert runner._controlled_exit_final_decision == "FAIL_CLOSED_DEWPOINT_GATE_INSUFFICIENT_COVERAGE"


class _FakeDewpointGateMeter:
    def __init__(self, values: list[float]):
        self.values = list(values)
        self.index = 0

    def get_current_fast(self, *_, **__) -> dict[str, float]:
        if self.index < len(self.values):
            value = self.values[self.index]
            self.index += 1
        else:
            value = self.values[-1]
        return {"dewpoint_c": value, "temp_c": 20.0, "rh_pct": 5.0}


class _FakeClock:
    def __init__(self, start: float = 1000.0):
        self.current = float(start)

    def time(self) -> float:
        return self.current

    def sleep(self, seconds: float) -> None:
        self.current += max(0.0, float(seconds))


def _patch_runner_clock(monkeypatch: pytest.MonkeyPatch, clock: _FakeClock) -> None:
    class _PatchedDateTime:
        @classmethod
        def now(cls, *_, **__) -> datetime:
            return datetime.fromtimestamp(clock.time())

        @classmethod
        def fromtimestamp(cls, value: float, *_, **__) -> datetime:
            return datetime.fromtimestamp(float(value))

    monkeypatch.setattr(runner_module.time, "time", clock.time)
    monkeypatch.setattr(runner_module.time, "sleep", clock.sleep)
    monkeypatch.setattr(runner_module, "datetime", _PatchedDateTime)


def _dewpoint_gate_runner(
    tmp_path: Path,
    *,
    dewpoints: list[float],
    max_total_wait_s: float = 120.0,
) -> tuple[CalibrationRunner, RunLogger, CalibrationPoint]:
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(
        {
            "workflow": {
                "stability": {
                    "gas_route_dewpoint_gate_enabled": True,
                    "gas_route_dewpoint_gate_window_s": 30.0,
                    "gas_route_dewpoint_gate_poll_s": 1.0,
                    "gas_route_dewpoint_gate_max_total_wait_s": max_total_wait_s,
                    "gas_route_dewpoint_gate_log_interval_s": 999.0,
                    "gas_route_dewpoint_gate_tail_min_coverage_ratio": 0.8,
                    "gas_route_dewpoint_gate_tail_max_gap_s": 5.0,
                    "gas_route_dewpoint_gate_tail_reference_method": "median",
                }
            }
        },
        {"dewpoint": _FakeDewpointGateMeter(dewpoints)},
        logger,
        lambda *_: None,
        lambda *_: None,
    )
    return runner, logger, _point_co2_low_pressure()


def _base_soak_runner(
    tmp_path: Path,
    *,
    dewpoints: list[float],
    gate_enabled: bool = False,
) -> tuple[CalibrationRunner, RunLogger, CalibrationPoint]:
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(
        {
            "workflow": {
                "stability": {
                    "co2_route": {"first_point_preseal_soak_s": 5.0},
                    "co2_route_base_soak_dewpoint_trace_enabled": True,
                    "co2_route_base_soak_dewpoint_sample_interval_s": 1.0,
                    "gas_route_dewpoint_gate_enabled": gate_enabled,
                    "gas_route_dewpoint_gate_window_s": 30.0,
                    "gas_route_dewpoint_gate_poll_s": 1.0,
                    "gas_route_dewpoint_gate_max_total_wait_s": 120.0,
                    "gas_route_dewpoint_gate_log_interval_s": 999.0,
                    "gas_route_dewpoint_gate_tail_min_coverage_ratio": 0.8,
                    "gas_route_dewpoint_gate_tail_max_gap_s": 5.0,
                    "gas_route_dewpoint_gate_tail_reference_method": "median",
                }
            }
        },
        {"dewpoint": _FakeDewpointGateMeter(dewpoints)},
        logger,
        lambda *_: None,
        lambda *_: None,
    )
    return runner, logger, _point_co2_low_pressure()


def test_base_soak_records_dewpoint_timeline(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner, logger, point = _base_soak_runner(
        tmp_path,
        dewpoints=[-24.8, -25.0, -25.3, -25.2, -25.1, -25.0],
        gate_enabled=False,
    )
    clock = _FakeClock()
    _patch_runner_clock(monkeypatch, clock)

    assert runner._wait_co2_route_soak_before_seal(point)
    logger.close()

    state = runner._point_runtime_state(point, phase="co2") or {}
    assert state["co2_route_base_soak_completed"] is True
    assert state["co2_route_base_soak_dewpoint_trace_enabled"] is True
    assert state["co2_route_base_soak_dewpoint_sample_count"] >= 4
    assert state["co2_route_base_soak_dewpoint_min_c"] == pytest.approx(-25.3)
    timeline_path = Path(state["co2_route_base_soak_dewpoint_timeline_csv"])
    assert timeline_path.exists()
    with timeline_path.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    assert rows
    assert {"ts", "elapsed_since_route_open_s", "dewpoint_c", "read_ok"}.issubset(rows[0])


def test_base_soak_dewpoint_not_counted_as_gate_coverage(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner, logger, point = _base_soak_runner(
        tmp_path,
        dewpoints=[-25.0] * 80,
        gate_enabled=True,
    )
    clock = _FakeClock()
    _patch_runner_clock(monkeypatch, clock)

    assert runner._wait_co2_route_soak_before_seal(point)
    logger.close()

    state = runner._point_runtime_state(point, phase="co2") or {}
    assert state["co2_route_base_soak_dewpoint_sample_count"] > 0
    assert state["dewpoint_gate_phase_elapsed_includes_base_soak"] is False
    assert state["dewpoint_gate_coverage_s"] >= 24.0
    assert state["dewpoint_gate_coverage_s"] < 30.0
    assert state["dewpoint_gate_elapsed_s"] < 120.0


def test_base_soak_dewpoint_timeline_csv_written(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner, logger, point = _base_soak_runner(
        tmp_path,
        dewpoints=[-24.8, -24.9, -25.0, -24.7, -24.6],
        gate_enabled=False,
    )
    clock = _FakeClock()
    _patch_runner_clock(monkeypatch, clock)

    assert runner._wait_co2_route_soak_before_seal(point)
    logger.close()

    timeline_path = logger.run_dir / "co2_route_base_soak_dewpoint_timeline.csv"
    assert timeline_path.exists()
    with timeline_path.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    assert len(rows) >= 4
    assert rows[0]["source"] == "co2_route_base_soak_dewpoint_trace"


def test_dewpoint_gate_runtime_fields_no_duplicate_kwargs(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner, logger, point = _dewpoint_gate_runner(
        tmp_path,
        dewpoints=[-24.81, -24.89],
        max_total_wait_s=1.0,
    )
    clock = _FakeClock()
    _patch_runner_clock(monkeypatch, clock)

    assert (
        runner._wait_co2_route_dewpoint_gate_before_seal(
            point,
            base_soak_s=300.0,
            log_context="unit test insufficient coverage",
        )
        is False
    )
    logger.close()

    state = runner._point_runtime_state(point, phase="co2") or {}
    assert state["co2_route_base_soak_s"] == pytest.approx(300.0)
    assert state["co2_route_base_soak_completed"] is True
    assert state["dewpoint_gate_phase_elapsed_includes_base_soak"] is False
    assert state["dewpoint_gate_coverage_s"] < 24.0
    assert state["flush_gate_status"] == "timeout"
    assert runner._controlled_exit_final_decision == "FAIL_CLOSED_DEWPOINT_GATE_INSUFFICIENT_COVERAGE"


def test_dewpoint_gate_phase_elapsed_includes_base_soak_recorded_once(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner, logger, point = _dewpoint_gate_runner(
        tmp_path,
        dewpoints=[-30.0] * 40,
        max_total_wait_s=120.0,
    )
    clock = _FakeClock()
    _patch_runner_clock(monkeypatch, clock)

    assert runner._wait_co2_route_dewpoint_gate_before_seal(
        point,
        base_soak_s=300.0,
        log_context="unit test pass",
    )
    logger.close()

    import csv

    path = logger.run_dir / "pressure_transition_trace.csv"
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        assert reader.fieldnames is not None
        assert reader.fieldnames.count("dewpoint_gate_phase_elapsed_includes_base_soak") == 1
        rows = list(reader)
    gate_end = [row for row in rows if row["trace_stage"] == "co2_precondition_dewpoint_gate_end"][-1]
    assert gate_end["dewpoint_gate_phase_elapsed_includes_base_soak"] == "False"


def test_dewpoint_gate_waiting_with_many_samples_does_not_abort(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner, logger, point = _dewpoint_gate_runner(
        tmp_path,
        dewpoints=[-30.0] * 40,
        max_total_wait_s=120.0,
    )
    clock = _FakeClock()
    _patch_runner_clock(monkeypatch, clock)

    assert runner._wait_co2_route_dewpoint_gate_before_seal(
        point,
        base_soak_s=300.0,
        log_context="unit test many samples",
    )
    logger.close()

    state = runner._point_runtime_state(point, phase="co2") or {}
    assert state["flush_gate_status"] == "pass"
    assert state["dewpoint_gate_elapsed_s"] >= 24.0
    assert state["dewpoint_gate_coverage_s"] >= 24.0
    assert state["dewpoint_gate_phase_elapsed_includes_base_soak"] is False
    assert state["dewpoint_gate_tail_reference_c"] == pytest.approx(-30.0)


@pytest.mark.parametrize(
    ("policy", "expected_allowed", "expected_blocked"),
    [
        ("pass", True, False),
        ("warn", True, False),
        ("reject", False, True),
    ],
)
def test_wait_postseal_dewpoint_gate_timeout_policy_variants(
    tmp_path: Path,
    policy: str,
    expected_allowed: bool,
    expected_blocked: bool,
) -> None:
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(
        {
            "workflow": {
                "pressure": {
                    "co2_postseal_dewpoint_window_s": 2.0,
                    "co2_postseal_dewpoint_timeout_s": 0.0,
                    "co2_postseal_dewpoint_span_c": 0.05,
                    "co2_postseal_dewpoint_slope_c_per_s": 0.05,
                    "co2_postseal_dewpoint_min_samples": 4,
                    "co2_postseal_timeout_policy": policy,
                }
            }
        },
        {"dewpoint": types.SimpleNamespace()},
        logger,
        lambda *_: None,
        lambda *_: None,
    )
    point = _point_co2_low_pressure()
    context = {"stop_event": None}

    runner._ensure_pressure_transition_fast_signal_cache = types.MethodType(lambda self, *_args, **_kwargs: [], runner)
    runner._cached_ready_check_trace_values = types.MethodType(
        lambda self, context=None, point=None: {
            "pace_pressure_hpa": 700.0,
            "pressure_gauge_hpa": 700.0,
            "dewpoint_live_c": -24.8,
            "dew_temp_live_c": 20.0,
            "dew_rh_live_pct": 6.0,
        },
        runner,
    )
    runner._recent_fast_signal_numeric_observation = types.MethodType(
        lambda self, *_args, **_kwargs: {
            "count": 1,
            "span": 0.2,
            "slope_per_s": 0.2,
            "window_s": 2.0,
        },
        runner,
    )

    assert runner._wait_postseal_dewpoint_gate(point, phase="co2", context=context) is expected_allowed
    logger.close()

    state = runner._point_runtime_state(point, phase="co2")
    assert state is not None
    assert state["dewpoint_gate_result"] == "timeout"
    assert state["postseal_timeout_policy"] == policy
    assert state["point_quality_timeout_flag"] is True
    assert state["postseal_timeout_blocked"] is expected_blocked


def test_wait_postseal_dewpoint_gate_rebound_vetoes_low_pressure_co2(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    messages: list[str] = []
    runner = CalibrationRunner(
        {
            "workflow": {
                "pressure": {
                    "co2_postseal_dewpoint_window_s": 2.0,
                    "co2_postseal_dewpoint_timeout_s": 5.5,
                    "co2_postseal_dewpoint_span_c": 0.05,
                    "co2_postseal_dewpoint_slope_c_per_s": 0.05,
                    "co2_postseal_dewpoint_min_samples": 4,
                    "co2_postseal_rebound_guard_enabled": True,
                    "co2_postseal_rebound_window_s": 8.0,
                    "co2_postseal_rebound_min_rise_c": 0.1,
                }
            }
        },
        {"dewpoint": types.SimpleNamespace()},
        logger,
        messages.append,
        lambda *_: None,
    )
    point = _point_co2_low_pressure()
    context = {"stop_event": None}
    seq = {"index": 0}
    dewpoints = [-25.0, -24.84, -24.84, -24.84]

    runner._ensure_pressure_transition_fast_signal_cache = types.MethodType(lambda self, *_args, **_kwargs: [], runner)
    runner._cached_ready_check_trace_values = types.MethodType(
        lambda self, context=None, point=None: {
            "pace_pressure_hpa": 700.0,
            "pressure_gauge_hpa": 700.0,
            "dewpoint_live_c": dewpoints[min(seq["index"], len(dewpoints) - 1)],
            "dew_temp_live_c": 20.0,
            "dew_rh_live_pct": 6.0,
        },
        runner,
    )
    runner._recent_fast_signal_numeric_observation = types.MethodType(
        lambda self, *_args, **_kwargs: {
            "count": min(seq["index"] + 1, 4),
            "span": 0.01,
            "slope_per_s": 0.0,
            "window_s": 2.0,
        },
        runner,
    )
    runner._sampling_window_wait = types.MethodType(
        lambda self, duration_s, stop_event=None: seq.__setitem__("index", seq["index"] + 1) or True,
        runner,
    )

    assert runner._wait_postseal_dewpoint_gate(point, phase="co2", context=context) is False
    logger.close()

    state = runner._point_runtime_state(point, phase="co2")
    assert state is not None
    assert state["dewpoint_gate_result"] == "rebound_veto"
    assert any("rebound veto" in message for message in messages)


def test_evaluate_co2_postseal_physical_qc_passes_when_delta_within_limit(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(
        {
            "workflow": {
                "pressure": {
                    "co2_postseal_physical_qc_enabled": True,
                    "co2_postseal_physical_qc_max_abs_delta_c": 0.5,
                    "co2_postseal_physical_qc_policy": "warn",
                }
            }
        },
        {},
        logger,
        lambda *_: None,
        lambda *_: None,
    )
    point = _point_co2_low_pressure()
    runner._preseal_dewpoint_snapshot = {
        "dewpoint_c": -18.0,
        "temp_c": 20.0,
        "rh_pct": 5.0,
        "pressure_hpa": 1140.0,
    }
    predicted = predict_pressure_scaled_dewpoint_c(-18.0, 1140.0, point.target_pressure_hpa)

    qc = runner._evaluate_co2_postseal_physical_qc(
        point,
        actual_dewpoint_c=float(predicted or -24.0) + 0.1,
    )
    logger.close()

    assert qc["postseal_expected_dewpoint_c"] is not None
    assert qc["postseal_physical_qc_status"] == "pass"
    assert qc["postseal_physical_qc_reason"] == ""


def test_evaluate_co2_postseal_physical_qc_fails_when_delta_exceeds_limit(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(
        {
            "workflow": {
                "pressure": {
                    "co2_postseal_physical_qc_enabled": True,
                    "co2_postseal_physical_qc_max_abs_delta_c": 0.5,
                    "co2_postseal_physical_qc_policy": "reject",
                }
            }
        },
        {},
        logger,
        lambda *_: None,
        lambda *_: None,
    )
    point = _point_co2_low_pressure()
    runner._preseal_dewpoint_snapshot = {
        "dewpoint_c": -18.0,
        "temp_c": 20.0,
        "rh_pct": 5.0,
        "pressure_hpa": 1140.0,
    }
    predicted = predict_pressure_scaled_dewpoint_c(-18.0, 1140.0, point.target_pressure_hpa)

    qc = runner._evaluate_co2_postseal_physical_qc(
        point,
        actual_dewpoint_c=float(predicted or -24.0) + 1.1,
    )
    logger.close()

    assert qc["postseal_physical_qc_status"] == "fail"
    assert "policy=reject" in qc["postseal_physical_qc_reason"]


@pytest.mark.parametrize(
    ("policy", "expected_status"),
    [
        ("warn", "warn"),
        ("reject", "fail"),
    ],
)
def test_evaluate_co2_postsample_late_rebound_warns_or_fails(
    tmp_path: Path,
    policy: str,
    expected_status: str,
) -> None:
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(
        {
            "workflow": {
                "pressure": {
                    "co2_postsample_late_rebound_guard_enabled": True,
                    "co2_postsample_late_rebound_max_rise_c": 0.12,
                    "co2_postsample_late_rebound_policy": policy,
                }
            }
        },
        {},
        logger,
        lambda *_: None,
        lambda *_: None,
    )
    point = _point_co2_low_pressure()
    runner._set_point_runtime_fields(point, phase="co2", dewpoint_gate_pass_live_c=-24.5)

    result = runner._evaluate_co2_postsample_late_rebound(
        point,
        phase="co2",
        first_effective_sample_dewpoint_c=-23.9,
    )
    logger.close()

    assert result["dewpoint_gate_pass_live_c"] == -24.5
    assert result["first_effective_sample_dewpoint_c"] == -23.9
    assert result["postgate_to_first_effective_dewpoint_rise_c"] == 0.6
    assert result["postsample_late_rebound_status"] == expected_status
    assert f"policy={policy}" in result["postsample_late_rebound_reason"]


def test_copy_point_runtime_exports_into_samples_includes_preseal_snapshot_fields(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner({}, {}, logger, lambda *_: None, lambda *_: None)
    point = _point_co2_low_pressure()
    runner._set_point_runtime_fields(
        point,
        phase="co2",
        preseal_dewpoint_c=-18.0,
        preseal_temp_c=20.0,
        preseal_rh_pct=5.0,
        preseal_pressure_hpa=1140.0,
        postseal_expected_dewpoint_c=-24.0,
        postseal_actual_dewpoint_c=-23.8,
        postseal_physical_delta_c=0.2,
        postseal_physical_qc_status="pass",
        postseal_physical_qc_reason="",
        postseal_timeout_policy="warn",
        postseal_timeout_blocked=False,
        point_quality_timeout_flag=True,
        dewpoint_gate_pass_live_c=-24.2,
        presample_long_guard_status="warn",
        presample_long_guard_reason="timeout_elapsed_s=20.000;rise_c=0.180>max_rise_c=0.120;policy=warn",
        presample_long_guard_elapsed_s=20.0,
        presample_long_guard_span_c=0.22,
        presample_long_guard_slope_c_per_s=0.03,
        presample_long_guard_rise_c=0.18,
        first_effective_sample_dewpoint_c=-23.9,
        postgate_to_first_effective_dewpoint_rise_c=0.3,
        postsample_late_rebound_status="warn",
        postsample_late_rebound_reason="rise_c=0.300>max_rise_c=0.120;policy=warn",
        sampling_window_dewpoint_first_c=-24.1,
        sampling_window_dewpoint_last_c=-23.5,
        sampling_window_dewpoint_range_c=0.6,
        sampling_window_dewpoint_rise_c=0.6,
        sampling_window_dewpoint_slope_c_per_s=0.066667,
        sampling_window_qc_status="warn",
        sampling_window_qc_reason="range_c=0.600>max_range_c=0.200;policy=warn",
        pressure_gauge_stale_count=10,
        pressure_gauge_total_count=10,
        pressure_gauge_stale_ratio=1.0,
        point_quality_status="fail",
        point_quality_reason="pressure_gauge_stale_ratio=1.000>reject_max=0.500",
        point_quality_flags="pressure_gauge_stale_ratio",
        point_quality_blocked=True,
    )
    rows = [{"sample_ts": "2026-04-03T09:00:00.000"}]

    runner._copy_point_runtime_exports_into_samples(point, phase="co2", samples=rows)
    logger.close()

    row = rows[0]
    assert row["preseal_dewpoint_c"] == -18.0
    assert row["preseal_temp_c"] == 20.0
    assert row["preseal_rh_pct"] == 5.0
    assert row["preseal_pressure_hpa"] == 1140.0
    assert row["postseal_expected_dewpoint_c"] == -24.0
    assert row["postseal_physical_qc_status"] == "pass"
    assert row["postseal_timeout_policy"] == "warn"
    assert row["point_quality_timeout_flag"] is True
    assert row["dewpoint_gate_pass_live_c"] == -24.2
    assert row["presample_long_guard_status"] == "warn"
    assert row["presample_long_guard_elapsed_s"] == 20.0
    assert row["presample_long_guard_rise_c"] == 0.18
    assert row["first_effective_sample_dewpoint_c"] == -23.9
    assert row["postsample_late_rebound_status"] == "warn"
    assert row["sampling_window_dewpoint_range_c"] == 0.6
    assert row["sampling_window_qc_status"] == "warn"
    assert row["pressure_gauge_stale_count"] == 10
    assert row["pressure_gauge_stale_ratio"] == 1.0
    assert row["point_quality_status"] == "fail"
    assert row["point_quality_flags"] == "pressure_gauge_stale_ratio"


def test_build_point_summary_row_includes_long_guard_and_sampling_window_qc_fields(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner({}, {}, logger, lambda *_: None, lambda *_: None)
    point = _point_co2_low_pressure()
    runner._set_point_runtime_fields(
        point,
        phase="co2",
        dewpoint_gate_result="stable",
        presample_long_guard_status="warn",
        presample_long_guard_reason="timeout_elapsed_s=20.000;policy=warn",
        presample_long_guard_elapsed_s=20.0,
        presample_long_guard_span_c=0.18,
        presample_long_guard_slope_c_per_s=0.021,
        presample_long_guard_rise_c=0.14,
        sampling_window_dewpoint_first_c=-24.1,
        sampling_window_dewpoint_last_c=-23.6,
        sampling_window_dewpoint_range_c=0.5,
        sampling_window_dewpoint_rise_c=0.5,
        sampling_window_dewpoint_slope_c_per_s=0.055556,
        sampling_window_qc_status="warn",
        sampling_window_qc_reason="range_c=0.500>max_range_c=0.200;policy=warn",
    )

    row = runner._build_point_summary_row(
        point,
        [
            {
                "pressure_hpa": 700.0,
                "pressure_gauge_hpa": 700.0,
                "dewpoint_c": -24.1,
                "dew_temp_c": 20.0,
                "dew_rh_pct": 6.0,
            }
        ],
        phase="co2",
        point_tag="",
        integrity_summary={},
    )
    logger.close()

    assert row["presample_long_guard_status"] == "warn"
    assert row["presample_long_guard_reason"] == "timeout_elapsed_s=20.000;policy=warn"
    assert row["presample_long_guard_rise_c"] == 0.14
    assert row["sampling_window_dewpoint_first_c"] == -24.1
    assert row["sampling_window_dewpoint_last_c"] == -23.6
    assert row["sampling_window_qc_status"] == "warn"
    assert row["sampling_window_qc_reason"] == "range_c=0.500>max_range_c=0.200;policy=warn"
