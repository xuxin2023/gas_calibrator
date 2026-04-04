from __future__ import annotations

import types
from pathlib import Path

import pytest

from gas_calibrator.data.points import CalibrationPoint
from gas_calibrator.logging_utils import RunLogger
from gas_calibrator.workflow import runner as runner_module
from gas_calibrator.workflow.runner import CalibrationRunner


def _point_co2_low_pressure() -> CalibrationPoint:
    return CalibrationPoint(
        index=1,
        temp_chamber_c=20.0,
        co2_ppm=500.0,
        hgen_temp_c=None,
        hgen_rh_pct=None,
        target_pressure_hpa=700.0,
        dewpoint_c=None,
        h2o_mmol=None,
        raw_h2o=None,
        co2_group="B",
    )


def _install_long_guard_sequence(
    runner: CalibrationRunner,
    monkeypatch,
    *,
    observations,
    ready_values,
    step_s: float,
) -> None:
    clock = {"mono": 100.0, "wall": 1000.0, "index": 0}
    monkeypatch.setattr(runner_module.time, "monotonic", lambda: clock["mono"])
    monkeypatch.setattr(runner_module.time, "time", lambda: clock["wall"])

    runner._ensure_pressure_transition_fast_signal_cache = types.MethodType(lambda self, *_a, **_k: [], runner)
    runner._pressure_transition_monitor_wait_s = types.MethodType(lambda self, point=None: 0.1, runner)
    runner._recent_fast_signal_numeric_observation = types.MethodType(
        lambda self, *_a, **_k: dict(observations[min(clock["index"], len(observations) - 1)]),
        runner,
    )
    runner._cached_ready_check_trace_values = types.MethodType(
        lambda self, context=None, point=None: dict(ready_values[min(clock["index"], len(ready_values) - 1)]),
        runner,
    )

    def _wait(self, duration_s, stop_event=None):
        advance_s = max(float(duration_s or 0.0), float(step_s))
        clock["mono"] += advance_s
        clock["wall"] += advance_s
        clock["index"] = min(clock["index"] + 1, max(len(observations), len(ready_values)) - 1)
        return True

    runner._sampling_window_wait = types.MethodType(_wait, runner)


def test_wait_co2_presample_long_guard_passes_after_full_window(monkeypatch, tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(
        {
            "workflow": {
                "pressure": {
                    "co2_presample_long_guard_enabled": True,
                    "co2_presample_long_guard_window_s": 8.0,
                    "co2_presample_long_guard_timeout_s": 20.0,
                    "co2_presample_long_guard_max_span_c": 0.15,
                    "co2_presample_long_guard_max_abs_slope_c_per_s": 0.02,
                    "co2_presample_long_guard_max_rise_c": 0.12,
                    "co2_presample_long_guard_policy": "reject",
                }
            }
        },
        {},
        logger,
        lambda *_: None,
        lambda *_: None,
    )
    point = _point_co2_low_pressure()
    runner._set_point_runtime_fields(point, phase="co2", dewpoint_gate_pass_live_c=-24.50)
    _install_long_guard_sequence(
        runner,
        monkeypatch,
        observations=[
            {"count": 4, "span": 0.18, "slope_per_s": 0.0300, "min_value": -24.50},
            {"count": 9, "span": 0.08, "slope_per_s": 0.0080, "min_value": -24.46},
        ],
        ready_values=[
            {"dewpoint_live_c": -24.47, "dew_temp_live_c": 20.0, "dew_rh_live_pct": 6.0, "pressure_gauge_hpa": 700.0},
            {"dewpoint_live_c": -24.42, "dew_temp_live_c": 20.0, "dew_rh_live_pct": 6.0, "pressure_gauge_hpa": 700.0},
        ],
        step_s=4.0,
    )

    assert runner._wait_co2_presample_long_guard(
        point,
        phase="co2",
        context={"stop_event": None},
    ) is True
    logger.close()

    state = runner._point_runtime_state(point, phase="co2")
    assert state is not None
    assert state["presample_long_guard_status"] == "pass"
    assert state["presample_long_guard_reason"] == ""
    assert state["presample_long_guard_elapsed_s"] >= 8.0
    assert state["presample_long_guard_span_c"] == 0.08
    assert state["presample_long_guard_slope_c_per_s"] == 0.008
    assert state["presample_long_guard_rise_c"] == 0.08


@pytest.mark.parametrize(
    ("policy", "expected_allowed", "expected_status"),
    [
        ("warn", True, "warn"),
        ("reject", False, "fail"),
    ],
)
def test_wait_co2_presample_long_guard_warns_or_rejects_on_timeout(
    monkeypatch,
    tmp_path: Path,
    policy: str,
    expected_allowed: bool,
    expected_status: str,
) -> None:
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(
        {
            "workflow": {
                "pressure": {
                    "co2_presample_long_guard_enabled": True,
                    "co2_presample_long_guard_window_s": 8.0,
                    "co2_presample_long_guard_timeout_s": 5.0,
                    "co2_presample_long_guard_max_span_c": 0.15,
                    "co2_presample_long_guard_max_abs_slope_c_per_s": 0.02,
                    "co2_presample_long_guard_max_rise_c": 0.12,
                    "co2_presample_long_guard_policy": policy,
                }
            }
        },
        {},
        logger,
        lambda *_: None,
        lambda *_: None,
    )
    point = _point_co2_low_pressure()
    runner._set_point_runtime_fields(point, phase="co2", dewpoint_gate_pass_live_c=-24.50)
    _install_long_guard_sequence(
        runner,
        monkeypatch,
        observations=[
            {"count": 9, "span": 0.22, "slope_per_s": 0.0300, "min_value": -24.50},
        ],
        ready_values=[
            {"dewpoint_live_c": -24.31, "dew_temp_live_c": 20.0, "dew_rh_live_pct": 6.0, "pressure_gauge_hpa": 700.0},
        ],
        step_s=10.0,
    )

    assert runner._wait_co2_presample_long_guard(
        point,
        phase="co2",
        context={"stop_event": None},
    ) is expected_allowed
    logger.close()

    state = runner._point_runtime_state(point, phase="co2")
    assert state is not None
    assert state["presample_long_guard_status"] == expected_status
    assert "timeout_elapsed_s=" in state["presample_long_guard_reason"]
    assert "rise_c=0.190>max_rise_c=0.120" in state["presample_long_guard_reason"]
    assert f"policy={policy}" in state["presample_long_guard_reason"]


def test_presample_long_guard_changes_do_not_affect_group2_source_mapping(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(
        {
            "valves": {
                "co2_path": 7,
                "co2_path_group2": 16,
                "co2_map": {"0": 1, "400": 3},
                "co2_map_group2": {"0": 21, "500": 24, "700": 25},
            }
        },
        {},
        logger,
        lambda *_: None,
        lambda *_: None,
    )
    point = _point_co2_low_pressure()

    assert runner._preferred_co2_group_for_ppm(500) == "B"
    assert runner._source_valve_for_point(point) == 24
    assert runner._co2_path_for_point(point) == 16
    logger.close()
