from __future__ import annotations

import types
from pathlib import Path

import pytest

from gas_calibrator.data.points import CalibrationPoint
from gas_calibrator.logging_utils import RunLogger
from gas_calibrator.validation.dewpoint_flush_gate import predict_pressure_scaled_dewpoint_c
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


def test_wait_co2_preseal_baseline_sanity_gate_rejects_stable_wrong_plateau_under_override(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    runner = CalibrationRunner(
        {
            "workflow": {
                "stability": {
                    "sensor": {
                        "baseline_sanity_gate": {
                            "enabled": True,
                            "policy": "reject",
                            "same_gas_only": True,
                            "target_co2_ppm": 800.0,
                            "target_co2_tolerance_ppm": 120.0,
                            "pressure_min_hpa": 980.0,
                            "pressure_max_hpa": 1020.0,
                            "plateau_sample_count": 4,
                            "plateau_read_interval_s": 0.0,
                            "max_plateau_span_ppm": 25.0,
                        }
                    }
                }
            }
        },
        {"gas_analyzer": object()},
        logger,
        lambda *_: None,
        lambda *_: None,
    )
    point = CalibrationPoint(
        index=3,
        temp_chamber_c=20.0,
        co2_ppm=800.0,
        hgen_temp_c=None,
        hgen_rh_pct=None,
        target_pressure_hpa=1000.0,
        dewpoint_c=None,
        h2o_mmol=None,
        raw_h2o=None,
        co2_group="A",
    )
    readings = iter([1514.8, 1515.2, 1515.5, 1515.1])

    runner._active_gas_analyzers = types.MethodType(
        lambda self: [("ga01", object(), {})],
        runner,
    )
    runner._resolve_sensor_frame_acceptance_mode = types.MethodType(
        lambda self, *_args, **_kwargs: "usable_only",
        runner,
    )
    runner._read_sensor_parsed = types.MethodType(
        lambda self, *_args, **_kwargs: ("", {"co2_ppm": next(readings)}),
        runner,
    )
    runner._sampling_window_wait = types.MethodType(lambda self, *_args, **_kwargs: True, runner)

    assert runner._wait_co2_preseal_baseline_sanity_gate(point) is False
    logger.close()

    state = runner._point_runtime_state(point, phase="co2")
    assert state is not None
    assert state["baseline_sanity_gate_status"] == "fail"
    assert state["baseline_sanity_target_co2_ppm"] == 800.0
    assert round(float(state["baseline_sanity_plateau_mean_ppm"]), 3) == 1515.15
    assert state["baseline_sanity_plateau_count"] == 4
    assert state["root_cause_reject_reason"] == "baseline_precondition_wrong_plateau_suspect"
