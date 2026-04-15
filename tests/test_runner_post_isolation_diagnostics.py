from __future__ import annotations

import csv
from pathlib import Path

import pytest

from gas_calibrator.data.points import CalibrationPoint
from gas_calibrator.logging_utils import RunLogger
from gas_calibrator.workflow import runner as runner_module
from gas_calibrator.workflow.runner import CalibrationRunner


def _co2_point(*, pressure_hpa: float = 800.0, index: int = 1) -> CalibrationPoint:
    return CalibrationPoint(
        index=index,
        temp_chamber_c=20.0,
        co2_ppm=800.0,
        hgen_temp_c=None,
        hgen_rh_pct=None,
        target_pressure_hpa=pressure_hpa,
        dewpoint_c=None,
        h2o_mmol=None,
        raw_h2o=None,
    )


def _prime_post_isolation_runtime(runner: CalibrationRunner, point: CalibrationPoint) -> None:
    runner._set_point_runtime_fields(
        point,
        phase="co2",
        handoff_mode="same_gas_pressure_step_handoff",
        capture_hold_status="pass",
        pace_output_state=0,
        pace_isolation_state=0,
        pace_vent_status=0,
        timing_stages={},
    )


def _configure_runner(tmp_path: Path, *, overrides: dict | None = None) -> tuple[RunLogger, CalibrationRunner]:
    logger = RunLogger(tmp_path)
    cfg = {
        "workflow": {
            "pressure": {
                "co2_post_isolation_diagnostic_enabled": True,
                "co2_post_isolation_window_s": 1.0,
                "co2_post_isolation_poll_s": 0.25,
                "co2_post_isolation_pressure_drift_hpa": 0.35,
                "co2_post_isolation_pressure_stable_span_hpa": 0.20,
                "co2_post_isolation_dewpoint_rise_c": 0.12,
                "co2_post_isolation_dewpoint_slope_c_per_s": 0.01,
                "co2_post_isolation_ambient_recovery_min_hpa": 0.20,
            }
        }
    }
    if overrides:
        cfg["workflow"]["pressure"].update(overrides)
    runner = CalibrationRunner(cfg, {}, logger, lambda *_: None, lambda *_: None)
    return logger, runner


def _install_post_isolation_sequences(
    monkeypatch: pytest.MonkeyPatch,
    runner: CalibrationRunner,
    *,
    pressures: list[float],
    dewpoints: list[float],
    h2o_values: list[float] | None = None,
) -> None:
    clock = {"wall": 1000.0, "mono": 500.0}
    index = {"value": 0}

    def _current_idx() -> int:
        return min(index["value"], len(pressures) - 1)

    def _cached_ready_check_trace_values(self, context=None, point=None):
        pos = _current_idx()
        return {
            "pace_pressure_hpa": pressures[pos],
            "pressure_gauge_hpa": pressures[pos],
            "dewpoint_c": dewpoints[pos],
            "dew_temp_c": 20.0,
            "dew_rh_pct": 6.0,
            "dewpoint_live_c": dewpoints[pos],
            "dew_temp_live_c": 20.0,
            "dew_rh_live_pct": 6.0,
        }

    def _latest_h2o(_key: str):
        if not h2o_values:
            return None, "unavailable"
        pos = _current_idx()
        return float(h2o_values[pos]), "analyzer"

    monkeypatch.setattr(runner_module.time, "time", lambda: clock["wall"])
    monkeypatch.setattr(runner_module.time, "monotonic", lambda: clock["mono"])
    runner._ensure_pressure_transition_fast_signal_cache = lambda *_args, **_kwargs: []
    runner._cached_ready_check_trace_values = _cached_ready_check_trace_values.__get__(runner, CalibrationRunner)
    runner._latest_fresh_analyzer_numeric_value = _latest_h2o

    def _wait(duration_s: float, stop_event=None):
        clock["wall"] += max(0.25, float(duration_s))
        clock["mono"] += max(0.25, float(duration_s))
        index["value"] += 1
        return True

    runner._sampling_window_wait = _wait


def _load_timing_rows(logger: RunLogger) -> list[dict[str, str]]:
    path = logger.run_dir / "point_timing_summary.csv"
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def test_post_isolation_diagnostic_flags_ambient_recovery_after_hard_isolation(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    logger, runner = _configure_runner(tmp_path)
    point = _co2_point()
    _prime_post_isolation_runtime(runner, point)
    runner._atmosphere_reference_hpa = 1006.0
    _install_post_isolation_sequences(
        monkeypatch,
        runner,
        pressures=[800.00, 800.14, 800.31, 800.47, 800.48],
        dewpoints=[-32.00, -31.95, -31.88, -31.81, -31.80],
        h2o_values=[0.40, 0.41, 0.43, 0.45, 0.45],
    )

    assert runner._wait_post_isolation_leak_test(
        point,
        phase="co2",
        context={"stop_event": None},
        handoff_mode="same_gas_pressure_step_handoff",
    ) is False
    runner._write_point_timing_summary(point, phase="co2")
    logger.close()

    state = runner._point_runtime_state(point, phase="co2") or {}
    assert state["post_isolation_diagnosis"] == "post_isolation_ambient_ingress_suspect"
    assert state["root_cause_reject_reason"] == "post_isolation_ambient_ingress_suspect"
    assert state["post_isolation_pressure_recovery_toward_ambient"] is True
    assert state["post_isolation_pressure_drift_hpa"] == pytest.approx(0.48, rel=1e-3)
    timing_rows = _load_timing_rows(logger)
    assert len(timing_rows) == 1
    assert timing_rows[0]["post_isolation_test_begin_ts"] != ""
    assert timing_rows[0]["post_isolation_test_end_ts"] != ""
    assert timing_rows[0]["post_isolation_diagnosis"] == "post_isolation_ambient_ingress_suspect"


def test_post_isolation_diagnostic_flags_dead_volume_wet_release_when_pressure_stays_stable(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    logger, runner = _configure_runner(tmp_path)
    point = _co2_point()
    _prime_post_isolation_runtime(runner, point)
    runner._atmosphere_reference_hpa = 1006.0
    _install_post_isolation_sequences(
        monkeypatch,
        runner,
        pressures=[800.00, 800.03, 800.01, 800.02, 800.02],
        dewpoints=[-32.00, -31.92, -31.83, -31.74, -31.72],
        h2o_values=[0.40, 0.44, 0.48, 0.52, 0.54],
    )

    assert runner._wait_post_isolation_leak_test(
        point,
        phase="co2",
        context={"stop_event": None},
        handoff_mode="same_gas_pressure_step_handoff",
    ) is False
    logger.close()

    state = runner._point_runtime_state(point, phase="co2") or {}
    assert state["post_isolation_diagnosis"] == "dead_volume_wet_release_suspect"
    assert state["root_cause_reject_reason"] == "dead_volume_wet_release_suspect"
    assert state["post_isolation_pressure_recovery_toward_ambient"] is False
    assert state["post_isolation_dewpoint_rise_c"] == pytest.approx(0.28, rel=1e-3)


def test_presample_failure_root_cause_only_allows_controller_hunting_when_controller_active(tmp_path: Path) -> None:
    logger, runner = _configure_runner(tmp_path)
    point = _co2_point()
    _prime_post_isolation_runtime(runner, point)
    runner._set_point_runtime_fields(
        point,
        phase="co2",
        post_isolation_diagnosis="sealed_path_leak_suspect",
        pace_output_state=0,
        pace_isolation_state=0,
        pressure_gate_status="fail",
        pressure_dew_sync_status="synchronous",
    )

    assert (
        runner._presample_failure_root_cause(point, phase="co2", failure_stage="pressure_gate")
        == "sealed_path_leak_suspect"
    )

    runner._set_point_runtime_fields(
        point,
        phase="co2",
        post_isolation_diagnosis="",
        pace_output_state=1,
        pace_isolation_state=0,
        pressure_gate_status="fail",
        pressure_dew_sync_status="synchronous",
    )

    assert (
        runner._presample_failure_root_cause(point, phase="co2", failure_stage="pressure_gate")
        == "controller_hunting_suspect"
    )
    logger.close()


def test_post_isolation_diagnostic_flags_pace_vent_in_progress(tmp_path: Path) -> None:
    logger, runner = _configure_runner(tmp_path)
    point = _co2_point()
    _prime_post_isolation_runtime(runner, point)
    runner._set_point_runtime_fields(
        point,
        phase="co2",
        post_isolation_pressure_drift_hpa=0.55,
        post_isolation_dewpoint_rise_c=0.22,
        post_isolation_dewpoint_slope_c_s=0.03,
        post_isolation_pressure_peak_hpa=800.55,
        post_isolation_pressure_min_hpa=800.00,
        post_isolation_pressure_recovery_toward_ambient=True,
        pace_outp_state_query=0,
        pace_isol_state_query=0,
        pace_vent_status_query=1,
        pace_vent_after_valve_state_query="OPEN",
        pace_vent_popup_state_query="DISABLED",
    )

    diagnosis = runner._diagnose_post_isolation_result(
        point,
        phase="co2",
        cfg=runner._post_isolation_leak_test_cfg(point),
    )

    assert diagnosis == "pace_vent_in_progress_suspect"
    logger.close()


def test_post_isolation_diagnostic_flags_pace_vent_valve_left_open(tmp_path: Path) -> None:
    logger, runner = _configure_runner(tmp_path)
    point = _co2_point()
    _prime_post_isolation_runtime(runner, point)
    runner._set_point_runtime_fields(
        point,
        phase="co2",
        post_isolation_pressure_drift_hpa=0.48,
        post_isolation_dewpoint_rise_c=0.20,
        post_isolation_dewpoint_slope_c_s=0.02,
        post_isolation_pressure_peak_hpa=800.48,
        post_isolation_pressure_min_hpa=800.00,
        post_isolation_pressure_recovery_toward_ambient=True,
        pace_outp_state_query=0,
        pace_isol_state_query=0,
        pace_vent_status_query=0,
        pace_vent_after_valve_state_query="OPEN",
        pace_vent_popup_state_query="DISABLED",
    )

    diagnosis = runner._diagnose_post_isolation_result(
        point,
        phase="co2",
        cfg=runner._post_isolation_leak_test_cfg(point),
    )

    assert diagnosis == "pace_vent_valve_left_open_suspect"
    logger.close()


def test_post_isolation_diagnostic_marks_popup_only_without_rebound(tmp_path: Path) -> None:
    logger, runner = _configure_runner(tmp_path)
    point = _co2_point()
    _prime_post_isolation_runtime(runner, point)
    runner._set_point_runtime_fields(
        point,
        phase="co2",
        post_isolation_pressure_drift_hpa=0.03,
        post_isolation_dewpoint_rise_c=0.02,
        post_isolation_dewpoint_slope_c_s=0.001,
        post_isolation_pressure_peak_hpa=800.03,
        post_isolation_pressure_min_hpa=800.00,
        post_isolation_pressure_recovery_toward_ambient=False,
        pace_outp_state_query=0,
        pace_isol_state_query=0,
        pace_vent_status_query=0,
        pace_vent_after_valve_state_query="CLOSED",
        pace_vent_popup_state_query="ENABLED",
    )

    diagnosis = runner._diagnose_post_isolation_result(
        point,
        phase="co2",
        cfg=runner._post_isolation_leak_test_cfg(point),
    )

    assert diagnosis == "pace_vent_popup_only"
    logger.close()


def test_post_isolation_diagnostic_flags_isolation_query_mismatch(tmp_path: Path) -> None:
    logger, runner = _configure_runner(tmp_path)
    point = _co2_point()
    _prime_post_isolation_runtime(runner, point)
    runner._set_point_runtime_fields(
        point,
        phase="co2",
        post_isolation_pressure_drift_hpa=0.12,
        post_isolation_dewpoint_rise_c=0.05,
        post_isolation_pressure_peak_hpa=800.12,
        post_isolation_pressure_min_hpa=800.00,
        post_isolation_pressure_recovery_toward_ambient=False,
        pace_output_state=0,
        pace_isolation_state=0,
        pace_outp_state_query=0,
        pace_isol_state_query=1,
        pace_vent_status_query=0,
        pace_vent_after_valve_state_query="CLOSED",
        pace_vent_popup_state_query="DISABLED",
    )

    diagnosis = runner._diagnose_post_isolation_result(
        point,
        phase="co2",
        cfg=runner._post_isolation_leak_test_cfg(point),
    )

    assert diagnosis == "pace_isolation_state_mismatch_suspect"
    logger.close()


def test_post_isolation_popup_only_is_informational_diagnosis(tmp_path: Path) -> None:
    logger, runner = _configure_runner(tmp_path)
    point = _co2_point()
    _prime_post_isolation_runtime(runner, point)
    runner._set_point_runtime_fields(
        point,
        phase="co2",
        post_isolation_pressure_drift_hpa=0.03,
        post_isolation_dewpoint_rise_c=0.02,
        post_isolation_dewpoint_slope_c_s=0.001,
        post_isolation_pressure_peak_hpa=800.03,
        post_isolation_pressure_min_hpa=800.00,
        post_isolation_pressure_recovery_toward_ambient=False,
        pace_output_state=0,
        pace_isolation_state=0,
        pace_outp_state_query=0,
        pace_isol_state_query=0,
        pace_vent_status_query=0,
        pace_vent_after_valve_state_query="CLOSED",
        pace_vent_popup_state_query="ENABLED",
    )

    diagnosis = runner._diagnose_post_isolation_result(
        point,
        phase="co2",
        cfg=runner._post_isolation_leak_test_cfg(point),
    )

    assert diagnosis == "pace_vent_popup_only"
    logger.close()
