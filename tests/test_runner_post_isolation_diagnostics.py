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
                "capture_then_hold_enabled": True,
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


def test_post_isolation_diagnostic_skips_when_capture_then_hold_disabled(tmp_path: Path) -> None:
    logger, runner = _configure_runner(
        tmp_path,
        overrides={
            "capture_then_hold_enabled": False,
            "co2_post_isolation_diagnostic_enabled": True,
        },
    )
    point = _co2_point()

    cfg = runner._post_isolation_leak_test_cfg(point)
    assert cfg["enabled"] is False

    assert runner._wait_post_isolation_leak_test(
        point,
        phase="co2",
        context={},
        handoff_mode="same_gas_pressure_step_handoff",
    ) is True
    logger.close()

    runtime_state = runner._point_runtime_state(point, phase="co2") or {}
    assert runtime_state["post_isolation_status"] == "skipped"


def _install_post_isolation_sequences(
    monkeypatch: pytest.MonkeyPatch,
    runner: CalibrationRunner,
    *,
    pressures: list[float],
    dewpoints: list[float],
    h2o_values: list[float] | None = None,
    controller_pressures: list[float] | None = None,
) -> None:
    clock = {"wall": 1000.0, "mono": 500.0}
    index = {"value": 0}
    controller_pressures = list(controller_pressures or pressures)

    def _current_idx() -> int:
        return min(index["value"], len(pressures) - 1)

    def _cached_ready_check_trace_values(self, context=None, point=None):
        pos = _current_idx()
        return {
            "pace_pressure_hpa": controller_pressures[pos],
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

    runner._set_point_runtime_fields(
        point,
        phase="co2",
        post_isolation_diagnosis="",
        pace_output_state=0,
        pace_isolation_state=0,
        pace_outp_state_query=0,
        pace_isol_state_query=0,
        pace_vent_status_query=0,
        pace_effort_query=0.03,
        pace_comp1_query=0.0,
        pace_comp2_query=0.0,
        pressure_gate_status="fail",
        pressure_dew_sync_status="synchronous",
    )

    assert (
        runner._presample_failure_root_cause(point, phase="co2", failure_stage="pressure_gate")
        == "pace_effort_nonzero_after_output_off_suspect"
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


def test_post_isolation_diagnostic_flags_pace_vent_completed_latched(tmp_path: Path) -> None:
    logger, runner = _configure_runner(tmp_path)
    point = _co2_point()
    _prime_post_isolation_runtime(runner, point)
    runner._set_point_runtime_fields(
        point,
        phase="co2",
        post_isolation_pressure_drift_hpa=0.03,
        post_isolation_dewpoint_rise_c=0.01,
        post_isolation_pressure_peak_hpa=800.03,
        post_isolation_pressure_min_hpa=800.00,
        post_isolation_pressure_recovery_toward_ambient=False,
        pace_outp_state_query=0,
        pace_isol_state_query=0,
        pace_vent_status_query=2,
        pace_oper_pres_cond_query=1,
        pace_oper_pres_even_query=1,
        pace_vent_after_valve_state_query="CLOSED",
        pace_vent_popup_state_query="DISABLED",
    )

    diagnosis = runner._diagnose_post_isolation_result(
        point,
        phase="co2",
        cfg=runner._post_isolation_leak_test_cfg(point),
    )

    assert diagnosis == "pace_vent_completed_latched_suspect"
    logger.close()


def test_post_isolation_diagnostic_flags_effort_nonzero_after_output_off(tmp_path: Path) -> None:
    logger, runner = _configure_runner(tmp_path)
    point = _co2_point()
    _prime_post_isolation_runtime(runner, point)
    runner._set_point_runtime_fields(
        point,
        phase="co2",
        post_isolation_pressure_drift_hpa=0.03,
        post_isolation_dewpoint_rise_c=0.01,
        post_isolation_pressure_peak_hpa=800.03,
        post_isolation_pressure_min_hpa=800.00,
        post_isolation_pressure_recovery_toward_ambient=False,
        pace_outp_state_query=0,
        pace_isol_state_query=0,
        pace_vent_status_query=0,
        pace_effort_query=0.03,
        pace_comp1_query=0.001,
        pace_comp2_query=0.0,
        pace_vent_after_valve_state_query="CLOSED",
        pace_vent_popup_state_query="DISABLED",
    )

    diagnosis = runner._diagnose_post_isolation_result(
        point,
        phase="co2",
        cfg=runner._post_isolation_leak_test_cfg(point),
    )

    assert diagnosis == "pace_effort_nonzero_after_output_off_suspect"
    logger.close()


def test_post_isolation_diagnostic_flags_supply_vacuum_compensation(tmp_path: Path) -> None:
    logger, runner = _configure_runner(tmp_path)
    point = _co2_point()
    _prime_post_isolation_runtime(runner, point)
    runner._set_point_runtime_fields(
        point,
        phase="co2",
        post_isolation_pressure_drift_hpa=0.04,
        post_isolation_dewpoint_rise_c=0.01,
        post_isolation_pressure_peak_hpa=800.04,
        post_isolation_pressure_min_hpa=800.00,
        post_isolation_pressure_recovery_toward_ambient=False,
        pace_outp_state_query=0,
        pace_isol_state_query=0,
        pace_vent_status_query=0,
        pace_effort_query=0.03,
        pace_comp1_query=0.15,
        pace_comp2_query=0.0,
        pace_vent_after_valve_state_query="CLOSED",
        pace_vent_popup_state_query="DISABLED",
    )

    diagnosis = runner._diagnose_post_isolation_result(
        point,
        phase="co2",
        cfg=runner._post_isolation_leak_test_cfg(point),
    )

    assert diagnosis == "pace_supply_vacuum_compensation_suspect"
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


def test_post_isolation_diagnostic_flags_aft_open_only_as_config_suspect_without_rebound(tmp_path: Path) -> None:
    logger, runner = _configure_runner(tmp_path)
    point = _co2_point()
    _prime_post_isolation_runtime(runner, point)
    runner._set_point_runtime_fields(
        point,
        phase="co2",
        post_isolation_pressure_drift_hpa=0.03,
        post_isolation_dewpoint_rise_c=0.01,
        post_isolation_dewpoint_slope_c_s=0.001,
        post_isolation_pressure_peak_hpa=800.03,
        post_isolation_pressure_min_hpa=800.00,
        post_isolation_pressure_recovery_toward_ambient=False,
        pace_outp_state_query=0,
        pace_isol_state_query=0,
        pace_vent_status_query=0,
        pace_vent_after_valve_state_query="OPEN",
        pace_vent_popup_state_query="DISABLED",
        pace_vent_orpv_state_query="DISABLED",
        pace_vent_pupv_state_query="DISABLED",
    )

    diagnosis = runner._diagnose_post_isolation_result(
        point,
        phase="co2",
        cfg=runner._post_isolation_leak_test_cfg(point),
    )

    assert diagnosis == "pace_vent_after_valve_config_open_suspect"
    logger.close()


def test_post_isolation_diagnostic_flags_protective_vent_when_enabled_and_abnormal(tmp_path: Path) -> None:
    logger, runner = _configure_runner(tmp_path)
    point = _co2_point()
    _prime_post_isolation_runtime(runner, point)
    runner._set_point_runtime_fields(
        point,
        phase="co2",
        post_isolation_pressure_drift_hpa=0.32,
        post_isolation_dewpoint_rise_c=0.16,
        post_isolation_dewpoint_slope_c_s=0.015,
        post_isolation_pressure_peak_hpa=800.32,
        post_isolation_pressure_min_hpa=800.00,
        post_isolation_pressure_recovery_toward_ambient=True,
        pace_outp_state_query=0,
        pace_isol_state_query=0,
        pace_vent_status_query=0,
        pace_vent_after_valve_state_query="CLOSED",
        pace_vent_popup_state_query="DISABLED",
        pace_vent_orpv_state_query="ENABLED",
        pace_vent_pupv_state_query="DISABLED",
    )

    diagnosis = runner._diagnose_post_isolation_result(
        point,
        phase="co2",
        cfg=runner._post_isolation_leak_test_cfg(point),
    )

    assert diagnosis == "pace_protective_vent_suspect"
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


def test_post_isolation_diagnostic_marks_popup_stale_when_only_elapsed_time_remains(tmp_path: Path) -> None:
    logger, runner = _configure_runner(tmp_path)
    point = _co2_point()
    _prime_post_isolation_runtime(runner, point)
    runner._set_point_runtime_fields(
        point,
        phase="co2",
        post_isolation_pressure_drift_hpa=0.02,
        post_isolation_dewpoint_rise_c=0.01,
        post_isolation_dewpoint_slope_c_s=0.001,
        post_isolation_pressure_peak_hpa=800.02,
        post_isolation_pressure_min_hpa=800.00,
        post_isolation_pressure_recovery_toward_ambient=False,
        pace_outp_state_query=0,
        pace_isol_state_query=0,
        pace_vent_status_query=0,
        pace_vent_after_valve_state_query="CLOSED",
        pace_vent_popup_state_query="ENABLED",
        pace_vent_elapsed_time_query=8.0,
        pace_vent_orpv_state_query="DISABLED",
        pace_vent_pupv_state_query="DISABLED",
    )

    diagnosis = runner._diagnose_post_isolation_result(
        point,
        phase="co2",
        cfg=runner._post_isolation_leak_test_cfg(point),
    )

    assert diagnosis == "pace_vent_popup_stale_suspect"
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


def test_post_isolation_fast_capture_failure_falls_back_to_extended_diagnostic(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    logger, runner = _configure_runner(
        tmp_path,
        overrides={
            "co2_post_isolation_window_s": 1.0,
            "co2_post_isolation_poll_s": 0.25,
            "post_isolation_fast_capture_enabled": True,
            "post_isolation_fast_capture_allow_early_sample": True,
            "post_isolation_fast_capture_min_s": 1.0,
            "post_isolation_fast_capture_fallback_to_extended_diag": True,
            "post_isolation_extended_diag_window_s": 2.0,
        },
    )
    point = _co2_point()
    _prime_post_isolation_runtime(runner, point)
    runner._atmosphere_reference_hpa = 1006.0
    _install_post_isolation_sequences(
        monkeypatch,
        runner,
        pressures=[800.00, 800.03, 800.04, 800.05, 800.06, 800.06],
        dewpoints=[-32.00, -31.99, -31.98, -31.98, -31.97, -31.97],
        h2o_values=[0.40, 0.40, 0.41, 0.41, 0.41, 0.41],
    )
    runner._pace_diagnostic_state_snapshot = lambda *args, **kwargs: {
        "pace_output_state": 0,
        "pace_isolation_state": 0,
        "pace_vent_status": 0,
        "pace_outp_state_query": 0,
        "pace_isol_state_query": 0,
        "pace_mode_query": "ACT",
        "pace_vent_status_query": 0,
        "pace_vent_completed_latched": False,
        "pace_vent_clear_attempted": False,
        "pace_vent_clear_result": "not_needed",
        "pace_vent_after_valve_state_query": "CLOSED",
        "pace_vent_popup_state_query": "DISABLED",
        "pace_vent_elapsed_time_query": 4.0,
        "pace_vent_orpv_state_query": "DISABLED",
        "pace_vent_pupv_state_query": "DISABLED",
        "pace_effort_query": 0.03,
        "pace_comp1_query": 0.12,
        "pace_comp2_query": 0.0,
        "pace_sens_pres_cont_query": 800.0,
        "pace_sens_pres_bar_query": 1006.0,
        "pace_sens_pres_inl_query": 800.0,
        "pace_sens_pres_inl_state_query": 1,
        "pace_sens_pres_inl_time_query": 5.0,
        "pace_sens_slew_query": 0.0,
        "pace_oper_cond_query": 1,
        "pace_oper_pres_cond_query": 2,
        "pace_oper_pres_even_query": 4,
        "pace_oper_pres_vent_complete_bit": False,
        "pace_oper_pres_in_limits_bit": True,
    }

    assert runner._wait_post_isolation_leak_test(
        point,
        phase="co2",
        context={"stop_event": None},
        handoff_mode="same_gas_pressure_step_handoff",
    ) is False
    state = runner._point_runtime_state(point, phase="co2") or {}
    assert state["post_isolation_fast_capture_status"] in {"fallback", "fallback_complete"}
    assert state["post_isolation_fast_capture_fallback"] is True
    assert state["post_isolation_capture_mode"] == "extended20s"
    assert state["post_isolation_fast_capture_reason"] == "effort_not_zero_after_output_off"
    assert state["post_isolation_diagnosis"] == "pace_supply_vacuum_compensation_suspect"
    logger.close()


def test_post_isolation_fast_capture_falls_back_when_vent_not_zero(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    logger, runner = _configure_runner(
        tmp_path,
        overrides={
            "co2_post_isolation_window_s": 1.0,
            "co2_post_isolation_poll_s": 0.25,
            "post_isolation_fast_capture_enabled": True,
            "post_isolation_fast_capture_allow_early_sample": True,
            "post_isolation_fast_capture_min_s": 1.0,
            "post_isolation_fast_capture_fallback_to_extended_diag": True,
            "post_isolation_extended_diag_window_s": 2.0,
        },
    )
    point = _co2_point()
    _prime_post_isolation_runtime(runner, point)
    runner._atmosphere_reference_hpa = 1006.0
    _install_post_isolation_sequences(
        monkeypatch,
        runner,
        pressures=[800.00, 800.02, 800.03, 800.03, 800.03],
        dewpoints=[-32.00, -31.99, -31.99, -31.98, -31.98],
        h2o_values=[0.40, 0.40, 0.40, 0.40, 0.40],
    )
    runner._pace_diagnostic_state_snapshot = lambda *args, **kwargs: {
        "pace_output_state": 0,
        "pace_isolation_state": 0,
        "pace_vent_status": 2,
        "pace_outp_state_query": 0,
        "pace_isol_state_query": 0,
        "pace_mode_query": "ACT",
        "pace_vent_status_query": 2,
        "pace_vent_completed_latched": True,
        "pace_vent_clear_attempted": False,
        "pace_vent_clear_result": "not_needed",
        "pace_vent_after_valve_state_query": "CLOSED",
        "pace_vent_popup_state_query": "DISABLED",
        "pace_vent_elapsed_time_query": 4.0,
        "pace_vent_orpv_state_query": "DISABLED",
        "pace_vent_pupv_state_query": "DISABLED",
        "pace_effort_query": 0.0,
        "pace_comp1_query": 0.0,
        "pace_comp2_query": 0.0,
        "pace_sens_pres_cont_query": 800.0,
        "pace_sens_pres_bar_query": 1006.0,
        "pace_sens_pres_inl_query": 800.0,
        "pace_sens_pres_inl_state_query": 1,
        "pace_sens_pres_inl_time_query": 5.0,
        "pace_sens_slew_query": 0.0,
        "pace_oper_cond_query": 1,
        "pace_oper_pres_cond_query": 1,
        "pace_oper_pres_even_query": 1,
        "pace_oper_pres_vent_complete_bit": True,
        "pace_oper_pres_in_limits_bit": True,
    }

    assert runner._wait_post_isolation_leak_test(
        point,
        phase="co2",
        context={"stop_event": None},
        handoff_mode="same_gas_pressure_step_handoff",
    ) is False
    state = runner._point_runtime_state(point, phase="co2") or {}
    assert state["post_isolation_fast_capture_reason"] == "vent_not_zero"
    assert state["post_isolation_capture_mode"] == "extended20s"
    assert state["post_isolation_diagnosis"] == "pace_vent_completed_latched_suspect"
    logger.close()


def test_post_isolation_fast_capture_falls_back_for_legacy_trapped_pressure_and_stays_blocked(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    logger, runner = _configure_runner(
        tmp_path,
        overrides={
            "co2_post_isolation_window_s": 1.0,
            "co2_post_isolation_poll_s": 0.25,
            "post_isolation_fast_capture_enabled": True,
            "post_isolation_fast_capture_allow_early_sample": True,
            "post_isolation_fast_capture_min_s": 1.0,
            "post_isolation_fast_capture_fallback_to_extended_diag": True,
            "post_isolation_extended_diag_window_s": 2.0,
        },
    )
    point = _co2_point()
    _prime_post_isolation_runtime(runner, point)
    runner._atmosphere_reference_hpa = 1006.0
    _install_post_isolation_sequences(
        monkeypatch,
        runner,
        pressures=[800.00, 800.01, 800.01, 800.01, 800.01],
        dewpoints=[-32.00, -32.00, -32.00, -32.00, -32.00],
        h2o_values=[0.40, 0.40, 0.40, 0.40, 0.40],
    )
    runner._pace_diagnostic_state_snapshot = lambda *args, **kwargs: {
        "pace_output_state": 0,
        "pace_isolation_state": 0,
        "pace_vent_status": 3,
        "pace_outp_state_query": 0,
        "pace_isol_state_query": 0,
        "pace_mode_query": "ACT",
        "pace_vent_status_query": 3,
        "pace_vent_completed_latched": False,
        "pace_vent_clear_attempted": False,
        "pace_vent_clear_result": "not_needed(status=3)",
        "pace_vent_after_valve_state_query": "CLOSED",
        "pace_vent_popup_state_query": "DISABLED",
        "pace_vent_elapsed_time_query": 4.0,
        "pace_vent_orpv_state_query": "DISABLED",
        "pace_vent_pupv_state_query": "DISABLED",
        "pace_effort_query": 0.0,
        "pace_comp1_query": 0.0,
        "pace_comp2_query": 0.0,
        "pace_sens_pres_cont_query": 800.0,
        "pace_sens_pres_bar_query": 1006.0,
        "pace_sens_pres_inl_query": 800.0,
        "pace_sens_pres_inl_state_query": 1,
        "pace_sens_pres_inl_time_query": 5.0,
        "pace_sens_slew_query": 0.0,
        "pace_oper_cond_query": 1,
        "pace_oper_pres_cond_query": 4,
        "pace_oper_pres_even_query": 4,
        "pace_oper_pres_vent_complete_bit": False,
        "pace_oper_pres_in_limits_bit": True,
    }

    assert runner._wait_post_isolation_leak_test(
        point,
        phase="co2",
        context={"stop_event": None},
        handoff_mode="same_gas_pressure_step_handoff",
    ) is False
    state = runner._point_runtime_state(point, phase="co2") or {}
    assert state["post_isolation_fast_capture_reason"] == "vent_not_zero"
    assert state["post_isolation_capture_mode"] == "extended20s"
    assert state["post_isolation_diagnosis"] == "pace_atmosphere_connected_latched_state_suspect"
    assert state["pace_atmosphere_connected_latched_state_suspect"] is True
    assert state["vent3_control_ready_prevented"] is True
    logger.close()


def test_checkvalve_fast_capture_uses_external_gauge_truth_but_still_blocks_vent3(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    logger, runner = _configure_runner(
        tmp_path,
        overrides={
            "co2_post_isolation_window_s": 10.0,
            "co2_post_isolation_poll_s": 1.0,
            "post_isolation_fast_capture_enabled": True,
            "post_isolation_fast_capture_allow_early_sample": True,
            "fast_smoke_capture_min_s": 5.0,
            "fast_smoke_capture_no_extended_fallback": True,
            "pace_upstream_check_valve_installed": True,
            "post_isolation_pressure_truth_source": "external_gauge",
        },
    )
    point = _co2_point()
    _prime_post_isolation_runtime(runner, point)
    runner._atmosphere_reference_hpa = 1006.0
    _install_post_isolation_sequences(
        monkeypatch,
        runner,
        pressures=[800.00, 800.01, 800.01, 800.01, 800.01, 800.01],
        controller_pressures=[799.60, 799.58, 799.58, 799.58, 799.58, 799.58],
        dewpoints=[-32.00, -32.00, -32.00, -32.00, -32.00, -32.00],
        h2o_values=[0.40, 0.40, 0.40, 0.40, 0.40, 0.40],
    )
    runner._pace_diagnostic_state_snapshot = lambda *args, **kwargs: {
        "pace_output_state": 0,
        "pace_isolation_state": 0,
        "pace_vent_status": 3,
        "pace_outp_state_query": 0,
        "pace_isol_state_query": 0,
        "pace_mode_query": "ACT",
        "pace_vent_status_query": 3,
        "pace_vent_completed_latched": False,
        "pace_vent_clear_attempted": False,
        "pace_vent_clear_result": "not_needed(status=3)",
        "pace_vent_after_valve_state_query": "CLOSED",
        "pace_vent_popup_state_query": "DISABLED",
        "pace_vent_elapsed_time_query": 4.0,
        "pace_vent_orpv_state_query": "DISABLED",
        "pace_vent_pupv_state_query": "DISABLED",
        "pace_effort_query": 0.0,
        "pace_comp1_query": 0.0,
        "pace_comp2_query": 0.0,
        "pace_sens_pres_cont_query": 799.58,
        "pace_sens_pres_bar_query": 1006.0,
        "pace_sens_pres_inl_query": 799.58,
        "pace_sens_pres_inl_state_query": 1,
        "pace_sens_pres_inl_time_query": 6.0,
        "pace_sens_slew_query": 0.0,
        "pace_oper_cond_query": 1,
        "pace_oper_pres_cond_query": 4,
        "pace_oper_pres_even_query": 4,
        "pace_oper_pres_vent_complete_bit": False,
        "pace_oper_pres_in_limits_bit": True,
    }

    assert runner._wait_post_isolation_leak_test(
        point,
        phase="co2",
        context={"stop_event": None},
        handoff_mode="same_gas_pressure_step_handoff",
    ) is False
    state = runner._point_runtime_state(point, phase="co2") or {}
    assert state["pace_upstream_check_valve_installed"] is True
    assert state["post_isolation_pressure_truth_source"] == "external_gauge"
    assert state["post_isolation_capture_mode"] == "fast5s"
    assert state["post_isolation_fast_capture_status"] == "fail"
    assert state["post_isolation_fast_capture_reason"] == "vent_not_zero"
    assert state["post_isolation_fast_capture_elapsed_s"] == pytest.approx(5.0, rel=1e-3)
    assert state["post_isolation_fast_capture_fallback"] is False
    assert state["post_isolation_pressure_start_hpa"] == pytest.approx(800.0, rel=1e-3)
    assert state["post_isolation_diagnosis"] == "pace_atmosphere_connected_latched_state_suspect"
    assert state["pace_atmosphere_connected_latched_state_suspect"] is True
    logger.close()


def test_checkvalve_fast_capture_does_not_fallback_to_extended_diag_on_external_pressure_drift(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    logger, runner = _configure_runner(
        tmp_path,
        overrides={
            "co2_post_isolation_window_s": 10.0,
            "co2_post_isolation_poll_s": 1.0,
            "post_isolation_fast_capture_enabled": True,
            "post_isolation_fast_capture_allow_early_sample": True,
            "fast_smoke_capture_min_s": 5.0,
            "fast_smoke_capture_no_extended_fallback": True,
            "fast_smoke_pressure_drift_max_hpa": 0.05,
            "pace_upstream_check_valve_installed": True,
            "post_isolation_pressure_truth_source": "external_gauge",
        },
    )
    point = _co2_point()
    _prime_post_isolation_runtime(runner, point)
    runner._atmosphere_reference_hpa = 1006.0
    _install_post_isolation_sequences(
        monkeypatch,
        runner,
        pressures=[800.00, 800.05, 800.10, 800.15, 800.20, 800.25],
        controller_pressures=[799.60, 799.60, 799.60, 799.60, 799.60, 799.60],
        dewpoints=[-32.00, -32.00, -32.00, -32.00, -32.00, -32.00],
        h2o_values=[0.40, 0.40, 0.40, 0.40, 0.40, 0.40],
    )
    runner._pace_diagnostic_state_snapshot = lambda *args, **kwargs: {
        "pace_output_state": 0,
        "pace_isolation_state": 0,
        "pace_vent_status": 3,
        "pace_outp_state_query": 0,
        "pace_isol_state_query": 0,
        "pace_mode_query": "ACT",
        "pace_vent_status_query": 3,
        "pace_vent_completed_latched": False,
        "pace_vent_clear_attempted": False,
        "pace_vent_clear_result": "not_needed(status=3)",
        "pace_vent_after_valve_state_query": "CLOSED",
        "pace_vent_popup_state_query": "DISABLED",
        "pace_vent_elapsed_time_query": 4.0,
        "pace_vent_orpv_state_query": "DISABLED",
        "pace_vent_pupv_state_query": "DISABLED",
        "pace_effort_query": 0.0,
        "pace_comp1_query": 0.0,
        "pace_comp2_query": 0.0,
        "pace_sens_pres_cont_query": 799.60,
        "pace_sens_pres_bar_query": 1006.0,
        "pace_sens_pres_inl_query": 799.60,
        "pace_sens_pres_inl_state_query": 1,
        "pace_sens_pres_inl_time_query": 6.0,
        "pace_sens_slew_query": 0.0,
        "pace_oper_cond_query": 1,
        "pace_oper_pres_cond_query": 4,
        "pace_oper_pres_even_query": 4,
        "pace_oper_pres_vent_complete_bit": False,
        "pace_oper_pres_in_limits_bit": True,
    }

    assert runner._wait_post_isolation_leak_test(
        point,
        phase="co2",
        context={"stop_event": None},
        handoff_mode="same_gas_pressure_step_handoff",
    ) is False
    state = runner._point_runtime_state(point, phase="co2") or {}
    assert state["post_isolation_capture_mode"] == "fast5s"
    assert state["post_isolation_fast_capture_status"] == "fail"
    assert state["post_isolation_fast_capture_reason"] == "vent_not_zero"
    assert state["post_isolation_fast_capture_fallback"] is False
    assert state["post_isolation_diagnosis"] == "pace_atmosphere_connected_latched_state_suspect"
    assert state["pace_atmosphere_connected_latched_state_suspect"] is True
    logger.close()


def test_checkvalve_mode_marks_clean_vent3_as_legacy_suspect(tmp_path: Path) -> None:
    logger, runner = _configure_runner(
        tmp_path,
        overrides={"pace_upstream_check_valve_installed": True},
    )
    point = _co2_point()
    _prime_post_isolation_runtime(runner, point)
    runner._set_point_runtime_fields(
        point,
        phase="co2",
        pace_upstream_check_valve_installed=True,
        post_isolation_pressure_drift_hpa=0.03,
        post_isolation_dewpoint_rise_c=0.01,
        post_isolation_pressure_peak_hpa=800.03,
        post_isolation_pressure_min_hpa=800.00,
        post_isolation_pressure_recovery_toward_ambient=False,
        pace_output_state=0,
        pace_isolation_state=0,
        pace_outp_state_query=0,
        pace_isol_state_query=0,
        pace_vent_status_query=3,
        pace_legacy_vent_state_3_suspect=True,
        pace_atmosphere_connected_latched_state_suspect=True,
        pace_vent_after_valve_state_query="CLOSED",
        pace_vent_popup_state_query="DISABLED",
        pace_vent_orpv_state_query="DISABLED",
        pace_vent_pupv_state_query="DISABLED",
        pace_effort_query=0.0,
        pace_comp1_query=0.0,
        pace_comp2_query=0.0,
        pace_oper_cond_query=1,
        pace_oper_pres_cond_query=4,
        pace_oper_pres_even_query=4,
        pace_oper_pres_vent_complete_bit=False,
    )

    diagnosis = runner._diagnose_post_isolation_result(
        point,
        phase="co2",
        cfg=runner._post_isolation_leak_test_cfg(point),
    )

    assert diagnosis == "pace_atmosphere_connected_latched_state_suspect"
    logger.close()


def test_checkvalve_mode_still_keeps_vent_in_progress_as_hard_reject(tmp_path: Path) -> None:
    logger, runner = _configure_runner(
        tmp_path,
        overrides={"pace_upstream_check_valve_installed": True},
    )
    point = _co2_point()
    _prime_post_isolation_runtime(runner, point)
    runner._set_point_runtime_fields(
        point,
        phase="co2",
        pace_upstream_check_valve_installed=True,
        post_isolation_pressure_drift_hpa=0.55,
        post_isolation_dewpoint_rise_c=0.22,
        post_isolation_pressure_peak_hpa=800.55,
        post_isolation_pressure_min_hpa=800.00,
        post_isolation_pressure_recovery_toward_ambient=True,
        pace_output_state=0,
        pace_isolation_state=0,
        pace_outp_state_query=0,
        pace_isol_state_query=0,
        pace_vent_status_query=1,
        pace_vent_after_valve_state_query="CLOSED",
        pace_vent_popup_state_query="DISABLED",
        pace_vent_orpv_state_query="DISABLED",
        pace_vent_pupv_state_query="DISABLED",
        pace_effort_query=0.0,
        pace_comp1_query=0.0,
        pace_comp2_query=0.0,
        pace_oper_cond_query=1,
        pace_oper_pres_cond_query=1,
        pace_oper_pres_even_query=1,
        pace_oper_pres_vent_complete_bit=True,
    )

    diagnosis = runner._diagnose_post_isolation_result(
        point,
        phase="co2",
        cfg=runner._post_isolation_leak_test_cfg(point),
    )

    assert diagnosis == "pace_vent_in_progress_suspect"
    logger.close()


def test_post_isolation_fast_capture_falls_back_when_in_limits_not_verified(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    logger, runner = _configure_runner(
        tmp_path,
        overrides={
            "co2_post_isolation_window_s": 1.0,
            "co2_post_isolation_poll_s": 0.25,
            "post_isolation_fast_capture_enabled": True,
            "post_isolation_fast_capture_allow_early_sample": True,
            "post_isolation_fast_capture_min_s": 1.0,
            "post_isolation_fast_capture_fallback_to_extended_diag": True,
            "post_isolation_extended_diag_window_s": 2.0,
        },
    )
    point = _co2_point()
    _prime_post_isolation_runtime(runner, point)
    runner._atmosphere_reference_hpa = 1006.0
    _install_post_isolation_sequences(
        monkeypatch,
        runner,
        pressures=[800.00, 800.02, 800.03, 800.03, 800.03],
        dewpoints=[-32.00, -31.99, -31.99, -31.98, -31.98],
        h2o_values=[0.40, 0.40, 0.40, 0.40, 0.40],
    )
    runner._pace_diagnostic_state_snapshot = lambda *args, **kwargs: {
        "pace_output_state": 0,
        "pace_isolation_state": 0,
        "pace_vent_status": 0,
        "pace_outp_state_query": 0,
        "pace_isol_state_query": 0,
        "pace_mode_query": "ACT",
        "pace_vent_status_query": 0,
        "pace_vent_completed_latched": False,
        "pace_vent_clear_attempted": False,
        "pace_vent_clear_result": "not_needed",
        "pace_vent_after_valve_state_query": "CLOSED",
        "pace_vent_popup_state_query": "DISABLED",
        "pace_vent_elapsed_time_query": 4.0,
        "pace_vent_orpv_state_query": "DISABLED",
        "pace_vent_pupv_state_query": "DISABLED",
        "pace_effort_query": 0.0,
        "pace_comp1_query": 0.0,
        "pace_comp2_query": 0.0,
        "pace_sens_pres_cont_query": 800.0,
        "pace_sens_pres_bar_query": 1006.0,
        "pace_sens_pres_inl_query": 800.0,
        "pace_sens_pres_inl_state_query": 0,
        "pace_sens_pres_inl_time_query": 0.0,
        "pace_sens_slew_query": 0.0,
        "pace_oper_cond_query": 1,
        "pace_oper_pres_cond_query": 0,
        "pace_oper_pres_even_query": 0,
        "pace_oper_pres_vent_complete_bit": False,
        "pace_oper_pres_in_limits_bit": False,
    }

    assert runner._wait_post_isolation_leak_test(
        point,
        phase="co2",
        context={"stop_event": None},
        handoff_mode="same_gas_pressure_step_handoff",
    ) is False
    state = runner._point_runtime_state(point, phase="co2") or {}
    assert state["post_isolation_fast_capture_reason"] == "in_limits_not_verified"
    assert state["post_isolation_capture_mode"] == "extended20s"
    logger.close()


def test_post_isolation_fast_capture_falls_back_when_slew_not_quiet(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    logger, runner = _configure_runner(
        tmp_path,
        overrides={
            "co2_post_isolation_window_s": 1.0,
            "co2_post_isolation_poll_s": 0.25,
            "post_isolation_fast_capture_enabled": True,
            "post_isolation_fast_capture_allow_early_sample": True,
            "post_isolation_fast_capture_min_s": 1.0,
            "post_isolation_fast_capture_fallback_to_extended_diag": True,
            "post_isolation_extended_diag_window_s": 2.0,
            "post_isolation_fast_capture_slew_abs_max": 0.01,
        },
    )
    point = _co2_point()
    _prime_post_isolation_runtime(runner, point)
    runner._atmosphere_reference_hpa = 1006.0
    _install_post_isolation_sequences(
        monkeypatch,
        runner,
        pressures=[800.00, 800.02, 800.03, 800.03, 800.03],
        dewpoints=[-32.00, -31.99, -31.99, -31.98, -31.98],
        h2o_values=[0.40, 0.40, 0.40, 0.40, 0.40],
    )
    runner._pace_diagnostic_state_snapshot = lambda *args, **kwargs: {
        "pace_output_state": 0,
        "pace_isolation_state": 0,
        "pace_vent_status": 0,
        "pace_outp_state_query": 0,
        "pace_isol_state_query": 0,
        "pace_mode_query": "ACT",
        "pace_vent_status_query": 0,
        "pace_vent_completed_latched": False,
        "pace_vent_clear_attempted": False,
        "pace_vent_clear_result": "not_needed",
        "pace_vent_after_valve_state_query": "CLOSED",
        "pace_vent_popup_state_query": "DISABLED",
        "pace_vent_elapsed_time_query": 4.0,
        "pace_vent_orpv_state_query": "DISABLED",
        "pace_vent_pupv_state_query": "DISABLED",
        "pace_effort_query": 0.0,
        "pace_comp1_query": 0.0,
        "pace_comp2_query": 0.0,
        "pace_sens_pres_cont_query": 800.0,
        "pace_sens_pres_bar_query": 1006.0,
        "pace_sens_pres_inl_query": 800.0,
        "pace_sens_pres_inl_state_query": 1,
        "pace_sens_pres_inl_time_query": 5.0,
        "pace_sens_slew_query": 0.08,
        "pace_oper_cond_query": 1,
        "pace_oper_pres_cond_query": 4,
        "pace_oper_pres_even_query": 4,
        "pace_oper_pres_vent_complete_bit": False,
        "pace_oper_pres_in_limits_bit": True,
    }

    assert runner._wait_post_isolation_leak_test(
        point,
        phase="co2",
        context={"stop_event": None},
        handoff_mode="same_gas_pressure_step_handoff",
    ) is True
    state = runner._point_runtime_state(point, phase="co2") or {}
    assert state["post_isolation_fast_capture_status"] in {"fallback", "fallback_complete"}
    assert state["post_isolation_fast_capture_fallback"] is True
    assert state["post_isolation_fast_capture_reason"] == "slew_not_quiet_after_output_off"
    assert state["post_isolation_capture_mode"] == "extended20s"
    logger.close()


def test_post_isolation_standard_diagnostic_works_without_optional_extension_fields(tmp_path: Path) -> None:
    logger, runner = _configure_runner(tmp_path)
    point = _co2_point()
    _prime_post_isolation_runtime(runner, point)
    runner._set_point_runtime_fields(
        point,
        phase="co2",
        post_isolation_pressure_drift_hpa=0.03,
        post_isolation_dewpoint_rise_c=0.01,
        post_isolation_pressure_peak_hpa=800.03,
        post_isolation_pressure_min_hpa=800.00,
        post_isolation_pressure_recovery_toward_ambient=False,
        pace_outp_state_query=0,
        pace_isol_state_query=0,
        pace_mode_query="ACT",
        pace_vent_status_query=2,
        pace_vent_after_valve_state_query="",
        pace_vent_popup_state_query="",
        pace_vent_elapsed_time_query=None,
        pace_vent_orpv_state_query="",
        pace_vent_pupv_state_query="",
        pace_effort_query=0.0,
        pace_comp1_query=None,
        pace_comp2_query=None,
        pace_oper_cond_query=1,
        pace_oper_pres_cond_query=1,
        pace_oper_pres_even_query=1,
        pace_oper_pres_vent_complete_bit=True,
        pace_oper_pres_in_limits_bit=False,
    )

    diagnosis = runner._diagnose_post_isolation_result(
        point,
        phase="co2",
        cfg=runner._post_isolation_leak_test_cfg(point),
    )

    assert diagnosis == "pace_vent_completed_latched_suspect"
    logger.close()
