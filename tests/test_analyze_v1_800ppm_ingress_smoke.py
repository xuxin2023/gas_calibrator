import csv
import importlib.util
from pathlib import Path


def _load_module():
    path = Path("scripts/analyze_v1_800ppm_ingress_smoke.py").resolve()
    spec = importlib.util.spec_from_file_location("analyze_v1_800ppm_ingress_smoke", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _write_csv(path: Path, fieldnames, rows) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _make_run_dir(
    base: Path,
    *,
    co2_values,
    dew_values,
    h2o_values,
    fast_status: str = "pass",
    capture_mode: str = "fast5s",
    fallback: str = "false",
    diagnosis_by_pressure: dict[int, str] | None = None,
    checkvalve: bool = False,
) -> Path:
    run_dir = base
    timestamps = [
        "2026-04-15T10:00:00",
        "2026-04-15T10:01:00",
        "2026-04-15T10:02:00",
        "2026-04-15T10:03:00",
    ]
    pressures = [1000, 800, 600, 500]
    point_rows = []
    timing_rows = []
    trace_rows = []
    diagnosis_by_pressure = diagnosis_by_pressure or {}
    for index, pressure in enumerate(pressures, start=1):
        point_tag = f"round_step{index}_{pressure}hPa"
        diagnosis = diagnosis_by_pressure.get(pressure, "pass")
        point_rows.append(
            {
                "point_row": index,
                "point_phase": "co2",
                "point_tag": point_tag,
                "pressure_target_hpa": pressure,
                "target_co2_ppm": 800,
                "co2_mean_primary_or_first": co2_values[index - 1],
                "h2o_mean_primary_or_first": h2o_values[index - 1],
                "dewpoint_mean_c": dew_values[index - 1],
                "pace_upstream_check_valve_installed": str(checkvalve).lower(),
                "post_isolation_pressure_truth_source": "external_gauge" if checkvalve else "",
                "dewpoint_gate_result": "stable",
                "point_quality_reason": "",
                "post_isolation_diagnosis": diagnosis,
                "post_isolation_pressure_drift_hpa": 0.02 * index,
                "post_isolation_dewpoint_rise_c": 0.01 * index,
            }
        )
        timing_rows.append(
            {
                "point_row": index,
                "point_phase": "co2",
                "point_tag": point_tag,
                "pressure_target_hpa": pressure,
                "pressure_in_limits_ts": timestamps[index - 1],
                "sampling_begin_ts": timestamps[index - 1],
                "handoff_mode": "same_gas_pressure_step_handoff",
                "capture_hold_status": "pass",
                "post_isolation_status": "pass",
                "post_isolation_diagnosis": diagnosis,
                "post_isolation_capture_mode": capture_mode,
                "post_isolation_fast_capture_status": fast_status,
                "post_isolation_fast_capture_reason": "",
                "post_isolation_fast_capture_elapsed_s": "5.0",
                "post_isolation_fast_capture_fallback": fallback,
                "post_isolation_pressure_drift_hpa": 0.02 * index,
                "post_isolation_dewpoint_rise_c": 0.01 * index,
                "pace_output_state": "0",
                "pace_isolation_state": "0",
                "pace_vent_status": "0",
                "pace_outp_state_query": "0",
                "pace_isol_state_query": "0",
                "pace_mode_query": "ACT",
                "pace_vent_status_query": "0",
                "pace_vent_completed_latched": "false",
                "pace_vent_clear_attempted": "false",
                "pace_vent_clear_result": "not_needed",
                "pace_vent_after_valve_state_query": "CLOSED",
                "pace_vent_popup_state_query": "DISABLED",
                "pace_vent_elapsed_time_query": "0.0",
                "pace_vent_orpv_state_query": "DISABLED",
                "pace_vent_pupv_state_query": "DISABLED",
                "pace_oper_cond_query": "1",
                "pace_oper_pres_cond_query": "2",
                "pace_effort_query": "0.0",
                "pace_comp1_query": "0.0",
                "pace_comp2_query": "0.0",
                "pace_sens_pres_cont_query": str(float(pressure)),
                "pace_sens_pres_bar_query": "1006.0",
                "pace_sens_pres_inl_query": str(float(pressure)),
                "pace_sens_pres_inl_state_query": "1",
                "pace_sens_pres_inl_time_query": "5.0",
                "pace_sens_inl_query": str(float(pressure)),
                "pace_sens_inl_time_query": "5.0",
                "pace_sens_slew_query": "0.0",
                "pace_oper_pres_even_query": "4",
                "pace_oper_pres_vent_complete_bit": "false",
                "pace_oper_pres_in_limits_bit": "true",
                "root_cause_reject_reason": "",
            }
        )
        trace_rows.extend(
            [
                {
                    "ts": timestamps[index - 1],
                    "point_row": index,
                    "point_phase": "co2",
                    "point_tag": point_tag,
                    "trace_stage": "pressure_in_limits_ready_check",
                    "handoff_mode": "same_gas_pressure_step_handoff",
                    "capture_hold_status": "pass",
                    "post_isolation_status": "running",
                    "post_isolation_diagnosis": "",
                    "post_isolation_capture_mode": capture_mode,
                    "post_isolation_fast_capture_status": "running",
                    "post_isolation_fast_capture_reason": "",
                    "post_isolation_fast_capture_elapsed_s": "",
                    "post_isolation_fast_capture_fallback": fallback,
                    "post_isolation_pressure_drift_hpa": "",
                    "post_isolation_dewpoint_rise_c": "",
                    "pressure_gate_status": "running",
                    "pressure_gate_reason": "awaiting_window_fill",
                    "pace_output_state": "0",
                    "pace_isolation_state": "0",
                    "pace_vent_status": "0",
                    "pace_outp_state_query": "0",
                    "pace_isol_state_query": "0",
                    "pace_mode_query": "ACT",
                    "pace_vent_status_query": "0",
                    "pace_vent_completed_latched": "false",
                    "pace_vent_clear_attempted": "false",
                    "pace_vent_clear_result": "not_needed",
                    "pace_vent_after_valve_state_query": "CLOSED",
                    "pace_vent_popup_state_query": "DISABLED",
                    "pace_vent_elapsed_time_query": "0.0",
                    "pace_vent_orpv_state_query": "DISABLED",
                    "pace_vent_pupv_state_query": "DISABLED",
                    "pace_oper_cond_query": "1",
                    "pace_oper_pres_cond_query": "2",
                    "pace_effort_query": "0.0",
                    "pace_comp1_query": "0.0",
                    "pace_comp2_query": "0.0",
                    "pace_sens_pres_cont_query": str(float(pressure)),
                    "pace_sens_pres_bar_query": "1006.0",
                    "pace_sens_pres_inl_query": str(float(pressure)),
                    "pace_sens_pres_inl_state_query": "1",
                    "pace_sens_pres_inl_time_query": "5.0",
                    "pace_sens_inl_query": str(float(pressure)),
                    "pace_sens_inl_time_query": "5.0",
                    "pace_sens_slew_query": "0.0",
                    "pace_oper_pres_even_query": "4",
                    "pace_oper_pres_vent_complete_bit": "false",
                    "pace_oper_pres_in_limits_bit": "true",
                    "root_cause_reject_reason": "",
                    "note": "",
                },
                {
                    "ts": timestamps[index - 1],
                    "point_row": index,
                    "point_phase": "co2",
                    "point_tag": point_tag,
                    "trace_stage": "sampling_begin",
                    "handoff_mode": "same_gas_pressure_step_handoff",
                    "capture_hold_status": "pass",
                    "post_isolation_status": "pass",
                    "post_isolation_diagnosis": diagnosis,
                    "post_isolation_capture_mode": capture_mode,
                    "post_isolation_fast_capture_status": fast_status,
                    "post_isolation_fast_capture_reason": "",
                    "post_isolation_fast_capture_elapsed_s": "5.0",
                    "post_isolation_fast_capture_fallback": fallback,
                    "post_isolation_pressure_drift_hpa": 0.02 * index,
                    "post_isolation_dewpoint_rise_c": 0.01 * index,
                    "pressure_gate_status": "pass",
                    "pressure_gate_reason": "",
                    "pace_output_state": "0",
                    "pace_isolation_state": "0",
                    "pace_vent_status": "0",
                    "pace_outp_state_query": "0",
                    "pace_isol_state_query": "0",
                    "pace_mode_query": "ACT",
                    "pace_vent_status_query": "0",
                    "pace_vent_completed_latched": "false",
                    "pace_vent_clear_attempted": "false",
                    "pace_vent_clear_result": "not_needed",
                    "pace_vent_after_valve_state_query": "CLOSED",
                    "pace_vent_popup_state_query": "DISABLED",
                    "pace_vent_elapsed_time_query": "0.0",
                    "pace_vent_orpv_state_query": "DISABLED",
                    "pace_vent_pupv_state_query": "DISABLED",
                    "pace_oper_cond_query": "1",
                    "pace_oper_pres_cond_query": "2",
                    "pace_effort_query": "0.0",
                    "pace_comp1_query": "0.0",
                    "pace_comp2_query": "0.0",
                    "pace_sens_pres_cont_query": str(float(pressure)),
                    "pace_sens_pres_bar_query": "1006.0",
                    "pace_sens_pres_inl_query": str(float(pressure)),
                    "pace_sens_pres_inl_state_query": "1",
                    "pace_sens_pres_inl_time_query": "5.0",
                    "pace_sens_inl_query": str(float(pressure)),
                    "pace_sens_inl_time_query": "5.0",
                    "pace_sens_slew_query": "0.0",
                    "pace_oper_pres_even_query": "4",
                    "pace_oper_pres_vent_complete_bit": "false",
                    "pace_oper_pres_in_limits_bit": "true",
                    "root_cause_reject_reason": "",
                    "note": "",
                },
            ]
        )

    _write_csv(
        run_dir / "points_test.csv",
        [
            "point_row",
            "point_phase",
            "point_tag",
            "pressure_target_hpa",
            "target_co2_ppm",
            "co2_mean_primary_or_first",
                "h2o_mean_primary_or_first",
                "dewpoint_mean_c",
                "pace_upstream_check_valve_installed",
                "post_isolation_pressure_truth_source",
                "dewpoint_gate_result",
                "point_quality_reason",
                "post_isolation_diagnosis",
                "post_isolation_pressure_drift_hpa",
                "post_isolation_dewpoint_rise_c",
        ],
        point_rows,
    )
    _write_csv(
        run_dir / "point_timing_summary.csv",
        list(timing_rows[0].keys()),
        timing_rows,
    )
    _write_csv(
        run_dir / "pressure_transition_trace.csv",
        list(trace_rows[0].keys()),
        trace_rows,
    )
    return run_dir


def test_analyze_v1_800ppm_ingress_smoke_outputs_expected_files(tmp_path: Path) -> None:
    module = _load_module()
    run1 = _make_run_dir(
        tmp_path / "run1",
        co2_values=[801.0, 799.0, 802.0, 798.0],
        dew_values=[-31.9, -32.0, -31.8, -31.9],
        h2o_values=[0.49, 0.50, 0.48, 0.49],
    )
    run2 = _make_run_dir(
        tmp_path / "run2",
        co2_values=[800.0, 798.5, 799.5, 797.5],
        dew_values=[-32.1, -32.0, -32.2, -32.1],
        h2o_values=[0.48, 0.49, 0.47, 0.48],
    )

    summary = module.analyze_runs([run1, run2], output_dir=tmp_path / "analysis")

    assert summary["conclusion"] == "混气已基本解决"
    assert Path(summary["point_summary_csv"]).exists()
    assert Path(summary["presample_lock_violations_csv"]).exists()
    assert Path(summary["reject_reason_summary_csv"]).exists()
    assert Path(summary["post_isolation_diagnosis_summary_csv"]).exists()
    assert Path(summary["pace_post_isolation_diagnosis_summary_csv"]).exists()
    assert Path(summary["pace_standard_status_summary_csv"]).exists()
    assert Path(summary["baseline_plateau_summary_csv"]).exists()
    assert Path(summary["pace_protective_vent_state_summary_csv"]).exists()
    assert Path(summary["fast5s_vs_extended20s_with_effort_summary_csv"]).exists()
    assert Path(summary["checkvalve_fast_smoke_summary_csv"]).exists()
    assert Path(summary["plots"]["co2_plot"]).exists()
    assert Path(summary["plots"]["dewpoint_h2o_plot"]).exists()
    assert Path(summary["plots"]["pace_vent_state_vs_time_plot"]).exists()
    assert Path(summary["plots"]["pace_effort_vs_time_plot"]).exists()
    assert Path(summary["plots"]["pace_comp_supply_vacuum_vs_time_plot"]).exists()
    assert Path(summary["plots"]["pace_inlimits_vs_time_plot"]).exists()
    assert Path(summary["plots"]["post_isolation_drift_plot"]).exists()
    assert Path(summary["plots"]["post_isolation_dewpoint_rise_plot"]).exists()
    assert (tmp_path / "analysis" / "same_gas_two_round_summary.json").exists()
    assert summary["conclusion_code"] == "cleared_or_not_observed"


def test_classify_ingress_result_flags_clear_low_pressure_pullback() -> None:
    module = _load_module()
    point_results = [
        {
            "round_index": 1,
            "pressure_hpa": pressure,
            "co2_mean_ppm": co2,
            "dewpoint_mean_c": dew,
            "h2o_mean_mmol": h2o,
            "reject_reason": "",
            "post_isolation_diagnosis": "",
            "post_isolation_fast_capture_status": "",
            "post_isolation_capture_mode": "",
            "post_isolation_fast_capture_fallback": False,
            "forbidden_pre_sampling_actions": "",
            "handoff_mode": "same_gas_pressure_step_handoff",
        }
        for pressure, co2, dew, h2o in [
            (1000, 800.0, -35.0, 0.30),
            (800, 735.0, -34.5, 0.42),
            (600, 690.0, -34.0, 0.63),
            (500, 650.0, -33.6, 0.88),
        ]
    ]

    conclusion, metrics = module.classify_ingress_result(point_results)

    assert conclusion == "混气仍明显存在"
    assert metrics["round_metrics"][1]["co2_monotonic_down"] is True


def test_analyze_runs_supplements_trace_only_rejected_points(tmp_path: Path) -> None:
    module = _load_module()
    run_dir = _make_run_dir(
        tmp_path / "run_trace_only_rejects",
        co2_values=[801.0, 799.0, 802.0, 798.0],
        dew_values=[-31.9, -32.0, -31.8, -31.9],
        h2o_values=[0.49, 0.50, 0.48, 0.49],
    )

    with (run_dir / "points_test.csv").open("r", encoding="utf-8-sig", newline="") as handle:
        sampled_rows = list(csv.DictReader(handle))
    with (run_dir / "points_test.csv").open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=sampled_rows[0].keys())
        writer.writeheader()
        writer.writerow(sampled_rows[0])

    with (run_dir / "point_timing_summary.csv").open("r", encoding="utf-8-sig", newline="") as handle:
        timing_rows = list(csv.DictReader(handle))
    timing_rows[1]["sampling_begin_ts"] = ""
    timing_rows[1]["capture_hold_status"] = "fail"
    timing_rows[1]["root_cause_reject_reason"] = "pace_vent_in_progress_suspect"
    timing_rows[1]["post_isolation_diagnosis"] = "pace_vent_in_progress_suspect"
    timing_rows[2]["sampling_begin_ts"] = ""
    timing_rows[2]["capture_hold_status"] = "fail"
    timing_rows[3]["sampling_begin_ts"] = ""
    timing_rows[3]["capture_hold_status"] = "fail"
    _write_csv(run_dir / "point_timing_summary.csv", timing_rows[0].keys(), timing_rows)

    with (run_dir / "pressure_transition_trace.csv").open("r", encoding="utf-8-sig", newline="") as handle:
        trace_rows = list(csv.DictReader(handle))
    for row in trace_rows:
        if row["trace_stage"] == "sampling_begin" and row["point_row"] in {"2", "3", "4"}:
            row["trace_stage"] = "post_isolation_failed"
    trace_rows.extend(
        [
            {
                "ts": "2026-04-15T10:01:00",
                "point_row": 2,
                "point_phase": "co2",
                "point_tag": "round_step2_800hPa",
                "trace_stage": "handoff_mode_selected",
                "handoff_mode": "same_gas_pressure_step_handoff",
                "capture_hold_status": "",
                "post_isolation_status": "running",
                "post_isolation_diagnosis": "",
                "post_isolation_capture_mode": "extended20s",
                "post_isolation_fast_capture_status": "fallback",
                "post_isolation_fast_capture_reason": "vent_not_zero",
                "post_isolation_fast_capture_elapsed_s": "5.0",
                "post_isolation_fast_capture_fallback": "true",
                "post_isolation_pressure_drift_hpa": "",
                "post_isolation_dewpoint_rise_c": "",
                "pressure_gate_status": "",
                "pressure_gate_reason": "",
                "pace_output_state": "0",
                "pace_isolation_state": "0",
                "pace_vent_status": "1",
                "pace_outp_state_query": "0",
                "pace_isol_state_query": "0",
                "pace_mode_query": "ACT",
                "pace_vent_status_query": "1",
                "pace_vent_completed_latched": "false",
                "pace_vent_clear_attempted": "false",
                "pace_vent_clear_result": "not_needed",
                "pace_vent_after_valve_state_query": "OPEN",
                "pace_vent_popup_state_query": "DISABLED",
                "pace_vent_elapsed_time_query": "9.0",
                "pace_vent_orpv_state_query": "DISABLED",
                "pace_vent_pupv_state_query": "DISABLED",
                "pace_oper_cond_query": "1",
                "pace_oper_pres_cond_query": "2",
                "pace_effort_query": "0.0",
                "pace_comp1_query": "0.0",
                "pace_comp2_query": "0.0",
                "pace_sens_pres_cont_query": "800.0",
                "pace_sens_pres_bar_query": "1006.0",
                "pace_sens_pres_inl_query": "800.0",
                "pace_sens_pres_inl_state_query": "1",
                "pace_sens_pres_inl_time_query": "5.0",
                "pace_sens_inl_query": "800.0",
                "pace_sens_inl_time_query": "5.0",
                "pace_sens_slew_query": "0.0",
                "pace_oper_pres_even_query": "4",
                "pace_oper_pres_vent_complete_bit": "false",
                "pace_oper_pres_in_limits_bit": "true",
                "root_cause_reject_reason": "",
                "note": "",
            },
            {
                "ts": "2026-04-15T10:01:05",
                "point_row": 2,
                "point_phase": "co2",
                "point_tag": "round_step2_800hPa",
                "trace_stage": "capture_hold_failed",
                "handoff_mode": "same_gas_pressure_step_handoff",
                "capture_hold_status": "fail",
                "post_isolation_status": "fail",
                "post_isolation_diagnosis": "pace_vent_in_progress_suspect",
                "post_isolation_capture_mode": "extended20s",
                "post_isolation_fast_capture_status": "fallback",
                "post_isolation_fast_capture_reason": "vent_not_zero",
                "post_isolation_fast_capture_elapsed_s": "5.0",
                "post_isolation_fast_capture_fallback": "true",
                "post_isolation_pressure_drift_hpa": "0.60",
                "post_isolation_dewpoint_rise_c": "0.25",
                "pressure_gate_status": "",
                "pressure_gate_reason": "",
                "pace_output_state": "0",
                "pace_isolation_state": "0",
                "pace_vent_status": "1",
                "pace_outp_state_query": "0",
                "pace_isol_state_query": "0",
                "pace_mode_query": "ACT",
                "pace_vent_status_query": "1",
                "pace_vent_completed_latched": "false",
                "pace_vent_clear_attempted": "false",
                "pace_vent_clear_result": "not_needed",
                "pace_vent_after_valve_state_query": "OPEN",
                "pace_vent_popup_state_query": "DISABLED",
                "pace_vent_elapsed_time_query": "10.0",
                "pace_vent_orpv_state_query": "DISABLED",
                "pace_vent_pupv_state_query": "DISABLED",
                "pace_oper_cond_query": "1",
                "pace_oper_pres_cond_query": "2",
                "pace_effort_query": "0.0",
                "pace_comp1_query": "0.0",
                "pace_comp2_query": "0.0",
                "pace_sens_pres_cont_query": "800.0",
                "pace_sens_pres_bar_query": "1006.0",
                "pace_sens_pres_inl_query": "800.0",
                "pace_sens_pres_inl_state_query": "1",
                "pace_sens_pres_inl_time_query": "5.0",
                "pace_sens_inl_query": "800.0",
                "pace_sens_inl_time_query": "5.0",
                "pace_sens_slew_query": "0.0",
                "pace_oper_pres_even_query": "4",
                "pace_oper_pres_vent_complete_bit": "false",
                "pace_oper_pres_in_limits_bit": "true",
                "root_cause_reject_reason": "pace_vent_in_progress_suspect",
                "note": "vent_status=1",
            },
        ]
    )
    _write_csv(run_dir / "pressure_transition_trace.csv", trace_rows[0].keys(), trace_rows)

    summary = module.analyze_runs([run_dir], output_dir=tmp_path / "analysis_trace_only_rejects")
    by_pressure = {row["pressure_hpa"]: row for row in summary["point_results"]}

    assert set(by_pressure) == {1000, 800, 600, 500}
    assert by_pressure[800]["status"] == "rejected_before_sampling"
    assert by_pressure[800]["handoff_mode"] == "same_gas_pressure_step_handoff"
    assert by_pressure[800]["capture_hold_state"] == "fail"
    assert by_pressure[800]["reject_reason"] == "pace_vent_in_progress_suspect"


def test_classify_ingress_result_tracks_post_isolation_categories_and_fast_capture_assessment() -> None:
    module = _load_module()
    point_results = [
        {
            "round_index": 1,
            "pressure_hpa": 1000,
            "co2_mean_ppm": 800.0,
            "dewpoint_mean_c": -35.0,
            "h2o_mean_mmol": 0.30,
            "reject_reason": "",
            "post_isolation_diagnosis": "pass",
            "post_isolation_capture_mode": "fast5s",
            "post_isolation_fast_capture_status": "pass",
            "post_isolation_fast_capture_fallback": False,
            "forbidden_pre_sampling_actions": "",
            "handoff_mode": "same_gas_pressure_step_handoff",
        },
        {
            "round_index": 1,
            "pressure_hpa": 800,
            "co2_mean_ppm": None,
            "dewpoint_mean_c": None,
            "h2o_mean_mmol": None,
            "reject_reason": "pace_vent_after_valve_config_open_suspect",
            "post_isolation_diagnosis": "pace_vent_after_valve_config_open_suspect",
            "post_isolation_capture_mode": "extended20s",
            "post_isolation_fast_capture_status": "fallback",
            "post_isolation_fast_capture_fallback": True,
            "forbidden_pre_sampling_actions": "",
            "handoff_mode": "same_gas_pressure_step_handoff",
        },
        {
            "round_index": 1,
            "pressure_hpa": 600,
            "co2_mean_ppm": None,
            "dewpoint_mean_c": None,
            "h2o_mean_mmol": None,
            "reject_reason": "dead_volume_wet_release_suspect",
            "post_isolation_diagnosis": "dead_volume_wet_release_suspect",
            "post_isolation_capture_mode": "extended20s",
            "post_isolation_fast_capture_status": "fallback",
            "post_isolation_fast_capture_fallback": True,
            "forbidden_pre_sampling_actions": "",
            "handoff_mode": "same_gas_pressure_step_handoff",
        },
        {
            "round_index": 1,
            "pressure_hpa": 500,
            "co2_mean_ppm": None,
            "dewpoint_mean_c": None,
            "h2o_mean_mmol": None,
            "reject_reason": "controller_hunting_suspect",
            "post_isolation_diagnosis": "",
            "post_isolation_capture_mode": "extended20s",
            "post_isolation_fast_capture_status": "fallback",
            "post_isolation_fast_capture_fallback": True,
            "forbidden_pre_sampling_actions": "",
            "handoff_mode": "same_gas_pressure_step_handoff",
        },
    ]

    conclusion, metrics = module.classify_ingress_result(point_results)

    assert conclusion == "混气明显减轻但未完全解决"
    assert metrics["pace_vent_after_valve_config_open_count"] == 1
    assert metrics["dead_volume_wet_release_count"] == 1
    assert metrics["controller_hunting_count"] == 1
    assert metrics["fast_capture_assessment"] == "5 秒快采失败且提示 vent-after-valve / protective vent"


def test_classify_ingress_result_distinguishes_standard_vent_and_effort_findings() -> None:
    module = _load_module()
    point_results = [
        {
            "round_index": 1,
            "pressure_hpa": 800,
            "co2_mean_ppm": None,
            "dewpoint_mean_c": None,
            "h2o_mean_mmol": None,
            "reject_reason": "pace_vent_completed_latched_suspect",
            "post_isolation_diagnosis": "pace_vent_completed_latched_suspect",
            "post_isolation_capture_mode": "extended20s",
            "post_isolation_fast_capture_status": "fallback",
            "post_isolation_fast_capture_fallback": True,
            "forbidden_pre_sampling_actions": "",
            "handoff_mode": "same_gas_pressure_step_handoff",
        },
        {
            "round_index": 1,
            "pressure_hpa": 600,
            "co2_mean_ppm": None,
            "dewpoint_mean_c": None,
            "h2o_mean_mmol": None,
            "reject_reason": "pace_supply_vacuum_compensation_suspect",
            "post_isolation_diagnosis": "pace_supply_vacuum_compensation_suspect",
            "post_isolation_capture_mode": "extended20s",
            "post_isolation_fast_capture_status": "fallback",
            "post_isolation_fast_capture_fallback": True,
            "forbidden_pre_sampling_actions": "",
            "handoff_mode": "same_gas_pressure_step_handoff",
        },
    ]

    _conclusion, metrics = module.classify_ingress_result(point_results)

    assert metrics["pace_vent_completed_latched_count"] == 1
    assert metrics["pace_supply_vacuum_compensation_count"] == 1
    assert metrics["fast_capture_assessment"] == "5 秒快采失败且提示 VENT=2 锁存未清"

def test_classify_ingress_result_prioritizes_baseline_source_or_stream_problem() -> None:
    module = _load_module()
    point_results = [
        {
            "round_index": 1,
            "pressure_hpa": 1000,
            "target_co2_ppm": 800.0,
            "co2_mean_ppm": 1515.4,
            "dewpoint_mean_c": -35.0,
            "h2o_mean_mmol": 0.30,
            "reject_reason": "",
            "post_isolation_diagnosis": "pass",
            "post_isolation_capture_mode": "fast5s",
            "post_isolation_fast_capture_status": "pass",
            "post_isolation_fast_capture_fallback": False,
            "forbidden_pre_sampling_actions": "",
            "handoff_mode": "same_gas_pressure_step_handoff",
            "status": "sampled",
        },
        {
            "round_index": 1,
            "pressure_hpa": 800,
            "target_co2_ppm": 800.0,
            "co2_mean_ppm": None,
            "dewpoint_mean_c": None,
            "h2o_mean_mmol": None,
            "reject_reason": "sealed_path_leak_suspect",
            "post_isolation_diagnosis": "sealed_path_leak_suspect",
            "post_isolation_capture_mode": "extended20s",
            "post_isolation_fast_capture_status": "fallback",
            "post_isolation_fast_capture_fallback": True,
            "forbidden_pre_sampling_actions": "",
            "handoff_mode": "same_gas_pressure_step_handoff",
            "status": "rejected_before_sampling",
        },
    ]

    conclusion, metrics = module.classify_ingress_result(point_results)

    assert conclusion == "near-ambient baseline points to source/stream mismatch before low-pressure inference"
    assert metrics["conclusion_code"] == "baseline_source_or_stream_problem"
    assert metrics["accepted_near_ambient_count"] == 1
    assert metrics["baseline_large_delta_count"] == 1


def test_classify_ingress_result_prioritizes_legacy_vent_state_problem() -> None:
    module = _load_module()
    point_results = [
        {
            "round_index": 1,
            "pressure_hpa": 800,
            "target_co2_ppm": 800.0,
            "co2_mean_ppm": None,
            "dewpoint_mean_c": None,
            "h2o_mean_mmol": None,
            "reject_reason": "pace_atmosphere_connected_latched_state_suspect",
            "post_isolation_diagnosis": "pace_atmosphere_connected_latched_state_suspect",
            "post_isolation_capture_mode": "extended20s",
            "post_isolation_fast_capture_status": "fallback",
            "post_isolation_fast_capture_fallback": True,
            "forbidden_pre_sampling_actions": "",
            "handoff_mode": "same_gas_pressure_step_handoff",
            "status": "rejected_before_sampling",
        }
    ]

    conclusion, metrics = module.classify_ingress_result(point_results)

    assert conclusion == "legacy VENT=3 remains watchlist-only and blocks low-pressure inference"
    assert metrics["conclusion_code"] == "legacy_vent_state_problem"
    assert metrics["pace_legacy_vent_state_3_count"] == 1


def test_classify_ingress_result_reports_checkvalve_quick_smoke_pass() -> None:
    module = _load_module()
    point_results = [
        {
            "round_index": 1,
            "pressure_hpa": 1000,
            "target_co2_ppm": 800.0,
            "co2_mean_ppm": 801.0,
            "dewpoint_mean_c": -32.0,
            "h2o_mean_mmol": 0.40,
            "reject_reason": "",
            "post_isolation_diagnosis": "pass",
            "post_isolation_capture_mode": "fast5s",
            "post_isolation_fast_capture_status": "pass",
            "post_isolation_fast_capture_fallback": False,
            "post_isolation_pressure_drift_hpa": 0.01,
            "post_isolation_dewpoint_rise_c": 0.01,
            "forbidden_pre_sampling_actions": "",
            "handoff_mode": "same_gas_pressure_step_handoff",
            "status": "sampled",
            "pace_upstream_check_valve_installed": True,
            "post_isolation_pressure_truth_source": "external_gauge",
        },
        {
            "round_index": 1,
            "pressure_hpa": 800,
            "target_co2_ppm": 800.0,
            "co2_mean_ppm": 800.0,
            "dewpoint_mean_c": -32.0,
            "h2o_mean_mmol": 0.40,
            "reject_reason": "",
            "post_isolation_diagnosis": "controller_side_state_only_without_analyzer_side_effect",
            "post_isolation_capture_mode": "fast5s",
            "post_isolation_fast_capture_status": "pass",
            "post_isolation_fast_capture_fallback": False,
            "post_isolation_pressure_drift_hpa": 0.01,
            "post_isolation_dewpoint_rise_c": 0.01,
            "forbidden_pre_sampling_actions": "",
            "handoff_mode": "same_gas_pressure_step_handoff",
            "status": "sampled",
            "pace_upstream_check_valve_installed": True,
            "post_isolation_pressure_truth_source": "external_gauge",
        },
    ]

    conclusion, metrics = module.classify_ingress_result(point_results)

    assert conclusion == "check-valve quick smoke passed on analyzer-side evidence"
    assert metrics["conclusion_code"] == "checkvalve_quick_smoke_pass"


def test_classify_ingress_result_reports_checkvalve_controller_side_only_state() -> None:
    module = _load_module()
    point_results = [
        {
            "round_index": 1,
            "pressure_hpa": 800,
            "target_co2_ppm": 800.0,
            "co2_mean_ppm": None,
            "dewpoint_mean_c": None,
            "h2o_mean_mmol": None,
            "reject_reason": "",
            "post_isolation_diagnosis": "controller_side_state_only_without_analyzer_side_effect",
            "post_isolation_capture_mode": "fast5s",
            "post_isolation_fast_capture_status": "fail",
            "post_isolation_fast_capture_fallback": False,
            "post_isolation_pressure_drift_hpa": 0.02,
            "post_isolation_dewpoint_rise_c": 0.01,
            "forbidden_pre_sampling_actions": "",
            "handoff_mode": "same_gas_pressure_step_handoff",
            "status": "rejected_before_sampling",
            "pace_upstream_check_valve_installed": True,
            "post_isolation_pressure_truth_source": "external_gauge",
        }
    ]

    conclusion, metrics = module.classify_ingress_result(point_results)

    assert conclusion == "controller-side vent state persisted without analyzer-side effect under check-valve mode"
    assert metrics["conclusion_code"] == "controller_side_state_only_without_analyzer_side_effect"
    assert metrics["controller_side_only_without_analyzer_side_effect_count"] == 1


def test_load_run_point_results_supports_live_chinese_points_csv(tmp_path: Path) -> None:
    module = _load_module()
    run_dir = tmp_path / "run_live_cn"
    _write_csv(
        run_dir / "points_test.csv",
        [
            "运行ID",
            "会话ID",
            "设备ID(追溯)",
            "气体类型",
            "步骤",
            "点位编号",
            "目标值",
            "测量值",
            "采样时间",
            "窗口开始时间",
            "窗口结束时间",
            "窗口样本数",
            "稳态标记",
            "点位标题",
            "校准点行号",
            "流程阶段",
            "点位标签",
            "压力执行模式",
            "压力目标标签",
            "温箱目标温度C",
            "目标二氧化碳浓度ppm",
            "目标压力hPa",
            "二氧化碳平均值",
        ],
        [
            {
                "运行ID": "run_live_cn",
                "会话ID": "run_live_cn",
                "设备ID(追溯)": "073",
                "气体类型": "CO2",
                "步骤": "co2",
                "点位编号": "3",
                "目标值": "800.0",
                "测量值": "1515.4395",
                "采样时间": "2026-04-16T00:28:52.787",
                "窗口开始时间": "2026-04-16T00:28:43.785",
                "窗口结束时间": "2026-04-16T00:28:52.787",
                "窗口样本数": "10",
                "稳态标记": "True",
                "点位标题": "20°C环境，二氧化碳800ppm，气压1000hPa",
                "校准点行号": "3",
                "流程阶段": "co2",
                "点位标签": "co2_groupa_800ppm_1000hpa",
                "压力执行模式": "sealed_controlled",
                "压力目标标签": "1000hPa",
                "温箱目标温度C": "20.0",
                "目标二氧化碳浓度ppm": "800.0",
                "目标压力hPa": "1000.0",
                "二氧化碳平均值": "1515.4395",
            }
        ],
    )
    _write_csv(
        run_dir / "point_timing_summary.csv",
        [
            "point_row",
            "point_phase",
            "point_tag",
            "pressure_target_hpa",
            "pressure_in_limits_ts",
            "sampling_begin_ts",
            "handoff_mode",
            "capture_hold_status",
            "post_isolation_status",
            "post_isolation_diagnosis",
        ],
        [
            {
                "point_row": "3",
                "point_phase": "co2",
                "point_tag": "co2_groupa_800ppm_1000hpa",
                "pressure_target_hpa": "1000",
                "pressure_in_limits_ts": "2026-04-16T00:28:42.578",
                "sampling_begin_ts": "2026-04-16T00:28:36.424",
                "handoff_mode": "same_gas_pressure_step_handoff",
                "capture_hold_status": "pass",
                "post_isolation_status": "skipped",
                "post_isolation_diagnosis": "skipped",
            }
        ],
    )
    _write_csv(
        run_dir / "pressure_transition_trace.csv",
        ["ts", "point_row", "point_phase", "point_tag", "trace_stage"],
        [
            {
                "ts": "2026-04-16T00:28:36.424",
                "point_row": "3",
                "point_phase": "co2",
                "point_tag": "co2_groupa_800ppm_1000hpa",
                "trace_stage": "sampling_begin",
            }
        ],
    )

    results = module.load_run_point_results(run_dir)

    assert len(results) == 1
    assert results[0]["point_row"] == 3
    assert results[0]["pressure_hpa"] == 1000
    assert results[0]["point_tag"] == "co2_groupa_800ppm_1000hpa"
    assert results[0]["co2_mean_ppm"] == 1515.4395
    assert results[0]["status"] == "sampled"
