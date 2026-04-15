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


def _make_run_dir(base: Path, *, co2_values, dew_values, h2o_values) -> Path:
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
    for index, pressure in enumerate(pressures, start=1):
        point_tag = f"round_step{index}_{pressure}hPa"
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
                "dewpoint_gate_result": "stable",
                "point_quality_reason": "",
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
                "pace_output_state": "0",
                "pace_isolation_state": "0",
                "pace_vent_status": "0",
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
                    "pressure_gate_status": "running",
                    "pressure_gate_reason": "awaiting_window_fill",
                    "pace_output_state": "0",
                    "pace_isolation_state": "0",
                    "pace_vent_status": "0",
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
                    "pressure_gate_status": "pass",
                    "pressure_gate_reason": "",
                    "pace_output_state": "0",
                    "pace_isolation_state": "0",
                    "pace_vent_status": "0",
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
            "dewpoint_gate_result",
            "point_quality_reason",
        ],
        point_rows,
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
            "pace_output_state",
            "pace_isolation_state",
            "pace_vent_status",
            "root_cause_reject_reason",
        ],
        timing_rows,
    )
    _write_csv(
        run_dir / "pressure_transition_trace.csv",
        [
            "ts",
            "point_row",
            "point_phase",
            "point_tag",
            "trace_stage",
            "handoff_mode",
            "capture_hold_status",
            "pressure_gate_status",
            "pressure_gate_reason",
            "pace_output_state",
            "pace_isolation_state",
            "pace_vent_status",
            "root_cause_reject_reason",
            "note",
        ],
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
    assert Path(summary["plots"]["co2_plot"]).exists()
    assert Path(summary["plots"]["dewpoint_h2o_plot"]).exists()
    assert (tmp_path / "analysis" / "same_gas_two_round_summary.json").exists()


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
            "forbidden_pre_sampling_actions": "",
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
    timing_rows[1]["root_cause_reject_reason"] = "controller_hunting_suspect"
    timing_rows[2]["sampling_begin_ts"] = ""
    timing_rows[2]["capture_hold_status"] = "fail"
    timing_rows[3]["sampling_begin_ts"] = ""
    timing_rows[3]["capture_hold_status"] = "fail"
    _write_csv(run_dir / "point_timing_summary.csv", timing_rows[0].keys(), timing_rows)

    with (run_dir / "pressure_transition_trace.csv").open("r", encoding="utf-8-sig", newline="") as handle:
        trace_rows = list(csv.DictReader(handle))
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
                "pressure_gate_status": "",
                "pressure_gate_reason": "",
                "pace_output_state": "0",
                "pace_isolation_state": "0",
                "pace_vent_status": "3",
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
                "pressure_gate_status": "",
                "pressure_gate_reason": "",
                "pace_output_state": "0",
                "pace_isolation_state": "0",
                "pace_vent_status": "3",
                "root_cause_reject_reason": "controller_hunting_suspect",
                "note": "source=pressure_gauge span_hpa=0.5 max_abs_drift_hpa=0.5 limit_hpa=0.25 samples=3",
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
    assert by_pressure[800]["reject_reason"] == "controller_hunting_suspect"
