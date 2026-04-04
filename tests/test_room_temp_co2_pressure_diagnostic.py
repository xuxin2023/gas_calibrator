from __future__ import annotations

import csv
from datetime import datetime, timedelta

from pathlib import Path
import gas_calibrator.tools.run_room_temp_co2_pressure_diagnostic as room_temp_diag_tool

from gas_calibrator.tools.run_room_temp_co2_pressure_diagnostic import (
    _append_live_csv_row,
    _load_cli_config,
    _run_closed_pressure_swing_predry,
    parse_args,
)
from gas_calibrator.validation.room_temp_co2_pressure_diagnostic import (
    analyze_room_temp_diagnostic,
    build_analyzer_chain_compare_vs_baseline,
    build_analyzer_chain_compare_vs_8ch,
    build_analyzer_chain_isolation_comparison,
    build_analyzer_chain_pace_contribution_comparison,
    build_analyzer_chain_isolation_summary,
    build_aligned_rows,
    build_flush_summary,
    build_pressure_point_summary,
    evaluate_flush_gate,
    export_analyzer_chain_isolation_results,
    export_room_temp_diagnostic_results,
)


def _flush_summary(
    variant: str,
    layer: int,
    repeat_index: int,
    gas_ppm: int,
    *,
    vent_state: str = "VENT_ON",
    duration_s: float = 185.0,
    gate_status: str = "pass",
    gate_fail_reason: str = "",
    actual_deadtime_s: float = 2.0,
    last30_mean: float = 1.4,
    t95_s: float = 85.0,
):
    return {
        "process_variant": variant,
        "layer": layer,
        "repeat_index": repeat_index,
        "gas_ppm": gas_ppm,
        "flush_start_time": "2026-04-02T08:00:00",
        "flush_end_time": "2026-04-02T08:03:05",
        "actual_deadtime_s": actual_deadtime_s,
        "flush_duration_s": duration_s,
        "vent_state_during_flush": vent_state,
        "flush_gate_window_s": 60.0,
        "flush_gate_status": gate_status,
        "flush_gate_pass": gate_status == "pass",
        "flush_gate_fail_reason": gate_fail_reason,
        "flush_ratio_start": last30_mean + 0.1,
        "flush_ratio_end": last30_mean,
        "flush_ratio_t90_s": t95_s - 15.0,
        "flush_ratio_t95_s": t95_s,
        "flush_last30s_mean": last30_mean,
        "flush_last30s_std": 0.001,
        "flush_last30s_ratio_slope": 0.0001,
        "flush_last60s_mean": last30_mean,
        "flush_last60s_std": 0.0012,
        "flush_last60s_ratio_slope": 0.0001,
        "flush_last60s_dewpoint_span": 0.08,
        "flush_last60s_dewpoint_slope": 0.0004,
        "flush_pressure_start_hpa": 1005.0,
        "flush_pressure_peak_hpa": 1009.0,
        "flush_pressure_end_hpa": 1006.0,
        "flush_pressure_rise_hpa": 4.0,
        "flush_dewpoint_start": -30.2,
        "flush_dewpoint_end": -30.1,
        "flush_rh_start": 5.2,
        "flush_rh_end": 5.0,
        "flush_warning_flags": [] if gate_status == "pass" else [gate_fail_reason],
        "flush_pressure_median": 1006.0,
        "flush_pressure_p95": 1008.5,
        "flush_pressure_p99": 1008.8,
        "flush_spike_count": 0,
        "flush_spike_max": None,
        "flush_spike_duration_s": 0.0,
        "analyzer_raw_sample_count": 30,
        "gauge_raw_sample_count": 30,
        "dewpoint_raw_sample_count": 30,
        "aligned_sample_count": 25,
    }


def _hold_summary(
    variant: str,
    repeat_index: int,
    gas_ppm: int,
    *,
    phase_status: str = "pass",
    pressure_drift_per_min: float = 0.2,
    ratio_drift_per_min: float = 0.001,
    dew_drift_per_min: float = 0.02,
):
    return {
        "process_variant": variant,
        "layer": 2,
        "repeat_index": repeat_index,
        "gas_ppm": gas_ppm,
        "hold_start_time": "2026-04-02T08:10:00",
        "hold_end_time": "2026-04-02T08:13:00",
        "hold_duration_s": 180.0,
        "hold_pressure_start_hpa": 1110.0,
        "hold_pressure_end_hpa": 1109.4,
        "hold_pressure_drift_hpa": pressure_drift_per_min * 3.0,
        "hold_pressure_drift_hpa_per_min": pressure_drift_per_min,
        "hold_ratio_start": 1.40,
        "hold_ratio_end": 1.403,
        "hold_ratio_drift": ratio_drift_per_min * 3.0,
        "hold_ratio_drift_per_min": ratio_drift_per_min,
        "hold_dewpoint_start_c": -30.0,
        "hold_dewpoint_end_c": -29.94,
        "hold_dewpoint_drift_c": dew_drift_per_min * 3.0,
        "hold_dewpoint_drift_c_per_min": dew_drift_per_min,
        "hold_rh_start": 5.0,
        "hold_rh_end": 5.1,
        "hold_rh_drift": 0.1,
        "hold_warning_flags": [],
        "analyzer_raw_sample_count": 25,
        "gauge_raw_sample_count": 25,
        "dewpoint_raw_sample_count": 25,
        "aligned_sample_count": 25,
        "phase_status": phase_status,
    }


def _pressure_summary(
    variant: str,
    layer: int,
    repeat_index: int,
    gas_ppm: int,
    pressure_hpa: int,
    ratio_mean: float,
    *,
    phase_status: str = "pass",
    stable_sample_count: int = 20,
    bias: float = 1.2,
):
    return {
        "process_variant": variant,
        "layer": layer,
        "repeat_index": repeat_index,
        "gas_ppm": gas_ppm,
        "pressure_target_hpa": pressure_hpa,
        "start_time": "2026-04-02T08:20:00",
        "settle_reached_time": "2026-04-02T08:20:20",
        "sample_start_time": "2026-04-02T08:20:21",
        "sample_end_time": "2026-04-02T08:20:41",
        "stable_sample_count": stable_sample_count,
        "stable_sample_count_min": 10,
        "stable_sample_count_target": 20,
        "analyzer_raw_sample_count": stable_sample_count,
        "gauge_raw_sample_count": stable_sample_count,
        "dewpoint_raw_sample_count": stable_sample_count,
        "aligned_sample_count": stable_sample_count,
        "analyzer2_co2_ratio_mean": ratio_mean,
        "analyzer2_co2_ratio_std": 0.0012,
        "analyzer2_pressure_mean": pressure_hpa - bias,
        "analyzer2_pressure_std": 0.1,
        "gauge_pressure_mean": pressure_hpa,
        "gauge_pressure_std": 0.2,
        "gauge_minus_analyzer_pressure_mean": bias,
        "gauge_minus_analyzer_pressure_std": 0.08,
        "dewpoint_mean": -30.0 + (1100 - pressure_hpa) * 0.0001,
        "dewpoint_std": 0.05,
        "dewpoint_temp_mean": 22.0,
        "dewpoint_rh_mean": 5.0,
        "pressure_tracking_error": 0.0,
        "settle_time_s": 20.0,
        "dwell_to_stable_s": 1.0,
        "overshoot_hpa": 1.0,
        "rebound_hpa": 1.3,
        "pressure_monotonicity_score": 0.98,
        "point_window_ratio_slope_per_s": 0.0002,
        "point_window_dewpoint_slope_per_s": 0.0004,
        "phase_status": phase_status,
        "warning_flags": [] if phase_status == "pass" else ["stable_sample_count_below_min"],
    }


def _phase_gate(variant: str, layer: int, repeat_index: int, phase: str, gate_name: str, status: str):
    return {
        "timestamp": "2026-04-02T08:00:00",
        "process_variant": variant,
        "layer": layer,
        "repeat_index": repeat_index,
        "phase": phase,
        "gas_ppm": 0,
        "pressure_target_hpa": None,
        "gate_name": gate_name,
        "gate_status": status,
        "gate_pass": status == "pass",
        "gate_window_s": 60.0,
        "gate_value": {},
        "gate_threshold": {},
        "gate_fail_reason": "" if status == "pass" else gate_name,
        "note": "",
    }


def _pass_dataset():
    flush = []
    holds = []
    pressure = []
    gates = []
    endpoint_map = {
        1100: {0: 1.400, 1000: 1.150},
        800: {0: 1.398, 1000: 1.153},
        500: {0: 1.397, 1000: 1.156},
    }
    for repeat_index in range(1, 4):
        flush.append(_flush_summary("B", 1, repeat_index, 0, last30_mean=1.40))
        flush.append(_flush_summary("B", 1, repeat_index, 1000, last30_mean=1.15))
        flush.append(_flush_summary("B", 1, repeat_index, 0, last30_mean=1.401))
        flush.append(_flush_summary("B", 2, repeat_index, 0, last30_mean=1.40))
        flush.append(_flush_summary("B", 2, repeat_index, 1000, last30_mean=1.15))
        flush.append(_flush_summary("B", 3, repeat_index, 0, last30_mean=1.40))
        flush.append(_flush_summary("B", 3, repeat_index, 1000, last30_mean=1.15))
        holds.append(_hold_summary("B", repeat_index, 0))
        holds.append(_hold_summary("B", repeat_index, 1000))
        for pressure_hpa, gas_map in endpoint_map.items():
            for gas_ppm, ratio in gas_map.items():
                pressure.append(_pressure_summary("B", 3, repeat_index, gas_ppm, pressure_hpa, ratio))
        gates.append(_phase_gate("B", 1, repeat_index, "gas_flush_vent_on", "flush_gate", "pass"))
    return flush, holds, pressure, gates


def test_analyze_metrology_diagnostic_passes_for_clean_endpoint_screening() -> None:
    flush, holds, pressure, gates = _pass_dataset()

    result = analyze_room_temp_diagnostic(flush, holds, pressure, phase_gate_rows=gates)

    assert result["classification"] == "pass"
    assert result["recommended_variant"] == "B"
    assert result["eligible_for_layer4"] is True


def test_deadtime_deviation_is_warn_not_hard_fail_without_other_breakage() -> None:
    flush, holds, pressure, gates = _pass_dataset()
    for row in flush:
        row["actual_deadtime_s"] = 2.9

    result = analyze_room_temp_diagnostic(flush, holds, pressure, phase_gate_rows=gates)
    variant = result["variant_summaries"][0]
    deadtime_metric = next(item for item in variant["metrics"] if item["name"] == "gas_switch_deadtime_check")

    assert deadtime_metric["status"] == "warn"
    assert result["classification"] == "warn"


def test_flush_vent_off_is_hard_fail() -> None:
    flush, holds, pressure, gates = _pass_dataset()
    flush[0]["vent_state_during_flush"] = "VENT_OFF"

    result = analyze_room_temp_diagnostic(flush, holds, pressure, phase_gate_rows=gates)

    assert result["classification"] == "fail"
    variant = result["variant_summaries"][0]
    metric = next(item for item in variant["metrics"] if item["name"] == "flush_vent_state_check")
    assert metric["status"] == "fail"


def test_insufficient_evidence_when_stable_sample_count_below_min() -> None:
    flush, holds, pressure, gates = _pass_dataset()
    for row in pressure:
        row["stable_sample_count"] = 6
        row["analyzer_raw_sample_count"] = 6
        row["gauge_raw_sample_count"] = 6
        row["dewpoint_raw_sample_count"] = 6
        row["aligned_sample_count"] = 6
        row["phase_status"] = "insufficient_evidence"
        row["warning_flags"] = ["stable_sample_count_below_min", "insufficient_analyzer_samples"]

    result = analyze_room_temp_diagnostic(flush, holds, pressure, phase_gate_rows=gates)

    assert result["classification"] == "insufficient_evidence"
    assert result["missing_evidence"]
    assert result["recommended_variant"] is None


def test_recommended_variant_is_protected_when_all_variants_fail() -> None:
    flush, holds, pressure, gates = _pass_dataset()
    for row in flush:
        row["process_variant"] = "A"
        row["vent_state_during_flush"] = "VENT_OFF"
    for row in holds:
        row["process_variant"] = "A"
        row["phase_status"] = "fail"
    for row in pressure:
        row["process_variant"] = "A"
        row["phase_status"] = "fail"
        row["warning_flags"] = ["ratio_window_not_flat"]
    for row in gates:
        row["process_variant"] = "A"

    result = analyze_room_temp_diagnostic(flush, holds, pressure, phase_gate_rows=gates)

    assert result["recommended_variant"] is None
    assert result["recommendation_confidence"] == "low"
    assert "insufficient evidence" in result["recommendation_reason"]


def test_parse_args_smoke_presets_apply_expected_defaults() -> None:
    args = parse_args(["--allow-live-hardware", "--smoke-level", "s2"])

    assert args.variants == "B"
    assert args.layers == "1,2"
    assert args.repeats == 1
    assert args.early_stop is True
    assert args.treat_insufficient_as_stop is True


def test_parse_args_s1_recheck_enables_precondition() -> None:
    args = parse_args(["--allow-live-hardware", "--smoke-level", "s1-recheck"])

    assert args.variants == "B"
    assert args.layers == "1"
    assert args.repeats == 1
    assert args.enable_precondition is True


def test_parse_args_precondition_only_forces_precondition() -> None:
    args = parse_args(["--allow-live-hardware", "--precondition-only", "true"])

    assert args.precondition_only is True
    assert args.enable_precondition is True


def test_parse_args_analyzer_chain_isolation_preset() -> None:
    args = parse_args(
        [
            "--allow-live-hardware",
            "--smoke-level",
            "analyzer-chain-isolation",
            "--chain-mode",
            "analyzer_out_keep_rest",
        ]
    )

    assert args.variants == "B"
    assert args.precondition_only is True
    assert args.enable_precondition is True
    assert args.analyzer_count_in_path == 0


def test_parse_args_analyzer_chain_isolation_a1p0_defaults_analyzer_count() -> None:
    args = parse_args(
        [
            "--allow-live-hardware",
            "--smoke-level",
            "analyzer-chain-isolation",
            "--chain-mode",
            "analyzer_in_pace_out_keep_rest",
        ]
    )

    assert args.variants == "B"
    assert args.precondition_only is True
    assert args.enable_precondition is True
    assert args.analyzer_count_in_path == 8


def test_parse_args_analyzer_chain_isolation_a0p0_defaults_analyzer_count() -> None:
    args = parse_args(
        [
            "--allow-live-hardware",
            "--smoke-level",
            "analyzer-chain-isolation",
            "--chain-mode",
            "analyzer_out_pace_out_keep_rest",
        ]
    )

    assert args.variants == "B"
    assert args.precondition_only is True
    assert args.enable_precondition is True
    assert args.analyzer_count_in_path == 0


def test_load_cli_config_supports_base_config_overlay(tmp_path) -> None:
    project_root = Path(__file__).resolve().parents[1]
    overlay_path = tmp_path / "overlay.json"
    overlay_path.write_text(
        (
            '{\n'
            f'  "base_config": "{(project_root / "configs" / "analyzer_chain_isolation_4ch.json").as_posix()}",\n'
            '  "diagnostics": {\n'
            '    "analyzer_chain_isolation": {\n'
            '      "compare_vs_baseline_reference_dir": "results/diagnostics/analyzer_chain_isolation_4ch/20260403_005607"\n'
            "    },\n"
            '    "precondition": {\n'
            '      "closed_pressure_swing": {\n'
            '        "enabled": true,\n'
            '        "cycles": 1,\n'
            '        "high_pressure_hpa": 1105,\n'
            '        "low_pressure_hpa": 500,\n'
            '        "linear_slew_hpa_per_s": 20\n'
            "      }\n"
            "    }\n"
            "  }\n"
            "}\n"
        ),
        encoding="utf-8",
    )

    cfg = _load_cli_config(overlay_path)

    assert cfg["devices"]["gas_analyzers"][0]["name"] == "ga01"
    assert cfg["diagnostics"]["precondition"]["closed_pressure_swing"]["enabled"] is True
    assert cfg["diagnostics"]["precondition"]["closed_pressure_swing"]["linear_slew_hpa_per_s"] == 20
    assert (
        cfg["diagnostics"]["analyzer_chain_isolation"]["compare_vs_baseline_reference_dir"]
        == "results/diagnostics/analyzer_chain_isolation_4ch/20260403_005607"
    )


def test_evaluate_flush_gate_can_skip_vent_requirement_for_a1p0() -> None:
    start = datetime(2026, 4, 2, 8, 0, 0)
    rows = []
    for second in range(180):
        rows.append(
            {
                "timestamp": (start + timedelta(seconds=second)).isoformat(timespec="seconds"),
                "phase_elapsed_s": float(second),
                "analyzer2_co2_ratio": 1.4000 + (0.0001 if second % 2 else -0.0001),
                "gauge_pressure_hpa": 1007.0 + second * 0.0001,
                "dewpoint_c": -30.0 + second * 0.0001,
                "controller_vent_state": "NOT_APPLICABLE",
            }
        )

    gate = evaluate_flush_gate(
        rows,
        min_flush_s=120.0,
        target_flush_s=120.0,
        require_ratio=True,
        require_vent_on=False,
    )

    assert gate["gate_pass"] is True
    assert "flush_not_all_vent_on" not in gate["failing_subgates"]


def test_build_flush_summary_exposes_shared_dewpoint_gate_alias_fields() -> None:
    start = datetime(2026, 4, 2, 8, 0, 0)
    rows = []
    for second in range(180):
        rows.append(
            {
                "timestamp": (start + timedelta(seconds=second)).isoformat(timespec="seconds"),
                "phase_elapsed_s": float(second),
                "analyzer2_co2_ratio": 1.4000 + (0.0001 if second % 2 else -0.0001),
                "gauge_pressure_hpa": 1007.0 + second * 0.0001,
                "dewpoint_c": -30.0 + second * 0.0001,
                "controller_vent_state": "VENT_ON",
            }
        )

    summary = build_flush_summary(
        rows,
        process_variant="B",
        layer=1,
        repeat_index=1,
        gas_ppm=0,
        actual_deadtime_s=2.0,
        min_flush_s=120.0,
        target_flush_s=120.0,
        require_ratio=True,
        require_vent_on=True,
    )

    assert summary["flush_gate_status"] == "pass"
    assert summary["flush_gate_reason"] == ""
    assert summary["dewpoint_time_to_gate"] is not None
    assert summary["dewpoint_tail_span_60s"] == summary["flush_last60s_dewpoint_span"]
    assert summary["dewpoint_tail_slope_60s"] == summary["flush_last60s_dewpoint_slope"]


def test_build_flush_summary_marks_dewpoint_rebound_without_new_actuation() -> None:
    start = datetime(2026, 4, 2, 8, 0, 0)
    rows = []
    for second in range(220):
        if second <= 110:
            dew = -30.0 - second * 0.04
        elif second <= 170:
            dew = -34.4 + (second - 110) * 0.03
        else:
            dew = -32.6 - (second - 170) * 0.01
        rows.append(
            {
                "timestamp": (start + timedelta(seconds=second)).isoformat(timespec="seconds"),
                "elapsed_s": float(second),
                "phase_elapsed_s": float(second),
                "process_variant": "B",
                "layer": 0,
                "repeat_index": 0,
                "phase": "precondition_vent_on",
                "gas_ppm": 0,
                "pressure_target_hpa": None,
                "analyzer2_co2_ratio": 1.4000 + (0.0003 if second % 2 else -0.0002),
                "gauge_pressure_hpa": 1015.0 + second * 0.0005,
                "dewpoint_c": dew,
                "dewpoint_temp_c": 22.0 + second * 0.0001,
                "dewpoint_rh_percent": 5.0 + second * 0.0002,
                "controller_pressure_hpa": 1015.2,
                "controller_vent_state": "VENT_ON",
            }
        )

    summary = build_flush_summary(
        rows,
        process_variant="B",
        layer=0,
        repeat_index=0,
        gas_ppm=0,
        actual_deadtime_s=0.0,
        actuation_events=[
            {
                "timestamp": start.isoformat(timespec="seconds"),
                "run_id": "test",
                "process_variant": "B",
                "layer": 0,
                "repeat_index": 0,
                "gas_ppm": 0,
                "pressure_target_hpa": None,
                "event_type": "vent_on",
                "event_value": "VENT_ON",
                "note": "precondition start",
            }
        ],
        min_flush_s=180.0,
        target_flush_s=180.0,
        gate_window_s=60.0,
        rebound_window_s=180.0,
        rebound_min_rise_c=1.0,
    )

    assert summary["dewpoint_rebound_detected"] is True
    assert summary["rebound_rise_c"] is not None
    assert summary["rebound_rise_c"] > 1.0
    assert "dewpoint_rebound_detected" in summary["flush_warning_flags"]


def test_closed_pressure_swing_predry_fails_fast_when_vent_closed_unverified() -> None:
    class FakePace:
        def __init__(self) -> None:
            self.setpoints = []

        def get_vent_status(self) -> int:
            return 1

        def get_output_state(self) -> int:
            return 1

        def get_isolation_state(self) -> int:
            return 1

        def set_setpoint(self, value_hpa: float) -> None:
            self.setpoints.append(value_hpa)

    class FakeRunner:
        def __init__(self) -> None:
            self.devices = {"pace": FakePace()}
            self._pressure_atmosphere_hold_enabled = False
            self._pressure_atmosphere_hold_strategy = ""
            self._pace_vent_after_valve_open = False

        def _set_pressure_controller_vent(self, vent_on: bool, reason: str = "") -> bool:
            return True

        def _enable_pressure_controller_output(self, reason: str = "") -> bool:
            return True

        def _pressure_snapshot(self):
            return {}

        def _pressure_controller_output_on_failures(self, snapshot, pace):
            return ["vent_status=1"]

    cfg = {
        "diagnostics": {
            "precondition": {
                "closed_pressure_swing": {
                    "enabled": True,
                    "cycles": 1,
                    "high_pressure_hpa": 1105,
                    "low_pressure_hpa": 500,
                    "low_hold_s": 10,
                    "settle_after_repressurize_s": 5,
                    "require_vent_closed_verified": True,
                    "max_total_extra_s": 120,
                }
            }
        }
    }

    rows, trace_rows, state = _run_closed_pressure_swing_predry(
        FakeRunner(),
        analyzer=None,
        devices={},
        cfg=cfg,
        process_variant="B",
        layer=0,
        repeat_index=1,
        gas_ppm=0,
        route={"group": "A", "source_valve": 1, "path_valve": 7},
        gas_start_mono=0.0,
        sample_poll_s=1.0,
        print_every_s=10.0,
        actuation_events=[],
        run_id="test_run",
        chain_mode="analyzer_in_keep_rest",
    )

    assert rows == []
    assert trace_rows == []
    assert state["closed_pressure_swing_cycles_completed"] == 0
    assert state["closed_pressure_swing_abort_reason"].startswith("vent_closed_not_verified:")


def test_closed_pressure_swing_predry_preserves_vent_closed_verified_on_target_abort(monkeypatch) -> None:
    class FakePace:
        def __init__(self) -> None:
            self.setpoints = []

        def get_vent_status(self) -> int:
            return 3

        def get_output_state(self) -> int:
            return 1

        def get_isolation_state(self) -> int:
            return 1

        def set_setpoint(self, value_hpa: float) -> None:
            self.setpoints.append(value_hpa)

    class FakeRunner:
        def __init__(self) -> None:
            self.devices = {"pace": FakePace()}
            self._pressure_atmosphere_hold_enabled = False
            self._pressure_atmosphere_hold_strategy = ""
            self._pace_vent_after_valve_open = False

        def _set_pressure_controller_vent(self, vent_on: bool, reason: str = "") -> bool:
            return True

        def _enable_pressure_controller_output(self, reason: str = "") -> bool:
            return True

        def _pressure_snapshot(self):
            return {}

        def _pressure_controller_output_on_failures(self, snapshot, pace):
            return []

    def _fake_capture_until_pressure_target(*args, **kwargs):
        target = kwargs.get("target_hpa")
        return [], float(kwargs.get("phase_elapsed_offset_s", 0.0)), None, False, f"pressure_target_not_reached:{target}"

    monkeypatch.setattr(room_temp_diag_tool, "_capture_until_pressure_target", _fake_capture_until_pressure_target)

    cfg = {
        "diagnostics": {
            "precondition": {
                "closed_pressure_swing": {
                    "enabled": True,
                    "cycles": 1,
                    "high_pressure_hpa": 1105,
                    "low_pressure_hpa": 500,
                    "low_hold_s": 10,
                    "settle_after_repressurize_s": 5,
                    "require_vent_closed_verified": True,
                    "max_total_extra_s": 120,
                }
            }
        }
    }

    rows, trace_rows, state = _run_closed_pressure_swing_predry(
        FakeRunner(),
        analyzer=None,
        devices={},
        cfg=cfg,
        process_variant="B",
        layer=0,
        repeat_index=1,
        gas_ppm=0,
        route={"group": "A", "source_valve": 1, "path_valve": 7},
        gas_start_mono=0.0,
        sample_poll_s=1.0,
        print_every_s=10.0,
        actuation_events=[],
        run_id="test_run",
        chain_mode="analyzer_in_keep_rest",
    )

    assert rows == []
    assert state["closed_pressure_swing_vent_closed_verified"] is True
    assert state["closed_pressure_swing_abort_reason"] == "pressure_target_not_reached:1105.0"
    assert trace_rows[0]["output_state_before"] == 1
    assert trace_rows[0]["isolation_state_before"] == 1
    assert trace_rows[0]["output_state_after"] == 1
    assert trace_rows[0]["isolation_state_after"] == 1


def test_closed_pressure_swing_predry_closes_source_during_closed_volume_swing(monkeypatch) -> None:
    class FakePace:
        def __init__(self) -> None:
            self.setpoints = []

        def get_vent_status(self) -> int:
            return 3

        def get_output_state(self) -> int:
            return 1

        def get_isolation_state(self) -> int:
            return 1

        def set_setpoint(self, value_hpa: float) -> None:
            self.setpoints.append(value_hpa)

    class FakeRunner:
        def __init__(self) -> None:
            self.devices = {"pace": FakePace()}
            self.cfg = {"valves": {"h2o_path": 8, "gas_main": 11}}
            self._pressure_atmosphere_hold_enabled = False
            self._pressure_atmosphere_hold_strategy = ""
            self._pace_vent_after_valve_open = False
            self.applied_states = []
            self._relay_state_cache = {}

        def _set_pressure_controller_vent(self, vent_on: bool, reason: str = "") -> bool:
            return True

        def _enable_pressure_controller_output(self, reason: str = "") -> bool:
            return True

        def _pressure_snapshot(self):
            return {}

        def _pressure_controller_output_on_failures(self, snapshot, pace):
            return []

        def _resolve_valve_target(self, valve: int):
            return ("relay", int(valve))

        def _apply_valve_states(self, open_valves):
            self.applied_states.append(list(open_valves))
            open_set = set(int(v) for v in open_valves)
            for valve in (1, 7, 8, 11):
                self._relay_state_cache[("relay", valve)] = valve in open_set

    def _fake_capture_until_pressure_target(*args, **kwargs):
        target = float(kwargs.get("target_hpa"))
        context = kwargs.get("context") or {}
        phase = context.get("phase")
        if phase == "closed_pressure_swing_high_pressurize":
            return [], 1.0, "2026-04-04T13:00:01.000", True, ""
        if phase == "closed_pressure_swing_low_pressurize":
            return [], 2.0, "2026-04-04T13:00:02.000", True, ""
        if phase == "closed_pressure_swing_repressurize":
            return [], 3.0, "2026-04-04T13:00:03.000", True, ""
        return [], float(kwargs.get("phase_elapsed_offset_s", 0.0)), None, False, f"pressure_target_not_reached:{target}"

    monkeypatch.setattr(room_temp_diag_tool, "_capture_until_pressure_target", _fake_capture_until_pressure_target)
    monkeypatch.setattr(room_temp_diag_tool, "_capture_phase_rows", lambda *args, **kwargs: [])

    runner = FakeRunner()
    actuation_events = []
    cfg = {
        "diagnostics": {
            "precondition": {
                "closed_pressure_swing": {
                    "enabled": True,
                    "cycles": 1,
                    "high_pressure_hpa": 1105,
                    "low_pressure_hpa": 500,
                    "low_hold_s": 10,
                    "settle_after_repressurize_s": 5,
                    "require_vent_closed_verified": True,
                    "max_total_extra_s": 120,
                }
            }
        }
    }
    route = {"group": "A", "source_valve": 1, "path_valve": 7, "open_logical_valves": [8, 11, 7, 1]}

    rows, trace_rows, state = _run_closed_pressure_swing_predry(
        runner,
        analyzer=None,
        devices={},
        cfg=cfg,
        process_variant="B",
        layer=0,
        repeat_index=1,
        gas_ppm=0,
        route=route,
        gas_start_mono=0.0,
        sample_poll_s=1.0,
        print_every_s=10.0,
        actuation_events=actuation_events,
        run_id="test_run",
        chain_mode="analyzer_in_keep_rest",
    )

    assert rows == []
    assert state["closed_pressure_swing_cycles_completed"] == 1
    assert [7] in runner.applied_states
    assert [8, 11, 7, 1] in runner.applied_states
    event_types = [event["event_type"] for event in actuation_events]
    assert "gas_source_close" in event_types
    assert event_types.count("gas_source_open") >= 1
    assert trace_rows[0]["vent_state_before"] == 3
    assert trace_rows[0]["output_state_before"] == 1
    assert trace_rows[0]["isolation_state_before"] == 1


def test_closed_swing_open_valves_closes_upstream_feed_path() -> None:
    class FakeRunner:
        def __init__(self) -> None:
            self.cfg = {"valves": {"h2o_path": 8, "gas_main": 11}}

    route = {"source_valve": 1, "path_valve": 7, "open_logical_valves": [8, 11, 7, 1]}

    closed_open = room_temp_diag_tool._closed_swing_open_valves(FakeRunner(), route)

    assert closed_open == [7]


def test_closed_swing_feed_close_failures_require_upstream_closed_and_path_open() -> None:
    class FakeRunner:
        def __init__(self) -> None:
            self.cfg = {"valves": {"h2o_path": 8, "gas_main": 11}}
            self._relay_state_cache = {
                ("relay", 8): False,
                ("relay", 11): False,
                ("relay", 1): False,
                ("relay", 7): True,
            }

        def _resolve_valve_target(self, valve: int):
            return ("relay", int(valve))

    route = {"source_valve": 1, "path_valve": 7}

    failures = room_temp_diag_tool._closed_swing_feed_close_failures(FakeRunner(), route)

    assert failures == []


def test_closed_pressure_swing_state_snapshot_accepts_trapped_pressure_as_closed() -> None:
    class FakePace:
        VENT_STATUS_TRAPPED_PRESSURE = 3

        def get_vent_status(self) -> int:
            return 3

        def get_output_state(self) -> int:
            return 1

        def get_isolation_state(self) -> int:
            return 1

    class FakeRunner:
        _pressure_atmosphere_hold_enabled = False
        _pressure_atmosphere_hold_strategy = ""
        _pace_vent_after_valve_open = False

        def _pressure_snapshot(self):
            return {}

    snapshot, failures = room_temp_diag_tool._closed_pressure_swing_state_snapshot(FakeRunner(), FakePace())

    assert snapshot["pace_vent_status"] == 3
    assert failures == []


def test_export_metrology_results_generates_expected_artifacts(tmp_path) -> None:
    flush, holds, pressure, gates = _pass_dataset()
    actuation_events = [
        {
            "timestamp": "2026-04-02T08:00:00",
            "run_id": "test_run",
            "process_variant": "B",
            "layer": 1,
            "repeat_index": 1,
            "gas_ppm": 0,
            "pressure_target_hpa": None,
            "event_type": "gas_source_open",
            "event_value": 1,
            "note": "test",
        }
    ]
    start = datetime(2026, 4, 2, 8, 0, 0)
    raw_rows = []
    for second in range(25):
        raw_rows.append(
            {
                "timestamp": (start + timedelta(seconds=second)).isoformat(timespec="seconds"),
                "elapsed_s": float(second),
                "phase_elapsed_s": float(second),
                "process_variant": "B",
                "layer": 1,
                "repeat_index": 1,
                "phase": "gas_flush_vent_on",
                "gas_ppm": 0,
                "pressure_target_hpa": None,
                "analyzer2_co2_ratio": 1.40 - second * 0.0002,
                "analyzer2_pressure_hpa": 1008.0,
                "gauge_pressure_hpa": 1007.5,
                "dewpoint_c": -30.0,
                "dewpoint_temp_c": 22.0,
                "dewpoint_rh_percent": 5.0,
                "controller_pressure_hpa": 1007.8,
                "controller_vent_state": "VENT_ON",
                "actual_deadtime_s": 2.0,
                "gate_pass": True,
                "gate_fail_reason": "",
            }
        )

    aligned_rows = build_aligned_rows(raw_rows, interval_s=1.0)
    diagnostic_summary = analyze_room_temp_diagnostic(flush, holds, pressure, phase_gate_rows=gates)
    outputs = export_room_temp_diagnostic_results(
        tmp_path,
        raw_rows=raw_rows,
        aligned_rows=aligned_rows,
        actuation_events=actuation_events,
        flush_gate_trace_rows=[
            {
                "timestamp": "2026-04-02T08:00:00",
                "elapsed_s_real": 180.0,
                "elapsed_s_display": 180.0,
                "process_variant": "B",
                "layer": 1,
                "repeat_index": 1,
                "gas_ppm": 0,
                "phase": "gas_flush_vent_on",
                "dewpoint_c": -30.0,
                "dewpoint_span_window_c": 0.1,
                "dewpoint_slope_window_c_per_s": 0.0002,
                "ratio_value": 1.4,
                "ratio_span_window": 0.001,
                "ratio_slope_window_per_s": 0.0001,
                "gauge_hpa": 1007.5,
                "gauge_span_window_hpa": 0.2,
                "gauge_slope_window_hpa_per_s": 0.001,
                "gate_pass": True,
                "failing_subgates": "",
                "note": "gate_pass",
            }
        ],
        precondition_summaries=[
            {
                "gas_ppm": 0,
                "flush_duration_s": 180.0,
                "precondition_status": "pass",
                "precondition_fail_reason": "",
                "dewpoint_rebound_detected": False,
                "rebound_note": "",
            }
        ],
        flush_summaries=flush,
        seal_hold_summaries=holds,
        pressure_summaries=pressure,
        phase_gate_rows=gates,
        diagnostic_summary=diagnostic_summary,
    )

    assert outputs["raw_timeseries"].exists()
    assert outputs["aligned_timeseries"].exists()
    assert outputs["actuation_events"].exists()
    assert outputs["flush_gate_trace"].exists()
    assert outputs["seal_hold_summary"].exists()
    assert outputs["phase_gate_summary"].exists()
    assert outputs["diagnostic_workbook"].exists()
    assert outputs["variant_comparison_summary"].exists()


def test_chain_isolation_comparison_flags_analyzer_chain_as_dominant() -> None:
    out_summary = build_analyzer_chain_isolation_summary(
        {
            "process_variant": "B",
            "flush_gate_status": "pass",
            "flush_gate_pass": True,
            "flush_gate_fail_reason": "",
            "flush_duration_s": 180.0,
            "flush_last60s_dewpoint_span": 0.12,
            "flush_last60s_dewpoint_slope": 0.0008,
            "flush_last60s_gauge_span_hpa": 0.02,
            "flush_last60s_gauge_slope_hpa_per_s": 0.0002,
            "dewpoint_rebound_detected": False,
            "rebound_rise_c": None,
            "gauge_raw_sample_count": 60,
            "dewpoint_raw_sample_count": 60,
            "analyzer_raw_sample_count": 0,
            "aligned_sample_count": 60,
        },
        run_id="out_run",
        smoke_level="analyzer-chain-isolation",
        chain_mode="analyzer_out_keep_rest",
        setup_metadata={"analyzer_chain_connected": False, "analyzer_count_in_path": 0, "output_dir": "out"},
    )
    in_summary = build_analyzer_chain_isolation_summary(
        {
            "process_variant": "B",
            "flush_gate_status": "fail",
            "flush_gate_pass": False,
            "flush_gate_fail_reason": "dewpoint_tail_slope_too_large",
            "flush_duration_s": 300.0,
            "flush_last60s_dewpoint_span": 1.10,
            "flush_last60s_dewpoint_slope": 0.0100,
            "flush_last60s_gauge_span_hpa": 0.03,
            "flush_last60s_gauge_slope_hpa_per_s": 0.0002,
            "flush_last60s_ratio_span": 0.02,
            "flush_last60s_ratio_slope": 0.0015,
            "dewpoint_rebound_detected": True,
            "rebound_rise_c": 2.4,
            "gauge_raw_sample_count": 60,
            "dewpoint_raw_sample_count": 60,
            "analyzer_raw_sample_count": 60,
            "aligned_sample_count": 60,
        },
        run_id="in_run",
        smoke_level="analyzer-chain-isolation",
        chain_mode="analyzer_in_keep_rest",
        setup_metadata={"analyzer_chain_connected": True, "analyzer_count_in_path": 8, "output_dir": "in"},
    )

    comparison = build_analyzer_chain_isolation_comparison([out_summary, in_summary])

    assert comparison["dominant_isolation_conclusion"] == "analyzer_chain_moisture_memory_suspicion"
    assert comparison["should_continue_s1"] is False


def test_pace_contribution_comparison_marks_a1p0_as_significantly_better() -> None:
    a1p1 = build_analyzer_chain_isolation_summary(
        {
            "process_variant": "B",
            "flush_gate_status": "fail",
            "flush_gate_pass": False,
            "flush_gate_fail_reason": "max_flush_timeout;dewpoint_tail_slope_too_large",
            "flush_duration_s": 900.0,
            "flush_last60s_dewpoint_span": 0.31,
            "flush_last60s_dewpoint_slope": -0.0053,
            "flush_last60s_gauge_span_hpa": 0.03,
            "flush_last60s_gauge_slope_hpa_per_s": 0.0002,
            "flush_last60s_ratio_span": 0.007,
            "flush_last60s_ratio_slope": 0.0008,
            "dewpoint_rebound_detected": True,
            "rebound_rise_c": 1.8,
            "gauge_raw_sample_count": 60,
            "dewpoint_raw_sample_count": 60,
            "analyzer_raw_sample_count": 60,
            "aligned_sample_count": 60,
        },
        run_id="a1p1",
        smoke_level="analyzer-chain-isolation",
        chain_mode="analyzer_in_keep_rest",
        setup_metadata={"analyzer_chain_connected": True, "analyzer_count_in_path": 8, "pace_in_path": True, "output_dir": "a1p1"},
    )
    a1p0 = build_analyzer_chain_isolation_summary(
        {
            "process_variant": "B",
            "flush_gate_status": "pass",
            "flush_gate_pass": True,
            "flush_gate_fail_reason": "",
            "flush_duration_s": 260.0,
            "flush_last60s_dewpoint_span": 0.12,
            "flush_last60s_dewpoint_slope": -0.0011,
            "flush_last60s_gauge_span_hpa": 0.02,
            "flush_last60s_gauge_slope_hpa_per_s": 0.0002,
            "flush_last60s_ratio_span": 0.004,
            "flush_last60s_ratio_slope": 0.0004,
            "dewpoint_rebound_detected": False,
            "rebound_rise_c": 0.3,
            "gauge_raw_sample_count": 60,
            "dewpoint_raw_sample_count": 60,
            "analyzer_raw_sample_count": 60,
            "aligned_sample_count": 60,
        },
        run_id="a1p0",
        smoke_level="analyzer-chain-isolation",
        chain_mode="analyzer_in_pace_out_keep_rest",
        setup_metadata={
            "analyzer_chain_connected": True,
            "analyzer_count_in_path": 8,
            "pace_in_path": False,
            "controller_vent_expected": False,
            "controller_vent_state": "NOT_APPLICABLE",
            "output_dir": "a1p0",
        },
    )

    comparison = build_analyzer_chain_pace_contribution_comparison([a1p0, a1p1])

    assert comparison["comparison_available"] is True
    assert comparison["pace_contribution_assessment"] == "significant_additional_contributor"


def test_compare_vs_8ch_marks_partial_improvement_when_a1p1_timeout_is_cleared() -> None:
    current_a1p0 = build_analyzer_chain_isolation_summary(
        {
            "process_variant": "B",
            "flush_gate_status": "pass",
            "flush_gate_pass": True,
            "flush_gate_fail_reason": "",
            "flush_duration_s": 320.0,
            "flush_last60s_dewpoint_span": 0.14,
            "flush_last60s_dewpoint_slope": -0.0018,
            "flush_last60s_gauge_span_hpa": 0.02,
            "flush_last60s_gauge_slope_hpa_per_s": 0.0002,
            "flush_last60s_ratio_span": 0.006,
            "flush_last60s_ratio_slope": 0.0005,
            "dewpoint_rebound_detected": False,
            "rebound_rise_c": None,
            "gauge_raw_sample_count": 60,
            "dewpoint_raw_sample_count": 60,
            "analyzer_raw_sample_count": 60,
            "aligned_sample_count": 60,
        },
        run_id="current_a1p0",
        smoke_level="analyzer-chain-isolation",
        chain_mode="analyzer_in_pace_out_keep_rest",
        setup_metadata={"analyzer_chain_connected": True, "analyzer_count_in_path": 4, "pace_in_path": False, "output_dir": "current_a1p0"},
    )
    current_a1p1 = build_analyzer_chain_isolation_summary(
        {
            "process_variant": "B",
            "flush_gate_status": "pass",
            "flush_gate_pass": True,
            "flush_gate_fail_reason": "",
            "flush_duration_s": 540.0,
            "flush_last60s_dewpoint_span": 0.22,
            "flush_last60s_dewpoint_slope": -0.0032,
            "flush_last60s_gauge_span_hpa": 0.03,
            "flush_last60s_gauge_slope_hpa_per_s": 0.0002,
            "flush_last60s_ratio_span": 0.007,
            "flush_last60s_ratio_slope": 0.0007,
            "dewpoint_rebound_detected": False,
            "rebound_rise_c": None,
            "gauge_raw_sample_count": 60,
            "dewpoint_raw_sample_count": 60,
            "analyzer_raw_sample_count": 60,
            "aligned_sample_count": 60,
        },
        run_id="current_a1p1",
        smoke_level="analyzer-chain-isolation",
        chain_mode="analyzer_in_keep_rest",
        setup_metadata={"analyzer_chain_connected": True, "analyzer_count_in_path": 4, "pace_in_path": True, "output_dir": "current_a1p1"},
    )
    baseline_a1p0 = build_analyzer_chain_isolation_summary(
        {
            "process_variant": "B",
            "flush_gate_status": "pass",
            "flush_gate_pass": True,
            "flush_gate_fail_reason": "",
            "flush_duration_s": 536.0,
            "flush_last60s_dewpoint_span": 0.16,
            "flush_last60s_dewpoint_slope": -0.0026,
            "flush_last60s_gauge_span_hpa": 0.02,
            "flush_last60s_gauge_slope_hpa_per_s": 0.0002,
            "flush_last60s_ratio_span": 0.006,
            "flush_last60s_ratio_slope": 0.0006,
            "dewpoint_rebound_detected": False,
            "rebound_rise_c": None,
            "gauge_raw_sample_count": 60,
            "dewpoint_raw_sample_count": 60,
            "analyzer_raw_sample_count": 60,
            "aligned_sample_count": 60,
        },
        run_id="baseline_a1p0",
        smoke_level="analyzer-chain-isolation",
        chain_mode="analyzer_in_pace_out_keep_rest",
        setup_metadata={"analyzer_chain_connected": True, "analyzer_count_in_path": 8, "pace_in_path": False, "output_dir": "baseline_a1p0"},
    )
    baseline_a1p1 = build_analyzer_chain_isolation_summary(
        {
            "process_variant": "B",
            "flush_gate_status": "fail",
            "flush_gate_pass": False,
            "flush_gate_fail_reason": "max_flush_timeout;dewpoint_tail_slope_too_large",
            "flush_duration_s": 899.0,
            "flush_last60s_dewpoint_span": 0.31,
            "flush_last60s_dewpoint_slope": -0.00533,
            "flush_last60s_gauge_span_hpa": 0.03,
            "flush_last60s_gauge_slope_hpa_per_s": 0.0002,
            "flush_last60s_ratio_span": 0.007,
            "flush_last60s_ratio_slope": 0.0008,
            "dewpoint_rebound_detected": False,
            "rebound_rise_c": None,
            "gauge_raw_sample_count": 60,
            "dewpoint_raw_sample_count": 60,
            "analyzer_raw_sample_count": 60,
            "aligned_sample_count": 60,
        },
        run_id="baseline_a1p1",
        smoke_level="analyzer-chain-isolation",
        chain_mode="analyzer_in_keep_rest",
        setup_metadata={"analyzer_chain_connected": True, "analyzer_count_in_path": 8, "pace_in_path": True, "output_dir": "baseline_a1p1"},
    )

    comparison = build_analyzer_chain_compare_vs_8ch(
        [current_a1p0, current_a1p1],
        [baseline_a1p0, baseline_a1p1],
    )

    assert comparison["overall_assessment"] == "明显改善"
    assert comparison["a1p1_timeout_cleared_vs_8ch"] is True
    assert comparison["a1p0_improvement_seconds_vs_8ch"] is not None


def test_compare_vs_baseline_marks_worth_continuing_when_gain_is_clear() -> None:
    current_a1p1 = build_analyzer_chain_isolation_summary(
        {
            "process_variant": "B",
            "flush_gate_status": "pass",
            "flush_gate_pass": True,
            "flush_gate_fail_reason": "",
            "flush_duration_s": 180.0,
            "flush_last60s_dewpoint_span": 0.10,
            "flush_last60s_dewpoint_slope": 0.0008,
            "flush_last60s_gauge_span_hpa": 0.02,
            "flush_last60s_gauge_slope_hpa_per_s": 0.0002,
            "flush_last60s_ratio_span": 0.006,
            "flush_last60s_ratio_slope": 0.0005,
            "dewpoint_rebound_detected": False,
            "rebound_rise_c": None,
            "gauge_raw_sample_count": 60,
            "dewpoint_raw_sample_count": 60,
            "analyzer_raw_sample_count": 60,
            "aligned_sample_count": 60,
        },
        run_id="current_a1p1",
        smoke_level="analyzer-chain-isolation",
        chain_mode="analyzer_in_keep_rest",
        setup_metadata={
            "analyzer_chain_connected": True,
            "analyzer_count_in_path": 4,
            "pace_in_path": True,
            "closed_pressure_swing_enabled": True,
            "closed_pressure_swing_cycles_requested": 1,
            "closed_pressure_swing_cycles_completed": 1,
            "closed_pressure_swing_vent_closed_verified": True,
            "closed_pressure_swing_abort_reason": "",
            "closed_pressure_swing_total_extra_s": 55.0,
            "extra_precondition_strategy_used": "closed_pressure_swing_predry",
            "extra_precondition_time_cost_s": 55.0,
            "output_dir": "current_a1p1",
        },
    )
    baseline_a1p1 = build_analyzer_chain_isolation_summary(
        {
            "process_variant": "B",
            "flush_gate_status": "pass",
            "flush_gate_pass": True,
            "flush_gate_fail_reason": "",
            "flush_duration_s": 226.997,
            "flush_last60s_dewpoint_span": 0.11,
            "flush_last60s_dewpoint_slope": 0.00093,
            "flush_last60s_gauge_span_hpa": 0.02,
            "flush_last60s_gauge_slope_hpa_per_s": 0.0002,
            "flush_last60s_ratio_span": 0.006,
            "flush_last60s_ratio_slope": 0.0005,
            "dewpoint_rebound_detected": False,
            "rebound_rise_c": None,
            "gauge_raw_sample_count": 60,
            "dewpoint_raw_sample_count": 60,
            "analyzer_raw_sample_count": 60,
            "aligned_sample_count": 60,
        },
        run_id="baseline_a1p1",
        smoke_level="analyzer-chain-isolation",
        chain_mode="analyzer_in_keep_rest",
        setup_metadata={"analyzer_chain_connected": True, "analyzer_count_in_path": 4, "pace_in_path": True, "output_dir": "baseline_a1p1"},
    )

    comparison = build_analyzer_chain_compare_vs_baseline([current_a1p1], [baseline_a1p1])

    assert comparison["comparison_available"] is True
    assert comparison["worth_continuing"] is True
    assert comparison["recommendation"] == "continue_to_2cycles"


def test_export_chain_isolation_results_generates_expected_artifacts(tmp_path) -> None:
    start = datetime(2026, 4, 2, 8, 0, 0)
    raw_rows = []
    for second in range(20):
        raw_rows.append(
            {
                "timestamp": (start + timedelta(seconds=second)).isoformat(timespec="seconds"),
                "phase_elapsed_s": float(second),
                "process_variant": "B",
                "layer": 0,
                "repeat_index": 1,
                "phase": "isolation_flush_vent_on",
                "gas_ppm": 0,
                "chain_mode": "analyzer_out_keep_rest",
                "gauge_pressure_hpa": 1007.0,
                "dewpoint_c": -30.0 + second * 0.001,
                "dewpoint_temp_c": 22.0,
                "dewpoint_rh_percent": 5.0,
                "controller_vent_state": "VENT_ON",
            }
        )
    isolation_summary = build_analyzer_chain_isolation_summary(
        {
            "process_variant": "B",
            "flush_gate_status": "warn",
            "flush_gate_pass": False,
            "flush_gate_fail_reason": "dewpoint_tail_slope_too_large",
            "flush_duration_s": 300.0,
            "flush_last60s_dewpoint_span": 0.4,
            "flush_last60s_dewpoint_slope": 0.004,
            "flush_last60s_gauge_span_hpa": 0.04,
            "flush_last60s_gauge_slope_hpa_per_s": 0.0002,
            "dewpoint_rebound_detected": True,
            "rebound_rise_c": 1.2,
            "gauge_raw_sample_count": 20,
            "dewpoint_raw_sample_count": 20,
            "analyzer_raw_sample_count": 0,
            "aligned_sample_count": 20,
        },
        run_id="chain_run",
        smoke_level="analyzer-chain-isolation",
        chain_mode="analyzer_out_keep_rest",
        setup_metadata={"analyzer_chain_connected": False, "analyzer_count_in_path": 0, "output_dir": str(tmp_path)},
    )
    comparison_summary = build_analyzer_chain_isolation_comparison([isolation_summary])
    comparison_summary["pace_vs_standard_in_comparison"] = {
        "comparison_available": True,
        "pace_contribution_assessment": "minor_additional_contributor",
        "classification_candidate": "pass",
        "classification_reference": "warn",
        "flush_gate_status_candidate": "pass",
        "flush_gate_status_reference": "warn",
        "dewpoint_time_to_gate_candidate": 210.0,
        "dewpoint_time_to_gate_reference": 300.0,
        "dewpoint_tail_span_60s_candidate": 0.12,
        "dewpoint_tail_span_60s_reference": 0.40,
        "dewpoint_tail_slope_60s_candidate": -0.0010,
        "dewpoint_tail_slope_60s_reference": -0.0040,
        "dewpoint_rebound_detected_candidate": False,
        "dewpoint_rebound_detected_reference": True,
        "rebound_rise_c_candidate": 0.3,
        "rebound_rise_c_reference": 1.2,
    }
    compare_vs_8ch_summary = {
        "overall_assessment": "部分改善",
        "conclusion": "4 台串路已有改善，但 residual contribution 仍在。",
        "rows": [
            {
                "case": "A1P0",
                "current_outcome": "pass",
                "current_time_to_gate_s": 320.0,
                "current_rebound_detected": False,
                "current_dewpoint_tail_slope_60s": -0.0018,
                "current_dewpoint_tail_span_60s": 0.14,
                "current_timeout": False,
                "baseline_outcome_8ch": "pass",
                "baseline_time_to_gate_s_8ch": 536.0,
                "baseline_rebound_detected_8ch": False,
                "baseline_dewpoint_tail_slope_60s_8ch": -0.0026,
                "baseline_dewpoint_tail_span_60s_8ch": 0.16,
                "baseline_timeout_8ch": False,
                "time_to_gate_improvement_s": 216.0,
                "time_to_gate_improvement_pct": 0.4029850746,
            }
        ],
        "a1p0_improvement_seconds_vs_8ch": 216.0,
        "a1p0_improvement_pct_vs_8ch": 0.4029850746,
        "a1p1_timeout_cleared_vs_8ch": True,
        "a1p1_slope_improvement_pct_vs_8ch": 0.3996257036,
    }
    compare_vs_baseline_summary = {
        "comparison_available": True,
        "worth_continuing": False,
        "recommendation": "do_not_continue",
        "conclusion": "改善不明显，不建议纳入流程。",
        "time_to_gate_improvement_s": -20.0,
        "time_to_gate_improvement_pct": -0.088,
        "tail_span_not_worse": False,
        "tail_slope_not_worse": False,
        "rebound_still_false": False,
        "closed_pressure_swing_vent_closed_verified": True,
        "closed_pressure_swing_abort_reason": "",
        "rows": [
            {
                "case": "A1P1",
                "classification": "pass",
                "flush_gate_status": "pass",
                "dewpoint_time_to_gate": 247.0,
                "dewpoint_tail_span_60s": 0.13,
                "dewpoint_tail_slope_60s": 0.0011,
                "dewpoint_rebound_detected": True,
                "timeout": False,
                "baseline_classification": "pass",
                "baseline_flush_gate_status": "pass",
                "baseline_dewpoint_time_to_gate": 226.997,
                "baseline_dewpoint_tail_span_60s": 0.11,
                "baseline_dewpoint_tail_slope_60s": 0.00093,
                "baseline_dewpoint_rebound_detected": False,
                "baseline_timeout": False,
                "extra_precondition_strategy_used": "closed_pressure_swing_predry",
                "extra_precondition_time_cost_s": 55.0,
                "closed_pressure_swing_vent_closed_verified": True,
                "closed_pressure_swing_abort_reason": "",
                "time_to_gate_improvement_s": -20.0,
                "time_to_gate_improvement_pct": -0.088,
                "tail_span_not_worse": False,
                "tail_slope_not_worse": False,
                "worth_continuing": False,
            }
        ],
    }
    outputs = export_analyzer_chain_isolation_results(
        tmp_path,
        raw_rows=raw_rows,
        flush_gate_trace_rows=[
            {
                "timestamp": start.isoformat(timespec="seconds"),
                "elapsed_s_real": 180.0,
                "elapsed_s_display": 180.0,
                "process_variant": "B",
                "layer": 0,
                "repeat_index": 1,
                "gas_ppm": 0,
                "chain_mode": "analyzer_out_keep_rest",
                "phase": "isolation_flush_vent_on",
                "dewpoint_c": -30.0,
                "dewpoint_span_window_c": 0.1,
                "dewpoint_slope_window_c_per_s": 0.001,
                "ratio_value": None,
                "ratio_span_window": None,
                "ratio_slope_window_per_s": None,
                "gauge_hpa": 1007.0,
                "gauge_span_window_hpa": 0.02,
                "gauge_slope_window_hpa_per_s": 0.0002,
                "gate_pass": False,
                "failing_subgates": "dewpoint_tail_slope_too_large",
                "note": "heartbeat",
            }
        ],
        actuation_events=[
            {
                "timestamp": start.isoformat(timespec="seconds"),
                "run_id": "chain_run",
                "process_variant": "B",
                "layer": 0,
                "repeat_index": 1,
                "gas_ppm": 0,
                "chain_mode": "analyzer_out_keep_rest",
                "pressure_target_hpa": None,
                "event_type": "vent_on",
                "event_value": "VENT_ON",
                "note": "test",
            }
        ],
        closed_pressure_swing_trace_rows=[
            {
                "cycle_index": 1,
                "start_ts": start.isoformat(timespec="seconds"),
                "reached_high_pressure_ts": (start + timedelta(seconds=5)).isoformat(timespec="seconds"),
                "reached_low_pressure_ts": (start + timedelta(seconds=15)).isoformat(timespec="seconds"),
                "repressurized_ts": (start + timedelta(seconds=25)).isoformat(timespec="seconds"),
                "vent_state_before": 0,
                "vent_state_during_low": 0,
                "vent_state_after": 0,
                "dewpoint_before_cycle": -30.0,
                "dewpoint_after_cycle": -30.2,
                "abort_reason": "",
            }
        ],
        setup_metadata={
            "run_id": "chain_run",
            "smoke_level": "analyzer-chain-isolation",
            "chain_mode": "analyzer_out_keep_rest",
            "analyzer_count_in_path": 0,
            "analyzer_chain_connected": False,
            "pace_in_path": True,
            "pace_expected_vent_on": True,
            "controller_vent_expected": True,
            "controller_vent_state": "VENT_ON",
            "valve_block_in_path": True,
            "dewpoint_meter_in_path": True,
            "gauge_in_path": True,
            "closed_pressure_swing_enabled": True,
            "closed_pressure_swing_cycles_requested": 1,
            "closed_pressure_swing_cycles_completed": 1,
            "closed_pressure_swing_high_pressure_hpa": 1105.0,
            "closed_pressure_swing_low_pressure_hpa": 500.0,
            "closed_pressure_swing_low_hold_s": 10.0,
            "closed_pressure_swing_vent_closed_verified": True,
            "closed_pressure_swing_abort_reason": "",
            "closed_pressure_swing_total_extra_s": 55.0,
            "extra_precondition_strategy_used": "closed_pressure_swing_predry",
            "extra_precondition_time_cost_s": 55.0,
            "setup_note": "",
            "operator_note": "",
            "output_dir": str(tmp_path),
        },
        isolation_summaries=[isolation_summary],
        comparison_summary=comparison_summary,
        operator_checklist="当前模式：analyzer_out_keep_rest",
        compare_vs_8ch_rows=compare_vs_8ch_summary["rows"],
        compare_vs_8ch_summary=compare_vs_8ch_summary,
        compare_vs_baseline_rows=compare_vs_baseline_summary["rows"],
        compare_vs_baseline_summary=compare_vs_baseline_summary,
    )

    assert outputs["summary"].exists()
    assert outputs["setup_metadata"].exists()
    assert outputs["isolation_summary"].exists()
    assert outputs["isolation_comparison_summary"].exists()
    assert outputs["compare_vs_8ch_csv"].exists()
    assert outputs["compare_vs_8ch_md"].exists()
    assert outputs["compare_vs_baseline_csv"].exists()
    assert outputs["compare_vs_baseline_md"].exists()
    assert outputs["closed_pressure_swing_trace"].exists()
    assert outputs["diagnostic_workbook"].exists()
    assert outputs["compare_vs_8ch_time_to_gate"].exists()
    assert outputs["dewpoint_time_series_analyzer_out_keep_rest"].exists()
    assert outputs["flush_gate_trace_overlay"].exists()
    assert "pace_contribution_assessment" in outputs["readable_report"].read_text(encoding="utf-8")


def test_pressure_point_summary_carries_runner_gate_timing_fields() -> None:
    transition_rows = [
        {
            "timestamp": "2026-04-02T08:20:20.000",
            "trace_stage": "pressure_in_limits",
            "gauge_pressure_hpa": 1100.0,
            "controller_pressure_hpa": 1100.0,
            "dewpoint_c": -30.0,
        },
        {
            "timestamp": "2026-04-02T08:20:20.400",
            "trace_stage": "dewpoint_gate_begin",
            "dewpoint_gate_elapsed_s": 0.0,
            "dewpoint_gate_count": 0,
        },
        {
            "timestamp": "2026-04-02T08:20:21.200",
            "trace_stage": "dewpoint_gate_pass",
            "dewpoint_gate_elapsed_s": 0.8,
            "dewpoint_gate_count": 5,
            "dewpoint_gate_span_c": 0.03,
            "dewpoint_gate_slope_c_per_s": 0.01,
        },
        {
            "timestamp": "2026-04-02T08:20:21.250",
            "trace_stage": "dewpoint_gate_end",
            "phase_note": "result=stable",
            "dewpoint_gate_elapsed_s": 0.8,
            "dewpoint_gate_count": 5,
            "dewpoint_gate_span_c": 0.03,
            "dewpoint_gate_slope_c_per_s": 0.01,
        },
        {
            "timestamp": "2026-04-02T08:20:21.300",
            "trace_stage": "sampling_begin",
        },
    ]
    stable_rows = [
        {
            "timestamp": "2026-04-02T08:20:21.900",
            "analyzer2_co2_ratio": 1.4,
            "analyzer2_pressure_hpa": 1099.5,
            "gauge_pressure_hpa": 1100.0,
            "dewpoint_c": -30.0,
            "dewpoint_temp_c": 20.0,
            "dewpoint_rh_percent": 5.0,
        },
        {
            "timestamp": "2026-04-02T08:20:22.900",
            "analyzer2_co2_ratio": 1.4,
            "analyzer2_pressure_hpa": 1099.5,
            "gauge_pressure_hpa": 1100.0,
            "dewpoint_c": -30.0,
            "dewpoint_temp_c": 20.0,
            "dewpoint_rh_percent": 5.0,
        },
    ]

    summary = build_pressure_point_summary(
        transition_rows,
        stable_rows,
        process_variant="B",
        layer=3,
        repeat_index=1,
        gas_ppm=0,
        pressure_target_hpa=1100,
    )

    assert summary["pressure_in_limits_to_dewpoint_gate_begin_ms"] == 400.0
    assert summary["dewpoint_gate_begin_to_dewpoint_gate_end_ms"] == 850.0
    assert summary["dewpoint_gate_end_to_sampling_begin_ms"] == 50.0
    assert summary["sampling_begin_to_first_effective_sample_ms"] == 600.0
    assert summary["dewpoint_gate_result"] == "stable"
    assert summary["dewpoint_gate_count"] == 5


def test_append_live_csv_row_rewrites_with_expanded_header(tmp_path) -> None:
    path = tmp_path / "pressure_point_summary.csv"
    _append_live_csv_row(path, {"gas_ppm": 0, "pressure_target_hpa": 1100})
    _append_live_csv_row(path, {"gas_ppm": 1000, "pressure_target_hpa": 900, "dewpoint_gate_result": "timeout"})

    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        rows = list(csv.DictReader(handle))

    assert len(rows) == 2
    assert rows[0]["dewpoint_gate_result"] == ""
    assert rows[1]["dewpoint_gate_result"] == "timeout"
