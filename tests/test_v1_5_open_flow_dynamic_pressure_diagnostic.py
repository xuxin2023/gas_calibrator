from __future__ import annotations

from pathlib import Path

import pytest

from gas_calibrator.tools.run_v1_5_open_flow_dynamic_pressure_diagnostic import (
    DEFAULT_GAS_PPM,
    DEFAULT_OPEN_FLOW_MAX_SAFE_PRESSURE_HPA,
    DEFAULT_OPEN_FLOW_SOURCE_MAX_RISE_HPA,
    DynamicTrialPlan,
    PACE_VENT_WRITE_RE,
    assert_no_forbidden_writes,
    build_default_trial_plan,
    command_is_forbidden_write,
    planned_commands_for_trial,
    rank_results,
    read_pace_pressure_hpa,
    resolve_0ppm_open_flow_valves,
    row_exceeds_open_flow_source_rise,
    row_exceeds_open_flow_pressure_safety,
    run_offline_plan,
    summarize_samples,
    validate_dynamic_targets,
)


def _cfg() -> dict:
    return {
        "valves": {
            "co2_path": 7,
            "co2_path_group2": 16,
            "gas_main": 11,
            "h2o_path": 8,
            "hold": 9,
            "flow_switch": 10,
            "co2_map": {"0": 1, "1000": 6},
            "co2_map_group2": {"0": 21},
            "relay_map": {
                "1": {"device": "relay", "channel": 7},
                "7": {"device": "relay", "channel": 15},
                "8": {"device": "relay_8", "channel": 8},
                "11": {"device": "relay_8", "channel": 3},
                "16": {"device": "relay", "channel": 16},
                "21": {"device": "relay", "channel": 6},
            },
        }
    }


def _samples(
    *,
    target: float = 1000.0,
    pressure: float = 1000.4,
    dewpoint: float = -39.0,
    effort: float = 0.0,
    count: int = 10,
    analyzer: bool = True,
) -> list[dict]:
    rows: list[dict] = []
    for idx in range(count):
        rows.append(
            {
                "ts": 100.0 + idx * 0.2,
                "pace_pressure_hpa": pressure + (0.02 if idx % 2 else 0.0),
                "dewpoint_c": dewpoint + idx * 0.01,
                "analyzer_co2_ppm": 0.2 if analyzer else "",
                "analyzer_h2o_mmol": 0.01 if analyzer else "",
                "sour_pres_eff_pct": effort,
                "vent_status": 2,
                "actual_open_valves": "8,11,7,1",
                "target_hpa": target,
            }
        )
    return rows


def test_default_targets_are_0ppm_below_ambient_and_exclude_1100() -> None:
    assert DEFAULT_GAS_PPM == 0
    assert validate_dynamic_targets([1000, 900], ambient_hpa=1006) == (1000.0, 900.0)
    with pytest.raises(ValueError, match="1100"):
        validate_dynamic_targets([1100], ambient_hpa=1006)
    with pytest.raises(ValueError, match=">= ambient"):
        validate_dynamic_targets([1006], ambient_hpa=1006)


def test_default_plan_is_open_flow_not_sealed_and_uses_0ppm() -> None:
    plan = build_default_trial_plan([1000, 900], ambient_hpa=1006)

    assert plan[0].trial_id == "open_flow_outp0_observe"
    assert all(item.gas_ppm == 0 for item in plan)
    assert all(item.open_flow_route_active is True for item in plan)
    assert all(item.route_sealed is False for item in plan)
    assert {item.mode_requested for item in plan} == {"OUTP0", "ACT"}


def test_gaug_is_explicit_diagnostic_opt_in_not_default() -> None:
    default_plan = build_default_trial_plan([1000], ambient_hpa=1006)
    gaug_plan = build_default_trial_plan([1000], ambient_hpa=1006, include_gaug=True)

    assert "GAUG" not in {item.mode_requested for item in default_plan}
    assert "GAUG" in {item.mode_requested for item in gaug_plan}


def test_planned_commands_record_telemetry_without_pace_vent_control() -> None:
    trial = next(item for item in build_default_trial_plan([1000], ambient_hpa=1006) if item.mode_requested == "ACT")
    commands = planned_commands_for_trial(trial)

    assert ":SOUR:PRES:EFF?" in commands
    assert ":SOUR:PRES:COMP1?" in commands
    assert ":SOUR:PRES:COMP2?" in commands
    assert ":SENS:PRES:INL?" in commands
    assert ":SOUR:PRES:RANG?" in commands
    assert ":SENS:PRES:RANG?" in commands
    assert ":SOUR:PRES:LEV:IMM:AMPL:VENT:RATE?" in commands
    assert ":SOUR:PRES:LEV:IMM:AMPL:VENT:UNIT?" in commands
    assert not any(PACE_VENT_WRITE_RE.search(command) for command in commands)
    assert_no_forbidden_writes(commands)


def test_outp0_baseline_does_not_write_control_profile() -> None:
    trial = build_default_trial_plan([1000], ambient_hpa=1006)[0]
    commands = planned_commands_for_trial(trial)

    assert trial.mode_requested == "OUTP0"
    assert not any(command.startswith(":SOUR:PRES:SLEW:MODE ") for command in commands)
    assert not any(command.startswith(":SOUR:PRES:SLEW:OVER ") for command in commands)
    assert not any(command.startswith(":SOUR:PRES:LEV:IMM:AMPL ") for command in commands)


def test_pace_vent_start_or_abort_is_forbidden_for_sampling_control() -> None:
    assert command_is_forbidden_write(":SOUR:PRES:LEV:IMM:AMPL:VENT 1") is True
    assert command_is_forbidden_write(":SOUR:PRES:LEV:IMM:AMPL:VENT 0") is True
    assert command_is_forbidden_write(":SOUR:PRES:LEV:IMM:AMPL:VENT?") is False


def test_resolve_0ppm_route_opens_source_and_open_flow_path() -> None:
    route = resolve_0ppm_open_flow_valves(_cfg(), gas_ppm=0)

    assert route["gas_ppm"] == 0
    assert route["group"] == "A"
    assert route["source_valve"] == 1
    assert route["path_valve"] == 7
    assert route["path_open_logical_valves"] == [8, 11, 7]
    assert route["open_logical_valves"] == [8, 11, 7, 1]


def test_micro_positive_effort_can_remain_A_when_pressure_dewpoint_and_analyzer_are_good() -> None:
    plan = DynamicTrialPlan(
        trial_id="open_flow_act_over0_max_1000",
        label="ACT 1000",
        mode_requested="ACT",
        target_hpa=1000.0,
    )
    result = summarize_samples(
        _samples(target=1000.0, pressure=1000.3, dewpoint=-39.0, effort=0.1),
        plan=plan,
        ambient_hpa=1006.0,
        candidate_ts=100.0,
        candidate_pressure_hpa=1000.3,
        outp1_ts=99.0,
        mode_confirmed="ACT",
    )

    assert result.positive_effort_class == "micro_ok"
    assert result.sample_can_enter_calibration_fit is True
    assert result.candidate_row_quality_grade == "A_calibration_eligible"


def test_tiny_target_crossing_like_999_999_does_not_block_A() -> None:
    plan = DynamicTrialPlan(
        trial_id="open_flow_act_over0_max_1000",
        label="ACT 1000",
        mode_requested="ACT",
        target_hpa=1000.0,
    )
    rows = _samples(target=1000.0, pressure=1000.2, dewpoint=-39.0, effort=0.0)
    rows[-1]["pace_pressure_hpa"] = 999.999

    result = summarize_samples(
        rows,
        plan=plan,
        ambient_hpa=1006.0,
        candidate_ts=100.0,
        candidate_pressure_hpa=1000.2,
        outp1_ts=99.0,
        mode_confirmed="ACT",
    )

    assert result.target_crossing_severity_hpa == pytest.approx(0.001)
    assert "target_crossing_nontrivial" not in result.rejection_reasons
    assert result.sample_can_enter_calibration_fit is True


def test_candidate_hit_without_stable_hold_blocks_A() -> None:
    plan = DynamicTrialPlan(
        trial_id="open_flow_act_over0_max_1000",
        label="ACT 1000",
        mode_requested="ACT",
        target_hpa=1000.0,
    )
    rows = _samples(target=1000.0, pressure=1000.2, dewpoint=-39.0, effort=0.0)
    rows[0]["pace_pressure_hpa"] = 1000.2
    rows[-1]["pace_pressure_hpa"] = 1003.4

    result = summarize_samples(
        rows,
        plan=plan,
        ambient_hpa=1006.0,
        candidate_ts=100.0,
        candidate_pressure_hpa=1000.2,
        outp1_ts=99.0,
        mode_confirmed="ACT",
    )

    assert result.candidate_detected is True
    assert result.pressure_stable_for_calibration is False
    assert result.sample_can_enter_calibration_fit is False
    assert "pressure_not_stable_for_fit" in result.rejection_reasons


def test_persistent_positive_effort_blocks_A_but_keeps_diagnostic_row() -> None:
    plan = DynamicTrialPlan(
        trial_id="open_flow_act_over0_max_1000",
        label="ACT 1000",
        mode_requested="ACT",
        target_hpa=1000.0,
    )
    result = summarize_samples(
        _samples(target=1000.0, pressure=1000.2, dewpoint=-39.0, effort=1.0),
        plan=plan,
        ambient_hpa=1006.0,
        candidate_ts=100.0,
        candidate_pressure_hpa=1000.2,
        outp1_ts=99.0,
        mode_confirmed="ACT",
    )

    assert result.positive_effort_class == "diagnostic_only"
    assert result.sample_can_enter_calibration_fit is False
    assert result.sample_can_enter_diagnostic_model is True
    assert "positive_effort_diagnostic_only" in result.rejection_reasons


def test_open_flow_pressure_safety_abort_uses_pace_or_com22_pressure() -> None:
    assert row_exceeds_open_flow_pressure_safety(
        {"pace_pressure_hpa": DEFAULT_OPEN_FLOW_MAX_SAFE_PRESSURE_HPA + 0.1},
        DEFAULT_OPEN_FLOW_MAX_SAFE_PRESSURE_HPA,
    )
    assert row_exceeds_open_flow_pressure_safety(
        {"pace_pressure_hpa": 1002.0, "com22_pressure_hpa": DEFAULT_OPEN_FLOW_MAX_SAFE_PRESSURE_HPA + 5.0},
        DEFAULT_OPEN_FLOW_MAX_SAFE_PRESSURE_HPA,
    )
    assert not row_exceeds_open_flow_pressure_safety(
        {"pace_pressure_hpa": 1002.0, "com22_pressure_hpa": 1003.0},
        DEFAULT_OPEN_FLOW_MAX_SAFE_PRESSURE_HPA,
    )


def test_source_open_pressure_rise_blocks_dynamic_control_entry() -> None:
    assert row_exceeds_open_flow_source_rise(
        {"pace_pressure_hpa": 1027.0},
        ambient_hpa=1006.0,
        max_rise_hpa=DEFAULT_OPEN_FLOW_SOURCE_MAX_RISE_HPA,
    )
    assert not row_exceeds_open_flow_source_rise(
        {"pace_pressure_hpa": 1018.0},
        ambient_hpa=1006.0,
        max_rise_hpa=DEFAULT_OPEN_FLOW_SOURCE_MAX_RISE_HPA,
    )


def test_pace_pressure_reading_uses_driver_fallback_when_cont_query_is_blank() -> None:
    class FakePace:
        def read_pressure(self) -> float:
            return 1003.9614868

        def query(self, command: str) -> str:
            if command == ":SENS:PRES:CONT?":
                return ""
            raise AssertionError(command)

    value, source = read_pace_pressure_hpa(FakePace())

    assert value == pytest.approx(1003.9614868)
    assert source == "PACE::read_pressure"


def test_pace_pressure_reading_falls_back_to_inl_query() -> None:
    class FakePace:
        def read_pressure(self) -> float:
            raise RuntimeError("NO_RESPONSE")

        def query(self, command: str) -> str:
            if command == ":SENS:PRES:INL?":
                return ":SENS:PRES:INL 1003.9614868, 0"
            if command == ":SENS:PRES:CONT?":
                return ""
            return ""

    value, source = read_pace_pressure_hpa(FakePace())

    assert value == pytest.approx(1003.9614868)
    assert source == "PACE::SENS:PRES:INL?"


def test_missing_dewpoint_or_analyzer_blocks_A() -> None:
    plan = DynamicTrialPlan(
        trial_id="open_flow_gaug_over0_max_1000",
        label="GAUG 1000",
        mode_requested="GAUG",
        target_hpa=1000.0,
    )
    rows = _samples(target=1000.0, pressure=1000.2, dewpoint=-39.0, effort=0.0, analyzer=False)
    for row in rows:
        row["dewpoint_c"] = ""

    result = summarize_samples(
        rows,
        plan=plan,
        ambient_hpa=1006.0,
        candidate_ts=100.0,
        candidate_pressure_hpa=1000.2,
        outp1_ts=99.0,
        mode_confirmed="GAUG",
    )

    assert result.dewpoint_evidence_missing is True
    assert result.analyzer_evidence_missing is True
    assert result.sample_can_enter_calibration_fit is False


def test_below_ambient_open_flow_without_exhaust_evidence_flags_backdiffusion_risk() -> None:
    plan = DynamicTrialPlan(
        trial_id="open_flow_gaug_over0_max_900",
        label="GAUG 900",
        mode_requested="GAUG",
        target_hpa=900.0,
    )
    result = summarize_samples(
        _samples(target=900.0, pressure=900.2, dewpoint=-39.0, effort=0.0),
        plan=plan,
        ambient_hpa=1006.0,
        candidate_ts=100.0,
        candidate_pressure_hpa=900.2,
        outp1_ts=99.0,
        mode_confirmed="GAUG",
    )

    assert result.backdiffusion_risk is True
    assert result.sample_can_enter_calibration_fit is False
    assert "below_ambient_open_flow_backdiffusion_risk" in result.rejection_reasons


def test_rank_results_prefers_A_row_and_reports_no_1100_or_vent_control() -> None:
    plan = DynamicTrialPlan(
        trial_id="open_flow_act_over0_max_1000",
        label="ACT 1000",
        mode_requested="ACT",
        target_hpa=1000.0,
    )
    a_row = summarize_samples(
        _samples(target=1000.0, pressure=1000.2, dewpoint=-39.0, effort=0.1),
        plan=plan,
        ambient_hpa=1006.0,
        candidate_ts=100.0,
        candidate_pressure_hpa=1000.2,
        outp1_ts=99.0,
        mode_confirmed="ACT",
    )

    ranking = rank_results([a_row])

    assert ranking["best_mode_for_open_flow_dynamic_pressure"] == "ACT"
    assert ranking["a_calibration_eligible_count"] == 1
    assert ranking["whether_1100_excluded"] is True
    assert ranking["whether_pace_vent_used_for_control"] is False
    assert ranking["whether_positive_effort_micro_topoff_can_remain_A"] is True


def test_offline_plan_writes_json_without_real_com(tmp_path: Path) -> None:
    payload = run_offline_plan(
        output_dir=tmp_path,
        targets_hpa=(1000.0, 900.0),
        ambient_hpa=1006.0,
        gas_ppm=0,
    )

    assert payload["gas_ppm"] == 0
    assert payload["uses_1100"] is False
    assert "GAUG" not in {item["mode_requested"] for item in payload["trial_plan"]}
    assert (tmp_path / "open_flow_dynamic_pressure_plan.json").exists()
