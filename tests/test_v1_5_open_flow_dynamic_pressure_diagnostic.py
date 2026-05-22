from __future__ import annotations

from pathlib import Path

import pytest

from gas_calibrator.tools.run_v1_5_open_flow_dynamic_pressure_diagnostic import (
    DEFAULT_GAS_PPM,
    DEFAULT_OPEN_FLOW_MAX_SAFE_PRESSURE_HPA,
    DEFAULT_OPEN_FLOW_SOURCE_MAX_RISE_HPA,
    DynamicTrialPlan,
    PACE_VENT_WRITE_RE,
    _collect_fast_pressure_sample,
    _confirm_control_command_state,
    _enable_control_output_confirmed,
    assert_no_forbidden_writes,
    build_arg_parser,
    build_default_trial_plan,
    command_is_forbidden_write,
    open_flow_dynamic_control_runaway_reason,
    planned_commands_for_trial,
    open_flow_pressure_abort_reason,
    rank_results,
    read_pace_pressure_hpa,
    refresh_direct_control_keepalive,
    resolve_0ppm_open_flow_valves,
    row_exceeds_open_flow_source_rise,
    row_exceeds_open_flow_pressure_safety,
    run_offline_plan,
    start_open_flow_atmosphere_hold,
    stop_open_flow_atmosphere_hold_before_control,
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


def test_direct_control_plan_excludes_outp0_baseline() -> None:
    plans = build_default_trial_plan([1000.0], ambient_hpa=1006.0, include_outp0_baseline=False)

    assert [plan.mode_requested for plan in plans] == ["ACT"]
    assert plans[0].target_hpa == pytest.approx(1000.0)


def test_direct_control_atmosphere_flag_is_explicit() -> None:
    parser = build_arg_parser()
    args = parser.parse_args(
        [
            "--direct-control-only",
            "--keep-atmosphere-hold-during-direct-control",
            "--real-com",
            "--i-understand-open-flow-no-write",
            "--operator-confirm-0ppm-flow",
            "--targets",
            "1000",
        ]
    )

    assert args.direct_control_only is True
    assert args.keep_atmosphere_hold_during_direct_control is True
    assert args.no_open_flow_atmosphere_hold is False


def test_fastest_diagnostic_flags_are_explicit() -> None:
    parser = build_arg_parser()
    args = parser.parse_args(
        [
            "--include-over1",
            "--set-slew-value-max",
            "--targets",
            "1000",
        ]
    )

    assert args.include_over1 is True
    assert args.set_slew_value_max is True


def test_control_command_confirmation_requires_outp1_and_setpoint() -> None:
    class FakePace:
        def query(self, command: str) -> str:
            return {
                ":OUTP:STAT?": ":OUTP:STAT 1",
                ":SOUR:PRES:LEV:IMM:AMPL?": ":SOUR:PRES:LEV:IMM:AMPL 1000.0000000",
                ":SOUR:PRES:LEV:IMM:AMPL:VENT?": ":SOUR:PRES:LEV:IMM:AMPL:VENT 3",
                ":SOUR:PRES:EFF?": ":SOUR:PRES:EFF -0.1",
                ":SYST:ERR?": ":SYST:ERR 0, No error",
            }.get(command, "")

        def read_pressure(self) -> float:
            return 1001.34

    confirmation = _confirm_control_command_state(FakePace(), target_hpa=1000.0)

    assert confirmation["control_command_confirmed"] is True
    assert confirmation["control_outp_state_after_command"] == 1
    assert confirmation["control_setpoint_after_command_hpa"] == pytest.approx(1000.0)
    assert confirmation["control_pressure_after_command_hpa"] == pytest.approx(1001.34)
    assert confirmation["pace_vent_hold_during_outp1_allowed"] is False


def test_enable_control_output_confirmation_retries_documented_outp_stat() -> None:
    class FakePace:
        def __init__(self) -> None:
            self.outp = 0
            self.writes: list[str] = []

        def enable_control_output(self, **kwargs) -> None:
            self.writes.append("enable_control_output")

        def write(self, command: str) -> None:
            self.writes.append(command)
            if command == ":OUTP:STAT 1":
                self.outp = 1

        def query(self, command: str) -> str:
            return {
                ":OUTP:STAT?": f":OUTP:STAT {self.outp}",
                ":SOUR:PRES:LEV:IMM:AMPL?": ":SOUR:PRES:LEV:IMM:AMPL 1000.0000000",
                ":SOUR:PRES:LEV:IMM:AMPL:VENT?": ":SOUR:PRES:LEV:IMM:AMPL:VENT 3",
                ":SOUR:PRES:EFF?": ":SOUR:PRES:EFF -0.1",
                ":SYST:ERR?": ":SYST:ERR 0, No error",
            }.get(command, "")

        def read_pressure(self) -> float:
            return 1001.34

    pace = FakePace()
    confirmation = _enable_control_output_confirmed(pace, target_hpa=1000.0, timeout_s=1.0, poll_s=0.01)

    assert confirmation["control_command_confirmed"] is True
    assert ":OUTP:STAT 1" in pace.writes


def test_enable_control_output_confirmation_accepts_delayed_outp_after_final_retry() -> None:
    class FakePace:
        def __init__(self) -> None:
            self.raw_outp_written = False
            self.outp_queries_after_raw = 0

        def enable_control_output(self, **kwargs) -> None:
            pass

        def write(self, command: str) -> None:
            if command == ":OUTP:STAT 1":
                self.raw_outp_written = True

        def query(self, command: str) -> str:
            if command == ":OUTP:STAT?":
                if self.raw_outp_written:
                    self.outp_queries_after_raw += 1
                value = 1 if self.outp_queries_after_raw >= 1 else 0
                return f":OUTP:STAT {value}"
            return {
                ":SOUR:PRES:LEV:IMM:AMPL?": ":SOUR:PRES:LEV:IMM:AMPL 1000.0000000",
                ":SOUR:PRES:LEV:IMM:AMPL:VENT?": ":SOUR:PRES:LEV:IMM:AMPL:VENT 2",
                ":SOUR:PRES:EFF?": ":SOUR:PRES:EFF -0.1",
                ":SYST:ERR?": ":SYST:ERR 0, No error",
            }.get(command, "")

        def read_pressure(self) -> float:
            return 1000.25

    confirmation = _enable_control_output_confirmed(
        FakePace(),
        target_hpa=1000.0,
        timeout_s=0.5,
        poll_s=0.6,
    )

    assert confirmation["control_command_confirmed"] is True
    assert confirmation["control_outp_state_after_command"] == 1


def test_fast_pressure_sample_uses_pace_read_without_slow_queries() -> None:
    class FakePace:
        def read_pressure(self) -> float:
            return 1000.25

        def query(self, command: str) -> str:
            raise AssertionError(f"slow query should not be used: {command}")

    plan = DynamicTrialPlan(
        trial_id="open_flow_act_over0_max_1000",
        label="ACT 1000",
        mode_requested="ACT",
        target_hpa=1000.0,
    )

    row = _collect_fast_pressure_sample(FakePace(), plan=plan, actual_open_valves=[8, 11, 7, 1])

    assert row["pace_pressure_hpa"] == pytest.approx(1000.25)
    assert row["pace_pressure_source"] == "PACE::read_pressure"
    assert row["phase"] == "open_flow_dynamic_pressure_fast_control"
    assert row["actual_open_valves"] == "8,11,7,1"


def test_direct_control_keepalive_reasserts_only_when_output_drops() -> None:
    class FakePace:
        def __init__(self) -> None:
            self.outp = 0
            self.writes: list[str] = []

        def enable_control_output(self, **kwargs) -> None:
            self.writes.append("enable_control_output")

        def write(self, command: str) -> None:
            self.writes.append(command)
            if command == ":OUTP:STAT 1":
                self.outp = 1

        def query(self, command: str) -> str:
            return {
                ":OUTP:STAT?": f":OUTP:STAT {self.outp}",
                ":SOUR:PRES:LEV:IMM:AMPL?": ":SOUR:PRES:LEV:IMM:AMPL 1000.0000000",
                ":SOUR:PRES:LEV:IMM:AMPL:VENT?": ":SOUR:PRES:LEV:IMM:AMPL:VENT 2",
                ":SOUR:PRES:EFF?": ":SOUR:PRES:EFF -0.2",
                ":SOUR:PRES:COMP1?": ":SOUR:PRES:COMP1 3165.0",
                ":SOUR:PRES:COMP2?": ":SOUR:PRES:COMP2 -964.0",
                ":SOUR:PRES:RANG?": ':SOUR:PRES:RANG "3.50barg"',
                ":SENS:PRES:RANG?": ':SENS:PRES:RANG "3.50barg"',
                ":SOUR:PRES:SLEW?": ":SOUR:PRES:SLEW 99999999.0000000",
                ":SENS:PRES:SLEW?": ":SENS:PRES:SLEW -20.0",
                ":SOUR:PRES:SLEW:MODE?": ":SOUR:PRES:SLEW:MODE MAX",
                ":SOUR:PRES:SLEW:OVER?": ":SOUR:PRES:SLEW:OVER:STAT 0",
                ":SYST:ERR?": ":SYST:ERR 0, No error",
            }.get(command, "")

        def read_pressure(self) -> float:
            return 1000.4

    plan = DynamicTrialPlan(
        trial_id="open_flow_act_over0_max_1000",
        label="ACT 1000",
        mode_requested="ACT",
        target_hpa=1000.0,
    )
    row = _collect_fast_pressure_sample(FakePace(), plan=plan, actual_open_valves=[8, 11, 7, 1])
    state: dict[str, object] = {}
    pace = FakePace()

    refresh_direct_control_keepalive(pace, row, plan=plan, state=state)

    assert row["control_keepalive_checked"] is True
    assert row["control_output_dropout_seen"] is True
    assert row["control_output_reasserted"] is True
    assert row["control_output_reassert_count"] == 1
    assert row["outp_state"] == 1
    assert row["sour_pres_eff_pct"] == pytest.approx(-0.2)
    assert row["sour_pres_comp2_hpa"] == pytest.approx(-964.0)
    assert row["sour_pres_slew_hpa_per_s"] == pytest.approx(99999999.0)
    assert row["sens_pressure_slew_hpa_per_s"] == pytest.approx(-20.0)
    assert row["slew_mode"] == ":SOUR:PRES:SLEW:MODE MAX"
    assert ":OUTP:STAT 1" in pace.writes


def test_include_over1_adds_fastest_diagnostic_trial_without_changing_default() -> None:
    default_plan = build_default_trial_plan([1000], ambient_hpa=1006)
    fastest_plan = build_default_trial_plan(
        [1000],
        ambient_hpa=1006,
        include_over1=True,
        set_slew_value_max=True,
    )

    assert all(item.overshoot_allowed is not True for item in default_plan)
    over1 = [item for item in fastest_plan if item.mode_requested == "ACT" and item.overshoot_allowed is True]
    assert len(over1) == 1
    assert over1[0].slew_mode == "MAX"
    assert over1[0].slew_value_max is True
    assert over1[0].diagnostic_only is True


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
    assert ":SOUR:PRES:SLEW?" in commands
    assert ":SENS:PRES:SLEW?" in commands
    assert ":SOUR:PRES:RANG?" in commands
    assert ":SENS:PRES:RANG?" in commands
    assert ":SOUR:PRES:LEV:IMM:AMPL:VENT:RATE?" in commands
    assert ":SOUR:PRES:LEV:IMM:AMPL:VENT:UNIT?" in commands
    assert not any(PACE_VENT_WRITE_RE.search(command) for command in commands)
    assert_no_forbidden_writes(commands)


def test_planned_fastest_commands_use_manual_slew_max_without_pace_vent() -> None:
    trial = next(
        item
        for item in build_default_trial_plan(
            [1000],
            ambient_hpa=1006,
            include_over1=True,
            set_slew_value_max=True,
        )
        if item.overshoot_allowed is True
    )
    commands = planned_commands_for_trial(trial)

    assert ":SOUR:PRES:SLEW max" in commands
    assert ":SOUR:PRES:SLEW:MODE MAX" in commands
    assert ":SOUR:PRES:SLEW:OVER 1" in commands
    assert commands.index(":SOUR:PRES:SLEW max") < commands.index(":SOUR:PRES:SLEW:MODE MAX")
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


def test_open_flow_pressure_safety_abort_uses_pace_pressure_only() -> None:
    assert row_exceeds_open_flow_pressure_safety(
        {"pace_pressure_hpa": DEFAULT_OPEN_FLOW_MAX_SAFE_PRESSURE_HPA + 0.1},
        DEFAULT_OPEN_FLOW_MAX_SAFE_PRESSURE_HPA,
    )
    assert not row_exceeds_open_flow_pressure_safety(
        {"pace_pressure_hpa": 1002.0, "com22_pressure_hpa": DEFAULT_OPEN_FLOW_MAX_SAFE_PRESSURE_HPA + 5.0},
        DEFAULT_OPEN_FLOW_MAX_SAFE_PRESSURE_HPA,
    )
    assert not row_exceeds_open_flow_pressure_safety(
        {"pace_pressure_hpa": 1002.0, "com22_pressure_hpa": 1003.0},
        DEFAULT_OPEN_FLOW_MAX_SAFE_PRESSURE_HPA,
    )


def test_direct_control_allows_short_source_open_pressure_transient() -> None:
    row = {"pace_pressure_hpa": 1082.4}

    assert (
        open_flow_pressure_abort_reason(
            row,
            max_safe_pressure_hpa=1050.0,
            transient_limit_hpa=1150.0,
            transient_grace_s=3.0,
            transient_elapsed_s=0.15,
        )
        == ""
    )
    assert "open_flow_pressure_safety_abort" in open_flow_pressure_abort_reason(
        row,
        max_safe_pressure_hpa=1050.0,
        transient_limit_hpa=1150.0,
        transient_grace_s=3.0,
        transient_elapsed_s=3.2,
    )
    assert "open_flow_pressure_hard_abort" in open_flow_pressure_abort_reason(
        {"pace_pressure_hpa": 1150.1},
        max_safe_pressure_hpa=1050.0,
        transient_limit_hpa=1150.0,
        transient_grace_s=3.0,
        transient_elapsed_s=0.1,
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
    assert not row_exceeds_open_flow_source_rise(
        {"pace_pressure_hpa": 1006.5, "com22_pressure_hpa": 1064.0},
        ambient_hpa=1006.0,
        max_rise_hpa=DEFAULT_OPEN_FLOW_SOURCE_MAX_RISE_HPA,
    )


def test_dynamic_control_runaway_allows_grace_then_aborts_against_target() -> None:
    assert (
        open_flow_dynamic_control_runaway_reason(
            {"pace_pressure_hpa": 1358.0},
            target_hpa=1000.0,
            max_rise_hpa=20.0,
            transient_grace_s=3.0,
            transient_elapsed_s=1.0,
        )
        == ""
    )
    reason = open_flow_dynamic_control_runaway_reason(
        {"pace_pressure_hpa": 1113.0},
        target_hpa=1000.0,
        max_rise_hpa=20.0,
        transient_grace_s=3.0,
        transient_elapsed_s=3.2,
    )

    assert reason == "open_flow_dynamic_control_runaway_abort:1113.000>1020.000"


def test_open_flow_atmosphere_hold_uses_pace_hold_open_before_source() -> None:
    class FakePace:
        def __init__(self) -> None:
            self.calls: list[tuple] = []
            self.active = False

        def enter_atmosphere_mode(self, **kwargs) -> None:
            self.calls.append(("enter_atmosphere_mode", kwargs))
            self.active = bool(kwargs.get("hold_open"))

        def is_atmosphere_hold_active(self) -> bool:
            return self.active

    pace = FakePace()
    result = start_open_flow_atmosphere_hold(pace, interval_s=0.2)

    assert result["active"] is True
    assert result["strategy"] == "enter_atmosphere_mode_hold_open"
    assert pace.calls[0][0] == "enter_atmosphere_mode"
    assert pace.calls[0][1]["hold_open"] is True
    assert pace.calls[0][1]["hold_interval_s"] == pytest.approx(0.2)


def test_atmosphere_hold_is_stopped_before_setpoint_control() -> None:
    class FakePace:
        def __init__(self) -> None:
            self.calls: list[tuple] = []

        def stop_atmosphere_hold(self, **kwargs) -> bool:
            self.calls.append(("stop_atmosphere_hold", kwargs))
            return True

        def set_output(self, on: bool) -> None:
            self.calls.append(("set_output", bool(on)))

        def vent(self, on: bool) -> None:
            self.calls.append(("vent", bool(on)))

        def wait_for_vent_idle(self, **kwargs) -> int:
            self.calls.append(("wait_for_vent_idle", kwargs))
            return 3

        def set_isolation_open(self, is_open: bool) -> None:
            self.calls.append(("set_isolation_open", bool(is_open)))

    pace = FakePace()
    result = stop_open_flow_atmosphere_hold_before_control(pace)

    assert result["stopped"] is True
    assert result["vent_abort_sent"] is True
    assert result["vent_idle_status"] == 3
    assert result["output_off_sent"] is True
    assert ("set_output", False) in pace.calls
    assert ("vent", False) in pace.calls
    assert any(call[0] == "wait_for_vent_idle" for call in pace.calls)
    assert ("set_isolation_open", True) in pace.calls


def test_atmosphere_hold_stop_can_skip_redundant_outp0_for_direct_control() -> None:
    class FakePace:
        def __init__(self) -> None:
            self.calls: list[tuple] = []

        def stop_atmosphere_hold(self, **kwargs) -> bool:
            self.calls.append(("stop_atmosphere_hold", kwargs))
            return True

        def set_output(self, on: bool) -> None:
            self.calls.append(("set_output", bool(on)))

        def vent(self, on: bool) -> None:
            self.calls.append(("vent", bool(on)))

        def wait_for_vent_idle(self, **kwargs) -> int:
            self.calls.append(("wait_for_vent_idle", kwargs))
            return 3

        def set_isolation_open(self, is_open: bool) -> None:
            self.calls.append(("set_isolation_open", bool(is_open)))

    pace = FakePace()
    result = stop_open_flow_atmosphere_hold_before_control(pace, force_output_off=False)

    assert result["stopped"] is True
    assert result["vent_abort_sent"] is True
    assert result["vent_idle_status"] == 3
    assert result["output_off_sent"] is False
    assert ("set_output", False) not in pace.calls
    assert ("vent", False) in pace.calls
    assert any(call[0] == "wait_for_vent_idle" for call in pace.calls)
    assert ("set_isolation_open", True) in pace.calls


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
