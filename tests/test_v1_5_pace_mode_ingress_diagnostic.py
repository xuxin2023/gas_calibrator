from __future__ import annotations

import pytest

from gas_calibrator.tools.run_v1_5_pace_mode_ingress_diagnostic import (
    REQUIRED_SAMPLE_FIELDS,
    TrialResult,
    assert_no_forbidden_writes,
    build_default_trial_plan,
    classify_supply_involvement,
    planned_commands_for_trial,
    rank_trial_results,
    score_trial_result,
    should_continue_control_after_mode_set,
    validate_targets_below_ambient,
)


def test_targets_are_below_ambient_and_do_not_include_1100() -> None:
    assert validate_targets_below_ambient([980, 950, 900], ambient_hpa=1006) == (980.0, 950.0, 900.0)
    with pytest.raises(ValueError, match="1100"):
        validate_targets_below_ambient([1100], ambient_hpa=1006)
    with pytest.raises(ValueError, match=">= ambient"):
        validate_targets_below_ambient([1006], ambient_hpa=1006)


def test_trials_include_outp0_act_pass_gaug() -> None:
    trials = build_default_trial_plan([980, 950, 900], ambient_hpa=1006)
    labels = {trial.trial_id for trial in trials}
    modes = {trial.mode_requested for trial in trials}

    assert "trial_0_outp0_sealed_hold" in labels
    assert {"OUTP0", "ACT", "PASS", "GAUG"} <= modes
    assert all(1100.0 not in trial.targets_hpa for trial in trials)


def test_act_over0_trial_records_eff_comp_dewpoint() -> None:
    trial = next(
        item
        for item in build_default_trial_plan([980], ambient_hpa=1006)
        if item.trial_id == "trial_1_act_over0_max"
    )
    commands = planned_commands_for_trial(trial, target_hpa=980)

    assert "sour_pres_eff_pct" in REQUIRED_SAMPLE_FIELDS
    assert "sour_pres_comp1_hpa" in REQUIRED_SAMPLE_FIELDS
    assert "sour_pres_comp2_hpa" in REQUIRED_SAMPLE_FIELDS
    assert "dewpoint_latest_c" in REQUIRED_SAMPLE_FIELDS
    assert ":SOUR:PRES:EFF?" in commands
    assert ":SOUR:PRES:COMP1?" in commands
    assert ":SOUR:PRES:COMP2?" in commands


def test_pass_mode_trial_restores_original_mode() -> None:
    trial = next(
        item
        for item in build_default_trial_plan([950], ambient_hpa=1006)
        if item.trial_id == "trial_3_pass_over0_max"
    )

    assert trial.restores_original_mode is True
    assert should_continue_control_after_mode_set(
        trial,
        mode_supported=True,
        syst_err_after_mode_set="0,No error",
    )


def test_gaug_mode_unsupported_does_not_continue_control() -> None:
    trial = next(
        item
        for item in build_default_trial_plan([900], ambient_hpa=1006)
        if item.trial_id == "trial_4_gaug_cautious"
    )

    assert trial.continue_control_if_unsupported is False
    assert not should_continue_control_after_mode_set(
        trial,
        mode_supported=False,
        syst_err_after_mode_set="-113,Undefined header",
    )


def test_over1_trial_marked_diagnostic_only() -> None:
    trial = next(
        item
        for item in build_default_trial_plan([980], ambient_hpa=1006)
        if item.trial_id == "trial_2_act_over1_max"
    )

    assert trial.overshoot_allowed is True
    assert trial.diagnostic_only is True
    assert trial.not_real_acceptance_evidence is True


def test_positive_effort_dewpoint_correlation_flags_supply_involvement() -> None:
    result = TrialResult(
        trial_id="t",
        mode_requested="ACT",
        eff_positive_seen=True,
        eff_positive_duration_s=3.0,
        eff_positive_integral_pct_s=1.0,
        dewpoint_delta_c=2.0,
        dewpoint_rise_rate_c_per_s=0.1,
    )

    classified = classify_supply_involvement(result)

    assert classified["supply_involvement_confidence"] == "high"


def test_mode_ranking_prefers_low_dewpoint_low_positive_effort() -> None:
    act = TrialResult(
        trial_id="act",
        mode_requested="ACT",
        mode_confirmed="ACT",
        target_hpa=950,
        outp1_to_candidate_s=5.0,
        dewpoint_delta_c=5.0,
        eff_positive_seen=True,
        eff_positive_duration_s=3.0,
        eff_positive_integral_pct_s=1.0,
        eff_negative_integral_pct_s=0.2,
        candidate_row_possible=True,
        overshoot_allowed=False,
        slew_mode="MAX",
    )
    passive = TrialResult(
        trial_id="pass",
        mode_requested="PASS",
        mode_confirmed="PASS",
        target_hpa=950,
        outp1_to_candidate_s=6.0,
        dewpoint_delta_c=0.1,
        eff_positive_seen=False,
        eff_negative_integral_pct_s=4.0,
        eff_negative_duration_s=5.0,
        candidate_row_possible=True,
        overshoot_allowed=False,
        slew_mode="MAX",
    )

    ranking = rank_trial_results([act, passive])

    assert ranking["best_mode_for_clean_exhaust_control"] == "PASS_OVER0_MAX"
    assert ranking["whether_PASS_should_be_promoted_to_next_limited_workflow_test"] is True
    assert ranking["whether_ACT_is_suspected_of_supply_ingress"] is True


def test_vent_violation_rejects_mode() -> None:
    result = score_trial_result(
        TrialResult(
            trial_id="act",
            mode_requested="ACT",
            target_hpa=980,
            candidate_row_possible=True,
            vent_violation=True,
            overshoot_allowed=False,
            slew_mode="MAX",
        )
    )

    assert result.score <= -100
    assert "vent_violation" in result.rejection_reasons
    assert result.trial_recommended_for_workflow is False


def test_trial_results_are_diagnostic_not_acceptance() -> None:
    trial = build_default_trial_plan([980], ambient_hpa=1006)[1]
    result = TrialResult(trial_id=trial.trial_id, mode_requested=trial.mode_requested)

    assert trial.diagnostic_only is True
    assert trial.not_real_acceptance_evidence is True
    assert result.diagnostic_only is True
    assert result.not_real_acceptance_evidence is True
    assert result.line_contaminated is True
    assert result.next_run_requires_full_open_flow_flush is True


def test_no_forbidden_writes_added() -> None:
    for trial in build_default_trial_plan([980, 950, 900], ambient_hpa=1006):
        for target in trial.targets_hpa or (None,):
            commands = planned_commands_for_trial(trial, target)
            assert_no_forbidden_writes(commands)
