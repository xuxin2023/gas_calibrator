from gas_calibrator.validation.v1_5_calibratable_point_policy import (
    evaluate_calibratable_point,
)


def _readiness(**overrides):
    data = {
        "readiness_status": "pass",
        "candidate_fit_ready": True,
        "blockers": [],
        "warnings": [],
        "route_evidence_present": True,
        "actual_purge_s": 600.0,
        "minimum_purge_s": 360.0,
    }
    data.update(overrides)
    return data


def _qc_summary():
    return {"analyzer_prefix": "ga01", "analyzer_device_id": "ID100"}


def _co2_row(index: int, **overrides):
    row = {
        "dewpoint_c": -40.0 + index * 0.001,
        "ga01_co2_ratio_f": 1.2000 + index * 0.00005,
        "ga01_h2o_ratio_f": 0.7000,
        "ga01_h2o_mmol": 1.0 + index * 0.001,
    }
    row.update(overrides)
    return row


def _h2o_row(index: int, **overrides):
    row = {
        "dewpoint_c": -10.0 + index * 0.001,
        "ga01_h2o_ratio_f": 0.8000 + index * 0.00005,
        "ga01_h2o_mmol": 12.0 + index * 0.001,
    }
    row.update(overrides)
    return row


def test_co2_dry_stable_point_samples_now_without_chasing_lower_dewpoint():
    rows = [_co2_row(i) for i in range(10)]

    result = evaluate_calibratable_point(
        rows,
        component="co2",
        analyzer_prefix="ga01",
        sample_readiness=_readiness(),
        qc_summary=_qc_summary(),
    )

    assert result["calibratability_grade"] == "A"
    assert result["fit_input_role"] == "direct_fit"
    assert result["time_optimization_action"] == "sample_now_do_not_chase_lower_dewpoint"
    assert result["candidate_fit_ready"] is True


def test_co2_stable_wet_state_can_be_sampled_with_normalization_review():
    rows = [_co2_row(i, ga01_h2o_mmol=25.0 + i * 0.001) for i in range(10)]

    result = evaluate_calibratable_point(
        rows,
        component="co2",
        analyzer_prefix="ga01",
        sample_readiness=_readiness(),
        qc_summary=_qc_summary(),
    )

    assert result["calibratability_grade"] == "B"
    assert result["fit_input_role"] == "state_normalized_fit_review"
    assert result["time_optimization_action"] == "sample_now_with_h2o_state_normalization"
    assert "co2_h2o_state_requires_normalization_and_uncertainty" in result["warnings"]


def test_unstable_dewpoint_h2o_or_ratio_keeps_route_open_and_waits():
    rows = [
        _co2_row(
            i,
            dewpoint_c=-30.0 + i * 0.05,
            ga01_co2_ratio_f=1.2000 + i * 0.0002,
            ga01_h2o_mmol=10.0 + i * 0.2,
        )
        for i in range(10)
    ]

    result = evaluate_calibratable_point(
        rows,
        component="co2",
        analyzer_prefix="ga01",
        sample_readiness=_readiness(),
        qc_summary=_qc_summary(),
    )

    assert result["calibratability_grade"] == "C"
    assert result["candidate_fit_ready"] is False
    assert result["time_optimization_action"] == "continue_stability_wait_with_route_open"
    assert "dewpoint_tail_not_stable" in result["reasons"]
    assert "h2o_state_not_stable" in result["reasons"]
    assert "component_ratio_not_stable" in result["reasons"]


def test_minimum_purge_is_a_lower_bound_before_any_shortcut_sampling():
    rows = [_co2_row(i) for i in range(10)]

    result = evaluate_calibratable_point(
        rows,
        component="co2",
        analyzer_prefix="ga01",
        sample_readiness=_readiness(
            readiness_status="fail",
            blockers=["minimum_purge_not_met:120<360"],
            actual_purge_s=120.0,
            minimum_purge_s=360.0,
        ),
        qc_summary=_qc_summary(),
    )

    assert result["calibratability_grade"] == "C"
    assert result["time_optimization_action"] == "continue_purge_to_minimum_with_route_open"
    assert "minimum_purge_is_lower_bound_not_acceptance" in result["reasons"]


def test_h2o_point_requires_water_quantity_evidence_not_only_zero_gas_identity():
    rows = [
        {
            "dewpoint_c": -30.0 + i * 0.001,
            "ga01_h2o_ratio_f": 0.7000 + i * 0.00005,
        }
        for i in range(10)
    ]

    result = evaluate_calibratable_point(
        rows,
        component="h2o",
        analyzer_prefix="ga01",
        sample_readiness=_readiness(),
        qc_summary=_qc_summary(),
    )

    assert result["calibratability_grade"] == "C"
    assert result["fit_input_role"] == "reject_missing_h2o_reference"
    assert result["time_optimization_action"] == "stop_and_restore_humidity_reference_evidence"


def test_h2o_stable_reference_point_is_direct_h2o_fit_evidence():
    rows = [_h2o_row(i) for i in range(10)]

    result = evaluate_calibratable_point(
        rows,
        component="h2o",
        analyzer_prefix="ga01",
        sample_readiness=_readiness(),
        qc_summary=_qc_summary(),
    )

    assert result["calibratability_grade"] == "A"
    assert result["fit_input_role"] == "direct_h2o_fit"
    assert (
        result["time_optimization_action"]
        == "sample_now_when_dewpoint_and_h2o_ratio_are_stable"
    )
