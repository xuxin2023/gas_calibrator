import json

from gas_calibrator.validation.formal_open_flow import (
    FormalOpenFlowConfig,
    build_formal_open_flow_report,
    classify_open_flow_samples,
    evaluate_pressure_channel_quick_check,
    state_sequence,
    validate_plan_snapshot,
)


def _plan():
    return {
        "plan_id": "v1_5_open_flow_demo",
        "plan_version": "2026-05-24",
        "config_hash": "abc123",
        "operator": "operator-a",
        "standard_gases": [
            {
                "component": "co2",
                "cylinder_id": "CO2-001",
                "certificate_value": 900.0,
                "certificate_uncertainty": 0.9,
                "valid_until": "2027-01-01",
                "supplier": "standard-lab",
                "certificate_hash": "cert-hash",
            }
        ],
    }


def _row(index: int, *, pressure_kpa: float = 100.05, ref_hpa: float = 1000.6, **overrides):
    row = {
        "sample_index": index,
        "pressure_mode": "ambient_open",
        "pressure_gauge_hpa": ref_hpa,
        "dewpoint_c": -30.0 + index * 0.001,
        "ga01_frame_usable": True,
        "ga01_mode2_contract_status": "pass",
        "ga01_mode2_qc_status": "pass",
        "ga01_mode2_tokens_json": json.dumps(
            ["YGAS", "001", "0900.000", "00.500", "1768.000", "00.410"],
            separators=(",", ":"),
        ),
        "ga01_raw": "YGAS,001,...",
        "ga01_ref_signal": 3322.0,
        "ga01_co2_signal": 4356.0,
        "ga01_h2o_signal": 2631.0,
        "ga01_chamber_temp_c": 25.0 + index * 0.001,
        "ga01_case_temp_c": 25.5,
        "ga01_pressure_kpa": pressure_kpa,
        "ga01_co2_ratio_f": 1.3000 + index * 0.0001,
        "ga01_co2_ppm": 900.0 + index * 0.02,
        "ga01_h2o_ratio_f": 0.7000,
        "ga01_h2o_mmol": 0.5,
    }
    row.update(overrides)
    return row


def test_formal_open_flow_state_order_puts_pressure_check_before_sampling():
    states = list(state_sequence())
    assert states == [
        "LOAD_PLAN",
        "PRECHECK",
        "PRESSURE_CHANNEL_QUICK_CHECK",
        "OPEN_FLOW_PURGE",
        "STABILITY_GATE",
        "SAMPLE_WINDOW",
        "QC_CLASSIFICATION",
        "POINT_REVIEW",
        "NEXT_POINT_OR_FINISH",
        "RUN_SUMMARY",
    ]
    assert states.index("PRESSURE_CHANNEL_QUICK_CHECK") < states.index("OPEN_FLOW_PURGE")
    assert states.index("PRESSURE_CHANNEL_QUICK_CHECK") < states.index("SAMPLE_WINDOW")


def test_plan_snapshot_requires_traceable_standard_gas_metadata():
    status, reasons = validate_plan_snapshot(_plan())
    assert status == "pass"
    assert reasons == []

    bad = dict(_plan())
    bad["standard_gases"] = [{"cylinder_id": "CO2-001"}]
    status, reasons = validate_plan_snapshot(bad)
    assert status == "fail"
    assert "standard_gas_1_missing_certificate_hash" in reasons


def test_pressure_channel_quick_check_passes_when_internal_p_matches_reference():
    rows = [_row(i, pressure_kpa=100.05 + i * 0.001, ref_hpa=1000.6 + i * 0.01) for i in range(1, 5)]
    result = evaluate_pressure_channel_quick_check(rows)
    assert result.status == "pass"
    assert result.allowed_for_formal_sampling is True
    assert result.sample_count == 4
    assert abs(result.mean_delta_hpa) < 1.0


def test_pressure_channel_quick_check_fails_when_bias_exceeds_limit():
    rows = [_row(i, pressure_kpa=101.5, ref_hpa=1000.0) for i in range(1, 5)]
    result = evaluate_pressure_channel_quick_check(rows)
    assert result.status == "fail"
    assert result.allowed_for_formal_sampling is False
    assert "mean_delta_hpa" in result.reason


def test_open_flow_report_outputs_a_grade_samples_and_reject_reasons():
    rows = [_row(i) for i in range(1, 11)]
    rows.append(_row(11, pressure_mode="sealed_controlled"))
    rows.append(_row(12, ga01_mode2_contract_status="fail"))
    cfg = FormalOpenFlowConfig(
        min_a_grade_samples=10,
        co2_ratio_span_max=0.01,
        dewpoint_span_c_max=0.2,
    )

    report = build_formal_open_flow_report(
        plan=_plan(),
        sample_rows=rows,
        pressure_check_rows=rows[:10],
        component="co2",
        cfg=cfg,
    )

    assert report.candidate_fit_allowed is True
    assert report.qc_summary["a_grade_count"] == 10
    assert len(report.a_grade_samples) == 10
    assert len(report.rejected_samples) == 2
    assert report.pressure_channel_quick_check["analyzer_prefix"] == "ga01"
    assert report.pressure_channel_quick_check["analyzer_device_id"] == "001"
    assert report.a_grade_samples[0]["analyzer_device_id"] == "001"
    assert report.a_grade_samples[0]["analyzer_identity_source"] == "prefixed_mode2_tokens_json"
    reasons = ";".join(row["formal_reject_reasons"] for row in report.rejected_samples)
    assert "non_open_flow_pressure_mode(sealed_controlled)" in reasons
    assert "mode2_contract_fail" in reasons
    assert report.formal_fit_boundary["fit_input_grade"] == "A"
    assert report.formal_fit_boundary["analyzer_device_id"] == "001"


def test_open_flow_report_uses_device_id_not_channel_label_for_ga02():
    rows = [
        _row(
            i,
            ga01_mode2_contract_status="missing",
            ga01_mode2_qc_status="missing",
            ga02_frame_usable=True,
            ga02_mode2_contract_status="pass",
            ga02_mode2_qc_status="pass",
            ga02_mode2_tokens_json=json.dumps(
                ["YGAS", "033", "0900.000", "00.500", "1768.000", "00.410"],
                separators=(",", ":"),
            ),
            ga02_raw="YGAS,033,...",
            ga02_ref_signal=3322.0,
            ga02_co2_signal=4356.0,
            ga02_h2o_signal=2631.0,
            ga02_chamber_temp_c=25.0 + i * 0.001,
            ga02_case_temp_c=25.5,
            ga02_pressure_kpa=100.05 + i * 0.0005,
            ga02_co2_ratio_f=1.3000 + i * 0.0001,
            ga02_co2_ppm=900.0 + i * 0.01,
            ga02_h2o_ratio_f=0.7000,
            ga02_h2o_mmol=0.5,
        )
        for i in range(1, 11)
    ]

    report = build_formal_open_flow_report(
        plan=_plan(),
        sample_rows=rows,
        pressure_check_rows=rows,
        component="co2",
        analyzer_prefix="ga02",
        cfg=FormalOpenFlowConfig(min_a_grade_samples=10),
    )

    assert report.candidate_fit_allowed is True
    assert report.qc_summary["analyzer_prefix"] == "ga02"
    assert report.qc_summary["analyzer_device_id"] == "033"
    assert report.a_grade_samples[0]["analyzer_prefix"] == "ga02"
    assert report.a_grade_samples[0]["analyzer_device_id"] == "033"
    assert report.a_grade_samples[0]["analyzer_device_id"] != "ga02"


def test_open_flow_report_blocks_candidate_fit_when_pressure_check_fails():
    rows = [_row(i, pressure_kpa=101.5, ref_hpa=1000.0) for i in range(1, 11)]

    report = build_formal_open_flow_report(
        plan=_plan(),
        sample_rows=rows,
        component="co2",
    )

    assert report.candidate_fit_allowed is False
    assert "pressure_channel_quick_check_not_passed" in report.candidate_fit_blockers
    assert report.qc_summary["a_grade_count"] == 0


def test_classification_marks_h2o_drifting_dewpoint_window_as_b_not_fit_input():
    rows = [_row(i, dewpoint_c=-30 + i * 0.1) for i in range(1, 11)]
    pressure = evaluate_pressure_channel_quick_check(rows)
    classes, a_rows, rejected, summary = classify_open_flow_samples(
        rows,
        component="h2o",
        pressure_check=pressure,
        cfg=FormalOpenFlowConfig(dewpoint_span_c_max=0.2),
    )

    assert not rejected
    assert not a_rows
    assert summary["b_grade_count"] == 10
    assert all(item.grade == "B" for item in classes)
    assert any("dewpoint_c_span" in reason for reason in classes[0].warning_reasons)


def test_classification_keeps_co2_dry_route_dewpoint_drift_as_report_warning():
    rows = [_row(i, dewpoint_c=-30 + i * 0.1) for i in range(1, 11)]
    pressure = evaluate_pressure_channel_quick_check(rows)
    classes, a_rows, rejected, summary = classify_open_flow_samples(
        rows,
        component="co2",
        pressure_check=pressure,
        cfg=FormalOpenFlowConfig(dewpoint_span_c_max=0.2),
    )

    assert len(a_rows) == 10
    assert not rejected
    assert summary["a_grade_count"] == 10
    assert all(item.grade == "A" for item in classes)
    assert any("dry_route_dewpoint_report_only" in reason for reason in classes[0].report_warning_reasons)


def test_classification_keeps_analyzer_pressure_span_as_report_warning_by_default():
    rows = [_row(i, pressure_kpa=100.0 + i * 0.05, ref_hpa=1000.0) for i in range(1, 11)]
    pressure_rows = [_row(i, pressure_kpa=100.05 + i * 0.0001, ref_hpa=1000.5 + i * 0.001) for i in range(1, 5)]
    pressure = evaluate_pressure_channel_quick_check(pressure_rows)

    classes, a_rows, rejected, summary = classify_open_flow_samples(
        rows,
        component="co2",
        pressure_check=pressure,
        cfg=FormalOpenFlowConfig(analyzer_pressure_span_hpa_max=2.0),
    )

    assert len(a_rows) == 10
    assert not rejected
    assert all(item.grade == "A" for item in classes)
    assert all("analyzer_pressure_hpa_span" not in ";".join(item.warning_reasons) for item in classes)
    assert any("pressure_not_polynomial_fit_variable" in reason for reason in classes[0].report_warning_reasons)
    assert summary["pressure_condition_warning_count"] == 1


def test_classification_keeps_output_extreme_as_report_warning_when_ratio_signal_qc_passes():
    rows = [
        _row(
            i,
            ga01_frame_status="极值已标记",
            ga01_co2_ppm=3000.0,
            ga01_h2o_mmol=72.0,
            ga01_co2_ratio_f=1.3000 + i * 0.0001,
            ga01_co2_ratio_raw=1.2999 + i * 0.0001,
        )
        for i in range(1, 11)
    ]
    pressure = evaluate_pressure_channel_quick_check(rows)

    classes, a_rows, rejected, summary = classify_open_flow_samples(
        rows,
        component="co2",
        pressure_check=pressure,
        cfg=FormalOpenFlowConfig(co2_ratio_span_max=0.01),
    )

    assert len(a_rows) == 10
    assert not rejected
    assert summary["a_grade_count"] == 10
    assert all(item.grade == "A" for item in classes)
    assert all(
        "component_output_extreme_report_only;ratio_signal_fit_input_allowed"
        in item.report_warning_reasons
        for item in classes
    )


def test_open_flow_report_keeps_good_analyzer_when_another_channel_has_failed_quality():
    rows = [
        _row(
            i,
            ga01_frame_usable=False,
            ga01_mode2_contract_status="fail",
            ga01_mode2_qc_status="fail",
            ga01_co2_ratio_f=0.0,
            ga01_co2_ppm=0.0,
            point_quality_status="fail",
            ga02_point_quality_status="pass",
            ga02_frame_usable=True,
            ga02_mode2_contract_status="pass",
            ga02_mode2_qc_status="pass",
            ga02_mode2_tokens_json=json.dumps(
                ["YGAS", "033", "0900.000", "00.500", "1768.000", "00.410"],
                separators=(",", ":"),
            ),
            ga02_raw="YGAS,033,...",
            ga02_ref_signal=3322.0,
            ga02_co2_signal=4356.0,
            ga02_h2o_signal=2631.0,
            ga02_chamber_temp_c=25.0 + i * 0.001,
            ga02_case_temp_c=25.5,
            ga02_pressure_kpa=100.05 + i * 0.0005,
            ga02_co2_ratio_f=1.3000 + i * 0.0001,
            ga02_co2_ppm=900.0 + i * 0.01,
            ga02_h2o_ratio_f=0.7000,
            ga02_h2o_mmol=0.5,
        )
        for i in range(1, 11)
    ]

    report = build_formal_open_flow_report(
        plan=_plan(),
        sample_rows=rows,
        pressure_check_rows=rows,
        component="co2",
        analyzer_prefix="ga02",
        cfg=FormalOpenFlowConfig(min_a_grade_samples=10),
    )

    assert report.candidate_fit_allowed is True
    assert report.qc_summary["analyzer_device_id"] == "033"
    assert report.qc_summary["a_grade_count"] == 10
    assert all(row["analyzer_prefix"] == "ga02" for row in report.a_grade_samples)
    assert all(row["analyzer_device_id"] == "033" for row in report.a_grade_samples)


def test_analyzer_gate_status_is_applied_per_device_not_whole_point():
    statuses = [
        {
            "label": "ga01",
            "stable": False,
            "dropped": False,
            "stable_window_span": 0.002,
            "stable_window_span_after_spike_filter": 0.002,
            "stability_spike_filtered_count": 0,
        },
        {
            "label": "ga02",
            "stable": True,
            "dropped": False,
            "stable_window_span": 0.0018,
            "stable_window_span_after_spike_filter": 0.0007,
            "stability_spike_filtered_count": 1,
        },
    ]
    rows = [
        _row(
            i,
            analyzer_gate_per_analyzer_status=json.dumps(statuses, separators=(",", ":")),
            analyzer_gate_final_decision_reason="min_valid_not_met",
            ga02_frame_usable=True,
            ga02_mode2_contract_status="pass",
            ga02_mode2_qc_status="pass",
            ga02_mode2_tokens_json=json.dumps(
                ["YGAS", "022", "0900.000", "00.500", "1768.000", "00.410"],
                separators=(",", ":"),
            ),
            ga02_raw="YGAS,022,...",
            ga02_ref_signal=3322.0,
            ga02_co2_signal=4356.0,
            ga02_h2o_signal=2631.0,
            ga02_chamber_temp_c=25.0 + i * 0.001,
            ga02_case_temp_c=25.5,
            ga02_pressure_kpa=100.05 + i * 0.0005,
            ga02_co2_ratio_f=1.3000 + i * 0.0001,
            ga02_co2_ppm=900.0 + i * 0.01,
            ga02_h2o_ratio_f=0.7000,
            ga02_h2o_mmol=0.5,
        )
        for i in range(1, 11)
    ]
    pressure = evaluate_pressure_channel_quick_check(rows, analyzer_prefix="ga02")

    ga01_classes, ga01_a_rows, _ga01_rejected, ga01_summary = classify_open_flow_samples(
        rows,
        component="co2",
        analyzer_prefix="ga01",
        pressure_check=pressure,
        cfg=FormalOpenFlowConfig(min_a_grade_samples=10),
    )
    ga02_classes, ga02_a_rows, _ga02_rejected, ga02_summary = classify_open_flow_samples(
        rows,
        component="co2",
        analyzer_prefix="ga02",
        pressure_check=pressure,
        cfg=FormalOpenFlowConfig(min_a_grade_samples=10),
    )

    assert not ga01_a_rows
    assert ga01_summary["b_grade_count"] == 10
    assert "analyzer_ratio_gate_not_stable_for_device" in ga01_classes[0].warning_reasons
    assert len(ga02_a_rows) == 10
    assert ga02_summary["a_grade_count"] == 10
    assert any("analyzer_ratio_gate_spike_filtered_count=1" in reason for reason in ga02_classes[0].report_warning_reasons)


def test_sealed_diagnostic_rows_do_not_pollute_open_flow_window_statistics():
    rows = [_row(i) for i in range(1, 11)]
    rows.append(
        _row(
            101,
            pressure_mode="sealed_controlled",
            ga01_co2_ratio_f=9.0,
            dewpoint_c=10.0,
        )
    )
    pressure = evaluate_pressure_channel_quick_check(rows[:10])

    classes, a_rows, rejected, summary = classify_open_flow_samples(
        rows,
        component="co2",
        pressure_check=pressure,
        cfg=FormalOpenFlowConfig(co2_ratio_span_max=0.01, dewpoint_span_c_max=0.2),
    )

    assert len(a_rows) == 10
    assert len(rejected) == 1
    assert summary["b_grade_count"] == 0
    assert all(item.grade == "A" for item in classes[:10])
    assert classes[-1].grade == "REJECT"


def test_sample_readiness_warns_on_legacy_rows_without_route_or_purge_evidence():
    rows = [_row(i) for i in range(1, 11)]

    report = build_formal_open_flow_report(
        plan=_plan(),
        sample_rows=rows,
        pressure_check_rows=rows,
        component="co2",
        cfg=FormalOpenFlowConfig(min_a_grade_samples=10),
    )

    assert report.candidate_fit_allowed is True
    assert report.sample_readiness["readiness_status"] == "warn"
    assert "route_open_until_sample_end_not_recorded" in report.sample_readiness["warnings"]
    assert "purge_evidence_not_recorded" in report.sample_readiness["warnings"]


def test_sample_readiness_blocks_candidate_fit_when_route_closed_before_sample_end():
    rows = [
        _row(
            i,
            route_open_until_sample_end=False,
            actual_purge_s=600,
            minimum_purge_s=360,
        )
        for i in range(1, 11)
    ]

    report = build_formal_open_flow_report(
        plan=_plan(),
        sample_rows=rows,
        pressure_check_rows=rows,
        component="co2",
        cfg=FormalOpenFlowConfig(min_a_grade_samples=10),
    )

    assert report.candidate_fit_allowed is False
    assert "sample_readiness_failed" in report.candidate_fit_blockers
    assert "route_not_open_until_sample_end" in report.sample_readiness["blockers"]


def test_sample_readiness_blocks_candidate_fit_when_minimum_purge_is_not_met():
    rows = [
        _row(
            i,
            route_open_until_sample_end=True,
            actual_purge_s=180,
            minimum_purge_s=360,
        )
        for i in range(1, 11)
    ]

    report = build_formal_open_flow_report(
        plan=_plan(),
        sample_rows=rows,
        pressure_check_rows=rows,
        component="co2",
        cfg=FormalOpenFlowConfig(min_a_grade_samples=10),
    )

    assert report.candidate_fit_allowed is False
    assert "sample_readiness_failed" in report.candidate_fit_blockers
    assert any(reason.startswith("minimum_purge_not_met") for reason in report.sample_readiness["blockers"])


def test_sample_readiness_passes_when_route_purge_and_physical_gates_are_recorded():
    rows = [
        _row(
            i,
            route_open_until_sample_end=True,
            actual_purge_s=600,
            minimum_purge_s=360,
        )
        for i in range(1, 11)
    ]

    report = build_formal_open_flow_report(
        plan=_plan(),
        sample_rows=rows,
        pressure_check_rows=rows,
        component="co2",
        cfg=FormalOpenFlowConfig(min_a_grade_samples=10),
    )

    assert report.candidate_fit_allowed is True
    assert report.sample_readiness["readiness_status"] == "pass"
    assert report.qc_summary["sample_readiness_status"] == "pass"
    assert report.point_calibratability["calibratability_grade"] == "A"
    assert report.point_calibratability["fit_input_role"] == "direct_fit"
    assert (
        report.point_calibratability["time_optimization_action"]
        == "sample_now_do_not_chase_lower_dewpoint"
    )


def test_co2_stable_wet_state_is_reported_as_normalized_review_point():
    rows = [
        _row(
            i,
            route_open_until_sample_end=True,
            actual_purge_s=600,
            minimum_purge_s=360,
            dewpoint_c=-15.0 + i * 0.001,
            ga01_h2o_mmol=25.0 + i * 0.001,
        )
        for i in range(1, 11)
    ]

    report = build_formal_open_flow_report(
        plan=_plan(),
        sample_rows=rows,
        pressure_check_rows=rows,
        component="co2",
        cfg=FormalOpenFlowConfig(min_a_grade_samples=10),
    )

    assert report.candidate_fit_allowed is True
    assert report.point_calibratability["calibratability_grade"] == "B"
    assert report.point_calibratability["fit_input_role"] == "state_normalized_fit_review"
    assert (
        report.point_calibratability["time_optimization_action"]
        == "sample_now_with_h2o_state_normalization"
    )
    assert report.qc_summary["point_calibratability_grade"] == "B"
