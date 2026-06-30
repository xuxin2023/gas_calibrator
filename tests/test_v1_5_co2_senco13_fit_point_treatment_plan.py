import csv

from gas_calibrator.validation.co2_senco13_fit_point_treatment_plan import (
    TreatmentPlanInputs,
    build_co2_senco13_fit_point_treatment_plan,
    write_co2_senco13_fit_point_treatment_plan,
)


def _write_csv(path, rows):
    header = []
    for row in rows:
        for key in row:
            if key not in header:
                header.append(key)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=header)
        writer.writeheader()
        writer.writerows(rows)


def _audit_rows():
    return [
        {
            "point_identity": "T20_600ppm",
            "target_ppm_median": "599.54",
            "devices": "001;002;003",
            "device_count": "3",
            "common_mode_status": "common_mode_suspect",
            "root_cause_hypothesis": "source_route_target_or_model_common_bias",
            "ratio_std_max": "0.00007",
            "dewpoint_c_mean": "-32.0",
            "dewpoint_derived_h2o_mmol_mean": "0.30",
            "h2o_mmol_mean": "48.0",
            "h2o_bridge_input_status": "do_not_use_analyzer_h2o_output_for_co2_bridge",
            "zero_anchor_classes": "standard_fit_point",
            "mean_error_ppm": "18.0",
            "max_abs_error_ppm": "21.0",
            "max_abs_relative_error_percent": "3.5",
        },
        {
            "point_identity": "T30_0ppm",
            "target_ppm_median": "0",
            "devices": "001;002;003",
            "device_count": "3",
            "common_mode_status": "common_mode_suspect",
            "root_cause_hypothesis": "estimated_zero_anchor_common_bias",
            "ratio_std_max": "0.00005",
            "dewpoint_c_mean": "-40.0",
            "dewpoint_derived_h2o_mmol_mean": "0.12",
            "h2o_mmol_mean": "48.0",
            "h2o_bridge_input_status": "do_not_use_analyzer_h2o_output_for_co2_bridge",
            "zero_anchor_classes": "estimated_zero_anchor",
            "mean_error_ppm": "7.0",
            "max_abs_error_ppm": "9.0",
            "max_abs_relative_error_percent": "",
        },
        {
            "point_identity": "T10_300ppm",
            "target_ppm_median": "299.73",
            "devices": "001;002;003",
            "device_count": "3",
            "common_mode_status": "not_common_mode",
            "root_cause_hypothesis": "not_common_mode",
            "ratio_std_max": "0.0015",
            "dewpoint_c_mean": "-31.0",
            "dewpoint_derived_h2o_mmol_mean": "0.34",
            "h2o_mmol_mean": "0.4",
            "h2o_bridge_input_status": "dewpoint_and_analyzer_h2o_consistent_for_bridge_review",
            "zero_anchor_classes": "standard_fit_point",
            "mean_error_ppm": "1.0",
            "max_abs_error_ppm": "1.2",
            "max_abs_relative_error_percent": "0.4",
        },
    ]


def test_fit_point_treatment_keeps_ratio_stable_points_and_disables_bad_h2o_bridge(tmp_path):
    audit_csv = tmp_path / "audit.csv"
    _write_csv(audit_csv, _audit_rows())

    tables = build_co2_senco13_fit_point_treatment_plan(
        inputs=TreatmentPlanInputs(common_mode_audit_csv=audit_csv)
    )
    by_point = {
        row["point_identity"]: row
        for row in tables["co2_senco13_fit_point_treatment_plan"]
    }

    assert by_point["T20_600ppm"]["fit_policy"] == "include_after_target_route_model_review"
    assert by_point["T20_600ppm"]["exclusion_basis"] == "do_not_exclude_by_uncalibrated_output"
    assert by_point["T20_600ppm"]["bridge_policy"] == "disable_h2o_bridge_for_s1s3"
    assert by_point["T30_0ppm"]["fit_policy"] == "include_as_zero_anchor_with_uncertainty"
    assert by_point["T10_300ppm"]["fit_policy"] == "hold_for_ratio_window_review"


def test_fit_point_treatment_writes_chinese_markdown_and_meta(tmp_path):
    audit_csv = tmp_path / "audit.csv"
    _write_csv(audit_csv, _audit_rows())

    outputs = write_co2_senco13_fit_point_treatment_plan(
        inputs=TreatmentPlanInputs(common_mode_audit_csv=audit_csv),
        output_dir=tmp_path / "out",
    )

    assert outputs["markdown"].exists()
    assert outputs["meta_json"].exists()
    text = outputs["markdown"].read_text(encoding="utf-8")
    assert "不能因为 CO2/H2O 输出不一致而剔除点" in text
    assert "S5" in text
