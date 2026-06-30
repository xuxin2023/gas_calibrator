import csv
import json

from gas_calibrator.validation.co2_s13_low_end_model_correction_review import (
    build_co2_s13_low_end_model_correction_review,
    write_co2_s13_low_end_model_correction_review,
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


def _fixture_dirs(tmp_path):
    model_dir = tmp_path / "model"
    anchor_dir = tmp_path / "anchor"
    residual_dir = tmp_path / "residual"
    _write_csv(
        model_dir / "co2_s13_selected_structure_candidates.csv",
        [
            {
                "device_id": "58",
                "best_structure_id": "core_plus_full_temp",
                "best_objective_id": "relative_irls_lstsq",
                "best_zero_offset_ppm": "5.0",
                "best_max_abs_relative_error_percent": "4.1",
                "best_low_end_max_abs_relative_error_percent": "4.1",
                "uses_pressure_terms": "False",
                "uses_s5_output_trim": "False",
                "auto_write_allowed": "False",
            },
            {
                "device_id": "70",
                "best_structure_id": "core_plus_full_temp",
                "best_objective_id": "absolute_lstsq",
                "best_zero_offset_ppm": "0.0",
                "best_max_abs_relative_error_percent": "1.2",
                "best_low_end_max_abs_relative_error_percent": "1.3",
                "uses_pressure_terms": "False",
                "uses_s5_output_trim": "False",
                "auto_write_allowed": "False",
            },
        ],
    )
    _write_csv(
        model_dir / "co2_s13_temperature_bias_diagnostic.csv",
        [
            {
                "device_id": "058",
                "temperature_group": "T20",
                "point_count": "11",
                "mean_error_ppm": "-4.0",
                "max_abs_error_ppm": "7.0",
                "same_sign_residual_fraction": "1.0",
                "temperature_model_review": "temperature_group_bias_candidate",
            },
            {
                "device_id": "070",
                "temperature_group": "T20",
                "point_count": "11",
                "mean_error_ppm": "-3.0",
                "max_abs_error_ppm": "6.0",
                "same_sign_residual_fraction": "1.0",
                "temperature_model_review": "temperature_group_bias_candidate",
            },
        ],
    )
    _write_csv(
        anchor_dir / "co2_s13_low_end_common_mode_audit.csv",
        [
            {
                "point_identity": "T20_100ppm",
                "device_count": "6",
                "target_ppm": "99.94",
                "temperature_c": "20.4",
                "mean_error_ppm": "-4.1",
                "same_sign_residual_fraction": "1.0",
                "ratio_grade_counts": "A:6",
                "dryness_grade_counts": "deep_dry:6",
                "common_mode_status": "common_mode_suspect",
            }
        ],
    )
    _write_csv(
        anchor_dir / "co2_s13_point_exclusion_sensitivity.csv",
        [
            {
                "excluded_point_identity": "T20_100ppm",
                "improved_device_count": "5",
                "worsened_device_count": "1",
                "mean_max_relative_error_improvement_percent_points": "0.82",
                "exclusion_interpretation": "deleting_this_point_often_improves_fit_but_not_uniformly",
                "auto_exclude_allowed": "False",
            }
        ],
    )
    _write_csv(
        anchor_dir / "co2_s13_zero_offset_selection_summary.csv",
        [
            {
                "zero_offset_ppm": "0",
                "mean_max_abs_relative_error_percent": "5.47",
                "best_max_abs_relative_error_percent": "3.82",
                "worst_max_abs_relative_error_percent": "9.88",
                "mean_low_end_max_abs_relative_error_percent": "5.41",
            },
            {
                "zero_offset_ppm": "5",
                "mean_max_abs_relative_error_percent": "5.80",
                "best_max_abs_relative_error_percent": "3.45",
                "worst_max_abs_relative_error_percent": "11.04",
                "mean_low_end_max_abs_relative_error_percent": "5.59",
            },
        ],
    )
    _write_csv(
        residual_dir / "co2_s13_low_end_pattern_summary.csv",
        [{"pattern": "T20_low_end_all_negative", "count": "6"}],
    )
    return model_dir, anchor_dir, residual_dir


def test_low_end_model_correction_review_keeps_s5_and_pressure_out(tmp_path):
    model_dir, anchor_dir, residual_dir = _fixture_dirs(tmp_path)

    tables = build_co2_s13_low_end_model_correction_review(
        model_structure_dir=model_dir,
        anchor_target_audit_dir=anchor_dir,
        residual_root_cause_dir=residual_dir,
    )

    assert tables["run_summary"][0]["opens_com_ports"] is False
    assert tables["run_summary"][0]["writes_coefficients"] is False
    assert tables["run_summary"][0]["uses_pressure_terms"] is False
    assert tables["run_summary"][0]["uses_s5_output_trim"] is False
    assert tables["device_model_decision"][0]["device_id"] == "058"
    assert "零气" in tables["device_model_decision"][0]["main_model_decision"]
    assert tables["device_model_decision"][1]["main_model_decision"] == (
        "S1/S3 主模型候选可进入写入前评审"
    )


def test_low_end_model_correction_review_requires_physical_cause_before_deleting(tmp_path):
    model_dir, anchor_dir, residual_dir = _fixture_dirs(tmp_path)

    tables = build_co2_s13_low_end_model_correction_review(
        model_structure_dir=model_dir,
        anchor_target_audit_dir=anchor_dir,
        residual_root_cause_dir=residual_dir,
    )
    low_end = tables["low_end_common_mode_decision"][0]

    assert low_end["common_mode_decision"] == "好物理状态下仍有共同偏差，优先查目标值/零气/S1S3形状"
    assert low_end["exclusion_decision"] == "高影响但不一致，不允许自动剔除"
    assert low_end["auto_exclude_allowed"] is False


def test_low_end_model_correction_review_writes_utf8_chinese_artifacts(tmp_path):
    model_dir, anchor_dir, residual_dir = _fixture_dirs(tmp_path)
    outputs = write_co2_s13_low_end_model_correction_review(
        model_structure_dir=model_dir,
        anchor_target_audit_dir=anchor_dir,
        residual_root_cause_dir=residual_dir,
        output_dir=tmp_path / "out",
    )

    meta = json.loads(outputs["metadata"].read_text(encoding="utf-8"))
    assert meta["boundary"]["opens_com_ports"] is False
    assert meta["boundary"]["writes_coefficients"] is False
    assert meta["boundary"]["uses_s5_output_trim"] is False
    text = outputs["markdown"].read_text(encoding="utf-8-sig")
    assert "V1.5 CO2 S1/S3 低端模型修正评审" in text
    assert "S5 输出层修正不参与 S1/S3 主链路判断" in text
    assert "乱码" not in text
