import csv
import json

from gas_calibrator.validation.co2_s13_model_structure_review import (
    build_co2_s13_model_structure_review,
    write_co2_s13_model_structure_review,
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


def _target(ratio, temp_c):
    temp_k = temp_c + 273.15
    return (
        8.0
        + 720.0 * ratio
        + 85.0 * ratio**2
        - 6.0 * ratio**3
        + 0.11 * temp_k
        - 0.00008 * temp_k**2
        + 0.16 * ratio * temp_k
    )


def _fit_rows(*, with_bad_point=False, with_common_low_bias=False):
    rows = []
    for temp_c in (-20.0, 0.0, 20.0, 40.0):
        rows.append(
            {
                "component": "co2",
                "analyzer_device_id": "123",
                "analyzer_prefix": "GA01",
                "source_role": "fit",
                "point_identity": f"T{temp_c:g}_0ppm",
                "target_value": "0.0",
                "zero_anchor_class": "estimated_zero_anchor",
                "target_uncertainty_ppm": "8.0",
                "ratio": f"{0.002 + 0.00001 * temp_c:.12f}",
                "temperature_c": f"{temp_c:.3f}",
                "pressure_hpa": "1013.1",
                "co2_ratio_f_std": "0.00015",
                "dewpoint_c_mean": "-32.0",
            }
        )
        for ratio in (0.08, 0.18, 0.36, 0.62, 0.88, 1.1):
            value = _target(ratio, temp_c)
            identity = f"T{temp_c:g}_R{ratio:g}"
            ratio_std = "0.0002"
            dewpoint = "-32.0"
            if with_bad_point and temp_c == 20.0 and ratio == 0.18:
                value += 35.0
                identity = "T20_200ppm_bad_physics"
                ratio_std = "0.0025"
                dewpoint = "-15.0"
            if with_common_low_bias and temp_c == 20.0 and ratio == 0.08:
                value -= 5.0
                identity = "T20_100ppm"
            rows.append(
                {
                    "component": "co2",
                    "analyzer_device_id": "123",
                    "analyzer_prefix": "GA01",
                    "source_role": "fit",
                    "point_identity": identity,
                    "target_value": f"{value:.12f}",
                    "ratio": f"{ratio:.12f}",
                    "temperature_c": f"{temp_c:.3f}",
                    "pressure_hpa": "1013.1",
                    "co2_ratio_f_std": ratio_std,
                    "dewpoint_c_mean": dewpoint,
                }
            )
    return rows


def test_full_temperature_structure_beats_ratio_only_without_pressure_or_s5(tmp_path):
    evidence = tmp_path / "fit_points.csv"
    _write_csv(evidence, _fit_rows())

    tables = build_co2_s13_model_structure_review(
        fit_points_csv=evidence,
        structures=("core_ratio_only_diagnostic", "core_plus_full_temp"),
        objectives=("absolute_lstsq",),
        zero_offsets_ppm=(0.0,),
    )

    summary = {
        row["structure_id"]: row
        for row in tables["structure_summary"]
        if row["device_id"] == "123"
    }
    selected = tables["selected_structure_candidates"][0]

    assert float(summary["core_plus_full_temp"]["rmse_ppm"]) < float(
        summary["core_ratio_only_diagnostic"]["rmse_ppm"]
    )
    assert selected["best_structure_id"] == "core_plus_full_temp"
    assert selected["uses_pressure_terms"] is False
    assert selected["uses_s5_output_trim"] is False
    assert selected["auto_write_allowed"] is False
    assert tables["model_capacity_boundary"]
    assert tables["selected_low_end_common_mode_patterns"]


def test_bad_physics_point_is_flagged_for_degrade_not_silent_delete(tmp_path):
    evidence = tmp_path / "fit_points.csv"
    _write_csv(evidence, _fit_rows(with_bad_point=True))

    tables = build_co2_s13_model_structure_review(
        fit_points_csv=evidence,
        structures=("core_plus_full_temp",),
        objectives=("absolute_lstsq", "low_end_priority_lstsq"),
        zero_offsets_ppm=(0.0,),
    )

    influence = {
        row["point_identity"]: row
        for row in tables["point_influence_review"]
        if row["point_identity"] == "T20_200ppm_bad_physics"
    }
    recommendations = {
        row["point_identity"]: row
        for row in tables["point_treatment_recommendations"]
        if row["point_identity"] == "T20_200ppm_bad_physics"
    }

    assert influence
    assert influence["T20_200ppm_bad_physics"]["physical_qc_label"] == "physical_qc_degrade_candidate"
    assert influence["T20_200ppm_bad_physics"]["exclusion_recommendation"] == (
        "degrade_or_exclude_if_raw_evidence_confirms_bad_physical_state"
    )
    assert recommendations
    assert recommendations["T20_200ppm_bad_physics"]["auto_exclude"] is False


def test_structure_review_writes_no_write_artifacts_with_bom(tmp_path):
    evidence = tmp_path / "fit_points.csv"
    _write_csv(evidence, _fit_rows(with_common_low_bias=True))

    outputs = write_co2_s13_model_structure_review(
        fit_points_csv=evidence,
        output_dir=tmp_path / "review",
        structures=("core_plus_linear_temp", "core_plus_full_temp"),
        objectives=("absolute_lstsq", "relative_weighted_lstsq"),
        zero_offsets_ppm=(0.0, 5.0),
    )

    meta = json.loads(outputs["metadata"].read_text(encoding="utf-8"))
    assert meta["boundary"] == {
        "opens_com_ports": False,
        "controls_water_or_gas_routes": False,
        "writes_coefficients": False,
        "uses_pressure_terms": False,
        "uses_s5_output_trim": False,
        "not_real_acceptance_evidence": True,
    }
    selected = list(csv.DictReader(outputs["selected_structure_candidates"].open(encoding="utf-8-sig")))
    assert selected
    assert selected[0]["best_s1_payload_scientific"]
    assert selected[0]["best_s3_payload_scientific"]
    capacity = list(csv.DictReader(outputs["model_capacity_boundary"].open(encoding="utf-8-sig")))
    assert capacity
    assert "model_capacity_status" in capacity[0]
    common = list(csv.DictReader(outputs["selected_low_end_common_mode_patterns"].open(encoding="utf-8-sig")))
    assert common
    assert "common_mode_status" in common[0]
    assert outputs["markdown"].read_bytes()[:3] == b"\xef\xbb\xbf"
    markdown_text = outputs["markdown"].read_text(encoding="utf-8-sig")
    assert "V1.5 CO2 S1/S3 主模型结构修正评审" in markdown_text
    assert "S5 输出层线性修正不参与本轮 S1/S3 主模型判断" in markdown_text
    assert "乱码" not in markdown_text
