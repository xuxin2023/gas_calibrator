import csv
import json

from gas_calibrator.validation.co2_fit_algorithm_matrix import (
    build_co2_fit_algorithm_matrix_tables,
    write_co2_fit_algorithm_matrix_report,
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


def _co2_target(ratio, temp_c):
    temp_k = temp_c + 273.15
    return 1.0 + 25.0 * ratio - 3.0 * ratio**2 + 0.05 * temp_k + 0.01 * ratio * temp_k


def _fit_rows():
    rows = []
    for temp_c in (-20.0, 0.0, 20.0, 40.0):
        for ratio in (0.15, 0.55, 0.95, 1.35):
            rows.append(
                {
                    "component": "co2",
                    "analyzer_device_id": "030",
                    "analyzer_prefix": "GA03",
                    "source_role": "fit",
                    "point_identity": f"fit_T{temp_c}_R{ratio}",
                    "target_value": f"{_co2_target(ratio, temp_c):.12f}",
                    "ratio": f"{ratio:.12f}",
                    "temperature_c": f"{temp_c:.3f}",
                    "pressure_hpa": "1013.1",
                }
            )
    for temp_c, ratio in ((10.0, 0.75), (30.0, 1.15)):
        rows.append(
            {
                "component": "co2",
                "analyzer_device_id": "030",
                "analyzer_prefix": "GA03",
                "source_role": "verification",
                "point_identity": f"verify_T{temp_c}_R{ratio}",
                "target_value": f"{_co2_target(ratio, temp_c):.12f}",
                "ratio": f"{ratio:.12f}",
                "temperature_c": f"{temp_c:.3f}",
                "pressure_hpa": "1013.2",
            }
        )
    return rows


def _tables(tmp_path):
    evidence = tmp_path / "fit_points.csv"
    snapshot = tmp_path / "old_getco.json"
    _write_csv(evidence, _fit_rows())
    snapshot.write_text(
        json.dumps(
            {
                "030": {
                    "GETCO1_before_live": [10.0, 20.0, 30.0, 40.0, 0.0, 0.0],
                    "GETCO3_before_live": [1.0, 2.0, 3.0, -40.6241, 0.0856692, 6.0],
                }
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    return build_co2_fit_algorithm_matrix_tables(
        fit_residuals_csv=evidence,
        old_snapshot_json=snapshot,
    )


def test_temperature_terms_are_preferred_over_ratio_only_for_multitemperature_data(tmp_path):
    tables = _tables(tmp_path)
    summary = {
        row["model_id"]: row
        for row in tables["co2_fit_algorithm_matrix_summary"]
        if row["device_id"] == "030"
    }

    assert (
        float(summary["senco13_temperature_terms_pressure_zero"]["fit_rmse_ppm"])
        < float(summary["senco1_ratio_only"]["fit_rmse_ppm"])
    )
    assert (
        float(summary["senco13_temperature_terms_pressure_zero"]["verification_rmse_ppm"])
        < float(summary["senco1_ratio_only"]["verification_rmse_ppm"])
    )
    assert summary["senco13_temperature_terms_pressure_zero"]["fit_basis"] == (
        "centered_R_T_transformed_to_firmware_absolute_terms"
    )
    assert float(summary["senco13_temperature_terms_pressure_zero"]["condition_number_scaled"]) < float(
        summary["senco13_temperature_terms_pressure_zero"]["absolute_condition_number_scaled"]
    )


def test_current_atmosphere_data_blocks_new_pressure_terms_and_zeroes_target_slots(tmp_path):
    tables = _tables(tmp_path)
    summary = {
        row["model_id"]: row
        for row in tables["co2_fit_algorithm_matrix_summary"]
        if row["device_id"] == "030"
    }

    assert (
        summary["legacy_v1_a0_a8_full_rt_p_kpa"]["recommendation_status"]
        == "blocked_pressure_span_insufficient_for_pressure_terms"
    )
    payload = json.loads(summary["senco13_temperature_terms_pressure_zero"]["secondary_payload"])
    assert payload[3:] == [0.0, 0.0, 0.0]
    assert "P" not in summary["senco13_temperature_terms_pressure_zero"]["terms"].split(";")
    assert "RTP" not in summary["senco13_temperature_terms_pressure_zero"]["terms"].split(";")


def test_report_records_zero_gas_and_no_write_boundaries(tmp_path):
    evidence = tmp_path / "fit_points.csv"
    rows = _fit_rows()
    rows.append(
        {
            "component": "co2",
            "analyzer_device_id": "030",
            "source_role": "fit",
            "point_identity": "zero_gas_anchor",
            "target_value": "0.0",
            "zero_anchor_class": "estimated_zero_anchor",
            "target_uncertainty_ppm": "10.0",
            "ratio": "0.0",
            "temperature_c": "20.0",
            "pressure_hpa": "1013.2",
        }
    )
    _write_csv(evidence, rows)

    outputs = write_co2_fit_algorithm_matrix_report(
        fit_residuals_csv=evidence,
        output_dir=tmp_path / "matrix",
        exclude_device_ids=["023"],
    )

    assert outputs["markdown"].exists()
    meta = json.loads(outputs["meta_json"].read_text(encoding="utf-8"))
    assert meta["boundary"] == {
        "opens_com_ports": False,
        "controls_water_or_gas_routes": False,
        "writes_coefficients": False,
    }
    recommendation = list(csv.DictReader(outputs["co2_fit_algorithm_matrix_recommendation_csv"].open(encoding="utf-8-sig")))
    zero_row = {row["recommendation_item"]: row for row in recommendation}["zero_gas_anchor"]
    assert zero_row["status"] == "included_as_estimated_zero_anchor_for_sensitivity_not_formal_release"
    summary = list(csv.DictReader(outputs["co2_fit_algorithm_matrix_summary_csv"].open(encoding="utf-8-sig")))
    selected = [
        row
        for row in summary
        if row["device_id"] == "030" and row["model_id"] == "senco13_temperature_terms_pressure_zero"
    ][0]
    assert selected["zero_anchor_policy_status"] == "estimated_zero_anchor_sensitivity_only_not_formal_release"
    assert selected["estimated_zero_anchor_count"] == "1"
