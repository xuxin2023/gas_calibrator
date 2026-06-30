import csv
import json

from gas_calibrator.validation.co2_s13_error_root_cause_resolution_review import (
    write_co2_s13_error_root_cause_resolution_review,
)


def _write_csv(path, rows):
    headers = []
    for row in rows:
        for key in row:
            if key not in headers:
                headers.append(key)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=headers)
        writer.writeheader()
        writer.writerows(rows)


def _fit_rows():
    rows = []
    for device in ("001", "002", "003"):
        for point, target, ratio_std, dewpoint in (
            ("T20_100ppm", 99.94, 0.00004, -35.2),
            ("T20_400ppm", 399.56, 0.00005, -34.7),
            ("T30_100ppm", 99.94, 0.00004, -33.9),
        ):
            rows.append(
                {
                    "component": "co2",
                    "analyzer_device_id": device,
                    "source_role": "fit",
                    "point_identity": point,
                    "target_value": f"{target}",
                    "ratio": "1.234",
                    "temperature_c": "20.0",
                    "pressure_hpa": "1010.0",
                    "co2_ratio_f_std": f"{ratio_std}",
                    "dewpoint_mean_c": f"{dewpoint}",
                    "h2o_mmol_mean": "72.0",
                    "sample_count": "10",
                    "usable_sample_count": "10",
                }
            )
    return rows


def _baseline_review_rows():
    best = []
    s5_best = []
    residuals = []
    errors = {"001": -4.0, "002": -5.0, "003": -6.0}
    for device, error in errors.items():
        best.append(
            {
                "device_id": device,
                "strategy_id": "baseline",
                "max_abs_relative_error_percent": abs(error),
                "low_end_max_abs_relative_error_percent": abs(error),
                "rmse_ppm": "2.0",
                "s1_payload_scientific": "1.0e0",
                "s3_payload_scientific": "2.0e0",
            }
        )
        s5_best.append(
            {
                "device_id": device,
                "strategy_id": "baseline",
                "s5_C0": "0.0",
                "s5_C1": "1.0",
                "s5_command_preview": "SENCO5,YGAS,FFF,0.000,1.000",
                "s5_max_abs_relative_error_percent": abs(error),
                "s5_worst_point_identity": "T20_100ppm",
            }
        )
        residuals.append(
            {
                "device_id": device,
                "strategy_id": "baseline",
                "point_identity": "T20_100ppm",
                "target_ppm": "99.94",
                "prediction_ppm": f"{99.94 + error}",
                "error_ppm": f"{error}",
                "relative_error_percent": f"{error / 99.94 * 100.0}",
                "s1s3_error_ppm_before_s5": f"{error}",
                "s1s3_relative_error_percent_before_s5": f"{error / 99.94 * 100.0}",
                "s5_C0": "0.0",
                "s5_C1": "1.0",
                "ratio": "1.234",
                "temperature_c": "20.0",
                "pressure_hpa": "1010.0",
                "h2o_mmol": "72.0",
            }
        )
        residuals.append(
            {
                "device_id": device,
                "strategy_id": "baseline",
                "point_identity": "T20_400ppm",
                "target_ppm": "399.56",
                "prediction_ppm": "400.0",
                "error_ppm": "0.44",
                "relative_error_percent": "0.11",
                "s1s3_error_ppm_before_s5": "0.44",
                "s1s3_relative_error_percent_before_s5": "0.11",
                "s5_C0": "0.0",
                "s5_C1": "1.0",
                "ratio": "1.5",
                "temperature_c": "20.0",
                "pressure_hpa": "1010.0",
                "h2o_mmol": "72.0",
            }
        )
    return best, s5_best, residuals


def test_root_cause_review_flags_good_physics_common_mode_point(tmp_path):
    fit_points = tmp_path / "fit_points.csv"
    baseline = tmp_path / "baseline"
    _write_csv(fit_points, _fit_rows())
    best, s5_best, residuals = _baseline_review_rows()
    _write_csv(baseline / "co2_s13_multistrategy_best_by_device.csv", best)
    _write_csv(baseline / "co2_s13_multistrategy_s5_best_by_device.csv", s5_best)
    _write_csv(baseline / "co2_s13_multistrategy_s5_best_residuals.csv", residuals)

    outputs = write_co2_s13_error_root_cause_resolution_review(
        fit_points_csv=fit_points,
        baseline_review_dir=baseline,
        output_dir=tmp_path / "out",
        run_sensitivity=False,
    )

    common = list(csv.DictReader(outputs["common_mode_points"].open(encoding="utf-8-sig")))
    t20_100 = next(row for row in common if row["point_identity"] == "T20_100ppm")
    assert t20_100["root_cause_class"] == "point_common_mode_model_or_target_state_boundary"
    assert t20_100["physical_state_label"] == "ratio_A_and_deep_dry"
    assert t20_100["auto_exclude"] == "False"

    meta = json.loads(outputs["metadata"].read_text(encoding="utf-8"))
    assert meta["boundary"]["opens_com_ports"] is False
    assert meta["boundary"]["controls_water_or_gas_routes"] is False
    assert meta["boundary"]["writes_coefficients"] is False
    assert meta["boundary"]["not_real_acceptance_evidence"] is True
    text = outputs["markdown"].read_text(encoding="utf-8-sig")
    assert "V1.5 CO2 S1/S3 大误差根因收敛评审" in text
    assert "同一温度/气点在多台设备上同向偏差" in text
