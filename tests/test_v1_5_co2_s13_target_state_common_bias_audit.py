import csv
import json

from gas_calibrator.validation.co2_s13_target_state_common_bias_audit import (
    build_co2_s13_target_state_common_bias_audit,
    write_co2_s13_target_state_common_bias_audit,
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


def _fit_points():
    rows = []
    for device in ("058", "070", "082"):
        for point, target, temp, dew, ratio_std in (
            ("T20_0ppm", 0.0, 20.0, -30.5, 0.0002),
            ("T20_100ppm", 100.0, 20.0, -29.8, 0.0002),
            ("T20_400ppm", 400.0, 20.0, -30.0, 0.0002),
            ("T40_100ppm", 100.0, 40.0, -27.0, 0.0007),
            ("T40_900ppm", 900.0, 40.0, -26.5, 0.0008),
        ):
            rows.append(
                {
                    "component": "co2",
                    "analyzer_device_id": device,
                    "point_identity": point,
                    "target_value": target,
                    "source_nominal_ppm": target,
                    "temp_set_c": temp,
                    "ratio": 1.0 + target / 1000.0,
                    "temperature_c": temp,
                    "pressure_hpa": 1013.2,
                    "zero_anchor_class": "estimated_zero_anchor" if target == 0.0 else "standard_fit_point",
                    "sample_count": 20,
                    "usable_sample_count": 20,
                    "co2_ratio_f_std": ratio_std,
                    "thermometer_temp_mean_c": temp,
                    "dewpoint_mean_c": dew,
                    "h2o_mmol_mean": 1.0,
                    "fit_inclusion_status": "included",
                    "status_register_qc_values": "0",
                }
            )
    return rows


def _residuals():
    rows = []
    # S1/S3 prediction has a shared low-end / T40 bias. A final affine S5 trim
    # can correct the simple display-layer bias in this synthetic fixture.
    for device in ("058", "070", "082"):
        for point, target in (
            ("T20_0ppm", 0.0),
            ("T20_100ppm", 100.0),
            ("T20_400ppm", 400.0),
            ("T40_100ppm", 100.0),
            ("T40_900ppm", 900.0),
        ):
            error = -3.0 if target == 0.0 else target * 0.03 + 2.0
            rows.append(
                {
                    "device_id": device,
                    "objective_id": "low_end_priority_lstsq",
                    "zero_offset_ppm": 0.0,
                    "model_id": "m",
                    "source_role": "fit",
                    "point_identity": point,
                    "target_ppm": target,
                    "zero_anchor_class": "estimated_zero_anchor" if target == 0.0 else "standard_fit_point",
                    "prediction_ppm": target + error,
                    "error_ppm": error,
                    "relative_error_percent": "" if target == 0.0 else error / target * 100.0,
                    "ratio": 1.0 + target / 1000.0,
                    "temperature_c": 20.0,
                    "pressure_hpa": 1013.2,
                    "h2o_mmol": 1.0,
                    "strategy_id": "s",
                    "structure_id": "core_plus_full_temp",
                    "diagnostic_only": False,
                    "selected_strategy": True,
                    "uses_pressure_terms": False,
                    "uses_s5_output_trim": False,
                }
            )
    return rows


def _best_rows():
    return [
        {
            "device_id": dev,
            "best_strategy_id": "s",
            "structure_id": "core_plus_full_temp",
            "objective_id": "low_end_priority_lstsq",
            "zero_offset_ppm": 0.0,
            "max_abs_relative_error_percent": 4.0,
        }
        for dev in ("058", "070", "082")
    ]


def test_target_state_common_bias_audit_keeps_no_write_boundary(tmp_path):
    fit_csv = tmp_path / "fit.csv"
    residuals_csv = tmp_path / "residuals.csv"
    best_csv = tmp_path / "best.csv"
    _write_csv(fit_csv, _fit_points())
    _write_csv(residuals_csv, _residuals())
    _write_csv(best_csv, _best_rows())

    tables = build_co2_s13_target_state_common_bias_audit(
        fit_points_csv=fit_csv,
        selected_residuals_csv=residuals_csv,
        best_by_device_csv=best_csv,
    )

    summary = tables["run_summary"][0]
    assert summary["opens_com_ports"] is False
    assert summary["controls_water_or_gas_routes"] is False
    assert summary["writes_coefficients"] is False
    assert summary["uses_pressure_terms"] is False
    assert summary["s5_evaluation_theoretical_only"] is True


def test_target_state_common_bias_classifies_shared_low_end_and_physical_state(tmp_path):
    fit_csv = tmp_path / "fit.csv"
    residuals_csv = tmp_path / "residuals.csv"
    best_csv = tmp_path / "best.csv"
    _write_csv(fit_csv, _fit_points())
    _write_csv(residuals_csv, _residuals())
    _write_csv(best_csv, _best_rows())

    tables = build_co2_s13_target_state_common_bias_audit(
        fit_points_csv=fit_csv,
        selected_residuals_csv=residuals_csv,
        best_by_device_csv=best_csv,
    )

    zero_point = next(row for row in tables["point_common_bias"] if row["point_identity"] == "T20_0ppm")
    assert "零气 CO2" in zero_point["root_cause_candidate"]
    t40_point = next(row for row in tables["point_common_bias"] if row["point_identity"] == "T40_900ppm")
    assert "露点" in t40_point["root_cause_candidate"]
    assert "ratio" in t40_point["root_cause_candidate"]


def test_target_state_common_bias_reports_theoretical_s5_capability(tmp_path):
    fit_csv = tmp_path / "fit.csv"
    residuals_csv = tmp_path / "residuals.csv"
    best_csv = tmp_path / "best.csv"
    _write_csv(fit_csv, _fit_points())
    _write_csv(residuals_csv, _residuals())
    _write_csv(best_csv, _best_rows())

    tables = build_co2_s13_target_state_common_bias_audit(
        fit_points_csv=fit_csv,
        selected_residuals_csv=residuals_csv,
        best_by_device_csv=best_csv,
    )

    assert tables["s5_theoretical_trim_by_device"]
    for row in tables["s5_theoretical_trim_by_device"]:
        assert row["theoretical_only"] is True
        assert row["writes_coefficients"] is False
        assert float(row["post_s5_max_abs_relative_error_percent"]) <= 1.0
        assert str(row["command_preview"]).startswith("SENCO5,YGAS,FFF,")


def test_target_state_common_bias_excludes_diagnostic_source_role_from_s5(tmp_path):
    fit_csv = tmp_path / "fit.csv"
    residuals_csv = tmp_path / "residuals.csv"
    best_csv = tmp_path / "best.csv"
    residuals = _residuals()
    residuals.append(
        {
            "device_id": "058",
            "objective_id": "low_end_priority_lstsq",
            "zero_offset_ppm": 0.0,
            "model_id": "m",
            "source_role": "diagnostic",
            "point_identity": "T20_600ppm",
            "target_ppm": 600.0,
            "prediction_ppm": 700.0,
            "error_ppm": 100.0,
            "relative_error_percent": 16.6667,
            "strategy_id": "s",
            "diagnostic_only": False,
            "selected_strategy": True,
        }
    )
    _write_csv(fit_csv, _fit_points())
    _write_csv(residuals_csv, residuals)
    _write_csv(best_csv, _best_rows())

    tables = build_co2_s13_target_state_common_bias_audit(
        fit_points_csv=fit_csv,
        selected_residuals_csv=residuals_csv,
        best_by_device_csv=best_csv,
    )

    assert all(
        row["point_identity"] != "T20_600ppm"
        for row in tables["selected_residuals_with_physical_state"]
    )
    assert all(
        row["point_identity"] != "T20_600ppm"
        for row in tables["point_common_bias"]
    )


def test_target_state_common_bias_writes_utf8_chinese_report(tmp_path):
    fit_csv = tmp_path / "fit.csv"
    residuals_csv = tmp_path / "residuals.csv"
    best_csv = tmp_path / "best.csv"
    _write_csv(fit_csv, _fit_points())
    _write_csv(residuals_csv, _residuals())
    _write_csv(best_csv, _best_rows())

    outputs = write_co2_s13_target_state_common_bias_audit(
        fit_points_csv=fit_csv,
        selected_residuals_csv=residuals_csv,
        best_by_device_csv=best_csv,
        output_dir=tmp_path / "audit",
    )

    meta = json.loads(outputs["metadata"].read_text(encoding="utf-8"))
    assert meta["boundary"]["writes_coefficients"] is False
    text = outputs["markdown"].read_text(encoding="utf-8-sig")
    assert "V1.5 CO2 目标状态与低端共同偏差根因审计" in text
    assert "零气 CO2 锚点" in text
    assert "乱码" not in text
