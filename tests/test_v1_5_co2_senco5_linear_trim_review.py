import csv

from gas_calibrator.validation.co2_senco5_linear_trim_review import (
    Co2Senco5LinearTrimConfig,
    build_co2_senco5_linear_trim_review,
)


def _write_csv(path, rows):
    keys = []
    for row in rows:
        for key in row:
            if key not in keys:
                keys.append(key)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=keys)
        writer.writeheader()
        writer.writerows(rows)


def test_senco5_linear_trim_review_fits_final_concentration_affine_model(tmp_path):
    rows = []
    for index, target in enumerate((100.0, 300.0, 700.0, 900.0, 1000.0), start=1):
        measured = target + 20.0
        rows.append(
            {
                "point_run_id": f"p{index}",
                "point_status": "ok",
                "source_nominal_ppm": target,
                "certificate_co2_ppm": target,
                "device_id": "022",
                "measured_co2_ppm": measured,
            }
        )
    path = tmp_path / "summary.csv"
    _write_csv(path, rows)

    tables = build_co2_senco5_linear_trim_review(
        verification_summary_csv=path,
        cfg=Co2Senco5LinearTrimConfig(target_device_ids=("022",), exclude_device_ids=(), acceptance_pct=1.0),
    )

    summary = tables["candidate_summary"][0]
    coeff = tables["candidate_coefficients"][0]
    residuals = tables["candidate_residuals"]

    assert summary["candidate_status"] == "review_ready"
    assert abs(float(summary["candidate_C0"]) + 20.0) < 1.0e-9
    assert abs(float(summary["candidate_C1"]) - 1.0) < 1.0e-9
    assert summary["fit_contract_stage"] == "integrated_firmware_output_candidate"
    assert summary["candidate_package_role"] == "senco5_final_output_layer_with_senco13"
    assert summary["not_ad_hoc_post_acceptance_repair"] is True
    assert coeff["senco_group"] == "SENCO5"
    assert coeff["auto_write_allowed"] is False
    assert coeff["fit_contract_stage"] == "integrated_firmware_output_candidate"
    assert coeff["not_ad_hoc_post_acceptance_repair"] is True
    assert coeff["command_preview"] == "SENCO5,YGAS,FFF,-20.0,1.0"
    assert all(row["status"] == "pass" for row in residuals)


def test_senco5_linear_trim_review_blocks_when_any_point_exceeds_acceptance(tmp_path):
    rows = []
    for index, (target, measured) in enumerate(
        ((100.0, 153.0), (300.0, 346.0), (700.0, 745.0), (900.0, 938.0), (1000.0, 1045.0)),
        start=1,
    ):
        rows.append(
            {
                "point_run_id": f"p{index}",
                "point_status": "ok",
                "source_nominal_ppm": target,
                "certificate_co2_ppm": target,
                "device_id": "022",
                "measured_co2_ppm": measured,
            }
        )
    path = tmp_path / "summary.csv"
    _write_csv(path, rows)

    tables = build_co2_senco5_linear_trim_review(
        verification_summary_csv=path,
        cfg=Co2Senco5LinearTrimConfig(target_device_ids=("022",), exclude_device_ids=(), acceptance_pct=1.0),
    )

    summary = tables["candidate_summary"][0]
    assert summary["candidate_status"] == "blocked"
    assert "max_abs_error_pct>1" in summary["blocked_reasons"]
    assert summary["one_decimal_C0_C1_max_abs_error_pct"] != ""
    assert any(row["status"] == "fail" for row in tables["candidate_residuals"])


def test_senco5_linear_trim_review_blocks_extreme_final_trim_scope(tmp_path):
    rows = [
        {
            "point_run_id": "p200",
            "point_status": "ok",
            "source_nominal_ppm": 200.0,
            "certificate_co2_ppm": 200.0,
            "device_id": "030",
            "measured_co2_ppm": 800.0,
        },
        {
            "point_run_id": "p800",
            "point_status": "ok",
            "source_nominal_ppm": 800.0,
            "certificate_co2_ppm": 800.0,
            "device_id": "030",
            "measured_co2_ppm": 1600.0,
        },
    ]
    path = tmp_path / "summary.csv"
    _write_csv(path, rows)

    tables = build_co2_senco5_linear_trim_review(
        verification_summary_csv=path,
        cfg=Co2Senco5LinearTrimConfig(
            target_device_ids=("030",),
            exclude_device_ids=(),
            acceptance_pct=1.0,
            min_points=2,
        ),
    )

    summary = tables["candidate_summary"][0]
    assert summary["candidate_status"] == "blocked"
    assert "senco5_c0_exceeds_final_trim_scope" in summary["blocked_reasons"]
    assert "senco5_c1_exceeds_final_trim_scope" in summary["blocked_reasons"]


def test_senco5_linear_trim_review_accepts_runner_point_error_schema(tmp_path):
    rows = []
    for point, target, measured in (
        ("open_flow_700ppm", 700.33, 730.245),
        ("open_flow_900ppm_a", 897.04, 923.308),
        ("open_flow_900ppm_b", 897.04, 922.779),
        ("open_flow_1000ppm", 998.62, 1028.945),
    ):
        rows.append(
            {
                "point": point,
                "device": "022",
                "target_ppm": target,
                "co2_ppm": measured,
            }
        )
    path = tmp_path / "runner_point_errors.csv"
    _write_csv(path, rows)

    tables = build_co2_senco5_linear_trim_review(
        verification_summary_csv=path,
        cfg=Co2Senco5LinearTrimConfig(target_device_ids=("022",), exclude_device_ids=(), acceptance_pct=1.0),
    )

    summary = tables["candidate_summary"][0]
    residuals = tables["candidate_residuals"]

    assert summary["candidate_status"] == "review_ready"
    assert summary["point_count"] == 4
    assert all(row["point_run_id"].startswith("open_flow_") for row in residuals)


def test_senco5_linear_trim_review_accepts_mean_schema_and_three_decimal_payload(tmp_path):
    rows = []
    for index, target in enumerate((100.0, 900.0), start=1):
        rows.append(
            {
                "point_run_id": f"p{index}",
                "point_status": "pass",
                "source_nominal_ppm": target,
                "target_ppm": target,
                "device_id": "100",
                "co2_mean_ppm": target + 1.234,
            }
        )
    path = tmp_path / "summary_by_device.csv"
    _write_csv(path, rows)

    tables = build_co2_senco5_linear_trim_review(
        verification_summary_csv=path,
        cfg=Co2Senco5LinearTrimConfig(
            target_device_ids=("100",),
            exclude_device_ids=(),
            acceptance_pct=1.0,
            min_points=2,
            command_c0_decimals=3,
            command_c1_decimals=3,
        ),
    )

    summary = tables["candidate_summary"][0]
    coeff = tables["candidate_coefficients"][0]

    assert summary["candidate_status"] == "review_ready"
    assert summary["payload_C0"] == -1.234
    assert summary["payload_C1"] == 1.0
    assert coeff["command_preview"] == "SENCO5,YGAS,FFF,-1.234,1.000"


def test_senco5_linear_trim_review_accepts_post_write_reverification_schema(tmp_path):
    rows = []
    for index, (target, measured) in enumerate(((99.94, 112.31), (299.73, 312.47), (599.54, 612.05)), start=1):
        rows.append(
            {
                "component": "co2",
                "device_id": "070",
                "analyzer_label": "GA01",
                "point_id": f"p{index}",
                "sample_role": "post_write_verification",
                "standard_value": target,
                "measured_value": measured,
                "unit": "ppm",
                "point_status": "ok",
            }
        )
    path = tmp_path / "post_write_reverification_by_device.csv"
    _write_csv(path, rows)

    tables = build_co2_senco5_linear_trim_review(
        verification_summary_csv=path,
        cfg=Co2Senco5LinearTrimConfig(
            target_device_ids=("070",),
            exclude_device_ids=(),
            acceptance_pct=1.5,
            min_points=3,
            command_c0_decimals=3,
            command_c1_decimals=3,
        ),
    )

    summary = tables["candidate_summary"][0]
    residuals = tables["candidate_residuals"]

    assert summary["point_count"] == 3
    assert summary["candidate_status"] == "review_ready"
    assert len(residuals) == 3
