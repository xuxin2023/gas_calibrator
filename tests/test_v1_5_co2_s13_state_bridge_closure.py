import csv

from gas_calibrator.validation.co2_s13_state_bridge_closure import (
    build_co2_s13_state_bridge_closure,
    write_co2_s13_state_bridge_closure,
)


def _write_csv(path, rows):
    headers = []
    for row in rows:
        for key in row:
            if key not in headers:
                headers.append(key)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=headers)
        writer.writeheader()
        writer.writerows(rows)


def test_state_bridge_closure_removes_common_point_bias_loo(tmp_path):
    residuals = tmp_path / "residuals.csv"
    _write_csv(
        residuals,
        [
            {
                "device_id": "001",
                "point_identity": "T20_100ppm",
                "target_ppm": "100",
                "s5_error_ppm": "-4.0",
                "s5_relative_error_percent": "-4.0",
                "ratio_grade": "A",
                "dryness_grade": "deep_dry",
            },
            {
                "device_id": "002",
                "point_identity": "T20_100ppm",
                "target_ppm": "100",
                "s5_error_ppm": "-4.2",
                "s5_relative_error_percent": "-4.2",
                "ratio_grade": "A",
                "dryness_grade": "deep_dry",
            },
            {
                "device_id": "003",
                "point_identity": "T20_100ppm",
                "target_ppm": "100",
                "s5_error_ppm": "-3.8",
                "s5_relative_error_percent": "-3.8",
                "ratio_grade": "A",
                "dryness_grade": "deep_dry",
            },
        ],
    )

    tables = build_co2_s13_state_bridge_closure(
        corrected_residuals_csv=residuals,
        min_bridge_support=3,
    )

    point = tables["point_summary"][0]
    assert point["point_identity"] == "T20_100ppm"
    assert point["base_max_abs_relative_error_percent"] > 4.0
    assert point["bridged_max_abs_relative_error_percent"] < 0.5
    assert point["bridge_closes_point_to_acceptance"] is True
    assert point["recommended_action"] == "accept_existing_point_with_bridge_evidence"
    assert tables["run_summary"][0]["writes_coefficients"] is False


def test_state_bridge_closure_writes_artifacts(tmp_path):
    residuals = tmp_path / "residuals.csv"
    _write_csv(
        residuals,
        [
            {
                "device_id": "007",
                "point_identity": "T20_900ppm",
                "target_ppm": "900",
                "s5_error_ppm": "1.0",
                "s5_relative_error_percent": "0.111",
            },
            {
                "device_id": "008",
                "point_identity": "T20_900ppm",
                "target_ppm": "900",
                "s5_error_ppm": "1.2",
                "s5_relative_error_percent": "0.133",
            },
        ],
    )
    outputs = write_co2_s13_state_bridge_closure(
        corrected_residuals_csv=residuals,
        output_dir=tmp_path / "out",
        min_bridge_support=2,
    )

    report = (tmp_path / "out" / "co2_s13_state_bridge_review_zh.md").read_text(encoding="utf-8")
    assert "CO2 状态桥接" in report
    assert "不打开 COM" in report
    assert (tmp_path / "out" / "co2_s13_state_bridge_point_summary.csv").exists()
    assert outputs["metadata"].endswith("co2_s13_state_bridge_meta.json")
