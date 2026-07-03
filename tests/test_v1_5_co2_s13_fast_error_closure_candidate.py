import csv

from gas_calibrator.validation.co2_s13_fast_error_closure_candidate import (
    build_co2_s13_fast_error_closure_candidate,
    write_co2_s13_fast_error_closure_candidate,
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


def test_fast_error_closure_computes_s5_from_selected_strategy(tmp_path):
    best = tmp_path / "best.csv"
    residuals = tmp_path / "residuals.csv"
    strategy_id = "profile|core|objective|zero=0|lowx=1"
    _write_csv(
        best,
        [
            {
                "device_id": "7",
                "strategy_id": strategy_id,
                "strategy_profile_id": "profile",
                "objective_id": "objective",
                "zero_offset_ppm": "0",
                "fit_point_count": "3",
                "max_abs_relative_error_percent": "10",
                "low_end_max_abs_relative_error_percent": "10",
                "s1_payload_scientific": "1,2,3,4",
                "s3_payload_scientific": "5,6,7,0,0,0",
            }
        ],
    )
    _write_csv(
        residuals,
        [
            {
                "device_id": "007",
                "strategy_id": strategy_id,
                "point_identity": "T20_100ppm",
                "target_ppm": "100",
                "prediction_ppm": "90",
            },
            {
                "device_id": "007",
                "strategy_id": strategy_id,
                "point_identity": "T20_900ppm",
                "target_ppm": "900",
                "prediction_ppm": "810",
            },
            {
                "device_id": "007",
                "strategy_id": strategy_id,
                "point_identity": "T20_0ppm",
                "target_ppm": "0",
                "prediction_ppm": "1",
            },
        ],
    )

    tables = build_co2_s13_fast_error_closure_candidate(
        best_by_device_csv=best,
        residuals_csv=residuals,
        s5_c1_min=1.0,
        s5_c1_max=1.2,
        s5_c0_decimals=3,
        s5_c1_decimals=3,
    )

    row = tables["summary"][0]
    assert row["device_id"] == "007"
    assert row["s5_C1"] == 1.111
    assert row["s5_max_abs_relative_error_percent"] < 0.02
    assert row["recommended_action"] == "candidate_for_controlled_write_review_s1s3_plus_s5"
    assert row["opens_com_ports"] is False
    assert row["writes_coefficients"] is False


def test_fast_error_closure_writes_report_artifacts(tmp_path):
    best = tmp_path / "best.csv"
    residuals = tmp_path / "residuals.csv"
    strategy_id = "profile|core|objective|zero=0|lowx=1"
    _write_csv(
        best,
        [
            {
                "device_id": "8",
                "strategy_id": strategy_id,
                "strategy_profile_id": "profile",
                "objective_id": "objective",
                "zero_offset_ppm": "0",
                "fit_point_count": "2",
                "max_abs_relative_error_percent": "5",
                "low_end_max_abs_relative_error_percent": "5",
            }
        ],
    )
    _write_csv(
        residuals,
        [
            {"device_id": "008", "strategy_id": strategy_id, "point_identity": "P1", "target_ppm": "100", "prediction_ppm": "100"},
            {"device_id": "008", "strategy_id": strategy_id, "point_identity": "P2", "target_ppm": "900", "prediction_ppm": "900"},
        ],
    )
    outputs = write_co2_s13_fast_error_closure_candidate(
        best_by_device_csv=best,
        residuals_csv=residuals,
        output_dir=tmp_path / "out",
    )

    assert "CO2 S1/S3 + S5 快速误差闭合评审" in (tmp_path / "out" / "co2_s13_fast_error_closure_review_zh.md").read_text(encoding="utf-8")
    assert (tmp_path / "out" / "co2_s13_fast_error_closure_summary.csv").exists()
    assert outputs["metadata"].endswith("co2_s13_fast_error_closure_meta.json")
