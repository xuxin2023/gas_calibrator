import csv

from gas_calibrator.tools.export_v1_5_co2_s13_minimal_bridge_plan import main as cli_main
from gas_calibrator.validation.co2_s13_minimal_bridge_plan import (
    build_co2_s13_minimal_bridge_plan,
    write_co2_s13_minimal_bridge_plan,
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


def test_minimal_bridge_plan_prioritizes_common_state_point(tmp_path):
    summary = tmp_path / "summary.csv"
    residuals = tmp_path / "residuals.csv"
    _write_csv(
        summary,
        [
            {
                "device_id": "1",
                "s1s3_max_abs_relative_error_percent": "3.0",
                "s5_max_abs_relative_error_percent": "2.5",
                "s5_worst_relative_point_identity": "T20_100ppm",
                "s5_command_preview": "SENCO5,YGAS,FFF,0,1",
            },
            {
                "device_id": "2",
                "s1s3_max_abs_relative_error_percent": "3.5",
                "s5_max_abs_relative_error_percent": "2.0",
                "s5_worst_relative_point_identity": "T20_100ppm",
                "s5_command_preview": "SENCO5,YGAS,FFF,0,1",
            },
        ],
    )
    _write_csv(
        residuals,
        [
            {
                "device_id": "001",
                "point_identity": "T20_100ppm",
                "target_ppm": "100",
                "s5_relative_error_percent": "-2.0",
                "ratio_grade": "A",
                "dryness_grade": "deep_dry",
                "physical_qc_label": "physical_qc_good",
            },
            {
                "device_id": "002",
                "point_identity": "T20_100ppm",
                "target_ppm": "100",
                "s5_relative_error_percent": "-2.5",
                "ratio_grade": "A",
                "dryness_grade": "deep_dry",
                "physical_qc_label": "physical_qc_good",
            },
            {
                "device_id": "001",
                "point_identity": "T20_900ppm",
                "target_ppm": "900",
                "s5_relative_error_percent": "0.2",
            },
        ],
    )

    tables = build_co2_s13_minimal_bridge_plan(
        closure_summary_csv=summary,
        corrected_residuals_csv=residuals,
        common_device_count=2,
    )

    point = tables["point_plan"][0]
    assert point["point_identity"] == "T20_100ppm"
    assert point["recommended_action"] == "bridge_or_resample_common_source_state"
    assert point["signed_coherence"] == "same_direction_bias_negative"
    assert point["opens_com_ports"] is False
    assert tables["device_plan"][0]["writes_coefficients"] is False


def test_minimal_bridge_plan_writes_chinese_report(tmp_path):
    summary = tmp_path / "summary.csv"
    residuals = tmp_path / "residuals.csv"
    _write_csv(
        summary,
        [
            {
                "device_id": "7",
                "s1s3_max_abs_relative_error_percent": "0.5",
                "s5_max_abs_relative_error_percent": "0.4",
                "s5_worst_relative_point_identity": "T20_100ppm",
                "s5_command_preview": "SENCO5,YGAS,FFF,0,1",
            }
        ],
    )
    _write_csv(
        residuals,
        [
            {
                "device_id": "007",
                "point_identity": "T20_100ppm",
                "target_ppm": "100",
                "s5_relative_error_percent": "0.4",
            }
        ],
    )
    outputs = write_co2_s13_minimal_bridge_plan(
        closure_summary_csv=summary,
        corrected_residuals_csv=residuals,
        output_dir=tmp_path / "out",
    )

    report = (tmp_path / "out" / "co2_s13_minimal_bridge_plan_zh.md").read_text(encoding="utf-8")
    assert "CO2 最小补采" in report
    assert "不打开 COM" in report
    assert (tmp_path / "out" / "co2_s13_minimal_bridge_point_plan.csv").exists()
    assert outputs["metadata"].endswith("co2_s13_minimal_bridge_meta.json")


def test_minimal_bridge_plan_cli_exports_artifacts(tmp_path):
    summary = tmp_path / "summary.csv"
    residuals = tmp_path / "residuals.csv"
    _write_csv(
        summary,
        [
            {
                "device_id": "8",
                "s1s3_max_abs_relative_error_percent": "2.1",
                "s5_max_abs_relative_error_percent": "1.4",
                "s5_worst_relative_point_identity": "T30_500ppm",
                "s5_command_preview": "SENCO5,YGAS,FFF,0,1",
            }
        ],
    )
    _write_csv(
        residuals,
        [
            {
                "device_id": "008",
                "point_identity": "T30_500ppm",
                "target_ppm": "500",
                "s5_relative_error_percent": "1.4",
            }
        ],
    )
    output = tmp_path / "cli_out"

    rc = cli_main(
        [
            "--closure-summary-csv",
            str(summary),
            "--corrected-residuals-csv",
            str(residuals),
            "--output-dir",
            str(output),
        ]
    )

    assert rc == 0
    assert output.joinpath("co2_s13_minimal_bridge_plan_zh.md").exists()
    assert output.joinpath("co2_s13_minimal_bridge_meta.json").exists()
