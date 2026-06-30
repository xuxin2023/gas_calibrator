import ast
import csv
from pathlib import Path

from gas_calibrator.tools.export_v1_5_co2_s13_minimal_resampling_runlist import main as cli_main
from gas_calibrator.validation.co2_s13_minimal_resampling_runlist import (
    build_co2_s13_minimal_resampling_runlist,
    write_co2_s13_minimal_resampling_runlist,
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


def test_minimal_resampling_source_has_single_helper_definitions():
    source_path = (
        Path(__file__).resolve().parents[1]
        / "src"
        / "gas_calibrator"
        / "validation"
        / "co2_s13_minimal_resampling_runlist.py"
    )
    tree = ast.parse(source_path.read_text(encoding="utf-8"))
    names = [node.name for node in tree.body if isinstance(node, ast.FunctionDef)]

    assert names.count("_physical_gates") == 1
    assert names.count("render_markdown") == 1


def test_minimal_resampling_excludes_minus_20_points(tmp_path):
    summary = tmp_path / "point_summary.csv"
    _write_csv(
        summary,
        [
            {
                "point_identity": "T-20_400ppm",
                "temperature_c": "-20",
                "gas_ppm": "400",
                "device_count": "6",
                "base_over_acceptance_count": "4",
                "bridged_over_acceptance_count": "4",
                "base_max_abs_relative_error_percent": "3.66",
                "bridged_max_abs_relative_error_percent": "4.23",
                "recommended_action": "minimal_resample_this_point",
            },
            {
                "point_identity": "T30_200ppm",
                "temperature_c": "30",
                "gas_ppm": "200",
                "device_count": "6",
                "base_over_acceptance_count": "6",
                "bridged_over_acceptance_count": "6",
                "base_max_abs_relative_error_percent": "4.50",
                "bridged_max_abs_relative_error_percent": "5.69",
                "recommended_action": "minimal_resample_this_point",
            },
            {
                "point_identity": "T10_700ppm",
                "temperature_c": "10",
                "gas_ppm": "700",
                "device_count": "6",
                "base_over_acceptance_count": "3",
                "bridged_over_acceptance_count": "4",
                "base_max_abs_relative_error_percent": "1.32",
                "bridged_max_abs_relative_error_percent": "1.92",
                "recommended_action": "minimal_resample_this_point",
            },
        ],
    )

    tables = build_co2_s13_minimal_resampling_runlist(
        point_summary_csv=summary,
        max_points=5,
    )

    selected = [row["point_identity"] for row in tables["runlist"]]
    excluded = [row["point_identity"] for row in tables["excluded_points"]]
    assert "T-20_400ppm" not in selected
    assert "T-20_400ppm" in excluded
    assert selected[0] == "T30_200ppm"
    assert tables["run_summary"][0]["writes_coefficients"] is False
    assert tables["run_summary"][0]["controls_water_or_gas_routes"] is False


def test_minimal_resampling_writes_runlist_artifacts(tmp_path):
    summary = tmp_path / "point_summary.csv"
    _write_csv(
        summary,
        [
            {
                "point_identity": "T20_600ppm",
                "temperature_c": "20",
                "gas_ppm": "600",
                "device_count": "6",
                "base_over_acceptance_count": "6",
                "bridged_over_acceptance_count": "3",
                "base_max_abs_relative_error_percent": "3.88",
                "bridged_max_abs_relative_error_percent": "1.73",
                "recommended_action": "minimal_resample_this_point",
            }
        ],
    )

    outputs = write_co2_s13_minimal_resampling_runlist(
        point_summary_csv=summary,
        output_dir=tmp_path / "out",
        max_points=1,
    )

    report = (tmp_path / "out" / "co2_s13_minimal_resampling_runlist_zh.md").read_text(encoding="utf-8")
    assert "CO2 最小补采运行清单" in report
    assert "不打开 COM" in report
    assert (tmp_path / "out" / "co2_s13_minimal_resampling_runlist.csv").exists()
    assert (tmp_path / "out" / "co2_s13_minimal_resampling_canonical_co2_queue.csv").exists()
    assert (tmp_path / "out" / "co2_s13_minimal_resampling_queue_manifest.csv").exists()
    assert outputs["metadata"].endswith("co2_s13_minimal_resampling_meta.json")


def test_canonical_queue_inherits_template_group_and_excludes_minus_20(tmp_path):
    summary = tmp_path / "point_summary.csv"
    template = tmp_path / "co2_runner_queue.csv"
    _write_csv(
        summary,
        [
            {
                "point_identity": "T-20_1000ppm",
                "temperature_c": "-20",
                "gas_ppm": "1000",
                "device_count": "6",
                "base_over_acceptance_count": "4",
                "bridged_over_acceptance_count": "4",
                "base_max_abs_relative_error_percent": "3.0",
                "bridged_max_abs_relative_error_percent": "4.0",
                "recommended_action": "minimal_resample_this_point",
            },
            {
                "point_identity": "T30_300ppm",
                "temperature_c": "30",
                "gas_ppm": "300",
                "device_count": "6",
                "base_over_acceptance_count": "6",
                "bridged_over_acceptance_count": "6",
                "base_max_abs_relative_error_percent": "4.5",
                "bridged_max_abs_relative_error_percent": "5.6",
                "recommended_action": "minimal_resample_this_point",
            },
        ],
    )
    _write_csv(
        template,
        [
            {
                "point_id": "co2_T30_300ppm_ambient",
                "component": "co2",
                "temp_c": "30.0",
                "source_nominal_ppm": "300",
                "co2_group": "B",
                "sample_role": "verification",
                "fit_eligible": "False",
                "verification_eligible": "True",
                "purge_s": "360.0",
                "sample_count": "10",
                "analyzer_acquisition": "active_stream_1hz",
            },
        ],
    )

    outputs = write_co2_s13_minimal_resampling_runlist(
        point_summary_csv=summary,
        template_queue_csv=template,
        output_dir=tmp_path / "out",
        max_points=5,
    )

    with (tmp_path / "out" / "co2_s13_minimal_resampling_canonical_co2_queue.csv").open(
        "r", encoding="utf-8-sig", newline=""
    ) as handle:
        rows = list(csv.DictReader(handle))

    assert len(rows) == 1
    assert rows[0]["point_id"] == "co2_T30_300ppm_ambient"
    assert rows[0]["co2_group"] == "B"
    assert rows[0]["sample_role"] == "fit"
    assert rows[0]["fit_eligible"] == "True"
    assert rows[0]["verification_eligible"] == "False"
    assert rows[0]["not_real_acceptance_evidence"] == "True"
    assert "T-20" not in rows[0]["point_id"]
    assert outputs["canonical_co2_queue"].endswith("co2_s13_minimal_resampling_canonical_co2_queue.csv")


def test_minimal_resampling_cli_exports_artifacts(tmp_path):
    summary = tmp_path / "point_summary.csv"
    _write_csv(
        summary,
        [
            {
                "point_identity": "T20_500ppm",
                "temperature_c": "20",
                "gas_ppm": "500",
                "device_count": "6",
                "base_over_acceptance_count": "5",
                "bridged_over_acceptance_count": "4",
                "base_max_abs_relative_error_percent": "2.8",
                "bridged_max_abs_relative_error_percent": "2.1",
                "recommended_action": "minimal_resample_this_point",
            }
        ],
    )
    output = tmp_path / "cli_out"

    rc = cli_main(
        [
            "--point-summary-csv",
            str(summary),
            "--output-dir",
            str(output),
            "--max-points",
            "1",
        ]
    )

    assert rc == 0
    assert output.joinpath("co2_s13_minimal_resampling_runlist_zh.md").exists()
    assert output.joinpath("co2_s13_minimal_resampling_meta.json").exists()
