import csv
import json
from pathlib import Path

from openpyxl import Workbook

from gas_calibrator.tools.export_v1_5_open_flow_canonical_points import main as cli_main
from gas_calibrator.validation.open_flow_canonical_points import (
    build_open_flow_canonical_point_tables,
    write_open_flow_canonical_point_plan,
)


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def _write_source_points(path: Path) -> None:
    h2o_text = (
        "20\u2103\uff08\u6e7f\u5ea6\u53d1\u751f\u5668\uff09 "
        "50%\uff08\u6e7f\u5ea6\u53d1\u751f\u5668\uff09 "
        "9.26\u2103\uff08\u9732\u70b9\u6e29\u5ea6\uff09 11.6416mmol/mol\uff08\u6c14\u4f53\u5206\u6790\u4eea\uff09"
    )
    wb = Workbook()
    ws = wb.active
    ws.title = "points"
    ws.append(["Normalized calibration points", None, None, None, None])
    ws.append(["Temp_C", "CO2_ppm", "H2O_text", "Pressure_hPa", "CO2_group"])
    ws.append([-20, 0, None, 1100, None])
    ws.append([-20, 400, None, 800, None])
    ws.append([20, 0, h2o_text, 1100, None])
    ws.append([20, 100, None, 1000, "B"])
    ws.append([20, 300, None, 900, "B"])
    ws.append([20, 500, None, 800, "B"])
    ws.append([20, 700, None, 700, "B"])
    ws.append([20, 900, None, 600, "B"])
    ev = wb.create_sheet("execution_view")
    ev.append(["Temp_C", "Order", "H2O_targets", "CO2_sources_ppm", "Pressures_hPa", "Notes"])
    ev.append([-20, "CO2 only", None, "0, 400, 1000", "1100, 800, 550", "sub-zero fixed CO2 only"])
    ev.append([20, "H2O then CO2", "20C / 50%RH", "0, 100, 200, 300, 400, 500, 600, 700, 800, 900, 1000", "1100, 1000, 900, 800, 700, 600, 550", "open flow canonical source"])
    path.parent.mkdir(parents=True, exist_ok=True)
    wb.save(path)
    wb.close()


def test_open_flow_canonical_points_strip_legacy_pressure_targets(tmp_path):
    source = tmp_path / "points.xlsx"
    _write_source_points(source)

    tables = build_open_flow_canonical_point_tables(source_points_xlsx=source)

    summary = tables["v1_5_open_flow_canonical_summary"][0]
    assert summary["plan_status"] == "ready_for_no_write_open_flow_execution"
    assert summary["opens_com_ports"] is False
    assert summary["controls_water_or_gas_routes"] is False
    assert summary["writes_coefficients"] is False

    co2_rows = tables["co2_open_flow_multitemp_ambient"]
    assert any(row["source_nominal_ppm"] == 0 and row["sample_role"] == "fit" for row in co2_rows)
    assert any(row["source_nominal_ppm"] == 200 and row["sample_role"] == "fit" for row in co2_rows)
    assert {row["sample_role"] for row in co2_rows} == {"fit"}
    assert all(row["fit_eligible"] is True for row in co2_rows)
    assert all(row["verification_eligible"] is False for row in co2_rows)
    assert all(row["pressure_mode"] == "ambient_open" for row in co2_rows)
    assert all(row["target_pressure_hpa"] == "" for row in co2_rows)
    assert all(row["pressure_channel_precheck_required"] is True for row in co2_rows)

    h2o_rows = tables["h2o_open_flow_multitemp_ambient"]
    assert len(h2o_rows) == 1
    assert h2o_rows[0]["pressure_mode"] == "ambient_open"
    assert h2o_rows[0]["target_pressure_hpa"] == ""
    assert h2o_rows[0]["reference_h2o_mmol"] == 11.6416

    excluded = tables["legacy_pressure_targets_excluded"]
    assert excluded
    assert {row["formal_v1_5_action"] for row in excluded} == {"excluded_from_main_calibration"}


def test_open_flow_canonical_point_plan_writer_and_cli(tmp_path):
    source = tmp_path / "points.xlsx"
    _write_source_points(source)
    output = tmp_path / "out"

    outputs = write_open_flow_canonical_point_plan(
        source_points_xlsx=source,
        output_dir=output,
    )

    assert outputs["workbook"].exists()
    co2_rows = _read_csv(outputs["co2_open_flow_multitemp_ambient_csv"])
    assert co2_rows[0]["pressure_mode"] == "ambient_open"
    manifest = json.loads(outputs["manifest"].read_text(encoding="utf-8"))
    assert manifest["opens_com_ports"] is False
    assert manifest["pressure_model"] == "pressure_channel_verified_before_sampling_then_ambient_open_evidence_only"

    cli_output = tmp_path / "cli"
    rc = cli_main(
        [
            "--source-points-xlsx",
            str(source),
            "--output-dir",
            str(cli_output),
            "--co2-fit-ppm",
            "0,100,300,500,700,900",
            "--co2-verification-ppm",
            "200,400,600,800,1000",
        ]
    )
    assert rc == 0
    assert (cli_output / "v1_5_open_flow_canonical_point_plan_manifest.json").exists()
    cli_rows = _read_csv(cli_output / "co2_open_flow_multitemp_ambient.csv")
    assert {row["sample_role"] for row in cli_rows} == {"fit"}
    assert all(row["verification_eligible"] == "False" for row in cli_rows)
