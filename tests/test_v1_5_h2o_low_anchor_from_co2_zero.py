import csv
import json
from pathlib import Path

from gas_calibrator.tools.export_v1_5_h2o_low_anchor_from_co2_zero import main as cli_main
from gas_calibrator.validation.h2o_low_anchor_from_co2_zero import (
    H2OLowAnchorFromCO2ZeroConfig,
    build_h2o_low_anchor_from_co2_zero_tables,
    write_h2o_low_anchor_from_co2_zero_review,
)


def _write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields: list[str] = []
    for row in rows:
        for key in row:
            if key not in fields:
                fields.append(key)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def _make_co2_route(tmp_path: Path) -> Path:
    root = tmp_path / "co2_route"
    for point_name, temp, target, ratio, residual, dew in [
        ("p001_Tm20_0ppm_fit", -20.0, 0, 0.7811, 0.18, -41.0),
        ("p002_T10_400ppm_fit", 10.0, 400, 0.7411, 8.0, 4.0),
        ("p003_T40_0ppm_fit", 40.0, 0, 0.7622, 0.42, -31.5),
    ]:
        point = root / point_name
        _write_csv(
            point / "co2_summary.csv",
            [
                {
                    "Analyzer": "GA01",
                    "ppm_H2O_Dew": residual,
                    "ppm_H2O": residual,
                    "R_H2O": ratio,
                    "R_H2O_dev": 0.0001,
                    "T1": temp + 3.1,
                    "Temp": temp,
                    "Dew": dew,
                    "P": 1010.0,
                    "BAR": 101.0,
                    "ValidFrames": 10,
                    "TotalFrames": 10,
                    "FrameStatus": "all_usable",
                    "PointIntegrity": "complete",
                    "TargetCO2": target,
                }
            ],
        )
        _write_csv(
            point / "samples_machine_readable.csv",
            [
                {
                    "sample_alignment_ok": "true",
                    "sampling_time_alignment_max_age_ms": 100.0,
                    "ga01_analyzer_device_id": "001",
                }
            ],
        )
    return root


def test_h2o_low_anchor_from_co2_zero_uses_only_zero_gas_points(tmp_path: Path) -> None:
    co2_route = _make_co2_route(tmp_path)

    tables = build_h2o_low_anchor_from_co2_zero_tables(
        co2_zero_run_dirs=(co2_route,),
        cfg=H2OLowAnchorFromCO2ZeroConfig(
            max_residual_h2o_mmol=0.5,
            max_dewpoint_c=-30.0,
            min_distinct_temperatures=2,
            preferred_distinct_temperatures=2,
        ),
    )

    manifest = tables["manifest"]
    assert manifest["no_write"] is True
    assert manifest["opens_com_ports"] is False
    assert manifest["controls_water_or_gas_routes"] is False
    assert "Residual water is never forced to zero" in manifest["source_contract"]

    anchors = tables["h2o_low_anchor_inputs"]
    assert {row["co2_zero_point_run_id"] for row in anchors} == {
        "p001_Tm20_0ppm_fit",
        "p003_T40_0ppm_fit",
    }
    assert {row["anchor_status"] for row in anchors} == {"fit_ready_low_anchor"}
    assert all(row["not_forced_to_zero"] is True for row in anchors)
    assert all(float(row["residual_h2o_mmol_from_dewpoint_pressure"]) > 0 for row in anchors)
    assert all(row["r0_equation_basis"].startswith("ln(R_H2O)=ln(R0_H2O") for row in anchors)

    summary = tables["h2o_low_anchor_device_summary"]
    assert summary == [
        {
            "component": "h2o",
            "analyzer_device_id": "001",
            "anchor_count": 2,
            "fit_ready_anchor_count": 2,
            "fit_ready_distinct_temperature_count": 2,
            "fit_ready_temperatures_c": "-20;40",
            "recommendation": "ready_for_new_algorithm_r0_h2o_fit",
        }
    ]


def test_h2o_low_anchor_from_co2_zero_writer_and_cli(tmp_path: Path) -> None:
    co2_route = _make_co2_route(tmp_path)
    output = tmp_path / "anchors"

    paths = write_h2o_low_anchor_from_co2_zero_review(
        co2_zero_run_dirs=(co2_route,),
        output_dir=output,
        cfg=H2OLowAnchorFromCO2ZeroConfig(
            min_distinct_temperatures=2,
            preferred_distinct_temperatures=2,
        ),
    )

    assert Path(paths["h2o_low_anchor_inputs"]).exists()
    manifest = json.loads(
        output.joinpath("h2o_low_anchor_from_co2_zero_manifest.json").read_text(
            encoding="utf-8-sig"
        )
    )
    assert manifest["writes_coefficients"] is False

    cli_output = tmp_path / "cli_anchors"
    rc = cli_main(
        [
            "--co2-zero-run-dir",
            str(co2_route),
            "--output-dir",
            str(cli_output),
            "--min-distinct-temperatures",
            "2",
            "--preferred-distinct-temperatures",
            "2",
        ]
    )
    assert rc == 0
    assert cli_output.joinpath("h2o_low_anchor_inputs.csv").exists()
