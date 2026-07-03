import json
import csv
from pathlib import Path

from gas_calibrator.tools.export_v1_5_h2o_dry_anchor_bridge_review import main as cli_main
from gas_calibrator.validation.h2o_dry_anchor_bridge_review import (
    H2ODryAnchorBridgeConfig,
    build_h2o_dry_anchor_bridge_tables,
    write_h2o_dry_anchor_bridge_review,
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


def _make_h2o_run(tmp_path: Path) -> Path:
    root = tmp_path / "h2o_run"
    queue_dir = root / "h2o_mt_no_write_r1"
    manifest_rows = []
    ratios = [0.21, 0.36, 0.29, 0.48, 0.62, 0.41, 0.72, 0.55]
    temps = [0.0, 10.0, 20.0, 30.0, 40.0, 5.0, 25.0, 35.0]
    for idx, (ratio, temp) in enumerate(zip(ratios, temps), start=1):
        point_name = f"p{idx:03d}_T{int(temp)}_HG{idx}_h2o"
        point = root / point_name
        target = 5.0 + 20.0 * ratio + 0.05 * temp
        manifest_rows.append(
            {
                "point_run_id": point_name,
                "point_id": f"h2o_point_{idx}",
                "temp_c": temp,
                "hgen_temp_c": temp,
                "hgen_rh_pct": 50.0,
                "reference_h2o_mmol": target - 0.25,
                "reference_dewpoint_c": temp - 10.0,
                "sample_role": "fit",
            }
        )
        rows = [
            {
                "Analyzer": "GA022",
                "ppm_H2O_Dew": target,
                "ppm_H2O": target + 0.05,
                "R_H2O": ratio,
                "R_H2O_dev": 0.0001,
                "T1": temp,
                "Temp": temp - 0.1,
                "Dew": temp - 10.0,
                "P": 1010.0,
                "BAR": 101.0,
                "ValidFrames": 10,
                "TotalFrames": 10,
                "FrameStatus": "all_usable",
                "PointIntegrity": "complete",
            },
        ]
        _write_csv(point / "point_h2o_summary.csv", rows)
        _write_csv(
            point / "samples_machine_readable.csv",
            [
                {
                    "sample_alignment_ok": "true",
                    "sampling_time_alignment_max_age_ms": 100.0,
                    "thermometer_cache_age_ms": 100.0,
                    "hgen_cache_age_ms": 100.0,
                    "dewpoint_sample_age_ms": 100.0,
                }
            ],
        )
    _write_csv(queue_dir / "queue_manifest.csv", manifest_rows)
    return root


def _make_co2_dry_anchor_run(tmp_path: Path) -> Path:
    root = tmp_path / "co2_dry_anchor_run"
    for idx, temp in enumerate((0.0, 20.0), start=1):
        point_name = f"p{idx:03d}_T{int(temp)}_0ppm_fit"
        point = root / point_name
        _write_csv(
            point / "co2_summary.csv",
            [
                {
                    "Analyzer": "GA01",
                    "ppm_H2O_Dew": 0.35 + idx * 0.1,
                    "ppm_H2O": 2.1 + idx * 0.2,
                    "R_H2O": 0.18 + idx * 0.01,
                    "R_H2O_dev": 0.0001,
                    "T1": temp + 0.2,
                    "Temp": temp,
                    "Dew": -32.0 + idx,
                    "P": 1012.0,
                    "BAR": 101.2,
                    "ValidFrames": 10,
                    "TotalFrames": 10,
                    "FrameStatus": "all_usable",
                    "PointIntegrity": "complete",
                }
            ],
        )
        _write_csv(
            point / "samples_machine_readable.csv",
            [
                {
                    "sample_alignment_ok": "true",
                    "sampling_time_alignment_max_age_ms": 100.0,
                    "thermometer_cache_age_ms": 100.0,
                    "hgen_cache_age_ms": 100.0,
                    "dewpoint_sample_age_ms": 100.0,
                    "ga01_analyzer_device_id": "022",
                }
            ],
        )
    return root


def test_h2o_dry_anchor_bridge_evaluates_dry_points_without_writing(tmp_path):
    wet_run = _make_h2o_run(tmp_path)
    dry_run = _make_co2_dry_anchor_run(tmp_path)

    tables = build_h2o_dry_anchor_bridge_tables(
        wet_run_dir=wet_run,
        dry_anchor_run_dirs=(dry_run,),
        cfg=H2ODryAnchorBridgeConfig(
            min_points=8,
            fit_temperature_source="digital_thermometer",
            dry_anchor_max_temp_c=0.0,
            require_component_snapshot_for_layer_review=False,
        ),
    )

    manifest = tables["manifest"]
    assert manifest["no_write"] is True
    assert manifest["opens_com_ports"] is False
    assert manifest["controls_water_or_gas_routes"] is False
    assert manifest["fit_temperature_source"] == "digital_thermometer"

    summary = {row["analyzer_device_id"]: row for row in tables["device_summary"]}
    assert summary["022"]["dry_anchor_count"] == 1
    assert summary["022"]["wet_fit_complete_wet_point_count"] == 8
    assert summary["022"]["recommendation"] in {
        "dry_anchors_can_enter_low_end_fit_review",
        "compatible_dry_anchor_subset_can_enter_low_end_review",
        "wet_points_main_fit_keep_dry_anchors_as_qc",
        "collect_new_formal_dry_h2o_anchor_evidence",
    }

    predictions = tables["dry_anchor_predictions"]
    assert len(predictions) == 1
    assert predictions[0]["physical_role"] == "dry_anchor_bridge_qc"
    assert predictions[0]["bridge_status"] in {
        "bridge_fit_compatible",
        "bridge_qc_only_relative_limit",
        "bridge_qc_only_model_mismatch",
    }

    strategies = {row["strategy_id"] for row in tables["strategy_comparison"]}
    assert {"wet_only_relative", "dry_all_absolute", "dry_le_0_relative"} <= strategies


def test_h2o_dry_anchor_bridge_writer_and_cli_create_artifacts(tmp_path):
    wet_run = _make_h2o_run(tmp_path)
    dry_run = _make_co2_dry_anchor_run(tmp_path)

    output = tmp_path / "bridge"
    outputs = write_h2o_dry_anchor_bridge_review(
        wet_run_dir=wet_run,
        dry_anchor_run_dirs=(dry_run,),
        output_dir=output,
        cfg=H2ODryAnchorBridgeConfig(
            min_points=8,
            fit_temperature_source="digital_thermometer",
            dry_anchor_max_temp_c=0.0,
            require_component_snapshot_for_layer_review=False,
        ),
    )

    assert output.joinpath("h2o_dry_anchor_bridge_review.md").exists()
    manifest = json.loads(output.joinpath("h2o_dry_anchor_bridge_manifest.json").read_text(encoding="utf-8-sig"))
    assert manifest["writes_coefficients"] is False
    assert outputs["strategy_comparison"].endswith("h2o_dry_anchor_bridge_strategy_comparison.csv")

    cli_output = tmp_path / "cli_bridge"
    rc = cli_main(
        [
            "--wet-run-dir",
            str(wet_run),
            "--dry-anchor-run-dir",
            str(dry_run),
            "--output-dir",
            str(cli_output),
            "--fit-temperature-source",
            "digital_thermometer",
            "--dry-anchor-max-temp-c",
            "0",
            "--fit-objective",
            "relative_mmol_floor",
            "--no-require-component-snapshot-for-layer-review",
        ]
    )
    assert rc == 0
    cli_manifest = json.loads(
        cli_output.joinpath("h2o_dry_anchor_bridge_manifest.json").read_text(encoding="utf-8-sig")
    )
    assert cli_manifest["fit_objective"] == "relative_mmol_floor"
    assert cli_output.joinpath("h2o_dry_anchor_bridge_predictions.csv").exists()
