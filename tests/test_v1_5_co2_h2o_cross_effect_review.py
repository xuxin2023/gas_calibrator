import csv
from pathlib import Path

from gas_calibrator.validation.co2_h2o_cross_effect_review import (
    build_co2_h2o_cross_effect_review,
    normalize_cross_effect_rows,
    write_co2_h2o_cross_effect_review,
)


def _write_csv(path: Path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    headers = []
    for row in rows:
        for key in row:
            if key not in headers:
                headers.append(key)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=headers)
        writer.writeheader()
        writer.writerows(rows)


def test_cross_review_normalizes_firmware_dry_correction_layers():
    rows = normalize_cross_effect_rows(
        [
            {
                "device_id": "051",
                "target_ppm": "897.04",
                "raw_senco13_co2_ppm": "893.319",
                "h2o_mmol_mol": "72.0",
                "R_CO2": "1.31",
                "temperature_c": "20.0",
                "pressure_hpa": "1013.25",
            }
        ],
        source_label="wet_bias_example",
    )

    row = rows[0]
    assert row["device_id"] == "051"
    assert row["co2_raw_estimated_from_final"] is False
    assert round(row["co2_dry0_ppm"], 3) == 962.628
    assert row["row_status"] == "eligible_cross_review"
    assert row["target_role"] == "co2_nonzero_cross_candidate"


def test_cross_review_flags_large_residual_not_explainable_by_low_h2o():
    rows = normalize_cross_effect_rows(
        [
            {
                "ActualDeviceId": "030",
                "Y_true": "900",
                "Y_pred_simple": "940",
                "ppm_H2O_Dew": "0.4",
                "R_CO2": "1.25",
            }
        ],
        source_label="low_h2o_large_residual",
    )

    row = rows[0]
    assert row["co2_raw_estimated_from_final"] is True
    assert row["row_status"] == "residual_too_large_for_dry_dilution"
    assert abs(row["h2o_dry_correction_pct"]) < 0.1
    assert row["residual_after_dry_pct"] < -4.0


def test_cross_review_fits_low_order_h2o_residual_model(tmp_path):
    csv_path = tmp_path / "cross.csv"
    rows = []
    for idx, h2o in enumerate([0.1, 0.2, 0.4, 0.6, 0.8, 1.0, 1.2, 1.4]):
        target = 500.0 + idx * 25.0
        residual = 1.5 + 2.0 * h2o
        raw = (target - residual) * (1.0 - h2o / 1000.0)
        rows.append(
            {
                "device_id": "022",
                "target_ppm": f"{target:.6f}",
                "raw_senco13_co2_ppm": f"{raw:.9f}",
                "h2o_mmol_mol": f"{h2o:.6f}",
                "temperature_c": "20.0",
                "pressure_hpa": "1013.25",
            }
        )
    _write_csv(csv_path, rows)

    payload = build_co2_h2o_cross_effect_review(csv_paths=[csv_path], source_labels=["synthetic"])
    summary = payload["summary"][0]
    source_summary = payload["source_summary"][0]
    models = {row["model_id"]: row for row in payload["model_candidates"]}

    assert summary["nonzero_cross_candidate_count"] == 8
    assert source_summary["source_label"] == "synthetic"
    assert source_summary["nonzero_cross_candidate_count"] == 8
    assert models["h2o_linear"]["status"] == "fit_ok"
    assert models["h2o_linear"]["after_rms_ppm"] < 1e-6
    assert summary["best_model_id"] == "h2o_linear"


def test_cross_review_auto_joins_split_co2_and_h2o_artifacts(tmp_path):
    co2_csv = tmp_path / "co2_points.csv"
    h2o_csv = tmp_path / "h2o_state.csv"
    _write_csv(
        co2_csv,
        [
            {
                "ActualDeviceId": "79",
                "点位行号": "12",
                "Y_true": "500",
                "Y_pred_simple": "510",
                "R": "1.25",
            }
        ],
    )
    _write_csv(
        h2o_csv,
        [
            {
                "ActualDeviceId": "079",
                "PointRow": "12",
                "ppm_H2O_Dew": "1.5",
                "Temp": "20",
                "BAR": "1013.25",
            }
        ],
    )

    payload = build_co2_h2o_cross_effect_review(
        csv_paths=[co2_csv, h2o_csv],
        source_labels=["co2", "h2o"],
    )

    summary = payload["summary"][0]
    joined = [
        row
        for row in payload["normalized_rows"]
        if str(row.get("source_label", "")).startswith("auto_join:")
    ]
    assert summary["auto_joined_row_count"] == 1
    assert joined
    assert joined[0]["device_id"] == "079"
    assert joined[0]["h2o_mmol_mol"] == 1.5
    assert joined[0]["co2_target_ppm"] == 500.0


def test_cross_review_cross_source_validation_uses_holdout_sources(tmp_path):
    a_csv = tmp_path / "source_a.csv"
    b_csv = tmp_path / "source_b.csv"

    def make_rows(source_offset: int, h2os):
        rows = []
        for idx, h2o in enumerate(h2os):
            target = 300.0 + (idx + source_offset) * 50.0
            residual = 1.0 + 2.0 * h2o
            raw = (target - residual) * (1.0 - h2o / 1000.0)
            rows.append(
                {
                    "device_id": "022",
                    "target_ppm": f"{target:.6f}",
                    "raw_senco13_co2_ppm": f"{raw:.9f}",
                    "h2o_mmol_mol": f"{h2o:.6f}",
                    "temperature_c": "20.0",
                    "pressure_hpa": "1013.25",
                }
            )
        return rows

    _write_csv(a_csv, make_rows(0, [0.2, 0.4, 0.6, 0.8, 1.0]))
    _write_csv(b_csv, make_rows(5, [0.3, 0.5, 0.7, 0.9, 1.1]))

    payload = build_co2_h2o_cross_effect_review(csv_paths=[a_csv, b_csv], source_labels=["a", "b"])
    validations = [
        row
        for row in payload["cross_source_validation"]
        if row["model_id"] == "h2o_linear" and row["status"] == "validation_improved"
    ]

    assert len(validations) == 2
    assert all(row["after_rms_ppm"] < 1e-6 for row in validations)
    assert payload["summary"][0]["cross_source_validation_improved_count"] >= 2


def test_cross_review_builds_same_device_same_target_humidity_pairs(tmp_path):
    csv_path = tmp_path / "paired.csv"
    _write_csv(
        csv_path,
        [
            {
                "device_id": "022",
                "target_ppm": "500",
                "raw_senco13_co2_ppm": "499.95",
                "h2o_mmol_mol": "0.1",
                "temperature_c": "20",
                "pressure_hpa": "1013.25",
            },
            {
                "device_id": "022",
                "target_ppm": "500",
                "raw_senco13_co2_ppm": "496.00",
                "h2o_mmol_mol": "4.0",
                "temperature_c": "20.2",
                "pressure_hpa": "1013.40",
            },
            {
                "device_id": "022",
                "target_ppm": "900",
                "raw_senco13_co2_ppm": "899.0",
                "h2o_mmol_mol": "0.2",
            },
        ],
    )

    payload = build_co2_h2o_cross_effect_review(csv_paths=[csv_path], source_labels=["paired"])
    pairs = payload["paired_humidity_contrast"]
    device_rows = payload["device_summary"]

    assert len(pairs) == 1
    assert pairs[0]["device_id"] == "022"
    assert pairs[0]["co2_target_ppm"] == 500.0
    assert pairs[0]["h2o_delta_mmol_mol"] == 3.9
    assert pairs[0]["status"] == "cross_candidate"
    assert device_rows[0]["device_id"] == "022"
    assert payload["summary"][0]["paired_humidity_cross_candidate_count"] == 1


def test_cross_review_writes_chinese_artifacts(tmp_path):
    csv_path = tmp_path / "cross.csv"
    _write_csv(
        csv_path,
        [
            {
                "device_id": "022",
                "target_ppm": "900",
                "raw_senco13_co2_ppm": "899",
                "h2o_mmol_mol": "1.0",
            }
        ],
    )

    outputs = write_co2_h2o_cross_effect_review(
        csv_paths=[csv_path],
        source_labels=["single"],
        output_dir=tmp_path / "out",
    )

    assert outputs["summary_csv"].exists()
    assert outputs["source_summary_csv"].exists()
    assert outputs["normalized_rows_csv"].exists()
    assert outputs["model_candidates_csv"].exists()
    assert outputs["cross_source_validation_csv"].exists()
    assert outputs["device_summary_csv"].exists()
    assert outputs["paired_humidity_contrast_csv"].exists()
    assert "水汽交叉影响" in outputs["markdown"].read_text(encoding="utf-8")
