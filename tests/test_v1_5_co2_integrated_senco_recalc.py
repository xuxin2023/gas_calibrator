import csv
import json

from gas_calibrator.validation.co2_integrated_senco_recalc import (
    build_co2_integrated_senco_recalc_tables,
    write_co2_integrated_senco_recalc_report,
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


def _raw_co2(ratio, temp_c):
    temp_k = temp_c + 273.15
    return 12.0 + 80.0 * ratio - 7.0 * ratio**2 + 0.2 * temp_k + 0.03 * ratio * temp_k


def _fit_rows(device_id="022", c0=11.0, c1=0.66):
    rows = []
    for temp_c in (-10.0, 0.0, 20.0, 40.0):
        for ratio in (0.2, 0.6, 1.0, 1.4):
            rows.append(
                {
                    "component": "co2",
                    "analyzer_device_id": device_id,
                    "analyzer_prefix": f"GA{int(device_id):02d}",
                    "residual_role": "fit",
                    "point_identity": f"T{temp_c}_R{ratio}",
                    "target_value": f"{_raw_co2(ratio, temp_c) * c1 + c0:.12f}",
                    "ratio": f"{ratio:.12f}",
                    "temperature_c": f"{temp_c:.3f}",
                    "pressure_hpa": "1013.25",
                }
            )
    return rows


def _senco5_rows(device_id="022", c0=11.0, c1=0.66):
    return [
        {
            "analyzer_device_id": device_id,
            "getco_group": "5",
            "coefficient_values_json": json.dumps([c0, c1]),
        }
    ]


def test_preserved_senco5_is_diagnostic_not_senco13_write_candidate(tmp_path):
    points = tmp_path / "fit_points.csv"
    snapshot = tmp_path / "sampling_senco5.csv"
    _write_csv(points, _fit_rows(c0=11.0, c1=0.66))
    _write_csv(snapshot, _senco5_rows(c0=11.0, c1=0.66))

    tables = build_co2_integrated_senco_recalc_tables(
        fit_residuals_csv=points,
        sampling_senco5_snapshot_csv=snapshot,
        target_device_ids=("022",),
    )

    summary = {
        row["scenario"]: row
        for row in tables["integrated_senco_recalc_summary"]
        if row["device_id"] == "022"
    }
    assert summary["sampling_snapshot_preserve_senco5"]["linear_layer_status"] == (
        "large_existing_linear_layer_preserve_only_with_explicit_review"
    )
    assert summary["sampling_snapshot_preserve_senco5"]["fit_strategy"] == "preserve_existing_final_linear_layer"
    assert summary["sampling_snapshot_preserve_senco5"]["status"] == (
        "diagnostic_only_senco5_final_affine_replay_not_senco13_write_candidate"
    )
    assert summary["sampling_snapshot_preserve_senco5"]["senco13_write_candidate"] is False
    assert summary["sampling_snapshot_preserve_senco5"]["wrong_layer_merge_blocked"] is True
    assert float(summary["sampling_snapshot_preserve_senco5"]["rounded_max_abs_error_ppm"]) < 0.05

    coeffs = {
        (row["scenario"], row["term"]): float(row["coefficient"])
        for row in tables["integrated_senco_recalc_coefficients"]
    }
    assert coeffs[("force_neutral_senco5", "intercept")] != coeffs[
        ("sampling_snapshot_preserve_senco5", "intercept")
    ]


def test_negative_existing_senco5_is_diagnostic_not_silent_neutralization(tmp_path):
    points = tmp_path / "fit_points.csv"
    snapshot = tmp_path / "sampling_senco5.csv"
    _write_csv(points, _fit_rows(device_id="033", c0=1037.0, c1=-1.0))
    _write_csv(snapshot, _senco5_rows(device_id="033", c0=1037.0, c1=-1.0))

    tables = build_co2_integrated_senco_recalc_tables(
        fit_residuals_csv=points,
        sampling_senco5_snapshot_csv=snapshot,
        target_device_ids=("033",),
    )

    summary = {
        row["scenario"]: row
        for row in tables["integrated_senco_recalc_summary"]
        if row["device_id"] == "033"
    }
    assert summary["sampling_snapshot_preserve_senco5"]["status"] == (
        "diagnostic_only_senco5_final_affine_replay_not_senco13_write_candidate"
    )
    assert summary["sampling_snapshot_preserve_senco5"]["linear_layer_status"] == (
        "nonmonotonic_negative_slope_high_risk_final_affine_layer_diagnostic"
    )
    assert summary["sampling_snapshot_preserve_senco5"]["senco13_write_candidate"] is False
    assert summary["sampling_snapshot_preserve_senco5"]["wrong_layer_merge_blocked"] is True
    assert float(summary["sampling_snapshot_preserve_senco5"]["rounded_max_abs_error_ppm"]) < 0.05


def test_report_is_offline_only_and_records_sampling_snapshot(tmp_path):
    points = tmp_path / "fit_points.csv"
    snapshot = tmp_path / "sampling_senco5.csv"
    _write_csv(points, _fit_rows(c0=0.0, c1=1.0))
    _write_csv(snapshot, _senco5_rows(c0=0.0, c1=1.0))

    outputs = write_co2_integrated_senco_recalc_report(
        fit_residuals_csv=points,
        sampling_senco5_snapshot_csv=snapshot,
        output_dir=tmp_path / "review",
        target_device_ids=("022",),
    )

    assert outputs["markdown"].exists()
    meta = json.loads(outputs["meta"].read_text(encoding="utf-8"))
    assert meta["boundary"] == {
        "opens_com_ports": False,
        "controls_water_or_gas_routes": False,
        "writes_coefficients": False,
    }
    assert meta["inputs"]["sampling_senco5_snapshot_csv"].endswith("sampling_senco5.csv")
