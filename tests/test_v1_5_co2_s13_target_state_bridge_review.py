import csv
import json

from gas_calibrator.validation.co2_s13_target_state_bridge_review import (
    build_co2_s13_target_state_bridge_review,
    write_co2_s13_target_state_bridge_review,
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


def _fit_rows():
    rows = []
    for temp, pressure, dewpoint in ((20.0, 1012.0, -34.0), (30.0, 1011.8, -33.8)):
        for target, ratio, rel_error in ((100.0, 1.1, -3.0 if temp == 20.0 else 3.2), (300.0, 1.0, 0.6)):
            rows.append(
                {
                    "component": "co2",
                    "analyzer_device_id": "058",
                    "source_role": "fit",
                    "point_identity": f"T{temp:g}_{target:g}ppm",
                    "target_value": f"{target}",
                    "ratio": f"{ratio}",
                    "temperature_c": f"{temp}",
                    "pressure_hpa": f"{pressure}",
                    "co2_ratio_f_std": "0.0002",
                    "dewpoint_mean_c": f"{dewpoint}",
                    "h2o_mmol_mean": "0.2",
                }
            )
    return rows


def _residual_rows():
    rows = []
    for row in _fit_rows():
        target = float(row["target_value"])
        rel = -3.0 if row["point_identity"] == "T20_100ppm" else 3.2 if row["point_identity"] == "T30_100ppm" else 0.6
        rows.append(
            {
                "device_id": "058",
                "source_role": "fit",
                "point_identity": row["point_identity"],
                "target_ppm": row["target_value"],
                "temperature_c": row["temperature_c"],
                "pressure_hpa": row["pressure_hpa"],
                "ratio": row["ratio"],
                "relative_error_percent": f"{rel}",
                "error_ppm": f"{target * rel / 100.0}",
                "objective_id": "low_end_priority_lstsq",
                "zero_offset_ppm": "0.0",
                "model_id": "core_plus_full_temp__low_end_priority_lstsq__zero0",
            }
        )
    rows.append(
        {
            "device_id": "058",
            "source_role": "fit",
            "point_identity": "T20_100ppm_wrong_model",
            "target_ppm": "100.0",
            "temperature_c": "20.0",
            "relative_error_percent": "99.0",
            "error_ppm": "99.0",
            "objective_id": "absolute_lstsq",
            "zero_offset_ppm": "0.0",
            "model_id": "core_plus_full_temp__absolute_lstsq__zero0",
            "structure_id": "core_plus_full_temp",
        }
    )
    rows.append(
        {
            "device_id": "058",
            "source_role": "fit",
            "point_identity": "T20_100ppm_wrong_structure",
            "target_ppm": "100.0",
            "temperature_c": "20.0",
            "relative_error_percent": "88.0",
            "error_ppm": "88.0",
            "objective_id": "low_end_priority_lstsq",
            "zero_offset_ppm": "0.0",
            "model_id": "core_ratio_only_diagnostic__low_end_priority_lstsq__zero0",
            "structure_id": "core_ratio_only_diagnostic",
        }
    )
    return rows


def _selected_rows():
    return [
        {
            "device_id": "058",
            "best_objective_id": "low_end_priority_lstsq",
            "best_zero_offset_ppm": "0.0",
            "best_model_id": "core_plus_full_temp__low_end_priority_lstsq__zero0",
            "best_structure_id": "core_plus_full_temp",
        }
    ]


def test_bridge_review_detects_same_target_sign_flip_without_mixing_models(tmp_path):
    fit_csv = tmp_path / "fit.csv"
    residuals_csv = tmp_path / "residuals.csv"
    selected_csv = tmp_path / "selected.csv"
    _write_csv(fit_csv, _fit_rows())
    _write_csv(residuals_csv, _residual_rows())
    _write_csv(selected_csv, _selected_rows())

    tables = build_co2_s13_target_state_bridge_review(
        fit_points_csv=fit_csv,
        residuals_csv=residuals_csv,
        selected_candidates_csv=selected_csv,
    )

    enriched_ids = {row["point_identity"] for row in tables["selected_residual_state_rows"]}
    assert "T20_100ppm_wrong_model" not in enriched_ids
    assert "T20_100ppm_wrong_structure" not in enriched_ids

    bridge = {
        float(row["target_ppm"]): row
        for row in tables["same_target_state_bridge"]
        if row["device_id"] == "058"
    }
    assert bridge[100.0]["bridge_status"] == "sign_flip_without_obvious_state_span"
    assert bridge[100.0]["ratio_span"] != ""
    assert tables["root_cause_summary"][0]["same_target_sign_flip_count"] == 1
    assert "low_end_temperature_shape" in tables["root_cause_summary"][0]["primary_hypothesis"]


def test_bridge_review_writes_artifacts_with_no_write_boundary(tmp_path):
    fit_csv = tmp_path / "fit.csv"
    residuals_csv = tmp_path / "residuals.csv"
    selected_csv = tmp_path / "selected.csv"
    _write_csv(fit_csv, _fit_rows())
    _write_csv(residuals_csv, _residual_rows())
    _write_csv(selected_csv, _selected_rows())

    outputs = write_co2_s13_target_state_bridge_review(
        fit_points_csv=fit_csv,
        residuals_csv=residuals_csv,
        selected_candidates_csv=selected_csv,
        output_dir=tmp_path / "out",
    )

    meta = json.loads(outputs["metadata"].read_text(encoding="utf-8"))
    assert meta["boundary"]["opens_com_ports"] is False
    assert meta["boundary"]["writes_coefficients"] is False
    assert outputs["markdown"].read_bytes()[:3] == b"\xef\xbb\xbf"
    rows = list(csv.DictReader(outputs["same_target_state_bridge"].open(encoding="utf-8-sig")))
    assert rows
