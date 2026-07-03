import csv
import json

from gas_calibrator.validation.co2_relative_s13_objective_review import (
    build_co2_relative_s13_objective_review,
    write_co2_relative_s13_objective_review,
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


def _nonlinear_target(ratio, temp_c):
    return 5.0 + 850.0 * ratio + 120.0 * ratio**2 + 70.0 * ratio**4 + 0.18 * temp_c + 0.25 * ratio * temp_c


def _rows_with_low_end_relative_pressure():
    rows = []
    for temp_c in (0.0, 20.0, 40.0):
        rows.append(
            {
                "component": "co2",
                "analyzer_device_id": "123",
                "analyzer_prefix": "GA01",
                "source_role": "fit",
                "point_identity": f"T{temp_c:g}_zero",
                "target_value": "0.0",
                "zero_anchor_class": "estimated_zero_anchor",
                "target_uncertainty_ppm": "10.0",
                "ratio": "0.0",
                "temperature_c": f"{temp_c:.3f}",
                "pressure_hpa": "1013.1",
            }
        )
        for ratio in (0.04, 0.08, 0.14, 0.25, 0.45, 0.68, 0.86, 1.0):
            rows.append(
                {
                    "component": "co2",
                    "analyzer_device_id": "123",
                    "analyzer_prefix": "GA01",
                    "source_role": "fit",
                    "point_identity": f"T{temp_c:g}_R{ratio:g}",
                    "target_value": f"{_nonlinear_target(ratio, temp_c):.12f}",
                    "ratio": f"{ratio:.12f}",
                    "temperature_c": f"{temp_c:.3f}",
                    "pressure_hpa": "1013.1",
                }
            )
    return rows


def test_relative_objective_can_reduce_low_end_relative_error(tmp_path):
    evidence = tmp_path / "co2_points.csv"
    _write_csv(evidence, _rows_with_low_end_relative_pressure())

    tables = build_co2_relative_s13_objective_review(
        fit_residuals_csv=evidence,
        zero_offsets_ppm=(0.0, 5.0),
        objectives=("absolute_lstsq", "relative_weighted_lstsq", "low_end_priority_lstsq"),
    )

    selected = tables["selected_candidates"][0]
    baseline_low = float(selected["baseline_low_end_max_abs_relative_error_percent"])
    best_low = float(selected["best_low_end_max_abs_relative_error_percent"])
    by_objective_zero0 = {
        row["objective_id"]: row
        for row in tables["objective_summary"]
        if row["device_id"] == "123" and float(row["zero_offset_ppm"]) == 0.0
    }

    assert selected["device_id"] == "123"
    assert best_low < baseline_low
    assert selected["recommended_no_write_action"] == "review_zero_anchor_s1s3_candidate"
    assert (
        float(by_objective_zero0["low_end_priority_lstsq"]["low_end_max_abs_relative_error_percent"])
        < float(by_objective_zero0["absolute_lstsq"]["low_end_max_abs_relative_error_percent"])
    )
    assert selected["auto_write_allowed"] is False
    assert selected["requires_zero_gas_traceability_review"] is True


def test_relative_objective_review_outputs_no_write_artifacts(tmp_path):
    evidence = tmp_path / "co2_points.csv"
    _write_csv(evidence, _rows_with_low_end_relative_pressure())

    outputs = write_co2_relative_s13_objective_review(
        fit_residuals_csv=evidence,
        output_dir=tmp_path / "review",
        zero_offsets_ppm=(0.0, 5.0),
    )

    meta = json.loads(outputs["metadata"].read_text(encoding="utf-8"))
    assert meta["boundary"] == {
        "opens_com_ports": False,
        "controls_water_or_gas_routes": False,
        "writes_coefficients": False,
        "not_real_acceptance_evidence": True,
    }

    selected = list(csv.DictReader(outputs["selected_candidates"].open(encoding="utf-8-sig")))
    assert selected
    assert selected[0]["best_s1_payload_scientific"]
    assert selected[0]["best_s3_payload_scientific"]

    residuals = list(csv.DictReader(outputs["objective_residuals"].open(encoding="utf-8-sig")))
    assert residuals
    assert all(row["controls_water_or_gas_routes"] == "False" for row in residuals)
    assert all(row["writes_coefficients"] == "False" for row in residuals)

    raw = outputs["markdown"].read_bytes()
    assert raw[:3] == b"\xef\xbb\xbf"
