import csv
import json

from gas_calibrator.validation.co2_zero_s5_sensitivity_review import (
    build_co2_zero_s5_sensitivity_review,
    write_co2_zero_s5_sensitivity_review,
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


def _rows_with_trace_zero_offset():
    rows = []
    for temp_c in (0.0, 20.0, 40.0):
        rows.append(
            {
                "component": "co2",
                "analyzer_device_id": "100",
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
        for ratio in (0.1, 0.3, 0.6, 0.9):
            rows.append(
                {
                    "component": "co2",
                    "analyzer_device_id": "100",
                    "analyzer_prefix": "GA01",
                    "source_role": "fit",
                    "point_identity": f"T{temp_c:g}_R{ratio:g}",
                    "target_value": f"{5.0 + 1000.0 * ratio:.12f}",
                    "ratio": f"{ratio:.12f}",
                    "temperature_c": f"{temp_c:.3f}",
                    "pressure_hpa": "1013.1",
                }
            )
    return rows


def test_zero_offset_sensitivity_can_recover_low_end_intercept(tmp_path):
    evidence = tmp_path / "co2_points.csv"
    _write_csv(evidence, _rows_with_trace_zero_offset())

    tables = build_co2_zero_s5_sensitivity_review(
        fit_residuals_csv=evidence,
        zero_offsets_ppm=(0.0, 5.0),
    )
    selected = tables["selected_candidates"][0]

    assert selected["device_id"] == "100"
    assert float(selected["best_s1s3_zero_offset_ppm"]) == 5.0
    assert float(selected["best_s1s3_max_abs_relative_error_percent"]) < 1.0e-8
    assert float(selected["baseline_zero0_s1s3_max_abs_relative_error_percent"]) > 0.1
    assert selected["requires_zero_gas_traceability_review"] is True


def test_senco5_review_is_output_layer_and_no_write(tmp_path):
    evidence = tmp_path / "co2_points.csv"
    _write_csv(evidence, _rows_with_trace_zero_offset())

    outputs = write_co2_zero_s5_sensitivity_review(
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
    assert selected[0]["best_s5_command_preview"].startswith("SENCO5,YGAS,FFF,")
    assert len(selected[0]["best_s5_command_preview"].split(",")[-1].split(".")[-1]) == 3

    scenario = list(csv.DictReader(outputs["scenario_summary"].open(encoding="utf-8-sig")))
    assert scenario
    assert all(row["controls_water_or_gas_routes"] == "False" for row in scenario)
    assert all(row["writes_coefficients"] == "False" for row in scenario)
