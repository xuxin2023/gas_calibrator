import csv
import json

from gas_calibrator.validation.co2_s13_zero_anchor_contract_review import (
    build_co2_s13_zero_anchor_contract_review,
    write_co2_s13_zero_anchor_contract_review,
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


def _target(ratio, temp_c):
    temp_k = temp_c + 273.15
    return (
        5.0
        + 650.0 * ratio
        + 45.0 * ratio**2
        - 2.5 * ratio**3
        + 0.08 * temp_k
        - 0.00005 * temp_k**2
        + 0.13 * ratio * temp_k
    )


def _rows_with_five_ppm_zero_anchor():
    rows = []
    for temp_c in (-20.0, 0.0, 20.0, 40.0):
        rows.append(
            {
                "component": "co2",
                "analyzer_device_id": "88",
                "analyzer_prefix": "GA01",
                "source_role": "fit",
                "point_identity": f"T{temp_c:g}_0ppm",
                "target_value": "0.0",
                "zero_anchor_class": "estimated_zero_anchor",
                "target_uncertainty_ppm": "8.0",
                "ratio": "0.0",
                "temperature_c": f"{temp_c:.3f}",
                "pressure_hpa": "1013.1",
                "co2_ratio_f_std": "0.00018",
                "dewpoint_c_mean": "-33.0",
            }
        )
        for ratio in (0.12, 0.22, 0.42, 0.70, 1.0):
            rows.append(
                {
                    "component": "co2",
                    "analyzer_device_id": "88",
                    "analyzer_prefix": "GA01",
                    "source_role": "fit",
                    "point_identity": f"T{temp_c:g}_R{ratio:g}",
                    "target_value": f"{_target(ratio, temp_c):.12f}",
                    "ratio": f"{ratio:.12f}",
                    "temperature_c": f"{temp_c:.3f}",
                    "pressure_hpa": "1013.1",
                    "co2_ratio_f_std": "0.0002",
                    "dewpoint_c_mean": "-32.0",
                }
            )
    return rows


def test_zero_anchor_contract_selects_nonzero_assigned_value_without_s5(tmp_path):
    evidence = tmp_path / "fit_points.csv"
    _write_csv(evidence, _rows_with_five_ppm_zero_anchor())

    tables = build_co2_s13_zero_anchor_contract_review(
        fit_points_csv=evidence,
        zero_offsets_ppm=(0.0, 5.0),
        objectives=("absolute_lstsq", "low_end_priority_lstsq"),
    )

    selected = tables["zero_anchor_contract_selection"][0]
    assert selected["device_id"] == "088"
    assert float(selected["best_zero_offset_ppm"]) == 5.0
    assert selected["uses_pressure_terms"] is False
    assert selected["uses_s5_output_trim"] is False
    assert selected["auto_write_allowed"] is False
    assert "zero" in selected["recommended_no_write_action"]
    assert tables["low_end_residual_drivers"]
    assert all(
        not str(row["point_identity"]).endswith("_0ppm")
        for row in tables["low_end_residual_drivers"]
    )


def test_zero_anchor_contract_writes_chinese_no_write_artifacts(tmp_path):
    evidence = tmp_path / "fit_points.csv"
    _write_csv(evidence, _rows_with_five_ppm_zero_anchor())

    outputs = write_co2_s13_zero_anchor_contract_review(
        fit_points_csv=evidence,
        output_dir=tmp_path / "review",
        zero_offsets_ppm=(0.0, 5.0),
        objectives=("absolute_lstsq", "relative_weighted_lstsq"),
    )

    meta = json.loads(outputs["metadata"].read_text(encoding="utf-8"))
    assert meta["boundary"] == {
        "opens_com_ports": False,
        "controls_water_or_gas_routes": False,
        "writes_coefficients": False,
        "uses_pressure_terms": False,
        "uses_s5_output_trim": False,
        "not_real_acceptance_evidence": True,
    }
    selected = list(
        csv.DictReader(outputs["zero_anchor_contract_selection"].open(encoding="utf-8-sig"))
    )
    assert selected
    assert selected[0]["best_s1_payload_scientific"]
    assert selected[0]["best_s3_payload_scientific"]
    assert outputs["markdown"].read_bytes()[:3] == b"\xef\xbb\xbf"
    markdown_text = outputs["markdown"].read_text(encoding="utf-8-sig")
    assert "V1.5 CO2 S1/S3 零气锚定合同评审" in markdown_text
    assert "H2O 干气锚点和 CO2 零气锚点不是同一个概念" in markdown_text
