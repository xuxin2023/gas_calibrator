import csv
import json

from gas_calibrator.validation.co2_senco13_scope_candidate_review import (
    build_co2_senco13_scope_candidate_review_tables,
    write_co2_senco13_scope_candidate_review,
)


def _write_csv(path, rows):
    headers = []
    for row in rows:
        for key in row:
            if key not in headers:
                headers.append(key)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=headers)
        writer.writeheader()
        writer.writerows(rows)


def _target_from_ratio_temp(ratio, temp_c):
    temp_k = temp_c + 273.15
    return 5.0 + 160.0 * ratio - 9.0 * ratio**2 + 2.0 * ratio**3 + 0.08 * temp_k + 0.018 * ratio * temp_k


def _rows():
    rows = []
    for temp in (-20, -10, 0, 10, 20, 30, 40):
        targets = range(0, 1100, 100) if temp in {10, 20, 30} else (0, 400, 1000)
        for target in targets:
            role = "fit" if target == 0 or target in {100, 300, 500, 700, 900} else "verification"
            ratio = (target - 10.0) / 900.0 + temp * 0.002
            rows.append(
                {
                    "source_set": "old_fulltemp_prewrite",
                    "point_identity": f"p_T{'m' + str(abs(temp)) if temp < 0 else temp}_{target}ppm_{role}",
                    "device_id": "100",
                    "role": role,
                    "target_ppm": f"{_target_from_ratio_temp(ratio, temp):.12f}",
                    "ratio": f"{ratio:.12f}",
                    "temperature_c": f"{float(temp):.3f}",
                    "pressure_hpa": "1013.25",
                    "h2o_mmol_mol": "2.0",
                    "sample_count": "10",
                    "usable_count": "10",
                }
            )
    for temp, target in ((20, 100), (20, 900)):
        ratio = (target - 10.0) / 900.0 + temp * 0.002
        rows.append(
            {
                "source_set": "current_bridge",
                "point_identity": f"current_{target}ppm",
                "device_id": "100",
                "role": "verification",
                "target_ppm": f"{_target_from_ratio_temp(ratio, temp):.12f}",
                "ratio": f"{ratio:.12f}",
                "temperature_c": f"{float(temp):.3f}",
                "pressure_hpa": "1013.25",
                "h2o_mmol_mol": "2.0",
                "sample_count": "10",
                "usable_count": "10",
            }
        )
    return rows


def test_senco13_scope_candidate_keeps_three_training_scopes_distinct(tmp_path):
    points = tmp_path / "points.csv"
    _write_csv(points, _rows())

    tables = build_co2_senco13_scope_candidate_review_tables(points_csv=points, target_device_id="100")
    current = [
        row
        for row in tables["co2_senco13_scope_candidate_summary"]
        if row["eval_set"] == "current_bridge"
    ]
    by_scope = {row["training_scope"]: row for row in current}

    assert int(by_scope["fit_only_previous_candidate_subset"]["train_count"]) == 22
    assert int(by_scope["all_sampled_points"]["train_count"]) == 45
    assert int(by_scope["central_full_grid_T10_T20_T30"]["train_count"]) == 33


def test_senco13_candidate_payload_freezes_pressure_slots(tmp_path):
    points = tmp_path / "points.csv"
    _write_csv(points, _rows())

    tables = build_co2_senco13_scope_candidate_review_tables(points_csv=points, target_device_id="100")
    summary = [
        row
        for row in tables["co2_senco13_scope_candidate_summary"]
        if row["training_scope"] == "all_sampled_points" and row["eval_set"] == "old_all"
    ][0]

    senco3 = json.loads(summary["rounded_senco3_payload_json"])
    assert senco3[3:] == [0.0, 0.0, 0.0]
    assert summary["pressure_terms"] == "frozen_zero"
    assert summary["controls_water_or_gas_routes"] is False
    assert summary["writes_coefficients"] is False


def test_senco13_scope_candidate_report_is_offline_and_chinese(tmp_path):
    points = tmp_path / "points.csv"
    _write_csv(points, _rows())

    outputs = write_co2_senco13_scope_candidate_review(
        points_csv=points,
        output_dir=tmp_path / "review",
        target_device_id="100",
    )

    meta = json.loads(outputs["manifest_json"].read_text(encoding="utf-8"))
    assert meta["boundary"] == {
        "opens_com_ports": False,
        "controls_water_or_gas_routes": False,
        "writes_coefficients": False,
    }
    markdown = outputs["markdown"].read_text(encoding="utf-8")
    assert "SENCO1/SENCO3 训练口径候选评审" in markdown
    assert "压力通道由 SENCO9 独立流程处理" in markdown
