from __future__ import annotations

import csv
from pathlib import Path

from gas_calibrator.validation.v1_5_temperature_channel_review import (
    build_temperature_channel_summary,
    build_temperature_observations_from_point_dirs,
    evaluate_co2_residual_temperature_impact,
    export_temperature_channel_review,
)


def _write_point_csv(path: Path, *, ref_temp_c: float, temp_setpoint_c: float) -> None:
    point_dir = path / f"p001_T{temp_setpoint_c:g}_HG0C_50RH_h2o"
    point_dir.mkdir(parents=True)
    csv_path = point_dir / "points_test.csv"
    headers = [
        "保存时间",
        "点位标题",
        "点位标签",
        "温箱目标温度C",
        "数字温度计缓存年龄ms_平均值",
        "数字温度计温度C_平均值",
        "气体分析仪22_温度箱温度C_平均值",
        "气体分析仪22_机壳温度C_平均值",
        "气体分析仪30_温度箱温度C_平均值",
        "气体分析仪30_机壳温度C_平均值",
        "气体分析仪23_温度箱温度C_平均值",
        "气体分析仪23_机壳温度C_平均值",
        "气体分析仪100_温度箱温度C_平均值",
        "气体分析仪100_机壳温度C_平均值",
    ]
    row = {
        "保存时间": "2026-05-30T00:00:00",
        "点位标题": "synthetic h2o point",
        "点位标签": "synthetic",
        "温箱目标温度C": temp_setpoint_c,
        "数字温度计缓存年龄ms_平均值": 1000,
        "数字温度计温度C_平均值": ref_temp_c,
        "气体分析仪22_温度箱温度C_平均值": ref_temp_c + 2.0,
        "气体分析仪22_机壳温度C_平均值": ref_temp_c + 2.5,
        "气体分析仪30_温度箱温度C_平均值": ref_temp_c + 1.0,
        "气体分析仪30_机壳温度C_平均值": ref_temp_c + 1.5,
        "气体分析仪23_温度箱温度C_平均值": 60.0,
        "气体分析仪23_机壳温度C_平均值": 60.0,
        "气体分析仪100_温度箱温度C_平均值": ref_temp_c,
        "气体分析仪100_机壳温度C_平均值": ref_temp_c,
    }
    with csv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=headers)
        writer.writeheader()
        writer.writerow(row)


def _write_residual_csv(path: Path) -> None:
    rows = []
    for analyzer_id in ("022", "030"):
        for idx, ratio in enumerate([0.10, 0.20, 0.30, 0.40, 0.50, 0.60, 0.70]):
            temperature_c = float(idx * 5)
            rows.append(
                {
                    "analyzer_id": analyzer_id,
                    "target_value": 50.0 + 1000.0 * ratio + 0.1 * temperature_c,
                    "ratio": ratio,
                    "temperature_c": temperature_c,
                }
            )
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["analyzer_id", "target_value", "ratio", "temperature_c"])
        writer.writeheader()
        writer.writerows(rows)


def test_build_temperature_observations_and_summary(tmp_path: Path) -> None:
    for temp in (0.0, 20.0, 40.0):
        _write_point_csv(tmp_path, ref_temp_c=temp + 0.2, temp_setpoint_c=temp)

    observations = build_temperature_observations_from_point_dirs(
        tmp_path.glob("p*_h2o"),
        target_device_ids=("022", "030"),
        excluded_device_ids=("023", "100"),
    )

    assert {row["analyzer_id"] for row in observations} == {"022", "030", "023", "100"}
    assert all(row["valid_for_cell_fit"] for row in observations if row["analyzer_id"] == "022")
    assert all(row["valid_for_shell_fit"] for row in observations if row["analyzer_id"] == "030")
    assert all(not row["valid_for_cell_fit"] for row in observations if row["analyzer_id"] == "023")
    assert all(not row["valid_for_cell_fit"] for row in observations if row["analyzer_id"] == "100")

    summary = build_temperature_channel_summary(observations, target_device_ids=("022", "030"))
    row_022 = next(row for row in summary if row["analyzer_id"] == "022")
    row_030 = next(row for row in summary if row["analyzer_id"] == "030")
    assert row_022["cell_valid_points"] == 3
    assert row_022["cell_delta_mean_c"] == 2.0
    assert row_030["shell_delta_mean_c"] == 1.5
    assert row_022["coverage_status"] == "review"


def test_evaluate_co2_temperature_impact_and_export(tmp_path: Path) -> None:
    point_parent = tmp_path / "points"
    for temp in (0.0, 10.0, 20.0, 30.0, 40.0):
        _write_point_csv(point_parent, ref_temp_c=temp, temp_setpoint_c=temp)
    residual_csv = tmp_path / "candidate_fit_residuals.csv"
    _write_residual_csv(residual_csv)

    observations = build_temperature_observations_from_point_dirs(
        point_parent.glob("p*_h2o"),
        target_device_ids=("022", "030"),
        excluded_device_ids=("023", "100"),
    )
    impact_rows = evaluate_co2_residual_temperature_impact(
        residual_csv,
        observations,
        target_device_ids=("022", "030"),
    )
    assert {(row["analyzer_id"], row["temperature_mode"]) for row in impact_rows} >= {
        ("022", "raw_internal_temperature"),
        ("022", "candidate_corrected_temperature"),
        ("030", "raw_internal_temperature"),
        ("030", "candidate_corrected_temperature"),
    }

    output_dir = tmp_path / "review"
    payload = export_temperature_channel_review(
        output_dir,
        h2o_points_parent=point_parent,
        co2_residual_csv=residual_csv,
        target_device_ids=("022", "030"),
        excluded_device_ids=("023", "100"),
    )
    assert payload["summary_rows"]
    assert payload["impact_rows"]
    assert (output_dir / "temperature_channel_review.md").exists()
    assert (output_dir / "temperature_compensation_coefficients.csv").exists()
