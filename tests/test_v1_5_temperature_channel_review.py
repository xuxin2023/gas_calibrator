from __future__ import annotations

import csv
import json
from pathlib import Path

from gas_calibrator.validation.v1_5_temperature_channel_review import (
    build_temperature_channel_summary,
    build_temperature_observations_from_open_flow_point_dirs,
    build_temperature_observations_from_point_dirs,
    build_temperature_observations_from_snapshot_run_dirs,
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


def _slot_column(slot: int, suffix: str) -> str:
    return f"\u6c14\u4f53\u5206\u6790\u4eea{slot}_{suffix}"


def _write_snapshot_run(path: Path) -> None:
    path.mkdir(parents=True)
    summary = {
        "summary": [
            {"metric": "digital_thermometer_temp_c", "count": 3, "mean": 40.0, "min": 39.99, "max": 40.01},
            {"metric": "temperature_chamber_temp_c", "count": 3, "mean": 40.0, "min": 40.0, "max": 40.0},
        ]
    }
    (path / "temperature_evidence_from_io_summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    headers = [
        "\u91c7\u6837\u65f6\u95f4",
        "\u6e29\u7bb1\u8bbe\u5b9a\u6e29\u5ea6C",
        _slot_column(1, "\u8bbe\u5907ID"),
        _slot_column(1, "\u6e29\u5ea6\u7bb1\u6e29\u5ea6C"),
        _slot_column(1, "\u673a\u58f3\u6e29\u5ea6C"),
        _slot_column(1, "\u5206\u6790\u4eea\u538b\u529bkPa"),
        _slot_column(1, "\u5206\u6790\u4eea\u7f13\u5b58\u5e74\u9f84ms"),
    ]
    rows = [
        {
            "\u91c7\u6837\u65f6\u95f4": "2026-06-07T19:00:00",
            "\u6e29\u7bb1\u8bbe\u5b9a\u6e29\u5ea6C": "40",
            _slot_column(1, "\u8bbe\u5907ID"): "022",
            _slot_column(1, "\u6e29\u5ea6\u7bb1\u6e29\u5ea6C"): "41.0",
            _slot_column(1, "\u673a\u58f3\u6e29\u5ea6C"): "40.5",
            _slot_column(1, "\u5206\u6790\u4eea\u538b\u529bkPa"): "100.8",
            _slot_column(1, "\u5206\u6790\u4eea\u7f13\u5b58\u5e74\u9f84ms"): "20",
        }
        for _ in range(3)
    ]
    with (path / "samples_test.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=headers)
        writer.writeheader()
        writer.writerows(rows)


def _write_open_flow_point(
    path: Path,
    *,
    temp_setpoint_c: float,
    ref_temp_c: float,
    stale_ref: bool = False,
    bad_device_id: str = "023",
    route: str = "co2",
) -> None:
    path.mkdir(parents=True)
    headers = [
        "sample_ts",
        "point_title",
        "point_tag",
        "route",
        "temp_set_c",
        "thermometer_temp_c",
        "thermometer_cache_age_ms",
        "ga01_analyzer_device_id",
        "ga01_chamber_temp_c",
        "ga01_case_temp_c",
        "ga01_pressure_kpa",
        "ga01_frame_cache_age_ms",
        "ga01_frame_usable",
        "ga02_analyzer_device_id",
        "ga02_chamber_temp_c",
        "ga02_case_temp_c",
        "ga02_pressure_kpa",
        "ga02_frame_cache_age_ms",
        "ga02_frame_usable",
    ]
    rows = []
    for index in range(3):
        rows.append(
            {
                "sample_ts": f"2026-06-08T00:00:0{index}",
                "point_title": f"synthetic {route} point",
                "point_tag": f"{route}_open_flow",
                "route": route,
                "temp_set_c": temp_setpoint_c,
                "thermometer_temp_c": ref_temp_c + index * 0.01,
                "thermometer_cache_age_ms": 9000 if stale_ref else 20,
                "ga01_analyzer_device_id": "022",
                "ga01_chamber_temp_c": ref_temp_c + 1.0 + index * 0.01,
                "ga01_case_temp_c": ref_temp_c + 1.5 + index * 0.01,
                "ga01_pressure_kpa": "101.3",
                "ga01_frame_cache_age_ms": "120",
                "ga01_frame_usable": "True",
                "ga02_analyzer_device_id": bad_device_id,
                "ga02_chamber_temp_c": "60.0",
                "ga02_case_temp_c": "60.0",
                "ga02_pressure_kpa": "101.3",
                "ga02_frame_cache_age_ms": "120",
                "ga02_frame_usable": "True",
            }
        )
    with (path / "samples_machine_readable.csv").open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=headers)
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


def test_open_flow_temperature_observations_use_machine_readable_samples(tmp_path: Path) -> None:
    parent = tmp_path / "co2_open_flow"
    for temp in (-20.0, 0.0, 40.0):
        _write_open_flow_point(parent / f"p_T{temp:g}_400ppm", temp_setpoint_c=temp, ref_temp_c=temp + 0.2)

    observations = build_temperature_observations_from_open_flow_point_dirs(
        parent.glob("p*"),
        target_device_ids=("022",),
        excluded_device_ids=("023",),
    )

    assert {row["analyzer_id"] for row in observations} == {"022", "023"}
    assert all(row["route_type"] == "co2_open_flow_full_temperature" for row in observations)
    assert all(row["valid_for_cell_fit"] for row in observations if row["analyzer_id"] == "022")
    assert all(not row["valid_for_cell_fit"] for row in observations if row["analyzer_id"] == "023")
    row_022 = next(row for row in observations if row["analyzer_id"] == "022")
    assert row_022["ref_temp_source"] == "digital_thermometer_from_co2_open_flow_samples"
    assert row_022["analyzer_frame_count"] == 3
    assert row_022["cell_temp_span_c"] > 0

    output_dir = tmp_path / "open_flow_review"
    payload = export_temperature_channel_review(
        output_dir,
        open_flow_points_parent=parent,
        target_device_ids=("022", "023"),
        excluded_device_ids=(),
    )
    assert payload["summary_rows"][0]["cell_valid_points"] == 3
    assert "SENCO7,YGAS,FFF" in (output_dir / "temperature_compensation_commands.txt").read_text(encoding="utf-8")
    report_text = (output_dir / "temperature_channel_review.md").read_text(encoding="utf-8")
    assert "Open-flow evidence parent" in report_text


def test_temperature_command_export_blocks_devices_with_bad_temperature_segments(tmp_path: Path) -> None:
    parent = tmp_path / "co2_open_flow"
    _write_open_flow_point(parent / "p001_T0_400ppm", temp_setpoint_c=0.0, ref_temp_c=0.2, bad_device_id="002")
    _write_open_flow_point(parent / "p002_T20_400ppm", temp_setpoint_c=20.0, ref_temp_c=20.2, bad_device_id="002")
    _write_open_flow_point(parent / "p003_T40_400ppm", temp_setpoint_c=40.0, ref_temp_c=40.2, bad_device_id="002")

    output_dir = tmp_path / "blocked_segment_review"
    payload = export_temperature_channel_review(
        output_dir,
        open_flow_points_parent=parent,
        target_device_ids=("022", "002"),
        excluded_device_ids=(),
    )

    rows_by_device_channel = {
        (row["analyzer_id"], row["senco_channel"]): row
        for row in payload["temperature_results"]
    }
    assert rows_by_device_channel[("022", "SENCO7")]["write_eligible"] is True
    assert rows_by_device_channel[("002", "SENCO7")]["write_eligible"] is False
    assert (
        rows_by_device_channel[("002", "SENCO7")]["write_block_reason"]
        == "temperature_channel_has_blocked_segments:hard_bad_value"
    )
    commands_text = (output_dir / "temperature_compensation_commands.txt").read_text(encoding="utf-8")
    assert "SENCO7,YGAS,FFF" in commands_text
    assert rows_by_device_channel[("002", "SENCO7")]["command_string"] == ""


def test_h2o_temperature_review_prefers_machine_readable_samples(tmp_path: Path) -> None:
    parent = tmp_path / "h2o_points"
    for temp in (0.0, 20.0, 40.0):
        _write_open_flow_point(
            parent / f"p001_T{temp:g}_HG20C_50RH_h2o",
            temp_setpoint_c=temp,
            ref_temp_c=temp + 0.2,
            route="h2o",
        )

    output_dir = tmp_path / "h2o_review"
    payload = export_temperature_channel_review(
        output_dir,
        h2o_points_parent=parent,
        target_device_ids=("022",),
        excluded_device_ids=(),
    )

    observations = payload["observations"]
    assert observations
    assert all(row["route_type"] == "h2o_open_flow_full_temperature" for row in observations)
    assert all(
        row["ref_temp_source"] == "digital_thermometer_from_h2o_full_temp"
        for row in observations
    )
    summary = payload["summary_rows"]
    row_022 = next(row for row in summary if row["analyzer_id"] == "022")
    assert row_022["cell_valid_points"] == 3
    assert row_022["shell_valid_points"] == 3
    assert "SENCO7,YGAS,FFF" in (output_dir / "temperature_compensation_commands.txt").read_text(encoding="utf-8")


def test_open_flow_temperature_observations_block_stale_reference(tmp_path: Path) -> None:
    point_dir = tmp_path / "p001_T20_400ppm"
    _write_open_flow_point(point_dir, temp_setpoint_c=20.0, ref_temp_c=20.0, stale_ref=True)

    observations = build_temperature_observations_from_open_flow_point_dirs(
        [point_dir],
        target_device_ids=("022",),
        excluded_device_ids=(),
        max_reference_age_ms=5000.0,
    )

    row = next(row for row in observations if row["analyzer_id"] == "022")
    assert row["valid_for_cell_fit"] is False
    assert row["cell_fit_gate_reason"] == "reference_temperature_stale_or_missing"


def test_temperature_summary_blocks_missing_analyzer_temperature_evidence(tmp_path: Path) -> None:
    for temp in (0.0, 20.0, 40.0):
        _write_point_csv(tmp_path, ref_temp_c=temp + 0.2, temp_setpoint_c=temp)

    observations = build_temperature_observations_from_point_dirs(
        tmp_path.glob("p*_h2o"),
        target_device_ids=("001",),
        excluded_device_ids=(),
    )

    summary = build_temperature_channel_summary(observations, target_device_ids=("001",))
    row_001 = next(row for row in summary if row["analyzer_id"] == "001")
    assert row_001["cell_valid_points"] == 0
    assert row_001["shell_valid_points"] == 0
    assert row_001["coverage_status"] == "blocked_missing_analyzer_temperature_evidence"

    output_dir = tmp_path / "missing_review"
    export_temperature_channel_review(
        output_dir,
        h2o_points_parent=tmp_path,
        co2_residual_csv=None,
        target_device_ids=("001",),
        excluded_device_ids=(),
    )
    report_text = (output_dir / "temperature_channel_review.md").read_text(encoding="utf-8")
    assert "Do not generate or write SENCO7/SENCO8 candidates from this run" in report_text


def test_snapshot_temperature_review_blocks_single_setpoint_and_missing_device(tmp_path: Path) -> None:
    snapshot_dir = tmp_path / "snapshot"
    _write_snapshot_run(snapshot_dir)

    observations = build_temperature_observations_from_snapshot_run_dirs(
        [snapshot_dir],
        target_device_ids=("022", "090"),
        excluded_device_ids=(),
    )
    assert {row["analyzer_id"] for row in observations} == {"022", "090"}
    row_022 = next(row for row in observations if row["analyzer_id"] == "022")
    row_090 = next(row for row in observations if row["analyzer_id"] == "090")
    assert row_022["valid_for_cell_fit"] is True
    assert row_090["valid_for_cell_fit"] is False
    assert row_090["cell_fit_gate_reason"] == "missing_analyzer_temperature_evidence"

    summary = build_temperature_channel_summary(observations, target_device_ids=("022", "090"))
    by_id = {row["analyzer_id"]: row for row in summary}
    assert by_id["022"]["coverage_status"] == "blocked_insufficient_temperature_setpoints"
    assert by_id["090"]["coverage_status"] == "blocked_missing_analyzer_temperature_evidence"

    output_dir = tmp_path / "snapshot_review"
    payload = export_temperature_channel_review(
        output_dir,
        snapshot_run_dirs=[snapshot_dir],
        co2_residual_csv=None,
        target_device_ids=("022", "090"),
        excluded_device_ids=(),
    )
    assert payload["summary_rows"]
    assert not (output_dir / "temperature_compensation_commands.txt").read_text(encoding="utf-8").strip()
    report_text = (output_dir / "temperature_channel_review.md").read_text(encoding="utf-8")
    assert "temperature setpoint coverage is insufficient" in report_text


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
