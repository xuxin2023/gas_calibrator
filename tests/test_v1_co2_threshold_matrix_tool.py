from __future__ import annotations

import csv
import json
from datetime import datetime, timedelta
from pathlib import Path

from gas_calibrator.logging_utils import _field_label
from gas_calibrator.tools.audit_v1_co2_threshold_matrix import run_threshold_matrix_audit


def _write_csv(path: Path, rows: list[dict]) -> None:
    header: list[str] = []
    for row in rows:
        for key in row.keys():
            if key not in header:
                header.append(key)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=header)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _runtime_cfg() -> dict:
    return {
        "devices": {
            "gas_analyzers": [
                {"name": "ga01", "enabled": True},
                {"name": "ga02", "enabled": True},
            ]
        },
        "workflow": {
            "sampling": {
                "interval_s": 1.0,
                "co2_interval_s": 1.0,
                "quality": {
                    "co2_steady_state_enabled": True,
                    "co2_steady_state_policy": "warn",
                    "co2_steady_state_min_samples": 4,
                    "co2_steady_state_fallback_samples": 4,
                    "co2_steady_state_max_std_ppm": 0.2,
                    "co2_steady_state_max_range_ppm": 0.4,
                    "co2_steady_state_max_abs_slope_ppm_per_s": 0.2,
                    "co2_bad_frame_quarantine_enabled": True,
                    "co2_source_trust_enabled": True,
                    "co2_bad_frame_isolated_spike_delta_ppm": 50.0,
                    "co2_bad_frame_neighbor_match_max_delta_ppm": 8.0,
                },
            }
        },
    }


def _sample_rows() -> list[dict]:
    start = datetime(2026, 4, 13, 12, 0, 0)
    rows: list[dict] = []
    spec = [
        (1, "co2_20c_500ppm", 500.0, [500.0, 500.1, 499.9, 500.0, 500.1, 499.9], None),
        (2, "co2_20c_500ppm_dirty", 500.0, [500.0, 500.0, 999999.0, 500.0, 500.0], None),
        (3, "co2_20c_620ppm_fallback", 620.0, [999999.0, 999999.0, 999999.0, 999999.0], "ga02"),
    ]
    cursor = 0
    for point_row, tag, target, values, fallback_source in spec:
        for idx, value in enumerate(values):
            ts = start + timedelta(seconds=cursor)
            row = {
                "point_row": point_row,
                "point_phase": "co2",
                "point_tag": tag,
                "point_title": f"{tag}_title",
                "temp_chamber_c": 20.0,
                "co2_ppm_target": target,
                "pressure_target_hpa": 700.0,
                "pressure_mode": "sealed_low_pressure",
                "sample_ts": ts.isoformat(timespec="milliseconds"),
                "sample_start_ts": ts.isoformat(timespec="milliseconds"),
                "sample_end_ts": (ts + timedelta(milliseconds=100)).isoformat(timespec="milliseconds"),
                "co2_ppm": value,
                "frame_usable": True,
                "frame_status": "可用",
                "chamber_temp_c": 1.2,
                "case_temp_c": 1.1,
            }
            if fallback_source == "ga02":
                row.update(
                    {
                        "ga01_co2_ppm": 999999.0,
                        "ga01_frame_usable": True,
                        "ga01_frame_status": "可用",
                        "ga02_co2_ppm": [620.0, 620.1, 619.9, 620.0][idx],
                        "ga02_frame_usable": True,
                        "ga02_frame_status": "可用",
                    }
                )
            rows.append(row)
            cursor += 1
    return rows


def _points_rows() -> list[dict]:
    return [
        {
            "point_row": 1,
            "point_phase": "co2",
            "point_tag": "co2_20c_500ppm",
            "point_title": "clean_point",
            "temp_chamber_c": 20.0,
            "co2_ppm_target": 500.0,
            "pressure_target_hpa": 700.0,
            "pressure_mode": "sealed_low_pressure",
        },
        {
            "point_row": 2,
            "point_phase": "co2",
            "point_tag": "co2_20c_500ppm_dirty",
            "point_title": "dirty_point",
            "temp_chamber_c": 20.0,
            "co2_ppm_target": 500.0,
            "pressure_target_hpa": 700.0,
            "pressure_mode": "sealed_low_pressure",
        },
        {
            "point_row": 3,
            "point_phase": "co2",
            "point_tag": "co2_20c_620ppm_fallback",
            "point_title": "fallback_point",
            "temp_chamber_c": 20.0,
            "co2_ppm_target": 620.0,
            "pressure_target_hpa": 700.0,
            "pressure_mode": "sealed_low_pressure",
        },
    ]


def test_threshold_matrix_audit_outputs_artifacts_and_expected_stats(tmp_path: Path) -> None:
    samples_csv = tmp_path / "samples.csv"
    points_csv = tmp_path / "points.csv"
    output_dir = tmp_path / "audit"
    _write_csv(samples_csv, _sample_rows())
    _write_csv(points_csv, _points_rows())

    result = run_threshold_matrix_audit(
        samples_csvs=[samples_csv],
        points_csvs=[points_csv],
        runtime_cfgs=[_runtime_cfg()],
        output_dir=output_dir,
    )

    assert result["summary_csv"].exists()
    assert result["summary_json"].exists()
    assert result["report_md"].exists()
    assert result["details_csv"].exists()

    summary = json.loads(result["summary_json"].read_text(encoding="utf-8"))
    scenario_names = {row["scenario"] for row in summary["summary"]}
    assert "legacy_baseline" in scenario_names
    assert "current_hardened_baseline" in scenario_names
    assert "quarantine_disabled" in scenario_names
    assert summary["not_real_acceptance_evidence"] is True

    with result["details_csv"].open("r", encoding="utf-8-sig", newline="") as handle:
        details = list(csv.DictReader(handle))

    current_dirty = next(
        row for row in details if row["scenario"] == "current_hardened_baseline" and row["point_row"] == "2"
    )
    legacy_dirty = next(row for row in details if row["scenario"] == "legacy_baseline" and row["point_row"] == "2")
    fallback_point = next(
        row for row in details if row["scenario"] == "current_hardened_baseline" and row["point_row"] == "3"
    )
    assert float(current_dirty["measured_value"]) == 500.0
    assert float(legacy_dirty["measured_value"]) > 100000.0
    assert "co2_value_sentinel" in current_dirty["co2_quarantine_reason_summary"]
    assert fallback_point["co2_source_selected"] == "ga02"
    assert "primary_lost_to=ga02" in fallback_point["co2_source_switch_reason"]

    report_text = result["report_md"].read_text(encoding="utf-8")
    assert "replay evidence only" in report_text
    assert "legacy baseline" in report_text
    assert "current hardened baseline" in report_text
    assert "建议默认阈值" in report_text


def test_threshold_matrix_audit_accepts_chinese_headers_and_missing_fields(tmp_path: Path) -> None:
    samples_csv = tmp_path / "samples_zh.csv"
    output_dir = tmp_path / "audit_zh"
    start = datetime(2026, 4, 13, 13, 0, 0)
    rows = []
    for idx, value in enumerate([400.0, 400.1, 399.9, 400.0]):
        ts = start + timedelta(seconds=idx)
        rows.append(
            {
                _field_label("point_row"): 11,
                _field_label("point_phase"): "co2",
                _field_label("point_tag"): "zh_point",
                _field_label("point_title"): "中文点",
                _field_label("temp_chamber_c"): 10.0,
                _field_label("sample_ts"): ts.isoformat(timespec="milliseconds"),
                _field_label("sample_end_ts"): (ts + timedelta(milliseconds=100)).isoformat(timespec="milliseconds"),
                _field_label("co2_ppm"): value,
                _field_label("frame_usable"): True,
                _field_label("frame_status"): "可用",
            }
        )
    _write_csv(samples_csv, rows)

    result = run_threshold_matrix_audit(
        samples_csvs=[samples_csv],
        points_csvs=[None],
        runtime_cfgs=[_runtime_cfg()],
        output_dir=output_dir,
    )

    summary = json.loads(result["summary_json"].read_text(encoding="utf-8"))
    assert summary["summary"]
    with result["details_csv"].open("r", encoding="utf-8-sig", newline="") as handle:
        details = list(csv.DictReader(handle))
    current = next(row for row in details if row["scenario"] == "current_hardened_baseline")
    assert current["point_row"] == "11"
    assert current["point_tag"] == "zh_point"
    assert current["co2_ppm_target"] == ""
    assert result["report_md"].exists()
