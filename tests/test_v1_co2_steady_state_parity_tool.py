from __future__ import annotations

import csv
import json
from datetime import datetime, timedelta
from pathlib import Path

from gas_calibrator.logging_utils import _field_label
from gas_calibrator.tools import audit_v1_co2_steady_state_parity as module


def _write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _english_point_samples(path: Path, values: list[float]) -> None:
    start = datetime(2026, 4, 13, 9, 0, 0)
    rows: list[dict[str, object]] = []
    for idx, value in enumerate(values, start=1):
        ts = start + timedelta(seconds=idx - 1)
        rows.append(
            {
                "sample_index": idx,
                "sample_ts": ts.isoformat(timespec="milliseconds"),
                "sample_start_ts": ts.isoformat(timespec="milliseconds"),
                "sample_end_ts": (ts + timedelta(milliseconds=100)).isoformat(timespec="milliseconds"),
                "point_row": 3,
                "point_phase": "co2",
                "point_tag": "co2_groupa_500ppm_ambient",
                "point_title": "CO2 500ppm 当前大气压",
                "co2_ppm_target": 500.0,
                "pressure_target_hpa": 1013.0,
                "temp_chamber_c": 20.0,
                "co2_ppm": value,
                "frame_usable": True,
                "id": "086",
            }
        )
    _write_csv(path, list(rows[0].keys()), rows)


def _english_points_readable(path: Path) -> None:
    _write_csv(
        path,
        [
            "point_row",
            "point_phase",
            "point_tag",
            "point_title",
            "co2_ppm_target",
            "pressure_target_hpa",
            "temp_chamber_c",
            "pressure_mode",
            "route",
            "measured_value_source",
            "co2_steady_window_status",
        ],
        [
            {
                "point_row": 3,
                "point_phase": "co2",
                "point_tag": "co2_groupa_500ppm_ambient",
                "point_title": "CO2 500ppm 当前大气压",
                "co2_ppm_target": 500.0,
                "pressure_target_hpa": 1013.0,
                "temp_chamber_c": 20.0,
                "pressure_mode": "ambient_open",
                "route": "co2",
                "measured_value_source": "legacy_placeholder",
                "co2_steady_window_status": "missing",
            }
        ],
    )


def _translated_row(row: dict[str, object]) -> dict[str, object]:
    return {_field_label(key): value for key, value in row.items()}


def _chinese_point_samples_missing_ts(path: Path) -> None:
    rows: list[dict[str, object]] = []
    for idx, value in enumerate([100.0, 200.0, 300.0, 400.0], start=1):
        rows.append(
            _translated_row(
                {
                    "sample_index": idx,
                    "point_row": 12,
                    "point_phase": "co2",
                    "point_tag": "co2_groupa_1000ppm_ambient",
                    "point_title": "CO2 1000ppm 当前大气压",
                    "co2_ppm_target": 1000.0,
                    "pressure_target_hpa": 1013.0,
                    "temp_chamber_c": 0.0,
                    "co2_ppm": value,
                    "frame_usable": True,
                    "id": "008",
                }
            )
        )
    _write_csv(path, list(rows[0].keys()), rows)


def test_replay_audit_compares_legacy_and_steady_state_and_batches_runs(tmp_path: Path) -> None:
    run_a = tmp_path / "run_a"
    run_b = tmp_path / "run_b"
    out_dir = tmp_path / "audit"
    run_a.mkdir()
    run_b.mkdir()

    _english_point_samples(
        run_a / "point_0003_co2_co2_groupa_500ppm_ambient_samples.csv",
        [400.0, 400.1, 399.9, 400.0, 450.0, 480.0, 500.0, 500.2, 499.9, 500.1],
    )
    _english_points_readable(run_a / "points_readable_20260413.csv")
    _english_point_samples(
        run_b / "point_0004_co2_co2_groupa_800ppm_ambient_samples.csv",
        [780.0, 790.0, 795.0, 798.0, 800.0, 800.1, 799.9, 800.0],
    )
    _write_csv(
        run_b / "points_readable_20260413.csv",
        ["point_row", "point_phase", "point_tag", "point_title", "co2_ppm_target", "pressure_target_hpa", "temp_chamber_c"],
        [
            {
                "point_row": 4,
                "point_phase": "co2",
                "point_tag": "co2_groupa_800ppm_ambient",
                "point_title": "CO2 800ppm 当前大气压",
                "co2_ppm_target": 800.0,
                "pressure_target_hpa": 1013.0,
                "temp_chamber_c": 30.0,
            }
        ],
    )

    result = module.run_v1_co2_steady_state_parity_audit(
        run_dirs=[run_a, run_b],
        output_dir=out_dir,
    )

    assert result["total_points"] == 2
    assert result["status_counts"]["pass"] >= 1
    assert Path(result["summary_csv"]).exists()
    assert Path(result["summary_json"]).exists()
    assert Path(result["report_md"]).exists()

    with Path(result["summary_csv"]).open("r", encoding="utf-8-sig", newline="") as handle:
        rows = list(csv.DictReader(handle))
    assert len(rows) == 2
    target_row = next(row for row in rows if row["point_row"] == "3")
    assert abs(float(target_row["legacy_representative_value"]) - 453.02) < 1e-6
    assert abs(float(target_row["new_representative_value"]) - 500.05) < 1e-6
    assert target_row["measured_value_source"] == "co2_steady_state_window"
    assert target_row["co2_steady_window_found"] == "True"
    assert target_row["audit_status"] == "pass"

    summary_payload = json.loads(Path(result["summary_json"]).read_text(encoding="utf-8"))
    assert summary_payload["evidence_source"] == "replay"
    assert summary_payload["not_real_acceptance_evidence"] is True
    assert "co2_ppm_target" in summary_payload["bucket_summaries"]
    report_text = Path(result["report_md"]).read_text(encoding="utf-8")
    assert "replay evidence only" in report_text
    assert "not real acceptance evidence" in report_text
    assert "变化最大的点" in report_text


def test_replay_audit_handles_chinese_headers_and_missing_fields_with_degraded_note(tmp_path: Path) -> None:
    run_dir = tmp_path / "run_cn"
    out_dir = tmp_path / "audit_cn"
    run_dir.mkdir()

    _chinese_point_samples_missing_ts(run_dir / "point_0012_co2_co2_groupa_1000ppm_ambient_samples.csv")

    result = module.run_v1_co2_steady_state_parity_audit(
        run_dirs=[run_dir],
        output_dir=out_dir,
    )

    with Path(result["summary_csv"]).open("r", encoding="utf-8-sig", newline="") as handle:
        rows = list(csv.DictReader(handle))
    assert len(rows) == 1
    row = rows[0]
    assert row["point_row"] == "12"
    assert row["measured_value_source"] == "co2_trailing_window_fallback"
    assert row["audit_status"] == "degraded"
    assert "sample_ts_missing=row_index_fallback" in row["degraded_reason"]
    assert "no_qualified_steady_state_window" in row["co2_steady_window_reason"]
    assert "timestamp_strategy=row_index_fallback" in row["co2_steady_window_reason"]

