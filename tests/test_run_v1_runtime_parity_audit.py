from __future__ import annotations

import csv
import json
from pathlib import Path

from gas_calibrator.tools.run_v1_runtime_parity_audit import run_runtime_parity_audit


def _write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    headers: list[str] = []
    for row in rows:
        for key in row:
            if key not in headers:
                headers.append(key)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=headers)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _prepare_run_dir(tmp_path: Path) -> Path:
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    _write_csv(
        run_dir / "download_plan_no_500.csv",
        [
            {
                "Analyzer": "GA03",
                "ActualDeviceId": "079",
                "Gas": "CO2",
                "a0": 100.0,
                "a1": 200.0,
                "a2": 0.0,
                "a3": 0.0,
                "a4": 0.0,
                "a5": 0.0,
                "a6": 0.0,
                "a7": 0.0,
                "a8": 0.0,
            }
        ],
    )
    _write_csv(
        run_dir / "temperature_coefficients_target.csv",
        [
            {"analyzer_id": "GA03", "senco_channel": "SENCO7", "A": 0.0, "B": 1.0, "C": 0.0, "D": 0.0},
            {"analyzer_id": "GA03", "senco_channel": "SENCO8", "A": 0.0, "B": 1.0, "C": 0.0, "D": 0.0},
        ],
    )
    return run_dir


def test_runtime_parity_legacy_only_stream_is_inconclusive(tmp_path: Path) -> None:
    run_dir = _prepare_run_dir(tmp_path)
    capture_path = run_dir / "baseline_stream_079.csv"
    _write_csv(
        capture_path,
        [
            {
                "timestamp": "2026-04-20T09:00:00",
                "stream_format": "legacy",
                "device_id": "079",
                "co2_ppm": 755.0,
                "h2o_mmol": 0.0,
                "co2_sig": 0.99,
                "h2o_sig": 0.99,
                "temp_c": 28.8,
                "pressure_kpa": 104.2,
            },
            {
                "timestamp": "2026-04-20T09:00:01",
                "stream_format": "legacy",
                "device_id": "079",
                "co2_ppm": 756.0,
                "h2o_mmol": 0.0,
                "co2_sig": 0.99,
                "h2o_sig": 0.99,
                "temp_c": 28.9,
                "pressure_kpa": 104.2,
            },
        ],
    )

    result = run_runtime_parity_audit(
        run_dir=str(run_dir),
        analyzer="GA03",
        actual_device_id="079",
        baseline_capture_path=str(capture_path),
    )

    assert result["parity_verdict"] == "parity_inconclusive_missing_runtime_inputs"
    assert result["legacy_stream_only"] is True
    assert result["final_write_ready"] is False
    assert result["readiness_code"] == "legacy_stream_insufficient_for_runtime_parity"
    assert (Path(result["runtime_parity_candidates_path"])).exists()
    assert (Path(result["runtime_parity_points_path"])).exists()
    assert (Path(result["runtime_parity_report_path"])).exists()


def test_runtime_parity_missing_ratio_fields_stays_inconclusive(tmp_path: Path) -> None:
    run_dir = _prepare_run_dir(tmp_path)
    capture_path = run_dir / "baseline_stream_mode2_missing_ratio.csv"
    _write_csv(
        capture_path,
        [
            {
                "timestamp": "2026-04-20T09:00:00",
                "stream_format": "mode2",
                "device_id": "079",
                "co2_ppm": 300.0,
                "temp_c": 25.0,
                "pressure_kpa": 101.3,
                "chamber_temp_c": 25.1,
                "case_temp_c": 25.2,
            },
            {
                "timestamp": "2026-04-20T09:00:01",
                "stream_format": "mode2",
                "device_id": "079",
                "co2_ppm": 302.0,
                "temp_c": 25.0,
                "pressure_kpa": 101.3,
                "chamber_temp_c": 25.1,
                "case_temp_c": 25.2,
            },
        ],
    )

    result = run_runtime_parity_audit(
        run_dir=str(run_dir),
        analyzer="GA03",
        actual_device_id="079",
        baseline_capture_path=str(capture_path),
    )

    assert result["parity_verdict"] == "parity_inconclusive_missing_runtime_inputs"
    assert result["final_write_ready"] is False


def test_runtime_parity_missing_chamber_case_stays_inconclusive(tmp_path: Path) -> None:
    run_dir = _prepare_run_dir(tmp_path)
    capture_path = run_dir / "baseline_stream_mode2_missing_temps.csv"
    _write_csv(
        capture_path,
        [
            {
                "timestamp": "2026-04-20T09:00:00",
                "stream_format": "mode2",
                "device_id": "079",
                "co2_ppm": 300.0,
                "co2_ratio_f": 1.0,
                "temp_c": 25.0,
                "pressure_kpa": 101.3,
            },
            {
                "timestamp": "2026-04-20T09:00:01",
                "stream_format": "mode2",
                "device_id": "079",
                "co2_ppm": 300.0,
                "co2_ratio_f": 1.0,
                "temp_c": 25.0,
                "pressure_kpa": 101.3,
            },
        ],
    )

    result = run_runtime_parity_audit(
        run_dir=str(run_dir),
        analyzer="GA03",
        actual_device_id="079",
        baseline_capture_path=str(capture_path),
    )

    assert result["parity_verdict"] == "parity_inconclusive_missing_runtime_inputs"
    candidate_rows = result["candidate_rows"]
    assert any(row["candidate_name"] == "ratio_f_plus_temperature" and row["candidate_status"] == "tested" for row in candidate_rows)
    assert any(row["candidate_name"] == "ratio_f_plus_senco7_chamber" and row["candidate_status"] == "insufficient_inputs" for row in candidate_rows)
