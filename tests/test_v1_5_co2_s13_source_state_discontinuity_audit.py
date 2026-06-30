from __future__ import annotations

import csv
from pathlib import Path

from gas_calibrator.validation.co2_s13_source_state_discontinuity_audit import (
    build_co2_s13_source_state_discontinuity_audit,
)


def _write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    keys: list[str] = []
    for row in rows:
        for key in row:
            if key not in keys:
                keys.append(key)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=keys)
        writer.writeheader()
        writer.writerows(rows)


def test_source_state_audit_flags_mixed_sources_and_sawtooth_bias(tmp_path: Path) -> None:
    fit_rows: list[dict[str, object]] = []
    residual_rows: list[dict[str, object]] = []
    for device in ("001", "002", "003"):
        for target, source, pressure, error in (
            (100, "main_run", 1001.0, -5.0),
            (200, "supplement_run", 1010.0, 6.0),
            (300, "main_run", 1002.0, -5.5),
        ):
            point = f"T20_{target}ppm"
            fit_rows.append(
                {
                    "analyzer_device_id": device,
                    "point_identity": point,
                    "source_label": source,
                    "temp_set_c": 20.0,
                    "temperature_c": 20.1,
                    "thermometer_temp_mean_c": 20.1,
                    "analyzer_chamber_temp_mean_c": 20.0,
                    "pressure_hpa": pressure,
                    "dewpoint_mean_c": -35.0,
                    "co2_ratio_f_std": 0.00004,
                    "h2o_mmol_mean": 0.4,
                    "sample_count": 10,
                    "usable_sample_count": 10,
                }
            )
            residual_rows.append(
                {
                    "device_id": device,
                    "structure_id": "current_writable_senco13",
                    "objective_id": "low_end_priority_lstsq",
                    "zero_offset_ppm": 0.0,
                    "point_identity": point,
                    "certificate_target_ppm": float(target),
                    "target_ppm_for_fit": float(target),
                    "prediction_ppm": float(target) + error,
                    "error_ppm": error,
                    "relative_error_percent": 100.0 * error / float(target),
                    "ratio": 1.0 - target * 0.001,
                    "temperature_c": 20.1,
                }
            )
    summary_rows = [
        {
            "device_id": device,
            "structure_id": "current_writable_senco13",
            "diagnostic_only": "False",
            "objective_id": "low_end_priority_lstsq",
            "zero_offset_ppm": 0.0,
            "max_abs_relative_error_percent": 5.0,
            "rmse_ppm": 5.0,
        }
        for device in ("001", "002", "003")
    ]

    fit_path = tmp_path / "fit.csv"
    residual_path = tmp_path / "residuals.csv"
    summary_path = tmp_path / "summary.csv"
    _write_csv(fit_path, fit_rows)
    _write_csv(residual_path, residual_rows)
    _write_csv(summary_path, summary_rows)

    tables = build_co2_s13_source_state_discontinuity_audit(
        fit_points_csv=fit_path,
        enhanced_summary_csv=summary_path,
        enhanced_residuals_csv=residual_path,
    )

    partition = tables["temperature_partition_audit"][0]
    assert partition["temperature_group"] == "T20"
    assert "mixed_source_temperature_group" in partition["partition_flags"]
    assert "point_pressure_outlier" in partition["partition_flags"]

    sawtooth = tables["sawtooth_bias_audit"][0]
    assert sawtooth["sign_change_count"] == 2
    assert "non_affine_sawtooth_bias" in sawtooth["sawtooth_flags"]

    decision_text = "\n".join(row["topic"] for row in tables["root_cause_decision"])
    assert "当前可写 S1/S3 不能直接放行" in decision_text
    assert tables["run_summary"][0]["opens_com_ports"] is False
    assert tables["run_summary"][0]["controls_water_or_gas_routes"] is False
    assert tables["run_summary"][0]["writes_coefficients"] is False
    assert tables["run_summary"][0]["candidate_write_allowed"] is False
    assert tables["run_summary"][0]["write_gate_status"] == "blocked_source_state_discontinuity"
    assert tables["run_summary"][0]["write_gate_blocker_count"] >= 1
