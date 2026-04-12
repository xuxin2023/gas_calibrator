from __future__ import annotations

import csv
from pathlib import Path

from gas_calibrator.calibration.temperature_compensation_fit import (
    fit_temperature_compensation,
    format_senco_coeffs,
)
from gas_calibrator.logging_utils import RunLogger
from gas_calibrator.workflow.runner import CalibrationRunner
from gas_calibrator.data.points import CalibrationPoint


def _point() -> CalibrationPoint:
    return CalibrationPoint(
        index=1,
        temp_chamber_c=20.0,
        co2_ppm=400.0,
        hgen_temp_c=None,
        hgen_rh_pct=None,
        target_pressure_hpa=1000.0,
        dewpoint_c=None,
        h2o_mmol=None,
        raw_h2o=None,
    )


def test_fit_temperature_compensation_downgrades_safely() -> None:
    result_two_points = fit_temperature_compensation([10.0, 20.0], [11.0, 21.0])
    assert result_two_points["fit_ok"] is True
    assert result_two_points["polynomial_degree_used"] == 1
    assert result_two_points["C"] == 0.0
    assert result_two_points["D"] == 0.0

    result_one_point = fit_temperature_compensation([10.0], [11.0])
    assert result_one_point["fit_ok"] is False
    assert result_one_point["A"] == 0.0
    assert result_one_point["B"] == 1.0
    assert result_one_point["C"] == 0.0
    assert result_one_point["D"] == 0.0


def test_format_senco_coeffs_uses_scientific_notation() -> None:
    formatted = format_senco_coeffs((0.0, 1.0, -0.15, 2.5e-6))
    assert formatted == ("0.00000e00", "1.00000e00", "-1.50000e-01", "2.50000e-06")


class _FakeAnalyzer:
    def __init__(self, *, device_id: str, cell_temp_c: float, shell_temp_c: float | None) -> None:
        self._device_id = device_id
        self._cell_temp_c = cell_temp_c
        self._shell_temp_c = shell_temp_c

    def read_latest_data(self, allow_passive_fallback: bool = False) -> str:
        shell_text = "" if self._shell_temp_c is None else f"{self._shell_temp_c:.2f}"
        return (
            f"YGAS,{self._device_id},400.0,5.0,1.0,1.0,1.0,1.0,1.0,1.0,"
            f"100.0,100.0,100.0,{self._cell_temp_c:.2f},{shell_text},101.30"
        )

    def parse_line_mode2(self, line: str) -> dict:
        return {
            "raw": line,
            "id": self._device_id,
            "mode": 2,
            "mode2_field_count": 16,
            "co2_ppm": 400.0,
            "h2o_mmol": 5.0,
            "co2_density": 1.0,
            "h2o_density": 1.0,
            "co2_ratio_f": 1.0,
            "co2_ratio_raw": 1.0,
            "h2o_ratio_f": 1.0,
            "h2o_ratio_raw": 1.0,
            "ref_signal": 100.0,
            "co2_signal": 100.0,
            "h2o_signal": 100.0,
            "chamber_temp_c": self._cell_temp_c,
            "case_temp_c": self._shell_temp_c,
            "pressure_kpa": 101.3,
        }

    def close(self) -> None:
        return None


class _FakeThermometer:
    def __init__(self, temp_c: float) -> None:
        self._temp_c = temp_c

    def read_temp_c(self) -> float:
        return self._temp_c

    def close(self) -> None:
        return None


class _FakeChamber:
    def __init__(self, temp_c: float) -> None:
        self._temp_c = temp_c

    def read_temp_c(self) -> float:
        return self._temp_c

    def close(self) -> None:
        return None


def test_runner_temperature_calibration_snapshot_and_export(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    cfg = {
        "temperature_calibration": {
            "enabled": True,
            "snapshot_window_s": 0,
            "min_ref_samples": 1,
            "env_stable_span_c": 1.0,
            "box_stable_span_c": 1.0,
            "export_commands": True,
            "polynomial_order": 3,
        }
    }
    devices = {
        "gas_analyzer": _FakeAnalyzer(device_id="001", cell_temp_c=20.10, shell_temp_c=20.80),
        "thermometer": _FakeThermometer(20.55),
        "temp_chamber": _FakeChamber(20.00),
    }
    messages: list[str] = []
    runner = CalibrationRunner(cfg, devices, logger, messages.append, lambda *_: None)

    ok = runner._capture_temperature_calibration_snapshot(_point(), route_type="co2")
    assert ok is True
    assert len(runner._temperature_calibration_records) == 1
    record = runner._temperature_calibration_records[0]
    assert record["analyzer_id"] == "GA01"
    assert record["ref_temp_source"] == "env"
    assert record["valid_for_cell_fit"] is True
    assert record["valid_for_shell_fit"] is True

    runner._finalize_temperature_calibration_outputs()
    logger.close()

    observation_csv = logger.run_dir / "temperature_calibration_observations.csv"
    result_csv = logger.run_dir / "temperature_compensation_coefficients.csv"
    commands_txt = logger.run_dir / "temperature_compensation_commands.txt"
    workbook = logger.run_dir / "temperature_compensation.xlsx"
    assert observation_csv.exists()
    assert result_csv.exists()
    assert commands_txt.exists()
    assert workbook.exists()

    with result_csv.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    assert len(rows) == 2
    cell_row = next(row for row in rows if row["fit_type"] == "cell")
    shell_row = next(row for row in rows if row["fit_type"] == "shell")
    assert cell_row["senco_channel"] == "SENCO7"
    assert shell_row["senco_channel"] == "SENCO8"
    assert cell_row["command_string"] == "SENCO7,YGAS,FFF,0.00000e00,1.00000e00,0.00000e00,0.00000e00"
    assert shell_row["command_string"] == "SENCO8,YGAS,FFF,0.00000e00,1.00000e00,0.00000e00,0.00000e00"


def test_runner_temperature_calibration_shell_missing_downgrades(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    cfg = {
        "temperature_calibration": {
            "enabled": True,
            "snapshot_window_s": 0,
            "min_ref_samples": 1,
            "env_stable_span_c": 1.0,
            "box_stable_span_c": 1.0,
        }
    }
    devices = {
        "gas_analyzer": _FakeAnalyzer(device_id="001", cell_temp_c=20.10, shell_temp_c=None),
        "thermometer": _FakeThermometer(20.55),
        "temp_chamber": _FakeChamber(20.00),
    }
    runner = CalibrationRunner(cfg, devices, logger, lambda *_: None, lambda *_: None)

    runner._capture_temperature_calibration_snapshot(_point(), route_type="co2")
    assert runner._temperature_calibration_records[0]["valid_for_shell_fit"] is False
    assert runner._temperature_calibration_records[0]["shell_fit_gate_reason"] == "missing_shell_temp"
    runner._finalize_temperature_calibration_outputs()
    logger.close()

    result_csv = logger.run_dir / "temperature_compensation_coefficients.csv"
    with result_csv.open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    shell_row = next(row for row in rows if row["fit_type"] == "shell")
    assert shell_row["availability"] == "unavailable"
    assert shell_row["command_string"] == ""


def test_runner_temperature_calibration_rejects_implausible_internal_snapshot(tmp_path: Path) -> None:
    logger = RunLogger(tmp_path)
    cfg = {
        "temperature_calibration": {
            "enabled": True,
            "snapshot_window_s": 0,
            "min_ref_samples": 1,
            "env_stable_span_c": 1.0,
            "box_stable_span_c": 1.0,
            "plausibility": {
                "enabled": True,
                "raw_temp_min_c": -30.0,
                "raw_temp_max_c": 85.0,
                "max_abs_delta_from_ref_c": 15.0,
                "max_cell_shell_gap_c": 12.0,
                "hard_bad_values_c": [-40.0, 60.0],
                "hard_bad_value_tolerance_c": 0.05,
            },
        }
    }
    devices = {
        "gas_analyzer": _FakeAnalyzer(device_id="001", cell_temp_c=60.0, shell_temp_c=-40.0),
        "thermometer": _FakeThermometer(0.55),
        "temp_chamber": _FakeChamber(0.00),
    }
    runner = CalibrationRunner(cfg, devices, logger, lambda *_: None, lambda *_: None)

    ok = runner._capture_temperature_calibration_snapshot(_point(), route_type="co2")
    assert ok is True
    assert len(runner._temperature_calibration_records) == 1
    record = runner._temperature_calibration_records[0]
    assert record["valid_for_cell_fit"] is False
    assert record["valid_for_shell_fit"] is False
    assert record["cell_fit_gate_reason"] == "hard_bad_value"
    assert record["shell_fit_gate_reason"] == "hard_bad_value"

    runner._finalize_temperature_calibration_outputs()
    logger.close()

    with (logger.run_dir / "temperature_compensation_coefficients.csv").open("r", encoding="utf-8", newline="") as handle:
        rows = list(csv.DictReader(handle))
    assert all(row["availability"] == "unavailable" for row in rows)
