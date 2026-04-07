from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from gas_calibrator.tools import run_v1_no500_postprocess


def _write_summary_csv(path: Path, rows: list[dict[str, object]]) -> None:
    pd.DataFrame(rows).to_csv(path, index=False, encoding="utf-8-sig")


def test_filter_no_500_frame_removes_only_500hpa_rows() -> None:
    frame = pd.DataFrame(
        [
            {"Analyzer": "GA01", "PressureMode": "ambient_open", "PressureTargetLabel": "当前大气压", "PressureTarget": None},
            {"Analyzer": "GA01", "PressureMode": "sealed_controlled", "PressureTargetLabel": "500hPa", "PressureTarget": 500.0},
            {"Analyzer": "GA01", "PressureMode": "sealed_controlled", "PressureTargetLabel": "700hPa", "PressureTarget": 700.0},
        ]
    )

    filtered, stats = run_v1_no500_postprocess._filter_no_500_frame(frame)

    assert list(filtered["PressureTargetLabel"]) == ["当前大气压", "700hPa"]
    assert stats == {"original_rows": 3, "removed_rows": 1, "kept_rows": 2}


def test_run_from_cli_filters_inputs_and_calls_postprocess_runner(
    tmp_path: Path,
    monkeypatch,
) -> None:
    gas = tmp_path / "gas.csv"
    water = tmp_path / "water.csv"
    _write_summary_csv(
        gas,
        [
            {"Analyzer": "GA01", "PointPhase": "气路", "PressureMode": "ambient_open", "PressureTargetLabel": "当前大气压", "PressureTarget": None, "Temp": 20.0, "BAR": 101.0, "ppm_CO2_Tank": 400.0, "R_CO2": 1.2},
            {"Analyzer": "GA01", "PointPhase": "气路", "PressureMode": "sealed_controlled", "PressureTargetLabel": "500hPa", "PressureTarget": 500.0, "Temp": 20.0, "BAR": 50.0, "ppm_CO2_Tank": 400.0, "R_CO2": 1.2},
        ],
    )
    _write_summary_csv(
        water,
        [
            {"Analyzer": "GA01", "PointPhase": "水路", "PressureMode": "ambient_open", "PressureTargetLabel": "当前大气压", "PressureTarget": None, "Temp": 20.0, "BAR": 101.0, "ppm_H2O_Dew": 10.0, "R_H2O": 0.5},
            {"Analyzer": "GA01", "PointPhase": "水路", "PressureMode": "sealed_controlled", "PressureTargetLabel": "500hPa", "PressureTarget": 500.0, "Temp": 20.0, "BAR": 50.0, "ppm_H2O_Dew": 10.0, "R_H2O": 0.5},
        ],
    )

    captured: dict[str, object] = {}

    def _fake_postprocess_runner(**kwargs):
        captured.update(kwargs)
        output_dir = Path(str(kwargs["output_dir"]))
        (output_dir / "calibration_coefficients.xlsx").write_text("stub", encoding="utf-8")
        (output_dir / "calibration_coefficients_postprocess_summary.json").write_text("{}", encoding="utf-8")
        return {"status": "completed", "path": str(output_dir / "calibration_coefficients.xlsx")}

    monkeypatch.setattr(run_v1_no500_postprocess.v1_postprocess_runner, "run_from_cli", _fake_postprocess_runner)

    output_dir = tmp_path / "out"
    exported = run_v1_no500_postprocess.run_from_cli(
        summary_paths=[str(gas), str(water)],
        output_dir=str(output_dir),
    )

    assert captured["skip_qc"] is True
    assert captured["skip_refit"] is True
    assert captured["skip_ai"] is True
    assert captured["skip_analytics"] is True
    assert captured["skip_measurement_analytics"] is True
    assert captured["download"] is False
    assert captured["import_db"] is False
    filtered_paths = [Path(item) for item in captured["summary_paths"]]
    assert len(filtered_paths) == 2
    for path in filtered_paths:
        frame = pd.read_csv(path)
        assert "500hPa" not in set(frame["PressureTargetLabel"].fillna(""))

    summary_payload = json.loads((output_dir / "no_500_filter_summary.json").read_text(encoding="utf-8"))
    assert len(summary_payload) == 2
    assert Path(exported["output_dir"]) == output_dir.resolve()
