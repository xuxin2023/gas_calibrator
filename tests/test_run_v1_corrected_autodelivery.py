from __future__ import annotations

from pathlib import Path

import pandas as pd

from gas_calibrator.tools import run_v1_corrected_autodelivery as module


def test_extract_run_device_ids_uses_most_common_value(tmp_path: Path) -> None:
    run_dir = tmp_path / "run_1"
    run_dir.mkdir()
    frame = pd.DataFrame(
        [
            {"气体分析仪1_设备ID": "7", "气体分析仪2_设备ID": "021"},
            {"气体分析仪1_设备ID": "007", "气体分析仪2_设备ID": "021"},
            {"气体分析仪1_设备ID": "007", "气体分析仪2_设备ID": "21"},
        ]
    )
    frame.to_csv(run_dir / "samples_20260407.csv", index=False, encoding="utf-8-sig")

    mapping = module.extract_run_device_ids(run_dir)

    assert mapping == {"GA01": "007", "GA02": "021"}


def test_build_corrected_download_plan_rows_maps_groups_by_gas() -> None:
    simplified = pd.DataFrame(
        [
            {"分析仪": "GA01", "气体": "CO2", **{f"a{i}": float(i + 1) for i in range(9)}},
            {"分析仪": "GA01", "气体": "H2O", **{f"a{i}": float((i + 1) * 10) for i in range(9)}},
        ]
    )

    rows = module.build_corrected_download_plan_rows(simplified)

    assert len(rows) == 2
    co2 = next(row for row in rows if row["Gas"] == "CO2")
    h2o = next(row for row in rows if row["Gas"] == "H2O")
    assert co2["PrimarySENCO"] == "1"
    assert co2["SecondarySENCO"] == "3"
    assert co2["PrimaryCommand"].startswith("SENCO1,YGAS,FFF,1.00000e00,2.00000e00,3.00000e00,4.00000e00")
    assert co2["SecondaryCommand"].startswith("SENCO3,YGAS,FFF,5.00000e00,6.00000e00,7.00000e00,8.00000e00,9.00000e00")
    assert h2o["PrimarySENCO"] == "2"
    assert h2o["SecondarySENCO"] == "4"


def test_compute_pressure_offset_rows_uses_ambient_pressure_gauge_samples(tmp_path: Path) -> None:
    run_dir = tmp_path / "run_2"
    run_dir.mkdir()
    frame = pd.DataFrame(
        [
            {
                "压力执行模式": "ambient_open",
                "数字压力计压力hPa": 1019.5,
                "气体分析仪1_设备ID": "005",
                "气体分析仪1_分析仪压力kPa": 103.0,
                "气体分析仪1_分析仪可用帧": True,
            },
            {
                "压力执行模式": "ambient_open",
                "数字压力计压力hPa": 1019.7,
                "气体分析仪1_设备ID": "005",
                "气体分析仪1_分析仪压力kPa": 103.1,
                "气体分析仪1_分析仪可用帧": True,
            },
            {
                "压力执行模式": "sealed_controlled",
                "数字压力计压力hPa": 500.0,
                "气体分析仪1_设备ID": "005",
                "气体分析仪1_分析仪压力kPa": 50.0,
                "气体分析仪1_分析仪可用帧": True,
            },
        ]
    )
    frame.to_csv(run_dir / "samples_20260407.csv", index=False, encoding="utf-8-sig")

    rows = module.compute_pressure_offset_rows(run_dir)

    assert len(rows) == 1
    row = rows[0]
    assert row["Analyzer"] == "GA01"
    assert row["DeviceId"] == "005"
    assert row["Samples"] == 2
    assert abs(float(row["OffsetA_kPa"]) + 1.09) < 1e-9
    assert str(row["Command"]).startswith("SENCO9,YGAS,FFF,-1.09000e00,1.00000e00")
