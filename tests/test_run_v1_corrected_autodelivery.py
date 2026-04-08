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


def test_load_startup_pressure_calibration_rows_uses_latest_summary(tmp_path: Path) -> None:
    run_dir = tmp_path / "run_3"
    older = run_dir / "startup_pressure_sensor_calibration_20260407_120000"
    newer = run_dir / "startup_pressure_sensor_calibration_20260407_130000"
    older.mkdir(parents=True)
    newer.mkdir(parents=True)
    pd.DataFrame(
        [
            {"Analyzer": "GA01", "DeviceId": "005", "Samples": 4, "OffsetA_kPa": -1.0, "WriteApplied": True, "ReadbackOk": True, "Status": "ok"},
        ]
    ).to_csv(older / "summary.csv", index=False, encoding="utf-8-sig")
    pd.DataFrame(
        [
            {"Analyzer": "GA01", "DeviceId": "005", "Samples": 5, "OffsetA_kPa": -2.5, "WriteApplied": True, "ReadbackOk": True, "Status": "ok"},
        ]
    ).to_csv(newer / "summary.csv", index=False, encoding="utf-8-sig")

    rows = module.load_startup_pressure_calibration_rows(run_dir)

    assert len(rows) == 1
    row = rows[0]
    assert row["Analyzer"] == "GA01"
    assert row["DeviceId"] == "005"
    assert row["Samples"] == 5
    assert abs(float(row["OffsetA_kPa"]) + 2.5) < 1e-9
    assert row["ReferenceSource"] == "startup_pressure_sensor_calibration"
    assert str(row["Command"]).startswith("SENCO9,YGAS,FFF,-2.50000e00,1.00000e00")
    assert str(row["SourceSummary"]).endswith("startup_pressure_sensor_calibration_20260407_130000\\summary.csv")


def test_build_corrected_delivery_prefers_startup_pressure_rows(tmp_path: Path, monkeypatch) -> None:
    run_dir = tmp_path / "run_4"
    out_dir = tmp_path / "out_4"
    run_dir.mkdir()
    out_dir.mkdir()

    monkeypatch.setattr(module, "_filter_no_500_summary_paths", lambda *_args, **_kwargs: ([run_dir / "gas.csv", run_dir / "water.csv"], []))
    monkeypatch.setattr(module, "_append_dataframe_sheet", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        module,
        "build_corrected_water_points_report",
        lambda *_args, **_kwargs: {
            "summary": pd.DataFrame([{"鍒嗘瀽浠?": "GA01", "姘斾綋": "CO2"}]),
            "simplified": pd.DataFrame([{"鍒嗘瀽浠?": "GA01", "姘斾綋": "CO2", **{f"a{i}": float(i + 1) for i in range(9)}}]),
        },
    )
    monkeypatch.setattr(module, "extract_run_device_ids", lambda *_args, **_kwargs: {"GA01": "005"})
    monkeypatch.setattr(module, "load_temperature_coefficient_rows", lambda *_args, **_kwargs: [{"analyzer_id": "GA01", "senco_channel": "SENCO7", "A": 1, "B": 2, "C": 3, "D": 4}])
    monkeypatch.setattr(module, "load_startup_pressure_calibration_rows", lambda *_args, **_kwargs: [{"Analyzer": "GA01", "DeviceId": "005", "OffsetA_kPa": -2.5, "Command": "SENCO9,YGAS,FFF,-2.50000e00,1.00000e00,0.00000e00,0.00000e00"}])
    compute_calls = {"count": 0}

    def _fake_compute(*_args, **_kwargs):
        compute_calls["count"] += 1
        return [{"Analyzer": "GA01", "DeviceId": "005", "OffsetA_kPa": -9.9}]

    monkeypatch.setattr(module, "compute_pressure_offset_rows", _fake_compute)

    result = module.build_corrected_delivery(run_dir=run_dir, output_dir=out_dir, pressure_row_source="startup_calibration")

    assert compute_calls["count"] == 0
    assert result["pressure_rows"] == [{"Analyzer": "GA01", "DeviceId": "005", "OffsetA_kPa": -2.5, "Command": "SENCO9,YGAS,FFF,-2.50000e00,1.00000e00,0.00000e00,0.00000e00"}]


def test_write_coefficients_to_live_devices_can_skip_pressure_rows(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setattr(
        module,
        "scan_live_targets",
        lambda *_args, **_kwargs: [
            {
                "Analyzer": "GA01",
                "Port": "COM35",
                "Baudrate": 115200,
                "Timeout": 0.6,
                "ConfiguredDeviceId": "005",
                "LiveDeviceId": "005",
                "ActiveSend": True,
                "FtdHz": 10,
                "AverageFilter": 49,
            }
        ],
    )

    writes: list[int] = []

    class _FakeGasAnalyzer:
        def __init__(self, *args, **kwargs) -> None:
            self.values = {1: [1.0] * 6, 7: [7.0] * 4, 8: [8.0] * 4}

        def open(self) -> None:
            return None

        def close(self) -> None:
            return None

        def set_comm_way_with_ack(self, *args, **kwargs) -> bool:
            return True

        def set_mode_with_ack(self, *args, **kwargs) -> bool:
            return True

        def set_active_freq_with_ack(self, *args, **kwargs) -> bool:
            return True

        def set_average_filter_with_ack(self, *args, **kwargs) -> bool:
            return True

        def set_senco(self, group: int, coeffs) -> bool:
            writes.append(int(group))
            self.values[int(group)] = [float(value) for value in coeffs]
            return True

        def read_coefficient_group(self, group: int):
            values = self.values[int(group)]
            return {f"C{idx}": float(value) for idx, value in enumerate(values)}

    monkeypatch.setattr(module, "GasAnalyzer", _FakeGasAnalyzer)

    result = module.write_coefficients_to_live_devices(
        cfg={},
        output_dir=tmp_path / "write_out",
        download_plan_rows=[{"Analyzer": "GA01", "PrimaryCommand": "SENCO1,YGAS,FFF,1,1,1,1,0,0", "SecondaryCommand": ""}],
        temperature_rows=[{"analyzer_id": "GA01", "senco_channel": "SENCO7", "A": 1, "B": 2, "C": 3, "D": 4}],
        pressure_rows=[{"Analyzer": "GA01", "OffsetA_kPa": -2.5}],
        actual_device_ids={"GA01": "005"},
        write_pressure_rows=False,
    )

    assert 9 not in writes
    assert result["summary_rows"][0]["MatchedGroups"] == 2
