from __future__ import annotations

from pathlib import Path

import pandas as pd
from openpyxl import Workbook, load_workbook

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

    rows = module.build_corrected_download_plan_rows(simplified, actual_device_ids={"GA01": "086"})

    assert len(rows) == 2
    co2 = next(row for row in rows if row["Gas"] == "CO2")
    h2o = next(row for row in rows if row["Gas"] == "H2O")
    assert co2["ActualDeviceId"] == "086"
    assert h2o["ActualDeviceId"] == "086"
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


def test_compute_pressure_offset_rows_marks_large_gauge_controller_gap_as_not_recommended(tmp_path: Path) -> None:
    run_dir = tmp_path / "run_2_backpressure"
    run_dir.mkdir()
    device_id_col = "气体分析仪1_设备ID"
    pressure_col = "气体分析仪1_分析仪压力kPa"
    usable_col = "气体分析仪1_分析仪可用帧"
    frame = pd.DataFrame(
        [
            {
                "PressureMode": "ambient_open",
                "pressure_gauge_hpa": 1019.0 + idx * 0.1,
                "pressure_controller_hpa": 1014.0 + idx * 0.1,
                device_id_col: "005",
                pressure_col: 103.0 + idx * 0.01,
                usable_col: True,
            }
            for idx in range(5)
        ]
    )
    frame.to_csv(run_dir / "samples_20260407.csv", index=False, encoding="utf-8-sig")

    rows = module.compute_pressure_offset_rows(run_dir, fallback_to_controller=True)

    assert len(rows) == 1
    row = rows[0]
    assert row["GaugeControllerOverlapSamples"] == 5
    assert abs(float(row["GaugeControllerMeanAbsDiff_hPa"]) - 5.0) < 1e-9
    assert abs(float(row["GaugeControllerMaxAbsDiff_hPa"]) - 5.0) < 1e-9
    assert row["PressureWriteRecommended"] is False
    assert row["PressureWriteReason"] == "ambient_open_backpressure_too_large"


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
    assert row["PressureWriteRecommended"] is True
    assert row["PressureWriteReason"] == ""
    assert str(row["Command"]).startswith("SENCO9,YGAS,FFF,-2.50000e00,1.00000e00")
    assert str(row["SourceSummary"]).endswith("startup_pressure_sensor_calibration_20260407_130000\\summary.csv")


def test_load_startup_pressure_calibration_rows_marks_unstable_reference_as_not_recommended(tmp_path: Path) -> None:
    run_dir = tmp_path / "run_3_unstable"
    target = run_dir / "startup_pressure_sensor_calibration_20260407_130000"
    target.mkdir(parents=True)
    pd.DataFrame(
        [
            {"Analyzer": "GA01", "DeviceId": "005", "Samples": 5, "OffsetA_kPa": -2.5, "WriteApplied": True, "ReadbackOk": True, "Status": "ok"},
        ]
    ).to_csv(target / "summary.csv", index=False, encoding="utf-8-sig")
    pd.DataFrame(
        [
            {"Analyzer": "GA01", "DeviceId": "005", "ReferenceHpa": 999.0, "ReferenceSource": "pressure_gauge", "AnalyzerPressureKPa": 99.9, "OffsetA_kPa": 0.0, "FrameOk": True},
            {"Analyzer": "GA01", "DeviceId": "005", "ReferenceHpa": 1002.2, "ReferenceSource": "pressure_gauge", "AnalyzerPressureKPa": 99.9, "OffsetA_kPa": 0.0, "FrameOk": True},
            {"Analyzer": "GA01", "DeviceId": "005", "ReferenceHpa": 1001.5, "ReferenceSource": "pressure_gauge", "AnalyzerPressureKPa": 99.9, "OffsetA_kPa": 0.0, "FrameOk": True},
        ]
    ).to_csv(target / "detail.csv", index=False, encoding="utf-8-sig")

    rows = module.load_startup_pressure_calibration_rows(run_dir)

    assert len(rows) == 1
    row = rows[0]
    assert row["StartupDetailSamples"] == 3
    assert row["StartupPressureGaugeSamples"] == 3
    assert abs(float(row["StartupReferenceSpanHpa"]) - 3.2) < 1e-9
    assert row["PressureWriteRecommended"] is False
    assert row["PressureWriteReason"] == "startup_pressure_reference_unstable"


def test_build_corrected_delivery_prefers_startup_pressure_rows(tmp_path: Path, monkeypatch) -> None:
    run_dir = tmp_path / "run_4"
    out_dir = tmp_path / "out_4"
    run_dir.mkdir()
    out_dir.mkdir()
    report_kwargs: dict[str, object] = {}
    appended_sheets: list[str] = []

    pd.DataFrame(
        [
            {
                "analyzer_id": "GA01",
                "analyzer_device_id": "005",
                "snapshot_time": "2026-04-12T10:00:00",
                "route_type": "co2",
                "ref_temp_c": 0.0,
                "cell_temp_raw_c": 60.0,
                "shell_temp_raw_c": -40.0,
                "cell_temp_span_c": 0.0,
                "shell_temp_span_c": 0.0,
                "valid_for_cell_fit": False,
                "valid_for_shell_fit": False,
                "cell_fit_gate_reason": "hard_bad_value",
                "shell_fit_gate_reason": "hard_bad_value",
            }
        ]
    ).to_csv(run_dir / "temperature_calibration_observations.csv", index=False, encoding="utf-8-sig")

    monkeypatch.setattr(module, "_filter_no_500_summary_paths", lambda *_args, **_kwargs: ([run_dir / "gas.csv", run_dir / "water.csv"], []))
    monkeypatch.setattr(
        module,
        "_append_dataframe_sheet",
        lambda _path, sheet_name, _frame: appended_sheets.append(str(sheet_name)),
    )
    def _fake_report(*_args, **_kwargs):
        report_kwargs.update(_kwargs)
        return {
            "summary": pd.DataFrame([{"鍒嗘瀽浠?": "GA01", "姘斾綋": "CO2"}]),
            "simplified": pd.DataFrame([{"鍒嗘瀽浠?": "GA01", "姘斾綋": "CO2", **{f"a{i}": float(i + 1) for i in range(9)}}]),
            "h2o_selected_rows": pd.DataFrame(
                [
                    {
                        "Analyzer": "GA01",
                        "PointRow": 3,
                        "PointTag": "co2_m20_0",
                        "SelectionOrigin": "co2_zero_ppm_anchor",
                        "EnvTempC": -20.0,
                    }
                ]
            ),
            "h2o_anchor_gate_hits": pd.DataFrame(
                [
                    {
                        "Analyzer": "GA01",
                        "PointRow": 12,
                        "PointTag": "co2_0_0",
                        "GateReason": "anchor_h2o_dew_above_limit",
                    }
                ]
            ),
        }

    monkeypatch.setattr(module, "build_corrected_water_points_report", _fake_report)
    monkeypatch.setattr(module, "extract_run_device_ids", lambda *_args, **_kwargs: {"GA01": "005"})
    monkeypatch.setattr(module, "load_temperature_coefficient_rows", lambda *_args, **_kwargs: [{"analyzer_id": "GA01", "senco_channel": "SENCO7", "A": 1, "B": 2, "C": 3, "D": 4}])
    monkeypatch.setattr(module, "load_startup_pressure_calibration_rows", lambda *_args, **_kwargs: [{"Analyzer": "GA01", "DeviceId": "005", "OffsetA_kPa": -2.5, "Command": "SENCO9,YGAS,FFF,-2.50000e00,1.00000e00,0.00000e00,0.00000e00"}])
    compute_calls = {"count": 0}

    def _fake_compute(*_args, **_kwargs):
        compute_calls["count"] += 1
        return [{"Analyzer": "GA01", "DeviceId": "005", "OffsetA_kPa": -9.9}]

    monkeypatch.setattr(module, "compute_pressure_offset_rows", _fake_compute)

    result = module.build_corrected_delivery(
        run_dir=run_dir,
        output_dir=out_dir,
        coeff_cfg={"h2o_summary_selection": {"include_co2_temp_groups_c": [], "include_co2_zero_ppm_temp_groups_c": [-20.0, -10.0, 0.0]}},
        pressure_row_source="startup_calibration",
    )

    assert compute_calls["count"] == 0
    assert result["pressure_rows"] == [{"Analyzer": "GA01", "DeviceId": "005", "OffsetA_kPa": -2.5, "Command": "SENCO9,YGAS,FFF,-2.50000e00,1.00000e00,0.00000e00,0.00000e00"}]
    assert report_kwargs["coeff_cfg"] == {"h2o_summary_selection": {"include_co2_temp_groups_c": [], "include_co2_zero_ppm_temp_groups_c": [-20.0, -10.0, 0.0]}}
    assert "H2O锚点入选" in appended_sheets
    assert "H2O锚点门禁" in appended_sheets
    assert "推荐运行结构提示" in appended_sheets
    assert "温补异常快照" in appended_sheets
    assert result["h2o_selected_rows"] == [{"Analyzer": "GA01", "PointRow": 3, "PointTag": "co2_m20_0", "SelectionOrigin": "co2_zero_ppm_anchor", "EnvTempC": -20.0, "ActualDeviceId": "005"}]
    assert result["h2o_anchor_gate_hits"] == [{"Analyzer": "GA01", "PointRow": 12, "PointTag": "co2_0_0", "GateReason": "anchor_h2o_dew_above_limit", "ActualDeviceId": "005"}]
    assert result["temperature_gate_hits"][0]["analyzer_id"] == "GA01"
    assert any(item["CheckCode"] == "pressure_structure" for item in result["run_structure_hints"])


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


def test_write_coefficients_to_live_devices_skips_unrecommended_pressure_rows(tmp_path: Path, monkeypatch) -> None:
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
            return None

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
            return True

    monkeypatch.setattr(module, "GasAnalyzer", _FakeGasAnalyzer)

    result = module.write_coefficients_to_live_devices(
        cfg={},
        output_dir=tmp_path / "write_out_skip_pressure",
        download_plan_rows=[],
        temperature_rows=[],
        pressure_rows=[
            {
                "Analyzer": "GA01",
                "OffsetA_kPa": -2.5,
                "PressureWriteRecommended": False,
                "PressureWriteReason": "ambient_open_backpressure_too_large",
            }
        ],
        actual_device_ids={"GA01": "005"},
        write_pressure_rows=True,
    )

    assert writes == []
    assert result["summary_rows"][0]["ExpectedGroups"] == 0
    assert result["detail_rows"][0]["Group"] == 9
    assert result["detail_rows"][0]["Error"] == "SKIPPED_PRESSURE_WRITE:ambient_open_backpressure_too_large"


def test_write_coefficients_to_live_devices_retries_transient_readback_failure(tmp_path: Path, monkeypatch) -> None:
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

    class _FakeGasAnalyzer:
        def __init__(self, *args, **kwargs) -> None:
            self.values = {9: [-2.5, 1.0, 0.0, 0.0]}
            self.read_attempts = {9: 0}

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
            self.values[int(group)] = [float(value) for value in coeffs]
            return True

        def read_coefficient_group(self, group: int):
            group = int(group)
            self.read_attempts[group] = self.read_attempts.get(group, 0) + 1
            if group == 9 and self.read_attempts[group] == 1:
                raise RuntimeError("GETCO9 read failed: NO_RESPONSE")
            values = self.values[group]
            return {f"C{idx}": float(value) for idx, value in enumerate(values)}

    monkeypatch.setattr(module, "GasAnalyzer", _FakeGasAnalyzer)

    result = module.write_coefficients_to_live_devices(
        cfg={},
        output_dir=tmp_path / "write_out_retry",
        download_plan_rows=[],
        temperature_rows=[],
        pressure_rows=[{"Analyzer": "GA01", "OffsetA_kPa": -2.5}],
        actual_device_ids={"GA01": "005"},
        write_pressure_rows=True,
    )

    assert result["summary_rows"][0]["MatchedGroups"] == 1
    assert result["detail_rows"][0]["Group"] == 9
    assert result["detail_rows"][0]["ReadbackOk"] is True


def test_annotate_workbook_with_actual_device_ids_inserts_column(tmp_path: Path) -> None:
    workbook_path = tmp_path / "report.xlsx"
    wb = Workbook()
    ws = wb.active
    ws.title = "summary"
    ws.append(["Analyzer", "Metric"])
    ws.append(["GA01", 1.23])
    ws.append(["GA02", 4.56])
    wb.save(workbook_path)
    wb.close()

    module._annotate_workbook_with_actual_device_ids(workbook_path, {"GA01": "086", "GA02": "008"})

    wb = load_workbook(workbook_path)
    try:
        ws = wb["summary"]
        assert ws.cell(1, 2).value == "ActualDeviceId"
        assert ws.cell(2, 2).value == "086"
        assert ws.cell(3, 2).value == "008"
    finally:
        wb.close()


def test_run_from_cli_runs_short_verify_after_successful_write(tmp_path: Path, monkeypatch) -> None:
    run_dir = tmp_path / "run_ok"
    run_dir.mkdir()
    cfg_path = run_dir / "runtime_config_snapshot.json"
    cfg_path.write_text("{}", encoding="utf-8")
    captured: dict[str, object] = {}

    monkeypatch.setattr(
        module,
        "build_corrected_delivery",
        lambda **_kwargs: {
            "report_path": str(run_dir / "calibration_coefficients.xlsx"),
            "output_dir": str(run_dir / "corrected"),
            "filtered_summary_paths": [],
            "filter_stats": [],
            "actual_device_ids": {"GA01": "086"},
            "download_plan_rows": [],
            "temperature_rows": [],
            "pressure_rows": [],
            "pressure_row_source": "startup_calibration",
        },
    )
    monkeypatch.setattr(module, "load_config", lambda _path: {"_base_dir": str(tmp_path)})
    monkeypatch.setattr(
        module,
        "write_coefficients_to_live_devices",
        lambda **_kwargs: {
            "scan_rows": [],
            "summary_rows": [{"Analyzer": "GA01", "Status": "ok"}],
            "detail_rows": [],
        },
    )

    from gas_calibrator.tools import verify_short_run as verify_module

    monkeypatch.setattr(
        verify_module,
        "run_short_verification",
        lambda **kwargs: captured.update(kwargs) or {"ok": True, "run_dir": str(tmp_path / "short_verify_run")},
    )

    result = module.run_from_cli(
        run_dir=str(run_dir),
        config_path=str(cfg_path),
        output_dir=str(tmp_path / "out"),
        write_devices=True,
        verify_short_run_cfg={
            "enabled": True,
            "temp_c": 20.0,
            "skip_co2_ppm": [500],
            "enable_connect_check": False,
            "points_excel": "configs/points_tiny_short_run_20c_even500.xlsx",
        },
    )

    assert captured["actual_device_ids"] == {"GA01": "086"}
    assert captured["temp_c"] == 20.0
    assert captured["skip_co2_ppm"] == [500]
    assert str(captured["points_excel_override"]).endswith("configs\\points_tiny_short_run_20c_even500.xlsx")
    assert result["short_verify_outputs"]["ok"] is True


def test_run_from_cli_skips_short_verify_when_writeback_incomplete(tmp_path: Path, monkeypatch) -> None:
    run_dir = tmp_path / "run_partial"
    run_dir.mkdir()
    cfg_path = run_dir / "runtime_config_snapshot.json"
    cfg_path.write_text("{}", encoding="utf-8")

    monkeypatch.setattr(
        module,
        "build_corrected_delivery",
        lambda **_kwargs: {
            "report_path": str(run_dir / "calibration_coefficients.xlsx"),
            "output_dir": str(run_dir / "corrected"),
            "filtered_summary_paths": [],
            "filter_stats": [],
            "actual_device_ids": {"GA01": "086"},
            "download_plan_rows": [],
            "temperature_rows": [],
            "pressure_rows": [],
            "pressure_row_source": "startup_calibration",
        },
    )
    monkeypatch.setattr(module, "load_config", lambda _path: {"_base_dir": str(tmp_path)})
    monkeypatch.setattr(
        module,
        "write_coefficients_to_live_devices",
        lambda **_kwargs: {
            "scan_rows": [],
            "summary_rows": [{"Analyzer": "GA01", "Status": "partial"}],
            "detail_rows": [],
        },
    )

    result = module.run_from_cli(
        run_dir=str(run_dir),
        config_path=str(cfg_path),
        output_dir=str(tmp_path / "out"),
        write_devices=True,
        verify_short_run_cfg={"enabled": True},
    )

    assert result["short_verify_outputs"]["skipped"] is True
    assert result["short_verify_outputs"]["reason"] == "writeback_incomplete"
