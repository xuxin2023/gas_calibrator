import csv
import json
import os
from pathlib import Path
import subprocess
import sys

from gas_calibrator.tools import (
    run_prevalidation_no_sources,
    run_v1_merged_calibration_sidecar,
    validate_dry_collect,
    validate_offline_run,
    validate_pressure_only,
    verify_coefficient_roundtrip,
)
from openpyxl import Workbook


def _write_json(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _base_cfg(output_dir: Path) -> dict:
    return {
        "paths": {
            "output_dir": str(output_dir),
            "points_excel": "points.xlsx",
        },
        "devices": {
            "gas_analyzer": {"enabled": False, "active_send": False, "ftd_hz": 10, "average_co2": 1, "average_h2o": 1},
            "gas_analyzers": [
                {"enabled": True, "name": "GA01", "port": "COM1", "baud": 115200, "device_id": "010", "active_send": False, "ftd_hz": 10, "average_co2": 1, "average_h2o": 1},
            ],
            "pressure_controller": {"enabled": False, "port": "COM2", "baud": 9600, "in_limits_pct": 0.02, "in_limits_time_s": 10},
            "pressure_gauge": {"enabled": False, "port": "COM3", "baud": 9600, "dest_id": "001"},
            "humidity_generator": {"enabled": False, "port": "COM4", "baud": 9600},
            "dewpoint_meter": {"enabled": False, "port": "COM5", "baud": 9600, "station": 1},
            "temperature_chamber": {"enabled": False, "port": "COM6", "baud": 9600, "addr": 1},
            "thermometer": {"enabled": False, "port": "COM7", "baud": 9600},
            "relay": {"enabled": False, "port": "COM8", "baud": 9600, "addr": 1},
            "relay_8": {"enabled": False, "port": "COM9", "baud": 9600, "addr": 1},
        },
        "workflow": {
            "sampling": {
                "count": 3,
                "stable_count": 3,
                "interval_s": 0.0,
                "quality": {"enabled": False},
            }
        },
        "coefficients": {
            "model": "ratio_poly_rt_p",
            "summary_columns": {
                "co2": {"target": "ppm_CO2_Tank", "ratio": "R_CO2", "temperature": "T1", "pressure": "BAR", "pressure_scale": 1.0},
                "h2o": {"target": "ppm_H2O_Dew", "ratio": "R_H2O", "temperature": "T1", "pressure": "BAR", "pressure_scale": 1.0},
            },
            "ratio_poly_fit": {"pressure_source_preference": "reference_first"},
            "fit_h2o": True,
            "save_residuals": False,
            "min_samples": 0,
            "enabled": False,
        },
    }


def _write_csv(path: Path, rows: list[dict]) -> None:
    header = []
    for row in rows:
        for key in row.keys():
            if key not in header:
                header.append(key)
    with path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=header)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _offline_samples() -> list[dict]:
    rows = []
    for point_row, ratio, target, pressure in [
        (1, 1.00, 400.0, 1000.0),
        (2, 1.10, 500.0, 1005.0),
        (3, 1.20, 600.0, 1010.0),
    ]:
        for idx in range(3):
            rows.append(
                {
                    "point_row": point_row,
                    "point_phase": "co2",
                    "point_tag": f"demo_{point_row}",
                    "point_title": f"demo_{point_row}",
                    "pressure_target_hpa": pressure,
                    "co2_ppm_target": target,
                    "thermometer_temp_c": 20.0 + idx * 0.1,
                    "dewpoint_c": 1.0 + idx * 0.1,
                    "dew_pressure_hpa": pressure,
                    "pressure_gauge_hpa": pressure,
                    "ga01_device_id": "010",
                    "ga01_mode2_field_count": 16,
                    "ga01_status": "OK",
                    "ga01_co2_ppm": target + 0.5,
                    "ga01_h2o_mmol": 1.0,
                    "ga01_co2_ratio_f": ratio + idx * 0.001,
                    "ga01_h2o_ratio_f": 0.2 + idx * 0.001,
                    "ga01_pressure_kpa": 101.0 + idx * 0.01,
                }
            )
    return rows


class _FakeGasAnalyzer:
    def __init__(self):
        self.calls = []
        self._counter = 0

    def set_mode(self, mode):
        self.calls.append(("mode", mode))
        return True

    def set_mode_with_ack(self, mode, require_ack=True):
        self.calls.append(("mode", mode, require_ack))
        return True

    def set_comm_way(self, active):
        self.calls.append(("active", active))
        return True

    def set_comm_way_with_ack(self, active, require_ack=True):
        self.calls.append(("active", active, require_ack))
        return True

    def set_average_filter(self, window_n):
        self.calls.append(("avg_filter", window_n))
        return True

    def set_average_filter_with_ack(self, window_n, require_ack=True):
        self.calls.append(("avg_filter", window_n, require_ack))
        return True

    def read_latest_data(self, *args, **kwargs):
        self._counter += 1
        return "YGAS,010,0400.0,01.0,0003.0,00.5,1.0000,1.0001,0.2000,0.2001,04000,05000,02000,020.00,021.00,101.00,OK"

    def parse_line_mode2(self, _line):
        self._counter += 1
        return {
            "mode2_field_count": 17,
            "status": "OK",
            "co2_ppm": 400.0 + self._counter * 0.1,
            "h2o_mmol": 1.0 + self._counter * 0.01,
            "co2_ratio_f": 1.0 + self._counter * 0.001,
            "h2o_ratio_f": 0.2 + self._counter * 0.001,
            "pressure_kpa": 101.0 + self._counter * 0.01,
            "chamber_temp_c": 20.0,
            "case_temp_c": 21.0,
            "ref_signal": 4000.0,
            "co2_signal": 5000.0,
            "h2o_signal": 2000.0,
            "id": "010",
            "mode": 2,
        }

    def close(self):
        return None


class _FakePressureGauge:
    def read_pressure(self):
        return 1000.5

    def close(self):
        return None


class _FakeRoundtripAnalyzer:
    def __init__(self, *args, **kwargs):
        self.groups = {
            1: {"C0": 1.0, "C1": 2.0},
            2: {"C0": 3.0, "C1": 4.0},
            3: {"C0": 5.0, "C1": 6.0},
            4: {"C0": 7.0, "C1": 8.0},
        }
        self.calls = []

    def open(self):
        return None

    def close(self):
        return None

    def set_mode(self, mode):
        self.calls.append(("mode", mode))
        return True

    def set_senco(self, index, *coefficients):
        self.calls.append(("set_senco", index, list(coefficients)))
        self.groups[int(index)] = {f"C{i}": float(value) for i, value in enumerate(coefficients)}
        return True

    def read_coefficient_group(self, index, **kwargs):
        return dict(self.groups[int(index)])


def _build_points_workbook(path: Path) -> None:
    wb = Workbook()
    ws = wb.active
    ws.cell(row=1, column=1, value="温度")
    ws.cell(row=2, column=1, value="温度")
    rows = [
        (-20.0, 0.0, None, 1100.0, None),
        (0.0, None, "0℃（湿度发生器） 50%（湿度发生器） -9.16℃（露点温度） 3.0233mmol/mol（气体分析仪）", 1100.0, None),
        (20.0, None, "20℃（湿度发生器） 70%（湿度发生器） 14.36℃（露点温度） 16.3715mmol/mol（气体分析仪）", 500.0, None),
        (30.0, 200.0, None, 1000.0, None),
        (30.0, 500.0, None, 800.0, "B"),
        (30.0, 800.0, None, 700.0, None),
        (40.0, 1000.0, None, 500.0, None),
    ]
    excel_row = 3
    for temp, co2, h2o, pressure, group in rows:
        ws.cell(row=excel_row, column=1, value=temp)
        ws.cell(row=excel_row, column=2, value=co2)
        ws.cell(row=excel_row, column=3, value=h2o)
        ws.cell(row=excel_row, column=4, value=pressure)
        ws.cell(row=excel_row, column=5, value=group)
        excel_row += 1
    wb.save(path)
    wb.close()


def test_validate_offline_run_generates_expected_tables(tmp_path: Path) -> None:
    run_dir = tmp_path / "run_demo"
    run_dir.mkdir()
    cfg = _base_cfg(tmp_path / "logs")
    (run_dir / "runtime_config_snapshot.json").write_text(json.dumps(cfg, ensure_ascii=False), encoding="utf-8")
    _write_csv(run_dir / "samples_demo.csv", _offline_samples())

    assert validate_offline_run.main(["--run-dir", str(run_dir), "--output-dir", str(tmp_path / "out")]) == 0

    out_dir = tmp_path / "out"
    assert (out_dir / "frame_quality_summary.csv").exists()
    assert (out_dir / "pressure_source_check.csv").exists()
    assert (out_dir / "fit_input_overview.csv").exists()


def test_merged_sidecar_merge_keeps_gas_and_water_separate() -> None:
    older = Path("D:/old")
    newer = Path("D:/new")
    point_rows = {
        str(older): [
            {"流程阶段": "co2", "温箱目标温度C": 20, "目标二氧化碳浓度ppm": 400, "目标压力hPa": 1000, "来源": "older-gas"},
            {"流程阶段": "h2o", "温箱目标温度C": 20, "湿度发生器目标温度C": 20, "湿度发生器目标湿度%": 70, "目标压力hPa": 500, "来源": "older-water"},
        ],
        str(newer): [
            {"流程阶段": "co2", "温箱目标温度C": 20, "目标二氧化碳浓度ppm": 400, "目标压力hPa": 1000, "来源": "newer-gas"},
        ],
    }

    merged_rows, selected_sources = run_v1_merged_calibration_sidecar._merge_point_rows(
        [older, newer],
        point_rows,
        allowed_gas_ppm=run_v1_merged_calibration_sidecar.DEFAULT_GAS_PPM,
    )

    assert len(merged_rows) == 2
    assert any(row["来源"] == "newer-gas" for row in merged_rows)
    assert any(row["来源"] == "older-water" for row in merged_rows)
    water_key = run_v1_merged_calibration_sidecar._point_identity_from_row(
        {"流程阶段": "h2o", "温箱目标温度C": 20, "湿度发生器目标温度C": 20, "湿度发生器目标湿度%": 70, "目标压力hPa": 500}
    )
    assert selected_sources[water_key] == str(older)


def test_merged_sidecar_builds_verify_subset_points_workbook(tmp_path: Path) -> None:
    source = tmp_path / "points.xlsx"
    target = tmp_path / "verify_points.xlsx"
    _build_points_workbook(source)

    info = run_v1_merged_calibration_sidecar._build_verify_points_workbook(source, target)

    assert target.exists()
    assert info["point_count"] == 5
    points = run_v1_merged_calibration_sidecar._build_verify_point_rows_from_workbook(target)
    assert len(points) == 5
    assert sum(1 for row in points if row["流程阶段"] == "co2") == 3
    assert sum(1 for row in points if row["流程阶段"] == "h2o") == 2


def test_merged_sidecar_defaults_to_standalone_non_write_mode() -> None:
    args = run_v1_merged_calibration_sidecar._parse_args(["--run-dir", "D:/completed_run"])

    assert args.write_temperature is False
    assert args.write_gas is False
    assert args.run_verify is False


def test_merged_sidecar_non_write_path_survives_without_sqlalchemy() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    src_root = repo_root / "src"
    script = """
import builtins
import json
import tempfile
from pathlib import Path

from openpyxl import Workbook

real_import = builtins.__import__

def fake_import(name, globals=None, locals=None, fromlist=(), level=0):
    if name == "sqlalchemy" or name.startswith("sqlalchemy."):
        raise ModuleNotFoundError("No module named 'sqlalchemy'")
    return real_import(name, globals, locals, fromlist, level)

builtins.__import__ = fake_import

try:
    import gas_calibrator.tools.run_v1_merged_calibration_sidecar as sidecar

    tmp = Path(tempfile.mkdtemp(prefix="merged_sidecar_no_sqlalchemy_"))
    cfg_path = tmp / "cfg.json"
    points_path = tmp / "points.xlsx"
    points_book = Workbook()
    points_book.save(points_path)
    points_book.close()
    cfg_path.write_text(
        json.dumps(
            {
                "paths": {"output_dir": str(tmp / "logs"), "points_excel": str(points_path)},
                "devices": {
                    "gas_analyzer": {"enabled": False},
                    "gas_analyzers": [{"enabled": False, "name": "GA01", "port": "COM1", "baud": 115200, "device_id": "010"}],
                },
                "workflow": {"sampling": {"quality": {"enabled": False}}},
                "coefficients": {
                    "enabled": False,
                    "model": "ratio_poly_rt_p",
                    "summary_columns": {
                        "co2": {"target": "ppm_CO2_Tank", "ratio": "R_CO2", "temperature": "Temp", "pressure": "BAR"},
                        "h2o": {"target": "ppm_H2O_Dew", "ratio": "R_H2O", "temperature": "Temp", "pressure": "BAR"},
                    },
                },
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    run_dir = tmp / "run_demo"
    run_dir.mkdir()

    point_rows = [
        {"流程阶段": "co2", "温箱目标温度C": 20, "目标二氧化碳浓度ppm": 200, "目标压力hPa": 1000, "点位标题": "co2-20"},
        {"流程阶段": "h2o", "温箱目标温度C": 20, "湿度发生器目标温度C": 20, "湿度发生器目标湿度%": 70, "目标压力hPa": 500, "点位标题": "h2o-20"},
    ]
    summary_rows = [
        {"Analyzer": "GA01", "PointPhase": "co2", "TempSet": 20, "ppm_CO2_Tank": 200, "PressureTarget": 1000, "ppm_CO2": 205, "PointRow": 1},
        {"Analyzer": "GA01", "PointPhase": "h2o", "TempSet": 20, "HgenTempSet": 20, "HgenRhSet": 70, "PressureTarget": 500, "ppm_H2O_Dew": 16.0, "ppm_H2O": 15.5, "PointRow": 2},
    ]
    temp_rows = [
        {"analyzer_id": "GA01", "temp_setpoint_c": 20, "ref_temp_c": 20, "analyzer_cell_temp_raw_c": 20.02, "analyzer_shell_temp_raw_c": 20.03},
        {"analyzer_id": "GA01", "temp_setpoint_c": 30, "ref_temp_c": 30, "analyzer_cell_temp_raw_c": 30.01, "analyzer_shell_temp_raw_c": 30.02},
    ]

    sidecar._load_merge_inputs = lambda run_dirs: {
        "summary_rows_by_run": {str(run_dir): summary_rows},
        "point_rows_by_run": {str(run_dir): point_rows},
        "temperature_rows_by_run": {str(run_dir): temp_rows},
    }

    def fake_export_ratio_poly(summary_frame, *, out_dir, coeff_cfg):
        out_dir.mkdir(parents=True, exist_ok=True)
        report = out_dir / "calibration_coefficients.xlsx"
        wb = Workbook()
        ws1 = wb.active
        ws1.title = "汇总"
        ws1.append(["分析仪", "气体", "Constant"])
        ws1.append(["GA01", "CO2", 1.23])
        ws2 = wb.create_sheet("download_plan")
        ws2.append(["Analyzer", "Gas", "PrimaryCommand", "SecondaryCommand", "ModeEnterCommand", "ModeExitCommand"])
        ws2.append(["GA01", "CO2", "SENCO1,YGAS,FFF,1.0,2.0,3.0,4.0", "", "", ""])
        wb.save(report)
        wb.close()
        return report

    sidecar.export_ratio_poly_report_from_summary_frame = fake_export_ratio_poly

    rc = sidecar.main(
        [
            "--config",
            str(cfg_path),
            "--run-dir",
            str(run_dir),
            "--output-dir",
            str(tmp / "out"),
        ]
    )
    assert rc == 0
    assert (tmp / "out" / "merge_manifest.json").exists()
    assert list((tmp / "out").glob("校准汇总与验证结论_*.xlsx"))
finally:
    builtins.__import__ = real_import
"""
    env = os.environ.copy()
    existing_pythonpath = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = str(src_root) if not existing_pythonpath else os.pathsep.join([str(src_root), existing_pythonpath])
    result = subprocess.run(
        [sys.executable, "-c", script],
        cwd=repo_root,
        env=env,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr or result.stdout


def test_merged_sidecar_postprocess_runtime_skips_sqlalchemy_backed_steps_when_dependency_missing() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    src_root = repo_root / "src"
    script = """
import builtins
import tempfile
from pathlib import Path

real_import = builtins.__import__

def fake_import(name, globals=None, locals=None, fromlist=(), level=0):
    if name == "sqlalchemy" or name.startswith("sqlalchemy."):
        raise ModuleNotFoundError("No module named 'sqlalchemy'")
    return real_import(name, globals, locals, fromlist, level)

builtins.__import__ = fake_import

try:
    import gas_calibrator.v2.adapters.v1_postprocess_runner as runner

    tmp = Path(tempfile.mkdtemp(prefix="postprocess_no_sqlalchemy_"))
    run_dir = tmp / "run_demo"
    run_dir.mkdir()

    database_step = runner._database_import_step(
        run_dir=run_dir,
        artifact_dir=tmp,
        config_path=None,
        dsn=None,
        stage="raw",
    )
    assert database_step["status"] == "skipped"
    assert database_step["dependency"] == "sqlalchemy"
    assert "sqlalchemy" in database_step["reason"].lower()

    analytics_step = runner._analytics_step(
        run_dir=run_dir,
        target_dir=tmp,
        run_id="demo",
        config_path=None,
        dsn=None,
        import_db=False,
        raw_database_step={"status": "skipped"},
        enrich_database_step={"status": "skipped"},
        run_analytics=True,
        skip_analytics=False,
    )
    assert analytics_step["status"] == "skipped"
    assert analytics_step["dependency"] == "sqlalchemy"
    assert "sqlalchemy" in analytics_step["reason"].lower()

    measurement_step = runner._measurement_analytics_step(
        run_dir=run_dir,
        target_dir=tmp,
        run_id="demo",
        config_path=None,
        dsn=None,
        import_db=False,
        raw_database_step={"status": "skipped"},
        enrich_database_step={"status": "skipped"},
        run_measurement_analytics=True,
        skip_measurement_analytics=False,
    )
    assert measurement_step["status"] == "skipped"
    assert measurement_step["dependency"] == "sqlalchemy"
    assert "sqlalchemy" in measurement_step["reason"].lower()
finally:
    builtins.__import__ = real_import
"""
    env = os.environ.copy()
    existing_pythonpath = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = str(src_root) if not existing_pythonpath else os.pathsep.join([str(src_root), existing_pythonpath])
    result = subprocess.run(
        [sys.executable, "-c", script],
        cwd=repo_root,
        env=env,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr or result.stdout


def test_validate_dry_collect_runs_without_aux_devices(monkeypatch, tmp_path: Path) -> None:
    cfg_path = tmp_path / "cfg.json"
    _write_json(cfg_path, _base_cfg(tmp_path / "logs"))
    fake = _FakeGasAnalyzer()

    monkeypatch.setattr(validate_dry_collect, "_build_devices", lambda cfg, io_logger=None: {"gas_analyzer": fake, "gas_analyzer_01": fake})
    monkeypatch.setattr(validate_dry_collect, "_close_devices", lambda devices: None)

    assert validate_dry_collect.main(["--config", str(cfg_path), "--output-dir", str(tmp_path / "out"), "--count", "2", "--interval-s", "0"]) == 0
    assert next((tmp_path / "out").glob("dry_collect_*")).is_dir()


def test_merged_sidecar_main_writes_summary_workbook(monkeypatch, tmp_path: Path) -> None:
    cfg = _base_cfg(tmp_path / "logs")
    points_path = tmp_path / "points.xlsx"
    _build_points_workbook(points_path)
    cfg["paths"]["points_excel"] = str(points_path)
    cfg_path = tmp_path / "cfg.json"
    _write_json(cfg_path, cfg)
    run_dir = tmp_path / "run_demo"
    run_dir.mkdir()

    point_rows = [
        {"流程阶段": "co2", "温箱目标温度C": 20, "目标二氧化碳浓度ppm": 200, "目标压力hPa": 1000, "点位标题": "co2-20"},
        {"流程阶段": "h2o", "温箱目标温度C": 20, "湿度发生器目标温度C": 20, "湿度发生器目标湿度%": 70, "目标压力hPa": 500, "点位标题": "h2o-20"},
    ]
    summary_rows = [
        {"Analyzer": "GA01", "PointPhase": "co2", "TempSet": 20, "ppm_CO2_Tank": 200, "PressureTarget": 1000, "ppm_CO2": 205, "PointRow": 1},
        {"Analyzer": "GA01", "PointPhase": "h2o", "TempSet": 20, "HgenTempSet": 20, "HgenRhSet": 70, "PressureTarget": 500, "ppm_H2O_Dew": 16.0, "ppm_H2O": 15.5, "PointRow": 2},
    ]
    temp_rows = [
        {"analyzer_id": "GA01", "temp_setpoint_c": 20, "ref_temp_c": 20, "analyzer_cell_temp_raw_c": 20.02, "analyzer_shell_temp_raw_c": 20.03},
        {"analyzer_id": "GA01", "temp_setpoint_c": 30, "ref_temp_c": 30, "analyzer_cell_temp_raw_c": 30.01, "analyzer_shell_temp_raw_c": 30.02},
    ]

    monkeypatch.setattr(
        run_v1_merged_calibration_sidecar,
        "_load_merge_inputs",
        lambda run_dirs: {
            "summary_rows_by_run": {str(run_dir): summary_rows},
            "point_rows_by_run": {str(run_dir): point_rows},
            "temperature_rows_by_run": {str(run_dir): temp_rows},
        },
    )

    def _fake_export_ratio_poly(summary_frame, *, out_dir, coeff_cfg):
        out_dir.mkdir(parents=True, exist_ok=True)
        report = out_dir / "calibration_coefficients.xlsx"
        wb = Workbook()
        ws1 = wb.active
        ws1.title = "汇总"
        ws1.append(["分析仪", "气体", "Constant"])
        ws1.append(["GA01", "CO2", 1.23])
        ws2 = wb.create_sheet("download_plan")
        ws2.append(["Analyzer", "Gas", "PrimaryCommand", "SecondaryCommand", "ModeEnterCommand", "ModeExitCommand"])
        ws2.append(["GA01", "CO2", "SENCO1,YGAS,FFF,1.0,2.0,3.0,4.0", "SENCO3,YGAS,FFF,5.0,6.0,7.0,8.0", "MODE,YGAS,FFF,2", "MODE,YGAS,FFF,1"])
        wb.save(report)
        wb.close()
        return report

    monkeypatch.setattr(run_v1_merged_calibration_sidecar, "export_ratio_poly_report_from_summary_frame", _fake_export_ratio_poly)

    assert (
        run_v1_merged_calibration_sidecar.main(
            [
                "--config",
                str(cfg_path),
                "--run-dir",
                str(run_dir),
                "--output-dir",
                str(tmp_path / "out"),
            ]
        )
        == 0
    )

    assert (tmp_path / "out" / "merge_manifest.json").exists()
    assert any((tmp_path / "out").glob("校准汇总与验证结论_*.xlsx"))


def test_validate_pressure_only_exports_pressure_checks(monkeypatch, tmp_path: Path) -> None:
    cfg = _base_cfg(tmp_path / "logs")
    cfg["devices"]["pressure_gauge"]["enabled"] = True
    cfg_path = tmp_path / "cfg.json"
    _write_json(cfg_path, cfg)
    fake = _FakeGasAnalyzer()

    monkeypatch.setattr(
        validate_pressure_only,
        "_build_devices",
        lambda cfg, io_logger=None: {"gas_analyzer": fake, "gas_analyzer_01": fake, "pressure_gauge": _FakePressureGauge()},
    )
    monkeypatch.setattr(validate_pressure_only, "_close_devices", lambda devices: None)

    assert validate_pressure_only.main(["--config", str(cfg_path), "--output-dir", str(tmp_path / "out"), "--pressure-points", "ambient,900", "--count", "2", "--interval-s", "0", "--no-prompt"]) == 0
    run_dir = next((tmp_path / "out").glob("pressure_only_*"))
    assert (run_dir / "pressure_source_check.csv").exists()


def test_verify_coefficient_roundtrip_with_same_value_write(monkeypatch, tmp_path: Path) -> None:
    cfg_path = tmp_path / "cfg.json"
    _write_json(cfg_path, _base_cfg(tmp_path / "logs"))
    monkeypatch.setattr(verify_coefficient_roundtrip, "GasAnalyzer", _FakeRoundtripAnalyzer)

    assert verify_coefficient_roundtrip.main(["--config", str(cfg_path), "--analyzer", "GA01", "--write-back-same", "--output-dir", str(tmp_path / "out")]) == 0
    assert any((tmp_path / "out").glob("coefficient_roundtrip_*.xlsx"))


def test_validation_tool_import_smoke() -> None:
    import gas_calibrator.tools.run_prevalidation_no_sources  # noqa: F401
    import gas_calibrator.tools.run_headless  # noqa: F401
    import gas_calibrator.tools.verify_short_run  # noqa: F401
    import gas_calibrator.ui.app  # noqa: F401


def test_run_prevalidation_no_sources_parses_flags() -> None:
    args = run_prevalidation_no_sources._parse_args(
        [
            "--skip-offline",
            "--include-pressure",
            "--include-roundtrip",
            "--allow-write-back-same",
            "--analyzer",
            "GA01",
            "--fail-fast",
        ]
    )

    assert args.skip_offline is True
    assert args.include_pressure is True
    assert args.include_roundtrip is True
    assert args.allow_write_back_same is True
    assert args.analyzer == "GA01"
    assert args.fail_fast is True


def test_run_prevalidation_no_sources_roundtrip_defaults_to_readonly(monkeypatch, tmp_path: Path) -> None:
    cfg_path = tmp_path / "cfg.json"
    _write_json(cfg_path, _base_cfg(tmp_path / "logs"))
    captured = {}

    def _roundtrip(argv):
        captured["argv"] = list(argv)
        return 0

    monkeypatch.setattr(run_prevalidation_no_sources.verify_coefficient_roundtrip, "main", _roundtrip)

    assert (
        run_prevalidation_no_sources.main(
            [
                "--config",
                str(cfg_path),
                "--output-dir",
                str(tmp_path / "out"),
                "--skip-offline",
                "--skip-dry-collect",
                "--include-roundtrip",
            ]
        )
        == 0
    )
    assert "--write-back-same" not in captured["argv"]
    assert (tmp_path / "out" / "summary.json").exists()


def test_run_prevalidation_no_sources_continues_after_failed_step(monkeypatch, tmp_path: Path) -> None:
    cfg_path = tmp_path / "cfg.json"
    _write_json(cfg_path, _base_cfg(tmp_path / "logs"))
    offline_run_dir = tmp_path / "run_demo"
    offline_run_dir.mkdir()
    calls: list[str] = []

    def _offline(_argv):
        calls.append("offline")
        return 0

    def _dry(_argv):
        calls.append("dry_collect")
        return 1

    def _roundtrip(_argv):
        calls.append("roundtrip")
        return 0

    monkeypatch.setattr(run_prevalidation_no_sources.validate_offline_run, "main", _offline)
    monkeypatch.setattr(run_prevalidation_no_sources.validate_dry_collect, "main", _dry)
    monkeypatch.setattr(run_prevalidation_no_sources.verify_coefficient_roundtrip, "main", _roundtrip)

    assert (
        run_prevalidation_no_sources.main(
            [
                "--config",
                str(cfg_path),
                "--output-dir",
                str(tmp_path / "out"),
                "--offline-run-dir",
                str(offline_run_dir),
                "--include-roundtrip",
            ]
        )
        == 1
    )
    assert calls == ["offline", "dry_collect", "roundtrip"]

    summary = json.loads((tmp_path / "out" / "summary.json").read_text(encoding="utf-8"))
    assert [step["name"] for step in summary["steps"]] == ["offline", "dry_collect", "roundtrip"]
    assert [step["status"] for step in summary["steps"]] == ["PASS", "FAIL", "PASS"]


def test_run_prevalidation_no_sources_writes_summary_files(monkeypatch, tmp_path: Path) -> None:
    cfg_path = tmp_path / "cfg.json"
    _write_json(cfg_path, _base_cfg(tmp_path / "logs"))

    def _dry(argv):
        out_dir = Path(argv[argv.index("--output-dir") + 1])
        out_dir.mkdir(parents=True, exist_ok=True)
        (out_dir / "dry_collect_report.txt").write_text("ok", encoding="utf-8")
        return 0

    monkeypatch.setattr(run_prevalidation_no_sources.validate_dry_collect, "main", _dry)

    assert (
        run_prevalidation_no_sources.main(
            [
                "--config",
                str(cfg_path),
                "--output-dir",
                str(tmp_path / "out"),
                "--skip-offline",
            ]
        )
        == 0
    )
    summary_json = tmp_path / "out" / "summary.json"
    summary_md = tmp_path / "out" / "summary.md"
    assert summary_json.exists()
    assert summary_md.exists()
    assert "dry_collect" in summary_json.read_text(encoding="utf-8")
    md_text = summary_md.read_text(encoding="utf-8")
    assert "dry_collect" in md_text
    assert "frame_quality_summary" in md_text


def test_run_prevalidation_no_sources_old_config_compatible(tmp_path: Path) -> None:
    cfg_path = tmp_path / "cfg_min.json"
    _write_json(
        cfg_path,
        {
            "paths": {"output_dir": str(tmp_path / "logs")},
            "devices": {
                "gas_analyzers": [{"enabled": True, "name": "GA01", "device_id": "010"}],
                "gas_analyzer": {"enabled": False},
            },
        },
    )

    assert (
        run_prevalidation_no_sources.main(
            [
                "--config",
                str(cfg_path),
                "--output-dir",
                str(tmp_path / "out"),
                "--skip-offline",
                "--skip-dry-collect",
            ]
        )
        == 0
    )
    assert (tmp_path / "out" / "summary.json").exists()
