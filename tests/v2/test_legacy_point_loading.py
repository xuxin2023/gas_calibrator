import json
from pathlib import Path

import pytest

from gas_calibrator.v2.config import AppConfig
from gas_calibrator.v2.core.calibration_service import CalibrationService
from gas_calibrator.v2.core.point_parser import LegacyExcelPointLoader, PointParser


def test_point_parser_uses_legacy_excel_loader_for_title_row_workbooks(tmp_path: Path) -> None:
    openpyxl = pytest.importorskip("openpyxl")
    h2o_text = (
        "25\u2103\uff08\u6e7f\u5ea6\u53d1\u751f\u5668\uff09 "
        "50%\uff08\u6e7f\u5ea6\u53d1\u751f\u5668\uff09 "
        "1\u2103\uff08\u9732\u70b9\u6e29\u5ea6\uff09 "
        "2 mmol/mol"
    )
    workbook = openpyxl.Workbook()
    sheet = workbook.active
    sheet.append(["Normalized calibration points"])
    sheet.append(["Temp_C", "CO2_ppm", "H2O_text", "Pressure_hPa", "CO2_group"])
    sheet.append([-10.0, 0.0, h2o_text, 1100.0, "A"])
    sheet.append([10.0, None, h2o_text, None, None])
    path = tmp_path / "legacy_points.xlsx"
    workbook.save(path)

    parser = PointParser(
        legacy_excel_loader=LegacyExcelPointLoader(
            missing_pressure_policy="carry_forward",
            carry_forward_h2o=True,
        )
    )

    points = parser.parse(path)

    assert [point.index for point in points] == [3, 4]
    assert points[0].temperature_c == -10.0
    assert points[0].route == "co2"
    assert points[0].humidity_pct is None
    assert points[0].pressure_hpa == 1100.0
    assert points[1].temperature_c == 10.0
    assert points[1].route == "h2o"
    assert points[1].humidity_pct == 50.0
    assert points[1].pressure_hpa == 1100.0


def test_calibration_service_load_points_matches_v1_legacy_order_for_repo_points(tmp_path: Path) -> None:
    repo_root = Path(__file__).resolve().parents[2]
    config = AppConfig.from_dict(
        {
            "workflow": {
                "missing_pressure_policy": "carry_forward",
                "h2o_carry_forward": True,
            },
            "paths": {
                "points_excel": str(repo_root / "points.xlsx"),
                "output_dir": str(tmp_path / "out"),
            },
        }
    )

    service = CalibrationService(config)

    count = service.load_points()

    assert count == 58
    assert [group.temperature_c for group in service._temperature_groups] == [40.0, 30.0, 20.0, 10.0, 0.0, -10.0, -20.0]
    assert all(not point.is_h2o_point for point in service._points if point.temperature_c < 0.0)

    for temperature_c in (40.0, 30.0, 20.0, 10.0, 0.0):
        routes = [point.route for point in service._points if point.temperature_c == temperature_c]
        assert routes == sorted(routes, key=lambda route: 0 if route == "h2o" else 1)


def test_calibration_service_applies_selected_temperature_filter_before_grouping(tmp_path: Path) -> None:
    repo_root = Path(__file__).resolve().parents[2]
    config = AppConfig.from_dict(
        {
            "workflow": {
                "missing_pressure_policy": "carry_forward",
                "h2o_carry_forward": True,
                "selected_temps_c": [20.0, 0.0],
            },
            "paths": {
                "points_excel": str(repo_root / "points.xlsx"),
                "output_dir": str(tmp_path / "out"),
            },
        }
    )

    service = CalibrationService(config)

    count = service.load_points()

    assert count == 18
    assert [group.temperature_c for group in service._temperature_groups] == [20.0, 0.0]
    assert {point.temperature_c for point in service._points} == {20.0, 0.0}


def test_calibration_service_honors_temperature_descending_flag_when_loading_points(tmp_path: Path) -> None:
    points_path = tmp_path / "points.json"
    points_path.write_text(
        json.dumps(
            {
                "points": [
                    {"index": 1, "temperature_c": 20.0, "co2_ppm": 400.0, "pressure_hpa": 1000.0, "route": "co2"},
                    {"index": 2, "temperature_c": 40.0, "co2_ppm": 800.0, "pressure_hpa": 1000.0, "route": "co2"},
                    {"index": 3, "temperature_c": 0.0, "co2_ppm": 0.0, "pressure_hpa": 1000.0, "route": "co2"},
                ]
            }
        ),
        encoding="utf-8",
    )
    config = AppConfig.from_dict(
        {
            "workflow": {
                "temperature_descending": False,
            },
            "paths": {
                "points_excel": str(points_path),
                "output_dir": str(tmp_path / "out"),
            },
        }
    )

    service = CalibrationService(config)

    count = service.load_points()

    assert count == 3
    assert [group.temperature_c for group in service._temperature_groups] == [0.0, 20.0, 40.0]


def test_calibration_service_honors_water_first_threshold_when_loading_points(tmp_path: Path) -> None:
    points_path = tmp_path / "points.json"
    points_path.write_text(
        json.dumps(
            {
                "points": [
                    {"index": 1, "temperature_c": 20.0, "co2_ppm": 400.0, "pressure_hpa": 1000.0, "route": "co2"},
                    {"index": 2, "temperature_c": 20.0, "humidity_pct": 35.0, "pressure_hpa": 1000.0, "route": "h2o"},
                    {"index": 3, "temperature_c": 30.0, "co2_ppm": 800.0, "pressure_hpa": 1000.0, "route": "co2"},
                    {"index": 4, "temperature_c": 30.0, "humidity_pct": 40.0, "pressure_hpa": 1000.0, "route": "h2o"},
                ]
            }
        ),
        encoding="utf-8",
    )
    config = AppConfig.from_dict(
        {
            "workflow": {
                "water_first_temp_gte": 25.0,
            },
            "paths": {
                "points_excel": str(points_path),
                "output_dir": str(tmp_path / "out"),
            },
        }
    )

    service = CalibrationService(config)

    count = service.load_points()

    assert count == 4
    assert [group.temperature_c for group in service._temperature_groups] == [30.0, 20.0]
    assert [point.route for point in service._points if point.temperature_c == 30.0] == ["h2o", "co2"]
    assert [point.route for point in service._points if point.temperature_c == 20.0] == ["co2", "h2o"]
