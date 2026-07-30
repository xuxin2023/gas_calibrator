import csv
from importlib.util import find_spec
import json
from pathlib import Path

import pytest

from gas_calibrator.validation.exceptions import (
    CalibrationError,
    DataError,
    DataParseError,
    DataValidationError,
)
from gas_calibrator.validation.simulation.point_parser import (
    LegacyExcelPointLoader,
    PointFilter,
    PointParser,
    TemperatureGroup,
)
from gas_calibrator.validation.simulation.runtime_point import CalibrationPoint
from gas_calibrator.v2 import core as v2_core
from gas_calibrator.v2 import exceptions as v2_exceptions
from gas_calibrator.v2.core import models as v2_models


def test_point_parser_parses_json_legacy_fields(tmp_path: Path) -> None:
    path = tmp_path / "points.json"
    payload = {
        "points": [
            {"index": 1, "temperature": 20.0, "CO2": 400.0, "pressure": 1000.0, "route": "co2"},
            {"index": 2, "温度": 25.0, "湿度": 35.0, "路线": "h2o"},
        ]
    }
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")

    points = PointParser().parse(path)

    assert len(points) == 2
    assert points[0].temperature_c == 20.0
    assert points[0].co2_ppm == 400.0
    assert points[0].pressure_hpa == 1000.0
    assert points[1].temperature_c == 25.0
    assert points[1].humidity_pct == 35.0
    assert points[1].route == "h2o"


def test_point_parser_parses_csv_and_infers_route(tmp_path: Path) -> None:
    path = tmp_path / "points.csv"
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=["index", "temp", "rh", "pressure"])
        writer.writeheader()
        writer.writerow({"index": 1, "temp": 20.0, "rh": 40.0, "pressure": 1005.0})

    points = PointParser().parse(path)

    assert len(points) == 1
    assert points[0].temperature_c == 20.0
    assert points[0].humidity_pct == 40.0
    assert points[0].pressure_hpa == 1005.0
    assert points[0].route == "h2o"


def test_point_parser_parses_xlsx(tmp_path: Path) -> None:
    openpyxl = pytest.importorskip("openpyxl")
    workbook = openpyxl.Workbook()
    sheet = workbook.active
    sheet.append(["index", "temperature", "co2_ppm", "route"])
    sheet.append([1, 30.0, 800.0, "co2"])
    path = tmp_path / "points.xlsx"
    workbook.save(path)

    points = PointParser().parse(path)

    assert len(points) == 1
    assert points[0].temperature_c == 30.0
    assert points[0].co2_ppm == 800.0
    assert points[0].route == "co2"


def test_point_parser_filter_limits_points() -> None:
    parser = PointParser()
    points = [
        parser._row_to_point(1, {"index": 1, "temperature": 20.0, "co2_ppm": 400.0, "route": "co2"}),
        parser._row_to_point(2, {"index": 2, "temperature": 25.0, "co2_ppm": 800.0, "route": "co2"}),
        parser._row_to_point(3, {"index": 3, "temperature": 30.0, "humidity": 40.0, "route": "h2o"}),
    ]

    filtered = parser.filter(
        points,
        PointFilter(
            temperature_min=22.0,
            temperature_max=30.0,
            routes=["co2"],
            co2_ppm_values=[800.0],
            max_points=1,
        ),
    )

    assert [point.index for point in filtered] == [2]


def test_point_parser_filters_by_indices_and_groups_temperature() -> None:
    parser = PointParser()
    points = [
        parser._row_to_point(1, {"index": 1, "temperature": 20.0, "co2": 400.0}),
        parser._row_to_point(2, {"index": 2, "temperature": 20.0, "co2": 600.0}),
        parser._row_to_point(3, {"index": 3, "temperature": 25.0, "co2": 800.0}),
    ]

    filtered = parser.filter(points, PointFilter(point_indices=[1, 3]))
    groups = parser.group_by_temperature(filtered)

    assert [point.index for point in filtered] == [1, 3]
    assert len(groups) == 2
    assert groups[0].temperature_c == 20.0
    assert [point.index for point in groups[0].points] == [1]
    assert groups[1].temperature_c == 25.0
    assert [point.index for point in groups[1].points] == [3]


def test_point_parser_and_runtime_point_have_shared_single_owners() -> None:
    assert (
        PointParser.__module__
        == "gas_calibrator.validation.simulation.point_parser"
    )
    assert (
        CalibrationPoint.__module__
        == "gas_calibrator.validation.simulation.runtime_point"
    )
    assert DataParseError.__module__ == "gas_calibrator.validation.exceptions"
    assert v2_core.PointParser is PointParser
    assert v2_core.PointFilter is PointFilter
    assert v2_core.TemperatureGroup is TemperatureGroup
    assert v2_core.CalibrationPoint is CalibrationPoint
    assert v2_models.CalibrationPoint is CalibrationPoint
    assert v2_exceptions.CalibrationError is CalibrationError
    assert v2_exceptions.DataError is DataError
    assert v2_exceptions.DataParseError is DataParseError
    assert v2_exceptions.DataValidationError is DataValidationError
    assert v2_core.PointParser().legacy_excel_loader == LegacyExcelPointLoader()
    assert find_spec("gas_calibrator.v2.core.point_parser") is None
