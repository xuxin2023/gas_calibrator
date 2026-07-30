from __future__ import annotations

import ast
from copy import deepcopy
from pathlib import Path

from gas_calibrator.validation.simulation.plan_preview import (
    build_preview_rows,
    match_runtime_row,
)
from gas_calibrator.validation.simulation.point_parser import PointParser
from gas_calibrator.validation.simulation.pressure_selection import (
    AMBIENT_PRESSURE_LABEL,
    AMBIENT_PRESSURE_TOKEN,
)
from gas_calibrator.v2.core.plan_compiler import CompiledPlan


def _runtime_rows() -> list[dict[str, object]]:
    return [
        {
            "index": 1,
            "temperature": 25.0,
            "route": "co2",
            "co2_ppm": 400.0,
            "co2_group": "A",
            "cylinder_nominal_ppm": 401.0,
            "pressure_hpa": None,
            "pressure_mode": "ambient_open",
            "pressure_target_label": AMBIENT_PRESSURE_LABEL,
            "pressure_selection_token": AMBIENT_PRESSURE_TOKEN,
        },
        {
            "index": 2,
            "temperature": 25.0,
            "route": "co2",
            "co2_ppm": 400.0,
            "co2_group": "B",
            "cylinder_nominal_ppm": 405.0,
            "pressure_hpa": None,
            "pressure_mode": "ambient_open",
            "pressure_target_label": AMBIENT_PRESSURE_LABEL,
            "pressure_selection_token": AMBIENT_PRESSURE_TOKEN,
        },
        {
            "index": 3,
            "temperature": 25.0,
            "route": "h2o",
            "humidity_pct": 50.0,
            "humidity_generator_temp_c": 25.0,
            "dewpoint_c": 13.9,
            "pressure_hpa": 900.0,
            "pressure_mode": "sealed_controlled",
            "pressure_target_label": "900hPa",
            "pressure_selection_token": "",
        },
    ]


def test_plan_preview_functions_have_one_shared_owner_and_no_v2_import() -> None:
    functions = (build_preview_rows, match_runtime_row)
    assert all(
        function.__module__
        == "gas_calibrator.validation.simulation.plan_preview"
        for function in functions
    )

    path = (
        Path(__file__).resolve().parents[2]
        / "src/gas_calibrator/validation/simulation/plan_preview.py"
    )
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    imported_modules = {
        str(node.module or "")
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom)
    }
    assert all(
        not module.startswith("gas_calibrator.v2")
        for module in imported_modules
    )
    assert not hasattr(CompiledPlan, "_match_runtime_row")


def test_plan_preview_matches_pressure_co2_group_and_nominal_value() -> None:
    runtime_rows = _runtime_rows()
    original_rows = deepcopy(runtime_rows)
    parser = PointParser()
    points = [
        parser._row_to_point(index, row)
        for index, row in enumerate(runtime_rows, start=1)
    ]
    missing = parser._row_to_point(
        4,
        {
            "index": 4,
            "temperature": 30.0,
            "route": "co2",
            "co2_ppm": 800.0,
            "co2_group": "B",
            "pressure_hpa": 1000.0,
            "pressure_mode": "sealed_controlled",
        },
    )

    preview_rows = build_preview_rows(
        [points[1], points[0], points[2], missing],
        runtime_rows=runtime_rows,
    )

    assert [row["cylinder_nominal_ppm"] for row in preview_rows] == [
        405.0,
        401.0,
        None,
        None,
    ]
    assert preview_rows[0]["pressure_selection_token"] == (
        AMBIENT_PRESSURE_TOKEN
    )
    assert preview_rows[2]["pressure_hpa"] == 900.0
    assert preview_rows[3]["temperature_c"] == 30.0
    assert runtime_rows == original_rows


def test_compiled_plan_report_payload_remains_v2_owned() -> None:
    plan = CompiledPlan(
        profile_name="report_boundary",
        runtime_rows=_runtime_rows(),
        metadata={
            "formal_calibration_report": True,
            "report_family": "v2_product_report_family",
            "report_templates": {"co2": "template"},
        },
    )

    payload = plan.to_runtime_payload()

    assert CompiledPlan.__module__ == (
        "gas_calibrator.v2.core.plan_compiler"
    )
    assert payload["formal_calibration_report"] is True
    assert payload["report_family"] == "v2_product_report_family"
    assert payload["report_templates"] == {"co2": "template"}
