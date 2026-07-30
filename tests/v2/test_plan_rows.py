from __future__ import annotations

import ast
from pathlib import Path
from types import SimpleNamespace

from gas_calibrator.validation.simulation.plan_models import (
    CalibrationPlanProfile,
    GasPointSpec,
    HumiditySpec,
    PlanOrderingOptions,
    PressureSpec,
    TemperatureSpec,
)
from gas_calibrator.validation.simulation.plan_rows import (
    build_source_rows,
    expand_runtime_rows,
    preview_points_in_execution_order,
    rows_to_points,
)
from gas_calibrator.validation.simulation.point_parser import PointParser
from gas_calibrator.validation.simulation.pressure_selection import (
    AMBIENT_PRESSURE_TOKEN,
)
from gas_calibrator.validation.simulation.route_planner import RoutePlanner
from gas_calibrator.v2.core.plan_compiler import PlanCompiler


def _profile() -> CalibrationPlanProfile:
    return CalibrationPlanProfile(
        name="shared_rows",
        temperatures=[
            TemperatureSpec(temperature_c=20.0, order=1),
            TemperatureSpec(temperature_c=-10.0, order=2),
            TemperatureSpec(temperature_c=99.0, enabled=False, order=0),
        ],
        humidities=[
            HumiditySpec(
                hgen_temp_c=20.0,
                hgen_rh_pct=60.0,
                dewpoint_c=11.0,
            )
        ],
        gas_points=[
            GasPointSpec(co2_ppm=0.0, order=1),
            GasPointSpec(
                co2_ppm=400.0,
                co2_group="B",
                cylinder_nominal_ppm=405.0,
                order=2,
            ),
        ],
        pressures=[
            PressureSpec(pressure_hpa=1100.0, order=1),
            PressureSpec(pressure_hpa=900.0, order=2),
        ],
        ordering=PlanOrderingOptions(),
    )


def test_plan_row_functions_have_one_shared_owner_and_no_v2_import() -> None:
    functions = (
        build_source_rows,
        expand_runtime_rows,
        preview_points_in_execution_order,
        rows_to_points,
    )
    assert all(
        function.__module__
        == "gas_calibrator.validation.simulation.plan_rows"
        for function in functions
    )

    path = (
        Path(__file__).resolve().parents[2]
        / "src/gas_calibrator/validation/simulation/plan_rows.py"
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


def test_plan_compiler_no_longer_owns_plan_row_helpers() -> None:
    removed_methods = (
        "_build_source_rows",
        "_expand_runtime_rows",
        "_rows_to_points",
        "_preview_points_in_execution_order",
        "_ordered_temperatures",
        "_ordered_specs",
        "_ordered_pressure_specs",
        "_pressure_row_payload",
    )

    assert all(not hasattr(PlanCompiler, name) for name in removed_methods)


def test_shared_plan_rows_preserve_filter_order_pressure_and_carry_forward() -> None:
    source_rows = build_source_rows(
        _profile(),
        selected_temps_c=[20.0, -10.0],
        selected_pressure_points=[AMBIENT_PRESSURE_TOKEN, 900.0],
        skip_co2_ppm=[0],
        h2o_carry_forward=True,
    )
    runtime_rows = expand_runtime_rows(
        source_rows,
        h2o_carry_forward=True,
    )

    assert len(source_rows) == 6
    assert [row["temperature"] for row in source_rows] == [
        20.0,
        20.0,
        20.0,
        20.0,
        -10.0,
        -10.0,
    ]
    assert [row["route"] for row in source_rows] == [
        "h2o",
        "h2o",
        "co2",
        "co2",
        "co2",
        "co2",
    ]
    assert source_rows[0]["pressure_selection_token"] == AMBIENT_PRESSURE_TOKEN
    assert source_rows[1]["pressure_hpa"] == 900.0
    assert source_rows[0]["humidity_pct"] == 60.0
    assert "humidity_pct" not in source_rows[1]
    assert runtime_rows[1]["humidity_pct"] == 60.0
    assert runtime_rows[1]["dewpoint_c"] == 11.0
    assert all(row.get("co2_ppm") != 0.0 for row in source_rows)
    assert all(row.get("temperature") != 99.0 for row in source_rows)


def test_shared_plan_rows_convert_and_preview_without_v2_config() -> None:
    source_rows = build_source_rows(
        _profile(),
        selected_temps_c=[20.0],
        selected_pressure_points=[AMBIENT_PRESSURE_TOKEN, 900.0],
        skip_co2_ppm=[0],
        h2o_carry_forward=True,
    )
    runtime_rows = expand_runtime_rows(
        source_rows,
        h2o_carry_forward=True,
    )
    point_parser = PointParser()
    points = rows_to_points(runtime_rows, point_parser=point_parser)
    config = SimpleNamespace(
        workflow=SimpleNamespace(
            route_mode="h2o_then_co2",
            water_first_all_temps=True,
            water_first_temp_gte=None,
            skip_co2_ppm=[0],
            h2o_carry_forward=True,
        )
    )
    preview_points = preview_points_in_execution_order(
        points,
        route_planner=RoutePlanner(config, point_parser),
    )

    assert [point.route for point in preview_points] == [
        "h2o",
        "h2o",
        "co2",
        "co2",
    ]
    assert [
        point.pressure_selection_token_value
        for point in preview_points
    ] == [
        AMBIENT_PRESSURE_TOKEN,
        "",
        AMBIENT_PRESSURE_TOKEN,
        "",
    ]
