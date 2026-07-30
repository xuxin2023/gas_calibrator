"""Pure plan-row construction shared by offline simulation planning."""

from __future__ import annotations

from typing import Any

from .plan_models import (
    CalibrationPlanProfile,
    GasPointSpec,
    HumiditySpec,
    PressureSpec,
    TemperatureSpec,
)
from .point_parser import PointParser
from .pressure_selection import AMBIENT_PRESSURE_LABEL, AMBIENT_PRESSURE_TOKEN
from .route_planner import RoutePlanner
from .runtime_point import CalibrationPoint

__all__ = [
    "build_source_rows",
    "expand_runtime_rows",
    "preview_points_in_execution_order",
    "rows_to_points",
]


def build_source_rows(
    profile: CalibrationPlanProfile,
    *,
    selected_temps_c: Any = None,
    selected_pressure_points: Any = None,
    skip_co2_ppm: Any = None,
    h2o_carry_forward: bool = False,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    selected_temps = {
        round(float(value), 9)
        for value in list(selected_temps_c or [])
    }
    skip_co2 = {
        int(value)
        for value in list(skip_co2_ppm or [])
    }
    carry_forward_h2o = bool(h2o_carry_forward)
    temperatures = _ordered_temperatures(
        profile.temperatures,
        selected_temps=selected_temps,
    )
    humidities = _ordered_specs(profile.humidities)
    gas_points = [
        item
        for item in _ordered_specs(profile.gas_points)
        if int(round(float(item.co2_ppm))) not in skip_co2
    ]
    pressures = _ordered_pressure_specs(
        profile.pressures,
        selected_pressure_points=list(selected_pressure_points or []),
    )
    pressure_values = pressures or [None]
    next_index = 1

    for temperature in temperatures:
        temperature_c = float(temperature.temperature_c)

        if temperature_c >= 0.0:
            for humidity in humidities:
                for pressure_index, pressure in enumerate(pressure_values):
                    pressure_payload = _pressure_row_payload(pressure)
                    row: dict[str, Any] = {
                        "index": next_index,
                        "temperature": temperature_c,
                        "route": "h2o",
                    }
                    row.update(pressure_payload)
                    if not carry_forward_h2o or pressure_index == 0:
                        row["humidity_pct"] = humidity.hgen_rh_pct
                        row["humidity_generator_temp_c"] = (
                            float(humidity.hgen_temp_c)
                            if humidity.hgen_temp_c is not None
                            else temperature_c
                        )
                        if humidity.dewpoint_c is not None:
                            row["dewpoint_c"] = float(humidity.dewpoint_c)
                    rows.append(row)
                    next_index += 1

        for gas_point in gas_points:
            for pressure in pressure_values:
                pressure_payload = _pressure_row_payload(pressure)
                rows.append(
                    {
                        "index": next_index,
                        "temperature": temperature_c,
                        "route": "co2",
                        "co2_ppm": float(gas_point.co2_ppm),
                        "co2_group": str(
                            getattr(gas_point, "co2_group", "A") or "A"
                        ).strip().upper()
                        or "A",
                        "cylinder_nominal_ppm": getattr(
                            gas_point,
                            "cylinder_nominal_ppm",
                            None,
                        ),
                        **pressure_payload,
                    }
                )
                next_index += 1

    return rows


def expand_runtime_rows(
    source_rows: list[dict[str, Any]],
    *,
    h2o_carry_forward: bool,
) -> list[dict[str, Any]]:
    if not bool(h2o_carry_forward):
        return [dict(row) for row in source_rows]

    runtime_rows: list[dict[str, Any]] = []
    current_h2o_context: dict[float, dict[str, Any]] = {}
    for row in source_rows:
        runtime_row = dict(row)
        temperature_c = float(runtime_row.get("temperature"))
        route = str(runtime_row.get("route", "")).strip().lower()
        if route != "h2o":
            runtime_rows.append(runtime_row)
            continue

        explicit_payload = {
            "humidity_pct": runtime_row.get("humidity_pct"),
            "humidity_generator_temp_c": runtime_row.get(
                "humidity_generator_temp_c"
            ),
            "dewpoint_c": runtime_row.get("dewpoint_c"),
        }
        has_explicit_payload = any(
            value is not None for value in explicit_payload.values()
        )
        if has_explicit_payload:
            current_h2o_context[temperature_c] = explicit_payload
        else:
            payload = current_h2o_context.get(temperature_c)
            if payload is not None:
                for key, value in payload.items():
                    runtime_row[key] = value
        runtime_rows.append(runtime_row)
    return runtime_rows


def rows_to_points(
    rows: list[dict[str, Any]],
    *,
    point_parser: PointParser,
) -> list[CalibrationPoint]:
    return [
        point_parser._row_to_point(index, row)
        for index, row in enumerate(rows, start=1)
    ]


def preview_points_in_execution_order(
    points: list[CalibrationPoint],
    *,
    route_planner: RoutePlanner,
) -> list[CalibrationPoint]:
    ordered: list[CalibrationPoint] = []
    for group in route_planner.group_by_temperature(points):
        group_points = list(group.points)
        for route_name in route_planner.route_sequence(group_points):
            if route_name == "h2o":
                pressure_points = route_planner.h2o_pressure_points(group_points)
                for h2o_group in route_planner.group_h2o_points(group_points):
                    if not h2o_group:
                        continue
                    lead = h2o_group[0]
                    for pressure_point in pressure_points or h2o_group:
                        ordered.append(
                            route_planner.build_h2o_pressure_point(
                                lead,
                                pressure_point,
                            )
                        )
                continue

            if route_name == "co2":
                for source_point in route_planner.co2_sources(group_points):
                    pressure_points = (
                        route_planner.co2_pressure_points(
                            source_point,
                            group_points,
                        )
                        or [source_point]
                    )
                    for pressure_point in pressure_points:
                        ordered.append(
                            route_planner.build_co2_pressure_point(
                                source_point,
                                pressure_point,
                            )
                        )
    return ordered


def _ordered_temperatures(
    specs: list[TemperatureSpec],
    *,
    selected_temps: set[float],
) -> list[TemperatureSpec]:
    ordered = _ordered_specs(specs)
    if not selected_temps:
        return ordered
    return [
        item
        for item in ordered
        if round(float(item.temperature_c), 9) in selected_temps
    ]


def _ordered_specs(
    specs: list[TemperatureSpec]
    | list[HumiditySpec]
    | list[GasPointSpec]
    | list[PressureSpec],
) -> list[Any]:
    decorated: list[tuple[int, int, Any]] = []
    for position, item in enumerate(specs):
        if not bool(getattr(item, "enabled", True)):
            continue
        raw_order = getattr(item, "order", None)
        order_value = (
            int(raw_order)
            if raw_order is not None
            else 10_000 + position
        )
        decorated.append((order_value, position, item))
    decorated.sort(key=lambda item: (item[0], item[1]))
    return [item for _, _, item in decorated]


def _ordered_pressure_specs(
    specs: list[PressureSpec],
    *,
    selected_pressure_points: list[Any],
) -> list[PressureSpec]:
    ordered = _ordered_specs(specs)
    if not selected_pressure_points:
        return ordered

    grouped: dict[float | str, list[PressureSpec]] = {}
    for item in ordered:
        key = item.selection_key()
        if key is None:
            continue
        grouped.setdefault(key, []).append(item)

    selected_specs: list[PressureSpec] = []
    for selection in list(selected_pressure_points or []):
        key = (
            selection
            if isinstance(selection, str)
            else round(float(selection), 6)
        )
        matching = list(grouped.get(key, []))
        if matching:
            selected_specs.extend(matching)
            continue
        if key == AMBIENT_PRESSURE_TOKEN:
            selected_specs.append(
                PressureSpec(
                    pressure_hpa=None,
                    pressure_mode="ambient_open",
                    pressure_target_label=AMBIENT_PRESSURE_LABEL,
                    pressure_selection_token=AMBIENT_PRESSURE_TOKEN,
                    enabled=True,
                )
            )
    return selected_specs


def _pressure_row_payload(
    pressure: PressureSpec | None,
) -> dict[str, Any]:
    if pressure is None:
        return {
            "pressure_hpa": None,
            "pressure_mode": "",
            "pressure_target_label": None,
            "pressure_selection_token": "",
        }
    return {
        "pressure_hpa": (
            None
            if pressure.is_ambient_pressure_point
            else pressure.pressure_hpa
        ),
        "pressure_mode": pressure.effective_pressure_mode,
        "pressure_target_label": pressure.pressure_label(),
        "pressure_selection_token": pressure.pressure_selection_token
        or (
            AMBIENT_PRESSURE_TOKEN
            if pressure.is_ambient_pressure_point
            else ""
        ),
    }
