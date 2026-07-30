"""Offline point preparation shared by simulation planning and execution."""

from __future__ import annotations

from pathlib import Path
from typing import Any, Callable, Optional

from gas_calibrator.data.points import reorder_points as legacy_reorder_points

from .point_parser import PointFilter, PointParser
from .route_planner import RoutePlanner
from .runtime_point import CalibrationPoint

__all__ = [
    "filter_selected_temperatures",
    "normalize_negative_temperature_route",
    "parse_points_for_execution",
    "prepare_points_for_execution",
    "reorder_points_for_execution",
]


def normalize_negative_temperature_route(point: CalibrationPoint) -> CalibrationPoint:
    if float(point.temp_chamber_c) >= 0.0 or not point.is_h2o_point:
        return point
    return CalibrationPoint(
        index=point.index,
        temperature_c=point.temperature_c,
        co2_ppm=point.co2_ppm,
        humidity_pct=None,
        pressure_hpa=point.pressure_hpa,
        route="co2",
        humidity_generator_temp_c=None,
        dewpoint_c=None,
        h2o_mmol=None,
        raw_h2o=None,
        co2_group=point.co2_group,
        cylinder_nominal_ppm=point.cylinder_nominal_ppm,
    )


def filter_selected_temperatures(
    points: list[CalibrationPoint],
    *,
    selected_temps_c: Any,
    log: Optional[Callable[[str], None]] = None,
) -> list[CalibrationPoint]:
    raw = selected_temps_c
    if raw in (None, "", []):
        return list(points)
    if not isinstance(raw, list):
        raw = [raw]

    selected: list[float] = []
    for item in raw:
        try:
            selected.append(float(item))
        except Exception:
            continue
    if not selected:
        if log is not None:
            log("Temperature filter requested but no valid selected_temps_c values were parsed; keep all points")
        return list(points)

    filtered = [
        point
        for point in points
        if any(abs(float(point.temp_chamber_c) - target) < 1e-9 for target in selected)
    ]
    if log is not None:
        selected_text = ",".join(f"{value:g}" for value in selected)
        log(f"Temperature filter: temps=[{selected_text}]C -> {len(filtered)}/{len(points)} points")
    return filtered


def reorder_points_for_execution(
    points: list[CalibrationPoint],
    *,
    route_planner: RoutePlanner,
    temperature_descending: bool,
) -> list[CalibrationPoint]:
    normalized = [normalize_negative_temperature_route(point) for point in points]
    return legacy_reorder_points(
        normalized,
        route_planner.water_first_temp_threshold(),
        descending_temperatures=temperature_descending,
    )


def prepare_points_for_execution(
    points: list[CalibrationPoint],
    *,
    selected_temps_c: Any,
    temperature_descending: bool,
    route_planner: RoutePlanner,
    point_parser: Optional[PointParser] = None,
    point_filter: Optional[PointFilter] = None,
    log: Optional[Callable[[str], None]] = None,
) -> list[CalibrationPoint]:
    prepared = filter_selected_temperatures(
        points,
        selected_temps_c=selected_temps_c,
        log=log,
    )
    prepared = reorder_points_for_execution(
        prepared,
        route_planner=route_planner,
        temperature_descending=temperature_descending,
    )
    if point_filter is not None and point_parser is not None:
        prepared = point_parser.filter(prepared, point_filter)
    return list(prepared)


def parse_points_for_execution(
    path: Path,
    *,
    point_parser: PointParser,
    selected_temps_c: Any,
    temperature_descending: bool,
    route_planner: RoutePlanner,
    point_filter: Optional[PointFilter] = None,
    log: Optional[Callable[[str], None]] = None,
) -> list[CalibrationPoint]:
    points = point_parser.parse(path)
    return prepare_points_for_execution(
        points,
        selected_temps_c=selected_temps_c,
        temperature_descending=temperature_descending,
        route_planner=route_planner,
        point_parser=point_parser,
        point_filter=point_filter,
        log=log,
    )
