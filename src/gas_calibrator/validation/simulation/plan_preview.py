"""Pure compiled-plan preview helpers for offline validation."""

from __future__ import annotations

from typing import Any

from .pressure_selection import pressure_selection_key
from .runtime_point import CalibrationPoint

__all__ = [
    "build_preview_rows",
    "match_runtime_row",
]


def build_preview_rows(
    preview_points: list[CalibrationPoint],
    *,
    runtime_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for sequence, point in enumerate(preview_points, start=1):
        source_row = match_runtime_row(
            point,
            runtime_rows=runtime_rows,
        )
        rows.append(
            {
                "sequence": sequence,
                "index": point.index,
                "temperature_c": float(point.temp_chamber_c),
                "route": str(point.route),
                "co2_ppm": point.co2_ppm,
                "humidity_pct": point.humidity_pct,
                "humidity_generator_temp_c": (
                    point.humidity_generator_temp_c
                ),
                "dewpoint_c": point.dewpoint_c,
                "pressure_hpa": point.target_pressure_hpa,
                "pressure_mode": point.effective_pressure_mode,
                "pressure_target_label": point.pressure_display_label,
                "pressure_selection_token": (
                    point.pressure_selection_token_value
                ),
                "co2_group": point.co2_group,
                "cylinder_nominal_ppm": source_row.get(
                    "cylinder_nominal_ppm"
                ),
            }
        )
    return rows


def match_runtime_row(
    point: CalibrationPoint,
    *,
    runtime_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    for row in runtime_rows:
        if (
            str(row.get("route", "")).strip().lower()
            != str(point.route).strip().lower()
        ):
            continue
        if float(row.get("temperature", 0.0)) != float(
            point.temp_chamber_c
        ):
            continue
        row_pressure_key = pressure_selection_key(
            pressure_hpa=row.get("pressure_hpa"),
            pressure_mode=row.get("pressure_mode"),
            pressure_selection_token=row.get("pressure_selection_token"),
        )
        if row_pressure_key != point.pressure_selection_key:
            continue
        if str(point.route).strip().lower() == "co2":
            if row.get("co2_ppm") is None or point.co2_ppm is None:
                continue
            if float(row.get("co2_ppm")) != float(point.co2_ppm):
                continue
            if (
                str(row.get("co2_group", "")).strip().upper()
                != str(point.co2_group or "").strip().upper()
            ):
                continue
        return dict(row)
    return {}
