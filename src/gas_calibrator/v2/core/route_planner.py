from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Optional

from ..config import AppConfig
from ..domain.pressure_selection import AMBIENT_PRESSURE_TOKEN
from ..utils import as_float
from .models import CalibrationPoint
from .point_parser import PointParser, TemperatureGroup


@dataclass(frozen=True)
class RoutePlanner:
    """Shared route planning helpers for temperature groups and route runners."""

    config: AppConfig
    point_parser: PointParser

    def route_mode(self) -> str:
        route_mode = str(getattr(self.config.workflow, "route_mode", "h2o_then_co2") or "").strip().lower()
        if route_mode in {"h2o_only", "co2_only", "h2o_then_co2"}:
            return route_mode
        return "h2o_then_co2"

    def group_by_temperature(self, points: Iterable[CalibrationPoint]) -> list[TemperatureGroup]:
        return list(self.point_parser.group_by_temperature(list(points)))

    def water_first_temp_threshold(self) -> float:
        if bool(getattr(self.config.workflow, "water_first_all_temps", False)):
            return float("-inf")
        raw = getattr(self.config.workflow, "water_first_temp_gte", None)
        if raw is None:
            return 0.0
        return float(raw)

    def should_run_h2o(self, points: list[CalibrationPoint]) -> bool:
        if not points:
            return False
        if self.route_mode() == "co2_only":
            return False
        if float(points[0].temp_chamber_c) < 0.0:
            return False
        return any(point.is_h2o_point for point in points)

    def should_run_h2o_first(self, points: list[CalibrationPoint]) -> bool:
        if not self.should_run_h2o(points):
            return False
        return float(points[0].temp_chamber_c) >= self.water_first_temp_threshold()

    def route_sequence(self, points: list[CalibrationPoint]) -> list[str]:
        if not points:
            return []

        route_mode = self.route_mode()
        run_h2o = self.should_run_h2o(points)
        run_co2 = route_mode != "h2o_only" and any(
            not point.is_h2o_point and point.co2_ppm is not None for point in points
        )

        if route_mode == "co2_only":
            return ["co2"] if run_co2 else []
        if route_mode == "h2o_only":
            return ["h2o"] if run_h2o else []
        if run_h2o and run_co2:
            return ["h2o", "co2"] if self.should_run_h2o_first(points) else ["co2", "h2o"]
        if run_h2o:
            return ["h2o"]
        if run_co2:
            return ["co2"]
        return []

    def h2o_pressure_points(self, points: list[CalibrationPoint]) -> list[CalibrationPoint]:
        return self._pressure_reference_points(points)

    def co2_sources(self, points: list[CalibrationPoint]) -> list[CalibrationPoint]:
        selected: dict[tuple[int, str], CalibrationPoint] = {}
        skip_ppm = self._co2_skip_ppm_set()
        for point in points:
            if point.is_h2o_point:
                continue
            ppm = as_float(point.co2_ppm)
            if ppm is None:
                continue
            ppm_key = int(round(ppm))
            if ppm_key in skip_ppm:
                continue
            group = str(point.co2_group or "").strip().upper()
            key = (ppm_key, group)
            current = selected.get(key)
            if current is None:
                selected[key] = point
                continue
            if self._carry_forward_pressure_mode() and self._pressure_value(point) > self._pressure_value(current):
                selected[key] = point
        out = list(selected.values())
        out.sort(
            key=lambda item: (
                float(as_float(item.co2_ppm) or 0.0),
                str(item.co2_group or "").strip().upper(),
                -self._pressure_value(item),
            )
        )
        return out

    def co2_pressure_points(self, source: Optional[CalibrationPoint], points: list[CalibrationPoint]) -> list[CalibrationPoint]:
        pressure_points = self._pressure_reference_points(points)
        if source is not None:
            source_ppm = float(source.co2_ppm or 0)
            pressure_points = [p for p in pressure_points if abs(float(p.co2_ppm or 0) - source_ppm) < 0.5]
            if not pressure_points:
                pressure_points = self._pressure_reference_points(points)
        if source is None or not pressure_points or not self._carry_forward_pressure_mode():
            return pressure_points

        # Sparse carry-forward compare tables encode the top CO2 span as a single
        # high-pressure anchor. Keep only the primary reference pressure so the
        # expanded V2 route matches the historical V1 tag set.
        source_ppm = self._as_int(source.co2_ppm)
        source_pressure = self._as_int(source.target_pressure_hpa)
        ppm_values = [
            self._as_int(point.co2_ppm)
            for point in points
            if not point.is_h2o_point and self._as_int(point.co2_ppm) is not None
        ]
        source_pressures = [
            self._as_int(point.target_pressure_hpa)
            for point in points
            if not point.is_h2o_point and self._as_int(point.target_pressure_hpa) is not None
        ]
        if (
            source_ppm is not None
            and source_pressure is not None
            and len(ppm_values) >= 3
            and len(source_pressures) >= 3
            and source_ppm == max(ppm_values)
            and source_pressure == min(source_pressures)
        ):
            sealed_points = [point for point in pressure_points if not point.is_ambient_pressure_point]
            return sealed_points[:1] or pressure_points[:1]
        return pressure_points

    def group_h2o_points(self, points: list[CalibrationPoint]) -> list[list[CalibrationPoint]]:
        groups: list[list[CalibrationPoint]] = []
        current: list[CalibrationPoint] = []
        current_key: Optional[tuple[Optional[float], Optional[float]]] = None
        for point in points:
            if not point.is_h2o_point:
                continue
            key = (as_float(point.hgen_temp_c), as_float(point.hgen_rh_pct))
            if current and key != current_key:
                groups.append(current)
                current = []
            current.append(point)
            current_key = key
        if current:
            groups.append(current)
        return groups

    def progress_point_keys(self, points: Iterable[CalibrationPoint]) -> list[str]:
        planned_keys: list[str] = []
        for group in self.group_by_temperature(list(points)):
            group_points = list(group.points)
            h2o_points = [point for point in group_points if point.is_h2o_point]

            for route_name in self.route_sequence(group_points):
                if route_name == "h2o":
                    pressure_points = self.h2o_pressure_points(group_points)
                    for h2o_group in self.group_h2o_points(h2o_points):
                        if not h2o_group:
                            continue
                        lead = h2o_group[0]
                        for pressure_point in pressure_points or h2o_group:
                            sample_point = self.build_h2o_pressure_point(lead, pressure_point)
                            planned_keys.append(self.progress_point_key(sample_point))
                    continue

                if route_name == "co2":
                    for source_point in self.co2_sources(group_points):
                        pressure_points = self.co2_pressure_points(source_point, group_points) or [source_point]
                        for pressure_point in pressure_points:
                            sample_point = self.build_co2_pressure_point(source_point, pressure_point)
                            planned_keys.append(self.progress_point_key(sample_point))

        deduped: list[str] = []
        seen: set[str] = set()
        for key in planned_keys:
            if key in seen:
                continue
            seen.add(key)
            deduped.append(key)
        return deduped

    def progress_point_key(self, point: CalibrationPoint, *, point_tag: str = "") -> str:
        route = str(point.route or "").strip().lower()
        tag = str(point_tag or "").strip()
        if not tag:
            if route == "h2o":
                tag = self.h2o_point_tag(point)
            elif route == "co2":
                tag = self.co2_point_tag(point)
        if tag:
            return f"{route}:{tag}"
        return f"{route}:{int(point.index)}"

    @staticmethod
    def build_co2_pressure_point(source_point: CalibrationPoint, pressure_point: CalibrationPoint) -> CalibrationPoint:
        is_ambient = bool(pressure_point.is_ambient_pressure_point)
        return CalibrationPoint(
            index=source_point.index if is_ambient else pressure_point.index,
            temperature_c=source_point.temperature_c,
            co2_ppm=source_point.co2_ppm,
            humidity_pct=None,
            pressure_hpa=None if is_ambient else pressure_point.target_pressure_hpa,
            route="co2",
            co2_group=source_point.co2_group,
            cylinder_nominal_ppm=source_point.cylinder_nominal_ppm,
            pressure_mode=pressure_point.effective_pressure_mode,
            pressure_target_label=pressure_point.pressure_display_label,
            pressure_selection_token=pressure_point.pressure_selection_token_value,
        )

    @staticmethod
    def build_h2o_pressure_point(source_point: CalibrationPoint, pressure_point: CalibrationPoint) -> CalibrationPoint:
        is_ambient = bool(pressure_point.is_ambient_pressure_point)
        return CalibrationPoint(
            index=source_point.index if is_ambient else pressure_point.index,
            temperature_c=source_point.temperature_c,
            co2_ppm=None,
            humidity_pct=source_point.hgen_rh_pct,
            pressure_hpa=None if is_ambient else pressure_point.target_pressure_hpa,
            route="h2o",
            humidity_generator_temp_c=source_point.hgen_temp_c,
            dewpoint_c=source_point.dewpoint_c,
            h2o_mmol=source_point.h2o_mmol,
            raw_h2o=source_point.raw_h2o,
            co2_group=source_point.co2_group,
            cylinder_nominal_ppm=source_point.cylinder_nominal_ppm,
            pressure_mode=pressure_point.effective_pressure_mode,
            pressure_target_label=pressure_point.pressure_display_label,
            pressure_selection_token=pressure_point.pressure_selection_token_value,
        )

    @staticmethod
    def co2_point_tag(point: CalibrationPoint) -> str:
        ppm = RoutePlanner._as_int(point.co2_ppm) or 0
        group = str(point.co2_group or "A").strip().upper() or "A"
        pressure_text = (
            AMBIENT_PRESSURE_TOKEN
            if point.is_ambient_pressure_point
            else f"{RoutePlanner._as_int(point.target_pressure_hpa) or 0}hpa"
        )
        return f"co2_group{group.lower()}_{ppm}ppm_{pressure_text}"

    @staticmethod
    def h2o_point_tag(point: CalibrationPoint) -> str:
        hgen_temp = RoutePlanner._as_int(point.hgen_temp_c) or 0
        hgen_rh = RoutePlanner._as_int(point.hgen_rh_pct) or 0
        pressure_text = (
            AMBIENT_PRESSURE_TOKEN
            if point.is_ambient_pressure_point
            else f"{RoutePlanner._as_int(point.target_pressure_hpa) or 0}hpa"
        )
        return f"h2o_{hgen_temp}c_{hgen_rh}rh_{pressure_text}"

    def _co2_skip_ppm_set(self) -> set[int]:
        raw = getattr(self.config.workflow, "skip_co2_ppm", [])
        if not isinstance(raw, list):
            raw = [raw]
        out: set[int] = set()
        for item in raw:
            value = self._as_int(item)
            if value is not None:
                out.add(value)
        return out

    def _carry_forward_pressure_mode(self) -> bool:
        policy = str(getattr(self.config.workflow, "missing_pressure_policy", "require") or "").strip().lower()
        return policy == "carry_forward"

    @staticmethod
    def _pressure_reference_points(points: list[CalibrationPoint]) -> list[CalibrationPoint]:
        ambient_refs: list[CalibrationPoint] = []
        numeric_refs: list[CalibrationPoint] = []
        seen_numeric: set[float] = set()
        ambient_seen = False
        for point in points:
            if point.is_ambient_pressure_point:
                if not ambient_seen:
                    ambient_refs.append(point)
                    ambient_seen = True
                continue
            pressure = as_float(point.target_pressure_hpa)
            if pressure is None:
                continue
            key = round(float(pressure), 6)
            if key in seen_numeric:
                continue
            seen_numeric.add(key)
            numeric_refs.append(point)
        numeric_refs.sort(key=lambda item: float(as_float(item.target_pressure_hpa) or 0.0), reverse=True)
        return ambient_refs + numeric_refs

    @staticmethod
    def _pressure_value(point: CalibrationPoint) -> float:
        return float(as_float(point.target_pressure_hpa) or 0.0)

    @staticmethod
    def _as_int(value: object) -> Optional[int]:
        if value is None:
            return None
        try:
            return int(value)
        except Exception:
            try:
                return int(float(value))
            except Exception:
                return None
