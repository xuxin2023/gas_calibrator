from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass, field
from typing import Any, Optional

from ..config import AppConfig
from ..domain.mode_models import ModeProfile
from ..domain.plan_models import (
    AnalyzerSetupSpec,
    CalibrationPlanProfile,
    GasPointSpec,
    HumiditySpec,
    PressureSpec,
    TemperatureSpec,
)
from ..domain.pressure_selection import AMBIENT_PRESSURE_LABEL, AMBIENT_PRESSURE_TOKEN, pressure_selection_key
from ..export.product_report_plan import build_product_report_manifest
from .calibration_service import prepare_points_for_execution
from .models import CalibrationPoint
from .point_parser import LegacyExcelPointLoader, PointParser
from .route_planner import RoutePlanner


@dataclass(frozen=True)
class CompiledPlan:
    profile_name: str
    source_rows: list[dict[str, Any]] = field(default_factory=list)
    runtime_rows: list[dict[str, Any]] = field(default_factory=list)
    points: list[CalibrationPoint] = field(default_factory=list)
    preview_points: list[CalibrationPoint] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_runtime_payload(self) -> dict[str, Any]:
        metadata = dict(self.metadata)
        return {
            "points": [dict(row) for row in self.runtime_rows],
            "profile_name": self.profile_name,
            "profile_version": metadata.get("profile_version", "1.0"),
            "run_mode": metadata.get("run_mode", "auto_calibration"),
            "route_mode": metadata.get("route_mode", "h2o_then_co2"),
            "formal_calibration_report": bool(metadata.get("formal_calibration_report", True)),
            "report_family": metadata.get("report_family"),
            "report_templates": dict(metadata.get("report_templates") or {}),
            "analyzer_setup": dict(metadata.get("analyzer_setup") or {}),
        }

    def preview_rows(self) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for sequence, point in enumerate(self.preview_points, start=1):
            source_row = self._match_runtime_row(point)
            rows.append(
                {
                    "sequence": sequence,
                    "index": point.index,
                    "temperature_c": float(point.temp_chamber_c),
                    "route": str(point.route),
                    "co2_ppm": point.co2_ppm,
                    "humidity_pct": point.humidity_pct,
                    "humidity_generator_temp_c": point.humidity_generator_temp_c,
                    "dewpoint_c": point.dewpoint_c,
                    "pressure_hpa": point.target_pressure_hpa,
                    "pressure_mode": point.effective_pressure_mode,
                    "pressure_target_label": point.pressure_display_label,
                    "pressure_selection_token": point.pressure_selection_token_value,
                    "co2_group": point.co2_group,
                    "cylinder_nominal_ppm": source_row.get("cylinder_nominal_ppm"),
                }
            )
        return rows

    def _match_runtime_row(self, point: CalibrationPoint) -> dict[str, Any]:
        for row in self.runtime_rows:
            if str(row.get("route", "")).strip().lower() != str(point.route).strip().lower():
                continue
            if float(row.get("temperature", 0.0)) != float(point.temp_chamber_c):
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
                if str(row.get("co2_group", "")).strip().upper() != str(point.co2_group or "").strip().upper():
                    continue
            return dict(row)
        return {}


class PlanCompiler:
    """Compile editable plan profiles into standard V2 point rows and preview points."""

    def __init__(
        self,
        config: Optional[AppConfig] = None,
        *,
        point_parser: Optional[PointParser] = None,
    ) -> None:
        self.config = deepcopy(config or AppConfig.from_dict({}))
        self.point_parser = point_parser or PointParser(
            legacy_excel_loader=LegacyExcelPointLoader(
                missing_pressure_policy=str(getattr(self.config.workflow, "missing_pressure_policy", "require") or "require"),
                carry_forward_h2o=bool(getattr(self.config.workflow, "h2o_carry_forward", False)),
            )
        )

    def compile(self, profile: CalibrationPlanProfile) -> CompiledPlan:
        effective_config = self._effective_config(profile)
        mode_profile = ModeProfile.from_value(getattr(profile, "mode_profile", None))
        analyzer_setup = AnalyzerSetupSpec.from_dict(getattr(profile, "analyzer_setup", None).to_dict() if isinstance(getattr(profile, "analyzer_setup", None), AnalyzerSetupSpec) else getattr(profile, "analyzer_setup", None))
        report_manifest = build_product_report_manifest(
            run_mode=str(getattr(effective_config.workflow, "run_mode", "auto_calibration") or "auto_calibration"),
            route_mode=str(getattr(effective_config.workflow, "route_mode", "h2o_then_co2") or "h2o_then_co2"),
        )
        source_rows = self._build_source_rows(profile, effective_config=effective_config)
        runtime_rows = self._expand_runtime_rows(source_rows, effective_config=effective_config)
        points = self._rows_to_points(runtime_rows)
        planner = RoutePlanner(effective_config, self.point_parser)
        prepared_points = prepare_points_for_execution(
            points,
            selected_temps_c=getattr(effective_config.workflow, "selected_temps_c", None),
            temperature_descending=bool(getattr(effective_config.workflow, "temperature_descending", True)),
            route_planner=planner,
            point_parser=self.point_parser,
        )
        preview_points = self._preview_points_in_execution_order(prepared_points, route_planner=planner)
        return CompiledPlan(
            profile_name=profile.name,
            source_rows=source_rows,
            runtime_rows=runtime_rows,
            points=prepared_points,
            preview_points=preview_points,
            metadata={
                "selected_temps_c": list(getattr(effective_config.workflow, "selected_temps_c", []) or []),
                "selected_pressure_points": list(getattr(effective_config.workflow, "selected_pressure_points", []) or []),
                "temperature_descending": bool(getattr(effective_config.workflow, "temperature_descending", True)),
                "skip_co2_ppm": list(getattr(effective_config.workflow, "skip_co2_ppm", []) or []),
                "profile_version": str(getattr(profile, "profile_version", "1.0") or "1.0"),
                "run_mode": str(getattr(effective_config.workflow, "run_mode", "auto_calibration") or "auto_calibration"),
                "route_mode": str(getattr(effective_config.workflow, "route_mode", "h2o_then_co2") or "h2o_then_co2"),
                "formal_calibration_report": mode_profile.formal_report_enabled(),
                "report_family": str(report_manifest.get("report_family", "") or ""),
                "report_templates": report_manifest,
                "analyzer_setup": analyzer_setup.to_dict(),
                "water_first_all_temps": bool(getattr(effective_config.workflow, "water_first_all_temps", False)),
                "water_first_temp_gte": getattr(effective_config.workflow, "water_first_temp_gte", None),
                "h2o_carry_forward": bool(getattr(effective_config.workflow, "h2o_carry_forward", False)),
                "source_row_count": len(source_rows),
                "runtime_row_count": len(runtime_rows),
                "prepared_point_count": len(prepared_points),
                "preview_point_count": len(preview_points),
            },
        )

    def preview(self, profile: CalibrationPlanProfile) -> list[dict[str, Any]]:
        return self.compile(profile).preview_rows()

    def _effective_config(self, profile: CalibrationPlanProfile) -> AppConfig:
        config = deepcopy(self.config)
        workflow = config.workflow
        ordering = profile.ordering
        mode_profile = ModeProfile.from_value(getattr(profile, "mode_profile", None))
        analyzer_setup = AnalyzerSetupSpec.from_dict(getattr(profile, "analyzer_setup", None).to_dict() if isinstance(getattr(profile, "analyzer_setup", None), AnalyzerSetupSpec) else getattr(profile, "analyzer_setup", None))
        workflow.run_mode = mode_profile.run_mode.value
        workflow.route_mode = mode_profile.effective_route_mode(
            str(getattr(workflow, "route_mode", "h2o_then_co2") or "h2o_then_co2")
        )
        workflow.analyzer_setup = analyzer_setup.to_dict()
        workflow.selected_temps_c = (
            list(ordering.selected_temps_c)
            if ordering.selected_temps_c
            else list(getattr(workflow, "selected_temps_c", []) or [])
        )
        workflow.selected_pressure_points = (
            list(ordering.selected_pressure_points)
            if ordering.selected_pressure_points
            else list(getattr(workflow, "selected_pressure_points", []) or [])
        )
        workflow.skip_co2_ppm = (
            list(ordering.skip_co2_ppm)
            if ordering.skip_co2_ppm
            else list(getattr(workflow, "skip_co2_ppm", []) or [])
        )
        workflow.temperature_descending = bool(ordering.temperature_descending)
        if bool(ordering.water_first) or bool(getattr(ordering, "water_first_explicit", False)):
            workflow.water_first_all_temps = bool(ordering.water_first)
        if ordering.water_first_temp_gte is not None or bool(getattr(ordering, "water_first_temp_gte_explicit", False)):
            workflow.water_first_temp_gte = ordering.water_first_temp_gte
        return config

    def _build_source_rows(
        self,
        profile: CalibrationPlanProfile,
        *,
        effective_config: AppConfig,
    ) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        selected_temps = {
            round(float(value), 9)
            for value in list(getattr(effective_config.workflow, "selected_temps_c", []) or [])
        }
        skip_co2 = {
            int(value)
            for value in list(getattr(effective_config.workflow, "skip_co2_ppm", []) or [])
        }
        carry_forward_h2o = bool(getattr(effective_config.workflow, "h2o_carry_forward", False))
        temperatures = self._ordered_temperatures(profile.temperatures, selected_temps=selected_temps)
        humidities = self._ordered_specs(profile.humidities)
        gas_points = [
            item
            for item in self._ordered_specs(profile.gas_points)
            if int(round(float(item.co2_ppm))) not in skip_co2
        ]
        pressures = self._ordered_pressure_specs(
            profile.pressures,
            selected_pressure_points=list(getattr(effective_config.workflow, "selected_pressure_points", []) or []),
        )
        pressure_values = pressures or [None]
        next_index = 1

        for temperature in temperatures:
            temperature_c = float(temperature.temperature_c)

            if temperature_c >= 0.0:
                for humidity in humidities:
                    for pressure_index, pressure in enumerate(pressure_values):
                        pressure_payload = self._pressure_row_payload(pressure)
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
                    pressure_payload = self._pressure_row_payload(pressure)
                    rows.append(
                        {
                            "index": next_index,
                            "temperature": temperature_c,
                            "route": "co2",
                            "co2_ppm": float(gas_point.co2_ppm),
                            "co2_group": str(getattr(gas_point, "co2_group", "A") or "A").strip().upper() or "A",
                            "cylinder_nominal_ppm": getattr(gas_point, "cylinder_nominal_ppm", None),
                            **pressure_payload,
                        }
                    )
                    next_index += 1

        return rows

    def _expand_runtime_rows(
        self,
        source_rows: list[dict[str, Any]],
        *,
        effective_config: AppConfig,
    ) -> list[dict[str, Any]]:
        if not bool(getattr(effective_config.workflow, "h2o_carry_forward", False)):
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
                "humidity_generator_temp_c": runtime_row.get("humidity_generator_temp_c"),
                "dewpoint_c": runtime_row.get("dewpoint_c"),
            }
            has_explicit_payload = any(value is not None for value in explicit_payload.values())
            if has_explicit_payload:
                current_h2o_context[temperature_c] = explicit_payload
            else:
                payload = current_h2o_context.get(temperature_c)
                if payload is not None:
                    for key, value in payload.items():
                        runtime_row[key] = value
            runtime_rows.append(runtime_row)
        return runtime_rows

    def _rows_to_points(self, rows: list[dict[str, Any]]) -> list[CalibrationPoint]:
        return [
            self.point_parser._row_to_point(index, row)
            for index, row in enumerate(rows, start=1)
        ]

    @staticmethod
    def _preview_points_in_execution_order(
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
                            ordered.append(route_planner.build_h2o_pressure_point(lead, pressure_point))
                    continue

                if route_name == "co2":
                    for source_point in route_planner.co2_sources(group_points):
                        pressure_points = route_planner.co2_pressure_points(source_point, group_points) or [source_point]
                        for pressure_point in pressure_points:
                            ordered.append(route_planner.build_co2_pressure_point(source_point, pressure_point))
        return ordered

    @staticmethod
    def _ordered_temperatures(
        specs: list[TemperatureSpec],
        *,
        selected_temps: set[float],
    ) -> list[TemperatureSpec]:
        ordered = PlanCompiler._ordered_specs(specs)
        if not selected_temps:
            return ordered
        return [
            item
            for item in ordered
            if round(float(item.temperature_c), 9) in selected_temps
        ]

    @staticmethod
    def _ordered_specs(
        specs: list[TemperatureSpec] | list[HumiditySpec] | list[GasPointSpec] | list[PressureSpec],
    ) -> list[Any]:
        decorated: list[tuple[int, int, Any]] = []
        for position, item in enumerate(specs):
            if not bool(getattr(item, "enabled", True)):
                continue
            raw_order = getattr(item, "order", None)
            order_value = int(raw_order) if raw_order is not None else 10_000 + position
            decorated.append((order_value, position, item))
        decorated.sort(key=lambda item: (item[0], item[1]))
        return [item for _, _, item in decorated]

    @staticmethod
    def _ordered_pressure_specs(
        specs: list[PressureSpec],
        *,
        selected_pressure_points: list[Any],
    ) -> list[PressureSpec]:
        ordered = PlanCompiler._ordered_specs(specs)
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
            key = selection if isinstance(selection, str) else round(float(selection), 6)
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

    @staticmethod
    def _pressure_row_payload(pressure: PressureSpec | None) -> dict[str, Any]:
        if pressure is None:
            return {
                "pressure_hpa": None,
                "pressure_mode": "",
                "pressure_target_label": None,
                "pressure_selection_token": "",
            }
        return {
            "pressure_hpa": None if pressure.is_ambient_pressure_point else pressure.pressure_hpa,
            "pressure_mode": pressure.effective_pressure_mode,
            "pressure_target_label": pressure.pressure_label(),
            "pressure_selection_token": pressure.pressure_selection_token or (
                AMBIENT_PRESSURE_TOKEN if pressure.is_ambient_pressure_point else ""
            ),
        }
