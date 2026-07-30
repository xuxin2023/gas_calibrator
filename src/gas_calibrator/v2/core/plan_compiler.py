from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass, field
from typing import Any, Optional

from gas_calibrator.validation.simulation.domain import ModeProfile
from gas_calibrator.validation.simulation.plan_models import (
    AnalyzerSetupSpec as _AnalyzerSetupSpec,
    CalibrationPlanProfile as _CalibrationPlanProfile,
)
from gas_calibrator.validation.simulation.point_parser import (
    LegacyExcelPointLoader,
    PointParser,
)
from gas_calibrator.validation.simulation.point_preparation import (
    prepare_points_for_execution as _prepare_points_for_execution,
)
from gas_calibrator.validation.simulation.plan_preview import (
    build_preview_rows as _build_preview_rows,
)
from gas_calibrator.validation.simulation.plan_rows import (
    build_source_rows as _build_source_rows,
    expand_runtime_rows as _expand_runtime_rows,
    preview_points_in_execution_order as _preview_points_in_execution_order,
    rows_to_points as _rows_to_points,
)
from gas_calibrator.validation.simulation.route_planner import RoutePlanner

from ..config.models import AppConfig
from ..export.product_report_plan import build_product_report_manifest
from .models import CalibrationPoint


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
        return _build_preview_rows(
            self.preview_points,
            runtime_rows=self.runtime_rows,
        )


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

    def compile(self, profile: _CalibrationPlanProfile) -> CompiledPlan:
        effective_config = self._effective_config(profile)
        mode_profile = ModeProfile.from_value(getattr(profile, "mode_profile", None))
        analyzer_setup = _AnalyzerSetupSpec.from_dict(getattr(profile, "analyzer_setup", None).to_dict() if isinstance(getattr(profile, "analyzer_setup", None), _AnalyzerSetupSpec) else getattr(profile, "analyzer_setup", None))
        report_manifest = build_product_report_manifest(
            run_mode=str(getattr(effective_config.workflow, "run_mode", "auto_calibration") or "auto_calibration"),
            route_mode=str(getattr(effective_config.workflow, "route_mode", "h2o_then_co2") or "h2o_then_co2"),
        )
        source_rows = _build_source_rows(
            profile,
            selected_temps_c=getattr(
                effective_config.workflow,
                "selected_temps_c",
                None,
            ),
            selected_pressure_points=getattr(
                effective_config.workflow,
                "selected_pressure_points",
                None,
            ),
            skip_co2_ppm=getattr(
                effective_config.workflow,
                "skip_co2_ppm",
                None,
            ),
            h2o_carry_forward=bool(
                getattr(
                    effective_config.workflow,
                    "h2o_carry_forward",
                    False,
                )
            ),
        )
        runtime_rows = _expand_runtime_rows(
            source_rows,
            h2o_carry_forward=bool(
                getattr(
                    effective_config.workflow,
                    "h2o_carry_forward",
                    False,
                )
            ),
        )
        points = _rows_to_points(
            runtime_rows,
            point_parser=self.point_parser,
        )
        planner = RoutePlanner(effective_config, self.point_parser)
        prepared_points = _prepare_points_for_execution(
            points,
            selected_temps_c=getattr(effective_config.workflow, "selected_temps_c", None),
            temperature_descending=bool(getattr(effective_config.workflow, "temperature_descending", True)),
            route_planner=planner,
            point_parser=self.point_parser,
        )
        preview_points = _preview_points_in_execution_order(
            prepared_points,
            route_planner=planner,
        )
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

    def preview(self, profile: _CalibrationPlanProfile) -> list[dict[str, Any]]:
        return self.compile(profile).preview_rows()

    def _effective_config(self, profile: _CalibrationPlanProfile) -> AppConfig:
        config = deepcopy(self.config)
        workflow = config.workflow
        ordering = profile.ordering
        mode_profile = ModeProfile.from_value(getattr(profile, "mode_profile", None))
        analyzer_setup = _AnalyzerSetupSpec.from_dict(getattr(profile, "analyzer_setup", None).to_dict() if isinstance(getattr(profile, "analyzer_setup", None), _AnalyzerSetupSpec) else getattr(profile, "analyzer_setup", None))
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
