from __future__ import annotations

from typing import Any, Sequence

from ...exceptions import WorkflowInterruptedError, WorkflowValidationError
from ..models import CalibrationPhase, CalibrationPoint
from .co2_route_runner import Co2RouteRunner
from .h2o_route_runner import H2oRouteRunner


class TemperatureGroupRunner:
    """Temperature-group orchestration using route planner and route runners."""

    def __init__(self, service: Any, points: Sequence[CalibrationPoint], *, next_group: Sequence[CalibrationPoint] | None = None):
        self.service = service
        self.points = list(points)
        self.next_group = list(next_group or [])

    def execute(self) -> None:
        if not self.points:
            return

        lead = self.points[0]
        h2o_points = [point for point in self.points if point.is_h2o_point]
        co2_points = [point for point in self.points if not point.is_h2o_point and point.co2_ppm is not None]
        self.service.route_context.enter(
            current_route="temperature_group",
            current_phase=CalibrationPhase.TEMPERATURE_GROUP,
            current_point=lead,
            active_point=lead,
            point_tag="",
            retry=0,
            route_state={
                "temperature_c": float(lead.temp_chamber_c),
                "point_indices": [point.index for point in self.points],
                "h2o_indices": [point.index for point in h2o_points],
                "co2_indices": [point.index for point in co2_points],
            },
        )
        self.service.status_service.check_stop()
        self.service.status_service.update_status(
            phase=CalibrationPhase.TEMPERATURE_GROUP,
            current_point=lead,
            message=f"Temperature group {lead.temp_chamber_c:.2f} C",
        )
        self.service.analyzer_fleet_service.attempt_reenable_disabled_analyzers()
        points_by_index = {point.index: point for point in self.points}
        completed_keys: set[str] = set()
        route_failures: list[dict[str, Any]] = []

        preconditioned = False
        for route_name in self.service.route_planner.route_sequence(self.points):
            if route_name == "co2" and self.next_group and not preconditioned:
                self.service._precondition_next_temperature_humidity(self.next_group)
                self.service._precondition_next_temperature_chamber(self.next_group)
                preconditioned = True

            if route_name == "h2o":
                pressure_points = self.service.route_planner.h2o_pressure_points(self.points)
                for group in self.service.route_planner.group_h2o_points(h2o_points):
                    self.service.route_context.update(
                        active_point=group[0],
                        point_tag="",
                        retry=0,
                        route_state={
                            "active_subroute": "h2o",
                            "active_group_indices": [point.index for point in group],
                            "pressure_indices": [point.index for point in pressure_points],
                        },
                    )
                    result = H2oRouteRunner(self.service, group, pressure_points).execute()
                    self.service.route_context.enter(
                        current_route="temperature_group",
                        current_phase=CalibrationPhase.TEMPERATURE_GROUP,
                        current_point=lead,
                        active_point=lead,
                        point_tag="",
                        retry=0,
                        route_state={
                            "temperature_c": float(lead.temp_chamber_c),
                            "point_indices": [point.index for point in self.points],
                            "h2o_indices": [point.index for point in h2o_points],
                            "co2_indices": [point.index for point in co2_points],
                        },
                    )
                    if result.stopped:
                        self.service.route_context.clear()
                        raise WorkflowInterruptedError(reason=result.error or "Stop requested")
                    self._mark_completed_points(
                        result=result,
                        points_by_index=points_by_index,
                        completed_keys=completed_keys,
                    )
                    if not result.success:
                        route_failures.append(
                            {
                                "route": "h2o",
                                "source_point_index": group[0].index,
                                "completed_point_indices": list(result.completed_point_indices),
                                "sampled_point_indices": list(result.sampled_point_indices),
                                "skipped_point_indices": list(result.skipped_point_indices),
                                "error": result.error,
                            }
                        )
                continue

            if route_name == "co2":
                for source_point in self.service.route_planner.co2_sources(self.points):
                    pressure_points = self.service.route_planner.co2_pressure_points(source_point, self.points)
                    self.service.route_context.update(
                        active_point=source_point,
                        point_tag=self.service.route_planner.co2_point_tag(source_point),
                        retry=0,
                        route_state={
                            "active_subroute": "co2",
                            "source_point_index": source_point.index,
                            "pressure_indices": [point.index for point in pressure_points],
                        },
                    )
                    result = Co2RouteRunner(self.service, source_point, pressure_points).execute()
                    self.service.route_context.enter(
                        current_route="temperature_group",
                        current_phase=CalibrationPhase.TEMPERATURE_GROUP,
                        current_point=lead,
                        active_point=lead,
                        point_tag="",
                        retry=0,
                        route_state={
                            "temperature_c": float(lead.temp_chamber_c),
                            "point_indices": [point.index for point in self.points],
                            "h2o_indices": [point.index for point in h2o_points],
                            "co2_indices": [point.index for point in co2_points],
                        },
                    )
                    if result.stopped:
                        self.service.route_context.clear()
                        raise WorkflowInterruptedError(reason=result.error or "Stop requested")
                    self._mark_completed_points(
                        result=result,
                        points_by_index=points_by_index,
                        completed_keys=completed_keys,
                    )
                    if not result.success:
                        route_failures.append(
                            {
                                "route": "co2",
                                "source_point_index": source_point.index,
                                "completed_point_indices": list(result.completed_point_indices),
                                "sampled_point_indices": list(result.sampled_point_indices),
                                "skipped_point_indices": list(result.skipped_point_indices),
                                "error": result.error,
                            }
                        )

        if self.next_group and not preconditioned:
            self.service._precondition_next_temperature_humidity(self.next_group)
            self.service._precondition_next_temperature_chamber(self.next_group)

        self.service.route_context.clear()
        if route_failures:
            raise WorkflowValidationError(
                "Temperature group route execution failed",
                details={"route_failures": route_failures, "temperature_c": float(lead.temp_chamber_c)},
            )

    def _mark_completed_points(
        self,
        *,
        result: Any,
        points_by_index: dict[int, CalibrationPoint],
        completed_keys: set[str],
    ) -> None:
        completed_points = list(getattr(result, "completed_points", []) or [])
        if not completed_points:
            for point_index in getattr(result, "completed_point_indices", []) or []:
                point = points_by_index.get(point_index)
                if point is not None:
                    completed_points.append(point)

        for point in completed_points:
            point_tag = self._point_tag(point)
            point_key = self.service.route_planner.progress_point_key(point, point_tag=point_tag)
            if point_key in completed_keys:
                continue
            completed_keys.add(point_key)
            self.service.status_service.mark_point_completed(point, point_tag=point_tag)

    def _point_tag(self, point: CalibrationPoint) -> str:
        route = str(point.route or "").strip().lower()
        if route == "h2o":
            return self.service.route_planner.h2o_point_tag(point)
        if route == "co2":
            return self.service.route_planner.co2_point_tag(point)
        return ""
