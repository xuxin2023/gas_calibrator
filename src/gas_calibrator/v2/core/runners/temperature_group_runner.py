from __future__ import annotations

from typing import Any, Sequence

from ...exceptions import WorkflowInterruptedError, WorkflowValidationError
from ..event_bus import EventType
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
                    all_pressure_points = list(self.service.route_planner.co2_pressure_points(source_point, self.points))
                    ambient_points = [p for p in all_pressure_points if getattr(p, "is_ambient_pressure_point", False)]
                    sealable_points = [p for p in all_pressure_points if not getattr(p, "is_ambient_pressure_point", False)]
                    effective_pressure_points = sealable_points if sealable_points else all_pressure_points
                    self.service.route_context.update(
                        active_point=source_point,
                        point_tag=self.service.route_planner.co2_point_tag(source_point),
                        retry=0,
                        route_state={
                            "active_subroute": "co2",
                            "source_point_index": source_point.index,
                            "pressure_indices": [point.index for point in all_pressure_points],
                        },
                    )
                    co2_runner = Co2RouteRunner(self.service, source_point, effective_pressure_points)
                    co2_runner.interpoint_flush()
                    result = co2_runner.execute()
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
                    for amb_point in ambient_points:
                        sampling_service = getattr(self.service, "sampling_service", None)
                        if sampling_service is None:
                            continue
                        pressure_service = getattr(self.service, "pressure_control_service", None)
                        if pressure_service is not None:
                            pressure_service.set_pressure_controller_vent(
                                True, reason="ambient point sampling"
                            )
                        self.service.status_service.check_stop()
                        sample_point = self.service.route_planner.build_co2_pressure_point(source_point, amb_point)
                        point_tag = self.service.route_planner.co2_point_tag(sample_point)
                        self.service.status_service.begin_point_timing(sample_point, phase="co2", point_tag=point_tag)
                        self.service.status_service.update_status(
                            phase=CalibrationPhase.SAMPLING,
                            current_point=sample_point,
                            message=f"CO2 ambient sampling point {sample_point.index}",
                        )
                        self.service.status_service.record_route_trace(
                            action="sample_start",
                            route="co2",
                            point=sample_point,
                            point_tag=point_tag,
                            target={"pressure_hpa": None, "co2_ppm": sample_point.co2_ppm, "ambient": True},
                            result="ok",
                            message="CO2 ambient sampling start (vent open)",
                        )
                        results = sampling_service.sample_point(sample_point, phase="co2", point_tag=point_tag)
                        if results:
                            event_bus = getattr(self.service, "event_bus", None)
                            if event_bus is not None:
                                for r in results:
                                    event_bus.publish(EventType.SAMPLE_COLLECTED, r)
                            qc_service = getattr(self.service, "qc_service", None)
                            if qc_service is not None:
                                qc_service.run_point_qc(sample_point, phase="co2", point_tag=point_tag)
                            self.service.status_service.record_route_trace(
                                action="sample_end",
                                route="co2",
                                point=sample_point,
                                point_tag=point_tag,
                                actual={"sample_count": len(results), "ambient": True},
                                result="ok",
                                message="CO2 ambient sampling complete",
                            )
                            completed_keys_key = self.service.route_planner.progress_point_key(sample_point, point_tag=point_tag)
                            if completed_keys_key not in completed_keys:
                                completed_keys.add(completed_keys_key)
                                self.service.status_service.mark_point_completed(sample_point, point_tag=point_tag)
                        else:
                            self.service.status_service.record_route_trace(
                                action="sample_end",
                                route="co2",
                                point=sample_point,
                                point_tag=point_tag,
                                result="skip",
                                message="CO2 ambient sampling returned no results",
                            )
                        self.service.status_service.clear_point_timing(sample_point, phase="co2", point_tag=point_tag)

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
