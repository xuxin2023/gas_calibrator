from __future__ import annotations

import time
from typing import Any, Optional, Sequence

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
                source_list = self.service.route_planner.co2_sources(self.points)
                prev_ppm: Optional[float] = None
                for source_point in source_list:
                    current_ppm = float(source_point.co2_ppm or 0)
                    if prev_ppm is not None and abs(current_ppm - prev_ppm) > 0.5:
                        self._wait_co2_stable(current_ppm, timeout_s=120)
                    prev_ppm = current_ppm
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

    def _read_co2_ppm_stabilize(self) -> Optional[float]:
        dm = getattr(self.service, "device_manager", None)
        if dm is None:
            return None
        values: list[float] = []
        for logical_id in range(4):
            device = dm.get_device(f"gas_analyzer_{logical_id}")
            if device is None:
                continue
            for method_name in ("read_latest_data", "read_data_passive", "read"):
                fn = getattr(device, method_name, None)
                if not callable(fn):
                    continue
                try:
                    raw = str(fn()).strip()
                    parts = raw.split(",")
                    if len(parts) >= 3:
                        ppm = float(parts[2].strip())
                        if 0.0 <= ppm <= 5000.0:
                            values.append(ppm)
                            break
                except (ValueError, Exception):
                    continue
        return (sum(values) / len(values)) if values else None

    def _wait_co2_stable(self, target_ppm: float, timeout_s: float = 120.0) -> None:
        tol = 20.0 if target_ppm < 10.0 else 30.0
        deadline = time.monotonic() + timeout_s
        log = self.service.status_service.log
        log(f"CO2 wait-stable start: target={target_ppm:.0f} ppm, tol={tol:.0f} ppm, timeout={timeout_s:.0f}s")
        readings: list[float] = []
        window = int(self.service._cfg_get("co2.stabilize_window_samples", 3))
        poll_s = float(self.service._cfg_get("co2.stabilize_poll_s", 10.0))
        cycle = 0
        while time.monotonic() < deadline:
            co2_val = self._read_co2_ppm_stabilize()
            if co2_val is not None:
                readings.append(co2_val)
                if len(readings) > window:
                    readings.pop(0)
            cycle += 1
            elapsed = time.monotonic() + timeout_s - deadline
            co2_display = f"{co2_val:.1f}" if co2_val is not None else "N/A"
            log(f"CO2 wait-stable cycle {cycle} ({elapsed:.0f}s/{timeout_s:.0f}s): CO2={co2_display} ppm")
            if co2_val is not None and abs(co2_val - target_ppm) <= tol:
                log(f"CO2 wait-stable passed at cycle {cycle}: {co2_val:.1f} ppm within {tol:.0f} ppm of target")
                return
            if len(readings) >= window and co2_val is not None:
                span = max(readings) - min(readings)
                if span <= tol and abs(co2_val - target_ppm) <= tol * 2:
                    log(f"CO2 wait-stable passed at cycle {cycle}: span={span:.1f} ppm, last={co2_val:.1f} ppm")
                    return
            time.sleep(poll_s)
        log(f"CO2 wait-stable timeout after {timeout_s:.0f}s, proceeding")

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
