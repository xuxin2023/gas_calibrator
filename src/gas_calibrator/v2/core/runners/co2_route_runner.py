from __future__ import annotations

import time
from typing import Any, Sequence

from ...exceptions import WorkflowInterruptedError, WorkflowValidationError
from ..event_bus import EventType
from ..models import CalibrationPhase, CalibrationPoint
from .route_run_result import RouteRunResult


class Co2RouteRunner:
    """Direct CO2 route flow using concrete services and route planner helpers."""

    def __init__(self, service: Any, point: CalibrationPoint, pressure_points: Sequence[CalibrationPoint]):
        self.service = service
        self.point = point
        self.pressure_points = list(pressure_points)

    def execute(self) -> RouteRunResult:
        point = self.point
        phase = "co2"
        route_context = self.service.route_context
        special_zero_flush = self._special_zero_flush_pending(point)
        completed_points: list[CalibrationPoint] = []
        completed_point_indices: list[int] = []
        sampled_points: list[CalibrationPoint] = []
        sampled_point_indices: list[int] = []
        skipped_point_indices: list[int] = []
        pressure_refs = self.pressure_points or [point]
        expected_indices = [self.service.route_planner.build_co2_pressure_point(point, item).index for item in pressure_refs]
        route_context.enter(
            current_route="co2",
            current_phase=CalibrationPhase.CO2_ROUTE,
            current_point=point,
            source_point=point,
            active_point=point,
            point_tag="",
            retry=0,
            route_state={
                "source_point_index": point.index,
                "source_point_tag": self.service.route_planner.co2_point_tag(point),
                "pressure_indices": [item.index for item in self.pressure_points],
            },
        )
        self.service.event_bus.publish(EventType.POINT_STARTED, {"point": point, "route": phase})
        try:
            self.service.status_service.check_stop()
            self.service.status_service.update_status(
                phase=CalibrationPhase.CO2_ROUTE,
                current_point=point,
                message=f"CO2 route for point {point.index}",
            )

            temperature_wait = self.service.temperature_control_service.set_temperature_for_point(point, phase=phase)
            self.service.status_service.record_route_trace(
                action="wait_temperature",
                route=phase,
                point=point,
                target={"temp_c": point.temp_chamber_c},
                actual={"temp_c": getattr(temperature_wait, "final_temp_c", None)},
                result="ok"
                if bool(getattr(temperature_wait, "ok", False))
                else ("timeout" if bool(getattr(temperature_wait, "timed_out", False)) else "fail"),
                message=str(getattr(temperature_wait, "error", "") or "Temperature wait complete"),
            )
            if not temperature_wait.ok:
                self.service.status_service.log(f"CO2 row {point.index} skipped: chamber did not stabilize")
                skipped_point_indices.extend(expected_indices)
                return RouteRunResult(
                    success=False,
                    skipped_point_indices=skipped_point_indices,
                    error="CO2 chamber did not stabilize",
                )
            self.service.event_bus.publish(EventType.STABILITY_PASSED, {"point": point, "stability_type": "temperature"})
            self.service.temperature_control_service.capture_temperature_calibration_snapshot(point, route_type=phase)
            self.service._record_workflow_timing("route_baseline_start", "start", stage="route_baseline", point=point)
            self.service.valve_routing_service.set_co2_route_baseline(reason="before CO2 route conditioning")
            self.service._record_workflow_timing("route_baseline_end", "end", stage="route_baseline", point=point)
            self.service.status_service.log("Pressure controller kept at atmosphere for CO2 route conditioning")
            begin_conditioning = getattr(self.service, "_begin_a2_co2_route_conditioning_at_atmosphere", None)
            if callable(begin_conditioning):
                begin_conditioning(point, pressure_refs)
            confirm_conditioning_vent = getattr(self.service, "_confirm_a2_co2_conditioning_before_route_open", None)
            if callable(confirm_conditioning_vent):
                confirm_conditioning_vent(point)
            high_pressure_first_point_mode = False
            self.service._record_workflow_timing("co2_route_open_start", "start", stage="co2_route_open", point=point)
            self.service.valve_routing_service.set_valves_for_co2(point)
            route_open_completed_monotonic_s = time.monotonic()
            setattr(self.service, "_a2_co2_route_open_monotonic_s", route_open_completed_monotonic_s)
            refresh_conditioning_vent = getattr(self.service, "_refresh_a2_co2_conditioning_after_route_open", None)
            if callable(refresh_conditioning_vent):
                refresh_conditioning_vent(point)
            route_open_pressure = None
            pressure_reader = getattr(getattr(self.service, "pressure_control_service", None), "_current_pressure", None)
            if callable(pressure_reader):
                try:
                    route_open_pressure = self.service._as_float(pressure_reader())
                except Exception:
                    route_open_pressure = None
            setattr(self.service, "_a2_co2_route_open_pressure_hpa", route_open_pressure)
            setattr(self.service, "_a2_preseal_pressure_rise_detected", False)
            setattr(self.service, "_a2_route_open_pressure_first_sample_recorded", False)
            route_open_state = {
                "high_pressure_first_point_mode": False,
                "co2_route_conditioning_at_atmosphere": bool(
                    getattr(self.service, "_a2_co2_route_conditioning_at_atmosphere_active", False)
                ),
            }
            stream_snapshot = getattr(
                getattr(self.service, "pressure_control_service", None),
                "digital_gauge_continuous_stream_snapshot",
                None,
            )
            if callable(stream_snapshot):
                route_open_state["digital_gauge_stream"] = stream_snapshot()
            self.service._record_workflow_timing(
                "co2_route_open_end",
                "end",
                stage="co2_route_open",
                point=point,
                pressure_hpa=route_open_pressure,
                route_state=route_open_state,
            )
            route_soak_ok = self._wait_route_soak_before_seal(point)
            route_soak_actual = dict(getattr(self.service, "_last_co2_route_dewpoint_gate_summary", {}) or {})
            end_conditioning = getattr(self.service, "_end_a2_co2_route_conditioning_at_atmosphere", None)
            if callable(end_conditioning):
                end_conditioning(point, route_soak_ok=route_soak_ok, route_soak_actual=route_soak_actual)
            self.service.status_service.record_route_trace(
                action="wait_route_soak",
                route=phase,
                point=point,
                actual=route_soak_actual,
                result="ok" if route_soak_ok else "fail",
                message="CO2 route soak before seal",
            )
            if not route_soak_ok:
                self.service.status_service.log(
                    f"CO2 row {point.index} skipped: route conditioning interrupted before sealing"
                )
                self.service.valve_routing_service.cleanup_co2_route(reason="after CO2 route soak interrupted")
                skipped_point_indices.extend(expected_indices)
                return RouteRunResult(
                    success=False,
                    skipped_point_indices=skipped_point_indices,
                    error="CO2 route soak interrupted",
                )
            if special_zero_flush:
                self._reassert_route_after_special_zero_flush(point)

            preseal_analyzer_gate = getattr(self.service, "_preseal_analyzer_gate_after_conditioning", None)
            if callable(preseal_analyzer_gate):
                preseal_analyzer_gate(point, route_soak_ok=route_soak_ok, route_soak_actual=route_soak_actual)
            elif bool(getattr(self.service, "_gas_route_dewpoint_gate_enabled", lambda: False)()):
                self.service.status_service.log("CO2 preseal dewpoint gate passed")
            else:
                self.service.status_service.log("CO2 preseal analyzer stability check skipped")
            prepare_high_pressure = getattr(self.service, "_prepare_a2_high_pressure_first_point_after_conditioning", None)
            if callable(prepare_high_pressure):
                prepare_high_pressure(point, pressure_refs)
            high_pressure_first_point_mode = bool(
                getattr(self.service, "_a2_high_pressure_first_point_mode_enabled", False)
            )
            if high_pressure_first_point_mode:
                self.service.status_service.log(
                    "A2 1100 hPa high-pressure first-point mode armed after CO2 route conditioning"
                )
            if not self.service.pressure_control_service.pressurize_and_hold(point, route=phase).ok:
                self._clear_active_post_h2o_zero_flush_flag()
                self.service.status_service.log(f"CO2 row {point.index} skipped: route sealing failed")
                self.service.valve_routing_service.cleanup_co2_route(reason="after CO2 pressure-seal failure")
                skipped_point_indices.extend(expected_indices)
                return RouteRunResult(
                    success=False,
                    skipped_point_indices=skipped_point_indices,
                    error="CO2 pressure seal failed",
                )

            retry_total = self._co2_pressure_retry_total()
            for pressure_point in pressure_refs:
                self.service.status_service.check_stop()
                sample_point = self.service.route_planner.build_co2_pressure_point(point, pressure_point)
                point_tag = self.service.route_planner.co2_point_tag(sample_point)
                route_context.update(
                    current_point=sample_point,
                    source_point=point,
                    active_point=sample_point,
                    point_tag=point_tag,
                    retry=0,
                    route_state={
                        "sample_point_index": sample_point.index,
                        "pressure_point_index": pressure_point.index,
                        "pressure_target_hpa": sample_point.target_pressure_hpa,
                    },
                )
                self.service.status_service.begin_point_timing(sample_point, phase=phase, point_tag=point_tag)
                self.service._record_workflow_timing(
                    "pressure_point_start",
                    "start",
                    stage="pressure_point",
                    point=sample_point,
                    target_pressure_hpa=sample_point.target_pressure_hpa,
                )
                pressure_ok = self.service.pressure_control_service.set_pressure_to_target(sample_point).ok
                retry_done = 0
                while not pressure_ok and retry_done < retry_total:
                    retry_done += 1
                    route_context.update(retry=retry_done, route_state={"retry": retry_done})
                    pressure_ok = self._retry_pressure_point_after_timeout(
                        point,
                        sample_point,
                        attempt=retry_done,
                        total=retry_total,
                    )
                if not pressure_ok:
                    self.service.status_service.log(
                        f"CO2 {sample_point.co2_ppm} ppm @ {sample_point.target_pressure_hpa} hPa skipped: "
                        f"pressure did not stabilize"
                    )
                    self.service.status_service.clear_point_timing(sample_point, phase=phase, point_tag=point_tag)
                    self.service._record_workflow_timing(
                        "pressure_point_end",
                        "warning",
                        stage="pressure_point",
                        point=sample_point,
                        target_pressure_hpa=sample_point.target_pressure_hpa,
                        decision="pressure_not_stable",
                    )
                    skipped_point_indices.append(sample_point.index)
                    continue
                self.service.event_bus.publish(EventType.STABILITY_PASSED, {"point": sample_point, "stability_type": "pressure"})
                if not self.service.pressure_control_service.wait_after_pressure_stable_before_sampling(sample_point).ok:
                    self.service.status_service.log(
                        f"CO2 {sample_point.co2_ppm} ppm @ {sample_point.target_pressure_hpa} hPa skipped: "
                        f"post-pressure hold before sampling interrupted"
                    )
                    self.service.status_service.clear_point_timing(sample_point, phase=phase, point_tag=point_tag)
                    self.service._record_workflow_timing(
                        "pressure_point_end",
                        "warning",
                        stage="pressure_point",
                        point=sample_point,
                        target_pressure_hpa=sample_point.target_pressure_hpa,
                        decision="wait_gate_interrupted",
                    )
                    skipped_point_indices.append(sample_point.index)
                    continue
                self.service.status_service.mark_point_stable_for_sampling(sample_point, phase=phase, point_tag=point_tag)
                self.service.status_service.update_status(
                    phase=CalibrationPhase.SAMPLING,
                    current_point=sample_point,
                    message=f"CO2 sampling point {sample_point.index}",
                )
                sample_count_expected, sample_interval_s = self.service.sampling_service.sampling_params(phase)
                sample_expected_max_s = max(5.0, float(sample_count_expected) * max(0.0, float(sample_interval_s)) + 30.0)
                self.service._record_workflow_timing(
                    "sample_start",
                    "start",
                    stage="sample",
                    point=sample_point,
                    target_pressure_hpa=sample_point.target_pressure_hpa,
                    expected_max_s=sample_expected_max_s,
                    sample_count=sample_count_expected,
                )
                self.service.status_service.record_route_trace(
                    action="sample_start",
                    route=phase,
                    point=sample_point,
                    point_tag=point_tag,
                    target={"pressure_hpa": sample_point.target_pressure_hpa, "co2_ppm": sample_point.co2_ppm},
                    result="ok",
                    message="CO2 sampling start",
                )
                results = self.service.sampling_service.sample_point(sample_point, phase=phase, point_tag=point_tag)
                if not results:
                    self.service._record_workflow_timing(
                        "sample_end",
                        "warning",
                        stage="sample",
                        point=sample_point,
                        target_pressure_hpa=sample_point.target_pressure_hpa,
                        expected_max_s=sample_expected_max_s,
                        sample_count=0,
                        decision="no_results",
                    )
                    self.service.status_service.record_route_trace(
                        action="sample_end",
                        route=phase,
                        point=sample_point,
                        point_tag=point_tag,
                        result="skip",
                        message="CO2 sampling returned no results",
                    )
                    skipped_point_indices.append(sample_point.index)
                    self.service.status_service.clear_point_timing(sample_point, phase=phase, point_tag=point_tag)
                    self.service._record_workflow_timing(
                        "pressure_point_end",
                        "warning",
                        stage="pressure_point",
                        point=sample_point,
                        target_pressure_hpa=sample_point.target_pressure_hpa,
                        decision="sample_no_results",
                    )
                    continue
                for result in results:
                    self.service.event_bus.publish(EventType.SAMPLE_COLLECTED, result)
                self.service._record_workflow_timing(
                    "sample_end",
                    "end",
                    stage="sample",
                    point=sample_point,
                    target_pressure_hpa=sample_point.target_pressure_hpa,
                    expected_max_s=sample_expected_max_s,
                    sample_count=len(results),
                    decision="ok",
                )
                self.service.status_service.record_route_trace(
                    action="sample_end",
                    route=phase,
                    point=sample_point,
                    point_tag=point_tag,
                    actual={"sample_count": len(results)},
                    result="ok",
                    message="CO2 sampling complete",
                )
                self.service.qc_service.run_point_qc(sample_point, phase=phase, point_tag=point_tag)
                sampled_points.append(sample_point)
                sampled_point_indices.append(sample_point.index)
                completed_points.append(sample_point)
                completed_point_indices.append(sample_point.index)
                self.service._record_workflow_timing(
                    "pressure_point_end",
                    "end",
                    stage="pressure_point",
                    point=sample_point,
                    target_pressure_hpa=sample_point.target_pressure_hpa,
                    decision="ok",
                )

            self.service.valve_routing_service.cleanup_co2_route(reason="after CO2 source complete")
            return RouteRunResult(
                success=bool(completed_point_indices) and not skipped_point_indices,
                completed_points=completed_points,
                completed_point_indices=completed_point_indices,
                sampled_points=sampled_points,
                sampled_point_indices=sampled_point_indices,
                skipped_point_indices=skipped_point_indices,
                error=None if completed_point_indices and not skipped_point_indices else "CO2 route completed with skipped points",
            )
        except WorkflowInterruptedError as exc:
            try:
                self.service.valve_routing_service.cleanup_co2_route(reason="after CO2 stop requested")
            except Exception:
                pass
            return RouteRunResult(
                success=False,
                completed_points=completed_points,
                completed_point_indices=completed_point_indices,
                sampled_points=sampled_points,
                sampled_point_indices=sampled_point_indices,
                skipped_point_indices=skipped_point_indices,
                stopped=True,
                error=str(exc),
            )
        except WorkflowValidationError as exc:
            try:
                self.service.valve_routing_service.cleanup_co2_route(reason="after CO2 route fail-closed")
            except Exception:
                pass
            skipped_point_indices.extend(index for index in expected_indices if index not in skipped_point_indices)
            return RouteRunResult(
                success=False,
                completed_points=completed_points,
                completed_point_indices=completed_point_indices,
                sampled_points=sampled_points,
                sampled_point_indices=sampled_point_indices,
                skipped_point_indices=skipped_point_indices,
                error=str(exc),
            )
        finally:
            route_context.clear()

    def _special_zero_flush_pending(self, point: CalibrationPoint) -> bool:
        has_pending = getattr(self.service, "_has_special_co2_zero_flush_pending", None)
        is_zero_point = getattr(self.service, "_is_zero_co2_point", None)
        if not callable(has_pending) or not callable(is_zero_point):
            return False
        try:
            return bool(has_pending()) and bool(is_zero_point(point))
        except Exception:
            return False

    def _reassert_route_after_special_zero_flush(self, point: CalibrationPoint) -> None:
        self.service.status_service.log(
            f"CO2 zero-gas flush complete; reassert route before pressure sealing (row {point.index})"
        )
        self.service.valve_routing_service.set_co2_route_baseline(reason="before CO2 pressure-seal recharge")
        self.service.valve_routing_service.set_valves_for_co2(point)

    def _clear_active_post_h2o_zero_flush_flag(self) -> None:
        run_state = getattr(self.service, "run_state", None)
        humidity_state = getattr(run_state, "humidity", None)
        if humidity_state is not None:
            try:
                humidity_state.active_post_h2o_co2_zero_flush = False
            except Exception:
                pass
        if hasattr(self.service, "_active_post_h2o_co2_zero_flush"):
            try:
                self.service._active_post_h2o_co2_zero_flush = False
            except Exception:
                pass

    def _wait_route_soak_before_seal(self, point: CalibrationPoint) -> bool:
        waiter = getattr(self.service, "_wait_co2_route_soak_before_seal", None)
        if callable(waiter):
            return bool(waiter(point))
        return True

    def _co2_pressure_retry_total(self) -> int:
        retry_total_reader = getattr(self.service, "_co2_pressure_timeout_reseal_retries", None)
        if callable(retry_total_reader):
            try:
                return max(0, int(retry_total_reader()))
            except Exception:
                return 0
        return max(0, self._cfg_int("workflow.pressure.co2_reseal_retry_count", 1))

    def _retry_pressure_point_after_timeout(
        self,
        source_point: CalibrationPoint,
        sample_point: CalibrationPoint,
        *,
        attempt: int,
        total: int,
    ) -> bool:
        retry_helper = getattr(self.service, "_retry_co2_pressure_point_after_timeout", None)
        if callable(retry_helper):
            return bool(
                retry_helper(
                    source_point,
                    sample_point,
                    attempt=attempt,
                    total=total,
                )
            )
        self.service.status_service.log(
            f"CO2 {sample_point.co2_ppm} ppm @ {sample_point.target_pressure_hpa} hPa timeout; "
            f"retry within sealed route {attempt}/{total}"
        )
        return bool(self.service.pressure_control_service.set_pressure_to_target(sample_point).ok)

    def _cfg_int(self, path: str, default: int) -> int:
        getter = getattr(self.service, "_cfg_get", None)
        if not callable(getter):
            return default
        try:
            return int(getter(path, default))
        except Exception:
            return default
