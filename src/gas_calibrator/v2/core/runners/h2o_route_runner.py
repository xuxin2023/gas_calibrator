from __future__ import annotations

import threading
from typing import Any, Sequence

from ...exceptions import WorkflowInterruptedError
from ..event_bus import EventType
from ..models import CalibrationPhase, CalibrationPoint
from .route_run_result import RouteRunResult


class H2oRouteRunner:
    """Direct H2O route flow using concrete services instead of host _xxx proxies."""

    def __init__(self, service: Any, points: Sequence[CalibrationPoint], pressure_points: Sequence[CalibrationPoint]):
        self.service = service
        self.points = list(points)
        self.pressure_points = list(pressure_points)
        self._vent_keepalive_stop = threading.Event()
        self._vent_keepalive_thread = None  # type: threading.Thread | None

    def execute(self) -> RouteRunResult:
        if not self.points:
            return RouteRunResult(success=True)
        lead = self.points[0]
        phase = "h2o"
        route_context = self.service.route_context
        completed_points: list[CalibrationPoint] = []
        completed_point_indices: list[int] = []
        sampled_points: list[CalibrationPoint] = []
        sampled_point_indices: list[int] = []
        skipped_point_indices: list[int] = []
        effective_pressure_points = self.pressure_points or self.points
        expected_indices = [point.index for point in effective_pressure_points]
        route_context.enter(
            current_route="h2o",
            current_phase=CalibrationPhase.H2O_ROUTE,
            current_point=lead,
            route_state={
                "lead_point_index": lead.index,
                "point_indices": [point.index for point in self.points],
                "pressure_indices": [point.index for point in self.pressure_points],
            },
        )
        self.service.event_bus.publish(EventType.POINT_STARTED, {"point": lead, "route": phase})
        try:
            self.service.status_service.check_stop()
            self.service.status_service.update_status(
                phase=CalibrationPhase.H2O_ROUTE,
                current_point=lead,
                message=f"H2O route for point {lead.index}",
            )

            self.service.valve_routing_service.apply_route_baseline_valves()
            self.service.pressure_control_service.prepare_pressure_for_h2o(lead)
            self.service.humidity_generator_service.prepare_humidity_generator(lead)
            temperature_wait = self.service.temperature_control_service.set_temperature_for_point(lead, phase=phase)
            self.service.status_service.record_route_trace(
                action="wait_temperature",
                route=phase,
                point=lead,
                target={"temp_c": lead.temp_chamber_c},
                actual={"temp_c": getattr(temperature_wait, "final_temp_c", None)},
                result="ok"
                if bool(getattr(temperature_wait, "ok", False))
                else ("timeout" if bool(getattr(temperature_wait, "timed_out", False)) else "fail"),
                message=str(getattr(temperature_wait, "error", "") or "Temperature wait complete"),
            )
            if not temperature_wait.ok:
                self.service.valve_routing_service.cleanup_h2o_route(lead, reason="after H2O chamber timeout")
                skipped_point_indices.extend(expected_indices)
                return RouteRunResult(
                    success=False,
                    skipped_point_indices=skipped_point_indices,
                    error="H2O chamber did not stabilize",
                )
            self.service.event_bus.publish(EventType.STABILITY_PASSED, {"point": lead, "stability_type": "temperature"})
            humidity_wait = self.service.humidity_generator_service.wait_humidity_generator_stable(lead)
            self.service.status_service.record_route_trace(
                action="wait_humidity",
                route=phase,
                point=lead,
                target={
                    "temp_c": getattr(humidity_wait, "target_temp_c", None),
                    "humidity_pct": getattr(humidity_wait, "target_rh_pct", None),
                },
                actual={
                    "temp_c": getattr(humidity_wait, "final_temp_c", None),
                    "humidity_pct": getattr(humidity_wait, "final_rh_pct", None),
                },
                result="ok"
                if bool(getattr(humidity_wait, "ok", False))
                else ("timeout" if bool(getattr(humidity_wait, "timed_out", False)) else "fail"),
                message=str(getattr(humidity_wait, "error", "") or "Humidity wait complete"),
            )
            if not humidity_wait.ok and not self._continue_after_humidity_timeout(humidity_wait):
                self.service.valve_routing_service.cleanup_h2o_route(lead, reason="after H2O humidity timeout")
                skipped_point_indices.extend(expected_indices)
                return RouteRunResult(
                    success=False,
                    skipped_point_indices=skipped_point_indices,
                    error="H2O humidity wait failed",
                )
            if humidity_wait.ok:
                self.service.event_bus.publish(EventType.STABILITY_PASSED, {"point": lead, "stability_type": "humidity"})
            self.service.temperature_control_service.capture_temperature_calibration_snapshot(lead, route_type=phase)
            self._start_h2o_vent_keepalive()
            route_ready = self.service.dewpoint_alignment_service.open_h2o_route_and_wait_ready(lead)
            self.service.status_service.record_route_trace(
                action="wait_route_ready",
                route=phase,
                point=lead,
                result="ok" if route_ready else "timeout",
                message="H2O route open/ready check",
            )
            if not route_ready:
                self.service.valve_routing_service.cleanup_h2o_route(lead, reason="after H2O route timeout")
                skipped_point_indices.extend(expected_indices)
                return RouteRunResult(
                    success=False,
                    skipped_point_indices=skipped_point_indices,
                    error="H2O route readiness failed",
                )
            dewpoint_ok = self.service.dewpoint_alignment_service.wait_dewpoint_alignment_stable(lead)
            self.service.status_service.record_route_trace(
                action="wait_dewpoint",
                route=phase,
                point=lead,
                result="ok" if dewpoint_ok else "timeout",
                message="Dewpoint alignment wait complete" if dewpoint_ok else "Dewpoint alignment timed out",
            )
            if not dewpoint_ok:
                self.service.valve_routing_service.cleanup_h2o_route(lead, reason="after dewpoint alignment timeout")
                skipped_point_indices.extend(expected_indices)
                return RouteRunResult(
                    success=False,
                    skipped_point_indices=skipped_point_indices,
                    error="H2O dewpoint alignment failed",
                )
            self.service.event_bus.publish(EventType.STABILITY_PASSED, {"point": lead, "stability_type": "dewpoint"})
            self.service.valve_routing_service.mark_post_h2o_co2_zero_flush_pending()

            if effective_pressure_points and not getattr(effective_pressure_points[0], "is_ambient_pressure_point", False):
                ambient_ref = CalibrationPoint(
                    index=lead.index,
                    temperature_c=lead.temperature_c,
                    humidity_pct=lead.hgen_rh_pct,
                    pressure_hpa=None,
                    route="h2o",
                    humidity_generator_temp_c=lead.hgen_temp_c,
                    dewpoint_c=lead.dewpoint_c,
                    h2o_mmol=lead.h2o_mmol,
                    raw_h2o=lead.raw_h2o,
                    pressure_selection_token="ambient_open",
                )
                effective_pressure_points = [ambient_ref] + list(effective_pressure_points)

            first_point_is_ambient = bool(
                effective_pressure_points
                and getattr(effective_pressure_points[0], "is_ambient_pressure_point", False)
            )
            seal_deferred = False

            if not first_point_is_ambient:
                if not self.service.pressure_control_service.pressurize_and_hold(lead, route=phase).ok:
                    self.service.valve_routing_service.cleanup_h2o_route(lead, reason="after H2O pressure-seal failure")
                    skipped_point_indices.extend(expected_indices)
                    return RouteRunResult(
                        success=False,
                        skipped_point_indices=skipped_point_indices,
                        error="H2O pressure seal failed",
                    )
            else:
                seal_deferred = True
                self.service.pressure_control_service.set_pressure_controller_vent(
                    True, reason="H2O first point ambient: keep atmosphere open, seal deferred"
                )
                self.service.status_service.log("Pressure controller kept at atmosphere for H2O ambient first point (seal deferred)")
                self.service.status_service.record_route_trace(
                    action="pressure_skip",
                    route=phase,
                    point=lead,
                    target={"pressure_hpa": None, "vent_on": True},
                    result="skipped",
                    message="H2O first point ambient: seal/pressurize bypassed, vent stays open",
                )

            for pressure_point in effective_pressure_points:
                self.service.status_service.check_stop()
                sample_point = self.service.route_planner.build_h2o_pressure_point(lead, pressure_point)
                point_tag = self.service.route_planner.h2o_point_tag(sample_point)
                is_current_ambient = bool(getattr(sample_point, "is_ambient_pressure_point", False))
                route_context.update(current_point=sample_point, route_state={"sample_point_index": sample_point.index})
                self.service.status_service.begin_point_timing(sample_point, phase=phase, point_tag=point_tag)
                if is_current_ambient:
                    self.service.status_service.record_route_trace(
                        action="pressure_skip",
                        route=phase,
                        point=sample_point,
                        target={"pressure_hpa": None, "vent_on": True},
                        result="skipped",
                        message="H2O ambient pressure point: vent stays open, set_pressure bypassed, P3 ambient read used",
                    )
                    self.service.event_bus.publish(EventType.STABILITY_PASSED, {"point": sample_point, "stability_type": "pressure"})
                else:
                    if seal_deferred:
                        if not self.service.pressure_control_service.pressurize_and_hold(lead, route=phase).ok:
                            self.service.valve_routing_service.cleanup_h2o_route(lead, reason="after H2O deferred pressure-seal failure")
                            skipped_point_indices.extend(expected_indices)
                            return RouteRunResult(
                                success=False,
                                skipped_point_indices=skipped_point_indices,
                                error="H2O deferred pressure seal failed",
                            )
                        seal_deferred = False
                    if not self.service.pressure_control_service.set_pressure_to_target(sample_point).ok:
                        self.service.status_service.log(f"H2O row {sample_point.index} skipped: pressure did not stabilize")
                        self.service.status_service.clear_point_timing(sample_point, phase=phase, point_tag=point_tag)
                        skipped_point_indices.append(sample_point.index)
                        continue
                    self.service.event_bus.publish(EventType.STABILITY_PASSED, {"point": sample_point, "stability_type": "pressure"})
                    if not self.service.pressure_control_service.wait_after_pressure_stable_before_sampling(sample_point).ok:
                        self.service.status_service.log(
                            f"H2O row {sample_point.index} skipped: post-pressure hold before sampling interrupted"
                        )
                        self.service.status_service.clear_point_timing(sample_point, phase=phase, point_tag=point_tag)
                        skipped_point_indices.append(sample_point.index)
                        continue
                self.service.status_service.mark_point_stable_for_sampling(sample_point, phase=phase, point_tag=point_tag)
                self.service.status_service.update_status(
                    phase=CalibrationPhase.SAMPLING,
                    current_point=sample_point,
                    message=f"H2O sampling point {sample_point.index}",
                )
                self.service.status_service.record_route_trace(
                    action="sample_start",
                    route=phase,
                    point=sample_point,
                    point_tag=point_tag,
                    target={"pressure_hpa": sample_point.target_pressure_hpa, "temp_c": sample_point.temp_chamber_c},
                    result="ok",
                    message="H2O sampling start",
                )
                results = self.service.sampling_service.sample_point(sample_point, phase=phase, point_tag=point_tag)
                if not results:
                    self.service.status_service.record_route_trace(
                        action="sample_end",
                        route=phase,
                        point=sample_point,
                        point_tag=point_tag,
                        result="skip",
                        message="H2O sampling returned no results",
                    )
                    skipped_point_indices.append(sample_point.index)
                    self.service.status_service.clear_point_timing(sample_point, phase=phase, point_tag=point_tag)
                    continue
                for result in results:
                    self.service.event_bus.publish(EventType.SAMPLE_COLLECTED, result)
                self.service.status_service.record_route_trace(
                    action="sample_end",
                    route=phase,
                    point=sample_point,
                    point_tag=point_tag,
                    actual={"sample_count": len(results)},
                    result="ok",
                    message="H2O sampling complete",
                )
                self.service.qc_service.run_point_qc(sample_point, phase=phase, point_tag=point_tag)
                sampled_points.append(sample_point)
                sampled_point_indices.append(sample_point.index)
                completed_points.append(sample_point)
                completed_point_indices.append(sample_point.index)
                if is_current_ambient:
                    self._stop_h2o_vent_keepalive()
                    self.service.pressure_control_service.set_pressure_controller_vent(
                        False,
                        reason="H2O ambient complete: close vent before sealed pressure control",
                        prefer_direct_command=True,
                    )

            self.service.valve_routing_service.cleanup_h2o_route(lead, reason="after H2O group complete")
            return RouteRunResult(
                success=bool(completed_point_indices) and not skipped_point_indices,
                completed_points=completed_points,
                completed_point_indices=completed_point_indices,
                sampled_points=sampled_points,
                sampled_point_indices=sampled_point_indices,
                skipped_point_indices=skipped_point_indices,
                error=None if completed_point_indices and not skipped_point_indices else "H2O route completed with skipped points",
            )
        except WorkflowInterruptedError as exc:
            try:
                self.service.valve_routing_service.cleanup_h2o_route(lead, reason="after H2O stop requested")
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
        finally:
            self._stop_h2o_vent_keepalive()
            route_context.clear()

    def _start_h2o_vent_keepalive(self) -> None:
        if self._vent_keepalive_thread is not None:
            return
        self._vent_keepalive_stop.clear()
        svc = self.service
        interval_s = 1.0

        def _keepalive() -> None:
            self.service.status_service.log("[h2o-vent-keepalive] thread started")
            tick = 0
            while not self._vent_keepalive_stop.wait(interval_s):
                tick += 1
                try:
                    result = svc.pressure_control_service.set_pressure_controller_vent_fast_reassert(
                        True,
                        reason="h2o-vent-keepalive",
                        wait_after_command=False,
                        capture_pressure=False,
                        query_state=False,
                        confirm_transition=False,
                    )
                    if tick == 1 or tick % 60 == 0:
                        ok_str = "ok" if result.get("command_result") == "ok" else result.get("command_result", "?")
                        self.service.status_service.log(
                            f"[h2o-vent-keepalive] tick={tick} result={ok_str}"
                        )
                except Exception as exc:
                    self.service.status_service.log(f"[h2o-vent-keepalive] tick={tick} error={exc}")

        t = threading.Thread(target=_keepalive, daemon=True, name="h2o-vent-keepalive")
        t.start()
        self._vent_keepalive_thread = t

    def _stop_h2o_vent_keepalive(self) -> None:
        self._vent_keepalive_stop.set()
        t = self._vent_keepalive_thread
        if t is not None and t.is_alive():
            t.join(timeout=5.0)
        self._vent_keepalive_thread = None

    def _continue_after_humidity_timeout(self, result: Any) -> bool:
        if bool(getattr(result, "ok", False)):
            return True
        if not bool(getattr(result, "timed_out", False)):
            return False
        if self._humidity_timeout_policy() != "continue_after_timeout":
            return False
        if not self._collect_only_mode():
            self.service.status_service.log(
                "H2O humidity wait timed out; continue_after_timeout policy ignored because collect_only is disabled"
            )
            return False
        self.service.status_service.log(
            "Collect-only mode: humidity wait timed out; continue H2O sampling with current generator state "
            "(policy=continue_after_timeout)"
        )
        return True

    def _humidity_timeout_policy(self) -> str:
        getter = getattr(self.service, "_cfg_get", None)
        raw_value: Any = None
        if callable(getter):
            try:
                raw_value = getter("workflow.stability.h2o_route.humidity_timeout_policy", None)
            except Exception:
                raw_value = None
        if raw_value is None:
            workflow = getattr(getattr(self.service, "config", None), "workflow", None)
            stability = getattr(workflow, "stability", None)
            h2o_route = getattr(stability, "h2o_route", None)
            if isinstance(h2o_route, dict):
                raw_value = h2o_route.get("humidity_timeout_policy")
        policy = str(raw_value or "").strip().lower()
        if policy == "continue_after_timeout":
            return policy
        return "abort_like_v1"

    def _collect_only_mode(self) -> bool:
        reader = getattr(self.service, "_collect_only_mode", None)
        if callable(reader):
            try:
                return bool(reader())
            except Exception:
                return False
        getter = getattr(self.service, "_cfg_get", None)
        if callable(getter):
            try:
                return bool(getter("workflow.collect_only", False))
            except Exception:
                return False
        workflow = getattr(getattr(self.service, "config", None), "workflow", None)
        return bool(getattr(workflow, "collect_only", False))
