from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Sequence

from ..event_bus import EventType
from ..models import CalibrationPhase, CalibrationPoint
from .pressure_control_service import PressureWaitResult


@dataclass
class PressureBlockResult:
    completed_points: list[CalibrationPoint] = field(default_factory=list)
    completed_point_indices: list[int] = field(default_factory=list)
    sampled_points: list[CalibrationPoint] = field(default_factory=list)
    sampled_point_indices: list[int] = field(default_factory=list)
    skipped_point_indices: list[int] = field(default_factory=list)


class RoutePressureBlockService:

    def __init__(self, service: Any):
        self._service = service

    def split_pressure_blocks(
        self, pressure_refs: Sequence[CalibrationPoint]
    ) -> tuple[list[CalibrationPoint], list[CalibrationPoint]]:
        from ..route_planner import RoutePlanner
        ordered = RoutePlanner._pressure_reference_points(list(pressure_refs))
        ambient_refs = [p for p in ordered if p.is_ambient_pressure_point]
        sealed_refs = [
            p for p in ordered
            if not p.is_ambient_pressure_point and p.target_pressure_hpa is not None
        ]
        return ambient_refs, sealed_refs

    def run_co2_ambient_block(
        self,
        source_point: CalibrationPoint,
        ambient_refs: Sequence[CalibrationPoint],
    ) -> PressureBlockResult:
        phase = "co2"
        result = PressureBlockResult()
        svc = self._service

        svc.pressure_control_service.set_pressure_controller_vent(
            True, reason="CO2 first point ambient: keep atmosphere open"
        )
        svc.status_service.log("CO2 first point ambient: seal deferred, vent=ON")
        svc.status_service.record_route_trace(
            action="pressure_skip",
            route=phase,
            point=source_point,
            target={"pressure_hpa": None, "vent_on": True},
            result="deferred",
            message="CO2 first point ambient: seal/pressurize bypassed, vent stays open",
        )

        for ambient_ref in ambient_refs:
            svc.status_service.check_stop()
            sample_point = svc.route_planner.build_co2_pressure_point(source_point, ambient_ref)
            point_tag = svc.route_planner.co2_point_tag(sample_point)
            svc.route_context.update(
                current_point=sample_point,
                source_point=source_point,
                active_point=sample_point,
                point_tag=point_tag,
                retry=0,
                route_state={
                    "sample_point_index": sample_point.index,
                    "pressure_point_index": ambient_ref.index,
                    "pressure_target_hpa": sample_point.target_pressure_hpa,
                },
            )
            svc.status_service.begin_point_timing(sample_point, phase=phase, point_tag=point_tag)

            atmosphere_gate = self._maintain_ambient_vent_and_verify_atmosphere(
                sample_point, phase=phase
            )
            if atmosphere_gate.get("gate_result") != "PASS":
                svc.status_service.log(
                    f"CO2 ambient point {sample_point.index} skipped: "
                    f"pressure {atmosphere_gate.get('pressure_hpa')} hPa "
                    f"not near atmosphere {atmosphere_gate.get('atmosphere_reference_hpa')} hPa"
                )
                svc.status_service.clear_point_timing(sample_point, phase=phase, point_tag=point_tag)
                result.skipped_point_indices.append(sample_point.index)
                svc._record_workflow_timing(
                    "pressure_point_end",
                    "warning",
                    stage="pressure_point",
                    point=sample_point,
                    target_pressure_hpa=sample_point.target_pressure_hpa,
                    decision="ambient_atmosphere_fail",
                )
                continue

            svc.status_service.record_route_trace(
                action="pressure_skip",
                route=phase,
                point=sample_point,
                target={"pressure_hpa": None, "vent_on": True},
                result="skipped",
                message="CO2 ambient pressure point: vent stays open, set_pressure bypassed, P3 ambient read used",
            )
            svc.event_bus.publish(
                EventType.STABILITY_PASSED, {"point": sample_point, "stability_type": "pressure"}
            )
            svc.status_service.mark_point_stable_for_sampling(
                sample_point, phase=phase, point_tag=point_tag
            )
            svc.status_service.update_status(
                phase=CalibrationPhase.SAMPLING,
                current_point=sample_point,
                message=f"CO2 sampling point {sample_point.index}",
            )
            sample_count_expected, sample_interval_s = svc.sampling_service.sampling_params(phase)
            sample_expected_max_s = max(
                5.0, float(sample_count_expected) * max(0.0, float(sample_interval_s)) + 30.0
            )
            svc._record_workflow_timing(
                "sample_start",
                "start",
                stage="sample",
                point=sample_point,
                target_pressure_hpa=sample_point.target_pressure_hpa,
                expected_max_s=sample_expected_max_s,
                sample_count=sample_count_expected,
            )
            svc.status_service.record_route_trace(
                action="sample_start",
                route=phase,
                point=sample_point,
                point_tag=point_tag,
                target={"pressure_hpa": sample_point.target_pressure_hpa, "co2_ppm": sample_point.co2_ppm},
                result="ok",
                message="CO2 sampling start",
            )
            svc.pressure_control_service.set_pressure_controller_vent(
                True, reason="CO2 ambient sampling: keep vent open"
            )
            results = svc.sampling_service.sample_point(sample_point, phase=phase, point_tag=point_tag)
            svc.pressure_control_service.set_pressure_controller_vent(
                True, reason="CO2 ambient sampling: vent heartbeat after sample"
            )
            if not results:
                svc._record_workflow_timing(
                    "sample_end",
                    "warning",
                    stage="sample",
                    point=sample_point,
                    target_pressure_hpa=sample_point.target_pressure_hpa,
                    expected_max_s=sample_expected_max_s,
                    sample_count=0,
                    decision="no_results",
                )
                svc.status_service.record_route_trace(
                    action="sample_end",
                    route=phase,
                    point=sample_point,
                    point_tag=point_tag,
                    result="skip",
                    message="CO2 sampling returned no results",
                )
                result.skipped_point_indices.append(sample_point.index)
                svc.status_service.clear_point_timing(sample_point, phase=phase, point_tag=point_tag)
                svc._record_workflow_timing(
                    "pressure_point_end",
                    "warning",
                    stage="pressure_point",
                    point=sample_point,
                    target_pressure_hpa=sample_point.target_pressure_hpa,
                    decision="sample_no_results",
                )
                continue
            for sample_result in results:
                svc.event_bus.publish(EventType.SAMPLE_COLLECTED, sample_result)
            svc._record_workflow_timing(
                "sample_end",
                "end",
                stage="sample",
                point=sample_point,
                target_pressure_hpa=sample_point.target_pressure_hpa,
                expected_max_s=sample_expected_max_s,
                sample_count=len(results),
                decision="ok",
            )
            svc.status_service.record_route_trace(
                action="sample_end",
                route=phase,
                point=sample_point,
                point_tag=point_tag,
                actual={"sample_count": len(results)},
                result="ok",
                message="CO2 sampling complete",
            )
            svc.qc_service.run_point_qc(sample_point, phase=phase, point_tag=point_tag)
            result.sampled_points.append(sample_point)
            result.sampled_point_indices.append(sample_point.index)
            result.completed_points.append(sample_point)
            result.completed_point_indices.append(sample_point.index)
            svc._record_workflow_timing(
                "pressure_point_end",
                "end",
                stage="pressure_point",
                point=sample_point,
                target_pressure_hpa=sample_point.target_pressure_hpa,
                decision="ok",
            )

        return result

    def transition_co2_ambient_to_sealed(
        self,
        source_point: CalibrationPoint,
        first_sealed_sample_point: CalibrationPoint,
    ) -> PressureWaitResult:
        phase = "co2"
        svc = self._service

        if getattr(svc.a2_hooks, "co2_route_conditioning_at_atmosphere_active", False):
            svc.a2_hooks.co2_route_conditioning_at_atmosphere_active = False
        ctx = getattr(svc.a2_hooks, "co2_route_conditioning_at_atmosphere_context", None)
        if isinstance(ctx, dict):
            ctx["route_conditioning_phase"] = "ready_to_seal_phase"

        vent_off_sent_at = datetime.now(timezone.utc).isoformat()
        vent_off_mono = time.monotonic()
        svc.pressure_control_service.set_pressure_controller_vent(
            False,
            reason="CO2 ambient_open to sealed pressure: vent off before route close",
            prefer_direct_command=True,
        )

        settle_s = max(0.1, float(
            getattr(svc, "_cfg_get", lambda p, d: d)(
                "workflow.pressure.co2_ambient_to_sealed_vent_off_settle_s", 1.5
            )
        ))
        time.sleep(settle_s)

        route_close_sent_at = datetime.now(timezone.utc).isoformat()
        route_close_mono = time.monotonic()
        relay_state = svc.valve_routing_service.apply_valve_states([])

        svc.a2_hooks.co2_sealed_route_no_vent_active = True
        svc.a2_hooks.co2_sealed_route_no_vent_context = {
            "route": phase,
            "point_index": first_sealed_sample_point.index,
            "target_pressure_hpa": first_sealed_sample_point.target_pressure_hpa,
            "reason": "CO2 ambient_open -> sealed pressure",
            "activated_at": datetime.now(timezone.utc).isoformat(),
        }

        vent_off_to_route_close_s = round(max(0.0, route_close_mono - vent_off_mono), 3)
        limit_s = max(0.1, self._cfg_float(
            "workflow.pressure.co2_vent_off_to_route_close_max_s", 1.5
        ))

        svc.status_service.record_route_trace(
            action="co2_ambient_to_sealed_transition",
            route=phase,
            point=first_sealed_sample_point,
            actual={
                "vent_off_sent_at": vent_off_sent_at,
                "vent_off_to_route_close_s": vent_off_to_route_close_s,
                "vent_off_to_route_close_limit_s": limit_s,
                "route_close_sent_at": route_close_sent_at,
                "pressure_read_between_vent_off_and_route_close": False,
                "vent_reassert_between_vent_off_and_route_close": False,
                "preseal_atmosphere_hold_used": False,
                "positive_preseal_used": False,
                "target_pressure_hpa": first_sealed_sample_point.target_pressure_hpa,
                "relay_state": relay_state,
                "sealed_no_vent_guard_active_before_set_pressure": True,
                "vent_on_attempt_count_after_route_close": 0,
                "vent_on_blocked_count_after_route_close": 0,
                "vent_on_command_sent_after_route_close": False,
            },
            target={"pressure_hpa": first_sealed_sample_point.target_pressure_hpa, "vent_on": False},
            result="ok",
            message="CO2 ambient_open to sealed pressure: minimal transition complete, no-vent guard armed",
        )

        return PressureWaitResult(
            ok=True,
            diagnostics={
                "vent_off_to_route_close_s": vent_off_to_route_close_s,
                "transition_type": "ambient_to_sealed_minimal",
            },
        )

    def run_co2_sealed_block(
        self,
        source_point: CalibrationPoint,
        sealed_refs: Sequence[CalibrationPoint],
        *,
        already_sealed: bool = False,
    ) -> PressureBlockResult:
        phase = "co2"
        result = PressureBlockResult()
        svc = self._service

        if not already_sealed:
            pressurize_result = svc.pressure_control_service.pressurize_and_hold(
                source_point, route=phase
            )
            if not pressurize_result.ok:
                result.skipped_point_indices.extend(
                    svc.route_planner.build_co2_pressure_point(source_point, ref).index
                    for ref in sealed_refs
                )
                return result

        retry_total = self._co2_pressure_retry_total()
        for sealed_ref in sealed_refs:
            svc.status_service.check_stop()
            sample_point = svc.route_planner.build_co2_pressure_point(source_point, sealed_ref)
            point_tag = svc.route_planner.co2_point_tag(sample_point)
            svc.route_context.update(
                current_point=sample_point,
                source_point=source_point,
                active_point=sample_point,
                point_tag=point_tag,
                retry=0,
                route_state={
                    "sample_point_index": sample_point.index,
                    "pressure_point_index": sealed_ref.index,
                    "pressure_target_hpa": sample_point.target_pressure_hpa,
                },
            )
            svc.status_service.begin_point_timing(sample_point, phase=phase, point_tag=point_tag)
            svc._record_workflow_timing(
                "pressure_point_start",
                "start",
                stage="pressure_point",
                point=sample_point,
                target_pressure_hpa=sample_point.target_pressure_hpa,
            )

            pressure_ok = svc.pressure_control_service.set_pressure_to_target(sample_point).ok
            retry_done = 0
            while not pressure_ok and retry_done < retry_total:
                retry_done += 1
                svc.route_context.update(retry=retry_done, route_state={"retry": retry_done})
                pressure_ok = self._retry_pressure_point(
                    source_point, sample_point, attempt=retry_done, total=retry_total
                )
            if not pressure_ok:
                svc.status_service.log(
                    f"CO2 {sample_point.co2_ppm} ppm @ {sample_point.target_pressure_hpa} hPa skipped: "
                    f"pressure did not stabilize"
                )
                svc.status_service.clear_point_timing(sample_point, phase=phase, point_tag=point_tag)
                svc._record_workflow_timing(
                    "pressure_point_end",
                    "warning",
                    stage="pressure_point",
                    point=sample_point,
                    target_pressure_hpa=sample_point.target_pressure_hpa,
                    decision="pressure_not_stable",
                )
                result.skipped_point_indices.append(sample_point.index)
                continue

            svc.event_bus.publish(
                EventType.STABILITY_PASSED, {"point": sample_point, "stability_type": "pressure"}
            )
            if not svc.pressure_control_service.wait_after_pressure_stable_before_sampling(
                sample_point
            ).ok:
                svc.status_service.log(
                    f"CO2 {sample_point.co2_ppm} ppm @ {sample_point.target_pressure_hpa} hPa skipped: "
                    f"post-pressure hold before sampling interrupted"
                )
                svc.status_service.clear_point_timing(sample_point, phase=phase, point_tag=point_tag)
                svc._record_workflow_timing(
                    "pressure_point_end",
                    "warning",
                    stage="pressure_point",
                    point=sample_point,
                    target_pressure_hpa=sample_point.target_pressure_hpa,
                    decision="wait_gate_interrupted",
                )
                result.skipped_point_indices.append(sample_point.index)
                continue

            svc.status_service.mark_point_stable_for_sampling(
                sample_point, phase=phase, point_tag=point_tag
            )
            svc.status_service.update_status(
                phase=CalibrationPhase.SAMPLING,
                current_point=sample_point,
                message=f"CO2 sampling point {sample_point.index}",
            )
            sample_count_expected, sample_interval_s = svc.sampling_service.sampling_params(phase)
            sample_expected_max_s = max(
                5.0, float(sample_count_expected) * max(0.0, float(sample_interval_s)) + 30.0
            )
            svc._record_workflow_timing(
                "sample_start",
                "start",
                stage="sample",
                point=sample_point,
                target_pressure_hpa=sample_point.target_pressure_hpa,
                expected_max_s=sample_expected_max_s,
                sample_count=sample_count_expected,
            )
            svc.status_service.record_route_trace(
                action="sample_start",
                route=phase,
                point=sample_point,
                point_tag=point_tag,
                target={"pressure_hpa": sample_point.target_pressure_hpa, "co2_ppm": sample_point.co2_ppm},
                result="ok",
                message="CO2 sampling start",
            )
            results = svc.sampling_service.sample_point(sample_point, phase=phase, point_tag=point_tag)
            if not results:
                svc._record_workflow_timing(
                    "sample_end",
                    "warning",
                    stage="sample",
                    point=sample_point,
                    target_pressure_hpa=sample_point.target_pressure_hpa,
                    expected_max_s=sample_expected_max_s,
                    sample_count=0,
                    decision="no_results",
                )
                svc.status_service.record_route_trace(
                    action="sample_end",
                    route=phase,
                    point=sample_point,
                    point_tag=point_tag,
                    result="skip",
                    message="CO2 sampling returned no results",
                )
                result.skipped_point_indices.append(sample_point.index)
                svc.status_service.clear_point_timing(sample_point, phase=phase, point_tag=point_tag)
                svc._record_workflow_timing(
                    "pressure_point_end",
                    "warning",
                    stage="pressure_point",
                    point=sample_point,
                    target_pressure_hpa=sample_point.target_pressure_hpa,
                    decision="sample_no_results",
                )
                continue
            for sample_result in results:
                svc.event_bus.publish(EventType.SAMPLE_COLLECTED, sample_result)
            svc._record_workflow_timing(
                "sample_end",
                "end",
                stage="sample",
                point=sample_point,
                target_pressure_hpa=sample_point.target_pressure_hpa,
                expected_max_s=sample_expected_max_s,
                sample_count=len(results),
                decision="ok",
            )
            svc.status_service.record_route_trace(
                action="sample_end",
                route=phase,
                point=sample_point,
                point_tag=point_tag,
                actual={"sample_count": len(results)},
                result="ok",
                message="CO2 sampling complete",
            )
            svc.qc_service.run_point_qc(sample_point, phase=phase, point_tag=point_tag)
            result.sampled_points.append(sample_point)
            result.sampled_point_indices.append(sample_point.index)
            result.completed_points.append(sample_point)
            result.completed_point_indices.append(sample_point.index)
            svc._record_workflow_timing(
                "pressure_point_end",
                "end",
                stage="pressure_point",
                point=sample_point,
                target_pressure_hpa=sample_point.target_pressure_hpa,
                decision="ok",
            )

        return result

    def _co2_pressure_retry_total(self) -> int:
        return self._cfg_int("workflow.pressure.co2_reseal_retry_count", 1)

    def _retry_pressure_point(
        self,
        point: CalibrationPoint,
        sample_point: CalibrationPoint,
        *,
        attempt: int,
        total: int,
    ) -> bool:
        retrier = getattr(self._service, "_retry_co2_pressure_point_after_timeout", None)
        if callable(retrier):
            return bool(retrier(point, sample_point, attempt=attempt, total=total))
        self._service.status_service.log(
            f"CO2 {sample_point.co2_ppm} ppm @ {sample_point.target_pressure_hpa} hPa: "
            f"pressure retry {attempt}/{total}"
        )
        return self._service.pressure_control_service.set_pressure_to_target(sample_point).ok

    def _cfg_float(self, path: str, default: float) -> float:
        getter = getattr(self._service, "_cfg_get", None)
        if not callable(getter):
            return default
        try:
            return float(getter(path, default))
        except Exception:
            return default

    def _cfg_int(self, path: str, default: int) -> int:
        getter = getattr(self._service, "_cfg_get", None)
        if not callable(getter):
            return default
        try:
            return int(getter(path, default))
        except Exception:
            return default

    def _get_atmosphere_reference_hpa(self) -> float:
        svc = self._service
        atmosphere = None
        try:
            atmosphere = svc.pressure_control_service._coerce_float(
                getattr(
                    getattr(svc, "conditioning_service", None),
                    "_last_measured_atmospheric_pressure_hpa",
                    None,
                )
            )
        except Exception:
            pass
        if atmosphere is None:
            try:
                state = getattr(getattr(svc, "run_state", None), "pressure", None)
                atmosphere = getattr(state, "measured_atmospheric_pressure_hpa", None)
            except Exception:
                pass
        if atmosphere is None:
            atmosphere = self._cfg_float("workflow.pressure.default_atmosphere_hpa", 1013.25)
        return float(atmosphere)

    def _read_current_pressure_hpa(self) -> float | None:
        try:
            return self._service.pressure_control_service._current_pressure()
        except Exception:
            return None

    def _maintain_ambient_vent_and_verify_atmosphere(
        self,
        sample_point: CalibrationPoint,
        *,
        phase: str = "co2",
    ) -> dict[str, Any]:
        svc = self._service
        atmosphere_hpa = self._get_atmosphere_reference_hpa()
        margin_hpa = self._cfg_float("workflow.pressure.ambient_atmosphere_margin_hpa", 20.0)
        max_wait_s = self._cfg_float("workflow.pressure.ambient_atmosphere_wait_s", 30.0)
        vent_interval_s = max(0.1, self._cfg_float(
            "workflow.pressure.atmosphere_vent_heartbeat_interval_s", 0.5
        ))

        vent_count = 0
        start = time.monotonic()
        final_pressure = None
        near_atmosphere = False

        while time.monotonic() - start < max_wait_s:
            svc.status_service.check_stop()
            svc.pressure_control_service.set_pressure_controller_vent(
                True, reason="CO2 ambient atmosphere verification"
            )
            vent_count += 1
            current = self._read_current_pressure_hpa()
            if current is not None:
                final_pressure = current
                if abs(current - atmosphere_hpa) <= margin_hpa:
                    near_atmosphere = True
                    break
            time.sleep(vent_interval_s)

        gate_result = "PASS" if near_atmosphere else "FAIL"
        svc.status_service.record_route_trace(
            action="ambient_atmosphere_gate",
            route=phase,
            point=sample_point,
            target={"pressure_hpa": atmosphere_hpa, "margin_hpa": margin_hpa},
            actual={
                "pressure_hpa": final_pressure,
                "atmosphere_reference_hpa": atmosphere_hpa,
                "margin_hpa": margin_hpa,
                "vent_tick_count": vent_count,
                "near_atmosphere": near_atmosphere,
                "wait_s": round(time.monotonic() - start, 3),
            },
            result=gate_result,
            message="Atmosphere verified before ambient sampling" if near_atmosphere
            else f"Pressure {final_pressure} too far from atmosphere {atmosphere_hpa}",
        )
        return {
            "gate_result": gate_result,
            "pressure_hpa": final_pressure,
            "atmosphere_reference_hpa": atmosphere_hpa,
            "margin_hpa": margin_hpa,
            "vent_count": vent_count,
            "near_atmosphere": near_atmosphere,
        }
