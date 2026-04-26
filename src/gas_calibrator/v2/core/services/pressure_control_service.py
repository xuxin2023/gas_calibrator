from __future__ import annotations

from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
import time
from typing import Any, Callable, Mapping, Optional

from ...exceptions import WorkflowValidationError
from ..models import CalibrationPoint
from ..orchestration_context import OrchestrationContext
from ..run_state import RunState


@dataclass(frozen=True)
class PressureWaitResult:
    ok: bool
    timed_out: bool = False
    target_hpa: Optional[float] = None
    final_pressure_hpa: Optional[float] = None
    in_limits: bool = False
    attempt_count: int = 0
    diagnostics: dict[str, Any] = field(default_factory=dict)
    error: Optional[str] = None


@dataclass
class StartupPressurePrecheckResult:
    """Result of startup pressure precheck for summary/manifest."""
    passed: bool
    route: str = "co2"
    point_index: Optional[int] = None
    target_pressure_hpa: Optional[float] = None
    warning_count: int = 0
    error_count: int = 0
    details: dict[str, Any] = field(default_factory=dict)
    error: Optional[str] = None


class PressureControlService:
    """Pressure controller vent, seal, and stabilization helpers."""

    def _positive_preseal_enabled(
        self,
        point: CalibrationPoint,
        *,
        route: str,
        measured_atmospheric_pressure_hpa: Optional[float] = None,
    ) -> bool:
        if str(route or "").strip().lower() == "h2o":
            return False
        configured = self._coerce_bool(
            self.host._cfg_get("workflow.pressure.positive_preseal_pressurization_enabled", None)
        )
        if configured is not None:
            return bool(configured)
        return False

    def _positive_preseal_ready_pressure_hpa(self, point: CalibrationPoint) -> Optional[float]:
        configured = self._coerce_float(self.host._cfg_get("workflow.pressure.preseal_ready_pressure_hpa", None))
        if configured is not None:
            return float(configured)
        target = self.host._as_float(point.target_pressure_hpa)
        if target is None:
            return None
        margin = self._coerce_float(
            self.host._cfg_get("workflow.pressure.preseal_ready_margin_hpa", 0.0),
            default=0.0,
        )
        return float(target) + float(margin or 0.0)

    def _positive_preseal_abort_pressure_hpa(self, ready_pressure_hpa: Optional[float]) -> Optional[float]:
        configured = self._coerce_float(self.host._cfg_get("workflow.pressure.preseal_abort_pressure_hpa", None))
        if configured is not None:
            return float(configured)
        if ready_pressure_hpa is None:
            return None
        margin = self._coerce_float(
            self.host._cfg_get("workflow.pressure.preseal_abort_margin_hpa", 40.0),
            default=40.0,
        )
        return float(ready_pressure_hpa) + abs(float(margin or 0.0))

    def _pressure_controller_state_snapshot(self, controller: Any) -> dict[str, Any]:
        vent_status = self._pressure_controller_vent_status(controller)
        return {
            "vent_on": self._pressure_controller_vent_on(),
            "vent_status_raw": vent_status,
            "vent_status_interpreted": self._pressure_vent_status_interpretation(controller, vent_status),
            "output_state": self._pressure_controller_output_state(controller),
            "isolation_state": self._pressure_controller_isolation_state(controller),
        }

    def _remember_ambient_reference_pressure(
        self,
        pressure_hpa: Optional[float],
        *,
        source: str,
        timestamp: Optional[str] = None,
        monotonic_s: Optional[float] = None,
    ) -> None:
        value = self._coerce_float(pressure_hpa)
        if value is None:
            return
        recorded_at = timestamp or datetime.now(timezone.utc).isoformat()
        recorded_monotonic = time.monotonic() if monotonic_s is None else float(monotonic_s)
        state = self.run_state.pressure
        state.ambient_reference_pressure_hpa = float(value)
        state.ambient_reference_source = str(source or "pressure_controller_atmosphere_reference")
        state.ambient_reference_timestamp = recorded_at
        state.ambient_reference_monotonic_s = recorded_monotonic
        setattr(self.host, "_ambient_reference_pressure_hpa", float(value))
        setattr(self.host, "_ambient_reference_source", state.ambient_reference_source)
        setattr(self.host, "_ambient_reference_timestamp", recorded_at)
        setattr(self.host, "_ambient_reference_monotonic_s", recorded_monotonic)

    def _ambient_reference_payload(self) -> dict[str, Any]:
        state = self.run_state.pressure
        pressure_hpa = self._coerce_float(
            getattr(state, "ambient_reference_pressure_hpa", None)
            if getattr(state, "ambient_reference_pressure_hpa", None) is not None
            else getattr(self.host, "_ambient_reference_pressure_hpa", None)
        )
        source = str(
            getattr(state, "ambient_reference_source", "")
            or getattr(self.host, "_ambient_reference_source", "")
            or ""
        )
        timestamp = str(
            getattr(state, "ambient_reference_timestamp", "")
            or getattr(self.host, "_ambient_reference_timestamp", "")
            or ""
        )
        monotonic_s = self._coerce_float(
            getattr(state, "ambient_reference_monotonic_s", None)
            if getattr(state, "ambient_reference_monotonic_s", None) is not None
            else getattr(self.host, "_ambient_reference_monotonic_s", None)
        )
        age_s: Optional[float] = None
        if monotonic_s is not None:
            age_s = max(0.0, time.monotonic() - float(monotonic_s))
        return {
            "ambient_reference_pressure_hpa": pressure_hpa,
            "ambient_reference_source": source or ("unavailable" if pressure_hpa is None else "unknown"),
            "ambient_reference_timestamp": timestamp,
            "ambient_reference_age_s": age_s,
            "measured_atmospheric_pressure_hpa": pressure_hpa,
            "measured_atmospheric_pressure_source": (
                "deprecated_alias_of_ambient_reference_pressure_hpa"
                if pressure_hpa is not None
                else "deprecated_alias_unavailable"
            ),
        }

    def _fail_positive_preseal(
        self,
        point: CalibrationPoint,
        *,
        route: str,
        started_at: float,
        target_pressure_hpa: Optional[float],
        measured_atmospheric_pressure_hpa: Optional[float],
        ambient_reference: Optional[dict[str, Any]],
        ready_pressure_hpa: Optional[float],
        abort_pressure_hpa: Optional[float],
        timeout_s: float,
        poll_interval_s: float,
        pressure_hpa: Optional[float],
        pressure_peak_hpa: Optional[float],
        pressure_last_hpa: Optional[float],
        reason: str,
        message: str,
        extra: Optional[dict[str, Any]] = None,
    ) -> PressureWaitResult:
        elapsed_s = max(0.0, time.time() - started_at)
        actual = {
            "stage": "positive_preseal_pressurization",
            "target_pressure_hpa": target_pressure_hpa,
            "measured_atmospheric_pressure_hpa": measured_atmospheric_pressure_hpa,
            **dict(ambient_reference or {}),
            "preseal_ready_pressure_hpa": ready_pressure_hpa,
            "ready_pressure_hpa": ready_pressure_hpa,
            "preseal_abort_pressure_hpa": abort_pressure_hpa,
            "abort_pressure_hpa": abort_pressure_hpa,
            "preseal_ready_timeout_s": timeout_s,
            "preseal_pressure_poll_interval_s": poll_interval_s,
            "elapsed_s": elapsed_s,
            "pressure_hpa": pressure_hpa,
            "current_line_pressure_hpa": pressure_hpa,
            "positive_preseal_pressure_hpa": pressure_hpa,
            "preseal_pressure_peak_hpa": pressure_peak_hpa,
            "preseal_pressure_last_hpa": pressure_last_hpa,
            "pressure_max_hpa": (extra or {}).get("pressure_max_hpa", pressure_peak_hpa),
            "pressure_min_hpa": (extra or {}).get("pressure_min_hpa", pressure_last_hpa),
            "pressure_samples_count": int((extra or {}).get("pressure_samples_count", 0) or 0),
            "ready_reached": False,
            "seal_command_sent": False,
            "sealed": False,
            "pressure_control_started": False,
            "abort_reason": reason,
            "decision": "FAIL",
            **dict(extra or {}),
        }
        self._record_route_trace(
            action="positive_preseal_abort",
            route=route,
            point=point,
            actual=actual,
            result="fail",
            message=message,
        )
        timing_recorder = getattr(self.host, "_record_workflow_timing", None)
        if callable(timing_recorder):
            timing_recorder(
                "positive_preseal_abort",
                "fail",
                stage="positive_preseal_pressurization",
                point=point,
                target_pressure_hpa=target_pressure_hpa,
                duration_s=elapsed_s,
                expected_max_s=timeout_s,
                pressure_hpa=pressure_hpa,
                blocking_condition=reason,
                decision="abort",
                error_code=reason,
            )
        return PressureWaitResult(
            ok=False,
            timed_out=reason == "preseal_ready_timeout",
            target_hpa=target_pressure_hpa,
            final_pressure_hpa=pressure_hpa,
            diagnostics=actual,
            error=message,
        )

    def _verify_positive_preseal_vent_closed(
        self,
        controller: Any,
        *,
        pressure_reader: Optional[Callable[[], Optional[float]]],
        ambient_reference: Optional[dict[str, Any]],
        command_diagnostics: Optional[dict[str, Any]],
    ) -> dict[str, Any]:
        verify_timeout_s = max(
            0.1,
            float(self.host._cfg_get("workflow.pressure.preseal_vent_close_verify_timeout_s", 1.5)),
        )
        poll_s = max(
            0.05,
            float(self.host._cfg_get("workflow.pressure.preseal_vent_close_verify_poll_s", 0.2)),
        )
        ambient_pressure = self._coerce_float((ambient_reference or {}).get("ambient_reference_pressure_hpa"))
        command = dict(command_diagnostics or {})
        command_return_status = self._coerce_float(
            command.get("vent_command_return_status", command.get("command_return_status"))
        )
        command_status_allows = (
            command_return_status is not None
            and self._pressure_vent_status_allows_control(controller, int(command_return_status))
        )
        samples: list[dict[str, Any]] = []
        deadline = time.monotonic() + verify_timeout_s
        last: dict[str, Any] = {}
        accepted = False
        reason = "verification_window_expired"
        while True:
            state = self._pressure_controller_state_snapshot(controller)
            pressure_hpa = None if pressure_reader is None else self._coerce_float(pressure_reader())
            delta_from_ambient = (
                None
                if pressure_hpa is None or ambient_pressure is None
                else float(pressure_hpa) - float(ambient_pressure)
            )
            vent_status = self._coerce_float(state.get("vent_status_raw"))
            vent_status_allows = (
                vent_status is not None
                and self._pressure_vent_status_allows_control(controller, int(vent_status))
            )
            output_ok = state.get("output_state") == 0
            isolation_ok = state.get("isolation_state") == 1
            vent_on = state.get("vent_on")
            vent_on_closed = vent_on is not True
            status_lag_accepted = (
                command_status_allows
                and vent_status is not None
                and int(vent_status) == 1
            )
            status_ok = bool(vent_status_allows or status_lag_accepted)
            if vent_status is None:
                status_ok = bool(command_status_allows)
            sample = {
                **state,
                "pressure_hpa": pressure_hpa,
                "current_line_pressure_hpa": pressure_hpa,
                "positive_preseal_pressure_hpa": pressure_hpa,
                "ambient_reference_pressure_hpa": ambient_pressure,
                "pressure_delta_from_ambient_hpa": delta_from_ambient,
                "command_return_status": None if command_return_status is None else int(command_return_status),
                "command_status_allows_control": bool(command_status_allows),
                "vent_status_allows_control": bool(vent_status_allows),
                "vent_status_lag_accepted": bool(status_lag_accepted),
                "output_state_verified_idle": bool(output_ok),
                "isolation_state_verified_open": bool(isolation_ok),
                "vent_on_closed": bool(vent_on_closed),
                "status_ok_for_positive_preseal": bool(status_ok),
            }
            samples.append(sample)
            last = sample
            if output_ok and isolation_ok and vent_on_closed and status_ok:
                accepted = True
                reason = (
                    "command_return_status_allows_control"
                    if command_status_allows and not vent_status_allows
                    else "vent_status_allows_control"
                )
                break
            if time.monotonic() >= deadline:
                if not output_ok:
                    reason = "output_state_not_idle_after_vent_close"
                elif not isolation_ok:
                    reason = "isolation_state_not_open_after_vent_close"
                elif not vent_on_closed:
                    reason = "atmosphere_mode_still_reported_open"
                elif not status_ok:
                    reason = "vent_status_not_control_ready_after_vent_close"
                break
            time.sleep(min(poll_s, max(0.01, deadline - time.monotonic())))

        return {
            "ok": bool(accepted),
            "vent_command_result": "closed" if accepted else "not_closed",
            "vent_close_verification_status": "PASS" if accepted else "FAIL",
            "vent_close_verification_reason": reason,
            "vent_close_verify_timeout_s": verify_timeout_s,
            "vent_close_verify_poll_s": poll_s,
            "vent_close_verification_samples": samples,
            **command,
            **last,
        }

    def _positive_preseal_pressurization(
        self,
        controller: Any,
        point: CalibrationPoint,
        *,
        route: str,
        pressure_reader: Optional[Callable[[], Optional[float]]],
        measured_atmospheric_pressure_hpa: Optional[float],
        ambient_reference: Optional[dict[str, Any]] = None,
    ) -> PressureWaitResult:
        target_pressure_hpa = self.host._as_float(point.target_pressure_hpa)
        ready_pressure_hpa = self._positive_preseal_ready_pressure_hpa(point)
        abort_pressure_hpa = self._positive_preseal_abort_pressure_hpa(ready_pressure_hpa)
        timeout_s = max(
            0.1,
            float(self.host._cfg_get("workflow.pressure.preseal_ready_timeout_s", 30.0)),
        )
        poll_interval_s = max(
            0.05,
            float(self.host._cfg_get("workflow.pressure.preseal_pressure_poll_interval_s", 0.2)),
        )
        started_at = time.time()
        vent_closed_at = datetime.now(timezone.utc).isoformat()
        timing_recorder = getattr(self.host, "_record_workflow_timing", None)
        if callable(timing_recorder):
            timing_recorder(
                "positive_preseal_pressurization_start",
                "start",
                stage="positive_preseal_pressurization",
                point=point,
                target_pressure_hpa=target_pressure_hpa,
                expected_max_s=timeout_s,
                wait_reason="positive_preseal_pressurization",
            )
        self._record_route_trace(
            action="positive_preseal_pressurization_start",
            route=route,
            point=point,
            actual={
                "stage": "positive_preseal_pressurization",
                "target_pressure_hpa": target_pressure_hpa,
                "measured_atmospheric_pressure_hpa": measured_atmospheric_pressure_hpa,
                **dict(ambient_reference or {}),
                "preseal_ready_pressure_hpa": ready_pressure_hpa,
                "ready_pressure_hpa": ready_pressure_hpa,
                "preseal_abort_pressure_hpa": abort_pressure_hpa,
                "abort_pressure_hpa": abort_pressure_hpa,
                "preseal_ready_timeout_s": timeout_s,
                "preseal_pressure_poll_interval_s": poll_interval_s,
                "ready_reached": False,
                "sealed": False,
                "pressure_control_started": False,
                "decision": "START",
            },
            result="ok",
            message="Positive preseal pressurization started",
        )
        if callable(timing_recorder):
            timing_recorder(
                "positive_preseal_vent_close_start",
                "start",
                stage="positive_preseal_vent_close",
                point=point,
                target_pressure_hpa=target_pressure_hpa,
                expected_max_s=self.host._cfg_get(
                    "workflow.pressure.preseal_vent_close_verify_timeout_s",
                    self.host._cfg_get("workflow.pressure.vent_transition_timeout_s", None),
                ),
                wait_reason="close_pressure_controller_atmosphere_vent",
            )
        try:
            vent_command_diagnostics = self.set_pressure_controller_vent(
                False,
                reason="positive CO2 preseal pressurization before route seal",
                wait_after_command=False,
                capture_pressure=False,
            )
        except Exception as exc:
            vent_fail_elapsed_s = max(0.0, time.time() - started_at)
            if callable(timing_recorder):
                timing_recorder(
                    "positive_preseal_vent_close_fail",
                    "fail",
                    stage="positive_preseal_vent_close",
                    point=point,
                    target_pressure_hpa=target_pressure_hpa,
                    duration_s=vent_fail_elapsed_s,
                    decision="fail",
                    error_code="preseal_vent_close_failed",
                )
            return self._fail_positive_preseal(
                point,
                route=route,
                started_at=started_at,
                target_pressure_hpa=target_pressure_hpa,
                measured_atmospheric_pressure_hpa=measured_atmospheric_pressure_hpa,
                ambient_reference=ambient_reference,
                ready_pressure_hpa=ready_pressure_hpa,
                abort_pressure_hpa=abort_pressure_hpa,
                timeout_s=timeout_s,
                poll_interval_s=poll_interval_s,
                pressure_hpa=None,
                pressure_peak_hpa=None,
                pressure_last_hpa=None,
                reason="preseal_vent_close_failed",
                message="Positive preseal pressurization failed: pressure controller vent close command failed",
                extra={
                    "vent_closed_at": vent_closed_at,
                    "vent_command_result": "fail",
                    "command_error": str(exc),
                },
            )
        vent_close = self._verify_positive_preseal_vent_closed(
            controller,
            pressure_reader=pressure_reader,
            ambient_reference=ambient_reference,
            command_diagnostics=vent_command_diagnostics if isinstance(vent_command_diagnostics, dict) else {},
        )
        vent_close_elapsed_s = max(0.0, time.time() - started_at)
        if not bool(vent_close.get("ok")):
            if callable(timing_recorder):
                timing_recorder(
                    "positive_preseal_vent_close_fail",
                    "fail",
                    stage="positive_preseal_vent_close",
                    point=point,
                    target_pressure_hpa=target_pressure_hpa,
                    duration_s=vent_close_elapsed_s,
                    decision="fail",
                    error_code="preseal_vent_close_failed",
                    pressure_hpa=vent_close.get("pressure_hpa"),
                    pace_output_state=vent_close.get("output_state"),
                    pace_isolation_state=vent_close.get("isolation_state"),
                    pace_vent_status=vent_close.get("vent_status_raw"),
                )
            return self._fail_positive_preseal(
                point,
                route=route,
                started_at=started_at,
                target_pressure_hpa=target_pressure_hpa,
                measured_atmospheric_pressure_hpa=measured_atmospheric_pressure_hpa,
                ambient_reference=ambient_reference,
                ready_pressure_hpa=ready_pressure_hpa,
                abort_pressure_hpa=abort_pressure_hpa,
                timeout_s=timeout_s,
                poll_interval_s=poll_interval_s,
                pressure_hpa=None,
                pressure_peak_hpa=None,
                pressure_last_hpa=None,
                reason="preseal_vent_close_failed",
                message="Positive preseal pressurization failed: pressure controller vent did not close",
                extra={
                    "vent_closed_at": vent_closed_at,
                    "vent_command_result": "not_closed",
                    **vent_close,
                },
            )
        if callable(timing_recorder):
            timing_recorder(
                "positive_preseal_vent_close_end",
                "end",
                stage="positive_preseal_vent_close",
                point=point,
                target_pressure_hpa=target_pressure_hpa,
                duration_s=vent_close_elapsed_s,
                decision="ok",
                pressure_hpa=vent_close.get("pressure_hpa"),
                pace_output_state=vent_close.get("output_state"),
                pace_isolation_state=vent_close.get("isolation_state"),
                pace_vent_status=vent_close.get("vent_status_raw"),
            )
        state_after_vent = {
            key: vent_close.get(key)
            for key in (
                "vent_on",
                "vent_status_raw",
                "vent_status_interpreted",
                "output_state",
                "isolation_state",
                "command_method",
                "command_error",
                "command_return_status",
                "vent_command_return_status",
                "vent_close_verification_status",
                "vent_close_verification_reason",
                "command_status_allows_control",
                "vent_status_allows_control",
                "vent_status_lag_accepted",
                "status_ok_for_positive_preseal",
                "pressure_delta_from_ambient_hpa",
            )
        }

        previous_pressure: Optional[float] = None
        previous_elapsed: Optional[float] = None
        pressure_peak_hpa: Optional[float] = None
        pressure_last_hpa: Optional[float] = None
        pressure_min_hpa: Optional[float] = None
        pressure_samples_count = 0
        while True:
            self.host._check_stop()
            elapsed_s = max(0.0, time.time() - started_at)
            pressure_hpa = None if pressure_reader is None else pressure_reader()
            pressure_hpa = self._coerce_float(pressure_hpa)
            pressure_rise_rate: Optional[float] = None
            if pressure_hpa is not None:
                pressure_samples_count += 1
                pressure_last_hpa = float(pressure_hpa)
                pressure_peak_hpa = (
                    float(pressure_hpa)
                    if pressure_peak_hpa is None
                    else max(float(pressure_peak_hpa), float(pressure_hpa))
                )
                pressure_min_hpa = (
                    float(pressure_hpa)
                    if pressure_min_hpa is None
                    else min(float(pressure_min_hpa), float(pressure_hpa))
                )
                if previous_pressure is not None and previous_elapsed is not None and elapsed_s > previous_elapsed:
                    pressure_rise_rate = (float(pressure_hpa) - float(previous_pressure)) / (
                        float(elapsed_s) - float(previous_elapsed)
                    )
                previous_pressure = float(pressure_hpa)
                previous_elapsed = float(elapsed_s)
            check_payload = {
                "stage": "positive_preseal_pressurization",
                "target_pressure_hpa": target_pressure_hpa,
                "measured_atmospheric_pressure_hpa": measured_atmospheric_pressure_hpa,
                **dict(ambient_reference or {}),
                "preseal_ready_pressure_hpa": ready_pressure_hpa,
                "ready_pressure_hpa": ready_pressure_hpa,
                "preseal_abort_pressure_hpa": abort_pressure_hpa,
                "abort_pressure_hpa": abort_pressure_hpa,
                "preseal_ready_timeout_s": timeout_s,
                "preseal_pressure_poll_interval_s": poll_interval_s,
                "vent_closed_at": vent_closed_at,
                "vent_command_result": "closed",
                "elapsed_s": elapsed_s,
                "pressure_hpa": pressure_hpa,
                "current_line_pressure_hpa": pressure_hpa,
                "positive_preseal_pressure_hpa": pressure_hpa,
                "pressure_samples_count": pressure_samples_count,
                "pressure_max_hpa": pressure_peak_hpa,
                "pressure_min_hpa": pressure_min_hpa,
                "pressure_rise_rate_hpa_per_s": pressure_rise_rate,
                "ready_reached": False,
                "seal_command_sent": False,
                "sealed": False,
                "pressure_control_started": False,
                **state_after_vent,
            }
            self._record_route_trace(
                action="positive_preseal_pressure_check",
                route=route,
                point=point,
                actual=check_payload,
                result="ok",
                message="Positive preseal pressure check",
            )
            if callable(timing_recorder):
                timing_recorder(
                    "positive_preseal_pressure_check",
                    "tick",
                    stage="positive_preseal_pressurization",
                    point=point,
                    target_pressure_hpa=target_pressure_hpa,
                    duration_s=elapsed_s,
                    expected_max_s=timeout_s,
                    wait_reason="positive_preseal_pressure_rise",
                    pressure_hpa=pressure_hpa,
                    pace_output_state=state_after_vent.get("output_state"),
                    pace_isolation_state=state_after_vent.get("isolation_state"),
                    pace_vent_status=state_after_vent.get("vent_status_raw"),
                )
            if (
                pressure_hpa is not None
                and abort_pressure_hpa is not None
                and float(pressure_hpa) > float(abort_pressure_hpa)
            ):
                return self._fail_positive_preseal(
                    point,
                    route=route,
                    started_at=started_at,
                    target_pressure_hpa=target_pressure_hpa,
                    measured_atmospheric_pressure_hpa=measured_atmospheric_pressure_hpa,
                    ambient_reference=ambient_reference,
                    ready_pressure_hpa=ready_pressure_hpa,
                    abort_pressure_hpa=abort_pressure_hpa,
                    timeout_s=timeout_s,
                    poll_interval_s=poll_interval_s,
                    pressure_hpa=float(pressure_hpa),
                    pressure_peak_hpa=pressure_peak_hpa,
                    pressure_last_hpa=pressure_last_hpa,
                    reason="preseal_abort_pressure_exceeded",
                    message="Positive preseal pressurization exceeded abort pressure",
                    extra={
                        **state_after_vent,
                        "vent_closed_at": vent_closed_at,
                        "vent_command_result": "closed",
                        "pressure_samples_count": pressure_samples_count,
                        "pressure_max_hpa": pressure_peak_hpa,
                        "pressure_min_hpa": pressure_min_hpa,
                    },
                )
            if (
                pressure_hpa is not None
                and ready_pressure_hpa is not None
                and float(pressure_hpa) >= float(ready_pressure_hpa)
            ):
                ready_payload = {
                    **check_payload,
                    "ready_reached": True,
                    "ready_reached_at_pressure_hpa": float(pressure_hpa),
                    "seal_trigger_pressure_hpa": float(pressure_hpa),
                    "seal_trigger_elapsed_s": elapsed_s,
                    "decision": "READY",
                }
                self._record_route_trace(
                    action="positive_preseal_ready",
                    route=route,
                    point=point,
                    actual=ready_payload,
                    result="ok",
                    message="Positive preseal ready threshold reached",
                )
                if callable(timing_recorder):
                    timing_recorder(
                        "positive_preseal_ready",
                        "info",
                        stage="positive_preseal_pressurization",
                        point=point,
                        target_pressure_hpa=target_pressure_hpa,
                        duration_s=elapsed_s,
                        expected_max_s=timeout_s,
                        wait_reason="positive_preseal_pressure_rise",
                        pressure_hpa=pressure_hpa,
                        decision="ready",
                        pace_output_state=state_after_vent.get("output_state"),
                        pace_isolation_state=state_after_vent.get("isolation_state"),
                        pace_vent_status=state_after_vent.get("vent_status_raw"),
                    )
                return PressureWaitResult(
                    ok=True,
                    target_hpa=target_pressure_hpa,
                    final_pressure_hpa=float(pressure_hpa),
                    in_limits=True,
                    diagnostics={
                        **ready_payload,
                        "preseal_pressure_peak_hpa": pressure_peak_hpa,
                        "preseal_pressure_last_hpa": pressure_last_hpa,
                        "pressure_samples_count": pressure_samples_count,
                        "pressure_max_hpa": pressure_peak_hpa,
                        "pressure_min_hpa": pressure_min_hpa,
                        "preseal_trigger": "positive_preseal_ready",
                        "preseal_trigger_pressure_hpa": float(pressure_hpa),
                        "preseal_trigger_threshold_hpa": ready_pressure_hpa,
                    },
                )
            if elapsed_s >= timeout_s:
                return self._fail_positive_preseal(
                    point,
                    route=route,
                    started_at=started_at,
                    target_pressure_hpa=target_pressure_hpa,
                    measured_atmospheric_pressure_hpa=measured_atmospheric_pressure_hpa,
                    ambient_reference=ambient_reference,
                    ready_pressure_hpa=ready_pressure_hpa,
                    abort_pressure_hpa=abort_pressure_hpa,
                    timeout_s=timeout_s,
                    poll_interval_s=poll_interval_s,
                    pressure_hpa=pressure_hpa,
                    pressure_peak_hpa=pressure_peak_hpa,
                    pressure_last_hpa=pressure_last_hpa,
                    reason="preseal_ready_timeout",
                    message="Positive preseal pressurization timed out before ready pressure",
                    extra={
                        **state_after_vent,
                        "vent_closed_at": vent_closed_at,
                        "vent_command_result": "closed",
                        "pressure_samples_count": pressure_samples_count,
                        "pressure_max_hpa": pressure_peak_hpa,
                        "pressure_min_hpa": pressure_min_hpa,
                    },
                )
            time.sleep(min(poll_interval_s, max(0.05, timeout_s - elapsed_s)))

    def __init__(self, context: OrchestrationContext, run_state: RunState, *, host: Any) -> None:
        self.context = context
        self.run_state = run_state
        self.host = host

    def _log_pressure_controller_io(self, direction: str, data: str) -> None:
        logger = getattr(self.host, "run_logger", None)
        log_io = getattr(logger, "log_io", None)
        if callable(log_io):
            try:
                log_io("pressure_controller", direction, data)
            except Exception:
                pass

    def _pressure_controller_atmosphere_evidence(
        self,
        controller: Any,
        *,
        command_method: str,
        command_error: str = "",
    ) -> dict[str, Any]:
        vent_status = self._pressure_controller_vent_status(controller)
        vent_on = self._pressure_controller_vent_on()
        output_state = self._pressure_controller_output_state(controller)
        isolation_state = self._pressure_controller_isolation_state(controller)
        vent_status_ok = vent_status in {0, 1, 2}
        atmosphere_ready = bool(
            command_method
            and not command_error
            and output_state == 0
            and isolation_state == 1
            and (vent_on is True or vent_status_ok)
        )
        hard_blockers: list[str] = []
        if not command_method:
            hard_blockers.append("vent_command_method_unavailable")
        if command_error:
            hard_blockers.append("vent_command_failed")
        if output_state != 0:
            hard_blockers.append(
                "output_state_unavailable" if output_state is None else f"output_state={int(output_state)}"
            )
        if isolation_state != 1:
            hard_blockers.append(
                "isolation_state_unavailable" if isolation_state is None else f"isolation_state={int(isolation_state)}"
            )
        if vent_on is not True and not vent_status_ok:
            hard_blockers.append(
                "vent_status_unavailable" if vent_status is None else f"vent_status={int(vent_status)}"
            )
        return {
            "command_method": command_method,
            "command_error": command_error,
            "vent_on": vent_on,
            "vent_status_raw": vent_status,
            "vent_status_interpreted": self._pressure_vent_status_interpretation(controller, vent_status),
            "output_state": output_state,
            "isolation_state": isolation_state,
            "atmosphere_ready": atmosphere_ready,
            "hard_blockers": hard_blockers,
        }

    def _remember_startup_pressure_precheck_result(self, result: StartupPressurePrecheckResult) -> None:
        setattr(self.host, "_startup_pressure_precheck_result", result)

    def set_pressure_controller_vent(
        self,
        vent_on: bool,
        reason: str = "",
        *,
        wait_after_command: bool = True,
        capture_pressure: bool = True,
    ) -> dict[str, Any]:
        controller = self.host._device("pressure_controller")
        if controller is None:
            return {}
        extra = f" ({reason})" if reason else ""
        trace_result = "ok"
        trace_message = reason or ("vent on" if vent_on else "vent off")
        command_method = ""
        command_error = ""
        command_return_status: Optional[int] = None
        diagnostics: dict[str, Any] = {}
        timing_recorder = getattr(self.host, "_record_workflow_timing", None)
        if vent_on and callable(timing_recorder):
            timing_recorder(
                "pressure_atmosphere_vent_start",
                "start",
                stage="pressure_atmosphere_vent",
                wait_reason=reason or "pressure_controller_vent",
                expected_max_s=self.host._cfg_get("workflow.pressure.vent_transition_timeout_s", None),
            )
        try:
            if vent_on:
                self.host._call_first(controller, ("set_output",), False)
                self.host._call_first(controller, ("set_isolation_open",), True)
                if self.host._call_first(controller, ("vent",), True):
                    command_method = "set_output_false_set_isolation_open_vent_true"
                else:
                    enter = getattr(controller, "enter_atmosphere_mode", None)
                    if callable(enter):
                        command_method = "enter_atmosphere_mode"
                        self._log_pressure_controller_io(
                            "TX",
                            "enter_atmosphere_mode(vent_on=True)",
                        )
                        enter(
                            timeout_s=float(self.host._cfg_get("workflow.pressure.vent_transition_timeout_s", 30.0)),
                            hold_open=bool(self.host._cfg_get("workflow.pressure.continuous_atmosphere_hold", True)),
                            hold_interval_s=float(self.host._cfg_get("workflow.pressure.vent_hold_interval_s", 2.0)),
                        )
                        self._log_pressure_controller_io("RX", "ok")
            else:
                exit_mode = getattr(controller, "exit_atmosphere_mode", None)
                if callable(exit_mode):
                    command_method = "exit_atmosphere_mode"
                    self._log_pressure_controller_io("TX", "exit_atmosphere_mode(vent_on=False)")
                    command_return = exit_mode(
                        timeout_s=float(self.host._cfg_get("workflow.pressure.vent_transition_timeout_s", 30.0))
                    )
                    coerced_return = self._coerce_float(command_return)
                    command_return_status = None if coerced_return is None else int(coerced_return)
                    self._log_pressure_controller_io("RX", "ok")
                else:
                    self.host._call_first(controller, ("set_output",), False)
                    if self.host._call_first(controller, ("vent",), False):
                        command_method = "set_output_false_vent_false"
                    self.host._call_first(controller, ("set_isolation_open",), True)
            self.host._log(f"Pressure controller vent={'ON' if vent_on else 'OFF'}{extra}")
        except Exception as exc:
            self.host._log(f"Pressure controller vent command failed: {exc}")
            trace_result = "fail"
            trace_message = str(exc)
            command_error = str(exc)
        if vent_on and controller is not None:
            diagnostics = self._pressure_controller_atmosphere_evidence(
                controller,
                command_method=command_method,
                command_error=command_error,
            )
            if not bool(diagnostics.get("atmosphere_ready")) and trace_result == "ok":
                trace_result = "fail"
                trace_message = "pressure_controller_atmosphere_not_verified"
        if not vent_on and controller is not None:
            state = self._pressure_controller_state_snapshot(controller)
            diagnostics = {
                "command_method": command_method,
                "command_error": command_error,
                "vent_command_return_status": command_return_status,
                "command_return_status": command_return_status,
                **state,
            }
        current_pressure = self._current_pressure() if capture_pressure else None
        trace_ts = datetime.now(timezone.utc).isoformat()
        if (
            vent_on
            and trace_result == "ok"
            and bool(diagnostics.get("atmosphere_ready"))
            and current_pressure is not None
        ):
            self._remember_ambient_reference_pressure(
                current_pressure,
                source="pressure_controller_atmosphere_hold",
                timestamp=trace_ts,
            )
        if vent_on and callable(timing_recorder):
            timing_recorder(
                "pressure_atmosphere_vent_end",
                "end" if trace_result == "ok" else "fail",
                stage="pressure_atmosphere_vent",
                wait_reason=reason or "pressure_controller_vent",
                expected_max_s=self.host._cfg_get("workflow.pressure.vent_transition_timeout_s", None),
                decision=trace_result,
                pressure_hpa=diagnostics.get("pressure_hpa"),
                pace_output_state=diagnostics.get("output_state"),
                pace_isolation_state=diagnostics.get("isolation_state"),
                pace_vent_status=diagnostics.get("vent_status_raw"),
                error_code=trace_message if trace_result != "ok" else None,
            )
        self._record_route_trace(
            action="set_vent",
            target={"vent_on": bool(vent_on)},
            actual={
                "pressure_hpa": current_pressure,
                **diagnostics,
            },
            result=trace_result,
            message=trace_message,
        )
        if trace_result != "ok":
            raise WorkflowValidationError(
                "Pressure controller vent command failed"
                if command_error
                else "Pressure controller atmosphere mode not verified",
                details={
                    "vent_on": bool(vent_on),
                    "reason": reason,
                    "message": trace_message,
                    **diagnostics,
                },
            )
        if vent_on and trace_result == "ok":
            self._clear_pressure_route_seal_state()
        if vent_on and wait_after_command:
            wait_s = max(0.0, float(self.host._cfg_get("workflow.pressure.vent_time_s", 0.0)))
            if wait_s > 0:
                time.sleep(wait_s)
        return diagnostics

    def enable_pressure_controller_output(self, reason: str = "") -> None:
        controller = self.host._device("pressure_controller")
        if controller is None:
            return
        try:
            if not self.host._call_first(controller, ("enable_control_output",)):
                self.host._call_first(controller, ("set_output_mode_active",))
                self.host._call_first(controller, ("set_output",), True)
            extra = f" ({reason})" if reason else ""
            self.host._log(f"Pressure controller output=ON{extra}")
        except Exception as exc:
            self.host._log(f"Pressure controller output enable failed: {exc}")

    def disable_pressure_controller_output(self, reason: str = "") -> None:
        controller = self.host._device("pressure_controller")
        if controller is None:
            return
        trace_result = "ok"
        trace_message = reason or "pressure controller output off"
        try:
            if not self.host._call_first(controller, ("disable_control_output",)):
                self.host._call_first(controller, ("set_output_mode_idle",))
                self.host._call_first(controller, ("set_output",), False)
            extra = f" ({reason})" if reason else ""
            self.host._log(f"Pressure controller output=OFF{extra}")
        except Exception as exc:
            trace_result = "fail"
            trace_message = str(exc)
            self.host._log(f"Pressure controller output disable failed: {exc}")
        self._record_route_trace(
            action="set_output",
            target={"enabled": False},
            actual={"pressure_hpa": self._current_pressure()},
            result=trace_result,
            message=trace_message,
        )

    def prepare_pressure_for_h2o(self, point: CalibrationPoint) -> None:
        self._set_h2o_prepared_target(None)
        self.host._set_pressure_controller_vent(True, reason="H2O route precondition")
        self.host._log("Pressure controller kept at atmosphere for H2O route conditioning")

    def safe_stop_after_run(self, *, reason: str = "") -> dict[str, Any]:
        summary: dict[str, Any] = {}
        message = reason or "final safe stop"
        self._set_h2o_prepared_target(None)
        self._set_active_post_h2o_co2_zero_flush(False)

        try:
            self.disable_pressure_controller_output(reason=message)
        except Exception as exc:
            self.host._log(f"Final pressure safe stop warning: output disable failed: {exc}")
        try:
            self.set_pressure_controller_vent(True, reason=message)
        except Exception as exc:
            self.host._log(f"Final pressure safe stop warning: vent command failed: {exc}")

        pressure_meter = self.host._device("pressure_meter", "pressure_gauge")
        pressure_controller = self.host._device("pressure_controller", "pace")
        summary["gauge_pressure_hpa"] = (
            None if pressure_meter is None else self._read_pressure_from_device(pressure_meter, source="pressure_meter")
        )
        summary["pressure_controller_hpa"] = (
            None
            if pressure_controller is None
            else self._read_pressure_from_device(pressure_controller, source="pressure_controller")
        )
        summary["vent_on"] = self._pressure_controller_vent_on()
        summary["output_enabled"] = self._pressure_controller_output_enabled()
        result = "ok" if summary.get("output_enabled") in (False, None) else "warn"
        self._record_route_trace(
            action="final_safe_stop_pressure",
            target={"reason": message},
            actual=summary,
            result=result,
            message="Pressure controller returned to safe state",
        )
        return summary

    def run_startup_pressure_precheck(self, points: list[CalibrationPoint]) -> StartupPressurePrecheckResult:
        cfg = self._startup_pressure_precheck_cfg()
        if not bool(cfg.get("enabled", False)):
            self.host._log("Startup pressure precheck skipped: disabled by configuration")
            self._record_route_trace(
                action="startup_pressure_precheck",
                result="skip",
                message="disabled by configuration",
            )
            result = StartupPressurePrecheckResult(
                passed=True,
                route="co2",
                details={"skipped": True, "reason": "disabled"},
            )
            self._remember_startup_pressure_precheck_result(result)
            return result

        route = "h2o" if str(cfg.get("route", "co2") or "co2").strip().lower() == "h2o" else "co2"
        strict = bool(cfg.get("strict", True))
        point = self._startup_pressure_precheck_point(points, route=route, cfg=cfg)
        if point is None:
            message = f"Startup pressure precheck skipped: no usable {route.upper()} point found"
            self.host._log(message)
            self._record_route_trace(
                action="startup_pressure_precheck",
                route=route,
                result="fail" if strict else "skip",
                message=message,
            )
            result = StartupPressurePrecheckResult(
                passed=not strict,
                route=route,
                warning_count=0 if strict else 1,
                error_count=1 if strict else 0,
                details={"skipped": True, "reason": "no_usable_point"},
                error=message if strict else None,
            )
            self._remember_startup_pressure_precheck_result(result)
            if strict:
                raise WorkflowValidationError(
                    "Startup pressure precheck failed",
                    details={"route": route, "reason": "no_usable_point"},
                )
            return result

        route_soak_s = max(0.0, self._coerce_float(cfg.get("route_soak_s"), default=3.0))
        target_hpa = self._coerce_float(point.target_pressure_hpa)
        self.host._log(
            "Startup pressure precheck: "
            f"route={route.upper()} row={point.index} target={target_hpa} "
            f"co2={point.co2_ppm} group={getattr(point, 'co2_group', None)}"
        )

        last_detail: dict[str, Any] = {}
        result = StartupPressurePrecheckResult(passed=False, route=route, point_index=point.index, target_pressure_hpa=target_hpa)
        try:
            if route == "h2o":
                self.set_pressure_controller_vent(True, reason="startup pressure precheck H2O route open")
                self.host._set_h2o_path(True, point)
            else:
                self.host._set_co2_route_baseline(reason="before startup pressure precheck")
                self.host._set_valves_for_co2(point)

            if route_soak_s > 0:
                self.host._log(
                    "Startup pressure precheck: "
                    f"wait {route_soak_s:.0f}s with {route.upper()} route open before sealing"
                )
                self._sleep_with_stop(route_soak_s)

            readiness = self._startup_pressure_status()
            self.host._log(
                "Startup pressure precheck route ready: "
                f"gauge={readiness.get('pressure_meter_hpa')} "
                f"controller={readiness.get('pressure_controller_hpa')} "
                f"vent_on={readiness.get('vent_on')} "
                f"output_enabled={readiness.get('output_enabled')}"
            )
            self._record_route_trace(
                action="startup_pressure_precheck_route_ready",
                route=route,
                point=point,
                target={"route_soak_s": route_soak_s},
                actual=readiness,
                result="ok",
                message="Startup pressure precheck route ready",
            )

            seal_result = self.pressurize_and_hold(point, route=route)
            if not seal_result.ok:
                raise WorkflowValidationError(
                    "Startup pressure precheck failed",
                    details={
                        "route": route,
                        "point_index": point.index,
                        "target_pressure_hpa": target_hpa,
                        "stage": "seal_route",
                        "error": seal_result.error,
                        "diagnostics": dict(seal_result.diagnostics),
                    },
                )

            setpoint_result = self.set_pressure_to_target(point)
            if not setpoint_result.ok:
                raise WorkflowValidationError(
                    "Startup pressure precheck failed",
                    details={
                        "route": route,
                        "point_index": point.index,
                        "target_pressure_hpa": target_hpa,
                        "stage": "set_pressure",
                        "error": setpoint_result.error,
                        "diagnostics": dict(setpoint_result.diagnostics),
                    },
                )

            hold_ok, detail = self._observe_startup_pressure_hold(cfg)
            last_detail = dict(detail)
            hold_message = (
                "Startup pressure precheck hold result: "
                f"{'pass' if hold_ok else 'fail'} "
                f"source={detail.get('source')} "
                f"start={detail.get('start_hpa')} "
                f"end={detail.get('end_hpa')} "
                f"drift={detail.get('max_abs_drift_hpa')} "
                f"limit={detail.get('limit_hpa')} "
                f"samples={detail.get('samples')}"
            )
            self.host._log(hold_message)
            self._record_route_trace(
                action="startup_pressure_hold",
                route=route,
                point=point,
                target={
                    "hold_s": self._coerce_float(cfg.get("hold_s"), default=15.0),
                    "sample_interval_s": self._coerce_float(cfg.get("sample_interval_s"), default=1.0),
                    "max_abs_drift_hpa": self._coerce_float(cfg.get("max_abs_drift_hpa"), default=3.0),
                },
                actual=detail,
                result="ok" if hold_ok else "fail",
                message=hold_message,
            )
            if not hold_ok:
                raise WorkflowValidationError(
                    "Startup pressure precheck failed",
                    details={
                        "route": route,
                        "point_index": point.index,
                        "target_pressure_hpa": target_hpa,
                        "stage": "hold",
                        "hold": detail,
                    },
                )

            self._record_route_trace(
                action="startup_pressure_precheck",
                route=route,
                point=point,
                target={"pressure_hpa": target_hpa},
                actual=detail,
                result="ok",
                message="Startup pressure precheck passed",
            )
            result = StartupPressurePrecheckResult(
                passed=True,
                route=route,
                point_index=point.index,
                target_pressure_hpa=target_hpa,
                details=detail,
            )
            self._remember_startup_pressure_precheck_result(result)
        except Exception as exc:
            trace_result = "fail" if strict else "warn"
            self._record_route_trace(
                action="startup_pressure_precheck",
                route=route,
                point=point,
                target={"pressure_hpa": target_hpa},
                actual=last_detail,
                result=trace_result,
                message=str(exc),
            )
            result = StartupPressurePrecheckResult(
                passed=not strict,
                route=route,
                point_index=point.index,
                target_pressure_hpa=target_hpa,
                warning_count=0 if strict else 1,
                error_count=1 if strict else 0,
                details={"error": str(exc), **last_detail},
                error=str(exc),
            )
            self._remember_startup_pressure_precheck_result(result)
            if strict:
                if isinstance(exc, WorkflowValidationError):
                    raise
                raise WorkflowValidationError(
                    "Startup pressure precheck failed",
                    details={
                        "route": route,
                        "point_index": point.index,
                        "target_pressure_hpa": target_hpa,
                        "error": str(exc),
                    },
                ) from exc
            self.host._log(f"Startup pressure precheck warning: {exc}; strict=false, continue run")
        finally:
            if route == "h2o":
                self.host._cleanup_h2o_route(point, reason="after startup pressure precheck")
            else:
                self.host._cleanup_co2_route(reason="after startup pressure precheck")
        return result

    def pressure_reading_and_in_limits(self, target_hpa: float) -> tuple[Optional[float], bool]:
        controller = self.host._device("pressure_controller")
        if controller is None:
            return None, True
        get_in_limits = getattr(controller, "get_in_limits", None)
        if callable(get_in_limits):
            try:
                pressure_now, in_limits = get_in_limits()
                return self.host._as_float(pressure_now), bool(self.host._as_int(in_limits) == 1)
            except Exception:
                pass
        reader = self.host._make_pressure_reader()
        pressure_now = None if reader is None else reader()
        tolerance = float(self.host._cfg_get("workflow.pressure_control.setpoint_tolerance_hpa", 0.5))
        return pressure_now, pressure_now is not None and abs(float(pressure_now) - target_hpa) <= tolerance

    def soft_recover_pressure_controller(self, *, reason: str = "") -> PressureWaitResult:
        controller = self.host._device("pressure_controller")
        if controller is None:
            return PressureWaitResult(ok=False, diagnostics={"skipped": "pressure controller unavailable"})
        self.host._log(f"Pressure controller soft recovery start ({reason})")
        ok = True
        error = None
        try:
            closer = getattr(controller, "close", None)
            opener = getattr(controller, "open", None)
            if callable(closer) and callable(opener):
                closer()
                time.sleep(float(self.host._cfg_get("workflow.pressure.soft_recover_reopen_delay_s", 1.0)))
                opener()
        except Exception as exc:
            ok = False
            error = str(exc)
            self.host._log(f"Pressure controller soft recovery reopen failed: {exc}")
        self.host._configure_pressure_controller_in_limits()
        self.host._set_pressure_controller_vent(True, reason="soft recovery")
        self.host._log(
            "Pressure controller soft recovery complete"
            if ok
            else "Pressure controller soft recovery finished with errors"
        )
        return PressureWaitResult(
            ok=ok,
            attempt_count=1,
            diagnostics={"reason": reason},
            error=error,
        )

    def _apply_pressure_setpoint(self, controller: Any, target_hpa: float) -> dict[str, Any]:
        method_used = ""
        error = ""
        for method_name in ("set_setpoint", "set_pressure_hpa", "set_pressure"):
            method = getattr(controller, method_name, None)
            if not callable(method):
                continue
            method_used = method_name
            try:
                method(float(target_hpa))
            except Exception as exc:
                error = str(exc)
                return {
                    "command_sent": True,
                    "method": method_used,
                    "accepted": False,
                    "readback_hpa": None,
                    "error": error,
                    "reason": "setpoint_command_failed",
                }
            break
        if not method_used:
            return {
                "command_sent": False,
                "method": "",
                "accepted": False,
                "readback_hpa": None,
                "error": "setpoint_method_unavailable",
                "reason": "setpoint_method_unavailable",
            }
        getter = None
        for getter_name in ("get_setpoint", "get_pressure_setpoint", "read_setpoint"):
            candidate = getattr(controller, getter_name, None)
            if callable(candidate):
                getter = candidate
                break
        if getter is None:
            return {
                "command_sent": True,
                "method": method_used,
                "accepted": True,
                "readback_hpa": None,
                "error": "",
                "reason": "setpoint_command_completed_without_readback",
            }
        try:
            readback = self._coerce_float(getter())
        except Exception as exc:
            return {
                "command_sent": True,
                "method": method_used,
                "accepted": False,
                "readback_hpa": None,
                "error": str(exc),
                "reason": "setpoint_readback_failed",
            }
        tolerance = float(self.host._cfg_get("workflow.pressure_control.setpoint_tolerance_hpa", 0.5))
        accepted = readback is not None and abs(float(readback) - float(target_hpa)) <= max(0.05, tolerance)
        return {
            "command_sent": True,
            "method": method_used,
            "accepted": bool(accepted),
            "readback_hpa": readback,
            "error": "" if accepted else "setpoint_readback_mismatch",
            "reason": "setpoint_readback_accepted" if accepted else "setpoint_readback_mismatch",
        }

    def _pressure_output_enabled_for_control(self, controller: Any) -> Optional[bool]:
        state = self._pressure_controller_output_state(controller)
        if state is None:
            return None
        return int(state) == 1

    def set_pressure_to_target(
        self,
        point: CalibrationPoint,
        *,
        recovery_attempted: bool = False,
    ) -> PressureWaitResult:
        controller = self.host._device("pressure_controller")
        if controller is None:
            return PressureWaitResult(ok=True, diagnostics={"skipped": "pressure controller unavailable"})
        target = self.host._as_float(point.target_pressure_hpa)
        if target is None:
            self.host._log("Missing target pressure, skipping pressure control.")
            return PressureWaitResult(ok=False, diagnostics={"missing_target": True}, error="Missing target pressure")
        timing_recorder = getattr(self.host, "_record_workflow_timing", None)
        pressure_timeout_s = float(self.host._cfg_get("workflow.pressure.stabilize_timeout_s", 120.0))
        if callable(timing_recorder):
            timing_recorder(
                "pressure_setpoint_start",
                "start",
                stage="pressure_setpoint",
                point=point,
                target_pressure_hpa=target,
                expected_max_s=pressure_timeout_s,
                wait_reason="pressure_stabilize",
            )
        seal_context = self._active_pressure_route_seal_context(point)
        if seal_context is None:
            self.host._set_pressure_controller_vent(False, reason="before setpoint control")
        else:
            control_ready = self._pressure_control_ready_gate(controller, point, seal_context=seal_context)
            if not control_ready.ok:
                if callable(timing_recorder):
                    timing_recorder(
                        "pressure_timeout",
                        "warning",
                        stage="pressure_setpoint",
                        point=point,
                        target_pressure_hpa=target,
                        expected_max_s=pressure_timeout_s,
                        blocking_condition="pressure_control_ready_gate",
                        decision="not_ready",
                        error_code=control_ready.error,
                        )
                return control_ready
            if (
                str(seal_context.get("route") or "").strip().lower() == "co2"
                and bool(self.host._cfg_get("workflow.pressure.fail_if_sealed_pressure_below_target", False))
            ):
                current_pressure = self._current_pressure()
                margin_hpa = abs(
                    float(self.host._cfg_get("workflow.pressure.sealed_pressure_min_margin_hpa", 0.0))
                )
                if current_pressure is not None and float(current_pressure) + margin_hpa < float(target):
                    diagnostics = {
                        **dict(seal_context),
                        **dict(control_ready.diagnostics),
                        "pressure_hpa": current_pressure,
                        "target_pressure_hpa": target,
                        "hard_blockers": ["sealed_pressure_below_target"],
                        "pressure_control_started": False,
                    }
                    result = PressureWaitResult(
                        ok=False,
                        target_hpa=target,
                        final_pressure_hpa=current_pressure,
                        diagnostics=diagnostics,
                        error="Sealed CO2 pressure is below target; refusing to re-pressurize from outside gas",
                    )
                    self._record_route_trace(
                        action="set_pressure",
                        point=point,
                        target={"pressure_hpa": target},
                        actual=diagnostics,
                        result="fail",
                        message=result.error,
                    )
                    if callable(timing_recorder):
                        timing_recorder(
                            "pressure_timeout",
                            "warning",
                            stage="pressure_setpoint",
                            point=point,
                            target_pressure_hpa=target,
                            pressure_hpa=current_pressure,
                            expected_max_s=pressure_timeout_s,
                            blocking_condition="sealed_pressure_below_target",
                            decision="sealed_pressure_below_target",
                            error_code=result.error,
                        )
                    return result
        setpoint_result = self._apply_pressure_setpoint(controller, target)
        if not bool(setpoint_result.get("accepted")):
            result = PressureWaitResult(
                ok=False,
                target_hpa=target,
                diagnostics={"setpoint": setpoint_result, "hard_blockers": ["setpoint_not_accepted"]},
                error="Pressure setpoint not accepted",
            )
            self._record_route_trace(
                action="set_pressure",
                point=point,
                target={"pressure_hpa": target},
                actual=result.diagnostics,
                result="fail",
                message="Pressure setpoint not accepted",
            )
            if callable(timing_recorder):
                timing_recorder(
                    "pressure_timeout",
                    "warning",
                    stage="pressure_setpoint",
                    point=point,
                    target_pressure_hpa=target,
                    expected_max_s=pressure_timeout_s,
                    blocking_condition=str(setpoint_result.get("reason") or "setpoint_not_accepted"),
                    decision="setpoint_not_accepted",
                    error_code=result.error,
                )
            return result
        self.host._enable_pressure_controller_output(reason="after setpoint update")
        output_enabled = self._pressure_output_enabled_for_control(controller)
        if output_enabled is not True:
            result = PressureWaitResult(
                ok=False,
                target_hpa=target,
                diagnostics={
                    "setpoint": setpoint_result,
                    "output_enabled": output_enabled,
                    "hard_blockers": ["output_not_enabled"],
                },
                error="Pressure controller output not enabled",
            )
            self._record_route_trace(
                action="set_pressure",
                point=point,
                target={"pressure_hpa": target},
                actual=result.diagnostics,
                result="fail",
                message="Pressure controller output not enabled",
            )
            if callable(timing_recorder):
                timing_recorder(
                    "pressure_timeout",
                    "warning",
                    stage="pressure_setpoint",
                    point=point,
                    target_pressure_hpa=target,
                    expected_max_s=pressure_timeout_s,
                    blocking_condition="output_not_enabled",
                    decision="output_not_enabled",
                    error_code=result.error,
                )
            return result
        timeout_s = pressure_timeout_s
        retry_count = int(self.host._cfg_get("workflow.pressure.restabilize_retries", 2))
        retry_interval_s = float(self.host._cfg_get("workflow.pressure.restabilize_retry_interval_s", 10.0))
        started_at = time.time()
        next_retry_at = started_at + retry_interval_s
        retries_done = 0
        final_pressure: Optional[float] = None
        while time.time() - started_at < timeout_s:
            self.host._check_stop()
            final_pressure, in_limits = self.pressure_reading_and_in_limits(target)
            if in_limits:
                if seal_context is not None:
                    self.run_state.pressure.sealed_route_pressure_control_started = True
                    self.run_state.pressure.sealed_route_last_controlled_pressure_hpa = target
                self.host._log(f"Pressure in-limits at target {target} hPa")
                result = PressureWaitResult(
                    ok=True,
                    target_hpa=target,
                    final_pressure_hpa=final_pressure,
                    in_limits=True,
                    attempt_count=retries_done + 1,
                )
                self._record_route_trace(
                    action="set_pressure",
                    point=point,
                    target={"pressure_hpa": target},
                    actual={"pressure_hpa": final_pressure, "attempt_count": retries_done + 1},
                    result="ok",
                    message="Pressure stabilized in limits",
                )
                if callable(timing_recorder):
                    timing_recorder(
                        "pressure_ready",
                        "end",
                        stage="pressure_setpoint",
                        point=point,
                        target_pressure_hpa=target,
                        expected_max_s=timeout_s,
                        pressure_hpa=final_pressure,
                        decision="ok",
                    )
                return result
            if retries_done < retry_count and time.time() >= next_retry_at:
                retries_done += 1
                self.host._log(
                    f"Pressure not stable yet at {target} hPa; re-apply setpoint ({retries_done}/{retry_count})"
                )
                self.host._call_first(controller, ("set_setpoint", "set_pressure_hpa", "set_pressure"), target)
                self.host._enable_pressure_controller_output(reason="after setpoint re-apply")
                next_retry_at = time.time() + retry_interval_s
            time.sleep(0.5)
        self.host._log(f"Pressure stabilize timeout at target {target} hPa")
        if not recovery_attempted and bool(self.host._cfg_get("workflow.pressure.soft_recover_on_pressure_timeout", False)):
            recovered = self.soft_recover_pressure_controller(reason=f"pressure timeout @ {target} hPa")
            if recovered.ok:
                return self.set_pressure_to_target(point, recovery_attempted=True)
        result = PressureWaitResult(
            ok=False,
            timed_out=True,
            target_hpa=target,
            final_pressure_hpa=final_pressure,
            attempt_count=retries_done + 1,
            diagnostics={"recovery_attempted": recovery_attempted},
            error=f"Pressure stabilize timeout at target {target} hPa",
        )
        self._record_route_trace(
            action="set_pressure",
            point=point,
            target={"pressure_hpa": target},
            actual={"pressure_hpa": final_pressure, "attempt_count": retries_done + 1},
            result="timeout",
            message=result.error or "Pressure stabilize timeout",
        )
        if callable(timing_recorder):
            timing_recorder(
                "pressure_timeout",
                "warning",
                stage="pressure_setpoint",
                point=point,
                target_pressure_hpa=target,
                expected_max_s=timeout_s,
                pressure_hpa=final_pressure,
                decision="timeout",
                error_code=result.error,
            )
        return result

    def pressurize_and_hold(self, point: CalibrationPoint, route: str = "co2") -> PressureWaitResult:
        route_text = str(route or "").strip().lower()
        timing_recorder = getattr(self.host, "_record_workflow_timing", None)
        if callable(timing_recorder):
            timing_recorder(
                "seal_start",
                "start",
                stage="seal",
                point=point,
                target_pressure_hpa=point.target_pressure_hpa,
                wait_reason=f"{route_text}_pressure_seal",
            )
        controller = self.host._device("pressure_controller")
        if controller is None:
            self.host._log("Pressure controller unavailable, cannot seal route")
            result = PressureWaitResult(ok=False, diagnostics={"missing_controller": True}, error="Pressure controller unavailable")
            self._record_route_trace(
                action="seal_route",
                route=route,
                point=point,
                result="fail",
                message=result.error or "Pressure controller unavailable",
            )
            if callable(timing_recorder):
                timing_recorder(
                    "seal_end",
                    "fail",
                    stage="seal",
                    point=point,
                    target_pressure_hpa=point.target_pressure_hpa,
                    decision="missing_controller",
                    error_code=result.error,
                )
            return result
        if route_text == "h2o":
            self.host._capture_preseal_dewpoint_snapshot()
        final_vent_off_command_sent = True
        wait_after_vent_off_s = float(self.host._cfg_get("workflow.pressure.pressurize_wait_after_vent_off_s", 5.0))
        if route_text != "h2o" and self.run_state.humidity.active_post_h2o_co2_zero_flush:
            wait_after_vent_off_s = float(
                self.host._cfg_get("workflow.pressure.co2_post_h2o_vent_off_wait_s", wait_after_vent_off_s)
            )
        preseal_pressure_peak: Optional[float] = None
        preseal_pressure_last: Optional[float] = None
        preseal_trigger_source = "skipped"
        preseal_trigger_pressure_hpa: Optional[float] = None
        preseal_trigger_threshold_hpa: Optional[float] = None
        pressure_reader: Optional[Callable[[], Optional[float]]] = None
        positive_preseal = False
        positive_preseal_diagnostics: dict[str, Any] = {}
        measured_atmospheric_pressure_hpa: Optional[float] = None
        ambient_reference: dict[str, Any] = {}
        if route_text != "h2o":
            pressure_reader = self._make_preseal_observation_reader()
            positive_cfg = self._coerce_bool(
                self.host._cfg_get("workflow.pressure.positive_preseal_pressurization_enabled", None)
            )
            if positive_cfg:
                ambient_reference = self._ambient_reference_payload()
                measured_atmospheric_pressure_hpa = self._coerce_float(
                    ambient_reference.get("ambient_reference_pressure_hpa")
                )
                positive_preseal = self._positive_preseal_enabled(
                    point,
                    route=route_text,
                    measured_atmospheric_pressure_hpa=measured_atmospheric_pressure_hpa,
                )
            if positive_preseal:
                positive_result = self._positive_preseal_pressurization(
                    controller,
                    point,
                    route=route_text,
                    pressure_reader=pressure_reader,
                    measured_atmospheric_pressure_hpa=measured_atmospheric_pressure_hpa,
                    ambient_reference=ambient_reference,
                )
                if not positive_result.ok:
                    if callable(timing_recorder):
                        timing_recorder(
                            "seal_end",
                            "fail",
                            stage="seal",
                            point=point,
                            target_pressure_hpa=point.target_pressure_hpa,
                            pressure_hpa=positive_result.final_pressure_hpa,
                            decision="positive_preseal_failed",
                            error_code=positive_result.error,
                        )
                    return positive_result
                positive_preseal_diagnostics = dict(positive_result.diagnostics)
                preseal_pressure_peak = self._coerce_float(
                    positive_preseal_diagnostics.get("preseal_pressure_peak_hpa")
                )
                preseal_pressure_last = self._coerce_float(
                    positive_preseal_diagnostics.get("preseal_pressure_last_hpa")
                )
                preseal_trigger_source = str(
                    positive_preseal_diagnostics.get("preseal_trigger") or "positive_preseal_ready"
                )
                preseal_trigger_pressure_hpa = self._coerce_float(
                    positive_preseal_diagnostics.get("preseal_trigger_pressure_hpa")
                )
                preseal_trigger_threshold_hpa = self._coerce_float(
                    positive_preseal_diagnostics.get("preseal_trigger_threshold_hpa")
                )
            else:
                self.host._set_pressure_controller_vent(False, reason=f"before {route_text.upper()} pressure seal")
                preseal_trigger_threshold_hpa = self._coerce_float(
                    self.host._cfg_get("workflow.pressure.co2_preseal_pressure_gauge_trigger_hpa", 1110.0)
                )
        else:
            self.host._set_pressure_controller_vent(False, reason=f"before {route_text.upper()} pressure seal")
        if not positive_preseal:
            if wait_after_vent_off_s > 0:
                start = time.time()
                sample_interval_s = min(0.5, wait_after_vent_off_s)
                while True:
                    self.host._check_stop()
                    pressure_now = None if pressure_reader is None else pressure_reader()
                    if pressure_now is not None:
                        preseal_pressure_last = float(pressure_now)
                        if preseal_pressure_peak is None or float(pressure_now) > preseal_pressure_peak:
                            preseal_pressure_peak = float(pressure_now)
                    if (
                        route_text != "h2o"
                        and preseal_trigger_threshold_hpa is not None
                        and pressure_now is not None
                        and preseal_pressure_last is not None
                        and preseal_pressure_last >= preseal_trigger_threshold_hpa
                    ):
                        preseal_trigger_source = "pressure_gauge_threshold"
                        preseal_trigger_pressure_hpa = preseal_pressure_last
                        break
                    remain = wait_after_vent_off_s - (time.time() - start)
                    if remain <= 0:
                        preseal_trigger_source = "timeout"
                        break
                    time.sleep(min(sample_interval_s, remain))
            elif route_text != "h2o":
                preseal_trigger_source = "no_wait"
        final_pressure: Optional[float] = None
        try:
            if route_text != "h2o":
                if preseal_pressure_peak is not None:
                    trigger_detail = ""
                    if positive_preseal:
                        trigger_detail = (
                            f"positive preseal ready={preseal_trigger_pressure_hpa} hPa "
                            f"threshold={preseal_trigger_threshold_hpa} hPa; "
                        )
                    elif (
                        preseal_trigger_source == "pressure_gauge_threshold"
                        and preseal_trigger_pressure_hpa is not None
                        and preseal_trigger_threshold_hpa is not None
                    ):
                        trigger_detail = (
                            f"pressure gauge trigger={preseal_trigger_pressure_hpa:.3f} hPa "
                            f">= {preseal_trigger_threshold_hpa:.3f} hPa; "
                        )
                    elif preseal_trigger_source == "timeout":
                        trigger_detail = f"trigger timeout={wait_after_vent_off_s:.3f}s; "
                    self.host._log(
                        "CO2 route vent OFF settle complete; "
                        f"pre-seal pressure peak={preseal_pressure_peak:.3f} hPa "
                        f"last={preseal_pressure_last:.3f} hPa; "
                        f"{trigger_detail}"
                        "seal route directly before pressure control"
                    )
                else:
                    self.host._log("CO2 route vent OFF settle complete; seal route directly before pressure control")
            preseal_exit = self._preseal_final_atmosphere_exit_gate(
                controller,
                point,
                route=route_text,
                final_vent_off_command_sent=final_vent_off_command_sent,
                positive_preseal_diagnostics=positive_preseal_diagnostics if positive_preseal else None,
            )
            if not preseal_exit.ok:
                if positive_preseal and callable(timing_recorder):
                    timing_recorder(
                        "positive_preseal_abort",
                        "fail",
                        stage="positive_preseal_pressurization",
                        point=point,
                        target_pressure_hpa=point.target_pressure_hpa,
                        pressure_hpa=preseal_pressure_last,
                        decision="preseal_final_atmosphere_exit_failed",
                        error_code=preseal_exit.error,
                    )
                if callable(timing_recorder):
                    timing_recorder(
                        "seal_end",
                        "fail",
                        stage="seal",
                        point=point,
                        target_pressure_hpa=point.target_pressure_hpa,
                        decision="preseal_final_atmosphere_exit_failed",
                        error_code=preseal_exit.error,
                    )
                return preseal_exit
            if route_text == "h2o":
                self.host._set_h2o_path(False, point)
                relay_state = {}
            else:
                if positive_preseal and callable(timing_recorder):
                    timing_recorder(
                        "positive_preseal_seal_start",
                        "info",
                        stage="positive_preseal_pressurization",
                        point=point,
                        target_pressure_hpa=point.target_pressure_hpa,
                        pressure_hpa=preseal_trigger_pressure_hpa,
                        wait_reason="close_co2_route_valves",
                    )
                relay_state = self.host._apply_valve_states([])
            seal_transition = self._seal_transition_gate(point, route=route_text, relay_state=relay_state)
            if not seal_transition.ok:
                if positive_preseal and callable(timing_recorder):
                    timing_recorder(
                        "positive_preseal_abort",
                        "fail",
                        stage="positive_preseal_pressurization",
                        point=point,
                        target_pressure_hpa=point.target_pressure_hpa,
                        pressure_hpa=preseal_pressure_last,
                        decision="seal_transition_failed",
                        error_code=seal_transition.error,
                    )
                    timing_recorder(
                        "positive_preseal_seal_end",
                        "fail",
                        stage="positive_preseal_pressurization",
                        point=point,
                        target_pressure_hpa=point.target_pressure_hpa,
                        pressure_hpa=preseal_pressure_last,
                        decision="seal_transition_failed",
                        error_code=seal_transition.error,
                    )
                if callable(timing_recorder):
                    timing_recorder(
                        "seal_end",
                        "fail",
                        stage="seal",
                        point=point,
                        target_pressure_hpa=point.target_pressure_hpa,
                        decision="seal_transition_failed",
                        error_code=seal_transition.error,
                    )
                return PressureWaitResult(
                    ok=False,
                    diagnostics={**preseal_exit.diagnostics, **seal_transition.diagnostics},
                    error=seal_transition.error,
                )
            reader = self.host._make_pressure_reader()
            final_pressure = None if reader is None else reader()
            watchlist = self._preseal_watchlist_snapshot(
                controller,
                route=route_text,
                final_vent_off_command_sent=final_vent_off_command_sent,
            )
            if positive_preseal and callable(timing_recorder):
                timing_recorder(
                    "positive_preseal_seal_end",
                    "end",
                    stage="positive_preseal_pressurization",
                    point=point,
                    target_pressure_hpa=point.target_pressure_hpa,
                    pressure_hpa=final_pressure,
                    decision="sealed",
                )
            target_pressure_hpa = self.host._as_float(point.target_pressure_hpa)
            sealed_pressure_guard_enabled = bool(
                self.host._cfg_get("workflow.pressure.fail_if_sealed_pressure_below_target", False)
            )
            sealed_margin_hpa = float(
                self.host._cfg_get("workflow.pressure.sealed_pressure_min_margin_hpa", 0.0)
            )
            if (
                route_text != "h2o"
                and positive_preseal
                and sealed_pressure_guard_enabled
                and final_pressure is not None
                and target_pressure_hpa is not None
                and float(final_pressure) + abs(sealed_margin_hpa) < float(target_pressure_hpa)
            ):
                diagnostics = {
                    "route": route_text,
                    **preseal_exit.diagnostics,
                    **seal_transition.diagnostics,
                    **positive_preseal_diagnostics,
                    **watchlist,
                    "pressure_hpa": final_pressure,
                    "sealed_pressure_hpa": final_pressure,
                    "target_pressure_hpa": target_pressure_hpa,
                    "hard_blockers": ["sealed_pressure_below_target"],
                    "pressure_control_started": False,
                }
                self._record_route_trace(
                    action="sealed_pressure_control_start",
                    route=route_text,
                    point=point,
                    actual=diagnostics,
                    result="fail",
                    message="Sealed CO2 pressure is below target before pressure control",
                )
                if callable(timing_recorder):
                    timing_recorder(
                        "positive_preseal_abort",
                        "fail",
                        stage="positive_preseal_pressurization",
                        point=point,
                        target_pressure_hpa=target_pressure_hpa,
                        pressure_hpa=final_pressure,
                        decision="sealed_pressure_below_target",
                        error_code="sealed_pressure_below_target",
                    )
                    timing_recorder(
                        "seal_end",
                        "fail",
                        stage="seal",
                        point=point,
                        target_pressure_hpa=target_pressure_hpa,
                        pressure_hpa=final_pressure,
                        decision="sealed_pressure_below_target",
                        error_code="sealed_pressure_below_target",
                    )
                return PressureWaitResult(
                    ok=False,
                    final_pressure_hpa=final_pressure,
                    diagnostics=diagnostics,
                    error="Sealed CO2 pressure is below target before pressure control",
                )
            if route_text != "h2o" and preseal_pressure_peak is not None:
                self.host._log(
                    f"{route_text.upper()} route sealed for pressure control "
                    f"(pre-seal peak={preseal_pressure_peak:.3f} hPa, "
                    f"pre-seal last={preseal_pressure_last:.3f} hPa, "
                    f"sealed pressure={final_pressure})"
                )
            else:
                self.host._log(f"{route_text.upper()} route sealed for pressure control")
            self._mark_pressure_route_sealed(
                point,
                route=route_text,
                final_vent_off_command_sent=final_vent_off_command_sent,
                watchlist=watchlist,
                sealed_pressure_hpa=final_pressure,
                preseal_pressure_peak_hpa=preseal_pressure_peak,
                preseal_pressure_last_hpa=preseal_pressure_last,
                preseal_trigger=preseal_trigger_source,
                preseal_trigger_pressure_hpa=preseal_trigger_pressure_hpa,
                preseal_trigger_threshold_hpa=preseal_trigger_threshold_hpa,
            )
            result = PressureWaitResult(
                ok=True,
                final_pressure_hpa=final_pressure,
                diagnostics={
                    "route": route_text,
                    **preseal_exit.diagnostics,
                    **seal_transition.diagnostics,
                    **positive_preseal_diagnostics,
                    "positive_preseal_pressurization_enabled": positive_preseal,
                    "measured_atmospheric_pressure_hpa": measured_atmospheric_pressure_hpa,
                    **ambient_reference,
                    "preseal_trigger": preseal_trigger_source,
                    "preseal_trigger_pressure_hpa": preseal_trigger_pressure_hpa,
                    "preseal_trigger_threshold_hpa": preseal_trigger_threshold_hpa,
                    **watchlist,
                },
            )
            self._record_route_trace(
                action="seal_route",
                route=route_text,
                point=point,
                actual={
                    "pressure_hpa": final_pressure,
                    "preseal_pressure_peak_hpa": preseal_pressure_peak,
                    "preseal_pressure_last_hpa": preseal_pressure_last,
                    "measured_atmospheric_pressure_hpa": measured_atmospheric_pressure_hpa,
                    **ambient_reference,
                    "positive_preseal_pressurization_enabled": positive_preseal,
                    **preseal_exit.diagnostics,
                    **seal_transition.diagnostics,
                    **positive_preseal_diagnostics,
                    "preseal_trigger": preseal_trigger_source,
                    "preseal_trigger_pressure_hpa": preseal_trigger_pressure_hpa,
                    "preseal_trigger_threshold_hpa": preseal_trigger_threshold_hpa,
                    **watchlist,
                },
                result="ok",
                message=f"{route_text.upper()} route sealed for pressure control",
            )
            if callable(timing_recorder):
                timing_recorder(
                    "seal_end",
                    "end",
                    stage="seal",
                    point=point,
                    target_pressure_hpa=point.target_pressure_hpa,
                    pressure_hpa=final_pressure,
                    decision="ok",
                )
            return result
        finally:
            if route_text != "h2o":
                self._set_active_post_h2o_co2_zero_flush(False)

    def wait_after_pressure_stable_before_sampling(self, point: CalibrationPoint) -> PressureWaitResult:
        if self.host._collect_only_fast_path_enabled():
            self.host._log("Collect-only mode: post-pressure sample hold skipped")
            result = PressureWaitResult(ok=True, diagnostics={"skipped": "collect_only_fast_path"})
            timing_recorder = getattr(self.host, "_record_workflow_timing", None)
            if callable(timing_recorder):
                timing_recorder(
                    "wait_gate_start",
                    "start",
                    stage="wait_gate",
                    point=point,
                    target_pressure_hpa=point.target_pressure_hpa,
                    expected_max_s=0.0,
                    wait_reason="collect_only_fast_path",
                )
                timing_recorder(
                    "wait_gate_end",
                    "end",
                    stage="wait_gate",
                    point=point,
                    target_pressure_hpa=point.target_pressure_hpa,
                    expected_max_s=0.0,
                    wait_reason="collect_only_fast_path",
                    decision="skipped",
                )
            self._record_route_trace(
                action="wait_post_pressure",
                point=point,
                target={"hold_s": 0.0},
                result="ok",
                message="Collect-only fast path skipped post-pressure wait",
            )
            return result
        hold_key = (
            "workflow.pressure.post_stable_sample_delay_s"
            if point.is_h2o_point
            else "workflow.pressure.co2_post_stable_sample_delay_s"
        )
        hold_s = float(
            self.host._cfg_get(hold_key, self.host._cfg_get("workflow.pressure.post_stable_sample_delay_s", 60.0))
        )
        if hold_s <= 0:
            result = PressureWaitResult(ok=True, diagnostics={"hold_s": hold_s})
            timing_recorder = getattr(self.host, "_record_workflow_timing", None)
            if callable(timing_recorder):
                timing_recorder(
                    "wait_gate_start",
                    "start",
                    stage="wait_gate",
                    point=point,
                    target_pressure_hpa=point.target_pressure_hpa,
                    expected_max_s=0.0,
                    wait_reason="post_pressure_hold_disabled",
                )
                timing_recorder(
                    "wait_gate_end",
                    "end",
                    stage="wait_gate",
                    point=point,
                    target_pressure_hpa=point.target_pressure_hpa,
                    expected_max_s=0.0,
                    wait_reason="post_pressure_hold_disabled",
                    decision="disabled",
                )
            self._record_route_trace(
                action="wait_post_pressure",
                point=point,
                target={"hold_s": hold_s},
                result="ok",
                message="Post-pressure wait disabled",
            )
            return result
        started_at = time.time()
        timing_recorder = getattr(self.host, "_record_workflow_timing", None)
        expected_max_s = hold_s + max(5.0, hold_s * 0.1)
        if callable(timing_recorder):
            timing_recorder(
                "wait_gate_start",
                "start",
                stage="wait_gate",
                point=point,
                target_pressure_hpa=point.target_pressure_hpa,
                expected_max_s=expected_max_s,
                wait_reason="post_pressure_hold",
            )
        while time.time() - started_at < hold_s:
            self.host._check_stop()
            time.sleep(min(1.0, max(0.05, hold_s - (time.time() - started_at))))
        result = PressureWaitResult(ok=True, diagnostics={"hold_s": hold_s})
        if callable(timing_recorder):
            timing_recorder(
                "wait_gate_end",
                "end",
                stage="wait_gate",
                point=point,
                target_pressure_hpa=point.target_pressure_hpa,
                duration_s=max(0.0, time.time() - started_at),
                expected_max_s=expected_max_s,
                wait_reason="post_pressure_hold",
                decision="ok",
            )
        self._record_route_trace(
            action="wait_post_pressure",
            point=point,
            target={"hold_s": hold_s},
            actual={"elapsed_s": max(0.0, time.time() - started_at)},
            result="ok",
            message="Post-pressure wait complete",
        )
        return result

    def _startup_pressure_precheck_cfg(self) -> dict[str, Any]:
        cfg = self.host._cfg_get("workflow.startup_pressure_precheck", {})
        return dict(cfg) if isinstance(cfg, dict) else {}

    def _startup_pressure_precheck_point(
        self,
        points: list[CalibrationPoint],
        *,
        route: str,
        cfg: Optional[dict[str, Any]] = None,
    ) -> Optional[CalibrationPoint]:
        config = cfg if isinstance(cfg, dict) else self._startup_pressure_precheck_cfg()
        override_target = self._coerce_float(config.get("target_hpa"))
        route_name = "h2o" if str(route or "").strip().lower() == "h2o" else "co2"
        candidate: Optional[CalibrationPoint] = None
        if route_name == "h2o":
            for point in points:
                if point.is_h2o_point:
                    candidate = point
                    break
        else:
            source_selector = getattr(self.host, "_co2_source_points", None)
            if callable(source_selector):
                try:
                    gas_sources = list(source_selector(points))
                except Exception:
                    gas_sources = []
            else:
                gas_sources = []
            if gas_sources:
                candidate = gas_sources[0]
            else:
                for point in points:
                    if point.co2_ppm is not None:
                        candidate = point
                        break
        if candidate is None:
            return None

        valid_targets = [
            float(value)
            for value in (
                self._coerce_float(point.target_pressure_hpa)
                for point in points
            )
            if value is not None
        ]
        chosen_target = override_target
        if chosen_target is None:
            chosen_target = max(valid_targets) if valid_targets else self._coerce_float(candidate.target_pressure_hpa)
        if chosen_target is None:
            return None
        return replace(candidate, pressure_hpa=float(chosen_target))

    def _observe_startup_pressure_hold(self, cfg: dict[str, Any]) -> tuple[bool, dict[str, Any]]:
        hold_s = max(0.0, self._coerce_float(cfg.get("hold_s"), default=15.0))
        interval_s = max(0.05, self._coerce_float(cfg.get("sample_interval_s"), default=1.0))
        limit_hpa = max(0.0, self._coerce_float(cfg.get("max_abs_drift_hpa"), default=3.0))

        self.disable_pressure_controller_output(reason="during startup pressure hold check")
        if hold_s <= 0:
            value, source = self._read_startup_pressure_precheck_value(cfg)
            return True, {
                "source": source,
                "start_hpa": value,
                "end_hpa": value,
                "max_abs_drift_hpa": 0.0,
                "span_hpa": 0.0,
                "samples": 1 if value is not None else 0,
                "limit_hpa": limit_hpa,
            }

        readings: list[float] = []
        source = "unavailable"
        started_at = time.time()
        while True:
            self.host._check_stop()
            value, source = self._read_startup_pressure_precheck_value(cfg)
            if value is not None:
                readings.append(float(value))
            elapsed = time.time() - started_at
            if elapsed >= hold_s:
                break
            time.sleep(min(interval_s, max(0.05, hold_s - elapsed)))

        if not readings:
            return False, {
                "source": source,
                "start_hpa": None,
                "end_hpa": None,
                "max_abs_drift_hpa": None,
                "span_hpa": None,
                "samples": 0,
                "limit_hpa": limit_hpa,
            }

        baseline = readings[0]
        max_abs_drift = max(abs(one - baseline) for one in readings)
        span_hpa = max(readings) - min(readings) if len(readings) > 1 else 0.0
        return max_abs_drift <= limit_hpa, {
            "source": source,
            "start_hpa": baseline,
            "end_hpa": readings[-1],
            "max_abs_drift_hpa": max_abs_drift,
            "span_hpa": span_hpa,
            "samples": len(readings),
            "limit_hpa": limit_hpa,
        }

    def _read_startup_pressure_precheck_value(self, cfg: dict[str, Any]) -> tuple[Optional[float], str]:
        prefer_gauge = bool(cfg.get("prefer_gauge", True))
        candidates = [
            (("pressure_meter", "pressure_gauge"), "pressure_meter"),
            (("pressure_controller", "pace"), "pressure_controller"),
        ]
        if not prefer_gauge:
            candidates.reverse()
        for device_names, source in candidates:
            device = self.host._device(*device_names)
            if device is None:
                continue
            value = self._read_pressure_from_device(device, source=source)
            if value is not None:
                return value, source
        fallback = self._current_pressure()
        if fallback is not None:
            return fallback, "fallback"
        return None, "unavailable"

    def _startup_pressure_status(self) -> dict[str, Any]:
        pressure_meter = self.host._device("pressure_meter", "pressure_gauge")
        pressure_controller = self.host._device("pressure_controller", "pace")
        return {
            "pressure_meter_hpa": None if pressure_meter is None else self._read_pressure_from_device(pressure_meter, source="pressure_meter"),
            "pressure_controller_hpa": None if pressure_controller is None else self._read_pressure_from_device(pressure_controller, source="pressure_controller"),
            "vent_on": self._pressure_controller_vent_on(),
            "output_enabled": self._pressure_controller_output_enabled(),
        }

    def _pressure_controller_output_enabled(self) -> Optional[bool]:
        controller = self.host._device("pressure_controller")
        if controller is None:
            return None
        return self._read_bool_state(
            controller,
            "is_output_enabled",
            "get_output_enabled",
            "get_output",
            "output_enabled",
            "output",
        )

    def _pressure_controller_vent_on(self) -> Optional[bool]:
        controller = self.host._device("pressure_controller")
        if controller is None:
            return None
        return self._read_bool_state(
            controller,
            "is_in_atmosphere_mode",
            "get_atmosphere_mode",
            "is_vent_open",
            "get_vent",
            "vent",
        )

    def _pressure_controller_output_state(self, controller: Any) -> Optional[int]:
        return self._read_int_state(
            controller,
            "is_output_enabled",
            "get_output_enabled",
            "get_output_state",
            "get_output",
            "output_enabled",
            "output_state",
            "output",
        )

    def _pressure_controller_isolation_state(self, controller: Any) -> Optional[int]:
        return self._read_int_state(
            controller,
            "is_isolation_open",
            "get_isolation_open",
            "get_isolation_state",
            "get_isolation",
            "isolation_open",
            "isolation_state",
            "isolation",
        )

    def _pressure_route_for_point(self, point: CalibrationPoint) -> str:
        route_text = str(getattr(point, "route", "") or "").strip().lower()
        if route_text:
            return "h2o" if route_text == "h2o" else "co2"
        return "h2o" if point.is_h2o_point else "co2"

    def _active_pressure_route_seal_context(self, point: CalibrationPoint) -> Optional[dict[str, Any]]:
        state = self.run_state.pressure
        route = self._pressure_route_for_point(point)
        if not state.final_vent_off_command_sent or str(state.sealed_route or "") != route:
            return None
        return {
            "route": route,
            "sealed_source_point_index": state.sealed_source_point_index,
            "final_vent_off_command_sent": state.final_vent_off_command_sent,
            "pressure_hpa": state.sealed_pressure_hpa,
            "sealed_pressure_hpa": state.sealed_pressure_hpa,
            "preseal_pressure_peak_hpa": state.preseal_pressure_peak_hpa,
            "preseal_pressure_last_hpa": state.preseal_pressure_last_hpa,
            "preseal_trigger": state.preseal_trigger,
            "preseal_trigger_pressure_hpa": state.preseal_trigger_pressure_hpa,
            "preseal_trigger_threshold_hpa": state.preseal_trigger_threshold_hpa,
            "preseal_final_atmosphere_exit_required": state.preseal_final_atmosphere_exit_required,
            "preseal_final_atmosphere_exit_started": state.preseal_final_atmosphere_exit_started,
            "preseal_final_atmosphere_exit_verified": state.preseal_final_atmosphere_exit_verified,
            "preseal_final_atmosphere_exit_phase": state.preseal_final_atmosphere_exit_phase,
            "preseal_final_atmosphere_exit_reason": state.preseal_final_atmosphere_exit_reason,
            "preseal_watchlist_status_seen": state.preseal_watchlist_status_seen,
            "preseal_watchlist_status_accepted": state.preseal_watchlist_status_accepted,
            "preseal_watchlist_status_reason": state.preseal_watchlist_status_reason,
            "preseal_status_lag_accepted": state.preseal_status_lag_accepted,
            "preseal_status_lag_reason": state.preseal_status_lag_reason,
            "seal_transition_completed": state.seal_transition_completed,
            "seal_transition_status": state.seal_transition_status,
            "seal_transition_reason": state.seal_transition_reason,
            "control_ready_watchlist_status_accepted": state.control_ready_watchlist_status_accepted,
            "sealed_route_pressure_control_started": state.sealed_route_pressure_control_started,
            "sealed_route_last_controlled_pressure_hpa": state.sealed_route_last_controlled_pressure_hpa,
        }

    def _mark_pressure_route_sealed(
        self,
        point: CalibrationPoint,
        *,
        route: str,
        final_vent_off_command_sent: bool,
        watchlist: dict[str, Any],
        sealed_pressure_hpa: Optional[float] = None,
        preseal_pressure_peak_hpa: Optional[float] = None,
        preseal_pressure_last_hpa: Optional[float] = None,
        preseal_trigger: str = "",
        preseal_trigger_pressure_hpa: Optional[float] = None,
        preseal_trigger_threshold_hpa: Optional[float] = None,
    ) -> None:
        state = self.run_state.pressure
        state.sealed_route = "h2o" if str(route or "").strip().lower() == "h2o" else "co2"
        state.sealed_source_point_index = int(point.index)
        state.final_vent_off_command_sent = bool(final_vent_off_command_sent)
        state.sealed_pressure_hpa = self._coerce_float(sealed_pressure_hpa)
        state.preseal_pressure_peak_hpa = self._coerce_float(preseal_pressure_peak_hpa)
        state.preseal_pressure_last_hpa = self._coerce_float(preseal_pressure_last_hpa)
        state.preseal_trigger = str(preseal_trigger or "")
        state.preseal_trigger_pressure_hpa = self._coerce_float(preseal_trigger_pressure_hpa)
        state.preseal_trigger_threshold_hpa = self._coerce_float(preseal_trigger_threshold_hpa)
        state.preseal_watchlist_status_seen = bool(watchlist.get("preseal_watchlist_status_seen"))
        state.preseal_watchlist_status_accepted = bool(watchlist.get("preseal_watchlist_status_accepted"))
        state.preseal_watchlist_status_reason = str(watchlist.get("preseal_watchlist_status_reason") or "")
        state.preseal_status_lag_accepted = bool(watchlist.get("preseal_status_lag_accepted"))
        state.preseal_status_lag_reason = str(watchlist.get("preseal_status_lag_reason") or "")
        state.control_ready_watchlist_status_accepted = False
        state.sealed_route_pressure_control_started = False
        state.sealed_route_last_controlled_pressure_hpa = None

    def _clear_pressure_route_seal_state(self) -> None:
        state = self.run_state.pressure
        state.sealed_route = ""
        state.sealed_source_point_index = None
        state.final_vent_off_command_sent = False
        state.sealed_pressure_hpa = None
        state.preseal_pressure_peak_hpa = None
        state.preseal_pressure_last_hpa = None
        state.preseal_trigger = ""
        state.preseal_trigger_pressure_hpa = None
        state.preseal_trigger_threshold_hpa = None
        state.preseal_final_atmosphere_exit_required = False
        state.preseal_final_atmosphere_exit_started = False
        state.preseal_final_atmosphere_exit_verified = False
        state.preseal_final_atmosphere_exit_phase = ""
        state.preseal_final_atmosphere_exit_reason = ""
        state.preseal_watchlist_status_seen = False
        state.preseal_watchlist_status_accepted = False
        state.preseal_watchlist_status_reason = ""
        state.preseal_status_lag_accepted = False
        state.preseal_status_lag_reason = ""
        state.seal_transition_completed = False
        state.seal_transition_status = ""
        state.seal_transition_reason = ""
        state.control_ready_watchlist_status_accepted = False
        state.sealed_route_pressure_control_started = False
        state.sealed_route_last_controlled_pressure_hpa = None

    def _mark_preseal_final_atmosphere_exit(self, diagnostics: dict[str, Any]) -> None:
        state = self.run_state.pressure
        state.preseal_final_atmosphere_exit_required = bool(
            diagnostics.get("preseal_final_atmosphere_exit_required")
        )
        state.preseal_final_atmosphere_exit_started = bool(
            diagnostics.get("preseal_final_atmosphere_exit_started")
        )
        state.preseal_final_atmosphere_exit_verified = bool(
            diagnostics.get("preseal_final_atmosphere_exit_verified")
        )
        state.preseal_final_atmosphere_exit_phase = str(
            diagnostics.get("preseal_final_atmosphere_exit_phase") or ""
        )
        state.preseal_final_atmosphere_exit_reason = str(
            diagnostics.get("preseal_final_atmosphere_exit_reason") or ""
        )
        state.preseal_status_lag_accepted = bool(diagnostics.get("preseal_status_lag_accepted"))
        state.preseal_status_lag_reason = str(diagnostics.get("preseal_status_lag_reason") or "")

    def _mark_seal_transition(self, diagnostics: dict[str, Any]) -> None:
        state = self.run_state.pressure
        state.seal_transition_completed = bool(diagnostics.get("seal_transition_completed"))
        state.seal_transition_status = str(diagnostics.get("seal_transition_status") or "")
        state.seal_transition_reason = str(diagnostics.get("seal_transition_reason") or "")

    def _preseal_final_atmosphere_exit_gate(
        self,
        controller: Any,
        point: CalibrationPoint,
        *,
        route: str,
        final_vent_off_command_sent: bool,
        positive_preseal_diagnostics: Optional[Mapping[str, Any]] = None,
    ) -> PressureWaitResult:
        vent_status = self._pressure_controller_vent_status(controller)
        positive = dict(positive_preseal_diagnostics or {})
        route_text = "h2o" if str(route or "").strip().lower() == "h2o" else "co2"
        positive_preseal_pressure_evidence = (
            route_text == "co2"
            and str(positive.get("preseal_trigger") or "") == "positive_preseal_ready"
            and self._coerce_float(positive.get("preseal_trigger_pressure_hpa")) is not None
            and self._coerce_float(positive.get("preseal_pressure_peak_hpa")) is not None
            and int(positive.get("pressure_samples_count") or 0) > 0
        )
        preseal_status_lag_accepted = bool(
            vent_status == 1
            and final_vent_off_command_sent
            and str(positive.get("vent_close_verification_status") or "").upper() == "PASS"
            and bool(positive.get("vent_status_lag_accepted"))
            and bool(positive.get("status_ok_for_positive_preseal"))
            and positive_preseal_pressure_evidence
        )
        watchlist = self._preseal_watchlist_snapshot(
            controller,
            route=route_text,
            final_vent_off_command_sent=final_vent_off_command_sent,
        )
        blocked = vent_status == 1 and not preseal_status_lag_accepted
        reason = (
            "vent_status=1(in_progress_before_full_seal)"
            if blocked
            else (
                "positive_preseal_vent_status_lag_accepted_with_pressure_evidence"
                if preseal_status_lag_accepted
                else str(watchlist.get("preseal_watchlist_status_reason") or "vent_exit_verified_before_full_seal")
            )
        )
        diagnostics = {
            "route": route_text,
            "preseal_final_atmosphere_exit_required": True,
            "preseal_final_atmosphere_exit_started": True,
            "preseal_final_atmosphere_exit_verified": not blocked,
            "preseal_final_atmosphere_exit_phase": "preseal_before_full_seal",
            "preseal_final_atmosphere_exit_reason": reason,
            "final_vent_off_command_sent": bool(final_vent_off_command_sent),
            "pressure_controller_vent_status": vent_status,
            "preseal_status_lag_accepted": preseal_status_lag_accepted,
            "preseal_status_lag_reason": reason if preseal_status_lag_accepted else "",
            "positive_preseal_pressure_evidence": bool(positive_preseal_pressure_evidence),
            **watchlist,
        }
        self._mark_preseal_final_atmosphere_exit(diagnostics)
        if blocked:
            result = PressureWaitResult(
                ok=False,
                diagnostics=diagnostics,
                error="Preseal final atmosphere exit not verified before full seal",
            )
            self._record_route_trace(
                action="preseal_final_atmosphere_exit",
                route=diagnostics["route"],
                point=point,
                actual=diagnostics,
                result="fail",
                message=reason,
            )
            return result
        result = PressureWaitResult(ok=True, diagnostics=diagnostics)
        self._record_route_trace(
            action="preseal_final_atmosphere_exit",
            route=diagnostics["route"],
            point=point,
            actual=diagnostics,
            result="ok",
            message=reason,
        )
        return result

    def _seal_transition_gate(
        self,
        point: CalibrationPoint,
        *,
        route: str,
        relay_state: Any,
    ) -> PressureWaitResult:
        diagnostics = self._seal_transition_evidence(route=route, relay_state=relay_state)
        self._mark_seal_transition(diagnostics)
        if not diagnostics["seal_transition_completed"]:
            result = PressureWaitResult(
                ok=False,
                diagnostics=diagnostics,
                error="Seal transition incomplete before pressure control",
            )
            self._record_route_trace(
                action="seal_transition",
                route=diagnostics["route"],
                point=point,
                actual=diagnostics,
                result="fail",
                message=diagnostics["seal_transition_reason"],
            )
            return result
        result = PressureWaitResult(ok=True, diagnostics=diagnostics)
        self._record_route_trace(
            action="seal_transition",
            route=diagnostics["route"],
            point=point,
            actual=diagnostics,
            result="ok",
            message=diagnostics["seal_transition_reason"],
        )
        return result

    def _seal_transition_evidence(self, *, route: str, relay_state: Any) -> dict[str, Any]:
        route_text = "h2o" if str(route or "").strip().lower() == "h2o" else "co2"
        relay_payload = dict(relay_state) if isinstance(relay_state, dict) else {}
        open_channels: list[dict[str, Any]] = []
        for relay_name, channels in sorted(relay_payload.items()):
            if not isinstance(channels, dict):
                continue
            for channel, state in sorted(channels.items(), key=lambda item: str(item[0])):
                if bool(state):
                    open_channels.append(
                        {
                            "relay": str(relay_name),
                            "channel": str(channel),
                            "actual": True,
                            "target": False,
                        }
                    )
        completed = not open_channels
        status = "verified_closed" if completed else "blocked_open_channels"
        reason = (
            "all reported route valves closed before pressure control"
            if completed
            else "reported route valve still open before pressure control"
        )
        return {
            "route": route_text,
            "seal_transition_completed": completed,
            "seal_transition_status": status,
            "seal_transition_reason": reason,
            "seal_open_channels": open_channels,
            "seal_relay_state": relay_payload,
        }

    def _preseal_watchlist_snapshot(
        self,
        controller: Any,
        *,
        route: str,
        final_vent_off_command_sent: bool,
    ) -> dict[str, Any]:
        vent_status = self._pressure_controller_vent_status(controller)
        watchlist_seen = vent_status == 3
        watchlist_accepted = bool(
            watchlist_seen
            and final_vent_off_command_sent
            and str(route or "").strip().lower() != "h2o"
        )
        reason = ""
        if watchlist_seen:
            reason = (
                "preseal_watchlist_only_but_accepted"
                if watchlist_accepted
                else "preseal_watchlist_not_accepted"
            )
        return {
            "pressure_controller_vent_status": vent_status,
            "preseal_watchlist_status_seen": watchlist_seen,
            "preseal_watchlist_status_accepted": watchlist_accepted,
            "preseal_watchlist_status_reason": reason,
            "control_ready_watchlist_status_accepted": False,
        }

    @staticmethod
    def pressure_control_ready_gate_policy() -> dict[str, Any]:
        return {
            "policy_id": "run001_a1_v1_compatible_pressure_ready_gate",
            "version": 1,
            "v1_semantic_compatibility": True,
            "vent_status_2_strategy": (
                "warning_only_when_controller_allows_control_and_seal_output_isolation_pressure_evidence_is_valid"
            ),
            "hard_blockers": [
                "vent_status_unavailable",
                "vent_status_in_progress_or_dangerous",
                "vent_status_not_allowed_by_controller",
                "final_vent_off_not_confirmed",
                "preseal_exit_not_verified",
                "seal_transition_not_completed",
                "seal_open_channels_present",
                "output_state_not_idle",
                "isolation_state_not_open",
                "pressure_evidence_missing",
            ],
            "warnings": [
                "vent_status_2_observed_accepted_under_v1_compatible_pressure_evidence",
                "output_state_active_accepted_for_continued_sealed_route_control",
            ],
            "evidence_required_before_setpoint": [
                "final_vent_off_command_sent",
                "preseal_final_atmosphere_exit_verified",
                "seal_transition_completed",
                "output_state_idle_or_active_continued_sealed_route_control",
                "isolation_state_open",
                "pressure_observed_before_control",
            ],
            "post_gate_evidence_required": [
                "setpoint_accepted",
                "output_enabled",
                "pressure_in_limits",
            ],
        }

    def _pressure_vent_status_interpretation(self, controller: Any, vent_status: Optional[int]) -> dict[str, Any]:
        if vent_status is None:
            return {
                "raw": None,
                "value": None,
                "classification": "unavailable",
                "text": "unavailable",
                "source": "v2_fallback",
            }
        describer = getattr(controller, "describe_vent_status", None)
        if callable(describer):
            try:
                described = describer(vent_status)
            except Exception:
                described = None
            if isinstance(described, dict):
                payload = dict(described)
                payload.setdefault("raw", vent_status)
                payload.setdefault("value", vent_status)
                payload.setdefault("source", "controller.describe_vent_status")
                return payload
        mapping = {
            0: "idle",
            1: "in_progress",
            2: "completed_latched_or_timed_out_ambiguous",
            3: "trapped_pressure_or_watchlist",
            4: "aborted",
        }
        classification = mapping.get(int(vent_status), f"unknown_status_{int(vent_status)}")
        return {
            "raw": vent_status,
            "value": vent_status,
            "classification": classification,
            "text": classification,
            "source": "v2_fallback",
        }

    def _pressure_vent_status_allows_control(self, controller: Any, vent_status: Optional[int]) -> bool:
        if vent_status is None:
            return False
        checker = getattr(controller, "vent_status_allows_control", None)
        if callable(checker):
            try:
                return bool(checker(vent_status))
            except Exception:
                return False
        return int(vent_status) == 0

    def _sealed_pressure_evidence(self, seal_context: dict[str, Any]) -> dict[str, Any]:
        keys = (
            "pressure_hpa",
            "sealed_pressure_hpa",
            "preseal_pressure_peak_hpa",
            "preseal_pressure_last_hpa",
            "preseal_trigger_pressure_hpa",
        )
        values = {
            key: self._coerce_float(seal_context.get(key))
            for key in keys
        }
        observed_values = {
            key: value
            for key, value in values.items()
            if value is not None
        }
        trigger = str(seal_context.get("preseal_trigger") or "").strip()
        return {
            **values,
            "preseal_trigger": trigger,
            "pressure_observed": bool(observed_values),
            "observed_keys": sorted(observed_values),
        }

    @staticmethod
    def _pressure_gate_message(hard_blockers: list[str], warnings: list[str]) -> str:
        if hard_blockers:
            return hard_blockers[0]
        if warnings:
            return warnings[0]
        return "Pressure controller ready after sealed-route gate"

    def _pressure_control_ready_gate(
        self,
        controller: Any,
        point: CalibrationPoint,
        *,
        seal_context: dict[str, Any],
    ) -> PressureWaitResult:
        vent_status = self._pressure_controller_vent_status(controller)
        vent_interpreted = self._pressure_vent_status_interpretation(controller, vent_status)
        output_state = self._pressure_controller_output_state(controller)
        isolation_state = self._pressure_controller_isolation_state(controller)
        pressure_evidence = self._sealed_pressure_evidence(dict(seal_context))
        hard_blockers: list[str] = []
        warnings: list[str] = []
        decision_basis: list[str] = []
        if vent_status is None:
            hard_blockers.append("vent_status_unavailable")
        elif int(vent_status) == 1:
            lag_accepted = bool(
                seal_context.get("preseal_status_lag_accepted")
                and seal_context.get("preseal_final_atmosphere_exit_verified")
                and seal_context.get("final_vent_off_command_sent")
                and pressure_evidence.get("pressure_observed")
            )
            if lag_accepted:
                warnings.append("vent_status=1 observed after positive preseal; accepted with prior vent-close and pressure evidence")
                decision_basis.append("vent_status_1_lag_accepted_after_positive_preseal_verification")
            else:
                hard_blockers.append("vent_status=1(in_progress_after_seal)")
        elif int(vent_status) == 3:
            hard_blockers.append("vent_status=3(watchlist_only_after_seal)")
        elif int(vent_status) != 0 and not self._pressure_vent_status_allows_control(controller, vent_status):
            hard_blockers.append(f"vent_status={int(vent_status)}(not_allowed_by_controller)")
        elif int(vent_status) == 2:
            warnings.append("vent_status=2 observed; accepted under V1-compatible pressure evidence")
            decision_basis.append("vent_status=2_allowed_by_controller_legacy_semantics")
        else:
            decision_basis.append("vent_status_idle_or_controller_allowed")

        if not bool(seal_context.get("final_vent_off_command_sent")):
            hard_blockers.append("final_vent_off_not_confirmed")
        else:
            decision_basis.append("final_vent_off_confirmed")
        if not bool(seal_context.get("preseal_final_atmosphere_exit_verified")):
            hard_blockers.append("preseal_exit_not_verified")
        else:
            decision_basis.append("preseal_exit_verified")
        if not bool(seal_context.get("seal_transition_completed")):
            hard_blockers.append("seal_transition_not_completed")
        else:
            decision_basis.append("seal_transition_completed")
        if list(seal_context.get("seal_open_channels") or []):
            hard_blockers.append("seal_open_channels_present")
        elif str(seal_context.get("seal_transition_status") or "") == "verified_closed":
            decision_basis.append("seal_transition_verified_closed")
        continuing_sealed_control = bool(seal_context.get("sealed_route_pressure_control_started"))
        if output_state is None:
            hard_blockers.append("output_state_unavailable")
        elif int(output_state) != 0 and not (continuing_sealed_control and int(output_state) == 1):
            hard_blockers.append(f"output_state={int(output_state)}(not_idle_before_control)")
        elif int(output_state) == 1:
            decision_basis.append("output_state_active_for_continued_sealed_route_control")
            warnings.append("output_state=1 accepted for continued sealed-route setpoint update")
        else:
            decision_basis.append("output_state_idle_before_control")
        if isolation_state is None:
            hard_blockers.append("isolation_state_unavailable")
        elif int(isolation_state) != 1:
            hard_blockers.append(f"isolation_state={int(isolation_state)}(not_open_before_control)")
        else:
            decision_basis.append("isolation_state_open_before_control")
        if not bool(pressure_evidence.get("pressure_observed")):
            hard_blockers.append("pressure_evidence_missing")
        else:
            decision_basis.append("pressure_observed_before_control")

        decision = "blocked" if hard_blockers else "ready"
        diagnostics = {
            **dict(seal_context),
            "pressure_controller_vent_status": vent_status,
            "vent_status_raw": vent_status,
            "vent_status_interpreted": vent_interpreted,
            "pressure_controller_output_state": output_state,
            "pressure_controller_isolation_state": isolation_state,
            "pressure_evidence": pressure_evidence,
            "redundant_vent_command_skipped": True,
            "control_ready_watchlist_status_accepted": False,
            "pressure_gate_policy": self.pressure_control_ready_gate_policy(),
            "gate_decision": decision,
            "decision_basis": decision_basis,
            "v1_semantic_compatibility": {
                "compatible": True,
                "basis": "V1 Pace5000.vent_status_allows_control plus output/isolation/seal/pressure evidence",
                "vent_status_2_allowed_by_controller": (
                    vent_status == 2 and self._pressure_vent_status_allows_control(controller, vent_status)
                ),
            },
            "warnings": warnings,
            "hard_blockers": hard_blockers,
        }
        if hard_blockers:
            diagnostics["control_ready_status"] = "blocked"
            diagnostics["control_ready_failure_reason"] = self._pressure_gate_message(hard_blockers, warnings)
            result = PressureWaitResult(
                ok=False,
                diagnostics=diagnostics,
                error="Pressure controller not ready for control after route seal",
            )
            self._record_route_trace(
                action="pressure_control_ready_gate",
                route=diagnostics["route"],
                point=point,
                actual=diagnostics,
                result="fail",
                message=diagnostics["control_ready_failure_reason"],
            )
            return result
        diagnostics["control_ready_status"] = "ready"
        diagnostics["control_ready_decision_basis"] = decision_basis
        result = PressureWaitResult(ok=True, diagnostics=diagnostics)
        self._record_route_trace(
            action="pressure_control_ready_gate",
            route=diagnostics["route"],
            point=point,
            actual=diagnostics,
            result="ok",
            message=self._pressure_gate_message([], warnings),
        )
        return result

    def _pressure_controller_vent_status(self, controller: Any) -> Optional[int]:
        for method_name in ("get_vent_status", "read_vent_status", "query_vent_status"):
            method = getattr(controller, method_name, None)
            if not callable(method):
                continue
            try:
                value = self._coerce_float(method())
            except Exception:
                continue
            if value is not None:
                return int(value)
        for attr_name in ("vent_status", "pace_vent_status"):
            if not hasattr(controller, attr_name):
                continue
            value = self._coerce_float(getattr(controller, attr_name))
            if value is not None:
                return int(value)
        for method_name in ("status", "fetch_all"):
            method = getattr(controller, method_name, None)
            if not callable(method):
                continue
            try:
                snapshot = self._normalize_snapshot(method())
            except Exception:
                continue
            value = self._pick_numeric(snapshot, "vent_status", "pace_vent_status")
            if value is not None:
                return int(value)
        return None

    def _make_preseal_observation_reader(self) -> Optional[Callable[[], Optional[float]]]:
        gauge = self.host._device("pressure_meter", "pressure_gauge")
        if gauge is not None:
            return lambda gauge=gauge: self._read_pressure_from_device(gauge, source="pressure_gauge")
        return None

    def _read_pressure_from_device(self, device: Any, *, source: str) -> Optional[float]:
        for method_name in ("read_pressure", "read_pressure_hpa", "get_pressure", "get_pressure_hpa"):
            method = getattr(device, method_name, None)
            if not callable(method):
                continue
            try:
                return self._coerce_float(method())
            except Exception as exc:
                self.host._log(f"Startup pressure precheck read failed ({source}): {exc}")
                return None
        for method_name in ("status", "fetch_all"):
            method = getattr(device, method_name, None)
            if not callable(method):
                continue
            try:
                snapshot = self._normalize_snapshot(method())
            except Exception as exc:
                self.host._log(f"Startup pressure precheck read failed ({source}): {exc}")
                return None
            value = self._pick_numeric(snapshot, "pressure_hpa", "pressure")
            if value is not None:
                return value
        return None

    def _read_bool_state(self, device: Any, *names: str) -> Optional[bool]:
        for name in names:
            if not name:
                continue
            member = getattr(device, name, None)
            try:
                value = member() if callable(member) else member
            except Exception:
                continue
            normalized = self._coerce_bool(value)
            if normalized is not None:
                return normalized
        return None

    def _read_int_state(self, device: Any, *names: str) -> Optional[int]:
        for name in names:
            if not name:
                continue
            member = getattr(device, name, None)
            try:
                value = member() if callable(member) else member
            except Exception:
                continue
            parsed = self._coerce_float(value)
            if parsed is not None:
                return int(parsed)
            normalized = self._coerce_bool(value)
            if normalized is not None:
                return 1 if bool(normalized) else 0
        return None

    def _sleep_with_stop(self, duration_s: float) -> None:
        started_at = time.time()
        while True:
            self.host._check_stop()
            remaining = float(duration_s) - (time.time() - started_at)
            if remaining <= 0:
                return
            time.sleep(min(1.0, max(0.05, remaining)))

    @staticmethod
    def _normalize_snapshot(snapshot: Any) -> dict[str, Any]:
        if isinstance(snapshot, dict):
            if isinstance(snapshot.get("data"), dict):
                return dict(snapshot["data"])
            return dict(snapshot)
        return {}

    @staticmethod
    def _pick_numeric(snapshot: dict[str, Any], *keys: str) -> Optional[float]:
        for key in keys:
            value = snapshot.get(key)
            parsed = PressureControlService._coerce_float(value)
            if parsed is not None:
                return parsed
        return None

    @staticmethod
    def _coerce_float(value: Any, default: Optional[float] = None) -> Optional[float]:
        if value is None:
            return default
        try:
            return float(value)
        except Exception:
            return default

    @staticmethod
    def _coerce_bool(value: Any) -> Optional[bool]:
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return bool(int(value))
        if isinstance(value, str):
            text = value.strip().lower()
            if text in {"1", "true", "on", "open", "enabled", "yes"}:
                return True
            if text in {"0", "false", "off", "closed", "disabled", "no"}:
                return False
        return None

    def _set_h2o_prepared_target(self, value: Optional[float]) -> None:
        self.run_state.humidity.h2o_pressure_prepared_target = value
        setattr(self.host, "_h2o_pressure_prepared_target", value)

    def _set_active_post_h2o_co2_zero_flush(self, value: bool) -> None:
        flag = bool(value)
        self.run_state.humidity.active_post_h2o_co2_zero_flush = flag
        setattr(self.host, "_active_post_h2o_co2_zero_flush", flag)

    def _current_pressure(self) -> Optional[float]:
        reader = self.host._make_pressure_reader()
        try:
            return None if reader is None else self.host._as_float(reader())
        except Exception:
            return None

    def _record_route_trace(self, **kwargs: Any) -> None:
        status_service = getattr(self.host, "status_service", None)
        recorder = getattr(status_service, "record_route_trace", None)
        if callable(recorder):
            recorder(**kwargs)
