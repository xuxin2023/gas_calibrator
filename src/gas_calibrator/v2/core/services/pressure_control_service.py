from __future__ import annotations

from dataclasses import dataclass, field, replace
import time
from typing import Any, Callable, Optional

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

    def __init__(self, context: OrchestrationContext, run_state: RunState, *, host: Any) -> None:
        self.context = context
        self.run_state = run_state
        self.host = host

    def _remember_startup_pressure_precheck_result(self, result: StartupPressurePrecheckResult) -> None:
        setattr(self.host, "_startup_pressure_precheck_result", result)

    def set_pressure_controller_vent(self, vent_on: bool, reason: str = "") -> None:
        controller = self.host._device("pressure_controller")
        if controller is None:
            return
        extra = f" ({reason})" if reason else ""
        trace_result = "ok"
        trace_message = reason or ("vent on" if vent_on else "vent off")
        try:
            if vent_on:
                enter = getattr(controller, "enter_atmosphere_mode", None)
                if callable(enter):
                    enter(
                        timeout_s=float(self.host._cfg_get("workflow.pressure.vent_transition_timeout_s", 30.0)),
                        hold_open=bool(self.host._cfg_get("workflow.pressure.continuous_atmosphere_hold", True)),
                        hold_interval_s=float(self.host._cfg_get("workflow.pressure.vent_hold_interval_s", 2.0)),
                    )
                else:
                    self.host._call_first(controller, ("set_output",), False)
                    self.host._call_first(controller, ("set_isolation_open",), True)
                    self.host._call_first(controller, ("vent",), True)
            else:
                exit_mode = getattr(controller, "exit_atmosphere_mode", None)
                if callable(exit_mode):
                    exit_mode(timeout_s=float(self.host._cfg_get("workflow.pressure.vent_transition_timeout_s", 30.0)))
                else:
                    self.host._call_first(controller, ("set_output",), False)
                    self.host._call_first(controller, ("vent",), False)
                    self.host._call_first(controller, ("set_isolation_open",), True)
            self.host._log(f"Pressure controller vent={'ON' if vent_on else 'OFF'}{extra}")
        except Exception as exc:
            self.host._log(f"Pressure controller vent command failed: {exc}")
            trace_result = "fail"
            trace_message = str(exc)
        self._record_route_trace(
            action="set_vent",
            target={"vent_on": bool(vent_on)},
            actual={"pressure_hpa": self._current_pressure()},
            result=trace_result,
            message=trace_message,
        )
        if vent_on:
            wait_s = max(0.0, float(self.host._cfg_get("workflow.pressure.vent_time_s", 0.0)))
            if wait_s > 0:
                time.sleep(wait_s)

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
        self.host._set_pressure_controller_vent(False, reason="before setpoint control")
        self.host._call_first(controller, ("set_setpoint", "set_pressure_hpa", "set_pressure"), target)
        self.host._enable_pressure_controller_output(reason="after setpoint update")
        timeout_s = float(self.host._cfg_get("workflow.pressure.stabilize_timeout_s", 120.0))
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
        return result

    def pressurize_and_hold(self, point: CalibrationPoint, route: str = "co2") -> PressureWaitResult:
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
            return result
        route_text = str(route or "").strip().lower()
        if route_text == "h2o":
            self.host._capture_preseal_dewpoint_snapshot()
        self.host._set_pressure_controller_vent(False, reason=f"before {route_text.upper()} pressure seal")
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
        if route_text != "h2o":
            pressure_reader = self._make_preseal_observation_reader()
            preseal_trigger_threshold_hpa = self._coerce_float(
                self.host._cfg_get("workflow.pressure.co2_preseal_pressure_gauge_trigger_hpa", 1110.0)
            )
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
                    if (
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
            if route_text == "h2o":
                self.host._set_h2o_path(False, point)
            else:
                self.host._apply_valve_states([])
            reader = self.host._make_pressure_reader()
            final_pressure = None if reader is None else reader()
            if route_text != "h2o" and preseal_pressure_peak is not None:
                self.host._log(
                    f"{route_text.upper()} route sealed for pressure control "
                    f"(pre-seal peak={preseal_pressure_peak:.3f} hPa, "
                    f"pre-seal last={preseal_pressure_last:.3f} hPa, "
                    f"sealed pressure={final_pressure})"
                )
            else:
                self.host._log(f"{route_text.upper()} route sealed for pressure control")
            result = PressureWaitResult(
                ok=True,
                final_pressure_hpa=final_pressure,
                diagnostics={
                    "route": route_text,
                    "preseal_trigger": preseal_trigger_source,
                    "preseal_trigger_pressure_hpa": preseal_trigger_pressure_hpa,
                    "preseal_trigger_threshold_hpa": preseal_trigger_threshold_hpa,
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
                    "preseal_trigger": preseal_trigger_source,
                    "preseal_trigger_pressure_hpa": preseal_trigger_pressure_hpa,
                    "preseal_trigger_threshold_hpa": preseal_trigger_threshold_hpa,
                },
                result="ok",
                message=f"{route_text.upper()} route sealed for pressure control",
            )
            return result
        finally:
            if route_text != "h2o":
                self._set_active_post_h2o_co2_zero_flush(False)

    def wait_after_pressure_stable_before_sampling(self, point: CalibrationPoint) -> PressureWaitResult:
        if self.host._collect_only_fast_path_enabled():
            self.host._log("Collect-only mode: post-pressure sample hold skipped")
            result = PressureWaitResult(ok=True, diagnostics={"skipped": "collect_only_fast_path"})
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
            self._record_route_trace(
                action="wait_post_pressure",
                point=point,
                target={"hold_s": hold_s},
                result="ok",
                message="Post-pressure wait disabled",
            )
            return result
        started_at = time.time()
        while time.time() - started_at < hold_s:
            self.host._check_stop()
            time.sleep(min(1.0, max(0.05, hold_s - (time.time() - started_at))))
        result = PressureWaitResult(ok=True, diagnostics={"hold_s": hold_s})
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
