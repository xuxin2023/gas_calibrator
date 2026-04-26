from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
import json
import math
from pathlib import Path
import time
from typing import Any, Callable, Mapping, Optional

from ...exceptions import StabilityTimeoutError
from ..models import CalibrationPhase, CalibrationPoint
from ..orchestration_context import OrchestrationContext
from ..run_state import RunState
from ..stability_checker import StabilityType
from .sampling_service import read_device_snapshot_with_retry, read_numeric_with_retry


@dataclass(frozen=True)
class WaitResult:
    ok: bool
    timed_out: bool = False
    reused_previous_stability: bool = False
    target_c: Optional[float] = None
    final_temp_c: Optional[float] = None
    attempt_count: int = 0
    diagnostics: dict[str, Any] = field(default_factory=dict)
    error: Optional[str] = None


class _TemperatureTransitionStalledError(RuntimeError):
    def __init__(
        self,
        message: str,
        *,
        current_temp_c: Optional[float] = None,
        run_state: Optional[int] = None,
    ) -> None:
        super().__init__(message)
        self.current_temp_c = current_temp_c
        self.run_state = run_state


class TemperatureControlService:
    """Temperature wait and snapshot helpers with structured results."""

    TEMPERATURE_FRAME_KEYS = ("temperature_c", "temp_c", "chamber_temp_c")
    ANALYZER_TEMPERATURE_KEYS = ("chamber_temp_c", "temp_c", "case_temp_c")

    def __init__(self, context: OrchestrationContext, run_state: RunState, *, host: Any) -> None:
        self.context = context
        self.run_state = run_state
        self.host = host

    def _record_temperature_timing(self, event_name: str, event_type: str, **kwargs: Any) -> None:
        recorder = getattr(self.host, "_record_workflow_timing", None)
        if callable(recorder):
            recorder(event_name, event_type, **kwargs)

    def _mark_temperature_chamber_settle(
        self,
        status: str,
        *,
        timestamp: Optional[str] = None,
    ) -> str:
        recorded_at = timestamp or datetime.now(timezone.utc).isoformat()
        self.run_state.temperature.chamber_settle_status = str(status or "")
        if status in {"passed", "already_satisfied", "not_applicable", "skipped"}:
            self.run_state.temperature.chamber_settle_passed_at = recorded_at
            setattr(self.host, "_temperature_chamber_settle_passed_at", recorded_at)
        setattr(self.host, "_temperature_chamber_settle_status", str(status or ""))
        return recorded_at

    def set_temperature_for_point(self, point: CalibrationPoint, *, phase: str) -> WaitResult:
        target_c = float(point.temp_chamber_c)
        chamber_expected_max_s = float(self.host._cfg_get("workflow.stability.temperature.timeout_s", 3600.0))
        chamber = self.host._device("temperature_chamber")
        if chamber is None:
            recorded_at = self._mark_temperature_chamber_settle("not_applicable")
            self._record_temperature_timing(
                "temperature_chamber_settle_start",
                "start",
                stage="temperature_chamber_settle",
                point=point,
                expected_max_s=chamber_expected_max_s,
                wait_reason="temperature_chamber_not_applicable",
            )
            self._record_temperature_timing(
                "temperature_chamber_settle_end",
                "end",
                stage="temperature_chamber_settle",
                point=point,
                duration_s=0.0,
                expected_max_s=chamber_expected_max_s,
                decision="not_applicable",
                chamber_temperature_c=None,
            )
            return self._store_result(
                WaitResult(
                    ok=True,
                    target_c=target_c,
                    diagnostics={
                        "temperature_chamber_settle_status": "not_applicable",
                        "temperature_chamber_settle_passed_at": recorded_at,
                        "skipped": "temperature chamber unavailable",
                    },
                )
            )

        tol_c = abs(float(self.host._cfg_get("workflow.stability.temperature.tol", 0.2)))
        timeout_s = chamber_expected_max_s
        command_offset_c = float(self.host._cfg_get("workflow.stability.temperature.command_offset_c", 0.0) or 0.0)
        wait_for_target_before_continue = bool(
            self.host._cfg_get("workflow.stability.temperature.wait_for_target_before_continue", True)
        )
        require_chamber_settle_before_analyzer = bool(
            self.host._cfg_get("workflow.stability.temperature.require_chamber_settle_before_analyzer", False)
        )
        restart_on_target_change = bool(
            self.host._cfg_get("workflow.stability.temperature.restart_on_target_change", False)
        )
        reuse_running_in_tol = bool(
            self.host._cfg_get("workflow.stability.temperature.reuse_running_in_tol_without_soak", True)
        )
        transition_check_window_s = float(
            self.host._cfg_get("workflow.stability.temperature.transition_check_window_s", 120.0) or 0.0
        )
        transition_min_delta_c = abs(
            float(self.host._cfg_get("workflow.stability.temperature.transition_min_delta_c", 0.3) or 0.0)
        )
        soak_s = self._temperature_soak_after_reach_s()
        command_target_c = target_c + command_offset_c

        last_target_c = self.run_state.temperature.last_target_c
        target_changed = last_target_c is None or not math.isclose(float(last_target_c), target_c, abs_tol=1e-9)
        if target_changed:
            self.run_state.temperature.last_target_c = target_c
            self.run_state.temperature.last_soak_done = False

        need_soak = soak_s > 0 and not self.run_state.temperature.last_soak_done
        self.host._update_status(
            phase=CalibrationPhase.H2O_ROUTE if phase == "h2o" else CalibrationPhase.CO2_ROUTE,
            current_point=point,
            message=f"{phase.upper()} chamber wait {target_c:g}C",
        )
        reader = self._make_temperature_reader(chamber)
        if reader is None:
            return self._store_result(
                WaitResult(
                    ok=True,
                    target_c=target_c,
                    attempt_count=1,
                    diagnostics={"reader": "unavailable"},
                )
            )

        run_state_reader = self._make_run_state_reader(chamber)
        current_temp = self._safe_read(reader)
        current_run_state: Optional[int] = None
        current_running = True

        if target_changed and need_soak and reuse_running_in_tol and current_temp is not None:
            current_run_state = self._safe_read_int(run_state_reader)
            current_running = current_run_state is None or current_run_state == 1
            in_target_range = abs(current_temp - target_c) <= tol_c
            if in_target_range and current_running:
                self.host._log(
                    "Temperature chamber already in target range at startup; "
                    f"reuse current thermal state for soak/stability wait: temp={current_temp:.3f}C, "
                    f"target={target_c:g}C, run_state={current_run_state}"
                )
                target_changed = False
            elif in_target_range and not current_running:
                self.host._log(
                    "Temperature chamber is in target range but controller is not running; "
                    f"restart and wait for stability: temp={current_temp:.3f}C, "
                    f"target={target_c:g}C, run_state={current_run_state}"
                )

        if not target_changed:
            current_run_state = self._safe_read_int(run_state_reader)
            current_running = current_run_state is None or current_run_state == 1

        if (
            not target_changed
            and self.run_state.temperature.last_soak_done
            and current_running
            and current_temp is not None
            and abs(current_temp - target_c) <= tol_c
        ):
            recorded_at = self._mark_temperature_chamber_settle("already_satisfied")
            self._record_temperature_timing(
                "temperature_chamber_settle_start",
                "start",
                stage="temperature_chamber_settle",
                point=point,
                expected_max_s=timeout_s,
                wait_reason="temperature_chamber_already_satisfied",
            )
            self._record_temperature_timing(
                "temperature_chamber_settle_end",
                "end",
                stage="temperature_chamber_settle",
                point=point,
                duration_s=0.0,
                expected_max_s=timeout_s,
                decision="already_satisfied",
                chamber_temperature_c=current_temp,
            )
            self.host._log(
                "Temperature chamber target unchanged; reuse current thermal state: "
                f"temp={current_temp:.3f}C, target={target_c:g}C, run_state={current_run_state}"
            )
            self._set_ready_target(target_c)
            return self._store_result(
                WaitResult(
                    ok=True,
                    reused_previous_stability=True,
                    target_c=target_c,
                    final_temp_c=current_temp,
                    attempt_count=1,
                    diagnostics={
                        "reuse_previous": True,
                        "tolerance_c": tol_c,
                        "temperature_chamber_settle_status": "already_satisfied",
                        "temperature_chamber_settle_passed_at": recorded_at,
                    },
                )
            )

        start_result = WaitResult(ok=True, attempt_count=0)
        command_diagnostics: dict[str, Any] = {}
        if target_changed:
            self._set_ready_target(None)
            try:
                if restart_on_target_change:
                    self.host._call_first(chamber, ("stop",))
                self.host._call_first(
                    chamber,
                    ("set_temp_c", "set_temperature_c", "set_temperature"),
                    command_target_c,
                )
                command_diagnostics = self._verify_commanded_setpoint(
                    chamber,
                    command_target_c=command_target_c,
                    tol_c=tol_c,
                )
            except Exception as exc:
                self.host._log(f"Temperature chamber command failed: {exc}")
                return self._store_result(
                    WaitResult(
                        ok=False,
                        target_c=target_c,
                        attempt_count=1,
                        diagnostics={"stage": "set_temperature", **command_diagnostics},
                        error=str(exc),
                    )
                )
        elif current_running:
            self.host._log(
                "Temperature chamber target unchanged; keep current command: "
                f"target={target_c:g}C, run_state={current_run_state}"
            )
        else:
            self.host._log(
                "Temperature chamber target unchanged but controller is not running; "
                f"retry start: target={target_c:g}C, run_state={current_run_state}"
            )

        if target_changed or not current_running:
            start_result = self._start_chamber(chamber)
            if not start_result.ok:
                return self._store_result(
                    WaitResult(
                        ok=False,
                        target_c=target_c,
                        attempt_count=max(1, start_result.attempt_count),
                        diagnostics={"stage": "start", **start_result.diagnostics},
                        error=start_result.error,
                    )
                )

        if wait_for_target_before_continue is False:
            recorded_at = self._mark_temperature_chamber_settle("skipped")
            self._record_temperature_timing(
                "temperature_chamber_settle_start",
                "start",
                stage="temperature_chamber_settle",
                point=point,
                expected_max_s=timeout_s,
                wait_reason="temperature_chamber_wait_skipped",
            )
            self._record_temperature_timing(
                "temperature_chamber_settle_end",
                "end",
                stage="temperature_chamber_settle",
                point=point,
                duration_s=0.0,
                expected_max_s=timeout_s,
                decision="skipped",
                chamber_temperature_c=current_temp,
            )
            self.host._log(
                "Temperature chamber wait skipped by configuration: "
                f"target={target_c:g}C, command={command_target_c:g}C"
            )
            self.run_state.temperature.last_soak_done = False
            return self._store_result(
                WaitResult(
                    ok=True,
                    target_c=target_c,
                    final_temp_c=current_temp,
                    attempt_count=max(1, start_result.attempt_count),
                    diagnostics={
                        "wait_skipped": True,
                        "command_target_c": command_target_c,
                        "wait_for_target_before_continue": False,
                        "temperature_chamber_settle_status": "skipped",
                        "temperature_chamber_settle_passed_at": recorded_at,
                        **command_diagnostics,
                    },
                )
            )

        monitored_reader = self._make_transition_monitor_reader(
            reader,
            chamber,
            target_c=target_c,
            tol_c=tol_c,
            target_changed=target_changed,
            transition_check_window_s=transition_check_window_s,
            transition_min_delta_c=transition_min_delta_c,
        )
        monitored_reader = self._make_live_snapshot_reader(monitored_reader, reason="temperature_wait")
        chamber_wait_started = time.time()
        self._record_temperature_timing(
            "temperature_chamber_settle_start",
            "start",
            stage="temperature_chamber_settle",
            point=point,
            expected_max_s=timeout_s,
            wait_reason="temperature_chamber_settle",
        )
        try:
            stability = self._wait_for_temperature_stability(
                monitored_reader,
                soak_s=soak_s,
                timeout_s=timeout_s,
            )
            chamber_elapsed_s = max(0.0, time.time() - chamber_wait_started)
            if bool(getattr(stability, "stopped", False)):
                self.host._log("Temperature chamber wait interrupted by stop request")
                self.host._check_stop()
            if not bool(getattr(stability, "stable", True)):
                self._mark_temperature_chamber_settle("failed")
                self._record_temperature_timing(
                    "temperature_chamber_settle_end",
                    "fail",
                    stage="temperature_chamber_settle",
                    point=point,
                    duration_s=getattr(stability, "elapsed_s", chamber_elapsed_s),
                    expected_max_s=timeout_s,
                    decision="not_stable",
                    chamber_temperature_c=self._safe_read(reader),
                    error_code="temperature_chamber_not_stable",
                )
                self._set_ready_target(None)
                self.run_state.temperature.last_soak_done = False
                self.host._log("Temperature chamber did not stabilize")
                return self._store_result(
                    WaitResult(
                        ok=False,
                        target_c=target_c,
                        final_temp_c=self._safe_read(reader),
                        attempt_count=1,
                        diagnostics={
                            "elapsed_s": getattr(stability, "elapsed_s", None),
                            "stopped": bool(getattr(stability, "stopped", False)),
                            "timed_out": bool(getattr(stability, "timed_out", False)),
                            "soak_after_reach_s": soak_s,
                            "temperature_chamber_settle_status": "failed",
                        },
                        error="Temperature chamber did not stabilize",
                    )
                )
            chamber_passed_at = self._mark_temperature_chamber_settle("passed")
            self._record_temperature_timing(
                "temperature_chamber_settle_end",
                "end",
                stage="temperature_chamber_settle",
                point=point,
                duration_s=getattr(stability, "elapsed_s", chamber_elapsed_s),
                expected_max_s=timeout_s,
                decision="ok",
                chamber_temperature_c=self._safe_read(reader) or getattr(stability, "last_value", None),
            )
            if not self._wait_analyzer_chamber_temp_stable(target_c):
                self._set_ready_target(None)
                self.run_state.temperature.last_soak_done = False
                analyzer_evidence = dict(self.run_state.temperature.analyzer_chamber_temp_stability_evidence or {})
                failure_reason = str(
                    analyzer_evidence.get("failure_reason")
                    or "Analyzer chamber temperature did not stabilize"
                )
                self.host._log("Analyzer chamber temperature did not stabilize after chamber wait")
                return self._store_result(
                    WaitResult(
                        ok=False,
                        target_c=target_c,
                        final_temp_c=self._safe_read(reader),
                        attempt_count=max(1, start_result.attempt_count),
                        diagnostics={
                            "stage": "analyzer_chamber_temperature_stability",
                            "failure_stage": analyzer_evidence.get(
                                "failure_stage",
                                "analyzer_chamber_temperature_stability",
                            ),
                            "failure_reason": failure_reason,
                            "soak_after_reach_s": soak_s,
                            "temperature_chamber_settle_status": "passed",
                            "temperature_chamber_settle_passed_at": chamber_passed_at,
                            "analyzer_chamber_temperature_stability": analyzer_evidence,
                        },
                        error=failure_reason,
                    )
                )
            self._set_ready_target(target_c)
            self.run_state.temperature.last_soak_done = True
            return self._store_result(
                WaitResult(
                    ok=True,
                    target_c=target_c,
                    final_temp_c=self._safe_read(reader) or getattr(stability, "last_value", None),
                    attempt_count=max(1, start_result.attempt_count),
                    diagnostics={
                        "soak_after_reach_s": soak_s,
                        "elapsed_s": getattr(stability, "elapsed_s", None),
                        "command_target_c": command_target_c,
                        "temperature_chamber_settle_status": "passed",
                        "temperature_chamber_settle_passed_at": chamber_passed_at,
                        **command_diagnostics,
                    },
                )
            )
        except _TemperatureTransitionStalledError as exc:
            self._mark_temperature_chamber_settle("failed")
            self._record_temperature_timing(
                "temperature_chamber_settle_end",
                "fail",
                stage="temperature_chamber_settle",
                point=point,
                duration_s=max(0.0, time.time() - chamber_wait_started),
                expected_max_s=timeout_s,
                decision="transition_stalled",
                chamber_temperature_c=exc.current_temp_c,
                error_code="temperature_chamber_transition_stalled",
            )
            self._set_ready_target(None)
            self.run_state.temperature.last_soak_done = False
            self.host._log(str(exc))
            return self._store_result(
                WaitResult(
                    ok=False,
                    target_c=target_c,
                    final_temp_c=exc.current_temp_c,
                    attempt_count=max(1, start_result.attempt_count),
                    diagnostics={
                        "stage": "transition",
                        "transition_check_window_s": transition_check_window_s,
                        "transition_min_delta_c": transition_min_delta_c,
                        "run_state": exc.run_state,
                    },
                    error=str(exc),
                )
            )
        except StabilityTimeoutError as exc:
            final_temp = self._safe_read(reader)
            self._record_temperature_timing(
                "temperature_chamber_settle_timeout",
                "timeout",
                stage="temperature_chamber_settle",
                point=point,
                duration_s=max(0.0, time.time() - chamber_wait_started),
                expected_max_s=timeout_s,
                decision="timeout",
                chamber_temperature_c=final_temp,
                error_code="temperature_chamber_settle_timeout",
            )
            if require_chamber_settle_before_analyzer:
                self._mark_temperature_chamber_settle("timeout")
                self._set_ready_target(None)
                self.run_state.temperature.last_soak_done = False
                self.host._log(f"Temperature chamber stability timeout: {exc}")
                return self._store_result(
                    WaitResult(
                        ok=False,
                        timed_out=True,
                        target_c=target_c,
                        final_temp_c=final_temp,
                        attempt_count=max(1, start_result.attempt_count),
                        diagnostics={
                            "stage": "temperature_chamber_settle",
                            "temperature_chamber_settle_status": "timeout",
                            "require_chamber_settle_before_analyzer": True,
                            "tolerance_c": tol_c,
                        },
                        error=str(exc),
                    )
                )
            if final_temp is not None and abs(final_temp - target_c) <= tol_c:
                chamber_passed_at = self._mark_temperature_chamber_settle("already_satisfied")
                if self._wait_analyzer_chamber_temp_stable(target_c):
                    self._set_ready_target(target_c)
                    self.run_state.temperature.last_soak_done = True
                    self.host._log(
                        f"Temperature chamber stability timeout reached in-band value {final_temp:.3f}C "
                        f"for target {target_c:g}C; continue with current chamber state"
                    )
                    return self._store_result(
                        WaitResult(
                            ok=True,
                            timed_out=True,
                            target_c=target_c,
                            final_temp_c=final_temp,
                            attempt_count=max(1, start_result.attempt_count),
                            diagnostics={
                                "timeout_recovered_in_band": True,
                                "tolerance_c": tol_c,
                                "temperature_chamber_settle_status": "already_satisfied",
                                "temperature_chamber_settle_passed_at": chamber_passed_at,
                            },
                        )
                    )
                analyzer_evidence = dict(self.run_state.temperature.analyzer_chamber_temp_stability_evidence or {})
                failure_reason = str(
                    analyzer_evidence.get("failure_reason")
                    or "Analyzer chamber temperature did not stabilize after chamber timeout recovery"
                )
                self._set_ready_target(None)
                self.run_state.temperature.last_soak_done = False
                self.host._log(f"Analyzer chamber temperature did not stabilize after in-band timeout: {failure_reason}")
                return self._store_result(
                    WaitResult(
                        ok=False,
                        timed_out=True,
                        target_c=target_c,
                        final_temp_c=final_temp,
                        attempt_count=max(1, start_result.attempt_count),
                        diagnostics={
                            "stage": "analyzer_chamber_temperature_stability",
                            "failure_stage": analyzer_evidence.get(
                                "failure_stage",
                                "analyzer_chamber_temperature_stability",
                            ),
                            "failure_reason": failure_reason,
                            "timeout_recovered_in_band": False,
                            "tolerance_c": tol_c,
                            "temperature_chamber_settle_status": "already_satisfied",
                            "temperature_chamber_settle_passed_at": chamber_passed_at,
                            "analyzer_chamber_temperature_stability": analyzer_evidence,
                        },
                        error=failure_reason,
                    )
                )
            self._set_ready_target(None)
            self.run_state.temperature.last_soak_done = False
            self._mark_temperature_chamber_settle("timeout")
            self.host._log(f"Temperature chamber stability timeout: {exc}")
            return self._store_result(
                WaitResult(
                    ok=False,
                    timed_out=True,
                    target_c=target_c,
                    final_temp_c=final_temp,
                    attempt_count=max(1, start_result.attempt_count),
                    diagnostics={
                        "tolerance_c": tol_c,
                        "temperature_chamber_settle_status": "timeout",
                    },
                    error=str(exc),
                )
            )

    def capture_temperature_calibration_snapshot(self, point: CalibrationPoint, *, route_type: str) -> bool:
        key = (float(point.temp_chamber_c), str(route_type or "").strip().lower())
        if key in self.run_state.temperature.snapshot_keys:
            return True
        self.run_state.temperature.snapshot_keys.add(key)
        analyzers = self.host._all_gas_analyzers()
        if not analyzers:
            self.host._log("Temperature calibration snapshot skipped: no analyzers available")
            return False
        chamber = self.host._device("temperature_chamber")
        chamber_temp = None
        if chamber is not None:
            reader = self._make_temperature_reader(chamber)
            chamber_temp = None if reader is None else reader()
        snapshot_time = datetime.now(timezone.utc).isoformat()
        added = 0
        for label, analyzer, _ in analyzers:
            snapshot = self.host._normalize_snapshot(
                read_device_snapshot_with_retry(
                    analyzer,
                    host=self.host,
                    context=f"analyzer {label} temperature snapshot",
                    required_keys=self.ANALYZER_TEMPERATURE_KEYS,
                    retry_on_empty=True,
                    log_failures=False,
                )
            )
            self.run_state.temperature.snapshots.append(
                {
                    "timestamp": snapshot_time,
                    "route_type": route_type,
                    "point_index": point.index,
                    "temp_setpoint_c": point.temp_chamber_c,
                    "chamber_temperature_box_c": chamber_temp,
                    "analyzer_id": label,
                    "analyzer_chamber_temp_c": self.host._pick_numeric(snapshot, "chamber_temp_c", "temp_c"),
                    "case_temp_c": self.host._pick_numeric(snapshot, "case_temp_c"),
                }
            )
            added += 1
        self.host._log(f"Temperature calibration snapshot saved: records={added} route={route_type}")
        return added > 0

    def export_temperature_snapshots(self) -> dict[str, str]:
        if not self.run_state.temperature.snapshots:
            return {"status": "skipped", "error": "no temperature snapshots"}
        path = Path(self.context.result_store.run_dir) / "temperature_snapshots.json"
        path.write_text(
            json.dumps(self.run_state.temperature.snapshots, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        self.host._remember_output_file(str(path))
        self.host._log(f"Temperature calibration snapshot export saved: {path}")
        return {"status": "ok", "path": str(path)}

    def _verify_commanded_setpoint(
        self,
        chamber: Any,
        *,
        command_target_c: float,
        tol_c: float,
    ) -> dict[str, Any]:
        reader = self._make_setpoint_reader(chamber)
        if reader is None:
            self.host._log(
                "Temperature chamber setpoint readback unsupported; "
                "continue without setpoint verification"
            )
            return {
                "setpoint_readback_supported": False,
                "setpoint_readback_warning": "unsupported",
            }

        diagnostics: dict[str, Any] = {
            "setpoint_readback_supported": True,
            "setpoint_verify_tol_c": max(0.2, abs(float(tol_c))),
            "setpoint_rewrite_attempted": False,
        }
        readback = self._safe_read(reader)
        diagnostics["setpoint_readback_c"] = readback
        if readback is None:
            self.host._log("Temperature chamber setpoint readback unavailable after command")
            diagnostics["setpoint_readback_warning"] = "unavailable_after_command"
            return diagnostics

        verify_tol_c = float(diagnostics["setpoint_verify_tol_c"])
        self.host._log(
            "Temperature chamber setpoint readback: "
            f"read={readback:.3f}C target={command_target_c:g}C tol=卤{verify_tol_c:.3f}C"
        )
        if abs(readback - command_target_c) <= verify_tol_c:
            return diagnostics

        diagnostics["setpoint_rewrite_attempted"] = True
        self.host._log(
            "Temperature chamber setpoint mismatch; rewrite target: "
            f"read={readback:.3f}C target={command_target_c:g}C tol=卤{verify_tol_c:.3f}C"
        )
        self.host._call_first(
            chamber,
            ("set_temp_c", "set_temperature_c", "set_temperature"),
            command_target_c,
        )
        confirm = self._safe_read(reader)
        diagnostics["setpoint_readback_after_rewrite_c"] = confirm
        if confirm is not None:
            self.host._log(
                "Temperature chamber setpoint readback after rewrite: "
                f"read={confirm:.3f}C target={command_target_c:g}C"
            )
        return diagnostics

    def _start_chamber(self, chamber: Any) -> WaitResult:
        try:
            self.host._call_first(chamber, ("start",))
            return WaitResult(ok=True, attempt_count=1)
        except Exception as exc:
            if "START_STATE_MISMATCH" not in str(exc):
                self.host._log(f"Temperature chamber command failed: {exc}")
                return WaitResult(ok=False, attempt_count=1, error=str(exc))

            run_state_reader = self._make_run_state_reader(chamber)
            run_state = self._safe_read_int(run_state_reader)
            if run_state == 1:
                self.host._log("Temperature chamber already running after START_STATE_MISMATCH; continue")
                return WaitResult(ok=True, attempt_count=1, diagnostics={"already_running": True})

            call_with_addr = getattr(chamber, "_call_with_addr", None)
            raise_on_error = getattr(chamber, "_raise_on_modbus_error", None)
            if not callable(call_with_addr):
                self.host._log(f"Temperature chamber command failed: {exc}")
                return WaitResult(ok=False, attempt_count=2, error=str(exc), diagnostics={"fallback_start": False})
            try:
                response = call_with_addr("write_register", 8010, 1)
                if callable(raise_on_error):
                    raise_on_error(response)
            except Exception as fallback_exc:
                self.host._log(f"Temperature chamber fallback start failed: {fallback_exc}")
                return WaitResult(
                    ok=False,
                    attempt_count=2,
                    error=str(fallback_exc),
                    diagnostics={"fallback_start": True},
                )

            deadline = time.monotonic() + 10.0
            while time.monotonic() < deadline:
                self.host._check_stop()
                run_state = self._safe_read_int(run_state_reader)
                if run_state == 1:
                    self.host._log("Temperature chamber fallback start succeeded")
                    return WaitResult(ok=True, attempt_count=2, diagnostics={"fallback_start": True})
                time.sleep(0.5)

            self.host._log(f"Temperature chamber command failed: {exc}")
            return WaitResult(
                ok=False,
                attempt_count=2,
                error=str(exc),
                diagnostics={"fallback_start": True, "fallback_verified": False},
            )

    def _set_ready_target(self, value: Optional[float]) -> None:
        self.run_state.temperature.ready_target_c = value
        setattr(self.host, "_temperature_ready_target_c", value)

    def _store_result(self, result: WaitResult) -> WaitResult:
        self.run_state.temperature.last_wait_result = result
        return result

    def _refresh_live_snapshots(self, *, force: bool = False, reason: str = "") -> bool:
        refresher = getattr(self.host, "_refresh_live_analyzer_snapshots", None)
        if callable(refresher):
            return bool(refresher(force=force, reason=reason))
        analyzer_service = getattr(self.host, "analyzer_fleet_service", None)
        refresher = getattr(analyzer_service, "refresh_live_snapshots", None)
        if callable(refresher):
            return bool(refresher(force=force, reason=reason))
        return False

    def _make_live_snapshot_reader(
        self,
        reader: Callable[[], Optional[float]],
        *,
        reason: str,
    ) -> Callable[[], Optional[float]]:
        def wrapped() -> Optional[float]:
            self._refresh_live_snapshots(reason=reason)
            return reader()

        return wrapped

    def _make_temperature_reader(self, chamber: Any) -> Optional[Callable[[], Optional[float]]]:
        if chamber is None:
            return None
        method = self.host._first_method(chamber, ("read_temp_c", "read_temperature_c", "get_temperature"))
        if method is not None:
            return lambda method=method: read_numeric_with_retry(
                method,
                host=self.host,
                context="temperature chamber read",
                log_failures=False,
            )
        if self.host._first_method(chamber, ("fetch_all", "status", "read")) is None:
            return None
        return lambda chamber=chamber: self.host._pick_numeric(
            self.host._normalize_snapshot(
                read_device_snapshot_with_retry(
                    chamber,
                    host=self.host,
                    context="temperature chamber snapshot",
                    required_keys=self.TEMPERATURE_FRAME_KEYS,
                    retry_on_empty=True,
                    log_failures=False,
                )
            ),
            *self.TEMPERATURE_FRAME_KEYS,
        )

    def _make_setpoint_reader(self, chamber: Any) -> Optional[Callable[[], Optional[float]]]:
        if chamber is None:
            return None
        method = self.host._first_method(chamber, ("read_set_temp_c", "read_set_temperature_c", "get_set_temperature"))
        if method is None:
            return None
        return lambda method=method: read_numeric_with_retry(
            method,
            host=self.host,
            context="temperature chamber setpoint readback",
            log_failures=False,
        )

    def _make_run_state_reader(self, chamber: Any) -> Optional[Callable[[], Optional[float]]]:
        if chamber is None:
            return None
        method = self.host._first_method(chamber, ("read_run_state",))
        if method is None:
            return None
        return lambda method=method: read_numeric_with_retry(
            method,
            host=self.host,
            context="temperature chamber run_state",
            transform=lambda value: self._as_optional_float(self.host._as_int(value)),
            log_failures=False,
        )

    def _wait_for_temperature_stability(
        self,
        reader: Callable[[], Optional[float]],
        *,
        soak_s: float,
        timeout_s: float,
    ) -> Any:
        checker = self.context.stability_checker
        try:
            return checker.wait_for_stability(
                StabilityType.TEMPERATURE,
                reader,
                self.context.stop_event,
                min_wait_s=soak_s,
                max_wait_s=timeout_s,
            )
        except TypeError:
            try:
                return checker.wait_for_stability(
                    StabilityType.TEMPERATURE,
                    reader,
                    self.context.stop_event,
                    min_wait_s=soak_s,
                )
            except TypeError:
                return checker.wait_for_stability(
                    StabilityType.TEMPERATURE,
                    reader,
                    self.context.stop_event,
                )

    def _temperature_soak_after_reach_s(self) -> float:
        if self.host._collect_only_fast_path_enabled():
            return 0.0
        soak_after_reach = self.host._cfg_get("workflow.stability.temperature.soak_after_reach_s", None)
        if soak_after_reach is None:
            soak_after_reach = self.host._cfg_get("workflow.stability.temperature.wait_after_reach_s", 0.0)
        return max(0.0, float(soak_after_reach or 0.0))

    def _make_transition_monitor_reader(
        self,
        reader: Callable[[], Optional[float]],
        chamber: Any,
        *,
        target_c: float,
        tol_c: float,
        target_changed: bool,
        transition_check_window_s: float,
        transition_min_delta_c: float,
    ) -> Callable[[], Optional[float]]:
        if not target_changed or transition_check_window_s <= 0.0 or transition_min_delta_c <= 0.0:
            return reader

        run_state_reader = self._make_run_state_reader(chamber)
        start_temp_c: Optional[float] = None
        start_ts: Optional[float] = None
        transition_direction = 0.0
        movement_seen = False

        def monitored_reader() -> Optional[float]:
            nonlocal start_temp_c, start_ts, transition_direction, movement_seen

            value = reader()
            if value is None:
                return None

            current_temp_c = float(value)
            now = time.monotonic()
            if start_temp_c is None:
                start_temp_c = current_temp_c
                start_ts = now
                delta_to_target = float(target_c) - current_temp_c
                if abs(delta_to_target) <= tol_c:
                    movement_seen = True
                else:
                    transition_direction = 1.0 if delta_to_target > 0 else -1.0
                return current_temp_c

            if movement_seen or transition_direction == 0.0:
                return current_temp_c

            moved_toward_target = (current_temp_c - float(start_temp_c)) * transition_direction
            if moved_toward_target >= transition_min_delta_c:
                movement_seen = True
                return current_temp_c

            if start_ts is not None and (now - start_ts) >= transition_check_window_s:
                transition_run_state = self._safe_read_int(run_state_reader)
                raise _TemperatureTransitionStalledError(
                    "Temperature chamber transition stalled: "
                    f"start_temp={float(start_temp_c):.3f}C, current_temp={current_temp_c:.3f}C, "
                    f"target={target_c:g}C, min_delta={transition_min_delta_c:.3f}C, "
                    f"window={transition_check_window_s:g}s, run_state={transition_run_state}",
                    current_temp_c=current_temp_c,
                    run_state=transition_run_state,
                )
            return current_temp_c

        return monitored_reader

    def _no_write_guard_active(self) -> bool:
        service = getattr(self.host, "service", None)
        guard = getattr(service, "no_write_guard", None)
        return bool(getattr(guard, "enabled", False))

    @staticmethod
    def _config_value(config: Any, *names: str) -> str:
        for name in names:
            value: Any = None
            if isinstance(config, Mapping):
                value = config.get(name)
            else:
                value = getattr(config, name, None)
            if value is not None and str(value).strip():
                return str(value)
        return ""

    def _analyzer_identity(self, label: str, config: Any) -> dict[str, str]:
        return {
            "logical_analyzer_name": str(label),
            "port": self._config_value(config, "port", "configured_port", "resolved_port"),
            "device_id": self._config_value(config, "device_id", "expected_device_id", "stable_device_id"),
        }

    @staticmethod
    def _snapshot_stale_status(snapshot: Any) -> str:
        if not isinstance(snapshot, Mapping):
            return "not_checked_non_mapping_snapshot"
        for key in ("stale", "is_stale", "frame_stale"):
            if key in snapshot:
                return "stale" if bool(snapshot.get(key)) else "fresh"
        for key in ("frame_age_s", "age_s", "last_frame_age_s"):
            if key not in snapshot:
                continue
            try:
                return "fresh" if float(snapshot.get(key)) <= 2.0 else "stale"
            except Exception:
                return "not_checked_invalid_frame_age"
        for key in ("timestamp", "frame_timestamp", "last_frame_time"):
            if snapshot.get(key) is not None:
                return "timestamp_present_age_not_available"
        return "not_checked_no_frame_timestamp"

    def _publish_analyzer_chamber_temp_evidence(self, evidence: Mapping[str, Any]) -> None:
        payload = dict(evidence)
        self.run_state.temperature.analyzer_chamber_temp_stability_evidence = payload
        setattr(self.host, "_last_analyzer_chamber_temp_stability_evidence", payload)

    def _wait_analyzer_chamber_temp_stable(self, target_c: float) -> bool:
        enabled = bool(self.host._cfg_get("workflow.stability.temperature.analyzer_chamber_temp_enabled", True))
        started_at_iso = datetime.now(timezone.utc).isoformat()
        started_at_monotonic = time.time()
        self.run_state.temperature.analyzer_chamber_stability_started_at = started_at_iso
        setattr(self.host, "_analyzer_chamber_stability_started_at", started_at_iso)
        chamber_passed_at = str(
            self.run_state.temperature.chamber_settle_passed_at
            or getattr(self.host, "_temperature_chamber_settle_passed_at", "")
            or ""
        )
        chamber_status = str(
            self.run_state.temperature.chamber_settle_status
            or getattr(self.host, "_temperature_chamber_settle_status", "")
            or ""
        )
        if not enabled:
            self._publish_analyzer_chamber_temp_evidence(
                {
                    "schema_version": "run001_a1.temperature_stability.1",
                    "artifact_type": "temperature_stability_evidence",
                    "stage": "analyzer_chamber_temperature_stability",
                    "target_c": float(target_c),
                    "enabled": False,
                    "temperature_chamber_settle_status": chamber_status,
                    "temperature_chamber_settle_passed_at": chamber_passed_at,
                    "analyzer_chamber_stability_started_at": started_at_iso,
                    "analyzer_chamber_stability_elapsed_s": 0.0,
                    "decision": "SKIPPED",
                    "failure_stage": "",
                    "failure_reason": "",
                    "samples": [],
                    "route_opened": False,
                    "no_write_guard_active": self._no_write_guard_active(),
                }
            )
            return True

        window_s = max(0.0, float(self.host._cfg_get("workflow.stability.temperature.analyzer_chamber_temp_window_s", 60.0)))
        span_tol_c = abs(float(self.host._cfg_get("workflow.stability.temperature.analyzer_chamber_temp_span_c", 0.03)))
        timeout_raw = float(self.host._cfg_get("workflow.stability.temperature.analyzer_chamber_temp_timeout_s", 1800.0))
        timeout_s: Optional[float] = timeout_raw if timeout_raw > 0 else None
        first_valid_timeout_default = 120.0 if timeout_s is None else min(timeout_s, 120.0)
        first_valid_timeout_raw = float(
            self.host._cfg_get(
                "workflow.stability.temperature.analyzer_chamber_temp_first_valid_timeout_s",
                first_valid_timeout_default,
            )
        )
        first_valid_timeout_s: Optional[float] = first_valid_timeout_raw if first_valid_timeout_raw > 0 else None
        poll_s = max(0.1, float(self.host._cfg_get("workflow.stability.temperature.analyzer_chamber_temp_poll_s", 1.0)))

        active_getter = getattr(self.host, "_active_gas_analyzers", None)
        active_analyzers = list(active_getter()) if callable(active_getter) else list(self.host._all_gas_analyzers())
        no_write_active = self._no_write_guard_active()
        self._record_temperature_timing(
            "analyzer_chamber_temperature_stability_start",
            "start",
            stage="analyzer_chamber_temperature_stability",
            expected_max_s=timeout_s,
            wait_reason="analyzer_chamber_temperature_rolling_span",
        )
        evidence: dict[str, Any] = {
            "schema_version": "run001_a1.temperature_stability.1",
            "artifact_type": "temperature_stability_evidence",
            "stage": "analyzer_chamber_temperature_stability",
            "target_c": float(target_c),
            "enabled": True,
            "temperature_chamber_settle_status": chamber_status,
            "temperature_chamber_settle_passed_at": chamber_passed_at,
            "analyzer_chamber_stability_started_at": started_at_iso,
            "analyzer_chamber_stability_elapsed_s": 0.0,
            "temperature_source": "active_send_snapshot",
            "source_selection_policy": "first_active_analyzer_with_chamber_temp_c",
            "rolling_window_s": window_s,
            "tolerance_c": span_tol_c,
            "timeout_s": timeout_s,
            "first_valid_timeout_s": first_valid_timeout_s,
            "sampling_interval_s": poll_s,
            "active_analyzers": [
                self._analyzer_identity(str(label), cfg)
                for label, _analyzer, cfg in active_analyzers
            ],
            "samples": [],
            "sample_count": 0,
            "decision": "RUNNING",
            "failure_stage": "",
            "failure_reason": "",
            "observed_min_c": None,
            "observed_max_c": None,
            "observed_span_c": None,
            "stale_frame_status": "not_checked",
            "data_gap_status": "not_checked",
            "route_opened": False,
            "no_write_guard_active": no_write_active,
        }

        def finish(decision: str, failure_reason: str = "") -> bool:
            elapsed_s = max(0.0, time.time() - started_at_monotonic)
            values = [
                float(sample["chamber_temperature_c"])
                for sample in evidence["samples"]
                if sample.get("chamber_temperature_c") is not None
            ]
            if values:
                evidence["observed_min_c"] = min(values)
                evidence["observed_max_c"] = max(values)
                evidence["observed_span_c"] = self._span(values)
            evidence["sample_count"] = len(evidence["samples"])
            evidence["decision"] = decision
            evidence["failure_reason"] = failure_reason
            evidence["failure_stage"] = "" if decision == "PASS" else "analyzer_chamber_temperature_stability"
            evidence["analyzer_chamber_stability_elapsed_s"] = elapsed_s
            evidence["completed_at"] = datetime.now(timezone.utc).isoformat()
            self._publish_analyzer_chamber_temp_evidence(evidence)
            event_name = (
                "analyzer_chamber_temperature_stability_end"
                if decision in {"PASS", "SKIPPED"}
                else "analyzer_chamber_temperature_stability_timeout"
                if "timeout" in str(failure_reason).lower()
                else "analyzer_chamber_temperature_stability_end"
            )
            event_type = "end" if decision in {"PASS", "SKIPPED"} else ("timeout" if "timeout" in str(failure_reason).lower() else "fail")
            self._record_temperature_timing(
                event_name,
                event_type,
                stage="analyzer_chamber_temperature_stability",
                duration_s=elapsed_s,
                expected_max_s=timeout_s,
                decision=decision.lower(),
                chamber_temperature_c=values[-1] if values else None,
                error_code=failure_reason if decision not in {"PASS", "SKIPPED"} else None,
            )
            return decision == "PASS"

        if not active_analyzers:
            all_analyzers = list(self.host._all_gas_analyzers())
            if callable(active_getter) and all_analyzers:
                reason = "no active analyzers remain before analyzer chamber temperature stability"
                self.host._log(f"Analyzer chamber-temp wait failed: {reason}")
                return finish("FAIL", reason)
            return finish("PASS")

        start = time.time()
        current_label: Optional[str] = None
        current_identity: dict[str, str] = {}
        window_start: Optional[float] = None
        window_values: list[float] = []

        while timeout_s is None or (time.time() - start) < timeout_s:
            self.host._check_stop()
            self._refresh_live_snapshots(reason="temperature_analyzer_chamber_wait")
            active_analyzers = list(active_getter()) if callable(active_getter) else list(self.host._all_gas_analyzers())
            evidence["active_analyzers"] = [
                self._analyzer_identity(str(label), cfg)
                for label, _analyzer, cfg in active_analyzers
            ]
            if not active_analyzers:
                all_analyzers = list(self.host._all_gas_analyzers())
                if callable(active_getter) and all_analyzers:
                    reason = "no active analyzers remain during analyzer chamber temperature stability"
                    self.host._log(f"Analyzer chamber-temp wait failed: {reason}")
                    return finish("FAIL", reason)
                return finish("PASS")

            selected_label: Optional[str] = None
            selected_value: Optional[float] = None
            selected_identity: dict[str, str] = {}
            selected_snapshot: Any = None
            for label, analyzer, cfg in active_analyzers:
                try:
                    snapshot = self.host._normalize_snapshot(
                        read_device_snapshot_with_retry(
                            analyzer,
                            host=self.host,
                            context=f"analyzer {label} chamber temperature",
                            required_keys=self.ANALYZER_TEMPERATURE_KEYS,
                            retry_on_empty=True,
                            log_failures=False,
                        )
                    )
                except Exception as exc:
                    self.host._log(f"Analyzer chamber-temp read failed: {label} err={exc}")
                    continue
                value = self.host._pick_numeric(snapshot, "chamber_temp_c", "temp_c")
                if value is None:
                    continue
                selected_label = str(label)
                selected_value = float(value)
                selected_identity = self._analyzer_identity(str(label), cfg)
                selected_snapshot = snapshot
                break

            now = time.time()
            elapsed_s = max(0.0, now - start)
            if selected_label is None or selected_value is None:
                sample = {
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "elapsed_s": elapsed_s,
                    "logical_analyzer_name": "",
                    "port": "",
                    "device_id": "",
                    "temperature_source": "active_send_snapshot",
                    "chamber_temperature_c": None,
                    "rolling_window_s": window_s,
                    "rolling_min_c": min(window_values) if window_values else None,
                    "rolling_max_c": max(window_values) if window_values else None,
                    "rolling_span_c": self._span(window_values) if window_values else None,
                    "tolerance_c": span_tol_c,
                    "timeout_s": timeout_s,
                    "decision": "WAITING_FOR_FIRST_VALID_SAMPLE",
                    "failure_reason": "",
                    "stale_frame_status": "not_checked_no_valid_frame",
                    "data_gap_status": "no_valid_chamber_temperature",
                    "route_opened": False,
                    "no_write_guard_active": no_write_active,
                }
                evidence["samples"].append(sample)
                evidence["stale_frame_status"] = sample["stale_frame_status"]
                evidence["data_gap_status"] = sample["data_gap_status"]
                self._publish_analyzer_chamber_temp_evidence(evidence)
                if first_valid_timeout_s is not None and elapsed_s >= first_valid_timeout_s:
                    reason = (
                        "first valid analyzer chamber temperature timeout: "
                        f"target={target_c:g}C timeout={first_valid_timeout_raw:g}s "
                        f"window={window_s:g}s tolerance={span_tol_c:.4f}C"
                    )
                    self.host._log(reason)
                    return finish("FAIL", reason)
                time.sleep(poll_s)
                continue

            if current_label != selected_label:
                current_label = selected_label
                current_identity = dict(selected_identity)
                window_start = now
                window_values = []
                self.host._log(
                    "Analyzer chamber-temp stability source selected: "
                    f"{selected_label} target={target_c:g}C"
                )

            if window_start is None:
                window_start = now
            window_values.append(selected_value)
            rolling_span = self._span(window_values)
            sample = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "elapsed_s": elapsed_s,
                "logical_analyzer_name": current_identity.get("logical_analyzer_name", selected_label),
                "port": current_identity.get("port", selected_identity.get("port", "")),
                "device_id": current_identity.get("device_id", selected_identity.get("device_id", "")),
                "temperature_source": "active_send_snapshot",
                "chamber_temperature_c": selected_value,
                "rolling_window_s": window_s,
                "rolling_min_c": min(window_values),
                "rolling_max_c": max(window_values),
                "rolling_span_c": rolling_span,
                "tolerance_c": span_tol_c,
                "timeout_s": timeout_s,
                "decision": "RUNNING",
                "failure_reason": "",
                "stale_frame_status": self._snapshot_stale_status(selected_snapshot),
                "data_gap_status": "ok",
                "route_opened": False,
                "no_write_guard_active": no_write_active,
            }
            evidence["samples"].append(sample)
            evidence["stale_frame_status"] = sample["stale_frame_status"]
            evidence["data_gap_status"] = sample["data_gap_status"]

            if (now - window_start) >= window_s:
                if rolling_span <= span_tol_c:
                    sample["decision"] = "PASS"
                    self.host._log(
                        "Analyzer chamber temp stable: "
                        f"{current_label} value={selected_value:.3f}C span={rolling_span:.4f}C "
                        f"window={window_s:g}s tol=+/-{span_tol_c:.4f}C"
                    )
                    return finish("PASS")
                sample["decision"] = "RESTART_WINDOW_SPAN_GT_TOLERANCE"
                sample["failure_reason"] = (
                    f"span={rolling_span:.4f}C > tolerance={span_tol_c:.4f}C "
                    f"window={window_s:g}s timeout={timeout_raw:g}s"
                )
                self.host._log(
                    "Analyzer chamber temp not stable; restart window: "
                    f"{current_label} last={selected_value:.3f}C span={rolling_span:.4f}C "
                    f"window={window_s:g}s tol=+/-{span_tol_c:.4f}C"
                )
                window_start = now
                window_values = [selected_value]

            self._publish_analyzer_chamber_temp_evidence(evidence)
            time.sleep(poll_s)

        observed_span = self._span(window_values) if window_values else None
        reason = (
            "analyzer chamber temperature stability timeout: "
            f"target={target_c:g}C label={current_label or ''} "
            f"window={window_s:g}s tolerance={span_tol_c:.4f}C timeout={timeout_raw:g}s "
            f"observed_span={observed_span if observed_span is not None else 'none'}"
        )
        self.host._log(reason)
        return finish("FAIL", reason)

    @staticmethod
    def _span(values: list[float]) -> float:
        if not values:
            return 0.0
        return float(max(values) - min(values))

    @staticmethod
    def _as_optional_float(value: Any) -> Optional[float]:
        if value is None:
            return None
        try:
            return float(value)
        except Exception:
            return None

    @staticmethod
    def _safe_read(reader: Any) -> Optional[float]:
        try:
            return None if reader is None else float(reader())
        except Exception:
            return None

    def _safe_read_int(self, reader: Any) -> Optional[int]:
        value = self._safe_read(reader)
        return self.host._as_int(value)
