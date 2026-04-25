from __future__ import annotations

import json
from typing import Any, Optional

from ..event_bus import EventType
from ..models import CalibrationPhase


class FinalizationRunner:
    """Unified exit path for workflow finalization, artifacts, AI, and completion events."""

    def __init__(self, service: Any) -> None:
        self.service = service

    def run(
        self,
        *,
        final_phase: CalibrationPhase,
        final_message: str,
        final_error: Optional[str],
    ) -> None:
        phase = final_phase
        message = final_message
        error = final_error
        try:
            self._perform_safe_stop()
            try:
                self.service._run_finalization()
            except Exception as exc:
                phase, message, error = self._handle_finalization_failure(
                    phase=phase,
                    message=message,
                    error=error,
                    exc=exc,
                    prefix="Finalization failed",
                )

            self._apply_terminal_state(phase=phase, message=message, error=error)
            current_status = self.service.state_manager.status
            self.service.session.phase = current_status.phase
            self.service.session.current_point = current_status.current_point
            self.service.session.end(message if phase is not CalibrationPhase.COMPLETED else "")
            self.service._export_summary(self.service.session, current_status=current_status)
            self.service._export_manifest(
                self.service.session,
                source_points_file=getattr(self.service, "_points_path", None),
            )
            self._refresh_run001_a1_artifacts_after_terminal_summary()
            self.service._generate_ai_outputs()
            self.service._sync_results_to_storage()
        except Exception as exc:
            phase, message, error = self._handle_finalization_failure(
                phase=phase,
                message=message,
                error=error,
                exc=exc,
                prefix="Finalization failed",
            )
            self._apply_terminal_state(phase=phase, message=message, error=error)
            current_status = self.service.state_manager.status
            self.service.session.phase = current_status.phase
            self.service.session.current_point = current_status.current_point
            if self.service.session.ended_at is None:
                self.service.session.end(message)
        finally:
            self._publish_completion(phase=phase, error=error)
            self.service._done_event.set()

    def _perform_safe_stop(self) -> None:
        orchestrator = getattr(self.service, "orchestrator", None)
        if orchestrator is None:
            return
        restore_baseline = self._cfg_bool("workflow.restore_baseline_on_finish", True)
        summary: dict[str, Any] = {}
        baseline_done = False

        if restore_baseline:
            restore_method = getattr(getattr(orchestrator, "valve_routing_service", None), "restore_baseline_after_run", None)
            if callable(restore_method):
                try:
                    summary["restore_baseline"] = restore_method(reason="restore baseline on finish")
                    baseline_done = True
                except Exception as exc:
                    orchestrator._log(f"Final safe stop warning: baseline restore failed: {exc}")
        else:
            orchestrator._log("Final safe stop: restore_baseline_on_finish=false; run minimal safe stop only")

        pressure_method = getattr(getattr(orchestrator, "pressure_control_service", None), "safe_stop_after_run", None)
        if callable(pressure_method):
            try:
                summary["pressure"] = pressure_method(reason="final safe stop")
            except Exception as exc:
                orchestrator._log(f"Final safe stop warning: pressure safe stop failed: {exc}")

        route_method = getattr(getattr(orchestrator, "valve_routing_service", None), "safe_stop_after_run", None)
        if callable(route_method):
            try:
                summary["routes"] = route_method(
                    baseline_already_restored=baseline_done,
                    reason="final safe stop",
                )
            except Exception as exc:
                orchestrator._log(f"Final safe stop warning: route safe stop failed: {exc}")

        if summary:
            try:
                orchestrator._log(
                    "Final safe stop summary: "
                    + json.dumps(summary, ensure_ascii=False, separators=(",", ":"), default=str)
                )
            except Exception:
                orchestrator._log("Final safe stop summary available")

    def _apply_terminal_state(
        self,
        *,
        phase: CalibrationPhase,
        message: str,
        error: Optional[str],
    ) -> None:
        if phase is CalibrationPhase.COMPLETED:
            self.service.state_manager.complete()
        elif phase is CalibrationPhase.STOPPED:
            self.service.state_manager.stop(message)
        else:
            self.service.state_manager.set_error(error or message)

    def _handle_finalization_failure(
        self,
        *,
        phase: CalibrationPhase,
        message: str,
        error: Optional[str],
        exc: Exception,
        prefix: str,
    ) -> tuple[CalibrationPhase, str, str]:
        updated_message = f"{prefix}: {exc}"
        updated_error = str(exc)
        updated_phase = phase
        if phase is not CalibrationPhase.STOPPED and phase is not CalibrationPhase.ERROR:
            updated_phase = CalibrationPhase.ERROR
        self.service.session.add_error(updated_error)
        self.service.orchestrator._log(updated_message)
        return updated_phase, message if phase is CalibrationPhase.STOPPED else updated_message, updated_error

    def _refresh_run001_a1_artifacts_after_terminal_summary(self) -> None:
        orchestrator = getattr(self.service, "orchestrator", None)
        artifact_service = getattr(orchestrator, "artifact_service", None)
        exporter = getattr(artifact_service, "_export_run001_a1_artifacts", None)
        if not callable(exporter):
            return
        try:
            exporter()
        except Exception as exc:
            try:
                self.service.orchestrator._log(f"Run-001/A1 terminal evidence refresh failed: {exc}")
            except Exception:
                pass

    def _cfg_bool(self, path: str, default: bool) -> bool:
        orchestrator = getattr(self.service, "orchestrator", None)
        getter = getattr(orchestrator, "_cfg_get", None)
        if callable(getter):
            try:
                return bool(getter(path, default))
            except Exception:
                return bool(default)
        return bool(default)

    def _publish_completion(self, *, phase: CalibrationPhase, error: Optional[str]) -> None:
        try:
            self.service.event_bus.publish(
                EventType.WORKFLOW_COMPLETED,
                {"run_id": self.service.run_id, "phase": phase.value, "error": error},
            )
        except Exception as exc:
            self.service.orchestrator._log(f"Completion event publish failed: {exc}")
