from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
import time
from typing import Any, Optional

from ..event_bus import EventType
from ..models import CalibrationPhase, CalibrationPoint
from ..orchestration_context import OrchestrationContext
from ..run_state import RunState
from ...exceptions import WorkflowInterruptedError
from .trace_size_guard import guard_trace_event


class StatusService:
    """Unified runtime status, logging, and timing helpers."""

    def __init__(self, context: OrchestrationContext, run_state: RunState, *, host: Any) -> None:
        self.context = context
        self.run_state = run_state
        self.host = host

    def check_stop(self) -> None:
        if self.context.stop_event.is_set():
            raise WorkflowInterruptedError(reason="Stop requested")
        while not self.context.pause_event.is_set():
            if self.context.stop_event.is_set():
                raise WorkflowInterruptedError(reason="Stop requested")
            time.sleep(0.05)

    def update_status(
        self,
        *,
        phase: Optional[CalibrationPhase] = None,
        current_point: Optional[CalibrationPoint] = None,
        message: Optional[str] = None,
        error: Optional[str] = None,
    ) -> None:
        self.context.state_manager.update_status(
            phase=phase,
            current_point=current_point,
            message=message,
            error=error,
        )
        status = self.context.state_manager.status
        self.context.session.phase = status.phase
        self.context.session.current_point = current_point
        self.context.session.total_points = int(status.total_points)
        self.context.session.completed_points = int(status.completed_points)
        self.context.session.progress = float(status.progress)
        if error:
            self.context.session.add_error(error)

    def log(self, message: str) -> None:
        self.context.data_writer.write_log("INFO", message)
        if "warn" in str(message).lower():
            self.context.session.add_warning(message)
            self.context.event_bus.publish(EventType.WARNING_RAISED, {"message": message})
        callback = getattr(self.host, "_log_callback", None)
        if callback is not None:
            try:
                callback(message)
            except Exception:
                pass

    def remember_output_file(self, path: str) -> None:
        if path not in self.run_state.artifacts.output_files:
            self.run_state.artifacts.output_files.append(path)

    def record_route_trace(
        self,
        *,
        action: str,
        route: Optional[str] = None,
        point: Optional[CalibrationPoint] = None,
        point_index: Optional[int] = None,
        point_tag: str = "",
        target: Optional[dict[str, Any]] = None,
        actual: Optional[dict[str, Any]] = None,
        relay_state: Optional[dict[str, Any]] = None,
        result: str = "ok",
        message: str = "",
    ) -> None:
        try:
            trace_path = Path(self.context.result_store.run_dir) / "route_trace.jsonl"
            resolved_route, resolved_point_index, resolved_point_tag = self._resolve_route_trace_context(
                route=route,
                point=point,
                point_index=point_index,
                point_tag=point_tag,
            )
            payload: dict[str, Any] = {
                "ts": datetime.now(timezone.utc).isoformat(),
                "run_id": self.context.session.run_id,
                "route": resolved_route,
                "point_index": resolved_point_index,
                "point_tag": resolved_point_tag,
                "action": str(action or "").strip(),
                "target": self._json_safe_dict(target),
                "actual": self._json_safe_dict(actual),
                "relay_state": self._json_safe_dict(relay_state),
                "result": str(result or "ok").strip().lower() or "ok",
                "message": str(message or ""),
            }
            guarded_payload = guard_trace_event(payload, trace_name="route_trace")
            with trace_path.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(guarded_payload, ensure_ascii=False, separators=(",", ":")) + "\n")
            self.remember_output_file(str(trace_path))
        except Exception as exc:
            try:
                self.context.data_writer.write_log("WARNING", f"Route trace write failed: {exc}")
            except Exception:
                pass

    def begin_point_timing(self, point: CalibrationPoint, *, phase: str = "", point_tag: str = "") -> None:
        self.run_state.timing.point_contexts[self.host._timing_key(point, phase=phase, point_tag=point_tag)] = {
            "phase": str(phase or point.route or "").strip().lower(),
            "point_tag": str(point_tag or "").strip(),
            "started_at": time.monotonic(),
            "stability_time_s": None,
            "total_time_s": None,
        }

    def mark_point_stable_for_sampling(self, point: CalibrationPoint, *, phase: str = "", point_tag: str = "") -> None:
        key = self.host._timing_key(point, phase=phase, point_tag=point_tag)
        context = self.run_state.timing.point_contexts.get(key)
        if not context:
            return
        started_at = context.get("started_at")
        if started_at is None:
            return
        context["stability_time_s"] = max(0.0, time.monotonic() - float(started_at))

    def finish_point_timing(self, point: CalibrationPoint, *, phase: str = "", point_tag: str = "") -> dict[str, Any]:
        key = self.host._timing_key(point, phase=phase, point_tag=point_tag)
        context = dict(self.run_state.timing.point_contexts.get(key) or {})
        started_at = context.get("started_at")
        if started_at is not None:
            context["total_time_s"] = max(0.0, time.monotonic() - float(started_at))
            if context.get("stability_time_s") is None:
                context["stability_time_s"] = context["total_time_s"]
        self.run_state.timing.point_contexts[key] = context
        return context

    def clear_point_timing(self, point: CalibrationPoint, *, phase: str = "", point_tag: str = "") -> None:
        self.run_state.timing.point_contexts.pop(self.host._timing_key(point, phase=phase, point_tag=point_tag), None)

    def mark_point_completed(
        self,
        point: CalibrationPoint,
        *,
        point_tag: str = "",
        stability_time_s: Optional[float] = None,
        total_time_s: Optional[float] = None,
    ) -> None:
        point_key = self._progress_point_key(point, point_tag=point_tag)
        self.context.run_logger.log_point(
            point,
            "completed",
            point_tag=point_tag,
            stability_time_s=stability_time_s,
            total_time_s=total_time_s,
        )
        self.context.state_manager.mark_point_completed(point, point_key=point_key)
        status = self.context.state_manager.status
        self.context.session.current_point = point
        self.context.session.total_points = int(status.total_points)
        self.context.session.completed_points = int(status.completed_points)
        self.context.session.progress = float(status.progress)
        self.context.event_bus.publish(EventType.POINT_COMPLETED, {"point": point})

    def _resolve_route_trace_context(
        self,
        *,
        route: Optional[str],
        point: Optional[CalibrationPoint],
        point_index: Optional[int],
        point_tag: str,
    ) -> tuple[str, Optional[int], str]:
        route_context = getattr(self.host, "route_context", None)
        context_point = (
            point
            or getattr(route_context, "active_point", None)
            or getattr(route_context, "current_point", None)
            or getattr(route_context, "source_point", None)
        )
        resolved_route = str(
            route
            or getattr(route_context, "current_route", "")
            or getattr(context_point, "route", "")
            or ""
        ).strip().lower()
        resolved_point_index = point_index
        if resolved_point_index is None and context_point is not None:
            try:
                resolved_point_index = int(context_point.index)
            except Exception:
                resolved_point_index = None
        resolved_point_tag = str(point_tag or getattr(route_context, "point_tag", "") or "").strip()
        if not resolved_point_tag and context_point is not None:
            resolved_point_tag = self._derive_point_tag(resolved_route, context_point)
        return resolved_route, resolved_point_index, resolved_point_tag

    def _derive_point_tag(self, route: str, point: CalibrationPoint) -> str:
        planner = getattr(self.host, "route_planner", None)
        try:
            if route == "h2o" and planner is not None:
                return str(planner.h2o_point_tag(point) or "").strip()
            if route == "co2" and planner is not None:
                return str(planner.co2_point_tag(point) or "").strip()
        except Exception:
            return ""
        return ""

    def _json_safe_dict(self, value: Optional[dict[str, Any]]) -> dict[str, Any]:
        if not isinstance(value, dict):
            return {}
        out: dict[str, Any] = {}
        for key, item in value.items():
            out[str(key)] = self._json_safe_value(item)
        return out

    def _json_safe_value(self, value: Any) -> Any:
        if value is None or isinstance(value, (str, int, float, bool)):
            return value
        if isinstance(value, dict):
            return {str(key): self._json_safe_value(item) for key, item in value.items()}
        if isinstance(value, (list, tuple, set)):
            return [self._json_safe_value(item) for item in value]
        if isinstance(value, Path):
            return str(value)

    def _progress_point_key(self, point: CalibrationPoint, *, point_tag: str = "") -> str:
        planner = getattr(self.host, "route_planner", None)
        if planner is not None and hasattr(planner, "progress_point_key"):
            try:
                return str(planner.progress_point_key(point, point_tag=point_tag) or "").strip()
            except Exception:
                pass
        route = str(getattr(point, "route", "") or "").strip().lower()
        if point_tag:
            return f"{route}:{point_tag}"
        return f"{route}:{int(point.index)}"
        return str(value)
