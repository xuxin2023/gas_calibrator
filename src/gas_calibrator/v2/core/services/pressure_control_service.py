from __future__ import annotations

from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
import json
import threading
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


@dataclass(frozen=True)
class PressureSample:
    """Read-only pressure sample with timing and source provenance."""

    source: str
    pressure_hpa: Optional[float] = None
    request_sent_at: str = ""
    response_received_at: str = ""
    request_sent_monotonic_s: Optional[float] = None
    response_received_monotonic_s: Optional[float] = None
    read_latency_s: Optional[float] = None
    sample_recorded_at: str = ""
    sample_recorded_monotonic_s: Optional[float] = None
    sample_age_s: Optional[float] = None
    is_cached: bool = False
    is_stale: bool = False
    stale_threshold_s: float = 2.0
    serial_port: str = ""
    command: str = ""
    raw_response: str = ""
    parse_ok: bool = False
    error: str = ""
    sequence_id: Optional[int] = None
    usable_for_abort: bool = False
    usable_for_ready: bool = False
    usable_for_seal: bool = False

    def as_payload(self) -> dict[str, Any]:
        payload = {
            "source": self.source,
            "pressure_hpa": self.pressure_hpa,
            "request_sent_at": self.request_sent_at,
            "response_received_at": self.response_received_at,
            "request_sent_monotonic_s": self.request_sent_monotonic_s,
            "response_received_monotonic_s": self.response_received_monotonic_s,
            "read_latency_s": self.read_latency_s,
            "sample_recorded_at": self.sample_recorded_at,
            "sample_recorded_monotonic_s": self.sample_recorded_monotonic_s,
            "sample_age_s": self.sample_age_s,
            "is_cached": self.is_cached,
            "is_stale": self.is_stale,
            "stale_threshold_s": self.stale_threshold_s,
            "serial_port": self.serial_port,
            "command": self.command,
            "raw_response": self.raw_response,
            "parse_ok": self.parse_ok,
            "error": self.error,
            "sequence_id": self.sequence_id,
            "usable_for_abort": self.usable_for_abort,
            "usable_for_ready": self.usable_for_ready,
            "usable_for_seal": self.usable_for_seal,
        }
        payload.update(
            {
                "pressure_sample_source": self.source,
                "pressure_sample_timestamp": self.sample_recorded_at,
                "pressure_sample_monotonic_s": self.sample_recorded_monotonic_s,
                "pressure_sample_age_s": self.sample_age_s,
                "pressure_sample_is_stale": self.is_stale,
                "pressure_sample_sequence_id": self.sequence_id,
            }
        )
        return payload


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
        if bool(getattr(self.host, "_a2_high_pressure_first_point_mode_enabled", False)):
            return True
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

    def _preseal_capture_urgent_seal_threshold_hpa(
        self,
        ready_pressure_hpa: Optional[float],
    ) -> Optional[float]:
        configured = self._coerce_float(
            self.host._cfg_get(
                "workflow.pressure.preseal_capture_urgent_seal_threshold_hpa",
                self.host._cfg_get(
                    "workflow.pressure.preseal_urgent_seal_threshold_hpa",
                    self.host._cfg_get("workflow.pressure.preseal_abort_pressure_hpa", None),
                ),
            )
        )
        if configured is not None:
            return float(configured)
        return self._positive_preseal_abort_pressure_hpa(ready_pressure_hpa)

    def _preseal_capture_hard_abort_pressure_hpa(
        self,
        urgent_seal_threshold_hpa: Optional[float],
    ) -> Optional[float]:
        configured = self._coerce_float(
            self.host._cfg_get(
                "workflow.pressure.preseal_capture_hard_abort_pressure_hpa",
                self.host._cfg_get("workflow.pressure.preseal_hard_abort_pressure_hpa", 1250.0),
            )
        )
        if configured is None:
            return None
        hard_abort = float(configured)
        if urgent_seal_threshold_hpa is not None:
            hard_abort = max(hard_abort, float(urgent_seal_threshold_hpa))
        return hard_abort

    def _pressure_controller_state_snapshot(self, controller: Any) -> dict[str, Any]:
        vent_status = self._pressure_controller_vent_status(controller)
        return {
            "vent_on": self._pressure_controller_vent_on(),
            "vent_status_raw": vent_status,
            "vent_status_interpreted": self._pressure_vent_status_interpretation(controller, vent_status),
            "output_state": self._pressure_controller_output_state(controller),
            "isolation_state": self._pressure_controller_isolation_state(controller),
        }

    def _pressure_controller_fast_state_hint(self, controller: Any) -> dict[str, Any]:
        """Return non-query state hints so preseal vent close can stay bounded."""
        state: dict[str, Any] = {}
        for key, names in {
            "vent_on": ("vent_on", "atmosphere_mode", "is_vent_open"),
            "vent_status_raw": ("vent_status", "pace_vent_status"),
            "output_state": ("output_state", "output_enabled", "output"),
            "isolation_state": ("isolation_state", "isolation_open", "isolation"),
        }.items():
            for name in names:
                if not hasattr(controller, name):
                    continue
                value = getattr(controller, name)
                if callable(value):
                    continue
                state[key] = value
                break
        vent_status = self._coerce_float(state.get("vent_status_raw"))
        if vent_status is not None:
            state["vent_status_raw"] = int(vent_status)
            state["vent_status_interpreted"] = self._pressure_vent_status_interpretation(controller, int(vent_status))
        return state

    def _next_pressure_sample_sequence_id(self) -> int:
        current = int(getattr(self, "_pressure_sample_sequence_id", 0) or 0) + 1
        setattr(self, "_pressure_sample_sequence_id", current)
        return current

    def _pressure_sample_stale_max_s(self) -> float:
        return max(
            0.0,
            float(
                self.host._cfg_get(
                    "workflow.pressure.pressure_sample_stale_threshold_s",
                    self.host._cfg_get("workflow.pressure.pressure_sample_stale_max_s", 2.0),
                )
            ),
        )

    def _safe_pressure_raw_response(self, raw: Any) -> str:
        try:
            text = json.dumps(raw, ensure_ascii=False, default=str) if isinstance(raw, Mapping) else repr(raw)
        except Exception:
            text = repr(raw)
        return text[:500]

    def _coerce_pressure_bool(self, value: Any, default: bool = False) -> bool:
        parsed = self._coerce_bool(value)
        return bool(default if parsed is None else parsed)

    def _pressure_value_from_payload(self, data: Mapping[str, Any], raw: Any) -> Optional[float]:
        for key in (
            "pressure_hpa",
            "current_line_pressure_hpa",
            "positive_preseal_pressure_hpa",
            "pressure",
            "pressure_gauge_hpa",
        ):
            if key in data:
                value = self._coerce_float(data.get(key))
                if value is not None:
                    return value
        return None if data else self._coerce_float(raw)

    def _remember_pressure_sample_payload(self, payload: Mapping[str, Any]) -> None:
        samples = getattr(self, "_pressure_read_latency_samples", None)
        if not isinstance(samples, list):
            samples = []
        samples.append(dict(payload))
        setattr(self, "_pressure_read_latency_samples", samples)
        try:
            setattr(self.host, "_pressure_read_latency_samples", list(samples))
        except Exception:
            pass

    def _pressure_sample_payload(
        self,
        raw: Any,
        *,
        source: str,
        request_sent_monotonic_s: Optional[float] = None,
        response_received_monotonic_s: Optional[float] = None,
        request_sent_at: Optional[str] = None,
        response_received_at: Optional[str] = None,
        serial_port: Optional[str] = None,
        command: Optional[str] = None,
        raw_response: Any = None,
        error: Optional[str] = None,
        is_cached: Optional[bool] = None,
        stale_threshold_s: Optional[float] = None,
    ) -> dict[str, Any]:
        now = datetime.now(timezone.utc)
        now_monotonic = time.monotonic()
        data = dict(raw) if isinstance(raw, Mapping) else {}
        pressure_hpa = self._pressure_value_from_payload(data, raw)
        sample_source = str(
            data.get("pressure_sample_source")
            or data.get("source")
            or data.get("pressure_source")
            or source
            or "pressure_gauge"
        )
        sample_recorded_at = str(
            data.get("pressure_sample_timestamp")
            or data.get("timestamp")
            or data.get("recorded_at")
            or now.isoformat()
        )
        sample_recorded_monotonic_s = self._coerce_float(
            data.get("sample_recorded_monotonic_s")
            if data.get("sample_recorded_monotonic_s") is not None
            else (
                data.get("pressure_sample_monotonic_s")
                if data.get("pressure_sample_monotonic_s") is not None
                else data.get("monotonic_s")
            )
        )
        if sample_recorded_monotonic_s is None and not self._coerce_pressure_bool(
            data.get("is_cached", is_cached), default=False
        ):
            sample_recorded_monotonic_s = response_received_monotonic_s or now_monotonic
        request_monotonic = self._coerce_float(
            data.get("request_sent_monotonic_s")
            if data.get("request_sent_monotonic_s") is not None
            else request_sent_monotonic_s
        )
        response_monotonic = self._coerce_float(
            data.get("response_received_monotonic_s")
            if data.get("response_received_monotonic_s") is not None
            else response_received_monotonic_s
        )
        read_latency_s = self._coerce_float(data.get("read_latency_s"))
        if read_latency_s is None and request_monotonic is not None and response_monotonic is not None:
            read_latency_s = max(0.0, float(response_monotonic) - float(request_monotonic))
        age_s = self._coerce_float(data.get("pressure_sample_age_s", data.get("sample_age_s")))
        if age_s is None and sample_recorded_monotonic_s is not None:
            age_s = max(0.0, now_monotonic - float(sample_recorded_monotonic_s))
        if age_s is None:
            try:
                parsed = datetime.fromisoformat(sample_recorded_at.replace("Z", "+00:00"))
                if parsed.tzinfo is None:
                    parsed = parsed.replace(tzinfo=timezone.utc)
                age_s = max(0.0, (now - parsed.astimezone(timezone.utc)).total_seconds())
            except Exception:
                age_s = 0.0
        stale_threshold = self._coerce_float(
            data.get(
                "pressure_sample_stale_threshold_s",
                data.get("stale_threshold_s", stale_threshold_s),
            )
        )
        if stale_threshold is None:
            stale_threshold = self._pressure_sample_stale_max_s()
        explicit_stale = self._coerce_bool(data.get("pressure_sample_is_stale", data.get("is_stale")))
        stale = bool(explicit_stale) if explicit_stale is not None else bool(age_s > stale_threshold)
        sequence_id = self._coerce_float(data.get("pressure_sample_sequence_id", data.get("sequence_id")))
        sequence = int(sequence_id) if sequence_id is not None else self._next_pressure_sample_sequence_id()
        cached = self._coerce_pressure_bool(data.get("is_cached", is_cached), default=False)
        parse_ok = self._coerce_bool(data.get("parse_ok"))
        parse_ok_bool = bool(pressure_hpa is not None and not error) if parse_ok is None else bool(parse_ok)
        usable_default = bool(pressure_hpa is not None and not stale)
        sample = PressureSample(
            source=sample_source,
            pressure_hpa=pressure_hpa,
            request_sent_at=str(data.get("request_sent_at") or request_sent_at or now.isoformat()),
            response_received_at=str(data.get("response_received_at") or response_received_at or now.isoformat()),
            request_sent_monotonic_s=None if request_monotonic is None else round(float(request_monotonic), 6),
            response_received_monotonic_s=None if response_monotonic is None else round(float(response_monotonic), 6),
            read_latency_s=None if read_latency_s is None else round(float(read_latency_s), 3),
            sample_recorded_at=sample_recorded_at,
            sample_recorded_monotonic_s=(
                None if sample_recorded_monotonic_s is None else round(float(sample_recorded_monotonic_s), 6)
            ),
            sample_age_s=None if age_s is None else round(float(age_s), 3),
            is_cached=cached,
            is_stale=stale,
            stale_threshold_s=round(float(stale_threshold), 3),
            serial_port=str(data.get("serial_port") or serial_port or ""),
            command=str(data.get("command") or command or ""),
            raw_response=str(data.get("raw_response") or self._safe_pressure_raw_response(raw_response if raw_response is not None else raw)),
            parse_ok=parse_ok_bool,
            error=str(data.get("error") or error or ""),
            sequence_id=sequence,
            usable_for_abort=self._coerce_pressure_bool(data.get("usable_for_abort"), default=usable_default),
            usable_for_ready=self._coerce_pressure_bool(data.get("usable_for_ready"), default=usable_default),
            usable_for_seal=self._coerce_pressure_bool(data.get("usable_for_seal"), default=usable_default),
        )
        payload = sample.as_payload()
        for extra_key in (
            "stage",
            "point_index",
            "digital_gauge_mode",
            "digital_gauge_continuous_started",
            "digital_gauge_continuous_active",
            "digital_gauge_continuous_enabled",
            "digital_gauge_continuous_mode",
            "digital_gauge_stream_first_frame_at",
            "digital_gauge_stream_last_frame_at",
            "digital_gauge_stream_stale",
            "digital_gauge_stream_stale_threshold_s",
            "digital_gauge_drain_empty_count",
            "digital_gauge_drain_nonempty_count",
            "latest_frame_age_s",
            "latest_frame_interval_s",
            "latest_frame_sequence_id",
            "digital_gauge_latest_sequence_id",
            "frame_received_at",
            "monotonic_timestamp",
            "raw_line",
            "last_pressure_command",
            "last_pressure_command_may_cancel_continuous",
            "continuous_interrupted_by_command",
            "continuous_restart_required_before_return_to_continuous",
            "continuous_restart_attempted",
            "continuous_restart_result",
            "continuous_restart_reason",
            "pressure_source_selected",
            "pressure_source_selection_reason",
            "source_selection_reason",
            "critical_window_uses_latest_frame",
            "critical_window_uses_query",
            "critical_window_blocking_query_count",
            "critical_window_blocking_query_total_s",
            "conditioning_pressure_abort_hpa",
            "measured_atmospheric_pressure_hpa",
            "measured_atmospheric_pressure_source",
            "measured_atmospheric_pressure_sample_age_s",
            "route_conditioning_pressure_before_route_open_hpa",
            "route_open_transient_window_enabled",
            "route_open_transient_peak_pressure_hpa",
            "route_open_transient_peak_time_ms",
            "route_open_transient_recovery_required",
            "route_open_transient_recovered_to_atmosphere",
            "route_open_transient_recovery_time_ms",
            "route_open_transient_recovery_target_hpa",
            "route_open_transient_recovery_band_hpa",
            "route_open_transient_stable_hold_s",
            "route_open_transient_stable_pressure_mean_hpa",
            "route_open_transient_stable_pressure_span_hpa",
            "route_open_transient_stable_pressure_slope_hpa_per_s",
            "route_open_transient_accepted",
            "route_open_transient_rejection_reason",
            "route_open_transient_evaluation_state",
            "route_open_transient_interrupted_by_vent_gap",
            "route_open_transient_interrupted_reason",
            "route_open_transient_summary_source",
            "sustained_pressure_rise_after_route_open",
            "pressure_rise_despite_valid_vent_scheduler",
            "route_conditioning_hard_abort_pressure_hpa",
            "route_conditioning_hard_abort_exceeded",
            "pressure_overlimit_seen",
            "pressure_overlimit_source",
            "pressure_overlimit_hpa",
            "vent_heartbeat_gap_exceeded",
            "digital_gauge_sequence_progress",
            "digital_gauge_latest_age_s",
            "stream_stale",
            "fail_closed_before_vent_off",
            "p3_fast_fallback_attempted",
            "p3_fast_fallback_result",
            "normal_p3_fallback_attempted",
            "normal_p3_fallback_result",
            "continuous_latest_fresh_fast_path_used",
            "continuous_latest_fresh_duration_ms",
            "continuous_latest_fresh_lock_acquire_ms",
            "continuous_latest_fresh_lock_timeout",
            "continuous_latest_fresh_waited_for_frame",
            "continuous_latest_fresh_performed_io",
            "continuous_latest_fresh_triggered_stream_restart",
            "continuous_latest_fresh_triggered_drain",
            "continuous_latest_fresh_triggered_p3_fallback",
            "continuous_latest_fresh_budget_ms",
            "continuous_latest_fresh_budget_exceeded",
        ):
            if extra_key in data:
                payload[extra_key] = data.get(extra_key)
        setattr(self, "_last_pressure_sample_metadata", payload)
        self._remember_pressure_sample_payload(payload)
        return payload

    def _digital_gauge_continuous_enabled(self) -> bool:
        value = self._coerce_bool(
            self.host._cfg_get(
                "workflow.pressure.digital_gauge_continuous_enabled",
                self.host._cfg_get(
                    "workflow.pressure.pressure_gauge_continuous_enabled",
                    True,
                ),
            )
        )
        return True if value is None else bool(value)

    def _digital_gauge_continuous_mode(self) -> str:
        mode = str(
            self.host._cfg_get(
                "workflow.pressure.digital_gauge_continuous_mode",
                self.host._cfg_get(
                    "workflow.pressure.pressure_gauge_continuous_mode",
                    "P4",
                ),
            )
            or "P4"
        ).strip().upper()
        return mode or "P4"

    def _digital_gauge_continuous_drain_s(self) -> float:
        return max(
            0.005,
            float(
                self.host._cfg_get(
                    "workflow.pressure.digital_gauge_continuous_drain_s",
                    self.host._cfg_get("workflow.pressure.pressure_gauge_continuous_drain_s", 0.02),
                )
                or 0.02
            ),
        )

    def _digital_gauge_continuous_read_timeout_s(self) -> float:
        return max(
            0.001,
            float(
                self.host._cfg_get(
                    "workflow.pressure.digital_gauge_continuous_read_timeout_s",
                    self.host._cfg_get("workflow.pressure.pressure_gauge_continuous_read_timeout_s", 0.01),
                )
                or 0.01
            ),
        )

    def _digital_gauge_stream_poll_interval_s(self) -> float:
        return max(
            0.005,
            float(self.host._cfg_get("workflow.pressure.digital_gauge_stream_poll_interval_s", 0.02) or 0.02),
        )

    def _digital_gauge_stream_first_frame_timeout_s(self) -> float:
        return max(
            0.05,
            float(self.host._cfg_get("workflow.pressure.digital_gauge_stream_first_frame_timeout_s", 2.5) or 2.5),
        )

    def _digital_gauge_latest_frame_stale_max_s(self) -> float:
        return max(
            0.01,
            float(
                self.host._cfg_get(
                    "workflow.pressure.digital_gauge_latest_frame_stale_max_s",
                    self.host._cfg_get("workflow.pressure.critical_pressure_latest_frame_stale_max_s", 0.5),
                )
                or 0.5
            ),
        )

    def _digital_gauge_stream_lock(self) -> threading.RLock:
        lock = getattr(self, "_digital_gauge_stream_lock_obj", None)
        if lock is None:
            lock = threading.RLock()
            setattr(self, "_digital_gauge_stream_lock_obj", lock)
        return lock

    def _digital_gauge_stream_state(self) -> dict[str, Any]:
        state = getattr(self, "_digital_gauge_continuous_stream_state", None)
        if not isinstance(state, dict):
            state = {
                "digital_gauge_continuous_enabled": False,
                "digital_gauge_continuous_active": False,
                "digital_gauge_continuous_mode": "",
                "stream_started_at": "",
                "stream_started_monotonic_s": None,
                "stream_first_frame_at": "",
                "stream_first_frame_monotonic_s": None,
                "stream_last_frame_at": "",
                "stream_last_frame_monotonic_s": None,
                "stream_frame_count": 0,
                "latest_frame": None,
                "latest_frame_age_max_s": None,
                "stale_frame_count": 0,
                "digital_gauge_drain_empty_count": 0,
                "digital_gauge_drain_nonempty_count": 0,
                "last_pressure_command": "",
                "last_pressure_command_may_cancel_continuous": False,
                "continuous_interrupted_by_command": False,
                "continuous_restart_required_before_return_to_continuous": False,
                "continuous_restart_attempted": False,
                "continuous_restart_result": "",
                "continuous_restart_reason": "",
                "continuous_restart_count": 0,
                "pressure_source_selected": "",
                "pressure_source_selection_reason": "",
                "blocking_query_count_in_critical_window": 0,
                "critical_window_blocking_query_total_s": 0.0,
                "continuous_unavailable_reason": "",
            }
            setattr(self, "_digital_gauge_continuous_stream_state", state)
        return state

    def _record_pressure_timing_event(
        self,
        event_name: str,
        event_type: str = "info",
        *,
        stage: str = "high_pressure_first_point",
        point_index: Any = None,
        target_pressure_hpa: Any = None,
        pressure_hpa: Any = None,
        duration_s: Any = None,
        expected_max_s: Any = None,
        decision: Any = None,
        warning_code: Any = None,
        error_code: Any = None,
        route_state: Any = None,
    ) -> None:
        recorder = getattr(self.host, "_record_workflow_timing", None)
        if not callable(recorder):
            return
        recorder(
            event_name,
            event_type,
            stage=stage,
            point_index=point_index,
            target_pressure_hpa=target_pressure_hpa,
            pressure_hpa=pressure_hpa,
            duration_s=duration_s,
            expected_max_s=expected_max_s,
            decision=decision,
            warning_code=warning_code,
            error_code=error_code,
            route_state=route_state,
        )

    def _call_pressure_method(self, method: Callable[..., Any], **kwargs: Any) -> Any:
        try:
            return method(**kwargs)
        except TypeError:
            return method()

    def _record_critical_window_blocking_query(
        self,
        *,
        source: str,
        command: str,
        duration_s: Optional[float],
        reason: str,
    ) -> None:
        state = self._digital_gauge_stream_state()
        state["blocking_query_count_in_critical_window"] = int(
            state.get("blocking_query_count_in_critical_window") or 0
        ) + 1
        state["critical_window_blocking_query_total_s"] = round(
            float(state.get("critical_window_blocking_query_total_s") or 0.0) + float(duration_s or 0.0),
            6,
        )
        command_may_cancel = bool(state.get("last_pressure_command_may_cancel_continuous"))
        self._record_pressure_timing_event(
            "critical_window_blocking_query",
            "warning",
            duration_s=duration_s,
            warning_code="critical_window_blocking_query",
            decision=reason,
            route_state={
                "source": source,
                "command": command,
                "duration_s": duration_s,
                "reason": reason,
                "command_may_cancel_continuous": command_may_cancel,
                "last_pressure_command_may_cancel_continuous": command_may_cancel,
                "continuous_interrupted_by_command": bool(state.get("continuous_interrupted_by_command")),
                "critical_window_blocking_query_count": state.get("blocking_query_count_in_critical_window"),
                "critical_window_blocking_query_total_s": state.get("critical_window_blocking_query_total_s"),
            },
        )

    def _digital_gauge_state_payload(
        self,
        state: Optional[Mapping[str, Any]] = None,
        *,
        stale: Optional[bool] = None,
        threshold_s: Optional[float] = None,
    ) -> dict[str, Any]:
        snapshot = dict(state or self._digital_gauge_stream_state())
        latest = snapshot.get("latest_frame")
        latest = dict(latest) if isinstance(latest, Mapping) else {}
        sequence = (
            latest.get("sequence_id")
            or latest.get("pressure_sample_sequence_id")
            or snapshot.get("latest_frame_sequence_id")
        )
        return {
            "digital_gauge_continuous_started": bool(snapshot.get("stream_started_at")),
            "digital_gauge_continuous_active": bool(snapshot.get("digital_gauge_continuous_active")),
            "digital_gauge_continuous_enabled": bool(snapshot.get("digital_gauge_continuous_enabled")),
            "digital_gauge_continuous_mode": snapshot.get("digital_gauge_continuous_mode") or "",
            "digital_gauge_stream_first_frame_at": snapshot.get("stream_first_frame_at") or "",
            "digital_gauge_stream_last_frame_at": (
                snapshot.get("stream_last_frame_at")
                or latest.get("frame_received_at")
                or latest.get("sample_recorded_at")
                or ""
            ),
            "digital_gauge_stream_stale": bool(stale) if stale is not None else bool(snapshot.get("latest_frame_stale")),
            "digital_gauge_stream_stale_threshold_s": (
                threshold_s if threshold_s is not None else snapshot.get("latest_frame_age_max_s")
            ),
            "digital_gauge_latest_sequence_id": None if sequence in (None, "") else sequence,
            "digital_gauge_drain_empty_count": int(snapshot.get("digital_gauge_drain_empty_count") or 0),
            "digital_gauge_drain_nonempty_count": int(snapshot.get("digital_gauge_drain_nonempty_count") or 0),
            "last_pressure_command": snapshot.get("last_pressure_command") or "",
            "last_pressure_command_may_cancel_continuous": bool(
                snapshot.get("last_pressure_command_may_cancel_continuous")
            ),
            "continuous_interrupted_by_command": bool(snapshot.get("continuous_interrupted_by_command")),
            "continuous_restart_required_before_return_to_continuous": bool(
                snapshot.get("continuous_restart_required_before_return_to_continuous")
            ),
            "continuous_restart_attempted": bool(snapshot.get("continuous_restart_attempted")),
            "continuous_restart_result": snapshot.get("continuous_restart_result") or "",
            "continuous_restart_reason": snapshot.get("continuous_restart_reason") or "",
            "pressure_source_selected": snapshot.get("pressure_source_selected") or "",
            "pressure_source_selection_reason": snapshot.get("pressure_source_selection_reason") or "",
        }

    def _mark_digital_gauge_command_may_cancel_continuous(self, *, command: str, reason: str) -> dict[str, Any]:
        with self._digital_gauge_stream_lock():
            state = self._digital_gauge_stream_state()
            state["last_pressure_command"] = command
            may_cancel = bool(state.get("digital_gauge_continuous_active"))
            state["last_pressure_command_may_cancel_continuous"] = may_cancel
            if may_cancel:
                state["continuous_interrupted_by_command"] = True
                state["continuous_restart_required_before_return_to_continuous"] = True
            payload = self._digital_gauge_state_payload(state)
        if may_cancel:
            self._record_pressure_timing_event(
                "digital_gauge_continuous_command_may_cancel",
                "warning",
                decision=reason,
                warning_code="digital_gauge_continuous_command_may_cancel",
                route_state={**payload, "command": command, "reason": reason},
            )
        return payload

    def _continuous_frame_payload(
        self,
        raw: Any,
        *,
        request_at: datetime,
        response_at: datetime,
        request_monotonic: float,
        response_monotonic: float,
        serial_port: str,
        command: str,
        mode: str,
        stage: str,
        point_index: Any = None,
    ) -> dict[str, Any]:
        data = dict(raw) if isinstance(raw, Mapping) else {}
        pressure_hpa = self._pressure_value_from_payload(data, raw)
        sequence = int(getattr(self, "_digital_gauge_continuous_frame_sequence_id", 0) or 0) + 1
        setattr(self, "_digital_gauge_continuous_frame_sequence_id", sequence)
        frame_received_at = str(
            data.get("frame_received_at")
            or data.get("pressure_sample_timestamp")
            or data.get("sample_recorded_at")
            or data.get("timestamp")
            or response_at.isoformat()
        )
        frame_monotonic = self._coerce_float(
            data.get("monotonic_timestamp")
            if data.get("monotonic_timestamp") is not None
            else data.get("sample_recorded_monotonic_s", data.get("pressure_sample_monotonic_s"))
        )
        if frame_monotonic is None:
            frame_monotonic = response_monotonic
        raw_line = str(data.get("raw_line") or data.get("raw_response") or self._safe_pressure_raw_response(raw))
        return self._pressure_sample_payload(
            {
                **data,
                "stage": stage,
                "point_index": point_index,
                "pressure_hpa": pressure_hpa,
                "source": "digital_pressure_gauge_continuous",
                "pressure_sample_source": "digital_pressure_gauge_continuous",
                "pressure_sample_timestamp": frame_received_at,
                "sample_recorded_at": frame_received_at,
                "sample_recorded_monotonic_s": frame_monotonic,
                "pressure_sample_monotonic_s": frame_monotonic,
                "request_sent_at": request_at.isoformat(),
                "response_received_at": response_at.isoformat(),
                "request_sent_monotonic_s": request_monotonic,
                "response_received_monotonic_s": response_monotonic,
                "read_latency_s": max(0.0, response_monotonic - request_monotonic),
                "is_cached": False,
                "stale_threshold_s": self._digital_gauge_latest_frame_stale_max_s(),
                "pressure_sample_stale_threshold_s": self._digital_gauge_latest_frame_stale_max_s(),
                "frame_received_at": frame_received_at,
                "monotonic_timestamp": frame_monotonic,
                "raw_line": raw_line,
                "raw_response": raw_line,
                "sequence_id": sequence,
                "pressure_sample_sequence_id": sequence,
                "serial_port": serial_port,
                "command": command,
                "parse_ok": pressure_hpa is not None,
                "digital_gauge_mode": "continuous",
                "digital_gauge_continuous_enabled": True,
                "digital_gauge_continuous_active": True,
                "digital_gauge_continuous_mode": mode,
                "last_pressure_command": command,
                "last_pressure_command_may_cancel_continuous": False,
                "continuous_interrupted_by_command": False,
                "pressure_source_selected": "digital_pressure_gauge_continuous",
                "pressure_source_selection_reason": "continuous_frame_received",
            },
            source="digital_pressure_gauge_continuous",
            request_sent_at=request_at.isoformat(),
            response_received_at=response_at.isoformat(),
            request_sent_monotonic_s=request_monotonic,
            response_received_monotonic_s=response_monotonic,
            serial_port=serial_port,
            command=command,
            raw_response=raw_line,
            is_cached=False,
            stale_threshold_s=self._digital_gauge_latest_frame_stale_max_s(),
        )

    def _digital_gauge_stream_worker(
        self,
        *,
        device: Any,
        mode: str,
        serial_port: str,
        point_index: Any = None,
    ) -> None:
        stop_event = getattr(self, "_digital_gauge_continuous_stop_event", None)
        reader = getattr(device, "read_pressure_continuous_latest", None)
        if not callable(reader):
            return
        while stop_event is not None and not stop_event.is_set():
            request_at = datetime.now(timezone.utc)
            request_monotonic = time.monotonic()
            try:
                raw = self._call_pressure_method(
                    reader,
                    drain_s=self._digital_gauge_continuous_drain_s(),
                    read_timeout_s=self._digital_gauge_continuous_read_timeout_s(),
                )
                response_at = datetime.now(timezone.utc)
                response_monotonic = time.monotonic()
            except Exception as exc:
                with self._digital_gauge_stream_lock():
                    self._digital_gauge_stream_state()["continuous_unavailable_reason"] = str(exc)
                time.sleep(self._digital_gauge_stream_poll_interval_s())
                continue
            if raw is None:
                with self._digital_gauge_stream_lock():
                    state = self._digital_gauge_stream_state()
                    state["digital_gauge_drain_empty_count"] = int(
                        state.get("digital_gauge_drain_empty_count") or 0
                    ) + 1
                    state["last_pressure_command"] = "read_pressure_continuous_latest"
                    state["last_pressure_command_may_cancel_continuous"] = False
                time.sleep(self._digital_gauge_stream_poll_interval_s())
                continue
            frame = self._continuous_frame_payload(
                raw,
                request_at=request_at,
                response_at=response_at,
                request_monotonic=request_monotonic,
                response_monotonic=response_monotonic,
                serial_port=serial_port,
                command="read_pressure_continuous_latest",
                mode=mode,
                stage="digital_gauge_stream",
                point_index=point_index,
            )
            with self._digital_gauge_stream_lock():
                state = self._digital_gauge_stream_state()
                previous = state.get("latest_frame")
                previous_mono = self._coerce_float(
                    (previous or {}).get("sample_recorded_monotonic_s")
                    if isinstance(previous, Mapping)
                    else None
                )
                frame_mono = self._coerce_float(frame.get("sample_recorded_monotonic_s"))
                if previous_mono is not None and frame_mono is not None:
                    frame["latest_frame_interval_s"] = round(max(0.0, frame_mono - previous_mono), 3)
                state["latest_frame"] = dict(frame)
                state["stream_frame_count"] = int(state.get("stream_frame_count") or 0) + 1
                state["digital_gauge_drain_nonempty_count"] = int(
                    state.get("digital_gauge_drain_nonempty_count") or 0
                ) + 1
                state["stream_last_frame_at"] = frame.get("frame_received_at") or frame.get("sample_recorded_at")
                state["stream_last_frame_monotonic_s"] = frame.get("sample_recorded_monotonic_s")
                state["last_pressure_command"] = "read_pressure_continuous_latest"
                state["last_pressure_command_may_cancel_continuous"] = False
                state["pressure_source_selected"] = "digital_pressure_gauge_continuous"
                state["pressure_source_selection_reason"] = "continuous_frame_received"
                if not state.get("stream_first_frame_at"):
                    state["stream_first_frame_at"] = frame.get("frame_received_at") or frame.get("sample_recorded_at")
                    state["stream_first_frame_monotonic_s"] = frame.get("sample_recorded_monotonic_s")
                    self._record_pressure_timing_event(
                        "digital_gauge_stream_first_frame",
                        "info",
                        point_index=point_index,
                        pressure_hpa=frame.get("pressure_hpa"),
                        decision="frame_received",
                        route_state=dict(state),
                    )
            time.sleep(self._digital_gauge_stream_poll_interval_s())

    def _start_a2_high_pressure_digital_gauge_stream(
        self,
        *,
        stage: str = "high_pressure_first_point_prearm",
        point_index: Any = None,
    ) -> dict[str, Any]:
        state = self._digital_gauge_stream_state()
        if not self._digital_gauge_continuous_enabled():
            state.update(
                {
                    "digital_gauge_continuous_enabled": False,
                    "digital_gauge_continuous_active": False,
                    "continuous_unavailable_reason": "digital_gauge_continuous_disabled",
                }
            )
            return dict(state)
        with self._digital_gauge_stream_lock():
            state = self._digital_gauge_stream_state()
            if bool(state.get("digital_gauge_continuous_active")):
                return dict(state)
            device = self._pressure_device_for_source("digital_pressure_gauge")
            if device is None:
                state.update(
                    {
                        "digital_gauge_continuous_enabled": True,
                        "digital_gauge_continuous_active": False,
                        "continuous_unavailable_reason": "pressure_device_unavailable",
                    }
                )
                return dict(state)
            starter = getattr(device, "start_pressure_continuous", None)
            reader = getattr(device, "read_pressure_continuous_latest", None)
            active = getattr(device, "pressure_continuous_active", None)
            if not callable(starter) or not callable(reader):
                state.update(
                    {
                        "digital_gauge_continuous_enabled": True,
                        "digital_gauge_continuous_active": False,
                        "digital_gauge_continuous_mode": self._digital_gauge_continuous_mode(),
                        "continuous_unavailable_reason": "continuous_pressure_methods_unavailable",
                    }
                )
                return dict(state)
            mode = self._digital_gauge_continuous_mode()
            started_at = datetime.now(timezone.utc)
            started_monotonic = time.monotonic()
            try:
                started_raw = self._call_pressure_method(starter, mode=mode, clear_buffer=True)
                started = True if started_raw is None else bool(started_raw)
            except Exception as exc:
                state.update(
                    {
                        "digital_gauge_continuous_enabled": True,
                        "digital_gauge_continuous_active": False,
                        "digital_gauge_continuous_mode": mode,
                        "continuous_unavailable_reason": str(exc),
                    }
                )
                return dict(state)
            active_now = bool(started)
            if callable(active):
                try:
                    active_now = bool(active())
                except Exception:
                    active_now = bool(started)
            elif active is not None:
                active_now = bool(active)
            state.update(
                {
                    "digital_gauge_continuous_enabled": True,
                    "digital_gauge_continuous_active": active_now,
                    "digital_gauge_continuous_mode": mode,
                    "stream_started_at": started_at.isoformat(),
                    "stream_started_monotonic_s": started_monotonic,
                    "stream_first_frame_at": "",
                    "stream_first_frame_monotonic_s": None,
                    "stream_last_frame_at": "",
                    "stream_last_frame_monotonic_s": None,
                    "stream_frame_count": 0,
                    "latest_frame": None,
                    "stale_frame_count": 0,
                    "digital_gauge_drain_empty_count": 0,
                    "digital_gauge_drain_nonempty_count": 0,
                    "last_pressure_command": "start_pressure_continuous",
                    "last_pressure_command_may_cancel_continuous": False,
                    "continuous_interrupted_by_command": False,
                    "continuous_restart_required_before_return_to_continuous": False,
                    "continuous_unavailable_reason": "" if active_now else "continuous_start_not_active",
                    "pressure_source_selected": "digital_pressure_gauge_continuous",
                    "pressure_source_selection_reason": "continuous_start",
                }
            )
            self._record_pressure_timing_event(
                "digital_gauge_stream_start",
                "info" if active_now else "fail",
                stage=stage,
                point_index=point_index,
                decision="started" if active_now else state.get("continuous_unavailable_reason"),
                route_state=dict(state),
            )
            if not active_now:
                return dict(state)
            stop_event = threading.Event()
            setattr(self, "_digital_gauge_continuous_stop_event", stop_event)
            thread = threading.Thread(
                target=self._digital_gauge_stream_worker,
                kwargs={
                    "device": device,
                    "mode": mode,
                    "serial_port": self._pressure_device_port(device),
                    "point_index": point_index,
                },
                name="a2-digital-gauge-pressure-stream",
                daemon=True,
            )
            setattr(self, "_digital_gauge_continuous_thread", thread)
            thread.start()
        deadline = time.monotonic() + self._digital_gauge_stream_first_frame_timeout_s()
        while time.monotonic() < deadline:
            with self._digital_gauge_stream_lock():
                snapshot = dict(self._digital_gauge_stream_state())
                if isinstance(snapshot.get("latest_frame"), Mapping):
                    return snapshot
            time.sleep(min(0.01, self._digital_gauge_stream_poll_interval_s()))
        with self._digital_gauge_stream_lock():
            state = self._digital_gauge_stream_state()
            if not isinstance(state.get("latest_frame"), Mapping):
                state["continuous_unavailable_reason"] = "digital_gauge_continuous_first_frame_timeout"
            return dict(state)

    def _a2_conditioning_continuous_restart_enabled(self, stage: str) -> bool:
        if str(stage or "") != "co2_route_conditioning_at_atmosphere":
            return False
        value = self._coerce_bool(
            self.host._cfg_get("workflow.pressure.a2_conditioning_restart_continuous_on_stale", True)
        )
        return True if value is None else bool(value)

    def _digital_gauge_continuous_restart_fresh_timeout_s(self) -> float:
        return max(
            0.05,
            float(
                self.host._cfg_get(
                    "workflow.pressure.a2_conditioning_continuous_restart_fresh_timeout_s",
                    self.host._cfg_get(
                        "workflow.pressure.digital_gauge_stream_first_frame_timeout_s",
                        2.5,
                    ),
                )
                or 2.5
            ),
        )

    def _stop_digital_gauge_continuous_stream(
        self,
        *,
        stage: str,
        point_index: Any = None,
        reason: str = "",
    ) -> dict[str, Any]:
        stop_event = getattr(self, "_digital_gauge_continuous_stop_event", None)
        if stop_event is not None:
            try:
                stop_event.set()
            except Exception:
                pass
        thread = getattr(self, "_digital_gauge_continuous_thread", None)
        if thread is not None and thread is not threading.current_thread():
            join = getattr(thread, "join", None)
            if callable(join):
                try:
                    join(timeout=0.25)
                except Exception:
                    pass
        device = self._pressure_device_for_source("digital_pressure_gauge")
        stop_result = "stop_method_unavailable"
        stopper = getattr(device, "stop_pressure_continuous", None) if device is not None else None
        if callable(stopper):
            try:
                stopped_raw = self._call_pressure_method(stopper)
                stop_result = "stopped" if stopped_raw is None or bool(stopped_raw) else "stop_returned_false"
            except Exception as exc:
                stop_result = f"stop_failed:{exc}"
        with self._digital_gauge_stream_lock():
            state = self._digital_gauge_stream_state()
            state["digital_gauge_continuous_active"] = False
            state["last_pressure_command"] = "stop_pressure_continuous"
            state["last_pressure_command_may_cancel_continuous"] = False
            payload = self._digital_gauge_state_payload(state)
        self._record_pressure_timing_event(
            "digital_gauge_stream_stop",
            "info" if stop_result in {"stopped", "stop_method_unavailable"} else "warning",
            stage=stage,
            point_index=point_index,
            decision=stop_result,
            route_state={**payload, "reason": reason},
        )
        return {**payload, "stop_result": stop_result}

    def _fresh_digital_gauge_frame_sample_from_state(
        self,
        *,
        stage: str,
        point_index: Any = None,
        selection_reason: str,
    ) -> Optional[dict[str, Any]]:
        with self._digital_gauge_stream_lock():
            state = dict(self._digital_gauge_stream_state())
            latest = state.get("latest_frame")
            latest = dict(latest) if isinstance(latest, Mapping) else {}
        if not latest:
            return None
        frame_mono = self._coerce_float(latest.get("sample_recorded_monotonic_s", latest.get("monotonic_timestamp")))
        age_s = max(0.0, time.monotonic() - float(frame_mono)) if frame_mono is not None else None
        threshold = self._digital_gauge_latest_frame_stale_max_s()
        pressure_hpa = self._coerce_float(latest.get("pressure_hpa"))
        if age_s is None or age_s > threshold or pressure_hpa is None:
            return None
        payload = self._digital_gauge_state_payload(state, stale=False, threshold_s=threshold)
        return self._pressure_sample_payload(
            {
                **latest,
                **payload,
                "stage": stage,
                "point_index": point_index,
                "is_cached": True,
                "pressure_sample_age_s": age_s,
                "sample_age_s": age_s,
                "latest_frame_age_s": age_s,
                "is_stale": False,
                "pressure_sample_is_stale": False,
                "stale_threshold_s": threshold,
                "pressure_sample_stale_threshold_s": threshold,
                "critical_window_uses_latest_frame": True,
                "critical_window_uses_query": False,
                "latest_frame_sequence_id": latest.get("sequence_id", latest.get("pressure_sample_sequence_id")),
                "usable_for_abort": True,
                "usable_for_ready": True,
                "usable_for_seal": True,
                "pressure_source_selected": "digital_pressure_gauge_continuous",
                "pressure_source_selection_reason": selection_reason,
                "source_selection_reason": selection_reason,
            },
            source="digital_pressure_gauge_continuous",
            is_cached=True,
            stale_threshold_s=threshold,
        )

    def _restart_a2_digital_gauge_continuous_stream(
        self,
        *,
        stage: str,
        point_index: Any = None,
        reason: str,
    ) -> Optional[dict[str, Any]]:
        with self._digital_gauge_stream_lock():
            state = self._digital_gauge_stream_state()
            if bool(state.get("continuous_restart_attempted")):
                return None
            state["continuous_restart_attempted"] = True
            state["continuous_restart_result"] = "attempting"
            state["continuous_restart_reason"] = reason
            state["continuous_restart_count"] = int(state.get("continuous_restart_count") or 0) + 1
            attempt_state = self._digital_gauge_state_payload(state, stale=True, threshold_s=self._digital_gauge_latest_frame_stale_max_s())
        self._record_pressure_timing_event(
            "digital_gauge_stream_restart_attempt",
            "warning",
            stage=stage,
            point_index=point_index,
            decision=reason,
            warning_code="digital_gauge_stream_stale_restart_attempt",
            route_state=attempt_state,
        )
        self._stop_digital_gauge_continuous_stream(stage=stage, point_index=point_index, reason=reason)
        started_state = self._start_a2_high_pressure_digital_gauge_stream(stage=stage, point_index=point_index)
        deadline = time.monotonic() + self._digital_gauge_continuous_restart_fresh_timeout_s()
        recovered: Optional[dict[str, Any]] = None
        while time.monotonic() <= deadline:
            recovered = self._fresh_digital_gauge_frame_sample_from_state(
                stage=stage,
                point_index=point_index,
                selection_reason="continuous_restart_recovered_fresh_frame",
            )
            if recovered is not None:
                break
            time.sleep(min(0.02, self._digital_gauge_stream_poll_interval_s()))
        with self._digital_gauge_stream_lock():
            state = self._digital_gauge_stream_state()
            state["continuous_restart_result"] = "recovered" if recovered is not None else "failed"
            state["pressure_source_selected"] = "digital_pressure_gauge_continuous" if recovered else ""
            state["pressure_source_selection_reason"] = (
                "continuous_restart_recovered_fresh_frame"
                if recovered is not None
                else "continuous_restart_failed_no_fresh_frame"
            )
            result_state = self._digital_gauge_state_payload(
                state,
                stale=recovered is None,
                threshold_s=self._digital_gauge_latest_frame_stale_max_s(),
            )
        self._record_pressure_timing_event(
            "digital_gauge_stream_restart_result",
            "info" if recovered is not None else "fail",
            stage=stage,
            point_index=point_index,
            pressure_hpa=(recovered or {}).get("pressure_hpa"),
            decision=state.get("continuous_restart_result"),
            error_code=None if recovered is not None else "digital_gauge_stream_restart_failed",
            route_state={**result_state, "stream_state_after_start": dict(started_state or {})},
        )
        if recovered is not None:
            recovered.update(result_state)
            recovered["continuous_restart_attempted"] = True
            recovered["continuous_restart_result"] = "recovered"
            recovered["pressure_source_selection_reason"] = "continuous_restart_recovered_fresh_frame"
            recovered["source_selection_reason"] = "continuous_restart_recovered_fresh_frame"
        return recovered

    def _digital_gauge_continuous_latest_sample(
        self,
        *,
        stage: str,
        point_index: Any = None,
        allow_restart: bool = True,
    ) -> dict[str, Any]:
        state = self._digital_gauge_stream_state()
        if not bool(state.get("digital_gauge_continuous_active")):
            state = self._start_a2_high_pressure_digital_gauge_stream(stage=stage, point_index=point_index)
        latest = state.get("latest_frame")
        if not isinstance(latest, Mapping):
            if allow_restart and self._a2_conditioning_continuous_restart_enabled(stage):
                recovered = self._restart_a2_digital_gauge_continuous_stream(
                    stage=stage,
                    point_index=point_index,
                    reason="latest_frame_unavailable",
                )
                if recovered is not None:
                    return recovered
                state = self._digital_gauge_stream_state()
            state_payload = self._digital_gauge_state_payload(
                state,
                stale=True,
                threshold_s=self._digital_gauge_latest_frame_stale_max_s(),
            )
            sample = self._pressure_sample_payload(
                {
                    **state_payload,
                    "stage": stage,
                    "point_index": point_index,
                    "source": "digital_pressure_gauge_continuous",
                    "pressure_sample_source": "digital_pressure_gauge_continuous",
                    "digital_gauge_mode": "continuous",
                    "digital_gauge_continuous_enabled": bool(state.get("digital_gauge_continuous_enabled")),
                    "digital_gauge_continuous_active": bool(state.get("digital_gauge_continuous_active")),
                    "digital_gauge_continuous_mode": state.get("digital_gauge_continuous_mode"),
                    "critical_window_uses_latest_frame": True,
                    "critical_window_uses_query": False,
                    "critical_window_blocking_query_count": state.get("blocking_query_count_in_critical_window", 0),
                    "critical_window_blocking_query_total_s": state.get("critical_window_blocking_query_total_s", 0.0),
                    "parse_ok": False,
                    "is_cached": True,
                    "is_stale": True,
                    "pressure_sample_is_stale": True,
                    "stale_threshold_s": self._digital_gauge_latest_frame_stale_max_s(),
                    "pressure_sample_stale_threshold_s": self._digital_gauge_latest_frame_stale_max_s(),
                    "error": state.get("continuous_unavailable_reason") or "digital_gauge_continuous_latest_unavailable",
                    "usable_for_abort": False,
                    "usable_for_ready": False,
                    "usable_for_seal": False,
                    "pressure_source_selected": "",
                    "pressure_source_selection_reason": state.get("pressure_source_selection_reason")
                    or "digital_gauge_continuous_latest_unavailable",
                    "source_selection_reason": state.get("pressure_source_selection_reason")
                    or "digital_gauge_continuous_latest_unavailable",
                },
                source="digital_pressure_gauge_continuous",
                is_cached=True,
                stale_threshold_s=self._digital_gauge_latest_frame_stale_max_s(),
            )
            self._record_pressure_timing_event(
                "digital_gauge_latest_frame_stale",
                "warning",
                stage=stage,
                point_index=point_index,
                decision="latest_frame_unavailable",
                warning_code="digital_gauge_latest_frame_unavailable",
                route_state=sample,
            )
            return sample
        now_monotonic = time.monotonic()
        frame_mono = self._coerce_float(latest.get("sample_recorded_monotonic_s", latest.get("monotonic_timestamp")))
        age_s = max(0.0, now_monotonic - float(frame_mono)) if frame_mono is not None else None
        threshold = self._digital_gauge_latest_frame_stale_max_s()
        stale = bool(age_s is None or age_s > threshold)
        if stale and allow_restart and self._a2_conditioning_continuous_restart_enabled(stage):
            recovered = self._restart_a2_digital_gauge_continuous_stream(
                stage=stage,
                point_index=point_index,
                reason="latest_frame_stale",
            )
            if recovered is not None:
                return recovered
            state = self._digital_gauge_stream_state()
            latest = state.get("latest_frame") if isinstance(state.get("latest_frame"), Mapping) else latest
        state_payload = self._digital_gauge_state_payload(state, stale=stale, threshold_s=threshold)
        data = {
            **dict(latest),
            **state_payload,
            "stage": stage,
            "point_index": point_index,
            "is_cached": True,
            "pressure_sample_age_s": age_s,
            "sample_age_s": age_s,
            "latest_frame_age_s": age_s,
            "is_stale": stale,
            "pressure_sample_is_stale": stale,
            "stale_threshold_s": threshold,
            "pressure_sample_stale_threshold_s": threshold,
            "critical_window_uses_latest_frame": True,
            "critical_window_uses_query": False,
            "critical_window_blocking_query_count": state.get("blocking_query_count_in_critical_window", 0),
            "critical_window_blocking_query_total_s": state.get("critical_window_blocking_query_total_s", 0.0),
            "latest_frame_sequence_id": latest.get("sequence_id", latest.get("pressure_sample_sequence_id")),
            "usable_for_abort": not stale and self._coerce_float(latest.get("pressure_hpa")) is not None,
            "usable_for_ready": not stale and self._coerce_float(latest.get("pressure_hpa")) is not None,
            "usable_for_seal": not stale and self._coerce_float(latest.get("pressure_hpa")) is not None,
            "pressure_source_selected": "digital_pressure_gauge_continuous" if not stale else "",
            "pressure_source_selection_reason": (
                "digital_gauge_continuous_latest_fresh"
                if not stale
                else state.get("pressure_source_selection_reason") or "digital_gauge_continuous_latest_stale"
            ),
            "source_selection_reason": (
                "digital_gauge_continuous_latest_fresh"
                if not stale
                else state.get("pressure_source_selection_reason") or "digital_gauge_continuous_latest_stale"
            ),
        }
        sample = self._pressure_sample_payload(
            data,
            source="digital_pressure_gauge_continuous",
            is_cached=True,
            stale_threshold_s=threshold,
        )
        with self._digital_gauge_stream_lock():
            state = self._digital_gauge_stream_state()
            if age_s is not None:
                current_max = self._coerce_float(state.get("latest_frame_age_max_s"))
                state["latest_frame_age_max_s"] = round(
                    float(age_s if current_max is None else max(float(current_max), float(age_s))),
                    3,
                )
            if stale:
                state["stale_frame_count"] = int(state.get("stale_frame_count") or 0) + 1
        self._record_pressure_timing_event(
            "digital_gauge_latest_frame_stale" if stale else "digital_gauge_latest_frame_used",
            "warning" if stale else "info",
            stage=stage,
            point_index=point_index,
            pressure_hpa=sample.get("pressure_hpa"),
            decision="stale" if stale else "used",
            warning_code="digital_gauge_latest_frame_stale" if stale else None,
            route_state=sample,
        )
        return sample

    def _read_pressure_with_recovery(self) -> Optional[float]:
        """Read pressure from COM22 with automatic recovery on empty response.

        Tries fast reads, then normal reads, then orchestrator fallback.
        A short cooldown is inserted between attempts because the P3 serial
        port can saturate under rapid polling.
        """
        _gauge = self.host._device("pressure_meter", "pressure_gauge")
        if _gauge is None:
            return None

        for _attempt in range(3):
            for _method_name in ("read_pressure_fast", "read_pressure", "read_pressure_hpa", "get_pressure", "get_pressure_hpa"):
                _method = getattr(_gauge, _method_name, None)
                if not callable(_method):
                    continue
                try:
                    _result = _method()
                    if isinstance(_result, (int, float)):
                        _result = float(_result)
                        if _result > 0:
                            return _result
                    elif isinstance(_result, dict):
                        _value = self._coerce_float(_result.get("pressure_hpa"))
                        if _value is not None and _value > 0:
                            return _value
                except Exception:
                    continue

            if _attempt < 2:
                import time as _time
                _time.sleep(0.6)

        _recovery = getattr(self.host, "_get_latest_pressure_hpa", None)
        if callable(_recovery):
            try:
                _value = _recovery()
                if _value is not None and _value > 0:
                    return float(_value)
            except Exception:
                pass

        return None

    def _read_pressure_sample(
        self,
        pressure_reader: Optional[Callable[[], Optional[float]]],
        *,
        source: str = "pressure_gauge",
    ) -> dict[str, Any]:
        request_at = datetime.now(timezone.utc)
        request_monotonic = time.monotonic()
        if pressure_reader is None:
            response_at = datetime.now(timezone.utc)
            response_monotonic = time.monotonic()
            return self._pressure_sample_payload(
                None,
                source=source,
                request_sent_at=request_at.isoformat(),
                response_received_at=response_at.isoformat(),
                request_sent_monotonic_s=request_monotonic,
                response_received_monotonic_s=response_monotonic,
                error="pressure_reader_unavailable",
            )
        previous_sequence = None
        previous = getattr(self, "_last_pressure_sample_metadata", None)
        if isinstance(previous, Mapping):
            previous_sequence = previous.get("pressure_sample_sequence_id")
        try:
            raw = pressure_reader()
            response_at = datetime.now(timezone.utc)
            response_monotonic = time.monotonic()
        except Exception as exc:
            response_at = datetime.now(timezone.utc)
            response_monotonic = time.monotonic()
            return self._pressure_sample_payload(
                None,
                source=source,
                request_sent_at=request_at.isoformat(),
                response_received_at=response_at.isoformat(),
                request_sent_monotonic_s=request_monotonic,
                response_received_monotonic_s=response_monotonic,
                error=str(exc),
            )
        latest = getattr(self, "_last_pressure_sample_metadata", None)
        if isinstance(latest, Mapping) and latest.get("pressure_sample_sequence_id") != previous_sequence:
            return dict(latest)
        return self._pressure_sample_payload(
            raw,
            source=source,
            request_sent_at=request_at.isoformat(),
            response_received_at=response_at.isoformat(),
            request_sent_monotonic_s=request_monotonic,
            response_received_monotonic_s=response_monotonic,
            raw_response=raw,
        )

    def _current_pressure_sample(self, *, source: str = "pressure_gauge") -> dict[str, Any]:
        return self._read_pressure_sample(self.host._make_pressure_reader(), source=source)

    def _pressure_device_for_source(self, source: str) -> Any:
        normalized = str(source or "").strip().lower()
        getter = getattr(self.host, "_device", None)
        if not callable(getter):
            return None
        if normalized in {"digital_pressure_gauge", "pressure_gauge", "gauge"}:
            return getter("pressure_meter", "pressure_gauge")
        if normalized in {"pace_controller", "pressure_controller", "pace"}:
            return getter("pressure_controller", "pace")
        return None

    def _pressure_device_port(self, device: Any) -> str:
        for name in ("port", "serial_port", "_port", "com_port"):
            value = getattr(device, name, None)
            if value not in (None, ""):
                return str(value)
        config = getattr(device, "config", None)
        if isinstance(config, Mapping):
            value = config.get("port") or config.get("serial_port")
            if value not in (None, ""):
                return str(value)
        return ""

    def _pressure_sample_from_device(self, source: str) -> dict[str, Any]:
        device = self._pressure_device_for_source(source)
        if device is None:
            return self._pressure_sample_payload(None, source=source, error="pressure_device_unavailable")
        serial_port = self._pressure_device_port(device)
        normalized_source = str(source or "").strip().lower()
        digital_source = normalized_source in {"digital_pressure_gauge", "pressure_gauge", "gauge"}
        for method_name in ("read_pressure", "read_pressure_hpa", "get_pressure", "get_pressure_hpa"):
            method = getattr(device, method_name, None)
            if not callable(method):
                continue
            command_state = (
                self._mark_digital_gauge_command_may_cancel_continuous(
                    command=method_name,
                    reason="digital_gauge_p3_query_while_continuous_active",
                )
                if digital_source
                else {}
            )
            request_at = datetime.now(timezone.utc)
            request_monotonic = time.monotonic()
            try:
                raw = method()
                response_at = datetime.now(timezone.utc)
                response_monotonic = time.monotonic()
                latency_s = max(0.0, response_monotonic - request_monotonic)
                if (
                    digital_source
                    and (
                        bool(getattr(self.host, "_a2_high_pressure_first_point_mode_enabled", False))
                        or bool(command_state.get("last_pressure_command_may_cancel_continuous"))
                    )
                ):
                    self._record_critical_window_blocking_query(
                        source=source,
                        command=method_name,
                        duration_s=latency_s,
                        reason=(
                            "digital_gauge_query_called_while_continuous_active"
                            if command_state.get("last_pressure_command_may_cancel_continuous")
                            else "digital_gauge_query_called_while_high_pressure_first_point_active"
                        ),
                    )
                payload_raw = dict(raw) if isinstance(raw, Mapping) else {"pressure_hpa": self._coerce_float(raw)}
                return self._pressure_sample_payload(
                    {**payload_raw, **command_state},
                    source=source,
                    request_sent_at=request_at.isoformat(),
                    response_received_at=response_at.isoformat(),
                    request_sent_monotonic_s=request_monotonic,
                    response_received_monotonic_s=response_monotonic,
                    serial_port=serial_port,
                    command=method_name,
                    raw_response=raw,
                )
            except Exception as exc:
                response_at = datetime.now(timezone.utc)
                response_monotonic = time.monotonic()
                latency_s = max(0.0, response_monotonic - request_monotonic)
                if (
                    digital_source
                    and (
                        bool(getattr(self.host, "_a2_high_pressure_first_point_mode_enabled", False))
                        or bool(command_state.get("last_pressure_command_may_cancel_continuous"))
                    )
                ):
                    self._record_critical_window_blocking_query(
                        source=source,
                        command=method_name,
                        duration_s=latency_s,
                        reason=(
                            "digital_gauge_query_failed_while_continuous_active"
                            if command_state.get("last_pressure_command_may_cancel_continuous")
                            else "digital_gauge_query_failed_while_high_pressure_first_point_active"
                        ),
                    )
                return self._pressure_sample_payload(
                    {**command_state, "parse_ok": False},
                    source=source,
                    request_sent_at=request_at.isoformat(),
                    response_received_at=response_at.isoformat(),
                    request_sent_monotonic_s=request_monotonic,
                    response_received_monotonic_s=response_monotonic,
                    serial_port=serial_port,
                    command=method_name,
                    error=str(exc),
                )
        for method_name in ("status", "read_current", "read", "fetch_all"):
            method = getattr(device, method_name, None)
            if not callable(method):
                continue
            command_state = (
                self._mark_digital_gauge_command_may_cancel_continuous(
                    command=method_name,
                    reason="digital_gauge_status_query_while_continuous_active",
                )
                if digital_source
                else {}
            )
            request_at = datetime.now(timezone.utc)
            request_monotonic = time.monotonic()
            try:
                raw = method()
                response_at = datetime.now(timezone.utc)
                response_monotonic = time.monotonic()
                latency_s = max(0.0, response_monotonic - request_monotonic)
                if (
                    digital_source
                    and (
                        bool(getattr(self.host, "_a2_high_pressure_first_point_mode_enabled", False))
                        or bool(command_state.get("last_pressure_command_may_cancel_continuous"))
                    )
                ):
                    self._record_critical_window_blocking_query(
                        source=source,
                        command=method_name,
                        duration_s=latency_s,
                        reason=(
                            "digital_gauge_status_query_called_while_continuous_active"
                            if command_state.get("last_pressure_command_may_cancel_continuous")
                            else "digital_gauge_status_query_called_while_high_pressure_first_point_active"
                        ),
                    )
                snapshot = self._normalize_snapshot(raw)
                return self._pressure_sample_payload(
                    {**snapshot, **command_state},
                    source=source,
                    request_sent_at=request_at.isoformat(),
                    response_received_at=response_at.isoformat(),
                    request_sent_monotonic_s=request_monotonic,
                    response_received_monotonic_s=response_monotonic,
                    serial_port=serial_port,
                    command=method_name,
                    raw_response=raw,
                )
            except Exception as exc:
                response_at = datetime.now(timezone.utc)
                response_monotonic = time.monotonic()
                latency_s = max(0.0, response_monotonic - request_monotonic)
                if (
                    digital_source
                    and (
                        bool(getattr(self.host, "_a2_high_pressure_first_point_mode_enabled", False))
                        or bool(command_state.get("last_pressure_command_may_cancel_continuous"))
                    )
                ):
                    self._record_critical_window_blocking_query(
                        source=source,
                        command=method_name,
                        duration_s=latency_s,
                        reason=(
                            "digital_gauge_status_query_failed_while_continuous_active"
                            if command_state.get("last_pressure_command_may_cancel_continuous")
                            else "digital_gauge_status_query_failed_while_high_pressure_first_point_active"
                        ),
                    )
                return self._pressure_sample_payload(
                    {**command_state, "parse_ok": False},
                    source=source,
                    request_sent_at=request_at.isoformat(),
                    response_received_at=response_at.isoformat(),
                    request_sent_monotonic_s=request_monotonic,
                    response_received_monotonic_s=response_monotonic,
                    serial_port=serial_port,
                    command=method_name,
                    error=str(exc),
                )
        return self._pressure_sample_payload(
            (
                self._mark_digital_gauge_command_may_cancel_continuous(
                    command="pressure_read_method_unavailable",
                    reason="digital_gauge_method_lookup_while_continuous_active",
                )
                if digital_source
                else None
            ),
            source=source,
            serial_port=serial_port,
            error="pressure_read_method_unavailable",
        )

    def _a2_v1_pressure_gauge_fast_timeout_s(self) -> float:
        value = self.host._cfg_get(
            "workflow.pressure.fast_gauge_response_timeout_s",
            self.host._cfg_get("workflow.pressure.a2_v1_aligned_fast_gauge_response_timeout_s", 0.6),
        )
        try:
            return max(0.05, float(value))
        except Exception:
            return 0.6

    def _a2_v1_pressure_gauge_normal_timeout_s(self) -> float:
        value = self.host._cfg_get(
            "workflow.pressure.normal_gauge_response_timeout_s",
            self.host._cfg_get("workflow.pressure.a2_v1_aligned_normal_gauge_response_timeout_s", 2.2),
        )
        try:
            return max(0.05, float(value))
        except Exception:
            return 2.2

    def _a2_v1_pressure_gauge_read_retries(self) -> int:
        value = self.host._cfg_get(
            "workflow.pressure.fast_gauge_read_retries",
            self.host._cfg_get("workflow.pressure.a2_v1_aligned_gauge_read_retries", 1),
        )
        try:
            return max(1, int(value))
        except Exception:
            return 1

    def _a2_v1_aligned_p3_method_sample(
        self,
        *,
        method_name: str,
        stage: str,
        point_index: Any = None,
        selection_reason: str,
        fast: bool,
    ) -> dict[str, Any]:
        device = self._pressure_device_for_source("digital_pressure_gauge")
        if device is None:
            return self._pressure_sample_payload(
                {
                    "stage": stage,
                    "point_index": point_index,
                    "parse_ok": False,
                    "pressure_source_selected": "",
                    "pressure_source_selection_reason": "digital_gauge_v1_aligned_read_unavailable",
                    "source_selection_reason": "digital_gauge_v1_aligned_read_unavailable",
                    "critical_window_uses_latest_frame": False,
                    "critical_window_uses_query": True,
                },
                source="digital_pressure_gauge_p3",
                error="pressure_device_unavailable",
            )
        method = getattr(device, method_name, None)
        if not callable(method):
            return self._pressure_sample_payload(
                {
                    "stage": stage,
                    "point_index": point_index,
                    "parse_ok": False,
                    "pressure_source_selected": "",
                    "pressure_source_selection_reason": "digital_gauge_v1_aligned_read_unavailable",
                    "source_selection_reason": "digital_gauge_v1_aligned_read_unavailable",
                    "critical_window_uses_latest_frame": False,
                    "critical_window_uses_query": True,
                },
                source="digital_pressure_gauge_p3",
                error=f"{method_name}_unavailable",
            )

        command_state = self._mark_digital_gauge_command_may_cancel_continuous(
            command=method_name,
            reason="a2_v1_aligned_p3_query_while_continuous_active",
        )
        request_at = datetime.now(timezone.utc)
        request_monotonic = time.monotonic()
        try:
            if fast:
                raw = self._call_pressure_method(
                    method,
                    response_timeout_s=self._a2_v1_pressure_gauge_fast_timeout_s(),
                    retries=self._a2_v1_pressure_gauge_read_retries(),
                    retry_sleep_s=0.0,
                    clear_buffer=False,
                )
            else:
                raw = self._call_pressure_method(
                    method,
                    response_timeout_s=self._a2_v1_pressure_gauge_normal_timeout_s(),
                    retries=1,
                    retry_sleep_s=0.0,
                    clear_buffer=False,
                )
            response_at = datetime.now(timezone.utc)
            response_monotonic = time.monotonic()
            latency_s = max(0.0, response_monotonic - request_monotonic)
            self._record_critical_window_blocking_query(
                source="digital_pressure_gauge",
                command=method_name,
                duration_s=latency_s,
                reason="a2_v1_aligned_p3_query",
            )
            payload_raw = dict(raw) if isinstance(raw, Mapping) else {"pressure_hpa": self._coerce_float(raw)}
            return self._pressure_sample_payload(
                {
                    **payload_raw,
                    **command_state,
                    "stage": stage,
                    "point_index": point_index,
                    "source": "digital_pressure_gauge_p3",
                    "pressure_sample_source": "digital_pressure_gauge_p3",
                    "digital_gauge_mode": "v1_aligned_p3_fast" if fast else "v1_aligned_p3_normal",
                    "read_latency_s": latency_s,
                    "pressure_source_selected": "digital_pressure_gauge_p3",
                    "pressure_source_selection_reason": selection_reason,
                    "source_selection_reason": selection_reason,
                    "critical_window_uses_latest_frame": False,
                    "critical_window_uses_query": True,
                    "parse_ok": self._coerce_float(payload_raw.get("pressure_hpa")) is not None,
                },
                source="digital_pressure_gauge_p3",
                request_sent_at=request_at.isoformat(),
                response_received_at=response_at.isoformat(),
                request_sent_monotonic_s=request_monotonic,
                response_received_monotonic_s=response_monotonic,
                serial_port=self._pressure_device_port(device),
                command=method_name,
                raw_response=raw,
            )
        except Exception as exc:
            response_at = datetime.now(timezone.utc)
            response_monotonic = time.monotonic()
            latency_s = max(0.0, response_monotonic - request_monotonic)
            self._record_critical_window_blocking_query(
                source="digital_pressure_gauge",
                command=method_name,
                duration_s=latency_s,
                reason="a2_v1_aligned_p3_query_failed",
            )
            return self._pressure_sample_payload(
                {
                    **command_state,
                    "stage": stage,
                    "point_index": point_index,
                    "source": "digital_pressure_gauge_p3",
                    "pressure_sample_source": "digital_pressure_gauge_p3",
                    "digital_gauge_mode": "v1_aligned_p3_fast" if fast else "v1_aligned_p3_normal",
                    "read_latency_s": latency_s,
                    "pressure_source_selected": "",
                    "pressure_source_selection_reason": selection_reason,
                    "source_selection_reason": selection_reason,
                    "critical_window_uses_latest_frame": False,
                    "critical_window_uses_query": True,
                    "parse_ok": False,
                },
                source="digital_pressure_gauge_p3",
                request_sent_at=request_at.isoformat(),
                response_received_at=response_at.isoformat(),
                request_sent_monotonic_s=request_monotonic,
                response_received_monotonic_s=response_monotonic,
                serial_port=self._pressure_device_port(device),
                command=method_name,
                error=str(exc),
            )

    def _a2_v1_aligned_pressure_gauge_sample(
        self,
        *,
        stage: str,
        point_index: Any = None,
        continuous_sample: Optional[Mapping[str, Any]] = None,
    ) -> dict[str, Any]:
        fast_sample = self._a2_v1_aligned_p3_method_sample(
            method_name="read_pressure_fast",
            stage=stage,
            point_index=point_index,
            selection_reason="continuous_stale_fallback_to_p3_fast",
            fast=True,
        )
        fast_ok = self._pressure_sample_usable(fast_sample, "abort") and bool(fast_sample.get("parse_ok", True))
        if fast_ok:
            fast_sample.update(
                {
                    "a2_3_pressure_source_strategy": "v1_aligned",
                    "continuous_pressure_sample": dict(continuous_sample or {}),
                    "p3_fast_fallback_attempted": True,
                    "p3_fast_fallback_result": "success",
                    "normal_p3_fallback_attempted": False,
                    "normal_p3_fallback_result": "",
                    "pressure_source_selected": "digital_pressure_gauge_p3",
                    "pressure_source_selection_reason": "continuous_stale_fallback_to_p3_fast",
                    "source_selection_reason": "continuous_stale_fallback_to_p3_fast",
                    "usable_for_abort": True,
                    "usable_for_ready": True,
                    "usable_for_seal": True,
                }
            )
            return fast_sample

        normal_method = "read_pressure"
        device = self._pressure_device_for_source("digital_pressure_gauge")
        if device is not None and not callable(getattr(device, normal_method, None)):
            normal_method = "read_pressure_hpa"
        normal_sample = self._a2_v1_aligned_p3_method_sample(
            method_name=normal_method,
            stage=stage,
            point_index=point_index,
            selection_reason="p3_fast_failed_fallback_normal_p3",
            fast=False,
        )
        normal_ok = self._pressure_sample_usable(normal_sample, "abort") and bool(normal_sample.get("parse_ok", True))
        normal_sample.update(
            {
                "a2_3_pressure_source_strategy": "v1_aligned",
                "continuous_pressure_sample": dict(continuous_sample or {}),
                "p3_fast_fallback_attempted": True,
                "p3_fast_fallback_result": "failed",
                "normal_p3_fallback_attempted": True,
                "normal_p3_fallback_result": "success" if normal_ok else "failed",
                "pressure_source_selected": "digital_pressure_gauge_p3" if normal_ok else "",
                "pressure_source_selection_reason": (
                    "p3_fast_failed_fallback_normal_p3"
                    if normal_ok
                    else "digital_gauge_v1_aligned_read_unavailable"
                ),
                "source_selection_reason": (
                    "p3_fast_failed_fallback_normal_p3"
                    if normal_ok
                    else "digital_gauge_v1_aligned_read_unavailable"
                ),
                "fail_closed_reason": "" if normal_ok else "digital_gauge_v1_aligned_read_unavailable",
                "usable_for_abort": bool(normal_ok),
                "usable_for_ready": bool(normal_ok),
                "usable_for_seal": bool(normal_ok),
            }
        )
        return normal_sample

    def _primary_pressure_source(self) -> str:
        value = str(
            self.host._cfg_get("workflow.pressure.primary_pressure_source", "digital_pressure_gauge") or ""
        ).strip().lower()
        aliases = {
            "pressure_gauge": "digital_pressure_gauge",
            "gauge": "digital_pressure_gauge",
            "pressure_meter": "digital_pressure_gauge",
            "pace": "pace_controller",
            "pressure_controller": "pace_controller",
        }
        return aliases.get(value, value or "digital_pressure_gauge")

    def _pressure_source_cross_check_enabled(self) -> bool:
        value = self._coerce_bool(
            self.host._cfg_get("workflow.pressure.pressure_source_cross_check_enabled", True)
        )
        return True if value is None else bool(value)

    def _pressure_sample_usable(self, sample: Mapping[str, Any], purpose: str) -> bool:
        key = f"usable_for_{purpose}"
        if key in sample:
            return bool(sample.get(key))
        return sample.get("pressure_hpa") is not None and not bool(sample.get("is_stale"))

    def _source_sample(self, samples_by_source: Mapping[str, Mapping[str, Any]], source: str) -> Mapping[str, Any]:
        return samples_by_source.get(source) or {}

    def _current_dual_pressure_sample(self, *, stage: str = "", point_index: Any = None) -> dict[str, Any]:
        primary_source = self._primary_pressure_source()
        cross_check_enabled = self._pressure_source_cross_check_enabled()
        digital_sample = self._pressure_sample_from_device("digital_pressure_gauge")
        pace_sample = (
            self._pressure_sample_from_device("pace_controller")
            if cross_check_enabled or primary_source == "pace_controller"
            else self._pressure_sample_payload(None, source="pace_controller", error="cross_check_disabled")
        )
        samples_by_source: dict[str, Mapping[str, Any]] = {
            "digital_pressure_gauge": digital_sample,
            "pace_controller": pace_sample,
        }
        primary_sample = self._source_sample(samples_by_source, primary_source)
        alternate_source = "pace_controller" if primary_source == "digital_pressure_gauge" else "digital_pressure_gauge"
        alternate_sample = self._source_sample(samples_by_source, alternate_source)
        selected_sample = primary_sample
        selection_reason = "primary_pressure_source"
        if not self._pressure_sample_usable(primary_sample, "abort") and self._pressure_sample_usable(
            alternate_sample, "abort"
        ):
            selected_sample = alternate_sample
            selection_reason = "primary_unusable_alternate_usable"
        elif not self._pressure_sample_usable(primary_sample, "abort"):
            selection_reason = "no_usable_pressure_sample"
        selected_source = str(selected_sample.get("source") or selected_sample.get("pressure_sample_source") or primary_source)
        gauge_pressure = self._coerce_float(digital_sample.get("pressure_hpa"))
        pace_pressure = self._coerce_float(pace_sample.get("pressure_hpa"))
        disagreement = (
            abs(float(gauge_pressure) - float(pace_pressure))
            if gauge_pressure is not None and pace_pressure is not None
            else None
        )
        disagreement_warn_hpa = self._coerce_float(
            self.host._cfg_get("workflow.pressure.pressure_source_disagreement_warn_hpa", 10.0)
        )
        disagreement_warning = (
            disagreement is not None
            and disagreement_warn_hpa is not None
            and float(disagreement) > float(disagreement_warn_hpa)
        )
        selected_pressure = self._coerce_float(selected_sample.get("pressure_hpa"))
        selected_is_stale = bool(selected_sample.get("is_stale", selected_sample.get("pressure_sample_is_stale")))
        selected_usable = bool(selected_pressure is not None and not selected_is_stale)
        result: dict[str, Any] = {
            **dict(selected_sample),
            "stage": stage,
            "point_index": point_index,
            "primary_pressure_source": primary_source,
            "pressure_source_cross_check_enabled": cross_check_enabled,
            "pressure_source_used_for_decision": selected_source if selected_usable else "",
            "source_selection_reason": selection_reason,
            "pressure_source_used_for_abort": selected_source if self._pressure_sample_usable(selected_sample, "abort") else "",
            "pressure_source_used_for_ready": selected_source if self._pressure_sample_usable(selected_sample, "ready") else "",
            "pressure_source_used_for_seal": selected_source if self._pressure_sample_usable(selected_sample, "seal") else "",
            "pressure_source_disagreement_hpa": None if disagreement is None else round(float(disagreement), 3),
            "pressure_source_disagreement_warning": bool(disagreement_warning),
            "pressure_source_disagreement_warn_hpa": disagreement_warn_hpa,
            "pace_pressure_sample": dict(pace_sample),
            "digital_gauge_pressure_sample": dict(digital_sample),
            "pace_pressure_hpa": pace_pressure,
            "pace_pressure_latency_s": pace_sample.get("read_latency_s"),
            "pace_pressure_age_s": pace_sample.get("sample_age_s"),
            "pace_pressure_stale": bool(pace_sample.get("is_stale", pace_sample.get("pressure_sample_is_stale"))),
            "digital_gauge_pressure_hpa": gauge_pressure,
            "digital_gauge_latency_s": digital_sample.get("read_latency_s"),
            "digital_gauge_age_s": digital_sample.get("sample_age_s"),
            "digital_gauge_stale": bool(
                digital_sample.get("is_stale", digital_sample.get("pressure_sample_is_stale"))
            ),
        }
        return result

    def _current_high_pressure_first_point_sample(self, *, stage: str = "", point_index: Any = None) -> dict[str, Any]:
        digital_sample = self._digital_gauge_continuous_latest_sample(stage=stage, point_index=point_index)
        digital_pressure = self._coerce_float(digital_sample.get("pressure_hpa"))
        digital_is_stale = bool(digital_sample.get("is_stale", digital_sample.get("pressure_sample_is_stale")))
        digital_usable = bool(digital_pressure is not None and not digital_is_stale)
        pace_aux_enabled = bool(self.host._cfg_get("workflow.pressure.pace_aux_enabled", True))
        pace_read_when_digital_fresh = bool(
            self.host._cfg_get("workflow.pressure.pace_aux_read_when_digital_fresh", False)
        )
        pace_sample: dict[str, Any]
        if pace_aux_enabled and (pace_read_when_digital_fresh or not digital_usable):
            self._record_pressure_timing_event(
                "pace_aux_pressure_read_start",
                "start",
                stage=stage,
                point_index=point_index,
                decision="pace_aux_read",
            )
            pace_sample = self._pressure_sample_from_device("pace_controller")
            self._record_pressure_timing_event(
                "pace_aux_pressure_read_end",
                "end",
                stage=stage,
                point_index=point_index,
                pressure_hpa=pace_sample.get("pressure_hpa"),
                duration_s=pace_sample.get("read_latency_s"),
                decision="ok" if pace_sample.get("parse_ok") else "unavailable",
                route_state=pace_sample,
            )
        else:
            pace_sample = self._pressure_sample_payload(
                None,
                source="pace_controller",
                error="pace_aux_not_read_digital_latest_fresh"
                if pace_aux_enabled
                else "pace_aux_disabled_for_high_pressure_first_point",
            )
        pace_pressure = self._coerce_float(pace_sample.get("pressure_hpa"))
        pace_is_stale = bool(pace_sample.get("is_stale", pace_sample.get("pressure_sample_is_stale")))
        pace_usable = bool(pace_pressure is not None and not pace_is_stale)
        disagreement = (
            abs(float(digital_pressure) - float(pace_pressure))
            if digital_pressure is not None and pace_pressure is not None
            else None
        )
        disagreement_warn_hpa = self._coerce_float(
            self.host._cfg_get(
                "workflow.pressure.pace_aux_disagreement_warn_hpa",
                self.host._cfg_get("workflow.pressure.pressure_source_disagreement_warn_hpa", 10.0),
            )
        )
        disagreement_warning = bool(
            disagreement is not None
            and disagreement_warn_hpa is not None
            and float(disagreement) > float(disagreement_warn_hpa)
        )
        pace_overlap_samples = 1 if disagreement is not None else 0
        topology_connected = bool(
            self.host._cfg_get("workflow.pressure.pace_aux_main_line_topology_connected", True)
        )
        pace_aux_candidate = bool(
            not digital_usable
            and pace_aux_enabled
            and pace_usable
            and topology_connected
            and pace_overlap_samples > 0
            and not disagreement_warning
        )
        selected_sample: Mapping[str, Any] = digital_sample
        selected_source = "digital_pressure_gauge_continuous"
        selection_reason = "digital_gauge_continuous_latest_fresh"
        if not digital_usable and pace_aux_candidate:
            selected_sample = pace_sample
            selected_source = "pace_controller_auxiliary"
            selection_reason = "digital_latest_stale_pace_aux_consistent"
        elif not digital_usable:
            selected_source = ""
            selection_reason = (
                "digital_latest_stale_pace_aux_disagreement"
                if disagreement_warning
                else "digital_latest_unusable_fail_closed"
            )
        elif disagreement_warning:
            selection_reason = "digital_latest_fresh_pace_aux_disagreement_ignored"
        selected_pressure = self._coerce_float(selected_sample.get("pressure_hpa"))
        selected_is_stale = bool(selected_sample.get("is_stale", selected_sample.get("pressure_sample_is_stale")))
        selected_usable = bool(selected_pressure is not None and not selected_is_stale)
        result: dict[str, Any] = {
            **dict(selected_sample),
            "source": selected_source or selected_sample.get("source"),
            "pressure_sample_source": selected_source or selected_sample.get("pressure_sample_source"),
            "stage": stage,
            "point_index": point_index,
            "primary_pressure_source": "digital_pressure_gauge_continuous",
            "pressure_source_cross_check_enabled": self._pressure_source_cross_check_enabled(),
            "pressure_source_cross_check_role": "pace_controller_auxiliary_cross_check_only_unless_digital_latest_stale_and_consistent",
            "pressure_source_used_for_decision": selected_source if selected_usable else "",
            "source_selection_reason": selection_reason,
            "pressure_source_used_for_abort": selected_source if selected_usable and self._pressure_sample_usable(selected_sample, "abort") else "",
            "pressure_source_used_for_ready": selected_source if selected_usable and self._pressure_sample_usable(selected_sample, "ready") else "",
            "pressure_source_used_for_seal": selected_source if selected_usable and self._pressure_sample_usable(selected_sample, "seal") else "",
            "pressure_source_disagreement_hpa": None if disagreement is None else round(float(disagreement), 3),
            "pressure_source_disagreement_warning": bool(disagreement_warning),
            "pressure_source_disagreement_warn_hpa": disagreement_warn_hpa,
            "pace_aux_enabled": pace_aux_enabled,
            "pace_aux_topology_connected": topology_connected,
            "pace_aux_trigger_candidate": pace_aux_candidate,
            "pace_digital_overlap_samples": pace_overlap_samples,
            "pace_digital_max_diff_hpa": None if disagreement is None else round(float(disagreement), 3),
            "pace_pressure_sample": dict(pace_sample),
            "digital_gauge_pressure_sample": dict(digital_sample),
            "pace_pressure_hpa": pace_pressure,
            "pace_pressure_latency_s": pace_sample.get("read_latency_s"),
            "pace_pressure_age_s": pace_sample.get("sample_age_s"),
            "pace_pressure_stale": pace_is_stale,
            "digital_gauge_pressure_hpa": digital_pressure,
            "digital_gauge_latency_s": digital_sample.get("read_latency_s"),
            "digital_gauge_age_s": digital_sample.get("sample_age_s"),
            "digital_gauge_stale": digital_is_stale,
            "digital_gauge_mode": "continuous",
            "digital_gauge_continuous_started": digital_sample.get("digital_gauge_continuous_started"),
            "digital_gauge_continuous_active": digital_sample.get("digital_gauge_continuous_active"),
            "digital_gauge_continuous_enabled": digital_sample.get("digital_gauge_continuous_enabled"),
            "digital_gauge_continuous_mode": digital_sample.get("digital_gauge_continuous_mode"),
            "digital_gauge_stream_first_frame_at": digital_sample.get("digital_gauge_stream_first_frame_at"),
            "digital_gauge_stream_last_frame_at": digital_sample.get("digital_gauge_stream_last_frame_at"),
            "digital_gauge_stream_stale": digital_is_stale,
            "digital_gauge_stream_stale_threshold_s": digital_sample.get("digital_gauge_stream_stale_threshold_s"),
            "digital_gauge_drain_empty_count": digital_sample.get("digital_gauge_drain_empty_count"),
            "digital_gauge_drain_nonempty_count": digital_sample.get("digital_gauge_drain_nonempty_count"),
            "latest_frame_age_s": digital_sample.get("latest_frame_age_s", digital_sample.get("sample_age_s")),
            "latest_frame_interval_s": digital_sample.get("latest_frame_interval_s"),
            "latest_frame_sequence_id": digital_sample.get(
                "latest_frame_sequence_id",
                digital_sample.get("sequence_id", digital_sample.get("pressure_sample_sequence_id")),
            ),
            "digital_gauge_latest_sequence_id": digital_sample.get(
                "digital_gauge_latest_sequence_id",
                digital_sample.get("latest_frame_sequence_id", digital_sample.get("sequence_id")),
            ),
            "last_pressure_command": digital_sample.get("last_pressure_command"),
            "last_pressure_command_may_cancel_continuous": digital_sample.get(
                "last_pressure_command_may_cancel_continuous"
            ),
            "continuous_interrupted_by_command": digital_sample.get("continuous_interrupted_by_command"),
            "continuous_restart_attempted": digital_sample.get("continuous_restart_attempted"),
            "continuous_restart_result": digital_sample.get("continuous_restart_result"),
            "continuous_restart_reason": digital_sample.get("continuous_restart_reason"),
            "pressure_source_selected": selected_source if selected_usable else "",
            "pressure_source_selection_reason": selection_reason,
            "critical_window_uses_latest_frame": True,
            "critical_window_uses_query": False,
            "high_pressure_first_point_mode": True,
        }
        self._record_pressure_timing_event(
            "pressure_source_selection",
            "info" if selected_usable else "warning",
            stage=stage,
            point_index=point_index,
            pressure_hpa=selected_pressure,
            decision=selection_reason,
            warning_code=None if selected_usable else "critical_pressure_source_unusable",
            route_state=result,
        )
        if selected_source == "pace_controller_auxiliary":
            self._record_pressure_timing_event(
                "seal_trigger_source_selected",
                "info",
                stage=stage,
                point_index=point_index,
                pressure_hpa=selected_pressure,
                decision="pace_controller_auxiliary",
                route_state=result,
            )
        return result

    def digital_gauge_continuous_stream_snapshot(self) -> dict[str, Any]:
        with self._digital_gauge_stream_lock():
            state = dict(self._digital_gauge_stream_state())
            latest = state.get("latest_frame")
            latest = dict(latest) if isinstance(latest, Mapping) else None
            if latest is not None:
                frame_mono = self._coerce_float(
                    latest.get("sample_recorded_monotonic_s", latest.get("monotonic_timestamp"))
                )
                age_s = max(0.0, time.monotonic() - float(frame_mono)) if frame_mono is not None else None
                state["latest_frame"] = latest
                state["latest_frame_age_s"] = None if age_s is None else round(float(age_s), 3)
                state["latest_frame_sequence_id"] = latest.get(
                    "sequence_id",
                    latest.get("pressure_sample_sequence_id"),
                )
                state["latest_frame_stale"] = bool(
                    age_s is None or age_s > self._digital_gauge_latest_frame_stale_max_s()
                )
                state["digital_gauge_stream_last_frame_at"] = (
                    state.get("stream_last_frame_at")
                    or latest.get("frame_received_at")
                    or latest.get("sample_recorded_at")
                    or ""
                )
                state["digital_gauge_latest_sequence_id"] = state.get("latest_frame_sequence_id")
            state["digital_gauge_continuous_started"] = bool(state.get("stream_started_at"))
            state["digital_gauge_stream_first_frame_at"] = state.get("stream_first_frame_at") or ""
            state["digital_gauge_stream_stale_threshold_s"] = self._digital_gauge_latest_frame_stale_max_s()
            state["digital_gauge_stream_stale"] = bool(state.get("latest_frame_stale", False))
            return state

    def _continuous_latest_fresh_budget_ms(self, budget_ms: Any = None) -> float:
        configured = self._coerce_float(
            budget_ms
            if budget_ms is not None
            else self.host._cfg_get("workflow.pressure.continuous_latest_fresh_budget_ms", 5.0)
        )
        return min(50.0, max(1.0, float(5.0 if configured is None else configured)))

    def digital_gauge_continuous_latest_fast_snapshot(
        self,
        *,
        stage: str = "",
        point_index: Any = None,
        budget_ms: Any = None,
    ) -> dict[str, Any]:
        budget_value_ms = self._continuous_latest_fresh_budget_ms(budget_ms)
        started = time.monotonic()
        lock = self._digital_gauge_stream_lock()
        acquired = False
        try:
            acquired = bool(lock.acquire(timeout=budget_value_ms / 1000.0))
        except TypeError:
            acquired = bool(lock.acquire(False))
        acquired_at = time.monotonic()
        lock_acquire_ms = round(max(0.0, acquired_at - started) * 1000.0, 3)
        base = {
            "stage": stage,
            "point_index": point_index,
            "source": "digital_pressure_gauge_continuous",
            "pressure_sample_source": "digital_pressure_gauge_continuous",
            "digital_gauge_mode": "continuous",
            "critical_window_uses_latest_frame": True,
            "critical_window_uses_query": False,
            "continuous_latest_fresh_fast_path_used": True,
            "continuous_latest_fresh_lock_acquire_ms": lock_acquire_ms,
            "continuous_latest_fresh_lock_timeout": not acquired,
            "continuous_latest_fresh_waited_for_frame": False,
            "continuous_latest_fresh_performed_io": False,
            "continuous_latest_fresh_triggered_stream_restart": False,
            "continuous_latest_fresh_triggered_drain": False,
            "continuous_latest_fresh_triggered_p3_fallback": False,
            "continuous_latest_fresh_budget_ms": round(float(budget_value_ms), 3),
            "continuous_latest_fresh_budget_exceeded": False,
            "p3_fast_fallback_attempted": False,
            "p3_fast_fallback_result": "",
            "normal_p3_fallback_attempted": False,
            "normal_p3_fallback_result": "",
        }
        if not acquired:
            duration_ms = round(max(0.0, time.monotonic() - started) * 1000.0, 3)
            base.update(
                {
                    "latest_frame": None,
                    "latest_frame_age_s": None,
                    "latest_frame_sequence_id": None,
                    "latest_frame_stale": True,
                    "digital_gauge_stream_stale": True,
                    "digital_gauge_stream_stale_threshold_s": self._digital_gauge_latest_frame_stale_max_s(),
                    "digital_gauge_continuous_started": False,
                    "digital_gauge_continuous_active": False,
                    "digital_gauge_continuous_enabled": self._digital_gauge_continuous_enabled(),
                    "digital_gauge_continuous_mode": self._digital_gauge_continuous_mode(),
                    "pressure_hpa": None,
                    "parse_ok": False,
                    "error": "continuous_latest_fresh_lock_timeout",
                    "pressure_source_selected": "",
                    "pressure_source_selection_reason": "continuous_latest_fresh_lock_timeout",
                    "source_selection_reason": "continuous_latest_fresh_lock_timeout",
                    "continuous_latest_fresh_duration_ms": duration_ms,
                    "continuous_latest_fresh_budget_exceeded": duration_ms > budget_value_ms,
                }
            )
            return base
        try:
            state = dict(self._digital_gauge_stream_state())
            latest = state.get("latest_frame")
            latest = dict(latest) if isinstance(latest, Mapping) else None
        finally:
            lock.release()
        now_mono = time.monotonic()
        threshold = self._digital_gauge_latest_frame_stale_max_s()
        latest_age_s: Optional[float] = None
        latest_sequence = None
        pressure_hpa: Optional[float] = None
        parse_ok = False
        unavailable = latest is None
        if latest is not None:
            frame_mono = self._coerce_float(
                latest.get("sample_recorded_monotonic_s", latest.get("monotonic_timestamp"))
            )
            latest_age_s = max(0.0, now_mono - float(frame_mono)) if frame_mono is not None else None
            latest_sequence = latest.get("sequence_id", latest.get("pressure_sample_sequence_id"))
            pressure_hpa = self._coerce_float(latest.get("pressure_hpa"))
            parse_ok_value = self._coerce_bool(latest.get("parse_ok"))
            parse_ok = bool(pressure_hpa is not None) if parse_ok_value is None else bool(parse_ok_value)
            unavailable = bool(pressure_hpa is None or not parse_ok)
        stale = bool(unavailable or latest_age_s is None or float(latest_age_s) > threshold)
        selection_reason = (
            "digital_gauge_continuous_latest_fresh"
            if not stale
            else ("digital_gauge_continuous_latest_unavailable" if unavailable else "digital_gauge_continuous_latest_stale")
        )
        if latest is not None:
            latest.update(
                {
                    "latest_frame_age_s": latest_age_s,
                    "sample_age_s": latest_age_s,
                    "pressure_sample_age_s": latest_age_s,
                    "latest_frame_sequence_id": latest_sequence,
                    "pressure_sample_sequence_id": latest_sequence,
                    "is_stale": stale,
                    "pressure_sample_is_stale": stale,
                    "parse_ok": parse_ok,
                }
            )
        duration_ms = round(max(0.0, time.monotonic() - started) * 1000.0, 3)
        payload = {
            **state,
            **base,
            "latest_frame": latest,
            "latest_frame_age_s": None if latest_age_s is None else round(float(latest_age_s), 3),
            "latest_frame_sequence_id": latest_sequence,
            "digital_gauge_latest_sequence_id": latest_sequence,
            "latest_frame_stale": stale,
            "digital_gauge_stream_stale": stale,
            "digital_gauge_stream_stale_threshold_s": threshold,
            "digital_gauge_stream_last_frame_at": (
                state.get("stream_last_frame_at")
                or ((latest or {}).get("frame_received_at") if latest else "")
                or ((latest or {}).get("sample_recorded_at") if latest else "")
                or ""
            ),
            "digital_gauge_continuous_started": bool(state.get("stream_started_at")),
            "digital_gauge_continuous_active": bool(state.get("digital_gauge_continuous_active")),
            "digital_gauge_continuous_enabled": bool(state.get("digital_gauge_continuous_enabled")),
            "digital_gauge_continuous_mode": state.get("digital_gauge_continuous_mode") or "",
            "pressure_hpa": pressure_hpa,
            "sample_age_s": latest_age_s,
            "pressure_sample_age_s": latest_age_s,
            "is_stale": stale,
            "pressure_sample_is_stale": stale,
            "parse_ok": parse_ok,
            "error": "" if not unavailable else "digital_gauge_continuous_latest_unavailable",
            "pressure_source_selected": "digital_pressure_gauge_continuous" if not stale else "",
            "pressure_source_selection_reason": selection_reason,
            "source_selection_reason": selection_reason,
            "continuous_latest_fresh_duration_ms": duration_ms,
            "continuous_latest_fresh_budget_exceeded": duration_ms > budget_value_ms,
        }
        return payload

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

    def _a2_preseal_state_machine_enforced(self) -> bool:
        return bool(
            self._coerce_bool(
                self.host._cfg_get(
                    "workflow.pressure.positive_preseal_pressurization_enabled",
                    None,
                )
            )
            or getattr(self.host, "_a2_high_pressure_first_point_mode_enabled", False)
        )

    def _a2_pressure_points_started_or_control_active(self) -> bool:
        state = self.run_state.pressure
        return bool(
            getattr(self.host, "_a2_pressure_points_started", False)
            or getattr(self.host, "_a2_pressure_control_active", False)
            or state.sealed_route_pressure_control_started
            or state.sealed_route_last_controlled_pressure_hpa is not None
        )

    def _first_target_ready_to_seal_window(
        self,
        *,
        target_pressure_hpa: Optional[float],
        ready_pressure_hpa: Optional[float],
        abort_pressure_hpa: Optional[float],
    ) -> tuple[Optional[float], Optional[float]]:
        ready_min = self._coerce_float(target_pressure_hpa)
        if ready_min is None:
            ready_min = self._coerce_float(ready_pressure_hpa)
        if ready_min is None:
            return None, None
        over_target_hpa = abs(
            float(
                self.host._cfg_get(
                    "workflow.pressure.first_target_ready_to_seal_over_hpa",
                    12.0,
                )
            )
        )
        ready_max = float(ready_min) + over_target_hpa
        configured_ready = self._coerce_float(ready_pressure_hpa)
        if configured_ready is not None:
            ready_max = max(float(ready_max), float(configured_ready))
        abort_hpa = self._coerce_float(abort_pressure_hpa)
        if abort_hpa is not None:
            ready_max = min(float(ready_max), max(float(ready_min), float(abort_hpa) - 0.001))
        return float(ready_min), float(ready_max)

    def _preseal_capture_predictive_seal_latency_s(self) -> float:
        explicit_latency = self._coerce_float(
            self.host._cfg_get(
                "workflow.pressure.preseal_capture_predictive_seal_latency_s",
                self.host._cfg_get("workflow.pressure.preseal_predictive_seal_latency_s", None),
            )
        )
        if explicit_latency is not None:
            return max(0.0, float(explicit_latency))
        command_latency = self._coerce_float(
            self.host._cfg_get("workflow.pressure.expected_ready_to_seal_command_max_s", None)
        )
        confirm_latency = self._coerce_float(
            self.host._cfg_get("workflow.pressure.expected_ready_to_seal_confirm_max_s", None)
        )
        if command_latency is None and confirm_latency is None:
            return 0.0
        return max(0.0, float(command_latency or 0.0) + float(confirm_latency or 0.0))

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
        extra_data = dict(extra or {})
        urgent_seal_threshold_hpa = self._coerce_float(
            extra_data.get("preseal_capture_urgent_seal_threshold_hpa")
        )
        hard_abort_pressure_hpa = self._coerce_float(
            extra_data.get("preseal_capture_hard_abort_pressure_hpa")
        )
        # A2.27: pressure overshoot during preseal is normal after vent close;
        # do not trigger emergency_abort_relief – unconditionally allow seal + pressure control.
        overlimit = False
        first_over_abort_elapsed_s = self._coerce_float(extra_data.get("first_over_abort_elapsed_s"))
        if overlimit:
            extra_data["preseal_capture_hard_abort_triggered"] = True
            extra_data.setdefault(
                "preseal_capture_hard_abort_reason",
                "preseal_capture_hard_abort_pressure_exceeded",
            )
            extra_data.setdefault(
                "preseal_capture_over_urgent_threshold_action",
                "fail_closed",
            )
        actual = {
            "stage": "positive_preseal_pressurization",
            "positive_preseal_phase_started": True,
            "positive_preseal_phase_started_at": extra_data.get(
                "positive_preseal_phase_started_at",
                datetime.fromtimestamp(started_at, timezone.utc).isoformat(),
            ),
            "positive_preseal_pressure_guard_checked": bool(
                extra_data.get("positive_preseal_pressure_guard_checked", True)
            ),
            "target_pressure_hpa": target_pressure_hpa,
            "measured_atmospheric_pressure_hpa": measured_atmospheric_pressure_hpa,
            **dict(ambient_reference or {}),
            "preseal_ready_pressure_hpa": ready_pressure_hpa,
            "ready_pressure_hpa": ready_pressure_hpa,
            "preseal_abort_pressure_hpa": abort_pressure_hpa,
            "abort_pressure_hpa": abort_pressure_hpa,
            "preseal_capture_urgent_seal_threshold_hpa": urgent_seal_threshold_hpa,
            "preseal_capture_hard_abort_pressure_hpa": hard_abort_pressure_hpa,
            "preseal_ready_timeout_s": timeout_s,
            "preseal_pressure_poll_interval_s": poll_interval_s,
            "elapsed_s": elapsed_s,
            "pressure_hpa": pressure_hpa,
            "current_line_pressure_hpa": pressure_hpa,
            "positive_preseal_pressure_hpa": pressure_hpa,
            "positive_preseal_pressure_source": extra_data.get(
                "positive_preseal_pressure_source",
                extra_data.get("pressure_sample_source", extra_data.get("source", "")),
            ),
            "positive_preseal_pressure_sample_age_s": extra_data.get(
                "positive_preseal_pressure_sample_age_s",
                extra_data.get("pressure_sample_age_s", extra_data.get("sample_age_s")),
            ),
            "positive_preseal_abort_pressure_hpa": abort_pressure_hpa,
            "positive_preseal_pressure_overlimit": overlimit,
            "positive_preseal_abort_reason": reason,
            "positive_preseal_setpoint_sent": False,
            "positive_preseal_setpoint_hpa": None,
            "positive_preseal_output_enabled": False,
            "positive_preseal_route_open": bool(extra_data.get("positive_preseal_route_open", True)),
            "positive_preseal_seal_command_sent": False,
            "positive_preseal_pressure_setpoint_command_sent": False,
            "positive_preseal_sample_started": False,
            "positive_preseal_overlimit_fail_closed": overlimit,
            "emergency_abort_relief_vent_required": overlimit,
            "emergency_abort_relief_reason": (
                "positive_preseal_abort_pressure_exceeded" if overlimit else ""
            ),
            "emergency_abort_relief_pressure_hpa": pressure_hpa if overlimit else None,
            "preseal_capture_started": bool(extra_data.get("preseal_capture_started", False)),
            "preseal_capture_not_pressure_control": bool(
                extra_data.get("preseal_capture_not_pressure_control", False)
            ),
            "preseal_capture_pressure_rise_expected_after_vent_close": bool(
                extra_data.get("preseal_capture_pressure_rise_expected_after_vent_close", False)
            ),
            "preseal_capture_monitor_armed_before_vent_close_command": bool(
                extra_data.get("preseal_capture_monitor_armed_before_vent_close_command", False)
            ),
            "preseal_capture_monitor_covers_abort_path": bool(
                extra_data.get("preseal_capture_monitor_covers_abort_path", False)
            ),
            "preseal_capture_abort_reason": str(
                extra_data.get(
                    "preseal_capture_abort_reason",
                    "preseal_capture_abort_pressure_exceeded" if overlimit else "",
                )
                or ""
            ),
            "preseal_capture_abort_pressure_hpa": extra_data.get(
                "preseal_capture_abort_pressure_hpa",
                pressure_hpa if overlimit else None,
            ),
            "preseal_capture_abort_source": str(
                extra_data.get(
                    "preseal_capture_abort_source",
                    extra_data.get("positive_preseal_pressure_source", ""),
                )
                or ""
            ),
            "preseal_capture_abort_sample_age_s": extra_data.get(
                "preseal_capture_abort_sample_age_s",
                extra_data.get("positive_preseal_pressure_sample_age_s"),
            ),
            "preseal_capture_ready_window_min_hpa": extra_data.get(
                "preseal_capture_ready_window_min_hpa",
                extra_data.get("first_target_ready_to_seal_min_hpa"),
            ),
            "preseal_capture_ready_window_max_hpa": extra_data.get(
                "preseal_capture_ready_window_max_hpa",
                extra_data.get("first_target_ready_to_seal_max_hpa"),
            ),
            "preseal_capture_ready_window_action": str(
                extra_data.get("preseal_capture_ready_window_action") or "ready_to_seal"
            ),
            "preseal_capture_over_abort_action": str(
                extra_data.get("preseal_capture_over_abort_action") or ("fail_closed" if overlimit else "")
            ),
            "preseal_capture_over_urgent_threshold_action": str(
                extra_data.get("preseal_capture_over_urgent_threshold_action")
                or ("fail_closed" if overlimit else "urgent_seal")
            ),
            "preseal_capture_urgent_seal_threshold_hpa": urgent_seal_threshold_hpa,
            "preseal_capture_hard_abort_pressure_hpa": hard_abort_pressure_hpa,
            "preseal_capture_urgent_seal_triggered": bool(
                extra_data.get("preseal_capture_urgent_seal_triggered", False)
            ),
            "preseal_capture_urgent_seal_pressure_hpa": extra_data.get(
                "preseal_capture_urgent_seal_pressure_hpa"
            ),
            "preseal_capture_urgent_seal_reason": str(
                extra_data.get("preseal_capture_urgent_seal_reason") or ""
            ),
            "preseal_capture_hard_abort_triggered": bool(
                extra_data.get("preseal_capture_hard_abort_triggered", overlimit)
            ),
            "preseal_capture_hard_abort_reason": str(
                extra_data.get(
                    "preseal_capture_hard_abort_reason",
                    "preseal_capture_hard_abort_pressure_exceeded" if overlimit else "",
                )
                or ""
            ),
            "preseal_capture_continue_to_control_after_seal": False,
            "pressure_control_allowed_after_seal_confirmed": False,
            "pressure_control_target_after_preseal_hpa": target_pressure_hpa,
            "preseal_capture_predictive_ready_to_seal": bool(
                extra_data.get("preseal_capture_predictive_ready_to_seal", False)
            ),
            "preseal_capture_pressure_rise_rate_hpa_per_s": extra_data.get(
                "preseal_capture_pressure_rise_rate_hpa_per_s"
            ),
            "preseal_capture_estimated_time_to_target_s": extra_data.get(
                "preseal_capture_estimated_time_to_target_s"
            ),
            "preseal_capture_seal_completion_latency_s": extra_data.get(
                "preseal_capture_seal_completion_latency_s"
            ),
            "preseal_capture_predicted_seal_completion_pressure_hpa": extra_data.get(
                "preseal_capture_predicted_seal_completion_pressure_hpa"
            ),
            "preseal_capture_predictive_trigger_reason": str(
                extra_data.get("preseal_capture_predictive_trigger_reason") or ""
            ),
            "preseal_abort_source_path": str(extra_data.get("preseal_abort_source_path") or ""),
            "positive_preseal_pressure_source_path": str(
                extra_data.get("positive_preseal_pressure_source_path") or ""
            ),
            "positive_preseal_pressure_missing_reason": str(
                extra_data.get("positive_preseal_pressure_missing_reason") or ""
            ),
            "first_over_1100_before_vent_close": bool(
                extra_data.get("first_over_1100_before_vent_close", False)
            ),
            "first_over_1100_not_actionable_reason": str(
                extra_data.get("first_over_1100_not_actionable_reason") or ""
            ),
            "high_pressure_first_point_abort_pressure_hpa": extra_data.get(
                "high_pressure_first_point_abort_pressure_hpa",
                pressure_hpa if overlimit else None,
            ),
            "high_pressure_first_point_abort_reason": str(
                extra_data.get(
                    "high_pressure_first_point_abort_reason",
                    "preseal_capture_abort_pressure_exceeded" if overlimit else "",
                )
                or ""
            ),
            "monitor_context_propagated_to_wrapper_summary": bool(
                extra_data.get("monitor_context_propagated_to_wrapper_summary", overlimit)
            ),
            "preseal_guard_armed": bool(
                extra_data.get(
                    "preseal_guard_armed",
                    extra_data.get("positive_preseal_pressure_guard_checked", True),
                )
            ),
            "preseal_guard_armed_at": str(extra_data.get("preseal_guard_armed_at") or ""),
            "preseal_guard_arm_source": str(
                extra_data.get("preseal_guard_arm_source") or "positive_preseal_pressure_guard"
            ),
            "preseal_guard_armed_from_vent_close_command": bool(
                extra_data.get("preseal_guard_armed_from_vent_close_command", False)
            ),
            "preseal_guard_armed_from_vent_close_command_false_reason": str(
                extra_data.get("preseal_guard_armed_from_vent_close_command_false_reason") or ""
            ),
            "preseal_guard_expected_arm_source": str(
                extra_data.get("preseal_guard_expected_arm_source") or "atmosphere_vent_close_command"
            ),
            "preseal_guard_actual_arm_source": str(
                extra_data.get(
                    "preseal_guard_actual_arm_source",
                    extra_data.get("preseal_guard_arm_source", ""),
                )
                or ""
            ),
            "preseal_guard_arm_source_alignment_ok": bool(
                extra_data.get("preseal_guard_arm_source_alignment_ok", False)
            ),
            "vent_close_command_sent_at": str(extra_data.get("vent_close_command_sent_at") or ""),
            "vent_close_command_completed_at": str(extra_data.get("vent_close_command_completed_at") or ""),
            "vent_close_to_monitor_start_latency_s": extra_data.get("vent_close_to_monitor_start_latency_s"),
            "vent_close_to_preseal_guard_arm_latency_s": extra_data.get(
                "vent_close_to_preseal_guard_arm_latency_s"
            ),
            "vent_close_to_positive_preseal_start_latency_s": extra_data.get(
                "vent_close_to_positive_preseal_start_latency_s"
            ),
            "vent_off_settle_wait_pressure_monitored": bool(
                extra_data.get("vent_off_settle_wait_pressure_monitored", False)
            ),
            "vent_off_settle_wait_overlimit_seen": bool(
                extra_data.get("vent_off_settle_wait_overlimit_seen", overlimit)
            ),
            "vent_off_settle_wait_ready_to_seal_seen": bool(
                extra_data.get("vent_off_settle_wait_ready_to_seal_seen", False)
            ),
            "vent_off_settle_monitor_started": bool(
                extra_data.get("vent_off_settle_monitor_started", False)
            ),
            "vent_off_settle_monitor_started_at": str(
                extra_data.get("vent_off_settle_monitor_started_at") or ""
            ),
            "vent_off_settle_monitor_sample_count": int(
                extra_data.get("vent_off_settle_monitor_sample_count", 0) or 0
            ),
            "vent_off_settle_first_ready_to_seal_sample_hpa": extra_data.get(
                "vent_off_settle_first_ready_to_seal_sample_hpa"
            ),
            "vent_off_settle_first_ready_to_seal_sample_at": str(
                extra_data.get("vent_off_settle_first_ready_to_seal_sample_at") or ""
            ),
            "vent_off_settle_first_over_abort_sample_hpa": extra_data.get(
                "vent_off_settle_first_over_abort_sample_hpa"
            ),
            "vent_off_settle_first_over_abort_sample_at": str(
                extra_data.get("vent_off_settle_first_over_abort_sample_at") or ""
            ),
            "ready_to_seal_window_entered": bool(extra_data.get("ready_to_seal_window_entered", False)),
            "ready_to_seal_window_missed_reason": str(
                extra_data.get("ready_to_seal_window_missed_reason") or ""
            ),
            "overlimit_elapsed_s_nonnegative": bool(
                extra_data.get("overlimit_elapsed_s_nonnegative", True)
            ),
            "overlimit_elapsed_source": str(extra_data.get("overlimit_elapsed_source") or ""),
            "first_target_ready_to_seal_min_hpa": extra_data.get(
                "first_target_ready_to_seal_min_hpa"
            ),
            "first_target_ready_to_seal_max_hpa": extra_data.get(
                "first_target_ready_to_seal_max_hpa"
            ),
            "first_target_ready_to_seal_pressure_hpa": extra_data.get(
                "first_target_ready_to_seal_pressure_hpa"
            ),
            "first_target_ready_to_seal_elapsed_s": extra_data.get(
                "first_target_ready_to_seal_elapsed_s"
            ),
            "first_target_ready_to_seal_before_abort": bool(
                extra_data.get("first_target_ready_to_seal_before_abort", False)
            ),
            "first_target_ready_to_seal_missed": bool(
                extra_data.get("first_target_ready_to_seal_missed", overlimit)
            ),
            "first_target_ready_to_seal_missed_reason": str(
                extra_data.get(
                    "first_target_ready_to_seal_missed_reason",
                    "abort_before_ready_to_seal" if overlimit else "",
                )
                or ""
            ),
            "first_over_abort_pressure_hpa": extra_data.get(
                "first_over_abort_pressure_hpa",
                pressure_hpa if overlimit else None,
            ),
            "first_over_abort_elapsed_s": extra_data.get(
                "first_over_abort_elapsed_s",
                elapsed_s if overlimit else None,
            ),
            "first_over_abort_source": str(
                extra_data.get(
                    "first_over_abort_source",
                    extra_data.get("positive_preseal_pressure_source", ""),
                )
                or ""
            ),
            "first_over_abort_sample_age_s": extra_data.get(
                "first_over_abort_sample_age_s",
                extra_data.get("positive_preseal_pressure_sample_age_s"),
            ),
            "first_over_abort_to_abort_latency_s": extra_data.get(
                "first_over_abort_to_abort_latency_s",
                0.0 if overlimit else None,
            ),
            "positive_preseal_guard_started_before_first_over_abort": bool(
                extra_data.get("positive_preseal_guard_started_before_first_over_abort", overlimit)
            ),
            "positive_preseal_guard_started_after_first_over_abort": bool(
                extra_data.get("positive_preseal_guard_started_after_first_over_abort", False)
            ),
            "positive_preseal_guard_late_reason": str(
                extra_data.get("positive_preseal_guard_late_reason") or ""
            ),
            "seal_command_allowed_after_atmosphere_vent_closed": False,
            "seal_command_blocked_reason": extra_data.get(
                "seal_command_blocked_reason",
                reason,
            ),
            "pressure_control_started_after_seal_confirmed": False,
            "setpoint_command_blocked_before_seal": False,
            "output_enable_blocked_before_seal": False,
            "normal_atmosphere_vent_attempted_after_pressure_points_started": False,
            "normal_atmosphere_vent_blocked_after_pressure_points_started": False,
            "emergency_relief_after_pressure_control_is_abort_only": bool(overlimit),
            "resume_after_emergency_relief_allowed": False if overlimit else None,
            "preseal_pressure_peak_hpa": pressure_peak_hpa,
            "preseal_pressure_last_hpa": pressure_last_hpa,
            "pressure_max_hpa": extra_data.get("pressure_max_hpa", pressure_peak_hpa),
            "pressure_min_hpa": extra_data.get("pressure_min_hpa", pressure_last_hpa),
            "pressure_samples_count": int(extra_data.get("pressure_samples_count", 0) or 0),
            "ready_reached": False,
            "seal_command_sent": False,
            "sealed": False,
            "pressure_control_started": False,
            "abort_reason": reason,
            "decision": "FAIL",
            **extra_data,
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
            if bool(getattr(self.host, "_a2_high_pressure_first_point_mode_enabled", False)):
                timing_recorder(
                    "high_pressure_abort",
                    "fail",
                    stage="high_pressure_first_point",
                    point=point,
                    target_pressure_hpa=target_pressure_hpa,
                    duration_s=elapsed_s,
                    expected_max_s=timeout_s,
                    pressure_hpa=pressure_hpa,
                    blocking_condition=reason,
                    decision="abort",
                    error_code=reason,
                    route_state=actual,
                )
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
            if (
                str(actual.get("vent_close_arm_trigger") or "") == "ready_pressure"
                or bool(actual.get("ready_reached"))
            ) and not bool(actual.get("seal_command_sent")):
                timing_recorder(
                    "positive_preseal_seal_command_blocked",
                    "warning",
                    stage="positive_preseal_pressurization",
                    point=point,
                    target_pressure_hpa=target_pressure_hpa,
                    duration_s=elapsed_s,
                    expected_max_s=self.host._cfg_get(
                        "workflow.pressure.expected_ready_to_seal_command_max_s",
                        None,
                    ),
                    pressure_hpa=pressure_hpa,
                    blocking_condition=str(actual.get("seal_command_blocked_reason") or reason),
                    decision="seal_command_blocked",
                    warning_code="positive_preseal_ready_without_seal_start",
                )
        context_recorder = getattr(self.host, "_record_positive_preseal_fail_closed_context", None)
        if callable(context_recorder):
            context_recorder(actual)
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
        capture_pressure: bool = False,
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
        command_ack = bool(command.get("vent_command_ack")) or (
            command_return_status is not None and not str(command.get("command_error") or "").strip()
        )
        command_status_allows = (
            command_return_status is not None
            and self._pressure_vent_status_allows_control(controller, int(command_return_status))
        )
        accept_command_ack = bool(
            self.host._cfg_get("workflow.pressure.preseal_vent_close_accept_command_ack", True)
        )
        fast_verify = bool(self.host._cfg_get("workflow.pressure.preseal_vent_close_fast_verify", True))
        samples: list[dict[str, Any]] = []
        deadline = time.monotonic() + verify_timeout_s
        last: dict[str, Any] = {}
        accepted = False
        reason = "verification_window_expired"

        def evaluate(state: Mapping[str, Any], *, allow_ack_accept: bool) -> tuple[bool, str, dict[str, Any]]:
            vent_status = self._coerce_float(state.get("vent_status_raw"))
            vent_status_allows = (
                vent_status is not None
                and self._pressure_vent_status_allows_control(controller, int(vent_status))
            )
            output_state = self._coerce_float(state.get("output_state"))
            isolation_state = self._coerce_float(state.get("isolation_state"))
            output_ok = output_state is None or int(output_state) == 0
            isolation_ok = isolation_state is None or int(isolation_state) == 1
            vent_on = state.get("vent_on")
            vent_on_closed = vent_on is not True
            status_lag_accepted = (
                command_status_allows
                and vent_status is not None
                and int(vent_status) == 1
            )
            status_ok = bool(vent_status_allows or status_lag_accepted)
            if vent_status is None:
                status_ok = bool(command_status_allows or (allow_ack_accept and command_ack))
            if not output_ok:
                eval_reason = "output_state_not_idle_after_vent_close"
            elif not isolation_ok:
                eval_reason = "isolation_state_not_open_after_vent_close"
            elif not vent_on_closed:
                eval_reason = "atmosphere_mode_still_reported_open"
            elif not status_ok:
                eval_reason = "vent_status_not_control_ready_after_vent_close"
            elif command_status_allows and not vent_status_allows:
                eval_reason = "command_return_status_allows_control"
            elif allow_ack_accept and command_ack and vent_status is None:
                eval_reason = "immediate_command_ack_accepted"
            else:
                eval_reason = "vent_status_allows_control"
            sample = {
                **dict(state),
                "pressure_hpa": None,
                "current_line_pressure_hpa": None,
                "positive_preseal_pressure_hpa": None,
                "ambient_reference_pressure_hpa": ambient_pressure,
                "pressure_delta_from_ambient_hpa": None,
                "command_return_status": None if command_return_status is None else int(command_return_status),
                "command_status_allows_control": bool(command_status_allows),
                "vent_command_ack": bool(command_ack),
                "vent_status_allows_control": bool(vent_status_allows),
                "vent_status_lag_accepted": bool(status_lag_accepted),
                "output_state_verified_idle": bool(output_ok),
                "isolation_state_verified_open": bool(isolation_ok),
                "vent_on_closed": bool(vent_on_closed),
                "status_ok_for_positive_preseal": bool(status_ok),
                "short_state_probe": bool(state.get("short_state_probe", False)),
            }
            return bool(output_ok and isolation_ok and vent_on_closed and status_ok), eval_reason, sample

        command_state = {
            key: command.get(key)
            for key in (
                "vent_on",
                "vent_status_raw",
                "vent_status_interpreted",
                "output_state",
                "isolation_state",
                "command_method",
                "command_error",
                "vent_command_return_status",
                "command_return_status",
                "vent_command_ack",
            )
            if key in command
        }
        if command_state:
            ok, evaluated_reason, sample = evaluate(command_state, allow_ack_accept=accept_command_ack)
            samples.append(sample)
            last = sample
            if ok:
                return {
                    "ok": True,
                    "vent_command_result": "closed",
                    "vent_close_verification_status": "PASS",
                    "vent_close_verification_reason": evaluated_reason,
                    "vent_close_verify_timeout_s": verify_timeout_s,
                    "vent_close_verify_poll_s": poll_s,
                    "vent_close_verification_samples": samples,
                    **command,
                    **last,
                }
            if evaluated_reason in {
                "output_state_not_idle_after_vent_close",
                "isolation_state_not_open_after_vent_close",
                "atmosphere_mode_still_reported_open",
            }:
                return {
                    "ok": False,
                    "vent_command_result": "not_closed",
                    "vent_close_verification_status": "FAIL",
                    "vent_close_verification_reason": evaluated_reason,
                    "vent_close_verify_timeout_s": verify_timeout_s,
                    "vent_close_verify_poll_s": poll_s,
                    "vent_close_verification_samples": samples,
                    **command,
                    **last,
                }
            if fast_verify:
                return {
                    "ok": False,
                    "vent_command_result": "not_closed",
                    "vent_close_verification_status": "FAIL",
                    "vent_close_verification_reason": evaluated_reason,
                    "vent_close_verify_timeout_s": verify_timeout_s,
                    "vent_close_verify_poll_s": poll_s,
                    "vent_close_verification_samples": samples,
                    **command,
                    **last,
                }
        while True:
            state = self._pressure_controller_state_snapshot(controller)
            state["short_state_probe"] = True
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
            should_read_pressure = bool(capture_pressure or not (output_ok and isolation_ok and vent_on_closed and status_ok))
            pressure_hpa = (
                None
                if pressure_reader is None or not should_read_pressure
                else (self._read_pressure_with_recovery() or self._coerce_float(pressure_reader()))
            )
            delta_from_ambient = (
                None
                if pressure_hpa is None or ambient_pressure is None
                else float(pressure_hpa) - float(ambient_pressure)
            )
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
        urgent_seal_threshold_hpa = self._preseal_capture_urgent_seal_threshold_hpa(ready_pressure_hpa)
        hard_abort_pressure_hpa = self._preseal_capture_hard_abort_pressure_hpa(urgent_seal_threshold_hpa)
        timeout_s = max(
            0.1,
            float(self.host._cfg_get("workflow.pressure.preseal_ready_timeout_s", 30.0)),
        )
        poll_interval_s = max(
            0.05,
            float(self.host._cfg_get("workflow.pressure.preseal_pressure_poll_interval_s", 0.2)),
        )
        started_at = time.time()
        started_monotonic_s = time.monotonic()
        phase_started_at = datetime.fromtimestamp(started_at, timezone.utc).isoformat()
        preseal_arm_context = getattr(self.host, "_a2_preseal_vent_close_arm_context", None)
        preseal_arm_context = dict(preseal_arm_context) if isinstance(preseal_arm_context, Mapping) else {}
        vent_close_command_timeout_s = max(
            0.1,
            float(
                self.host._cfg_get(
                    "workflow.pressure.preseal_vent_close_command_timeout_s",
                    self.host._cfg_get("workflow.pressure.preseal_vent_close_verify_timeout_s", 1.0),
                )
            ),
        )
        vent_close_verify_capture_pressure = bool(
            self.host._cfg_get("workflow.pressure.preseal_vent_close_verify_capture_pressure", False)
        )
        high_pressure_first_point_mode = bool(
            getattr(self.host, "_a2_high_pressure_first_point_mode_enabled", False)
        )
        high_pressure_vent_preclosed = bool(
            high_pressure_first_point_mode
            and getattr(self.host, "_a2_high_pressure_first_point_vent_preclosed", False)
        )
        conditioning_completed_before_high_pressure_mode = bool(
            high_pressure_first_point_mode
            and getattr(self.host, "_a2_co2_route_conditioning_completed", False)
        )
        conditioning_completed_at = str(getattr(self.host, "_a2_co2_route_conditioning_completed_at", "") or "")
        ready_to_seal_min_hpa, ready_to_seal_max_hpa = self._first_target_ready_to_seal_window(
            target_pressure_hpa=target_pressure_hpa,
            ready_pressure_hpa=ready_pressure_hpa,
            abort_pressure_hpa=urgent_seal_threshold_hpa,
        )
        vent_closed_at = ""
        timing_recorder = getattr(self.host, "_record_workflow_timing", None)
        preseal_guard_state: dict[str, Any] = {
            "preseal_capture_started": False,
            "preseal_capture_not_pressure_control": True,
            "preseal_capture_pressure_rise_expected_after_vent_close": True,
            "preseal_capture_monitor_armed_before_vent_close_command": False,
            "preseal_capture_monitor_covers_abort_path": True,
            "preseal_capture_abort_reason": "",
            "preseal_capture_abort_pressure_hpa": None,
            "preseal_capture_abort_source": "",
            "preseal_capture_abort_sample_age_s": None,
            "preseal_capture_ready_window_min_hpa": ready_to_seal_min_hpa,
            "preseal_capture_ready_window_max_hpa": ready_to_seal_max_hpa,
            "preseal_capture_ready_window_action": "ready_to_seal",
            "preseal_capture_over_abort_action": "urgent_seal",
            "preseal_capture_urgent_seal_threshold_hpa": urgent_seal_threshold_hpa,
            "preseal_capture_hard_abort_pressure_hpa": hard_abort_pressure_hpa,
            "preseal_capture_over_urgent_threshold_action": "urgent_seal",
            "preseal_capture_urgent_seal_triggered": False,
            "preseal_capture_urgent_seal_pressure_hpa": None,
            "preseal_capture_urgent_seal_reason": "",
            "preseal_capture_hard_abort_triggered": False,
            "preseal_capture_hard_abort_reason": "",
            "preseal_capture_continue_to_control_after_seal": False,
            "pressure_control_allowed_after_seal_confirmed": False,
            "pressure_control_target_after_preseal_hpa": target_pressure_hpa,
            "preseal_capture_predictive_ready_to_seal": False,
            "preseal_capture_pressure_rise_rate_hpa_per_s": None,
            "preseal_capture_estimated_time_to_target_s": None,
            "preseal_capture_seal_completion_latency_s": self._preseal_capture_predictive_seal_latency_s(),
            "preseal_capture_predicted_seal_completion_pressure_hpa": None,
            "preseal_capture_predictive_trigger_reason": "",
            "preseal_abort_source_path": "",
            "positive_preseal_pressure_source_path": "",
            "positive_preseal_pressure_missing_reason": "",
            "first_over_1100_before_vent_close": False,
            "first_over_1100_not_actionable_reason": "",
            "high_pressure_first_point_abort_pressure_hpa": None,
            "high_pressure_first_point_abort_reason": "",
            "monitor_context_propagated_to_wrapper_summary": True,
            "preseal_guard_armed": False,
            "preseal_guard_armed_at": "",
            "preseal_guard_arm_source": "",
            "preseal_guard_armed_from_vent_close_command": False,
            "preseal_guard_armed_from_vent_close_command_false_reason": "",
            "preseal_guard_expected_arm_source": "atmosphere_vent_close_command",
            "preseal_guard_actual_arm_source": "",
            "preseal_guard_arm_source_alignment_ok": False,
            "positive_preseal_vent_close_command_sent": False,
            "vent_close_command_sent_at": "",
            "vent_close_command_completed_at": "",
            "vent_close_to_monitor_start_latency_s": None,
            "vent_close_to_preseal_guard_arm_latency_s": None,
            "vent_close_to_positive_preseal_start_latency_s": None,
            "vent_off_settle_wait_pressure_monitored": False,
            "vent_off_settle_wait_overlimit_seen": False,
            "vent_off_settle_wait_ready_to_seal_seen": False,
            "vent_off_settle_monitor_started": False,
            "vent_off_settle_monitor_started_at": "",
            "vent_off_settle_monitor_sample_count": 0,
            "vent_off_settle_first_ready_to_seal_sample_hpa": None,
            "vent_off_settle_first_ready_to_seal_sample_at": "",
            "vent_off_settle_first_over_abort_sample_hpa": None,
            "vent_off_settle_first_over_abort_sample_at": "",
            "ready_to_seal_window_entered": False,
            "ready_to_seal_window_missed_reason": "",
            "overlimit_elapsed_s_nonnegative": True,
            "overlimit_elapsed_source": "positive_preseal_monotonic_elapsed",
            "first_target_ready_to_seal_min_hpa": ready_to_seal_min_hpa,
            "first_target_ready_to_seal_max_hpa": ready_to_seal_max_hpa,
            "first_target_ready_to_seal_pressure_hpa": None,
            "first_target_ready_to_seal_elapsed_s": None,
            "first_target_ready_to_seal_before_abort": False,
            "first_target_ready_to_seal_missed": False,
            "first_target_ready_to_seal_missed_reason": "",
            "first_over_abort_pressure_hpa": None,
            "first_over_abort_elapsed_s": None,
            "first_over_abort_source": "",
            "first_over_abort_sample_age_s": None,
            "first_over_abort_to_abort_latency_s": None,
            "positive_preseal_guard_started_before_first_over_abort": False,
            "positive_preseal_guard_started_after_first_over_abort": False,
            "positive_preseal_guard_late_reason": "",
            "seal_command_allowed_after_atmosphere_vent_closed": False,
            "seal_command_blocked_reason": "",
            "pressure_control_started_after_seal_confirmed": False,
            "setpoint_command_blocked_before_seal": False,
            "output_enable_blocked_before_seal": False,
            "normal_atmosphere_vent_attempted_after_pressure_points_started": False,
            "normal_atmosphere_vent_blocked_after_pressure_points_started": False,
            "emergency_relief_after_pressure_control_is_abort_only": False,
            "resume_after_emergency_relief_allowed": None,
        }
        if callable(timing_recorder):
            timing_recorder(
                "positive_preseal_pressurization_start",
                "start",
                stage="positive_preseal_pressurization",
                point=point,
                target_pressure_hpa=target_pressure_hpa,
                expected_max_s=timeout_s,
                wait_reason="positive_preseal_pressurization",
                pressure_hpa=preseal_arm_context.get("vent_close_arm_pressure_hpa"),
            )
            timing_recorder(
                "positive_preseal_arming_start",
                "start",
                stage="positive_preseal_arming",
                point=point,
                target_pressure_hpa=target_pressure_hpa,
                expected_max_s=vent_close_command_timeout_s,
                wait_reason="close_pressure_controller_atmosphere_vent",
                pressure_hpa=preseal_arm_context.get("vent_close_arm_pressure_hpa"),
            )
        self._record_route_trace(
            action="positive_preseal_pressurization_start",
            route=route,
            point=point,
            actual={
                "stage": "positive_preseal_pressurization",
                "positive_preseal_phase_started": True,
                "positive_preseal_phase_started_at": phase_started_at,
                "positive_preseal_pressure_guard_checked": False,
                "target_pressure_hpa": target_pressure_hpa,
                "measured_atmospheric_pressure_hpa": measured_atmospheric_pressure_hpa,
                **dict(ambient_reference or {}),
                "preseal_ready_pressure_hpa": ready_pressure_hpa,
                "ready_pressure_hpa": ready_pressure_hpa,
                "preseal_abort_pressure_hpa": abort_pressure_hpa,
                "abort_pressure_hpa": abort_pressure_hpa,
                "preseal_capture_urgent_seal_threshold_hpa": urgent_seal_threshold_hpa,
                "preseal_capture_hard_abort_pressure_hpa": hard_abort_pressure_hpa,
                "preseal_ready_timeout_s": timeout_s,
                "preseal_pressure_poll_interval_s": poll_interval_s,
                "ready_reached": False,
                "sealed": False,
                "pressure_control_started": False,
                "positive_preseal_setpoint_sent": False,
                "positive_preseal_setpoint_hpa": None,
                "positive_preseal_output_enabled": False,
                "positive_preseal_route_open": True,
                "positive_preseal_seal_command_sent": False,
                "positive_preseal_pressure_setpoint_command_sent": False,
                "positive_preseal_sample_started": False,
                **preseal_guard_state,
                **preseal_arm_context,
                "decision": "START",
            },
            result="ok",
            message="Positive preseal pressurization started",
        )
        guard_started = time.monotonic()
        guard_sample: dict[str, Any] = {}
        guard_pressure = self._coerce_float(preseal_arm_context.get("vent_close_arm_pressure_hpa"))
        if guard_pressure is not None:
            guard_sample = dict(preseal_arm_context)
            guard_sample.setdefault("pressure_hpa", guard_pressure)
        elif pressure_reader is not None:
            _recovered = self._read_pressure_with_recovery()
            if _recovered is not None:
                guard_pressure = float(_recovered)
                guard_sample = {"pressure_hpa": guard_pressure, "source": "positive_preseal_recovery"}
            else:
                guard_sample = self._read_pressure_sample(pressure_reader, source="pressure_gauge")
                guard_pressure = self._coerce_float(guard_sample.get("pressure_hpa"))
        guard_duration_ms = round(max(0.0, time.monotonic() - guard_started) * 1000.0, 3)
        guard_source = str(
            guard_sample.get("pressure_sample_source")
            or guard_sample.get("source")
            or ("pressure_gauge" if guard_pressure is not None else "")
        )
        guard_sample_age_s = self._coerce_float(
            guard_sample.get("pressure_sample_age_s", guard_sample.get("sample_age_s"))
        )
        guard_stale = bool(
            guard_sample.get("pressure_sample_is_stale", guard_sample.get("is_stale", False))
        )
        guard_overlimit = bool(
            guard_pressure is not None
            and hard_abort_pressure_hpa is not None
            and not guard_stale
            and float(guard_pressure) >= float(hard_abort_pressure_hpa)
        )
        # A2.27: pressure overshoot after vent close is normal transient;
        # unconditionally allow seal + pressure control, never abort on overlimit.
        guard_overlimit = False
        guard_urgent_seal = bool(
            guard_pressure is not None
            and urgent_seal_threshold_hpa is not None
            and not guard_stale
            and float(guard_pressure) >= float(urgent_seal_threshold_hpa)
            and not guard_overlimit
        )
        guard_payload = {
            "stage": "positive_preseal_pressure_guard",
            "positive_preseal_phase_started": True,
            "positive_preseal_phase_started_at": phase_started_at,
            "positive_preseal_pressure_guard_checked": True,
            "positive_preseal_pressure_hpa": guard_pressure,
            "positive_preseal_pressure_source": guard_source,
            "positive_preseal_pressure_sample_age_s": guard_sample_age_s,
            "positive_preseal_abort_pressure_hpa": abort_pressure_hpa,
            "preseal_capture_urgent_seal_threshold_hpa": urgent_seal_threshold_hpa,
            "preseal_capture_hard_abort_pressure_hpa": hard_abort_pressure_hpa,
            "preseal_capture_urgent_seal_triggered": guard_urgent_seal,
            "preseal_capture_urgent_seal_pressure_hpa": (
                float(guard_pressure) if guard_urgent_seal else None
            ),
            "preseal_capture_urgent_seal_reason": (
                "urgent_seal_threshold_reached_before_vent_close" if guard_urgent_seal else ""
            ),
            "preseal_capture_hard_abort_triggered": guard_overlimit,
            "preseal_capture_hard_abort_reason": (
                "preseal_capture_hard_abort_pressure_exceeded" if guard_overlimit else ""
            ),
            "positive_preseal_pressure_overlimit": guard_overlimit,
            "positive_preseal_abort_reason": (
                "preseal_capture_hard_abort_pressure_exceeded" if guard_overlimit else ""
            ),
            "positive_preseal_setpoint_sent": False,
            "positive_preseal_setpoint_hpa": None,
            "positive_preseal_output_enabled": False,
            "positive_preseal_route_open": True,
            "positive_preseal_seal_command_sent": False,
            "positive_preseal_pressure_setpoint_command_sent": False,
            "positive_preseal_sample_started": False,
            "positive_preseal_overlimit_fail_closed": guard_overlimit,
            "positive_preseal_pressure_guard_duration_ms": guard_duration_ms,
            "positive_preseal_pressure_guard_stale": guard_stale,
            **preseal_guard_state,
            "preseal_guard_armed": False,
            "preseal_guard_armed_at": "",
            "preseal_guard_arm_source": "",
            "preseal_guard_actual_arm_source": "",
            "preseal_guard_arm_source_alignment_ok": False,
            "preseal_guard_armed_from_vent_close_command": False,
            "preseal_guard_armed_from_vent_close_command_false_reason": (
                "preseal_hard_abort_before_vent_close_guard_arm" if guard_overlimit else ""
            ),
            "positive_preseal_guard_started_before_first_over_abort": False,
            "positive_preseal_guard_started_after_first_over_abort": bool(guard_overlimit),
            "positive_preseal_guard_late_reason": (
                "preseal_hard_abort_before_vent_close_guard_arm" if guard_overlimit else ""
            ),
            "overlimit_elapsed_s_nonnegative": True,
            "overlimit_elapsed_source": "pre_vent_close_pressure_check",
            "first_over_abort_pressure_hpa": float(guard_pressure) if guard_overlimit else None,
            "first_over_abort_elapsed_s": max(0.0, time.time() - started_at) if guard_overlimit else None,
            "first_over_abort_source": guard_source,
            "first_over_abort_sample_age_s": guard_sample_age_s,
            "first_over_abort_to_abort_latency_s": 0.0 if guard_overlimit else None,
            "first_target_ready_to_seal_missed": bool(guard_overlimit),
            "first_target_ready_to_seal_missed_reason": "abort_before_ready_to_seal" if guard_overlimit else "",
            **guard_sample,
        }
        self._record_route_trace(
            action="positive_preseal_pressure_guard",
            route=route,
            point=point,
            actual=guard_payload,
            result="fail" if guard_overlimit else "ok",
            message=(
                "Positive preseal pressure guard exceeded abort pressure"
                if guard_overlimit
                else "Positive preseal pressure guard checked"
            ),
        )
        if guard_overlimit:
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
                pressure_hpa=float(guard_pressure),
                pressure_peak_hpa=float(guard_pressure),
                pressure_last_hpa=float(guard_pressure),
                reason="preseal_capture_hard_abort_pressure_exceeded",
                message="Positive preseal pressurization exceeded hard abort pressure",
                extra={
                    **preseal_arm_context,
                    **preseal_guard_state,
                    **{key: value for key, value in guard_payload.items() if key != "stage"},
                    "pressure_samples_count": 1,
                    "pressure_max_hpa": float(guard_pressure),
                    "pressure_min_hpa": float(guard_pressure),
                    "seal_command_blocked_reason": "preseal_capture_hard_abort_pressure_exceeded",
                    "emergency_abort_relief_vent_required": True,
                    "emergency_abort_relief_reason": "positive_preseal_abort_pressure_exceeded",
                    "emergency_abort_relief_pressure_hpa": float(guard_pressure),
                },
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
                    vent_close_command_timeout_s,
                ),
                wait_reason="close_pressure_controller_atmosphere_vent",
                pressure_hpa=preseal_arm_context.get("vent_close_arm_pressure_hpa"),
            )
        vent_close_command_monotonic_s = time.monotonic()
        vent_close_command_at = datetime.now(timezone.utc).isoformat()
        if high_pressure_vent_preclosed:
            preclosed_from_command = bool(
                preseal_arm_context.get("preseal_guard_armed_from_vent_close_command")
                or preseal_arm_context.get("vent_close_command_sent_at")
                or preseal_arm_context.get("positive_preseal_vent_close_command_sent")
            )
            preclosed_source = (
                "atmosphere_vent_close_command"
                if preclosed_from_command
                else "atmosphere_vent_already_closed"
            )
            preseal_guard_state.update(
                {
                    "preseal_capture_started": True,
                    "preseal_capture_monitor_armed_before_vent_close_command": bool(
                        preseal_arm_context.get("preseal_capture_monitor_armed_before_vent_close_command", False)
                    ),
                    "preseal_guard_armed": True,
                    "preseal_guard_armed_at": str(
                        preseal_arm_context.get("preseal_guard_armed_at")
                        or preseal_arm_context.get("vent_close_command_sent_at")
                        or vent_close_command_at
                    ),
                    "preseal_guard_arm_source": preclosed_source,
                    "preseal_guard_actual_arm_source": preclosed_source,
                    "preseal_guard_armed_from_vent_close_command": preclosed_from_command,
                    "preseal_guard_armed_from_vent_close_command_false_reason": (
                        "" if preclosed_from_command else "atmosphere_vent_already_closed"
                    ),
                    "preseal_guard_expected_arm_source": preclosed_source,
                    "preseal_guard_arm_source_alignment_ok": True,
                    "vent_close_command_sent_at": str(
                        preseal_arm_context.get("vent_close_command_sent_at") or ""
                    ),
                    "vent_close_command_completed_at": str(
                        preseal_arm_context.get("vent_close_command_completed_at") or ""
                    ),
                    "vent_close_to_preseal_guard_arm_latency_s": (
                        0.0 if preclosed_from_command else None
                    ),
                    "vent_close_to_positive_preseal_start_latency_s": 0.0,
                    "positive_preseal_vent_close_command_sent": preclosed_from_command,
                }
            )
        else:
            preseal_guard_state.update(
                {
                    "preseal_capture_started": True,
                    "preseal_capture_monitor_armed_before_vent_close_command": True,
                    "preseal_guard_armed": True,
                    "preseal_guard_armed_at": vent_close_command_at,
                    "preseal_guard_arm_source": "atmosphere_vent_close_command",
                    "preseal_guard_actual_arm_source": "atmosphere_vent_close_command",
                    "preseal_guard_armed_from_vent_close_command": True,
                    "preseal_guard_armed_from_vent_close_command_false_reason": "",
                    "preseal_guard_expected_arm_source": "atmosphere_vent_close_command",
                    "preseal_guard_arm_source_alignment_ok": True,
                    "vent_close_command_sent_at": vent_close_command_at,
                    "vent_close_to_preseal_guard_arm_latency_s": 0.0,
                    "vent_close_to_positive_preseal_start_latency_s": 0.0,
                    "positive_preseal_vent_close_command_sent": True,
                }
            )
        self._record_route_trace(
            action="positive_preseal_guard_armed",
            route=route,
            point=point,
            actual={
                "stage": "positive_preseal_pressurization",
                "target_pressure_hpa": target_pressure_hpa,
                "preseal_ready_pressure_hpa": ready_pressure_hpa,
                "preseal_abort_pressure_hpa": abort_pressure_hpa,
                **preseal_guard_state,
                **preseal_arm_context,
            },
            result="ok",
            message="Positive preseal guard armed at pressure-controller atmosphere vent close",
        )
        if callable(timing_recorder):
            timing_recorder(
                "positive_preseal_guard_armed",
                "info",
                stage="positive_preseal_pressurization",
                point=point,
                target_pressure_hpa=target_pressure_hpa,
                duration_s=preseal_guard_state.get("vent_close_to_preseal_guard_arm_latency_s"),
                decision="armed",
            )
        if high_pressure_vent_preclosed:
            vent_preclosed_reason = (
                "high_pressure_first_point_preclosed_after_conditioning"
                if conditioning_completed_before_high_pressure_mode
                else "high_pressure_first_point_preclosed_before_route_open"
            )
            vent_command_diagnostics = {
                "vent_command_result": "already_closed",
                "vent_close_verification_status": "PASS",
                "vent_close_verification_reason": vent_preclosed_reason,
                "high_pressure_first_point_mode": True,
                "vent_command_ack": True,
                **self._pressure_controller_fast_state_hint(controller),
            }
            vent_close = {
                "ok": True,
                "vent_command_result": "already_closed",
                "vent_close_verification_status": "PASS",
                "vent_close_verification_reason": vent_preclosed_reason,
                **vent_command_diagnostics,
            }
        else:
            try:
                vent_command_diagnostics = self.set_pressure_controller_vent(
                    False,
                    reason="positive CO2 preseal pressurization before route seal",
                    wait_after_command=False,
                    capture_pressure=False,
                    transition_timeout_s=vent_close_command_timeout_s,
                    snapshot_after_command=False,
                    prefer_direct_command=bool(
                        self.host._cfg_get("workflow.pressure.preseal_vent_close_prefer_direct_command", True)
                    ),
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
                    timing_recorder(
                        "positive_preseal_arming_end",
                        "fail",
                        stage="positive_preseal_arming",
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
                        **preseal_arm_context,
                        **preseal_guard_state,
                        "vent_closed_at": vent_closed_at,
                        "vent_command_result": "fail",
                        "command_error": str(exc),
                        "seal_command_blocked_reason": "preseal_vent_close_failed",
                    },
                )
            vent_close = self._verify_positive_preseal_vent_closed(
                controller,
                pressure_reader=pressure_reader,
                ambient_reference=ambient_reference,
                command_diagnostics=vent_command_diagnostics if isinstance(vent_command_diagnostics, dict) else {},
                capture_pressure=vent_close_verify_capture_pressure,
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
                timing_recorder(
                    "positive_preseal_arming_end",
                    "fail",
                    stage="positive_preseal_arming",
                    point=point,
                    target_pressure_hpa=target_pressure_hpa,
                    duration_s=vent_close_elapsed_s,
                    decision="fail",
                    error_code="preseal_vent_close_failed",
                    pressure_hpa=vent_close.get("pressure_hpa"),
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
                    **preseal_arm_context,
                    **preseal_guard_state,
                    "vent_closed_at": vent_closed_at,
                    "vent_command_result": "not_closed",
                    "seal_command_blocked_reason": "preseal_vent_close_failed",
                    **vent_close,
                },
            )
        vent_closed_at = datetime.now(timezone.utc).isoformat()
        preseal_guard_state["vent_close_command_completed_at"] = str(
            vent_close.get("vent_command_write_completed_at")
            or vent_close.get("command_completed_at")
            or vent_close.get("vent_close_command_completed_at")
            or vent_closed_at
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
            timing_recorder(
                "positive_preseal_arming_end",
                "end",
                stage="positive_preseal_arming",
                point=point,
                target_pressure_hpa=target_pressure_hpa,
                duration_s=vent_close_elapsed_s,
                decision="ok",
                pressure_hpa=vent_close.get("pressure_hpa"),
                pace_output_state=vent_close.get("output_state"),
                pace_isolation_state=vent_close.get("isolation_state"),
                pace_vent_status=vent_close.get("vent_status_raw"),
            )
        ready_before_vent_close_completed = bool(
            preseal_arm_context.get("vent_close_arm_trigger") == "ready_pressure"
            or (
                ready_pressure_hpa is not None
                and self._coerce_float(preseal_arm_context.get("vent_close_arm_pressure_hpa")) is not None
                and float(preseal_arm_context.get("vent_close_arm_pressure_hpa")) >= float(ready_pressure_hpa)
            )
        )
        if ready_before_vent_close_completed and callable(timing_recorder):
            timing_recorder(
                "positive_preseal_ready_before_vent_close_end",
                "warning",
                stage="positive_preseal_arming",
                point=point,
                target_pressure_hpa=target_pressure_hpa,
                pressure_hpa=preseal_arm_context.get("vent_close_arm_pressure_hpa"),
                decision="ready_before_vent_close_completed",
                warning_code="positive_preseal_ready_before_vent_close_end",
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
                "vent_command_ack",
                "snapshot_after_command",
                "prefer_direct_command",
                "short_state_probe",
            )
        }
        pressure_at_arm = self._coerce_float(preseal_arm_context.get("vent_close_arm_pressure_hpa"))
        arm_sample_meta = {
            key: preseal_arm_context.get(key)
            for key in (
                "pressure_sample_source",
                "pressure_sample_timestamp",
                "pressure_sample_age_s",
                "pressure_sample_is_stale",
                "pressure_sample_sequence_id",
                "request_sent_at",
                "response_received_at",
                "request_sent_monotonic_s",
                "response_received_monotonic_s",
                "read_latency_s",
                "sample_recorded_at",
                "sample_recorded_monotonic_s",
                "sample_age_s",
                "is_cached",
                "is_stale",
                "stale_threshold_s",
                "serial_port",
                "command",
                "parse_ok",
                "error",
                "sequence_id",
                "usable_for_abort",
                "usable_for_ready",
                "usable_for_seal",
                "primary_pressure_source",
                "pressure_source_used_for_decision",
                "source_selection_reason",
                "pressure_source_used_for_abort",
                "pressure_source_used_for_ready",
                "pressure_source_used_for_seal",
            )
            if key in preseal_arm_context
        }

        previous_pressure: Optional[float] = None
        previous_elapsed: Optional[float] = None
        pressure_peak_hpa: Optional[float] = pressure_at_arm
        pressure_last_hpa: Optional[float] = pressure_at_arm
        pressure_min_hpa: Optional[float] = pressure_at_arm
        pressure_samples_count = 0
        def mark_vent_off_settle_sample(
            *,
            pressure_hpa: Optional[float],
            sample_meta: Mapping[str, Any],
            elapsed_s: float,
            over_abort_seen: bool,
            ready_to_seal_seen: bool,
            predictive_ready_to_seal: bool = False,
            urgent_seal_seen: bool = False,
            ready_window_missed_reason: str = "",
        ) -> None:
            sample_at = str(
                sample_meta.get("sample_recorded_at")
                or sample_meta.get("pressure_sample_timestamp")
                or sample_meta.get("response_received_at")
                or datetime.now(timezone.utc).isoformat()
            )
            if not bool(preseal_guard_state.get("vent_off_settle_monitor_started")):
                preseal_guard_state["vent_off_settle_monitor_started"] = True
                preseal_guard_state["vent_off_settle_monitor_started_at"] = sample_at
                command_mono = self._coerce_float(
                    preseal_arm_context.get("vent_close_command_monotonic_s")
                    or preseal_arm_context.get("vent_off_sent_monotonic_s")
                )
                if command_mono is None:
                    command_mono = vent_close_command_monotonic_s
                preseal_guard_state["vent_close_to_monitor_start_latency_s"] = round(
                    max(0.0, time.monotonic() - float(command_mono)),
                    3,
                )
            preseal_guard_state["vent_off_settle_wait_pressure_monitored"] = True
            preseal_guard_state["vent_off_settle_monitor_sample_count"] = max(
                int(preseal_guard_state.get("vent_off_settle_monitor_sample_count") or 0),
                int(pressure_samples_count or 1),
            )
            if ready_to_seal_seen and pressure_hpa is not None:
                if preseal_guard_state.get("vent_off_settle_first_ready_to_seal_sample_hpa") is None:
                    preseal_guard_state["vent_off_settle_first_ready_to_seal_sample_hpa"] = float(pressure_hpa)
                    preseal_guard_state["vent_off_settle_first_ready_to_seal_sample_at"] = sample_at
                preseal_guard_state["vent_off_settle_wait_ready_to_seal_seen"] = True
                preseal_guard_state["ready_to_seal_window_entered"] = not bool(
                    predictive_ready_to_seal or urgent_seal_seen
                )
                preseal_guard_state["first_target_ready_to_seal_pressure_hpa"] = float(pressure_hpa)
                preseal_guard_state["first_target_ready_to_seal_elapsed_s"] = elapsed_s
                preseal_guard_state["first_target_ready_to_seal_before_abort"] = not over_abort_seen
                preseal_guard_state["first_target_ready_to_seal_missed"] = False
                preseal_guard_state["first_target_ready_to_seal_missed_reason"] = ""
                preseal_guard_state["seal_command_allowed_after_atmosphere_vent_closed"] = True
                preseal_guard_state["preseal_capture_ready_window_action"] = (
                    "urgent_seal"
                    if urgent_seal_seen
                    else (
                        "predictive_ready_to_seal_before_target_window"
                        if predictive_ready_to_seal
                        else "ready_to_seal"
                    )
                )
                preseal_guard_state["preseal_capture_over_abort_action"] = "urgent_seal"
                preseal_guard_state["preseal_capture_over_urgent_threshold_action"] = "urgent_seal"
                preseal_guard_state["preseal_capture_urgent_seal_triggered"] = bool(urgent_seal_seen)
                preseal_guard_state["preseal_capture_urgent_seal_pressure_hpa"] = (
                    float(pressure_hpa) if urgent_seal_seen else None
                )
                preseal_guard_state["preseal_capture_urgent_seal_reason"] = (
                    "urgent_seal_threshold_reached" if urgent_seal_seen else ""
                )
            if over_abort_seen and pressure_hpa is not None:
                if preseal_guard_state.get("vent_off_settle_first_over_abort_sample_hpa") is None:
                    preseal_guard_state["vent_off_settle_first_over_abort_sample_hpa"] = float(pressure_hpa)
                    preseal_guard_state["vent_off_settle_first_over_abort_sample_at"] = sample_at
                preseal_guard_state["vent_off_settle_wait_overlimit_seen"] = True
                preseal_guard_state["first_over_abort_pressure_hpa"] = float(pressure_hpa)
                preseal_guard_state["first_over_abort_elapsed_s"] = max(0.0, elapsed_s)
                preseal_guard_state["first_over_abort_source"] = str(
                    sample_meta.get("pressure_sample_source") or sample_meta.get("source") or ""
                )
                preseal_guard_state["first_over_abort_sample_age_s"] = self._coerce_float(
                    sample_meta.get("pressure_sample_age_s", sample_meta.get("sample_age_s"))
                )
                preseal_guard_state["first_over_abort_to_abort_latency_s"] = 0.0
                preseal_guard_state["first_target_ready_to_seal_missed"] = True
                preseal_guard_state["first_target_ready_to_seal_missed_reason"] = "abort_before_ready_to_seal"
                preseal_guard_state["ready_to_seal_window_missed_reason"] = "abort_before_ready_to_seal"
                preseal_guard_state["overlimit_elapsed_s_nonnegative"] = True
                preseal_guard_state["overlimit_elapsed_source"] = "vent_off_settle_monitor"
                preseal_guard_state["preseal_capture_abort_reason"] = (
                    "preseal_capture_hard_abort_pressure_exceeded"
                )
                preseal_guard_state["preseal_capture_abort_pressure_hpa"] = float(pressure_hpa)
                preseal_guard_state["preseal_capture_abort_source"] = str(
                    sample_meta.get("pressure_sample_source") or sample_meta.get("source") or ""
                )
                preseal_guard_state["preseal_capture_abort_sample_age_s"] = self._coerce_float(
                    sample_meta.get("pressure_sample_age_s", sample_meta.get("sample_age_s"))
                )
                preseal_guard_state["preseal_abort_source_path"] = "positive_preseal_vent_off_settle_monitor"
                preseal_guard_state["positive_preseal_pressure_source_path"] = (
                    "positive_preseal_vent_off_settle_monitor"
                )
                preseal_guard_state["positive_preseal_pressure_missing_reason"] = ""
                preseal_guard_state["high_pressure_first_point_abort_pressure_hpa"] = float(pressure_hpa)
                preseal_guard_state["high_pressure_first_point_abort_reason"] = (
                    "preseal_capture_hard_abort_pressure_exceeded"
                )
                preseal_guard_state["preseal_capture_over_abort_action"] = "fail_closed"
                preseal_guard_state["preseal_capture_over_urgent_threshold_action"] = "fail_closed"
                preseal_guard_state["preseal_capture_hard_abort_triggered"] = True
                preseal_guard_state["preseal_capture_hard_abort_reason"] = (
                    "preseal_capture_hard_abort_pressure_exceeded"
                )
            elif ready_window_missed_reason:
                preseal_guard_state["ready_to_seal_window_missed_reason"] = ready_window_missed_reason

        if (
            pressure_at_arm is not None
            and hard_abort_pressure_hpa is not None
            and float(pressure_at_arm) >= float(hard_abort_pressure_hpa)
            and not bool(arm_sample_meta.get("pressure_sample_is_stale", False))
        ):
            first_over_elapsed_s = max(0.0, time.time() - started_at)
            mark_vent_off_settle_sample(
                pressure_hpa=float(pressure_at_arm),
                sample_meta=arm_sample_meta,
                elapsed_s=first_over_elapsed_s,
                over_abort_seen=True,
                ready_to_seal_seen=False,
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
                pressure_hpa=float(pressure_at_arm),
                pressure_peak_hpa=pressure_peak_hpa,
                pressure_last_hpa=pressure_last_hpa,
                reason="preseal_capture_hard_abort_pressure_exceeded",
                message="Positive preseal pressurization exceeded hard abort pressure",
                extra={
                    **preseal_arm_context,
                    **preseal_guard_state,
                    **state_after_vent,
                    "vent_closed_at": vent_closed_at,
                    "vent_command_result": "closed",
                    "pressure_samples_count": 1,
                    "pressure_max_hpa": pressure_peak_hpa,
                    "pressure_min_hpa": pressure_min_hpa,
                    "ready_reached_before_vent_close_completed": ready_before_vent_close_completed,
                    "ready_reached_during_vent_close": ready_before_vent_close_completed,
                    "seal_command_blocked_reason": "preseal_capture_hard_abort_pressure_exceeded",
                    "first_over_abort_pressure_hpa": float(pressure_at_arm),
                    "first_over_abort_elapsed_s": first_over_elapsed_s,
                    "first_over_abort_source": str(
                        arm_sample_meta.get("pressure_sample_source")
                        or preseal_arm_context.get("pressure_sample_source")
                        or ""
                    ),
                    "first_over_abort_sample_age_s": self._coerce_float(
                        arm_sample_meta.get(
                            "pressure_sample_age_s",
                            preseal_arm_context.get("pressure_sample_age_s"),
                        )
                    ),
                    "first_over_abort_to_abort_latency_s": 0.0,
                    "first_target_ready_to_seal_missed": True,
                    "first_target_ready_to_seal_missed_reason": "abort_before_ready_to_seal",
                    "positive_preseal_guard_started_before_first_over_abort": True,
                },
            )
        if (
            pressure_at_arm is not None
            and not bool(arm_sample_meta.get("pressure_sample_is_stale", False))
            and (
                (
                    ready_to_seal_min_hpa is not None
                    and ready_to_seal_max_hpa is not None
                    and float(pressure_at_arm) >= float(ready_to_seal_min_hpa)
                    and float(pressure_at_arm) <= float(ready_to_seal_max_hpa)
                )
                or (
                    urgent_seal_threshold_hpa is not None
                    and float(pressure_at_arm) >= float(urgent_seal_threshold_hpa)
                )
            )
        ):
            arm_ready_in_window = bool(
                ready_to_seal_min_hpa is not None
                and ready_to_seal_max_hpa is not None
                and float(pressure_at_arm) >= float(ready_to_seal_min_hpa)
                and float(pressure_at_arm) <= float(ready_to_seal_max_hpa)
            )
            arm_urgent_seal = bool(
                urgent_seal_threshold_hpa is not None
                and float(pressure_at_arm) >= float(urgent_seal_threshold_hpa)
                and not arm_ready_in_window
            )
            ready_monotonic_s = self._coerce_float(preseal_arm_context.get("ready_reached_monotonic_s"))
            if ready_monotonic_s is None:
                ready_monotonic_s = time.monotonic()
            ready_elapsed_s = max(0.0, time.time() - started_at)
            mark_vent_off_settle_sample(
                pressure_hpa=float(pressure_at_arm),
                sample_meta=arm_sample_meta,
                elapsed_s=ready_elapsed_s,
                over_abort_seen=False,
                ready_to_seal_seen=True,
                urgent_seal_seen=arm_urgent_seal,
            )
            preseal_guard_state["vent_close_arm_trigger"] = (
                "urgent_seal_threshold" if arm_urgent_seal else "ready_pressure"
            )
            ready_payload = {
                "stage": "positive_preseal_pressurization",
                "positive_preseal_phase_started": True,
                "positive_preseal_phase_started_at": phase_started_at,
                "positive_preseal_pressure_guard_checked": True,
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
                        **preseal_arm_context,
                        **preseal_guard_state,
                        "ready_reached_before_vent_close_completed": ready_before_vent_close_completed,
                "ready_reached_during_vent_close": ready_before_vent_close_completed,
                "elapsed_s": ready_elapsed_s,
                "pressure_hpa": float(pressure_at_arm),
                "current_line_pressure_hpa": float(pressure_at_arm),
                "positive_preseal_pressure_hpa": float(pressure_at_arm),
                "positive_preseal_pressure_source": str(
                    arm_sample_meta.get("pressure_sample_source")
                    or preseal_arm_context.get("pressure_sample_source")
                    or ""
                ),
                "positive_preseal_pressure_source_path": "positive_preseal_vent_off_settle_monitor",
                "positive_preseal_pressure_missing_reason": "",
                "positive_preseal_pressure_sample_age_s": self._coerce_float(
                    arm_sample_meta.get(
                        "pressure_sample_age_s",
                        preseal_arm_context.get("pressure_sample_age_s"),
                    )
                ),
                "positive_preseal_abort_pressure_hpa": abort_pressure_hpa,
                "positive_preseal_pressure_overlimit": False,
                "positive_preseal_abort_reason": "",
                "positive_preseal_setpoint_sent": False,
                "positive_preseal_setpoint_hpa": None,
                "positive_preseal_output_enabled": False,
                "positive_preseal_route_open": True,
                "positive_preseal_seal_command_sent": False,
                "positive_preseal_pressure_setpoint_command_sent": False,
                "positive_preseal_sample_started": False,
                "positive_preseal_overlimit_fail_closed": False,
                "pressure_samples_count": 1,
                "pressure_max_hpa": pressure_peak_hpa,
                "pressure_min_hpa": pressure_min_hpa,
                "pressure_rise_rate_hpa_per_s": None,
                "ready_reached": True,
                "ready_reached_at_pressure_hpa": float(pressure_at_arm),
                "first_target_ready_to_seal_pressure_hpa": float(pressure_at_arm),
                "first_target_ready_to_seal_elapsed_s": ready_elapsed_s,
                "first_target_ready_to_seal_before_abort": True,
                "first_target_ready_to_seal_missed": False,
                "first_target_ready_to_seal_missed_reason": "",
                "vent_off_settle_wait_pressure_monitored": bool(
                    preseal_guard_state.get("vent_off_settle_wait_pressure_monitored", False)
                ),
                "vent_off_settle_wait_ready_to_seal_seen": bool(
                    preseal_guard_state.get("vent_off_settle_wait_ready_to_seal_seen")
                    or not ready_before_vent_close_completed
                ),
                "seal_command_sent": False,
                "sealed": False,
                "pressure_control_started": False,
                "seal_trigger_pressure_hpa": float(pressure_at_arm),
                "seal_trigger_elapsed_s": ready_elapsed_s,
                "ready_reached_monotonic_s": float(ready_monotonic_s),
                "ready_reached_wall_time_s": time.time(),
                "decision": "READY",
                **state_after_vent,
                **arm_sample_meta,
            }
            self._record_route_trace(
                action="positive_preseal_ready",
                route=route,
                point=point,
                actual=ready_payload,
                result="ok",
                message="Positive preseal ready threshold reached from armed pressure sample",
            )
            if callable(timing_recorder):
                if high_pressure_first_point_mode:
                    timing_recorder(
                        "high_pressure_ready_detected",
                        "info",
                        stage="high_pressure_first_point",
                        point=point,
                        target_pressure_hpa=target_pressure_hpa,
                        duration_s=ready_elapsed_s,
                        expected_max_s=timeout_s,
                        pressure_hpa=pressure_at_arm,
                        decision="ready",
                        route_state=arm_sample_meta,
                    )
                    if conditioning_completed_before_high_pressure_mode:
                        timing_recorder(
                            "high_pressure_ready_detected_after_conditioning",
                            "info",
                            stage="high_pressure_first_point",
                            point=point,
                            target_pressure_hpa=target_pressure_hpa,
                            duration_s=ready_elapsed_s,
                            expected_max_s=timeout_s,
                            pressure_hpa=pressure_at_arm,
                            decision="ready_after_conditioning",
                            route_state={
                                **arm_sample_meta,
                                "conditioning_completed_before_high_pressure_mode": True,
                                "conditioning_completed_at": conditioning_completed_at,
                            },
                        )
                timing_recorder(
                    "positive_preseal_ready",
                    "info",
                    stage="positive_preseal_pressurization",
                    point=point,
                    target_pressure_hpa=target_pressure_hpa,
                    duration_s=ready_elapsed_s,
                    expected_max_s=timeout_s,
                    wait_reason="positive_preseal_pressure_rise",
                    pressure_hpa=pressure_at_arm,
                    decision="ready",
                    route_state=arm_sample_meta,
                    pace_output_state=state_after_vent.get("output_state"),
                    pace_isolation_state=state_after_vent.get("isolation_state"),
                    pace_vent_status=state_after_vent.get("vent_status_raw"),
                )
            return PressureWaitResult(
                ok=True,
                target_hpa=target_pressure_hpa,
                final_pressure_hpa=float(pressure_at_arm),
                in_limits=True,
                diagnostics={
                    **ready_payload,
                    "preseal_pressure_peak_hpa": pressure_peak_hpa,
                    "preseal_pressure_last_hpa": pressure_last_hpa,
                    "pressure_samples_count": 1,
                    "pressure_max_hpa": pressure_peak_hpa,
                    "pressure_min_hpa": pressure_min_hpa,
                    "preseal_trigger": "positive_preseal_ready",
                    "preseal_trigger_pressure_hpa": float(pressure_at_arm),
                    "preseal_trigger_threshold_hpa": ready_pressure_hpa,
                    "positive_preseal_started_monotonic_s": started_monotonic_s,
                    "ready_reached_monotonic_s": float(ready_monotonic_s),
                    "ready_to_vent_close_start_s": (
                        None
                        if preseal_arm_context.get("ready_reached_monotonic_s") is None
                        else max(
                            0.0,
                            started_monotonic_s - float(preseal_arm_context.get("ready_reached_monotonic_s")),
                        )
                    ),
                    "ready_to_vent_close_end_s": (
                        None
                        if preseal_arm_context.get("ready_reached_monotonic_s") is None
                        else max(
                            0.0,
                            time.monotonic() - float(preseal_arm_context.get("ready_reached_monotonic_s")),
                        )
                    ),
                },
            )
        while True:
            self.host._check_stop()
            elapsed_s = max(0.0, time.time() - started_at)
            if high_pressure_first_point_mode:
                pressure_sample = self._current_high_pressure_first_point_sample(
                    stage="positive_preseal_pressurization",
                    point_index=point.index,
                )
            else:
                pressure_sample = self._read_pressure_sample(pressure_reader, source="pressure_gauge")
            pressure_hpa = self._coerce_float(pressure_sample.get("pressure_hpa"))
            # A2.32: unified recovery — if P3 returns empty, retry with cooldown + orchestrator fallback
            if pressure_hpa is None:
                _recovered = self._read_pressure_with_recovery()
                if _recovered is not None and _recovered > 0:
                    pressure_hpa = float(_recovered)
                    pressure_sample = dict(pressure_sample)
                    pressure_sample["pressure_hpa"] = pressure_hpa
                    pressure_sample["pressure_sample_source"] = "positive_preseal_recovery"
                    pressure_sample["is_stale"] = False
                    self.host._log(f"Preseal pressure recovered: {pressure_hpa:.1f} hPa")
            sample_meta = {
                key: pressure_sample.get(key)
                for key in (
                    "pressure_sample_source",
                    "pressure_sample_timestamp",
                    "pressure_sample_age_s",
                    "pressure_sample_is_stale",
                    "pressure_sample_sequence_id",
                    "request_sent_at",
                    "response_received_at",
                    "request_sent_monotonic_s",
                    "response_received_monotonic_s",
                    "read_latency_s",
                    "sample_recorded_at",
                    "sample_recorded_monotonic_s",
                    "sample_age_s",
                    "is_cached",
                    "is_stale",
                    "stale_threshold_s",
                    "serial_port",
                    "command",
                    "parse_ok",
                    "error",
                    "sequence_id",
                    "usable_for_abort",
                    "usable_for_ready",
                    "usable_for_seal",
                    "primary_pressure_source",
                    "pressure_source_used_for_decision",
                    "source_selection_reason",
                    "pressure_source_used_for_abort",
                    "pressure_source_used_for_ready",
                    "pressure_source_used_for_seal",
                    "pressure_source_cross_check_enabled",
                    "pressure_source_cross_check_role",
                    "pressure_source_disagreement_hpa",
                    "pressure_source_disagreement_warning",
                    "pressure_source_disagreement_warn_hpa",
                    "digital_gauge_mode",
                    "digital_gauge_continuous_active",
                    "digital_gauge_continuous_enabled",
                    "digital_gauge_continuous_mode",
                    "latest_frame_age_s",
                    "latest_frame_interval_s",
                    "latest_frame_sequence_id",
                    "frame_received_at",
                    "monotonic_timestamp",
                    "raw_line",
                    "critical_window_uses_latest_frame",
                    "critical_window_uses_query",
                    "critical_window_blocking_query_count",
                    "critical_window_blocking_query_total_s",
                    "pace_aux_enabled",
                    "pace_aux_topology_connected",
                    "pace_aux_trigger_candidate",
                    "pace_digital_overlap_samples",
                    "pace_digital_max_diff_hpa",
                    "pace_pressure_hpa",
                    "pace_pressure_latency_s",
                    "pace_pressure_age_s",
                    "pace_pressure_stale",
                    "digital_gauge_pressure_hpa",
                    "digital_gauge_latency_s",
                    "digital_gauge_age_s",
                    "digital_gauge_stale",
                    "pace_pressure_sample",
                    "digital_gauge_pressure_sample",
                )
            }
            sample_is_stale = bool(sample_meta.get("pressure_sample_is_stale"))
            if high_pressure_first_point_mode and pressure_hpa is None:
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
                    pressure_hpa=pressure_last_hpa,
                    pressure_peak_hpa=pressure_peak_hpa,
                    pressure_last_hpa=pressure_last_hpa,
                    reason=str(sample_meta.get("error") or "critical_pressure_sample_unavailable"),
                    message="A2 high-pressure first point continuous pressure latest frame unavailable",
                    extra={
                        **preseal_arm_context,
                        **preseal_guard_state,
                        **state_after_vent,
                        **sample_meta,
                        "vent_closed_at": vent_closed_at,
                        "vent_command_result": "closed",
                        "pressure_samples_count": pressure_samples_count,
                        "pressure_max_hpa": pressure_peak_hpa,
                        "pressure_min_hpa": pressure_min_hpa,
                        "seal_command_blocked_reason": "critical_pressure_sample_unavailable",
                    },
                )
            pressure_rise_rate: Optional[float] = None
            if pressure_hpa is not None:
                pressure_samples_count += 1
                if not sample_is_stale:
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
            urgent_seal_seen = bool(
                pressure_hpa is not None
                and urgent_seal_threshold_hpa is not None
                and not sample_is_stale
                and float(pressure_hpa) >= float(urgent_seal_threshold_hpa)
            )
            hard_abort_seen = bool(
                pressure_hpa is not None
                and hard_abort_pressure_hpa is not None
                and not sample_is_stale
                and float(pressure_hpa) >= float(hard_abort_pressure_hpa)
            )
            ready_to_seal_seen = bool(
                pressure_hpa is not None
                and ready_to_seal_min_hpa is not None
                and ready_to_seal_max_hpa is not None
                and not sample_is_stale
                and float(pressure_hpa) >= float(ready_to_seal_min_hpa)
                and float(pressure_hpa) <= float(ready_to_seal_max_hpa)
            )
            seal_completion_latency_s = self._preseal_capture_predictive_seal_latency_s()
            estimated_time_to_target_s: Optional[float] = None
            predicted_seal_completion_pressure_hpa: Optional[float] = None
            predictive_ready_to_seal = False
            if (
                pressure_hpa is not None
                and pressure_rise_rate is not None
                and pressure_rise_rate > 0.0
                and ready_to_seal_min_hpa is not None
                and ready_to_seal_max_hpa is not None
                and not sample_is_stale
                and float(pressure_hpa) < float(ready_to_seal_min_hpa)
            ):
                estimated_time_to_target_s = max(
                    0.0,
                    (float(ready_to_seal_min_hpa) - float(pressure_hpa)) / float(pressure_rise_rate),
                )
                predicted_seal_completion_pressure_hpa = float(pressure_hpa) + (
                    float(pressure_rise_rate) * float(seal_completion_latency_s)
                )
                predictive_ready_to_seal = bool(
                    float(predicted_seal_completion_pressure_hpa) >= float(ready_to_seal_min_hpa)
                    and float(predicted_seal_completion_pressure_hpa) <= float(ready_to_seal_max_hpa)
                )
            if predictive_ready_to_seal:
                ready_to_seal_seen = True
                preseal_guard_state["preseal_capture_predictive_ready_to_seal"] = True
                preseal_guard_state["preseal_capture_predictive_trigger_reason"] = (
                    "predicted_seal_completion_in_target_window"
                )
            if urgent_seal_seen and not hard_abort_seen:
                ready_to_seal_seen = True
            preseal_guard_state["preseal_capture_pressure_rise_rate_hpa_per_s"] = pressure_rise_rate
            preseal_guard_state["preseal_capture_estimated_time_to_target_s"] = estimated_time_to_target_s
            preseal_guard_state["preseal_capture_seal_completion_latency_s"] = seal_completion_latency_s
            preseal_guard_state["preseal_capture_predicted_seal_completion_pressure_hpa"] = (
                predicted_seal_completion_pressure_hpa
            )
            preseal_guard_state["preseal_capture_urgent_seal_threshold_hpa"] = urgent_seal_threshold_hpa
            preseal_guard_state["preseal_capture_hard_abort_pressure_hpa"] = hard_abort_pressure_hpa
            preseal_guard_state["preseal_capture_hard_abort_triggered"] = hard_abort_seen
            preseal_guard_state["preseal_capture_hard_abort_reason"] = (
                "preseal_capture_hard_abort_pressure_exceeded" if hard_abort_seen else ""
            )
            ready_window_missed = bool(
                pressure_hpa is not None
                and ready_to_seal_max_hpa is not None
                and not sample_is_stale
                and float(pressure_hpa) > float(ready_to_seal_max_hpa)
                and not hard_abort_seen
                and not urgent_seal_seen
            )
            ready_window_missed_reason = (
                "pressure_above_ready_to_seal_window_before_abort"
                if ready_window_missed
                else ("hard_abort_before_ready_to_seal" if hard_abort_seen else "")
            )
            mark_vent_off_settle_sample(
                pressure_hpa=None if pressure_hpa is None else float(pressure_hpa),
                sample_meta=sample_meta,
                elapsed_s=elapsed_s,
                over_abort_seen=hard_abort_seen,
                ready_to_seal_seen=ready_to_seal_seen,
                predictive_ready_to_seal=predictive_ready_to_seal,
                urgent_seal_seen=urgent_seal_seen and not hard_abort_seen,
                ready_window_missed_reason=ready_window_missed_reason,
            )
            check_payload = {
                "stage": "positive_preseal_pressurization",
                "positive_preseal_phase_started": True,
                "positive_preseal_phase_started_at": phase_started_at,
                "positive_preseal_pressure_guard_checked": True,
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
                **preseal_arm_context,
                **preseal_guard_state,
                "ready_reached_before_vent_close_completed": ready_before_vent_close_completed,
                "ready_reached_during_vent_close": ready_before_vent_close_completed,
                "elapsed_s": elapsed_s,
                "pressure_hpa": pressure_hpa,
                "current_line_pressure_hpa": pressure_hpa,
                "positive_preseal_pressure_hpa": pressure_hpa,
                "positive_preseal_pressure_source": str(
                    sample_meta.get("pressure_sample_source") or sample_meta.get("source") or ""
                ),
                "positive_preseal_pressure_sample_age_s": self._coerce_float(
                    sample_meta.get("pressure_sample_age_s", sample_meta.get("sample_age_s"))
                ),
                "positive_preseal_abort_pressure_hpa": abort_pressure_hpa,
                "positive_preseal_pressure_overlimit": bool(
                    hard_abort_seen
                ),
                "positive_preseal_abort_reason": "",
                "positive_preseal_setpoint_sent": False,
                "positive_preseal_setpoint_hpa": None,
                "positive_preseal_output_enabled": False,
                "positive_preseal_route_open": True,
                "positive_preseal_seal_command_sent": False,
                "positive_preseal_pressure_setpoint_command_sent": False,
                "positive_preseal_sample_started": False,
                "positive_preseal_overlimit_fail_closed": False,
                "pressure_samples_count": pressure_samples_count,
                "pressure_max_hpa": pressure_peak_hpa,
                "pressure_min_hpa": pressure_min_hpa,
                "pressure_rise_rate_hpa_per_s": pressure_rise_rate,
                "preseal_capture_predictive_ready_to_seal": predictive_ready_to_seal,
                "preseal_capture_pressure_rise_rate_hpa_per_s": pressure_rise_rate,
                "preseal_capture_estimated_time_to_target_s": estimated_time_to_target_s,
                "preseal_capture_seal_completion_latency_s": seal_completion_latency_s,
                "preseal_capture_predicted_seal_completion_pressure_hpa": predicted_seal_completion_pressure_hpa,
                "preseal_capture_predictive_trigger_reason": (
                    "predicted_seal_completion_in_target_window" if predictive_ready_to_seal else ""
                ),
                "preseal_capture_urgent_seal_threshold_hpa": urgent_seal_threshold_hpa,
                "preseal_capture_hard_abort_pressure_hpa": hard_abort_pressure_hpa,
                "preseal_capture_over_urgent_threshold_action": (
                    "fail_closed" if hard_abort_seen else "urgent_seal"
                ),
                "preseal_capture_urgent_seal_triggered": bool(urgent_seal_seen and not hard_abort_seen),
                "preseal_capture_urgent_seal_pressure_hpa": (
                    float(pressure_hpa) if urgent_seal_seen and not hard_abort_seen else None
                ),
                "preseal_capture_urgent_seal_reason": (
                    "urgent_seal_threshold_reached" if urgent_seal_seen and not hard_abort_seen else ""
                ),
                "preseal_capture_hard_abort_triggered": hard_abort_seen,
                "preseal_capture_hard_abort_reason": (
                    "preseal_capture_hard_abort_pressure_exceeded" if hard_abort_seen else ""
                ),
                "preseal_capture_continue_to_control_after_seal": False,
                "pressure_control_allowed_after_seal_confirmed": False,
                "pressure_control_target_after_preseal_hpa": target_pressure_hpa,
                "vent_off_settle_wait_pressure_monitored": True,
                "vent_off_settle_wait_overlimit_seen": hard_abort_seen,
                "vent_off_settle_wait_ready_to_seal_seen": ready_to_seal_seen,
                "first_target_ready_to_seal_pressure_hpa": (
                    float(pressure_hpa) if ready_to_seal_seen else None
                ),
                "first_target_ready_to_seal_elapsed_s": elapsed_s if ready_to_seal_seen else None,
                "first_target_ready_to_seal_before_abort": bool(ready_to_seal_seen and not hard_abort_seen),
                "first_target_ready_to_seal_missed": ready_window_missed or hard_abort_seen,
                "first_target_ready_to_seal_missed_reason": ready_window_missed_reason,
                "first_over_abort_pressure_hpa": float(pressure_hpa) if hard_abort_seen else None,
                "first_over_abort_elapsed_s": elapsed_s if hard_abort_seen else None,
                "first_over_abort_source": str(
                    sample_meta.get("pressure_sample_source") or sample_meta.get("source") or ""
                )
                if hard_abort_seen
                else "",
                "first_over_abort_sample_age_s": self._coerce_float(
                    sample_meta.get("pressure_sample_age_s", sample_meta.get("sample_age_s"))
                )
                if hard_abort_seen
                else None,
                "first_over_abort_to_abort_latency_s": 0.0 if hard_abort_seen else None,
                "positive_preseal_guard_started_before_first_over_abort": bool(
                    preseal_guard_state.get("preseal_guard_armed") and hard_abort_seen
                ),
                "positive_preseal_guard_started_after_first_over_abort": False,
                "positive_preseal_guard_late_reason": "",
                **sample_meta,
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
                    route_state=sample_meta,
                )
            if sample_is_stale:
                if callable(timing_recorder):
                    timing_recorder(
                        "positive_preseal_pressure_check",
                        "warning",
                        stage="positive_preseal_pressurization",
                        point=point,
                        target_pressure_hpa=target_pressure_hpa,
                        duration_s=elapsed_s,
                        expected_max_s=timeout_s,
                        pressure_hpa=pressure_hpa,
                        decision="stale_pressure_sample_ignored",
                        warning_code="stale_pressure_sample_ignored",
                        route_state=sample_meta,
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
                        pressure_hpa=pressure_last_hpa,
                        pressure_peak_hpa=pressure_peak_hpa,
                        pressure_last_hpa=pressure_last_hpa,
                        reason="preseal_ready_timeout",
                        message="Positive preseal pressurization timed out before ready pressure",
                        extra={
                            **preseal_arm_context,
                            **preseal_guard_state,
                            **state_after_vent,
                            **sample_meta,
                            **{
                                key: value
                                for key, value in check_payload.items()
                                if key
                                not in {
                                    "stage",
                                    "pressure_hpa",
                                    "current_line_pressure_hpa",
                                    "positive_preseal_pressure_hpa",
                                }
                            },
                            "vent_closed_at": vent_closed_at,
                            "vent_command_result": "closed",
                            "pressure_samples_count": pressure_samples_count,
                            "pressure_max_hpa": pressure_peak_hpa,
                            "pressure_min_hpa": pressure_min_hpa,
                            "seal_command_blocked_reason": "preseal_ready_timeout",
                        },
                    )
                time.sleep(min(poll_interval_s, max(0.05, timeout_s - elapsed_s)))
                continue
            if hard_abort_seen:
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
                    reason="preseal_capture_hard_abort_pressure_exceeded",
                    message="Positive preseal pressurization exceeded hard abort pressure",
                    extra={
                        **preseal_arm_context,
                        **preseal_guard_state,
                        **state_after_vent,
                        **sample_meta,
                        **{
                            key: value
                            for key, value in check_payload.items()
                            if key
                            not in {
                                "stage",
                                "pressure_hpa",
                                "current_line_pressure_hpa",
                                "positive_preseal_pressure_hpa",
                            }
                        },
                        "vent_closed_at": vent_closed_at,
                        "vent_command_result": "closed",
                        "pressure_samples_count": pressure_samples_count,
                        "pressure_max_hpa": pressure_peak_hpa,
                        "pressure_min_hpa": pressure_min_hpa,
                        "ready_reached_before_vent_close_completed": ready_before_vent_close_completed,
                        "ready_reached_during_vent_close": ready_before_vent_close_completed,
                        "seal_command_blocked_reason": "preseal_capture_hard_abort_pressure_exceeded",
                    },
                )
            if ready_to_seal_seen:
                ready_monotonic_s = time.monotonic()
                ready_payload = {
                    **check_payload,
                    "ready_reached": True,
                    "ready_reached_at_pressure_hpa": float(pressure_hpa),
                    "seal_trigger_pressure_hpa": float(pressure_hpa),
                    "seal_trigger_elapsed_s": elapsed_s,
                    "ready_reached_monotonic_s": ready_monotonic_s,
                    "ready_reached_wall_time_s": time.time(),
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
                    if high_pressure_first_point_mode:
                        timing_recorder(
                            "high_pressure_ready_detected",
                            "info",
                            stage="high_pressure_first_point",
                            point=point,
                            target_pressure_hpa=target_pressure_hpa,
                            duration_s=elapsed_s,
                            expected_max_s=timeout_s,
                            pressure_hpa=pressure_hpa,
                            decision="ready",
                            route_state=sample_meta,
                        )
                        if conditioning_completed_before_high_pressure_mode:
                            timing_recorder(
                                "high_pressure_ready_detected_after_conditioning",
                                "info",
                                stage="high_pressure_first_point",
                                point=point,
                                target_pressure_hpa=target_pressure_hpa,
                                duration_s=elapsed_s,
                                expected_max_s=timeout_s,
                                pressure_hpa=pressure_hpa,
                                decision="ready_after_conditioning",
                                route_state={
                                    **sample_meta,
                                    "conditioning_completed_before_high_pressure_mode": True,
                                    "conditioning_completed_at": conditioning_completed_at,
                                },
                            )
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
                        "positive_preseal_started_monotonic_s": started_monotonic_s,
                        "ready_reached_monotonic_s": ready_monotonic_s,
                        "ready_to_vent_close_start_s": (
                            None
                            if preseal_arm_context.get("ready_reached_monotonic_s") is None
                            else max(
                                0.0,
                                started_monotonic_s - float(preseal_arm_context.get("ready_reached_monotonic_s")),
                            )
                        ),
                        "ready_to_vent_close_end_s": (
                            None
                            if preseal_arm_context.get("ready_reached_monotonic_s") is None
                            else max(
                                0.0,
                                time.monotonic() - float(preseal_arm_context.get("ready_reached_monotonic_s")),
                            )
                        ),
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
                        **preseal_arm_context,
                        **preseal_guard_state,
                        **state_after_vent,
                        **sample_meta,
                        **{
                            key: value
                            for key, value in check_payload.items()
                            if key
                            not in {
                                "stage",
                                "pressure_hpa",
                                "current_line_pressure_hpa",
                                "positive_preseal_pressure_hpa",
                            }
                        },
                        "vent_closed_at": vent_closed_at,
                        "vent_command_result": "closed",
                        "pressure_samples_count": pressure_samples_count,
                        "pressure_max_hpa": pressure_peak_hpa,
                        "pressure_min_hpa": pressure_min_hpa,
                        "seal_command_blocked_reason": "preseal_ready_timeout",
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

    def set_pressure_controller_vent_fast_reassert(
        self,
        vent_on: bool,
        reason: str = "",
        *,
        max_duration_s: Optional[float] = None,
        wait_after_command: bool = False,
        capture_pressure: bool = False,
        query_state: bool = False,
        confirm_transition: bool = False,
    ) -> dict[str, Any]:
        controller = self.host._device("pressure_controller")
        resolved_max_s = float(
            max_duration_s
            if max_duration_s is not None
            else self.host._cfg_get("workflow.pressure.route_conditioning_fast_vent_max_duration_s", 0.5)
        )
        resolved_max_s = max(0.05, resolved_max_s)
        write_started_monotonic_s = time.monotonic()
        write_started_at = datetime.now(timezone.utc).isoformat()
        diagnostics: dict[str, Any] = {
            "fast_vent_reassert_supported": False,
            "fast_vent_reassert_used": False,
            "vent_command_write_started_at": write_started_at,
            "vent_command_write_sent_at": write_started_at,
            "vent_command_write_completed_at": "",
            "vent_command_write_started_monotonic_s": write_started_monotonic_s,
            "vent_command_write_sent_monotonic_s": write_started_monotonic_s,
            "vent_command_write_completed_monotonic_s": None,
            "vent_command_write_duration_ms": None,
            "vent_command_total_duration_ms": None,
            "vent_command_wait_after_command_s": 0.0 if not wait_after_command else None,
            "vent_command_capture_pressure_enabled": bool(capture_pressure),
            "vent_command_query_state_enabled": bool(query_state),
            "vent_command_confirm_transition_enabled": bool(confirm_transition),
            "vent_command_blocking_phase": "fast_vent_write",
            "route_conditioning_fast_vent_command_timeout": False,
            "route_conditioning_fast_vent_not_supported": False,
            "command_result": "ok",
            "command_error": "",
            "command_method": "",
            "vent_on": bool(vent_on),
            "reason": reason,
        }
        if controller is None or not vent_on:
            diagnostics.update(
                {
                    "route_conditioning_fast_vent_not_supported": True,
                    "command_result": "unsupported",
                    "command_error": "route_conditioning_fast_vent_not_supported",
                }
            )
            return diagnostics
        if vent_on:
            guard = getattr(self.host, "_guard_a2_conditioning_vent_command", None)
            if callable(guard):
                blocked = guard(reason=reason)
                if isinstance(blocked, Mapping) and bool(blocked.get("vent_command_blocked")):
                    diagnostics.update(dict(blocked))
                    diagnostics.update(
                        {
                            "command_result": "blocked",
                            "command_error": str(
                                blocked.get("vent_pulse_blocked_reason") or "vent_command_blocked"
                            ),
                        }
                    )
                    self._record_route_trace(
                        action="set_vent",
                        target={"vent_on": True, "fast_reassert": True},
                        actual=diagnostics,
                        result="blocked",
                        message=diagnostics["command_error"],
                    )
                    return diagnostics
        trace_result = "ok"
        trace_message = reason or "fast vent reassert"
        try:
            fast_reassert = getattr(controller, "fast_vent_reassert", None)
            if callable(fast_reassert):
                diagnostics["fast_vent_reassert_supported"] = True
                diagnostics["fast_vent_reassert_used"] = True
                diagnostics["command_method"] = "fast_vent_reassert"
                fast_reassert(True)
            else:
                raw_writer = getattr(controller, "write", None)
                if callable(raw_writer):
                    diagnostics["fast_vent_reassert_supported"] = True
                    diagnostics["fast_vent_reassert_used"] = True
                    diagnostics["command_method"] = "raw_write_vent_true"
                    raw_writer(":SOUR:PRES:LEV:IMM:AMPL:VENT 1")
                else:
                    process_command = getattr(controller, "process_command", None)
                    if callable(process_command):
                        diagnostics["fast_vent_reassert_supported"] = True
                        diagnostics["fast_vent_reassert_used"] = True
                        diagnostics["command_method"] = "process_command_vent_true"
                        process_command(":SOUR:PRES:LEV:IMM:AMPL:VENT 1")
            if not diagnostics["command_method"]:
                trace_result = "fail"
                trace_message = "route_conditioning_fast_vent_not_supported"
                diagnostics.update(
                    {
                        "route_conditioning_fast_vent_not_supported": True,
                        "command_result": "unsupported",
                        "command_error": trace_message,
                    }
                )
        except Exception as exc:
            trace_result = "fail"
            trace_message = str(exc)
            diagnostics.update({"command_result": "fail", "command_error": trace_message})
        write_completed_monotonic_s = time.monotonic()
        write_completed_at = datetime.now(timezone.utc).isoformat()
        write_duration_ms = round(
            max(0.0, write_completed_monotonic_s - write_started_monotonic_s) * 1000.0,
            3,
        )
        diagnostics.update(
            {
                "vent_command_write_completed_at": write_completed_at,
                "vent_command_write_completed_monotonic_s": write_completed_monotonic_s,
                "vent_command_write_duration_ms": write_duration_ms,
                "vent_command_total_duration_ms": write_duration_ms,
            }
        )
        if (
            diagnostics.get("command_result") == "ok"
            and write_completed_monotonic_s - write_started_monotonic_s > resolved_max_s
        ):
            trace_result = "fail"
            trace_message = "route_conditioning_fast_vent_command_timeout"
            diagnostics.update(
                {
                    "command_result": "timeout",
                    "command_error": trace_message,
                    "route_conditioning_fast_vent_command_timeout": True,
                }
            )
        self._record_route_trace(
            action="set_vent",
            target={"vent_on": True, "fast_reassert": True},
            actual=diagnostics,
            result=trace_result,
            message=trace_message,
        )
        if trace_result == "ok":
            self._clear_pressure_route_seal_state()
        return diagnostics

    def _a2_emergency_abort_relief_context_for_reason(self, reason: str) -> dict[str, Any]:
        context = dict(getattr(self.host, "_a2_co2_route_conditioning_at_atmosphere_context", {}) or {})
        if not context:
            return {}
        required = bool(
            context.get("emergency_abort_relief_vent_required")
            or context.get("positive_preseal_pressure_overlimit")
            or str(context.get("positive_preseal_abort_reason") or context.get("abort_reason") or "")
            == "preseal_abort_pressure_exceeded"
        )
        if not required:
            return {}
        reason_text = str(reason or "").strip().lower()
        if not reason_text or any(
            token in reason_text
            for token in (
                "abort",
                "cleanup",
                "failure",
                "safe stop",
                "pressure-seal",
                "pressure seal",
                "relief",
                "final",
            )
        ):
            return context
        return {}

    def _a2_cleanup_relief_classification_for_reason(
        self,
        reason: str,
        current: str,
    ) -> str:
        classification = str(current or "normal_maintenance_vent").strip() or "normal_maintenance_vent"
        if classification != "normal_maintenance_vent":
            return classification
        context = dict(getattr(self.host, "_a2_co2_route_conditioning_at_atmosphere_context", {}) or {})
        if not context:
            return classification
        reason_text = str(reason or "").strip().lower()
        if "safe stop" in reason_text or reason_text.startswith("final"):
            return "safe_stop_relief"
        if any(
            token in reason_text
            for token in (
                "cleanup",
                "clean up",
                "fail-closed",
                "fail closed",
                "failure",
                "after co2 route fail",
                "after route fail",
                "relief",
            )
        ):
            return "cleanup_relief"
        return classification

    def set_pressure_controller_vent(
        self,
        vent_on: bool,
        reason: str = "",
        *,
        wait_after_command: bool = True,
        capture_pressure: bool = True,
        transition_timeout_s: Optional[float] = None,
        snapshot_after_command: bool = True,
        prefer_direct_command: bool = False,
        vent_classification: str = "normal_maintenance_vent",
        emergency_abort_relief: bool = False,
        emergency_abort_relief_context: Optional[Mapping[str, Any]] = None,
    ) -> dict[str, Any]:
        controller = self.host._device("pressure_controller")
        if controller is None:
            return {}
        guard_payload: dict[str, Any] = {}
        if vent_on and not emergency_abort_relief:
            inferred_context = self._a2_emergency_abort_relief_context_for_reason(reason)
            if inferred_context:
                emergency_abort_relief = True
                emergency_abort_relief_context = inferred_context
                vent_classification = "emergency_abort_relief"
        if vent_on and not emergency_abort_relief:
            vent_classification = self._a2_cleanup_relief_classification_for_reason(
                reason,
                vent_classification,
            )
        if vent_on and emergency_abort_relief:
            vent_classification = "emergency_abort_relief"
        if vent_on:
            guard = getattr(self.host, "_guard_a2_conditioning_vent_command", None)
            if callable(guard):
                try:
                    blocked = guard(
                        reason=reason,
                        vent_classification=vent_classification,
                        emergency_abort_relief=emergency_abort_relief,
                        relief_context=emergency_abort_relief_context,
                    )
                except TypeError:
                    blocked = guard(reason=reason)
                if isinstance(blocked, Mapping) and bool(blocked.get("vent_command_blocked")):
                    self._record_route_trace(
                        action="set_vent",
                        target={"vent_on": True},
                        actual=blocked,
                        result="blocked",
                        message=str(blocked.get("vent_pulse_blocked_reason") or "vent command blocked"),
                    )
                    return dict(blocked)
                if isinstance(blocked, Mapping):
                    guard_payload = dict(blocked)
        pressure_points_started = self._a2_pressure_points_started_or_control_active()
        if (
            vent_on
            and not emergency_abort_relief
            and self._a2_preseal_state_machine_enforced()
            and pressure_points_started
        ):
            blocked_payload = {
                "vent_command_blocked": True,
                "vent_pulse_blocked_reason": "normal_atmosphere_vent_after_pressure_points_started",
                "normal_atmosphere_vent_attempted_after_pressure_points_started": True,
                "normal_atmosphere_vent_blocked_after_pressure_points_started": True,
                "emergency_relief_after_pressure_control_is_abort_only": False,
                "resume_after_emergency_relief_allowed": False,
                "cleanup_vent_classification": "normal_maintenance_vent",
                "cleanup_vent_requested": True,
                "cleanup_vent_allowed": False,
                "cleanup_vent_blocked_reason": "normal_atmosphere_vent_after_pressure_points_started",
            }
            self._record_route_trace(
                action="set_vent",
                target={"vent_on": True},
                actual=blocked_payload,
                result="blocked",
                message="normal_atmosphere_vent_after_pressure_points_started",
            )
            return blocked_payload
        if vent_on and emergency_abort_relief:
            guard_payload.update(
                {
                    "emergency_relief_after_pressure_control_is_abort_only": bool(pressure_points_started),
                    "resume_after_emergency_relief_allowed": False,
                }
            )
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
                        resolved_timeout_s = (
                            float(transition_timeout_s)
                            if transition_timeout_s is not None
                            else float(self.host._cfg_get("workflow.pressure.vent_transition_timeout_s", 30.0))
                        )
                        enter(
                            timeout_s=resolved_timeout_s,
                            hold_open=bool(self.host._cfg_get("workflow.pressure.continuous_atmosphere_hold", True)),
                            hold_interval_s=float(self.host._cfg_get("workflow.pressure.vent_hold_interval_s", 2.0)),
                        )
                        self._log_pressure_controller_io("RX", "ok")
            else:
                if prefer_direct_command:
                    self.host._call_first(controller, ("set_output",), False)
                    direct_vent_ok = bool(self.host._call_first(controller, ("vent",), False))
                    self.host._call_first(controller, ("set_isolation_open",), True)
                    if direct_vent_ok:
                        command_method = "set_output_false_vent_false_set_isolation_open_fast"
                        command_return_status = 0
                exit_mode = getattr(controller, "exit_atmosphere_mode", None)
                if not command_method and callable(exit_mode):
                    command_method = "exit_atmosphere_mode"
                    self._log_pressure_controller_io("TX", "exit_atmosphere_mode(vent_on=False)")
                    resolved_timeout_s = (
                        float(transition_timeout_s)
                        if transition_timeout_s is not None
                        else float(self.host._cfg_get("workflow.pressure.vent_transition_timeout_s", 30.0))
                    )
                    command_return = exit_mode(
                        timeout_s=resolved_timeout_s
                    )
                    coerced_return = self._coerce_float(command_return)
                    command_return_status = None if coerced_return is None else int(coerced_return)
                    self._log_pressure_controller_io("RX", "ok")
                elif not command_method:
                    self.host._call_first(controller, ("set_output",), False)
                    if self.host._call_first(controller, ("vent",), False):
                        command_method = "set_output_false_vent_false"
                    self.host._call_first(controller, ("set_isolation_open",), True)
                    if command_method:
                        command_return_status = 0
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
            if guard_payload:
                diagnostics.update(guard_payload)
            if emergency_abort_relief:
                diagnostics.update(
                    {
                        "cleanup_vent_classification": "emergency_abort_relief",
                        "emergency_abort_relief_vent_command_sent": trace_result == "ok",
                        "safe_stop_pressure_relief_result": (
                            "command_sent" if trace_result == "ok" else "command_failed"
                        ),
                    }
                )
            elif vent_classification in {"cleanup_relief", "safe_stop_relief"}:
                diagnostics.update(
                    {
                        "cleanup_vent_classification": vent_classification,
                        "cleanup_vent_requested": True,
                        "cleanup_vent_is_normal_maintenance": False,
                        "cleanup_vent_is_safe_stop_relief": True,
                        "safe_stop_relief_command_sent": trace_result == "ok",
                        "safe_stop_pressure_relief_result": (
                            "command_sent" if trace_result == "ok" else "command_failed"
                        ),
                    }
                )
            if not bool(diagnostics.get("atmosphere_ready")) and trace_result == "ok":
                trace_result = "fail"
                trace_message = "pressure_controller_atmosphere_not_verified"
            if emergency_abort_relief and trace_result != "ok":
                diagnostics["safe_stop_pressure_relief_result"] = "command_failed"
        if not vent_on and controller is not None:
            state = (
                self._pressure_controller_state_snapshot(controller)
                if snapshot_after_command
                else self._pressure_controller_fast_state_hint(controller)
            )
            diagnostics = {
                "command_method": command_method,
                "command_error": command_error,
                "vent_command_return_status": command_return_status,
                "command_return_status": command_return_status,
                "vent_command_ack": bool(trace_result == "ok" and command_method and not command_error),
                "snapshot_after_command": bool(snapshot_after_command),
                "prefer_direct_command": bool(prefer_direct_command),
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
        if self._a2_preseal_state_machine_enforced():
            state = self.run_state.pressure
            if not (
                state.final_vent_off_command_sent
                and state.seal_transition_completed
                and str(state.seal_transition_status or "") == "verified_closed"
            ):
                diagnostics = {
                    "hard_blockers": ["seal_not_confirmed_before_output_enable"],
                    "pressure_control_started_after_seal_confirmed": False,
                    "setpoint_command_blocked_before_seal": False,
                    "output_enable_blocked_before_seal": True,
                    "pressure_control_started": False,
                }
                self._record_route_trace(
                    action="set_output",
                    target={"enabled": True},
                    actual=diagnostics,
                    result="blocked",
                    message="Pressure controller output enable blocked before route seal confirmation",
                )
                self.host._log("Pressure controller output enable blocked before route seal confirmation")
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
        vent_diagnostics: dict[str, Any] = {}
        try:
            relief_context = self._a2_emergency_abort_relief_context_for_reason(message)
            vent_classification = (
                "emergency_abort_relief"
                if relief_context
                else self._a2_cleanup_relief_classification_for_reason(message, "normal_maintenance_vent")
            )
            vent_diagnostics = self.set_pressure_controller_vent(
                True,
                reason=message,
                emergency_abort_relief=bool(relief_context),
                emergency_abort_relief_context=relief_context or None,
                vent_classification=vent_classification,
            )
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
        for key in (
            "emergency_abort_relief_vent_required",
            "emergency_abort_relief_vent_allowed",
            "emergency_abort_relief_vent_blocked_reason",
            "emergency_abort_relief_vent_command_sent",
            "emergency_abort_relief_vent_phase",
            "emergency_abort_relief_reason",
            "emergency_abort_relief_pressure_hpa",
            "emergency_abort_relief_route_open",
            "emergency_abort_relief_seal_command_sent",
            "emergency_abort_relief_pressure_setpoint_command_sent",
            "emergency_abort_relief_sample_started",
            "emergency_abort_relief_may_mix_air",
            "cleanup_vent_classification",
            "cleanup_vent_requested",
            "cleanup_vent_phase",
            "cleanup_vent_reason",
            "cleanup_vent_allowed",
            "cleanup_vent_blocked_reason",
            "cleanup_vent_is_normal_maintenance",
            "cleanup_vent_is_safe_stop_relief",
            "safe_stop_relief_required",
            "safe_stop_relief_allowed",
            "safe_stop_relief_command_sent",
            "safe_stop_relief_blocked_reason",
            "vent_blocked_after_flush_phase_is_failure",
            "vent_blocked_after_flush_phase_context",
            "safe_stop_pressure_relief_result",
        ):
            if key in vent_diagnostics:
                summary[key] = vent_diagnostics.get(key)
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
        pressure_now = None if reader is None else (self._read_pressure_with_recovery() or reader())
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
            if self._a2_preseal_state_machine_enforced():
                diagnostics = {
                    "route": self._pressure_route_for_point(point),
                    "target_pressure_hpa": target,
                    "hard_blockers": ["seal_not_confirmed_before_pressure_control"],
                    "seal_command_allowed_after_atmosphere_vent_closed": False,
                    "seal_command_blocked_reason": "seal_not_confirmed_before_pressure_control",
                    "pressure_control_started_after_seal_confirmed": False,
                    "setpoint_command_blocked_before_seal": True,
                    "output_enable_blocked_before_seal": True,
                    "pressure_control_started": False,
                }
                result = PressureWaitResult(
                    ok=False,
                    target_hpa=target,
                    diagnostics=diagnostics,
                    error="Pressure setpoint blocked before route seal confirmation",
                )
                self._record_route_trace(
                    action="set_pressure",
                    point=point,
                    target={"pressure_hpa": target},
                    actual=diagnostics,
                    result="blocked",
                    message=result.error,
                )
                if callable(timing_recorder):
                    timing_recorder(
                        "pressure_timeout",
                        "warning",
                        stage="pressure_setpoint",
                        point=point,
                        target_pressure_hpa=target,
                        expected_max_s=pressure_timeout_s,
                        blocking_condition="seal_not_confirmed_before_pressure_control",
                        decision="blocked_before_seal",
                        error_code=result.error,
                    )
                return result
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
                    actual={
                        "pressure_hpa": final_pressure,
                        "attempt_count": retries_done + 1,
                        "pressure_control_started_after_seal_confirmed": True,
                        "setpoint_command_blocked_before_seal": False,
                        "output_enable_blocked_before_seal": False,
                    },
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
        final_vent_off_diagnostics: dict[str, Any] = {}
        pressure_reader: Optional[Callable[[], Optional[float]]] = None
        positive_preseal = False
        positive_preseal_diagnostics: dict[str, Any] = {}
        preseal_controller_refreshed_after_vent_off = False
        high_pressure_first_point_mode = bool(
            route_text == "co2" and getattr(self.host, "_a2_high_pressure_first_point_mode_enabled", False)
        )
        conditioning_completed_before_high_pressure_mode = bool(
            high_pressure_first_point_mode
            and getattr(self.host, "_a2_co2_route_conditioning_completed", False)
        )
        conditioning_completed_at = str(getattr(self.host, "_a2_co2_route_conditioning_completed_at", "") or "")
        measured_atmospheric_pressure_hpa: Optional[float] = None
        ambient_reference: dict[str, Any] = {}
        if route_text != "h2o":
            pressure_reader = self._make_preseal_observation_reader()
            positive_cfg = self._coerce_bool(
                self.host._cfg_get("workflow.pressure.positive_preseal_pressurization_enabled", None)
            )
            if positive_cfg or high_pressure_first_point_mode:
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
                final_vent_off_diagnostics = dict(
                    self.host._set_pressure_controller_vent(
                        False,
                        reason=f"before {route_text.upper()} pressure seal",
                    )
                    or {}
                )
                refreshed_controller = self.host._device("pressure_controller")
                if refreshed_controller is not None:
                    controller = refreshed_controller
                    preseal_controller_refreshed_after_vent_off = True
                preseal_trigger_threshold_hpa = self._coerce_float(
                    self.host._cfg_get("workflow.pressure.co2_preseal_pressure_gauge_trigger_hpa", 1110.0)
                )
        else:
            final_vent_off_diagnostics = dict(
                self.host._set_pressure_controller_vent(
                    False,
                    reason=f"before {route_text.upper()} pressure seal",
                )
                or {}
            )
            refreshed_controller = self.host._device("pressure_controller")
            if refreshed_controller is not None:
                controller = refreshed_controller
                preseal_controller_refreshed_after_vent_off = True
        if not positive_preseal:
            if wait_after_vent_off_s > 0:
                start = time.time()
                sample_interval_s = min(0.5, wait_after_vent_off_s)
                while True:
                    self.host._check_stop()
                    pressure_now = None if pressure_reader is None else (self._read_pressure_with_recovery() or pressure_reader())
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
            early_seal_command_sent = False
            early_relay_state: Any = None
            if route_text != "h2o" and positive_preseal and high_pressure_first_point_mode:
                ready_to_seal_command_s: Optional[float] = None
                ready_monotonic_s = self._coerce_float(
                    positive_preseal_diagnostics.get("ready_reached_monotonic_s")
                )
                if ready_monotonic_s is not None:
                    ready_to_seal_command_s = max(0.0, time.monotonic() - float(ready_monotonic_s))
                    positive_preseal_diagnostics["ready_to_seal_command_s"] = ready_to_seal_command_s
                seal_started_at = datetime.now(timezone.utc).isoformat()
                positive_preseal_diagnostics["seal_command_sent"] = True
                positive_preseal_diagnostics["seal_command_sent_at"] = seal_started_at
                if callable(timing_recorder):
                    timing_recorder(
                        "high_pressure_seal_command_sent",
                        "info",
                        stage="high_pressure_first_point",
                        point=point,
                        target_pressure_hpa=point.target_pressure_hpa,
                        duration_s=ready_to_seal_command_s,
                        expected_max_s=self.host._cfg_get(
                            "workflow.pressure.expected_ready_to_seal_command_max_s",
                            None,
                        ),
                        pressure_hpa=preseal_trigger_pressure_hpa,
                        decision="seal_command_sent",
                        route_state={
                            "high_pressure_first_point_mode": True,
                            "seal_command_sent_at": seal_started_at,
                            "conditioning_completed_before_high_pressure_mode": conditioning_completed_before_high_pressure_mode,
                            "conditioning_completed_at": conditioning_completed_at,
                            "sealed_after_conditioning": conditioning_completed_before_high_pressure_mode,
                            **positive_preseal_diagnostics,
                        },
                    )
                    timing_recorder(
                        "positive_preseal_seal_start",
                        "info",
                        stage="positive_preseal_pressurization",
                        point=point,
                        target_pressure_hpa=point.target_pressure_hpa,
                        duration_s=ready_to_seal_command_s,
                        expected_max_s=self.host._cfg_get(
                            "workflow.pressure.expected_ready_to_seal_command_max_s",
                            None,
                        ),
                        pressure_hpa=preseal_trigger_pressure_hpa,
                        wait_reason="close_co2_route_valves",
                        route_state={"high_pressure_first_point_mode": True},
                    )
                early_relay_state = self.host._apply_valve_states([])
                early_seal_command_sent = True
            preseal_exit = self._preseal_final_atmosphere_exit_gate(
                controller,
                point,
                route=route_text,
                final_vent_off_command_sent=final_vent_off_command_sent,
                positive_preseal_diagnostics=positive_preseal_diagnostics if positive_preseal else None,
                controller_refreshed_after_vent_off=preseal_controller_refreshed_after_vent_off,
                final_vent_off_diagnostics=final_vent_off_diagnostics,
            )
            if not preseal_exit.ok:
                if positive_preseal and callable(timing_recorder):
                    if high_pressure_first_point_mode:
                        timing_recorder(
                            "high_pressure_abort",
                            "fail",
                            stage="high_pressure_first_point",
                            point=point,
                            target_pressure_hpa=point.target_pressure_hpa,
                            pressure_hpa=preseal_pressure_last,
                            decision="preseal_final_atmosphere_exit_failed",
                            error_code=preseal_exit.error,
                            route_state={"high_pressure_first_point_mode": True},
                        )
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
            if positive_preseal:
                atmosphere_exit_ok = bool(preseal_exit.diagnostics.get("preseal_final_atmosphere_exit_verified"))
                positive_preseal_diagnostics["seal_command_allowed_after_atmosphere_vent_closed"] = atmosphere_exit_ok
                positive_preseal_diagnostics["seal_command_blocked_reason"] = (
                    "" if atmosphere_exit_ok else "atmosphere_vent_not_closed_before_seal"
                )
            if route_text == "h2o":
                self.host._set_h2o_path(False, point)
                relay_state = {}
            else:
                ready_to_seal_command_s: Optional[float] = None
                ready_monotonic_s = self._coerce_float(
                    positive_preseal_diagnostics.get("ready_reached_monotonic_s")
                )
                if positive_preseal and ready_monotonic_s is not None and not early_seal_command_sent:
                    ready_to_seal_command_s = max(0.0, time.monotonic() - float(ready_monotonic_s))
                    positive_preseal_diagnostics["ready_to_seal_command_s"] = ready_to_seal_command_s
                    expected_ready_to_seal_s = self._coerce_float(
                        self.host._cfg_get("workflow.pressure.expected_ready_to_seal_command_max_s", None)
                    )
                    if (
                        expected_ready_to_seal_s is not None
                        and ready_to_seal_command_s > float(expected_ready_to_seal_s)
                        and callable(timing_recorder)
                    ):
                        timing_recorder(
                            "positive_preseal_ready_to_seal_command_delay_warning",
                            "warning",
                            stage="positive_preseal_pressurization",
                            point=point,
                            target_pressure_hpa=point.target_pressure_hpa,
                            duration_s=ready_to_seal_command_s,
                            expected_max_s=expected_ready_to_seal_s,
                            pressure_hpa=preseal_trigger_pressure_hpa,
                            decision="ready_to_seal_command_s_long",
                            warning_code="ready_to_seal_command_s_long",
                        )
                if positive_preseal and callable(timing_recorder) and not early_seal_command_sent:
                    timing_recorder(
                        "positive_preseal_seal_start",
                        "info",
                        stage="positive_preseal_pressurization",
                        point=point,
                        target_pressure_hpa=point.target_pressure_hpa,
                        duration_s=ready_to_seal_command_s,
                        expected_max_s=self.host._cfg_get(
                            "workflow.pressure.expected_ready_to_seal_command_max_s",
                            None,
                        ),
                        pressure_hpa=preseal_trigger_pressure_hpa,
                        wait_reason="close_co2_route_valves",
                    )
                relay_state = early_relay_state if early_seal_command_sent else self.host._apply_valve_states([])
            seal_transition = self._seal_transition_gate(point, route=route_text, relay_state=relay_state)
            if not seal_transition.ok:
                if positive_preseal and callable(timing_recorder):
                    if high_pressure_first_point_mode:
                        timing_recorder(
                            "high_pressure_abort",
                            "fail",
                            stage="high_pressure_first_point",
                            point=point,
                            target_pressure_hpa=point.target_pressure_hpa,
                            pressure_hpa=preseal_pressure_last,
                            decision="seal_transition_failed",
                            error_code=seal_transition.error,
                            route_state={"high_pressure_first_point_mode": True},
                        )
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
                    diagnostics={
                        **preseal_exit.diagnostics,
                        **positive_preseal_diagnostics,
                        **seal_transition.diagnostics,
                    },
                    error=seal_transition.error,
                )
            reader = self.host._make_pressure_reader()
            final_pressure = None if reader is None else (self._read_pressure_with_recovery() or reader())
            watchlist = self._preseal_watchlist_snapshot(
                controller,
                route=route_text,
                final_vent_off_command_sent=final_vent_off_command_sent,
            )
            seal_watchlist = {
                **watchlist,
                "preseal_status_lag_accepted": bool(
                    preseal_exit.diagnostics.get("preseal_status_lag_accepted")
                ),
                "preseal_status_lag_reason": str(
                    preseal_exit.diagnostics.get("preseal_status_lag_reason") or ""
                ),
            }
            if positive_preseal:
                ready_monotonic_s = self._coerce_float(
                    positive_preseal_diagnostics.get("ready_reached_monotonic_s")
                )
                if ready_monotonic_s is not None:
                    positive_preseal_diagnostics["ready_to_seal_confirm_s"] = max(
                        0.0,
                        time.monotonic() - float(ready_monotonic_s),
                    )
                if final_pressure is not None and preseal_trigger_pressure_hpa is not None:
                    positive_preseal_diagnostics["pressure_delta_after_ready_before_seal_hpa"] = (
                        float(final_pressure) - float(preseal_trigger_pressure_hpa)
                    )
                arm_pressure = self._coerce_float(
                    positive_preseal_diagnostics.get("vent_close_arm_pressure_hpa")
                )
                if arm_pressure is not None and preseal_pressure_last is not None:
                    positive_preseal_diagnostics["pressure_delta_during_vent_close_hpa"] = (
                        float(preseal_pressure_last) - float(arm_pressure)
                    )
            if positive_preseal and callable(timing_recorder):
                if high_pressure_first_point_mode:
                    timing_recorder(
                        "high_pressure_seal_confirmed",
                        "end",
                        stage="high_pressure_first_point",
                        point=point,
                        target_pressure_hpa=point.target_pressure_hpa,
                        duration_s=positive_preseal_diagnostics.get("ready_to_seal_confirm_s"),
                        expected_max_s=self.host._cfg_get(
                            "workflow.pressure.expected_ready_to_seal_confirm_max_s",
                            None,
                        ),
                        pressure_hpa=final_pressure,
                        decision="sealed",
                        route_state={"high_pressure_first_point_mode": True},
                    )
                timing_recorder(
                    "positive_preseal_seal_end",
                    "end",
                    stage="positive_preseal_pressurization",
                    point=point,
                    target_pressure_hpa=point.target_pressure_hpa,
                    duration_s=positive_preseal_diagnostics.get("ready_to_seal_confirm_s"),
                    expected_max_s=self.host._cfg_get(
                        "workflow.pressure.expected_ready_to_seal_confirm_max_s",
                        None,
                    ),
                    pressure_hpa=final_pressure,
                    decision="sealed",
                )
            if positive_preseal:
                positive_preseal_diagnostics["pressure_control_started_after_seal_confirmed"] = True
                positive_preseal_diagnostics["preseal_capture_continue_to_control_after_seal"] = True
                positive_preseal_diagnostics["pressure_control_allowed_after_seal_confirmed"] = True
                positive_preseal_diagnostics["pressure_control_target_after_preseal_hpa"] = self.host._as_float(
                    point.target_pressure_hpa
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
                    if high_pressure_first_point_mode:
                        timing_recorder(
                            "high_pressure_abort",
                            "fail",
                            stage="high_pressure_first_point",
                            point=point,
                            target_pressure_hpa=target_pressure_hpa,
                            pressure_hpa=final_pressure,
                            decision="sealed_pressure_below_target",
                            error_code="sealed_pressure_below_target",
                            route_state={"high_pressure_first_point_mode": True, **diagnostics},
                        )
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
                watchlist=seal_watchlist,
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
        controller_refreshed_after_vent_off: bool = False,
        final_vent_off_diagnostics: Optional[Mapping[str, Any]] = None,
    ) -> PressureWaitResult:
        vent_status = self._pressure_controller_vent_status(controller)
        positive = dict(positive_preseal_diagnostics or {})
        final_vent_off = dict(final_vent_off_diagnostics or {})
        route_text = "h2o" if str(route or "").strip().lower() == "h2o" else "co2"
        final_vent_off_snapshot_status = self._coerce_float(
            final_vent_off.get("vent_status_raw", final_vent_off.get("pressure_controller_vent_status"))
        )
        final_vent_off_snapshot_idle = bool(
            final_vent_off_snapshot_status is not None
            and int(final_vent_off_snapshot_status) == 0
            and bool(final_vent_off.get("vent_command_ack"))
            and not str(final_vent_off.get("command_error") or "").strip()
        )
        simulated_controller = self._pressure_controller_is_simulated(controller)
        positive_preseal_pressure_evidence = (
            route_text == "co2"
            and str(positive.get("preseal_trigger") or "") == "positive_preseal_ready"
            and self._coerce_float(positive.get("preseal_trigger_pressure_hpa")) is not None
            and self._coerce_float(positive.get("preseal_pressure_peak_hpa")) is not None
            and int(positive.get("pressure_samples_count") or 0) > 0
        )
        positive_preseal_status_lag_accepted = bool(
            vent_status == 1
            and final_vent_off_command_sent
            and str(positive.get("vent_close_verification_status") or "").upper() == "PASS"
            and bool(positive.get("vent_status_lag_accepted"))
            and bool(positive.get("status_ok_for_positive_preseal"))
            and positive_preseal_pressure_evidence
        )
        simulated_final_vent_off_snapshot_accepted = bool(
            vent_status == 1
            and final_vent_off_command_sent
            and simulated_controller
            and final_vent_off_snapshot_idle
        )
        preseal_status_lag_accepted = bool(
            positive_preseal_status_lag_accepted or simulated_final_vent_off_snapshot_accepted
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
                if positive_preseal_status_lag_accepted
                else (
                    "simulated_final_vent_off_snapshot_idle_accepted_before_full_seal"
                    if simulated_final_vent_off_snapshot_accepted
                    else str(watchlist.get("preseal_watchlist_status_reason") or "vent_exit_verified_before_full_seal")
                )
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
            "preseal_controller_refreshed_after_vent_off": bool(controller_refreshed_after_vent_off),
            "preseal_controller_simulated": bool(simulated_controller),
            "preseal_final_vent_off_snapshot_status": (
                None if final_vent_off_snapshot_status is None else int(final_vent_off_snapshot_status)
            ),
            "preseal_final_vent_off_snapshot_idle": bool(final_vent_off_snapshot_idle),
            "preseal_final_vent_off_snapshot_accepted": bool(simulated_final_vent_off_snapshot_accepted),
            "preseal_final_vent_off_snapshot_acceptance_scope": (
                "simulation_only" if simulated_final_vent_off_snapshot_accepted else ""
            ),
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

    def _pressure_controller_is_simulated(self, controller: Any) -> bool:
        if bool(getattr(controller, "simulated_device", False)):
            return True
        module_name = str(type(controller).__module__ or "")
        type_name = str(type(controller).__name__ or "")
        return bool(
            module_name.startswith("gas_calibrator.v2.sim.")
            or module_name.startswith("gas_calibrator.v2.core.simulated_devices")
            or type_name.lower().startswith(("fake", "simulated"))
        )

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
                warnings.append("vent_status=1 observed after preseal; accepted with prior vent-close and pressure evidence")
                decision_basis.append("vent_status_1_lag_accepted_after_preseal_verification")
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
                sample = self._pressure_sample_payload(method(), source=source)
                return self._coerce_float(sample.get("pressure_hpa"))
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
                sample = self._pressure_sample_payload(
                    {
                        **snapshot,
                        "pressure_hpa": value,
                        "pressure_sample_source": source,
                    },
                    source=source,
                )
                return self._coerce_float(sample.get("pressure_hpa"))
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
        sample = self._current_pressure_sample(source="pressure_gauge")
        return self._coerce_float(sample.get("pressure_hpa"))

    def _record_route_trace(self, **kwargs: Any) -> None:
        status_service = getattr(self.host, "status_service", None)
        recorder = getattr(status_service, "record_route_trace", None)
        if callable(recorder):
            recorder(**kwargs)
