from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Any, Mapping, Optional


class ConditioningService:
    """Config accessors and helpers for A2 route conditioning."""

    def __init__(self, *, host: Any) -> None:
        self.host = host

    def _a2_cfg_bool(self, path: str, default: bool) -> bool:
        val = self.host._cfg_get(path, default)
        if isinstance(val, bool):
            return val
        if isinstance(val, str):
            lowered = val.strip().lower()
            if lowered in {"1", "true", "yes", "y", "on"}:
                return True
            if lowered in {"0", "false", "no", "n", "off"}:
                return False
        return bool(default if val is None else val)

    def _a2_route_open_transient_window_enabled(self) -> bool:
        return self._a2_cfg_bool("workflow.pressure.route_open_transient_window_enabled", True)

    def _a2_route_open_transient_recovery_timeout_s(self) -> float:
        value = self.host._as_float(
            self.host._cfg_get("workflow.pressure.route_open_transient_recovery_timeout_s", 10.0)
        )
        return max(0.1, float(10.0 if value is None else value))

    def _a2_route_open_transient_recovery_band_hpa(self) -> float:
        value = self.host._as_float(
            self.host._cfg_get("workflow.pressure.route_open_transient_recovery_band_hpa", 10.0)
        )
        return max(0.1, float(10.0 if value is None else value))

    def _a2_route_open_transient_stable_hold_s(self) -> float:
        value = self.host._as_float(
            self.host._cfg_get("workflow.pressure.route_open_transient_stable_hold_s", 2.0)
        )
        return max(0.0, float(2.0 if value is None else value))

    def _a2_route_open_transient_stable_span_hpa(self) -> float:
        value = self.host._as_float(
            self.host._cfg_get(
                "workflow.pressure.route_open_transient_stable_pressure_span_hpa",
                self.host._cfg_get(
                    "workflow.pressure.route_open_transient_stable_span_hpa",
                    self._a2_route_open_transient_recovery_band_hpa(),
                ),
            )
        )
        return max(0.1, float(self._a2_route_open_transient_recovery_band_hpa() if value is None else value))

    def _a2_route_open_transient_stable_slope_hpa_per_s(self) -> float:
        value = self.host._as_float(
            self.host._cfg_get("workflow.pressure.route_open_transient_stable_slope_hpa_per_s", 1.0)
        )
        return max(0.0, float(1.0 if value is None else value))

    def _a2_route_open_transient_sustained_rise_min_samples(self) -> int:
        value = self.host._as_float(
            self.host._cfg_get("workflow.pressure.route_open_transient_sustained_rise_min_samples", 3)
        )
        return max(2, int(3 if value is None else value))

    def _a2_conditioning_vent_heartbeat_interval_s(self) -> float:
        value = self.host._as_float(
            self.host._cfg_get(
                "workflow.pressure.atmosphere_vent_heartbeat_interval_s",
                self.host._cfg_get("workflow.pressure.conditioning_vent_heartbeat_interval_s", 1.0),
            )
        )
        return max(0.1, float(1.0 if value is None else value))

    def _a2_conditioning_vent_max_gap_s(self) -> float:
        value = self.host._as_float(
            self.host._cfg_get(
                "workflow.pressure.atmosphere_vent_max_gap_s",
                self.host._cfg_get("workflow.pressure.conditioning_vent_max_gap_s", 3.0),
            )
        )
        return max(0.1, float(3.0 if value is None else value))

    def _a2_conditioning_high_frequency_vent_max_gap_s(self) -> float:
        value = self.host._as_float(
            self.host._cfg_get(
                "workflow.pressure.route_conditioning_high_frequency_max_gap_s",
                self.host._cfg_get("workflow.pressure.conditioning_high_frequency_vent_max_gap_s", 1.0),
            )
        )
        return max(0.1, float(1.0 if value is None else value))

    def _a2_conditioning_vent_maintenance_interval_s(self) -> float:
        value = self.host._as_float(
            self.host._cfg_get(
                "workflow.pressure.route_conditioning_vent_maintenance_interval_s",
                self.host._cfg_get(
                    "workflow.pressure.conditioning_vent_maintenance_interval_s",
                    self._a2_conditioning_vent_heartbeat_interval_s(),
                ),
            )
        )
        return max(0.1, float(1.0 if value is None else value))

    def _a2_conditioning_vent_maintenance_max_gap_s(self) -> float:
        value = self.host._as_float(
            self.host._cfg_get(
                "workflow.pressure.route_conditioning_vent_maintenance_max_gap_s",
                self.host._cfg_get("workflow.pressure.conditioning_vent_maintenance_max_gap_s", 2.0),
            )
        )
        return max(0.1, float(2.0 if value is None else value))

    def _a2_conditioning_scheduler_sleep_step_s(self) -> float:
        value = self.host._as_float(
            self.host._cfg_get(
                "workflow.pressure.route_conditioning_scheduler_sleep_step_s",
                self.host._cfg_get("workflow.pressure.conditioning_scheduler_sleep_step_s", 0.1),
            )
        )
        return min(0.2, max(0.01, float(0.1 if value is None else value)))

    def _a2_conditioning_defer_reschedule_latency_budget_ms(self) -> float:
        value = self.host._as_float(
            self.host._cfg_get(
                "workflow.pressure.route_conditioning_defer_reschedule_latency_budget_ms",
                self.host._cfg_get("workflow.pressure.conditioning_defer_reschedule_latency_budget_ms", 200.0),
            )
        )
        return min(1000.0, max(50.0, float(200.0 if value is None else value)))

    def _a2_conditioning_high_frequency_vent_window_s(self) -> float:
        value = self.host._as_float(
            self.host._cfg_get(
                "workflow.pressure.route_conditioning_high_frequency_vent_window_s",
                self.host._cfg_get("workflow.pressure.conditioning_high_frequency_vent_window_s", 20.0),
            )
        )
        return max(0.0, float(20.0 if value is None else value))

    def _a2_conditioning_high_frequency_vent_interval_s(self) -> float:
        value = self.host._as_float(
            self.host._cfg_get(
                "workflow.pressure.route_conditioning_high_frequency_vent_interval_s",
                self.host._cfg_get("workflow.pressure.conditioning_high_frequency_vent_interval_s", 0.5),
            )
        )
        return max(0.1, float(0.5 if value is None else value))

    def _a2_conditioning_fast_vent_max_duration_s(self) -> float:
        value = self.host._as_float(
            self.host._cfg_get(
                "workflow.pressure.route_conditioning_fast_vent_max_duration_s",
                self.host._cfg_get("workflow.pressure.preseal_atmosphere_hold_reassert_timeout_s", 0.5),
            )
        )
        return max(0.05, float(0.5 if value is None else value))

    def _a2_route_open_transition_block_threshold_s(self) -> float:
        value = self.host._as_float(
            self.host._cfg_get(
                "workflow.pressure.route_open_transition_blocked_vent_scheduler_threshold_s",
                self._a2_conditioning_high_frequency_vent_max_gap_s(),
            )
        )
        return max(0.1, float(self._a2_conditioning_high_frequency_vent_max_gap_s() if value is None else value))

    def _a2_route_open_settle_wait_s(self) -> float:
        value = self.host._as_float(
            self.host._cfg_get(
                "workflow.pressure.route_open_settle_wait_s",
                self.host._cfg_get("workflow.pressure.co2_route_open_settle_wait_s", 0.0),
            )
        )
        return max(0.0, float(0.0 if value is None else value))

    def _a2_route_open_settle_wait_slice_s(self) -> float:
        value = self.host._as_float(
            self.host._cfg_get(
                "workflow.pressure.route_open_settle_wait_slice_s",
                self._a2_conditioning_scheduler_sleep_step_s(),
            )
        )
        return min(0.2, max(0.01, float(self._a2_conditioning_scheduler_sleep_step_s() if value is None else value)))

    def _a2_conditioning_pressure_rise_vent_trigger_hpa(self) -> float:
        value = self.host._as_float(
            self.host._cfg_get(
                "workflow.pressure.route_conditioning_pressure_rise_vent_trigger_hpa",
                self.host._cfg_get("workflow.pressure.pressure_rise_detection_threshold_hpa", 2.0),
            )
        )
        return max(0.1, float(2.0 if value is None else value))

    def _a2_conditioning_pressure_rise_vent_min_interval_s(self) -> float:
        value = self.host._as_float(
            self.host._cfg_get(
                "workflow.pressure.route_conditioning_pressure_rise_vent_min_interval_s",
                self._a2_conditioning_high_frequency_vent_interval_s(),
            )
        )
        return max(0.1, float(self._a2_conditioning_high_frequency_vent_interval_s() if value is None else value))

    def _a2_conditioning_pressure_monitor_interval_s(self) -> float:
        value = self.host._as_float(
            self.host._cfg_get(
                "workflow.pressure.pressure_monitor_interval_s",
                self.host._cfg_get("workflow.pressure.conditioning_pressure_monitor_interval_s", 0.5),
            )
        )
        return max(0.05, float(0.5 if value is None else value))

    def _a2_conditioning_diagnostic_budget_ms(self) -> float:
        value = self.host._as_float(
            self.host._cfg_get(
                "workflow.pressure.route_conditioning_diagnostic_budget_ms",
                self.host._cfg_get("workflow.pressure.conditioning_diagnostic_budget_ms", 100.0),
            )
        )
        return min(200.0, max(10.0, float(100.0 if value is None else value)))

    def _a2_conditioning_pressure_monitor_budget_ms(self) -> float:
        value = self.host._as_float(
            self.host._cfg_get(
                "workflow.pressure.route_conditioning_pressure_monitor_budget_ms",
                self.host._cfg_get(
                    "workflow.pressure.conditioning_pressure_monitor_budget_ms",
                    self._a2_conditioning_diagnostic_budget_ms(),
                ),
            )
        )
        return min(200.0, max(10.0, float(self._a2_conditioning_diagnostic_budget_ms() if value is None else value)))

    def _a2_conditioning_continuous_latest_fresh_budget_ms(self) -> float:
        value = self.host._as_float(
            self.host._cfg_get(
                "workflow.pressure.continuous_latest_fresh_budget_ms",
                self.host._cfg_get("workflow.pressure.conditioning_continuous_latest_fresh_budget_ms", 5.0),
            )
        )
        return min(50.0, max(1.0, float(5.0 if value is None else value)))

    def _a2_conditioning_selected_pressure_sample_stale_budget_ms(self) -> float:
        value = self.host._as_float(
            self.host._cfg_get(
                "workflow.pressure.selected_pressure_sample_stale_budget_ms",
                self.host._cfg_get("workflow.pressure.conditioning_selected_pressure_sample_stale_budget_ms", 10.0),
            )
        )
        return min(50.0, max(1.0, float(10.0 if value is None else value)))

    def _a2_conditioning_monitor_pressure_max_defer_ms(self) -> float:
        value = self.host._as_float(
            self.host._cfg_get(
                "workflow.pressure.conditioning_monitor_pressure_max_defer_ms",
                self.host._cfg_get("workflow.pressure.route_conditioning_pressure_max_defer_ms", 5000.0),
            )
        )
        return max(100.0, float(5000.0 if value is None else value))

    def _a2_conditioning_trace_write_budget_ms(self) -> float:
        value = self.host._as_float(
            self.host._cfg_get(
                "workflow.pressure.route_conditioning_trace_write_budget_ms",
                self.host._cfg_get("workflow.pressure.conditioning_trace_write_budget_ms", 50.0),
            )
        )
        return min(200.0, max(5.0, float(50.0 if value is None else value)))

    def _a2_conditioning_digital_gauge_max_age_s(self) -> float:
        value = self.host._as_float(
            self.host._cfg_get(
                "workflow.pressure.conditioning_digital_gauge_max_age_s",
                self.host._cfg_get("workflow.pressure.digital_gauge_max_age_s", 3.0),
            )
        )
        return max(0.1, float(3.0 if value is None else value))

    def _a2_conditioning_pressure_abort_hpa(self) -> float:
        value = self.host._as_float(
            self.host._cfg_get(
                "workflow.pressure.conditioning_pressure_abort_hpa",
                self.host._cfg_get(
                    "workflow.pressure.preseal_atmosphere_flush_abort_pressure_hpa",
                    self.host._cfg_get("workflow.pressure.preseal_abort_pressure_hpa", 1150.0),
                ),
            )
        )
        return float(1150.0 if value is None else value)

    def _a2_route_conditioning_hard_abort_pressure_hpa(self) -> float:
        value = self.host._as_float(
            self.host._cfg_get(
                "workflow.pressure.route_conditioning_hard_abort_pressure_hpa",
                self.host._cfg_get("workflow.pressure.conditioning_hard_abort_pressure_hpa", None),
            )
        )
        return float(1250.0 if value is None else value)

    def _a2_conditioning_pressure_source_mode(self) -> str:
        value = str(
            self.host._cfg_get(
                "workflow.pressure.a2_conditioning_pressure_source",
                self.host._cfg_get("workflow.pressure.conditioning_pressure_source", "continuous"),
            )
            or "continuous"
        ).strip().lower()
        aliases = {
            "p3": "p3_fast_poll",
            "p3_fast": "p3_fast_poll",
            "fast_poll": "p3_fast_poll",
            "continuous_stream": "continuous",
            "v1": "v1_aligned",
            "v1_aligned_p3": "v1_aligned",
        }
        value = aliases.get(value, value)
        return value if value in {"continuous", "p3_fast_poll", "auto", "v1_aligned"} else "continuous"

    def _a2_prearm_route_conditioning_baseline_max_age_s(self) -> float:
        value = self.host._as_float(
            self.host._cfg_get(
                "workflow.pressure.a2_prearm_route_conditioning_baseline_max_age_s",
                self.host._cfg_get("workflow.pressure.prearm_route_conditioning_baseline_max_age_s", 2.0),
            )
        )
        return min(10.0, max(0.1, float(2.0 if value is None else value)))

    def _a2_prearm_baseline_freshness_max_s(self) -> float:
        value = self.host._as_float(
            self.host._cfg_get(
                "workflow.pressure.a2_prearm_baseline_freshness_max_s",
                self.host._cfg_get(
                    "workflow.pressure.prearm_baseline_freshness_max_s",
                    self.host._cfg_get("workflow.pressure.pressure_sample_stale_threshold_s", 2.0),
                ),
            )
        )
        return min(10.0, max(0.1, float(2.0 if value is None else value)))

    def _a2_prearm_baseline_atmosphere_band_hpa(self) -> float:
        value = self.host._as_float(
            self.host._cfg_get(
                "workflow.pressure.a2_prearm_baseline_atmosphere_band_hpa",
                self.host._cfg_get("workflow.pressure.prearm_baseline_atmosphere_band_hpa", 2.0),
            )
        )
        return min(25.0, max(0.01, float(2.0 if value is None else value)))

    def _a2_preseal_capture_seal_latency_s(self) -> float:
        explicit_latency = self.host._as_float(
            self.host._cfg_get(
                "workflow.pressure.preseal_capture_predictive_seal_latency_s",
                self.host._cfg_get("workflow.pressure.preseal_predictive_seal_latency_s"),
            )
        )
        if explicit_latency is not None:
            return max(0.0, float(explicit_latency))
        command_latency = self.host._as_float(
            self.host._cfg_get("workflow.pressure.expected_ready_to_seal_command_max_s")
        )
        confirm_latency = self.host._as_float(
            self.host._cfg_get("workflow.pressure.expected_ready_to_seal_confirm_max_s")
        )
        if command_latency is None and confirm_latency is None:
            return 0.0
        return max(0.0, float(command_latency or 0.0) + float(confirm_latency or 0.0))

    def _begin_a2_co2_route_conditioning_at_atmosphere(
        self,
        point: "CalibrationPoint",
        pressure_points: Optional[Any] = None,
    ) -> dict[str, Any]:
        self.host.a2_hooks.high_pressure_first_point_mode_enabled = False
        self.host.a2_hooks.high_pressure_first_point_context = {}
        self.host.a2_hooks.high_pressure_first_point_initial_decision = ""
        self.host.a2_hooks.high_pressure_first_point_vent_preclosed = False
        self.host.a2_hooks.co2_route_conditioning_completed = False
        self.host.a2_hooks.co2_route_conditioning_completed_at = ""
        if not self.host._a2_co2_route_conditioning_required(point, pressure_points):
            self.host.a2_hooks.co2_route_conditioning_at_atmosphere_active = False
            self.host.a2_hooks.co2_route_conditioning_at_atmosphere_context = {}
            return {}
        stream_state: dict[str, Any] = {}
        pressure_source_mode = self._a2_conditioning_pressure_source_mode()
        stream_starter = getattr(self.host.pressure_control_service, "_start_a2_high_pressure_digital_gauge_stream", None)
        if pressure_source_mode in {"continuous", "auto", "v1_aligned"} and callable(stream_starter):
            stream_state = dict(
                stream_starter(stage="co2_route_conditioning_at_atmosphere", point_index=point.index) or {}
            )
        now = datetime.now(timezone.utc).isoformat()
        started_monotonic_s = time.monotonic()
        context = {
            "route_open_started_at": "",
            "route_open_completed_at": "",
            "route_open_completed_monotonic_s": None,
            "atmosphere_vent_enabled": True,
            "vent_command_before_route_open": True,
            "route_conditioning_phase": "route_conditioning_flush_phase",
            "ready_to_seal_phase_started": False,
            "route_conditioning_flush_min_time_completed": False,
            "vent_off_blocked_during_flush": True,
            "seal_blocked_during_flush": True,
            "pressure_setpoint_blocked_during_flush": True,
            "sample_blocked_during_flush": True,
            "conditioning_soak_s": self.host._co2_conditioning_soak_s(point),
            "conditioning_started_at": now,
            "conditioning_started_monotonic_s": started_monotonic_s,
            "conditioning_completed_at": "",
            "conditioning_duration_s": None,
            "pressure_monitoring_enabled": True,
            "pressure_max_during_conditioning_hpa": None,
            "pressure_min_during_conditioning_hpa": None,
            "route_conditioning_pressure_returned_to_atmosphere": False,
            "route_conditioning_atmosphere_stable_before_flush": False,
            "route_conditioning_atmosphere_stable_hold_s": None,
            "route_conditioning_high_pressure_seen_before_preseal": False,
            "route_conditioning_high_pressure_seen_before_preseal_hpa": None,
            "route_conditioning_high_pressure_seen_phase": "",
            "route_conditioning_high_pressure_seen_source": "",
            "route_conditioning_high_pressure_seen_sample_age_s": None,
            "route_conditioning_high_pressure_seen_decision": "",
            "pressure_source": (
                "digital_pressure_gauge_p3_fast_poll"
                if pressure_source_mode == "p3_fast_poll"
                else (
                    "digital_pressure_gauge_v1_aligned"
                    if pressure_source_mode == "v1_aligned"
                    else "digital_pressure_gauge_continuous"
                )
            ),
            "pressure_source_selected": pressure_source_mode,
            "pressure_source_selection_reason": "a2_conditioning_pressure_source_config",
            "vent_heartbeat_interval_s": self._a2_conditioning_vent_heartbeat_interval_s(),
            "atmosphere_vent_max_gap_s": self._a2_conditioning_vent_max_gap_s(),
            "vent_heartbeat_gap_exceeded": False,
            "route_conditioning_vent_maintenance_active": True,
            "vent_maintenance_started_at": now,
            "vent_maintenance_started_monotonic_s": started_monotonic_s,
            "route_conditioning_high_frequency_vent_interval_s": (
                self._a2_conditioning_high_frequency_vent_interval_s()
            ),
            "route_conditioning_high_frequency_max_gap_s": (
                self._a2_conditioning_high_frequency_vent_max_gap_s()
            ),
            "route_conditioning_high_frequency_vent_window_s": (
                self._a2_conditioning_high_frequency_vent_window_s()
            ),
            "route_conditioning_vent_maintenance_interval_s": (
                self._a2_conditioning_vent_maintenance_interval_s()
            ),
            "route_conditioning_vent_maintenance_max_gap_s": (
                self._a2_conditioning_vent_maintenance_max_gap_s()
            ),
            "route_conditioning_effective_vent_interval_s": self._a2_conditioning_vent_maintenance_interval_s(),
            "route_conditioning_effective_max_gap_s": self._a2_conditioning_vent_maintenance_max_gap_s(),
            "route_conditioning_vent_gap_exceeded": False,
            "route_open_to_first_vent_s": None,
            "route_open_to_first_vent_ms": None,
            "route_open_to_first_vent_write_ms": None,
            "last_vent_command_age_s": None,
            "pre_route_vent_phase_started": False,
            "pre_route_fast_vent_required": True,
            "pre_route_fast_vent_sent": False,
            "pre_route_fast_vent_duration_ms": None,
            "pre_route_fast_vent_timeout": False,
            "fast_vent_reassert_supported": False,
            "fast_vent_reassert_used": False,
            "vent_command_write_started_at": "",
            "vent_command_write_sent_at": "",
            "vent_command_write_completed_at": "",
            "vent_command_write_started_monotonic_s": None,
            "vent_command_write_sent_monotonic_s": None,
            "vent_command_write_completed_monotonic_s": None,
            "vent_command_write_duration_ms": None,
            "vent_command_total_duration_ms": None,
            "vent_command_wait_after_command_s": 0.0,
            "vent_command_capture_pressure_enabled": False,
            "vent_command_query_state_enabled": False,
            "vent_command_confirm_transition_enabled": False,
            "vent_command_blocking_phase": "",
            "route_conditioning_fast_vent_command_timeout": False,
            "route_conditioning_fast_vent_not_supported": False,
            "route_conditioning_diagnostic_blocked_vent_scheduler": False,
            "vent_scheduler_priority_mode": True,
            "vent_scheduler_checked_before_diagnostic": False,
            "diagnostic_deferred_for_vent_priority": False,
            "diagnostic_deferred_count": 0,
            "diagnostic_budget_ms": self._a2_conditioning_diagnostic_budget_ms(),
            "diagnostic_budget_exceeded": False,
            "diagnostic_blocking_component": "",
            "diagnostic_blocking_operation": "",
            "diagnostic_blocking_duration_ms": None,
            "pressure_monitor_nonblocking": True,
            "pressure_monitor_deferred_for_vent_priority": False,
            "pressure_monitor_budget_ms": self._a2_conditioning_pressure_monitor_budget_ms(),
            "pressure_monitor_duration_ms": None,
            "pressure_monitor_blocked_vent_scheduler": False,
            "conditioning_monitor_pressure_deferred": False,
            "trace_write_budget_ms": self._a2_conditioning_trace_write_budget_ms(),
            "trace_write_duration_ms": None,
            "trace_write_blocked_vent_scheduler": False,
            "trace_write_deferred_for_vent_priority": False,
            "route_open_transition_started": False,
            "route_open_transition_started_at": "",
            "route_open_transition_started_monotonic_s": None,
            "route_open_command_write_started_at": "",
            "route_open_command_write_completed_at": "",
            "route_open_command_write_started_monotonic_s": None,
            "route_open_command_write_completed_monotonic_s": None,
            "route_open_command_write_duration_ms": None,
            "route_open_settle_wait_sliced": False,
            "route_open_settle_wait_slice_count": 0,
            "route_open_settle_wait_total_ms": 0.0,
            "route_open_transition_total_duration_ms": None,
            "vent_ticks_during_route_open_transition": 0,
            "route_open_transition_max_vent_write_gap_ms": None,
            "route_open_transition_terminal_vent_write_age_ms": None,
            "route_open_transition_blocked_vent_scheduler": False,
            "route_open_settle_wait_blocked_vent_scheduler": False,
            "terminal_vent_write_age_ms_at_gap_gate": None,
            "max_vent_pulse_write_gap_ms_including_terminal_gap": None,
            "route_conditioning_vent_gap_exceeded_source": "",
            "terminal_gap_source": "",
            "terminal_gap_operation": "",
            "terminal_gap_duration_ms": None,
            "terminal_gap_started_at": "",
            "terminal_gap_detected_at": "",
            "terminal_gap_stack_marker": "",
            "max_vent_pulse_write_gap_phase": "",
            "max_vent_pulse_write_gap_threshold_ms": self._a2_conditioning_vent_maintenance_max_gap_s() * 1000.0,
            "max_vent_pulse_write_gap_threshold_source": "route_conditioning_vent_maintenance_max_gap_s",
            "max_vent_pulse_write_gap_exceeded": False,
            "max_vent_pulse_write_gap_not_exceeded_reason": "",
            "defer_source": "",
            "defer_operation": "",
            "defer_started_at": "",
            "defer_returned_to_vent_loop": False,
            "defer_to_next_vent_loop_ms": None,
            "defer_reschedule_latency_ms": None,
            "defer_reschedule_latency_budget_ms": self._a2_conditioning_defer_reschedule_latency_budget_ms(),
            "defer_reschedule_latency_exceeded": False,
            "defer_reschedule_latency_warning": False,
            "defer_reschedule_caused_vent_gap_exceeded": False,
            "defer_reschedule_requested": False,
            "defer_reschedule_completed": False,
            "defer_reschedule_reason": "",
            "vent_tick_after_defer_ms": None,
            "fast_vent_after_defer_sent": False,
            "fast_vent_after_defer_write_ms": None,
            "terminal_gap_after_defer": False,
            "terminal_gap_after_defer_ms": None,
            "vent_gap_exceeded_after_defer": False,
            "vent_gap_after_defer_ms": None,
            "vent_gap_after_defer_threshold_ms": None,
            "defer_path_no_reschedule": False,
            "defer_path_no_reschedule_reason": "",
            "fail_closed_path_started": False,
            "fail_closed_path_started_while_route_open": False,
            "fail_closed_path_vent_maintenance_required": False,
            "fail_closed_path_vent_maintenance_active": False,
            "fail_closed_path_duration_ms": None,
            "fail_closed_path_blocked_vent_scheduler": False,
            "route_open_high_frequency_vent_phase_started": False,
            "max_vent_pulse_write_gap_ms": None,
            "max_vent_command_total_duration_ms": None,
            "selected_pressure_source_for_conditioning_monitor": "",
            "selected_pressure_source_for_pressure_gate": "",
            "a2_conditioning_pressure_source_strategy": pressure_source_mode,
            "pressure_monitor_interval_s": self._a2_conditioning_pressure_monitor_interval_s(),
            "digital_gauge_max_age_s": self._a2_conditioning_digital_gauge_max_age_s(),
            "digital_gauge_latest_age_s": None,
            "digital_gauge_sequence_progress": None,
            "digital_gauge_monitoring_required": True,
            "conditioning_pressure_abort_hpa": self._a2_conditioning_pressure_abort_hpa(),
            "route_conditioning_hard_abort_pressure_hpa": (
                self._a2_route_conditioning_hard_abort_pressure_hpa()
            ),
            "route_conditioning_hard_abort_exceeded": False,
            "pressure_overlimit_seen": False,
            "pressure_overlimit_source": "",
            "pressure_overlimit_hpa": None,
            "fail_closed_before_vent_off": False,
            "vent_off_sent_at": "",
            "vent_off_command_sent": False,
            "seal_command_sent": False,
            "pressure_setpoint_command_sent": False,
            "sample_count": 0,
            "points_completed": 0,
            "latest_frame_age_max_s": None,
            "abnormal_pressure_events": [],
            "vent_ticks": [],
            "vent_pulse_count": 0,
            "vent_pulse_interval_ms": [],
            "max_vent_pulse_gap_ms": None,
            "max_vent_pulse_gap_limit_ms": self._a2_conditioning_vent_maintenance_max_gap_s() * 1000.0,
            "vent_scheduler_tick_count": 0,
            "vent_scheduler_loop_gap_ms": [],
            "max_vent_scheduler_loop_gap_ms": None,
            "last_vent_scheduler_tick_monotonic_s": None,
            "pressure_drop_after_vent_hpa": [],
            "pressure_samples": [],
            "route_open_to_first_pressure_read_ms": None,
            "route_open_to_overlimit_ms": None,
            "measured_atmospheric_pressure_hpa": None,
            "route_conditioning_pressure_before_route_open_hpa": None,
            "route_conditioning_pressure_after_route_open_hpa": None,
            "route_conditioning_pressure_rise_rate_hpa_per_s": None,
            "route_conditioning_peak_pressure_hpa": None,
            "latest_route_conditioning_pressure_hpa": None,
            "latest_route_conditioning_pressure_source": "",
            "latest_route_conditioning_pressure_age_s": None,
            "latest_route_conditioning_pressure_recorded_monotonic_s": None,
            "latest_route_conditioning_pressure_eligible_for_prearm_baseline": False,
            "route_conditioning_pressure_overlimit": False,
            "route_open_transient_window_enabled": self._a2_route_open_transient_window_enabled(),
            "route_open_transient_peak_pressure_hpa": None,
            "route_open_transient_peak_time_ms": None,
            "route_open_transient_recovery_required": False,
            "route_open_transient_recovered_to_atmosphere": False,
            "route_open_transient_recovery_time_ms": None,
            "route_open_transient_recovery_target_hpa": None,
            "route_open_transient_recovery_band_hpa": self._a2_route_open_transient_recovery_band_hpa(),
            "route_open_transient_stable_hold_s": self._a2_route_open_transient_stable_hold_s(),
            "route_open_transient_stable_pressure_mean_hpa": None,
            "route_open_transient_stable_pressure_span_hpa": None,
            "route_open_transient_stable_pressure_slope_hpa_per_s": None,
            "route_open_transient_accepted": False,
            "route_open_transient_rejection_reason": "",
            "route_open_transient_evaluation_state": "not_started",
            "route_open_transient_interrupted_by_vent_gap": False,
            "route_open_transient_interrupted_reason": "",
            "route_open_transient_summary_source": "route_conditioning_context",
            "measured_atmospheric_pressure_source": "",
            "measured_atmospheric_pressure_sample_age_s": None,
            "sustained_pressure_rise_after_route_open": False,
            "pressure_rise_despite_valid_vent_scheduler": False,
            "vent_pulse_blocked_after_flush_phase": False,
            "vent_pulse_blocked_reason": "",
            "attempted_unsafe_vent_after_seal_or_pressure_control": False,
            "unsafe_vent_after_seal_or_pressure_control_command_sent": False,
            "conditioning_decision": "START",
            "did_not_seal_during_conditioning": True,
            "stream_state_at_start": stream_state,
            "a2_conditioning_pressure_source": pressure_source_mode,
            "a2_3_pressure_source_strategy": pressure_source_mode,
        }
        self.host.a2_hooks.co2_route_conditioning_at_atmosphere_active = True
        self.host.a2_hooks.co2_route_conditioning_at_atmosphere_context = context
        self.host._record_workflow_timing(
            "co2_route_conditioning_start",
            "start",
            stage="co2_route_conditioning_at_atmosphere",
            point=point,
            expected_max_s=context["conditioning_soak_s"],
            wait_reason="co2_route_conditioning_atmosphere",
            route_state=context,
        )
        self.host._record_a2_co2_conditioning_vent_tick(point, phase="before_route_open")
        return self.host.a2_hooks.co2_route_conditioning_at_atmosphere_context

    def _record_a2_co2_conditioning_vent_tick(self, point: Any, *, phase: str = "") -> dict[str, Any]:
        context = self.host.a2_hooks.co2_route_conditioning_at_atmosphere_context
        if not context:
            return {}
        tick_started_monotonic_s = time.monotonic()
        schedule = self.host._a2_conditioning_vent_schedule(context, now_mono=tick_started_monotonic_s)
        max_gap_s = float(schedule["route_conditioning_effective_max_gap_s"])
        active_interval_s = float(schedule["route_conditioning_effective_vent_interval_s"])
        context.update(schedule)
        defer_started = self.host._as_float(context.get("last_diagnostic_defer_monotonic_s"))
        if defer_started is not None and context.get("vent_tick_after_defer_ms") in (None, ""):
            vent_after_defer_ms = round(max(0.0, tick_started_monotonic_s - float(defer_started)) * 1000.0, 3)
            defer_state = self.host._a2_conditioning_defer_reschedule_state(
                context,
                now_mono=tick_started_monotonic_s,
                max_gap_s=max_gap_s,
                defer_loop_ms=vent_after_defer_ms,
            )
            vent_gap_exceeded = bool(defer_state.get("vent_gap_exceeded_after_defer"))
            operation = str(
                context.get("defer_operation")
                or context.get("last_diagnostic_defer_operation")
                or context.get("diagnostic_blocking_operation")
                or "deferred_diagnostic"
            )
            context.update(defer_state)
            context["defer_to_next_vent_loop_ms"] = context.get("defer_to_next_vent_loop_ms", vent_after_defer_ms)
            context["defer_returned_to_vent_loop"] = True
            context["defer_reschedule_requested"] = True
            context["defer_reschedule_completed"] = not vent_gap_exceeded
            context["defer_reschedule_reason"] = str(
                context.get("defer_reschedule_reason") or f"fast_vent_tick_after_{operation}"
            )
            if vent_gap_exceeded:
                context["terminal_gap_source"] = "defer_path_no_reschedule"
                context["terminal_gap_operation"] = operation
                context["terminal_gap_duration_ms"] = defer_state.get("vent_gap_after_defer_ms")
                context["terminal_gap_detected_at"] = datetime.now(timezone.utc).isoformat()
            elif bool(defer_state.get("defer_reschedule_latency_warning")):
                context = self.host._a2_route_open_transient_mark_continuing_after_defer_warning(context)
        context.setdefault("vent_scheduler_priority_mode", True)
        context.setdefault("diagnostic_budget_ms", self._a2_conditioning_diagnostic_budget_ms())
        context.setdefault("pressure_monitor_budget_ms", self._a2_conditioning_pressure_monitor_budget_ms())
        context.setdefault("trace_write_budget_ms", self._a2_conditioning_trace_write_budget_ms())
        context["route_conditioning_vent_maintenance_active"] = True
        route_open_monotonic = self.host._as_float(
            context.get("route_open_completed_monotonic_s")
            or self.host.a2_hooks.co2_route_open_monotonic_s
        )
        if route_open_monotonic is None:
            context["pre_route_vent_phase_started"] = True
        elif bool(schedule.get("route_conditioning_high_frequency_window_active")):
            context["route_open_high_frequency_vent_phase_started"] = True
        blocked_reason = self.host._a2_conditioning_unsafe_vent_reason(context)
        if blocked_reason:
            # A2.35: seal_command_sent / pressure_setpoint_command_sent means the
            # conditioning flush phase has concluded normally and the vent must be
            # closed.  route_conditioning_phase_not_flush means the 300s flush
            # wait has ended (preseal phase), which is also expected.
            # Do not treat any of these as a failure — just stop vent maintenance.
            _expected_post_flush = blocked_reason in {
                "seal_command_sent",
                "pressure_setpoint_command_sent",
                "route_conditioning_phase_not_flush",
                "ready_to_seal_phase_started",
            }
            if not _expected_post_flush:
                blocked_context = self.host._a2_conditioning_mark_vent_blocked(context, reason=blocked_reason)
                self.host._fail_a2_co2_route_conditioning_closed(
                    point,
                    reason="unsafe_vent_after_flush_phase_blocked",
                    details={
                        **blocked_context,
                        "phase": phase,
                        "vent_command_sent": False,
                        "command_result": "blocked",
                        "command_error": blocked_reason,
                    },
                    event_name="co2_route_conditioning_vent_blocked_after_flush_phase",
                    route_trace_action="co2_route_conditioning_vent_blocked_after_flush_phase",
                )
            else:
                context["vent_maintenance_ended_on_post_flush_transition"] = True
                context["vent_maintenance_ended_reason"] = blocked_reason
                self.host.a2_hooks.co2_route_conditioning_at_atmosphere_context = context
        gap_state = self.host._a2_conditioning_heartbeat_gap_state(
            context,
            now_mono=tick_started_monotonic_s,
            max_gap_s=max_gap_s,
            interval_s=active_interval_s,
        )
        vent_gap_s = self.host._as_float(gap_state.get("vent_heartbeat_gap_s"))
        emission_gap_s = self.host._as_float(gap_state.get("heartbeat_emission_gap_s"))
        effective_gap_s = vent_gap_s if vent_gap_s is not None else emission_gap_s
        first_after_route_vent = bool(
            route_open_monotonic is not None
            and context.get("route_open_to_first_vent_s") in (None, "")
            and str(phase or "") == "after_route_open"
        )
        if route_open_monotonic is not None and effective_gap_s is not None and not first_after_route_vent:
            if effective_gap_s > max_gap_s:
                gap_ms = round(float(effective_gap_s) * 1000.0, 3)
                source = self.host._a2_conditioning_vent_gap_source(context)
                terminal = self.host._a2_conditioning_terminal_gap_details(
                    context,
                    now_mono=tick_started_monotonic_s,
                    max_gap_s=max_gap_s,
                    source=source,
                )
                self.host._fail_a2_co2_route_conditioning_closed(
                    point,
                    reason="route_conditioning_vent_gap_exceeded",
                    details={
                        "phase": phase,
                        **gap_state,
                        "vent_heartbeat_gap_s": round(float(effective_gap_s), 3),
                        "vent_heartbeat_interval_s": active_interval_s,
                        "atmosphere_vent_max_gap_s": max_gap_s,
                        "vent_heartbeat_gap_exceeded": True,
                        "route_conditioning_vent_gap_exceeded": True,
                        "max_vent_pulse_gap_ms": gap_ms,
                        "max_vent_pulse_gap_limit_ms": round(max_gap_s * 1000.0, 3),
                        **terminal,
                        "last_vent_command_age_s": round(float(effective_gap_s), 3),
                        "fail_closed_reason": "route_conditioning_vent_gap_exceeded",
                        "whether_safe_to_continue": False,
                        **schedule,
                    },
                    event_name="co2_route_conditioning_vent_heartbeat_gap",
                    route_trace_action="co2_route_conditioning_vent_heartbeat_gap",
                )
        route_open_to_first_vent_s = None
        if route_open_monotonic is not None and context.get("route_open_to_first_vent_s") in (None, ""):
            route_open_to_first_vent_s = max(0.0, tick_started_monotonic_s - float(route_open_monotonic))
            if route_open_to_first_vent_s > max_gap_s:
                gap_ms = round(float(route_open_to_first_vent_s) * 1000.0, 3)
                source = self.host._a2_conditioning_vent_gap_source(context)
                terminal = self.host._a2_conditioning_terminal_gap_details(
                    context,
                    now_mono=tick_started_monotonic_s,
                    max_gap_s=max_gap_s,
                    source=source,
                )
                self.host._fail_a2_co2_route_conditioning_closed(
                    point,
                    reason="route_conditioning_vent_gap_exceeded",
                    details={
                        "phase": phase,
                        "route_open_to_first_vent_s": round(float(route_open_to_first_vent_s), 3),
                        "route_open_to_first_vent_ms": gap_ms,
                        "atmosphere_vent_max_gap_s": max_gap_s,
                        "vent_heartbeat_gap_exceeded": True,
                        "route_conditioning_vent_gap_exceeded": True,
                        "max_vent_pulse_gap_ms": gap_ms,
                        "max_vent_pulse_gap_limit_ms": round(max_gap_s * 1000.0, 3),
                        **terminal,
                        **schedule,
                    },
                    event_name="co2_route_conditioning_route_open_first_vent_gap",
                    route_trace_action="co2_route_conditioning_route_open_first_vent_gap",
                )
        diagnostics: dict[str, Any] = {}
        command_result = "ok"
        command_error = ""
        try:
            fast_reassert = getattr(self.host.pressure_control_service, "set_pressure_controller_vent_fast_reassert", None)
            if not callable(fast_reassert):
                diagnostics = {
                    "command_result": "unsupported",
                    "command_error": "route_conditioning_fast_vent_not_supported",
                    "route_conditioning_fast_vent_not_supported": True,
                    "fast_vent_reassert_supported": False,
                    "fast_vent_reassert_used": False,
                }
            else:
                diagnostics = fast_reassert(
                    True,
                    reason="A2 route conditioning fast vent maintenance",
                    max_duration_s=self._a2_conditioning_fast_vent_max_duration_s(),
                    wait_after_command=False,
                    capture_pressure=False,
                    query_state=False,
                    confirm_transition=False,
                )
        except Exception as exc:
            command_result = "fail"
            command_error = str(exc)
            diagnostics = {"command_error": command_error}
        diagnostics = dict(diagnostics or {})
        if bool(diagnostics.get("vent_command_blocked")):
            command_result = "blocked"
            command_error = str(diagnostics.get("vent_pulse_blocked_reason") or "vent_command_blocked")
        elif str(diagnostics.get("command_result") or "").lower() in {"fail", "failed", "timeout", "unsupported"}:
            command_result = str(diagnostics.get("command_result") or "fail").lower()
            command_error = str(diagnostics.get("command_error") or command_result)
        tick_completed_monotonic_s = time.monotonic()
        blocking_duration_s = max(0.0, tick_completed_monotonic_s - tick_started_monotonic_s)
        write_sent_monotonic_s = self.host._as_float(
            diagnostics.get("vent_command_write_sent_monotonic_s")
            or diagnostics.get("vent_command_write_started_monotonic_s")
            or tick_started_monotonic_s
        )
        pressure_hpa = None
        latest_age_s = None
        continuous_age_s = context.get("digital_gauge_latest_age_s")
        abort_hpa = context.get("conditioning_pressure_abort_hpa")
        hard_abort_hpa = context.get(
            "route_conditioning_hard_abort_pressure_hpa",
            self._a2_route_conditioning_hard_abort_pressure_hpa(),
        )
        pressure_abnormal = bool(
            context.get("pressure_overlimit_seen")
            or context.get("route_conditioning_pressure_overlimit")
            or context.get("route_conditioning_hard_abort_exceeded")
        )
        selected_freshness_ok = True
        sample_stale = False
        elapsed_s = max(0.0, time.monotonic() - float(context.get("conditioning_started_monotonic_s") or tick_started_monotonic_s))
        context = self.host._a2_conditioning_update_pressure_metrics(
            context,
            phase=phase,
            pressure_hpa=pressure_hpa,
            event_monotonic_s=float(write_sent_monotonic_s or tick_started_monotonic_s),
            vent_command_sent=True,
            vent_command_write_sent_monotonic_s=write_sent_monotonic_s,
        )
        if bool(diagnostics.get("route_conditioning_fast_vent_command_timeout")):
            context["route_conditioning_fast_vent_command_timeout"] = True
            if phase.startswith("pre_route") or route_open_monotonic is None:
                context["pre_route_fast_vent_timeout"] = True
        if bool(diagnostics.get("route_conditioning_fast_vent_not_supported")):
            context["route_conditioning_fast_vent_not_supported"] = True
        if route_open_monotonic is None:
            context["pre_route_fast_vent_sent"] = bool(command_result == "ok")
            context["pre_route_fast_vent_duration_ms"] = diagnostics.get("vent_command_total_duration_ms")
        if bool(context.get("route_open_transition_started")) and not bool(
            context.get("route_open_transition_completed", False)
        ):
            previous_transition_write = self.host._as_float(
                context.get("_route_open_transition_last_vent_write_sent_monotonic_s")
            )
            if previous_transition_write is not None and write_sent_monotonic_s is not None:
                transition_gap_ms = round(
                    max(0.0, float(write_sent_monotonic_s) - float(previous_transition_write)) * 1000.0,
                    3,
                )
                previous_transition_max = self.host._as_float(context.get("route_open_transition_max_vent_write_gap_ms"))
                context["route_open_transition_max_vent_write_gap_ms"] = (
                    transition_gap_ms
                    if previous_transition_max is None
                    else max(float(previous_transition_max), transition_gap_ms)
                )
            if write_sent_monotonic_s is not None:
                context["_route_open_transition_last_vent_write_sent_monotonic_s"] = float(write_sent_monotonic_s)
            context["vent_ticks_during_route_open_transition"] = (
                int(context.get("vent_ticks_during_route_open_transition") or 0) + 1
            )
            context["route_open_transition_terminal_vent_write_age_ms"] = 0.0
        context.update(
            {
                "fast_vent_reassert_supported": bool(diagnostics.get("fast_vent_reassert_supported")),
                "fast_vent_reassert_used": bool(diagnostics.get("fast_vent_reassert_used")),
                "vent_command_write_started_at": diagnostics.get("vent_command_write_started_at", ""),
                "vent_command_write_sent_at": diagnostics.get("vent_command_write_sent_at", ""),
                "vent_command_write_completed_at": diagnostics.get("vent_command_write_completed_at", ""),
                "vent_command_write_started_monotonic_s": diagnostics.get("vent_command_write_started_monotonic_s"),
                "vent_command_write_sent_monotonic_s": diagnostics.get("vent_command_write_sent_monotonic_s"),
                "vent_command_write_completed_monotonic_s": diagnostics.get("vent_command_write_completed_monotonic_s"),
                "vent_command_write_duration_ms": diagnostics.get("vent_command_write_duration_ms"),
                "vent_command_total_duration_ms": diagnostics.get("vent_command_total_duration_ms"),
                "vent_command_wait_after_command_s": diagnostics.get("vent_command_wait_after_command_s"),
                "vent_command_capture_pressure_enabled": bool(diagnostics.get("vent_command_capture_pressure_enabled")),
                "vent_command_query_state_enabled": bool(diagnostics.get("vent_command_query_state_enabled")),
                "vent_command_confirm_transition_enabled": bool(diagnostics.get("vent_command_confirm_transition_enabled")),
                "vent_command_blocking_phase": diagnostics.get("vent_command_blocking_phase", ""),
                "selected_pressure_source_for_conditioning_monitor": context.get("selected_pressure_source") or "",
                "a2_conditioning_pressure_source_strategy": context.get("a2_conditioning_pressure_source", self._a2_conditioning_pressure_source_mode()),
            }
        )
        if defer_started is not None:
            context["fast_vent_after_defer_sent"] = bool(command_result == "ok")
            context["fast_vent_after_defer_write_ms"] = (
                diagnostics.get("vent_command_write_duration_ms")
                or diagnostics.get("vent_command_total_duration_ms")
            )
        tick = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "phase": phase,
            "elapsed_s": round(elapsed_s, 3),
            "route_conditioning_phase": context.get("route_conditioning_phase", "route_conditioning_flush_phase"),
            "ready_to_seal_phase_started": bool(context.get("ready_to_seal_phase_started", False)),
            "route_conditioning_flush_min_time_completed": bool(
                context.get("route_conditioning_flush_min_time_completed", False)
            ),
            "vent_off_blocked_during_flush": bool(context.get("vent_off_blocked_during_flush", True)),
            "seal_blocked_during_flush": bool(context.get("seal_blocked_during_flush", True)),
            "pressure_setpoint_blocked_during_flush": bool(
                context.get("pressure_setpoint_blocked_during_flush", True)
            ),
            "sample_blocked_during_flush": bool(context.get("sample_blocked_during_flush", True)),
            "vent_off_command_sent": bool(context.get("vent_off_command_sent", False)),
            "seal_command_sent": bool(context.get("seal_command_sent", False)),
            "pressure_setpoint_command_sent": bool(context.get("pressure_setpoint_command_sent", False)),
            "vent_command_sent": True,
            "route_conditioning_vent_maintenance_active": True,
            "vent_maintenance_started_at": context.get("vent_maintenance_started_at"),
            "vent_maintenance_started_monotonic_s": context.get("vent_maintenance_started_monotonic_s"),
            "route_conditioning_high_frequency_vent_interval_s": self._a2_conditioning_high_frequency_vent_interval_s(),
            "route_conditioning_high_frequency_max_gap_s": self._a2_conditioning_high_frequency_vent_max_gap_s(),
            "route_conditioning_high_frequency_vent_window_s": self._a2_conditioning_high_frequency_vent_window_s(),
            "route_conditioning_vent_maintenance_interval_s": self._a2_conditioning_vent_maintenance_interval_s(),
            "route_conditioning_vent_maintenance_max_gap_s": self._a2_conditioning_vent_maintenance_max_gap_s(),
            "route_conditioning_high_frequency_window_active": schedule.get(
                "route_conditioning_high_frequency_window_active"
            ),
            "route_conditioning_effective_vent_interval_s": schedule.get(
                "route_conditioning_effective_vent_interval_s"
            ),
            "route_conditioning_effective_max_gap_s": schedule.get("route_conditioning_effective_max_gap_s"),
            "max_vent_pulse_gap_limit_ms": schedule.get("max_vent_pulse_gap_limit_ms"),
            "vent_pulse_count": context.get("vent_pulse_count"),
            "vent_pulse_interval_ms": context.get("vent_pulse_interval_ms"),
            "max_vent_pulse_gap_ms": context.get("max_vent_pulse_gap_ms"),
            "vent_scheduler_tick_count": context.get("vent_scheduler_tick_count", 0),
            "vent_scheduler_loop_gap_ms": context.get("vent_scheduler_loop_gap_ms", []),
            "max_vent_scheduler_loop_gap_ms": context.get("max_vent_scheduler_loop_gap_ms"),
            "pressure_drop_after_vent_hpa": context.get("last_pressure_drop_after_vent_hpa"),
            "pressure_drop_after_vent_hpa_all": context.get("pressure_drop_after_vent_hpa"),
            "command_result": command_result,
            "command_error": command_error,
            "vent_pulse_blocked_after_flush_phase": bool(
                context.get("vent_pulse_blocked_after_flush_phase", False)
            ),
            "vent_pulse_blocked_reason": context.get("vent_pulse_blocked_reason", ""),
            "attempted_unsafe_vent_after_seal_or_pressure_control": bool(
                context.get("attempted_unsafe_vent_after_seal_or_pressure_control", False)
            ),
            "unsafe_vent_after_seal_or_pressure_control_command_sent": False,
            "output_state": diagnostics.get("output_state"),
            "isolation_state": diagnostics.get("isolation_state"),
            "vent_status": diagnostics.get("vent_status_raw"),
            "pre_route_vent_phase_started": bool(context.get("pre_route_vent_phase_started", False)),
            "pre_route_fast_vent_required": bool(context.get("pre_route_fast_vent_required", True)),
            "pre_route_fast_vent_sent": bool(context.get("pre_route_fast_vent_sent", False)),
            "pre_route_fast_vent_duration_ms": context.get("pre_route_fast_vent_duration_ms"),
            "pre_route_fast_vent_timeout": bool(context.get("pre_route_fast_vent_timeout", False)),
            "fast_vent_reassert_supported": bool(context.get("fast_vent_reassert_supported", False)),
            "fast_vent_reassert_used": bool(context.get("fast_vent_reassert_used", False)),
            "vent_command_write_started_at": context.get("vent_command_write_started_at", ""),
            "vent_command_write_sent_at": context.get("vent_command_write_sent_at", ""),
            "vent_command_write_completed_at": context.get("vent_command_write_completed_at", ""),
            "vent_command_write_duration_ms": context.get("vent_command_write_duration_ms"),
            "vent_command_total_duration_ms": context.get("vent_command_total_duration_ms"),
            "vent_command_wait_after_command_s": context.get("vent_command_wait_after_command_s"),
            "vent_command_capture_pressure_enabled": bool(context.get("vent_command_capture_pressure_enabled", False)),
            "vent_command_query_state_enabled": bool(context.get("vent_command_query_state_enabled", False)),
            "vent_command_confirm_transition_enabled": bool(
                context.get("vent_command_confirm_transition_enabled", False)
            ),
            "vent_command_blocking_phase": context.get("vent_command_blocking_phase", ""),
            "route_conditioning_fast_vent_command_timeout": bool(
                context.get("route_conditioning_fast_vent_command_timeout", False)
            ),
            "route_conditioning_fast_vent_not_supported": bool(
                context.get("route_conditioning_fast_vent_not_supported", False)
            ),
            "route_conditioning_diagnostic_blocked_vent_scheduler": bool(
                context.get("route_conditioning_diagnostic_blocked_vent_scheduler", False)
            ),
            "route_open_transition_started": bool(context.get("route_open_transition_started", False)),
            "route_open_transition_started_at": context.get("route_open_transition_started_at", ""),
            "route_open_transition_started_monotonic_s": context.get(
                "route_open_transition_started_monotonic_s"
            ),
            "route_open_command_write_started_at": context.get("route_open_command_write_started_at", ""),
            "route_open_command_write_completed_at": context.get("route_open_command_write_completed_at", ""),
            "route_open_command_write_duration_ms": context.get("route_open_command_write_duration_ms"),
            "route_open_settle_wait_sliced": bool(context.get("route_open_settle_wait_sliced", False)),
            "route_open_settle_wait_slice_count": int(context.get("route_open_settle_wait_slice_count") or 0),
            "route_open_settle_wait_total_ms": context.get("route_open_settle_wait_total_ms"),
            "route_open_transition_total_duration_ms": context.get("route_open_transition_total_duration_ms"),
            "vent_ticks_during_route_open_transition": int(
                context.get("vent_ticks_during_route_open_transition") or 0
            ),
            "route_open_transition_max_vent_write_gap_ms": context.get(
                "route_open_transition_max_vent_write_gap_ms"
            ),
            "route_open_transition_terminal_vent_write_age_ms": context.get(
                "route_open_transition_terminal_vent_write_age_ms"
            ),
            "route_open_transition_blocked_vent_scheduler": bool(
                context.get("route_open_transition_blocked_vent_scheduler", False)
            ),
            "route_open_settle_wait_blocked_vent_scheduler": bool(
                context.get("route_open_settle_wait_blocked_vent_scheduler", False)
            ),
            "terminal_vent_write_age_ms_at_gap_gate": context.get(
                "terminal_vent_write_age_ms_at_gap_gate"
            ),
            "max_vent_pulse_write_gap_ms_including_terminal_gap": context.get(
                "max_vent_pulse_write_gap_ms_including_terminal_gap"
            ),
            "route_conditioning_vent_gap_exceeded_source": context.get(
                "route_conditioning_vent_gap_exceeded_source",
                "",
            ),
            "route_open_high_frequency_vent_phase_started": bool(
                context.get("route_open_high_frequency_vent_phase_started", False)
            ),
            "route_open_to_first_vent_write_ms": context.get("route_open_to_first_vent_write_ms"),
            "max_vent_pulse_write_gap_ms": context.get("max_vent_pulse_write_gap_ms"),
            "max_vent_command_total_duration_ms": context.get("max_vent_command_total_duration_ms"),
            "digital_gauge_pressure_hpa": pressure_hpa,
            "pressure_hpa": pressure_hpa,
            "pressure_sample_source": context.get("selected_pressure_source") or context.get("pressure_source_selected"),
            "pressure_sample_age_s": latest_age_s,
            "digital_gauge_latest_age_s": continuous_age_s,
            "pressure_sample_stale": sample_stale,
            "pressure_freshness_ok": selected_freshness_ok,
            "pressure_abnormal": pressure_abnormal,
            "abort_pressure_hpa": abort_hpa,
            "conditioning_pressure_abort_hpa": abort_hpa,
            "route_conditioning_hard_abort_pressure_hpa": hard_abort_hpa,
            "route_conditioning_hard_abort_exceeded": bool(
                context.get("route_conditioning_hard_abort_exceeded", False)
            ),
            "pressure_overlimit_seen": pressure_abnormal,
            "pressure_overlimit_source": context.get("pressure_overlimit_source"),
            "pressure_overlimit_hpa": context.get("pressure_overlimit_hpa"),
            "latest_frame_sequence_id": context.get("latest_frame_sequence_id"),
            "digital_gauge_sequence_progress": context.get("digital_gauge_sequence_progress"),
            "stream_stale": sample_stale,
            "latest_frame_interval_s": context.get("latest_frame_interval_s"),
            "stream_frame_count": context.get("stream_frame_count"),
            "vent_heartbeat_interval_s": self._a2_conditioning_vent_heartbeat_interval_s(),
            "atmosphere_vent_max_gap_s": max_gap_s,
            "vent_heartbeat_gap_s": None if vent_gap_s is None else round(float(vent_gap_s), 3),
            "vent_heartbeat_gap_exceeded": False,
            "route_conditioning_vent_gap_exceeded": False,
            "heartbeat_gap_threshold_ms": gap_state.get("heartbeat_gap_threshold_ms"),
            "heartbeat_gap_observed_ms": gap_state.get("heartbeat_gap_observed_ms"),
            "heartbeat_emission_gap_ms": gap_state.get("heartbeat_emission_gap_ms"),
            "heartbeat_gap_explained_by_blocking_operation": gap_state.get(
                "heartbeat_gap_explained_by_blocking_operation"
            ),
            "blocking_operation_name": "a2_conditioning_vent_tick",
            "blocking_operation_duration_ms": round(blocking_duration_s * 1000.0, 3),
            "whether_safe_to_continue": bool(command_result == "ok" and not pressure_abnormal and selected_freshness_ok),
            "route_open_to_first_vent_s": context.get("route_open_to_first_vent_s"),
            "route_open_to_first_vent_ms": context.get("route_open_to_first_vent_ms"),
            "route_open_to_first_pressure_read_ms": context.get("route_open_to_first_pressure_read_ms"),
            "route_open_to_overlimit_ms": context.get("route_open_to_overlimit_ms"),
            "route_conditioning_pressure_before_route_open_hpa": context.get(
                "route_conditioning_pressure_before_route_open_hpa"
            ),
            "route_conditioning_pressure_after_route_open_hpa": context.get(
                "route_conditioning_pressure_after_route_open_hpa"
            ),
            "route_conditioning_pressure_rise_rate_hpa_per_s": context.get(
                "route_conditioning_pressure_rise_rate_hpa_per_s"
            ),
            "route_conditioning_peak_pressure_hpa": context.get("route_conditioning_peak_pressure_hpa"),
            "route_conditioning_pressure_overlimit": bool(context.get("route_conditioning_pressure_overlimit", False)),
            **self.host._a2_route_open_transient_evidence(context),
            "pressure_rise_since_last_vent_hpa": context.get("pressure_rise_since_last_vent_hpa"),
            "pressure_monitor_interval_s": self._a2_conditioning_pressure_monitor_interval_s(),
            "selected_pressure_source_for_conditioning_monitor": context.get(
                "selected_pressure_source_for_conditioning_monitor",
                "",
            ),
            "selected_pressure_source_for_pressure_gate": context.get("selected_pressure_source_for_pressure_gate", ""),
            "a2_conditioning_pressure_source_strategy": context.get(
                "a2_conditioning_pressure_source_strategy",
                self._a2_conditioning_pressure_source_mode(),
            ),
            **self.host._a2_conditioning_scheduler_evidence(context),
            **self.host._a2_conditioning_digital_gauge_evidence(context),
        }
        ticks = [item for item in list(context.get("vent_ticks") or []) if isinstance(item, Mapping)]
        ticks.append(tick)
        context["vent_ticks"] = ticks
        context["last_vent_tick_monotonic_s"] = float(write_sent_monotonic_s or tick_started_monotonic_s)
        context["last_vent_heartbeat_started_monotonic_s"] = float(write_sent_monotonic_s or tick_started_monotonic_s)
        context["last_vent_heartbeat_completed_monotonic_s"] = tick_completed_monotonic_s
        context["last_vent_tick_completed_monotonic_s"] = tick_completed_monotonic_s
        if context.get("last_pressure_monitor_monotonic_s") in (None, ""):
            context["last_pressure_monitor_monotonic_s"] = tick_completed_monotonic_s
        context["last_vent_command_age_s"] = 0.0
        context["last_vent_command_duration_s"] = blocking_duration_s
        context["last_blocking_operation_name"] = "a2_conditioning_vent_tick"
        context["last_blocking_operation_started_monotonic_s"] = tick_started_monotonic_s
        context["last_blocking_operation_completed_monotonic_s"] = tick_completed_monotonic_s
        context["last_blocking_operation_duration_s"] = blocking_duration_s
        context["last_blocking_operation_safe_to_continue"] = bool(
            command_result == "ok" and not pressure_abnormal and selected_freshness_ok
        )
        context["vent_heartbeat_interval_s"] = self._a2_conditioning_vent_heartbeat_interval_s()
        context["atmosphere_vent_max_gap_s"] = max_gap_s
        context["route_conditioning_high_frequency_vent_interval_s"] = self._a2_conditioning_high_frequency_vent_interval_s()
        context["route_conditioning_high_frequency_max_gap_s"] = self._a2_conditioning_high_frequency_vent_max_gap_s()
        context["route_conditioning_high_frequency_vent_window_s"] = self._a2_conditioning_high_frequency_vent_window_s()
        context["route_conditioning_vent_maintenance_interval_s"] = self._a2_conditioning_vent_maintenance_interval_s()
        context["route_conditioning_vent_maintenance_max_gap_s"] = self._a2_conditioning_vent_maintenance_max_gap_s()
        context["route_conditioning_effective_vent_interval_s"] = active_interval_s
        context["route_conditioning_effective_max_gap_s"] = max_gap_s
        context["max_vent_pulse_gap_limit_ms"] = round(max_gap_s * 1000.0, 3)
        context["route_conditioning_vent_maintenance_active"] = True
        context["pressure_monitor_interval_s"] = self._a2_conditioning_pressure_monitor_interval_s()
        if context.get("route_open_to_first_vent_write_ms") not in (None, ""):
            context["route_open_to_first_vent_ms"] = context.get("route_open_to_first_vent_write_ms")
        samples = [item for item in list(context.get("pressure_samples") or []) if isinstance(item, Mapping)]
        samples.append(tick)
        context["pressure_samples"] = samples
        pressure_values = [
            self.host._as_float(item.get("digital_gauge_pressure_hpa", item.get("pressure_hpa")))
            for item in samples
            if isinstance(item, Mapping)
            and self.host._as_float(item.get("digital_gauge_pressure_hpa", item.get("pressure_hpa"))) is not None
        ]
        if pressure_values:
            context["pressure_max_during_conditioning_hpa"] = max(float(value) for value in pressure_values if value is not None)
            context["pressure_min_during_conditioning_hpa"] = min(float(value) for value in pressure_values if value is not None)
        if continuous_age_s is not None:
            context["digital_gauge_latest_age_s"] = continuous_age_s
            previous_max = self.host._as_float(context.get("latest_frame_age_max_s"))
            context["latest_frame_age_max_s"] = (
                continuous_age_s if previous_max is None else max(float(previous_max), float(continuous_age_s))
            )
        if context.get("latest_frame_sequence_id") is not None:
            context["last_digital_gauge_sequence_id"] = context.get("latest_frame_sequence_id")
        context.update(self.host._a2_conditioning_digital_gauge_evidence(context))
        context["conditioning_pressure_abort_hpa"] = abort_hpa
        context["route_conditioning_hard_abort_pressure_hpa"] = hard_abort_hpa
        context["pressure_overlimit_seen"] = bool(context.get("pressure_overlimit_seen") or pressure_abnormal)
        if pressure_abnormal:
            context["pressure_overlimit_source"] = context.get("pressure_overlimit_source")
            context["pressure_overlimit_hpa"] = context.get("pressure_overlimit_hpa")
            context["route_conditioning_high_pressure_seen_before_preseal"] = True
            context["route_conditioning_high_pressure_seen_before_preseal_hpa"] = context.get(
                "route_conditioning_high_pressure_seen_before_preseal_hpa",
                context.get("pressure_overlimit_hpa"),
            )
            context["route_conditioning_high_pressure_seen_phase"] = context.get(
                "route_conditioning_high_pressure_seen_phase",
                "co2_route_conditioning_at_atmosphere",
            )
            context["route_conditioning_high_pressure_seen_source"] = context.get(
                "route_conditioning_high_pressure_seen_source",
                context.get("pressure_overlimit_source"),
            )
            context["route_conditioning_high_pressure_seen_decision"] = context.get(
                "route_conditioning_high_pressure_seen_decision",
                "fail_closed",
            )
        if pressure_abnormal or command_result != "ok":
            abnormal_events = list(context.get("abnormal_pressure_events") or [])
            abnormal_events.append(tick)
            context["abnormal_pressure_events"] = abnormal_events
        context = self.host._a2_conditioning_context_with_counts(context)
        tick.update(
            {
                "max_vent_pulse_gap_ms": context.get("max_vent_pulse_gap_ms"),
                "max_vent_pulse_write_gap_ms": context.get("max_vent_pulse_write_gap_ms"),
                "max_vent_pulse_write_gap_ms_including_terminal_gap": context.get(
                    "max_vent_pulse_write_gap_ms_including_terminal_gap"
                ),
                "max_vent_pulse_write_gap_phase": context.get("max_vent_pulse_write_gap_phase"),
                "max_vent_pulse_write_gap_threshold_ms": context.get(
                    "max_vent_pulse_write_gap_threshold_ms"
                ),
                "max_vent_pulse_write_gap_threshold_source": context.get(
                    "max_vent_pulse_write_gap_threshold_source"
                ),
                "max_vent_pulse_write_gap_exceeded": context.get(
                    "max_vent_pulse_write_gap_exceeded"
                ),
                "max_vent_pulse_write_gap_not_exceeded_reason": context.get(
                    "max_vent_pulse_write_gap_not_exceeded_reason"
                ),
                "max_vent_scheduler_loop_gap_ms": context.get("max_vent_scheduler_loop_gap_ms"),
                "max_vent_command_total_duration_ms": context.get("max_vent_command_total_duration_ms"),
                "route_open_transition_max_vent_write_gap_ms": context.get(
                    "route_open_transition_max_vent_write_gap_ms"
                ),
                "route_open_transition_terminal_vent_write_age_ms": context.get(
                    "route_open_transition_terminal_vent_write_age_ms"
                ),
            }
        )
        self.host.a2_hooks.co2_route_conditioning_at_atmosphere_context = context
        if bool(schedule.get("route_conditioning_high_frequency_window_active")):
            context.update(
                {
                    "trace_write_budget_ms": self._a2_conditioning_trace_write_budget_ms(),
                    "trace_write_duration_ms": 0.0,
                    "trace_write_blocked_vent_scheduler": False,
                    "trace_write_deferred_for_vent_priority": True,
                }
            )
            tick.update(
                {
                    "trace_write_budget_ms": context["trace_write_budget_ms"],
                    "trace_write_duration_ms": 0.0,
                    "trace_write_blocked_vent_scheduler": False,
                    "trace_write_deferred_for_vent_priority": True,
                }
            )
            self.host.a2_hooks.co2_route_conditioning_at_atmosphere_context = context
        else:
            self.host._record_pressure_source_latency_events(tick, point=point, stage="co2_route_conditioning_at_atmosphere")
        self.host._record_a2_conditioning_workflow_timing(
            context,
            "co2_route_conditioning_vent_tick",
            "fail" if command_result != "ok" else "tick",
            stage="co2_route_conditioning_at_atmosphere",
            point=point,
            duration_s=tick["elapsed_s"],
            expected_max_s=self.host._cfg_get("workflow.pressure.vent_hold_interval_s", 2.0),
            wait_reason=phase,
            decision=command_result,
            pressure_hpa=pressure_hpa,
            pace_output_state=diagnostics.get("output_state"),
            pace_isolation_state=diagnostics.get("isolation_state"),
            pace_vent_status=diagnostics.get("vent_status_raw"),
            route_state=tick,
            warning_code="co2_route_conditioning_pressure_abnormal" if pressure_abnormal else None,
            error_code=command_error or ("route_conditioning_pressure_overlimit" if pressure_abnormal else None),
        )
        if command_result != "ok":
            if bool(tick.get("route_conditioning_fast_vent_command_timeout")):
                fail_reason = "route_conditioning_fast_vent_command_timeout"
            elif bool(tick.get("route_conditioning_fast_vent_not_supported")):
                fail_reason = "route_conditioning_fast_vent_not_supported"
            else:
                fail_reason = "route_conditioning_vent_command_failed"
            self.host._fail_a2_co2_route_conditioning_closed(
                point,
                reason=fail_reason,
                details=tick,
                event_name=f"co2_{fail_reason}",
                route_trace_action=f"co2_{fail_reason}",
                pressure_hpa=pressure_hpa,
            )
        if not selected_freshness_ok:
            reason = str(
                context.get("fail_closed_reason")
                or context.get("selected_pressure_fail_closed_reason")
                or "selected_pressure_sample_stale"
            )
            self.host._fail_a2_co2_route_conditioning_closed(
                point,
                reason=reason,
                details={
                    **tick,
                    "stream_stale": sample_stale,
                    "continuous_stream_stale": bool(context.get("continuous_stream_stale")),
                },
                event_name="co2_route_conditioning_stream_stale",
                route_trace_action="co2_route_conditioning_stream_stale",
                pressure_hpa=pressure_hpa,
            )
        if pressure_abnormal:
            self.host._fail_a2_co2_route_conditioning_closed(
                point,
                reason="route_conditioning_pressure_overlimit",
                details=tick,
                event_name="co2_route_conditioning_pressure_overlimit",
                route_trace_action="co2_preseal_atmosphere_hold_pressure_guard",
                pressure_hpa=pressure_hpa,
            )
        return tick

def _record_a2_co2_conditioning_pressure_monitor(
    self,
    point: CalibrationPoint,
    *,
    phase: str,
) -> dict[str, Any]:
    context = self.host.a2_hooks.co2_route_conditioning_at_atmosphere_context
    if not context:
        return {}
    context.setdefault("route_conditioning_diagnostic_blocked_vent_scheduler", False)
    context.setdefault("route_conditioning_vent_gap_exceeded", False)
    context.setdefault("pressure_monitor_blocked_vent_scheduler", False)
    context.setdefault("trace_write_blocked_vent_scheduler", False)
    now_mono = time.monotonic()
    monitor_started_monotonic_s = now_mono
    schedule = self.host._a2_conditioning_vent_schedule(context, now_mono=now_mono)
    context.update(schedule)
    context["vent_scheduler_priority_mode"] = True
    context["vent_scheduler_checked_before_diagnostic"] = True
    context["diagnostic_budget_ms"] = self.host._a2_conditioning_diagnostic_budget_ms()
    context["pressure_monitor_budget_ms"] = self.host._a2_conditioning_pressure_monitor_budget_ms()
    context["trace_write_budget_ms"] = self.host._a2_conditioning_trace_write_budget_ms()
    self.host.a2_hooks.co2_route_conditioning_at_atmosphere_context = context
    monitor_budget_ms = self.host._a2_conditioning_pressure_monitor_budget_ms()
    high_frequency_window = bool(schedule.get("route_conditioning_high_frequency_window_active"))
    flush_maintenance_window = bool(
        str(context.get("route_conditioning_phase") or "route_conditioning_flush_phase")
        == "route_conditioning_flush_phase"
        and context.get("route_conditioning_vent_maintenance_active", True)
        and not bool(context.get("ready_to_seal_phase_started", False))
        and not bool(context.get("seal_command_sent", False))
        and not bool(context.get("pressure_setpoint_command_sent", False))
        and not bool(context.get("pressure_ready_started", False))
        and not bool(context.get("sampling_started", False))
    )
    nonblocking_pressure_monitor = bool(high_frequency_window or flush_maintenance_window)
    deferred = self.host._a2_conditioning_defer_if_diagnostic_budget_unsafe(
        point,
        context,
        now_mono=now_mono,
        max_gap_s=float(schedule["route_conditioning_effective_max_gap_s"]),
        budget_ms=monitor_budget_ms,
        component="pressure_monitor",
        operation="conditioning_pressure_monitor_budget_check",
        pressure_monitor=True,
    )
    if deferred is not None:
        return dict(deferred)
    last_vent = self.host._as_float(context.get("last_vent_tick_monotonic_s"))
    if last_vent is not None:
        context["last_vent_command_age_s"] = round(max(0.0, now_mono - float(last_vent)), 3)
    if nonblocking_pressure_monitor:
        snapshot_started = time.monotonic()
        snapshot = self.host._a2_conditioning_stream_snapshot(
            point=point,
            phase=phase,
            fast=True,
            budget_ms=self.host._a2_conditioning_continuous_latest_fresh_budget_ms(),
        )
        snapshot_completed = time.monotonic()
        snapshot_duration_ms = round(max(0.0, snapshot_completed - snapshot_started) * 1000.0, 3)
        sample = self.host._a2_conditioning_pressure_sample_from_snapshot(snapshot, point, phase=phase)
        sample["pressure_monitor_nonblocking"] = True
        sample["conditioning_monitor_pressure_deferred"] = False
        sample["pressure_monitor_deferred_for_vent_priority"] = False
        sample["pressure_monitor_budget_ms"] = monitor_budget_ms
    else:
        sample = self.host._a2_conditioning_pressure_sample(point, phase=phase)
        snapshot_started = time.monotonic()
        snapshot = self.host._a2_conditioning_stream_snapshot(point=point, phase=phase)
        snapshot_completed = time.monotonic()
        snapshot_duration_ms = round(max(0.0, snapshot_completed - snapshot_started) * 1000.0, 3)
    snapshot_budget_exceeded = bool(nonblocking_pressure_monitor and snapshot_duration_ms > monitor_budget_ms)
    monitor_completed_monotonic_s = time.monotonic()
    monitor_duration_s = max(0.0, monitor_completed_monotonic_s - monitor_started_monotonic_s)
    details = self.host._a2_conditioning_pressure_details(sample, snapshot, context=context)
    if nonblocking_pressure_monitor and (
        not bool(details.get("selected_pressure_freshness_ok"))
        or bool(details.get("continuous_latest_fresh_budget_exceeded"))
        or bool(details.get("selected_pressure_sample_stale_budget_exceeded"))
    ):
        operation = str(
            details.get("selected_pressure_fail_closed_reason")
            or (
                "continuous_latest_fresh_budget_exceeded"
                if details.get("continuous_latest_fresh_budget_exceeded")
                else ""
            )
            or (
                "selected_pressure_sample_stale_budget_exceeded"
                if details.get("selected_pressure_sample_stale_budget_exceeded")
                else ""
            )
            or "continuous_snapshot_not_fresh"
        )
        deferred_context = self.host._a2_conditioning_defer_diagnostic_for_vent_priority(
            {
                **context,
                **details,
                "pressure_monitor_duration_ms": round(monitor_duration_s * 1000.0, 3),
            },
            point=point,
            component="pressure_monitor",
            operation=operation,
            now_mono=monitor_completed_monotonic_s,
            pressure_monitor=True,
        )
        return dict(deferred_context)
    elapsed_s = max(0.0, now_mono - float(context.get("conditioning_started_monotonic_s") or now_mono))
    context = self.host._a2_conditioning_update_pressure_metrics(
        context,
        phase=phase,
        pressure_hpa=details.get("pressure_hpa"),
        event_monotonic_s=monitor_completed_monotonic_s,
        vent_command_sent=False,
    )
    context["selected_pressure_source_for_conditioning_monitor"] = (
        details.get("selected_pressure_source")
        or details.get("pressure_source_selected")
        or details.get("pressure_sample_source")
        or ""
    )
    context["a2_conditioning_pressure_source_strategy"] = self.host._a2_conditioning_pressure_source_mode()
    context["latest_route_conditioning_pressure_source"] = context[
        "selected_pressure_source_for_conditioning_monitor"
    ]
    context["latest_route_conditioning_pressure_age_s"] = details.get("selected_pressure_sample_age_s")
    context["latest_route_conditioning_pressure_eligible_for_prearm_baseline"] = bool(
        details.get("pressure_hpa") is not None
        and details.get("selected_pressure_freshness_ok")
        and not details.get("pressure_overlimit_seen")
    )
    monitor_state = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "phase": phase,
        "elapsed_s": round(elapsed_s, 3),
        "route_conditioning_phase": context.get("route_conditioning_phase", "route_conditioning_flush_phase"),
        "ready_to_seal_phase_started": bool(context.get("ready_to_seal_phase_started", False)),
        "vent_off_blocked_during_flush": bool(context.get("vent_off_blocked_during_flush", True)),
        "seal_blocked_during_flush": bool(context.get("seal_blocked_during_flush", True)),
        "pressure_setpoint_blocked_during_flush": bool(
            context.get("pressure_setpoint_blocked_during_flush", True)
        ),
        "sample_blocked_during_flush": bool(context.get("sample_blocked_during_flush", True)),
        "vent_off_command_sent": bool(context.get("vent_off_command_sent", False)),
        "seal_command_sent": bool(context.get("seal_command_sent", False)),
        "pressure_setpoint_command_sent": bool(context.get("pressure_setpoint_command_sent", False)),
        "vent_command_sent": False,
        "whether_safe_to_continue": bool(
            details.get("selected_pressure_freshness_ok")
            and not details.get("pressure_overlimit_seen")
            and not context.get("route_open_transient_rejection_reason")
        ),
        "pressure_monitor_interval_s": self.host._a2_conditioning_pressure_monitor_interval_s(),
        "last_vent_command_age_s": context.get("last_vent_command_age_s"),
        "blocking_operation_name": "a2_conditioning_pressure_monitor",
        "blocking_operation_duration_ms": round(monitor_duration_s * 1000.0, 3),
        "diagnostic_duration_ms": round(monitor_duration_s * 1000.0, 3),
        "vent_scheduler_priority_mode": True,
        "vent_scheduler_checked_before_diagnostic": True,
        "diagnostic_deferred_for_vent_priority": bool(
            context.get("diagnostic_deferred_for_vent_priority", False)
        ),
        "diagnostic_deferred_count": int(context.get("diagnostic_deferred_count") or 0),
        "diagnostic_budget_ms": self.host._a2_conditioning_diagnostic_budget_ms(),
        "diagnostic_budget_exceeded": bool(
            context.get("diagnostic_budget_exceeded", False)
            or (round(monitor_duration_s * 1000.0, 3) > self.host._a2_conditioning_diagnostic_budget_ms())
            or snapshot_budget_exceeded
        ),
        "diagnostic_blocking_component": "pressure_monitor",
        "diagnostic_blocking_operation": (
            str(details.get("pressure_source_selection_reason") or "continuous_latest_fast_snapshot")
            if nonblocking_pressure_monitor
            else str(details.get("pressure_source_selection_reason") or "pressure_monitor")
        ),
        "diagnostic_blocking_duration_ms": round(monitor_duration_s * 1000.0, 3),
        "pressure_monitor_nonblocking": bool(nonblocking_pressure_monitor),
        "pressure_monitor_deferred_for_vent_priority": False,
        "pressure_monitor_budget_ms": monitor_budget_ms,
        "pressure_monitor_duration_ms": round(monitor_duration_s * 1000.0, 3),
        "pressure_monitor_blocked_vent_scheduler": False,
        "conditioning_monitor_pressure_deferred": False,
        "conditioning_monitor_pressure_deferred_count": int(
            context.get("conditioning_monitor_pressure_deferred_count") or 0
        ),
        "conditioning_monitor_max_defer_ms": context.get(
            "conditioning_monitor_max_defer_ms",
            self.host._a2_conditioning_monitor_pressure_max_defer_ms(),
        ),
        "conditioning_monitor_pressure_stale_timeout": bool(
            context.get("conditioning_monitor_pressure_stale_timeout", False)
        ),
        "conditioning_monitor_pressure_unavailable_fail_closed": bool(
            context.get("conditioning_monitor_pressure_unavailable_fail_closed", False)
        ),
        "trace_write_budget_ms": self.host._a2_conditioning_trace_write_budget_ms(),
        "trace_write_duration_ms": context.get("trace_write_duration_ms"),
        "trace_write_blocked_vent_scheduler": bool(context.get("trace_write_blocked_vent_scheduler", False)),
        "trace_write_deferred_for_vent_priority": bool(
            context.get("trace_write_deferred_for_vent_priority", False)
        ),
        "route_conditioning_diagnostic_blocked_vent_scheduler": bool(
            context.get("route_conditioning_diagnostic_blocked_vent_scheduler", False)
        ),
        "route_open_transition_started": bool(context.get("route_open_transition_started", False)),
        "route_open_transition_started_at": context.get("route_open_transition_started_at", ""),
        "route_open_transition_started_monotonic_s": context.get(
            "route_open_transition_started_monotonic_s"
        ),
        "route_open_command_write_started_at": context.get("route_open_command_write_started_at", ""),
        "route_open_command_write_completed_at": context.get("route_open_command_write_completed_at", ""),
        "route_open_command_write_duration_ms": context.get("route_open_command_write_duration_ms"),
        "route_open_settle_wait_sliced": bool(context.get("route_open_settle_wait_sliced", False)),
        "route_open_settle_wait_slice_count": int(context.get("route_open_settle_wait_slice_count") or 0),
        "route_open_settle_wait_total_ms": context.get("route_open_settle_wait_total_ms"),
        "route_open_transition_total_duration_ms": context.get("route_open_transition_total_duration_ms"),
        "vent_ticks_during_route_open_transition": int(
            context.get("vent_ticks_during_route_open_transition") or 0
        ),
        "route_open_transition_max_vent_write_gap_ms": context.get(
            "route_open_transition_max_vent_write_gap_ms"
        ),
        "route_open_transition_terminal_vent_write_age_ms": context.get(
            "route_open_transition_terminal_vent_write_age_ms"
        ),
        "route_open_transition_blocked_vent_scheduler": bool(
            context.get("route_open_transition_blocked_vent_scheduler", False)
        ),
        "route_open_settle_wait_blocked_vent_scheduler": bool(
            context.get("route_open_settle_wait_blocked_vent_scheduler", False)
        ),
        "terminal_vent_write_age_ms_at_gap_gate": context.get(
            "terminal_vent_write_age_ms_at_gap_gate"
        ),
        "max_vent_pulse_write_gap_ms_including_terminal_gap": context.get(
            "max_vent_pulse_write_gap_ms_including_terminal_gap"
        ),
        "max_vent_pulse_write_gap_phase": context.get("max_vent_pulse_write_gap_phase", ""),
        "max_vent_pulse_write_gap_threshold_ms": context.get(
            "max_vent_pulse_write_gap_threshold_ms"
        ),
        "max_vent_pulse_write_gap_threshold_source": context.get(
            "max_vent_pulse_write_gap_threshold_source",
            "",
        ),
        "max_vent_pulse_write_gap_exceeded": bool(
            context.get("max_vent_pulse_write_gap_exceeded", False)
        ),
        "max_vent_pulse_write_gap_not_exceeded_reason": context.get(
            "max_vent_pulse_write_gap_not_exceeded_reason",
            "",
        ),
        "route_conditioning_vent_gap_exceeded_source": context.get(
            "route_conditioning_vent_gap_exceeded_source",
            "",
        ),
        "terminal_gap_source": context.get("terminal_gap_source", ""),
        "terminal_gap_operation": context.get("terminal_gap_operation", ""),
        "terminal_gap_duration_ms": context.get("terminal_gap_duration_ms"),
        "terminal_gap_started_at": context.get("terminal_gap_started_at", ""),
        "terminal_gap_detected_at": context.get("terminal_gap_detected_at", ""),
        "terminal_gap_stack_marker": context.get("terminal_gap_stack_marker", ""),
        "defer_returned_to_vent_loop": bool(context.get("defer_returned_to_vent_loop", False)),
        "defer_to_next_vent_loop_ms": context.get("defer_to_next_vent_loop_ms"),
        "vent_tick_after_defer_ms": context.get("vent_tick_after_defer_ms"),
        "terminal_gap_after_defer": bool(context.get("terminal_gap_after_defer", False)),
        "terminal_gap_after_defer_ms": context.get("terminal_gap_after_defer_ms"),
        "defer_path_no_reschedule": bool(context.get("defer_path_no_reschedule", False)),
        "fail_closed_path_started": bool(context.get("fail_closed_path_started", False)),
        "fail_closed_path_started_while_route_open": bool(
            context.get("fail_closed_path_started_while_route_open", False)
        ),
        "fail_closed_path_vent_maintenance_required": bool(
            context.get("fail_closed_path_vent_maintenance_required", False)
        ),
        "fail_closed_path_vent_maintenance_active": bool(
            context.get("fail_closed_path_vent_maintenance_active", False)
        ),
        "fail_closed_path_duration_ms": context.get("fail_closed_path_duration_ms"),
        "fail_closed_path_blocked_vent_scheduler": bool(
            context.get("fail_closed_path_blocked_vent_scheduler", False)
        ),
        "route_open_to_first_pressure_read_ms": context.get("route_open_to_first_pressure_read_ms"),
        "route_open_to_overlimit_ms": context.get("route_open_to_overlimit_ms"),
        "route_conditioning_pressure_before_route_open_hpa": context.get(
            "route_conditioning_pressure_before_route_open_hpa"
        ),
        "route_conditioning_pressure_after_route_open_hpa": context.get(
            "route_conditioning_pressure_after_route_open_hpa"
        ),
        "route_conditioning_pressure_rise_rate_hpa_per_s": context.get(
            "route_conditioning_pressure_rise_rate_hpa_per_s"
        ),
        "route_conditioning_peak_pressure_hpa": context.get("route_conditioning_peak_pressure_hpa"),
        "latest_route_conditioning_pressure_hpa": context.get("latest_route_conditioning_pressure_hpa"),
        "latest_route_conditioning_pressure_source": context.get(
            "latest_route_conditioning_pressure_source",
            "",
        ),
        "latest_route_conditioning_pressure_age_s": context.get(
            "latest_route_conditioning_pressure_age_s"
        ),
        "latest_route_conditioning_pressure_eligible_for_prearm_baseline": bool(
            context.get("latest_route_conditioning_pressure_eligible_for_prearm_baseline", False)
        ),
        "route_conditioning_pressure_overlimit": bool(context.get("route_conditioning_pressure_overlimit", False)),
        **self.host._a2_route_open_transient_evidence(context),
        "pressure_rise_since_last_vent_hpa": context.get("pressure_rise_since_last_vent_hpa"),
        "pressure_sample_stale": bool(details.get("selected_pressure_sample_is_stale")),
        "pressure_freshness_ok": bool(details.get("selected_pressure_freshness_ok")),
        "selected_pressure_source_for_conditioning_monitor": context.get(
            "selected_pressure_source_for_conditioning_monitor",
            "",
        ),
        "a2_conditioning_pressure_source_strategy": context.get(
            "a2_conditioning_pressure_source_strategy",
            self.host._a2_conditioning_pressure_source_mode(),
        ),
        **{key: value for key, value in details.items() if key not in {"sample", "digital_sample"}},
    }
    contextual_sample = {**sample, **monitor_state}
    samples = [item for item in list(context.get("pressure_samples") or []) if isinstance(item, Mapping)]
    samples.append(monitor_state)
    context["pressure_samples"] = samples
    context["last_pressure_monitor_monotonic_s"] = now_mono
    pressure_values = [
        self.host._as_float(item.get("pressure_hpa", item.get("digital_gauge_pressure_hpa")))
        for item in samples
        if isinstance(item, Mapping)
        and self.host._as_float(item.get("pressure_hpa", item.get("digital_gauge_pressure_hpa"))) is not None
    ]
    if pressure_values:
        context["pressure_max_during_conditioning_hpa"] = max(float(value) for value in pressure_values if value is not None)
        context["pressure_min_during_conditioning_hpa"] = min(float(value) for value in pressure_values if value is not None)
    if details.get("digital_gauge_latest_age_s") is not None:
        context["digital_gauge_latest_age_s"] = details.get("digital_gauge_latest_age_s")
        previous_max = self.host._as_float(context.get("latest_frame_age_max_s"))
        context["latest_frame_age_max_s"] = (
            details["digital_gauge_latest_age_s"]
            if previous_max is None
            else max(float(previous_max), float(details["digital_gauge_latest_age_s"]))
        )
    if details.get("latest_frame_sequence_id") is not None:
        context["last_digital_gauge_sequence_id"] = details.get("latest_frame_sequence_id")
    context["digital_gauge_sequence_progress"] = details.get("digital_gauge_sequence_progress")
    context.update(self.host._a2_conditioning_digital_gauge_evidence(details))
    context["selected_pressure_source_for_conditioning_monitor"] = monitor_state.get(
        "selected_pressure_source_for_conditioning_monitor",
        "",
    )
    context["a2_conditioning_pressure_source_strategy"] = monitor_state.get(
        "a2_conditioning_pressure_source_strategy",
        self.host._a2_conditioning_pressure_source_mode(),
    )
    context["pressure_monitor_interval_s"] = self.host._a2_conditioning_pressure_monitor_interval_s()
    context["conditioning_pressure_abort_hpa"] = details.get("conditioning_pressure_abort_hpa")
    context["route_conditioning_hard_abort_pressure_hpa"] = details.get(
        "route_conditioning_hard_abort_pressure_hpa",
        context.get("route_conditioning_hard_abort_pressure_hpa"),
    )
    context["route_conditioning_hard_abort_exceeded"] = bool(
        context.get("route_conditioning_hard_abort_exceeded")
        or details.get("route_conditioning_hard_abort_exceeded")
    )
    context["pressure_overlimit_seen"] = bool(context.get("pressure_overlimit_seen") or details.get("pressure_overlimit_seen"))
    if details.get("pressure_overlimit_seen"):
        context["pressure_overlimit_source"] = details.get("pressure_overlimit_source")
        context["pressure_overlimit_hpa"] = details.get("pressure_overlimit_hpa")
        context["route_conditioning_high_pressure_seen_before_preseal"] = True
        context["route_conditioning_high_pressure_seen_before_preseal_hpa"] = details.get(
            "route_conditioning_high_pressure_seen_before_preseal_hpa",
            details.get("pressure_overlimit_hpa"),
        )
        context["route_conditioning_high_pressure_seen_phase"] = details.get(
            "route_conditioning_high_pressure_seen_phase",
            "co2_route_conditioning_at_atmosphere",
        )
        context["route_conditioning_high_pressure_seen_source"] = details.get(
            "route_conditioning_high_pressure_seen_source",
            details.get("pressure_overlimit_source"),
        )
        context["route_conditioning_high_pressure_seen_sample_age_s"] = details.get(
            "route_conditioning_high_pressure_seen_sample_age_s"
        )
        context["route_conditioning_high_pressure_seen_decision"] = details.get(
            "route_conditioning_high_pressure_seen_decision",
            "fail_closed",
        )
    context.update(
        {
            "vent_scheduler_priority_mode": True,
            "vent_scheduler_checked_before_diagnostic": True,
            "diagnostic_budget_ms": monitor_state["diagnostic_budget_ms"],
            "diagnostic_budget_exceeded": monitor_state["diagnostic_budget_exceeded"],
            "diagnostic_blocking_component": monitor_state["diagnostic_blocking_component"],
            "diagnostic_blocking_operation": monitor_state["diagnostic_blocking_operation"],
            "diagnostic_blocking_duration_ms": monitor_state["diagnostic_blocking_duration_ms"],
            "pressure_monitor_nonblocking": monitor_state["pressure_monitor_nonblocking"],
            "pressure_monitor_deferred_for_vent_priority": False,
            "pressure_monitor_budget_ms": monitor_state["pressure_monitor_budget_ms"],
            "pressure_monitor_duration_ms": monitor_state["pressure_monitor_duration_ms"],
            "pressure_monitor_blocked_vent_scheduler": False,
            "conditioning_monitor_pressure_deferred": False,
            "trace_write_budget_ms": monitor_state["trace_write_budget_ms"],
        }
    )
    context["last_blocking_operation_name"] = "a2_conditioning_pressure_monitor"
    context["last_blocking_operation_started_monotonic_s"] = monitor_started_monotonic_s
    context["last_blocking_operation_completed_monotonic_s"] = monitor_completed_monotonic_s
    context["last_blocking_operation_duration_s"] = monitor_duration_s
    context["last_blocking_operation_safe_to_continue"] = bool(
        details.get("selected_pressure_freshness_ok")
        and not details.get("pressure_overlimit_seen")
        and not context.get("route_open_transient_rejection_reason")
    )
    context = self.host._a2_conditioning_context_with_counts(context)
    self.host.a2_hooks.co2_route_conditioning_at_atmosphere_context = context
    schedule = self.host._a2_conditioning_vent_schedule(context, now_mono=monitor_completed_monotonic_s)
    diagnostic_max_gap_s = float(schedule.get("route_conditioning_effective_max_gap_s") or 0.0)
    if diagnostic_max_gap_s > 0.0 and monitor_duration_s > diagnostic_max_gap_s:
        source = self.host._a2_conditioning_diagnostic_source(context, fallback="pressure_monitor")
        terminal = self.host._a2_conditioning_terminal_gap_details(
            context,
            now_mono=monitor_completed_monotonic_s,
            max_gap_s=diagnostic_max_gap_s,
            source=source,
        )
        monitor_state["route_conditioning_diagnostic_blocked_vent_scheduler"] = True
        monitor_state["pressure_monitor_blocked_vent_scheduler"] = True
        monitor_state["diagnostic_blocking_component"] = source
        monitor_state["diagnostic_blocking_operation"] = context.get(
            "diagnostic_blocking_operation",
            "pressure_monitor",
        )
        monitor_state.update(terminal)
        monitor_state["fail_closed_reason"] = "route_conditioning_diagnostic_blocked_vent_scheduler"
        context.update(
            {
                **terminal,
                "route_conditioning_diagnostic_blocked_vent_scheduler": True,
                "pressure_monitor_blocked_vent_scheduler": True,
                "diagnostic_duration_ms": monitor_state["diagnostic_duration_ms"],
                "diagnostic_blocking_component": monitor_state["diagnostic_blocking_component"],
                "diagnostic_blocking_operation": monitor_state["diagnostic_blocking_operation"],
                "diagnostic_blocking_duration_ms": monitor_state["diagnostic_blocking_duration_ms"],
                "fail_closed_reason": "route_conditioning_diagnostic_blocked_vent_scheduler",
            }
        )
        self.host.a2_hooks.co2_route_conditioning_at_atmosphere_context = context
        self.host._fail_a2_co2_route_conditioning_closed(
            point,
            reason="route_conditioning_diagnostic_blocked_vent_scheduler",
            details=monitor_state,
            event_name="co2_route_conditioning_diagnostic_blocked_vent_scheduler",
            route_trace_action="co2_route_conditioning_diagnostic_blocked_vent_scheduler",
        )
    if high_frequency_window:
        context = self.host.a2_hooks.co2_route_conditioning_at_atmosphere_context
        context.update(
            {
                "trace_write_budget_ms": self.host._a2_conditioning_trace_write_budget_ms(),
                "trace_write_duration_ms": 0.0,
                "trace_write_blocked_vent_scheduler": False,
                "trace_write_deferred_for_vent_priority": True,
            }
        )
        monitor_state.update(
            {
                "trace_write_budget_ms": context["trace_write_budget_ms"],
                "trace_write_duration_ms": 0.0,
                "trace_write_blocked_vent_scheduler": False,
                "trace_write_deferred_for_vent_priority": True,
            }
        )
        self.host.a2_hooks.co2_route_conditioning_at_atmosphere_context = context
    else:
        self.host._record_pressure_source_latency_events(
            contextual_sample,
            point=point,
            stage="co2_route_conditioning_at_atmosphere",
        )
    transient_rejection_reason = str(context.get("route_open_transient_rejection_reason") or "").strip()
    pressure_fail = bool(details.get("pressure_overlimit_seen") or transient_rejection_reason)
    event_type = "fail" if pressure_fail else "tick"
    event_name = (
        "co2_route_conditioning_pressure_warning"
        if details.get("pressure_overlimit_seen")
        else (
            "co2_route_conditioning_transient_recovery_failed"
            if transient_rejection_reason
            else "co2_route_conditioning_pressure_sample"
        )
    )
    self.host._record_a2_conditioning_workflow_timing(
        context,
        event_name,
        event_type,
        stage="co2_route_conditioning_at_atmosphere",
        point=point,
        duration_s=monitor_state["elapsed_s"],
        pressure_hpa=details.get("pressure_hpa"),
        decision=(
            "hard_abort_pressure_exceeded"
            if details.get("pressure_overlimit_seen")
            else (transient_rejection_reason or "monitor_only_no_seal")
        ),
        warning_code="conditioning_pressure_above_hard_abort_threshold"
        if details.get("pressure_overlimit_seen")
        else None,
        error_code="route_conditioning_pressure_overlimit"
        if details.get("pressure_overlimit_seen")
        else (transient_rejection_reason or None),
        route_state=monitor_state,
    )
    if not bool(details.get("selected_pressure_freshness_ok")):
        reason = str(
            details.get("fail_closed_reason")
            or details.get("selected_pressure_fail_closed_reason")
            or "selected_pressure_sample_stale"
        )
        self.host._fail_a2_co2_route_conditioning_closed(
            point,
            reason=reason,
            details={
                **monitor_state,
                "stream_stale": bool(details.get("selected_pressure_sample_is_stale")),
                "continuous_stream_stale": bool(details.get("continuous_stream_stale")),
            },
            event_name="co2_route_conditioning_stream_stale",
            route_trace_action="co2_route_conditioning_stream_stale",
            pressure_hpa=details.get("pressure_hpa"),
        )
    if details.get("pressure_overlimit_seen"):
        self.host._fail_a2_co2_route_conditioning_closed(
            point,
            reason="route_conditioning_pressure_overlimit",
            details=monitor_state,
            event_name="co2_route_conditioning_pressure_overlimit",
            route_trace_action="co2_preseal_atmosphere_hold_pressure_guard",
            pressure_hpa=details.get("pressure_hpa"),
        )
    if transient_rejection_reason and not bool(context.get("route_open_transient_accepted", False)):
        self.host._fail_a2_co2_route_conditioning_closed(
            point,
            reason=transient_rejection_reason,
            details=monitor_state,
            event_name="co2_route_conditioning_transient_recovery_failed",
            route_trace_action="co2_route_conditioning_transient_recovery_failed",
            pressure_hpa=details.get("pressure_hpa"),
        )

