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
