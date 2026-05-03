from __future__ import annotations

from typing import Any


class ConditioningService:

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
