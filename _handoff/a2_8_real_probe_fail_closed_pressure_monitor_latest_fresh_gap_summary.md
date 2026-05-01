# A2.8 real probe FAIL_CLOSED pressure monitor gap summary

## Scope

- Probe: Step3A A2.8 v1_aligned CO2-only 7-pressure no-write engineering probe
- Output directory: `D:\gas_calibrator_step3a_a2_8_v1_aligned_co2_7_pressure_no_write_skip_temp_wait_20260429_0223`
- Commit HEAD: `b08a692533743362b70a1170fc6439673d13c434`
- Commit label: `b08a6925 A2.8 Resolve terminal gap and pressure gate staging`
- Evidence state: `engineering_probe_only`, `promotion_state=blocked`, `not_real_acceptance_evidence=true`
- Final decision: `FAIL_CLOSED`

## Summary fields

- `final_decision=FAIL_CLOSED`
- `rejection_reasons=["a2_route_conditioning_vent_gap_exceeded", "a2_route_conditioning_diagnostic_blocked_vent_scheduler"]`
- `fast_vent_reassert_supported=true`
- `fast_vent_reassert_used=true`
- `route_open_transition_started=true`
- `vent_scheduler_priority_mode=true`
- `vent_scheduler_checked_before_diagnostic=true`
- `diagnostic_deferred_for_vent_priority=true`
- `diagnostic_deferred_count=25`
- `diagnostic_blocking_component=pressure_monitor`
- `diagnostic_blocking_operation=digital_gauge_continuous_latest_fresh`
- `diagnostic_blocking_duration_ms=3853.648`
- `pressure_monitor_nonblocking=false`
- `pressure_monitor_deferred_for_vent_priority=false`
- `pressure_monitor_duration_ms=3853.648`
- `pressure_monitor_blocked_vent_scheduler=true`
- `conditioning_monitor_pressure_deferred=false`
- `terminal_gap_source=pressure_monitor`
- `terminal_gap_operation=selected_pressure_sample_stale`
- `terminal_gap_duration_ms=4547.179`
- `terminal_vent_write_age_ms_at_gap_gate=4547.179`
- `max_vent_pulse_write_gap_ms=563.633`
- `max_vent_pulse_write_gap_ms_including_terminal_gap=4547.179`
- `max_vent_scheduler_loop_gap_ms=4547.179`
- `route_conditioning_vent_gap_exceeded_source=pressure_monitor`
- `route_conditioning_pressure_rise_rate_hpa_per_s=-2.067`
- `route_conditioning_peak_pressure_hpa=1075.157`
- `route_conditioning_pressure_overlimit=false`
- `selected_pressure_source_for_conditioning_monitor=digital_pressure_gauge_continuous`
- `selected_pressure_source_for_pressure_gate=""`
- `conditioning_monitor_pressure_source_allowed=true`
- `pressure_gate_reached=false`
- `pressure_gate_not_reached_reason=route_conditioning_fail_closed`
- `pressure_gate_source_alignment_ready=false`
- `pressure_gate_source_alignment_reasons=[]`
- `selected_pressure_freshness_ok=true`
- `pressure_points_completed=0`
- `points_completed=0`
- `sample_count_total=0`

## digital_gauge_continuous_latest_fresh context

During `route_conditioning_flush_phase`, the route conditioning diagnostic reported:

- `diagnostic_blocking_operation=digital_gauge_continuous_latest_fresh`
- `diagnostic_blocking_duration_ms=3853.648`
- `pressure_monitor_duration_ms=3853.648`
- `pressure_monitor_nonblocking=false`
- `pressure_monitor_blocked_vent_scheduler=true`

The selected pressure source was `digital_pressure_gauge_continuous`, with
`pressure_source_selection_reason=digital_gauge_continuous_latest_fresh`.
The latest frame was fresh by metadata (`digital_gauge_latest_age_s=0.011`,
`selected_pressure_sample_age_s=0.011`, `selected_pressure_freshness_ok=true`),
but the path still consumed 3853.648 ms inside the high-frequency vent window.
This indicates that the "latest fresh" path was not a strict O(1) memory
snapshot during flush maintenance.

## selected_pressure_sample_stale context

The FAIL_CLOSED terminal gap was recorded as:

- `terminal_gap_source=pressure_monitor`
- `terminal_gap_operation=selected_pressure_sample_stale`
- `terminal_gap_duration_ms=4547.179`
- `terminal_gap_stack_marker=defer:pressure_monitor:selected_pressure_sample_stale`
- `terminal_vent_write_age_ms_at_gap_gate=4547.179`

Before the terminal fail-closed event, diagnostic deferral protected vent
priority for 25 iterations, but the final pressure monitor operation still
spent 3853.648 ms and caused the terminal write age to exceed the A2 route
conditioning gate. The stale decision therefore needs to be metadata-only and
budgeted in the high-frequency vent window.

## A2.8 conclusion

A2.8 fixed the old `a2_4_pressure_source_not_v1_aligned` false rejection: when
`pressure_gate_reached=false`, the run no longer misreports pressure-gate
source alignment. The new A2.9 target is narrower: make the flush maintenance
pressure monitor strictly nonblocking, especially
`digital_gauge_continuous_latest_fresh` and `selected_pressure_sample_stale`,
without changing V1, `run_app.py`, pressure gate semantics, no-write policy, or
Step3A engineering-probe boundaries.
