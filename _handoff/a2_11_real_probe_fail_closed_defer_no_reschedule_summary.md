# A2.11 real engineering probe FAIL_CLOSED handoff

## Scope

- Probe output directory: `D:\gas_calibrator_step3a_a2_11_v1_aligned_co2_7_pressure_no_write_skip_temp_wait_20260429_2100`
- A2.11R commit HEAD: `3dd441240ed9e6ce031b4d23f3ff978bfe2c6290`
- Branch: `codex/run001-a1-no-write-dry-run`
- This record is engineering evidence only, not real acceptance.

## Summary fields

- `evidence_source`: `real_probe_a2_10_co2_7_pressure_no_write` *(bug: should not remain A2.10 for A2.11/A2.12 evidence)*
- `final_decision`: `FAIL_CLOSED`
- `rejection_reasons`: `a2_route_conditioning_vent_gap_exceeded`
- `route_conditioning_hard_abort_pressure_hpa`: `1250.0`
- `route_conditioning_hard_abort_exceeded`: `false`
- `route_conditioning_pressure_overlimit`: `false`
- `route_conditioning_peak_pressure_hpa`: `1195.639`
- `pressure_rise_despite_valid_vent_scheduler`: `false`
- `sustained_pressure_rise_after_route_open`: `false`
- `max_vent_pulse_write_gap_ms_including_terminal_gap`: `2113.793`
- `pressure_monitor_blocked_vent_scheduler`: `false`
- `terminal_gap_source`: `defer_path_no_reschedule`
- `terminal_vent_write_age_ms_at_gap_gate`: `2113.793`
- `pressure_points_completed`: `0`
- `points_completed`: `0`
- `sample_count_total`: `0`
- `pressure_gate_reached`: `false`
- `no_write`: `true`

## Route trace context

The early route trace contains a usable measured atmosphere sample before route conditioning:

- `route_trace.jsonl` line 13: `route_baseline`, message `Baseline before CO2 route conditioning`
- `route_trace.jsonl` line 14: `set_vent`, message `Vent atmosphere before CO2 route conditioning`
- line 14 `actual.pressure_hpa`: `1014.361`
- line 14 vent command: `set_output_false_set_isolation_open_vent_true`
- line 14 `vent_status_raw`: `1`
- line 14 interpreted `vent_status`: `in_progress`
- line 14 `atmosphere_ready`: `true`

This should propagate to:

- `measured_atmospheric_pressure_hpa`
- `measured_atmospheric_pressure_source`
- `route_open_transient_recovery_target_hpa`

## Terminal gap failure context

`route_trace.jsonl` line 249 records:

- action: `co2_route_conditioning_vent_heartbeat_gap`
- result: `fail`
- message: `A2 CO2 route conditioning fail-closed: route_conditioning_vent_gap_exceeded`
- `route_conditioning_phase`: `route_conditioning_flush_phase`
- `ready_to_seal_phase_started`: `false`
- `route_conditioning_flush_min_time_completed`: `false`
- `vent_off_blocked_during_flush`: `true`
- `seal_blocked_during_flush`: `true`
- `pressure_setpoint_blocked_during_flush`: `true`
- `sample_blocked_during_flush`: `true`
- `vent_heartbeat_gap_exceeded`: `true`
- `route_conditioning_vent_gap_exceeded`: `true`
- `last_vent_command_age_s`: about `2.114`
- `terminal_gap_source`: `defer_path_no_reschedule`
- `terminal_vent_write_age_ms_at_gap_gate`: `2113.793`
- `fast_vent_reassert_supported`: `true`
- `fast_vent_reassert_used`: `true`
- `pressure_monitor_nonblocking`: `true`
- `pressure_monitor_blocked_vent_scheduler`: `false`
- `diagnostic_deferred_for_vent_priority`: `true`
- `trace_write_deferred_for_vent_priority`: `true`
- `diagnostic_deferred_count`: `204`
- `diagnostic_blocking_component`: `pressure_monitor`
- `diagnostic_blocking_operation`: `digital_gauge_continuous_latest_fresh`

Interpretation: the failure was not hard-abort pressure, not pressure overlimit, and not pressure monitor blocking. The high-frequency conditioning path deferred lower-priority work, but did not reschedule itself back into the vent loop soon enough. That allowed the terminal vent write age to reach `2113.793 ms`, exceeding the `2.0 s` terminal gap gate.

## Evidence propagation issues to fix

- Wrapper summary still labels the run as `real_probe_a2_10_co2_7_pressure_no_write`.
- `measured_atmospheric_pressure_hpa` is null even though route trace line 14 has `1014.361 hPa`.
- `route_open_transient_*` fields do not distinguish not-started/evaluating/accepted/rejected/interrupted/hard-abort.
- `route_open_transient_rejection_reason` is empty even though evaluation was effectively interrupted by a vent gap.

## A2.12 target

- Any high-frequency route conditioning defer must return to the route conditioning vent loop within `<= 200 ms`.
- The next loop tick must check fast vent due before diagnostics, trace/timing aggregation, transient evaluation, gate evaluation, or fail-path work.
- If defer rescheduling fails, record `defer_path_no_reschedule=true` with the source/operation/reason and fail closed without widening the `2.0 s` terminal gap gate.
