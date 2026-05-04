# A2.7 real probe FAIL_CLOSED: terminal gap source not yet classified

This handoff note preserves the single A2.7 Step3A engineering probe evidence for the next A2 offline fix pass.

- Output directory: `D:\gas_calibrator_step3a_a2_7_v1_aligned_co2_7_pressure_no_write_skip_temp_wait_20260429_0136`
- A2.7 commit HEAD: `75bc27ef809d323220c5f6a63f514bbe1298a132`
- Branch: `codex/run001-a1-no-write-dry-run`
- Evidence level: `engineering_probe_only`
- Not real acceptance evidence: `true`
- A3 allowed: `false`

## Summary fields

- `final_decision=FAIL_CLOSED`
- `rejection_reasons=["a2_route_conditioning_vent_gap_exceeded","a2_4_pressure_source_not_v1_aligned"]`
- `route_conditioning_phase=route_conditioning_flush_phase`
- `terminal_vent_write_age_ms_at_gap_gate=4917.728`
- `max_vent_pulse_write_gap_ms_including_terminal_gap=4917.728`
- `max_vent_scheduler_loop_gap_ms=4917.728`
- `route_conditioning_vent_gap_exceeded_source=terminal_gap`
- `route_conditioning_diagnostic_blocked_vent_scheduler=false`
- `pressure_monitor_blocked_vent_scheduler=false`
- `trace_write_blocked_vent_scheduler=false`
- `diagnostic_blocking_duration_ms=0.0`
- `diagnostic_blocking_component=pressure_monitor`
- `diagnostic_blocking_operation=selected_pressure_sample_stale`
- `diagnostic_deferred_for_vent_priority=true`
- `diagnostic_deferred_count=1`
- `diagnostic_budget_exceeded=false`
- `pressure_monitor_nonblocking=true`
- `conditioning_monitor_pressure_deferred=true`
- `trace_write_deferred_for_vent_priority=true`
- `route_open_transition_started=true`
- `route_open_command_write_duration_ms=554.344`
- `route_open_transition_total_duration_ms=557.35`
- `route_open_to_first_vent_write_ms=0.138`
- `selected_pressure_source_for_conditioning_monitor=digital_pressure_gauge_continuous`
- `selected_pressure_source_for_pressure_gate=""`
- `selected_pressure_freshness_ok=false`
- `pressure_points_completed=0`
- `points_completed=0`
- `sample_count_total=0`
- `route_conditioning_pressure_overlimit=false`
- `route_conditioning_peak_pressure_hpa=1072.489`

## Route trace context

The decisive route trace action is `co2_route_conditioning_vent_heartbeat_gap` in `route_trace.jsonl`.

Neighboring route trace actions:

- `set_vent` result `ok`: A2 route conditioning fast vent maintenance
- `set_vent` result `ok`: A2 route conditioning fast vent maintenance
- `set_co2_valves` result `ok`: CO2 route valves set
- `set_vent` result `ok`: A2 route conditioning fast vent maintenance
- `co2_route_conditioning_vent_heartbeat_gap` result `fail`

Key fail route trace fields:

- `route_conditioning_phase=route_conditioning_flush_phase`
- `vent_phase=route_open_high_frequency_vent_phase`
- `terminal_vent_write_age_ms_at_gap_gate=4917.728`
- `max_vent_pulse_write_gap_ms_including_terminal_gap=4917.728`
- `max_vent_scheduler_loop_gap_ms=4917.728`
- `route_conditioning_vent_gap_exceeded_source=terminal_gap`
- `route_conditioning_diagnostic_blocked_vent_scheduler=false`
- `pressure_monitor_blocked_vent_scheduler=false`
- `trace_write_blocked_vent_scheduler=false`
- `fail_closed_reason=route_conditioning_vent_gap_exceeded`

## Pressure source rejection context

`a2_4_pressure_source_not_v1_aligned` was emitted even though the pressure gate was not reached:

- `selected_pressure_source_for_conditioning_monitor=digital_pressure_gauge_continuous`
- `selected_pressure_source_for_pressure_gate=""`
- `pressure_points_completed=0`
- `points_completed=0`
- `sample_count_total=0`

Interpretation: conditioning monitor may use fresh/nonblocking continuous snapshots. V1-aligned source enforcement belongs to pressure gate/ready/sample stage only. If pressure gate is not reached because route conditioning fail-closed, an empty pressure gate source should not be reported as source-not-v1-aligned.

## Working interpretation for A2.8

A2.7 fixed direct diagnostic/pressure-monitor/trace-write blocking. The remaining terminal gap is a scheduler/reschedule/fail-path classification gap: the code detects a 4.9 s terminal scheduler loop gap but only records `terminal_gap`, so the next offline pass must classify the operation path and ensure defer/fail-closed paths either immediately return to the vent loop or safe-stop before slow aggregation.

## Safety notes

- Pressure did not exceed abort threshold: peak was `1072.489 hPa`.
- No pressure point completed and no sampling occurred.
- The run remained no-write evidence only and did not refresh `real_primary_latest`.
- This is not real acceptance evidence.
