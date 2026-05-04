# A2.6 real probe FAIL_CLOSED: diagnostic terminal gap

This handoff note preserves the single A2.6 Step3A engineering probe evidence for the next A2 offline fix pass.

- Output directory: `D:\gas_calibrator_step3a_a2_6_v1_aligned_co2_7_pressure_no_write_skip_temp_wait_20260429_0036`
- A2.6 commit HEAD: `8b8e3bdc217af6e913bba4b16b5a36fa7f137c84`
- Branch: `codex/run001-a1-no-write-dry-run`
- Evidence level: `engineering_probe_only`
- Not real acceptance evidence: `true`
- A3 allowed: `false`

## Summary fields

- `final_decision=FAIL_CLOSED`
- `rejection_reasons=["a2_route_conditioning_vent_gap_exceeded","a2_route_conditioning_diagnostic_blocked_vent_scheduler"]`
- `route_conditioning_phase=route_conditioning_flush_phase`
- `route_open_transition_started=true`
- `route_open_command_write_duration_ms=557.191`
- `route_open_transition_total_duration_ms=563.428`
- `vent_ticks_during_route_open_transition=1`
- `terminal_vent_write_age_ms_at_gap_gate=3800.794`
- `max_vent_pulse_write_gap_ms=592.8`
- `max_vent_pulse_write_gap_ms_including_terminal_gap=3800.794`
- `route_conditioning_vent_gap_exceeded_source=diagnostic`
- `route_conditioning_diagnostic_blocked_vent_scheduler=true`
- `max_vent_scheduler_loop_gap_ms=3800.794`
- `route_conditioning_pressure_after_route_open_hpa=1025.329`
- `route_conditioning_peak_pressure_hpa=1025.329`
- `route_conditioning_pressure_overlimit=false`
- `pressure_points_completed=0`
- `points_completed=0`
- `sample_count_total=0`
- `selected_pressure_source_for_conditioning_monitor=digital_pressure_gauge_continuous`
- `selected_pressure_source_for_pressure_gate=""`
- `selected_pressure_freshness_ok=true`

## Route trace context

The decisive route trace action is `co2_route_conditioning_diagnostic_blocked_vent_scheduler` in `route_trace.jsonl`.

Key route trace fields:

- `phase=conditioning_pressure_monitor`
- `route_conditioning_high_frequency_window_active=true`
- `vent_phase=route_open_high_frequency_vent_phase`
- `blocking_operation_name=a2_conditioning_pressure_monitor`
- `blocking_operation_duration_ms=3794.249`
- `diagnostic_duration_ms=3794.249`
- `last_blocking_operation_name=a2_conditioning_pressure_monitor`
- `last_blocking_operation_duration_s=3.7942494000308216`
- `terminal_vent_write_age_ms_at_gap_gate=3800.794`
- `max_vent_pulse_write_gap_ms_including_terminal_gap=3800.794`
- `route_conditioning_vent_gap_exceeded_source=diagnostic`
- `route_conditioning_diagnostic_blocked_vent_scheduler=true`
- `fail_closed_reason=route_conditioning_diagnostic_blocked_vent_scheduler`

Interpretation: route open itself completed in about 563 ms and the post-open fast vent write landed in about 2.667 ms, but the next pressure-monitor diagnostic occupied about 3.794 s during the high-frequency vent window. That delayed the next vent scheduler opportunity and caused the terminal gap gate to see 3800.794 ms, correctly FAIL_CLOSED.

## Safety notes

- Pressure did not exceed the abort threshold: peak was `1025.329 hPa`.
- No pressure point completed and no sampling occurred.
- The run remained no-write evidence only and did not refresh `real_primary_latest`.
- This is not real acceptance evidence.
