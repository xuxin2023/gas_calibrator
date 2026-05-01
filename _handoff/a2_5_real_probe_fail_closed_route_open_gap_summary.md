# A2.5 real probe FAIL_CLOSED: route-open terminal vent gap

- Branch: `codex/run001-a1-no-write-dry-run`
- A2.5 commit HEAD: `6c6dd7452763c2a8a04df55c97029ba8aef76118`
- Probe output dir: `D:\gas_calibrator_step3a_a2_5_v1_aligned_co2_7_pressure_no_write_skip_temp_wait_20260428_1828`
- Evidence source: `real_probe_a2_5_co2_7_pressure_no_write`
- Final decision: `FAIL_CLOSED`
- Rejection reasons: `["a2_route_conditioning_vent_gap_exceeded"]`

## Summary key fields

- `route_conditioning_phase=route_conditioning_flush_phase`
- `fast_vent_reassert_supported=true`
- `fast_vent_reassert_used=true`
- `pre_route_fast_vent_sent=true`
- `pre_route_fast_vent_duration_ms=0.12`
- `pre_route_fast_vent_timeout=false`
- `vent_command_write_duration_ms=0.12`
- `vent_command_total_duration_ms=0.12`
- `vent_command_capture_pressure_enabled=false`
- `vent_command_query_state_enabled=false`
- `vent_command_confirm_transition_enabled=false`
- `route_conditioning_fast_vent_command_timeout=false`
- `route_conditioning_fast_vent_not_supported=false`
- `route_conditioning_diagnostic_blocked_vent_scheduler=false`
- `vent_pulse_count=3`
- `vent_pulse_interval_ms=[1.302, 556.235]`
- `max_vent_pulse_write_gap_ms=556.235`
- `route_conditioning_vent_gap_exceeded=true`
- `route_conditioning_pressure_overlimit=false`
- `route_open_to_first_vent_write_ms=null`
- `route_open_to_first_pressure_read_ms=null`
- `pressure_points_completed=0`
- `points_completed=0`
- `sample_count_total=0`
- `no_write=true`
- `attempted_write_count=0`
- `any_write_command_sent=false`
- `real_primary_latest_refresh=false`

## Route trace heartbeat-gap context

Fail route trace action:

- `action=co2_route_conditioning_vent_heartbeat_gap`
- `result=fail`
- `fail_closed_reason=route_conditioning_vent_gap_exceeded`
- `route_conditioning_high_frequency_window_active=true`
- `vent_phase=route_open_high_frequency_vent_phase`
- `heartbeat_gap_threshold_ms=1000.0`
- `heartbeat_gap_observed_ms=2218.567`
- `heartbeat_emission_gap_ms=2218.145`
- `last_vent_command_write_sent_monotonic_s=339576.219518`
- `last_vent_tick_completed_monotonic_s=339576.21994`
- `max_vent_pulse_write_gap_ms=556.235`
- `vent_pulse_interval_ms=[1.302, 556.235]`
- `route_conditioning_pressure_overlimit=false`

## Terminal gap interpretation

A2.5 proved that the fast vent write itself is no longer the 12 s blocker:

- The final fast reassert write took `0.12 ms`.
- Fast path did not capture pressure.
- Fast path did not query output/isolation/vent status.
- Fast path did not wait after command.

However, the gate failed on the terminal gap from the last vent write to the gap check:

- Pulse-to-pulse max write gap: `556.235 ms`
- Terminal age at fail gate: `2218.567 ms`
- Gate threshold in the route-open high-frequency window: `1000 ms`

Therefore `max_vent_pulse_write_gap_ms=556.235` under-reported the actual no-vent window because it excluded the terminal age from the last pulse to the fail gate.

## A2.6 repair focus

- Make route-open command/settle/diagnostics sliced and scheduler-aware.
- Run fast vent maintenance inside route-open transition waits.
- Add terminal gap fields:
  - `terminal_vent_write_age_ms_at_gap_gate`
  - `max_vent_pulse_write_gap_ms_including_terminal_gap`
  - `route_conditioning_vent_gap_exceeded_source`
- Add route-open transition fields:
  - `route_open_transition_started`
  - `route_open_transition_started_at`
  - `route_open_command_write_duration_ms`
  - `route_open_settle_wait_sliced`
  - `route_open_settle_wait_slice_count`
  - `route_open_transition_blocked_vent_scheduler`
