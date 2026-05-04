# A2.17 real probe FAIL_CLOSED audit: positive preseal pressure overlimit

## Scope
- Probe: A2.17/A2.13 v1_aligned CO2-only / skip0 / single route / single temperature / seven pressure / no-write / skip temperature stabilization wait.
- Execution count: exactly one real-machine engineering probe execution in this round.
- Output directory: `D:\gas_calibrator_step3a_a2_17_v1_aligned_co2_7_pressure_no_write_skip_temp_wait_after_prearm_cleanup_fix_20260501_0818`
- Underlying execution: `D:\gas_calibrator_step3a_a2_17_v1_aligned_co2_7_pressure_no_write_skip_temp_wait_after_prearm_cleanup_fix_20260501_0818\underlying_execution\run_20260501_081849`
- Wrapper exit code: `2`
- Process state: `completed`
- Artifact completeness: `true`
- Not real acceptance evidence; promotion remains blocked; A3 not allowed.

## Final decision
- `final_decision=FAIL_CLOSED`
- `fail_closed_reason=a2_positive_preseal_pressure_overlimit`
- `rejection_reasons=["a2_positive_preseal_pressure_overlimit"]`
- `pressure_points_completed=0`
- `points_completed=0`
- `sample_count_total=0`

## What A2.17 fixed relative to A2.16R
- High-pressure first-point prearm was not blocked.
- `high_pressure_first_point_prearm_started=true`
- `high_pressure_first_point_prearm_blocked=false`
- `high_pressure_first_point_prearm_block_reason=""`
- `baseline_pressure_hpa=1014.967`
- `baseline_pressure_source=digital_pressure_gauge_continuous`
- `baseline_pressure_sample_age_s=0.793`
- `baseline_pressure_freshness_ok=true`
- `baseline_pressure_stale_reason=""`
- `prearm_pressure_source_expected=v1_aligned`
- `prearm_pressure_source_observed="digital_pressure_gauge_continuous vs pace_controller"`
- `prearm_pressure_source_alignment_ok=true`
- `prearm_pressure_source_disagreement=true`
- `prearm_pressure_source_disagreement_reason=digital_latest_stale_pace_aux_disagreement`
- `v1_aligned_pressure_source_decision=latest_route_conditioning_pressure_selected_for_prearm_baseline`
- `latest_route_conditioning_pressure_hpa=1014.967`
- `latest_route_conditioning_pressure_source=digital_pressure_gauge_continuous`
- `latest_route_conditioning_pressure_age_s=1.433`
- `latest_route_conditioning_pressure_eligible_for_prearm_baseline=true`

Interpretation: A2.17 correctly allowed the latest route-conditioning atmospheric pressure as the prearm baseline under the v1_aligned strategy, so the A2.16R stale-baseline/source-disagreement fail_closed did not recur.

## Cleanup/final safe-stop vent classification
- `normal_maintenance_vent_blocked_after_flush_phase=false`
- `vent_blocked_after_flush_phase_is_failure=false`
- `vent_blocked_after_flush_phase_context={}`
- Wrapper did not report `a2_route_conditioning_vent_blocked_after_flush_phase`.
- Emergency abort relief was allowed and sent after positive preseal overlimit:
  - `emergency_abort_relief_vent_required=true`
  - `emergency_abort_relief_vent_allowed=true`
  - `emergency_abort_relief_vent_command_sent=true`
  - `emergency_abort_relief_reason=positive_preseal_abort_pressure_exceeded`
  - `safe_stop_pressure_relief_result=command_sent`

Interpretation: A2.17 cleanup/safe-stop vent classification fix held. Safe relief was not conflated with normal maintenance vent after flush.

## New failure root cause
The route conditioning phase passed and then high-pressure prearm armed. During positive preseal pressurization, pressure rose beyond the abort threshold before seal or sampling:

- `measured_atmospheric_pressure_hpa=1014.967`
- `route_conditioning_pressure_overlimit=false`
- `route_conditioning_hard_abort_exceeded=false`
- `route_conditioning_vent_gap_exceeded=false`
- `high_pressure_first_point_decision=abort`
- `positive_preseal_phase_started=true`
- `positive_preseal_pressure_hpa=1305.784`
- `positive_preseal_pressure_source=pressure_gauge`
- `positive_preseal_pressure_sample_age_s=0.0`
- `positive_preseal_abort_pressure_hpa=1150.0`
- `positive_preseal_pressure_overlimit=true`
- `positive_preseal_abort_reason=preseal_abort_pressure_exceeded`
- `positive_preseal_seal_command_sent=false`
- `positive_preseal_pressure_setpoint_command_sent=false`
- `positive_preseal_sample_started=false`

The pressure jump was observed at the positive-preseal guard and triggered the fail-closed safety abort before any seal/sample/write. The next audit should focus on A2 high-pressure first-point positive preseal ramp/vent-close timing and pressure source sampling cadence, not on trace guard, COM mapping, temperature, cleanup vent classification, or prearm baseline admission.

## Vent gap and trace guard
- `max_vent_pulse_write_gap_ms_including_terminal_gap=1369.369`
- `max_vent_pulse_write_gap_phase=route_conditioning_flush_phase`
- `max_vent_pulse_gap_limit_ms=2000.0`
- `max_vent_pulse_write_gap_exceeded=false`
- `route_conditioning_vent_gap_exceeded=false`
- `workflow_timing_trace=13,942,904 bytes`
- `wrapper route_trace=475,391 bytes`
- `wrapper pressure_trace=478,181 bytes`
- `trace_file_size_guard_triggered=false`
- `trace_large_line_warning_count=0`
- `trace_streaming_read_used=true`
- `trace_inline_load_blocked=false`

## No-write and boundaries
- `no_write=true`
- `no_write_assertion_status=pass`
- `attempted_write_count=0`
- `any_write_command_sent=false`
- `identity_write_command_sent=false`
- `senco_write_command_sent=false`
- `calibration_write_command_sent=false`
- `chamber_set_temperature_command_sent=false`
- `chamber_start_command_sent=false`
- `chamber_stop_command_sent=false`
- `real_primary_latest_refresh=false`
- No A3, no H2O, no full group, no multi-temperature, no MODE switch.
