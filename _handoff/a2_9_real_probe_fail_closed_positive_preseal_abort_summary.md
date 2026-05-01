# A2.9 real engineering probe FAIL_CLOSED handoff

- Branch: codex/run001-a1-no-write-dry-run
- HEAD used: 2be2a107b53d57dd80a20859f6d7fe97b961535e
- Output dir: D:\gas_calibrator_step3a_a2_9_v1_aligned_co2_7_pressure_no_write_skip_temp_wait_20260429_1143
- Underlying execution dir: D:\output\run001_a2\co2_only_7_pressure_no_write\run_20260429_114540
- Evidence source: real_probe_a2_9_co2_7_pressure_no_write
- Acceptance level: engineering_probe_only
- Not real acceptance evidence: true
- Promotion state: blocked
- final_decision: FAIL_CLOSED
- rejection_reasons: [a2_route_conditioning_vent_blocked_after_flush_phase]
- A3 allowed: false

## Key facts

- Downstream aligned config explicitly set workflow.pressure.a2_conditioning_pressure_source=v1_aligned.
- Wrapper summary still reported a2_conditioning_pressure_source_strategy=continuous because the summary fallback reads the original raw config when no conditioning-monitor metric is emitted. Treat this as an A2 evidence aggregation bug for the next offline fix; do not reinterpret the actual downstream config as pure continuous.
- Route conditioning did not fail on a pressure-monitor terminal gap in this run.
- route_conditioning_vent_gap_exceeded=false.
- terminal_gap_source="" and terminal_gap_duration_ms=null.
- pressure_monitor_nonblocking=false because the run did not reach the high-frequency conditioning monitor evidence path that emits the A2.9 fields.
- The actual fail path was after route conditioning: positive_preseal_pressurization exceeded abort pressure.
- positive_preseal_abort: pressure_hpa=1280.989, preseal_abort_pressure_hpa=1150.0, abort_reason=preseal_abort_pressure_exceeded, seal_command_sent=false, sealed=false, pressure_control_started=false.
- Cleanup/final-safe-stop attempted vent pulses after route_conditioning_phase_not_flush; A2.9 safety blocked them, producing vent_pulse_blocked_after_flush_phase=true and wrapper rejection a2_route_conditioning_vent_blocked_after_flush_phase.

## No-write / bounds

- no_write=true
- attempted_write_count=0
- any_write_command_sent=false
- identity_write_command_sent=false
- mode_switch_command_sent=false
- senco_write_command_sent=false
- calibration_write_command_sent=false
- chamber_write_register_command_sent=false
- chamber_set_temperature_command_sent=false
- chamber_start_command_sent=false
- chamber_stop_command_sent=false
- real_primary_latest_refresh=false
- temperature_stabilization_wait_skipped=true
- temperature_gate_mode=current_pv_engineering_probe

## Point results

- pressure_points_expected=7
- pressure_points_completed=0
- points_completed=0
- sample_count_total=0
- sample_count_by_pressure={1100:0,1000:0,900:0,800:0,700:0,600:0,500:0}

## Next A2-only repair direction

- Do not rerun A2.9 in the same round.
- Stay in A2; do not enter A3/H2O/full group/multi-temperature.
- Fix evidence aggregation so summary reports v1_aligned from execution_config/downstream_aligned_config when no runtime conditioning-monitor metric is emitted.
- Review positive preseal cleanup/final-safe-stop vent policy after a pressure abort: safety correctly prevented vent after flush, but the wrapper currently treats the blocked cleanup vent as the primary rejection instead of preserving positive_preseal_abort as the root fail-closed reason.
- Review why the first point overshot to 1280.989 hPa before ready/seal; this is not the A2.8 pressure-monitor gap root cause.
