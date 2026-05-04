# A2.22 / A2.19 corrected-COM v1_aligned real probe fail-closed summary

Run date: 2026-05-01

## Scope and one-run boundary

- Branch: `codex/run001-a1-no-write-dry-run`
- HEAD: `5ff58de2cfd6e4cb8e3198a175502bb166dafe93`
- Origin HEAD checked before run: `5ff58de2cfd6e4cb8e3198a175502bb166dafe93`
- Probe command with `--execute-probe` was started once only.
- Code changes: none.
- Allowed artifact writes: `_handoff/a2_22_a2_19_operator_confirmation.json`, this summary, and generated probe artifacts.
- Not A3, not H2O, not full group, not multi-temperature.
- Not real acceptance evidence.

## Artifacts

- Wrapper output dir: `D:\gas_calibrator_step3a_a2_22_a2_19_corrected_com_v1_aligned_probe_20260501_1242`
- Underlying execution dir: `D:\output\run001_a2\co2_only_7_pressure_no_write\run_20260501_124308`
- Operator confirmation: `D:\gas_calibrator\_handoff\a2_22_a2_19_operator_confirmation.json`
- Wrapper summary: `D:\gas_calibrator_step3a_a2_22_a2_19_corrected_com_v1_aligned_probe_20260501_1242\summary.json`
- Underlying positive preseal evidence: `D:\output\run001_a2\co2_only_7_pressure_no_write\run_20260501_124308\positive_preseal_pressurization_evidence.json`
- Underlying high-pressure first point evidence: `D:\output\run001_a2\co2_only_7_pressure_no_write\run_20260501_124308\high_pressure_first_point_evidence.json`
- Underlying route-open surge evidence: `D:\output\run001_a2\co2_only_7_pressure_no_write\run_20260501_124308\route_open_pressure_surge_evidence.json`
- Process result: nonzero / fail-closed.

## Decision

- `final_decision`: `FAIL_CLOSED`
- `fail_closed_reason`: `a2_pass_conditions_not_met`
- `failure_stage`: not populated in wrapper summary
- Underlying abort reason: `co2_preseal_atmosphere_flush_abort_pressure_exceeded`
- Root cause category: pressure exceeded the 1150 hPa abort threshold before ready-to-seal/seal could occur.
- Root cause evidence: after vent-off, first `>1150 hPa` pressure sample was `1153.674 hPa` at `2026-05-01T04:48:57.044399+00:00`; high-pressure first point evidence then saw `1250.07 hPa` at `2026-05-01T04:48:58.421138+00:00` and aborted. The A2.22 vent-off settle monitor did not start, so the ready-to-seal gate was not proven.

## A2.22 focus fields

- `route_conditioning_pressure_returned_to_atmosphere`: `true`
- `route_conditioning_atmosphere_stable_before_flush`: `true`
- `route_conditioning_atmosphere_stable_hold_s`: `2.0`
- `route_conditioning_high_pressure_seen_before_preseal`: `false`
- `preseal_guard_armed`: `false`
- `preseal_guard_arm_source`: empty
- `preseal_guard_armed_from_vent_close_command`: `false`
- `preseal_guard_arm_source_alignment_ok`: `false`
- `vent_close_command_sent_at`: empty in positive preseal evidence; IO TX `vent(False)` at local `2026-05-01T12:48:54.535`
- `vent_close_command_completed_at`: empty
- `vent_close_to_monitor_start_latency_s`: `null`
- `vent_off_settle_monitor_started`: `false`
- `vent_off_settle_wait_pressure_monitored`: `false`
- `vent_off_settle_monitor_sample_count`: `0`
- `vent_off_settle_first_ready_to_seal_sample_hpa`: `null`
- `vent_off_settle_first_over_abort_sample_hpa`: `null`
- `ready_to_seal_window_entered`: `false`
- `ready_to_seal_window_missed_reason`: empty
- `overlimit_elapsed_s_nonnegative`: `true`
- `overlimit_elapsed_source`: `vent_close_command_delta`
- `prearm_primary_source_disagreement`: `false`
- `prearm_aux_source_disagreement`: `true`
- `prearm_aux_source_disagreement_nonblocking`: `true`

## Pressure and command timeline

- `positive_preseal_pressure_hpa`: `null`
- `positive_preseal_abort_pressure_hpa`: `1150.0`
- Positive-preseal artifact peak: `1250.07 hPa`
- Overall pressure peak in `pressure_read_latency_samples.csv`: `1332.836 hPa` at `2026-05-01T04:49:01.115229+00:00`
- First `>1100 hPa`: `2026-05-01T04:43:53.164336+00:00`, `1104.54 hPa`, source `digital_pressure_gauge_continuous`
- First `>1150 hPa`: `2026-05-01T04:48:57.044399+00:00`, `1153.674 hPa`, source `digital_pressure_gauge_continuous`
- Vent close command: IO TX `vent(False)` at local `2026-05-01T12:48:54.535`
- Ready-to-seal time/pressure: none
- Seal command time: none
- Pressure setpoint command time: none
- Output enable time: none
- Output disable times observed: local `2026-05-01T12:43:33.537`, `12:48:53.478`, `12:48:59.369`, `12:49:14.737`, `12:49:18.018`
- Relief vent commands observed: local `2026-05-01T12:49:01.505` after fail-closed and `12:49:20.156` during final safe stop

## Points, samples, and traces

- `pressure_points_completed`: `0`
- `points_completed`: `0`
- `sample_count_total`: `0`
- `artifact_completeness_pass`: `true`
- Wrapper `a2_pressure_sweep_trace.jsonl`: 7 lines, 9317 bytes
- Wrapper `route_trace.jsonl`: 335 lines, 467273 bytes
- Wrapper `pressure_trace.jsonl`: 319 lines, 469931 bytes
- Underlying `workflow_timing_trace.jsonl`: 4069 lines, 14218995 bytes
- `artifact_finalize_duration_s`: `4.57`
- `safe_stop_duration_s`: `23.265`
- `trace_file_size_guard_triggered`: `false`
- `trace_inline_load_blocked`: `false`
- `trace_event_truncated_count`: `8`

## Regression checks

- A2.21 COM mapping did not regress: `pressure_controller=COM23`, `pressure_meter=COM22`, `relay_a=COM20`, `relay_b=COM21`.
- A2.20 device precheck did not block: `critical_devices_failed=[]`, `optional_context_devices_failed=[]`, `device_precheck_failure_stage=""`.
- A2.17 cleanup/safe-stop relief was not misclassified as after-flush maintenance vent: `cleanup_vent_classification=cleanup_relief`, `cleanup_vent_is_normal_maintenance=false`, `cleanup_vent_is_safe_stop_relief=true`, `vent_blocked_after_flush_phase_is_failure=false`.
- No-write passed: `no_write_assertion_status=pass`, `attempted_write_count=0`, `any_write_command_sent=false`.
- Forbidden writes stayed blocked: `identity_write_command_sent=false`, `senco_write_command_sent=false`, `calibration_write_command_sent=false`, `chamber_set_temperature_command_sent=false`, `chamber_start_command_sent=false`, `chamber_stop_command_sent=false`.
- `real_primary_latest_refresh=false`.
- Trace guard did not run away.
- `v1_untouched=true`; `run_app_py_untouched=true`.

## Boundaries confirmed

- The probe was not repeated.
- Code was not modified.
- ID/SENCO/calibration coefficients were not written.
- Temperature chamber SV was not written.
- Temperature chamber was not started or stopped.
- `real_primary_latest` was not refreshed.
- A3 was not entered.
- H2O was not run.
- Full group was not run.
- Multi-temperature was not run.
- This is engineering probe evidence only, not real acceptance.

## Next step

- Do not rerun automatically.
- Do not enter A3.
- Recommended next step: offline failure audit focused on why the A2.22 vent-off settle monitor did not arm/start before the high-pressure readiness abort, and on the physical pressure surge after vent-off while keeping the 1150 hPa abort threshold unchanged.
