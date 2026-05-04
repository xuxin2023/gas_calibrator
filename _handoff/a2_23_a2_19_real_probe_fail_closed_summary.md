# A2.23 / A2.19 corrected-COM v1_aligned real probe fail-closed summary

Run date: 2026-05-01

## Scope and one-run boundary

- Branch: `codex/run001-a1-no-write-dry-run`
- HEAD / origin baseline: `076191058acf26a8962654c773fae1953001ff11`
- Probe command with `--execute-probe` was started once only.
- Code changes: none.
- Allowed artifact writes: `_handoff/a2_23_a2_19_operator_confirmation.json`, this summary, and generated probe artifacts.
- Not A3, not H2O, not full group, not multi-temperature.
- Not real acceptance evidence.

## Artifacts

- Wrapper output dir: `D:\gas_calibrator_step3a_a2_23_a2_19_corrected_com_v1_aligned_probe_20260501_1346`
- Underlying execution dir: `D:\output\run001_a2\co2_only_7_pressure_no_write\run_20260501_134733`
- Operator confirmation: `D:\gas_calibrator\_handoff\a2_23_a2_19_operator_confirmation.json`
- Wrapper summary: `D:\gas_calibrator_step3a_a2_23_a2_19_corrected_com_v1_aligned_probe_20260501_1346\summary.json`
- Underlying high-pressure first point evidence: `D:\output\run001_a2\co2_only_7_pressure_no_write\run_20260501_134733\high_pressure_first_point_evidence.json`
- Underlying positive preseal evidence: `D:\output\run001_a2\co2_only_7_pressure_no_write\run_20260501_134733\positive_preseal_pressurization_evidence.json`
- Underlying workflow timing summary: `D:\output\run001_a2\co2_only_7_pressure_no_write\run_20260501_134733\workflow_timing_summary.json`
- Process result: nonzero / fail-closed.

## Decision

- `final_decision`: `FAIL_CLOSED`
- `fail_closed_reason`: `a2_pass_conditions_not_met`
- Underlying final decision: `FAIL`
- Root cause: high-pressure first-point prearm blocked before vent-off because the baseline pressure sample was stale.
- Specific root cause evidence:
  - `high_pressure_first_point_prearm_blocked=true`
  - `high_pressure_first_point_prearm_block_reason=baseline_pressure_sample_stale`
  - baseline pressure sample: `1014.42 hPa` at `2026-05-01T05:53:15.055085+00:00`
  - baseline sample age: `0.788 s`, stale threshold `0.5 s`
  - `baseline_pressure_stale_reason=digital_latest_stale_pace_aux_disagreement`
  - pressure-source disagreement: digital `1014.42 hPa` vs PACE `3.4828198 hPa`, disagreement `1010.937 hPa`

## Required output fields

- `pressure_points_completed`: `0`
- `points_completed`: `0`
- `sample_count_total`: `0`
- Pressure peak in `pressure_read_latency_samples.csv`: `1068.693 hPa` at `2026-05-01T05:48:16.607313+00:00`, source `digital_pressure_gauge_continuous`
- `positive_preseal_pressure_peak_hpa`: `1068.693`
- First `>1100 hPa`: none observed
- First `>1150 hPa`: none observed
- `preseal_capture_started`: `false`
- `preseal_capture_not_pressure_control`: `false`
- `preseal_capture_pressure_rise_expected_after_vent_close`: `false`
- `preseal_capture_monitor_armed_before_vent_close_command`: `false`
- `preseal_capture_monitor_covers_abort_path`: `false`
- `preseal_guard_armed`: `false`
- `preseal_guard_arm_source`: empty
- `preseal_guard_armed_from_vent_close_command`: `false`
- `vent_off_settle_wait_pressure_monitored`: `false`
- `vent_off_settle_monitor_sample_count`: `0`
- `preseal_capture_pressure_rise_rate_hpa_per_s`: `null`
- `preseal_capture_seal_completion_latency_s`: `null`
- `preseal_capture_predicted_seal_completion_pressure_hpa`: `null`
- `preseal_capture_predictive_ready_to_seal`: `false`
- `ready_to_seal_window_entered`: `false`
- `first_target_ready_to_seal_pressure_hpa`: `null`
- `seal_command_allowed_after_atmosphere_vent_closed`: `false`
- `preseal_capture_abort_reason`: empty
- `preseal_capture_abort_pressure_hpa`: `null`
- `positive_preseal_pressure_hpa`: `null`
- `positive_preseal_pressure_source_path`: empty
- `monitor_context_propagated_to_wrapper_summary`: `false`

## Command ordering and no-write evidence

- Vent-off command time: none; `vent_off_command_sent=false`
- Seal command time: none; `seal_command_sent=false`
- Pressure setpoint command time: none; `pressure_setpoint_command_sent=false`
- Output enable time: none; `positive_preseal_output_enable_sent=false`
- Output disable was sent as a safe command; no output enable or setpoint was sent before seal.
- No-write passed:
  - `no_write_assertion_status=pass`
  - `attempted_write_count=0`
  - `any_write_command_sent=false`
  - `identity_write_command_sent=false`
  - `senco_write_command_sent=false`
  - `calibration_write_command_sent=false`
  - `chamber_set_temperature_command_sent=false`
  - `chamber_start_command_sent=false`
  - `chamber_stop_command_sent=false`
  - `final_safe_stop_chamber_stop_blocked_by_no_write=true`
  - `real_primary_latest_refresh=false`

## Trace and finalize size

- Wrapper artifact total size: `1,148,595 bytes`
- Wrapper traces:
  - `a2_pressure_sweep_trace.jsonl`: `9,282 bytes`, `7` rows
  - `route_trace.jsonl`: `457,540 bytes`, `332` rows
  - `pressure_trace.jsonl`: `460,077 bytes`, `316` rows
  - `pressure_ready_trace.jsonl`: `6,379 bytes`, `7` rows
  - `heartbeat_trace.jsonl`: `2,653 bytes`, `7` rows
  - `analyzer_sampling_rows.jsonl`: `0 bytes`, `0` rows
- Underlying artifact total size: `31,189,553 bytes`
- Underlying `workflow_timing_trace.jsonl`: `14,095,932 bytes`, `4025` events
- Trace guard: `trace_file_size_guard_triggered=false`, `trace_inline_load_blocked=false`, `trace_event_truncated_count=0`
- Artifact completeness: `artifact_completeness_pass=true`
- Underlying `artifact_finalize_duration_s`: `4.69`
- Underlying `safe_stop_duration_s`: `24.148`

## Interpretation

- The objective was not met: predictive ready-to-seal and preseal capture monitor were not exercised because the run failed closed before vent-off.
- This was a safe fail-closed result: the probe stopped before seal, before pressure setpoint/output enable, before sampling, and before any write-capable action.
- `positive_preseal_pressure_hpa` is `null` because positive-preseal did not execute; this run does not provide evidence that the prior path-split null issue is fixed in an executed preseal-capture path.

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
- Next step: offline failure audit focused on stale baseline pressure selection and digital/PACE source disagreement in the v1_aligned prearm path.
