# A2.21 / A2.19 corrected-COM v1_aligned real probe fail-closed summary

Run date: 2026-05-01

## Scope and one-run boundary

- Branch: `codex/run001-a1-no-write-dry-run`
- HEAD: `37572f82a72118e8375b775bdc8611f15cb71de8`
- Origin HEAD checked before run: `37572f82a72118e8375b775bdc8611f15cb71de8`
- Probe command was started once only.
- Code changes: none.
- Allowed artifact writes: `_handoff/a2_21_a2_19_operator_confirmation.json`, this summary, and generated probe artifacts.
- Not A3, not H2O, not full group, not multi-temperature.
- Not real acceptance evidence.

## Artifacts

- Wrapper output dir: `D:\gas_calibrator_step3a_a2_21_a2_19_corrected_com_v1_aligned_probe_20260501_114900`
- Underlying execution dir: `D:\output\run001_a2\co2_only_7_pressure_no_write\run_20260501_114749`
- Operator confirmation: `D:\gas_calibrator\_handoff\a2_21_a2_19_operator_confirmation.json`
- Wrapper summary: `D:\gas_calibrator_step3a_a2_21_a2_19_corrected_com_v1_aligned_probe_20260501_114900\summary.json`
- Underlying positive preseal evidence: `D:\output\run001_a2\co2_only_7_pressure_no_write\run_20260501_114749\positive_preseal_pressurization_evidence.json`
- Exit code: `2`

## Decision

- final_decision: `FAIL_CLOSED`
- fail_closed_reason: `a2_positive_preseal_pressure_overlimit`
- failure_stage: not populated in wrapper summary
- interrupted_execution: `false`
- execution_error: empty
- Root cause category: `preseal state machine / positive preseal pressure overlimit`
- Root cause candidate from evidence: `vent_close_timing_positive_preseal_ramp_exceeded_abort_cutoff_before_setpoint_or_output_enable`

## A2.21 COM mapping

- `advantech_com_shift_mapping_applied`: `true`
- `advantech_com_shift_delta`: `-8`
- `stale_advantech_ports_found`: `false`
- Actual downstream device port assignments:
  - humidity_generator: `COM16`
  - dewpoint_meter: `COM17`
  - thermometer: `COM18`
  - temperature_chamber: `COM19`
  - relay / relay_a: `COM20`
  - relay_8 / relay_b: `COM21`
  - pressure_gauge / pressure_meter / P3: `COM22`
  - pressure_controller / PACE: `COM23`
- Stale `devices.*.port` assignments in downstream aligned config: none.
- Note: the aligned config still contains historical metadata strings such as `advantech_com_shift_old_range=COM24-COM31`, rejected candidate fields `COM30/COM31`, and candidate relay ports `COM28/COM29`. Actual device port assignments are clean, but a literal whole-file string scan still finds COM24-COM31 in metadata/reference fields.

## Device precheck

- `critical_devices_failed`: `[]`
- `optional_context_devices_failed`: `[]`
- `device_precheck_legacy_expected_ports_match`: all true for pressure_controller, pressure_meter, relay_a, relay_b, temperature_chamber.
- `device_precheck_wrapper_underlying_config_match`: `true`
- Critical device gate did not block the probe.
- Wrapper post-processing did not record clean open/query evidence: `device_precheck_open_all_results.*.attempted=false`, `ok=null`; pressure controller identity query was `unsupported_identity_query_not_offline_decision`; pressure meter/P3 precheck raw response was empty and `pressure_meter_first_read_result=FAIL_CLOSED`.
- This was not the terminal fail-closed reason; the run reached route conditioning and positive-preseal logic.

## A2.19 preseal state machine fields

- `preseal_guard_armed`: `true`
- `preseal_guard_armed_at`: `2026-05-01T03:53:37.705516+00:00`
- `preseal_guard_arm_source`: `pre_vent_close_pressure_guard`
- `preseal_guard_armed_from_vent_close_command`: `false`
- `vent_close_to_preseal_guard_arm_latency_s`: `null`
- `vent_close_to_positive_preseal_start_latency_s`: `null`
- `vent_off_settle_wait_pressure_monitored`: `false`
- `vent_off_settle_wait_overlimit_seen`: `false`
- `vent_off_settle_wait_ready_to_seal_seen`: `false`
- `first_target_ready_to_seal_min_hpa`: `1100.0`
- `first_target_ready_to_seal_max_hpa`: `1112.0`
- `first_target_ready_to_seal_pressure_hpa`: `null`
- `first_target_ready_to_seal_elapsed_s`: `null`
- `first_target_ready_to_seal_before_abort`: `false`
- `first_target_ready_to_seal_missed`: `true`
- `first_target_ready_to_seal_missed_reason`: `abort_before_ready_to_seal`
- `first_over_abort_pressure_hpa`: `1307.896`
- `first_over_abort_elapsed_s`: `2.2206859588623047`
- `first_over_abort_source`: `pressure_gauge`
- `first_over_abort_sample_age_s`: `0.0`
- `first_over_abort_to_abort_latency_s`: `0.0`
- `positive_preseal_guard_started_before_first_over_abort`: `true`
- `positive_preseal_guard_started_after_first_over_abort`: `false`
- `positive_preseal_guard_late_reason`: empty
- `seal_command_allowed_after_atmosphere_vent_closed`: `false`
- `seal_command_blocked_reason`: `preseal_abort_pressure_exceeded`
- `pressure_control_started_after_seal_confirmed`: `false`
- `setpoint_command_blocked_before_seal`: `false`
- `output_enable_blocked_before_seal`: `false`
- `normal_atmosphere_vent_attempted_after_pressure_points_started`: `false`
- `normal_atmosphere_vent_blocked_after_pressure_points_started`: `false`
- `emergency_relief_after_pressure_control_is_abort_only`: `false`
- `resume_after_emergency_relief_allowed`: `false` in wrapper summary; `null` in positive preseal evidence sample.

## Pressure and command timeline

- Overall first measured `>1100 hPa` in pressure latency samples: `2026-05-01T03:48:32.989770+00:00`, `1249.738 hPa`.
- Overall first measured `>1150 hPa` in pressure latency samples: `2026-05-01T03:48:32.989770+00:00`, `1249.738 hPa`.
- Positive-preseal first overlimit audit: elapsed `-0.778 s`, `1154.074 hPa`, source `digital_pressure_gauge_continuous`, sequence `226`.
- Positive-preseal guard sample / first over abort: `2026-05-01T03:53:39.926303+00:00`, `1307.896 hPa`.
- Pressure peak: `1307.896 hPa` at `2026-05-01T03:53:39.926303+00:00`.
- Vent close command completion: `2026-05-01T03:53:36.528999+00:00`; IO TX `vent(False)` at local `2026-05-01T11:53:34.371`.
- Seal command: not sent.
- Pressure setpoint command: not sent.
- Output enable command: not sent.
- Output disable commands observed: local `2026-05-01T11:53:40.484`, `2026-05-01T11:53:56.567`, `2026-05-01T11:53:59.869`.
- Relief vent commands observed: local `2026-05-01T11:53:42.616` after pressure-seal failure, and `2026-05-01T11:54:02.008` during final safe stop.

## Points, samples, and traces

- `pressure_points_completed`: `0`
- `points_completed`: `0`
- `sample_count_total`: `0`
- `artifact_completeness_pass`: `true`
- Wrapper `a2_pressure_sweep_trace.jsonl`: 7 lines, 9282 bytes
- Wrapper `route_trace.jsonl`: 337 lines, 474703 bytes
- Wrapper `pressure_trace.jsonl`: 321 lines, 477250 bytes
- Underlying `workflow_timing_trace.jsonl`: 4076 lines, 13965377 bytes
- `artifact_finalize_duration_s`: `4.668`
- `safe_stop_duration_s`: `23.773`
- `trace_file_size_guard_triggered`: `false`
- `trace_inline_load_blocked`: `false`
- `trace_event_truncated_count`: `12`

## No-write and forbidden actions

- `no_write`: `true`
- `no_write_assertion_status`: `pass`
- `attempted_write_count`: `0`
- `any_write_command_sent`: `false`
- `identity_write_command_sent`: `false`
- `mode_switch_command_sent`: `false`
- `senco_write_command_sent`: `false`
- `calibration_write_command_sent`: `false`
- `chamber_set_temperature_command_sent`: `false`
- `chamber_start_command_sent`: `false`
- `chamber_stop_command_sent`: `false`
- `real_primary_latest_refresh`: `false`
- Final chamber stop was blocked by no-write policy: `final_safe_stop_chamber_stop_blocked_by_no_write=true`.

## Regression checks

- A2.18 positive preseal overlimit did recur: peak `1307.896 hPa`, so the old 1305 hPa-class issue is not closed.
- A2.17 prearm baseline evidence still reports `prearm_pressure_source_disagreement=true` with reason `digital_latest_stale_pace_aux_disagreement`, although `prearm_pressure_source_alignment_ok=true`; this is not a clean non-regression.
- A2.17 cleanup/safe-stop relief was not classified as after-flush maintenance vent; cleanup classification was `emergency_abort_relief`.
- A2.20 device_precheck config port mismatch did not recur in actual port assignments; all expected critical ports matched.
- Trace guard did not report runaway/file-size guard trigger.
- No-write assertion passed.
- V1 and `run_app.py` remained untouched.

## Next step

- Do not rerun the real probe automatically.
- Do not enter A3.
- Recommended next step: offline failure audit focused on positive-preseal vent-off-to-seal timing and the pressure-source/prearm disagreement evidence.
