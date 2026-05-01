# A2.19 real probe FAIL_CLOSED summary

## Scope
- Probe: A2.19/A2.13 v1_aligned CO2-only / skip0 / single route / single temperature / seven pressure / no-write / skip temperature stabilization wait.
- Execution count in this round: exactly one real-machine engineering probe execution.
- Wrapper output directory: `D:\gas_calibrator_step3a_a2_19_v1_aligned_co2_7_pressure_no_write_skip_temp_wait_vent_off_to_seal_probe_20260501_103830`
- Underlying execution directory: `D:\output\run001_a2\co2_only_7_pressure_no_write\run_20260501_103832`
- Wrapper process: PID `14904`, started 2026-05-01 10:38:30 +08:00, exited naturally before final audit.
- Not real acceptance evidence; promotion remains blocked; A3 not allowed.

## Git gate
- Branch: `codex/run001-a1-no-write-dry-run`
- HEAD: `112d887a825cd72bae0c31b29505306203898dac`
- origin/codex/run001-a1-no-write-dry-run: `112d887a825cd72bae0c31b29505306203898dac`
- `git status --short`: only `_handoff/` untracked before the run.
- `run_app.py` and `src/gas_calibrator/v1`: no diff before and after the run.
- `git diff --check`: clean before and after the run.

## Final decision
- `final_decision=FAIL_CLOSED`
- Wrapper `fail_closed_reason=a2_pass_conditions_not_met`
- Underlying root cause: `failure_stage=device_precheck`
- Underlying failure reason: `Device precheck failed [failed_devices=['pressure_controller', 'pressure_meter', 'relay_a', 'relay_b']]`
- Run log also recorded `pressure_controller`, `pressure_meter`, `relay_a`, and `relay_b` as critical failed devices, with `optional_context_devices_failed=['temperature_chamber']`.
- `pressure_points_completed=0`
- `points_completed=0`
- `sample_count_total=0`
- `artifact_completeness_pass=true`

## A2.19 preseal state machine result
The A2.19 vent-off-to-seal preseal state machine was not reached, so this run does not verify whether A2.19 fixes the A2.17/A2.18 positive preseal overlimit.

Key fields:
- `preseal_guard_armed=false`
- `preseal_guard_armed_at=""`
- `preseal_guard_arm_source=""`
- `preseal_guard_armed_from_vent_close_command=false`
- `vent_close_to_preseal_guard_arm_latency_s=null`
- `vent_close_to_positive_preseal_start_latency_s=null`
- `vent_off_settle_wait_pressure_monitored=false`
- `vent_off_settle_wait_overlimit_seen=false`
- `vent_off_settle_wait_ready_to_seal_seen=false`
- `first_target_ready_to_seal_pressure_hpa=null`
- `first_target_ready_to_seal_elapsed_s=null`
- `first_target_ready_to_seal_before_abort=false`
- `first_target_ready_to_seal_missed=false`
- `first_target_ready_to_seal_missed_reason=""`
- `first_over_abort_pressure_hpa=null`
- `first_over_abort_elapsed_s=null`
- `first_over_abort_source=""`
- `first_over_abort_sample_age_s=null`
- `first_over_abort_to_abort_latency_s=null`
- `positive_preseal_guard_started_before_first_over_abort=false`
- `positive_preseal_guard_started_after_first_over_abort=false`
- `positive_preseal_guard_late_reason=""`
- `seal_command_allowed_after_atmosphere_vent_closed=false`
- `seal_command_blocked_reason=""`
- `pressure_control_started_after_seal_confirmed=false`
- `setpoint_command_blocked_before_seal=false`
- `output_enable_blocked_before_seal=false`
- `normal_atmosphere_vent_attempted_after_pressure_points_started=false`
- `normal_atmosphere_vent_blocked_after_pressure_points_started=false`
- `emergency_relief_after_pressure_control_is_abort_only=false`
- `resume_after_emergency_relief_allowed=null` in underlying positive preseal evidence; wrapper summary normalized it to `false`.

## Pressure and command timeline
- `positive_preseal_pressure_hpa=null`
- `positive_preseal_abort_pressure_hpa=1150.0`
- Pressure peak: none observed; no valid pressure sample with pressure value was recorded.
- First `>1100 hPa`: none observed.
- First `>1150 hPa`: none observed.
- Seal command: not sent.
- Pressure setpoint command: not sent.
- Output enable command: not sent.
- Output disable command: attempted as safe-stop at `2026-05-01T02:40:17.148998+00:00`, failed with `PACE_COMMAND_ERROR(command=:OUTP:STAT 0, error=)`.
- Relief / vent command: attempted at `2026-05-01T02:41:35.210161+00:00`, failed with `PACE_COMMAND_ERROR(command=:OUTP:STAT 0, error=)`.
- Final safe-stop pressure record: `2026-05-01T02:42:36.040670+00:00`, pressure values unavailable.

## Old issue regression checks
- A2.17 prearm baseline did not regress to stale/source disagreement; it was not evaluated because high-pressure prearm did not start. Underlying high-pressure evidence has `high_pressure_first_point_prearm_started=false`, `prearm_pressure_source_alignment_ok=true`, and `prearm_pressure_source_disagreement=false`.
- A2.17 cleanup/safe-stop relief was not misclassified as after-flush maintenance vent; `normal_maintenance_vent_blocked_after_flush_phase=false` and `vent_blocked_after_flush_phase_is_failure=false`.
- Trace guard did not trigger runaway: `trace_file_size_guard_triggered=false`, `trace_large_line_warning_count=0`, `trace_streaming_read_used=true`, `trace_inline_load_blocked=false`.
- No-write assertion passed: `no_write_assertion_status=pass`, `attempted_write_count=0`, `any_write_command_sent=false`.

## Artifact metrics
- Wrapper route trace: 13 rows, `7,864` bytes.
- Wrapper pressure trace: 0 rows, `0` bytes.
- Wrapper A2 pressure sweep trace: 7 rows, `9,284` bytes.
- Underlying route trace: `7,435` bytes.
- Underlying workflow timing trace: `13,652` bytes.
- Underlying workflow timing event count: `15`.
- `artifact_finalize_duration_s=3.357`
- `safe_stop_duration_s=164.782`
- `total_duration_s=251.124`

## Boundary confirmations
- No ID write.
- No SENCO write.
- No calibration coefficient write.
- No temperature chamber SV write.
- No temperature chamber start/stop.
- No MODE switch.
- No `real_primary_latest` refresh.
- No A3.
- No H2O.
- No full group.
- No multi-temperature.
- V1 untouched.
- `run_app.py` untouched.
- This is not real acceptance evidence.

## Next step
- Do not rerun this real probe in the current round.
- Next step should be offline failure audit of the `device_precheck` / pressure-controller / pressure-meter / relay initialization path only.
