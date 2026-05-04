# A2.13 Failure Audit: Workflow Timing Trace Runaway

## Scope

- A2 output directory: `D:\gas_calibrator_step3a_a2_13_v1_aligned_co2_7_pressure_no_write_skip_temp_wait_after_com_mapping_fix_20260430_1819`
- Audit type: offline artifact audit only
- Real COM used in this audit: false
- A2 rerun in this audit: false
- Not real acceptance evidence: true

## Decision

- `final_decision=FAIL_CLOSED`
- `fail_closed_reason=a2_route_conditioning_vent_gap_exceeded`
- `rejection_reasons=["a2_route_conditioning_vent_gap_exceeded","a2_route_conditioning_diagnostic_blocked_vent_scheduler"]`
- `no_write=true`
- `attempted_write_count=0`
- `any_write_command_sent=false`

## Vent Gap

- Vent gap: `2365.407 ms`
- Event: `co2_route_conditioning_terminal_vent_gap`
- Trace line:
  - `pressure_trace.jsonl` line `284`
  - `route_trace.jsonl` line `295`
- Event timestamp: `2026-04-30T10:25:25.811739+00:00`
- Last vent write sent: `2026-04-30T10:25:23.444516+00:00`
- Terminal gap detected: `2026-04-30T10:25:25.809974+00:00`
- Stage/phase:
  - `route_conditioning_phase=route_conditioning_flush_phase`
  - `vent_phase=route_conditioning_flush_maintenance_phase`
  - point `1`, `co2_groupa_100ppm_1100hpa`
- Gap limit: `2000.0 ms`
- Maximum non-terminal vent pulse write gap: `1810.779 ms`
- Terminal gap including final age: `2365.407 ms`

## Diagnostic Blocked Source

- `route_conditioning_diagnostic_blocked_vent_scheduler=true`
- `terminal_gap_source=pressure_monitor`
- `terminal_gap_operation=selected_pressure_sample_stale`
- `terminal_gap_stack_marker=defer:pressure_monitor:selected_pressure_sample_stale`
- `route_conditioning_vent_gap_exceeded_source=pressure_monitor`
- `diagnostic_deferred_count=211`
- `last_diagnostic_defer_component=pressure_monitor`
- `last_diagnostic_defer_operation=selected_pressure_sample_stale`
- `last_diagnostic_defer_at=2026-04-30T10:25:12.321404+00:00`
- The final blocking operation itself was short:
  - `diagnostic_blocking_operation=digital_gauge_continuous_latest_fresh`
  - `diagnostic_blocking_duration_ms=0.122`
  - `diagnostic_budget_exceeded=false`

Interpretation: the fail-closed source is not a long synchronous pressure query at the terminal moment. It is the scheduler's accounting path that associated the terminal vent gap with repeated pressure monitor stale-sample deferrals.

## Workflow Timing Runaway Evidence

- Original `workflow_timing_trace.jsonl`: `5,059,477,875 bytes`
- Original line count: `1941`
- Lines over `1 MB`: `876`
- Maximum line size: `14,010,219 bytes`
- Forensic summary:
  `D:\gas_calibrator_step3a_a2_13_v1_aligned_co2_7_pressure_no_write_skip_temp_wait_after_com_mapping_fix_20260430_1819\underlying_execution\run_20260430_181942\workflow_timing_trace.forensic_summary.json`
- Old 5GB trace was replaced with a `657 byte` placeholder after evidence capture.
- `original_sha256_not_computed_due_to_runaway_trace_size=true`

The runaway came from workflow timing events embedding full `route_state` snapshots repeatedly during A2 conditioning / preseal ticks. The same context shape is visible in the terminal gap event: `pressure_trace.jsonl` line `284` is `14,010,701 bytes`.

Largest fields in the terminal context:

- `pressure_samples`: `7,217,698 bytes`, `643` entries
- `vent_ticks`: `5,213,882 bytes`, `279` entries
- `diagnostic_deferred_events`: `618,138 bytes`, `211` entries
- `vent_scheduler_loop_gap_ms`: `1068` entries
- `vent_pulse_interval_ms`: `278` entries

Route state expansion starts at the accumulated A2 conditioning context, especially `pressure_samples` and `vent_ticks`, then gets repeatedly copied into workflow timing `route_state.route_state`. `diagnostic_deferred_events` is a secondary large contributor.

## Timing Relationship

- `workflow_timing_summary.event_count=1940`, matching the original trace scale.
- `preseal_soak=300.701 s`
- `artifact_finalize=1292.314 s`
- `longest_stage=artifact_finalize`
- `repeated_sleep_warnings` includes:
  - `co2_route_conditioning_vent_tick_gap_gt_max_gap`
  - `gap_s=21.423`
  - `point_index=1`

The huge workflow timing trace was produced during the same A2 conditioning / preseal window as the terminal vent-gap failure. The final `artifact_finalize_duration_s=1292.314` is after the run/safe-stop path and therefore did not itself cause the in-loop vent gap. However, the runaway trace also produced oversized workflow timing rows during the loop, so it is a credible contributor to I/O pressure and scheduler jitter before finalization.

The evidence supports this guarded conclusion:

- Trace runaway likely amplified timing jitter and artifact pressure.
- The direct fail-closed source recorded by A2 remains `pressure_monitor:selected_pressure_sample_stale`.
- The exact proportion of causality between trace write overhead and pressure-monitor defer accounting cannot be proven after the original 5GB trace was quarantined, but the shared stage, shared context, and massive line sizes make the trace guard necessary before any rerun.

## Trace Guard Fix Commit

- Commit: `2ac520ce5c4a45fb01dd6b6b48b6174651b5fe8b`
- Message: `A2.16 Guard workflow timing trace size`
- Main protections:
  - workflow timing event line cap: `64 KiB`
  - large `route_state` compaction
  - trace load cap: `128 MB`
  - line load skip threshold: `2 MB`
  - streaming JSONL load in the A2 wrapper
  - oversized trace warning evidence instead of full-file load

## Should The Next Round Rerun A2?

Recommended next real-device action: yes, but only in the next round, only after explicit human confirmation, and only once.

Reason:

- The A2.16 trace guard removes the most severe artifact/runaway risk seen in the failed run.
- A rerun is needed to learn whether the `2365.407 ms` vent gap disappears when workflow timing trace writes are bounded.
- This round must not rerun A2 because its scope is commit + offline audit only, and the user explicitly prohibited real COM / A2 route probe in this round.

## Remaining Work Before Treating A2 As Stable

Further audit/fix is still recommended:

- Keep `pressure_monitor:selected_pressure_sample_stale` defer accounting under review.
- Consider compacting large context fields in `route_trace.jsonl` and `pressure_trace.jsonl` as a separate artifact-size guard, because the terminal event itself was about `14 MB`.
- Confirm on the next single A2 engineering probe whether:
  - terminal vent gap remains below limit
  - `route_conditioning_diagnostic_blocked_vent_scheduler` clears
  - workflow timing trace remains bounded
  - no-write assertions remain true

## Explicit Non-Actions

- No real COM opened in this audit
- No A2 route probe run in this audit
- No route open
- No relay output
- No vent-off
- No seal
- No pressure setpoint
- No sample
- No A3
- No H2O
- No full group
- No multi-temperature
- No ID/SENCO/calibration coefficient write
- No temperature chamber SV write
- No temperature chamber start/stop
- No `real_primary_latest` refresh
- Not real acceptance evidence
