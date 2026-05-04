# A2.16R FAIL_CLOSED: Prearm Baseline / Cleanup Vent Audit Seed

## Scope

- Audit type: offline evidence capture only.
- A2 output directory:
  `D:\gas_calibrator_step3a_a2_16r_v1_aligned_co2_7_pressure_no_write_skip_temp_wait_after_trace_guard_20260430_225404`
- Underlying execution directory:
  `D:\gas_calibrator_step3a_a2_16r_v1_aligned_co2_7_pressure_no_write_skip_temp_wait_after_trace_guard_20260430_225404\underlying_execution\run_20260430_225445`
- Real COM in this audit: false.
- A2 rerun in this audit: false.
- Not real acceptance evidence: true.

## Final Decision

- `final_decision=FAIL_CLOSED`
- `fail_closed_reason=a2_route_conditioning_vent_blocked_after_flush_phase`
- Wrapper exit code: `2`
- `process_exit_record.process_state=completed`
- `artifact_completeness_pass=true`
- `evidence_source=real_probe_a2_12r_co2_7_pressure_no_write`
- `no_write=true`
- `critical_devices_failed=[]`
- `optional_context_devices_failed=[]`
- Ports:
  - pressure controller: `COM23`
  - pressure meter/P3: `COM22`
  - relay: `COM20`
  - relay_8: `COM21`

## Trace Guard Evidence

- Top-level `route_trace.jsonl`: `452,695 bytes`, 330 rows, max line about `1.4 KiB`.
- Top-level `pressure_trace.jsonl`: `456,442 bytes`, 314 rows, max line about `1.5 KiB`.
- Underlying `workflow_timing_trace.jsonl`: `13,732,793 bytes`, 4001 rows, max line `8,405 bytes`.
- `trace_event_truncated_count=0`
- `trace_large_line_warning_count=0`
- `trace_file_size_guard_triggered=false`
- `trace_streaming_read_used=true`
- `trace_inline_load_blocked=false`
- `artifact_finalize_duration_s=7.032`

Conclusion: A2.16/A2.16R trace guard worked. This failure is not a trace runaway recurrence.

## Pressure / Route Conditioning Evidence

- `measured_atmospheric_pressure_hpa=1007.817`
- `route_open_first_sample_hpa=1007.819`
- `route_open_pressure_delta_hpa=0.002`
- `route_conditioning_pressure_overlimit=false`
- `route_conditioning_hard_abort_exceeded=false`
- `route_conditioning_vent_gap_exceeded=false`
- `route_conditioning_diagnostic_blocked_vent_scheduler=false`
- `pressure_points_completed=0`
- `points_completed=0`
- `sample_count_total=0`

The route-open sample was essentially atmospheric, but the high-pressure first-point path did not accept a usable prearm baseline before moving toward the first 1100 hPa point.

## Prearm Baseline / Source Disagreement Context

Underlying run log reported:

- high-pressure first-point requires a fresh baseline pressure sample before route open.
- Trigger reason: `baseline_pressure_sample_stale`.
- Baseline sample:
  - source: `digital_pressure_gauge_continuous`
  - pressure: `1014.508 hPa`
  - sample age: `0.836 s`
  - stale threshold: `0.5 s`
  - stale: true
- Source selection reason: `digital_latest_stale_pace_aux_disagreement`.
- Digital gauge sample: `1014.508 hPa`.
- PACE auxiliary sample: `3.3364493 hPa`.
- Disagreement: about `1011.172 hPa`.

Critical freshness evidence:

- `source_selection_decision=digital_latest_stale_pace_aux_disagreement`
- `stale_frame_count=2`
- `blocking_query_count_in_critical_window=0`
- `critical_window_blocking_query_total_s=0`
- `decision=FAIL`

High-pressure evidence currently does not expose enough prearm fields:

- `enabled=false`
- `trigger_reason=disabled`
- `baseline_pressure_sample=null`
- `baseline_pressure_source=null`
- `baseline_pressure_age_s=null`
- `decision=FAIL`

Audit target: A2.17 must clarify whether the latest route-conditioning pressure or route-open first sample can be eligible as the prearm baseline under `v1_aligned`, and must record exact age/source/alignment reasons when it cannot.

## Cleanup Vent Block Context

Top-level summary classified the cleanup/final safe stop vent as:

- `cleanup_vent_classification=normal_maintenance_vent`
- `normal_maintenance_vent_blocked_after_flush_phase=true`
- `vent_pulse_blocked_reason=route_conditioning_phase_not_flush`

Route trace examples:

- line 324: `action=set_vent`, `result=blocked`, `message=route_conditioning_phase_not_flush`, reason `after CO2 route fail-closed`
- line 328: `action=set_vent`, `result=blocked`, `message=route_conditioning_phase_not_flush`, reason `final safe stop`

Audit target: A2.17 must distinguish ordinary route-conditioning maintenance vent from cleanup/safe-stop relief. Ordinary maintenance vent remains flush-phase only; cleanup/safe-stop relief must have its own classification and explicit allow/block reason, especially when no seal, no pressure setpoint, and no sample occurred.

## Vent Gap Context

- `max_vent_pulse_write_gap_ms_including_terminal_gap=1107.851`
- `max_vent_pulse_gap_limit_ms=2000.0`
- `route_conditioning_vent_gap_exceeded=false`
- `route_conditioning_diagnostic_blocked_vent_scheduler=false`
- `max_vent_scheduler_loop_gap_ms=203.686`
- Workflow timing summary recorded `co2_route_conditioning_vent_tick_max_gap_s=21.405`, but the A2 route-conditioning write gap gate used the bounded write-gap metric above and did not exceed the 2000 ms threshold.

Audit target: A2.17 must record the max vent write gap phase, threshold, threshold source, exceeded boolean, and a plain reason for why `1107.851 ms` did not trigger `route_conditioning_vent_gap_exceeded`.

## Explicit Non-Actions

- No real COM opened in this audit.
- No A2 route probe rerun in this audit.
- No route open.
- No relay output.
- No vent-off.
- No seal.
- No pressure setpoint.
- No sample.
- No A3.
- No H2O.
- No full group.
- No multi-temperature.
- No ID/SENCO/calibration coefficient write.
- No temperature chamber SV write.
- No temperature chamber start/stop.
- No `real_primary_latest` refresh.
- Not real acceptance evidence.
