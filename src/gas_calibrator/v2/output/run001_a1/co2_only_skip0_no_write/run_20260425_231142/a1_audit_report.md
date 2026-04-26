# Run-001 A1 no-write dry-run audit report

- run_id: run_20260425_231142
- audit_scope: A1 audit only
- audit_time_local: 2026-04-26
- branch_at_audit_start: codex/run001-a1-no-write-dry-run
- head_at_audit_start: d0de6408dec5b9f7c685094f0fa6cdae6a6aba8d
- artifact_root: D:\gas_calibrator\src\gas_calibrator\v2\output\run001_a1\co2_only_skip0_no_write\run_20260425_231142

## Audit conclusion

A1 no-write real-machine dry-run evidence is internally consistent and sufficient for A1 green audit status.

This audit does not authorize A2, H2O, full group execution, real calibration writes, identity writes, device_id changes, V1 production-flow changes, default V2 cutover, or any statement that V2 can replace V1. V1 remains the production fallback.

## Green evidence

- summary final_decision: PASS
- summary a1_final_decision: PASS
- no_write_guard final_decision: PASS
- no_write_guard a1_final_decision: PASS
- a1_execution_result: completed
- points_completed: 4
- sample_count: 160
- route_completed: true
- pressure_completed: true
- wait_gate_completed: true
- sample_completed: true
- A1 green: true

## Artifact completeness

All required A1 audit artifacts were present.

| Artifact | Path | Audit status |
| --- | --- | --- |
| summary.json | D:\gas_calibrator\src\gas_calibrator\v2\output\run001_a1\co2_only_skip0_no_write\run_20260425_231142\summary.json | present, PASS |
| no_write_guard.json | D:\gas_calibrator\src\gas_calibrator\v2\output\run001_a1\co2_only_skip0_no_write\run_20260425_231142\no_write_guard.json | present, PASS |
| run_manifest.json | D:\gas_calibrator\src\gas_calibrator\v2\output\run001_a1\co2_only_skip0_no_write\run_20260425_231142\run_manifest.json | present, references key artifacts |
| human_readable_report.md | D:\gas_calibrator\src\gas_calibrator\v2\output\run001_a1\co2_only_skip0_no_write\run_20260425_231142\human_readable_report.md | present, consistent with summary |
| temperature_stability_evidence.json | D:\gas_calibrator\src\gas_calibrator\v2\output\run001_a1\co2_only_skip0_no_write\run_20260425_231142\temperature_stability_evidence.json | present, PASS |
| temperature_stability_samples.csv | D:\gas_calibrator\src\gas_calibrator\v2\output\run001_a1\co2_only_skip0_no_write\run_20260425_231142\temperature_stability_samples.csv | present, 33 rows |
| effective_analyzer_fleet.json | D:\gas_calibrator\src\gas_calibrator\v2\output\run001_a1\co2_only_skip0_no_write\run_20260425_231142\effective_analyzer_fleet.json | present, mapping_status=match |
| route_trace.jsonl | D:\gas_calibrator\src\gas_calibrator\v2\output\run001_a1\co2_only_skip0_no_write\run_20260425_231142\route_trace.jsonl | present, 60 records |
| points.csv | D:\gas_calibrator\src\gas_calibrator\v2\output\run001_a1\co2_only_skip0_no_write\run_20260425_231142\points.csv | present, 4 completed rows |
| io_log.csv | D:\gas_calibrator\src\gas_calibrator\v2\output\run001_a1\co2_only_skip0_no_write\run_20260425_231142\io_log.csv | present, no disallowed write markers found |
| run.log | D:\gas_calibrator\src\gas_calibrator\v2\output\run001_a1\co2_only_skip0_no_write\run_20260425_231142\run.log | present, graceful final safe stop recorded |

No missing artifact was found. The manifest references summary, no_write_guard, readiness, route pressure sample trace, effective analyzer fleet, temperature stability evidence, temperature samples, manifest, and human-readable report. No contradiction was found between summary, no_write_guard, manifest, report, temperature evidence, effective analyzer fleet, route trace, points, io log, and run log.

## No-write audit

- attempted_write_count: 0
- blocked_write_events: []
- identity_write_command_sent: false
- persistent_write_command_sent: false
- set_device_id_with_ack occurrences in io_log/run.log: 0
- set_device_id occurrences in io_log/run.log: 0
- ID,YGAS occurrences in io_log/run.log: 0
- calibration write markers in io_log/run.log: 0
- coefficient, zero, span write markers in io_log/run.log: 0

The analyzer setup records device-id keep events only. Existing identities were retained under the no-write guard. No identity write, calibration write, or persistent write evidence was found.

## Analyzer audit

Effective analyzer mapping is consistent with the intended A1 analyzer list.

| Logical analyzer | Port | Stable device_id | MODE2 ready | Mapping evidence |
| --- | --- | --- | --- | --- |
| ga01 | COM35 | 001 | true | detected truth list |
| ga02 | COM37 | 029 | true | detected truth list |
| ga03 | COM41 | 003 | true | detected truth list |
| ga04 | COM42 | 004 | true | detected truth list |

- mapping_source: analyzer_id_truth_audit_20260425_122251_after_mode2_preparation
- mapping_status: match
- intended_effective_match: true
- all_enabled_mode2_ready: true
- READDATA response status: not required because passive MODE2 active-send evidence was present
- no silent fallback to COM36 or COM38 was present in the effective A1 analyzer fleet

## Temperature stability audit

- gate code: D:\gas_calibrator\src\gas_calibrator\v2\core\services\temperature_control_service.py, _wait_analyzer_chamber_temp_stable()
- artifact/report integration: D:\gas_calibrator\src\gas_calibrator\v2\core\run001_a1_dry_run.py
- A1 config source: D:\gas_calibrator\src\gas_calibrator\v2\configs\validation\run001_a1_co2_only_skip0_no_write_real_machine_dry_run.json
- tolerance_c: 0.08
- rolling_window_s: 60.0
- timeout_s: 3600.0
- first_valid_timeout_s: 120.0
- sampling_interval_s: 1.0
- temperature_source: active_send_snapshot
- source_selection_policy: first_active_analyzer_with_chamber_temp_c
- selected source in successful evidence: ga01 / COM35 / device_id 001
- observed_min_c: 20.82
- observed_max_c: 20.84
- observed_span_c: 0.0200
- decision: PASS
- stale_frame_status: not_checked_no_frame_timestamp
- data_gap_status: ok
- route_opened while temperature evidence was collected: false

The observed rolling-window span, 0.0200C, is below the 0.08C A1 analyzer chamber temperature stability tolerance. This is not a bypass: the rolling window, timeout, evidence publication, and hard-fail path remain active. The 0.08C threshold applies only to analyzer chamber temperature short-window stability. It is not temperature chamber control precision, dewpoint stability, pressure stability, or final calibration uncertainty.

## Pressure atmosphere vent audit

The pressure atmosphere vent verification occurred before the first CO2 route valve opening.

Evidence sequence from route_trace/run.log:

1. Analyzer chamber temperature stable at 2026-04-25T23:43:08 local.
2. Route baseline applied before CO2 route conditioning at 2026-04-25T15:43:10Z.
3. Pressure controller vent set before CO2 route conditioning at 2026-04-25T15:43:21Z, with output_state=0, isolation_state=1, atmosphere_ready=true, hard_blockers=[].
4. CO2 route valves opened at 2026-04-25T15:43:27Z.

This satisfies the A1 safety requirement that CO2 route opening must be preceded by pressure controller atmosphere vent evidence.

## Flow completion audit

- analyzer setup: completed
- sensor precheck: completed for ga01, ga02, ga03, ga04
- chamber temperature stability: PASS
- pressure atmosphere vent before CO2 route: completed
- CO2 route opened: yes
- CO2 route completed: yes
- pressure control completed: yes
- wait gates completed: yes
- sample completed: yes
- points.csv completed rows: 4
- route_trace records: 60
- pressure setpoint records: completed
- sample_end records: 4, each with sample_count=40
- total summary sample_count: 160

No evidence was found that route/pressure/sample completion was counted without the corresponding trace and point artifacts. The route/pressure/wait/sample order is preserved in the trace.

## Runtime warnings and residual risks

The successful A1 run recorded repeated dewpoint and humidity-generator "serial not open" messages, plus a final safe-stop warning for humidity generator verification. These warnings were recorded, not hidden. They do not block this CO2-only A1 no-write audit, but they must remain explicit preconditions before any H2O, A2, or full group work.

No KeyboardInterrupt, Traceback, manual kill, or non-graceful exit was found in the successful A1 artifact set. Final route baseline, pressure safe stop, and route safe stop were recorded.

## Previous commit risk review

The previous commit changed only V2 A1-related files:

- D:\gas_calibrator\src\gas_calibrator\v2\configs\validation\run001_a1_co2_only_skip0_no_write_real_machine_dry_run.json
- D:\gas_calibrator\src\gas_calibrator\v2\core\run001_a1_dry_run.py
- D:\gas_calibrator\src\gas_calibrator\v2\core\run_state.py
- D:\gas_calibrator\src\gas_calibrator\v2\core\services\pressure_control_service.py
- D:\gas_calibrator\src\gas_calibrator\v2\core\services\temperature_control_service.py
- D:\gas_calibrator\src\gas_calibrator\v2\tests\test_a1_pressure_ready_gate.py
- D:\gas_calibrator\src\gas_calibrator\v2\tests\test_a1_temperature_stability_gate.py

No V1 production code or run_app.py change was found. The pressure_control_service change records sealed-route pressure-control state and allows output_state=1 only for continued sealed-route setpoint updates with existing seal/pressure evidence. The run_state change stores A1 pressure and temperature evidence state. No calibration write path was introduced. No logic was found that skips the analyzer chamber temperature stability gate.

The 0.08C tolerance is configured in the A1 no-write real-machine dry-run validation config. It was not propagated into V1 or an unreviewed global production default.

## V2 production design decision record

Recommended future V2 production criterion:

- analyzer_chamber_temperature_stability_tolerance_c: 0.08
- rolling_window_s: 60
- decision rule: rolling-window max(chamber_temperature_c) - min(chamber_temperature_c) <= 0.08C

Rationale and constraints:

1. This threshold is for gas analyzer chamber temperature short-term stability only.
2. It is not temperature chamber control precision.
3. It is not dewpoint stability.
4. It is not pressure stability.
5. It is not final calibration uncertainty.
6. It is based on the A1 real-machine no-write evidence where observed_span_c=0.0200C.
7. The previous 0.03C criterion was too strict for observed real analyzer chamber temperature noise and could cause unnecessary waits.
8. The 0.08C criterion still preserves the temperature stability gate and evidence requirements.
9. A2/A4/A5/A6 must continue accumulating evidence across more pressure points, temperature groups, and analyzers.
10. Before V2 replaces V1, A6 V1/V2 comparison acceptance must confirm this criterion has no negative calibration impact.
11. This audit does not modify V1 and does not switch the default production flow to V2.

## A2 status and preconditions

A2 remains prohibited after this audit.

Before A2 can be considered, at minimum:

- A1 audit result must be accepted explicitly.
- A2 scope, no-write boundary, and abort criteria must be written and approved.
- No real calibration writes, identity writes, or device_id changes may be introduced.
- Analyzer mapping must remain explicit, with no silent fallback.
- Valve and PACE safety preflight must be repeated before any real-machine step.
- Dewpoint and humidity-generator serial warnings must be triaged before any H2O or full group work.
- A2 must not be inferred from A1 green alone.

## Final audit status

- A1 audit conclusion: PASS for A1 green evidence
- Allowed next state: A1 audit passed / awaiting explicit next-stage authorization
- Allowed to enter A2: no
- Allowed to enter H2O: no
- Allowed to enter full group: no
- Allowed to perform real calibration writes: no
- Allowed to modify V1 production flow: no
- V1 fallback: remains required
