# A2.12R-I Interrupted Execution Missing Artifacts Summary

Date: 2026-04-30

## Scope

- Probe: A2.12R v1_aligned CO2-only seven-pressure no-write engineering probe.
- Evidence directory: `D:\gas_calibrator_step3a_a2_12r_v1_aligned_co2_7_pressure_no_write_skip_temp_wait_20260430_1057`
- Current branch at triage: `codex/run001-a1-no-write-dry-run`
- Current HEAD at triage: `cb9a29def2550189a81a3ae497a332eca954e3a8`
- Origin branch HEAD at triage: `8ffb4bbfe314d35863cc1e9696aa45b2bae86ddf`

## Generated Files Found

- `operator_confirmation_input.json`
- `a2_3_v1_aligned_downstream_config.json`

## Required Files Missing

- `summary.json`
- `safety_assertions.json`
- `operator_confirmation_record.json`
- `route_trace.jsonl`
- `pressure_trace.jsonl`
- `heartbeat_trace.jsonl`
- `point_results.json`
- `point_results.csv`
- `samples`
- `analyzer_sampling_rows`
- `generated_points_json_path`
- `generated_points_json_sha256`

## Command Interruption Context

- The previous turn issued exactly one A2.12R `v1_aligned --execute-probe` command.
- The command was interrupted before required terminal artifacts were written.
- No app terminal session is attached to the current thread, so the previous shell exit source is not available from terminal history in this turn.
- The output directory timestamps show `operator_confirmation_input.json` at 2026-04-30 10:57:17 and `a2_3_v1_aligned_downstream_config.json` at 2026-04-30 10:57:30.
- No `summary.json`, safety assertion file, trace file, point result, or wrapper operator confirmation record exists in the output directory.

## Process And Downstream Session Check

- Residual Python process check: no `python.exe`, `python3.exe`, or `py.exe` process was listed by `Get-CimInstance Win32_Process`.
- Downstream session check: no `run001_a2/co2_only_7_pressure_no_write` downstream run session newer than 2026-04-30 10:56 was found under the known V2 output directory.

## Fail-Closed Interpretation

- This is not A2.12R PASS.
- This is not real acceptance evidence.
- Required artifacts are incomplete, so the only valid interpretation is FAIL_CLOSED.
- The fail-closed reason is `probe_execution_interrupted_required_artifacts_incomplete`.
- Because `safety_assertions.json` is missing, no-write must not be claimed as a complete pass. The no-write status is only decidable if the interruption stage and command audit prove it; otherwise it must be `unknown`.

## Code Triage Findings

- The A2 wrapper created the output directory and wrote the downstream aligned config before invoking the downstream executor.
- In the pre-fix code, wrapper-level `summary.json`, `safety_assertions.json`, `operator_confirmation_record.json`, trace files, and point result files were written only after the downstream executor returned.
- `KeyboardInterrupt` is not a subclass of `Exception`, and an outer process timeout/termination can also stop execution before the normal final writer runs.
- Therefore a command interrupted after admission/downstream config but before executor return could leave exactly the observed two files and no wrapper audit record.
- No evidence in the interrupted directory can prove a complete no-write pass, and no downstream run session after 10:56 was found; the real COM/open-command state for the prior interrupted command is not reconstructable from required artifacts.
- A2.12R-I fixes this by writing guard artifacts immediately after admission/output directory creation, and by refreshing them to an executor-started/unknown-COM state before invoking the downstream executor.

## A2.12R-I Offline Verification

- `python -m pytest tests/test_a2_co2_only_7_pressure_no_write_probe.py -q`: 36 passed.
- `python -m pytest tests/test_a2_no_write_pressure_sweep.py -q`: 112 passed.
- `python -m pytest tests/test_a1r_minimal_no_write_sampling_probe.py -q`: 4 passed.
- `python -m pytest tests/test_r1_conditioning_only_probe.py -q`: 7 passed.
- `python -m pytest tests/test_query_only_real_com_probe.py -q`: 16 passed.
- `python -m pytest tests/test_r0_1_reference_read_probe.py -q`: 9 passed.
- `python -m pytest tests/test_paroscientific_driver.py -q`: 3 passed.
- `python -m pytest tests -q`: 231 passed.
- `python -m gas_calibrator.v2.scripts.run_simulation_suite --suite parity ...`: 1/1 passed.
- `python -m gas_calibrator.v2.scripts.run_simulation_suite --suite smoke ...`: 5/5 passed.
- `git diff --check`: passed with line-ending warnings only.

## Boundary

- This handoff file records the interrupted prior run only.
- It does not refresh `real_primary_latest`.
- It does not promote V2 over V1.
- It does not authorize A3.
- It does not authorize any real COM rerun.
