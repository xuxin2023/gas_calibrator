# V1.5 PR #31 Formal Queue Backfill Review - 2026-07-03

This note reviews the V1.5 formal CO2/H2O queue and shared sampling files in
PR #31. It is review evidence only. It does not authorize live queue execution,
real COM access, PostgreSQL import, coefficient writes, or production release.

## Scope

Reviewed files:

```text
src/gas_calibrator/tools/run_v1_5_formal_co2_open_flow_queue.py
src/gas_calibrator/tools/run_v1_5_formal_h2o_open_flow_queue.py
src/gas_calibrator/tools/run_v1_5_formal_open_flow_sampling.py
```

These files are new relative to `origin/main`:

```text
A src/gas_calibrator/tools/run_v1_5_formal_co2_open_flow_queue.py
A src/gas_calibrator/tools/run_v1_5_formal_h2o_open_flow_queue.py
A src/gas_calibrator/tools/run_v1_5_formal_open_flow_sampling.py
```

Their approximate added line counts relative to `origin/main` are:

```text
1098 src/gas_calibrator/tools/run_v1_5_formal_co2_open_flow_queue.py
1097 src/gas_calibrator/tools/run_v1_5_formal_h2o_open_flow_queue.py
1440 src/gas_calibrator/tools/run_v1_5_formal_open_flow_sampling.py
```

## Mature V1.5 Source Check

The three reviewed files are blob-identical to the original V1.5 mature branch
`codex/v1.5-original-flow-key-trace`.

```text
run_v1_5_formal_co2_open_flow_queue.py
HEAD=605290d3a8786bac06cec5e7e27c29d0be191f31
original_v1_5=605290d3a8786bac06cec5e7e27c29d0be191f31
identical=True

run_v1_5_formal_h2o_open_flow_queue.py
HEAD=c1173cb202b39b5268420d802679ad0774b6bed1
original_v1_5=c1173cb202b39b5268420d802679ad0774b6bed1
identical=True

run_v1_5_formal_open_flow_sampling.py
HEAD=aea4745f5262667e28526445f206e9f4df63ee6d
original_v1_5=aea4745f5262667e28526445f206e9f4df63ee6d
identical=True
```

Interpretation:

- PR #31 is backfilling the mature V1.5 formal queue files into a clean
  `main`-based branch.
- PR #31 is not rewriting these queue/sampling files during the clean-main
  review branch.
- Human review should still inspect these files because they are large and
  become new files relative to `main`.

## Live-Adjacent Term Scan

The reviewed queue/sampling files were scanned for these terms:

```text
formal_route_readiness
route_readiness
CHECK
check_monitor
15-field
live_queue_execution_allowed
database_import_allowed
connects_postgresql
```

Result:

```text
no_forbidden_live_adjacent_terms_found=true
```

Interpretation:

- The reviewed files do not contain the CHECK / new-protocol package.
- The reviewed files do not contain PostgreSQL import gates.
- The reviewed files do not contain the dry-run/live queue authorization status
  gates used by the separate profile handoff contracts.

## Protected Core Files

This review does not change these high-risk core files:

```text
src/gas_calibrator/workflow/runner.py
src/gas_calibrator/devices/gas_analyzer.py
configs/default_config.json
run_app.py
```

## Artifact Row Compatibility Finding

During the queue/sampling focused test run, one artifact-sidecar test initially
failed outside the queue files:

```text
tests/test_v1_5_formal_open_flow_artifacts.py::test_normalize_sample_row_maps_translated_headers_and_analyzer_prefix
```

Root cause:

- `normalize_sample_row()` mapped translated analyzer pressure headers such as
  `气体分析仪1_分析仪压力kPa`, but did not map the related MODE2 translated suffix
  `MODE2数据合同状态`.
- The row was normalized to `ga01_MODE2数据合同状态` instead of
  `ga01_mode2_contract_status`.

Resolution:

- Added a small translated-header alias map in
  `src/gas_calibrator/validation/artifact_rows.py`.
- This affects CSV artifact readback normalization only.
- It does not change queue control, route control, serial communication,
  PostgreSQL import, or coefficient writing.

## Verification

Single failing test after the artifact-row fix:

```text
python -m pytest tests/test_v1_5_formal_open_flow_artifacts.py::test_normalize_sample_row_maps_translated_headers_and_analyzer_prefix -q
```

Result:

```text
1 passed in 3.55s
```

Queue/sampling focused review suite:

```text
python -m pytest tests/test_v1_5_formal_co2_open_flow_queue.py tests/test_v1_5_formal_h2o_open_flow_queue.py tests/test_v1_5_formal_open_flow_sampling_runner.py tests/test_v1_5_formal_h2o_open_flow_sampling_runner.py tests/test_v1_5_formal_open_flow.py tests/test_v1_5_formal_open_flow_artifacts.py -q
```

Result:

```text
118 passed in 10.79s
```

## Review Conclusion

The formal CO2 queue, formal H2O queue, and shared formal sampling files in
PR #31 are consistent with the original mature V1.5 branch at the blob level.
They are large new files relative to `main`, so human review is still required,
but the clean-main PR did not mutate them away from the mature V1.5 source.

The only fix made during this review is an artifact normalization compatibility
fix for translated MODE2 CSV headers. It is scoped to sidecar/evidence readback
and does not affect live execution behavior.
