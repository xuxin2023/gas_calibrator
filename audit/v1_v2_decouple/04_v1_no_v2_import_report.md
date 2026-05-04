# V1 Runtime No-V2 Import Report

- base_head: `367a1089ebaca1388dbb9d11648f74513316e502`
- guard_test: `tests/test_v1_has_no_v2_runtime_imports.py`
- result: `PASS`

## Files Guarded

- `src/gas_calibrator/config.py`
- `src/gas_calibrator/workflow/runner.py`
- `src/gas_calibrator/devices/gas_analyzer.py`
- `src/gas_calibrator/logging_utils.py`
- `src/gas_calibrator/tools/run_headless.py`
- `src/gas_calibrator/tools/run_v1_corrected_autodelivery.py`
- `src/gas_calibrator/tools/run_v1_online_acceptance.py`

## What Was Excluded On Purpose

- `src/gas_calibrator/tools/run_v1_merged_calibration_sidecar.py`
- `src/gas_calibrator/tools/run_v1_no500_postprocess.py`

These two files are engineering/offline sidecars, not the V1 production runtime path. They still depend on V2 utilities and should be treated as boundary tooling rather than as proof of a main-runtime coupling.

## Evidence

- Guard test pass: `tests/test_v1_has_no_v2_runtime_imports.py`
- Manual scan:
  - `src/gas_calibrator/tools/run_headless.py:28-29`
  - `src/gas_calibrator/workflow/runner.py:16562`
  - `src/gas_calibrator/config.py:264-324`

## Recommendation

- Keep the guard test in the regular V1 focused set.
- Do not move the V1 runtime toward `src/gas_calibrator/v2/**`.
