# Bridge Allowlist

- BASE_HEAD: `367a1089ebaca1388dbb9d11648f74513316e502`
- guard_test: [tests/test_v1_v2_bridge_allowlist.py](D:/gas_calibrator/tests/test_v1_v2_bridge_allowlist.py:115)

## Allowed Cross-Boundary Files

- [src/gas_calibrator/tools/run_v1_merged_calibration_sidecar.py](D:/gas_calibrator/src/gas_calibrator/tools/run_v1_merged_calibration_sidecar.py:26)
  Allowed because it is an offline engineering sidecar that merges V1 run artifacts with V2 config/export/download utilities.
- [src/gas_calibrator/tools/run_v1_no500_postprocess.py](D:/gas_calibrator/src/gas_calibrator/tools/run_v1_no500_postprocess.py:22)
  Allowed because it is an offline postprocess wrapper around V2 postprocess/export code.
- [src/gas_calibrator/v2/adapters/legacy_runner.py](D:/gas_calibrator/src/gas_calibrator/v2/adapters/legacy_runner.py:40)
  Allowed because it is an explicit V2 compatibility adapter that can invoke the V1 runner.
- [src/gas_calibrator/v2/adapters/v1_route_trace.py](D:/gas_calibrator/src/gas_calibrator/v2/adapters/v1_route_trace.py:16)
  Allowed because it is a bridge wrapper for external V1 route tracing.
- [src/gas_calibrator/v2/scripts/run_v1_route_trace.py](D:/gas_calibrator/src/gas_calibrator/v2/scripts/run_v1_route_trace.py:31)
  Allowed because it is a V2 script that intentionally reuses V1 device bootstrap helpers for tracing.
- [src/gas_calibrator/v2/sim/parity.py](D:/gas_calibrator/src/gas_calibrator/v2/sim/parity.py:9)
  Allowed because it is a dev-only parity surface that reads V1 artifact/log contracts.
- [src/gas_calibrator/tools/run_v1_corrected_autodelivery.py](D:/gas_calibrator/src/gas_calibrator/tools/run_v1_corrected_autodelivery.py:33)
  Allowed to cross only into the shared helper [src/gas_calibrator/tools/_no500_filter.py](D:/gas_calibrator/src/gas_calibrator/tools/_no500_filter.py:55); it is not allowed to import bridge/sidecar files or `v2` modules directly.

## Files That Must Not Be Added To This Allowlist

- [src/gas_calibrator/workflow/runner.py](D:/gas_calibrator/src/gas_calibrator/workflow/runner.py:4574)
- [src/gas_calibrator/tools/run_headless.py](D:/gas_calibrator/src/gas_calibrator/tools/run_headless.py:1)
- [src/gas_calibrator/tools/run_v1_online_acceptance.py](D:/gas_calibrator/src/gas_calibrator/tools/run_v1_online_acceptance.py:1)
- `src/gas_calibrator/v2/entry.py`
- `src/gas_calibrator/v2/config/**`
- `src/gas_calibrator/v2/core/**`
- `src/gas_calibrator/v2/ui_v2/**`

These are runtime boundaries, not bridge surfaces. New cross-boundary imports here should fail tests rather than be absorbed into the allowlist.
