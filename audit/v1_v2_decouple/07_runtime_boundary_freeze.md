# Runtime Boundary Freeze

- BASE_HEAD: `367a1089ebaca1388dbb9d11648f74513316e502`
- branch: `codex/v1-v2-decouple-367a108`
- freeze_goal: make the V1 / V2 runtime boundary auditable, testable, and resistant to regression without large directory moves.

## V1_RUNTIME

- [src/gas_calibrator/config.py](D:/gas_calibrator/src/gas_calibrator/config.py:229)
  Owns `postrun_corrected_delivery` defaults, `h2o_zero_span` capability, and the fail-fast helper at [config.py](D:/gas_calibrator/src/gas_calibrator/config.py:325).
- [src/gas_calibrator/workflow/runner.py](D:/gas_calibrator/src/gas_calibrator/workflow/runner.py:4574)
  Owns the V1 runtime safety boundary and the main writeback entry that reuses the shared helper at [runner.py](D:/gas_calibrator/src/gas_calibrator/workflow/runner.py:16976).
- [src/gas_calibrator/devices/gas_analyzer.py](D:/gas_calibrator/src/gas_calibrator/devices/gas_analyzer.py)
  V1 device protocol layer.
- [src/gas_calibrator/logging_utils.py](D:/gas_calibrator/src/gas_calibrator/logging_utils.py)
  V1 run logging and traceability schema.
- [src/gas_calibrator/tools/run_headless.py](D:/gas_calibrator/src/gas_calibrator/tools/run_headless.py:1)
  Default V1 headless runtime entry.
- [src/gas_calibrator/tools/run_v1_corrected_autodelivery.py](D:/gas_calibrator/src/gas_calibrator/tools/run_v1_corrected_autodelivery.py:1)
  V1 corrected-delivery / writeback helper entrypoint; now depends only on the V1-safe helper at [run_v1_corrected_autodelivery.py](D:/gas_calibrator/src/gas_calibrator/tools/run_v1_corrected_autodelivery.py:33).
- [src/gas_calibrator/tools/run_v1_online_acceptance.py](D:/gas_calibrator/src/gas_calibrator/tools/run_v1_online_acceptance.py:1)
  Guarded V1 online acceptance tool with dual-gate protection.

## V2_RUNTIME

- [src/gas_calibrator/v2/entry.py](D:/gas_calibrator/src/gas_calibrator/v2/entry.py)
- `src/gas_calibrator/v2/config/**`
- `src/gas_calibrator/v2/core/**`
- `src/gas_calibrator/v2/ui_v2/**`
- `src/gas_calibrator/v2/storage/**`
- `src/gas_calibrator/v2/analytics/**`
- `src/gas_calibrator/v2/qc/**`
- `src/gas_calibrator/v2/domain/**`

These roots are guarded by [tests/test_v2_has_no_v1_runtime_imports.py](D:/gas_calibrator/tests/test_v2_has_no_v1_runtime_imports.py:54).

## BRIDGE_OR_SIDECAR

- [src/gas_calibrator/tools/run_v1_merged_calibration_sidecar.py](D:/gas_calibrator/src/gas_calibrator/tools/run_v1_merged_calibration_sidecar.py:1)
  Offline engineering sidecar; outside V1 default workflow and allowed to reuse V2 config/export helpers at [run_v1_merged_calibration_sidecar.py](D:/gas_calibrator/src/gas_calibrator/tools/run_v1_merged_calibration_sidecar.py:26).
- [src/gas_calibrator/tools/run_v1_no500_postprocess.py](D:/gas_calibrator/src/gas_calibrator/tools/run_v1_no500_postprocess.py:1)
  Offline bridge/sidecar for no-500 postprocess; still uses V2 postprocess/export utilities at [run_v1_no500_postprocess.py](D:/gas_calibrator/src/gas_calibrator/tools/run_v1_no500_postprocess.py:22).
- [src/gas_calibrator/v2/adapters/legacy_runner.py](D:/gas_calibrator/src/gas_calibrator/v2/adapters/legacy_runner.py:28)
  V2 compatibility adapter that intentionally calls the V1 runner at [legacy_runner.py](D:/gas_calibrator/src/gas_calibrator/v2/adapters/legacy_runner.py:40).
- [src/gas_calibrator/v2/adapters/v1_route_trace.py](D:/gas_calibrator/src/gas_calibrator/v2/adapters/v1_route_trace.py:1)
  V2 bridge wrapper around V1 runner/logging for route tracing.
- [src/gas_calibrator/v2/scripts/run_v1_route_trace.py](D:/gas_calibrator/src/gas_calibrator/v2/scripts/run_v1_route_trace.py:31)
  V2 script that intentionally reuses V1 device bootstrap helpers.
- [src/gas_calibrator/v2/sim/parity.py](D:/gas_calibrator/src/gas_calibrator/v2/sim/parity.py:9)
  Dev-only parity tool that reads V1 artifact/log contracts.

## SHARED_CANDIDATE

- [src/gas_calibrator/tools/_no500_filter.py](D:/gas_calibrator/src/gas_calibrator/tools/_no500_filter.py:1)
  Pure dataframe helper used by both V1 corrected delivery and the offline no-500 sidecar.
- `write_senco_groups_with_full_verification` at [run_v1_corrected_autodelivery.py](D:/gas_calibrator/src/gas_calibrator/tools/run_v1_corrected_autodelivery.py:781)
  Still lives in a V1 tool file, but is reused by the runner and online acceptance as a shared writeback primitive.

## Real Runtime Coupling Found?

- Yes, one indirect coupling was found and frozen in this round:
  [src/gas_calibrator/tools/run_v1_corrected_autodelivery.py](D:/gas_calibrator/src/gas_calibrator/tools/run_v1_corrected_autodelivery.py:33) previously imported a helper from `run_v1_no500_postprocess.py`, and that bridge file imported `v2` modules at [run_v1_no500_postprocess.py](D:/gas_calibrator/src/gas_calibrator/tools/run_v1_no500_postprocess.py:22).
- Fix:
  the pure filter logic now lives in [src/gas_calibrator/tools/_no500_filter.py](D:/gas_calibrator/src/gas_calibrator/tools/_no500_filter.py:55), which keeps the V1 runtime path v2-free.

## Need Large Split?

- No.
- Current repository state does not justify a large V1/V2 folder move or a broad shared-package refactor.
- Boundary freeze is now enforced primarily by tests and explicit bridge ownership.
