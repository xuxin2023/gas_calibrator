# V1 / V2 Split Plan

- base_head: `367a1089ebaca1388dbb9d11648f74513316e502`
- current assessment: current codebase is mostly repository co-existence plus bridge tooling, not a hard runtime knot between the V1 CO2 production path and the V2 runtime.

## Stage 1 Findings

- No direct `src/gas_calibrator/v2/**` import was found in the key V1 runtime files:
  `config.py`, `workflow/runner.py`, `devices/gas_analyzer.py`, `logging_utils.py`,
  `tools/run_headless.py`, `tools/run_v1_corrected_autodelivery.py`,
  `tools/run_v1_online_acceptance.py`.
- Direct V1/V2 crossing points are concentrated in:
  - V1-named offline tools that already behave like engineering sidecars:
    `src/gas_calibrator/tools/run_v1_merged_calibration_sidecar.py:26-34`,
    `src/gas_calibrator/tools/run_v1_no500_postprocess.py:19-20`
  - V2 compatibility/trace/parity tooling:
    `src/gas_calibrator/v2/adapters/legacy_runner.py:37`,
    `src/gas_calibrator/v2/adapters/v1_route_trace.py:10-11`,
    `src/gas_calibrator/v2/scripts/run_v1_route_trace.py:31-43`,
    `src/gas_calibrator/v2/sim/parity.py:9`
- One non-decoupling regression was found while running the required V1 focused tests:
  `src/gas_calibrator/config.py:220-223` had drifted back to `enabled=True` and `write_devices=True`.
  This was restored to the V1-safe default because it breaks a declared V1 invariant.

## What Must Stay In V1

- `workflow.postrun_corrected_delivery` runtime safety defaults and source logging:
  `src/gas_calibrator/config.py:220-241`,
  `src/gas_calibrator/workflow/runner.py:4573-4609`
- H2O zero/span NOT_SUPPORTED boundary:
  `src/gas_calibrator/config.py:264-324`,
  `src/gas_calibrator/workflow/runner.py:4625-4713`,
  `src/gas_calibrator/tools/run_headless.py:235-241`,
  `src/gas_calibrator/tools/run_v1_online_acceptance.py:157-171`
- CO2 writeback safety chain used by the V1 runner:
  `src/gas_calibrator/workflow/runner.py:16562`

## What Should Be Shared

- `write_senco_groups_with_full_verification` currently lives in
  `src/gas_calibrator/tools/run_v1_corrected_autodelivery.py:616`.
  It is already used by the V1 runner and the online acceptance tool, so the long-term clean home is a neutral shared/V1-common module rather than a CLI file.
- V1 artifact/log schema used by V2 parity/trace helpers should be treated as shared contract rather than a hidden dependency on `RunLogger` implementation details.

## What Must Stay In V2

- `legacy_runner`, `v1_route_trace`, `run_v1_route_trace`, and `v1_postprocess_runner` are V2-owned compatibility tooling.
- `run_v1_merged_calibration_sidecar` and `run_v1_no500_postprocess` are better treated as V2-owned engineering/offline utilities even though their names mention V1.
- `app_facade`, `review_center`, `scan_contracts`, and other UI/reviewer surfaces remain V2-only.

## Runtime Coupling vs UI / Audit Coupling

- Runtime coupling:
  - not found in the key V1 runtime path scanned this round
  - not found in the V2 main runtime roots guarded in this round (`v2/config`, `v2/core`, `v2/ui_v2`, `v2/storage`, `v2/analytics`, `v2/qc`, `v2/domain`, `v2/entry.py`)
- UI / audit / acceptance coupling:
  - found in V2 review surfaces consuming V1-produced artifacts
  - found in V2 route trace/parity helpers reading V1 logger outputs
  - found in V1 engineering sidecars using V2 postprocess/export helpers

## Ownership Of `run_structure_hints.enabled`

- owner: `V1`
- evidence:
  - config definition: `src/gas_calibrator/config.py:229-231`
  - only runtime consumer found: `src/gas_calibrator/tools/run_v1_corrected_autodelivery.py:1239-1521`
- why:
  - it controls an engineering recommendation artifact emitted from the V1 corrected-autodelivery flow
  - no V2 runtime consumer was found

## Execution Recommendation

- Current repository state does **not** justify a large runtime split.
- Minimum action is:
  1. keep V1 runtime free of `v2` imports with guard tests
  2. keep V2 main runtime free of V1 runtime imports with guard tests
  3. keep bridge/offline tools explicitly labeled as V2-owned or SHARED
  4. restore any drift that breaks V1 invariants while doing the decouple pass
