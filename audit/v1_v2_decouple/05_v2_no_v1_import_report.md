# V2 Runtime No-V1 Import Report

- base_head: `367a1089ebaca1388dbb9d11648f74513316e502`
- guard_test: `tests/test_v2_has_no_v1_runtime_imports.py`
- result: `PASS`

## Runtime Roots Guarded

- `src/gas_calibrator/v2/entry.py`
- `src/gas_calibrator/v2/config/**`
- `src/gas_calibrator/v2/core/**`
- `src/gas_calibrator/v2/ui_v2/**`
- `src/gas_calibrator/v2/storage/**`
- `src/gas_calibrator/v2/analytics/**`
- `src/gas_calibrator/v2/qc/**`
- `src/gas_calibrator/v2/domain/**`

## Bridge / Dev Paths Excluded On Purpose

- `src/gas_calibrator/v2/adapters/legacy_runner.py`
- `src/gas_calibrator/v2/adapters/v1_route_trace.py`
- `src/gas_calibrator/v2/scripts/run_v1_route_trace.py`
- `src/gas_calibrator/v2/sim/parity.py`

These modules are explicitly compatibility, trace, or parity tooling. They cross the boundary on purpose and should stay labeled as bridge/dev-only paths rather than being folded into the V2 main runtime.

## Evidence

- Guard test pass: `tests/test_v2_has_no_v1_runtime_imports.py`
- Manual scan:
  - `src/gas_calibrator/v2/adapters/legacy_runner.py:37`
  - `src/gas_calibrator/v2/adapters/v1_route_trace.py:10-11`
  - `src/gas_calibrator/v2/scripts/run_v1_route_trace.py:31-43`
  - `src/gas_calibrator/v2/sim/parity.py:9`

## Recommendation

- Keep V2 main runtime import-free from V1 runtime modules.
- Leave bridge modules in place, but keep them clearly isolated from V2 default runtime entrypoints.
