# Breaking Surface Check

- base_head: `367a1089ebaca1388dbb9d11648f74513316e502`
- scope: CLI, config keys, report/audit artifacts, tests, UI adapters

| surface | kind | current_owner | risk_if_decoupled_badly | evidence | note |
| --- | --- | --- | --- | --- | --- |
| `src/gas_calibrator/tools/run_headless.py` | CLI / runtime | V1 | High | `src/gas_calibrator/tools/run_headless.py:28-29,235-241` | Default V1 runtime entry; must remain CO2-safe and must not pick up V2 runtime imports. |
| `src/gas_calibrator/workflow/runner.py` | runtime | V1 | High | `src/gas_calibrator/workflow/runner.py:4573-4609,4625-4713,16562` | Holds postrun safety defaults, H2O fail-fast, and the shared writeback helper call. |
| `workflow.postrun_corrected_delivery.*` | config keys | V1 | High | `src/gas_calibrator/config.py:220-241` | Behavior changes here can flip default device-write safety. |
| `coefficients.h2o_zero_span.*` | config keys | V1 | High | `src/gas_calibrator/config.py:264-324` | Must stay `NOT_SUPPORTED` for V1 unless a new requirement is explicitly accepted. |
| `run_structure_hints.enabled` | config / artifact | V1 | Low | `src/gas_calibrator/config.py:229-231`, `src/gas_calibrator/tools/run_v1_corrected_autodelivery.py:1239-1521` | Report-only hint generation; no evidence of main runtime ownership by V2. |
| `src/gas_calibrator/tools/run_v1_online_acceptance.py` | CLI / engineering runtime | V1 | High | `src/gas_calibrator/tools/run_v1_online_acceptance.py:157-171,608-783` | Must keep dual-gate dry-run default and CO2-only boundary. |
| `src/gas_calibrator/tools/run_v1_merged_calibration_sidecar.py` | offline sidecar | V2 | Medium | `src/gas_calibrator/tools/run_v1_merged_calibration_sidecar.py:26-34` | Naming suggests V1, but implementation depends on V2 config/export/download helpers. |
| `src/gas_calibrator/tools/run_v1_no500_postprocess.py` | offline sidecar | V2 | Medium | `src/gas_calibrator/tools/run_v1_no500_postprocess.py:19-20` | Another V1-named wrapper around V2 postprocess/export code. |
| `src/gas_calibrator/v2/adapters/legacy_runner.py` | compatibility adapter | V2 | Medium | `src/gas_calibrator/v2/adapters/legacy_runner.py:37` | Must stay out of V2 main runtime boundaries or be clearly marked as bridge-only. |
| `src/gas_calibrator/v2/adapters/v1_route_trace.py` / `src/gas_calibrator/v2/scripts/run_v1_route_trace.py` | trace tooling | V2 | Medium | `src/gas_calibrator/v2/adapters/v1_route_trace.py:10-11`, `src/gas_calibrator/v2/scripts/run_v1_route_trace.py:31-43` | Intended bridge tooling; not proof of V1 production runtime coupling. |
| `tests/test_v1_has_no_v2_runtime_imports.py` | guard test | SHARED | Low | new in this round | Fails if key V1 runtime files start importing `v2`. |
| `tests/test_v2_has_no_v1_runtime_imports.py` | guard test | SHARED | Low | new in this round | Fails if V2 main runtime roots start importing V1 runtime modules. |

## Readout

- The biggest breaking surfaces are still config defaults and runtime entrypoints, not the bridge tooling.
- The V1/V2 boundary is better protected by tests than by a large folder move right now.
