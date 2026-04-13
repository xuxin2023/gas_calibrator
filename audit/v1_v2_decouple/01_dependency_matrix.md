# V1 / V2 Dependency Matrix

- generated_at: 2026-04-13
- base_head: `367a1089ebaca1388dbb9d11648f74513316e502`
- branch: `codex/v1-v2-decouple-367a108`
- conclusion: current repository state is mostly co-resident tooling plus bridge modules, not a hard V1-main-runtime -> V2-main-runtime dependency.

| source_file | symbol_or_config_key | depends_on | dependency_type | required_by | runtime_or_dev_only | recommended_owner | why | risk_level |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `src/gas_calibrator/workflow/runner.py:16562` | `CalibrationRunner._maybe_write_coefficients` | `src/gas_calibrator/tools/run_v1_corrected_autodelivery.py:616` `write_senco_groups_with_full_verification` | function call | V1 | runtime | SHARED | V1 runner now reuses one writeback helper for snapshot -> write -> GETCO -> rollback -> restore, but the helper still lives in a CLI/tool module. | Medium |
| `src/gas_calibrator/workflow/runner.py:4573-4609` | `workflow.postrun_corrected_delivery.*` | `src/gas_calibrator/config.py:220-241` | config key | V1 | runtime | V1 | This is a V1 runtime safety boundary and must stay owned by V1 defaults, not by V2 UI or postprocess code. | High |
| `src/gas_calibrator/config.py:229-231` | `workflow.postrun_corrected_delivery.run_structure_hints.enabled` | `src/gas_calibrator/tools/run_v1_corrected_autodelivery.py:1239-1521` | config key | V1 | dev_only | V1 | The key is only consumed by V1 corrected-autodelivery reporting to emit recommendation artifacts such as `run_structure_hints.csv`; no V2 runtime consumer was found. | Low |
| `src/gas_calibrator/config.py:264-324` | `coefficients.h2o_zero_span` and `require_v1_h2o_zero_span_supported` | `src/gas_calibrator/tools/run_headless.py:235-241`, `src/gas_calibrator/workflow/runner.py:4625-4713`, `src/gas_calibrator/tools/run_v1_corrected_autodelivery.py:1485-1545`, `src/gas_calibrator/tools/run_v1_online_acceptance.py:157-171` | config key / function call | V1 | runtime | V1 | H2O zero/span NOT_SUPPORTED is now a V1 runtime guard shared across all V1 entrypoints; moving it to V2 would blur the boundary again. | High |
| `src/gas_calibrator/tools/run_headless.py:28-29` | `run_headless` main entry | `src/gas_calibrator/workflow/runner.py` | import | V1 | runtime | V1 | The default V1 runtime entry calls the V1 runner directly and does not import `src/gas_calibrator/v2/**`. | Low |
| `src/gas_calibrator/tools/run_v1_online_acceptance.py:608-783` | `run_online_acceptance` | `src/gas_calibrator/tools/run_v1_corrected_autodelivery.py:616`, `src/gas_calibrator/config.py:316-324` | function call / config key | V1 | runtime | V1 | The online acceptance tool stays on the V1 side and only reuses V1 safety guards plus the shared writeback helper; it does not need V2 runtime. | Medium |
| `src/gas_calibrator/tools/run_v1_merged_calibration_sidecar.py:26-34` | merged sidecar offline workflow | `src/gas_calibrator/v2/config`, `src/gas_calibrator/v2/adapters/analyzer_coefficient_downloader.py`, `src/gas_calibrator/v2/export/ratio_poly_report.py` | import | BOTH | dev_only | V2 | This is an engineering sidecar operating on completed V1 runs with V2 report/export utilities. The docstring already says it is outside the V1 UI and default workflow. | Medium |
| `src/gas_calibrator/tools/run_v1_no500_postprocess.py:19-20` | no-500 offline postprocess | `src/gas_calibrator/v2/adapters/v1_postprocess_runner.py`, `src/gas_calibrator/v2/export` | import | BOTH | dev_only | V2 | Another offline-only V1-named tool that is functionally a V2 postprocess wrapper. It should remain outside V1 runtime. | Medium |
| `src/gas_calibrator/v2/adapters/legacy_runner.py:37` | `LegacyCalibrationRunner` | `src/gas_calibrator/workflow/runner.py` | import | V2 | dev_only | V2 | This is a compatibility adapter allowing V2 surfaces to invoke V1. It is not the V2 main runtime, but it is an intentional bridge that must stay clearly marked as such. | Medium |
| `src/gas_calibrator/v2/adapters/v1_route_trace.py:10-11` | `TracedCalibrationRunner` | `src/gas_calibrator/logging_utils.py`, `src/gas_calibrator/workflow/runner.py` | import | V2 | dev_only | V2 | Route tracing is a V2-aligned wrapper around V1 execution. It is bridge tooling, not proof of V1 runtime depending on V2. | Medium |
| `src/gas_calibrator/v2/scripts/run_v1_route_trace.py:31-43` | `run_v1_route_trace` device bootstrap helpers | `src/gas_calibrator/tools/run_headless.py` | function call | V2 | dev_only | V2 | The script intentionally reuses V1 headless device setup helpers for tracing completed or simulated V1 runs. Keep it as bridge tooling. | Medium |
| `src/gas_calibrator/v2/sim/parity.py:9` | `V1RunLogger` parity formatter dependency | `src/gas_calibrator/logging_utils.py` | import | V2 | dev_only | SHARED | Parity compares V1-format artifacts and currently reaches into the V1 logger type; the artifact schema is shared, but this is not a V1 runtime dependency. | Low |

## Readout

- `V1 runtime -> V2 runtime`: not found in the key V1 runtime files scanned in this round.
- `V1 offline sidecars -> V2`: found in merged sidecar and no-500 postprocess tooling; both are engineering/offline surfaces, not the default V1 production runtime.
- `V2 bridge/dev tooling -> V1`: found in `legacy_runner`, `v1_route_trace`, `run_v1_route_trace`, and parity helpers; these are explicit compatibility/dev paths.
