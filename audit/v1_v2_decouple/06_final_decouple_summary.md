# Final Decouple Summary

- base_head: `367a1089ebaca1388dbb9d11648f74513316e502`
- branch: `codex/v1-v2-decouple-367a108`

## Final Verdict

- No direct V1 main-runtime -> V2 main-runtime dependency was found.
- No direct V2 main-runtime -> V1 main-runtime dependency was found in the guarded V2 runtime roots.
- Current repository state is mainly:
  - shared repository co-existence
  - explicit bridge/adaptor tooling
  - offline sidecars that reuse V2 utilities on top of completed V1 artifacts

## What Changed In This Round

- Restored `workflow.postrun_corrected_delivery.enabled=False` and `write_devices=False` in `src/gas_calibrator/config.py:220-223` so the V1-safe default is back in force.
- Added import-boundary guard tests:
  - `tests/test_v1_has_no_v2_runtime_imports.py`
  - `tests/test_v2_has_no_v1_runtime_imports.py`
- Updated `tests/test_config_runtime_defaults.py` so the config-default test matches the restored V1-safe runtime contract.

## V1 Invariants Still Held After This Round

- CO2 main chain remains the V1 production path.
- H2O zero/span remains `NOT_SUPPORTED`.
- Default real-device write is disabled again.
- Online acceptance remains double-gated and CO2-only.
- Point/sample traceability fields are untouched in this round.

## Recommendation

- Current repository state does **not** need a large split or a broad code move right now.
- Keep the bridge modules as explicit boundary tooling.
- Keep the new import guard tests and the V1 writeback safety tests in the focused regression set.
