# V1.5 Legacy Historical Evidence Catalog Review

## Decision

Legacy point directories are useful diagnostic and traceability evidence, but they are not a substitute for one closed 0613/0620/0621 mature queue run. Segmented runs, retries, direct recoveries, and derived accepted-composite manifests remain ineligible for route attestation, formal fitting, release, and database import.

## Offline Scan Scope

- `D:\gas_calibrator\_p9_20260705`
- `D:\gas_calibrator\_p9_20260706`
- `D:\gas_calibrator\_handoff\v1_5_formal_route_real_20260624`
- `D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean\_handoff\new_algo_001_init_20260627`
- `D:\_gas_calibrator_archive\20260622_first_gate`
- accepted composite manifest: `co2_6old_0620clean_mature45_acceptance_manifest_20260706/accepted_co2_45_point_manifest.csv`

The scan is explicit-root and read-only. It does not search arbitrary disks, open COM ports, control pressure/routes, write coefficients or identity, or connect PostgreSQL.

## Result

- Point directories indexed: `181`
- CO2 / H2O point directories: `145 / 36`
- Accepted composite rows bound to point directories: `45 / 45`
- Accepted composite members from retry/direct recovery lineage: `11`
- Accepted composite members with warnings: `2`
- 0624/migration point directories: `47`, all forbidden for formal promotion
- Missing component-level QC: CO2 `102`, H2O `36`
- Legacy archive `D:\_gas_calibrator_archive\20260622_first_gate`: `0` recognized V1.5 formal point directories

The accepted 45-point composite does not prove continuous route execution. It contains points drawn from multiple queue segments plus retry/direct recovery sources. The two warning rows retain their warnings instead of being normalized to clean points.

## Interpretation Locks

- `continuous_route_attestation_allowed=false`
- `historical_fit_allowed=false`
- `formal_release_allowed=false`
- `database_import_allowed=false`
- `not_real_acceptance_evidence=true`
- CO2 zero gas is not interchangeable with an H2O dry-gas anchor.
- This catalog never infers an anchor role from a filename or nominal concentration.

## Next Use

The catalog may support point-level anomaly review, lineage repair, and identification of missing QC artifacts. A later fit-input normalizer may consume a point only after its own immutable input/QC contract passes; this catalog alone never authorizes fitting.

## Validation

Focused compatibility command:

```text
python -m pytest tests/test_v1_5_legacy_historical_evidence_catalog.py tests/test_v1_5_historical_mature_root_discovery.py tests/test_v1_5_historical_route_attestation_binder.py tests/test_v1_5_historical_fit_evidence_normalizer.py tests/test_v1_5_mature_route_continuity_gate.py tests/test_v1_5_historical_replay_evidence.py tests/test_v1_5_entrypoint_inventory.py -q
```

Result: `92 passed, 1 existing marker warning in 23.34s`.

Static validation: ruff passed, Python compilation passed, and `git diff --check` passed apart from Windows line-ending notices.
