# V1.5 production controlled executor test evidence

Date: 2026-07-13

No production DSN was read. No PostgreSQL production connection, migration,
schema change, or row write was attempted.

## Database-chain regression

```text
python -m pytest tests\test_v1_5_formal_database_import_production_controlled_executor.py tests\test_v1_5_formal_database_import_production_promotion_preflight.py tests\test_v1_5_formal_database_import_staging_executor.py tests\test_v1_5_formal_database_dry_run.py tests\test_v1_5_formal_database_import_transaction_plan.py tests\test_v1_5_evidence_registry.py tests\test_v1_5_entrypoint_inventory.py -q

116 passed, 1 skipped, 1 warning in 42.78s
```

The skipped test requires an explicitly configured PostgreSQL 18 staging/test
DSN. The warning is the existing unregistered `v1_5_formal_gate` pytest mark.

## Parity and export resilience

```text
python -m pytest tests\test_v1_5_historical_fit_profile_parity.py tests\v2\test_summary_parity.py tests\v2\test_closeout_readiness_ui_parity.py tests\v2\test_export_resilience.py -q

30 passed in 22.29s
```

## Final focused rerun

```text
python -m pytest tests\test_v1_5_formal_database_import_production_controlled_executor.py tests\test_v1_5_formal_database_import_staging_executor.py tests\test_v1_5_formal_database_import_production_promotion_preflight.py -q

38 passed, 1 skipped in 6.93s
```

## Static checks

```text
python -m ruff check src\gas_calibrator\storage\v1_5_evidence\production_import.py src\gas_calibrator\validation\v1_5_formal_database_import_production_controlled_executor.py src\gas_calibrator\tools\run_v1_5_formal_database_import_production_controlled_executor.py tests\test_v1_5_formal_database_import_production_controlled_executor.py

All checks passed!
```

`git diff --check` also passed; only the existing Windows LF/CRLF warnings were
reported.

## Final review hardening

The merge review found and closed two production-safety gaps without reading a
DSN or opening a PostgreSQL connection:

- Authorization, promotion preflight, transaction plan, and evidence bundle
  are now consumed from immutable byte snapshots whose SHA256 values must still
  match the three-party authorization immediately before the transaction runner.
- A connection loss after COMMIT is attempted is now reported as
  `production_database_write_state=unknown_commit_uncertain`; it is never
  represented as a confirmed no-write result and never enables database import
  or formal release.

Focused tests prove that a source file changed after authorization keeps the
transaction runner closed, and that a commit-uncertain result preserves the
unknown write state while all release gates remain false.
