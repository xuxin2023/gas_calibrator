# V1.5 production controlled executor test evidence

Date: 2026-07-13

No production DSN was read. No PostgreSQL production connection, migration,
schema change, or row write was attempted.

## Database-chain regression

```text
python -m pytest tests\test_v1_5_formal_database_import_production_controlled_executor.py tests\test_v1_5_formal_database_import_production_promotion_preflight.py tests\test_v1_5_formal_database_import_staging_executor.py tests\test_v1_5_formal_database_dry_run.py tests\test_v1_5_formal_database_import_transaction_plan.py tests\test_v1_5_evidence_registry.py tests\test_v1_5_entrypoint_inventory.py -q

114 passed, 1 skipped, 1 warning in 27.53s
```

The skipped test requires an explicitly configured PostgreSQL 18 staging/test
DSN. The warning is the existing unregistered `v1_5_formal_gate` pytest mark.

## Parity and export resilience

```text
python -m pytest tests\test_v1_5_historical_fit_profile_parity.py tests\v2\test_summary_parity.py tests\v2\test_closeout_readiness_ui_parity.py tests\v2\test_export_resilience.py -q

30 passed in 23.71s
```

## Final focused rerun

```text
python -m pytest tests\test_v1_5_formal_database_import_production_controlled_executor.py tests\test_v1_5_formal_database_import_staging_executor.py tests\test_v1_5_formal_database_import_production_promotion_preflight.py -q

36 passed, 1 skipped in 10.09s
```

## Static checks

```text
python -m ruff check src\gas_calibrator\storage\v1_5_evidence\production_import.py src\gas_calibrator\validation\v1_5_formal_database_import_production_controlled_executor.py src\gas_calibrator\tools\run_v1_5_formal_database_import_production_controlled_executor.py tests\test_v1_5_formal_database_import_production_controlled_executor.py

All checks passed!
```

`git diff --check` also passed; only the existing Windows LF/CRLF warnings were
reported.
