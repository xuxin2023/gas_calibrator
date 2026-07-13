# V1.5 production import migration-evidence gate test evidence

Date: 2026-07-14

No production DSN was read. No PostgreSQL connection, migration, schema change,
or evidence import was attempted. All migration execution artifacts used by the
tests were local fixtures.

## Gate behavior

The production import package now remains blocked unless it consumes a
confirmed migration-002 controlled-executor artifact for the fixed PostgreSQL
18 target. The artifact must prove current repository migration checksums, the
exact ledger shape, committed or exact idempotent state, and its own
three-party authorization/source bindings.

Migration precheck/postcheck and the later production import are bound to the
same PostgreSQL cluster using `pg_control_system().system_identifier`. A
missing, malformed, changed, or different live identifier holds before the
import performs a row write.

The production-import authorization must bind the exact path and SHA256 of the
migration artifact. Missing, malformed, replaced, or subsequently changed
migration evidence holds before the CLI reads `V1_5_POSTGRES_DSN` or invokes the
transaction runner.

## Focused tests

```text
python -m pytest tests/test_v1_5_formal_database_import_production_controlled_executor.py -q

24 passed in 7.59s
```

```text
python -m pytest tests/test_v1_5_formal_database_migration_production_controlled_executor.py -q

20 passed in 3.33s
```

## Database-chain regression

```text
$dbTests = @(Get-ChildItem tests -Filter 'test_v1_5_formal_database*.py' | ForEach-Object { $_.FullName })
$dbTests += (Resolve-Path tests\test_v1_5_evidence_registry.py).Path
$dbTests += (Resolve-Path tests\test_v1_5_entrypoint_inventory.py).Path
python -m pytest @dbTests -q

225 passed, 1 skipped, 1 warning in 48.46s
```

The skipped test requires the explicitly configured
`V1_5_POSTGRES_STAGING_DSN_TEST`. The warning is the existing unregistered
`v1_5_formal_gate` pytest mark.

## Parity and export resilience

```text
python -m pytest tests/test_v1_5_historical_fit_profile_parity.py tests/v2/test_summary_parity.py tests/v2/test_closeout_readiness_ui_parity.py tests/v2/test_export_resilience.py -q

30 passed in 19.55s
```

## Static checks

```text
python -m ruff check src/gas_calibrator/storage/v1_5_evidence/production_import.py src/gas_calibrator/storage/v1_5_evidence/production_migration.py src/gas_calibrator/validation/v1_5_formal_database_import_production_controlled_executor.py src/gas_calibrator/tools/run_v1_5_formal_database_import_production_controlled_executor.py tests/test_v1_5_formal_database_import_production_controlled_executor.py tests/test_v1_5_formal_database_migration_production_controlled_executor.py

All checks passed!
```

`git diff --check` passed apart from the existing Windows LF/CRLF conversion
notices.

## Boundary result

- Mature 0613/0620/0621 CO2 and H2O paths were not changed.
- `run_app.py`, analyzer protocol code, shared sampling, and route control were
  not changed.
- The production importer still does not apply migrations.
- `database_import_allowed` and `formal_release_allowed` remain false in the
  preview and all blocked outcomes.
- A real migration and a real production import remain separate future
  operations, each requiring its own current three-party authorization.
