# V1.5 PostgreSQL 18 migration 002 controlled executor test evidence

Date: 2026-07-14

## Scope

This package adds the fixed-target, manual-authorized execution path for
`002_v1_5_production_import_ledger`. The checked-in artifact is the default
locked preview. No test or documentation command reads `V1_5_POSTGRES_DSN`,
connects PostgreSQL, or applies a migration.

## Focused verification

```text
19 passed in 3.38s
```

Command:

```powershell
python -m pytest tests/test_v1_5_formal_database_migration_production_controlled_executor.py -q
```

The tests cover exact DBA packet/script binding, three-party authorization,
target/version/checksum/schema holds, source drift, no-DSN preview behavior,
rollback, commit-uncertain handling, postcommit postcheck hold, exact idempotent
replay, and entrypoint classification.

## Database-chain verification

```text
214 passed, 1 skipped, 1 warning in 50.34s
```

Command:

```powershell
$dbTests = @(Get-ChildItem tests -Filter 'test_v1_5_formal_database*.py' | ForEach-Object { $_.FullName })
$dbTests += (Resolve-Path tests\test_v1_5_evidence_registry.py).Path
$dbTests += (Resolve-Path tests\test_v1_5_entrypoint_inventory.py).Path
python -m pytest @dbTests -q
```

The skip is the explicitly gated PostgreSQL staging integration test because
`V1_5_POSTGRES_STAGING_DSN_TEST` was not configured. The warning is the
existing unregistered `v1_5_formal_gate` marker.

## Parity and resilience verification

```text
30 passed in 26.07s
```

Command:

```powershell
python -m pytest tests/test_v1_5_historical_fit_profile_parity.py tests/v2/test_summary_parity.py tests/v2/test_closeout_readiness_ui_parity.py tests/v2/test_export_resilience.py -q
```

## Boundary conclusion

- Default CLI execution is no-DSN/no-connect and produces only locked preview artifacts.
- Real execution requires the exact execute flag, three distinct actors, a current authorization, fixed production target, and four immutable source bindings.
- Migration execution never imports calibration evidence and never grants database import or formal release.
- The package does not open COM, write device identity or coefficients, control pressure/routes, or modify mature V1.5 CO2/H2O execution paths.
- A real migration has not been executed by this package or this test evidence.
