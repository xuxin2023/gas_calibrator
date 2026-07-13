# V1.5 PostgreSQL 18 DBA migration readiness test evidence

Date: 2026-07-14

## Scope

This evidence covers the no-connect DBA handoff for repository migrations
`001_v1_5_evidence_registry` and `002_v1_5_production_import_ledger`, the
production database import contract chain, entrypoint classification, evidence
registry compatibility, and parity/export resilience.

## Focused and database-chain verification

```text
195 passed, 1 skipped, 1 warning in 45.34s
```

Command:

```powershell
$dbTests = @(Get-ChildItem tests -Filter 'test_v1_5_formal_database*.py' | ForEach-Object { $_.FullName })
$dbTests += (Resolve-Path tests\test_v1_5_evidence_registry.py).Path
$dbTests += (Resolve-Path tests\test_v1_5_entrypoint_inventory.py).Path
python -m pytest @dbTests -q
```

The skipped test is the explicitly gated PostgreSQL 18 staging integration
test because `V1_5_POSTGRES_STAGING_DSN_TEST` was not configured. This package
did not read a DSN or open a PostgreSQL connection. The warning is the existing
unregistered `v1_5_formal_gate` marker.

## Parity and resilience verification

```text
30 passed in 20.50s
```

Command:

```powershell
python -m pytest tests\test_v1_5_historical_fit_profile_parity.py tests\v2\test_summary_parity.py tests\v2\test_closeout_readiness_ui_parity.py tests\v2\test_export_resilience.py -q
```

## Static verification

```text
All checks passed!
```

Command:

```powershell
python -m ruff check src/gas_calibrator/validation/v1_5_formal_database_migration_dba_readiness.py src/gas_calibrator/tools/export_v1_5_formal_database_migration_dba_readiness.py tests/test_v1_5_formal_database_migration_dba_readiness.py src/gas_calibrator/validation/v1_5_entrypoint_inventory.py
```

## Boundary conclusion

- The exporter reads only repository migration files.
- It emits checksum-bound precheck/apply/postcheck SQL and a template-only
  operator/reviewer/approver execution record.
- It rejects DSN, connection, execute, migration-apply, and production-import
  arguments.
- It does not connect PostgreSQL, apply a migration, write database rows, open
  COM, write analyzer identity or coefficients, or control pressure/gas/water
  routes.
- `dba_packet_ready=true` means ready for DBA review only. It does not authorize
  migration execution, production import, formal release, or real acceptance.
