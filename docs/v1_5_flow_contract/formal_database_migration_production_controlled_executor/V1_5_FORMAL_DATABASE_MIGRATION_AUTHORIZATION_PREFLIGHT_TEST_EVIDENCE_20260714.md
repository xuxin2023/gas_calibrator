# V1.5 PostgreSQL 18 migration authorization preflight test evidence

Date: 2026-07-14

## Scope

This follow-up adds `--validate-authorization-only` to the existing controlled
migration executor. The mode validates the completed three-party packet,
freshness window, fixed production target, no-import/no-release boundaries, and
all four source paths and SHA256 hashes.

- It is mutually exclusive with `--execute-postgresql18-migration`.
- It does not read `V1_5_POSTGRES_DSN`.
- It does not connect PostgreSQL or apply migration 002.
- It does not import calibration evidence or grant database import/release.
- It does not open COM, write analyzer state, or control pressure/gas/water.

## Focused verification

```text
python -m pytest tests\test_v1_5_formal_database_migration_production_controlled_executor.py -q

22 passed in 4.22s
```

The added tests prove that a valid authorization produces
`ready_for_postgresql18_migration_execution_operator_handoff` while retaining
`dsn_value_read=false`, `execution_attempted=false`, and
`connects_postgresql=false`. Invalid actor identity or combining validation and
execution is rejected without reading the DSN.

## Database-chain regression

```text
$dbTests = @(Get-ChildItem tests -Filter 'test_v1_5_formal_database*.py' | ForEach-Object { $_.FullName })
$dbTests += (Resolve-Path tests\test_v1_5_evidence_registry.py).Path
$dbTests += (Resolve-Path tests\test_v1_5_entrypoint_inventory.py).Path
python -m pytest @dbTests -q

227 passed, 1 skipped, 1 warning in 70.56s
```

The skip is the explicitly gated real staging integration test because its
dedicated test DSN was not enabled in this no-connect regression. The warning
is the existing unregistered `v1_5_formal_gate` marker.

## Export resilience and parity

```text
python -m pytest tests\v2\test_export_resilience.py tests\v2\test_summary_parity.py -q

6 passed in 20.23s
```

## Static validation

```text
python -m ruff check src\gas_calibrator\tools\run_v1_5_formal_database_migration_production_controlled_executor.py src\gas_calibrator\validation\v1_5_formal_database_migration_production_controlled_executor.py tests\test_v1_5_formal_database_migration_production_controlled_executor.py

All checks passed!
```

## Result

The program-side authorization preparation chain is complete. A real
authorization artifact still requires three current, distinct human actors and
does not exist until they supply and review those identities.
