# V1.5 formal database import blocked executor test evidence

Generated: 2026-07-02

## Scope

This focused test run covers the PostgreSQL 18 import command blocked-executor stub and the V1.5 flow/status guards that consume it.

The package remains offline:

- opens_com_ports: `false`
- connects_postgresql: `false`
- applies_migrations: `false`
- database_import_attempted: `false`
- database_written: `false`
- writes_sn/device_id/coefficients: `false`
- not_real_acceptance_evidence: `true`

## Command

```powershell
python -m pytest `
  tests\test_v1_5_evidence_registry.py::test_evidence_import_cli_dry_run_writes_bundle_json_without_database `
  tests\test_v1_5_evidence_registry.py::test_evidence_import_cli_dry_run_indexes_run_evidence_status_artifacts `
  tests\test_v1_5_formal_database_import_blocked_executor.py `
  tests\test_v1_5_formal_run_status.py `
  tests\test_v1_5_full_flow_orchestration.py `
  tests\test_v1_5_formal_flow_contract.py `
  tests\test_v1_5_entrypoint_inventory.py `
  -q
```

## Result

```text
101 passed in 35.06s
```

## Contract Verified

- `import_v1_5_evidence_package.py` no longer exposes a real PostgreSQL import path in this V1.5 flow package.
- Legacy evidence-bundle generation remains dry-run only.
- The new command-contract mode writes a blocked executor artifact instead of connecting to PostgreSQL.
- `formal_run_status` keeps `formal_release_allowed` separate from `database_import_allowed`.
- The full-flow plan orders `formal_database_import_blocked_executor_snapshot` after the command contract and before the future `database_import` stage.
