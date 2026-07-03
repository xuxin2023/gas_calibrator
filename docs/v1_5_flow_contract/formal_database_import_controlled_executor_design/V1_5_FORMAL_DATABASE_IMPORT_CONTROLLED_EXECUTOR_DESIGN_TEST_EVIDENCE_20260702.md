# V1.5 formal database import controlled executor design test evidence

Generated: 2026-07-02

## Scope

This focused test run covers the offline controlled-executor design contract for a future PostgreSQL 18 import.

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
  tests\test_v1_5_formal_database_import_controlled_executor_design.py `
  tests\test_v1_5_formal_database_import_blocked_executor.py `
  tests\test_v1_5_formal_run_status.py `
  tests\test_v1_5_full_flow_orchestration.py `
  tests\test_v1_5_formal_flow_contract.py `
  tests\test_v1_5_entrypoint_inventory.py `
  -q
```

## Result

```text
105 passed in 35.73s
```

## Contract Verified

- Future import execution requires a separate `--execute-controlled-import` design, not the current blocked stub.
- Future executor must require exact operator confirmation and distinct reviewer/approver authorization.
- DSN must be read from environment/secret store only; DSN value is not serialized.
- Future import must use a PostgreSQL 18 transaction with pre-commit row-count/hash readback.
- Validation/readback failures before commit require transaction rollback and no partial acceptance.
- Post-commit discrepancies require DBA/reviewer hold, not automatic deletion.
