# V1.5 formal database import execute-lock test evidence

Generated: 2026-07-03

## Scope

This focused follow-up verifies that the current V1.5 PostgreSQL 18 import command
continues to be a no-connect, no-write blocked executor stub. It explicitly
rejects future real-import execution and authorization metadata flags instead of
allowing them to look like a usable production import path.

The package remains offline:

- opens_com_ports: `false`
- connects_postgresql: `false`
- applies_migrations: `false`
- database_import_attempted: `false`
- database_written: `false`
- database_import_allowed: `false`
- real_import_execution_allowed: `false`
- not_real_acceptance_evidence: `true`

## Command

```powershell
python -m pytest `
  tests\test_v1_5_formal_database_import_blocked_executor.py `
  tests\test_v1_5_formal_database_import_controlled_executor_design.py `
  tests\test_v1_5_formal_database_import_command_contract.py `
  tests\test_v1_5_formal_database_import_preflight.py `
  tests\test_v1_5_formal_database_import_authorization.py `
  tests\test_v1_5_formal_run_status.py `
  tests\test_v1_5_full_flow_orchestration.py `
  tests\test_v1_5_entrypoint_inventory.py `
  -q
```

## Result

```text
98 passed, 1 warning in 72.66s
```

Warning:

```text
PytestUnknownMarkWarning: Unknown pytest.mark.v1_5_formal_gate
```

## Contract Verified

- `--execute-controlled-import` is accepted only as a locked future flag and returns a refusal.
- Operator/reviewer/approver/authorization metadata is rejected by the current blocked stub.
- DSN and migration flags remain locked.
- The blocked executor still writes only offline evidence artifacts.
- `formal_run_status` and full-flow orchestration continue to keep database import locked.
