# V1.5 Formal Database Import Command Contract Test Evidence

- Date: 2026-07-02
- Scope: PostgreSQL 18 import command contract, database dry-run/preflight/authorization chain, full-flow wiring, formal run status, flow contract guard, and entrypoint inventory.
- Boundary: no COM, no PostgreSQL connection, no migration, no database import, no route control, no SN/device ID/SENCO/coefficient writes.

## Command

```powershell
python -m pytest tests\test_v1_5_formal_database_dry_run.py tests\test_v1_5_formal_database_import_preflight.py tests\test_v1_5_formal_database_import_authorization.py tests\test_v1_5_formal_database_import_command_contract.py tests\test_v1_5_formal_run_status.py tests\test_v1_5_full_flow_orchestration.py tests\test_v1_5_formal_flow_contract.py tests\test_v1_5_entrypoint_inventory.py -q
```

## Result

```text
109 passed in 29.53s
```
