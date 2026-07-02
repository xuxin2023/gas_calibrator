# V1.5 Formal Run Status Import-Allowed Gate Test Evidence

Date: 2026-07-02

Scope:

- Tighten `database_import_allowed` so database import requires actual ready gates.
- `formal_database_import_controlled_executor_design` must exist and be ready.
- Missing or review-required controlled executor design evidence keeps database import locked.
- This evidence is offline only; it does not connect PostgreSQL, open COM ports, control routes, or write devices.

Command:

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

Result:

```text
106 passed in 42.66s
```

Boundary:

- `connects_postgresql=false`
- `database_written=false`
- `database_import_allowed=false` unless the full import gate chain is present and ready
- mature CO2/H2O runners are not part of this test scope
