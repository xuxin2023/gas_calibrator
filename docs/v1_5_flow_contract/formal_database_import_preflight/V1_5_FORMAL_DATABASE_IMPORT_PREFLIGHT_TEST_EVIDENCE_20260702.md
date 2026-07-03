# V1.5 Formal Database Import Preflight Test Evidence

Date: 2026-07-02

Scope:

- Adds the offline `formal_database_import_preflight` sidecar after PostgreSQL 18 dry-run and before any database import step.
- Verifies DSN presence/fingerprint, migration lock, import execution lock, SN/device_code identity contract, and explicit authorization boundary.
- Does not connect to PostgreSQL, apply migrations, import rows, open COM ports, control routes, write SN/device ID, or write coefficients.

Focused pytest:

```text
$env:PYTHONPATH='src'; python -m pytest tests\test_v1_5_full_flow_orchestration.py tests\test_v1_5_formal_flow_contract.py tests\test_v1_5_formal_run_status.py tests\test_v1_5_formal_database_dry_run.py tests\test_v1_5_formal_database_import_preflight.py tests\test_v1_5_entrypoint_inventory.py -q
........................................................................ [ 75%]
.......................                                                  [100%]
95 passed in 17.50s
```

Boundary conclusion:

- `database_import_allowed` remains false when DSN/import preflight is not ready.
- Missing DSN is a review item, not a physical-flow blocker.
- The preflight is offline evidence only and is not real database import authorization.
