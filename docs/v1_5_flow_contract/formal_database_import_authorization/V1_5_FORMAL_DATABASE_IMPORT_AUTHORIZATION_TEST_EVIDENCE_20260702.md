# V1.5 Formal Database Import Authorization Test Evidence

Date: 2026-07-02

Scope:

- Adds the offline `formal_database_import_authorization` sidecar after PostgreSQL 18 import preflight and before any database import step.
- Verifies formal archive release evidence, import preflight readiness, and manual operator/reviewer/approver authorization labels.
- Does not connect to PostgreSQL, apply migrations, import rows, open COM ports, control routes, write SN/device ID, or write coefficients.

Focused pytest:

```text
$env:PYTHONPATH='src'; python -m pytest tests\test_v1_5_full_flow_orchestration.py tests\test_v1_5_formal_flow_contract.py tests\test_v1_5_formal_run_status.py tests\test_v1_5_formal_database_dry_run.py tests\test_v1_5_formal_database_import_preflight.py tests\test_v1_5_formal_database_import_authorization.py tests\test_v1_5_entrypoint_inventory.py -q
........................................................................ [ 70%]
..............................                                           [100%]
102 passed in 18.61s
```

Boundary conclusion:

- A passing preflight alone is not database import authorization.
- Missing archive release or manual authorization remains `review_required`.
- The authorization sidecar is no-connect/no-import evidence; the real import command must consume it separately.
