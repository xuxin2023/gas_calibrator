# V1.5 Formal Database Dry-Run Test Evidence

- Date: 2026-07-02
- Scope: focused pytest evidence for the V1.5 PostgreSQL 18 schema/insert-preview contract.
- Boundary: offline tests only; no COM, no PostgreSQL connection, no pressure control, no gas/water route control, no SN/device ID write, no SENCO write, no database import.

## Command

```powershell
python -m pytest tests\test_v1_5_formal_database_dry_run.py tests\test_v1_5_entrypoint_inventory.py -q
```

## Stdout

```text
..............................                                           [100%]
30 passed in 6.13s
```

## Coverage

- `test_v1_5_formal_database_dry_run.py`
  - Confirms production backend is PostgreSQL 18.
  - Confirms `sn_code/device_code` is the production primary identity.
  - Confirms protocol device ID is a compatibility alias, not the primary identity.
  - Confirms COM/GA labels are run-local transport mapping only.
  - Confirms core storage constraints for sensors, aliases, points, samples, frames, fits, and coefficient versions.
  - Confirms `v1_5_evidence` registry tables and key evidence roles.
  - Confirms duplicate planned SN/device_code rows block the dry-run.
  - Confirms non-PostgreSQL-18 requirements block the dry-run.
  - Confirms writer and CLI export JSON, CSV, and Markdown evidence without importing data.
- `test_v1_5_entrypoint_inventory.py`
  - Confirms `export_v1_5_formal_database_dry_run.py` is classified as offline formal review evidence, not a database import runner.

## Compatibility Check

```powershell
python -m pytest tests\test_v1_5_formal_database_dry_run.py tests\test_v1_5_initialization_db_preflight.py tests\test_v1_5_evidence_registry.py tests\test_v1_5_entrypoint_inventory.py -q
```

Stdout:

```text
................................................................         [100%]
64 passed in 51.01s
```

## Full-Flow Integration Check

This additional check verifies that the PostgreSQL 18 dry-run contract is wired into the V1.5 full-flow planner and formal run-status rollup without becoming a real database import.

```powershell
python -m pytest tests\test_v1_5_full_flow_orchestration.py tests\test_v1_5_formal_flow_contract.py tests\test_v1_5_formal_run_status.py tests\test_v1_5_formal_database_dry_run.py tests\test_v1_5_entrypoint_inventory.py -q
```

Stdout:

```text
........................................................................ [ 81%]
................                                                         [100%]
88 passed in 22.50s
```

Coverage:

- `formal_database_dry_run_snapshot` is generated after `formal_evidence_sidecar` and before `database_import`.
- `formal_run_status_snapshot` consumes `v1_5_formal_database_dry_run.json`.
- The formal database dry-run gate is `ready` when PostgreSQL 18, `sn_code/device_code`, and dry-run boundaries hold.
- `connects_postgresql=false`, `database_written=false`, and `database_import_allowed=false` remain true inside the dry-run artifact.
- The dry-run gate does not open COM, control routes, write SN/device IDs, write coefficients, or modify mature CO2/H2O runners.

## Result

The V1.5 production database contract now has an offline PostgreSQL 18 schema and insert-preview guard, and the full-flow planner consumes it before the separate database import stage. Passing this guard confirms schema shape and identity semantics only; it does not authorize production database import, archive release, device writes, coefficient writes, or real acceptance.
