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

## Result

The V1.5 production database contract now has an offline PostgreSQL 18 schema and insert-preview guard. Passing this guard confirms schema shape and identity semantics only; it does not authorize production database import, archive release, device writes, coefficient writes, or real acceptance.
