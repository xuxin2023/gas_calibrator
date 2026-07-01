# V1.5 formal database import preflight

This is an offline preflight for a future separately authorized PostgreSQL 18 import.

- overall_status: `review_required`
- blocker_count: `0`
- review_required_count: `1`
- production backend: `postgresql` `18`
- dsn_configured: `False`
- dry_run_contract_ready: `True`
- database_import_allowed: `False`
- This preflight does not connect PostgreSQL, apply migrations, import data, open COM, control routes, or write analyzer state.

## Checks

- `formal_database_dry_run_contract_ready`: `ready`
- `postgresql_dsn_configuration_preview`: `review_required` dsn_missing
- `migration_execution_lock`: `ready`
- `database_import_execution_lock`: `ready`
- `identity_key_and_alias_contract`: `ready`
- `release_and_authorization_gate_required`: `ready`
