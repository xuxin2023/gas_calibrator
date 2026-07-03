# V1.5 formal database dry-run contract

This is an offline PostgreSQL 18 schema and insert-preview contract for V1.5 production evidence.

- overall_status: `ready_for_postgresql18_schema_dry_run_review`
- blocker_count: `0`
- production backend: `postgresql` `18`
- primary identity: `sn_code/device_code`
- protocol device ID role: `compatibility_alias_and_command_identity`
- database_import_allowed: `False`
- formal_release_allowed: `False`
- This dry-run does not connect PostgreSQL, open COM, control routes, write SN/device IDs, write coefficients, release archives, or import data.

## Checks

- `postgresql18_backend_contract`: `ready`
- `core_storage_schema_contract`: `ready`
- `evidence_registry_schema_contract`: `ready`
- `sn_device_code_identity_contract`: `ready`
- `planned_device_identity_preview`: `ready`
- `insert_preview_contract`: `ready`
- `dry_run_does_not_authorize_import_or_release`: `ready`
