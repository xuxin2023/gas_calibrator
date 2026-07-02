# V1.5 formal database import command contract

This is an offline command contract for a future separately controlled PostgreSQL 18 import.

- overall_status: `review_required`
- blocker_count: `0`
- review_required_count: `4`
- command_contract_ready: `False`
- real_import_execution_allowed: `False`
- database_import_allowed: `False`
- requested_command_module: `gas_calibrator.tools.import_v1_5_evidence_package`
- This artifact does not connect PostgreSQL, apply migrations, import data, open COM, control routes, or write analyzer state.

## Checks

- `formal_database_import_authorization_ready`: `review_required` authorization_status=review_required;authorization_review_required_count=3;preflight_ready=False;archive_release_ready=False;manual_authorization_ready=False;database_import_allowed=False
- `formal_database_import_preflight_ready`: `review_required` preflight_status=review_required;preflight_review_required_count=1;dsn_configured_not_true
- `formal_archive_closure_ready`: `review_required` archive_closure_missing
- `formal_evidence_bundle_ready`: `review_required` evidence_bundle_missing
- `controlled_import_command_contract`: `ready`
- `migration_execution_lock`: `ready`
