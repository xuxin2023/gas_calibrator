# V1.5 formal database import authorization

This is an offline authorization guard for a future separately controlled PostgreSQL 18 import.

- overall_status: `review_required`
- blocker_count: `0`
- review_required_count: `3`
- preflight_ready: `False`
- archive_release_ready: `False`
- manual_authorization_ready: `False`
- database_import_allowed: `False`
- This artifact does not connect PostgreSQL, apply migrations, import data, open COM, control routes, or write analyzer state.

## Checks

- `formal_database_import_preflight_ready`: `review_required` preflight_status=review_required;preflight_review_required_count=1;dsn_configured_not_true
- `formal_archive_release_ready`: `review_required` archive_closure_missing
- `manual_database_import_authorization_record`: `review_required` reviewer_missing;approver_missing;authorization_id_missing
- `migration_execution_lock`: `ready`
- `real_import_command_must_consume_authorization`: `ready`
