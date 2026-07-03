# V1.5 formal database import controlled executor design

This is an offline design review for a future PostgreSQL 18 import executor.

- overall_status: `ready_for_controlled_import_executor_design_review`
- production_state: `blocked_design_only`
- execution_supported: `False`
- real_import_execution_allowed: `False`
- database_import_allowed: `False`
- connects_postgresql: `False`
- database_written: `False`

Future executor requirements:

- Explicit `--execute-controlled-import` flag and exact operator confirmation text.
- Distinct reviewer and approver plus authorization id.
- DSN via environment/secret store only; DSN value must not be serialized.
- One PostgreSQL 18 transaction with pre-commit row-count/hash readback.
- Rollback on validation/readback failure before commit; post-commit discrepancies require DBA/reviewer hold.

Current package remains blocked and does not implement the real executor.
