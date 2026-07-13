# V1.5 PostgreSQL 18 staging import executor

## Purpose

This executor is the first database-writing V1.5 import surface, but it is restricted to PostgreSQL 18 staging/test databases and isolated staging schemas. It proves transaction, identity, idempotency, rollback, and lookup behavior before any production database importer is enabled.

It is not production database import evidence and it does not authorize formal release.

## Inputs

- reviewed `v1_5_formal_database_import_transaction_plan_v1` JSON;
- frozen `v1_5_evidence_registry` bundle JSON;
- 1-6 planned analyzers with unique eight-digit `sn_code/device_code`;
- unique three-digit protocol device IDs for the current run;
- staging DSN from `V1_5_POSTGRES_STAGING_DSN` or another staging/test-scoped environment variable;
- authorization ID, operator, reviewer, approver, and exact staging-only confirmation text.

The reviewer and approver must be different people.

## Execution boundary

Default invocation is preview-only and does not read the DSN environment value.

Staging execution requires all of:

```text
--execute-staging-import
--authorization-id <id>
--operator <name>
--reviewer <name>
--approver <different-name>
--operator-confirmation-text "I AUTHORIZE V1.5 POSTGRESQL 18 STAGING IMPORT ONLY"
```

The database name must contain `staging` or `test`. The only accepted schema families are:

```text
v1_5_core_staging[_suffix]
v1_5_evidence_staging[_suffix]
```

`public` and production `v1_5_evidence` are rejected.

## Atomic transaction

One transaction performs:

1. PostgreSQL server-major check (`18` only);
2. optional creation of isolated staging schemas;
3. idempotency-ledger lock by `run_db_id`;
4. core identity insertion for SN/device_code/protocol-ID aliases;
5. evidence-registry bundle insertion;
6. pre-commit table-count readback;
7. pre-commit identity and protocol-alias readback;
8. staging ledger insertion;
9. commit.

Any exception or readback mismatch rolls back both staging schemas. Existing rows without a matching ledger are held for review rather than deleted or replaced.

If the database reports an error after commit has been attempted and rollback cannot be confirmed, the result is `staging_import_commit_uncertain_hold`; it is never reported as a confirmed rollback or a successful import.

Re-importing the same run, plan hash, and bundle hash is an idempotent no-op. Reusing a run with changed content is rejected and rolled back.

## Query support

`query_v1_5_formal_database_import_staging` supports explicit read-only lookup by:

- `sn_code`;
- `device_code`;
- `protocol_device_id`;
- `run_id` or `run_db_id`.

It requires `--execute-staging-query`, accepts only staging/test DSNs and staging schemas, and never writes rows.

## Permanent locks

Both preview and execution evidence keep these boundaries:

- `production_database_written=false`;
- `database_written=false` for production status;
- `database_import_allowed=false`;
- `real_import_execution_allowed=false`;
- `formal_release_allowed=false`;
- `opens_com_ports=false`;
- `writes_sn=false`;
- `writes_device_id=false`;
- `writes_coefficients=false`;
- `controls_pressure=false`;
- `controls_water_or_gas_routes=false`;
- `not_real_acceptance_evidence=true`.

The executor is intentionally not wired into the formal V1.5 full-flow planner. Production import remains represented by the existing blocked executor chain.
