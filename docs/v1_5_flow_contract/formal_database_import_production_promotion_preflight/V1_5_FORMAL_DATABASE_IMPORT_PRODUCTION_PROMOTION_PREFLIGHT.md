# V1.5 PostgreSQL 18 production promotion preflight

This package adds the offline gate between a successful PostgreSQL 18 staging import and review of a future production controlled executor.

## Required inputs

- committed or exact-idempotent `v1_5_formal_database_import_staging_executor_v1` JSON;
- the exact `v1_5_formal_database_import_transaction_plan_v1` JSON proven by staging;
- the exact `v1_5_evidence_registry` bundle proven by staging.

The transaction plan must still bind byte-identical copies of:

- controlled executor design;
- command contract;
- production import authorization;
- production import preflight;
- formal archive closure;
- evidence bundle.

## Independent checks

The preflight does not trust a `ready` flag by itself. It reopens the bound files and verifies:

- PostgreSQL 18 staging transaction committed or returned an exact idempotent readback;
- staging schemas remain isolated under the V1.5 staging prefixes;
- staging authorization record is complete and reviewer/approver are distinct;
- transaction plan and evidence bundle paths and SHA-256 match staging evidence;
- production authorization, archive release, preflight, command contract, and design semantics remain ready;
- 1-6 SN/device_code identities, protocol aliases, run IDs, and all evidence table counts match staging readback;
- the future production DSN environment name remains exactly `V1_5_POSTGRES_DSN` without reading its value.

Any missing input, path/hash drift, hand-edited ready flag, identity mismatch, table-count mismatch, invalid authorization, non-PostgreSQL-18 staging proof, or production-boundary violation is a blocker.

## Safety boundary

Even when every check passes:

- `production_import_executor_review_allowed=true` only permits review of a separate future executor;
- `production_import_execution_allowed=false`;
- `database_import_allowed=false`;
- `connects_postgresql=false`;
- `production_database_written=false`;
- `formal_release_allowed=false`;
- no COM, analyzer write, pressure control, or gas/water route action is performed;
- `not_real_acceptance_evidence=true`.

This package does not promote staging data, apply migrations, connect to the production database, or refresh formal acceptance.
