# V1.5 PostgreSQL 18 production controlled executor

## Purpose

`run_v1_5_formal_database_import_production_controlled_executor.py` is the first
V1.5 executor that can perform the final production evidence import. It is a
manual, separately authorized database operation. It is not part of the CO2 or
H2O route runner and does not grant calibration release.

The target is fixed:

- backend: `postgresql`
- PostgreSQL major: `18`
- DSN environment variable: `V1_5_POSTGRES_DSN`
- database: `gas_calibrator`
- core schema: `public`
- evidence schema: `v1_5_evidence`

The command does not accept target overrides and never creates databases,
schemas, tables, or migrations. Migration
`002_v1_5_production_import_ledger` must already be applied by the existing DBA
migration path before an import can start. The importer requires the confirmed
migration controlled-executor artifact and binds that artifact's SHA256 into the
production-import authorization packet.

## Default locked preview

Without `--execute-production-import`, the command revalidates the promotion
preflight, transaction plan, evidence bundle, staging readback, identities, and
table counts. It does not read the DSN environment variable and does not connect
PostgreSQL.

```text
python -m gas_calibrator.tools.run_v1_5_formal_database_import_production_controlled_executor \
  --promotion-preflight-json <promotion-preflight.json> \
  --transaction-plan-json <transaction-plan.json> \
  --evidence-bundle-json <evidence-bundle.json> \
  --migration-execution-json <migration-execution.json> \
  --output-dir <review-output>
```

## Explicit execution boundary

Real execution additionally requires:

1. `--execute-production-import`;
2. a current execution authorization packet with a lifetime no longer than 24 hours;
3. three distinct actors: operator, reviewer, and approver;
4. structured confirmation template `v1_5_postgresql18_production_import_reviewed_v1`;
5. a confirmed migration 002 execution artifact for the fixed PostgreSQL 18 target, including exact migration checksums, ledger schema readback, and its own three-party authorization record;
6. exact path and SHA256 bindings for the promotion preflight, transaction plan, evidence bundle, and migration execution artifact;
7. exact fixed production target and explicit no-COM/no-SENCO/no-route/no-migration boundaries.

Only after those checks pass may the CLI read `V1_5_POSTGRES_DSN`. The executor
then revalidates the package once more before starting the transaction.

## Transaction behavior

- verifies PostgreSQL 18 and the pre-applied production schemas/tables;
- locks `v1_5_evidence.production_import_ledger` for the run;
- rejects an existing evidence run without a matching ledger;
- atomically writes core identity aliases and all evidence-bundle tables;
- reads back every evidence table count and every SN/device_code/protocol-ID alias before commit;
- records plan, bundle, promotion, and execution-authorization hashes in the ledger;
- returns an idempotent no-op only when all four hashes and readback state match;
- rolls back on any failure before a confirmed commit;
- reports `production_import_commit_uncertain_hold` if commit outcome cannot be proven.

## Hard exclusions

This executor never:

- opens COM ports;
- reads or writes analyzer SN/device ID;
- writes SENCO coefficients;
- controls pressure, gas routes, water routes, chamber temperature, or humidity;
- changes the 0613/0620/0621 mature calibration path;
- applies database migrations;
- sets `formal_release_allowed=true`.

## Current verification scope

This implementation was verified without reading `V1_5_POSTGRES_DSN` and
without connecting or writing the production database. Tests cover locked
preview, 1-6 device packages, immutable promotion rebuild, expired/rebound
authorization, three-party identity, fixed production target, no-migration
source guard, confirmed migration-002 evidence, migration authorization/source
integrity, and exact hashes passed into the previously tested atomic import
kernel. A later separately authorized production import remains an operational
action, not a result of this code review.
