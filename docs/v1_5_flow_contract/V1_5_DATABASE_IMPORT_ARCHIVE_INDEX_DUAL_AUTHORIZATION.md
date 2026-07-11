# V1.5 database import archive-index and dual-authorization contract

This contract closes two remaining gaps before any future PostgreSQL 18 controlled-import executor is implemented.

## Frozen archive index

- Manual database-import authorization records the resolved formal archive closure index path and its SHA-256.
- The command contract must receive that same path and re-hash the current file.
- The blocked executor must re-hash the archive index again before accepting the command contract as reviewable.
- A changed file, replacement path, missing file, missing hash, or malformed hash holds the import chain.
- This whole-index binding complements the existing SENCO authorization/manifest/writer/readback artifact binding; it does not replace it.

## Independent authorization roles

- `operator`, `reviewer`, `approver`, and `authorization_id` remain required.
- `reviewer` and `approver` must be different identities after trimming whitespace and case-normalizing labels.
- A same-person reviewer/approver record is a blocker, not a review warning.
- The command contract freezes the complete authorization JSON path and SHA-256 after validating those identities.
- The blocked executor re-hashes that authorization JSON, so changing either identity after command review causes a hold.

## Safety boundary

- `connects_postgresql=false`
- `database_import_attempted=false`
- `database_written=false`
- `applies_migrations=false`
- `real_import_execution_allowed=false`
- no COM access, device writes, pressure control, or gas/water route control
- no changes to the 0613/0620/0621 mature CO2/H2O execution paths

This contract is offline gate evidence only. It is not production database-import authorization and does not implement a real executor.

## Validation

Focused database-import, formal-status, full-flow, and formal-flow regression on 2026-07-11:

```text
150 passed, 1 existing PytestUnknownMarkWarning
```
