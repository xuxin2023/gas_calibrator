# V1.5 database import preflight and evidence-bundle binding

This offline contract freezes the remaining mutable inputs before any future PostgreSQL 18 controlled-import executor.

## Preflight binding

- Manual database-import authorization records the resolved import-preflight JSON path and SHA-256.
- The command contract re-hashes that same preflight and rejects changed content or a replacement path.
- The blocked executor re-hashes the preflight again before accepting the command contract as reviewable.

## Evidence-bundle binding

- The command contract records the resolved formal evidence-bundle JSON path and SHA-256.
- The blocked executor requires the same path and current hash.
- Missing, replaced, or changed evidence bundles hold the database-import chain.
- The controlled-executor design and formal-run status require both binding gates to be ready.

## Safety boundary

- `connects_postgresql=false`
- `database_import_attempted=false`
- `database_written=false`
- `applies_migrations=false`
- `real_import_execution_allowed=false`
- no COM access, analyzer writes, pressure control, or gas/water route control
- no changes to the 0613/0620/0621 mature CO2/H2O execution paths

This is hash-binding and review evidence only. It neither reads a DSN value nor implements a real database executor.

## Validation

```text
154 passed, 1 existing PytestUnknownMarkWarning
```
