# V1.5 Resume Executor Authorization Validator

This package validates a future resume authorization packet offline. A ready result is not execution permission.

## Required Binding

- controlled design, blocked executor, consumer contract, full-flow plan, and authoritative-state paths plus SHA256;
- exact run ID, canonical next-step ID, and normalized command SHA256;
- immutable authorization ID;
- operator, reviewer, and a distinct approver;
- timezone-aware issue and expiry timestamps with a maximum 30-minute lifetime;
- structured resume-only, no-implicit-write, no-database-import, and no-unrelated-permission confirmation;
- exact least-privilege capability flags required by the canonical next step.

## Fail-Closed Rules

Missing, expired, future-dated, overlong, self-approved, path-mismatched, hash-mismatched, step-mismatched, command-mismatched, or over-broad authorization is `review_required`.

PostgreSQL import must always remain false. Resume authorization cannot replace database import authorization.

## Current Boundary

- `execution_supported=false`
- `resume_execution_allowed=false`
- `would_execute=false`
- no COM, pressure, route, device/coefficient write, PostgreSQL, release, or import action

The validator does not modify the 0613 fitting path or the 0620/0621 mature CO2/H2O route implementations.
