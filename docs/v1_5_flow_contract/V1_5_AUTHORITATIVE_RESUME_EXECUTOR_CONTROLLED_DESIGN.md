# V1.5 Authoritative Resume Executor Controlled Design

This package defines the authorization and least-privilege boundary for a future resume executor. It is not an executor and cannot resume a run.

## Evidence Binding

The future authorization must bind all of the following:

- immutable authorization ID;
- operator, reviewer, and distinct approver;
- UTC issue and expiry timestamps;
- run ID, full-flow plan SHA256, authoritative-state SHA256, consumer-contract SHA256, and blocked-executor SHA256;
- exact canonical `next_step_id` and normalized command SHA256;
- structured confirmation that no unrelated physical or database authority is granted.

## Least Privilege

Real COM, pressure control, route control, and device/coefficient writes default to false. A capability may be authorized only when the verified canonical next step requires it. PostgreSQL import is never granted by resume authorization and remains a separate controlled stage.

## Failure Holds

Missing, expired, self-approved, mismatched, tampered, or over-broad authorization must hold before execution. A future runtime failure must stop, preserve evidence, and must not silently advance authoritative state.

## Current Locks

- `production_state=blocked_design_only`
- `execution_supported=false`
- `resume_execution_allowed=false`
- `execute_flag_allowed=false`
- no COM, pressure, route, write, PostgreSQL, release, or import action

The 0613 fitting baseline and 0620/0621 mature CO2/H2O route implementations remain unchanged.
