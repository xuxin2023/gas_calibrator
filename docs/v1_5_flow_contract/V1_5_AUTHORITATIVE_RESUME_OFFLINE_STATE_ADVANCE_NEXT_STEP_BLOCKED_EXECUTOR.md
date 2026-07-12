# V1.5 Offline Next-Step Blocked Executor

## Purpose

This package proves that an approved review of one exact next-step plan does
not create execution authority. It freshly revalidates the #113 authorization
preflight, then emits a blocked-executor evidence record.

It does not execute the next step.

## Input Contract

The only operational input is the canonical
`v1_5_authoritative_resume_offline_state_advance_next_step_authorization_preflight.json`.

The blocked executor requires:

- a canonical, non-reparse preflight path;
- ready authorization-preflight schema and status;
- `plan_review_allowed=true`;
- no review reasons;
- a fresh independent recomputation of the complete authorization preflight;
- all execution, COM, pressure, route, write, database, release, and import
  fields to remain false.

## Rejected Interface

The CLI exposes no execute or unlock argument. Argparse rejects, before any
artifact is created:

- `--execute`;
- `--execute-next-step`;
- `--allow-real-com`;
- `--allow-pressure-control`;
- `--allow-route-control`;
- `--allow-writes`;
- `--allow-database-import`.

It accepts no next-step command, COM inventory, route configuration, device
identity, coefficient payload, or PostgreSQL input.

## Output Boundary

A successful lock proof reports:

- `blocked_executor_ready=true`;
- `execution_supported=false`;
- `next_step_execution_allowed=false`;
- `resume_execution_allowed=false`;
- `execute_flag_allowed=false`;
- `would_execute=false`;
- `opens_com_ports=false`;
- `controls_pressure=false`;
- `controls_water_or_gas_routes=false`;
- all state, identity, coefficient, and database writes false;
- formal release and database import false;
- `not_real_acceptance_evidence=true`.

The exporter remains out of the canonical full-flow sequence. Formal status
binds its path and SHA256 to the detected #113 preflight and independently
rebuilds the lock proof.

## Verification Evidence

```text
20 blocked-executor and authorization-preflight tests passed
16 blocked-executor/status integration tests passed after path hardening
64 formal-run-status tests passed
35 entrypoint-inventory tests passed
20 authorization, next-step-plan, and mature-route tests passed
39 formal-flow-contract tests passed
33 full-flow-orchestration tests passed
```

The two warnings are existing unregistered `v1_5_formal_gate` pytest markers.
They are unrelated to this package.
