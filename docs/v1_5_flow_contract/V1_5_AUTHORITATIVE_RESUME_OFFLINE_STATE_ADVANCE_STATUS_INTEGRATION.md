# V1.5 Offline State Advance Status Integration

This package connects the #109 offline state-advance post-write verifier and
consumer-readiness gate to the canonical V1.5 full-flow plan and formal status
rollup.

## Canonical order

The first resumed offline step after batch closeout is the temperature review.
Before the mature CO2 route may resume, the plan now requires:

1. `temperature_channel_fast_review`
2. `authoritative_resume_offline_state_advance_post_write_verification`
3. `authoritative_resume_offline_state_advance_consumer_readiness`
4. `co2_open_flow_sampling`

The post-write verifier consumes evidence from the separately and manually
authorized #108 atomic writer. The full-flow plan does not call that writer.
The consumer gate may set `state_consumption_allowed=true` only for read-only
planning; `resume_execution_allowed` remains false.

## Formal status behavior

- A detected manual atomic writer without valid post-write verification blocks
  physical continuation.
- Missing, stale, tampered, or non-read-only consumer evidence blocks physical
  continuation.
- Ready verifier and consumer gates do not authorize CO2, pressure, route,
  device, coefficient, database, release, or import actions.
- The existing CO2 route authorization remains the only gate that can permit
  the physical CO2 stage after all prior evidence is ready.

## Safety boundary

This integration does not open COM, control pressure, control gas or water
routes, write authoritative state, write analyzer identity or coefficients,
connect PostgreSQL, release a run, or import data. It does not modify the mature
0613 fitting or 0620/0621 CO2/H2O implementations.

## Verification

Recorded on 2026-07-12:

- Full-flow, formal-flow contract, and integration tests: `76 passed, 1 existing marker warning`.
- Formal-run-status and integration tests: `68 passed`.
- #109 post-write chain, integration, and mature-route contract tests: `20 passed`.
- `py_compile`, Ruff, and `git diff --check` pass.
