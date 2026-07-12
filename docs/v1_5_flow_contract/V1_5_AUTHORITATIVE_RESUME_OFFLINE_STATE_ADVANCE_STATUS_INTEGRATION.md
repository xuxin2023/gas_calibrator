# V1.5 Offline State Advance Status Integration

This package connects the #109 offline state-advance post-write verifier and
consumer-readiness gate to the canonical V1.5 full-flow plan and formal status
rollup.

## Canonical order

The canonical `completed_step_ids` sequence remains:

1. `temperature_channel_fast_review`
2. `co2_open_flow_sampling`

The manually authorized #108 writer advances the state once from the completed
temperature review to CO2. Post-write verification and consumer readiness are
out-of-band evidence gates, not additional state steps. Treating them as steps
would make verification of a state write require another state write.

The formal-status snapshot consumes their explicit artifact paths before any
physical continuation decision. The post-write verifier consumes evidence from
the separately and manually authorized writer; the full-flow plan does not call
that writer or either sidecar as a canonical step. The consumer gate may set
`state_consumption_allowed=true` only for read-only planning;
`resume_execution_allowed` remains false.

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
- Formal-run-status, #109 post-write chain, and integration tests: `80 passed`.
- Focused real post-write/consumer chain and out-of-band integration tests: `16 passed`.
- `py_compile`, Ruff, and `git diff --check` pass.
