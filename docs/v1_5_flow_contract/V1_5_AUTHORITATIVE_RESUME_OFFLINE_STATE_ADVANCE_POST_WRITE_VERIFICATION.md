# V1.5 Offline State Advance Post-Write Verification

This package verifies the exact result of the #108 one-step offline resume-state
writer and separately gates read-only consumption of the advanced state.

## Verification contract

- The committed writer JSON, #107 authorization validation, #106 preflight,
  authorization packet, plan, candidate, final state, rollback snapshot, and
  invocation are hash- and path-bound.
- Writer time must fall within the authorization packet validity window.
- The rollback snapshot must equal the pre-write CAS SHA256.
- The final state must be byte-identical to the candidate and the shared writer
  lock must be absent.
- Run, attempt, verified-step, and next-step identities must agree across all
  evidence.

## Consumer readiness

The consumer gate independently recomputes post-write verification, validates
the plan and authoritative-state SHA256 values, and requires an exact contiguous
completed-step prefix. `state_consumption_allowed=true` means only that the
state may be read by a later planner; `resume_execution_allowed` remains false.

Neither tool opens COM, controls pressure/routes, writes state or coefficients,
connects PostgreSQL, releases calibration, or imports data. The mature 0613
fitting and 0620/0621 CO2/H2O paths remain unchanged.

## Verification evidence

- New state-advance post-write/consumer chain plus #108/#107 and legacy
  compatibility: `82 passed`.
- Formal run status and flow contract compatibility: `103 passed`.
- Mature-route and full-flow compatibility: `38 passed`.
- Existing atomic writer/post-write/consumer chain: `29 passed`.
- `py_compile`, Ruff, and `git diff --check` pass.
