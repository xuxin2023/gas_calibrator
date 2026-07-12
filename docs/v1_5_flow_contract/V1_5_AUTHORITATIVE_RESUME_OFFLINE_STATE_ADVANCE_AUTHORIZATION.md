# V1.5 Authoritative Resume Offline State Advance Authorization

This package validates a short-lived, least-privilege authorization packet for
one future authoritative-state compare-and-swap after a verified offline step.
It also provides a blocked executor proving that no state-write implementation
is available in this package.

## Required binding

- Exact exported #106 preflight path and SHA256.
- Exact canonical candidate preview path and SHA256.
- Exact authoritative-state path and expected-current SHA256.
- Exact run, attempt, verified step, and next-step identity.
- Distinct operator, reviewer, and approver identities.
- Maximum 30-minute authorization TTL and structured confirmation of atomic
  replace, readback, rollback, and all no-COM/no-route/no-device/no-database
  boundaries.
- Preflight, candidate preview, authorization packet, and authoritative-state
  targets must not use symlink/reparse-point files or parents.

## Safety boundary

The authorization validator does not execute a state write. The blocked
executor rejects generic execute/write/replace arguments because those options
do not exist. Both outputs retain `state_write_execution_allowed=false`,
`writes_authoritative_state=false`, and all physical/database permissions false.

## Verification

- Focused authorization/preflight/atomic-writer/inventory suite: `61 passed`.
- Formal status and flow-contract compatibility: `103 passed`.
- Mature-route and full-flow compatibility: `38 passed`.
- Adjacent controlled-write/atomic-write/post-write suite: `34 passed`.
