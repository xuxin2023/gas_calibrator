# V1.5 Authoritative Resume Offline State Advance Preflight

This package turns a ready post-execution verifier into a deterministic,
read-only candidate for advancing the authoritative full-flow state by exactly
one completed offline step.

## Contract

- Recompute the post-execution verifier from its executor evidence.
- Require the current state SHA256 to remain exactly equal to the verifier-bound
  SHA256 immediately before any later compare-and-swap writer.
- Require the verified step to be the single next step after the existing exact
  contiguous completed prefix.
- Require all verified output files and hashes to remain unchanged.
- Generate a locked candidate state with no failed steps and all COM, pressure,
  route, device-write, coefficient-write, and database permissions false.
- Do not emit any candidate state when verifier, state, output, plan, or prefix
  evidence has a blocker. The candidate timestamp comes from the hash-bound
  offline executor completion evidence.

## Safety boundary

This preflight does not execute a plan step, write or replace authoritative
state, open COM, control pressure or routes, write analyzer state, connect
PostgreSQL, release a run, or import data. A ready result only supplies the
expected-current and candidate SHA256 values for a separately authorized atomic
writer.

## Verification

Recorded on 2026-07-12:

- Focused state-advance/verifier/executor/consumer/inventory suite: `58 passed, 1 warning`.
- Formal run-status and formal-flow compatibility suite: `103 passed, 1 warning`.
- Mature-route and full-flow compatibility suite: `38 passed`.
- Existing state preflight/atomic-writer/post-write suite: `34 passed`.

The warnings are the existing unregistered `v1_5_formal_gate` pytest marker.
