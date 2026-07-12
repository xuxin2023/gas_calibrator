# V1.5 Authoritative Resume Offline Post-Execution Verifier

This package verifies one completed canonical offline resume step after the
manual-authorized offline executor returns successfully. It is a read-only
evidence verifier, not a state writer and not a physical-flow executor.

## Required bindings

- The executor evidence must report the executed status, zero return code,
  fresh expected outputs, no hold reasons, and no authoritative-state advance.
- The candidate gate SHA256, schema, attempt ID, next step, plan SHA256, exact
  runtime command, and expected-output path set must remain canonical.
- Every output SHA256 must equal the executor-recorded after-execution SHA256
  and differ from its before-execution SHA256.
- The authorization packet and authoritative resume-state SHA256 must remain
  unchanged throughout the offline execution and post-execution verification.

## Safety boundary

The verifier does not execute a command, open COM, control pressure or routes,
write analyzer coefficients, connect PostgreSQL, release a run, import data, or
advance the authoritative resume state. A ready result only permits design of a
separate compare-and-swap state-advance preflight bound to this exact evidence.

## Verification

Focused tests cover the ready path, output mutation, state mutation, gate
replacement, executor boundary mutation, offline entrypoint classification,
and rejection of an `--execute` argument before any artifact is written.

Recorded on 2026-07-12:

- Focused verifier/executor/gate/preflight/inventory suite: `61 passed, 1 warning`.
- Formal run-status and formal-flow compatibility suite: `103 passed, 1 warning`.
- Mature-route and full-flow compatibility suite: `38 passed`.

The warnings are the existing unregistered `v1_5_formal_gate` pytest marker;
they do not represent a verifier failure.
