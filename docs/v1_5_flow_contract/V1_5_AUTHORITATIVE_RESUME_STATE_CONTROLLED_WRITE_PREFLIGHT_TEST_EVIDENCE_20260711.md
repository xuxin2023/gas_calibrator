# V1.5 Authoritative Resume State Controlled-Write Preflight Test Evidence

## Scope

This package verifies deterministic candidate-state generation, current-state compare-and-swap review, authorization binding, independent upstream recomputation, and the no-state-write boundary.

## Covered Boundaries

- first-pass candidate hash review and second-pass exact authorization
- canonical target limited to `v1_5_full_flow_state.json`
- `absent` or exact existing-state SHA256 comparison
- exact contiguous completed prefix and empty failed-step list
- all real COM, pressure, route, write, release, and database permissions false
- forged #92 lock evidence rejected
- alternate authorization path rejected
- execute/write/replace flags rejected before artifact creation
- candidate preview hash verified by formal-run-status recomputation
- mature 0613/0620/0621 protected paths unchanged

## Test Results

Focused writer-design/blocked-executor/preflight/full-flow/formal-flow/formal-status suite:

```text
156 passed, 1 warning in 108.97s
```

Compatibility resume-prefix/post-closeout/batch/profile/entrypoint/initialization/mature-route/pressure/pre-gas suite:

```text
102 passed, 1 warning in 21.66s
```

Both warnings are the existing unregistered `v1_5_formal_gate` pytest marker. No COM port, analyzer write, pressure/route control, PostgreSQL connection, authoritative-state mutation, release, or database import was used.
