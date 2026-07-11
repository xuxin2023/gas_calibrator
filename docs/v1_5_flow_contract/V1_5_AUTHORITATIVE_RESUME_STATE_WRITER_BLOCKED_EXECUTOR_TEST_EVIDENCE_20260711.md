# V1.5 Authoritative Resume State Writer Blocked Executor Test Evidence

## Scope

This package verifies the blocked-by-default state-writer stub without writing state or touching hardware.

## Covered Boundaries

- exact plan/application/design path binding
- plan/application/design SHA256 verification
- independent #91 design recomputation
- rejection of forged completed-step content
- rejection of same-hash alternate design paths
- rejection of execute, target, hash, authorization, COM, route, write, and database flags before artifact creation
- no authoritative state file creation, replacement, snapshot, or rollback
- formal-status independent lock-evidence recomputation
- full-flow and formal-flow adjacency before temperature review
- mature 0613/0620/0621 protected paths unchanged

## Test Results

Focused resume/full-flow/formal-status suite:

```text
162 passed, 1 warning in 227.33s
```

Compatibility suite covering algorithm profiles, entrypoint inventory,
initialization readiness, mature-route contract, pressure completion, and
pre-gas readiness:

```text
84 passed, 1 warning in 64.45s
```

Both warnings are the existing unregistered `v1_5_formal_gate` pytest marker.
No COM port, device write, PostgreSQL connection, pressure control, or gas/water
route action was used by either suite.
