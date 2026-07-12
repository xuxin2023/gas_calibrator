# V1.5 Next-Step Controlled Execution

## Scope

This package closes the implementation gap after the controlled-executor design. It adds:

1. A short-lived, three-party execution authorization validator.
2. A last-moment preflight that rehashes the complete evidence chain and exact command.
3. A default-locked executor that can launch at most one exact process with `shell=False`.

It does not alter the 0613/0620/0621 mature CO2 or H2O queue, sampling worker, workflow runner, analyzer protocol, default configuration, or `run_app.py`.

## Execution Boundary

- Execution requires the explicit `--execute-next-step` flag.
- Operator, reviewer, and approver must be distinct.
- Authorization lifetime is at most 1800 seconds.
- The packet binds the design, blocked proof, review preflight, next-step plan, consumer readiness, full-flow plan, authoritative state, run, attempt, verified step, mature tool module, and normalized command SHA256.
- Capabilities must exactly match the reviewed step; PostgreSQL import is always false.
- The executor launches one process at most, never uses a shell, never retries, never substitutes another entrypoint, and never automatically advances authoritative state.
- The executor uses the currently reviewed Python executable rather than resolving `python` through `PATH`, and prepends the current repository `src` directory to child `PYTHONPATH`.
- Child-process failure, missing fresh output, evidence drift, expiry, identity conflict, over-broad capability, or command drift produces a hold.
- Executor evidence alone is not real acceptance or release/import authority.

## Verification

No live execution was performed. The execution-success test uses an injected fake subprocess runner.

- New authorization/preflight/executor tests: `15 passed`
- Prior next-step plan/review/blocked/design regression: `31 passed`
- Entrypoint inventory and formal-flow contract regression: `75 passed, 2 warnings`
- Ruff: passed
- Python bytecode compilation: passed

The two warnings are existing unregistered `v1_5_formal_gate` pytest markers.
