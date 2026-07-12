# V1.5 Next-Step Operator Bundle

## Purpose

The operator bundle removes the need to hand-author the execution-authorization JSON introduced by PR #116. It consumes one reviewed controlled-executor design and creates a fresh, single-use evidence directory containing:

1. Exact execution authorization packet.
2. Authorization validation.
3. Last-moment execution preflight.
4. Default-locked controlled-executor evidence.

## Safety Boundary

- Operator, reviewer, and approver are required and must be distinct.
- Authorization TTL defaults to 900 seconds and cannot validate above 1800 seconds.
- Evidence paths, SHA256 values, command hash, run, attempt, verified step, next step, and mature module are derived from the reviewed plan rather than typed by the operator.
- Real COM, pressure, route, and write capabilities are derived from the exact plan; they cannot be selected independently on the CLI.
- PostgreSQL import is always false.
- The output directory must be absent or empty, preventing stale authorization evidence from being overwritten or mixed.
- The default invocation never starts a process.
- Execution still requires `--execute-next-step`, the exact attempt id, and the #116 operator confirmation text.
- At most one shell-free child process may start. There is no executor retry, fallback entry, or automatic authoritative-state advance.
- This launcher remains outside the canonical full-flow steps.

## Current Evidence Status

No live authoritative state was present when this package was built. The most recent local physical evidence was a completed historical closeout, so no COM, pressure, route, write, or database operation was attempted.

## Verification

- Operator bundle and #116 execution chain: `23 passed`
- Entrypoint inventory and formal-flow contract: `75 passed, 2 existing marker warnings`
- Ruff and Python bytecode compilation: passed
- Mature CO2/H2O queues and protected core files: unchanged
