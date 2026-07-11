# V1.5 SENCO Authorization Status Rollup Test Evidence

Date: 2026-07-11

## Scope

This package wires the existing `main_senco_artifact_authorization.json` evidence into the canonical V1.5 full-flow plan, formal-run-status rollup, and read-only operation console.

The package does not create authorization automatically. The full-flow plan records unresolved reviewer, approver, authorization ID, writer-scope, and device-ID placeholders and keeps the controlled-write stage blocked until a reviewed precheck pack exists.

## Validation

Focused orchestration, formal-flow contract, formal-run-status, operation-console, authorization, precheck, and final prewrite-gate suite:

```text
139 passed, 1 existing marker warning in 69.79s
```

Controlled SENCO1/3, SENCO2/4, SENCO5, and SENCO6 writer compatibility suite:

```text
43 passed in 83.84s
```

Additional checks:

```text
python -m compileall: pass
git diff --check: pass, line-ending warnings only
```

## Guarded behavior

- Missing authorization blocks controlled-write readiness and formal release, but does not block mature CO2/H2O sampling.
- A changed manifest fails the recorded SHA256 binding and changes the formal status to blocked.
- The status rollup exposes authorization ID, reviewer, approver, writer scopes, and exact authorized analyzer device IDs.
- The operation console remains read-only and presents no SENCO write action.
- The 0613 fitting and 0620/0621 mature physical route files are unchanged.

## Safety boundary

- no COM access
- no coefficient write or clear
- no pressure, valve, gas-route, water-route, chamber, or humidity-generator control
- no PostgreSQL connection or import
- not real acceptance evidence
