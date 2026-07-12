# V1.5 Authoritative Resume Execution Preflight

This package performs a last-moment offline revalidation immediately before a future resume executor. It does not execute the next step.

## Revalidation

- independently rerun the authorization validator at the current UTC time;
- reject authorization that expired after its earlier validation;
- confirm controlled design and authorization packet hashes remain unchanged;
- confirm authoritative state, plan, run ID, canonical next step, and command hash still match;
- preserve the exact least-privilege capability envelope;
- generate a unique attempt ID tied to authorization hash, run, step, command, and evaluation time.

## Fail-Closed Rules

Changed state, plan, command, authorization, permission envelope, or expiry status produces `review_required`. A new authorization and validation chain is required; the preflight does not repair evidence.

## Current Boundary

- `execution_supported=false`
- `resume_execution_allowed=false`
- `would_execute=false`
- command and capability envelope are recorded-only
- no COM, pressure, gas/water route, write, PostgreSQL, release, or import action

The 0613 fitting baseline and 0620/0621 mature CO2/H2O route implementations remain unchanged.
