# V1.5 Authoritative Resume Offline Candidate Gate

This package classifies whether the canonical next step may be considered by a future offline-only resume executor. It does not execute the step.

## Accepted Candidate

- execution preflight is valid and no more than 60 seconds old;
- authorization remains valid at classification time;
- canonical plan step exactly matches the preflight step and command;
- execution mode is explicitly offline;
- the step does not open COM, control pressure or routes, write device identity or coefficients, or import PostgreSQL;
- command paths do not reference `_handoff`, 0624, V2, or diagnostic surfaces.

## Rejected Candidate

Read-only COM, pressure, CO2/H2O route, device/coefficient write, authoritative-state writer, and database-import steps remain assigned to their dedicated controlled executors. Resume authorization cannot turn them into generic offline commands.

## Current Boundary

- `execution_supported=false`
- `resume_execution_allowed=false`
- `would_execute=false`
- no subprocess or command execution
- no COM, pressure, route, write, PostgreSQL, release, or import action

The 0613 fitting baseline and 0620/0621 mature CO2/H2O route implementations remain unchanged.
