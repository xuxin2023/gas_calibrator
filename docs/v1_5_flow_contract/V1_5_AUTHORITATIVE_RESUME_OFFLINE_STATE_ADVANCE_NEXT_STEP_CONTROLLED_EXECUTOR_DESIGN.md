# V1.5 Offline Next-Step Controlled Executor Design

## Purpose

This package defines the final implementation contract before a controlled
executor may be built for one exact V1.5 next step. It consumes the #114 lock
proof and records authorization, capability, hold, and output-evidence rules.

It is an offline design. It does not execute the next step.

## Mature-Path Rule

The future executor may invoke only the exact command and mature V1.5 module
already hash-bound by the #111 plan. It may not reconstruct, translate, or
replace CO2/H2O point logic.

For route steps this protects the 0613/0620/0621 implementation, including its
PACE vent behavior, pressure and dewpoint gates, ratio/QC rules, sampling,
storage, and failure evidence.

`automatic_retry_allowed=false` applies to the outer executor. It must not
silently relaunch, switch entrypoints, or continue through another recovery
script. It does not remove a retry policy already owned and recorded by the
exact mature runner command.

## Authorization Contract

A future execution authorization must bind:

- authorization ID and distinct operator, reviewer, and approver identities;
- short UTC issue and expiry times;
- blocked proof, review preflight, plan, consumer, run, attempt, verified-step,
  and current-state SHA256 evidence;
- exact next-step ID, mature module, normalized command SHA256, and runtime
  identity configuration;
- only the minimum physical capabilities required by that exact step.

PostgreSQL import is never a next-step execution capability.

## Failure-Hold Matrix

The exported matrix requires a hold for:

- path, hash, state, plan, or recomputation mismatch;
- authorization expiry, identity conflict, or excess capability;
- next-step ID, module, command, or runtime-config mismatch;
- V1/V2, 0624, migration, diagnostic, worker, or `_handoff` substitution;
- child-process launch or exit failure;
- PACE vent, pressure, dewpoint, ratio, QC, or analyzer-device gate failure;
- missing outputs or output hashes;
- partial side effects or operator abort.

Failure never advances authoritative state and never triggers an outer
automatic retry or fallback entrypoint.

## Required Future Evidence

- `executor_invocation.json`
- `pre_execution_revalidation.json`
- `command_attempts.csv`
- `child_process_result.json`
- `hold_events.csv`
- `post_execution_evidence_index.json`

## Current Locked State

- `production_state=blocked_design_only`
- `single_exact_command_only=true`
- `shell_execution_allowed=false`
- `automatic_retry_allowed=false`
- `fallback_entry_allowed=false`
- `automatic_state_advance_allowed=false`
- `execution_supported=false`
- `next_step_execution_allowed=false`
- no COM, pressure, route, device, coefficient, database, release, or import
  action is performed by this package.

## Verification Evidence

```text
16 controlled-design and blocked-executor tests passed
8 controlled-design/status integration tests passed
64 formal-run-status tests passed
36 entrypoint-inventory tests passed
26 blocked-executor, authorization, and mature-route tests passed
39 formal-flow-contract tests passed
33 full-flow-orchestration tests passed
```

The two warnings are existing unregistered `v1_5_formal_gate` markers and are
unrelated to this package.
