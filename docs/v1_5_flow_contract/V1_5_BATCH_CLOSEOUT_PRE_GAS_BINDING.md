# V1.5 Batch Closeout Before Mature Open Flow

## Purpose

This gate binds the completed initialization evidence for the active 1-6 analyzer batch into one offline index before temperature review or mature CO2/H2O open-flow execution.

It closes a specific automation gap: the early `pre_gas_readiness_snapshot` is a contract and gap-list sidecar created before live identity and pressure work. It must not be treated as proof that the current devices have completed initialization.

## Canonical Order

The full-flow order is:

1. formal initialization contract and early readiness sidecars
2. authorized read-only COM identity, SN/device_code, GETCO1-9, and runtime evidence
3. S5/S6/S7/S8 neutralization gate
4. pressure/SENCO9 acquisition, review, controlled completion, and readback evidence
5. `batch_initialization_closeout_index`
6. temperature review
7. mature CO2 and H2O open-flow queues

The closeout step is after `pressure_channel_completion_audit` and before `temperature_channel_fast_review`, `co2_open_flow_sampling`, and `h2o_open_flow_sampling`.

## Required Evidence

The binder requires:

- the actual authorized read-only COM executor artifact for the active batch
- unique 8-digit `sn_code/device_code` and protocol IDs for 1-6 devices
- complete GETCO1-9 and MODE2/1Hz/AVERAGE1/AVERAGE2 runtime evidence
- legacy analyzers with no CHECK command; new-algorithm CHECK evidence only where supported
- neutral S5/S6/S7/S8 readback per device
- per-device pressure/SENCO9 readiness or documented no-write pass
- formal route readiness
- the early pre-gas contract sidecar as supporting context

The physical route baseline remains the 0620/0621 clean-worktree mature route. The fitting baseline remains the 0613 V1.5 fitting path.

## Fail-Closed Behavior

The canonical command includes `--fail-on-review-required`. Missing, pending, inconsistent, or review-required evidence returns a non-zero result and stops the generated full-flow command chain before mature open flow.

`formal_run_status` also consumes the closeout index. A missing or incomplete index sets `can_continue_physical_flow=false`.

## Safety Boundary

The binder:

- opens no COM ports
- writes no SN/device_code or protocol ID
- writes no SENCO coefficient
- controls no pressure, gas route, or water route
- does not connect to PostgreSQL
- does not authorize release or database import

It is an offline evidence gate, not real acceptance evidence and not a replacement for the mature CO2/H2O queue implementations.
