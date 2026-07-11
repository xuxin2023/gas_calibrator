# V1.5 Post-Closeout Resume Gate

## Purpose

The existing V1.5 full-flow state accepts manually supplied `--completed-step` values. Those values describe state, but by themselves they do not prove that the active device batch completed initialization, pressure/SENCO9, and route readiness.

The post-closeout resume gate binds a resumable completed-step prefix to:

- the exact current `v1_5_full_flow_plan.json` SHA256
- the exact current batch initialization closeout SHA256
- the canonical step order after pressure completion
- the mature 0613 fitting and 0620/0621 physical-route boundaries already enforced by the batch closeout index

## Canonical Position

The gate is placed after:

`pressure_channel_completion_audit -> batch_initialization_closeout_index`

and before:

`temperature_channel_fast_review -> co2_open_flow_sampling -> h2o_open_flow_sampling`

When ready, the artifact identifies `temperature_channel_fast_review` as the next stage. CO2 and H2O route execution still require separate explicit route authorization.

## Fail-Closed Rules

The gate is blocked when:

- the batch closeout is missing, review-required, or incomplete for any active device
- the plan is missing a canonical step or uses a noncanonical module
- step order is changed
- the batch closeout is not fail-closed
- the resume gate is not offline/no-write
- `_handoff`, 0624, migration, diagnostic, worker, V1, or V2 surfaces appear in the resume path
- the mature route or fitting baseline differs
- the plan or batch closeout changes after the gate is generated

## Output Contract

The artifact contains:

- plan and batch-closeout paths and SHA256 values
- the evidence-bound completed-step prefix
- a machine-readable `resume_cli_arguments` preview
- the next step ID
- downstream CO2/H2O route step IDs
- remaining authorization requirements

It does not apply those arguments. A later, separately reviewed package is required before the state machine may consume them.

## Safety Boundary

- `does_not_execute_commands=true`
- `applies_completed_steps=false`
- `live_resume_execution_allowed=false`
- `route_authorization_still_required=true`
- no COM access
- no SN/device ID or SENCO write
- no pressure, gas-route, or water-route control
- no PostgreSQL connection or import
- no release authorization

The mature CO2/H2O queues and their point-internal physical behavior are unchanged.
