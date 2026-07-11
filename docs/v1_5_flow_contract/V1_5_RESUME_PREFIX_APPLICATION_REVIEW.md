# V1.5 Resume Prefix Application Review

Date: 2026-07-11

## Purpose

This package adds the offline `post_closeout_resume_prefix_application_review` stage after the hash-bound post-closeout resume gate and before temperature review.

It consumes the resume-gate artifact for validation only. It does not write the authoritative full-flow state and does not execute the next stage.

## Required Bindings

- the current canonical V1.5 full-flow plan path and SHA256
- the exact post-closeout resume-gate path and SHA256 declared by that plan
- the batch initialization closeout path and SHA256 already bound by the resume gate
- the exact contiguous plan prefix through `post_closeout_resume_gate_snapshot`
- the resume gate, application review, and temperature review are adjacent with no skipped step
- the exact flattened `--completed-step` preview generated from that prefix
- the next reviewed stage remains `temperature_channel_fast_review`

Any changed plan, changed closeout artifact, changed resume gate, alternate gate path, missing step, noncontiguous prefix, or extra completed step blocks the review.

The formal-run-status rollup independently reloads the bound plan, resume gate, and batch closeout. It recomputes both reviewed step lists and both CLI argument lists instead of trusting a sidecar that merely reports `ready=true`.

## Forbidden Inputs

The application-review step must reject state or execution flags, including:

- `--completed-step`
- `--failed-step`
- `--execute`
- `--execute-offline-commands`
- `--supervised-run-ready-offline`
- all real-COM, pressure, route, write, and database authorization flags

## Physical Boundary

- `applies_completed_steps=false`
- `writes_authoritative_state=false`
- `would_execute=false`
- `live_resume_execution_allowed=false`
- `opens_com_ports=false`
- `controls_pressure=false`
- `controls_water_or_gas_routes=false`
- `writes_sn=false`
- `writes_device_id=false`
- `writes_coefficients=false`
- `connects_postgresql=false`
- `database_written=false`
- `formal_release_allowed=false`
- `database_import_allowed=false`
- `not_real_acceptance_evidence=true`

The 0613 fitting baseline and 0620/0621 mature CO2/H2O physical-route implementations remain unchanged.

## Next Boundary

A later, separately reviewed package may write an authoritative resumable state from this review. That later package must still keep route execution behind explicit authorization and must not infer completion from arbitrary user-supplied `--completed-step` values.
