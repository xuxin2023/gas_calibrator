# V1.5 Authoritative Resume State Writer Design

## Purpose

This offline review defines the contract for a future writer of `v1_5_full_flow_state.json`. It consumes the exact, hash-bound resume-prefix application review and independently derives the completed prefix through `authoritative_resume_state_writer_design`.

The package does not write or replace the authoritative state. It does not execute the next stage.

## Required Sequence

1. `batch_initialization_closeout_index`
2. `post_closeout_resume_gate_snapshot`
3. `post_closeout_resume_prefix_application_review`
4. `authoritative_resume_state_writer_design`
5. `authoritative_resume_state_writer_blocked_executor`
6. `authoritative_resume_state_controlled_write_preflight`
7. `temperature_channel_fast_review`

The first four stages must form one exact contiguous prefix before the design can hand off to the blocked executor. The blocked executor and controlled-write preflight must remain adjacent before temperature review.

## Source Binding

The design binds and rechecks:

- canonical full-flow plan path and SHA256
- resume-prefix application review path and SHA256
- post-closeout resume gate path and SHA256
- batch initialization closeout path and SHA256
- run ID
- exact completed-step list and flattened CLI representation
- independently recomputed application-review source chain from the canonical plan and resume gate

Same-content copies at alternate paths are not interchangeable with the paths declared by the full-flow plan.

## Future Transaction Contract

A future real state writer must require all of the following:

- single-writer lock
- existing-state snapshot
- existing-state SHA256 compare-and-swap
- temporary file in the same directory
- temporary-file flush and fsync
- atomic replacement
- post-replacement readback and SHA256 verification
- rollback snapshot
- rollback on readback mismatch
- exact run-ID match
- exact contiguous completed prefix
- rejection of symlink or reparse-point targets

These are design requirements only. This package does not implement the transaction.

## Locked Boundary

- `design_review_only=true`
- `execution_supported=false`
- `authoritative_state_write_allowed=false`
- `writes_authoritative_state=false`
- `opens_com_ports=false`
- `controls_pressure=false`
- `controls_water_or_gas_routes=false`
- `writes_coefficients=false`
- `connects_postgresql=false`
- `formal_release_allowed=false`
- `database_import_allowed=false`

The 0613 V1.5 fitting baseline and 0620/0621 mature CO2/H2O physical-route implementations remain unchanged.
