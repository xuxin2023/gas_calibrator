# V1.5 Authoritative Resume State Controlled-Write Preflight

## Purpose

This offline preflight prepares a future separately reviewed atomic state writer. It validates exact source hashes, the current canonical state target, deterministic candidate bytes, and a distinct operator/reviewer/approver authorization packet.

It does not create, replace, snapshot, or roll back `v1_5_full_flow_state.json`.

## Two-Pass Review

1. Create the authorization packet with a stable `authorized_at`, exact upstream paths/hashes, the canonical state target, and expected current state (`absent` or SHA256).
2. Run the preflight to generate `v1_5_resume_state_candidate_preview.json` and its SHA256. This first pass remains `review_required` when the expected candidate hash is absent.
3. Review the candidate, add its exact SHA256 to the authorization packet, and rerun the preflight.
4. Ready status means only that the preflight evidence is complete. It does not allow state writing.

## Required Binding

- canonical full-flow plan path and SHA256
- resume-prefix application review path and SHA256
- #91 writer-design path and SHA256
- #92 blocked-executor evidence path and SHA256, independently recomputed
- canonical `v1_5_full_flow_state.json` target
- current target state recorded as `absent` or an exact SHA256
- deterministic candidate state bytes and exact SHA256
- distinct operator, reviewer, and approver
- fixed structured confirmation template
- no-COM, no-pressure, no-route, no-device-write, and no-database authorization boundary

## Candidate State

The candidate is generated from the canonical plan. It marks only the exact contiguous prefix through `authoritative_resume_state_controlled_write_preflight`, leaves failed steps empty, keeps every authorization false, and sets the next step to `temperature_channel_fast_review`.

The candidate preview is an ordinary review artifact in the preflight output directory. It is not the authoritative state.

## Locked Boundary

- `execution_supported=false`
- `authoritative_state_write_allowed=false`
- `writes_authoritative_state=false`
- `state_file_created=false`
- `state_file_replaced=false`
- `state_snapshot_created=false`
- `rollback_executed=false`
- `opens_com_ports=false`
- `controls_pressure=false`
- `controls_water_or_gas_routes=false`
- `writes_coefficients=false`
- `connects_postgresql=false`
- `formal_release_allowed=false`
- `database_import_allowed=false`

The 0613 V1.5 fitting baseline and 0620/0621 mature CO2/H2O physical-route implementations remain unchanged.
