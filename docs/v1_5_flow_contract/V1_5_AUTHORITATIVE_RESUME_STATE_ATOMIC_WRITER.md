# V1.5 Authoritative Resume State Atomic Writer

## Purpose

This manual-authorized tool is the only entrypoint in this package that can create or replace the canonical `v1_5_full_flow_state.json`. It consumes the exact ready #93 controlled-write preflight and writes only the deterministic candidate preview bound by that preflight.

It does not open COM ports, read or write analyzers, control pressure or gas/water routes, connect PostgreSQL, authorize formal release, or import calibration data.

## Explicit Authorization

The command is rejected before artifact creation unless all of the following are present:

- `--execute-controlled-state-write`
- the fixed confirmation template `v1_5_authoritative_resume_state_atomic_write_v1`
- a valid authorization ID
- the exact canonical ready preflight
- a separate atomic-write authorization packet

The #93 preflight authorization packet is deliberately `preflight_only=true` and cannot authorize this write. The atomic writer requires its own packet with schema `v1_5_authoritative_resume_state_atomic_write_authorization_v1`, three distinct operator/reviewer/approver identities, exact preflight and target bindings, and the approved current-state and candidate SHA256 values.

## Atomic Transaction

After independently recomputing the preflight and validating the separate writer authorization, the writer:

1. acquires an exclusive same-directory lock;
2. re-reads the authoritative target SHA256 while holding the lock;
3. re-reads and hashes the candidate preview while holding the lock;
4. returns a locked no-op when the target already equals the candidate;
5. snapshots existing target bytes when replacement is required;
6. writes a same-directory temporary file, flushes it, and calls `fsync`;
7. atomically replaces the target with `os.replace`;
8. reads back and verifies the exact candidate SHA256;
9. restores the previous bytes, or removes a newly created target, if readback fails;
10. removes temporary and lock files before returning.

No stale-state, changed-candidate, lock-conflict, malformed-authorization, or readback-failure condition is guessed through. Each condition blocks or rolls back and is recorded in the writer evidence.

## Evidence

An explicitly requested transaction writes:

- `v1_5_resume_state_atomic_write_invocation.json`
- `v1_5_resume_state_atomic_write.json`
- `v1_5_resume_state_atomic_write_summary.csv`
- `V1_5_RESUME_STATE_ATOMIC_WRITE.md`

Successful tests create and replace state files only inside pytest temporary directories. They do not write the repository's real authoritative state and are not real acceptance evidence.

## Protected Boundary

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

The 0613 V1.5 fitting baseline and 0620/0621 mature CO2/H2O physical-route implementations remain unchanged.
