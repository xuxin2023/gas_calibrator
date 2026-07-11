# V1.5 Authoritative Resume State Post-Write Verification

This offline verifier independently checks a completed or no-op #94 state transaction. It binds the atomic-writer evidence to the exact preflight, separate write authorization, deterministic candidate preview, canonical authoritative state, readback SHA256, replacement snapshot, and released writer lock.

It does not call the writer or alter `v1_5_full_flow_state.json`. It is not a new full-flow stage and therefore does not change the #93 candidate prefix or the next `temperature_channel_fast_review` step.

Ready requires:

- writer status is committed or already-current no-op;
- preflight, authorization, candidate, target, and readback hashes agree;
- target bytes exactly equal candidate bytes;
- replacement transactions retain the expected rollback snapshot;
- the single-writer lock is no longer present;
- writer evidence contains no failure or rollback state;
- all COM, device-write, coefficient-write, route, PostgreSQL, release, and import boundaries remain false.

Blocked, rolled-back, rollback-failed, stale, tampered, or incomplete transactions cannot become verified evidence.

The 0613 fitting baseline and 0620/0621 mature CO2/H2O physical routes remain unchanged.
