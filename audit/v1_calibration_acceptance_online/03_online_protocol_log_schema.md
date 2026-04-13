# Online Protocol Log Schema

- generated_at: 2026-04-13T14:58:16+08:00
- head: `367a1089ebaca1388dbb9d11648f74513316e502`

## JSONL Fields

- `ts`
- `stage`
- `action`
- `raw_command`
- `raw_response`
- `error`

## Required Run Summary Fields

- `run_id`
- `session_id`
- `device_id`
- `start_ts`
- `end_ts`
- `mode_before`
- `mode_after`
- `mode_exit_attempted`
- `mode_exit_confirmed`
- `coeff_before`
- `coeff_target`
- `coeff_readback`
- `rollback_attempted`
- `rollback_confirmed`
- `write_status`
- `verify_status`
- `rollback_status`
- `unsafe`
- `failure_reason`

## Notes

- `raw_command` and `raw_response` must be preserved for each high-level protocol action.
- `mode_exit_attempted` and `mode_exit_confirmed` must remain distinct.
- `rollback_attempted` and `rollback_confirmed` must remain distinct.
- If final mode cannot be confirmed, the run must be marked `unsafe=true`.
