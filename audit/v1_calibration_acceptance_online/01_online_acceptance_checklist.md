# Online Acceptance Checklist

- generated_at: 2026-04-13T12:47:29+08:00
- head: `f41b7b20c35a5051943fecd35bdaf62c05ae8d34`
- dual_gate_required: CLI `--real-device` + ENV `ALLOW_REAL_DEVICE_WRITE=1`
- capability_boundary: Current HEAD V1 only supports the CO2 main chain; H2O zero/span is NOT_SUPPORTED.

## Startup Checks

- Confirm this tool is running on the intended fixed HEAD before any real-device action.
- Confirm V1 H2O zero/span is NOT_SUPPORTED and do not request H2O groups.
- Confirm default runtime safety remains intact: postrun real write stays disabled unless explicitly opted in elsewhere.
- Confirm the selected analyzer/device ID matches the bench device you intend to observe.
- Confirm the planned coefficient groups are CO2-only (`1`, `3`) and no H2O group is requested.

## Dual-Gate Confirmation

- Gate 1: pass CLI flag `--real-device` only for an intentional bench session.
- Gate 2: set environment variable `ALLOW_REAL_DEVICE_WRITE=1` in the same shell/session.
- If either gate is missing, this tool must stay in dry-run mode and must not instantiate or write any device.

## Real-Device Steps

- Run one dry-run first and confirm only templates/checklists are produced.
- For an authorized bench session, capture baseline mode snapshot before any write.
- Enter calibration mode (`MODE=2`).
- Write the planned CO2 coefficient group(s).
- Perform immediate `GETCO` readback and compare against target values.
- If readback mismatches or any protocol step fails, confirm rollback was attempted and review rollback readback.
- Confirm final mode-restore attempt back to normal mode and collect final mode snapshot.

## Manual Observations

- Record visible bench symptoms during mode switch, write, readback, rollback, and exit.
- Record whether the analyzer front panel / service UI / serial trace indicates normal mode after the run.
- Record any protocol noise, delayed ACK, empty readback, parse anomaly, or operator intervention.

## Abort Conditions

- Abort immediately if the selected target is not a CO2-only group or if any H2O zero/span request appears.
- Abort immediately if either real-device gate is missing or ambiguous.
- Abort immediately if the baseline mode cannot be read, if calibration mode cannot be entered, or if final mode cannot be confirmed after restore attempt.
- Abort immediately if rollback is attempted but cannot be confirmed.

## When The Result Must Stay ONLINE_EVIDENCE_REQUIRED

- No real-device `online_run_*.json` and `online_protocol_*.jsonl` have been captured yet.
- A run stayed dry-run only, even if all offline checks passed.
- Final mode exit was attempted but not confirmed (`mode_exit_confirmed=false`).
- Rollback was attempted but not confirmed (`rollback_confirmed=false`).
- The run ended with `unsafe=true`, `FAILED`, or any missing raw protocol evidence.

## Reminder

- Offline evidence does not replace real acceptance evidence; online abnormal-recovery proof remains required until real logs are captured and reviewed.
