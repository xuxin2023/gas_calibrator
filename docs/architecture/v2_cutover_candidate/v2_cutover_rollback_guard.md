# V2 Rollback And Fallback Guard

This guard is a worksheet/SOP only. It does not switch the default entry and does not execute rollback.

## Default Entry
- Status: v1_remains_default
- Path: run_app.py
- V1 remains the production default and rollback reference.

## Future Dry-Run Boundary
- real_write_allowed: false
- real_com_open_allowed_in_this_batch: false
- real_acceptance_allowed_in_this_batch: false
- real_primary_latest_refresh_allowed: false
- operator_manual_device_control_allowed: false

## Preserve V1 Baselines
- Current V1 run logs for the production path.
- Current V1 exported summaries, raw rows, and coefficient/writeback references.
- Current V1 runtime configuration and points source used for comparison.
- Known-good V1 sidecar/latest metadata, without rewriting real_primary_latest.

## Rollback-Sensitive Files
- run_app.py
- src/gas_calibrator/workflow/**
- src/gas_calibrator/ui/**
- src/gas_calibrator/devices/**
- any /v1/ or /legacy/ path
- real_primary_latest and any real latest pointer

## Rollback Triggers
- Any V2 dry-run preparation attempts to open real write paths.
- Any coefficient, zero, span, or calibration-parameter write is attempted.
- V2 route trace enters an unexpected route or cleanup cannot restore safe state.
- V2 dry-run preparation produces false completed/progress semantics.
- V1 baseline outputs, logs, or configs are missing before a dry-run window.
- Any default-entry or V1 production path diff appears in review.

## Rollback Steps
- Stop the V2 preparation path and do not retry against real devices.
- Keep run_app.py pointed at the existing V1 default entry.
- Use the preserved V1 configuration, logs, and output baseline as the operating reference.
- Discard or quarantine the failed V2 dry-run artifacts as diagnostic-only evidence.
- Record the trigger, artifact paths, and operator/reviewer notes in the worksheet.
- Resume only the known-good V1 production workflow after V1 baseline verification passes.

## Post-Rollback Verification
- git diff shows no changes to run_app.py or V1 workflow/ui/devices paths.
- V1 baseline config and points source are still present.
- V1 output/log storage path is still writable by the existing production process.
- No real_primary_latest pointer was refreshed by the V2 preparation attempt.
- No instrument zero/span/coefficient/calibration parameter write occurred.

## Prohibited Actions
- Do not switch the default entry to V2.
- Do not close or delete V1 as a fallback path.
- Do not run real compare/real verify from this worksheet.
- Do not open real COM/serial or PLC/valve/instrument/PACE control from this batch.
- Do not refresh real_primary_latest.
