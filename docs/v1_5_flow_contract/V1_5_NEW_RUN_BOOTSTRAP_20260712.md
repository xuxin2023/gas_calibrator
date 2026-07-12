# V1.5 New-Run Bootstrap

## Purpose

This bootstrap creates the first authoritative plan and state for a new V1.5 batch. It reuses the existing `build_full_flow_plan` and `build_full_flow_state` APIs rather than introducing another calibration planner.

## Contract

- The run id must be explicit and unique under the selected runs root.
- Operator, reviewer, and approver must be present and distinct.
- The source configuration must be a readable JSON object and must not traverse a reparse point.
- The configuration bytes are copied into `bootstrap_input/config_snapshot.json`; the generated plan consumes the snapshot path, not the mutable source path.
- Source and snapshot SHA256 values must still match immediately before atomic publish.
- An exclusive same-parent bootstrap lock prevents two operator windows from creating the same run id concurrently.
- The run is assembled in a same-parent temporary directory and atomically renamed only after all checks pass; temporary files and the lock are removed on failure.
- Existing run directories are never overwritten or reused.
- The initial state always has an empty completed/failed prefix and starts at the first canonical step.
- Real COM, pressure, route, device/coefficient write, PostgreSQL import, release, and database permissions are all false.
- The bootstrap CLI exposes no execute, completed-step, failed-step, or allow-physical-capability flags.
- CO2 and H2O steps continue to reference the mature V1.5 formal queue modules.

## Current Bench Status

Only Windows COM1-COM7 were visible during implementation; no analyzer ports or active authoritative batch were present. Therefore this package was verified offline and did not open any serial port.

## Verification

- New-run bootstrap and full-flow orchestration: `44 passed`
- Entrypoint inventory and formal-flow contract: `75 passed, 2 existing marker warnings`
- Ruff, Python bytecode compilation, and diff checks: passed
