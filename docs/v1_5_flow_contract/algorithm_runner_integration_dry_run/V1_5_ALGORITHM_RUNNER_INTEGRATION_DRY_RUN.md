# V1.5 algorithm runner integration dry-run

This is an offline runner integration plan for the new-algorithm 47/14 runlist preview.

- overall_status: `ready_for_runner_integration_dry_run_review`
- blocker_count: `0`
- CO2/H2O runlist counts: `47` / `14`
- Planned route order: `co2 -> h2o`.
- Commands are preview strings only and include `--dry-run --no-prompt`.
- This sidecar does not execute commands, open COM ports, control routes, write coefficients, release archives, or import databases.

## Checks

- `algorithm_runlist_readiness_gate`: `ready`
- `co2_queue_runner_dry_run_plan`: `ready`
- `h2o_queue_runner_dry_run_plan`: `ready`
- `integration_dry_run_boundary`: `ready`
