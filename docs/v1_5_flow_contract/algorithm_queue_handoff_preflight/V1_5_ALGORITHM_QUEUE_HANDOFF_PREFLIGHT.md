# V1.5 algorithm queue handoff preflight

This offline guard prevents profile-generated 47/14 runlists from being mistaken for authorized live queue execution.

- overall_status: `ready_for_dry_run_queue_handoff_review`
- blocker_count: `0`
- profile: `absorption_ratio_shadow`
- CO2/H2O runlist counts: `47` / `14`
- dry_run_handoff_review_allowed: `True`
- live_queue_execution_allowed: `False`
- Required pre-live-review flags: `--dry-run --no-prompt`.
- This guard does not execute queues, open COM ports, connect PostgreSQL, control gas/water routes, write SN/device IDs, write coefficients, release archives, or import databases.

## Checks

- `profile_runner_dry_run_bundle_gate`: `ready`
- `runner_integration_dry_run_gate`: `ready`
- `co2_dry_run_no_prompt_handoff_gate`: `ready`
- `h2o_dry_run_no_prompt_handoff_gate`: `ready`
- `live_queue_execution_lock`: `ready`
