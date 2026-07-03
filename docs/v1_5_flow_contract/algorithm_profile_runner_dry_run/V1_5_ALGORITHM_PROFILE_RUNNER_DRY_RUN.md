# V1.5 algorithm profile runner dry-run

This is an offline profile-driven bundle for the new-algorithm 47/14 formal runlist path.

- overall_status: `ready_for_profile_driven_runner_dry_run_review`
- blocker_count: `0`
- profile: `absorption_ratio_shadow`
- CO2/H2O runlist counts: `47` / `14`
- runner_integration_status: `profile_driven_dry_run_bundle_only_not_runner_wired`
- This bundle generates runlist preview, runlist readiness, and runner integration dry-run artifacts.
- It does not execute commands, open COM ports, connect PostgreSQL, control routes, write SN/device IDs, write coefficients, release archives, import databases, or modify mature runners.

## Output directories

- `runlist_preview`: `D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean\docs\v1_5_flow_contract\algorithm_profile_runner_dry_run\algorithm_formal_runlist_preview`
- `runlist_readiness`: `D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean\docs\v1_5_flow_contract\algorithm_profile_runner_dry_run\algorithm_runlist_readiness`
- `runner_integration_dry_run`: `D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean\docs\v1_5_flow_contract\algorithm_profile_runner_dry_run\algorithm_runner_integration_dry_run`

## Checks

- `formal_runlist_preview_generation`: `ready`
- `runlist_readiness_gate`: `ready`
- `runner_integration_dry_run_plan`: `ready`
- `profile_runner_dry_run_offline_boundary`: `ready`
