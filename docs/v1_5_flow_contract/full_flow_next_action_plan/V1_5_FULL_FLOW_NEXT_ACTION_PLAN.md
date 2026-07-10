# V1.5 Full-Flow Next-Action Plan

- schema: `v1_5_full_flow_next_action_plan_v1`
- overall_status: `review_ready`
- recommended_next_action_id: `batch_initialization_closeout_pre_gas_evidence_index`
- completed_action_ids: `0`
- remaining_next_action_count: `7`
- mature_fitting_baseline: `0613 V1.5 fitting path`
- mature_route_baseline: `0620/0621 clean-worktree mature physical route path`
- full_production_auto_allowed: `false`

## Meaning

This plan ranks the remaining V1.5 automation handoffs. It does not execute the handoffs.

## Next Actions

| priority | status | action_id | action_type | recommended_pr_scope | done_when |
|---:|---|---|---|---|---|
| 1 | `recommended` | `batch_initialization_closeout_pre_gas_evidence_index` | `offline_evidence_binder` | Bind per-batch SN/device_code, protocol ID, GETCO1-9, runtime, S5-S8 neutralization, S9 pressure readiness, and read-only COM closeout into one pre-gas evidence index. | A generated pre-gas evidence index can prove which batch is ready for mature CO2/H2O routes without reopening COM or writing any coefficient. |
| 2 | `pending` | `pressure_s9_exception_and_reverify_evidence_index` | `offline_evidence_binder` | Normalize offset-only S9 and explicit linear-S9 exception evidence into pressure readiness without changing the mature route runners. | S9 write/readback/reverify evidence is explicit per device and cannot be confused with route data. |
| 3 | `pending` | `route_physical_recovery_live_smoke_binding_contract` | `offline_contract` | Define the reviewed mature-path smoke evidence packet required before a fresh continuous CO2/H2O run. | PACE vent, pressure gauge, dewpoint, and fresh queue readiness can be reviewed without using diagnostic points as fit data. |
| 4 | `pending` | `mature_route_continuity_run_manifest_gate` | `offline_guard` | Require a fresh continuous mature-run manifest before CO2/H2O route evidence can feed fitting or release. | Segmented, retry, direct-recovery, and empty-manifest route attempts are blocked from formal continuous-run status. |
| 5 | `pending` | `0613_fit_strategy_matrix_no_write` | `offline_no_write_review` | Codify the 0613 multi-strategy fit review for CO2/H2O, including physical reject/supersede rules and anchor roles. | Candidate coefficients are reviewed through one no-write strategy matrix before any controlled write package. |
| 6 | `pending` | `controlled_write_readback_reverify_bundle` | `controlled_write_contract` | Unify old-value snapshot, controlled write, GETCO readback, and short reverify references into one post-fit write bundle. | Write success and validation success are reported separately per device and coefficient family. |
| 7 | `pending` | `archive_release_postgresql18_import_unlock_sequence` | `offline_import_gate` | Keep PostgreSQL 18 import locked behind archive release, dry-run, authorization, controlled executor, and readback evidence. | Database import cannot run from route, fit, smoke, or no-write evidence alone. |

## Non-Execution Boundary

- opens_com_ports: `false`
- controls_water_or_gas_routes: `false`
- connects_postgresql: `false`
- writes_coefficients: `false`
- formal_release_allowed: `false`
- database_import_allowed: `false`
- not_real_acceptance_evidence: `true`
