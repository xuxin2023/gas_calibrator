# V2 Cutover Candidate Worksheet

Selected conclusion: Can enter V2 real-machine dry-run preparation, but real write remains prohibited.

Boundary:
- This worksheet supports V2 real-machine dry-run preparation only.
- Real write, real acceptance, default-entry switch, and real_primary_latest refresh remain prohibited.
- V2 is not declared a formal replacement for V1.

| Item | Status | Evidence | Reason | Remaining blocker |
| --- | --- | --- | --- | --- |
| Smoke | green | tests/v2/test_run_validation_replay.py; tests/v2/test_compare_v1_v2_control_flow.py | Offline replay and compare smoke coverage is available for the current preparation worksheet. | No dry-run-preparation blocker; real smoke remains explicitly out of scope. |
| Safe | green | src/gas_calibrator/v2/scripts/_cli_safety.py; tests/v2/test_export_resilience.py | Step-2 safety messaging and export resilience are available without opening real ports. | No dry-run-preparation blocker; real COM/serial and live write remain prohibited. |
| route_trace | green | src/gas_calibrator/v2/scripts/route_trace_diff.py | Route traces are produced and diffable as offline review artifacts. | Real route trace refresh is not allowed in this batch. |
| Fit-Ready | green | src/gas_calibrator/v2/output/fit_ready_smoke/run_*; tests/v2/test_export_resilience.py | Fit-ready/export artifact paths are covered as offline evidence and resilience tests. | No real fit-ready acceptance is claimed. |
| completed/progress semantics | green | tests/v2/test_run_validation_replay.py; tests/v2/test_run_state.py | Replay/state coverage protects completed and progress semantics from false-green drift. | No dry-run-preparation blocker. |
| headless entry | green | src/gas_calibrator/v2/scripts/run_v2.py; tests/v2/test_run_v2.py | The formal V2 headless entry remains the offline launch path for review and simulation. | Default production entry remains V1; no V2 default switch is allowed. |
| H2O single-route readiness | yellow | src/gas_calibrator/v2/output/v1_v2_compare/replacement_h2o_only_simulated_latest.json; tests/v2/test_verify_v1_v2_h2o_only_replacement.py | H2O has diagnostic/offline coverage, but it is not the main replacement route for this phase. | H2O hardware equivalence and humidity-feedback behavior remain future dry-run/bench questions. |
| CO2 single-route readiness | green | src/gas_calibrator/v2/output/v1_v2_compare/replacement_skip0_co2_only_simulated_latest.json | The narrowed CO2-only skip0 path is usable for replacement-validation preparation in simulation. | Real-machine dry-run evidence remains pending and must not include live write. |
| single temperature group readiness | yellow | src/gas_calibrator/v2/configs/validation/simulated/replacement_skip0_co2_only_simulated.json | The current preparation path is narrowed to one CO2 temperature group, not a full H2O+CO2 group. | Full single-temperature H2O+CO2 group evidence remains future dry-run preparation work. |
| route trace diff readiness | green | src/gas_calibrator/v2/scripts/route_trace_diff.py; tests/v2/test_route_trace_diff.py | Route trace diff has structured output and tests for the narrowed replacement path. | Real compare/verify remains prohibited. |
| narrowed skip0 replacement readiness | green | src/gas_calibrator/v2/output/v1_v2_compare/replacement_skip0_co2_only_simulated_latest.json | Third-batch scope is narrowed_skip0_co2_only and the path is usable for preparation. | cutover_ready remains false; this does not prove full V1 replacement. |
| rollback strategy ready | green | v2_cutover_rollback_guard.md | A fallback guard/SOP is generated with V1 as the unchanged default entry. | Dry-run rollback rehearsal remains document-only in this batch. |
| V1 remains frozen | green | v1_freeze_check_summary.json | No V1 production/default-entry paths changed. | none |

Status semantics:
- green: sufficient for this dry-run-preparation worksheet.
- yellow: reviewer attention or future dry-run evidence remains, but it is not a P0 blocker here.
- red: P0 blocker for entering V2 real-machine dry-run preparation.
