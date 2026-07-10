# V1.5 Full-Flow Automation Closure Map

- schema: `v1_5_full_flow_automation_closure_v1`
- overall_status: `review_ready`
- automation_closure_status: `structure_closed_live_full_auto_still_gated`
- mature_fitting_baseline: `0613 V1.5 fitting path`
- mature_route_baseline: `0620/0621 clean-worktree mature physical route path`
- remaining_full_auto_gap_count: `9`
- full_production_auto_allowed: `false`

## Meaning

This map says V1.5 structure and guardrails are organized, but full production automation is still gated by explicit live handoffs, controlled writes, short reverify, archive release, and PostgreSQL 18 import locks.

## Automation Stages

| stage | automation_state | entrypoint | production_rule | next_gap |
|---|---|---|---|---|
| `01_initialization_identity_runtime_closeout` | `controlled_real_readonly_available_with_authorization` | `run_v1_5_formal_initialization_runner.py` | Do not enter CO2/H2O routes until SN/device_code, protocol ID, GETCO1-9, S5-S8, and S9 are closed. | Package the per-batch initialization closeout into the pre-gas evidence index automatically. |
| `02_pressure_s9_readiness` | `controlled_live_pressure_when_authorized` | `validate_pressure_only.py` | Use PACE INL absolute pressure. Do not let CO2/H2O fitting absorb pressure error. | Keep offset-only default and linear S9 exceptions explicitly tagged with readback and pressure-only reverify. |
| `03_route_physical_readiness_guard` | `offline_guard_only` | `export_v1_5_route_physical_recovery_readiness.py` | Route recovery evidence can unblock a fresh run, but diagnostic/smoke data cannot become formal fit data. | For future continuous runs, gather live physical recovery evidence through reviewed mature-path smoke, then bind it offline. |
| `04_mature_legacy_co2_45_route` | `mature_runner_real_route_when_authorized` | `run_v1_5_formal_co2_open_flow_queue.py` | Legacy analyzers run CO2 45 points. Single-analyzer ratio instability downgrades that analyzer/point; public physical gates block the point. | Add a pre-run continuity guard that refuses segmented carry-over as a formal continuous run. |
| `05_mature_legacy_h2o_13_route` | `mature_runner_real_route_when_authorized` | `run_v1_5_formal_h2o_open_flow_queue.py` | Legacy analyzers run H2O 13 wet points; new algorithm 14-point plans stay profile/dry-run until reviewed live wiring. | Require run manifests before declaring a water route segment formal; empty queue attempts remain rejected diagnostics. |
| `06_fit_strategy_review` | `offline_no_write_review` | `export_v1_5_fit_input_quality.py` | Use physical fit roles, current GETCO state, dry/low-water anchors with dewpoint/pressure evidence, and explicit reject/supersede reasoning. | Codify the 0613 multi-strategy fit review into one canonical no-write strategy matrix for CO2 and H2O. |
| `07_controlled_write_readback` | `manual_authorized_controlled_write` | `run_v1_5_*_controlled_write.py` | S5/S6 are final linear layers; S5 composition must use current GETCO5 and clear-before-write where required. | Unify per-coefficient write summaries into one post-fit write package without hiding per-device exceptions. |
| `08_short_reverify` | `offline_review_from_real_samples` | `export_v1_5_post_write_reverification.py` | Write success and validation success must be reported separately. | Bind short reverify points to the exact coefficient write package and reject stale reverify evidence. |
| `09_archive_report_database` | `archive_offline_ready_database_import_locked` | `run_v1_5_formal_archive_closure.py` | SN/device_code traceability, readback, reverify, archive release, and DB dry-run must all pass before import. | Implement real PostgreSQL import only after archive release and controlled executor review are complete. |

## Forbidden Formal Surfaces

- `_handoff`
- `D:/gas_calibrator root dirty/migration surface`
- `0624 migrated route path`
- `diagnostic-only tools`
- `sampling workers as top-level launchers`
- `legacy V1 launchers`
- `V2 launchers`

## Protected Core Files

- `src/gas_calibrator/tools/run_v1_5_formal_co2_open_flow_queue.py`
- `src/gas_calibrator/tools/run_v1_5_formal_h2o_open_flow_queue.py`
- `src/gas_calibrator/tools/run_v1_5_formal_open_flow_sampling.py`
- `src/gas_calibrator/tools/run_v1_5_formal_h2o_open_flow_sampling.py`
- `src/gas_calibrator/workflow/runner.py`
- `src/gas_calibrator/devices/gas_analyzer.py`
- `configs/default_config.json`
- `run_app.py`

## Checks

| check_id | status | severity | topic | requirement |
|---|---|---|---|---|
| `AUTO-CLOSURE-001` | `pass` | `blocker` | `baseline` | Mature gas/water execution baseline is 0613 fitting plus 0620/0621 clean-worktree route behavior. |
| `AUTO-CLOSURE-002` | `pass` | `blocker` | `forbidden_surfaces` | Formal automation plans must not reference _handoff, root migration, 0624, diagnostic, worker, V1, or V2 surfaces. |
| `AUTO-CLOSURE-003` | `pass` | `blocker` | `route_core` | This package must not edit mature route queue/worker, runner, analyzer protocol, default config, or app entry. |
| `AUTO-CLOSURE-004` | `pass` | `blocker` | `legacy_vs_new_algorithm` | Legacy remains CO2 45 and H2O 13; new algorithm 47/14 remains profile/dry-run unless separately live-reviewed. |
| `AUTO-CLOSURE-005` | `pass` | `blocker` | `fit_and_write` | Fit review is no-write; controlled write requires live old-value snapshot, write, readback, and short reverify. |
| `AUTO-CLOSURE-006` | `pass` | `blocker` | `full_auto_state` | Current V1.5 is structurally organized but still not one-click full production automation. |
| `AUTO-CLOSURE-007` | `pass` | `blocker` | `database` | PostgreSQL 18 import remains after archive/release and controlled import gates. |

## Non-Execution Boundary

- opens_com_ports: `false`
- controls_water_or_gas_routes: `false`
- connects_postgresql: `false`
- writes_coefficients: `false`
- formal_release_allowed: `false`
- database_import_allowed: `false`
- not_real_acceptance_evidence: `true`
