# V1.5 Automation Control Contract

- schema: `v1_5_automation_control_contract_v1`
- status: `pass`
- blocker_count: `0`
- mature_fitting_baseline: `0613-style V1.5 fitting method`
- mature_physical_baseline: `0620/0621 mature physical execution path`
- automation_model: `mature_core_with_automation_shell`

## Principle

V1.5 automation is an orchestration shell around the mature core. It may prepare inputs, enforce gates, collect evidence, and call reviewed entrypoints, but it must not reimplement the 0613 fitting method or the 0620/0621 physical CO2/H2O route kernel.

## Canonical Automation Stages

1. `01_initialization_identity_runtime_closeout`
2. `02_pre_gas_readiness_and_pressure_s9`
3. `03_mature_legacy_co2_45_route`
4. `04_mature_legacy_h2o_13_route`
5. `05_no_write_fit_strategy_review`
6. `06_controlled_write_with_readback`
7. `07_short_reverify`
8. `08_archive_and_database_dry_run`

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
|---|---:|---|---|---|
| `AUTO-CORE-001` | `pass` | `blocker` | `mature_core` | V1.5 automation must call the 0613 fitting baseline and the 0620/0621 mature physical execution path; automation is a shell, not a rewritten route kernel. |
| `AUTO-CORE-002` | `pass` | `blocker` | `protected_files` | This contract package must not modify protected mature route, protocol, default, or app-entry files. |
| `AUTO-ENTRY-001` | `pass` | `blocker` | `entrypoints` | Production launchers are initialization, pre-gas readiness, mature CO2/H2O queues, no-write fit review, controlled write, reverify, archive, and database dry-run/import gates. |
| `AUTO-ROUTE-001` | `pass` | `blocker` | `legacy_route` | Legacy production remains CO2 45 points and H2O 13 wet points unless a separate reviewed profile says otherwise. |
| `AUTO-ROUTE-002` | `pass` | `blocker` | `point_quality` | Analyzer-local ratio instability downgrades that analyzer/point quality; public physical gates such as pressure, route, dewpoint, and source failure are the point-level blockers. |
| `AUTO-PRESS-001` | `pass` | `blocker` | `pressure` | Pressure uses PACE INL absolute pressure evidence before CO2/H2O route execution. |
| `AUTO-FIT-001` | `pass` | `blocker` | `co2_fit` | CO2 fitting uses S1/S3 as the main model and S5 as a final linear layer; S5 writes must account for current GETCO5 state and prefer CLEARSENCO5,YGAS,FFF before writing. |
| `AUTO-FIT-002` | `pass` | `blocker` | `h2o_fit` | H2O fitting uses S2/S4 as the main model and S6 as a separate final linear layer. |
| `AUTO-FIT-003` | `pass` | `blocker` | `anchors` | Keep CO2 zero gas and H2O dry-gas or low-water anchor physically separate. |
| `AUTO-EVID-001` | `pass` | `blocker` | `evidence_state` | Separate real_pass, no_write_candidate, diagnostic_only, review_required, superseded, and rejected evidence states. |
| `AUTO-DB-001` | `pass` | `blocker` | `database` | PostgreSQL 18 import remains after archive/release gates and SN/device_code traceability are closed. |
| `AUTO-LIVE-001` | `pass` | `blocker` | `live_actions` | This contract is offline only and does not execute COM, route, pressure, database, or coefficient writes. |

## Non-Execution Boundary

- opens_com_ports: `false`
- controls_water_or_gas_routes: `false`
- connects_postgresql: `false`
- writes_coefficients: `false`
- writes_sn_or_device_code: `false`
- formal_release_allowed: `false`
- database_import_allowed: `false`
- not_real_acceptance_evidence: `true`
