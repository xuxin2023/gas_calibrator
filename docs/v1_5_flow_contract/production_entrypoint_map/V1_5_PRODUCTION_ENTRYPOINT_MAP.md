# V1.5 Production Entrypoint Map

- schema: `v1_5_production_entrypoint_map_v1`
- status: `pass`
- blocker_count: `0`
- mature_fitting_baseline: `0613-style V1.5 fitting method`
- mature_physical_baseline: `0620/0621 mature physical execution path`
- legacy CO2 points: `45`
- legacy H2O wet points: `13`
- new algorithm CO2 points: `47`
- new algorithm H2O wet points: `14`

## Production Entrypoints

| id | group | path | launch policy | point contract |
|---|---|---|---|---|
| `initialization_planner` | `01_initialization` | `src/gas_calibrator/tools/run_v1_5_formal_initialization_runner.py` | `offline_plan_and_gate_only` | `not_a_route_runner` |
| `readonly_com_identity_getco_closeout` | `01_initialization` | `src/gas_calibrator/tools/run_v1_5_formal_readonly_com_minimal_executor.py` | `manual_authorized_read_only_com_only` | `not_a_route_runner` |
| `pressure_s9_no_write` | `02_pressure` | `src/gas_calibrator/tools/validate_pressure_only.py` | `manual_authorized_pressure_no_write` | `PACE INL absolute pressure, S9 no-write first` |
| `co2_mature_legacy_queue` | `03_co2` | `src/gas_calibrator/tools/run_v1_5_formal_co2_open_flow_queue.py` | `manual_authorized_route_runner` | `legacy CO2 45 points` |
| `h2o_mature_legacy_queue` | `04_h2o` | `src/gas_calibrator/tools/run_v1_5_formal_h2o_open_flow_queue.py` | `manual_authorized_route_runner` | `legacy H2O 13 wet points` |
| `candidate_fit_review` | `05_fitting` | `src/gas_calibrator/tools/export_v1_5_candidate_coefficients.py` | `offline_no_write_review` | `uses eligible production evidence only` |
| `controlled_coefficient_writes` | `06_controlled_write` | `src/gas_calibrator/tools/run_v1_5_co2_senco13_controlled_write.py` | `manual_authorized_controlled_write_only` | `write/readback/reverify required` |
| `archive_and_database_locked_chain` | `07_archive_database` | `src/gas_calibrator/tools/export_v1_5_formal_run_status.py` | `offline_status_only` | `not_a_route_runner` |

## Forbidden As Production Launchers

| surface | policy | examples | reason |
|---|---|---|---|
| `handoff_evidence` | `not_a_production_launcher` | _handoff/*, ad hoc run folders, copied evidence bundles | Evidence and scratch artifacts can explain a decision, but must not be used as executable production entrypoints. |
| `root_migration_area` | `not_a_mature_baseline` | root-level migrated runners, temporary queue experiments, 0624 handoff logic | The current production baseline is the 0613 fitting method plus 0620/0621 mature physical path, not later migration drafts. |
| `diagnostic_probes` | `diagnostic_only` | dynamic pressure diagnostics, no-OUTP probes, sealed-pressure tuning, extended hold experiments | Diagnostic rows may explain physics, but are not production fit or release evidence unless separately reviewed. |
| `sampling_workers` | `worker_not_top_level` | run_v1_5_formal_open_flow_sampling.py, run_v1_5_formal_h2o_open_flow_sampling.py | Workers must be called by canonical CO2/H2O queue runners so point order, route setup, and evidence indexing stay coherent. |
| `legacy_v1_or_v2_surfaces` | `not_v1_5_production_entry` | run_v1_*, V2 device workbench, V2 engineering probes | V1 is historical/fallback reference and V2 remains outside V1.5 production entrypoint control. |

## Checks

| check_id | status | severity | requirement |
|---|---|---|---|
| `ENTRY-MAP-001` | `pass` | `blocker` | Legacy CO2 production entry must remain the mature 45-point queue. |
| `ENTRY-MAP-002` | `pass` | `blocker` | Legacy H2O production entry must remain the mature 13 wet-point queue. |
| `ENTRY-MAP-003` | `pass` | `blocker` | Sampling workers must not be top-level production launchers. |
| `ENTRY-MAP-004` | `pass` | `blocker` | Root migration and 0624 handoff areas must not be treated as mature V1.5 baseline. |
| `ENTRY-MAP-005` | `pass` | `blocker` | Coefficient writes are only manual-authorized controlled-write surfaces with readback/reverify. |
| `ENTRY-MAP-006` | `pass` | `blocker` | This entrypoint map is offline documentation and must not grant release, import, COM, route, pressure, or write actions. |

## Non-Execution Boundary

- opens_com_ports: `false`
- controls_pressure: `false`
- controls_water_or_gas_routes: `false`
- connects_postgresql: `false`
- writes_coefficients: `false`
- writes_sn_or_device_code: `false`
- formal_release_allowed: `false`
- database_import_allowed: `false`
- not_real_acceptance_evidence: `true`

This map is a review artifact. It does not replace authorization packets, pressure readiness, route readiness, controlled writes, reverify, archive closure, or PostgreSQL import gates.
