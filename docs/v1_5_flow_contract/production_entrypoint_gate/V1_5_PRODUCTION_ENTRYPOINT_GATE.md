# V1.5 Production Entrypoint Gate

- schema: `v1_5_production_entrypoint_gate_v1`
- status: `pass`
- blocker_count: `0`
- review_required_count: `0`
- reference_count: `8`
- mature_fitting_baseline: `0613-style V1.5 fitting method`
- mature_physical_baseline: `0620/0621 mature physical execution path`
- plan_path: `D:\gas_calibrator\_worktrees\v1_5_production_entrypoint_gate_20260709\docs\v1_5_flow_contract\production_entrypoint_gate\v1_5_production_entrypoint_gate_sample_plan.json`

## Reviewed References

| step | status | policy | normalized reference | reason |
|---|---|---|---|---|
| `formal_initialization` | `pass` | `production_entrypoint_allowed` | `src/gas_calibrator/tools/run_v1_5_formal_initialization_runner.py` | Reference is listed in the V1.5 production entrypoint map. |
| `readonly_com_identity_getco_closeout` | `pass` | `production_entrypoint_allowed` | `src/gas_calibrator/tools/run_v1_5_formal_readonly_com_minimal_executor.py` | Reference is listed in the V1.5 production entrypoint map. |
| `pressure_s9_no_write` | `pass` | `production_entrypoint_allowed` | `src/gas_calibrator/tools/validate_pressure_only.py` | Reference is listed in the V1.5 production entrypoint map. |
| `legacy_co2_45` | `pass` | `production_entrypoint_allowed` | `src/gas_calibrator/tools/run_v1_5_formal_co2_open_flow_queue.py` | Reference is listed in the V1.5 production entrypoint map. |
| `legacy_h2o_13` | `pass` | `production_entrypoint_allowed` | `src/gas_calibrator/tools/run_v1_5_formal_h2o_open_flow_queue.py` | Reference is listed in the V1.5 production entrypoint map. |
| `candidate_fit_review` | `pass` | `production_entrypoint_allowed` | `src/gas_calibrator/tools/export_v1_5_candidate_coefficients.py` | Reference is listed in the V1.5 production entrypoint map. |
| `controlled_coefficient_write` | `pass` | `production_entrypoint_allowed` | `src/gas_calibrator/tools/run_v1_5_co2_senco13_controlled_write.py` | Reference is listed in the V1.5 production entrypoint map. |
| `formal_run_status` | `pass` | `production_entrypoint_allowed` | `src/gas_calibrator/tools/export_v1_5_formal_run_status.py` | Reference is listed in the V1.5 production entrypoint map. |

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

This gate reviews references only. It does not execute a formal plan or authorize live calibration.
