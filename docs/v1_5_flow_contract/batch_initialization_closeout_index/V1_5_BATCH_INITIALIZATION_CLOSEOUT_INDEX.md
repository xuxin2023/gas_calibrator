# V1.5 Batch Initialization Closeout Index

- schema: `v1_5_batch_initialization_closeout_index_v1`
- overall_status: `review_required`
- batch_initialization_closeout_ready: `false`
- ready_for_mature_open_flow_from_initialization_index: `false`
- device_count: `0`
- mature_route_baseline: `0620/0621 clean worktree mature physical route`
- mature_fitting_baseline: `0613 V1.5 fitting path`

## Meaning

This artifact binds batch initialization closeout evidence into one pre-gas index. It is not a live runner and it is not release or database import evidence.

## Gates

| gate_id | status | required_before | reason |
|---|---|---|---|
| `readonly_com_identity_getco_closeout` | `review_required` | `pre_gas_readiness_index` | readonly_com_executor_evidence_missing |
| `active_device_count_1_to_6` | `review_required` | `pre_gas_readiness_index` | active_device_count=0_outside_1_to_6 |
| `device_identity_getco_runtime_auxiliary_s5_s8_closeout` | `review_required` | `pre_gas_readiness_index` | one_or_more_device_closeout_rows_not_ready |
| `pressure_s9_closeout` | `review_required` | `mature_open_flow_route` | pressure_s9_evidence_missing |
| `formal_route_readiness` | `review_required` | `mature_open_flow_route` | route_readiness_evidence_missing |
| `pre_gas_readiness_sidecar_reference` | `pass` | `review_traceability` |  |

## Devices

| GA | port | protocol_id | SN | ready | reasons |
|---|---|---|---|---|---|

## Review Reasons

- `readonly_com_executor_evidence_missing`
- `active_device_count=0_outside_1_to_6`
- `one_or_more_device_closeout_rows_not_ready`
- `pressure_s9_evidence_missing`
- `route_readiness_evidence_missing`

## Non-Execution Boundary

- opens_com_ports: `false`
- read_only_real_com_execution_allowed: `false`
- controls_pressure: `false`
- controls_water_or_gas_routes: `false`
- connects_postgresql: `false`
- writes_sn: `false`
- writes_device_id: `false`
- writes_coefficients: `false`
- formal_release_allowed: `false`
- database_import_allowed: `false`
- not_real_acceptance_evidence: `true`
