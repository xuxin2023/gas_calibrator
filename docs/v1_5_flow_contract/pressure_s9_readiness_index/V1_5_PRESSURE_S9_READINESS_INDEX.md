# V1.5 Pressure/SENCO9 Readiness Index

- schema: `v1_5_pressure_s9_readiness_index_v1`
- overall_status: `review_required`
- ready_for_mature_open_flow_pressure_s9_index: `false`
- device_count: `0`
- device_ready_count: `0`
- linear_exception_count: `0`
- mature_route_baseline: `0620/0621 clean worktree mature physical route`

## Meaning

This artifact explains pressure/SENCO9 readiness before mature CO2/H2O open-flow routes. It separates no-write fit evidence, controlled write/readback, and pressure-only reverify evidence.

## Gates

| gate_id | status | required_before | reason |
|---|---|---|---|
| `pressure_s9_device_count_1_to_6` | `review_required` | `batch_initialization_closeout_pre_gas_index` | active_pressure_s9_device_count=0_outside_1_to_6 |
| `pressure_s9_no_write_fit_basis` | `review_required` | `senco9_controlled_write_readback` | one_or_more_devices_missing_no_write_fit_basis |
| `pressure_s9_write_readback` | `review_required` | `post_write_pressure_reverify` | one_or_more_devices_missing_senco9_readback |
| `linear_s9_controlled_exception_scope` | `pass` | `post_write_pressure_reverify` |  |
| `pressure_s9_post_write_reverify` | `review_required` | `mature_open_flow_route` | one_or_more_devices_missing_post_write_pressure_reverify |

## Devices

| GA | port | protocol_id | SN | S9 model | ready | residual hPa | reasons |
|---|---|---|---|---|---|---:|---|

## Review Reasons

- `active_pressure_s9_device_count=0_outside_1_to_6`
- `one_or_more_devices_missing_no_write_fit_basis`
- `one_or_more_devices_missing_senco9_readback`
- `one_or_more_devices_missing_post_write_pressure_reverify`

## Policy

- Default mature S9 model is `offset_only`.
- Linear S9 is allowed only as an explicit controlled exception with write/readback/reverify evidence.
- This artifact is not a SENCO9 writer and not pressure hardware control.

## Non-Execution Boundary

- opens_com_ports: `false`
- controls_pressure: `false`
- controls_water_or_gas_routes: `false`
- connects_postgresql: `false`
- writes_sn: `false`
- writes_device_id: `false`
- writes_coefficients: `false`
- writes_senco9: `false`
- formal_release_allowed: `false`
- database_import_allowed: `false`
- not_real_acceptance_evidence: `true`
