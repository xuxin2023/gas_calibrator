# V1.5 Mature Route Contract

- schema: `v1_5_mature_route_contract_v1`
- status: `pass`
- blocker_count: `0`
- profile_path: `D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean\configs\v1_5_algorithm_route_profiles.json`

## Physical Boundaries

- opens_com_ports: `False`
- connects_postgresql: `False`
- controls_pressure: `False`
- controls_water_or_gas_routes: `False`
- writes_coefficients: `False`
- writes_device_id: `False`
- not_real_acceptance_evidence: `True`

## Mature Route Contract

| Key | Value |
|---|---|
| `route_behavior` | `preserve_mature_v1_5_0620_route_timing_and_quality_gates` |
| `legacy_co2_point_count` | `45` |
| `legacy_h2o_wet_point_count` | `13` |
| `co2_runner` | `gas_calibrator.tools.run_v1_5_formal_co2_open_flow_queue` |
| `h2o_runner` | `gas_calibrator.tools.run_v1_5_formal_h2o_open_flow_queue` |
| `new_algorithm_difference_layer` | `profile_fit_input_R0_contract_supplements_write_contract` |

## Checks

| Check | Status | Reason | Physical meaning |
|---|---|---|---|
| `shared_route_behavior_0620` | `pass` | route_behavior must stay pinned to the mature V1.5 0620 timing/QC contract | Algorithm or report changes must not rewrite the mature physical gas/water route timing and QC behavior. |
| `shared_route_runner_names` | `pass` | old/new algorithm profiles must point at the same mature CO2/H2O queue runners | New algorithm differences are allowed in fit inputs and write contracts, not in the route runner identity. |
| `legacy_default_profile` | `pass` | legacy ratio production remains default until absorption production blockers are closed | A new algorithm profile may exist as a candidate without silently becoming the production route. |
| `legacy_co2_45_point_contract` | `pass` | legacy CO2 point count and temperature/ppm plan must not drift | CO2 production fitting uses the mature 45-point open-flow gas route, not ad hoc replay or diagnostic points. |
| `legacy_h2o_13_point_contract` | `pass` | legacy H2O wet point count and temperature/HGEN plan must not drift | H2O production fitting uses the mature 13-point wet route; dry/low-water anchors are separate evidence roles. |
| `legacy_ratio_fit_input_contract` | `pass` | legacy production profile must not silently switch from ratio R to absorption A | Old algorithm replay/fitting should continue to interpret measured ratios directly. |
| `absorption_profile_fit_input_only` | `pass` | new algorithm may add candidate evidence, but the mature route baseline remains 45/13 | Absorption A is a fitting-layer change; it does not justify changing mature gas/water route sequencing. |
| `supplement_points_do_not_modify_legacy_queue` | `pass` | supplement points are candidate evidence, not legacy queue edits | Supplemental points are run as normal gas/water points only when selected by the new algorithm profile. |
| `r0_writer_contract_blocks_absorption_release` | `pass` | absorption profile cannot be complete production until R0 writer/readback exists | R0_CO2(T) and R0_H2O(T) are physical calibration models that require controlled SENCOA/SENCOB write/readback contracts. |
| `co2_zero_h2o_dry_anchor_separation` | `pass` | low-end anchors must preserve their measured physical quantity | CO2 zero gas constrains CO2; H2O dry/low-water anchors require dewpoint/pressure/T evidence and are not forced to zero. |
| `canonical_entrypoint_guard` | `pass` | sampling workers must not become top-level formal start points | The queue owns route-level timing and point progression; workers only execute a queue-selected point. |
