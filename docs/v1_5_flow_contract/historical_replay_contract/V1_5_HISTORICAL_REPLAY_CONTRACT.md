# V1.5 Historical Replay Contract

- schema: `v1_5_historical_replay_contract_v1`
- status: `pass`
- blocker_count: `0`
- profile_path: `D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean\configs\v1_5_algorithm_route_profiles.json`
- focused_pytest_evidence: `V1_5_HISTORICAL_REPLAY_CONTRACT_TEST_EVIDENCE_20260701.md`

## Physical Boundaries

- opens_com_ports: `False`
- connects_postgresql: `False`
- controls_pressure: `False`
- controls_water_or_gas_routes: `False`
- writes_coefficients: `False`
- writes_device_id: `False`
- not_real_acceptance_evidence: `True`

## Replay Contract

| Key | Value |
|---|---|
| `purpose` | `offline_program_level_regression_guard` |
| `source_families` | `["mature_0620_legacy_ratio", "later_legacy_regression_runs", "new_algorithm_shadow_run"]` |
| `required_roles` | `["initialization_readiness", "pressure_senco9_review", "co2_open_flow_points", "h2o_open_flow_points", "point_qc_and_quality_grade", "fit_input_review", "post_write_reverify", "archive_status"]` |
| `legacy_replay_fit_input` | `R_CO2/R_H2O` |
| `new_algorithm_replay_fit_input` | `A=-ln(R/R0(T))/(P_kPa/100)` |
| `release_policy` | `replay_pass_does_not_authorize_archive_or_database_import` |

## Replay Source Families

| Family | Profile | Role | Fit inputs | Release from replay |
|---|---|---|---|---|
| `mature_0620_legacy_ratio` | `legacy_ratio_production` | `mature_path_reference` | `{"co2": "R_CO2", "formula": "", "h2o": "R_H2O"}` | `archive=False, db=False` |
| `later_legacy_regression_runs` | `legacy_ratio_production` | `regression_reference` | `{"co2": "R_CO2", "formula": "", "h2o": "R_H2O"}` | `archive=False, db=False` |
| `new_algorithm_shadow_run` | `absorption_ratio_shadow` | `new_algorithm_fit_input_shadow` | `{"co2": "A_CO2_from_R_CO2_and_R0_CO2_T", "formula": "A=-ln(R/R0(T))/(P_kPa/100)", "h2o": "A_H2O_from_R_H2O_and_R0_H2O_T"}` | `archive=False, db=False` |

## Checks

| Check | Status | Reason | Physical meaning |
|---|---|---|---|
| `historical_replay_is_offline_only` | `pass` | this contract exports only JSON/CSV/Markdown and performs no hardware or database action | Replay can find program regressions, but it cannot replace a physical run, post-write reverify, archive closure, or database release. |
| `replay_source_families_have_required_roles` | `pass` | each replay family must carry initialization, pressure, CO2, H2O, QC, fit, reverify, and archive-status roles | Historical data replay must preserve where each row came from and why it is or is not fit-eligible. |
| `legacy_replay_uses_mature_ratio_profile` | `pass` | legacy replay must not silently reinterpret mature ratio data as absorption data | The old algorithm's replay physics is ratio R; absorption A belongs only to the new algorithm shadow profile. |
| `legacy_replay_preserves_45_13_counts` | `pass` | historical replay may compare runs, but it must not redefine the mature route size | The replay contract protects the 0620 mature point sequence from being diluted by diagnostics or retry fragments. |
| `new_algorithm_replay_uses_absorption_shadow` | `pass` | new algorithm replay can evaluate A and supplemental candidates, but it cannot replace mature runners | The new algorithm changes concentration math through A and R0(T); it does not authorize a second physical route runner. |
| `new_algorithm_replay_requires_r0_evidence` | `pass` | A=-ln(R/R0)/(P/100) cannot be production-complete without R0_CO2(T)/R0_H2O(T) evidence and writer/readback contracts | R0 is not a cosmetic metadata field; it is part of the physical absorption transform and must stay traceable. |
| `replay_qc_rejections_remain_non_fit` | `pass` | historical rejected rows must not become fit-eligible just because replay needs more points | Replay should improve program confidence by preserving quality labels, not by washing out unstable or failed points. |
| `replay_does_not_authorize_archive_or_database_release` | `pass` | historical replay is regression evidence, not current-run archive or PostgreSQL 18 import evidence | A replay pass can say the program still understands old evidence; it cannot say today's run is ready to release. |
| `evidence_zone_is_not_code_source` | `pass` | _handoff and root-draft evidence can be read as historical inputs only; formal code still comes from the clean V1.5 worktree | This prevents replay from reintroducing the earlier V1/V2/root/worktree mixing problem. |
