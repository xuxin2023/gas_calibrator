# V1.5 Route Run Failure Root-Cause Audit

- schema: `v1_5_route_run_failure_root_cause_audit_v1`
- status: `blocked`
- run_dir_count: `18`
- finding_count: `33`
- blocker_count: `12`
- review_required_count: `21`

## Category Counts

| category | count |
|---|---:|
| `direct_or_retry_point_without_queue_manifest` | 8 |
| `dry_gas_dewpoint_rebound_or_not_dry_enough` | 1 |
| `manual_parameter_or_execution_mode_change` | 13 |
| `pressure_controller_vent_no_response` | 3 |
| `pressure_gauge_no_response` | 1 |
| `queue_aborted_before_sampling_no_manifest` | 3 |
| `running_manifest_without_completed_point_artifacts` | 2 |
| `stale_running_manifest_with_completed_point_artifacts` | 2 |

## Findings

| severity | category | run | point | root cause | required action |
|---|---|---|---|---|---|
| `blocker` | `dry_gas_dewpoint_rebound_or_not_dry_enough` | `co2_6old_0620clean_mature45_g3_finalparams` | `p002_T40_400ppm_fit` | The dry-gas route did not hold a stable dry dewpoint before sampling. | Do not fit this point; dry/purge the route and rerun the point or select a reviewed retry. |
| `blocker` | `stale_running_manifest_with_completed_point_artifacts` | `co2_6old_0620clean_mature45_g3_finalparams` | `p003_T40_1000ppm_fit` | Queue manifest was not finalized after point artifacts were written. | Regenerate or review the accepted manifest; do not treat the original queue as continuous. |
| `review` | `manual_parameter_or_execution_mode_change` | `co2_6old_0620clean_mature45_g3_finalparams` | `` | Run naming indicates changed execution parameters such as no-temp wait, 240 s purge, finalparams, direct, or retry. | Require parameter-hash review and accepted-manifest selection before fitting. |
| `blocker` | `pressure_controller_vent_no_response` | `co2_6old_0620clean_mature45_g4_T30_to_Tm20_finalparams` | `p015_T20_300ppm_fit` | PACE atmosphere vent command returned NO_RESPONSE during startup/pre-point reset. | Stop the queue, recover PACE communication/vent state, then rerun from a clean queue segment. |
| `blocker` | `stale_running_manifest_with_completed_point_artifacts` | `co2_6old_0620clean_mature45_g4_T30_to_Tm20_finalparams` | `p016_T20_400ppm_fit` | Queue manifest was not finalized after point artifacts were written. | Regenerate or review the accepted manifest; do not treat the original queue as continuous. |
| `review` | `manual_parameter_or_execution_mode_change` | `co2_6old_0620clean_mature45_g4_T30_to_Tm20_finalparams` | `` | Run naming indicates changed execution parameters such as no-temp wait, 240 s purge, finalparams, direct, or retry. | Require parameter-hash review and accepted-manifest selection before fitting. |
| `blocker` | `running_manifest_without_completed_point_artifacts` | `co2_6old_0620clean_mature45_g8_T20_500_to_1000_notemp_finalparams` | `p001_T20_500ppm_fit` | Queue stopped or was interrupted before the point completed. | Rerun the point from a clean queue segment. |
| `review` | `manual_parameter_or_execution_mode_change` | `co2_6old_0620clean_mature45_g8_T20_500_to_1000_notemp_finalparams` | `` | Run naming indicates changed execution parameters such as no-temp wait, 240 s purge, finalparams, direct, or retry. | Require parameter-hash review and accepted-manifest selection before fitting. |
| `blocker` | `running_manifest_without_completed_point_artifacts` | `co2_6old_0620clean_mature45_g8b_T20_500_to_1000_notemp_240purge_finalparams` | `p001_T20_500ppm_fit` | Queue stopped or was interrupted before the point completed. | Rerun the point from a clean queue segment. |
| `review` | `manual_parameter_or_execution_mode_change` | `co2_6old_0620clean_mature45_g8b_T20_500_to_1000_notemp_240purge_finalparams` | `` | Run naming indicates changed execution parameters such as no-temp wait, 240 s purge, finalparams, direct, or retry. | Require parameter-hash review and accepted-manifest selection before fitting. |
| `review` | `direct_or_retry_point_without_queue_manifest` | `co2_6old_0620clean_mature45_g8c_T20_500_direct_240purge_finalparams` | `` | A completed point was produced outside a closed formal queue manifest. | Bind it into an accepted manifest with the failed point it supersedes; never call it continuous. |
| `review` | `manual_parameter_or_execution_mode_change` | `co2_6old_0620clean_mature45_g8c_T20_500_direct_240purge_finalparams` | `` | Run naming indicates changed execution parameters such as no-temp wait, 240 s purge, finalparams, direct, or retry. | Require parameter-hash review and accepted-manifest selection before fitting. |
| `review` | `direct_or_retry_point_without_queue_manifest` | `co2_6old_0620clean_mature45_g8d_T20_600_direct_240purge_finalparams` | `` | A completed point was produced outside a closed formal queue manifest. | Bind it into an accepted manifest with the failed point it supersedes; never call it continuous. |
| `review` | `manual_parameter_or_execution_mode_change` | `co2_6old_0620clean_mature45_g8d_T20_600_direct_240purge_finalparams` | `` | Run naming indicates changed execution parameters such as no-temp wait, 240 s purge, finalparams, direct, or retry. | Require parameter-hash review and accepted-manifest selection before fitting. |
| `review` | `direct_or_retry_point_without_queue_manifest` | `co2_6old_0620clean_mature45_g8e_T20_700_direct_240purge_finalparams` | `` | A completed point was produced outside a closed formal queue manifest. | Bind it into an accepted manifest with the failed point it supersedes; never call it continuous. |
| `review` | `manual_parameter_or_execution_mode_change` | `co2_6old_0620clean_mature45_g8e_T20_700_direct_240purge_finalparams` | `` | Run naming indicates changed execution parameters such as no-temp wait, 240 s purge, finalparams, direct, or retry. | Require parameter-hash review and accepted-manifest selection before fitting. |
| `review` | `direct_or_retry_point_without_queue_manifest` | `co2_6old_0620clean_mature45_g8f_T20_800_direct_240purge_finalparams` | `` | A completed point was produced outside a closed formal queue manifest. | Bind it into an accepted manifest with the failed point it supersedes; never call it continuous. |
| `review` | `manual_parameter_or_execution_mode_change` | `co2_6old_0620clean_mature45_g8f_T20_800_direct_240purge_finalparams` | `` | Run naming indicates changed execution parameters such as no-temp wait, 240 s purge, finalparams, direct, or retry. | Require parameter-hash review and accepted-manifest selection before fitting. |
| `review` | `direct_or_retry_point_without_queue_manifest` | `co2_6old_0620clean_mature45_g8g_T20_900_direct_240purge_finalparams` | `` | A completed point was produced outside a closed formal queue manifest. | Bind it into an accepted manifest with the failed point it supersedes; never call it continuous. |
| `review` | `manual_parameter_or_execution_mode_change` | `co2_6old_0620clean_mature45_g8g_T20_900_direct_240purge_finalparams` | `` | Run naming indicates changed execution parameters such as no-temp wait, 240 s purge, finalparams, direct, or retry. | Require parameter-hash review and accepted-manifest selection before fitting. |
| `review` | `direct_or_retry_point_without_queue_manifest` | `co2_6old_0620clean_mature45_g8h_T20_1000_direct_240purge_finalparams` | `` | A completed point was produced outside a closed formal queue manifest. | Bind it into an accepted manifest with the failed point it supersedes; never call it continuous. |
| `review` | `manual_parameter_or_execution_mode_change` | `co2_6old_0620clean_mature45_g8h_T20_1000_direct_240purge_finalparams` | `` | Run naming indicates changed execution parameters such as no-temp wait, 240 s purge, finalparams, direct, or retry. | Require parameter-hash review and accepted-manifest selection before fitting. |
| `blocker` | `pressure_gauge_no_response` | `co2_6old_0620clean_mature45_g9_T10_to_Tm20_finalparams` | `p019_Tm20_400ppm_fit` | Pressure gauge readback returned NO_RESPONSE during pre-seal/open-flow verification. | Hold the run, restore pressure gauge communication, and rerun the affected point. |
| `review` | `manual_parameter_or_execution_mode_change` | `co2_6old_0620clean_mature45_g9_T10_to_Tm20_finalparams` | `` | Run naming indicates changed execution parameters such as no-temp wait, 240 s purge, finalparams, direct, or retry. | Require parameter-hash review and accepted-manifest selection before fitting. |
| `review` | `direct_or_retry_point_without_queue_manifest` | `co2_6old_0620clean_mature45_g10_Tm20_400_retry_finalparams` | `` | A completed point was produced outside a closed formal queue manifest. | Bind it into an accepted manifest with the failed point it supersedes; never call it continuous. |
| `review` | `manual_parameter_or_execution_mode_change` | `co2_6old_0620clean_mature45_g10_Tm20_400_retry_finalparams` | `` | Run naming indicates changed execution parameters such as no-temp wait, 240 s purge, finalparams, direct, or retry. | Require parameter-hash review and accepted-manifest selection before fitting. |
| `review` | `direct_or_retry_point_without_queue_manifest` | `co2_6old_0620clean_mature45_g11_Tm20_1000_direct_finalparams` | `` | A completed point was produced outside a closed formal queue manifest. | Bind it into an accepted manifest with the failed point it supersedes; never call it continuous. |
| `review` | `manual_parameter_or_execution_mode_change` | `co2_6old_0620clean_mature45_g11_Tm20_1000_direct_finalparams` | `` | Run naming indicates changed execution parameters such as no-temp wait, 240 s purge, finalparams, direct, or retry. | Require parameter-hash review and accepted-manifest selection before fitting. |
| `blocker` | `queue_aborted_before_sampling_no_manifest` | `h2o_6old_0620clean_mature13_g1` | `` | The queue planned points but aborted before producing queue_manifest.csv. | Treat the segment as not executed; restart with a valid queue runner and require queue_manifest.csv. |
| `blocker` | `queue_aborted_before_sampling_no_manifest` | `h2o_6old_0620clean_mature13_g2` | `` | The queue planned points but aborted before producing queue_manifest.csv. | Treat the segment as not executed; restart with a valid queue runner and require queue_manifest.csv. |
| `blocker` | `queue_aborted_before_sampling_no_manifest` | `h2o_6old_0620clean_mature13_g3` | `` | The queue planned points but aborted before producing queue_manifest.csv. | Treat the segment as not executed; restart with a valid queue runner and require queue_manifest.csv. |
| `blocker` | `pressure_controller_vent_no_response` | `h2o_6old_0620clean_mature13_g4` | `p002_T10_HG10C_30RH_h2o` | PACE atmosphere vent command returned NO_RESPONSE during startup/pre-point reset. | Stop the queue, recover PACE communication/vent state, then rerun from a clean queue segment. |
| `blocker` | `pressure_controller_vent_no_response` | `h2o_6old_0620clean_mature13_g4` | `p002_T10_HG10C_30RH_h2o` | PACE atmosphere vent command returned NO_RESPONSE during startup/pre-point reset. | Stop the queue, recover PACE communication/vent state, then rerun from a clean queue segment. |

## Boundary

- Offline evidence review only.
- Does not open COM ports, control pressure, control gas/water routes, connect PostgreSQL, or write coefficients/SN.
- Findings are not real acceptance evidence.
