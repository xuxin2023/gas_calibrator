# V1.5 Historical Replay Missing Point Audit

- schema: `v1_5_historical_replay_missing_point_audit_v1`
- status: `review_required`
- blocker_count: `0`
- review_required_count: `2`
- missing_point_count: `9`
- segmented_quality_candidate_count: `6`
- supplemental_unresolved_count: `3`
- unresolved_point_count: `3`
- replay_evidence_path: `D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean\docs\v1_5_flow_contract\historical_replay_evidence\v1_5_historical_replay_evidence.json`

## Physical Boundaries

- opens_com_ports: `False`
- connects_postgresql: `False`
- controls_water_or_gas_routes: `False`
- writes_coefficients: `False`
- formal_release_allowed: `False`
- database_import_allowed: `False`
- not_real_acceptance_evidence: `True`

## Missing Points

| Point | Family | Route | Supplemental? | Recommendation | Candidates |
|---|---|---|---:|---|---:|
| `-20/0` | `new_algorithm_shadow_run` | `co2` | `False` | `review_bind_segmented_quality_candidate` | 2 |
| `-20/400` | `new_algorithm_shadow_run` | `co2` | `False` | `review_bind_segmented_quality_candidate` | 2 |
| `-20/600` | `new_algorithm_shadow_run` | `co2` | `True` | `targeted_supplemental_resampling_candidate` | 0 |
| `-20/1000` | `new_algorithm_shadow_run` | `co2` | `False` | `review_bind_segmented_quality_candidate` | 3 |
| `-10/0` | `new_algorithm_shadow_run` | `co2` | `False` | `review_bind_segmented_quality_candidate` | 2 |
| `-10/400` | `new_algorithm_shadow_run` | `co2` | `False` | `review_bind_segmented_quality_candidate` | 2 |
| `-10/600` | `new_algorithm_shadow_run` | `co2` | `True` | `targeted_supplemental_resampling_candidate` | 0 |
| `-10/1000` | `new_algorithm_shadow_run` | `co2` | `False` | `review_bind_segmented_quality_candidate` | 2 |
| `40/30/30` | `new_algorithm_shadow_run` | `h2o` | `True` | `targeted_supplemental_resampling_candidate` | 0 |

## Candidate Evidence

| Point | Decision | Quality source | Path |
|---|---|---|---|
| `-20/0` | `segmented_quality_candidate_review_bind` | `frame_quality_summary` | `D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean\_handoff\new_algo_001_init_20260627\formal\co2_resume_20260628\co2_Tm10_Tm20_COM35_r1\p004_Tm20_0ppm_fit` |
| `-20/0` | `cross_family_reference_not_direct_bind` | `formal_open_flow_data_quality_by_analyzer` | `D:\gas_calibrator\_handoff\v1_5_formal_route_real_20260624\current6_20260624_r10_task_direct_continuous_nowrite\co2\p043_Tm20_0ppm_fit` |
| `-20/400` | `segmented_quality_candidate_review_bind` | `frame_quality_summary` | `D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean\_handoff\new_algo_001_init_20260627\formal\co2_resume_20260628\co2_Tm10_Tm20_COM35_r1\p005_Tm20_400ppm_fit` |
| `-20/400` | `cross_family_reference_not_direct_bind` | `formal_open_flow_data_quality_by_analyzer` | `D:\gas_calibrator\_handoff\v1_5_formal_route_real_20260624\current6_20260624_r10_task_direct_continuous_nowrite\co2\p044_Tm20_400ppm_fit` |
| `-20/1000` | `segmented_raw_only_qc_derivation_required` | `` | `D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean\_handoff\new_algo_001_init_20260627\formal\co2_resume_20260628\co2_Tm10_Tm20_COM35_r1\p006_Tm20_1000ppm_fit` |
| `-20/1000` | `segmented_quality_candidate_review_bind` | `frame_quality_summary` | `D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean\_handoff\new_algo_001_init_20260627\formal\co2_resume_20260628\co2_Tm10_Tm20_COM35_r1\p006_Tm20_1000ppm_fit_retry1` |
| `-20/1000` | `cross_family_reference_not_direct_bind` | `formal_open_flow_data_quality_by_analyzer` | `D:\gas_calibrator\_handoff\v1_5_formal_route_real_20260624\current6_20260624_r10_task_direct_continuous_nowrite\co2\p045_Tm20_1000ppm_fit` |
| `-10/0` | `segmented_quality_candidate_review_bind` | `frame_quality_summary` | `D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean\_handoff\new_algo_001_init_20260627\formal\co2_resume_20260628\co2_Tm10_Tm20_COM35_r1\p001_Tm10_0ppm_fit` |
| `-10/0` | `cross_family_reference_not_direct_bind` | `` | `D:\gas_calibrator\_handoff\v1_5_formal_route_real_20260624\current6_20260624_r10_task_direct_continuous_nowrite\co2\p040_Tm10_0ppm_fit` |
| `-10/400` | `segmented_quality_candidate_review_bind` | `frame_quality_summary` | `D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean\_handoff\new_algo_001_init_20260627\formal\co2_resume_20260628\co2_Tm10_Tm20_COM35_r1\p002_Tm10_400ppm_fit` |
| `-10/400` | `cross_family_reference_not_direct_bind` | `` | `D:\gas_calibrator\_handoff\v1_5_formal_route_real_20260624\current6_20260624_r10_task_direct_continuous_nowrite\co2\p041_Tm10_400ppm_fit` |
| `-10/1000` | `segmented_quality_candidate_review_bind` | `frame_quality_summary` | `D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean\_handoff\new_algo_001_init_20260627\formal\co2_resume_20260628\co2_Tm10_Tm20_COM35_r1\p003_Tm10_1000ppm_fit` |
| `-10/1000` | `cross_family_reference_not_direct_bind` | `formal_open_flow_data_quality_by_analyzer` | `D:\gas_calibrator\_handoff\v1_5_formal_route_real_20260624\current6_20260624_r10_task_direct_continuous_nowrite\co2\p042_Tm10_1000ppm_fit` |

## Checks

| Check | Status | Reason | Physical meaning |
|---|---|---|---|
| `missing_points_loaded` | `pass` | the audit starts from route-summarized missing expected points | This separates truly absent physical points from points that only lack QC evidence. |
| `segmented_quality_candidates_found` | `pass` | split runs can provide missing physical points, but must be reviewed before binding | A split-run point can close a missing-point gap only after matching physical state and QC are reviewed. |
| `supplemental_points_remain_explicit` | `review_required` | new algorithm supplemental points are candidate-specific requirements and must not be hidden by mature 45/13 replay | The extra -20/-10 600ppm and 40C/HGEN30C/30RH points are part of the new-algorithm candidate contract. |
| `unresolved_points_not_promoted` | `review_required` | missing physical points require segmented evidence review, QC derivation, or targeted resampling | Replay must not manufacture calibration evidence for physical points that were not observed. |
| `missing_point_audit_is_read_only` | `pass` | this audit writes only JSON/CSV/Markdown artifacts | A replay missing-point audit can plan evidence binding, not operate hardware or release data. |
