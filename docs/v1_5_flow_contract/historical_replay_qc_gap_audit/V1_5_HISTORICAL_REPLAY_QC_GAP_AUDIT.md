# V1.5 Historical Replay QC Gap Audit

- schema: `v1_5_historical_replay_qc_gap_audit_v1`
- status: `review_required`
- blocker_count: `0`
- review_required_count: `1`
- missing_qc_point_count: `3`
- direct_bindable_point_count: `2`
- unresolved_point_count: `1`
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

| Point | Family | Route | Recommendation | Candidate count |
|---|---|---|---|---:|
| `p040_Tm10_0ppm_fit` | `mature_0620_legacy_ratio` | `co2` | `bind_same_run_reject_only_quality` | 3 |
| `p041_Tm10_400ppm_fit` | `mature_0620_legacy_ratio` | `co2` | `bind_same_run_reject_only_quality` | 3 |
| `p017_T20_200ppm_fit` | `new_algorithm_shadow_run` | `co2` | `cross_run_reference_only_find_same_run_qc_or_retry` | 2 |

## Candidate Evidence

| Point | Candidate type | Decision | Fit? | Path |
|---|---|---|---|---|
| `p040_Tm10_0ppm_fit` | `same_run_queue_manifest_with_quality` | `bindable_reject_only_not_fit` | `False` | `D:\gas_calibrator\_handoff\v1_5_formal_route_real_20260624\current6_20260624_r10_task_direct_continuous_nowrite\co2\current6_20260624_r10_co2_continuous_full45_task_direct\quality_backfill_20260625\queue_manifest_with_quality.csv` |
| `p040_Tm10_0ppm_fit` | `raw_sampling_evidence_without_qc` | `not_bindable_raw_only` | `None` | `D:\gas_calibrator\_handoff\v1_5_formal_route_real_20260624\current6_20260624_r10_task_direct_continuous_nowrite\co2\p040_Tm10_0ppm_fit` |
| `p040_Tm10_0ppm_fit` | `frame_quality_summary` | `cross_run_reference_not_direct_bind` | `None` | `D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean\_handoff\new_algo_001_init_20260627\formal\co2_resume_20260628\co2_Tm10_Tm20_COM35_r1\p001_Tm10_0ppm_fit\frame_quality_summary.csv` |
| `p041_Tm10_400ppm_fit` | `same_run_queue_manifest_with_quality` | `bindable_reject_only_not_fit` | `False` | `D:\gas_calibrator\_handoff\v1_5_formal_route_real_20260624\current6_20260624_r10_task_direct_continuous_nowrite\co2\current6_20260624_r10_co2_continuous_full45_task_direct\quality_backfill_20260625\queue_manifest_with_quality.csv` |
| `p041_Tm10_400ppm_fit` | `raw_sampling_evidence_without_qc` | `not_bindable_raw_only` | `None` | `D:\gas_calibrator\_handoff\v1_5_formal_route_real_20260624\current6_20260624_r10_task_direct_continuous_nowrite\co2\p041_Tm10_400ppm_fit` |
| `p041_Tm10_400ppm_fit` | `frame_quality_summary` | `cross_run_reference_not_direct_bind` | `None` | `D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean\_handoff\new_algo_001_init_20260627\formal\co2_resume_20260628\co2_Tm10_Tm20_COM35_r1\p002_Tm10_400ppm_fit\frame_quality_summary.csv` |
| `p017_T20_200ppm_fit` | `raw_sampling_evidence_without_qc` | `not_bindable_raw_only` | `None` | `D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean\_handoff\new_algo_001_init_20260627\formal\co2_open_flow_0620_mature_20260627_r1\co2_r2\p017_T20_200ppm_fit` |
| `p017_T20_200ppm_fit` | `formal_open_flow_data_quality_by_analyzer` | `cross_run_reference_not_direct_bind` | `True` | `D:\gas_calibrator\_handoff\v1_5_formal_route_real_20260624\current6_20260624_r10_task_direct_continuous_nowrite\co2\p017_T20_200ppm_fit\formal_open_flow_data_quality_by_analyzer.csv` |

## Checks

| Check | Status | Reason | Physical meaning |
|---|---|---|---|
| `missing_qc_points_loaded` | `pass` | the audit starts from binder-discovered missing QC points | The audit must explain the exact replay blocker instead of scanning unrelated historical data. |
| `same_run_reject_only_bindings_identified` | `pass` | queue-manifest C_reject rows can close missing-QC metadata but cannot enter calibration fit | A failed or C_reject point can be traceable evidence without becoming a calibration point. |
| `cross_run_quality_not_directly_bound` | `pass` | quality from another family/run is reported as reference and never used as direct replacement | Same physical gas point in another run can guide diagnosis, but it does not prove this device/run was stable. |
| `unresolved_gaps_remain_review_required` | `review_required` | points with only raw IO or cross-run reference still need QC derivation, retry evidence, or targeted rerun | This keeps replay from silently manufacturing fit-ready evidence. |
| `qc_gap_audit_is_read_only` | `pass` | this audit writes only JSON/CSV/Markdown artifacts | QC gap closure planning must not become hidden hardware control or release authorization. |
