# Multi-source Stability Evidence

- title: Multi-source Stability Evidence
- role: diagnostic_analysis
- reviewer_note: Step 2 tail / Stage 3 bridge reviewer evidence. Shadow evaluation only and does not modify live sampling gate by default.

## Boundary

- Step 2 tail / Stage 3 bridge
- simulation / offline / headless only
- not real acceptance
- cannot replace real metrology validation
- shadow evaluation only
- does not modify live sampling gate by default

## Signal Group Coverage

- reference: complete | available temperature_c, dew_point_c, pressure_hpa, pressure_gauge_hpa, pressure_reference_status | missing --
- analyzer_raw: complete | available co2_ratio_raw, h2o_ratio_raw, ref_signal, co2_signal, h2o_signal | missing --
- output: complete | available co2_ppm, h2o_mmol, co2_ratio_f, h2o_ratio_f | missing --
- data_quality: complete | available frame_has_data, frame_usable, sample_index, stability_time_s, total_time_s | missing --

## Decisions

- water/sample_ready: hold_time_gap | score 0.75 | hold False | partial_coverage False | candidate candidate_hold_time_gap | diff hold_time_gap_vs_candidate_hold_time_gap
- water/sample_ready: hold_time_gap | score 0.75 | hold False | partial_coverage False | candidate candidate_hold_time_gap | diff hold_time_gap_vs_candidate_hold_time_gap
- water/sample_ready: hold_time_gap | score 0.75 | hold False | partial_coverage False | candidate candidate_hold_time_gap | diff hold_time_gap_vs_candidate_hold_time_gap
- water/sample_ready: hold_time_gap | score 0.75 | hold False | partial_coverage False | candidate candidate_hold_time_gap | diff hold_time_gap_vs_candidate_hold_time_gap

## Artifact Paths

- json: D:\gas_calibrator\_handoff\a2_12r_i_suite_reports\smoke_a2_12r_i\relay_stuck_channel_causes_route_mismatch\v2_output\run_20260430_121048\multi_source_stability_evidence.json
- markdown: D:\gas_calibrator\_handoff\a2_12r_i_suite_reports\smoke_a2_12r_i\relay_stuck_channel_causes_route_mismatch\v2_output\run_20260430_121048\multi_source_stability_evidence.md
