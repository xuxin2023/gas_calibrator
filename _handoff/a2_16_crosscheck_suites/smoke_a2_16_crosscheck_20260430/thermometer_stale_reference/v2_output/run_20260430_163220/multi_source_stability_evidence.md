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
- water/sample_ready: hold_time_gap | score 0.75 | hold False | partial_coverage False | candidate candidate_hold_time_gap | diff hold_time_gap_vs_candidate_hold_time_gap
- water/sample_ready: hold_time_gap | score 0.75 | hold False | partial_coverage False | candidate candidate_hold_time_gap | diff hold_time_gap_vs_candidate_hold_time_gap
- water/sample_ready: hold_time_gap | score 0.75 | hold False | partial_coverage False | candidate candidate_hold_time_gap | diff hold_time_gap_vs_candidate_hold_time_gap
- water/sample_ready: hold_time_gap | score 0.75 | hold False | partial_coverage False | candidate candidate_hold_time_gap | diff hold_time_gap_vs_candidate_hold_time_gap
- gas/sample_ready: hold_time_gap | score 0.70 | hold False | partial_coverage False | candidate candidate_hold_time_gap | diff hold_time_gap_vs_candidate_hold_time_gap
- gas/sample_ready: hold_time_gap | score 0.74 | hold False | partial_coverage False | candidate candidate_hold_time_gap | diff hold_time_gap_vs_candidate_hold_time_gap
- gas/sample_ready: hold_time_gap | score 0.74 | hold False | partial_coverage False | candidate candidate_hold_time_gap | diff hold_time_gap_vs_candidate_hold_time_gap
- gas/sample_ready: hold_time_gap | score 0.74 | hold False | partial_coverage False | candidate candidate_hold_time_gap | diff hold_time_gap_vs_candidate_hold_time_gap
- gas/sample_ready: hold_time_gap | score 0.70 | hold False | partial_coverage False | candidate candidate_hold_time_gap | diff hold_time_gap_vs_candidate_hold_time_gap
- gas/sample_ready: hold_time_gap | score 0.74 | hold False | partial_coverage False | candidate candidate_hold_time_gap | diff hold_time_gap_vs_candidate_hold_time_gap
- gas/sample_ready: hold_time_gap | score 0.74 | hold False | partial_coverage False | candidate candidate_hold_time_gap | diff hold_time_gap_vs_candidate_hold_time_gap
- gas/sample_ready: hold_time_gap | score 0.74 | hold False | partial_coverage False | candidate candidate_hold_time_gap | diff hold_time_gap_vs_candidate_hold_time_gap
- gas/sample_ready: hold_time_gap | score 0.70 | hold False | partial_coverage False | candidate candidate_hold_time_gap | diff hold_time_gap_vs_candidate_hold_time_gap
- gas/sample_ready: hold_time_gap | score 0.74 | hold False | partial_coverage False | candidate candidate_hold_time_gap | diff hold_time_gap_vs_candidate_hold_time_gap
- gas/sample_ready: hold_time_gap | score 0.74 | hold False | partial_coverage False | candidate candidate_hold_time_gap | diff hold_time_gap_vs_candidate_hold_time_gap
- gas/sample_ready: hold_time_gap | score 0.74 | hold False | partial_coverage False | candidate candidate_hold_time_gap | diff hold_time_gap_vs_candidate_hold_time_gap
- water/sample_ready: hold_time_gap | score 0.75 | hold False | partial_coverage False | candidate candidate_hold_time_gap | diff hold_time_gap_vs_candidate_hold_time_gap
- water/sample_ready: hold_time_gap | score 0.75 | hold False | partial_coverage False | candidate candidate_hold_time_gap | diff hold_time_gap_vs_candidate_hold_time_gap
- water/sample_ready: hold_time_gap | score 0.75 | hold False | partial_coverage False | candidate candidate_hold_time_gap | diff hold_time_gap_vs_candidate_hold_time_gap
- water/sample_ready: hold_time_gap | score 0.75 | hold False | partial_coverage False | candidate candidate_hold_time_gap | diff hold_time_gap_vs_candidate_hold_time_gap
- water/sample_ready: hold_time_gap | score 0.75 | hold False | partial_coverage False | candidate candidate_hold_time_gap | diff hold_time_gap_vs_candidate_hold_time_gap
- water/sample_ready: hold_time_gap | score 0.75 | hold False | partial_coverage False | candidate candidate_hold_time_gap | diff hold_time_gap_vs_candidate_hold_time_gap
- water/sample_ready: hold_time_gap | score 0.75 | hold False | partial_coverage False | candidate candidate_hold_time_gap | diff hold_time_gap_vs_candidate_hold_time_gap
- water/sample_ready: hold_time_gap | score 0.75 | hold False | partial_coverage False | candidate candidate_hold_time_gap | diff hold_time_gap_vs_candidate_hold_time_gap
- gas/sample_ready: hold_time_gap | score 0.74 | hold False | partial_coverage False | candidate candidate_hold_time_gap | diff hold_time_gap_vs_candidate_hold_time_gap
- gas/sample_ready: hold_time_gap | score 0.74 | hold False | partial_coverage False | candidate candidate_hold_time_gap | diff hold_time_gap_vs_candidate_hold_time_gap
- gas/sample_ready: hold_time_gap | score 0.74 | hold False | partial_coverage False | candidate candidate_hold_time_gap | diff hold_time_gap_vs_candidate_hold_time_gap
- gas/sample_ready: hold_time_gap | score 0.74 | hold False | partial_coverage False | candidate candidate_hold_time_gap | diff hold_time_gap_vs_candidate_hold_time_gap
- gas/sample_ready: hold_time_gap | score 0.74 | hold False | partial_coverage False | candidate candidate_hold_time_gap | diff hold_time_gap_vs_candidate_hold_time_gap
- gas/sample_ready: hold_time_gap | score 0.74 | hold False | partial_coverage False | candidate candidate_hold_time_gap | diff hold_time_gap_vs_candidate_hold_time_gap
- gas/sample_ready: hold_time_gap | score 0.74 | hold False | partial_coverage False | candidate candidate_hold_time_gap | diff hold_time_gap_vs_candidate_hold_time_gap
- gas/sample_ready: hold_time_gap | score 0.74 | hold False | partial_coverage False | candidate candidate_hold_time_gap | diff hold_time_gap_vs_candidate_hold_time_gap
- gas/sample_ready: hold_time_gap | score 0.74 | hold False | partial_coverage False | candidate candidate_hold_time_gap | diff hold_time_gap_vs_candidate_hold_time_gap
- gas/sample_ready: hold_time_gap | score 0.74 | hold False | partial_coverage False | candidate candidate_hold_time_gap | diff hold_time_gap_vs_candidate_hold_time_gap
- gas/sample_ready: hold_time_gap | score 0.74 | hold False | partial_coverage False | candidate candidate_hold_time_gap | diff hold_time_gap_vs_candidate_hold_time_gap
- gas/sample_ready: hold_time_gap | score 0.74 | hold False | partial_coverage False | candidate candidate_hold_time_gap | diff hold_time_gap_vs_candidate_hold_time_gap

## Artifact Paths

- json: D:\gas_calibrator\_handoff\a2_16_crosscheck_suites\smoke_a2_16_crosscheck_20260430\thermometer_stale_reference\v2_output\run_20260430_163220\multi_source_stability_evidence.json
- markdown: D:\gas_calibrator\_handoff\a2_16_crosscheck_suites\smoke_a2_16_crosscheck_20260430\thermometer_stale_reference\v2_output\run_20260430_163220\multi_source_stability_evidence.md
