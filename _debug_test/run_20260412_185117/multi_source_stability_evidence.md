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

- reference: complete | available temperature_c, dew_point_c, pressure_hpa | missing --
- analyzer_raw: partial | available ref_signal, co2_signal, h2o_signal | missing co2_ratio_raw
- output: complete | available co2_ppm, h2o_mmol, co2_ratio_f, h2o_ratio_f | missing --
- data_quality: complete | available frame_has_data, frame_usable, sample_index, stability_time_s, total_time_s | missing --

## Decisions

- gas/sample_ready: partial_coverage_gap | score 0.91 | hold True | partial_coverage True

## Artifact Paths

- json: d:\gas_calibrator\_debug_test\run_20260412_185117\multi_source_stability_evidence.json
- markdown: d:\gas_calibrator\_debug_test\run_20260412_185117\multi_source_stability_evidence.md
