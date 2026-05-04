# Measurement Phase Coverage Report

- title: Measurement Phase Coverage Report
- role: diagnostic_analysis
- reviewer_note: Step 2 tail / Stage 3 bridge reviewer evidence for richer simulation coverage only. This is readiness mapping for measurement-core evidence and not a runtime control surface.

## Boundary

- Step 2 tail / Stage 3 bridge
- simulation / offline / headless only
- not real acceptance
- cannot replace real metrology validation
- shadow evaluation only
- does not modify live sampling gate by default

## Phase Coverage

- ambient/ambient_diagnostic: gap | payload not_available | provenance gap | decision gap_coverage | hold not_applicable
  layers available -- | missing reference, analyzer_raw, output, data_quality
  channels available -- | missing temperature_c, pressure_hpa, ref_signal, co2_ppm, frame_has_data, frame_usable
  readiness impact scope, decision rule, method confirmation, validation matrix, uncertainty inputs, traceability stub remains open because this phase has only gap reviewer coverage | next artifacts scope_definition_pack, scope_readiness_summary, decision_rule_profile, uncertainty_model, uncertainty_input_set, sensitivity_coefficient_set
  blockers Phase remains gap; richer simulated payload evidence is still missing, Missing signal layers: reference, analyzer_raw, output, data_quality, Linked method confirmation items remain open: Ambient baseline stabilization rule, Ambient diagnostic decision threshold, Ambient diagnostic drift review, Linked uncertainty inputs remain open: Ambient pressure baseline, Ambient humidity baseline, Ambient temperature baseline, Linked traceability nodes remain stub-only: Ambient environment reference chain, Ambient pressure reference link, Ambient climate baseline stub | non-claim simulation / synthetic reviewer evidence only | not real acceptance | not live gate | not compliance claim | not accreditation claim
  method items Ambient baseline stabilization rule, Ambient diagnostic decision threshold, Ambient diagnostic drift review | uncertainty inputs Ambient pressure baseline, Ambient humidity baseline, Ambient temperature baseline
  traceability nodes Ambient environment reference chain, Ambient pressure reference link, Ambient climate baseline stub | gap ambient_baseline_gap / high
  reviewer next step Confirm ambient diagnostic baseline method items first, then add ambient pressure/humidity/temperature uncertainty inputs, then tie the ambient references into the traceability stub while keeping this phase reviewer-only.
- water/preseal: trace_only_not_evaluated | payload trace_only | provenance simulated_trace_only | decision actual_trace_observed | hold trace_only_not_evaluated
  layers available -- | missing reference, analyzer_raw, output, data_quality
  channels available -- | missing temperature_c, dew_point_c, pressure_hpa, h2o_ratio_raw, h2o_signal, ref_signal
  readiness impact scope, decision rule, method confirmation, validation matrix, uncertainty inputs, traceability stub remains open because this phase is still trace-only and not payload-evaluated | next artifacts scope_definition_pack, scope_readiness_summary, decision_rule_profile, uncertainty_model, uncertainty_input_set, sensitivity_coefficient_set
  blockers Phase is still trace-only; simulated payload layers have not been promoted yet, Missing signal layers: reference, analyzer_raw, output, data_quality, Linked method confirmation items remain open: Water preseal window definition, Water route conditioning repeatability, Water preseal release criteria, Linked uncertainty inputs remain open: Humidity reference window, Preseal pressure term, Preseal temperature term, Linked traceability nodes remain stub-only: Humidity reference chain, Dew-point reference link, Preseal conditioning window stub, Preseal partial is an honesty boundary, not a measurement-core bug | non-claim simulation / synthetic reviewer evidence only | not real acceptance | not live gate | not compliance claim | not accreditation claim
  method items Water preseal window definition, Water route conditioning repeatability, Water preseal release criteria | uncertainty inputs Humidity reference window, Preseal pressure term, Preseal temperature term
  traceability nodes Humidity reference chain, Dew-point reference link, Preseal conditioning window stub | gap conditioning_window_partial_payload / high
  reviewer next step Confirm the water preseal window method first, then add the preseal pressure/temperature uncertainty inputs, then tie the humidity and dew-point references into the traceability stub while keeping preseal partial explicit as payload-partial.
- gas/preseal: model_only | payload not_available | provenance model_only | decision model_only_coverage | hold not_applicable
  layers available -- | missing reference, analyzer_raw, output, data_quality
  channels available -- | missing temperature_c, pressure_hpa, co2_ratio_raw, co2_signal, ref_signal, co2_ppm
  readiness impact scope, decision rule, method confirmation, validation matrix, uncertainty inputs, traceability stub remains open because this phase has only model_only reviewer coverage | next artifacts scope_definition_pack, scope_readiness_summary, decision_rule_profile, uncertainty_model, uncertainty_input_set, sensitivity_coefficient_set
  blockers Phase remains model_only; richer simulated payload evidence is still missing, Missing signal layers: reference, analyzer_raw, output, data_quality, Linked method confirmation items remain open: Gas preseal window definition, Gas route conditioning repeatability, Gas preseal release criteria, Linked uncertainty inputs remain open: Reference gas window, Preseal pressure term, Preseal temperature term, Linked traceability nodes remain stub-only: Standard gas chain, Pressure reference link, Preseal conditioning window stub, Preseal partial is an honesty boundary, not a measurement-core bug | non-claim simulation / synthetic reviewer evidence only | not real acceptance | not live gate | not compliance claim | not accreditation claim
  method items Gas preseal window definition, Gas route conditioning repeatability, Gas preseal release criteria | uncertainty inputs Reference gas window, Preseal pressure term, Preseal temperature term
  traceability nodes Standard gas chain, Pressure reference link, Preseal conditioning window stub | gap conditioning_window_partial_payload / high
  reviewer next step Confirm the gas preseal window method first, then add the preseal pressure/temperature uncertainty inputs, then tie the standard-gas and pressure references into the traceability stub while keeping preseal partial explicit as payload-partial.
- water/pressure_stable: trace_only_not_evaluated | payload trace_only | provenance simulated_trace_only | decision actual_trace_observed | hold trace_only_not_evaluated
  layers available -- | missing reference, analyzer_raw, output, data_quality
  channels available -- | missing temperature_c, dew_point_c, pressure_hpa, h2o_ratio_raw, h2o_signal, ref_signal
  readiness impact reference asset / certificate, certificate lifecycle, pre-run readiness gate, validation matrix, traceability, uncertainty / method remains open because this phase is still trace-only and not payload-evaluated | next artifacts reference_asset_registry, certificate_lifecycle_summary, certificate_readiness_summary, pre_run_readiness_gate, uncertainty_model, uncertainty_input_set
  blockers Phase is still trace-only; simulated payload layers have not been promoted yet, Missing signal layers: reference, analyzer_raw, output, data_quality, Linked method confirmation items remain open: Water pressure stabilization hold confirmation, Linked uncertainty inputs remain open: Humidity reference, Pressure reference, Temperature reference, Linked traceability nodes remain stub-only: Humidity reference chain, Dew-point reference link, Pressure reference link | non-claim simulation / synthetic reviewer evidence only | not real acceptance | not live gate | not compliance claim | not accreditation claim
  method items Water pressure stabilization hold confirmation | uncertainty inputs Humidity reference, Pressure reference, Temperature reference
  traceability nodes Humidity reference chain, Dew-point reference link, Pressure reference link | gap payload_complete_synthetic_reviewer_anchor / info
  reviewer next step Use the water pressure-stable payload as the synthetic reviewer anchor, then keep certificate and traceability closure in readiness-only artifacts until released reference evidence exists.
- gas/pressure_stable: model_only | payload not_available | provenance model_only | decision model_only_coverage | hold not_applicable
  layers available -- | missing reference, analyzer_raw, output, data_quality
  channels available -- | missing temperature_c, pressure_hpa, co2_ratio_raw, co2_signal, ref_signal, co2_ppm
  readiness impact reference asset / certificate, certificate lifecycle, pre-run readiness gate, validation matrix, traceability, uncertainty / method remains open because this phase has only model_only reviewer coverage | next artifacts reference_asset_registry, certificate_lifecycle_summary, certificate_readiness_summary, pre_run_readiness_gate, uncertainty_model, uncertainty_input_set
  blockers Phase remains model_only; richer simulated payload evidence is still missing, Missing signal layers: reference, analyzer_raw, output, data_quality, Linked method confirmation items remain open: Gas pressure stabilization hold confirmation, Linked uncertainty inputs remain open: Reference gas value, Pressure reference, Temperature reference, Linked traceability nodes remain stub-only: Standard gas chain, Pressure reference link, Temperature reference link | non-claim simulation / synthetic reviewer evidence only | not real acceptance | not live gate | not compliance claim | not accreditation claim
  method items Gas pressure stabilization hold confirmation | uncertainty inputs Reference gas value, Pressure reference, Temperature reference
  traceability nodes Standard gas chain, Pressure reference link, Temperature reference link | gap payload_complete_synthetic_reviewer_anchor / info
  reviewer next step Use the gas pressure-stable payload as the synthetic reviewer anchor, then keep certificate and traceability closure in readiness-only artifacts until released reference evidence exists.
- ambient/sample_ready: gap | payload not_available | provenance gap | decision gap_coverage | hold not_applicable
  layers available -- | missing reference, analyzer_raw, output, data_quality
  channels available -- | missing temperature_c, pressure_hpa, ref_signal, co2_ppm, frame_has_data, frame_usable
  readiness impact scope, method confirmation, validation matrix, uncertainty inputs, traceability stub remains open because this phase has only gap reviewer coverage | next artifacts scope_definition_pack, scope_readiness_summary, decision_rule_profile, uncertainty_model, uncertainty_input_set, sensitivity_coefficient_set
  blockers Phase remains gap; richer simulated payload evidence is still missing, Missing signal layers: reference, analyzer_raw, output, data_quality, Linked method confirmation items remain open: Ambient sample-ready dwell confirmation, Ambient sample release criteria, Linked uncertainty inputs remain open: Ambient stabilization window, Ambient pressure drift allowance, Linked traceability nodes remain stub-only: Ambient environment reference chain, Sample release trace stub | non-claim simulation / synthetic reviewer evidence only | not real acceptance | not live gate | not compliance claim | not accreditation claim
  method items Ambient sample-ready dwell confirmation, Ambient sample release criteria | uncertainty inputs Ambient stabilization window, Ambient pressure drift allowance
  traceability nodes Ambient environment reference chain, Sample release trace stub | gap ambient_sample_ready_gap / high
  reviewer next step Confirm ambient sample-ready dwell and release method items first, then add stabilization uncertainty inputs, then tie the ambient release references into the traceability stub while keeping this phase reviewer-only.
- water/sample_ready: actual_simulated_run_with_payload_complete | payload complete | provenance actual_simulated_payload | decision hold_time_gap | hold hold_time_gap
  layers available reference, analyzer_raw, output, data_quality | missing --
  channels available temperature_c, dew_point_c, pressure_hpa, pressure_gauge_hpa, pressure_reference_status, co2_ratio_raw | missing --
  readiness impact scope, method confirmation, validation matrix, uncertainty inputs, traceability stub linkage is available from synthetic payload-backed reviewer evidence | next artifacts scope_definition_pack, scope_readiness_summary, decision_rule_profile, uncertainty_model, uncertainty_input_set, sensitivity_coefficient_set
  blockers -- | non-claim simulation / synthetic reviewer evidence only | not real acceptance | not live gate | not compliance claim | not accreditation claim
  method items Water sample-ready dwell confirmation, Water sample release criteria | uncertainty inputs Humidity stabilization window, Pressure settling allowance
  traceability nodes Humidity reference chain, Sample release trace stub | gap water_sample_ready_payload_complete_anchor / info
  reviewer next step Use the water sample-ready payload as synthetic reviewer release evidence, then keep uncertainty and traceability closure in readiness-only artifacts.
- gas/sample_ready: model_only | payload not_available | provenance model_only | decision model_only_coverage | hold not_applicable
  layers available -- | missing reference, analyzer_raw, output, data_quality
  channels available -- | missing temperature_c, pressure_hpa, co2_ratio_raw, co2_signal, ref_signal, co2_ppm
  readiness impact scope, method confirmation, validation matrix, uncertainty inputs, traceability stub remains open because this phase has only model_only reviewer coverage | next artifacts scope_definition_pack, scope_readiness_summary, decision_rule_profile, uncertainty_model, uncertainty_input_set, sensitivity_coefficient_set
  blockers Phase remains model_only; richer simulated payload evidence is still missing, Missing signal layers: reference, analyzer_raw, output, data_quality, Linked method confirmation items remain open: Gas sample-ready dwell confirmation, Gas sample release criteria, Linked uncertainty inputs remain open: Reference gas stabilization window, Pressure settling allowance, Linked traceability nodes remain stub-only: Standard gas chain, Sample release trace stub | non-claim simulation / synthetic reviewer evidence only | not real acceptance | not live gate | not compliance claim | not accreditation claim
  method items Gas sample-ready dwell confirmation, Gas sample release criteria | uncertainty inputs Reference gas stabilization window, Pressure settling allowance
  traceability nodes Standard gas chain, Sample release trace stub | gap gas_sample_ready_model_only_gap / medium
  reviewer next step Confirm gas sample-ready dwell and release method items first, then add reference-gas/pressure uncertainty inputs, then tie the release references into the traceability stub while keeping this phase reviewer-only.
- system/recovery_retry: test_only | payload not_available | provenance test_only | decision test_only_coverage | hold not_applicable
  layers available -- | missing reference, analyzer_raw, output, data_quality
  channels available -- | missing temperature_c, pressure_hpa, ref_signal, frame_status, co2_ppm, frame_has_data
  readiness impact software validation, verification digest, audit, method confirmation, uncertainty inputs, traceability stub remains open because this phase has only test_only reviewer coverage | next artifacts verification_digest, verification_rollup, software_validation_traceability_matrix, requirement_design_code_test_links, validation_evidence_index, change_impact_summary
  blockers Phase remains test_only; richer simulated payload evidence is still missing, Missing signal layers: reference, analyzer_raw, output, data_quality, Linked method confirmation items remain open: Recovery retry scenario confirmation, Safe recovery procedure confirmation, Linked uncertainty inputs remain open: Retry timing tolerance, Fault capture debounce window, Linked traceability nodes remain stub-only: Software event log chain, Recovery audit trail stub | non-claim simulation / synthetic reviewer evidence only | not real acceptance | not live gate | not compliance claim | not accreditation claim
  method items Recovery retry scenario confirmation, Safe recovery procedure confirmation | uncertainty inputs Retry timing tolerance, Fault capture debounce window
  traceability nodes Software event log chain, Recovery audit trail stub | gap recovery_retry_test_only_gap / medium
  reviewer next step Keep recovery/retry in test-only reviewer coverage until synthetic payload captures retry timing, fault capture, and audit-trace linkage.

## Artifact Paths

- json: D:\gas_calibrator\_handoff\a2_16_crosscheck_suites\smoke_a2_16_crosscheck_20260430\relay_stuck_channel_causes_route_mismatch\v2_output\run_20260430_163159\measurement_phase_coverage_report.json
- markdown: D:\gas_calibrator\_handoff\a2_16_crosscheck_suites\smoke_a2_16_crosscheck_20260430\relay_stuck_channel_causes_route_mismatch\v2_output\run_20260430_163159\measurement_phase_coverage_report.md
