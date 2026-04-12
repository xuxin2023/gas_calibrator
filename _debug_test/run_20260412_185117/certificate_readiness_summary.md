# Certificate Readiness Summary

- summary: reference asset / certificate readiness | assets 8 | certificate gaps 2 | intermediate-check gaps 2 | lot-binding gaps 1 | open OOT 1
- assets tracked: 8
- certificate missing or expired: 2
- intermediate check missing or overdue: 2
- lot binding gaps: 1
- open out-of-tolerance: 1
- missing evidence: no released certificate files attached | no intermediate check execution evidence attached | lot binding and substitute approval remain reviewer-only | traceability chain remains readiness-only
- reference_asset_registry: d:\gas_calibrator\_debug_test\run_20260412_185117\reference_asset_registry.json
- metrology_traceability_stub: d:\gas_calibrator\_debug_test\run_20260412_185117\metrology_traceability_stub.json
- boundary: Step 2 reviewer readiness only
- boundary: simulation / offline / headless only
- boundary: file-artifact-first reviewer evidence
- boundary: not real acceptance
- boundary: not compliance claim
- boundary: not accreditation claim
- boundary: cannot replace real metrology validation

## Readiness Linkage

- anchor_id: certificate-readiness-summary
- readiness_status: certificate_readiness_summary_readiness_stub
- linked_artifact_refs: reference_asset_registry | certificate_lifecycle_summary | pre_run_readiness_gate | metrology_traceability_stub
- linked_measurement_phases: water/pressure_stable=gap | gas/pressure_stable=model_only
- linked_measurement_gaps: water/pressure_stable: Missing-layer reason: reference: no simulated evidence captured for this layer | analyzer_raw: no simulated evidence captured for this layer | output: no simulated evidence captured for this layer | data_quality: no simulated evidence captured for this layer | gas/pressure_stable: Missing-layer reason: reference: model_only coverage only; this layer has not been promoted into simulated payload evidence | analyzer_raw: model_only coverage only; this layer has not been promoted into simulated payload evidence | output: model_only coverage only; this layer has not been promoted into simulated payload evidence | data_quality: model_only coverage only; this layer has not been promoted into simulated payload evidence
- linked_method_confirmation_items: Water pressure stabilization hold confirmation | Gas pressure stabilization hold confirmation
- linked_uncertainty_inputs: Humidity reference | Pressure reference | Temperature reference | Reference gas value
- linked_traceability_nodes: Humidity reference chain | Dew-point reference link | Pressure reference link | Standard gas chain | Temperature reference link
- preseal_partial_gap: --
- gap_reason: water/pressure_stable: Missing-layer reason: reference: no simulated evidence captured for this layer | analyzer_raw: no simulated evidence captured for this layer | output: no simulated evidence captured for this layer | data_quality: no simulated evidence captured for this layer | gas/pressure_stable: Missing-layer reason: reference: model_only coverage only; this layer has not been promoted into simulated payload evidence | analyzer_raw: model_only coverage only; this layer has not been promoted into simulated payload evidence | output: model_only coverage only; this layer has not been promoted into simulated payload evidence | data_quality: model_only coverage only; this layer has not been promoted into simulated payload evidence
- missing_evidence: no released certificate files attached | no intermediate check execution evidence attached | lot binding and substitute approval remain reviewer-only | traceability chain remains readiness-only
- blockers: Certificate files and intermediate checks remain missing | Traceability chain stays reviewer-facing only | water/pressure_stable: Phase remains gap; richer simulated payload evidence is still missing | Missing signal layers: reference, analyzer_raw, output, data_quality | Linked method confirmation items remain open: Water pressure stabilization hold confirmation | Linked uncertainty inputs remain open: Humidity reference, Pressure reference, Temperature reference | Linked traceability nodes remain stub-only: Humidity reference chain, Dew-point reference link, Pressure reference link | gas/pressure_stable: Phase remains model_only; richer simulated payload evidence is still missing | Missing signal layers: reference, analyzer_raw, output, data_quality | Linked method confirmation items remain open: Gas pressure stabilization hold confirmation | Linked uncertainty inputs remain open: Reference gas value, Pressure reference, Temperature reference | Linked traceability nodes remain stub-only: Standard gas chain, Pressure reference link, Temperature reference link
- next_required_artifacts: pre_run_readiness_gate | metrology_traceability_stub | reference_asset_registry | certificate_lifecycle_summary | certificate_readiness_summary | uncertainty_model | uncertainty_input_set | sensitivity_coefficient_set | budget_case | uncertainty_golden_cases | uncertainty_report_pack | uncertainty_digest | uncertainty_rollup | uncertainty_budget_stub | method_confirmation_matrix | route_specific_validation_matrix | validation_run_set | verification_digest | verification_rollup | uncertainty_method_readiness_summary
- reviewer_next_step: Use the water pressure-stable payload as the synthetic reviewer anchor, then keep certificate and traceability closure in readiness-only artifacts until released reference evidence exists. | Use the gas pressure-stable payload as the synthetic reviewer anchor, then keep certificate and traceability closure in readiness-only artifacts until released reference evidence exists.
- boundary_digest: Step 2 reviewer readiness only | simulation / offline / headless only | file-artifact-first reviewer evidence | not real acceptance | not compliance claim | not accreditation claim | cannot replace real metrology validation
- non_claim_digest: Step 2 reviewer readiness only | simulation / offline / headless only | file-artifact-first reviewer evidence | not real acceptance | not compliance claim | not accreditation claim | cannot replace real metrology validation
