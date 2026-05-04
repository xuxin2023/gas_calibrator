# Certificate Lifecycle Summary

- certificate_validity_summary: valid_certificate 1 | reviewer_stub_only 4 | expired_certificate 1 | missing_certificate 1
- lot_binding_summary: missing_binding_approval 1
- intermediate_check_summary: pass 3 | due_soon 2 | overdue 1 | missing_intermediate_check 1
- calibration_source_summary: external_calibration 4 | internal_calibration 3 | not_applicable 1
- certificate_file_link_summary: linked assets 5/6 | files 5
- certificate: CERT-SG-2026-001 | asset_id=asset-standard-gas-sg-001 | status=reviewer_stub_only | source=external_calibration
- certificate: CERT-HG-2024-017 | asset_id=asset-humidity-generator-hg-001 | status=expired_certificate | source=external_calibration
- certificate: CERT-DP-MISSING | asset_id=asset-dewpoint-meter-dp-001 | status=missing_certificate | source=external_calibration
- certificate: CERT-PG-2026-021 | asset_id=asset-pressure-gauge-pg-001 | status=reviewer_stub_only | source=internal_calibration
- certificate: CERT-TC-2026-004 | asset_id=asset-temp-chamber-tc-001 | status=reviewer_stub_only | source=internal_calibration
- certificate: CERT-TH-2026-031 | asset_id=asset-thermometer-th-001 | status=valid_certificate | source=external_calibration
- certificate: CERT-PC-2026-008 | asset_id=asset-pressure-controller-pc-001 | status=reviewer_stub_only | source=internal_calibration
- lot_binding: LOT-SG-2026-CO2-01 | binding_status=missing_binding_approval | usage=lot_usage_sidecar_pending
- non_claim_note: Certificate lifecycle rows are simulated reviewer artifacts only and cannot support real acceptance.

## Readiness Linkage

- anchor_id: certificate-lifecycle-summary
- readiness_status: certificate_lifecycle_summary_readiness_stub
- linked_artifact_refs: scope_definition_pack | decision_rule_profile | reference_asset_registry | certificate_readiness_summary | pre_run_readiness_gate
- linked_measurement_phases: water/pressure_stable=gap | gas/pressure_stable=trace_only
- linked_measurement_gaps: water/pressure_stable: Missing-layer reason: reference: no simulated evidence captured for this layer | analyzer_raw: no simulated evidence captured for this layer | output: no simulated evidence captured for this layer | data_quality: no simulated evidence captured for this layer | gas/pressure_stable: Missing-layer reason: reference: phase currently has trace bucket only; no simulated sample payload captured for this layer | analyzer_raw: phase currently has trace bucket only; no simulated sample payload captured for this layer | output: phase currently has trace bucket only; no simulated sample payload captured for this layer | data_quality: phase currently has trace bucket only; no simulated sample payload captured for this layer
- linked_method_confirmation_items: Water pressure stabilization hold confirmation | Gas pressure stabilization hold confirmation
- linked_uncertainty_inputs: Humidity reference | Pressure reference | Temperature reference | Reference gas value
- linked_traceability_nodes: Humidity reference chain | Dew-point reference link | Pressure reference link | Standard gas chain | Temperature reference link
- preseal_partial_gap: --
- gap_reason: water/pressure_stable: Missing-layer reason: reference: no simulated evidence captured for this layer | analyzer_raw: no simulated evidence captured for this layer | output: no simulated evidence captured for this layer | data_quality: no simulated evidence captured for this layer | gas/pressure_stable: Missing-layer reason: reference: phase currently has trace bucket only; no simulated sample payload captured for this layer | analyzer_raw: phase currently has trace bucket only; no simulated sample payload captured for this layer | output: phase currently has trace bucket only; no simulated sample payload captured for this layer | data_quality: phase currently has trace bucket only; no simulated sample payload captured for this layer
- missing_evidence: released lifecycle records and lot bindings remain incomplete | internal/external lifecycle closure remains reviewer mapping only
- blockers: certificate lifecycle remains reviewer stub only | lot binding / intermediate check / out-of-tolerance closure is not released for formal claim | water/pressure_stable: Phase remains gap; richer simulated payload evidence is still missing | Missing signal layers: reference, analyzer_raw, output, data_quality | Linked method confirmation items remain open: Water pressure stabilization hold confirmation | Linked uncertainty inputs remain open: Humidity reference, Pressure reference, Temperature reference | Linked traceability nodes remain stub-only: Humidity reference chain, Dew-point reference link, Pressure reference link | gas/pressure_stable: Phase is still trace-only; simulated payload layers have not been promoted yet | Missing signal layers: reference, analyzer_raw, output, data_quality | Linked method confirmation items remain open: Gas pressure stabilization hold confirmation | Linked uncertainty inputs remain open: Reference gas value, Pressure reference, Temperature reference | Linked traceability nodes remain stub-only: Standard gas chain, Pressure reference link, Temperature reference link
- next_required_artifacts: certificate_readiness_summary | pre_run_readiness_gate | reference_asset_registry | certificate_lifecycle_summary | uncertainty_model | uncertainty_input_set | sensitivity_coefficient_set | budget_case | uncertainty_golden_cases | uncertainty_report_pack | uncertainty_digest | uncertainty_rollup | metrology_traceability_stub | uncertainty_budget_stub | method_confirmation_matrix | route_specific_validation_matrix | validation_run_set | verification_digest | verification_rollup | uncertainty_method_readiness_summary
- reviewer_next_step: Use the water pressure-stable payload as the synthetic reviewer anchor, then keep certificate and traceability closure in readiness-only artifacts until released reference evidence exists. | Use the gas pressure-stable payload as the synthetic reviewer anchor, then keep certificate and traceability closure in readiness-only artifacts until released reference evidence exists.
- boundary_digest: Step 2 reviewer readiness only | simulation / offline / headless only | file-artifact-first reviewer evidence | not real acceptance | not compliance claim | not accreditation claim | cannot replace real metrology validation
- non_claim_digest: Step 2 reviewer readiness only | simulation / offline / headless only | file-artifact-first reviewer evidence | not real acceptance | not compliance claim | not accreditation claim | cannot replace real metrology validation
