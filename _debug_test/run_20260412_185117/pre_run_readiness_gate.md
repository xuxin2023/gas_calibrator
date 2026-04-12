# Pre-run Readiness Gate

- gate_status: blocked_for_formal_claim
- blocking_item: certificate missing: 露点仪 DP-STEP2-01
- blocking_item: certificate expired: 湿度发生器 HG-STEP2-01
- blocking_item: intermediate check not ready: 露点仪 DP-STEP2-01 | 数字压力计 PG-STEP2-01
- blocking_item: lot binding missing: LOT-SG-2026-CO2-01
- blocking_item: out-of-tolerance open: asset-temp-chamber-tc-001
- warning_item: substitute standard approval missing: 标准气体 Lot SG-2026-CO2-01
- warning_item: gate is reviewer-facing advisory only and cannot drive equipment
- reviewer_action: 补齐缺失或过期证书，仅用于 reviewer mapping 后续闭环计划。
- reviewer_action: 补 lot binding / use batch / 替代标准审批 sidecar，不引入默认 DB 主链。
- reviewer_action: 关闭期间核查逾期与 OOT reviewer digest，再讨论未来 formal path。
- reviewer_action: 保持 current run 仅为 readiness mapping，不形成 formal compliance / acceptance claim。
- non_claim_note: Current run is limited to readiness mapping; Step 2 gate output is advisory only and cannot be used as a formal claim gate.

## Readiness Linkage

- anchor_id: pre-run-readiness-gate
- readiness_status: ready_for_readiness_mapping
- linked_artifact_refs: scope_definition_pack | decision_rule_profile | reference_asset_registry | certificate_lifecycle_summary | certificate_readiness_summary
- linked_measurement_phases: water/pressure_stable=gap | gas/pressure_stable=model_only
- linked_measurement_gaps: water/pressure_stable: Missing-layer reason: reference: no simulated evidence captured for this layer | analyzer_raw: no simulated evidence captured for this layer | output: no simulated evidence captured for this layer | data_quality: no simulated evidence captured for this layer | gas/pressure_stable: Missing-layer reason: reference: model_only coverage only; this layer has not been promoted into simulated payload evidence | analyzer_raw: model_only coverage only; this layer has not been promoted into simulated payload evidence | output: model_only coverage only; this layer has not been promoted into simulated payload evidence | data_quality: model_only coverage only; this layer has not been promoted into simulated payload evidence
- linked_method_confirmation_items: Water pressure stabilization hold confirmation | Gas pressure stabilization hold confirmation
- linked_uncertainty_inputs: Humidity reference | Pressure reference | Temperature reference | Reference gas value
- linked_traceability_nodes: Humidity reference chain | Dew-point reference link | Pressure reference link | Standard gas chain | Temperature reference link
- preseal_partial_gap: --
- gap_reason: water/pressure_stable: Missing-layer reason: reference: no simulated evidence captured for this layer | analyzer_raw: no simulated evidence captured for this layer | output: no simulated evidence captured for this layer | data_quality: no simulated evidence captured for this layer | gas/pressure_stable: Missing-layer reason: reference: model_only coverage only; this layer has not been promoted into simulated payload evidence | analyzer_raw: model_only coverage only; this layer has not been promoted into simulated payload evidence | output: model_only coverage only; this layer has not been promoted into simulated payload evidence | data_quality: model_only coverage only; this layer has not been promoted into simulated payload evidence
- missing_evidence: advisory gate cannot be used as formal compliance or acceptance evidence | blocking items must still be closed outside Step 2
- blockers: pre-run gate is advisory only and cannot drive live equipment | formal claim gate remains disabled in Step 2 | water/pressure_stable: Phase remains gap; richer simulated payload evidence is still missing | Missing signal layers: reference, analyzer_raw, output, data_quality | Linked method confirmation items remain open: Water pressure stabilization hold confirmation | Linked uncertainty inputs remain open: Humidity reference, Pressure reference, Temperature reference | Linked traceability nodes remain stub-only: Humidity reference chain, Dew-point reference link, Pressure reference link | gas/pressure_stable: Phase remains model_only; richer simulated payload evidence is still missing | Missing signal layers: reference, analyzer_raw, output, data_quality | Linked method confirmation items remain open: Gas pressure stabilization hold confirmation | Linked uncertainty inputs remain open: Reference gas value, Pressure reference, Temperature reference | Linked traceability nodes remain stub-only: Standard gas chain, Pressure reference link, Temperature reference link
- next_required_artifacts: metrology_traceability_stub | certificate_readiness_summary | reference_asset_registry | certificate_lifecycle_summary | pre_run_readiness_gate | uncertainty_model | uncertainty_input_set | sensitivity_coefficient_set | budget_case | uncertainty_golden_cases | uncertainty_report_pack | uncertainty_digest | uncertainty_rollup | uncertainty_budget_stub | method_confirmation_matrix | route_specific_validation_matrix | validation_run_set | verification_digest | verification_rollup | uncertainty_method_readiness_summary
- reviewer_next_step: Use the water pressure-stable payload as the synthetic reviewer anchor, then keep certificate and traceability closure in readiness-only artifacts until released reference evidence exists. | Use the gas pressure-stable payload as the synthetic reviewer anchor, then keep certificate and traceability closure in readiness-only artifacts until released reference evidence exists.
- boundary_digest: Step 2 reviewer readiness only | simulation / offline / headless only | file-artifact-first reviewer evidence | not real acceptance | not compliance claim | not accreditation claim | cannot replace real metrology validation
- non_claim_digest: Step 2 reviewer readiness only | simulation / offline / headless only | file-artifact-first reviewer evidence | not real acceptance | not compliance claim | not accreditation claim | cannot replace real metrology validation
