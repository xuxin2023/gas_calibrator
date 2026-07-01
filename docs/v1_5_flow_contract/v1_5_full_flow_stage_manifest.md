# V1.5 Full-Flow Stage Manifest

- schema: `v1_5_full_flow_stage_manifest_v1`
- run_id: `v1_5_contract_reference`
- contract: `pressure_first_temperature_review_then_open_flow_components`
- automation_level: `supervised_tool_chain_with_controlled_live_gates`
- one_button_live_runner_ready: `False`

## Safety Summary

- `does_not_modify_run_app`: `True`
- `planner_opens_com_ports`: `False`
- `planner_controls_routes`: `False`
- `planner_writes_coefficients`: `False`
- `planner_writes_device_id`: `False`
- `identity_key`: `analyzer_device_id_not_com_port_or_ga_alias`
- `live_runner_readiness_artifact`: `v1_5_full_flow_live_runner_readiness.json`
- `not_one_button_live_runner_reason`: `pressure, open-flow route, and coefficient-write stages remain explicit controlled gates`

## Automation Summary

| State | Count |
| --- | ---: |
| `blocked_controlled_write` | 2 |
| `dedicated_open_flow_runner_requires_authorization` | 2 |
| `dedicated_pressure_runner_requires_authorization` | 2 |
| `offline_database_requires_dsn` | 1 |
| `offline_review_auto_candidate` | 9 |
| `offline_review_waiting_for_run_artifacts` | 11 |
| `read_only_real_com_requires_authorization` | 1 |

## Stage Contract

| Order | Phase | Step | Automation | Gate | Tool |
| ---: | --- | --- | --- | --- | --- |
| 1 | `LOAD_PLAN` | `load_plan_and_traceability` | `offline_review_waiting_for_run_artifacts` | `required_before_sampling` | `gas_calibrator.tools.prepare_v1_5_formal_run_package` |
| 2 | `INITIALIZATION_CONTRACT` | `formal_initialization_contract_plan` | `offline_review_waiting_for_run_artifacts` | `required_before_identity_getco_snapshot` | `gas_calibrator.tools.run_v1_5_formal_initialization_runner` |
| 3 | `INITIALIZATION_READINESS` | `initialization_readiness_snapshot` | `offline_review_auto_candidate` | `required_before_identity_getco_snapshot` | `gas_calibrator.tools.export_v1_5_initialization_readiness` |
| 4 | `PRE_GAS_READINESS` | `pre_gas_readiness_snapshot` | `offline_review_auto_candidate` | `required_before_identity_getco_snapshot` | `gas_calibrator.tools.export_v1_5_pre_gas_readiness` |
| 5 | `PRECHECK` | `device_identity_and_getco_snapshot` | `read_only_real_com_requires_authorization` | `required_before_any_write` | `gas_calibrator.tools.probe_v1_5_getco_component_snapshot` |
| 6 | `IDENTITY_GETCO_READINESS` | `identity_getco_readiness_snapshot` | `offline_review_auto_candidate` | `required_after_epoch0_getco_before_auxiliary_neutralization` | `gas_calibrator.tools.export_v1_5_getco_identity_readiness` |
| 7 | `AUXILIARY_COEFFICIENT_NEUTRALIZATION` | `auxiliary_senco56789_neutralization_gate` | `blocked_controlled_write` | `required_after_epoch0_getco_before_pressure_and_component_sampling` | `manual/placeholder` |
| 8 | `PRESSURE_CHANNEL_QUICK_CHECK` | `pressure_quick_check` | `dedicated_pressure_runner_requires_authorization` | `block_component_write_if_failed` | `gas_calibrator.tools.validate_pressure_only` |
| 9 | `PRESSURE_CHANNEL_SENCO9_ACQUISITION` | `pressure_senco9_no_write_acquisition` | `dedicated_pressure_runner_requires_authorization` | `only_needed_when_pressure_quick_check_fails_before_component_sampling` | `gas_calibrator.tools.validate_pressure_only` |
| 10 | `PRESSURE_CHANNEL_SENCO9_REVIEW` | `pressure_senco9_no_write_review` | `offline_review_auto_candidate` | `only_needed_when_pressure_quick_check_fails_after_no_write_acquisition` | `gas_calibrator.tools.export_v1_5_pressure_senco9_evaluation` |
| 11 | `PRESSURE_CHANNEL_COMPLETION` | `pressure_channel_completion_audit` | `offline_review_waiting_for_run_artifacts` | `required_when_senco9_was_written_before_component_sampling` | `gas_calibrator.tools.export_v1_5_pressure_channel_completion` |
| 12 | `TEMPERATURE_CHANNEL_REVIEW` | `temperature_channel_fast_review` | `offline_review_auto_candidate` | `review_before_final_component_write` | `gas_calibrator.tools.export_v1_5_temperature_channel_review` |
| 13 | `CO2_OPEN_FLOW` | `co2_open_flow_sampling` | `dedicated_open_flow_runner_requires_authorization` | `requires_pressure_pass` | `gas_calibrator.tools.run_v1_5_formal_co2_open_flow_queue` |
| 14 | `H2O_OPEN_FLOW` | `h2o_open_flow_sampling` | `dedicated_open_flow_runner_requires_authorization` | `requires_pressure_pass` | `gas_calibrator.tools.run_v1_5_formal_h2o_open_flow_queue` |
| 15 | `FACTORY_SIGNAL_HEALTH_REVIEW` | `factory_signal_health_review` | `offline_review_waiting_for_run_artifacts` | `required_before_component_write_review` | `gas_calibrator.tools.export_v1_5_factory_signal_health_review` |
| 16 | `QC_AND_FIT_INPUT_REVIEW` | `fit_input_quality_review` | `offline_review_waiting_for_run_artifacts` | `requires_factory_signal_health_review` | `gas_calibrator.tools.export_v1_5_fit_input_quality` |
| 17 | `POST_RUN_COEFFICIENT_EXECUTOR` | `post_run_coefficient_executor` | `offline_review_waiting_for_run_artifacts` | `required_after_component_acquisition_before_controlled_write` | `gas_calibrator.tools.export_v1_5_post_run_coefficient_executor` |
| 18 | `FULL_FLOW_CLOSURE_READINESS` | `full_flow_closure_readiness` | `offline_review_auto_candidate` | `required_before_controlled_write_review` | `gas_calibrator.tools.export_v1_5_full_flow_closure_readiness` |
| 19 | `CO2_CANDIDATE_REVIEW` | `co2_candidate_write_review` | `offline_review_waiting_for_run_artifacts` | `reviewer_approver_required_before_write` | `gas_calibrator.tools.export_v1_5_co2_senco_pair_model_scope` |
| 20 | `CONTROLLED_WRITE` | `controlled_component_write_placeholder` | `blocked_controlled_write` | `never_auto_execute_from_full_flow_planner` | `manual/placeholder` |
| 21 | `POST_WRITE_REVERIFY` | `post_write_reverification_placeholder` | `offline_review_waiting_for_run_artifacts` | `required_after_controlled_write_before_final_archive` | `gas_calibrator.tools.export_v1_5_post_write_reverification` |
| 22 | `EVIDENCE_BUNDLE` | `formal_evidence_sidecar` | `offline_review_waiting_for_run_artifacts` | `required_before_report_and_database_import` | `gas_calibrator.tools.run_v1_5_formal_evidence_sidecar` |
| 23 | `FORMAL_DATABASE_DRY_RUN` | `formal_database_dry_run_snapshot` | `offline_review_auto_candidate` | `required_before_database_import_authorization` | `gas_calibrator.tools.export_v1_5_formal_database_dry_run` |
| 24 | `DATABASE_IMPORT` | `database_import` | `offline_database_requires_dsn` | `required_for_formal_archive` | `gas_calibrator.tools.import_v1_5_evidence_package` |
| 25 | `REPORTS` | `zh_calibration_reports` | `offline_review_waiting_for_run_artifacts` | `final_deliverable` | `gas_calibrator.tools.export_v1_5_calibration_reports` |
| 26 | `FINAL_EVIDENCE_STATUS` | `final_evidence_status_refresh` | `offline_review_waiting_for_run_artifacts` | `required_after_reports_for_archive_closure` | `gas_calibrator.tools.export_v1_5_run_evidence_status` |
| 27 | `ALGORITHM_PROFILE_RUNNER_DRY_RUN` | `algorithm_profile_runner_dry_run_snapshot` | `offline_review_auto_candidate` | `optional_new_algorithm_runner_preflight_before_status_rollup` | `gas_calibrator.tools.export_v1_5_algorithm_profile_runner_dry_run` |
| 28 | `FORMAL_RUN_STATUS` | `formal_run_status_snapshot` | `offline_review_auto_candidate` | `final_reviewer_status_overview` | `gas_calibrator.tools.export_v1_5_formal_run_status` |

## Live And Write Gates

| Step | Real COM | Pressure | Route | Write | Device ID |
| --- | --- | --- | --- | --- | --- |
| `device_identity_and_getco_snapshot` | `True` | `False` | `False` | `False` | `False` |
| `auxiliary_senco56789_neutralization_gate` | `True` | `False` | `False` | `True` | `False` |
| `pressure_quick_check` | `True` | `True` | `False` | `False` | `False` |
| `pressure_senco9_no_write_acquisition` | `True` | `True` | `False` | `False` | `False` |
| `co2_open_flow_sampling` | `True` | `False` | `True` | `False` | `False` |
| `h2o_open_flow_sampling` | `True` | `False` | `True` | `False` | `False` |
| `controlled_component_write_placeholder` | `False` | `False` | `False` | `True` | `False` |

## Evidence Requirements

### 1. `load_plan_and_traceability`

Required inputs:
- runtime config
- standard gas certificates
- COM22 pressure certificate

Expected outputs:
- formal_plan_snapshot.json
- com22_pressure_reference.json
- evidence_run_manifest.json

### 2. `formal_initialization_contract_plan`

Required inputs:
- runtime config
- operator identity for traceability

Expected outputs:
- formal_initialization/v1_5_formal_initialization_plan.json
- formal_initialization/v1_5_formal_initialization_contract.json
- formal_initialization/v1_5_formal_initialization_db_bundle.json
- formal_initialization/v1_5_formal_initialization_commands.ps1

### 3. `initialization_readiness_snapshot`

Required inputs:
- runtime config
- planned initialization evidence directory

Expected outputs:
- formal_initialization/v1_5_initialization_readiness.json
- formal_initialization/v1_5_initialization_readiness.md
- formal_initialization/v1_5_initialization_database_sidecar.json

### 4. `pre_gas_readiness_snapshot`

Required inputs:
- formal initialization contract plan
- initialization readiness sidecar
- PostgreSQL 18/SN/device_code/CHECK/S7-S8/S9 contracts

Expected outputs:
- pre_gas_readiness/v1_5_pre_gas_readiness.json
- pre_gas_readiness/v1_5_pre_gas_readiness.md
- pre_gas_readiness/v1_5_pre_gas_readiness_checks.csv

### 5. `device_identity_and_getco_snapshot`

Required inputs:
- enabled analyzer device_id mapping

Expected outputs:
- old_component_coefficients_snapshot.json
- getco_component_snapshot_identity.csv
- runtime_identity_bound_config.json

### 6. `identity_getco_readiness_snapshot`

Required inputs:
- old_component_coefficients_snapshot.json
- getco_component_snapshot_identity.csv
- getco_component_snapshot_conclusion.csv
- runtime_identity_bound_config.json

Expected outputs:
- identity_getco_readiness/v1_5_getco_identity_readiness.json
- identity_getco_readiness/v1_5_getco_identity_readiness.md
- identity_getco_readiness/v1_5_getco_identity_readiness_checks.csv

### 7. `auxiliary_senco56789_neutralization_gate`

Required inputs:
- old GETCO1-9 epoch-0 snapshot
- runtime_identity_bound_config.json
- identity_getco_readiness/v1_5_getco_identity_readiness.json
- reviewer
- approver

Expected outputs:
- D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean\docs\v1_5_flow_contract\auxiliary_senco56789_neutralization\senco5_neutral_write_events.csv
- D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean\docs\v1_5_flow_contract\auxiliary_senco56789_neutralization\senco6_neutral_write_events.csv
- D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean\docs\v1_5_flow_contract\auxiliary_senco56789_neutralization\senco78_neutral_write_events.csv
- D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean\docs\v1_5_flow_contract\auxiliary_senco56789_neutralization\senco9_clear_write_events.csv
- auxiliary_coefficient_epoch_neutralized_snapshot.json

### 8. `pressure_quick_check`

Required inputs:
- GETCO9 snapshot
- COM22 pressure certificate

Expected outputs:
- pressure quick-check CSV

### 9. `pressure_senco9_no_write_acquisition`

Required inputs:
- failed or marginal pressure quick-check
- GETCO9 snapshot
- COM22 pressure certificate

Expected outputs:
- pressure_transition_trace.csv
- pressure_only_validation_meta.json
- pressure_channel_multi_analyzer_summary.csv
- pressure quick-check CSVs by analyzer

### 10. `pressure_senco9_no_write_review`

Required inputs:
- pressure_senco9_no_write_acquisition artifacts

Expected outputs:
- pressure_senco9_fit_evaluation workbook
- pressure_fit_summary.csv

### 11. `pressure_channel_completion_audit`

Required inputs:
- SENCO9 controlled write summary
- post-write pressure verification fit summary
- COM22 pressure certificate
- GETCO9 epoch-0 snapshot

Expected outputs:
- pressure_channel_completion/pressure_channel_completion_summary.csv
- pressure_channel_completion/pressure_channel_device_readiness.csv
- pressure_channel_completion/pressure_channel_completion_report.md

### 12. `temperature_channel_fast_review`

Required inputs:
- digital thermometer evidence
- analyzer chamber/case temperature evidence

Expected outputs:
- temperature_channel_review_summary.json
- temperature_channel_review.md

### 13. `co2_open_flow_sampling`

Required inputs:
- CO2 queue
- pressure verified
- temperature evidence policy

Expected outputs:
- CO2 point sidecars
- MODE2 ratio/signal evidence
- digital thermometer evidence

### 14. `h2o_open_flow_sampling`

Required inputs:
- H2O queue
- dewpoint reference
- pressure verified
- temperature evidence policy

Expected outputs:
- H2O point sidecars
- dewpoint/H2O mmol evidence
- H2O ratio/signal evidence

### 15. `factory_signal_health_review`

Required inputs:
- MODE2 point means
- candidate residuals

Expected outputs:
- factory_signal_health_summary.csv
- factory_signal_health_point_flags.csv
- factory_signal_health_report_zh.md

### 16. `fit_input_quality_review`

Required inputs:
- CO2 candidate residuals
- H2O candidate residuals

Expected outputs:
- fit_input_quality.md

### 17. `post_run_coefficient_executor`

Required inputs:
- completed CO2 open-flow evidence
- completed H2O open-flow evidence
- pressure and temperature input reviews
- factory signal health review
- fit input quality review

Expected outputs:
- post_run_coefficient_executor/executor_manifest.json
- post_run_coefficient_executor/executor_summary.md
- post_run_coefficient_executor/device_eligibility.csv
- post_run_coefficient_executor/coefficient_execution_plan.csv
- post_run_coefficient_executor/controlled_write_package.csv
- post_run_coefficient_executor/post_write_reverification_plan.csv
- post_run_coefficient_executor/archive_gap_list.csv

### 18. `full_flow_closure_readiness`

Required inputs:
- v1_5_run_evidence_status.json
- post_run_coefficient_executor/executor_manifest.json
- post_run_coefficient_executor/controlled_write_package.csv
- post_run_coefficient_executor/post_write_reverification_plan.csv

Expected outputs:
- full_flow_closure_readiness/v1_5_full_flow_closure_readiness.json
- full_flow_closure_readiness/v1_5_full_flow_closure_readiness.md
- full_flow_closure_readiness/v1_5_full_flow_closure_gaps.csv
- full_flow_closure_readiness/v1_5_full_flow_device_closure.csv
- full_flow_closure_readiness/v1_5_full_flow_release_domains.csv

### 19. `co2_candidate_write_review`

Required inputs:
- CO2 ratio fit candidates
- zero-gas/low-end CO2 anchor evidence

Expected outputs:
- CO2 SENCO model-scope review

### 20. `controlled_component_write_placeholder`

Required inputs:
- approved CO2/H2O/S5/S6 write packages
- old GETCO1-9 snapshot
- reviewer
- approver

Expected outputs:
- write events
- readback verification
- coefficient_epoch_n snapshots

### 21. `post_write_reverification_placeholder`

Required inputs:
- coefficient_epoch_n readback snapshots
- approved post-write verification gas/H2O points
- pressure channel pass evidence

Expected outputs:
- post_write_reverification_summary.json
- post_write_reverification_point_errors.csv
- D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean\docs\v1_5_flow_contract\post_write_reverification

### 22. `formal_evidence_sidecar`

Required inputs:
- completed V1.5 run directory
- pressure quick-check
- formal plan snapshot

Expected outputs:
- formal_evidence_sidecar/evidence_bundle.json

### 23. `formal_database_dry_run_snapshot`

Required inputs:
- V1.5 core storage schema
- V1.5 evidence registry schema
- SN/device_code identity contract
- formal evidence sidecar or reviewed insert-preview inputs

Expected outputs:
- formal_database_dry_run/v1_5_formal_database_dry_run.json
- formal_database_dry_run/V1_5_FORMAL_DATABASE_DRY_RUN.md
- formal_database_dry_run/v1_5_formal_database_dry_run_checks.csv
- formal_database_dry_run/v1_5_formal_database_insert_preview.csv

### 24. `database_import`

Required inputs:
- V1.5 evidence DSN
- evidence_bundle.json

Expected outputs:
- database_imported=true summary

### 25. `zh_calibration_reports`

Required inputs:
- evidence_bundle.json

Expected outputs:
- run report
- technical report
- formal calibration report
- per_device_certificates
- per_device_certificate_manifest.json
- per_device_certificate_artifact_hashes.csv

### 26. `final_evidence_status_refresh`

Required inputs:
- evidence_bundle.json
- report artifacts
- per-device certificate package

Expected outputs:
- v1_5_run_evidence_status.json
- v1_5_run_evidence_status.md

### 27. `algorithm_profile_runner_dry_run_snapshot`

Required inputs:
- configs/v1_5_algorithm_route_profiles.json

Expected outputs:
- algorithm_profile_runner_dry_run/v1_5_algorithm_profile_runner_dry_run.json
- algorithm_profile_runner_dry_run/v1_5_algorithm_profile_runner_dry_run_checks.csv
- algorithm_profile_runner_dry_run/algorithm_formal_runlist_preview/v1_5_new_algorithm_formal_co2_runlist_preview.csv
- algorithm_profile_runner_dry_run/algorithm_formal_runlist_preview/v1_5_new_algorithm_formal_h2o_runlist_preview.csv
- algorithm_profile_runner_dry_run/algorithm_runlist_readiness/v1_5_algorithm_runlist_readiness.json
- algorithm_profile_runner_dry_run/algorithm_runner_integration_dry_run/v1_5_algorithm_runner_integration_dry_run.json

### 28. `formal_run_status_snapshot`

Required inputs:
- initialization readiness sidecar
- identity/GETCO readiness sidecar
- pre-gas readiness sidecar
- v1_5_run_evidence_status.json
- full-flow closure readiness or archive sidecar when available
- optional new-algorithm profile runner dry-run bundle
- formal PostgreSQL 18 database dry-run contract

Expected outputs:
- formal_run_status/v1_5_formal_run_status.json
- formal_run_status/v1_5_formal_run_status.md
- formal_run_status/v1_5_formal_run_status_gates.csv
- formal_run_status/v1_5_formal_run_status_gaps.csv

## Guardrail

- This manifest is generated from the V1.5 full-flow plan.
- It does not execute commands, open COM ports, control valves, control PACE, or write SENCO.
- A future executor must treat `automation_state` and `authorization_required` as hard gates.
