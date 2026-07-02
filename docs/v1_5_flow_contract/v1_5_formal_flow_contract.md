# V1.5 Formal Flow Contract

- status: `pass`
- contract: `pressure_first_temperature_review_then_open_flow_components`

## Physical Boundaries

- `offline_contract_audit`: `True`
- `opens_com_ports`: `False`
- `controls_water_or_gas_routes`: `False`
- `controls_valves_or_pace`: `False`
- `writes_coefficients`: `False`
- `not_real_acceptance_evidence`: `True`

## Physical Flow

- LOAD_PLAN: freeze plan, certificates, config hash, and run identity
- INITIALIZATION_CONTRACT: generate the formal initialization plan, PostgreSQL 18 sidecar, and readiness snapshot without COM or writes
- PRE_GAS_READINESS: summarize SN/device_code, PostgreSQL 18, MODE2/1Hz, GETCO, S7/S8, S9, route, and CHECK gates before live identity
- PRECHECK: bind analyzer device IDs to ports and snapshot GETCO1-9
- IDENTITY_GETCO_READINESS: verify epoch-0 GETCO artifacts, no-write conclusion, and runtime identity-bound config before auxiliary coefficient changes
- AUX_NEUTRALIZE: after immutable GETCO backup, neutralize SENCO5/6/7/8/9 through controlled tools
- PRESSURE: verify analyzer P against COM22 before component calibration
- PRESSURE_SENCO9: if needed, use the full V1.5 no-write sealed-pressure runner and transition trace
- PRESSURE_COMPLETION: after SENCO9 write and reverification, freeze traceable pressure-channel completion evidence
- TEMPERATURE: review chamber/case temperature evidence before final approval
- CO2_OPEN_FLOW: sample clean dry gas under continuous open flow
- H2O_OPEN_FLOW: sample water route under dewpoint/reference evidence
- QC: keep raw frames, rejected frames, reasons, and fit-eligible samples
- CANDIDATE_REVIEW: derive coefficients only from role-eligible evidence
- CONTROLLED_WRITE: write only through explicit controlled tools and readback
- POST_WRITE_REVERIFY: verify updated output before archive and report
- FORMAL_DATABASE_DRY_RUN: preview PostgreSQL 18 schema, SN/device_code identity, and insert contracts without connecting or importing
- FORMAL_DATABASE_IMPORT_PREFLIGHT: review DSN, migration lock, archive-release dependency, and import authorization without connecting
- FORMAL_DATABASE_IMPORT_AUTHORIZATION: review archive release and manual import authorization without connecting
- FORMAL_DATABASE_IMPORT_COMMAND_CONTRACT: review the future import command inputs and execution lock without connecting or importing
- ARCHIVE_REPORT: bundle evidence, database index, and Chinese reports
- FORMAL_RUN_STATUS: refresh the top-level current-stage and release-readiness dashboard from offline sidecars

## Step Sequence

1. `LOAD_PLAN` / `load_plan_and_traceability`
2. `INITIALIZATION_CONTRACT` / `formal_initialization_contract_plan`
3. `INITIALIZATION_READINESS` / `initialization_readiness_snapshot`
4. `PRE_GAS_READINESS` / `pre_gas_readiness_snapshot`
5. `PRECHECK` / `device_identity_and_getco_snapshot`
6. `IDENTITY_GETCO_READINESS` / `identity_getco_readiness_snapshot`
7. `AUXILIARY_COEFFICIENT_NEUTRALIZATION` / `auxiliary_senco56789_neutralization_gate`
8. `PRESSURE_CHANNEL_QUICK_CHECK` / `pressure_quick_check`
9. `PRESSURE_CHANNEL_SENCO9_ACQUISITION` / `pressure_senco9_no_write_acquisition`
10. `PRESSURE_CHANNEL_SENCO9_REVIEW` / `pressure_senco9_no_write_review`
11. `PRESSURE_CHANNEL_COMPLETION` / `pressure_channel_completion_audit`
12. `TEMPERATURE_CHANNEL_REVIEW` / `temperature_channel_fast_review`
13. `CO2_OPEN_FLOW` / `co2_open_flow_sampling`
14. `H2O_OPEN_FLOW` / `h2o_open_flow_sampling`
15. `FACTORY_SIGNAL_HEALTH_REVIEW` / `factory_signal_health_review`
16. `QC_AND_FIT_INPUT_REVIEW` / `fit_input_quality_review`
17. `POST_RUN_COEFFICIENT_EXECUTOR` / `post_run_coefficient_executor`
18. `FULL_FLOW_CLOSURE_READINESS` / `full_flow_closure_readiness`
19. `CO2_CANDIDATE_REVIEW` / `co2_candidate_write_review`
20. `CONTROLLED_WRITE` / `controlled_component_write_placeholder`
21. `POST_WRITE_REVERIFY` / `post_write_reverification_placeholder`
22. `EVIDENCE_BUNDLE` / `formal_evidence_sidecar`
23. `FORMAL_DATABASE_DRY_RUN` / `formal_database_dry_run_snapshot`
24. `FORMAL_DATABASE_IMPORT_PREFLIGHT` / `formal_database_import_preflight_snapshot`
25. `FORMAL_DATABASE_IMPORT_AUTHORIZATION` / `formal_database_import_authorization_snapshot`
26. `FORMAL_DATABASE_IMPORT_COMMAND_CONTRACT` / `formal_database_import_command_contract_snapshot`
27. `DATABASE_IMPORT` / `database_import`
28. `REPORTS` / `zh_calibration_reports`
29. `FINAL_EVIDENCE_STATUS` / `final_evidence_status_refresh`
30. `ALGORITHM_PROFILE_RUNNER_DRY_RUN` / `algorithm_profile_runner_dry_run_snapshot`
31. `FORMAL_RUN_STATUS` / `formal_run_status_snapshot`

## Formal Route Runners

- none

## Issues

- none

## Warnings

- `warning` `entrypoint_not_in_inventory` (load_plan_and_traceability): gas_calibrator.tools.prepare_v1_5_formal_run_package is not present in the supplied V1.5 inventory
- `warning` `entrypoint_not_in_inventory` (formal_initialization_contract_plan): gas_calibrator.tools.run_v1_5_formal_initialization_runner is not present in the supplied V1.5 inventory
- `warning` `entrypoint_not_in_inventory` (initialization_readiness_snapshot): gas_calibrator.tools.export_v1_5_initialization_readiness is not present in the supplied V1.5 inventory
- `warning` `entrypoint_not_in_inventory` (pre_gas_readiness_snapshot): gas_calibrator.tools.export_v1_5_pre_gas_readiness is not present in the supplied V1.5 inventory
- `warning` `entrypoint_not_in_inventory` (device_identity_and_getco_snapshot): gas_calibrator.tools.probe_v1_5_getco_component_snapshot is not present in the supplied V1.5 inventory
- `warning` `entrypoint_not_in_inventory` (identity_getco_readiness_snapshot): gas_calibrator.tools.export_v1_5_getco_identity_readiness is not present in the supplied V1.5 inventory
- `warning` `entrypoint_not_in_inventory` (pressure_senco9_no_write_review): gas_calibrator.tools.export_v1_5_pressure_senco9_evaluation is not present in the supplied V1.5 inventory
- `warning` `entrypoint_not_in_inventory` (pressure_channel_completion_audit): gas_calibrator.tools.export_v1_5_pressure_channel_completion is not present in the supplied V1.5 inventory
- `warning` `entrypoint_not_in_inventory` (temperature_channel_fast_review): gas_calibrator.tools.export_v1_5_temperature_channel_review is not present in the supplied V1.5 inventory
- `warning` `entrypoint_not_in_inventory` (co2_open_flow_sampling): gas_calibrator.tools.run_v1_5_formal_co2_open_flow_queue is not present in the supplied V1.5 inventory
- `warning` `entrypoint_not_in_inventory` (h2o_open_flow_sampling): gas_calibrator.tools.run_v1_5_formal_h2o_open_flow_queue is not present in the supplied V1.5 inventory
- `warning` `entrypoint_not_in_inventory` (factory_signal_health_review): gas_calibrator.tools.export_v1_5_factory_signal_health_review is not present in the supplied V1.5 inventory
- `warning` `entrypoint_not_in_inventory` (fit_input_quality_review): gas_calibrator.tools.export_v1_5_fit_input_quality is not present in the supplied V1.5 inventory
- `warning` `entrypoint_not_in_inventory` (post_run_coefficient_executor): gas_calibrator.tools.export_v1_5_post_run_coefficient_executor is not present in the supplied V1.5 inventory
- `warning` `entrypoint_not_in_inventory` (full_flow_closure_readiness): gas_calibrator.tools.export_v1_5_full_flow_closure_readiness is not present in the supplied V1.5 inventory
- `warning` `entrypoint_not_in_inventory` (co2_candidate_write_review): gas_calibrator.tools.export_v1_5_co2_senco_pair_model_scope is not present in the supplied V1.5 inventory
- `warning` `entrypoint_not_in_inventory` (post_write_reverification_placeholder): gas_calibrator.tools.export_v1_5_post_write_reverification is not present in the supplied V1.5 inventory
- `warning` `entrypoint_not_in_inventory` (formal_evidence_sidecar): gas_calibrator.tools.run_v1_5_formal_evidence_sidecar is not present in the supplied V1.5 inventory
- `warning` `entrypoint_not_in_inventory` (formal_database_dry_run_snapshot): gas_calibrator.tools.export_v1_5_formal_database_dry_run is not present in the supplied V1.5 inventory
- `warning` `entrypoint_not_in_inventory` (formal_database_import_preflight_snapshot): gas_calibrator.tools.export_v1_5_formal_database_import_preflight is not present in the supplied V1.5 inventory
- `warning` `entrypoint_not_in_inventory` (formal_database_import_authorization_snapshot): gas_calibrator.tools.export_v1_5_formal_database_import_authorization is not present in the supplied V1.5 inventory
- `warning` `entrypoint_not_in_inventory` (formal_database_import_command_contract_snapshot): gas_calibrator.tools.export_v1_5_formal_database_import_command_contract is not present in the supplied V1.5 inventory
- `warning` `entrypoint_not_in_inventory` (database_import): gas_calibrator.tools.import_v1_5_evidence_package is not present in the supplied V1.5 inventory
- `warning` `entrypoint_not_in_inventory` (zh_calibration_reports): gas_calibrator.tools.export_v1_5_calibration_reports is not present in the supplied V1.5 inventory
- `warning` `entrypoint_not_in_inventory` (final_evidence_status_refresh): gas_calibrator.tools.export_v1_5_run_evidence_status is not present in the supplied V1.5 inventory
- `warning` `entrypoint_not_in_inventory` (algorithm_profile_runner_dry_run_snapshot): gas_calibrator.tools.export_v1_5_algorithm_profile_runner_dry_run is not present in the supplied V1.5 inventory
- `warning` `entrypoint_not_in_inventory` (formal_run_status_snapshot): gas_calibrator.tools.export_v1_5_formal_run_status is not present in the supplied V1.5 inventory
