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
- PRECHECK: bind analyzer device IDs to ports and snapshot GETCO1-9
- PRESSURE: verify analyzer P against COM22 before component calibration
- TEMPERATURE: review chamber/case temperature evidence before final approval
- CO2_OPEN_FLOW: sample clean dry gas under continuous open flow
- H2O_OPEN_FLOW: sample water route under dewpoint/reference evidence
- QC: keep raw frames, rejected frames, reasons, and fit-eligible samples
- CANDIDATE_REVIEW: derive coefficients only from role-eligible evidence
- CONTROLLED_WRITE: write only through explicit controlled tools and readback
- POST_WRITE_REVERIFY: verify updated output before archive and report
- ARCHIVE_REPORT: bundle evidence, database index, and Chinese reports

## Step Sequence

1. `LOAD_PLAN` / `load_plan_and_traceability`
2. `PRECHECK` / `device_identity_and_getco_snapshot`
3. `PRESSURE_CHANNEL_QUICK_CHECK` / `pressure_quick_check`
4. `PRESSURE_CHANNEL_SENCO9_REVIEW` / `pressure_senco9_no_write_review`
5. `TEMPERATURE_CHANNEL_REVIEW` / `temperature_channel_fast_review`
6. `CO2_OPEN_FLOW` / `co2_open_flow_sampling`
7. `H2O_OPEN_FLOW` / `h2o_open_flow_sampling`
8. `QC_AND_FIT_INPUT_REVIEW` / `fit_input_quality_review`
9. `CO2_CANDIDATE_REVIEW` / `co2_candidate_write_review`
10. `CONTROLLED_WRITE` / `controlled_component_write_placeholder`
11. `POST_WRITE_REVERIFY` / `post_write_reverification_placeholder`
12. `EVIDENCE_BUNDLE` / `formal_evidence_sidecar`
13. `DATABASE_IMPORT` / `database_import`
14. `REPORTS` / `zh_calibration_reports`

## Formal Route Runners

- none

## Issues

- none

## Warnings

- `warning` `entrypoint_not_in_inventory` (load_plan_and_traceability): gas_calibrator.tools.prepare_v1_5_formal_run_package is not present in the supplied V1.5 inventory
- `warning` `entrypoint_not_in_inventory` (device_identity_and_getco_snapshot): gas_calibrator.tools.probe_v1_5_getco_component_snapshot is not present in the supplied V1.5 inventory
- `warning` `entrypoint_not_in_inventory` (pressure_senco9_no_write_review): gas_calibrator.tools.export_v1_5_pressure_senco9_no_write_preflight is not present in the supplied V1.5 inventory
- `warning` `entrypoint_not_in_inventory` (temperature_channel_fast_review): gas_calibrator.tools.export_v1_5_temperature_channel_review is not present in the supplied V1.5 inventory
- `warning` `entrypoint_not_in_inventory` (co2_open_flow_sampling): gas_calibrator.tools.run_v1_5_formal_co2_open_flow_queue is not present in the supplied V1.5 inventory
- `warning` `entrypoint_not_in_inventory` (h2o_open_flow_sampling): gas_calibrator.tools.run_v1_5_formal_h2o_open_flow_queue is not present in the supplied V1.5 inventory
- `warning` `entrypoint_not_in_inventory` (fit_input_quality_review): gas_calibrator.tools.export_v1_5_fit_input_quality is not present in the supplied V1.5 inventory
- `warning` `entrypoint_not_in_inventory` (co2_candidate_write_review): gas_calibrator.tools.export_v1_5_co2_senco_pair_model_scope is not present in the supplied V1.5 inventory
- `warning` `entrypoint_not_in_inventory` (post_write_reverification_placeholder): gas_calibrator.tools.export_v1_5_post_write_reverification is not present in the supplied V1.5 inventory
- `warning` `entrypoint_not_in_inventory` (formal_evidence_sidecar): gas_calibrator.tools.run_v1_5_formal_evidence_sidecar is not present in the supplied V1.5 inventory
- `warning` `entrypoint_not_in_inventory` (database_import): gas_calibrator.tools.import_v1_5_evidence_package is not present in the supplied V1.5 inventory
- `warning` `entrypoint_not_in_inventory` (zh_calibration_reports): gas_calibrator.tools.export_v1_5_calibration_reports is not present in the supplied V1.5 inventory
