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
23. `DATABASE_IMPORT` / `database_import`
24. `REPORTS` / `zh_calibration_reports`
25. `FINAL_EVIDENCE_STATUS` / `final_evidence_status_refresh`
26. `FORMAL_RUN_STATUS` / `formal_run_status_snapshot`

## Formal Route Runners

- `co2_open_flow_sampling`
- `h2o_open_flow_sampling`

## Issues

- none

## Warnings

- none
