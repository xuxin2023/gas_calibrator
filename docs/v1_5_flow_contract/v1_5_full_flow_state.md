# V1.5 Full Calibration Flow State

- run_id: `v1_5_contract_reference`
- current_step_id: `load_plan_and_traceability`
- current_status: `ready`
- allow_real_com: `False`
- allow_pressure_control: `False`
- allow_route_control: `False`
- allow_writes: `False`

## Stage State

| Step | Status | Can execute | Reason |
| --- | --- | --- | --- |
| `load_plan_and_traceability` | `ready` | `True` | ready_for_manual_or_supervised_execution |
| `formal_initialization_contract_plan` | `pending_previous_stage` | `False` | waiting_for_load_plan_and_traceability |
| `initialization_readiness_snapshot` | `pending_previous_stage` | `False` | waiting_for_load_plan_and_traceability |
| `pre_gas_readiness_snapshot` | `pending_previous_stage` | `False` | waiting_for_load_plan_and_traceability |
| `device_identity_and_getco_snapshot` | `pending_previous_stage` | `False` | waiting_for_load_plan_and_traceability |
| `identity_getco_readiness_snapshot` | `pending_previous_stage` | `False` | waiting_for_load_plan_and_traceability |
| `auxiliary_senco56789_neutralization_gate` | `pending_previous_stage` | `False` | waiting_for_load_plan_and_traceability |
| `pressure_quick_check` | `pending_previous_stage` | `False` | waiting_for_load_plan_and_traceability |
| `pressure_senco9_no_write_acquisition` | `pending_previous_stage` | `False` | waiting_for_load_plan_and_traceability |
| `pressure_senco9_no_write_review` | `pending_previous_stage` | `False` | waiting_for_load_plan_and_traceability |
| `pressure_channel_completion_audit` | `pending_previous_stage` | `False` | waiting_for_load_plan_and_traceability |
| `temperature_channel_fast_review` | `pending_previous_stage` | `False` | waiting_for_load_plan_and_traceability |
| `co2_open_flow_sampling` | `pending_previous_stage` | `False` | waiting_for_load_plan_and_traceability |
| `h2o_open_flow_sampling` | `pending_previous_stage` | `False` | waiting_for_load_plan_and_traceability |
| `factory_signal_health_review` | `pending_previous_stage` | `False` | waiting_for_load_plan_and_traceability |
| `fit_input_quality_review` | `pending_previous_stage` | `False` | waiting_for_load_plan_and_traceability |
| `post_run_coefficient_executor` | `pending_previous_stage` | `False` | waiting_for_load_plan_and_traceability |
| `full_flow_closure_readiness` | `pending_previous_stage` | `False` | waiting_for_load_plan_and_traceability |
| `co2_candidate_write_review` | `pending_previous_stage` | `False` | waiting_for_load_plan_and_traceability |
| `controlled_component_write_placeholder` | `pending_previous_stage` | `False` | waiting_for_load_plan_and_traceability |
| `post_write_reverification_placeholder` | `pending_previous_stage` | `False` | waiting_for_load_plan_and_traceability |
| `formal_evidence_sidecar` | `pending_previous_stage` | `False` | waiting_for_load_plan_and_traceability |
| `formal_database_dry_run_snapshot` | `pending_previous_stage` | `False` | waiting_for_load_plan_and_traceability |
| `formal_database_import_preflight_snapshot` | `pending_previous_stage` | `False` | waiting_for_load_plan_and_traceability |
| `formal_database_import_authorization_snapshot` | `pending_previous_stage` | `False` | waiting_for_load_plan_and_traceability |
| `formal_database_import_command_contract_snapshot` | `pending_previous_stage` | `False` | waiting_for_load_plan_and_traceability |
| `formal_database_import_blocked_executor_snapshot` | `pending_previous_stage` | `False` | waiting_for_load_plan_and_traceability |
| `database_import` | `pending_previous_stage` | `False` | waiting_for_load_plan_and_traceability |
| `zh_calibration_reports` | `pending_previous_stage` | `False` | waiting_for_load_plan_and_traceability |
| `final_evidence_status_refresh` | `pending_previous_stage` | `False` | waiting_for_load_plan_and_traceability |
| `algorithm_profile_runner_dry_run_snapshot` | `pending_previous_stage` | `False` | waiting_for_load_plan_and_traceability |
| `formal_run_status_snapshot` | `pending_previous_stage` | `False` | waiting_for_load_plan_and_traceability |

## Safety Notes

- This state file does not execute any command by itself.
- Real COM, pressure, gas route, water route, and coefficient-write stages stay blocked unless separately authorized.
- Coefficient-write stages remain manual review stages in this planner even if write authorization is recorded elsewhere.
