# V1.5 Run Evidence Status

- overall_status: `incomplete`
- current_stage: `plan_traceability`
- run_dir: `D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean\docs\v1_5_flow_contract`
- contract_status: `pass`

## Physical Boundaries

- `offline_status_only`: `True`
- `opens_com_ports`: `False`
- `controls_water_or_gas_routes`: `False`
- `controls_valves_or_pace`: `False`
- `writes_coefficients`: `False`
- `not_real_acceptance_evidence`: `True`

## Full-Flow Stage Manifest

- status: `present`
- source_path: `D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean\docs\v1_5_flow_contract\v1_5_full_flow_stage_manifest.json`
- schema: `v1_5_full_flow_stage_manifest_v1`
- current_manifest_stage: `load_plan_and_traceability`
- one_button_live_runner_ready: `False`

Stage status counts:
- `authorization_required`: `5`
- `blocked_controlled_gate`: `2`
- `pass`: `5`
- `waiting_for_artifacts`: `19`

Manifest stages:
- `load_plan_and_traceability`: `waiting_for_artifacts` - manifest_expected_outputs_missing
- `formal_initialization_contract_plan`: `waiting_for_artifacts` - manifest_expected_outputs_missing
- `initialization_readiness_snapshot`: `waiting_for_artifacts` - manifest_expected_outputs_missing
- `pre_gas_readiness_snapshot`: `waiting_for_artifacts` - manifest_expected_outputs_missing
- `device_identity_and_getco_snapshot`: `authorization_required` - live_stage_requires_explicit_authorization_and_external_execution
- `identity_getco_readiness_snapshot`: `waiting_for_artifacts` - manifest_expected_outputs_missing
- `auxiliary_senco56789_neutralization_gate`: `blocked_controlled_gate` - controlled_write_or_device_id_gate_requires_explicit_review
- `pressure_quick_check`: `authorization_required` - live_stage_requires_explicit_authorization_and_external_execution
- `pressure_senco9_no_write_acquisition`: `authorization_required` - live_stage_requires_explicit_authorization_and_external_execution
- `pressure_senco9_no_write_review`: `waiting_for_artifacts` - manifest_expected_outputs_missing
- `pressure_channel_completion_audit`: `waiting_for_artifacts` - manifest_expected_outputs_missing
- `temperature_channel_fast_review`: `waiting_for_artifacts` - manifest_expected_outputs_missing
- `co2_open_flow_sampling`: `authorization_required` - live_stage_requires_explicit_authorization_and_external_execution
- `h2o_open_flow_sampling`: `authorization_required` - live_stage_requires_explicit_authorization_and_external_execution
- `factory_signal_health_review`: `waiting_for_artifacts` - manifest_expected_outputs_missing
- `fit_input_quality_review`: `waiting_for_artifacts` - manifest_expected_outputs_missing
- `post_run_coefficient_executor`: `waiting_for_artifacts` - manifest_expected_outputs_missing
- `full_flow_closure_readiness`: `waiting_for_artifacts` - manifest_expected_outputs_missing
- `co2_candidate_write_review`: `waiting_for_artifacts` - manifest_expected_outputs_missing
- `controlled_component_write_placeholder`: `blocked_controlled_gate` - controlled_write_or_device_id_gate_requires_explicit_review
- `post_write_reverification_placeholder`: `waiting_for_artifacts` - manifest_expected_outputs_missing
- `formal_evidence_sidecar`: `waiting_for_artifacts` - manifest_expected_outputs_missing
- `formal_database_dry_run_snapshot`: `pass` - all_manifest_expected_outputs_present
- `formal_database_import_preflight_snapshot`: `pass` - all_manifest_expected_outputs_present
- `formal_database_import_authorization_snapshot`: `pass` - all_manifest_expected_outputs_present
- `formal_database_import_command_contract_snapshot`: `waiting_for_artifacts` - manifest_expected_outputs_missing
- `database_import`: `waiting_for_artifacts` - manifest_expected_outputs_missing
- `zh_calibration_reports`: `waiting_for_artifacts` - manifest_expected_outputs_missing
- `final_evidence_status_refresh`: `waiting_for_artifacts` - manifest_expected_outputs_missing
- `algorithm_profile_runner_dry_run_snapshot`: `pass` - all_manifest_expected_outputs_present
- `formal_run_status_snapshot`: `pass` - all_manifest_expected_outputs_present

## Full-Flow Live Runner Readiness

- status: `present`
- source_path: `D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean\docs\v1_5_flow_contract\v1_5_full_flow_live_runner_readiness.json`
- schema: `v1_5_full_flow_live_runner_readiness_v1`
- one_button_live_runner_ready: `False`
- blocked_domains: `identity_and_epoch0`, `auxiliary_coefficients`, `pressure_channel`, `temperature_channel`, `co2_open_flow`, `h2o_open_flow`, `candidate_fit_and_qc`, `controlled_write_and_reverify`, `archive_and_release`
- required_authorizations: `real_com`, `pressure_control`, `route_control`, `coefficient_write`

## Stages

- `full_flow_stage_manifest` Full-flow stage manifest: `pass` - full-flow stage manifest artifact is present
  - physical_meaning: The stage manifest maps the complete V1.5 flow into machine-readable automation states, evidence contracts, and live/write authorization gates.
- `full_flow_live_runner_readiness` Full-flow live-runner readiness: `pass` - full-flow live-runner readiness artifact is present
  - physical_meaning: The readiness artifact states which V1.5 domains are offline-supervised and which still require controlled real-COM, pressure, route, or SENCO-write gates before a future one-button live runner may be trusted.
- `full_flow_contract_gate` Full-flow contract audit gate: `pass` - contract_status=pass
  - physical_meaning: Before any formal run advance, the plan must prove pressure-first, open-flow component sampling, no V2 real COM, and no auto-write boundaries.
- `plan_traceability` Plan and traceability snapshots: `missing` - formal plan or pressure reference snapshot missing
  - physical_meaning: The calibration result must bind to a plan, standard/reference certificates, and config identity before sampling.
- `identity_getco_epoch0` Analyzer identity and GETCO epoch 0: `missing` - GETCO snapshot or runtime identity-bound config missing
  - physical_meaning: COM ports are transport only; analyzer device IDs and GETCO1-9 define the pre-calibration coefficient epoch.
- `pressure_quick_check` Pressure channel quick check or completion: `missing` - pressure quick-check or pressure-channel completion evidence missing
  - physical_meaning: Analyzer pressure P is an input to CO2/H2O compensation and must be verified before component calibration.
- `co2_open_flow` CO2 open-flow evidence: `missing` - CO2 open-flow sample evidence missing
  - physical_meaning: CO2 fitting must be based on clean open-flow samples and factory ratio evidence, not sealed contaminated pressure points.
- `co2_queue_failure_audit` CO2 queue failure audit: `not_attempted` - CO2 queue failure audit not generated
  - physical_meaning: A finished CO2 queue must preserve why any point failed: insufficiently dry dewpoint, dewpoint rebound, analyzer startup, route readiness, or other diagnostic causes. Failed or downgraded points remain evidence, but must not silently enter formal coefficient fitting.
- `h2o_open_flow` H2O open-flow evidence: `missing` - H2O open-flow sample evidence missing
  - physical_meaning: H2O fitting must preserve dewpoint/reference-backed water evidence and dry-gas low-water anchors separately from CO2 zero gas.
- `h2o_queue_failure_audit` H2O queue failure audit: `not_attempted` - no H2O queue failure audit artifact found
  - physical_meaning: A finished H2O queue must preserve why any point failed: humidity generator state, dewpoint/reference instability, H2O ratio or signal instability, route readiness, or diagnostic pressure causes. Failed or downgraded H2O points remain evidence, but must not silently enter formal H2O fitting.
- `h2o_queue_exclusion` H2O queue abort/exclusion evidence: `not_attempted` - no H2O queue abort/exclusion artifact found
  - physical_meaning: Aborted H2O queue rows are retained as diagnostic evidence and must not enter formal fit, acceptance, or SENCO review.
- `candidate_review` Candidate coefficient review: `partial` - present_roles=candidate_review
  - physical_meaning: Only stable, role-eligible samples should enter SENCO candidate fitting and reviewer approval.
- `post_run_coefficient_executor` Post-run coefficient closure executor: `not_attempted` - post-run coefficient executor not generated
  - physical_meaning: After CO2/H2O acquisition and fit-input review, this offline executor binds per-device eligibility, controlled write package, post-write reverification plan, and archive gap list before any SENCO write is allowed.
- `full_flow_closure_readiness` Full-flow closure readiness before controlled write: `not_attempted` - full-flow closure readiness not generated
  - physical_meaning: Before controlled SENCO writes, the run must show one auditable chain from plan, raw open-flow evidence, QC decisions, candidate write package, reverification plan, archive gaps, and formal release domains. This gate is offline and does not touch COM ports or routes.
- `formal_run_status` Formal run status rollup: `pass` - formal run status JSON, markdown, gate table, and gap table are indexed
  - physical_meaning: The top-level run status separates physical-flow continuation from formal archive, database import, and release readiness without touching analyzer state.
- `controlled_write_events` Controlled coefficient write events: `not_attempted` - no controlled write artifact found
  - physical_meaning: Any SENCO write starts a new coefficient epoch and must have command, readback, approval, and rollback evidence.
- `post_write_reverification` Post-write reverification: `not_attempted` - post-write verification not attempted
  - physical_meaning: After any coefficient write, independent open-flow verification points must prove the updated measurement model.
- `evidence_bundle` Formal evidence bundle: `missing` - evidence bundle missing
  - physical_meaning: The evidence bundle freezes raw artifacts, QC, traceability, coefficient events, hashes, and report inputs for reconstruction.
- `database_import` Evidence database import: `pass` - database import summary is present
  - physical_meaning: PostgreSQL indexes traceability and audit state; raw evidence remains in hashed evidence packages.
- `reports` Run, technical, and formal calibration reports: `not_attempted` - one or more formal report artifacts missing
  - physical_meaning: Reports are the reviewer-facing summary of method, QC, traceability, uncertainty, coefficient write status, and limitations.
- `per_device_certificates` Per-device calibration and verification certificate package: `not_attempted` - per-device certificate package not generated
  - physical_meaning: Per-device certificates bind the final device identity, QC result, coefficient state, traceability, hashes, and report-release boundary.

## Traceability Checks

- none

## Artifact Roles

- `candidate_review`: `2`
- `csv_evidence`: `32`
- `database_import_summary`: `10`
- `evidence_file`: `42`
- `formal_run_status`: `2`
- `formal_run_status_gaps`: `2`
- `formal_run_status_gates`: `2`
- `formal_run_status_report`: `2`
- `full_flow_contract`: `1`
- `full_flow_live_runner_readiness`: `1`
- `full_flow_live_runner_readiness_markdown`: `1`
- `full_flow_plan`: `1`
- `full_flow_stage_manifest`: `1`
- `full_flow_stage_manifest_markdown`: `1`
- `json_evidence`: `18`
