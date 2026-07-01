# V1.5 Formal Run Status

- schema: `v1_5_formal_run_status_v1`
- overall_status: `review_required`
- current_stage: `initialization_readiness`
- formal_release_allowed: `False`
- database_import_allowed: `False`
- can_continue_physical_flow: `False`
- next_action: Generate or refresh initialization readiness before any open-flow step.

## Physical Boundaries

- offline_status_only: `True`
- opens_com_ports: `False`
- connects_postgresql: `False`
- controls_pressure: `False`
- controls_water_or_gas_routes: `False`
- writes_coefficients: `False`
- writes_device_id: `False`
- not_real_acceptance_evidence: `True`

## Gates

### initialization_readiness

- title: Initialization readiness
- status: `missing`
- source_status: ``
- source_path: `D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean\docs\v1_5_flow_contract\formal_initialization\v1_5_initialization_readiness.json`
- reason: initialization readiness sidecar missing
- next_action: Generate or refresh initialization readiness before any open-flow step.
- blocks_release: `True`
- blocks_physical_flow: `True`
- physical_meaning: Confirms SN/device_code identity contract, MODE2 runtime, 1Hz upload, neutral temperature coefficients, PostgreSQL 18 preflight, and initialization evidence.

### identity_getco_sn_traceability

- title: Identity, GETCO epoch-0, and SN traceability
- status: `missing`
- source_status: ``
- source_path: `D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean\docs\v1_5_flow_contract\identity_getco_readiness\v1_5_getco_identity_readiness.json`
- reason: identity/GETCO readiness sidecar missing
- next_action: Refresh read-only GETCO/SN identity evidence or resolve traceability review before release.
- blocks_release: `True`
- blocks_physical_flow: `True`
- physical_meaning: Binds transport COM/GA labels to protocol ID, SN/device_code, and GETCO1-9 epoch-0 coefficients so later writes and reports remain traceable.

### pre_gas_readiness

- title: Pre-gas readiness
- status: `missing`
- source_status: ``
- source_path: `D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean\docs\v1_5_flow_contract\pre_gas_readiness\v1_5_pre_gas_readiness.json`
- reason: pre-gas readiness sidecar missing
- next_action: Close pre-gas gaps before starting mature CO2/H2O open-flow queues.
- blocks_release: `True`
- blocks_physical_flow: `True`
- physical_meaning: Collects the gap list from initialization to gas-flow entry: pressure S9, route readiness, GETCO baseline, S7/S8 neutral state, CHECK timing, and database preflight.

### pressure_senco9_pre_open_flow

- title: Pressure/SENCO9 pre-open-flow check
- status: `review_required`
- source_status: `missing`
- source_path: `D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean\docs\v1_5_flow_contract\v1_5_run_evidence_status.json`
- reason: pressure_quick_check=missing
- next_action: Complete pressure/SENCO9 no-write review or controlled pressure write package before gas flow.
- blocks_release: `True`
- blocks_physical_flow: `True`
- physical_meaning: Pressure P must be traceable before CO2/H2O fitting so gas coefficients do not absorb pressure bias.

### algorithm_profile_runner_dry_run

- title: New-algorithm profile runner dry-run bundle
- status: `ready`
- source_status: `ready_for_profile_driven_runner_dry_run_review`
- source_path: `D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean\docs\v1_5_flow_contract\algorithm_profile_runner_dry_run\v1_5_algorithm_profile_runner_dry_run.json`
- reason: profile-driven new-algorithm dry-run bundle is ready: CO2/H2O=47/14 and offline boundaries hold
- next_action: Review the profile-generated 47/14 runlist, readiness gate, and dry-run queue handoff before any future runner wiring.
- blocks_release: `False`
- blocks_physical_flow: `False`
- physical_meaning: Records that the new-algorithm profile can generate CO2 47 / H2O 14 runlist evidence and dry-run mature-queue handoff plans without executing queues or modifying mature runners.

### co2_open_flow_mature_queue

- title: CO2 mature open-flow queue
- status: `review_required`
- source_status: `missing`
- source_path: `D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean\docs\v1_5_flow_contract\v1_5_run_evidence_status.json`
- reason: co2_open_flow=missing
- next_action: Run or register the mature V1.5 CO2 open-flow queue evidence.
- blocks_release: `True`
- blocks_physical_flow: `False`
- physical_meaning: CO2 calibration points must come from mature open-flow samples, not diagnostic sealed/pressure rows.

### h2o_open_flow_mature_queue

- title: H2O mature open-flow queue
- status: `review_required`
- source_status: `missing`
- source_path: `D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean\docs\v1_5_flow_contract\v1_5_run_evidence_status.json`
- reason: h2o_open_flow=missing
- next_action: Run or register the mature V1.5 H2O open-flow queue evidence.
- blocks_release: `True`
- blocks_physical_flow: `False`
- physical_meaning: H2O fitting must preserve dewpoint-backed wet points and dry-gas low-water anchors separately.

### candidate_fit_review

- title: Candidate fit/QC review
- status: `review_required`
- source_status: `partial`
- source_path: `D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean\docs\v1_5_flow_contract\v1_5_run_evidence_status.json`
- reason: candidate_review=partial
- next_action: Run no-write candidate fitting/QC review before any controlled write package.
- blocks_release: `True`
- blocks_physical_flow: `False`
- physical_meaning: Only eligible A-grade and explicitly reviewed samples should enter SENCO candidate fitting.

### post_run_write_package

- title: Post-run controlled-write package
- status: `not_attempted`
- source_status: `not_attempted`
- source_path: `D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean\docs\v1_5_flow_contract\v1_5_run_evidence_status.json`
- reason: post-run coefficient executor package has not passed
- next_action: Generate the post-run executor package with eligibility, write plan, and reverify plan.
- blocks_release: `True`
- blocks_physical_flow: `False`
- physical_meaning: The write package separates no-write review from manual authorized controlled SENCO writes.

### controlled_write_and_reverification

- title: Controlled write and post-write reverification
- status: `not_attempted`
- source_status: `not_attempted`
- source_path: `D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean\docs\v1_5_flow_contract\v1_5_run_evidence_status.json`
- reason: post-write reverification has not passed or has not been attempted
- next_action: After authorized writes, run independent post-write reverification evidence.
- blocks_release: `True`
- blocks_physical_flow: `False`
- physical_meaning: A coefficient write is not a formal release until independent open-flow reverification is present.

### formal_archive_database_release

- title: Formal archive, database, and release gate
- status: `missing`
- source_status: `closure=missing; archive=missing`
- source_path: `D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean\docs\v1_5_flow_contract\formal_archive_closure_from_full_chain\v1_5_formal_archive_closure_index.json`
- reason: closure readiness and formal archive closure sidecars missing
- next_action: Close archive/database/report traceability gaps before formal release or database import.
- blocks_release: `True`
- blocks_physical_flow: `False`
- physical_meaning: Final release binds raw evidence, coefficient epochs, reverification, reports, database indexing, and SN/device_code traceability without changing analyzer state.

## Gaps

- `initialization_readiness`: missing - initialization readiness sidecar missing (next: Generate or refresh initialization readiness before any open-flow step.)
- `identity_getco_sn_traceability`: missing - identity/GETCO readiness sidecar missing (next: Refresh read-only GETCO/SN identity evidence or resolve traceability review before release.)
- `pre_gas_readiness`: missing - pre-gas readiness sidecar missing (next: Close pre-gas gaps before starting mature CO2/H2O open-flow queues.)
- `pressure_senco9_pre_open_flow`: review_required - pressure_quick_check=missing (next: Complete pressure/SENCO9 no-write review or controlled pressure write package before gas flow.)
- `co2_open_flow_mature_queue`: review_required - co2_open_flow=missing (next: Run or register the mature V1.5 CO2 open-flow queue evidence.)
- `h2o_open_flow_mature_queue`: review_required - h2o_open_flow=missing (next: Run or register the mature V1.5 H2O open-flow queue evidence.)
- `candidate_fit_review`: review_required - candidate_review=partial (next: Run no-write candidate fitting/QC review before any controlled write package.)
- `post_run_write_package`: not_attempted - post-run coefficient executor package has not passed (next: Generate the post-run executor package with eligibility, write plan, and reverify plan.)
- `controlled_write_and_reverification`: not_attempted - post-write reverification has not passed or has not been attempted (next: After authorized writes, run independent post-write reverification evidence.)
- `formal_archive_database_release`: missing - closure readiness and formal archive closure sidecars missing (next: Close archive/database/report traceability gaps before formal release or database import.)
