# V1.5 Formal Run Status

- schema: `v1_5_formal_run_status_v1`
- overall_status: `blocked`
- current_stage: `initialization_readiness`
- formal_release_allowed: `False`
- database_import_allowed: `False`
- can_continue_physical_flow: `False`
- next_action: Generate or refresh initialization readiness before any open-flow step.

## Physical Boundaries

- offline_status_only: `True`
- opens_com_ports: `False`
- connects_postgresql: `False`
- real_import_execution_allowed: `False`
- database_written: `False`
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
- source_path: ``
- reason: initialization readiness sidecar missing
- next_action: Generate or refresh initialization readiness before any open-flow step.
- blocks_release: `True`
- blocks_physical_flow: `True`
- physical_meaning: Confirms SN/device_code identity contract, MODE2 runtime, 1Hz upload, neutral temperature coefficients, PostgreSQL 18 preflight, and initialization evidence.

### route_physical_recovery_readiness

- title: Route physical recovery readiness
- status: `blocked`
- source_status: `blocked`
- source_path: `D:\gas_calibrator\_worktrees\v1_5_post64_main_health_20260710\docs\v1_5_flow_contract\route_physical_recovery_readiness\v1_5_route_physical_recovery_readiness.json`
- reason: route physical blockers remain: blocker_count=4; PACE vent, pressure gauge, dry-gas dewpoint, or fresh queue policy is not recovered
- next_action: Recover PACE vent, pressure-gauge readback, and dry-gas dewpoint stability; then bind the next run to a fresh 0613/0620/0621 canonical queue before starting continuous CO2/H2O.
- blocks_release: `False`
- blocks_physical_flow: `True`
- physical_meaning: Prevents PACE vent NO_RESPONSE, pressure-gauge NO_RESPONSE, dry-gas dewpoint rebound, stale running manifests, and direct/retry/manual segments from being treated as a valid next continuous run.

### identity_getco_sn_traceability

- title: Identity, GETCO epoch-0, and SN traceability
- status: `missing`
- source_status: ``
- source_path: ``
- reason: identity/GETCO readiness sidecar missing
- next_action: Refresh read-only GETCO/SN identity evidence or resolve traceability review before release.
- blocks_release: `True`
- blocks_physical_flow: `True`
- physical_meaning: Binds transport COM/GA labels to protocol ID, SN/device_code, and GETCO1-9 epoch-0 coefficients so later writes and reports remain traceable.

### pre_gas_readiness

- title: Pre-gas readiness
- status: `missing`
- source_status: ``
- source_path: ``
- reason: pre-gas readiness sidecar missing
- next_action: Close pre-gas gaps before starting mature CO2/H2O open-flow queues.
- blocks_release: `True`
- blocks_physical_flow: `True`
- physical_meaning: Collects the gap list from initialization to gas-flow entry: pressure S9, route readiness, GETCO baseline, S7/S8 neutral state, CHECK timing, and database preflight.

### pressure_senco9_pre_open_flow

- title: Pressure/SENCO9 pre-open-flow check
- status: `review_required`
- source_status: `missing`
- source_path: `D:\gas_calibrator\_worktrees\v1_5_post64_main_health_20260710\docs\v1_5_flow_contract\v1_5_run_evidence_status.json`
- reason: pressure_quick_check=missing
- next_action: Complete pressure/SENCO9 no-write review or controlled pressure write package before gas flow.
- blocks_release: `True`
- blocks_physical_flow: `True`
- physical_meaning: Pressure P must be traceable before CO2/H2O fitting so gas coefficients do not absorb pressure bias.

### algorithm_profile_runner_dry_run

- title: New-algorithm profile runner dry-run bundle
- status: `ready`
- source_status: `ready_for_profile_driven_runner_dry_run_review`
- source_path: `D:\gas_calibrator\_worktrees\v1_5_post64_main_health_20260710\docs\v1_5_flow_contract\algorithm_profile_runner_dry_run\v1_5_algorithm_profile_runner_dry_run.json`
- reason: profile-driven new-algorithm dry-run bundle is ready: CO2/H2O=47/14 and offline boundaries hold
- next_action: Review the profile-generated 47/14 runlist, readiness gate, and dry-run queue handoff before any future runner wiring.
- blocks_release: `False`
- blocks_physical_flow: `False`
- physical_meaning: Records that the new-algorithm profile can generate CO2 47 / H2O 14 runlist evidence and dry-run mature-queue handoff plans without executing queues or modifying mature runners.

### full_flow_automation_closure

- title: Full-flow automation closure map
- status: `ready`
- source_status: `review_ready`
- source_path: `D:\gas_calibrator\_worktrees\v1_5_post64_main_health_20260710\docs\v1_5_flow_contract\full_flow_automation_closure\v1_5_full_flow_automation_closure.json`
- reason: V1.5 structure is organized and mature baselines are locked; full production automation still has 9 gated handoff(s)
- next_action: Use this closure map to decide the next automation PR. Do not interpret it as live-route, coefficient-write, PostgreSQL import, or formal release evidence.
- blocks_release: `False`
- blocks_physical_flow: `False`
- physical_meaning: Summarizes the current V1.5 production automation boundary: the formal structure is organized around 0613 fitting and 0620/0621 mature physical routes, while live execution, writes, reverify, archive, and database import remain explicit gates.

### formal_database_dry_run

- title: PostgreSQL 18 formal database dry-run contract
- status: `ready`
- source_status: `ready_for_postgresql18_schema_dry_run_review`
- source_path: `D:\gas_calibrator\_worktrees\v1_5_post64_main_health_20260710\docs\v1_5_flow_contract\formal_database_dry_run\v1_5_formal_database_dry_run.json`
- reason: PostgreSQL 18 schema/insert dry-run is ready while real import remains unauthorized
- next_action: Review the PostgreSQL 18 schema, SN/device_code identity, insert-preview, and dry-run boundaries before enabling any separate database import step.
- blocks_release: `False`
- blocks_physical_flow: `False`
- physical_meaning: Checks database schema and insert-preview semantics without connecting to PostgreSQL or importing data; this keeps database readiness separate from formal archive release.

### formal_database_import_preflight

- title: PostgreSQL 18 formal database import preflight
- status: `review_required`
- source_status: `review_required`
- source_path: `D:\gas_calibrator\_worktrees\v1_5_post64_main_health_20260710\docs\v1_5_flow_contract\formal_database_import_preflight\v1_5_formal_database_import_preflight.json`
- reason: source_status=review_required; review_required_count=1; dsn_configured=False
- next_action: Review DSN configuration, migration lock, archive-release dependency, and explicit import authorization before running any separate production database import.
- blocks_release: `False`
- blocks_physical_flow: `False`
- physical_meaning: Checks that a production database import could be reviewed without opening PostgreSQL, applying migrations, or importing rows; this separates preflight evidence from real import execution.

### formal_database_import_authorization

- title: PostgreSQL 18 formal database import authorization
- status: `review_required`
- source_status: `review_required`
- source_path: `D:\gas_calibrator\_worktrees\v1_5_post64_main_health_20260710\docs\v1_5_flow_contract\formal_database_import_authorization\v1_5_formal_database_import_authorization.json`
- reason: source_status=review_required; review_required_count=3; preflight_ready=False; archive_release_ready=False; manual_authorization_ready=False; database_import_allowed=False
- next_action: Complete archive release and manual import authorization, then run a separate controlled database import command that consumes this authorization artifact.
- blocks_release: `False`
- blocks_physical_flow: `False`
- physical_meaning: Separates manual database-import authorization from both preflight review and actual PostgreSQL writes; the status artifact itself remains no-connect/no-import.

### formal_database_import_command_contract

- title: PostgreSQL 18 formal database import command contract
- status: `review_required`
- source_status: `review_required`
- source_path: `D:\gas_calibrator\_worktrees\v1_5_post64_main_health_20260710\docs\v1_5_flow_contract\formal_database_import_command_contract\v1_5_formal_database_import_command_contract.json`
- reason: source_status=review_required; review_required_count=4; authorization_ready=False; preflight_ready=False; archive_release_ready=False; evidence_bundle_ready=False; command_contract_ready=False
- next_action: Review the no-connect import command contract. A separate controlled command must consume the contract, authorization, preflight, archive, evidence bundle, and DSN env before any production import.
- blocks_release: `False`
- blocks_physical_flow: `False`
- physical_meaning: Separates manual import authorization from executable command inputs and keeps migration/import execution locked off until a future controlled command re-checks the full evidence chain.

### formal_database_import_blocked_executor

- title: PostgreSQL 18 formal database import blocked executor
- status: `review_required`
- source_status: `review_required`
- source_path: `D:\gas_calibrator\_worktrees\v1_5_post64_main_health_20260710\docs\v1_5_flow_contract\formal_database_import_blocked_executor\v1_5_formal_database_import_blocked_executor.json`
- reason: blocked executor input review_required_count=3; regenerate command contract/input references before executor review
- next_action: Keep database import locked. Build a separate controlled executor with double authorization before any PostgreSQL connection, migration, or row import is allowed.
- blocks_release: `False`
- blocks_physical_flow: `False`
- physical_meaning: Proves the future import command currently consumes reviewed inputs but remains a no-connect, no-migration, no-write stub rather than a production import.

### formal_database_import_controlled_executor_design

- title: PostgreSQL 18 controlled import executor design
- status: `ready`
- source_status: `ready_for_controlled_import_executor_design_review`
- source_path: `D:\gas_calibrator\_worktrees\v1_5_post64_main_health_20260710\docs\v1_5_flow_contract\formal_database_import_controlled_executor_design\v1_5_formal_database_import_controlled_executor_design.json`
- reason: controlled PostgreSQL 18 import executor design is ready; execution remains blocked
- next_action: Use this design only as future implementation guidance. Do not connect PostgreSQL until a separate controlled executor adds explicit execute authorization, transaction, readback, rollback, and import evidence.
- blocks_release: `False`
- blocks_physical_flow: `False`
- physical_meaning: Defines the future real-import safety contract while preserving the current no-connect, no-migration, no-write V1.5 boundary.

### co2_open_flow_mature_queue

- title: CO2 mature open-flow queue
- status: `review_required`
- source_status: `missing`
- source_path: `D:\gas_calibrator\_worktrees\v1_5_post64_main_health_20260710\docs\v1_5_flow_contract\v1_5_run_evidence_status.json`
- reason: co2_open_flow=missing
- next_action: Run or register the mature V1.5 CO2 open-flow queue evidence.
- blocks_release: `True`
- blocks_physical_flow: `False`
- physical_meaning: CO2 calibration points must come from mature open-flow samples, not diagnostic sealed/pressure rows.

### h2o_open_flow_mature_queue

- title: H2O mature open-flow queue
- status: `review_required`
- source_status: `missing`
- source_path: `D:\gas_calibrator\_worktrees\v1_5_post64_main_health_20260710\docs\v1_5_flow_contract\v1_5_run_evidence_status.json`
- reason: h2o_open_flow=missing
- next_action: Run or register the mature V1.5 H2O open-flow queue evidence.
- blocks_release: `True`
- blocks_physical_flow: `False`
- physical_meaning: H2O fitting must preserve dewpoint-backed wet points and dry-gas low-water anchors separately.

### candidate_fit_review

- title: Candidate fit/QC review
- status: `review_required`
- source_status: `partial`
- source_path: `D:\gas_calibrator\_worktrees\v1_5_post64_main_health_20260710\docs\v1_5_flow_contract\v1_5_run_evidence_status.json`
- reason: candidate_review=partial
- next_action: Run no-write candidate fitting/QC review before any controlled write package.
- blocks_release: `True`
- blocks_physical_flow: `False`
- physical_meaning: Only eligible A-grade and explicitly reviewed samples should enter SENCO candidate fitting.

### post_run_write_package

- title: Post-run controlled-write package
- status: `not_attempted`
- source_status: `not_attempted`
- source_path: `D:\gas_calibrator\_worktrees\v1_5_post64_main_health_20260710\docs\v1_5_flow_contract\v1_5_run_evidence_status.json`
- reason: post-run coefficient executor package has not passed
- next_action: Generate the post-run executor package with eligibility, write plan, and reverify plan.
- blocks_release: `True`
- blocks_physical_flow: `False`
- physical_meaning: The write package separates no-write review from manual authorized controlled SENCO writes.

### controlled_write_and_reverification

- title: Controlled write and post-write reverification
- status: `not_attempted`
- source_status: `not_attempted`
- source_path: `D:\gas_calibrator\_worktrees\v1_5_post64_main_health_20260710\docs\v1_5_flow_contract\v1_5_run_evidence_status.json`
- reason: post-write reverification has not passed or has not been attempted
- next_action: After authorized writes, run independent post-write reverification evidence.
- blocks_release: `True`
- blocks_physical_flow: `False`
- physical_meaning: A coefficient write is not a formal release until independent open-flow reverification is present.

### formal_archive_database_release

- title: Formal archive, database, and release gate
- status: `missing`
- source_status: `closure=missing; archive=missing`
- source_path: ``
- reason: closure readiness and formal archive closure sidecars missing
- next_action: Close archive/database/report traceability gaps before formal release or database import.
- blocks_release: `True`
- blocks_physical_flow: `False`
- physical_meaning: Final release binds raw evidence, coefficient epochs, reverification, reports, database indexing, and SN/device_code traceability without changing analyzer state.

## Gaps

- `initialization_readiness`: missing - initialization readiness sidecar missing (next: Generate or refresh initialization readiness before any open-flow step.)
- `route_physical_recovery_readiness`: blocked - route physical blockers remain: blocker_count=4; PACE vent, pressure gauge, dry-gas dewpoint, or fresh queue policy is not recovered (next: Recover PACE vent, pressure-gauge readback, and dry-gas dewpoint stability; then bind the next run to a fresh 0613/0620/0621 canonical queue before starting continuous CO2/H2O.)
- `identity_getco_sn_traceability`: missing - identity/GETCO readiness sidecar missing (next: Refresh read-only GETCO/SN identity evidence or resolve traceability review before release.)
- `pre_gas_readiness`: missing - pre-gas readiness sidecar missing (next: Close pre-gas gaps before starting mature CO2/H2O open-flow queues.)
- `pressure_senco9_pre_open_flow`: review_required - pressure_quick_check=missing (next: Complete pressure/SENCO9 no-write review or controlled pressure write package before gas flow.)
- `formal_database_import_preflight`: review_required - source_status=review_required; review_required_count=1; dsn_configured=False (next: Review DSN configuration, migration lock, archive-release dependency, and explicit import authorization before running any separate production database import.)
- `formal_database_import_authorization`: review_required - source_status=review_required; review_required_count=3; preflight_ready=False; archive_release_ready=False; manual_authorization_ready=False; database_import_allowed=False (next: Complete archive release and manual import authorization, then run a separate controlled database import command that consumes this authorization artifact.)
- `formal_database_import_command_contract`: review_required - source_status=review_required; review_required_count=4; authorization_ready=False; preflight_ready=False; archive_release_ready=False; evidence_bundle_ready=False; command_contract_ready=False (next: Review the no-connect import command contract. A separate controlled command must consume the contract, authorization, preflight, archive, evidence bundle, and DSN env before any production import.)
- `formal_database_import_blocked_executor`: review_required - blocked executor input review_required_count=3; regenerate command contract/input references before executor review (next: Keep database import locked. Build a separate controlled executor with double authorization before any PostgreSQL connection, migration, or row import is allowed.)
- `co2_open_flow_mature_queue`: review_required - co2_open_flow=missing (next: Run or register the mature V1.5 CO2 open-flow queue evidence.)
- `h2o_open_flow_mature_queue`: review_required - h2o_open_flow=missing (next: Run or register the mature V1.5 H2O open-flow queue evidence.)
- `candidate_fit_review`: review_required - candidate_review=partial (next: Run no-write candidate fitting/QC review before any controlled write package.)
- `post_run_write_package`: not_attempted - post-run coefficient executor package has not passed (next: Generate the post-run executor package with eligibility, write plan, and reverify plan.)
- `controlled_write_and_reverification`: not_attempted - post-write reverification has not passed or has not been attempted (next: After authorized writes, run independent post-write reverification evidence.)
- `formal_archive_database_release`: missing - closure readiness and formal archive closure sidecars missing (next: Close archive/database/report traceability gaps before formal release or database import.)
