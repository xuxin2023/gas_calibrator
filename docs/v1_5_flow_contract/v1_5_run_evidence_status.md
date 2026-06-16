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

## Stages

- `full_flow_contract_gate` Full-flow contract audit gate: `pass` - contract_status=pass
  - physical_meaning: Before any formal run advance, the plan must prove pressure-first, open-flow component sampling, no V2 real COM, and no auto-write boundaries.
- `plan_traceability` Plan and traceability snapshots: `missing` - formal plan or pressure reference snapshot missing
  - physical_meaning: The calibration result must bind to a plan, standard/reference certificates, and config identity before sampling.
- `identity_getco_epoch0` Analyzer identity and GETCO epoch 0: `missing` - GETCO snapshot or runtime identity-bound config missing
  - physical_meaning: COM ports are transport only; analyzer device IDs and GETCO1-9 define the pre-calibration coefficient epoch.
- `pressure_quick_check` Pressure channel quick check: `missing` - pressure quick-check evidence missing
  - physical_meaning: Analyzer pressure P is an input to CO2/H2O compensation and must be verified before component calibration.
- `co2_open_flow` CO2 open-flow evidence: `missing` - CO2 open-flow sample evidence missing
  - physical_meaning: CO2 fitting must be based on clean open-flow samples and factory ratio evidence, not sealed contaminated pressure points.
- `h2o_open_flow` H2O open-flow evidence: `missing` - H2O open-flow sample evidence missing
  - physical_meaning: H2O fitting must preserve dewpoint/reference-backed water evidence and dry-gas low-water anchors separately from CO2 zero gas.
- `candidate_review` Candidate coefficient review: `not_attempted` - candidate review not attempted
  - physical_meaning: Only stable, role-eligible samples should enter SENCO candidate fitting and reviewer approval.
- `controlled_write_events` Controlled coefficient write events: `not_attempted` - no controlled write artifact found
  - physical_meaning: Any SENCO write starts a new coefficient epoch and must have command, readback, approval, and rollback evidence.
- `post_write_reverification` Post-write reverification: `not_attempted` - post-write verification not attempted
  - physical_meaning: After any coefficient write, independent open-flow verification points must prove the updated measurement model.
- `evidence_bundle` Formal evidence bundle: `missing` - evidence bundle missing
  - physical_meaning: The evidence bundle freezes raw artifacts, QC, traceability, coefficient events, hashes, and report inputs for reconstruction.
- `database_import` Evidence database import: `not_attempted` - database import not attempted or summary not found
  - physical_meaning: PostgreSQL indexes traceability and audit state; raw evidence remains in hashed evidence packages.
- `reports` Run, technical, and formal calibration reports: `not_attempted` - one or more formal report artifacts missing
  - physical_meaning: Reports are the reviewer-facing summary of method, QC, traceability, uncertainty, coefficient write status, and limitations.

## Traceability Checks

- none

## Artifact Roles

- `evidence_file`: `5`
- `full_flow_contract`: `1`
- `full_flow_plan`: `1`
- `json_evidence`: `1`
