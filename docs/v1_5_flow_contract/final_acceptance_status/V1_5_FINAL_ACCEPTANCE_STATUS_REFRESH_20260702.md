# V1.5 Final Acceptance Status Refresh

Date: 2026-07-02

Purpose:

- Refresh the final acceptance rollup after adding the PostgreSQL 18 controlled executor design gate.
- Confirm `database_import_allowed` requires the full import gate chain, including `formal_database_import_controlled_executor_design`.
- Keep the refresh strictly offline: no PostgreSQL connection, no COM access, no gas/water route control, and no coefficient/device write.

Refresh command:

```powershell
$env:PYTHONPATH='src'
python -m gas_calibrator.tools.export_v1_5_formal_run_status `
  --run-dir docs\v1_5_flow_contract `
  --output-dir docs\v1_5_flow_contract\final_acceptance_status `
  --initialization-readiness-json docs\v1_5_flow_contract\formal_initialization\v1_5_initialization_readiness.json `
  --pre-gas-readiness-json docs\v1_5_flow_contract\pre_gas_readiness\v1_5_pre_gas_readiness.json `
  --getco-readiness-json docs\v1_5_flow_contract\identity_getco_readiness\v1_5_getco_identity_readiness.json `
  --run-evidence-status-json docs\v1_5_flow_contract\v1_5_run_evidence_status.json `
  --full-flow-closure-readiness-json docs\v1_5_flow_contract\full_flow_closure_readiness\v1_5_full_flow_closure_readiness.json `
  --archive-closure-json docs\v1_5_flow_contract\formal_archive_closure_from_full_chain\v1_5_formal_archive_closure_index.json `
  --algorithm-profile-runner-dry-run-json docs\v1_5_flow_contract\algorithm_profile_runner_dry_run\v1_5_algorithm_profile_runner_dry_run.json `
  --formal-database-dry-run-json docs\v1_5_flow_contract\formal_database_dry_run\v1_5_formal_database_dry_run.json `
  --formal-database-import-preflight-json docs\v1_5_flow_contract\formal_database_import_preflight\v1_5_formal_database_import_preflight.json `
  --formal-database-import-authorization-json docs\v1_5_flow_contract\formal_database_import_authorization\v1_5_formal_database_import_authorization.json `
  --formal-database-import-command-contract-json docs\v1_5_flow_contract\formal_database_import_command_contract\v1_5_formal_database_import_command_contract.json `
  --formal-database-import-blocked-executor-json docs\v1_5_flow_contract\formal_database_import_blocked_executor\v1_5_formal_database_import_blocked_executor.json `
  --formal-database-import-controlled-executor-design-json docs\v1_5_flow_contract\formal_database_import_controlled_executor_design\v1_5_formal_database_import_controlled_executor_design.json
```

Result:

```text
overall_status=review_required
current_stage=initialization_readiness
formal_release_allowed=false
database_import_allowed=false
can_continue_physical_flow=false
connects_postgresql=false
real_import_execution_allowed=false
database_written=false
not_real_acceptance_evidence=true
```

Gate chain:

- `formal_database_dry_run`
- `formal_database_import_preflight`
- `formal_database_import_authorization`
- `formal_database_import_command_contract`
- `formal_database_import_blocked_executor`
- `formal_database_import_controlled_executor_design`

Interpretation:

V1.5 structure and offline status rollup are current, but this is not a real production database import. The PostgreSQL 18 chain remains locked until a separate future controlled executor adds explicit execution authorization, transaction handling, readback evidence, rollback handling, and post-commit hold rules.

Focused validation:

```powershell
python -m pytest `
  tests\test_v1_5_formal_run_status.py `
  tests\test_v1_5_operation_console.py `
  tests\test_v1_5_full_flow_orchestration.py `
  tests\test_v1_5_run_evidence_status.py `
  -q
```

```text
71 passed in 20.00s
```
