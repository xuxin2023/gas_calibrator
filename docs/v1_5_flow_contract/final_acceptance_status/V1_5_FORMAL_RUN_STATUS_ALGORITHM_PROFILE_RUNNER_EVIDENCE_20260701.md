# V1.5 formal run status algorithm profile runner evidence

- Date: 2026-07-01
- Scope: wire the offline new-algorithm profile runner dry-run bundle into the final formal run status rollup.
- Boundary: no COM, no PostgreSQL, no route control, no coefficient writes, no SN/device ID writes, no mature runner modification, no real acceptance.

## Rollup behavior

- `algorithm_profile_runner_dry_run` is included only when `v1_5_algorithm_profile_runner_dry_run.json` exists or is passed explicitly.
- A ready bundle is shown as `ready` with `release_gate=false`, `blocks_release=false`, and `blocks_physical_flow=false`.
- A blocked/review bundle becomes a review item for new-algorithm runner preparation, but it does not turn legacy mature-route release gates into a hidden new-algorithm requirement.

## Export command

```powershell
$env:PYTHONPATH='src'; python -m gas_calibrator.tools.export_v1_5_formal_run_status --run-dir docs\v1_5_flow_contract --output-dir docs\v1_5_flow_contract\final_acceptance_status
```

## Export result

```text
overall_status=review_required
current_stage=initialization_readiness
formal_release_allowed=false
database_import_allowed=false
can_continue_physical_flow=false
algorithm_profile_runner_dry_run=ready
```

## Focused pytest

```powershell
python -m pytest tests\test_v1_5_formal_run_status.py tests\test_v1_5_algorithm_profile_runner_dry_run.py tests\test_v1_5_algorithm_runner_integration_dry_run.py tests\test_v1_5_algorithm_runlist_readiness.py tests\test_v1_5_entrypoint_inventory.py -q
```

```text
............................................                             [100%]
44 passed in 6.48s
```
