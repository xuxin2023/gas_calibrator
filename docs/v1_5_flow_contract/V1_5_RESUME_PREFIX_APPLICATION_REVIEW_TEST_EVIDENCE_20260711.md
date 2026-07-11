# V1.5 Resume Prefix Application Review Test Evidence

Date: 2026-07-11

## Focused Regression

```powershell
$env:PYTHONPATH='src'
python -m pytest tests\test_v1_5_resume_prefix_application_review.py tests\test_v1_5_post_closeout_resume_gate.py tests\test_v1_5_batch_initialization_closeout_index.py tests\test_v1_5_full_flow_orchestration.py tests\test_v1_5_formal_flow_contract.py tests\test_v1_5_formal_run_status.py -q
```

Result:

```text
143 passed, 1 warning in 171.95s
```

## Compatibility Regression

```powershell
$env:PYTHONPATH='src'
python -m pytest tests\test_v1_5_algorithm_route_profiles.py tests\test_v1_5_entrypoint_inventory.py tests\test_v1_5_formal_initialization_runner.py tests\test_v1_5_initialization_readiness.py tests\test_v1_5_mature_route_contract.py tests\test_v1_5_pressure_channel_completion.py tests\test_v1_5_pressure_channel_completion_db.py tests\test_v1_5_pre_gas_readiness.py -q
```

Result:

```text
84 passed, 1 warning in 54.87s
```

Both warnings are the existing unregistered `v1_5_formal_gate` pytest marker.

## Verified Boundaries

- the reviewed completed-step prefix is exact and contiguous
- arbitrary, missing, reordered, or extra completed steps are blocked
- plan, batch closeout, and resume-gate hashes are rebound and rechecked
- an alternate resume-gate path is blocked
- state and execution flags are forbidden
- the state preview stops at temperature review
- no authoritative state is written
- route authorization remains separate
- formal status fails closed when the review is missing, blocked, or stale
- legacy 45/13 and new-algorithm 47/14 contracts remain unchanged
- mature CO2/H2O queue, sampling, protocol, runner, default configuration, and `run_app.py` protected files have no diff
