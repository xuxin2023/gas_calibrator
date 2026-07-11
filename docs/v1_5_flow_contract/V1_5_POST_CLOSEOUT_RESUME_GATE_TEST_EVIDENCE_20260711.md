# V1.5 Post-Closeout Resume Gate Test Evidence

Date: 2026-07-11

## Focused Regression

```powershell
$env:PYTHONPATH='src'
python -m pytest tests\test_v1_5_post_closeout_resume_gate.py tests\test_v1_5_batch_initialization_closeout_index.py tests\test_v1_5_full_flow_orchestration.py tests\test_v1_5_formal_flow_contract.py tests\test_v1_5_formal_run_status.py -q
```

Result:

```text
134 passed, 1 warning in 179.28s
```

## Compatibility Regression

```powershell
$env:PYTHONPATH='src'
python -m pytest tests\test_v1_5_algorithm_route_profiles.py tests\test_v1_5_entrypoint_inventory.py tests\test_v1_5_formal_initialization_runner.py tests\test_v1_5_initialization_readiness.py tests\test_v1_5_mature_route_contract.py tests\test_v1_5_pressure_channel_completion.py tests\test_v1_5_pressure_channel_completion_db.py tests\test_v1_5_pre_gas_readiness.py -q
```

Result:

```text
84 passed, 1 warning in 57.43s
```

Both warnings are the existing unregistered `v1_5_formal_gate` pytest marker.

## Verified Boundaries

- incomplete batch closeout produces no resume prefix
- canonical plan and mature queue modules are required
- 0624 and noncanonical queue references are blocked
- plan and closeout SHA256 values are bound into the resume artifact
- source changes after gate generation block formal physical-flow continuation
- a resume gate bound to a different batch-closeout path is blocked
- the gate does not apply completed steps or execute commands
- route authorization remains required
- legacy 45/13 and new-algorithm 47/14 contracts remain unchanged
- mature CO2/H2O queue, sampling, protocol, runner, default configuration, and `run_app.py` protected files have no diff
