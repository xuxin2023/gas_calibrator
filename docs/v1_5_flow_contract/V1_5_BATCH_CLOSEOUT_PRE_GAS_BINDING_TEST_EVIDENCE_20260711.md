# V1.5 Batch Closeout Pre-Gas Binding Test Evidence

Date: 2026-07-11

## Scope

This evidence covers the offline binding of completed batch initialization evidence into the canonical V1.5 full-flow plan after pressure/SENCO9 completion and before temperature review or mature CO2/H2O open-flow execution.

## Focused Regression

Command:

```powershell
$env:PYTHONPATH='src'
python -m pytest tests\test_v1_5_batch_initialization_closeout_index.py tests\test_v1_5_full_flow_orchestration.py tests\test_v1_5_formal_flow_contract.py tests\test_v1_5_formal_run_status.py -q
```

Result:

```text
126 passed, 1 warning in 175.80s
```

The warning is the existing unregistered `v1_5_formal_gate` pytest marker.

## Compatibility Regression

Command:

```powershell
$env:PYTHONPATH='src'
python -m pytest tests\test_v1_5_algorithm_route_profiles.py tests\test_v1_5_entrypoint_inventory.py tests\test_v1_5_formal_initialization_runner.py tests\test_v1_5_initialization_readiness.py tests\test_v1_5_mature_route_contract.py tests\test_v1_5_pressure_channel_completion.py tests\test_v1_5_pressure_channel_completion_db.py tests\test_v1_5_pre_gas_readiness.py -q
```

Result:

```text
84 passed, 1 warning in 59.38s
```

The warning is the existing unregistered `v1_5_formal_gate` pytest marker.

## Verified Boundaries

- the batch closeout step follows pressure-channel completion
- the batch closeout step precedes temperature review and mature CO2/H2O queues
- review-required or incomplete closeout returns a non-zero CLI status
- incomplete closeout sets `can_continue_physical_flow=false`
- 0613 fitting and 0620/0621 mature route baselines remain explicit
- legacy 45/13 and new-algorithm 47/14 profile contracts remain unchanged
- protected mature queue, sampling, workflow runner, protocol, default configuration, and `run_app.py` files have no diff
- no COM, device write, PostgreSQL connection, pressure control, gas-route control, or water-route control was used by this change or its tests
