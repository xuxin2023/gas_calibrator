# V1.5 Resume State Verification Formal Status Test Evidence

Tests are offline and do not write authoritative state or touch COM, analyzers, pressure, routes, PostgreSQL, release, or import state.

## Focused

```text
python -m pytest tests/test_v1_5_formal_run_status.py tests/test_v1_5_authoritative_resume_state_post_write_verification.py tests/test_v1_5_authoritative_resume_state_atomic_writer.py tests/test_v1_5_entrypoint_inventory.py -q
115 passed, 1 warning in 97.43s
```

## Compatibility

```text
python -m pytest tests/test_v1_5_full_flow_orchestration.py tests/test_v1_5_formal_flow_contract.py tests/test_v1_5_mature_route_contract.py tests/test_v1_5_algorithm_route_profiles.py tests/test_v1_5_pre_gas_readiness.py tests/test_v1_5_pressure_s9_readiness_index.py -q
92 passed, 1 warning in 135.88s
```

Both warnings are the existing unregistered `v1_5_formal_gate` marker.
