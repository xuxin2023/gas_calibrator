# V1.5 Authoritative Resume State Atomic Writer Test Evidence

## Scope

This evidence covers the manual-authorized atomic writer, its separate write authorization, lock-bound compare-and-swap behavior, snapshots, atomic replacement, readback, rollback, CLI default lock, and entrypoint classification.

All state writes in this test package use pytest temporary directories. No real V1.5 state, COM port, analyzer, pressure controller, route, PostgreSQL database, or mature CO2/H2O runner is touched.

## Focused Result

Command:

```text
python -m pytest tests/test_v1_5_authoritative_resume_state_atomic_writer.py tests/test_v1_5_authoritative_resume_state_controlled_write_preflight.py tests/test_v1_5_authoritative_resume_state_writer_blocked_executor.py tests/test_v1_5_authoritative_resume_state_writer_design.py tests/test_v1_5_entrypoint_inventory.py tests/test_v1_5_formal_initialization_runner.py -q
```

Result:

```text
76 passed, 1 warning in 53.32s
```

The warning is the existing unregistered `v1_5_formal_gate` pytest marker.

## Compatibility Result

Command:

```text
python -m pytest tests/test_v1_5_full_flow_orchestration.py tests/test_v1_5_formal_flow_contract.py tests/test_v1_5_formal_run_status.py tests/test_v1_5_mature_route_contract.py tests/test_v1_5_algorithm_route_profiles.py tests/test_v1_5_pre_gas_readiness.py tests/test_v1_5_pressure_s9_readiness_index.py -q
```

Result:

```text
154 passed, 1 warning in 108.34s
```

The warning is the existing unregistered `v1_5_formal_gate` pytest marker.

## Review Boundary

- the #93 preflight-only packet cannot authorize an actual write;
- operator, reviewer, and approver must be distinct;
- the final target SHA256 and candidate SHA256 are checked under the exclusive writer lock;
- stale state and lock conflicts block before replacement;
- existing state is snapshotted before replacement;
- post-replacement mismatch rolls back;
- no COM, device write, pressure/route control, PostgreSQL, release, or import permission is opened;
- this evidence is not real acceptance evidence.
