# V1.5 Authoritative Resume Offline Candidate Gate Test Evidence

Date: 2026-07-12

## Focused

```text
python -m pytest tests/test_v1_5_authoritative_resume_offline_candidate_gate.py tests/test_v1_5_authoritative_resume_execution_preflight.py tests/test_v1_5_authoritative_resume_executor_authorization_validator.py tests/test_v1_5_authoritative_resume_executor_controlled_design.py tests/test_v1_5_entrypoint_inventory.py -q
60 passed, 1 warning in 48.26s
```

## Compatibility

```text
python -m pytest tests/test_v1_5_formal_run_status.py tests/test_v1_5_formal_flow_contract.py -q
103 passed, 1 warning in 114.59s

python -m pytest tests/test_v1_5_mature_route_contract.py tests/test_v1_5_full_flow_orchestration.py -q
38 passed in 71.35s
```

The warnings are the existing unregistered `v1_5_formal_gate` pytest marker. No test failed.

Static validation passed with `py_compile`, Ruff, and `git diff --check`.

No COM, pressure, gas/water route, device/coefficient write, subprocess execution, PostgreSQL, release, or import action occurred.
