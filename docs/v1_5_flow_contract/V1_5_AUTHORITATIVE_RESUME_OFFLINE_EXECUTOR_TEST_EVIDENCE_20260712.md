# V1.5 Authoritative Resume Offline Executor Test Evidence

Date: 2026-07-12

## Focused

```text
python -m pytest tests/test_v1_5_authoritative_resume_offline_executor.py tests/test_v1_5_authoritative_resume_offline_candidate_gate.py tests/test_v1_5_authoritative_resume_execution_preflight.py tests/test_v1_5_authoritative_resume_executor_authorization_validator.py tests/test_v1_5_entrypoint_inventory.py -q
61 passed, 1 warning in 43.05s
```

The first focused run exposed that argparse abbreviated `--execute` to `--execute-offline-step`. The CLI now uses `allow_abbrev=false`; the complete focused suite above passed after that P1 fix.

## Compatibility

```text
python -m pytest tests/test_v1_5_formal_run_status.py tests/test_v1_5_formal_flow_contract.py -q
103 passed, 1 warning in 117.66s

python -m pytest tests/test_v1_5_mature_route_contract.py tests/test_v1_5_full_flow_orchestration.py -q
38 passed in 71.43s
```

The warnings are the existing unregistered `v1_5_formal_gate` pytest marker. No final test failed.

Static validation passed with `py_compile`, Ruff, and `git diff --check`.

All execution tests used an injected fake subprocess. No real V1.5 command, COM, pressure, gas/water route, device/coefficient write, PostgreSQL, release, or import action occurred.
