# V1.5 Formal Read-Only COM Minimal Executor Review Test Evidence

## Scope

This package adds an offline, blocked-by-default implementation review for a future minimal V1.5 read-only COM executor.

It consumes the read-only COM plan preview and defines:

- required future output evidence artifacts
- future failure/hold matrix
- old-algorithm CHECK skip enforcement
- new-algorithm CHECK review hold behavior
- >=1s serial command/retry pacing expectation
- no-write / no-database / no-pressure / no-route boundaries

It does not implement real COM I/O.

## Safety Boundary

- opens COM: `false`
- reads analyzers: `false`
- writes SN/device_code: `false`
- writes SENCO/coefficients: `false`
- connects PostgreSQL: `false`
- controls pressure: `false`
- controls gas/water route: `false`
- formal release allowed: `false`
- database import allowed: `false`
- real acceptance evidence: `false`

## Focused Test Command

```powershell
& 'C:\Users\A\AppData\Local\Programs\Python\Python313\python.exe' -m pytest tests\test_v1_5_formal_readonly_com_minimal_executor_review.py tests\test_v1_5_full_flow_orchestration.py tests\test_v1_5_formal_flow_contract.py tests\test_v1_5_formal_run_status.py tests\test_v1_5_entrypoint_inventory.py -q
```

## Result

```text
130 passed, 2 warnings in 63.00s
```

The two warnings are the existing unregistered `v1_5_formal_gate` pytest marks in formal-flow tests.
