# V1.5 Formal Read-Only COM Minimal Executor Stub Test Evidence

Generated: 2026-07-03

## Scope

This evidence covers the plan-only minimal read-only COM executor stub. The stub records would-execute evidence for a future executor but remains locked:

- opens_com_ports=false
- read_only_real_com_execution_allowed=false
- writes_sn=false
- writes_device_id=false
- writes_coefficients=false
- connects_postgresql=false
- controls_pressure=false
- controls_water_or_gas_routes=false
- formal_release_allowed=false
- database_import_allowed=false

## Command

```powershell
& 'C:\Users\A\AppData\Local\Programs\Python\Python313\python.exe' -m pytest tests\test_v1_5_formal_readonly_com_minimal_executor_stub.py tests\test_v1_5_full_flow_orchestration.py tests\test_v1_5_formal_flow_contract.py tests\test_v1_5_formal_run_status.py tests\test_v1_5_entrypoint_inventory.py -q
```

## Result

```text
133 passed, 2 warnings in 66.15s (0:01:06)
```

The warnings are existing `pytest.mark.v1_5_formal_gate` registration warnings.

## Boundary

This is not live execution evidence. It does not open COM, read analyzers, write SN/device_code, write SENCO coefficients, connect PostgreSQL, control pressure, or control gas/water routes.
