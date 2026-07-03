# V1.5 formal initialization read-only COM preflight controlled executor design test evidence

Generated: 2026-07-03

Scope:

- `tests/test_v1_5_formal_initialization_readonly_com_preflight_controlled_executor_design.py`
- `tests/test_v1_5_full_flow_orchestration.py`
- `tests/test_v1_5_formal_flow_contract.py`
- `tests/test_v1_5_formal_run_status.py`
- `tests/test_v1_5_entrypoint_inventory.py`

Command:

```powershell
$env:PYTHONPATH=(Resolve-Path 'src').Path
& 'C:\Users\A\AppData\Local\Programs\Python\Python313\python.exe' -m pytest tests\test_v1_5_formal_initialization_readonly_com_preflight_controlled_executor_design.py tests\test_v1_5_full_flow_orchestration.py tests\test_v1_5_formal_flow_contract.py tests\test_v1_5_formal_run_status.py tests\test_v1_5_entrypoint_inventory.py -q
```

Result:

```text
116 passed, 2 warnings in 77.82s (0:01:17)
```

Warnings:

- `PytestUnknownMarkWarning: Unknown pytest.mark.v1_5_formal_gate` in `tests/test_v1_5_formal_flow_contract.py`
- `PytestUnknownMarkWarning: Unknown pytest.mark.v1_5_formal_gate` in `tests/test_v1_5_entrypoint_inventory.py`

Boundary:

- `opens_com_ports=false`
- `read_only_real_com_execution_allowed=false`
- `controlled_write_execution_allowed=false`
- `writes_sn=false`
- `writes_device_id=false`
- `writes_coefficients=false`
- `connects_postgresql=false`
- `controls_pressure=false`
- `controls_water_or_gas_routes=false`
- `not_real_acceptance_evidence=true`

Conclusion:

This package is ready for Draft PR review as an offline design guard only. It does not implement real COM execution and does not authorize live initialization.
