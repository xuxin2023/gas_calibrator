# V1.5 formal read-only COM structured confirmation test evidence

Generated on: 2026-07-03

Scope:

- `tests/test_v1_5_formal_readonly_com_execution_packet_validator.py`
- `tests/test_v1_5_formal_flow_contract.py`
- `tests/test_v1_5_formal_run_status.py`
- `tests/test_v1_5_full_flow_orchestration.py`
- `tests/test_v1_5_entrypoint_inventory.py`

Command:

```powershell
& 'C:\Users\A\AppData\Local\Programs\Python\Python313\python.exe' -m pytest tests\test_v1_5_formal_readonly_com_execution_packet_validator.py tests\test_v1_5_formal_flow_contract.py tests\test_v1_5_formal_run_status.py tests\test_v1_5_full_flow_orchestration.py tests\test_v1_5_entrypoint_inventory.py -q
```

Result:

```text
131 passed, 2 warnings in 87.14s (0:01:27)
```

Warning notes:

- `PytestUnknownMarkWarning: Unknown pytest.mark.v1_5_formal_gate` in existing formal gate tests.
- The warnings do not come from the structured confirmation validator change.

Boundary:

- No COM ports opened.
- No analyzer reads executed.
- No SN/device_code writes.
- No SENCO/coefficient writes.
- No PostgreSQL connection or import.
- No pressure, gas-route, or water-route control.
- `packet_validated_offline` remains offline review evidence only and does not authorize live execution.
