# V1.5 formal initialization blocked executor test evidence

Date: 2026-07-03

Scope: verify the new V1.5 formal initialization blocked executor contract before any live initialization executor work.

Command:

```powershell
python -m pytest tests\test_v1_5_formal_initialization_blocked_executor.py tests\test_v1_5_formal_initialization_executor_dry_run.py tests\test_v1_5_full_flow_orchestration.py tests\test_v1_5_formal_flow_contract.py tests\test_v1_5_entrypoint_inventory.py -q
```

Result:

```text
86 passed, 2 warnings in 56.06s
```

Boundary confirmed:

- `execution_supported=false`
- `live_execution_allowed=false`
- `read_only_real_com_execution_allowed=false`
- `controlled_write_execution_allowed=false`
- `opens_com_ports=false`
- `connects_postgresql=false`
- `writes_sn=false`
- `writes_device_id=false`
- `writes_coefficients=false`
- `database_written=false`
- not real acceptance evidence

The two warnings are existing unregistered `pytest.mark.v1_5_formal_gate` warnings in the focused test set.
