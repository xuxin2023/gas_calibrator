# V1.5 formal initialization read-only COM preflight design test evidence

Generated: 2026-07-03

## Scope

This evidence covers the offline V1.5 initialization read-only real-COM preflight design package.

The package remains design-only:

- `opens_com_ports=false`
- `read_only_real_com_execution_allowed=false`
- `live_execution_allowed=false`
- `writes_sn=false`
- `writes_device_id=false`
- `writes_coefficients=false`
- `connects_postgresql=false`
- `database_written=false`
- `not_real_acceptance_evidence=true`

## Command

```powershell
& 'C:\Users\A\AppData\Local\Programs\Python\Python313\python.exe' -m pytest tests\test_v1_5_formal_initialization_readonly_com_preflight_design.py tests\test_v1_5_formal_initialization_controlled_executor_design.py tests\test_v1_5_full_flow_orchestration.py tests\test_v1_5_formal_flow_contract.py tests\test_v1_5_formal_run_status.py tests\test_v1_5_entrypoint_inventory.py -q
```

## Result

```text
........................................................................ [ 63%]
.........................................                                [100%]
============================== warnings summary ===============================
tests\test_v1_5_formal_flow_contract.py:15
  D:\gas_calibrator\_worktrees\v1_5_initialization_readonly_com_preflight_design_20260703\tests\test_v1_5_formal_flow_contract.py:15: PytestUnknownMarkWarning: Unknown pytest.mark.v1_5_formal_gate - this mark existed before this package.
    pytestmark = pytest.mark.v1_5_formal_gate

tests\test_v1_5_entrypoint_inventory.py:20
  D:\gas_calibrator\_worktrees\v1_5_initialization_readonly_com_preflight_design_20260703\tests\test_v1_5_entrypoint_inventory.py:20: PytestUnknownMarkWarning: Unknown pytest.mark.v1_5_formal_gate - this mark existed before this package.
    pytestmark = pytest.mark.v1_5_formal_gate

113 passed, 2 warnings in 98.71s (0:01:38)
```
