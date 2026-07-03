# V1.5 Formal Read-only COM Execution Packet Validator Test Evidence

Date: 2026-07-03

Scope:
- Offline read-only COM execution packet validator
- Full-flow planner integration
- Formal-flow contract guard
- Formal-run-status gate
- Entrypoint inventory classification

Safety boundary:
- No COM ports opened
- No analyzer reads
- No SN/device_code writes
- No SENCO/coefficient writes
- No PostgreSQL connection or import
- No pressure, gas-route, or water-route control
- Not real acceptance evidence

Command:

```powershell
python -m pytest tests\test_v1_5_formal_readonly_com_execution_packet_validator.py tests\test_v1_5_full_flow_orchestration.py tests\test_v1_5_formal_flow_contract.py tests\test_v1_5_formal_run_status.py tests\test_v1_5_entrypoint_inventory.py -q
```

Result:

```text
129 passed, 2 warnings in 87.08s (0:01:27)
```

Warnings:
- Existing unregistered `pytest.mark.v1_5_formal_gate` warnings in formal-flow and entrypoint inventory tests.
