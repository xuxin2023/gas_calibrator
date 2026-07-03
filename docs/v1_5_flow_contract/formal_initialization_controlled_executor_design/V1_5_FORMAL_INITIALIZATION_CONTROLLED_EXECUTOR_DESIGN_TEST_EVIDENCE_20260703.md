# V1.5 Formal Initialization Controlled Executor Design Test Evidence

- Date: 2026-07-03
- Scope: initialization controlled executor design review, blocked executor linkage, full-flow wiring, formal-flow guard, formal run status rollup, and entrypoint inventory.
- Execution boundary: offline only; no COM, no SN/device_code write, no SENCO write, no PostgreSQL connection, no pressure control, no gas/water route control.

Command:

```powershell
python -m pytest tests\test_v1_5_formal_initialization_controlled_executor_design.py tests\test_v1_5_formal_initialization_blocked_executor.py tests\test_v1_5_full_flow_orchestration.py tests\test_v1_5_formal_flow_contract.py tests\test_v1_5_formal_run_status.py tests\test_v1_5_entrypoint_inventory.py -q
```

Result:

```text
110 passed, 2 warnings in 54.80s
```

Warnings:

- `PytestUnknownMarkWarning` for the existing `v1_5_formal_gate` mark in formal-flow and entrypoint inventory tests.

Confirmed locks:

- `execution_supported=false`
- `live_execution_allowed=false`
- `read_only_real_com_execution_allowed=false`
- `controlled_write_execution_allowed=false`
- `opens_com_ports=false`
- `writes_sn=false`
- `writes_device_id=false`
- `writes_coefficients=false`
- `connects_postgresql=false`
- `database_written=false`
- `not_real_acceptance_evidence=true`
