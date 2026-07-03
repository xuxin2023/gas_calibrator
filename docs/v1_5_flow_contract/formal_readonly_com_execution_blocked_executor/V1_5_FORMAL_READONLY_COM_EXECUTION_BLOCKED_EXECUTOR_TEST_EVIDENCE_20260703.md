# V1.5 Formal Read-Only COM Execution Blocked Executor Test Evidence

This package adds a blocked/no-COM wrapper after the read-only COM execution
packet contract. It is not a live executor and it does not open analyzer COM.

## Boundary

- `execution_supported=false`
- `live_execution_allowed=false`
- `read_only_real_com_execution_allowed=false`
- `opens_com_ports=false`
- `writes_sn=false`
- `writes_device_id=false`
- `writes_coefficients=false`
- `connects_postgresql=false`
- `controls_pressure=false`
- `controls_water_or_gas_routes=false`
- `formal_release_allowed=false`
- `database_import_allowed=false`
- `not_real_acceptance_evidence=true`

The blocked wrapper rejects `--execute-read-only-real-com`, authorization packet
fields, reviewed port inventory, active analyzer list, generic execute flags,
controlled-write flags, PostgreSQL inputs, pressure, and route actions.

## Focused pytest

```text
tests\test_v1_5_formal_readonly_com_execution_blocked_executor.py
tests\test_v1_5_full_flow_orchestration.py
tests\test_v1_5_formal_flow_contract.py
tests\test_v1_5_formal_run_status.py
tests\test_v1_5_entrypoint_inventory.py
```

Result:

```text
124 passed, 2 warnings in 60.88s (0:01:00)
```

The two warnings are the existing `PytestUnknownMarkWarning` entries for
`v1_5_formal_gate`.
