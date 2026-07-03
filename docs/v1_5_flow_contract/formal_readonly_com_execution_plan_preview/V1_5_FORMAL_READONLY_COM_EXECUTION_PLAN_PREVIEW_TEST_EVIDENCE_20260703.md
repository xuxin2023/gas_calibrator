# V1.5 formal read-only COM execution plan preview test evidence

Generated on 2026-07-03 for the V1.5 read-only COM execution plan-preview package.

## Scope

- Adds an offline plan-preview sidecar after the read-only COM execution packet validator.
- Converts a validated future packet plus reviewed ports and active analyzers into a future read-order preview.
- Read order covers MODE2 protocol ID evidence, `SN,YGAS,FFF`, `GETCO1..9,YGAS,FFF`, runtime 1Hz/filter evidence, and `CHECK,YGAS,FFF` only for CHECK-capable/new-algorithm analyzers.
- Old-algorithm analyzers are explicitly CHECK-skipped.
- The full-flow planner calls the preview without detailed packet inputs, so it remains locked/review-only by default.
- The preview re-checks that the active analyzer list has protocol device IDs, 8-digit numeric SN/device_code values, no duplicate SN values, and the same reviewed-port and active-list paths validated by the packet validator.
- Old-algorithm analyzers are rejected if they are marked `check_required=true` or `check_capable=true`; CHECK planning is driven by the algorithm being new/CHECK-capable in the supported hardware sense, not by an erroneous legacy input flag.

## Safety boundaries

- `opens_com_ports=false`
- `read_only_real_com_execution_allowed=false`
- `writes_sn=false`
- `writes_device_id=false`
- `writes_coefficients=false`
- `connects_postgresql=false`
- `controls_pressure=false`
- `controls_water_or_gas_routes=false`
- `does_not_execute_commands=true`
- `not_real_acceptance_evidence=true`

## Focused pytest

```text
python -m pytest tests\test_v1_5_formal_readonly_com_execution_packet_validator.py tests\test_v1_5_formal_readonly_com_execution_plan_preview.py tests\test_v1_5_full_flow_orchestration.py tests\test_v1_5_formal_flow_contract.py tests\test_v1_5_formal_run_status.py tests\test_v1_5_entrypoint_inventory.py -q
147 passed, 2 warnings in 68.31s
```

The two warnings are existing unregistered `v1_5_formal_gate` pytest markers.
