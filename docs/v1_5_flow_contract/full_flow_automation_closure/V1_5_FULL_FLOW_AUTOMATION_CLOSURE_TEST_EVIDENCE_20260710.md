# V1.5 Full-Flow Automation Closure Test Evidence

- date: 2026-07-10
- package: `full_flow_automation_closure`
- branch: `codex/v1.5-full-flow-automation-closure`
- scope: offline automation closure map only

## Commands

```powershell
python -m pytest tests\test_v1_5_full_flow_automation_closure.py -q
```

Result:

```text
4 passed in 2.18s
```

```powershell
python -m pytest tests\test_v1_5_full_flow_automation_closure.py tests\test_v1_5_automation_control_contract.py tests\test_v1_5_formal_flow_contract.py tests\test_v1_5_formal_run_status.py tests\test_v1_5_entrypoint_inventory.py tests\test_v1_5_production_entrypoint_gate.py -q
```

Result:

```text
114 passed, 2 warnings in 95.02s
```

The two warnings are existing `PytestUnknownMarkWarning` notices for `v1_5_formal_gate`.

## Boundary

- opens_com_ports: `false`
- controls_water_or_gas_routes: `false`
- connects_postgresql: `false`
- writes_coefficients: `false`
- formal_release_allowed: `false`
- database_import_allowed: `false`
- not_real_acceptance_evidence: `true`

This evidence does not collect live data, does not validate a real calibration run, and does not release archive or database import.
