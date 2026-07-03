# V1.5 Formal Read-Only COM Execution Contract Test Evidence

Generated for the V1.5 read-only COM execution packet contract review package.

## Scope

- Adds an offline contract sidecar after the controlled read-only COM blocked executor.
- Defines the future real read-only COM execution packet fields without accepting them as unlocks.
- Requires the future packet to cover explicit `--execute-read-only-real-com`, authorization id, operator confirmation, reviewer, approver, reviewed port inventory, active analyzer list, 1s serial pacing, and old-algorithm CHECK skip behavior.
- Keeps write, database, pressure, route, and generic real-COM unlocks denied.

## Safety Boundary

- `execution_supported=false`
- `opens_com_ports=false`
- `read_only_real_com_execution_allowed=false`
- `controlled_write_execution_allowed=false`
- `writes_sn=false`
- `writes_device_id=false`
- `writes_coefficients=false`
- `connects_postgresql=false`
- `controls_pressure=false`
- `controls_water_or_gas_routes=false`
- `formal_release_allowed=false`
- `database_import_allowed=false`
- `not_real_acceptance_evidence=true`

## Focused Pytest

Command:

```powershell
$env:PYTHONPATH=(Resolve-Path 'src').Path; & 'C:\Users\A\AppData\Local\Programs\Python\Python313\python.exe' -m pytest tests\test_v1_5_formal_readonly_com_execution_contract.py tests\test_v1_5_full_flow_orchestration.py tests\test_v1_5_formal_flow_contract.py tests\test_v1_5_formal_run_status.py tests\test_v1_5_entrypoint_inventory.py -q
```

Result:

```text
121 passed, 2 warnings in 84.91s (0:01:24)
```

The two warnings are existing `PytestUnknownMarkWarning` notices for `v1_5_formal_gate`; they are not failures in this package.

## Protected Path Check

This package must not modify mature V1.5 route runners, shared sampling, analyzer protocol, default config, or the app entrypoint.

Checked protected paths:

- `src/gas_calibrator/tools/run_v1_5_formal_co2_open_flow_queue.py`
- `src/gas_calibrator/tools/run_v1_5_formal_h2o_open_flow_queue.py`
- `src/gas_calibrator/tools/run_v1_5_formal_open_flow_sampling.py`
- `src/gas_calibrator/workflow/runner.py`
- `src/gas_calibrator/devices/gas_analyzer.py`
- `configs/default_config.json`
- `run_app.py`
