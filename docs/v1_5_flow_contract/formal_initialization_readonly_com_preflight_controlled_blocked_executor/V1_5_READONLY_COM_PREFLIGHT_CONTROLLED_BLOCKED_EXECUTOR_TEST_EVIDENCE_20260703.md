# V1.5 Read-Only COM Preflight Controlled Blocked Executor Test Evidence

Generated for the V1.5 initialization read-only COM preflight controlled blocked executor review package.

## Scope

- Adds an offline blocked executor wrapper after the read-only COM preflight controlled executor design gate.
- Keeps the future read-only real COM path locked by default.
- Rejects live unlock, authorization, reviewed port inventory, and active analyzer payload inputs in this blocked layer.
- Wires the blocked executor sidecar into full-flow planning, formal-flow contract checks, formal-run-status gates, and entrypoint inventory.

## Safety Boundary

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
$env:PYTHONPATH=(Resolve-Path 'src').Path; & 'C:\Users\A\AppData\Local\Programs\Python\Python313\python.exe' -m pytest tests\test_v1_5_formal_initialization_readonly_com_preflight_controlled_blocked_executor.py tests\test_v1_5_full_flow_orchestration.py tests\test_v1_5_formal_flow_contract.py tests\test_v1_5_formal_run_status.py tests\test_v1_5_entrypoint_inventory.py -q
```

Result:

```text
119 passed, 2 warnings in 91.07s (0:01:31)
```

The two warnings are existing `PytestUnknownMarkWarning` notices for `v1_5_formal_gate`; they do not indicate a failure in this package.

## Protected Path Check

The package does not modify the mature V1.5 CO2/H2O queue, shared sampling, workflow runner, analyzer protocol, default config, or app entrypoint.

Checked protected paths:

- `src/gas_calibrator/tools/run_v1_5_formal_co2_open_flow_queue.py`
- `src/gas_calibrator/tools/run_v1_5_formal_h2o_open_flow_queue.py`
- `src/gas_calibrator/tools/run_v1_5_formal_open_flow_sampling.py`
- `src/gas_calibrator/workflow/runner.py`
- `src/gas_calibrator/devices/gas_analyzer.py`
- `configs/default_config.json`
- `run_app.py`
