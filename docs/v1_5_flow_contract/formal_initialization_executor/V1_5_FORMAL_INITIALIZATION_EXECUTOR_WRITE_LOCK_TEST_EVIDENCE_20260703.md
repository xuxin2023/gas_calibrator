# V1.5 Formal Initialization Executor Write Lock Test Evidence - 2026-07-03

## Scope

This evidence covers the V1.5 formal initialization executor lock that requires
controlled coefficient-write steps to also have read-only real-COM execution
unlocked.

The initialization executor can still generate the formal plan, PostgreSQL 18
sidecar, readiness contract, and offline evidence without touching devices. Real
device interaction remains explicit: coefficient writes cannot run with only
`--execute-controlled-writes`; they also require the real-COM unlock because the
writer commands operate through analyzer serial ports.

## Validation Command

```powershell
python -m pytest tests\test_v1_5_formal_initialization_runner.py tests\test_v1_5_initialization_readiness.py tests\test_v1_5_pre_gas_readiness.py tests\test_v1_5_formal_flow_contract.py tests\test_v1_5_entrypoint_inventory.py -q
```

## Result

```text
84 passed, 2 warnings in 106.56s (0:01:46)
```

The warnings are existing unregistered `v1_5_formal_gate` pytest marker warnings.

## Boundary

- No COM ports were opened.
- No SN/device_code values were written.
- No SENCO coefficients were written.
- No PostgreSQL connection or import was performed.
- Mature CO2/H2O queue files were not changed.
- Shared formal sampling was not changed.
- `workflow/runner.py`, `devices/gas_analyzer.py`, `configs/default_config.json`,
  and `run_app.py` were not changed.
