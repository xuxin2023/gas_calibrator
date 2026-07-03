# V1.5 Formal Initialization Executor Dry-Run Test Evidence - 2026-07-03

## Scope

This evidence covers the offline V1.5 formal initialization executor dry-run
review sidecar. The sidecar consumes `v1_5_formal_initialization_plan.json` and
classifies planned steps into offline dry-run commands, read-only real-COM
locked steps, controlled-write locked steps, and contract-only gates.

It does not execute the initialization plan and does not unlock live
initialization automation.

## Validation Command

```powershell
python -m pytest tests\test_v1_5_formal_initialization_executor_dry_run.py tests\test_v1_5_formal_initialization_runner.py tests\test_v1_5_full_flow_orchestration.py tests\test_v1_5_entrypoint_inventory.py -q
```

## Result

```text
75 passed, 1 warning in 30.07s
```

The warning is the existing unregistered `v1_5_formal_gate` pytest marker
warning.

## Boundary

- No COM ports were opened.
- No SN/device_code values were written.
- No SENCO coefficients were written.
- No PostgreSQL connection or import was performed.
- No gas, water, or pressure route control was performed.
- Mature CO2/H2O queue files were not changed.
- Shared formal sampling was not changed.
- `workflow/runner.py`, `devices/gas_analyzer.py`, `configs/default_config.json`,
  and `run_app.py` were not changed.
