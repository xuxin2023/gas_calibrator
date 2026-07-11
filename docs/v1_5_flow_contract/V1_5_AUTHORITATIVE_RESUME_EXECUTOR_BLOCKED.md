# V1.5 Authoritative Resume Executor Blocked Contract

This package is the fail-closed boundary after the plan-only resume preview. It does not implement resume execution.

## Inputs

- Exact `v1_5_resume_executor_plan_preview.json` from the offline #98 preview.
- The preview must remain bound to its consumer contract and independently recompute without differences.

## Fixed Locks

- `execution_supported=false`
- `resume_execution_allowed=false`
- `would_execute=false`
- `opens_com_ports=false`
- `controls_pressure=false`
- `controls_water_or_gas_routes=false`
- `writes_coefficients=false`
- `connects_postgresql=false`
- `formal_release_allowed=false`
- `database_import_allowed=false`

The CLI does not accept execute, resume, real-COM, pressure, route, write, or database-import unlock flags. Argument rejection occurs before an output directory or evidence artifact is created.

## Mature Path Boundary

This package is offline review evidence only. It does not modify or call the 0613 fitting path, the 0620/0621 CO2/H2O queues, shared sampling, the workflow runner, analyzer protocol code, default configuration, or `run_app.py`.

## Meaning

`blocked_executor_ready=true` means the lock proof is internally consistent. It does not mean the next physical step is authorized or executable.
