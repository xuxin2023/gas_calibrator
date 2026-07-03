# V1.5 Algorithm Queue Handoff Preflight Test Evidence

- Date: 2026-07-01
- Scope: focused pytest evidence for the new-algorithm queue handoff preflight guard.
- Boundary: offline tests only; no COM, no PostgreSQL, no pressure control, no gas/water route control, no SN/device ID write, no SENCO write, no mature queue execution.

## Command

```powershell
python -m pytest tests\test_v1_5_algorithm_queue_handoff_preflight.py tests\test_v1_5_entrypoint_inventory.py -q
```

## Stdout

```text
..............................                                           [100%]
30 passed in 6.35s
```

## Coverage

- `test_v1_5_algorithm_queue_handoff_preflight.py`
  - Confirms the preflight consumes a ready profile runner dry-run bundle.
  - Confirms CO2/H2O queue handoff remains dry-run/no-prompt only.
  - Confirms `live_queue_execution_allowed=false`, `formal_release_allowed=false`, and `database_import_allowed=false`.
  - Confirms missing `--dry-run` blocks the handoff.
  - Confirms wrong profile point counts block the handoff.
  - Confirms writer and CLI export produce JSON, CSV, and Markdown evidence.
- `test_v1_5_entrypoint_inventory.py`
  - Confirms `export_v1_5_algorithm_queue_handoff_preflight.py` is classified as offline formal review evidence, not a formal runner.

## Compatibility Check

```powershell
python -m pytest tests\test_v1_5_algorithm_queue_handoff_preflight.py tests\test_v1_5_algorithm_profile_runner_dry_run.py tests\test_v1_5_algorithm_runner_integration_dry_run.py tests\test_v1_5_algorithm_runlist_readiness.py tests\test_v1_5_entrypoint_inventory.py -q
```

Stdout:

```text
........................................                                 [100%]
40 passed in 6.46s
```

## Result

The profile-generated 47/14 runlists now have an explicit program guard before any future queue handoff. Passing this guard allows only dry-run/no-prompt handoff review; it does not authorize live COM, route control, mature queue execution, coefficient writes, archive release, or database import.
