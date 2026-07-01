# V1.5 algorithm runner integration dry-run test evidence

Date: 2026-07-01

Command:

```powershell
python -m pytest tests\test_v1_5_algorithm_runner_integration_dry_run.py tests\test_v1_5_algorithm_runlist_readiness.py tests\test_v1_5_algorithm_formal_runlist_preview.py tests\test_v1_5_algorithm_formal_point_plan_guard.py tests\test_v1_5_new_algorithm_test_point_plan.py tests\test_v1_5_algorithm_route_profiles.py tests\test_v1_5_mature_route_contract.py tests\test_v1_5_historical_replay_evidence.py tests\test_v1_5_entrypoint_inventory.py -q
```

Result:

```text
..............................................................           [100%]
62 passed in 9.10s
```

Scope:

- Confirms the runner integration dry-run plan is generated only after runlist readiness is clean.
- Confirms blocked runlist readiness blocks runner integration dry-run planning.
- Confirms the planned CO2/H2O queue commands include `--dry-run --no-prompt`.
- Confirms the sidecar does not execute commands, open COM ports, control routes, connect PostgreSQL, modify formal runners, or write coefficients.
