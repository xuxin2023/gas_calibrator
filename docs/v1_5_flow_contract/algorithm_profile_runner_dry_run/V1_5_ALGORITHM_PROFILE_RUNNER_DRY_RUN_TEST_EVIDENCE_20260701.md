# V1.5 algorithm profile runner dry-run test evidence

- Date: 2026-07-01
- Scope: offline profile-driven bundle for new-algorithm runlist preview, runlist readiness, and runner integration dry-run evidence.
- Boundary: no COM, no PostgreSQL, no route control, no coefficient writes, no device ID writes, no mature runner modification, no real acceptance.

## Command

```powershell
python -m pytest tests\test_v1_5_algorithm_profile_runner_dry_run.py tests\test_v1_5_algorithm_runner_integration_dry_run.py tests\test_v1_5_algorithm_runlist_readiness.py tests\test_v1_5_algorithm_formal_runlist_preview.py tests\test_v1_5_algorithm_formal_point_plan_guard.py tests\test_v1_5_new_algorithm_test_point_plan.py tests\test_v1_5_algorithm_route_profiles.py tests\test_v1_5_mature_route_contract.py tests\test_v1_5_historical_replay_evidence.py tests\test_v1_5_entrypoint_inventory.py -q
```

## Result

```text
.................................................................        [100%]
65 passed in 8.64s
```
