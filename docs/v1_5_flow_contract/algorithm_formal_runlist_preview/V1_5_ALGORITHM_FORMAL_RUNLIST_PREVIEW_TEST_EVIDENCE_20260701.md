# V1.5 algorithm formal runlist preview test evidence

Date: 2026-07-01

Command:

```powershell
python -m pytest tests\test_v1_5_algorithm_formal_runlist_preview.py tests\test_v1_5_algorithm_formal_point_plan_guard.py tests\test_v1_5_new_algorithm_test_point_plan.py tests\test_v1_5_algorithm_route_profiles.py tests\test_v1_5_mature_route_contract.py tests\test_v1_5_historical_replay_evidence.py tests\test_v1_5_entrypoint_inventory.py -q
```

Result:

```text
.......................................................                  [100%]
55 passed in 11.68s
```

Scope:

- Confirms legacy route coverage remains CO2 45 points / H2O 13 wet points.
- Confirms new-algorithm formal runlist preview is CO2 47 points / H2O 14 wet points.
- Confirms `-20C/600ppm`, `-10C/600ppm`, and `40C/HGEN30C/30RH` are scheduled inside their temperature segments.
- Confirms the generated CSVs can be loaded by the mature V1.5 CO2/H2O queue loaders.
- Confirms the exporter remains offline review evidence and does not open COM ports, control routes, connect PostgreSQL, or write coefficients.
