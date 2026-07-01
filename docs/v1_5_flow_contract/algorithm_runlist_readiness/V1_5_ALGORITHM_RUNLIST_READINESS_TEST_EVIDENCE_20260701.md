# V1.5 algorithm runlist readiness test evidence

Date: 2026-07-01

Command:

```powershell
python -m pytest tests\test_v1_5_algorithm_runlist_readiness.py tests\test_v1_5_algorithm_formal_runlist_preview.py tests\test_v1_5_algorithm_formal_point_plan_guard.py tests\test_v1_5_new_algorithm_test_point_plan.py tests\test_v1_5_algorithm_route_profiles.py tests\test_v1_5_mature_route_contract.py tests\test_v1_5_historical_replay_evidence.py tests\test_v1_5_entrypoint_inventory.py -q
```

Result:

```text
...........................................................              [100%]
59 passed in 8.52s
```

Scope:

- Confirms the new-algorithm runlist readiness gate passes with complete CO2 47 / H2O 14 preview CSVs.
- Confirms missing `-20C/600ppm` blocks the CO2 runlist and supplemental semantics gates.
- Confirms missing `40C/HGEN30C/30RH` blocks the H2O runlist and supplemental semantics gates.
- Confirms the readiness exporter remains offline review evidence and does not open COM ports, control routes, connect PostgreSQL, or write coefficients.
