# V1.5 Mature Route Contract Test Evidence

- Date: 2026-07-01
- Scope: focused pytest evidence for the V1.5 mature-route program guard.
- Boundary: offline tests only; no COM, no PostgreSQL, no pressure control, no gas/water route control, no SN write, no SENCO write.

## Command

```powershell
& 'C:\Users\A\AppData\Local\Programs\Python\Python313\python.exe' -m pytest tests\test_v1_5_mature_route_contract.py tests\test_v1_5_algorithm_route_profiles.py tests\test_v1_5_entrypoint_inventory.py -q
```

## Stdout

```text
......................................                                   [100%]
38 passed in 7.21s
```

## Coverage

- `test_v1_5_mature_route_contract.py`
  - Freezes the 0620 mature route behavior.
  - Guards legacy CO2 45-point and H2O 13-wet-point contracts.
  - Blocks absorption-profile runner forks.
  - Blocks premature SENCOA/SENCOB R0 writer promotion.
  - Verifies the offline exporter and generated contract artifacts.
- `test_v1_5_algorithm_route_profiles.py`
  - Confirms legacy ratio remains production default.
  - Confirms new algorithm differences stay in `A=-ln(R/R0(T))/(P_kPa/100)`, R0, supplement, and write-contract layers.
  - Confirms new algorithm supplement points do not modify the legacy default queue.
- `test_v1_5_entrypoint_inventory.py`
  - Confirms mature route guard is classified as offline formal review evidence.
  - Confirms canonical queue runners remain top-level and sampling workers stay subordinate.
  - Confirms the final structure document records mature-route guard boundaries.

## Result

The mature-route program guard is covered by current focused pytest stdout. This evidence does not replace real production execution; it prevents accidental program-level drift before future replay, database dry-run, or real production runs.
