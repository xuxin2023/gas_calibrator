# V1.5 Formal Run Continuity Gate Test Evidence

Date: 2026-07-09

Command:

```powershell
& 'C:\Users\A\AppData\Local\Programs\Python\Python313\python.exe' -m pytest tests\test_v1_5_formal_run_continuity_gate.py tests\test_v1_5_production_entrypoint_gate.py tests\test_v1_5_mature_route_contract.py -q
```

Result:

```text
19 passed in 5.07s
```

Coverage intent:

- Continuous single-segment CO2 run passes.
- Segmented CO2 run without accepted manifest is blocked.
- Complete segmented run with accepted manifest is review-required, not pass.
- Parameter/config hash changes without review are blocked.
- Per-point workers, `_handoff`, 0624 migration, and root migration references are blocked.
- H2O remaining-point segment pattern requires accepted manifest coverage.
- Exporter is classified as offline formal review evidence.

The test run does not open COM ports, control pressure, control gas/water routes,
connect PostgreSQL, write SN/device_code, or write coefficients.
