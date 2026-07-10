# V1.5 Pressure/SENCO9 Readiness Index Test Evidence

- Date: 2026-07-10
- Branch: `codex/v1.5-pressure-s9-readiness-index`
- Scope: offline pressure/SENCO9 readiness evidence index
- Boundary: no COM, no pressure control, no gas/water route control, no PostgreSQL, no SN/device ID write, no SENCO9 write

## Focused Command

```powershell
& 'C:\Users\A\AppData\Local\Programs\Python\Python313\python.exe' -m pytest tests\test_v1_5_pressure_s9_readiness_index.py -q
```

Result:

```text
4 passed in 1.89s
```

## Compatibility Command

```powershell
& 'C:\Users\A\AppData\Local\Programs\Python\Python313\python.exe' -m pytest tests\test_v1_5_pressure_s9_readiness_index.py tests\test_v1_5_batch_initialization_closeout_index.py tests\test_v1_5_entrypoint_inventory.py tests\test_v1_5_full_flow_next_action_plan.py -q
```

Result:

```text
40 passed, 1 warning in 8.61s
```

The warning is the existing `pytest.mark.v1_5_formal_gate` registration warning from the entrypoint inventory suite.

## Review Notes

- Default S9 model remains `offset_only`.
- Linear S9 is accepted only as an explicit controlled exception with no-write fit basis, write/readback evidence, and pressure-only reverify evidence.
- Missing per-device readback or pressure reverify keeps the index in `review_required`.
- The generated default artifact is conservative: without input evidence it remains `review_required` and cannot unlock mature open-flow routes.
