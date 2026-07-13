# V1.5 final production external-gate freeze test evidence

Date: 2026-07-14

## Scope

This evidence validates the offline status package that separates completed
V1.5 program capabilities from remaining real-production external gates.

- No COM port or analyzer was opened.
- No SN/device ID or SENCO coefficient was read or written.
- No pressure, CO2, H2O, chamber, or humidity route was controlled.
- No PostgreSQL connection, migration, production import, archive release, or
  formal release was executed.
- The 0613 fitting and 0620/0621 mature-route baselines were not modified.

## Focused and compatibility regression

```text
python -m pytest tests\test_v1_5_final_production_external_gate_freeze.py tests\test_v1_5_final_production_gap_freeze.py tests\test_v1_5_legacy_full_flow_offline_replay.py tests\test_v1_5_final_offline_acceptance_suite.py tests\test_v1_5_entrypoint_inventory.py -q

61 passed, 1 warning in 8.10s
```

The warning is the existing unregistered `v1_5_formal_gate` pytest mark.

## Export resilience and parity

```text
python -m pytest tests\v2\test_export_resilience.py tests\v2\test_summary_parity.py -q

6 passed in 24.54s
```

## Static validation

```text
python -m ruff check src\gas_calibrator\validation\v1_5_final_production_external_gate_freeze.py src\gas_calibrator\tools\export_v1_5_final_production_external_gate_freeze.py src\gas_calibrator\validation\v1_5_entrypoint_inventory.py tests\test_v1_5_final_production_external_gate_freeze.py

All checks passed!
```

## Result

The package reports seven implemented program capabilities and six remaining
real-production external gates. It does not authorize hardware, writes,
production migration, production import, or formal release.
