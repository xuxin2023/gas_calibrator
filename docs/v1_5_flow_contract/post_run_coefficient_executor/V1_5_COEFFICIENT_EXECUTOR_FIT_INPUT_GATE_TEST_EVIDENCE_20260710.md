# V1.5 Coefficient Executor Fit-Input Gate Test Evidence - 2026-07-10

## Scope

This package gates the offline post-run coefficient executor on the V1.5 fit-input quality audit.

The executor remains offline only:

- `opens_com_ports=false`
- `controls_water_or_gas_routes=false`
- `controls_valves_or_pace=false`
- `writes_coefficients=false`
- no PostgreSQL connection
- no mature CO2/H2O runner changes

## Contract Added

The post-run coefficient executor now requires `fit_input_quality_review` before controlled write review can be considered ready.

Accepted evidence:

- `v1_5_fit_input_quality_summary.csv` with `run_status=pass`
- `fit_input_continuity_gate_status=pass`
- no COM, route-control, or coefficient-write flags

Per-device evidence:

- `v1_5_fit_input_quality_devices.csv`
- each device/component that will enter controlled write review must have `fit_input_grade=A`

Missing or rejected fit-input quality evidence blocks the device from the controlled write package. This prevents segmented, migration, retry, direct-recovery, or otherwise non-continuous route evidence from reaching SENCO write review merely because candidate coefficient rows exist.

## Focused Test

Command:

```text
python -m pytest tests\test_v1_5_post_run_coefficient_executor.py -q
```

Result:

```text
26 passed, 1 warning in 21.52s
```

## Compatibility Test

Command:

```text
python -m pytest tests\test_v1_5_post_run_coefficient_executor.py tests\test_v1_5_fit_input_quality.py tests\test_v1_5_formal_run_status.py tests\test_v1_5_mature_route_continuity_gate.py -q
```

Result:

```text
88 passed, 1 warning in 30.75s
```

The warning is the existing `pytest.mark.v1_5_formal_gate` registration warning from `tests\test_v1_5_post_run_coefficient_executor.py`.
