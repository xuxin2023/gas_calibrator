# V1.5 Fit Input Continuity Consumer Test Evidence - 2026-07-10

## Scope

This package makes the offline fit-input quality audit consume the mature-route continuity evidence before any CO2/H2O candidate input can be graded as fit eligible.

The audit remains offline only:

- `opens_com_ports=false`
- `controls_water_or_gas_routes=false`
- `writes_coefficients=false`
- no PostgreSQL connection
- no mature CO2/H2O queue modification

## Contract Added

The fit-input quality audit must see one of these ready evidences before returning `run_status=pass`:

- `formal_run_status_json` containing `mature_route_continuity_gate` with `status=ready`
- `mature_route_continuity_gate_json` with `status=pass`, `continuous_route_run_fit_eligible=true`, `blocker_count=0`, and `review_required_count=0`

If the continuity evidence is missing or blocked, every target device/component fit input is downgraded to:

- `fit_input_grade=REJECT`
- `fit_input_status=excluded_from_candidate_fit`
- reject reason includes `fit_input_continuity_gate_not_ready`

This prevents segmented, migration, retry, direct-recovery, or otherwise non-continuous route evidence from feeding coefficient fitting merely because residual CSV rows exist.

## Focused Test

Command:

```text
python -m pytest tests\test_v1_5_fit_input_quality.py -q
```

Result:

```text
6 passed in 3.94s
```

## Compatibility Test

Command:

```text
python -m pytest tests\test_v1_5_fit_input_quality.py tests\test_v1_5_formal_run_status.py tests\test_v1_5_mature_route_continuity_gate.py tests\test_v1_5_post_run_coefficient_executor.py -q
```

Result:

```text
86 passed, 1 warning in 52.33s
```

The warning is the existing `pytest.mark.v1_5_formal_gate` registration warning from `tests\test_v1_5_post_run_coefficient_executor.py`.
