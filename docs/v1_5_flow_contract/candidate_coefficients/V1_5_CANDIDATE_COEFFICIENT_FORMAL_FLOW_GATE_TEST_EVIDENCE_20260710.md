# V1.5 Candidate Coefficient Formal-Flow Gate Test Evidence

Date: 2026-07-10

## Scope

This package closes the formal orchestration gap after the candidate-coefficient
fit-input gate was added:

- `fit_input_quality_review` now declares its summary and per-device CSV outputs.
- `post_run_coefficient_executor` receives those exact CSV paths from the
  preceding full-flow step.
- CO2 and H2O candidate commands require both artifacts and
  `--require-fit-input-quality`.
- Fit-input quality review precedes both component candidate fits.
- The formal-flow contract blocks a generated plan if either binding is absent
  or points outside the canonical `fit_input_quality` output directory.

This is an offline orchestration and evidence contract only. It does not open
COM ports, control pressure or gas/water routes, write coefficients, connect
PostgreSQL, or change the 0613/0620/0621 mature route and fitting implementations.

## Focused validation

```text
python -m pytest tests\test_v1_5_full_flow_orchestration.py tests\test_v1_5_formal_flow_contract.py tests\test_v1_5_post_run_coefficient_executor.py -q

91 passed, 2 warnings in 256.79s
```

The warnings are the existing unregistered `pytest.mark.v1_5_formal_gate`
warnings in the formal-flow and post-run executor test modules.

## Compatibility validation

```text
python -m pytest tests\test_v1_5_fit_input_quality.py tests\test_v1_5_candidate_coefficients.py tests\test_v1_5_post_run_coefficient_executor.py -q

58 passed, 1 warning in 38.84s
```

The warning is the same existing `pytest.mark.v1_5_formal_gate` warning.

## Result

The formal V1.5 plan can no longer present an unguarded candidate-fit command:
route-continuity-aware fit-input evidence is generated first, bound by exact
path into the post-run executor, and required again by every CO2/H2O candidate
coefficient command.
