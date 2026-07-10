# V1.5 Model Selection Fit-Input Gate Test Evidence

Date: 2026-07-10

## Scope

This package prevents the V1.5 model-selection and S5/S6 no-write review from
re-fitting legacy or replacement residual packages outside the formal
fit-input quality chain.

When the formal gate is required, model selection now checks both:

- the supplied fit-input summary and per-device CSV are continuity-aware,
  offline/no-write, and pass for the current component/device; and
- the candidate package records that the same two artifacts were required,
  passed, and bound when its residuals were generated.

An ineligible device receives one blocked audit row and produces no model,
residual, or S5/S6 trim candidate. Other A-grade devices continue independently.
The post-run executor now emits the model-selection command with both artifact
paths and `--require-fit-input-quality`.

This is an offline consumer gate. It does not change any fitting equation,
S5/S6 trim method, GETCO composition rule, COM behavior, route control,
coefficient writer, PostgreSQL path, or 0613/0620/0621 mature route code.

## Focused validation

```text
python -m pytest tests\test_v1_5_candidate_model_selection_review.py -q

4 passed in 9.26s
```

The focused cases cover a fully passing package, a legacy package without the
gate, a mixed A/REJECT device batch, and replacement artifacts whose paths do
not match the candidate package provenance.

## Formal-chain compatibility

```text
python -m pytest tests\test_v1_5_fit_input_quality.py tests\test_v1_5_candidate_coefficients.py tests\test_v1_5_candidate_model_selection_review.py tests\test_v1_5_post_run_coefficient_executor.py -q

62 passed, 1 warning in 42.92s
```

The warning is the existing unregistered `pytest.mark.v1_5_formal_gate`
warning in the post-run executor test module.

## S5/S6 mathematical compatibility

```text
python -m pytest tests\test_v1_5_co2_senco5_linear_trim_review.py tests\test_v1_5_h2o_senco6_linear_trim_review.py -q

14 passed in 9.34s
```

## Result

The formal V1.5 chain now keeps fit-input continuity and device-grade evidence
attached through candidate fitting and model/S5/S6 selection. The package
remains no-write; later controlled writes still require current GETCO state,
explicit approval, readback, and post-write reverification.
