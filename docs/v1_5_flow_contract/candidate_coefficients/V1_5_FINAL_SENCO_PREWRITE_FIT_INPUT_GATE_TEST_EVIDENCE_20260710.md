# V1.5 Final SENCO Prewrite Fit-Input Gate Test Evidence

## Scope

This package closes the final prewrite gap after PR #77. The final no-write
review for SENCO1/3, SENCO2/4, and S5/S6 now independently verifies that:

- `candidate_run_summary.csv` requires and passes fit-input quality;
- `candidate_policy_summary.csv` and `model_selection_summary.csv` retain an
  A-grade, usable fit-input decision for the same device/component;
- candidate summary, candidate policy, and model selection point to the same
  fit-input summary and per-device CSV files;
- both source files still exist and contain a passing continuity summary and
  one A-grade usable row for the reviewed device/component.

Historical candidate packages without these fields are blocked. Candidate and
model-selection packages rebound to different fit-input sources are also
blocked. When blocked, SENCO1/3 and SENCO2/4 command previews are blank, and
SENCO5/SENCO6 prerequisite commands are not emitted.

## Focused Verification

```text
python -m pytest tests\test_v1_5_candidate_coefficients.py tests\test_v1_5_candidate_model_selection_review.py tests\test_v1_5_main_senco_write_precheck_pack.py tests\test_v1_5_candidate_write_review.py tests\test_v1_5_post_run_coefficient_executor.py -q
71 passed, 1 warning in 35.26s
```

The warning is the existing unregistered `v1_5_formal_gate` pytest marker.

## Controlled Writer Compatibility

```text
python -m pytest tests\test_v1_5_co2_senco13_controlled_write.py tests\test_v1_5_h2o_senco24_controlled_write.py tests\test_v1_5_co2_senco5_linear_controlled_write.py tests\test_v1_5_h2o_senco6_linear_controlled_write.py -q
31 passed in 86.04s
```

## Safety Boundary

- opens COM ports: false
- writes SENCO coefficients: false
- controls gas or water routes: false
- connects PostgreSQL: false
- changes mature CO2/H2O sampling: false
- real acceptance evidence: false

The stable blocker is `fit_input_traceability_missing_or_invalid`. Per-device,
per-component details remain available in `candidate_write_review_checks.csv`,
the command table, the S5/S6 prerequisite table, and the precheck summary.
