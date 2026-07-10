# V1.5 Fit-Input Artifact SHA256 Binding Evidence

Date: 2026-07-11

## Scope

- Generate one authoritative SHA256 manifest in the final SENCO prewrite package.
- Bind component-specific fit-input quality tables, candidate summaries/policies, model-selection summaries, and writer-consumed precheck CSV files.
- Bind the actual `--candidate-coefficients-csv` consumed by the SENCO5 and SENCO6 controlled writers.
- Recompute hashes at each controlled writer invocation before constructing `GasAnalyzer`.
- Reject missing roles, duplicate roles, path rebinding, size mismatch, digest mismatch, and unsafe manifest boundaries.
- Preserve per-device/component isolation: a CO2 writer does not validate unused H2O artifacts, and vice versa.

## Verification

```text
python -m pytest tests\test_v1_5_artifact_hash_binding.py tests\test_v1_5_main_senco_write_precheck_pack.py tests\test_v1_5_final_senco_prewrite_gate.py tests\test_v1_5_co2_senco13_controlled_write.py tests\test_v1_5_h2o_senco24_controlled_write.py tests\test_v1_5_co2_senco5_linear_controlled_write.py tests\test_v1_5_h2o_senco6_linear_controlled_write.py -q
55 passed in 85.03s
```

```text
python -m pytest tests\test_v1_5_candidate_coefficients.py tests\test_v1_5_candidate_model_selection_review.py tests\test_v1_5_main_senco_write_precheck_pack.py tests\test_v1_5_candidate_write_review.py tests\test_v1_5_post_run_coefficient_executor.py tests\test_v1_5_artifact_hash_binding.py tests\test_v1_5_final_senco_prewrite_gate.py -q
83 passed, 1 existing marker warning in 17.65s
```

The focused writer tests replace the reviewed SENCO5 and SENCO6 candidate files at the same paths after manifest generation. Both writers return locked status before constructing `GasAnalyzer`.

## Safety Boundary

- No COM ports were opened.
- No SENCO coefficients were written.
- No route, valve, PACE, temperature chamber, or humidity generator was controlled.
- No PostgreSQL connection or import was performed.
- Mature V1.5 CO2/H2O route, sampling, protocol, runner, and default configuration files were not changed.

## Residual Risk

- P0: none.
- P1: none.
- P2: SHA256 provides content integrity, not signer authenticity. A future signed-manifest layer may add operator/reviewer provenance without changing this hash contract.
