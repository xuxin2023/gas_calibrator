# V1.5 SENCO Artifact Authorization Binding Evidence

Date: 2026-07-11

## Scope

- Generate `main_senco_artifact_authorization.json` beside the final SHA256 manifest.
- Bind the authorization record to the exact manifest path and manifest SHA256.
- Require a non-empty authorization ID, distinct reviewer/approver labels, and explicit writer scopes.
- Require an explicit authorized device-ID set; every selected writer device must be a member.
- Require the controlled writer CLI reviewer/approver labels to match the authorization record.
- Validate authorization before constructing `GasAnalyzer` for SENCO1/3, SENCO2/4, SENCO5, and SENCO6.
- Keep existing explicit unlock, operator confirmation, serial write, readback, and rollback behavior unchanged.

## Writer Scopes

- `co2_senco13_pair`
- `h2o_senco24_pair`
- `co2_senco5_linear`
- `h2o_senco6_linear`

## Verification

```text
python -m pytest tests\test_v1_5_senco_artifact_authorization.py tests\test_v1_5_artifact_hash_binding.py tests\test_v1_5_main_senco_write_precheck_pack.py tests\test_v1_5_final_senco_prewrite_gate.py tests\test_v1_5_co2_senco13_controlled_write.py tests\test_v1_5_h2o_senco24_controlled_write.py tests\test_v1_5_co2_senco5_linear_controlled_write.py tests\test_v1_5_h2o_senco6_linear_controlled_write.py -q
67 passed in 90.07s
```

```text
python -m pytest tests\test_v1_5_candidate_coefficients.py tests\test_v1_5_candidate_model_selection_review.py tests\test_v1_5_main_senco_write_precheck_pack.py tests\test_v1_5_candidate_write_review.py tests\test_v1_5_post_run_coefficient_executor.py tests\test_v1_5_artifact_hash_binding.py tests\test_v1_5_senco_artifact_authorization.py tests\test_v1_5_final_senco_prewrite_gate.py -q
90 passed, 1 existing marker warning in 22.39s
```

Four writer-level negative tests replace the authorization reviewer after package generation. A separate writer-level test removes the selected device from the authorized set. Each writer returns locked status before constructing `GasAnalyzer`.

## Safety Boundary

- No COM ports were opened.
- No SENCO coefficients were written.
- No gas or water route, valve, PACE, chamber, or humidity generator was controlled.
- No PostgreSQL connection or import was performed.
- Mature 0613/0620/0621 route, sampling, runner, protocol, default configuration, and `run_app.py` files were not changed.

## Residual Risk

- P0: none.
- P1: none.
- P2: this is a structured provenance record, not a cryptographic signature. A person with write access can still rewrite the manifest and authorization together. A future signed-record layer can add signer authenticity without changing the controlled writer contract.
