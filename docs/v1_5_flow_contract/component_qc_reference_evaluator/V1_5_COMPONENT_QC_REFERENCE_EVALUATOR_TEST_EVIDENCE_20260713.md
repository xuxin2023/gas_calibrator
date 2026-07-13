# V1.5 Component-QC Reference Evaluator Test Evidence

## Focused verification

```text
python -m pytest tests\test_v1_5_component_qc_reference_evaluator.py tests\test_v1_5_component_qc_generator_contract.py -q
22 passed in 2.45s
```

## Expanded evidence-governance regression

```text
python -m pytest tests\test_v1_5_component_qc_reference_evaluator.py tests\test_v1_5_component_qc_generator_contract.py tests\test_v1_5_component_qc_authority_audit.py tests\test_v1_5_p2_qc_derivation_design.py tests\test_v1_5_historical_fit_evidence_normalizer.py tests\test_v1_5_mature_route_contract.py tests\test_v1_5_entrypoint_inventory.py tests\test_v1_5_calibratable_point_policy.py tests\test_v1_5_advanced_qc_exporter.py -q
96 passed, 1 warning in 53.77s
```

The warning is the existing unregistered `pytest.mark.v1_5_formal_gate` marker in `test_v1_5_entrypoint_inventory.py`.

## Boundaries exercised

- Synthetic-only fixture admission.
- Per-analyzer independent A/B/C grading.
- CO2 and H2O inclusive ratio-span boundaries.
- Usable frame-count floors and temporal evidence handling.
- Point-wide physical hard blockers.
- Raw usable rows cannot be hidden by summary outlier filtering.
- No historical path, COM, device identity, write, fitting, release, or database-import capability.
- Offline entrypoint classification and mature-route contract compatibility.

These results are simulated review evidence and are not real acceptance evidence.
