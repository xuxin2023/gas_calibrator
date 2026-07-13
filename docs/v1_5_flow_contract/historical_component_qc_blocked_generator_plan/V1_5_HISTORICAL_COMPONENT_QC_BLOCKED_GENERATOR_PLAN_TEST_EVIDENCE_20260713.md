# V1.5 Historical Component-QC Blocked Generator Plan Test Evidence

## Live Historical Replay

```text
overall_status=ready_for_historical_component_qc_blocked_generator_plan_review
candidate_plan_ready_count=125
candidate_blocked_count=0
source_artifact_check_count=697
source_artifact_check_blocked_count=0
operation_plan_count=125
would_write_true=0
would_evaluate_true=0
```

## Focused And Governance Regression

```text
155 passed, 1 warning in 129.19s
```

Command:

```powershell
python -m pytest tests\test_v1_5_historical_component_qc_blocked_generator_plan.py tests\test_v1_5_historical_component_qc_generator_preflight.py tests\test_v1_5_component_qc_reference_evaluator.py tests\test_v1_5_component_qc_generator_contract.py tests\test_v1_5_component_qc_authority_audit.py tests\test_v1_5_p2_qc_derivation_design.py tests\test_v1_5_p1_evidence_lineage_audit.py tests\test_v1_5_legacy_evidence_gap_task_plan.py tests\test_v1_5_entrypoint_inventory.py tests\test_v1_5_historical_route_attestation_binder.py tests\test_v1_5_historical_fit_evidence_normalizer.py tests\test_v1_5_mature_route_contract.py tests\test_v1_5_calibratable_point_policy.py tests\test_v1_5_advanced_qc_exporter.py -q
```

The warning is the existing unregistered `v1_5_formal_gate` marker. No test or replay opened COM, controlled pressure or routes, wrote identity or coefficients, connected PostgreSQL, derived historical component-QC grades, wrote historical point artifacts, authorized fitting, or produced real acceptance evidence.
