# V1.5 Unified Controlled Write / Readback / Reverify Test Evidence

Date: `2026-07-13`

## Focused validation

```text
python -m pytest tests/test_v1_5_unified_controlled_write_reverify.py tests/test_v1_5_legacy_full_flow_offline_replay.py tests/test_v1_5_entrypoint_inventory.py -q

52 passed, 1 warning in 16.31s
```

## Writer and compatibility validation

```text
python -m pytest tests/test_v1_5_unified_controlled_write_reverify.py tests/test_v1_5_post_run_coefficient_executor.py tests/test_v1_5_final_senco_prewrite_gate.py tests/test_v1_5_post_write_reverification.py tests/test_v1_5_candidate_write_review.py tests/test_v1_5_main_senco_write_precheck_pack.py tests/test_v1_5_senco_artifact_authorization.py tests/test_v1_5_co2_senco13_controlled_write.py tests/test_v1_5_h2o_senco24_controlled_write.py tests/test_v1_5_co2_senco5_linear_controlled_write.py tests/test_v1_5_h2o_senco6_linear_controlled_write.py tests/test_v1_5_pressure_senco9_controlled_write.py tests/test_v1_5_sencoa_sencob_controlled_writer_preflight.py tests/test_v1_5_sencoa_sencob_writer_design_review.py tests/test_v1_5_formal_run_status.py tests/test_v1_5_production_component_qc_fit_matrix.py tests/test_v1_5_legacy_full_flow_offline_replay.py tests/test_v1_5_entrypoint_inventory.py tests/test_v1_5_mature_route_contract.py tests/test_v1_5_algorithm_route_profiles.py -q

248 passed, 3 warnings in 189.47s
```

The warnings are existing unregistered `v1_5_formal_gate` pytest markers.

## Confirmed boundaries

- Offline evidence review only; no COM ports are opened.
- No SENCO, SN/device_code, PostgreSQL, pressure, gas-route, or water-route execution occurs.
- S1/S3 and S2/S4 retain paired scientific-notation contracts.
- S5/S6 require current GETCO state and composed affine targets; write/readback and physical reverify are separate statuses.
- S7/S8 remain neutral-only; S9 linear mode requires explicit exception evidence.
- SENCOA/SENCOB remain blocked-design-only.
- Current checked-in historical matrix has zero fit-ready strategies, so the operation plan remains empty and unauthorized.
- The legacy replay consumes this artifact at stages 7 and 8 without claiming real acceptance.
