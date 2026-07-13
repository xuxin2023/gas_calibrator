# V1.5 Legacy Full-Flow Offline Replay Test Evidence

- source origin/main: `661b2b280b43d85df06c81df09e9d3f02165278b`
- evidence date: `2026-07-13`
- focused result: `47 passed, 1 warning in 30.44s`
- orchestration compatibility result: `16 passed in 30.86s`
- total recorded passing tests: `63`
- warning: existing unregistered `v1_5_formal_gate` pytest marker

## Focused Command

```powershell
$env:PYTHONPATH='src'
python -m pytest `
  tests\test_v1_5_legacy_full_flow_offline_replay.py `
  tests\test_v1_5_final_production_gap_freeze.py `
  tests\test_v1_5_entrypoint_inventory.py -q
```

## Orchestration Compatibility

```powershell
$env:PYTHONPATH='src'
python -m pytest `
  tests\test_v1_5_full_flow_orchestration.py::test_full_flow_plan_keeps_pressure_and_temperature_before_components `
  tests\test_v1_5_full_flow_orchestration.py::test_full_flow_plan_preserves_validated_co2_h2o_route_contracts `
  tests\test_v1_5_full_flow_orchestration.py::test_full_flow_plan_binds_final_batch_closeout_before_mature_open_flow `
  tests\test_v1_5_full_flow_orchestration.py::test_full_flow_plan_adds_no_write_post_run_coefficient_executor `
  tests\test_v1_5_full_flow_orchestration.py::test_full_flow_plan_adds_offline_closure_readiness_gate `
  tests\test_v1_5_full_flow_orchestration.py::test_write_stage_is_never_auto_executable_from_state `
  tests\test_v1_5_automation_control_contract.py `
  tests\test_v1_5_production_entrypoint_gate.py -q
```

## Boundaries

- The replay consumes checked-in summaries only and does not follow stale absolute paths into live execution.
- It opens no COM ports, controls no pressure/gas/water route, and writes no coefficient or identity.
- It does not connect to PostgreSQL or authorize archive release/import.
- Segmented/retry/composite evidence remains diagnostic and is not promoted to continuous mature-route evidence.
- `evidence_source=historical_replay` and `not_real_acceptance_evidence=true` remain mandatory.

## 2026-07-13 Component-QC / 0613 Integration Refresh

- replay source origin/main advanced to `d423b15a32c4ef2476a2806f2de7a2414941e47b`.
- the replay now binds `production_component_qc_fit_matrix` as its thirteenth source artifact.
- stage 6 no longer reports a missing evaluator or an unclosed strategy selector.
- stage 6 remains held only because the catalog and current QC packet are not fit-eligible continuous mature-route evidence.
- integration is covered by the `150 passed, 1 warning in 60.97s` suite recorded in the production component-QC test evidence.
