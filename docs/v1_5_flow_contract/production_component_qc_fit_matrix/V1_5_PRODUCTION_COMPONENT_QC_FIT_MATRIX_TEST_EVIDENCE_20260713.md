# V1.5 Production Component-QC and 0613 Fit Matrix Test Evidence

- base origin/main: `d423b15a32c4ef2476a2806f2de7a2414941e47b`
- evidence date: `2026-07-13`
- result: `150 passed, 1 warning in 60.97s`
- warning: existing unregistered `v1_5_formal_gate` pytest marker

## Coverage

- production component-QC evaluator and central review-only writer
- per-analyzer A/B/C independence
- immutable source-artifact SHA binding
- CO2 zero-gas and H2O dry-gas anchor separation
- H2O declared-purge physical blocker
- canonical 0613 model-family parity
- 0613 point-selection strategy matrix
- legacy full-flow replay integration
- component-QC contracts, preflight, blocked writer plan, and writer design
- fit-input quality, mature-route, historical-replay, entrypoint, and final-gap guards

## Command

```powershell
python -m pytest `
  tests\test_v1_5_production_component_qc_fit_matrix.py `
  tests\test_v1_5_legacy_full_flow_offline_replay.py `
  tests\test_v1_5_entrypoint_inventory.py `
  tests\test_v1_5_mature_route_contract.py `
  tests\test_v1_5_historical_replay_contract.py `
  tests\test_v1_5_historical_replay_evidence.py `
  tests\test_v1_5_component_qc_authority_audit.py `
  tests\test_v1_5_component_qc_generator_contract.py `
  tests\test_v1_5_component_qc_reference_evaluator.py `
  tests\test_v1_5_historical_component_qc_generator_preflight.py `
  tests\test_v1_5_historical_component_qc_blocked_generator_plan.py `
  tests\test_v1_5_historical_component_qc_controlled_writer_design.py `
  tests\test_v1_5_candidate_model_selection_review.py `
  tests\test_v1_5_fit_input_quality.py `
  tests\test_v1_5_final_production_gap_freeze.py -q
```

## Live Evidence Result

- bound historical candidate points: `125`
- CO2/H2O points: `89 / 36`
- per-analyzer QC rows: `460`
- grades: `312 A / 2 B / 146 C`
- 0613 strategy rows: `420`
- fit-ready strategies: `0`
- production fit remains blocked because no continuous mature route root is attested.
- all 36 H2O candidate points retain a declared-purge physical conflict and are not promoted.

## Boundaries

- No historical point directory is written or backfilled.
- No fit is executed and no candidate coefficient is emitted for writing.
- No COM, pressure, gas/water route, PostgreSQL, release, or import operation occurs.
- This is historical replay evidence and is not real acceptance evidence.
