# V1.5 algorithm runlist readiness

This is an offline readiness gate for the new-algorithm formal runlist preview.

- overall_status: `ready_for_new_algorithm_runner_integration_review`
- blocker_count: `0`
- legacy CO2/H2O counts: `45` / `13`
- new-algorithm CO2/H2O runlist counts: `47` / `14`
- Required supplemental formal points: `-20/600`, `-10/600`, `40/30/30`.
- This sidecar is not real acceptance evidence and does not authorize route execution.

## Checks

- `runlist_preview_manifest_contract`: `ready`
- `new_algorithm_co2_runlist_47_point_gate`: `ready`
- `new_algorithm_h2o_runlist_14_point_gate`: `ready`
- `formal_supplemental_point_semantics_gate`: `ready`
