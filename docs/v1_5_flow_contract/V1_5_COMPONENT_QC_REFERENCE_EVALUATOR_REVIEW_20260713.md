# V1.5 Component-QC Reference Evaluator Review

## Decision

The reference evaluator is available for synthetic, in-memory contract tests only.
The production component-QC generator remains unavailable and historical QC generation remains blocked.

## Evaluated semantics

- Grading is independent per analyzer.
- A point-level worst grade is informational and cannot remove another analyzer's fit eligibility.
- CO2 span limits are inclusive: A at or below `0.0005`, B at or below `0.001`, otherwise C.
- H2O is A at or below `0.001`; a larger span is B diagnostic-only and cannot cause C by itself.
- All raw sample-window ratios whose frame is usable participate; summary outlier filtering is not an input.
- A full required frame count can receive A; at least 90 percent can receive B; fewer frames receive C.
- A cadence warning caps an otherwise valid analyzer at B; a missing/incomplete temporal window receives C.
- Global alignment false alone does not reject a point.
- Route/reference physical hard blockers reject every analyzer for the point.
- CO2 zero gas and H2O dry-gas evidence retain separate physical roles from the approved contract.

## Synthetic-only boundary

The fixture must contain all of:

- `synthetic_fixture=true`
- `evidence_source=simulated`
- `not_real_acceptance_evidence=true`

Historical point directories, source sample paths, COM fields, device identity fields, and production source paths are rejected before evaluation. The CLI writes only JSON/CSV/Markdown review artifacts into an explicitly supplied output directory.

## Locks

- `production_component_qc_generator_available=false`
- `historical_component_qc_generation_allowed=false`
- `historical_component_qc_write_allowed=false`
- `component_qc_backfill_allowed=false`
- `historical_fit_allowed=false`
- `formal_release_allowed=false`
- `database_import_allowed=false`
- `opens_com_ports=false`
- `controls_pressure=false`
- `controls_water_or_gas_routes=false`
- `writes_coefficients=false`
- `writes_sn_or_device_code=false`
- `connects_postgresql=false`
- `not_real_acceptance_evidence=true`

## Production consequence

This package proves that the reviewed grading contract is executable on synthetic evidence. It does not recover a 0613/0620/0621 historical writer, does not authorize backfill of the 125 candidate point directories, and does not change the mature pre-sample gates or sampling actions.
