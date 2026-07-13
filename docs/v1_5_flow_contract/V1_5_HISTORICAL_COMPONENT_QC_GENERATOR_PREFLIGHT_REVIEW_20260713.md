# V1.5 Historical Component-QC Generator Preflight Review

## Purpose

This package adds the offline input-integrity gate between the reviewed synthetic component-QC evaluator and any future historical writer. It does not derive A/B/C grades and does not create or replace `formal_open_flow_data_quality_by_analyzer.csv` in a historical point directory.

## Live Offline Result

- overall status: `ready_for_historical_component_qc_generator_preflight_manual_review`
- P2 candidates: `125`
- candidate source packets ready for manual review: `125`
- blocked candidate source packets: `0`
- source artifact checks: `697`
- missing, size-mismatched, or SHA256-mismatched artifacts: `0`
- existing component-QC output targets: `0`
- candidates retaining manual gate review: `125`

The result means the recorded source packets are intact enough for the next design review. It does not mean historical QC generation, formal fitting, release, or database import is allowed.

## Guarded Boundaries

The preflight requires the exact artifact inventory declared by the P2 design JSON. It rejects missing roles, duplicate role rows, files outside the point directory, empty files, size or SHA256 drift, malformed upstream locks, a mismatched synthetic reference contract, and any existing target that would be overwritten.

All execution locks remain closed:

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

The evidence source is `historical_replay` and remains `not_real_acceptance_evidence=true`.

## Mature Path Boundary

This package does not modify the 0613 fitting baseline, 0620/0621 mature CO2/H2O queues, shared sampling, workflow runner, analyzer protocol, default configuration, or `run_app.py`. It also does not use 0624/migration component-QC decisions as threshold authority.

## Verification

- focused component-QC/preflight tests: `42 passed`
- authority/P1/P2/legacy evidence tests: `25 passed`
- entrypoint inventory tests: `36 passed, 1 existing marker warning`
- historical route-attestation tests: `16 passed`
- historical fit-normalizer tests: `13 passed`
- total distinct tests: `132 passed, 1 existing marker warning`

The warning is the existing unregistered `v1_5_formal_gate` pytest marker and is not a functional failure.

## Next Allowed Step

The next package may design a blocked historical component-QC generator plan that consumes this exact preflight and emits a would-write preview. It must still refuse historical file creation or replacement until a separate authorization, atomic writer, readback, rollback, and post-write verification chain is reviewed.
