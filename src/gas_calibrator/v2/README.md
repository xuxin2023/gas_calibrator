# Gas Calibrator V2

## Scope

V2 is the active development line, but the repository is still in `Step 2: production-grade platformization`.

Authoritative scope for current work:

1. `D:\gas_calibrator\AGENTS.md`
2. `D:\gas_calibrator\.ai-context\PROJECT_STATUS.md`
3. `D:\gas_calibrator\.ai-context\TODO.md`

If this README conflicts with those files, those files win.

## Current Boundaries

Current V2 work is limited to:

- simulation
- replay
- suite regression
- parity
- resilience
- offline review
- UI contract
- artifact governance
- acceptance and lineage scaffolding
- Chinese-first UI and 1080p layout hardening
- simulation-only workbench productization

Current V2 work does not include:

- real device bring-up
- real serial or COM access
- formal trial runs
- real compare or real verify
- refreshing `real_primary_latest`
- default entry switch to V2
- any claim that V2 replaces V1

Any simulated or offline result is not real acceptance evidence.

## Recommended Step 2 Entrypoints

Safe simulation suite:

```powershell
PYTHONPATH=src python -m gas_calibrator.v2.scripts.test_v2_safe
```

Headless simulation run:

```powershell
PYTHONPATH=src python -m gas_calibrator.v2.scripts.run_v2 --config src/gas_calibrator/v2/configs/smoke_v2_minimal.json --simulation --headless
```

Simulation-only device helper:

```powershell
PYTHONPATH=src python -m gas_calibrator.v2.scripts.test_v2_device connection
PYTHONPATH=src python -m gas_calibrator.v2.scripts.test_v2_device single
PYTHONPATH=src python -m gas_calibrator.v2.scripts.test_v2_device full
```

`test_v2_device` is simulation-only by default. Any future real bench path must remain explicitly locked and non-default.

## Module Map

V2 currently contains active code in these areas:

- `adapters/`
- `algorithms/`
- `analytics/`
- `calibration/`
- `config/`
- `configs/`
- `core/`
- `domain/`
- `export/`
- `intelligence/`
- `qc/`
- `scripts/`
- `sim/`
- `storage/`
- `ui_v2/`

This README should not describe implemented modules as TODO placeholders.

## Artifact Governance

Artifact roles must stay stable:

- `execution_rows`
- `execution_summary`
- `diagnostic_analysis`
- `formal_analysis`

Export status values must stay stable:

- `ok`
- `skipped`
- `missing`
- `error`

User-visible summary layers should explicitly surface:

- `evidence_source`
- `not_real_acceptance_evidence`
- `acceptance_level`
- `promotion_state`

## V1 Boundary

- Do not modify V1 production logic unless explicitly required for a non-behavioral boundary fix.
- Do not modify `run_app.py` as part of V2 Step 2 work.
- Do not connect new V2 features back into the V1 UI.
