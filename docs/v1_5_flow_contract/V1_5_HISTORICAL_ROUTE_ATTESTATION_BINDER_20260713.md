# V1.5 Historical Mature-Root Attestation Binder

## Purpose

The binder prevents a historical directory label from being treated as proof of the mature V1.5 route. It emits a normalizer-compatible reviewed attestation only when the exact route root proves all of the following:

- the 0613 fitting contract and 0620/0621 physical-route contract are both current and passing;
- exactly one closed queue summary and manifest belong to the exact output root;
- the profile-specific point set is complete and unique (`45/13` legacy or `47/14` absorption candidate);
- every queue point completed successfully;
- every point has a mature continuous-atmosphere-hold sidecar, 1 Hz acquisition, samples, and component quality evidence;
- sample, QC, sidecar, queue summary, and manifest files are bound by SHA-256;
- the fit evidence normalizer rechecks the bound queue summary, manifest, inventory, and every inventory-listed evidence file before consuming a reviewed family;
- the source is not 0624, migration, segmented, retry, direct recovery, or diagnostic evidence.

Historical `_handoff` storage is not rejected by location alone. Execution provenance is reviewed separately. A root stored under `_handoff` can only pass when its queue and point evidence independently prove the mature route. Cross-run quality backfills and successful retry points do not turn a segmented root into continuous evidence.

## Current Read-Only Audit

The audit is intentionally blocked and emits no reviewed family:

| Family / route | Observed | Result |
| --- | --- | --- |
| `mature_0620_legacy_ratio:co2` | 45 planned points, but the source is dated 20260624, uses migration/direct provenance, and closes at 43 ok / 2 failed | blocked |
| `new_algorithm_shadow_run:co2` | 39 observed points versus required 47; route readiness was skipped; component QC is absent | blocked |
| `new_algorithm_shadow_run:h2o` | 13 observed points versus required 14; route readiness was not proven; component QC is absent; sidecars record 360 s actual purge below 720 s minimum | blocked |

The expanded audit contains 89 blocker rows because point-level missing evidence is listed separately. This count must not be interpreted as 89 independent program defects.

## Outputs

- `historical_route_attestation_binder/v1_5_historical_route_baseline_attestation.json`
- `historical_route_attestation_binder/v1_5_historical_route_attestation_roots.csv`
- `historical_route_attestation_binder/v1_5_historical_route_attestation_blockers.csv`
- `historical_route_attestation_binder/v1_5_historical_route_attestation_evidence_inventory.csv`
- `historical_route_attestation_binder/V1_5_HISTORICAL_ROUTE_ATTESTATION_BINDER.md`

All outputs are offline review evidence. They do not open COM, control pressure or routes, write coefficients or identity, connect PostgreSQL, authorize release, or authorize database import.
