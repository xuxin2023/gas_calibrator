# V1.5 Legacy Evidence Gap Task Plan Review

## Decision

The legacy catalog now has a deterministic manual work queue. The queue is for offline evidence review only: it does not derive QC, mutate source artifacts, authorize fitting, or promote segmented/retry/recovery evidence to a continuous mature route.

## Source

- Catalog: `docs/v1_5_flow_contract/legacy_historical_evidence_catalog/v1_5_legacy_historical_evidence_catalog.json`
- Catalog points: `181`
- Cataloged artifact rows revalidated: `892`
- Artifact integrity mismatches: `0`

Every cataloged source artifact was re-read and matched its committed size and SHA-256. Any future missing, size-changed, or hash-changed artifact becomes a P0 integrity blocker.

## Task Result

- Total tasks: `181`
- P1 core evidence: `2` CO2 points
- P2 quality/traceability: `125` points (`89` CO2, `36` H2O)
- P3 superseded reference: `7` CO2 attempts
- P3 forbidden reference: `47` CO2 points from 0624/migration sources

The two P1 points are historical new-algorithm CO2 points with sidecars but no samples/frame QC/component QC:

- `p017_T20_200ppm_fit`
- `p006_Tm20_1000ppm_fit`

Seven older smoke/failed attempts have an accepted-composite alternative for the same root and physical point. They are retained as original evidence and are not repaired, copied, or directly quality-bound to the alternative.

## Hard Boundaries

- Same-point evidence is required for any later QC backfill review.
- Cross-run QC is reference-only and cannot be directly bound.
- Automatic QC derivation is forbidden in this task plan.
- 0624/migration evidence remains diagnostic-only even when files are complete.
- Accepted composite warnings remain visible and cannot be cleared by this plan.
- CO2 zero gas and H2O dry-gas anchors remain physically distinct; no anchor role is inferred.
- `continuous_route_attestation_allowed=false`
- `historical_fit_allowed=false`
- `formal_release_allowed=false`
- `database_import_allowed=false`

## Next Action

The next offline step should inspect only the two P1 new-algorithm directories for same-point sample artifacts in their immediate run lineage. If none exist, mark those attempts unrecoverable instead of borrowing legacy or cross-run samples. P2 component-QC gaps require a separate QC-derivation design review before any generated quality file is accepted.

## Validation

Focused compatibility coverage included the new task planner, legacy catalog, historical QC-gap and missing-point audits, fit-input normalizer, route attestation binder, mature continuity gate, and entrypoint inventory.

Result: `92 passed, 1 existing marker warning in 35.95s`.

Static validation: ruff passed, Python compilation passed, and `git diff --check` passed apart from Windows line-ending notices.
