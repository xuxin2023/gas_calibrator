# V1.5 P2 component-QC derivation design review

## 1. Review purpose

This review classifies whether legacy P2 point evidence has the same-point inputs
needed by a future component-QC generator. It does not derive grades, create
`formal_open_flow_data_quality_by_analyzer.csv`, authorize fitting, or promote
segmented evidence into a continuous 0613/0620/0621 route run.

The implementation and generated artifacts are offline only. They do not open
COM ports, control pressure or gas/water routes, write device identity or SENCO
coefficients, connect PostgreSQL, authorize release, or import data.

## 2. Live evidence result

The live export over the legacy evidence task plan and the P1 same-lineage audit
produced these results:

- unique candidate directories: `125`
- CO2 candidates: `89`
- H2O candidates: `36`
- structurally complete same-point input sets: `125`
- structurally incomplete input sets: `0`
- manual gate review required: `125`
- reviewed mature component-QC generator available: `false`
- QC derivation execution allowed: `false`
- generated QC write allowed: `false`
- historical fitting allowed: `false`

The candidate count remains `125`, not `126`. The P1-recovered
`p006_Tm20_1000ppm_fit_retry1` directory already exists in the P2 task set. The
design classifier de-duplicates by resolved point directory and preserves the
stronger `p1_same_lineage_retry_reference` source role. No candidate is lost.

## 3. Input interpretation

All 125 candidates contain the route-specific source bundle required for design
review: point samples, frame quality summary, runtime configuration, and the
corresponding CO2 or H2O route evidence. This only proves that the inputs can be
reviewed together.

All 125 candidates still require manual gate review because their point samples
contain at least one `sample_alignment_ok=false` row. The 36 H2O candidates also
record an actual purge duration below the sidecar's declared minimum. Two
candidates retain accepted-manifest warnings. These conditions must remain
visible to any future per-analyzer derivation and cannot be silently converted
to an A-grade point.

`sample_alignment_ok=false` is not treated as automatic point rejection here.
It is treated as an unresolved grading input because this package intentionally
does not recreate the mature point-level/per-analyzer QC algorithm.

## 4. Missing authority

The repository contains consumers of
`formal_open_flow_data_quality_by_analyzer.csv`, but this review did not find a
reviewed 0613/0620/0621 writer that can reproduce the mature per-analyzer grade,
reason, and fit-eligibility fields from the recorded samples and frame evidence.

Component-QC files observed under 0624 or migration evidence may be used only as
output-schema references. Their thresholds and decisions are not authoritative
for the mature V1.5 path.

Before QC generation can be implemented, a separate reviewed contract must
define, from 0613/0620/0621 evidence and code:

1. CO2 and H2O ratio inputs and tolerances.
2. Per-analyzer frame and sample usability rules.
3. Treatment of alignment failures, accepted warnings, purge exceptions, and
   point-level quality blocks.
4. Exact grade, reason, and fit-eligibility output semantics.
5. Source hashes, immutable-input rules, and idempotent output behavior.

## 5. Physical boundary

CO2 zero-gas evidence and H2O dry/low-water evidence remain different physical
roles. This design package does not manufacture either anchor and does not move
evidence between routes or runs.

The next package should therefore be a generator-contract authority audit, not
a QC backfill executor. Until that contract is reviewed, every candidate remains
blocked from generated QC, fitting, release, and database import.

## 6. Verification

Focused P2/P1/task-plan/entrypoint tests:

```text
56 passed, 1 warning
```

Legacy evidence, historical replay, mature-route, fit-input, and entrypoint
regression set:

```text
107 passed, 2 warnings
```

The warnings are the existing unregistered `v1_5_formal_gate` pytest marker and
do not represent a functional failure in this package.
