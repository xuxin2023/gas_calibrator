# V1.5 Route Physical Recovery Readiness

- schema: `v1_5_route_physical_recovery_readiness_v1`
- status: `blocked`
- next_continuous_run_allowed: `False`
- segmented_evidence_fit_use_allowed: `False`
- blocker_count: `4`
- review_required_count: `1`

## Physical Meaning

- PACE vent or pressure-gauge NO_RESPONSE must be recovered before the next continuous queue.
- Dry-gas dewpoint rebound must be resolved with stable dry evidence before the next CO2 zero-gas point.
- Segmented, direct, retry, or stale-running evidence is not a continuous formal run unless an accepted manifest reviews it.
- A fresh canonical 0613/0620/0621 queue is required before starting the next continuous run after these failures.

## Findings

| severity | category | requirement | status | reason | required action |
|---|---|---|---|---|---|
| `blocker` | `dry_gas_dewpoint_rebound_or_not_dry_enough` | `dry_gas_dewpoint_recovery` | `missing_or_failed` | Dry-gas dewpoint blocker exists but recovery evidence is missing, not dry enough, unstable, or route/dryer check is absent. | Recover the dry-gas source/route, prove dewpoint <= threshold with stable tail, then rerun a clean smoke before a continuous queue. |
| `blocker` | `pressure_controller_vent_no_response` | `pace_vent_recovery` | `missing_or_failed` | PACE vent NO_RESPONSE blocker exists without reviewed vent roundtrip recovery. | Recover PACE vent communication and prove ON/OFF roundtrip before opening a continuous route queue. |
| `blocker` | `pressure_gauge_no_response` | `pressure_gauge_recovery` | `missing_or_failed` | Pressure gauge NO_RESPONSE blocker exists without reviewed INL absolute-pressure readback recovery. | Restore pressure gauge readback and prove the mature INL absolute-pressure source before a continuous queue. |
| `blocker` | `fresh_canonical_queue_policy` | `next_run_policy` | `missing_or_failed` | Prior segmented/aborted/direct evidence exists, but the next run policy does not prove a fresh canonical queue. | Open the next CO2/H2O run from the mature 0613/0620/0621 formal queue, not from _handoff, 0624, worker, diagnostic, retry, or root migration surfaces. |
| `review` | `accepted_manifest_review` | `accepted_manifest_review` | `missing` | Segmented/direct/retry evidence lacks accepted-manifest supersedence review. | Keep old segmented evidence out of fitting, or bind it through an accepted manifest before any coefficient calculation. |

## Boundary

- Offline evidence review only.
- Does not open COM ports, control pressure, control gas/water routes, connect PostgreSQL, or write coefficients/SN.
- This readiness is not formal release, database import, or real acceptance evidence.
