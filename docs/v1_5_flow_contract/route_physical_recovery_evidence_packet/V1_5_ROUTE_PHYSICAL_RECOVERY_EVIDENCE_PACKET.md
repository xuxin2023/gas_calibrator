# V1.5 Route Physical Recovery Evidence Packet

This offline validator checks the reviewed evidence packet that can be passed to
the V1.5 route physical recovery readiness gate. It is not a live route runner
and it is not real acceptance evidence.

## Purpose

- Prove the dry-gas route has recovered from dewpoint rebound before the next
  CO2 zero-gas point.
- Prove PACE vent communication has recovered before the next continuous queue.
- Prove pressure-gauge readback uses the mature INL absolute-pressure source.
- Prove the next run policy is a fresh canonical 0613/0620/0621 queue, not a
  root migration, 0624, `_handoff`, diagnostic, worker, or retry surface.
- Keep segmented/direct/retry evidence out of fitting unless an accepted
  manifest supersedence review exists.

## Required Packet Fields

- `dry_gas_dewpoint_recovery`
  - `status=pass`
  - `dewpoint_c <= dry_enough_threshold_c`, default threshold `-28 C`
  - `tail_span_c <= max_tail_span_c`, default max `0.5 C`
  - `tail_slope_abs_c_per_s <= max_tail_slope_abs_c_per_s`, default max `0.01`
  - `route_or_dryer_checked=true`
  - non-empty `evidence`
- `pace_vent_recovery`
  - `status=pass`
  - `vent_on_off_roundtrip_pass=true`
  - `no_response_absent=true`
  - non-empty `evidence`
- `pressure_gauge_recovery`
  - `status=pass`
  - `readback_status=pass`
  - `absolute_pressure_source` must be the mature INL absolute-pressure source
  - `no_response_absent=true`
  - non-empty `evidence`
- `next_run_policy`
  - `fresh_canonical_queue=true`
  - `mature_physical_baseline` includes `0613`, `0620`, and `0621`
  - `forbidden_surfaces_absent=true`
  - references canonical CO2/H2O queue entrypoints
  - must not reference root migration paths such as `D:/gas_calibrator/src/...`
- `accepted_manifest_review`
  - optional for opening a fresh run
  - required before segmented/direct/retry evidence can be fit-eligible

## Boundary

- Offline packet validation only.
- Does not open COM ports.
- Does not control pressure.
- Does not control gas or water routes.
- Does not connect PostgreSQL.
- Does not write SN, device ID, SENCO, or calibration coefficients.
- Does not allow formal release or database import.
- Passing this packet only makes the evidence suitable for the route physical
  recovery readiness gate; it does not itself start the next run.
