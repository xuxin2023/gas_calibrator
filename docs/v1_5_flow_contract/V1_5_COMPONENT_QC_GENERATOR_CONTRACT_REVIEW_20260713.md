# V1.5 component-QC generator contract review

## Decision

The design-only component-QC contract is ready for manual review. It does not
implement a writer and does not authorize generation or historical backfill.

## Physical grading rules

- Grading is per analyzer. One unstable analyzer cannot invalidate another
  analyzer that satisfies its own quality and physical gates.
- Point summary grade is informational and must not replace per-analyzer fit
  eligibility.
- Public physical failures such as a missing sample window, route closure before
  sampling completes, invalid reference target, or explicit point quality block
  reject the whole point.
- QC uses all raw sample-window ratios whose analyzer frame is usable. The
  summary-only one-outlier filter cannot hide instability from QC.
- Global `sample_alignment_ok=false` is not, by itself, a point-wide rejection;
  route-critical analyzer/reference evidence is evaluated separately.

## Route thresholds

CO2 uses `co2_ratio_f`:

- span `<=0.0005`: `A_calibration_eligible`
- `0.0005 < span <= 0.001`: `B_diagnostic_model_only`
- span `>0.001`: `C_reject`

H2O uses `h2o_ratio_f`:

- span `<=0.001`: `A_calibration_eligible`
- span `>0.001`: `B_diagnostic_model_only`
- ratio span alone cannot assign H2O `C_reject`; C requires missing/invalid data
  or a hard physical/data-integrity failure.

Every A grade enters calibration fit and diagnostic review. B is diagnostic
only and cannot enter calibration fit. C enters neither.

## Frame and cadence rules

- A requires the full runtime-declared usable sample count (10 for the audited
  points).
- 90% to less than full usable count is capped at B.
- Below 90%, missing/nonfinite ratio, or incomplete sample window is C.
- A cadence warning with the required rows is capped at B rather than rejecting
  all analyzers. Missing timestamps or an incomplete temporal window is C.

## Traceability

The future writer must record hashes for source samples, frame QC, runtime
configuration, and this contract. Inputs are immutable and identical hashes
must produce identical output.

CO2 zero gas remains a CO2 concentration anchor. H2O dry gas remains a low-water
anchor and requires dewpoint/pressure evidence. The generator cannot exchange or
manufacture these roles.

## Locked state

- `implementation_available=false`
- `component_qc_generation_allowed=false`
- `component_qc_backfill_allowed=false`
- `historical_fit_allowed=false`
- `formal_release_allowed=false`
- `database_import_allowed=false`

The next package may implement a pure in-memory/reference evaluator against
synthetic fixtures only. It must still not write the 125 historical QC files.

## Verification

Focused contract/authority/P2/entrypoint tests:

```text
52 passed, 1 warning
```

Expanded historical evidence, mature-route, fit-input, and CO2/H2O sampling
configuration regression:

```text
153 passed, 2 warnings
```

The warnings are the existing unregistered `v1_5_formal_gate` marker.
