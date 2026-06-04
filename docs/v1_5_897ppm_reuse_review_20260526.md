# V1.5 897 ppm CO2 Historical Evidence Reuse Review 2026-05-26

This review is an offline/no-write review of already recorded V1.5 open-flow
evidence. It does not open COM ports, control PACE, switch gas/water routes, or
write SENCO/ID values.

## Inputs

```text
CO2 run:
logs/logs/v1_5_formal_co2_open_flow_5ch_mode2_active_900ppm_20260526/
co2_897ppm_keep_invalid_frames_20260526_01

H2O run checked for contrast:
logs/logs/v1_5_formal_h2o_open_flow_5ch_mode2_active_20260526/
h2o20c70_keep_invalid_frames_20260526_01

Formal plan:
logs/v1_5_formal_no_write_897ppm_co2_h2o_refs_20260525/
formal_plan_snapshot.json

Pressure reference:
logs/v1_5_formal_no_write_897ppm_co2_h2o_refs_20260525/
com22_pressure_reference.json

External pressure quick check:
logs/v1_5_pressure_channel_quick_check_5ch_20260526/
pressure_quick_check_5ch_atm_20260526_01
```

## CO2 Reuse Decision

The previous CO2 data can be reused per analyzer device ID. The bad devices do
not invalidate the entire run.

| Analyzer | Device ID | Pressure Check | A-grade CO2 Samples | Decision |
| --- | --- | --- | ---: | --- |
| ga01 | ID023 | pass | 10 | keep as A-grade single-target review evidence |
| ga02 | ID030 | pass | 10 | keep as A-grade single-target review evidence |
| ga03 | ID033 | pass | 10 | keep as A-grade single-target review evidence |
| ga04 | ID001 | insufficient evidence | 0 | reject this device only |
| ga05 | ID027 | insufficient evidence | 0 | reject this device only |

The retained CO2 samples are not a complete formal coefficient fit because the
run contains only one CO2 target, `897.04 ppm`. The correct classification is:

```text
evidence_reuse_class = a_grade_single_target_review_only
blocked_reasons = distinct_fit_targets<2
```

This means the data can support a single-point bias review or later independent
verification, but it cannot by itself identify a full CO2 calibration curve.

## H2O Contrast Decision

The previous H2O data must not be upgraded to A-grade fit input. The pressure
quick check itself can pass, but the H2O sample window shows analyzer internal
pressure instability:

| Analyzer | Device ID | B-grade Cause |
| --- | --- | --- |
| ga01 | ID023 | analyzer_pressure_hpa_span=5.7>limit=2 |
| ga02 | ID030 | h2o_ratio_f_span=0.001>limit=0.001; analyzer_pressure_hpa_span=9.7>limit=2 |
| ga03 | ID033 | analyzer_pressure_hpa_span=11.3>limit=2 |

This is a physical water-route sampling-window problem, not just a bad
ID001/ID027 problem. H2O should be rerun with separate dewpoint, H2O ratio, and
analyzer internal pressure P stability before the formal sample window.

## Generated Review Artifacts

```text
CO2 candidate review:
logs/v1_5_candidate_coefficient_review_20260526/
co2_897ppm_external_pressure_salvage_20260526_06

H2O candidate review:
logs/v1_5_candidate_coefficient_review_20260526/
h2o20c70_external_pressure_salvage_20260526_06

CO2 open-flow QC sidecar:
logs/v1_5_formal_open_flow_review_20260526/
co2_897ppm_external_pressure_salvage_20260526_04

H2O open-flow QC sidecar:
logs/v1_5_formal_open_flow_review_20260526/
h2o20c70_external_pressure_salvage_20260526_04
```

## Physical Meaning

Every analyzer is an independent measurement chain. If one analyzer reports
zero/suspect MODE2 frames, that proves a failure of that analyzer chain; it
does not prove the standard gas state was invalid for all analyzers.

For CO2, the clean 897 ppm open-flow evidence from ID023, ID030, and ID033 can
be preserved. For H2O, the water vapor state must also satisfy internal
pressure stability because the analyzer concentration algorithm uses pressure
P. A moving P during the water sample window makes the H2O result hard to
interpret and must remain B-grade/review evidence.
