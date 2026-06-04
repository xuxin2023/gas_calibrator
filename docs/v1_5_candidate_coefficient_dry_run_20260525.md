# V1.5 Candidate Coefficient Dry Run 2026-05-25

This dry run used existing V1.5 no-write logs only. It did not open COM ports,
control PACE, switch water/gas routes, or write SENCO coefficients.

## Inputs

```text
run_dir:
logs/v1_5_h2o_co2_900ppm_ambient_1100_no_write_20260524_145656

plan:
logs/v1_5_real_no_write_900ppm_engineering_20260524/formal_evidence/formal_plan_snapshot.json

pressure reference:
logs/pressure_reference_certificates/com22_pressure_reference_FRGsz25038057_118288.json

output:
logs/v1_5_candidate_coefficient_dry_run_20260525/h2o_co2_900ppm_145656
```

The latest sibling run `153542` was also checked, but its `samples_*.csv` file
has length 0, so it cannot support candidate preparation.

## Result

The candidate export completed and produced no-write artifacts, but the run is
blocked:

```text
candidate_run_status = blocked
package_status = blocked
pressure_check_source = sample_rows_fallback
auto_write_allowed = false
opens_com_ports = false
controls_water_or_gas_routes = false
writes_coefficients = false
```

Main blockers from the policy summary:

```text
formal_candidate_review_not_ready
fit_samples<10
distinct_fit_targets<2
```

The formal open-flow package also reported:

```text
mode2_contract_missing
mode2_qc_missing
mode2_tokens_missing_or_invalid
pressure_channel_quick_check_fail
```

## Physical Meaning

This is the correct outcome. The old 900 ppm log is useful engineering history,
but it is not a complete formal candidate-coefficient evidence set because:

```text
1. It does not contain the current MODE2 contract/QC/token evidence required by
   the formal V1.5 evidence chain.
2. It does not contain an in-directory formal pressure-channel quick-check
   artifact, so pressure evidence had to fall back to sample rows.
3. The fallback pressure comparison exceeded the formal quick-check limit.
4. It contains only one CO2 standard concentration, so the CO2 fit is not
   identifiable as a calibration curve.
5. It has no independent verification point.
```

Therefore, no CO2/H2O candidate coefficients should be calculated or reviewed
from this run.

## Required Next Real Run Evidence

The next real open-flow no-write run should record:

```text
PRECHECK evidence
pressure_channel_quick_check_*.csv with continuous atmosphere hold verified
samples_*.csv with MODE2 contract status, QC status, and tokens
device identity from analyzer MODE2 ID, not serial port label
CO2/H2O fit point roles
an independent verification point role
standard gas and humidity reference snapshots
COM22 certificate snapshot
```

For CO2 coefficients:

```text
at least two distinct CO2 fit targets are required
900 ppm alone is smoke/verification evidence, not a full CO2 fit
```

For H2O coefficients:

```text
at least two distinct H2O fit targets are required
water-route evidence must keep dewpoint, H2O ratio, H2O signal, pressure, and
H2O target information
```

For current-atmosphere V1.5:

```text
P/RP/RTP pressure terms remain frozen
T/T2/RT temperature terms remain frozen unless a reviewed multi-temperature
design explicitly enables them
```
