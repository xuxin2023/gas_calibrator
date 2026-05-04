# A2.12R Real Probe FAIL_CLOSED Summary

Date: 2026-04-30

## Scope

- Branch: `codex/run001-a1-no-write-dry-run`
- HEAD: `e6e242c567a4f55daca865a28c534b1cf40142ec`
- Origin HEAD: `8ffb4bbfe314d35863cc1e9696aa45b2bae86ddf`
- Probe output directory: `D:\gas_calibrator_step3a_a2_12r_v1_aligned_co2_7_pressure_no_write_skip_temp_wait_20260430_1231`
- Wrapper exit code: `2`
- Final decision: `FAIL_CLOSED`
- Real probe executions in this turn: exactly one.

## Result

- `artifact_completeness_pass=true`
- `required_artifacts_missing=[]`
- `interrupted_execution=false`
- `no_write_assertion_status=pass`
- `no_write=true`
- `attempted_write_count=0`
- `any_write_command_sent=false`
- `a3_allowed=false`
- `evidence_source=real_probe_a2_12r_co2_7_pressure_no_write`
- `a2_conditioning_pressure_source_strategy=v1_aligned`
- `pressure_source_strategy_aggregation_mismatch=false`

## Fail-Closed Reason

- Wrapper `fail_closed_reason`: `a2_pass_conditions_not_met`
- Underlying V2 run failure: `Critical device initialization failed [failed_devices=['temperature_chamber']]`
- Underlying V2 run directory: `D:\output\run001_a2\co2_only_7_pressure_no_write\run_20260430_123401`
- A2.12R-I commit HEAD: `e6e242c567a4f55daca865a28c534b1cf40142ec`
- Summary key fields:
  - `artifact_completeness_pass=true`
  - `required_artifacts_missing=[]`
  - `interrupted_execution=false`
  - `no_write_assertion_status=pass`
  - `evidence_source=real_probe_a2_12r_co2_7_pressure_no_write`
  - `probe_identity=A2.12R CO2-only seven-pressure no-write engineering probe`
  - `probe_version=A2.12R`
  - `no_write=true`
  - `attempted_write_count=0`
  - `any_write_command_sent=false`
  - `chamber_set_temperature_command_sent=false`
  - `chamber_start_command_sent=false`
  - `chamber_stop_command_sent=false`
  - `real_primary_latest_refresh=false`
- No pressure point was completed:
  - `pressure_points_expected=7`
  - `pressure_points_completed=0`
  - `points_completed=0`
  - `sample_count_total=0`

## Temperature Chamber Critical-Device Context

- The direct blocker was emitted before A2 route conditioning began, during startup device initialization.
- This A2.12R probe was configured as CO2-only, single route, single temperature, no-write, and skip temperature stabilization wait.
- In that mode, the temperature chamber should provide optional read-only temperature context if available; it should not block CO2 route conditioning when unavailable.
- The chamber must remain critical for multi-temperature, real acceptance, and explicit temperature-control modes.
- A2.13 should split device policy into route/pressure critical devices versus optional context devices, and record `temperature_context_unavailable_reason` rather than adding `temperature_chamber` to critical `failed_devices` for skip-temp engineering probes.

## Safety Boundary

- V1 fallback remains required.
- This is engineering probe evidence only, not real acceptance.
- No A3, H2O, full group, multi-temperature, mode switch, ID/SENCO/calibration coefficient write, chamber SV write, chamber start/stop, or `real_primary_latest` refresh was performed.
- Because the first run failed closed, this turn did not run a second A2.12R probe.
