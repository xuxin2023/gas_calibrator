# A2.13 Real Probe FAIL_CLOSED Summary: Pressure Controller Command Error

Generated for A2.14 offline repair planning.

## Context

- Branch: `codex/run001-a1-no-write-dry-run`
- A2.13 commit HEAD: `abff1408f643e8bc1fb120a35c1d78417e97fd96`
- Origin HEAD at run time: `8ffb4bbfe314d35863cc1e9696aa45b2bae86ddf`
- A2.13 output directory:
  `D:\gas_calibrator_step3a_a2_13_v1_aligned_co2_7_pressure_no_write_skip_temp_wait_after_serial_fix_20260430_1417`
- Underlying execution directory:
  `D:\output\run001_a2\co2_only_7_pressure_no_write\run_20260430_142005`

## Summary Fields

- `final_decision=FAIL_CLOSED`
- `fail_closed_reason=a2_pass_conditions_not_met`
- `admission_approved=true`
- `operator_confirmation_valid=true`
- `a2_conditioning_pressure_source_strategy=v1_aligned`
- `pressure_source_strategy_aggregation_mismatch=false`
- `pressure_points_completed=0`
- `points_completed=0`
- `sample_count_total=0`
- `no_write=true`
- `attempted_write_count=0`
- `any_write_command_sent=false`
- `critical_devices_failed=[]`

## Query-Only Sanity Check

- Output directory:
  `D:\gas_calibrator_step3a_a2_13_v1_aligned_co2_7_pressure_no_write_skip_temp_wait_after_serial_fix_20260430_1417\query_only_init_sanity`
- `final_decision=FAIL_CLOSED`
- Opened ports: `COM28`, `COM29`, `COM30`, `COM31`
- No-write held:
  - `attempted_write_count=0`
  - `any_write_command_sent=false`
  - `relay_output_command_sent=false`
  - `pressure_setpoint_command_sent=false`
  - `vent_off_command_sent=false`
  - `seal_command_sent=false`
  - `sample_started=false`

## Pressure Controller Identity Context

- Device key: `pressure_controller`
- Port: `COM31`
- Query-only command attempted: `*IDN?`
- Result: `unavailable`
- Raw response: empty
- Interpretation for A2.14: COM open succeeded, so this is not evidence of a disconnected serial port by itself. A2.14 should determine whether `*IDN?` is V1-aligned or unsupported for the real K0472/PACE path and should use a V1-aligned read-only identity/status ping when available.

## Pressure Gauge / P3 Context

- Device key: `pressure_gauge`
- Port: `COM30`
- Dest ID: `01`
- P3 command preview: `*0100P3\r\n`
- Read methods attempted:
  - `generic_read_frame_raw_capture`
  - `ParoscientificGauge.read_pressure`
  - `ParoscientificGauge.read_pressure_fast`
- Result: `known_v1_driver_readonly_failed`
- Raw bytes length: `0`
- Parser status: `no_raw_bytes`
- P3 error: `NO_RESPONSE`
- Fast read error: `NO_RESPONSE`
- Interpretation for A2.14: COM open succeeded, but no P3 response was parsed. A2.14 should report command, terminator, dest_id, drain behavior, mode assumption, timeout, raw response, and V1/V2 read alignment.

## PACE Command Error Context

Route trace recorded command-layer failures before CO2 route conditioning:

```text
action=set_output target={"enabled": false} result=fail
message=PACE_COMMAND_ERROR(command=:OUTP:STAT 0, error=)
```

```text
action=set_vent target={"vent_on": true} result=fail
message=PACE_COMMAND_ERROR(command=:OUTP:STAT 0, error=)
actual.hard_blockers=[
  "vent_command_method_unavailable",
  "vent_command_failed",
  "output_state_unavailable",
  "isolation_state_unavailable",
  "vent_status_unavailable"
]
```

The underlying `io_log.csv` shows these calls were made through `NoWriteDeviceProxy`, preserving no-write evidence:

```text
NoWriteDeviceProxy,TX,"set_in_limits(0.02, 10.0)"
NoWriteDeviceProxy,TX,"set_valve(1, False)"
NoWriteDeviceProxy,TX,set_output(False)
NoWriteDeviceProxy,TX,set_output(False)
NoWriteDeviceProxy,TX,"set_valve(1, False)"
```

## Boundary Statement

A2.13 did not enter CO2 route conditioning, did not route open, did not reach pressure gate, did not sample, did not enter A3, did not run H2O/full group/multi-temperature, did not write ID/SENCO/calibration coefficients, did not write chamber SV, did not start/stop chamber, and did not refresh `real_primary_latest`.

This is engineering-probe evidence only and is not real acceptance evidence.
