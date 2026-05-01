# A2.14 RS485 Command Diagnostic FAIL_CLOSED Summary

- Output directory: `D:\gas_calibrator_step3a_a2_14_rs485_command_diagnostic_20260430_1511`
- A2.14 code HEAD: `165d75aad0a6fc41006f45517276aa695fa46b0b`
- Branch: `codex/run001-a1-no-write-dry-run`
- Origin HEAD at start of A2.15 follow-up: `8ffb4bbfe314d35863cc1e9696aa45b2bae86ddf`
- Evidence source: `rs485_command_diagnostic_a2_14_no_write`
- Acceptance level: `engineering_probe_only`
- Not real acceptance: `true`
- Promotion state: `blocked`

## Decision

- `final_decision=FAIL_CLOSED`
- `fail_closed_reason=pressure_controller_identity_query_unsupported;pressure_controller_v1_aligned_readonly_ping_no_response;pressure_meter_p3_no_response_or_parse_failed`
- `command_profile_mismatch=true`
- `command_profile_mismatch_reason=pressure_controller_identity_query_unsupported;pressure_controller_v1_aligned_readonly_ping_no_response;pressure_meter_p3_no_response_or_parse_failed`

## Pressure Controller Context

- Port opened: `COM31`
- Protocol profile: `pace5000_scpi_v1_aligned_readonly`
- `pressure_controller_identity_query_result=unsupported_identity_query`
- `pressure_controller_identity_query_unsupported=true`
- `pressure_controller_v1_aligned_readonly_ping_result=no_response`
- `v1_v2_pressure_controller_command_alignment=code_profile_aligned_but_v1_aligned_readonly_ping_no_response`
- `pressure_controller_pace_command_error_raw=not_executed_in_readonly_diagnostic`
- Previous A2.13 context: `PACE_COMMAND_ERROR(command=:OUTP:STAT 0, error=)` before CO2 route open.

Interpretation: COM open succeeded, so this is not evidence of a disconnected serial port. The observed failure is at RS485 command/protocol/address/timing alignment.

## Pressure Meter / P3 Context

- Port opened: `COM30`
- `pressure_meter_alias_resolved=true`
- `pressure_meter_selected_device_key=pressure_gauge`
- `pressure_meter_dest_id=01`
- `pressure_meter_first_read_result=NO_RESPONSE`
- `pressure_meter_raw_response=""`
- `v1_v2_pressure_meter_read_alignment=code_profile_aligned_but_p3_no_response`

Interpretation: the P3 single-read profile was attempted with the configured destination id, but no response bytes were captured.

## Relay Context

- `COM28` open/close succeeded.
- `COM29` open/close succeeded.
- `relay_init_open_result={"relay": true, "relay_8": true}`
- `relay_output_command_sent=false`

Interpretation: relay sanity remained inside the allowed open/close boundary. It did not prove coil/channel output behavior, and no relay output was sent.

## No-Write / No-Route Assertions

- `attempted_write_count=0`
- `any_write_command_sent=false`
- `route_open_command_sent=false`
- `relay_output_command_sent=false`
- `vent_off_command_sent=false`
- `seal_command_sent=false`
- `pressure_setpoint_command_sent=false`
- `sample_started=false`
- `real_primary_latest_refresh=false`
- No A3, H2O, full group, or multi-temperature action was performed.
