# A2.15 RS485 no-response evidence summary for A2.16 crosscheck

- Branch: `codex/run001-a1-no-write-dry-run`
- HEAD at A2.15 diagnostic review: `7f810e0884f8e2a887a934077e5cae751bc4b4d8`
- A2.15 diagnostic output directory: `D:\gas_calibrator_step3a_a2_15_rs485_command_diagnostic_20260430_1616`
- Evidence source: `rs485_command_diagnostic_a2_15_no_write`
- Final decision: `FAIL_CLOSED`
- Fail-closed reason: `a2_15_readonly_rs485_command_diagnostic_no_response_or_parse_failed`

## Current port mapping

- COM24-COM31 are RS485 in the current site wiring.
- Pressure controller: `COM31`
- P3 / pressure gauge: `COM30`
- Relay A / relay B: `COM28` / `COM29`

## Pressure controller no-response context

- `:OUTP:STAT?`
  - Terminator: `LF`
  - Raw request hex: `3A4F5554503A535441543F0A`
  - Raw response: empty
  - Result: `NO_RESPONSE`
- `:SENS:PRES?`
  - Terminator: `LF`
  - Raw request hex: `3A53454E533A505245533F0A`
  - Raw response: empty
  - Result: `NO_RESPONSE`
- `*IDN?`
  - Raw request hex: `2A49444E3F0A`
  - Raw response: empty
  - Result: `unsupported_identity_query`
  - Offline decision: not made from identity query alone.

## P3 no-response context

- Device: pressure gauge / P3
- Port: `COM30`
- dest_id: `01`
- Command: `*0100P3\r\n`
- Raw request hex: `2A3031303050330D0A`
- Raw response: empty
- Timeout: `2.2 s`
- Result: `NO_RESPONSE`
- Parse OK: `false`
- Drain strategy recorded: `v1_aligned_read_pressure_then_fast_read_buffered_drain_on_no_response`

## Relay context

- `COM28` / `COM29` open-close succeeded.
- Relay channel mapping matched V1 config.
- Relay output command sent: `false`
- Relay output command allowed in probe: `false`

## Historical success evidence

- PACE identity success evidence:
  `D:\gas_calibrator\audit\real_pace_controller_acceptance\pace_identity_probe_20260420_123551\pace_identity_probe_20260420_123610.json`
- PACE readback success evidence:
  `D:\gas_calibrator\audit\real_pace_controller_acceptance\pace_readback_probe_20260420_postfix\pace_readback_probe_result.json`
- P3 success evidence:
  `D:\gas_calibrator\logs\atm_hold_probe_20260306_191622\io_20260306_191622.csv`

## A2.16 implication

This is not a serial-port-discovery problem. COM ports open successfully and the configured ports match V1/historical evidence. The next step is current-site same-COM, same-address, same-parameter V1/V2 read-only crosscheck. No A2 route probe should run until pressure controller and P3 read-only responses are restored.
