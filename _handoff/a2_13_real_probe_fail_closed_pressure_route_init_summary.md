# A2.13 real probe FAIL_CLOSED - pressure/route device initialization

Date: 2026-04-30

Branch: `codex/run001-a1-no-write-dry-run`
HEAD: `abff1408f643e8bc1fb120a35c1d78417e97fd96`
Origin HEAD: `8ffb4bbfe314d35863cc1e9696aa45b2bae86ddf`

Wrapper output directory:
`D:\gas_calibrator_step3a_a2_13_v1_aligned_co2_7_pressure_no_write_skip_temp_wait_20260430_1316`

Downstream execution directory:
`D:\gas_calibrator_step3a_a2_13_v1_aligned_co2_7_pressure_no_write_skip_temp_wait_20260430_1316\downstream_run\run_20260430_131720`

Wrapper command was executed once with `--execute-probe`.

Result:
- `final_decision=FAIL_CLOSED`
- `fail_closed_reason=a2_pass_conditions_not_met`
- `artifact_completeness_pass=true`
- `required_artifacts_missing=[]`
- `interrupted_execution=false`
- `no_write_assertion_status=pass`
- `no_write=true`
- `attempted_write_count=0`
- `any_write_command_sent=false`
- `real_primary_latest_refresh=false`
- `pressure_points_completed=0`
- `points_completed=0`
- `sample_count_total=0`

Direct downstream blocker:
`Critical device initialization failed [failed_devices=['pressure_controller', 'pressure_meter', 'relay_a', 'relay_b'], critical_devices_failed=['pressure_controller', 'pressure_meter', 'relay_a', 'relay_b'], optional_context_devices_failed=['temperature_chamber'], critical_device_init_failure_blocks_probe=True]`

A2.13 temperature policy result:
- The previous temperature-only blocker was removed.
- `temperature_chamber` is no longer a critical blocker in this skip-temp A2 engineering probe.
- Downstream error context records `optional_context_devices_failed=['temperature_chamber']`.
- Blocking devices are A2 route/pressure critical devices, not the temperature chamber.

No-write evidence:
- `attempted_write_count=0`
- `any_write_command_sent=false`
- `identity_write_command_sent=false`
- `mode_switch_command_sent=false`
- `senco_write_command_sent=false`
- `calibration_write_command_sent=false`
- `chamber_write_register_command_sent=false`
- `chamber_set_temperature_command_sent=false`
- `chamber_start_command_sent=false`
- `chamber_stop_command_sent=false`

Conclusion:
This is not A2.13 PASS and not real acceptance. It is an engineering probe FAIL_CLOSED caused before route conditioning by route/pressure critical device initialization failures. Do not rerun in the same round. Next work must stay in A2 and investigate real COM/device initialization for pressure controller, pressure gauge, and relay devices, plus wrapper surfacing of downstream optional-context device lists.
