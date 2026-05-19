# V1.5 post-route-close delay root cause

Run audited: `logs/v1_5_skip_abort_no_write_limited_2sealed_f600fabc_20260519_001`.

## Observed delay

- `route_close_actual_ts`: 2026-05-19T13:05:07.877
- first `OUTP1`: 2026-05-19T13:07:08.039, about 120.162 s after route close
- first setpoint: 2026-05-19T13:07:13.608, about 125.731 s after route close
- dewpoint was about -34 C at route close and about -13.18 C near control-ready; pressure-only effect was about +1.16 C, so the observed rise remains far beyond pressure effect.

## Blocking chain in V1.5 before the fix

1. `runner.py:20656` records `route_valves_closed_after_vent0`, then the old path records full post-route evidence.
2. `runner.py:11834` `_post_seal_vent_abort_clear_if_needed` runs before control; useful for VENT3, but it is not enough by itself to justify a long passive dwell.
3. `runner.py:11587` `_record_controlled_exit_atmosphere_after_route_close` takes a full controlled-exit snapshot after route close.
4. `runner.py:10347` `_controlled_operator_window_check_after_fixed_preseal_wait` can wait for console/operator input; latest run used the 30 s timeout path.
5. `runner.py:13011` `_ensure_pressure_controller_ready_for_control` can perform another full live ready check before setpoint.
6. `runner.py:11975` `_set_pressure_to_target` previously enabled controlled CO2 OUTP1 before sending the first setpoint.

## Triggering config keys

- `workflow.pressure.require_operator_window_clear_after_vent0`
- `workflow.pressure.operator_window_confirm_mode`
- `workflow.pressure.operator_window_clear_timeout_s`
- `workflow.pressure.post_seal_vent_abort_clear_enabled`
- `workflow.pressure.controlled_output_confirm_timeout_s`
- `workflow.pressure.control_ready_wait_timeout_s`

## Scope and impact

The repaired fast path is limited to controlled OUTP transition with `workflow.collect_only=true` and `workflow.pressure.fail_if_sealed_passive_exceeds_max=true`. It targets the limited no-write V1.5 engineering run and does not change analyzer gate, V2, `run_app.py`, `configs/default_config.json`, or `pace5000.py`.

## Minimal fix point

- `runner.py:11691` adds `_post_route_close_minimal_control_ready` for route-closed/no-VENT1/VENT-not-1-or-3/OUTP0/ISOL1/hard-limit checks.
- `runner.py:20997` takes the fast route-sealed branch and skips long pre-OUTP1 operator/status evidence.
- `runner.py:11975` now sends the first setpoint before OUTP1 for controlled CO2.
- `runner.py:13618` enforces the route-close-to-OUTP1 deadline before enabling output.

This is replay/suite evidence only, not real acceptance.
