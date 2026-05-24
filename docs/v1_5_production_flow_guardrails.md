# V1.5 production flow guardrails

This note preserves the V1.5 route and pressure-control lessons from the
2026-05-24 recovery work. It exists so future changes do not rely on chat
history or memory.

## Scope

- Applies to V1.5 production-calibration flow.
- Does not promote V2.
- Does not change `run_app.py`.
- Real COM validation for V1.5 remains operator-authorized, no-write unless
  explicitly approved otherwise.

## Baseline that must be preserved

1. Open-route sampling for H2O and CO2 runs with the PACE kept at atmosphere.
   The atmosphere/VENT hold is part of the V1.5 open-route baseline, not an
   error by itself.
2. Open-route dewpoint stabilization must remain in place. Do not remove or
   bypass the water/gas open-route dewpoint stability logic.
3. Before sealed pressure control, the flow must exit atmosphere first:
   stop continuous atmosphere hold, send VENT off, wait the configured
   1.5 s window, seal/close the route valves, then enable PACE output for
   sealed control.
4. During open-route sampling, PACE queries can disturb atmosphere behavior.
   Prefer route evidence and the digital pressure gauge fallback when judging
   whether the route is physically at atmosphere.
5. Current-atmosphere open-route points are valid production candidates when
   analyzer freshness, dewpoint stability, route state, and no-write audit pass.
6. Sealed pressure points must not be blocked by V2-style idealized gates that
   do not match the old V1.5 field behavior.

## V2-style gates that must not be hard defaults in V1.5

The following checks may be useful as diagnostics, but must not default to
fail-closed in V1.5 production flow:

- hard fresh-atmosphere gate before H2O/CO2 open-route dewpoint sampling;
- analyzer dry-enough violation before gas sampling when the dewpoint trend is
  otherwise acceptable;
- humidity-generator shutdown flow verification or flow set commands during
  safe stop;
- open-flow VENT1 keepalive-gap fail-closed;
- PACE VENT status 3 as a hard sealed-control blocker;
- small sealed-control undershoot when pressure approaches the target from
  above.

## Current code defaults that protect this behavior

- `workflow.pressure.open_flow_vent1_gap_fail_closed_enabled = false`
- `workflow.stability.analyzer_gate_dry_enough_violation_policy = "warn"`
- `workflow.humidity_generator.safe_stop_verify_flow = false`
- `workflow.humidity_generator.safe_stop_enforce_flow_check = false`
- `workflow.pressure.control_ready_allowed_vent_statuses` includes `3`
- `workflow.pressure.exhaust_only_undershoot_fail_closed_enabled = false`
- `workflow.pressure.exhaust_only_target_crossing_fail_closed_enabled = false`
- `workflow.pressure.exhaust_only_undershoot_hard_fail_hpa = 5.0`

Strict behavior may still be enabled deliberately in tests or diagnostic
configs, but it must be opt-in.

## Evidence from 2026-05-24 no-write run

Run:

`v1_5_h2o_co2_900ppm_ambient_1100_no_write_20260524_145656`

Observed:

- H2O current-atmosphere open route completed 10 samples.
- CO2 900 ppm current-atmosphere open route completed 10 samples.
- CO2 open-route dewpoint gate passed after fixed precondition.
- CO2 sealed transition did close atmosphere/route first, then enabled OUTP.
- The CO2 1100 hPa sealed point reached the target region, then failed because
  a small undershoot around 1099 hPa was treated as
  `FAIL_CLOSED_PRESSURE_UNDERSHOOT_EXHAUST_ONLY`.
- H2O 1100 hPa did not stabilize because PACE positive source evidence was only
  about 1009 hPa, which is a source/hardware limitation rather than an
  open-route flow bug.
- No-write audit showed no identity, calibration, or coefficient writes.

## Future-change checklist

Before changing V1.5 route or pressure logic:

1. Compare against the known-good gas-route behavior around commit
   `c97935aeb3fa8172858b591beacb6339e5781fb4`.
2. Keep original V1.5 water-route timing unless a field failure proves it needs
   a narrow fix.
3. Preserve 10 samples per point unless the operator explicitly requests a
   shorter diagnostic run.
4. Do not copy V2 protected-route gates into V1.5 as hard defaults.
5. Run targeted tests for open-route atmosphere, sealed transition, safe stop,
   and pressure undershoot behavior before any real V1.5 smoke run.

Recommended targeted checks:

```powershell
python -m py_compile src\gas_calibrator\workflow\runner.py src\gas_calibrator\config.py
python -m pytest tests\test_safe_stop_tool.py tests\test_config_runtime_defaults.py -q
python -m pytest tests\test_v1_5_controlled_outp_seal_transition.py::test_exhaust_only_small_undershoot_can_continue_until_inlimit -q
python -m pytest tests\test_v1_5_pace_audit_guards.py::test_analyzer_gate_dry_enough_small_overshoot_warns_not_fail -q
```

