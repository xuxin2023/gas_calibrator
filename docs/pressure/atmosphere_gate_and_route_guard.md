# Atmosphere Gate And Route Guard

This branch adds two separate protections for the V1 formal pressure path after live evidence showed `route_open` could push the line from roughly `1012 hPa` to roughly `1420 hPa`.

## Why this exists

The pressure rise observed after route opening means "fresh vent completed" is not enough to prove the route is still safe. We therefore need both:

- an atmosphere verification step before dewpoint stabilization
- a route-open pressure guard during valve staging

## Guard split

### `AtmosphereGate`

`AtmosphereGate` verifies that the route is at atmosphere after the fresh vent action. It uses pressure near ambient and rising-pressure checks before allowing dewpoint gating to continue.

### `RoutePressureGuard`

`RoutePressureGuard` runs while the route is being opened. It stages valves by role priority and aborts if opening the route causes an unsafe pressure rise or other pressure-side warning.

### Flush pressure guard

After the route is open and dewpoint gating has begun, the code still keeps checking for pressure rise during the open-route flush window. This is a separate guard from the route-open guard.

## Execution order

For the guarded CO2/H2O route path, the intended order is:

1. fresh vent / atmosphere entry
2. `_open_route_with_pressure_guard(...)`
3. `_run_route_open_pressure_guard(...)` during route staging
4. `route_open`
5. soak
6. dewpoint gate begin
7. `_require_atmosphere_gate_before_dewpoint(...)`
8. `_check_flush_pressure_guard(...)` during the dewpoint gate loop

So the route-open guard is before soak and before dewpoint stabilization. The flush pressure guard is inside the dewpoint-gate loop.

## Things this branch intentionally does not do

- no periodic `VENT` refresh during formal pressure setpoint hold
- no formal dependence on `:SENS:PRES:CONT?`
- no claim that `VENT?=2` means the route is continuously open to atmosphere

## Trace expectations

Pressure trace rows for this work should include route-open guard stages such as:

- `route_open_pressure_guard_begin`
- `route_open_pressure_guard_sample`
- `route_open_pressure_guard_end`

Abort paths should also preserve the reason in runtime state and trace rows so live review can distinguish:

- atmosphere gate failure
- route-open guard failure
- pressure rise during flush / dewpoint gate
