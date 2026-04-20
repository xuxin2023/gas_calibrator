# OLD_PACE5000 Control Notes

This note records the current V1 engineering facts for the live device seen on 2026-04-20. It is for implementation and review only. It is not, by itself, a real acceptance sign-off.

## Live device facts

- Detected profile: `OLD_PACE5000`
- `*IDN?`: `GE Druck,Pace5000 User Interface,3213201,02.00.07`
- Available ranges include `"1.00barg"`, `"BAROMETER"`, and `"2.00bara"`
- The current engineering unit in use is `HPA`

## Formal control path

Formal setpoint control must use this chain:

1. `:OUTP:STAT 1`
2. `:SOUR:PRES <target>`
3. `:SOUR:PRES?`
4. `:SENS:PRES:INL?`

The formal flow must not depend on `:SENS:PRES:CONT?` for this device.

## Unsupported query handling

On the live `OLD_PACE5000` unit, `:SENS:PRES:CONT?` is field-unsupported. The observed failure mode is a blank response followed by `-113` in `:SYST:ERR?`.

Driver behavior in this branch therefore does two things:

- it treats `:SENS:PRES:CONT?` as unsupported for `OLD_PACE5000`
- it prefers `:SENS:PRES:INL?` / other safe readback fallbacks for pressure confirmation

The driver also exposes system-error reading and error-queue draining so unsupported optional probes do not poison later formal commands.

## VENT semantics

For `OLD_PACE5000`, `VENT?=2` is treated only as a fresh-vent-completed observation. It must not be interpreted as a persistent "still open to atmosphere" state.

That means:

- `VENT?=2` can help prove a fresh vent action completed
- `VENT?=2` cannot, by itself, authorize later closed-loop pressure control
- formal control-ready checks must still prove the correct control path state directly

## Safety consequences

- Formal setpoint hold must not rely on periodic `VENT` refresh.
- Abort and cleanup paths should prefer verified safe-stop helpers over ad hoc raw commands.
- Unsupported optional capability probes must fail closed and leave the formal path usable.
