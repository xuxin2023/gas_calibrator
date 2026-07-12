# V1.5 Offline State-Advance Next-Step Plan Status Integration

## Purpose

This package wires the offline next-step plan preview into the V1.5 formal-run
status rollup. It answers one narrow question: does the existing verified
offline state identify exactly one reviewable next canonical step?

It does not execute that step.

## Evidence Chain

The status gate requires the exact chain below:

1. offline state-advance atomic writer evidence;
2. independently recomputed post-write verification;
3. hash-bound consumer readiness;
4. hash-bound and independently recomputed next-step plan preview.

The next-step plan must preserve all of these locked values:

- `execution_supported=false`
- `next_step_execution_allowed=false`
- `resume_execution_allowed=false`
- `would_execute=false`
- `opens_com_ports=false`
- `controls_pressure=false`
- `controls_water_or_gas_routes=false`
- `writes_authoritative_state=false`
- `writes_sn=false`
- `writes_device_id=false`
- `writes_coefficients=false`
- `connects_postgresql=false`
- `formal_release_allowed=false`
- `database_import_allowed=false`

Missing, modified, path-mismatched, hash-mismatched, execution-capable, or
independently non-reproducible evidence blocks physical continuation.

## Canonical Boundary

The next-step plan remains an out-of-band review artifact. The full-flow plan
passes its expected path only to `formal_run_status_snapshot`; it does not add a
new canonical step and does not invoke the preview exporter.

When the reviewed next step is CO2 or H2O, the preview remains responsible for
proving the exact mature V1.5 queue module and command contract. Formal status
recomputes that preview instead of trusting its ready flag.

## Safety Boundary

This integration:

- opens no COM ports;
- controls no pressure, gas route, or water route;
- writes no analyzer identity or coefficients;
- connects to no PostgreSQL instance;
- grants no release, import, resume, or next-step execution authority;
- does not modify the mature CO2/H2O queue or shared sampling implementation;
- is not real acceptance evidence.

## Verification

Focused evidence-chain tests:

```text
22 passed in 35.29s
```

Regression evidence:

```text
64 formal-run-status tests passed in 81.86s
39 formal-flow-contract tests passed in 96.49s
5 mature-route-contract tests passed in 6.08s
33 full-flow-orchestration tests passed in 71.28s
```

The single warning is the existing unregistered `v1_5_formal_gate` pytest
marker and is unrelated to this package.
