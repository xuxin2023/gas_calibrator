# V1.5 Offline Next-Step Authorization Preflight

## Purpose

This package adds accountable human review to the exact no-execute next-step
plan produced after a verified offline state advance. It validates permission
to review one immutable plan. It does not authorize or perform that plan.

## Required Inputs

The preflight consumes:

1. the canonical next-step plan JSON;
2. a short-lived authorization packet named
   `v1_5_authoritative_resume_offline_state_advance_next_step_authorization_packet.json`.

The packet must bind all of these values exactly:

- next-step plan path and SHA256;
- consumer-readiness path and SHA256;
- run ID and attempt ID;
- verified step ID and next step ID;
- exact next-step tool module.

## Human Review Contract

The packet requires three distinct identities: operator, reviewer, and
approver. Its lifetime must be positive and no longer than 30 minutes.

The structured confirmation must explicitly accept:

- the exact plan only;
- review only and no execution;
- no COM access;
- no pressure, gas-route, or water-route control;
- no device, identity, or coefficient write;
- no PostgreSQL, release, or import action;
- no change to the mature route implementation.

## Independent Revalidation

The validator independently rebuilds the next-step plan from its recorded
consumer sidecar. A missing, expired, path-mismatched, hash-mismatched,
identity-conflicted, execution-capable, or non-reproducible packet produces
`review_required`.

Formal status repeats the validation and binds the preflight to the detected
next-step plan. The preflight exporter remains an out-of-band support tool and
is forbidden from the canonical full-flow step sequence.

## Locked Boundary

Even when the preflight passes:

- `plan_review_allowed=true`;
- `execution_supported=false`;
- `next_step_execution_allowed=false`;
- `resume_execution_allowed=false`;
- `opens_com_ports=false`;
- `controls_pressure=false`;
- `controls_water_or_gas_routes=false`;
- all identity, coefficient, state, and database writes remain false;
- formal release and database import remain false.

A later executor package must obtain separate physical authority and revalidate
the full evidence chain immediately before any action.

## Verification Evidence

```text
15 next-step plan and authorization-preflight tests passed
13 authorization/status integration tests passed
64 formal-run-status tests passed
34 entrypoint-inventory tests passed
39 formal-flow-contract tests passed
33 full-flow-orchestration tests passed
11 mature-route and next-step-plan tests passed
```

The two observed warnings are existing unregistered `v1_5_formal_gate` pytest
markers. They are unrelated to this package.
