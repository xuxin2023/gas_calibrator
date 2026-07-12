# V1.5 Authoritative Resume Offline State-Advance Next-Step Plan

## Purpose

This offline reviewer consumes the verified consumer-readiness artifact produced
after one authorized offline state advance. It exposes the exact next canonical
V1.5 step for review without executing that step or advancing state again.

The reviewer closes the gap after PR #110: formal status can prove that the new
state is safe to consume, while this artifact explains what that state points to.

## Evidence Chain

The reviewer independently binds and recomputes:

1. offline state-advance consumer readiness;
2. post-write verification and its source writer evidence;
3. the canonical full-flow plan and SHA256;
4. the authoritative state and SHA256;
5. the exact contiguous completed prefix and next step;
6. the next step tool module, command, execution mode, and authorization needs.

If the next step is a mature physical route, the reviewer additionally requires:

- `co2_open_flow_sampling` to use
  `gas_calibrator.tools.run_v1_5_formal_co2_open_flow_queue`;
- `h2o_open_flow_sampling` to use
  `gas_calibrator.tools.run_v1_5_formal_h2o_open_flow_queue`;
- the route step to remain `real_com_route_requires_authorization`;
- the correct COM and gas/water route-control boundaries.
- runtime identity-bound configuration, mature temperature order, ratio policy,
  and the existing CO2/H2O forbidden-flag contract.

This preserves the original 0613 fitting and 0620/0621 mature route baseline.
It does not authorize a migrated root runner, 0624 evidence path, diagnostic
entrypoint, sampling worker, V1/V2 entrypoint, or `_handoff` artifact.

## Safety Boundary

A ready artifact means only that the next-step plan may be reviewed:

- `plan_consumption_allowed=true`;
- `next_step_execution_allowed=false`;
- `resume_execution_allowed=false`;
- `would_execute=false`.

The exporter does not open COM, control pressure, control gas or water routes,
write authoritative state, write SN/device ID, write coefficients, connect to
PostgreSQL, release evidence, or import data.

The writer, post-write verifier, consumer readiness gate, and this preview all
remain out-of-band evidence. None may become a canonical `completed_step_id`.

## Command

```powershell
python -m gas_calibrator.tools.export_v1_5_authoritative_resume_offline_state_advance_next_step_plan `
  --consumer-readiness-json <v1_5_authoritative_resume_offline_state_advance_consumer_readiness.json> `
  --output-dir <output-dir> `
  --fail-on-blocker
```

## Result

The output is
`v1_5_authoritative_resume_offline_state_advance_next_step_plan.json`.
It is offline review evidence, not real acceptance and not route authorization.
