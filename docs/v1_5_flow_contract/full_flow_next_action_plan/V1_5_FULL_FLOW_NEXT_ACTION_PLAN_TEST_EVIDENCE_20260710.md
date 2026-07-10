# V1.5 Full-Flow Next-Action Plan Test Evidence

Date: 2026-07-10

Scope:

- Add an offline next-action plan derived from the V1.5 full-flow automation closure map.
- Rank remaining automation handoffs without opening COM, controlling gas/water routes, connecting PostgreSQL, writing coefficients, or unlocking formal release.
- Refresh the planner after the batch closeout and pressure/S9 readiness packages so reviewed completed action ids can advance the recommendation to route physical recovery.
- Keep default behavior conservative: without explicit completed-action evidence, the first recommendation remains batch initialization closeout.
- Keep the mature baseline explicit: 0613 fitting and 0620/0621 clean-worktree mature physical route behavior.
- Keep legacy route point counts at CO2 45 / H2O 13 and new-algorithm profile counts at CO2 47 / H2O 14.

Focused command:

```powershell
python -m pytest tests\test_v1_5_full_flow_next_action_plan.py -q
```

Focused result:

```text
6 passed in 1.74s
```

Compatibility command:

```powershell
python -m pytest `
  tests\test_v1_5_full_flow_next_action_plan.py `
  tests\test_v1_5_formal_run_status.py `
  tests\test_v1_5_pressure_s9_readiness_index.py `
  tests\test_v1_5_batch_initialization_closeout_index.py `
  tests\test_v1_5_entrypoint_inventory.py `
  -q
```

Compatibility result:

```text
89 passed, 1 warning in 25.66s
```

Post-#69 behavior checked:

- With no completed-action ids, `recommended_next_action_id=batch_initialization_closeout_pre_gas_evidence_index`.
- With `batch_initialization_closeout_pre_gas_evidence_index` and `pressure_s9_exception_and_reverify_evidence_index` supplied as completed ids, `recommended_next_action_id=route_physical_recovery_live_smoke_binding_contract`.
- Unknown completed-action ids force `overall_status=review_required`.

Boundary:

- `full_production_auto_allowed=false`
- `opens_com_ports=false`
- `controls_water_or_gas_routes=false`
- `connects_postgresql=false`
- `writes_coefficients=false`
- `formal_release_allowed=false`
- `database_import_allowed=false`
- mature CO2/H2O runner logic is not part of this change
