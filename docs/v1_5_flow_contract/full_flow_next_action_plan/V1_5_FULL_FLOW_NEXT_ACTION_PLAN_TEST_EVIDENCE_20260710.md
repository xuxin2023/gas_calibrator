# V1.5 Full-Flow Next-Action Plan Test Evidence

Date: 2026-07-10

Scope:

- Add an offline next-action plan derived from the V1.5 full-flow automation closure map.
- Rank remaining automation handoffs without opening COM, controlling gas/water routes, connecting PostgreSQL, writing coefficients, or unlocking formal release.
- Keep the mature baseline explicit: 0613 fitting and 0620/0621 clean-worktree mature physical route behavior.
- Keep legacy route point counts at CO2 45 / H2O 13 and new-algorithm profile counts at CO2 47 / H2O 14.

Focused command:

```powershell
python -m pytest tests\test_v1_5_full_flow_next_action_plan.py -q
```

Focused result:

```text
4 passed in 2.94s
```

Compatibility command:

```powershell
python -m pytest `
  tests\test_v1_5_full_flow_next_action_plan.py `
  tests\test_v1_5_full_flow_automation_closure.py `
  tests\test_v1_5_formal_run_status.py `
  tests\test_v1_5_entrypoint_inventory.py `
  tests\test_v1_5_production_entrypoint_gate.py `
  -q
```

Compatibility result:

```text
85 passed, 1 warning in 25.82s
```

Boundary:

- `full_production_auto_allowed=false`
- `opens_com_ports=false`
- `controls_water_or_gas_routes=false`
- `connects_postgresql=false`
- `writes_coefficients=false`
- `formal_release_allowed=false`
- `database_import_allowed=false`
- mature CO2/H2O runner logic is not part of this change
