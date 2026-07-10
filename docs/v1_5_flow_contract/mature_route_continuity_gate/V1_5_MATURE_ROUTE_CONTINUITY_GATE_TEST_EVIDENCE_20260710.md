# V1.5 Mature Route Continuity Gate Test Evidence

Date: 2026-07-10

Scope:

- Add an offline gate that reviews whether one mature V1.5 CO2/H2O `queue_manifest.csv` can be treated as fresh continuous route-run evidence.
- Keep the production physical baseline pinned to the 0613 fitting path plus the 0620/0621 clean-worktree mature route path.
- Block empty manifests, incomplete point counts, failed/running/aborted points, duplicate points, segmented/direct/retry/recovery lineage, `_handoff`, 0624/migration, diagnostic, and worker-surface evidence from formal fit eligibility.
- Consume an optional route root-cause audit; any blocker or unresolved review keeps the route manifest out of formal fitting.

Focused command:

```powershell
python -m pytest tests\test_v1_5_mature_route_continuity_gate.py -q
```

Focused result:

```text
8 passed in 2.67s
```

Compatibility command:

```powershell
python -m pytest tests\test_v1_5_mature_route_continuity_gate.py tests\test_v1_5_route_run_failure_root_cause.py tests\test_v1_5_route_physical_recovery_readiness.py tests\test_v1_5_full_flow_next_action_plan.py tests\test_v1_5_entrypoint_inventory.py -q
```

Compatibility result:

```text
54 passed, 1 warning in 17.72s
```

Boundary:

- `opens_com_ports=false`
- `controls_pressure=false`
- `controls_water_or_gas_routes=false`
- `connects_postgresql=false`
- `writes_coefficients=false`
- `formal_release_allowed=false`
- `database_import_allowed=false`
- `not_real_acceptance_evidence=true`
- mature CO2/H2O queue and sampling runner logic is not part of this change

Physical meaning:

- A clean 45-point CO2 manifest or 13-point H2O manifest is only eligible to feed the next offline fit-input review when it is complete, continuous, and free of segmented/manual recovery lineage.
- Physical recovery evidence and root-cause audit evidence remain separate from the route manifest itself; they must not turn a failed or segmented route into formal continuous-run evidence by implication.
