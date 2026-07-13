# V1.5 New-Algorithm Mature-Queue Live-Handoff Test Evidence

This evidence covers the offline 47/14 handoff contract and blocked executor. It is not real acceptance evidence and does not authorize COM, gas/water routes, coefficient writes, PostgreSQL, release, or import.

## Focused contract and compatibility suite

```text
python -m pytest tests\test_v1_5_new_algorithm_mature_queue_live_handoff.py tests\test_v1_5_algorithm_mature_queue_inputs.py tests\test_v1_5_algorithm_queue_handoff_preflight.py tests\test_v1_5_algorithm_profile_lineage_gate.py tests\test_v1_5_algorithm_formal_point_plan_guard.py tests\test_v1_5_algorithm_formal_runlist_preview.py tests\test_v1_5_algorithm_route_profiles.py tests\test_v1_5_mature_route_contract.py tests\test_v1_5_entrypoint_inventory.py -q

95 passed, 1 warning in 25.93s
```

## Full-flow and production-boundary compatibility suite

```text
python -m pytest tests\test_v1_5_final_production_gap_freeze.py tests\test_v1_5_legacy_full_flow_offline_replay.py tests\test_v1_5_production_entrypoint_gate.py tests\test_v1_5_mature_route_continuity_gate.py tests\test_v1_5_unified_controlled_write_reverify.py tests\test_v1_5_full_flow_orchestration.py tests\test_v1_5_formal_flow_contract.py tests\test_v1_5_formal_run_status.py tests\test_v1_5_algorithm_profile_runner_dry_run.py tests\test_v1_5_algorithm_runner_integration_dry_run.py tests\test_v1_5_algorithm_runlist_readiness.py -q

181 passed, 1 warning in 171.67s
```

## Final focused rerun after portable-path fix

```text
python -m pytest tests\test_v1_5_new_algorithm_mature_queue_live_handoff.py -q

22 passed in 6.34s
```

The warnings are the existing unregistered `v1_5_formal_gate` pytest marker. No test failed.

## Verified boundaries

- Legacy production remains the default `45 CO2 / 13 H2O` profile.
- The new algorithm is exactly `47 CO2 / 14 H2O`, with three supplemental points inside their mature temperature segments.
- Both profiles bind the same mature CO2/H2O queue runners and protected point workers.
- The new algorithm changes fit input to `A=-ln(R/R0(T))/(P_kPa/100)`; pressure/S9 remains first and S7/S8 remain neutral.
- CO2 zero gas and the H2O dry/low-water anchor remain separate evidence roles.
- The blocked executor rejects live, COM, authorization, device-list, queue, route, write, and database inputs.
- `live_queue_execution_allowed=false`, `formal_release_allowed=false`, and `database_import_allowed=false` remain locked.
