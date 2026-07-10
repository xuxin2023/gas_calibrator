# V1.5 Mature Route Continuity Status Gate Test Evidence

Date: 2026-07-10

Scope:

- Wire the mature route continuity gate into the offline V1.5 formal run status rollup.
- Keep the gate offline: no COM, no pressure control, no gas/water route control, no PostgreSQL, no SN/device-code write, and no SENCO write.
- Require a passing mature-route continuity sidecar before CO2/H2O run evidence can be treated as formal release / fit-input eligible.
- Missing or blocked continuity evidence blocks formal release and database import, but does not block continued physical recovery work.

Focused command:

```powershell
python -m pytest tests\test_v1_5_formal_run_status.py -q
```

Focused result:

```text
48 passed in 21.36s
```

Compatibility command:

```powershell
python -m pytest tests\test_v1_5_formal_run_status.py tests\test_v1_5_mature_route_continuity_gate.py tests\test_v1_5_full_flow_orchestration.py tests\test_v1_5_entrypoint_inventory.py -q
```

Compatibility result:

```text
115 passed, 1 warning in 161.92s
```

Boundary:

- `opens_com_ports=false`
- `controls_pressure=false`
- `controls_water_or_gas_routes=false`
- `connects_postgresql=false`
- `writes_coefficients=false`
- `writes_sn_or_device_code=false`
- `formal_release_allowed=false` unless continuity and downstream archive gates are ready
- `database_import_allowed=false`
- mature CO2/H2O queue and sampling runner logic is not part of this change

Physical meaning:

- Route physical recovery says whether the plant-side route problems are understood and recoverable.
- Mature route continuity says whether the actual CO2/H2O manifest is a fresh, complete, continuous 0613/0620/0621 mature run.
- Segmented, retry, direct-recovery, 0624/migration, diagnostic, worker, empty, running, or failed evidence remains diagnostic/recovery evidence and cannot feed formal fitting by implication.
