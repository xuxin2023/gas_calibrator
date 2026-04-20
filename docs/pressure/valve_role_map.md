# Valve Role Map

This note records the current logical role map used by the guarded route-open logic. It is based on `configs/default_config.json` plus the role builder in `runner.py`. It is not a full pneumatic truth table for every physical hose or tee.

When a managed valve does not have an explicit role entry, the code records it as `unknown` and the route-open guard may abort with `UnknownValveRole` instead of pretending the role is known.

## Requested valves

| Valve | Logical role | Source in config/code | Physical relay mapping | Notes |
| --- | --- | --- | --- | --- |
| `4` | `co2_source_600ppm_group1` | `co2_map["600"]` | `relay` channel `10` | Pressure-source selector for group 1. Gas identity is inferred from config, not independently bench-proved here. |
| `7` | `co2_path_group1` | `valves.co2_path` | `relay` channel `15` | Group 1 path valve from source selector toward the common gas route. |
| `8` | `h2o_path` | `valves.h2o_path` | `relay_8` channel `8` | Common downstream valve seen in the guarded CO2 route path as well. |
| `11` | `gas_main` | `valves.gas_main` | `relay_8` channel `3` | Common gas manifold valve. |
| `16` | `co2_path_group2` | `valves.co2_path_group2` | `relay` channel `16` | Group 2 path valve from source selector toward the common gas route. |
| `24` | `co2_source_500ppm_group2` | `co2_map_group2["500"]` | `relay` channel `3` | Pressure-source selector for group 2. Gas identity is inferred from config, not independently bench-proved here. |

## Guard staging priority

The current logical staging order is:

- priority `10`: shared downstream / common valves such as `8` and `11`
- priority `20`: route path valves such as `7` and `16`
- priority `30`: source-select valves such as `4` and `24`

This lets the route-open guard observe pressure after each staged addition instead of opening every valve at once.

## Known limits

- This file is a logical role map, not a complete hardware acceptance document.
- Any valve not explicitly registered in config or the role builder should be treated as `unknown`.
- The exact root-cause valve inside a multi-valve route rise still needs live isolation evidence when more than one valve is opened together.
