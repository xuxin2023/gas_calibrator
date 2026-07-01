# V1.5 Historical Replay Contract Test Evidence

- date: `2026-07-01`
- cwd: `D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean`
- scope: `historical replay contract + mature route contract + entrypoint inventory`
- opens_com_ports: `false`
- connects_postgresql: `false`
- controls_water_or_gas_routes: `false`
- writes_coefficients: `false`
- not_real_acceptance_evidence: `true`

## Command

```powershell
python -m pytest tests\test_v1_5_historical_replay_contract.py tests\test_v1_5_mature_route_contract.py tests\test_v1_5_entrypoint_inventory.py -q
```

## Stdout

```text
.......................................                                  [100%]
39 passed in 6.65s
```

## Coverage Meaning

- Confirms historical replay is offline program-level evidence only.
- Confirms legacy replay keeps mature ratio `R` inputs and the 0620 `45/13` point contract.
- Confirms new algorithm replay stays in the absorption `A=-ln(R/R0(T))/(P_kPa/100)` and R0 evidence layer.
- Confirms rejected/QC-failed historical rows remain non-fit evidence.
- Confirms replay pass does not authorize archive release or PostgreSQL 18 database import.
- Confirms the replay exporter is classified as an offline formal-review support tool, not a physical runner.
