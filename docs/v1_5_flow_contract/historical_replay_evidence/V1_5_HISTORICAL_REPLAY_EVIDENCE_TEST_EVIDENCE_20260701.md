# V1.5 Historical Replay Evidence Test Evidence

- date: `2026-07-01`
- cwd: `D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean`
- scope: `historical replay evidence binder + replay contract + mature route contract + entrypoint inventory`
- opens_com_ports: `false`
- connects_postgresql: `false`
- controls_water_or_gas_routes: `false`
- writes_coefficients: `false`
- not_real_acceptance_evidence: `true`

## Command

```powershell
python -m pytest tests\test_v1_5_historical_replay_evidence.py tests\test_v1_5_historical_replay_contract.py tests\test_v1_5_mature_route_contract.py tests\test_v1_5_entrypoint_inventory.py -q
```

## Stdout

```text
.............................................                            [100%]
45 passed in 8.56s
```

## Live Evidence Binding Output

The generated live binding under `docs\v1_5_flow_contract\historical_replay_evidence\` is intentionally conservative:

- `status=blocked`
- `blocker_count=1`
- `review_required_count=1`
- Mature 0620 legacy CO2 evidence matched `45/45` points, while preserving rejected analyzer rows.
- New-algorithm CO2 evidence matched `39/45` points, so the missing segment remains review-required.
- New-algorithm H2O evidence matched `13/13` wet points.
- Missing point-level QC evidence remains a blocker for fit-input promotion.

This is program-level replay evidence only. It is not real acceptance, archive release, or PostgreSQL 18 database-import evidence.
