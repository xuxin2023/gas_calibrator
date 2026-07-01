# V1.5 Historical Replay Missing Point Audit Test Evidence

- date: `2026-07-01`
- cwd: `D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean`
- scope: `historical replay evidence binder + missing-point audit + QC gap audit + replay contract + mature route contract + entrypoint inventory`
- opens_com_ports: `false`
- connects_postgresql: `false`
- controls_water_or_gas_routes: `false`
- writes_coefficients: `false`
- not_real_acceptance_evidence: `true`

## Command

```powershell
python -m pytest tests\test_v1_5_historical_replay_evidence.py tests\test_v1_5_historical_replay_missing_point_audit.py tests\test_v1_5_historical_replay_qc_gap_audit.py tests\test_v1_5_historical_replay_contract.py tests\test_v1_5_mature_route_contract.py tests\test_v1_5_entrypoint_inventory.py -q
```

## Stdout

```text
...................................................                      [100%]
51 passed in 12.98s
```

## Live Missing-Point Audit Output

The generated live audit under `docs\v1_5_flow_contract\historical_replay_missing_point_audit\` is intentionally conservative:

- `status=review_required`
- `blocker_count=0`
- `review_required_count=2`
- `missing_point_count=9`
- `segmented_quality_candidate_count=6`
- `supplemental_unresolved_count=3`
- `unresolved_point_count=3`
- New-algorithm base CO2 low-temperature points `-20C/{0,400,1000}` and `-10C/{0,400,1000}` have segmented quality candidates under the new-algorithm run area and require review before binding.
- Cross-family legacy same-physical-point evidence is preserved as reference-only and is not direct binding evidence for the new-algorithm run.
- New-algorithm supplemental points `-20C/600ppm`, `-10C/600ppm`, and `40C/HGEN30C/30RH` have no candidate evidence and remain targeted supplemental resampling candidates.

This is program-level replay evidence only. It is not real acceptance, archive release, or PostgreSQL 18 database-import evidence.
