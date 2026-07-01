# V1.5 Historical Replay QC Gap Audit Test Evidence

- date: `2026-07-01`
- cwd: `D:\gas_calibrator\_worktrees\v1_5_fixed_wait_window_gate_1aee26d_clean`
- scope: `historical replay evidence binder + QC gap audit + replay contract + mature route contract + entrypoint inventory`
- opens_com_ports: `false`
- connects_postgresql: `false`
- controls_water_or_gas_routes: `false`
- writes_coefficients: `false`
- not_real_acceptance_evidence: `true`

## Command

```powershell
python -m pytest tests\test_v1_5_historical_replay_evidence.py tests\test_v1_5_historical_replay_qc_gap_audit.py tests\test_v1_5_historical_replay_contract.py tests\test_v1_5_mature_route_contract.py tests\test_v1_5_entrypoint_inventory.py -q
```

## Stdout

```text
.................................................                        [100%]
49 passed in 11.29s
```

## Live QC Gap Audit Output

The generated live audit under `docs\v1_5_flow_contract\historical_replay_qc_gap_audit\` is intentionally conservative:

- `status=review_required`
- `blocker_count=0`
- `review_required_count=1`
- `missing_qc_point_count=3`
- `direct_bindable_point_count=2`
- `unresolved_point_count=1`
- `p040_Tm10_0ppm_fit`: same-run `queue_manifest_with_quality.csv` has `C_reject`; bind as traceability/reject-only evidence, not fit evidence.
- `p041_Tm10_400ppm_fit`: same-run `queue_manifest_with_quality.csv` has `C_reject`; bind as traceability/reject-only evidence, not fit evidence.
- `p017_T20_200ppm_fit`: only raw IO plus cross-run same-point quality reference was found; keep review-required until same-run QC derivation, retry evidence, or targeted rerun exists.

This is program-level replay evidence only. It is not real acceptance, archive release, or PostgreSQL 18 database-import evidence.
