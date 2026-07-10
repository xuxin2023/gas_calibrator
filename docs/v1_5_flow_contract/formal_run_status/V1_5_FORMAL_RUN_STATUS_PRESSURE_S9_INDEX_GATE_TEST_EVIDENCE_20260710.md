# V1.5 Formal Run Status Pressure/S9 Index Gate Test Evidence

Generated: 2026-07-10

## Scope

This evidence covers the offline formal-run-status wiring for the V1.5 pressure/SENCO9 readiness index introduced by PR #68.

The change keeps the legacy `run_evidence_status.pressure_quick_check` fallback, but when a `v1_5_pressure_s9_readiness_index.json` sidecar is present it becomes the authoritative `pressure_senco9_pre_open_flow` gate.

## Focused Test

```text
python -m pytest tests\test_v1_5_formal_run_status.py -q
46 passed in 19.04s
```

## Compatibility Test

```text
python -m pytest tests\test_v1_5_formal_run_status.py tests\test_v1_5_pressure_s9_readiness_index.py tests\test_v1_5_batch_initialization_closeout_index.py tests\test_v1_5_full_flow_next_action_plan.py tests\test_v1_5_entrypoint_inventory.py -q
87 passed, 1 warning in 39.41s
```

The warning is the existing `pytest.mark.v1_5_formal_gate` registration warning.

## Boundary

- opens_com_ports: `false`
- controls_pressure: `false`
- controls_water_or_gas_routes: `false`
- connects_postgresql: `false`
- writes_sn: `false`
- writes_device_id: `false`
- writes_coefficients: `false`
- writes_senco9: `false`
- formal_release_allowed is not unlocked by this evidence alone
- database_import_allowed is not unlocked by this evidence alone
