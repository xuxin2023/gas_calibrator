# V1.5 formal initialization read-only COM preflight blocked executor test evidence

Generated on 2026-07-03 for the V1.5 initialization read-only COM preflight blocked executor package.

## Scope

This evidence covers the no-COM/no-write blocked executor stub that sits after the read-only COM preflight design review and before any future analyzer contact.

The package keeps these boundaries locked:

- `opens_com_ports=false`
- `read_only_real_com_execution_allowed=false`
- `controlled_write_execution_allowed=false`
- `writes_sn=false`
- `writes_device_id=false`
- `writes_coefficients=false`
- `connects_postgresql=false`
- `controls_pressure=false`
- `controls_water_or_gas_routes=false`
- `not_real_acceptance_evidence=true`

## Focused pytest

Command:

```powershell
& 'C:\Users\A\AppData\Local\Programs\Python\Python313\python.exe' -m pytest tests\test_v1_5_formal_initialization_readonly_com_preflight_blocked_executor.py tests\test_v1_5_formal_initialization_readonly_com_preflight_design.py tests\test_v1_5_full_flow_orchestration.py tests\test_v1_5_formal_flow_contract.py tests\test_v1_5_formal_run_status.py tests\test_v1_5_entrypoint_inventory.py -q
```

Result:

```text
116 passed, 2 warnings in 74.85s (0:01:14)
```

The two warnings are existing unregistered `pytest.mark.v1_5_formal_gate` warnings in focused guard tests. They do not indicate a failed assertion.

## Conclusion

The blocked executor remains a review-only stub. It consumes the read-only COM preflight design artifact, rejects live/authorization unlock flags, and does not open COM, write identity, write SENCO, connect PostgreSQL, or control pressure/routes.
