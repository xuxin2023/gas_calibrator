# V1.5 final offline acceptance suite test evidence

- Date: `2026-07-13`
- Source `origin/main`: `9125fcda192cd9b02938a92fae267e7993d37833`
- Result: `325 passed, 2 warnings in 399.25s`
- Suite status: `offline_program_acceptance_passed_real_acceptance_blocked`
- Artifact contracts: `11/11 pass`
- Allowlisted test files: `23`

## Command

```powershell
$env:PYTHONPATH='src'
python -m gas_calibrator.tools.run_v1_5_final_offline_acceptance_suite `
  --repository-root . `
  --source-origin-main-commit 9125fcda192cd9b02938a92fae267e7993d37833 `
  --output-dir docs/v1_5_flow_contract/final_offline_acceptance_suite `
  --timeout-s 1800
```

## Regression closure

The first complete run exposed V2 service-extraction host-contract gaps in the
simulation smoke suite. The narrow V2-only repair restored the H2O path bridge,
CO2 preseal pressure guard, analyzer snapshot hook, and simulation-only
next-temperature precondition hooks. The final allowlist includes the focused
orchestrator regression tests so these gaps cannot silently return.

## Safety boundary

This evidence is program-level and offline only:

- `not_real_acceptance_evidence=true`
- `opens_com_ports=false`
- `controls_pressure=false`
- `controls_water_or_gas_routes=false`
- `writes_coefficients=false`
- `connects_postgresql=false`
- `database_written=false`
- `live_queue_execution_allowed=false`
- `formal_release_allowed=false`
- `database_import_allowed=false`

The two warnings are pre-existing unregistered `v1_5_formal_gate` pytest marks.
They do not change the pass result or any safety lock.
