# V1.5 SENCOA/SENCOB Write Lock Test Evidence - 2026-07-03

## Scope

This evidence covers the V1.5 SENCOA/SENCOB controlled-writer preflight CLI lock.

The preflight command remains no-COM and no-write. It now rejects future real-write
options such as `--execute-controlled-writes`, COM/target selectors, coefficient-write
flags, and authorization/confirmation metadata. A real SENCOA/SENCOB writer still
requires a separate reviewed implementation with old-value snapshot, readback, rollback,
and explicit live-operation authorization.

## Validation Command

```powershell
python -m pytest tests\test_v1_5_sencoa_sencob_controlled_writer_preflight.py tests\test_v1_5_sencoa_sencob_writer_design_review.py tests\test_v1_5_algorithm_write_contract_review.py tests\test_v1_5_entrypoint_inventory.py -q
```

## Result

```text
38 passed, 1 warning in 27.71s
```

The warning is the existing unregistered `v1_5_formal_gate` pytest marker warning in
`tests/test_v1_5_entrypoint_inventory.py`.

## Boundary

- `opens_com_ports=false`
- `writes_coefficients=false`
- `database_written=false`
- Mature CO2/H2O queue files were not changed.
- Shared formal sampling was not changed.
- `workflow/runner.py`, `devices/gas_analyzer.py`, `configs/default_config.json`, and
  `run_app.py` were not changed.
