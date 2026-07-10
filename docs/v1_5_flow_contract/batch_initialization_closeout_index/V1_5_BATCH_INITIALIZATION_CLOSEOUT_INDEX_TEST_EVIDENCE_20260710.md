# V1.5 Batch Initialization Closeout Index Test Evidence

This evidence belongs to the offline batch initialization closeout index package.
It does not open COM ports, control pressure, control gas/water routes, connect
PostgreSQL, write SN/device_code, or write SENCO coefficients.

## Focused command

```text
python -m pytest tests\test_v1_5_batch_initialization_closeout_index.py tests\test_v1_5_entrypoint_inventory.py tests\test_v1_5_full_flow_next_action_plan.py -q
```

## Result

```text
36 passed, 1 warning in 14.85s
```

The warning is the existing `v1_5_formal_gate` pytest marker registration
warning from the entrypoint inventory tests.

## Static check

```text
git diff --check
```

Result: no whitespace/content errors. Git emitted the existing Windows LF/CRLF
normalization warning for `v1_5_entrypoint_inventory.py`.
