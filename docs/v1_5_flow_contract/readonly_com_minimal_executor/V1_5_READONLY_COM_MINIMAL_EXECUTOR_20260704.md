# V1.5 Read-Only COM Minimal Executor

This package adds the first minimal real read-only COM executor for V1.5
initialization evidence. It is manual-authorized and does not run from the
default full-flow planner.

## Allowed Scope

- Opens reviewed analyzer COM ports only with `--execute-read-only-real-com`.
- Requires authorization packet, reviewed port inventory, active analyzer list,
  packet validator evidence, plan preview evidence, and minimal executor stub
  evidence.
- Reads only identity/SN/GETCO/runtime/CHECK evidence.
- Preserves `>=1s` serial command spacing.
- Holds on SN mismatch, GETCO parse failure, non-neutral S5/S6/S7/S8, runtime
  evidence gaps, legacy CHECK mistakes, and serial failures.
- Writes:
  - `readonly_com_executor_invocation.json`
  - `readonly_com_command_attempts.csv`
  - `readonly_com_raw_responses.csv`
  - `readonly_com_hold_events.csv`
  - `readonly_com_identity_getco_snapshot.json`

## Forbidden Scope

- Does not write SN or `device_code`.
- Does not write S5/S6/S7/S8/S9/SENCOA/SENCOB or any coefficient.
- Does not connect PostgreSQL or import database rows.
- Does not control pressure, CO2 routes, H2O routes, humidity generator, or temperature box.
- Does not refresh formal release, database import, or real acceptance.
- Does not modify the mature 0620 CO2/H2O queue path, shared sampling, `runner.py`,
  `gas_analyzer.py`, `default_config.json`, or `run_app.py`.

## Legacy CHECK Boundary

Legacy/old algorithm analyzers must not receive `CHECK,YGAS,FFF`. If an active
analyzer list marks a legacy analyzer as `check_capable=true` or
`check_required=true`, the executor holds before opening COM.

## Authorization Binding

The executor requires the current `authorization_packet_json` path to match the
authorization packet path recorded by the packet validator. It also rechecks the
minimum authorization shape before opening COM: operator/reviewer/approver,
distinct reviewer and approver, structured or legacy confirmation, no-write /
no-database / no-route boundaries, and `>=1s` serial pacing.

## Focused Test Evidence

```text
python -m pytest tests\test_v1_5_formal_readonly_com_minimal_executor.py tests\test_v1_5_entrypoint_inventory.py tests\test_v1_5_formal_run_status.py -q
75 passed, 1 warning in 16.09s

python -m pytest tests\test_v1_5_formal_readonly_com_execution_packet_validator.py tests\test_v1_5_formal_readonly_com_execution_plan_preview.py tests\test_v1_5_formal_readonly_com_minimal_executor_review.py tests\test_v1_5_formal_readonly_com_minimal_executor_stub.py tests\test_v1_5_formal_readonly_com_minimal_executor.py -q
40 passed in 5.96s
```
