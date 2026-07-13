# V1.5 PostgreSQL 18 transaction plan test evidence

Date: 2026-07-13

## Scope

- Deterministic PostgreSQL 18 transaction-stage preview.
- SN/device_code primary identity and protocol-ID alias validation for 1-6 analyzers.
- Duplicate GA/COM/SN/device_code/protocol-ID rejection.
- Command-contract SHA256 rebinding for authorization, preflight, archive, and evidence bundle.
- Pre-commit identity/count/hash readback and rollback contract.
- Default-blocked executor, full-flow order, formal-flow guard, formal-run-status, and entrypoint inventory.

## Focused result

```text
256 passed, 2 warnings in 117.74s
```

The warnings are the existing unregistered `v1_5_formal_gate` pytest marker notices.

After adding command-contract SHA256 rebinding and making
`production_transaction_package_ready` a required formal-status import gate, the
new transaction tests plus the full formal-run-status suite were rerun:

```text
93 passed in 28.88s
```

## Parity and resilience result

```text
60 passed, 3 deselected in 132.83s
```

The three deselected V2 storage tests require the historical untracked
`src/gas_calibrator/v2/output/v1_v2_compare/v2_collect_0c/run_20260320_043540/samples_runtime.csv`
fixture. The fixture exists in the root workspace but is intentionally absent from the clean worktree.
The first run failed only those three fixture-dependent tests; the other 60 passed.

## Current checked artifact status

- `transaction_plan_contract_ready=true`
- `production_transaction_package_ready=false`
- `planned_device_count=0`
- `connects_postgresql=false`
- `database_written=false`
- `database_import_allowed=false`
- `emits_executable_sql=false`
- `dsn_value_read=false`

Production remains blocked because the checked review package has no current
1-6 device preview, no current formal archive closure/evidence bundle, and the
older command/authorization/preflight/design artifacts are not a fresh hash-bound
production import packet.

This closes the offline transaction-plan portion of frozen gap 5. It does not
close the real PostgreSQL 18 controlled-import gap and is not real acceptance evidence.
