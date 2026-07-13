# V1.5 PostgreSQL 18 staging real integration test evidence

Date: 2026-07-14

## Boundary

This was a database-only integration test against a temporary, dedicated local
PostgreSQL 18 test database named
`gas_calibrator_v15_staging_test_20260714`. It was not the production
`gas_calibrator` database.

- No analyzer or COM port was opened.
- No SN/device ID or SENCO coefficient was read or written.
- No pressure, CO2, H2O, chamber, or humidity route was controlled.
- No production migration or production evidence import was executed.
- The temporary test database was dropped after the tests completed.
- Credentials and the DSN value are not stored in this evidence file.

## Runtime check

The local service reported:

```text
current_database=postgres
server_version_num=180003
current_user=postgres
```

## Focused real staging transaction

The explicitly gated PostgreSQL 18 test exercised:

- schema initialization in random staging-only schemas;
- atomic staging evidence import;
- exact idempotent replay;
- SN/device_code/protocol-ID/run-ID query readback;
- changed-payload conflict hold;
- injected failure rollback and absence readback;
- staging import CLI and query CLI paths;
- cleanup of both random staging schemas.

```text
$env:V1_5_POSTGRES_STAGING_DSN_TEST='<dedicated-test-dsn>'
python -m pytest tests/test_v1_5_formal_database_import_staging_executor.py::test_postgresql18_staging_atomic_idempotent_and_queryable -q

1 passed in 22.63s
```

## Staging executor suite

```text
$env:V1_5_POSTGRES_STAGING_DSN_TEST='<dedicated-test-dsn>'
python -m pytest tests/test_v1_5_formal_database_import_staging_executor.py -q

13 passed in 5.79s
```

## Full database-chain regression with integration enabled

```text
$env:V1_5_POSTGRES_STAGING_DSN_TEST='<dedicated-test-dsn>'
$dbTests = @(Get-ChildItem tests -Filter 'test_v1_5_formal_database*.py' | ForEach-Object { $_.FullName })
$dbTests += (Resolve-Path tests\test_v1_5_evidence_registry.py).Path
$dbTests += (Resolve-Path tests\test_v1_5_entrypoint_inventory.py).Path
python -m pytest @dbTests -q

226 passed, 1 warning in 93.39s
```

The warning is the existing unregistered `v1_5_formal_gate` pytest mark. The
previously skipped PostgreSQL 18 staging integration test ran and passed.

## Result

The PostgreSQL 18 staging transaction, rollback, idempotency, and query path is
now verified against a real isolated database. This evidence does not authorize
or imply a production migration, production evidence import, or formal
calibration release.
