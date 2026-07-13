# V1.5 PostgreSQL 18 staging import test evidence

Date: 2026-07-13

## Focused result

```text
13 passed in 15.99s
```

Command family:

```text
python -m pytest tests/test_v1_5_formal_database_import_staging_executor.py -q
```

The environment supplied a staging-only DSN through `V1_5_POSTGRES_STAGING_DSN_TEST`; no DSN value is stored in this repository.

## PostgreSQL integration scope

- server version number: `180003` (PostgreSQL 18);
- database: dedicated local `gas_calibrator_staging` database;
- schemas: random `v1_5_core_staging_<suffix>` and `v1_5_evidence_staging_<suffix>`;
- cleanup: both random schemas dropped in `finally`;
- leftover test schemas after the run: `0`.

## Behaviors proved

- 1-device and 6-device package validation;
- batches larger than 6 devices are rejected before connection;
- eight-digit SN/device_code uniqueness;
- protocol-ID binding between transaction plan and evidence bundle;
- explicit staging-only authorization;
- first import commits as one transaction;
- exact re-import is an idempotent no-op;
- same run with changed payload is rejected and rolled back;
- injected failure after partial work leaves no committed run ledger;
- the same injected failure also leaves no committed core run row;
- lookup by SN, protocol device ID, and run ID;
- end-to-end executor CLI plus explicit read-only query CLI;
- preview mode does not read a DSN or connect PostgreSQL;
- production schema, production import, formal release, COM, coefficient writes, pressure, and routes remain locked.

This evidence is staging integration evidence only. It is not real calibration acceptance and is not production database-import authorization.

## Compatibility regression

Database/evidence/entrypoint/parity regression:

```text
159 passed, 1 warning in 41.68s
```

Existing V1.5 final offline acceptance allowlist:

```text
325 passed, 2 warnings in 511.02s
```

The warnings are the pre-existing unregistered `v1_5_formal_gate` pytest marker. No test failed.
