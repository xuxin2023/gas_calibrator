# V1.5 Database Import Archive Binding

This contract binds the PostgreSQL 18 import review chain to the exact SENCO authorization archive binding produced by formal archive closure.

## Required chain

Before a database import command can be reviewed, the offline guards must verify:

1. The formal archive index contains exactly one `senco_authorization_write_traceability_json` artifact.
2. The binding JSON still matches the SHA-256 recorded in the archive index.
3. The binding payload exactly matches the copy embedded in the archive index.
4. The binding remains `ready_for_archive_release` or `not_applicable_no_main_senco_write_evidence`.
5. Every bound authorization, manifest, writer metadata, and readback CSV still exists and matches both its binding SHA-256 and archive artifact SHA-256.
6. The manual database-import authorization records the same binding path and SHA-256.
7. The command contract and blocked executor independently re-hash the binding before proceeding to any later implementation review.

Missing, changed, unindexed, or mismatched evidence is a hard import blocker. It does not block mature CO2/H2O sampling.

## Safety boundary

This package remains offline and non-executable:

- `connects_postgresql=false`
- `database_import_attempted=false`
- `database_written=false`
- `applies_migrations=false`
- `opens_com_ports=false`
- `writes_coefficients=false`
- `controls_water_or_gas_routes=false`

It does not add a real `--execute-controlled-import` path and does not change the mature 0613/0620/0621 calibration route.
