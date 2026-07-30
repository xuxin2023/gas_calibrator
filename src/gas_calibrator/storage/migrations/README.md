# Shared storage PostgreSQL migration history

This directory owns the reviewed PostgreSQL upgrade history for
`gas_calibrator.storage`.

The files are ordered and cumulative:

1. `001_initial.sql` creates the original run, point, sample, measurement,
   quality, fit, coefficient, event, and alarm schema.
2. `002_sensor_dimension_and_run_metadata.sql` adds the sensor dimension,
   stable `sensor_id` joins, and run/point metadata.
3. `003_sensor_sn_device_code_identity.sql` promotes reviewed V1.5 SN and
   device codes from compatibility metadata into indexed columns.
4. `004_sensor_identity_aliases.sql` preserves searchable identity history
   without treating non-unique protocol IDs as primary identity.

These files are deployment assets, not an application startup mechanism. The
application does not discover or execute them automatically. Applying them to
a real database requires an explicit database change review, backup/restore
plan, target-version record, and operator approval.

Local tests and new SQLite databases use `Base.metadata.create_all()` through
`DatabaseManager.initialize()`. That produces the same logical tables and
columns required by the current import/query services, but the PostgreSQL DDL
and ORM-generated DDL are not byte-identical.

Known reviewed differences that remain separate P1 schema-governance work:

- PostgreSQL history declares `runs.run_mode` and `runs.route_mode` as
  `VARCHAR(64)`; the ORM currently uses `VARCHAR(32)`.
- PostgreSQL history declares `points.co2_group` as `VARCHAR(32)`; the ORM
  currently uses `VARCHAR(16)`.
- PostgreSQL history indexes run mode, route mode, profile name, and profile
  version together; the ORM fresh-schema index currently uses run mode and
  profile name.
- PostgreSQL history uses `ON DELETE SET NULL` for fact-table `sensor_id`
  foreign keys; ORM-created schemas currently use the default foreign-key
  delete behavior.
- Several PostgreSQL JSON/channel columns have server defaults while the ORM
  supplies equivalent values at the Python layer.

Do not resolve those differences as an incidental cleanup. Each needs a
separate compatibility review against existing PostgreSQL deployments before
changing either the migration history or shared ORM.
