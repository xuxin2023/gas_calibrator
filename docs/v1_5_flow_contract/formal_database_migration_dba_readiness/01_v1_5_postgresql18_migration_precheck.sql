\set ON_ERROR_STOP on
-- V1.5 PostgreSQL 18 DBA precheck. Read-only; run before migration 002.
SELECT current_database() AS database_name,
       current_setting('server_version_num')::integer AS server_version_num;
SELECT version, checksum, applied_at
FROM v1_5_evidence.schema_migrations
WHERE version IN (
    '001_v1_5_evidence_registry',
    '002_v1_5_production_import_ledger'
)
ORDER BY version;
SELECT table_schema, table_name
FROM information_schema.tables
WHERE table_schema = 'v1_5_evidence'
  AND table_name IN ('runs', 'schema_migrations', 'production_import_ledger')
ORDER BY table_name;

-- Expected 001_v1_5_evidence_registry checksum: fdb8f9d3d3d47ca34633dc757b1fb97741d178bd4fa8dabc18a23c795e0eefbc
-- Expected 002_v1_5_production_import_ledger checksum: b2d50313eb22c78f7975c3e77380f5d4ce616e494c928295a76ff5edaf208e31
