\set ON_ERROR_STOP on
-- V1.5 PostgreSQL 18 migration 002 postcheck. Read-only.
SELECT current_database() AS database_name,
       current_setting('server_version_num')::integer AS server_version_num;
SELECT version, checksum, applied_at
FROM v1_5_evidence.schema_migrations
WHERE version = '002_v1_5_production_import_ledger';
SELECT column_name, data_type, is_nullable, ordinal_position
FROM information_schema.columns
WHERE table_schema = 'v1_5_evidence'
  AND table_name = 'production_import_ledger'
ORDER BY ordinal_position;
SELECT conname, contype, pg_get_constraintdef(oid) AS definition
FROM pg_constraint
WHERE conrelid = 'v1_5_evidence.production_import_ledger'::regclass
ORDER BY contype, conname;
SELECT indexname, indexdef
FROM pg_indexes
WHERE schemaname = 'v1_5_evidence'
  AND tablename = 'production_import_ledger'
ORDER BY indexname;
-- Expected migration checksum: b2d50313eb22c78f7975c3e77380f5d4ce616e494c928295a76ff5edaf208e31
