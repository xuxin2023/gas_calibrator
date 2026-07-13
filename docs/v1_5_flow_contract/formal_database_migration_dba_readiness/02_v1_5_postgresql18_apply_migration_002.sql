\set ON_ERROR_STOP on
-- V1.5 PostgreSQL 18 migration 002 DBA packet.
-- Review and execute manually with ON_ERROR_STOP enabled.
BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '60s';
LOCK TABLE v1_5_evidence.schema_migrations IN SHARE ROW EXCLUSIVE MODE;
DO $v1_5_migration_guard$
DECLARE
    server_version integer := current_setting('server_version_num')::integer;
BEGIN
    IF current_database() <> 'gas_calibrator' THEN
        RAISE EXCEPTION 'wrong production database: %', current_database();
    END IF;
    IF server_version < 180000 OR server_version >= 190000 THEN
        RAISE EXCEPTION 'PostgreSQL major must be 18: %', server_version;
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM v1_5_evidence.schema_migrations
        WHERE version = '001_v1_5_evidence_registry'
          AND checksum = 'fdb8f9d3d3d47ca34633dc757b1fb97741d178bd4fa8dabc18a23c795e0eefbc'
    ) THEN
        RAISE EXCEPTION 'migration 001 missing or checksum mismatch';
    END IF;
    IF EXISTS (
        SELECT 1 FROM v1_5_evidence.schema_migrations
        WHERE version = '002_v1_5_production_import_ledger'
          AND checksum <> 'b2d50313eb22c78f7975c3e77380f5d4ce616e494c928295a76ff5edaf208e31'
    ) THEN
        RAISE EXCEPTION 'migration 002 checksum mismatch';
    END IF;
    IF (EXISTS (
        SELECT 1 FROM v1_5_evidence.schema_migrations
        WHERE version = '002_v1_5_production_import_ledger'
          AND checksum = 'b2d50313eb22c78f7975c3e77380f5d4ce616e494c928295a76ff5edaf208e31'
    )) <> (to_regclass('v1_5_evidence.production_import_ledger') IS NOT NULL) THEN
        RAISE EXCEPTION 'migration 002 ledger/table state mismatch';
    END IF;
END
$v1_5_migration_guard$;

CREATE TABLE IF NOT EXISTS v1_5_evidence.production_import_ledger (
    run_db_id TEXT PRIMARY KEY REFERENCES v1_5_evidence.runs(id) ON DELETE CASCADE,
    run_id TEXT NOT NULL UNIQUE,
    evidence_bundle_sha256 TEXT NOT NULL,
    transaction_plan_sha256 TEXT NOT NULL,
    promotion_preflight_sha256 TEXT NOT NULL,
    execution_authorization_sha256 TEXT NOT NULL,
    authorization_id TEXT NOT NULL UNIQUE,
    operator_name TEXT NOT NULL,
    reviewer_name TEXT NOT NULL,
    approver_name TEXT NOT NULL,
    table_counts JSONB NOT NULL,
    committed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_v1_5_evidence_production_import_ledger_run_id
ON v1_5_evidence.production_import_ledger (run_id);

INSERT INTO v1_5_evidence.schema_migrations (version, checksum)
VALUES ('002_v1_5_production_import_ledger', 'b2d50313eb22c78f7975c3e77380f5d4ce616e494c928295a76ff5edaf208e31')
ON CONFLICT (version) DO NOTHING;

DO $v1_5_migration_verify$
DECLARE
    actual_columns text[];
BEGIN
    SELECT array_agg(column_name::text ORDER BY ordinal_position)
    INTO actual_columns
    FROM information_schema.columns
    WHERE table_schema = 'v1_5_evidence'
      AND table_name = 'production_import_ledger';
    IF actual_columns <> ARRAY['run_db_id', 'run_id', 'evidence_bundle_sha256', 'transaction_plan_sha256', 'promotion_preflight_sha256', 'execution_authorization_sha256', 'authorization_id', 'operator_name', 'reviewer_name', 'approver_name', 'table_counts', 'committed_at']::text[] THEN
        RAISE EXCEPTION 'production_import_ledger columns mismatch: %', actual_columns;
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM v1_5_evidence.schema_migrations
        WHERE version = '002_v1_5_production_import_ledger'
          AND checksum = 'b2d50313eb22c78f7975c3e77380f5d4ce616e494c928295a76ff5edaf208e31'
    ) THEN
        RAISE EXCEPTION 'migration 002 ledger row missing or checksum mismatch';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'v1_5_evidence.production_import_ledger'::regclass
          AND contype = 'p'
          AND pg_get_constraintdef(oid) = 'PRIMARY KEY (run_db_id)'
    ) THEN
        RAISE EXCEPTION 'production_import_ledger run_db_id primary key missing';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'v1_5_evidence.production_import_ledger'::regclass
          AND contype = 'u'
          AND pg_get_constraintdef(oid) = 'UNIQUE (run_id)'
    ) THEN
        RAISE EXCEPTION 'production_import_ledger run_id unique constraint missing';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'v1_5_evidence.production_import_ledger'::regclass
          AND contype = 'u'
          AND pg_get_constraintdef(oid) = 'UNIQUE (authorization_id)'
    ) THEN
        RAISE EXCEPTION 'production_import_ledger authorization_id unique constraint missing';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'v1_5_evidence.production_import_ledger'::regclass
          AND contype = 'f'
          AND pg_get_constraintdef(oid) = 'FOREIGN KEY (run_db_id) REFERENCES v1_5_evidence.runs(id) ON DELETE CASCADE'
    ) THEN
        RAISE EXCEPTION 'production_import_ledger run_db_id foreign key missing';
    END IF;
    IF to_regclass('v1_5_evidence.ix_v1_5_evidence_production_import_ledger_run_id') IS NULL THEN
        RAISE EXCEPTION 'production_import_ledger run_id index missing';
    END IF;
END
$v1_5_migration_verify$;
COMMIT;
