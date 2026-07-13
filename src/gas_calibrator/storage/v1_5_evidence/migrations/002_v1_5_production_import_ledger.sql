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
