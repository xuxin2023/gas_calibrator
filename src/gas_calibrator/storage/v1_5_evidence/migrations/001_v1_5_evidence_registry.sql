CREATE SCHEMA IF NOT EXISTS v1_5_evidence;

CREATE TABLE IF NOT EXISTS v1_5_evidence.schema_migrations (
    version TEXT PRIMARY KEY,
    checksum TEXT NOT NULL,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS v1_5_evidence.runs (
    id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL UNIQUE,
    run_dir TEXT NOT NULL,
    plan_id TEXT NULL,
    plan_version TEXT NULL,
    analyzer_id TEXT NULL,
    operator_name TEXT NULL,
    config_hash TEXT NULL,
    package_status TEXT NULL,
    package_blockers JSONB NOT NULL DEFAULT '[]'::jsonb,
    evidence_status TEXT NOT NULL DEFAULT 'indexed',
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS v1_5_evidence.devices (
    id TEXT PRIMARY KEY,
    device_type TEXT NOT NULL,
    device_role TEXT NOT NULL,
    display_name TEXT NOT NULL,
    serial_number TEXT NULL,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS v1_5_evidence.run_devices (
    id TEXT PRIMARY KEY,
    run_db_id TEXT NOT NULL REFERENCES v1_5_evidence.runs(id) ON DELETE CASCADE,
    device_id TEXT NOT NULL REFERENCES v1_5_evidence.devices(id) ON DELETE CASCADE,
    role TEXT NOT NULL,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (run_db_id, device_id, role)
);

CREATE TABLE IF NOT EXISTS v1_5_evidence.standard_gases (
    id TEXT PRIMARY KEY,
    run_db_id TEXT NOT NULL REFERENCES v1_5_evidence.runs(id) ON DELETE CASCADE,
    component TEXT NOT NULL,
    cylinder_id TEXT NOT NULL,
    certificate_value DOUBLE PRECISION NULL,
    certificate_uncertainty DOUBLE PRECISION NULL,
    valid_until DATE NULL,
    supplier TEXT NULL,
    certificate_hash TEXT NULL,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS v1_5_evidence.reference_certificates (
    id TEXT PRIMARY KEY,
    run_db_id TEXT NOT NULL REFERENCES v1_5_evidence.runs(id) ON DELETE CASCADE,
    device_id TEXT NULL REFERENCES v1_5_evidence.devices(id) ON DELETE SET NULL,
    reference_role TEXT NOT NULL,
    certificate_id TEXT NULL,
    certificate_hash TEXT NULL,
    valid_until DATE NULL,
    uncertainty DOUBLE PRECISION NULL,
    unit TEXT NULL,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS v1_5_evidence.calibration_points (
    id TEXT PRIMARY KEY,
    run_db_id TEXT NOT NULL REFERENCES v1_5_evidence.runs(id) ON DELETE CASCADE,
    component TEXT NOT NULL,
    point_key TEXT NOT NULL,
    point_tag TEXT NULL,
    pressure_mode TEXT NULL,
    target_value DOUBLE PRECISION NULL,
    sample_count INTEGER NOT NULL DEFAULT 0,
    a_grade_count INTEGER NOT NULL DEFAULT 0,
    b_grade_count INTEGER NOT NULL DEFAULT 0,
    rejected_count INTEGER NOT NULL DEFAULT 0,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS v1_5_evidence.sample_files (
    id TEXT PRIMARY KEY,
    run_db_id TEXT NOT NULL REFERENCES v1_5_evidence.runs(id) ON DELETE CASCADE,
    artifact_role TEXT NOT NULL,
    path TEXT NOT NULL,
    sha256 TEXT NOT NULL,
    size_bytes BIGINT NOT NULL DEFAULT 0,
    modified_at TIMESTAMPTZ NULL,
    required BOOLEAN NOT NULL DEFAULT FALSE,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (run_db_id, path)
);

CREATE TABLE IF NOT EXISTS v1_5_evidence.qc_results (
    id TEXT PRIMARY KEY,
    run_db_id TEXT NOT NULL REFERENCES v1_5_evidence.runs(id) ON DELETE CASCADE,
    scope TEXT NOT NULL,
    subject_id TEXT NULL,
    rule_name TEXT NOT NULL,
    status TEXT NOT NULL,
    severity TEXT NOT NULL DEFAULT 'info',
    reasons JSONB NOT NULL DEFAULT '[]'::jsonb,
    metrics JSONB NOT NULL DEFAULT '{}'::jsonb,
    source_artifact_id TEXT NULL REFERENCES v1_5_evidence.sample_files(id) ON DELETE SET NULL,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS v1_5_evidence.coefficient_snapshots (
    id TEXT PRIMARY KEY,
    run_db_id TEXT NOT NULL REFERENCES v1_5_evidence.runs(id) ON DELETE CASCADE,
    analyzer_id TEXT NULL,
    snapshot_type TEXT NOT NULL,
    coefficients JSONB NOT NULL DEFAULT '{}'::jsonb,
    coefficients_hash TEXT NULL,
    source_artifact_id TEXT NULL REFERENCES v1_5_evidence.sample_files(id) ON DELETE SET NULL,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS v1_5_evidence.coefficient_candidates (
    id TEXT PRIMARY KEY,
    run_db_id TEXT NOT NULL REFERENCES v1_5_evidence.runs(id) ON DELETE CASCADE,
    component TEXT NOT NULL,
    candidate_status TEXT NOT NULL,
    allowed_for_review BOOLEAN NOT NULL DEFAULT FALSE,
    auto_write_allowed BOOLEAN NOT NULL DEFAULT FALSE,
    blockers JSONB NOT NULL DEFAULT '[]'::jsonb,
    coefficients JSONB NOT NULL DEFAULT '{}'::jsonb,
    source_artifact_id TEXT NULL REFERENCES v1_5_evidence.sample_files(id) ON DELETE SET NULL,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS v1_5_evidence.coefficient_write_events (
    id TEXT PRIMARY KEY,
    run_db_id TEXT NOT NULL REFERENCES v1_5_evidence.runs(id) ON DELETE CASCADE,
    analyzer_id TEXT NULL,
    event_type TEXT NOT NULL,
    status TEXT NOT NULL,
    approved_by TEXT NULL,
    command_summary TEXT NULL,
    old_coefficients_hash TEXT NULL,
    candidate_id TEXT NULL REFERENCES v1_5_evidence.coefficient_candidates(id) ON DELETE SET NULL,
    readback JSONB NOT NULL DEFAULT '{}'::jsonb,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    event_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS v1_5_evidence.reports (
    id TEXT PRIMARY KEY,
    run_db_id TEXT NOT NULL REFERENCES v1_5_evidence.runs(id) ON DELETE CASCADE,
    report_type TEXT NOT NULL,
    path TEXT NOT NULL,
    sha256 TEXT NULL,
    status TEXT NOT NULL DEFAULT 'available',
    generated_at TIMESTAMPTZ NULL,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS v1_5_evidence.audit_events (
    id TEXT PRIMARY KEY,
    run_db_id TEXT NOT NULL REFERENCES v1_5_evidence.runs(id) ON DELETE CASCADE,
    event_type TEXT NOT NULL,
    actor TEXT NULL,
    event_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    payload JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS v1_5_evidence.evidence_integrity_checks (
    id TEXT PRIMARY KEY,
    run_db_id TEXT NOT NULL REFERENCES v1_5_evidence.runs(id) ON DELETE CASCADE,
    check_name TEXT NOT NULL,
    status TEXT NOT NULL,
    severity TEXT NOT NULL DEFAULT 'info',
    details JSONB NOT NULL DEFAULT '{}'::jsonb,
    checked_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ix_v1_5_evidence_runs_run_id ON v1_5_evidence.runs (run_id);
CREATE INDEX IF NOT EXISTS ix_v1_5_evidence_runs_status ON v1_5_evidence.runs (package_status, evidence_status);
CREATE INDEX IF NOT EXISTS ix_v1_5_evidence_standard_gases_run ON v1_5_evidence.standard_gases (run_db_id, component);
CREATE INDEX IF NOT EXISTS ix_v1_5_evidence_calibration_points_run ON v1_5_evidence.calibration_points (run_db_id, component);
CREATE INDEX IF NOT EXISTS ix_v1_5_evidence_sample_files_run_role ON v1_5_evidence.sample_files (run_db_id, artifact_role);
CREATE INDEX IF NOT EXISTS ix_v1_5_evidence_sample_files_sha256 ON v1_5_evidence.sample_files (sha256);
CREATE INDEX IF NOT EXISTS ix_v1_5_evidence_qc_run_rule ON v1_5_evidence.qc_results (run_db_id, rule_name, status);
CREATE INDEX IF NOT EXISTS ix_v1_5_evidence_candidates_run ON v1_5_evidence.coefficient_candidates (run_db_id, component, candidate_status);
CREATE INDEX IF NOT EXISTS ix_v1_5_evidence_write_events_run ON v1_5_evidence.coefficient_write_events (run_db_id, event_type, status);
CREATE INDEX IF NOT EXISTS ix_v1_5_evidence_reports_run ON v1_5_evidence.reports (run_db_id, report_type);
CREATE INDEX IF NOT EXISTS ix_v1_5_evidence_audit_run_time ON v1_5_evidence.audit_events (run_db_id, event_at);
CREATE INDEX IF NOT EXISTS ix_v1_5_evidence_integrity_run ON v1_5_evidence.evidence_integrity_checks (run_db_id, check_name, status);

CREATE OR REPLACE VIEW v1_5_evidence.run_evidence_summary AS
SELECT
    r.id,
    r.run_id,
    r.plan_id,
    r.plan_version,
    r.analyzer_id,
    r.package_status,
    r.evidence_status,
    COALESCE(files.file_count, 0) AS file_count,
    COALESCE(files.required_file_count, 0) AS required_file_count,
    COALESCE(points.point_count, 0) AS point_count,
    COALESCE(qc.failed_qc_count, 0) AS failed_qc_count,
    COALESCE(candidates.ready_candidate_count, 0) AS ready_candidate_count,
    COALESCE(writes.write_attempt_count, 0) AS write_attempt_count,
    COALESCE(checks.failed_integrity_count, 0) AS failed_integrity_count,
    r.updated_at
FROM v1_5_evidence.runs r
LEFT JOIN (
    SELECT run_db_id, COUNT(*) AS file_count, COUNT(*) FILTER (WHERE required) AS required_file_count
    FROM v1_5_evidence.sample_files
    GROUP BY run_db_id
) files ON files.run_db_id = r.id
LEFT JOIN (
    SELECT run_db_id, COUNT(*) AS point_count
    FROM v1_5_evidence.calibration_points
    GROUP BY run_db_id
) points ON points.run_db_id = r.id
LEFT JOIN (
    SELECT run_db_id, COUNT(*) FILTER (WHERE status NOT IN ('pass', 'ready_for_reviewer', 'ok')) AS failed_qc_count
    FROM v1_5_evidence.qc_results
    GROUP BY run_db_id
) qc ON qc.run_db_id = r.id
LEFT JOIN (
    SELECT run_db_id, COUNT(*) FILTER (WHERE candidate_status = 'ready_for_reviewer') AS ready_candidate_count
    FROM v1_5_evidence.coefficient_candidates
    GROUP BY run_db_id
) candidates ON candidates.run_db_id = r.id
LEFT JOIN (
    SELECT run_db_id, COUNT(*) FILTER (WHERE status NOT IN ('not_attempted', 'blocked')) AS write_attempt_count
    FROM v1_5_evidence.coefficient_write_events
    GROUP BY run_db_id
) writes ON writes.run_db_id = r.id
LEFT JOIN (
    SELECT run_db_id, COUNT(*) FILTER (WHERE status <> 'pass') AS failed_integrity_count
    FROM v1_5_evidence.evidence_integrity_checks
    GROUP BY run_db_id
) checks ON checks.run_db_id = r.id;
