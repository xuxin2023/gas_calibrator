CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS runs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    start_time TIMESTAMPTZ NULL,
    end_time TIMESTAMPTZ NULL,
    status VARCHAR(32) NOT NULL CHECK (status IN ('running', 'completed', 'failed', 'aborted')),
    config_hash VARCHAR(128) NULL,
    software_version VARCHAR(64) NULL,
    operator VARCHAR(128) NULL,
    total_points INTEGER NOT NULL DEFAULT 0,
    successful_points INTEGER NOT NULL DEFAULT 0,
    failed_points INTEGER NOT NULL DEFAULT 0,
    warnings INTEGER NOT NULL DEFAULT 0,
    errors INTEGER NOT NULL DEFAULT 0,
    notes TEXT NULL
);

CREATE TABLE IF NOT EXISTS points (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id UUID NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    sequence INTEGER NOT NULL,
    temperature_c DOUBLE PRECISION NULL,
    humidity_rh DOUBLE PRECISION NULL,
    pressure_hpa DOUBLE PRECISION NULL,
    route_type VARCHAR(16) NOT NULL CHECK (route_type IN ('h2o', 'co2')),
    co2_target_ppm DOUBLE PRECISION NULL,
    status VARCHAR(32) NOT NULL CHECK (status IN ('pending', 'running', 'completed', 'failed', 'skipped')),
    stability_time_s DOUBLE PRECISION NULL,
    total_time_s DOUBLE PRECISION NULL,
    retry_count INTEGER NOT NULL DEFAULT 0,
    CONSTRAINT uq_points_run_sequence UNIQUE (run_id, sequence)
);

CREATE TABLE IF NOT EXISTS samples (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    point_id UUID NOT NULL REFERENCES points(id) ON DELETE CASCADE,
    analyzer_id VARCHAR(64) NOT NULL,
    analyzer_serial VARCHAR(64) NULL,
    sample_index INTEGER NOT NULL,
    "timestamp" TIMESTAMPTZ NULL,
    co2_ppm DOUBLE PRECISION NULL,
    h2o_mmol DOUBLE PRECISION NULL,
    pressure_hpa DOUBLE PRECISION NULL,
    co2_ratio_f DOUBLE PRECISION NULL,
    h2o_ratio_f DOUBLE PRECISION NULL,
    co2_ratio_raw DOUBLE PRECISION NULL,
    h2o_ratio_raw DOUBLE PRECISION NULL,
    chamber_temp_c DOUBLE PRECISION NULL,
    case_temp_c DOUBLE PRECISION NULL,
    dewpoint_c DOUBLE PRECISION NULL,
    CONSTRAINT uq_samples_point_analyzer_index UNIQUE (point_id, analyzer_id, sample_index)
);

CREATE TABLE IF NOT EXISTS measurement_frames (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id UUID NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    point_id UUID NOT NULL REFERENCES points(id) ON DELETE CASCADE,
    sample_index INTEGER NOT NULL,
    sample_ts TIMESTAMPTZ NULL,
    analyzer_label VARCHAR(32) NOT NULL,
    analyzer_id VARCHAR(64) NULL,
    analyzer_serial VARCHAR(64) NULL,
    frame_has_data BOOLEAN NOT NULL DEFAULT FALSE,
    frame_usable BOOLEAN NOT NULL DEFAULT FALSE,
    analyzer_status VARCHAR(64) NULL,
    mode VARCHAR(32) NULL,
    mode2_field_count INTEGER NULL,
    co2_ppm DOUBLE PRECISION NULL,
    h2o_mmol DOUBLE PRECISION NULL,
    co2_ratio_f DOUBLE PRECISION NULL,
    h2o_ratio_f DOUBLE PRECISION NULL,
    co2_ratio_raw DOUBLE PRECISION NULL,
    h2o_ratio_raw DOUBLE PRECISION NULL,
    ref_signal DOUBLE PRECISION NULL,
    co2_signal DOUBLE PRECISION NULL,
    h2o_signal DOUBLE PRECISION NULL,
    chamber_temp_c DOUBLE PRECISION NULL,
    case_temp_c DOUBLE PRECISION NULL,
    pressure_kpa DOUBLE PRECISION NULL,
    raw_payload JSONB NOT NULL,
    context_payload JSONB NOT NULL,
    CONSTRAINT uq_measurement_frames_natural_key
        UNIQUE (run_id, point_id, analyzer_label, sample_index, sample_ts)
);

CREATE TABLE IF NOT EXISTS qc_results (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    point_id UUID NOT NULL REFERENCES points(id) ON DELETE CASCADE,
    rule_name VARCHAR(128) NOT NULL,
    passed BOOLEAN NOT NULL,
    value DOUBLE PRECISION NULL,
    threshold DOUBLE PRECISION NULL,
    message TEXT NULL,
    CONSTRAINT uq_qc_results_point_rule_message UNIQUE (point_id, rule_name, message)
);

CREATE TABLE IF NOT EXISTS fit_results (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id UUID NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    analyzer_id VARCHAR(64) NOT NULL,
    algorithm VARCHAR(64) NOT NULL,
    coefficients JSONB NOT NULL,
    rmse DOUBLE PRECISION NULL,
    r_squared DOUBLE PRECISION NULL,
    n_points INTEGER NULL,
    CONSTRAINT uq_fit_results_run_analyzer_algorithm UNIQUE (run_id, analyzer_id, algorithm)
);

CREATE TABLE IF NOT EXISTS coefficient_versions (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    analyzer_id VARCHAR(64) NOT NULL,
    analyzer_serial VARCHAR(64) NULL,
    version INTEGER NOT NULL,
    coefficients JSONB NOT NULL,
    created_at TIMESTAMPTZ NULL,
    created_by VARCHAR(128) NULL,
    approved BOOLEAN NOT NULL DEFAULT FALSE,
    approved_by VARCHAR(128) NULL,
    approved_at TIMESTAMPTZ NULL,
    deployed BOOLEAN NOT NULL DEFAULT FALSE,
    deployed_at TIMESTAMPTZ NULL,
    notes TEXT NULL,
    CONSTRAINT uq_coefficient_versions_analyzer_version UNIQUE (analyzer_id, analyzer_serial, version)
);

CREATE TABLE IF NOT EXISTS device_events (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id UUID NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    device_name VARCHAR(128) NOT NULL,
    event_type VARCHAR(64) NOT NULL,
    event_data JSONB NOT NULL,
    "timestamp" TIMESTAMPTZ NULL
);

CREATE TABLE IF NOT EXISTS alarms_incidents (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id UUID NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
    severity VARCHAR(16) NOT NULL CHECK (severity IN ('info', 'warning', 'error', 'critical')),
    category VARCHAR(128) NULL,
    message TEXT NOT NULL,
    details JSONB NOT NULL,
    "timestamp" TIMESTAMPTZ NULL,
    resolved BOOLEAN NOT NULL DEFAULT FALSE,
    resolved_at TIMESTAMPTZ NULL,
    CONSTRAINT uq_alarms_incidents_natural_key UNIQUE (run_id, category, message, "timestamp")
);

CREATE INDEX IF NOT EXISTS ix_runs_time_window ON runs (start_time, end_time);
CREATE INDEX IF NOT EXISTS ix_runs_operator ON runs (operator);
CREATE INDEX IF NOT EXISTS ix_points_run_id ON points (run_id);
CREATE INDEX IF NOT EXISTS ix_points_route_status ON points (route_type, status);
CREATE INDEX IF NOT EXISTS ix_samples_point_id ON samples (point_id);
CREATE INDEX IF NOT EXISTS ix_samples_timestamp ON samples ("timestamp");
CREATE INDEX IF NOT EXISTS ix_samples_analyzer ON samples (analyzer_id, analyzer_serial);
CREATE INDEX IF NOT EXISTS ix_measurement_frames_run_time ON measurement_frames (run_id, sample_ts);
CREATE INDEX IF NOT EXISTS ix_measurement_frames_point_sample ON measurement_frames (point_id, sample_index);
CREATE INDEX IF NOT EXISTS ix_measurement_frames_analyzer_time ON measurement_frames (analyzer_label, sample_ts);
CREATE INDEX IF NOT EXISTS ix_qc_results_point_id ON qc_results (point_id);
CREATE INDEX IF NOT EXISTS ix_qc_results_passed ON qc_results (passed);
CREATE INDEX IF NOT EXISTS ix_fit_results_run_id ON fit_results (run_id);
CREATE INDEX IF NOT EXISTS ix_fit_results_analyzer ON fit_results (analyzer_id);
CREATE INDEX IF NOT EXISTS ix_coefficient_versions_lookup ON coefficient_versions (analyzer_id, analyzer_serial, version);
CREATE INDEX IF NOT EXISTS ix_coefficient_versions_deployed ON coefficient_versions (analyzer_id, analyzer_serial, deployed);
CREATE INDEX IF NOT EXISTS ix_device_events_run_id ON device_events (run_id);
CREATE INDEX IF NOT EXISTS ix_device_events_timestamp ON device_events ("timestamp");
CREATE INDEX IF NOT EXISTS ix_alarms_incidents_run_id ON alarms_incidents (run_id);
CREATE INDEX IF NOT EXISTS ix_alarms_incidents_severity ON alarms_incidents (severity);
CREATE INDEX IF NOT EXISTS ix_alarms_incidents_timestamp ON alarms_incidents ("timestamp");

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_extension WHERE extname = 'timescaledb') THEN
        EXECUTE 'SELECT create_hypertable(''samples'', ''timestamp'', if_not_exists => TRUE)';
        EXECUTE 'SELECT create_hypertable(''measurement_frames'', ''sample_ts'', if_not_exists => TRUE)';
        EXECUTE 'SELECT create_hypertable(''device_events'', ''timestamp'', if_not_exists => TRUE)';
        EXECUTE 'SELECT create_hypertable(''alarms_incidents'', ''timestamp'', if_not_exists => TRUE)';
    END IF;
END $$;
