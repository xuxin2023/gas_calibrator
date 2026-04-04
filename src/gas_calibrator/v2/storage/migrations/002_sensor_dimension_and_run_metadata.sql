CREATE TABLE IF NOT EXISTS sensors (
    sensor_id UUID PRIMARY KEY,
    device_key VARCHAR(128) NOT NULL,
    analyzer_id VARCHAR(64) NULL,
    analyzer_serial VARCHAR(64) NULL,
    software_version VARCHAR(64) NULL,
    model VARCHAR(128) NULL,
    channel_type VARCHAR(64) NOT NULL DEFAULT 'co2_h2o_dual',
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_sensors_device_key ON sensors (device_key);
CREATE INDEX IF NOT EXISTS ix_sensors_legacy_identity ON sensors (analyzer_id, analyzer_serial);
CREATE INDEX IF NOT EXISTS ix_sensors_serial ON sensors (analyzer_serial);
CREATE INDEX IF NOT EXISTS ix_sensors_channel_type ON sensors (channel_type);

ALTER TABLE runs ADD COLUMN IF NOT EXISTS run_mode VARCHAR(64) NULL;
ALTER TABLE runs ADD COLUMN IF NOT EXISTS route_mode VARCHAR(64) NULL;
ALTER TABLE runs ADD COLUMN IF NOT EXISTS profile_name VARCHAR(128) NULL;
ALTER TABLE runs ADD COLUMN IF NOT EXISTS profile_version VARCHAR(64) NULL;
ALTER TABLE runs ADD COLUMN IF NOT EXISTS report_family VARCHAR(128) NULL;
ALTER TABLE runs ADD COLUMN IF NOT EXISTS report_templates JSONB NOT NULL DEFAULT '{}'::jsonb;
ALTER TABLE runs ADD COLUMN IF NOT EXISTS analyzer_setup JSONB NOT NULL DEFAULT '{}'::jsonb;

ALTER TABLE points ADD COLUMN IF NOT EXISTS co2_group VARCHAR(32) NULL;
ALTER TABLE points ADD COLUMN IF NOT EXISTS cylinder_nominal_ppm DOUBLE PRECISION NULL;

ALTER TABLE samples ADD COLUMN IF NOT EXISTS sensor_id UUID NULL;
ALTER TABLE measurement_frames ADD COLUMN IF NOT EXISTS sensor_id UUID NULL;
ALTER TABLE fit_results ADD COLUMN IF NOT EXISTS sensor_id UUID NULL;
ALTER TABLE coefficient_versions ADD COLUMN IF NOT EXISTS sensor_id UUID NULL;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_samples_sensor_id') THEN
        ALTER TABLE samples
            ADD CONSTRAINT fk_samples_sensor_id
            FOREIGN KEY (sensor_id) REFERENCES sensors(sensor_id) ON DELETE SET NULL;
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_measurement_frames_sensor_id') THEN
        ALTER TABLE measurement_frames
            ADD CONSTRAINT fk_measurement_frames_sensor_id
            FOREIGN KEY (sensor_id) REFERENCES sensors(sensor_id) ON DELETE SET NULL;
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_fit_results_sensor_id') THEN
        ALTER TABLE fit_results
            ADD CONSTRAINT fk_fit_results_sensor_id
            FOREIGN KEY (sensor_id) REFERENCES sensors(sensor_id) ON DELETE SET NULL;
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_coefficient_versions_sensor_id') THEN
        ALTER TABLE coefficient_versions
            ADD CONSTRAINT fk_coefficient_versions_sensor_id
            FOREIGN KEY (sensor_id) REFERENCES sensors(sensor_id) ON DELETE SET NULL;
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS ix_runs_mode_profile ON runs (run_mode, route_mode, profile_name, profile_version);
CREATE INDEX IF NOT EXISTS ix_points_group_nominal ON points (co2_group, cylinder_nominal_ppm);
CREATE INDEX IF NOT EXISTS ix_samples_sensor_id ON samples (sensor_id);
CREATE INDEX IF NOT EXISTS ix_measurement_frames_sensor_id ON measurement_frames (sensor_id);
CREATE INDEX IF NOT EXISTS ix_fit_results_sensor_id ON fit_results (sensor_id);
CREATE INDEX IF NOT EXISTS ix_coefficient_versions_sensor_id ON coefficient_versions (sensor_id);
