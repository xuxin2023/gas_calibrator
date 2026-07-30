-- Shared storage PostgreSQL migration history; apply only after 003.
-- Preserve searchable sensor identity history, especially non-unique protocol
-- device IDs that must remain queryable but must not become primary identity.

CREATE TABLE IF NOT EXISTS sensor_identity_aliases (
    id UUID PRIMARY KEY,
    sensor_id UUID NOT NULL REFERENCES sensors(sensor_id) ON DELETE CASCADE,
    alias_type VARCHAR(64) NOT NULL,
    alias_value VARCHAR(128) NOT NULL,
    source_run_id UUID NULL,
    observed_at TIMESTAMPTZ NULL,
    valid_from TIMESTAMPTZ NULL,
    valid_to TIMESTAMPTZ NULL,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    CONSTRAINT uq_sensor_identity_alias_source UNIQUE (sensor_id, alias_type, alias_value, source_run_id)
);

CREATE INDEX IF NOT EXISTS ix_sensor_identity_alias_lookup
    ON sensor_identity_aliases(alias_type, alias_value);

CREATE INDEX IF NOT EXISTS ix_sensor_identity_alias_sensor
    ON sensor_identity_aliases(sensor_id);

CREATE INDEX IF NOT EXISTS ix_sensor_identity_alias_run
    ON sensor_identity_aliases(source_run_id);
