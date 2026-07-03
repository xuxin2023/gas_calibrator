-- Add first-class V1.5 gas-analyzer identity columns.
--
-- Existing deployments may already have V1.5 identity values inside
-- sensors.metadata. Keep the new columns nullable so non-V1.5 or legacy sensor
-- rows remain valid, then enforce uniqueness only when a formal code exists.

ALTER TABLE sensors ADD COLUMN IF NOT EXISTS sn_code VARCHAR(32) NULL;
ALTER TABLE sensors ADD COLUMN IF NOT EXISTS device_code VARCHAR(32) NULL;

UPDATE sensors
SET sn_code = NULLIF(metadata->>'sn_code', '')
WHERE sn_code IS NULL
  AND metadata ? 'sn_code'
  AND (metadata->>'sn_code') ~ '^[0-9]{8}$';

UPDATE sensors
SET device_code = NULLIF(metadata->>'device_code', '')
WHERE device_code IS NULL
  AND metadata ? 'device_code'
  AND (metadata->>'device_code') ~ '^[0-9]{8}$';

CREATE UNIQUE INDEX IF NOT EXISTS uq_sensors_sn_code_not_null
    ON sensors(sn_code)
    WHERE sn_code IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS uq_sensors_device_code_not_null
    ON sensors(device_code)
    WHERE device_code IS NOT NULL;

CREATE INDEX IF NOT EXISTS ix_sensors_sn_code ON sensors(sn_code);
CREATE INDEX IF NOT EXISTS ix_sensors_device_code ON sensors(device_code);
