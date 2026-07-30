# Shared Storage Sensor Dimension Plan

## Goal

The V1.5 final product and retained offline analytics need one stable
sensor/device dimension for storage, traceability, and per-device reporting,
without rewriting deployed databases.

This document defines the minimum production direction for that work.

## What is implemented now

The shared `gas_calibrator.storage` layer supports:

- a new `sensors` table
- `sensor_id` on:
  - `samples`
  - `measurement_frames`
  - `fit_results`
  - `coefficient_versions`
- run-level metadata on `runs`:
  - `run_mode`
  - `route_mode`
  - `profile_name`
  - `profile_version`
  - `report_family`
  - `report_templates`
  - `analyzer_setup`
- point-level metadata on `points`:
  - `co2_group`
  - `cylinder_nominal_ppm`

Importer and query paths remain backward-compatible with older data. Formal
user-visible reports are owned by the V1.5 evidence/report chain.

## Identity strategy

### Stable key

The current resolver builds a stable `sensor_id` from a normalized
`device_key`.

Preferred identity order:

1. `channel_type + analyzer_serial`
2. fallback to `channel_type + analyzer_id`

This keeps the key stable across repeated imports while still tolerating
legacy artifacts that do not contain full serial data.

### Missing serial compatibility

Some artifacts, especially fit results or runtime frames, may contain only
`analyzer_id`.

Current compatibility rule:

- if serial is missing, reuse the unique known sensor with the same
  `channel_type + analyzer_id`
- if no unique sensor can be resolved, create a legacy-fallback sensor record

This prevents the same analyzer from being split into multiple sensor records
within one imported run when one artifact has serial data and another does not.

## Table intent

### `sensors`

`sensors` is the stable device/sensor dimension table for shared traceability.

Current fields:

- `sensor_id`
- `device_key`
- `analyzer_id`
- `analyzer_serial`
- `software_version`
- `model`
- `channel_type`
- `metadata`

`metadata` is the compatibility bucket for:

- legacy analyzer labels
- profile linkage
- analyzer setup snapshot
- source artifact hints

### Fact tables

The fact tables still keep legacy analyzer columns for compatibility:

- `samples.analyzer_id / analyzer_serial`
- `measurement_frames.analyzer_id / analyzer_serial / analyzer_label`
- `fit_results.analyzer_id`
- `coefficient_versions.analyzer_id / analyzer_serial`

New `sensor_id` does not replace those fields yet. It adds a stable join path.

## Query and export contract

The shared storage query layer supports:

- `runs_by_sensor(sensor_id)`
- `samples_by_sensor(sensor_id)`
- `measurement_frames_by_sensor(sensor_id)`
- `fit_results_by_sensor(sensor_id)`
- `coefficient_versions_by_sensor(sensor_id)`

The shared query layer remains the authoritative read path for run, sample,
measurement-frame, fit-result, coefficient-version, and sensor history.
The unused legacy database re-exporter has been retired; final user-visible reports,
per-device certificates, artifact hashes, uncertainty, and release gates are
owned by the V1.5 formal evidence/report chain.

## Migration scope

The minimum schema migration for this phase is:

1. create `sensors`
2. add run metadata columns
3. add point metadata columns
4. add nullable `sensor_id` columns to the main fact tables
5. add indexes and foreign keys

The cumulative PostgreSQL migration history is now owned beside the shared
storage implementation:

- [migration governance](../../src/gas_calibrator/storage/migrations/README.md)
- [002_sensor_dimension_and_run_metadata.sql](../../src/gas_calibrator/storage/migrations/002_sensor_dimension_and_run_metadata.sql)

## Compatibility boundary

This shared schema does not claim these things:

- it does not remove `analyzer_id` / `analyzer_serial`
- it does not rewrite `CoefficientVersionStore` to be `sensor_id`-first
- it does not guarantee that every historical artifact can be resolved to a
  single high-confidence physical asset
- it does not introduce a separate enterprise asset-management model

## Next steps

Recommended next storage steps:

1. let coefficient-version writes optionally accept `sensor_id`
2. let analytics/report services prefer `sensor_id` joins over legacy analyzer
   joins
3. enrich `sensors.metadata` and identity aliases with stronger device lineage
   only from reviewed V1.5 bench evidence
4. make per-device product reports consume `sensor_id` as the primary device
   handle
