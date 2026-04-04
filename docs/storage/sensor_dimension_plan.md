# V2 Sensor Dimension Plan

## Goal

V2 now needs a stable sensor/device dimension for storage, traceability, and
per-device reporting, without rewriting the whole database.

This document defines the minimum production direction for that work.

## What is implemented now

The current V2 storage layer now supports:

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

Importer, query, and exporter paths are backward-compatible with older data.

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

`sensors` is the stable device/sensor dimension table for V2 traceability.

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

The current V2 storage layer now supports:

- `runs_by_sensor(sensor_id)`
- `samples_by_sensor(sensor_id)`
- `measurement_frames_by_sensor(sensor_id)`
- `fit_results_by_sensor(sensor_id)`
- `coefficient_versions_by_sensor(sensor_id)`

Exporter support now includes:

- run bundle summary carrying run/profile/report metadata
- `product_report_manifest.json` with report-family summary and per-device
  sensor list
- `export_sensor_bundle(sensor_id, output_dir)` for traceability exports

## Migration scope

The minimum schema migration for this phase is:

1. create `sensors`
2. add run metadata columns
3. add point metadata columns
4. add nullable `sensor_id` columns to the main fact tables
5. add indexes and foreign keys

That migration is captured in:

- [002_sensor_dimension_and_run_metadata.sql](/D:/gas_calibrator/src/gas_calibrator/v2/storage/migrations/002_sensor_dimension_and_run_metadata.sql)

## Compatibility boundary

This phase does not do these things yet:

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
3. enrich `sensors.metadata` with stronger device lineage once bench workflows
   start assigning V2 device IDs formally
4. make per-device product reports consume `sensor_id` as the primary device
   handle
