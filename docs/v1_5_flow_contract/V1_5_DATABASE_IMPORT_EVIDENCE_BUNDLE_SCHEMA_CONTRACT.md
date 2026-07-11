# V1.5 database import evidence-bundle schema contract

This contract is an offline gate for the future PostgreSQL 18 controlled import path. It does not connect to PostgreSQL, open COM ports, write analyzers, or control pressure, gas, or water routes.

## Required registry shape

- `schema` must be `v1_5_evidence_registry`.
- `schema_version` must be `001`.
- Top-level `run_id` and `run_db_id` must be present and match the single row in `tables.runs`.
- Every registry table named by `TABLE_NAMES` must be present as an array.
- Physical evidence, identity, calibration, QC, report, audit, and integrity tables must be non-empty.
- The run evidence and package statuses must be ready for reviewer/release use, not blocked or merely indexed.

## Required artifact roles

The frozen `tables.sample_files` index must contain hashed, absolute-path evidence for:

- `raw_samples`
- `formal_plan_snapshot`
- `pressure_reference_snapshot`
- either `pressure_channel_quick_check` or `pressure_channel_completion`
- `run_evidence_status`
- `formal_run_status`
- `formal_calibration_report`

Every row marked `required=true` must have an artifact id, absolute path, and 64-character SHA-256. Duplicate artifact ids and failed error-severity integrity checks block command-contract readiness.

## Two-stage enforcement

1. The database import command contract validates the schema and roles before freezing the evidence-bundle path and SHA-256.
2. The blocked executor independently reloads and revalidates the same bundle after checking its frozen path and SHA-256.

This prevents a structurally empty or role-incomplete JSON file from becoming database-import evidence merely because its file hash is stable.
