# V2 Product Report Templates

## Goal

V2 is moving from generic run exports toward four product-facing report families:

- CO2 test report
- CO2 calibration report
- H2O test report
- H2O calibration report

Only `auto_calibration` mode should produce the formal calibration reports.
`co2_measurement`, `h2o_measurement`, and `experiment_measurement` remain
test / measurement flows and must not be forced through the formal calibration
report chain.

## Current Boundary

Current V2 reporting/export capabilities are split across three layers:

1. `storage.exporter`
   - run-level raw exports such as summary, points, samples, and QC
   - good as evidence/raw data
   - not a final product report family
2. `export.ratio_poly_report`
   - calibration-oriented workbook export
   - quality analysis and coefficient-centric output
   - still closer to an engineering workbook than the final product templates
3. `core.run_manifest`
   - captures run mode, route mode, and report policy
   - now also records which product report templates apply to the run
   - now preserves `profile_name`, `profile_version`, `report_family`, and
     `analyzer_setup` so product reports can trace back to the plan/runtime
     contract

## Template Direction

The new skeleton in `v2.export.product_report_plan` defines the target report
family without rewriting existing exporters yet.

### Per-device output

Each report family is planned as per-device output, not one mixed workbook for
all analyzers.

Planned file stubs:

- `reports/co2_test/{analyzer}.xlsx`
- `reports/co2_calibration/{analyzer}.xlsx`
- `reports/h2o_test/{analyzer}.xlsx`
- `reports/h2o_calibration/{analyzer}.xlsx`

Current bridge artifact:

- `product_report_manifest.json`
  - exported with each run bundle
  - records `report_family`
  - records `report_templates`
  - records the per-device sensor list currently visible in storage/export
  - acts as the stable handoff between current engineering exports and later
    final product workbooks

### Mode gating

- `auto_calibration`
  - enables test reports
  - enables calibration reports
- `co2_measurement`
  - CO2 test report only
- `h2o_measurement`
  - H2O test report only
- `experiment_measurement`
  - test-report family only, depending on route mode

## What This Does Not Do Yet

- It does not replace `ratio_poly_report`.
- It does not yet generate the four final product workbooks.
- It does not yet split every existing export artifact by analyzer/device.

This step only fixes the product model and manifest contract so later exporter
work can land without changing run-mode semantics again.

## Current per-device boundary

V2 does not yet generate four final per-device Excel workbooks automatically.

What it does provide now:

- run-level raw exports with profile/report metadata
- sensor-aware traceability export via `export_sensor_bundle(...)`
- `product_report_manifest.json` as the per-device report planning artifact
- first real per-device exporter for `H2O calibration report`
  - gated by `auto_calibration` mode
  - emitted under `reports/h2o_calibration/{device}.json`
  - linked from `product_report_manifest.json`
  - intentionally JSON-first for now, while the final workbook template is still being designed

This is enough to keep storage, manifest, and reporting direction aligned while
the final workbook templates are still under construction.
