# V1.5 Evidence Registry Database

This database is an evidence-chain index for V1.5 formal calibration. It is not
part of the water-route or gas-route runner, and it must not open COM ports,
control PACE, switch valves, or write SENCO coefficients.

## Purpose

The registry answers these review questions:

- Which formal plan, standard gases, pressure reference, and analyzer were used?
- Which raw files and sidecar reports form the evidence package?
- What are the SHA-256 hashes of every evidence artifact?
- Which open-flow samples are A-grade, review-grade, or rejected?
- Why were points rejected?
- Did the pressure-channel quick check prove analyzer internal pressure P is
  fit for CO2/H2O formal work?
- Which candidate coefficient reviews are ready, blocked, or still missing
  evidence?
- Was any coefficient write attempted?

Raw frames remain in the file evidence package. PostgreSQL stores the index,
hashes, summaries, QC outcomes, traceability links, candidate-review state, and
audit events.

## Schema

The schema name is `v1_5_evidence`. Core tables are:

- `runs`
- `devices`
- `run_devices`
- `standard_gases`
- `reference_certificates`
- `calibration_points`
- `sample_files`
- `qc_results`
- `coefficient_snapshots`
- `coefficient_candidates`
- `coefficient_write_events`
- `reports`
- `audit_events`
- `evidence_integrity_checks`

The view `v1_5_evidence.run_evidence_summary` gives one-row reviewer status per
run.

## Migration

Use a DSN through the environment or CLI. Do not put database passwords in code
or committed config files.

```powershell
$env:PYTHONPATH = "src"
$env:GAS_CAL_DB_DSN = "postgresql://postgres:<password>@localhost:5432/gas_calibrator"
python -m gas_calibrator.tools.migrate_v1_5_evidence_db
```

For DBA review without connecting:

```powershell
$env:PYTHONPATH = "src"
python -m gas_calibrator.tools.migrate_v1_5_evidence_db --dry-run --print-sql
```

## Import

The importer builds a bundle from existing artifacts only:

```powershell
$env:PYTHONPATH = "src"
python -m gas_calibrator.tools.import_v1_5_evidence_package `
  --run-dir <existing_v1_5_run_dir> `
  --plan-json <formal_plan_snapshot.json> `
  --pressure-reference-json <com22_pressure_reference.json> `
  --apply-migrations
```

Dry-run export without database access:

```powershell
$env:PYTHONPATH = "src"
python -m gas_calibrator.tools.import_v1_5_evidence_package `
  --run-dir <existing_v1_5_run_dir> `
  --plan-json <formal_plan_snapshot.json> `
  --pressure-reference-json <com22_pressure_reference.json> `
  --output-json <evidence_bundle.json> `
  --summary-json <evidence_summary.json> `
  --dry-run
```

## Import Pressure-Channel Completion Evidence

Pressure-channel completion is a separate evidence package from the open-flow
CO2/H2O main calibration package. It indexes the controlled SENCO9 pressure
channel write, old/readback GETCO9 snapshots, post-write pressure verification,
and COM22 certificate evidence. It must not be treated as CO2/H2O fitting
evidence by itself.

```powershell
$env:PYTHONPATH = "src"
$env:GAS_CAL_DB_DSN = "postgresql://postgres:<password>@localhost:5432/gas_calibrator"
python -m gas_calibrator.tools.import_v1_5_pressure_channel_completion_package `
  --completion-dir <pressure_channel_completion_dir> `
  --output-json <pressure_channel_completion_evidence_bundle.json> `
  --summary-json <pressure_channel_completion_db_import_summary.json> `
  --apply-migrations
```

Dry-run without database access:

```powershell
$env:PYTHONPATH = "src"
python -m gas_calibrator.tools.import_v1_5_pressure_channel_completion_package `
  --completion-dir <pressure_channel_completion_dir> `
  --output-json <pressure_channel_completion_evidence_bundle.json> `
  --summary-json <pressure_channel_completion_evidence_summary.json> `
  --dry-run
```

Physical boundary:

- analyzer identity is the reported device/sensor ID; `ga01` ... `ga08` remain
  acquisition-channel labels only;
- the import reads files only and does not open COM ports, control PACE, switch
  valves, or touch water/gas routes;
- SENCO9 write events are indexed as pressure-channel write audit records;
- `standard_gases` remains empty for this pressure-only package;
- CO2/H2O candidate review remains blocked until open-flow component samples and
  standard-gas evidence are imported separately.

## Prepare A Formal Evidence Run

Before a real no-write V1.5 run, create the formal templates,
plan/reference/manifest snapshots, and the operator runbook. This step only
writes files; it does not open COM ports, control water/gas routes, control
PACE/valves, or write coefficients.

One-command package preparation:

```powershell
$env:PYTHONPATH = "src"
python -m gas_calibrator.tools.prepare_v1_5_formal_run_package `
  --output-dir <formal_run_package_dir> `
  --operator <operator-name> `
  --analyzer-id <analyzer-id> `
  --run-id <planned-run-id> `
  --config <v1_5_no_write_runtime_config.json> `
  --standard-gases-json <reviewed_standard_gases.json> `
  --pressure-reference-json <reviewed_com22_pressure_reference.json>
```

Outputs:

- `formal_plan_snapshot_template.json`
- `standard_gases_template.json`
- `com22_pressure_reference_template.json`
- `released_uncertainty_inputs_template.json`
- `formal_plan_snapshot.json`
- `com22_pressure_reference.json`
- `evidence_run_manifest.json`
- `v1_5_formal_no_write_runbook.md`

The reviewed JSON arguments are optional. If they are omitted, the tool uses
the fill-in templates and the snapshots remain a draft skeleton. Fill the
standard-gas and COM22 certificate templates, then regenerate the package with
the reviewed JSON files before sampling. Keep
`released_uncertainty_inputs_template.json` unreleased until the GUM budget is
reviewed; while it is incomplete, reports must remain `draft_only`.

If reviewed standard-gas and pressure-reference JSON files already exist, the
lower-level snapshot tool can still be used directly:

```powershell
$env:PYTHONPATH = "src"
python -m gas_calibrator.tools.prepare_v1_5_formal_evidence_run `
  --output-dir <formal_evidence_dir> `
  --operator <operator-name> `
  --analyzer-id <analyzer-id> `
  --run-id <planned-run-id> `
  --config <v1_5_no_write_runtime_config.json> `
  --standard-gases-json <standard_gases.json> `
  --pressure-reference-json <com22_pressure_reference_source.json>
```

Outputs:

- `formal_plan_snapshot.json`
- `com22_pressure_reference.json`
- `evidence_run_manifest.json`

`allow_device_write` is forced to `false`. The manifest records
`opens_com_ports=false`, `controls_water_or_gas_routes=false`, and
`writes_coefficients=false`.

## Formal Readiness Assessment

Before any real-device no-write action, run the offline readiness assessment:

```powershell
$env:PYTHONPATH = "src"
python -m gas_calibrator.tools.export_v1_5_formal_readiness `
  --run-dir <planned_or_existing_v1_5_run_dir> `
  --plan-json <formal_plan_snapshot.json> `
  --pressure-reference-json <com22_pressure_reference.json> `
  --config <v1_5_no_write_runtime_config.json>
```

Outputs:

- `formal_readiness.json`
- `formal_readiness.md`
- `formal_readiness.xlsx`

The readiness report is an operator/engineer gap report. It says whether the
run is blocked in offline setup, ready for pressure quick-check authorization,
ready for open-flow sampling authorization, ready for reviewer, or blocked by
evidence. It reads files only and does not connect to COM ports, PACE, valves,
water routes, gas routes, or coefficient-write commands.

## Formal Offline Review Chain

To package all available offline review surfaces in one directory:

```powershell
$env:PYTHONPATH = "src"
python -m gas_calibrator.tools.run_v1_5_formal_offline_review_chain `
  --run-dir <planned_or_existing_v1_5_run_dir> `
  --plan-json <formal_plan_snapshot.json> `
  --pressure-reference-json <com22_pressure_reference.json> `
  --config <v1_5_no_write_runtime_config.json>
```

The chain writes:

- `formal_offline_review_chain_summary.json/.md`;
- readiness report;
- preflight sidecar output;
- evidence bundle and calibration reports when required evidence exists;
- advanced QC summary when samples exist;
- formal workbench;
- operation console;
- unified review surface.

It is an offline orchestrator. It must report missing pressure quick-check or
open-flow samples as pending/blocked rather than fabricating evidence.

## One-Step Sidecar Chain

After the pressure quick-check and open-flow samples exist, the sidecar chain
can run preflight, formal package export, evidence bundle generation, and
optional DB import:

```powershell
$env:PYTHONPATH = "src"
$env:GAS_CAL_DB_DSN = "postgresql://postgres:<password>@localhost:5432/gas_calibrator"
python -m gas_calibrator.tools.run_v1_5_formal_evidence_sidecar `
  --run-dir <existing_v1_5_run_dir> `
  --plan-json <formal_plan_snapshot.json> `
  --pressure-reference-json <com22_pressure_reference.json> `
  --config <v1_5_no_write_runtime_config.json> `
  --import-db `
  --apply-migrations
```

Before sampling, use the same tool as preflight-only:

```powershell
$env:PYTHONPATH = "src"
python -m gas_calibrator.tools.run_v1_5_formal_evidence_sidecar `
  --run-dir <planned_or_existing_run_dir> `
  --plan-json <formal_plan_snapshot.json> `
  --pressure-reference-json <com22_pressure_reference.json> `
  --config <v1_5_no_write_runtime_config.json> `
  --stage preflight
```

## Formal Evidence Workbench

The V1.5 workbench is a static offline evidence surface for operators,
engineers, reviewers, and approvers. It is not a device-control UI and does not
connect to the water route, gas route, PACE, valves, COM ports, or SENCO writes.

```powershell
$env:PYTHONPATH = "src"
python -m gas_calibrator.tools.export_v1_5_formal_workbench `
  --output-dir <workbench_output_dir> `
  --run-dir <existing_v1_5_run_dir> `
  --plan-json <formal_plan_snapshot.json> `
  --pressure-reference-json <com22_pressure_reference.json> `
  --config <v1_5_no_write_runtime_config.json> `
  --evidence-bundle-json <evidence_bundle.json> `
  --sidecar-summary-json <formal_evidence_sidecar_summary.json>
```

Outputs:

- `v1_5_formal_workbench.html`
- `v1_5_formal_workbench.json`
- `v1_5_formal_workbench.md`

The workbench shows sidecar/no-write boundary, formal execution order, standard
gas and COM22 readiness, pressure quick-check status, open-flow A/B/rejected
sample counts, candidate-review blockers, report release decision, uncertainty
status, and database import status. It is generated from evidence artifacts and
can be opened directly in a browser; it cannot be used as real acceptance
evidence by itself.

## Query

```powershell
$env:PYTHONPATH = "src"
python -m gas_calibrator.tools.query_v1_5_evidence_run --run-id <run_id>
```

## Calibration Reports

Reports are generated from the evidence bundle, not by hand. The generator
creates three report layers:

- run report: operator/engineer status, point pass/fail, candidate review state;
- technical report: open-flow QC, pressure-channel check, rejected rows, hashes;
- formal calibration report: scope, method, standards, results, uncertainty
  budget, traceability, coefficient-write statement, limitations.

```powershell
$env:PYTHONPATH = "src"
python -m gas_calibrator.tools.export_v1_5_calibration_reports `
  --evidence-bundle-json <evidence_bundle.json> `
  --output-dir <report_output_dir> `
  --report-no <report-number> `
  --reviewer <reviewer-name> `
  --approver <approver-name> `
  --location <calibration-location> `
  --calibration-date <YYYY-MM-DD> `
  --uncertainty-json <released_uncertainty_inputs.json>
```

Outputs:

- `run_report.md` / `.docx` / `.pdf`
- `technical_report.md` / `.docx` / `.pdf`
- `formal_calibration_report.md` / `.docx` / `.pdf`
- `report_model.json`

The formal report must explicitly state:

- open-flow current-atmosphere CO2/H2O is the formal main calibration scope;
- sealed pressure points and dynamic pressure diagnostics are excluded from
  formal CO2/H2O fitting by default;
- pressure channel validation status;
- pressure-compensation coverage or non-coverage;
- whether coefficients were written;
- blocked evidence cannot be issued as a formal calibration certificate;
- uncertainty rows with missing inputs remain `not_evaluated` / `not_released`.

The report release gate writes `report_release_decision` into
`report_model.json`:

- `blocked`: the evidence package or pressure channel cannot support formal
  review;
- `draft_only`: evidence may be readable, but the uncertainty budget is not
  released;
- `review_ready`: released uncertainty exists, but reviewer/approver signatures
  are still pending;
- `formal_release_ready`: evidence, pressure, uncertainty, and signatures pass;
- `not_releasable`: a high-risk condition exists, such as an unaudited
  coefficient write event.

Any status other than `formal_release_ready` prints
`DRAFT / NOT FOR FORMAL ISSUE` in the generated reports.

## Traceability Query

After an evidence bundle is imported, the registry must be able to answer the
physical traceability question:

```text
Which standard gas, COM22 certificate, raw frames, QC decisions, candidate
coefficient rows, no-write records, and report artifacts support this run?
```

Use:

```powershell
python -m gas_calibrator.tools.query_v1_5_evidence_run `
  --run-id <run_id> `
  --traceability
```

To find every run that references a specific artifact hash:

```powershell
python -m gas_calibrator.tools.query_v1_5_evidence_run `
  --artifact-sha256 <sha256>
```

Physical meaning:

- standard gas rows establish the CO2/H2O component reference values;
- H2O standard or humidity-reference rows establish the water-route reference,
  not just a secondary display value;
- COM22 rows establish the pressure reference for analyzer pressure P;
- analyzer `device_id` / sensor ID is the instrument identity used for
  traceability and database review; `ga01` ... `ga08` are acquisition-channel
  labels for the current wiring only and must not be treated as stable
  instrument identity;
- analyzer identity writes are not part of the V1.5 formal calibration flow;
  an `ID` command requires a separate identity-maintenance authorization and
  cannot be used to make a channel label match the connected instrument;
- raw sample artifacts preserve MODE2 and factory signal evidence, including
  dewpoint, H2O dry/wet ppmv, H2O ratio, H2O signal, and H2O mmol/mol fields;
- pressure quick-check artifacts prove P was checked independently before
  component fitting;
- QC rows explain why samples were A-grade, B-grade, or rejected;
- candidate coefficient rows are review evidence only and keep
  `auto_write_allowed=false`;
- coefficient write events remain `not_attempted` unless a separate future
  write workflow is explicitly authorized.

These queries still do not create real acceptance evidence. They only make the
evidence chain inspectable after import.

The optional uncertainty JSON uses this shape:

```json
{
  "released": true,
  "coverage_factor": 2.0,
  "release_basis": "reviewed GUM budget",
  "inputs": [
    {
      "component": "CO2",
      "input_quantity": "standard_gas_certificate",
      "distribution": "normal",
      "standard_uncertainty": 0.9,
      "sensitivity_coefficient": 1.0,
      "status": "released",
      "evidence_source": "CO2 certificate"
    }
  ]
}
```

The released budget must cover the required quantities in the report model,
including standard gas certificate uncertainty, repeatability, fit residual,
analyzer resolution, pressure-channel bias, temperature effect, sampling
stability, and H2O humidity/water-vapor correction terms where applicable.

## Formal Boundaries

- `pressure_channel_quick_check*.csv` is required by default.
- Missing COM22 traceability blocks formal pressure validation.
- Candidate coefficients are reviewer material only.
- Automatic coefficient writes are always recorded as disabled in this importer.
- If old GETCO/coefficient snapshots are missing, the integrity check is a
  warning. They should be attached before any future coefficient approval.
- Database import is not real acceptance evidence by itself; it indexes the
  evidence package so the result can be reviewed and rebuilt.
