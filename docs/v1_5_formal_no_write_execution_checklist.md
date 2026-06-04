# V1.5 formal no-write execution checklist

This checklist is for the future operator-authorized V1.5 formal no-write run.
It does not authorize a real-device run by itself.

## 1. Prepare traceability files

Create the formal plan snapshot from:

`docs/templates/formal_plan_snapshot_template.json`

Required evidence:

- analyzer under test ID;
- operator;
- runtime config hash;
- CO2/H2O standard source identity;
- certificate value and uncertainty;
- certificate valid-until date;
- supplier;
- certificate file hash;
- `allow_device_write = false`.

Create the COM22 pressure-reference snapshot from:

`docs/templates/com22_pressure_reference_template.json`

Required evidence:

- COM22 device ID;
- certificate ID;
- certificate uncertainty in hPa;
- certificate valid-until date;
- certificate file hash.

If either file is incomplete or expired, the run can only be engineering
diagnostic evidence.

## 2. Offline preflight before any real-device action

First run the readiness assessment. It tells the operator whether the package is
blocked in offline setup, ready for pressure-channel quick-check authorization,
ready for open-flow sampling authorization, or ready for reviewer.

```powershell
python -m gas_calibrator.tools.export_v1_5_formal_readiness `
  --run-dir <planned_or_existing_v1_5_run_dir> `
  --plan-json <formal_plan_snapshot.json> `
  --pressure-reference-json <com22_pressure_reference.json> `
  --config <v1_5_no_write_runtime_config.json>
```

The readiness report is offline. It must not open COM ports, control PACE,
switch valves, switch water/gas routes, or write SENCO. Any status that says
`requires_explicit_v1_5_no_write_authorization` is an execution prompt, not
real acceptance evidence.

For a single offline reviewer package, run the formal offline review chain:

```powershell
python -m gas_calibrator.tools.run_v1_5_formal_offline_review_chain `
  --run-dir <planned_or_existing_v1_5_run_dir> `
  --plan-json <formal_plan_snapshot.json> `
  --pressure-reference-json <com22_pressure_reference.json> `
  --config <v1_5_no_write_runtime_config.json>
```

This chain runs readiness, preflight, and the static review surfaces. If both
the pressure quick-check artifact and open-flow sample artifact exist, it also
builds the evidence bundle, reports, and advanced QC summary. If they do not
exist, it stops at pending/blocked status and does not pretend sampling was
completed.

Run preflight against the intended run directory or latest completed run
artifacts:

```powershell
python -m gas_calibrator.tools.export_v1_5_formal_preflight `
  --run-dir <existing_v1_5_run_dir> `
  --plan-json <formal_plan_snapshot.json> `
  --pressure-reference-json <com22_pressure_reference.json> `
  --config <v1_5_no_write_runtime_config.json>
```

Preflight checks:

- plan contract;
- COM22 pressure-reference contract;
- no-write config;
- sample artifact presence;
- dedicated `pressure_channel_quick_check*.csv` presence;
- formal package readiness.

Preflight is offline. It must not open COM ports or control water/gas routes.

## 3. Pressure-channel quick check

If PACE or COM22 is not physically present, do not run the real pressure
quick-check command. Mark the current bench state in the runtime/config overlay
with `devices.pressure_controller.present=false` and/or
`devices.pressure_gauge.present=false`, then run the offline readiness report.
The expected status is `pressure_hardware_blocked`. In that state, continue
only sidecar work such as evidence review surfaces, report templates, database
imports, or advanced QC over existing artifacts.

Only after explicit operator authorization for V1.5 no-write real-device work:

```powershell
python -m gas_calibrator.tools.validate_pressure_only `
  --config <v1_5_no_write_runtime_config.json> `
  --pressure-reference-json <com22_pressure_reference.json> `
  --pressure-points ambient `
  --count 10 `
  --continuous-atmosphere-hold `
  --require-continuous-atmosphere-hold
```

This produces:

- `pressure_channel_quick_check_<run_id>.csv`;
- `pressure_channel_validation.xlsx`;
- paired analyzer `pressure_kpa` versus COM22 `pressure_hpa` samples.

The ambient quick check must establish the same PACE continuous-atmosphere
condition used by the V1.5 H2O/CO2 open-route workflow before sampling. If the
PACE atmosphere hold cannot be verified, the quick check must stop or remain
engineering diagnostic evidence; it cannot unlock CO2/H2O formal work.

This step does not calibrate CO2/H2O and does not write `SENCO9`.

## 4. Open-flow CO2/H2O run

Run the existing V1.5 open-flow water/gas workflow without changing its route
logic. Preserve:

- PACE atmosphere during open-route sampling;
- water/gas dewpoint stabilization;
- 10 samples per point;
- no-write audit.

Do not add sealed pressure points, VENT-hold, dynamic PACE output, or pressure
compensation validation to the formal CO2/H2O fit input.

## 5. Formal package export

After pressure quick check and open-flow sampling:

```powershell
python -m gas_calibrator.tools.export_v1_5_formal_calibration_package `
  --run-dir <v1_5_run_dir> `
  --plan-json <formal_plan_snapshot.json> `
  --pressure-reference-json <com22_pressure_reference.json>
```

The package is ready only when:

- plan traceability passes;
- COM22 traceability passes;
- dedicated pressure quick-check artifact exists and passes;
- open-flow A-grade samples are sufficient;
- rejected samples have explicit reasons.

The package never writes analyzer coefficients. It only outputs
`ready_for_reviewer` or `blocked`.

## 6. Candidate coefficient review

Candidate review may use only A-grade open-flow samples by default.

Not default fit inputs:

- sealed pressure CO2/H2O rows;
- dynamic pressure-control probes;
- PACE continuous sink rows;
- VENT-hold rows;
- pressure compensation validation rows.

Any device write, including `SENCO9`, requires a separate explicit
authorization and a separate evidence package.

Analyzer `ID` writes are also excluded from the formal V1.5 calibration flow.
The analyzer's own sensor ID is the traceability identity; `ga01` ... `ga08`
are only acquisition-channel labels. Do not rewrite an analyzer ID to make it
match the current COM-port wiring.
