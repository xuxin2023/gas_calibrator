# V1.5 Stage 8-10 Software Interface Plan

This note records the first implementation boundary for V1.5 stages 8, 9, and
10. These modules are sidecar-first and offline by default. They must not open
COM ports, control the water route, control the gas route, control PACE/valves,
write SENCO, clear SENCO, or modify `run_app.py`.

## Stage 8: Operation Interface

Implemented first as a read-only operation-console model:

- `src/gas_calibrator/v1_5/ui/operation_console.py`
- `src/gas_calibrator/tools/export_v1_5_operation_console.py`

The console has eight formal pages:

1. Dashboard
2. Plan Select
3. Precheck
4. Pressure Channel Verify
5. Open Flow Sampling
6. QC Review
7. Report Review
8. Approval

The first version only displays status, physical signals, calibration gates,
and blockers. It is not a device-control UI. It keeps these hard flags false:

- `opens_com_ports`
- `controls_water_or_gas_routes`
- `controls_valves_or_pace`
- `writes_coefficients`

Export command:

```powershell
$env:PYTHONPATH = "src"
python -m gas_calibrator.tools.export_v1_5_operation_console `
  --output-dir <operation_console_dir> `
  --workbench-json <v1_5_formal_workbench.json> `
  --role operator
```

## Stage 9: Parameter Governance

Implemented first as parameter classification and audit rules:

- `src/gas_calibrator/v1_5/parameters/governance.py`

Parameter levels:

- A: run parameters, written only to run/config snapshots;
- B: QC parameters, engineer/admin plus approval;
- C: standard gas and reference certificate data, traceability required;
- D: controlled device working parameters, readback and rollback required;
- E: high-risk device parameters, hidden by default and not writable in v0.

High-risk examples include:

- `SENCO1`-`SENCO9`
- `CLEARSENCO1`-`CLEARSENCO9`
- `SETPOW`
- `SETILLUM`
- `SETCO2`
- `SETCOM`
- `ID`
- `RESET`

The v0 parameter UI does not write devices. It only validates proposed changes
and builds audit events with old value, new value, actor, role, reason, time,
approval, readback, rollback plan, and decision reasons.

## Stage 10: Advanced QC

Implemented first as small testable QC engines:

- `steady_state_selector.py`
- `humidity_diagnostics.py`
- `factory_signal_health.py`
- `pressure_trend.py`
- `control_charts.py`
- `uncertainty_budget.py`
- `root_cause_classifier.py`

These modules consume already-recorded evidence rows. They explain why a point
is acceptable, review-grade, or rejected. They do not collect samples and do not
change formal fit inputs.

Current outputs include:

- selected steady-state window and metrics;
- pressure-effect versus real-moisture humidity classification;
- factory-mode ratio/signal drift findings;
- analyzer pressure trend against COM22;
- 2-sigma / 3-sigma control-chart flags;
- input-quantity uncertainty budget table;
- root-cause summary in human-readable Chinese.

## Boundary

These stages build the software shape around V1.5 formal calibration:

- operator clarity;
- parameter control;
- QC explainability;
- evidence traceability;
- report review support.

They do not make diagnostic data real acceptance evidence and do not make sealed
pressure points formal CO2/H2O fit inputs by default.

## Advanced QC Exporter

The advanced QC exporter is an offline stage 10 evidence reader. It loads
existing V1.5 sample artifacts, normalizes MODE2/runner fields into the
advanced QC schema, and writes a reviewable JSON/Markdown summary.

Implementation:

- `src/gas_calibrator/v1_5/qc_advanced/exporter.py`
- `src/gas_calibrator/tools/export_v1_5_advanced_qc.py`

Export command:

```powershell
$env:PYTHONPATH = "src"
python -m gas_calibrator.tools.export_v1_5_advanced_qc `
  --run-dir <existing_v1_5_run_dir> `
  --output-dir <advanced_qc_output_dir>
```

Optional pressure quick-check input:

```powershell
python -m gas_calibrator.tools.export_v1_5_advanced_qc `
  --run-dir <existing_v1_5_run_dir> `
  --pressure-quick-check-csv <pressure_channel_quick_check.csv>
```

Outputs:

- `advanced_qc_summary.json`
- `advanced_qc_summary.md`

The summary can be fed into the unified review surface with
`--advanced-qc-json <advanced_qc_summary.json>`.

The exporter preserves the V1.5 formal boundary:

- it opens no COM ports;
- it performs no water/gas route control;
- it performs no PACE, valve, or OUTP control;
- it writes no SENCO/CLEARSENCO or calibration coefficients;
- it analyzes only open-flow rows as formal component-calibration candidates;
- sealed pressure points, dynamic pressure rows, VENT-hold rows, and other
  non-open pressure modes are retained only as excluded diagnostic rows.

## Unified Review Surface

The unified review surface combines the stage 8 operation console, stage 9
parameter governance, stage 10 advanced QC, formal evidence workbench, and
report release gate into one static review entry.

Implementation:

- `src/gas_calibrator/v1_5/review_surface.py`
- `src/gas_calibrator/tools/export_v1_5_review_surface.py`

Export command:

```powershell
$env:PYTHONPATH = "src"
python -m gas_calibrator.tools.export_v1_5_review_surface `
  --output-dir <review_surface_dir> `
  --formal-workbench-json <v1_5_formal_workbench.json> `
  --operation-console-json <v1_5_operation_console.json> `
  --advanced-qc-json <advanced_qc_summary.json> `
  --role reviewer
```

Outputs:

- `v1_5_review_surface.html`
- `v1_5_review_surface.json`
- `v1_5_review_surface.md`

The review surface answers:

- whether the run can enter review;
- which evidence is missing;
- whether parameters remain no-write and high-risk-hidden;
- whether advanced QC found a root cause;
- whether the report is draft, review-ready, or release-ready;
- what the next action should be.

It preserves the same hard boundary:

- no COM;
- no water/gas route control;
- no PACE/valve control;
- no SENCO or coefficient write;
- no diagnostic-to-acceptance promotion.
