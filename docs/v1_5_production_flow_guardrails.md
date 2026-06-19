# V1.5 production flow guardrails

This note preserves the V1.5 route and pressure-control lessons from the
2026-05-24 recovery work. It exists so future changes do not rely on chat
history or memory.

## Scope

- Applies to V1.5 production-calibration flow.
- Does not promote V2.
- Does not change `run_app.py`.
- Real COM validation for V1.5 remains operator-authorized, no-write unless
  explicitly approved otherwise.

## Baseline that must be preserved

1. Open-route sampling for H2O and CO2 runs with the PACE kept at atmosphere.
   The atmosphere/VENT hold is part of the V1.5 open-route baseline, not an
   error by itself.
2. Open-route dewpoint stabilization must remain in place. Do not remove or
   bypass the water/gas open-route dewpoint stability logic.
   For formal CO2 main-calibration sampling, stability alone is not enough:
   the gas route must be dried to the configured dry-enough threshold, whose
   current formal default is `dewpoint <= -28 C`, before the formal sample
   window can start. Nitrogen pre-purge may be used as a conditioning step to
   dry tubing and remove humidity-route memory, but N2 pre-purge evidence is
   not a CO2 standard point and must not enter CO2 fitting.
3. Before sealed pressure control, the flow must exit atmosphere first:
   stop continuous atmosphere hold, send VENT off, wait the configured
   1.5 s window, seal/close the route valves, then enable PACE output for
   sealed control.
4. During open-route sampling, PACE queries can disturb atmosphere behavior.
   Prefer route evidence and the digital pressure gauge fallback when judging
   whether the route is physically at atmosphere.
5. Current-atmosphere open-route points are valid production candidates when
   analyzer freshness, dewpoint stability, route state, and no-write audit pass.
6. Sealed pressure points must not be blocked by V2-style idealized gates that
   do not match the old V1.5 field behavior.

## H2O humidity-generator flow boundary

For V1.5 water-route calibration, the humidity-generator flow is treated as
device-internal control evidence, not as a formal hard gate by default.

- Do not force a flow target during normal V1.5 H2O open-flow sampling unless
  the operator explicitly requests a diagnostic trial.
- If an explicit flow target is requested, record the requested value and
  readback trend as evidence only; do not reject the formal sample solely
  because the flow readback is outside a target tolerance.
- Do not set humidity-generator flow to zero during safe stop by default. Safe
  stop should close the H2O route and stop generator control/heating/cooling;
  flow readback can be recorded for review but should not be the default
  acceptance gate.
- Formal H2O eligibility remains based on the physical water-vapor evidence:
  dewpoint/humidity reference consistency, H2O signal stability, analyzer
  MODE2 frame quality, open-flow route state, and no-write audit.

This boundary prevents software from fighting the humidity generator's own
internal control loop. Flow remains useful operational evidence, but it is not
the water-vapor standard value and must not replace the dewpoint/humidity
validity gates.

## Formal V1.5 main-flow definition

From 2026-05-24 onward, the V1.5 formal calibration main flow is:

1. Prove the CO2/H2O calibration gas is clean and stable in open flow at
   current atmosphere.
2. Validate whether the analyzer internal pressure channel `P` is trustworthy;
   this is an independent check, not a component-fit source.
3. Run pressure-compensation validation only after component stability is proven;
   it is optional and downstream of the component calibration.

The core route is therefore not "refit CO2/H2O with contaminated sealed
pressure points." Formal CO2/H2O fitting starts from clean, stable open-flow
component evidence. Pressure `P` trust and pressure compensation remain
separate validation layers.

The following remain engineering diagnostics and must not be default inputs to
formal CO2/H2O calibration fitting or real acceptance:

- sealed-route multi-pressure CO2/H2O sampling;
- long open-route dynamic control through PACE OUTPUT;
- PACE ACT plus sink bias as the primary pressure-point generator;
- VENT-hold pressure-point control.

The code default for ratio-poly formal auto-fit therefore keeps only legacy
open-flow rows and `PressureMode=ambient_open` rows whose fit role is blank or
explicitly component-calibration:

- `coefficients.ratio_poly_fit.formal_pressure_modes = ["", "ambient_open"]`
- `coefficients.ratio_poly_fit.formal_fit_roles = ["", "component_calibration", "formal_component_calibration", "main_component_calibration", "primary_component_calibration"]`
- `coefficients.ratio_poly_fit.include_sealed_pressure_points_in_formal_fit = false`

Sealed pressure points, dynamic pressure probes, PACE continuous sink, and
VENT-hold evidence may be retained as diagnostic artifacts, but they must stay
outside formal CO2/H2O fit inputs unless a future, explicit, reviewed opt-in is
added. Such diagnostic artifacts are not real acceptance evidence.
Analyzer pressure-channel validation and pressure-compensation validation rows
must also stay outside formal CO2/H2O fit inputs by default.

## MODE2 data contract and QC boundary

The gas analyzer is operated in factory `MODE2`. All MODE2 evidence must be
retained losslessly for V1.5:

- keep the original analyzer frame string;
- keep the full token sequence as JSON;
- keep a field mapping JSON with both ordinal fields and known semantic names;
- keep known semantic fields for CO2/H2O concentration, density, filtered/raw
  ratios, REF/CO2/H2O signals, chamber/case temperature, and analyzer pressure;
- keep optional status and any future/extra tokens rather than dropping them.

These MODE2 fields are quality and traceability evidence. They support analyzer
health checks, pressure-channel validation, stability review, drift diagnosis,
signal saturation/low-signal diagnosis, and future reprocessing. Retaining all
MODE2 data does not mean every field becomes a formal CO2/H2O fit input.

Default V1.5 fitting policy remains:

- use clean, stable open-flow component-calibration evidence for formal CO2/H2O
  fitting;
- use MODE2 internal pressure `P` for independent pressure-channel validation;
- use sealed pressure, dynamic-control, and pressure-compensation rows only as
  diagnostics or explicit downstream validation unless a reviewed opt-in changes
  the formal fit scope.

## Formal open-flow runner order

Development may build the open-flow sampling runner and pressure-channel tool
as separate modules, but formal execution order must remain:

1. `LOAD_PLAN`
2. `PRECHECK`
3. `PRESSURE_CHANNEL_QUICK_CHECK`
4. `OPEN_FLOW_PURGE`
5. `STABILITY_GATE`
6. `SAMPLE_WINDOW`
7. `QC_CLASSIFICATION`
8. `POINT_REVIEW`
9. `NEXT_POINT_OR_FINISH`
10. `RUN_SUMMARY`

The pressure-channel quick check is intentionally before open-flow component
sampling. Analyzer pressure `P` is part of the analyzer's internal CO2/H2O
calculation model, so an untrusted pressure channel must block formal
coefficient-write decisions and downgrade the run to diagnostic evidence.

The formal open-flow runner must produce, at minimum:

- traceable raw sample rows, including full MODE2 evidence;
- a pressure-channel quick-check result comparing analyzer `P` to an external
  reference pressure;
- A-grade samples eligible for candidate coefficient fitting;
- rejected samples with explicit reasons;
- a run summary explaining whether candidate fitting is allowed.

A-grade means the sample came from open-flow component calibration evidence,
the MODE2 contract and frame QC passed, the pressure-channel quick check passed,
and the point/window stability gates did not flag the row. Rejected samples must
remain visible in the artifacts; they must not be silently deleted just because
they make a fit look worse.

## Formal open-flow sidecar report

The first production-safe implementation is a sidecar exporter:

```powershell
python -m gas_calibrator.tools.export_v1_5_formal_open_flow_report `
  --run-dir <existing_v1_5_run_dir> `
  --plan-json <formal_plan_snapshot.json> `
  --pressure-reference-json <com22_pressure_reference.json>
```

This tool only reads existing sample artifacts and writes report artifacts. It
does not open COM ports, control valves, control PACE, switch routes, or write
analyzer coefficients. Its outputs are:

- `run_summary.csv` / workbook sheet;
- `pressure_check.csv`;
- `a_grade_samples.csv`;
- `b_grade_review_samples.csv`;
- `rejected_samples.csv`;
- `formal_fit_boundary.csv`;
- metadata noting the sidecar-only boundary.

If no dedicated pressure quick-check artifact is present, the exporter can fall
back to pressure evidence in the sample rows so old runs can still be reviewed.
That fallback is a review aid, not a replacement for the required formal run
order where `PRESSURE_CHANNEL_QUICK_CHECK` happens before open-flow sampling.
If the pressure-reference snapshot is absent or invalid, the open-flow report
must keep candidate fitting blocked with
`pressure_reference_traceability_failed`.

## CO2 coefficient model boundary

The analyzer manual makes the CO2 coefficient chain broader than a simple
`SENCO1` write:

- `SENCO1`: CO2 density/ratio primary response coefficients;
- `SENCO3`: CO2 ratio temperature-compensation coefficients;
- `SENCO5`: CO2-family A/B final output linear correction, observed and
  treated in V1.5 as `corrected_concentration = concentration*C1 + C0`;
- `SENCO9`: pressure input calibration, handled separately by the pressure
  channel workflow.

Therefore a V1.5 CO2 candidate must not be treated as a complete device-write
contract just because `SENCO1` and `SENCO3` can be fitted from open-flow point
means. `SENCO5` is in CO2 model scope and must be explicitly reviewed. It
must not be silently preserved when old A/B values are non-neutral. It remains
separate from the `SENCO1`/`SENCO3` optical-temperature fit, and any write needs
its own final-linear-trim evidence, readback, and verification.

The current V1.5 no-write CO2 candidate is allowed to review ratio and
temperature terms from clean open-flow evidence, but it must keep coefficient
writes blocked when any of these are missing:

- confirmed firmware formula path: direct ratio `R` versus zero-gas
  `R0(K)`/normalized absorbance/filtered `AbsFinal`;
- old `GETCO1`, `GETCO3`, and any required CO2-family snapshot needed for
  rollback;
- explicit `SENCO5` final-linear-trim preserve/neutralize decision;
- independent post-write verification plan.

This preserves the physical meaning of CO2 calibration: the fitted output must
match the analyzer firmware's real calculation chain, not only an offline
regression that happens to fit the sampled point means.

### V1.5 fixed CO2 concentration formula contract

The reviewed V1.5 CO2 concentration contract is:

- fitted input data come from open-flow, QC-approved point means;
- fitted dependent variable is the standard-gas CO2 amount fraction /
  concentration target;
- fitted independent variables are analyzer CO2 ratio `R` and chamber
  temperature `T` in kelvin;
- pressure `P` is a validated input from the independent pressure-channel
  workflow and is not a fitted CO2 main-calibration dimension;
- sealed or contaminated pressure points are not allowed in the CO2/H2O main
  calibration fit.

The candidate polynomial is therefore:

```text
CO2_target ~= a0 + a1*R + a2*R^2 + a3*R^3 + a4*T + a5*T^2 + a6*R*T
```

The write mapping is:

```text
SENCO1 = [a0, a1, a2, a3, 0, 0]
SENCO3 = [a4, a5, a6, 0, 0, 0]
SENCO9 = handled only by the pressure-channel workflow
```

`a5` in this polynomial is the `T^2` coefficient. It is not `SENCO5`.
`SENCO5` is the separate CO2 final-output linear trim. A run that writes only
`SENCO1`/`SENCO3` must state whether existing `SENCO5` is neutral, preserved, or
blocked for a separate controlled write.

### SENCO5 final linear-trim contract

`SENCO5` is not ignored. It is deliberately separated from the CO2
optical-temperature candidate because it is an output-stage affine correction,
not a ratio-polynomial term:

```text
CO2_output_after_SENCO5 = CO2_output_before_SENCO5 * C1 + C0
```

The legacy V1/V2 ratio-polynomial algorithm:

```text
Y = a0 + a1*R + a2*R^2 + a3*R^3 + a4*T + a5*T^2 + a6*R*T + a7*P + a8*R*T*P
```

maps CO2 terms to `SENCO1` and `SENCO3`. It does not calculate or identify
`SENCO5`. Therefore the old algorithm is reusable as the main concentration
fitting foundation, but it is not a SENCO5 final-linear-trim method.

A future `SENCO5` write is allowed only under this separate contract:

1. The fitted target must be a small final CO2 concentration residual after the
   `SENCO1`/`SENCO3` main optical-temperature model has already passed review.
   `SENCO5` must not be used to hide a bad ratio fit, bad pressure channel, bad
   temperature channel, unstable route, or wrong standard gas value.
2. The neutral target is `C0=0.0, C1=1.0`. Any non-neutral target must be
   justified as a final affine trim and verified against independent standard
   gas points.
3. Eligible rows must be open-flow A-grade point means with full MODE2
   concentration, ratio/signal, pressure, temperature, and certificate evidence.
4. Pressure channel `SENCO9` and temperature channels `SENCO7`/`SENCO8` must be
   accepted first. Devices with bad internal temperature evidence, such as a
   channel stuck near an impossible value, cannot be used for `SENCO5`.
5. The write protocol must be confirmed by old `GETCO5` backup, command ACK,
   and post-write `GETCO5` readback. A `2026-05-29` direct read probe showed
   that `SENCO5,YGAS,FFF` without coefficients returned `YGAS,<id>,F` on the
   tested analyzers, so it is not a valid V1.5 readback command. The reliable
   readback path remains `GETCO,YGAS,<target>,5`. A subsequent controlled
   probe showed that neutralizing a non-neutral final trim is done with
   `CLEARSENCO5,YGAS,FFF`, verified by `GETCO5` returning `C0:0,C1:1`.
6. Do not fit `SENCO5` from sealed pressure diagnostics, pressure-compensation
   rows, wet/unstable rows, or any row where concentration/pressure/temperature
   evidence is unstable.
7. Any future non-neutral `SENCO5` write requires old `GETCO5` backup,
   controlled A/B write, readback, rollback plan, and independent CO2
   verification. If the objective is only to remove an old trim, prefer the
   controlled `CLEARSENCO5` neutralization path. If any command ACKs but
   `GETCO5` does not change as expected, treat the action as failed and stop
   before expanding to more devices.

This contract keeps the physical chain clean:

```text
SENCO9: pressure input P
SENCO7/SENCO8: temperature inputs
SENCO1/SENCO3: CO2 concentration / ratio-temperature response
SENCO5: final CO2 concentration affine trim, only under a separate reviewed workflow
```

## Pressure-channel validation

Analyzer internal pressure `P` is a measurement-model input for the analyzer
CO2/H2O outputs. It must be validated independently from component calibration
because a wrong `P` can make CO2/H2O concentration, density, and later pressure
compensation evidence uninterpretable.

The V1.5 pressure-channel validation tool answers only this question:

`analyzer pressure_kpa * 10 hPa/kPa` versus `COM22 pressure_hpa`.

It does not use CO2/H2O residuals, does not fit CO2/H2O coefficients, and does
not write `SENCO9`. If `SENCO9` work is ever needed, it must remain a separate
pressure-channel calibration task with its own authorization and evidence.

Mode A, current-atmosphere quick validation, is the first supported mode:

- route condition: analyzer and pressure references see the same open
  atmosphere / open-flow pressure state;
- primary reference: COM22 digital pressure gauge;
- auxiliary reference: PACE pressure controller;
- verified object: analyzer internal `pressure_kpa`;
- output decision: whether pressure `P` is trustworthy enough for formal
  CO2/H2O work.

The pressure quick check must actively establish and retain the same PACE
continuous-atmosphere state used by the V1.5 H2O/CO2 open-route paths before
sampling. A row without verified continuous-atmosphere evidence is diagnostic
only and must not unlock formal CO2/H2O work.

### Pressure-channel calibration boundary versus serial wet-route pressure

The pressure-channel `SENCO9` calibration/verification and the H2O serial
open-flow pressure diagnostic answer different physical questions:

- `SENCO9` pressure-channel calibration verifies whether each analyzer's
  internal `pressure_kpa` agrees with the traceable COM22 pressure reference
  under a pressure-validation condition where the analyzer pressure input and
  the external pressure reference are intended to represent the same pressure
  state. It is pressure-channel-only evidence and does not fit CO2/H2O.
- A serial H2O open-flow route can create a real pressure gradient from the
  humidity generator through analyzer 1..N, the dewpoint meter, COM22 branch,
  and the PACE atmosphere outlet. In that topology, COM22 near the downstream
  branch proves downstream pressure stability; it does not automatically prove
  that every upstream analyzer cavity is at the same pressure.
- A stable COM22/PACE tail pressure plus unstable analyzer `pressure_kpa` in a
  serial wet route is therefore not evidence that `SENCO9` was calibrated
  incorrectly. It is evidence that the wet-route local pressure at each
  analyzer must be treated as a separate sampling-condition QC input.

For the current bench topology:

```text
humidity generator -> analyzer 1 -> analyzer 2 -> ... -> analyzer N
  -> dewpoint meter -> PACE atmosphere outlet
                         |
                         +-> COM22 branch
```

the formal H2O open-flow runner must require:

- dewpoint stability at the downstream reference;
- H2O ratio/signal stability for each analyzer that will enter evidence;
- analyzer internal `pressure_kpa` stability evidence per analyzer before the
  sample window;
- pressure quick-check evidence bound to the same analyzer device ID, but not
  interpreted as proof that the serial wet-route pressure gradient is absent.

This boundary preserves the pressure-channel calibration result while still
exposing wet-route analyzer pressure as a sampling-condition QC input. In the
current V1.5 runner, an unstable analyzer `pressure_kpa` pre-sample gate should
not stop the operator from collecting traceable H2O evidence by default. It must
continue as a pressure-condition warning and remain visible in QC/report
review. It must not, by itself, remove a CO2/H2O sample from polynomial
candidate fitting when the pressure channel has been independently validated
and the component evidence remains stable: ratio, concentration/density,
dewpoint or humidity reference, factory signal, temperature, and frame quality.
A site may still opt into a `fail` policy when the wet-route pressure problem
is known to invalidate the measurement model.

The offline formal-fit contract follows the same boundary:

- analyzer pressure span warnings are exported as report/QC warnings by
  default;
- pressure terms `P`, `RP`, and `RTP` remain frozen for the V1.5 open-flow
  CO2/H2O polynomial candidate fit;
- a pressure warning does not downgrade an otherwise stable CO2/H2O sample from
  A grade unless the site explicitly enables a pressure-affects-grade policy;
- analyzer devices are reviewed independently by analyzer device ID, so one
  failed serial channel must not invalidate another analyzer's clean evidence.

Sidecar export command:

```powershell
python -m gas_calibrator.tools.export_v1_5_pressure_channel_validation `
  --run-dir <existing_v1_5_run_dir> `
  --pressure-reference-json <com22_pressure_reference.json>
```

The pressure-reference snapshot must include:

```json
{
  "device_id": "COM22-DPG-001",
  "certificate_id": "P-CERT-001",
  "certificate_uncertainty": 0.15,
  "valid_until": "2027-01-01",
  "certificate_hash": "..."
}
```

If the COM22 certificate snapshot is missing, incomplete, invalid, or expired,
the pressure check can still be useful engineering evidence, but it must be
reported as `engineering_diagnostic` and must not unlock formal CO2/H2O
coefficient-write decisions.

When an operator explicitly authorizes a V1.5 no-write pressure quick check,
the existing pressure-only collector may be used to produce the dedicated
quick-check artifact:

```powershell
python -m gas_calibrator.tools.validate_pressure_only `
  --config <v1_5_no_write_config.json> `
  --pressure-reference-json <com22_pressure_reference.json> `
  --pressure-points ambient `
  --count 10
```

This collector still does not switch the water/gas route and does not control
PACE pressure points by default. It writes
`pressure_channel_quick_check_<run_id>.csv`, then runs the sidecar pressure
channel validation on that artifact. It must only be executed with explicit
real-device authorization.

## Formal calibration evidence package

Template files:

- `docs/templates/formal_plan_snapshot_template.json`
- `docs/templates/standard_gases_template.json`
- `docs/templates/com22_pressure_reference_template.json`
- `docs/templates/released_uncertainty_inputs_template.json`

Before a formal no-write run, prepare a complete offline run package:

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

The package command writes fill-in standard-gas, pressure-reference, and
uncertainty templates, the immutable plan/reference snapshots, an
`evidence_run_manifest.json`, and a no-write runbook. It is sidecar-only:
`opens_com_ports=false`, `controls_water_or_gas_routes=false`,
`controls_valves_or_pace=false`, and `writes_coefficients=false`.

The reviewed JSON arguments are optional for first-time skeleton generation.
If they are omitted, regenerate the package with filled and reviewed
standard-gas and COM22 certificate files before sampling, so the immutable
run-start snapshots do not contain placeholders.

`released_uncertainty_inputs_template.json` must stay `released=false` until
the uncertainty budget has been reviewed. Using that unreleased template in the
report generator keeps the formal report in `draft_only`; it cannot be issued
as a formal calibration certificate.

Offline preflight command:

```powershell
python -m gas_calibrator.tools.export_v1_5_formal_preflight `
  --run-dir <existing_v1_5_run_dir> `
  --plan-json <formal_plan_snapshot.json> `
  --pressure-reference-json <com22_pressure_reference.json> `
  --config <v1_5_no_write_runtime_config.json>
```

Preflight reads files only. It checks the plan contract, COM22 pressure
reference contract, no-write config, sample artifact presence, dedicated
pressure quick-check artifact presence, and formal package readiness. It must
not open COM ports or control water/gas routes.

The reviewer-facing V1.5 package command is:

```powershell
python -m gas_calibrator.tools.export_v1_5_formal_calibration_package `
  --run-dir <existing_v1_5_run_dir> `
  --plan-json <formal_plan_snapshot.json> `
  --pressure-reference-json <com22_pressure_reference.json>
```

This package merges:

- formal plan traceability;
- COM22 pressure-reference traceability;
- pressure-channel quick-check evidence;
- open-flow CO2/H2O QC classification;
- A-grade samples;
- B-grade review samples;
- rejected samples and reasons;
- candidate coefficient review status.

By default the package requires a dedicated
`pressure_channel_quick_check*.csv` artifact. If it only finds pressure columns
inside normal sample rows, it may still export a review aid, but candidate
coefficient review remains blocked with
`pressure_quick_check_artifact_missing`.

The package never writes analyzer coefficients. Its decision is limited to
`ready_for_reviewer` versus `blocked`; final coefficient adoption remains a
separate human review and explicit write authorization step.

The operator checklist for the future no-write real-device sequence is kept in
`docs/v1_5_formal_no_write_execution_checklist.md`.

## Evidence registry database

V1.5 uses a file evidence package plus PostgreSQL index:

- raw CSV/JSON/XLSX/report artifacts remain on disk;
- PostgreSQL stores run, device, standard gas, certificate, artifact hash, QC,
  candidate-review, write-event, report, and audit indexes;
- database import must be sidecar-only and must not open COM ports, control
  PACE, switch water/gas routes, or write coefficients.

Migration:

```powershell
$env:PYTHONPATH = "src"
python -m gas_calibrator.tools.migrate_v1_5_evidence_db
```

Dry-run import bundle:

```powershell
$env:PYTHONPATH = "src"
python -m gas_calibrator.tools.import_v1_5_evidence_package `
  --run-dir <existing_v1_5_run_dir> `
  --plan-json <formal_plan_snapshot.json> `
  --pressure-reference-json <com22_pressure_reference.json> `
  --output-json <evidence_bundle.json> `
  --dry-run
```

Database guidance is in `docs/v1_5_evidence_registry_database.md`. The DSN is
provided by `GAS_CAL_DB_DSN` or `--dsn`; passwords must not be committed.

Before a formal run, prepare the immutable plan/reference snapshots:

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

Before any real-device action, run the formal readiness assessment:

```powershell
$env:PYTHONPATH = "src"
python -m gas_calibrator.tools.export_v1_5_formal_readiness `
  --run-dir <planned_or_existing_v1_5_run_dir> `
  --plan-json <formal_plan_snapshot.json> `
  --pressure-reference-json <com22_pressure_reference.json> `
  --config <v1_5_no_write_runtime_config.json>
```

This readiness report summarizes the current execution position:

- `setup_blocked`;
- `ready_for_pressure_quick_check_authorization`;
- `pressure_channel_blocked`;
- `ready_for_open_flow_sampling_authorization`;
- `ready_for_reviewer`;
- `evidence_blocked`.

The report is an offline operator/engineer decision aid only. It must not open
COM ports, change water/gas routes, control PACE/valves, write coefficients, or
turn an authorization prompt into real acceptance evidence.

For a one-command offline reviewer package, run:

```powershell
$env:PYTHONPATH = "src"
python -m gas_calibrator.tools.run_v1_5_formal_offline_review_chain `
  --run-dir <planned_or_existing_v1_5_run_dir> `
  --plan-json <formal_plan_snapshot.json> `
  --pressure-reference-json <com22_pressure_reference.json> `
  --config <v1_5_no_write_runtime_config.json>
```

The chain always writes readiness, preflight, workbench, operation console, and
review surface outputs. It only writes evidence bundles, reports, and advanced
QC summaries when the required pressure quick-check and open-flow sample
artifacts already exist. Missing real artifacts must remain `pending` or
`blocked`; the chain must not synthesize acceptance evidence.

After pressure quick-check and open-flow samples are available, use the sidecar
chain:

```powershell
$env:PYTHONPATH = "src"
python -m gas_calibrator.tools.run_v1_5_formal_evidence_sidecar `
  --run-dir <existing_v1_5_run_dir> `
  --plan-json <formal_plan_snapshot.json> `
  --pressure-reference-json <com22_pressure_reference.json> `
  --config <v1_5_no_write_runtime_config.json>
```

The sidecar chain writes preflight, formal package, evidence bundle, and
optional DB import artifacts. It must remain offline/sidecar-only:
`opens_com_ports=false`, `controls_water_or_gas_routes=false`, and
`writes_coefficients=false`.

The offline evidence workbench command is:

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

It writes `v1_5_formal_workbench.html/.json/.md`. This is a static review
surface, not a device-control UI. It must keep the same sidecar boundary:
`opens_com_ports=false`, `controls_water_or_gas_routes=false`,
`controls_valves_or_pace=false`, and `writes_coefficients=false`.

## Formal calibration reports

Reports must be generated from the evidence bundle, not hand-written. The V1.5
report generator produces:

- `run_report.md/.docx/.pdf`
- `technical_report.md/.docx/.pdf`
- `formal_calibration_report.md/.docx/.pdf`
- `report_model.json`

Command:

```powershell
$env:PYTHONPATH = "src"
python -m gas_calibrator.tools.export_v1_5_calibration_reports `
  --evidence-bundle-json <evidence_bundle.json> `
  --output-dir <report_output_dir> `
  --report-no <report-number> `
  --reviewer <reviewer-name> `
  --approver <approver-name> `
  --uncertainty-json <released_uncertainty_inputs.json>
```

The formal report must state:

- what was calibrated;
- which standard gases and reference devices were used;
- whether data were stable and traceable;
- the pressure-channel validation status;
- pressure-compensation coverage or non-coverage;
- CO2/H2O results and uncertainty-budget status;
- whether SENCOx coefficients were written;
- that sealed pressure points and dynamic pressure diagnostics were not used in
  formal CO2/H2O fitting by default;
- that blocked evidence cannot be issued as a formal calibration certificate.

The report release gate must be enforced:

- `blocked`: evidence or pressure validation is not sufficient;
- `draft_only`: uncertainty is incomplete or not released;
- `review_ready`: uncertainty is released, but signatures are pending;
- `formal_release_ready`: evidence, pressure, uncertainty, reviewer, and
  approver are all present;
- `not_releasable`: a high-risk event such as unaudited coefficient write is
  present.

Only `formal_release_ready` may be considered ready for formal issue. All other
states must print `DRAFT / NOT FOR FORMAL ISSUE` in Markdown/DOCX/PDF.

## Stage 8-10 software interface boundary

The V1.5 operation console, parameter-governance surface, and advanced QC
engines are documented in
`docs/v1_5_stage_8_9_10_software_interface_plan.md`.

These modules are sidecar-first:

- operation console: read-only eight-page status model, not device control;
- parameter governance: A-E parameter classification, audit events, and
  high-risk parameters hidden by default;
- advanced QC: steady-state selection, humidity diagnostics, factory signal
  health, pressure trends, control charts, uncertainty budget, and root-cause
  classification from existing evidence rows.

They must keep `opens_com_ports=false`,
`controls_water_or_gas_routes=false`, `controls_valves_or_pace=false`, and
`writes_coefficients=false` unless a future reviewed implementation explicitly
adds a separate real-device control boundary.

The unified review surface combines these stage 8-10 models with the formal
evidence workbench and report release gate:

```powershell
$env:PYTHONPATH = "src"
python -m gas_calibrator.tools.export_v1_5_review_surface `
  --output-dir <review_surface_dir> `
  --formal-workbench-json <v1_5_formal_workbench.json> `
  --operation-console-json <v1_5_operation_console.json> `
  --advanced-qc-json <advanced_qc_summary.json>
```

This page is a static reviewer/operator surface only. It may summarize
blockers, next actions, parameter-governance status, and advanced QC root
causes, but it must not trigger sampling, route switching, PACE control,
SENCO/CLEARSENCO commands, or real acceptance promotion.

The stage 10 advanced QC exporter must follow the same sidecar boundary:

```powershell
$env:PYTHONPATH = "src"
python -m gas_calibrator.tools.export_v1_5_advanced_qc `
  --run-dir <existing_v1_5_run_dir> `
  --output-dir <advanced_qc_output_dir>
```

It may read existing `samples_*.csv` and pressure quick-check artifacts to build
`advanced_qc_summary.json/.md`, but it must not open COM ports, change water/gas
routes, control PACE/valves, write coefficients, or promote diagnostic pressure
evidence into formal acceptance. Non-open pressure modes must remain excluded
diagnostic rows unless a future reviewed opt-in explicitly changes the formal
fit scope.

## V2-style gates that must not be hard defaults in V1.5

The following checks may be useful as diagnostics, but must not default to
fail-closed in V1.5 production flow:

- hard fresh-atmosphere gate before H2O/CO2 open-route dewpoint sampling;
- analyzer dry-enough violation before gas sampling when the dewpoint trend is
  otherwise acceptable;
- humidity-generator shutdown flow verification or flow set commands during
  safe stop;
- open-flow VENT1 keepalive-gap fail-closed;
- PACE VENT status 3 as a hard sealed-control blocker;
- small sealed-control undershoot when pressure approaches the target from
  above.

## Current code defaults that protect this behavior

- `workflow.pressure.open_flow_vent1_gap_fail_closed_enabled = false`
- `workflow.stability.analyzer_gate_dry_enough_violation_policy = "warn"`
- `workflow.humidity_generator.safe_stop_verify_flow = false`
- `workflow.humidity_generator.safe_stop_enforce_flow_check = false`
- `workflow.pressure.control_ready_allowed_vent_statuses` includes `3`
- `workflow.pressure.exhaust_only_undershoot_fail_closed_enabled = false`
- `workflow.pressure.exhaust_only_target_crossing_fail_closed_enabled = false`
- `workflow.pressure.exhaust_only_undershoot_hard_fail_hpa = 5.0`
- `coefficients.ratio_poly_fit.formal_pressure_modes = ["", "ambient_open"]`
- `coefficients.ratio_poly_fit.formal_fit_roles = ["", "component_calibration", "formal_component_calibration", "main_component_calibration", "primary_component_calibration"]`
- `coefficients.ratio_poly_fit.include_sealed_pressure_points_in_formal_fit = false`
- `MODE2` parser/export retains raw frame, token JSON, field mapping JSON, known
  semantic fields, optional status, and extra tokens.

Strict behavior may still be enabled deliberately in tests or diagnostic
configs, but it must be opt-in.

## Evidence from 2026-05-24 no-write run

Run:

`v1_5_h2o_co2_900ppm_ambient_1100_no_write_20260524_145656`

Observed:

- H2O current-atmosphere open route completed 10 samples.
- CO2 900 ppm current-atmosphere open route completed 10 samples.
- CO2 open-route dewpoint gate passed after fixed precondition.
- CO2 sealed transition did close atmosphere/route first, then enabled OUTP.
- The CO2 1100 hPa sealed point reached the target region, then failed because
  a small undershoot around 1099 hPa was treated as
  `FAIL_CLOSED_PRESSURE_UNDERSHOOT_EXHAUST_ONLY`.
- H2O 1100 hPa did not stabilize because PACE positive source evidence was only
  about 1009 hPa, which is a source/hardware limitation rather than an
  open-route flow bug.
- No-write audit showed no identity, calibration, or coefficient writes.

## Future-change checklist

Before changing V1.5 route or pressure logic:

1. Compare against the known-good gas-route behavior around commit
   `c97935aeb3fa8172858b591beacb6339e5781fb4`.
2. Keep original V1.5 water-route timing unless a field failure proves it needs
   a narrow fix.
3. Preserve 10 samples per point unless the operator explicitly requests a
   shorter diagnostic run.
4. Do not copy V2 protected-route gates into V1.5 as hard defaults.
5. Run targeted tests for open-route atmosphere, sealed transition, safe stop,
   and pressure undershoot behavior before any real V1.5 smoke run.

Recommended targeted checks:

```powershell
python -m py_compile src\gas_calibrator\workflow\runner.py src\gas_calibrator\config.py
python -m pytest tests\test_safe_stop_tool.py tests\test_config_runtime_defaults.py -q
python -m pytest tests\test_v1_5_controlled_outp_seal_transition.py::test_exhaust_only_small_undershoot_can_continue_until_inlimit -q
python -m pytest tests\test_v1_5_pace_audit_guards.py::test_analyzer_gate_dry_enough_small_overshoot_warns_not_fail -q
```
