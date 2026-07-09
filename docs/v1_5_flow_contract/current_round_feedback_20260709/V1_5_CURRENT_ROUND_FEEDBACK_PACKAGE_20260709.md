# V1.5 current-round feedback package

Generated: 2026-07-09

Scope: documentation and checklist only. This package does not open COM ports,
does not control routes, does not write SN/device_code, does not write SENCO
coefficients, does not connect PostgreSQL, and does not modify CO2/H2O runner
code.

## Executive conclusion

This round showed that most failures were not caused by the mature V1.5
calibration concept itself. They came from running or analyzing with paths that
were not exactly the mature 0613/0620/0621 paths, or from mixing data states
across staged writes and changing physical conditions.

The V1.5 production rule is therefore:

1. Use the 0620/0621 mature physical execution path for legacy CO2/H2O routing
   and sampling.
2. Use the 0613-style V1.5 fitting method for CO2/H2O coefficient review and
   write planning.
3. Treat migrated, 0624, root-directory, diagnostic, and handoff scripts as
   review evidence only unless they are proven blob-identical or
   contract-equivalent to the mature path.
4. Separate real pass evidence from no-write math, review-only candidates,
   smoke tests, and diagnostic runs.

## Authoritative baselines

| Area | Authoritative baseline | Meaning |
|---|---|---|
| Legacy CO2 route | 0620/0621 mature V1.5 path | 45 CO2 points, mature point-internal route/vent/pressure/dewpoint/sample timing |
| Legacy H2O route | 0620/0621 mature V1.5 path | 13 H2O wet points, mature humidifier/temperature/flow timing |
| CO2 fitting | 0613-style V1.5 method | S1/S3 main curve plus S5 final affine trim; zero gas is not forced to absolute zero without review |
| H2O fitting | 0613/0620-style V1.5 method | S2/S4 main H2O ratio-temperature model; S6 remains a separate final affine layer |
| New algorithm | Profile/contract layer until live path is separately reviewed | 47 CO2 / 14 H2O candidate profile does not change old-algorithm mature queue |
| Initialization identity | SN/device_code primary identity; protocol ID command alias | SN must be unique 8 digits; protocol ID and COM/GA mapping are runtime identities |
| Pressure | Mature pressure chain with PACE INL absolute pressure | S9 default offset-only, with documented linear exception only after write/readback/reverify |

## Formal entrypoint policy

These are production-intent entrypoint classes. The exact file used must be
verified against the mature baseline before a live production run.

| Stage | Formal intent | Required guard |
|---|---|---|
| Initialization read-only | Identify COM/GA/protocol ID/SN/GETCO/runtime | Old algorithm must not issue CHECK; commands paced >=1s |
| Initialization controlled write | SN/device_code and S5/S6/S7/S8 neutralization only when authorized | Read old state, write, readback, and closeout evidence |
| Pressure/S9 | Pressure-only no-write and controlled S9 write when needed | PACE INL absolute pressure; write/readback/reverify |
| Route readiness | PACE, dewpoint meter, relay/flow path readiness | No CO2/H2O queue until route readiness pass |
| CO2 queue | Legacy 45-point mature path for old algorithm | 0620/0621 point-internal logic, not migrated path |
| H2O queue | Legacy 13-point mature path for old algorithm | 0620/0621 point-internal logic, not migrated path |
| Fitting review | 0613/0620 V1.5 fitting logic | no-write first, then controlled write, then short verification |
| Controlled write | S1/S3/S5/S2/S4/S6/S9 only by reviewed tool | Read current GETCO first; write second; readback third |
| Archive/database | Release and PostgreSQL import only after all gates pass | Review packages are not release/import evidence by themselves |

Forbidden as production start points:

- Root-directory migrated scripts unless proven equivalent to the mature
  baseline.
- 0624 handoff queue as the maturity reference.
- `_handoff` scripts, diagnostic probes, smoke-test wrappers, replay-only tools,
  no-write review tools, and worker sampling scripts.
- Any path that changes `run_app.py` or V2 defaults to claim V1.5 production
  readiness.
- Any path that treats no-write math or smoke points as real acceptance
  evidence.

## Issue-to-contract map from this round

| Observed issue | Root cause | Contract to lock |
|---|---|---|
| CO2 40C points failed with COM22 pressure around 1400 hPa | Migrated point-internal logic did not preserve mature vent-hold/open-flow pressure behavior | CO2/H2O production route must use 0620/0621 mature point-internal logic, especially legacy hold thread, route vent-hold, COM22 atmosphere pressure, and PACE INL pressure |
| 0624 path appeared usable but data quality was poorer | 0624 was not the final mature path | 0624 is diagnostic/migration evidence, not the maturity reference |
| Ratio stability failure risk could drop an entire point | Point gate treated one analyzer's ratio instability as global failure | Public physical gates can fail the point; per-analyzer ratio gates should produce per-analyzer quality grade and still sample eligible analyzers |
| Pressure-only first failed with 1100 hPa read near 102 hPa | Wrong PACE pressure command path used gauge/differential style value | Use PACE INL absolute pressure for mature pressure/S9 |
| GA04 pressure failed offset-only S9 but passed linear S9 | Device had stable pressure scale bias | S9 default remains offset-only; linear S9 allowed only as explicit controlled exception with no-write review, write/readback, and pressure-only reverify |
| H2O segmented runs disagreed | Physical state, humidifier dynamics, chamber temperature, and reference timing changed between segments | Do not merge staged water evidence blindly; replace or block physically inconsistent segments and keep same-state evidence together |
| 005 water looked bad in raw verification but fit candidate could be low-error | Raw output and reviewed model residual were different evidence states | Report raw verification error separately from no-write fitted residual; do not call a candidate a real pass before write/readback/reverify |
| 010/090 CO2 appeared contradictory | Mixed prewrite full-run data with postwrite/current short-verification state | Partition evidence by coefficient state and physical state before fitting |
| S1/S3 write initially produced bad CO2 | Temperature basis mismatch with firmware evaluation | Use mature V1.5 firmware-compatible S1/S3 payload format and replay against actual firmware temperature basis |
| CO2 final errors only became acceptable after S5 | Main curve and final affine layer were different responsibilities | Fit S1/S3 main curve first, then compute S5 final layer from current GETCO5 composition; use CLEARSENCO5 before writing final target |
| S2/S4/S6 confusion in H2O | S6 was mixed into main H2O model decisions | Keep S2/S4 as main H2O model and S6 as separate final affine layer |
| SN/ID confusion before initialization closeout | Protocol ID and production identity were not separated | SN/device_code is the production primary identity; protocol ID is command alias; COM/GA is transport mapping |

## Evidence vocabulary

Use these terms consistently in reports, checklists, and code comments.

| Term | Meaning | Allowed downstream use |
|---|---|---|
| real_pass | Live run/write/readback/reverify evidence passed required gates | Can feed release/readiness if all other gates pass |
| no_write_candidate | Offline or live no-write math indicates a possible coefficient | Can justify a controlled write review, not release |
| review_required | Evidence is promising but has identity, state, fit, or physical caveat | Requires human or additional controlled evidence |
| diagnostic_only | Smoke/replay/probe evidence, not a production point | Root-cause analysis only |
| superseded | Data was replaced by a cleaner point or state-specific rerun | Keep for traceability, do not fit |
| rejected_input | Physically inconsistent or contract-invalid point | Do not fit unless explicitly reopened with reason |

## CO2 fitting rules to preserve

1. Use the 0613-style V1.5 fitting approach, not old V1 shortcuts.
2. The old-algorithm CO2 production candidate is legacy 45-point evidence, not
   new-algorithm 47-point evidence.
3. Zero gas can be modeled as a reviewed low-end anchor; do not assume absolute
   zero without traceability.
4. Use the firmware-compatible S1/S3 payload format and temperature basis.
5. S5 is the final CO2 affine layer. It must be calculated from current GETCO5
   state as a composed transform.
6. Neutralizing S5 requires `CLEARSENCO5,YGAS,FFF`; `SENCO5,YGAS,FFF,0,1` is not
   a safe substitute.
7. S5 write success and later open-flow verification success must be reported
   separately.

## H2O fitting rules to preserve

1. Use dewpoint-meter plus pressure-derived H2O reference, not humidity-generator
   setpoint as the calibration target.
2. Use analyzer chamber temperature/T1 as the model temperature basis when that
   is the firmware/runtime basis.
3. H2O low-end dry-gas anchors must not be collapsed into CO2 zero gas. Keep
   their physical meaning separate.
4. S2/S4 is the main H2O ratio-temperature chain. S6 is a separate final affine
   layer.
5. A staged or supplementary H2O point can replace a conflicting earlier point
   only when the physical state and coefficient state are documented.
6. If a point is raw-output-bad but model-residual-good, report both; the latter
   is not a real pass until write/readback/reverify.
7. Segmented water evidence must carry temperature/humidifier/dewpoint/pressure
   timing status because physical hysteresis is significant.

## Physical execution rules to preserve

1. Open-flow CO2/H2O point internals must keep mature vent-hold and pressure
   behavior from 0620/0621.
2. COM22 should remain near atmospheric pressure during open-flow unless a
   deliberate pressure mode is being tested.
3. Dewpoint dry gate must pass before formal CO2 dry/zero points are accepted.
4. Humidity generator flow appears only after generator start; do not evaluate
   water route as if flow exists before that.
5. For H2O, do not restart or stop the generator unnecessarily between
   same-temperature humidity points unless the mature path explicitly does so.
6. Public physical gates can fail a point; analyzer-specific gates should mark
   analyzer quality and keep eligible analyzer samples.
7. Sampling evidence must preserve per-analyzer quality grade, valid frame count,
   raw ratios, T1, pressure, dewpoint, and reference source.

## Traceability pointers from this round

These paths are local traceability pointers, not canonical production
entrypoints.

| Evidence | Path |
|---|---|
| Final CO2 four-device closeout | `D:\gas_calibrator\_p9_20260709\co2_four_device_final_co2_closeout_20260709_v1` |
| CO2 S5 no-write review at 100/500/900 | `D:\gas_calibrator\_p9_20260709\co2_100500900_no_write_s5_s13_review_v1` |
| CO2 S5 controlled write/readback | `D:\gas_calibrator\_p9_20260709\co2_four_device_s5_controlled_write_after_100500900_v3_active_scan` |
| CO2 post-S5 verification | `D:\gas_calibrator\_p9_20260709\co2_four_device_post_s5_verify_100500900_notemp_v2` |
| CO2 firmware temperature root cause | `D:\gas_calibrator\_p9_20260709\co2_s13_firmware_formula_root_cause_after_bad400_v1` |
| H2O multistrategy review | `D:\gas_calibrator\_p9_20260708\h2o_v15_refit_with_postwrite25_multistrategy_v1` |
| H2O 005 S09 write/readback | `D:\gas_calibrator\_p9_20260708\h2o_S09_senco24_controlled_write_v1\device_005` |
| H2O 25C verification raw points | `D:\gas_calibrator\_p9_20260707\h2o_K_post_senco24_verification_25c_305070_v1` |

## Next hardening package

The next code-facing package should be a guard/contract package, not a runner
rewrite:

1. Add a mature-path baseline guard that rejects CO2/H2O production execution
   when the point-internal sampling kernel is not mature 0620/0621-equivalent.
2. Add a per-analyzer quality policy guard: analyzer ratio instability downgrades
   that analyzer, not the whole point, unless public physical gates fail.
3. Add a fitting-state partition guard: prewrite full-run evidence and postwrite
   short verification cannot be fit together without an explicit composition
   model.
4. Add a report vocabulary guard so raw output, model residual,
   no-write_candidate, real_pass, review_required, and diagnostic_only are not
   collapsed.
5. Add a checklist gate before database/import/archive that confirms each
   coefficient write has readback and independent verification.

Do not implement live route changes until this package is reviewed and accepted.
