# V2 Cutover Checklist

## Purpose

This checklist defines the minimum cutover gate before V2 can be considered a
valid replacement candidate for the frozen V1 workflow.

Important usage note for the current branch:
- This document is a future cutover / real-acceptance gate reference.
- It is not the current Step 2 iteration work order.
- While the project remains in Step 2, do not use this checklist as
  authorization to start real-device bring-up, real compare / real verify, or
  `real_primary_latest` refresh.

The goal is not "code looks cleaner". The goal is that V2 can reliably finish
the same operator-visible successful chain:

1. Control devices and route valves
2. Wait for ready / stable conditions
3. Sample
4. Persist results
5. Export summary / QC / coefficients / artifacts

V1 remains frozen and available as rollback / historical reference. Cutover is
allowed only after the items below are explicitly verified.

## Current Status Snapshot (2026-03-28)

Current recommendation:
- Do not switch the default entry to V2 yet.
- The program is still in the three-step strategy's Step 2: production-grade
  platform construction. This phase may harden simulation-only workbench,
  review center, suite/parity/resilience governance, analytics, lineage, and
  Chinese-default UI/1080p ergonomics, but it must not enter Step 3 real
  acceptance release or default-entry cutover.
- Current-branch validation remains restricted to simulation / replay / suite
  regression / parity / resilience / offline review / UI contract only.
- V2 is ready for continued smoke/safe/fit-ready simulation, route-trace
  review, parity/resilience hardening, and offline review-center/workbench
  productization, but it is still in pre-cutover integration state and must
  not be described as bench/device bring-up readiness or V1 replacement
  readiness.
- ui_v2 is now Chinese-first for operator-facing copy (`zh_CN` default,
  `en_US` fallback). This is a product-surface hardening step only; it is not
  acceptance closure.
- ui_v2 `Devices` now includes a device-operation workbench for analyzer,
  PACE, GRZ5013, chamber, relay, thermometer, and pressure gauge, but the
  current workbench is strictly simulation-only:
  - fake/simulated devices only
  - offline validation only
  - no real COM/serial open
  - no real manual device control
  - no real primary-latest refresh
- The same workbench can now generate standalone simulated diagnostic evidence
  (`workbench_action_report.json/.md`, `workbench_action_snapshot.json`) and
  records it under `diagnostic_analysis`. This evidence is explicitly marked
  `simulated_workbench`, `not_real_acceptance_evidence=true`, and must never
  publish or replace the real primary latest.
- The workbench now separates an operator-oriented default view from an
  engineer diagnostic view. This is a product-surface layering change only; it
  is not real-hardware enablement or acceptance closure.
- The simulation-only workbench now also exposes operator presets, filtered
  action history, and simple before/after snapshot comparison so common
  offline drill paths no longer require low-level button-by-button replay.
  These remain review/diagnostic aids only; they do not authorize real-device
  control.
- Offline suite/parity/resilience/workbench evidence can now be read together
  through a unified review center in `ui_v2`, with separate operator /
  reviewer / approver focus summaries, evidence filters, acceptance-readiness
  summary, and analytics/lineage digest. This is Step 2 reviewer/operator
  workflow hardening only and must not be interpreted as real acceptance
  evidence.
- That review center is now moving from a latest-artifact digest toward a
  lightweight cross-run review index. It can aggregate recent run folders,
  recent suites, parity/resilience artifacts, and recent workbench evidence
  without introducing a heavy database. This remains Step 2 offline reviewer
  ergonomics only and must not be treated as real acceptance closure.
- The review center now also keeps a lightweight per-source/run index summary
  so reviewer / approver workflow can scan cross-run coverage and missing
  evidence types without implying any real acceptance release.
- The same review center now also exposes source-aware filtering
  (`run` / `suite` / `workbench`) and keeps operator / reviewer / approver
  summaries explicitly framed as offline simulated/replay/diagnostic review
  evidence. This is still Step 2 review workflow hardening only.
- Review center 2.1 now also carries analytics/lineage summary entries,
  risk-level rollups, source-kind coverage summaries, and recent-window
  filtering over lightweight indexed run/suite/workbench evidence without
  introducing a database. This remains Step 2 offline review-center
  hardening only and must not be misread as real acceptance closure.
- Review center 3.0 now also presents a clearer list/detail review-workbench
  split inside existing results/report surfaces, with structured evidence
  summary, risk, key fields, artifact paths, and acceptance-readiness hints.
  This remains Step 2 offline reviewer/approver workflow hardening only.
- Review center 4.0 now also keeps structured analytics/lineage detail rows in
  that same list/detail review workbench, so reviewer/approver workflow can
  inspect evidence summary, acceptance hints, analytics digest, and lineage
  context without leaving the current offline review surface.
- Review center 5.0 now also keeps analytics/lineage context carried into each
  evidence detail row, so reviewer/approver workflow can inspect a selected
  offline item without losing surrounding cross-run review context.
- ui_v2 shell/pages are now being hardened for a 1920x1080 operator screen:
  major pages use page-level scrolling or internal scrolling instead of silent
  overflow, and the shell gives more vertical space to the main workspace via
  a resizable workspace/log split. This is a UI productization change only; it
  does not alter the V2 execution chain or authorize real acceptance claims.
- The shell/workspace split now also remembers the last local sash position,
  with safe fallback to a default layout if restore fails. This is local UI
  convenience state only and does not change execution flow or evidence
  semantics.
- The simulation-only workbench now also persists local product preferences for
  operator/engineer view mode, compact/standard layout mode, display profile,
  recent presets, favorites, and pinned presets. This is Step 2 local-product
  state only; it is not a permission system and does not authorize real-device
  control.
- The simulation-only preset center now supports a lightweight custom preset
  editor for create/edit/delete of fake action combinations, plus local
  persistence and review-center/evidence-chain linkage. This remains offline
  simulation productization only and must not be interpreted as real-device
  scripting or acceptance evidence.
- That preset surface now also acts as a lightweight preset manager for
  simulation-only workbench use:
  - duplicate preset
  - favorite / pin / recent usage
  - grouped preset organization
  - JSON import/export with basic schema checks
  - imported/exported bundles remain simulation-only and must not be treated
    as real-device scripts or acceptance evidence
- Preset manager 2.1 now also keeps lightweight preset metadata
  (`created_at`, `updated_at`, `origin`, `preset_version`,
  `schema_version`, `imported_from`) plus fake-capability summaries,
  rename/overwrite import-conflict handling, and future-sharing placeholders.
  This is still Step 2 local preset governance only; it must not become a
  real-device scripting or sharing service.
- Preset manager 3.0 now also exposes a lightweight preset directory/index
  across built-in, local custom, and imported simulation-only presets, while
  keeping import/export/conflict handling and fake-capability summaries inside
  the same local-only preset governance boundary.
- Preset manager 4.0 now also keeps readable bundle-format / import-conflict /
  future-sharing-prep summaries on the same local JSON surface. This remains
  Step 2 simulation-only preset governance only and is not a shared service.
- Preset manager 5.0 now also carries explicit bundle/conflict-policy metadata,
  reserved future-sharing fields, and denser selected-preset
  metadata/fake-capability summaries on the same local JSON surface. This
  remains Step 2 simulation-only preset governance only.
- Engineer view now adds compact trend/anomaly blocks and grouped summary
  cards so recent actions, evidence state, snapshot deltas, and highlighted
  anomalies are easier to scan on a 1920x1080 screen without expanding raw
  diagnostic text first. This is Step 2 UI density/productization only.
- Engineer trend/analysis hardening now also includes recent-action
  statistics, reference-quality trend summaries, and artifact/lineage summary
  cards. This remains a compact Step 2 diagnostic surface, not a Step 3
  analytics platform.
- Engineer trend view 3.0 now also carries suite/analytics summary cards and
  compact suite/analytics detail sections alongside artifact/lineage and
  recent-action summaries. This remains a compact Step 2 offline analysis aid.
- Engineer view 4.0 now also carries denser suite/analytics/reference/export
  detail lines inside the same compact cards/sections, still without becoming
  a Step 3 analytics platform.
- Engineer view 5.0 now also condenses suite/analytics card and trend summaries
  with coverage/export/reference context while staying inside the same compact
  Step 2 offline analysis surface.
- Display-profile preferences now also carry lightweight family / monitor
  metadata and may refresh from the current shell window/screen size to choose
  a better default 1080p compact vs standard profile. This remains a local
  UI preference only; it does not affect execution semantics, acceptance
  semantics, or any real-device path.
- Display-profile 2.1 now also stores strategy-version, resolution-class,
  window-class, monitor-family metadata, and refined 1920x1080 auto-selection
  reasons for lightweight monitor/window mapping. This remains Step 2 local
  UI preference state only.
- Display-profile 3.0 now also refreshes from shell window-size changes with
  lightweight debouncing so 1920x1080 defaults stay aligned while still
  allowing manual local override. This remains local UI preference state only.
- Display-profile 4.0 now also keeps richer readable profile metadata
  (`selected_profile`, `resolved_profile`, `selection_mode`, mapping summary)
  around the same local family/monitor/window strategy, still without altering
  execution semantics or implying any real-device path.
- Display-profile 5.0 now also carries readable aspect-ratio / screen-area /
  window-area / profile-summary metadata on top of the same local
  family/monitor/window strategy, still without altering execution semantics
  or implying any real-device path.
- During V1 production windows, keep V2 progress on simulation, replay, and
  offline compare/report regression first. Do not rely on long exclusive
  device windows.
- Current branch validation for UI/product wording changes remains strictly
  offline: simulation, replay, suite regression, parity/resilience, and UI
  snapshot/tests only. No real-device workflow, real compare/verify, or real
  primary-latest refresh is part of this batch.
- Simulated full-route coverage is now a required parallel track for V2
  development, but simulated evidence must never be treated as real acceptance
  evidence.

Current phase:
- Step 2 offline platformization period, not formal handover and not a
  real-device bring-up window.
- Still within Step 2 of the three-step strategy:
  - Step 1 simulation completion remains a prerequisite baseline.
  - Step 2 focuses on production-grade platformization without touching real
    acceptance closure, real-device control, or default-entry switch.
  - Step 3 items such as real acceptance release, real primary latest refresh,
    and V1 replacement claims remain blocked.
- Current-branch validation scope remains restricted to simulation / replay /
  suite regression / parity / resilience / offline review / UI contract.
- Platformization scope for the current branch is now:
  - simulation/replay/parity/resilience as offline regression matrix
  - review center aggregation and lightweight indexing across
    suite/parity/resilience/workbench evidence
  - review center source-aware filtering across recent run/suite/workbench
    evidence without introducing a database
  - review center 2.1 risk/analytics/lineage/source-coverage summaries over
    the same offline evidence index
  - review center 3.0 structured list/detail review-workbench presentation on
    top of the same lightweight offline evidence index
  - review center 5.0 analytics/lineage context carry-through on each selected
    evidence detail row inside that same offline review-workbench surface
  - acceptance evidence governance and dry-run promotion planning
  - analytics/trend/SPC/drift registry scaffolding
  - lineage / evidence index / operator-review snapshot density
  - preset-center persistence, lightweight JSON import/export management, and
    engineer-view trend-density improvements inside the simulation-only
    workbench
  - preset manager 2.1 metadata/conflict-governance and fake-capability
    summaries for local simulation-only presets
  - preset manager 3.0 lightweight directory/index over built-in, local, and
    imported simulation-only presets
  - preset manager 5.0 explicit bundle/conflict/sharing-prep metadata on the
    same local JSON governance surface
  - display-profile family / monitor mapping for Chinese-default 1080p
    ergonomics inside the V2 shell/workbench only
  - display-profile 2.1 strategy/resolution/window/monitor metadata for local
    1080p profile selection only
  - display-profile 3.0 debounced shell/window-driven profile-context refresh
    for local 1920x1080 ergonomics only
  - display-profile 5.0 readable aspect/window/screen/profile metadata on top
    of the same local family/monitor/window strategy only
  - engineer trend view 3.0 suite/analytics summary cards and compact detail
    sections inside the simulation-only workbench
  - engineer view 5.0 denser suite/analytics trend/card summaries with
    coverage/export/reference context inside the same simulation-only
    workbench
  - Chinese-default cockpit copy, localized review center/workbench summaries,
    and 1080p high-density page ergonomics without adding new major UI pages
- Future Step 3 bench assumptions (reference only, not current Step 2 work order):
  - `0 ppm` unavailable
  - other gas cylinders restored
  - humidity generator fault: chamber temperature changes, humidity feedback stays static
  - main replacement route should be `co2_only + skip_co2_ppm=[0]`
  - `H2O-only` is now a fallback diagnostic gate only, not the main acceptance route
- Do not modify V1 production logic during this phase; V1 remains the frozen
  baseline and rollback path.
- Do not switch the default entry or operator default workflow to V2 during
  this phase.
- Do not connect real devices, open real COM/serial ports, run real compare /
  real verify, or refresh `real_primary_latest` during this phase.
- Use V2 to keep closing simulation/replay/suite/parity/resilience,
  artifact/export governance, Chinese-default UI, and review-center/workbench
  gaps first. Bench/device proof remains a future Step 3 gate after explicit
  re-authorization.

Completed alignment items that support validation, but do not by themselves
justify cutover:
- Formal V2 headless entry and formal V2 service builder are the preferred
  mainline path.
- `test_v2_device.py` default path now follows the formal V2 builder/service
  path instead of a separate richer bench world.
- Temperature wait, dewpoint wait, startup prechecks, final safe-stop, and
  sensor-read retry behavior are closer to current V1 semantics than before.
- `sensor_precheck` now has explicit compatibility modes, including a
  `v1_frame_like` option for replacement-validation use.
- V1/V2 control-flow compare now supports a fixed `--replacement-skip0`
  validation preset and emits structured replacement-validation fields.
- V1/V2 control-flow compare now distinguishes:
  - real route mismatch
  - `NOT_EXECUTED` when the target route was never entered
  - `INVALID_PROFILE_INPUT` when points source / temp filtering is invalid
- H2O-only / no-gas replacement validation now has its own fixed preset and
  explicit early-stop-path reporting.
- `verify_v1_v2_skip0_replacement.py` is now the fixed reusable wrapper for
  the narrowed `skip_co2_ppm=[0]` replacement-validation flow.
- Dedicated compare source-of-truth configs now live under
  `src/gas_calibrator/v2/configs/validation/` instead of `output/`.
- The compare output now leaves stable evidence artifacts per run:
  `v1_route_trace.jsonl`, `v2_route_trace.jsonl`, `route_trace_diff.txt`,
  `point_presence_diff.json`, `sample_count_diff.json`,
  `control_flow_compare_report.json`, and `control_flow_compare_report.md`.
- Standardized skip0 bundles now also include a machine-readable artifact
  inventory, so downstream review can tell whether the fixed evidence set is
  complete without manual directory inspection.
- The replacement compare wrappers now support dedicated source-of-truth
  configs under `src/gas_calibrator/v2/configs/validation/`, including the
  current primary `replacement_skip0_co2_only_real.json`.
- A fixture-driven offline replay path now exists for compare/report/latest
  regression, so V2 can continue moving while V1 keeps the production bench.
- Dedicated simulated validation profiles now live under
  `src/gas_calibrator/v2/configs/validation/simulated/` and can generate
  route trace / compare report / latest / bundle without touching real serial
  ports.
- V2 sim/replay has now moved beyond fixture-only regression into
  protocol-backed simulation for the next critical devices:
  - GRZ5013 humidity generator fake
  - temperature chamber Modbus fake
  - analyzer fleet protocol fake
  - PACE pressure-controller fake
  - relay / valve Modbus fake
  - digital thermometer ASCII-stream fake
  These protocol fakes are for offline logic coverage only and must remain
  explicitly marked `simulated_protocol`.
- The digital thermometer fake now models the RCY-A style continuous ASCII
  stream more closely:
  - 2400 baud
  - 8N1
  - sign / integer / decimal / `°C` / `CRLF`
  - stable / drift / stale / no-response / warmup-unstable / `+200°C`
    offset modes
  - malformed ASCII diagnostic modes
  This remains an offline protocol-fidelity improvement only.
- The current simulated pressure-reference strategy now follows the active V2
  driver contract. Because the current pressure-meter path uses the
  Paroscientific-style gauge driver, the fake now simulates the `*IDIDCMD`
  request/response pattern and core command family (`P*`, `Q*`, `DB`, `EW`,
  `UN`, `TU`, `MD`) needed by V2. If the real driver contract changes later,
  the fake should follow the driver/log truth first rather than a handbook
  alone.
- V2 simulated protocol runs now also carry route-layer relay state and
  aligned reference-sample evidence, so offline validation can exercise route
  switching and summary/export semantics without holding the bench.
- V2 sampling/runtime rows now carry V1-aligned reference quantities:
  `pressure_gauge_hpa`, `thermometer_temp_c`, `frame_has_data`,
  `frame_usable`, and `frame_status`.
- V2 summary/export now partially aligns to current V1 semantics:
  `reference_on_aligned_rows`, reference pressure/temperature preference,
  and fleet completeness fields (`AnalyzerCoverage`, `UsableAnalyzers`,
  `ExpectedAnalyzers`, `MissingAnalyzers`, `UnusableAnalyzers`,
  `ValidFrames`, `TotalFrames`, `FrameStatus`, `PointIntegrity`).
- V2 storage/export resilience is now moving toward current V1 behavior:
  sample/point CSVs can expand headers dynamically, rewrite safely when new
  fields appear, and isolate auxiliary export failures instead of collapsing
  the whole run artifact chain.
- V2 now emits an execution-level readable point artifact,
  `points_readable.csv`, so operator/engineer review no longer depends only on
  machine-oriented `points.csv` / `results.json` / `summary.json`.
- V2 artifact roles are now explicitly separated in manifest/summary/export
  state:
  - `execution_rows`
  - `execution_summary`
  - `diagnostic_analysis`
  - `formal_analysis`
  This keeps readable execution evidence and diagnostic fleet detail from
  silently contaminating formal analyzer-summary / coefficient outputs.
- V2 now exposes an explicit reporting-mode split through
  `workflow.reporting.include_fleet_stats`, so formal summary/export output can
  stay cleaner while simulated/replay diagnostics can opt into fleet detail.
- Simulated protocol compare now treats relay physical mismatch as an explicit
  validation failure instead of leaving the evidence only in `route_trace`.
- Simulated protocol compare now exposes `reference_quality` /
  `reference_integrity` so stale or missing thermometer / pressure references
  become explicit degraded gates even when parity itself still reports `MATCH`.
- Reference-instrument degraded/failure states now also persist through the V2
  sampling/export/analytics chain via explicit
  `pressure_reference_status` / `thermometer_reference_status` fields, so
  wrong-unit, stale, malformed, or no-response cases do not remain trapped in
  scenario metadata only.
- V1/V2 final summary parity is now under golden regression using the same
  runtime/sample inputs; remaining differences must be treated as explicit
  divergences rather than silently accepted.
- V2 simulation has now moved from loose scenario commands into explicit suite
  groups:
  - `smoke`
  - `regression`
  - `nightly`
  - `parity`
  These are offline developer-validation tools only and must not be confused
  with real acceptance closure.
- Suite execution now produces both machine-readable and operator-readable
  gate artifacts:
  - `suite_summary.json`
  - `suite_summary.md`
  `nightly` is now a real executable gate, not a placeholder definition.
- V2 now also emits an explicit offline acceptance-governance layer for
  run/suite review:
  - `acceptance_plan.json`
  - `suite_acceptance_plan.json`
  - dry-run-only promotion target: `real_primary_latest`
  Simulated/replay/diagnostic evidence is structurally blocked from promoting
  the real primary latest.
- V2 now emits analytics/traceability scaffolding beside execution summary:
  - `analytics_summary.json`
  - `trend_registry.json`
  - `lineage_summary.json`
  - `evidence_registry.json`
  - `coefficient_registry.json`
  These are platform-building artifacts for reviewer/approver workflow, not
  evidence that real acceptance has closed.
- `ui_v2` now uses a lightweight locale-resource layer with `zh_CN` as the
  default operator locale and `en_US` as fallback. Shell navigation, run
  control, notifications, QC overview, acceptance/analytics/lineage digests,
  suite markdown summaries, the new device-operation workbench, and peripheral
  dialogs are now Chinese by default. Major high-density pages now also use
  page-level scroll handling so long content stays reachable on 1920x1080
  screens. This must remain a V2-only surface and must not be wired back into
  the frozen V1 UI.
- The device-operation workbench is a future real-hardware safety-gating
  surface, not current real-hardware acceptance closure. Current real
  acceptance and cutover review remain open.
- Workbench-generated evidence is reviewable offline evidence only. It helps
  operator/engineer diagnosis without requiring a full suite run, but it does
  not close real acceptance and must remain outside formal-analysis outputs.

What current `co2_only + skip_co2_ppm=[0]` can validate right now:
- CO2 control sequence / route action order
- CO2 point presence consistency for non-zero points
- CO2 sample count consistency for non-zero points
- whether remaining CO2 route differences are explicit and reviewable

What current `co2_only + skip_co2_ppm=[0]` does not validate by itself:
- true `0 ppm` behavior
- H2O route equivalence
- final numeric replacement quality
- signed-off bench/device readiness
- default-cutover readiness
- simulated protocol `MATCH` results, including full-route relay/thermometer
  scenarios, remain offline logic evidence only and must not be read as real
  acceptance closure

What current `H2O-only` diagnostic validation can validate:
- whether V1 and V2 both reach the intended H2O-only control-flow path
- whether the current fallback early-stop path is explicit, reviewable, and
  traceable
- whether route-entry failure is clearly classified as invalid input or
  not-executed instead of being misread as a real route mismatch

What current `H2O-only` diagnostic evidence cannot validate by itself:
- full H2O seal / sample equivalence
- current-bench CO2 replacement readiness
- final acceptance or cutover readiness

Bench/device green lights still missing:
- H2O single-route bring-up green
- CO2 single-route bring-up green
- single temperature group bring-up green
- accepted V1/V2 route-diff baseline on a reviewed bench/device scenario
- numeric agreement closure good enough for cutover, not just presence/sample
  count agreement

Current gate summary:

| Gate | State | Notes / Current evidence |
|------|-------|--------------------------|
| 1. Smoke Green | Green | [summary.json](/D:/gas_calibrator/src/gas_calibrator/v2/output/smoke_v2_minimal/run_20260324_141703/summary.json) completed with both H2O and CO2 point summaries and `route_trace.jsonl`. |
| 2. Safe Green | Green | [test_report.json](/D:/gas_calibrator/src/gas_calibrator/v2/output/test_v2_safe/test_report.json) shows `overall_passed = true`; latest safe run artifacts are under `run_20260324_144026`. |
| 3. route_trace Present | Green | smoke / safe / fit-ready smoke outputs all contain `route_trace.jsonl`. |
| 4. Fit-Ready Smoke Green | Green | [summary.json](/D:/gas_calibrator/src/gas_calibrator/v2/output/fit_ready_smoke/run_20260323_183955/summary.json) completed; [manifest.json](/D:/gas_calibrator/src/gas_calibrator/v2/output/fit_ready_smoke/run_20260323_183955/manifest.json), `results.json`, `qc_report.json`, `route_trace.jsonl`, and `calibration_coefficients.xlsx` are present. |
| 5. completed/progress Semantics Correct | Green in simulation | Recent smoke / fit-ready smoke summaries report consistent `completed_points`, `total_points`, and `progress`. Bench/device signoff is still open. |
| 6. `run_v2.py --headless` clean entry | Green | Formal V2 headless mainline is the recommended non-UI launcher. |
| 7. H2O Single-Route Bring-Up Green | Red | H2O-only no-gas replacement validation can now classify invalid input vs not-executed vs real mismatch, but signed-off bench/device evidence for a successful H2O route is still missing. |
| 8. CO2 Single-Route Bring-Up Green | Red | Control logic and trace hooks are ready, but there is no signed-off bench/device CO2-only evidence yet. |
| 9. Single Temperature Group Bring-Up Green | Red | No signed-off single-temperature-group bench/device run yet. |
| 10. `ui_v2` uses formal V2 mainline | Green | UI preview / run path now reuses formal V2 builder and point-preprocess path. |
| 11. legacy `use_v2=True` green | Green enough for continued validation | Compatibility path creates the real V2 service, but this does not by itself justify cutover. |
| 12. V1/V2 route trace Diff Green | Yellow | Tooling is now automated enough for repeatable review, but accepted-diff baseline still needs real validation runs. |
| 12A. `co2_only + skip_co2_ppm=[0]` replacement validation | Yellow | This is the planned primary route for a future Step 3 real-acceptance round after that phase is explicitly reopened; it is not a current Step 2 work order. Dedicated config, wrapper, bundle, and latest-index schema are in place. Latest primary evidence is [skip0_co2_only_replacement_latest.json](/D:/gas_calibrator/src/gas_calibrator/v2/output/v1_v2_compare/skip0_co2_only_replacement_latest.json), currently `NOT_EXECUTED`: V1 baseline acquisition progressed, but the current real primary latest still blocks before a valid CO2 route diff. |
| 12B. `H2O-only` diagnostic validation | Yellow | This is now fallback diagnostic-only evidence. Historical H2O latest artifacts are stale for the current bench because humidity feedback is invalid; they are not current acceptance evidence and must not drive current gate decisions. |
| 12C. Simulated Full-Route Coverage | Green for regression, not acceptance | Offline replay/sim can now generate full-route compare artifacts, latest indexes, bundles, and UI-consumable evidence without holding the bench. Current protocol-backed simulation covers analyzer, PACE, GRZ5013, chamber, relay, and thermometer behavior. These artifacts must remain explicitly marked `simulated` / `simulated_protocol` and must not replace the real primary latest. |
| 12D. Simulation Suite Gates (`smoke/regression/nightly/parity`) | Green for offline regression, not acceptance | `run_simulation_suite` now executes all defined suites and leaves `suite_summary.json`, `suite_summary.md`, `suite_analytics_summary.json`, `suite_acceptance_plan.json`, and `suite_evidence_registry.json`. These gates protect parity/resilience/simulated protocol regressions only; they do not replace real acceptance. |
| 12E. Acceptance Governance / Promotion Dry Run | Yellow by design | V2 now defines evidence source/state, acceptance level/scope, review/approval state, and dry-run promotion planning for `real_primary_latest`, but the real acceptance path is intentionally still blocked until bench/device closure exists. |
| 13. Default-cutover rollback strategy ready | Red | Checklist maintenance is in place, but no default-switch dry run should be performed yet. |
| 14. V1 remains frozen | Green | This V2 alignment work has stayed out of V1 production paths. |

Why default cutover is still not recommended:
- Real replacement validation is not numerically closed yet. In
  [compare_report_0c_quick_skip0_fast_20260323_154847.json](/D:/gas_calibrator/src/gas_calibrator/v2/output/v1_v2_compare/compare_report_0c_quick_skip0_fast_20260323_154847.json),
  `overall_score_pct = 59.18`, `h2o_mmol = 0/7`, `h2o_ratio_f = 0/7`,
  `co2_ppm = 4/7`, and `co2_ratio_f = 4/7`.
- The latest current primary evidence
  [control_flow_compare_report.json](/D:/gas_calibrator/src/gas_calibrator/v2/output/v1_v2_compare/real_skip0_co2_only_20c_afterfix/control_flow_compare_report.json)
  is still not acceptance evidence:
  - compare status is `NOT_EXECUTED`
  - target route remains `co2`
  - evaluable flags are correctly `false`
  - V2 failed at `first_failure_phase = "v2:precheck.device_connection"`
- the main latest index is
  [skip0_co2_only_replacement_latest.json](/D:/gas_calibrator/src/gas_calibrator/v2/output/v1_v2_compare/skip0_co2_only_replacement_latest.json)
  and is now the only current primary evidence entry
- simulated full-route artifacts may be green while the real primary latest is
  still yellow; this is expected and does not justify cutover
- simulated protocol artifacts now cover relay switching and aligned reference
  rows, but this still only proves offline logic coverage, not real bench
  device acceptance
- acceptance readiness and promotion-plan artifacts may show "dry_run_only"
  green-on-offline gates while real acceptance is still missing; this is
  expected and must not be misread as approval to publish real primary latest
- The newest short-window V1 evidence has progressed past MODE2 precheck and
  into the 20C CO2 sweep pre-temperature-stability phase, but that still does
  not prove final route equivalence or sample equivalence.
- The current H2O-only path is a fallback diagnostic route. A `NOT_EXECUTED`
  or `INVALID_PROFILE_INPUT` H2O-only compare result is diagnostic, not
  acceptance evidence.
- The current compare diagnosis still points to analyzer mapping / fleet mismatch
  risk and H2O-side control or settling bias as open causes.
- H2O-only, CO2-only, and single-temperature-group bench/device bring-up have
  not been signed off.
- `0 ppm` remains explicitly skippable in replacement validation because it is
  currently unavailable on bench.
- H2O remains out of scope for the main replacement route because the humidity
  generator temperature changes while humidity feedback stays static.
- This branch is still in replacement-validation mode: V1 remains untouched,
  and the default entry remains unchanged on purpose.

Conditions that should be true before recommending cutover:
- Smoke, safe, and fit-ready smoke remain green on the current branch.
- V1/V2 route-trace review is automated and the accepted differences are
  documented for the target bench scenario.
- `co2_only + skip_co2_ppm=[0]` replacement validation is repeatable for the
  in-scope CO2 points with matching control sequence, presence, and sample
  count, plus acceptable numeric agreement.
- H2O single-route, CO2 single-route, and single-temperature-group bench/device
  validations are all green.
- Rollback SOP and default-entry change plan are written, reviewed, and dry-run
  tested.

## Related References

- [v1_to_v2_behavior_contract.md](/D:/gas_calibrator/docs/architecture/v1_to_v2_behavior_contract.md)
- [run_v2.py](/D:/gas_calibrator/src/gas_calibrator/v2/scripts/run_v2.py)
- [route_trace_diff.py](/D:/gas_calibrator/src/gas_calibrator/v2/scripts/route_trace_diff.py)
- [compare_v1_v2_control_flow.py](/D:/gas_calibrator/src/gas_calibrator/v2/scripts/compare_v1_v2_control_flow.py)
- [verify_v1_v2_skip0_co2_only_replacement.py](/D:/gas_calibrator/src/gas_calibrator/v2/scripts/verify_v1_v2_skip0_co2_only_replacement.py)
- [verify_v1_v2_skip0_replacement.py](/D:/gas_calibrator/src/gas_calibrator/v2/scripts/verify_v1_v2_skip0_replacement.py)
- [run_v1_route_trace.py](/D:/gas_calibrator/src/gas_calibrator/v2/scripts/run_v1_route_trace.py)
- [test_v2_safe.py](/D:/gas_calibrator/src/gas_calibrator/v2/scripts/test_v2_safe.py)
- [test_v2_device.py](/D:/gas_calibrator/src/gas_calibrator/v2/scripts/test_v2_device.py)
- [app.py](/D:/gas_calibrator/src/gas_calibrator/v2/ui_v2/app.py)
- [legacy_runner.py](/D:/gas_calibrator/src/gas_calibrator/v2/adapters/legacy_runner.py)

## Gate Definitions

Each checklist item must be verified with:

- a concrete command or manual action
- a concrete expected result
- a concrete artifact or evidence path when applicable

Do not mark a gate green based only on "completed" status text. Prefer
artifact-backed evidence.

## Cutover Gates

### 1. Smoke Green

Purpose:
- Verify the minimal V2 headless control flow completes end-to-end.

Current status (2026-03-24):
- Green in simulation.
- Latest evidence:
  - [summary.json](/D:/gas_calibrator/src/gas_calibrator/v2/output/smoke_v2_minimal/run_20260324_141703/summary.json)
  - [point_summaries.json](/D:/gas_calibrator/src/gas_calibrator/v2/output/smoke_v2_minimal/run_20260324_141703/point_summaries.json)
  - [route_trace.jsonl](/D:/gas_calibrator/src/gas_calibrator/v2/output/smoke_v2_minimal/run_20260324_141703/route_trace.jsonl)

Recommended command:
```powershell
$env:PYTHONPATH='D:\gas_calibrator\src'
python -m gas_calibrator.v2.scripts.run_v2 --config src/gas_calibrator/v2/configs/smoke_v2_minimal.json --simulation --headless
```

Pass condition:
- process exits `0`
- run `summary.json` reports `status.phase = completed`
- H2O and CO2 both appear in `point_summaries.json`

Evidence:
- latest run under `src/gas_calibrator/v2/output/smoke_v2_minimal/run_*`
- `summary.json`
- `point_summaries.json`

### 2. Safe Green

Purpose:
- Verify the safe path is a reliable minimum fallback chain, not just a
  connection check.

Current status (2026-03-24):
- Green in simulation.
- Latest evidence:
  - [test_report.json](/D:/gas_calibrator/src/gas_calibrator/v2/output/test_v2_safe/test_report.json)
  - [summary.json](/D:/gas_calibrator/src/gas_calibrator/v2/output/test_v2_safe/summary.json)
  - [route_trace.jsonl](/D:/gas_calibrator/src/gas_calibrator/v2/output/test_v2_safe/run_20260324_144026/route_trace.jsonl)
  - `overall_passed = true`
  - single-point flow, QC, and algorithms all passed

Recommended command:
```powershell
$env:PYTHONPATH='D:\gas_calibrator\src'
python -m gas_calibrator.v2.scripts.test_v2_safe
```

Pass condition:
- script exits `0`
- output/report contains all `PASS`:
  - Connection test
  - Single-point flow test
  - QC pipeline test
  - Algorithm engine test

Evidence:
- [test_report.json](/D:/gas_calibrator/src/gas_calibrator/v2/output/test_v2_safe/test_report.json)
- latest safe run under `src/gas_calibrator/v2/output/test_v2_safe/run_*`

### 3. route_trace Present

Purpose:
- Verify each run leaves a machine-readable execution trace for V1/V2 route
  comparison.

Current status (2026-03-24):
- Green.
- Trace files are present in recent smoke, safe, and fit-ready smoke outputs.
- Route-trace review is now scriptable through
  [route_trace_diff.py](/D:/gas_calibrator/src/gas_calibrator/v2/scripts/route_trace_diff.py)
  and
  [compare_v1_v2_control_flow.py](/D:/gas_calibrator/src/gas_calibrator/v2/scripts/compare_v1_v2_control_flow.py).

Recommended command:
- Can be checked on either smoke, safe, or fit-ready smoke output.

Pass condition:
- run directory contains `route_trace.jsonl`
- trace includes ordered H2O and CO2 actions
- trace generation failure does not break the run

Minimum actions expected in trace:
- route baseline / path switch
- vent on / off
- pressure set / seal
- wait result
- sample start / sample end
- cleanup

Evidence:
- `run_*/route_trace.jsonl`
- `summary.json.stats.output_files`
- `manifest.json.artifacts.output_files`

### 4. Fit-Ready Smoke Green

Purpose:
- Verify V2 can produce not only control-flow artifacts but also fit-ready-ish
  result outputs under simulation.

Current status (2026-03-24):
- Green in simulation.
- Latest evidence:
  - [summary.json](/D:/gas_calibrator/src/gas_calibrator/v2/output/fit_ready_smoke/run_20260323_183955/summary.json)
  - [manifest.json](/D:/gas_calibrator/src/gas_calibrator/v2/output/fit_ready_smoke/run_20260323_183955/manifest.json)
  - `results.json`, `qc_report.json`, `route_trace.jsonl`, and
    `calibration_coefficients.xlsx` are present in the same run directory.

Recommended command:
```powershell
$env:PYTHONPATH='D:\gas_calibrator\src'
python -m gas_calibrator.v2.scripts.run_v2 --config src/gas_calibrator/v2/configs/fit_ready_smoke.json --simulation --headless
```

Pass condition:
- process exits `0`
- `summary.json.status.phase = completed`
- output contains:
  - `summary.json`
  - `manifest.json`
  - `results.json`
  - `point_summaries.json`
  - `qc_report.json`
  - `qc_report.csv`
  - `route_trace.jsonl`
- if configuration enables auto-fit, `calibration_coefficients.xlsx` is present

Evidence:
- latest run under `src/gas_calibrator/v2/output/fit_ready_smoke/run_*`

### 5. completed/progress Semantics Correct

Purpose:
- Verify top-level run status is trustworthy enough to serve as a production
  replacement signal.

Verification method:
- run fit-ready smoke or another route-expanded scenario
- inspect the generated `summary.json`
- confirm the top-level counts match actual logical completed points rather
  than raw route-expansion side effects

Pass condition:
- `completed_points <= total_points`
- `0.0 <= progress <= 1.0`
- `completed_points` matches the logical completed point set reflected in
  `point_summaries.json`
- no `completed but no samples` false-success cases

Evidence:
- `summary.json`
- `point_summaries.json`
- relevant state/progress tests under `tests/v2/`

### 6. run_v2.py --headless Is a Clean Independent Entry

Purpose:
- Verify the formal V2 headless entry is independent of UI and unrelated side
  chains, so it can be trusted as the production-grade non-UI launcher.

Recommended command:
```powershell
$env:PYTHONPATH='D:\gas_calibrator\src'
python -m gas_calibrator.v2.scripts.run_v2 --config src/gas_calibrator/v2/configs/smoke_v2_minimal.json --simulation --headless
```

Pass condition:
- process exits `0`
- headless mode does not require importing `ui_v2` at module import time
- headless mode does not fail because of unrelated UI / analytics / adapter
  side-chain imports
- underlying execution path is:
  - `entry.load_config_bundle()`
  - `entry.create_calibration_service_from_config()`
  - `CalibrationService`

Evidence:
- [run_v2.py](/D:/gas_calibrator/src/gas_calibrator/v2/scripts/run_v2.py)
- `tests/v2/test_run_v2.py`
- a successful headless smoke run

### 7. H2O Single-Route Bring-Up Green

Purpose:
- Verify H2O route can independently complete: path switch, humidity wait,
  dewpoint readiness, pressure seal, sample, QC.

Current status (2026-03-24):
- Not green yet.
- V2 control order, prechecks, safe stop, retry behavior, and route trace are
  aligned enough for focused H2O bench validation.
- What is still missing is signed-off real-device evidence for an H2O-only
  scenario.

Verification method:
- Use a single-route H2O bench/device scenario or a narrowed compare scenario
  that only exercises H2O.
- Prefer real device validation after smoke/safe are green.

Pass condition:
- H2O route reaches sample phase
- H2O point summaries are present
- no false-completed points for failed H2O waits/seal
- For current no-gas bench use, a replacement-validation run must at minimum
  classify:
  - invalid preset input
  - target route not executed
  - executed target route with real action diff
  before it can be used as meaningful evidence

Evidence:
- run log showing H2O ordering
- `point_summaries.json` with H2O tags
- `route_trace.jsonl` showing:
  - `set_h2o_path`
  - humidity/dewpoint waits
  - `seal_route`
  - `sample_start`
  - `sample_end`

### 8. CO2 Single-Route Bring-Up Green

Purpose:
- Verify CO2 route can independently complete: baseline, conditioning, seal,
  pressure control, sample, QC, cleanup.

Current status (2026-03-24):
- Not green yet.
- CO2 control sequence, skip rules, retry, prechecks, and route trace are in
  place for focused validation.
- What is still missing is signed-off real-device evidence for a CO2-only
  scenario.

Verification method:
- Use a single-route CO2 bench/device scenario or narrowed compare scenario.

Pass condition:
- CO2 route reaches sample phase
- CO2 point summaries are present
- no false-completed points for failed route conditioning / seal / pressure

Evidence:
- run log showing CO2 ordering
- `point_summaries.json` with CO2 tags
- `route_trace.jsonl` showing:
  - `route_baseline`
  - `set_co2_valves`
  - `wait_route_soak`
  - `seal_route`
  - `set_pressure`
  - `sample_start`
  - `sample_end`
  - `cleanup`

### 9. Single Temperature Group Bring-Up Green

Purpose:
- Verify one complete temperature group can run H2O then CO2 without false
  completion and with correct per-route completion accounting.

Current status (2026-03-24):
- Not green yet.
- Single-temperature-group logic is close enough to V1 for targeted bring-up,
  but there is still no signed-off bench/device run showing one full group is
  stable end-to-end.

Verification method:
- Run a narrowed config with one selected temperature group and both routes.

Pass condition:
- temperature group reaches finalization
- completed-point count matches actual sampled points
- failed/skip route points are not reported as completed

Evidence:
- `summary.json`
- `point_summaries.json`
- `route_trace.jsonl`

### 10. ui_v2 Uses Formal V2 Mainline

Purpose:
- Verify the new UI is not running a hidden alternate calibration world.

Verification method:
- start UI with a config path
- confirm it builds service through the shared V2 entry/builder path

Recommended command:
```powershell
$env:PYTHONPATH='D:\gas_calibrator\src'
python -m gas_calibrator.v2.ui_v2.app --config D:\gas_calibrator\src\gas_calibrator\v2\configs\smoke_v2_minimal.json
```

Pass condition:
- UI launches
- underlying service creation path resolves to:
  - `entry.load_config_bundle()`
  - `entry.create_calibration_service_from_config()`
  - `CalibrationService`

Evidence:
- [app.py](/D:/gas_calibrator/src/gas_calibrator/v2/ui_v2/app.py)
- [app_facade.py](/D:/gas_calibrator/src/gas_calibrator/v2/ui_v2/controllers/app_facade.py)
- relevant UI entry tests under `tests/v2/`

### 11. legacy `use_v2=True` Green

Purpose:
- Verify legacy callers can explicitly opt into the real V2 mainline instead
  of falling back to a TODO or separate legacy world.

Verification method:
- run the legacy compatibility tests for V2
- inspect creation path

Pass condition:
- `create_runner(..., use_v2=True)` creates a real `CalibrationService`
- uses the same shared V2 builder path as `run_v2.py`
- output/result structure matches formal V2 shape

Evidence:
- [legacy_runner.py](/D:/gas_calibrator/src/gas_calibrator/v2/adapters/legacy_runner.py)
- `tests/v2/test_legacy_runner_v2.py`

### 12. V1/V2 route trace Diff Green

Purpose:
- Verify V2 control-flow ordering is close enough to V1 to support real
  replacement, not just isolated route success.

Current status (2026-03-24):
- Yellow.
- The compare path is now more automated, but "diff green" still requires a
  reviewed bench/device scenario, not only tool availability.
- Route-diff interpretation now must respect execution validity:
  - `INVALID_PROFILE_INPUT` means the compare input was wrong
  - `NOT_EXECUTED` means the target route never actually ran
  - only a valid target-route execution can be treated as a real route mismatch

Verification method:
1. preferred single-command flow:
```powershell
$env:PYTHONPATH='D:\gas_calibrator\src'
python -m gas_calibrator.v2.scripts.compare_v1_v2_control_flow --v1-config <v1_config.json> --v2-config <v2_config.json> --temp <temp> --replacement-skip0 --skip-connect-check
```
2. low-level/manual flow if needed:
   generate a V1 trace with:
```powershell
$env:PYTHONPATH='D:\gas_calibrator\src'
python -m gas_calibrator.v2.scripts.run_v1_route_trace --config <v1_config.json> --temp <temp>
```
3. generate the matching V2 trace from smoke / single-route / single-temp run
4. compare them with:
```powershell
$env:PYTHONPATH='D:\gas_calibrator\src'
python -m gas_calibrator.v2.scripts.route_trace_diff --v1-trace <v1_route_trace.jsonl> --v2-trace <v2_route_trace.jsonl>
```

Pass condition:
- no unexplained route-level action gaps in H2O or CO2
- no unexplained ordering mismatches in:
  - route baseline / path switch
  - vent on/off
  - seal / pressure set
  - post-pressure wait
  - sample start / sample end
  - cleanup
- any accepted differences are documented in the review record

Evidence:
- `route_trace.jsonl` from both sides
- route diff output
- review note of accepted vs rejected differences

### 12B. `H2O-only` Diagnostic Validation

Purpose:
- Keep diagnostic comparison moving when the main skip0 route cannot yet be
  evaluated, without overstating what has been proven.

Current status (2026-03-25):
- Yellow.
- The H2O-only preset is now a fallback diagnostic preset, not the main
  acceptance route.
- The current `h2o_only_replacement_latest.json` artifact should be treated as
  stale diagnostic evidence for the present bench, not as current acceptance
  evidence.
- It must first prove that the compare run is valid before any route mismatch
  is interpreted.
- Latest evidence index:
  - [h2o_only_replacement_latest.json](/D:/gas_calibrator/src/gas_calibrator/v2/output/v1_v2_compare/h2o_only_replacement_latest.json)
- Latest reviewed run directory in this workspace:
  - [codex_h2o_only_status_check2](/D:/gas_calibrator/src/gas_calibrator/v2/output/v1_v2_compare/codex_h2o_only_status_check2)

Verification method:
1. run the fixed H2O-only wrapper
2. confirm the report classifies the run as one of:
   - `MATCH`
   - `MISMATCH`
   - `NOT_EXECUTED`
   - `INVALID_PROFILE_INPUT`
3. if the result is `NOT_EXECUTED`, review the first failure phase before
   discussing route mismatch
4. if the result is `INVALID_PROFILE_INPUT`, fix points source / temp filter
   before re-running
5. for current H2O-only fallback work, treat the early-stop path as the first
   target behavior to align

Recommended command:
```powershell
$env:PYTHONPATH='D:\gas_calibrator\src'
python -m gas_calibrator.v2.scripts.verify_v1_v2_h2o_only_replacement --temp 20
```

Pass condition for current bench phase:
- compare report is not misleading
- report clearly exposes:
  - `compare_status`
  - `entered_target_route`
  - `target_route_event_count`
  - `valid_for_route_diff`
  - `first_failure_phase`
- if target route is entered on both sides, route diff can be reviewed
- if target route is not entered, result is treated as diagnostic, not as a
  real H2O mismatch

Interpretation boundary:
- This gate currently proves early-stop-path consistency first.
- The latest reviewed V1 early-stop evidence in this workspace is still
  `wait_temperature(timeout)`, not a newer humidity-timeout proof.
- This gate does not yet prove full H2O seal/sample equivalence.
- This gate does not justify cutover or default-entry change.

### 12A. `co2_only + skip_co2_ppm=[0]` Replacement Validation

Purpose:
- Keep V1/V2 replacement validation moving when `0 ppm` is not a trustworthy
  acceptance target, while still proving the important control-flow and sample
  existence behavior.

Current status (2026-03-25):
- Yellow.
- This is now the main bench replacement route because:
  - `0 ppm` is unavailable
  - other gases are restored
  - H2O is out of scope while humidity feedback remains invalid
- Reviewed numeric reference remains useful but not yet good enough for cutover:
  [compare_report_0c_quick_skip0_fast_20260323_154847.json](/D:/gas_calibrator/src/gas_calibrator/v2/output/v1_v2_compare/compare_report_0c_quick_skip0_fast_20260323_154847.json)
  shows:
  - `presence_matches = 7 / 7`
  - `sample_count_matches = 7 / 7`
  - `overall_score_pct = 59.18`
- The current source-of-truth config is:
  [replacement_skip0_co2_only_real.json](/D:/gas_calibrator/src/gas_calibrator/v2/configs/validation/replacement_skip0_co2_only_real.json)
- The stable latest-index path for this main route is now:
  [skip0_co2_only_replacement_latest.json](/D:/gas_calibrator/src/gas_calibrator/v2/output/v1_v2_compare/skip0_co2_only_replacement_latest.json)
- Current real-run result is still not acceptance evidence:
  - current compare status is expected to remain non-green until bench blockers are cleared
  - V1 port access was blocked by a live `run_app.py`
  - V2 reported `first_failure_phase = "v2:startup.sensor_precheck"`
- This means the primary replacement-validation path is now standardized on a
  dedicated CO2-only real compare config, but bench/device closure is still open.

This gate is specifically for narrowed replacement validation, not for proving
that V2 has already taken over from V1.

Verification method:
1. run the fixed CO2-only skip0 wrapper
2. focus review on:
   - CO2 400 ppm points
   - CO2 1000 ppm points
3. treat `0 ppm` as intentionally skipped for this phase

Recommended command:
```powershell
$env:PYTHONPATH='D:\gas_calibrator\src'
python -m gas_calibrator.v2.scripts.verify_v1_v2_skip0_co2_only_replacement --temp 20
```

Pass condition:
- control sequence is reviewed and acceptable
- presence matches for the narrowed point set
- sample count matches for the narrowed point set
- compare report explicitly shows:
  - `replacement_validation.only_in_v1 = []`
  - `replacement_validation.only_in_v2 = []`
  - `replacement_validation.sample_count_matches = true`
  - `replacement_validation.route_action_order_differences = []`
- stable latest index points to a run directory that contains:
  - `v1_route_trace.jsonl`
  - `v2_route_trace.jsonl`
  - `route_trace_diff.txt`
  - `point_presence_diff.json`
  - `sample_count_diff.json`
  - `control_flow_compare_report.json`
  - `control_flow_compare_report.md`
- route-sequence / vent / valves / pressure / sample action diffs are either
  green or explicitly accepted
- numeric agreement for H2O / CO2 400 ppm / CO2 1000 ppm is improving enough to
  support the next bench round

Interpretation boundary:
- A green `skip0` run proves only narrowed control-flow replacement validation.
- A green `co2_only + skip0` run still does not prove H2O equivalence.
- A green `skip0` run does not prove true `0 ppm` equivalence.
- A green `skip0` run does not prove full numeric equivalence.
- A green `skip0` run does not by itself justify default cutover.

Non-pass interpretations:
- If presence and sample count are green but numeric agreement is still weak,
  the result is "replacement-validation path usable", not "ready to cut over".
- If route/action diffs are explainable in simulation but not yet reviewed on
  bench/device, keep this gate yellow.

Evidence:
- [compare_report_0c_quick_skip0_fast_20260323_154847.json](/D:/gas_calibrator/src/gas_calibrator/v2/output/v1_v2_compare/compare_report_0c_quick_skip0_fast_20260323_154847.json)
- [control_flow_compare_report.json](/D:/gas_calibrator/src/gas_calibrator/v2/output/v1_v2_compare/skip0_replacement_20260324_115209/control_flow_compare_report.json)
- `route_trace_diff.txt`
- `v1_route_trace.jsonl`
- `v2_route_trace.jsonl`
- `point_presence_diff.json`
- `sample_count_diff.json`
- `control_flow_compare_report.json`
- `control_flow_compare_report.md`
- `compare_report_*.json`
- grouped key-action maintenance lives in
  [route_trace_diff.py](/D:/gas_calibrator/src/gas_calibrator/v2/scripts/route_trace_diff.py)
  `KEY_ACTION_GROUPS`

### 13. Default-Cutover Rollback Strategy Ready

Purpose:
- Ensure switching the default entry to V2 is reversible and operationally
  safe.

Verification method:
- review the exact default-entry change plan before cutover
- verify the rollback command / config switch is documented and tested in a dry
  run

Pass condition:
- rollback path is explicitly documented
- V1 entry remains runnable as historical fallback
- operator can state:
  - what gets switched
  - how to switch back
  - where V1 reference output/logs still live
- cutover batch does not remove V1 entry or emergency fallback instructions

Evidence:
- release / cutover note
- operator rollback SOP
- documented default-entry diff plan

### 14. V1 Remains Frozen

Purpose:
- Ensure cutover preparation does not destabilize the current rollback path.

Verification method:
- verify no V1 production files were changed as part of V2 cutover work

Pass condition:
- no changes under:
  - `src/gas_calibrator/workflow/**`
  - `src/gas_calibrator/ui/**`
  - `src/gas_calibrator/devices/**`
- V1 remains available as rollback reference

Evidence:
- code review / diff review
- repository change list for the cutover batch

## Recommended Verification Order

Run the gates in this order:

1. Smoke Green
2. Safe Green
3. route_trace Present
4. Fit-Ready Smoke Green
5. completed/progress Semantics Correct
6. run_v2.py --headless Is a Clean Independent Entry
7. H2O Single-Route Bring-Up Green
8. CO2 Single-Route Bring-Up Green
9. Single Temperature Group Bring-Up Green
10. ui_v2 Uses Formal V2 Mainline
11. legacy `use_v2=True` Green
12. V1/V2 route trace Diff Green
12A. `skip_co2_ppm=[0]` Replacement Validation
12B. `H2O-only / no-gas` Replacement Validation
13. Default-Cutover Rollback Strategy Ready
14. V1 Remains Frozen

This ordering reduces noise:
- first prove the simulation mainline
- then prove route observability
- then prove fit/artifact closure and top-level status trustworthiness
- then confirm the formal headless entry is production-usable
- then move to route-level and bench/device bring-up
- then confirm alternate entry compatibility and route-level equivalence
- finally close rollback / freeze conditions

## Cutover Decision Rule

V2 can be proposed as a formal cutover candidate only when:

- all checklist items above are green
- there is no known false-completed route behavior
- route trace exists for validation runs
- route-trace diff against V1 is reviewed and acceptable
- narrowed `skip_co2_ppm=[0]` replacement validation is green enough to justify
  the final bench/device round
- fit-ready export chain is proven
- completed/progress semantics are trustworthy
- `run_v2.py --headless` is a clean production-grade entry
- single-route and single-temperature-group bring-up are stable
- rollback strategy is written and reviewed
- V1 remains untouched and available as rollback
- offline replay / simulation regression continues to stay green while V1 keeps
  the production window
- simulated latest indexes never override the real primary latest

If any one of these is red, V2 is still in pre-cutover integration state.
As of 2026-03-24, this rule is not yet satisfied, so default cutover to V2 is
still not recommended.

Practical interpretation for the next round:
- Treat this checklist as a future bench/device cutover worksheet, not as
  authorization for the current Step 2 branch.
- Keep V1 frozen.
- Keep the default entry unchanged.
- Do not start real-device bring-up, real compare / real verify, or
  `real_primary_latest` refresh from this document alone.
- Use V2 to continue simulation / replay / suite / parity / resilience /
  offline review hardening until Step 3 is explicitly opened.
- Use the new acceptance/analytics/lineage artifacts to organize operator /
  reviewer / approver work without touching the current V1 production path.
What simulated full-route coverage can validate right now:
- compare/report/latest/bundle generation for 0 ppm, H2O, CO2, multi-temp, and
  multi-pressure route logic
- protocol-level device behavior for chamber/H2O timing, waits, timeouts, and
  skipped-by-profile semantics without touching bench serial ports
- failure classification, stale/latest governance, and UI validation cockpit
  behavior without bench access
- regression of artifact schema, `first_failure_phase`, `evidence_state`, and
  `primary real latest missing` handling
- storage/export resilience behavior such as dynamic CSV header expansion,
  auxiliary export failure isolation, and artifact-level parity reporting

What simulated full-route coverage cannot validate by itself:
- real-device acceptance
- true 0 ppm equivalence
- H2O hardware equivalence
- production cutover readiness
