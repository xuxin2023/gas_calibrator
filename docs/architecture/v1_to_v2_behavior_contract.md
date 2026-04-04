# V1 to V2 Behavior Contract

## Purpose

This document defines the behavior contract V2 must satisfy before it can be
accepted as a replacement for the current V1 calibration workflow.

The target is not internal code similarity. The target is operator-visible
behavioral equivalence for the successful chain:

1. Control the correct devices and valves
2. Wait for ready and stable conditions in the correct order
3. Sample only after the required route and pressure conditions are met
4. Persist samples and point-level results
5. Export run summary, QC outputs, coefficients, and artifacts

V1 is frozen maintenance and historical reference only. New work must satisfy
this contract in V2.

## Current Bench Context (2026-03-26)

Current replacement-validation assumptions:

- `0 ppm` is unavailable
- other gas cylinders are restored
- humidity generator fault: chamber temperature changes, humidity feedback
  stays static
- the primary bench replacement route is `co2_only + skip_co2_ppm=[0]`
- `H2O-only` is a fallback diagnostic route only; it is currently out of scope
  for main replacement acceptance

Interpretation boundary:

- quick diagnostic != formal replacement validation
- replacement validation != cutover readiness
- current work remains inside the three-step strategy's Step 2
  (production-grade platform construction), not Step 3 release/cutover
- simulated full-route coverage != real acceptance evidence
- simulated protocol coverage != replay coverage
- Chinese-default V2 UI != real acceptance closure
- current CO2-only replacement validation does not prove `0 ppm` equivalence
- current CO2-only replacement validation does not prove H2O equivalence
- the default entry remains V1 on purpose
- while V1 keeps production ownership, offline simulation / replay is the main
  way to keep V2 validation moving without holding the bench
- the current simulation mainline now includes protocol-backed GRZ5013 and
  temperature-chamber fakes, plus analyzer fleet, PACE, relay, and digital
  thermometer protocol fakes, but those runs remain non-acceptance evidence
- the current `Devices` workbench in `ui_v2` is part of that same offline-only
  validation line: it operates simulated/fake analyzer, PACE, GRZ5013,
  chamber, relay, thermometer, and pressure-gauge panels only, and must not
  open real serial/COM ports or issue real manual device actions in this phase
- the same workbench may now emit standalone simulated diagnostic evidence for
  operator/engineer review, but that evidence must stay explicitly marked as
  simulated diagnostic-only evidence and must not masquerade as real
  acceptance evidence or refresh the real primary latest
- the same simulation-only workbench now also supports operator presets,
  preset groups, recent presets, filtered action history, evidence/snapshot
  quick jumps, and simple before/after snapshot comparison so common offline
  diagnostic drills are faster to review without exposing any real-device
  control path
- the digital thermometer fake now follows a closer continuous ASCII-stream
  contract (`2400` / `8N1` / sign / decimal / `°C` / `CRLF`) and exposes
  warmup, stale, drift, malformed, and `+200°C`-offset behaviors for offline
  regression only
- the current pressure-reference fake strategy follows the active V2 driver
  contract: because the present device path is Paroscientific-style, V2 now
  simulates the matching `*IDIDCMD` command family and reference-state modes
  needed by sampling/parity/analytics; if a future real driver differs, that
  driver/log contract should override handbook assumptions
- simulated protocol runs can now exercise route-layer relay switching and
  aligned reference-row summary logic, but they still do not replace real
  bench/device acceptance evidence
- simulated protocol compare now also carries explicit relay physical mismatch
  and reference-quality gates; these improve offline diagnosability but still
  do not upgrade simulated evidence into real acceptance evidence
- reference-instrument state now remains explicit through V2 runtime rows and
  readable/export artifacts via `pressure_reference_status` and
  `thermometer_reference_status`, so degraded or failed reference evidence is
  visible to suite/parity/analytics review instead of being hidden inside
  simulation-only metadata
- V2 is also syncing V1's current storage/export resilience behavior:
  dynamic CSV header expansion, safe CSV rewrite when schema evolves,
  auxiliary export failure isolation, and a cleaner reporting-mode split
  between formal output and diagnostic fleet statistics
- V2 now also separates artifact roles more explicitly:
  - `execution_rows`
  - `execution_summary`
  - `diagnostic_analysis`
  - `formal_analysis`
  and emits `points_readable.csv` as an execution-level readable artifact.
  This improves operator/engineer readability without turning readable point
  summaries into formal analyzer-summary evidence.
- offline validation is now organized into explicit simulation suites
  (`smoke`, `regression`, `nightly`, `parity`) so parity and artifact
  resilience are exercised as first-class regression products rather than
  ad hoc commands
- simulation suites now write both `suite_summary.json` and
  `suite_summary.md`, so parity/resilience/nightly behave more like sustained
  offline quality gates than one-off developer commands
- the current review surface is a compact review center within existing V2
  result/report pages, aggregating suite/parity/resilience/workbench evidence,
  acceptance-readiness summary, analytics digest, and lineage summary for
  operator/reviewer/approver views without opening a new major page
- that same review center may now maintain a lightweight cross-run index over
  recent run folders and evidence artifacts so reviewer/approver workflow is
  no longer limited to the current run snapshot; this remains offline
  simulation/replay evidence only and must not imply real acceptance
- that same review center may now also filter offline evidence by source
  family (`run` / `suite` / `workbench`) while keeping operator / reviewer /
  approver summaries explicit about remaining acceptance-readiness gaps; this
  remains Step 2 reviewer ergonomics only and must not imply real acceptance
- that same review center may now also expose analytics/lineage summaries,
  lightweight risk levels, source-kind coverage summaries, and recent-window
  filters on top of the same lightweight cross-run index; this remains Step 2
  offline reviewer/approver ergonomics only and must not imply real
  acceptance
- that same review center may now also expose a clearer list/detail review
  workbench split with evidence summary, risk, key fields, artifact paths,
  and acceptance-readiness hints, still within the same offline Step 2 review
  boundary and still not implying real acceptance
- that same review center may now also carry structured analytics/lineage
  detail rows inside the same list/detail review surface, so reviewer /
  approver workflow can inspect evidence context without leaving the offline
  Step 2 review boundary
- that same review center may now also keep analytics/lineage context attached
  to each selected offline evidence detail row, so list/detail review keeps
  cross-run context without implying any real acceptance
- V2 now also carries a separate acceptance-governance layer for offline and
  future real evidence:
  - `evidence_source`
  - `evidence_state`
  - `acceptance_level`
  - `acceptance_scope`
  - `promotion_state`
  - `review_state`
  - `approval_state`
  - `ready_for_promotion`
  Simulated/replay/diagnostic evidence must stay structurally unable to
  masquerade as `real_acceptance`.
- V2 now emits platform-level analytics and traceability artifacts:
  - `analytics_summary.json`
  - `trend_registry.json`
  - `lineage_summary.json`
  - `evidence_registry.json`
  - `coefficient_registry.json`
  These support operator/reviewer/approver workflow and future real-history
  extension; they are not real acceptance evidence by themselves.

## UI Presentation Contract

Current V2 UI/product-surface rules that must now hold:

- `ui_v2` defaults to `zh_CN`; `en_US` exists as fallback only.
- Operator-facing shell/navigation/cards/widgets/digests should present Chinese
  labels by default, even when internal artifact keys or enums remain English.
- The `Devices` page should now behave as a Chinese-first device-operation
  workbench, not just a status overview, but it must remain simulation/fake
  driven in the current phase.
- The workbench should present a default operator view and a richer engineer
  diagnostic view within the same page rather than requiring many new pages.
- The workbench should keep common operator presets, compact history, and
  latest evidence/review cues visible in operator view; deeper context,
  history detail, and snapshot comparison may stay in engineer view.
- The workbench should group presets by device family (`analyzer`, `pace`,
  `grz`, `chamber`, `relay`, `thermometer`, `pressure`), keep featured presets
  ahead of lower-frequency entries, expose recent presets, and now allow a
  lightweight simulation-only custom preset editor for create/edit/delete of
  fake action combinations in this phase.
- The same preset surface may now act as a lightweight simulation-only preset
  manager:
  - duplicate preset
  - favorite / pin / recent usage
  - grouped preset organization
  - JSON import/export with basic schema validation
  Imported/exported bundles remain simulation-only preset definitions only;
  they must not become a real-device scripting path.
- That same preset manager may now also retain lightweight preset metadata
  such as origin/source, schema version, preset version, created/updated
  timestamps, and fake-capability summaries, plus local rename/overwrite
  import-conflict handling. This is still Step 2 local preset governance
  only; it must not be mistaken for a shared preset service or a real-device
  scripting surface.
- That same preset manager may now also maintain a lightweight preset
  directory/index across built-in, local custom, and imported
  simulation-only presets. This remains Step 2 local governance only and does
  not constitute shared preset infrastructure.
- That same preset manager may now also expose readable bundle-format,
  conflict-summary, and future-sharing-prep summaries on top of the same local
  JSON import/export path. This remains Step 2 local preset governance only
  and does not constitute a shared preset service.
- That same preset manager may now also expose explicit bundle/conflict-policy
  metadata, reserved future-sharing fields, and denser selected-preset
  metadata/fake-capability summaries on the same local JSON import/export
  path. This remains Step 2 local preset governance only.
- The workbench may persist local preset preferences such as recent presets,
  favorites, pins, layout mode, and engineer/operator display mode, but these
  are Step 2 ergonomics only. They are not a permission system and do not
  unlock real-device access.
- Display-profile preferences may now persist a lightweight local profile
  context (`1080p_compact` / `1080p_standard`) for future density tuning, but
  this remains a local Step 2 UI preference only and must not affect any
  execution or acceptance semantics.
- That display-profile context may now also carry lightweight family / monitor
  metadata and refresh from current shell window/screen size to choose a
  better 1080p compact vs standard default. This remains local V2 UI state
  only; it must not alter any simulation/runtime logic or acceptance meaning.
- That same display-profile context may now also retain strategy-version,
  resolution-class, window-class, monitor-family metadata, and refined
  auto-selection-reason metadata for lightweight monitor/window mapping. This
  remains local Step 2 UI preference state only and must not alter
  simulation/runtime logic or acceptance meaning.
- That same display-profile context may now also refresh on shell window-size
  changes with lightweight debouncing so 1920x1080 defaults remain aligned
  while manual local override still wins. This remains local Step 2 UI
  preference state only and does not alter simulation/runtime logic or
  acceptance meaning.
- That same display-profile context may now also carry readable metadata such
  as selected/resolved profile, selection mode, and mapping summary around the
  same local family/monitor/window strategy. This remains local Step 2 UI
  preference state only and does not alter simulation/runtime logic or
  acceptance meaning.
- That same display-profile context may now also carry readable aspect-ratio,
  window-area, screen-area, and profile-summary metadata around the same local
  family/monitor/window strategy. This remains local Step 2 UI preference
  state only and does not alter simulation/runtime logic or acceptance
  meaning.
- Engineer view should prefer grouped cards and collapsible sections for
  high-frequency fields first and low-frequency diagnostics second, instead of
  long undifferentiated text blocks.
- Engineer view may add small status/trend cards to compress high-frequency
  diagnostics, but it should still remain an offline simulation-only review
  surface and must not imply a Step 3 analytics platform or approval system.
- Engineer view may now also highlight recent evidence/action trends and
  anomaly-focused summary blocks, but this remains a compact diagnostic view
  over offline simulation-only data rather than a full analytics platform.
- Engineer view may also surface lightweight reference-quality trend,
  recent-action statistics blocks, and artifact/lineage summary cards, but
  this remains a compact Step 2 analysis aid rather than a Step 3 analytics
  platform or approval system.
- Engineer view may now also surface suite/analytics summary cards and compact
  suite/analytics detail sections, but this remains a compact Step 2 offline
  analysis aid rather than a Step 3 analytics platform.
- Engineer view may now also densify those same suite/analytics detail
  sections with reference-trend, export-status, and lineage context, still as
  a compact Step 2 offline analysis aid rather than a Step 3 analytics
  platform.
- Engineer view may now also condense suite/analytics card and trend summaries
  with coverage/export/reference context, still as a compact Step 2 offline
  analysis aid rather than a Step 3 analytics platform.
- High-density V2 pages should be usable on a 1920x1080 operator display:
  main content should be visible with compact layout first, and any overflow
  must remain reachable through explicit page-level or panel-level scrolling.
  Silent clipping below the visible workspace is not acceptable.
- The shell/log split may remember the last local sash height for usability,
  but restore failure must fall back to a safe default and must never block UI
  startup.
- Workbench-triggered evidence generation should produce reviewable offline
  artifacts under diagnostic-analysis semantics only; it must not pollute
  formal-analysis artifacts or imply real acceptance closure.
- Review UI should make suite/parity/resilience/workbench evidence easy to
  read together, with evidence-type, status, time, and source-family filters
  plus operator / reviewer / approver summaries, but simulated/replay
  diagnostic digests must continue to be labeled as offline review evidence
  rather than real acceptance.
- Workbench labels, summaries, button text, empty states, and fault/status
  wording must map protocol details to operator-facing Chinese semantics rather
  than exposing raw protocol command tokens as primary copy.
- Acceptance, analytics, lineage, artifact-role, review, approval, and
  promotion information shown in the UI should not leak raw internal English
  keys when a display mapping exists.
- Chinese-default UI work must remain on the V2 line only. Do not wire these
  changes back into V1 UI, and do not change `run_app.py` as part of this UI
  hardening.
- V1 production logic remains frozen during this platformization phase. Any
  parity or storage/export alignment work must stay on the V2 side unless a
  user explicitly authorizes V1 changes.
- Shell chrome should not waste the 1080p workspace budget: top metrics,
  navigation, and logs may stay visible, but the main workspace should retain
  the majority of vertical space and long log/detail areas must support
  scrolling or resizing instead of fixed-height clipping.
- Verification for this UI/localization layer remains offline-only in the
  current phase: simulation, replay, suite regression, parity/resilience, and
  UI snapshot/tests. It does not authorize real-device acceptance claims, real
  manual device control, or real primary-latest refresh.

## Reference Sources

V1 reference implementation:
- [runner.py](/D:/gas_calibrator/src/gas_calibrator/workflow/runner.py)

V2 execution chain:
- [calibration_service.py](/D:/gas_calibrator/src/gas_calibrator/v2/core/calibration_service.py)
- [temperature_group_runner.py](/D:/gas_calibrator/src/gas_calibrator/v2/core/runners/temperature_group_runner.py)
- [h2o_route_runner.py](/D:/gas_calibrator/src/gas_calibrator/v2/core/runners/h2o_route_runner.py)
- [co2_route_runner.py](/D:/gas_calibrator/src/gas_calibrator/v2/core/runners/co2_route_runner.py)

## Top-Level Replacement Contract

At the top level, a V2 run is considered behaviorally equivalent to a
successful V1 run only if all of the following are true in the same run:

1. Devices are controlled through the correct route services.
2. Wait and readiness gates are executed in the correct order.
3. Sampling starts only after route-specific gating is satisfied.
4. Non-empty results are written and exposed through `CalibrationService`.
5. `summary / QC / results / coefficients / manifest` are exported when the
   scenario is fit-ready.

This contract is intentionally sequence-oriented. A run that reaches
`COMPLETED` while skipping actual route gating or sampling does not satisfy the
replacement standard.

## Run Input Contract

V2 currently supports two input-preparation modes for UI-triggered runs:

1. `use_points_file`
2. `use_default_profile`

These modes are not equivalent in authoring workflow, but they must converge to
the same execution semantics before `CalibrationService.start(...)` begins:

- `use_points_file` remains the baseline and existing stable path.
- `use_default_profile` is an additive capability. It must compile the default
  `CalibrationPlanProfile` into a standard V2 points JSON payload first, then
  hand that compiled payload to the normal run chain.
- Neither mode may bypass the normal point-preparation semantics.
- smoke / safe / replacement-validation scripts should continue to prefer the
  configured points file path until profile-driven runs have their own explicit
  validation evidence.

Current V2 UI implementation follows this contract:

- authoring/editing profiles happens through `PlanGateway` + `ProfileStore`
- profile execution preview happens through `PlanCompiler`
- default-profile runs materialize a compiled points JSON file under UI runtime
  cache, then start the same `CalibrationService` path used by points files

## Sampling And Summary Alignment Contract

Current V1 alignment work that V2 must preserve:

- runtime sampling rows should keep the `primary_or_first_usable` analyzer
  semantics without regressing the per-analyzer prefixed data
- reference quantities must stay explicit in runtime rows:
  - `pressure_gauge_hpa`
  - `thermometer_temp_c`
  - `frame_has_data`
  - `frame_usable`
  - `frame_status`
- summary/export should support `reference_on_aligned_rows`
- aligned reference preference should follow current V1 semantics:
  - temperature prefers `thermometer_temp_c`, then chamber temperature
  - pressure prefers `pressure_gauge_hpa`
- final summary/export should expose fleet completeness rather than assuming a
  hard-coded `1/1` analyzer world

Current V2 status:

- runtime sampling semantics are aligned closely enough for offline validation:
  reference pressure, thermometer temperature, and frame usability now flow
  through sampling rows and persisted sample files
- summary/export semantics are now closer to current V1 behavior for aligned
  references and fleet completeness, but real acceptance evidence is still
  required before claiming parity
- V2 golden parity tests now compare V1 and V2 summary/export rows from the
  same synthetic runtime inputs. Any remaining mismatch should be treated as
  explicit `expected_divergence`, not as an implicit acceptable drift
- V2 now also emits artifact-level parity/resilience reports in offline runs,
  but those reports remain simulated/offline evidence and must not be confused
  with real production acceptance
- V2 parity is now both a pytest gate and a suite-level gate: the parity suite
  leaves readable/machine-readable artifacts that state tolerance rules,
  matched fields, failed fields, and expected divergence explicitly
- V2 execution-layer readable artifacts now sit beside, not inside, the formal
  analyzer summary/export chain:
  - execution rows: runtime/sample/point traces
  - execution summary: `summary.json`, `manifest.json`, `points_readable.csv`
  - diagnostic analysis: QC and temperature snapshot style outputs
  - formal analysis: analyzer/coefficient report outputs

## V1 to V2 Module Mapping

| V1 behavior | V2 replacement |
| --- | --- |
| Top-level lifecycle | [calibration_service.py](/D:/gas_calibrator/src/gas_calibrator/v2/core/calibration_service.py) |
| Temperature-group orchestration | [temperature_group_runner.py](/D:/gas_calibrator/src/gas_calibrator/v2/core/runners/temperature_group_runner.py) |
| H2O route | [h2o_route_runner.py](/D:/gas_calibrator/src/gas_calibrator/v2/core/runners/h2o_route_runner.py) |
| CO2 route | [co2_route_runner.py](/D:/gas_calibrator/src/gas_calibrator/v2/core/runners/co2_route_runner.py) |
| Valve switching | `valve_routing_service` |
| Pressure seal / target / post-stable hold | `pressure_control_service` |
| Temperature wait | `temperature_control_service` |
| Humidity-generator wait | `humidity_generator_service` |
| Dewpoint readiness and alignment | `dewpoint_alignment_service` |
| Sampling | `sampling_service.sample_point()` |
| Point QC | `qc_service.run_point_qc()` |
| Summary / manifest / exports | `artifact_service` |
| Coefficient export | `coefficient_service` |

## H2O Route Order Contract

### V1 Reference Order

In V1, a successful H2O group follows this order:

1. Close H2O path to reset route baseline.
2. Prepare pressure controller for H2O routing.
3. Prepare humidity generator target.
4. Wait for chamber temperature to reach and stabilize.
5. Wait for humidity generator to reach target.
6. Capture temperature calibration snapshot.
7. Open H2O route.
8. Wait H2O open-route pre-seal soak.
9. Wait dewpoint meter alignment to humidity-generator state.
10. Mark post-H2O CO2 zero-flush pending.
11. Start pressure-seal preparation:
    - capture pre-seal dewpoint snapshot
    - vent off
    - wait vent-off settle
    - close H2O path to seal the route
12. For each H2O pressure point:
    - set pressure target
    - wait post-pressure sample hold
    - sample
    - write result / QC
13. Cleanup H2O route back to baseline.

### V2 Required Order

V2 must preserve the same operator-visible sequence:

1. `valve_routing_service.set_h2o_path(False, lead)`
2. `pressure_control_service.prepare_pressure_for_h2o(lead)`
3. `humidity_generator_service.prepare_humidity_generator(lead)`
4. `temperature_control_service.set_temperature_for_point(lead, phase="h2o")`
5. `humidity_generator_service.wait_humidity_generator_stable(lead)`
6. `temperature_control_service.capture_temperature_calibration_snapshot(lead, route_type="h2o")`
7. `dewpoint_alignment_service.open_h2o_route_and_wait_ready(lead)`
   - vent on
   - H2O path open
   - dewpoint meter ready
   - H2O pre-seal soak
8. `dewpoint_alignment_service.wait_dewpoint_alignment_stable(lead)`
9. `valve_routing_service.mark_post_h2o_co2_zero_flush_pending()`
10. `pressure_control_service.pressurize_and_hold(lead, route="h2o")`
11. For each H2O pressure point:
    - `pressure_control_service.set_pressure_to_target(sample_point)`
    - `pressure_control_service.wait_after_pressure_stable_before_sampling(sample_point)`
    - `sampling_service.sample_point(sample_point, phase="h2o", point_tag=...)`
    - `qc_service.run_point_qc(sample_point, phase="h2o", point_tag=...)`
12. `valve_routing_service.cleanup_h2o_route(...)`

### H2O Sequence Invariants

These behaviors must remain aligned with V1:

- H2O route must not be sealed before humidity-generator wait and dewpoint
  readiness/alignment gates have completed or explicitly timed out under an
  allowed explicit timeout policy.
- Post-H2O CO2 zero-flush pending must be marked before leaving H2O route.
- Sampling must happen only after:
  - route opened and aligned
  - route sealed
  - target pressure reached
  - post-pressure hold finished
- In current no-gas bench replacement validation, the first target is the
  V1-like early-stop path.
- In the current workspace, the latest reviewed H2O-only early-stop evidence is
  the temperature-timeout path:
  - `set_h2o_path`
  - `set_vent (precondition)`
  - `wait_temperature(timeout)`
  - `set_vent(after timeout)`
  - `cleanup`
  This is valid control-flow evidence for early-stop consistency only. It is
  not evidence that full seal/sample equivalence has been proven.

### H2O Config-Driven Parameters

The following remain configuration-driven and are allowed to differ by scenario:

- `workflow.stability.temperature.*`
- `workflow.humidity_generator.*`
- `workflow.stability.h2o_route.preseal_soak_s`
- `workflow.stability.h2o_route.humidity_timeout_policy`
- `workflow.stability.dewpoint.*`
- `workflow.pressure.pressurize_wait_after_vent_off_s`
- `workflow.pressure.post_stable_sample_delay_s`
- `workflow.collect_only`
- `workflow.collect_only_fast_path`

Current policy boundary:

- replacement validation should default to `abort_like_v1`
- `continue_after_timeout` is an explicit engineering shortcut, not the default
  replacement-validation claim
- quick engineering presets that use `continue_after_timeout` must be treated as
  bring-up helpers, not formal acceptance evidence

## CO2 Route Order Contract

### V1 Reference Order

In V1, a successful CO2 source route follows this order:

1. Wait for chamber temperature to reach and stabilize.
2. Capture temperature calibration snapshot.
3. Apply CO2 baseline route:
   - vent on
   - baseline valves restored
4. Open the selected CO2 source route.
5. Wait open-route conditioning soak.
   - includes first-point flush / zero-gas flush variants when configured
6. Pre-seal analyzer check step (currently skipped in both V1 and V2).
7. Begin pressure-seal preparation:
   - vent off
   - wait vent-off settle
   - close route valves to seal downstream volume
8. For each pressure point:
   - set pressure target
   - retry sealed-route pressure target when configured
   - wait post-pressure sample hold
   - sample
   - write result / QC
9. Cleanup CO2 route back to baseline.

### V2 Required Order

V2 must preserve the same operator-visible sequence:

1. `temperature_control_service.set_temperature_for_point(point, phase="co2")`
2. `temperature_control_service.capture_temperature_calibration_snapshot(point, route_type="co2")`
3. `valve_routing_service.set_co2_route_baseline(reason="before CO2 route conditioning")`
4. `valve_routing_service.set_valves_for_co2(point)`
5. `_wait_co2_route_soak_before_seal(point)`  
   This is still a runner/helper seam, but the behavior is contractual.
6. `pressure_control_service.pressurize_and_hold(point, route="co2")`
7. For each CO2 pressure point:
   - `pressure_control_service.set_pressure_to_target(sample_point)`
   - optional sealed-route retry
   - `pressure_control_service.wait_after_pressure_stable_before_sampling(sample_point)`
   - `sampling_service.sample_point(sample_point, phase="co2", point_tag=...)`
   - `qc_service.run_point_qc(sample_point, phase="co2", point_tag=...)`
8. `valve_routing_service.cleanup_co2_route(...)`

### CO2 Sequence Invariants

These behaviors must remain aligned with V1:

- CO2 source route must be conditioned at atmosphere before sealing.
- CO2 seal must happen after route conditioning, not before.
- For post-H2O or first-zero flush scenarios, the extra soak happens before
  pressure seal.
- Pressure retry, if configured, must happen inside the sealed route, not by
  silently reopening a different flow order.
- Sampling must happen only after:
  - route conditioning soak
  - seal
  - target pressure reached
  - post-pressure hold finished

### CO2 Config-Driven Parameters

The following remain configuration-driven and are allowed to differ by scenario:

- `workflow.stability.temperature.*`
- `workflow.stability.co2_route.preseal_soak_s`
- `workflow.stability.co2_route.first_point_preseal_soak_s`
- `workflow.stability.co2_route.post_h2o_zero_ppm_soak_s`
- `workflow.stability.co2_route.post_h2o_zero_ppm_values`
- `workflow.pressure.pressurize_wait_after_vent_off_s`
- `workflow.pressure.co2_post_h2o_vent_off_wait_s`
- `workflow.pressure.co2_reseal_retry_count`
- `workflow.pressure.co2_post_stable_sample_delay_s`

## Behaviors That Must Match V1

The following behaviors are non-negotiable replacement criteria:

1. Route baseline must be explicit before switching into a new route.
2. H2O and CO2 route conditioning must happen before pressure seal.
3. Pressure seal and pressure target control are distinct steps.
4. Sampling must not begin before post-pressure waiting completes.
5. Route cleanup must happen after the route loop, not before the last sample.
6. Failed readiness / wait / seal / pressure target steps must not be reported
   as completed successful points.

## Behaviors That May Remain Configurable

The following are allowed to vary by scenario or tuning pack:

- exact timeout values
- soak durations
- retry counts
- collect-only shortcuts explicitly allowed by config
- whether coefficient export is enabled for the current scenario

The order contract above still applies even when those numbers differ.

## Product Report Policy

V2 report policy is now explicitly mode-gated:

- `auto_calibration` may produce formal calibration reports
- `co2_measurement`, `h2o_measurement`, and `experiment_measurement` must stay
  on the measurement / test-report path

The product template direction is documented in
[v2_product_report_templates.md](/D:/gas_calibrator/docs/reporting/v2_product_report_templates.md).
That document defines the four report families and keeps them separate from the
current engineering exporters and raw run bundles.

## Verification Anchors

Current lightweight trace anchors:

- [test_h2o_route_runner.py](/D:/gas_calibrator/tests/v2/test_h2o_route_runner.py)::`test_h2o_route_runner_preserves_v1_ordering_contract`
- [test_h2o_route_runner.py](/D:/gas_calibrator/tests/v2/test_h2o_route_runner.py)::`test_h2o_route_runner_collect_only_abort_like_v1_by_default`
- [test_h2o_route_runner.py](/D:/gas_calibrator/tests/v2/test_h2o_route_runner.py)::`test_h2o_route_runner_collect_only_can_continue_after_humidity_timeout_when_policy_enables_it`
- [test_co2_route_runner.py](/D:/gas_calibrator/tests/v2/test_co2_route_runner.py)::`test_co2_route_runner_preserves_v1_ordering_contract`

Current top-level replacement anchors:

- [test_calibration_service.py](/D:/gas_calibrator/tests/v2/test_calibration_service.py)::`test_progress_callback_receives_state_updates`
- [test_calibration_service.py](/D:/gas_calibrator/tests/v2/test_calibration_service.py)::`test_sampling_results_are_recorded`
- [test_calibration_service.py](/D:/gas_calibrator/tests/v2/test_calibration_service.py)::`test_v2_replacement_contract_minimal_flow_persists_results_and_artifacts`

Recommended simulation smoke:

```powershell
python -m gas_calibrator.v2.scripts.run_v2 --config src/gas_calibrator/v2/configs/smoke_v2_minimal.json --simulation --headless
```

Recommended offline replay / compare regression:

```powershell
python -m gas_calibrator.v2.scripts.run_validation_replay --scenario co2_only_skip0_success_single_temp --report-root src/gas_calibrator/v2/output/v1_v2_compare --run-name replay_co2_success
python -m gas_calibrator.v2.scripts.run_simulated_compare --profile replacement_full_route_simulated --scenario full_route_success_all_temps_all_sources --report-root src/gas_calibrator/v2/output/v1_v2_compare --run-name sim_full_route_success
python -m gas_calibrator.v2.scripts.run_simulated_compare --profile replacement_skip0_co2_only_simulated --scenario sensor_precheck_mode2_partial_frame_fail --report-root src/gas_calibrator/v2/output/v1_v2_compare --run-name sim_mode2_partial
```

Replay governance note:

- offline replay writes run-local `*_latest.json` by default
- it must not overwrite the current real-bench primary latest unless
  `--publish-latest` is explicitly requested for a controlled review
- simulated compare publishes only profile-specific simulated latest indexes
  such as `replacement_full_route_simulated_latest.json`; these are developer
  coverage artifacts, not real acceptance evidence
- protocol-backed simulated runs must be marked `evidence_state =
  simulated_protocol`; replayed artifacts must remain `evidence_state = replay`
  and neither may override the real primary latest
- dry-run promotion plans may be generated for review readiness, but the
  current branch must not promote the real primary latest from simulated,
  replay, or diagnostic evidence

Current primary evidence posture:

- strict acceptance profile:
  `skip0_co2_only_replacement`
- diagnostic route-unblock profile:
  `skip0_co2_only_diagnostic_relaxed`
- strict main profile and relaxed diagnostic profile are not interchangeable
- a diagnostic route-entry result is never acceptance evidence by itself
- a simulated green result is never acceptance evidence by itself
- a CO2-only green result still does not prove:
  - `0 ppm` equivalence
  - H2O equivalence
  - full cutover readiness

## Out of Scope

This contract does not by itself prove:

- V1 and V2 numeric equivalence
- real-device tuning equivalence
- zero-gas residual equivalence
- full bench or production readiness
- default-entry cutover readiness

Those must be validated separately after the behavior order contract is
consistently satisfied.
