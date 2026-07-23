# V1.5 Main Integration Audit

Date: 2026-07-23

Status: integration review evidence; not real acceptance

## Decision

The final product should use:

- native mature V1.5 0613/0620/0621 as the production calibration core;
- neutral shared packages for storage, evidence, lineage, protocols, and reports;
- V2/Web as the product platform and simulation, replay, parity, resilience, and shadow-algorithm layer.

V2 is not deleted as a tree and V2 algorithms are not promoted into the production route.

## Why the Mature Branch Is Not Merged Wholesale

At the audit point:

- `origin/main` and `codex/v1.5-mature-route-execution-lock-20260714` have merge base
  `65cac423eb95c781b689fde347d0164664a7f39d`;
- `origin/main` has 595 commits not present in the mature branch;
- the mature branch has 235 commits not present in `origin/main`.

The raw tree difference also includes generated evidence, historical handoff packages, V2 platform
artifacts, and production runtime changes. A whole-branch merge would combine code history,
generated artifacts, and metrology behavior in one review. That is not an acceptable production
integration unit.

Integration therefore proceeds by bounded capability slices with contract and parity tests.

## Current Slice

This slice contains:

1. source guards that prevent the native V1.5 runtime from importing migrated or V2 execution paths;
2. movement of shared database connection and SQLAlchemy models into `gas_calibrator.storage`;
3. V2 storage compatibility forwards for existing callers;
4. architecture and repository hygiene evidence;
5. a read-only V2 module disposition inventory.
6. standalone PACE command classification and exclusive COM-lock support;
7. observational raw serial evidence for the pressure-controller connection;
8. headless-entry lock acquisition/release and fail-closed precheck evidence;
9. pressure-trace compatibility required by the no-write extended dewpoint diagnostic;
10. PACE manual-state baseline snapshots and startup-command audit evidence;
11. open-flow, analyzer-gate, and preseal PACE phase-profile checks;
12. live dewpoint monitoring while the filtered CO2 ratio is stabilizing;
13. a final dewpoint freshness check before VENT0 and route seal;
14. route-terminal failure propagation and explicit skipped-pressure-point evidence.

The new control behavior is bounded to fail-closed PACE/dewpoint gates and audited PACE startup
configuration. Startup configuration applies pressure units, active mode, and in-limits settings;
the pressure range is changed only when explicitly enabled. It does not issue calibration, zero,
identity, or coefficient writes.

The slice does not change `run_app.py`, calibration timing, valve routing, fitting formulas, device
identifiers, calibration coefficients, analyzer sampling rate, or the default V1/V2 entry boundary.

## PACE Audit Collection Closure

Unmodified `origin/main` contains:

- `tests/test_v1_5_pace_audit_guards.py`;
- `tests/test_v1_5_dewpoint_gate_extended_hold_diagnostic.py`;
- `gas_calibrator.tools.run_v1_5_dewpoint_gate_extended_hold_after_gate`.

Those files imported `gas_calibrator.pace_audit`, but that module and its dependent observational
logger/serial/entrypoint interfaces were not present in `origin/main`.

Consequently, `python -m pytest -m v1_5_formal_gate -q` fails during collection on unmodified
`origin/main`. A single-file stub was not used. The bounded integration restores the standalone
module, process lock, raw serial tap, headless-entry lifecycle, workflow-stage evidence, and
pressure-trace extension points as one tested capability.

After the initial observational closure:

- `python -m pytest -q -m v1_5_formal_gate`: 148 passed;
- entrypoint, production-map, extended-hold, COM-lock, and raw-tap selection: 73 passed;
- extended dewpoint hold diagnostic: 12 passed;
- summary parity, export resilience, and historical fit-profile parity: 16 passed.

The follow-on PACE control review compared those 35 remaining contracts against the
0613/0620/0621 mature native runner. The result is a bounded implementation rather than a whole
runner transplant:

- manual PACE state and vent-status evidence are captured before and after startup configuration;
- startup commands are classified and written to `pace_startup_config_audit.csv`;
- unexpected state-changing PACE writes during open-flow or analyzer stability fail closed;
- dewpoint is sampled during filtered-ratio stability without issuing additional PACE commands;
- a stale, missing, discontinuous, or no-longer-dry dewpoint state blocks VENT0 and route seal;
- the first terminal decision is preserved and every remaining selected pressure point is recorded
  as skipped.

Current verification for this reviewed control slice:

- `tests/test_v1_5_pace_audit_guards.py`: 50 passed;
- `python -m pytest -q -m v1_5_formal_gate`: 148 passed;
- native-entry, production-map, mature-route, formal-flow, serial-safety, and historical fit-profile
  selection: 202 passed;
- artifact hash, evidence registry, namespace, event snapshot, canonical package, and offline
  acceptance selection: 52 passed;
- Python compilation and `git diff --check`: passed.

Static checking still reports 16 pre-existing issues in `runner.py`; none are in this slice's added
PACE/dewpoint blocks. They remain repository cleanup work and are not hidden by this audit.

Likewise, the bulk-backfilled default-global no-write-guard tests are not used to redefine future
controlled coefficient-write semantics. The current implementation still lacks that independent
global guard, so its 10 dedicated tests remain outside this release gate. Enabling it by default
would change controlled production write behavior and requires an explicit product and governance
decision.

## A1 Fixture Reconciliation

The A1 validation configuration was updated on 2026-04-25 from the original sequential fixture to
the identity-audited four-device fixture:

- `GA01 / COM35 / 001`;
- `GA02 / COM37 / 029`;
- `GA03 / COM41 / 003`;
- `GA04 / COM42 / 004`.

The same mapping is retained by the A1R, A2, and R1 validation configurations and their offline
contract tests. One older diagnostic test still expected the superseded `COM36/COM37/COM38`
sequence, and two older artifact tests expected only the original six artifacts after temperature
stability and effective-fleet evidence had been added. Only those stale assertions were updated;
runtime configuration, device access, and real-COM behavior were not changed.

The complete Run-001 A1 offline contract selection now passes: 142 passed.

## V2 Disposition Inventory

The inventory is generated by:

```text
python -m gas_calibrator.tools.export_v2_module_disposition_inventory
```

Each Python module is classified as:

- `platform_keep`;
- `shadow_algorithm_keep`;
- `shared_migration_candidate`;
- `compatibility_wrapper`;
- `archive_review`.

`archive_review` means manual review only. The inventory never authorizes automatic deletion.
It also scans protected V1.5 paths for real Python imports of `gas_calibrator.v2`.

## Next Integration Slices

1. Review the global no-write product policy separately from the completed PACE/dewpoint safety
   contracts; do not make it the production default implicitly.
2. Move remaining neutral persistence code from `v2.storage` into `gas_calibrator.storage`, one
   contract-tested group at a time.
3. Keep V2 algorithm and execution code simulation/replay/shadow-only until independent real
   acceptance is completed.
4. Preserve the V1 fallback and keep the default entry unchanged throughout the integration.
