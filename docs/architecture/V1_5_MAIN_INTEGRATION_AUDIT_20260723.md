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
14. route-terminal failure propagation and explicit skipped-pressure-point evidence;
15. semantics-preserving cleanup of the native mature runner's remaining static findings;
16. shared ownership of the file/SQLite sidecar index with V2 compatibility exports;
17. shared ownership of the read-only history query service with V2 compatibility exports;
18. shared ownership of the offline run-artifact importer with V2 compatibility exports;
19. shared ownership of coefficient-version metadata persistence with V2 compatibility exports.

The new control behavior is bounded to fail-closed PACE/dewpoint gates and audited PACE startup
configuration. Startup configuration applies pressure units, active mode, and in-limits settings;
the pressure range is changed only when explicitly enabled. It does not issue calibration, zero,
identity, or coefficient writes.

The slice does not change `run_app.py`, calibration timing, valve routing, fitting formulas, device
identifiers, calibration coefficients, analyzer sampling rate, or the default V1/V2 entry boundary.
The runner cleanup only removes unused local assignments and an unreachable return, and restores
the missing `Iterable` type import.
The sidecar index keeps its existing API, record normalization, collection names, file schema, and
SQLite schema. `gas_calibrator.v2.storage.sidecar_index` is now a compatibility forwarder to
`gas_calibrator.storage.sidecar_index`, and V2 analytics consumers use the shared implementation
directly.
The history query service also keeps its existing API and serialized result fields.
`gas_calibrator.v2.storage.queries` is now a compatibility forwarder to
`gas_calibrator.storage.queries`, and the V2 storage exporter uses the shared query service
directly. This ownership change does not add schema migration, persistence writes, coefficient
deployment, or calibration execution behavior.
The artifact importer likewise keeps its existing public class, raw/enrich/all stage semantics,
normalization rules, schema mappings, batching, and idempotent update behavior.
`gas_calibrator.v2.storage.importer` is now a compatibility forwarder to
`gas_calibrator.storage.importer`, and the V2 import CLI uses the shared owner directly. The V2
storage exporter remains V2-owned because it also generates V2 acceptance and product-report
artifacts. This change does not alter database schemas, transaction boundaries, write policy, or
calibration execution.
The coefficient version store now has the same shared ownership. Its existing save, approval,
deployment-marker, lookup, history, and rollback-as-new-version behavior is preserved, and
`gas_calibrator.v2.storage.coefficient_store` is an identity-preserving compatibility forwarder.
The word `deploy` in this API updates database metadata only; it does not communicate with an
analyzer or write SENCO coefficients. Transaction testing confirms that a failed deployment-marker
update rolls back both the old and candidate marker changes.

Two existing policy limits are intentionally not changed by this namespace migration:

- the store currently permits an unapproved version to be marked as deployed;
- version allocation uses `max(version) + 1`, with the database uniqueness constraint surfacing a
  concurrent collision rather than an internal retry.

Approval enforcement and concurrent version-allocation policy require a separate product and
governance decision. This slice does not silently redefine either behavior.

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
- cleanup-focused pressure selection, post-isolation diagnostics, temperature soak, humidity
  reach, and sample aggregation selection: 89 passed;
- stable historical runner audit, collect-only, pressure-order, route-handoff, and write-safety
  selection: 142 passed;
- sidecar namespace, compatibility, file/SQLite backend, package-lazy-load, and disposition
  selection: 12 passed;
- sidecar result-center and device-workbench consumer selection: 4 passed;
- query namespace, compatibility identity, history lookup, sensor identity, sample, fit,
  coefficient-history, statistics, and export selection: 13 passed;
- artifact-importer namespace, compatibility identity, raw/enrich/all stages, idempotency, package
  initialization, and import/export integration selection: 17 passed;
- coefficient-store namespace, compatibility identity, version lifecycle, deployment transaction
  rollback, lazy package loading, and query integration selection: 16 passed;
- summary parity, export resilience, historical fit-profile parity, offline artifacts, and offline
  governance artifacts: 30 passed;
- Ruff checks for `runner.py` and the modified storage/consumer/test modules: passed;
- Python compilation and `git diff --check`: passed.

The 16 pre-existing Ruff findings in `runner.py` are now closed without changing mature route
behavior. A broader historical runner selection still contains stale assertions for older
VENT-call signatures, preseal callback arguments, route ordering, and preseal-ready state shape.
Four representative failures reproduce unchanged on the unmodified parent commit
`dc69f6892`; they are recorded as pre-existing test debt and are not resolved by reverting current
mature contracts.

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
2. Review `profile_store.py` separately to distinguish neutral profile persistence from V2
   product-profile policy before moving any further storage code.
3. Keep V2 algorithm and execution code simulation/replay/shadow-only until independent real
   acceptance is completed.
4. Preserve the V1 fallback and keep the default entry unchanged throughout the integration.
