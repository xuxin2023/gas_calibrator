# V1.5 Completion and Gap Assessment

Date: 2026-06-05

This assessment is scoped to the V1.5 formal calibration program. It does not
promote V2, it does not change `run_app.py`, and it does not authorize hidden
real-device operation. The purpose is to make the current V1.5 state readable
despite the repository containing many legacy, diagnostic, and review tools.

## Formal Route

The current V1.5 formal route is:

```text
device precheck
pressure-channel quick verification
optional pressure-channel correction and reverify
temperature-channel evidence review
open-flow CO2 sampling
open-flow H2O sampling
QC classification and point review
candidate coefficient review
controlled write, when explicitly authorized
post-write reverify
archive, database index, and reports
```

Physical meaning:

- CO2/H2O main calibration is based on open-flow samples, where standard gas or
  generated humidity continuously refreshes the analyzer cavity and downstream
  line.
- Analyzer pressure P is checked separately before CO2/H2O fitting so pressure
  error is not absorbed into concentration coefficients.
- Temperature evidence is reviewed separately because chamber/case temperature
  affects compensation and multi-temperature interpretation.
- Sealed-pressure points, VENT-hold, dynamic pressure probes, and PACE
  continuous-sink experiments remain diagnostic unless a future reviewed method
  explicitly promotes them.

## Start-Here Inventory

The authoritative entrypoint inventory is generated here:

```text
docs/v1_5_entrypoint_inventory/v1_5_formal_entrypoints.md
docs/v1_5_entrypoint_inventory/v1_5_entrypoint_inventory.json
docs/v1_5_entrypoint_inventory/v1_5_entrypoint_inventory.csv
```

Regenerate it with:

```powershell
python -m gas_calibrator.tools.export_v1_5_entrypoint_inventory `
  --repo-root . `
  --output-dir docs\v1_5_entrypoint_inventory
```

Use the `Canonical V1.5 Formal Path` section before searching through the
whole repository. It is the first navigation layer for formal work.

## Current Completion Matrix

| Layer | State | Evidence | Remaining boundary |
| --- | --- | --- | --- |
| Full-flow order | Implemented as an offline/supervised sequence contract. | `tests/test_v1_5_full_flow_orchestration.py` | Do not treat the planner as an implicit real-device runner. |
| Identity and GETCO | Implemented as read-only epoch-0 evidence. | GETCO snapshot tooling and inventory tests. | Device identity is analyzer ID, not COM alias or GA label. |
| Serial binding | Implemented as runtime binding evidence. | Serial binding tools and full-flow plan metadata. | COM bank-shift probing is optional and default-off. |
| Pressure channel | Validation, completion package, SENCO9 review, and controlled write tools exist. | Pressure-channel validation/completion tests. | Pressure correction remains separate from CO2/H2O fitting. |
| Temperature channel | Review tooling exists from evidence. | Temperature review and full-flow tests. | Temperature coefficient calibration is not automatic. |
| CO2 open-flow sampling | Formal runner exists and is route/COM-risk classified. | CO2 open-flow artifacts, candidate, report, reverify tests. | Only clean open-flow A-grade rows enter formal CO2 fitting. |
| H2O open-flow sampling | Formal runner exists and is route/COM-risk classified. | H2O candidate/review/report evidence and tests. | H2O dry-gas low-water anchors require dewpoint/reference support. |
| Candidate coefficients | Offline candidate and write-review tools exist. | Candidate coefficient and candidate write-review tests. | Diagnostic or rejected rows do not silently enter fitting. |
| Controlled writes | SENCO write tools exist as manually authorized entrypoints. | Controlled-write tests and inventory risk flags. | Never execute writes from archive/report automation. |
| Post-write reverify | Reverify exporter exists. | Post-write reverify tests. | A write is incomplete without reverify evidence. |
| Evidence database | Bundle, migration, import, query, pressure package, and run-status tools exist. | Evidence registry, pressure completion DB, archive closure tests. | PostgreSQL operation remains deployment/run specific. |
| Reports | Draft/formal report generators exist, including Chinese coefficient reports. | Calibration report tests and archive closure tests. | Formal certificate release still needs run-specific approval metadata and uncertainty budget. |
| UI | Review/sidecar surfaces exist. | Operation console and review-surface tests. | A new V1.5 live operator UI is still pending and must remain no-write by default. |

## What Is Complete Enough to Use as V1.5 Backbone

- The formal open-flow route decision is implemented in code, docs, and tests.
- MODE2/factory/raw evidence contracts are preserved through sample, QC, report,
  and evidence artifacts.
- Pressure-channel verification and pressure coefficient handling are separate
  from CO2/H2O fitting.
- CO2/H2O controlled-write tools are separated from no-write review and archive
  tools.
- Evidence package, registry, report, and archive closure paths exist.
- Entrypoint inventory separates formal runners, review tools, controlled-write
  tools, diagnostics, tests, storage, UI, and support libraries.

## Remaining Work

### P0: Keep the canonical inventory current

Every time a V1.5 formal runner, controlled-write tool, or review artifact is
added, update the inventory tests if the tool belongs in the canonical path.

### P0: Keep fitting contracts traceable

CO2 zero-gas anchors and H2O dry-gas anchors are different physical concepts.
CO2 fitting can use low-CO2 evidence for the CO2 baseline. H2O fitting should
use dry-gas evidence only when dewpoint/reference evidence proves the water
state. Do not reuse a low-end point across gases without physical evidence.

### P1: Finish live V1.5 operator UI

The old V1 UI can be a reference, but the V1.5 UI should be a new no-write
operator console first. It should show identity, pressure quick-check, chamber
temperature, CO2/H2O ratio stability, dewpoint, QC blockers, candidate review,
and report/archive status.

### P1: Improve current-state normalization and reverify workflows

Recent CO2/H2O work showed that the same nominal gas can produce different
ratios when humidity, dewpoint, pressure, temperature, and line state differ.
The next fitting workflow should preserve those state variables and evaluate
whether data can be normalized or bridged before forcing a rerun.

### P1: Strengthen report release criteria

Draft reports must remain visibly different from released calibration
certificates. Formal release needs uncertainty budget, reviewer/approver
metadata, write/readback evidence when applicable, and post-write reverify.

### P2: Archive or hide obsolete one-off scripts

Do not delete evidence. Instead, use inventory classification, archive notes, or
documentation to mark one-off scripts as diagnostic or historical so they do not
look like formal entrypoints.

## Not Recommended

- Do not connect V2 to real COM.
- Do not use sealed-pressure or dynamic-pressure diagnostic data as formal
  CO2/H2O fitting evidence.
- Do not clear or write SENCO coefficients from review/report/archive tools.
- Do not change `run_app.py` as part of V1.5 cleanup.
- Do not treat simulated/replay evidence as real acceptance evidence.
